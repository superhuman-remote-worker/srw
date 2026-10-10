"""Owner-level regression tests for extracted B09 job controls."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services import job_control_delivery as job_control_delivery_module
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.job_control_delivery import (
    JobDeliveryDependencies,
    dispatch_job_to_agent,
    initiate_pause,
    resume_job_on_agent,
)
from orchestrator.services.job_start_bundle import JobStartRefusal
from orchestrator.services.model_availability import (
    WHERE_JOB,
    ModelUnavailable,
    UnavailableModel,
)
from orchestrator.services.workspace_tier_policy import LiteWorkspaceConfigError
from orchestrator.services.job_mutation_controls import (
    JobControlDependencies,
    JobControlOperations,
)


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class _Payload:
    managed_repository_credentials: list[dict] = []

    def __init__(self, values=None):
        self.values = values or {}

    def model_copy(self, *, update):
        return _Payload({**self.values, **update})

    def model_dump(self, **_kwargs):
        return self.values


def _controls(store, **overrides) -> JobControlOperations:
    control = SimpleNamespace(
        claim_pause=AsyncMock(return_value=SimpleNamespace(claim_id="claim")),
        abort=AsyncMock(),
        active_claim=MagicMock(return_value=False),
        claim_detail=MagicMock(return_value="completion finalizing"),
        dispatch_guard_kwargs=MagicMock(return_value={}),
    )
    values = dict(
        store=store,
        logger=logging.getLogger(__name__),
        completion_commands_enabled=lambda: True,
        completion_control=control,
        manifest_cancel=AsyncMock(return_value=False),
        prepare_pinned_mutation_target=AsyncMock(return_value=None),
        archive_and_cleanup_workspace=AsyncMock(),
        http_client_factory=MagicMock(),
        handle_scholar_completion=AsyncMock(),
        maybe_wake_session=AsyncMock(),
        kick_session_wake_drain=MagicMock(),
        trigger_dispatch=MagicMock(),
        resolve_job_notifications=AsyncMock(),
        snapshot_service=SimpleNamespace(is_available=False),
        gitea_client=SimpleNamespace(is_initialized=False),
        revoke_and_delete_managed_repository=AsyncMock(return_value=True),
        vector_db=SimpleNamespace(
            acquire=MagicMock(return_value=_AsyncContext(AsyncMock()))
        ),
    )
    values.update(overrides)
    return JobControlOperations(JobControlDependencies(**values))


@pytest.mark.asyncio
async def test_pinned_cancel_commits_before_retirement_and_prunes_last():
    events: list[str] = []
    store = SimpleNamespace(
        manifests_ready=False,
        linearize_pinned_cancel=AsyncMock(
            side_effect=lambda *_a, **_k: events.append("linearize") or True
        ),
        get_descendant_jobs=AsyncMock(return_value=[]),
        cancel_job=AsyncMock(side_effect=lambda *_a: events.append("cancel") or True),
        delete_checkpoint_thread=AsyncMock(
            side_effect=lambda *_a: events.append("checkpoint")
        ),
    )
    cleanup = AsyncMock(side_effect=lambda *_a: events.append("cleanup"))
    operations = _controls(store, archive_and_cleanup_workspace=cleanup)

    result = await operations.cancel(
        "job-1",
        job={
            "id": "job-1",
            "runtime_kind": "srw",
            "execution_lane": "pinned",
            "status": "processing",
        },
    )

    assert result == {"status": "cancelled"}
    assert events == ["linearize", "cleanup", "cancel", "checkpoint"]


@pytest.mark.asyncio
async def test_pause_retains_claim_without_positive_recipient_quiescence():
    store = SimpleNamespace(get_descendant_jobs=AsyncMock(return_value=[]))
    target = AsyncMock(return_value=None)
    operations = _controls(store, prepare_pinned_mutation_target=target)

    result = await operations.pause(
        "job-1",
        job={
            "id": "job-1",
            "runtime_kind": "srw",
            "execution_lane": "pinned",
            "status": "processing",
            "assigned_agent_id": "agent-old",
        },
    )

    assert result["status"] == "paused"
    operations.dependencies.completion_control.abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_pause_holds_the_job_but_cascaded_children_stay_dispatchable():
    child = {
        "id": "child-1",
        "status": "processing",
        "execution_lane": "pinned",
        "assigned_agent_id": "agent-child",
    }
    store = SimpleNamespace(get_descendant_jobs=AsyncMock(return_value=[child]))
    operations = _controls(store)

    await operations.pause(
        "job-1",
        job={
            "id": "job-1",
            "execution_lane": "pinned",
            "status": "processing",
            "assigned_agent_id": "agent-old",
        },
        paused_by="user-a",
    )

    claims = operations.dependencies.completion_control.claim_pause.await_args_list
    assert claims[0].args == ("job-1",)
    assert claims[0].kwargs == {
        "source": "public_pause",
        "expected_agent_id": "agent-old",
        "operator_hold": True,
        "paused_by": "user-a",
    }
    # Children wait behind the paused parent's ancestor guard and must run
    # again when the parent is resumed, so they never get their own hold.
    assert claims[1].kwargs == {
        "source": "cascade_pause",
        "expected_agent_id": "agent-child",
    }


@pytest.mark.asyncio
async def test_stateless_public_pause_holds_but_worker_release_does_not():
    store = SimpleNamespace(
        get_descendant_jobs=AsyncMock(return_value=[]),
        pause_stateless_job=AsyncMock(return_value=True),
        get_job=AsyncMock(return_value={"id": "job-1", "execution_lane": "stateless"}),
    )
    operations = _controls(store)

    await operations.pause(
        "job-1",
        job={"id": "job-1", "execution_lane": "stateless", "status": "processing"},
        paused_by="user-a",
    )
    await operations.release("job-1", lease_token=7)

    public, release = store.pause_stateless_job.await_args_list
    assert public.kwargs == {"operator_hold": True, "paused_by": "user-a"}
    assert release.kwargs == {
        "completion_commands_enabled": True,
        "expected_lease_token": 7,
    }


@pytest.mark.asyncio
async def test_legacy_public_pause_writes_the_hold_with_the_status_flip():
    store = SimpleNamespace(
        get_descendant_jobs=AsyncMock(return_value=[]),
        pause_job=AsyncMock(return_value=True),
    )
    operations = _controls(store, completion_commands_enabled=lambda: False)

    result = await operations.pause(
        "job-1",
        job={"id": "job-1", "execution_lane": "pinned", "status": "processing"},
    )

    assert result == {"status": "paused", "job_id": "job-1"}
    store.pause_job.assert_awaited_once_with(
        "job-1", operator_hold=True, paused_by=None
    )


@pytest.mark.asyncio
async def test_delete_refuses_non_owner_before_any_retirement_effect():
    store = SimpleNamespace(get_user_role_in_project=AsyncMock(return_value="viewer"))
    cleanup = AsyncMock()
    operations = _controls(store, archive_and_cleanup_workspace=cleanup)

    with pytest.raises(HTTPException) as raised:
        await operations.delete(
            "job-1",
            caller={"id": "other", "is_admin": False},
            job={
                "id": "job-1",
                "runtime_kind": "srw",
                "user_id": "owner",
                "project_id": "project-1",
            },
        )

    assert raised.value.status_code == 403
    cleanup.assert_not_awaited()


def _delivery(**overrides) -> JobDeliveryDependencies:
    values = {
        field: MagicMock() for field in JobDeliveryDependencies.__dataclass_fields__
    }
    values.update(
        store=SimpleNamespace(),
        logger=logging.getLogger(__name__),
        completion_commands_enabled=lambda: True,
        pause_pending_job_ids=set(),
    )
    values.update(overrides)
    return JobDeliveryDependencies(**values)


@pytest.mark.asyncio
async def test_dispatch_refuses_stateless_job_before_any_delivery_effect():
    prepare = AsyncMock()
    dependencies = _delivery(prepare_job_workspace_runtime=prepare)

    assert not await dispatch_job_to_agent(
        {"id": "job-1", "runtime_kind": "srw", "execution_lane": "stateless"},
        {"id": "agent-1", "pod_ip": "10.0.0.1"},
        dependencies=dependencies,
    )
    prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_confirms_command_owner_after_exact_recipient_accepts():
    recipient = SimpleNamespace(
        model_dump=MagicMock(return_value={"agent_id": "agent-1", "generation": 3})
    )
    target = SimpleNamespace(
        agent={"pod_ip": "10.0.0.2", "pod_port": 8001}, recipient=recipient
    )
    response = SimpleNamespace(status_code=202)
    client = SimpleNamespace(post=AsyncMock(return_value=response))
    store = SimpleNamespace(
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
        confirm_pinned_job_dispatch=AsyncMock(return_value=True),
        update_job_status=AsyncMock(),
        heartbeat=AsyncMock(),
    )
    dependencies = _delivery(
        store=store,
        prepare_job_workspace_runtime=AsyncMock(
            side_effect=lambda job: ("proceed", job, None)
        ),
        attest_pinned_k8s_job_workspace=AsyncMock(side_effect=lambda job: (job, None)),
        build_job_start_request=AsyncMock(return_value=_Payload()),
        pinned_k8s_job_workspace_authority_is_current=AsyncMock(return_value=True),
        prepare_pinned_job_mutation_target=AsyncMock(return_value=target),
        redispatch_livelock_trip=MagicMock(return_value=None),
        bind_log_context=MagicMock(return_value="token"),
        reset_log_context=MagicMock(),
        http_client_factory=MagicMock(return_value=_AsyncContext(client)),
    )

    assert await dispatch_job_to_agent(
        {"id": "job-1", "runtime_kind": "srw", "execution_lane": "pinned"},
        {"id": "agent-1", "pod_ip": "10.0.0.1"},
        dependencies=dependencies,
    )

    dependencies.prepare_pinned_job_mutation_target.assert_awaited_once_with(
        agent_id="agent-1", job_id="job-1", require_idle=True
    )
    store.confirm_pinned_job_dispatch.assert_awaited_once_with(
        "job-1", "agent-1", pinned_delivery_id=None,
        pinned_projection_digest=None,
    )
    store.update_job_status.assert_not_awaited()
    assert client.post.await_args.kwargs["json"]["recipient"] is recipient


@pytest.mark.asyncio
async def test_legacy_dispatch_does_not_resurrect_a_control_that_won_after_claim():
    """Flag off: a pause/cancel landing during delivery must keep its row."""
    recipient = SimpleNamespace(model_dump=MagicMock(return_value={}))
    target = SimpleNamespace(
        agent={"pod_ip": "10.0.0.2", "pod_port": 8001}, recipient=recipient
    )
    client = SimpleNamespace(
        post=AsyncMock(return_value=SimpleNamespace(status_code=202))
    )
    store = SimpleNamespace(
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
        # The row is no longer processing: the status CAS loses.
        update_job_status=AsyncMock(return_value=False),
        heartbeat=AsyncMock(),
    )
    dependencies = _delivery(
        store=store,
        completion_commands_enabled=lambda: False,
        prepare_job_workspace_runtime=AsyncMock(
            side_effect=lambda job: ("proceed", job, None)
        ),
        attest_pinned_k8s_job_workspace=AsyncMock(side_effect=lambda job: (job, None)),
        build_job_start_request=AsyncMock(return_value=_Payload()),
        pinned_k8s_job_workspace_authority_is_current=AsyncMock(return_value=True),
        prepare_pinned_job_mutation_target=AsyncMock(return_value=target),
        redispatch_livelock_trip=MagicMock(return_value=None),
        bind_log_context=MagicMock(return_value="token"),
        reset_log_context=MagicMock(),
        http_client_factory=MagicMock(return_value=_AsyncContext(client)),
    )

    assert not await dispatch_job_to_agent(
        {"id": "job-1", "runtime_kind": "srw", "execution_lane": "pinned"},
        {"id": "agent-1", "pod_ip": "10.0.0.1"},
        dependencies=dependencies,
    )

    store.update_job_status.assert_awaited_once_with(
        job_id="job-1",
        status="processing",
        assigned_agent_id="agent-1",
        expected_status="processing",
    )
    store.heartbeat.assert_not_awaited()


# A refused start under completion commands (connector drivers decision 34):
# the claim owner submits it through the completion-command path under its
# agent fence instead of leaving the claimed job to its lease.

JOB_ID = "00000000-0000-0000-0000-0000000000a1"


def _refusing_delivery(*, commands_on: bool, build) -> JobDeliveryDependencies:
    return _delivery(
        store=SimpleNamespace(update_job_status=AsyncMock()),
        completion_commands_enabled=lambda: commands_on,
        prepare_job_workspace_runtime=AsyncMock(
            side_effect=lambda job: ("proceed", job, None)
        ),
        attest_pinned_k8s_job_workspace=AsyncMock(side_effect=lambda job: (job, None)),
        build_job_start_request=build,
        prepare_pinned_job_mutation_target=AsyncMock(),
        redispatch_livelock_trip=MagicMock(return_value=None),
        bind_log_context=MagicMock(return_value="token"),
        reset_log_context=MagicMock(),
        refuse_job_start=AsyncMock(return_value=True),
    )


@pytest.mark.asyncio
async def test_a_refused_start_fails_the_job_through_the_completion_ledger():
    refusal = JobStartRefusal("unrouted_model", "Pinned model(s) have no endpoint")
    dependencies = _refusing_delivery(
        commands_on=True, build=AsyncMock(return_value=refusal)
    )

    assert not await dispatch_job_to_agent(
        {"id": JOB_ID, "execution_lane": "pinned"},
        {"id": "agent-1", "pod_ip": "10.0.0.1"},
        dependencies=dependencies,
    )

    build_kwargs = dependencies.build_job_start_request.await_args.kwargs
    assert build_kwargs["persist_dispatch_state"] is False
    dependencies.refuse_job_start.assert_awaited_once_with(
        JOB_ID,
        reason="unrouted_model",
        message="Pinned model(s) have no endpoint",
        # A fresh start: the finalizer tears down what was provisioned.
        resume=False,
        agent_id="agent-1",
    )
    # Nothing reaches the agent, and the status is the ledger's to write.
    dependencies.prepare_pinned_job_mutation_target.assert_not_awaited()
    dependencies.store.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_completion_commands_off_the_build_writes_the_refusal():
    dependencies = _refusing_delivery(
        commands_on=False, build=AsyncMock(return_value=None)
    )

    assert not await dispatch_job_to_agent(
        {"id": JOB_ID, "execution_lane": "pinned"},
        {"id": "agent-1", "pod_ip": "10.0.0.1"},
        dependencies=dependencies,
    )

    build_kwargs = dependencies.build_job_start_request.await_args.kwargs
    assert build_kwargs["persist_dispatch_state"] is True
    dependencies.refuse_job_start.assert_not_awaited()


def test_pinned_delivery_routes_refusals_to_the_completion_ledger():
    import orchestrator.main as main
    from orchestrator.application import controls as controls_composition
    from orchestrator.services import job_completion

    dependencies = controls_composition.job_delivery_operations(
        main.app.state.resources
    ).dependencies

    assert dependencies.refuse_job_start.__wrapped__ is job_completion.refuse_job_start


def _unavailable_model() -> ModelUnavailable:
    return ModelUnavailable([UnavailableModel("llm", "MiniMax-M3", "disabled")])


def _resume_refusal(case: str, *, commands_on: bool, monkeypatch):
    """A resume that reaches one refusal; returns (dependencies, message).

    ``message`` is ``None`` where the resume path composes the text itself.
    """

    decision = SimpleNamespace(
        ready=True,
        state="ready",
        effective_backend="virtual",
        reason=None,
        safe_projection=lambda: {},
    )
    values = dict(
        store=SimpleNamespace(
            update_job_status=AsyncMock(),
            fetchrow=AsyncMock(return_value=None),
        ),
        completion_commands_enabled=lambda: commands_on,
        prepare_job_workspace_runtime=AsyncMock(
            side_effect=lambda job: ("proceed", job, None)
        ),
        attest_pinned_k8s_job_workspace=AsyncMock(side_effect=lambda job: (job, None)),
        redispatch_livelock_trip=MagicMock(return_value=None),
        resume_missing_workspace=MagicMock(return_value=None),
        resolve_authorized_job_datasources=AsyncMock(return_value=[]),
        apply_cloud_storage_override=MagicMock(),
        build_datasources_payload=MagicMock(return_value=None),
        job_project_repositories=AsyncMock(return_value=None),
        build_datasource_tool_override=MagicMock(side_effect=lambda ds, co: co),
        inject_matching_workspace_config=MagicMock(
            side_effect=lambda job, co, **kw: (co or {}, decision)
        ),
        authorize_job_repository_transport=AsyncMock(return_value=(None, None, None)),
        apply_sticky_sudo_denial=MagicMock(side_effect=lambda job, co: co),
        backend_from_override=MagicMock(return_value=None),
        shell_connector_names=MagicMock(return_value=[]),
        inject_lite_workspace_config=MagicMock(side_effect=lambda co, **kw: co),
        is_experts_db_enabled=MagicMock(return_value=False),
        user_experts_enabled=AsyncMock(return_value=False),
        inject_dispatch_credentials=AsyncMock(side_effect=lambda job, co, **kw: co),
        grant_violations_detail=MagicMock(
            side_effect=lambda violations: f"denied: {violations[0]}"
        ),
        refuse_job_start=AsyncMock(return_value=True),
    )
    message: str | None
    if case == "connector_unavailable":
        values["resolve_authorized_job_datasources"] = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="gone")
        )
        message = "connector_unavailable"
    elif case == "lite_shell_connector":
        values["resolve_authorized_job_datasources"] = AsyncMock(
            return_value=[{"id": "d1", "type": "repository", "name": "app"}]
        )
        values["backend_from_override"] = MagicMock(return_value="virtual")
        values["shell_connector_names"] = MagicMock(return_value=["app"])
        message = None
    elif case == "lite_config":
        values["backend_from_override"] = MagicMock(return_value="virtual")
        values["inject_lite_workspace_config"] = MagicMock(
            side_effect=LiteWorkspaceConfigError("no object store configured")
        )
        message = "no object store configured"
    elif case == "grant_denied":
        values.update(
            user_experts_enabled=AsyncMock(return_value=True),
            gather_in_scope_skills=AsyncMock(return_value=[]),
            seed_registry_model_overrides=AsyncMock(return_value={}),
            resolve_default_models=AsyncMock(return_value={}),
            prefetch_roster_refs=AsyncMock(return_value={}),
            enforce_dispatch_grants=AsyncMock(
                side_effect=GrantDenied(["tools.shell not granted"])
            ),
        )
        monkeypatch.setattr(
            job_control_delivery_module,
            "resolve_config",
            lambda **kwargs: (kwargs["capture"].__setitem__("merged_fragment", {}))
            or {"llm": {}},
        )
        message = "denied: tools.shell not granted"
    elif case == "model_unavailable":
        values["inject_dispatch_credentials"] = AsyncMock(
            side_effect=_unavailable_model()
        )
        message = _unavailable_model().message(where=WHERE_JOB)
    elif case == "grant_denied/frozen":
        # Current jobs resume from their frozen execution snapshot.
        from orchestrator.services import manifest_execution_snapshot

        monkeypatch.setattr(
            manifest_execution_snapshot,
            "read_execution",
            AsyncMock(return_value={"frozen": True}),
        )
        monkeypatch.setattr(
            manifest_execution_snapshot,
            "srw_snapshot_config",
            lambda _snapshot: ({"agent": {"llm": {}}}, {}),
        )
        monkeypatch.setattr(
            manifest_execution_snapshot,
            "apply_srw_delivery_bindings",
            lambda blob, policy, _override: (blob, policy),
        )
        ready = SimpleNamespace(
            status_code=200,
            json=lambda: {"capabilities": {"resolved_config_resume": True}},
        )
        values.update(
            http_client_factory=MagicMock(
                return_value=_AsyncContext(
                    SimpleNamespace(get=AsyncMock(return_value=ready))
                )
            ),
            user_experts_enabled=AsyncMock(return_value=True),
            enforce_dispatch_grants=AsyncMock(
                side_effect=GrantDenied(["tools.shell not granted"])
            ),
        )
        message = "denied: tools.shell not granted"
    elif case in {"workspace_contract", "workspace_waiting"}:
        definitive = case == "workspace_contract"
        values["inject_matching_workspace_config"] = MagicMock(
            side_effect=lambda job, co, **kw: (
                co or {},
                SimpleNamespace(
                    ready=False,
                    state="failed" if definitive else "pending",
                    effective_backend="sandbox",
                    reason="sandbox_provisioning_failed" if definitive else None,
                    safe_projection=lambda: {},
                ),
            )
        )
        message = "Workspace contract refused dispatch: sandbox_provisioning_failed"
    elif case in {"connector_bind_refused", "connector_bind_pending"}:
        from orchestrator.services import (
            connector_bind_time,
            connector_credential_leases,
        )

        error = (
            connector_bind_time.BindTimeRefused("connector 'gitea' failed to bind")
            if case == "connector_bind_refused"
            else connector_bind_time.BindTimePending("still binding")
        )
        monkeypatch.setattr(
            connector_credential_leases, "prepare_lease_delivery", AsyncMock()
        )
        monkeypatch.setattr(
            connector_credential_leases,
            "deliver_connector_leases_with",
            AsyncMock(side_effect=error),
        )
        values["mint_worker_runtime_actor"] = AsyncMock(
            return_value=SimpleNamespace(to_payload=lambda: {})
        )
        message = "connector 'gitea' failed to bind"
    else:  # pragma: no cover - parametrization guard
        raise AssertionError(case)
    return _delivery(**values), message


#: Refusals the resume path writes with completion commands off.
RESUME_REFUSALS = [
    "connector_unavailable",
    "lite_shell_connector",
    "lite_config",
    "grant_denied",
    "model_unavailable",
]
#: Definitive refusals the resume path routes only with completion commands
#: on; with them off it returns without a write, as before.
ROUTED_ONLY_RESUME_REFUSALS = [
    "grant_denied/frozen",
    "workspace_contract",
    "connector_bind_refused",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", RESUME_REFUSALS + ROUTED_ONLY_RESUME_REFUSALS)
async def test_a_refused_resume_fails_the_job_through_the_completion_ledger(
    case, monkeypatch
):
    dependencies, message = _resume_refusal(
        case, commands_on=True, monkeypatch=monkeypatch
    )

    assert not await resume_job_on_agent(
        {"id": JOB_ID, "execution_lane": "pinned", "status": "paused"},
        {"id": "agent-1", "pod_ip": "10.0.0.1", "pod_port": 8001},
        dependencies=dependencies,
    )

    dependencies.refuse_job_start.assert_awaited_once()
    call = dependencies.refuse_job_start.await_args
    assert call.args == (JOB_ID,)
    assert call.kwargs["reason"] == case.split("/")[0]
    assert call.kwargs["agent_id"] == "agent-1"
    # A refused resume keeps the paused job's workspace.
    assert call.kwargs["resume"] is True
    if message is None:
        assert "lite tier" in call.kwargs["message"]
        assert "app" in call.kwargs["message"]
    else:
        assert call.kwargs["message"] == message
    dependencies.store.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", RESUME_REFUSALS)
async def test_with_completion_commands_off_a_refused_resume_is_written(
    case, monkeypatch
):
    dependencies, message = _resume_refusal(
        case, commands_on=False, monkeypatch=monkeypatch
    )

    assert not await resume_job_on_agent(
        {"id": JOB_ID, "execution_lane": "pinned", "status": "paused"},
        {"id": "agent-1", "pod_ip": "10.0.0.1", "pod_port": 8001},
        dependencies=dependencies,
    )

    dependencies.refuse_job_start.assert_not_awaited()
    dependencies.store.update_job_status.assert_awaited_once()
    written = dependencies.store.update_job_status.await_args.kwargs
    assert written["status"] == "failed"
    if message is None:
        assert "lite tier" in written["error_message"]
    else:
        assert written["error_message"] == message


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ROUTED_ONLY_RESUME_REFUSALS)
async def test_with_completion_commands_off_these_resume_refusals_stay_unwritten(
    case, monkeypatch
):
    dependencies, _message = _resume_refusal(
        case, commands_on=False, monkeypatch=monkeypatch
    )

    assert not await resume_job_on_agent(
        {"id": JOB_ID, "execution_lane": "pinned", "status": "paused"},
        {"id": "agent-1", "pod_ip": "10.0.0.1", "pod_port": 8001},
        dependencies=dependencies,
    )

    dependencies.refuse_job_start.assert_not_awaited()
    dependencies.store.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["workspace_waiting", "connector_bind_pending"])
async def test_a_resume_that_must_wait_is_not_refused(case, monkeypatch):
    dependencies, _message = _resume_refusal(
        case, commands_on=True, monkeypatch=monkeypatch
    )

    assert not await resume_job_on_agent(
        {"id": JOB_ID, "execution_lane": "pinned", "status": "paused"},
        {"id": "agent-1", "pod_ip": "10.0.0.1", "pod_port": 8001},
        dependencies=dependencies,
    )

    dependencies.refuse_job_start.assert_not_awaited()
    dependencies.store.update_job_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatcher_pause_always_releases_in_memory_pending_marker():
    pending = {"job-1"}
    dependencies = _delivery(
        pause_pending_job_ids=pending,
        prepare_pinned_job_mutation_target=AsyncMock(return_value=None),
        completion_control=SimpleNamespace(
            claim_pause=AsyncMock(return_value=SimpleNamespace(claim_id="claim")),
            abort=AsyncMock(),
        ),
    )

    await initiate_pause(
        {
            "id": "job-1",
            "runtime_kind": "srw",
            "assigned_agent_id": "agent-old",
            "pod_ip": "10.0.0.1",
        },
        dependencies=dependencies,
    )

    assert "job-1" not in pending
    dependencies.completion_control.abort.assert_not_awaited()
