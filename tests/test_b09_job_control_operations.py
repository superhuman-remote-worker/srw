"""Owner-level regression tests for extracted B09 job controls."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services.job_control_delivery import (
    JobDeliveryDependencies,
    dispatch_job_to_agent,
    initiate_pause,
)
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
    store.confirm_pinned_job_dispatch.assert_awaited_once_with("job-1", "agent-1")
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
