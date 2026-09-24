"""Exact failed-attach abort rotation and readback contract."""

from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main as main

# R1.B06: main no longer re-exports this constant. It belongs to the
# workspace-binding service and the successor path reads it from there.
from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from orchestrator.services.workspace_lifecycle import EnsureOutcome, EnsureResult
from orchestrator.application import sessions as sessions_composition
from orchestrator.security import access as access_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import (
    session_attach_binding as session_attach_binding_module,
)
from orchestrator.services import (
    session_attach_recovery as session_attach_recovery_module,
)
from orchestrator.services import session_provisioner as session_provisioner_module
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from orchestrator.services import workspace_suspension as workspace_suspension_module

THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
AGENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
ATTACH_TOKEN = "22222222-2222-4222-8222-222222222222"
WORKSPACE_GENERATION = "33333333-3333-4333-8333-333333333333"
WORKSPACE_RUNTIME = "44444444-4444-4444-8444-444444444444"
SUCCESSOR_GENERATION = "55555555-5555-4555-8555-555555555555"
SUCCESSOR_WORKSPACE_RUNTIME = "66666666-6666-4666-8666-666666666666"
POD_UID = "pod-uid-a"


def _thread(*, workspace: bool = False, **updates):
    metadata = {"config_override": {"workspace": {"backend": "sandbox"}}}
    if workspace:
        metadata.update(
            {
                "workspace_container": {
                    container_provisioner_module.WORKSPACE_RUNTIME_INCARNATION_KEY: WORKSPACE_RUNTIME,
                },
                "_workspace_binding": {"generation": WORKSPACE_GENERATION},
            }
        )
    row = {
        "id": THREAD_ID,
        "agent_id": AGENT_ID,
        "status": "created",
        "metadata": metadata,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
        "runtime_authority_exposed": True,
    }
    row.update(updates)
    return row


def _agent(**updates):
    row = {
        "id": AGENT_ID,
        "thread_id": THREAD_ID,
        "current_job_id": None,
        "status": "session",
        "pod_uid": POD_UID,
    }
    row.update(updates)
    return row


def _successor_thread(
    *,
    workspace_runtime: str | None = WORKSPACE_RUNTIME,
    workspace_status: str = "ready",
    provisioner: str = "k8s",
    runtime_generation: str = SUCCESSOR_GENERATION,
):
    workspace = {
        "status": workspace_status,
        "provisioner": provisioner,
        "pod_ip": "10.42.0.25",
        "port": 30022,
        CANVAS_WORKSPACE_GENERATION_KEY: WORKSPACE_GENERATION,
        container_provisioner_module.WORKSPACE_RUNTIME_INCARNATION_KEY: workspace_runtime,
    }
    if workspace_runtime is None:
        workspace["pod_ip"] = None
        workspace["port"] = None
        workspace[CANVAS_WORKSPACE_GENERATION_KEY] = None
    return {
        "id": THREAD_ID,
        "user_id": "user-a",
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": None,
        "runtime_generation": runtime_generation,
        "runtime_attach_token": None,
        "runtime_retirement_token": None,
        "config_name": "session_base",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "datasource_ids": ["datasource-a"],
            "workspace_container": workspace,
            "_workspace_binding": {
                "generation": WORKSPACE_GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pvc:workspace-pvc-a",
                "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
            },
        },
    }


def _workspace_zero_candidate():
    return {
        "thread_id": THREAD_ID,
        "retired_runtime_generation": RUNTIME_GENERATION,
        "retired_attach_token": ATTACH_TOKEN,
        "retired_agent_id": AGENT_ID,
        "successor_generation": SUCCESSOR_GENERATION,
        "quiescence_protocol": "workspace_process_zero_v1",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
    }


async def _release(
    thread,
    agent,
    *,
    prior=None,
    fetchvals=(False, False),
    execute_results=("UPDATE 1", "UPDATE 1", "INSERT 0 1"),
    **kwargs,
):
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(side_effect=[thread, agent, prior])
    conn.fetchval = AsyncMock(side_effect=list(fetchvals))
    conn.execute = AsyncMock(side_effect=list(execute_results))
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = False
    conn.transaction = MagicMock(return_value=transaction)

    @asynccontextmanager
    async def acquire():
        yield conn

    with patch.object(
        main.app.state.resources.postgres_db, "acquire", side_effect=acquire
    ):
        outcome = await session_attach_binding_module.release_session_attach_binding(
            AGENT_ID,
            THREAD_ID,
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_attach_token=ATTACH_TOKEN,
            **kwargs,
            dependencies=sessions_composition.session_attach_binding_dependencies(
                main.app.state.resources
            ),
        )
    return outcome, conn, transaction


@asynccontextmanager
async def _owned_lifecycle_lock(*_args, **_kwargs):
    yield True


def _assert_bound_call(mock, *args, awaited: bool = True, **kwargs) -> None:
    """``mock`` (a patched owner operation) ran once with exactly these
    arguments, plus the ``dependencies`` the application's composition built
    for it from the application's store."""

    if awaited:
        mock.assert_awaited_once()
        call = mock.await_args
    else:
        mock.assert_called_once()
        call = mock.call_args
    assert call.args == args
    passed = {key: value for key, value in call.kwargs.items() if key != "dependencies"}
    assert passed == kwargs
    dependencies = call.kwargs["dependencies"]
    assert dependencies.store is main.app.state.resources.postgres_db


async def _release_agent_via_owner(request, thread_id):
    """Drive release-agent through its R1.B06 owner.

    The handler moved to ``routers/agent_thread_status`` and its policy to
    ``services/agent_thread_status``; the transport half — the internal-key
    guard and the raw JSON read whose *unparseable* case is the 400 — stayed in
    the router. These cases are about the outcomes, so they call the service
    and hand it the body the fake request would have yielded.

    Dependencies come from ``sessions.agent_thread_status_dependencies`` of
    the application rather than a hand-built object on purpose: the factory
    binds each owner operation when it runs, so every patch of an owner below
    still steers exactly what it steered before the extraction.
    """
    from orchestrator.services import agent_thread_status

    body = await request.json()
    return await agent_thread_status.release_thread_agent(
        thread_id,
        body,
        dependencies=sessions_composition.agent_thread_status_dependencies(
            main.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_server_pre_delivery_abort_rotates_exact_pair_atomically():
    outcome, conn, transaction = await _release(
        _thread(),
        _agent(),
        pre_delivery=True,
    )

    assert outcome == "released"
    assert conn.execute.await_count == 3
    thread_sql = " ".join(conn.execute.await_args_list[0].args[0].split())
    assert "runtime_generation=$5::uuid" in thread_sql
    assert "runtime_authority_exposed=false" in thread_sql
    assert "runtime_attach_abort_receipt=$6::jsonb" in thread_sql
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_delivered_workspace_abort_requires_exact_process_zero_tuple():
    outcome, _, _ = await _release(
        _thread(workspace=True),
        _agent(),
        expected_agent_pod_uid=POD_UID,
        local_runtime_quiesced=True,
        local_quiescence_protocol="workspace_process_zero_v1",
        workspace_generation=WORKSPACE_GENERATION,
        workspace_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    assert outcome == "released"


@pytest.mark.asyncio
async def test_pre_setup_agent_refusal_uses_distinct_monotonic_latch_protocol():
    outcome, _, _ = await _release(
        _thread(workspace=True),
        _agent(),
        expected_agent_pod_uid=POD_UID,
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        workspace_generation=WORKSPACE_GENERATION,
        workspace_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    assert outcome == "released"


@pytest.mark.asyncio
async def test_delivered_abort_without_process_zero_stays_unsafe():
    outcome, conn, _ = await _release(
        _thread(workspace=True),
        _agent(),
        expected_agent_pod_uid=POD_UID,
    )
    assert outcome == "unsafe"
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("thread_updates", "agent_updates", "pod_uid"),
    [
        ({"status": "active"}, {}, POD_UID),
        ({}, {"pod_uid": "replacement-pod"}, POD_UID),
        ({}, {}, "wrong-pod"),
    ],
)
async def test_status_or_pod_identity_mismatch_stays_unsafe(
    thread_updates, agent_updates, pod_uid
):
    outcome, conn, _ = await _release(
        _thread(**thread_updates),
        _agent(**agent_updates),
        expected_agent_pod_uid=pod_uid,
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_runtime_zero_v1",
    )
    assert outcome == "unsafe"
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fetchvals", [(True, False), (False, True)])
async def test_admitted_input_or_control_forbids_generation_rollback(fetchvals):
    outcome, conn, _ = await _release(
        _thread(),
        _agent(),
        pre_delivery=True,
        fetchvals=fetchvals,
    )
    assert outcome == "unsafe"
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_outcome_is_the_only_already_detached_proof():
    outcome, conn, _ = await _release(
        None,
        None,
        prior={"successor_generation": WORKSPACE_GENERATION},
    )
    assert outcome == "already_detached"
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_detached_rows_without_exact_outcome_stay_unsafe():
    outcome, conn, _ = await _release(
        _thread(agent_id=None, runtime_attach_token=None),
        _agent(thread_id=None, status="ready"),
        pre_delivery=True,
    )
    assert outcome == "unsafe"
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execute_results",
    [
        ("UPDATE 1", RuntimeError("agent update failed")),
        ("UPDATE 1", "UPDATE 1", RuntimeError("outcome insert failed")),
    ],
)
async def test_mutation_failure_escapes_the_single_transaction(execute_results):
    with pytest.raises(RuntimeError):
        await _release(
            _thread(),
            _agent(),
            pre_delivery=True,
            execute_results=execute_results,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["released", "already_detached", "unsafe"])
async def test_http_boundary_preserves_exact_release_outcome(outcome):
    request = MagicMock()
    request.json = AsyncMock(
        return_value={
            "agent_id": AGENT_ID,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "agent_pod_uid": POD_UID,
            "local_runtime_quiesced": True,
            "local_quiescence_protocol": "workspace_process_zero_v1",
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }
    )
    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=None),
        ) as get_thread,
        patch.object(
            session_attach_binding_module,
            "release_session_attach_binding",
            AsyncMock(return_value=outcome),
        ) as release,
        patch.object(
            session_attach_binding_module,
            "acknowledge_retiring_failed_attach",
            AsyncMock(return_value=False),
        ) as acknowledge_retirement,
        patch.object(
            session_attach_recovery_module, "schedule_attach_abort_successor"
        ) as schedule,
    ):
        response = await _release_agent_via_owner(request, THREAD_ID)

    assert response == {"status": outcome}
    # Exact append-only outcome readback remains reachable after a concurrent
    # permanent thread deletion; generic thread absence is never itself proof.
    get_thread.assert_not_awaited()
    _assert_bound_call(
        release,
        AGENT_ID,
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_attach_token=ATTACH_TOKEN,
        expected_agent_pod_uid=POD_UID,
        local_runtime_quiesced=True,
        local_quiescence_protocol="workspace_process_zero_v1",
        workspace_generation=WORKSPACE_GENERATION,
        workspace_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    if outcome == "unsafe":
        _assert_bound_call(
            acknowledge_retirement,
            AGENT_ID,
            THREAD_ID,
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_attach_token=ATTACH_TOKEN,
            expected_agent_pod_uid=POD_UID,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_RUNTIME,
        )
    else:
        acknowledge_retirement.assert_not_awaited()
    if outcome in {"released", "already_detached"}:
        _assert_bound_call(
            schedule,
            THREAD_ID,
            awaited=False,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
        )
    else:
        schedule.assert_not_called()


@pytest.mark.asyncio
async def test_http_boundary_routes_failed_attach_proof_into_retirement_only():
    request = MagicMock()
    request.json = AsyncMock(
        return_value={
            "agent_id": AGENT_ID,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "agent_pod_uid": POD_UID,
            "local_runtime_quiesced": True,
            "local_quiescence_protocol": "workspace_process_zero_v1",
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }
    )
    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(
            session_attach_binding_module,
            "release_session_attach_binding",
            AsyncMock(return_value="unsafe"),
        ),
        patch.object(
            session_attach_binding_module,
            "acknowledge_retiring_failed_attach",
            AsyncMock(return_value=True),
        ) as acknowledge_retirement,
        patch.object(
            session_attach_recovery_module, "schedule_attach_abort_successor"
        ) as schedule,
    ):
        response = await _release_agent_via_owner(request, THREAD_ID)

    assert response == {"status": "retirement_acknowledged"}
    acknowledge_retirement.assert_awaited_once()
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_retiring_pre_setup_latch_derives_existing_workspace_receipt():
    retirement_token = "77777777-7777-4777-8777-777777777777"
    acknowledge = AsyncMock(return_value={"version": 1})
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(
                return_value={
                    "runtime_retirement_token": retirement_token,
                    "runtime_retirement_context": {"settle_status": "ended"},
                }
            ),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "acknowledge_pinned_thread_local_quiescence",
            acknowledge,
        ),
    ):
        assert await session_attach_binding_module.acknowledge_retiring_failed_attach(
            AGENT_ID,
            THREAD_ID,
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_attach_token=ATTACH_TOKEN,
            expected_agent_pod_uid=POD_UID,
            local_quiescence_protocol="agent_attach_not_started_v1",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_RUNTIME,
            dependencies=sessions_composition.session_attach_binding_dependencies(
                main.app.state.resources
            ),
        )

    acknowledge.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=retirement_token,
        expected_agent_id=AGENT_ID,
        expected_attach_token=ATTACH_TOKEN,
        expected_settle_status="ended",
        expected_quiescence_protocol="workspace_process_zero_v1",
        expected_workspace_generation=WORKSPACE_GENERATION,
        expected_workspace_runtime_incarnation=WORKSPACE_RUNTIME,
        expected_agent_pod_uid=POD_UID,
        require_zero_admission=True,
    )


@pytest.mark.asyncio
async def test_retiring_failed_attach_lost_ack_reads_append_only_outcome():
    readback = AsyncMock(return_value=True)
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=None),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "has_exact_pinned_runtime_retirement_outcome",
            readback,
        ),
    ):
        assert await session_attach_binding_module.acknowledge_retiring_failed_attach(
            AGENT_ID,
            THREAD_ID,
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_attach_token=ATTACH_TOKEN,
            expected_agent_pod_uid=POD_UID,
            local_quiescence_protocol="agent_runtime_zero_v1",
            workspace_generation=None,
            workspace_runtime_incarnation=None,
            dependencies=sessions_composition.session_attach_binding_dependencies(
                main.app.state.resources
            ),
        )

    readback.assert_awaited_once_with(
        THREAD_ID,
        runtime_generation=RUNTIME_GENERATION,
        agent_id=AGENT_ID,
        runtime_attach_token=ATTACH_TOKEN,
    )


@pytest.mark.asyncio
async def test_exact_attach_abort_strongly_owns_successor_provisioning():
    current = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": None,
        "runtime_generation": SUCCESSOR_GENERATION,
        "runtime_retirement_token": None,
        "config_name": "session_base",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "datasource_ids": ["datasource-a"],
        },
    }
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(
        return_value={
            "thread_id": THREAD_ID,
            "retired_runtime_generation": RUNTIME_GENERATION,
            "retired_attach_token": ATTACH_TOKEN,
            "retired_agent_id": AGENT_ID,
            "successor_generation": SUCCESSOR_GENERATION,
            "quiescence_protocol": "agent_attach_not_started_v1",
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }
    )

    @asynccontextmanager
    async def acquire():
        yield conn

    provision = AsyncMock()
    main.app.state.resources.attach_abort_successor_tasks.clear()
    with (
        patch.object(
            main.app.state.resources.postgres_db, "acquire", side_effect=acquire
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(side_effect=[current, current]),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            thread_mount_rows_module,
            "thread_project_ids",
            AsyncMock(return_value=["project-a"]),
        ),
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        task = session_attach_recovery_module.schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=sessions_composition.session_attach_recovery_dependencies(
                main.app.state.resources
            ),
        )
        duplicate = session_attach_recovery_module.schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=sessions_composition.session_attach_recovery_dependencies(
                main.app.state.resources
            ),
        )
        assert duplicate is task
        await task

    # The binder now also receives its constructed port. Assert the subject of
    # the call — who is provisioned, for which exact successor generation —
    # rather than the composition root's dependency object.
    provision.assert_awaited_once()
    args, kwargs = provision.await_args
    assert args == (
        "user-a",
        THREAD_ID,
        "session_base",
        {"workspace": {"backend": "sandbox"}},
        ["project-a"],
        ["datasource-a"],
    )
    assert kwargs["runtime_generation"] == SUCCESSOR_GENERATION


@pytest.mark.asyncio
@pytest.mark.parametrize("old_pod_authority", ["exact_live", "exact_absent"])
async def test_workspace_zero_abort_recreates_exact_pod_and_health_checks_ide(
    old_pod_authority,
):
    old = _successor_thread()
    deleted = _successor_thread(
        workspace_runtime=None,
        workspace_status="deleted",
    )
    replacement = _successor_thread(workspace_runtime=SUCCESSOR_WORKSPACE_RUNTIME)
    get_thread = AsyncMock(
        side_effect=[old, deleted, replacement, replacement, replacement]
    )
    pod_authority = AsyncMock(return_value=old_pod_authority)
    delete_workspace = AsyncMock(return_value=True)
    clear_endpoint = AsyncMock(return_value=True)
    ensure_workspace = AsyncMock(
        return_value=EnsureResult(EnsureOutcome.PENDING, status="creating")
    )
    code_server = AsyncMock(return_value=True)
    provision = AsyncMock()
    events = MagicMock()
    events.attach_mock(delete_workspace, "delete")
    events.attach_mock(clear_endpoint, "clear")
    events.attach_mock(ensure_workspace, "ensure")
    events.attach_mock(code_server, "health")
    events.attach_mock(provision, "provision")

    with (
        patch.object(main.app.state.resources.postgres_db, "get_thread", get_thread),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "clear_pinned_attach_abort_workspace_endpoint",
            clear_endpoint,
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "workspace_pod_authority",
            pod_authority,
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "delete_workspace",
            delete_workspace,
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "wait_for_workspace_code_server",
            code_server,
        ),
        patch.object(
            session_provisioner_module, "ensure_session_workspace", ensure_workspace
        ),
        patch.object(
            thread_mount_rows_module, "thread_project_ids", AsyncMock(return_value=[])
        ),
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        assert (
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                _workspace_zero_candidate(),
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
            is True
        )

    pod_authority.assert_awaited_once_with(
        WorkspaceOwner.session(THREAD_ID),
        expected_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    if old_pod_authority == "exact_live":
        delete_workspace.assert_awaited_once_with(
            WorkspaceOwner.session(THREAD_ID),
            expected_runtime_incarnation=WORKSPACE_RUNTIME,
            captured_teardown_uid=WORKSPACE_RUNTIME,
            wait_for_exact_absence=True,
            defer_context_clear=True,
        )
    else:
        delete_workspace.assert_not_awaited()
    clear_endpoint.assert_awaited_once_with(
        THREAD_ID,
        retired_runtime_generation=RUNTIME_GENERATION,
        retired_attach_token=ATTACH_TOKEN,
        retired_agent_id=AGENT_ID,
        successor_generation=SUCCESSOR_GENERATION,
        workspace_generation=WORKSPACE_GENERATION,
        workspace_runtime_incarnation=WORKSPACE_RUNTIME,
    )
    ensure_workspace.assert_awaited_once_with(
        THREAD_ID,
        db=main.app.state.resources.postgres_db,
        provisioner=container_provisioner_module.container_provisioner,
        suspension=workspace_suspension_module.workspace_suspension_service,
        expected_runtime_generation=SUCCESSOR_GENERATION,
        _pinned_runtime_lock_held=True,
    )
    code_server.assert_awaited_once_with(
        WorkspaceOwner.session(THREAD_ID),
        expected_runtime_incarnation=SUCCESSOR_WORKSPACE_RUNTIME,
    )
    provision.assert_awaited_once()
    effect_names = [call[0] for call in events.mock_calls]
    required = ["clear", "ensure", "health", "provision"]
    assert [name for name in effect_names if name in required] == required
    if old_pod_authority == "exact_live":
        assert effect_names.index("delete") < effect_names.index("clear")


@pytest.mark.asyncio
async def test_workspace_zero_health_failure_keeps_successor_unbound_and_retryable():
    replacement = _successor_thread(workspace_runtime=SUCCESSOR_WORKSPACE_RUNTIME)
    provision = AsyncMock()
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=replacement),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "wait_for_workspace_code_server",
            AsyncMock(return_value=False),
        ) as health,
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        assert (
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                _workspace_zero_candidate(),
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
            is False
        )

    health.assert_awaited_once()
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_zero_abort_fails_closed_for_static_docker_workspace():
    docker = _successor_thread(provisioner="docker")
    provision = AsyncMock()
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=docker),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "delete_workspace",
            AsyncMock(),
        ) as delete_workspace,
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        assert (
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                _workspace_zero_candidate(),
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
            is False
        )

    delete_workspace.assert_not_awaited()
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_zero_delete_cas_loss_never_recreates_or_touches_u2():
    old = _successor_thread()
    delete_workspace = AsyncMock(return_value=True)
    ensure_workspace = AsyncMock()
    provision = AsyncMock()
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=old),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "clear_pinned_attach_abort_workspace_endpoint",
            AsyncMock(return_value=False),
        ) as clear_endpoint,
        patch.object(
            container_provisioner_module.container_provisioner,
            "workspace_pod_authority",
            AsyncMock(return_value="exact_live"),
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "delete_workspace",
            delete_workspace,
        ),
        patch.object(
            session_provisioner_module, "ensure_session_workspace", ensure_workspace
        ),
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        assert (
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                _workspace_zero_candidate(),
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
            is False
        )

    delete_workspace.assert_awaited_once()
    assert (
        delete_workspace.await_args.kwargs["expected_runtime_incarnation"]
        == WORKSPACE_RUNTIME
    )
    clear_endpoint.assert_awaited_once()
    ensure_workspace.assert_not_awaited()
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_attach_abort_successor_owner_never_adopts_a_later_generation():
    current = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": None,
        "runtime_generation": WORKSPACE_GENERATION,
        "runtime_retirement_token": None,
        "metadata": {},
    }
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(
        return_value={
            "thread_id": THREAD_ID,
            "retired_runtime_generation": RUNTIME_GENERATION,
            "retired_attach_token": ATTACH_TOKEN,
            "retired_agent_id": AGENT_ID,
            "successor_generation": SUCCESSOR_GENERATION,
            "quiescence_protocol": "agent_attach_not_started_v1",
        }
    )

    @asynccontextmanager
    async def acquire():
        yield conn

    provision = AsyncMock()
    main.app.state.resources.attach_abort_successor_tasks.clear()
    with (
        patch.object(
            main.app.state.resources.postgres_db, "acquire", side_effect=acquire
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=current),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        await session_attach_recovery_module.schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=sessions_composition.session_attach_recovery_dependencies(
                main.app.state.resources
            ),
        )

    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_successor_candidate_retries_after_transient_failure():
    current = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": None,
        "runtime_generation": SUCCESSOR_GENERATION,
        "runtime_retirement_token": None,
        "config_name": "session_base",
        "metadata": {"config_override": {}, "datasource_ids": []},
    }
    candidate = {
        "thread_id": THREAD_ID,
        "retired_runtime_generation": RUNTIME_GENERATION,
        "retired_attach_token": ATTACH_TOKEN,
        "retired_agent_id": AGENT_ID,
        "successor_generation": SUCCESSOR_GENERATION,
        "quiescence_protocol": "agent_attach_not_started_v1",
    }
    provision = AsyncMock(side_effect=[RuntimeError("transient"), None])
    with (
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=current),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "try_thread_advisory_lock",
            side_effect=_owned_lifecycle_lock,
        ),
        patch.object(
            thread_mount_rows_module, "thread_project_ids", AsyncMock(return_value=[])
        ),
        patch(
            "orchestrator.services.provision_or_assign.provision_or_assign", provision
        ),
    ):
        with pytest.raises(RuntimeError, match="transient"):
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                candidate,
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
        assert (
            await session_attach_recovery_module.reconcile_attach_abort_successor(
                candidate,
                dependencies=sessions_composition.session_attach_recovery_dependencies(
                    main.app.state.resources
                ),
            )
            is True
        )

    assert provision.await_count == 2
    assert all(
        call.kwargs["runtime_generation"] == SUCCESSOR_GENERATION
        for call in provision.await_args_list
    )


@pytest.mark.asyncio
async def test_http_boundary_refuses_a_claim_without_process_zero():
    request = MagicMock()
    request.json = AsyncMock(
        return_value={
            "agent_id": AGENT_ID,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": ATTACH_TOKEN,
            "agent_pod_uid": POD_UID,
        }
    )
    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value={"id": THREAD_ID}),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await _release_agent_via_owner(request, THREAD_ID)
    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "pinned_attach_quiescence_required"
