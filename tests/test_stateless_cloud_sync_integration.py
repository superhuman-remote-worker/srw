"""Integration seams for the stateless cloud generation fence.

The transport/store algorithms have their own focused tests. These contracts
pin the lifecycle ordering around them so a later refactor cannot pull before
recovering, complete while a PUT is live, or reintroduce an unfenced teardown
push.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import agent.api.persistent_app as papp
from agent.api.lease_context import LeaseHandle, current_lease
from agent.api.turn_executor import StatelessTurnExecutor
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
from agent.services.cloud_sync.coordinator import (
    CloudSyncGenerationError,
    MountSync,
    WorkspaceSyncCoordinator,
)
from shared.cloud_sync_generations import (
    EMPTY_BASELINE_SHA256,
    CloudSyncRequirement,
)


THREAD_ID = "11111111-1111-4111-8111-111111111111"
RUNTIME_GENERATION = "22222222-2222-4222-8222-222222222222"
AGENT_ID = "33333333-3333-4333-8333-333333333333"
RUNTIME_ATTACH_TOKEN = "44444444-4444-4444-8444-444444444444"
WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKSPACE_RUNTIME_INCARNATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
WORKSPACE_SSH_HOST_KEY_FINGERPRINT = "SHA256:trusted"
LEASE_TOKEN = 47
SCOPE_SHA256 = "a" * 64
MOUNT_ID = "mount:logical-destination"


def _requirement(generation: int = LEASE_TOKEN) -> CloudSyncRequirement:
    return CloudSyncRequirement(
        mount_id=MOUNT_ID,
        required_generation=generation,
        acknowledged_generation=0,
        required_lease_token=generation,
        workspace_generation=WORKSPACE_GENERATION,
        sync_scope_sha256=SCOPE_SHA256,
    )


def _install_lease(token: int = LEASE_TOKEN):
    handle = LeaseHandle()
    handle.update(THREAD_ID, token)
    reset_token = current_lease.set(handle)
    return handle, reset_token


@pytest.fixture(autouse=True)
def _reset_pending_cloud_task():
    papp._pending_cloud_push_task = None
    yield
    task = papp._pending_cloud_push_task
    if task is not None and not task.done():
        task.cancel()
    papp._pending_cloud_push_task = None


@pytest.fixture(autouse=True)
def _restore_loop_primitives():
    """`_attach_session` installs loop primitives bound to *this* test's event
    loop. Leaking them across tests hands a later test an `asyncio.Event` bound
    to a dead loop, whose `wait()` then raises mid-await in whichever module
    global consumer runs next (e.g. `_wait_for_permission_resolution`)."""

    names = (
        "_loop_user_queue",
        "_loop_interrupt_flag",
        "_loop_last_user_content",
        "_hard_interrupt_event",
    )
    saved = {name: getattr(papp, name) for name in names}
    yield
    for name, value in saved.items():
        setattr(papp, name, value)


@pytest.mark.asyncio
async def test_stateless_start_recovers_then_strict_pulls_then_arms(monkeypatch):
    order: list[str] = []

    async def pull(
        *, strict: bool = False, before_write=None, force_unknown: bool = False
    ):
        assert strict is True
        assert before_write is not None
        assert force_unknown is True
        await before_write()
        order.append("pull")
        return []

    async def capture_generation_baseline():
        order.append("baseline")
        return {}, EMPTY_BASELINE_SHA256

    mount_sync = SimpleNamespace(
        pull=pull,
        capture_generation_baseline=capture_generation_baseline,
    )
    coordinator = WorkspaceSyncCoordinator(
        [
            MountSync(
                mount_id=MOUNT_ID,
                target_path="",
                sync=mount_sync,
                sync_scope_sha256=SCOPE_SHA256,
            )
        ],
        thread_id=THREAD_ID,
        workspace_generation=WORKSPACE_GENERATION,
    )

    async def recover(*_args, **_kwargs):
        order.append("recover")
        return {MOUNT_ID: []}

    coordinator.reconcile_before_pull = AsyncMock(side_effect=recover)
    postgres = object()
    session = SimpleNamespace(postgres_conn=postgres, cloud_sync_requirements={})
    requirement = _requirement()

    async def arm(*_args, **_kwargs):
        order.append("arm")
        return {MOUNT_ID: requirement}

    handle, reset_token = _install_lease()
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    try:
        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_thread_id", THREAD_ID),
            patch(
                "shared.cloud_sync_generations.cloud_sync_lease_is_current",
                AsyncMock(return_value=True),
            ),
            patch(
                "shared.cloud_sync_generations.adopt_push_ownership",
                AsyncMock(return_value={}),
            ),
            patch(
                "shared.cloud_sync_generations.load_cloud_sync_requirements",
                AsyncMock(return_value={}),
            ),
            patch(
                "shared.cloud_sync_generations.arm_cloud_sync_generations",
                AsyncMock(side_effect=arm),
            ),
            patch.object(papp, "_broadcast"),
        ):
            await papp._prepare_stateless_cloud_sync(coordinator, turn_id=3)
    finally:
        current_lease.reset(reset_token)

    assert handle.active
    assert order == ["recover", "pull", "baseline", "arm"]
    assert session.cloud_sync_requirements == {MOUNT_ID: requirement}


@pytest.mark.asyncio
async def test_stateless_pull_failure_blocks_arm_and_turn_start(monkeypatch):
    async def failed_pull(
        *, strict: bool = False, before_write=None, force_unknown: bool = False
    ):
        assert strict is True
        assert before_write is not None
        assert force_unknown is True
        raise RuntimeError("remote listing failed")

    mount_sync = SimpleNamespace(pull=failed_pull)
    coordinator = WorkspaceSyncCoordinator(
        [
            MountSync(
                mount_id=MOUNT_ID,
                target_path="",
                sync=mount_sync,
                sync_scope_sha256=SCOPE_SHA256,
            )
        ],
        thread_id=THREAD_ID,
        workspace_generation=WORKSPACE_GENERATION,
    )
    coordinator.reconcile_before_pull = AsyncMock(return_value={MOUNT_ID: []})
    session = SimpleNamespace(postgres_conn=object(), cloud_sync_requirements={})
    arm = AsyncMock()
    _handle, reset_token = _install_lease()
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    try:
        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_thread_id", THREAD_ID),
            patch(
                "shared.cloud_sync_generations.cloud_sync_lease_is_current",
                AsyncMock(return_value=True),
            ),
            patch(
                "shared.cloud_sync_generations.adopt_push_ownership",
                AsyncMock(return_value={}),
            ),
            patch(
                "shared.cloud_sync_generations.load_cloud_sync_requirements",
                AsyncMock(return_value={}),
            ),
            patch("shared.cloud_sync_generations.arm_cloud_sync_generations", arm),
            patch.object(papp.asyncio, "sleep", AsyncMock()),
            patch.object(papp, "_broadcast"),
        ):
            with pytest.raises(
                CloudSyncGenerationError, match="pull failed|pull refused"
            ):
                await papp._prepare_stateless_cloud_sync(coordinator, turn_id=4)
    finally:
        current_lease.reset(reset_token)

    arm.assert_not_awaited()
    assert session.cloud_sync_requirements == {}


@pytest.mark.asyncio
async def test_stateless_turn_refuses_unrecovered_cloud_setup(monkeypatch):
    """A failed late-start retry must not silently run a cloud-backed turn."""
    session = SimpleNamespace(workspace_sync=None, turn_count=0)
    retry = AsyncMock()
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")

    with (
        patch.object(papp, "_session", session),
        patch.object(papp, "_thread_id", THREAD_ID),
        patch.object(papp, "_cloud_sync_retry_pending", True),
        patch.object(papp, "_retry_cloud_sync_start", retry),
        patch.object(papp, "_broadcast"),
    ):
        with pytest.raises(CloudSyncGenerationError, match="remains degraded"):
            await papp._loop_on_turn_start(5)

    retry.assert_awaited_once_with(5)


@pytest.mark.asyncio
async def test_turn_start_failure_ends_loop_before_persist_or_completion_callback():
    """Recovery/pull/arm failure must leave the queue input unanswered.

    ``run_persistent_loop`` owns the exact ordering at issue: the stateless
    executor injects an already-persisted row, the turn-start fence raises, and
    the loop task dies without firing the completion hook that advances the
    queue's consumed watermark. The executor's loop-died -> release behavior is
    covered by ``tests/test_turn_executor.py::TestTurnError``.
    """

    start = AsyncMock(side_effect=CloudSyncGenerationError("recovery failed"))
    complete = AsyncMock()
    persist = AsyncMock()
    llm = MagicMock()
    callbacks = PersistentLoopCallbacks(
        get_user_input=AsyncMock(
            return_value={"content": "queued input", "id": "persisted-row-id"}
        ),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=start,
        on_turn_complete=complete,
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        persist_message=persist,
    )

    with pytest.raises(CloudSyncGenerationError, match="recovery failed"):
        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=MagicMock(),
            config=SimpleNamespace(
                llm=SimpleNamespace(timeout=1),
                memory=None,
            ),
            system_prompt="system",
            callbacks=callbacks,
            messages=[],
        )

    start.assert_awaited_once_with(1)
    persist.assert_not_awaited()
    complete.assert_not_awaited()
    llm.astream.assert_not_called()


@pytest.mark.asyncio
async def test_turn_end_task_captures_token_and_requirement_snapshot(monkeypatch):
    release = asyncio.Event()
    observed: list[tuple[dict, object]] = []
    original = _requirement()
    sync = SimpleNamespace(workspace_generation=WORKSPACE_GENERATION)
    session = SimpleNamespace(
        postgres_conn=object(),
        messages=[],
        tool_decisions={},
        auxiliary_llm=None,
        workspace_sync=sync,
        cloud_sync_requirements={MOUNT_ID: original},
        overlay_mount_manager=None,
    )

    async def record_task(
        _sync, _turn_id, *, requirements=None, claim=None, staged_event=None
    ) -> None:
        del staged_event
        await release.wait()
        observed.append((requirements, claim))

    handle, reset_token = _install_lease()
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    try:
        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_thread_id", THREAD_ID),
            patch.object(papp, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(papp, "_wire_session_aux_archiver"),
            patch.object(papp, "_save_turn_ai_messages", AsyncMock()),
            patch.object(papp, "_auto_title_after_first_turn", AsyncMock()),
            patch.object(papp, "_should_notify_cloud_stage", return_value=False),
            patch.object(papp, "_broadcast"),
            patch.object(papp, "_run_turn_end_cloud_push", record_task),
        ):
            await papp._loop_on_turn_complete_body(8)
            task = papp._pending_cloud_push_task
            assert task is not None and not task.done()

            replacement = _requirement(generation=99)
            session.cloud_sync_requirements[MOUNT_ID] = replacement
            handle.update(THREAD_ID, 88)
            release.set()
            await task
    finally:
        current_lease.reset(reset_token)

    captured_requirements, captured_claim = observed[0]
    assert captured_requirements[MOUNT_ID] is original
    assert captured_claim.lease_token == LEASE_TOKEN
    assert captured_claim.workspace_generation == WORKSPACE_GENERATION
    # The shared handle is intentionally mutable for affinity, but the task's
    # authorization query uses the scalar token captured above.
    assert captured_claim.lease_handle.lease_token == 88


@pytest.mark.asyncio
async def test_executor_never_completes_while_cloud_task_is_live():
    release = asyncio.Event()

    async def push():
        await release.wait()

    cloud_task = asyncio.create_task(push())
    pa = SimpleNamespace(_pending_cloud_push_task=cloud_task)
    executor = StatelessTurnExecutor(cloud_push_wait_seconds=0.001)

    waiter = asyncio.create_task(executor._await_cloud_push(pa))
    await asyncio.sleep(0.02)
    assert not waiter.done()
    assert not cloud_task.done()

    release.set()
    await waiter
    assert cloud_task.done()


@pytest.mark.parametrize("stateless", [True, False], ids=["stateless", "pinned"])
@pytest.mark.asyncio
async def test_teardown_skips_raw_sync_only_for_stateless(stateless: bool):
    sync = SimpleNamespace(
        push_all=AsyncMock(),
        pull_all=AsyncMock(),
        aclose=AsyncMock(),
    )
    session = SimpleNamespace(
        memory_service=None,
        final_memory_extracted=False,
        messages=[],
        shell_owner_token=None,
        workspace_sync=sync,
        workspace_manager=None,
        retire_shell_owner=MagicMock(),
        quiesce_background_tasks=AsyncMock(),
        cleanup=AsyncMock(),
    )

    with (
        patch.object(papp, "_session", session),
        patch.object(papp, "_thread_id", THREAD_ID),
        patch.object(papp, "_loop_task", None),
        patch.object(papp, "_event_writer", None),
        patch.object(papp, "_control_owner_agent_id", None),
        patch.object(papp, "_registered_pinned_agent_id", return_value=None),
        patch.object(papp, "_stateless_mode", return_value=stateless),
        patch.object(papp, "_stop_watchdogs"),
        patch.object(papp, "_stop_thread_control_watcher", AsyncMock()),
        patch.object(papp, "_await_pending_cloud_push", AsyncMock()),
        patch.object(papp, "_clear_all_canvas_awareness"),
        patch.object(papp, "_subscribers", {}),
        patch.object(papp, "_sessions_served", 0),
        patch.object(papp, "_max_sessions_per_process", 0),
    ):
        await papp._terminate_session_inner("test", mark_thread=False)

    if stateless:
        sync.push_all.assert_not_awaited()
        sync.pull_all.assert_not_awaited()
    else:
        sync.push_all.assert_awaited_once_with()
        sync.pull_all.assert_awaited_once_with()
    sync.aclose.assert_awaited_once_with()


@pytest.mark.parametrize(
    ("workspace_response", "expected_backend"),
    [
        (
            {"status": "ready", "pod_ip": "10.0.0.8"},
            "sandbox",
        ),
        (
            {"vm_status": "ready", "vm_ssh_host": "10.0.0.9"},
            "vm",
        ),
    ],
)
@pytest.mark.asyncio
async def test_workspace_poll_preserves_generation(
    workspace_response: dict, expected_backend: str
):
    workspace_response["workspace_generation"] = WORKSPACE_GENERATION
    workspace_response["workspace_runtime_incarnation"] = WORKSPACE_RUNTIME_INCARNATION
    workspace_response["workspace_ssh_host_key_fingerprint"] = (
        WORKSPACE_SSH_HOST_KEY_FINGERPRINT
    )
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace_response)
    )

    result = await papp._poll_workspace_ready(client, THREAD_ID, timeout=1)

    assert result is not None
    assert result["backend"] == expected_backend
    assert result["workspace_generation"] == WORKSPACE_GENERATION
    assert result["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME_INCARNATION
    assert result["workspace_ssh_host_key_fingerprint"] == (
        WORKSPACE_SSH_HOST_KEY_FINGERPRINT if expected_backend == "sandbox" else None
    )


@pytest.mark.asyncio
async def test_internal_workspace_payload_exposes_private_binding_generation():
    import orchestrator.main as orch_main
    from orchestrator.services import thread_workspace_delivery

    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "status": "active",
        "execution_lane": "pinned",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_retirement_token": None,
        "agent_id": AGENT_ID,
        "runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        "metadata": {
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.0.0.8",
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
                "_runtime_incarnation": WORKSPACE_RUNTIME_INCARNATION,
            },
            "_workspace_binding": {
                "generation": WORKSPACE_GENERATION,
                "kind": "remote",
            },
        },
    }
    with (
        patch.object(
            orch_main.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            orch_main.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            orch_main.postgres_db,
            "pinned_thread_agent_is_reciprocal",
            AsyncMock(return_value=True),
        ),
        patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
        patch.object(
            orch_main,
            "_revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            orch_main, "_resolve_thread_datasources", AsyncMock(return_value=None)
        ),
        patch.object(
            orch_main, "_resolve_thread_repositories", AsyncMock(return_value=None)
        ),
        patch.object(
            thread_workspace_delivery,
            "agent_canvas_workspace_capabilities",
            return_value=(False, False, False),
        ),
        patch.object(
            orch_main, "_build_agent_cloud_mount", AsyncMock(return_value=None)
        ),
        patch.object(orch_main, "_build_agent_cloud_sync", return_value=None),
        patch.object(orch_main, "_cloud_workspace_driver", return_value="sync"),
        patch.object(
            orch_main,
            "main_cloud_router",
            SimpleNamespace(active=SimpleNamespace(is_initialized=False)),
        ),
        patch.object(
            orch_main, "_resolve_session_config", AsyncMock(return_value=None)
        ),
        patch.object(
            orch_main,
            "_inject_thread_dispatch_credentials",
            AsyncMock(side_effect=lambda value, **_kwargs: value),
        ),
        patch.object(
            orch_main,
            "_inject_lite_workspace_config",
            side_effect=lambda value, **_kwargs: value,
        ),
    ):
        response = (
            await orch_main.thread_workspace_delivery.agent_get_thread_workspace_locked(
                THREAD_ID,
                presented_agent_id=AGENT_ID,
                presented_runtime_generation=RUNTIME_GENERATION,
                presented_attach_token=RUNTIME_ATTACH_TOKEN,
                dependencies=orch_main._thread_workspace_delivery_dependencies(),
            )
        )

    assert response["workspace_generation"] == WORKSPACE_GENERATION
    assert response["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME_INCARNATION
    assert response["workspace_ssh_host_key_fingerprint"] is None


def test_owner_payload_redacts_private_workspace_runtime_incarnation():
    from orchestrator.services.thread_projection import redact_thread_metadata

    record = {
        "metadata": {
            "workspace_container": {
                "status": "ready",
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
                "_runtime_incarnation": WORKSPACE_RUNTIME_INCARNATION,
            }
        }
    }

    redacted = redact_thread_metadata(record)

    workspace = redacted["metadata"]["workspace_container"]
    assert "_canvas_workspace_generation" not in workspace
    assert "_runtime_incarnation" not in workspace


async def _internal_workspace_response_for_lite_thread(
    thread: dict,
    *,
    cloud_mount=None,
    cloud_sync=None,
    thread_reads=None,
):
    import orchestrator.main as orch_main
    from orchestrator.services import thread_workspace_delivery

    def _with_live_runtime(row):
        normalized = dict(row)
        normalized.setdefault("status", "active")
        normalized.setdefault("runtime_generation", RUNTIME_GENERATION)
        normalized.setdefault("runtime_retirement_token", None)
        if normalized.get("execution_lane") == "pinned":
            normalized.setdefault("agent_id", AGENT_ID)
            normalized.setdefault("runtime_attach_token", RUNTIME_ATTACH_TOKEN)
        return normalized

    thread = _with_live_runtime(thread)
    get_thread = AsyncMock(return_value=thread)
    if thread_reads is not None:
        normalized_reads = [_with_live_runtime(row) for row in thread_reads]
        read_index = 0

        async def _read_thread(*_args, **_kwargs):
            nonlocal read_index
            row = normalized_reads[min(read_index, len(normalized_reads) - 1)]
            read_index += 1
            return row

        get_thread.side_effect = _read_thread
    pinned = thread.get("execution_lane") == "pinned"
    with (
        patch.object(
            orch_main.postgres_db,
            "get_thread",
            get_thread,
        ),
        patch.object(
            orch_main.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            orch_main.postgres_db,
            "pinned_thread_agent_is_reciprocal",
            AsyncMock(return_value=True),
        ),
        patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
        patch.object(
            orch_main,
            "_revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            orch_main, "_resolve_thread_datasources", AsyncMock(return_value=None)
        ),
        patch.object(
            orch_main, "_resolve_thread_repositories", AsyncMock(return_value=None)
        ),
        patch.object(
            thread_workspace_delivery,
            "agent_canvas_workspace_capabilities",
            return_value=(False, False, False),
        ),
        patch.object(
            orch_main,
            "_build_agent_cloud_mount",
            AsyncMock(return_value=cloud_mount),
        ),
        patch.object(orch_main, "_build_agent_cloud_sync", return_value=cloud_sync),
        patch.object(orch_main, "_cloud_workspace_driver", return_value="sync"),
        patch.object(
            orch_main,
            "main_cloud_router",
            SimpleNamespace(active=SimpleNamespace(is_initialized=False)),
        ),
        patch.object(
            orch_main, "_resolve_session_config", AsyncMock(return_value=None)
        ),
        patch.object(
            orch_main,
            "_inject_thread_dispatch_credentials",
            AsyncMock(side_effect=lambda value, **_kwargs: value),
        ),
        patch.object(
            orch_main,
            "_inject_lite_workspace_config",
            side_effect=lambda value, **_kwargs: value,
        ),
    ):
        return (
            await orch_main.thread_workspace_delivery.agent_get_thread_workspace_locked(
                THREAD_ID,
                presented_agent_id=AGENT_ID if pinned else None,
                presented_runtime_generation=RUNTIME_GENERATION if pinned else None,
                presented_attach_token=RUNTIME_ATTACH_TOKEN if pinned else None,
                dependencies=orch_main._thread_workspace_delivery_dependencies(),
            )
        )


def _stateless_sandbox_thread() -> dict:
    return {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "status": "active",
        "execution_lane": "stateless",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.0.25",
                "port": 30022,
                "pod_name": "ws-thread-111111111111",
                "namespace": "agent-workspaces",
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
                "_runtime_incarnation": WORKSPACE_RUNTIME_INCARNATION,
            },
            "_workspace_binding": {
                "generation": WORKSPACE_GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "ssh_host_key_fingerprint": WORKSPACE_SSH_HOST_KEY_FINGERPRINT,
            },
        },
    }


@pytest.mark.asyncio
async def test_internal_workspace_ready_requires_exact_live_runtime_uid():
    import orchestrator.main as orch_main

    probe = AsyncMock(return_value=True)
    schedule = MagicMock()
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(
            _stateless_sandbox_thread()
        )

    probe.assert_awaited_once()
    assert probe.await_args.kwargs == {
        "expected_runtime_incarnation": WORKSPACE_RUNTIME_INCARNATION
    }
    schedule.assert_not_called()
    assert response["status"] == "ready"
    assert response["pod_ip"] == "10.42.0.25"
    assert response["workspace_generation"] == WORKSPACE_GENERATION
    assert response["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME_INCARNATION
    assert response["workspace_ssh_host_key_fingerprint"] == (
        WORKSPACE_SSH_HOST_KEY_FINGERPRINT
    )


@pytest.mark.asyncio
async def test_internal_workspace_restore_intent_suppresses_ready_credentials():
    """A newly created pod is not attachable until snapshot extraction acks."""
    import orchestrator.main as orch_main

    thread = _stateless_sandbox_thread()
    thread["metadata"]["workspace_container"]["_snapshot_restore_required"] = True
    schedule = MagicMock()
    probe = AsyncMock(side_effect=AssertionError("restore intent precedes UID probe"))
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(thread)

    schedule.assert_called_once_with(THREAD_ID)
    probe.assert_not_awaited()
    assert response["status"] == "restoring"
    assert response["pod_ip"] is None
    assert response["workspace_generation"] is None
    assert response["workspace_runtime_incarnation"] is None
    assert response["workspace_ssh_host_key_fingerprint"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [None, 0, "", [], {}, "false", 1])
async def test_internal_workspace_malformed_restore_marker_refuses_credentials(
    malformed,
):
    import orchestrator.main as orch_main

    thread = _stateless_sandbox_thread()
    thread["metadata"]["workspace_container"]["_snapshot_restore_required"] = malformed
    schedule = MagicMock()
    probe = AsyncMock(side_effect=AssertionError("malformed marker precedes probe"))
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        with pytest.raises(orch_main.HTTPException) as exc_info:
            await _internal_workspace_response_for_lite_thread(thread)

    assert exc_info.value.status_code == 503
    schedule.assert_not_called()
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_internal_workspace_ready_authority_requires_host_fingerprint():
    import orchestrator.main as orch_main

    thread = _stateless_sandbox_thread()
    thread["metadata"]["_workspace_binding"].pop("ssh_host_key_fingerprint")
    with patch.object(
        orch_main.container_provisioner,
        "workspace_pod_live",
        AsyncMock(return_value=True),
    ):
        response = await _internal_workspace_response_for_lite_thread(thread)

    assert response["status"] == "ready"
    assert response["workspace_generation"] is None
    assert response["workspace_runtime_incarnation"] is None
    assert response["workspace_ssh_host_key_fingerprint"] is None


@pytest.mark.asyncio
async def test_internal_workspace_stale_ready_is_local_nonready_and_single_flight():
    import asyncio

    import orchestrator.main as orch_main

    started = asyncio.Event()
    release = asyncio.Event()

    async def _ensure(*_args, **_kwargs):
        started.set()
        await release.wait()

    probe = AsyncMock(return_value=False)
    ensure = AsyncMock(side_effect=_ensure)
    # R1.B05 moved the module dict into an application-owned registry.
    orch_main._stateless_workspace_ensure_registry.discard(THREAD_ID)
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(orch_main, "ensure_session_workspace", ensure),
    ):
        first = await _internal_workspace_response_for_lite_thread(
            _stateless_sandbox_thread()
        )
        await started.wait()
        task = orch_main._stateless_workspace_ensure_registry.get(THREAD_ID)
        assert task is not None
        second = await _internal_workspace_response_for_lite_thread(
            _stateless_sandbox_thread()
        )

        ensure.assert_awaited_once()
        assert probe.await_count == 2
        for response in (first, second):
            assert response["status"] == "creating"
            assert response["pod_ip"] is None
            assert response["pod_port"] is None
            assert response["workspace_generation"] is None
            assert response["workspace_runtime_incarnation"] is None
            assert response["workspace_ssh_host_key_fingerprint"] is None

        release.set()
        await task
        await asyncio.sleep(0)

    assert orch_main._stateless_workspace_ensure_registry.get(THREAD_ID) is None


@pytest.mark.asyncio
async def test_internal_workspace_unknown_probe_never_returns_cached_endpoint():
    import orchestrator.main as orch_main

    schedule = MagicMock()
    with (
        patch.object(
            orch_main.container_provisioner,
            "workspace_pod_live",
            AsyncMock(return_value=None),
        ),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(
            _stateless_sandbox_thread()
        )

    schedule.assert_not_called()
    assert response["status"] == "creating"
    assert response["pod_ip"] is None
    assert response["workspace_generation"] is None
    assert response["workspace_runtime_incarnation"] is None
    assert response["workspace_ssh_host_key_fingerprint"] is None


@pytest.mark.asyncio
async def test_internal_workspace_rechecks_row_after_delayed_live_probe():
    """A delayed U1 probe cannot publish U1 after durable state moved to U2."""
    import copy

    import orchestrator.main as orch_main

    old = _stateless_sandbox_thread()
    replacement = copy.deepcopy(old)
    replacement_ws = replacement["metadata"]["workspace_container"]
    replacement_ws.update(
        {
            "pod_ip": "10.42.0.99",
            "_runtime_incarnation": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        }
    )
    schedule = MagicMock()
    with (
        patch.object(
            orch_main.container_provisioner,
            "workspace_pod_live",
            AsyncMock(return_value=True),
        ),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(
            old,
            thread_reads=[old, replacement],
        )

    # U2 has not itself been probed yet, so this response is deliberately
    # non-ready and carries neither U1 nor U2 endpoint credentials.
    assert response["status"] == "creating"
    assert response["pod_ip"] is None
    assert response["workspace_generation"] is None
    assert response["workspace_runtime_incarnation"] is None
    assert response["workspace_ssh_host_key_fingerprint"] is None
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_internal_workspace_final_read_rejects_real_nonready_replacement():
    """Response redaction must not weaken the final durable U1 -> U2 fence."""
    import copy

    import orchestrator.main as orch_main

    old = _stateless_sandbox_thread()
    old["metadata"]["workspace_container"]["status"] = "creating"
    replacement = copy.deepcopy(old)
    replacement["metadata"]["workspace_container"].update(
        {
            "_canvas_workspace_generation": ("55555555-5555-4555-8555-555555555555"),
            "_runtime_incarnation": "66666666-6666-4666-8666-666666666666",
        }
    )
    replacement["metadata"]["_workspace_binding"]["generation"] = (
        "55555555-5555-4555-8555-555555555555"
    )
    schedule = MagicMock()

    with (
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
        pytest.raises(HTTPException) as exc_info,
    ):
        await _internal_workspace_response_for_lite_thread(
            old,
            thread_reads=[old, replacement],
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {"code": "workspace_runtime_identity_changed"}
    schedule.assert_called_once_with(THREAD_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace_status",
    ["deleted", "failed", "suspended", "restoring", "created", "creating", "pending"],
)
async def test_internal_workspace_nonready_claim_restarts_reconcile_without_endpoint(
    workspace_status,
):
    """A durable pending input needs no second user action after lifecycle drift."""
    import orchestrator.main as orch_main

    thread = _stateless_sandbox_thread()
    thread["metadata"]["workspace_container"]["status"] = workspace_status
    schedule = MagicMock()
    probe = AsyncMock(side_effect=AssertionError("non-ready rows need no live probe"))
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(thread)

    schedule.assert_called_once_with(THREAD_ID)
    probe.assert_not_awaited()
    assert response["status"] == workspace_status
    assert response["pod_ip"] is None
    assert response["pod_port"] is None
    assert response["workspace_generation"] is None
    assert response["workspace_runtime_incarnation"] is None
    assert response["workspace_ssh_host_key_fingerprint"] is None


@pytest.mark.asyncio
async def test_internal_workspace_pinned_sandbox_requires_exact_live_authority():
    import orchestrator.main as orch_main

    thread = _stateless_sandbox_thread()
    thread["execution_lane"] = "pinned"
    probe = AsyncMock(side_effect=AssertionError("pinned route must not attest UID"))
    attestation = orch_main.WorkspaceRuntimeAttestation(
        backing_id="k8s-pvc:agent-workspaces:pvc-uid",
        workspace_generation=WORKSPACE_GENERATION,
        runtime_incarnation=WORKSPACE_RUNTIME_INCARNATION,
        ssh_host_key_fingerprint=WORKSPACE_SSH_HOST_KEY_FINGERPRINT,
        host="ws-thread-111111111111.agent-workspaces.svc.cluster.local",
        pod_ip="10.42.0.25",
        port=30022,
    )
    attest = AsyncMock(return_value=attestation)
    schedule = MagicMock()
    with (
        patch.object(orch_main.container_provisioner, "workspace_pod_live", probe),
        patch.object(
            orch_main.container_provisioner,
            "attest_workspace_runtime",
            attest,
        ),
        patch.object(orch_main, "_schedule_stateless_workspace_ensure", schedule),
    ):
        response = await _internal_workspace_response_for_lite_thread(thread)

    probe.assert_not_awaited()
    assert attest.await_count == 2
    assert all(
        call.args == (orch_main.WorkspaceOwner.session(THREAD_ID),)
        for call in attest.await_args_list
    )
    schedule.assert_not_called()
    assert response["status"] == "ready"
    assert response["pod_ip"] == "10.42.0.25"
    assert response["workspace_generation"] == WORKSPACE_GENERATION
    assert response["workspace_runtime_incarnation"] == WORKSPACE_RUNTIME_INCARNATION
    assert (
        response["workspace_ssh_host_key_fingerprint"]
        == WORKSPACE_SSH_HOST_KEY_FINGERPRINT
    )


@pytest.mark.asyncio
async def test_none_tier_suppresses_structured_and_legacy_cloud_surfaces():
    """ScratchBackend state is intentionally disposable and must not mirror."""

    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "stateless",
        "nc_session_folder": "Sessions/should-not-be-used",
        "metadata": {"config_override": {"workspace": {"backend": "none"}}},
    }
    response = await _internal_workspace_response_for_lite_thread(
        thread,
        cloud_mount={"driver": "rclone", "mounts": [{"id": "unexpected"}]},
        cloud_sync={"backend": "nextcloud", "webdav_url": "http://unexpected"},
    )

    assert response["workspace_generation"] is None
    assert response["cloud_mount"] is None
    assert response["cloud_sync"] is None
    assert response["nc_session_folder"] is None


@pytest.mark.asyncio
async def test_pinned_none_tier_preserves_historical_cloud_surfaces():
    """The S2 stateless guard must not change the pinned attach contract."""

    cloud_sync = {"backend": "nextcloud", "webdav_url": "http://historical"}
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "pinned",
        "nc_session_folder": "Sessions/pinned-none",
        "metadata": {"config_override": {"workspace": {"backend": "none"}}},
    }
    response = await _internal_workspace_response_for_lite_thread(
        thread,
        cloud_mount=None,
        cloud_sync=cloud_sync,
    )

    assert response["cloud_mount"] is None
    assert response["cloud_sync"] == cloud_sync
    assert response["nc_session_folder"] == "Sessions/pinned-none"


@pytest.mark.asyncio
async def test_stateless_virtual_workspace_without_binding_fails_attach_payload():
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "stateless",
        "nc_session_folder": "Sessions/thread",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }

    with pytest.raises(HTTPException) as exc_info:
        await _internal_workspace_response_for_lite_thread(
            thread,
            cloud_sync={"backend": "nextcloud", "webdav_url": "http://cloud"},
        )

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_stateless_virtual_workspace_wrong_backing_fails_closed():
    """A UUID from an old object-store namespace is not an attestation."""

    import orchestrator.main as orch_main

    current_spec = {
        "type": "s3",
        "root": "current-bucket",
        "config": {"endpoint": "https://current.example.test"},
    }
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "stateless",
        "nc_session_folder": "Sessions/thread",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "kind": "virtual",
                "generation": WORKSPACE_GENERATION,
                "backing_id": f"rclone:{'0' * 64}",
            },
        },
    }

    with (
        patch.object(
            orch_main, "_virtual_workspace_rclone_spec", return_value=current_spec
        ),
        pytest.raises(HTTPException) as exc_info,
    ):
        await _internal_workspace_response_for_lite_thread(
            thread,
            cloud_sync={"backend": "nextcloud", "webdav_url": "http://cloud"},
        )

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_stateless_virtual_workspace_exact_backing_exposes_generation():
    """The current deterministic namespace may attest its binding UUID."""

    import orchestrator.main as orch_main
    from orchestrator.services.workspace_binding import virtual_thread_backing_id

    current_spec = {
        "type": "s3",
        "root": "current-bucket",
        "config": {"endpoint": "https://current.example.test"},
    }
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "stateless",
        "nc_session_folder": "Sessions/thread",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "kind": "virtual",
                "generation": WORKSPACE_GENERATION,
                "backing_id": virtual_thread_backing_id(THREAD_ID, current_spec),
            },
        },
    }

    with patch.object(
        orch_main, "_virtual_workspace_rclone_spec", return_value=current_spec
    ):
        response = await _internal_workspace_response_for_lite_thread(
            thread,
            cloud_sync={"backend": "nextcloud", "webdav_url": "http://cloud"},
        )

    assert response["workspace_generation"] == WORKSPACE_GENERATION


@pytest.mark.parametrize(("stateless", "expects_sync"), [(True, False), (False, True)])
@pytest.mark.asyncio
async def test_none_agent_cloud_suppression_is_stateless_only(
    monkeypatch, stateless: bool, expects_sync: bool
):
    """Pinned ScratchBackend keeps its sync and pinned-inbox attach paths."""

    if stateless:
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    else:
        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)

    postgres = MagicMock(name="non_null_pool_connection")

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.cloud_mount_manager = None
            self.cloud_mount_error = None
            self.overlay_mount_manager = None
            self.workspace_manager = SimpleNamespace(
                path="/workspace",
                backend=SimpleNamespace(supports_file_tools=False),
            )
            self.workspace_sync = None
            self.cloud_sync_workspace_generation = ""
            self.postgres_conn = postgres
            self.tool_context = None

        async def setup(self, **kwargs):
            return None

        async def cleanup(self, **kwargs):
            return None

        async def recover_subagents(self):
            return []

    cloud_sync = {"backend": "nextcloud", "webdav_url": "http://historical"}
    workspace_payload = {
        "cloud_mount": None,
        "cloud_sync": cloud_sync,
        "nc_session_folder": "Sessions/pinned-none",
        "protected_cloud": False,
        "project_ids": [],
        "datasources": None,
    }
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace_payload),
        agent_id=None,
    )
    agent = SimpleNamespace(
        config=SimpleNamespace(workspace=SimpleNamespace(backend="none")),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=None,
        postgres_conn=postgres,
        vector_conn=None,
    )
    workspace_sync = MagicMock()
    workspace_sync.pull_all = AsyncMock()
    workspace_sync.__len__.return_value = 1

    with (
        patch.object(papp, "_session", None),
        patch.object(papp, "_thread_id", None),
        patch.object(papp, "_event_writer", None),
        patch.object(papp, "_agent", agent),
        patch.object(papp, "_orchestrator_client", client),
        patch.object(papp, "PersistentSession", FakeSession),
        patch.object(papp, "_session_backend_is_lite", return_value=True),
        patch.object(
            papp, "_build_sync_coordinator", return_value=workspace_sync
        ) as build_sync,
        patch.object(papp, "_restore_session_messages", AsyncMock()),
        patch.object(
            papp,
            "_reclaim_pending_pinned_inputs",
            AsyncMock(return_value=set()),
        ) as reclaim_pinned,
        patch.object(
            papp, "_resolve_event_journal_epoch", AsyncMock(return_value=(1, 0))
        ),
        patch.object(papp, "_OrderedPersistentEventWriter") as writer_cls,
        patch.object(papp, "_update_thread_status", AsyncMock()),
        patch.object(papp, "_start_watchdogs"),
        patch.object(papp, "_officer_cfg", return_value=None),
        patch.object(papp, "_apply_session_embedding_env"),
    ):
        lease_reset = None
        try:
            if stateless:
                _, lease_reset = _install_lease()
            await papp._attach_session(THREAD_ID, config_override={})
        finally:
            if lease_reset is not None:
                current_lease.reset(lease_reset)
            papp._session = None
            papp._thread_id = None

    if expects_sync:
        build_sync.assert_called_once()
        assert build_sync.call_args.kwargs["cloud_cfg"] == cloud_sync
        workspace_sync.pull_all.assert_awaited_once_with()
    else:
        build_sync.assert_not_called()
        workspace_sync.pull_all.assert_not_awaited()
    if stateless:
        reclaim_pinned.assert_not_awaited()
    else:
        reclaim_pinned.assert_awaited_once_with()
    writer_cls.assert_called_once()


@pytest.mark.asyncio
async def test_late_workspace_fetch_retains_generation_without_coordinator():
    """No-cloud pending-row checks must see the final authoritative identity."""

    instances = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            instances.append(self)
            self.cloud_mount_manager = None
            self.cloud_mount_error = None
            self.overlay_mount_manager = None
            self.workspace_manager = SimpleNamespace(
                path="/workspace",
                backend=SimpleNamespace(supports_file_tools=True),
            )
            self.workspace_sync = None
            self.cloud_sync_workspace_generation = ""
            self.postgres_conn = None
            self.tool_context = None

        async def setup(self, **kwargs):
            return None

        async def recover_subagents(self):
            return []

    # The readiness response has no generation. The first metadata hydration
    # fetch also lacks it; only the later cloud-config fetch carries the binding
    # generation, and deliberately carries no cloud config/coordinator.
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(
            side_effect=[
                {"project_ids": [], "datasources": None, "cloud_mount": None},
                {
                    "workspace_generation": WORKSPACE_GENERATION,
                    "cloud_sync": None,
                    "nc_session_folder": None,
                    "cloud_sync_degraded": False,
                },
            ]
        )
    )
    readiness = {
        "backend": "sandbox",
        "remote": {"host": "10.0.0.8", "port": 22},
        "cloud_sync": None,
        "nc_session_folder": None,
    }
    agent = SimpleNamespace(
        config=object(),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=None,
        postgres_conn=None,
        vector_conn=None,
    )

    with (
        patch.object(papp, "_session", None),
        patch.object(papp, "_thread_id", None),
        patch.object(papp, "_event_writer", None),
        patch.object(papp, "_agent", agent),
        patch.object(papp, "_orchestrator_client", client),
        patch.object(papp, "PersistentSession", FakeSession),
        patch.object(papp, "_poll_workspace_ready", AsyncMock(return_value=readiness)),
        patch.object(papp, "_build_sync_coordinator") as build_sync,
        patch.object(papp, "_restore_session_messages", AsyncMock()),
        patch.object(papp, "_update_thread_status", AsyncMock()),
        patch.object(papp, "_start_watchdogs"),
        patch.object(papp, "_officer_cfg", return_value=None),
        patch.object(papp, "_apply_session_embedding_env"),
    ):
        await papp._attach_session(THREAD_ID, config_override={})

    assert client.get_thread_workspace.await_count == 2
    assert len(instances) == 1
    assert instances[0].workspace_sync is None
    assert instances[0].cloud_sync_workspace_generation == WORKSPACE_GENERATION
    build_sync.assert_not_called()
