"""A dedicated VM attach can wait while its Pod serves process health.

The finite startup allowance covers initialization and mixed-version pods.
The agent's own exits still bound non-waiting attach paths:

* a genuinely failed VM ends the poll at once (``vm_status='failed'``), and
  a VM that never becomes ready ends it at the agent's budget, which is below
  the orchestrator's readiness budget and therefore below the allowance;
* an End or Delete while the VM still boots reaches the poll as the ended-
  session fence (409 ``session_ended``), which the dedicated lifespan turns into
  ``_exit_session_ended``: deregister and exit 0 without ending the thread a
  second time and without waiting for the VM.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from agent.api import persistent_app, session_workspace
from agent.api.orchestrator_client import OrchestratorClient, SessionEnded
from orchestrator.services import session_workspace_policy


_FIELDS = {
    "_thread_id": (persistent_app._session_identity, "_thread_id"),
    "_session_runtime_generation": (persistent_app._session_identity, "_runtime_generation"),
    "_session_runtime_attach_token": (persistent_app._session_identity, "_runtime_attach_token"),
    "_pinned_runtime_generation_enabled": (persistent_app._session_identity, "_runtime_contract"),
    "_pool_attach_claim": (persistent_app._session_attach, "_pool_claim"),
    "_pool_attach_task": (persistent_app._session_attach, "_pool_task"),
    "_dedicated_attach_task": (persistent_app._session_attach, "_startup_task"),
    "_failed_attach_release_receipt": (persistent_app._session_attach, "_release_receipt"),
    "_pending_drain_suspend": (persistent_app._session_termination, "pending_drain_suspend"),
    "_retirement_admission_identity": (persistent_app._session_termination, "retirement_admission_identity"),
    "_terminating": (persistent_app._session_termination, "terminating"),
    "_termination_task": (persistent_app._session_termination, "termination_task"),
    "_sessions_served": (persistent_app._session_termination, "sessions_served"),
    "_max_sessions_per_process": (persistent_app._session_termination, "max_sessions_per_process"),
    "_attach_session": (persistent_app._session_attach, "attach"),
    "_begin_exact_session_retirement": (persistent_app._session_termination, "begin_retirement"),
    "_settle_exact_retirement_after_quiescence": (persistent_app._session_termination, "settle_exact_retirement_after_quiescence"),
    "_stop_and_join_watchdogs": (persistent_app._session_termination, "stop_and_join_watchdogs"),
    "_quiesce_session_side_tasks": (persistent_app._session_termination, "quiesce_session_side_tasks"),
}

def _patch(monkeypatch, name, value):
    owner, attr = _FIELDS.get(name, (persistent_app, name))
    monkeypatch.setattr(owner, attr, value)

def _preserve(monkeypatch, name):
    owner, attr = _FIELDS.get(name, (persistent_app, name))
    monkeypatch.setattr(owner, attr, getattr(owner, attr))

async def _poll_workspace_ready(*args, **kwargs):
    return await session_workspace.poll_workspace_ready(
        *args, **kwargs
    )

@pytest.mark.asyncio
async def test_dedicated_lifespan_serves_health_while_exact_attach_waits(monkeypatch):
    """A capacity wait beyond the old probe allowance must not block startup."""
    for name in (
        "_config_path",
        "_thread_id",
        "_agent",
        "_orchestrator_client",
        "_heartbeat_task",
        "_dedicated_attach_task",
        "_started_at",
    ):
        _preserve(monkeypatch, name)
    thread_id = str(uuid4())
    attach_started = asyncio.Event()
    admit = asyncio.Event()
    shutdown_order = []
    client = MagicMock()
    client.connect = AsyncMock()
    client.register = AsyncMock(return_value=True)
    client.deregister = AsyncMock()
    client.close = AsyncMock(side_effect=lambda: shutdown_order.append("client_closed"))

    async def heartbeat(**_kwargs):
        await asyncio.Event().wait()

    client.run_heartbeat_loop = heartbeat
    agent = MagicMock(config=SimpleNamespace(agent_id=str(uuid4())))
    agent.initialize = AsyncMock()
    agent.shutdown = AsyncMock()
    monkeypatch.setattr(persistent_app.UniversalAgent, "from_config", lambda _: agent)
    monkeypatch.setattr(
        persistent_app, "create_orchestrator_client_from_env", lambda _: client
    )
    monkeypatch.setattr(persistent_app, "_app_guide_health", lambda: {"state": "ready"})
    monkeypatch.setattr(persistent_app, "_session", None)
    _patch(monkeypatch, "_pool_attach_claim", None)

    async def attach(_thread_id):
        assert _thread_id == thread_id
        attach_started.set()
        try:
            await admit.wait()
        finally:
            shutdown_order.append("attach_stopped")

    _patch(monkeypatch, "_attach_session", attach)
    app = persistent_app.create_persistent_app("session_base", thread_id=thread_id)
    context = persistent_app.lifespan(app)
    enter = asyncio.create_task(context.__aenter__())
    try:
        await asyncio.wait_for(attach_started.wait(), timeout=1)
        await asyncio.wait_for(asyncio.shield(enter), timeout=0.1)
        # The old default startup probe gives this VM Session only 1060 s.
        monkeypatch.setattr(
            persistent_app,
            "_started_at",
            persistent_app._started_at - timedelta(seconds=1100),
        )
        health = next(route.endpoint for route in app.routes if route.path == "/health")
        ready = next(route.endpoint for route in app.routes if route.path == "/ready")
        assert (await health()).status_code == 200
        assert (await ready()).status_code == 503
        assert persistent_app._session_attach.pool_heartbeat_status() != "ready"
        monkeypatch.setattr(
            persistent_app, "_pinned_session_recipient_refusal", lambda *_a, **_k: None
        )
        second = await persistent_app._pool_session_attach_response(
            {"thread_id": str(uuid4())}
        )
        assert second.status_code == 409
    finally:
        if enter.done() and not enter.cancelled() and enter.exception() is None:
            await context.__aexit__(None, None, None)
        else:
            enter.cancel()
            try:
                await enter
            except asyncio.CancelledError:
                pass
    assert shutdown_order == ["attach_stopped", "client_closed"]


@pytest.mark.asyncio
async def test_completed_dedicated_attach_can_detach_and_rejoin_idle_pool(monkeypatch):
    """Normal REST detach leaves a completed startup task but frees the process."""
    mod = persistent_app
    first_thread = str(uuid4())
    next_thread = str(uuid4())
    attached = []
    session = MagicMock()
    session.config = SimpleNamespace(officer=SimpleNamespace(enabled=False))
    session.workspace_sync = None
    session.workspace_manager = None
    session.memory_service = None
    session.messages = []
    session.shell_owner_token = None
    session.terminal_finalization_attempted = False
    session.quiesce_background_tasks = AsyncMock()
    session.cleanup = AsyncMock()

    async def attach(thread_id, **_kwargs):
        attached.append(thread_id)
        if thread_id == first_thread:
            mod._session_identity.bind_thread(thread_id)
            mod._session = session

    for name, value in (
        ("_thread_id", None),
        ("_session", None),
        ("_loop_task", None),
        ("_event_writer", None),
        ("_orchestrator_client", None),
        ("_pool_attach_claim", None),
        ("_pool_attach_task", None),
        ("_dedicated_attach_task", None),
        ("_pending_drain_suspend", None),
        ("_failed_attach_release_receipt", None),
        ("_session_runtime_generation", None),
        ("_session_runtime_attach_token", None),
        ("_retirement_admission_identity", None),
        ("_pinned_runtime_generation_enabled", False),
        ("_control_owner_agent_id", None),
        ("_terminating", False),
        ("_termination_task", None),
        ("_sessions_served", 0),
        ("_max_sessions_per_process", 0),
    ):
        _patch(monkeypatch, name, value)
    monkeypatch.delenv("POD_UID", raising=False)
    monkeypatch.delenv("SESSION_BOUND_THREAD_ID", raising=False)
    _patch(monkeypatch, "_attach_session", attach)
    monkeypatch.setattr(mod, "_registered_pinned_agent_id", lambda: None)
    monkeypatch.setattr(mod, "_stateless_mode", lambda: False)
    monkeypatch.setattr(
        mod._session_termination, "begin_retirement", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        mod._session_termination, "settle_exact_retirement_after_quiescence", AsyncMock(return_value=True)
    )
    for name in (
        "_stop_and_join_watchdogs",
        "_retire_announced_permission_rows",
        "_stop_thread_interrupt_watcher",
        "_stop_thread_control_watcher",
        "_quiesce_session_side_tasks",
    ):
        _patch(monkeypatch, name, AsyncMock())

    dedicated = asyncio.create_task(mod._session_attach.run_dedicated_attach(first_thread, on_failure=mod._session_termination.handle_attach_failure))
    _patch(monkeypatch, "_dedicated_attach_task", dedicated)
    await dedicated
    assert attached == [first_thread]
    assert mod._session_attach.pool_heartbeat_status() == "session"
    active_refusal = await mod._pool_session_attach_response({"thread_id": next_thread})
    assert active_refusal.status_code == 409

    await mod._session_termination.terminate("rest_detach")
    assert mod._session is None
    assert mod._session_identity.thread_id is None
    assert dedicated.done() and mod._session_attach.startup_task is dedicated
    assert mod._session_attach.pool_heartbeat_status() == "ready"

    response = await mod._pool_session_attach_response({"thread_id": next_thread})
    assert response.status_code == 200
    assert mod._session_attach.pool_heartbeat_status() == "session"
    pool_task = mod._session_attach.pool_task
    assert pool_task is not None
    await pool_task
    assert attached == [first_thread, next_thread]
    assert mod._session_attach.pool_claim is None


@pytest.mark.asyncio
async def test_pending_dedicated_attach_refuses_without_thread_or_drain(monkeypatch):
    """An in-flight task alone must remain nonclaimable with a controlled 409."""
    mod = persistent_app
    pending = asyncio.create_task(asyncio.Event().wait())
    for name, value in (
        ("_thread_id", None),
        ("_session", None),
        ("_pool_attach_claim", None),
        ("_pending_drain_suspend", None),
        ("_failed_attach_release_receipt", None),
        ("_dedicated_attach_task", pending),
    ):
        _patch(monkeypatch, name, value)
    monkeypatch.delenv("POD_UID", raising=False)
    try:
        assert mod._session_attach.pool_heartbeat_status() == "session"
        response = await mod._pool_session_attach_response({"thread_id": str(uuid4())})
        assert response.status_code == 409
        assert b"Already attached" in response.body
        assert mod._session_attach.pool_claim is None
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,exit_name",
    [
        (SessionEnded("ended"), "_exit_session_ended"),
        (persistent_app.SessionGrantDenied("denied"), "_exit_grant_denied"),
        (persistent_app.MemoryUnavailableError("memory"), "_exit_memory_unavailable"),
        (persistent_app.WorkspaceNotReady("workspace"), "_exit_workspace_not_ready"),
    ],
)
async def test_dedicated_background_attach_preserves_exact_exit_reason(
    monkeypatch, error, exit_name
):
    _patch(monkeypatch, "_attach_session", AsyncMock(side_effect=error))
    exit_handler = AsyncMock()
    monkeypatch.setattr(persistent_app._session_termination, exit_name.removeprefix("_"), exit_handler)
    await persistent_app._session_attach.run_dedicated_attach("tid", on_failure=persistent_app._session_termination.handle_attach_failure)
    assert exit_handler.await_count == 1
    assert exit_handler.await_args.args[0] == "tid"


def _client():
    return OrchestratorClient(
        orchestrator_url="http://localhost:8085",
        pod_ip="10.0.0.5",
        pod_port=8001,
        hostname="test-agent",
        config_name="creator",
        pid=12345,
    )


@pytest.mark.asyncio
async def test_workspace_poll_reports_the_ended_session_fence():
    client = _client()
    response = MagicMock()
    response.status_code = 409
    response.json.return_value = {
        "detail": {"code": "session_ended", "message": "Session has ended."}
    }
    client._client = MagicMock()
    client._client.get = AsyncMock(return_value=response)

    with pytest.raises(SessionEnded):
        await client.get_thread_workspace("tid", raise_on_denied=True)


@pytest.mark.asyncio
async def test_vm_attach_stops_when_the_session_ends_mid_boot():
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [
        {"vm_status": "provisioning"},
        {"vm_status": "created"},
        SessionEnded("session ended before workspace attach"),
    ]

    with pytest.raises(SessionEnded):
        await session_workspace.poll_workspace_ready(
            client, "tid", timeout=120, poll_interval=0, require_vm=True
        )
    assert client.get_thread_workspace.call_count == 3


@pytest.mark.asyncio
async def test_vm_attach_that_never_becomes_ready_ends_at_the_agent_budget(
    monkeypatch,
):
    """The agent's own deadline, not the kubelet, ends a VM that never boots."""

    clock = {"now": 0.0}
    monkeypatch.setattr("time.monotonic", lambda: clock["now"])

    async def _sleep(seconds):
        clock["now"] += 30.0

    monkeypatch.setattr(persistent_app.asyncio, "sleep", _sleep)
    client = AsyncMock()
    client.get_thread_workspace.return_value = {"vm_status": "provisioning"}

    result = await session_workspace.poll_workspace_ready(
        client, "tid", timeout=120, poll_interval=30, require_vm=True, vm_timeout=900
    )

    assert result is None
    assert 900 <= clock["now"] < session_workspace_policy.session_ready_timeout_s("vm")


@pytest.fixture
def vm_startup_clock(monkeypatch):
    runtime, request, provision = (str(uuid4()) for _ in range(3))
    clock = {"now": 0.0}
    monkeypatch.setattr("time.monotonic", lambda: clock["now"])
    _patch(monkeypatch, "_session_runtime_generation", runtime)

    async def sleep(seconds):
        clock["now"] += seconds

    monkeypatch.setattr(persistent_app.asyncio, "sleep", sleep)
    identity = {
        "contract_version": 1,
        "request_id": request,
        "provision_generation": provision,
        "runtime_generation": runtime,
    }
    wait = {
        "status": "creating",
        "vm_status": "waiting_capacity",
        "session_runtime_generation": runtime,
        "vm_startup": {**identity, "phase": "resource_wait"},
    }
    admitted = {
        **wait,
        "vm_status": "provisioning",
        "vm_startup": {**identity, "phase": "admitted", "admission_elapsed_s": 30.0},
    }
    ready = {
        **admitted,
        "status": "ready",
        "vm_status": "ready",
        "vm_ssh_host": "10.42.1.23",
        "vm_ssh_port": 22,
        "workspace_generation": provision,
        "workspace_runtime_incarnation": str(uuid4()),
        "workspace_ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
    }
    return clock, wait, admitted, ready


@pytest.mark.asyncio
async def test_current_resource_wait_can_outlast_900_then_admit_same_vm(
    vm_startup_clock,
):
    clock, wait, admitted, ready = vm_startup_clock
    client = AsyncMock()

    async def response(*_args, **_kwargs):
        if clock["now"] < 1830:
            return wait
        if clock["now"] < 1860:
            return admitted
        return ready

    client.get_thread_workspace.side_effect = response
    result = await _poll_workspace_ready(
        client, "tid", timeout=120, poll_interval=30, require_vm=True, vm_timeout=900
    )
    assert clock["now"] == 1860
    assert result["backend"] == "vm"
    assert result["session_runtime_generation"] == wait["session_runtime_generation"]
    assert result["workspace_generation"] == ready["workspace_generation"]
    assert (
        result["workspace_runtime_incarnation"]
        == ready["workspace_runtime_incarnation"]
    )
    assert (
        result["workspace_ssh_host_key_fingerprint"]
        == ready["workspace_ssh_host_key_fingerprint"]
    )
    assert result["remote"]["host"] == ready["vm_ssh_host"]


@pytest.mark.asyncio
async def test_current_resource_wait_signal_loss_expires_after_120(vm_startup_clock):
    clock, wait, _, _ = vm_startup_clock
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [wait] + [None] * 8
    assert (
        await _poll_workspace_ready(
            client,
            "tid",
            timeout=120,
            poll_interval=30,
            require_vm=True,
            vm_timeout=900,
        )
        is None
    )
    assert clock["now"] == 120
    assert client.get_thread_workspace.await_count == 4


@pytest.mark.asyncio
async def test_typed_resource_wait_propagates_session_ended(vm_startup_clock):
    _, wait, _, _ = vm_startup_clock
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [
        wait,
        SessionEnded("session ended while waiting for resources"),
    ]
    with pytest.raises(SessionEnded):
        await _poll_workspace_ready(
            client, "tid", timeout=120, poll_interval=30, require_vm=True
        )
    assert client.get_thread_workspace.await_count == 2


@pytest.mark.asyncio
async def test_typed_resource_wait_refuses_ready_sandbox(vm_startup_clock):
    clock, wait, _, _ = vm_startup_clock
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [
        {**wait, "status": "ready", "pod_ip": "10.0.0.9", "pod_port": 30022}
    ] + [None] * 8
    assert (
        await _poll_workspace_ready(
            client, "tid", timeout=120, poll_interval=30, require_vm=True
        )
        is None
    )
    assert clock["now"] == 120


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_version", True),
        ("contract_version", 2),
        ("request_id", "not-a-uuid"),
        ("provision_generation", "not-a-uuid"),
        ("runtime_generation", "not-a-uuid"),
        ("admission_elapsed_s", True),
        ("admission_elapsed_s", float("nan")),
        ("admission_elapsed_s", float("inf")),
        ("admission_elapsed_s", -1),
    ],
)
async def test_malformed_startup_contract_cannot_extend_or_ready(
    vm_startup_clock, field, value
):
    clock, wait, admitted, ready = vm_startup_clock
    broken = {**admitted, "vm_startup": {**admitted["vm_startup"], field: value}}
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [wait, broken, ready]
    assert (
        await _poll_workspace_ready(
            client,
            "tid",
            timeout=120,
            poll_interval=30,
            require_vm=True,
            vm_timeout=900,
        )
        is None
    )
    assert clock["now"] == 30
    assert client.get_thread_workspace.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_field",
    [
        "request_id",
        "provision_generation",
        "runtime_generation",
        "outer_runtime",
    ],
)
async def test_changed_startup_tuple_and_expired_admission_refuse_ready(
    vm_startup_clock, changed_field
):
    clock, wait, admitted, ready = vm_startup_clock
    changed = {**admitted, "vm_startup": {**admitted["vm_startup"]}}
    if changed_field == "outer_runtime":
        changed["session_runtime_generation"] = str(uuid4())
    else:
        changed["vm_startup"][changed_field] = str(uuid4())
    for signal in (
        changed,
        {**ready, "vm_startup": {**ready["vm_startup"], "admission_elapsed_s": 901}},
    ):
        clock["now"] = 0
        client = AsyncMock()
        client.get_thread_workspace.side_effect = [wait, signal, ready]
        assert (
            await _poll_workspace_ready(
                client,
                "tid",
                timeout=120,
                poll_interval=30,
                require_vm=True,
                vm_timeout=900,
            )
            is None
        )
        assert client.get_thread_workspace.await_count == 2


@pytest.mark.asyncio
async def test_first_vm_ready_after_expired_admission_is_refused(vm_startup_clock):
    _, _, _, ready = vm_startup_clock
    ready = {**ready, "vm_startup": {**ready["vm_startup"], "admission_elapsed_s": 901}}
    client = AsyncMock()
    client.get_thread_workspace.return_value = ready
    assert (
        await _poll_workspace_ready(
            client,
            "tid",
            timeout=120,
            poll_interval=30,
            require_vm=True,
            vm_timeout=900,
        )
        is None
    )
    assert client.get_thread_workspace.await_count == 1


@pytest.mark.asyncio
async def test_admitted_budget_never_increases_or_returns_to_wait(vm_startup_clock):
    clock, wait, admitted, ready = vm_startup_clock
    client = AsyncMock()

    async def response(*_args, **_kwargs):
        if clock["now"] == 0:
            return wait
        if clock["now"] == 30:
            return admitted
        if clock["now"] == 60:
            return {
                **admitted,
                "vm_startup": {**admitted["vm_startup"], "admission_elapsed_s": 600},
            }
        if clock["now"] == 90:
            return {
                **admitted,
                "vm_startup": {**admitted["vm_startup"], "admission_elapsed_s": 1},
            }
        if clock["now"] == 120:
            return wait
        return ready

    client.get_thread_workspace.side_effect = response
    assert (
        await _poll_workspace_ready(
            client,
            "tid",
            timeout=120,
            poll_interval=30,
            require_vm=True,
            vm_timeout=900,
        )
        is None
    )
    assert clock["now"] == 120
    assert client.get_thread_workspace.await_count == 5
