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

from agent.api import persistent_app
from agent.api.orchestrator_client import OrchestratorClient, SessionEnded
from orchestrator.services import session_workspace_policy


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
        monkeypatch.setattr(persistent_app, name, getattr(persistent_app, name))
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
    monkeypatch.setattr(persistent_app, "_pool_attach_claim", None)

    async def attach(_thread_id):
        assert _thread_id == thread_id
        attach_started.set()
        try:
            await admit.wait()
        finally:
            shutdown_order.append("attach_stopped")

    monkeypatch.setattr(persistent_app, "_attach_session", attach)
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
        assert persistent_app._pool_heartbeat_status() != "ready"
        monkeypatch.setattr(
            persistent_app, "_pinned_session_recipient_refusal", lambda *_a, **_k: None
        )
        second = await persistent_app._admit_pool_session_attach(
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
    monkeypatch.setattr(persistent_app, "_attach_session", AsyncMock(side_effect=error))
    exit_handler = AsyncMock()
    monkeypatch.setattr(persistent_app, exit_name, exit_handler)
    await persistent_app._run_dedicated_attach("tid")
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
        await persistent_app._poll_workspace_ready(
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

    result = await persistent_app._poll_workspace_ready(
        client, "tid", timeout=120, poll_interval=30, require_vm=True, vm_timeout=900
    )

    assert result is None
    assert 900 <= clock["now"] < session_workspace_policy.session_ready_timeout_s("vm")


@pytest.fixture
def vm_startup_clock(monkeypatch):
    runtime, request, provision = (str(uuid4()) for _ in range(3))
    clock = {"now": 0.0}
    monkeypatch.setattr("time.monotonic", lambda: clock["now"])
    monkeypatch.setattr(persistent_app, "_session_runtime_generation", runtime)

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
    result = await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
            await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
        await persistent_app._poll_workspace_ready(
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
