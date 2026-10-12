"""Session transport owners driven through fake ports, without the runtime.

Composed-route behaviour is characterized in
tests/test_session_transport_characterization.py. These tests show the
transport's own contract: it reads every runtime value through call-time
providers, invokes runtime operations only through its ports, and releases
exactly one connection's resources on every exit path.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient

from agent.api import session_transport, session_websocket
from agent.api._session_auth import SessionAuthBindings, validate_session_token
from agent.api.session_canvas_control import CanvasControlChannel
from agent.api.session_contract import (
    AcceptedInput,
    SessionOperations,
    SessionRuntimeView,
)
from agent.api.session_http import (
    SessionHttpPorts,
    handle_input,
    register_session_http_routes,
)
from agent.api.session_websocket import (
    SessionConnectionPorts,
    SessionSocketCommands,
    SessionSocketPorts,
    SessionWelcomePorts,
    serve_session_websocket,
)

FP_A = "sha256:" + ("a" * 64)
FP_B = "sha256:" + ("b" * 64)


def _session(**overrides):
    values = dict(
        thread_id="thread-a",
        turn_count=3,
        messages=[],
        config=SimpleNamespace(llm=SimpleNamespace(model="m", temperature=0.2)),
        session_task_manager=None,
        shell_owner_token=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class FakeRuntime:
    """Mutable runtime state read through the transport's providers."""

    def __init__(self):
        self.session = _session()
        self.fingerprint = FP_A
        self.stateless = False
        self.closed = False
        self.retiring = False
        self.ready = True
        self.queue = asyncio.Queue()
        self.accepted = []
        self.subscribers: dict[str, asyncio.Queue] = {}
        self.events: list[tuple] = []

    def view(self) -> SessionRuntimeView:
        return SessionRuntimeView(
            stateless_mode=lambda: self.stateless,
            session=lambda: self.session,
            thread_id=lambda: getattr(self.session, "thread_id", None),
            identity_fingerprint=lambda: self.fingerprint,
            runtime_admission_closed=lambda: self.closed,
            retirement_admission_closed=lambda: self.retiring,
            protected_cloud_ready=lambda: True,
            session_ready=lambda: self.ready,
            input_queue=lambda: self.queue,
            turn_open=lambda: False,
            tool_inflight=lambda: False,
        )

    async def accept_input(self, content, **kwargs):
        self.accepted.append((content, kwargs))
        return AcceptedInput(
            message_id="m-1",
            delivery_id="d-1",
            delivery_state="deferred",
            claim_generation=1,
            enqueued=False,
            deferred=True,
        )

    def operations(self) -> SessionOperations:
        return SessionOperations(
            ensure_loop_started=MagicMock(return_value=True),
            accept_input=self.accept_input,
            signal_interrupt=MagicMock(return_value=None),
            resolve_permission=AsyncMock(return_value=None),
        )

    def subscribe(self, client_id):
        self.events.append(("subscribe", client_id))
        queue = session_transport.subscribe(self.subscribers, client_id, maxsize=10)
        return queue

    def unsubscribe(self, client_id):
        self.events.append(("unsubscribe", client_id))
        session_transport.unsubscribe(self.subscribers, client_id)

    def socket_ports(self, canvas=None) -> SessionSocketPorts:
        return SessionSocketPorts(
            runtime=self.view(),
            operations=self.operations(),
            connection=SessionConnectionPorts(
                subscribe=self.subscribe,
                unsubscribe=self.unsubscribe,
                note_connection_arrived=lambda: self.events.append(("arrived",)),
                track_side_task=lambda task: task,
            ),
            welcome=SessionWelcomePorts(
                durable_control_modes=AsyncMock(return_value=("supervised", "auto")),
                pending_permissions=AsyncMock(return_value=[]),
                running_tool=lambda _session: None,
            ),
            commands=SessionSocketCommands(
                config_update=AsyncMock(),
                compact=AsyncMock(),
                archive=AsyncMock(),
                vm_upgrade=AsyncMock(),
                workspace_upgrade=AsyncMock(),
                rewind=AsyncMock(),
            ),
            canvas=canvas
            or CanvasControlChannel(
                load_state=AsyncMock(return_value=None),
                invalidate_recent_read=MagicMock(),
                identity_fingerprint=lambda: self.fingerprint,
                broadcast=MagicMock(),
                fan_out_live=MagicMock(),
            ),
        )


def _socket(fingerprint=FP_A, frames=()):
    ws = MagicMock()
    ws.state = SimpleNamespace(session_identity_fingerprint=fingerprint)
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.send_json = AsyncMock()
    ws.receive_text = AsyncMock(side_effect=[*frames, WebSocketDisconnect()])
    return ws


def _sent(ws, method):
    return [
        call.args[0]["params"]
        for call in ws.send_json.await_args_list
        if call.args[0]["method"] == method
    ]


@pytest.mark.asyncio
async def test_socket_reads_the_runtime_at_each_boundary_not_at_binding():
    runtime = FakeRuntime()
    ports = runtime.socket_ports()
    # Replace the attached session after the ports were built: the handshake
    # recheck must observe the successor and refuse the predecessor's socket.
    runtime.session = _session(thread_id="thread-b")
    runtime.fingerprint = FP_B

    ws = _socket(fingerprint=FP_A)
    await serve_session_websocket(ws, ports)

    ws.close.assert_awaited_once_with(code=4403, reason="session identity changed")
    assert runtime.subscribers == {}
    assert ("subscribe",) not in {event[:1] for event in runtime.events}


@pytest.mark.asyncio
async def test_deferred_socket_input_is_acknowledged_with_the_shared_payload():
    runtime = FakeRuntime()
    ws = _socket(frames=[json.dumps({"method": "message", "content": "hi"})])

    await serve_session_websocket(ws, runtime.socket_ports())

    assert runtime.accepted == [("hi", {"expected_session_identity_fingerprint": FP_A})]
    assert _sent(ws, "input.accepted") == [
        {
            "accepted": True,
            "message_id": "m-1",
            "duplicate": False,
            "deferred": True,
            "retryable": False,
            "delivery_id": "d-1",
            "delivery_state": "deferred",
        }
    ]
    # Disconnect released exactly this connection.
    client_ids = [event[1] for event in runtime.events if event[0] == "subscribe"]
    assert len(client_ids) == 1
    assert ("unsubscribe", client_ids[0]) in runtime.events
    assert runtime.subscribers == {}


@pytest.mark.asyncio
async def test_cancelled_connection_releases_only_its_own_resources(monkeypatch):
    runtime = FakeRuntime()
    other = session_transport.subscribe(runtime.subscribers, "other", maxsize=10)
    canvas = MagicMock(spec=CanvasControlChannel)
    ports = runtime.socket_ports(canvas=canvas)
    pump_started, pump_cancelled = asyncio.Event(), asyncio.Event()

    async def _pump(ws, queue, *, ping_interval):
        assert ping_interval == session_websocket.WS_PING_INTERVAL_S
        pump_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pump_cancelled.set()
            raise

    monkeypatch.setattr(session_transport, "run_subscriber_pump", _pump)
    blocked = asyncio.Event()

    async def _receive_forever():
        blocked.set()
        await asyncio.Future()

    ws = _socket()
    ws.receive_text = AsyncMock(side_effect=_receive_forever)
    task = asyncio.create_task(serve_session_websocket(ws, ports))
    await asyncio.wait_for(blocked.wait(), timeout=2)
    await asyncio.wait_for(pump_started.wait(), timeout=2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert pump_cancelled.is_set()
    client_id = next(event[1] for event in runtime.events if event[0] == "subscribe")
    canvas.release.assert_called_once_with(client_id)
    canvas.clear_all.assert_not_called()
    assert runtime.subscribers == {"other": other}
    ports.operations.ensure_loop_started.assert_called_once_with(
        "websocket", client_id=client_id
    )


@pytest.mark.asyncio
async def test_stateless_runtime_refuses_the_socket_before_subscribing():
    runtime = FakeRuntime()
    runtime.stateless = True
    ws = _socket()

    await serve_session_websocket(ws, runtime.socket_ports())

    ws.close.assert_awaited_once_with(code=4409, reason="stateless executor")
    assert runtime.events == []


@pytest.mark.asyncio
async def test_http_input_reads_queue_and_turn_at_response_time():
    runtime = FakeRuntime()
    runtime.queue.put_nowait("earlier")
    ports = SessionHttpPorts(runtime=runtime.view(), operations=runtime.operations())
    request = SimpleNamespace(
        headers={},
        json=AsyncMock(
            return_value={"content": "hello", "session_identity_fingerprint": FP_A}
        ),
    )

    response = await handle_input(request, ports)

    assert response.status_code == 202
    assert json.loads(response.body) == {
        "accepted": True,
        "message_id": "m-1",
        "duplicate": False,
        "deferred": True,
        "retryable": False,
        "delivery_id": "d-1",
        "delivery_state": "deferred",
        "turn_id": 3,
        "queue_depth": 1,
    }
    ports.operations.ensure_loop_started.assert_called_once_with("rest_input")


def test_http_routes_run_the_mode_precheck_before_the_runtime():
    runtime = FakeRuntime()
    operations = runtime.operations()
    app = FastAPI()
    register_session_http_routes(
        app,
        SessionHttpPorts(runtime=runtime.view(), operations=operations),
        precheck=lambda: __import__("fastapi").responses.JSONResponse(
            {"error": "Pod is not in session mode"}, status_code=404
        ),
        tags=["Session"],
    )
    client = TestClient(app)

    for path in ("/api/input", "/api/interrupt", "/api/approve"):
        response = client.post(path, json={"content": "x"})
        assert response.status_code == 404
        assert response.json() == {"error": "Pod is not in session mode"}
    operations.ensure_loop_started.assert_not_called()
    assert {
        route.path: route.tags for route in app.routes if route.path.startswith("/api/")
    } == {
        "/api/input": ["Session"],
        "/api/interrupt": ["Session"],
        "/api/approve": ["Session"],
    }


class _AuthSocket:
    def __init__(self, token=None):
        self.query_params = {"t": token} if token else {}
        self.state = SimpleNamespace()
        self.accept = AsyncMock()
        self.close = AsyncMock()


def _mint(thread_id, fingerprint, secret="pod-secret-for-bindings-tests-0123456789"):
    from orchestrator.services.session_tokens import SessionTokenService

    token, _ = SessionTokenService(secret).mint(
        "u1", thread_id, session_identity_fingerprint=fingerprint
    )
    return token


@pytest.mark.asyncio
async def test_auth_bindings_follow_the_currently_attached_session(monkeypatch):
    monkeypatch.setenv("SESSION_JWT_SECRET", "pod-secret-for-bindings-tests-0123456789")
    monkeypatch.delenv("SESSION_BOUND_THREAD_ID", raising=False)
    attached = {"thread": "thread-a", "fingerprint": FP_A}
    bindings = SessionAuthBindings(
        attached_thread_id=lambda: attached["thread"],
        identity_fingerprint=lambda: attached["fingerprint"],
    )
    token_a = _mint("thread-a", FP_A)

    ws = _AuthSocket(token_a)
    assert await validate_session_token(ws, bindings) is True
    assert ws.state.session_identity_fingerprint == FP_A

    # A pool pod re-attached to a new session: the old token is refused and
    # the successor's accepted, with the same bindings object.
    attached.update(thread="thread-b", fingerprint=FP_B)
    stale = _AuthSocket(token_a)
    assert await validate_session_token(stale, bindings) is False
    stale.close.assert_awaited_once_with(code=4403, reason="session token mismatch")
    fresh = _AuthSocket(_mint("thread-b", FP_B))
    assert await validate_session_token(fresh, bindings) is True

    # Same thread, successor generation: identity mismatch, not tid mismatch.
    attached.update(thread="thread-b", fingerprint=FP_A)
    successor = _AuthSocket(_mint("thread-b", FP_B))
    assert await validate_session_token(successor, bindings) is False
    successor.close.assert_awaited_once_with(
        code=4403, reason="session token identity mismatch"
    )


@pytest.mark.asyncio
async def test_env_bound_thread_takes_precedence_over_the_attached_session(
    monkeypatch,
):
    monkeypatch.setenv("SESSION_JWT_SECRET", "pod-secret-for-bindings-tests-0123456789")
    monkeypatch.setenv("SESSION_BOUND_THREAD_ID", "thread-env")
    attached_thread = MagicMock(return_value="thread-attached")
    bindings = SessionAuthBindings(
        attached_thread_id=attached_thread,
        identity_fingerprint=lambda: FP_A,
    )

    assert await validate_session_token(
        _AuthSocket(_mint("thread-env", FP_A)), bindings
    )
    refused = _AuthSocket(_mint("thread-attached", FP_A))
    assert await validate_session_token(refused, bindings) is False
    refused.close.assert_awaited_once_with(code=4403, reason="session token mismatch")
    attached_thread.assert_not_called()


@pytest.mark.asyncio
async def test_failing_or_missing_bindings_fail_closed(monkeypatch):
    monkeypatch.setenv("SESSION_JWT_SECRET", "pod-secret-for-bindings-tests-0123456789")
    monkeypatch.delenv("SESSION_BOUND_THREAD_ID", raising=False)

    def _boom():
        raise RuntimeError("runtime not composed")

    for bindings in (
        SessionAuthBindings(
            attached_thread_id=_boom, identity_fingerprint=lambda: FP_A
        ),
        SessionAuthBindings(
            attached_thread_id=lambda: "thread-a", identity_fingerprint=_boom
        ),
        SessionAuthBindings(
            attached_thread_id=lambda: "thread-a", identity_fingerprint=lambda: None
        ),
        SessionAuthBindings(
            attached_thread_id=lambda: "", identity_fingerprint=lambda: FP_A
        ),
    ):
        ws = _AuthSocket(_mint("thread-a", FP_A))
        assert await validate_session_token(ws, bindings) is False
        ws.close.assert_awaited_once_with(
            code=4500, reason="pod missing session auth config"
        )
