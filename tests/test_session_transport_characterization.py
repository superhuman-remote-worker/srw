"""Characterize the session client transport through the composed routes only.

R3.2 moves the session HTTP/WebSocket handlers (``/api/input``,
``/api/interrupt``, ``/api/approve``, ``/ws/chat``, ``/p/{thread_id}/ws``) out
of ``persistent_app`` into transport modules. These tests drive nothing but the
public routes of ``create_persistent_app`` and ``create_dual_app`` and patch
only runtime state/functions that stay owned by ``persistent_app`` /
``dual_app``, so they must pass unchanged before and after the move.

The production lifespan is replaced by a no-op ASGI shim (it would register
with an orchestrator); every route is served by the composed app itself. One
``TestClient`` portal per test keeps every socket on one event loop, so live
fan-out reaches concurrent clients the way it does in a pod.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import anyio
import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage, HumanMessage
from starlette.websockets import WebSocket, WebSocketDisconnect

from agent.services.workspace_undo import (
    WorkspaceUndoRetryable,
    WorkspaceUndoUnavailable,
)
from orchestrator.services.session_tokens import SessionTokenService
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint


SECRET = "r3-transport-characterization-secret-0123456789"
THREAD_ID = "11111111-1111-4111-8111-111111111111"
OTHER_THREAD_ID = "66666666-6666-4666-8666-666666666666"
GEN_A = "22222222-2222-4222-8222-222222222222"
GEN_B = "77777777-7777-4777-8777-777777777777"
AGENT_ID = "33333333-3333-4333-8333-333333333333"
ATTACH_A = "44444444-4444-4444-8444-444444444444"
ATTACH_B = "88888888-8888-4888-8888-888888888888"
POD_UID = "55555555-5555-4555-8555-555555555555"
DELIVERY_ID = "99999999-9999-4999-8999-999999999999"
INTERNAL_KEY = "internal-key-for-characterization"


def _fingerprint(
    *,
    thread_id: str = THREAD_ID,
    generation: str = GEN_A,
    attach_token: str = ATTACH_A,
) -> str:
    value = pinned_session_ready_identity_fingerprint(
        thread_id=thread_id,
        runtime_generation=generation,
        agent_id=AGENT_ID,
        runtime_attach_token=attach_token,
        pod_uid=POD_UID,
    )
    assert value is not None
    return value


FP_A = _fingerprint()
FP_B = _fingerprint(generation=GEN_B, attach_token=ATTACH_B)


def _token(
    *,
    thread_id: str = THREAD_ID,
    fingerprint: str = FP_A,
    secret: str = SECRET,
    ttl_seconds: int = 60,
) -> str:
    token, _ = SessionTokenService(secret, ttl_seconds=ttl_seconds).mint(
        "user-1", thread_id, session_identity_fingerprint=fingerprint
    )
    return token


def _url(path: str, token: str | None) -> str:
    return path if token is None else f"{path}?t={token}"


STATELESS_REST_BODY = {
    "error": (
        "stateless executor: this pod serves queued turns from the "
        "run_queue (threads.execution_lane='stateless'); direct "
        "session attach/input is not accepted here"
    )
}
STATELESS_WS_ERROR = {
    "method": "error",
    "params": {
        "message": (
            "stateless executor: this pod serves queued turns; "
            "no direct session WebSocket is available"
        )
    },
}
TERMINATING_REST_BODY = {
    "error": "runtime_terminating",
    "retryable": True,
    "message": "Persistent runtime is terminating; retry on its replacement.",
}
TERMINATING_WS_PARAMS = {
    "error": "runtime_terminating",
    "retryable": True,
    "message": "Retry input on the replacement runtime.",
}
TERMINATING_WS_REJECTION = {"method": "input.rejected", "params": TERMINATING_WS_PARAMS}
PROTECTED_REST_BODY = {
    "error": "protected_cloud_unavailable",
    "retryable": True,
    "message": "Protected cloud is temporarily unavailable; retry when it recovers.",
}
DURABLE_REST_BODY = {
    "error": "durable_input_unavailable",
    "retryable": True,
    "message": "Durable input admission is temporarily unavailable.",
}
MISMATCH_BODY = {"error": "session_identity_mismatch", "retryable": True}
SESSION_NOT_ACTIVE_BODY = {"error": "Session not active"}
NOT_SESSION_MODE_BODY = {"error": "Pod is not in session mode"}
CONTROL_RETIRED_FRAME = {
    "method": "error",
    "params": {
        "code": "control_transport_retired",
        "message": "Use the session control REST endpoint",
    },
}

APP_KINDS = ["persistent", "dual"]
WS_PATHS = ["/ws/chat", f"/p/{THREAD_ID}/ws"]
REST_ROUTES = ["/api/input", "/api/interrupt", "/api/approve"]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _without_lifespan(app):
    """Serve the composed app's routes without its production lifespan."""

    async def asgi(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        await app(scope, receive, send)

    return asgi


def _recv(ws, timeout: float = 5.0) -> dict:
    """Receive one JSON frame; a server close raises WebSocketDisconnect.

    Bounded so a missing frame fails the test instead of hanging the run.
    """

    async def _get():
        with anyio.fail_after(timeout):
            return await ws._send_rx.receive()

    message = ws.portal.call(_get)
    ws._raise_on_close(message)
    return json.loads(message["text"])


def _until_close(client: TestClient, url: str) -> tuple[list[dict], int, str]:
    """Connect, collect every frame, and return them with the close code/reason."""
    frames: list[dict] = []
    with client.websocket_connect(url) as ws:
        while True:
            try:
                frames.append(_recv(ws))
            except WebSocketDisconnect as exc:
                return frames, exc.code, exc.reason


def _barrier(ws) -> None:
    """Round-trip an unknown verb: every earlier frame has been fully handled
    and nothing else was sent to this socket in between."""
    ws.send_json({"method": "__barrier__"})
    assert _recv(ws) == {
        "method": "error",
        "params": {"message": "Unknown method: __barrier__"},
    }


def _admission(
    *,
    state: str = "queued",
    deferred: bool = False,
    duplicate: bool = False,
    enqueued: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        message_id="msg-1",
        delivery_id=DELIVERY_ID,
        delivery_state=state,
        claim_generation=4,
        enqueued=enqueued,
        duplicate=duplicate,
        deferred=deferred,
    )


def _accepted_payload(admission: SimpleNamespace) -> dict:
    return {
        "accepted": True,
        "message_id": admission.message_id,
        "duplicate": admission.duplicate,
        "deferred": admission.deferred,
        "retryable": False,
        "delivery_id": admission.delivery_id,
        "delivery_state": admission.delivery_state,
    }


def _make_session(*, thread_id: str = THREAD_ID, name: str = "session-A") -> MagicMock:
    session = MagicMock(name=name)
    session.thread_id = thread_id
    session.llm_with_tools = MagicMock(name=f"{name}-llm")
    session.protected_cloud_required = False
    session.turn_count = 3
    session.messages = []
    session.config.llm.model = "test-model"
    session.config.llm.temperature = 0.25
    session.session_task_manager = None
    session.permission_mode = "ram-permission"
    session.narration_mode = "ram-narration"
    session.postgres_conn = None
    session.shell_owner_token = None
    session.tool_context = None
    return session


class _Runtime:
    """Test-owned view of the persistent runtime seams."""

    def __init__(self, pa, monkeypatch):
        self.pa = pa
        self._monkeypatch = monkeypatch
        self._signatures: dict[str, inspect.Signature] = {}

    def set(self, name: str, value) -> None:
        self._monkeypatch.setattr(self.pa, name, value)

    def patch(self, name: str, mock):
        self._signatures[name] = inspect.signature(getattr(self.pa, name))
        self.set(name, mock)
        return mock

    def bound(self, name: str, mock_call) -> dict:
        """Normalise one recorded call against the real function's signature."""
        bound = self._signatures[name].bind(*mock_call.args, **mock_call.kwargs)
        bound.apply_defaults()
        return dict(bound.arguments)


@pytest.fixture
def pa():
    import agent.api.persistent_app as module

    return module


@pytest.fixture
def dual():
    import agent.api.dual_app as module

    return module


@pytest.fixture
def rt(pa, monkeypatch):
    monkeypatch.setenv("SESSION_JWT_SECRET", SECRET)
    monkeypatch.setenv("SESSION_BOUND_THREAD_ID", THREAD_ID)
    monkeypatch.setenv("POD_UID", POD_UID)
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    monkeypatch.delenv("MCP_INTERNAL_KEY", raising=False)

    runtime = _Runtime(pa, monkeypatch)
    runtime.session = _make_session()
    runtime.queue = asyncio.Queue()
    runtime.hard_event = asyncio.Event()
    for name, value in {
        "_session": runtime.session,
        "_thread_id": THREAD_ID,
        "_session_runtime_generation": GEN_A,
        "_session_runtime_attach_token": ATTACH_A,
        "_input_runtime_generation": GEN_A,
        "_orchestrator_client": MagicMock(agent_id=AGENT_ID),
        "_loop_user_queue": runtime.queue,
        "_loop_task": None,
        "_ws_connected_event": None,
        "_turn_event_open": False,
        "_tool_inflight": False,
        "_loop_interrupt_flag": None,
        "_loop_interrupt_target_turn_id": None,
        "_hard_interrupt_event": runtime.hard_event,
        "_subscribers": {},
        "_termination_admission_fenced": False,
        "_retirement_admission_identity": None,
    }.items():
        runtime.set(name, value)

    runtime.accept = runtime.patch(
        "_accept_user_input",
        AsyncMock(name="_accept_user_input", return_value=_admission()),
    )
    runtime.loop_start = runtime.patch(
        "_ensure_persistent_loop_started",
        MagicMock(name="_ensure_persistent_loop_started", return_value=True),
    )
    runtime.control_modes = runtime.patch(
        "_durable_session_control_modes",
        AsyncMock(return_value=("durable-permission", "durable-narration")),
    )
    runtime.pending = runtime.patch(
        "_pending_permission_requests", AsyncMock(return_value=[])
    )
    runtime.thread_status = runtime.patch("_safe_set_thread_status", AsyncMock())
    runtime.resolve = runtime.patch(
        "_resolve_pending_permission", AsyncMock(return_value=None)
    )
    return runtime


@contextmanager
def _serve(kind: str, pa, dual, monkeypatch):
    if kind == "persistent":
        app = pa.create_persistent_app("dummy_config", THREAD_ID)
    else:
        monkeypatch.setattr(dual, "_pod_state", dual.PodState.SESSION)
        monkeypatch.setattr(dual, "_pending_exit_task", None)
        app = dual.create_dual_app(None)
    with TestClient(_without_lifespan(app)) as test_client:
        yield test_client


@pytest.fixture(params=APP_KINDS)
def app_kind(request):
    return request.param


@pytest.fixture
def client(app_kind, rt, pa, dual, monkeypatch):
    with _serve(app_kind, pa, dual, monkeypatch) as test_client:
        yield test_client


@pytest.fixture
def dual_client(rt, pa, dual, monkeypatch):
    with _serve("dual", pa, dual, monkeypatch) as test_client:
        yield test_client


@pytest.fixture(params=WS_PATHS)
def ws_path(request):
    return request.param


@contextmanager
def _session_ws(client: TestClient, path: str, token: str | None = None):
    """An authenticated socket whose welcome frame has been consumed."""
    with client.websocket_connect(_url(path, token or _token())) as ws:
        welcome = _recv(ws)
        assert welcome["method"] == "session.state"
        ws.welcome = welcome
        yield ws


def _input_body(**overrides) -> dict:
    return {"content": "hello", "session_identity_fingerprint": FP_A, **overrides}


# ---------------------------------------------------------------------------
# 1. WebSocket handshake authentication
# ---------------------------------------------------------------------------


class TestWebSocketHandshakeAuth:
    def test_missing_token_closes_4401(self, client, ws_path):
        assert _until_close(client, ws_path) == ([], 4401, "missing session token")

    def test_bad_signature_closes_4401(self, client, ws_path):
        token = _token(secret="another-secret-that-is-long-enough-0123456789")
        assert _until_close(client, _url(ws_path, token)) == (
            [],
            4401,
            "invalid session token",
        )

    def test_expired_token_closes_4401(self, client, ws_path):
        token = _token(ttl_seconds=-60)
        assert _until_close(client, _url(ws_path, token)) == (
            [],
            4401,
            "invalid session token",
        )

    def test_token_for_other_thread_closes_4403(self, client, ws_path):
        token = _token(thread_id=OTHER_THREAD_ID)
        assert _until_close(client, _url(ws_path, token)) == (
            [],
            4403,
            "session token mismatch",
        )

    def test_predecessor_generation_token_closes_4403(self, client, ws_path):
        token = _token(fingerprint=_fingerprint(generation=GEN_B))
        assert _until_close(client, _url(ws_path, token)) == (
            [],
            4403,
            "session token identity mismatch",
        )

    def test_missing_secret_closes_4500(self, client, ws_path, monkeypatch):
        monkeypatch.delenv("SESSION_JWT_SECRET")
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4500,
            "pod missing session auth config",
        )

    def test_unregistered_agent_identity_closes_4500(self, client, ws_path, rt):
        rt.set("_orchestrator_client", None)
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4500,
            "pod missing session auth config",
        )

    def test_valid_token_reaches_the_session(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            assert ws.welcome["params"]["thread_id"] == THREAD_ID
            _barrier(ws)

    def test_session_path_segment_is_not_authority(self, client, rt):
        # The /p/{thread_id} segment is ignored; the token + bound thread decide.
        with _session_ws(client, f"/p/{OTHER_THREAD_ID}/ws") as ws:
            assert ws.welcome["params"]["thread_id"] == THREAD_ID

    def test_env_bound_thread_wins_over_attached_session_thread(
        self, client, ws_path, rt
    ):
        rt.session.thread_id = OTHER_THREAD_ID
        with _session_ws(client, ws_path, _token(thread_id=THREAD_ID)) as ws:
            assert ws.welcome["params"]["thread_id"] == THREAD_ID
        assert _until_close(
            client, _url(ws_path, _token(thread_id=OTHER_THREAD_ID))
        ) == ([], 4403, "session token mismatch")

    def test_pool_pod_without_env_uses_attached_session_thread(
        self, client, ws_path, monkeypatch
    ):
        monkeypatch.delenv("SESSION_BOUND_THREAD_ID")
        with _session_ws(client, ws_path, _token(thread_id=THREAD_ID)) as ws:
            assert ws.welcome["params"]["thread_id"] == THREAD_ID
        assert _until_close(
            client, _url(ws_path, _token(thread_id=OTHER_THREAD_ID))
        ) == ([], 4403, "session token mismatch")

    def test_pool_pod_without_env_or_session_closes_4500(
        self, client, ws_path, rt, monkeypatch
    ):
        monkeypatch.delenv("SESSION_BOUND_THREAD_ID")
        rt.set("_session", None)
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4500,
            "pod missing session auth config",
        )

    @pytest.mark.parametrize(
        "scenario", ["same_thread_successor", "pool_reattach_other_thread"]
    )
    def test_replacement_session_rebinds_the_handshake(
        self, client, ws_path, rt, monkeypatch, scenario
    ):
        if scenario == "pool_reattach_other_thread":
            monkeypatch.delenv("SESSION_BOUND_THREAD_ID")
        token_a = _token()
        with _session_ws(client, ws_path, token_a) as ws:
            assert ws.welcome["params"]["thread_id"] == THREAD_ID

        if scenario == "same_thread_successor":
            thread_b = THREAD_ID
            stale_reason = "session token identity mismatch"
        else:
            thread_b = OTHER_THREAD_ID
            stale_reason = "session token mismatch"
            rt.set("_thread_id", thread_b)
        rt.set("_session", _make_session(thread_id=thread_b, name="session-B"))
        rt.set("_session_runtime_generation", GEN_B)
        rt.set("_session_runtime_attach_token", ATTACH_B)
        rt.set("_input_runtime_generation", GEN_B)

        assert _until_close(client, _url(ws_path, token_a)) == (
            [],
            4403,
            stale_reason,
        )
        token_b = _token(
            thread_id=thread_b,
            fingerprint=_fingerprint(
                thread_id=thread_b, generation=GEN_B, attach_token=ATTACH_B
            ),
        )
        with _session_ws(client, ws_path, token_b) as ws:
            assert ws.welcome["params"]["thread_id"] == thread_b
            _barrier(ws)


# ---------------------------------------------------------------------------
# 2. Handshake -> handler session identity
# ---------------------------------------------------------------------------


class TestSessionIdentityChanges:
    def test_identity_change_after_validation_closes_4403(
        self, client, ws_path, rt, pa
    ):
        calls: list[int] = []

        def _fingerprint_then_rotate():
            calls.append(1)
            return FP_A if len(calls) == 1 else FP_B

        rt.set("_current_pinned_session_identity_fingerprint", _fingerprint_then_rotate)
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4403,
            "session identity changed",
        )
        rt.accept.assert_not_called()
        rt.loop_start.assert_not_called()
        assert pa._subscribers == {}

    @pytest.mark.parametrize("hook", ["control_modes", "pending"])
    def test_identity_change_while_welcome_is_assembled_closes_4403(
        self, client, ws_path, rt, pa, hook
    ):
        mock = getattr(rt, hook)
        result = mock.return_value

        def _rotate(*_args, **_kwargs):
            rt.set("_session_runtime_generation", GEN_B)
            return result

        mock.side_effect = _rotate
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4403,
            "session identity changed",
        )
        rt.loop_start.assert_not_called()
        assert pa._subscribers == {}

    def test_identity_rotation_on_live_socket_closes_before_dispatch(
        self, client, ws_path, rt, pa
    ):
        with _session_ws(client, ws_path) as ws:
            rt.set("_session_runtime_generation", GEN_B)
            ws.send_json({"method": "message", "content": "late"})
            with pytest.raises(WebSocketDisconnect) as exc_info:
                _recv(ws)
        assert (exc_info.value.code, exc_info.value.reason) == (
            4403,
            "session identity changed",
        )
        rt.accept.assert_not_called()
        assert pa._subscribers == {}

    def test_replaced_session_object_with_same_identity_closes_4403(
        self, client, ws_path, rt
    ):
        with _session_ws(client, ws_path) as ws:
            rt.set("_session", _make_session(name="session-B"))
            ws.send_json({"method": "interrupt"})
            with pytest.raises(WebSocketDisconnect) as exc_info:
                _recv(ws)
        assert (exc_info.value.code, exc_info.value.reason) == (
            4403,
            "session identity changed",
        )


# ---------------------------------------------------------------------------
# 3. Stateless executor
# ---------------------------------------------------------------------------


class TestStatelessExecutor:
    @pytest.mark.parametrize(
        "route,body",
        [
            ("/api/input", _input_body()),
            ("/api/interrupt", {"session_identity_fingerprint": FP_A}),
            ("/api/approve", {"decision": "approve"}),
        ],
    )
    def test_rest_routes_refuse_with_409(self, client, rt, monkeypatch, route, body):
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        rt.set("_turn_event_open", True)
        response = client.post(route, json=body)
        assert response.status_code == 409
        assert response.json() == STATELESS_REST_BODY
        rt.accept.assert_not_called()
        rt.resolve.assert_not_called()
        assert rt.pa._loop_interrupt_flag is None

    def test_ws_with_pinned_identity_gets_error_then_4409(
        self, client, ws_path, rt, pa, monkeypatch
    ):
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        assert _until_close(client, _url(ws_path, _token())) == (
            [STATELESS_WS_ERROR],
            4409,
            "stateless executor",
        )
        assert pa._subscribers == {}
        rt.loop_start.assert_not_called()

    def test_ws_without_registered_identity_closes_4500(
        self, client, ws_path, rt, monkeypatch
    ):
        # A stateless pod never registers a pinned agent id, so the handshake
        # validator fails closed before the stateless refusal is reached.
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        rt.set("_orchestrator_client", None)
        assert _until_close(client, _url(ws_path, _token())) == (
            [],
            4500,
            "pod missing session auth config",
        )


# ---------------------------------------------------------------------------
# 4. Termination / retirement fence
# ---------------------------------------------------------------------------


FENCES = [
    "runtime_admission_closed",
    "termination_fence_latched",
    "retirement_identity",
]


def _close_fence(rt: _Runtime, fence: str) -> None:
    if fence == "runtime_admission_closed":
        rt.set("_runtime_admission_closed", lambda: True)
    elif fence == "termination_fence_latched":
        rt.set("_termination_admission_fenced", True)
    else:
        rt.set("_retirement_admission_identity", (THREAD_ID, GEN_A, ATTACH_A))


class TestAdmissionFence:
    @pytest.mark.parametrize("fence", FENCES)
    def test_rest_input_returns_503_runtime_terminating(self, client, rt, fence):
        _close_fence(rt, fence)
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 503
        assert response.json() == TERMINATING_REST_BODY
        assert response.headers["retry-after"] == "5"
        rt.accept.assert_not_called()
        rt.loop_start.assert_not_called()

    def test_fence_latched_while_loop_starts_returns_503(self, client, rt):
        def _latch(*_args, **_kwargs):
            rt.set("_termination_admission_fenced", True)
            return False

        rt.loop_start.side_effect = _latch
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 503
        assert response.json() == TERMINATING_REST_BODY
        assert response.headers["retry-after"] == "5"
        rt.accept.assert_not_called()

    @pytest.mark.parametrize("fence", FENCES)
    def test_ws_connect_after_fence_rejects_then_4512(
        self, client, ws_path, rt, pa, fence
    ):
        event = asyncio.Event()
        rt.set("_ws_connected_event", event)
        _close_fence(rt, fence)
        assert _until_close(client, _url(ws_path, _token())) == (
            [TERMINATING_WS_REJECTION],
            4512,
            "runtime terminating",
        )
        # The fence answers before the boot watchdog is told a client arrived.
        assert not event.is_set()
        assert pa._subscribers == {}
        rt.loop_start.assert_not_called()

    @pytest.mark.parametrize(
        "frame",
        [
            {"method": "message", "content": "late"},
            {"method": "approve", "approval_id": "approval-1"},
            {"method": "config.update", "config": {"llm": {"model": "x"}}},
        ],
    )
    def test_retirement_on_live_socket_rejects_next_frame_then_4512(
        self, client, ws_path, rt, pa, frame
    ):
        handler = rt.patch("_handle_config_update", AsyncMock())
        with _session_ws(client, ws_path) as ws:
            _close_fence(rt, "retirement_identity")
            ws.send_json(frame)
            assert _recv(ws) == TERMINATING_WS_REJECTION
            with pytest.raises(WebSocketDisconnect) as exc_info:
                _recv(ws)
        assert (exc_info.value.code, exc_info.value.reason) == (
            4512,
            "runtime terminating",
        )
        rt.accept.assert_not_called()
        rt.resolve.assert_not_called()
        handler.assert_not_called()
        assert pa._subscribers == {}


# ---------------------------------------------------------------------------
# 5. Readiness and dual pod-state gates
# ---------------------------------------------------------------------------


class TestReadiness:
    def test_ws_not_ready_sends_error_then_4503(self, client, ws_path, rt, pa):
        event = asyncio.Event()
        rt.set("_ws_connected_event", event)
        rt.set("_session_ready", lambda: False)
        assert _until_close(client, _url(ws_path, _token())) == (
            [{"method": "error", "params": {"message": "Agent not ready"}}],
            4503,
            "Agent not ready",
        )
        # Even a not-ready connection counts as the user having come back.
        assert event.is_set()
        assert pa._subscribers == {}
        rt.loop_start.assert_not_called()

    def test_ws_without_loop_queue_is_not_ready(self, client, ws_path, rt):
        rt.set("_loop_user_queue", None)
        assert _until_close(client, _url(ws_path, _token())) == (
            [{"method": "error", "params": {"message": "Agent not ready"}}],
            4503,
            "Agent not ready",
        )

    def test_ws_without_llm_binding(self, client, app_kind, ws_path, rt):
        rt.session.llm_with_tools = None
        expected = {
            "persistent": (
                [{"method": "error", "params": {"message": "Agent not ready"}}],
                4503,
                "Agent not ready",
            ),
            "dual": (
                [{"method": "error", "params": {"message": "Session not ready"}}],
                4503,
                "Session not ready",
            ),
        }[app_kind]
        assert _until_close(client, _url(ws_path, _token())) == expected

    def test_ws_without_attached_session(self, client, app_kind, ws_path, rt):
        rt.set("_session", None)
        expected = {
            # Observed: the persistent route reports an identity change, not
            # "not ready", when a dedicated pod has no attached session.
            "persistent": ([], 4403, "session identity changed"),
            "dual": (
                [{"method": "error", "params": {"message": "Session not ready"}}],
                4503,
                "Session not ready",
            ),
        }[app_kind]
        assert _until_close(client, _url(ws_path, _token())) == expected

    @pytest.mark.parametrize("route", REST_ROUTES)
    def test_rest_without_session_returns_503(self, client, rt, route):
        rt.set("_session", None)
        response = client.post(route, json=_input_body(decision="approve"))
        assert response.status_code == 503
        assert response.json() == SESSION_NOT_ACTIVE_BODY

    def test_rest_input_without_loop_queue_returns_503(self, client, rt):
        rt.set("_loop_user_queue", None)
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 503
        assert response.json() == SESSION_NOT_ACTIVE_BODY
        rt.accept.assert_not_called()

    @pytest.mark.parametrize("pod_state", ["IDLE", "WORKING"])
    def test_dual_ws_outside_session_mode_closes_4403(
        self, dual_client, dual, ws_path, rt, pa, monkeypatch, pod_state
    ):
        monkeypatch.setattr(dual, "_pod_state", getattr(dual.PodState, pod_state))
        assert _until_close(dual_client, _url(ws_path, _token())) == (
            [{"method": "error", "params": {"message": "Pod is not in session mode"}}],
            4403,
            "Not in session mode",
        )
        assert pa._subscribers == {}

    def test_dual_ws_authenticates_before_pod_state(
        self, dual_client, dual, ws_path, monkeypatch
    ):
        monkeypatch.setattr(dual, "_pod_state", dual.PodState.IDLE)
        assert _until_close(dual_client, ws_path) == (
            [],
            4401,
            "missing session token",
        )

    @pytest.mark.parametrize("pod_state", ["IDLE", "WORKING"])
    @pytest.mark.parametrize("route", REST_ROUTES)
    def test_dual_rest_outside_session_mode_returns_404(
        self, dual_client, dual, rt, monkeypatch, route, pod_state
    ):
        monkeypatch.setattr(dual, "_pod_state", getattr(dual.PodState, pod_state))
        response = dual_client.post(route, json=_input_body(decision="approve"))
        assert response.status_code == 404
        assert response.json() == NOT_SESSION_MODE_BODY
        rt.accept.assert_not_called()
        rt.resolve.assert_not_called()

    @pytest.mark.parametrize("route", REST_ROUTES)
    def test_dual_pod_state_gate_precedes_stateless_refusal(
        self, dual_client, dual, monkeypatch, route
    ):
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        monkeypatch.setattr(dual, "_pod_state", dual.PodState.IDLE)
        response = dual_client.post(route, json=_input_body(decision="approve"))
        assert response.status_code == 404
        assert response.json() == NOT_SESSION_MODE_BODY

    def test_dual_ws_cancels_pending_exit_after_auth(
        self, dual_client, dual, ws_path, monkeypatch
    ):
        pending = MagicMock(name="pending-exit")
        pending.done.return_value = False
        monkeypatch.setattr(dual, "_pending_exit_task", pending)
        with _session_ws(dual_client, ws_path):
            pass
        pending.cancel.assert_called_once_with()
        assert dual._pending_exit_task is None

    def test_dual_ws_keeps_pending_exit_on_auth_failure(
        self, dual_client, dual, ws_path, monkeypatch
    ):
        pending = MagicMock(name="pending-exit")
        pending.done.return_value = False
        monkeypatch.setattr(dual, "_pending_exit_task", pending)
        assert _until_close(dual_client, ws_path)[1] == 4401
        pending.cancel.assert_not_called()
        assert dual._pending_exit_task is pending


# ---------------------------------------------------------------------------
# 6. Welcome frame
# ---------------------------------------------------------------------------


class TestWelcomeFrame:
    def test_session_state_snapshot(self, client, ws_path, rt):
        rt.session.turn_count = 7
        rt.session.messages = [HumanMessage(content="q"), AIMessage(content="a")]
        rt.session.config.llm.model = "model-x"
        rt.session.config.llm.temperature = 0.4
        manager = MagicMock()
        manager.to_dict_list.return_value = [{"id": "task-1", "status": "running"}]
        rt.session.session_task_manager = manager
        rt.set("_turn_event_open", True)
        pending = [{"approval_id": "approval-1", "tool": "run_command"}]
        rt.pending.return_value = pending

        with _session_ws(client, ws_path) as ws:
            welcome = ws.welcome

        assert welcome == {
            "method": "session.state",
            "params": {
                "thread_id": THREAD_ID,
                "permission_mode": "durable-permission",
                "narration_mode": "durable-narration",
                "turn_count": 7,
                "turn_in_flight": True,
                "message_count": 2,
                "model": "model-x",
                "temperature": 0.4,
                "running_tool": None,
                "pending_permissions": pending,
                "tasks": [{"id": "task-1", "status": "running"}],
            },
        }

    def test_idle_defaults(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            params = ws.welcome["params"]
        assert params["turn_in_flight"] is False
        assert params["running_tool"] is None
        assert params["pending_permissions"] == []
        assert params["tasks"] == []
        assert params["message_count"] == 0

    def test_running_tool_when_a_tool_is_in_flight(self, client, ws_path, rt):
        rt.session.messages = [
            HumanMessage(content="list files"),
            AIMessage(
                content="",
                tool_calls=[
                    {"id": "call-1", "name": "run_command", "args": {"cmd": "ls"}}
                ],
            ),
        ]
        rt.set("_tool_inflight", True)
        with _session_ws(client, ws_path) as ws:
            assert ws.welcome["params"]["running_tool"] == {
                "id": "call-1",
                "tool": "run_command",
                "args": {"cmd": "ls"},
            }

    def test_loop_started_for_the_subscriber_after_welcome(
        self, client, ws_path, rt, pa
    ):
        with _session_ws(client, ws_path) as ws:
            _barrier(ws)
            client_ids = list(pa._subscribers)
            assert len(client_ids) == 1
            assert rt.loop_start.call_count == 1
            assert rt.bound(
                "_ensure_persistent_loop_started", rt.loop_start.call_args
            ) == {
                "source": "websocket",
                "client_id": client_ids[0],
            }


# ---------------------------------------------------------------------------
# 7. Input over REST and WebSocket
# ---------------------------------------------------------------------------


REJECTIONS = [
    (
        "TerminationAdmissionClosed",
        503,
        TERMINATING_REST_BODY,
        "5",
        TERMINATING_WS_PARAMS,
    ),
    (
        "ProtectedCloudUnavailable",
        503,
        PROTECTED_REST_BODY,
        "5",
        {
            "error": "protected_cloud_unavailable",
            "retryable": True,
            "message": "Retry input when the protected cloud mount recovers.",
        },
    ),
    (
        "DurableInputUnavailable",
        503,
        DURABLE_REST_BODY,
        "5",
        {
            "error": "durable_input_unavailable",
            "retryable": True,
            "message": "Retry input when durable storage recovers.",
        },
    ),
    ("SessionIdentityMismatch", 409, MISMATCH_BODY, None, None),
]


class TestInput:
    def test_rest_deferred_admission_returns_202(self, client, rt):
        admission = _admission(state="deferred", deferred=True, enqueued=False)
        rt.accept.return_value = admission
        rt.queue.put_nowait("queued-1")
        rt.queue.put_nowait("queued-2")

        response = client.post("/api/input", json=_input_body())

        assert response.status_code == 202
        assert response.json() == {
            **_accepted_payload(admission),
            "turn_id": 3,
            "queue_depth": 2,
        }
        assert rt.bound("_accept_user_input", rt.accept.call_args) == {
            "content": "hello",
            "role": "human",
            "delivery_id": None,
            "expected_session_identity_fingerprint": FP_A,
        }
        assert rt.bound("_ensure_persistent_loop_started", rt.loop_start.call_args) == {
            "source": "rest_input",
            "client_id": None,
        }

    def test_rest_queued_admission_returns_202(self, client, rt):
        admission = _admission()
        rt.accept.return_value = admission
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 202
        assert response.json() == {
            **_accepted_payload(admission),
            "turn_id": 3,
            "queue_depth": 0,
        }

    @pytest.mark.parametrize("state", ["admitted", "settled", "cancelled"])
    def test_rest_duplicate_of_a_consumed_delivery_returns_200(self, client, rt, state):
        admission = _admission(state=state, duplicate=True, enqueued=False)
        rt.accept.return_value = admission
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 200
        assert response.json() == {
            **_accepted_payload(admission),
            "turn_id": 3,
            "queue_depth": 0,
        }

    def test_rest_event_with_internal_authority(self, client, rt, monkeypatch):
        monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
        response = client.post(
            "/api/input",
            json=_input_body(role="event", delivery_id=DELIVERY_ID.upper()),
            headers={"X-Internal-Key": INTERNAL_KEY},
        )
        assert response.status_code == 202
        assert rt.bound("_accept_user_input", rt.accept.call_args) == {
            "content": "hello",
            "role": "event",
            "delivery_id": DELIVERY_ID,
            "expected_session_identity_fingerprint": FP_A,
        }

    def test_ws_deferred_admission_frame_matches_rest_payload(
        self, client, ws_path, rt
    ):
        admission = _admission(state="deferred", deferred=True, enqueued=False)
        rt.accept.return_value = admission
        rest_body = client.post("/api/input", json=_input_body()).json()

        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "message", "content": "hello"})
            frame = _recv(ws)

        assert frame == {
            "method": "input.accepted",
            "params": _accepted_payload(admission),
        }
        rest_body.pop("turn_id")
        rest_body.pop("queue_depth")
        assert frame["params"] == rest_body
        assert [
            rt.bound("_accept_user_input", c) for c in rt.accept.call_args_list
        ] == [
            {
                "content": "hello",
                "role": "human",
                "delivery_id": None,
                "expected_session_identity_fingerprint": FP_A,
            }
        ] * 2

    @pytest.mark.parametrize(
        "admission",
        [
            _admission(),
            _admission(state="admitted", duplicate=True, enqueued=False),
        ],
        ids=["queued", "admitted-duplicate"],
    )
    def test_ws_non_deferred_admission_sends_no_frame(
        self, client, ws_path, rt, admission
    ):
        rt.accept.return_value = admission
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "message", "content": "hello"})
            _barrier(ws)
        rt.accept.assert_awaited_once()

    def test_ws_empty_content_is_ignored(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "message", "content": ""})
            ws.send_json({"method": "message"})
            _barrier(ws)
        rt.accept.assert_not_called()

    def test_ws_message_is_dropped_silently_without_loop_queue(
        self, client, ws_path, rt
    ):
        with _session_ws(client, ws_path) as ws:
            rt.set("_loop_user_queue", None)
            ws.send_json({"method": "message", "content": "hello"})
            # Observed: no rejection frame; the input is silently dropped.
            _barrier(ws)
        rt.accept.assert_not_called()

    @pytest.mark.parametrize(
        "name,status,body,retry_after,_ws_params",
        REJECTIONS,
        ids=[r[0] for r in REJECTIONS],
    )
    def test_rest_rejections(
        self, client, rt, pa, name, status, body, retry_after, _ws_params
    ):
        rt.accept.side_effect = getattr(pa, name)()
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == status
        assert response.json() == body
        assert response.headers.get("retry-after") == retry_after

    @pytest.mark.parametrize(
        "name,_status,_body,_retry_after,ws_params",
        REJECTIONS,
        ids=[r[0] for r in REJECTIONS],
    )
    def test_ws_rejections(
        self, client, ws_path, rt, pa, name, _status, _body, _retry_after, ws_params
    ):
        rt.accept.side_effect = getattr(pa, name)()
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "message", "content": "hello"})
            if ws_params is None:
                with pytest.raises(WebSocketDisconnect) as exc_info:
                    _recv(ws)
                assert (exc_info.value.code, exc_info.value.reason) == (
                    4403,
                    "session identity changed",
                )
            else:
                assert _recv(ws) == {"method": "input.rejected", "params": ws_params}
                _barrier(ws)
        assert (
            rt.bound("_accept_user_input", rt.accept.call_args)[
                "expected_session_identity_fingerprint"
            ]
            == FP_A
        )

    def test_rest_protected_cloud_unavailable_precedes_body_parsing(self, client, rt):
        rt.set("_protected_cloud_runtime_ready", lambda: False)
        response = client.post(
            "/api/input",
            content=b"{not json",
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 503
        assert response.json() == PROTECTED_REST_BODY
        assert response.headers["retry-after"] == "5"
        rt.accept.assert_not_called()

    @pytest.mark.parametrize(
        "payload,headers,status,body",
        [
            pytest.param(
                b"{not json", {}, 400, {"error": "invalid JSON"}, id="invalid-json"
            ),
            pytest.param(
                {"session_identity_fingerprint": FP_A},
                {},
                400,
                {"error": "content must be a non-empty string"},
                id="missing-content",
            ),
            pytest.param(
                _input_body(content=""),
                {},
                400,
                {"error": "content must be a non-empty string"},
                id="empty-content",
            ),
            pytest.param(
                _input_body(content=5),
                {},
                400,
                {"error": "content must be a non-empty string"},
                id="non-string-content",
            ),
            pytest.param(
                _input_body(role="system"),
                {},
                400,
                {"error": "role must be one of ['event', 'human']"},
                id="bad-role",
            ),
            pytest.param(
                _input_body(role="event"),
                {"X-Internal-Key": INTERNAL_KEY},
                400,
                {"error": "durable event input requires a delivery_id"},
                id="event-without-delivery-id",
            ),
            pytest.param(
                _input_body(role="event", delivery_id=DELIVERY_ID),
                {"X-Internal-Key": "wrong-key"},
                403,
                {"error": "durable event delivery requires internal authority"},
                id="event-wrong-internal-key",
            ),
            pytest.param(
                _input_body(role="event", delivery_id=DELIVERY_ID),
                {},
                403,
                {"error": "durable event delivery requires internal authority"},
                id="event-missing-internal-key",
            ),
            pytest.param(
                _input_body(role="event", delivery_id="not-a-uuid"),
                {"X-Internal-Key": "wrong-key"},
                403,
                {"error": "durable event delivery requires internal authority"},
                id="event-authority-checked-before-uuid",
            ),
            pytest.param(
                _input_body(role="event", delivery_id="not-a-uuid"),
                {"X-Internal-Key": INTERNAL_KEY},
                400,
                {"error": "delivery_id must be a UUID"},
                id="event-non-uuid-delivery-id",
            ),
            pytest.param(
                _input_body(delivery_id=DELIVERY_ID),
                {},
                400,
                {"error": "delivery_id is reserved for durable event input"},
                id="human-with-delivery-id",
            ),
            pytest.param(
                {"content": "hello"},
                {},
                409,
                MISMATCH_BODY,
                id="missing-fingerprint",
            ),
            pytest.param(
                _input_body(session_identity_fingerprint="sha256:ABC"),
                {},
                409,
                MISMATCH_BODY,
                id="malformed-fingerprint",
            ),
            pytest.param(
                _input_body(session_identity_fingerprint=FP_B),
                {},
                409,
                MISMATCH_BODY,
                id="stale-fingerprint",
            ),
        ],
    )
    def test_rest_validation(
        self, client, rt, monkeypatch, payload, headers, status, body
    ):
        monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
        if isinstance(payload, bytes):
            response = client.post(
                "/api/input",
                content=payload,
                headers={"content-type": "application/json", **headers},
            )
        else:
            response = client.post("/api/input", json=payload, headers=headers)
        assert response.status_code == status
        assert response.json() == body
        rt.accept.assert_not_called()
        rt.loop_start.assert_not_called()

    def test_rest_event_refused_when_pod_has_no_internal_key(self, client, rt):
        response = client.post(
            "/api/input",
            json=_input_body(role="event", delivery_id=DELIVERY_ID),
            headers={"X-Internal-Key": ""},
        )
        assert response.status_code == 403
        assert response.json() == {
            "error": "durable event delivery requires internal authority"
        }
        rt.accept.assert_not_called()

    def test_rest_loop_not_startable_returns_503(self, client, rt):
        rt.loop_start.return_value = False
        response = client.post("/api/input", json=_input_body())
        assert response.status_code == 503
        assert response.json() == {"error": "Session not ready"}
        rt.accept.assert_not_called()

    @pytest.mark.parametrize("route", ["/api/input", "/api/approve"])
    def test_rest_non_object_json_body_is_a_server_error(self, client, rt, route):
        # Observed: a JSON array body is not validated and surfaces as a 500.
        raw = TestClient(client.app, raise_server_exceptions=False)
        response = raw.post(route, json=["hello"])
        assert response.status_code == 500
        rt.accept.assert_not_called()
        rt.resolve.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Interrupt
# ---------------------------------------------------------------------------


def _interrupt_state(pa) -> tuple:
    return pa._loop_interrupt_flag, pa._loop_interrupt_target_turn_id


class TestInterrupt:
    def test_rest_legacy_interrupt_applies_hard(self, client, rt, pa):
        rt.set("_turn_event_open", True)
        response = client.post(
            "/api/interrupt", json={"session_identity_fingerprint": FP_A}
        )
        assert response.status_code == 200
        assert response.json() == {
            "ack": True,
            "applied": True,
            "target_turn_id": 3,
            "mode": "hard",
        }
        assert _interrupt_state(pa) == ("hard", 3)
        assert rt.hard_event.is_set()

    def test_rest_legacy_interrupt_is_graceful_while_a_tool_runs(self, client, rt, pa):
        rt.set("_turn_event_open", True)
        rt.set("_tool_inflight", True)
        response = client.post(
            "/api/interrupt", json={"session_identity_fingerprint": FP_A}
        )
        assert response.status_code == 200
        assert response.json()["mode"] == "graceful"
        assert _interrupt_state(pa) == ("graceful", 3)
        assert not rt.hard_event.is_set()

    def test_rest_legacy_interrupt_when_idle_returns_409(self, client, rt, pa):
        response = client.post(
            "/api/interrupt", json={"session_identity_fingerprint": FP_A}
        )
        assert response.status_code == 409
        assert response.json() == {
            "ack": False,
            "applied": False,
            "target_turn_id": 3,
            "error": "target turn is no longer active",
            "error_code": "target_turn_not_active",
        }
        assert _interrupt_state(pa) == (None, None)

    def test_rest_correlated_interrupt_echoes_identifiers(self, client, rt, pa):
        rt.set("_turn_event_open", True)
        response = client.post(
            "/api/interrupt",
            json={
                "session_identity_fingerprint": FP_A,
                "client_request_id": "client-1",
                "target_turn_id": 3,
                "request_id": "request-1",
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "client_request_id": "client-1",
            "target_turn_id": 3,
            "request_id": "request-1",
            "ack": True,
            "applied": True,
            "mode": "hard",
        }
        assert _interrupt_state(pa) == ("hard", 3)

    def test_rest_correlated_interrupt_without_request_id(self, client, rt):
        rt.set("_turn_event_open", True)
        response = client.post(
            "/api/interrupt",
            json={
                "session_identity_fingerprint": FP_A,
                "client_request_id": "client-1",
                "target_turn_id": 3,
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "client_request_id": "client-1",
            "target_turn_id": 3,
            "ack": True,
            "applied": True,
            "mode": "hard",
        }

    def test_rest_correlated_interrupt_for_stale_turn_returns_409(self, client, rt, pa):
        rt.set("_turn_event_open", True)
        response = client.post(
            "/api/interrupt",
            json={
                "session_identity_fingerprint": FP_A,
                "client_request_id": "client-1",
                "target_turn_id": 2,
                "request_id": "request-1",
            },
        )
        assert response.status_code == 409
        # Observed: the correlated refusal carries no "ack" key.
        assert response.json() == {
            "client_request_id": "client-1",
            "target_turn_id": 2,
            "request_id": "request-1",
            "applied": False,
            "error": "target turn is no longer active",
            "error_code": "target_turn_not_active",
        }
        assert _interrupt_state(pa) == (None, None)

    @pytest.mark.parametrize(
        "payload,status,body",
        [
            pytest.param(
                b"{nope",
                400,
                {"error": "invalid JSON", "error_code": "invalid_request"},
                id="invalid-json",
            ),
            pytest.param(
                b"[1]",
                400,
                {
                    "error": "body must be a JSON object",
                    "error_code": "invalid_request",
                },
                id="non-object",
            ),
            pytest.param(b"", 409, MISMATCH_BODY, id="empty-body"),
            pytest.param({}, 409, MISMATCH_BODY, id="no-fingerprint"),
            pytest.param(
                {"client_request_id": "client-1", "target_turn_id": 3},
                409,
                MISMATCH_BODY,
                id="fingerprint-checked-before-fields",
            ),
            pytest.param(
                {"session_identity_fingerprint": FP_B},
                409,
                MISMATCH_BODY,
                id="stale-fingerprint",
            ),
            pytest.param(
                {"session_identity_fingerprint": FP_A, "target_turn_id": 3},
                400,
                {
                    "error": "client_request_id must be a non-empty string",
                    "error_code": "invalid_request",
                },
                id="missing-client-request-id",
            ),
            pytest.param(
                {
                    "session_identity_fingerprint": FP_A,
                    "client_request_id": "",
                    "target_turn_id": 3,
                },
                400,
                {
                    "error": "client_request_id must be a non-empty string",
                    "error_code": "invalid_request",
                },
                id="empty-client-request-id",
            ),
            *[
                pytest.param(
                    {
                        "session_identity_fingerprint": FP_A,
                        "client_request_id": "client-1",
                        "target_turn_id": target,
                    },
                    400,
                    {
                        "error": "target_turn_id must be a positive integer",
                        "error_code": "invalid_request",
                    },
                    id=f"bad-target-{target_id}",
                )
                for target_id, target in [
                    ("bool", True),
                    ("zero", 0),
                    ("string", "3"),
                    ("missing", None),
                ]
            ],
            *[
                pytest.param(
                    {
                        "session_identity_fingerprint": FP_A,
                        "client_request_id": "client-1",
                        "target_turn_id": 3,
                        "request_id": request_id,
                    },
                    400,
                    {
                        "error": "request_id must be a non-empty string",
                        "error_code": "invalid_request",
                    },
                    id=f"bad-request-id-{request_id_id}",
                )
                for request_id_id, request_id in [("empty", ""), ("int", 5)]
            ],
        ],
    )
    def test_rest_invalid_bodies(self, client, rt, pa, payload, status, body):
        rt.set("_turn_event_open", True)
        if isinstance(payload, bytes):
            response = client.post(
                "/api/interrupt",
                content=payload,
                headers={"content-type": "application/json"},
            )
        else:
            response = client.post("/api/interrupt", json=payload)
        assert response.status_code == status
        assert response.json() == body
        assert _interrupt_state(pa) == (None, None)
        assert not rt.hard_event.is_set()

    def test_ws_interrupt_active_turn(self, client, ws_path, rt, pa):
        rt.set("_turn_event_open", True)
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "interrupt"})
            assert _recv(ws) == {
                "method": "interrupt.ack",
                "params": {"applied": True, "target_turn_id": 3, "mode": "hard"},
            }
        assert _interrupt_state(pa) == ("hard", 3)
        assert rt.hard_event.is_set()

    def test_ws_interrupt_graceful_while_a_tool_runs(self, client, ws_path, rt, pa):
        rt.set("_turn_event_open", True)
        rt.set("_tool_inflight", True)
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "interrupt"})
            assert _recv(ws)["params"] == {
                "applied": True,
                "target_turn_id": 3,
                "mode": "graceful",
            }
        assert not rt.hard_event.is_set()

    def test_ws_interrupt_idle(self, client, ws_path, rt, pa):
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "interrupt"})
            assert _recv(ws) == {
                "method": "interrupt.ack",
                "params": {
                    "applied": False,
                    "target_turn_id": 3,
                    "error_code": "target_turn_not_active",
                },
            }
        assert _interrupt_state(pa) == (None, None)

    def test_ws_interrupt_before_first_turn(self, client, ws_path, rt, pa):
        rt.session.turn_count = 0
        rt.set("_turn_event_open", True)
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "interrupt"})
            assert _recv(ws) == {
                "method": "interrupt.ack",
                "params": {
                    "applied": False,
                    "target_turn_id": None,
                    "error_code": "target_turn_not_active",
                },
            }
        assert _interrupt_state(pa) == (None, None)


# ---------------------------------------------------------------------------
# 9. Approve
# ---------------------------------------------------------------------------


class TestApprove:
    @pytest.mark.parametrize(
        "decision,resolved_decision", [("approve", "approved"), ("deny", "denied")]
    )
    def test_rest_resolves_the_gate(self, client, rt, decision, resolved_decision):
        approval_uuid = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        rt.resolve.return_value = {"id": approval_uuid, "tool_call_id": "call-1"}
        # Observed: approve carries no session identity fingerprint.
        response = client.post(
            "/api/approve",
            json={"decision": decision, "approval_id": "approval-1"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "accepted": True,
            "decision": decision,
            "approval_id": str(approval_uuid),
            "tool_call_id": "call-1",
        }
        assert rt.bound("_resolve_pending_permission", rt.resolve.call_args) == {
            "decision": resolved_decision,
            "approval_id": "approval-1",
            "decided_by": "rest_client",
        }

    @pytest.mark.parametrize(
        "body,approval_id",
        [
            ({"decision": "approve", "approval_id": "approval-1"}, "approval-1"),
            ({"decision": "approve"}, None),
        ],
        ids=["explicit-id", "most-recent-pending"],
    )
    def test_rest_without_matching_gate_returns_404(
        self, client, rt, body, approval_id
    ):
        response = client.post("/api/approve", json=body)
        assert response.status_code == 404
        assert response.json() == {
            "error": "No matching pending request",
            "approval_id": approval_id,
        }
        assert rt.bound("_resolve_pending_permission", rt.resolve.call_args) == {
            "decision": "approved",
            "approval_id": approval_id,
            "decided_by": "rest_client",
        }

    @pytest.mark.parametrize("decision", ["approved", "", None, "APPROVE"])
    def test_rest_bad_decision_returns_400(self, client, rt, decision):
        response = client.post("/api/approve", json={"decision": decision})
        assert response.status_code == 400
        assert response.json() == {"error": "decision must be 'approve' or 'deny'"}
        rt.resolve.assert_not_called()

    def test_rest_invalid_json_returns_400(self, client, rt):
        response = client.post(
            "/api/approve",
            content=b"{nope",
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 400
        assert response.json() == {"error": "invalid JSON"}
        rt.resolve.assert_not_called()

    @pytest.mark.parametrize(
        "method,resolved_decision", [("approve", "approved"), ("deny", "denied")]
    )
    @pytest.mark.parametrize("approval_id", ["approval-1", None])
    def test_ws_resolves_the_gate_without_a_reply(
        self, client, ws_path, rt, method, resolved_decision, approval_id
    ):
        rt.resolve.return_value = {"id": "approval-1", "tool_call_id": "call-1"}
        frame = {"method": method}
        if approval_id is not None:
            frame["approval_id"] = approval_id
        with _session_ws(client, ws_path) as ws:
            ws.send_json(frame)
            _barrier(ws)
            rt.resolve.assert_awaited_once()
        assert rt.bound("_resolve_pending_permission", rt.resolve.call_args) == {
            "decision": resolved_decision,
            "approval_id": approval_id,
            "decided_by": "ws_client",
        }


# ---------------------------------------------------------------------------
# 10. WebSocket verbs, commands and Canvas controls
# ---------------------------------------------------------------------------


COMMANDS = [
    pytest.param(
        {
            "method": "config.update",
            "config": {"llm": {"model": "model-y"}},
            "datasource_ids": ["ds-1"],
            "request_id": "req-1",
        },
        "_handle_config_update",
        {
            "config_override": {"llm": {"model": "model-y"}},
            "datasource_ids": ["ds-1"],
            "request_id": "req-1",
        },
        id="config.update",
    ),
    pytest.param(
        {"method": "config.update", "datasource_ids": []},
        "_handle_config_update",
        {"config_override": {}, "datasource_ids": [], "request_id": None},
        id="config.update-datasources-only",
    ),
    pytest.param(
        {"method": "compact", "focus": "tests", "boundary_message_id": "msg-9"},
        "_handle_compact",
        {"focus": "tests", "boundary_message_id": "msg-9"},
        id="compact",
    ),
    pytest.param(
        {"method": "compact"},
        "_handle_compact",
        {"focus": "", "boundary_message_id": None},
        id="compact-defaults",
    ),
    pytest.param({"method": "archive"}, "_handle_archive", {}, id="archive"),
    pytest.param(
        {"method": "upgrade-to-vm"}, "_handle_vm_upgrade", {}, id="upgrade-to-vm"
    ),
    pytest.param(
        {"method": "upgrade-to-workspace", "target_tier": "vm"},
        "_handle_workspace_upgrade",
        {"target_tier": "vm"},
        id="upgrade-to-workspace",
    ),
    pytest.param(
        {"method": "upgrade-to-workspace"},
        "_handle_workspace_upgrade",
        {"target_tier": "sandbox"},
        id="upgrade-to-workspace-default",
    ),
    pytest.param(
        {"method": "rewind", "message_id": "msg-3", "request_id": "req-2"},
        "_handle_rewind",
        {"data": {"method": "rewind", "message_id": "msg-3", "request_id": "req-2"}},
        id="rewind",
    ),
]
COMMAND_HANDLERS = [
    "_handle_config_update",
    "_handle_compact",
    "_handle_archive",
    "_handle_vm_upgrade",
    "_handle_workspace_upgrade",
    "_handle_rewind",
]


class TestWebSocketVerbs:
    @pytest.mark.parametrize("method", ["mode.set", "narration.set"])
    def test_retired_control_verbs(self, client, ws_path, rt, method):
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": method, "mode": "auto"})
            assert _recv(ws) == CONTROL_RETIRED_FRAME

    def test_unknown_method(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "x"})
            assert _recv(ws) == {
                "method": "error",
                "params": {"message": "Unknown method: x"},
            }

    def test_plain_text_is_a_message(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            ws.send_text("hello there")
            _barrier(ws)
        assert rt.bound("_accept_user_input", rt.accept.call_args) == {
            "content": "hello there",
            "role": "human",
            "delivery_id": None,
            "expected_session_identity_fingerprint": FP_A,
        }

    def test_json_without_method_is_a_message(self, client, ws_path, rt):
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"content": "implicit"})
            _barrier(ws)
        assert rt.bound("_accept_user_input", rt.accept.call_args)["content"] == (
            "implicit"
        )

    def test_non_object_json_text_ends_the_handler_without_close(
        self, client, ws_path, rt, pa
    ):
        # Observed (likely a bug): "42" parses as JSON but not as an object; the
        # handler's catch-all ends it and unsubscribes, but no close frame is sent.
        with _session_ws(client, ws_path) as ws:
            ws.send_text("42")
            with pytest.raises(TimeoutError):
                _recv(ws, timeout=0.2)
            assert pa._subscribers == {}
        rt.accept.assert_not_called()

    @pytest.mark.parametrize("frame,handler_name,expected", COMMANDS)
    def test_commands_dispatch_to_runtime_handlers(
        self, client, ws_path, rt, frame, handler_name, expected
    ):
        handlers = {
            name: rt.patch(name, AsyncMock(name=name)) for name in COMMAND_HANDLERS
        }
        with _session_ws(client, ws_path) as ws:
            ws.send_json(frame)
            _barrier(ws)
            handler = handlers[handler_name]
            handler.assert_awaited_once()
        arguments = rt.bound(handler_name, handler.call_args)
        assert isinstance(arguments.pop("ws"), WebSocket)
        assert arguments == expected
        for name, other in handlers.items():
            if name != handler_name:
                other.assert_not_called()

    def test_config_update_without_changes_is_ignored(self, client, ws_path, rt):
        handler = rt.patch("_handle_config_update", AsyncMock())
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "config.update", "config": {}})
            ws.send_json({"method": "config.update"})
            _barrier(ws)
        handler.assert_not_called()

    def test_undo_requires_rest_while_a_shell_owner_exists(self, client, ws_path, rt):
        rt.session.shell_owner_token = "owner-token"
        rt.session.undo_turn = AsyncMock()
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "undo", "turn_id": 2})
            assert _recv(ws) == {
                "method": "error",
                "params": {
                    "code": "control_transport_required",
                    "message": "Use the session control REST endpoint",
                },
            }
        rt.session.undo_turn.assert_not_called()

    def test_undo_restores_files(self, client, ws_path, rt):
        restored = {
            "paths": ["a.txt"],
            "restored_to_sha": "sha-old",
            "restore_commit_sha": "sha-new",
        }
        rt.session.undo_turn = AsyncMock(return_value=restored)
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "undo", "turn_id": 2})
            assert _recv(ws) == {
                "method": "files.restored",
                "params": {**restored, "turn_id": 2},
            }
        rt.session.undo_turn.assert_awaited_once_with(2)

    @pytest.mark.parametrize(
        "error,params",
        [
            (
                WorkspaceUndoUnavailable("no_history", "Nothing to undo"),
                {"code": "no_history", "message": "Nothing to undo"},
            ),
            (
                WorkspaceUndoRetryable("workspace busy"),
                {"code": "workspace_undo_retryable", "message": "workspace busy"},
            ),
        ],
        ids=["unavailable", "retryable"],
    )
    def test_undo_failures(self, client, ws_path, rt, error, params):
        rt.session.undo_turn = AsyncMock(side_effect=error)
        with _session_ws(client, ws_path) as ws:
            ws.send_json({"method": "undo", "turn_id": 2})
            assert _recv(ws) == {"method": "error", "params": params}
            _barrier(ws)


SOURCE_VERSION = "sha256:" + "a" * 64
CANVAS_PATH = "notes/plan.md"
CANVAS_STATE = {
    "presentation_revision": 4,
    "source": {"type": "workspace_file", "path": CANVAS_PATH},
    "source_version": SOURCE_VERSION,
    "updated_at": "2026-09-25T00:00:00Z",
}
EDITING = {
    "method": "canvas.user_editing",
    "canvas_id": "main",
    "path": CANVAS_PATH,
    "presentation_revision": 4,
    "source_version": SOURCE_VERSION,
    "editing_session_id": "editing-session-1",
}


class TestCanvasControl:
    @pytest.fixture
    def canvas(self, rt):
        rt.canvas_state = rt.patch(
            "_current_canvas_for_control", AsyncMock(return_value=dict(CANVAS_STATE))
        )
        rt.broadcast = rt.patch("_broadcast", MagicMock(name="_broadcast"))
        return rt

    @pytest.mark.parametrize(
        "frame,message",
        [
            (
                {**EDITING, "extra": 1},
                "Canvas control message is invalid",
            ),
            (
                {**EDITING, "editing_session_id": "bad id!"},
                "Canvas editing session is invalid",
            ),
            (
                {**EDITING, "presentation_revision": 0},
                "Canvas control message is invalid",
            ),
            (
                {**EDITING, "source_version": "sha1:abc"},
                "Canvas control message is invalid",
            ),
        ],
        ids=["extra-field", "bad-editing-session", "bad-revision", "bad-version"],
    )
    def test_invalid_frames(self, client, ws_path, canvas, frame, message):
        with _session_ws(client, ws_path) as ws:
            ws.send_json(frame)
            assert _recv(ws) == {
                "method": "error",
                "params": {"code": "invalid_canvas_control", "message": message},
            }
        canvas.canvas_state.assert_not_called()

    def test_presentation_update_broadcasts_once_per_revision(
        self, client, ws_path, canvas
    ):
        frame = {
            "method": "canvas.presentation_updated",
            "canvas_id": "main",
            "presentation_revision": 4,
        }
        with _session_ws(client, ws_path) as ws:
            ws.send_json(frame)
            ws.send_json(frame)
            _barrier(ws)
        canvas.broadcast.assert_called_once_with(
            "canvas.updated",
            {
                "canvas_id": "main",
                "presentation_revision": 4,
                "source_type": "workspace_file",
                "updated_at": "2026-09-25T00:00:00Z",
            },
        )
        canvas.canvas_state.assert_awaited_once()

    def test_source_update_invalidates_read_and_broadcasts(
        self, client, ws_path, canvas
    ):
        canvas.session.tool_context = MagicMock(name="tool-context")
        frame = {k: v for k, v in EDITING.items() if k != "editing_session_id"}
        frame["method"] = "canvas.source_updated"
        with _session_ws(client, ws_path) as ws:
            ws.send_json(frame)
            _barrier(ws)
        canvas.session.tool_context.invalidate_recent_read.assert_called_once_with(
            CANVAS_PATH
        )
        canvas.broadcast.assert_called_once_with(
            "canvas.source_updated",
            {
                "canvas_id": "main",
                "presentation_revision": 4,
                "source_type": "workspace_file",
                "updated_at": "2026-09-25T00:00:00Z",
            },
        )

    def test_stale_state_is_refused(self, client, ws_path, canvas):
        canvas.canvas_state.return_value = {**CANVAS_STATE, "presentation_revision": 5}
        with _session_ws(client, ws_path) as ws:
            ws.send_json(EDITING)
            assert _recv(ws) == {
                "method": "error",
                "params": {
                    "code": "canvas_control_stale",
                    "message": "Canvas state changed; reload before continuing",
                },
            }
        canvas.broadcast.assert_not_called()

    def test_unreadable_state_is_reported(self, client, ws_path, canvas):
        canvas.canvas_state.side_effect = RuntimeError("orchestrator down")
        with _session_ws(client, ws_path) as ws:
            ws.send_json(EDITING)
            assert _recv(ws) == {
                "method": "error",
                "params": {
                    "code": "canvas_control_unavailable",
                    "message": "Canvas state could not be validated",
                },
            }

    def test_identity_change_during_validation_drops_the_frame(
        self, client, ws_path, canvas, pa
    ):
        def _rotate():
            canvas.set("_session_runtime_generation", GEN_B)
            return dict(CANVAS_STATE)

        canvas.canvas_state.side_effect = _rotate
        with _session_ws(client, ws_path) as ws:
            ws.send_json(
                {
                    "method": "canvas.presentation_updated",
                    "canvas_id": "main",
                    "presentation_revision": 4,
                }
            )
            ws.send_json({"method": "__after__"})
            with pytest.raises(WebSocketDisconnect) as exc_info:
                _recv(ws)
        assert (exc_info.value.code, exc_info.value.reason) == (
            4403,
            "session identity changed",
        )
        canvas.broadcast.assert_not_called()

    def test_awareness_reaches_every_client_and_expires_on_disconnect(
        self, client, ws_path, canvas, pa
    ):
        with _session_ws(client, ws_path) as ws_b:
            b_id = next(iter(pa._subscribers))
            with _session_ws(client, ws_path) as ws_a:
                a_id = next(key for key in pa._subscribers if key != b_id)
                ws_a.send_json(EDITING)
                awareness = {
                    "canvas_id": "main",
                    "path": CANVAS_PATH,
                    "presentation_revision": 4,
                    "source_version": SOURCE_VERSION,
                    "editing_session_id": "editing-session-1",
                }
                editing_frame = {
                    "method": "canvas.user_editing",
                    "params": {**awareness, "sender_id": a_id, "ttl_ms": 15000},
                }
                assert _recv(ws_a) == editing_frame
                assert _recv(ws_b) == editing_frame
            assert _recv(ws_b) == {
                "method": "canvas.user_idle",
                "params": {**awareness, "sender_id": a_id},
            }
            assert set(pa._subscribers) == {b_id}

    def test_rapid_distinct_awareness_is_rate_limited(self, client, ws_path, canvas):
        with _session_ws(client, ws_path) as ws:
            ws.send_json(EDITING)
            assert _recv(ws)["method"] == "canvas.user_editing"
            ws.send_json({**EDITING, "editing_session_id": "editing-session-2"})
            assert _recv(ws) == {
                "method": "error",
                "params": {
                    "code": "canvas_control_rate_limited",
                    "message": "Canvas control messages are arriving too quickly",
                },
            }
        canvas.canvas_state.assert_awaited_once()


# ---------------------------------------------------------------------------
# 11-12. Subscribers: multi-client fan-out and first-subscriber effects
# ---------------------------------------------------------------------------


class TestSubscribers:
    def test_two_clients_fan_out_and_disconnect_independently(
        self, client, ws_path, rt, pa
    ):
        loop_task = MagicMock(name="persistent-loop")
        loop_task.done.return_value = False
        rt.set("_loop_task", loop_task)

        with _session_ws(client, ws_path) as ws_b:
            b_id = next(iter(pa._subscribers))
            b_queue = pa._subscribers[b_id]
            with _session_ws(client, ws_path) as ws_a:
                _barrier(ws_a)
                _barrier(ws_b)
                a_id = next(key for key in pa._subscribers if key != b_id)
                assert set(pa._subscribers) == {a_id, b_id}
                assert pa._subscribers[a_id] is not b_queue
                assert [
                    rt.bound("_ensure_persistent_loop_started", c)["client_id"]
                    for c in rt.loop_start.call_args_list
                ] == [b_id, a_id]

                first = {"method": "turn.started", "params": {"turn_id": 4}}
                client.portal.call(pa._fan_out_live_frame, first)
                assert _recv(ws_a) == first
                assert _recv(ws_b) == first

            assert set(pa._subscribers) == {b_id}
            assert pa._subscribers[b_id] is b_queue
            second = {"method": "token", "params": {"content": "still here"}}
            client.portal.call(pa._fan_out_live_frame, second)
            assert _recv(ws_b) == second
            _barrier(ws_b)

        assert pa._subscribers == {}
        loop_task.cancel.assert_not_called()

    def test_first_subscriber_schedules_one_active_revert(
        self, client, ws_path, rt, pa
    ):
        with _session_ws(client, ws_path) as ws_a:
            _barrier(ws_a)
            assert rt.thread_status.await_args_list == [call("active")]
            with _session_ws(client, ws_path) as ws_b:
                _barrier(ws_b)
                assert rt.thread_status.await_count == 1
        with _session_ws(client, ws_path) as ws_c:
            _barrier(ws_c)
        # The registry emptied in between, so the next attach is first again.
        assert rt.thread_status.await_args_list == [call("active"), call("active")]

    def test_no_active_revert_without_orchestrator_client(
        self, client, ws_path, rt, pa
    ):
        rt.set("_orchestrator_client", None)
        rt.set("_current_pinned_session_identity_fingerprint", lambda: FP_A)
        with _session_ws(client, ws_path) as ws:
            _barrier(ws)
            assert len(pa._subscribers) == 1
        rt.thread_status.assert_not_called()
