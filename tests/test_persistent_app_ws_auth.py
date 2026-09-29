"""Tests for JWT validation on the agent pod's /ws/chat handshake."""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from unittest.mock import MagicMock

from orchestrator.services.session_tokens import SessionTokenService
from shared.pinned_session_identity import (
    pinned_session_ready_identity_fingerprint,
)


THREAD_ID = "11111111-1111-4111-8111-111111111111"
RUNTIME_GENERATION = "22222222-2222-4222-8222-222222222222"
AGENT_ID = "33333333-3333-4333-8333-333333333333"
ATTACH_TOKEN = "44444444-4444-4444-8444-444444444444"
POD_UID = "55555555-5555-4555-8555-555555555555"


def _fingerprint(*, generation: str = RUNTIME_GENERATION) -> str:
    value = pinned_session_ready_identity_fingerprint(
        thread_id=THREAD_ID,
        runtime_generation=generation,
        agent_id=AGENT_ID,
        runtime_attach_token=ATTACH_TOKEN,
        pod_uid=POD_UID,
    )
    assert value is not None
    return value


@pytest.fixture(autouse=True)
def configure_pod_env(monkeypatch):
    monkeypatch.setenv("SESSION_JWT_SECRET", "test-pod-secret-do-not-use")
    monkeypatch.setenv("SESSION_BOUND_THREAD_ID", THREAD_ID)
    monkeypatch.setenv("POD_UID", POD_UID)

    import agent.api.persistent_app as pa

    monkeypatch.setattr(pa._session_identity, "_thread_id", THREAD_ID)
    monkeypatch.setattr(pa._session_identity, "_session_generation", RUNTIME_GENERATION)
    monkeypatch.setattr(pa._session_identity, "_attach_token", ATTACH_TOKEN)
    monkeypatch.setattr(
        pa,
        "_orchestrator_client",
        MagicMock(agent_id=AGENT_ID),
    )


@pytest.fixture
def app():
    from agent.api.persistent_app import create_persistent_app

    # config_path is required; we never actually run the lifespan / session,
    # but the constructor needs a value. A dummy string is fine — the route
    # under test (the validator gate on /ws/chat) doesn't touch it.
    return create_persistent_app("dummy_config", THREAD_ID)


def _connect_and_capture_close(client: TestClient, url: str) -> int:
    """Open the WS, drain frames, and return the close code.

    Starlette's TestClient does not surface a server-side close as an
    exception during ``websocket_connect()`` — the server-accepted upgrade
    completes from the client's POV. The close shows up on the next
    ``receive_*`` as ``WebSocketDisconnect``. We rely on that to capture
    the code the validator emitted.
    """
    with client.websocket_connect(url) as ws:
        try:
            ws.receive_text()
        except WebSocketDisconnect as exc:
            return exc.code
    return -1  # No disconnect observed — test should fail.


def test_ws_chat_rejects_missing_token(app):
    """No `?t=` query param → close with 4401."""
    client = TestClient(app)
    code = _connect_and_capture_close(client, "/ws/chat")
    assert code == 4401


def test_ws_chat_rejects_token_for_other_thread(app):
    """Token's `tid` doesn't match pod's bound thread → close with 4403."""
    other_token, _ = SessionTokenService("test-pod-secret-do-not-use").mint(
        "u1",
        "66666666-6666-4666-8666-666666666666",
        session_identity_fingerprint=_fingerprint(),
    )
    client = TestClient(app)
    code = _connect_and_capture_close(client, f"/ws/chat?t={other_token}")
    assert code == 4403


def test_ws_chat_rejects_invalid_signature(app):
    """Token signed with a different secret → close with 4401."""
    bad_token, _ = SessionTokenService("WRONG-SECRET").mint(
        "u1",
        THREAD_ID,
        session_identity_fingerprint=_fingerprint(),
    )
    client = TestClient(app)
    code = _connect_and_capture_close(client, f"/ws/chat?t={bad_token}")
    assert code == 4401


def test_ws_chat_rejects_when_pod_misconfigured(app, monkeypatch):
    """Missing SESSION_JWT_SECRET → close with 4500 (fail-closed)."""
    monkeypatch.delenv("SESSION_JWT_SECRET", raising=False)
    client = TestClient(app)
    code = _connect_and_capture_close(client, "/ws/chat?t=anything")
    assert code == 4500


def test_ws_chat_accepts_valid_token(app, monkeypatch):
    """Token with matching exact identity passes the validator gate.

    The session itself isn't started so the session socket transport will
    drop the connection shortly after — but the close code must come from
    the downstream handler, not from the validator. The validator's codes
    are 4401 (auth), 4403 (mismatch), and 4500 (misconfig). Anything else
    means the validator allowed the request through.
    """
    from agent.api import session_websocket

    async def _accepted_downstream(ws, _ports):
        await ws.accept()
        await ws.close(code=4000, reason="validator passed")

    # The real downstream handler also uses 4403 when no live session object
    # exists. Stub it so this test identifies the validator boundary rather
    # than conflating two independent exact-identity checks.
    monkeypatch.setattr(
        session_websocket, "serve_session_websocket", _accepted_downstream
    )

    token, _ = SessionTokenService("test-pod-secret-do-not-use").mint(
        "u1",
        THREAD_ID,
        session_identity_fingerprint=_fingerprint(),
    )
    client = TestClient(app)
    code = _connect_and_capture_close(client, f"/ws/chat?t={token}")
    assert code == 4000


def test_ws_chat_rejects_predecessor_generation_token(app):
    predecessor_generation = "77777777-7777-4777-8777-777777777777"
    token, _ = SessionTokenService("test-pod-secret-do-not-use").mint(
        "u1",
        THREAD_ID,
        session_identity_fingerprint=_fingerprint(generation=predecessor_generation),
    )

    code = _connect_and_capture_close(TestClient(app), f"/ws/chat?t={token}")

    assert code == 4403
