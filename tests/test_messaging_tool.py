"""Tests for the ``send_message`` communication tool.

When the agent is running inside a persistent session, ``ToolContext``
carries the originating user's id. The orchestrator's messages-send
endpoint should accept these calls via ``_get_user_from_mcp_headers``
(``X-Internal-Key`` + ``X-MCP-User-Id``) so the same identity model
applies as for the rest of the agent's orchestrator surface. Without
``X-MCP-User-Id`` the call only authenticates as ``require_internal``,
which keeps working today but loses the per-user audit trail and breaks
the dual-callable paths that branch on ``user`` vs. internal.
"""

from __future__ import annotations
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from agent.tools.communication.messaging import create_communication_tools  # noqa: E402
from agent.tools.context import ToolContext  # noqa: E402


def _tool_by_name(tools, name):
    for t in tools:
        if t.name == name:
            return t
    raise AssertionError(f"tool {name!r} not in {[t.name for t in tools]}")


class _CapturingAsyncClient:
    """Captures the constructor headers and the first POST call.

    httpx.AsyncClient is normally used as ``async with`` — this stand-in
    mimics that contract.
    """

    last_init_headers: dict[str, str] | None = None
    last_post_json: dict | None = None
    post_payloads: list[dict] = []

    def __init__(self, *args, **kwargs):
        type(self).last_init_headers = dict(kwargs.get("headers") or {})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, **kwargs):
        type(self).last_post_json = dict(json or {})
        type(self).post_payloads.append(dict(json or {}))
        resp = MagicMock()
        resp.status_code = 200
        resp.json = MagicMock(
            return_value={
                "thread_id": json.get("thread_id") if json else "t",
                "status": "delivered",
            }
        )
        resp.text = ""
        return resp


@pytest.mark.asyncio
async def test_send_message_forwards_user_id_from_context(monkeypatch):
    """A persistent-session context (user_id set) must produce a POST
    whose headers carry X-MCP-User-Id alongside X-Internal-Key."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    _CapturingAsyncClient.last_init_headers = None
    monkeypatch.setattr(
        "agent.tools.communication.messaging.httpx.AsyncClient",
        _CapturingAsyncClient,
    )

    ctx = ToolContext(_job_id="job-1", user_id="user-xyz")
    tools = create_communication_tools(ctx)
    send = _tool_by_name(tools, "send_message")

    result = await send.ainvoke(
        {"to": "user", "subject": "hi", "message": "hello there", "mode": "async"}
    )

    # The tool may still report partial success; we only care about headers.
    assert isinstance(result, str)
    headers = _CapturingAsyncClient.last_init_headers
    assert headers is not None, "httpx.AsyncClient was never constructed"
    assert headers.get("X-Internal-Key") == "test-internal-key"
    assert headers.get("X-MCP-User-Id") == "user-xyz"


@pytest.mark.asyncio
async def test_send_message_omits_user_id_header_when_context_lacks_user(monkeypatch):
    """Worker-mode (no user_id) keeps the old behavior — X-Internal-Key
    only. The orchestrator's require_internal gate on messages-send is
    happy with that."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    _CapturingAsyncClient.last_init_headers = None
    monkeypatch.setattr(
        "agent.tools.communication.messaging.httpx.AsyncClient",
        _CapturingAsyncClient,
    )

    ctx = ToolContext(_job_id="job-1")
    tools = create_communication_tools(ctx)
    send = _tool_by_name(tools, "send_message")

    await send.ainvoke(
        {"to": "user", "subject": "hi", "message": "hello there", "mode": "async"}
    )

    headers = _CapturingAsyncClient.last_init_headers
    assert headers is not None
    assert headers.get("X-Internal-Key") == "test-internal-key"
    assert "X-MCP-User-Id" not in headers


@pytest.mark.asyncio
async def test_blocking_stateless_message_carries_exact_worker_token(monkeypatch):
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    _CapturingAsyncClient.last_post_json = None
    monkeypatch.setattr(
        "agent.tools.communication.messaging.httpx.AsyncClient",
        _CapturingAsyncClient,
    )

    ctx = ToolContext(
        _job_id="job-1",
        _stateless_worker=True,
        _worker_lease_token=17,
    )
    send = _tool_by_name(create_communication_tools(ctx), "send_message")

    await send.ainvoke(
        {"to": "user", "subject": "need input", "message": "reply", "mode": "blocking"}
    )

    assert _CapturingAsyncClient.last_post_json is not None
    assert _CapturingAsyncClient.last_post_json["lease_token"] == 17


@pytest.mark.asyncio
async def test_transport_retry_reuses_the_same_hidden_routing_generation(monkeypatch):
    class _RetryClient(_CapturingAsyncClient):
        calls = 0

        async def post(self, url, json=None, **kwargs):
            type(self).calls += 1
            type(self).post_payloads.append(dict(json or {}))
            if type(self).calls == 1:
                import httpx

                raise httpx.ReadError("response lost")
            return await super().post(url, json=json, **kwargs)

    _RetryClient.calls = 0
    _RetryClient.post_payloads = []
    monkeypatch.setattr(
        "agent.tools.communication.messaging.httpx.AsyncClient", _RetryClient
    )
    send = _tool_by_name(
        create_communication_tools(ToolContext(_job_id="job-1")), "send_message"
    )
    result = await send.ainvoke(
        {"to": "user", "subject": "hi", "message": "hello", "mode": "async"}
    )
    assert "Message sent" in result
    assert _RetryClient.calls == 2
    assert len(_RetryClient.post_payloads) >= 2
    generations = {
        payload["routing_generation"] for payload in _RetryClient.post_payloads
    }
    assert len(generations) == 1


@pytest.mark.asyncio
async def test_pinned_delivery_proof_rides_the_httpx_post(monkeypatch):
    """A pinned worker's delivery proof is read off the orchestrator client,
    but the send itself must still go through the header-carrying httpx
    client: the orchestrator client has no ``post``, so rebinding to it
    turned every pinned send_message into an AttributeError."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    monkeypatch.setenv("POD_UID", "pod-uid-1")
    _CapturingAsyncClient.last_init_headers = None
    _CapturingAsyncClient.last_post_json = None
    monkeypatch.setattr(
        "agent.tools.communication.messaging.httpx.AsyncClient",
        _CapturingAsyncClient,
    )
    pinned = SimpleNamespace(
        agent_id="agent-1",
        pinned_delivery_job_id="job-1",
        pinned_delivery_id="delivery-1",
        pinned_projection_digest="digest-1",
        pinned_delivery_proof="proof-1",
        dispatch_process_generation="generation-1",
    )
    send = _tool_by_name(
        create_communication_tools(
            ToolContext(_job_id="job-1", orchestrator_client=pinned)
        ),
        "send_message",
    )

    result = await send.ainvoke(
        {"to": "user", "subject": "hi", "message": "hello", "mode": "async"}
    )

    assert "Message sent" in result
    assert _CapturingAsyncClient.last_init_headers == {
        "X-Internal-Key": "test-internal-key"
    }
    payload = _CapturingAsyncClient.last_post_json
    assert payload["agent_id"] == "agent-1"
    assert {key: payload[key] for key in payload if key.startswith("pinned_")} == {
        "pinned_delivery_id": "delivery-1",
        "pinned_projection_digest": "digest-1",
        "pinned_delivery_proof": "proof-1",
        "pinned_process_generation": "generation-1",
        "pinned_pod_uid": "pod-uid-1",
    }
