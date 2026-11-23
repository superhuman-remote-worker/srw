"""``shared.mcp_sdk``: the HTTP refusals that end an MCP connection.

Since SDK 2.x an HTTP error the server did not phrase as JSON-RPC answers the
call with a stand-in error instead of ending the connection. The hook keeps
the earlier rule: once connected, such a refusal of a POST ends the
connection, so the caller reconnects rather than reading it as the tool's
answer. While connecting, only a 401 does, since a server from before
2026-07-28 may refuse the ``server/discover`` probe in any shape.
"""

from __future__ import annotations

import builtins

import httpx2
import pytest

from shared.mcp_sdk import _HttpRefusals, http_status, walk_exception

URL = "https://mcp.example/mcp"


def _response(status: int, method: str = "POST", **body) -> httpx2.Response:
    return httpx2.Response(status, request=httpx2.Request(method, URL), **body)


async def _refused(hook: _HttpRefusals, response: httpx2.Response) -> int | None:
    try:
        await hook(response)
    except httpx2.HTTPStatusError as refused:
        return refused.response.status_code
    return None


@pytest.mark.asyncio
async def test_while_connecting_only_a_401_ends_the_connection():
    hook = _HttpRefusals()
    assert await _refused(hook, _response(401, text="lease_required")) == 401
    for status in (400, 405, 500, 503):
        assert await _refused(hook, _response(status, text="no")) is None


@pytest.mark.asyncio
async def test_once_connected_an_http_refusal_ends_the_connection():
    hook = _HttpRefusals()
    hook.connected = True
    for status in (401, 403, 429, 502, 503):
        assert await _refused(hook, _response(status, json={"error": "x"})) == status
    # Answers: a success, a terminated session (the SDK's own case), and a
    # JSON-RPC error the server sent with an HTTP error status.
    assert await _refused(hook, _response(200, json={})) is None
    assert await _refused(hook, _response(404, text="gone")) is None
    rpc_error = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "no"}}
    assert await _refused(hook, _response(400, json=rpc_error)) is None
    # Only a POST carries a call: the server-sent stream's GET is the SDK's.
    assert await _refused(hook, _response(405, method="GET", text="no")) is None


def test_the_status_is_found_wherever_the_transport_put_it():
    refused = httpx2.HTTPStatusError(
        "refused", request=httpx2.Request("POST", URL), response=_response(401)
    )
    group = builtins.BaseExceptionGroup("transport", [RuntimeError("x"), refused])
    try:
        raise ConnectionError("ended") from group
    except ConnectionError as wrapped:
        assert http_status(wrapped) == 401
        assert refused in list(walk_exception(wrapped))
    assert http_status(ConnectionError("gone")) is None


@pytest.mark.parametrize(
    ("token", "header"),
    [
        ("abc123", "Bearer abc123"),
        ("Bearer abc123", "Bearer abc123"),
        ("  bearer   abc123 ", "Bearer abc123"),
        ("BEARER abc123", "Bearer abc123"),
        ("Bearerabc123", "Bearer Bearerabc123"),
    ],
)
def test_a_pasted_bearer_scheme_is_sent_once(token, header):
    from shared.mcp_sdk import bearer_authorization

    assert bearer_authorization(token) == header


def test_the_agent_sends_a_pasted_bearer_token_once():
    from agent.tools.mcp.manager import parse_mcp_config

    config = parse_mcp_config(
        {
            "type": "mcp",
            "name": "Civitai",
            "connection_url": "https://orchestration.civitai.example/mcp/v2",
            "credentials": {
                "transport": "http",
                "auth": {"type": "bearer", "token": "Bearer key-1"},
            },
        }
    )
    assert config.headers == {"Authorization": "Bearer key-1"}
