"""Lazy access to the installed MCP SDK, and the one way SRW opens a client.

The application lives in ``mcp_server`` and cannot shadow the SDK's ``mcp``
package. Python's import machinery owns module identity and synchronization.

:func:`open_client` is how the agent's MCP datasources and the orchestrator's
connector test reach a remote server. It speaks the 2026-07-28 protocol to a
server that does and falls back to the ``initialize`` handshake for one that
does not (``mode="auto"``).

Since SDK 2.x, an HTTP error the server did not phrase as JSON-RPC (a proxy's
503, a front's 401) no longer ends the connection: the SDK answers the call
with a stand-in error, which reads like the tool's own. :class:`_HttpRefusals`
keeps the earlier rule, which the managed-server design relies on. Once
connected, such a response ends the connection, so the caller reconnects
instead of reading a refusal as the tool's answer. While connecting, only a
401 does: a server that predates 2026-07-28 may refuse the new ``server/discover``
probe in any shape, and the SDK falls back on it.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from importlib import import_module
from types import ModuleType
from typing import Any

#: Transports a remote server is reached over.
REMOTE_TRANSPORTS = ("http", "sse")


def ensure_mcp_sdk() -> ModuleType:
    """Return the canonical third-party MCP package."""
    return import_module("mcp")


class _HttpRefusals:
    """httpx2 response hook: an HTTP refusal of a POST ends the connection.

    A 404 is left to the SDK, which reads it as a terminated session (or,
    before a session exists, as an unknown method). A JSON-RPC error in the
    body is the server's answer and passes through.
    """

    def __init__(self) -> None:
        #: Set once the connection is negotiated.
        self.connected = False

    async def __call__(self, response: Any) -> None:
        status = response.status_code
        if status < 400 or status == 404 or response.request.method != "POST":
            return
        if status != 401:
            if not self.connected:
                return
            if await _is_json_rpc_error(response):
                return
        response.raise_for_status()


async def _is_json_rpc_error(response: Any) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    if not content_type.startswith("application/json"):
        return False
    import json

    try:
        body = json.loads(await response.aread())
    except ValueError:
        return False
    return isinstance(body, dict) and "jsonrpc" in body and "error" in body


def _http_client_factory(refusals: _HttpRefusals):
    """``create_mcp_http_client`` with the refusal hook installed."""

    def factory(headers=None, timeout=None, auth=None):
        from mcp.shared._httpx_utils import create_mcp_http_client

        client = create_mcp_http_client(headers=headers, timeout=timeout, auth=auth)
        client.event_hooks["response"].append(refusals)
        return client

    return factory


async def open_client(
    stack: AsyncExitStack,
    *,
    url: str,
    transport: str = "http",
    headers: dict[str, str] | None = None,
    elicitation_callback: Any = None,
) -> Any:
    """Connect an ``mcp.Client`` to a remote server, entered on ``stack``.

    The connection lives until ``stack`` closes. ``headers`` (the server's
    credential) go on every request. An HTTP refusal ends the connection
    with an ``httpx2.HTTPStatusError`` somewhere in the exception (group)
    the stack raises; see the module docstring.
    """
    ensure_mcp_sdk()
    from mcp import Client

    if transport not in REMOTE_TRANSPORTS:
        raise ValueError(f"unknown transport (expected one of {REMOTE_TRANSPORTS})")
    refusals = _HttpRefusals()
    factory = _http_client_factory(refusals)
    if transport == "sse":
        from mcp.client.sse import sse_client

        connection = sse_client(
            url, headers=headers or None, httpx_client_factory=factory
        )
    else:
        from mcp.client.streamable_http import streamable_http_client

        http_client = await stack.enter_async_context(factory(headers=headers or None))
        connection = streamable_http_client(url, http_client=http_client)
    client = await stack.enter_async_context(
        Client(connection, mode="auto", elicitation_callback=elicitation_callback)
    )
    refusals.connected = True
    return client


def http_status(exc: BaseException) -> int | None:
    """The HTTP status of the first ``httpx2.HTTPStatusError`` in ``exc``,
    its exception groups and its causes."""
    try:
        import httpx2
    except ImportError:  # pragma: no cover - the SDK depends on it
        return None
    for current in walk_exception(exc):
        if isinstance(current, httpx2.HTTPStatusError):
            return current.response.status_code
    return None


def walk_exception(exc: BaseException):
    """``exc``, the members of its exception groups and its causes, each once."""
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        grouped = getattr(current, "exceptions", None)
        if isinstance(grouped, (list, tuple)):
            pending.extend(e for e in grouped if isinstance(e, BaseException))
        pending.extend((current.__cause__, current.__context__))
