"""MCP client lifecycle for user-attached MCP datasources.

One manager holds every MCP datasource for a job or session because
``ToolContext`` stores datasource connections by type. Each server is owned
by one asyncio task: that task enters and exits the MCP transport and session
contexts, satisfying anyio's cancel-scope ownership rule.

A **managed** MCP server (connector drivers D5a) runs in a pod SRW hosts
behind its front. Its entry carries the endpoint URL and a lease token,
which is the client's bearer; the upstream credential never reaches this
process. The pod may still be starting when a session binds it, so the
connect first waits for the front's ``/readyz`` (up to
:data:`MANAGED_MCP_START_TIMEOUT`), and a pod replaced mid-session (a lost
pod, a re-pin, a new credential generation) is reconnected transparently
within a budget (:data:`MANAGED_MCP_RECONNECTS` per
:data:`MANAGED_MCP_RECONNECT_WINDOW`) instead of the single reconnect a
remote server gets. When the front refuses the lease (it was revoked or
expired: a 401, or :data:`LEASE_ENDED_CODE` on a call in flight), the agent
is told so in plain words and nothing reconnects until the entry carries
another lease.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from agent.tools.mcp.naming import mcp_server_slug, namespace_mcp_tool
from shared.mcp_sdk import ensure_mcp_sdk

logger = logging.getLogger(__name__)

MCP_CONNECT_TIMEOUT = 10.0
MCP_CALL_TIMEOUT = 60.0
#: How long a managed server's pod may take to become ready (the service
#: pod start timeout's default): scheduling, the image pull, the start-up
#: wait and the server's own start.
MANAGED_MCP_START_TIMEOUT = 180.0
#: Connect, initialize and list tools once the pod is ready.
MANAGED_MCP_CONNECT_TIMEOUT = 30.0
#: Reconnects a managed server may use in a sliding window.
MANAGED_MCP_RECONNECTS = 3
MANAGED_MCP_RECONNECT_WINDOW = 600.0
_READY_POLL_SECONDS = (1.0, 2.0, 3.0, 5.0)
_TRANSPORTS = ("http", "sse")
#: JSON-RPC errors that mean the session is gone, not that a tool failed:
#: the transport's "Session terminated" (a 404 from the server) and a
#: closed connection.
_SESSION_ERROR_CODES = frozenset({32600, -32000})
#: The error a managed server's front gives a call in flight when its lease
#: ends (drivers/mcp-front, ``leaseEndedCode``), with this message prefix.
LEASE_ENDED_CODE = -32091
_LEASE_ENDED_PREFIX = "lease revoked"
#: What the agent is told when the front refuses this execution's lease.
LEASE_REFUSED = (
    "the connector's lease was revoked or has expired, so this execution no "
    "longer holds this connector"
)


@dataclass
class MCPServerConfig:
    """Validated connection parameters for one MCP datasource."""

    name: str
    transport: str
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    #: A managed server's front readiness URL (``None`` for any other).
    ready_url: str | None = None

    @property
    def managed(self) -> bool:
        return self.ready_url is not None


def _managed_spec(ds: dict[str, Any]) -> bool:
    from shared.connectors.builtin import spec_for_type
    from shared.connectors.contract import managed_mcp_driver

    spec = spec_for_type(ds.get("type"))
    return spec is not None and managed_mcp_driver(spec)


def _parse_managed(ds: dict[str, Any], name: str) -> MCPServerConfig:
    """A managed server: its endpoint and the lease token as the bearer."""
    from shared.connectors.leases import LEASE_TOKEN_PREFIX, token_shape_valid

    credentials = ds.get("credentials") or {}
    lease = credentials.get("lease") if isinstance(credentials, dict) else None
    token = lease.get("token") if isinstance(lease, dict) else None
    if not token_shape_valid(token, LEASE_TOKEN_PREFIX):
        raise ValueError("no credential lease was delivered for this server")
    url = ds.get("connection_url")
    parts = urlsplit(url) if isinstance(url, str) else None
    if parts is None or parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError("the managed server has no endpoint")
    return MCPServerConfig(
        name=name,
        transport="http",
        url=url,
        headers={"Authorization": f"Bearer {token}"},
        ready_url=f"{parts.scheme}://{parts.netloc}/readyz",
    )


def parse_mcp_config(ds: dict[str, Any]) -> MCPServerConfig:
    """Validate a raw datasource without including credential values in errors."""
    if _managed_spec(ds):
        return _parse_managed(ds, str(ds.get("name") or "unnamed"))
    credentials = ds.get("credentials") or {}
    if not isinstance(credentials, dict):
        raise ValueError("credentials must be an object")

    raw_transport = credentials.get("transport") or "http"
    if not isinstance(raw_transport, str):
        raise ValueError("transport must be a string")
    transport = raw_transport.lower().strip()
    if transport == "stdio":
        # Never a subprocess of the agent: an orchestrator older than this
        # agent may still send a stored stdio server.
        from shared.connectors.builtin import MCP_STDIO_RETIRED

        raise ValueError(MCP_STDIO_RETIRED)
    if transport not in _TRANSPORTS:
        raise ValueError(f"unknown transport (expected one of {_TRANSPORTS})")

    name = str(ds.get("name") or "unnamed")
    url = ds.get("connection_url")
    if not isinstance(url, str) or not url.strip():
        raise ValueError(f"{transport} transport requires connection_url")

    auth = credentials.get("auth") or {}
    if not isinstance(auth, dict):
        raise ValueError("credentials.auth must be an object")

    headers: dict[str, str] = {}
    auth_type = auth.get("type") or "none"
    if auth_type == "bearer":
        token = auth.get("token")
        if not isinstance(token, str) or not token:
            raise ValueError("bearer auth requires a token")
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "headers":
        custom_headers = auth.get("headers") or {}
        if not isinstance(custom_headers, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in custom_headers.items()
        ):
            raise ValueError("custom headers must map strings to strings")
        headers.update(custom_headers)
    elif auth_type not in ("none", ""):
        raise ValueError("unknown auth type")

    return MCPServerConfig(
        name=name,
        transport=transport,
        url=url,
        headers=headers,
    )


@dataclass
class _ServerHandle:
    ds: dict[str, Any]
    config: MCPServerConfig | None
    slug: str
    status: str = "pending"
    tools: list[Any] = field(default_factory=list)
    session: Any = None
    task: asyncio.Task[None] | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)
    restart_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    reconnected_once: bool = False
    #: When a managed server reconnected, within the budget's window.
    reconnects: deque[float] = field(default_factory=deque)
    generation: int = 0
    #: The headers (its bearer) a managed server's front refused as a dead
    #: lease; ``None`` while none was refused.
    refused_lease: dict[str, str] | None = None

    @property
    def name(self) -> str:
        return str(self.ds.get("name") or "unnamed")

    @property
    def managed(self) -> bool:
        return self.config is not None and self.config.managed

    @property
    def connect_timeout(self) -> float:
        if self.managed:
            return MANAGED_MCP_START_TIMEOUT + MANAGED_MCP_CONNECT_TIMEOUT
        return MCP_CONNECT_TIMEOUT


class MCPManager:
    """Own all MCP connections and discovered tools for one agent runtime."""

    def __init__(self, ds_configs: list[dict[str, Any]]):
        self._handles: list[_ServerHandle] = []
        self._closing = False
        taken_slugs: set[str] = set()

        for ds in ds_configs:
            slug = self._unique_slug(ds.get("name"), taken_slugs)
            taken_slugs.add(slug)
            try:
                config = parse_mcp_config(ds)
            except ValueError as exc:
                handle = _ServerHandle(
                    ds=ds,
                    config=None,
                    slug=slug,
                    status=f"unavailable: invalid config ({exc})",
                )
                handle.ready.set()
                self._handles.append(handle)
                continue
            self._handles.append(_ServerHandle(ds=ds, config=config, slug=slug))

    @staticmethod
    def _unique_slug(name: Any, taken: set[str]) -> str:
        base = mcp_server_slug(str(name or "server"))
        if base not in taken:
            return base
        suffix_number = 2
        while True:
            suffix = f"_{suffix_number}"
            candidate = f"{base[: 16 - len(suffix)].rstrip('_')}{suffix}"
            if candidate not in taken:
                return candidate
            suffix_number += 1

    async def connect_all(self) -> None:
        """Connect all valid servers concurrently, degrading failures per server."""
        waiters = []
        for handle in self._handles:
            if handle.config is None:
                continue
            if handle.task is None:
                handle.task = asyncio.create_task(
                    self._run_server(handle),
                    name=f"mcp-owner-{handle.slug}",
                )
            waiters.append(self._await_ready(handle))

        if waiters:
            await asyncio.gather(*waiters)

        connected = [
            handle.name for handle in self._handles if handle.status == "connected"
        ]
        failed = [
            handle.name for handle in self._handles if handle.status != "connected"
        ]
        logger.info(
            "MCP discovery finished: %d connected, %d unavailable",
            len(connected),
            len(failed),
        )

    async def _await_ready(self, handle: _ServerHandle) -> None:
        timeout = handle.connect_timeout
        try:
            await asyncio.wait_for(
                handle.ready.wait(),
                timeout=timeout,
            )
        except TimeoutError:
            handle.status = f"unavailable: connect timed out after {int(timeout)}s"
            if handle.task is not None:
                handle.task.cancel()

    async def _wait_until_serving(self, config: MCPServerConfig) -> None:
        """Wait for a managed server's front to report ready.

        The pod may not exist yet (its endpoint does not resolve or refuses
        connections) or may still be starting (503). The readiness route
        carries no credential and answers nothing secret.
        """
        import httpx

        deadline = time.monotonic() + MANAGED_MCP_START_TIMEOUT
        attempt = 0
        async with _ready_client() as client:
            while True:
                try:
                    response = await client.get(config.ready_url)
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                pause = _READY_POLL_SECONDS[min(attempt, len(_READY_POLL_SECONDS) - 1)]
                attempt += 1
                if time.monotonic() + pause > deadline:
                    raise TimeoutError(
                        f"not ready after {int(MANAGED_MCP_START_TIMEOUT)}s"
                    )
                await asyncio.sleep(pause)

    async def _run_server(self, handle: _ServerHandle) -> None:
        """Enter, use, and exit one server entirely within its owner task."""
        config = handle.config
        if config is None:
            handle.ready.set()
            return

        try:
            ensure_mcp_sdk()
            if config.managed:
                try:
                    await self._wait_until_serving(config)
                except TimeoutError as exc:
                    handle.status = f"unavailable: {exc}"
                    logger.warning("Managed MCP server %s %s", handle.name, exc)
                    return
            async with AsyncExitStack() as stack:
                if config.transport == "sse":
                    from mcp.client.sse import sse_client

                    read, write = await stack.enter_async_context(
                        sse_client(config.url, headers=config.headers or None)
                    )
                else:
                    from mcp.client import streamable_http

                    http_transport = getattr(
                        streamable_http,
                        "streamable_http_client",
                        None,
                    )
                    if http_transport is not None:
                        from mcp.shared._httpx_utils import create_mcp_http_client

                        http_client = await stack.enter_async_context(
                            create_mcp_http_client(headers=config.headers or None)
                        )
                        transport_context = http_transport(
                            config.url,
                            http_client=http_client,
                        )
                    else:
                        transport_context = streamable_http.streamablehttp_client(
                            config.url,
                            headers=config.headers or None,
                        )
                    read, write, _ = await stack.enter_async_context(transport_context)

                from mcp import ClientSession

                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
                handle.session = session

                from langchain_mcp_adapters.tools import load_mcp_tools

                raw_tools = await load_mcp_tools(session)
                handle.generation += 1
                handle.tools = self._namespace_and_wrap(handle, raw_tools)
                handle.refused_lease = None
                handle.status = "connected"
                handle.ready.set()
                await handle.shutdown.wait()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if handle.shutdown.is_set():
                pass
            elif handle.managed and _lease_ended(exc):
                _refuse_lease(handle)
            else:
                handle.status = f"unavailable: {type(exc).__name__}"
                logger.warning(
                    "MCP server %s became unavailable (%s)",
                    handle.name,
                    type(exc).__name__,
                )
        finally:
            handle.session = None
            handle.ready.set()

    def close(self) -> None:
        """Schedule async teardown when called through the sync close protocol."""
        self._closing = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            loop.create_task(self.aclose(), name="mcp-manager-close")
        else:
            asyncio.run(self.aclose())

    async def aclose(self) -> None:
        """Signal every owner task and wait for transport/subprocess teardown."""
        self._closing = True
        for handle in self._handles:
            handle.shutdown.set()
        tasks = [handle.task for handle in self._handles if handle.task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_langchain_tools(self) -> list[Any]:
        """Return tools backed by currently connected server handles."""
        return [
            tool
            for handle in self._handles
            if handle.status == "connected"
            for tool in handle.tools
        ]

    @property
    def statuses(self) -> dict[str, str]:
        return {handle.name: handle.status for handle in self._handles}

    def annotate_configs(self) -> None:
        """Attach non-secret discovery status to the original datasource dicts."""
        for handle in self._handles:
            handle.ds["_mcp_status"] = handle.status
            handle.ds["_mcp_tools"] = [tool.name for tool in handle.tools]

    def _namespace_and_wrap(
        self,
        handle: _ServerHandle,
        raw_tools: list[Any],
    ) -> list[Any]:
        from langchain_core.tools import StructuredTool

        taken = {
            tool.name for other_handle in self._handles for tool in other_handle.tools
        }
        wrapped = []
        for tool in raw_tools:
            namespaced_name = namespace_mcp_tool(handle.slug, tool.name, taken)
            taken.add(namespaced_name)
            wrapped.append(
                StructuredTool(
                    name=namespaced_name,
                    description=tool.description or "",
                    args_schema=tool.args_schema,
                    coroutine=self._guarded(handle, tool),
                    metadata={
                        "mcp_server": handle.name,
                        "mcp_server_slug": handle.slug,
                        "mcp_tool_name": tool.name,
                    },
                )
            )
        return wrapped

    @staticmethod
    def _is_live(handle: _ServerHandle) -> bool:
        return bool(
            handle.status == "connected"
            and handle.session is not None
            and handle.task is not None
            and not handle.task.done()
        )

    @staticmethod
    def _may_reconnect(handle: _ServerHandle) -> bool:
        """A remote server reconnects once per runtime; a managed
        one within its budget, since its pod is replaced on a re-pin, a lost
        pod or a credential change while the session lives on."""
        if not handle.managed:
            return not handle.reconnected_once
        now = time.monotonic()
        while (
            handle.reconnects
            and now - handle.reconnects[0] > MANAGED_MCP_RECONNECT_WINDOW
        ):
            handle.reconnects.popleft()
        return len(handle.reconnects) < MANAGED_MCP_RECONNECTS

    async def _restart_server(
        self,
        handle: _ServerHandle,
        *,
        force: bool = False,
    ) -> bool:
        """Reconnect a server, within what it may still use."""
        async with handle.restart_lock:
            if self._closing:
                return False
            if handle.refused_lease is not None and not _lease_renewed(handle):
                # The same dead lease would be refused again: no reconnect,
                # no budget spent.
                return False
            if not self._may_reconnect(handle):
                return self._is_live(handle)
            if not force and self._is_live(handle):
                return True

            handle.reconnected_once = True
            handle.reconnects.append(time.monotonic())
            old_task = handle.task
            handle.shutdown.set()
            if old_task is not None and not old_task.done():
                try:
                    # The old transport's own teardown, never a pod start.
                    await asyncio.wait_for(
                        asyncio.shield(old_task),
                        timeout=MCP_CONNECT_TIMEOUT,
                    )
                except TimeoutError:
                    old_task.cancel()
                    await asyncio.gather(old_task, return_exceptions=True)

            try:
                handle.config = parse_mcp_config(handle.ds)
            except ValueError as exc:
                handle.status = f"unavailable: invalid config ({exc})"
                handle.tools = []
                return False

            handle.status = "pending"
            handle.tools = []
            handle.session = None
            handle.ready = asyncio.Event()
            handle.shutdown = asyncio.Event()
            handle.task = asyncio.create_task(
                self._run_server(handle),
                name=f"mcp-owner-{handle.slug}-reconnect",
            )
            await self._await_ready(handle)
            return self._is_live(handle)

    async def _call_current_session(
        self,
        handle: _ServerHandle,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> str:
        result = await self._raced(
            handle, handle.session.call_tool(tool_name, arguments)
        )
        if getattr(result, "isError", False):
            return _tool_error(handle, tool_name, "server reported an error")
        return _content_to_str(result)

    @staticmethod
    async def _raced(handle: _ServerHandle, call: Any) -> Any:
        """A managed server's call, failed as soon as its session ends.

        The SDK does not fail a request in flight when its transport dies
        (a replaced pod refuses the connection), so the call would wait for
        the call timeout and then never reconnect. Racing it against the
        owner task turns a dead session into an error the reconnect path
        handles at once.
        """
        owner = handle.task
        if not handle.managed or owner is None:
            return await call
        pending = asyncio.ensure_future(call)
        try:
            done, _ = await asyncio.wait(
                {pending, owner}, return_when=asyncio.FIRST_COMPLETED
            )
        except BaseException:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            raise
        if pending in done:
            return pending.result()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        raise ConnectionError("the server's session ended")

    def _guarded(self, handle: _ServerHandle, tool: Any):
        """Bound calls, reconnect one dead server once, and return string errors."""
        original_name = tool.name
        wrapper_generation = handle.generation

        async def _invoke(**kwargs):
            if handle.generation != wrapper_generation:
                return await self._call_current_session(
                    handle,
                    original_name,
                    kwargs,
                )
            if tool.coroutine is not None:
                return await self._raced(handle, tool.coroutine(**kwargs))
            return await self._raced(handle, tool.ainvoke(kwargs))

        async def _call(**kwargs):
            if not self._is_live(handle):
                if not await self._restart_server(handle):
                    detail = (
                        LEASE_REFUSED
                        if handle.refused_lease is not None
                        else "server unavailable"
                    )
                    return _tool_error(handle, original_name, detail)
                try:
                    return await asyncio.wait_for(
                        self._call_current_session(handle, original_name, kwargs),
                        timeout=MCP_CALL_TIMEOUT,
                    )
                except TimeoutError:
                    return _tool_error(
                        handle,
                        original_name,
                        f"timed out after {int(MCP_CALL_TIMEOUT)}s",
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return _tool_error(handle, original_name, _mark_failed(handle, exc))

            try:
                return await asyncio.wait_for(
                    _invoke(**kwargs),
                    timeout=MCP_CALL_TIMEOUT,
                )
            except TimeoutError:
                return _tool_error(
                    handle,
                    original_name,
                    f"timed out after {int(MCP_CALL_TIMEOUT)}s",
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if handle.managed and (
                    _lease_ended(exc) or handle.refused_lease is not None
                ):
                    # The front refused the lease (on this call, or the
                    # session ended on it): a reconnect would be refused too.
                    return _tool_error(handle, original_name, _mark_failed(handle, exc))
                if handle.managed and _tool_level(exc):
                    # The server answered: the session is alive, the tool
                    # (or the front, for a tool this binding may not call)
                    # refused. Reconnecting would only spend the budget.
                    return _tool_error(handle, original_name, type(exc).__name__)
                handle.status = f"unavailable: {type(exc).__name__}"
                if await self._restart_server(handle, force=True):
                    try:
                        return await asyncio.wait_for(
                            self._call_current_session(handle, original_name, kwargs),
                            timeout=MCP_CALL_TIMEOUT,
                        )
                    except TimeoutError:
                        return _tool_error(
                            handle,
                            original_name,
                            f"timed out after {int(MCP_CALL_TIMEOUT)}s",
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as retry_exc:
                        return _tool_error(
                            handle,
                            original_name,
                            _mark_failed(handle, retry_exc),
                        )
                if handle.refused_lease is not None:
                    # The reconnect was refused the lease.
                    return _tool_error(handle, original_name, LEASE_REFUSED)
                return _tool_error(handle, original_name, type(exc).__name__)

        return _call


def _ready_client() -> Any:
    """The HTTP client a managed server's readiness is polled with: no
    credential, no redirect."""
    import httpx

    return httpx.AsyncClient(timeout=5.0, follow_redirects=False)


def _mark_failed(handle: _ServerHandle, exc: BaseException) -> str:
    """A failed call marks the server unavailable, unless a managed server
    answered with a tool error (its session is alive). Returns what the
    agent is told: the lease refusal in plain words, else the error type."""
    if handle.managed and (_lease_ended(exc) or handle.refused_lease is not None):
        _refuse_lease(handle)
        return LEASE_REFUSED
    if not (handle.managed and _tool_level(exc)):
        handle.status = f"unavailable: {type(exc).__name__}"
    return type(exc).__name__


def _refuse_lease(handle: _ServerHandle) -> None:
    """The front refused this execution's lease: say so in the server's
    status, and remember the bearer, so only another lease reconnects."""
    if handle.refused_lease is None:
        logger.warning(
            "Managed MCP server %s refused this execution's lease (revoked or "
            "expired); it stays unavailable",
            handle.name,
        )
    config = handle.config
    handle.refused_lease = dict((config.headers if config else None) or {})
    handle.status = f"unavailable: {LEASE_REFUSED}"
    # A session that survived (the refusal came on one call) is of no
    # further use: its owner task closes it.
    handle.shutdown.set()


def _lease_renewed(handle: _ServerHandle) -> bool:
    """Whether the entry carries another lease than the refused one."""
    try:
        config = parse_mcp_config(handle.ds)
    except ValueError:
        return False
    return dict(config.headers or {}) != handle.refused_lease


def _lease_ended(exc: BaseException) -> bool:
    """Whether a managed server's front refused the lease somewhere in this
    failure: an HTTP 401 (a new request on a dead lease), or the front's
    lease-ended error on a call in flight. Exception groups (the
    transport's task group) and causes are searched."""
    import httpx

    try:
        from mcp.shared.exceptions import McpError
    except ImportError:  # pragma: no cover - the SDK is a dependency
        McpError = None  # noqa: N806
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, httpx.HTTPStatusError):
            if current.response.status_code == 401:
                return True
        elif McpError is not None and isinstance(current, McpError):
            error = getattr(current, "error", None)
            if getattr(error, "code", None) == LEASE_ENDED_CODE and str(
                getattr(error, "message", "")
            ).startswith(_LEASE_ENDED_PREFIX):
                return True
        grouped = getattr(current, "exceptions", None)  # an exception group
        if isinstance(grouped, (list, tuple)):
            pending.extend(e for e in grouped if isinstance(e, BaseException))
        pending.extend((current.__cause__, current.__context__))
    return False


def _tool_level(exc: BaseException) -> bool:
    """Whether a call failed in the tool, not in the session: the adapter's
    ToolException (a result with isError) or a JSON-RPC error other than a
    terminated session or a closed connection."""
    from langchain_core.tools import ToolException

    if isinstance(exc, ToolException):
        return True
    try:
        from mcp.shared.exceptions import McpError
    except ImportError:  # pragma: no cover - the SDK is a dependency
        return False
    if isinstance(exc, McpError):
        code = getattr(getattr(exc, "error", None), "code", None)
        return code not in _SESSION_ERROR_CODES
    return False


def _tool_error(handle: _ServerHandle, tool_name: str, detail: str) -> str:
    """Build a useful error without reflecting transport or credential data."""
    return (
        f"MCP tool error ({handle.name}/{tool_name}): {detail}. "
        "Continue without this tool."
    )


def _content_to_str(result: Any) -> str:
    """Flatten an MCP ``CallToolResult`` into its textual representation."""
    try:
        parts = []
        for block in getattr(result, "content", []) or []:
            text = getattr(block, "text", None)
            parts.append(text if text is not None else str(block))
        return "\n".join(parts) if parts else str(result)
    except Exception:
        return str(result)
