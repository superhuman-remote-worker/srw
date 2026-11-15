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

Every call goes through one path, :meth:`MCPManager._call_session`, and
every result through one converter, :func:`_render_result`. Text reaches the
model as text. An image reaches a multimodal model as an image, through the
``<image_data>`` contract both graphs lift into a real image block. A tool's
own error (``isError``, or a JSON-RPC error from a live session) reaches the
model in the server's words. A call the server answered is never re-run: only
a dead session reconnects
(knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
Phase A).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from collections.abc import Callable
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
#: The most of a server's error text the model is shown.
MCP_ERROR_TEXT_LIMIT = 4000
#: The largest inline image (base64 characters, ~15 MB decoded) passed on
#: to the model; a larger one is described instead.
MCP_INLINE_IMAGE_LIMIT = 20 * 1024 * 1024
#: Image types every provider the agent talks to accepts.
_VISIBLE_IMAGE_TYPES = frozenset({"image/png", "image/jpeg", "image/gif", "image/webp"})
_BASE64 = re.compile(r"[A-Za-z0-9+/=\s]+")
#: Pages of tools/list read from one server before giving up on the rest.
_MAX_TOOL_PAGES = 50


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


def mcp_tool_effect(annotations: Any, managed_block: Any = None, name: str = "") -> str:
    """The permission effect of one MCP tool (tool_permission_tiers.md, D11).

    A managed server's driver spec classes its tools, and only that decides:
    the tools it names as reads are ``read``, the rest act on the upstream
    service. Any other server's annotation hints are mapped as Codex maps
    them; a missing hint takes the MCP default (destructive, open world).
    """
    from shared.permission_modes import EXTERNAL, READ, WRITE

    if managed_block is not None:
        return READ if managed_block.tool_class(name) == READ else EXTERNAL
    hints = annotations if isinstance(annotations, dict) else {}
    if hints.get("readOnlyHint") is True:
        return READ
    if hints.get("destructiveHint") is False and hints.get("openWorldHint") is False:
        return WRITE
    return EXTERNAL


def _managed_block(ds: dict[str, Any]) -> Any:
    """The parsed ``mcp`` block of a managed server's driver spec, else None."""
    from shared.connectors.builtin import spec_for_type
    from shared.connectors.mcp import managed_mcp

    spec = spec_for_type(ds.get("type"))
    return managed_mcp(spec) if spec is not None else None


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

                listed = await _list_all_tools(session)
                handle.tools = self._wrap_tools(handle, listed)
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

    def get_langchain_tools(
        self, sees_images: Callable[[], bool] | None = None
    ) -> list[Any]:
        """Return tools backed by currently connected server handles.

        ``sees_images`` is the loading agent's own "does my model take
        images" check, asked at call time. Each agent (a Session and its
        subagents share this manager) gets copies bound to its check, so
        an image only ever reaches a model that can take it. Without one,
        images are described in text.
        """
        tools = [
            tool
            for handle in self._handles
            if handle.status == "connected"
            for tool in handle.tools
        ]
        if sees_images is None:
            return tools
        handles = {handle.slug: handle for handle in self._handles}
        return [
            tool.model_copy(
                update={
                    "coroutine": self._guarded(
                        handles[tool.metadata["mcp_server_slug"]],
                        tool.metadata["mcp_tool_name"],
                        sees_images,
                    )
                }
            )
            for tool in tools
        ]

    @property
    def statuses(self) -> dict[str, str]:
        return {handle.name: handle.status for handle in self._handles}

    def annotate_configs(self) -> None:
        """Attach non-secret discovery status to the original datasource dicts."""
        for handle in self._handles:
            handle.ds["_mcp_status"] = handle.status
            handle.ds["_mcp_tools"] = [tool.name for tool in handle.tools]

    def _wrap_tools(self, handle: _ServerHandle, listed: list[Any]) -> list[Any]:
        """One LangChain tool per MCP tool, each calling through this manager.

        The tool's ``inputSchema`` is its argument schema as is. Its
        annotations decide the permission effect.
        """
        from langchain_core.tools import StructuredTool

        taken = {
            tool.name for other_handle in self._handles for tool in other_handle.tools
        }
        managed_block = _managed_block(handle.ds) if handle.managed else None
        wrapped = []
        for tool in listed:
            namespaced_name = namespace_mcp_tool(handle.slug, tool.name, taken)
            taken.add(namespaced_name)
            annotations = (
                tool.annotations.model_dump() if tool.annotations is not None else None
            )
            wrapped.append(
                StructuredTool(
                    name=namespaced_name,
                    description=tool.description or "",
                    args_schema=tool.inputSchema,
                    coroutine=self._guarded(handle, tool.name),
                    metadata={
                        "mcp_server": handle.name,
                        "mcp_server_slug": handle.slug,
                        "mcp_tool_name": tool.name,
                        "effect": mcp_tool_effect(
                            annotations, managed_block, tool.name
                        ),
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

    async def _call_session(
        self,
        handle: _ServerHandle,
        tool_name: str,
        arguments: dict[str, Any],
        sees_images: Callable[[], bool] | None = None,
    ) -> str:
        """Call a tool on the server's current session, within the call
        timeout, and render what it answered."""
        session = handle.session
        if session is None:
            raise ConnectionError("the server's session ended")
        try:
            result = await asyncio.wait_for(
                self._raced(handle, _send_tool_call(session, tool_name, arguments)),
                timeout=MCP_CALL_TIMEOUT,
            )
        except TimeoutError:
            return _tool_error(
                handle,
                tool_name,
                f"no answer within {int(MCP_CALL_TIMEOUT)}s. The call may still "
                "have run on the server; check before calling it again",
            )
        return _render_result(handle, tool_name, result, _asks_yes(sees_images))

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

    def _guarded(
        self,
        handle: _ServerHandle,
        tool_name: str,
        sees_images: Callable[[], bool] | None = None,
    ):
        """Bound calls, reconnect a dead session, and return string errors.

        Only a session that died reconnects and retries the call. A server
        that answered, even with an error, keeps its session, and the call is
        never sent again: it reached the tool, which may have acted.
        """

        async def _call(**kwargs):
            if not self._is_live(handle):
                if not await self._restart_server(handle):
                    detail = (
                        LEASE_REFUSED
                        if handle.refused_lease is not None
                        else "server unavailable"
                    )
                    return _tool_error(handle, tool_name, detail)
                try:
                    return await self._call_session(
                        handle, tool_name, kwargs, sees_images
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return _tool_error(handle, tool_name, _mark_failed(handle, exc))

            try:
                return await self._call_session(handle, tool_name, kwargs, sees_images)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if (
                    handle.managed
                    and (_lease_ended(exc) or handle.refused_lease is not None)
                ) or _tool_level(exc):
                    # The front refused the lease (a reconnect would be
                    # refused too), or the server answered with an error (the
                    # session is alive and the call reached it).
                    return _tool_error(handle, tool_name, _mark_failed(handle, exc))
                handle.status = f"unavailable: {type(exc).__name__}"
                if await self._restart_server(handle, force=True):
                    try:
                        return await self._call_session(
                            handle, tool_name, kwargs, sees_images
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as retry_exc:
                        return _tool_error(
                            handle, tool_name, _mark_failed(handle, retry_exc)
                        )
                if handle.refused_lease is not None:
                    # The reconnect was refused the lease.
                    return _tool_error(handle, tool_name, LEASE_REFUSED)
                return _tool_error(handle, tool_name, type(exc).__name__)

        return _call


def _asks_yes(probe: Callable[[], bool] | None) -> bool:
    """A yes/no check's answer; a missing or failing check is no."""
    if probe is None:
        return False
    try:
        return bool(probe())
    except Exception:
        return False


def _ready_client() -> Any:
    """The HTTP client a managed server's readiness is polled with: no
    credential, no redirect."""
    import httpx

    return httpx.AsyncClient(timeout=5.0, follow_redirects=False)


def _mark_failed(handle: _ServerHandle, exc: BaseException) -> str:
    """A failed call marks the server unavailable, unless the server answered
    with an error (its session is alive). Returns what the agent is told:
    the lease refusal in plain words, the server's own error message, or
    else only the error type (a transport error may carry a URL or a
    header)."""
    if handle.managed and (_lease_ended(exc) or handle.refused_lease is not None):
        _refuse_lease(handle)
        return LEASE_REFUSED
    if _tool_level(exc):
        return _server_error_message(exc)
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
    """Whether the server answered the call with an error, so its session is
    alive: a JSON-RPC error other than a terminated session or a closed
    connection. (A result with ``isError`` is an answer, not an exception.)"""
    try:
        from mcp.shared.exceptions import McpError
    except ImportError:  # pragma: no cover - the SDK is a dependency
        return False
    if isinstance(exc, McpError):
        code = getattr(getattr(exc, "error", None), "code", None)
        return code not in _SESSION_ERROR_CODES
    return False


def _server_error_message(exc: BaseException) -> str:
    """A JSON-RPC error's message, the server's own words, bounded."""
    message = str(getattr(getattr(exc, "error", None), "message", "") or "")
    return _bounded(message) or type(exc).__name__


def _bounded(text: str, limit: int = MCP_ERROR_TEXT_LIMIT) -> str:
    text = text.strip()
    if len(text) > limit:
        return text[:limit].rstrip() + " […]"
    return text


def _tool_error(handle: _ServerHandle, tool_name: str, detail: str) -> str:
    """A failed call, as the model reads it.

    It starts with ``Error:``, which is how a Session tells a failed call
    from a result. It names the server and the tool, carries the server's
    own words when it answered, and only says to give up on the server
    when the server is no longer usable. Transport errors are reported by
    type only, so no URL or credential is echoed.
    """
    detail = detail.rstrip()
    if not detail.endswith((".", "!", "?", "]", ")")):
        detail += "."
    message = f"Error: MCP tool {handle.name}/{tool_name} failed: {detail}"
    if handle.status != "connected":
        message += " Continue without this server's tools."
    return message


async def _list_all_tools(session: Any) -> list[Any]:
    """Every tool a server lists, across pages."""
    from mcp.types import PaginatedRequestParams

    tools: list[Any] = []
    cursor = None
    for _ in range(_MAX_TOOL_PAGES):
        params = PaginatedRequestParams(cursor=cursor) if cursor else None
        page = await session.list_tools(params=params)
        tools.extend(page.tools)
        cursor = page.nextCursor
        if not cursor:
            break
    return tools


async def _send_tool_call(session: Any, name: str, arguments: dict[str, Any]) -> Any:
    """Send ``tools/call`` and return the server's result as it came.

    ``ClientSession.call_tool`` validates ``structuredContent`` against the
    tool's output schema *after* the tool ran and raises a bare
    ``RuntimeError`` on a mismatch. That would read as a dead session,
    reconnect, run the tool a second time and lose its answer, so the
    request is sent directly. A server's off-schema answer is still its
    answer.
    """
    from mcp import types

    return await session.send_request(
        types.ClientRequest(
            types.CallToolRequest(
                params=types.CallToolRequestParams(name=name, arguments=arguments),
            )
        ),
        types.CallToolResult,
    )


def _render_result(
    handle: _ServerHandle, tool_name: str, result: Any, sees_images: bool
) -> str:
    """What the model reads for a tool's result.

    A result with ``isError`` is the tool's own failure, reported in the
    server's words. Otherwise each content block becomes text: images go
    through the ``<image_data>`` contract when the model sees images, and
    are described when it does not. ``structuredContent`` is shown only when
    the result has no content blocks: the spec asks servers to put the same
    data in a text block, and a server that answered with blocks (an image
    its structured copy repeats, say) has said what it meant to show.
    """
    blocks = list(getattr(result, "content", None) or [])
    structured = getattr(result, "structuredContent", None)
    if getattr(result, "isError", False):
        text = "\n".join(_render_block(b, sees_images=False) for b in blocks)
        if not blocks and structured is not None:
            text = _json(structured)
        return _tool_error(
            handle,
            tool_name,
            _bounded(text) or "the server reported an error without a message",
        )
    parts = [_render_block(block, sees_images) for block in blocks]
    if not blocks and structured is not None:
        parts.append(_json(structured))
    text = "\n".join(part for part in parts if part)
    return text if text else "(the tool returned no content)"


def _render_block(block: Any, sees_images: bool) -> str:
    kind = getattr(block, "type", None)
    if kind == "text":
        return str(getattr(block, "text", "") or "")
    if kind == "image":
        return _render_image(
            str(getattr(block, "data", "") or ""),
            str(getattr(block, "mimeType", "") or ""),
            sees_images,
            "image",
        )
    if kind == "audio":
        mime = getattr(block, "mimeType", "") or "audio"
        size = _decoded_size(str(getattr(block, "data", "") or ""))
        return (
            f"[audio: {mime}, {size:,} bytes, not shown: the model cannot hear audio]"
        )
    if kind == "resource_link":
        name = getattr(block, "title", None) or getattr(block, "name", "") or ""
        line = f"[resource link] {name}: {getattr(block, 'uri', '')}"
        mime = getattr(block, "mimeType", None)
        if mime:
            line += f" ({mime})"
        description = getattr(block, "description", None)
        if description:
            line += f" - {description}"
        return line
    if kind == "resource":
        resource = getattr(block, "resource", None)
        uri = getattr(resource, "uri", "")
        text = getattr(resource, "text", None)
        if text is not None:
            return f"[resource {uri}]\n{text}"
        blob = str(getattr(resource, "blob", "") or "")
        mime = str(getattr(resource, "mimeType", "") or "")
        if mime.startswith("image/"):
            return _render_image(blob, mime, sees_images, f"resource {uri}")
        return (
            f"[resource {uri}: {mime or 'binary'}, {_decoded_size(blob):,} bytes, "
            "not shown]"
        )
    return f"[{kind or 'unknown'} content not shown]"


def _render_image(data: str, mime: str, sees_images: bool, label: str) -> str:
    size = _decoded_size(data)
    if not sees_images:
        return f"[{label}: {mime}, {size:,} bytes, not shown: this model cannot see images]"
    if mime not in _VISIBLE_IMAGE_TYPES:
        return f"[{label}: {mime}, {size:,} bytes, not shown: not an image type the model can view]"
    if len(data) > MCP_INLINE_IMAGE_LIMIT:
        return f"[{label}: {mime}, {size:,} bytes, not shown: too large to view]"
    if not data or _BASE64.fullmatch(data) is None:
        return f"[{label}: {mime}, not shown: the server sent malformed image data]"
    from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE

    return IMAGE_DATA_TAG_TEMPLATE.format(mime=mime, b64=data)


def _decoded_size(data: str) -> int:
    return len(data) * 3 // 4


def _json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)
