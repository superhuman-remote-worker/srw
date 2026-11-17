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

Two more tools serve every remote server that offers resources (Phase D):
:data:`READ_RESOURCE_TOOL` reads a resource by its URI, a ``resource_link``
in a tool's result, say, through the same converter, and
:data:`SAVE_RESOURCE_TOOL` writes one into the workspace without passing it
through the model's context. They take the server as an argument, as Claude
Code's resource tools do, because nearly every server declares resources
whether or not it has any.

A server may ask a question mid-call (elicitation, Phase C): "Spend 12 Buzz?"
The connector's ``server_questions`` setting (:mod:`shared.mcp_questions`)
and the runtime's question handler decide the answer. A yes/no is accepted
by itself under ``accept``; otherwise a handler that can ask a person (an
interactive Session) does, and without one the question is declined. The
call's timeout stops while a person is being asked, and the model reads
one line about each question with the server's result.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack, contextmanager
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from agent.tools.mcp.naming import mcp_server_slug, namespace_mcp_tool
from agent.tools.mcp.schema import model_safe_schema
from shared.mcp_questions import (
    ACCEPT,
    FACTORY_DEFAULT,
    SERVER_QUESTIONS_KEY,
    confirmation_content,
    content_problem,
    form_schema_problem,
    setting_or_none,
)
from shared.mcp_sdk import (
    REMOTE_TRANSPORTS,
    bare_bearer_token,
    bearer_authorization,
    ensure_mcp_sdk,
    http_status,
    open_client,
    walk_exception,
)

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
_TRANSPORTS = REMOTE_TRANSPORTS
#: JSON-RPC errors that mean the session is gone, not that a tool failed:
#: the transport's "Session terminated" (a 404 from the server) and a
#: closed connection.
_SESSION_ERROR_CODES = frozenset({-32600, -32000})
#: Rounds of a 2026-07-28 server's "input required" answer one call may take.
_MAX_INPUT_ROUNDS = 10
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
#: The tools that read and save a server's resources (Phase D).
READ_RESOURCE_TOOL = "read_mcp_resource"
SAVE_RESOURCE_TOOL = "save_mcp_resource"
#: The most text of one resource the model is shown; the rest is saved.
MCP_RESOURCE_TEXT_LIMIT = 100_000
#: The largest resource saved into the workspace (decoded bytes).
MCP_RESOURCE_SAVE_LIMIT = 200 * 1024 * 1024
#: What a call does on the server: call a tool, or read a resource to show
#: it or to save it.
_CALL_TOOL = "tool"
_READ_RESOURCE = "read_resource"
_SAVE_RESOURCE = "save_resource"


#: The tool call a runtime is running, ``(tool name, tool call id)``. A
#: runtime that can show a question sets it right before it runs a tool, so
#: the question names the call it belongs to.
RUNNING_TOOL_CALL: ContextVar[tuple[str, str] | None] = ContextVar(
    "mcp_running_tool_call", default=None
)
#: The most of a server's question the model is shown.
_QUESTION_TEXT_LIMIT = 300


@dataclass
class ServerQuestion:
    """One question an MCP server asks while a tool call runs."""

    #: The connector's name, and its slug in the agent's tool names.
    server: str
    server_slug: str
    #: The agent's tool and its call, when the question could be matched.
    tool: str | None
    tool_call_id: str | None
    #: ``form`` (fields to fill in) or ``url`` (a link to open).
    mode: str
    message: str
    requested_schema: dict[str, Any] | None
    url: str | None
    #: What accepting sends when the form is a yes/no, else ``None``.
    confirmation: dict[str, Any] | None
    #: The connector's setting: ``decline`` or ``accept``.
    setting: str
    #: The connector's id, which keeps a Session's "Always accept".
    datasource_id: str | None = None


@dataclass
class QuestionAnswer:
    """The answer to a server's question: ``accept`` (with the form's
    content), ``decline`` (a refusal), or ``cancel`` (nobody decided)."""

    action: str
    content: dict[str, Any] | None = None
    #: One line for the model about what happened.
    note: str = ""
    #: The person chose "Always accept in this session".
    always_accept: bool = False


#: A runtime's way to ask a person; ``None`` means nobody can be asked.
QuestionHandler = Callable[[ServerQuestion], Awaitable[QuestionAnswer | None]]


@dataclass(eq=False)
class _CallState:
    """One call in flight on a server: what a question during it belongs
    to, the time a person spent answering, and the notes for the model."""

    tool_name: str
    agent_tool: str | None
    tool_call_id: str | None
    started: float = field(default_factory=time.monotonic)
    paused_total: float = 0.0
    paused_since: float | None = None
    pauses: int = 0
    notes: list[str] = field(default_factory=list)
    #: The calling tool's context. A handshake-era question arrives on the
    #: session's own task; the runtime's handler runs in this one instead,
    #: with the turn's lease and identity.
    context: Context = field(default_factory=copy_context)

    @property
    def paused(self) -> bool:
        return self.pauses > 0

    def pause(self) -> None:
        if self.pauses == 0:
            self.paused_since = time.monotonic()
        self.pauses += 1

    def resume(self) -> None:
        self.pauses = max(0, self.pauses - 1)
        if self.pauses == 0 and self.paused_since is not None:
            self.paused_total += time.monotonic() - self.paused_since
            self.paused_since = None

    def active_seconds(self) -> float:
        """Seconds the call has run, not counting a person's answer."""
        now = time.monotonic()
        paused = self.paused_total
        if self.paused_since is not None:
            paused += now - self.paused_since
        return now - self.started - paused


_ACTIVE_CALL: ContextVar[_CallState | None] = ContextVar(
    "mcp_active_call", default=None
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
        if not isinstance(token, str) or not bare_bearer_token(token):
            raise ValueError("bearer auth requires a token")
        headers["Authorization"] = bearer_authorization(token)
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
    #: Whether the server declares resources the agent can read. A managed
    #: server's front forwards tool calls only.
    offers_resources: bool = False
    #: What a question the server asks gets by itself (``server_questions``,
    #: resolved by the orchestrator): ``decline`` or ``accept``.
    server_questions: str = FACTORY_DEFAULT
    #: Whether ``accept`` is the Session's own "Always accept in this
    #: session", not the connector's setting.
    accepted_by_session: bool = False
    #: The calls in flight, for a question that arrives outside its call's
    #: task (a handshake-era server asks on the session's own stream).
    calls: list[_CallState] = field(default_factory=list)
    #: Full-size originals the server's results pointed at: blob id -> its
    #: signed URL. The model sees these URLs only redacted (their signature
    #: is a credential), so save_mcp_resource fetches the original itself.
    original_urls: dict[str, str] = field(default_factory=dict)

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

    def __init__(
        self,
        ds_configs: list[dict[str, Any]],
        questions: QuestionHandler | None = None,
    ):
        self._handles: list[_ServerHandle] = []
        self._closing = False
        self._questions = questions
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
            self._handles.append(
                _ServerHandle(
                    ds=ds,
                    config=config,
                    slug=slug,
                    server_questions=setting_or_none(ds.get(SERVER_QUESTIONS_KEY))
                    or FACTORY_DEFAULT,
                    accepted_by_session=ds.get("server_questions_by_session") is True,
                )
            )

    def set_question_handler(self, questions: QuestionHandler | None) -> None:
        """How this runtime asks a person a server's question (a Session's
        card); ``None`` when nobody can be asked (a job)."""
        self._questions = questions

    def accept_questions_from(self, server_slug: str) -> None:
        """ "Always accept in this session": the server's yes/no questions are
        accepted from now on, in this runtime."""
        for handle in self._handles:
            if handle.slug == server_slug:
                handle.server_questions = ACCEPT
                handle.accepted_by_session = True

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
                client = await open_client(
                    stack,
                    url=config.url,
                    transport=config.transport,
                    headers=config.headers,
                    elicitation_callback=self._elicitation_callback(handle),
                )
                handle.session = client

                listed = await _list_all_tools(client)
                logger.info(
                    "MCP server %s connected over protocol %s with %d tools",
                    handle.name,
                    client.protocol_version,
                    len(listed),
                )
                handle.offers_resources = not config.managed and _offers_resources(
                    client
                )
                handle.tools = self._wrap_tools(handle, listed)
                handle.refused_lease = None
                handle.status = "connected"
                handle.ready.set()
                await handle.shutdown.wait()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if handle.managed and _lease_ended(exc):
                # Recorded even when a reconnect already asked this task to
                # stop: the SDK fails a call in flight as soon as the
                # connection drops, so the reconnect can start before the
                # refusal is known, and must not use the same lease again.
                _refuse_lease(handle)
            elif handle.shutdown.is_set():
                pass
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
        self,
        sees_images: Callable[[], bool] | None = None,
        save_file: Callable[[str, bytes, bool], str] | None = None,
    ) -> list[Any]:
        """Return tools backed by currently connected server handles.

        ``sees_images`` is the loading agent's own "does my model take
        images" check, asked at call time. Each agent (a Session and its
        subagents share this manager) gets copies bound to its check, so
        an image only ever reaches a model that can take it. Without one,
        images are described in text.

        ``save_file(path, data, overwrite)`` writes into the loading agent's
        workspace and returns what the model is told. Without one, the
        resource-saving tool says there is no workspace.
        """
        tools = [
            tool
            for handle in self._handles
            if handle.status == "connected"
            for tool in handle.tools
        ]
        if sees_images is not None:
            handles = {handle.slug: handle for handle in self._handles}
            tools = [
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
        return tools + self._resource_tools(sees_images, save_file)

    @property
    def statuses(self) -> dict[str, str]:
        return {handle.name: handle.status for handle in self._handles}

    def annotate_configs(self) -> None:
        """Attach non-secret discovery status to the original datasource dicts."""
        for handle in self._handles:
            handle.ds["_mcp_status"] = handle.status
            handle.ds["_mcp_tools"] = [tool.name for tool in handle.tools]

    def _resource_tools(
        self,
        sees_images: Callable[[], bool] | None,
        save_file: Callable[[str, bytes, bool], str] | None,
    ) -> list[Any]:
        """The two resource tools, when a connected server offers resources."""
        from langchain_core.tools import StructuredTool

        from shared.permission_modes import READ, WRITE

        offering = [
            handle
            for handle in self._handles
            if handle.status == "connected" and handle.offers_resources
        ]
        if not offering:
            return []
        server = {
            "type": "string",
            "enum": [handle.slug for handle in offering],
            "description": (
                "The server, as its tools are named: mcp__<server>__<tool>."
            ),
        }
        uri = {
            "type": "string",
            "description": (
                "The resource's URI, for example from a [resource link] in one "
                "of the server's tool results."
            ),
        }
        read = StructuredTool(
            name=READ_RESOURCE_TOOL,
            description=(
                "Read a resource from a connected MCP server by its URI, such "
                "as a [resource link] one of its tools returned. Text comes back "
                "as text and an image as an image. To keep a file, or for a "
                f"large or binary one, use {SAVE_RESOURCE_TOOL} instead."
            ),
            args_schema={
                "type": "object",
                "properties": {"server": server, "uri": uri},
                "required": ["server", "uri"],
            },
            coroutine=self._resource_call(_READ_RESOURCE, sees_images, None),
            metadata={
                "mcp_server": None,
                "mcp_server_slug": None,
                "mcp_tool_name": READ_RESOURCE_TOOL,
                "effect": READ,
            },
        )
        save = StructuredTool(
            name=SAVE_RESOURCE_TOOL,
            description=(
                "Save a resource from a connected MCP server into the workspace, "
                "by its URI (such as a [resource link] one of its tools "
                "returned). The file goes straight to `path`, relative to the "
                "workspace root, without passing through your context. When "
                "the server's result named a full-size original for it (a "
                "signed `url`), the original is saved instead of the preview. "
                "An existing file is replaced only with overwrite=true."
            ),
            args_schema={
                "type": "object",
                "properties": {
                    "server": server,
                    "uri": uri,
                    "path": {
                        "type": "string",
                        "description": (
                            "Where to save it, relative to the workspace root "
                            "(e.g. output/image.png)."
                        ),
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Replace a file already at `path`.",
                        "default": False,
                    },
                },
                "required": ["server", "uri", "path"],
            },
            coroutine=self._resource_call(_SAVE_RESOURCE, sees_images, save_file),
            metadata={
                "mcp_server": None,
                "mcp_server_slug": None,
                "mcp_tool_name": SAVE_RESOURCE_TOOL,
                "effect": WRITE,
            },
        )
        return [read, save]

    def _resource_call(
        self,
        operation: str,
        sees_images: Callable[[], bool] | None,
        save_file: Callable[[str, bytes, bool], str] | None,
    ):
        label = (
            READ_RESOURCE_TOOL if operation == _READ_RESOURCE else SAVE_RESOURCE_TOOL
        )

        async def _call(**kwargs):
            wanted = str(kwargs.get("server") or "")
            handle = next(
                (
                    handle
                    for handle in self._handles
                    if handle.offers_resources and wanted in (handle.slug, handle.name)
                ),
                None,
            )
            if handle is None:
                offering = ", ".join(
                    handle.slug for handle in self._handles if handle.offers_resources
                )
                return (
                    f"Error: {label} failed: no connected MCP server {wanted!r} "
                    f"offers resources (servers that do: {offering or 'none'})."
                )
            uri = kwargs.get("uri")
            if not isinstance(uri, str) or not uri.strip():
                return f"Error: {label} failed: `uri` must be the resource's URI."
            arguments: dict[str, Any] = {"uri": uri.strip()}
            if operation == _SAVE_RESOURCE:
                path = kwargs.get("path")
                if not isinstance(path, str) or not path.strip():
                    return (
                        f"Error: {label} failed: `path` must be where to save "
                        "the file, relative to the workspace root."
                    )
                arguments["path"] = path.strip()
                arguments["overwrite"] = kwargs.get("overwrite") is True
            return await self._guarded(
                handle, label, sees_images, operation=operation, save_file=save_file
            )(**arguments)

        return _call

    def _wrap_tools(self, handle: _ServerHandle, listed: list[Any]) -> list[Any]:
        """One LangChain tool per MCP tool, each calling through this manager.

        The tool's ``inputSchema`` is its argument schema, with its regular
        expressions moved into descriptions (:mod:`agent.tools.mcp.schema`).
        Its annotations decide the permission effect.
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
                tool.annotations.model_dump(by_alias=True, exclude_none=True)
                if tool.annotations is not None
                else None
            )
            wrapped.append(
                StructuredTool(
                    name=namespaced_name,
                    description=tool.description or "",
                    # Without regular expressions a provider would check
                    # with its own engine (one it rejects fails the turn).
                    args_schema=model_safe_schema(tool.input_schema),
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
            if handle.refused_lease is not None and not _lease_renewed(handle):
                # The old connection ended on a refused lease, which its
                # teardown has now recorded.
                return False

            handle.reconnected_once = True
            handle.reconnects.append(time.monotonic())

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
        *,
        operation: str = _CALL_TOOL,
        save_file: Callable[[str, bytes, bool], str] | None = None,
    ) -> str:
        """Send one request on the server's current session, within the call
        timeout, and render what it answered: a tool's result, or a resource
        shown or saved."""
        session = handle.session
        if session is None:
            raise ConnectionError("the server's session ended")
        running = RUNNING_TOOL_CALL.get()
        state = _CallState(
            tool_name=tool_name,
            agent_tool=running[0] if running else None,
            tool_call_id=running[1] if running else None,
        )
        handle.calls.append(state)
        token = _ACTIVE_CALL.set(state)
        try:
            if operation == _CALL_TOOL:
                request = _send_tool_call(session, tool_name, arguments)
            else:
                request = session.read_resource(arguments["uri"])
            try:
                result = await _within_call_timeout(state, self._raced(handle, request))
            except TimeoutError:
                return _with_notes(
                    state.notes,
                    _tool_error(
                        handle,
                        tool_name,
                        f"no answer within {int(MCP_CALL_TIMEOUT)}s. The call may "
                        "still have run on the server; check before calling it again",
                    ),
                )
            except Exception as exc:
                # The server's own failure (or a refused question) is
                # reported with what was asked during the call.
                _attach_notes(exc, state.notes)
                raise
        finally:
            _ACTIVE_CALL.reset(token)
            handle.calls.remove(state)
        if operation == _READ_RESOURCE:
            text = _render_resource(result, _asks_yes(sees_images))
        elif operation == _SAVE_RESOURCE:
            text = await _save_resource(handle, tool_name, result, arguments, save_file)
        else:
            _remember_originals(handle, result)
            text = _render_result(handle, tool_name, result, _asks_yes(sees_images))
        return _with_notes(state.notes, text)

    def _elicitation_callback(self, handle: _ServerHandle):
        """The SDK callback for this server's questions, in both protocol
        eras: a 2026-07-28 server's "input required" answer and a
        handshake-era server's ``elicitation/create``."""

        async def callback(context: Any, params: Any) -> Any:
            from mcp import types

            state = _ACTIVE_CALL.get()
            if state is None or state not in handle.calls:
                # A handshake-era question arrives on the session's own
                # stream, outside the call's task: it belongs to the one
                # call in flight, if there is only one.
                state = handle.calls[0] if len(handle.calls) == 1 else None
            answer = await self._answer_question(handle, state, params)
            if state is not None and answer.note:
                state.notes.append(answer.note)
            logger.info(
                "MCP server %s asked a question (%s); answered %s",
                handle.name,
                getattr(params, "mode", None) or "form",
                answer.action,
            )
            return types.ElicitResult(
                action=answer.action,
                content=answer.content if answer.action == "accept" else None,
            )

        return callback

    async def _answer_question(
        self, handle: _ServerHandle, state: _CallState | None, params: Any
    ) -> QuestionAnswer:
        """Decide a server's question: by the connector's setting, by a
        person through the runtime's handler, or by declining."""
        mode = "url" if getattr(params, "mode", None) == "url" else "form"
        message = str(getattr(params, "message", "") or "")
        asked = f"[server question] {handle.name} asked {_quoted(message)}"
        schema = _schema_dict(getattr(params, "requested_schema", None))
        url = str(getattr(params, "url", "") or "") or None
        if mode == "form":
            problem = form_schema_problem(schema)
            if problem is not None:
                logger.warning(
                    "MCP server %s asked with a form SRW cannot show (%s)",
                    handle.name,
                    problem,
                )
                return QuestionAnswer(
                    "decline",
                    note=f"{asked}, with a form SRW cannot show ({problem}). "
                    "SRW declined it.",
                )
        confirmation = confirmation_content(schema) if mode == "form" else None
        if confirmation is not None and handle.server_questions == ACCEPT:
            because = (
                "the user chose to always accept this server's yes/no questions "
                "in this session"
                if handle.accepted_by_session
                else "this connector's setting for server questions is accept"
            )
            return QuestionAnswer(
                "accept", confirmation, note=f"{asked}. SRW accepted it: {because}."
            )
        if self._questions is not None:
            question = ServerQuestion(
                server=handle.name,
                server_slug=handle.slug,
                tool=state.agent_tool if state else None,
                tool_call_id=state.tool_call_id if state else None,
                mode=mode,
                message=message,
                requested_schema=schema if mode == "form" else None,
                url=url,
                confirmation=confirmation,
                setting=handle.server_questions,
                datasource_id=str(handle.ds.get("datasource_id") or "") or None,
            )
            with _paused(state, handle):
                asking = self._questions(question)
                if state is not None:
                    asking = asyncio.create_task(asking, context=state.context)
                answer = await asking
            if answer is not None:
                return self._checked(handle, answer, schema, mode, asked)
        if mode == "url":
            reason = "it asks to open a link, which only a person can do"
        elif confirmation is None:
            reason = "it needs typed input, which only a person can give"
        else:
            reason = (
                "this connector's setting for server questions is decline. "
                "The user can change it to accept in the connector's settings"
            )
        return QuestionAnswer("decline", note=f"{asked}. SRW declined it: {reason}.")

    @staticmethod
    def _checked(
        handle: _ServerHandle,
        answer: QuestionAnswer,
        schema: dict[str, Any] | None,
        mode: str,
        asked: str,
    ) -> QuestionAnswer:
        """A person's answer, as it goes to the server. An accepted form
        must fit the form; a misfit is sent as no answer at all."""
        if answer.action not in ("accept", "decline", "cancel"):
            answer = QuestionAnswer("cancel", note=answer.note)
        if answer.action == "accept" and mode == "form":
            content = dict(answer.content or {})
            problem = content_problem(schema, content)
            if problem is not None:
                logger.warning(
                    "An answer to MCP server %s's question did not fit its form (%s)",
                    handle.name,
                    problem,
                )
                return QuestionAnswer(
                    "cancel",
                    note=f"{asked}. The answer did not fit the server's form "
                    f"({problem}), so SRW sent none.",
                )
            answer.content = content
        else:
            # A declined or dismissed question carries no content, and
            # neither does an accepted link: it only says the user agreed.
            answer.content = None
        if answer.always_accept:
            handle.server_questions = ACCEPT
            handle.accepted_by_session = True
        if not answer.note:
            outcome = {
                "accept": "The user accepted it.",
                "decline": "The user declined it.",
                "cancel": "Nobody answered it, so SRW sent no answer.",
            }[answer.action]
            answer.note = f"{asked}. {outcome}"
        return answer

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
        *,
        operation: str = _CALL_TOOL,
        save_file: Callable[[str, bytes, bool], str] | None = None,
    ):
        """Bound calls, reconnect a dead session, and return string errors.

        Only a session that died reconnects and retries the call. A server
        that answered, even with an error, keeps its session, and the call is
        never sent again: it reached the tool, which may have acted.
        """

        async def call_session(kwargs: dict[str, Any]) -> str:
            return await self._call_session(
                handle,
                tool_name,
                kwargs,
                sees_images,
                operation=operation,
                save_file=save_file,
            )

        def failed(exc: BaseException) -> str:
            return _with_notes(
                _notes_of(exc),
                _tool_error(handle, tool_name, _mark_failed(handle, exc)),
            )

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
                    return await call_session(kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return failed(exc)

            try:
                return await call_session(kwargs)
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
                    return failed(exc)
                handle.status = f"unavailable: {type(exc).__name__}"
                if await self._restart_server(handle, force=True):
                    try:
                        return await call_session(kwargs)
                    except asyncio.CancelledError:
                        raise
                    except Exception as retry_exc:
                        return failed(retry_exc)
                if handle.refused_lease is not None:
                    # The reconnect was refused the lease.
                    return _tool_error(handle, tool_name, LEASE_REFUSED)
                return _tool_error(handle, tool_name, type(exc).__name__)

        return _call


async def _within_call_timeout(state: _CallState, awaitable: Any) -> Any:
    """Await a call within :data:`MCP_CALL_TIMEOUT` of its own running time.

    The time a person spends answering one of the server's questions does
    not count: a slow click must not time out the call it belongs to.
    """
    task = asyncio.ensure_future(awaitable)
    try:
        while True:
            remaining = MCP_CALL_TIMEOUT - state.active_seconds()
            if remaining <= 0 and not state.paused:
                raise TimeoutError
            done, _ = await asyncio.wait(
                {task}, timeout=1.0 if state.paused else max(remaining, 0.01)
            )
            if task in done:
                return task.result()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@contextmanager
def _paused(state: _CallState | None, handle: _ServerHandle) -> Iterator[None]:
    """Stop the clock of the call a question belongs to, or of every call
    on the server when the question could not be matched to one."""
    states = [state] if state is not None else list(handle.calls)
    for current in states:
        current.pause()
    try:
        yield
    finally:
        for current in states:
            current.resume()


def _schema_dict(schema: Any) -> dict[str, Any] | None:
    if schema is None:
        return None
    if hasattr(schema, "model_dump"):
        schema = schema.model_dump(by_alias=True, exclude_none=True)
    return dict(schema) if isinstance(schema, dict) else None


def _quoted(message: str) -> str:
    text = " ".join(message.split())
    if len(text) > _QUESTION_TEXT_LIMIT:
        text = text[:_QUESTION_TEXT_LIMIT].rstrip() + " […]"
    return f"\u201c{text}\u201d"


def _with_notes(notes: list[str], text: str) -> str:
    """The model's view of a call: what the server asked first, then what
    it answered."""
    return "\n".join([*notes, text]) if notes else text


def _attach_notes(exc: BaseException, notes: list[str]) -> None:
    if notes:
        try:
            exc._mcp_question_notes = list(notes)  # type: ignore[attr-defined]
        except AttributeError:
            pass


def _notes_of(exc: BaseException) -> list[str]:
    return list(getattr(exc, "_mcp_question_notes", None) or [])


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
    from mcp.shared.exceptions import MCPError

    if http_status(exc) == 401:
        return True
    return any(
        isinstance(current, MCPError)
        and current.code == LEASE_ENDED_CODE
        and str(current.message).startswith(_LEASE_ENDED_PREFIX)
        for current in walk_exception(exc)
    )


class _InputRefused(Exception):
    """A 2026-07-28 server asked for input mid-call that this client did not
    give. The server answered: the call is not sent again."""


def _tool_level(exc: BaseException) -> bool:
    """Whether the server answered the call, so its session is alive and the
    call must not be sent again: a JSON-RPC error other than a terminated
    session or a closed connection, a result that is not valid MCP, or a
    request for input this client did not give. (A result with ``isError``
    is an answer, not an exception.)"""
    from mcp.shared.exceptions import MCPError
    from pydantic import ValidationError

    if isinstance(exc, MCPError):
        return exc.code not in _SESSION_ERROR_CODES
    return isinstance(exc, (ValidationError, _InputRefused))


def _server_error_message(exc: BaseException) -> str:
    """What the server answered, in its own words where it has any, bounded."""
    from pydantic import ValidationError

    if isinstance(exc, ValidationError):
        return "the server's answer is not a valid MCP tool result"
    if isinstance(exc, _InputRefused):
        return _bounded(str(exc)) or type(exc).__name__
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


async def _list_all_tools(client: Any, *, refresh: bool = False) -> list[Any]:
    """Every tool a server lists, across pages. The client keeps what it
    needs to call them on a 2026-07-28 connection (``Mcp-Param`` headers);
    ``refresh`` reads past its cache."""
    tools: list[Any] = []
    cursor = None
    for _ in range(_MAX_TOOL_PAGES):
        page = await client.list_tools(
            cursor=cursor, cache_mode="refresh" if refresh else "use"
        )
        tools.extend(page.tools)
        cursor = page.next_cursor
        if not cursor:
            break
    return tools


_CALL_RESULT: Any = None


def _call_result_type() -> Any:
    """``tools/call``'s answer: its result, or a 2026-07-28 server's request
    for input first."""
    global _CALL_RESULT
    if _CALL_RESULT is None:
        from mcp import types
        from pydantic import TypeAdapter

        _CALL_RESULT = TypeAdapter(types.CallToolResult | types.InputRequiredResult)
    return _CALL_RESULT


async def _send_tool_call(client: Any, name: str, arguments: dict[str, Any]) -> Any:
    """Send ``tools/call`` and return the server's result as it came.

    The SDK's ``call_tool`` validates ``structuredContent`` against the
    tool's output schema *after* the tool ran and raises a bare
    ``RuntimeError`` on a mismatch. That would read as a dead session,
    reconnect, run the tool a second time and lose its answer, so the
    request is sent directly. A server's off-schema answer is still its
    answer.

    Two 2026-07-28 answers are not results yet, and the call is sent again
    as the protocol asks. ``HEADER_MISMATCH`` means the server refused the
    call unrun because this client's copy of the tool is stale: the tools
    are listed again and the call is resent once. An "input required"
    answer carries the server's questions and its state: they go to the
    session's callbacks, and the call is resent with the answers and the
    state. A question the client cannot answer ends the call
    (:class:`_InputRefused`).
    """
    import anyio
    from mcp import types
    from mcp.client import ClientRequestContext
    from mcp.shared.exceptions import MCPError

    session = client.session

    async def send(responses: Any = None, state: str | None = None) -> Any:
        return await session.send_request(
            types.CallToolRequest(
                params=types.CallToolRequestParams(
                    name=name,
                    arguments=arguments,
                    input_responses=responses,
                    request_state=state,
                ),
            ),
            _call_result_type(),
        )

    try:
        result = await send()
    except MCPError as exc:
        if exc.code != types.HEADER_MISMATCH:
            raise
        await _list_all_tools(client, refresh=True)
        result = await send()

    for round_number in range(_MAX_INPUT_ROUNDS):
        if not isinstance(result, types.InputRequiredResult):
            return result
        responses: dict[str, Any] = {}
        for key, request in (result.input_requests or {}).items():
            meta = request.params.meta if request.params else None
            answer = await session.dispatch_input_request(
                ClientRequestContext(session=session, request_id=key, meta=meta),
                request,
            )
            if isinstance(answer, types.ErrorData):
                raise _InputRefused(
                    "the server asked for input this client cannot give "
                    f"({answer.message})"
                )
            responses[key] = answer
        if not responses:
            # Only its state came back: the server is not done yet.
            await anyio.sleep(min(0.05 * 2**round_number, 0.25))
        result = await send(responses or None, result.request_state)
    if isinstance(result, types.InputRequiredResult):
        raise _InputRefused(
            f"the server still asked for input after {_MAX_INPUT_ROUNDS} rounds"
        )
    return result


def _offers_resources(client: Any) -> bool:
    """Whether the server declares resources (a missing or odd answer is no)."""
    try:
        return getattr(client.server_capabilities, "resources", None) is not None
    except Exception:
        return False


def _render_resource(result: Any, sees_images: bool) -> str:
    """What the model reads for a resource: its text, bounded, an image as
    an image, and anything else described with how to save it."""
    contents = list(getattr(result, "contents", None) or [])
    if not contents:
        return "(the resource is empty)"
    parts = []
    for item in contents:
        uri = str(getattr(item, "uri", "") or "")
        mime = str(getattr(item, "mime_type", "") or "")
        text = getattr(item, "text", None)
        if text is not None and not _is_textual_mime(mime):
            # A binary resource sent as text: shown only if it is base64.
            decoded = _lenient_b64decode(text)
            if decoded is None:
                parts.append(
                    f"[resource {uri}: {mime}, sent as text that is not base64, "
                    "not shown: the server's copy is unusable here; use the "
                    "download URL the server gave for it, if any]"
                )
            elif mime.startswith("image/"):
                parts.append(
                    _render_image(
                        base64.b64encode(decoded).decode("ascii"),
                        mime,
                        sees_images,
                        f"resource {uri}",
                    )
                )
            else:
                parts.append(
                    f"[resource {uri}: {mime}, {len(decoded):,} bytes, not shown: "
                    f"save it into the workspace with {SAVE_RESOURCE_TOOL}]"
                )
            continue
        if text is not None:
            header = f"[resource {uri}" + (f" ({mime})" if mime else "") + "]"
            if len(text) > MCP_RESOURCE_TEXT_LIMIT:
                text = (
                    text[:MCP_RESOURCE_TEXT_LIMIT]
                    + f"\n[… {len(text) - MCP_RESOURCE_TEXT_LIMIT:,} more characters "
                    f"not shown: save the whole resource with {SAVE_RESOURCE_TOOL}]"
                )
            parts.append(f"{header}\n{text}")
            continue
        blob = str(getattr(item, "blob", "") or "")
        if mime.startswith("image/"):
            parts.append(_render_image(blob, mime, sees_images, f"resource {uri}"))
        else:
            parts.append(
                f"[resource {uri}: {mime or 'binary'}, {_decoded_size(blob):,} "
                f"bytes, not shown: save it into the workspace with "
                f"{SAVE_RESOURCE_TOOL}]"
            )
    return "\n".join(parts)


async def _save_resource(
    handle: _ServerHandle,
    tool_name: str,
    result: Any,
    arguments: dict[str, Any],
    save_file: Callable[[str, bytes, bool], str] | None,
) -> str:
    """Write a resource the server returned into the workspace."""
    if save_file is None:
        return _tool_error(
            handle, tool_name, "this agent has no workspace to save the file into"
        )
    uri = arguments["uri"]
    contents = list(getattr(result, "contents", None) or [])
    chosen = [item for item in contents if str(getattr(item, "uri", "")) == uri]
    if not chosen and len(contents) == 1:
        chosen = contents
    if len(chosen) != 1:
        parts = ", ".join(str(getattr(item, "uri", "")) for item in contents[:10])
        return _tool_error(
            handle,
            tool_name,
            f"the resource came back as {len(contents)} parts ({parts or 'none'}); "
            "save one part by its own URI",
        )
    item = chosen[0]
    original = await _save_original(handle, uri, arguments, save_file)
    if original is not None and original[0] == "saved":
        return original[1]
    preview_note = (
        f" (the preview: the full-size original could not be downloaded: {original[1]})"
        if original is not None
        else ""
    )
    text = getattr(item, "text", None)
    mime = str(getattr(item, "mime_type", "") or "")
    if text is not None and not _is_textual_mime(mime):
        # Saving the characters of a binary file as UTF-8 writes a corrupt
        # file that looks like the real one (image_generation.md FM19).
        decoded = _lenient_b64decode(text)
        if decoded is None:
            return _tool_error(
                handle,
                tool_name,
                f"the server sent this {mime} resource as text that is not "
                "base64, so saving it would write a corrupt file; download it "
                "from the URL the server gave for it instead",
            )
        data = decoded
    elif text is not None:
        data = text.encode("utf-8")
    else:
        blob = str(getattr(item, "blob", "") or "")
        if _decoded_size(blob) > MCP_RESOURCE_SAVE_LIMIT:
            return _tool_error(
                handle,
                tool_name,
                f"the resource is larger than {MCP_RESOURCE_SAVE_LIMIT:,} bytes",
            )
        decoded_blob = _lenient_b64decode(blob)
        if decoded_blob is None:
            return _tool_error(
                handle, tool_name, "the server sent malformed resource data"
            )
        data = decoded_blob
    if len(data) > MCP_RESOURCE_SAVE_LIMIT:
        return _tool_error(
            handle,
            tool_name,
            f"the resource is larger than {MCP_RESOURCE_SAVE_LIMIT:,} bytes",
        )
    saved = await asyncio.to_thread(
        save_file, arguments["path"], data, bool(arguments.get("overwrite"))
    )
    if preview_note and not saved.startswith("Error"):
        return saved + preview_note
    return saved


#: Originals remembered per server; the oldest are forgotten first.
_MAX_REMEMBERED_ORIGINALS = 1000
#: How long one original may take to download.
_ORIGINAL_TIMEOUT = 120.0
_MAX_REDIRECTS = 3
#: Test seam: an ``httpx`` transport for the original downloads.
_ORIGINAL_TRANSPORT: Any = None


def _remember_originals(handle: _ServerHandle, result: Any) -> None:
    """Note every ``{"id": …, "url": "https://…"}`` a result carries.

    Civitai's run_step and get_workflow answer with image objects that hold
    a signed ``url`` (the full-size file) next to a ``previewUrl``; the
    resource link they return (``spine://blobs/<id>``) is the preview
    (image_generation.md WP0.2/WP6). Both the structured content and JSON in
    text blocks are read. Never raises.
    """
    found: dict[str, str] = {}

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(value, dict):
            blob_id, url = value.get("id"), value.get("url")
            if (
                isinstance(blob_id, str)
                and blob_id
                and isinstance(url, str)
                and url.startswith("https://")
            ):
                found[blob_id] = url
            for child in value.values():
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value[:500]:
                walk(child, depth + 1)

    try:
        walk(getattr(result, "structured_content", None))
        for block in getattr(result, "content", None) or []:
            text = (
                getattr(block, "text", None)
                if getattr(block, "type", None) == "text"
                else None
            )
            if not text or len(text) > 2_000_000:
                continue
            # Prose and JSON mix in one block ("Workflow … Succeeded." then a
            # JSON list): try each line that opens a JSON value.
            starts = [0] + [n + 1 for n, ch in enumerate(text) if ch == "\n"]
            for start in starts[:200]:
                if text[start : start + 1] not in ("[", "{"):
                    continue
                try:
                    value, _end = json.JSONDecoder().raw_decode(text, start)
                except ValueError:
                    continue
                walk(value)
    except Exception:  # noqa: BLE001 - remembering is best effort
        logger.debug("MCP: could not scan a result for originals", exc_info=True)
        return
    for blob_id, url in found.items():
        handle.original_urls.pop(blob_id, None)
        handle.original_urls[blob_id] = url
    while len(handle.original_urls) > _MAX_REMEMBERED_ORIGINALS:
        handle.original_urls.pop(next(iter(handle.original_urls)))


async def _fetch_original(url: str) -> bytes:
    """GET a public https URL without credentials, re-checking every
    redirect, within the save limit. Raises ``ValueError`` with the reason."""
    import httpx

    from shared.url_safety import UnsafeURLError, check_public_http_url

    current = url
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_ORIGINAL_TIMEOUT, connect=15.0),
        follow_redirects=False,
        trust_env=False,
        transport=_ORIGINAL_TRANSPORT,
    ) as client:
        for _ in range(_MAX_REDIRECTS + 1):
            if not current.startswith("https://"):
                raise ValueError("the original is not on an https URL")
            try:
                await asyncio.to_thread(check_public_http_url, current)
            except UnsafeURLError as exc:
                raise ValueError(
                    f"the original's address is not public ({exc})"
                ) from None
            except OSError:
                raise ValueError("the original's host does not resolve") from None
            async with client.stream("GET", current) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get("location")
                    if not location:
                        raise ValueError("a redirect without a location")
                    current = str(response.url.join(location))
                    continue
                if response.status_code != 200:
                    raise ValueError(
                        f"the download answered HTTP {response.status_code}"
                    )
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MCP_RESOURCE_SAVE_LIMIT:
                        raise ValueError(
                            f"the original is larger than {MCP_RESOURCE_SAVE_LIMIT:,} bytes"
                        )
                    chunks.append(chunk)
                return b"".join(chunks)
    raise ValueError("too many redirects")


async def _save_original(
    handle: _ServerHandle,
    uri: str,
    arguments: dict[str, Any],
    save_file: Callable[[str, bytes, bool], str] | None,
) -> tuple[str, str] | None:
    """Save the full-size original of ``uri`` when a result named one.

    ``None`` when no original is known (the caller saves the resource as it
    is); ``("saved", text)`` with what the workspace answered, success or not
    (an existing file is not a reason to save the preview instead); or
    ``("fallback", reason)`` when the download failed, for the caller to save
    the preview and say why.
    """
    if save_file is None:
        return None
    blob_id = uri.rstrip("/").rsplit("/", 1)[-1]
    url = handle.original_urls.get(blob_id)
    if not url:
        return None
    try:
        data = await _fetch_original(url)
    except ValueError as exc:
        return ("fallback", str(exc))
    except Exception as exc:  # noqa: BLE001 - any failure falls back to the preview
        return ("fallback", f"the download failed ({type(exc).__name__})")
    from agent.services.image_compose import sniff_mime

    mime = sniff_mime(data)
    if mime is None:
        return ("fallback", "the original is not a PNG, JPEG, WebP or GIF image")
    path = str(arguments["path"])
    extension = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }[mime]
    accepted = {extension, ".jpeg"} if extension == ".jpg" else {extension}
    stem, dot, suffix = path.rpartition(".")
    if dot and "/" not in suffix and f".{suffix.lower()}" not in accepted:
        path = stem + extension
    saved = await asyncio.to_thread(
        save_file, path, data, bool(arguments.get("overwrite"))
    )
    if saved.startswith("Error"):
        return ("saved", saved)
    note = " (the full-size original"
    if path != arguments["path"]:
        note += f"; saved as {path}, since it is {mime.split('/')[1].upper()}"
    return ("saved", saved + note + ")")


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
    structured = getattr(result, "structured_content", None)
    if getattr(result, "is_error", False):
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
            str(getattr(block, "mime_type", "") or ""),
            sees_images,
            "image",
        )
    if kind == "audio":
        mime = getattr(block, "mime_type", "") or "audio"
        size = _decoded_size(str(getattr(block, "data", "") or ""))
        return (
            f"[audio: {mime}, {size:,} bytes, not shown: the model cannot hear audio]"
        )
    if kind == "resource_link":
        name = getattr(block, "title", None) or getattr(block, "name", "") or ""
        line = f"[resource link] {name}: {getattr(block, 'uri', '')}"
        mime = getattr(block, "mime_type", None)
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
        mime = str(getattr(resource, "mime_type", "") or "")
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
    """The decoded size of base64 data: four characters are three bytes,
    less the padding."""
    data = "".join(data.split())
    return len(data) * 3 // 4 - (len(data) - len(data.rstrip("=")))


_TEXTUAL_MIME_EXACT = frozenset(
    {
        "application/json",
        "application/ld+json",
        "application/xml",
        "application/javascript",
        "application/ecmascript",
        "application/x-yaml",
        "application/yaml",
        "application/toml",
        "application/sql",
        "application/csv",
        "application/x-sh",
        "application/graphql",
        "image/svg+xml",
    }
)


def _is_textual_mime(mime: str) -> bool:
    """Whether a resource of ``mime`` may arrive as text (no MIME type: yes)."""
    base = mime.split(";", 1)[0].strip().lower()
    if not base:
        return True
    return (
        base.startswith("text/")
        or base in _TEXTUAL_MIME_EXACT
        or base.endswith("+json")
        or base.endswith("+xml")
    )


def _lenient_b64decode(data: str) -> bytes | None:
    """Decode standard or URL-safe base64, padded or not, wrapped or not;
    ``None`` when ``data`` is not base64 at all."""
    compact = "".join(data.split())
    if not compact:
        return None
    compact = compact.translate(str.maketrans("-_", "+/"))
    compact += "=" * (-len(compact) % 4)
    try:
        return base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError):
        return None


def _json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)
