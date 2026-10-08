"""Managed MCP servers: the ``mcp`` block of a service driver's spec (D5a).

A managed MCP driver is an MCP server image SRW hosts as a service driver:
one shared pod per connector, image digest and credential generation
(decision 7), made of the server and SRW's **front**. The front is the pod's
only exposed port (``srw-driver``). It authenticates every request with the
caller's lease token (the exchange's introspection route), injects the
upstream credential the exchange returns, and enforces the binding's access
level by hiding and refusing the tools the level does not allow. The agent
process is the client (``callers: ["harness"]``) and holds only the lease
token, so the upstream credential never reaches the agent pod.

The block, ``ServiceSpec.mcp``, is plain JSON, so it can ride an image label:

``transport``
    ``http``: the server speaks streamable HTTP. ``stdio`` images are D5b.
``port``, ``path``
    Where the server listens inside the pod (``127.0.0.1`` where the image
    allows it; otherwise only the front's port is reachable from outside the
    pod, by its NetworkPolicy). Never the front's port.
``protocol``
    ``legacy`` (initialize-based 2025 sessions), ``modern`` (the stateless
    2026-07-28 protocol, ``server/discover``) or ``both``: how the front
    probes readiness. SRW's own client is session-based (``mcp<2``), so a
    managed server must accept ``initialize``.
``tools``
    Tool classes: ``{"read": [names or patterns]}``. A tool no class names is
    ``write``, so a tool the image adds later stays hidden from a read-only
    binding until the spec classes it (fail closed). A pattern is a name
    whose ``*`` matches any run of characters.
``access``
    The classes each of the spec's access levels sees, for example
    ``{"ReadOnly": ["read"], "ReadWrite": ["read", "write"]}``. Tool
    annotations never decide a class: the MCP spec says clients must treat
    them as untrusted.
``credential``
    How the front hands the server the upstream credential on each request:
    ``{"header": "Authorization", "scheme": "Bearer"}`` (``scheme`` may be
    empty), or ``null`` for a server that needs none.
``env``, ``args``, ``command``
    The server container's environment, arguments and (optional) program,
    else the image's own. ``${config.<key>}`` in a value is the connector's
    config value. They are never secret: the credential travels per request.
``max_in_flight_per_binding``
    Calls one binding may have open at once (the front answers 429 past it).
``tool_pinning``
    ``warn`` or ``block`` when the server's tool list changes under one
    digest during a pod's life.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP".
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: The path the front serves MCP at, on the ``srw-driver`` port.
FRONT_PATH = "/mcp"
READ = "read"
WRITE = "write"
TOOL_CLASSES: tuple[str, ...] = (READ, WRITE)
PROTOCOLS: tuple[str, ...] = ("legacy", "modern", "both")
TOOL_PINNING: tuple[str, ...] = ("warn", "block")
#: A tool name or pattern: the characters MCP tool names use, plus ``*``.
_PATTERN = re.compile(r"[A-Za-z0-9_.*-]{1,128}\Z")
_HEADER = re.compile(r"[A-Za-z0-9-]{1,64}\Z")
_SCHEME = re.compile(r"[A-Za-z0-9._-]{0,32}\Z")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_PATH = re.compile(r"/[A-Za-z0-9._~/-]{0,127}\Z")
_TEMPLATE = re.compile(r"\$\{config\.([a-z][a-z0-9_]*)\}")
#: Environment names the server may not be given: SRW's own, and the
#: loader's, which would run code before the image's program.
_RESERVED_ENV = re.compile(r"(SRW_|LD_|DYLD_).*", re.IGNORECASE)
_KEYS = frozenset(
    {
        "transport",
        "port",
        "path",
        "protocol",
        "tools",
        "access",
        "credential",
        "env",
        "args",
        "command",
        "max_in_flight_per_binding",
        "tool_pinning",
    }
)


class TemplateError(ValueError):
    """A ``${config.<key>}`` the connector's config cannot fill."""


def pattern_matches(pattern: str, name: str) -> bool:
    """Whether a tool ``name`` matches a class ``pattern`` (``*`` matches
    any run of characters, everything else itself). The front implements
    the same rule (drivers/mcp-front)."""
    regex = "".join(
        ".*" if part == "*" else re.escape(part) for part in re.split(r"(\*)", pattern)
    )
    return re.fullmatch(regex, name, flags=re.DOTALL) is not None


def render(template: str, config: Mapping[str, Any]) -> str:
    """``template`` with each ``${config.<key>}`` replaced by its value.

    A value must be a string, a number or a boolean; a missing key or
    another value is a :class:`TemplateError`.
    """

    def value(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in config:
            raise TemplateError(
                f"the managed MCP block names config.{key}, which is not set"
            )
        found = config[key]
        if isinstance(found, bool):
            return "true" if found else "false"
        if isinstance(found, (str, int, float)):
            return str(found)
        raise TemplateError(f"config.{key} is not a string or a number")

    return _TEMPLATE.sub(value, template)


@dataclass(frozen=True)
class ManagedMcp:
    """A parsed, valid ``mcp`` block."""

    port: int
    access: Mapping[str, tuple[str, ...]]
    transport: str = "http"
    path: str = "/mcp"
    protocol: str = "legacy"
    read_tools: tuple[str, ...] = ()
    credential_header: str | None = "Authorization"
    credential_scheme: str = "Bearer"
    env: Mapping[str, str] = field(default_factory=dict)
    args: tuple[str, ...] = ()
    command: tuple[str, ...] = ()
    max_in_flight_per_binding: int = 4
    tool_pinning: str = "warn"

    @classmethod
    def parse(
        cls,
        block: Any,
        *,
        access_levels: Sequence[str],
        front_port: int | None = None,
    ) -> ManagedMcp:
        """The block, or a :class:`ValueError` naming the first problem."""
        problems = mcp_problems(
            block, access_levels=access_levels, front_port=front_port
        )
        if problems:
            raise ValueError(problems[0])
        credential = block.get(
            "credential", {"header": "Authorization", "scheme": "Bearer"}
        )
        return cls(
            transport=str(block.get("transport", "http")),
            port=int(block["port"]),
            path=str(block.get("path", "/mcp")),
            protocol=str(block.get("protocol", "legacy")),
            read_tools=tuple(
                str(item) for item in (block.get("tools") or {}).get(READ, ())
            ),
            access={
                str(level): tuple(str(c) for c in classes)
                for level, classes in block["access"].items()
            },
            credential_header=(
                str(credential["header"]) if credential is not None else None
            ),
            credential_scheme=(
                str(credential.get("scheme", "")) if credential is not None else ""
            ),
            env={str(k): str(v) for k, v in (block.get("env") or {}).items()},
            args=tuple(str(item) for item in block.get("args") or ()),
            command=tuple(str(item) for item in block.get("command") or ()),
            max_in_flight_per_binding=int(block.get("max_in_flight_per_binding", 4)),
            tool_pinning=str(block.get("tool_pinning", "warn")),
        )

    def tool_class(self, name: str) -> str:
        """``read`` when a read pattern names the tool, else ``write``."""
        return READ if any(pattern_matches(p, name) for p in self.read_tools) else WRITE

    def allowed(self, name: str, access: str | None) -> bool:
        """Whether a binding at ``access`` may see and call ``name``.

        An access level the block does not know sees nothing (fail closed).
        """
        return self.tool_class(name) in self.access.get(access or "", ())

    @property
    def upstream(self) -> str:
        """The server's URL as the front reaches it, inside the pod."""
        return f"http://127.0.0.1:{self.port}{self.path}"

    def server_env(self, config: Mapping[str, Any]) -> dict[str, str]:
        return {name: render(value, config) for name, value in self.env.items()}

    def server_args(self, config: Mapping[str, Any]) -> list[str]:
        return [render(value, config) for value in self.args]

    def front_config(self) -> dict[str, Any]:
        """What the front reads from the pod's request file (``mcp``)."""
        return {
            "upstream": self.upstream,
            "protocol": self.protocol,
            "tools": {READ: list(self.read_tools)},
            "access": {level: list(classes) for level, classes in self.access.items()},
            "credential": (
                {"header": self.credential_header, "scheme": self.credential_scheme}
                if self.credential_header
                else None
            ),
            "max_in_flight_per_binding": self.max_in_flight_per_binding,
            "tool_pinning": self.tool_pinning,
        }


def _strings(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and all(
        isinstance(item, str) for item in value
    )


def _template_problems(where: str, value: str) -> list[str]:
    stripped = _TEMPLATE.sub("", value)
    if "${" in stripped:
        return [f"{where} has a placeholder that is not ${{config.<key>}}"]
    return []


def mcp_problems(
    block: Any, *, access_levels: Sequence[str], front_port: int | None = None
) -> list[str]:
    """Every problem with an ``mcp`` block, as human-readable lines."""
    if not isinstance(block, Mapping):
        return ["the mcp block must be an object"]
    problems = [
        f"the mcp block has an unknown key {key!r}"
        for key in sorted(set(block) - _KEYS)
    ]
    transport = block.get("transport", "http")
    if transport != "http":
        problems.append(
            f"mcp transport {transport!r} is not supported (http; stdio images come with D5b)"
        )
    port = block.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        problems.append("mcp port must be the server's port number")
    elif front_port is not None and port == front_port:
        problems.append("mcp port is the front's port; the server needs its own")
    path = block.get("path", "/mcp")
    if not isinstance(path, str) or not _PATH.fullmatch(path):
        problems.append("mcp path must be an absolute URL path")
    if block.get("protocol", "legacy") not in PROTOCOLS:
        problems.append(f"mcp protocol must be one of {PROTOCOLS}")
    tools = block.get("tools", {})
    if not isinstance(tools, Mapping) or set(tools) - {READ}:
        problems.append('mcp tools is {"read": [names or patterns]}')
    elif not _strings(tools.get(READ, [])) or not all(
        _PATTERN.fullmatch(item) for item in tools.get(READ, [])
    ):
        problems.append("mcp tools.read lists tool names or patterns")
    access = block.get("access")
    if not isinstance(access, Mapping):
        problems.append("mcp access maps each access level to the tool classes it sees")
    else:
        missing = sorted(set(access_levels) - set(access))
        unknown = sorted(set(access) - set(access_levels))
        if missing:
            problems.append(f"mcp access does not name the access levels {missing}")
        if unknown:
            problems.append(f"mcp access names unknown access levels {unknown}")
        for level, classes in access.items():
            if not _strings(classes) or set(classes) - set(TOOL_CLASSES):
                problems.append(
                    f"mcp access {level!r} lists tool classes from {TOOL_CLASSES}"
                )
    credential = block.get(
        "credential", {"header": "Authorization", "scheme": "Bearer"}
    )
    if credential is not None:
        if not isinstance(credential, Mapping) or set(credential) - {
            "header",
            "scheme",
        }:
            problems.append('mcp credential is {"header", "scheme"} or null')
        elif not isinstance(credential.get("header"), str) or not _HEADER.fullmatch(
            credential["header"]
        ):
            problems.append("mcp credential.header must be a header name")
        elif not isinstance(credential.get("scheme", ""), str) or not _SCHEME.fullmatch(
            credential.get("scheme", "")
        ):
            problems.append("mcp credential.scheme must be a word")
    env = block.get("env", {})
    if not isinstance(env, Mapping):
        problems.append("mcp env maps names to values")
    else:
        for name, value in env.items():
            if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
                problems.append(f"mcp env name {name!r} is not an environment name")
            elif _RESERVED_ENV.fullmatch(name):
                problems.append(f"mcp env name {name!r} is reserved")
            if not isinstance(value, str):
                problems.append(f"mcp env {name!r} must be a string")
            else:
                problems += _template_problems(f"mcp env {name!r}", value)
    for key in ("args", "command"):
        value = block.get(key, [])
        if not _strings(value):
            problems.append(f"mcp {key} must be a list of strings")
        else:
            for item in value:
                problems += _template_problems(f"mcp {key}", item)
    in_flight = block.get("max_in_flight_per_binding", 4)
    if (
        isinstance(in_flight, bool)
        or not isinstance(in_flight, int)
        or not 1 <= in_flight <= 64
    ):
        problems.append("mcp max_in_flight_per_binding must be between 1 and 64")
    if block.get("tool_pinning", "warn") not in TOOL_PINNING:
        problems.append(f"mcp tool_pinning must be one of {TOOL_PINNING}")
    return problems


def managed_mcp(spec: Any) -> ManagedMcp | None:
    """The parsed block of a managed MCP driver's spec, else ``None``."""
    service = getattr(spec, "service", None)
    block = getattr(service, "mcp", None)
    if block is None:
        return None
    return ManagedMcp.parse(
        block,
        access_levels=[level.id for level in spec.access_levels],
        front_port=service.port,
    )


__all__ = [
    "FRONT_PATH",
    "PROTOCOLS",
    "READ",
    "TOOL_CLASSES",
    "TOOL_PINNING",
    "WRITE",
    "ManagedMcp",
    "TemplateError",
    "managed_mcp",
    "mcp_problems",
    "pattern_matches",
    "render",
]
