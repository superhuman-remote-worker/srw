"""Managed MCP servers: the ``mcp`` block of a service driver's spec (D5a, D5b).

A managed MCP driver is an MCP server image SRW hosts as a service driver:
one shared pod per connector, image digest and credential generation
(decision 7), made of the server and SRW's **front**. The front is the pod's
only exposed port (``srw-driver``). It authenticates every request with the
caller's lease token (the exchange's introspection route), hands the server
the upstream credential the exchange returns, and enforces the binding's
access level by hiding and refusing the tools the level does not allow. The
agent process is the client (``callers: ["harness"]``) and holds only the
lease token, so the upstream credential never reaches the agent pod.

A **stdio** image (D5b) runs unchanged behind SRW's **stdio bridge**
(drivers/mcp-bridge), which an init container copies into the pod from the
front's image and which becomes the server container's command, with the
image's own program as its arguments. A stdio server serves one client at a
time, so the pod is shared but the bridge runs one process of the server per
binding: started on the binding's first request, serving the binding's
sessions one at a time (initialized once), stopped when its lease ends (the
front tells the bridge), when it exits or when it is idle, at most
``max_bindings_per_pod`` at once. The front stays the authorization
boundary; the bridge forwards exactly the bytes the front checked.

The block, ``ServiceSpec.mcp``, is plain JSON, so it can ride an image label:

``transport``
    ``http``: the server speaks streamable HTTP. ``stdio``: it speaks MCP on
    its stdin and stdout, behind the bridge.
``port``, ``path``
    Where the server (``http``) or the bridge (``stdio``) listens inside the
    pod, on ``127.0.0.1`` (for an HTTP image that cannot bind loopback, only
    the front's port is reachable from outside the pod, by its
    NetworkPolicy). Never the front's port.
``protocol``
    ``legacy`` (initialize-based 2025 sessions) or ``both`` (it also speaks
    the stateless 2026-07-28 protocol). SRW's own client is session-based
    (``mcp<2``) and the front probes with ``initialize``, so a server that
    speaks only the 2026 protocol (``modern``) is refused for now.
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
    How the server receives the upstream credential, or ``null`` for a
    server that needs none. ``http``: on each request, in a header,
    ``{"header": "Authorization", "scheme": "Bearer"}`` (the default;
    ``scheme`` may be empty), never a header the front forwards or sets or
    the transport owns (``Mcp-Session-Id``, ``Host``...). ``stdio``: in an
    environment variable of each binding's process, ``{"env": "NAME"}``
    (stdio servers read credentials from their environment), never a name
    that loads code (``NODE_OPTIONS``, ``PYTHONPATH``, ``PATH``...). Either
    way it comes from the lease exchange per binding, never from the pod.
``env``, ``args``, ``command``
    The server container's environment, arguments and (optional) program,
    else the image's own. ``${config.<key>}`` in an environment value or an
    argument is the connector's config value; they are never secret. A
    template can inject neither a shell nor an argument: never in
    ``command``; in ``args`` only as a whole argument or an option's value
    after ``=`` (``--root=${config.root}``), never after an option that
    takes code (``-c``, ``-e``, ``--eval``...) and never when the program is
    a shell; a whole-argument value may not start with ``-``; no value
    holds a NUL or a line break; and never in a variable that loads code.
    Never ``${config.access}``: the front decides access per lease, so an
    access change starts no new pod.
``max_in_flight_per_binding``
    Calls one binding may have open at once (the front answers 429 past it).
``tool_pinning``
    ``warn`` or ``block`` when the server's tool list changes under one
    digest during a pod's life.
``stdio_mode``, ``max_bindings_per_pod``, ``idle_seconds``
    ``stdio`` only: ``process-per-binding`` (the one mode built; a
    ``shared-process`` mode for stateless tool-only servers is not), how
    many binding processes the pod runs at once (the bridge answers 503
    past it), and how long a binding's process may go without a request or
    an open stream before it stops.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP".
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: The path the front serves MCP at, on the ``srw-driver`` port.
FRONT_PATH = "/mcp"
READ = "read"
WRITE = "write"
TOOL_CLASSES: tuple[str, ...] = (READ, WRITE)
TRANSPORTS: tuple[str, ...] = ("http", "stdio")
#: Protocols a managed server may declare (``modern`` alone is refused
#: until SRW's client and the front's probe speak the 2026 protocol).
PROTOCOLS: tuple[str, ...] = ("legacy", "both")
#: How the stdio bridge runs a stdio server: one process per binding.
STDIO_MODES: tuple[str, ...] = ("process-per-binding",)
#: Where the init container installs the stdio bridge in the server
#: container (drivers/mcp-bridge ``install``).
BRIDGE_DIR = "/srw/bin"
BRIDGE_PATH = f"{BRIDGE_DIR}/srw-mcp-bridge"
#: Headers the front forwards or sets, or the transport owns: the
#: credential may never be written over one (lowercase; drivers/mcp-front
#: reservedHeaders).
RESERVED_HEADERS: frozenset[str] = frozenset(
    {
        "accept",
        "connection",
        "content-length",
        "content-type",
        "cookie",
        "host",
        "keep-alive",
        "last-event-id",
        "mcp-method",
        "mcp-name",
        "mcp-protocol-version",
        "mcp-session-id",
        "origin",
        "proxy-connection",
        "srw-bridge-binding",
        "srw-bridge-credential",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
#: Environment variables a runtime reads code or its search path from: no
#: config value is templated into one and no credential is delivered in one
#: (drivers/mcp-bridge codeEnv holds the same list).
CODE_ENV: frozenset[str] = frozenset(
    {
        "BASH_ENV",
        "BASHOPTS",
        "CLASSPATH",
        "ENV",
        "GCONV_PATH",
        "HOME",
        "IFS",
        "JAVA_TOOL_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "_JAVA_OPTIONS",
        "NODE_OPTIONS",
        "NODE_PATH",
        "PATH",
        "PERL5LIB",
        "PERL5OPT",
        "PERLLIB",
        "PROMPT_COMMAND",
        "PS4",
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "RUBYLIB",
        "RUBYOPT",
        "SHELLOPTS",
        "ZDOTDIR",
    }
)
#: Programs that run their arguments as code or as another command: a
#: templated argument would be code.
SHELLS: frozenset[str] = frozenset(
    {
        "ash",
        "bash",
        "busybox",
        "cmd",
        "cmd.exe",
        "dash",
        "env",
        "fish",
        "ksh",
        "mksh",
        "powershell",
        "pwsh",
        "sh",
        "zsh",
    }
)
#: Options whose next argument is code or the module to run.
CODE_OPTIONS: frozenset[str] = frozenset(
    {
        "-c",
        "-e",
        "-ec",
        "-ic",
        "-lc",
        "-m",
        "-p",
        "-r",
        "--command",
        "--eval",
        "--exec",
        "--import",
        "--loader",
        "--print",
        "--require",
        "/c",
        "/k",
    }
)
#: Config keys a server's arguments and environment may not name.
_UNTEMPLATED = frozenset({"access"})
TOOL_PINNING: tuple[str, ...] = ("warn", "block")
#: A tool name or pattern: the characters MCP tool names use, plus ``*``.
_PATTERN = re.compile(r"[A-Za-z0-9_.*-]{1,128}\Z")
_HEADER = re.compile(r"[A-Za-z0-9-]{1,64}\Z")
_SCHEME = re.compile(r"[A-Za-z0-9._-]{0,32}\Z")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_PATH = re.compile(r"/[A-Za-z0-9._~/-]{0,127}\Z")
_TEMPLATE = re.compile(r"\$\{config\.([a-z][a-z0-9_]*)\}")
#: Where a template may stand in an argument: the whole argument, or an
#: option's value after ``=``.
_ARG_TEMPLATE = re.compile(
    r"(-{1,2}[A-Za-z0-9][A-Za-z0-9_.-]*=)?\$\{config\.([a-z][a-z0-9_]*)\}\Z"
)
#: Environment names the server may not be given: SRW's own, and the
#: loader's, which would run code before the image's program.
_RESERVED_ENV = re.compile(r"(SRW_|LD_|DYLD_).*", re.IGNORECASE)
_STDIO_KEYS = frozenset({"stdio_mode", "max_bindings_per_pod", "idle_seconds"})
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
        *_STDIO_KEYS,
    }
)
_HTTP_CREDENTIAL = {"header": "Authorization", "scheme": "Bearer"}


class TemplateError(ValueError):
    """A ``${config.<key>}`` the connector's config cannot fill safely."""


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

    A value must be a string, a number or a boolean, without a NUL or a line
    break; a missing key or another value is a :class:`TemplateError`.
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
            text = str(found)
            if any(ch in text for ch in "\x00\r\n"):
                raise TemplateError(f"config.{key} holds a NUL or a line break")
            return text
        raise TemplateError(f"config.{key} is not a string or a number")

    return _TEMPLATE.sub(value, template)


def program_name(program: Sequence[str]) -> str:
    """The base name of a program's executable (``/bin/sh`` -> ``sh``)."""
    return posixpath.basename(program[0]) if program else ""


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
    #: ``stdio``: the variable each binding's process gets its credential in.
    credential_env: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    args: tuple[str, ...] = ()
    command: tuple[str, ...] = ()
    max_in_flight_per_binding: int = 4
    tool_pinning: str = "warn"
    stdio_mode: str = "process-per-binding"
    max_bindings_per_pod: int = 8
    idle_seconds: int = 600

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
        transport = str(block.get("transport", "http"))
        stdio = transport == "stdio"
        credential = block.get("credential", None if stdio else _HTTP_CREDENTIAL)
        return cls(
            transport=transport,
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
                str(credential["header"])
                if credential is not None and not stdio
                else None
            ),
            credential_scheme=(
                str(credential.get("scheme", ""))
                if credential is not None and not stdio
                else ""
            ),
            credential_env=(
                str(credential["env"]) if credential is not None and stdio else None
            ),
            env={str(k): str(v) for k, v in (block.get("env") or {}).items()},
            args=tuple(str(item) for item in block.get("args") or ()),
            command=tuple(str(item) for item in block.get("command") or ()),
            max_in_flight_per_binding=int(block.get("max_in_flight_per_binding", 4)),
            tool_pinning=str(block.get("tool_pinning", "warn")),
            stdio_mode=str(block.get("stdio_mode", "process-per-binding")),
            max_bindings_per_pod=int(block.get("max_bindings_per_pod", 8)),
            idle_seconds=int(block.get("idle_seconds", 600)),
        )

    @property
    def stdio(self) -> bool:
        return self.transport == "stdio"

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
        """Where the front reaches the server (or the bridge), in the pod."""
        return f"http://127.0.0.1:{self.port}{self.path}"

    def server_env(self, config: Mapping[str, Any]) -> dict[str, str]:
        return {name: render(value, config) for name, value in self.env.items()}

    def server_args(self, config: Mapping[str, Any]) -> list[str]:
        """The arguments with the connector's config filled in; a whole
        templated argument whose value would read as an option is a
        :class:`TemplateError`."""
        out = []
        for value in self.args:
            rendered = render(value, config)
            if _TEMPLATE.fullmatch(value) and rendered.startswith("-"):
                key = _TEMPLATE.fullmatch(value).group(1)
                raise TemplateError(
                    f"config.{key} would be read as an option of the server"
                )
            out.append(rendered)
        return out

    def program_problem(self, program: Sequence[str]) -> str | None:
        """Why the server's program may not take this block's templated
        arguments (a shell runs them as code), else ``None``. The image's own
        program is known only at launch."""
        templated = any(_TEMPLATE.search(arg) for arg in self.args)
        if templated and program_name(program) in SHELLS:
            return (
                f"the server's program {program_name(program)!r} is a shell: "
                "it may not take templated arguments"
            )
        return None

    def bridge_command(self, program: Sequence[str]) -> list[str]:
        """The server container's command for a stdio server: the bridge,
        serving the front on the block's loopback port and path, then the
        server's own program."""
        command = [
            BRIDGE_PATH,
            "serve",
            "--listen",
            f"127.0.0.1:{self.port}",
            "--path",
            self.path,
            "--max-processes",
            str(self.max_bindings_per_pod),
            "--idle",
            f"{self.idle_seconds}s",
        ]
        if self.credential_env:
            command += ["--credential-env", self.credential_env]
        return [*command, "--", *program]

    def front_config(self) -> dict[str, Any]:
        """What the front reads from the pod's request file (``mcp``)."""
        if self.stdio:
            credential = {"env": self.credential_env} if self.credential_env else None
        else:
            credential = (
                {"header": self.credential_header, "scheme": self.credential_scheme}
                if self.credential_header
                else None
            )
        return {
            "transport": self.transport,
            "upstream": self.upstream,
            "protocol": self.protocol,
            "tools": {READ: list(self.read_tools)},
            "access": {level: list(classes) for level, classes in self.access.items()},
            "credential": credential,
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
    named = sorted(set(_TEMPLATE.findall(value)) & _UNTEMPLATED)
    if named:
        return [
            f"{where} names config.{named[0]}: the front decides access per "
            "lease, never the server's configuration"
        ]
    return []


def _argument_problems(args: Sequence[str], command: Sequence[str]) -> list[str]:
    """Templates that could inject a shell or an argument."""
    problems = [
        f"mcp command {item!r} holds a template: the program is the spec's"
        for item in command
        if _TEMPLATE.search(item)
    ]
    templated = False
    for index, item in enumerate(args):
        if not _TEMPLATE.search(item):
            continue
        templated = True
        if not _ARG_TEMPLATE.fullmatch(item):
            problems.append(
                f"mcp args {item!r}: a template is a whole argument or an "
                "option's value after '='"
            )
        before = args[index - 1] if index else (command[-1] if command else "")
        option = item.split("=", 1)[0] if "=" in item else ""
        if before in CODE_OPTIONS or option in CODE_OPTIONS:
            problems.append(
                f"mcp args {item!r} is the value of {option or before!r}: a "
                "template is never code"
            )
    if templated and (
        program_name(command) in SHELLS
        or any(program_name([item]) in SHELLS for item in command[1:])
    ):
        problems.append(
            "mcp args are templated, but the program is a shell: a template "
            "is never code"
        )
    return problems


def _credential_problems(credential: Any, *, stdio: bool, env: Any) -> list[str]:
    if credential is None:
        return []
    if stdio:
        if not isinstance(credential, Mapping) or set(credential) != {"env"}:
            return [
                'a stdio server\'s mcp credential is {"env": "NAME"}: its '
                "process reads it from its environment"
            ]
        name = credential["env"]
        if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
            return ["mcp credential.env must be an environment name"]
        if _RESERVED_ENV.fullmatch(name) or name.upper() in CODE_ENV:
            return [f"mcp credential.env {name!r} is reserved or loads code"]
        if isinstance(env, Mapping) and name in env:
            return [f"mcp credential.env {name!r} is also set in mcp env"]
        return []
    if not isinstance(credential, Mapping) or set(credential) - {"header", "scheme"}:
        return ['mcp credential is {"header", "scheme"} or null']
    if not isinstance(credential.get("header"), str) or not _HEADER.fullmatch(
        credential["header"]
    ):
        return ["mcp credential.header must be a header name"]
    if credential["header"].lower() in RESERVED_HEADERS:
        return [
            f"mcp credential.header {credential['header']!r} is a header the "
            "front forwards or sets, or the transport owns"
        ]
    if not isinstance(credential.get("scheme", ""), str) or not _SCHEME.fullmatch(
        credential.get("scheme", "")
    ):
        return ["mcp credential.scheme must be a word"]
    return []


def _bounded(value: Any, low: int, high: int) -> bool:
    return (
        not isinstance(value, bool) and isinstance(value, int) and low <= value <= high
    )


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
    stdio = transport == "stdio"
    if transport not in TRANSPORTS:
        problems.append(f"mcp transport {transport!r} is not one of {TRANSPORTS}")
    port = block.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        problems.append("mcp port must be the server's port number")
    elif front_port is not None and port == front_port:
        problems.append("mcp port is the front's port; the server needs its own")
    path = block.get("path", "/mcp")
    if not isinstance(path, str) or not _PATH.fullmatch(path):
        problems.append("mcp path must be an absolute URL path")
    elif stdio and path.startswith("/srw/"):
        problems.append("mcp path /srw/ is the stdio bridge's own")
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
    env = block.get("env", {})
    problems += _credential_problems(
        block.get("credential", None if stdio else _HTTP_CREDENTIAL),
        stdio=stdio,
        env=env,
    )
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
                if (
                    isinstance(name, str)
                    and name.upper() in CODE_ENV
                    and _TEMPLATE.search(value)
                ):
                    problems.append(
                        f"mcp env {name!r} loads code: no config value is "
                        "templated into it"
                    )
    lists = {}
    for key in ("args", "command"):
        value = block.get(key, [])
        if not _strings(value):
            problems.append(f"mcp {key} must be a list of strings")
        else:
            lists[key] = value
            for item in value:
                problems += _template_problems(f"mcp {key}", item)
    if len(lists) == 2:
        problems += _argument_problems(lists["args"], lists["command"])
    in_flight = block.get("max_in_flight_per_binding", 4)
    if not _bounded(in_flight, 1, 64):
        problems.append("mcp max_in_flight_per_binding must be between 1 and 64")
    if block.get("tool_pinning", "warn") not in TOOL_PINNING:
        problems.append(f"mcp tool_pinning must be one of {TOOL_PINNING}")
    if not stdio:
        problems += [
            f"mcp {key} is for stdio servers only"
            for key in sorted(_STDIO_KEYS & set(block))
        ]
    else:
        mode = block.get("stdio_mode", "process-per-binding")
        if mode == "shared-process":
            problems.append(
                "mcp stdio_mode shared-process is not built: every binding gets "
                "a process of its own (process-per-binding)"
            )
        elif mode not in STDIO_MODES:
            problems.append(f"mcp stdio_mode must be one of {STDIO_MODES}")
        if not _bounded(block.get("max_bindings_per_pod", 8), 1, 64):
            problems.append("mcp max_bindings_per_pod must be between 1 and 64")
        if not _bounded(block.get("idle_seconds", 600), 60, 86400):
            problems.append("mcp idle_seconds must be between 60 and 86400")
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
    "BRIDGE_DIR",
    "BRIDGE_PATH",
    "CODE_ENV",
    "CODE_OPTIONS",
    "FRONT_PATH",
    "PROTOCOLS",
    "RESERVED_HEADERS",
    "READ",
    "SHELLS",
    "STDIO_MODES",
    "TOOL_CLASSES",
    "TOOL_PINNING",
    "TRANSPORTS",
    "WRITE",
    "ManagedMcp",
    "TemplateError",
    "managed_mcp",
    "mcp_problems",
    "pattern_matches",
    "program_name",
    "render",
]
