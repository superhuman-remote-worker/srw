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
boundary; the bridge forwards exactly the bytes the front checked. The
bindings of one pod are isolated from each other by user: the server
container runs as root with only the capabilities the bridge needs
(``BRIDGE_CAPABILITIES``), and the bridge starts each process as a user of
its own (from ``BINDING_UID_BASE``) with no capability, no way to gain one,
a private directory as HOME and TMPDIR and private files, so a process can
read neither another binding's environment nor its files, signal it, nor
reach the bridge, which serves the front only on a unix socket in a
directory the front's group alone may enter. Each binding's user may have
at most ``process_limit`` processes and threads (RLIMIT_NPROC), so a fork
burst cannot exhaust the pod's process ids, and the bridge stops and kills
every process of a binding's user, retrying until none is left, before the
user serves another binding.

What the bindings of one pod still share, by design, is its memory: the
server container is one cgroup with one limit (the spec's
``service.resources``), so a server that outgrows it (a runtime without a
heap cap, native memory, a leak) is OOM-killed with every other binding's
process and the bridge, and the pod restarts: one binding's memory is every
binding's blast radius. A Node server's heap can be capped per process
(``NODE_OPTIONS=--max-old-space-size``); ``address_space_mb`` caps a
process's address space (RLIMIT_AS) for a runtime that does not reserve
address space up front. It is opt-in: Node, Go and the JVM reserve far
more than they use and fail under it.

The block, ``ServiceSpec.mcp``, is plain JSON, so it can ride an image label:

``transport``
    ``http``: the server speaks streamable HTTP. ``stdio``: it speaks MCP on
    its stdin and stdout, behind the bridge.
``port``, ``path``
    Where the server listens inside the pod, on ``127.0.0.1`` (for an HTTP
    image that cannot bind loopback, only the front's port is reachable
    from outside the pod, by its NetworkPolicy). Never the front's port. A
    stdio server names no port: the bridge serves the front ``path`` on a
    unix socket only the front's group may reach.
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
    The front's readiness probe has no lease: a stdio probe's process finds
    the placeholder ``srw-probe-placeholder`` in the variable, so a server
    that exits when it is unset still starts and lists its tools.
``env``, ``args``, ``command``
    The server container's environment, arguments and (optional) program,
    else the image's own. ``${config.<key>}`` in an environment value or an
    argument is the connector's config value; they are never secret. A
    template can inject neither a shell nor an argument: never in
    ``command``; in ``args`` only as a whole argument or an option's value
    after ``=`` (``--root=${config.root}``), never after an option that
    takes code (``-c``, ``-e``, ``--eval``, ``--require``, ``--env-file``,
    ``-W``...); never when a shell or a shell script is anywhere in the
    program (``tini -- /bin/sh -c``); never as the command a wrapper
    (``tini``, ``gosu``...) runs or the script, module or package an
    interpreter or a package runner (``node``, ``python``, ``npx``,
    ``uvx``...) runs: a literal one comes first; a whole-argument value may
    not start with ``-``; no value holds a NUL or a line break; and never in
    a variable that loads code or configures a package installer
    (``NODE_OPTIONS``, ``GIT_SSH_COMMAND``, ``npm_config_*``, ``PIP_*``...).
    The same check runs at registration and, over the whole argv (the
    image's entrypoint and command, or ``command``, then ``args``), at
    launch; with no ``command``, templated arguments need an image whose
    entrypoint is an interpreter or a package runner SRW knows. These deny
    lists are a lint, not a sandbox: the spec's author is the trust
    boundary (a spec is reviewed before it is installed), and the lint
    catches the templates a connector's config would turn into code.
    Never ``${config.access}``: the front decides access per lease, so an
    access change starts no new pod. ``${binding.home}`` (stdio only) is the
    process's private directory, for a server that keeps state in a file
    (``MEMORY_FILE_PATH=${binding.home}/memory.json``).
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
``process_limit``, ``address_space_mb``
    ``stdio`` only: the processes and threads each binding's user may have
    (RLIMIT_NPROC, 16 to 4096, default 256), and, opt-in, each binding's
    process's address space in MiB (RLIMIT_AS, 64 to 1048576): see above.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP".
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .env_names import CODE_ENV, CODE_ENV_PREFIXES

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
#: The bridge serves the front on a unix socket in a directory only the
#: front's group may enter (an emptyDir both containers mount), never on a
#: port every process of the pod could reach. The upstream URL's host is a
#: name only: the front dials the socket.
BRIDGE_SOCKET_DIR = "/srw/bridge"
BRIDGE_SOCKET = f"{BRIDGE_SOCKET_DIR}/bridge.sock"
BRIDGE_HOST = "srw-mcp-bridge"
#: The front's user and group (SRW's images run as it): the socket's group.
FRONT_USER = 65532
#: Each binding's process runs as a user of its own, from this one on, with
#: a private directory under the home root as HOME and TMPDIR.
BINDING_UID_BASE = 20000
BINDING_HOME_ROOT = "/srw/home"
#: In a stdio server's environment value or argument: its process's
#: private directory (a file only that binding's process may read).
BINDING_HOME = "${binding.home}"
#: What the bridge (the server container's root) needs to run each process
#: as a user of its own and to clean up after it: switch users, kill and
#: chown that user's processes and files. Pod Security ``baseline`` allows
#: each; the processes themselves keep none.
BRIDGE_CAPABILITIES: tuple[str, ...] = (
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "KILL",
    "SETGID",
    "SETUID",
)
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
#: Programs that run their arguments as code or as another command: a
#: templated argument would be code. A program whose name ends in a shell
#: script's suffix (``/docker-entrypoint.sh``) is one too.
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
_SCRIPT_SUFFIXES: tuple[str, ...] = (".sh", ".bash", ".ksh", ".zsh")
#: A shell under a versioned name (``bash5``, ``zsh5.9``).
_VERSIONED_SHELL = re.compile(r"(ash|bash|dash|fish|ksh|mksh|sh|zsh)[\d.]+\Z")
#: Options whose value is code, the module or package to run, where code
#: or packages come from, or a debugger anyone in the pod could reach.
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
        "-W",
        # The long forms of the short ones above, and the other options of
        # the runners SRW reads whose value is code, where packages or
        # code come from, or a debugger (node, bun, deno, npm and npx, uv
        # and uvx, pipx).
        "--call",
        "--command",
        "--default-index",
        "--env-file",
        "--env-file-if-exists",
        "--eval",
        "--exec",
        "--experimental-loader",
        "--extra-index-url",
        "--find-links",
        "--from",
        "--globalconfig",
        "--import",
        "--import-map",
        "--index-url",
        "--inspect",
        "--inspect-brk",
        "--inspect-port",
        "--inspect-wait",
        "--loader",
        "--node-options",
        "--package",
        "--pip-args",
        "--preload",
        "--print",
        "--python",
        "--registry",
        "--require",
        "--script-shell",
        "--userconfig",
        "--with",
        "--with-editable",
        "--with-requirements",
        "/c",
        "/k",
    }
)
#: Options whose value is code (or where it comes from) for one family of
#: runners only: the same name is ordinary elsewhere (``--config`` of a
#: server, ``-I`` of a compiler), so these are checked when the program is
#: known to be such a runner (at launch, or with mcp command).
RUNNER_CODE_OPTIONS: Mapping[str, frozenset[str]] = {
    # A config file or an import map names the modules to load.
    "bun": frozenset({"--config"}),
    "deno": frozenset({"--config", "--location"}),
    # The class path, the module path and the jar run are code.
    "java": frozenset({"-classpath", "-cp", "-jar", "--class-path", "--module-path"}),
    # -E runs code, -M loads a module, -I adds a module search path.
    "perl": frozenset({"-E", "-I", "-M"}),
    # Code to run per line or before or after them, an ini setting
    # (auto_prepend_file), the php.ini and the script.
    "php": frozenset({"-B", "-E", "-F", "-R", "-c", "-d", "-f"}),
    "ruby": frozenset({"-I"}),
    # Where packages come from: an index, a find-links page, a
    # requirement, a constraint or a spec.
    "uv": frozenset({"-f", "--constraints", "--index", "--overrides", "--spec"}),
}
_RUNNER_FAMILIES: Mapping[str, str] = {
    "bunx": "bun",
    "nodejs": "node",
    "npx": "npm",
    "pipx": "uv",
    "pnpm": "npm",
    "pnpx": "npm",
    "pypy": "python",
    "pypy3": "python",
    "python3": "python",
    "uvx": "uv",
    "yarn": "npm",
}
#: Programs that run the command their arguments name, after the
#: positional arguments they take first (a user, a duration, a
#: directory): the program behind them is the one that runs.
WRAPPERS: Mapping[str, int] = {
    "catatonit": 0,
    "chroot": 1,
    "doas": 0,
    "dumb-init": 0,
    "gosu": 1,
    "nice": 0,
    "nohup": 0,
    "s6-setuidgid": 1,
    "setpriv": 0,
    "setsid": 0,
    "stdbuf": 0,
    "su-exec": 1,
    "sudo": 0,
    "timeout": 1,
    "tini": 0,
    "unbuffer": 0,
    "xargs": 0,
}
#: Interpreters and package runners: each runs the script, module or
#: package its first argument that is no option names, so that argument is
#: never templated. With no ``command`` in the block, templated arguments
#: need an image entrypoint that is one of these: SRW can read no other
#: program's argv.
SCRIPT_RUNNERS: frozenset[str] = frozenset(
    {
        "bun",
        "bunx",
        "deno",
        "java",
        "node",
        "nodejs",
        "npm",
        "npx",
        "perl",
        "php",
        "pipx",
        "pnpm",
        "pnpx",
        "pypy",
        "pypy3",
        "python",
        "python3",
        "ruby",
        "uv",
        "uvx",
        "yarn",
    }
)
_VERSIONED_RUNNER = re.compile(r"(python3|pypy3|node|ruby|php)[.\d]*\d\Z")
#: A runner's subcommands that run what follows them (``deno run``,
#: ``bun x``, ``uv tool run``, ``deno eval``).
_RUNNER_SUBCOMMANDS = frozenset({"dlx", "eval", "exec", "run", "tool", "x"})
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
_STDIO_KEYS = frozenset(
    {
        "stdio_mode",
        "max_bindings_per_pod",
        "idle_seconds",
        "process_limit",
        "address_space_mb",
    }
)
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


def code_env(name: str) -> bool:
    """Whether a variable loads code, names a command or a package index:
    :data:`CODE_ENV` and :data:`CODE_ENV_PREFIXES` (kept in
    ``shared.connectors.env_names``), the list the bridge refuses on its
    own. The server's process is the server's: a workspace's longer list
    (``env_names.loads_code``) does not apply to it."""
    upper = name.upper()
    return upper in CODE_ENV or upper.startswith(CODE_ENV_PREFIXES)


def _shell(item: str) -> bool:
    name = program_name([item])
    return (
        name in SHELLS
        or name.endswith(_SCRIPT_SUFFIXES)
        or _VERSIONED_SHELL.fullmatch(name) is not None
    )


def _runner(name: str) -> bool:
    return name in SCRIPT_RUNNERS or _VERSIONED_RUNNER.fullmatch(name) is not None


def _runner_family(name: str) -> str:
    """A runner's family (``python3.12`` and ``pypy3`` are ``python``)."""
    versioned = _VERSIONED_RUNNER.fullmatch(name)
    base = versioned.group(1) if versioned else name
    return _RUNNER_FAMILIES.get(base, base)


def _real_program(argv: Sequence[str]) -> tuple[str, int] | None:
    """The program that runs behind any wrappers, and its index in
    ``argv``; ``None`` when a template stands where a wrapper's argument or
    command is."""
    index = 0
    while index < len(argv):
        if _TEMPLATE.search(argv[index]):
            return None
        name = program_name([argv[index]])
        if name not in WRAPPERS:
            return name, index
        index += 1
        positionals = WRAPPERS[name]
        while index < len(argv) and (argv[index].startswith("-") or positionals > 0):
            if _TEMPLATE.search(argv[index]):
                return None
            if not argv[index].startswith("-"):
                positionals -= 1
            index += 1
    return None


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
    #: ``stdio``: RLIMIT_NPROC of each binding's user, and its process's
    #: RLIMIT_AS in MiB (``None``: none, the default).
    process_limit: int = 256
    address_space_mb: int | None = None

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
            # A stdio server's bridge serves a unix socket, no port.
            port=int(block.get("port", 0)),
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
            process_limit=int(block.get("process_limit", 256)),
            address_space_mb=(
                int(block["address_space_mb"])
                if block.get("address_space_mb") is not None
                else None
            ),
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
        """Where the front reaches the server (or, by its socket, the
        bridge), in the pod."""
        if self.stdio:
            return f"http://{BRIDGE_HOST}{self.path}"
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

    def program_problem(
        self, program: Sequence[str], *, from_image: bool = True
    ) -> str | None:
        """Why the program the pod runs may not take this block's templated
        arguments, else ``None``: the registration check again, over the
        whole argv (the image's own entrypoint and command are known only at
        launch; ``from_image`` is false when the block names ``command``)."""
        problems = _argv_problems(program, self.args, from_image=from_image)
        return problems[0] if problems else None

    def bridge_command(
        self,
        program: Sequence[str],
        *,
        socket: str = BRIDGE_SOCKET,
        socket_group: int | None = FRONT_USER,
        uid_base: int = BINDING_UID_BASE,
        home_root: str = BINDING_HOME_ROOT,
    ) -> list[str]:
        """The server container's command for a stdio server: the bridge,
        serving the front on its socket at the block's path and running
        each binding's process as a user of its own, then the server's own
        program. The keywords are for a test outside a pod (no root: every
        process runs as the bridge's user, ``uid_base`` 0)."""
        command = [BRIDGE_PATH, "serve", "--socket", socket]
        if socket_group is not None:
            command += ["--socket-group", str(socket_group)]
        command += [
            "--uid-base",
            str(uid_base),
            "--home-root",
            home_root,
            "--path",
            self.path,
            "--max-processes",
            str(self.max_bindings_per_pod),
            "--idle",
            f"{self.idle_seconds}s",
            "--process-limit",
            str(self.process_limit),
        ]
        if self.address_space_mb is not None:
            command += ["--address-space-mb", str(self.address_space_mb)]
        if self.credential_env:
            command += ["--credential-env", self.credential_env]
        return [*command, "--", *program]

    def front_config(self, *, socket: str = BRIDGE_SOCKET) -> dict[str, Any]:
        """What the front reads from the pod's request file (``mcp``)."""
        if self.stdio:
            credential = {"env": self.credential_env} if self.credential_env else None
        else:
            credential = (
                {"header": self.credential_header, "scheme": self.credential_scheme}
                if self.credential_header
                else None
            )
        config: dict[str, Any] = {
            "transport": self.transport,
            "upstream": self.upstream,
            "protocol": self.protocol,
            "tools": {READ: list(self.read_tools)},
            "access": {level: list(classes) for level, classes in self.access.items()},
            "credential": credential,
            "max_in_flight_per_binding": self.max_in_flight_per_binding,
            "tool_pinning": self.tool_pinning,
        }
        if self.stdio:
            config["socket"] = socket
        return config


def _strings(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and all(
        isinstance(item, str) for item in value
    )


def _template_problems(where: str, value: str, *, stdio: bool) -> list[str]:
    stripped = _TEMPLATE.sub("", value)
    if BINDING_HOME in stripped:
        if not stdio:
            return [
                f"{where} names {BINDING_HOME}, a stdio process's private "
                "directory: an http server has none"
            ]
        stripped = stripped.replace(BINDING_HOME, "")
    if "${" in stripped:
        return [f"{where} has a placeholder that is not ${{config.<key>}}"]
    named = sorted(set(_TEMPLATE.findall(value)) & _UNTEMPLATED)
    if named:
        return [
            f"{where} names config.{named[0]}: the front decides access per "
            "lease, never the server's configuration"
        ]
    return []


def _argv_problems(
    program: Sequence[str], args: Sequence[str], *, from_image: bool = False
) -> list[str]:
    """Templates in the argv (``program`` then ``args``) that could inject a
    shell, an option, a command or a script.

    ``program`` is the block's ``command`` (registration, launch) or the
    image's entrypoint and command (``from_image``, at launch); empty, the
    program is not known yet and only the arguments are checked.
    """
    problems = [
        f"mcp command {item!r} holds a template: the program is the spec's"
        for item in program
        if _TEMPLATE.search(item)
    ]
    argv = [*program, *args]
    templated = [
        index
        for index in range(len(program), len(argv))
        if _TEMPLATE.search(argv[index])
    ]
    if templated and any(_shell(item) for item in program):
        problems.append(
            "mcp args are templated, but the program is a shell or runs one: "
            "a template is never code"
        )
    # A runner's own code options count once the program is known.
    found = _real_program(argv) if program else None
    code_options = CODE_OPTIONS
    if found is not None and _runner(found[0]):
        code_options = code_options | RUNNER_CODE_OPTIONS.get(
            _runner_family(found[0]), frozenset()
        )
    for index in templated:
        item = argv[index]
        if not _ARG_TEMPLATE.fullmatch(item):
            problems.append(
                f"mcp args {item!r}: a template is a whole argument or an "
                "option's value after '='"
            )
        before = argv[index - 1] if index else ""
        option = item.split("=", 1)[0] if "=" in item else ""
        if option in code_options or before in code_options:
            which = option if option in code_options else before
            problems.append(
                f"mcp args {item!r} is the value of {which!r}: a template is never code"
            )
    if not templated or not program or problems:
        return problems
    if found is None:
        problems.append(
            "mcp args put a template where a wrapper's command is: a "
            "template is never the program"
        )
        return problems
    name, index = found
    if not _runner(name):
        if from_image:
            problems.append(
                f"mcp args are templated, but the image's program {name!r} is "
                "no interpreter or package runner SRW can check: name the "
                "server's program in mcp command"
            )
        return problems
    for item in argv[index + 1 :]:
        if item.startswith("-") or item in _RUNNER_SUBCOMMANDS:
            continue
        if _TEMPLATE.search(item):
            problems.append(
                f"mcp args {item!r} would be the script, module or package "
                f"{name!r} runs: a literal one comes first"
            )
        break
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
        if _RESERVED_ENV.fullmatch(name) or code_env(name):
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
    if stdio:
        if "port" in block:
            problems.append(
                "mcp port is an http server's: a stdio server's bridge serves "
                "the front on a unix socket"
            )
    elif isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
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
                problems += _template_problems(f"mcp env {name!r}", value, stdio=stdio)
                if isinstance(name, str) and code_env(name) and _TEMPLATE.search(value):
                    problems.append(
                        f"mcp env {name!r} loads code or names a command or a "
                        "package index: no config value is templated into it"
                    )
    lists = {}
    for key in ("args", "command"):
        value = block.get(key, [])
        if not _strings(value):
            problems.append(f"mcp {key} must be a list of strings")
        else:
            lists[key] = value
            for item in value:
                problems += _template_problems(f"mcp {key}", item, stdio=stdio)
    if len(lists) == 2:
        problems += _argv_problems(lists["command"], lists["args"])
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
        if not _bounded(block.get("process_limit", 256), 16, 4096):
            problems.append("mcp process_limit must be between 16 and 4096")
        address_space = block.get("address_space_mb")
        if address_space is not None and not _bounded(address_space, 64, 1048576):
            problems.append("mcp address_space_mb must be between 64 and 1048576 (MiB)")
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
    "BINDING_HOME",
    "BINDING_HOME_ROOT",
    "BINDING_UID_BASE",
    "BRIDGE_CAPABILITIES",
    "BRIDGE_DIR",
    "BRIDGE_HOST",
    "BRIDGE_PATH",
    "BRIDGE_SOCKET",
    "BRIDGE_SOCKET_DIR",
    "CODE_ENV",
    "CODE_ENV_PREFIXES",
    "CODE_OPTIONS",
    "RUNNER_CODE_OPTIONS",
    "FRONT_PATH",
    "FRONT_USER",
    "PROTOCOLS",
    "RESERVED_HEADERS",
    "READ",
    "SCRIPT_RUNNERS",
    "SHELLS",
    "STDIO_MODES",
    "TOOL_CLASSES",
    "TOOL_PINNING",
    "TRANSPORTS",
    "WRAPPERS",
    "WRITE",
    "ManagedMcp",
    "TemplateError",
    "code_env",
    "managed_mcp",
    "mcp_problems",
    "pattern_matches",
    "program_name",
    "render",
]
