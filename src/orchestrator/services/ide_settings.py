"""Per-user code-server IDE settings store.

Persists a user's small code-server config (``settings.json``,
``keybindings.json``, snippets) under ``users.settings['ide']['files']`` and
reconciles config pulled from workspaces using filesystem mtimes — newest wins,
per file.

Storage shape::

    users.settings = {
        "ide": {"files": {
            "settings.json":        {"content": "...", "mtime": 1716800000.0},
            "keybindings.json":     {"content": "...", "mtime": 1716800012.0},
            "snippets/python.json": {"content": "...", "mtime": 1716799000.0},
        }},
        # ...other unrelated user settings live alongside "ide"...
    }

IDE components are merged atomically so a config/extensions reconciliation
cannot erase a concurrently published content-addressed profile pointer.

Conflict resolution is purely mtime-based: an applied file wins only if its mtime
is strictly newer than what is stored. That makes ``apply_pulled_files`` order-
independent across workspaces — the newest edit wins no matter which workspace is
reconciled first.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import shlex
from typing import Any, Awaitable, Callable, Optional

from orchestrator.services.blocking_effect import (
    joined_async_call,
    joined_blocking_call,
)
from orchestrator.services.subprocess_effect import (
    communicate_bounded,
    create_owned_subprocess_exec,
    stop_and_reap,
)

logger = logging.getLogger(__name__)

# code-server's user-data dir (set via --user-data-dir in the workspace image).
CODE_SERVER_USER_DIR = "/var/lib/code-server/User"
# Top-level config files we track (snippets/* are discovered dynamically).
TRACKED_FILES = ("settings.json", "keybindings.json")
SNIPPETS_SUBDIR = "snippets"

# Extensions live in a separate tree from the User config dir.
EXTENSIONS_DIR = "/var/lib/code-server/extensions"
GLOBAL_STORAGE_DIR = f"{CODE_SERVER_USER_DIR}/globalStorage"
# Sentinel the entrypoint waits on while the orchestrator streams license/
# globalStorage state into a freshly-provisioned workspace (Phase B).
SEED_STATE_SENTINEL = "/var/lib/code-server/.ide-seed-state-done"

# SSH ports: workspace containers run sshd on 30022; VMs on 22.
DEFAULT_WS_SSH_PORT = 30022
DEFAULT_VM_SSH_PORT = 22

# Markers framing each file in the remote pull script's stdout. Content is
# base64-encoded between them so arbitrary bytes/newlines survive transport.
_FILE_MARKER = "__SRWFILE__"
_END_MARKER = "__SRWEND__"
_SSH_STDOUT_MAX_BYTES = 8 * 1024 * 1024
_SSH_STDERR_MAX_BYTES = 64 * 1024
_PROFILE_COMPRESSED_MAX_BYTES = 512 * 1024 * 1024
_PROFILE_UNCOMPRESSED_MAX_BYTES = 2 * 1024 * 1024 * 1024


def build_pull_script() -> str:
    """Remote shell script that emits each tracked config file with its mtime.

    Output is a sequence of ``__SRWFILE__<name>\\t<mtime>`` headers, the file's
    base64 body, and an ``__SRWEND__`` terminator. Names are relative to the
    code-server User dir, so snippets come through as ``snippets/<file>``.
    """
    return (
        f"cd {CODE_SERVER_USER_DIR} 2>/dev/null || exit 0\n"
        f"for f in {' '.join(TRACKED_FILES)} {SNIPPETS_SUBDIR}/*; do\n"
        '  [ -f "$f" ] || continue\n'
        '  m=$(stat -c %Y "$f" 2>/dev/null) || continue\n'
        f'  printf \'{_FILE_MARKER}%s\\t%s\\n\' "$f" "$m"\n'
        '  base64 "$f"\n'
        f"  printf '{_END_MARKER}\\n'\n"
        "done\n"
    )


def parse_pull_output(stdout: str) -> dict[str, dict]:
    """Parse :func:`build_pull_script` output into ``{name: {content, mtime}}``.

    Malformed sections (bad base64, non-numeric mtime, non-utf8) are skipped
    rather than raising — a single corrupt file must not abort the whole pull.
    """
    result: dict[str, dict] = {}
    lines = stdout.split("\n")
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if not line.startswith(_FILE_MARKER):
            i += 1
            continue
        name, _, mtime_s = line[len(_FILE_MARKER) :].partition("\t")
        i += 1
        body: list[str] = []
        while i < n and lines[i] != _END_MARKER:
            body.append(lines[i].strip())
            i += 1
        try:
            content = base64.b64decode("".join(body)).decode("utf-8")
            mtime = float(mtime_s)
        except (binascii.Error, ValueError, UnicodeDecodeError):
            logger.warning("ide_settings: skipping unparseable config file %r", name)
        else:
            if name:
                result[name] = {"content": content, "mtime": mtime}
        i += 1  # step past the end marker
    return result


_EXT_THEME_FLAG = "THEME"


def build_extensions_list_script() -> str:
    """Remote shell: emit one ``<publisher>.<name>@<version>\\t<THEME|->`` line per
    installed extension. The theme flag is set when the extension's package.json
    declares a ``"themes"`` contribution, so the seed step can install theme
    providers first. Parses package.json with line-wise sed (top-level fields are
    one-per-line in published extensions); robust enough for ordering/inventory.
    """
    return (
        f"cd {EXTENSIONS_DIR} 2>/dev/null || exit 0\n"
        "for d in */ ; do\n"
        '  pj="${d%/}/package.json"\n'
        '  [ -f "$pj" ] || continue\n'
        '  pub=$(sed -n \'s/.*"publisher"[: ]*"\\([^"]*\\)".*/\\1/p\' "$pj" | head -1)\n'
        '  nm=$(sed -n \'s/.*"name"[: ]*"\\([^"]*\\)".*/\\1/p\' "$pj" | head -1)\n'
        '  ver=$(sed -n \'s/.*"version"[: ]*"\\([^"]*\\)".*/\\1/p\' "$pj" | head -1)\n'
        '  [ -n "$pub" ] && [ -n "$nm" ] && [ -n "$ver" ] || continue\n'
        '  flag="-"\n'
        f'  grep -q \'"themes"\' "$pj" && flag="{_EXT_THEME_FLAG}"\n'
        '  printf \'%s.%s@%s\\t%s\\n\' "$pub" "$nm" "$ver" "$flag"\n'
        "done\n"
    )


def parse_extensions_list(stdout: str) -> dict[str, dict]:
    """Parse :func:`build_extensions_list_script` output into
    ``{ext_id: {"version": str, "theme": bool}}``. Lines without a tab are skipped.
    """
    result: dict[str, dict] = {}
    for line in stdout.split("\n"):
        if "\t" not in line:
            continue
        id_ver, _, flag = line.partition("\t")
        ext_id, _, version = id_ver.rpartition("@")
        if not ext_id or not version:
            continue
        result[ext_id] = {"version": version, "theme": flag.strip() == _EXT_THEME_FLAG}
    return result


def build_signature_script() -> str:
    """Remote shell: a cheap content signature over the extensions dir and
    globalStorage (paths + sizes + mtimes), hashed. Used to skip byte-copy when
    nothing changed. ``find -printf`` is GNU; falls back to ``ls -laR`` if absent.
    """
    targets = f"{EXTENSIONS_DIR} {GLOBAL_STORAGE_DIR}"
    return (
        f"if find {targets} -maxdepth 0 >/dev/null 2>&1; then\n"
        f"  (find {targets} -printf '%p %s %T@\\n' 2>/dev/null "
        f"   || ls -laR {targets} 2>/dev/null) | sort | sha256sum\n"
        "else echo ''; fi\n"
    )


def parse_signature(stdout: str) -> str:
    """Take the first whitespace-delimited token (the sha256 hex) from the
    signature script's stdout; empty string when there's nothing to hash."""
    return stdout.strip().split()[0] if stdout.strip() else ""


OPEN_VSX_API = "https://open-vsx.org/api"

# Fetch signature: (url) -> http_status_int
VsxFetch = Callable[[str], Awaitable[int]]


async def _default_vsx_fetch(url: str) -> int:
    """GET an Open VSX API URL; return the HTTP status. Runs urllib in a thread to
    stay dependency-light (no aiohttp import at module load)."""
    import urllib.error
    import urllib.request

    def _get() -> int:
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    return await joined_blocking_call(_get)


class OpenVsxClassifier:
    """Classify an extension as installable from Open VSX (``"openvsx"``) or
    requiring byte-copy (``"bytes"``). Caches by (id, version). On any error,
    defaults to ``"bytes"`` — the safe side (we'll carry the bytes ourselves)."""

    def __init__(self, fetch: Optional[VsxFetch] = None) -> None:
        self._fetch = fetch or _default_vsx_fetch
        self._cache: dict[tuple[str, str], str] = {}

    async def classify(self, ext_id: str, version: str) -> str:
        key = (ext_id, version)
        if key in self._cache:
            return self._cache[key]
        ns, _, name = ext_id.partition(".")
        url = f"{OPEN_VSX_API}/{ns}/{name}/{version}"
        try:
            status = await self._fetch(url)
            source = "openvsx" if status == 200 else "bytes"
        except Exception:  # noqa: BLE001 — classification must never raise
            source = "bytes"
        self._cache[key] = source
        return source


def resolve_ssh_target(context: dict) -> Optional[tuple[str, int]]:
    """Resolve a workspace context to an ``(ssh_host, ssh_port)`` pair.

    Container → restored IDE session → VM. For container workspaces the
    stable per-workspace Service DNS (``workspace_container.host``, the
    headless Service from `7fb9e9e2`) is preferred over the ephemeral
    ``pod_ip``: a workspace pod that restarted keeps its Service name but not
    its IP, and dialing the stale IP was the residual "No route to host"
    source in the sweeper (knowledge-history/done/
    ide_settings_sweeper_probes_stale_workspace_endpoints.md, residual
    hardening). ``pod_ip`` stays as the fallback for legacy context rows that
    predate the headless Service. Returns None when no reachable target
    exists.
    """
    ws = context.get("workspace_container") or {}
    vm = context.get("vm") or {}
    ide = context.get("ide_session") or {}

    host = (
        ws.get("host")
        or ws.get("pod_ip")
        or ide.get("pod_ip")
        or vm.get("ssh_host")
        or vm.get("pod_ip")
    )
    if not host:
        return None

    if ws.get("host") or ws.get("pod_ip"):
        port = ws.get("port") or DEFAULT_WS_SSH_PORT
    elif ide.get("pod_ip"):
        port = ide.get("ssh_port") or DEFAULT_WS_SSH_PORT
    else:
        port = vm.get("ssh_port") or DEFAULT_VM_SSH_PORT
    return host, int(port)


def _shq(s: str) -> str:
    """POSIX single-quote-escape ``s`` for safe embedding in a shell command."""
    return "'" + s.replace("'", "'\\''") + "'"


def build_seed_script(files: dict[str, dict]) -> str:
    """Shell script that writes ``files`` into the code-server User dir.

    Content is delivered base64-encoded (binary-safe), each file's mtime is set
    with ``touch -d @<mtime>`` so an untouched seeded session never looks newer
    than the store, and ownership is reset to ``agent-host`` at the end. This same
    script is both mounted into workspace containers (as a ConfigMap ``seed.sh``
    the entrypoint runs) and piped over SSH to VMs on ready / IDE-session restore.
    """
    if not files:
        return "exit 0\n"

    parts: list[str] = [f"mkdir -p {CODE_SERVER_USER_DIR}/{SNIPPETS_SUBDIR}\n"]
    for name, entry in files.items():
        path = f"{CODE_SERVER_USER_DIR}/{name}"
        qpath = _shq(path)
        b64 = base64.b64encode((entry.get("content") or "").encode("utf-8")).decode(
            "ascii"
        )
        parts.append(f'mkdir -p "$(dirname {qpath})"\n')
        parts.append(f"base64 -d > {qpath} <<'__SRWB64__'\n{b64}\n__SRWB64__\n")
        mtime = entry.get("mtime")
        if mtime is not None:
            parts.append(f"touch -d @{mtime} {qpath}\n")
    parts.append(f"chown -R agent-host:agent-host {CODE_SERVER_USER_DIR}\n")
    return "".join(parts)


def build_extension_install_script(items: dict[str, dict]) -> str:
    """Shell that installs the user's Open-VSX extensions via the code-server CLI,
    run as ``agent-host``. Theme providers install **synchronously first** so the
    color theme is present when code-server first paints; the rest install in the
    background. Only ``source == "openvsx"`` items are handled here — ``bytes``
    items arrive via the orchestrator state stream (Phase B). Best-effort: a
    single failed install must not abort the rest (``|| true``)."""
    openvsx = {k: v for k, v in items.items() if v.get("source") == "openvsx"}
    if not openvsx:
        return "exit 0\n"

    def _install(ext_id: str, version: str) -> str:
        ref = _shq(f"{ext_id}@{version}")
        return (
            f"su -c 'code-server --install-extension {ref} "
            f"--extensions-dir {EXTENSIONS_DIR}' agent-host || true\n"
        )

    themes = [(k, v["version"]) for k, v in openvsx.items() if v.get("theme")]
    rest = [(k, v["version"]) for k, v in openvsx.items() if not v.get("theme")]

    parts = [f"mkdir -p {EXTENSIONS_DIR}\n"]
    for ext_id, version in themes:  # synchronous, theme-first
        parts.append(_install(ext_id, version))
    if rest:  # background the long tail
        parts.append("(\n")
        for ext_id, version in rest:
            parts.append(_install(ext_id, version))
        parts.append(f"chown -R agent-host:agent-host {EXTENSIONS_DIR}\n")
        parts.append(") &\n")
    parts.append(f"chown -R agent-host:agent-host {EXTENSIONS_DIR}\n")
    return "".join(parts)


# Runner signature: (host, port, remote_script, key_path, timeout) -> (rc, stdout, stderr)
SshRunner = Callable[..., Awaitable[tuple[int, Any, Any]]]
RemoteMutationAuthority = Callable[[], Awaitable[tuple[str, int, str] | None]]


async def _authorized_mutation_target(
    ssh_host: str,
    ssh_port: int,
    expected_host_key_fingerprint: Optional[str],
    mutation_authority: Optional[RemoteMutationAuthority],
) -> tuple[str, int, Optional[str]] | None:
    if mutation_authority is None:
        return ssh_host, ssh_port, expected_host_key_fingerprint
    target = await mutation_authority()
    if not (
        isinstance(target, tuple)
        and len(target) == 3
        and isinstance(target[0], str)
        and target[0]
        and isinstance(target[1], int)
        and target[1] > 0
        and isinstance(target[2], str)
        and target[2]
    ):
        return None
    return target


async def _default_ssh_runner(
    host: str,
    port: int,
    script: str,
    key_path: Optional[str] = None,
    timeout: int = 20,
    expected_host_key_fingerprint: Optional[str] = None,
) -> tuple[int, bytes, bytes]:
    """Run ``script`` on ``agent-host@host`` over SSH; return (rc, stdout, stderr)."""
    # Lazy import keeps this module import-light (no ssh/subprocess deps at import
    # time), mirroring ssh_helpers' own design.
    from orchestrator.services.ssh_helpers import (
        bounded_remote_mutation_command,
        build_agent_ssh_cmd,
        pinned_agent_ssh_command,
    )

    remote_script = bounded_remote_mutation_command(
        script,
        timeout_s=max(3, timeout - 1),
    )

    async def _run(cmd: list[str]) -> tuple[int, bytes, bytes]:
        proc = await create_owned_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await joined_async_call(
                communicate_bounded(
                    proc,
                    timeout=timeout,
                    stdout_limit=_SSH_STDOUT_MAX_BYTES,
                    stderr_limit=_SSH_STDERR_MAX_BYTES,
                )
            )
        except BaseException:
            if proc.returncode is None:
                await stop_and_reap(proc)
            raise
        return proc.returncode, stdout, stderr

    if expected_host_key_fingerprint is None:
        return await _run(
            build_agent_ssh_cmd(host, port, remote_script, key_path=key_path)
        )
    async with pinned_agent_ssh_command(
        host,
        port,
        remote_script,
        key_path=key_path,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    ) as cmd:
        return await _run(cmd)


async def _drain_stderr_tail(reader: asyncio.StreamReader) -> bytes:
    import inspect

    tail = bytearray()
    while True:
        value = reader.read(64 * 1024)
        if not inspect.isawaitable(value):
            return bytes(tail)
        chunk = await value
        if not isinstance(chunk, (bytes, bytearray)):
            return bytes(tail)
        if not chunk:
            return bytes(tail)
        tail.extend(chunk)
        if len(tail) > _SSH_STDERR_MAX_BYTES:
            del tail[: len(tail) - _SSH_STDERR_MAX_BYTES]


async def pull_ide_config(
    ssh_host: str,
    ssh_port: int,
    *,
    key_path: Optional[str] = None,
    timeout: int = 20,
    expected_host_key_fingerprint: Optional[str] = None,
    capture_authority: Optional[RemoteMutationAuthority] = None,
    _runner: Optional[SshRunner] = None,
) -> dict[str, dict]:
    """Read the code-server config files from a workspace over SSH.

    Returns ``{name: {content, mtime}}`` (possibly empty). Never raises — any SSH
    failure, timeout, or non-zero exit yields an empty dict so the reconciler can
    skip an unreachable workspace and move on.
    """
    runner = _runner or _default_ssh_runner
    try:
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        if target is None:
            return {}
        target_host, target_port, target_fingerprint = target
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": timeout}
        if target_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = target_fingerprint
        rc, stdout, stderr = await runner(
            target_host, target_port, build_pull_script(), **kwargs
        )
        if capture_authority is not None and (
            await _authorized_mutation_target(
                ssh_host,
                ssh_port,
                expected_host_key_fingerprint,
                capture_authority,
            )
            != target
        ):
            return {}
    except Exception as e:  # noqa: BLE001 — must not abort the sweep
        logger.warning(
            "ide_settings: pull failed for %s:%s — %s", ssh_host, ssh_port, e
        )
        return {}
    if rc != 0:
        err = (
            stderr.decode("utf-8", "replace")
            if isinstance(stderr, (bytes, bytearray))
            else (stderr or "")
        )
        logger.info(
            "ide_settings: pull rc=%s for %s:%s — %s", rc, ssh_host, ssh_port, err[:200]
        )
        return {}
    text = (
        stdout.decode("utf-8", "replace")
        if isinstance(stdout, (bytes, bytearray))
        else (stdout or "")
    )
    return parse_pull_output(text)


async def seed_ide_config(
    ssh_host: str,
    ssh_port: int,
    files: dict[str, dict],
    *,
    key_path: Optional[str] = None,
    timeout: int = 20,
    expected_host_key_fingerprint: Optional[str] = None,
    _runner: Optional[SshRunner] = None,
) -> bool:
    """Write the user's stored config into a workspace over SSH (restore path).

    Returns True on success. Never raises — a failed seed must not block restore.
    """
    if not files:
        return True
    runner = _runner or _default_ssh_runner
    try:
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": timeout}
        if expected_host_key_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = expected_host_key_fingerprint
        rc, _stdout, stderr = await runner(
            ssh_host, ssh_port, build_seed_script(files), **kwargs
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "ide_settings: seed failed for %s:%s — %s", ssh_host, ssh_port, e
        )
        return False
    if rc != 0:
        err = (
            stderr.decode("utf-8", "replace")
            if isinstance(stderr, (bytes, bytearray))
            else (stderr or "")
        )
        logger.warning(
            "ide_settings: seed rc=%s for %s:%s — %s", rc, ssh_host, ssh_port, err[:200]
        )
        return False
    return True


async def seed_ide_config_for_user(
    db: Any,
    user_id: Optional[str],
    ssh_host: str,
    ssh_port: int,
    *,
    key_path: Optional[str] = None,
    expected_host_key_fingerprint: Optional[str] = None,
    mutation_authority: Optional[RemoteMutationAuthority] = None,
    _runner: Optional[SshRunner] = None,
) -> bool:
    """Seed a user's stored code-server config + extensions into a workspace over
    SSH.

    Convenience wrapper used by the VM-ready and IDE-session-restore paths
    (containers seed via ConfigMap instead). Writes the config files and installs
    the user's Open-VSX extensions (theme-first). No-ops cleanly when there's no
    user or nothing stored. Never raises.
    """
    if not user_id:
        return True
    store = IdeSettingsStore(db)
    files = await store.get_ide_files(str(user_id))
    extensions = await store.get_extensions(str(user_id))
    if not files and not extensions:
        return True
    runner = _runner or _default_ssh_runner
    script = (
        build_seed_script(files) + "\n" + build_extension_install_script(extensions)
    )
    target = await _authorized_mutation_target(
        ssh_host,
        ssh_port,
        expected_host_key_fingerprint,
        mutation_authority,
    )
    if target is None:
        return False
    ssh_host, ssh_port, expected_host_key_fingerprint = target
    if mutation_authority is not None and expected_host_key_fingerprint is None:
        return False
    if expected_host_key_fingerprint is not None:
        # An authority-bound restore cannot let the background extension tail
        # outlive its renewable lease or pinned SSH transport.
        script += "\nwait\n"
    try:
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": 60}
        if expected_host_key_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = expected_host_key_fingerprint
        rc, _out, stderr = await runner(ssh_host, ssh_port, script, **kwargs)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "ide_settings: seed-for-user failed for %s:%s — %s", ssh_host, ssh_port, e
        )
        return False
    if rc != 0:
        err = (
            stderr.decode("utf-8", "replace")
            if isinstance(stderr, (bytes, bytearray))
            else (stderr or "")
        )
        logger.warning(
            "ide_settings: seed-for-user rc=%s for %s:%s — %s",
            rc,
            ssh_host,
            ssh_port,
            err[:200],
        )
        return False
    return True


PullFn = Callable[[str, int], Awaitable[dict]]


def _coerce_context(context: Any) -> dict:
    """Accept a context that may arrive as a dict or a JSON string."""
    if isinstance(context, str):
        try:
            return json.loads(context)
        except (json.JSONDecodeError, TypeError):
            return {}
    return context or {}


def is_kubernetes_capture_context(context: Any) -> bool:
    """Return whether a capture row names an unfenced Kubernetes runtime."""

    from shared.backend_kinds import is_vm_backend

    parsed = _coerce_context(context)
    backend = ((parsed.get("config_override") or {}).get("workspace") or {}).get(
        "backend"
    )
    if backend is not None and is_vm_backend(backend):
        return False
    container = parsed.get("workspace_container") or {}
    if isinstance(container, dict):
        provisioner = str(container.get("provisioner") or "").strip().lower()
        if provisioner == "k8s":
            return True
        if provisioner != "docker" and any(
            container.get(key)
            for key in (
                "pod_name",
                "pod_ip",
                "host",
                "_runtime_incarnation",
                "_workspace_generation",
            )
        ):
            return True

    ide_session = parsed.get("ide_session") or {}
    if not isinstance(ide_session, dict):
        return False
    restore_type = str(ide_session.get("restore_type") or "").strip().lower()
    if restore_type in {"vm", "container"}:
        return False
    if restore_type == "k8s_container":
        return True
    return bool(
        ide_session.get("pod_name")
        or ide_session.get("pod_ip")
        or ide_session.get("host")
        or ide_session.get("_runtime_incarnation")
        or ide_session.get("status") in {"active", "idle", "restoring"}
    )


def capture_safe_workspaces(workspaces: list[dict]) -> list[dict]:
    """Exclude Kubernetes rows before periodic capture can perform I/O."""

    return [
        workspace
        for workspace in workspaces
        if not is_kubernetes_capture_context(workspace.get("context"))
    ]


async def evict_dead_workspaces(
    workspaces: list[dict], provisioner: Any, db: Any
) -> list[dict]:
    """Drop container-backed worklist rows whose workspace pod is confirmed dead.

    The worklist selects on JSONB workspace status, which can stay ``'ready'``
    forever when a pod dies without a teardown clearing it. Before the sweeper
    serially SSH-dials each row, probe container-backed entries (those whose
    ``workspace_container`` carries a ``pod_name``/``pod_ip``) via
    ``provisioner.workspace_pod_live``: a confirmed-dead pod (``False``) gets
    ``{"status": "deleted", "pod_ip": None}`` merged into its entity's
    ``workspace_container`` context — so it drops out of the next worklist —
    and the row is evicted from this cycle. ``True``/``None`` (alive / can't
    tell) keeps the row; VM-backed rows pass through untouched. Any error while
    probing a row keeps it — the evictor must never abort the sweep.
    """
    from orchestrator.services.workspace_lifecycle import WorkspaceOwner

    kept: list[dict] = []
    for ws in workspaces:
        entity_type = ws.get("entity_type")
        try:
            context = _coerce_context(ws.get("context"))
            if is_vm_capture_context(context):
                kept.append(ws)
                continue
            container = context.get("workspace_container")
            if not isinstance(container, dict) or not (
                container.get("pod_name") or container.get("pod_ip")
            ):
                kept.append(ws)
                continue
            owner = (
                WorkspaceOwner.job(str(ws["id"]))
                if entity_type == "job"
                else WorkspaceOwner.session(str(ws["id"]))
            )
            live = await provisioner.workspace_pod_live(owner)
        except Exception as e:  # noqa: BLE001 — a probe blip must not kill the sweep
            logger.warning(
                "ide_settings: liveness probe failed for %s %s — keeping row (%s)",
                entity_type,
                ws.get("id"),
                e,
            )
            kept.append(ws)
            continue
        if live is False:
            logger.warning(
                "ide_settings: evicting %s %s from sweep — workspace pod confirmed "
                "dead; clearing stale container status",
                entity_type,
                ws.get("id"),
            )
            updates = {"status": "deleted", "pod_ip": None}
            try:
                if entity_type == "job":
                    await db.merge_workspace_container_context(str(ws["id"]), updates)
                else:
                    await db.merge_thread_workspace_context(str(ws["id"]), updates)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "ide_settings: failed to clear stale container context for "
                    "%s %s — %s",
                    entity_type,
                    ws.get("id"),
                    e,
                )
        else:
            kept.append(ws)
    return kept


async def reconcile_ide_settings(
    store: IdeSettingsStore, workspaces: list[dict], pull_fn: PullFn
) -> int:
    """Pull config from each active workspace and merge into per-user storage.

    ``workspaces`` is a list of ``{"user_id", "context"}`` rows. Each is resolved
    to an SSH target, pulled, and applied. Order-independent: the store keeps the
    newest mtime per file, so two workspaces of the same user converge on the
    latest edit regardless of iteration order. A failure on one workspace is
    logged and skipped — it never aborts the sweep.

    Returns the total number of files updated across all users this cycle.
    """
    updated_total = 0
    for ws in workspaces:
        user_id = ws.get("user_id")
        if not user_id:
            continue
        context = _coerce_context(ws.get("context"))
        if is_kubernetes_capture_context(context):
            logger.info(
                "ide_settings: refusing Kubernetes settings capture without "
                "durable exact-runtime capture authority"
            )
            continue
        target = resolve_ssh_target(context)
        if not target:
            continue
        host, port = target
        try:
            pulled = await pull_fn(host, port)
        except Exception as e:  # noqa: BLE001 — one bad workspace must not abort
            logger.warning(
                "ide_settings: reconcile pull failed for %s:%s — %s", host, port, e
            )
            continue
        if not pulled:
            continue
        try:
            updated = await store.apply_pulled_files(str(user_id), pulled)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "ide_settings: reconcile apply failed for user %s — %s", user_id, e
            )
            continue
        if updated:
            logger.info(
                "ide_settings: updated %d file(s) for user %s from %s",
                len(updated),
                user_id,
                host,
            )
        updated_total += len(updated)
    return updated_total


# List-fn signature: (host, port) -> {id: {version, theme}}
ListFn = Callable[[str, int], Awaitable[dict]]


async def list_ide_extensions(
    ssh_host: str,
    ssh_port: int,
    *,
    key_path: Optional[str] = None,
    timeout: int = 20,
    expected_host_key_fingerprint: Optional[str] = None,
    capture_authority: Optional[RemoteMutationAuthority] = None,
    _runner: Optional[SshRunner] = None,
) -> dict[str, dict]:
    """SSH into a workspace and return installed extensions. Never raises."""
    runner = _runner or _default_ssh_runner
    try:
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        if target is None:
            return {}
        target_host, target_port, target_fingerprint = target
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": timeout}
        if target_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = target_fingerprint
        rc, stdout, _ = await runner(
            target_host, target_port, build_extensions_list_script(), **kwargs
        )
        if capture_authority is not None and (
            await _authorized_mutation_target(
                ssh_host,
                ssh_port,
                expected_host_key_fingerprint,
                capture_authority,
            )
            != target
        ):
            return {}
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "ide_settings: ext-list failed for %s:%s — %s", ssh_host, ssh_port, e
        )
        return {}
    if rc != 0:
        return {}
    text = (
        stdout.decode("utf-8", "replace")
        if isinstance(stdout, (bytes, bytearray))
        else (stdout or "")
    )
    return parse_extensions_list(text)


async def reconcile_extensions(
    store: "IdeSettingsStore",
    workspaces: list[dict],
    list_fn: ListFn,
    classifier: "OpenVsxClassifier",
) -> int:
    """For each workspace, list extensions, classify each (openvsx|bytes), and
    merge into the user's manifest. Returns the count of ids added/bumped.
    Order-independent and failure-isolated like ``reconcile_ide_settings``."""
    changed_total = 0
    for ws in workspaces:
        user_id = ws.get("user_id")
        if not user_id:
            continue
        context = _coerce_context(ws.get("context"))
        if is_kubernetes_capture_context(context):
            logger.info(
                "ide_settings: refusing Kubernetes extension capture without "
                "durable exact-runtime capture authority"
            )
            continue
        target = resolve_ssh_target(context)
        if not target:
            continue
        host, port = target
        try:
            listed = await list_fn(host, port)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "ide_settings: ext reconcile list failed for %s:%s — %s", host, port, e
            )
            continue
        if not listed:
            continue
        items: dict[str, dict] = {}
        for ext_id, info in listed.items():
            source = await classifier.classify(ext_id, info.get("version", ""))
            items[ext_id] = {
                "version": info.get("version", ""),
                "source": source,
                "theme": bool(info.get("theme")),
            }
        try:
            changed = await store.apply_extensions(str(user_id), items)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "ide_settings: ext reconcile apply failed for user %s — %s", user_id, e
            )
            continue
        changed_total += len(changed)
    return changed_total


def is_vm_capture_context(context: Any) -> bool:
    """Use the canonical selected tier; never prefer stale opposite residue."""

    from shared.workspace_contract import (
        vm_mode_from_env,
        workspace_contract_authority_identity,
    )

    parsed = _coerce_context(context)
    selected = workspace_contract_authority_identity(
        {
            "context": parsed,
            "config_override": parsed.get("config_override"),
        },
        vm_mode=vm_mode_from_env(),
    )
    return bool(selected and selected[0] == "vm")


async def reconcile_vm_ide_workspace(
    *,
    store: "IdeSettingsStore",
    workspace: dict[str, Any],
    db: Any,
    vm_provisioner: Any,
    classifier: "OpenVsxClassifier",
    profile_store: Any = None,
) -> int:
    """Capture one VM under one renewable, host-key-pinned exact receipt."""

    from orchestrator.services.vm_remote_operation import (
        VMRemoteOperationLeaseLost,
        VMRemoteOperationUnavailable,
        claim_vm_remote_operation,
    )

    owner_id = str(workspace.get("id") or "")
    owner_kind = str(workspace.get("entity_type") or "")
    user_id = str(workspace.get("user_id") or "")
    if (
        not owner_id
        or owner_kind not in {"job", "thread"}
        or not user_id
        or not is_vm_capture_context(workspace.get("context"))
    ):
        return 0
    try:
        lease = await claim_vm_remote_operation(
            db=db,
            provisioner=vm_provisioner,
            owner_id=owner_id,
            owner_kind=owner_kind,
            operation_kind="ide_settings",
        )
        async with lease:
            identity = lease.identity
            pulled = await pull_ide_config(
                identity.ssh_host,
                identity.ssh_port,
                expected_host_key_fingerprint=identity.ssh_host_key_fingerprint,
                capture_authority=lease.revalidate,
            )
            updated: list[str] = []
            if pulled and await lease.revalidate() is not None:
                updated = await store.apply_pulled_files(user_id, pulled)
            listed = await list_ide_extensions(
                identity.ssh_host,
                identity.ssh_port,
                expected_host_key_fingerprint=identity.ssh_host_key_fingerprint,
                capture_authority=lease.revalidate,
            )
            if listed and await lease.revalidate() is not None:
                items: dict[str, dict] = {}
                for ext_id, info in listed.items():
                    items[ext_id] = {
                        "version": info.get("version", ""),
                        "source": await classifier.classify(
                            ext_id, info.get("version", "")
                        ),
                        "theme": bool(info.get("theme")),
                    }
                await store.apply_extensions(user_id, items)
            if profile_store is not None:
                await capture_ide_profile(
                    store,
                    user_id,
                    identity.ssh_host,
                    identity.ssh_port,
                    profile_store,
                    expected_host_key_fingerprint=(identity.ssh_host_key_fingerprint),
                    capture_authority=lease.revalidate,
                    vm_lease=lease,
                )
            if await lease.revalidate() is None:
                return 0
            return len(updated)
    except (VMRemoteOperationUnavailable, VMRemoteOperationLeaseLost):
        logger.info(
            "ide_settings: exact VM capture unavailable for %s %s",
            owner_kind,
            owner_id,
        )
        return 0


# Tar-fn signature: (host, port, remote_path, local_path, *, key_path) -> ok
TarFn = Callable[..., Awaitable[bool]]


async def _ssh_tar_to_file(
    ssh_host: str,
    ssh_port: int,
    remote_path: str,
    local_path: str,
    *,
    key_path: Optional[str] = None,
    timeout: int = 120,
    expected_host_key_fingerprint: Optional[str] = None,
) -> bool:
    """Stream ``ssh agent-host@host 'tar -cf - <remote_path> | zstd' > local`` —
    the snapshot_service transport, narrowed to one path. The remote command is
    wrapped in ``bash -c`` with a PIPESTATUS-discriminated verdict so a masked
    upstream ``tar`` failure can't hide behind ``zstd``'s own exit code (see
    knowledge-base/knowledge/features/workspace_durability_tiering.md §C1d). Returns False on error.
    """
    from orchestrator.services import resolve_ssh_key_path
    from orchestrator.services.ssh_helpers import (
        _scan_pinned_host_key,
        build_agent_ssh_cmd,
    )
    import tempfile

    kp = key_path if key_path is not None else resolve_ssh_key_path()
    # tar | zstd only reports the LAST stage's exit code, so a fatal tar
    # failure upstream of zstd is masked. Wrap in `bash -c` (guarantees
    # PIPESTATUS regardless of the agent-host login shell) and collapse to
    # an honest accept/reject verdict: accept tar rc in {0, 1} (rc==1 is the
    # benign "file changed as we read it" warning on a live workspace) with
    # a clean zstd; reject tar rc>=2 or any zstd failure. The `.tar.zst`
    # byte stream to stdout is unchanged — only the exit code's meaning is.
    quoted_remote_path = shlex.quote(remote_path)
    capture = (
        "__n=$(du -sb -- "
        + quoted_remote_path
        + " 2>/dev/null | cut -f1); "
        + f'[ -n "$__n" ] && [ "$__n" -le {_PROFILE_UNCOMPRESSED_MAX_BYTES} ] || exit 1; '
        + "tar -cf - -- "
        + quoted_remote_path
        + " 2>/dev/null | zstd -1 -T0; "
        '__ps=("${PIPESTATUS[@]}"); '
        'if [ "${__ps[1]}" -ne 0 ] || [ "${__ps[0]}" -ge 2 ]; then exit 1; else exit 0; fi'
    )
    remote = "bash -c " + shlex.quote(capture)
    known_hosts_path: str | None = None
    if expected_host_key_fingerprint is not None:
        known_host, _ = await _scan_pinned_host_key(
            ssh_host, ssh_port, expected_host_key_fingerprint
        )
        if known_host is None:
            return False
        known_hosts = tempfile.NamedTemporaryFile(
            mode="w", encoding="ascii", prefix="ide-profile-host-", delete=False
        )
        known_hosts.write(known_host + "\n")
        known_hosts.close()
        known_hosts_path = known_hosts.name
    cmd = build_agent_ssh_cmd(
        ssh_host,
        ssh_port,
        remote,
        key_path=kp,
        known_hosts_path=known_hosts_path,
        batch_mode=True,
    )
    proc = await create_owned_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stderr_task = asyncio.create_task(_drain_stderr_tail(proc.stderr))
    total = 0
    succeeded = False
    try:
        async with asyncio.timeout(timeout):
            with open(local_path, "wb") as f:
                while True:
                    chunk = await proc.stdout.read(1 << 20)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > _PROFILE_COMPRESSED_MAX_BYTES:
                        raise ValueError("IDE profile archive exceeds size cap")
                    f.write(chunk)
            await proc.wait()
        stderr = await stderr_task
        succeeded = proc.returncode == 0 and total > 0
    except BaseException as e:  # noqa: BLE001
        await stop_and_reap(proc)
        await asyncio.gather(stderr_task, return_exceptions=True)
        if isinstance(e, asyncio.CancelledError):
            raise
        logger.warning(
            "ide_settings: tar capture failed %s:%s — %s", ssh_host, ssh_port, e
        )
        return False
    finally:
        if known_hosts_path is not None:
            try:
                os.unlink(known_hosts_path)
            except OSError:
                pass
        if not succeeded:
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass
    if not succeeded:
        logger.warning(
            "ide_settings: tar capture rc=%s %s",
            proc.returncode,
            stderr.decode(errors="replace")[-200:],
        )
    return succeeded


async def _resolve_ext_dir(
    ssh_host: str,
    ssh_port: int,
    ext_id: str,
    version: str,
    *,
    key_path: Optional[str] = None,
    expected_host_key_fingerprint: Optional[str] = None,
    _runner: Optional[SshRunner] = None,
) -> Optional[str]:
    """Return the on-disk extension folder name for ``ext_id@version``.

    code-server names extension folders ``<id>-<version>`` and often appends a
    target-platform suffix (e.g. ``<id>-<version>-universal``). Checks the bare
    form and the suffixed form (the trailing ``-`` keeps ``2.0.1`` from matching
    ``2.0.13``). Returns the first match, or ``None`` if neither exists. Never
    raises — used only to locate ``bytes``-source extensions for byte-copy."""
    runner = _runner or _default_ssh_runner
    exact_folder = shlex.quote(f"{ext_id}-{version}")
    folder_glob = shlex.quote(f"{ext_id}-{version}-") + "*"
    script = (
        f"cd {EXTENSIONS_DIR} 2>/dev/null || exit 0\n"
        f"for d in {exact_folder} {folder_glob} ; do\n"
        '  [ -d "$d" ] && { printf \'%s\\n\' "$d"; break; }\n'
        "done\n"
    )
    try:
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": 20}
        if expected_host_key_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = expected_host_key_fingerprint
        rc, out, _ = await runner(ssh_host, ssh_port, script, **kwargs)
    except Exception:  # noqa: BLE001
        return None
    if rc != 0:
        return None
    text = (
        out.decode("utf-8", "replace")
        if isinstance(out, (bytes, bytearray))
        else (out or "")
    )
    name = text.strip().split("\n")[0].strip() if text.strip() else ""
    if (
        not name
        or name in {".", ".."}
        or "/" in name
        or "\x00" in name
        or len(name) > 512
    ):
        return None
    return name


async def capture_ide_profile(
    store: "IdeSettingsStore",
    user_id: str,
    ssh_host: str,
    ssh_port: int,
    profile_store: Any,
    *,
    key_path: Optional[str] = None,
    expected_host_key_fingerprint: Optional[str] = None,
    capture_authority: Optional[RemoteMutationAuthority] = None,
    _runner: Optional[SshRunner] = None,
    _tar_fn: Optional[TarFn] = None,
    vm_lease: Any = None,
) -> int:
    """If the workspace's extensions/globalStorage changed since last capture,
    tar globalStorage (and any ``bytes`` extension's folder) to the S3 profile
    store and record the new signature. Returns the number of blobs uploaded.
    Never raises."""
    import tempfile

    runner = _runner or _default_ssh_runner
    tar_fn = _tar_fn or _ssh_tar_to_file
    try:
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        if target is None:
            return 0
        target_host, target_port, target_fingerprint = target
        kwargs: dict[str, Any] = {"key_path": key_path, "timeout": 30}
        if target_fingerprint is not None:
            kwargs["expected_host_key_fingerprint"] = target_fingerprint
        rc, out, _ = await runner(
            target_host, target_port, build_signature_script(), **kwargs
        )
        if capture_authority is not None and (
            await _authorized_mutation_target(
                ssh_host,
                ssh_port,
                expected_host_key_fingerprint,
                capture_authority,
            )
            != target
        ):
            return 0
    except Exception:  # noqa: BLE001
        return 0
    if rc != 0:
        return 0
    sig = parse_signature(
        out.decode("utf-8", "replace")
        if isinstance(out, (bytes, bytearray))
        else (out or "")
    )
    if not sig or sig == await store.get_ext_signature(user_id):
        return 0

    uploaded = 0
    complete = True
    # globalStorage bundle
    global_name = "globalStorage"
    expected_global = await store.get_profile_pointer(user_id, global_name)
    with tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=True) as tmp:
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        if target is None:
            return uploaded
        target_host, target_port, target_fingerprint = target
        tar_kwargs: dict[str, Any] = {"key_path": key_path}
        if target_fingerprint is not None:
            tar_kwargs["expected_host_key_fingerprint"] = target_fingerprint
        if tar_fn and await tar_fn(
            target_host, target_port, GLOBAL_STORAGE_DIR, tmp.name, **tar_kwargs
        ):
            if capture_authority is not None and (
                await _authorized_mutation_target(
                    ssh_host,
                    ssh_port,
                    expected_host_key_fingerprint,
                    capture_authority,
                )
                != target
            ):
                return uploaded
            pointer = await profile_store.put_globalstorage(user_id, tmp.name)
            if not isinstance(pointer, dict):
                uploaded += 1
            elif await store.publish_profile_pointer(
                user_id,
                global_name,
                expected_pointer=expected_global,
                pointer=pointer,
                vm_lease=vm_lease,
            ):
                uploaded += 1
            else:
                complete = False
        else:
            complete = False
    # bytes extensions (only those classified bytes + not already stored)
    items = await store.get_extensions(user_id)
    for ext_id, info in items.items():
        if info.get("source") != "bytes":
            continue
        version = info.get("version", "")
        pointer_name = f"extension:{ext_id}@{version}"
        expected_pointer = await store.get_profile_pointer(user_id, pointer_name)
        if expected_pointer is not None:
            continue
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        if target is None:
            return uploaded
        target_host, target_port, target_fingerprint = target
        folder = await _resolve_ext_dir(
            target_host,
            target_port,
            ext_id,
            version,
            key_path=key_path,
            expected_host_key_fingerprint=target_fingerprint,
            _runner=runner,
        )
        if not folder:
            complete = False
            continue
        with tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=True) as tmp:
            remote = f"{EXTENSIONS_DIR}/{folder}"
            target = await _authorized_mutation_target(
                ssh_host,
                ssh_port,
                expected_host_key_fingerprint,
                capture_authority,
            )
            if target is None:
                return uploaded
            target_host, target_port, target_fingerprint = target
            tar_kwargs = {"key_path": key_path}
            if target_fingerprint is not None:
                tar_kwargs["expected_host_key_fingerprint"] = target_fingerprint
            if tar_fn and await tar_fn(
                target_host, target_port, remote, tmp.name, **tar_kwargs
            ):
                if capture_authority is not None and (
                    await _authorized_mutation_target(
                        ssh_host,
                        ssh_port,
                        expected_host_key_fingerprint,
                        capture_authority,
                    )
                    != target
                ):
                    return uploaded
                pointer = await profile_store.put_ext_bytes(
                    user_id, ext_id, version, tmp.name
                )
                if not isinstance(pointer, dict):
                    uploaded += 1
                elif await store.publish_profile_pointer(
                    user_id,
                    pointer_name,
                    expected_pointer=expected_pointer,
                    pointer=pointer,
                    vm_lease=vm_lease,
                ):
                    uploaded += 1
                else:
                    complete = False
            else:
                complete = False

    if capture_authority is not None and (
        await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            capture_authority,
        )
        is None
    ):
        return uploaded
    if complete:
        await store.set_ext_signature(user_id, sig)
    return uploaded


async def _ssh_untar_from_file(
    ssh_host: str,
    ssh_port: int,
    local_path: str,
    *,
    key_path: Optional[str] = None,
    timeout: int = 120,
    expected_host_key_fingerprint: Optional[str] = None,
) -> bool:
    """Reverse of :func:`_ssh_tar_to_file`: stream a local ``.tar.zst`` into the
    workspace via ``ssh ... 'zstd -d | tar -xf - -C /'``. The archive was created
    with absolute paths (e.g. ``/var/lib/code-server/User/globalStorage``) so it
    extracts back to the same location. Wrapped in ``bash -c`` with the same
    PIPESTATUS verdict as the capture side above, so a masked ``zstd -d``
    decompression failure on a corrupt archive can't hide behind tar's own rc
    (§C1d). Returns False on error."""
    from orchestrator.services import resolve_ssh_key_path
    from orchestrator.services.ssh_helpers import (
        bounded_remote_mutation_command,
        build_agent_ssh_cmd,
        pinned_agent_ssh_command,
    )
    import os

    try:
        if os.path.getsize(local_path) > _PROFILE_COMPRESSED_MAX_BYTES:
            return False
    except OSError:
        return False

    kp = key_path if key_path is not None else resolve_ssh_key_path()
    # Bound decompressed bytes *before* tar sees them. A compressed-size cap
    # alone admits a tiny high-ratio archive which can fill the workspace
    # disk. The middle filter exits before forwarding the overflowing chunk;
    # PIPESTATUS then rejects truncation even if tar happened to accept it.
    limiter = (
        "import sys\n"
        f"limit={_PROFILE_UNCOMPRESSED_MAX_BYTES}\n"
        "total=0\n"
        "while True:\n"
        " chunk=sys.stdin.buffer.read(1048576)\n"
        " if not chunk: break\n"
        " total += len(chunk)\n"
        " if total > limit: raise SystemExit(73)\n"
        " sys.stdout.buffer.write(chunk)\n"
    )
    extraction = (
        "zstd -dc | python3 -c "
        + shlex.quote(limiter)
        + " | tar --no-same-owner --no-same-permissions --no-overwrite-dir "
        "-xf - -C /; "
        '__ps=("${PIPESTATUS[@]}"); '
        'if [ "${__ps[0]}" -ne 0 ] || [ "${__ps[1]}" -ne 0 ] '
        '|| [ "${__ps[2]}" -ge 2 ]; then exit 1; else exit 0; fi'
    )
    remote = bounded_remote_mutation_command(
        extraction,
        timeout_s=max(3, timeout - 1),
    )

    async def _run(cmd: list[str]) -> bool:
        proc = None
        stderr_task: asyncio.Task[bytes] | None = None
        try:
            proc = await create_owned_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(_drain_stderr_tail(proc.stderr))

            async def _feed() -> None:
                with open(local_path, "rb") as f:
                    while True:
                        chunk = f.read(1 << 20)
                        if not chunk:
                            break
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
                proc.stdin.close()

            async def _complete() -> bytes:
                async with asyncio.timeout(timeout):
                    await asyncio.gather(_feed(), proc.wait())
                    return await stderr_task

            stderr = await joined_async_call(_complete())
        except BaseException as error:
            if proc is not None:
                await stop_and_reap(proc)
            if stderr_task is not None:
                await asyncio.gather(stderr_task, return_exceptions=True)
            if isinstance(error, asyncio.CancelledError):
                raise
            logger.warning(
                "ide_settings: untar seed failed %s:%s — %s",
                ssh_host,
                ssh_port,
                error,
            )
            return False
        if proc.returncode != 0:
            logger.warning(
                "ide_settings: untar seed rc=%s — %s",
                proc.returncode,
                stderr.decode(errors="replace")[-200:],
            )
        return proc.returncode == 0

    if expected_host_key_fingerprint is None:
        return await _run(build_agent_ssh_cmd(ssh_host, ssh_port, remote, key_path=kp))
    async with pinned_agent_ssh_command(
        ssh_host,
        ssh_port,
        remote,
        key_path=kp,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    ) as cmd:
        return await _run(cmd)


async def seed_ide_profile(
    *,
    user_id: str,
    ssh_host: str,
    ssh_port: int,
    profile_store: Any,
    ext_items: dict,
    profile_pointers: dict[str, dict[str, Any]] | None = None,
    key_path: Optional[str] = None,
    expected_host_key_fingerprint: Optional[str] = None,
    mutation_authority: Optional[RemoteMutationAuthority] = None,
    _runner: Optional[SshRunner] = None,
    _push_fn: Optional[Any] = None,
) -> bool:
    """Restore globalStorage (+ any bytes extensions) into a workspace, then touch
    the sentinel the entrypoint waits on. Best-effort; returns True if the sentinel
    was written. Never raises."""
    import tempfile

    runner = _runner or _default_ssh_runner
    push = _push_fn or _ssh_untar_from_file
    pointers = profile_pointers or {}
    try:
        # The profile store removes absent/rejected downloads. Own their directory
        # instead of a NamedTemporaryFile, whose close unlinks again on Python
        # 3.11/3.12 and can prevent a new user's workspace from becoming ready.
        with tempfile.TemporaryDirectory(prefix="srw-ide-seed-") as temp_dir:
            local_path = os.path.join(temp_dir, "globalStorage.tar.zst")
            global_pointer = pointers.get("globalStorage")
            got_global = (
                await profile_store.get_globalstorage(
                    user_id, local_path, global_pointer
                )
                if global_pointer is not None
                else await profile_store.get_globalstorage(user_id, local_path)
            )
            if got_global:
                target = await _authorized_mutation_target(
                    ssh_host,
                    ssh_port,
                    expected_host_key_fingerprint,
                    mutation_authority,
                )
                if target is None:
                    return False
                target_host, target_port, target_fingerprint = target
                push_kwargs: dict[str, Any] = {"key_path": key_path}
                if target_fingerprint is not None:
                    push_kwargs["expected_host_key_fingerprint"] = target_fingerprint
                await push(target_host, target_port, local_path, **push_kwargs)
        for ext_id, info in (ext_items or {}).items():
            if info.get("source") != "bytes":
                continue
            with tempfile.TemporaryDirectory(prefix="srw-ide-seed-") as temp_dir:
                local_path = os.path.join(temp_dir, "extension.tar.zst")
                pointer = pointers.get(f"extension:{ext_id}@{info.get('version', '')}")
                got_extension = (
                    await profile_store.get_ext_bytes(
                        user_id,
                        ext_id,
                        info.get("version", ""),
                        local_path,
                        pointer,
                    )
                    if pointer is not None
                    else await profile_store.get_ext_bytes(
                        user_id, ext_id, info.get("version", ""), local_path
                    )
                )
                if got_extension:
                    target = await _authorized_mutation_target(
                        ssh_host,
                        ssh_port,
                        expected_host_key_fingerprint,
                        mutation_authority,
                    )
                    if target is None:
                        return False
                    target_host, target_port, target_fingerprint = target
                    push_kwargs = {"key_path": key_path}
                    if target_fingerprint is not None:
                        push_kwargs["expected_host_key_fingerprint"] = (
                            target_fingerprint
                        )
                    await push(target_host, target_port, local_path, **push_kwargs)
        # chown + sentinel
        target = await _authorized_mutation_target(
            ssh_host,
            ssh_port,
            expected_host_key_fingerprint,
            mutation_authority,
        )
        if target is None:
            return False
        target_host, target_port, target_fingerprint = target
        runner_kwargs: dict[str, Any] = {"key_path": key_path, "timeout": 30}
        if target_fingerprint is not None:
            runner_kwargs["expected_host_key_fingerprint"] = target_fingerprint
        rc, _o, _e = await runner(
            target_host,
            target_port,
            f"chown -R agent-host:agent-host {CODE_SERVER_USER_DIR} {EXTENSIONS_DIR} 2>/dev/null; "
            f"touch {SEED_STATE_SENTINEL}\n",
            **runner_kwargs,
        )
        return rc == 0
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "ide_settings: profile seed failed %s:%s — %s", ssh_host, ssh_port, e
        )
        return False


def _ver_key(v: str) -> tuple:
    """Sort key for version strings: numeric-aware, falls back to string parts.
    ``"2.0.13"`` > ``"2.0.9"``; non-numeric segments compare lexicographically."""
    parts = []
    for seg in str(v).replace("-", ".").split("."):
        parts.append((0, int(seg)) if seg.isdigit() else (1, seg))
    return tuple(parts)


class IdeSettingsStore:
    """Read/write per-user code-server config in ``users.settings['ide']``."""

    def __init__(self, db: Any) -> None:
        self._db = db

    async def _merge_component(
        self, user_id: str, component: str, patch: dict[str, Any]
    ) -> None:
        merge = getattr(self._db, "merge_user_ide_component", None)
        if callable(merge):
            await merge(user_id, component=component, patch=patch)
            return

        # Compatibility seam for lightweight stores used outside Postgres and
        # by tests. Production uses the atomic component merge above.
        settings = await self._db.get_user_settings(user_id)
        settings = settings if isinstance(settings, dict) else {}
        ide = (
            dict(settings.get("ide") or {})
            if isinstance(settings.get("ide"), dict)
            else {}
        )
        current = (
            dict(ide.get(component) or {})
            if isinstance(ide.get(component), dict)
            else {}
        )
        current.update(patch)
        ide[component] = current
        await self._db.update_user_settings(user_id, {"ide": ide})

    async def get_profile_pointer(
        self, user_id: str, pointer_name: str
    ) -> dict[str, Any] | None:
        settings = await self._db.get_user_settings(user_id)
        ide = settings.get("ide") if isinstance(settings, dict) else None
        pointers = ide.get("profile_pointers") if isinstance(ide, dict) else None
        pointer = pointers.get(pointer_name) if isinstance(pointers, dict) else None
        return dict(pointer) if isinstance(pointer, dict) else None

    async def get_profile_pointers(self, user_id: str) -> dict[str, dict[str, Any]]:
        settings = await self._db.get_user_settings(user_id)
        ide = settings.get("ide") if isinstance(settings, dict) else None
        pointers = ide.get("profile_pointers") if isinstance(ide, dict) else None
        if not isinstance(pointers, dict):
            return {}
        return {
            str(name): dict(pointer)
            for name, pointer in pointers.items()
            if isinstance(name, str) and isinstance(pointer, dict)
        }

    async def publish_profile_pointer(
        self,
        user_id: str,
        pointer_name: str,
        *,
        expected_pointer: dict[str, Any] | None,
        pointer: dict[str, Any],
        vm_lease: Any = None,
    ) -> bool:
        vm_fields: dict[str, Any] = {}
        if vm_lease is not None:
            identity = vm_lease.identity
            vm_fields = {
                "vm_operation_id": str(vm_lease.receipt["id"]),
                "vm_claim_token": int(vm_lease.receipt["claim_token"]),
                "vm_claimant": vm_lease.claimant,
                "vm_owner_kind": identity.owner_kind,
                "vm_owner_id": identity.owner_id,
            }
        return bool(
            await self._db.cas_user_ide_profile_pointer(
                user_id,
                pointer_name=pointer_name,
                expected_pointer=expected_pointer,
                pointer=pointer,
                **vm_fields,
            )
        )

    async def get_ide_files(self, user_id: str) -> dict[str, dict]:
        """Return the stored config files: ``{name: {"content", "mtime"}}``.

        Empty dict when the user has no stored IDE config.
        """
        settings = await self._db.get_user_settings(user_id)
        if not isinstance(settings, dict):
            return {}
        ide = settings.get("ide")
        files = ide.get("files") if isinstance(ide, dict) else None
        return dict(files) if isinstance(files, dict) else {}

    async def apply_pulled_files(
        self, user_id: str, pulled: dict[str, dict]
    ) -> list[str]:
        """Merge freshly-pulled config files into the user's store.

        ``pulled`` maps a file name (e.g. ``"settings.json"``,
        ``"snippets/python.json"``) to ``{"content": str, "mtime": float}``. A
        file is written only if it is new or its mtime is strictly newer than the
        stored copy. Returns the names actually updated.
        """
        if not pulled:
            return []

        settings = await self._db.get_user_settings(user_id)
        if not isinstance(settings, dict):
            settings = {}
        ide = (
            dict(settings.get("ide") or {})
            if isinstance(settings.get("ide"), dict)
            else {}
        )
        files = (
            dict(ide.get("files") or {}) if isinstance(ide.get("files"), dict) else {}
        )

        updated: list[str] = []
        for name, entry in pulled.items():
            mtime = entry.get("mtime")
            if mtime is None:
                continue
            existing = files.get(name)
            if existing is None or mtime > existing.get("mtime", float("-inf")):
                files[name] = {"content": entry.get("content", ""), "mtime": mtime}
                updated.append(name)

        if not updated:
            return []

        await self._merge_component(
            user_id,
            "files",
            {name: files[name] for name in updated},
        )
        return updated

    async def get_extensions(self, user_id: str) -> dict[str, dict]:
        """Return the stored extension manifest items: ``{id: {version, source, theme}}``."""
        settings = await self._db.get_user_settings(user_id)
        if not isinstance(settings, dict):
            return {}
        ide = settings.get("ide")
        exts = ide.get("extensions") if isinstance(ide, dict) else None
        items = exts.get("items") if isinstance(exts, dict) else None
        return dict(items) if isinstance(items, dict) else {}

    async def apply_extensions(self, user_id: str, items: dict[str, dict]) -> list[str]:
        """Merge a workspace's installed extensions into the user's manifest.

        Union across workspaces, newest-version-wins per id (so an extension
        present only in workspace B survives a reconcile of workspace A). Returns
        the ids added or version-bumped. Read-modify-writes the whole ``ide``
        subtree because ``update_user_settings`` is a shallow merge.
        """
        if not items:
            return []
        settings = await self._db.get_user_settings(user_id)
        if not isinstance(settings, dict):
            settings = {}
        ide = (
            dict(settings.get("ide") or {})
            if isinstance(settings.get("ide"), dict)
            else {}
        )
        exts = (
            dict(ide.get("extensions") or {})
            if isinstance(ide.get("extensions"), dict)
            else {}
        )
        stored = (
            dict(exts.get("items") or {}) if isinstance(exts.get("items"), dict) else {}
        )

        changed: list[str] = []
        for ext_id, entry in items.items():
            version = entry.get("version")
            if not version:
                continue
            prev = stored.get(ext_id)
            if prev is None or _ver_key(version) > _ver_key(prev.get("version", "")):
                stored[ext_id] = {
                    "version": version,
                    "source": entry.get("source", "bytes"),
                    "theme": bool(entry.get("theme", False)),
                }
                changed.append(ext_id)

        if not changed:
            return []
        await self._merge_component(user_id, "extensions", {"items": stored})
        return changed

    async def get_ext_signature(self, user_id: str) -> str:
        """Return the last-captured content signature for this user's extensions+
        globalStorage, or empty string if none."""
        settings = await self._db.get_user_settings(user_id)
        ide = settings.get("ide") if isinstance(settings, dict) else None
        exts = ide.get("extensions") if isinstance(ide, dict) else None
        return exts.get("sig", "") if isinstance(exts, dict) else ""

    async def set_ext_signature(self, user_id: str, sig: str) -> None:
        """Record the cache signature without replacing items or pointers."""
        await self._merge_component(user_id, "extensions", {"sig": sig})


async def code_server_settings_sweeper(
    shutdown_event: asyncio.Event,
    *,
    db,
    container_provisioner,
    snapshot_service,
    vm_provisioner,
) -> None:
    """Background loop: reconcile per-user code-server IDE settings.

    Workspaces are network-isolated from the orchestrator (egress is denied), so
    instead of the workspace pushing changes, the orchestrator pulls inward on a
    ~10-minute cycle: it reads each active workspace's code-server config files
    (settings.json, keybindings.json, snippets) over SSH and merges any newer
    edits into the owning user's stored settings (``users.settings['ide']``).
    Conflict resolution is by filesystem mtime — newest wins, per file — so the
    cycle order across a user's workspaces doesn't matter. See
    orchestrator/services/ide_settings.py.
    """
    if os.environ.get("IDE_SETTINGS_SYNC_ENABLED", "true").lower() not in (
        "1",
        "true",
        "yes",
    ):
        # Park instead of returning: run_when_leader re-creates a loop that
        # exits on its next poll (~1s), which would respawn+log this every
        # second for the whole leadership tenure.
        logger.info("Code-server settings sweeper disabled (IDE_SETTINGS_SYNC_ENABLED)")
        await shutdown_event.wait()
        return

    interval = float(os.environ.get("IDE_SETTINGS_SYNC_INTERVAL_S", "600"))
    store = IdeSettingsStore(db)
    classifier = OpenVsxClassifier()  # cache persists across cycles for this process
    logger.info("Code-server settings sweeper started (interval=%.0fs)", interval)
    while not shutdown_event.is_set():
        try:
            workspaces = await db.list_active_ide_workspaces()
            vm_workspaces = [
                workspace
                for workspace in workspaces
                if is_vm_capture_context(workspace.get("context"))
            ]
            workspaces = [
                workspace
                for workspace in workspaces
                if not is_vm_capture_context(workspace.get("context"))
            ]
            workspaces = await evict_dead_workspaces(
                workspaces, container_provisioner, db
            )
            if workspaces or vm_workspaces:
                # Dial-target visibility: stable Service DNS survives pod
                # restarts; raw IPs are legacy rows predating the headless
                # Service and go stale with the pod.
                _dns_dials = sum(
                    1
                    for w in workspaces
                    if str(
                        (
                            (_coerce_context(w.get("context")) or {}).get(
                                "workspace_container"
                            )
                            or {}
                        ).get("host")
                        or ""
                    ).endswith(".svc.cluster.local")
                )
                logger.info(
                    "IDE settings sweeper: %d workspace(s), %d dialed via "
                    "stable service DNS",
                    len(workspaces),
                    _dns_dials,
                )
                count = await reconcile_ide_settings(store, workspaces, pull_ide_config)
                if count:
                    logger.info("IDE settings sweeper: synced %d file(s)", count)
                try:
                    ext_changed = await reconcile_extensions(
                        store, workspaces, list_ide_extensions, classifier
                    )
                    if ext_changed:
                        logger.info(
                            "IDE settings sweeper: synced %d extension(s)", ext_changed
                        )
                except Exception as e:  # noqa: BLE001
                    logger.error("Error reconciling extensions: %s", e)

                # Capture license/globalStorage + non-Open-VSX bytes to S3 when a
                # workspace's content signature changed (Phase B). Signature-gated
                # inside capture_ide_profile so most cycles are a cheap no-op.
                if snapshot_service.is_available:
                    from orchestrator.services.ide_profile_store import IdeProfileStore

                    profile = IdeProfileStore(
                        snapshot_service._s3, snapshot_service._bucket
                    )
                    for ws in workspaces:
                        uid = ws.get("user_id")
                        if not uid:
                            continue
                        tgt = resolve_ssh_target(_coerce_context(ws.get("context")))
                        if not tgt:
                            continue
                        try:
                            await capture_ide_profile(
                                store, str(uid), tgt[0], tgt[1], profile
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("ide profile capture failed: %s", e)
                    for ws in vm_workspaces:
                        try:
                            await reconcile_vm_ide_workspace(
                                store=store,
                                workspace=ws,
                                db=db,
                                vm_provisioner=vm_provisioner,
                                classifier=classifier,
                                profile_store=profile,
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("VM IDE capture failed: %s", e)
                else:
                    for ws in vm_workspaces:
                        await reconcile_vm_ide_workspace(
                            store=store,
                            workspace=ws,
                            db=db,
                            vm_provisioner=vm_provisioner,
                            classifier=classifier,
                        )
        except Exception as e:
            logger.error("Error in code-server settings sweeper: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Code-server settings sweeper stopped")
