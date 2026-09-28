"""Materialize server-owned managed-repository credentials into a workspace.

The payload is an internal orchestrator→runtime transport object. Callers pop
it from request/session metadata before invoking this module; this function
never logs or returns private material. Git remotes contain only an opaque SSH
host alias. The exact repo-scoped deploy key is loaded over the trusted SSH
transport into a dedicated ``ssh-agent`` and is never written into the
workspace, command line, environment, tmux scrollback, or Git configuration.
"""

from __future__ import annotations

import re
import shlex
from typing import Any, Iterable, Mapping, MutableMapping
from urllib.parse import urlparse
from uuid import UUID

from shared.runtime.utils.ssh_key import normalize_private_key

_VERSION = 1
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
_ALIAS = re.compile(r"^srw-repo-[a-f0-9]{32}$")
_AUTHORITY_SLUG = re.compile(r"^[a-f0-9]{32}$")


# Executed inside the workspace through the private control-plane SSH channel.
# It contains no credential material.  Keep the complete process classifier in
# one constant so materialization replacement, rollback, claim handoff, worker
# cleanup, and terminal End all use the same PID/start-time fence.
_SSH_AGENT_RETIRE_PROGRAM = r"""
import os
import re
import signal
import stat
import sys
import time
from pathlib import Path


UUID_PATTERN = r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"


def fail(code=86):
    raise SystemExit(code)


def process_uid(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("Uid:"):
                return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, ValueError):
        fail()
    fail()


def start_time(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError:
        fail()
    try:
        # Everything after the final ')' starts at proc field 3; starttime is
        # field 22, hence index 19 in this tail.  This remains correct if comm
        # itself contains spaces or parentheses.
        return raw.rsplit(")", 1)[1].split()[19]
    except (IndexError, ValueError):
        fail()


def state_and_start(pid):
    try:
        tail = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return tail[0], tail[19]
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, IndexError, ValueError):
        fail()


def runtime_scope():
    # Return the local kernel/PID-namespace incarnation, credential-free.
    # A PVC preserves the receipt across Pod replacement, while the old process
    # namespace no longer exists. PID/start-time alone would then confuse an
    # unrelated PID in the successor with a same-runtime reuse. The node boot
    # id separates VM reboots; the PID namespace inode separates containers on
    # one node. Both are kernel observations, never caller-authored metadata.

    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        pid_namespace = os.stat("/proc/self/ns/pid").st_ino
    except OSError:
        fail()
    if not re.fullmatch(UUID_PATTERN, boot) or pid_namespace <= 0:
        fail()
    return f"{boot}:{pid_namespace}"


def _identity_once(pid):
    proc = Path(f"/proc/{pid}")
    try:
        raw = (proc / "cmdline").read_bytes()
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError:
        if proc.exists() and process_uid(pid) == os.getuid():
            fail()
        return None
    if not raw:
        try:
            state = (proc / "stat").read_text().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            return None
        except (OSError, IndexError):
            if proc.exists() and process_uid(pid) == os.getuid():
                fail()
            return None
        if state == "Z":
            return None
        if process_uid(pid) == os.getuid():
            fail()
        return None
    try:
        argv = [part.decode("utf-8", "surrogateescape") for part in raw.split(b"\0") if part]
        command_name = (proc / "comm").read_text().strip()
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError:
        if proc.exists() and process_uid(pid) == os.getuid():
            fail()
        return None
    return command_name, argv, start_time(pid)


def identity(pid):
    # /proc exposes each exec/exit as several independently read files. Under
    # process churn, a same-UID process can disappear or finish exec between
    # cmdline, comm, status and stat, producing one conservative rc86 even when
    # it has no relationship to a managed socket. Retry only that ambiguous
    # observation for a short bound. A stable unreadable/foreign shape still
    # exhausts the bound and fails closed exactly as before.
    for attempt in range(5):
        try:
            return _identity_once(pid)
        except SystemExit as exc:
            if exc.code != 86 or attempt == 4:
                raise
            time.sleep(0.01)
    fail()  # pragma: no cover - the loop either returns or raises


mode = sys.argv[1]
if mode == "verify":
    try:
        verify_pid = int(sys.argv[2])
        verify_start = str(int(sys.argv[3]))
        verify_socket = str(Path(sys.argv[4]))
    except (IndexError, TypeError, ValueError):
        fail()
    current = identity(verify_pid)
    if current is None or process_uid(verify_pid) != os.getuid():
        fail()
    command_name, argv, started = current
    if (
        started != verify_start
        or command_name != "ssh-agent"
        or len(argv) != 4
        or Path(argv[0]).name != "ssh-agent"
        or argv[1:] != ["-a", verify_socket, "-s"]
    ):
        fail()
    raise SystemExit(0)
elif mode == "present":
    try:
        expected_generation = int(sys.argv[2])
        present_socket = str(Path(sys.argv[3]))
        expected_workspace_generation = sys.argv[4]
        expected_runtime_incarnation = sys.argv[5]
    except (IndexError, TypeError, ValueError):
        fail()
    if (
        expected_generation < 1
        or not re.fullmatch(rf"-|{UUID_PATTERN}", expected_workspace_generation)
        or not re.fullmatch(rf"-|{UUID_PATTERN}", expected_runtime_incarnation)
    ):
        fail()
    exact = {present_socket}
    base = None
elif mode == "replace":
    try:
        expected_workspace_generation = sys.argv[2]
        expected_runtime_incarnation = sys.argv[3]
        exact = {str(Path(value)) for value in sys.argv[4:]}
    except (IndexError, TypeError, ValueError):
        fail()
    if (
        not exact
        or not re.fullmatch(rf"-|{UUID_PATTERN}", expected_workspace_generation)
        or not re.fullmatch(rf"-|{UUID_PATTERN}", expected_runtime_incarnation)
    ):
        fail()
    base = None
elif mode == "exact":
    exact = {str(Path(value)) for value in sys.argv[2:]}
    base = None
elif mode in {"all", "zero"}:
    exact = set()
    base = str(Path(sys.argv[2]))
else:
    fail()


def selected(command_name, argv):
    # The server starts exactly this argv.  A lookalike with extra options is
    # not authority to kill and, inside our private namespace, is ambiguity.
    exact_shape = (
        command_name == "ssh-agent"
        and len(argv) == 4
        and Path(argv[0]).name == "ssh-agent"
        and argv[1] == "-a"
        and argv[3] == "-s"
    )
    candidate = argv[2] if exact_shape else None
    if mode in {"exact", "present", "replace"}:
        if candidate in exact:
            return True
        if command_name == "ssh-agent" and any(value in argv for value in exact):
            fail()
        return False
    assert base is not None
    touches_base = any(value == base or value.startswith(base + os.sep) for value in argv)
    if not exact_shape:
        if command_name == "ssh-agent" and touches_base:
            fail()
        return False
    parent = str(Path(candidate).parent)
    name = Path(candidate).name
    if parent != base:
        return False
    if not re.fullmatch(r"[a-f0-9]{32}\.sock", name):
        fail()
    return True


targets = []
for entry in Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    pid = int(entry.name)
    current = identity(pid)
    if current is None:
        continue
    command_name, argv, started = current
    if selected(command_name, argv):
        if process_uid(pid) != os.getuid() or started is None:
            fail()
        targets.append((pid, started, command_name, argv, argv[2]))


def validate_receipt(socket, socket_targets):
    socket_path = Path(socket)
    state_path = socket_path.parent.parent / "agents" / (socket_path.stem + ".state")
    try:
        state_stat = state_path.lstat()
    except FileNotFoundError:
        # Bounded rolling-upgrade seam for genuine pre-repair agents: the
        # private canonical socket, exact same-UID argv, and exact generated
        # SSH config must all agree. Missing/extra targets remain ambiguity.
        if not socket_targets:
            return None
        if len(socket_targets) != 1:
            fail()
        config_path = socket_path.parent.parent / "config.d" / (socket_path.stem + ".conf")
        try:
            config_stat = config_path.lstat()
            config_lines = config_path.read_text().splitlines()
        except OSError:
            fail()
        if (
            not stat.S_ISREG(config_stat.st_mode)
            or config_stat.st_uid != os.getuid()
            or stat.S_IMODE(config_stat.st_mode) & 0o077
            or f"Host srw-repo-{socket_path.stem}" not in config_lines
            or f"  IdentityAgent {socket_path}" not in config_lines
        ):
            fail()
        return 0, "-", "-"
    except OSError:
        fail()
    if not stat.S_ISREG(state_stat.st_mode) or state_stat.st_uid != os.getuid():
        fail()
    try:
        lines = state_path.read_text().splitlines()
        pairs = [line.split("=", 1) for line in lines]
        if any(len(pair) != 2 for pair in pairs):
            fail()
        values = dict(pairs)
        if len(values) != len(pairs):
            fail()
        receipt_pid = int(values["pid"])
        receipt_start = str(int(values["starttime"]))
        generation = int(values["generation"])
        receipt_scope = values["runtime_scope"]
        workspace_generation = values["workspace_generation"]
        runtime_incarnation = values["runtime_incarnation"]
    except (KeyError, OSError, TypeError, ValueError):
        fail()
    if (
        set(values)
        != {
            "version",
            "authority_id",
            "generation",
            "workspace_generation",
            "runtime_incarnation",
            "runtime_scope",
            "pid",
            "starttime",
            "socket",
        }
        or values["version"] != "2"
        or generation < 1
        or values["authority_id"] != socket_path.stem
        or not re.fullmatch(r"[a-f0-9]{32}", values["authority_id"])
        or values["socket"] != str(socket_path)
        or not re.fullmatch(rf"{UUID_PATTERN}:[1-9][0-9]*", receipt_scope)
        or not re.fullmatch(rf"-|{UUID_PATTERN}", workspace_generation)
        or not re.fullmatch(rf"-|{UUID_PATTERN}", runtime_incarnation)
    ):
        fail()
    matching = [
        target
        for target in socket_targets
        if target[0] == receipt_pid and target[1] == receipt_start
    ]
    if len(socket_targets) > 1 or len(matching) > 1:
        fail()
    if socket_targets:
        # One exact PID/start/argv target remains receipt-owned even when a
        # trusted server runtime has advanced; present returns rc4 and performs
        # the controlled replacement. A nonmatching target is ambiguity.
        if len(matching) == 1:
            return generation, workspace_generation, runtime_incarnation
        fail()
    # Trusted current runtime UUIDs can prove that a PVC-carried receipt came
    # from a predecessor even if a kernel namespace identifier were reused.
    # With no canonical-socket target, discard that stale receipt before
    # inspecting a possibly reused numeric PID in this successor runtime.
    if (
        mode in {"present", "replace"}
        and expected_workspace_generation != "-"
        and expected_runtime_incarnation != "-"
        and (
            workspace_generation != expected_workspace_generation
            or runtime_incarnation != expected_runtime_incarnation
        )
    ):
        return generation, workspace_generation, runtime_incarnation
    # A new Pod/PID namespace or VM boot cannot contain the predecessor.
    # Discard its durable receipt without inspecting a possibly-reused PID in
    # this successor.  An exact process on the canonical socket is still
    # ambiguity and cannot be silently adopted.
    if receipt_scope != runtime_scope():
        return generation, workspace_generation, runtime_incarnation
    # A stale receipt is removable only if both its exact process generation
    # and the socket's process set are gone.  PID reuse or an unrecorded
    # replacement is ambiguity, never authority to kill.
    observed = state_and_start(receipt_pid)
    if observed is not None:
        # The exact recorded generation may remain as a zombie when the agent
        # container has no reaping init. It has no executable/socket authority
        # and is process-zero for this purpose; a live/reused PID is ambiguity.
        if observed != ("Z", receipt_start):
            fail()
    return generation, workspace_generation, runtime_incarnation


if mode in {"exact", "present", "replace"}:
    selected_sockets = exact
else:
    assert base is not None
    socket_root = Path(base)
    state_root = socket_root.parent / "agents"
    selected_sockets = {target[4] for target in targets}
    try:
        for path in socket_root.iterdir():
            if re.fullmatch(r"[a-f0-9]{32}\.sock", path.name):
                selected_sockets.add(str(path))
        for path in state_root.iterdir():
            if re.fullmatch(r"[a-f0-9]{32}\.state", path.name):
                selected_sockets.add(str(socket_root / (path.stem + ".sock")))
    except FileNotFoundError:
        pass
    except OSError:
        fail()
receipt_generations = {}
for socket in selected_sockets:
    receipt_generations[socket] = validate_receipt(
        socket, [target for target in targets if target[4] == socket]
    )

if mode == "present":
    receipt = receipt_generations.get(present_socket)
    if receipt is not None and receipt[0] > expected_generation:
        raise SystemExit(5)
    if not targets:
        raise SystemExit(3)
    if len(targets) != 1:
        fail()
    if receipt is None:
        fail()
    receipt_generation, workspace_generation, runtime_incarnation = receipt
    # Repository authority generations are monotonic. A delayed generation N
    # claimant must never acquire the flock after N+1 and downgrade its config,
    # process, or loaded key. Legacy (0) and a lower resident can be upgraded;
    # a higher resident fails closed without mutation.
    if receipt_generation > expected_generation:
        raise SystemExit(5)
    if receipt_generation < expected_generation:
        raise SystemExit(4)
    raise SystemExit(
        0
        if (
            workspace_generation == expected_workspace_generation
            and runtime_incarnation == expected_runtime_incarnation
        )
        else 4
    )

if mode == "zero":
    raise SystemExit(85 if targets else 0)


def still_exact(target):
    pid, started, command_name, argv, socket = target
    current = identity(pid)
    return current is not None and current == (command_name, argv, started)


for target in targets:
    if still_exact(target):
        try:
            os.kill(target[0], signal.SIGTERM)
        except ProcessLookupError:
            pass
deadline = time.monotonic() + 3.0
while time.monotonic() < deadline and any(still_exact(target) for target in targets):
    time.sleep(0.05)
for target in targets:
    if still_exact(target):
        try:
            os.kill(target[0], signal.SIGKILL)
        except ProcessLookupError:
            pass
deadline = time.monotonic() + 2.0
while time.monotonic() < deadline and any(still_exact(target) for target in targets):
    time.sleep(0.05)
if any(still_exact(target) for target in targets):
    fail(87)

# Re-enumerate.  This catches a second predecessor or a process spawned while
# the first set was draining; no false-zero may be inferred from one snapshot.
for entry in Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    current = identity(int(entry.name))
    if current is not None and selected(current[0], current[1]):
        fail(87)
""".strip()


class ManagedRepositoryMaterializationError(RuntimeError):
    """Credential delivery is malformed or cannot be proven in the workspace."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validated_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        if int(payload.get("version")) != _VERSION:
            raise ValueError
        authority_id = str(UUID(str(payload["authority_id"])))
        generation = int(payload["generation"])
        access_mode = str(payload["access_mode"])
        repo_name = str(payload["repo_name"])
        owner = str(payload["repository_owner"])
        alias = str(payload["alias"])
        host = str(payload["ssh_host"])
        port = int(payload["ssh_port"])
        clone_url = str(payload["clone_url"])
        fingerprint = str(payload["public_key_fingerprint"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        ) from exc

    parsed = urlparse(clone_url)
    expected_path = f"/{owner}/{repo_name}.git"
    if (
        generation < 1
        or access_mode not in {"read", "write"}
        or not _NAME.fullmatch(repo_name)
        or not _NAME.fullmatch(owner)
        or not _ALIAS.fullmatch(alias)
        or alias != f"srw-repo-{authority_id.replace('-', '')}"
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,252}", host)
        or not 1 <= port <= 65535
        or parsed.scheme != "ssh"
        or parsed.hostname != alias
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.path != expected_path
        or not re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", fingerprint)
    ):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    try:
        normalized_private_key = normalize_private_key(str(payload["private_key"]))
        private_key = bytearray(normalized_private_key.encode("utf-8"))
        del normalized_private_key
    except (KeyError, TypeError, ValueError) as exc:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        ) from exc
    return {
        "authority_id": authority_id,
        "generation": generation,
        "access_mode": access_mode,
        "repo_name": repo_name,
        "repository_owner": owner,
        "alias": alias,
        "ssh_host": host,
        "ssh_port": port,
        "clone_url": clone_url,
        "private_key": private_key,
        "public_key_fingerprint": fingerprint,
    }


def repository_url_has_credentials(value: Any) -> bool:
    """Detect URI userinfo and scp-style Git identities."""

    text = str(value or "").strip()
    if not text:
        return False
    parsed = urlparse(text)
    if parsed.scheme:
        return parsed.username is not None or parsed.password is not None
    # "@" is excluded from the identity run so it cannot also be consumed by the
    # host run that follows: overlapping on every "@" made a string of them cost
    # quadratic time. An scp-style identity never contains one.
    return bool(re.match(r"^[^/\\\s@]+@[^:]+:", text))


def _wipe_validated_private_keys(validated: Iterable[dict[str, Any]]) -> None:
    """Best-effort zero every key that has not yet reached ``ssh-add``."""

    for item in validated:
        private_key = item.pop("private_key", None)
        if isinstance(private_key, bytearray):
            private_key[:] = b"\x00" * len(private_key)


def _authority_slug(value: Any) -> str:
    try:
        slug = str(UUID(str(value))).replace("-", "")
    except (TypeError, ValueError) as exc:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        ) from exc
    if not _AUTHORITY_SLUG.fullmatch(slug):  # pragma: no cover - UUID guarantees it
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    return slug


def _managed_root(home_path: str) -> str:
    home = str(home_path or "").rstrip("/")
    if not home.startswith("/") or "\x00" in home or "\n" in home or "\r" in home:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_materialization_failed"
        )
    return f"{home}/.ssh/srw-managed"


def managed_repository_agent_retirement_command(
    *,
    home_path: str,
    authority_ids: Iterable[str] | None = None,
    remove_configs: bool = True,
    workspace_generation: str | None = None,
    runtime_incarnation: str | None = None,
) -> str:
    """Return a credential-free exact ssh-agent retirement command.

    ``authority_ids=None`` is the terminal whole-workspace form.  A concrete
    list is used by runtime replacement/rollback and never touches another
    authority's socket.  The embedded classifier refuses ambiguous same-UID
    process observations; it does not infer process-zero from a missing socket.
    """

    root = _managed_root(home_path)
    socket_root = f"{root}/sockets"
    state_root = f"{root}/agents"
    config_root = f"{root}/config.d"
    raw_ids = None if authority_ids is None else list(authority_ids)
    if (workspace_generation is None) != (runtime_incarnation is None):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    if workspace_generation is not None:
        try:
            workspace_generation = str(UUID(str(workspace_generation)))
            runtime_incarnation = str(UUID(str(runtime_incarnation)))
        except (TypeError, ValueError) as exc:
            raise ManagedRepositoryMaterializationError(
                "managed_repository_credential_invalid"
            ) from exc
    if raw_ids is None:
        process_args = ["all", socket_root]
        cleanup = (
            f"for _srw_path in {shlex.quote(socket_root)}/*.sock; do "
            'test -e "$_srw_path" || test -S "$_srw_path" || continue; '
            'rm -f -- "$_srw_path"; done; '
            f"for _srw_path in {shlex.quote(state_root)}/*.state; do "
            'test -e "$_srw_path" || continue; rm -f -- "$_srw_path"; done; '
        )
        if remove_configs:
            cleanup += (
                f"for _srw_path in {shlex.quote(config_root)}/*.conf; do "
                'test -e "$_srw_path" || continue; rm -f -- "$_srw_path"; done; '
            )
    else:
        slugs = sorted({_authority_slug(value) for value in raw_ids})
        sockets = [f"{socket_root}/{slug}.sock" for slug in slugs]
        if workspace_generation is not None and runtime_incarnation is not None:
            process_args = [
                "replace",
                workspace_generation,
                runtime_incarnation,
                *sockets,
            ]
        else:
            process_args = ["exact", *sockets]
        paths: list[str] = []
        for slug, socket_path in zip(slugs, sockets, strict=True):
            paths.extend([socket_path, f"{state_root}/{slug}.state"])
            if remove_configs:
                paths.append(f"{config_root}/{slug}.conf")
        cleanup = "".join(f"rm -f -- {shlex.quote(path)}; " for path in paths)

    process_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(_SSH_AGENT_RETIRE_PROGRAM),
            *(shlex.quote(value) for value in process_args),
        ]
    )
    return f"set -eu; {process_command}; {cleanup}"


def managed_repository_agent_zero_command(*, home_path: str) -> str:
    """Return a read-only exact-process-zero proof for the private namespace."""

    socket_root = f"{_managed_root(home_path)}/sockets"
    process_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(_SSH_AGENT_RETIRE_PROGRAM),
            "zero",
            shlex.quote(socket_root),
        ]
    )
    return f"set -eu; {process_command}"


def managed_repository_agent_launch_command(
    *,
    home_path: str,
    authority_id: str,
    generation: int,
    preserve_existing: bool = False,
    keep_rollback_trap: bool = False,
    expected_fingerprint: str | None = None,
    probe_url: str | None = None,
    workspace_generation: str | None = None,
    runtime_incarnation: str | None = None,
    config_content: str | None = None,
) -> str:
    """Retire one predecessor and launch/record one exact replacement agent."""

    slug = _authority_slug(authority_id)
    if isinstance(generation, bool) or int(generation) < 1:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    if (expected_fingerprint is None) != (probe_url is None):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    if expected_fingerprint is not None and not re.fullmatch(
        r"SHA256:[A-Za-z0-9+/]{43}", expected_fingerprint
    ):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    if (workspace_generation is None) != (runtime_incarnation is None):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_invalid"
        )
    if workspace_generation is not None:
        try:
            workspace_generation = str(UUID(str(workspace_generation)))
            runtime_incarnation = str(UUID(str(runtime_incarnation)))
        except (TypeError, ValueError) as exc:
            raise ManagedRepositoryMaterializationError(
                "managed_repository_credential_invalid"
            ) from exc
    receipt_workspace_generation = workspace_generation or "-"
    receipt_runtime_incarnation = runtime_incarnation or "-"
    root = _managed_root(home_path)
    socket_path = f"{root}/sockets/{slug}.sock"
    state_path = f"{root}/agents/{slug}.state"
    config_path = f"{root}/config.d/{slug}.conf"
    lock_path = f"{root}/agents/{slug}.lock"
    retire = managed_repository_agent_retirement_command(
        home_path=home_path,
        authority_ids=[authority_id],
        remove_configs=False,
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
    )
    rollback = managed_repository_agent_retirement_command(
        home_path=home_path,
        authority_ids=[authority_id],
        remove_configs=True,
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
    )
    retire_inline = retire.rstrip().rstrip(";")
    verify_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(_SSH_AGENT_RETIRE_PROGRAM),
            "verify",
            '"$_srw_agent_pid"',
            '"$_srw_agent_start"',
            shlex.quote(socket_path),
        ]
    )
    present_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(_SSH_AGENT_RETIRE_PROGRAM),
            "present",
            str(int(generation)),
            shlex.quote(socket_path),
            shlex.quote(receipt_workspace_generation),
            shlex.quote(receipt_runtime_incarnation),
        ]
    )
    local_key_proof = "true"
    if expected_fingerprint is not None:
        local_key_proof = (
            f"_srw_agent_keys=$(SSH_AUTH_SOCK={shlex.quote(socket_path)} "
            "ssh-add -l) && "
            "test \"$(printf '%s\\n' \"$_srw_agent_keys\" | sed '/^$/d' "
            '| wc -l)" -eq 1 && '
            "printf '%s\\n' \"$_srw_agent_keys\" | "
            f"awk -v fp={shlex.quote(expected_fingerprint)} "
            "'$2 == fp { found += 1 } END { exit !(found == 1) }'"
        )
    presence = "_srw_agent_reused=no; "
    if preserve_existing:
        presence += (
            f"if {present_command}; then "
            # Receipt/process identity is necessary but insufficient for
            # reuse. A dead socket, removed key, or extra identity converges
            # by replacing this exact authority under the same lock. Keep the
            # local proof separate from the forge probe below: a healthy
            # resident survives a transient remote outage.
            f"if {local_key_proof}; then _srw_agent_reused=yes; "
            f"else {retire_inline}; fi; "
            "else _srw_present_rc=$?; "
            'case "$_srw_present_rc" in 3|4) '
            f'{retire_inline};; *) exit "$_srw_present_rc";; esac; fi; '
        )
    else:
        # Forced replacement still enters through ``present`` so a delayed
        # lower-generation caller cannot bypass the monotonic generation
        # check and downgrade a newer resident.
        presence += (
            f"if {present_command}; then {retire_inline}; "
            "else _srw_present_rc=$?; "
            'case "$_srw_present_rc" in 3|4) '
            f'{retire_inline};; *) exit "$_srw_present_rc";; esac; fi; '
        )
    publish_config = ""
    if config_content is not None:
        if "\x00" in config_content:
            raise ManagedRepositoryMaterializationError(
                "managed_repository_credential_invalid"
            )
        publish_config = (
            f"printf %s {shlex.quote(config_content)} > {shlex.quote(config_path)}; "
            f"chmod 600 {shlex.quote(config_path)}; "
        )
    successful_suffix = "" if keep_rollback_trap else "; trap - EXIT"
    proof_suffix = ""
    if expected_fingerprint is not None and probe_url is not None:
        proof_suffix = (
            f"; {local_key_proof} || exit 86; "
            f"GIT_TERMINAL_PROMPT=0 git ls-remote {shlex.quote(probe_url)} "
            "HEAD >/dev/null"
        )
    return (
        "set -eu; _srw_agent_spawned=no; "
        "_srw_managed_agent_rollback() { "
        "_srw_rc=$?; trap - EXIT; "
        'if test "$_srw_rc" -ne 0 && test "$_srw_agent_spawned" = yes; then '
        f"if ! ( {rollback} ); then _srw_rc=88; fi; "
        'fi; exit "$_srw_rc"; }; '
        "trap _srw_managed_agent_rollback EXIT; "
        f"mkdir -p {shlex.quote(root + '/agents')} {shlex.quote(root + '/sockets')} "
        f"{shlex.quote(root + '/config.d')}; "
        f"chmod 700 {shlex.quote(root + '/agents')} {shlex.quote(root + '/sockets')} "
        f"{shlex.quote(root + '/config.d')}; "
        f"exec 9>{shlex.quote(lock_path)}; flock -x 9; "
        + presence
        + publish_config
        + 'if test "$_srw_agent_reused" = yes; then cat >/dev/null; else '
        # Keep both locks in the launching shell. Older ssh-agent versions
        # retain inherited descriptors, so close the generation lock (9) and
        # the IDE caller's setup lock (8) before launching the resident.
        + f"_srw_agent_output=$(ssh-agent -a {shlex.quote(socket_path)} -s 9>&- 8>&-); "
        + "_srw_agent_spawned=yes; "
        + "_srw_agent_pid=$(printf '%s\\n' \"$_srw_agent_output\" "
        + "| sed -n 's/^SSH_AGENT_PID=\\([0-9][0-9]*\\);.*$/\\1/p'); "
        + "case \"$_srw_agent_pid\" in ''|*[!0-9]*) exit 86;; esac; "
        + "_srw_agent_start=$(awk '{ print $22 }' \"/proc/$_srw_agent_pid/stat\" "
        + "2>/dev/null) || exit 86; "
        + "case \"$_srw_agent_start\" in ''|*[!0-9]*) exit 86;; esac; "
        + "_srw_agent_scope=$(python3 -c "
        + shlex.quote(
            "import os,re; from pathlib import Path; "
            "b=Path('/proc/sys/kernel/random/boot_id').read_text().strip(); "
            "i=os.stat('/proc/self/ns/pid').st_ino; "
            "assert re.fullmatch(r'[a-f0-9-]{36}', b) and i > 0; "
            "print(f'{b}:{i}')"
        )
        + "); "
        + f"{verify_command}; "
        + f"_srw_state_tmp={shlex.quote(state_path)}.tmp.$$; "
        + "printf '%s\\n' 'version=2' "
        + f"'authority_id={slug}' 'generation={int(generation)}' "
        + f"'workspace_generation={receipt_workspace_generation}' "
        + f"'runtime_incarnation={receipt_runtime_incarnation}' "
        + '"runtime_scope=$_srw_agent_scope" '
        + '"pid=$_srw_agent_pid" "starttime=$_srw_agent_start" '
        + f"'socket={socket_path}' > \"$_srw_state_tmp\"; "
        + 'chmod 600 "$_srw_state_tmp"; '
        + f'mv -f -- "$_srw_state_tmp" {shlex.quote(state_path)}; '
        + f"SSH_AUTH_SOCK={shlex.quote(socket_path)} ssh-add - >/dev/null 2>&1; fi"
        + proof_suffix
        + successful_suffix
    )


def _backend_managed_home(backend: Any) -> str:
    workspace_path = str(
        getattr(backend, "_workspace_path", "")
        or getattr(backend, "_remote_root", "")
        or ""
    ).rstrip("/")
    if workspace_path.endswith("/workspace"):
        home = workspace_path[: -len("/workspace")]
        if home.startswith("/"):
            return home
    managed = str(backend.resolve_home_path(".ssh/srw-managed"))
    suffix = "/.ssh/srw-managed"
    if not managed.endswith(suffix):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_materialization_failed"
        )
    return managed[: -len(suffix)]


def _execute_managed_secret_command(
    backend: Any,
    command: str,
    secret: str | bytes | bytearray,
    *,
    timeout: int,
) -> bool:
    """Use the stateless claim fence when the backend provides that seam."""

    fenced = getattr(backend, "execute_claim_resource_with_secret_stdin", None)
    if callable(fenced):
        return bool(
            fenced(
                command,
                secret,
                timeout=timeout,
                operation="managed repository credential materialization",
            )
        )
    return bool(backend.execute_with_secret_stdin(command, secret, timeout=timeout))


def _materialize_validated_credentials(
    validated: list[dict[str, Any]], backend: Any
) -> dict[str, str]:
    """Materialize already-validated payloads; the caller owns key wiping."""

    if len({item["repo_name"] for item in validated}) != len(validated):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_duplicate"
        )
    if len({item["authority_id"] for item in validated}) != len(validated):
        raise ManagedRepositoryMaterializationError(
            "managed_repository_credential_duplicate"
        )

    home_path = _backend_managed_home(backend)
    runtime_authority = getattr(backend, "managed_repository_runtime_authority", None)
    if runtime_authority is not None:
        if not isinstance(runtime_authority, tuple) or len(runtime_authority) != 2:
            raise ManagedRepositoryMaterializationError(
                "managed_repository_materialization_failed"
            )
        try:
            runtime_workspace_generation = str(UUID(str(runtime_authority[0])))
            runtime_incarnation = str(UUID(str(runtime_authority[1])))
        except (TypeError, ValueError) as exc:
            raise ManagedRepositoryMaterializationError(
                "managed_repository_materialization_failed"
            ) from exc
    else:
        runtime_workspace_generation = None
        runtime_incarnation = None

    try:
        setup_ok = _execute_managed_secret_command(
            backend,
            "mkdir -p ~/.ssh/srw-managed/config.d ~/.ssh/srw-managed/sockets "
            "~/.ssh/srw-managed/agents "
            "&& chmod 700 ~/.ssh ~/.ssh/srw-managed "
            "~/.ssh/srw-managed/config.d ~/.ssh/srw-managed/sockets "
            "~/.ssh/srw-managed/agents "
            "&& exec 8>~/.ssh/srw-managed/setup.lock && flock -x 8 "
            "&& touch ~/.ssh/config "
            "&& (grep -qxF 'Include ~/.ssh/srw-managed/config.d/*.conf' ~/.ssh/config "
            "|| printf '\\nInclude ~/.ssh/srw-managed/config.d/*.conf\\n' "
            ">> ~/.ssh/config) "
            "&& chmod 600 ~/.ssh/config",
            b"",
            timeout=15,
        )
    except (NotImplementedError, OSError):
        setup_ok = False
    if not setup_ok:
        raise ManagedRepositoryMaterializationError(
            "managed_repository_materialization_failed"
        )

    result: dict[str, str] = {}
    try:
        for item in validated:
            authority_slug = item["authority_id"].replace("-", "")
            socket_path = f"{home_path}/.ssh/srw-managed/sockets/{authority_slug}.sock"
            known_hosts = f"{home_path}/.ssh/srw-managed/known_hosts"
            config = (
                f"Host {item['alias']}\n"
                f"  HostName {item['ssh_host']}\n"
                f"  Port {item['ssh_port']}\n"
                "  User git\n"
                f"  IdentityAgent {socket_path}\n"
                # This is a dedicated per-authority agent containing exactly one
                # key. ``IdentitiesOnly yes`` would suppress agent-only keys unless
                # a matching IdentityFile also existed, defeating the deliberate
                # no-private-key-file design.
                "  BatchMode yes\n"
                "  StrictHostKeyChecking accept-new\n"
                f"  UserKnownHostsFile {known_hosts}\n"
            )
            # Replacement is an acknowledged handoff: retire every exact
            # predecessor (including legacy agents whose socket was unlinked),
            # then persist a credential-free PID/start-time receipt before
            # loading the key from the private stdin channel.
            load_command = managed_repository_agent_launch_command(
                home_path=home_path,
                authority_id=item["authority_id"],
                generation=int(item["generation"]),
                preserve_existing=True,
                expected_fingerprint=item["public_key_fingerprint"],
                probe_url=item["clone_url"],
                workspace_generation=runtime_workspace_generation,
                runtime_incarnation=runtime_incarnation,
                config_content=config,
            )
            materialize_command = "set -eu; umask 077; " + load_command
            private_key = item.pop("private_key")
            try:
                try:
                    loaded = _execute_managed_secret_command(
                        backend,
                        materialize_command,
                        private_key,
                        timeout=30,
                    )
                except (NotImplementedError, OSError):
                    loaded = False
            finally:
                private_key[:] = b"\x00" * len(private_key)
                del private_key
            if not loaded:
                raise ManagedRepositoryMaterializationError(
                    "managed_repository_workspace_probe_failed"
                )
            result[item["repo_name"]] = item["clone_url"]
        return result
    except Exception:
        # The current launch command rolls back a process it spawned before
        # returning failure. Earlier proven authorities may already serve an
        # overlapping root/child job and remain workspace residents.
        raise


def materialize_managed_repository_credentials(
    payloads: Iterable[Mapping[str, Any]] | None,
    backend: Any,
) -> dict[str, str]:
    """Install exact repo keys and prove each clean remote from the workspace.

    Returns ``{repo_name: clone_url}``, a secret-free map used to replace the
    canonical HTTP URLs in the ephemeral runtime request.  Any malformed or
    unreachable authority fails before workspace initialization/provider work.
    """

    raw = list(payloads or [])
    if not raw:
        return {}
    if not getattr(backend, "supports_shell", False):
        for item in raw:
            if isinstance(item, MutableMapping):
                item.pop("private_key", None)
        raise ManagedRepositoryMaterializationError(
            "managed_repository_requires_workspace"
        )
    validated: list[dict[str, Any]] = []
    try:
        for item in raw:
            try:
                validated.append(_validated_payload(item))
            finally:
                if isinstance(item, MutableMapping):
                    item.pop("private_key", None)
        return _materialize_validated_credentials(validated, backend)
    finally:
        # Covers validation, duplicate detection, config injection, transfer,
        # fingerprint verification, and probe failures. Keys already sent to
        # ``ssh-add`` were popped and zeroed at the transfer boundary; every
        # not-yet-transferred bytearray is destroyed here.
        for item in raw:
            if isinstance(item, MutableMapping):
                item.pop("private_key", None)
        _wipe_validated_private_keys(validated)


__all__ = [
    "ManagedRepositoryMaterializationError",
    "managed_repository_agent_launch_command",
    "managed_repository_agent_retirement_command",
    "managed_repository_agent_zero_command",
    "materialize_managed_repository_credentials",
    "repository_url_has_credentials",
]
