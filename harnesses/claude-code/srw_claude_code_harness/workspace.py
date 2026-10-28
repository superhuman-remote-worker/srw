"""SSH access to the run's workspace: commands, files and the environment file.

Shell semantics copy Claude Code's (spec B4): every :meth:`Workspace.run` starts a
fresh ``bash`` with ``$HOME/.srw-env`` sourced and pagers disabled, in the working
directory the previous call ended in; shell variables do not survive between calls.

Each call runs two shells on the workspace (see ``_OUTER`` and ``_INNER``):

* The outer ``bash`` is not under a time limit. It makes a per-call directory under
  ``$HOME/.srw-run`` and runs the command as
  ``timeout -k 5 N bash -c <inner> >"$d/out" 2>"$d/err" </dev/null``. Coreutils
  ``timeout`` (without ``--foreground``) puts the command in its own process group
  and kills the whole group when the time is up. Background jobs inherit the two
  files, not the SSH channel, so the call ends when the command does.
* Afterwards the outer shell sends back the call's marker and a header on the
  channel's stdout, ``SRW1 \0 exit \0 stdout-bytes \0 stderr-bytes \0 spilled \0
  cwd \0`` (NUL-separated, so any working directory parses), then at most
  ``capture_limit`` bytes of the command's stdout on stdout and, after the marker
  again, of its stderr on stderr: the whole stream if it fits, otherwise its first
  2/3 and last 1/3 around ``\n… [N bytes omitted] …\n``. With ``spill_path`` and
  more than ``spill_threshold`` bytes in total, the full stdout followed by the
  full stderr is first copied to that file. SIGPIPE is ignored from then on, so a
  closed channel cannot stop the outer shell before it removes the directory.
* The inner shell records ``$PWD`` from an EXIT trap, so ``exit N`` and ``set -e``
  still carry the working directory to the next call.

The model controls the workspace, including the login shell's start-up files
(``~/.bashrc``), which run before every command sent over SSH. So every helper:

* is framed: the command prints a per-call random marker first (on stdout and on
  stderr), and only what follows it is the command's output. What a start-up file
  prints before it is dropped; the marker must appear within
  ``FRAME_SEARCH_LIMIT_BYTES`` of stdout. The marker is printed in two halves, so
  an echo of the command line never contains it;
* is bounded: a total time limit on the client (``asyncio.wait_for``) plus
  ``timeout`` on the workspace, so a remote process does not outlive the channel;
  stdout is capped by the caller's byte limit and stderr at ``STDERR_LIMIT_BYTES``;
* is started with ``exec``, so an EXIT trap a start-up file set cannot print after
  the command, and its own ``bash`` with ``--norc``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import math
import posixpath
import re
import secrets
import shlex
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass

import asyncssh


@dataclass(frozen=True)
class WorkspaceAccess:
    host: str
    port: int
    user: str
    root: str
    private_key_file: str
    known_hosts_file: str | None = None
    # SHA256 fingerprint the job pins for the workspace host key (checked on connect).
    host_key_fingerprint: str | None = None


# Tool-facing file operations (stat, read, write, exists) end within this long.
FILE_OP_TIMEOUT_SECONDS = 120.0
# Every helper keeps at most this much of a command's stderr.
STDERR_LIMIT_BYTES = 64 * 1024
# The per-call marker must appear within this many bytes of stdout.
FRAME_SEARCH_LIMIT_BYTES = 1024 * 1024
# ``read_bytes`` without a limit refuses a file larger than this.
READ_LIMIT_BYTES = 64 * 1024 * 1024

_CHUNK = 65536
_BINARY_PROBE = 8192
_TIMEOUT_EXIT = 124
_KILL_EXITS = (137, -9)  # `timeout -k` escalated to SIGKILL (shell view / signal view)
_KILL_AFTER = 5
_BACKSTOP_GRACE = 10.0
# Client-side bound of a helper: its timeout plus this, so the remote `timeout`
# usually reports first.
_HELPER_GRACE = 2.0
# Default stdout cap of a helper whose output is not file content.
_HELPER_STDOUT_LIMIT = 1024 * 1024
# Room on top of capture_limit for the header, the cwd and the elision line.
_CHANNEL_SLACK = 65536
_HEADER_TAG = b"SRW1"
_FRAME_TAG = "SRW-FRAME-"
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PREAMBLE_TOO_LONG = (
    "the workspace shell printed more than "
    f"{FRAME_SEARCH_LIMIT_BYTES} bytes before the command started "
    "(check the shell start-up files such as ~/.bashrc)"
)

# Positional parameters: $1 per-call dir, $2 cwd, $3 root. The command follows it.
# A literal NUL cannot travel in an SSH exec request, so printf makes the NULs.
_INNER = r"""__srw_d=$1 __srw_cwd=$2 __srw_root=$3
set --
readonly __srw_d
[ -f "$HOME/.srw-env" ] && . "$HOME/.srw-env"
export PAGER=cat GIT_PAGER=cat
if ! cd -- "$__srw_cwd" 2>/dev/null; then
  cd -- "$__srw_root" || exit 97
  printf 'Shell cwd was reset to %s\n' "$__srw_root" >&2
fi
unset __srw_cwd __srw_root
trap 'printf "%s" "$PWD" >"$__srw_d/cwd"' EXIT
"""

# Positional parameters: $1 cwd, $2 root, $3 seconds, $4 capture limit,
# $5 spill path ('' for none), $6 spill threshold, $7 inner script, $8 marker token.
_OUTER = r"""cwd=$1 root=$2 secs=$3 limit=$4 spill=$5 threshold=$6 inner=$7 token=$8
mkdir -p -- "$HOME/.srw-run" && d=$(mktemp -d "$HOME/.srw-run/XXXXXX") || exit 98
timeout -k 5 "$secs" bash -c "$inner" bash "$d" "$cwd" "$root" >"$d/out" 2>"$d/err" </dev/null
rc=$?
# From here on a closed channel must not kill this shell before the clean-up.
trap '' PIPE
o=$(( $(wc -c <"$d/out") ))
e=$(( $(wc -c <"$d/err") ))
spilled=0
if [ -n "$spill" ] && [ $(( o + e )) -gt "$threshold" ]; then
  mkdir -p -- "$(dirname -- "$spill")" &&
    { head -c "$o" -- "$d/out"; head -c "$e" -- "$d/err"; } >"$spill" && spilled=1
fi
printf '%s%s' SRW-FRAME- "$token"
printf 'SRW1\0%s\0%s\0%s\0%s\0' "$rc" "$o" "$e" "$spilled"
[ -f "$d/cwd" ] && cat -- "$d/cwd"
printf '\0'
emit() {
  # Sizes were taken once; a background job may still append, so never read more.
  if [ "$2" -le "$limit" ]; then
    head -c "$2" -- "$1"
  else
    h=$(( limit * 2 / 3 ))
    t=$(( limit - h ))
    head -c "$h" -- "$1"
    printf '\n… [%s bytes omitted] …\n' "$(( $2 - h - t ))"
    tail -c "+$(( $2 - t + 1 ))" -- "$1" | head -c "$t"
  fi
}
emit "$d/out" "$o"
printf '%s%s' SRW-FRAME- "$token" >&2
emit "$d/err" "$e" >&2
rm -rf -- "$d"
exit "$rc"
"""


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    cwd: str
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    spilled_to: str | None = None


# Connecting to a fresh workspace: total budget, per-attempt cap and backoff.
CONNECT_DEADLINE_SECONDS = 90.0
CONNECT_ATTEMPT_SECONDS = 10.0
CONNECT_RETRY_INITIAL_SECONDS = 0.5
CONNECT_RETRY_MAX_SECONDS = 5.0


class WorkspaceError(RuntimeError):
    pass


class _FrameError(Exception):
    """The marker did not appear within ``FRAME_SEARCH_LIMIT_BYTES`` of stdout."""


class _Overflow(Exception):
    """A helper's stdout payload exceeded its byte limit."""


@dataclass(frozen=True)
class _Header:
    exit_code: int
    stdout_bytes: int
    stderr_bytes: int
    spilled: bool
    cwd: bytes


def _within(root: str, path: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _decode(data: bytes | None) -> str:
    return (data or b"").decode("utf-8", errors="replace")


def _reject_nul(command: str) -> None:
    # sshd treats a NUL inside an exec request as a protocol error.
    if "\x00" in command:
        raise WorkspaceError("command contains a NUL byte")


def _seconds(timeout: float) -> int:
    return max(1, math.ceil(timeout))  # `timeout 0` would mean no limit


def _new_token() -> tuple[str, bytes]:
    token = secrets.token_hex(16)
    return token, f"{_FRAME_TAG}{token}".encode()


def _helper_remote(script: str, token: str, seconds: int) -> str:
    """The exec request for a framed helper: the marker first, then ``script``."""
    mark = f"printf '%s%s' {_FRAME_TAG} {token}"
    # A start-up file may have hung and ignored SIGPIPE: if the client has
    # already given up, the marker write fails and the script must not run late.
    body = f"{mark} || exit 1; {mark} >&2 || exit 1\n{script}\n"
    return f"exec timeout -k {_KILL_AFTER} {seconds} bash --norc -c {shlex.quote(body)}"


class _Framed:
    """One channel stream whose payload starts after the call's marker.

    Before the marker is seen only a tail is kept: enough to find a marker split
    across reads, plus ``keep`` bytes for diagnostics (``preamble``).
    """

    def __init__(self, marker: bytes, *, search_limit: int | None, keep: int = 0):
        self._marker = marker
        self._search_limit = search_limit
        self._keep = max(keep, len(marker) - 1)
        self._pre = bytearray()
        self._seen = 0
        self.found = False

    @property
    def preamble(self) -> bytes:
        return bytes(self._pre)

    def feed(self, chunk: bytes) -> bytes:
        """The payload bytes in ``chunk`` (nothing before the marker)."""
        if self.found:
            return chunk
        self._seen += len(chunk)
        self._pre += chunk
        index = self._pre.find(self._marker)
        if index >= 0:
            self.found = True
            payload = bytes(self._pre[index + len(self._marker) :])
            self._pre.clear()
            return payload
        if self._search_limit is not None and self._seen > self._search_limit + len(
            self._marker
        ):
            raise _FrameError
        if len(self._pre) > self._keep:
            del self._pre[: len(self._pre) - self._keep]
        return b""


async def _collect(
    reader: asyncssh.SSHReader,
    framed: _Framed,
    buffer: bytearray,
    cap: int,
    *,
    strict: bool,
) -> None:
    """Read a stream to its end, keeping at most ``cap`` payload bytes; with
    ``strict`` more than ``cap`` is an error, otherwise the rest is dropped."""
    while chunk := await reader.read(_CHUNK):
        payload = framed.feed(chunk)
        if not payload:
            continue
        room = cap - len(buffer)
        if len(payload) > room:
            if strict:
                raise _Overflow
            payload = payload[: max(0, room)]
        buffer += payload


async def _run_jobs(
    readers: Iterable[Awaitable[None]], feeder: Awaitable[None] | None = None
) -> None:
    """Await the stream readers while ``feeder`` writes stdin alongside.

    Once the readers are done the remote has exited, so a feeder still running is
    cancelled. The first failure cancels the rest and is raised.
    """
    reader_tasks = [asyncio.ensure_future(r) for r in readers]
    tasks = list(reader_tasks)
    if feeder is not None:
        tasks.append(asyncio.ensure_future(feeder))
    try:
        while True:
            for task in tasks:
                if task.done() and not task.cancelled() and task.exception():
                    raise task.exception()  # type: ignore[misc]
            if all(task.done() for task in reader_tasks):
                return
            pending = [task for task in tasks if not task.done()]
            await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _split_header(payload: bytes) -> tuple[_Header | None, bytes]:
    """Separate the outer shell's header from the captured stdout."""
    parts = payload.split(b"\0", 6)
    if len(parts) == 7 and parts[0] == _HEADER_TAG:
        try:
            header = _Header(
                int(parts[1]), int(parts[2]), int(parts[3]), parts[4] == b"1", parts[5]
            )
        except ValueError:
            pass
        else:
            return header, parts[6]
    # No complete header: the wrapper failed early or the backstop cut it short.
    # The command's own output only ever follows the header.
    return None, b""


def _count(stdout: bytes) -> int:
    try:
        return int(stdout.strip())
    except ValueError:
        raise WorkspaceError("unexpected output from the workspace") from None


_FINGERPRINT = re.compile(r"SHA256:[A-Za-z0-9+/]{43}")


class _PinnedHostKey(asyncssh.SSHClient):
    """Accepts exactly one SSH host key, by its OpenSSH SHA256 fingerprint, as
    SRW's ``_ExactSHA256HostKeyPolicy`` does: judged during the handshake."""

    def __init__(self, expected: str):
        self._expected = expected
        self.mismatched = False

    def validate_host_public_key(self, host, addr, port, key) -> bool:
        if hmac.compare_digest(key.get_fingerprint(), self._expected):
            return True
        self.mismatched = True
        return False


class Workspace:
    def __init__(self, connection: asyncssh.SSHClientConnection, root: str):
        self._connection = connection
        self.root = root
        self.cwd = root

    @classmethod
    async def connect(
        cls,
        access: WorkspaceAccess,
        *,
        deadline_seconds: float = CONNECT_DEADLINE_SECONDS,
    ) -> Workspace:
        """Connect, retrying a refused, unreachable or dropped connection until
        ``deadline_seconds`` have passed.

        A fresh workspace Pod's sshd and the cluster's network policy admitting
        this harness Pod both lag behind the Pod's readiness, and on some CNIs a
        not-yet-admitted connection is refused rather than dropped. SSH-level
        errors (a rejected host key, a refused client key) are permanent and
        fail at once.
        """
        root = posixpath.normpath(access.root)
        if not posixpath.isabs(root):
            raise WorkspaceError("workspace root must be an absolute path")
        # SRW's agent pins an attested workspace by an exact OpenSSH SHA256
        # fingerprint (RemoteBackend): a malformed one is refused, and a
        # Kubernetes worker without one is refused before connecting.
        expected = access.host_key_fingerprint
        if access.known_hosts_file is None:
            if not expected:
                raise WorkspaceError("no host key to verify the workspace against")
            if not _FINGERPRINT.fullmatch(expected):
                raise WorkspaceError("workspace host key fingerprint is malformed")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + deadline_seconds
        wait = CONNECT_RETRY_INITIAL_SECONDS
        attempts = 0
        while True:
            attempts += 1
            remaining = deadline - loop.time()
            pinned = None if access.known_hosts_file else _PinnedHostKey(expected)
            try:
                connection = await asyncssh.connect(
                    access.host,
                    access.port,
                    username=access.user,
                    client_keys=[access.private_key_file],
                    # known_hosts=() trusts no key by file, so asyncssh asks
                    # ``_PinnedHostKey`` during the handshake, before any auth.
                    known_hosts=access.known_hosts_file or (),
                    client_factory=(lambda: pinned) if pinned else None,
                    server_host_key_algs=["ssh-ed25519"],
                    # Only the delivered files count: no ~/.ssh/config, no ssh-agent.
                    config=None,
                    agent_path=None,
                    connect_timeout=max(1.0, min(CONNECT_ATTEMPT_SECONDS, remaining)),
                )
            except (OSError, asyncssh.ConnectionLost, asyncio.TimeoutError) as exc:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise WorkspaceError(
                        f"cannot connect to the workspace: {exc} "
                        f"({attempts} attempts in {deadline_seconds:g} s)"
                    ) from exc
                await asyncio.sleep(min(wait, remaining))
                wait = min(wait * 2, CONNECT_RETRY_MAX_SECONDS)
                continue
            except asyncssh.Error as exc:
                if pinned is not None and pinned.mismatched:
                    raise WorkspaceError("workspace host key mismatch") from exc
                raise WorkspaceError(f"cannot connect to the workspace: {exc}") from exc
            return cls(connection, root)

    async def close(self) -> None:
        self._connection.close()
        await self._connection.wait_closed()

    def resolve(self, path: str) -> str:
        if "\x00" in path:
            raise WorkspaceError("path contains a NUL byte")
        joined = path if path.startswith("/") else posixpath.join(self.cwd, path)
        resolved = posixpath.normpath(joined)
        if not _within(self.root, resolved):
            raise WorkspaceError("path outside the workspace")
        return resolved

    async def run(
        self,
        command: str,
        *,
        timeout: float = 120.0,
        capture_limit: int = 262_144,
        spill_path: str | None = None,
        spill_threshold: int = 30_000,
    ) -> CommandResult:
        _reject_nul(command)
        seconds = _seconds(timeout)
        limit = max(0, int(capture_limit))
        spill = self.resolve(spill_path) if spill_path is not None else None
        token, marker = _new_token()
        arguments = [
            _OUTER,
            "bash",
            self.cwd,
            self.root,
            str(seconds),
            str(limit),
            spill or "",
            str(max(0, int(spill_threshold))),
            f"{_INNER}{command}\n",
            token,
        ]
        remote = "exec bash --norc -c " + " ".join(shlex.quote(a) for a in arguments)
        loop = asyncio.get_running_loop()
        started = loop.time()
        stdout, stderr = bytearray(), bytearray()
        cap = limit + _CHANNEL_SLACK
        out = _Framed(marker, search_limit=FRAME_SEARCH_LIMIT_BYTES)
        err = _Framed(marker, search_limit=None, keep=cap)
        processes: list[asyncssh.SSHClientProcess] = []

        async def finish() -> None:
            process = await self._connection.create_process(
                remote, encoding=None, stdin=asyncssh.DEVNULL
            )
            processes.append(process)
            await _run_jobs(
                [
                    _collect(process.stdout, out, stdout, cap, strict=False),
                    _collect(process.stderr, err, stderr, cap, strict=False),
                ]
            )
            await process.wait_closed()

        backstop = False
        try:
            await asyncio.wait_for(finish(), seconds + _BACKSTOP_GRACE)
        except asyncio.TimeoutError:
            # The remote `timeout` did not end the call in time; keep what arrived.
            backstop = True
        except _FrameError:
            raise WorkspaceError(_PREAMBLE_TOO_LONG) from None
        except (OSError, asyncssh.Error) as exc:
            raise WorkspaceError(f"workspace command failed: {exc}") from exc
        finally:
            for process in processes:
                process.close()
        elapsed = loop.time() - started
        returncode = processes[0].returncode if processes else None
        header, body = _split_header(bytes(stdout)) if out.found else (None, b"")
        if header is not None:
            exit_code = header.exit_code
            if header.cwd:
                new_cwd = posixpath.normpath(_decode(header.cwd))
                self.cwd = new_cwd if _within(self.root, new_cwd) else self.root
        elif backstop or returncode is None:
            exit_code = -1
        else:
            exit_code = returncode
        timed_out = (
            backstop
            or exit_code == _TIMEOUT_EXIT
            or (exit_code in _KILL_EXITS and elapsed >= seconds)
        )
        captured = bytes(stderr) if err.found else err.preamble
        return CommandResult(
            exit_code=exit_code,
            stdout=_decode(body),
            stderr=_decode(captured),
            timed_out=timed_out,
            cwd=self.cwd,
            stdout_bytes=header.stdout_bytes if header else len(body),
            stderr_bytes=header.stderr_bytes if header else len(captured),
            spilled_to=spill if header and header.spilled else None,
        )

    async def write_env_file(
        self, values: Mapping[str, str], *, timeout: float = FILE_OP_TIMEOUT_SECONDS
    ) -> None:
        lines = []
        for name, value in values.items():
            if not _ENV_NAME.fullmatch(name):
                raise WorkspaceError("invalid environment variable name")
            lines.append(f"export {name}={shlex.quote(value)}\n")
        data = "".join(lines).encode()
        stdout = await self._checked(
            'umask 077 && cat > "$HOME/.srw-env" && chmod 600 "$HOME/.srw-env" '
            '&& wc -c < "$HOME/.srw-env"',
            data,
            timeout=timeout,
        )
        if _count(stdout) != len(data):
            raise WorkspaceError("writing the environment file failed")

    async def read_bytes(
        self,
        path: str,
        *,
        limit: int | None = None,
        timeout: float = FILE_OP_TIMEOUT_SECONDS,
    ) -> bytes:
        """The file's bytes, at most ``limit``; without a limit a file larger than
        ``READ_LIMIT_BYTES`` is an error."""
        target = shlex.quote(self.resolve(path))
        if limit is None:
            script, cap = f"cat -- {target}", READ_LIMIT_BYTES
        else:
            cap = max(0, int(limit))
            script = f"head -c {cap} -- {target}"
        return await self._checked(script, timeout=timeout, stdout_limit=cap)

    async def stat(
        self, path: str, *, timeout: float = FILE_OP_TIMEOUT_SECONDS
    ) -> tuple[str, int] | None:
        """``(type, size)`` of the file (following symlinks; the type as ``stat``
        names it, such as ``regular file``), or None when it cannot be stat'ed."""
        target = shlex.quote(self.resolve(path))
        rc, stdout, _ = await self._exec(
            f"stat -L -c '%F|%s' -- {target}", timeout=timeout
        )
        if rc != 0:
            return None
        kind, _, size = _decode(stdout).strip().partition("|")
        try:
            return kind, int(size)
        except ValueError:
            raise WorkspaceError("unexpected output from stat") from None

    async def write_bytes(
        self, path: str, data: bytes, *, timeout: float = FILE_OP_TIMEOUT_SECONDS
    ) -> None:
        """Write ``data`` to the file, creating its parents; an existing target that
        is not a regular file is refused."""
        stdout = await self._checked(self._write_script(path), data, timeout=timeout)
        received = _count(stdout)
        if received != len(data):
            raise WorkspaceError(
                f"writing {path} failed: the workspace received {received} of "
                f"{len(data)} bytes"
            )

    async def write_stream(
        self,
        path: str,
        chunks: AsyncIterator[bytes],
        *,
        timeout: float = FILE_OP_TIMEOUT_SECONDS,
    ) -> int:
        """Stream ``chunks`` into the file; returns the byte count. Exceptions
        raised by ``chunks`` propagate unchanged."""
        written = 0
        complete = False
        broken: BaseException | None = None

        async def feed(stdin: asyncssh.SSHWriter) -> None:
            nonlocal written, complete, broken
            async for chunk in chunks:
                try:
                    stdin.write(chunk)
                    await stdin.drain()
                except (OSError, asyncssh.Error) as exc:
                    broken = exc  # the remote's exit code says why, if it can
                    return
                written += len(chunk)
            complete = True
            with contextlib.suppress(OSError, asyncssh.Error):
                stdin.write_eof()

        stdout = await self._checked(self._write_script(path), feed, timeout=timeout)
        if broken is not None or not complete:
            reason = broken or "the workspace stopped reading"
            raise WorkspaceError(f"writing {path} failed: {reason}")
        received = _count(stdout)
        if received != written:
            raise WorkspaceError(
                f"writing {path} failed: the workspace received {received} of "
                f"{written} bytes"
            )
        return written

    async def read_stream(
        self,
        command: str,
        *,
        timeout: float = FILE_OP_TIMEOUT_SECONDS,
        limit: int | None = None,
    ) -> AsyncIterator[bytes]:
        """Yield the stdout of ``command`` run in the workspace root.

        The whole stream ends within ``timeout`` (the consumer's time counts) and
        carries at most ``limit`` bytes. Raises :class:`WorkspaceError` after the
        last chunk if the command exits non-zero, and when a bound is passed.
        """
        _reject_nul(command)
        seconds = _seconds(timeout)
        token, marker = _new_token()
        remote = _helper_remote(
            f"cd -- {shlex.quote(self.root)} || exit 97\n{command}", token, seconds
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds + _HELPER_GRACE

        async def bounded(awaitable: Awaitable):
            remaining = deadline - loop.time()
            try:
                if remaining <= 0:
                    raise asyncio.TimeoutError
                return await asyncio.wait_for(awaitable, remaining)
            except asyncio.TimeoutError:
                if asyncio.iscoroutine(awaitable):
                    awaitable.close()
                raise WorkspaceError(
                    f"workspace command timed out after {seconds} s"
                ) from None
            except (OSError, asyncssh.Error) as exc:
                raise WorkspaceError(f"workspace command failed: {exc}") from exc

        process = await bounded(
            self._connection.create_process(
                remote, encoding=None, stdin=asyncssh.DEVNULL
            )
        )
        out = _Framed(marker, search_limit=FRAME_SEARCH_LIMIT_BYTES)
        err = _Framed(marker, search_limit=None, keep=STDERR_LIMIT_BYTES)
        stderr = bytearray()
        # Drain stderr concurrently so a chatty command cannot stall stdout.
        stderr_task = asyncio.ensure_future(
            _collect(process.stderr, err, stderr, STDERR_LIMIT_BYTES, strict=False)
        )
        total = 0
        try:
            while chunk := await bounded(process.stdout.read(_CHUNK)):
                try:
                    payload = out.feed(chunk)
                except _FrameError:
                    raise WorkspaceError(_PREAMBLE_TOO_LONG) from None
                if not payload:
                    continue
                total += len(payload)
                if limit is not None and total > limit:
                    raise WorkspaceError(f"output exceeds {limit} bytes")
                yield payload
            await bounded(stderr_task)
            await bounded(process.wait_closed())
        finally:
            stderr_task.cancel()
            with contextlib.suppress(BaseException):
                await stderr_task
            process.close()
        detail = bytes(stderr) if err.found else err.preamble
        rc = process.returncode
        if not out.found:
            raise WorkspaceError(_not_started(rc, detail))
        if rc == _TIMEOUT_EXIT:
            raise WorkspaceError(f"workspace command timed out after {seconds} s")
        if rc != 0:
            raise WorkspaceError(_failure(rc, detail))

    async def exists(
        self, path: str, *, timeout: float = FILE_OP_TIMEOUT_SECONDS
    ) -> bool:
        target = shlex.quote(self.resolve(path))
        rc, stdout, stderr = await self._exec(
            f"if [ -e {target} ]; then printf 1; else printf 0; fi", timeout=timeout
        )
        if rc != 0 or stdout not in (b"0", b"1"):
            raise WorkspaceError(_failure(rc, stderr))
        return stdout == b"1"

    async def is_binary(
        self, path: str, *, timeout: float = FILE_OP_TIMEOUT_SECONDS
    ) -> bool:
        probe = await self.read_bytes(path, limit=_BINARY_PROBE, timeout=timeout)
        return b"\x00" in probe

    def _write_script(self, path: str) -> str:
        target = self.resolve(path)
        quoted = shlex.quote(target)
        parent = shlex.quote(posixpath.dirname(target))
        return (
            f"if [ -e {quoted} ] && [ ! -f {quoted} ]; then\n"
            f"  printf '%s is not a regular file\\n' {quoted} >&2; exit 1\n"
            "fi\n"
            f"mkdir -p -- {parent} && cat > {quoted} && wc -c < {quoted}"
        )

    async def _exec(
        self,
        command: str,
        data: bytes | Callable[[asyncssh.SSHWriter], Awaitable[None]] | None = None,
        *,
        timeout: float = FILE_OP_TIMEOUT_SECONDS,
        stdout_limit: int = _HELPER_STDOUT_LIMIT,
    ) -> tuple[int, bytes, bytes]:
        """Run ``command`` with ``bash`` on the workspace, framed and bounded.

        ``data`` is sent on stdin (bytes, or a coroutine function that writes it).
        Returns the exit code, the stdout after the marker (more than
        ``stdout_limit`` bytes is an error) and the stderr after the marker (at
        most ``STDERR_LIMIT_BYTES``). Raises :class:`WorkspaceError` when the
        command did not start, timed out or the connection failed.
        """
        _reject_nul(command)
        seconds = _seconds(timeout)
        token, marker = _new_token()
        remote = _helper_remote(command, token, seconds)
        if isinstance(data, bytes):
            payload = data

            async def feed(stdin: asyncssh.SSHWriter) -> None:
                # The remote may have exited early; its exit code says why.
                with contextlib.suppress(OSError, asyncssh.Error):
                    if payload:
                        stdin.write(payload)
                        await stdin.drain()
                    stdin.write_eof()

            data = feed
        out = _Framed(marker, search_limit=FRAME_SEARCH_LIMIT_BYTES)
        err = _Framed(marker, search_limit=None, keep=STDERR_LIMIT_BYTES)
        stdout, stderr = bytearray(), bytearray()
        processes: list[asyncssh.SSHClientProcess] = []

        async def call() -> int | None:
            process = await self._connection.create_process(
                remote,
                encoding=None,
                stdin=asyncssh.PIPE if data is not None else asyncssh.DEVNULL,
            )
            processes.append(process)
            await _run_jobs(
                [
                    _collect(process.stdout, out, stdout, stdout_limit, strict=True),
                    _collect(
                        process.stderr, err, stderr, STDERR_LIMIT_BYTES, strict=False
                    ),
                ],
                data(process.stdin) if data is not None else None,
            )
            await process.wait_closed()
            return process.returncode

        try:
            rc = await asyncio.wait_for(call(), seconds + _HELPER_GRACE)
        except asyncio.TimeoutError:
            raise WorkspaceError(
                f"workspace command timed out after {seconds} s"
            ) from None
        except _FrameError:
            raise WorkspaceError(_PREAMBLE_TOO_LONG) from None
        except _Overflow:
            raise WorkspaceError(
                f"workspace command output exceeds {stdout_limit} bytes"
            ) from None
        except (OSError, asyncssh.Error) as exc:
            raise WorkspaceError(f"workspace command failed: {exc}") from exc
        finally:
            for process in processes:
                process.close()
        detail = bytes(stderr) if err.found else err.preamble
        if not out.found:
            raise WorkspaceError(_not_started(rc, detail))
        if rc == _TIMEOUT_EXIT:
            raise WorkspaceError(f"workspace command timed out after {seconds} s")
        return (rc if rc is not None else -1), bytes(stdout), detail

    async def _checked(
        self,
        command: str,
        data: bytes | Callable[[asyncssh.SSHWriter], Awaitable[None]] | None = None,
        *,
        timeout: float = FILE_OP_TIMEOUT_SECONDS,
        stdout_limit: int = _HELPER_STDOUT_LIMIT,
    ) -> bytes:
        rc, stdout, stderr = await self._exec(
            command, data, timeout=timeout, stdout_limit=stdout_limit
        )
        if rc != 0:
            raise WorkspaceError(_failure(rc, stderr))
        return stdout


def _failure(rc: int | None, stderr: bytes | None) -> str:
    detail = _decode(stderr).strip()
    return detail or f"workspace command failed with exit code {rc}"


def _not_started(rc: int | None, stderr: bytes) -> str:
    detail = _decode(stderr).strip()
    reason = f": {detail}" if detail else f" (exit code {rc})"
    return f"the workspace shell did not run the command{reason}"
