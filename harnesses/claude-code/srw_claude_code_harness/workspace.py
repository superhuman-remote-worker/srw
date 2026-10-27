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
* Afterwards the outer shell sends back a header on the channel's stdout,
  ``SRW1 \0 exit \0 stdout-bytes \0 stderr-bytes \0 spilled \0 cwd \0`` (NUL-separated,
  so any working directory parses), then at most ``capture_limit`` bytes of the
  command's stdout on stdout and of its stderr on stderr: the whole stream if it
  fits, otherwise its first 2/3 and last 1/3 around ``\n… [N bytes omitted] …\n``.
  With ``spill_path`` and more than ``spill_threshold`` bytes in total, the full
  stdout followed by the full stderr is first copied to that file.
* The inner shell records ``$PWD`` from an EXIT trap, so ``exit N`` and ``set -e``
  still carry the working directory to the next call.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import posixpath
import re
import shlex
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass

import asyncssh

from .bindings import WorkspaceAccess

_CHUNK = 65536
_BINARY_PROBE = 8192
_TIMEOUT_EXIT = 124
_KILL_EXITS = (137, -9)  # `timeout -k` escalated to SIGKILL (shell view / signal view)
_BACKSTOP_GRACE = 10.0
# Room on top of capture_limit for the header, the cwd and the elision line.
_CHANNEL_SLACK = 65536
_HEADER_TAG = b"SRW1"
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

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
# $5 spill path ('' for none), $6 spill threshold, $7 inner script.
_OUTER = r"""cwd=$1 root=$2 secs=$3 limit=$4 spill=$5 threshold=$6 inner=$7
mkdir -p -- "$HOME/.srw-run" && d=$(mktemp -d "$HOME/.srw-run/XXXXXX") || exit 98
timeout -k 5 "$secs" bash -c "$inner" bash "$d" "$cwd" "$root" >"$d/out" 2>"$d/err" </dev/null
rc=$?
o=$(( $(wc -c <"$d/out") ))
e=$(( $(wc -c <"$d/err") ))
spilled=0
if [ -n "$spill" ] && [ $(( o + e )) -gt "$threshold" ]; then
  mkdir -p -- "$(dirname -- "$spill")" &&
    { head -c "$o" -- "$d/out"; head -c "$e" -- "$d/err"; } >"$spill" && spilled=1
fi
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


class WorkspaceError(RuntimeError):
    pass


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


def _split_header(stdout: bytes) -> tuple[_Header | None, bytes]:
    """Separate the outer shell's header from the captured stdout."""
    parts = stdout.split(b"\0", 6)
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
    return None, b"" if stdout.startswith(_HEADER_TAG[: len(stdout)]) else stdout


async def _drain(reader: asyncssh.SSHReader, buffer: bytearray, cap: int) -> None:
    """Read a stream to its end, keeping at most ``cap`` bytes."""
    while chunk := await reader.read(_CHUNK):
        room = cap - len(buffer)
        if room > 0:
            buffer += chunk[:room]


class Workspace:
    def __init__(self, connection: asyncssh.SSHClientConnection, root: str):
        self._connection = connection
        self.root = root
        self.cwd = root

    @classmethod
    async def connect(cls, access: WorkspaceAccess) -> Workspace:
        root = posixpath.normpath(access.root)
        if not posixpath.isabs(root):
            raise WorkspaceError("workspace root must be an absolute path")
        try:
            connection = await asyncssh.connect(
                access.host,
                access.port,
                username=access.user,
                client_keys=[access.private_key_file],
                known_hosts=access.known_hosts_file,
                server_host_key_algs=["ssh-ed25519"],
                # Only the delivered files count: no ~/.ssh/config, no ssh-agent.
                config=None,
                agent_path=None,
            )
        except (OSError, asyncssh.Error) as exc:
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
        seconds = max(1, math.ceil(timeout))  # `timeout 0` would mean no limit
        limit = max(0, int(capture_limit))
        spill = self.resolve(spill_path) if spill_path is not None else None
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
        ]
        remote = "bash -c " + " ".join(shlex.quote(a) for a in arguments)
        loop = asyncio.get_running_loop()
        started = loop.time()
        process = await self._connection.create_process(
            remote, encoding=None, stdin=asyncssh.DEVNULL
        )
        stdout, stderr = bytearray(), bytearray()
        cap = limit + _CHANNEL_SLACK

        async def finish() -> None:
            await asyncio.gather(
                _drain(process.stdout, stdout, cap),
                _drain(process.stderr, stderr, cap),
            )
            await process.wait_closed()

        backstop = False
        try:
            await asyncio.wait_for(finish(), seconds + _BACKSTOP_GRACE)
        except asyncio.TimeoutError:
            # The remote `timeout` did not end the call in time; keep what arrived.
            backstop = True
            process.close()
        elapsed = loop.time() - started
        header, body = _split_header(bytes(stdout))
        if header is not None:
            exit_code = header.exit_code
            if header.cwd:
                new_cwd = posixpath.normpath(_decode(header.cwd))
                self.cwd = new_cwd if _within(self.root, new_cwd) else self.root
        elif backstop or process.returncode is None:
            exit_code = -1
        else:
            exit_code = process.returncode
        timed_out = (
            backstop
            or exit_code == _TIMEOUT_EXIT
            or (exit_code in _KILL_EXITS and elapsed >= seconds)
        )
        return CommandResult(
            exit_code=exit_code,
            stdout=_decode(body),
            stderr=_decode(bytes(stderr)),
            timed_out=timed_out,
            cwd=self.cwd,
            stdout_bytes=header.stdout_bytes if header else len(body),
            stderr_bytes=header.stderr_bytes if header else len(stderr),
            spilled_to=spill if header and header.spilled else None,
        )

    async def write_env_file(self, values: Mapping[str, str]) -> None:
        lines = []
        for name, value in values.items():
            if not _ENV_NAME.fullmatch(name):
                raise WorkspaceError("invalid environment variable name")
            lines.append(f"export {name}={shlex.quote(value)}\n")
        await self._checked(
            'umask 077 && cat > "$HOME/.srw-env" && chmod 600 "$HOME/.srw-env"',
            "".join(lines).encode(),
        )

    async def read_bytes(self, path: str, *, limit: int | None = None) -> bytes:
        target = shlex.quote(self.resolve(path))
        if limit is None:
            return await self._checked(f"cat -- {target}")
        return await self._checked(f"head -c {max(0, int(limit))} -- {target}")

    async def write_bytes(self, path: str, data: bytes) -> None:
        await self._checked(self._write_command(path), data)

    async def write_stream(self, path: str, chunks: AsyncIterator[bytes]) -> int:
        process = await self._connection.create_process(
            self._write_command(path), encoding=None
        )
        written = 0
        try:
            async for chunk in chunks:
                try:
                    process.stdin.write(chunk)
                    await process.stdin.drain()
                except (OSError, asyncssh.Error) as exc:
                    raise WorkspaceError(f"writing {path} failed: {exc}") from exc
                written += len(chunk)
            process.stdin.write_eof()
            _, stderr = await process.communicate()
        finally:
            process.close()
        if process.returncode != 0:
            raise WorkspaceError(_failure(process.returncode, stderr))
        return written

    async def read_stream(self, command: str) -> AsyncIterator[bytes]:
        """Yield the stdout of ``command`` run in the workspace root.

        Raises :class:`WorkspaceError` after the last chunk if it exits non-zero.
        """
        _reject_nul(command)
        remote = f"cd -- {shlex.quote(self.root)} || exit 97; {command}"
        process = await self._connection.create_process(
            remote, encoding=None, stdin=asyncssh.DEVNULL
        )
        # Drain stderr concurrently so a chatty command cannot stall stdout.
        stderr_task = asyncio.create_task(process.stderr.read())
        try:
            while chunk := await process.stdout.read(_CHUNK):
                yield chunk
            stderr = await stderr_task
            await process.wait_closed()
        finally:
            stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncssh.Error):
                await stderr_task
            process.close()
        if process.returncode != 0:
            raise WorkspaceError(_failure(process.returncode, stderr))

    async def exists(self, path: str) -> bool:
        rc, _, stderr = await self._exec(f"[ -e {shlex.quote(self.resolve(path))} ]")
        if rc not in (0, 1):
            raise WorkspaceError(_failure(rc, stderr))
        return rc == 0

    async def is_binary(self, path: str) -> bool:
        return b"\x00" in await self.read_bytes(path, limit=_BINARY_PROBE)

    def _write_command(self, path: str) -> str:
        target = self.resolve(path)
        parent = shlex.quote(posixpath.dirname(target))
        return f"mkdir -p -- {parent} && cat > {shlex.quote(target)}"

    async def _exec(
        self, command: str, data: bytes | None = None
    ) -> tuple[int, bytes, bytes]:
        # asyncssh sends EOF after `input` only when it is non-empty.
        result = await self._connection.run(
            command,
            input=data or None,
            encoding=None,
            stdin=asyncssh.PIPE if data else asyncssh.DEVNULL,
            check=False,
        )
        rc = result.returncode if result.returncode is not None else -1
        return rc, result.stdout or b"", result.stderr or b""

    async def _checked(self, command: str, data: bytes | None = None) -> bytes:
        rc, stdout, stderr = await self._exec(command, data)
        if rc != 0:
            raise WorkspaceError(_failure(rc, stderr))
        return stdout


def _failure(rc: int | None, stderr: bytes | None) -> str:
    detail = _decode(stderr).strip()
    return detail or f"workspace command failed with exit code {rc}"
