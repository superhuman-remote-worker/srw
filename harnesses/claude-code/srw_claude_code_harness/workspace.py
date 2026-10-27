"""SSH access to the run's workspace: commands, files and the environment file.

Shell semantics copy Claude Code's (spec B4): every :meth:`Workspace.run` starts a
fresh ``bash`` with ``$HOME/.srw-env`` sourced, in the working directory the
previous call ended in; shell variables do not survive between calls. Commands
run under coreutils ``timeout`` (without ``--foreground``), which puts them in
their own process group and kills the whole group when the time is up.
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

_MARK = "\x00SRW_CWD:"
_MARK_BYTES = _MARK.encode()
# A literal NUL cannot travel inside an SSH exec command (argv is NUL-terminated),
# so printf produces the marker's leading NUL from an octal escape.
_MARK_PRINTF = "\\000SRW_CWD:"
_CHUNK = 65536
_BINARY_PROBE = 8192
_CD_FAILED = 97
_TIMEOUT_EXIT = 124
_KILL_EXITS = (137, -9)  # `timeout -k` escalated to SIGKILL (shell view / signal view)
_BACKSTOP_GRACE = 10.0
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    cwd: str


class WorkspaceError(RuntimeError):
    pass


def _within(root: str, path: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _decode(data: bytes | None) -> str:
    return (data or b"").decode("utf-8", errors="replace")


def _reject_nul(command: str) -> None:
    # sshd treats a NUL inside an exec request as a protocol error.
    if "\x00" in command:
        raise WorkspaceError("command contains a NUL byte")


def _split_marker(stderr: bytes) -> tuple[bytes, str | None]:
    """Remove the final working-directory marker line from stderr."""
    start = stderr.rfind(_MARK_BYTES)
    if start < 0:
        return stderr, None
    end = stderr.find(b"\n", start)
    value_end = len(stderr) if end < 0 else end
    pwd = _decode(stderr[start + len(_MARK_BYTES) : value_end])
    rest = b"" if end < 0 else stderr[end + 1 :]
    return stderr[:start] + rest, pwd


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

    async def run(self, command: str, *, timeout: float = 120.0) -> CommandResult:
        _reject_nul(command)
        seconds = max(1, math.ceil(timeout))  # `timeout 0` would mean no limit
        script = (
            f'[ -f "$HOME/.srw-env" ] && . "$HOME/.srw-env"; '
            f"cd -- {shlex.quote(self.cwd)} || exit {_CD_FAILED}; "
            f"{command}\n"
            f"__srw_rc=$?; printf '{_MARK_PRINTF}%s\\n' \"$PWD\" >&2; exit $__srw_rc"
        )
        remote = f"timeout -k 5 {seconds} bash -c {shlex.quote(script)}"
        loop = asyncio.get_running_loop()
        started = loop.time()
        process = await self._connection.create_process(
            remote, encoding=None, stdin=asyncssh.DEVNULL
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), seconds + _BACKSTOP_GRACE
            )
        except asyncio.TimeoutError:
            # Backstop: the remote `timeout` did not end the session in time.
            process.close()
            return CommandResult(-1, "", "", True, self.cwd)
        elapsed = loop.time() - started
        exit_code = process.returncode if process.returncode is not None else -1
        stderr, pwd = _split_marker(stderr or b"")
        if pwd is not None:
            new_cwd = posixpath.normpath(pwd)
            self.cwd = new_cwd if _within(self.root, new_cwd) else self.root
        elif exit_code == _CD_FAILED:
            # The previous directory is gone; start the next command at the root.
            self.cwd = self.root
        timed_out = exit_code == _TIMEOUT_EXIT or (
            exit_code in _KILL_EXITS and elapsed >= seconds
        )
        return CommandResult(
            exit_code, _decode(stdout), _decode(stderr), timed_out, self.cwd
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
        remote = f"cd -- {shlex.quote(self.root)} || exit {_CD_FAILED}; {command}"
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
