"""In-process SSH server that runs commands for real in a local directory.

Stands in for the workspace sshd in the Claude Code harness tests: every exec
request runs through ``/bin/sh -c`` with the workspace root as working directory
and ``HOME``, so the harness's shell wrapper, ``timeout`` and file transfers are
exercised against real processes. Channels are binary (``encoding=None``).

``prefix`` simulates the login shell's start-up files (``~/.bashrc``): when set,
it runs in the same shell before every command, as a start-up file would. Like
sshd, a channel the client closes closes the command's pipes, so its next write
fails (SIGPIPE) and its stdin ends.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import os
import shutil
import socket
import signal
import tempfile
from pathlib import Path

import asyncssh
from srw_claude_code_harness.workspace import WorkspaceAccess

_CHUNK = 65536


class _Server(asyncssh.SSHServer):
    def __init__(self, auth_attempts: list[str]):
        self._auth_attempts = auth_attempts

    def begin_auth(self, username: str) -> bool:
        self._auth_attempts.append(username)
        return False


async def _pump_stdin(source: asyncssh.SSHReader, child) -> None:
    try:
        while True:
            data = await source.read(_CHUNK)
            if not data:
                break
            child.stdin.write(data)
            await child.stdin.drain()
    except (BrokenPipeError, ConnectionResetError, asyncssh.Error):
        pass
    finally:
        with contextlib.suppress(Exception):
            child.stdin.close()


async def _pump_output(source: asyncio.StreamReader, target) -> None:
    while True:
        data = await source.read(_CHUNK)
        if not data:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError, asyncssh.Error):
            target.write(data)
            await target.drain()


class LocalSshd:
    def __init__(self, root: Path, key_dir: str):
        self._root = root
        self._key_dir = key_dir
        self._server: asyncssh.SSHAcceptor | None = None
        self._sessions: set[int] = set()
        self.access: WorkspaceAccess | None = None
        self.commands: list[str] = []
        self.prefix: str | None = None
        self.auth_attempts: list[str] = []
        self.fingerprint: str = ""

    @classmethod
    async def start(cls, root: Path, *, listen: bool = True) -> LocalSshd:
        """Create the keys and the access descriptor; with ``listen=False`` the
        port is reserved but nothing accepts until :meth:`listen`."""
        root.mkdir(parents=True, exist_ok=True)
        key_dir = tempfile.mkdtemp(prefix="srw-harness-sshd-")
        host_key = asyncssh.generate_private_key("ssh-ed25519")
        client_key = asyncssh.generate_private_key("ssh-ed25519")
        key_file = os.path.join(key_dir, "id_ed25519")
        with open(key_file, "wb") as handle:
            handle.write(client_key.export_private_key())
        # SRW delivers the key root-owned with mode 0444; keep the same mode here.
        os.chmod(key_file, 0o444)
        instance = cls(root, key_dir)
        instance._host_key = host_key
        if listen:
            port = await instance.listen(0)
        else:
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
        instance.fingerprint = "SHA256:" + base64.b64encode(
            hashlib.sha256(host_key.public_data).digest()
        ).decode().rstrip("=")
        algorithm, blob = host_key.export_public_key().decode().split()[:2]
        known_hosts_file = os.path.join(key_dir, "known_hosts")
        with open(known_hosts_file, "w") as handle:
            handle.write(f"[127.0.0.1]:{port} {algorithm} {blob}\n")
        instance.access = WorkspaceAccess(
            host="127.0.0.1",
            port=port,
            user="tester",
            root=str(root),
            private_key_file=key_file,
            known_hosts_file=known_hosts_file,
        )
        return instance

    async def listen(self, port: int | None = None) -> int:
        """Start accepting on ``port`` (default: the reserved one); returns it."""
        self._server = await asyncssh.create_server(
            lambda: _Server(self.auth_attempts),
            "127.0.0.1",
            self.access.port if port is None else port,
            server_host_keys=[self._host_key],
            process_factory=self._handle,
            encoding=None,
        )
        return self._server.get_port()

    async def _handle(self, process: asyncssh.SSHServerProcess) -> None:
        self.commands.append(process.command)
        command = process.command
        if self.prefix is not None:
            command = f"{self.prefix}\n{command}"
        child = await asyncio.create_subprocess_shell(
            command,
            cwd=self._root,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"HOME": str(self._root), "PATH": os.environ["PATH"]},
            start_new_session=True,
        )
        self._sessions.add(child.pid)
        closed = asyncio.ensure_future(process.channel.wait_closed())
        closed.add_done_callback(lambda _: _close_pipes(child))
        stdin_task = asyncio.create_task(_pump_stdin(process.stdin, child))
        try:
            # Like OpenSSH, the session ends only when every process holding the
            # output pipes has exited; leftovers are NOT killed on a normal exit,
            # so a command that fails to kill its own process group stays visible.
            await asyncio.gather(
                _pump_output(child.stdout, process.stdout),
                _pump_output(child.stderr, process.stderr),
            )
            returncode = await child.wait()
        except BaseException:
            _kill_sessions({child.pid})
            raise
        finally:
            stdin_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stdin_task
        process.exit(returncode if returncode >= 0 else 128 - returncode)
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(closed, 5)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        # Background jobs outlive their call (as on a real workspace); end them here.
        _kill_sessions(self._sessions)
        shutil.rmtree(self._key_dir, ignore_errors=True)


def _close_pipes(child) -> None:
    """Close the command's stdin, stdout and stderr pipes, as sshd does when the
    channel closes."""
    transport = getattr(child, "_transport", None)
    if transport is None:
        return
    for fd in (0, 1, 2):
        with contextlib.suppress(Exception):
            pipe = transport.get_pipe_transport(fd)
            if pipe is not None:
                pipe.close()


def _kill_sessions(sessions: set[int]) -> None:
    """Teardown only: SIGKILL every process left in the given sessions."""
    try:
        pids = [int(entry) for entry in os.listdir("/proc") if entry.isdigit()]
    except OSError:
        pids = []
    for pid in pids:
        with contextlib.suppress(OSError):
            if os.getsid(pid) in sessions:
                os.kill(pid, signal.SIGKILL)
    for session in sessions:
        with contextlib.suppress(OSError):
            os.killpg(session, signal.SIGKILL)
