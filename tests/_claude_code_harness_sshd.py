"""In-process SSH server that runs commands for real in a local directory.

Stands in for the workspace sshd in the Claude Code harness tests: every exec
request runs through ``/bin/sh -c`` with the workspace root as working directory
and ``HOME``, so the harness's shell wrapper, ``timeout`` and file transfers are
exercised against real processes. Channels are binary (``encoding=None``).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import tempfile
from pathlib import Path

import asyncssh
from srw_claude_code_harness.bindings import WorkspaceAccess

_CHUNK = 65536


class _Server(asyncssh.SSHServer):
    def begin_auth(self, _username: str) -> bool:
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

    @classmethod
    async def start(cls, root: Path) -> LocalSshd:
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
        instance._server = server = await asyncssh.create_server(
            _Server,
            "127.0.0.1",
            0,
            server_host_keys=[host_key],
            process_factory=instance._handle,
            encoding=None,
        )
        port = server.get_port()
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

    async def _handle(self, process: asyncssh.SSHServerProcess) -> None:
        self.commands.append(process.command)
        child = await asyncio.create_subprocess_shell(
            process.command,
            cwd=self._root,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"HOME": str(self._root), "PATH": os.environ["PATH"]},
            start_new_session=True,
        )
        self._sessions.add(child.pid)
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

    async def close(self) -> None:
        self._server.close()
        await self._server.wait_closed()
        # Background jobs outlive their call (as on a real workspace); end them here.
        _kill_sessions(self._sessions)
        shutil.rmtree(self._key_dir, ignore_errors=True)


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
