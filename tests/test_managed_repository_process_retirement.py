from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from orchestrator.services import managed_repository_process_retirement as subject


@pytest.mark.asyncio
@pytest.mark.parametrize("matching_pin", [True, False])
async def test_retirement_authenticates_host_before_sending_commands(
    monkeypatch, tmp_path, matching_pin
):
    asyncssh = pytest.importorskip("asyncssh")
    server_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = tmp_path / "client-key"
    client_key.write_bytes(
        asyncssh.generate_private_key("ssh-ed25519").export_private_key()
    )
    monkeypatch.setattr(subject, "resolve_ssh_key_path", lambda: str(client_key))
    commands = []

    class Server(asyncssh.SSHServer):
        def begin_auth(self, _username):
            return False

    def run(process):
        commands.append(process.command)
        process.exit(0)

    server = await asyncssh.create_server(
        Server, "127.0.0.1", 0, server_host_keys=[server_key], process_factory=run
    )
    try:
        pin = (
            server_key if matching_pin else asyncssh.generate_private_key("ssh-ed25519")
        )
        result = await subject.retire_managed_repository_processes(
            host="127.0.0.1",
            port=server.get_port(),
            host_key_fingerprint=pin.get_fingerprint("sha256"),
        )
        assert result is matching_pin
        assert len(commands) == (1 if matching_pin else 0)
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_whole_workspace_retirement_composes_valid_shell(monkeypatch):
    commands: list[str] = []

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def run(self, command, **_kwargs):
            commands.append(command)
            syntax = subprocess.run(
                ["bash", "-n"],
                input=command,
                text=True,
                capture_output=True,
                check=False,
            )
            return SimpleNamespace(exit_status=syntax.returncode)

        def close(self):
            return None

        async def wait_closed(self):
            return None

    class AsyncSSH:
        @staticmethod
        async def connect(*_args, **_kwargs):
            return Connection()

    monkeypatch.setattr(subject, "asyncssh", AsyncSSH())
    monkeypatch.setattr(subject, "resolve_ssh_key_path", lambda: "/test/key")

    assert await subject.retire_managed_repository_processes(
        host="192.0.2.1",
        port=30022,
        host_key_fingerprint="SHA256:" + "a" * 43,
    )
    assert len(commands) == 1
    assert "; ;" not in commands[0]
    assert " all " in commands[0]
    assert " zero " in commands[0]
