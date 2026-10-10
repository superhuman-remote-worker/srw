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


# The credential scrub of a workspace a refused resume keeps (connector
# drivers decision 34).


def test_the_credential_scrub_is_valid_shell():
    command = subject.workspace_credential_scrub_command()

    syntax = subprocess.run(
        ["bash", "-n"], input=command, text=True, capture_output=True, check=False
    )
    assert syntax.returncode == 0, syntax.stderr
    assert "; ;" not in command
    # The managed ssh-agents are retired as a terminal teardown retires them.
    assert " all " in command


@pytest.mark.skipif(
    not __import__("os").path.exists("/usr/bin/python3"),
    reason="the workspace programs run as /usr/bin/python3",
)
def test_the_credential_scrub_removes_srw_material_and_keeps_the_users(
    monkeypatch, tmp_path
):
    import json
    import os

    from shared.runtime.core.credential_env import INSTALL_CREDENTIAL_FILES

    # The process classifier is the terminal teardown's own, covered above;
    # this exercises what the scrub removes from the home.
    monkeypatch.setattr(subject, "_whole_workspace_retirement", lambda _home: "true")
    home = tmp_path / "home"
    home.mkdir()
    synced = subprocess.run(
        [
            "/usr/bin/python3",
            "-I",
            "-c",
            INSTALL_CREDENTIAL_FILES,
            str(home),
            "files-0123",
            "sync",
        ],
        input=json.dumps(
            {
                "files": [
                    {
                        "name": "kubeconfig",
                        "content": "secret",
                        "mode": 0o600,
                        "link": ".kube/config",
                    },
                    {
                        "name": "token",
                        "content": "secret",
                        "mode": 0o600,
                        "link": ".config/tool/token",
                    },
                ],
                "env": [{"name": "KUBECONFIG", "files": ["kubeconfig"]}],
            }
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert synced.returncode == 0, synced.stderr
    srw = [
        ".srw-credentials/leases/0f0f",
        ".srw-credentials/git/config",
        ".ssh/srw-managed/config.d/a.conf",
        ".ssh/repo_legacy",
    ]
    user = [
        ".ssh/id_ed25519",
        ".ssh/config",
        ".gitconfig",
        ".config/tool/settings",
        "workspace/notes.md",
    ]
    for relative in srw + user:
        (home / relative).parent.mkdir(parents=True, exist_ok=True)
        (home / relative).write_text("x")

    scrubbed = subprocess.run(
        ["bash", "-c", subject.workspace_credential_scrub_command(str(home))],
        text=True,
        capture_output=True,
        check=False,
    )

    assert scrubbed.returncode == 0, scrubbed.stderr
    remaining = {
        os.path.relpath(os.path.join(root, name), home)
        for root, _dirs, files in os.walk(home)
        for name in files
    }
    assert remaining == set(user)
    # The links it placed are gone with the store; nothing dangles.
    assert not os.path.lexists(home / ".kube" / "config")
    assert not os.path.lexists(home / ".config" / "tool" / "token")


@pytest.mark.asyncio
@pytest.mark.parametrize("fingerprint", ["", "MD5:aa", None])
async def test_the_credential_scrub_needs_an_exact_pin(monkeypatch, fingerprint):
    class AsyncSSH:
        @staticmethod
        async def connect(*_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("connected without a pinned host key")

    monkeypatch.setattr(subject, "asyncssh", AsyncSSH())
    monkeypatch.setattr(subject, "resolve_ssh_key_path", lambda: "/test/key")

    assert (
        await subject.scrub_workspace_credentials(
            host="192.0.2.1", port=30022, host_key_fingerprint=fingerprint
        )
        is False
    )
