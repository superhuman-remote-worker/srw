from __future__ import annotations

import json
import os
import shutil
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


_needs_workspace_tools = pytest.mark.skipif(
    not (os.path.exists("/usr/bin/python3") and shutil.which("git")),
    reason="the workspace programs run as /usr/bin/python3, with git",
)
_KEY = "-----BEGIN OPENSSH " + "PRIVATE KEY-----\nAAAA\n-----END\n"


def _block(ssh, name, host):
    """Exactly what a pre-agent clone appended to ~/.ssh/config."""
    return (
        f"\nHost {host}\n  IdentityFile {ssh}/{name}\n"
        "  StrictHostKeyChecking accept-new\n"
    )


def _git(home, *args):
    subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        env={"PATH": os.environ["PATH"], "HOME": str(home), "GIT_CONFIG_NOSYSTEM": "1"},
    )


def _remotes(home, checkout):
    listed = subprocess.run(
        [
            "git",
            "config",
            "--file",
            str(home / "workspace" / "repos" / checkout / ".git" / "config"),
            "--get-regexp",
            r"^remote\.",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], "HOME": str(home), "GIT_CONFIG_NOSYSTEM": "1"},
    )
    return listed.stdout.splitlines()


@pytest.fixture
def workspace_home(monkeypatch, tmp_path):
    """A throwaway workspace home holding SRW's material and the user's."""

    from shared.runtime.core.credential_env import INSTALL_CREDENTIAL_FILES

    # The process classifier is the terminal teardown's own, covered above;
    # these exercise what the scrub removes from the home.
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
    for relative in (
        ".srw-credentials/leases/0f0f",
        ".srw-credentials/git/config",
        ".ssh/srw-managed/config.d/a.conf",
        ".ssh/id_ed25519",
        ".gitconfig",
        ".config/tool/settings",
        "workspace/notes.md",
    ):
        (home / relative).parent.mkdir(parents=True, exist_ok=True)
        (home / relative).write_text("x")
    ssh = home / ".ssh"
    # A pre-agent clone's key, named by its exact block; and the user's own
    # key that happens to share the prefix.
    (ssh / "repo_legacy").write_text(_KEY)
    (ssh / "repo_deploy").write_text(_KEY)
    (ssh / "config").write_text(
        "Host mine\n  User me\n" + _block(ssh, "repo_legacy", "gitea.example")
    )
    for name in ("app", "tool"):
        _git(home, "init", "-q", str(home / "workspace" / "repos" / name))
    app = ["-C", str(home / "workspace" / "repos" / "app")]
    _git(
        home,
        *app,
        "remote",
        "add",
        "origin",
        "https://oauth2:tok1@gitea.example/o/app.git",
    )
    _git(
        home,
        *app,
        "remote",
        "set-url",
        "--push",
        "origin",
        "https://oauth2:tok1@gitea.example/o/app.git",
    )
    _git(
        home,
        *app,
        "remote",
        "add",
        "fork",
        "https://alice:pw@example.com/alice/app.git",
    )
    tool = ["-C", str(home / "workspace" / "repos" / "tool")]
    _git(
        home,
        *tool,
        "remote",
        "add",
        "origin",
        "https://x-access-token:ghs_tok2@github.com/o/tool.git",
    )
    _git(home, *tool, "remote", "add", "upstream", "git@github.com:o/tool.git")
    return home


def _scrub(home):
    return subprocess.run(
        ["bash", "-c", subject.workspace_credential_scrub_command(str(home))],
        text=True,
        capture_output=True,
        check=False,
        env={"PATH": os.environ["PATH"], "HOME": str(home)},
    )


def _files(home):
    """Every file in the home but the checkouts' own."""
    found = set()
    for root, _dirs, files in os.walk(home):
        for name in files:
            relative = os.path.relpath(os.path.join(root, name), home)
            if not relative.startswith("workspace/repos/"):
                found.add(relative)
    return found


@_needs_workspace_tools
def test_the_credential_scrub_removes_srw_material_and_keeps_the_users(
    workspace_home,
):
    home = workspace_home
    ssh = home / ".ssh"

    scrubbed = _scrub(home)

    assert scrubbed.returncode == 0, scrubbed.stderr
    assert _files(home) == {
        ".ssh/id_ed25519",
        ".ssh/config",
        ".ssh/repo_deploy",
        ".gitconfig",
        ".config/tool/settings",
        "workspace/notes.md",
    }
    # The links it placed are gone with the store; nothing dangles.
    assert not os.path.lexists(home / ".kube" / "config")
    assert not os.path.lexists(home / ".config" / "tool" / "token")
    # Only the exact pre-agent block went with its key.
    assert (ssh / "config").read_text() == "Host mine\n  User me\n"
    # SRW's tokens left the remotes; the user's own credentials stayed.
    assert sorted(_remotes(home, "app")) == sorted(
        [
            "remote.origin.url https://gitea.example/o/app.git",
            "remote.origin.fetch +refs/heads/*:refs/remotes/origin/*",
            "remote.origin.pushurl https://gitea.example/o/app.git",
            "remote.fork.url https://alice:pw@example.com/alice/app.git",
            "remote.fork.fetch +refs/heads/*:refs/remotes/fork/*",
        ]
    )
    assert "remote.origin.url https://github.com/o/tool.git" in _remotes(home, "tool")
    assert "remote.upstream.url git@github.com:o/tool.git" in _remotes(home, "tool")


@_needs_workspace_tools
def test_a_symlinked_credential_root_goes_as_a_link(workspace_home, tmp_path):
    home = workspace_home
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "files-9999").mkdir(parents=True)
    (elsewhere / "files-9999" / "keep").write_text("x")
    shutil.rmtree(home / ".srw-credentials")
    (home / ".srw-credentials").symlink_to(elsewhere)

    scrubbed = _scrub(home)

    assert scrubbed.returncode == 0, scrubbed.stderr
    assert not os.path.lexists(home / ".srw-credentials")
    assert (elsewhere / "files-9999" / "keep").read_text() == "x"


@_needs_workspace_tools
@pytest.mark.skipif(os.geteuid() == 0, reason="root removes a read-only directory")
def test_anything_left_behind_fails_the_scrub_after_every_step(workspace_home):
    home = workspace_home
    stuck = home / ".srw-credentials" / "stuck"
    stuck.mkdir()
    (stuck / "token").write_text("x")
    stuck.chmod(0o500)
    try:
        scrubbed = _scrub(home)

        assert scrubbed.returncode != 0
        # The other steps still ran.
        assert not (home / ".ssh" / "repo_legacy").exists()
        assert not (home / ".ssh" / "srw-managed").exists()
        assert "remote.origin.url https://gitea.example/o/app.git" in _remotes(
            home, "app"
        )
    finally:
        stuck.chmod(0o700)


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
