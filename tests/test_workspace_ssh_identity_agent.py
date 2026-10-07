"""Real-process acceptance for connector SSH identities (connector drivers C1).

External SSH repositories and ``ssh_key`` connectors load their key into a
dedicated ``ssh-agent`` in the managed namespace, never onto the workspace
disk. These tests run the generated commands with real ``bash``, ``ssh-agent``,
``ssh-add`` and ``ssh -G`` against a short temporary home.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from orchestrator.services.managed_repository_authority import _deploy_keypair
from shared.runtime.core.managed_repository import (
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
)
from shared.runtime.core.workspace_ssh_identity import (
    IDENTITY_READY,
    materialize_workspace_ssh_identities,
    prune_workspace_ssh_identities,
    retire_workspace_ssh_identities,
    workspace_ssh_identity_alias,
    workspace_ssh_identity_socket,
)


def _run(command: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["bash", "-c", command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=20,
    )


class _LocalShellBackend:
    supports_shell = True

    def __init__(self, home: Path) -> None:
        self._remote_root = str(home / "workspace")
        self.commands: list[tuple[str, bytes]] = []

    def resolve_home_path(self, relative_path: str) -> str:
        return str(Path(self._remote_root).parent / relative_path)

    def execute_with_secret_stdin(
        self, command: str, secret: bytes | bytearray | str, *, timeout: int = 30
    ) -> bool:
        payload = secret.encode() if isinstance(secret, str) else bytes(secret)
        self.commands.append((command, payload))
        return (
            subprocess.run(
                ["bash", "-c", command],
                input=payload,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=timeout,
            ).returncode
            == 0
        )


@pytest.fixture
def home() -> Path:
    # AF_UNIX socket paths are short; keep the home representative.
    with tempfile.TemporaryDirectory(prefix="srw-c1-", dir="/tmp") as value:
        path = Path(value)
        try:
            yield path
        finally:
            _run(managed_repository_agent_retirement_command(home_path=str(path)))


def _host_key() -> str:
    public = Ed25519PrivateKey.generate().public_key()
    return public.public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH).decode()


def _payload(
    *,
    kind: str = "repository",
    host: str | None = "github.com",
    port: int | None = 22,
    user: str | None = "git",
    known_hosts: list[str] | None = None,
    extra_hosts: list[str] | None = None,
    authority_id: str | None = None,
    key: tuple[str, str, str] | None = None,
) -> dict:
    authority_id = authority_id or str(uuid4())
    private_key, _public_key, fingerprint = key or _deploy_keypair()
    return {
        "version": 1,
        "authority_id": authority_id,
        "generation": 1,
        "kind": kind,
        "alias": workspace_ssh_identity_alias(authority_id),
        "ssh_host": host,
        "ssh_port": port,
        "ssh_user": user,
        "extra_hosts": extra_hosts or [],
        "known_hosts": known_hosts or [],
        "strict_host_key_checking": bool(known_hosts),
        "private_key": private_key,
        "public_key_fingerprint": fingerprint,
    }


def _agent_fingerprints(socket_path: str) -> list[str]:
    listed = subprocess.run(
        ["ssh-add", "-l"],
        env={**os.environ, "SSH_AUTH_SOCK": socket_path},
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if listed.returncode in (1, 2):  # no identities, or no agent at all
        return []
    assert listed.returncode == 0
    return [line.split()[1] for line in listed.stdout.decode().splitlines() if line]


def _ssh_options(home: Path, target: str) -> dict[str, str]:
    evaluated = subprocess.run(
        ["ssh", "-G", "-F", str(home / ".ssh" / "config"), target],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
    ).stdout.decode()
    return dict(line.split(" ", 1) for line in evaluated.splitlines() if " " in line)


def _no_private_key_on_disk(home: Path) -> None:
    for path in home.rglob("*"):
        if path.is_file() and not path.is_symlink():
            assert b"PRIVATE KEY" not in path.read_bytes(), path


def test_repository_and_ssh_key_identities_load_without_touching_disk(
    home: Path,
) -> None:
    pin = _host_key()
    repository = _payload(
        host="gitea.example.com", port=2222, user="git", known_hosts=[pin]
    )
    ssh_key = _payload(
        kind="ssh_key",
        host="bastion.example.com",
        port=None,
        user="deploy",
        extra_hosts=["bastion.example.com"],
    )
    fingerprints = {
        item["authority_id"]: item["public_key_fingerprint"]
        for item in (repository, ssh_key)
    }
    private_keys = [repository["private_key"], ssh_key["private_key"]]
    backend = _LocalShellBackend(home)

    status = materialize_workspace_ssh_identities([repository, ssh_key], backend)

    assert status == {authority: IDENTITY_READY for authority in fingerprints}
    assert "private_key" not in repository and "private_key" not in ssh_key
    for command, secret in backend.commands:
        assert not any(key in command for key in private_keys)
        assert not secret or secret.decode() in private_keys
        if secret:
            # A connector's static key expires on its own if never retired.
            assert "ssh-add -t 604800 -" in command
    _no_private_key_on_disk(home)
    for authority, fingerprint in fingerprints.items():
        socket_path = workspace_ssh_identity_socket(str(home), authority)
        assert _agent_fingerprints(socket_path) == [fingerprint]
        config = (
            home
            / ".ssh"
            / "srw-managed"
            / "config.d"
            / f"{authority.replace('-', '')}.conf"
        )
        assert config.read_text().splitlines()[:2] == [
            f"Host {workspace_ssh_identity_alias(authority)}",
            f"  IdentityAgent {socket_path}",
        ]
        assert oct(config.stat().st_mode & 0o777) == "0o600"

    # The repository alias reaches its real endpoint with a strict pin.
    options = _ssh_options(home, repository["alias"])
    slug = repository["authority_id"].replace("-", "")
    assert options["hostname"] == "gitea.example.com"
    assert options["port"] == "2222"
    assert options["user"] == "git"
    assert options["stricthostkeychecking"] == "true"
    assert options["hostkeyalias"] == repository["alias"]
    known_hosts = home / ".ssh" / "srw-managed" / "known_hosts.d" / slug
    assert options["userknownhostsfile"] == str(known_hosts)
    assert known_hosts.read_text() == f"{repository['alias']} {pin}\n"

    # A declared host makes plain ``ssh <host>`` use the identity's agent.
    plain = _ssh_options(home, "bastion.example.com")
    assert plain["identityagent"] == workspace_ssh_identity_socket(
        str(home), ssh_key["authority_id"]
    )
    assert plain["user"] == "deploy"
    assert plain["stricthostkeychecking"] == "accept-new"


def test_one_broken_identity_degrades_only_its_connector(home: Path) -> None:
    good = _payload()
    injected = _payload(host="github.com\n  ProxyCommand sh -c id")
    not_a_key = _payload()
    not_a_key["private_key"] = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----\n"
    )
    backend = _LocalShellBackend(home)

    status = materialize_workspace_ssh_identities([good, injected, not_a_key], backend)

    assert status[good["authority_id"]] == IDENTITY_READY
    assert status[injected["authority_id"]] == "workspace_ssh_identity_invalid"
    assert status[not_a_key["authority_id"]] == "workspace_ssh_identity_load_failed"
    assert all("private_key" not in item for item in (good, injected, not_a_key))
    assert _agent_fingerprints(
        workspace_ssh_identity_socket(str(home), good["authority_id"])
    ) == [good["public_key_fingerprint"]]
    # The failed load rolled back its own spawn and left no receipt.
    slug = not_a_key["authority_id"].replace("-", "")
    assert not (home / ".ssh" / "srw-managed" / "agents" / f"{slug}.state").exists()
    assert not any("ProxyCommand" in command for command, _ in backend.commands)


def test_no_shell_backend_refuses_every_identity_and_drops_keys() -> None:
    class _NoShell:
        supports_shell = False

    payload = _payload()
    status = materialize_workspace_ssh_identities([payload], _NoShell())
    assert status == {
        payload["authority_id"]: "workspace_ssh_identity_requires_workspace"
    }
    assert "private_key" not in payload


def test_rotated_key_replaces_the_resident_at_a_constant_generation(
    home: Path,
) -> None:
    authority_id = str(uuid4())
    backend = _LocalShellBackend(home)
    first = _payload(authority_id=authority_id)
    assert materialize_workspace_ssh_identities([first], backend) == {
        authority_id: IDENTITY_READY
    }
    socket_path = workspace_ssh_identity_socket(str(home), authority_id)
    state = (
        home
        / ".ssh"
        / "srw-managed"
        / "agents"
        / f"{authority_id.replace('-', '')}.state"
    )
    first_pid = dict(line.split("=", 1) for line in state.read_text().splitlines())[
        "pid"
    ]

    # No credential_revision exists: a rotated key arrives at generation 1
    # too, fails the resident's fingerprint proof, and replaces it.
    rotated = _payload(authority_id=authority_id)
    assert materialize_workspace_ssh_identities([rotated], backend) == {
        authority_id: IDENTITY_READY
    }
    assert _agent_fingerprints(socket_path) == [rotated["public_key_fingerprint"]]
    rotated_pid = dict(line.split("=", 1) for line in state.read_text().splitlines())[
        "pid"
    ]
    assert rotated_pid != first_pid


def test_proven_resident_is_reused(home: Path) -> None:
    authority_id = str(uuid4())
    key = _deploy_keypair()
    backend = _LocalShellBackend(home)
    for _ in range(2):
        assert materialize_workspace_ssh_identities(
            [_payload(authority_id=authority_id, key=key)], backend
        ) == {authority_id: IDENTITY_READY}
    state = (
        home
        / ".ssh"
        / "srw-managed"
        / "agents"
        / f"{authority_id.replace('-', '')}.state"
    )
    assert state.exists()
    socket_path = workspace_ssh_identity_socket(str(home), authority_id)
    assert _agent_fingerprints(socket_path) == [key[2]]


def test_two_deploy_keys_on_one_host_get_their_own_alias_and_agent(
    home: Path,
) -> None:
    first = _payload(host="github.com")
    second = _payload(host="github.com")
    backend = _LocalShellBackend(home)
    assert set(
        materialize_workspace_ssh_identities([first, second], backend).values()
    ) == {IDENTITY_READY}
    for item in (first, second):
        socket_path = workspace_ssh_identity_socket(str(home), item["authority_id"])
        options = _ssh_options(home, item["alias"])
        assert options["hostname"] == "github.com"
        assert options["identityagent"] == socket_path
        assert _agent_fingerprints(socket_path) == [item["public_key_fingerprint"]]


def test_detach_retires_exactly_one_identity(home: Path) -> None:
    kept = _payload()
    detached = _payload(known_hosts=[_host_key()])
    backend = _LocalShellBackend(home)
    materialize_workspace_ssh_identities([kept, detached], backend)
    root = home / ".ssh" / "srw-managed"
    slug = detached["authority_id"].replace("-", "")

    assert retire_workspace_ssh_identities([detached["authority_id"]], backend)

    assert not (root / "sockets" / f"{slug}.sock").exists()
    assert not (root / "agents" / f"{slug}.state").exists()
    assert not (root / "config.d" / f"{slug}.conf").exists()
    assert not (root / "known_hosts.d" / slug).exists()
    assert _agent_fingerprints(
        workspace_ssh_identity_socket(str(home), kept["authority_id"])
    ) == [kept["public_key_fingerprint"]]
    # Terminal teardown still owns the rest of the namespace.
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(home))).returncode
        == 85
    )
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(home))
        ).returncode
        == 0
    )
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(home))).returncode == 0
    )


def test_crash_before_the_receipt_still_retires(home: Path) -> None:
    """The legacy adoption seam: config present, live agent, no receipt."""

    payload = _payload(kind="ssh_key", host=None, port=None, user=None)
    backend = _LocalShellBackend(home)
    assert materialize_workspace_ssh_identities([payload], backend) == {
        payload["authority_id"]: IDENTITY_READY
    }
    slug = payload["authority_id"].replace("-", "")
    (home / ".ssh" / "srw-managed" / "agents" / f"{slug}.state").unlink()

    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(home))
        ).returncode
        == 0
    )
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(home))).returncode == 0
    )


def test_runtime_authority_is_recorded_in_the_receipt(home: Path) -> None:
    backend = _LocalShellBackend(home)
    workspace_generation, runtime_incarnation = str(uuid4()), str(uuid4())
    backend.managed_repository_runtime_authority = (
        workspace_generation,
        runtime_incarnation,
    )
    payload = _payload()
    assert materialize_workspace_ssh_identities([payload], backend) == {
        payload["authority_id"]: IDENTITY_READY
    }
    state = (
        home
        / ".ssh"
        / "srw-managed"
        / "agents"
        / f"{payload['authority_id'].replace('-', '')}.state"
    ).read_text()
    assert f"workspace_generation={workspace_generation}" in state
    assert f"runtime_incarnation={runtime_incarnation}" in state
    assert retire_workspace_ssh_identities([payload["authority_id"]], backend)


def test_session_attach_prunes_undelivered_identities_but_never_managed_ones(
    home: Path,
) -> None:
    """A stateless session applies a detach at its next attach."""

    from shared.runtime.core.managed_repository import (
        managed_repository_agent_launch_command,
    )

    kept = _payload()
    detached = _payload(kind="ssh_key", host=None, port=None, user=None)
    backend = _LocalShellBackend(home)
    materialize_workspace_ssh_identities([kept, detached], backend)
    # A managed-repository resident in the same namespace (no known_hosts.d).
    managed_id = str(uuid4())
    managed_key = _deploy_keypair()[0]
    assert (
        subprocess.run(
            [
                "bash",
                "-c",
                managed_repository_agent_launch_command(
                    home_path=str(home), authority_id=managed_id, generation=1
                ),
            ],
            input=managed_key.encode(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=20,
        ).returncode
        == 0
    )

    assert prune_workspace_ssh_identities([kept["authority_id"]], backend)

    root = home / ".ssh" / "srw-managed"
    slug = detached["authority_id"].replace("-", "")
    assert not (root / "config.d" / f"{slug}.conf").exists()
    assert not (root / "known_hosts.d" / slug).exists()
    assert (
        _agent_fingerprints(
            workspace_ssh_identity_socket(str(home), detached["authority_id"])
        )
        == []
    )
    assert _agent_fingerprints(
        workspace_ssh_identity_socket(str(home), kept["authority_id"])
    ) == [kept["public_key_fingerprint"]]
    assert (
        len(_agent_fingerprints(workspace_ssh_identity_socket(str(home), managed_id)))
        == 1
    )
    # Idempotent, and an empty keep-list prunes the rest of the connectors.
    assert prune_workspace_ssh_identities([kept["authority_id"]], backend)
    assert prune_workspace_ssh_identities([], backend)
    assert (
        _agent_fingerprints(
            workspace_ssh_identity_socket(str(home), kept["authority_id"])
        )
        == []
    )
    assert (
        len(_agent_fingerprints(workspace_ssh_identity_socket(str(home), managed_id)))
        == 1
    )


def test_prune_keeps_a_delivered_identity_whose_reload_failed(home: Path) -> None:
    """The session keeps every delivered authority, loaded or not."""

    first = _payload()
    authority = first["authority_id"]
    again = dict(first, private_key="not a key")
    backend = _LocalShellBackend(home)
    assert materialize_workspace_ssh_identities([first], backend) == {
        authority: "ready"
    }

    status = materialize_workspace_ssh_identities([again, {"version": 1}], backend)
    assert status[authority] != "ready"
    assert len(status) == 2  # plus a placeholder for the authority-less payload

    assert prune_workspace_ssh_identities(list(status), backend)
    slug = authority.replace("-", "")
    assert (home / ".ssh" / "srw-managed" / "known_hosts.d" / slug).exists()
    assert _agent_fingerprints(workspace_ssh_identity_socket(str(home), authority)) == [
        first["public_key_fingerprint"]
    ]


def test_prune_on_a_fresh_home_is_a_no_op(home: Path) -> None:
    assert prune_workspace_ssh_identities([], _LocalShellBackend(home))


@pytest.mark.parametrize(
    "pattern",
    ["*", "gitea.*", "git?a.example.com", "!gitea.example.com", "a,b", "-oops", ""],
)
def test_the_renderer_refuses_an_extra_host_that_is_a_pattern(pattern: str) -> None:
    """A second line of defence behind normalize_ssh_host, where it is written."""

    from shared.runtime.core.managed_repository import (
        ManagedRepositoryMaterializationError,
        render_ssh_identity_config,
    )

    with pytest.raises(ManagedRepositoryMaterializationError):
        render_ssh_identity_config(
            alias="srw-repo-" + "a" * 32,
            socket_path="/home/agent-host/.ssh/srw-managed/sockets/a.sock",
            known_hosts_path="/home/agent-host/.ssh/srw-managed/known_hosts.d/a",
            host="gitea.example.com",
            extra_hosts=[pattern],
        )


@pytest.mark.parametrize(
    "host", ["gitea.example.com", "Bastion-1", "10.0.0.7", "2001:db8::1"]
)
def test_the_renderer_accepts_a_plain_extra_host(host: str) -> None:
    from shared.runtime.core.managed_repository import render_ssh_identity_config

    config = render_ssh_identity_config(
        alias="srw-repo-" + "a" * 32,
        socket_path="/home/agent-host/.ssh/srw-managed/sockets/a.sock",
        known_hosts_path="/home/agent-host/.ssh/srw-managed/known_hosts.d/a",
        host=host,
        extra_hosts=[host],
    )
    assert f"\nHost {host}\n" in config


def test_a_declared_host_can_never_shadow_an_identity_alias(home: Path) -> None:
    """Review blocker: an ssh_key host spelled like an alias took it over.

    config.d is read in slug order and the first matching ``Host`` block
    wins, so an attacker connector whose slug sorts first, declaring the
    victim's alias as its host, would replace the victim's IdentityAgent,
    HostKeyAlias, UserKnownHostsFile and StrictHostKeyChecking.
    """

    from shared.runtime.core.managed_repository import render_ssh_identity_config

    victim_key = _deploy_keypair()
    victim = _payload(host="gitea.example.com", port=2222, key=victim_key)
    victim_socket = workspace_ssh_identity_socket(str(home), victim["authority_id"])
    attacker_id = "00000000-0000-4000-8000-000000000000"
    attacker_socket = workspace_ssh_identity_socket(str(home), attacker_id)
    root = home / ".ssh" / "srw-managed"

    # The precondition is real: a block for the alias in a file read first
    # would shadow the victim (what the renderer used to emit).
    backend = _LocalShellBackend(home)
    assert materialize_workspace_ssh_identities([victim], backend) == {
        victim["authority_id"]: IDENTITY_READY
    }
    attacker_file = root / "config.d" / f"{attacker_id.replace('-', '')}.conf"
    attacker_file.write_text(
        f"Host {victim['alias']}\n  IdentityAgent {attacker_socket}\n"
        "  StrictHostKeyChecking no\n"
    )
    assert _ssh_options(home, victim["alias"])["identityagent"] == attacker_socket
    attacker_file.unlink()

    # Every layer now refuses the alias shape as a declared host.
    for spelling in (victim["alias"], victim["alias"].upper()):
        with pytest.raises(Exception):
            render_ssh_identity_config(
                alias=f"srw-repo-{attacker_id.replace('-', '')}",
                socket_path=attacker_socket,
                known_hosts_path=str(root / "known_hosts.d" / "x"),
                host=None,
                extra_hosts=[spelling],
            )
    attacker = _payload(
        kind="ssh_key",
        host=victim["alias"],
        port=None,
        user=None,
        extra_hosts=[victim["alias"]],
        authority_id=attacker_id,
    )
    again = _payload(
        host="gitea.example.com",
        port=2222,
        key=victim_key,
        authority_id=victim["authority_id"],
    )
    status = materialize_workspace_ssh_identities([attacker, again], backend)
    assert status[attacker_id] == "workspace_ssh_identity_invalid"
    assert status[victim["authority_id"]] == IDENTITY_READY
    assert not attacker_file.exists()
    options = _ssh_options(home, victim["alias"])
    assert options["identityagent"] == victim_socket
    assert options["hostkeyalias"] == victim["alias"]
    assert options["hostname"] == "gitea.example.com"
