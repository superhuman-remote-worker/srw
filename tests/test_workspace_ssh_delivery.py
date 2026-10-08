"""C1 delivery: connector SSH keys leave ``datasources`` for a hidden field.

``datasources`` becomes job metadata and graph state, so an SSH key there is
checkpointed. The key now travels once, in ``workspace_ssh_identities``,
carried and stripped wherever ``managed_repository_credentials`` is; the
``datasources`` entry keeps only the non-secret alias it is reached through.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from orchestrator.services.agent_datasource_payload import (
    DatasourcePayloadDependencies,
    build_datasources_payload,
)
from orchestrator.services.agent_datasource_payload import (
    build_workspace_ssh_identities as _build_identities,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.workspace_ssh_connector import (
    WORKSPACE_SSH_KNOWN_HOSTS_ENV,
    workspace_ssh_authority_id,
)
from shared.runtime.utils.ssh_key import generate_ed25519_keypair, ssh_public_identity

_REPO_ID = "00000000-0000-4000-8000-0000000000a1"
_KEY_ID = "00000000-0000-4000-8000-0000000000a2"
_TOKEN_ID = "00000000-0000-4000-8000-0000000000a3"


def _deps() -> DatasourcePayloadDependencies:
    return DatasourcePayloadDependencies(
        logger=logging.getLogger("test"),
        mcp_datasources_enabled=lambda: True,
        mcp_stdio_enabled=lambda: True,
        connector_drivers=builtin_connector_drivers(),
    )


def build_workspace_ssh_identities(rows):
    """The delivery field, built through the connector drivers."""
    return _build_identities(rows, dependencies=_deps())


def _host_key() -> str:
    public = Ed25519PrivateKey.generate().public_key()
    return public.public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH).decode()


def _repository(**over) -> dict:
    row = {
        "id": _REPO_ID,
        "type": "repository",
        "name": "Widget",
        "connection_url": "git@github.com:acme/widget.git",
        "credentials": {
            "auth_method": "ssh",
            "ssh_key": generate_ed25519_keypair().private_key,
        },
        "config": {"forge": "github"},
        "project_read_only": False,
    }
    row.update(over)
    return row


def _ssh_key(**over) -> dict:
    row = {
        "id": _KEY_ID,
        "type": "ssh_key",
        "name": "Bastion",
        "connection_url": None,
        "credentials": {
            "files": [
                {
                    "name": "bastion",
                    "contents": generate_ed25519_keypair().private_key,
                    "target_path": "/home/srw/.ssh/bastion",
                    "mode": "0600",
                }
            ]
        },
        "config": {"host": "bastion.example.com", "user": "deploy"},
        "project_read_only": False,
    }
    row.update(over)
    return row


def _token_repository() -> dict:
    return {
        "id": _TOKEN_ID,
        "type": "repository",
        "name": "Docs",
        "connection_url": "https://github.com/acme/docs.git",
        "credentials": {"auth_method": "token", "token": "ghp_token"},
        "config": {"forge": "github"},
        "project_read_only": False,
    }


class TestDatasourcesPayload:
    def test_no_private_key_rides_datasources(self):
        rows = [_repository(), _ssh_key(), _token_repository()]
        payload = build_datasources_payload(rows, dependencies=_deps())

        assert "PRIVATE KEY" not in json.dumps(payload)
        repository, ssh_key, token = payload
        assert repository["credentials"] == {"auth_method": "ssh"}
        alias = f"srw-repo-{UUID(workspace_ssh_authority_id(_REPO_ID)).hex}"
        assert repository["ssh_identity"]["alias"] == alias
        assert (
            # scp form, relative path: the clone keeps it relative.
            repository["ssh_identity"]["clone_url"] == f"{alias}:acme/widget.git"
        )
        assert repository["ssh_identity"]["host"] == "github.com"
        assert ssh_key["credentials"]["files"] == [
            {
                "name": "bastion",
                "target_path": "/home/srw/.ssh/bastion",
                "mode": "0600",
            }
        ]
        assert ssh_key["ssh_identity"]["host"] == "bastion.example.com"
        assert ssh_key["ssh_identity"]["user"] == "deploy"
        assert ssh_key["ssh_identity"]["public_key"].startswith("ssh-ed25519 ")
        # Token repositories are C3's; C1 leaves them exactly as they were.
        assert token["credentials"] == {"auth_method": "token", "token": "ghp_token"}
        assert "ssh_identity" not in token

    def test_a_token_repository_drops_a_stray_key_and_delivers_no_identity(self):
        """Token auth with an SSH URL and a leftover ``ssh_key``: the API
        accepts the row, but its clone never reads a key, so the key neither
        rides ``datasources`` nor reaches the workspace's ssh-agent."""
        row = {
            **_token_repository(),
            "connection_url": "ssh://git@git.example.test:2222/acme/docs.git",
            "config": {"forge": "gitea"},
            "credentials": {
                "auth_method": "token",
                "token": "ghp_token",
                "ssh_key": generate_ed25519_keypair().private_key,
            },
        }
        assert build_workspace_ssh_identities([row]) is None
        (entry,) = build_datasources_payload([row], dependencies=_deps())
        assert entry["credentials"] == {"auth_method": "token", "token": "ghp_token"}
        assert "ssh_identity" not in entry
        assert "PRIVATE KEY" not in json.dumps(entry)

    def test_a_pre_c1_row_degrades_to_an_unavailable_connector(self):
        legacy = _repository(
            credentials={"auth_method": "ssh", "ssh_key": "not a parseable key"}
        )
        (entry,) = build_datasources_payload([legacy], dependencies=_deps())
        assert entry["credentials"] == {"auth_method": "ssh"}
        assert "unavailable" in entry["ssh_identity"]
        assert build_workspace_ssh_identities([legacy]) is None


class TestWorkspaceSshIdentities:
    def test_identity_payload_shape(self, monkeypatch):
        monkeypatch.delenv(WORKSPACE_SSH_KNOWN_HOSTS_ENV, raising=False)
        repository = _repository()
        identities = build_workspace_ssh_identities(
            [repository, _ssh_key(), _token_repository()]
        )

        assert [item["kind"] for item in identities] == ["repository", "ssh_key"]
        repo_identity, key_identity = identities
        assert repo_identity["authority_id"] == workspace_ssh_authority_id(_REPO_ID)
        assert repo_identity["generation"] == 1
        assert repo_identity["ssh_host"] == "github.com"
        assert repo_identity["ssh_port"] == 22
        assert repo_identity["ssh_user"] == "git"
        assert repo_identity["extra_hosts"] == []
        assert repo_identity["strict_host_key_checking"] is False
        assert repo_identity["private_key"] == repository["credentials"]["ssh_key"]
        assert (
            repo_identity["public_key_fingerprint"]
            == ssh_public_identity(repository["credentials"]["ssh_key"])[1]
        )
        assert key_identity["extra_hosts"] == ["bastion.example.com"]
        assert key_identity["ssh_user"] == "deploy"

    def test_authority_is_stable_per_connector(self):
        assert workspace_ssh_authority_id(_REPO_ID) == workspace_ssh_authority_id(
            _REPO_ID
        )
        assert workspace_ssh_authority_id(_REPO_ID) != workspace_ssh_authority_id(
            _KEY_ID
        )

    def test_two_deploy_keys_on_one_host_are_two_identities(self):
        first = _repository()
        second = _repository(
            id="00000000-0000-4000-8000-0000000000b1",
            connection_url="git@github.com:acme/other.git",
        )
        identities = build_workspace_ssh_identities([first, second])
        assert len({item["alias"] for item in identities}) == 2
        assert {item["ssh_host"] for item in identities} == {"github.com"}

    def test_connector_pin_wins_and_is_strict(self, monkeypatch):
        monkeypatch.setenv(WORKSPACE_SSH_KNOWN_HOSTS_ENV, f"github.com {_host_key()}")
        pin = _host_key()
        (identity,) = build_workspace_ssh_identities(
            [_repository(config={"forge": "github", "known_hosts": pin})]
        )
        assert identity["known_hosts"] == [pin]
        assert identity["strict_host_key_checking"] is True

    def test_deployment_default_pins_only_its_own_hosts(self, monkeypatch):
        github, gitlab = _host_key(), _host_key()
        monkeypatch.setenv(
            WORKSPACE_SSH_KNOWN_HOSTS_ENV,
            f"github.com {github}\ngitlab.com {gitlab}\n{_host_key()}\n",
        )
        on_github, on_bastion = build_workspace_ssh_identities(
            [_repository(), _ssh_key()]
        )
        assert on_github["known_hosts"] == [github]
        assert on_github["strict_host_key_checking"] is True
        assert on_bastion["known_hosts"] == []
        assert on_bastion["strict_host_key_checking"] is False

    def test_one_bad_default_line_degrades_only_its_host(self, monkeypatch, caplog):
        """Review: one unusable line used to disable every SSH connector."""
        github = _host_key()
        monkeypatch.setenv(
            WORKSPACE_SSH_KNOWN_HOSTS_ENV,
            "@cert-authority *.corp.example ssh-ed25519 AAAA\n"
            "github.com ssh-dss AAAAB3NzaC1kc3M=\n"
            f"github.com {github}\n",
        )
        on_github, on_bastion = build_workspace_ssh_identities(
            [_repository(), _ssh_key()]
        )
        assert on_github["known_hosts"] == [github]
        assert on_bastion["known_hosts"] == []
        assert caplog.text.count(WORKSPACE_SSH_KNOWN_HOSTS_ENV) == 1
        assert "PRIVATE KEY" not in caplog.text

    def test_an_unreadable_default_list_fails_closed(self, monkeypatch, caplog):
        monkeypatch.setenv(WORKSPACE_SSH_KNOWN_HOSTS_ENV, "x" * (65 * 1024))
        row = _repository()
        assert build_workspace_ssh_identities([row]) is None
        (entry,) = build_datasources_payload([row], dependencies=_deps())
        assert entry["ssh_identity"]["unavailable"] == "default_known_hosts_invalid"

    def test_unavailable_reasons_are_fixed_codes(self, caplog):
        """Review: the reason used to quote key lines into READMEs and logs."""
        secret_line = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo="
        row = _repository(
            credentials={
                "auth_method": "ssh",
                "ssh_key": f"{secret_line}\n{secret_line}\n{secret_line}\n",
            }
        )
        (entry,) = build_datasources_payload([row], dependencies=_deps())
        assert entry["ssh_identity"]["unavailable"] == "ssh_key_invalid"
        assert build_workspace_ssh_identities([row]) is None
        assert secret_line not in json.dumps(entry)
        assert secret_line not in caplog.text
        assert "ssh_key_invalid" in caplog.text

    def test_a_stored_alias_shaped_host_is_never_dispatched(self):
        """A row stored before the refusal must not reach any workspace."""
        alias = "srw-repo-" + "c" * 32
        row = _ssh_key(config={"host": alias})
        assert build_workspace_ssh_identities([row]) is None
        (entry,) = build_datasources_payload([row], dependencies=_deps())
        assert "unavailable" in entry["ssh_identity"]

    def test_host_less_ssh_key_has_no_host_settings(self):
        (identity,) = build_workspace_ssh_identities([_ssh_key(config={})])
        assert identity["ssh_host"] is None
        assert identity["ssh_port"] is None
        assert identity["extra_hosts"] == []
        assert identity["known_hosts"] == []


def test_job_start_request_never_reprs_the_keys():
    from orchestrator.schemas.job_runtime import JobStartRequest

    identities = build_workspace_ssh_identities([_repository()])
    request = JobStartRequest(
        job_id="00000000-0000-4000-8000-0000000000c1",
        description="repr stays secret-free",
        workspace_ssh_identities=identities,
    )
    assert "PRIVATE KEY" not in repr(request)


def test_direct_database_callers_cannot_persist_the_field():
    from orchestrator.database.postgres import _strip_managed_repository_authority

    stripped = _strip_managed_repository_authority(
        {
            "workspace_ssh_identities": [{"private_key": "x"}],
            "nested": {"workspace_ssh_identities": [], "kept": 1},
        }
    )
    assert stripped == {"nested": {"kept": 1}}


@pytest.mark.asyncio
async def test_thread_delivery_builds_both_halves_from_one_authorization():
    from types import SimpleNamespace

    from orchestrator.services import thread_mount_rows

    rows = [_repository(), _token_repository()]
    authorize = AsyncMock(return_value=rows)
    deps = SimpleNamespace(
        resolve_authorized_thread_datasources=authorize,
        build_datasources_payload=lambda resolved: build_datasources_payload(
            resolved, dependencies=_deps()
        ),
        build_workspace_ssh_identities=build_workspace_ssh_identities,
    )

    (
        datasources,
        identities,
    ) = await thread_mount_rows.resolve_thread_datasource_delivery(
        {"id": "t"}, {"datasource_ids": [_REPO_ID]}, dependencies=deps
    )

    authorize.assert_awaited_once()
    assert [entry["name"] for entry in datasources] == ["Widget", "Docs"]
    assert [item["alias"] for item in identities] == [
        datasources[0]["ssh_identity"]["alias"]
    ]
    authorize.return_value = []
    assert await thread_mount_rows.resolve_thread_datasource_delivery(
        {"id": "t"}, {}, dependencies=deps
    ) == (None, None)


def test_chart_default_pins_reach_the_orchestrator():
    """``orchestrator.workspaceSshKnownHosts``, modelled on kbGitSshKnownHosts."""
    from tests.test_persistent_lifecycle_helm import (
        _orchestrator_env,
        _render_orchestrator,
    )

    assert WORKSPACE_SSH_KNOWN_HOSTS_ENV not in _orchestrator_env(
        _render_orchestrator()
    )
    pins = "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
    env = _orchestrator_env(
        _render_orchestrator(
            "--set-string", f"orchestrator.workspaceSshKnownHosts={pins}"
        )
    )
    assert env[WORKSPACE_SSH_KNOWN_HOSTS_ENV] == pins
