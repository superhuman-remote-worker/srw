"""C1 validation of connectors whose SSH key reaches a workspace ssh-agent.

A shared ``ssh_key`` or SSH-key ``repository`` connector writes into other
users' ``~/.ssh/config``. Every value that lands on an SSH config line must be
refused rather than escaped when it could smuggle a directive (``ProxyCommand``
runs code), and a passphrase-protected key is refused because a workspace can
never unlock it.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from unittest.mock import AsyncMock

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    PrivateFormat,
    PublicFormat,
)
from fastapi.testclient import TestClient

from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services.workspace_ssh_connector import (
    WorkspaceSshConnectorError,
    repository_uses_ssh_key,
    validate_workspace_ssh_connector,
)
from shared.runtime.core.workspace_ssh_identity import (
    SshEndpointError,
    known_hosts_field_matches,
    normalize_ssh_host,
    normalize_ssh_port,
    normalize_ssh_user,
    parse_known_hosts,
    parse_ssh_repository_url,
    select_known_hosts,
)
from shared.runtime.utils.ssh_key import (
    InvalidSSHKeyError,
    generate_ed25519_keypair,
    private_key_is_encrypted,
    ssh_public_identity,
)

_OWNER_ID = "11111111-1111-1111-1111-111111111111"


# Deterministic, so parametrized test ids agree across xdist workers.
_FIXED_ED25519_BLOB = base64.b64encode(
    len(b"ssh-ed25519").to_bytes(4, "big")
    + b"ssh-ed25519"
    + (32).to_bytes(4, "big")
    + bytes(32)
).decode()


def _host_key() -> str:
    public = Ed25519PrivateKey.generate().public_key()
    return public.public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH).decode()


def _encrypted_openssh_key() -> str:
    return (
        Ed25519PrivateKey.generate()
        .private_bytes(
            Encoding.PEM, PrivateFormat.OpenSSH, BestAvailableEncryption(b"pw")
        )
        .decode()
    )


def _encrypted_pkcs8_key() -> str:
    return (
        Ed25519PrivateKey.generate()
        .private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, BestAvailableEncryption(b"pw")
        )
        .decode()
    )


class TestRepositoryUrl:
    @pytest.mark.parametrize(
        ("url", "host", "port", "user", "path"),
        [
            # The scp form used to parse to no host and wrote ``Host localhost``.
            (
                "git@github.com:acme/widget.git",
                "github.com",
                22,
                "git",
                "acme/widget.git",
            ),
            (
                "ssh://git@gitea.example.com:2222/acme/widget.git",
                "gitea.example.com",
                2222,
                "git",
                "acme/widget.git",
            ),
            (
                "https://github.com/acme/widget",
                "github.com",
                22,
                "git",
                "acme/widget.git",
            ),
            (
                "ssh://deploy@10.0.0.5/group/sub/repo.git",
                "10.0.0.5",
                22,
                "deploy",
                "group/sub/repo.git",
            ),
            ("git@[2001:db8::1]:o/r.git", "2001:db8::1", 22, "git", "o/r.git"),
        ],
    )
    def test_supported_forms_resolve_the_real_endpoint(
        self, url, host, port, user, path
    ):
        target = parse_ssh_repository_url(url)
        assert (target.host, target.port, target.user, target.path) == (
            host,
            port,
            user,
            path,
        )

    @pytest.mark.parametrize(
        "url",
        [
            "git@github.com\n  ProxyCommand sh -c id:o/r.git",
            "ssh://git@host:99999/o/r.git",
            "ssh://-oProxyCommand=id/o/r.git",
            "ssh://host%0aProxyCommand/o/r.git",
            "ssh://-oUser@host/o/r.git",
            "ssh://git@host/../etc/passwd",
            "ssh://git:secret@host/o/r.git",
            "https://oauth2:token@github.com/o/r.git",
            "ftp://host/o/r.git",
            "ssh://host/o/r.git?x=1",
            "",
        ],
    )
    def test_injection_and_credentials_are_refused(self, url):
        with pytest.raises(SshEndpointError):
            parse_ssh_repository_url(url)


class TestEndpointGrammar:
    @pytest.mark.parametrize(
        "host", ["github.com", "GitHub.com", "10.1.2.3", "srw-gitea-ssh.srw.svc"]
    )
    def test_hosts_accepted(self, host):
        assert normalize_ssh_host(host) == host.lower()

    @pytest.mark.parametrize(
        "host",
        [
            "",
            "a b",
            "host\nProxyCommand id",
            "-host",
            "host_name",
            "*",
            "fe80::1%eth0",
            # An identity alias: a Host line for it would take the alias over.
            "srw-repo-" + "0" * 32,
            "SRW-Repo-" + "a" * 32,
            "srw-repo-x.example.com",
        ],
    )
    def test_hosts_refused(self, host):
        with pytest.raises(SshEndpointError):
            normalize_ssh_host(host)

    def test_alias_shaped_hosts_are_refused_on_create_and_in_urls(self):
        alias = "srw-repo-" + "b" * 32
        with pytest.raises(WorkspaceSshConnectorError, match="srw-repo-"):
            validate_workspace_ssh_connector(
                "ssh_key",
                connection_url=None,
                config={"host": alias.upper()},
                credentials={
                    "files": [{"contents": generate_ed25519_keypair().private_key}]
                },
            )
        with pytest.raises(SshEndpointError):
            parse_ssh_repository_url(f"git@{alias}:o/r.git")

    @pytest.mark.parametrize("user", ["git", "APKAEIBAERJR2EXAMPLE", "_svc", "a.b-c"])
    def test_users_accepted(self, user):
        assert normalize_ssh_user(user) == user

    @pytest.mark.parametrize("user", ["", "-oProxyCommand", "a b", "a\nb", "a@b", "%u"])
    def test_users_refused(self, user):
        with pytest.raises(SshEndpointError):
            normalize_ssh_user(user)

    @pytest.mark.parametrize(
        "port", [True, 0, 65536, "22 ", "2x", 1.5, None, "²", "٢٢", "123456"]
    )
    def test_ports_refused(self, port):
        if port == "22 ":
            assert normalize_ssh_port(port) == 22
            return
        with pytest.raises(SshEndpointError):
            normalize_ssh_port(port)

    @pytest.mark.parametrize(
        "url", ["ssh://git@host:0/o/r.git", "ssh://git@host:²/o/r.git"]
    )
    def test_url_ports_are_refused_not_defaulted(self, url):
        with pytest.raises(SshEndpointError):
            parse_ssh_repository_url(url)

    @pytest.mark.parametrize(
        ("url", "clone_url"),
        [
            # Relative scp paths stay relative (a VPS user's home, a forge's
            # owner/repo); git sends exactly the original path.
            ("deploy@vps.example.com:repos/app.git", "ALIAS:repos/app.git"),
            ("git@github.com:acme/widget.git", "ALIAS:acme/widget.git"),
            ("deploy@vps.example.com:~/app.git", "ALIAS:~/app.git"),
            ("https://github.com/acme/widget", "ALIAS:acme/widget.git"),
            # Absolute paths stay absolute.
            ("deploy@vps.example.com:/srv/app.git", "ssh://ALIAS/srv/app.git"),
            ("ssh://git@host:2222/acme/widget.git", "ssh://ALIAS/acme/widget.git"),
            ("ssh://git@host/~deploy/app.git", "ssh://ALIAS/~deploy/app.git"),
        ],
    )
    def test_clone_url_keeps_the_path_meaning(self, url, clone_url):
        assert parse_ssh_repository_url(url).clone_url("ALIAS") == clone_url


class TestKnownHosts:
    def test_bare_and_full_lines_normalize_to_type_and_key(self):
        key = _host_key()
        text = f"# comment\n{key}\ngithub.com {key} trailing comment\n"
        assert parse_known_hosts(text, host="github.com", port=22) == [key]

    def test_non_default_port_uses_bracketed_form(self):
        key = _host_key()
        assert parse_known_hosts(
            f"[gitea.example.com]:2222 {key}", host="gitea.example.com", port=2222
        ) == [key]
        with pytest.raises(SshEndpointError, match="different host"):
            parse_known_hosts(
                f"gitea.example.com {key}", host="gitea.example.com", port=2222
            )

    def test_hashed_host_matches(self):
        key = _host_key()
        salt = b"0123456789abcdef0123"
        digest = hmac.new(salt, b"github.com", hashlib.sha1).digest()
        field = (
            f"|1|{base64.b64encode(salt).decode()}|{base64.b64encode(digest).decode()}"
        )
        assert known_hosts_field_matches(field, host="github.com", port=22)
        assert not known_hosts_field_matches(field, host="gitlab.com", port=22)
        assert parse_known_hosts(f"{field} {key}", host="github.com") == [key]

    def test_wildcards_and_negation(self):
        assert known_hosts_field_matches(
            "*.example.com", host="git.example.com", port=22
        )
        assert not known_hosts_field_matches(
            "*.example.com,!git.example.com", host="git.example.com", port=22
        )

    def test_default_list_selects_matching_lines_only(self):
        github, gitlab = _host_key(), _host_key()
        text = f"github.com {github}\ngitlab.com {gitlab}\n{_host_key()}\n"
        assert select_known_hosts(text, host="gitlab.com") == ([gitlab], 0)

    def test_one_bad_default_line_disables_only_itself(self):
        """Review: a marker or exotic key type used to refuse the whole list."""
        github = _host_key()
        text = (
            "@cert-authority *.example.com ssh-ed25519 " + _FIXED_ED25519_BLOB + "\n"
            "legacy.example.com ssh-dss AAAAB3NzaC1kc3M=\n"
            f"github.com {github}\n"
            "github.com ssh-dss AAAAB3NzaC1kc3M=\n"
            "@revoked github.com ssh-ed25519 " + _FIXED_ED25519_BLOB + "\n"
        )
        # Other hosts' bad lines are not even looked at; this host's are
        # skipped and counted for one warning.
        assert select_known_hosts(text, host="github.com") == ([github], 2)
        assert select_known_hosts(text, host="gitlab.com") == ([], 0)

    @pytest.mark.parametrize(
        "line",
        [
            "@cert-authority *.example.com ssh-ed25519 AAAA",
            "github.com ssh-dss AAAAB3NzaC1kc3M=",
            "github.com ssh-ed25519 not-base64!",
            # A key blob whose embedded type disagrees with the declared one.
            "github.com ssh-rsa " + _FIXED_ED25519_BLOB,
            "github.com",
        ],
    )
    def test_malformed_lines_are_refused(self, line):
        with pytest.raises(SshEndpointError):
            parse_known_hosts(line, host="github.com")


class TestPrivateKeyInspection:
    def test_fingerprint_matches_openssh_form(self):
        keypair = generate_ed25519_keypair("c1")
        public_line, fingerprint = ssh_public_identity(keypair.private_key)
        assert public_line.split()[:2] == keypair.public_key.split()[:2]
        digest = hashlib.sha256(base64.b64decode(public_line.split()[1])).digest()
        assert fingerprint == "SHA256:" + base64.b64encode(digest).decode().rstrip("=")
        assert not private_key_is_encrypted(keypair.private_key)

    @pytest.mark.parametrize("factory", [_encrypted_openssh_key, _encrypted_pkcs8_key])
    def test_encrypted_keys_are_detected_and_refused(self, factory):
        key = factory()
        assert private_key_is_encrypted(key)
        with pytest.raises(InvalidSSHKeyError, match="passphrase"):
            ssh_public_identity(key)

    def test_traditional_pem_proc_type_is_detected(self):
        key = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "Proc-Type: 4,ENCRYPTED\n"
            "DEK-Info: AES-128-CBC,00000000000000000000000000000000\n"
            "\n"
            "AAAA\n"
            "-----END RSA PRIVATE KEY-----\n"
        )
        assert private_key_is_encrypted(key)


class TestValidateWorkspaceSshConnector:
    def test_ssh_key_config_is_normalized(self):
        key = _host_key()
        config = validate_workspace_ssh_connector(
            "ssh_key",
            connection_url=None,
            config={
                "host": "Bastion.Example.com",
                "user": "deploy",
                "port": "2200",
                "known_hosts": f"[bastion.example.com]:2200 {key}",
            },
            credentials={
                "files": [{"contents": generate_ed25519_keypair().private_key}]
            },
        )
        assert config == {
            "host": "bastion.example.com",
            "user": "deploy",
            "port": 2200,
            # Stored host-qualified, so a later host or port edit cannot
            # silently keep trusting this key.
            "known_hosts": f"[bastion.example.com]:2200 {key}",
        }

    def test_a_bare_pin_is_qualified_and_a_host_change_refuses_it(self):
        key = _host_key()
        private = generate_ed25519_keypair().private_key
        stored = validate_workspace_ssh_connector(
            "ssh_key",
            connection_url=None,
            config={"host": "bastion.example.com", "known_hosts": key},
            credentials={"files": [{"contents": private}]},
        )
        assert stored["known_hosts"] == f"bastion.example.com {key}"
        for edit in ({"host": "other.example.com"}, {"port": 2222}):
            with pytest.raises(WorkspaceSshConnectorError, match="different host"):
                validate_workspace_ssh_connector(
                    "ssh_key",
                    connection_url=None,
                    config={**stored, **edit},
                    credentials={},
                    check_key=False,
                )

    def test_refusals_never_quote_the_key(self):
        secret_line = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo="
        for key in (
            f"-----BEGIN OPENSSH PRIVATE KEY-----\n{secret_line}\nnot-an-end-marker\n",
            f"{secret_line}\n{secret_line}\n{secret_line}\n",
        ):
            with pytest.raises(WorkspaceSshConnectorError) as exc:
                validate_workspace_ssh_connector(
                    "ssh_key",
                    connection_url=None,
                    config={},
                    credentials={"files": [{"contents": key}]},
                )
            assert secret_line not in str(exc.value)
            assert exc.value.code == "ssh_key_invalid"

    def test_ssh_key_without_host_has_no_config(self):
        assert (
            validate_workspace_ssh_connector(
                "ssh_key",
                connection_url=None,
                config={},
                credentials={
                    "files": [{"contents": generate_ed25519_keypair().private_key}]
                },
            )
            == {}
        )

    @pytest.mark.parametrize(
        "config",
        [
            {"host": "h\n  ProxyCommand sh -c id"},
            {"host": "h", "user": "u\nProxyCommand id"},
            {"host": "h", "port": "22\nProxyCommand id"},
            {"user": "deploy"},
            {"host": "h", "proxy_command": "id"},
        ],
    )
    def test_ssh_key_config_injection_is_refused(self, config):
        with pytest.raises(WorkspaceSshConnectorError):
            validate_workspace_ssh_connector(
                "ssh_key",
                connection_url=None,
                config=config,
                credentials={
                    "files": [{"contents": generate_ed25519_keypair().private_key}]
                },
            )

    def test_encrypted_ssh_key_is_refused_with_a_clear_message(self):
        with pytest.raises(WorkspaceSshConnectorError, match="passphrase"):
            validate_workspace_ssh_connector(
                "ssh_key",
                connection_url=None,
                config={},
                credentials={"files": [{"contents": _encrypted_openssh_key()}]},
            )

    def test_preserved_key_is_not_rechecked(self):
        assert validate_workspace_ssh_connector(
            "ssh_key",
            connection_url=None,
            config={"host": "h"},
            credentials={},
            check_key=False,
        ) == {"host": "h"}

    def test_ssh_repository_pins_are_normalized_against_its_host(self):
        key = _host_key()
        config = validate_workspace_ssh_connector(
            "repository",
            connection_url="git@github.com:acme/widget.git",
            config={"forge": "github", "known_hosts": f"github.com {key}"},
            credentials={
                "auth_method": "ssh",
                "ssh_key": generate_ed25519_keypair().private_key,
            },
        )
        assert config == {"forge": "github", "known_hosts": f"github.com {key}"}
        # A URL edit to another host or port no longer matches the pin.
        for url in (
            "ssh://git@github.com:2222/acme/widget.git",
            "git@gitlab.com:a/b.git",
        ):
            with pytest.raises(WorkspaceSshConnectorError, match="different host"):
                validate_workspace_ssh_connector(
                    "repository",
                    connection_url=url,
                    config=config,
                    credentials={"auth_method": "ssh"},
                    check_key=False,
                )

    def test_ssh_repository_url_injection_is_refused(self):
        with pytest.raises(WorkspaceSshConnectorError):
            validate_workspace_ssh_connector(
                "repository",
                connection_url="ssh://git@-oProxyCommand=id/o/r.git",
                config={"forge": "gitea"},
                credentials={"ssh_key": generate_ed25519_keypair().private_key},
            )

    def test_encrypted_repository_key_is_refused(self):
        with pytest.raises(WorkspaceSshConnectorError, match="passphrase"):
            validate_workspace_ssh_connector(
                "repository",
                connection_url="git@github.com:acme/widget.git",
                config={"forge": "github"},
                credentials={"auth_method": "ssh", "ssh_key": _encrypted_pkcs8_key()},
            )

    def test_token_repository_cannot_carry_known_hosts(self):
        assert not repository_uses_ssh_key({"auth_method": "token", "ssh_key": "x"})
        with pytest.raises(WorkspaceSshConnectorError, match="only to SSH-key"):
            validate_workspace_ssh_connector(
                "repository",
                connection_url="https://github.com/acme/widget",
                config={"forge": "github", "known_hosts": _host_key()},
                credentials={"token": "t"},
            )


# =============================================================================
# Endpoint level: the validation must be wired into create AND update.
# =============================================================================


@pytest.fixture
def app_client(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _OWNER_ID, "is_admin": False}),
    )
    return TestClient(main.app)


def _echo_create(**kwargs):
    return {
        "id": "22222222-2222-2222-2222-222222222222",
        "name": kwargs["name"],
        "type": kwargs["ds_type"],
        "connection_url": kwargs["connection_url"],
        "description": kwargs["description"],
        "credentials": kwargs["credentials"],
        "job_id": kwargs["job_id"],
        "cli_hint": kwargs["cli_hint"],
        "default_branch": kwargs["default_branch"],
        "config": kwargs["config"],
        "created_by": kwargs["created_by"],
        "is_global": kwargs["is_global"],
        "read_only": kwargs["read_only"],
    }


class TestCreateEndpoint:
    def test_ssh_key_connector_persists_validated_host_fields(
        self, app_client, monkeypatch
    ):
        from orchestrator import main

        create = AsyncMock(side_effect=lambda **kwargs: _echo_create(**kwargs))
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_datasource", create
        )
        response = app_client.post(
            "/api/datasources",
            json={
                "name": "bastion",
                "type": "ssh_key",
                "config": {"host": "bastion.example.com", "user": "deploy"},
                "credentials": {
                    "files": [{"contents": generate_ed25519_keypair().private_key}]
                },
            },
        )
        assert response.status_code < 300, response.text
        assert create.await_args.kwargs["config"] == {
            "host": "bastion.example.com",
            "user": "deploy",
        }

    @pytest.mark.parametrize(
        ("body", "detail"),
        [
            (
                {
                    "type": "ssh_key",
                    "config": {"host": "h\nProxyCommand id"},
                    "credentials": {
                        "files": [{"contents": generate_ed25519_keypair().private_key}]
                    },
                },
                "SSH host",
            ),
            (
                {
                    "type": "ssh_key",
                    "credentials": {"files": [{"contents": _encrypted_openssh_key()}]},
                },
                "passphrase",
            ),
            (
                {
                    "type": "repository",
                    "connection_url": "ssh://git@h/o/r.git",
                    "config": {"forge": "gitea"},
                    "credentials": {
                        "auth_method": "ssh",
                        "ssh_key": _encrypted_pkcs8_key(),
                    },
                },
                "passphrase",
            ),
            (
                {
                    "type": "repository",
                    "connection_url": "git@-oProxyCommand=id:o/r.git",
                    "config": {"forge": "gitea"},
                    "credentials": {
                        "auth_method": "ssh",
                        "ssh_key": generate_ed25519_keypair().private_key,
                    },
                },
                "SSH host",
            ),
        ],
    )
    def test_unsafe_ssh_connectors_are_400_and_never_persisted(
        self, app_client, monkeypatch, body, detail
    ):
        from orchestrator import main

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "create_datasource",
            AsyncMock(side_effect=AssertionError("must not persist")),
        )
        response = app_client.post("/api/datasources", json={"name": "c1", **body})
        assert response.status_code == 400, response.text
        assert detail in response.json()["detail"]
        assert "PRIVATE KEY" not in response.text


class TestUpdateEndpoint:
    def _patch_update(self, monkeypatch, existing_ds):
        from orchestrator import main

        monkeypatch.setattr(
            access_module,
            "require_datasource_owner",
            AsyncMock(return_value=({"id": _OWNER_ID, "is_admin": False}, existing_ds)),
        )
        update = AsyncMock(return_value=True)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "update_datasource", update
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_datasource",
            AsyncMock(return_value=existing_ds),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "list_datasource_projects",
            AsyncMock(return_value=[]),
        )
        return TestClient(main.app), update

    def test_url_only_edit_of_ssh_repository_is_validated(self, monkeypatch):
        existing = {
            "id": "33333333-3333-3333-3333-333333333333",
            "name": "widget",
            "type": "repository",
            "connection_url": "git@github.com:acme/widget.git",
            "credentials": {
                "auth_method": "ssh",
                "ssh_key": generate_ed25519_keypair().private_key,
            },
            "config": {"forge": "github"},
            "is_global": False,
            "read_only": None,
        }
        client, update = self._patch_update(monkeypatch, existing)
        response = client.put(
            f"/api/datasources/{existing['id']}",
            json={"connection_url": "git@host\nProxyCommand id:o/r.git"},
        )
        assert response.status_code == 400, response.text
        update.assert_not_awaited()

    def test_ssh_key_host_edit_is_validated_and_persisted(self, monkeypatch):
        existing = {
            "id": "44444444-4444-4444-4444-444444444444",
            "name": "bastion",
            "type": "ssh_key",
            "connection_url": None,
            "credentials": {
                "files": [{"contents": generate_ed25519_keypair().private_key}]
            },
            "config": {},
            "is_global": False,
            "read_only": None,
        }
        client, update = self._patch_update(monkeypatch, existing)
        response = client.put(
            f"/api/datasources/{existing['id']}",
            json={"config": {"host": "bastion.example.com", "port": 2200}},
        )
        assert response.status_code < 300, response.text
        assert update.await_args.kwargs["config"] == {
            "host": "bastion.example.com",
            "port": 2200,
        }

        update.reset_mock()
        response = client.put(
            f"/api/datasources/{existing['id']}",
            json={"config": {"host": "bastion", "user": "x y"}},
        )
        assert response.status_code == 400, response.text
        update.assert_not_awaited()

    def test_rename_does_not_revalidate_a_legacy_ssh_repository(self, monkeypatch):
        existing = {
            "id": "55555555-5555-5555-5555-555555555555",
            "name": "legacy",
            "type": "repository",
            # Created before C1: no longer passes the strict grammar.
            "connection_url": "git@host_with_underscore:o/r.git",
            "credentials": {"auth_method": "ssh", "ssh_key": "legacy"},
            "config": {"forge": "gitea"},
            "is_global": False,
            "read_only": None,
        }
        client, update = self._patch_update(monkeypatch, existing)
        response = client.put(
            f"/api/datasources/{existing['id']}", json={"name": "renamed"}
        )
        assert response.status_code < 300, response.text
        update.assert_awaited_once()

    def test_switch_to_token_auth_ignores_a_stored_pin(self, monkeypatch):
        existing = {
            "id": "66666666-6666-6666-6666-666666666666",
            "name": "widget",
            "type": "repository",
            "connection_url": "git@github.com:acme/widget.git",
            "credentials": {
                "auth_method": "ssh",
                "ssh_key": generate_ed25519_keypair().private_key,
            },
            "config": {"forge": "github", "known_hosts": _host_key()},
            "is_global": False,
            "read_only": None,
        }
        client, update = self._patch_update(monkeypatch, existing)
        response = client.put(
            f"/api/datasources/{existing['id']}",
            json={"credentials": {"auth_method": "token", "token": "t0k"}},
        )
        assert response.status_code < 300, response.text
        update.assert_awaited_once()
        assert update.await_args.kwargs["config"] is None


# =============================================================================
# Test connection: reach the endpoint, report the host key to pin.
# =============================================================================


def _ssh_key_row(config: dict) -> dict:
    return {
        "id": "77777777-7777-4777-8777-777777777777",
        "name": "Bastion",
        "type": "ssh_key",
        "connection_url": None,
        "credentials": {
            "files": [{"contents": generate_ed25519_keypair().private_key}]
        },
        "config": config,
    }


class TestProbe:
    @pytest.mark.asyncio
    async def test_fetches_the_real_host_key_without_authenticating(self):
        import asyncssh

        from orchestrator.services.workspace_ssh_connector import fetch_ssh_host_key

        host_key = asyncssh.generate_private_key("ssh-ed25519")
        server = await asyncssh.create_server(
            asyncssh.SSHServer,
            "127.0.0.1",
            0,
            server_host_keys=[host_key],
        )
        try:
            port = server.sockets[0].getsockname()[1]
            fetched = await fetch_ssh_host_key("127.0.0.1", port)
        finally:
            server.close()
            await server.wait_closed()
        expected = host_key.export_public_key("openssh").decode().split()[:2]
        assert fetched == " ".join(expected)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pin", [None, "match", "other"])
    async def test_pin_outcomes(self, monkeypatch, pin):
        from orchestrator.services import workspace_ssh_connector

        presented = _host_key()
        monkeypatch.setattr(
            workspace_ssh_connector,
            "fetch_ssh_host_key",
            AsyncMock(return_value=presented),
        )
        config = {"host": "bastion.example.com", "port": 2200}
        if pin is not None:
            config["known_hosts"] = presented if pin == "match" else _host_key()

        result = await workspace_ssh_connector.probe_workspace_ssh_connector(
            _ssh_key_row(config)
        )

        # Host-qualified: pinning it can never follow a later host change.
        assert (
            result["details"]["host_key"] == f"[bastion.example.com]:2200 {presented}"
        )
        assert result["details"]["port"] == 2200
        if pin == "other":
            assert result["status"] == "error"
            assert "not the pinned key" in result["message"]
        else:
            assert result["status"] == "ok"
            assert result["details"]["host_key_pinned"] is (pin == "match")

    @pytest.mark.asyncio
    async def test_unreachable_host_is_an_error_without_detail(self, monkeypatch):
        from orchestrator.services import workspace_ssh_connector

        monkeypatch.setattr(
            workspace_ssh_connector,
            "fetch_ssh_host_key",
            AsyncMock(side_effect=OSError("internal dns detail")),
        )
        result = await workspace_ssh_connector.probe_workspace_ssh_connector(
            _ssh_key_row({"host": "bastion.example.com"})
        )
        assert result == {
            "status": "error",
            "message": "Could not reach SSH host bastion.example.com:22",
        }

    @pytest.mark.asyncio
    async def test_route_probes_an_ssh_key_with_a_host(self, monkeypatch):
        from orchestrator.routers.datasources import test_datasource as route
        from orchestrator.services import workspace_ssh_connector
        from tests.test_repository_probe import _route_deps

        presented = _host_key()
        monkeypatch.setattr(
            workspace_ssh_connector,
            "fetch_ssh_host_key",
            AsyncMock(return_value=presented),
        )
        row = _ssh_key_row({"host": "bastion.example.com"})
        result = await route(object(), row["id"], dependencies=_route_deps(row))
        assert result["status"] == "ok"
        assert result["details"]["host_key"] == f"bastion.example.com {presented}"

    @pytest.mark.asyncio
    async def test_route_probes_the_edited_endpoint_not_the_saved_one(
        self, monkeypatch
    ):
        """The form tests before it saves: Test reaches what is being typed."""
        from orchestrator.routers.datasources import test_datasource as route
        from orchestrator.schemas.datasources import DatasourceTestRequest
        from orchestrator.services import workspace_ssh_connector
        from tests.test_repository_probe import _route_deps

        reached = []

        async def fetch(host, port):
            reached.append((host, port))
            return _host_key()

        monkeypatch.setattr(workspace_ssh_connector, "fetch_ssh_host_key", fetch)
        row = _ssh_key_row({"host": "bastion.example.com"})
        edited = DatasourceTestRequest(
            config={"host": "new-bastion.example.com", "port": 2200}
        )
        result = await route(
            object(), row["id"], body=edited, dependencies=_route_deps(row)
        )
        assert reached == [("new-bastion.example.com", 2200)]
        assert result["details"]["host_key"].startswith(
            "[new-bastion.example.com]:2200 "
        )

        # The edited values are validated exactly as an update would be.
        with pytest.raises(Exception) as refused:
            await route(
                object(),
                row["id"],
                body=DatasourceTestRequest(config={"host": "h\nProxyCommand id"}),
                dependencies=_route_deps(row),
            )
        assert getattr(refused.value, "status_code", None) == 400
        assert len(reached) == 1

    @pytest.mark.asyncio
    async def test_ssh_key_without_host_has_nothing_to_probe(self):
        from orchestrator.services import workspace_ssh_connector

        assert (
            await workspace_ssh_connector.probe_workspace_ssh_connector(
                _ssh_key_row({})
            )
            is None
        )
