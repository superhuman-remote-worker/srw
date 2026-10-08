"""GitHub App installation tokens for repository connectors (connector drivers C5).

The rules (``shared.connectors.github_app``), the App JWT and the token
calls against a fake GitHub API that verifies the JWT with the App's public
key and checks the requested repositories and permissions
(``tests/_provider_fakes.py``), the repository driver's create, update,
bind and Test, and the git swap driver's routing and exchange answer.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.services import connector_git_swap_delivery as swaps
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
    SupportsMintedLeaseUpstream,
)
from orchestrator.services.connector_drivers.git_swap import GitSwapDriver
from orchestrator.services.connector_drivers.github_app import (
    app_jwt,
    covers_only,
    granted_as_asked,
    mint_installation_token,
    normalize_private_key,
    repository_facts,
    revoke_installation_token,
)
from orchestrator.services.connector_drivers.provider_http import ProviderError
from orchestrator.services.connector_drivers.repository import RepositoryDriver
from shared.connectors.builtin import (
    GIT_SWAP_SPEC,
    REPOSITORY_SPEC,
    driver_spec_for_row,
)
from shared.connectors.github_app import (
    GITHUB_COM_API,
    GitHubAppConfigError,
    access_token_request,
    api_host_for,
    default_api_base,
    installation_permissions,
    jwt_claims,
    parse_github_app,
    repository_of,
    uses_github_app,
)
from tests._provider_fakes import (
    FAKE_CA,
    FakeGitHubApi,
    ProviderRouter,
    install,
    rsa_key_pair,
)

CONNECTOR = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
URL = "https://github.com/acme/repo.git"
APP = {"app_id": "4242", "installation_id": "9090"}
PRIVATE, PUBLIC = rsa_key_pair()


@pytest.fixture
def github(monkeypatch):
    api = FakeGitHubApi(public_key_pem=PUBLIC)
    install(monkeypatch, ProviderRouter(github=api))
    return api


def _options(**over):
    return parse_github_app({"github_app": {**APP, **over}}, URL)


class TestRules:
    def test_a_config_names_the_app_the_installation_and_the_repository(self):
        options = _options()
        assert (options.app_id, options.installation_id) == ("4242", "9090")
        assert (options.owner, options.repository) == ("acme", "repo")
        assert options.api_base == GITHUB_COM_API

    @pytest.mark.parametrize(
        ("url", "base"),
        [
            ("https://github.com/o/r", "https://api.github.com"),
            ("https://ghe.corp.example/o/r.git", "https://ghe.corp.example/api/v3"),
            ("https://acme.ghe.com/o/r", "https://api.acme.ghe.com"),
        ],
    )
    def test_the_api_base_follows_the_host(self, url, base):
        assert default_api_base(url) == base

    def test_an_enterprise_api_base_is_configurable(self):
        options = parse_github_app(
            {"github_app": {**APP, "api_base": "https://ghe.corp.example/api/v3/"}},
            "https://ghe.corp.example/acme/repo.git",
        )
        assert options.api_base == "https://ghe.corp.example/api/v3"
        assert options.as_config(configured_api_base=options.api_base) == {
            **APP,
            "api_base": "https://ghe.corp.example/api/v3",
        }

    @pytest.mark.parametrize(
        ("url", "api_base"),
        [
            # The App key would sign for a host the repository is not on.
            ("https://github.com/acme/repo.git", "https://ghe.corp.example/api/v3"),
            ("https://github.com/acme/repo.git", "https://github.com/api/v3"),
            ("https://ghe.corp.example/acme/repo.git", "https://api.github.com"),
            ("https://ghe.corp.example/acme/repo.git", "https://other.corp/api/v3"),
            ("https://acme.ghe.com/acme/repo.git", "https://api.github.com"),
        ],
    )
    def test_the_api_base_is_on_the_repositorys_api_host(self, url, api_base):
        with pytest.raises(GitHubAppConfigError, match="must be on"):
            parse_github_app({"github_app": {**APP, "api_base": api_base}}, url)

    @pytest.mark.parametrize(
        ("url", "host"),
        [
            ("https://github.com/o/r", "api.github.com"),
            ("https://GitHub.com/o/r", "api.github.com"),
            ("https://acme.ghe.com/o/r", "api.acme.ghe.com"),
            ("https://ghe.corp.example:8443/o/r", "ghe.corp.example"),
        ],
    )
    def test_the_api_host_of_a_repository(self, url, host):
        assert api_host_for(url) == host

    @pytest.mark.parametrize(
        "over",
        [
            {"app_id": "0"},
            {"app_id": "abc"},
            {"installation_id": True},
            {"api_base": "http://ghe.corp.example/api/v3"},
            {"api_base": "https://user:pw@ghe.corp.example/api/v3"},
            {"permissions": {"contents": "write"}},
        ],
    )
    def test_a_config_srw_cannot_use_is_refused(self, over):
        with pytest.raises(GitHubAppConfigError):
            _options(**over)

    @pytest.mark.parametrize(
        "url",
        [
            "git@github.com:o/r.git",
            "https://github.com/o",
            "https://github.com/o/r/extra",
            "https://token@github.com/o/r",
            "https://github.com/o/r?x=1",
        ],
    )
    def test_the_url_names_one_https_repository(self, url):
        with pytest.raises(GitHubAppConfigError):
            repository_of(url)

    def test_each_level_gets_contents_and_nothing_more(self):
        assert installation_permissions("ReadOnly") == {"contents": "read"}
        assert installation_permissions("ReadWrite") == {"contents": "write"}
        assert installation_permissions("Admin") == {"contents": "read"}
        assert access_token_request("repo", "ReadOnly") == {
            "repositories": ["repo"],
            "permissions": {"contents": "read"},
        }

    def test_the_jwt_is_backdated_and_under_ten_minutes(self):
        claims = jwt_claims("4242", 1_000_000)
        assert claims["iss"] == "4242"
        assert claims["iat"] == 1_000_000 - 60
        assert 0 < claims["exp"] - claims["iat"] <= 600
        assert claims["exp"] > 1_000_000

    def test_the_granted_permissions_are_checked(self):
        wanted = {"contents": "read"}
        assert granted_as_asked({"contents": "read", "metadata": "read"}, wanted)
        assert granted_as_asked({"contents": "read"}, wanted)
        assert not granted_as_asked({"contents": "write"}, wanted)
        assert not granted_as_asked({"metadata": "read"}, wanted)
        assert not granted_as_asked(
            {"contents": "read", "pull_requests": "write"}, wanted
        )
        assert not granted_as_asked(None, wanted)

    def test_the_connectors_own_read_only_clamps_the_level(self):
        from shared.connectors.contract import effective_access

        assert (
            effective_access(
                {"project_read_only": None, "read_only": True}, REPOSITORY_SPEC
            )
            == "ReadOnly"
        )
        assert effective_access({"is_global": True}, REPOSITORY_SPEC) == "ReadOnly"
        assert (
            effective_access({"project_read_only": True}, REPOSITORY_SPEC) == "ReadOnly"
        )
        assert effective_access({"read_only": False}, REPOSITORY_SPEC) == "ReadWrite"

    def test_the_answer_must_cover_the_one_repository(self):
        assert covers_only([{"full_name": "Acme/Repo"}], "acme", "repo")
        assert covers_only(
            [{"owner": {"login": "acme"}, "name": "repo"}], "acme", "repo"
        )
        assert not covers_only([], "acme", "repo")
        assert not covers_only(None, "acme", "repo")
        assert not covers_only([{"full_name": "acme/other"}], "acme", "repo")
        assert not covers_only([{"full_name": "evil/repo"}], "acme", "repo")
        assert not covers_only([{"name": "repo"}], "acme", "repo")
        assert not covers_only(
            [{"full_name": "acme/repo"}, {"full_name": "acme/other"}], "acme", "repo"
        )


class TestPrivateKey:
    def test_an_rsa_key_is_normalized(self):
        text = normalize_private_key("\r\n" + PRIVATE.replace("\n", "\r\n") + "  ")
        assert text.startswith("-----BEGIN RSA PRIVATE KEY-----")
        assert text.endswith("-----END RSA PRIVATE KEY-----\n")

    def test_other_keys_are_refused(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ec

        ec_key = (
            ec.generate_private_key(ec.SECP256R1())
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
        with pytest.raises(ValueError, match="not an RSA key"):
            normalize_private_key(ec_key)
        with pytest.raises(ValueError, match="not an unencrypted PEM"):
            normalize_private_key("-----BEGIN RSA PRIVATE KEY-----\nAAAA\n")
        with pytest.raises(ValueError, match="needs the App's private key"):
            normalize_private_key("")


class TestCalls:
    @pytest.mark.asyncio
    async def test_a_token_is_minted_for_one_repository_at_the_level(self, github):
        minted = await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert minted.token.startswith("ghs_")
        [(method, path, body)] = github.requests
        assert (method, path) == ("POST", "/app/installations/9090/access_tokens")
        assert body == {"repositories": ["repo"], "permissions": {"contents": "read"}}
        assert github.live(minted.token)["permissions"] == {
            "contents": "read",
            "metadata": "read",
        }
        write = await mint_installation_token(_options(), PRIVATE, "ReadWrite")
        assert github.requests[-1][2]["permissions"] == {"contents": "write"}
        assert github.live(write.token)

    @pytest.mark.asyncio
    async def test_a_jwt_another_key_signed_is_refused(self, github):
        other, _ = rsa_key_pair()
        with pytest.raises(ProviderError, match="HTTP 401") as caught:
            await mint_installation_token(_options(), other, "ReadOnly")
        assert caught.value.transient is False
        with pytest.raises(ProviderError, match="HTTP 401"):
            await mint_installation_token(_options(app_id="7"), PRIVATE, "ReadOnly")

    @pytest.mark.asyncio
    async def test_a_repository_the_installation_does_not_cover_is_refused(
        self, github
    ):
        options = parse_github_app({"github_app": APP}, "https://github.com/acme/other")
        with pytest.raises(ProviderError, match="HTTP 422"):
            await mint_installation_token(options, PRIVATE, "ReadOnly")

    @pytest.mark.asyncio
    async def test_a_level_the_app_lacks_is_refused(self, github):
        github.app_permissions = {"contents": "read"}
        with pytest.raises(ProviderError, match="HTTP 422"):
            await mint_installation_token(_options(), PRIVATE, "ReadWrite")

    @pytest.mark.asyncio
    async def test_more_than_was_asked_is_revoked_and_refused(self, github):
        github.grant_extra = {"administration": "write"}
        with pytest.raises(ProviderError, match="revoked") as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert caught.value.transient is False
        [token] = github.tokens
        assert github.live(token) is None

    @pytest.mark.asyncio
    async def test_other_repositories_in_the_answer_are_revoked_and_refused(
        self, github
    ):
        github.extra_repositories = ["other"]
        with pytest.raises(ProviderError, match="revoked") as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert caught.value.reason == "overbroad"
        [token] = github.tokens
        assert github.live(token) is None

    @pytest.mark.asyncio
    async def test_the_call_dials_the_resolved_address(self, monkeypatch):
        api = FakeGitHubApi(public_key_pem=PUBLIC)
        router = install(monkeypatch, ProviderRouter(github=api))
        await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert router.dialled == [("203.0.113.20", "api.github.com")]

    @pytest.mark.asyncio
    async def test_a_private_api_host_needs_a_tier_that_allows_it(self, monkeypatch):
        api = FakeGitHubApi(public_key_pem=PUBLIC)
        install(
            monkeypatch,
            ProviderRouter(github=api),
            addresses={"api.github.com": ("192.168.10.10",)},
        )
        with pytest.raises(ProviderError) as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert caught.value.reason == "address_refused"
        assert api.requests == []
        minted = await mint_installation_token(
            _options(), PRIVATE, "ReadOnly", allow_private=True
        )
        assert api.live(minted.token)

    @pytest.mark.asyncio
    async def test_the_upstream_ca_verifies_the_calls(self, monkeypatch):
        api = FakeGitHubApi(public_key_pem=PUBLIC)
        router = install(monkeypatch, ProviderRouter(github=api))
        await mint_installation_token(_options(), PRIVATE, "ReadOnly", ca_pem=FAKE_CA)
        subjects = [
            dict(field[0] for field in cert["subject"])
            for cert in router.verified[-1].get_ca_certs()
        ]
        assert {"commonName": "fake-kube-ca"} in subjects
        with pytest.raises(ProviderError) as caught:
            await mint_installation_token(
                _options(), PRIVATE, "ReadOnly", ca_pem="not a certificate"
            )
        assert caught.value.reason == "ca_unusable"

    @pytest.mark.asyncio
    async def test_an_overbroad_token_that_will_not_revoke_is_handed_back(self, github):
        from orchestrator.services.connector_drivers.provider_http import (
            UnrevokedToken,
        )

        github.grant_extra = {"administration": "write"}
        real = github.handle

        def no_revoke(request):
            if request.method == "DELETE":
                import httpx

                return httpx.Response(503, json={"message": "unavailable"})
            return real(request)

        github.handle = no_revoke
        with pytest.raises(UnrevokedToken) as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        [token] = github.tokens
        assert caught.value.minted.token == token and github.live(token)
        assert "keeps revoking" in str(caught.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "words"),
        [
            (302, "a redirect"),
            (409, "an HTTP 4xx answer"),
            (200, "unexpected HTTP 200"),
        ],
    )
    async def test_an_unexpected_status_is_named_by_its_class(
        self, github, status, words
    ):
        github.fail = [status]
        with pytest.raises(ProviderError) as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert words in str(caught.value)

    @pytest.mark.asyncio
    async def test_github_trouble_is_transient(self, github):
        github.fail = [502]
        with pytest.raises(ProviderError) as caught:
            await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        assert caught.value.transient is True

    @pytest.mark.asyncio
    async def test_revoke_deletes_the_token_and_a_dead_one_counts(self, github):
        minted = await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        await revoke_installation_token(GITHUB_COM_API, minted.token)
        assert github.live(minted.token) is None
        assert github.requests[-1][:2] == ("DELETE", "/installation/token")
        await revoke_installation_token(GITHUB_COM_API, minted.token)

    @pytest.mark.asyncio
    async def test_a_read_token_reads_its_repository(self, github):
        minted = await mint_installation_token(_options(), PRIVATE, "ReadOnly")
        facts = await repository_facts(_options(), minted.token)
        assert facts == {
            "repository": "acme/repo",
            "default_branch": "main",
            "private": True,
        }

    def test_the_jwt_verifies_with_the_public_key(self):
        import jwt

        token = app_jwt("4242", PRIVATE, now=2_000_000)
        claims = jwt.decode(
            token, PUBLIC, algorithms=["RS256"], options={"verify_exp": False}
        )
        assert claims["iss"] == "4242"


def _draft(**over):
    fields = dict(
        name="repo",
        connection_url=URL,
        credentials=None,
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    fields.update(over)
    return ConnectorDraft(**fields, supplied=frozenset(over))


def _ctx():
    return SimpleNamespace(
        environment=SimpleNamespace(gates=DeploymentGates(lambda: True, lambda: True)),
        can_autonomous_send=AsyncMock(),
    )


def _app_credentials():
    return {"auth_method": "github_app", "private_key": PRIVATE}


class TestRepositoryDriver:
    def test_the_specs_name_the_key_and_the_app(self):
        assert "github_app" in REPOSITORY_SPEC.config_schema["properties"]
        assert "github_app" in GIT_SWAP_SPEC.config_schema["properties"]
        slots = {slot.name: slot for slot in REPOSITORY_SPEC.credential_slots}
        assert slots["private_key"].delivery is None
        assert "contents: read" in REPOSITORY_SPEC.access_level("ReadOnly").enforced_by

    def test_the_key_is_a_secret_leaf_and_never_config(self):
        driver = RepositoryDriver()
        credentials = _app_credentials()
        assert ((("private_key",), "private_key")) in driver.secret_leaves(credentials)
        assert driver.credential_config(credentials) == {"auth_method": "github_app"}

    @pytest.mark.asyncio
    async def test_a_create_checks_the_app_and_the_key(self):
        normalized = await RepositoryDriver().validate(
            _draft(credentials=_app_credentials(), config={"github_app": APP}),
            existing=None,
            ctx=_ctx(),
        )
        assert normalized.config["github_app"] == APP
        assert normalized.config["forge"] == "github"
        assert set(normalized.credentials) == {"auth_method", "private_key"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("over", "detail"),
        [
            ({"config": {}}, "github_app"),
            ({"config": {"github_app": {"app_id": "1"}}}, "installation_id"),
            (
                {
                    "config": {"github_app": APP},
                    "credentials": {**_app_credentials(), "token": "ghp_x" * 4},
                },
                "not token",
            ),
            (
                {"config": {"github_app": APP, "forge": "gitea"}},
                "forge is github",
            ),
            (
                {
                    "config": {"github_app": APP},
                    "credentials": {"auth_method": "github_app", "private_key": "x"},
                },
                "PEM",
            ),
            (
                {
                    "config": {"github_app": APP},
                    "credentials": {"auth_method": "token", "token": "t" * 20},
                },
                "auth_method github_app",
            ),
        ],
    )
    async def test_a_create_srw_cannot_mint_for_is_refused(self, over, detail):
        body = {"credentials": _app_credentials(), **over}
        with pytest.raises(HTTPException, match=detail):
            await RepositoryDriver().validate(_draft(**body), existing=None, ctx=_ctx())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("edit", "stored_url"),
        [
            # Another API base on the same host is still another target.
            (
                {
                    "connection_url": "https://ghe.corp.example/acme/repo.git",
                    "config": {
                        "forge": "github",
                        "github_app": {
                            **APP,
                            "api_base": "https://ghe.corp.example/api/v4",
                        },
                    },
                },
                "https://ghe.corp.example/acme/repo.git",
            ),
            # Another repository host (and so another API host).
            ({"connection_url": "https://acme.ghe.com/acme/repo.git"}, URL),
            (
                {"connection_url": "https://other.corp.example/acme/repo.git"},
                "https://ghe.corp.example/acme/repo.git",
            ),
            # Another installation, App or repository on the same host.
            (
                {
                    "config": {
                        "forge": "github",
                        "github_app": {**APP, "installation_id": "999999"},
                    }
                },
                URL,
            ),
            (
                {"config": {"forge": "github", "github_app": {**APP, "app_id": "7"}}},
                URL,
            ),
            ({"connection_url": "https://github.com/other-org/secrets.git"}, URL),
            ({"connection_url": "https://github.com/acme/other.git"}, URL),
        ],
        ids=[
            "api-base",
            "github-to-ghe-cloud",
            "ghes-to-ghes",
            "installation",
            "app",
            "other-org",
            "other-repository",
        ],
    )
    async def test_an_edit_may_not_move_the_stored_key(self, edit, stored_url):
        existing = {
            "id": CONNECTOR,
            "name": "repo",
            "type": "repository",
            "connection_url": stored_url,
            "config": {"forge": "github", "github_app": APP},
            "credentials": _app_credentials(),
        }
        with pytest.raises(HTTPException, match="private key again") as caught:
            await RepositoryDriver().validate(
                _draft(**edit), existing=existing, ctx=_ctx()
            )
        assert caught.value.status_code == 400
        # With the key sent again, the move is fine.
        normalized = await RepositoryDriver().validate(
            _draft(**edit, credentials=_app_credentials()),
            existing=existing,
            ctx=_ctx(),
        )
        assert normalized.credentials["private_key"] == PRIVATE

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", ["username", "minted"])
    async def test_a_static_token_carries_no_username(self, key):
        credentials = {"auth_method": "token", "token": "t" * 20, key: "x"}
        with pytest.raises(HTTPException, match="carry no") as caught:
            await RepositoryDriver().validate(
                _draft(credentials=credentials, config={"forge": "github"}),
                existing=None,
                ctx=_ctx(),
            )
        assert caught.value.status_code == 400
        existing = {
            "id": CONNECTOR,
            "name": "repo",
            "type": "repository",
            "connection_url": URL,
            "config": {"forge": "github"},
            "credentials": {"auth_method": "token", "token": "t" * 20},
        }
        with pytest.raises(HTTPException, match="carry no"):
            await RepositoryDriver().validate(
                _draft(credentials=credentials), existing=existing, ctx=_ctx()
            )

    def test_a_public_or_read_only_connector_marks_its_entry(self):
        for over in ({"read_only": True}, {"is_global": True}):
            row = {
                "id": CONNECTOR,
                "type": "repository",
                "name": "repo",
                "connection_url": URL,
                "config": {"forge": "github", "github_app": APP},
                **over,
            }
            ctx = BindContext(
                gates=DeploymentGates(lambda: True, lambda: True),
                logger=logging.getLogger("test"),
                default_known_hosts="",
                git_swap=None,
            )
            entry = RepositoryDriver().bind(row, _app_credentials(), ctx=ctx)
            assert entry["minted"]["read_only"] is True

    @pytest.mark.asyncio
    async def test_dropping_the_app_needs_new_credentials(self):
        existing = {
            "id": CONNECTOR,
            "name": "repo",
            "type": "repository",
            "connection_url": URL,
            "config": {"forge": "github", "github_app": APP},
            "credentials": _app_credentials(),
        }
        with pytest.raises(HTTPException, match="github_app"):
            await RepositoryDriver().validate(
                _draft(config={"forge": "github"}), existing=existing, ctx=_ctx()
            )

    @pytest.mark.asyncio
    async def test_an_app_is_refused_where_the_deployment_turned_minting_off(
        self, monkeypatch
    ):
        from orchestrator.services import connector_minted_credentials as minted

        monkeypatch.setitem(minted._state, "enabled", False)
        with pytest.raises(HTTPException, match="providerMinting") as caught:
            await RepositoryDriver().validate(
                _draft(credentials=_app_credentials(), config={"github_app": APP}),
                existing=None,
                ctx=_ctx(),
            )
        assert caught.value.status_code == 403

    @pytest.mark.asyncio
    async def test_an_update_keeps_the_key_and_checks_the_url(self):
        existing = {
            "id": CONNECTOR,
            "name": "repo",
            "type": "repository",
            "connection_url": URL,
            "config": {"forge": "github", "github_app": APP},
            "credentials": _app_credentials(),
        }
        # The same repository (a .git suffix, another case): the key stays.
        normalized = await RepositoryDriver().validate(
            _draft(connection_url="https://github.com/Acme/Repo"),
            existing=existing,
            ctx=_ctx(),
        )
        assert normalized.credentials is None
        # Another repository needs the key again.
        with pytest.raises(HTTPException, match="private key again"):
            await RepositoryDriver().validate(
                _draft(connection_url="https://github.com/acme/repo2.git"),
                existing=existing,
                ctx=_ctx(),
            )
        with pytest.raises(HTTPException, match="one repository"):
            await RepositoryDriver().validate(
                _draft(connection_url="https://github.com/acme"),
                existing=existing,
                ctx=_ctx(),
            )

    def _bind(self, *, git_swap, fallback="token-in-url", url=URL):
        row = {
            "id": CONNECTOR,
            "type": "repository",
            "name": "repo",
            "connection_url": url,
            "config": {"forge": "github", "github_app": APP},
        }
        ctx = BindContext(
            gates=DeploymentGates(lambda: True, lambda: True),
            logger=logging.getLogger("test"),
            default_known_hosts="",
            git_swap=git_swap,
            git_swap_fallback=fallback,
        )
        return RepositoryDriver().bind(row, _app_credentials(), ctx=ctx)

    def test_bind_never_carries_the_key_and_names_the_provider(self):
        entry = self._bind(git_swap=GitSwapDriver("img"))
        assert entry["credentials"] == {"auth_method": "github_app"}
        assert entry["minted"] == {
            "provider": "github_app",
            "connector_id": CONNECTOR,
            "read_only": False,
        }
        assert "PRIVATE KEY" not in repr(entry)
        # A swap candidate, served by the driver's spec.
        assert entry["git_swap"] == {}
        assert driver_spec_for_row(entry) is GIT_SWAP_SPEC

    def test_without_the_driver_the_fallback_is_stated(self):
        entry = self._bind(git_swap=None)
        assert entry["git_swap"] == {"fallback": swaps.REASONS["not_installed"]}
        refused = self._bind(git_swap=None, fallback="refuse")
        assert "unavailable" in refused["git_swap"] and refused["credentials"] == {}

    def test_a_url_the_driver_cannot_serve_falls_back(self):
        entry = self._bind(
            git_swap=GitSwapDriver("img"), url="https://ghe.corp.example:8443/o/r.git"
        )
        assert entry["git_swap"] == {"fallback": swaps.REASONS["url_not_served"]}

    @pytest.mark.asyncio
    async def test_test_mints_reads_and_revokes(self, github):
        result = await RepositoryDriver().check(
            {
                "id": CONNECTOR,
                "type": "repository",
                "connection_url": URL,
                "config": {"forge": "github", "github_app": APP},
            },
            _app_credentials(),
            ctx=SimpleNamespace(),
        )
        assert result["status"] == "ok", result
        assert "revoked the token" in result["message"]
        assert all(github.live(token) is None for token in github.tokens)
        assert "ghs_" not in str(result) and "PRIVATE" not in str(result)

    @pytest.mark.asyncio
    async def test_test_reports_an_installation_without_the_repository(self, github):
        github.repositories = {"elsewhere"}
        result = await RepositoryDriver().check(
            {
                "id": CONNECTOR,
                "type": "repository",
                "connection_url": URL,
                "config": {"forge": "github", "github_app": APP},
            },
            _app_credentials(),
            ctx=SimpleNamespace(),
        )
        assert result["status"] == "error" and "HTTP 422" in result["message"]


class TestGitSwapDriver:
    def test_it_mints_for_github_app_rows_only(self):
        driver = GitSwapDriver("img")
        assert isinstance(driver, SupportsMintedLeaseUpstream)
        row = {
            "id": CONNECTOR,
            "type": "repository",
            "connection_url": URL,
            "config": {"github_app": APP},
            "credentials": _app_credentials(),
        }
        assert driver.mints_upstream(row)
        assert not driver.mints_upstream(
            {**row, "credentials": {"auth_method": "token", "token": "t" * 20}}
        )
        with pytest.raises(ValueError):
            driver.lease_upstream(row)

    @pytest.mark.asyncio
    async def test_the_exchange_answer_is_the_minted_token(self, monkeypatch):
        from orchestrator.services import connector_minted_credentials as minted

        seen = {}

        async def mint(store, row, *, owner, access):
            seen.update(row=row["id"], owner=owner, access=access)
            return SimpleNamespace(token="ghs_minted")

        monkeypatch.setattr(minted, "minted_lease_upstream", mint)
        answer = await GitSwapDriver("img").minted_lease_upstream(
            {"id": CONNECTOR, "connection_url": URL},
            store=object(),
            owner="thread:x",
            access="ReadOnly",
        )
        # An installation token authenticates as x-access-token (GitHub's
        # documented user); static tokens keep the swap driver's default.
        assert answer == {
            "credential": "ghs_minted",
            "allowed_upstream": ["https://github.com/acme/repo.git"],
            "username": "x-access-token",
        }
        assert seen == {"row": CONNECTOR, "owner": "thread:x", "access": "ReadOnly"}

    def test_a_static_token_names_its_username_too(self):
        answer = GitSwapDriver("img").lease_upstream(
            {
                "id": CONNECTOR,
                "type": "repository",
                "connection_url": "https://gitlab.example/acme/repo.git",
                "config": {"forge": "gitlab"},
                "credentials": {"auth_method": "token", "token": "glpat-" + "t" * 20},
            }
        )
        assert answer["username"] == "oauth2"

    def test_uses_github_app(self):
        assert uses_github_app({"auth_method": " GitHub_App "})
        assert not uses_github_app({"auth_method": "token"})
        assert not uses_github_app(None)


class TestDeliveryWiring:
    def test_a_minting_entry_takes_the_lease_delivery(self):
        from orchestrator.services.connector_credential_leases import needs_leases

        marker = {"provider": "kubernetes", "connector_id": CONNECTOR}
        assert needs_leases([{"type": "kubeconfig", "minted": marker}])
        assert not needs_leases([{"type": "kubeconfig"}])
        assert not needs_leases(
            [{"type": "kubeconfig", "minted": {**marker, "provider": "aws"}}]
        )

    @pytest.mark.asyncio
    async def test_a_github_app_candidate_needs_no_stored_token(self, monkeypatch):
        from orchestrator.services import connector_driver_ca, connector_service_images

        monkeypatch.setattr(connector_driver_ca, "driver_ca", lambda: object())
        monkeypatch.setattr(
            connector_service_images,
            "service_image_settings",
            lambda: SimpleNamespace(service_namespace="srw-connectors"),
        )
        monkeypatch.setattr(
            swaps, "owner_workspace_problem", AsyncMock(return_value=None)
        )
        monkeypatch.setattr(swaps, "current_generation", AsyncMock(return_value="g"))
        monkeypatch.setattr(swaps, "_serving", AsyncMock(return_value=True))
        monkeypatch.setattr(
            swaps, "_state", {"settings": swaps.GitSwapDeliverySettings(installed=True)}
        )
        entry = {
            "type": "repository",
            "connection_url": URL,
            "credentials": {"auth_method": "github_app"},
            "git_swap": {},
            "minted": {"provider": "github_app", "connector_id": CONNECTOR},
        }
        problem = await swaps.git_swap_problem(
            None, entry, connector_id=CONNECTOR, owner=None
        )
        assert problem is None
        entry.pop("minted")
        problem = await swaps.git_swap_problem(
            None, entry, connector_id=CONNECTOR, owner=None
        )
        assert problem is not None and problem.reason == "token_too_short"
