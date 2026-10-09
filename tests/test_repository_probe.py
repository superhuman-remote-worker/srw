"""Repository connector probe: who the token is, what it may do, no token
exposure, and only at an address the connector's projects may reach."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from orchestrator.routers.datasources import (
    test_datasource as probe_datasource_endpoint,
)
from orchestrator.services import datasource_config as datasource_config_module
from orchestrator.services.connector_drivers import provider_http
from orchestrator.services.connector_drivers import repository as repository_driver
from orchestrator.services.connector_drivers.provider_http import ProviderNetwork
from shared.runtime.services.forge import (
    ForgeError,
    ForgeRepo,
    ProbeAnswer,
    probe_repository_access,
)
from tests._provider_fakes import fake_resolver


def _route_deps(row: dict, *, store=None):
    """The connector router's collaborators with the owner gate resolved to ``row``.

    Replaces the old ``patch("orchestrator.main.require_datasource_owner")``:
    the route now reads that gate off the dependency dataclass, so patching a
    ``main`` global would no longer intercept it. The gate is still *bound*
    rather than awaited up front, which is what keeps an unexpected resolution
    failure inside the probe's own try/except. ``store`` is what Test reads
    the connector's project tier on (a mock that answers nothing: public
    addresses only).
    """
    from orchestrator.services.deployment_gates import (
        mcp_datasources_enabled as _mcp_datasources_enabled,
    )
    from orchestrator.routers.datasources import DatasourcesDependencies
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.connector_drivers import builtin_connector_drivers
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    store = store if store is not None else MagicMock()
    return DatasourcesDependencies(
        store=store,
        operations=DatasourceDependencies(
            store=store,
            vector_db=MagicMock(),
            knowledge_index=KnowledgeIndexDependencies(
                store=store,
                vector_db=MagicMock(),
                gitea_client=MagicMock(),
                logger=MagicMock(),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
            mcp_datasources_enabled=_mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
            connector_drivers=builtin_connector_drivers(),
        ),
        require_datasource_owner=AsyncMock(return_value=({}, row)),
    )


TOKEN = "ghp_secretsecretsecret"
DS_ID = "11111111-2222-3333-4444-555555555555"
#: What the fake resolver answers for the public forges (documentation range).
PUBLIC = "203.0.113.20"
#: The fixed texts Test answers with when it does not probe.
REFUSED = (
    "{forge}'s address is not one this connector's projects may reach (an "
    "operator may allow a private host in connectors.providerMinting.privateHosts)"
)
UNRESOLVED = "{forge}'s host does not resolve"
#: What a refused answer's body says: it must never reach the user.
LEAK = "internal-detail-7f3a"


def _target(
    forge_name: str, url_owner: str = "acme", repo: str = "widgets"
) -> ForgeRepo:
    api_base = {
        "github": "https://api.github.com",
        "gitea": "https://git.example.test/api/v1",
        "gitlab": "https://gitlab.example.test/api/v4",
    }[forge_name]
    return ForgeRepo(
        forge=forge_name, api_base=api_base, owner=url_owner, repo=repo, token=TOKEN
    )


def _handler(
    *,
    user: dict | int,
    repo: dict | int,
    user_headers: dict[str, str] | None = None,
):
    """Answer ``/user`` and the repository read; ints are bare status codes,
    answered with a body that must never reach the user."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/user"):
            if isinstance(user, int):
                return httpx.Response(user, json={"message": f"Bad credentials {LEAK}"})
            return httpx.Response(200, json=user, headers=user_headers or {})
        if isinstance(repo, int):
            return httpx.Response(repo, json={"message": f"Not Found {LEAK}"})
        return httpx.Response(200, json=repo)

    return handler


def _transport(
    *,
    user: dict | int,
    repo: dict | int,
    user_headers: dict[str, str] | None = None,
    seen: list | None = None,
) -> httpx.MockTransport:
    answer = _handler(user=user, repo=repo, user_headers=user_headers)

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return answer(request)

    return httpx.MockTransport(handler)


def _fetch(transport: httpx.MockTransport):
    """The probe's GET over ``transport``, unchecked: these tests read the
    forge's answers; the ones through Test below check the address."""

    async def fetch(url: str, headers: dict[str, str]) -> ProbeAnswer:
        async with httpx.AsyncClient(transport=transport) as client:
            response = await client.get(url, headers=headers)
        return ProbeAnswer(
            response.status_code,
            {name.lower(): value for name, value in response.headers.items()},
            response.content,
        )

    return fetch


def _forge(
    monkeypatch,
    handler,
    *,
    addresses: dict[str, tuple[str, ...]] | None = None,
    private_hosts: tuple[str, ...] = (),
) -> list[httpx.Request]:
    """Send Test's forge calls (``provider_http``) to ``handler`` (sync or
    async), resolving names from ``addresses`` (default: api.github.com is
    public); returns the requests it saw."""
    seen: list[httpx.Request] = []

    async def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        answer = handler(request)
        if asyncio.iscoroutine(answer):
            answer = await answer
        return answer

    transport = httpx.MockTransport(record)

    def make(*, verify=True, timeout=10.0) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=transport, timeout=timeout, follow_redirects=False
        )

    monkeypatch.setitem(provider_http._state, "factory", make)
    monkeypatch.setitem(
        provider_http._state,
        "network",
        ProviderNetwork(
            resolver=fake_resolver(
                {"api.github.com": (PUBLIC,)} if addresses is None else addresses
            ),
            private_hosts=frozenset(private_hosts),
        ),
    )
    return seen


class _TierStore:
    """A store whose connector is on a tier that allows private addresses
    (``allowed``) or not."""

    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed
        self.asked: list[tuple] = []

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def fetchrow(self, query: str, *args):
        self.asked.append(args)
        return {"is_global": False, "projects": 1, "allowed": int(self.allowed)}


class TestProbeGitHub:
    @pytest.mark.asyncio
    async def test_classic_admin_token_is_named_and_warned(self):
        seen: list[httpx.Request] = []
        transport = _transport(
            user={"login": "owner", "id": 7},
            user_headers={"X-OAuth-Scopes": "public_repo, read:org"},
            repo={
                "default_branch": "main",
                "permissions": {"admin": True, "push": True, "pull": True},
            },
            seen=seen,
        )

        facts = await probe_repository_access(
            _target("github"), fetch=_fetch(transport)
        )

        assert facts["principal"] == "owner"
        assert facts["token_class"] == "classic"
        assert facts["scopes"] == ["public_repo", "read:org"]
        assert facts["is_admin"] is True and facts["can_write"] is True
        assert facts["default_branch"] == "main"
        assert any("admin bypass" in w for w in facts["warnings"])
        assert seen[0].headers["Authorization"] == f"Bearer {TOKEN}"
        assert seen[0].url == "https://api.github.com/user"
        assert seen[1].url == "https://api.github.com/repos/acme/widgets"

    @pytest.mark.asyncio
    async def test_fine_grained_write_token_has_no_warnings(self):
        transport = _transport(
            user={"login": "srw-bot", "id": 99},
            repo={
                "default_branch": "main",
                "permissions": {"admin": False, "push": True, "pull": True},
            },
        )

        facts = await probe_repository_access(
            _target("github"), fetch=_fetch(transport)
        )

        assert facts["token_class"] == "fine-grained"
        assert facts["scopes"] is None
        assert facts["is_admin"] is False and facts["can_write"] is True
        assert facts["warnings"] == []

    @pytest.mark.asyncio
    async def test_classic_repo_scope_is_warned_as_account_wide(self):
        transport = _transport(
            user={"login": "srw-bot", "id": 99},
            user_headers={"X-OAuth-Scopes": "repo"},
            repo={"default_branch": "main", "permissions": {"push": True}},
        )

        facts = await probe_repository_access(
            _target("github"), fetch=_fetch(transport)
        )

        assert facts["token_class"] == "classic"
        assert any("'repo' scope" in w for w in facts["warnings"])


class TestProbeOtherForges:
    @pytest.mark.asyncio
    async def test_gitea_uses_token_scheme_and_unknown_class(self):
        seen: list[httpx.Request] = []
        transport = _transport(
            user={"login": "bot", "id": 3},
            repo={"default_branch": "main", "permissions": {"push": True}},
            seen=seen,
        )

        facts = await probe_repository_access(_target("gitea"), fetch=_fetch(transport))

        assert facts["token_class"] == "unknown"
        assert facts["can_write"] is True and facts["is_admin"] is False
        assert seen[0].headers["Authorization"] == f"token {TOKEN}"
        assert seen[1].url == "https://git.example.test/api/v1/repos/acme/widgets"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "level, can_write, is_admin",
        [(20, False, False), (30, True, False), (40, True, True)],
    )
    async def test_gitlab_maps_access_levels(self, level, can_write, is_admin):
        seen: list[httpx.Request] = []
        transport = _transport(
            user={"username": "bot", "id": 3},
            repo={
                "default_branch": "main",
                "permissions": {
                    "project_access": {"access_level": level},
                    "group_access": None,
                },
            },
            seen=seen,
        )

        facts = await probe_repository_access(
            _target("gitlab"), fetch=_fetch(transport)
        )

        assert facts["principal"] == "bot"
        assert facts["can_write"] is can_write and facts["is_admin"] is is_admin
        assert seen[0].headers["PRIVATE-TOKEN"] == TOKEN
        assert (
            seen[1].url == "https://gitlab.example.test/api/v4/projects/acme%2Fwidgets"
        )


class TestProbeFailures:
    @pytest.mark.asyncio
    async def test_rejected_token_never_echoes_it(self):
        transport = _transport(user=401, repo={})

        with pytest.raises(ForgeError, match="rejected the token") as exc:
            await probe_repository_access(_target("github"), fetch=_fetch(transport))
        assert TOKEN not in str(exc.value)

    @pytest.mark.asyncio
    async def test_invisible_repository_is_a_404_explanation(self):
        transport = _transport(user={"login": "bot", "id": 1}, repo=404)

        with pytest.raises(ForgeError, match="not found on github, or the token"):
            await probe_repository_access(_target("github"), fetch=_fetch(transport))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("user", "repo", "expected"),
        [
            (403, {}, "github could not identify the token (HTTP 403)"),
            (500, {}, "github could not identify the token (HTTP 500)"),
            ({"login": "bot"}, 403, "github refused the repository read (HTTP 403)"),
        ],
    )
    async def test_the_forges_own_words_stay_in_the_log(
        self, caplog, user, repo, expected
    ):
        transport = _transport(user=user, repo=repo)

        with caplog.at_level("INFO", logger="shared.runtime.services.forge"):
            with pytest.raises(ForgeError) as exc:
                await probe_repository_access(
                    _target("github"), fetch=_fetch(transport)
                )
        assert str(exc.value) == expected
        assert LEAK in caplog.text

    @pytest.mark.asyncio
    async def test_a_redirect_is_named_and_never_followed(self):
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                302, headers={"Location": "http://169.254.169.254/latest/"}
            )

        with pytest.raises(ForgeError) as exc:
            await probe_repository_access(
                _target("github"), fetch=_fetch(httpx.MockTransport(handler))
            )
        assert str(exc.value) == (
            "github answered the token check with a redirect (HTTP 302), which "
            "SRW does not follow"
        )
        assert len(seen) == 1

    @pytest.mark.asyncio
    async def test_a_non_json_answer_is_named_without_its_body(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=f"<html>{LEAK}</html>")

        with pytest.raises(ForgeError) as exc:
            await probe_repository_access(
                _target("github"), fetch=_fetch(httpx.MockTransport(handler))
            )
        assert str(exc.value) == "github returned a non-JSON probe response"
        assert exc.value.__cause__ is None and LEAK not in repr(exc.value)

    @pytest.mark.asyncio
    async def test_missing_token_is_refused_before_any_request(self):
        target = ForgeRepo(
            forge="github",
            api_base="https://api.github.com",
            owner="a",
            repo="b",
            token="",
        )

        async def fetch(url, headers):
            raise AssertionError("no request may be made")

        with pytest.raises(ForgeError, match="no token"):
            await probe_repository_access(target, fetch=fetch)


class TestRepositoryConnectorEndpoint:
    """``POST /api/datasources/{id}/test`` for ``repository`` rows."""

    DS_ID = DS_ID

    def _row(self, **overrides) -> dict:
        row = {
            "id": self.DS_ID,
            "type": "repository",
            "connection_url": "https://github.com/acme/widgets.git",
            "config": json.dumps({"forge": "github"}),
            "credentials": {"token": TOKEN},
            "default_branch": "develop",
            "read_only": False,
        }
        row.update(overrides)
        return row

    @pytest.mark.asyncio
    async def test_token_connector_reports_principal_and_branch(self, monkeypatch):
        _forge(
            monkeypatch,
            _handler(
                user={"login": "srw-bot", "id": 5},
                repo={"default_branch": "main", "permissions": {"push": True}},
            ),
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "ok"
        assert "Authenticated as srw-bot (fine-grained token)" in result["message"]
        assert "write access to acme/widgets" in result["message"]
        assert "default branch main (connector targets develop)" in result["message"]
        assert "WARNING" not in result["message"]
        assert result["details"]["principal"] == "srw-bot"
        assert TOKEN not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_read_only_mismatch_and_admin_are_warned(self, monkeypatch):
        _forge(
            monkeypatch,
            _handler(
                user={"login": "owner", "id": 5},
                user_headers={"X-OAuth-Scopes": "repo"},
                repo={
                    "default_branch": "main",
                    "permissions": {"admin": True, "push": False},
                },
            ),
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "ok"
        message = result["message"]
        assert "WARNING" in message
        assert "admin bypass" in message
        assert (
            "cannot push to acme/widgets but the connector is not marked read-only"
            in message
        )
        assert "'repo' scope" in message
        assert len(result["details"]["warnings"]) == 3

    @pytest.mark.asyncio
    async def test_rejected_token_is_an_error_result(self, monkeypatch):
        _forge(monkeypatch, _handler(user=401, repo={}))
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "error"
        assert "rejected the token" in result["message"]
        assert TOKEN not in result["message"]
        assert LEAK not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_ssh_connector_reports_its_host_key(self, monkeypatch):
        """C1: no forge API takes a deploy key; Test reports the host key, read
        at the address github.com resolved to."""
        from orchestrator.services import workspace_ssh_connector
        from shared.runtime.utils.ssh_key import generate_ed25519_keypair

        _forge(
            monkeypatch,
            _handler(user=401, repo={}),
            addresses={"github.com": (PUBLIC,)},
        )
        host_key = generate_ed25519_keypair().public_key.split()
        reached = []

        async def fetch(host, port):
            reached.append((host, port))
            return " ".join(host_key[:2])

        monkeypatch.setattr(workspace_ssh_connector, "fetch_ssh_host_key", fetch)
        private_key = generate_ed25519_keypair().private_key
        row = self._row(credentials={"auth_method": "ssh", "ssh_key": private_key})
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert reached == [(PUBLIC, 22)]
        assert result["status"] == "ok"
        assert result["details"]["host_key"] == "github.com " + " ".join(host_key[:2])
        assert result["details"]["host_key_pinned"] is False
        assert "PRIVATE KEY" not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_unusable_ssh_connector_is_an_error_result(self):
        row = self._row(credentials={"ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----"})
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result["status"] == "error"
        assert "Invalid SSH key" in result["message"]

    @pytest.mark.asyncio
    async def test_self_hosted_without_forge_is_an_error_not_a_guess(self):
        row = self._row(
            connection_url="https://git.example.test/acme/widgets.git", config="{}"
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result["status"] == "error"
        assert "forge" in result["message"]

    @pytest.mark.asyncio
    async def test_generic_connector_keeps_its_no_test_message(self):
        row = self._row(type="generic", credentials={})
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result == {
            "status": "ok",
            "message": "No connectivity test for generic connectors",
        }


def _repository(url: str, forge: str = "gitea") -> dict:
    return {
        "id": DS_ID,
        "type": "repository",
        "connection_url": url,
        "config": json.dumps({"forge": forge}),
        "credentials": {"auth_method": "token", "token": TOKEN},
        "read_only": True,
    }


def _ok() -> object:
    return _handler(
        user={"login": "bot", "id": 1},
        repo={"default_branch": "main", "permissions": {"push": False}},
    )


async def _test(row: dict, *, store=None) -> dict:
    return await probe_datasource_endpoint(
        object(), DS_ID, dependencies=_route_deps(row, store=store)
    )


class TestRepositoryTestEgress:
    """Test asks a forge only at an address the connector's projects may
    reach: nothing is sent to any other, and what the user reads is one of
    a fixed set of reasons."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "origin",
        [
            "https://10.1.2.3",  # private (RFC 1918)
            "https://192.168.1.10:3000",
            "https://100.64.0.9",  # CGNAT
            "https://127.0.0.1",  # loopback
            "https://[::1]",
            "https://[::ffff:127.0.0.1]",  # IPv4-mapped loopback
            "https://[::ffff:a9fe:a9fe]",  # IPv4-mapped metadata
            "http://169.254.169.254",  # link-local, cloud metadata
            "https://[fe80::1]",
            "https://[fd00:ec2::254]",  # AWS metadata over IPv6
            "https://168.63.129.16",  # Azure wireserver
            "https://10.43.0.10:8085",  # the cluster's service range
            "https://10.42.1.7",  # the cluster's pod range
            "https://0.0.0.0",
        ],
    )
    async def test_a_refused_address_is_never_sent_a_request(self, monkeypatch, origin):
        seen = _forge(monkeypatch, _ok(), addresses={})
        result = await _test(_repository(f"{origin}/acme/widgets.git"))

        assert result == {"status": "error", "message": REFUSED.format(forge="gitea")}
        assert seen == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "answers",
        [
            ("10.43.0.10",),  # srw-orchestrator.srw.svc
            ("10.0.0.5",),
            ("127.0.0.1",),
            ("169.254.169.254",),
            ("203.0.113.9", "10.0.0.5"),  # one public answer is not enough
        ],
    )
    async def test_a_name_that_resolves_to_a_refused_address(
        self, monkeypatch, answers
    ):
        seen = _forge(
            monkeypatch, _ok(), addresses={"srw-orchestrator.srw.svc": answers}
        )
        result = await _test(
            _repository("https://srw-orchestrator.srw.svc:8085/acme/widgets.git")
        )

        assert result == {"status": "error", "message": REFUSED.format(forge="gitea")}
        assert seen == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "answers",
        [
            ("::ffff:10.43.0.10",),  # IPv4-mapped cluster address
            ("2001:db8::5", "fd12::5"),  # one ULA (private) answer
            ("64:ff9b::a2b:a0a",),  # NAT64 of 10.43.10.10
        ],
    )
    async def test_a_dual_stack_answer_is_checked_too(self, monkeypatch, answers):
        seen = _forge(monkeypatch, _ok(), addresses={"git.example.test": answers})
        network = provider_http._state["network"]
        monkeypatch.setitem(
            provider_http._state,
            "network",
            ProviderNetwork(resolver=network.resolver, ipv6=True),
        )
        result = await _test(_repository("https://git.example.test/acme/widgets.git"))

        assert result == {"status": "error", "message": REFUSED.format(forge="gitea")}
        assert seen == []

    @pytest.mark.asyncio
    async def test_an_ipv4_only_network_never_dials_an_ipv6_answer(self, monkeypatch):
        seen = _forge(
            monkeypatch, _ok(), addresses={"git.example.test": ("::ffff:10.43.0.10",)}
        )
        result = await _test(_repository("https://git.example.test/acme/widgets.git"))

        assert result == {
            "status": "error",
            "message": UNRESOLVED.format(forge="gitea"),
        }
        assert seen == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("host", ["2130706433", "0x7f.1"])
    async def test_a_numeric_spelling_is_checked_as_what_it_resolves_to(
        self, monkeypatch, host
    ):
        """The system resolver turns these into 127.0.0.1 (glibc's
        inet_aton, no DNS); the check sees that address, and the request
        would dial it, never the spelling."""
        import socket

        try:
            answers = {
                info[4][0]
                for info in socket.getaddrinfo(
                    host, None, socket.AF_INET, socket.SOCK_STREAM
                )
            }
        except OSError:
            answers = set()
        if answers != {"127.0.0.1"}:
            pytest.skip(f"this platform's resolver reads {host!r} as {answers}")
        seen = _forge(monkeypatch, _ok())
        monkeypatch.setitem(
            provider_http._state,
            "network",
            ProviderNetwork(resolver=provider_http.provider_resolver),
        )
        result = await _test(_repository(f"https://{host}/acme/widgets.git"))

        assert result == {"status": "error", "message": REFUSED.format(forge="gitea")}
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_tests_lookups_run_on_threads_of_their_own(self, monkeypatch):
        """In the installation's own network (no installed resolver), Test
        resolves on the Test lane, never on the threads mints use."""
        seen = _forge(monkeypatch, _ok())
        asked: list[str] = []

        async def test_lane(host, ipv6):
            asked.append(host)
            return (PUBLIC,)

        async def mint_lane(host, ipv6):
            raise AssertionError("a Test resolved on the mints' threads")

        monkeypatch.setattr(provider_http, "connector_test_resolver", test_lane)
        monkeypatch.setattr(provider_http, "provider_resolver", mint_lane)
        monkeypatch.setitem(
            provider_http._state, "network", ProviderNetwork(resolver=mint_lane)
        )
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert result["status"] == "ok", result
        assert asked == ["api.github.com", "api.github.com"]
        assert len(seen) == 2

    @pytest.mark.asyncio
    async def test_an_ipv6_literal_on_an_ipv4_only_network_is_refused(
        self, monkeypatch
    ):
        """Its pod or service range (k3s: 2001:cafe:43::/112) is in no list
        an IPv4-only installation checks."""
        url = "https://[2001:cafe:43::a]:8085/acme/widgets.git"
        seen = _forge(monkeypatch, _ok())
        result = await _test(_repository(url))
        assert result == {"status": "error", "message": REFUSED.format(forge="gitea")}
        assert seen == []

        # A dual-stack installation checks it, as any address.
        monkeypatch.setitem(
            provider_http._state,
            "network",
            ProviderNetwork(resolver=fake_resolver({}), ipv6=True),
        )
        result = await _test(_repository(url))
        assert result["status"] == "ok", result
        assert {str(request.url.host) for request in seen} == {"2001:cafe:43::a"}

    @pytest.mark.asyncio
    async def test_an_international_name_is_resolved_and_named_as_its_ascii_form(
        self, monkeypatch
    ):
        """Python's resolver and TLS encode a Unicode name by IDNA 2003
        (``faß.de`` -> ``fass.de``), the Host header by IDNA 2008
        (``xn--fa-hia.de``): Test asks one name, the one the header names."""
        seen = _forge(monkeypatch, _ok(), addresses={"xn--fa-hia.de": (PUBLIC,)})
        result = await _test(_repository("https://faß.de/acme/widgets.git"))

        assert result["status"] == "ok", result
        assert {request.headers["host"] for request in seen} == {"xn--fa-hia.de"}
        assert {request.extensions.get("sni_hostname") for request in seen} == {
            "xn--fa-hia.de"
        }

    @pytest.mark.asyncio
    async def test_a_url_httpx_refuses_is_a_fixed_answer_not_a_server_error(
        self, monkeypatch
    ):
        seen = _forge(monkeypatch, _ok())
        result = await _test(_repository("https://010.0.0.1/acme/widgets.git"))

        assert result == {
            "status": "error",
            "message": "gitea's address is not one SRW can use",
        }
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_public_forge_is_asked_at_the_checked_address(self, monkeypatch):
        seen = _forge(monkeypatch, _ok())
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert result["status"] == "ok", result
        assert [request.url.host for request in seen] == [PUBLIC, PUBLIC]
        assert {request.headers["host"] for request in seen} == {"api.github.com"}
        assert {request.extensions.get("sni_hostname") for request in seen} == {
            "api.github.com"
        }
        assert seen[0].headers["authorization"] == f"Bearer {TOKEN}"
        assert seen[0].headers["accept-encoding"] == "identity"

    @pytest.mark.asyncio
    async def test_a_tier_that_allows_private_addresses_reaches_a_private_forge(
        self, monkeypatch
    ):
        seen = _forge(monkeypatch, _ok(), addresses={"git.lan": ("192.168.1.10",)})
        store = _TierStore(allowed=True)
        result = await _test(
            _repository("https://git.lan:3000/acme/widgets.git"), store=store
        )

        assert result["status"] == "ok", result
        assert store.asked, "the tier was not read"
        assert [str(request.url.host) for request in seen] == ["192.168.1.10"] * 2
        assert seen[0].headers["host"] == "git.lan:3000"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("origin", ["https://10.43.0.10", "http://169.254.169.254"])
    async def test_a_private_tier_never_reaches_the_cluster_or_metadata(
        self, monkeypatch, origin
    ):
        seen = _forge(monkeypatch, _ok())
        result = await _test(
            _repository(f"{origin}/acme/widgets.git"), store=_TierStore(allowed=True)
        )

        assert result["message"] == REFUSED.format(forge="gitea")
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_tier_without_private_addresses_is_refused(self, monkeypatch):
        seen = _forge(monkeypatch, _ok(), addresses={"git.lan": ("192.168.1.10",)})
        result = await _test(
            _repository("https://git.lan/acme/widgets.git"),
            store=_TierStore(allowed=False),
        )

        assert result["message"] == REFUSED.format(forge="gitea")
        assert seen == []

    @pytest.mark.asyncio
    async def test_an_operator_listed_host_may_be_in_the_cluster(self, monkeypatch):
        seen = _forge(
            monkeypatch,
            _ok(),
            addresses={"srw-gitea": ("10.43.0.7",)},
            private_hosts=("srw-gitea:3000",),
        )
        result = await _test(_repository("http://srw-gitea:3000/acme/widgets.git"))

        assert result["status"] == "ok", result
        assert [str(request.url.host) for request in seen] == ["10.43.0.7"] * 2
        assert seen[0].headers["host"] == "srw-gitea:3000"

        # The listing names its port: another port of the host is refused.
        seen.clear()
        result = await _test(_repository("http://srw-gitea:8085/acme/widgets.git"))
        assert result["message"] == REFUSED.format(forge="gitea")
        assert seen == []

    @pytest.mark.asyncio
    async def test_a_redirect_is_not_followed(self, monkeypatch):
        seen = _forge(
            monkeypatch,
            lambda request: httpx.Response(
                302, headers={"Location": "http://169.254.169.254/latest/meta-data/"}
            ),
        )
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert result == {
            "status": "error",
            "message": (
                "github answered the token check with a redirect (HTTP 302), which "
                "SRW does not follow"
            ),
        }
        assert len(seen) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("handler", "expected"),
        [
            (
                _handler(user=403, repo={}),
                "github could not identify the token (HTTP 403)",
            ),
            (
                _handler(user={"login": "bot"}, repo=500),
                "github refused the repository read (HTTP 500)",
            ),
            (
                lambda request: httpx.Response(200, text=f"<pre>{LEAK}</pre>"),
                "github returned a non-JSON probe response",
            ),
        ],
    )
    async def test_the_user_never_reads_an_answers_body(
        self, monkeypatch, handler, expected
    ):
        _forge(monkeypatch, handler)
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert result == {"status": "error", "message": expected}
        assert LEAK not in json.dumps(result)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (httpx.ConnectError, "github could not be reached"),
            (httpx.ReadError, "github could not be reached"),
        ],
    )
    async def test_a_transport_failure_is_a_fixed_reason(
        self, monkeypatch, error, expected
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            raise error(f"connect to 10.43.0.10:8085 {LEAK}", request=request)

        _forge(monkeypatch, handler)
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert result == {"status": "error", "message": expected}

    @pytest.mark.asyncio
    async def test_a_name_that_does_not_resolve(self, monkeypatch):
        seen = _forge(monkeypatch, _ok(), addresses={})
        result = await _test(_repository("https://git.example.test/acme/widgets.git"))

        assert result == {
            "status": "error",
            "message": UNRESOLVED.format(forge="gitea"),
        }
        assert seen == []

    @pytest.mark.asyncio
    async def test_both_reads_share_one_deadline(self, monkeypatch):
        calls: list[str] = []

        async def slow(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if request.url.path.endswith("/user"):
                await asyncio.sleep(0.25)
                return httpx.Response(200, json={"login": "bot"})
            await asyncio.sleep(3600)
            return httpx.Response(200, json={})

        _forge(monkeypatch, slow)
        monkeypatch.setattr(repository_driver, "PROBE_DEADLINE_SECONDS", 0.4)
        started = asyncio.get_running_loop().time()
        result = await _test(
            _repository("https://github.com/acme/widgets.git", forge="github")
        )

        assert asyncio.get_running_loop().time() - started < 2
        assert result["status"] == "error"
        assert "timed out" in result["message"]
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_the_connectors_upstream_ca_is_what_the_forge_is_verified_with(
        self, monkeypatch
    ):
        from tests._provider_fakes import FAKE_CA

        verified: list = []
        _forge(monkeypatch, _ok())
        made = provider_http._state["factory"]

        def make(*, verify=True, timeout=10.0):
            verified.append(verify)
            return made(verify=verify, timeout=timeout)

        monkeypatch.setitem(provider_http._state, "factory", make)
        row = _repository("https://github.com/acme/widgets.git", forge="github")
        row["config"] = json.dumps({"forge": "github", "upstream_ca": FAKE_CA})
        result = await _test(row)

        assert result["status"] == "ok", result
        assert verified and all(
            context.cert_store_stats()["x509_ca"] == 1 for context in verified
        )
