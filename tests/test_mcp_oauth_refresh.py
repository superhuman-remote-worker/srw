"""OAuth refresh for MCP connectors works, and outages are not logouts (G6, G6b).

Before: the bridge returned a "refresh token" that was really a 365-day srw_
access token and never stored it where FastMCP looks refresh tokens up, so
every refresh failed with invalid_grant and the user had to log in again.
That token also worked as an access token on MCP and REST, whatever expiry
the user chose. And the token verifier answered None (a 401) when the
orchestrator was merely unreachable, which an OAuth client treats as a dead
credential.

Now: a short-lived srw_ access token plus a srwr_ refresh token kept by the
orchestrator as a grant, rotated on every use; a replay ends the grant; an
outage surfaces as a server error. The orchestrator side runs on real
PostgreSQL in tests/test_mcp_oauth_grants_real_postgres.py; here a fake
orchestrator with the same semantics stands behind the bridge.
Design: knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md G6.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import fastmcp
import httpx
import pytest
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration, OIDCProxy

from mcp_server import auth as mcp_auth
from mcp_server import oauth_bridge
from mcp_server.auth import McpTokenVerifier, TokenVerificationUnavailable
from mcp_server.oauth_bridge import REFRESH_TOKEN_PREFIX, SRWOAuthProxy

requires_pinned_fastmcp = pytest.mark.skipif(
    tuple(int(part) for part in fastmcp.__version__.split(".")[:2]) < (3, 4),
    reason="needs fastmcp>=3.4 (pip install -r src/mcp_server/requirements.txt)",
)

CLAUDE_CALLBACK = "https://claude.ai/api/mcp/auth_callback"


def _h(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class FakeOrchestrator:
    """The four grant endpoints plus token verify, in memory."""

    def __init__(self):
        self.grants: dict[str, dict] = {}  # current refresh hash -> grant
        self.previous: dict[str, dict] = {}  # rotated-away hash -> grant
        self.access: dict[str, dict] = {}  # access hash -> {revoked, grant}
        self.calls: list[str] = []
        self.down = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request.url.path)
        if self.down:
            return httpx.Response(503, json={"detail": "restarting"})
        body = json.loads(request.content or b"{}")
        route = {
            "/api/internal/mcp-oauth-grant": self._create,
            "/api/internal/mcp-oauth-refresh/lookup": self._lookup,
            "/api/internal/mcp-oauth-refresh/rotate": self._rotate,
            "/api/internal/mcp-oauth-refresh/revoke": self._revoke,
            "/api/internal/mcp-token-verify": self._verify,
        }[request.url.path]
        return route(body)

    def seed(self, client_id: str, refresh: str, access: str, *, expires_at=None):
        grant = {
            "oauth_client_id": client_id,
            "scope": "user",
            "grant_expires_at": expires_at,
            "revoked": False,
            "access_hash": _h(access),
        }
        self.grants[_h(refresh)] = grant
        self.access[_h(access)] = {"revoked": False}
        return grant

    def _create(self, body):
        self.created = body
        self.seed(
            body["oauth_client_id"],
            "unused",
            "unused",
            expires_at=body["grant_expires_at"],
        )
        grant = self.grants.pop(_h("unused"))
        grant["access_hash"] = body["access_token_hash"]
        self.grants[body["refresh_token_hash"]] = grant
        self.access[body["access_token_hash"]] = {"revoked": False}
        return httpx.Response(200, json={"grant_id": "g-1"})

    def _live(self, refresh_hash):
        grant = self.grants.get(refresh_hash)
        if grant and not grant["revoked"]:
            return grant
        return None

    def _lookup(self, body):
        grant = self._live(body["refresh_token_hash"])
        if grant is None:
            replayed = self.previous.get(body["refresh_token_hash"])
            if replayed is not None:
                replayed["revoked"] = True
                self.access[replayed["access_hash"]]["revoked"] = True
            return httpx.Response(404, json={"detail": "Unknown refresh token"})
        return httpx.Response(
            200,
            json={
                "oauth_client_id": grant["oauth_client_id"],
                "scope": grant["scope"],
                "grant_expires_at": grant["grant_expires_at"],
            },
        )

    def _rotate(self, body):
        grant = self._live(body["refresh_token_hash"])
        if grant is None:
            return httpx.Response(404, json={"detail": "Unknown refresh token"})
        del self.grants[body["refresh_token_hash"]]
        self.previous[body["refresh_token_hash"]] = grant
        self.grants[body["new_refresh_token_hash"]] = grant
        self.access.pop(grant["access_hash"], None)
        grant["access_hash"] = body["new_access_token_hash"]
        self.access[grant["access_hash"]] = {"revoked": False}
        expires = datetime.now(timezone.utc) + timedelta(
            seconds=body["access_ttl_seconds"]
        )
        return httpx.Response(
            200,
            json={"scope": grant["scope"], "access_expires_at": expires.isoformat()},
        )

    def _revoke(self, body):
        grant = self._live(body["refresh_token_hash"])
        if grant is not None:
            grant["revoked"] = True
            self.access[grant["access_hash"]]["revoked"] = True
        return httpx.Response(200, json={"status": "revoked"})

    def _verify(self, body):
        entry = self.access.get(body["token_hash"])
        if entry is None or entry["revoked"]:
            return httpx.Response(401, json={"detail": "Invalid or expired token"})
        return httpx.Response(200, json={"user_id": "u-1", "scope": "user"})


@pytest.fixture
def orchestrator(monkeypatch):
    fake = FakeOrchestrator()
    real_client = httpx.AsyncClient

    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(fake.handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(oauth_bridge.httpx, "AsyncClient", client_factory)
    monkeypatch.setattr(mcp_auth.httpx, "AsyncClient", client_factory)
    return fake


def _oidc_config() -> OIDCConfiguration:
    return OIDCConfiguration.model_validate(
        {
            "issuer": "https://auth.example/realms/srw",
            "authorization_endpoint": "https://auth.example/realms/srw/auth",
            "token_endpoint": "https://auth.example/realms/srw/token",
            "jwks_uri": "https://auth.example/realms/srw/certs",
            # Keycloak advertises one; FastMCP mounts /revoke only then.
            "revocation_endpoint": "https://auth.example/realms/srw/revoke",
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        }
    )


@pytest.fixture
def proxy(tmp_path, monkeypatch, orchestrator):
    monkeypatch.setattr(fastmcp.settings, "home", tmp_path)
    with patch.object(OIDCProxy, "get_oidc_configuration", return_value=_oidc_config()):
        return SRWOAuthProxy(
            config_url="https://auth.example/realms/srw/.well-known/openid-configuration",
            client_id="mcp-server",
            client_secret="test-secret",
            base_url="https://mcp.example",
            mcp_verifier=McpTokenVerifier(),
            verify_id_token=True,
        )


def _http(proxy, *, raise_server_exceptions: bool = True):
    from starlette.testclient import TestClient

    server = fastmcp.FastMCP("refresh-test", auth=proxy)
    return TestClient(
        server.http_app(),
        base_url="https://mcp.example",
        raise_server_exceptions=raise_server_exceptions,
    )


def _invalid_grant(response) -> bool:
    # FastMCP answers invalid_grant with HTTP 401 (the MCP spec wants 401 for
    # an invalid token); the error code is what clients act on.
    return response.status_code in (400, 401) and response.json()["error"] == (
        "invalid_grant"
    )


def _register(http) -> str:
    response = http.post(
        "/register",
        json={
            "client_name": "Claude",
            "redirect_uris": [CLAUDE_CALLBACK],
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code", "refresh_token"],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["client_id"]


def _refresh(http, client_id: str, refresh_token: str):
    return http.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        },
    )


@requires_pinned_fastmcp
class TestCodeExchange:
    @pytest.mark.asyncio
    async def test_issues_a_short_access_token_and_a_separate_refresh_token(
        self, proxy, orchestrator
    ):
        from fastmcp.server.auth.oauth_proxy.models import ClientCode

        await proxy._code_store.put(
            key="code-1",
            value=ClientCode(
                code="code-1",
                client_id="dcr-1",
                redirect_uri=CLAUDE_CALLBACK,
                code_challenge="x",
                code_challenge_method="S256",
                scopes=["user", "expiry:30"],
                idp_tokens={
                    "_srw_user_claims": {"sub": "kc-1", "email": "a@example.test"}
                },
                expires_at=int(time.time() + 300),
                created_at=time.time(),
            ),
        )
        client = SimpleNamespace(client_id="dcr-1", client_name="Claude")

        token = await proxy.exchange_authorization_code(
            client, SimpleNamespace(code="code-1")
        )

        assert token.access_token.startswith("srw_")
        assert token.refresh_token.startswith(REFRESH_TOKEN_PREFIX)
        assert token.expires_in <= 3600
        created = orchestrator.created
        assert created["oauth_client_id"] == "dcr-1"
        assert created["refresh_token_hash"] == _h(token.refresh_token)
        assert created["access_token_hash"] == _h(token.access_token)
        grant_expires = datetime.fromisoformat(created["grant_expires_at"])
        assert (
            timedelta(days=29)
            < grant_expires - datetime.now(timezone.utc)
            <= timedelta(days=30)
        )


@requires_pinned_fastmcp
class TestRefreshOverHttp:
    def test_refresh_rotates_both_tokens(self, proxy, orchestrator):
        with _http(proxy) as http:
            client_id = _register(http)
            orchestrator.seed(client_id, "srwr_old", "srw_old")

            response = _refresh(http, client_id, "srwr_old")

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["access_token"].startswith("srw_")
        assert body["refresh_token"].startswith(REFRESH_TOKEN_PREFIX)
        assert body["refresh_token"] != "srwr_old"
        assert 0 < body["expires_in"] <= 3600

    def test_a_replayed_refresh_token_is_refused_and_ends_the_connection(
        self, proxy, orchestrator
    ):
        with _http(proxy) as http:
            client_id = _register(http)
            grant = orchestrator.seed(client_id, "srwr_old", "srw_old")
            first = _refresh(http, client_id, "srwr_old")
            replay = _refresh(http, client_id, "srwr_old")
            after = _refresh(http, client_id, first.json()["refresh_token"])

        assert first.status_code == 200
        assert _invalid_grant(replay)
        assert grant["revoked"] is True
        assert _invalid_grant(after)

    def test_another_client_cannot_use_the_refresh_token(self, proxy, orchestrator):
        with _http(proxy) as http:
            owner = _register(http)
            thief = _register(http)
            orchestrator.seed(owner, "srwr_old", "srw_old")

            response = _refresh(http, thief, "srwr_old")

        assert _invalid_grant(response)

    def test_an_orchestrator_outage_is_not_an_invalid_grant(self, proxy, orchestrator):
        """invalid_grant makes the client drop the connection; an outage must not."""
        with _http(proxy, raise_server_exceptions=False) as http:
            client_id = _register(http)
            orchestrator.seed(client_id, "srwr_old", "srw_old")
            orchestrator.down = True
            response = _refresh(http, client_id, "srwr_old")

        assert response.status_code >= 500

    def test_revoke_ends_the_connection(self, proxy, orchestrator):
        with _http(proxy) as http:
            client_id = _register(http)
            grant = orchestrator.seed(client_id, "srwr_old", "srw_old")
            response = http.post(
                "/revoke",
                data={
                    "token": "srwr_old",
                    "token_type_hint": "refresh_token",
                    "client_id": client_id,
                    # The SDK's RevocationRequest requires the field even for
                    # a public client (``client_secret: str | None``, no
                    # default), so an empty one is how a public client sends it.
                    "client_secret": "",
                },
            )

        assert response.status_code == 200
        assert grant["revoked"] is True


@requires_pinned_fastmcp
class TestRefreshTokenIsNotAnAccessToken:
    @pytest.mark.asyncio
    async def test_the_proxy_does_not_accept_it_as_a_bearer(self, proxy, orchestrator):
        orchestrator.seed("c", "srwr_tok", "srw_tok")

        assert await proxy.load_access_token("srwr_tok") is None
        assert orchestrator.calls == []  # not even sent for verification


class TestVerifierOutages:
    """G6b: only a definite answer may say "no such token"."""

    @pytest.mark.asyncio
    async def test_valid_token(self, orchestrator):
        orchestrator.access[_h("srw_ok")] = {"revoked": False}

        token = await McpTokenVerifier().verify_token("srw_ok")

        assert token is not None and token.client_id == "u-1"

    @pytest.mark.asyncio
    async def test_unknown_token_is_none(self, orchestrator):
        assert await McpTokenVerifier().verify_token("srw_unknown") is None

    @pytest.mark.asyncio
    async def test_orchestrator_5xx_raises(self, orchestrator):
        orchestrator.down = True

        with pytest.raises(TokenVerificationUnavailable):
            await McpTokenVerifier().verify_token("srw_ok")

    @pytest.mark.asyncio
    async def test_transport_error_raises(self, monkeypatch):
        real_client = httpx.AsyncClient

        def broken(request):
            raise httpx.ConnectError("connection refused", request=request)

        monkeypatch.setattr(
            mcp_auth.httpx,
            "AsyncClient",
            lambda *a, **k: real_client(
                *a, **{**k, "transport": httpx.MockTransport(broken)}
            ),
        )

        with pytest.raises(TokenVerificationUnavailable):
            await McpTokenVerifier().verify_token("srw_ok")

    @pytest.mark.asyncio
    async def test_a_refresh_token_is_never_sent_for_verification(self, orchestrator):
        assert await McpTokenVerifier().verify_token("srwr_tok") is None
        assert orchestrator.calls == []

    def test_an_outage_reaches_the_client_as_a_server_error_not_401(self, orchestrator):
        from starlette.testclient import TestClient

        orchestrator.down = True
        server = fastmcp.FastMCP("verify-test", auth=McpTokenVerifier())
        with TestClient(server.http_app(), raise_server_exceptions=False) as http:
            response = http.post(
                "/mcp",
                headers={
                    "Authorization": "Bearer srw_whatever",
                    "Accept": "application/json, text/event-stream",
                },
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            )

        assert response.status_code >= 500
        assert response.status_code != 401
