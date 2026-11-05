"""The MCP authorization server only redirects codes to allowlisted URIs (G7).

Without ``allowed_client_redirect_uris`` FastMCP accepts any safe redirect
URI, and it synthesizes a client for anyone presenting our upstream client id
(``mcp-server``, public in the chart values) that may redirect anywhere. One
click on the consent page by a user with a Keycloak session then hands the
authorization code to an attacker. Design:
knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md §4.1, §5.9.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import fastmcp
import pytest
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration, OIDCProxy
from mcp.server.auth.provider import RegistrationError
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull
from pydantic import AnyUrl

from mcp_server.auth import McpTokenVerifier
from mcp_server.oauth_bridge import (
    DEFAULT_ALLOWED_REDIRECT_URIS,
    SRWOAuthProxy,
    allowed_redirect_uris_from_env,
)

UPSTREAM_CLIENT_ID = "mcp-server"
CLAUDE_CALLBACK = "https://claude.ai/api/mcp/auth_callback"
ATTACKER_CALLBACK = "https://attacker.example/steal"


def _oidc_config() -> OIDCConfiguration:
    return OIDCConfiguration.model_validate(
        {
            "issuer": "https://auth.example/realms/srw",
            "authorization_endpoint": "https://auth.example/realms/srw/auth",
            "token_endpoint": "https://auth.example/realms/srw/token",
            "jwks_uri": "https://auth.example/realms/srw/certs",
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        }
    )


@pytest.fixture
def proxy(tmp_path, monkeypatch):
    monkeypatch.setattr(fastmcp.settings, "home", tmp_path)
    monkeypatch.delenv("MCP_OAUTH_ALLOWED_REDIRECT_URIS", raising=False)
    monkeypatch.delenv("MCP_OAUTH_EXTRA_REDIRECT_URIS", raising=False)
    with patch.object(OIDCProxy, "get_oidc_configuration", return_value=_oidc_config()):
        return SRWOAuthProxy(
            config_url="https://auth.example/realms/srw/.well-known/openid-configuration",
            client_id=UPSTREAM_CLIENT_ID,
            client_secret="test-secret",
            base_url="https://mcp.example",
            mcp_verifier=McpTokenVerifier(),
            verify_id_token=True,
        )


class TestAllowlistFromEnv:
    def test_defaults_when_unset(self):
        assert allowed_redirect_uris_from_env({}) == list(DEFAULT_ALLOWED_REDIRECT_URIS)

    def test_override_replaces_defaults(self):
        env = {
            "MCP_OAUTH_ALLOWED_REDIRECT_URIS": "https://a.example/cb, http://localhost"
        }

        assert allowed_redirect_uris_from_env(env) == [
            "https://a.example/cb",
            "http://localhost",
        ]

    def test_blank_override_keeps_defaults(self):
        env = {"MCP_OAUTH_ALLOWED_REDIRECT_URIS": "  ,  "}

        assert allowed_redirect_uris_from_env(env) == list(
            DEFAULT_ALLOWED_REDIRECT_URIS
        )

    def test_extra_is_appended_once(self):
        env = {
            "MCP_OAUTH_EXTRA_REDIRECT_URIS": (
                "https://chat.example/oauth/callback\n" + CLAUDE_CALLBACK
            )
        }

        patterns = allowed_redirect_uris_from_env(env)

        assert patterns[-1] == "https://chat.example/oauth/callback"
        assert patterns.count(CLAUDE_CALLBACK) == 1

    def test_pattern_without_scheme_is_dropped(self, caplog):
        env = {"MCP_OAUTH_EXTRA_REDIRECT_URIS": "attacker.example"}

        with caplog.at_level(logging.WARNING, logger="mcp_server.oauth_bridge"):
            patterns = allowed_redirect_uris_from_env(env)

        assert "attacker.example" not in patterns
        assert "without a scheme" in caplog.text


async def _register(proxy, client_id: str, redirect: str):
    await proxy.register_client(
        OAuthClientInformationFull(
            client_id=client_id,
            client_name="Claude",
            redirect_uris=[AnyUrl(redirect)],
        )
    )
    return await proxy.get_client(client_id)


class TestProxyAllowlist:
    def test_proxy_is_never_built_without_a_list(self, proxy):
        assert proxy._allowed_client_redirect_uris == list(
            DEFAULT_ALLOWED_REDIRECT_URIS
        )

    @pytest.mark.asyncio
    async def test_upstream_client_id_cannot_redirect_to_a_foreign_host(self, proxy):
        # FastMCP 3.4 synthesizes a client for the upstream client id; older
        # releases return none, which leaves nothing to redirect through.
        client = await proxy.get_client(UPSTREAM_CLIENT_ID)

        if client is not None:
            with pytest.raises(InvalidRedirectUriError):
                client.validate_redirect_uri(AnyUrl(ATTACKER_CALLBACK))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "redirect",
        [
            CLAUDE_CALLBACK,
            "https://claude.com/api/mcp/auth_callback",
            "https://chatgpt.com/connector_platform_oauth_redirect",
            "https://chatgpt.com/connector/oauth/abc123",
            "http://localhost:33418/callback",
            "http://127.0.0.1:8787/callback",
            "http://[::1]:5000/oauth/callback",
        ],
    )
    async def test_documented_client_callbacks_still_pass(self, proxy, redirect):
        client = await _register(proxy, "dcr-client", redirect)

        assert str(client.validate_redirect_uri(AnyUrl(redirect))) == redirect

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "redirect",
        [
            "https://claude.ai.attacker.example/api/mcp/auth_callback",
            "https://claude.ai/other/path",
            "http://localhost@attacker.example/cb",
            "https://chatgpt.com/connector/oauth/../../steal",
            ATTACKER_CALLBACK,
        ],
    )
    async def test_lookalike_redirects_are_refused(self, proxy, redirect):
        client = await _register(proxy, "dcr-claude", CLAUDE_CALLBACK)

        with pytest.raises(InvalidRedirectUriError):
            client.validate_redirect_uri(AnyUrl(redirect))

    @pytest.mark.asyncio
    async def test_registration_with_a_foreign_redirect_is_refused_and_logged(
        self, proxy, caplog
    ):
        info = OAuthClientInformationFull(
            client_id="dcr-attacker",
            client_name="Totally Claude",
            redirect_uris=[AnyUrl(ATTACKER_CALLBACK)],
        )

        with caplog.at_level(logging.WARNING, logger="mcp_server.oauth_bridge"):
            with pytest.raises(RegistrationError):
                await proxy.register_client(info)

        assert "Refused client registration" in caplog.text
        assert await proxy.get_client("dcr-attacker") is None


class TestAuthorizeEndpoint:
    """End to end over HTTP: /register and /authorize refuse foreign redirects."""

    PKCE = {
        "code_challenge": "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        "code_challenge_method": "S256",
    }

    def _client(self, proxy):
        from starlette.testclient import TestClient

        server = fastmcp.FastMCP("allowlist-test", auth=proxy)
        return TestClient(server.http_app(), base_url="https://mcp.example")

    def _authorize(self, http, client_id: str, redirect_uri: str):
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "state": "s1",
            "resource": "https://mcp.example/mcp",
            **self.PKCE,
        }
        return http.get("/authorize", params=params, follow_redirects=False)

    def test_upstream_client_id_cannot_send_a_code_abroad(self, proxy):
        with self._client(proxy) as http:
            response = self._authorize(http, UPSTREAM_CLIENT_ID, ATTACKER_CALLBACK)

        location = response.headers.get("location", "")
        assert "auth.example" not in location
        assert not location.startswith(ATTACKER_CALLBACK)
        assert response.status_code == 400

    def test_registration_endpoint_refuses_a_foreign_redirect(self, proxy):
        with self._client(proxy) as http:
            response = http.post(
                "/register",
                json={"client_name": "x", "redirect_uris": [ATTACKER_CALLBACK]},
            )

        assert response.status_code == 400

    def test_registered_client_proceeds_to_the_identity_provider(self, proxy):
        with self._client(proxy) as http:
            registered = http.post(
                "/register",
                json={
                    "client_name": "Claude",
                    "redirect_uris": [CLAUDE_CALLBACK],
                    "token_endpoint_auth_method": "none",
                },
            )
            assert registered.status_code == 201, registered.text
            client_id = registered.json()["client_id"]

            allowed = self._authorize(http, client_id, CLAUDE_CALLBACK)
            foreign = self._authorize(http, client_id, ATTACKER_CALLBACK)

        assert allowed.status_code in (302, 303, 307)
        assert allowed.headers["location"].startswith(
            "https://auth.example/realms/srw/auth"
        )
        assert foreign.status_code == 400
