"""The consent page says who is asking and where the code goes (G13).

Keycloak SSO makes the upstream login silent, so this page is the only
consent gate. It showed the app's self-chosen DCR name ("Claude" is free to
register) and nothing about the redirect, could be framed, and accepted an
approval from any browser that had the form fields.
Design: knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md G13.
"""

from __future__ import annotations

from unittest.mock import patch

import fastmcp
import pytest
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration, OIDCProxy

from mcp_server.auth import McpTokenVerifier
from mcp_server.oauth_bridge import (
    CONSENT_HEADERS,
    PendingAuth,
    SRWOAuthProxy,
    _render_consent,
    describe_client,
)

requires_pinned_fastmcp = pytest.mark.skipif(
    tuple(int(part) for part in fastmcp.__version__.split(".")[:2]) < (3, 4),
    reason="needs fastmcp>=3.4 (pip install -r src/mcp_server/requirements.txt)",
)

CLAUDE_CALLBACK = "https://claude.ai/api/mcp/auth_callback"


class TestDescribeClient:
    def test_a_cimd_client_is_named_by_its_verified_host(self):
        client = describe_client(
            "https://claude.ai/oauth/mcp-client-metadata",
            "Totally Not Claude",
            CLAUDE_CALLBACK,
        )

        assert client["display_name"] == "claude.ai"
        assert client["verified"] is True
        assert client["redirect_host"] == "claude.ai"
        assert client["loopback"] is False

    def test_a_dcr_name_is_shown_as_the_apps_own_claim(self):
        client = describe_client("3f1c-dcr-uuid", "Claude", CLAUDE_CALLBACK)

        assert client["display_name"] == "Claude"
        assert client["verified"] is False

    def test_a_loopback_redirect_is_flagged_with_its_port(self):
        client = describe_client(
            "dcr", "Claude Code", "http://127.0.0.1:33418/callback"
        )

        assert client["loopback"] is True
        assert client["redirect_host"] == "127.0.0.1:33418"

    def test_no_name_is_unknown_app(self):
        assert (
            describe_client("dcr", None, CLAUDE_CALLBACK)["display_name"]
            == "Unknown App"
        )


def _page(**overrides) -> str:
    values = dict(
        session_id="s",
        csrf_token="c",
        client_name="Claude",
        user_email="u@example.test",
        user_roles=[],
        is_admin=False,
    )
    values.update(overrides)
    return _render_consent(**values)


class TestConsentPage:
    def test_the_destination_host_is_shown(self):
        assert "claude.ai" in _page(redirect_host="claude.ai")

    def test_an_unverified_name_carries_a_warning(self):
        page = _page(client_verified=False)

        assert "chosen by the app itself" in page
        assert "verified by its web address" not in page

    def test_a_verified_host_says_so(self):
        page = _page(client_verified=True)

        assert "verified by its web address" in page
        assert "chosen by the app itself" not in page

    def test_a_loopback_redirect_warns(self):
        assert "localhost" in _page(loopback=True)
        assert "this computer" not in _page(loopback=False)

    def test_the_destination_is_escaped(self):
        page = _page(redirect_host='"><img src=x>')

        assert "<img" not in page

    def test_every_placeholder_is_filled(self):
        assert "{{" not in _page(client_verified=True, loopback=True, redirect_host="x")


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
    with patch.object(OIDCProxy, "get_oidc_configuration", return_value=_oidc_config()):
        return SRWOAuthProxy(
            config_url="https://auth.example/realms/srw/.well-known/openid-configuration",
            client_id="mcp-server",
            client_secret="test-secret",
            base_url="https://mcp.example",
            mcp_verifier=McpTokenVerifier(),
            verify_id_token=True,
        )


@requires_pinned_fastmcp
class TestConsentResponse:
    def test_the_page_cannot_be_framed_or_cached(self, proxy):
        response = proxy._consent_response("<html></html>", "binding-1")

        for header, value in CONSENT_HEADERS.items():
            assert response.headers[header] == value
        assert response.headers["content-security-policy"] == "frame-ancestors 'none'"

    def test_the_browser_binding_cookie_is_host_only_and_hidden_from_scripts(
        self, proxy
    ):
        cookie = proxy._consent_response("<html></html>", "binding-1").headers[
            "set-cookie"
        ]

        assert cookie.startswith("__Host-srw_consent=binding-1")
        for attribute in ("HttpOnly", "Secure", "SameSite=lax", "Path=/"):
            assert attribute.lower() in cookie.lower()
        assert "domain=" not in cookie.lower()


@requires_pinned_fastmcp
class TestScopeSelectNeedsTheBrowserThatSawThePage:
    def _pending(self, proxy) -> str:
        session_id = "session-1"
        proxy._pending_auths[session_id] = PendingAuth(
            transaction={
                "client_id": "dcr-1",
                "client_redirect_uri": CLAUDE_CALLBACK,
                "client_state": "st",
                "code_challenge": "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
                "code_challenge_method": "S256",
            },
            idp_tokens={},
            user_claims={"sub": "kc-1", "email": "u@example.test"},
            csrf_token="csrf-1",
            browser_binding="binding-1",
        )
        return session_id

    def _submit(self, proxy, cookie: str | None):
        from starlette.testclient import TestClient

        server = fastmcp.FastMCP("consent-test", auth=proxy)
        headers = {"Cookie": f"__Host-srw_consent={cookie}"} if cookie else {}
        with TestClient(server.http_app(), base_url="https://mcp.example") as http:
            return http.post(
                "/oauth/scope-select",
                data={
                    "session_id": self._pending(proxy),
                    "csrf_token": "csrf-1",
                    "action": "approve",
                    "scope": "user",
                    "expiry_days": "30",
                },
                headers=headers,
                follow_redirects=False,
            )

    def test_without_the_cookie_the_approval_is_refused(self, proxy):
        response = self._submit(proxy, None)

        assert response.status_code == 400
        assert "did not come from the browser" in response.text

    def test_with_another_browsers_cookie_the_approval_is_refused(self, proxy):
        assert self._submit(proxy, "someone-else").status_code == 400

    def test_with_its_cookie_the_code_goes_to_the_client(self, proxy):
        response = self._submit(proxy, "binding-1")

        assert response.status_code == 302
        assert response.headers["location"].startswith(CLAUDE_CALLBACK + "?code=")
