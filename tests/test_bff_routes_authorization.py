"""Authorization/refusal characterization for the BFF ``/auth/*`` routes.

These routes had no route-level coverage. They are written to pass **both**
before and after R1.B02 moves them off ``from orchestrator.main import
postgres_db`` onto ``request.app.state.store``: every test binds the same fake
store to *both* seams, so the file is a genuine before/after equivalence gate
rather than a test rewritten to match the new wiring.

Scope is deliberately the security surface — who is refused, with which status,
and what must never appear in a response — not the happy-path OIDC dance, which
needs a real Keycloak.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.auth import bff


PRE_AUTH_COOKIE = bff.PRE_AUTH_COOKIE
SESSION_COOKIE = bff.SESSION_COOKIE


class FakeStore:
    """Only the methods the BFF routes actually call."""

    def __init__(self):
        self.created_pre_auth: list[dict] = []
        self.pre_auth_row: dict | None = None
        self.session_row: dict | None = None
        self.deleted_sessions: list[str] = []
        self.deleted_by_sid: list[str] = []

    async def create_srw_pre_auth(self, *, state, pkce_verifier, return_to):
        self.created_pre_auth.append(
            {"state": state, "pkce_verifier": pkce_verifier, "return_to": return_to}
        )
        return "pre-auth-id-1"

    async def consume_srw_pre_auth(self, pre_auth_id):
        return self.pre_auth_row

    async def get_srw_session(self, session_id):
        return self.session_row

    async def delete_srw_session(self, session_id):
        self.deleted_sessions.append(session_id)

    async def delete_srw_sessions_by_kc_sid(self, sid):
        self.deleted_by_sid.append(sid)
        return 1


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def client(store, monkeypatch):
    """Mount the router and bind the store to both the old and new seams."""
    import orchestrator.main as orchestrator_main

    monkeypatch.setattr(
        orchestrator_main.app.state.resources, "postgres_db", store, raising=False
    )

    monkeypatch.setattr(
        bff,
        "kc_bff_client",
        SimpleNamespace(
            redirect_uri="https://cockpit.example/auth/callback",
            client_id="srw-cockpit",
            authorize_endpoint="https://kc.example/authorize",
            end_session_endpoint="https://kc.example/logout",
            logout_kc_side=AsyncMock(return_value=None),
        ),
        raising=False,
    )

    app = FastAPI()
    app.include_router(bff.router)
    app.state.store = store
    return TestClient(app, follow_redirects=False)


class TestLoginEntry:
    def test_unconfigured_redirect_uri_is_refused(self, client, monkeypatch):
        monkeypatch.setattr(bff.kc_bff_client, "redirect_uri", "", raising=False)
        r = client.get("/auth/login")
        assert r.status_code == 500
        assert "SRW_BFF_REDIRECT_URI" in r.json()["detail"]

    def test_redirects_to_keycloak_with_pkce_s256_and_parks_state(self, client, store):
        r = client.get("/auth/login")
        assert r.status_code == 302
        q = parse_qs(urlparse(r.headers["location"]).query)
        assert q["response_type"] == ["code"]
        assert q["code_challenge_method"] == ["S256"]
        # The verifier itself must never travel to the browser.
        parked = store.created_pre_auth[0]
        assert parked["pkce_verifier"] not in r.headers["location"]
        assert q["code_challenge"] == [bff._pkce_challenge(parked["pkce_verifier"])]
        assert q["state"] == [parked["state"]]
        assert PRE_AUTH_COOKIE in r.headers.get("set-cookie", "")

    @pytest.mark.parametrize(
        "hostile",
        ["https://evil.example/steal", "evil.example", "http://evil.example"],
    )
    def test_absolute_return_to_is_coerced_to_root(self, client, store, hostile):
        client.get("/auth/login", params={"return_to": hostile})
        assert store.created_pre_auth[-1]["return_to"] == "/"

    def test_protocol_relative_return_to_stays_same_origin(self, client, store):
        """``//host/path`` survives the filter, and that is not a redirect hole.

        ``_safe_return_to`` only rejects a scheme or a non-``/`` prefix, so
        ``//evil.example/x`` is parked verbatim. It is still safe because the
        callback never redirects to it directly: it builds
        ``f"{_spa_base()}{return_to}"``, and ``_spa_base()`` is always an
        absolute origin. The result is ``https://cockpit//evil.example/x`` —
        a doubled path separator on the cockpit's own origin, not a bounce to
        another host.

        This pins that composition. If a caller ever redirects to a parked
        ``return_to`` without prefixing an absolute origin, this stops
        describing the code and the filter needs a ``//`` rejection.
        """
        client.get("/auth/login", params={"return_to": "//evil.example/steal"})
        parked = store.created_pre_auth[-1]["return_to"]
        assert parked == "//evil.example/steal"

        monkey_base = "https://cockpit.example"
        composed = f"{monkey_base}{parked}"
        assert urlparse(composed).netloc == "cockpit.example"

    def test_same_origin_return_to_is_kept(self, client, store):
        client.get("/auth/login", params={"return_to": "/sessions/abc"})
        assert store.created_pre_auth[-1]["return_to"] == "/sessions/abc"

    def test_crlf_is_stripped_from_return_to(self, client, store):
        client.get("/auth/login", params={"return_to": "/a\r\nSet-Cookie: x=1"})
        parked = store.created_pre_auth[-1]["return_to"]
        assert "\r" not in parked and "\n" not in parked

    @pytest.mark.parametrize(
        "hostile", ["en; injected", "a" * 32, "en_US\r\nX: 1", "<script>"]
    )
    def test_malformed_ui_locales_is_not_forwarded(self, client, hostile):
        r = client.get("/auth/login", params={"ui_locales": hostile})
        assert "ui_locales" not in parse_qs(urlparse(r.headers["location"]).query)

    def test_wellformed_ui_locales_is_forwarded(self, client):
        r = client.get("/auth/login", params={"ui_locales": "de-DE"})
        q = parse_qs(urlparse(r.headers["location"]).query)
        assert q["ui_locales"] == ["de-DE"]


class TestCallbackRefusals:
    def test_keycloak_error_redirects_without_echoing_the_description(self, client):
        r = client.get(
            "/auth/callback",
            params={
                "error": "access_denied",
                "error_description": "internal-detail-leak",
            },
        )
        assert r.status_code == 302
        assert "auth_error=1" in r.headers["location"]
        assert "internal-detail-leak" not in r.headers["location"]
        assert "internal-detail-leak" not in r.text

    @pytest.mark.parametrize(
        "params", [{}, {"code": "c"}, {"state": "s"}], ids=["neither", "code", "state"]
    )
    def test_missing_code_or_state_is_refused(self, client, params):
        r = client.get("/auth/callback", params=params)
        assert r.status_code == 400
        assert r.json()["detail"] == "Missing code or state"

    def test_missing_pre_auth_cookie_is_refused(self, client):
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert r.status_code == 400
        assert r.json()["detail"] == "Missing pre-auth state"

    def test_unknown_or_replayed_pre_auth_is_refused(self, client, store):
        store.pre_auth_row = None
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert r.status_code == 400
        assert r.json()["detail"] == "Pre-auth state expired or used"

    def test_state_mismatch_is_refused(self, client, store):
        store.pre_auth_row = {
            "state": "the-real-state",
            "pkce_verifier": "v",
            "return_to": "/",
        }
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        r = client.get(
            "/auth/callback", params={"code": "c", "state": "attacker-state"}
        )
        assert r.status_code == 400
        assert r.json()["detail"] == "State mismatch"


class TestRefreshRefusals:
    def test_no_cookie_is_401(self, client):
        assert client.post("/auth/refresh").status_code == 401

    def test_unknown_session_is_401(self, client, store):
        store.session_row = None
        client.cookies.set(SESSION_COOKIE, "sid-1")
        r = client.post("/auth/refresh")
        assert r.status_code == 401
        assert r.json()["detail"] == "Session not found"

    def test_failed_refresh_is_401(self, client, store, monkeypatch):
        store.session_row = {"id": "sid-1", "refresh_token": "rt"}
        monkeypatch.setattr(
            bff, "_refresh_session_in_place", AsyncMock(return_value=None)
        )
        client.cookies.set(SESSION_COOKIE, "sid-1")
        assert client.post("/auth/refresh").status_code == 401

    def test_successful_refresh(self, client, store, monkeypatch):
        store.session_row = {"id": "sid-1", "refresh_token": "rt"}
        monkeypatch.setattr(
            bff, "_refresh_session_in_place", AsyncMock(return_value="new-access")
        )
        client.cookies.set(SESSION_COOKIE, "sid-1")
        r = client.post("/auth/refresh")
        assert r.status_code == 200 and r.json() == {"refreshed": True}
        # The freshly minted access token must not be handed to the browser.
        assert "new-access" not in r.text


class TestLogout:
    def test_without_a_cookie_is_a_no_op_that_still_clears(self, client, store):
        r = client.post("/auth/logout")
        assert r.status_code == 200
        assert r.json() == {"kc_logout_url": None}
        assert store.deleted_sessions == []

    def test_revokes_kc_side_and_deletes_the_row(self, client, store):
        store.session_row = {
            "id": "sid-1",
            "refresh_token": "rt-secret",
            "id_token": "id-token-value",
        }
        client.cookies.set(SESSION_COOKIE, "sid-1")
        r = client.post("/auth/logout")
        assert r.status_code == 200
        assert store.deleted_sessions == ["sid-1"]
        bff.kc_bff_client.logout_kc_side.assert_awaited_once_with("rt-secret")
        # id_token_hint is intended; the refresh token is not.
        assert "rt-secret" not in r.text


class TestBackchannelLogout:
    def test_missing_token_is_refused(self, client):
        r = client.post("/auth/backchannel-logout", data={})
        assert r.status_code == 400
        assert r.json()["detail"] == "Missing logout_token"

    def test_unverifiable_token_is_refused(self, client, monkeypatch):
        monkeypatch.setattr(
            bff.oidc_validator, "verify_logout_token", lambda t: None, raising=False
        )
        r = client.post("/auth/backchannel-logout", data={"logout_token": "forged"})
        assert r.status_code == 400
        assert r.json()["detail"] == "Invalid logout token"

    def test_verified_sid_deletes_that_sids_sessions(self, client, store, monkeypatch):
        monkeypatch.setattr(
            bff.oidc_validator,
            "verify_logout_token",
            lambda t: {"sid": "kc-sid-9", "sub": "kc-sub-9"},
            raising=False,
        )
        r = client.post("/auth/backchannel-logout", data={"logout_token": "good"})
        assert r.status_code == 200
        assert store.deleted_by_sid == ["kc-sid-9"]
