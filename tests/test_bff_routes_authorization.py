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

import html
import re
from contextlib import asynccontextmanager
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
        # Rows still in srw_pre_auth_states, by state, as the callback's
        # return-path lookup sees them. A row outlives its cookie until the
        # hourly cleanup, so a lost cookie usually still has one.
        self.parked_return_to: dict[str, str] = {}
        self.lookup_fails = False

    async def create_srw_pre_auth(
        self, *, state, pkce_verifier, return_to, ttl_seconds=300
    ):
        self.created_pre_auth.append(
            {
                "state": state,
                "pkce_verifier": pkce_verifier,
                "return_to": return_to,
                "ttl_seconds": ttl_seconds,
            }
        )
        return "pre-auth-id-1"

    @asynccontextmanager
    async def acquire(self):
        store = self

        class _Conn:
            async def fetchval(self, _query, state):
                if store.lookup_fails:
                    raise ConnectionError("database unavailable")
                return store.parked_return_to.get(state)

        yield _Conn()

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


def _restart_target(r):
    """The ``/auth/login`` restart a callback answered with, parsed."""
    assert r.status_code == 302, r.text
    loc = urlparse(r.headers["location"])
    # Relative, so it lands on the origin that served the callback, which is
    # the origin /auth/login lives on in both single- and split-origin setups.
    assert (loc.scheme, loc.netloc, loc.path) == ("", "", "/auth/login")
    return parse_qs(loc.query)


def _marked_state_from_restarted_login(client, restart_url):
    """Follow a restart to /auth/login and return the state it sent to KC."""
    r = client.get(restart_url)
    assert r.status_code == 302
    return parse_qs(urlparse(r.headers["location"]).query)["state"][0]


class TestCallbackLostPreAuth:
    """A callback whose pre-auth state is gone restarts login once.

    The cookie goes missing when the Keycloak form took longer than its
    lifetime, when another tab already finished with it, or when the
    browser blocks cookies. None of these is an attack, and the Keycloak
    session is usually valid, so one trip back through /auth/login signs
    the user in. A second miss in a row stops on a readable page.
    """

    def test_missing_cookie_restarts_login_instead_of_raw_json(self, client):
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        q = _restart_target(r)
        assert q["return_to"] == ["/"]

    def test_spent_or_expired_pre_auth_restarts_login(self, client, store):
        store.pre_auth_row = None  # consume found it used or past its TTL
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        _restart_target(r)
        # The spent cookie is dropped so it cannot shadow the next flow.
        set_cookie = r.headers.get("set-cookie", "")
        assert f"{PRE_AUTH_COOKIE}=" in set_cookie and "Max-Age=0" in set_cookie

    def test_restart_keeps_the_return_path_the_login_parked(self, client, store):
        store.parked_return_to = {"s": "/sessions/abc?tab=files"}
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert _restart_target(r)["return_to"] == ["/sessions/abc?tab=files"]

    def test_a_parked_return_path_is_sanitized_again(self, client, store):
        store.parked_return_to = {"s": "https://evil.example/steal"}
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert _restart_target(r)["return_to"] == ["/"]

    def test_a_failed_return_path_lookup_still_restarts_to_root(self, client, store):
        store.parked_return_to = {"s": "/sessions/abc"}
        store.lookup_fails = True
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert _restart_target(r)["return_to"] == ["/"]

    def test_second_miss_in_a_row_shows_a_page_instead_of_redirecting(
        self, client, store
    ):
        store.parked_return_to = {"s": "/sessions/abc"}
        first = client.get("/auth/callback", params={"code": "c", "state": "s"})
        _restart_target(first)

        # The restarted login keeps the return path ...
        marked = _marked_state_from_restarted_login(client, first.headers["location"])
        assert store.created_pre_auth[-1]["return_to"] == "/sessions/abc"
        store.parked_return_to[marked] = "/sessions/abc"

        # ... and the browser comes back without the cookie again
        # (cookies blocked for this site).
        client.cookies.clear()
        second = client.get("/auth/callback", params={"code": "c2", "state": marked})

        assert second.status_code == 400
        assert "location" not in second.headers
        assert second.headers["content-type"].startswith("text/html")
        # A link lets the user start over by hand; it is a fresh login, so a
        # deliberate retry gets the one automatic restart again.
        hrefs = [html.unescape(h) for h in re.findall(r'href="([^"]+)"', second.text)]
        assert len(hrefs) == 1
        link = urlparse(hrefs[0])
        assert link.path == "/auth/login"
        assert parse_qs(link.query) == {"return_to": ["/sessions/abc"]}

    def test_a_restarted_login_that_keeps_its_cookie_proceeds(
        self, client, store, monkeypatch
    ):
        first = client.get("/auth/callback", params={"code": "c", "state": "s"})
        marked = _marked_state_from_restarted_login(client, first.headers["location"])
        store.pre_auth_row = {"state": marked, "pkce_verifier": "v", "return_to": "/"}
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        exchange = AsyncMock(side_effect=bff.KeycloakClientError("stop here"))
        monkeypatch.setattr(bff.kc_bff_client, "exchange_code", exchange, raising=False)

        r = client.get("/auth/callback", params={"code": "c2", "state": marked})

        # Past the state check and on to the code exchange with its verifier.
        exchange.assert_awaited_once_with("c2", "v")
        assert r.status_code == 502

    def test_a_restarted_login_with_a_mismatched_state_is_still_refused(
        self, client, store
    ):
        """The restart marker never short-circuits the CSRF check."""
        first = client.get("/auth/callback", params={"code": "c", "state": "s"})
        marked = _marked_state_from_restarted_login(client, first.headers["location"])
        store.pre_auth_row = {
            "state": "the-real-state",
            "pkce_verifier": "v",
            "return_to": "/",
        }
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        r = client.get("/auth/callback", params={"code": "c2", "state": marked})
        assert r.status_code == 400
        assert r.json()["detail"] == "State mismatch"


class TestPreAuthLifetime:
    def test_cookie_and_row_share_a_lifetime_that_outlasts_a_slow_login(
        self, client, store
    ):
        """The cookie and its row expire together, after at least 10 minutes.

        If the row expired first, the cookie would arrive pointing at a dead
        row; if the cookie expired first, a live row would be unreachable.
        Ten minutes covers looking up a password or a first login with
        registration and email verification.
        """
        r = client.get("/auth/login")
        max_age = int(re.search(r"Max-Age=(\d+)", r.headers["set-cookie"]).group(1))
        assert max_age == store.created_pre_auth[-1]["ttl_seconds"]
        assert max_age >= 600


class TestCallbackSubjectCheck:
    """A code exchange whose tokens carry no usable subject opens no session."""

    @pytest.fixture
    def resolve(self, client, store, monkeypatch):
        store.pre_auth_row = {"state": "s", "pkce_verifier": "v", "return_to": "/"}
        client.cookies.set(PRE_AUTH_COOKIE, "pre-auth-id-1")
        monkeypatch.setattr(
            bff.kc_bff_client,
            "exchange_code",
            AsyncMock(
                return_value={
                    "access_token": "at",
                    "refresh_token": "rt",
                    "id_token": "it",
                    "expires_in": 300,
                }
            ),
            raising=False,
        )
        resolve = AsyncMock(side_effect=AssertionError("must not provision a user"))
        monkeypatch.setattr(bff, "_resolve_user_from_claims", resolve)
        return resolve

    @pytest.mark.parametrize(
        "bad_sub",
        ["__absent__", None, "", "   ", 123],
        ids=["absent", "none", "empty", "blank", "int"],
    )
    def test_unusable_subject_is_refused_before_provisioning(
        self, client, resolve, monkeypatch, bad_sub
    ):
        claims = {"realm_access": {"roles": ["user"]}}
        if bad_sub != "__absent__":
            claims["sub"] = bad_sub
        monkeypatch.setattr(
            bff,
            "oidc_validator",
            SimpleNamespace(
                validate_token=lambda _t: claims, decode_id_token=lambda _t: {}
            ),
        )
        r = client.get("/auth/callback", params={"code": "c", "state": "s"})
        assert r.status_code == 502
        assert r.json()["detail"] == "Authentication backend error"
        resolve.assert_not_awaited()


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
