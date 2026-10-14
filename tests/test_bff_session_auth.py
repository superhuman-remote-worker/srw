"""Tests for the BFF cookie-session auth path (`_resolve_from_cookie`).

These lock in the idle-renewal behavior: a session idle past
``SRW_SESSION_IDLE_TIMEOUT_S`` is re-validated against Keycloak (refreshed in
place) rather than blind-deleted, so an idle-but-still-valid session survives
instead of force-logging the user out. The absolute lifetime cap stays a hard
stop, and a definitive KC rejection (400 ``invalid_grant``) still deletes the
row and returns None.

They also lock in the outage posture: when Keycloak is merely unreachable or
broken (network error, 5xx, ``invalid_client`` misconfig, malformed body),
the session row is KEPT and the request fails with a retryable 503 — a KC
outage must not permanently log every user out (HA checklist P0 in
knowledge-base/knowledge/features/high_availability_setup.md).

Mirrors the AsyncMock + patch.object style of ``tests/test_mcp.py``. We call
``_resolve_from_cookie`` directly (it takes ``(session_id, db)``), patch
``_resolve_user_from_claims`` to a sentinel so the tests stay focused on the
cookie/refresh logic, and pin the idle/skew env so timing is deterministic
regardless of ambient ``.env``.
"""

import asyncio
import os
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, UTC
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.security.auth import _resolve_from_cookie, resolve_ws_user
from orchestrator.security.kc_client import KeycloakBFFClient, KeycloakClientError

SESSION_ID = "sess-123"
SENTINEL_USER = {"id": "user-1", "display_name": "Tester", "is_approved": True}

# Refresh response shape from kc_bff_client.refresh (see _refresh_session_in_place).
_REFRESHED = {
    "access_token": "refreshed.access.jwt",
    "refresh_token": "refreshed-refresh-token",
    "id_token": "refreshed.id.jwt",
    "expires_in": 600,
}


def _session(**overrides):
    """A valid srw_session row (timezone-aware datetimes), overridable per-test."""
    now = datetime.now(UTC)
    sess = {
        "id": SESSION_ID,
        "user_id": uuid4(),
        "kc_sub": "kc-sub-abc",
        "kc_sid": "kc-sid-xyz",
        "access_token": "stored.access.jwt",
        "refresh_token": "stored-refresh-token",
        "id_token": "stored.id.jwt",
        "access_expires_at": now + timedelta(seconds=600),
        "absolute_expires_at": now + timedelta(days=30),
        "created_at": now - timedelta(minutes=5),
        "last_seen_at": now,
        "user_agent": "pytest",
        "created_ip": "127.0.0.1",
    }
    sess.update(overrides)
    return sess


def _make_db(sess):
    db = AsyncMock()
    db.get_srw_session = AsyncMock(return_value=sess)
    db.delete_srw_session = AsyncMock()
    db.refresh_srw_session_tokens = AsyncMock()
    db.touch_srw_session_last_seen = AsyncMock()
    return db


_DEFAULT_CLAIMS = {"sub": "kc-sub-abc", "realm_access": {"roles": ["user"]}}
_DEFAULT_ID_CLAIMS = {
    "sub": "kc-sub-abc",
    "email": "u@t.com",
    "preferred_username": "tester",
}
_UNSET = object()


@contextmanager
def _auth_patches(
    *,
    validate_token=_UNSET,
    decode_id_token=_UNSET,
    refresh=_UNSET,
    refresh_side_effect=None,
):
    """Patch the auth module's KC/OIDC seams and pin the idle/skew env.

    Yields the mocks so tests can assert on calls. ``validate_token=None``
    means "stored token fails validation"; ``refresh_side_effect`` raises from
    kc_bff_client.refresh (the dead-KC case).
    """
    claims = _DEFAULT_CLAIMS if validate_token is _UNSET else validate_token
    id_claims = _DEFAULT_ID_CLAIMS if decode_id_token is _UNSET else decode_id_token
    refresh_payload = dict(_REFRESHED) if refresh is _UNSET else refresh

    refresh_mock = AsyncMock()
    if refresh_side_effect is not None:
        refresh_mock.side_effect = refresh_side_effect
    else:
        refresh_mock.return_value = refresh_payload

    with ExitStack() as stack:
        stack.enter_context(
            patch.dict(
                os.environ,
                {
                    "SRW_SESSION_IDLE_TIMEOUT_S": "1800",
                    "SRW_ACCESS_TOKEN_REFRESH_SKEW_S": "60",
                },
            )
        )
        m_validate = stack.enter_context(
            patch(
                "orchestrator.security.auth.oidc_validator.validate_token",
                return_value=claims,
            )
        )
        m_decode = stack.enter_context(
            patch(
                "orchestrator.security.auth.oidc_validator.decode_id_token",
                return_value=id_claims,
            )
        )
        stack.enter_context(
            patch("orchestrator.security.auth.kc_bff_client.refresh", refresh_mock)
        )
        m_resolve = stack.enter_context(
            patch(
                "orchestrator.security.auth._resolve_user_from_claims",
                AsyncMock(return_value=dict(SENTINEL_USER)),
            )
        )
        yield {
            "validate_token": m_validate,
            "decode_id_token": m_decode,
            "refresh": refresh_mock,
            "resolve_user": m_resolve,
        }


class TestResolveFromCookie:
    """Idle-renewal behavior + the unchanged guard paths."""

    @pytest.mark.asyncio
    async def test_idle_with_live_kc_renews_silently(self):
        """Idle past the window but KC alive → refresh in place, no delete."""
        now = datetime.now(UTC)
        sess = _session(
            last_seen_at=now - timedelta(hours=1),  # well past 1800s idle
            access_expires_at=now + timedelta(seconds=600),  # not near-expiry
        )
        db = _make_db(sess)

        with _auth_patches() as m:
            result = await _resolve_from_cookie(SESSION_ID, db)
            await asyncio.sleep(0)  # let the fire-and-forget touch task run

        assert result == SENTINEL_USER
        m["refresh"].assert_awaited_once_with("stored-refresh-token")
        db.refresh_srw_session_tokens.assert_awaited_once()
        db.delete_srw_session.assert_not_called()
        db.touch_srw_session_last_seen.assert_awaited_once_with(SESSION_ID)

    @pytest.mark.asyncio
    async def test_idle_with_kc_rejection_deletes_and_returns_none(self):
        """Idle + KC definitively rejects the refresh → row deleted, None."""
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)

        with _auth_patches(
            refresh_side_effect=KeycloakClientError(
                "KC token endpoint 400 (invalid_grant)",
                status_code=400,
                oauth_error="invalid_grant",
            )
        ) as m:
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_awaited_once_with(SESSION_ID)
        m["validate_token"].assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status_code", "oauth_error"),
        [
            (None, None),  # unreachable: connect/timeout/TLS
            (502, "non_json_response"),  # ingress 502 during a KC rollout
            (500, "unknown_error"),  # KC internal error
            (401, "invalid_client"),  # orchestrator misconfig, not a dead session
            (400, "unsupported_grant_type"),  # ditto
        ],
    )
    async def test_kc_outage_keeps_session_and_raises_503(
        self, status_code, oauth_error
    ):
        """KC unreachable/broken → row KEPT, 503 raised, no token write.

        This is the P0 fix: before, any KeycloakClientError deleted the row,
        so a >15-min KC outage force-re-logged-in every user permanently.
        """
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)

        with _auth_patches(
            refresh_side_effect=KeycloakClientError(
                f"KC token endpoint {status_code} ({oauth_error})",
                status_code=status_code,
                oauth_error=oauth_error,
            )
        ) as m:
            with pytest.raises(HTTPException) as exc_info:
                await _resolve_from_cookie(SESSION_ID, db)

        assert exc_info.value.status_code == 503
        db.delete_srw_session.assert_not_called()
        db.refresh_srw_session_tokens.assert_not_called()
        m["validate_token"].assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_refresh_payload_keeps_session_and_raises_503(self):
        """KC answers 200 with an unusable body → server breakage, keep + 503."""
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)

        with _auth_patches(refresh={}):
            with pytest.raises(HTTPException) as exc_info:
                await _resolve_from_cookie(SESSION_ID, db)

        assert exc_info.value.status_code == 503
        db.delete_srw_session.assert_not_called()
        db.refresh_srw_session_tokens.assert_not_called()

    @pytest.mark.asyncio
    async def test_absolute_expiry_hard_stops_without_refresh(self):
        """Past absolute lifetime → delete, no refresh — even if also idle."""
        now = datetime.now(UTC)
        sess = _session(
            absolute_expires_at=now - timedelta(seconds=1),
            last_seen_at=now - timedelta(hours=1),  # adversarial: absolute must win
        )
        db = _make_db(sess)

        with _auth_patches() as m:
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_awaited_once_with(SESSION_ID)
        m["refresh"].assert_not_called()

    @pytest.mark.asyncio
    async def test_near_expiry_not_idle_still_refreshes(self):
        """Access token within the refresh skew (not idle) → refresh, no delete."""
        now = datetime.now(UTC)
        sess = _session(
            last_seen_at=now,  # not idle
            access_expires_at=now + timedelta(seconds=30),  # within 60s skew
        )
        db = _make_db(sess)

        with _auth_patches() as m:
            result = await _resolve_from_cookie(SESSION_ID, db)
            await asyncio.sleep(0)

        assert result == SENTINEL_USER
        m["refresh"].assert_awaited_once_with("stored-refresh-token")
        db.delete_srw_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_fully_valid_session_no_refresh_no_delete(self):
        """Fresh + far from expiry → single read + touch, no KC round-trip."""
        now = datetime.now(UTC)
        sess = _session(
            last_seen_at=now,
            access_expires_at=now + timedelta(seconds=600),
        )
        db = _make_db(sess)

        with _auth_patches() as m:
            result = await _resolve_from_cookie(SESSION_ID, db)
            await asyncio.sleep(0)

        assert result == SENTINEL_USER
        m["refresh"].assert_not_called()
        db.delete_srw_session.assert_not_called()
        db.touch_srw_session_last_seen.assert_awaited_once_with(SESSION_ID)

    @pytest.mark.asyncio
    async def test_missing_session_returns_none(self):
        """No row → None, fall through to other auth paths; nothing mutated."""
        db = _make_db(None)

        with _auth_patches() as m:
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_not_called()
        m["refresh"].assert_not_called()

    @pytest.mark.asyncio
    async def test_post_refresh_token_fails_validation_deletes(self):
        """Idle refresh succeeds but the new token won't validate → delete, None."""
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)

        with _auth_patches(validate_token=None):
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_awaited_once_with(SESSION_ID)

    @pytest.mark.asyncio
    async def test_no_sub_in_claims_deletes(self):
        """Neither access nor id token yields a sub → kill the session."""
        now = datetime.now(UTC)
        sess = _session(
            last_seen_at=now, access_expires_at=now + timedelta(seconds=600)
        )
        db = _make_db(sess)

        with _auth_patches(
            validate_token={"realm_access": {"roles": ["user"]}},  # no sub
            decode_id_token={},  # no sub
        ):
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_awaited_once_with(SESSION_ID)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_sub", [None, "", "   ", 123])
    async def test_unusable_sub_in_claims_deletes(self, bad_sub):
        """A null, blank or non-string sub is as unusable as an absent one."""
        now = datetime.now(UTC)
        sess = _session(
            last_seen_at=now, access_expires_at=now + timedelta(seconds=600)
        )
        db = _make_db(sess)

        with _auth_patches(
            validate_token={"sub": bad_sub, "realm_access": {"roles": ["user"]}},
            decode_id_token={},
        ):
            result = await _resolve_from_cookie(SESSION_ID, db)

        assert result is None
        db.delete_srw_session.assert_awaited_once_with(SESSION_ID)

    @pytest.mark.asyncio
    async def test_idle_refresh_updates_in_memory_id_token(self):
        """A2: after refresh, the downstream decode sees the NEW id_token.

        Proves _refresh_session_in_place mutates sess in place — otherwise
        decode_id_token would be handed the stale stored id_token.
        """
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)
        new_id_token = "brand.new.id.jwt"

        with _auth_patches(
            refresh={
                "access_token": "refreshed.access.jwt",
                "refresh_token": "refreshed-refresh-token",
                "id_token": new_id_token,
                "expires_in": 600,
            }
        ) as m:
            result = await _resolve_from_cookie(SESSION_ID, db)
            await asyncio.sleep(0)

        assert result == SENTINEL_USER
        m["decode_id_token"].assert_called_once_with(new_id_token)


class TestResolveWsUser:
    """WS handshakes can't carry an HTTP 503 — outage maps to None, row kept."""

    @pytest.mark.asyncio
    async def test_kc_outage_maps_503_to_none_without_deleting(self):
        """KC down during a WS connect → None (caller closes 4401), row kept."""
        now = datetime.now(UTC)
        sess = _session(last_seen_at=now - timedelta(hours=1))
        db = _make_db(sess)
        ws = SimpleNamespace(cookies={"srw_session": SESSION_ID})

        with _auth_patches(
            refresh_side_effect=KeycloakClientError("KC token endpoint unreachable")
        ):
            result = await resolve_ws_user(ws, db)

        assert result is None
        db.delete_srw_session.assert_not_called()


class TestKeycloakClientErrorClassification:
    """The tagging contract the auth path relies on to tell outage from rejection."""

    def test_invalid_grant_400_is_definitive(self):
        e = KeycloakClientError("x", status_code=400, oauth_error="invalid_grant")
        assert e.definitive_rejection

    @pytest.mark.parametrize(
        ("status_code", "oauth_error"),
        [
            (None, None),
            (502, None),
            (500, "unknown_error"),
            (401, "invalid_client"),
            (400, "unsupported_grant_type"),
            (200, None),  # malformed-JSON-on-200 carries no oauth_error
        ],
    )
    def test_everything_else_is_not_definitive(self, status_code, oauth_error):
        e = KeycloakClientError("x", status_code=status_code, oauth_error=oauth_error)
        assert not e.definitive_rejection


class TestPostTokenErrorTagging:
    """kc_client must tag raised errors so downstream classification works."""

    def _kc(self) -> KeycloakBFFClient:
        kc = KeycloakBFFClient()
        kc.client_secret = "s3cret"  # bypass the config-error raise in _basic_auth
        return kc

    def _client_ctx(self, *, post_side_effect=None, response=None):
        """An `async with httpx.AsyncClient(...)` stand-in."""
        ctx = AsyncMock()
        if post_side_effect is not None:
            ctx.__aenter__.return_value.post = AsyncMock(side_effect=post_side_effect)
        else:
            ctx.__aenter__.return_value.post = AsyncMock(return_value=response)
        return ctx

    @pytest.mark.asyncio
    async def test_network_error_carries_no_status_code(self):
        ctx = self._client_ctx(post_side_effect=httpx.ConnectError("boom"))
        with patch(
            "orchestrator.security.kc_client.httpx.AsyncClient", return_value=ctx
        ):
            with pytest.raises(KeycloakClientError) as exc_info:
                await self._kc().refresh("rt")
        assert exc_info.value.status_code is None
        assert not exc_info.value.definitive_rejection

    @pytest.mark.asyncio
    async def test_400_invalid_grant_is_tagged_definitive(self):
        resp = Mock(status_code=400)
        resp.json = Mock(
            return_value={
                "error": "invalid_grant",
                "error_description": "Token is not active",
            }
        )
        ctx = self._client_ctx(response=resp)
        with patch(
            "orchestrator.security.kc_client.httpx.AsyncClient", return_value=ctx
        ):
            with pytest.raises(KeycloakClientError) as exc_info:
                await self._kc().refresh("rt")
        assert exc_info.value.status_code == 400
        assert exc_info.value.oauth_error == "invalid_grant"
        assert exc_info.value.definitive_rejection

    @pytest.mark.asyncio
    async def test_5xx_is_tagged_non_definitive(self):
        resp = Mock(status_code=502, text="<html>bad gateway</html>")
        resp.json = Mock(side_effect=ValueError("not json"))
        ctx = self._client_ctx(response=resp)
        with patch(
            "orchestrator.security.kc_client.httpx.AsyncClient", return_value=ctx
        ):
            with pytest.raises(KeycloakClientError) as exc_info:
                await self._kc().refresh("rt")
        assert exc_info.value.status_code == 502
        assert not exc_info.value.definitive_rejection
