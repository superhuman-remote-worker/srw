"""Cookie BFF endpoints.

Replaces the cockpit's in-memory Bearer-token model with server-side
sessions keyed by an opaque HttpOnly cookie. The browser never sees the
Keycloak access/refresh/id token.

See knowledge-base/knowledge/features/auth_bff_and_api_tokens.md.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
from datetime import datetime, timedelta, UTC
from urllib.parse import urlencode, urlparse

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse

from orchestrator.security.auth import (
    SESSION_COOKIE,
    _has_usable_subject,
    _merge_identity_claims,
    _refresh_session_in_place,
    _resolve_user_from_claims,
    get_current_user,
)
from orchestrator.security.kc_client import KeycloakClientError, kc_bff_client
from orchestrator.security.oidc import oidc_validator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth (BFF)"])

PRE_AUTH_COOKIE = "srw_pre_auth"

# Env knobs ---------------------------------------------------------------


def _store(request: Request):
    """The application's database, from the app the router is mounted on.

    R1.B02 replaced a function-local ``from orchestrator.main import
    postgres_db`` in each handler with this. The late import existed only to
    dodge a circular import; reading the mounted application's own state keeps
    the dependency explicit and lets a test mount this router standalone.
    """
    return request.app.state.store


def _cookie_domain() -> str | None:
    """Cookie Domain attribute. None = host-only (right for localhost dev).

    Production sets ``SRW_COOKIE_DOMAIN=.superhuman-remote-worker.com`` so
    the cookie rides on both api.* and the cockpit origin.
    """
    return os.getenv("SRW_COOKIE_DOMAIN") or None


def _cookie_secure() -> bool:
    return os.getenv("SRW_COOKIE_SECURE", "1") == "1"


def _cookie_samesite() -> str:
    """Cookie SameSite attribute. "lax" is right for same-site prod deployments
    (cockpit and api on the same base domain). For local dev where cockpit is
    on `localhost` and api is on `api.localhost`, browsers treat those as
    cross-site and SameSite=Lax blocks the cookie on cross-site XHR — set to
    "none" in that case. SameSite=None requires Secure=true (browser-enforced).
    """
    return os.getenv("SRW_COOKIE_SAMESITE", "lax").lower()


def _absolute_lifetime() -> timedelta:
    return timedelta(
        seconds=int(os.getenv("SRW_SESSION_ABSOLUTE_TIMEOUT_S", "2592000"))
    )


def _spa_base() -> str:
    """Public origin the cockpit is served from. Used for post-login redirects.

    Falls back to the first CORS_ORIGINS entry, then to localhost:4200, so
    dev works out of the box.
    """
    explicit = os.getenv("SRW_SPA_BASE_URL", "")
    if explicit:
        return explicit.rstrip("/")
    cors = os.environ.get("CORS_ORIGINS", "")
    for entry in cors.split(","):
        if entry:
            return entry.rstrip("/")
    return "http://localhost:4200"


def _safe_return_to(raw: str | None) -> str:
    """Constrain ``return_to`` to a same-origin path so we never bounce to
    an attacker-supplied URL. Anything that isn't a clean ``/foo`` is
    coerced to ``/``.
    """
    if not raw:
        return "/"
    # Reject anything that includes a scheme or authority — only relative
    # paths anchored at root are allowed.
    if "://" in raw:
        return "/"
    if not raw.startswith("/"):
        return "/"
    # Strip CR/LF and excess whitespace (defense-in-depth against header
    # injection if this ever ended up in a Location header unencoded).
    cleaned = raw.replace("\r", "").replace("\n", "").strip()
    return cleaned or "/"


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _set_session_cookie(resp: Response, session_id: str) -> None:
    resp.set_cookie(
        SESSION_COOKIE,
        session_id,
        max_age=int(_absolute_lifetime().total_seconds()),
        path="/",
        domain=_cookie_domain(),
        secure=_cookie_secure(),
        httponly=True,
        samesite=_cookie_samesite(),
    )


def _clear_session_cookie(resp: Response) -> None:
    # Match the original Domain/Path so the browser actually removes it.
    resp.delete_cookie(
        SESSION_COOKIE,
        path="/",
        domain=_cookie_domain(),
    )


def _set_pre_auth_cookie(resp: Response, pre_auth_id: str) -> None:
    resp.set_cookie(
        PRE_AUTH_COOKIE,
        pre_auth_id,
        max_age=300,
        path="/auth",
        domain=_cookie_domain(),
        secure=_cookie_secure(),
        httponly=True,
        samesite=_cookie_samesite(),
    )


def _clear_pre_auth_cookie(resp: Response) -> None:
    resp.delete_cookie(
        PRE_AUTH_COOKIE,
        path="/auth",
        domain=_cookie_domain(),
    )


# Endpoints ---------------------------------------------------------------


# nosec: public OIDC handshake entry — redirects to Keycloak, no session exists yet
@router.get("/login")
async def login(
    request: Request,
    return_to: str | None = None,
    ui_locales: str | None = None,
) -> Response:
    """Kick off an OAuth code+PKCE flow against Keycloak.

    The browser navigates directly to this endpoint (no SPA token involved).
    We generate state + a PKCE verifier, park them in ``srw_pre_auth_states``
    and a short-lived cookie, then 302 to Keycloak's /authorize endpoint.
    """
    postgres_db = _store(request)

    if not kc_bff_client.redirect_uri:
        raise HTTPException(
            status_code=500,
            detail="BFF not configured: SRW_BFF_REDIRECT_URI is unset",
        )

    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    safe_return = _safe_return_to(return_to)
    pre_auth_id = await postgres_db.create_srw_pre_auth(
        state=state,
        pkce_verifier=verifier,
        return_to=safe_return,
    )

    params: dict[str, str] = {
        "response_type": "code",
        "client_id": kc_bff_client.client_id,
        "redirect_uri": kc_bff_client.redirect_uri,
        "scope": "openid profile email",
        "state": state,
        "code_challenge": _pkce_challenge(verifier),
        "code_challenge_method": "S256",
    }
    # Forward the SPA's preferred language to Keycloak's login form. Only
    # short, well-formed BCP47 tags are passed through so a hostile query
    # param can't push arbitrary content into Keycloak's `Accept-Language`.
    if (
        ui_locales
        and len(ui_locales) <= 16
        and all(c.isalnum() or c == "-" for c in ui_locales)
    ):
        params["ui_locales"] = ui_locales
    query = urlencode(params)
    url = f"{kc_bff_client.authorize_endpoint}?{query}"

    resp = RedirectResponse(url, status_code=302)
    _set_pre_auth_cookie(resp, pre_auth_id)
    return resp


# nosec: public OIDC redirect_uri — authenticates by exchanging the Keycloak code
@router.get("/callback")
async def callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    """Handle the redirect back from Keycloak.

    Validates pre-auth state + PKCE, exchanges the code for tokens, JIT-
    provisions the local user row, opens a new BFF session, sets the
    session cookie, and redirects to the cockpit's intended URL.
    """
    postgres_db = _store(request)

    if error:
        logger.warning("KC callback returned error %s: %s", error, error_description)
        # Don't echo error_description into the response body (KC may include
        # implementation details we'd rather not surface). Redirect to a known
        # cockpit path with a generic flag.
        resp = RedirectResponse(f"{_spa_base()}/?auth_error=1", status_code=302)
        _clear_pre_auth_cookie(resp)
        return resp

    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    pre_auth_id = request.cookies.get(PRE_AUTH_COOKIE)
    if not pre_auth_id:
        raise HTTPException(status_code=400, detail="Missing pre-auth state")

    pre = await postgres_db.consume_srw_pre_auth(pre_auth_id)
    if not pre:
        raise HTTPException(status_code=400, detail="Pre-auth state expired or used")
    if not secrets.compare_digest(pre["state"], state):
        raise HTTPException(status_code=400, detail="State mismatch")

    try:
        tokens = await kc_bff_client.exchange_code(code, pre["pkce_verifier"])
    except KeycloakClientError as e:
        logger.warning("KC code exchange failed: %s", e)
        raise HTTPException(
            status_code=502, detail="Authentication backend error"
        ) from e

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    id_token = tokens.get("id_token")
    expires_in = tokens.get("expires_in")
    refresh_expires_in = tokens.get("refresh_expires_in") or expires_in
    if not access_token or not refresh_token or not id_token or not expires_in:
        logger.warning("KC token response missing required fields")
        raise HTTPException(status_code=502, detail="Authentication backend error")

    claims = oidc_validator.validate_token(access_token)
    if not claims:
        logger.warning("KC-issued access token failed validation; refusing session")
        raise HTTPException(status_code=502, detail="Authentication backend error")

    id_claims = oidc_validator.decode_id_token(id_token) or {}
    # KC 24+ stopped including `sub` in the access token by default; the
    # cockpit-bff client doesn't carry a client-local sub mapper (cockpit-the-
    # SPA does, which is why the Bearer flow keeps working). The id token
    # is required by OIDC to carry `sub`, so we lift identity claims from
    # there into the merged dict.
    merged_claims = _merge_identity_claims(claims, id_claims)
    if not _has_usable_subject(merged_claims):
        logger.warning(
            "KC token response had no usable sub claim in access_token or id_token"
        )
        raise HTTPException(status_code=502, detail="Authentication backend error")
    kc_sub = merged_claims["sub"]
    kc_sid = id_claims.get("sid")

    # JIT-provision / sync the local user row.
    user = await _resolve_user_from_claims(merged_claims, postgres_db)

    # Session-fixation defense (lifted from
    # Advanced-LLM-Chat/backend/security/auth.py:60-63). If the user already
    # has a session cookie when they land at /callback (re-login flow without
    # explicit logout), kill the old row first so the freshly-authenticated
    # user gets a brand-new session ID. Without this, an attacker who set a
    # known session cookie before login could keep using it afterwards.
    old_sess_id = request.cookies.get(SESSION_COOKIE)
    if old_sess_id:
        await postgres_db.delete_srw_session(old_sess_id)

    now = datetime.now(UTC)
    session_id = await postgres_db.create_srw_session(
        user_id=user["id"],
        kc_sub=kc_sub,
        kc_sid=kc_sid,
        access_token=access_token,
        refresh_token=refresh_token,
        id_token=id_token,
        access_expires_at=now + timedelta(seconds=int(expires_in)),
        absolute_expires_at=now + timedelta(seconds=int(refresh_expires_in)),
        user_agent=request.headers.get("user-agent"),
        created_ip=request.client.host if request.client else None,
    )

    return_to = pre["return_to"] or "/"
    target = f"{_spa_base()}{return_to}"
    resp = RedirectResponse(target, status_code=302)
    _set_session_cookie(resp, session_id)
    _clear_pre_auth_cookie(resp)
    logger.info("BFF session %s opened for sub=%s", session_id, kc_sub)
    return resp


@router.get("/me")
async def me(request: Request) -> dict:
    """Return the current user. Cockpit calls this at startup.

    No CSRF needed (GET). Cookie path runs first inside ``get_current_user``;
    a Bearer caller hits this fine too (transitional support).
    """
    postgres_db = _store(request)

    user = await get_current_user(request, postgres_db)
    # Mirror the shape of the existing /api/auth/me — id, email, display name,
    # admin / approved flags. Avoid leaking internal-only DB columns.
    return {
        "id": user["id"],
        "email": user.get("email"),
        "display_name": user.get("display_name"),
        "preferred_username": user.get("preferred_username"),
        "avatar_color": user.get("avatar_color"),
        "is_admin": bool(user.get("is_admin")),
        "is_approved": bool(user.get("is_approved")),
        "default_project_id": user.get("default_project_id"),
    }


# nosec: public BFF session cookie is validated in-handler (401 without one)
@router.post("/refresh")
async def refresh(request: Request) -> dict:
    """Force a server-side refresh of the stored access token.

    Rarely needed — the per-request validator refreshes transparently. The
    only legit caller is the cockpit when it wants to proactively pull
    fresh claims (e.g. after an admin granted a new role).
    """
    postgres_db = _store(request)

    session_id = request.cookies.get(SESSION_COOKIE)
    if not session_id:
        raise HTTPException(status_code=401, detail="No session")
    sess = await postgres_db.get_srw_session(session_id)
    if not sess:
        raise HTTPException(status_code=401, detail="Session not found")
    new_access = await _refresh_session_in_place(sess, postgres_db)
    if new_access is None:
        raise HTTPException(status_code=401, detail="Refresh failed")
    return {"refreshed": True}


# nosec: public revokes whatever session the cookie names; a no-op without one
@router.post("/logout")
async def logout(request: Request) -> JSONResponse:
    """Revoke the current BFF session and return the Keycloak logout URL.

    The cockpit redirects ``window.location.href`` to ``kc_logout_url`` so
    KC clears its SSO cookie too. ``id_token_hint`` is included so KC 19+
    skips the confirmation screen.
    """
    postgres_db = _store(request)

    session_id = request.cookies.get(SESSION_COOKIE)
    kc_logout_url: str | None = None
    if session_id:
        sess = await postgres_db.get_srw_session(session_id)
        if sess:
            # Best-effort KC-side revocation. Failures don't block the user
            # from logging out locally; KC's session expires naturally
            # within the refresh-token TTL even if this misses.
            await kc_bff_client.logout_kc_side(sess["refresh_token"])
            # Trailing slash matters: KC's post.logout.redirect.uris pattern is
            # `{cockpit_url}/*`, and bare `https://localhost` doesn't reliably
            # match `https://localhost/*` across KC versions. Send `.../` so
            # the wildcard `*` matches the empty path explicitly.
            qs = urlencode(
                {
                    "id_token_hint": sess["id_token"],
                    "post_logout_redirect_uri": _spa_base() + "/",
                }
            )
            kc_logout_url = f"{kc_bff_client.end_session_endpoint}?{qs}"
        await postgres_db.delete_srw_session(session_id)
        logger.info("BFF session %s closed by /auth/logout", session_id)

    resp = JSONResponse({"kc_logout_url": kc_logout_url})
    _clear_session_cookie(resp)
    return resp


# nosec: public OIDC back-channel logout — authenticated by Keycloak's signed logout_token
@router.post("/backchannel-logout")
async def backchannel_logout(request: Request) -> Response:
    """Receive Keycloak's signed logout token (OIDC BCL §2.5).

    Application/x-www-form-urlencoded body with a single ``logout_token``
    field. We verify the JWT, then delete every BFF session row tied to
    the matching KC SID.
    """
    postgres_db = _store(request)

    form = await request.form()
    logout_token = form.get("logout_token")
    if not logout_token or not isinstance(logout_token, str):
        raise HTTPException(status_code=400, detail="Missing logout_token")
    claims = oidc_validator.verify_logout_token(logout_token)
    if not claims:
        # Spec says: respond 400 on any verification failure.
        raise HTTPException(status_code=400, detail="Invalid logout token")
    sid = claims.get("sid")
    sub = claims.get("sub")
    deleted = 0
    if sid:
        deleted = await postgres_db.delete_srw_sessions_by_kc_sid(sid)
    elif sub:
        # Fallback: KC may emit logout-token without sid for some flows. We
        # don't yet index by sub directly; pull rows manually. This is rare
        # so a small scan is fine.
        async with postgres_db.acquire() as conn:
            rows = await conn.fetch(
                "DELETE FROM srw_sessions WHERE kc_sub = $1 RETURNING id",
                sub,
            )
            deleted = len(rows)
    logger.info(
        "Back-channel logout: deleted %d session(s) for sid=%s sub=%s",
        deleted,
        sid,
        sub,
    )
    # Spec § 2.8 — return 200 with empty body on success.
    return Response(status_code=200)


# Helpers exported for tests and admin tooling ---------------------------

__all__ = ["router", "PRE_AUTH_COOKIE"]


# Sanity-check at import time: warn loudly if the redirect URI is set but
# doesn't look right. Cheap protection against the most common mis-config.
def _validate_redirect_uri() -> None:
    uri = kc_bff_client.redirect_uri
    if not uri:
        return  # Will fail with a clear message at /auth/login; don't double-log.
    try:
        parsed = urlparse(uri)
    except ValueError:
        logger.warning("SRW_BFF_REDIRECT_URI is not a valid URL: %s", uri)
        return
    if parsed.path != "/auth/callback":
        logger.warning(
            "SRW_BFF_REDIRECT_URI path is %s; expected /auth/callback. "
            "Keycloak Valid Redirect URIs must match this exactly.",
            parsed.path,
        )


_validate_redirect_uri()
