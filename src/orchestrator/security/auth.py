"""Authentication for the orchestrator API.

Four auth paths, tried in this order:
1. ``srw_session`` cookie — cookie BFF; mid-stream access-token refresh is
   transparent. See knowledge-base/knowledge/features/auth_bff_and_api_tokens.md.
2. Bearer token (OIDC) — Keycloak access token. Transitional during the
   cockpit cutover; will remain for direct API consumers and for tests.
3. X-MCP-Token — API tokens for Claude Code / CLI clients (unchanged).
4. X-MCP-User-Id + X-Internal-Key — forwarded by the MCP server after
   it has already authenticated the caller via OAuth or API token.

Cookie-path additions are purely additive — the function signature and
the ~86 inline ``await require_approved_user(request, db)`` call sites
do not change.
"""

import asyncio
import hashlib
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta, UTC
from typing import Any

from fastapi import HTTPException, Request
from orchestrator.security import token_scopes
from orchestrator.security.account_approval import require_account_approved
from orchestrator.security.kc_client import KeycloakClientError, kc_bff_client
from orchestrator.security.oidc import oidc_validator

logger = logging.getLogger(__name__)

SESSION_COOKIE = "srw_session"

# Header opt-in for the admin "view as user" shadow. When an admin request
# carries ``X-Admin-View-As: user``, ``require_approved_user`` returns a dict
# with ``is_admin=False`` so visibility helpers narrow as if the caller were a
# regular user. The un-shadowed privilege flag is preserved on
# ``real_is_admin`` so ``_require_admin`` (and the eventual
# ``security.access.require_admin``) keep admin-only endpoints reachable.
# Design: knowledge-base/knowledge/features/admin_view_as_user.md.
VIEW_AS_HEADER = "X-Admin-View-As"


# Knobs read fresh on each request (so the orchestrator picks up env edits
# without restart). Caller cost is one os.getenv per request which is
# trivially cheap.
def _idle_timeout() -> timedelta:
    return timedelta(seconds=int(os.getenv("SRW_SESSION_IDLE_TIMEOUT_S", "1800")))


def _refresh_skew() -> timedelta:
    return timedelta(seconds=int(os.getenv("SRW_ACCESS_TOKEN_REFRESH_SKEW_S", "60")))


# Identity claims that live in the id token (per OIDC spec). When the access
# token's mappers don't include them — KC 24+ default for `sub` — we lift them
# from id token claims into the dict passed to `_resolve_user_from_claims`.
_ID_TOKEN_IDENTITY_FIELDS = (
    "sub",
    "email",
    "preferred_username",
    "name",
    "email_verified",
)


def _merge_identity_claims(access_claims: dict, id_claims: dict | None) -> dict:
    """Return access_claims augmented with id-token identity fields.

    Existing access_claims values win on overlap. Used by both the BFF
    callback and the per-request cookie validator so the rest of the
    auth code (e.g. `_resolve_user_from_claims`) can keep reading a
    single dict regardless of which client's mappers ran.
    """
    if not id_claims:
        return access_claims
    merged = dict(access_claims)
    for key in _ID_TOKEN_IDENTITY_FIELDS:
        if key not in merged and key in id_claims:
            merged[key] = id_claims[key]
    return merged


async def get_current_user(request: Request, db) -> dict:
    """FastAPI dependency: resolve the current user.

    Order of attempts:
      1. ``srw_session`` cookie (BFF) — server-side refresh of the stored
         access token when it's within the refresh skew window.
      2. ``Authorization: Bearer <jwt>`` — Keycloak access token directly.
      3. MCP internal header auth (X-MCP-User-Id + X-Internal-Key).

    Returns the full user record (id, display_name, avatar_color, email,
    default_project_id, is_admin, is_approved, keycloak_sub, created_at).
    The ``is_approved`` flag comes from the ``users.is_approved`` column
    (app-side admission), OR-combined with the legacy Keycloak ``user`` role
    during the transition. Because it's checked per request, an admin
    suspending a user takes effect immediately (cookie path: at most one
    ``access_token`` refresh later).

    Raises:
        HTTPException 401 if not authenticated or all paths fail.
        HTTPException 503 if the cookie path needed a Keycloak token refresh
            and Keycloak was unreachable/broken — the session is kept alive
            and the client should retry, not re-login.
    """
    # Path 1: cookie BFF.
    session_id = request.cookies.get(SESSION_COOKIE)
    if session_id:
        user = await _resolve_from_cookie(session_id, db)
        if user is not None:
            return user
        # Invalid/expired cookie: fall through. The cookie itself will be
        # cleared on the next /auth/me response (the SPA refreshes via the
        # interceptor's 401 handler).

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        # Path 3: MCP internal header auth.
        return await _get_user_from_mcp_headers(request, db)

    # Path 2: Bearer dispatch — PAT / legacy MCP / Keycloak JWT all share
    # this header. Shape-based: three-dot tokens are JWTs; ak_ are PATs;
    # srw_ are MCP tokens (now landing in auth_tokens with kind='mcp');
    # anything else is a malformed credential.
    token = auth_header[7:]
    if token.startswith("ak_"):
        return await _resolve_pat(token, request, db)
    if token.startswith("srw_"):
        return await _resolve_legacy_mcp_token(token, request, db)
    if token.count(".") == 2:
        claims = oidc_validator.validate_token(token)
        if not claims:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        return await _resolve_user_from_claims(claims, db)
    raise HTTPException(status_code=401, detail="Unrecognized token format")


async def _resolve_from_cookie(session_id: str, db) -> dict | None:
    """Validate a BFF session cookie. Returns user dict or None.

    None signals "fall through to other auth paths"; we don't raise here
    so that legacy Bearer clients with a stale cookie still work. The
    cookie gets cleared via the standard /auth/me round-trip.

    Refreshes the stored access token mid-session when it's within
    ``SRW_ACCESS_TOKEN_REFRESH_SKEW_S`` of expiry, and re-validates with
    Keycloak (the same refresh) once the session has been idle past
    ``SRW_SESSION_IDLE_TIMEOUT_S`` rather than deleting it: the BFF session
    is a renewable lease over the KC SSO session, so an idle-but-still-valid
    session is renewed instead of force-logging the user out (which would
    lose unsent work). A *definitive* refresh rejection (SSO ended, refresh
    token revoked) deletes the session row and returns None; an unreachable
    or broken KC keeps the row and raises 503 for this request instead (see
    ``_refresh_session_in_place``). The absolute lifetime cap remains a hard
    stop with no refresh attempt.
    """
    sess = await db.get_srw_session(session_id)
    if not sess:
        return None
    now = datetime.now(UTC)
    if sess["absolute_expires_at"] <= now:
        # Past absolute lifetime — refresh attempts would just bounce.
        await db.delete_srw_session(session_id)
        return None
    # Idle OR access token near expiry → re-validate with Keycloak by
    # refreshing in place. On idle this proves the SSO session is still alive
    # instead of blind-deleting a session KC still considers valid: the idle
    # window becomes a re-validation checkpoint, not a logout, so an idle user
    # stops being force-logged-out (and losing unsent work) mid-session. A
    # genuine KC rejection (SSO ended / refresh token revoked) deletes the row
    # inside _refresh_session_in_place and returns None → clean re-login.
    #
    # CONCURRENCY INVARIANT: a tab refocus can fan out several requests that all
    # hit this branch and refresh with the SAME refresh_token. That is safe ONLY
    # while Keycloak refresh-token rotation is OFF (revokeRefreshToken=false — the
    # realm default and our config in helm/ + HomeLab). Do NOT enable rotation
    # without first serializing refresh per session (asyncio.Lock keyed by
    # session_id, re-reading the row inside the lock).
    access_token = sess["access_token"]
    idle_expired = sess["last_seen_at"] + _idle_timeout() <= now
    near_expiry = sess["access_expires_at"] - now <= _refresh_skew()
    if idle_expired or near_expiry:
        access_token = await _refresh_session_in_place(sess, db)
        if access_token is None:
            return None

    claims = oidc_validator.validate_token(access_token)
    if not claims:
        # Stored access token won't validate (signing key rotated?). Kill
        # the session so the user re-authenticates cleanly.
        await db.delete_srw_session(session_id)
        return None

    # KC 24+ default doesn't put `sub` in the access token unless a client-
    # local mapper enables it; the id token always carries `sub` per OIDC.
    # Merge identity fields from the stored id_token so downstream user
    # resolution works regardless of the client's mapper config.
    id_claims = oidc_validator.decode_id_token(sess["id_token"]) or {}
    claims = _merge_identity_claims(claims, id_claims)
    if "sub" not in claims:
        logger.warning("Session %s tokens have no sub claim — killing", session_id)
        await db.delete_srw_session(session_id)
        return None

    # Bump idle anchor. Fire-and-forget — this is a hot path and the next
    # request's view of last_seen_at is allowed to lag by a few ms.
    asyncio.create_task(db.touch_srw_session_last_seen(session_id))

    return await _resolve_user_from_claims(claims, db)


async def _refresh_session_in_place(sess: dict, db) -> str | None:
    """Refresh KC tokens and write them back to the session row.

    Returns the new access token on success. Returns None only when Keycloak
    definitively rejected the refresh (400 ``invalid_grant``: SSO session
    ended / refresh token revoked) — the row is deleted and the caller treats
    the session as dead → clean re-login.

    When Keycloak is unreachable or otherwise broken (network error, 5xx,
    malformed body, client misconfig like ``invalid_client``), the row is
    KEPT and 503 is raised instead: the KC SSO session is in all likelihood
    still valid, and deleting the row would permanently log every active user
    out after any KC outage longer than one access-token lifespan (~15 min).
    This way the request fails retryably and sessions resume on their own
    once KC is back. The grace is bounded by KC's own timeouts: if the outage
    outlives the refresh token, the next refresh IS an ``invalid_grant`` and
    the row dies then.
    """
    try:
        tokens = await kc_bff_client.refresh(sess["refresh_token"])
    except KeycloakClientError as e:
        if e.definitive_rejection:
            logger.info("Session %s refresh rejected (%s) — deleting", sess["id"], e)
            await db.delete_srw_session(sess["id"])
            return None
        logger.warning(
            "Session %s refresh failed transiently (%s) — keeping session",
            sess["id"],
            e,
        )
        raise HTTPException(
            status_code=503,
            detail="Authentication service temporarily unavailable",
        ) from e
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token") or sess["refresh_token"]
    expires_in = tokens.get("expires_in")
    if not access_token or not expires_in:
        # KC answered 200 with an unusable body — server-side breakage, not a
        # grant rejection. Same posture as unreachable: keep the row, 503.
        logger.warning(
            "Session %s refresh returned malformed payload — keeping session",
            sess["id"],
        )
        raise HTTPException(
            status_code=503,
            detail="Authentication service temporarily unavailable",
        )
    new_access_expires_at = datetime.now(UTC) + timedelta(seconds=int(expires_in))
    await db.refresh_srw_session_tokens(
        sess["id"],
        access_token=access_token,
        refresh_token=refresh_token,
        access_expires_at=new_access_expires_at,
        id_token=tokens.get("id_token"),
    )
    # Keep the in-memory session authoritative for the rest of this request —
    # the caller still reads sess["id_token"] (and may read other token fields)
    # after we return. Without this, an idle-triggered refresh would leave the
    # stale id_token in sess for the downstream identity-claim merge.
    sess["access_token"] = access_token
    sess["refresh_token"] = refresh_token
    sess["access_expires_at"] = new_access_expires_at
    if tokens.get("id_token"):
        sess["id_token"] = tokens["id_token"]
    return access_token


async def _resolve_user_from_claims(claims: dict, db) -> dict:
    """JIT-provision (or sync) the local user row from Keycloak claims.

    Shared between cookie and Bearer paths. Admission lives in the
    ``users.is_approved`` column now (app-side admission): this function reads
    that flag and OR-combines it with the legacy ``user`` realm role during the
    transition window, writing the DB flag through for legacy role-holders on
    their first request after deploy. ``preferred_username`` is persisted to
    the row (approval-time provisioning needs it). The returned ``is_approved``
    / ``preferred_username`` keys carry the effective values for this request.
    """
    sub = claims["sub"]
    email = claims.get("email", "")
    display_name = (
        claims.get("preferred_username")
        or claims.get("name")
        or email.split("@")[0]
        or "User"
    )
    realm_roles = claims.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in realm_roles
    # Keycloak answers authn only; admission lives in users.is_approved.
    # `role_approved` is the legacy signal (the `user` realm role, or admin).
    # During the transition it still grants access AND drives the write-through
    # that migrates legacy role-holders onto the DB flag.
    role_approved = "user" in realm_roles or is_admin
    preferred_username = claims.get("preferred_username")

    user = await db.get_user_by_keycloak_sub(sub)
    if user:
        needs_update = {}
        if email and (not user.get("email") or user["email"] != email):
            needs_update["email"] = email
        if user.get("display_name") != display_name:
            needs_update["display_name"] = display_name
        if user.get("is_admin") != is_admin:
            needs_update["is_admin"] = is_admin
        if preferred_username and user.get("preferred_username") != preferred_username:
            needs_update["preferred_username"] = preferred_username
        db_approved = bool(user.get("is_approved"))
        # Write-through migration: a legacy role-holder still FALSE in the DB
        # self-migrates on this request. Never the reverse — absence of the
        # role does not clear the flag (suspension is admin-driven only).
        if role_approved and not db_approved:
            needs_update["is_approved"] = True
            needs_update["approved_at"] = datetime.now(UTC)
            needs_update["approved_by"] = None
        if needs_update:
            async with db.acquire() as conn:
                set_clause = ", ".join(
                    f"{k} = ${i + 2}" for i, k in enumerate(needs_update)
                )
                await conn.execute(
                    f"UPDATE users SET {set_clause} WHERE id = $1",
                    user["id"],
                    *needs_update.values(),
                )
            user.update(needs_update)
            logger.info(
                "Updated user %s from OIDC claims: %s", sub, list(needs_update.keys())
            )
            if needs_update.get("is_admin"):
                # Promotion to admin: seed max-level grants so the grants data
                # (and UI) reflect the bypass instead of an empty list that
                # reads as "no permission". Existing admins: migration 0049.
                await db.seed_admin_grants(str(user["id"]))
        user["is_approved"] = db_approved or role_approved
        user["preferred_username"] = preferred_username
        return user

    # First login — create local user row + seed cloud/Gitea in the background.
    user = await db.upsert_user_from_oidc(
        sub=sub,
        email=email,
        display_name=display_name,
        is_admin=is_admin,
        is_approved=role_approved,
        preferred_username=preferred_username,
    )
    if is_admin and user.get("id"):
        # First login of an admin: seed max-level grants (see promotion path).
        await db.seed_admin_grants(str(user["id"]))
    # Effective approval reflects the OR-merge inside upsert: an admin-created
    # or pre-seeded row can already be approved without carrying the role.
    effective_approved = bool(user.get("is_approved"))
    logger.info(
        "JIT-provisioned user %s (sub=%s, admin=%s, approved=%s)",
        display_name,
        sub,
        is_admin,
        effective_approved,
    )
    if effective_approved:
        # Approved on first login (admin, or a legacy `user` role-holder):
        # pre-provision cloud + Gitea so their first thread/repo finds them.
        asyncio.create_task(
            _ensure_cloud_user(
                sub=sub,
                issuer=claims.get("iss", ""),
                email=email,
                display_name=display_name,
                preferred_username=preferred_username,
            )
        )
        asyncio.create_task(
            _ensure_gitea_user(
                sub=sub,
                email=email,
                preferred_username=preferred_username,
                display_name=display_name,
            )
        )
    else:
        # New registrant is pending — do NOT provision cloud/Gitea accounts
        # for someone no admin has admitted (closes the pre-approval resource
        # leak). Provisioning runs at approval time instead, via
        # ensure_user_provisioned. Tell the admins there's someone to review.
        asyncio.create_task(_notify_admins_of_registration(user, email))
    # Dev-only: the moment the admin user first exists, seed the fixed dev MCP
    # token so a committed .mcp.json authenticates without a manual mint or an
    # orchestrator restart. Gated on MCP_DEV_TOKEN (unset in prod → skipped);
    # the function is idempotent and the startup seed in main.py covers restarts.
    if is_admin and os.environ.get("MCP_DEV_TOKEN", "").strip():
        from orchestrator.init import _seed_admin_mcp_token

        asyncio.create_task(_seed_admin_mcp_token(db))
    user["is_approved"] = effective_approved
    user["preferred_username"] = preferred_username
    return user


async def ensure_user_provisioned(user_row: dict) -> None:
    """Fire cloud + Gitea ensure-user for an approved user from row data.

    Approval-time provisioning for app-side admission. The JIT ensures only
    fire for users approved at first login; an app-side-approved user never
    re-triggers them (the Keycloak ``user`` role is never assigned, so every
    later login takes the existing-user branch). This is therefore the
    provisioning path for users admitted via the cockpit. Both ensures are
    idempotent, so re-running for an already-provisioned user is harmless.

    Best-effort and non-blocking: the two ensures swallow and log their own
    errors. A row without a ``keycloak_sub`` (admin-created / pre-OIDC) is
    skipped — it provisions on its owner's first real OIDC login instead.
    """
    sub = (user_row.get("keycloak_sub") or "").strip()
    if not sub:
        return
    email = user_row.get("email") or ""
    display_name = user_row.get("display_name") or (
        email.split("@")[0] if email else "User"
    )
    preferred_username = user_row.get("preferred_username")
    asyncio.create_task(
        _ensure_cloud_user(
            sub=sub,
            issuer="",  # resolved from the backend's configured issuer
            email=email,
            display_name=display_name,
            preferred_username=preferred_username,
        )
    )
    asyncio.create_task(
        _ensure_gitea_user(
            sub=sub,
            email=email,
            preferred_username=preferred_username,
            display_name=display_name,
        )
    )


async def _notify_admins_of_registration(user: dict, email: str) -> None:
    """Best-effort: tell admins a new user registered and is awaiting approval."""
    try:
        from orchestrator.services.notification_service import notification_service

        await notification_service.record_user_registered(
            new_user_id=str(user["id"]),
            display_name=user.get("display_name"),
            email=email or None,
        )
    except Exception as e:
        logger.warning("Registration notify failed: %s", e)


async def _resolve_pat(token: str, request: Request, db) -> dict:
    """Resolve a `ak_<…>` Personal Access Token against ``auth_tokens``.

    Hashes the token, checks the kind matches the prefix (so a hash that
    somehow showed up against a kind='mcp' row cannot impersonate a PAT),
    enforces revocation/expiry, and fires a non-blocking touch to update
    ``last_used_at`` + ``last_used_ip`` for audit/UX.

    Returns the user dict with ``auth_method='pat'``, ``scopes`` (the
    PAT's action scopes from the row), and ``token_id`` for downstream
    introspection. Admission flows from the user row (``is_approved``): the
    force-True is gone so that suspending a user also kills their PATs — if
    the owner is no longer approved, the token stops working on the next call.

    Action scopes are enforced here, against the route FastAPI matched, so
    every PAT request passes the check whichever gate its handler calls (see
    ``security.token_scopes``). Admin reach is itself a scope: without
    ``admin`` the owner's admin flag is dropped for this request, so admin-only
    gates refuse and admin visibility narrows to the owner's own rows.
    """
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    row = await db.get_auth_token_by_hash(digest)
    if not row or row["kind"] != "api":
        raise HTTPException(status_code=401, detail="Invalid token")
    user = await db.get_user(str(row["user_id"]))
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    asyncio.create_task(
        db.touch_auth_token(str(row["id"]), _client_ip(request)),
    )
    user["auth_method"] = token_scopes.PAT_AUTH_METHOD
    user["scopes"] = list(row.get("scopes") or [])
    user["token_id"] = str(row["id"])
    if token_scopes.ADMIN_SCOPE not in user["scopes"]:
        user["is_admin"] = False
    try:
        token_scopes.enforce_route_scopes(request, user)
    except HTTPException as exc:
        # Lazy: security.access imports this module.
        from orchestrator.security.access import log_security_event

        route = token_scopes.route_identity(request)
        await log_security_event(
            db,
            event_type="token_scope_denied",
            user=user,
            resource_type="api_route",
            resource_id=route[1] if route else None,
            detail=f"{exc.detail} (token scopes: {sorted(map(str, user['scopes']))})",
            request=request,
        )
        raise
    return user


async def _resolve_legacy_mcp_token(token: str, request: Request, db) -> dict:
    """Resolve a legacy `srw_<…>` MCP token directly against ``auth_tokens``.

    The MCP server's own TokenVerifier still routes through
    ``/api/internal/mcp-token-verify`` which calls
    ``get_mcp_token_by_hash`` + ``update_mcp_token_last_used``. This path
    is for callers that hit the orchestrator API *directly* with an MCP
    token in the Authorization header — the consolidated auth surface
    advertised in the design doc.

    Admission flows from the user row for the same reason as ``_resolve_pat``
    — suspending a user must also disable their MCP tokens.
    """
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    row = await db.get_auth_token_by_hash(digest)
    if not row or row["kind"] != "mcp":
        raise HTTPException(status_code=401, detail="Invalid token")
    user = await db.get_user(str(row["user_id"]))
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    asyncio.create_task(
        db.touch_auth_token(str(row["id"]), _client_ip(request)),
    )
    user["auth_method"] = "mcp"
    # MCP scope semantics differ from PAT scopes — `scope` is a single
    # string ('user'/'all'/'project:<uuid>'). Surface as a single-element
    # list so downstream scope-aware code can treat both kinds uniformly.
    legacy_scope = row.get("scope") or ""
    user["scopes"] = [legacy_scope] if legacy_scope else []
    user["token_id"] = str(row["id"])
    return user


def _client_ip(request: Request) -> str | None:
    """Best-effort client IP for audit logging.

    Behind ingress, ``request.client.host`` is the ingress pod IP; the
    real client lives in ``X-Forwarded-For``'s leftmost entry. We accept
    a single forwarded hop because that's what our nginx ingress sets.
    Multi-hop XFF chains are out of scope.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip() or None
    client = getattr(request, "client", None)
    return client.host if client else None


# Optional provisioning backends, bound once by application composition
# (``orchestrator.main``). Two named callables, deliberately not a context
# object: these helpers run as detached ``asyncio`` tasks with no request scope,
# so they cannot receive the collaborators through a dependency the way the
# request-path adapters do.
#
# Unbound means "no backend configured", which is the same observable outcome as
# an uninitialised client and is exactly how a deployment without cloud/forge
# behaves. A warning is emitted because unbound in a real application is a
# composition bug, not a deployment shape.
_cloud_router_provider: Callable[[], Any] | None = None
_forge_provider: Callable[[], Any] | None = None


def set_provisioning_backends(
    *,
    cloud_router: Callable[[], Any] | None,
    forge: Callable[[], Any] | None,
) -> None:
    """Bind the optional cloud/forge provisioning backends."""
    global _cloud_router_provider, _forge_provider
    _cloud_router_provider = cloud_router
    _forge_provider = forge


def _resolve_provisioning_backend(
    provider: Callable[[], Any] | None, label: str
) -> Any | None:
    if provider is None:
        logger.warning(
            "%s provisioning backend is unbound; skipping background "
            "provisioning (application composition did not call "
            "set_provisioning_backends)",
            label,
        )
        return None
    return provider()


async def _ensure_cloud_user(
    *,
    sub: str,
    issuer: str,
    email: str,
    display_name: str,
    preferred_username: str | None,
) -> None:
    """Background call to the active main-cloud backend's ensure_user.

    The router arrives through ``set_provisioning_backends`` rather than an
    import of the application module (R1.B02 caller-boundary closure).
    """
    try:
        main_cloud_router = _resolve_provisioning_backend(
            _cloud_router_provider, "main-cloud"
        )
        if main_cloud_router is None:
            return

        # Fresh per-owner provisioning — resolve via the owner seam, not
        # ``.active`` directly (Issue 16, knowledge-base/knowledge/issues/main_cloud.md).
        backend = main_cloud_router.for_owner({"keycloak_sub": sub, "email": email})
        if not backend.is_initialized:
            return
        await backend.ensure_user(
            sub=sub,
            # No OIDC claims at approval time → fall back to the backend's
            # configured Keycloak issuer.
            issuer=issuer or getattr(backend, "_keycloak_issuer", "") or "",
            email=email,
            display_name=display_name,
            preferred_username=preferred_username,
        )
    except Exception as e:
        logger.warning("Main-cloud ensure_user failed for sub=%s: %s", sub, e)


async def _ensure_gitea_user(
    *,
    sub: str,
    email: str,
    preferred_username: str | None,
    display_name: str,
) -> None:
    """Background pre-provision a Gitea user on first cockpit login.

    Mirrors _ensure_cloud_user but for Gitea. Without this, a user who
    creates a thread immediately after signing into cockpit (but before
    ever visiting Gitea) won't be a collaborator on their own thread
    repo — Gitea's OIDC auto-registration only fires on direct Gitea
    visits.

    Passes ``sub`` through so Gitea stores it as login_name — that's the
    key Gitea matches on during later direct OIDC login, preventing
    duplicate accounts.

    The client arrives through ``set_provisioning_backends`` rather than an
    import of the application module (R1.B02 caller-boundary closure).
    """
    try:
        gitea_client = _resolve_provisioning_backend(_forge_provider, "forge")
        if gitea_client is None:
            return

        if not gitea_client.is_initialized:
            return
        # preferred_username is the canonical Gitea login; fall back to
        # email-localpart for clients that don't emit the claim.
        username = preferred_username or (email.split("@")[0] if email else None)
        if not username or not email:
            return
        await gitea_client.ensure_user(
            email=email,
            username=username,
            full_name=display_name,
            sub=sub,
        )
    except Exception as e:
        logger.warning("Gitea ensure_user failed for email=%s: %s", email, e)


async def _get_user_from_mcp_headers(request: Request, db) -> dict:
    """Authenticate via MCP internal headers.

    The MCP server validates the caller (OAuth / API token) and then
    forwards requests to the orchestrator with X-MCP-User-Id and
    X-Internal-Key headers. We trust these headers only when the
    internal key matches MCP_INTERNAL_KEY.

    F7: ``X-MCP-Scope`` is captured into ``user['scopes']`` so the
    access.py visibility helpers can narrow further. Legacy MCP scope
    strings are ``'user'``, ``'all'`` (admin-equivalent for THIS user
    — no extra grant if the user isn't admin) or ``'project:<uuid>'``
    (restrict to one project). Anything else is treated as a no-op.
    """
    mcp_user_id = request.headers.get("X-MCP-User-Id")
    internal_key = request.headers.get("X-Internal-Key", "")
    expected_key = os.environ.get("MCP_INTERNAL_KEY", "")

    if mcp_user_id and expected_key and internal_key == expected_key:
        user = await db.get_user(mcp_user_id)
        if user:
            # Admission flows from the user row — a suspended owner's MCP
            # access stops here too (the header only proves the MCP server
            # validated the token, not that the user is still admitted).
            user["auth_method"] = "mcp"
            mcp_scope = request.headers.get("X-MCP-Scope")
            user["scopes"] = [mcp_scope] if mcp_scope else []
            return user
        logger.warning("MCP header auth: user %s not found in DB", mcp_user_id)

    raise HTTPException(status_code=401, detail="Not authenticated")


async def require_approved_user(request: Request, db) -> dict:
    """Like get_current_user but raises 403 if the user isn't approved.

    Approval is the ``users.is_approved`` flag (app-side admission), resolved
    onto the user dict by the auth path. Use this for all endpoints that
    require an admitted account. /api/auth/me should use get_current_user
    directly so the cockpit can display its "pending approval" screen.

    Admin shadow ("view as user"): when an admin request carries
    ``X-Admin-View-As: user``, the returned dict has ``is_admin=False`` so
    visibility helpers (``user_visible_project_ids``, ``query_jobs``,
    ``user_can_access_datasource``) narrow as if the caller were a regular
    user. The un-shadowed privilege flag is preserved on ``real_is_admin``
    so ``_require_admin`` keeps admin-only endpoints reachable while the
    shadow is on. The header is a no-op for non-admin callers — they get
    ``real_is_admin=False`` for parity. Design:
    knowledge-base/knowledge/features/admin_view_as_user.md.
    """
    user = await get_current_user(request, db)
    require_account_approved(user)
    view_as = request.headers.get(VIEW_AS_HEADER, "").lower()
    if view_as == "user" and user.get("is_admin"):
        return {**user, "is_admin": False, "real_is_admin": True}
    return {**user, "real_is_admin": bool(user.get("is_admin"))}


async def resolve_ws_user(ws, db) -> dict | None:
    """WebSocket auth: resolve the user from the ``srw_session`` cookie.

    WebSockets can't carry custom Authorization headers in browsers, so the
    cookie BFF is the only practical path for cockpit-initiated WS. Returns
    the user dict on success (with ``is_approved`` set), or None if the
    cookie is missing/invalid. The caller is expected to ``ws.close(4401)``
    on None — we don't take the WS state here, callers vary in whether
    they've already accepted.
    """
    session_id = ws.cookies.get(SESSION_COOKIE)
    if not session_id:
        return None
    try:
        return await _resolve_from_cookie(session_id, db)
    except HTTPException:
        # KC-unavailable surfaces as 503 from the refresh path; a WS
        # handshake has no HTTP response to carry it. Map to None so the
        # caller closes 4401 and the client retries — the session row was
        # kept, so the retry succeeds once KC is back.
        return None


async def cleanup_expired_tokens(db, shutdown_event: asyncio.Event) -> None:
    """Background task that cleans up expired MCP tokens every hour."""
    logger.info("Token cleanup task started")
    while not shutdown_event.is_set():
        try:
            await db.cleanup_expired_mcp_tokens()
            logger.debug("Expired MCP tokens cleanup completed")
        except Exception as e:
            logger.error("Error cleaning up MCP tokens: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=3600.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Token cleanup task stopped")


async def cleanup_expired_sessions(db, shutdown_event: asyncio.Event) -> None:
    """Prune dead BFF, pre-auth, and dependent Canvas viewer state.

    Hourly cadence matches cleanup_expired_tokens. Idle-timeout enforcement
    is handled by the per-request validator; this loop only removes rows
    past absolute lifetime / 7-day revocation tail.
    """
    logger.info("BFF session cleanup task started")
    while not shutdown_event.is_set():
        try:
            await db.cleanup_expired_srw_sessions()
            await db.cleanup_expired_srw_pre_auth()
            # Lazy import keeps the core authentication dependency direction
            # unchanged. Viewer cleanup is safe while the feature is disabled:
            # it removes only already-expired hashed credentials/presence rows.
            from orchestrator.services.canvas_viewer_sessions import (
                CanvasViewerSessionService,
            )

            await CanvasViewerSessionService(db).cleanup()
            logger.debug(
                "Expired BFF sessions / pre-auth / Canvas viewer cleanup completed"
            )
        except Exception as e:
            logger.error("Error cleaning up BFF sessions: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=3600.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("BFF session cleanup task stopped")
