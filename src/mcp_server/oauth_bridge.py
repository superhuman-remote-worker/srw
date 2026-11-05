"""OAuth 2.1 bridge for the MCP server.

Subclasses FastMCP's OIDCProxy to issue srw_* tokens instead of JWTs,
enabling Claude.ai, ChatGPT, and other OAuth-based platforms to connect
while preserving the existing srw_* token system (scoping, expiry, revocation,
audit trail).

Design: knowledge-base/knowledge/features/mcp_oauth_bridge.md
"""

from __future__ import annotations

import hashlib
import html
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
import jwt as pyjwt
from authlib.integrations.httpx_client import AsyncOAuth2Client
from fastmcp.server.auth import AccessToken, OIDCProxy, TokenVerifier
from mcp.server.auth.provider import RefreshToken
from fastmcp.server.auth.oauth_proxy.models import (
    DEFAULT_AUTH_CODE_EXPIRY_SECONDS,
    HTTP_TIMEOUT_SECONDS,
    ClientCode,
)
from fastmcp.server.auth.oauth_proxy.ui import create_error_html
from mcp.server.auth.provider import RegistrationError, TokenError
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)

# Template directory
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


# Redirect URIs a client may send the authorization code to, in FastMCP's
# pattern syntax: a loopback host without a port matches any port and path
# (RFC 8252 §7.3), and an empty or "/" path matches any path.
#
# Without a list FastMCP accepts any safe redirect URI. It also synthesizes a
# client for anyone who presents our upstream client id ("mcp-server", public
# in the chart values) and lets that client redirect anywhere. One click on
# the consent page by a user with a Keycloak session then sends their code to
# an attacker. The list is therefore never left empty or None.
#
# Hosted entries are the callbacks documented by each vendor; every other
# client (Claude Code, Codex, Gemini CLI, VS Code desktop, the MCP Inspector)
# uses a loopback redirect.
DEFAULT_ALLOWED_REDIRECT_URIS: tuple[str, ...] = (
    "https://claude.ai/api/mcp/auth_callback",
    "https://claude.com/api/mcp/auth_callback",
    "https://chatgpt.com/connector_platform_oauth_redirect",
    "https://chatgpt.com/connector/oauth/*",
    "https://vscode.dev/redirect",
    "https://www.cursor.com/agents/mcp/oauth/callback",
    "http://localhost",
    "http://127.0.0.1",
    "http://[::1]",
)


#: Access tokens minted for OAuth clients live this long; the refresh token
#: renews them for as long as the user chose on the consent page. Short, so a
#: leaked access token dies on its own (MCP authorization spec: SHOULD).
DEFAULT_ACCESS_TOKEN_TTL_SECONDS = 3600

#: Refresh tokens are not access tokens: a different prefix (the verifier and
#: the orchestrator accept only ``srw_``) and a different table.
REFRESH_TOKEN_PREFIX = "srwr_"


def _access_token_ttl_seconds() -> int:
    raw = os.environ.get("MCP_OAUTH_ACCESS_TOKEN_TTL_SECONDS", "")
    try:
        value = int(raw) if raw.strip() else DEFAULT_ACCESS_TOKEN_TTL_SECONDS
    except ValueError:
        logger.warning("Ignoring MCP_OAUTH_ACCESS_TOKEN_TTL_SECONDS=%r", raw)
        value = DEFAULT_ACCESS_TOKEN_TTL_SECONDS
    return max(60, min(value, 7 * 86400))


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _epoch(value: Any) -> int | None:
    if not value:
        return None
    return int(datetime.fromisoformat(str(value)).timestamp())


def _split_patterns(raw: str) -> list[str]:
    return [part for part in re.split(r"[\s,]+", raw or "") if part]


def allowed_redirect_uris_from_env(
    env: Any = None,
) -> list[str]:
    """Return the redirect-URI allowlist for the authorization server.

    ``MCP_OAUTH_ALLOWED_REDIRECT_URIS`` replaces the defaults when it names at
    least one pattern; ``MCP_OAUTH_EXTRA_REDIRECT_URIS`` is appended either
    way (self-hosted chat UIs add their callback there). Both take comma- or
    whitespace-separated patterns. A pattern without a scheme is dropped with a
    warning rather than widening the list.
    """
    env = os.environ if env is None else env
    patterns = _split_patterns(env.get("MCP_OAUTH_ALLOWED_REDIRECT_URIS", ""))
    if not patterns:
        patterns = list(DEFAULT_ALLOWED_REDIRECT_URIS)
    patterns += _split_patterns(env.get("MCP_OAUTH_EXTRA_REDIRECT_URIS", ""))

    allowed: list[str] = []
    for pattern in patterns:
        if "://" not in pattern:
            logger.warning(
                "Ignoring redirect URI pattern without a scheme: %r", pattern
            )
            continue
        if pattern not in allowed:
            allowed.append(pattern)
    return allowed


def _load_template() -> str:
    """Load the consent HTML template."""
    path = os.path.join(_TEMPLATE_DIR, "consent.html")
    with open(path) as f:
        return f.read()


def _render_consent(
    *,
    session_id: str,
    csrf_token: str,
    client_name: str,
    user_email: str,
    user_roles: list[str],
    is_admin: bool,
) -> str:
    """Render the consent template with placeholders replaced.

    ``client_name`` comes from unauthenticated Dynamic Client Registration
    and ``user_email`` from IdP claims, so every caller-derived value is
    HTML-escaped (quotes too: two of them land in attribute values). Only
    the ``admin_option`` literal is markup. The substitution is one pass, so
    a value that contains a placeholder is never expanded a second time.
    """
    template = _load_template()
    replacements = {
        "session_id": html.escape(session_id, quote=True),
        "csrf_token": html.escape(csrf_token, quote=True),
        "client_name": html.escape(client_name, quote=True),
        "user_email": html.escape(user_email, quote=True),
        "admin_option": (
            '<option value="all">Full Access (Admin)</option>' if is_admin else ""
        ),
    }
    return _PLACEHOLDER.sub(
        lambda match: replacements.get(match.group(1), match.group(0)), template
    )


class PendingAuth:
    """Temporary storage for in-flight OAuth transactions between Keycloak
    callback and scope selection form submission."""

    __slots__ = (
        "transaction",
        "idp_tokens",
        "user_claims",
        "csrf_token",
        "created_at",
    )

    def __init__(
        self,
        transaction: dict[str, Any],
        idp_tokens: dict[str, Any],
        user_claims: dict[str, Any],
        csrf_token: str,
    ):
        self.transaction = transaction
        self.idp_tokens = idp_tokens
        self.user_claims = user_claims
        self.csrf_token = csrf_token
        self.created_at = time.time()

    @property
    def expired(self) -> bool:
        return time.time() - self.created_at > 600  # 10 minutes


class SRWOAuthProxy(OIDCProxy):
    """OIDC proxy that issues srw_* tokens instead of FastMCP JWTs.

    Keycloak handles identity (login, OIDC discovery, JWKS).
    We handle authorization (scope selection, token creation via orchestrator API).
    """

    def __init__(
        self,
        *,
        mcp_verifier: TokenVerifier,
        api_url: str | None = None,
        internal_key: str | None = None,
        **kwargs: Any,
    ):
        # Disable built-in consent — we show our own after Keycloak login
        kwargs["require_authorization_consent"] = False
        # Never None: None means "any redirect URI" (see the constant above).
        if kwargs.get("allowed_client_redirect_uris") is None:
            kwargs["allowed_client_redirect_uris"] = allowed_redirect_uris_from_env()
        super().__init__(**kwargs)
        self._mcp_verifier = mcp_verifier
        self._api_url = api_url or os.environ.get(
            "COCKPIT_API_URL", "http://localhost:8085"
        )
        self._internal_key = internal_key or os.environ.get("MCP_INTERNAL_KEY", "")
        self._access_ttl = _access_token_ttl_seconds()
        # In-memory store for pending authorizations (between Keycloak callback
        # and scope selection). Short-lived, single-use.
        self._pending_auths: dict[str, PendingAuth] = {}
        # Track recently consumed session IDs so duplicate POSTs get a
        # friendly message instead of "Session Expired".
        self._consumed_sessions: dict[str, float] = {}  # session_id -> consumed_at

    # ------------------------------------------------------------------
    # Route registration — add our custom scope-select endpoint
    # ------------------------------------------------------------------

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        routes = super().get_routes(mcp_path)
        routes.append(
            Route(
                path="/oauth/scope-select",
                endpoint=self._handle_scope_select,
                methods=["POST"],
            )
        )
        return routes

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        """Register a DCR client, logging a refused redirect URI at WARNING.

        FastMCP logs the refusal at DEBUG; a registration aimed at a foreign
        redirect is how the consent-phishing attack starts, so it should show.
        """
        try:
            await super().register_client(client_info)
        except RegistrationError as exc:
            logger.warning(
                "Refused client registration (client_name=%r): %s",
                (getattr(client_info, "client_name", None) or "")[:80],
                getattr(exc, "error_description", None) or exc,
            )
            raise

    # ------------------------------------------------------------------
    # Override: intercept Keycloak callback to show scope/expiry consent
    # ------------------------------------------------------------------

    async def _handle_idp_callback(
        self, request: Request
    ) -> HTMLResponse | RedirectResponse:
        """Handle Keycloak callback: exchange code, show scope selection page."""
        try:
            idp_code = request.query_params.get("code")
            txn_id = request.query_params.get("state")
            error = request.query_params.get("error")

            if error:
                error_description = request.query_params.get("error_description")
                logger.error("IdP callback error: %s - %s", error, error_description)
                return HTMLResponse(
                    content=create_error_html(
                        error_title="OAuth Error",
                        error_message=f"Authentication failed: {error_description or error}",
                    ),
                    status_code=400,
                )

            if not idp_code or not txn_id:
                logger.error("IdP callback missing code or transaction ID")
                return HTMLResponse(
                    content=create_error_html(
                        error_title="OAuth Error",
                        error_message="Missing authorization code or transaction ID.",
                    ),
                    status_code=400,
                )

            # Look up transaction
            transaction_model = await self._transaction_store.get(key=txn_id)
            if not transaction_model:
                logger.error("Invalid transaction ID: %s", txn_id)
                return HTMLResponse(
                    content=create_error_html(
                        error_title="OAuth Error",
                        error_message="Invalid or expired session. Please try again.",
                    ),
                    status_code=400,
                )

            transaction = transaction_model.model_dump()

            # Exchange Keycloak code for tokens (server-side)
            oauth_client = AsyncOAuth2Client(
                client_id=self._upstream_client_id,
                client_secret=self._upstream_client_secret.get_secret_value(),
                token_endpoint_auth_method=self._token_endpoint_auth_method,
                timeout=HTTP_TIMEOUT_SECONDS,
            )

            try:
                idp_redirect_uri = (
                    f"{str(self.base_url).rstrip('/')}{self._redirect_path}"
                )
                token_params: dict[str, Any] = {
                    "url": self._upstream_token_endpoint,
                    "code": idp_code,
                    "redirect_uri": idp_redirect_uri,
                }
                proxy_code_verifier = transaction.get("proxy_code_verifier")
                if proxy_code_verifier:
                    token_params["code_verifier"] = proxy_code_verifier

                exchange_scopes = self._prepare_scopes_for_token_exchange(
                    transaction.get("scopes") or []
                )
                if exchange_scopes:
                    token_params["scope"] = " ".join(exchange_scopes)

                if self._extra_token_params:
                    token_params.update(self._extra_token_params)

                idp_tokens: dict[str, Any] = await oauth_client.fetch_token(
                    **token_params
                )
                logger.debug("Exchanged Keycloak code for tokens (txn=%s)", txn_id)

            except Exception as e:
                logger.error("Keycloak token exchange failed: %s", e)
                return HTMLResponse(
                    content=create_error_html(
                        error_title="OAuth Error",
                        error_message=f"Token exchange with identity provider failed: {e}",
                    ),
                    status_code=500,
                )

            # Decode Keycloak ID token (unverified — we trust Keycloak,
            # the TLS channel, and the authorization code flow)
            id_token_str = idp_tokens.get("id_token", "")
            try:
                user_claims = pyjwt.decode(
                    id_token_str, options={"verify_signature": False}
                )
            except Exception:
                user_claims = {}

            # Fallback: if ID token lacks email, try the access token
            # (Keycloak access tokens are JWTs with user claims too)
            if "email" not in user_claims:
                access_token_str = idp_tokens.get("access_token", "")
                try:
                    at_claims = pyjwt.decode(
                        access_token_str, options={"verify_signature": False}
                    )
                    if "email" in at_claims:
                        user_claims["email"] = at_claims["email"]
                except Exception:
                    pass

            # Get client name from DCR registration
            dcr_client = await self.get_client(transaction["client_id"])
            client_name = (
                getattr(dcr_client, "client_name", None) if dcr_client else None
            ) or "Unknown App"

            # Check admin status from Keycloak realm roles
            realm_access = user_claims.get("realm_access", {})
            roles = realm_access.get("roles", [])
            is_admin = "admin" in roles

            # Store pending auth with CSRF token
            session_id = secrets.token_urlsafe(32)
            csrf_token = secrets.token_urlsafe(32)
            self._pending_auths[session_id] = PendingAuth(
                transaction=transaction,
                idp_tokens=idp_tokens,
                user_claims=user_claims,
                csrf_token=csrf_token,
            )

            # Clean up expired pending auths
            self._cleanup_pending()

            # Clean up the transaction from the store (we've captured it)
            await self._transaction_store.delete(key=txn_id)

            # Show scope/expiry selection page
            consent_html = _render_consent(
                session_id=session_id,
                csrf_token=csrf_token,
                client_name=client_name,
                user_email=user_claims.get("email", "Unknown"),
                user_roles=roles,
                is_admin=is_admin,
            )
            return HTMLResponse(content=consent_html)

        except Exception as e:
            logger.error("Error in IdP callback: %s", e, exc_info=True)
            return HTMLResponse(
                content=create_error_html(
                    error_title="OAuth Error",
                    error_message="Internal error during authentication. Please try again.",
                ),
                status_code=500,
            )

    # ------------------------------------------------------------------
    # Custom endpoint: handle scope/expiry form submission
    # ------------------------------------------------------------------

    async def _handle_scope_select(
        self, request: Request
    ) -> HTMLResponse | RedirectResponse:
        """Process the scope/expiry selection form and redirect to client."""
        try:
            form = await request.form()
            session_id = str(form.get("session_id", ""))
            csrf_token = str(form.get("csrf_token", ""))
            action = str(form.get("action", ""))
            scope = str(form.get("scope", "user"))
            expiry_days = int(form.get("expiry_days", "90") or "90")

            # Look up pending auth
            pending = self._pending_auths.pop(session_id, None)
            if not pending or pending.expired:
                # Check if this was already consumed (duplicate POST)
                if session_id in self._consumed_sessions:
                    return HTMLResponse(
                        content=create_error_html(
                            error_title="Already Authorized",
                            error_message="Authorization was completed successfully. You can close this tab.",
                        ),
                        status_code=200,
                    )
                return HTMLResponse(
                    content=create_error_html(
                        error_title="Session Expired",
                        error_message="Your authorization session has expired. Please try again.",
                    ),
                    status_code=400,
                )

            # Verify CSRF token
            if csrf_token != pending.csrf_token:
                return HTMLResponse(
                    content=create_error_html(
                        error_title="Invalid Request",
                        error_message="CSRF token mismatch. Please try again.",
                    ),
                    status_code=400,
                )

            transaction = pending.transaction

            # Handle denial
            if action == "deny":
                callback_params = {
                    "error": "access_denied",
                    "state": transaction.get("client_state") or "",
                }
                sep = "&" if "?" in transaction["client_redirect_uri"] else "?"
                return RedirectResponse(
                    url=f"{transaction['client_redirect_uri']}{sep}{urlencode(callback_params)}",
                    status_code=302,
                )

            # Validate scope
            if scope not in ("user", "all") and not scope.startswith("project:"):
                scope = "user"

            # Encode scope + expiry in the authorization code's scopes list
            # so exchange_authorization_code can extract them
            code_scopes = [scope, f"expiry:{expiry_days}"]

            # Generate authorization code for the client
            client_code = secrets.token_urlsafe(32)
            code_expires_at = int(time.time() + DEFAULT_AUTH_CODE_EXPIRY_SECONDS)

            # Carry the user claims we already extracted in
            # _handle_idp_callback so exchange_authorization_code
            # doesn't need to re-decode the (possibly opaque) tokens.
            stored_tokens = dict(pending.idp_tokens)
            stored_tokens["_srw_user_claims"] = pending.user_claims

            await self._code_store.put(
                key=client_code,
                value=ClientCode(
                    code=client_code,
                    client_id=transaction["client_id"],
                    redirect_uri=transaction["client_redirect_uri"],
                    code_challenge=transaction["code_challenge"],
                    code_challenge_method=transaction["code_challenge_method"],
                    scopes=code_scopes,
                    idp_tokens=stored_tokens,
                    expires_at=code_expires_at,
                    created_at=time.time(),
                ),
                ttl=DEFAULT_AUTH_CODE_EXPIRY_SECONDS,
            )

            # Redirect to client callback
            client_redirect_uri = transaction["client_redirect_uri"]
            client_state = transaction["client_state"]
            callback_params = {"code": client_code, "state": client_state}
            sep = "&" if "?" in client_redirect_uri else "?"
            callback_url = f"{client_redirect_uri}{sep}{urlencode(callback_params)}"

            # Mark session as consumed so duplicate POSTs get a friendly message
            self._consumed_sessions[session_id] = time.time()

            logger.info(
                "OAuth scope selection complete: scope=%s, expiry=%dd, client=%s",
                scope,
                expiry_days,
                transaction["client_id"][:16],
            )
            return RedirectResponse(url=callback_url, status_code=302)

        except Exception as e:
            logger.error("Error in scope selection: %s", e, exc_info=True)
            return HTMLResponse(
                content=create_error_html(
                    error_title="Error",
                    error_message="Internal error. Please try again.",
                ),
                status_code=500,
            )

    # ------------------------------------------------------------------
    # Override: issue srw_* tokens instead of FastMCP JWTs
    # ------------------------------------------------------------------

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: Any,
    ) -> OAuthToken:
        """Create an srw_* token instead of a FastMCP JWT."""
        # Look up stored code data
        code_model = await self._code_store.get(key=authorization_code.code)
        if not code_model:
            logger.error("Authorization code not found: %s", authorization_code.code)
            raise TokenError("invalid_grant", "Authorization code not found")

        # Single-use
        await self._code_store.delete(key=authorization_code.code)

        # Extract scope and expiry from our encoded scopes
        scope = "user"
        expiry_days = 90
        for s in code_model.scopes or []:
            if s.startswith("expiry:"):
                try:
                    expiry_days = int(s.split(":", 1)[1])
                except (ValueError, IndexError):
                    pass
            elif s in ("user", "all") or s.startswith("project:"):
                scope = s

        # Use pre-extracted user claims (injected by _handle_scope_select)
        # to avoid re-decoding tokens that may be opaque or missing id_token.
        user_info: dict[str, Any] = code_model.idp_tokens.get("_srw_user_claims", {})
        if not isinstance(user_info, dict):
            user_info = {}

        # Fallback: try decoding JWT tokens directly
        if not user_info.get("sub"):
            for token_key in ("id_token", "access_token"):
                raw = code_model.idp_tokens.get(token_key, "")
                if not raw or not isinstance(raw, str):
                    continue
                try:
                    user_info = pyjwt.decode(raw, options={"verify_signature": False})
                    break
                except Exception as exc:
                    logger.warning(
                        "Could not decode Keycloak %s (len=%d): %s",
                        token_key,
                        len(raw),
                        exc,
                    )

        if not user_info.get("sub") and not user_info.get("email"):
            logger.error(
                "No user identity in Keycloak tokens (keys=%s)",
                list(code_model.idp_tokens.keys()),
            )
            raise TokenError("invalid_grant", "Failed to extract user identity")

        # Get client name
        client_name = "unknown"
        if hasattr(client, "client_name") and client.client_name:
            client_name = client.client_name

        # One OAuth connection: a short-lived srw_* access token (an ordinary
        # auth_tokens row) plus a srwr_* refresh token that renews it until
        # the lifetime the user chose. The refresh token used to be another
        # 365-day srw_* access token that FastMCP could never refresh with
        # (G6): every expiry or 401 forced a new login.
        now = datetime.now(timezone.utc)
        grant_expires_at = (
            now + timedelta(days=expiry_days) if expiry_days > 0 else None
        )
        access_expires_at = now + timedelta(seconds=self._access_ttl)
        if grant_expires_at is not None:
            access_expires_at = min(access_expires_at, grant_expires_at)
        access_token = f"srw_{secrets.token_urlsafe(32)}"
        refresh_token = f"{REFRESH_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
        status, _ = await self._internal_post(
            "/api/internal/mcp-oauth-grant",
            {
                "user_sub": user_info.get("sub", ""),
                "user_email": user_info.get("email", ""),
                "name": f"OAuth: {client_name}"[:200],
                "oauth_client_id": client.client_id,
                "scope": scope,
                "origin": f"oauth:{client_name}"[:300],
                "access_token_hash": _token_hash(access_token),
                "access_token_prefix": access_token[:12],
                "access_expires_at": access_expires_at.isoformat(),
                "refresh_token_hash": _token_hash(refresh_token),
                "grant_expires_at": (
                    grant_expires_at.isoformat() if grant_expires_at else None
                ),
            },
        )
        if status != 200:
            raise TokenError("invalid_grant", "Failed to create access token")

        return OAuthToken(
            access_token=access_token,
            token_type="Bearer",
            expires_in=max(1, int((access_expires_at - now).total_seconds())),
            scope=scope,
            refresh_token=refresh_token,
        )

    # ------------------------------------------------------------------
    # Override: refresh tokens are SRW grants, not FastMCP JWTs
    # ------------------------------------------------------------------

    async def load_refresh_token(  # type: ignore[override]
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        """Resolve a srwr_* refresh token to its grant (None = invalid_grant).

        The SDK then checks the grant belongs to the presenting client. An
        orchestrator outage raises instead of answering None: a client told
        ``invalid_grant`` throws the connection away and asks the user to log
        in again, which an outage must not cause.
        """
        if not refresh_token.startswith(REFRESH_TOKEN_PREFIX):
            return None
        status, data = await self._internal_post(
            "/api/internal/mcp-oauth-refresh/lookup",
            {"refresh_token_hash": _token_hash(refresh_token)},
        )
        if status == 404:
            return None
        if status != 200 or not data:
            raise RuntimeError(f"refresh-token lookup failed ({status})")
        return RefreshToken(
            token=refresh_token,
            client_id=str(data["oauth_client_id"]),
            scopes=[str(data["scope"])],
            expires_at=_epoch(data.get("grant_expires_at")),
        )

    async def exchange_refresh_token(  # type: ignore[override]
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        """Rotate: a new access token and a new refresh token, old ones dead."""
        access_token = f"srw_{secrets.token_urlsafe(32)}"
        new_refresh = f"{REFRESH_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
        status, data = await self._internal_post(
            "/api/internal/mcp-oauth-refresh/rotate",
            {
                "refresh_token_hash": _token_hash(refresh_token.token),
                "new_refresh_token_hash": _token_hash(new_refresh),
                "new_access_token_hash": _token_hash(access_token),
                "new_access_token_prefix": access_token[:12],
                "access_ttl_seconds": self._access_ttl,
            },
        )
        if status == 404:
            raise TokenError("invalid_grant", "refresh token is no longer valid")
        if status != 200 or not data:
            raise RuntimeError(f"refresh-token rotation failed ({status})")
        expires_at = _epoch(data.get("access_expires_at"))
        expires_in = (
            max(1, expires_at - int(time.time())) if expires_at else self._access_ttl
        )
        return OAuthToken(
            access_token=access_token,
            token_type="Bearer",
            expires_in=expires_in,
            scope=str(data.get("scope") or " ".join(refresh_token.scopes)),
            refresh_token=new_refresh,
        )

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:  # type: ignore[override]
        """OAuth /revoke: a refresh token ends its whole connection.

        An access token presented here is left alone: the SDK only calls this
        when the token's client matches, and srw_* access tokens carry the
        user id there; the cockpit's token list revokes them.
        """
        if isinstance(token, RefreshToken):
            await self._internal_post(
                "/api/internal/mcp-oauth-refresh/revoke",
                {"refresh_token_hash": _token_hash(token.token)},
            )

    # ------------------------------------------------------------------
    # Override: validate srw_* tokens instead of FastMCP JWTs
    # ------------------------------------------------------------------

    async def load_access_token(self, token: str) -> AccessToken | None:  # type: ignore[override]
        """Validate srw_* tokens via the existing McpTokenVerifier."""
        if token.startswith("srw_"):
            return await self._mcp_verifier.verify_token(token)
        # Fall through: not an srw_* token, not valid for this server
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _internal_post(
        self, path: str, payload: dict[str, Any]
    ) -> tuple[int, dict[str, Any] | None]:
        """POST to an orchestrator internal endpoint: ``(status, json)``.

        A transport failure is status 0, so callers can tell "the
        orchestrator said no" (4xx) from "nobody answered".
        """
        try:
            async with httpx.AsyncClient(
                base_url=self._api_url, timeout=10.0
            ) as client:
                resp = await client.post(
                    path, json=payload, headers={"X-Internal-Key": self._internal_key}
                )
        except httpx.HTTPError as exc:
            logger.error("Orchestrator call %s failed: %s", path, exc)
            return 0, None
        if resp.status_code != 200:
            if resp.status_code >= 500 or resp.status_code in (401, 403):
                logger.error("Orchestrator call %s answered %s", path, resp.status_code)
            return resp.status_code, None
        try:
            return 200, resp.json()
        except ValueError:
            return 0, None

    def _cleanup_pending(self) -> None:
        """Remove expired pending authorizations and stale consumed markers."""
        expired = [sid for sid, p in self._pending_auths.items() if p.expired]
        for sid in expired:
            del self._pending_auths[sid]
        # Clean consumed sessions older than 2 minutes
        cutoff = time.time() - 120
        stale = [sid for sid, ts in self._consumed_sessions.items() if ts < cutoff]
        for sid in stale:
            del self._consumed_sessions[sid]
