"""Issue, list, revoke and rotate the two long-lived API credentials.

Both kinds live in the consolidated ``auth_tokens`` table — legacy MCP
tokens with ``kind='mcp'``, personal access tokens with ``kind='api'`` — and
share one validator path in ``security.auth`` (``_resolve_pat``,
``_resolve_legacy_mcp_token``). They are kept together here for the property
that matters at this boundary: a plaintext credential is generated in exactly
one place per kind, returned to the caller exactly once, and never read back
out of the store afterwards.

Authorization is NOT here. Every entry point takes an already-authenticated
``user`` because the HTTP adapter owns the gate; what this module owns is the
scope *policy* over that identity (which scopes a non-admin may request,
which project a project-scoped token may name) and the hashing.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import logging
import secrets
from dataclasses import dataclass
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

from fastapi import HTTPException

from orchestrator.schemas.tokens import (
    ApiKeyCreate,
    McpTokenCreate,
    McpTokenCreateInternal,
    McpTokenVerifyRequest,
    VALID_PAT_SCOPES,
)

logger = logging.getLogger(__name__)


class AccessTokenStore(Protocol):
    """The store methods this domain reaches, and nothing else."""

    async def get_project_members(self, project_id: str) -> list[dict[str, Any]]: ...

    async def create_mcp_token(
        self,
        user_id: str,
        name: str,
        token_hash: str,
        token_prefix: str,
        scope: str = "user",
        expires_at: Any = None,
        origin: str | None = None,
        last_four: str | None = None,
    ) -> dict[str, Any]: ...

    async def list_mcp_tokens(self, user_id: str) -> list[dict[str, Any]]: ...

    async def revoke_mcp_token(self, token_id: str, user_id: str) -> bool: ...

    async def get_mcp_token_by_hash(self, token_hash: str) -> dict[str, Any] | None: ...

    async def update_mcp_token_last_used(self, token_hash: str) -> None: ...

    async def get_user_by_keycloak_sub(self, sub: str) -> dict[str, Any] | None: ...

    async def upsert_user_from_oidc(
        self, sub: str, email: str, display_name: str
    ) -> dict[str, Any]: ...

    async def create_api_key(
        self,
        user_id: str,
        name: str,
        token_hash: str,
        token_prefix: str,
        last_four: str,
        scopes: list[str],
        expires_at: Any = None,
    ) -> dict[str, Any]: ...

    async def list_api_keys(self, user_id: str) -> list[dict[str, Any]]: ...

    async def revoke_api_key(self, token_id: str, user_id: str) -> bool: ...

    async def rotate_api_key(
        self,
        old_id: str,
        user_id: str,
        token_hash: str,
        token_prefix: str,
        last_four: str,
    ) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class AccessTokenDependencies:
    store: AccessTokenStore
    #: Schedules the knowledge vault of an approved user's default project, in
    #: the background (``default_project_knowledge``); unset, nothing is
    #: scheduled. Returns at once; a failure is logged, never raised.
    schedule_default_project_knowledge: Callable[[Mapping[str, Any]], Any] | None = None


def serialize_token_row(row: dict[str, Any]) -> dict[str, Any]:
    """Coerce UUID / datetime values to strings so they JSON-serialise.

    One projection for both kinds, because both are rows of the same table
    and both must reach the wire with the same omissions: whatever the store
    does not select (``token_hash`` above all) simply is not here to leak.
    """
    return {k: str(v) if isinstance(v, (UUID, datetime)) else v for k, v in row.items()}


# =============================================================================
# MCP tokens
# =============================================================================


async def create_mcp_token(
    *,
    user: dict[str, Any],
    body: McpTokenCreate,
    dependencies: AccessTokenDependencies,
) -> dict[str, Any]:
    """Generate a new MCP API token. Returns the plaintext token once."""
    # Validate scope
    scope = body.scope.strip()
    if scope not in ("user", "all") and not scope.startswith("project:"):
        raise HTTPException(
            status_code=400,
            detail="Invalid scope. Use 'user', 'all', or 'project:<uuid>'",
        )
    if scope == "all" and not user.get("real_is_admin", False):
        raise HTTPException(
            status_code=403, detail="Only admins can create full-access tokens"
        )
    if scope.startswith("project:"):
        project_id = scope.split(":", 1)[1]
        members = await dependencies.store.get_project_members(project_id)
        if not any(str(m["user_id"]) == str(user["id"]) for m in members):
            raise HTTPException(status_code=403, detail="Not a member of this project")

    # Generate token
    token = "srw_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    token_prefix = token[:12]

    # Expiration
    expires_at = None
    if body.expires_in_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    row = await dependencies.store.create_mcp_token(
        user_id=str(user["id"]),
        name=body.name,
        token_hash=token_hash,
        token_prefix=token_prefix,
        scope=scope,
        expires_at=expires_at,
    )

    result = serialize_token_row(row)
    result["token"] = token  # Plaintext returned once only
    return result


async def list_mcp_tokens(
    *, user: dict[str, Any], dependencies: AccessTokenDependencies
) -> list[dict[str, Any]]:
    """List the current user's MCP tokens (no plaintext or hashes)."""
    rows = await dependencies.store.list_mcp_tokens(str(user["id"]))
    return [serialize_token_row(r) for r in rows]


async def revoke_mcp_token(
    *, user: dict[str, Any], token_id: str, dependencies: AccessTokenDependencies
) -> dict[str, str]:
    """Revoke an MCP token (soft delete)."""
    revoked = await dependencies.store.revoke_mcp_token(token_id, str(user["id"]))
    if not revoked:
        raise HTTPException(
            status_code=404, detail="Token not found or already revoked"
        )
    return {"status": "revoked"}


async def verify_mcp_token(
    *, body: McpTokenVerifyRequest, dependencies: AccessTokenDependencies
) -> dict[str, Any]:
    """Resolve a token hash to its owner, stamping ``last_used_at``.

    The response deliberately carries identity and scope only: the caller
    already holds the hash it presented, and nothing about the stored row
    (prefix, expiry, the hash itself) is echoed back.
    """
    token_data = await dependencies.store.get_mcp_token_by_hash(body.token_hash)
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Update last used
    await dependencies.store.update_mcp_token_last_used(body.token_hash)

    return {
        "user_id": str(token_data["user_id"]),
        "scope": token_data["scope"],
        "display_name": token_data["display_name"],
    }


async def create_internal_mcp_token(
    *, body: McpTokenCreateInternal, dependencies: AccessTokenDependencies
) -> dict[str, Any]:
    """Create an ``srw_*`` token row from a hash the OAuth bridge already minted.

    The bridge, not this process, generated the plaintext — so unlike
    :func:`create_mcp_token` there is no ``token`` field on the response, and
    there must never be one: this endpoint has never seen the secret.
    """
    # Look up or JIT-create user by Keycloak sub
    user = await dependencies.store.get_user_by_keycloak_sub(body.user_sub)
    if not user:
        # JIT-create via upsert (same as cockpit OIDC login)
        user = await dependencies.store.upsert_user_from_oidc(
            sub=body.user_sub,
            email=body.user_email,
            display_name=body.user_email.split("@")[0]
            if body.user_email
            else "OAuth User",
        )
        # A new row is pending, but one linked by email to an approved account
        # arrives approved: its default project needs its vault like any other
        # admission (the later cockpit login finds the row by sub and never
        # takes the first-login branch).
        schedule = dependencies.schedule_default_project_knowledge
        if user and user.get("is_approved") and schedule is not None:
            try:
                schedule(user)
            except Exception as exc:
                # Best-effort: never fails the token; the backfill retries.
                logger.warning(
                    "Knowledge vault for user %s's default project not scheduled: %s",
                    user.get("id"),
                    exc,
                )
    if not user:
        raise HTTPException(status_code=400, detail="Could not resolve user")

    # Parse expiry
    expires_at = None
    if body.expires_at:
        expires_at = datetime.fromisoformat(body.expires_at)

    row = await dependencies.store.create_mcp_token(
        user_id=str(user["id"]),
        name=body.name,
        token_hash=body.token_hash,
        token_prefix=body.token_prefix,
        scope=body.scope,
        expires_at=expires_at,
        origin=body.origin,
    )

    return serialize_token_row(row)


# =============================================================================
# Personal access tokens (PAT) — see auth_bff_and_api_tokens.md §3
# =============================================================================


async def create_api_key(
    *,
    user: dict[str, Any],
    body: ApiKeyCreate,
    dependencies: AccessTokenDependencies,
) -> dict[str, Any]:
    """Generate a new Personal Access Token. Plaintext returned once."""
    # Validate scope set. `admin` is gated on the user's admin flag.
    requested = set(body.scopes)
    if not requested:
        raise HTTPException(status_code=400, detail="At least one scope required")
    bad = requested - VALID_PAT_SCOPES
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown scopes: {sorted(bad)}",
        )
    if "admin" in requested and not user.get("real_is_admin", False):
        raise HTTPException(
            status_code=403, detail="Only admins can issue admin-scoped tokens"
        )

    token = "ak_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    token_prefix = token[:12]
    last_four = token[-4:]

    expires_at = None
    if body.expires_in_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    row = await dependencies.store.create_api_key(
        user_id=str(user["id"]),
        name=body.name,
        token_hash=token_hash,
        token_prefix=token_prefix,
        last_four=last_four,
        scopes=sorted(requested),
        expires_at=expires_at,
    )
    result = serialize_token_row(row)
    result["token"] = token  # Plaintext — caller must store immediately
    return result


async def list_api_keys(
    *, user: dict[str, Any], dependencies: AccessTokenDependencies
) -> list[dict[str, Any]]:
    """List the current user's PATs (no hashes, no plaintext)."""
    rows = await dependencies.store.list_api_keys(str(user["id"]))
    return [serialize_token_row(r) for r in rows]


async def revoke_api_key(
    *, user: dict[str, Any], token_id: str, dependencies: AccessTokenDependencies
) -> dict[str, str]:
    """Soft-revoke a PAT."""
    revoked = await dependencies.store.revoke_api_key(token_id, str(user["id"]))
    if not revoked:
        raise HTTPException(
            status_code=404, detail="Token not found or already revoked"
        )
    return {"status": "revoked"}


async def rotate_api_key(
    *, user: dict[str, Any], token_id: str, dependencies: AccessTokenDependencies
) -> dict[str, Any]:
    """Issue a successor PAT.

    Same name + scopes + expiry as the source token. The old row stays
    valid for 24h (cleanup loop revokes it) so an automation can roll
    over without an outage.
    """
    token = "ak_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    token_prefix = token[:12]
    last_four = token[-4:]

    row = await dependencies.store.rotate_api_key(
        old_id=token_id,
        user_id=str(user["id"]),
        token_hash=token_hash,
        token_prefix=token_prefix,
        last_four=last_four,
    )
    if not row:
        raise HTTPException(
            status_code=404,
            detail="Token not found, already revoked, or not yours",
        )
    result = serialize_token_row(row)
    result["token"] = token
    return result
