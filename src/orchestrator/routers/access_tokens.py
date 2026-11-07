"""HTTP adapters for MCP tokens and personal access tokens.

Note: ``MCP_INTERNAL_KEY`` is read in ``security/access.py`` (helpers
``require_internal`` / ``is_internal_call``) and used by every Track B
(P4b) endpoint. No module-level constant is needed here.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.schemas.tokens import (
    ApiKeyCreate,
    McpTokenCreate,
    McpOAuthGrantCreate,
    McpOAuthRefreshLookup,
    McpOAuthRefreshRotate,
    McpTokenCreateInternal,
    McpTokenVerifyRequest,
)
from orchestrator.security.access import require_internal, require_internal_service
from orchestrator.security.auth import require_approved_user
from orchestrator.services import access_tokens

router = APIRouter()


@dataclass(frozen=True)
class AccessTokenDependencies:
    store: Any
    tokens: access_tokens.AccessTokenDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_internal: Callable[..., Awaitable[Any]] = require_internal
    require_internal_service: Callable[..., Awaitable[Any]] = require_internal_service


def get_access_token_dependencies(request: Request) -> AccessTokenDependencies:
    return request.app.state.access_token_dependencies_factory()


# =============================================================================
# MCP Token Endpoints
# =============================================================================


@router.post("/api/mcp-tokens")
async def create_mcp_token(
    request: Request,
    body: McpTokenCreate,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Generate a new MCP API token. Returns the plaintext token once."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.create_mcp_token(
        user=user, body=body, dependencies=dependencies.tokens
    )


@router.get("/api/mcp-tokens")
async def list_mcp_tokens(
    request: Request,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> list[dict[str, Any]]:
    """List the current user's MCP tokens (no plaintext or hashes)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.list_mcp_tokens(
        user=user, dependencies=dependencies.tokens
    )


@router.delete("/api/mcp-tokens/{token_id}")
async def revoke_mcp_token(
    request: Request,
    token_id: str,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, str]:
    """Revoke an MCP token (soft delete)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.revoke_mcp_token(
        user=user, token_id=token_id, dependencies=dependencies.tokens
    )


@router.post("/api/internal/mcp-token-verify")
async def internal_mcp_token_verify(
    request: Request,
    body: McpTokenVerifyRequest,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Internal endpoint for MCP server to verify a token hash.
    **Internal** (P4b) — requires ``X-Internal-Key``. Ingress strips
    this path.
    """
    await dependencies.require_internal(request)
    return await access_tokens.verify_mcp_token(
        body=body, dependencies=dependencies.tokens
    )


@router.post("/api/internal/mcp-token-create")
async def internal_mcp_token_create(
    request: Request,
    body: McpTokenCreateInternal,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Internal endpoint for OAuth bridge to create an srw_* token.
    **Internal** (P4b) — requires ``X-Internal-Key``. Ingress strips
    this path.

    Looks up the user by Keycloak subject (JIT-creates if needed),
    then creates a token with the given hash, scope, and origin.
    """
    await dependencies.require_internal(request)
    return await access_tokens.create_internal_mcp_token(
        body=body, dependencies=dependencies.tokens
    )


# =============================================================================
# MCP OAuth grants — the bridge's refresh tokens (G6). Internal service calls
# only: the bridge sends the bare X-Internal-Key, never a delegated user.
# =============================================================================


@router.post("/api/internal/mcp-oauth-grant")
async def internal_mcp_oauth_grant_create(
    request: Request,
    body: McpOAuthGrantCreate,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Create an OAuth connection: its access token row and refresh grant."""
    await dependencies.require_internal_service(request)
    return await access_tokens.create_internal_mcp_oauth_grant(
        body=body, dependencies=dependencies.tokens
    )


@router.post("/api/internal/mcp-oauth-refresh/lookup")
async def internal_mcp_oauth_refresh_lookup(
    request: Request,
    body: McpOAuthRefreshLookup,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Resolve a refresh token to its live grant; 404 when there is none."""
    await dependencies.require_internal_service(request)
    return await access_tokens.lookup_internal_mcp_oauth_refresh(
        body=body, dependencies=dependencies.tokens
    )


@router.post("/api/internal/mcp-oauth-refresh/rotate")
async def internal_mcp_oauth_refresh_rotate(
    request: Request,
    body: McpOAuthRefreshRotate,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Swap a connection's refresh and access tokens; 404 = invalid_grant."""
    await dependencies.require_internal_service(request)
    return await access_tokens.rotate_internal_mcp_oauth_refresh(
        body=body, dependencies=dependencies.tokens
    )


@router.post("/api/internal/mcp-oauth-refresh/revoke")
async def internal_mcp_oauth_refresh_revoke(
    request: Request,
    body: McpOAuthRefreshLookup,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, str]:
    """End a connection by its refresh token (OAuth /revoke)."""
    await dependencies.require_internal_service(request)
    return await access_tokens.revoke_internal_mcp_oauth_grant(
        body=body, dependencies=dependencies.tokens
    )


# =============================================================================
# Personal Access Token (PAT) Endpoints — see auth_bff_and_api_tokens.md §3
# =============================================================================
#
# PATs live in the consolidated `auth_tokens` table with kind='api'. The
# legacy MCP-token endpoints (above) keep working unchanged on the same
# table with kind='mcp'. Validator path is shared (see security.auth
# `_resolve_pat`, `_resolve_legacy_mcp_token`).


@router.post("/api/api-keys")
async def create_api_key(
    request: Request,
    body: ApiKeyCreate,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Generate a new Personal Access Token. Plaintext returned once."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.create_api_key(
        user=user, body=body, dependencies=dependencies.tokens
    )


@router.get("/api/api-keys")
async def list_api_keys(
    request: Request,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> list[dict[str, Any]]:
    """List the current user's PATs (no hashes, no plaintext)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.list_api_keys(
        user=user, dependencies=dependencies.tokens
    )


@router.delete("/api/api-keys/{token_id}")
async def revoke_api_key(
    request: Request,
    token_id: str,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, str]:
    """Soft-revoke a PAT."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.revoke_api_key(
        user=user, token_id=token_id, dependencies=dependencies.tokens
    )


@router.post("/api/api-keys/{token_id}/rotate")
async def rotate_api_key(
    request: Request,
    token_id: str,
    *,
    dependencies: AccessTokenDependencies = Depends(get_access_token_dependencies),
) -> dict[str, Any]:
    """Issue a successor PAT.

    Same name + scopes + expiry as the source token. The old row stays
    valid for 24h (cleanup loop revokes it) so an automation can roll
    over without an outage.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await access_tokens.rotate_api_key(
        user=user, token_id=token_id, dependencies=dependencies.tokens
    )
