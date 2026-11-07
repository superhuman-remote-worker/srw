"""MCP and personal access token request contracts."""

from pydantic import BaseModel, Field


class McpTokenCreate(BaseModel):
    """Request body for creating an MCP API token."""

    name: str = Field(..., min_length=1, max_length=100, description="Token label")
    scope: str = Field(default="user", description="'user', 'project:<uuid>', or 'all'")
    expires_in_days: int | None = Field(
        None, description="Days until expiry (null = never)"
    )


class McpTokenVerifyRequest(BaseModel):
    """Internal request from MCP server to verify a token hash."""

    token_hash: str


class McpTokenCreateInternal(BaseModel):
    """Internal request from OAuth bridge to create an srw_* token."""

    user_sub: str = Field(..., description="Keycloak subject ID")
    user_email: str = Field(default="", description="User email for JIT user creation")
    name: str = Field(..., min_length=1, max_length=200)
    token_hash: str
    token_prefix: str
    scope: str = Field(default="user")
    origin: str | None = None
    expires_at: str | None = Field(None, description="ISO 8601 datetime")


class McpOAuthGrantCreate(BaseModel):
    """Internal request from the OAuth bridge: one new OAuth connection.

    Hashes only — the bridge minted both plaintexts and never sends them.
    """

    user_sub: str = Field(..., description="Keycloak subject ID")
    user_email: str = Field(default="", description="User email for JIT user creation")
    name: str = Field(..., min_length=1, max_length=200)
    oauth_client_id: str = Field(..., min_length=1, max_length=2048)
    scope: str = Field(default="user")
    origin: str = Field(..., min_length=1, max_length=300)
    access_token_hash: str = Field(..., min_length=64, max_length=64)
    access_token_prefix: str = Field(..., min_length=1, max_length=20)
    access_expires_at: str = Field(..., description="ISO 8601 datetime")
    refresh_token_hash: str = Field(..., min_length=64, max_length=64)
    grant_expires_at: str | None = Field(
        None, description="ISO 8601; None = the connection never expires"
    )


class McpOAuthRefreshLookup(BaseModel):
    """Internal request from the OAuth bridge naming a refresh token."""

    refresh_token_hash: str = Field(..., min_length=64, max_length=64)


class McpOAuthRefreshRotate(BaseModel):
    """Internal request from the OAuth bridge: swap a connection's tokens."""

    refresh_token_hash: str = Field(..., min_length=64, max_length=64)
    new_refresh_token_hash: str = Field(..., min_length=64, max_length=64)
    new_access_token_hash: str = Field(..., min_length=64, max_length=64)
    new_access_token_prefix: str = Field(..., min_length=1, max_length=20)
    access_ttl_seconds: int = Field(..., ge=60, le=7 * 86400)


VALID_PAT_SCOPES = {
    "jobs:read",
    "jobs:write",
    "chat:read",
    "chat:write",
    "knowledge:read",
    "knowledge:write",
    "admin",
}


class ApiKeyCreate(BaseModel):
    """Request body for creating a Personal Access Token."""

    name: str = Field(..., min_length=1, max_length=100, description="Display name")
    scopes: list[str] = Field(
        default_factory=lambda: ["jobs:read", "chat:read"],
        description="Action scopes — see VALID_PAT_SCOPES",
    )
    expires_in_days: int | None = Field(
        365,
        ge=1,
        le=3650,
        description="Days until expiry (null = never). Default 1 year per design.",
    )
