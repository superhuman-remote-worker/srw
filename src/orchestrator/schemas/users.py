"""User profile and administration request contracts."""

from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    """Request body for creating a user."""

    display_name: str = Field(..., description="Display name")
    avatar_color: str = Field("#89b4fa", description="Hex color for avatar")
    email: str | None = Field(None, description="Email address")


class UserUpdate(BaseModel):
    """Request body for updating a user's profile.

    E-mail is intentionally NOT settable here. It comes from the Keycloak
    claims, and the first OIDC login links an unlinked ``users`` row by
    e-mail (``upsert_user_from_oidc``), so a writable e-mail would be an
    account-linking lever. An ``email`` key in the body is ignored.
    """

    display_name: str | None = None
    avatar_color: str | None = None


class AdminUserUpdate(BaseModel):
    """Admin-only update body for toggling privileged user flags.

    Admin status is intentionally NOT settable here. It is derived from the
    Keycloak ``admin`` realm role and reconciled onto ``users.is_admin`` on
    every request (orchestrator/security/auth.py) — a write here would be
    silently clobbered on the user's next request. Admin is therefore granted
    in Keycloak, not the app; the cockpit users page shows it read-only.
    """

    can_use_vm: bool | None = None
    is_approved: bool | None = None


class AdminBulkApprove(BaseModel):
    """Admin-only body for bulk-approving pending users."""

    user_ids: list[str] = Field(..., min_length=1, description="User UUIDs to approve")
