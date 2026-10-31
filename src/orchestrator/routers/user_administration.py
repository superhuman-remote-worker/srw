"""HTTP adapters for capability grants, self-capabilities and user administration.

Capability grants — admin CRUD + audit + kill-switch + self-introspection
(User-Defined Experts, Slice 2; decisions 8, 9, 23). The PEPs live in the
application near ``_check_vm_permission``; these are the management/read
surfaces.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from orchestrator.schemas.users import (
    AdminBulkApprove,
    AdminUserUpdate,
    UserCreate,
    UserUpdate,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import user_administration

router = APIRouter()


class GrantSet(BaseModel):
    """Request body for setting a capability grant."""

    value_json: Any


@dataclass(frozen=True)
class UserAdministrationDependencies:
    store: Any
    operations: user_administration.UserAdministrationDependencies
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user


def get_user_administration_dependencies(
    request: Request,
) -> UserAdministrationDependencies:
    return request.app.state.user_administration_dependencies_factory()


# =============================================================================
# User-defined-experts kill-switch and capability grants
# =============================================================================


@router.get("/api/admin/system-settings/user_experts")
async def get_user_experts_settings(
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """Return the global user-defined-experts kill-switch (decision 8).
    Admin-only. Absent row is reported as enabled (fail-open default)."""
    await dependencies.require_admin(request)
    return await user_administration.get_user_experts_settings(
        dependencies=dependencies.operations
    )


@router.put("/api/admin/system-settings/user_experts")
async def put_user_experts_settings(
    body: dict[str, Any],
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """Toggle the user-defined-experts kill-switch. Admin-only. When disabled,
    DB-expert creation and grant enforcement are off (decision 8)."""
    admin = await dependencies.require_admin(request)
    return await user_administration.put_user_experts_settings(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.get("/api/admin/grants")
async def list_grants_endpoint(
    request: Request,
    scope_kind: str,
    scope_id: str | None = None,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict:
    """List the grants set on one scope, plus the catalog. Admin-only."""
    await dependencies.require_admin(request)
    return await user_administration.list_grants(
        scope_kind=scope_kind,
        scope_id=scope_id,
        dependencies=dependencies.operations,
    )


@router.put("/api/admin/grants/{scope_kind}/{scope_id}/{key}")
async def set_grant_endpoint(
    scope_kind: str,
    scope_id: str,
    key: str,
    body: GrantSet,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict:
    """Set/update one capability grant. Admin-only."""
    admin = await dependencies.require_admin(request)
    return await user_administration.set_grant(
        scope_kind=scope_kind,
        scope_id=scope_id,
        key=key,
        value_json=body.value_json,
        admin=admin,
        dependencies=dependencies.operations,
    )


@router.delete("/api/admin/grants/{scope_kind}/{scope_id}/{key}")
async def delete_grant_endpoint(
    scope_kind: str,
    scope_id: str,
    key: str,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict:
    """Revoke one capability grant. Admin-only."""
    await dependencies.require_admin(request)
    return await user_administration.delete_grant(
        scope_kind=scope_kind,
        scope_id=scope_id,
        key=key,
        dependencies=dependencies.operations,
    )


@router.get("/api/users/me/capabilities")
async def my_capabilities(
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict:
    """The caller's effective resolved grants + the catalog (drives editor greying
    in the fast-follow). Admins get null grants (unrestricted)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await user_administration.my_capabilities(
        user=user, dependencies=dependencies.operations
    )


# =============================================================================
# User Endpoints
# =============================================================================


@router.get("/api/users")
async def list_users(
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> list[dict[str, Any]]:
    """List all users (requires authentication)."""
    await dependencies.require_approved_user(request, dependencies.store)
    return await user_administration.list_users(dependencies=dependencies.operations)


@router.get("/api/users/{user_id}")
async def get_user(
    user_id: str,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """Get a single user by ID (requires authentication)."""
    await dependencies.require_approved_user(request, dependencies.store)
    return await user_administration.get_user(
        user_id=user_id, dependencies=dependencies.operations
    )


@router.post("/api/users")
async def create_user(
    body: UserCreate,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """Create a new user with a default project. Admin-only.

    Real users are JIT-provisioned via Keycloak OIDC login (see
    upsert_user_from_oidc). This endpoint is for admin user management
    and tests.
    """
    admin = await dependencies.require_admin(request)
    return await user_administration.create_user(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.put("/api/users/{user_id}")
async def update_user(
    user_id: str,
    body: UserUpdate,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, str]:
    """Update a user's profile: the caller's own, or anyone's for an admin.

    A cross-user edit goes through the admin gate, so a non-admin gets the
    audited ``admin_denied`` 403. E-mail is not editable here (Keycloak
    owns it, and first login links an unlinked row by it).
    """
    caller = await dependencies.require_approved_user(request, dependencies.store)
    if str(caller.get("id") or "").lower() != user_id.lower():
        await dependencies.require_admin(request)
    return await user_administration.update_user(
        user_id=user_id, body=body, dependencies=dependencies.operations
    )


@router.delete("/api/users/{user_id}")
async def delete_user(
    user_id: str,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, str]:
    """Delete a user. Admin-only.

    Self-service deletion isn't exposed yet — it needs Keycloak sync and
    explicit handling of orphaned jobs/threads/project_members. Add a
    separate endpoint if/when the cockpit needs it.
    """
    await dependencies.require_admin(request)
    return await user_administration.delete_user(
        user_id=user_id, dependencies=dependencies.operations
    )


@router.get("/api/admin/users")
async def admin_list_users(
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> list[dict[str, Any]]:
    """List all users including admin/VM flags (admin-only)."""
    await dependencies.require_admin(request)
    return await user_administration.admin_list_users(
        dependencies=dependencies.operations
    )


@router.patch("/api/admin/users/{user_id}")
async def admin_patch_user(
    user_id: str,
    body: AdminUserUpdate,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, str]:
    """Toggle privileged user flags (admin-only).

    Accepts partial updates of ``can_use_vm`` and ``is_approved``. Setting
    ``is_approved=True`` admits the user and stamps ``approved_at``/
    ``approved_by``; setting it False is suspension — the flag flips off
    (effective on the user's next request) while ``approved_at`` is kept as
    history. Admin status is NOT settable here — it is owned by the Keycloak
    ``admin`` realm role (see ``AdminUserUpdate``).
    """
    admin = await dependencies.require_admin(request)
    return await user_administration.admin_patch_user(
        user_id=user_id,
        body=body,
        admin=admin,
        dependencies=dependencies.operations,
    )


@router.post("/api/admin/users/approve")
async def admin_bulk_approve_users(
    body: AdminBulkApprove,
    request: Request,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """Bulk-approve pending users (admin-only).

    Stamps approval on every id that resolves to a real row in a single
    transaction and reports per-id status. This is the workflow Keycloak's
    console can't do (no bulk role assignment). Ids that don't match an
    existing user come back as ``not_found`` rather than failing the batch.
    """
    admin = await dependencies.require_admin(request)
    return await user_administration.admin_bulk_approve_users(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.get("/api/admin/security-events")
async def admin_list_security_events(
    request: Request,
    limit: int = 100,
    user_id: Optional[str] = None,
    event_type: Optional[str] = None,
    since: Optional[str] = None,
    *,
    dependencies: UserAdministrationDependencies = Depends(
        get_user_administration_dependencies
    ),
) -> dict[str, Any]:
    """List denied-access security events, newest first (admin-only).

    The read path for the cross-user 403 audit log — every 403 raised by
    a ``security/access.py`` gate (plus admin-gate and IDE-proxy denials)
    lands in ``security_events``. Filters: ``user_id`` (the denied
    caller), ``event_type`` (``access_denied`` / ``admin_denied``),
    ``since`` (ISO 8601). Rows are pruned on retention
    (``SECURITY_EVENTS_RETENTION_DAYS``, default 90). Design:
    knowledge-base/knowledge/features/security_event_log.md.
    """
    await dependencies.require_admin(request)
    return await user_administration.admin_list_security_events(
        limit=limit,
        user_id=user_id,
        event_type=event_type,
        since=since,
        dependencies=dependencies.operations,
    )
