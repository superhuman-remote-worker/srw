"""Capability grants, effective capabilities, and user administration.

Three related surfaces that all answer "who may do what":

* the user-defined-experts kill-switch and the capability-grant CRUD, which are
  the *management* side of the PEPs enforced at save/dispatch time;
* ``my_capabilities``, the caller's own resolved view of those grants;
* user CRUD plus the admin approval/suspension and security-event reads.

Grant values are validated against the catalog before they are stored, so a
malformed enum cannot later crash ``meet()``/``resolve_grants`` at dispatch
time — a write-time refusal instead of a dispatch-time crash.

Nothing here returns credential material. ``create_user`` provisions a personal
WebDAV datasource from the cloud backend's own credentials; those are handed
straight to the datasource store and never appear in the response, which is the
user row.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Protocol

from fastapi import HTTPException

from orchestrator.services.connector_drivers import ConnectorDriverRegistry
from orchestrator.services.connector_drivers.platform import (
    validate_platform_connector,
)
from shared.runtime.core import capability_grants

from orchestrator.schemas.users import (
    AdminBulkApprove,
    AdminUserUpdate,
    UserCreate,
    UserUpdate,
)
from orchestrator.services import grants_service

#: The scopes a capability grant may be attached to.
GRANT_SCOPE_KINDS = ("user", "project", "global")

#: ``system_settings`` key for the user-defined-experts kill-switch.
USER_EXPERTS_SETTING_KEY = "user_experts"


class UserAdministrationStore(Protocol):
    def get_system_setting(self, key: str) -> Awaitable[Mapping[str, Any] | None]: ...
    def upsert_system_setting(
        self, key: str, value: Mapping[str, Any], *, updated_by: str
    ) -> Awaitable[Any]: ...
    def list_grants(
        self, *, scope_kind: str, scope_id: str | None
    ) -> Awaitable[Any]: ...
    def set_grant(
        self,
        *,
        scope_kind: str,
        scope_id: str | None,
        key: str,
        value_json: Any,
        actor: str,
    ) -> Awaitable[Any]: ...
    def delete_grant(
        self, *, scope_kind: str, scope_id: str | None, key: str
    ) -> Awaitable[Any]: ...
    def list_users(self) -> Awaitable[Sequence[Mapping[str, Any]]]: ...
    def get_user(self, user_id: str) -> Awaitable[Mapping[str, Any] | None]: ...
    def delete_user(self, user_id: str) -> Awaitable[Any]: ...
    def approve_users(
        self, user_ids: Sequence[str], *, approved_by: str
    ) -> Awaitable[Sequence[str]]: ...
    def list_security_events(
        self,
        *,
        limit: int,
        user_id: str | None,
        event_type: str | None,
        since: datetime | None,
    ) -> Awaitable[Any]: ...


class UserAdministrationLogger(Protocol):
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


@dataclass(frozen=True)
class UserAdministrationDependencies:
    """Application collaborators, resolved per invocation by the app factory."""

    store: Any
    logger: UserAdministrationLogger
    #: Cloud router; ``for_owner(user)`` selects the owner's backend instance.
    main_cloud_router: Any
    #: ``services.cloud`` newtype wrapping a cloud username.
    user_id_type: Callable[[str], Any]
    #: Seeds the default project's knowledge base for a freshly created user.
    provision_default_project_knowledge: Callable[
        [Mapping[str, Any], Mapping[str, Any]], Awaitable[None]
    ]
    #: Idempotent cloud/Gitea provisioning for a newly approved user row.
    ensure_user_provisioned: Callable[[Mapping[str, Any]], Awaitable[Any]]
    #: Notification authority; used to settle "user pending approval" rows.
    notification_service: Any
    #: Project ids that contribute grants for this user.
    grant_project_ids: Callable[[Mapping[str, Any]], Awaitable[list[str]]]
    #: Deployment feature flags surfaced alongside the grant catalog.
    is_protected_cloud_mode_enabled: Callable[[], bool]
    datasource_scope_auto_attach_v1_enabled: Callable[[], bool]
    datasource_defaults_on_omission: Callable[[], bool]
    #: The application's installed connector drivers; they validate the
    #: personal cloud storage connector a new user gets.
    connector_drivers: ConnectorDriverRegistry


def _actor(admin: Mapping[str, Any]) -> str:
    return admin.get("email") or str(admin.get("id", ""))


# =============================================================================
# User-defined-experts kill-switch
# =============================================================================


async def get_user_experts_settings(
    *, dependencies: UserAdministrationDependencies
) -> dict[str, Any]:
    """Return the global user-defined-experts kill-switch (decision 8).
    Absent row is reported as enabled (fail-open default)."""
    row = await dependencies.store.get_system_setting(USER_EXPERTS_SETTING_KEY)
    value = (row or {}).get("value") or {}
    return {
        "enabled": not (isinstance(value, dict) and value.get("enabled") is False),
        "updated_by": (row or {}).get("updated_by"),
    }


async def put_user_experts_settings(
    *,
    body: Mapping[str, Any],
    admin: Mapping[str, Any],
    dependencies: UserAdministrationDependencies,
) -> dict[str, Any]:
    """Toggle the user-defined-experts kill-switch. When disabled, DB-expert
    creation and grant enforcement are off (decision 8)."""
    enabled = body.get("enabled")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="`enabled` must be a boolean")
    await dependencies.store.upsert_system_setting(
        USER_EXPERTS_SETTING_KEY,
        {"enabled": enabled},
        updated_by=_actor(admin),
    )
    return {"enabled": enabled}


# =============================================================================
# Capability grants
# =============================================================================


def validate_grant_value(key: str, value: Any) -> None:
    """Reject a grant value that doesn't match the catalog type, so a malformed
    enum can't later crash meet()/resolve_grants at dispatch time."""
    spec = capability_grants.CATALOG[key]
    t = spec["type"]
    if t == "bool" and not isinstance(value, bool):
        raise HTTPException(
            status_code=400, detail=f"{key}: value_json must be a boolean"
        )
    if t == "enum" and value not in spec["order"]:
        raise HTTPException(
            status_code=400, detail=f"{key}: value_json must be one of {spec['order']}"
        )
    if t == "list" and not (value is None or isinstance(value, list)):
        raise HTTPException(
            status_code=400, detail=f"{key}: value_json must be a list or null"
        )


async def list_grants(
    *,
    scope_kind: str,
    scope_id: str | None,
    dependencies: UserAdministrationDependencies,
) -> dict:
    """List the grants set on one scope, plus the catalog."""
    if scope_kind not in GRANT_SCOPE_KINDS:
        raise HTTPException(status_code=400, detail="bad scope_kind")

    return {
        "grants": await dependencies.store.list_grants(
            scope_kind=scope_kind,
            scope_id=(None if scope_kind == "global" else scope_id),
        ),
        "catalog": capability_grants.CATALOG,
    }


async def set_grant(
    *,
    scope_kind: str,
    scope_id: str,
    key: str,
    value_json: Any,
    admin: Mapping[str, Any],
    dependencies: UserAdministrationDependencies,
) -> dict:
    """Set/update one capability grant."""
    if key not in capability_grants.CATALOG or scope_kind not in GRANT_SCOPE_KINDS:
        raise HTTPException(status_code=400, detail="unknown key or scope_kind")
    validate_grant_value(key, value_json)
    return {
        "grant": await dependencies.store.set_grant(
            scope_kind=scope_kind,
            scope_id=(None if scope_kind == "global" else scope_id),
            key=key,
            value_json=value_json,
            actor=str(admin["id"]),
        )
    }


async def delete_grant(
    *,
    scope_kind: str,
    scope_id: str,
    key: str,
    dependencies: UserAdministrationDependencies,
) -> dict:
    """Revoke one capability grant."""
    if scope_kind not in GRANT_SCOPE_KINDS:
        raise HTTPException(status_code=400, detail="bad scope_kind")
    return {
        "deleted": await dependencies.store.delete_grant(
            scope_kind=scope_kind,
            scope_id=(None if scope_kind == "global" else scope_id),
            key=key,
        )
    }


async def my_capabilities(
    *, user: Mapping[str, Any], dependencies: UserAdministrationDependencies
) -> dict:
    """The caller's effective resolved grants + the catalog (drives editor greying
    in the fast-follow). Admins get null grants (unrestricted)."""
    features = {
        "protected_cloud": dependencies.is_protected_cloud_mode_enabled(),
        "datasource_scope_auto_attach_v1": (
            dependencies.datasource_scope_auto_attach_v1_enabled()
        ),
        "datasource_defaults_on_omission": (
            dependencies.datasource_defaults_on_omission()
        ),
    }
    if user.get("is_admin"):
        return {
            "is_admin": True,
            "grants": None,
            "catalog": capability_grants.CATALOG,
            "features": features,
        }

    grants = await grants_service.resolve_grants_for(
        dependencies.store,
        user_id=str(user["id"]),
        project_ids=await dependencies.grant_project_ids(user),
    )
    return {
        "is_admin": False,
        "grants": grants,
        "catalog": capability_grants.CATALOG,
        "features": features,
    }


# =============================================================================
# User CRUD
# =============================================================================


async def list_users(
    *, dependencies: UserAdministrationDependencies
) -> list[dict[str, Any]]:
    """List all users."""
    try:
        return await dependencies.store.list_users()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_user(
    *, user_id: str, dependencies: UserAdministrationDependencies
) -> dict[str, Any]:
    """Get a single user by ID."""
    user = await dependencies.store.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return user


async def create_user(
    *,
    body: UserCreate,
    admin: Mapping[str, Any],
    dependencies: UserAdministrationDependencies,
) -> dict[str, Any]:
    """Create a new user with a default project.

    Real users are JIT-provisioned via Keycloak OIDC login (see
    upsert_user_from_oidc). This path is for admin user management and tests.
    """
    store = dependencies.store
    try:
        user, project = await store.create_user_with_default_project(
            display_name=body.display_name,
            avatar_color=body.avatar_color or "#89b4fa",
            email=body.email,
        )
        # Admin creation *is* approval — admit immediately and stamp the
        # approving admin so the audit trail and cockpit status are correct.
        await store.update_user(
            user_id=str(user["id"]),
            is_approved=True,
            approved_at=datetime.now(timezone.utc),
            approved_by=str(admin.get("id")),
        )
        user["is_approved"] = True
        await dependencies.provision_default_project_knowledge(user, project)

        # Create personal WebDAV datasource for the default project.
        # Fresh owner provisioning — resolve via the owner seam (Issue 16).
        backend = dependencies.main_cloud_router.for_owner(user)
        if backend.is_initialized and body.email:
            try:
                # Phase 1: the Nextcloud adapter treats the email as the
                # username (legacy behavior — OIDC-backed setups where the NC
                # username differs from the email are broken here, inherited
                # bug from the pre-refactor code). Phase 2 resolves via
                # resolve_user_identity + get_user_home().handle.
                home = await backend.get_user_home(
                    dependencies.user_id_type(body.email)
                )
                webdav_url = home.webdav_url if home else None
                if webdav_url:
                    # Validated by its driver like any create; a refusal is
                    # logged below and the user is still created.
                    normalized = await validate_platform_connector(
                        dependencies.connector_drivers,
                        "webdav",
                        name="Cloud Storage (Personal)",
                        connection_url=webdav_url,
                        credentials=backend.webdav_credentials,
                    )
                    await store.create_datasource(
                        name="Cloud Storage (Personal)",
                        ds_type="webdav",
                        connection_url=normalized.connection_url,
                        description="Personal cloud storage",
                        credentials=normalized.credentials,
                        scope_mode="projects",
                        auto_attach=False,
                        project_ids=[str(project["id"])],
                    )
            except Exception as e:
                dependencies.logger.warning(
                    f"Failed to create personal cloud storage for user "
                    f"{user['id']}: {e}"
                )

        return user
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def update_user(
    *,
    user_id: str,
    body: UserUpdate,
    dependencies: UserAdministrationDependencies,
) -> dict[str, str]:
    """Update a user."""
    success = await dependencies.store.update_user(
        user_id=user_id,
        display_name=body.display_name,
        avatar_color=body.avatar_color,
        email=body.email,
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return {"status": "updated"}


async def delete_user(
    *, user_id: str, dependencies: UserAdministrationDependencies
) -> dict[str, str]:
    """Delete a user.

    Self-service deletion isn't exposed yet — it needs Keycloak sync and
    explicit handling of orphaned jobs/threads/project_members. Add a
    separate endpoint if/when the cockpit needs it.
    """
    success = await dependencies.store.delete_user(user_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return {"status": "deleted"}


# =============================================================================
# Admin user administration
# =============================================================================


async def admin_list_users(
    *, dependencies: UserAdministrationDependencies
) -> list[dict[str, Any]]:
    """List all users including admin/VM flags."""
    try:
        return await dependencies.store.list_users()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def admin_patch_user(
    *,
    user_id: str,
    body: AdminUserUpdate,
    admin: Mapping[str, Any],
    dependencies: UserAdministrationDependencies,
) -> dict[str, str]:
    """Toggle privileged user flags.

    Accepts partial updates of ``can_use_vm`` and ``is_approved``. Setting
    ``is_approved=True`` admits the user and stamps ``approved_at``/
    ``approved_by``; setting it False is suspension — the flag flips off
    (effective on the user's next request) while ``approved_at`` is kept as
    history. Admin status is NOT settable here — it is owned by the Keycloak
    ``admin`` realm role (see ``AdminUserUpdate``).
    """
    approved_at = None
    approved_by = None
    if body.is_approved is True:
        approved_at = datetime.now(timezone.utc)
        approved_by = str(admin.get("id"))
    success = await dependencies.store.update_user(
        user_id=user_id,
        can_use_vm=body.can_use_vm,
        is_approved=body.is_approved,
        approved_at=approved_at,
        approved_by=approved_by,
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    if body.is_approved is True:
        # Admission just granted — provision cloud/Gitea from the row. The JIT
        # ensures are gated on approval and never re-fire for an app-side
        # approved user, so this is their provisioning path. Idempotent.
        row = await dependencies.store.get_user(user_id)
        if row:
            await dependencies.ensure_user_provisioned(row)
    return {"status": "updated"}


async def admin_bulk_approve_users(
    *,
    body: AdminBulkApprove,
    admin: Mapping[str, Any],
    dependencies: UserAdministrationDependencies,
) -> dict[str, Any]:
    """Bulk-approve pending users.

    Stamps approval on every id that resolves to a real row in a single
    transaction and reports per-id status. This is the workflow Keycloak's
    console can't do (no bulk role assignment). Ids that don't match an
    existing user come back as ``not_found`` rather than failing the batch.
    """
    approved_ids = await dependencies.store.approve_users(
        body.user_ids, approved_by=str(admin.get("id"))
    )
    # Provision cloud/Gitea for each newly-approved user (idempotent; the JIT
    # ensures never re-fire for app-side approvals).
    for uid in approved_ids:
        row = await dependencies.store.get_user(uid)
        if row:
            await dependencies.ensure_user_provisioned(row)
        # Every admin's "new user pending approval" row is settled by whoever
        # approved (D6: resolution is a property of the source).
        await dependencies.notification_service.resolve_source(
            "user", str(uid), resolved_by=f"user:{admin.get('id')}"
        )
    approved_set = set(approved_ids)
    results = [
        {"id": uid, "status": "approved" if uid in approved_set else "not_found"}
        for uid in body.user_ids
    ]
    return {"approved_count": len(approved_set), "results": results}


async def admin_list_security_events(
    *,
    limit: int,
    user_id: Optional[str],
    event_type: Optional[str],
    since: Optional[str],
    dependencies: UserAdministrationDependencies,
) -> dict[str, Any]:
    """List denied-access security events, newest first.

    The read path for the cross-user 403 audit log — every 403 raised by
    a ``security/access.py`` gate (plus admin-gate and IDE-proxy denials)
    lands in ``security_events``. Filters: ``user_id`` (the denied
    caller), ``event_type`` (``access_denied`` / ``admin_denied``),
    ``since`` (ISO 8601). Rows are pruned on retention
    (``SECURITY_EVENTS_RETENTION_DAYS``, default 90). Design:
    knowledge-base/knowledge/features/security_event_log.md.
    """
    since_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="'since' must be an ISO 8601 timestamp"
            )
    events = await dependencies.store.list_security_events(
        limit=limit,
        user_id=user_id,
        event_type=event_type,
        since=since_dt,
    )
    return {"events": events, "count": len(events)}
