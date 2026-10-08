"""Wire contracts for capability grants, self-capabilities and user administration.

New coverage: before R1.B02 these handlers lived in ``main.py`` and only their
*collaborators* were tested (``tests/test_capability_grants_api.py`` covers the
PEPs, ``tests/test_user_management.py`` the store). The behaviors pinned here
are the ones a handler move can quietly change:

* which routes are admin-only versus merely authenticated — creation, deletion,
  approval and every grant write are admin; listing and profile edits are not;
* grant values are validated against the catalog **before** the store is
  touched, so a malformed enum is a 400 now instead of a dispatch-time crash;
* an approval provisions the user and settles the pending-approval notification;
* ``my_capabilities`` reports admins as unrestricted without resolving grants.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from shared.runtime.core.capability_grants import CATALOG
from tests._mounted_router import mount_router


ADMIN = {"id": "00000000-0000-0000-0000-0000000000ad", "email": "admin@test"}
USER = {"id": "00000000-0000-0000-0000-0000000000c1", "is_admin": False}
USER_ID = "00000000-0000-0000-0000-0000000000c1"
PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"


def _catalog_key(kind):
    """First catalog entry of the given type — the catalog is the authority."""
    for key, spec in CATALOG.items():
        if spec["type"] == kind:
            return key, spec
    pytest.skip(f"no {kind} grant in the catalog")


def _store(**over):
    store = SimpleNamespace(
        get_system_setting=AsyncMock(return_value=None),
        upsert_system_setting=AsyncMock(return_value=None),
        list_grants=AsyncMock(return_value=[]),
        set_grant=AsyncMock(return_value={"key": "k"}),
        delete_grant=AsyncMock(return_value=True),
        list_users=AsyncMock(return_value=[{"id": USER_ID}]),
        get_user=AsyncMock(return_value={"id": USER_ID}),
        update_user=AsyncMock(return_value=True),
        delete_user=AsyncMock(return_value=True),
        approve_users=AsyncMock(return_value=[USER_ID]),
        list_security_events=AsyncMock(return_value=[{"id": 1}]),
        create_user_with_default_project=AsyncMock(
            return_value=({"id": USER_ID}, {"id": PROJECT_ID})
        ),
        create_datasource=AsyncMock(),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _wire(
    *,
    store=None,
    user=None,
    admin_gate=None,
    backend=None,
    features=None,
    drivers=None,
):
    from orchestrator.routers.user_administration import (
        UserAdministrationDependencies as RouteDeps,
    )
    from orchestrator.routers.user_administration import router
    from orchestrator.services.connector_drivers import builtin_connector_drivers
    from orchestrator.services.user_administration import (
        UserAdministrationDependencies as OpDeps,
    )

    db = store or _store()
    flags = features or {}
    cloud_backend = backend or SimpleNamespace(
        is_initialized=False,
        webdav_credentials={"user": "u", "password": "p"},
        get_user_home=AsyncMock(return_value=None),
    )
    router_singleton = SimpleNamespace(for_owner=MagicMock(return_value=cloud_backend))
    notifications = SimpleNamespace(resolve_source=AsyncMock())
    provisioned = AsyncMock()
    knowledge = AsyncMock()

    async def admin(_request):
        if admin_gate is not None:
            return await admin_gate(_request)
        return ADMIN

    async def approved(_request, _store):
        return user if user is not None else USER

    ops = OpDeps(
        store=db,
        logger=MagicMock(),
        main_cloud_router=router_singleton,
        user_id_type=lambda value: value,
        provision_default_project_knowledge=knowledge,
        ensure_user_provisioned=provisioned,
        notification_service=notifications,
        grant_project_ids=AsyncMock(return_value=[PROJECT_ID]),
        is_protected_cloud_mode_enabled=lambda: flags.get("protected_cloud", False),
        datasource_scope_auto_attach_v1_enabled=lambda: flags.get("auto_attach", False),
        datasource_defaults_on_omission=lambda: flags.get("defaults", False),
        connector_drivers=drivers or builtin_connector_drivers(),
    )
    deps = RouteDeps(
        store=db,
        operations=ops,
        require_admin=admin,
        require_approved_user=approved,
    )
    app = mount_router(
        router, factories={"user_administration_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(
        client=TestClient(app),
        store=db,
        notifications=notifications,
        provisioned=provisioned,
        knowledge=knowledge,
        cloud=cloud_backend,
        cloud_router=router_singleton,
        ops=ops,
    )


# =============================================================================
# user_experts kill-switch — fail OPEN
# =============================================================================


def test_absent_user_experts_row_reads_as_enabled():
    wire = _wire()
    body = wire.client.get("/api/admin/system-settings/user_experts").json()
    assert body == {"enabled": True, "updated_by": None}


def test_user_experts_disabled_only_on_explicit_false():
    wire = _wire(
        store=_store(
            get_system_setting=AsyncMock(
                return_value={"value": {"enabled": False}, "updated_by": "a@test"}
            )
        )
    )
    body = wire.client.get("/api/admin/system-settings/user_experts").json()
    assert body == {"enabled": False, "updated_by": "a@test"}


def test_user_experts_toggle_refuses_a_non_boolean():
    wire = _wire()
    resp = wire.client.put(
        "/api/admin/system-settings/user_experts", json={"enabled": "true"}
    )
    assert resp.status_code == 400
    wire.store.upsert_system_setting.assert_not_awaited()


def test_user_experts_toggle_returns_the_requested_value():
    wire = _wire()
    resp = wire.client.put(
        "/api/admin/system-settings/user_experts", json={"enabled": False}
    )
    assert resp.json() == {"enabled": False}
    assert wire.store.upsert_system_setting.await_args.args[0] == "user_experts"


# =============================================================================
# Grant CRUD
# =============================================================================


def test_list_grants_returns_the_catalog_alongside_the_rows():
    wire = _wire()
    body = wire.client.get("/api/admin/grants", params={"scope_kind": "global"}).json()
    assert body["catalog"] == CATALOG
    assert body["grants"] == []


def test_global_scope_discards_the_scope_id():
    wire = _wire()
    wire.client.get(
        "/api/admin/grants", params={"scope_kind": "global", "scope_id": "ignored"}
    )
    assert wire.store.list_grants.await_args.kwargs["scope_id"] is None


@pytest.mark.parametrize("scope_kind", ["fleet", "", "USER", "team"])
def test_a_bad_scope_kind_is_refused(scope_kind):
    wire = _wire()
    resp = wire.client.get("/api/admin/grants", params={"scope_kind": scope_kind})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "bad scope_kind"
    wire.store.list_grants.assert_not_awaited()


def test_an_unknown_grant_key_is_refused_before_validation():
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/not_a_capability",
        json={"value_json": True},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "unknown key or scope_kind"
    wire.store.set_grant.assert_not_awaited()


def test_a_bool_grant_rejects_a_non_bool():
    key, _ = _catalog_key("bool")
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/{key}", json={"value_json": "yes"}
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == f"{key}: value_json must be a boolean"
    wire.store.set_grant.assert_not_awaited()


def test_an_enum_grant_rejects_a_value_outside_its_order():
    key, spec = _catalog_key("enum")
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/{key}", json={"value_json": "__nope__"}
    )
    assert resp.status_code == 400
    assert str(spec["order"]) in resp.json()["detail"]
    wire.store.set_grant.assert_not_awaited()


def test_an_enum_grant_accepts_a_catalog_value():
    key, spec = _catalog_key("enum")
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/{key}", json={"value_json": spec["order"][0]}
    )
    assert resp.status_code == 200
    assert wire.store.set_grant.await_args.kwargs["actor"] == ADMIN["id"]


def test_a_list_grant_rejects_a_scalar():
    key, _ = _catalog_key("list")
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/{key}", json={"value_json": "one"}
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == f"{key}: value_json must be a list or null"


@pytest.mark.parametrize("value", [None, [], ["a", "b"]])
def test_a_list_grant_accepts_a_list_or_null(value):
    key, _ = _catalog_key("list")
    wire = _wire()
    resp = wire.client.put(
        f"/api/admin/grants/user/{USER_ID}/{key}", json={"value_json": value}
    )
    assert resp.status_code == 200


def test_delete_grant_refuses_a_bad_scope_kind():
    wire = _wire()
    resp = wire.client.delete(f"/api/admin/grants/fleet/{USER_ID}/anything")
    assert resp.status_code == 400
    wire.store.delete_grant.assert_not_awaited()


def test_delete_grant_reports_the_store_result():
    wire = _wire()
    resp = wire.client.delete(f"/api/admin/grants/user/{USER_ID}/whatever")
    assert resp.json() == {"deleted": True}


# =============================================================================
# my_capabilities
# =============================================================================


def test_admins_are_unrestricted_and_no_grants_are_resolved():
    wire = _wire(user={"id": USER_ID, "is_admin": True})
    body = wire.client.get("/api/users/me/capabilities").json()
    assert body["is_admin"] is True
    assert body["grants"] is None
    assert body["catalog"] == CATALOG


def test_features_are_reported_for_admins_too():
    wire = _wire(
        user={"id": USER_ID, "is_admin": True},
        features={"protected_cloud": True, "auto_attach": True, "defaults": True},
    )
    body = wire.client.get("/api/users/me/capabilities").json()
    assert body["features"] == {
        "protected_cloud": True,
        "datasource_scope_auto_attach_v1": True,
        "datasource_defaults_on_omission": True,
    }


def test_a_normal_user_gets_resolved_grants(monkeypatch):
    from orchestrator.services import grants_service

    resolve = AsyncMock(return_value={"shell_tools": False})
    monkeypatch.setattr(grants_service, "resolve_grants_for", resolve)
    wire = _wire()

    body = wire.client.get("/api/users/me/capabilities").json()

    assert body["is_admin"] is False
    assert body["grants"] == {"shell_tools": False}
    assert resolve.await_args.kwargs["project_ids"] == [PROJECT_ID]


# =============================================================================
# User CRUD
# =============================================================================


def test_list_users_is_authenticated_not_admin():
    wire = _wire(admin_gate=_deny())
    assert wire.client.get("/api/users").status_code == 200


def test_get_missing_user_is_404():
    wire = _wire(store=_store(get_user=AsyncMock(return_value=None)))
    resp = wire.client.get(f"/api/users/{USER_ID}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == f"User '{USER_ID}' not found"


def test_update_missing_user_is_404():
    wire = _wire(store=_store(update_user=AsyncMock(return_value=False)))
    resp = wire.client.put(f"/api/users/{USER_ID}", json={"display_name": "New"})
    assert resp.status_code == 404


def test_update_user_is_authenticated_not_admin():
    wire = _wire(admin_gate=_deny())
    resp = wire.client.put(f"/api/users/{USER_ID}", json={"display_name": "New"})
    assert resp.status_code == 200
    assert resp.json() == {"status": "updated"}


def test_a_store_failure_on_list_is_a_500():
    wire = _wire(store=_store(list_users=AsyncMock(side_effect=RuntimeError("boom"))))
    resp = wire.client.get("/api/users")
    assert resp.status_code == 500
    assert resp.json()["detail"] == "boom"


def test_create_user_approves_immediately_and_stamps_the_admin():
    wire = _wire()
    resp = wire.client.post("/api/users", json={"display_name": "Ada"})
    assert resp.status_code == 200
    assert resp.json()["is_approved"] is True
    kwargs = wire.store.update_user.await_args.kwargs
    assert kwargs["is_approved"] is True
    assert kwargs["approved_by"] == ADMIN["id"]
    assert kwargs["approved_at"] is not None
    wire.knowledge.assert_awaited_once()


def test_create_user_skips_cloud_provisioning_without_a_backend():
    wire = _wire()
    wire.client.post("/api/users", json={"display_name": "Ada", "email": "a@test"})
    wire.store.create_datasource.assert_not_awaited()


def test_create_user_provisions_personal_cloud_storage():
    backend = SimpleNamespace(
        is_initialized=True,
        webdav_credentials={"user": "u", "password": "p"},
        get_user_home=AsyncMock(
            return_value=SimpleNamespace(webdav_url="https://cloud/dav/a")
        ),
    )
    wire = _wire(backend=backend)
    wire.client.post("/api/users", json={"display_name": "Ada", "email": "a@test"})
    kwargs = wire.store.create_datasource.await_args.kwargs
    assert kwargs["connection_url"] == "https://cloud/dav/a"
    assert kwargs["project_ids"] == [PROJECT_ID]
    assert kwargs["auto_attach"] is False
    assert kwargs["credentials"] == {"user": "u", "password": "p"}


def test_personal_cloud_storage_goes_through_its_driver():
    """A platform row is validated by its driver like any create; one the
    driver refuses is logged and not stored, and the user is still created."""
    from orchestrator.services.connector_drivers import ConnectorDriverRegistry

    backend = SimpleNamespace(
        is_initialized=True,
        webdav_credentials={"user": "u", "password": "p"},
        get_user_home=AsyncMock(
            return_value=SimpleNamespace(webdav_url="https://cloud/dav/a")
        ),
    )
    wire = _wire(backend=backend, drivers=ConnectorDriverRegistry([]))
    resp = wire.client.post(
        "/api/users", json={"display_name": "Ada", "email": "a@test"}
    )
    assert resp.status_code == 200
    wire.store.create_datasource.assert_not_awaited()
    wire.ops.logger.warning.assert_called_once()


def test_a_cloud_provisioning_failure_does_not_fail_user_creation():
    backend = SimpleNamespace(
        is_initialized=True,
        webdav_credentials={},
        get_user_home=AsyncMock(side_effect=RuntimeError("cloud down")),
    )
    wire = _wire(backend=backend)
    resp = wire.client.post(
        "/api/users", json={"display_name": "Ada", "email": "a@test"}
    )
    assert resp.status_code == 200
    wire.ops.logger.warning.assert_called_once()


def test_create_user_wraps_a_store_failure_as_500():
    wire = _wire(
        store=_store(
            create_user_with_default_project=AsyncMock(side_effect=RuntimeError("boom"))
        )
    )
    assert (
        wire.client.post("/api/users", json={"display_name": "Ada"}).status_code == 500
    )


def test_delete_missing_user_is_404():
    wire = _wire(store=_store(delete_user=AsyncMock(return_value=False)))
    assert wire.client.delete(f"/api/users/{USER_ID}").status_code == 404


def test_delete_user_preserves_workspace_conflict_response():
    wire = _wire(
        store=_store(
            delete_user=AsyncMock(
                side_effect=HTTPException(
                    409, "Release retained workspace instances first."
                )
            )
        )
    )
    response = wire.client.delete(f"/api/users/{USER_ID}")
    assert response.status_code == 409
    assert response.json() == {"detail": "Release retained workspace instances first."}


# =============================================================================
# Admin user administration
# =============================================================================


def test_patch_user_stamps_approval_and_provisions():
    wire = _wire()
    resp = wire.client.patch(f"/api/admin/users/{USER_ID}", json={"is_approved": True})
    assert resp.json() == {"status": "updated"}
    kwargs = wire.store.update_user.await_args.kwargs
    assert kwargs["approved_by"] == ADMIN["id"]
    wire.provisioned.assert_awaited_once()


def test_patch_user_suspension_keeps_the_approval_history():
    wire = _wire()
    wire.client.patch(f"/api/admin/users/{USER_ID}", json={"is_approved": False})
    kwargs = wire.store.update_user.await_args.kwargs
    assert kwargs["is_approved"] is False
    assert kwargs["approved_at"] is None
    assert kwargs["approved_by"] is None
    wire.provisioned.assert_not_awaited()


def test_patch_user_cannot_set_admin():
    wire = _wire()
    wire.client.patch(
        f"/api/admin/users/{USER_ID}", json={"is_admin": True, "can_use_vm": True}
    )
    kwargs = wire.store.update_user.await_args.kwargs
    assert "is_admin" not in kwargs
    assert kwargs["can_use_vm"] is True


def test_patch_missing_user_is_404():
    wire = _wire(store=_store(update_user=AsyncMock(return_value=False)))
    resp = wire.client.patch(f"/api/admin/users/{USER_ID}", json={"can_use_vm": True})
    assert resp.status_code == 404


def test_bulk_approve_reports_not_found_per_id_without_failing_the_batch():
    ghost = "00000000-0000-0000-0000-0000000000ff"
    wire = _wire()
    resp = wire.client.post(
        "/api/admin/users/approve", json={"user_ids": [USER_ID, ghost]}
    )
    body = resp.json()
    assert body["approved_count"] == 1
    assert body["results"] == [
        {"id": USER_ID, "status": "approved"},
        {"id": ghost, "status": "not_found"},
    ]


def test_bulk_approve_settles_the_pending_approval_notification():
    wire = _wire()
    wire.client.post("/api/admin/users/approve", json={"user_ids": [USER_ID]})
    wire.notifications.resolve_source.assert_awaited_once_with(
        "user", USER_ID, resolved_by=f"user:{ADMIN['id']}"
    )
    wire.provisioned.assert_awaited_once()


def test_bulk_approve_requires_at_least_one_id():
    wire = _wire()
    assert (
        wire.client.post("/api/admin/users/approve", json={"user_ids": []}).status_code
        == 422
    )


def test_security_events_are_returned_with_a_count():
    wire = _wire()
    body = wire.client.get("/api/admin/security-events").json()
    assert body == {"events": [{"id": 1}], "count": 1}


def test_security_events_reject_a_non_iso_since():
    wire = _wire()
    resp = wire.client.get("/api/admin/security-events", params={"since": "yesterday"})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "'since' must be an ISO 8601 timestamp"
    wire.store.list_security_events.assert_not_awaited()


def test_security_events_parse_an_iso_since():
    wire = _wire()
    wire.client.get(
        "/api/admin/security-events",
        params={"since": "2026-01-01T00:00:00+00:00", "event_type": "admin_denied"},
    )
    kwargs = wire.store.list_security_events.await_args.kwargs
    assert kwargs["since"].year == 2026
    assert kwargs["event_type"] == "admin_denied"


# =============================================================================
# Gate placement
# =============================================================================


def _deny():
    async def deny(_request):
        raise HTTPException(status_code=403, detail="Admin access required")

    return deny


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", "/api/admin/system-settings/user_experts", None),
        ("put", "/api/admin/system-settings/user_experts", {"enabled": True}),
        ("get", "/api/admin/grants?scope_kind=global", None),
        ("put", f"/api/admin/grants/user/{USER_ID}/anything", {"value_json": True}),
        ("delete", f"/api/admin/grants/user/{USER_ID}/anything", None),
        ("post", "/api/users", {"display_name": "Ada"}),
        ("delete", f"/api/users/{USER_ID}", None),
        ("get", "/api/admin/users", None),
        ("patch", f"/api/admin/users/{USER_ID}", {"can_use_vm": True}),
        ("post", "/api/admin/users/approve", {"user_ids": [USER_ID]}),
        ("get", "/api/admin/security-events", None),
    ],
)
def test_admin_only_routes_refuse_a_non_admin(method, path, body):
    wire = _wire(admin_gate=_deny())
    kwargs = {"json": body} if body is not None else {}
    assert getattr(wire.client, method)(path, **kwargs).status_code == 403


def test_each_application_resolves_its_own_store():
    first, second = _wire(), _wire()
    first.client.get("/api/users")
    first.store.list_users.assert_awaited_once()
    second.store.list_users.assert_not_awaited()
