"""Wire contracts for the admin main-cloud routes.

R1.B04 lane M; main_cloud_as_connectors.md slice 2. The main cloud is
configured by Helm only, so what is pinned here:

* **The page only reports.** ``GET /api/admin/main-cloud`` shows the provider,
  its installation, health, where the configuration comes from and the
  provider support matrix, and no response echoes a secret value.
* **The connection form's API is gone, on purpose.** Each retired route
  answers 410 with the same detail, after the admin gate.
* **Installation authority.** The backfill refuses — 409/503, never a guess —
  unless the live installation has just been re-attested and every named
  provider resolves to exactly one proof.

The admin gate is awaited before anything else on every route.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router

from orchestrator.routers import main_cloud_settings as route_module
from orchestrator.services import main_cloud_settings as ops
from orchestrator.services.cloud import instance_registry

BASE = "/api/admin/system-settings/main_cloud"
PAGE = "/api/admin/main-cloud"
ADMIN = {"id": "admin-1", "email": "admin@example.test"}
SECRET = "super-secret-client-value"
PROOF = "0" * 64
OTHER_PROOF = "f" * 64
ACTIVE_INSTANCE = "4e72e665-1f70-4b69-9804-d981b51416e6"


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #


class _Authority:
    """Stand-in for ``MainCloudBackendInstanceAuthority``.

    ``routing`` is a **property returning a fresh copy**, matching the sealed
    real class. That matters: the GET path stamps ``__secret_fields__`` onto
    the overlay's copy, and only a copying property keeps that synthetic key
    out of ``effective``.
    """

    def __init__(
        self,
        backend_id: str = "opencloud",
        instance_id: str = ACTIVE_INSTANCE,
        proof: str = PROOF,
        routing: dict[str, Any] | None = None,
        secret_refs: dict[str, str] | None = None,
    ) -> None:
        self.backend_id = backend_id
        self.backend_instance_id = instance_id
        self.installation_proof_sha256 = proof
        self.routing_sha256 = "a" * 64
        self.secret_revision = 3
        self._routing = dict(routing or {"base_url": "https://cloud.example"})
        self._secret_refs = dict(
            secret_refs or {"keycloak_client_secret": "env:OC_CLIENT_SECRET"}
        )

    @property
    def routing(self) -> dict[str, Any]:
        return dict(self._routing)

    @property
    def secret_refs(self) -> dict[str, str]:
        return dict(self._secret_refs)


def _authority(*args: Any, **kwargs: Any) -> _Authority:
    return _Authority(*args, **kwargs)


def _active_backend() -> SimpleNamespace:
    return SimpleNamespace(
        backend_id="opencloud",
        backend_instance_id=ACTIVE_INSTANCE,
        is_initialized=True,
        is_configured=True,
        _settings=SimpleNamespace(
            base_url="https://cloud.example",
            public_url="https://cloud.example",
            keycloak_issuer="https://auth.example/realms/srw",
            keycloak_client_id="srw",
            admin_role_claim_value="admin",
            default_quota_bytes=10,
            # A settings object always carries the live secret; nothing in the
            # read path may reach for it.
            keycloak_client_secret=SimpleNamespace(get_secret_value=lambda: SECRET),
        ),
    )


def _router(**over: Any) -> SimpleNamespace:
    router = SimpleNamespace(
        active=_active_backend(),
        active_instance_id=ACTIVE_INSTANCE,
    )
    for key, value in over.items():
        setattr(router, key, value)
    return router


def _store(**over: Any) -> SimpleNamespace:
    store = SimpleNamespace(
        get_active_main_cloud_backend_instance=AsyncMock(return_value=None),
        list_main_cloud_backend_instances=AsyncMock(return_value=[]),
        survey_unstamped_main_cloud_rows=AsyncMock(
            return_value={"projects": [], "threads": []}
        ),
        stamp_main_cloud_instance_authority=AsyncMock(
            return_value={"projects": 0, "threads": 0}
        ),
        delete_system_setting=AsyncMock(return_value=None),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _wire(*, store=None, cloud_router=None, admin_gate=None, replace_installation=""):
    calls = SimpleNamespace(admin=0)
    backing = store if store is not None else _store()

    async def require_admin(_request):
        calls.admin += 1
        if admin_gate is not None:
            return await admin_gate()
        return ADMIN

    operations = ops.MainCloudSettingsDependencies(
        store=backing,
        cloud_router=cloud_router if cloud_router is not None else _router(),
        thread_mount_dependencies=lambda: None,
        replace_installation=replace_installation,
    )
    dependencies = route_module.MainCloudSettingsRouteDependencies(
        operations=operations,
        require_admin=require_admin,
    )
    app = mount_router(
        route_module.router,
        factories={"main_cloud_settings_dependencies_factory": lambda: dependencies},
    )
    return SimpleNamespace(
        client=TestClient(app, raise_server_exceptions=False),
        calls=calls,
        store=backing,
        operations=operations,
    )


def _contains_secret(payload: Any) -> bool:
    return SECRET in json.dumps(payload, default=str)


# --------------------------------------------------------------------------- #
# The admin gate is the first thing every route does
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", PAGE, None),
        ("get", BASE, None),
        ("put", BASE, {"value": "not-an-object"}),
        ("post", f"{BASE}/test", {"value": {"backend_id": "bogus"}}),
        ("post", f"{BASE}/reload", None),
        ("post", f"{BASE}/backfill-instance-authority", None),
        ("post", f"{BASE}/repair-thread-mounts", None),
        ("delete", BASE, None),
    ],
)
def test_a_non_admin_is_refused_before_anything_else(method, path, body):
    async def deny():
        raise HTTPException(status_code=403, detail="Admin access required")

    wired = _wire(admin_gate=deny)
    kwargs = {"json": body} if body is not None else {}

    response = getattr(wired.client, method)(path, **kwargs)

    assert response.status_code == 403
    assert wired.calls.admin == 1


# --------------------------------------------------------------------------- #
# The connection form's API is retired: 410, not 404
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", BASE, None),
        ("put", BASE, {"value": {"backend_id": "nextcloud"}}),
        ("delete", BASE, None),
        ("post", f"{BASE}/test", {"value": {"backend_id": "opencloud"}}),
        ("post", f"{BASE}/reload", None),
    ],
)
def test_the_retired_connection_api_answers_410(method, path, body):
    activate = AsyncMock()
    wired = _wire(store=_store(delete_system_setting=activate))
    kwargs = {"json": body} if body is not None else {}

    response = getattr(wired.client, method)(path, **kwargs)

    assert response.status_code == 410
    assert response.json()["detail"] == route_module.RETIRED_DETAIL
    assert "GET /api/admin/main-cloud" in route_module.RETIRED_DETAIL
    activate.assert_not_awaited()


def test_the_service_has_no_configuration_operation_left():
    for name in (
        "put_main_cloud_settings",
        "test_main_cloud_settings",
        "reload_main_cloud_settings",
        "delete_main_cloud_settings",
        "get_main_cloud_settings",
    ):
        assert not hasattr(ops, name), name


# --------------------------------------------------------------------------- #
# GET /api/admin/main-cloud — the read-only page
# --------------------------------------------------------------------------- #


def _page_backend(**over: Any) -> SimpleNamespace:
    backend = _active_backend()
    backend.health_check = AsyncMock(
        return_value=SimpleNamespace(ok=True, latency_ms=12.345, detail="status=200")
    )
    for key, value in over.items():
        setattr(backend, key, value)
    return backend


def _page_store(*, authority=None, revision=4):
    from datetime import datetime, timezone

    active = {
        "authority": authority
        or _authority(
            routing={
                "base_url": "http://oc.internal",
                "public_url": "https://cloud.example",
            }
        ),
        "activation_revision": revision,
        "activated_at": datetime(2026, 10, 8, tzinfo=timezone.utc),
    }
    return _store(get_active_main_cloud_backend_instance=AsyncMock(return_value=active))


def _helm(monkeypatch, state="matches", detail=""):
    monkeypatch.setattr(
        ops,
        "helm_configuration_status",
        lambda _authority: instance_registry.HelmConfigurationStatus(
            state, "opencloud", detail
        ),
    )


def test_the_page_reports_provider_installation_health_and_matrix(monkeypatch):
    _helm(monkeypatch)
    wired = _wire(store=_page_store(), cloud_router=_router(active=_page_backend()))

    body = wired.client.get(PAGE).json()

    assert body["provider"] == {
        "backend_id": "opencloud",
        "title": "OpenCloud",
        "public_url": "https://cloud.example",
        "backend_instance_id": ACTIVE_INSTANCE,
        "activation_revision": 4,
        "activated_at": "2026-10-08T00:00:00+00:00",
        "initialized": True,
    }
    assert body["health"] == {"ok": True, "latency_ms": 12.3, "detail": "status=200"}
    assert body["configuration"] == {
        "source": "helm",
        "helm": {"state": "matches", "backend_id": "opencloud", "detail": ""},
        "replace_installation": None,
    }
    providers = body["matrix"]["providers"]
    assert {p["backend_id"]: p["active"] for p in providers} == {
        "nextcloud": False,
        "opencloud": True,
    }
    assert len(body["matrix"]["rows"]) == 8
    assert not _contains_secret(body)


def test_the_page_reports_helm_drift_and_a_pending_replacement(monkeypatch):
    _helm(monkeypatch, state="differs", detail="provider")
    wired = _wire(
        store=_page_store(),
        cloud_router=_router(active=_page_backend()),
        replace_installation=ACTIVE_INSTANCE,
    )

    configuration = wired.client.get(PAGE).json()["configuration"]

    assert configuration["helm"] == {
        "state": "differs",
        "backend_id": "opencloud",
        "detail": "provider",
    }
    assert configuration["replace_installation"] == ACTIVE_INSTANCE


def test_a_hanging_health_probe_does_not_hold_the_page(monkeypatch):
    import asyncio

    async def _hang():
        await asyncio.sleep(60)

    _helm(monkeypatch)
    monkeypatch.setattr(ops, "_HEALTH_TIMEOUT_SECONDS", 0.01)
    wired = _wire(
        store=_page_store(),
        cloud_router=_router(active=_page_backend(health_check=_hang)),
    )

    body = wired.client.get(PAGE).json()

    assert body["health"] == {"ok": False, "latency_ms": None, "detail": "unreachable"}


def test_the_page_degrades_when_the_registry_read_fails(monkeypatch):
    _helm(monkeypatch, state="differs", detail="no_active")
    store = _store(
        get_active_main_cloud_backend_instance=AsyncMock(side_effect=RuntimeError("db"))
    )
    wired = _wire(store=store, cloud_router=_router(active=_page_backend()))

    response = wired.client.get(PAGE)

    assert response.status_code == 200
    provider = response.json()["provider"]
    assert provider["backend_id"] == "opencloud"
    assert provider["backend_instance_id"] is None
    assert provider["public_url"] is None


def test_the_page_never_reads_the_settings_secret(monkeypatch):
    _helm(monkeypatch)
    backend = _page_backend()
    backend._settings.keycloak_client_secret = SimpleNamespace(
        get_secret_value=lambda: pytest.fail("the page read a secret")
    )
    wired = _wire(store=_page_store(), cloud_router=_router(active=backend))

    assert wired.client.get(PAGE).status_code == 200


# --------------------------------------------------------------------------- #
# POST .../backfill-instance-authority — an operator operation that stays
# --------------------------------------------------------------------------- #


def _legacy_project(provider="opencloud"):
    return {
        "id": uuid4(),
        "name": "Legacy",
        "status": "active",
        "main_cloud_backend": provider,
    }


def _backfill_store(*, projects=None, threads=None, registry=None, active=None):
    return _store(
        survey_unstamped_main_cloud_rows=AsyncMock(
            return_value={
                "projects": projects if projects is not None else [],
                "threads": threads if threads is not None else [],
            }
        ),
        list_main_cloud_backend_instances=AsyncMock(
            return_value=registry if registry is not None else [_authority()]
        ),
        get_active_main_cloud_backend_instance=AsyncMock(
            return_value=(
                active
                if active is not None
                else {"authority": _authority(), "activation_revision": 2}
            )
        ),
    )


def test_backfill_is_a_noop_without_reattesting_when_nothing_is_unstamped(monkeypatch):
    reattest = AsyncMock(return_value=True)
    monkeypatch.setattr(ops, "reload_active_main_cloud_instance", reattest)
    wired = _wire(store=_backfill_store())

    body = wired.client.post(f"{BASE}/backfill-instance-authority").json()

    assert body["status"] == "noop"
    assert body["applied"] is False
    reattest.assert_not_awaited()


def test_backfill_dry_runs_by_default(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(projects=[_legacy_project()])
    wired = _wire(store=store)

    body = wired.client.post(f"{BASE}/backfill-instance-authority").json()

    assert body["status"] == "dry_run"
    assert body["applied"] is False
    assert body["projects"] == 1
    assert body["plan"][0]["backend_instance_id"] == ACTIVE_INSTANCE
    store.stamp_main_cloud_instance_authority.assert_not_awaited()


def test_backfill_applies_only_when_asked(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(projects=[_legacy_project()])
    store.stamp_main_cloud_instance_authority = AsyncMock(
        return_value={"projects": 1, "threads": 0}
    )
    wired = _wire(store=store)

    body = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true").json()

    assert body["status"] == "ok"
    assert body["applied"] is True
    assert body["projects"] == 1
    store.stamp_main_cloud_instance_authority.assert_awaited_once_with(
        backend_id="opencloud", backend_instance_id=ACTIVE_INSTANCE
    )


def test_backfill_refuses_two_distinct_installations(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(
        projects=[_legacy_project()],
        registry=[
            _authority(),
            _authority(instance_id=str(uuid4()), proof=OTHER_PROOF),
        ],
    )
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 409
    assert "distinct" in response.json()["detail"]
    store.stamp_main_cloud_instance_authority.assert_not_awaited()


def test_backfill_refuses_a_provider_that_is_not_the_active_backend(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(
        projects=[_legacy_project(provider="nextcloud")],
        registry=[_authority(backend_id="nextcloud")],
    )
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 409
    assert "is not the active backend" in response.json()["detail"]


def test_backfill_refuses_when_the_registry_proof_does_not_match(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(
        projects=[_legacy_project()],
        registry=[_authority(proof=OTHER_PROOF)],
    )
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 409
    assert "does not match the installation just attested" in response.json()["detail"]


def test_backfill_refuses_when_reattestation_lost_the_race(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=False)
    )
    store = _backfill_store(projects=[_legacy_project()])
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 409
    store.stamp_main_cloud_instance_authority.assert_not_awaited()


def test_backfill_503s_when_the_live_proof_cannot_be_verified(monkeypatch):
    monkeypatch.setattr(
        ops,
        "reload_active_main_cloud_instance",
        AsyncMock(side_effect=RuntimeError("upstream down")),
    )
    store = _backfill_store(projects=[_legacy_project()])
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 503
    assert "cannot verify the live installation proof" in response.json()["detail"]


def test_backfill_refuses_without_an_active_instance(monkeypatch):
    monkeypatch.setattr(
        ops, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )
    store = _backfill_store(projects=[_legacy_project()])
    store.get_active_main_cloud_backend_instance = AsyncMock(return_value=None)
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/backfill-instance-authority?apply=true")

    assert response.status_code == 409
    assert "no active main-cloud instance" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# Per-invocation dependency resolution
# --------------------------------------------------------------------------- #


def test_a_rebound_router_singleton_is_observed_by_the_next_request(monkeypatch):
    monkeypatch.setattr(
        ops,
        "helm_configuration_status",
        lambda _a: instance_registry.HelmConfigurationStatus("matches"),
    )
    state = {"router": _router(active=_page_backend())}

    def _dependencies():
        async def require_admin(_request):
            return ADMIN

        return route_module.MainCloudSettingsRouteDependencies(
            operations=ops.MainCloudSettingsDependencies(
                store=_store(),
                cloud_router=state["router"],
                thread_mount_dependencies=lambda: None,
            ),
            require_admin=require_admin,
        )

    app = mount_router(
        route_module.router,
        factories={"main_cloud_settings_dependencies_factory": _dependencies},
    )
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get(PAGE).json()["provider"]["backend_id"] == "opencloud"

    swapped = _page_backend(backend_id="nextcloud")
    state["router"] = _router(active=swapped, active_instance_id="other")

    body = client.get(PAGE).json()
    assert body["provider"]["backend_id"] == "nextcloud"
    assert body["provider"]["title"] == "Nextcloud"


# --------------------------------------------------------------------------- #
# The cockpit's fixture is this response
# --------------------------------------------------------------------------- #

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "cockpit/src/app/core/models/fixtures/main-cloud.json"
)
UPDATE = os.environ.get("UPDATE_CONNECTOR_GOLDENS") == "1"


def test_the_cockpit_fixture_is_the_page_response(monkeypatch):
    """The Main cloud page spec renders exactly what the API returns."""
    monkeypatch.setattr(
        ops,
        "helm_configuration_status",
        lambda _authority: instance_registry.HelmConfigurationStatus(
            "matches", "nextcloud"
        ),
    )
    authority = _authority(
        backend_id="nextcloud",
        routing={
            "base_url": "http://srw-nextcloud",
            "public_url": "https://cloud.localhost",
        },
    )
    wired = _wire(
        store=_page_store(authority=authority, revision=1),
        cloud_router=_router(active=_page_backend(backend_id="nextcloud")),
    )
    rendered = json.dumps(wired.client.get(PAGE).json(), indent=2) + "\n"
    if UPDATE:
        FIXTURE.write_text(rendered)
    assert FIXTURE.read_text() == rendered, (
        "the cockpit fixture is stale; regenerate it with "
        "UPDATE_CONNECTOR_GOLDENS=1 python -m pytest "
        "tests/test_b04_lane_m_main_cloud_settings_routes.py"
    )
