"""Catalogue wire contracts through real FastAPI routing and authorization.

These cases first passed against the original handlers and now exercise the
domain routers. Stores, provider probes and configuration readers are isolated;
no application lifespan, database, cloud or external provider is started.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from fastapi import HTTPException

from tests._mounted_router import mount_router


ROW_ID = "11111111-1111-4111-8111-111111111111"
ADMIN_ID = "22222222-2222-4222-8222-222222222222"


def endpoint_row(**updates):
    return {
        "id": UUID(ROW_ID),
        "label": "Private endpoint",
        "base_url": "https://models.invalid/v1",
        "api_key": "test-secret-that-must-not-be-on-the-wire",
        "key_prefix": "test-pre",
        "transport_kind": None,
        "created_at": datetime(2026, 9, 7, tzinfo=timezone.utc),
        "updated_at": None,
        "unrelated_extension": "omit-from-endpoint-projection",
        **updates,
    }


def model_row(**updates):
    return {
        "id": UUID(ROW_ID),
        "provider_kind": "endpoint",
        "provider_ref": ROW_ID,
        "model_id": "test-model",
        "display_label": "Test model",
        "capabilities": ["chat", "vision"],
        "family": "default",
        "context_window": None,
        "params_json": None,
        "enabled": True,
        **updates,
    }


@dataclass
class CatalogueHarness:
    app: Any
    store: MagicMock
    resolve_user: AsyncMock
    audit: AsyncMock
    probe: AsyncMock
    subscription_discover: AsyncMock
    subscription_import: AsyncMock

    async def request(self, method: str, path: str, **kwargs):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            return await client.request(method, path, **kwargs)


@pytest.fixture
def catalogue(monkeypatch, tmp_path):
    import yaml

    from orchestrator.routers import config_catalog, model_catalog, provider_catalog
    from orchestrator.security.access import require_admin
    from orchestrator.services import discovery, family_matcher, subscription_discovery
    from orchestrator.services.config_catalog import ConfigCatalogService
    from orchestrator.services.llm_endpoint_probe import ProbeResult
    from orchestrator.services.model_catalog import ModelCatalogService
    from orchestrator.services.provider_catalog import ProviderCatalogService
    from shared.runtime.core import loader

    store = MagicMock()
    for name in (
        "list_system_api_keys",
        "upsert_system_api_key",
        "delete_system_api_key",
        "get_system_api_key",
        "get_system_api_key_discovery_cache",
        "set_system_api_key_discovery_cache",
        "get_system_setting",
        "list_system_llm_endpoints",
        "get_system_llm_endpoint",
        "create_system_llm_endpoint",
        "update_system_llm_endpoint",
        "delete_system_llm_endpoint",
        "get_default_llm_model",
        "set_default_llm_model",
        "list_models",
        "get_model",
        "create_model",
        "update_model",
        "delete_model",
        "list_config_overrides",
        "get_config_override",
        "upsert_config_override",
        "delete_config_override",
    ):
        setattr(store, name, AsyncMock())
    # The readiness auto-pin runs after catalog writes; with every required
    # kind pinned it is a no-op unless a test clears the pins.
    store.list_default_pin_capabilities = AsyncMock(
        return_value=["chat", "auxiliary", "embedding", "rerank"]
    )
    store.list_models_by_capability_alphabetical = AsyncMock(return_value=[])
    store.pin_default_llm_model_if_unset = AsyncMock(return_value=True)
    resolve_user = AsyncMock(
        return_value={"id": ADMIN_ID, "is_admin": False, "real_is_admin": True}
    )
    audit = AsyncMock()
    probe = AsyncMock(
        return_value=ProbeResult(True, 200, None, "https://models.invalid/v1/models")
    )
    subscription_discover = AsyncMock()
    subscription_import = AsyncMock()
    monkeypatch.setattr(
        subscription_discovery, "discover_subscription_models", subscription_discover
    )
    monkeypatch.setattr(
        subscription_discovery, "import_candidates", subscription_import
    )

    async def admin_guard(request):
        return await require_admin(
            request, store, resolve_user=resolve_user, audit=audit
        )

    async def approved_guard(request):
        return await resolve_user(request, store)

    catalog_path = tmp_path / "config" / "prompts" / "catalog.yaml"
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_text(
        yaml.safe_dump(
            [
                {
                    "kind": "settings",
                    "name": "temperature",
                    "type": "number",
                    "min": 0,
                    "max": 2,
                },
                {"kind": "settings", "name": "parallel_tool_calls", "type": "boolean"},
            ]
        )
    )
    provider_deps = provider_catalog.ProviderCatalogDependencies(
        service=ProviderCatalogService(store, discovery, probe, subscription_discovery),
        require_admin=admin_guard,
    )
    model_deps = model_catalog.ModelCatalogDependencies(
        service=ModelCatalogService(
            store=store,
            probe=probe,
            get_config_dir=lambda: tmp_path / "config",
            load_settings_matrix=lambda path: {"default": {}},
            settings_for_family=lambda family, name: 128000,
            family_detector=family_matcher.detect_family,
            reasoning_capability=lambda model: {"method": "none"},
        ),
        require_admin=admin_guard,
        require_approved_user=approved_guard,
    )
    config_deps = config_catalog.ConfigCatalogDependencies(
        service=ConfigCatalogService(
            store=store,
            project_root=lambda: tmp_path,
            settings_for_family=lambda family, name: 128000,
            guardrails_for_family=loader.bundled_guardrails_for_family,
            prompt_resolver=loader.PromptMatrixResolver,
            instruction_resolver=loader.InstructionMatrixResolver,
        ),
        require_admin=admin_guard,
    )
    app = mount_router(
        provider_catalog.router,
        model_catalog.router,
        config_catalog.router,
        factories={
            "provider_catalog_dependencies_factory": lambda: provider_deps,
            "model_catalog_dependencies_factory": lambda: model_deps,
            "config_catalog_dependencies_factory": lambda: config_deps,
        },
    )
    return CatalogueHarness(
        app,
        store,
        resolve_user,
        audit,
        probe,
        subscription_discover,
        subscription_import,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/admin/providers/keys",
        "/api/admin/providers/models",
        "/api/admin/config/overrides",
    ],
)
async def test_admin_gate_denies_before_catalogue_reads(catalogue, path):
    catalogue.resolve_user.return_value = {
        "id": ADMIN_ID,
        "is_admin": True,
        "real_is_admin": False,
    }
    response = await catalogue.request("GET", path)
    assert response.status_code == 403
    assert response.json() == {"detail": "Admin access required"}
    catalogue.audit.assert_awaited_once()
    assert catalogue.audit.await_args.kwargs["resource_id"] == path
    assert catalogue.store.mock_calls == []
    catalogue.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_approved_user_refusal_precedes_public_model_query(catalogue):
    catalogue.resolve_user.side_effect = HTTPException(403, "Account pending approval")
    response = await catalogue.request(
        "GET", "/api/models?project_id=unvalidated-compat-value"
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "Account pending approval"}
    assert catalogue.store.mock_calls == []


@pytest.mark.asyncio
async def test_endpoint_projection_omits_secrets_and_preserves_transport_nulls(
    catalogue,
):
    catalogue.store.list_system_llm_endpoints.return_value = [
        endpoint_row(transport_kind="subscription_proxy")
    ]
    response = await catalogue.request("GET", "/api/admin/providers/endpoints")
    assert response.status_code == 200
    row = response.json()[0]
    assert row["id"] == ROW_ID
    assert row["transport_kind"] == "subscription_proxy"
    assert row["updated_at"] is None and row["models"] == []
    assert "api_key" not in row and "unrelated_extension" not in row
    assert "test-secret" not in response.text
    # A shadow-view admin retains the real privilege flag.
    assert catalogue.resolve_user.return_value["is_admin"] is False


@pytest.mark.asyncio
async def test_endpoint_key_clear_and_url_refusal_preserve_write_order(catalogue):
    catalogue.store.update_system_llm_endpoint.return_value = endpoint_row(
        key_prefix=None
    )
    response = await catalogue.request(
        "PATCH",
        f"/api/admin/providers/endpoints/{ROW_ID}",
        json={"clear_api_key": True, "api_key": None},
    )
    assert response.status_code == 200
    assert (
        catalogue.store.update_system_llm_endpoint.await_args.kwargs["clear_api_key"]
        is True
    )
    catalogue.store.update_system_llm_endpoint.reset_mock()
    response = await catalogue.request(
        "PATCH",
        f"/api/admin/providers/endpoints/{ROW_ID}",
        json={"base_url": "http://models.invalid/v1", "clear_api_key": True},
    )
    assert response.status_code == 400
    catalogue.store.update_system_llm_endpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_discovery_setting_refusal_precedes_secret_read(catalogue):
    catalogue.store.get_system_setting.return_value = {"value": {"enabled": False}}
    response = await catalogue.request(
        "POST", "/api/admin/providers/keys/openai/rediscover"
    )
    assert response.status_code == 409
    catalogue.store.get_system_api_key.assert_not_awaited()
    catalogue.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_defaults_clear_has_actor_and_null_wire_value(catalogue):
    catalogue.store.get_default_llm_model.return_value = None
    response = await catalogue.request(
        "PUT", "/api/admin/providers/defaults/chat", json={"model": ""}
    )
    assert response.status_code == 200
    assert response.json() == {"kind": "chat", "model": None}
    catalogue.store.set_default_llm_model.assert_awaited_once_with(
        "chat", None, updated_by=ADMIN_ID, source="ui"
    )


@pytest.mark.asyncio
async def test_model_patch_distinguishes_null_from_omission_and_zero(catalogue):
    catalogue.store.update_model.return_value = model_row(
        context_window=0, params_json=None, enabled=False
    )
    response = await catalogue.request(
        "PATCH",
        f"/api/admin/providers/models/{ROW_ID}",
        json={"context_window": 0, "params_json": None, "enabled": False},
    )
    assert response.status_code == 200
    catalogue.store.update_model.assert_awaited_once_with(
        ROW_ID, context_window=0, params_json=None, enabled=False, source="ui"
    )
    catalogue.store.get_model.assert_not_awaited()
    row = response.json()
    assert row["context_window"] == 0 and row["enabled"] is False
    assert row["params_json"] is None
    # Keep the inherited truthiness distinction: explicit zero remains on the
    # wire, while the display's effective window falls back to the family.
    assert row["resolved_context_window"] == 128000
    assert row["context_window_source"] == "family_default"


@pytest.mark.asyncio
async def test_model_transport_refusal_precedes_insert(catalogue):
    catalogue.store.get_system_llm_endpoint.return_value = None
    response = await catalogue.request(
        "POST",
        "/api/admin/providers/models",
        json={
            "provider_kind": "endpoint",
            "provider_ref": ROW_ID,
            "model_id": "test-model",
            "display_label": "Test model",
            "capabilities": ["chat"],
            "family": "default",
        },
    )
    assert response.status_code == 400
    assert "No system endpoint" in response.json()["detail"]
    catalogue.store.create_model.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_models_fan_out_with_compat_project_and_no_transport_secrets(
    catalogue,
):
    catalogue.store.list_models.return_value = [model_row()]
    catalogue.store.list_system_llm_endpoints.return_value = [endpoint_row()]
    response = await catalogue.request("GET", "/api/models?project_id=not-a-uuid")
    assert response.status_code == 200
    data = response.json()
    assert data["groups"][0]["models"] == ["test-model"]
    assert data["vision_models"][0]["id"] == "test-model"
    assert data["auxiliary_models"] == []
    assert data["reasoning_by_model"]["test-model"] == {
        "method": "none",
        "default": None,
        "options": [],
    }
    assert "test-secret" not in response.text and "base_url" not in response.text
    catalogue.store.list_models.assert_awaited_once_with(enabled_only=True)


@pytest.mark.asyncio
async def test_override_update_uses_stored_identity_and_preserves_raw_extension(
    catalogue,
):
    catalogue.store.get_config_override.return_value = {
        "family": None,
        "kind": "settings",
        "name": "parallel_tool_calls",
    }
    catalogue.store.upsert_config_override.return_value = {
        "id": UUID(ROW_ID),
        "value_json": False,
        "extension": {"raw": None},
    }
    response = await catalogue.request(
        "PUT",
        f"/api/admin/config/overrides/{ROW_ID}",
        json={
            "family": "ignored",
            "name": "ignored",
            "value_json": False,
            "notes": "test",
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": ROW_ID,
        "value_json": False,
        "extension": {"raw": None},
    }
    catalogue.store.upsert_config_override.assert_awaited_once_with(
        family=None,
        kind="settings",
        name="parallel_tool_calls",
        content=None,
        content_format=None,
        value_json=False,
        notes="test",
        user_id=ADMIN_ID,
    )


@pytest.mark.asyncio
async def test_override_missing_row_precedes_payload_validation(catalogue):
    catalogue.store.get_config_override.return_value = None
    response = await catalogue.request(
        "PUT", f"/api/admin/config/overrides/{ROW_ID}", json={}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "override not found"}
    catalogue.store.upsert_config_override.assert_not_awaited()


@pytest.mark.asyncio
async def test_structured_override_rejects_bool_numeric_value(catalogue):
    response = await catalogue.request(
        "POST",
        "/api/admin/config/overrides",
        json={"kind": "settings", "name": "temperature", "value_json": False},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "temperature must be a number"}
    catalogue.store.upsert_config_override.assert_not_awaited()


@pytest.mark.asyncio
async def test_subscription_discovery_failure_does_not_import_empty_inventory(
    catalogue,
):
    from orchestrator.services.subscription_discovery import DiscoveryResult
    from shared.subscription_routing import SUBSCRIPTION_PROXY_TRANSPORT

    catalogue.store.get_system_llm_endpoint.return_value = endpoint_row(
        transport_kind=SUBSCRIPTION_PROXY_TRANSPORT
    )
    catalogue.store.list_models.return_value = [model_row()]
    catalogue.subscription_discover.return_value = DiscoveryResult(
        False, "https://models.invalid/v1/models", error="inventory unavailable"
    )
    response = await catalogue.request(
        "POST", f"/api/admin/providers/endpoints/{ROW_ID}/models/import", json={}
    )
    assert response.status_code == 502
    assert response.json() == {"detail": "inventory unavailable"}
    catalogue.subscription_import.assert_not_awaited()
    catalogue.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_plain_discovery_keeps_plain_shape_and_refuses_bulk_import(catalogue):
    catalogue.store.get_system_llm_endpoint.return_value = endpoint_row()
    response = await catalogue.request(
        "POST", f"/api/admin/providers/endpoints/{ROW_ID}/discover"
    )
    assert response.status_code == 200
    assert response.json() == {
        "subscription": False,
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": "https://models.invalid/v1/models",
        "models": [],
    }
    catalogue.probe.assert_awaited_once_with(
        base_url="https://models.invalid/v1",
        api_key="test-secret-that-must-not-be-on-the-wire",
    )
    response = await catalogue.request(
        "POST", f"/api/admin/providers/endpoints/{ROW_ID}/models/import", json={}
    )
    assert response.status_code == 400
    catalogue.subscription_discover.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_subscription_routing_metadata_survives_projection(catalogue):
    from shared.subscription_routing import (
        PROTOCOL_OPENAI_RESPONSES,
        merge_routing_into_params,
        routing_params_block,
    )

    params = merge_routing_into_params(
        {"temperature": 0},
        routing_params_block(
            client_protocol=PROTOCOL_OPENAI_RESPONSES,
            subscription_sources=["openai-codex"],
            needs_review=True,
        ),
    )
    catalogue.store.list_models.return_value = [model_row(params_json=params)]
    response = await catalogue.request("GET", "/api/admin/providers/models")
    assert response.status_code == 200
    row = response.json()[0]
    assert row["params_json"] == params
    assert row["client_protocol"] == PROTOCOL_OPENAI_RESPONSES
    assert row["subscription_sources"] == ["openai-codex"]
    assert row["routing_needs_review"] is True


@pytest.mark.asyncio
async def test_subscription_import_passes_selection_and_keeps_partial_outcome(
    catalogue,
):
    from orchestrator.services.subscription_discovery import (
        DiscoveryResult,
        ImportOutcome,
    )
    from shared.subscription_routing import SUBSCRIPTION_PROXY_TRANSPORT

    catalogue.store.get_system_llm_endpoint.return_value = endpoint_row(
        transport_kind=SUBSCRIPTION_PROXY_TRANSPORT
    )
    catalogue.store.list_models.return_value = []
    result = DiscoveryResult(True, "https://models.invalid/v1/models")
    catalogue.subscription_discover.return_value = result
    catalogue.subscription_import.return_value = ImportOutcome(
        created=["new-model"],
        skipped=["existing-model"],
        rejected=[{"id": "video-model", "reason": "unsupported_modality"}],
    )
    response = await catalogue.request(
        "POST",
        f"/api/admin/providers/endpoints/{ROW_ID}/models/import",
        json={
            "model_ids": ["new-model", "existing-model", "video-model"],
            "include_needs_review": True,
        },
    )
    assert response.status_code == 200
    assert response.json() == catalogue.subscription_import.return_value.to_public()
    catalogue.subscription_import.assert_awaited_once_with(
        db=catalogue.store,
        endpoint_id=ROW_ID,
        candidates=result.candidates,
        requested_ids=["new-model", "existing-model", "video-model"],
        include_review=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("created", [["new-model"], []])
async def test_subscription_import_auto_pins_only_when_it_created_rows(
    catalogue, created
):
    from orchestrator.services.subscription_discovery import (
        DiscoveryResult,
        ImportOutcome,
    )
    from shared.subscription_routing import SUBSCRIPTION_PROXY_TRANSPORT

    catalogue.store.get_system_llm_endpoint.return_value = endpoint_row(
        transport_kind=SUBSCRIPTION_PROXY_TRANSPORT
    )
    catalogue.subscription_discover.return_value = DiscoveryResult(
        True, "https://models.invalid/v1/models"
    )
    catalogue.subscription_import.return_value = ImportOutcome(created=created)
    catalogue.store.list_default_pin_capabilities.return_value = []
    catalogue.store.list_models_by_capability_alphabetical.side_effect = lambda cap: (
        [{"model_id": "new-model"}] if cap in ("chat", "auxiliary") else []
    )

    response = await catalogue.request(
        "POST", f"/api/admin/providers/endpoints/{ROW_ID}/models/import", json={}
    )

    assert response.status_code == 200
    pinned = [
        c.args[:2]
        for c in catalogue.store.pin_default_llm_model_if_unset.await_args_list
    ]
    if created:
        assert pinned == [("chat", "new-model"), ("auxiliary", "new-model")]
    else:
        assert pinned == []
        catalogue.store.list_default_pin_capabilities.assert_not_awaited()


@pytest.mark.asyncio
async def test_family_detection_and_bundled_default_use_canonical_readers(catalogue):
    response = await catalogue.request(
        "GET", "/api/admin/families/detect?model_id=nonexistent-family-model"
    )
    assert response.status_code == 200
    assert response.json() == {"family": "default", "source": "fallback"}
    response = await catalogue.request(
        "GET", "/api/admin/config/bundled/_/settings/temperature"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["family"] is None and data["content"] == 128000
    assert data["catalog"]["name"] == "temperature"


@pytest.mark.asyncio
async def test_provider_router_uses_the_application_handling_each_request():
    from orchestrator.routers import provider_catalog
    from orchestrator.services import discovery, subscription_discovery
    from orchestrator.services.provider_catalog import ProviderCatalogService

    apps = []
    stores = []
    for provider in ("openai", "anthropic"):
        store = MagicMock()
        store.get_system_setting = AsyncMock(return_value=None)
        store.list_system_api_keys = AsyncMock(
            return_value=[
                {
                    "id": UUID(ROW_ID),
                    "provider": provider,
                    "key_prefix": "prefix",
                }
            ]
        )
        deps = provider_catalog.ProviderCatalogDependencies(
            service=ProviderCatalogService(
                store, discovery, AsyncMock(), subscription_discovery
            ),
            require_admin=AsyncMock(return_value={"id": ADMIN_ID}),
        )
        apps.append(
            mount_router(
                provider_catalog.router,
                factories={
                    "provider_catalog_dependencies_factory": lambda deps=deps: deps,
                },
            )
        )
        stores.append(store)
    for index in (0, 1, 0):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=apps[index]), base_url="http://test"
        ) as client:
            response = await client.get("/api/admin/providers/keys")
        assert response.json()[0]["provider"] == ("openai", "anthropic")[index]
    assert [store.list_system_api_keys.await_count for store in stores] == [2, 1]
