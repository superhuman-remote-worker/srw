"""Admin API contracts for Helm-reconciled provider rows.

The seed Job records which entries it reconciles in the ``helm.reconcile``
manifest; the admin services annotate every provider row with ``source``,
``managed_by_helm`` and ``helm_drift`` from it, stamp Cockpit writes as
``source='ui'``, and expose one overview endpoint for the badges.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.schemas.model_catalog import CatalogModelUpdate
from orchestrator.schemas.provider_catalog import (
    AdminDefaultModelSet,
    ApiKeySet,
    LlmEndpointUpdate,
)
from orchestrator.services.model_catalog import ModelCatalogService
from orchestrator.services.provider_catalog import ProviderCatalogService
from shared.helm_provenance import AUTO_PIN_BREADCRUMB, RECONCILE_MANIFEST_KEY

NOW = datetime(2026, 9, 12, tzinfo=timezone.utc)
MANIFEST = {
    "systemApiKeys": ["openai"],
    "systemEndpoints": ["MiniMax"],
    "models": ["openai/gpt-5-mini", "endpoint:MiniMax/MiniMax-M3"],
    "defaults": ["chat"],
    "applied_at": "2026-09-12T18:00:00+00:00",
}


def _key(provider, source):
    return {
        "id": f"key-{provider}",
        "provider": provider,
        "key_prefix": "sk-xxxxx",
        "label": None,
        "seeded_from": None,
        "source": source,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _endpoint(eid, label, source):
    return {
        "id": eid,
        "label": label,
        "base_url": "https://x/v1",
        "key_prefix": None,
        "transport_kind": None,
        "source": source,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _model(mid, kind, ref, model_id, source):
    return {
        "id": mid,
        "provider_kind": kind,
        "provider_ref": ref,
        "model_id": model_id,
        "display_label": model_id,
        "capabilities": ["chat"],
        "family": "gpt-5",
        "context_window": None,
        "reasoning_level": None,
        "params_json": None,
        "enabled": True,
        "seeded_from": None,
        "source": source,
        "notes": None,
        "created_at": NOW,
        "updated_at": NOW,
    }


def _provider_store(*, manifest=MANIFEST):
    store = MagicMock()
    store.get_system_setting = AsyncMock(
        side_effect=lambda key: (
            {"key": key, "value": manifest, "source": "helm"}
            if key == RECONCILE_MANIFEST_KEY
            else (
                {"key": key, "value": {"model": "MiniMax-M3"}, "source": "ui"}
                if key == "llm.default_chat_model"
                else None
            )
        )
    )
    store.list_system_api_keys = AsyncMock(
        return_value=[_key("openai", "ui"), _key("anthropic", "ui")]
    )
    store.list_system_llm_endpoints = AsyncMock(
        return_value=[
            _endpoint("ep-1", "MiniMax", "helm"),
            _endpoint("ep-2", "Other", "ui"),
        ]
    )
    store.list_models = AsyncMock(
        return_value=[
            _model("m-1", "system", "openai", "gpt-5-mini", "helm"),
            _model("m-2", "endpoint", "ep-1", "MiniMax-M3", "ui"),
            _model("m-3", "endpoint", "ep-2", "other", "ui"),
        ]
    )
    store.upsert_system_api_key = AsyncMock(return_value=_key("openai", "ui"))
    store.update_system_llm_endpoint = AsyncMock(
        return_value=_endpoint("ep-1", "MiniMax", "ui")
    )
    store.set_default_llm_model = AsyncMock()
    store.get_default_llm_model = AsyncMock(return_value="MiniMax-M3")
    return store


def _provider_service(store):
    return ProviderCatalogService(store, MagicMock(), AsyncMock(), MagicMock())


class TestProviderRowsAnnotated:
    @pytest.mark.asyncio
    async def test_keys_carry_source_managed_and_drift(self):
        rows = await _provider_service(_provider_store()).list_provider_keys()
        by = {r["provider"]: r for r in rows}
        assert by["openai"]["source"] == "ui"
        assert by["openai"]["managed_by_helm"] is True
        assert by["openai"]["helm_drift"] is True  # admin wrote a managed row
        assert by["anthropic"]["managed_by_helm"] is False
        assert by["anthropic"]["helm_drift"] is False

    @pytest.mark.asyncio
    async def test_endpoints_annotated_by_label(self):
        rows = await _provider_service(_provider_store()).list_provider_endpoints()
        by = {r["label"]: r for r in rows}
        assert by["MiniMax"]["managed_by_helm"] is True
        assert by["MiniMax"]["helm_drift"] is False
        assert by["Other"]["managed_by_helm"] is False

    @pytest.mark.asyncio
    async def test_no_manifest_means_nothing_managed(self):
        rows = await _provider_service(
            _provider_store(manifest=None)
        ).list_provider_keys()
        assert all(r["managed_by_helm"] is False for r in rows)
        assert all(r["helm_drift"] is False for r in rows)


class TestCockpitWritesAreStampedUi:
    @pytest.mark.asyncio
    async def test_key_rotation(self):
        store = _provider_store()
        await _provider_service(store).set_provider_key(
            "openai", ApiKeySet(api_key="sk-newkey-123456")
        )
        assert store.upsert_system_api_key.await_args.kwargs["source"] == "ui"

    @pytest.mark.asyncio
    async def test_endpoint_update(self):
        store = _provider_store()
        await _provider_service(store).update_provider_endpoint(
            "ep-1", LlmEndpointUpdate(label="MiniMax 2")
        )
        assert store.update_system_llm_endpoint.await_args.kwargs["source"] == "ui"

    @pytest.mark.asyncio
    async def test_default_pin(self):
        store = _provider_store()
        await _provider_service(store).set_provider_default(
            "chat", AdminDefaultModelSet(model="gpt-5-mini"), admin={"id": "admin-1"}
        )
        kwargs = store.set_default_llm_model.await_args.kwargs
        assert kwargs["source"] == "ui"
        assert kwargs["updated_by"] == "admin-1"


class TestHelmManagedOverview:
    @pytest.mark.asyncio
    async def test_overview_shape(self):
        out = await _provider_service(_provider_store()).helm_managed_overview()
        assert out["manifest"]["systemApiKeys"] == ["openai"]
        assert out["applied_at"] == "2026-09-12T18:00:00+00:00"
        keys = {k["provider"]: k for k in out["keys"]}
        assert keys["openai"]["managed_by_helm"] and keys["openai"]["helm_drift"]
        models = {m["model_id"]: m for m in out["models"]}
        # endpoint rows are identified by the endpoint LABEL, not its uuid
        assert models["MiniMax-M3"]["managed_by_helm"] is True
        assert models["MiniMax-M3"]["helm_drift"] is True
        assert models["gpt-5-mini"]["managed_by_helm"] is True
        assert models["gpt-5-mini"]["helm_drift"] is False
        assert models["other"]["managed_by_helm"] is False
        chat = out["defaults"]["chat"]
        assert chat == {
            "model": "MiniMax-M3",
            "source": "ui",
            "managed_by_helm": True,
            "helm_drift": True,
            "auto_pinned": False,
        }
        assert out["defaults"]["embedding"]["managed_by_helm"] is False
        assert out["defaults"]["embedding"]["model"] is None

    @pytest.mark.asyncio
    async def test_auto_pin_is_flagged(self):
        """Admin → Defaults labels a pin the readiness auto-pin chose."""
        store = _provider_store()
        settings = {
            "llm.default_rerank_model": {
                "value": {"model": "qwen3-reranker-8b"},
                "source": "default",
                "updated_by": AUTO_PIN_BREADCRUMB,
            },
            # Same source, but written by a boot-time seeder, not the auto-pin.
            "llm.default_search_model": {
                "value": {"model": "searxng"},
                "source": "default",
                "updated_by": None,
            },
        }
        base = store.get_system_setting.side_effect
        store.get_system_setting = AsyncMock(
            side_effect=lambda key: (
                {"key": key, **settings[key]} if key in settings else base(key)
            )
        )
        out = await _provider_service(store).helm_managed_overview()

        assert out["defaults"]["rerank"]["auto_pinned"] is True
        assert out["defaults"]["rerank"]["model"] == "qwen3-reranker-8b"
        assert out["defaults"]["search"]["auto_pinned"] is False
        assert out["defaults"]["embedding"]["auto_pinned"] is False

    @pytest.mark.asyncio
    async def test_overview_without_manifest(self):
        out = await _provider_service(
            _provider_store(manifest=None)
        ).helm_managed_overview()
        assert out["manifest"] == {
            "systemApiKeys": [],
            "systemEndpoints": [],
            "models": [],
            "defaults": [],
        }
        assert out["applied_at"] is None


def _model_catalog_service(store):
    from orchestrator.services.family_matcher import detect_family
    from shared.runtime.core import loader

    return ModelCatalogService(
        store=store,
        probe=AsyncMock(),
        get_config_dir=lambda: loader.get_project_root() / "config",
        load_settings_matrix=lambda path: {},
        settings_for_family=loader.bundled_settings_for_family,
        family_detector=detect_family,
        reasoning_capability=loader.reasoning_capability,
    )


class TestCatalogRowsAnnotated:
    @pytest.mark.asyncio
    async def test_list_uses_endpoint_labels_for_identity(self):
        store = _provider_store()
        rows = await _model_catalog_service(store).list_catalog_models()
        by = {r["model_id"]: r for r in rows}
        assert by["gpt-5-mini"]["source"] == "helm"
        assert by["gpt-5-mini"]["managed_by_helm"] is True
        assert by["gpt-5-mini"]["helm_drift"] is False
        assert by["MiniMax-M3"]["managed_by_helm"] is True
        assert by["MiniMax-M3"]["helm_drift"] is True
        assert by["other"]["managed_by_helm"] is False

    @pytest.mark.asyncio
    async def test_update_is_stamped_ui(self):
        store = _provider_store()
        store.update_model = AsyncMock(
            return_value=_model("m-1", "system", "openai", "gpt-5-mini", "ui")
        )
        row = await _model_catalog_service(store).update_catalog_model(
            "m-1", CatalogModelUpdate(display_label="GPT-5 mini")
        )
        assert store.update_model.await_args.kwargs["source"] == "ui"
        assert row["source"] == "ui"
        assert row["helm_drift"] is True
