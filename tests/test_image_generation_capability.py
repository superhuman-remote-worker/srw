"""The ``image_generation`` catalog capability (image generation WP3).

An image model is a catalog row of its own (0571 NOT VALID + 0572 VALIDATE):
the row's only capability, never a chat or vision row, never required by
readiness, never priced by the OpenRouter token sync, and **pin-only** — its
default never falls back to the first enabled row, so the agent's
``generate_image`` tool appears only after an admin or the user pins a model
(owner decision D1, 2026-11-17). Dispatch delivers the pinned row as the
top-level ``image`` section on the job and the Session path; the resolved
blob seam carries it; redaction strips its key by name.
Design: knowledge-base/knowledge/features/image_generation.md §5 WP3.
"""

from __future__ import annotations

import asyncio
import copy
import os
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import ValidationError

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.database.postgres import PostgresDB  # noqa: E402
from orchestrator.schemas.model_catalog import (  # noqa: E402
    EXCLUSIVE_CATALOG_CAPABILITIES,
    PIN_ONLY_CAPABILITIES,
    VALID_CATALOG_CAPABILITIES,
    CatalogModelCreate,
    CatalogModelUpdate,
    exclusive_capability_problem,
)
from orchestrator.schemas.provider_catalog import VALID_DEFAULT_MODEL_KINDS  # noqa: E402
from orchestrator.security.config_redaction import redact_config_override  # noqa: E402
from orchestrator.seed.llm_config import (  # noqa: E402
    _CAPABILITY_ENUM,
    DEFAULT_PIN_CAPABILITY_BY_KIND,
)
from orchestrator.services import discovery, dispatch_credentials  # noqa: E402
from orchestrator.services.capability_credentials import (  # noqa: E402
    CapabilityCredentials,
)
from orchestrator.services.config_resolver import inject_blob_credentials  # noqa: E402
from orchestrator.services.llm_endpoint_probe import (  # noqa: E402
    _capability_hints,
    is_image_generation_id,
)
from orchestrator.services.openrouter_pricing import _catalog_pricing_pairs  # noqa: E402

REPO = Path(__file__).parent.parent
MIGRATIONS = REPO / "src/orchestrator/database/migrations/app"


# --- schema ------------------------------------------------------------------


def test_0571_swaps_the_capabilities_check_for_an_image_generation_superset():
    sql = " ".join(
        (MIGRATIONS / "0571_image_generation_capability.sql").read_text().split()
    )
    assert "DROP CONSTRAINT IF EXISTS models_capabilities_check" in sql
    assert "'rerank', 'live_voice', 'image_generation'" in sql
    assert ") NOT VALID;" in sql
    assert sql.startswith("-- migration: 0571_image_generation_capability.sql")
    assert "BEGIN;" in sql and "COMMIT;" in sql
    sql = (MIGRATIONS / "0572_validate_image_generation_capability.sql").read_text()
    assert "VALIDATE CONSTRAINT models_capabilities_check" in sql


def test_the_snapshot_spells_the_new_value():
    snapshot = (REPO / "src/orchestrator/database/schema_current.sql").read_text()
    assert "'image_generation'::text" in snapshot


def test_the_capability_is_in_every_list():
    assert "image_generation" in VALID_CATALOG_CAPABILITIES
    assert "image_generation" in PostgresDB._CATALOG_CAPABILITIES
    assert "image_generation" in PostgresDB._PIN_ONLY_CAPABILITIES
    assert "image_generation" in PIN_ONLY_CAPABILITIES
    assert "image_generation" in EXCLUSIVE_CATALOG_CAPABILITIES
    assert "image_generation" in VALID_DEFAULT_MODEL_KINDS
    assert "image_generation" in _CAPABILITY_ENUM
    assert DEFAULT_PIN_CAPABILITY_BY_KIND["image_generation"] == "image_generation"
    chart = (REPO / "helm/templates/orchestrator/llm-seed-configmap.yaml").read_text()
    assert '"image_generation"' in chart


def _create(**overrides):
    body = {
        "provider_kind": "system",
        "provider_ref": "openai",
        "model_id": "gpt-image-2.6-flare",
        "display_label": "GPT Image 2.6 Flare",
        "capabilities": ["image_generation"],
        "family": "default",
        "params_json": {"provider": "openai-images", "ops": ["generate", "edit"]},
    }
    body.update(overrides)
    return CatalogModelCreate(**body)


def test_an_image_row_is_accepted_alone():
    assert _create().capabilities == ["image_generation"]


@pytest.mark.parametrize(
    "caps",
    [
        ["image_generation", "chat"],
        ["vision", "image_generation"],
        ["image_generation", "live_voice"],
    ],
)
def test_image_generation_with_another_capability_contradicts_itself(caps):
    with pytest.raises(ValidationError, match="only capability"):
        _create(capabilities=caps)
    with pytest.raises(ValidationError, match="only capability"):
        CatalogModelUpdate(capabilities=caps)
    with pytest.raises(ValueError, match="only capability"):
        PostgresDB._canonicalize_capabilities(capabilities=caps)


def test_exclusive_problem_names_the_capability():
    assert exclusive_capability_problem(["image_generation"]) is None
    assert exclusive_capability_problem(["chat", "auxiliary"]) is None
    assert "image_generation" in (
        exclusive_capability_problem(["image_generation", "chat"]) or ""
    )
    assert "live_voice" in (exclusive_capability_problem(["live_voice", "chat"]) or "")


# --- defaults: pin-only --------------------------------------------------------


def _resolver_db(*, pin, row_enabled=True, candidates=("gpt-image-2",)):
    db = PostgresDB.__new__(PostgresDB)
    db.get_default_llm_model = AsyncMock(return_value=pin)
    db.resolve_catalog_model = AsyncMock(
        return_value={"enabled": row_enabled} if pin else None
    )
    db.list_models_by_capability_alphabetical = AsyncMock(
        return_value=[{"model_id": m} for m in candidates]
    )
    return db


def test_no_pin_means_no_image_model_even_with_enabled_rows():
    db = _resolver_db(pin=None)
    assert asyncio.run(db.resolve_default_for_capability("image_generation")) is None
    db.list_models_by_capability_alphabetical.assert_not_awaited()


def test_a_valid_pin_resolves_and_a_disabled_pin_does_not_fall_back():
    db = _resolver_db(pin="gpt-image-2.6-flare")
    assert (
        asyncio.run(db.resolve_default_for_capability("image_generation"))
        == "gpt-image-2.6-flare"
    )
    db = _resolver_db(pin="gpt-image-2.6-flare", row_enabled=False)
    assert asyncio.run(db.resolve_default_for_capability("image_generation")) is None


def test_other_capabilities_keep_the_first_enabled_fallback():
    db = _resolver_db(pin=None, candidates=("searxng",))
    assert asyncio.run(db.resolve_default_for_capability("search")) == "searxng"


# --- discovery and pricing -----------------------------------------------------


@pytest.mark.parametrize(
    "model_id",
    [
        "gpt-image-2",
        "gpt-image-2.6-sunburst",
        "gpt-image-1.5",
        "dall-e-3",
        "gemini-3-pro-image",
        "gemini-3.1-flash-image",
        "gemini-nano-banana-2.1",
        "imagen-4.0-generate",
        "chatgpt-image-latest",
    ],
)
def test_discovery_and_probe_classify_image_models_before_chat_and_vision(model_id):
    assert is_image_generation_id(model_id)
    assert discovery._capabilities_from_model_id(model_id) == ["image_generation"]
    assert _capability_hints(model_id) == ["image_generation"]


@pytest.mark.parametrize(
    "model_id",
    ["grok-imagine-video", "gpt-4o", "gemini-2.5-pro", "llama-3.2-11b-vision"],
)
def test_video_and_vision_models_are_not_image_generators(model_id):
    assert not is_image_generation_id(model_id)
    assert "image_generation" not in discovery._capabilities_from_model_id(model_id)


def test_pricing_sync_skips_image_rows():
    pairs = _catalog_pricing_pairs(
        [
            {
                "model_id": "gpt-image-2",
                "capabilities": ["image_generation"],
                "params_json": {},
            },
            {"model_id": "gpt-6", "capabilities": ["chat"], "params_json": {}},
        ]
    )
    assert pairs == [("gpt-6", None)]


# --- dispatch ------------------------------------------------------------------


def _deps():
    return Mock(store=Mock(), logger=Mock())


def _run(creds, config=None, user_settings=None):
    config = {} if config is None else config
    resolver = AsyncMock(return_value=creds)
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        resolver,
    ):
        out = asyncio.run(
            dispatch_credentials.inject_image_credentials(
                config,
                user_settings=user_settings or {},
                user_id="u",
                resolved_keys={},
                dependencies=_deps(),
            )
        )
    assert resolver.await_count == 1
    assert resolver.await_args.kwargs["capability"] == "image_generation"
    return out


def test_injects_the_pinned_proxy_row_as_a_subscription_image_section():
    creds = CapabilityCredentials(
        model="gpt-image-2",
        base_url="http://srw-codex-proxy:8317/v1",
        api_key="proxy-secret",
        provider="openai-images",
        params={"provider": "openai-images", "ops": ["generate", "edit"]},
        catalog_id="row-1",
        transport_kind="subscription-proxy",
    )
    assert _run(creds)["image"] == {
        "provider": "openai-images",
        "base_url": "http://srw-codex-proxy:8317/v1",
        "api_key": "proxy-secret",
        "model": "gpt-image-2",
        "catalog_model": "gpt-image-2",
        "ops": ["generate", "edit"],
        "billing": "subscription",
        "tls_pinned_cert": None,
        "max_paid_images": 20,
    }


def test_a_system_openai_row_maps_to_the_openai_adapter_and_api_billing():
    creds = CapabilityCredentials(
        model="gpt-image-2.6-flare",
        base_url=None,
        api_key="sk-openai",
        provider="openai",
        params={"image_defaults": {"quality": "low"}},
        served_model="gpt-image-2.6-flare",
    )
    section = _run(creds)["image"]
    assert section["provider"] == "openai-images"
    assert section["ops"] == ["generate", "edit"]
    assert section["billing"] == "api"
    assert section["base_url"] is None
    assert section["defaults"] == {"quality": "low"}


def test_a_google_row_maps_to_the_gemini_adapter():
    creds = CapabilityCredentials(
        model="gemini-nano-banana-2.1",
        api_key="g-key",
        provider="google",
        params={},
    )
    assert _run(creds)["image"]["provider"] == "gemini"


def test_nothing_pinned_removes_a_stale_section():
    out = _run(None, config={"image": {"provider": "stale"}})
    assert "image" not in out


def test_an_unknown_adapter_disables_the_tool():
    creds = CapabilityCredentials(
        model="flux-pro", provider="fal", params={"provider": "fal"}
    )
    out = _run(creds, config={"image": {"provider": "stale"}})
    assert "image" not in out


def test_the_section_survives_the_resolved_blob_seam_and_redaction():
    from orchestrator.services.config_resolver import resolve_config
    from shared.runtime.core.loader import load_config_from_resolved

    blob = resolve_config(
        base_config_name="persistent_defaults",
        request_override={"workspace": {"backend": "sandbox"}},
    )
    original = copy.deepcopy(blob["agent"].get("image"))
    section = {
        "provider": "openai-images",
        "base_url": "https://api.openai.com/v1",
        "api_key": "sk-image",
        "model": "gpt-image-2.6-flare",
        "catalog_model": "gpt-image-2.6-flare",
        "ops": ["generate", "edit"],
        "billing": "api",
    }

    async def injector(co):
        co["image"] = dict(section)
        return co

    delivered = asyncio.run(inject_blob_credentials(blob, injector))
    assert delivered["agent"]["image"] == section
    hydrated = load_config_from_resolved(delivered)
    assert hydrated.extra["image"] == section
    assert blob["agent"].get("image") == original

    async def removes(co):
        co.pop("image", None)
        return co

    stale = copy.deepcopy(delivered)
    assert "image" not in asyncio.run(inject_blob_credentials(stale, removes))["agent"]

    redacted = redact_config_override({"image": dict(section)})
    assert "api_key" not in redacted["image"]
    assert redacted["image"]["model"] == "gpt-image-2.6-flare"


def test_the_cap_comes_from_the_admin_setting():
    creds = CapabilityCredentials(
        model="gpt-image-2.6-flare", provider="openai", api_key="k", params={}
    )
    store = Mock()
    store.get_system_setting = AsyncMock(return_value={"value": {"max_paid_images": 5}})
    deps = Mock(store=store, logger=Mock())
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=creds),
    ):
        out = asyncio.run(
            dispatch_credentials.inject_image_credentials(
                {}, user_settings={}, user_id="u", resolved_keys={}, dependencies=deps
            )
        )
    assert out["image"]["max_paid_images"] == 5
