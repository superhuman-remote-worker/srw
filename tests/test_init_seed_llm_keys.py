"""Tests for ``orchestrator.init._seed_llm_keys_from_env``.

The init step builds a payload from ``SEED_<PROVIDER>_API_KEY`` env vars and
hands it to ``orchestrator.seed.llm_config.seed`` — the same insert-only
function the helm seeder Job uses. These tests pin: env-driven payload
construction, the legacy ``vision`` slot is excluded, and re-runs are
no-ops when rows already exist.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator import init as init_mod

ROOT = Path(__file__).resolve().parents[1]


def _fake_db(*, existing_providers: list[str] | None = None):
    db = MagicMock()
    db.list_system_api_keys = AsyncMock(
        return_value=[{"provider": p} for p in (existing_providers or [])]
    )
    db.list_system_llm_endpoints = AsyncMock(return_value=[])
    db.upsert_system_api_key = AsyncMock()
    return db


def _fake_db_with_catalog(
    *,
    openrouter_key: str | None = None,
    create_model_returns: dict | None = None,
):
    """Mock for the openrouter convenience path. Tracks ``create_model``
    calls so tests can assert which catalog rows got inserted.

    ``create_model_returns`` controls what the mock returns from each call:
    ``None`` simulates ``ON CONFLICT DO NOTHING`` skipping an existing row;
    a dict simulates a successful insert.
    """
    db = MagicMock()
    db.get_system_api_key = AsyncMock(
        side_effect=lambda provider: openrouter_key
        if provider == "openrouter"
        else None
    )
    db.create_model = AsyncMock(
        return_value=create_model_returns
        if create_model_returns is not None
        else {"id": "00000000-0000-0000-0000-000000000001"}
    )
    return db


@pytest.mark.asyncio
async def test_no_seed_env_vars_is_noop(monkeypatch):
    """With no SEED_* env vars set, the seeder isn't invoked."""
    for provider in ("openai", "anthropic", "google", "groq", "openrouter"):
        monkeypatch.delenv(f"SEED_{provider.upper()}_API_KEY", raising=False)

    db = _fake_db()
    await init_mod._seed_llm_keys_from_env(db)
    db.upsert_system_api_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_provider_seeded(monkeypatch):
    for provider in ("openai", "anthropic", "google", "groq", "openrouter"):
        monkeypatch.delenv(f"SEED_{provider.upper()}_API_KEY", raising=False)
    monkeypatch.setenv("SEED_OPENAI_API_KEY", "sk-test")

    db = _fake_db()
    await init_mod._seed_llm_keys_from_env(db)

    db.upsert_system_api_key.assert_awaited_once()
    kwargs = db.upsert_system_api_key.await_args.kwargs
    assert kwargs["provider"] == "openai"
    assert kwargs["api_key"] == "sk-test"
    assert kwargs["seeded_from"] == "helm:llm.seed"


@pytest.mark.asyncio
async def test_existing_row_skipped(monkeypatch):
    for provider in ("openai", "anthropic", "google", "groq", "openrouter"):
        monkeypatch.delenv(f"SEED_{provider.upper()}_API_KEY", raising=False)
    monkeypatch.setenv("SEED_OPENAI_API_KEY", "sk-rotated")

    db = _fake_db(existing_providers=["openai"])
    await init_mod._seed_llm_keys_from_env(db)

    # Existing row wins — admin-UI rotation is never clobbered by
    # subsequent boots.
    db.upsert_system_api_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_vision_legacy_slot_never_seeded(monkeypatch):
    """``vision`` is in VALID_SYSTEM_API_KEY_PROVIDERS for legacy reasons but
    must never be seeded — vision keys flow through the per-endpoint inline
    api_key on a custom endpoint row."""
    for provider in ("openai", "anthropic", "google", "groq", "openrouter"):
        monkeypatch.delenv(f"SEED_{provider.upper()}_API_KEY", raising=False)
    monkeypatch.setenv("SEED_VISION_API_KEY", "should-be-ignored")

    db = _fake_db()
    await init_mod._seed_llm_keys_from_env(db)
    db.upsert_system_api_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_multiple_providers_seeded(monkeypatch):
    for provider in ("openai", "anthropic", "google", "groq", "openrouter"):
        monkeypatch.delenv(f"SEED_{provider.upper()}_API_KEY", raising=False)
    monkeypatch.setenv("SEED_OPENAI_API_KEY", "sk-openai")
    monkeypatch.setenv("SEED_ANTHROPIC_API_KEY", "sk-ant")
    monkeypatch.setenv("SEED_GROQ_API_KEY", "gsk-test")

    db = _fake_db()
    await init_mod._seed_llm_keys_from_env(db)

    assert db.upsert_system_api_key.await_count == 3
    seeded_providers = {
        call.kwargs["provider"] for call in db.upsert_system_api_key.await_args_list
    }
    assert seeded_providers == {"openai", "anthropic", "groq"}


# ---------------------------------------------------------------------------
# OpenRouter default-pinning
# ---------------------------------------------------------------------------


class TestApplyOpenrouterDefaults:
    """Post-catalog migration: this step inserts catalog rows for the
    OpenRouter-routed auxiliary, embedding and rerank convenience models instead of
    pinning ``default_llm_models`` entries. The default-resolver's
    "first-enabled-alphabetical" fallback handles which one gets used.
    """

    @pytest.mark.asyncio
    async def test_no_openrouter_key_is_noop(self):
        db = _fake_db_with_catalog(openrouter_key=None)
        await init_mod._apply_openrouter_defaults(db)
        db.create_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_inserts_auxiliary_embedding_and_rerank_when_catalog_empty(self):
        db = _fake_db_with_catalog(openrouter_key="sk-or-v1-xxx")
        await init_mod._apply_openrouter_defaults(db)
        assert db.create_model.await_count == 3
        # Each convenience row is registered as a singleton-array (the
        # auxiliary openrouter row is purposefully aux-only, NOT
        # auxiliary+chat — it's a small/cheap model the operator routes to
        # the auxiliary slot specifically).
        capabilities_inserted = {
            tuple(call.kwargs["capabilities"])
            for call in db.create_model.await_args_list
        }
        assert capabilities_inserted == {("auxiliary",), ("embedding",), ("rerank",)}

    @pytest.mark.asyncio
    async def test_embedding_and_rerank_match_the_one_key_chart_seed(self):
        """SRW stores 4096-dim vectors, so the embedding row is Qwen3
        Embedding 8B: the pair the local overlay and docs/models.md seed from
        one OpenRouter key."""
        db = _fake_db_with_catalog(openrouter_key="sk-or-v1-xxx")
        await init_mod._apply_openrouter_defaults(db)
        by_capability = {
            call.kwargs["capabilities"][0]: call.kwargs["model_id"]
            for call in db.create_model.await_args_list
        }
        assert by_capability["embedding"] == "openrouter/qwen/qwen3-embedding-8b"
        assert by_capability["rerank"] == "openrouter/qwen/qwen3-reranker-8b"
        for path in ("deployment/values-local.yaml.example", "docs/models.md"):
            text = (ROOT / path).read_text()
            assert by_capability["embedding"] in text
            assert by_capability["rerank"] in text

    @pytest.mark.asyncio
    async def test_idempotent_when_rows_already_exist(self):
        """When ON CONFLICT DO NOTHING skips every row (create_model
        returns None), the step still runs every insert and exits clean."""
        db = _fake_db_with_catalog(
            openrouter_key="sk-or-v1-xxx", create_model_returns=None
        )
        await init_mod._apply_openrouter_defaults(db)
        assert db.create_model.await_count == 3  # all attempted, all skipped

    @pytest.mark.asyncio
    async def test_inserted_rows_route_through_openrouter(self):
        db = _fake_db_with_catalog(openrouter_key="sk-or-v1-xxx")
        await init_mod._apply_openrouter_defaults(db)
        for call in db.create_model.await_args_list:
            assert call.kwargs["provider_kind"] == "system"
            assert call.kwargs["provider_ref"] == "openrouter"
            assert call.kwargs["model_id"].startswith("openrouter/"), (
                f"expected openrouter/ prefix, got {call.kwargs['model_id']}"
            )

    @pytest.mark.asyncio
    async def test_inserted_rows_use_on_conflict_do_nothing(self):
        """Re-runs must not clobber admin edits — every insert sets
        on_conflict_do_nothing=True."""
        db = _fake_db_with_catalog(openrouter_key="sk-or-v1-xxx")
        await init_mod._apply_openrouter_defaults(db)
        for call in db.create_model.await_args_list:
            assert call.kwargs.get("on_conflict_do_nothing") is True

    @pytest.mark.asyncio
    async def test_inserted_rows_carry_helm_breadcrumb(self):
        db = _fake_db_with_catalog(openrouter_key="sk-or-v1-xxx")
        await init_mod._apply_openrouter_defaults(db)
        for call in db.create_model.await_args_list:
            assert call.kwargs.get("seeded_from") == "helm:openrouter-defaults"
