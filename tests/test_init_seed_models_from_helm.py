"""Tests for ``orchestrator.init._seed_models_from_helm``.

The seed step reads ``helm/values.yaml``'s ``llm.seed.systemModels[]`` block
and delegates to :func:`orchestrator.seed.llm_config.seed`. These tests pin:
provider-gating (skip rows for unseeded providers), the auxiliary / vision /
embedding helper lists map to non-chat capabilities, the ``helm:llm.seed``
breadcrumb travels with every insert, and re-runs are no-ops thanks to
``ON CONFLICT DO NOTHING``.

Post chunk 7 (models_yaml_removal), the legacy ``config/models.yaml`` bridge
is gone — these tests inject the systemModels payload via a temporary
``helm/values.yaml`` so they don't depend on the real chart values shipping
test fixtures.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator import init as init_mod
from shared.helm_provenance import value_hash


_TEST_HELM_VALUES = dedent(
    """
    llm:
      seed:
        enabled: true
        systemModels:
          - provider: openai
            id: gpt-4o
            displayName: GPT-4o
            capability: chat
            family: default
          - provider: openai
            id: gpt-4o-mini
            displayName: GPT-4o Mini
            capability: chat
            family: default
          - provider: openai
            id: gpt-4o-mini
            displayName: GPT-4o Mini (Aux)
            capability: auxiliary
            family: default
          - provider: openai
            id: gpt-4o
            displayName: GPT-4o (Vision)
            capability: vision
            family: default
          - provider: openai
            id: text-embedding-3-large
            displayName: OpenAI Embedding
            capability: embedding
            family: openai-embedding
          - provider: anthropic
            id: claude-opus-4-7
            displayName: Claude Opus 4.7
            capability: chat
            family: claude-opus
          - provider: openrouter
            id: openrouter/qwen/qwen3-embedding-8b
            displayName: Qwen Embedding (OR)
            capability: embedding
            family: default
    """
)


@pytest.fixture(autouse=True)
def _patch_helm_values(monkeypatch, tmp_path):
    """Redirect _seed_models_from_helm to a temp helm/values.yaml fixture.

    The function builds the path from ``__file__``; we monkey-patch
    ``Path`` resolution by replacing the project-relative file in a
    temporary directory and pointing the resolver at it.
    """
    helm_dir = tmp_path / "helm"
    helm_dir.mkdir()
    helm_path = helm_dir / "values.yaml"
    helm_path.write_text(_TEST_HELM_VALUES)

    real_resolve = Path.resolve

    def fake_resolve(self, *args, **kwargs):
        # The function resolves the repository root above src/orchestrator/.
        # Hijack any resolve() call on the init.py module file so the
        # subsequent .parents[2] / "helm" / "values.yaml" lands at our temp tree.
        out = real_resolve(self, *args, **kwargs)
        if str(out).endswith("/orchestrator/init.py"):
            return tmp_path / "src" / "orchestrator" / "init.py"
        return out

    monkeypatch.setattr(Path, "resolve", fake_resolve)
    yield


def _fake_db(*, seeded_providers: list[str] | None = None):
    db = MagicMock()
    db.list_system_api_keys = AsyncMock(
        return_value=[{"provider": p} for p in (seeded_providers or [])]
    )
    db.get_system_api_key = AsyncMock(return_value=None)
    db.create_model = AsyncMock(
        return_value={"id": "00000000-0000-0000-0000-000000000001"}
    )
    # A skipped insert asks who holds the id; nobody else does here.
    db.find_model_by_model_id = AsyncMock(return_value=None)
    return db


@pytest.mark.asyncio
async def test_no_keys_seeded_inserts_nothing():
    """No system_api_keys rows present → no catalog rows inserted."""
    db = _fake_db(seeded_providers=[])
    await init_mod._seed_models_from_helm(db)
    db.create_model.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_openai_seeded_only_openai_rows_inserted():
    """One provider seeded → only that provider's catalog rows are inserted.

    The fixture ships openai chat (gpt-4o, gpt-4o-mini) + auxiliary +
    vision + embedding entries plus anthropic and openrouter rows. Only
    openai rows should land when only openai has a key seeded.
    """
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    inserted_providers = {
        call.kwargs["provider_ref"] for call in db.create_model.await_args_list
    }
    assert inserted_providers == {"openai"}, (
        f"Expected only openai inserts, got {inserted_providers}"
    )
    assert db.create_model.await_count >= 2  # at least gpt-4o + gpt-4o-mini chat rows


@pytest.mark.asyncio
async def test_helper_lists_map_to_non_chat_capabilities():
    """Capability values from systemModels[] entries reach the catalog as
    arrays — auxiliary / vision / embedding singular entries land as
    single-element arrays, NOT defaulted to chat. The chat singular form
    is the only one that auto-expands (to ['chat','auxiliary'])."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    # Flatten every emitted capabilities[] across all inserts so we can
    # ask 'did each role get represented at least once'.
    seen_capabilities: set[str] = set()
    for call in db.create_model.await_args_list:
        for cap in call.kwargs["capabilities"]:
            seen_capabilities.add(cap)
    assert {"chat", "auxiliary", "vision", "embedding"}.issubset(seen_capabilities)


@pytest.mark.asyncio
async def test_chat_singular_auto_expands_to_chat_and_auxiliary():
    """The fixture has gpt-4o-mini at ``capability: chat`` and a separate
    entry at ``capability: auxiliary``. Both contribute to the same row
    under aggregation; the chat-singular auto-expand to ['chat','auxiliary']
    plus the explicit auxiliary dedupe should yield exactly ['chat','auxiliary']."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    rows = [
        call
        for call in db.create_model.await_args_list
        if call.kwargs["model_id"] == "gpt-4o-mini"
    ]
    # Aggregation collapses the two helm entries into one INSERT.
    assert len(rows) == 1
    assert rows[0].kwargs["capabilities"] == ["chat", "auxiliary"]


@pytest.mark.asyncio
async def test_duplicate_helm_entries_aggregate_into_one_row():
    """The fixture has two entries for gpt-4o (one chat, one vision). Under
    the array-capability model these collapse into one INSERT whose
    capabilities[] is the union: ['chat','auxiliary','vision']."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    gpt4o_rows = [
        call
        for call in db.create_model.await_args_list
        if call.kwargs["model_id"] == "gpt-4o"
    ]
    assert len(gpt4o_rows) == 1
    assert gpt4o_rows[0].kwargs["capabilities"] == [
        "chat",
        "auxiliary",
        "vision",
    ]


@pytest.mark.asyncio
async def test_singular_non_chat_capability_stays_singleton():
    """text-embedding-3-large is registered with `capability: embedding`.
    The pipeline must NOT auto-expand non-chat singular values."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    embedding_rows = [
        call
        for call in db.create_model.await_args_list
        if call.kwargs["model_id"] == "text-embedding-3-large"
    ]
    assert embedding_rows
    assert embedding_rows[0].kwargs["capabilities"] == ["embedding"]


@pytest.mark.asyncio
async def test_all_inserts_pass_on_conflict_do_nothing():
    """Every insert must use on_conflict_do_nothing=True so re-runs are
    safe and admin edits are never clobbered."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    for call in db.create_model.await_args_list:
        assert call.kwargs.get("on_conflict_do_nothing") is True


@pytest.mark.asyncio
async def test_seeded_from_breadcrumb_set():
    """All inserts must carry seeded_from='helm:llm.seed' so the catalog can
    distinguish helm-seed from admin-added rows."""
    db = _fake_db(seeded_providers=["openai"])
    await init_mod._seed_models_from_helm(db)

    breadcrumbs = {
        call.kwargs.get("seeded_from") for call in db.create_model.await_args_list
    }
    assert breadcrumbs == {"helm:llm.seed"}


@pytest.mark.asyncio
async def test_idempotent_when_create_model_returns_none():
    """When all rows already exist (create_model returns None), the function
    completes without error and reports zero inserts."""
    db = _fake_db(seeded_providers=["openai"])
    db.create_model = AsyncMock(return_value=None)  # simulate ON CONFLICT skip

    # Should not raise.
    await init_mod._seed_models_from_helm(db)
    # Still attempted the inserts.
    assert db.create_model.await_count >= 2


@pytest.mark.asyncio
async def test_anthropic_only_seeded_inserts_only_claude_rows():
    """The Anthropic chat row in the fixture should land alone when only
    the anthropic provider is seeded."""
    db = _fake_db(seeded_providers=["anthropic"])
    await init_mod._seed_models_from_helm(db)

    inserted_providers = {
        call.kwargs["provider_ref"] for call in db.create_model.await_args_list
    }
    assert inserted_providers == {"anthropic"}


_TEST_HELM_VALUES_WITH_DEFAULTS = dedent(
    """
    llm:
      seed:
        enabled: true
        systemModels:
          - provider: openai
            id: gpt-4o
            displayName: GPT-4o
            capability: chat
            family: default
        defaults:
          chat: gpt-4o
          auxiliary: gpt-4o
          embedding: text-embedding-3-large   # not in the catalog -> skipped
    """
)


@pytest.mark.asyncio
async def test_defaults_block_pins_only_catalog_models(tmp_path):
    (tmp_path / "helm" / "values.yaml").write_text(_TEST_HELM_VALUES_WITH_DEFAULTS)
    db = _fake_db(seeded_providers=["openai"])
    catalog: list[dict] = []

    async def _create_model(**kwargs):
        catalog.append(
            {
                "model_id": kwargs["model_id"],
                "capabilities": list(kwargs["capabilities"]),
                "enabled": True,
            }
        )
        return {"id": "00000000-0000-0000-0000-000000000001"}

    async def _list_models(*, capabilities=None, enabled_only=False, **_):
        return [
            row
            for row in catalog
            if not capabilities or set(capabilities) & set(row["capabilities"])
        ]

    db.create_model = AsyncMock(side_effect=_create_model)
    db.list_models = AsyncMock(side_effect=_list_models)
    db.get_default_llm_model = AsyncMock(return_value=None)
    db.set_default_llm_model = AsyncMock()

    await init_mod._seed_models_from_helm(db)

    pinned = {
        call.args[0]: call.args[1] for call in db.set_default_llm_model.await_args_list
    }
    assert pinned == {"chat": "gpt-4o", "auxiliary": "gpt-4o"}
    for call in db.set_default_llm_model.await_args_list:
        assert call.kwargs["updated_by"] == "helm:llm.seed"
        assert call.kwargs["source"] == "helm"
        assert call.kwargs["helm_value_hash"] == value_hash("gpt-4o")


@pytest.mark.asyncio
async def test_defaults_only_values_still_run_the_seed(tmp_path):
    (tmp_path / "helm" / "values.yaml").write_text(
        "llm:\n  seed:\n    defaults:\n      chat: gpt-4o\n"
    )
    db = _fake_db(seeded_providers=["openai"])
    db.list_models = AsyncMock(
        return_value=[{"model_id": "gpt-4o", "capabilities": ["chat"], "enabled": True}]
    )
    db.get_default_llm_model = AsyncMock(return_value=None)
    db.set_default_llm_model = AsyncMock()

    await init_mod._seed_models_from_helm(db)

    db.create_model.assert_not_awaited()
    db.set_default_llm_model.assert_awaited_once_with(
        "chat",
        "gpt-4o",
        updated_by="helm:llm.seed",
        source="helm",
        helm_value_hash=value_hash("gpt-4o"),
    )
