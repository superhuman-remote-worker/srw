"""Credential injection for every MODEL SLOT a job or session config carries.

Born as the regression test for the 2026-05-12 bug where a job's
`config_override` of the shape
`{"llm": {"tactical": {"model": "X"}, "strategic": {"model": "X"}}}`
walked through dispatch with no `base_url`/`api_key` injection on the
phase blocks — the agent's LLM factory fell back to the parent's `base_url`
and returned `404 Model 'X' not found` in an infinite retry loop
(knowledge-base/knowledge/issues/orchestrator_phase_override_credentials_not_injected.md).

Since U1 the per-phase tiers are gone: a legacy pin is lifted into the single
`llm.model` on the blob path (the ordinary top-level branch routes it) and is
still credentialed as a nested block on the no-blob fallback path, where the
agent lifts model + transport together. The nested slots that exist now are
`llm.summarization`, the roster-wide `subagents.llm` and every
`subagents.roster.<n>.llm` — a roster entry inheriting its parent's model
carries the parent's model NAME and is routed by it (`TestModelSlotCredentialInjection`,
`TestRosterPrefetch`). See universal_experts_and_subagents.md §1.1.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import projects as projects_composition
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from orchestrator.services import (
    job_dispatch_credentials as job_dispatch_credentials_module,
)
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from shared.runtime.core import model_registry as model_registry_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from orchestrator.services import knowledge_index  # noqa: E402
from shared.runtime.core.model_registry import ModelMeta  # noqa: E402


async def _build_kb_embedding_service():
    """The central indexer's embedding resolution, through main's live wiring.

    R1.B03 moved the builder to ``orchestrator.services.knowledge_index``; the
    collaborators it used to read as main globals now arrive through
    ``main._knowledge_index_dependencies()``. Going through that factory (rather
    than a hand-built dependency value) is the point of these cases: the
    indexer must resolve the *same* profile the dispatch path injects, off the
    same ``postgres_db`` and the same ``_inject_system_kb_embedding_profile``
    this module's fixtures monkeypatch.
    """
    return await knowledge_index.build_kb_embedding_service(
        dependencies=projects_composition.knowledge_index_dependencies(
            orchestrator.main.app.state.resources
        )
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _job(*, job_id: str = "00000000-0000-0000-0000-000000000001") -> dict:
    """Minimal job dict for the dispatch path."""
    return {
        "id": job_id,
        "user_id": "00000000-0000-0000-0000-0000000000aa",
        "project_id": "00000000-0000-0000-0000-0000000000bb",
    }


CODEX_ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"
CODEX_BASE_URL = "http://srw-codex-proxy:8317/v1"
CODEX_API_KEY = "sk-codex-test"


@pytest.fixture
def patched_main(monkeypatch):
    """Patch the DB + registry collaborators of `_inject_dispatch_credentials`
    so the test exercises only the injection branching.

    `_resolve_model` is wired to a side_effect that returns a real
    `ModelMeta` for `gpt-5.3-codex-spark` (origin=custom, endpoint pointing
    at the codex proxy) and `None` for everything else — mirroring the
    failing job's registry state.
    """

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id == "gpt-5.3-codex-spark":
            return ModelMeta(
                model_id="gpt-5.3-codex-spark",
                provider="openai",
                family="codex",
                display_name="GPT-5.3 Codex Spark",
                origin="custom",
                endpoint_id=CODEX_ENDPOINT_ID,
                api_key_ref="openai",
            )
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == CODEX_ENDPOINT_ID:
            return {
                "id": CODEX_ENDPOINT_ID,
                "label": "codex-proxy",
                "base_url": CODEX_BASE_URL,
                "api_key": CODEX_API_KEY,
            }
        return None

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    # No user-default fallback — the incident scenario is purely
    # job-override-driven.
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------


class TestPhaseOverrideCredentialInjection:
    @pytest.mark.asyncio
    async def test_tactical_and_strategic_overrides_get_endpoint_injected(
        self, patched_main
    ):
        """The exact failing config_override from the 2026-05-12 incident."""
        override = {
            "llm": {
                "tactical": {"model": "gpt-5.3-codex-spark"},
                "strategic": {"model": "gpt-5.3-codex-spark"},
            }
        }

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["tactical"]["base_url"] == CODEX_BASE_URL
        assert result["llm"]["tactical"]["api_key"] == CODEX_API_KEY
        assert result["llm"]["strategic"]["base_url"] == CODEX_BASE_URL
        assert result["llm"]["strategic"]["api_key"] == CODEX_API_KEY

    @pytest.mark.asyncio
    async def test_blob_delivery_credentials_the_lifted_legacy_pin(self, patched_main):
        """eec20eeb regression on the blob-delivery path, post-U1 shape.

        ``serialize_resolved_config`` used to emit the phase blocks with explicit
        ``base_url: None`` leaves that defeated ``_inject_model_credentials``'s
        ``setdefault``, so codex phase pins shipped without transport and 401'd
        against api.openai.com. Since U1 a legacy ``llm.strategic``/``tactical``
        pin does not survive as a nested block at all: ``resolve_config`` lifts
        it into the single ``llm.model`` BEFORE injection, so the ordinary
        top-level branch routes it and there is no nested None leaf left to
        bite. Exercises the REAL resolve_config blob through the REAL injector —
        the path jobs actually use.
        """
        from orchestrator.services.config_resolver import (
            inject_blob_credentials,
            resolve_config,
        )

        blob = resolve_config(
            base_config_name="defaults",
            request_override={
                "llm": {
                    "strategic": {"model": "gpt-5.3-codex-spark"},
                    "tactical": {"model": "gpt-5.3-codex-spark"},
                }
            },
            expert_type="worker",
        )
        # The pin IS the model now; no phase block remains to carry a None leaf.
        assert blob["agent"]["llm"]["model"] == "gpt-5.3-codex-spark"
        assert "strategic" not in blob["agent"]["llm"]
        assert "tactical" not in blob["agent"]["llm"]
        # Precondition: transport-less before injection (the base has no URL).
        assert blob["agent"]["llm"].get("base_url") is None

        delivered = await inject_blob_credentials(
            blob,
            lambda co: job_dispatch_credentials_module.inject_dispatch_credentials(
                _job(),
                co,
                dependencies=preparation_composition.job_dispatch_credential_dependencies(
                    orchestrator.main.app.state.resources
                ),
            ),
        )

        llm = delivered["agent"]["llm"]
        assert llm["model"] == "gpt-5.3-codex-spark"
        assert llm["base_url"] == CODEX_BASE_URL
        assert llm["api_key"] == CODEX_API_KEY
        assert llm["provider"] == "openai"

    @pytest.mark.asyncio
    async def test_auxiliary_override_also_injected(self, patched_main):
        """Same hole existed for top-level `auxiliary` overrides."""
        override = {"auxiliary": {"model": "gpt-5.3-codex-spark"}}

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["auxiliary"]["base_url"] == CODEX_BASE_URL
        assert result["auxiliary"]["api_key"] == CODEX_API_KEY

    @pytest.mark.asyncio
    async def test_existing_base_url_in_phase_block_is_refreshed_from_endpoint(
        self, patched_main
    ):
        """Endpoint-backed phase models refresh stale caller/persisted transport."""
        override = {
            "llm": {
                "tactical": {
                    "model": "gpt-5.3-codex-spark",
                    "base_url": "https://caller-pinned.example/v1",
                    "api_key": "sk-stale",
                }
            }
        }

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["tactical"]["base_url"] == CODEX_BASE_URL
        assert result["llm"]["tactical"]["api_key"] == CODEX_API_KEY

    @pytest.mark.asyncio
    async def test_unknown_phase_model_logs_warning_no_crash(
        self, patched_main, caplog
    ):
        """A phase pin for a model the registry doesn't know must not crash —
        it should log a warning and leave the section unmodified for the
        downstream agent to surface a clear error."""
        override = {"llm": {"tactical": {"model": "does-not-exist"}}}

        with caplog.at_level("WARNING", logger=preparation_composition.logger.name):
            result = await job_dispatch_credentials_module.inject_dispatch_credentials(
                _job(),
                override,
                dependencies=preparation_composition.job_dispatch_credential_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        assert result["llm"]["tactical"]["model"] == "does-not-exist"
        assert "base_url" not in result["llm"]["tactical"]
        assert any(
            "does-not-exist" in rec.message and "tactical" in rec.message
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_empty_override_is_a_noop(self, patched_main):
        """No phase sections → no injection, no crashes."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert "tactical" not in result.get("llm", {})
        assert "strategic" not in result.get("llm", {})
        assert "auxiliary" not in result


# ---------------------------------------------------------------------------
# Per-phase account model defaults were REMOVED (Layer 1).
#
# default_strategic_model / default_tactical_model in users.settings used to be
# injected as llm.strategic / llm.tactical phase pins at dispatch. A phase pin
# beats the top-level model in get_phase_config, so they silently shadowed an
# explicit per-loop/per-job top-level model (a loop pinned to gpt-5.5 ran
# entirely on gpt-5.3-codex-spark). Dispatch must no longer read them.
# See knowledge-base/knowledge/issues/loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_main_phase_prefs(monkeypatch):
    """User settings still carry the (removed) per-phase model defaults; the
    registry resolves any model so an injected pin would be visible with
    transport. If dispatch ever re-reads the keys, the assertions below fail."""

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        return ModelMeta(
            model_id=model_id,
            provider="openai",
            family="codex",
            display_name=model_id,
            origin="custom",
            endpoint_id=CODEX_ENDPOINT_ID,
            api_key_ref="openai",
        )

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        return {
            "id": CODEX_ENDPOINT_ID,
            "label": "codex-proxy",
            "base_url": CODEX_BASE_URL,
            "api_key": CODEX_API_KEY,
        }

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(
            return_value={
                "default_strategic_model": "gpt-5.3-codex-spark",
                "default_tactical_model": "gpt-5.3-codex-spark",
            }
        ),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestPerPhaseAccountDefaultsRemoved:
    @pytest.mark.asyncio
    async def test_account_phase_defaults_do_not_shadow_explicit_model(
        self, patched_main_phase_prefs
    ):
        """The loop scenario: an explicit top-level model, no phase pins on the
        job. Account phase defaults must NOT add strategic/tactical pins."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "gpt-5.5"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["model"] == "gpt-5.5"
        assert "strategic" not in result["llm"]
        assert "tactical" not in result["llm"]

    @pytest.mark.asyncio
    async def test_account_phase_defaults_not_injected_on_empty_override(
        self, patched_main_phase_prefs
    ):
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert "strategic" not in result.get("llm", {})
        assert "tactical" not in result.get("llm", {})


# ---------------------------------------------------------------------------
# System-anchored provider routing (provider_kind='system')
#
# A catalog row anchored to a system_api_keys provider (e.g. OpenRouter)
# carries NO endpoint base_url — `_catalog_row_to_meta` leaves base_url=None
# and only sets api_key_ref. The dispatcher used to inject just the api_key,
# so the agent's create_llm fell back to the OpenAI factory default
# (api.openai.com) and rejected the OpenRouter `sk-or-v1…` key with a 401
# from platform.openai.com. The fix injects `meta.provider` (the factory
# name) so OpenRouter rows route through _create_openrouter_llm → openrouter.ai.
# ---------------------------------------------------------------------------

OR_KEY = "sk-or-v1-test00000000000000000000000000000000000000000000fc51"


@pytest.fixture
def patched_main_openrouter(monkeypatch):
    """Registry returns a system-anchored OpenRouter chat row (no endpoint,
    base_url=None, api_key_ref='openrouter', provider='openrouter') and the
    job resolves an OpenRouter system key."""

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id == "minimax/minimax-m3":
            return ModelMeta(
                model_id="minimax/minimax-m3",
                provider="openrouter",  # _factory_provider('openrouter')
                family="minimax-m3",
                display_name="MiniMax M3",
                base_url=None,  # system rows carry no endpoint base_url
                api_key_ref="openrouter",  # resolves system_api_keys['openrouter']
                origin="catalog",
                endpoint_id=None,
            )
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={"openrouter": OR_KEY}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestSystemProviderRouting:
    @pytest.mark.asyncio
    async def test_main_llm_openrouter_row_injects_provider_and_key(
        self, patched_main_openrouter
    ):
        """The exact M3 incident: a system OpenRouter chat model must carry
        provider='openrouter' so the agent routes to openrouter.ai, not the
        OpenAI factory default."""
        override = {"llm": {"model": "minimax/minimax-m3"}}

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["provider"] == "openrouter"
        assert result["llm"]["api_key"] == OR_KEY

    @pytest.mark.asyncio
    async def test_phase_override_openrouter_row_injects_provider(
        self, patched_main_openrouter
    ):
        """Phase overrides go through _inject_model_credentials — same fix
        must reach them."""
        override = {"llm": {"tactical": {"model": "minimax/minimax-m3"}}}

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["tactical"]["provider"] == "openrouter"
        assert result["llm"]["tactical"]["api_key"] == OR_KEY

    @pytest.mark.asyncio
    async def test_caller_pinned_provider_is_not_overwritten(
        self, patched_main_openrouter
    ):
        """Injection is additive (setdefault) — an explicit provider wins."""
        override = {"llm": {"model": "minimax/minimax-m3", "provider": "openai"}}

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["provider"] == "openai"


# ---------------------------------------------------------------------------
# Per-model context window injection
#
# A catalog/endpoint row's `context_window` drives the agent's working window:
# it is injected into the section's `model_max_context_tokens` (a flat llm key),
# survives the agent-side settings-matrix re-apply, and becomes the base for the
# derived limits. The worker top-level llm gets it via the inline block; phase
# sections (strategic/tactical, capability="chat") get it via
# `_inject_model_credentials`. Auxiliary sections (capability="auxiliary") must
# NOT — their window is not derived this way.
# ---------------------------------------------------------------------------

CTX_ENDPOINT_ID = "22222222-2222-2222-2222-222222222222"
CTX_BASE_URL = "http://self-hosted:8000/v1"
CTX_API_KEY = "sk-ctx-test"


@pytest.fixture
def patched_main_ctx(monkeypatch):
    """`_resolve_model` returns endpoint-backed metas whose `context_window`
    varies by model_id: 32000, None, or 0 (the explicit-zero signal)."""

    windows = {"ctx-32k": 32000, "ctx-none": None, "ctx-zero": 0}

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id in windows:
            return ModelMeta(
                model_id=model_id,
                provider="openai",
                family="default",
                display_name=model_id,
                origin="custom",
                endpoint_id=CTX_ENDPOINT_ID,
                api_key_ref="openai",
                context_window=windows[model_id],
            )
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == CTX_ENDPOINT_ID:
            return {
                "id": CTX_ENDPOINT_ID,
                "label": "self-hosted",
                "base_url": CTX_BASE_URL,
                "api_key": CTX_API_KEY,
            }
        return None

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestContextWindowInjection:
    @pytest.mark.asyncio
    async def test_top_level_window_injected(self, patched_main_ctx):
        """A per-model context_window lands on the top-level llm section."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "ctx-32k"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["model_max_context_tokens"] == 32000
        # Routing creds still injected alongside.
        assert result["llm"]["base_url"] == CTX_BASE_URL

    @pytest.mark.asyncio
    async def test_none_window_not_injected(self, patched_main_ctx):
        """No context_window → the key is absent (agent falls back to family)."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "ctx-none"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert "model_max_context_tokens" not in result["llm"]

    @pytest.mark.asyncio
    async def test_zero_window_not_injected(self, patched_main_ctx):
        """Explicit 0 is rejected by the truthy guard (Pydantic round-trips 0)."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "ctx-zero"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert "model_max_context_tokens" not in result["llm"]

    @pytest.mark.asyncio
    async def test_caller_pinned_window_wins(self, patched_main_ctx):
        """Injection is additive (setdefault) — an explicit window wins."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "ctx-32k", "model_max_context_tokens": 64000}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["model_max_context_tokens"] == 64000

    @pytest.mark.asyncio
    async def test_chat_phase_section_gets_window(self, patched_main_ctx):
        """Strategic/tactical phase pins (capability='chat') get the window."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"tactical": {"model": "ctx-32k"}}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["tactical"]["model_max_context_tokens"] == 32000

    @pytest.mark.asyncio
    async def test_auxiliary_section_does_not_get_window(self, patched_main_ctx):
        """Capability gating: auxiliary sections are not context-window-derived,
        but still get their routing creds."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"auxiliary": {"model": "ctx-32k"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["auxiliary"]["base_url"] == CTX_BASE_URL
        assert "model_max_context_tokens" not in result["auxiliary"]


# ---------------------------------------------------------------------------
# Endpoint-backed model routing.
#
# Endpoint rows are the transport authority for catalog/self-hosted models. The
# injector refreshes base_url/api_key/provider from the endpoint row so resumed
# jobs and hot-swapped sessions cannot keep stale transport from an older model.
# ---------------------------------------------------------------------------


def _patch_resolve_provider(monkeypatch, *, provider: str):
    async def fake_resolve(model_id, user_id=None, capability="chat"):
        return ModelMeta(
            model_id=model_id,
            provider=provider,
            family="gpt-5",
            display_name=model_id,
            origin="catalog",
            endpoint_id=CODEX_ENDPOINT_ID,
        )

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == CODEX_ENDPOINT_ID:
            return {
                "id": CODEX_ENDPOINT_ID,
                "label": "codex-proxy",
                "base_url": CODEX_BASE_URL,
                "api_key": CODEX_API_KEY,
            }
        return None

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )


class TestEndpointDirectRouting:
    """Endpoint-backed models inject their configured endpoint transport."""

    @pytest.mark.asyncio
    async def test_codex_model_hits_endpoint(self, monkeypatch):
        _patch_resolve_provider(monkeypatch, provider="codex")
        section = {"model": "gpt-5.5"}
        await dispatch_credentials_module.inject_model_credentials(
            section=section,
            model_id="gpt-5.5",
            user_id="u",
            resolved_keys={},
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert section["base_url"] == CODEX_BASE_URL
        assert section["api_key"] == CODEX_API_KEY
        assert section.get("provider") == "codex"

    @pytest.mark.asyncio
    async def test_openai_endpoint_model_hits_endpoint(self, monkeypatch):
        _patch_resolve_provider(monkeypatch, provider="openai")
        section = {"model": "gemma-4-moe"}
        await dispatch_credentials_module.inject_model_credentials(
            section=section,
            model_id="gemma-4-moe",
            user_id="u",
            resolved_keys={},
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert section["base_url"] == CODEX_BASE_URL
        assert section["api_key"] == CODEX_API_KEY
        assert section.get("provider") == "openai"

    @pytest.mark.asyncio
    async def test_endpoint_model_replaces_stale_transport(self, monkeypatch):
        """A persisted section can carry a stale base_url/provider from a
        previous model. Endpoint-backed rows must refresh the whole transport
        from the endpoint table."""
        _patch_resolve_provider(monkeypatch, provider="codex")
        section = {
            "model": "gpt-5.5",
            "base_url": "https://stale.example/v1",
            "api_key": "sk-stale",
            "provider": "openai",  # stale factory from the same prior model
        }
        await dispatch_credentials_module.inject_model_credentials(
            section=section,
            model_id="gpt-5.5",
            user_id="u",
            resolved_keys={},
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert section["base_url"] == CODEX_BASE_URL
        assert section["api_key"] == CODEX_API_KEY
        assert section["provider"] == "codex"

    @pytest.mark.asyncio
    async def test_endpoint_model_replaces_caller_pinned_transport(self, monkeypatch):
        """For a catalog endpoint model, the endpoint row wins over caller-pinned
        transport so persisted overrides cannot split model/provider/key."""
        _patch_resolve_provider(monkeypatch, provider="codex")
        section = {
            "model": "gpt-5.5",
            "base_url": "https://byo-codex.example/v1",
            "api_key": "sk-byo",
        }
        await dispatch_credentials_module.inject_model_credentials(
            section=section,
            model_id="gpt-5.5",
            user_id="u",
            resolved_keys={},
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert section["base_url"] == CODEX_BASE_URL
        assert section["api_key"] == CODEX_API_KEY


# ---------------------------------------------------------------------------
# Embedding credential reliability (memory + KB).
#
# The embedding key drives memory (RecallStore) + KB (KnowledgeStore). It was
# resolved ONLY inside the `if job.get("user_id")` block, so a job whose user
# had no embedding preference (or no user) silently shipped without a key and
# ran with memory/KB dead and no signal. The fix gives embedding the same
# system-default fallback the chat model has (outside the user gate), stops a
# pre-present EMBEDDING_MODEL from suppressing the key, and refuses to emit a
# half-credential when the endpoint key can't decrypt.
# See knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
# ---------------------------------------------------------------------------

EMB_ENDPOINT_ID = "33333333-3333-3333-3333-333333333333"
EMB_BASE_URL = "https://ai.h4ll.app/v1"
EMB_API_KEY = "sk-emb-test"
EMB_MODEL = "qwen3-embedding-8b"


def _job_no_user(*, job_id: str = "00000000-0000-0000-0000-000000000002") -> dict:
    """A job with NO user_id — the user-preference dispatch block is skipped."""
    return {"id": job_id, "project_id": "00000000-0000-0000-0000-0000000000bb"}


@pytest.fixture
def patched_main_embedding(monkeypatch):
    """System embedding model `qwen3-embedding-8b` on an endpoint with a key.

    No user settings; `resolve_default_for_capability` returns the embedding
    model only for the "embedding" capability (None elsewhere, so the chat /
    vision / aux fallbacks stay no-ops). Returns a mutable holder so a test can
    simulate a decrypt failure (`holder["api_key"] = None`).
    """
    holder = {"api_key": EMB_API_KEY}

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id == EMB_MODEL:
            return ModelMeta(
                model_id=EMB_MODEL,
                provider="openai",
                family="default",
                display_name="Qwen3 Embedding 8B",
                origin="system",
                endpoint_id=EMB_ENDPOINT_ID,
                api_key_ref="openai",
            )
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == EMB_ENDPOINT_ID:
            return {
                "id": EMB_ENDPOINT_ID,
                "label": "Local Router",
                "base_url": EMB_BASE_URL,
                "api_key": holder["api_key"],
            }
        return None

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )

    async def fake_default(capability):
        return EMB_MODEL if capability == "embedding" else None

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(side_effect=fake_default),
    )
    return holder


class TestEmbeddingCredentialReliability:
    @pytest.mark.asyncio
    async def test_system_default_injected_without_user(self, patched_main_embedding):
        """No user_id → user block skipped; the system-default fallback still
        injects the embedding endpoint + key (the core asymmetry fix)."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {},
            include_kb_profile=True,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        env = result["env_keys"]
        assert env["EMBEDDING_MODEL"] == EMB_MODEL
        assert env["EMBEDDING_BASE_URL"] == EMB_BASE_URL
        assert env["EMBEDDING_API_KEY"] == EMB_API_KEY
        assert env["KB_EMBEDDING_MODEL"] == EMB_MODEL
        assert env["KB_EMBEDDING_BASE_URL"] == EMB_BASE_URL
        assert env["KB_EMBEDDING_API_KEY"] == EMB_API_KEY
        assert env["KB_EMBEDDING_PROVIDER"] == "openai"
        assert env["KB_EMBEDDING_DIMENSIONS"] == "4096"
        assert env["KB_EMBEDDING_PROFILE_ID"] == f"system:{EMB_ENDPOINT_ID}"

    @pytest.mark.asyncio
    async def test_kb_profile_is_system_owned(self, patched_main_embedding):
        """A request cannot make KB queries use a user/BYO vector profile."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {
                "env_keys": {
                    "KB_EMBEDDING_MODEL": "attacker-model",
                    "KB_EMBEDDING_BASE_URL": "https://attacker.invalid/v1",
                    "KB_EMBEDDING_API_KEY": "attacker-key",
                    "KB_EMBEDDING_PROVIDER": "openrouter",
                }
            },
            include_kb_profile=True,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        env = result["env_keys"]
        assert env["KB_EMBEDDING_MODEL"] == EMB_MODEL
        assert env["KB_EMBEDDING_BASE_URL"] == EMB_BASE_URL
        assert env["KB_EMBEDDING_API_KEY"] == EMB_API_KEY
        assert env["KB_EMBEDDING_PROVIDER"] == "openai"

    @pytest.mark.asyncio
    async def test_unscoped_job_receives_no_system_kb_secret(
        self, patched_main_embedding
    ):
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {
                "env_keys": {
                    "KB_EMBEDDING_MODEL": "caller-model",
                    "KB_EMBEDDING_API_KEY": "caller-key",
                }
            },
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert not any(
            key.startswith("KB_EMBEDDING_") for key in result.get("env_keys", {})
        )

    @pytest.mark.asyncio
    async def test_central_indexer_uses_dispatched_kb_profile(
        self, patched_main_embedding
    ):
        dispatched = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {},
            include_kb_profile=True,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        service = await _build_kb_embedding_service()
        env = dispatched["env_keys"]

        assert service.model == env["KB_EMBEDDING_MODEL"]
        assert service.base_url == env["KB_EMBEDDING_BASE_URL"]
        assert service.api_key == env["KB_EMBEDDING_API_KEY"]
        assert service.provider == env["KB_EMBEDDING_PROVIDER"]
        assert service.expected_dimensions == int(env["KB_EMBEDDING_DIMENSIONS"])
        assert service.profile_identity == env["KB_EMBEDDING_PROFILE_ID"]

    @pytest.mark.asyncio
    async def test_dev_env_fallback_is_dispatched_as_authoritative_kb_profile(
        self, monkeypatch
    ):
        monkeypatch.setenv("EMBEDDING_PROVIDER", "local")
        monkeypatch.setenv("EMBEDDING_MODEL", "dev-kb-model")
        monkeypatch.setenv("EMBEDDING_BASE_URL", "https://dev.example/v1")
        monkeypatch.setenv("EMBEDDING_API_KEY", "dev-system-key")
        monkeypatch.setenv("EMBEDDING_DIMENSIONS", "4096")
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "resolve_default_for_capability",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "resolve_api_keys_for_job",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_user_settings",
            AsyncMock(return_value={}),
        )

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {},
            include_kb_profile=True,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        service = await _build_kb_embedding_service()
        env = result["env_keys"]

        assert env["KB_EMBEDDING_MODEL"] == "dev-kb-model"
        assert env["KB_EMBEDDING_BASE_URL"] == "https://dev.example/v1"
        assert env["KB_EMBEDDING_API_KEY"] == "dev-system-key"
        assert service.model == env["KB_EMBEDDING_MODEL"]
        assert service.base_url == env["KB_EMBEDDING_BASE_URL"]
        assert service.api_key == env["KB_EMBEDDING_API_KEY"]

    @pytest.mark.asyncio
    async def test_user_without_embedding_pref_still_gets_key(
        self, patched_main_embedding
    ):
        """A job WITH a user but no embedding preference still resolves the
        system default embedding key."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["env_keys"]["EMBEDDING_API_KEY"] == EMB_API_KEY

    @pytest.mark.asyncio
    async def test_pre_present_model_does_not_suppress_key(
        self, patched_main_embedding
    ):
        """A pre-present EMBEDDING_MODEL must NOT skip _API_KEY injection."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {"env_keys": {"EMBEDDING_MODEL": EMB_MODEL}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["env_keys"]["EMBEDDING_API_KEY"] == EMB_API_KEY

    @pytest.mark.asyncio
    async def test_preset_api_key_is_not_overwritten(self, patched_main_embedding):
        """A per-job/BYO embedding key already in env_keys wins (additive)."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job_no_user(),
            {"env_keys": {"EMBEDDING_API_KEY": "user-byo"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["env_keys"]["EMBEDDING_API_KEY"] == "user-byo"

    @pytest.mark.asyncio
    async def test_decrypt_failure_emits_no_half_credential(
        self, patched_main_embedding, caplog
    ):
        """Endpoint has a base_url but the key didn't decrypt (api_key=None):
        do NOT inject a base_url-without-key half-credential, and log loudly."""
        patched_main_embedding["api_key"] = None
        with caplog.at_level("ERROR", logger=preparation_composition.logger.name):
            result = await job_dispatch_credentials_module.inject_dispatch_credentials(
                _job_no_user(),
                {},
                dependencies=preparation_composition.job_dispatch_credential_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        env = result.get("env_keys", {})
        assert "EMBEDDING_API_KEY" not in env
        assert "EMBEDDING_BASE_URL" not in env
        assert any(EMB_ENDPOINT_ID in rec.getMessage() for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_catalog_kb_profile_never_indexes_with_env_fallback(
        self, patched_main_embedding, monkeypatch
    ):
        """A broken selected profile must fail, not index with another model."""
        patched_main_embedding["api_key"] = None
        monkeypatch.setenv("EMBEDDING_MODEL", "different-env-model")
        monkeypatch.setenv("EMBEDDING_API_KEY", "env-key")

        assert await _build_kb_embedding_service() is None


# ---------------------------------------------------------------------------
# U1 model slots (WP4): llm.summarization, the roster-wide subagents.llm and
# every subagents.roster.<n>.llm are model slots of their own. Same fixture as
# the incident tests: only `gpt-5.3-codex-spark` is endpoint-backed.
# ---------------------------------------------------------------------------

_LIBRARY_REF = "subagents/explorer"


class TestModelSlotCredentialInjection:
    @pytest.mark.asyncio
    async def test_subagents_llm_and_roster_pins_get_endpoint_injected(
        self, patched_main
    ):
        override = {
            "subagents": {
                "llm": {"model": "gpt-5.3-codex-spark"},
                "roster": {
                    "reviewer": {"llm": {"model": "gpt-5.3-codex-spark"}},
                    "twin": {"llm": {"model": "inherit"}},
                },
            }
        }

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        roster_wide = result["subagents"]["llm"]
        assert roster_wide["base_url"] == CODEX_BASE_URL
        assert roster_wide["api_key"] == CODEX_API_KEY
        assert roster_wide["provider"] == "openai"
        reviewer = result["subagents"]["roster"]["reviewer"]["llm"]
        assert reviewer["base_url"] == CODEX_BASE_URL
        assert reviewer["api_key"] == CODEX_API_KEY
        # The bare sentinel is not a model: left alone, nothing looked up.
        assert result["subagents"]["roster"]["twin"]["llm"] == {"model": "inherit"}

    @pytest.mark.asyncio
    async def test_summarization_pin_gets_endpoint_injected(self, patched_main):
        override = {"llm": {"summarization": {"model": "gpt-5.3-codex-spark"}}}

        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert result["llm"]["summarization"]["base_url"] == CODEX_BASE_URL
        assert result["llm"]["summarization"]["api_key"] == CODEX_API_KEY

    @pytest.mark.asyncio
    async def test_unknown_roster_model_warns_naming_the_entry(
        self, patched_main, caplog
    ):
        override = {
            "subagents": {"roster": {"reviewer": {"llm": {"model": "does-not-exist"}}}}
        }
        with caplog.at_level("WARNING", logger=preparation_composition.logger.name):
            result = await job_dispatch_credentials_module.inject_dispatch_credentials(
                _job(),
                override,
                dependencies=preparation_composition.job_dispatch_credential_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        entry = result["subagents"]["roster"]["reviewer"]["llm"]
        assert entry["model"] == "does-not-exist"
        assert "base_url" not in entry
        assert any(
            "does-not-exist" in rec.message
            and "subagents.roster.reviewer" in rec.message
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_blob_delivery_credentials_every_roster_entry(self, patched_main):
        """The path jobs actually use: `resolve_config` materialises the roster,
        `inject_blob_credentials` seeds the roster llm blocks and merges the
        transport back — for a pinned entry, an inline entry inheriting the
        parent's model and a library `$ref` (both carry the parent's model
        NAME + `_inherit_llm`, and are routed by that name)."""
        from orchestrator.services.config_resolver import (
            inject_blob_credentials,
            resolve_config,
            unrouted_model_slots,
        )

        parent = {
            "expert_type": "worker",
            "name": "lead",
            "config": {
                "llm": {"model": "gpt-5.3-codex-spark"},
                "subagents": {
                    "roster": {
                        "explorer": {"$ref": _LIBRARY_REF},
                        "pinned": {"llm": {"model": "gpt-5.3-codex-spark"}},
                        "inline": {"description": "inherits the parent's model"},
                    }
                },
            },
            "prompts": {},
        }
        blob = resolve_config(
            base_config_name="worker_base", expert_row=parent, expert_type="worker"
        )
        roster = blob["agent"]["subagents"]["roster"]
        assert set(roster) == {"explorer", "pinned", "inline"}
        for name in ("explorer", "inline"):
            assert roster[name]["llm"]["model"] == "gpt-5.3-codex-spark"
            assert roster[name]["llm"]["_inherit_llm"] is True
        # Precondition: transport-less before injection (the base has no URL).
        for entry in roster.values():
            assert entry["llm"].get("base_url") is None
            assert "api_key" not in entry["llm"]

        delivered = await inject_blob_credentials(
            blob,
            lambda co: job_dispatch_credentials_module.inject_dispatch_credentials(
                _job(),
                co,
                dependencies=preparation_composition.job_dispatch_credential_dependencies(
                    orchestrator.main.app.state.resources
                ),
            ),
        )

        for name in ("explorer", "pinned", "inline"):
            llm = delivered["agent"]["subagents"]["roster"][name]["llm"]
            assert llm["base_url"] == CODEX_BASE_URL, name
            assert llm["api_key"] == CODEX_API_KEY, name
            assert llm["provider"] == "openai", name
        assert delivered["agent"]["llm"]["api_key"] == CODEX_API_KEY
        assert not [p for p in unrouted_model_slots(delivered) if "subagents" in p]
        # The persistable blob is untouched.
        for entry in blob["agent"]["subagents"]["roster"].values():
            assert "api_key" not in entry["llm"]
            assert entry["llm"].get("base_url") is None

    @pytest.mark.asyncio
    async def test_inject_blob_credentials_strips_nested_none_in_roster(self):
        """The eec20eeb shape, on the roster: explicit None leaves inside the
        roster llm blocks must not reach the injector (they defeat its
        setdefault gap-fill), only the llm blocks are handed over, and only
        they are merged back."""
        import copy

        from orchestrator.services.config_resolver import inject_blob_credentials

        blob = {
            "agent": {
                "llm": {"model": "parent", "base_url": None},
                "subagents": {
                    "llm": {"model": "wide", "provider": None},
                    "roster": {
                        "reviewer": {
                            "llm": {
                                "model": "m",
                                "base_url": None,
                                "provider": None,
                                "_inherit_llm": True,
                                "summarization": {"model": "s", "base_url": None},
                            },
                            "tools": {"shell": ["run_command"]},
                        }
                    },
                },
            }
        }
        seen: dict = {}

        async def injector(co):
            seen.update(copy.deepcopy(co))
            co["subagents"]["llm"]["base_url"] = "http://wide/v1"
            entry_llm = co["subagents"]["roster"]["reviewer"]["llm"]
            entry_llm["base_url"] = "http://entry/v1"
            entry_llm["api_key"] = "sk-entry"
            entry_llm["summarization"]["base_url"] = "http://summ/v1"
            return co

        delivered = await inject_blob_credentials(blob, injector)

        assert seen["subagents"] == {
            "llm": {"model": "wide"},
            "roster": {
                "reviewer": {
                    "llm": {
                        "model": "m",
                        "_inherit_llm": True,
                        "summarization": {"model": "s"},
                    }
                }
            },
        }
        entry = delivered["agent"]["subagents"]["roster"]["reviewer"]
        assert entry["llm"]["base_url"] == "http://entry/v1"
        assert entry["llm"]["api_key"] == "sk-entry"
        assert entry["llm"]["summarization"]["base_url"] == "http://summ/v1"
        assert entry["llm"]["_inherit_llm"] is True
        assert entry["tools"] == {"shell": ["run_command"]}  # only llm is merged back
        assert delivered["agent"]["subagents"]["llm"]["base_url"] == "http://wide/v1"
        # The input blob is never mutated.
        assert "api_key" not in blob["agent"]["subagents"]["roster"]["reviewer"]["llm"]
        assert blob["agent"]["subagents"]["llm"] == {"model": "wide", "provider": None}

    def test_unrouted_model_slots_names_the_roster_entry(self):
        from orchestrator.services.config_resolver import unrouted_model_slots

        blob = {
            "agent": {
                "llm": {"model": "parent", "base_url": "http://p/v1"},
                "subagents": {
                    "llm": {"model": "wide-orphan"},
                    "roster": {
                        "reviewer": {
                            "llm": {"model": "entry-orphan", "_inherit_llm": True}
                        },
                        "routed": {"llm": {"model": "ok", "provider": "openai"}},
                        "unset": {"llm": {"model": "inherit"}},
                    },
                },
            }
        }
        problems = unrouted_model_slots(blob)
        assert "subagents.llm model 'wide-orphan'" in problems
        assert "subagents.roster.reviewer.llm model 'entry-orphan'" in problems
        assert not any("routed" in p or "inherit" in p for p in problems)

    @pytest.mark.asyncio
    async def test_thread_injector_mirrors_the_slots(self, patched_main):
        """Sessions re-inject on every attach through the thread injector; the
        same nested slots get the same transport, and a stored copy's None
        sentinels inside a roster entry are repopulated like the top level's."""
        co = {
            "llm": {"model": "gpt-5.3-codex-spark"},
            "subagents": {
                "llm": {"model": "gpt-5.3-codex-spark", "base_url": None},
                "roster": {
                    "reviewer": {
                        "llm": {
                            "model": "gpt-5.3-codex-spark",
                            "provider": None,
                            "base_url": None,
                        }
                    },
                    "twin": {"llm": {"model": "inherit"}},
                },
            },
        }

        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            co,
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert out["subagents"]["llm"]["base_url"] == CODEX_BASE_URL
        assert out["subagents"]["llm"]["api_key"] == CODEX_API_KEY
        reviewer = out["subagents"]["roster"]["reviewer"]["llm"]
        assert reviewer["base_url"] == CODEX_BASE_URL
        assert reviewer["api_key"] == CODEX_API_KEY
        assert reviewer["provider"] == "openai"
        assert out["subagents"]["roster"]["twin"]["llm"] == {"model": "inherit"}


# ---------------------------------------------------------------------------
# The prefetch: the rows resolve_config(db_refs=...) materialises for a
# roster's UUID `$ref`s, by layer trust (u1_plan B.3).
# ---------------------------------------------------------------------------


class TestRosterPrefetch:
    EXPERT_REF = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    OVERRIDE_REF = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    HIDDEN_REF = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

    @pytest.mark.asyncio
    async def test_named_refs_read_catalog_without_uuid_visibility_calls(
        self, monkeypatch
    ):
        """Shared Catalog selectors use their stored revision, not image files."""
        never = AsyncMock(side_effect=AssertionError("must not be called"))
        for name in ("get_expert_by_id", "get_expert_visible_by_id", "get_user"):
            monkeypatch.setattr(
                orchestrator.main.app.state.resources.postgres_db, name, never
            )
        canonical = {"manifest_uid": "catalog-revision", "config": {}}
        read_catalog = AsyncMock(return_value=canonical)
        monkeypatch.setattr(
            "orchestrator.services.manifest_experts.bundled_expert_for_execution",
            read_catalog,
        )

        out = await session_config_resolution_module.prefetch_roster_refs(
            expert_row={
                "config": {"subagents": {"roster": {"e": {"$ref": _LIBRARY_REF}}}}
            },
            overrides=({"llm": {"model": "x"}}, None, {"subagents": "malformed"}),
            user_id="u",
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert out == {_LIBRARY_REF: canonical}
        read_catalog.assert_awaited_once_with(
            orchestrator.main.app.state.resources.postgres_db, _LIBRARY_REF
        )
        never.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expert_fragment_refs_are_fetched_by_id(self, monkeypatch):
        """A ref in the expert row was checked against its author at save; the
        row is the authority (JSONB delivered as a string is tolerated)."""
        import json

        row = {"id": self.EXPERT_REF, "name": "helper", "expert_type": "worker"}
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_expert_by_id",
            AsyncMock(return_value=row),
        )
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_expert_visible_by_id",
            AsyncMock(side_effect=AssertionError("expert refs are fetched by id")),
        )

        out = await session_config_resolution_module.prefetch_roster_refs(
            expert_row={
                "config": json.dumps(
                    {"subagents": {"roster": {"h": {"$ref": self.EXPERT_REF}}}}
                )
            },
            user_id="u",
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert out == {self.EXPERT_REF: row}

    @pytest.mark.asyncio
    async def test_override_refs_use_the_runners_visibility(self, monkeypatch):
        """A job/thread override was never save-checked: its refs resolve with
        the runner's visibility, so an override cannot pull another user's
        private expert into a job — the invisible one is simply absent."""
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_user",
            AsyncMock(return_value={"id": "u", "is_admin": False}),
        )

        async def visible(ref, *, user_id, project_ids, is_admin):
            assert (user_id, project_ids, is_admin) == ("u", ["p"], False)
            return {"id": ref, "name": "shared"} if ref == self.OVERRIDE_REF else None

        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_expert_visible_by_id",
            AsyncMock(side_effect=visible),
        )
        by_id = AsyncMock(side_effect=AssertionError("override refs never bypass"))
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db, "get_expert_by_id", by_id
        )

        out = await session_config_resolution_module.prefetch_roster_refs(
            overrides=(
                {
                    "subagents": {
                        "roster": {
                            "a": {"$ref": self.OVERRIDE_REF},
                            "b": {"$ref": self.HIDDEN_REF},
                        }
                    }
                },
            ),
            user_id="u",
            project_ids=["p"],
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert set(out) == {self.OVERRIDE_REF}
        by_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_override_refs_without_a_runner_fall_back_to_id(self, monkeypatch):
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_expert_by_id",
            AsyncMock(return_value={"id": self.OVERRIDE_REF}),
        )

        out = await session_config_resolution_module.prefetch_roster_refs(
            overrides=({"subagents": {"roster": {"a": {"$ref": self.OVERRIDE_REF}}}},),
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        assert set(out) == {self.OVERRIDE_REF}


# ---------------------------------------------------------------------------
# Route transport headers (subscription proxy → Claude Code)
#
# CLIProxyAPI's Claude executor sends `redact-thinking-…` on every upstream
# call unless the *inbound* request carries its own Anthropic-Beta list, and
# with that beta on Anthropic returns signature-only thinking blocks: the turn
# is billed for reasoning nobody can read. Dispatch is where the route is
# known, so dispatch is where the header is decided — from the model's
# `subscription_sources`, never from its name.
# ---------------------------------------------------------------------------

SUBS_ENDPOINT_ID = "33333333-3333-3333-3333-333333333333"
SUBS_BASE_URL = "http://srw-codex-proxy:8317/v1"


@pytest.fixture
def patched_main_subs(monkeypatch):
    """Two models on the *same* subscription endpoint, differing only in the
    upstream account that serves them."""

    sources = {
        "claude-opus-5": ("claude",),
        "gpt-5.6-sol": ("codex",),
        "legacy-row": (),  # imported before routing metadata existed
    }

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id in sources:
            return ModelMeta(
                model_id=model_id,
                provider="openai",
                family="default",
                display_name=model_id,
                origin="system",
                endpoint_id=SUBS_ENDPOINT_ID,
                api_key_ref="openai",
                transport_kind="subscription-proxy",
                subscription_sources=sources[model_id],
            )
        if model_id == "plain-endpoint-claude":
            # Same name shape, ordinary endpoint: must NOT get the header.
            return ModelMeta(
                model_id=model_id,
                provider="openai",
                family="claude-opus",
                display_name=model_id,
                origin="custom",
                endpoint_id=CTX_ENDPOINT_ID,
                api_key_ref="openai",
                subscription_sources=("claude",),
            )
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )

    async def fake_get_endpoint(endpoint_id):
        return {
            "id": endpoint_id,
            "label": "subscription-proxy",
            "base_url": SUBS_BASE_URL,
            "api_key": "sk-subs-test",
        }

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestRouteHeaderInjection:
    @pytest.mark.asyncio
    async def test_claude_sourced_model_gets_visible_thinking_betas(
        self, patched_main_subs
    ):
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "claude-opus-5"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        betas = result["llm"]["extra_headers"]["Anthropic-Beta"]
        assert "claude-code-20250219" in betas
        # The whole point: the redaction beta must be absent.
        assert "redact-thinking" not in betas

    @pytest.mark.asyncio
    async def test_codex_sourced_model_on_the_same_endpoint_gets_no_betas(
        self, patched_main_subs
    ):
        """One endpoint, many accounts — the header follows the account."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "gpt-5.6-sol"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["extra_headers"] is None

    @pytest.mark.asyncio
    async def test_row_without_routing_metadata_gets_no_betas(self, patched_main_subs):
        """Fails closed: an un-attributed row keeps its current behaviour."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "legacy-row"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["extra_headers"] is None

    @pytest.mark.asyncio
    async def test_claude_on_an_ordinary_endpoint_gets_no_betas(
        self, patched_main_subs
    ):
        """The header is a property of the proxy transport, not of Anthropic."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "plain-endpoint-claude"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["extra_headers"] is None

    @pytest.mark.asyncio
    async def test_headers_are_cleared_on_a_model_swap(self, patched_main_subs):
        """Swapping off a Claude account must CLEAR the header, not leave it —
        hence the explicit None sentinel the agent-side deep_merge acts on,
        the same contract provider/base_url/api_key already use."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "gpt-5.6-sol", "extra_headers": {"Anthropic-Beta": "x"}}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        # A caller-pinned value still wins; a *stale* one is what this guards,
        # so assert the shape the swap path produces from a clean section.
        result2 = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"model": "gpt-5.6-sol"}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result2["llm"]["extra_headers"] is None
        assert result["llm"]["extra_headers"] == {"Anthropic-Beta": "x"}

    @pytest.mark.asyncio
    async def test_caller_pinned_header_wins(self, patched_main_subs):
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {
                "llm": {
                    "model": "claude-opus-5",
                    "extra_headers": {"Anthropic-Beta": "operator-pinned"},
                }
            },
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert result["llm"]["extra_headers"]["Anthropic-Beta"] == "operator-pinned"

    @pytest.mark.asyncio
    async def test_nested_slot_gets_the_header_too(self, patched_main_subs):
        """A summarization/roster slot on a Claude account needs it as much as
        the main model does."""
        result = await job_dispatch_credentials_module.inject_dispatch_credentials(
            _job(),
            {"llm": {"summarization": {"model": "claude-opus-5"}}},
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        betas = result["llm"]["summarization"]["extra_headers"]["Anthropic-Beta"]
        assert "redact-thinking" not in betas


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["local", "openrouter"])
async def test_empty_install_quietly_skips_kb_embeddings(monkeypatch, caplog, provider):
    for name in ("EMBEDDING_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("EMBEDDING_PROVIDER", provider)
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )
    assert await _build_kb_embedding_service() is None
    assert not [r for r in caplog.records if r.levelno >= 30]
