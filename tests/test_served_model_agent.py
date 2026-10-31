"""Agent side of ``models.served_model_id`` (one model id, one row).

Design: knowledge-base/knowledge/issues/
same_model_id_on_two_providers_routes_to_the_api_key_row.md, rules 6, 7, 9.

The catalog id stays ``llm.model`` (family, matrix, usage); the dispatch puts
the row's upstream string into ``served_model`` (model sections) or
``{PREFIX}_SERVED_MODEL`` (flat env capabilities). The request carries the
served string; usage records the catalog id.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from shared.runtime.core.loader import (
    LLM_INHERITED_KEYS,
    AuxiliaryConfig,
    LLMConfig,
    PhaseLLMOverride,
    _create_anthropic_llm,
    _create_codex_llm,
    _create_google_llm,
    _create_groq_llm,
    _create_mistral_llm,
    _create_openai_llm,
    _create_openrouter_llm,
    _parse_auxiliary_config,
    _parse_llm_config,
    build_auxiliary_llm_config,
    create_llm,
    inherit_parent_llm,
    recorded_model_name,
    served_model_of,
    wire_model_name,
)
from shared.runtime.core.transport_resolution import (
    DISPATCH_ENV_KEY_NAMES,
    SERVED_MODEL_ENV_KEY_NAMES,
    catalog_model_of,
    clear_served_model_env,
    exportable_env_keys,
    served_model_from_env,
    stamp_catalog_model,
)

CATALOG_ID = "claude-opus-5-5-api"
SERVED = "claude-opus-5-5"


@pytest.fixture(autouse=True)
def _no_served_env(monkeypatch):
    for name in SERVED_MODEL_ENV_KEY_NAMES:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------
# Config: parse, overlay, inheritance
# ---------------------------------------------------------------------------


class TestConfigField:
    def test_llm_section_parses_served_model(self):
        cfg = _parse_llm_config({"model": CATALOG_ID, "served_model": SERVED})
        assert cfg.model == CATALOG_ID
        assert cfg.served_model == SERVED

    def test_absent_served_model_is_none(self):
        assert _parse_llm_config({"model": "gpt-4o"}).served_model is None

    def test_summarization_section_parses_served_model(self):
        cfg = _parse_llm_config(
            {
                "model": "main",
                "summarization": {"model": "aux-x", "served_model": "aux"},
            }
        )
        assert cfg.summarization.served_model == "aux"

    def test_auxiliary_section_reaches_its_llm_config(self):
        aux = _parse_auxiliary_config(
            {"model": "minimax-m3-256k", "served_model": "minimax-m3"}
        )
        assert aux.served_model == "minimax-m3"
        llm = build_auxiliary_llm_config(aux, {})
        assert llm.model == "minimax-m3-256k"
        assert llm.served_model == "minimax-m3"

    def test_auxiliary_default_is_none(self):
        assert AuxiliaryConfig().served_model is None


class TestOverlayInheritance:
    """A served string belongs to its model (rule 6, contract item 3)."""

    def _parent(self) -> LLMConfig:
        return LLMConfig(model=CATALOG_ID, served_model=SERVED, api_key="k")

    def test_overlay_without_a_model_keeps_the_parents_served_string(self):
        derived = self._parent().with_override(PhaseLLMOverride(temperature=0.5))
        assert derived.model == CATALOG_ID
        assert derived.served_model == SERVED

    def test_overlay_naming_the_same_model_keeps_it(self):
        derived = self._parent().with_override(PhaseLLMOverride(model=CATALOG_ID))
        assert derived.served_model == SERVED

    def test_overlay_with_its_own_model_never_inherits_it(self):
        derived = self._parent().with_override(PhaseLLMOverride(model="gpt-4o"))
        assert derived.model == "gpt-4o"
        assert derived.served_model is None

    def test_overlay_with_its_own_model_uses_its_own_served_string(self):
        derived = self._parent().with_override(
            PhaseLLMOverride(model="minimax-m3-256k", served_model="minimax-m3")
        )
        assert derived.served_model == "minimax-m3"

    def test_summarization_phase_with_another_model_sends_its_own_id(self):
        cfg = _parse_llm_config(
            {
                "model": CATALOG_ID,
                "served_model": SERVED,
                "summarization": {"model": "gpt-4o-mini"},
            }
        )
        summ = cfg.get_phase_config("summarization")
        assert summ.model == "gpt-4o-mini"
        assert summ.served_model is None
        assert wire_model_name(summ) == "gpt-4o-mini"

    def test_a_legacy_phase_name_is_the_model_itself(self):
        # strategic/tactical were folded into llm.model by normalize_llm_tiers,
        # so the phase config is the model's own, served string included.
        cfg = _parse_llm_config({"model": "A", "served_model": "a"})
        assert cfg.get_phase_config("strategic").served_model == "a"


class TestRosterInherit:
    def test_served_model_is_an_inherited_transport_key(self):
        assert "served_model" in LLM_INHERITED_KEYS

    def test_inherit_copies_the_parents_served_string(self):
        entry: dict = {"model": "inherit"}
        inherit_parent_llm(entry, {"model": CATALOG_ID, "served_model": SERVED})
        assert entry["model"] == CATALOG_ID
        assert entry["served_model"] == SERVED

    def test_a_parent_model_change_drops_the_old_served_string(self):
        entry = {"model": CATALOG_ID, "served_model": SERVED}
        inherit_parent_llm(entry, {"model": "gpt-4o"})
        assert entry["model"] == "gpt-4o"
        assert "served_model" not in entry

    def test_live_overlay_on_an_inherit_entry(self):
        from agent.subagents.child import overlay_live_llm

        child = LLMConfig(model=CATALOG_ID, served_model=None)
        live = LLMConfig(model=CATALOG_ID, served_model=SERVED)
        assert overlay_live_llm(child, live).served_model == SERVED
        # A fallback swap to another model takes its identity verbatim.
        swapped = LLMConfig(model="gpt-4o", served_model=None)
        child = LLMConfig(model=CATALOG_ID, served_model=SERVED)
        assert overlay_live_llm(child, swapped).served_model is None


class TestHelpers:
    def test_served_model_of_ignores_non_strings(self):
        mock = MagicMock()  # a MagicMock attribute is truthy, never a str
        assert served_model_of(mock) is None
        assert served_model_of(LLMConfig(model="x", served_model="")) is None
        assert served_model_of(LLMConfig(model="x", served_model="y")) == "y"

    def test_wire_model_name(self):
        assert wire_model_name(LLMConfig(model="gpt-4o")) == "gpt-4o"
        assert wire_model_name(LLMConfig(model=CATALOG_ID, served_model=SERVED)) == (
            SERVED
        )

    def test_recorded_model_name_prefers_the_catalog_id(self):
        cfg = LLMConfig(model=CATALOG_ID, served_model=SERVED)
        assert recorded_model_name(cfg, SERVED) == CATALOG_ID
        # No served string: the provider's echo is kept (dated snapshots etc.).
        assert recorded_model_name(LLMConfig(model="gpt-4o"), "gpt-4o-2024") == (
            "gpt-4o-2024"
        )
        assert recorded_model_name(None, "echo") == "echo"
        assert recorded_model_name(MagicMock(), "echo") == "echo"


# ---------------------------------------------------------------------------
# Factories send the served string
# ---------------------------------------------------------------------------


def _sent_model(mock_cls) -> str:
    return mock_cls.call_args.kwargs["model"]


class TestFactoriesSendTheServedString:
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openai(self, chat):
        cfg = LLMConfig(
            model="gpt-6.1-sol-proxy",
            served_model="gpt-6.1-sol",
            api_key="k",
            base_url="http://proxy.local/v1",
        )
        _create_openai_llm(cfg)
        assert _sent_model(chat) == "gpt-6.1-sol"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openai_without_served_sends_the_id(self, chat):
        _create_openai_llm(
            LLMConfig(model="gpt-4o", api_key="k", base_url="http://x/v1")
        )
        assert _sent_model(chat) == "gpt-4o"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openrouter_strips_the_prefix_from_the_served_string(self, chat):
        cfg = LLMConfig(
            model="openrouter/z-ai/glm-5.3-flash-cheap",
            served_model="openrouter/z-ai/glm-5.3-flash",
            api_key="sk-or",
        )
        _create_openrouter_llm(cfg)
        assert _sent_model(chat) == "z-ai/glm-5.3-flash"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openrouter_bare_served_string(self, chat):
        cfg = LLMConfig(
            model="z-ai/glm-5.3-flash-endpoint",
            served_model="z-ai/glm-5.3-flash",
            api_key="sk-or",
        )
        _create_openrouter_llm(cfg)
        assert _sent_model(chat) == "z-ai/glm-5.3-flash"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_mistral(self, chat):
        cfg = LLMConfig(
            model="mistral/mistral-large-x-eu",
            served_model="mistral/mistral-large-x",
            api_key="k",
        )
        _create_mistral_llm(cfg)
        assert _sent_model(chat) == "mistral-large-x"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_codex(self, chat):
        cfg = LLMConfig(
            model="codex/gpt-6.1-sol-team", served_model="codex/gpt-6.1-sol"
        )
        _create_codex_llm(cfg)
        assert _sent_model(chat) == "gpt-6.1-sol"

    @patch("langchain_groq.ChatGroq")
    def test_groq(self, chat):
        cfg = LLMConfig(
            model="groq/llama-5-70b-fast", served_model="groq/llama-5-70b", api_key="k"
        )
        _create_groq_llm(cfg)
        assert _sent_model(chat) == "llama-5-70b"

    @patch("langchain_anthropic.ChatAnthropic")
    def test_anthropic(self, chat):
        cfg = LLMConfig(model=CATALOG_ID, served_model=SERVED, api_key="sk-ant")
        _create_anthropic_llm(cfg)
        assert _sent_model(chat) == SERVED

    @patch("langchain_google_genai.ChatGoogleGenerativeAI")
    def test_google(self, chat):
        cfg = LLMConfig(
            model="gemini-4-argon-vertex", served_model="gemini-4-argon", api_key="k"
        )
        _create_google_llm(cfg)
        assert _sent_model(chat) == "gemini-4-argon"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_factory_log_names_both(self, chat, caplog):
        caplog.set_level("INFO", logger="shared.runtime.core.loader")
        _create_openai_llm(
            LLMConfig(
                model=CATALOG_ID, served_model=SERVED, api_key="k", base_url="http://x"
            )
        )
        assert f"model={CATALOG_ID}, served={SERVED}" in caplog.text


class TestCatalogStamp:
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_create_llm_stamps_the_catalog_id_for_a_served_row(self, chat):
        client = MagicMock(spec=[])
        chat.return_value = client
        create_llm(
            LLMConfig(
                model=CATALOG_ID,
                served_model=SERVED,
                provider="openai",
                api_key="k",
                base_url="http://x/v1",
            )
        )
        assert catalog_model_of(client) == CATALOG_ID

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_no_stamp_without_a_served_string(self, chat):
        client = MagicMock(spec=[])
        chat.return_value = client
        create_llm(LLMConfig(model="gpt-4o", provider="openai", api_key="k"))
        assert catalog_model_of(client) is None

    def test_auxiliary_archive_names_the_catalog_id(self):
        from langchain_openai import ChatOpenAI

        from shared.runtime.services.auxiliary import _get_model_name

        client = ChatOpenAI(model=SERVED, api_key="k")
        assert _get_model_name(client) == SERVED
        stamp_catalog_model(client, CATALOG_ID)
        assert _get_model_name(client) == CATALOG_ID


# ---------------------------------------------------------------------------
# Flat env capabilities: {PREFIX}_SERVED_MODEL
# ---------------------------------------------------------------------------


class TestServedEnvNames:
    @pytest.mark.parametrize(
        "prefix",
        ["EMBEDDING", "KB_EMBEDDING", "RERANK", "VISION", "WHISPER", "TTS"],
    )
    def test_every_prefix_is_exportable(self, prefix):
        name = f"{prefix}_SERVED_MODEL"
        assert name in DISPATCH_ENV_KEY_NAMES
        kept, dropped = exportable_env_keys({name: "x"})
        assert kept == {name: "x"} and dropped == []

    def test_citation_prefix(self):
        assert "CITATION_LLM_SERVED_MODEL" in DISPATCH_ENV_KEY_NAMES

    def test_empty_string_means_none(self):
        assert served_model_from_env("VISION", {"VISION_SERVED_MODEL": ""}) is None
        assert served_model_from_env("VISION", {}) is None
        assert served_model_from_env("VISION", {"VISION_SERVED_MODEL": "v"}) == "v"

    def test_clear_drops_every_served_name(self, monkeypatch):
        for name in SERVED_MODEL_ENV_KEY_NAMES:
            monkeypatch.setenv(name, "stale")
        monkeypatch.setenv("VISION_MODEL", "keep")
        clear_served_model_env()
        assert not any(name in os.environ for name in SERVED_MODEL_ENV_KEY_NAMES)
        assert os.environ["VISION_MODEL"] == "keep"

    def test_clear_only_the_named_prefixes(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_SERVED_MODEL", "e")
        monkeypatch.setenv("VISION_SERVED_MODEL", "v")
        clear_served_model_env(["EMBEDDING"])
        assert "EMBEDDING_SERVED_MODEL" not in os.environ
        assert os.environ["VISION_SERVED_MODEL"] == "v"


class TestEmbeddingService:
    def _service(self, **kw):
        from shared.runtime.services.embedding_service import EmbeddingService

        return EmbeddingService(api_key="k", base_url="http://emb.local/v1", **kw)

    def test_explicit_served_model_is_the_wire_name(self):
        svc = self._service(
            model="qwen/qwen3-embedding-8b-local",
            served_model="qwen/qwen3-embedding-8b",
        )
        assert svc.catalog_model == "qwen/qwen3-embedding-8b-local"
        assert svc.model == "qwen/qwen3-embedding-8b"

    def test_env_served_model_rides_with_the_env_model(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_MODEL", "qwen3-embedding-8b-gpu")
        monkeypatch.setenv("EMBEDDING_SERVED_MODEL", "qwen3-embedding-8b")
        assert self._service().model == "qwen3-embedding-8b"

    def test_env_served_model_is_ignored_for_an_explicit_model(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_SERVED_MODEL", "stale")
        assert self._service(model="text-embedding-4").model == "text-embedding-4"

    def test_openrouter_prefix_is_still_dropped(self):
        from shared.runtime.services.embedding_service import EmbeddingService

        svc = EmbeddingService(
            model="openrouter/qwen/qwen3-embedding-8b-cheap",
            served_model="openrouter/qwen/qwen3-embedding-8b",
            api_key="k",
            base_url="https://openrouter.ai/api/v1",
        )
        assert svc.model == "qwen/qwen3-embedding-8b"

    def test_kb_profile_rebuilds_when_the_served_string_changes(self, monkeypatch):
        from shared.runtime.services import embedding_service as mod

        monkeypatch.setattr(mod, "_kb_embedding_service", None)
        monkeypatch.setattr(mod, "_kb_embedding_profile", None)
        monkeypatch.setenv("KB_EMBEDDING_MODEL", "kb-emb-x")
        monkeypatch.setenv("KB_EMBEDDING_BASE_URL", "http://emb.local/v1")
        monkeypatch.setenv("KB_EMBEDDING_API_KEY", "k")
        monkeypatch.setenv("KB_EMBEDDING_SERVED_MODEL", "kb-emb")
        first = mod.get_kb_embedding_service()
        assert first.model == "kb-emb"
        monkeypatch.setenv("KB_EMBEDDING_SERVED_MODEL", "")
        second = mod.get_kb_embedding_service()
        assert second is not first
        assert second.model == "kb-emb-x"

    def test_kb_apply_clears_a_stale_served_string(self, monkeypatch):
        from shared.runtime.services import embedding_service as mod

        monkeypatch.setenv("KB_EMBEDDING_SERVED_MODEL", "stale")
        mod.apply_kb_embedding_env({"KB_EMBEDDING_MODEL": "kb"})
        assert "KB_EMBEDDING_SERVED_MODEL" not in os.environ
        mod.apply_kb_embedding_env(
            {"KB_EMBEDDING_MODEL": "kb", "KB_EMBEDDING_SERVED_MODEL": "kb-s"}
        )
        assert os.environ["KB_EMBEDDING_SERVED_MODEL"] == "kb-s"
        mod.apply_kb_embedding_env(None)


class TestHelpersReadTheServedEnv:
    def test_vision(self, monkeypatch):
        from agent.services.vision_helper import VisionHelper

        monkeypatch.setenv("VISION_BASE_URL", "http://vis.local/v1")
        monkeypatch.setenv("VISION_API_KEY", "k")
        monkeypatch.setenv("VISION_MODEL", "gpt-5-vision-eu")
        monkeypatch.setenv("VISION_SERVED_MODEL", "gpt-5-vision")
        assert VisionHelper().model == "gpt-5-vision"
        monkeypatch.setenv("VISION_SERVED_MODEL", "")
        assert VisionHelper().model == "gpt-5-vision-eu"

    def test_vision_served_without_a_model_is_ignored(self, monkeypatch):
        from agent.services.vision_helper import VisionHelper

        monkeypatch.delenv("VISION_MODEL", raising=False)
        monkeypatch.setenv("VISION_API_KEY", "k")
        monkeypatch.setenv("VISION_SERVED_MODEL", "orphan")
        assert VisionHelper().model == "gpt-4o"

    def test_whisper(self, monkeypatch):
        from agent.services.audio_helper import AudioHelper

        monkeypatch.setenv("WHISPER_BASE_URL", "http://stt.local/v1")
        monkeypatch.setenv("WHISPER_API_KEY", "k")
        monkeypatch.setenv("WHISPER_MODEL", "whisper-large-v4-gpu")
        monkeypatch.setenv("WHISPER_SERVED_MODEL", "whisper-large-v4")
        assert AudioHelper().model == "whisper-large-v4"

    def test_reranker(self):
        from types import SimpleNamespace

        from agent.services.memory.plugins.reranker import (
            RerankerScorer,
            resolve_reranker_served_model,
        )

        env = {
            "RERANK_MODEL": "qwen3-reranker-8b-gpu",
            "RERANK_SERVED_MODEL": "qwen3-reranker-8b",
            "RERANK_BASE_URL": "http://rr.local/v1",
        }
        cfg = SimpleNamespace(model=None, base_url=None, api_key=None)
        served = resolve_reranker_served_model(cfg, env)
        assert served == "qwen3-reranker-8b"
        scorer = RerankerScorer(
            model="qwen3-reranker-8b-gpu",
            served_model=served,
            base_url="http://rr.local/v1",
            api_key="k",
        )
        assert scorer.model == "qwen3-reranker-8b"
        # An explicit memory.reranker.model is sent as written.
        explicit = SimpleNamespace(model="my-reranker", base_url=None, api_key=None)
        assert resolve_reranker_served_model(explicit, env) is None
        # A served string without its model is never used.
        assert resolve_reranker_served_model(cfg, {"RERANK_SERVED_MODEL": "x"}) is None


class TestSessionAttachSeam:
    def test_attach_clears_every_served_name_then_applies_the_block(self, monkeypatch):
        from agent.api.session_attach import apply_session_embedding_env

        monkeypatch.setenv("VISION_SERVED_MODEL", "previous-tenant")
        monkeypatch.setenv("EMBEDDING_SERVED_MODEL", "previous-tenant")
        apply_session_embedding_env(
            {
                "EMBEDDING_MODEL": "qwen3-embedding-8b-gpu",
                "EMBEDDING_SERVED_MODEL": "qwen3-embedding-8b",
                "RERANK_MODEL": "rr",
                "RERANK_SERVED_MODEL": "",
            }
        )
        try:
            assert "VISION_SERVED_MODEL" not in os.environ
            assert os.environ["EMBEDDING_SERVED_MODEL"] == "qwen3-embedding-8b"
            assert served_model_from_env("RERANK") is None
        finally:
            apply_session_embedding_env(None)
        assert "EMBEDDING_SERVED_MODEL" not in os.environ


class TestJobExportSeam:
    def test_job_export_clears_served_names_before_applying_the_block(self):
        """A reused worker pod: the previous job's served string must not pair
        with the next job's {PREFIX}_MODEL."""
        from inspect import getsource

        from agent.agent import UniversalAgent

        src = getsource(UniversalAgent._setup_job_workspace)
        assert src.index("clear_served_model_env()") < src.index(
            "apply_kb_embedding_env(env_keys)"
        )
        assert src.index("clear_served_model_env()") < src.index("_os.environ[k] = v")
