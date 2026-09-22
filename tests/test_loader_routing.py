"""Tests for LLM provider routing based on model name prefix."""

import os
from unittest.mock import patch, MagicMock

import pytest

from shared.runtime.core.loader import (
    _should_use_reasoning_summary,
    _clamp_reasoning_level,
    _supported_efforts,
    _OPENAI_REASONING_LEVELS,
    _create_openai_llm,
    _create_openrouter_llm,
    _create_codex_llm,
    detect_reasoning_method,
    reasoning_capability,
    resolve_reasoning_plan,
    supports_parallel_tool_calls,
)


def _make_config(**overrides):
    """Create a mock LLMConfig with sensible defaults."""
    config = MagicMock()
    config.model = overrides.get("model", "gpt-4o")
    config.base_url = overrides.get("base_url", None)
    config.api_key = overrides.get("api_key", None)
    config.temperature = overrides.get("temperature", 0.0)
    config.top_p = overrides.get("top_p", None)
    config.top_k = overrides.get("top_k", None)
    config.max_retries = overrides.get("max_retries", 2)
    config.timeout = overrides.get("timeout", None)
    config.reasoning_level = overrides.get("reasoning_level", None)
    config.streaming = overrides.get("streaming", False)
    config.max_output_tokens = overrides.get("max_output_tokens", None)
    config.model_max_context_tokens = overrides.get("model_max_context_tokens", None)
    config.extra_body = overrides.get("extra_body", None)
    # Real value, not MagicMock truthiness — the factory's header arm gates on
    # `if config.extra_headers`.
    config.extra_headers = overrides.get("extra_headers", None)
    # Real attribute, not MagicMock truthiness — the factory's cache-key arm
    # gates on `if config.prompt_cache_key`.
    config.prompt_cache_key = overrides.get("prompt_cache_key", None)
    return config


# Patches applied to all routing integration tests
_common_patches = [
    patch("shared.runtime.core.loader.ReasoningChatOpenAI"),
    patch("shared.runtime.llm.key_ring.KeyRing", new_callable=MagicMock),
]


class TestOpenAILLMRouting:
    """Integration tests for base_url routing in _create_openai_llm.

    Post chunk-6 (models_yaml_removal), base_url resolution collapsed to
    a single source: ``config.base_url`` (dispatcher-injected from the
    catalog row's transport). The legacy YAML fallback that read
    ``LLM_BASE_URL`` for self-hosted "Local" group entries is gone — the
    orchestrator hard-fails at boot when ``LLM_BASE_URL`` is set, so the
    var being present in the test environment doesn't reach the loader.
    Self-hosted models now MUST come through a catalog row whose
    ``provider_kind='endpoint'`` row supplies the base_url at dispatch.
    """

    _LOCAL_MODEL = "RedHatAI/gemma-4-31B-it-FP8-Dynamic"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_self_hosted_model_uses_dispatcher_injected_base_url(self, mock_chat):
        """Self-hosted models receive base_url via dispatcher-injected
        config.base_url — not via the deleted LLM_BASE_URL fallback."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model=self._LOCAL_MODEL,
            base_url="http://my-vllm.cluster.local:8080/v1",
        )
        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["base_url"] == "http://my-vllm.cluster.local:8080/v1"
        # Regression: the original bug was that the openai/ prefix
        # leaked into the wire name and vLLM 404'd. The bare ID must
        # reach the SDK untouched.
        assert call_kwargs["model"] == self._LOCAL_MODEL
        assert not call_kwargs["model"].startswith("openai/")

    @patch.dict(
        os.environ, {"LLM_BASE_URL": "http://stale-leftover:8080/v1"}, clear=False
    )
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_env_var_does_not_leak_into_native_openai_models(self, mock_chat):
        """A stale LLM_BASE_URL in the test env (the orchestrator boot
        check would refuse to start in production) must NOT reach the
        OpenAI factory — chunk 6 deleted the env-var inheritance path."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.2-pro")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "base_url" not in call_kwargs

    @patch.dict(
        os.environ, {"LLM_BASE_URL": "http://stale-leftover:8080/v1"}, clear=False
    )
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gpt4o_routes_to_native_openai(self, mock_chat):
        """gpt-4o is native — no base_url override, ever."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-4o")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "base_url" not in call_kwargs

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_explicit_base_url_always_wins(self, mock_chat):
        """Explicit config.base_url is the dispatcher-injection path."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-4o", base_url="http://custom-proxy:9000/v1")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["base_url"] == "http://custom-proxy:9000/v1"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_self_hosted_without_dispatcher_injection_falls_to_native(self, mock_chat):
        """Self-hosted ID with no base_url leaks through to api.openai.com.

        This is intentional post-chunk-6: catch-all behavior at the loader
        is API-OpenAI-default, and the readiness gate (chunk 5) blocks
        catalog-row-less models from being dispatched in the first place.
        Test pins the absence of any env-driven fallback magic."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model=self._LOCAL_MODEL)
        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "base_url" not in call_kwargs


class TestShouldUseReasoningSummary:
    """Unit tests for the _should_use_reasoning_summary helper."""

    def test_o1_model(self):
        assert _should_use_reasoning_summary("o1-preview") is True
        assert _should_use_reasoning_summary("o1-mini") is True

    def test_o3_model(self):
        assert _should_use_reasoning_summary("o3-mini") is True
        assert _should_use_reasoning_summary("o3-pro") is True

    def test_o4_model(self):
        assert _should_use_reasoning_summary("o4-mini") is True

    def test_gpt5_model(self):
        assert _should_use_reasoning_summary("gpt-5.2-pro") is True
        assert _should_use_reasoning_summary("gpt-5") is True

    def test_gpt6_model(self):
        # Astra serves tool calls ONLY on the Responses API, and `max` effort
        # exists only there — so it must take the reasoning-summary path.
        assert _should_use_reasoning_summary("gpt-6-astra") is True

    def test_case_insensitive(self):
        assert _should_use_reasoning_summary("GPT-5.2-pro") is True
        assert _should_use_reasoning_summary("O3-mini") is True
        assert _should_use_reasoning_summary("GPT-6-Astra") is True

    def test_proxy_models_excluded(self):
        """Models with / are proxy models and should not use Responses API."""
        assert _should_use_reasoning_summary("openai/gpt-oss-120b") is False
        assert _should_use_reasoning_summary("groq/llama-3") is False

    def test_non_reasoning_models(self):
        assert _should_use_reasoning_summary("gpt-4o") is False
        assert _should_use_reasoning_summary("gpt-4o-mini") is False
        assert _should_use_reasoning_summary("claude-3-opus") is False
        assert _should_use_reasoning_summary("gemini-pro") is False

    def test_deepseek_excluded(self):
        assert _should_use_reasoning_summary("deepseek-reasoner") is False


class TestReasoningSummaryRouting:
    """Integration tests verifying correct reasoning kwargs reach ReasoningChatOpenAI."""

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gpt5_gets_reasoning_effort(self, mock_chat):
        """gpt-5.2-pro should use Chat Completions reasoning_effort (not Responses API)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.2-pro", reasoning_level="high")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "high"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_o3_gets_reasoning_effort(self, mock_chat):
        """o3-mini should use Chat Completions reasoning_effort (not Responses API)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="o3-mini", reasoning_level="medium")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "medium"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gpt_oss_is_prompt_delivered_not_api(self, mock_chat):
        """gpt-oss reasoning rides the system-prompt `Reasoning:` line
        (detect_reasoning_method=='prompt'), so the OpenAI factory must NOT also
        inject an API reasoning_effort — that was the double-injection bug."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="openai/gpt-oss-120b", reasoning_level="high")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert "reasoning_effort" not in call_kwargs.get("model_kwargs", {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_no_reasoning_when_none(self, mock_chat):
        """reasoning_level='none' should skip both paths."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.2-pro", reasoning_level="none")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert "model_kwargs" not in call_kwargs

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_no_reasoning_when_unset(self, mock_chat):
        """No reasoning_level should skip both paths."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-4o", reasoning_level=None)

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert "model_kwargs" not in call_kwargs


class TestOpenRouterLLMCreation:
    """Integration tests for _create_openrouter_llm."""

    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-or-test-key"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_strips_prefix_and_sets_base_url(self, mock_chat):
        """Should strip openrouter/ prefix and use OpenRouter base URL."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="openrouter/anthropic/claude-opus-4")

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model"] == "anthropic/claude-opus-4"
        assert call_kwargs["base_url"] == "https://openrouter.ai/api/v1"
        assert call_kwargs["use_responses_api"] is False

    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-or-test-key"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_explicit_base_url_overrides(self, mock_chat):
        """Explicit config.base_url should override the default OpenRouter URL."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/openai/gpt-4o",
            base_url="https://custom-proxy.example.com/v1",
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["base_url"] == "https://custom-proxy.example.com/v1"

    @patch.dict(
        os.environ,
        {
            "OPENROUTER_API_KEY": "sk-or-test-key",
            "OPENROUTER_REFERER": "https://my-app.com",
            "OPENROUTER_TITLE": "My Agent",
        },
        clear=False,
    )
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_custom_headers(self, mock_chat):
        """Should pass HTTP-Referer and X-Title headers when env vars are set."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="openrouter/openai/gpt-4o")

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["default_headers"] == {
            "HTTP-Referer": "https://my-app.com",
            "X-Title": "My Agent",
        }

    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-or-test-key"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_reasoning_in_model_kwargs(self, mock_chat):
        """Reasoning should use nested reasoning object for OpenRouter."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/deepseek/deepseek-r1", reasoning_level="high"
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        # reasoning rides in extra_body: a first-class `reasoning` kwarg lands
        # in the Chat Completions payload on langchain-openai >= 1.x and the
        # OpenAI SDK's typed create() rejects it with a TypeError.
        assert call_kwargs["extra_body"]["reasoning"] == {"effort": "high"}
        assert call_kwargs["use_responses_api"] is False
        assert "reasoning" not in call_kwargs
        assert "reasoning" not in call_kwargs.get("model_kwargs", {})

    def test_missing_api_key_raises(self):
        """Should raise ValueError when OPENROUTER_API_KEY is not set."""
        env = os.environ.copy()
        env.pop("OPENROUTER_API_KEY", None)

        with patch.dict(os.environ, env, clear=True):
            config = _make_config(model="openrouter/openai/gpt-4o")
            with pytest.raises(ValueError, match="OPENROUTER_API_KEY"):
                _create_openrouter_llm(config, limits=None)

    @patch.dict(
        os.environ, {"OPENROUTER_API_KEY": "sk-or-key1,sk-or-key2"}, clear=False
    )
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_multiple_keys(self, mock_chat):
        """Should support comma-separated keys for rotation."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="openrouter/openai/gpt-4o")

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        # First key should be passed to SDK
        assert call_kwargs["api_key"] == "sk-or-key1"


class TestReasoningLevelClamping:
    """Tests for _clamp_reasoning_level."""

    def test_supported_levels_unchanged(self):
        """low/medium/high pass through for OpenAI."""
        for level in ("low", "medium", "high"):
            assert _clamp_reasoning_level(level, _OPENAI_REASONING_LEVELS) == level

    def test_xhigh_clamped_to_high(self):
        """xhigh -> high for OpenAI."""
        assert _clamp_reasoning_level("xhigh", _OPENAI_REASONING_LEVELS) == "high"

    def test_minimal_clamped_to_low(self):
        """minimal -> low for OpenAI."""
        assert _clamp_reasoning_level("minimal", _OPENAI_REASONING_LEVELS) == "low"

    def test_unknown_level_falls_back_to_high(self):
        """Unknown levels should fall back to high."""
        assert _clamp_reasoning_level("turbo", _OPENAI_REASONING_LEVELS) == "high"

    def test_max_clamped_to_high_when_unsupported(self):
        """max walks the ladder down past xhigh to high on a low/medium/high family."""
        assert _clamp_reasoning_level("max", {"low", "medium", "high"}) == "high"

    def test_max_clamps_down_to_xhigh_first(self):
        """The nearest supported level below wins — never skip past one."""
        assert (
            _clamp_reasoning_level("max", {"low", "medium", "high", "xhigh"}) == "xhigh"
        )

    def test_xhigh_and_max_pass_when_supported(self):
        gpt56 = {"low", "medium", "high", "xhigh", "max"}
        assert _clamp_reasoning_level("xhigh", gpt56) == "xhigh"
        assert _clamp_reasoning_level("max", gpt56) == "max"

    def test_level_is_case_insensitive(self):
        assert (
            _clamp_reasoning_level("XHigh", {"low", "medium", "high", "xhigh"})
            == "xhigh"
        )


class TestSupportedEfforts:
    """_supported_efforts derives the clamp set from the family capability."""

    def test_reads_family_options(self):
        cap = {"options": ["low", "medium", "high", "xhigh", "max"]}
        assert _supported_efforts(cap) == {"low", "medium", "high", "xhigh", "max"}

    def test_falls_back_to_openai_set(self):
        assert _supported_efforts({}) == _OPENAI_REASONING_LEVELS
        assert _supported_efforts({"options": []}) == _OPENAI_REASONING_LEVELS

    def test_normalizes_case(self):
        assert _supported_efforts({"options": ["Low", "HIGH"]}) == {"low", "high"}


class TestFamilyOptionsDriveClamp:
    """The matrix `reasoning.options` — not a hardcoded transport set — decide
    what effort reaches the wire (knowledge-history/done/family_centered_reasoning.md)."""

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_deepseek_xhigh_survives_openai_factory(self, mock_chat):
        """deepseek declares xhigh in its options → no clamp on the OpenAI wire."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="deepseek-v4", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "xhigh"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gpt5_xhigh_still_clamped(self, mock_chat):
        """gpt-5 family still lists only low/medium/high → xhigh clamps to high."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.2-pro", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "high"


class TestGpt56Reasoning:
    """gpt-5.6 family: xhigh/max are declared in the matrix and survive the
    codex (Responses API) path un-clamped."""

    def test_capability_lists_xhigh_and_max(self):
        cap = reasoning_capability("gpt-5.6-sol")
        assert cap["method"] == "effort_enum"
        assert cap["default"] == "high"
        assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_reaches_codex_responses_api(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.6-sol", reasoning_level="max")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["reasoning"] == {"effort": "max", "summary": "auto"}

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_reaches_codex_responses_api(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-5.6-terra", reasoning_level="xhigh")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["reasoning"] == {"effort": "xhigh", "summary": "auto"}


class TestGpt6Reasoning:
    """gpt-6 (Astra): xhigh/max are declared in the matrix and reach the codex
    (Responses API) path un-clamped, with the reasoning summary requested."""

    def test_capability_lists_xhigh_and_max(self):
        cap = reasoning_capability("gpt-6-astra")
        assert cap["method"] == "effort_enum"
        assert cap["default"] == "high"
        assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_reaches_codex_responses_api(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-6-astra", reasoning_level="max")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        # Responses-API shape, NOT a Chat-Completions `reasoning_effort`:
        # `max` is Responses-only, so the flat form would silently degrade it.
        assert call_kwargs["reasoning"] == {"effort": "max", "summary": "auto"}
        assert "model_kwargs" not in call_kwargs or (
            "reasoning_effort" not in call_kwargs.get("model_kwargs", {})
        )

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_reaches_codex_responses_api(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-6-astra", reasoning_level="xhigh")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["reasoning"] == {"effort": "xhigh", "summary": "auto"}

    def test_none_injects_nothing_rather_than_400ing(self):
        """Astra rejects `none` with HTTP 400 — the plan must inject nothing
        instead of putting the literal on the wire."""
        config = _make_config(model="gpt-6-astra", reasoning_level="none")
        plan = resolve_reasoning_plan(config)
        assert plan["method"] == "effort_enum"
        assert plan["value"] is None


class TestClaudeOpus5Reasoning:
    """claude-opus-5 is its own matrix family: it is the only Opus that accepts
    the full effort ladder, and both OpenAI-shaped factories must carry xhigh /
    max through un-clamped while the generic `claude-opus` family stays at
    low/medium/high for the 4.x rows it still serves."""

    def test_capability_lists_xhigh_and_max(self):
        cap = reasoning_capability("claude-opus-5")
        assert cap["method"] == "effort_enum"
        assert cap["default"] == "high"
        assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]

    def test_older_opus_rows_stay_on_the_narrow_ladder(self):
        # Regression guard: widening the shared family instead of splitting it
        # would offer 4.x Opus rows a level their wire rejects.
        assert reasoning_capability("claude-opus-4-8")["options"] == [
            "low",
            "medium",
            "high",
        ]

    def test_settings_match_the_generic_opus_family(self):
        # A family block falls through to `default`, never to a sibling — if the
        # settings were dropped here Opus 5 would silently become non-multimodal
        # with a 128k window.
        from shared.runtime.core.loader import _apply_settings_matrix

        five = {"llm": {"model": "claude-opus-5"}}
        four = {"llm": {"model": "claude-opus-4-8"}}
        _apply_settings_matrix(five, expert_llm_keys=set())
        _apply_settings_matrix(four, expert_llm_keys=set())
        five["llm"].pop("model")
        four["llm"].pop("model")
        assert five == four

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_survives_the_chat_completions_factory(self, mock_chat):
        # Subscription-proxy Claude rows speak openai-chat; CLIProxyAPI maps
        # `reasoning_effort` onto Anthropic thinking + output_config.effort.
        mock_chat.return_value = MagicMock()
        config = _make_config(model="claude-opus-5", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "xhigh"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_survives_the_codex_factory_as_a_flat_effort(self, mock_chat):
        # A proxy row left on openai-responses still routes here. Claude is not
        # a reasoning-summary model, so the flat Chat-Completions field is the
        # correct shape — what matters is that `max` is not clamped away.
        mock_chat.return_value = MagicMock()
        config = _make_config(model="claude-opus-5", reasoning_level="max")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "max"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_on_an_older_opus_still_clamps(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="claude-opus-4-8", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "high"


class TestClaudeOpus55Reasoning:
    """claude-opus-5-5 splits off claude-opus-5 for its default: Anthropic
    calibrated 5.5 to `medium` (every other effort model defaults to `high`),
    and it thinks more per turn at a given level. Ladder and settings are
    otherwise Opus 5's."""

    def test_capability_defaults_to_medium_on_the_full_ladder(self):
        cap = reasoning_capability("claude-opus-5-5")
        assert cap["method"] == "effort_enum"
        assert cap["default"] == "medium"
        assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]

    def test_openrouter_dotted_id_resolves_the_same_capability(self):
        assert reasoning_capability("openrouter/anthropic/claude-opus-5.5") == (
            reasoning_capability("claude-opus-5-5")
        )

    def test_opus_5_keeps_its_high_default(self):
        # Regression guard: the split must not leak medium back onto Opus 5.
        assert reasoning_capability("claude-opus-5")["default"] == "high"

    def test_settings_match_opus_5(self):
        # A family block falls through to `default`, never to a sibling — if the
        # settings were dropped here Opus 5.5 would silently become
        # non-multimodal with a 128k window.
        from shared.runtime.core.loader import _apply_settings_matrix

        five_five = {"llm": {"model": "claude-opus-5-5"}}
        five = {"llm": {"model": "claude-opus-5"}}
        _apply_settings_matrix(five_five, expert_llm_keys=set())
        _apply_settings_matrix(five, expert_llm_keys=set())
        five_five["llm"].pop("model")
        five["llm"].pop("model")
        assert five_five == five

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_survives_the_chat_completions_factory(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="claude-opus-5-5", reasoning_level="max")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "max"


class TestClaudeFableReasoning:
    """claude-fable covers Fable 5 and 5.1 in one family: identical matrix
    knobs, and the full effort ladder on both."""

    def test_capability_lists_the_full_ladder(self):
        cap = reasoning_capability("claude-fable-5")
        assert cap["method"] == "effort_enum"
        assert cap["default"] == "high"
        assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]

    def test_five_and_five_one_share_the_family(self):
        assert reasoning_capability("claude-fable-5-1") == reasoning_capability(
            "claude-fable-5"
        )

    def test_settings_are_declared_not_inherited(self):
        # Falling through to `default` would make Fable non-multimodal on a
        # 128k window — the trap every Claude family block here exists to avoid.
        from shared.runtime.core.loader import _apply_settings_matrix

        data = {"llm": {"model": "claude-fable-5-1"}}
        _apply_settings_matrix(data, expert_llm_keys=set())
        assert data["llm"]["multimodal"] is True
        assert data["llm"]["model_max_context_tokens"] == 1_000_000
        assert data["limits"]["image_tokens"]["mode"] == "anthropic_patches"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_survives_the_chat_completions_factory(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="claude-fable-5", reasoning_level="max")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "max"


class TestFallThroughLadderIncludesXhigh:
    """The `default` family declares xhigh (2026-09-11), so a model with no
    family block of its own can be run at xhigh instead of being clamped down
    to high. `max` is deliberately still out — it clamps to xhigh, not high."""

    def test_unknown_model_offers_xhigh(self):
        cap = reasoning_capability("some-unknown-model")
        assert cap["options"] == ["low", "medium", "high", "xhigh"]

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_reaches_the_wire_unclamped(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="some-unknown-model", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "xhigh"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_max_clamps_to_xhigh_not_high(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="some-unknown-model", reasoning_level="max")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "xhigh"

    def test_narrow_families_are_unaffected(self):
        # Widening the fall-through must not leak into a family that declares
        # its own (narrower) ladder.
        assert reasoning_capability("gpt-5.2-pro")["options"] == [
            "low",
            "medium",
            "high",
        ]
        assert reasoning_capability("claude-opus-4-8")["options"] == [
            "low",
            "medium",
            "high",
        ]


class TestOpenAIReasoningClamping:
    """Integration tests verifying clamping reaches ReasoningChatOpenAI for OpenAI."""

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_clamped_to_high(self, mock_chat):
        """xhigh should be clamped to high for a native-effort OpenAI model.
        (gpt-oss is prompt-delivered, so an effort_enum/native family is used.)"""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.2-pro", reasoning_level="xhigh")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "high"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_minimal_clamped_to_low(self, mock_chat):
        """minimal should be clamped to low for OpenAI reasoning models."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="o3-mini", reasoning_level="minimal")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "low"


class TestOpenRouterReasoningFormat:
    """Verify OpenRouter gets nested reasoning object without clamping."""

    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-or-test-key"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_xhigh_not_clamped(self, mock_chat):
        """OpenRouter should pass xhigh through without clamping."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/deepseek/deepseek-r1", reasoning_level="xhigh"
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["reasoning"] == {"effort": "xhigh"}
        assert "reasoning" not in call_kwargs
        assert call_kwargs["use_responses_api"] is False

    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "sk-or-test-key"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_minimal_not_clamped(self, mock_chat):
        """OpenRouter should pass minimal through without clamping."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/z-ai/glm-5.2", reasoning_level="minimal"
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["reasoning"] == {"effort": "minimal"}
        assert "reasoning" not in call_kwargs
        assert call_kwargs["use_responses_api"] is False


class TestCodexLLMCreation:
    """Integration tests for _create_codex_llm."""

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_strips_prefix_and_sets_default_base_url(self, mock_chat):
        """Should strip codex/ prefix and use default CLIProxyAPI base URL."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-5.4-pro")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["model"] == "gpt-5.4-pro"
        assert call_kwargs["base_url"] == "http://localhost:8317/v1"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_explicit_base_url_overrides(self, mock_chat):
        """Explicit config.base_url should override env and default."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="codex/gpt-4o",
            base_url="http://custom-proxy:9000/v1",
        )

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["base_url"] == "http://custom-proxy:9000/v1"

    @patch.dict(os.environ, {"CODEX_BASE_URL": "http://remote:8317/v1"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_env_base_url_overrides_default(self, mock_chat):
        """CODEX_BASE_URL env var should override the default."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-4o")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["base_url"] == "http://remote:8317/v1"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_default_api_key_is_not_needed(self, mock_chat):
        """When no env var set, API key should default to 'not-needed'."""
        mock_chat.return_value = MagicMock()
        env = os.environ.copy()
        env.pop("CODEX_API_KEY", None)

        with patch.dict(os.environ, env, clear=True):
            config = _make_config(model="codex/gpt-5.4-pro")
            _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["api_key"] == "not-needed"

    @patch.dict(os.environ, {"CODEX_API_KEY": "sk-codex-test"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_explicit_api_key_from_env(self, mock_chat):
        """CODEX_API_KEY env var should be used when set."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-4o")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["api_key"] == "sk-codex-test"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_reasoning_uses_responses_api_for_native_models(self, mock_chat):
        """Native OpenAI reasoning models use Responses API (Codex proxy requires it)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/o3-pro", reasoning_level="high")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["reasoning"] == {"effort": "high", "summary": "auto"}
        assert "reasoning_effort" not in call_kwargs.get("model_kwargs", {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_reasoning_uses_chat_completions_for_proxy_models(self, mock_chat):
        """Non-native models (with / in name after prefix strip) use chat completions."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/some-custom/model", reasoning_level="high")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning" not in call_kwargs
        assert call_kwargs["model_kwargs"]["reasoning_effort"] == "high"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_reasoning_clamped(self, mock_chat):
        """xhigh should be clamped to high for Codex (OpenAI limits)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-5.4-pro", reasoning_level="xhigh")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["reasoning"] == {"effort": "high", "summary": "auto"}

    @patch.dict(os.environ, {"CODEX_API_KEY": "sk-key1,sk-key2"}, clear=False)
    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_multiple_keys(self, mock_chat):
        """Should support comma-separated keys for rotation."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-4o")

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["api_key"] == "sk-key1"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_top_k_not_forwarded(self, mock_chat):
        """top_k must NOT be forwarded — the Codex proxy speaks ONLY the
        Responses API, which rejects top_k with 400 'Unsupported parameter:
        top_k'. A stale top_k from a prior family (e.g. gemma's 64 carried
        onto a session switch to gpt-5.5/codex) must be dropped here because
        the codex lane talks directly to the Responses-only proxy and must
        self-sanitize."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="codex/gpt-5.5", top_k=64)

        _create_codex_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "top_k" not in call_kwargs.get("extra_body", {})
        assert "top_k" not in call_kwargs.get("model_kwargs", {})


class TestSupportsParallelToolCalls:
    """`parallel_tool_calls` is an OpenAI Chat Completions kwarg — it must be
    suppressed for providers/models that reject it."""

    @pytest.mark.parametrize(
        "provider,model",
        [
            ("openai", "gpt-4o"),
            ("openrouter", "openrouter/anthropic/claude-sonnet-4"),
            ("codex", "codex/gpt-5.5"),
            ("groq", "moonshotai/kimi-k2-instruct-0905"),
            ("anthropic", "claude-opus-4-5"),
            (None, "gpt-4o"),  # provider defaults to openai-compatible
        ],
    )
    def test_supported(self, provider, model):
        assert supports_parallel_tool_calls(provider, model) is True

    @pytest.mark.parametrize(
        "provider,model",
        [
            ("google", "gemini-3.5-flash"),  # GenerateContentConfig: extra_forbidden
            ("Google", "gemini-2.0-pro"),  # case-insensitive
            ("openai", "o1-preview"),  # o-series reasoning models
            ("openai", "o3-mini"),
            ("openai", "o4-mini"),
        ],
    )
    def test_suppressed(self, provider, model):
        assert supports_parallel_tool_calls(provider, model) is False

    def test_none_inputs_default_to_supported(self):
        assert supports_parallel_tool_calls(None, None) is True


class TestFamilyCenteredReasoning:
    """Family-driven reasoning capability + delivery
    (knowledge-base/knowledge/features/family_centered_reasoning.md)."""

    def test_capability_method_per_family(self):
        assert reasoning_capability("gemma-4-moe")["method"] == "binary_toggle"
        assert reasoning_capability("gpt-5.2-pro")["method"] == "effort_enum"
        assert reasoning_capability("openai/gpt-oss-120b")["delivery"] == "prompt"
        assert reasoning_capability("minimax-m2.7")["method"] == "none"
        assert reasoning_capability("claude-opus-4-8")["method"] == "effort_enum"
        # Unknown family falls through to the `default` block (effort_enum).
        assert reasoning_capability("some-unknown-model")["method"] == "effort_enum"

    def test_detect_reasoning_method_from_capability(self):
        assert detect_reasoning_method("gpt-5.2-pro") == "api"
        assert detect_reasoning_method("openai/gpt-oss-120b") == "prompt"
        assert detect_reasoning_method("gemma-4-moe") == "none"
        assert detect_reasoning_method("claude-opus-4-8") == "api"
        # Explicit override still wins.
        assert detect_reasoning_method("gemma-4-moe", explicit_method="api") == "api"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_claude_requests_an_effort_level(self, mock_chat):
        """Claude reaches adaptive thinking via `reasoning_effort`.

        Sending nothing is not "provider default": it leaves `thinking.type`
        unset, so the subscription proxy never asks for a visible summary and
        Claude reasons invisibly (thinking_tokens billed, empty blocks
        returned). Verified live 2026-09-07 — see
        knowledge-base/knowledge/features/subscription_proxy.md §13.
        """
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="claude-opus-5",
            base_url="http://srw-codex-proxy:8317/v1",
            reasoning_level="high",
        )

        _create_openai_llm(config, limits=None)

        assert mock_chat.call_args[1]["model_kwargs"]["reasoning_effort"] == "high"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_claude_unset_level_injects_nothing(self, mock_chat):
        """Shared effort_enum contract: an unset level is not re-defaulted here,
        so a caller that never asked for reasoning does not silently start
        paying for it. Claude then behaves as it does today — it still thinks,
        just invisibly."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="claude-opus-5",
            base_url="http://srw-codex-proxy:8317/v1",
            reasoning_level=None,
        )

        _create_openai_llm(config, limits=None)

        assert "reasoning_effort" not in (
            mock_chat.call_args[1].get("model_kwargs") or {}
        )

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_route_headers_reach_the_client(self, mock_chat):
        """Dispatch-injected transport headers become the client's
        default_headers — the other half of the Claude reasoning fix."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="claude-opus-5",
            base_url="http://srw-codex-proxy:8317/v1",
            extra_headers={"Anthropic-Beta": "claude-code-20250219"},
        )

        _create_openai_llm(config, limits=None)

        assert mock_chat.call_args[1]["default_headers"] == {
            "Anthropic-Beta": "claude-code-20250219"
        }

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_no_route_headers_leaves_the_client_untouched(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-5.6-sol", base_url="http://proxy/v1")

        _create_openai_llm(config, limits=None)

        assert "default_headers" not in mock_chat.call_args[1]

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gemma_enables_thinking_no_effort(self, mock_chat):
        """gemma (binary_toggle) → chat_template_kwargs.enable_thinking=True, and
        NO inert reasoning_effort (the bug this feature fixes)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="gemma-4-moe",
            base_url="http://vllm.cluster:8080/v1",
            reasoning_level="high",
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["chat_template_kwargs"] == {
            "enable_thinking": True
        }
        assert "reasoning_effort" not in call_kwargs.get("model_kwargs", {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gemma_thinking_off_when_level_none(self, mock_chat):
        """reasoning_level='none' on gemma → enable_thinking=False."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="gemma-4-moe",
            base_url="http://vllm.cluster:8080/v1",
            reasoning_level="none",
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["chat_template_kwargs"] == {
            "enable_thinking": False
        }

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_gemma_thinking_on_when_level_unset(self, mock_chat):
        """Unset level on gemma falls back to the family default (ON), so SRW
        gets reasoning regardless of the upstream endpoint default."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="gemma-4-moe",
            base_url="http://vllm.cluster:8080/v1",
            reasoning_level=None,
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["chat_template_kwargs"] == {
            "enable_thinking": True
        }

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_minimax_injects_nothing(self, mock_chat):
        """minimax (method=none) → neither reasoning_effort nor a toggle (was an
        inert reasoning_effort before)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="minimax-m2.7",
            base_url="http://vllm.cluster:8080/v1",
            reasoning_level="high",
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "reasoning_effort" not in call_kwargs.get("model_kwargs", {})
        assert "chat_template_kwargs" not in call_kwargs.get("extra_body", {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openrouter_effort_keeps_xhigh_unclamped(self, mock_chat):
        """An OpenRouter-served effort family keeps xhigh (no OpenAI clamp)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/z-ai/glm-5.2",
            reasoning_level="xhigh",
            api_key="sk-or-test",
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["reasoning"] == {"effort": "xhigh"}


class TestMinimaxM3ThinkingToggle:
    """minimax-m3 (binary_toggle) drives the native MiniMax API's
    ``thinking: {"type": enabled|disabled}`` — the control OpenRouter couldn't
    express (effort ignored, enabled:false 400s), now reachable since M3 runs
    against the MiniMax API directly."""

    def test_capability_is_binary_toggle(self):
        cap = reasoning_capability("MiniMax-M3")
        assert cap["method"] == "binary_toggle"
        assert cap["default"] == "on"
        assert cap["options"] == ["on", "off"]

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_default_sends_thinking_adaptive(self, mock_chat):
        """Unset level → family default ON → explicit thinking.type=adaptive,
        robust against an upstream endpoint-default flip (gemma precedent).
        MiniMax removed "enabled" (live 400 2026-07-23: allowed values are
        adaptive|disabled), so reasoning-on must send "adaptive"."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="MiniMax-M3")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["thinking"] == {"type": "adaptive"}
        assert "reasoning_effort" not in call_kwargs.get("model_kwargs", {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_off_sends_thinking_disabled(self, mock_chat):
        """'off' (the cockpit option) → thinking.type=disabled (M3 supports it)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="MiniMax-M3", reasoning_level="off")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["thinking"] == {"type": "disabled"}

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_toggle_coexists_with_declared_extra_body(self, mock_chat):
        """The family's declared reasoning_split must survive alongside the
        toggle — different extra_body keys, deep-merged."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="MiniMax-M3", extra_body={"reasoning_split": True})

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["thinking"] == {"type": "adaptive"}
        assert call_kwargs["extra_body"]["reasoning_split"] is True


# =============================================================================
# Declared provider params (config.extra_body → request extra_body)
# =============================================================================


class TestDeclaredExtraBody:
    """Family settings-matrix `extra_body` (e.g. MiniMax `reasoning_split`)
    must reach the request body via the factory extra_body merge."""

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openai_factory_forwards_declared_extra_body(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="MiniMax-M3",
            base_url="https://api.minimax.io/v1",
            extra_body={"reasoning_split": True},
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["reasoning_split"] is True

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_declared_merges_over_computed_without_clobbering(self, mock_chat):
        """Declared values deep-merge over factory-computed entries (gemma's
        enable_thinking toggle) while sibling computed keys survive."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="gemma-4-moe",
            base_url="http://vllm.cluster:8080/v1",
            reasoning_level="high",
            top_k=40,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["chat_template_kwargs"] == {
            "enable_thinking": False
        }
        assert call_kwargs["extra_body"]["top_k"] == 40

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_prompt_cache_key_reaches_first_party_openai(self, mock_chat):
        """The runtime per-thread cache-routing key is transmitted when the
        target is first-party OpenAI (no dispatcher-injected base_url)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(model="gpt-4o", prompt_cache_key="srw-thread-abc")

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["prompt_cache_key"] == "srw-thread-abc"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_prompt_cache_key_withheld_from_compatible_endpoints(self, mock_chat):
        """An explicit base_url means an OpenAI-compatible endpoint (vLLM et
        al.), which may reject unknown body fields — the key must not be
        sent there."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="MiniMax-M3",
            base_url="https://api.minimax.io/v1",
            prompt_cache_key="srw-thread-abc",
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert "prompt_cache_key" not in (call_kwargs.get("extra_body") or {})

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_prompt_cache_key_defers_to_declared_extra_body(self, mock_chat):
        """A value declared in config.extra_body wins over the runtime key
        (the house rule: declared beats factory-computed)."""
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="gpt-4o",
            prompt_cache_key="srw-thread-abc",
            extra_body={"prompt_cache_key": "declared-wins"},
        )

        _create_openai_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["prompt_cache_key"] == "declared-wins"

    @patch("shared.runtime.core.loader.ReasoningChatOpenAI")
    def test_openrouter_factory_forwards_declared_extra_body(self, mock_chat):
        mock_chat.return_value = MagicMock()
        config = _make_config(
            model="openrouter/minimax/minimax-m3",
            api_key="sk-or-test",
            extra_body={"reasoning_split": True},
        )

        _create_openrouter_llm(config, limits=None)

        call_kwargs = mock_chat.call_args[1]
        assert call_kwargs["extra_body"]["reasoning_split"] is True
