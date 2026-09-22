"""Tests for ``orchestrator.services.family_matcher.detect_family``.

Pins the regex registry against representative model IDs so a future edit
to ``_FAMILY_RULES`` that breaks an established mapping fails loudly. The
suite covers each rule, the OpenRouter prefix recursion, the
``default``-fallback path, and a few edge cases that specifically caused
trouble during the chunk-3 design (`o-series` reasoning models that look
like `o3` not `o3-mini`, `gpt-5` ahead of `gpt-4` ordering, etc.).
"""

from __future__ import annotations

import pytest

from orchestrator.services.family_matcher import detect_family


@pytest.mark.parametrize(
    "model_id,expected_family",
    [
        # Anthropic
        ("claude-opus-4-7", "claude-opus"),
        ("claude-opus-4-7-20251024", "claude-opus"),
        # Opus 5 splits off (full xhigh/max effort ladder); 4.x stays generic.
        ("claude-opus-5", "claude-opus-5"),
        ("openrouter/anthropic/claude-opus-5", "claude-opus-5"),
        ("claude-opus-4-5", "claude-opus"),
        # Opus 5.5 splits off Opus 5 (default effort moved to medium). Anthropic
        # and CLIProxyAPI spell it with a hyphen, OpenRouter with a dot; a dated
        # Opus 5 snapshot must not be mistaken for it.
        ("claude-opus-5-5", "claude-opus-5-5"),
        ("claude-opus-5-5-20260922", "claude-opus-5-5"),
        ("openrouter/anthropic/claude-opus-5.5", "claude-opus-5-5"),
        ("openrouter/anthropic/claude-opus-5.5:batch", "claude-opus-5-5"),
        ("claude-opus-5-20260401", "claude-opus-5"),
        ("claude-sonnet-4-6", "claude-sonnet"),
        ("claude-haiku-4-5", "claude-haiku"),
        # Fable 5 and 5.1 share one family.
        ("claude-fable-5", "claude-fable"),
        ("claude-fable-5-1", "claude-fable"),
        ("openrouter/anthropic/claude-fable-5-1", "claude-fable"),
        # OpenAI gpt-5 + o-series (split families post chunk 1)
        ("gpt-5", "gpt-5"),
        ("gpt-5.2", "gpt-5"),
        ("gpt-5-mini", "gpt-5"),
        # GPT-5.6 tiers (Luna/Terra/Sol) — dedicated family, must beat gpt-5;
        # codex variants keep the codex family (rule order).
        ("gpt-5.6-sol", "gpt-5.6"),
        ("gpt-5.6-terra", "gpt-5.6"),
        ("openrouter/openai/gpt-5.6-luna", "gpt-5.6"),
        ("gpt-5.6-codex", "codex"),
        # GPT-6 (Astra) — its own family; codex rules still win on a codex id.
        ("gpt-6-astra", "gpt-6"),
        ("openrouter/openai/gpt-6-astra", "gpt-6"),
        ("gpt-6-astra-codex", "codex"),
        ("o3", "o-series"),
        ("o3-mini", "o-series"),
        ("o4", "o-series"),
        ("o4-mini", "o-series"),
        # Codex variants — codex-spark must beat plain codex
        ("gpt-5.3-codex", "codex"),
        ("gpt-5.3-codex-spark", "codex-spark"),
        # gpt-4* → default (works with default prompts)
        ("gpt-4o", "default"),
        ("gpt-4o-mini", "default"),
        ("gpt-4-turbo", "default"),
        # Google
        ("gemini-2.5-pro", "gemini"),
        ("gemini-2.0-flash-exp", "gemini"),
        ("gemma-4-31b-it", "gemma"),
        # Open weights
        ("openai/gpt-oss-120b", "gpt-oss"),
        ("MiniMaxAI/MiniMax-M2.7", "minimax"),
        ("MiniMaxAI/MiniMax-M3", "minimax-m3"),
        ("deepseek-v3", "deepseek"),
        ("z-ai/glm-5.2", "glm"),
        ("glm-5.2", "glm"),
        # Mistral 3 family + specialists (settings-only family)
        ("mistral-large-latest", "mistral"),
        ("codestral-latest", "mistral"),
        ("magistral-medium-latest", "mistral"),
        # Kimi (Moonshot) → default per design (no custom prompts shipped)
        ("moonshotai/kimi-k2-instruct-0905", "default"),
        # Embeddings
        ("text-embedding-3-large", "openai-embedding"),
        ("text-embedding-ada-002", "openai-embedding"),
    ],
)
def test_detect_family_matched_rules(model_id: str, expected_family: str) -> None:
    """Each rule maps its representative model_id to the expected family."""
    detection = detect_family(model_id)
    assert detection.family == expected_family
    assert detection.source == "matched"


@pytest.mark.parametrize(
    "model_id,expected_family",
    [
        ("openrouter/anthropic/claude-opus-4-7", "claude-opus"),
        ("openrouter/anthropic/claude-sonnet-4-6", "claude-sonnet"),
        ("openrouter/openai/gpt-5", "gpt-5"),
        ("openrouter/openai/text-embedding-3-large", "openai-embedding"),
        ("openrouter/google/gemini-2.5-pro", "gemini"),
        ("openrouter/openai/gpt-oss-120b", "gpt-oss"),
        ("openrouter/z-ai/glm-5.2", "glm"),
        ("openrouter/mistralai/mistral-large-latest", "mistral"),
    ],
)
def test_openrouter_prefix_recurses(model_id: str, expected_family: str) -> None:
    """OpenRouter passthrough IDs route through the recursive prefix rule —
    no rule duplication required."""
    detection = detect_family(model_id)
    assert detection.family == expected_family
    assert detection.source == "matched"


def test_unknown_model_falls_back_to_default() -> None:
    """A model_id no rule matches resolves to default with provenance
    so the cockpit can flag "we guessed" vs "we know."""
    detection = detect_family("some-bespoke-model-99-x")
    assert detection.family == "default"
    assert detection.source == "fallback"


def test_empty_model_id_returns_default_fallback() -> None:
    """Empty/whitespace-only IDs are treated as the unknown case rather
    than raising, so the API surface can validate at the route boundary."""
    assert detect_family("").family == "default"
    assert detect_family("").source == "fallback"


def test_openrouter_unknown_inner_falls_back() -> None:
    """OpenRouter wrapper around an unknown model still recurses; the inner
    miss surfaces as ``fallback`` (not ``matched``)."""
    detection = detect_family("openrouter/something/never-heard-of")
    assert detection.family == "default"
    assert detection.source == "fallback"


def test_case_insensitive_match() -> None:
    """Provider catalogs sometimes return mixed-case IDs (Claude-Opus,
    GEMINI-2.5-pro). The matcher should not care."""
    assert detect_family("CLAUDE-OPUS-4-7").family == "claude-opus"
    assert detect_family("Claude-Opus-5").family == "claude-opus-5"
    assert detect_family("Claude-Opus-5-5").family == "claude-opus-5-5"
    assert detect_family("Gemini-2.5-pro").family == "gemini"
    assert detect_family("GPT-OSS-120B").family == "gpt-oss"


def test_codex_spark_ordering_pins_against_regression() -> None:
    """codex-spark is a substring of "codex" — if the rules table is ever
    reordered such that the plain `codex` rule fires first, codex-spark
    models silently route to the wrong matrix entry."""
    assert detect_family("codex-spark").family == "codex-spark"
    assert detect_family("gpt-5.3-codex-spark").family == "codex-spark"


def test_minimax_m3_ordering_pins_against_regression() -> None:
    """minimax-m3 IDs contain the "minimax" substring — if the rules table is
    ever reordered so the plain `minimax` rule fires first, M3 models silently
    route to the M2.7 matrix entry (wrong context window, no multimodal)."""
    assert detect_family("minimax-m3").family == "minimax-m3"
    assert detect_family("MiniMax-M3").family == "minimax-m3"
    assert detect_family("minimax/minimax-m3").family == "minimax-m3"
    assert detect_family("openrouter/minimax/minimax-m3").family == "minimax-m3"
    # M2.x must stay on the generic minimax family, not get captured by M3.
    assert detect_family("MiniMaxAI/MiniMax-M2.7").family == "minimax"
