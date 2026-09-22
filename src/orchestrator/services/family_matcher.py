"""Family auto-detection from model IDs.

The family taxonomy is the closed set of top-level keys in
``config/model_config_matrix.yaml`` (minus ``default``). A family appears
there iff we ship custom prompts, instructions, or settings for it.

This module pre-fills the family dropdown on the *Admin → Models* form and
the discovery confirmation dialog so the admin doesn't have to memorize the
mapping. The matcher is intentionally a small ordered regex list: families
are a closed set defined by the matrix YAML, so adding a new family
requires both a YAML edit (to surface custom prompts/settings) and a rule
here. Regex covers >95% of real model IDs deterministically; the dropdown
handles the rest with admin override.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import NamedTuple


class FamilyDetection(NamedTuple):
    """Outcome of :func:`detect_family`.

    ``source`` distinguishes a regex hit (``"matched"``) from the
    fallback to ``default`` (``"fallback"``); the API surface uses this so
    the cockpit can flag "we guessed, please confirm" vs. "we know."
    """

    family: str
    source: str  # "matched" | "fallback"


_FAMILY_RULES: list[tuple[re.Pattern, str | Callable[[re.Match], FamilyDetection]]] = [
    # OpenRouter prefix: strip and recurse on the trailing segment.
    # Handles `openrouter/anthropic/claude-opus-4-7` → claude-opus,
    # `openrouter/openai/text-embedding-3-large` → openai-embedding, etc.
    (
        re.compile(r"^openrouter/(.+)$"),
        lambda m: detect_family(m.group(1)),
    ),
    # Anthropic. claude-opus-5-5 must beat claude-opus-5, which must beat the
    # generic claude-opus rule: only Opus 5.x accepts the full effort ladder,
    # and 5.5 moved the default effort, so each has its own matrix family
    # (family_of() in shared/runtime/core/model_registry.py encodes the same
    # precedence). 5.5 matches the hyphen and OpenRouter's dotted form; the
    # digit guard keeps a dated Opus 5 snapshot on claude-opus-5. Older ids
    # stay on `claude-opus` — "claude-opus-4-5" does not contain "claude-opus-5".
    (re.compile(r"claude-opus-5[.-]5(?!\d)", re.IGNORECASE), "claude-opus-5-5"),
    (re.compile(r"claude-opus-5", re.IGNORECASE), "claude-opus-5"),
    (re.compile(r"claude-opus", re.IGNORECASE), "claude-opus"),
    (re.compile(r"claude-sonnet", re.IGNORECASE), "claude-sonnet"),
    (re.compile(r"claude-haiku", re.IGNORECASE), "claude-haiku"),
    # Fable 5 and 5.1 share one family (identical matrix knobs).
    (re.compile(r"claude-fable", re.IGNORECASE), "claude-fable"),
    # codex variants — must beat both gpt-5 and codex itself, since real
    # codex IDs (e.g. `gpt-5.3-codex`, `gpt-5.3-codex-spark`) contain both
    # the `gpt-5` prefix and the `codex` substring.
    (re.compile(r"codex-spark", re.IGNORECASE), "codex-spark"),
    (re.compile(r"codex", re.IGNORECASE), "codex"),
    # GPT-6 (Astra). Sits below the codex rules on purpose: those match any id
    # containing "codex", so a future gpt-6 codex variant keeps landing in the
    # codex family rather than here — family_of() in model_registry.py encodes
    # the same precedence.
    (re.compile(r"gpt-6", re.IGNORECASE), "gpt-6"),
    # GPT-5.6 tiers (Luna/Terra/Sol) — must beat the generic gpt-5 rule below.
    (re.compile(r"gpt-5\.6", re.IGNORECASE), "gpt-5.6"),
    # OpenAI gpt-5 family + o-series reasoning models
    (re.compile(r"gpt-5", re.IGNORECASE), "gpt-5"),
    (re.compile(r"^o[1-9](-|$)", re.IGNORECASE), "o-series"),
    # gpt-4 / gpt-4o use default prompts
    (re.compile(r"^gpt-4o", re.IGNORECASE), "default"),
    (re.compile(r"^gpt-4", re.IGNORECASE), "default"),
    # Google
    (re.compile(r"gemma", re.IGNORECASE), "gemma"),
    (re.compile(r"gemini", re.IGNORECASE), "gemini"),
    # Other open-weight families
    (re.compile(r"gpt-oss", re.IGNORECASE), "gpt-oss"),
    # minimax-m3 must beat the generic `minimax` rule below — real M3 IDs
    # (e.g. `MiniMax-M3`, `minimax/minimax-m3`) contain the `minimax` substring.
    (re.compile(r"minimax[-_]?m3", re.IGNORECASE), "minimax-m3"),
    (re.compile(r"minimax", re.IGNORECASE), "minimax"),
    (re.compile(r"deepseek", re.IGNORECASE), "deepseek"),
    # Mistral 3 family + specialists (Codestral/Magistral/Ministral/Devstral/
    # Pixtral/Voxtral). Native api.mistral.ai serves bare ids; the openrouter
    # prefix rule above recurses here for `mistralai/...`. Mirrors family_of()
    # in src/core/model_registry.py and the `mistral` model_config_matrix family.
    (
        re.compile(
            r"mistral|codestral|magistral|ministral|devstral|pixtral|voxtral",
            re.IGNORECASE,
        ),
        "mistral",
    ),
    # Vision-capable Flash before text-only 5.3, then older GLM models.
    (re.compile(r"glm-5\.3-flash", re.IGNORECASE), "glm-5.3-flash"),
    (re.compile(r"glm-5\.3", re.IGNORECASE), "glm-5.3"),
    (re.compile(r"glm", re.IGNORECASE), "glm"),
    (
        re.compile(r"(?:^|/)muse-spark-1\.3(?:$|[-:])", re.IGNORECASE),
        "muse-spark-1.3",
    ),
    (re.compile(r"kimi", re.IGNORECASE), "default"),
    # Embeddings
    (re.compile(r"text-embedding", re.IGNORECASE), "openai-embedding"),
]


def detect_family(model_id: str) -> FamilyDetection:
    """Return the family that matches ``model_id``, with provenance.

    Falls through to ``("default", "fallback")`` when no rule matches.
    """
    if not model_id:
        return FamilyDetection(family="default", source="fallback")
    for pattern, target in _FAMILY_RULES:
        match = pattern.search(model_id)
        if match is None:
            continue
        if callable(target):
            return target(match)
        return FamilyDetection(family=target, source="matched")
    return FamilyDetection(family="default", source="fallback")
