"""Per-provider model discovery for the Admin → Providers key save flow.

When an admin saves or rotates a system API key, the orchestrator calls
``discover_models(provider, api_key)`` to fetch the provider's model
listing, runs each entry through the family matcher, and stages the
result on ``system_api_keys.discovery_cache_json``. The cockpit reads
that cache to render a confirmation dialog so the admin can pick which
models actually become catalog rows — one source of truth, no automatic
catalog growth.

Provider sources match the table in ``knowledge-base/knowledge/features/models_yaml_removal.md``:

| Provider   | Source                                                        |
|------------|---------------------------------------------------------------|
| openai     | GET https://api.openai.com/v1/models                          |
| google     | GET https://generativelanguage.googleapis.com/v1beta/models   |
| groq       | GET https://api.groq.com/openai/v1/models                     |
| openrouter | GET https://openrouter.ai/api/v1/models                       |
| mistral    | GET https://api.mistral.ai/v1/models                          |
| anthropic  | Skipped — no authed listing with the metadata we need         |
| vision     | Skipped — env-bridge provider, not a chat catalog source      |

Each candidate carries:

- ``model_id``                — provider-native identifier
- ``detected_family``         — output of family_matcher.detect_family()
- ``supported``               — True iff family ≠ "default"
- ``suggested_capabilities``  — best-effort capabilities[] from the ID
- ``suggested_display_label``
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from orchestrator.services import family_matcher
from orchestrator.services.llm_endpoint_probe import declares_live_voice

logger = logging.getLogger(__name__)


# Discovery cache TTL — provider catalogs change on the order of weeks.
# 24h cap prevents stale lists from blocking new model adoption while keeping
# API call volume low. Admin can force-rediscover via the explicit endpoint.
DISCOVERY_CACHE_TTL = timedelta(hours=24)

# Providers we know how to enumerate. Anthropic and the env-bridge "vision"
# provider are deliberately absent — see module docstring for the rationale.
DISCOVERABLE_PROVIDERS = {"openai", "google", "groq", "openrouter", "mistral"}


# Substrings that flag a chat-capable model as also serving vision.
# Matched lower-case. Plain substring (not regex) for ergonomics: every
# ID variant of a known multimodal family stays covered.
#
# Adding a new entry: prefer the most specific common prefix (e.g.
# ``claude-opus`` covers claude-opus-4-1, claude-opus-4-7 etc; we don't
# need a separate row per dot-version).
_MULTIMODAL_PATTERNS = (
    "gpt-4o",
    "gpt-4-vision",
    "gpt-5",  # GPT-5 family is multimodal end-to-end
    "gemini-1.5",  # gemini-1.5-pro, -flash, -flash-8b all do vision
    "gemini-2",  # gemini-2.x family is multimodal
    "gemini-3",
    "gemini-4",
    "claude-opus",
    "claude-sonnet",
    "claude-3.5",
    "claude-3.7",
    "claude-haiku",
    "claude-4",  # claude-4 family is multimodal
    "qwen2-vl",
    "qwen2.5-vl",
    # Mistral 3 multimodal generalists (Large 3 / Medium 3.5 / Small 4) + Pixtral.
    # Codestral / Ministral stay text-only, so no entry for them.
    "mistral-large",
    "mistral-medium",
    "mistral-small",
    "pixtral",
)


def _is_multimodal_chat(model_id: str) -> bool:
    """True when ``model_id`` is a known multimodal chat family."""
    lower = model_id.lower()
    return any(p in lower for p in _MULTIMODAL_PATTERNS)


def _capability_from_model_id(model_id: str) -> str:
    """Best-effort PRIMARY capability guess from a provider model ID.

    Heuristics, evaluated top-down (a listing entry that declares
    ``live_voice`` skips them, see :func:`_candidate_from_id`):
    - ``gpt-live``/``realtime`` substring → ``live_voice``
    - ``embedding`` substring → ``embedding``
    - ``whisper`` substring → ``whisper``
    - ``tts``/``speech`` substring → ``tts``
    - matches a known multimodal-chat family → ``chat`` (the array form
      picks up ``vision`` separately via :func:`_is_multimodal_chat`)
    - bare ``vision``/``image`` substring → ``vision`` (vision-only model)
    - everything else → ``chat``

    Returns the singular primary capability — internal helper consumed
    by :func:`_capabilities_from_model_id` to decide whether to auto-
    expand chat into ``[chat, auxiliary]`` and add ``vision`` for
    multimodal chat families.
    """
    lower = model_id.lower()
    if "gpt-live" in lower or "realtime" in lower:
        # A live voice model is never a chat row (it is bound by a phone-line
        # connector, not as an LLM).
        return "live_voice"
    if "embedding" in lower:
        return "embedding"
    if "whisper" in lower:
        return "whisper"
    if "tts" in lower or "speech" in lower:
        return "tts"
    # Multimodal chat IDs are PRIMARY chat — vision is a secondary
    # capability surfaced via the array. Check this BEFORE the bare
    # 'vision' branch so gpt-4-vision-preview routes to chat (not vision).
    if _is_multimodal_chat(model_id):
        return "chat"
    # Vision-only / image-input models that aren't part of a known
    # multimodal-chat family (e.g. florence-vision, dall-e-3-image).
    if "vision" in lower or "image" in lower:
        return "vision"
    return "chat"


def _capabilities_from_model_id(model_id: str) -> list[str]:
    """Best-effort ``capabilities[]`` array for a provider model ID.

    Chat-capable IDs auto-expand to ``[chat, auxiliary]`` (chat-capable
    LLMs always serve auxiliary). Known multimodal families (gpt-4o,
    gemini-2, claude-opus, etc.) additionally pick up ``vision``.

    Embedding / whisper / tts / vision-only IDs land as singletons —
    each lives on its own runtime path and shouldn't be conflated with
    chat by the discovery default.
    """
    primary = _capability_from_model_id(model_id)
    if primary != "chat":
        return [primary]
    caps = ["chat", "auxiliary"]
    if _is_multimodal_chat(model_id):
        caps.append("vision")
    return caps


def _candidate_from_id(
    model_id: str, *, display_label: str | None = None, live_voice: bool = False
) -> dict[str, Any]:
    """Wrap a provider-native model ID in the shape the cockpit expects.

    ``supported = (detected_family != "default")``: when the family matcher
    has a non-default rule for this ID, we ship custom prompts/settings for
    it and the dialog defaults the row to checked. Models that fall through
    to ``default`` work but get plain prompts — surfaced as "Generic" with
    the row default-unchecked so the admin opts in deliberately.
    """
    detection = family_matcher.detect_family(model_id)
    return {
        "model_id": model_id,
        "detected_family": detection.family,
        "supported": detection.family != "default",
        "suggested_capabilities": (
            ["live_voice"] if live_voice else _capabilities_from_model_id(model_id)
        ),
        "suggested_display_label": display_label or model_id,
    }


async def _fetch_openai(api_key: str) -> list[dict[str, Any]]:
    """OpenAI ``/v1/models`` returns ``{"data": [{"id": "..."}, ...]}``."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
    return [
        _candidate_from_id(item["id"], live_voice=declares_live_voice(item))
        for item in data
        if isinstance(item, dict) and item.get("id")
    ]


async def _fetch_groq(api_key: str) -> list[dict[str, Any]]:
    """Groq exposes the OpenAI-shaped ``/openai/v1/models`` listing."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://api.groq.com/openai/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
    return [
        _candidate_from_id(item["id"], live_voice=declares_live_voice(item))
        for item in data
        if isinstance(item, dict) and item.get("id")
    ]


async def _fetch_openrouter(api_key: str) -> list[dict[str, Any]]:
    """OpenRouter exposes ``/api/v1/models``; IDs come pre-namespaced
    (`anthropic/claude-opus-4-7`). The family matcher's OpenRouter prefix
    rule recurses on the trailing segment, so we wrap every ID with
    ``openrouter/`` to keep the family detection deterministic."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://openrouter.ai/api/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
    out: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        raw_id = item.get("id")
        if not raw_id:
            continue
        # OpenRouter IDs already include vendor/, so prepend openrouter/ to
        # trigger the recursive prefix rule in the matcher.
        wrapped = f"openrouter/{raw_id}"
        label = item.get("name") or raw_id
        out.append(_candidate_from_id(wrapped, display_label=label))
    return out


async def _fetch_google(api_key: str) -> list[dict[str, Any]]:
    """Google's Generative Language API uses an API-key query param. The
    response shape is ``{"models": [{"name": "models/gemini-...", ...}]}``;
    we strip the ``models/`` prefix to get the raw ID."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://generativelanguage.googleapis.com/v1beta/models",
            params={"key": api_key},
        )
        resp.raise_for_status()
        data = resp.json().get("models") or []
    out: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name") or ""
        # Google returns names like "models/gemini-2.5-pro" — strip the prefix.
        model_id = raw_name.split("/", 1)[-1] if raw_name else ""
        if not model_id:
            continue
        label = item.get("displayName") or model_id
        out.append(_candidate_from_id(model_id, display_label=label))
    return out


async def _fetch_mistral(api_key: str) -> list[dict[str, Any]]:
    """Mistral exposes the OpenAI-shaped ``/v1/models`` listing. IDs are bare
    (`mistral-large-latest`, `codestral-latest`), which the family matcher's
    mistral rule maps to the ``mistral`` family."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://api.mistral.ai/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
    return [
        _candidate_from_id(item["id"], live_voice=declares_live_voice(item))
        for item in data
        if isinstance(item, dict) and item.get("id")
    ]


_FETCHERS = {
    "openai": _fetch_openai,
    "groq": _fetch_groq,
    "openrouter": _fetch_openrouter,
    "google": _fetch_google,
    "mistral": _fetch_mistral,
}


async def discover_models(provider: str, api_key: str) -> list[dict[str, Any]]:
    """Fetch the provider's model listing, return the cockpit-ready candidates.

    Returns an empty list for providers we can't enumerate (anthropic,
    vision) so the caller can stage an empty cache without special-casing.
    Errors are caught and logged — discovery failures must not break the
    key-save path. Admin can retry via ``rediscover``.
    """
    fetcher = _FETCHERS.get(provider)
    if fetcher is None:
        return []
    try:
        return await fetcher(api_key)
    except Exception as exc:
        logger.warning(
            "discovery: %s listing failed: %s", provider, exc, exc_info=False
        )
        return []


def is_discovery_cache_fresh(cache_at: datetime | None) -> bool:
    """Return True when the cached discovery payload is still within TTL."""
    if cache_at is None:
        return False
    if cache_at.tzinfo is None:
        cache_at = cache_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - cache_at < DISCOVERY_CACHE_TTL


def build_cache_payload(
    provider: str, candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    """Wrap candidates with provenance for storage on the key row.

    The cockpit reads this back via ``GET .../keys/{provider}/discovery``;
    the timestamp lets the UI show "Last discovered N hours ago" without
    a separate query.
    """
    return {
        "provider": provider,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "count": len(candidates),
        "candidates": candidates,
    }
