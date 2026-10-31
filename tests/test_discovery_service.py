"""Tests for ``orchestrator.services.discovery``.

Covers:
- The capability-from-id heuristic.
- ``_candidate_from_id`` shape + ``supported`` flag derivation.
- ``discover_models`` returns [] for non-discoverable providers (anthropic,
  vision) without raising and without making a network call.
- Per-provider response-shape parsing for openai, groq, openrouter, google
  using mocked httpx clients.
- Network errors are caught and surfaced as an empty list (failures must
  never block the key-save path).
- ``is_discovery_cache_fresh`` honours the 24h TTL.
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import discovery  # noqa: E402


# ---------------------------------------------------------------------------
# capability heuristic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model_id,expected",
    [
        ("text-embedding-3-large", "embedding"),
        ("whisper-large-v3", "whisper"),
        ("tts-1-hd", "tts"),
        # Multimodal chat IDs are PRIMARY chat (vision lives in the array).
        # The legacy heuristic returned 'vision' here; the array model treats
        # gpt-4-vision-preview as a chat model that ALSO does vision.
        ("gpt-4-vision-preview", "chat"),
        # Vision-only / image-input models that aren't a multimodal-chat
        # family stay primary 'vision'.
        ("dall-e-3-image", "vision"),
        ("gpt-4o", "chat"),
        ("claude-opus-4-7", "chat"),
        ("gemini-2.5-pro", "chat"),
    ],
)
def test_capability_from_model_id_heuristic(model_id: str, expected: str) -> None:
    assert discovery._capability_from_model_id(model_id) == expected


@pytest.mark.parametrize(
    "model_id,expected",
    [
        # Chat-capable IDs auto-expand to [chat, auxiliary]
        ("claude-opus-4-7", ["chat", "auxiliary", "vision"]),  # multimodal
        ("gpt-4o", ["chat", "auxiliary", "vision"]),  # multimodal
        ("gpt-4o-mini", ["chat", "auxiliary", "vision"]),  # multimodal substring
        ("gemini-2.5-pro", ["chat", "auxiliary", "vision"]),  # multimodal
        ("gemini-1.5-flash-8b", ["chat", "auxiliary", "vision"]),  # multimodal
        ("claude-haiku-3-5", ["chat", "auxiliary", "vision"]),  # multimodal
        # Plain chat IDs stay [chat, auxiliary] (no vision)
        ("groq/llama-3.1-70b", ["chat", "auxiliary"]),
        ("openai/gpt-3.5-turbo", ["chat", "auxiliary"]),
        # Non-chat capabilities land as singletons (no auxiliary auto-add).
        ("text-embedding-3-large", ["embedding"]),
        ("whisper-large-v3", ["whisper"]),
        ("tts-1-hd", ["tts"]),
        ("dall-e-3-image", ["vision"]),
    ],
)
def test_capabilities_from_model_id_array(model_id: str, expected: list[str]) -> None:
    """The array helper auto-expands chat → [chat,auxiliary] and adds 'vision'
    for known multimodal chat families. Non-chat primaries stay singleton."""
    assert discovery._capabilities_from_model_id(model_id) == expected


def test_candidate_supported_when_family_matches() -> None:
    """A model_id with a non-default family rule is `supported=True`."""
    cand = discovery._candidate_from_id("claude-opus-4-7")
    assert cand["model_id"] == "claude-opus-4-7"
    assert cand["detected_family"] == "claude-opus"
    assert cand["supported"] is True
    # Array field (source of truth) carries the full set — claude-opus is
    # multimodal so vision is included.
    assert cand["suggested_capabilities"] == ["chat", "auxiliary", "vision"]
    assert cand["suggested_display_label"] == "claude-opus-4-7"
    # Legacy singular field was dropped in chunk 7.
    assert "suggested_capability" not in cand


def test_candidate_unsupported_when_family_is_default() -> None:
    """Models whose family resolves to `default` are flagged unsupported
    so the cockpit defaults the row to unchecked in the dialog."""
    cand = discovery._candidate_from_id("some-bespoke-model-99-x")
    assert cand["detected_family"] == "default"
    assert cand["supported"] is False


def test_candidate_uses_explicit_label_when_provided() -> None:
    cand = discovery._candidate_from_id(
        "text-embedding-3-large", display_label="OpenAI Embedding L"
    )
    assert cand["suggested_display_label"] == "OpenAI Embedding L"
    assert cand["suggested_capabilities"] == ["embedding"]


# ---------------------------------------------------------------------------
# discover_models
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["anthropic", "vision", "unknown"])
async def test_non_discoverable_providers_return_empty(provider: str) -> None:
    """Anthropic + the env-bridge `vision` provider have no listing endpoint;
    discover_models must return [] without trying any network call."""
    with patch("httpx.AsyncClient") as mock_client:
        result = await discovery.discover_models(provider, "sk-test")
        assert result == []
        mock_client.assert_not_called()


def _mock_async_client(json_payload: dict) -> MagicMock:
    """Build a mock httpx.AsyncClient that returns the supplied JSON payload.

    httpx.AsyncClient is used as an async context manager; we mock both
    the ``__aenter__``/``__aexit__`` protocol and the ``.get`` call.
    """
    response = MagicMock()
    response.json.return_value = json_payload
    response.raise_for_status.return_value = None

    client_instance = MagicMock()
    client_instance.get = AsyncMock(return_value=response)
    client_instance.__aenter__ = AsyncMock(return_value=client_instance)
    client_instance.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=client_instance)
    return factory


@pytest.mark.asyncio
async def test_openai_discovery_parses_data_array() -> None:
    payload = {
        "data": [
            {"id": "gpt-4o"},
            {"id": "text-embedding-3-large"},
            {"id": "claude-opus-4-7"},  # provider-foreign — still listed verbatim
        ],
    }
    factory = _mock_async_client(payload)
    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("openai", "sk-test")

    ids = {c["model_id"] for c in candidates}
    assert ids == {"gpt-4o", "text-embedding-3-large", "claude-opus-4-7"}
    embedding = next(c for c in candidates if c["model_id"] == "text-embedding-3-large")
    assert embedding["suggested_capabilities"] == ["embedding"]
    assert embedding["detected_family"] == "openai-embedding"


@pytest.mark.asyncio
async def test_groq_discovery_uses_openai_shape() -> None:
    factory = _mock_async_client({"data": [{"id": "openai/gpt-oss-120b"}]})
    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("groq", "gsk-test")
    assert candidates[0]["model_id"] == "openai/gpt-oss-120b"
    assert candidates[0]["detected_family"] == "gpt-oss"
    assert candidates[0]["supported"] is True


@pytest.mark.asyncio
async def test_openrouter_wraps_ids_with_prefix() -> None:
    """OpenRouter IDs come as ``vendor/model`` — wrap them with
    ``openrouter/`` so the family matcher's recursive prefix rule fires."""
    factory = _mock_async_client(
        {"data": [{"id": "anthropic/claude-opus-4-7", "name": "Claude Opus 4.7"}]}
    )
    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("openrouter", "or-test")
    assert candidates[0]["model_id"] == "openrouter/anthropic/claude-opus-4-7"
    assert candidates[0]["detected_family"] == "claude-opus"
    assert candidates[0]["suggested_display_label"] == "Claude Opus 4.7"


@pytest.mark.asyncio
async def test_google_strips_models_prefix() -> None:
    """Google returns names like ``models/gemini-2.5-pro``; we strip the
    ``models/`` prefix to end up with the bare ID."""
    factory = _mock_async_client(
        {
            "models": [
                {"name": "models/gemini-2.5-pro", "displayName": "Gemini 2.5 Pro"},
                {"name": "models/text-embedding-004"},
            ]
        }
    )
    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("google", "g-test")
    ids = {c["model_id"] for c in candidates}
    assert ids == {"gemini-2.5-pro", "text-embedding-004"}
    pro = next(c for c in candidates if c["model_id"] == "gemini-2.5-pro")
    assert pro["suggested_display_label"] == "Gemini 2.5 Pro"


@pytest.mark.asyncio
async def test_network_failure_yields_empty_list() -> None:
    """A discovery probe that raises must surface as ``[]`` so the
    key-save path stays green."""
    failing = MagicMock()
    failing.get = AsyncMock(side_effect=Exception("connection reset"))
    failing.__aenter__ = AsyncMock(return_value=failing)
    failing.__aexit__ = AsyncMock(return_value=None)
    factory = MagicMock(return_value=failing)

    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("openai", "sk-test")
    assert candidates == []


# ---------------------------------------------------------------------------
# cache freshness
# ---------------------------------------------------------------------------


def test_cache_fresh_within_ttl() -> None:
    """Cache populated under 24h ago is fresh."""
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    assert discovery.is_discovery_cache_fresh(recent) is True


def test_cache_stale_past_ttl() -> None:
    """Cache populated >24h ago is stale; UI prompts for rediscover."""
    ancient = datetime.now(timezone.utc) - timedelta(hours=25)
    assert discovery.is_discovery_cache_fresh(ancient) is False


def test_cache_naive_datetime_treated_as_utc() -> None:
    """Some asyncpg paths return naive datetimes; the helper assumes UTC."""
    naive_recent = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        minutes=30
    )
    assert discovery.is_discovery_cache_fresh(naive_recent) is True


def test_cache_none_is_stale() -> None:
    assert discovery.is_discovery_cache_fresh(None) is False


def test_build_cache_payload_shape() -> None:
    """Payload carries provenance the cockpit needs without a second query."""
    candidates = [discovery._candidate_from_id("gpt-4o")]
    payload = discovery.build_cache_payload("openai", candidates)
    assert payload["provider"] == "openai"
    assert payload["count"] == 1
    assert payload["candidates"] == candidates
    # ISO-8601 with timezone — must round-trip through fromisoformat.
    parsed = datetime.fromisoformat(payload["fetched_at"])
    assert parsed.tzinfo is not None


@pytest.mark.asyncio
async def test_a_listing_that_declares_live_voice_needs_no_name_hint() -> None:
    payload = {
        "data": [
            {"id": "cascade-cpu", "capabilities": ["live_voice"]},
            {"id": "gpt-realtime"},
            {"id": "gpt-4o", "capabilities": ["vision"]},
        ]
    }
    factory = _mock_async_client(payload)
    with patch("orchestrator.services.discovery.httpx.AsyncClient", factory):
        candidates = await discovery.discover_models("openai", "sk-test")
    suggested = {c["model_id"]: c["suggested_capabilities"] for c in candidates}
    assert suggested["cascade-cpu"] == ["live_voice"]
    assert suggested["gpt-realtime"] == ["live_voice"]
    assert suggested["gpt-4o"] == ["chat", "auxiliary", "vision"]
