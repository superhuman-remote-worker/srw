"""Settings' resolved helper-model defaults follow the model registry."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.preference_defaults import resolve_preference_defaults

HELPER_KEYS = (
    "default_vision_model",
    "default_whisper_model",
    "default_tts_model",
    "default_embedding_model",
)


def registry(**defaults):
    return SimpleNamespace(
        resolve_default_for_capability=AsyncMock(side_effect=defaults.get)
    )


async def resolve(db, environ=None):
    return await resolve_preference_defaults(
        db, role_base=lambda role: {}, environ=environ or {}
    )


@pytest.mark.asyncio
async def test_helper_models_come_from_the_registry_dispatch_uses():
    resolved = await resolve(
        registry(
            vision="MiniMax-M3",
            whisper="whisper-large-v3",
            tts="eleven_multilingual_v2",
            embedding="qwen/qwen3-embedding-8b",
        ),
        # The registry wins over the env, as it does at dispatch.
        environ={"VISION_MODEL": "gpt-4o", "EMBEDDING_MODEL": "qwen3-embedding-8b"},
    )

    assert resolved["default_vision_model"] == "MiniMax-M3"
    assert resolved["default_whisper_model"] == "whisper-large-v3"
    assert resolved["default_tts_model"] == "eleven_multilingual_v2"
    assert resolved["default_embedding_model"] == "qwen/qwen3-embedding-8b"


@pytest.mark.asyncio
async def test_env_is_the_fallback_when_the_registry_has_no_default():
    resolved = await resolve(
        registry(),
        environ={
            "VISION_MODEL": "llava",
            "WHISPER_MODEL": "whisper-1",
            "TTS_MODEL": "kokoro",
            "EMBEDDING_MODEL": "bge-m3",
        },
    )

    assert [resolved[key] for key in HELPER_KEYS] == [
        "llava",
        "whisper-1",
        "kokoro",
        "bge-m3",
    ]


@pytest.mark.asyncio
async def test_no_model_is_invented_when_nothing_is_configured():
    resolved = await resolve(registry())

    assert {key: resolved[key] for key in HELPER_KEYS} == dict.fromkeys(HELPER_KEYS)
