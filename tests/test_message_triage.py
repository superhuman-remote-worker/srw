"""Tests for ``services.message_triage`` config resolution.

The triage LLM path itself uses httpx and isn't covered here — these tests
pin the DB-backed resolution that's the only resolution path now (the
env-var legacy fallback was removed once all call sites started passing
``db=postgres_db``).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import message_triage


class TestEnvFallbackRemoved:
    """Regression: the legacy ``_resolve_from_env`` is gone."""

    def test_no_resolve_from_env_attribute(self):
        assert not hasattr(message_triage, "_resolve_from_env")


class TestResolveFromDB:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_default_model(self):
        db = SimpleNamespace(
            get_default_llm_model=AsyncMock(return_value=None),
            get_system_api_key=AsyncMock(return_value=None),
        )
        assert await message_triage._resolve_triage_config(db) is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_api_key(self):
        db = SimpleNamespace(
            get_default_llm_model=AsyncMock(return_value="gpt-4o"),
            get_system_api_key=AsyncMock(return_value=None),
        )
        assert await message_triage._resolve_triage_config(db) is None

    @pytest.mark.asyncio
    async def test_uses_catalog_base_url_when_resolved(self):
        db = SimpleNamespace(
            get_default_llm_model=AsyncMock(
                return_value="RedHatAI/gemma-4-31B-it-FP8-Dynamic"
            ),
            get_system_api_key=AsyncMock(return_value="sk-sys"),
            list_models=AsyncMock(
                return_value=[
                    {
                        "model_id": "RedHatAI/gemma-4-31B-it-FP8-Dynamic",
                        "provider_kind": "endpoint",
                        "provider_ref": "ep-1",
                    }
                ]
            ),
            get_system_llm_endpoint=AsyncMock(
                return_value={"base_url": "http://vllm.svc/v1"}
            ),
        )
        model, base_url, api_key, tls_pin = await message_triage._resolve_triage_config(
            db
        )
        assert base_url == "http://vllm.svc/v1"
        assert api_key == "sk-sys"
        assert tls_pin is None
        assert model == "RedHatAI/gemma-4-31B-it-FP8-Dynamic"

    @pytest.mark.asyncio
    async def test_falls_back_to_openai_when_registry_misses(self):
        db = SimpleNamespace(
            get_default_llm_model=AsyncMock(return_value="gpt-4o"),
            get_system_api_key=AsyncMock(return_value="sk-sys"),
            list_models=AsyncMock(return_value=[]),
            get_system_llm_endpoint=AsyncMock(return_value=None),
        )
        model, base_url, api_key, tls_pin = await message_triage._resolve_triage_config(
            db
        )
        assert model == "gpt-4o"
        assert base_url == "https://api.openai.com/v1"
        assert api_key == "sk-sys"
        assert tls_pin is None
        db.get_system_api_key.assert_awaited_once_with("openai")

    @pytest.mark.asyncio
    async def test_provider_inference_anthropic(self):
        seen: list[str] = []

        async def capture(provider):
            seen.append(provider)
            return "key"

        db = SimpleNamespace(
            get_default_llm_model=AsyncMock(return_value="claude-opus-4-6"),
            get_system_api_key=AsyncMock(side_effect=capture),
            list_models=AsyncMock(return_value=[]),
            get_system_llm_endpoint=AsyncMock(return_value=None),
        )
        await message_triage._resolve_triage_config(db)
        assert seen == ["anthropic"]


class TestStructuredRecovery:
    def test_recover_structured_json_fences(self):
        payload = message_triage._recover_structured_json(
            '```json\n{"action":"interrupt","reason":"urgent"}\n```'
        )
        assert payload == {"action": "interrupt", "reason": "urgent"}

    def test_recover_structured_json_with_think_prefix(self):
        payload = message_triage._recover_structured_json(
            '<think>internal</think>{"action":"queue","reason":"ok"}'
        )
        assert payload == {"action": "queue", "reason": "ok"}

    def test_recover_structured_json_invalid_falls_back(self):
        assert message_triage._recover_structured_json("not json") is None


class TestInferProvider:
    def test_known_prefixes(self):
        assert message_triage._infer_provider("claude-opus-4-6") == "anthropic"
        assert message_triage._infer_provider("groq/moonshotai/kimi") == "groq"
        assert message_triage._infer_provider("gpt-4o") == "openai"
        assert message_triage._infer_provider("gemini-2.5-pro") == "google"

    def test_unknown_defaults_to_openai(self):
        assert message_triage._infer_provider("some-unknown-model") == "openai"
