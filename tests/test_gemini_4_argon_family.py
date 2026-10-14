"""Gemini 4 Argon and Gemini 3.8 Flash config, prompts, and offline requests."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage, SystemMessage

from orchestrator.services.discovery import _capabilities_from_model_id
from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat

_NEW_FAMILIES = {"gemini-4-argon", "gemini-3.8-flash"}


@pytest.mark.parametrize("prefix", ["", "google/", "models/", "openrouter/google/"])
@pytest.mark.parametrize(
    "model,family",
    [
        ("gemini-4-argon", "gemini-4-argon"),
        ("Gemini-4-Argon", "gemini-4-argon"),
        ("gemini-4-argon-cyber-permissive", "gemini-4-argon"),
        ("gemini-4-argon:batch", "gemini-4-argon"),
        ("gemini-3.8-flash", "gemini-3.8-flash"),
        ("gemini-3.8-flash-preview-09-2026", "gemini-3.8-flash"),
        ("gemini-3.8-flash:batch", "gemini-3.8-flash"),
    ],
)
def test_detectors_claim_the_new_models(prefix, model, family):
    assert family_of(prefix + model) == family
    assert detect_family(prefix + model).family == family


@pytest.mark.parametrize("prefix", ["", "openrouter/google/"])
@pytest.mark.parametrize(
    "model",
    [
        "gemini-3.7-flash",
        "gemini-3.5-flash",
        "gemini-3.1-pro-preview",
        "gemini-2.5-pro",
        "gemini-3.8-flash-lite-tts",
        "gemini-3.8-flash-tts",
        "gemini-3.8-flash-image",
        "gemini-3.8-live",
        "gemini-3.80-flash",
        "gemini-4-argonx",
    ],
)
def test_older_and_sibling_ids_stay_generic(prefix, model):
    assert family_of(prefix + model) == "gemini"
    assert detect_family(prefix + model).family == "gemini"


def test_rolling_alias_is_not_claimed():
    """A "latest" alias may point at another model; keep it off the new families."""
    for model in ("gemini-flash-latest", "~google/gemini-flash-latest"):
        assert family_of(model) not in _NEW_FAMILIES
        assert detect_family(model).family not in _NEW_FAMILIES


def test_discovery_flags_gemini_4_as_vision():
    assert "vision" in _capabilities_from_model_id("gemini-4-argon")
    assert "vision" in _capabilities_from_model_id("gemini-3.8-flash")


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "gemini-test",
                "display_name": "Gemini Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize(
    "model",
    ["gemini-4-argon", "gemini-3.8-flash", "openrouter/google/gemini-3.8-flash"],
)
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults_and_prompts(tmp_path, model, role):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "medium"
    assert config.llm.temperature == 1.0
    # Both models ignore sampling parameters; the generic gemini block's
    # top_p/top_k must not leak in.
    assert config.llm.top_p is None
    assert config.llm.top_k is None
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is True
    settings = loader.resolve_model_settings(config.llm.model)
    assert settings["structured_output_method"] == "json_schema"
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.llm.max_output_tokens == 65536
    assert config.limits.model_max_context_tokens == 1048576
    assert config.limits.context_threshold_tokens == int(1048576 * 0.8)
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "medium", "high"]
    assert cap["default"] == config.llm.reasoning_level

    prompt = loader.get_phase_system_prompt(
        config,
        is_strategic=False,
        prompt_type="interactive" if role == "session_base" else "systemprompt",
        model=config.llm.model,
        tool_names=[],
    )
    assert "# Core Mandates" in prompt
    assert "Never return an empty response." in prompt
    assert "never edit the same file twice in one turn" in prompt
    assert "Gemini Test" in prompt
    assert "{%" not in prompt
    assert "{expert_identity}" not in prompt
    assert "{available_skills}" not in prompt
    resolver = loader.PromptMatrixResolver(model_family=family_of(config.llm.model))
    assert resolver.resolve_filename("persona") == "persona_gemini_4_argon.txt"
    assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"


def test_explicit_settings_override_family_defaults(tmp_path):
    config = _load(
        tmp_path,
        "gemini-4-argon",
        reasoning_level="high",
        model_max_context_tokens=262144,
        max_output_tokens=32768,
    )
    assert config.llm.reasoning_level == "high"
    assert config.llm.max_output_tokens == 32768
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)


def _native_generation_config(tmp_path, model, **overrides):
    config = _load(
        tmp_path,
        model,
        provider="google",
        api_key="fixture-key",
        max_retries=0,
        **overrides,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    request = llm._prepare_request([SystemMessage("rules"), HumanMessage("hi")])
    return request["config"].model_dump(exclude_none=True)


@pytest.mark.parametrize("model", ["gemini-4-argon", "gemini-3.8-flash"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        (None, "MEDIUM"),  # the family default, also the provider's
        ("minimal", "LOW"),  # Argon returns 400 for MINIMAL
        ("low", "LOW"),
        ("medium", "MEDIUM"),
        ("high", "HIGH"),
        ("xhigh", "HIGH"),
        ("max", "HIGH"),
        ("off", "LOW"),  # thinking cannot be disabled
        ("none", None),  # no level sent; the provider default applies
    ],
)
def test_native_request_sends_thinking_level(tmp_path, model, requested, expected):
    overrides = {} if requested is None else {"reasoning_level": requested}
    gen = _native_generation_config(tmp_path, model, **overrides)
    thinking = gen["thinking_config"]
    assert thinking["include_thoughts"] is True
    level = thinking.get("thinking_level")
    assert (level.value if level is not None else None) == expected
    # thinking_budget returns 400 on Argon; sampling params are ignored.
    assert "thinking_budget" not in thinking
    assert "top_p" not in gen
    assert "top_k" not in gen
    assert gen["max_output_tokens"] == 65536


@pytest.mark.parametrize(
    "model", ["gemini-3.5-flash", "gemini-2.5-flash", "models/gemini-2.5-flash"]
)
def test_native_request_leaves_older_gemini_without_a_level(tmp_path, model):
    """The generic block declares no ladder; a `default` fall-through must not
    lend one either (2.5 takes a token budget, not a level)."""
    gen = _native_generation_config(tmp_path, model, reasoning_level="high")
    assert "thinking_level" not in gen["thinking_config"]


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("medium", "medium"),
        ("high", "high"),
        ("max", "high"),
        ("none", None),
    ],
)
@pytest.mark.asyncio
async def test_serialized_request(
    tmp_path, monkeypatch, mode, provider, requested, expected
):
    """Exercise the real SDK through offline HTTP; no provider acceptance claim."""
    captured = []
    wire_model = (
        "google/gemini-3.8-flash" if provider == "openrouter" else "gemini-3.8-flash"
    )

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "gemini-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": wire_model,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "Verified result."},
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                },
            },
        )

    original_client = reasoning_chat.ReasoningCapturingClient
    original_async_client = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: original_client(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original_async_client(
            **kw, transport=httpx.MockTransport(respond)
        ),
    )
    monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **kw: 10)
    config = _load(
        tmp_path,
        "openrouter/google/gemini-3.8-flash"
        if provider == "openrouter"
        else "gemini-3.8-flash",
        provider=provider,
        api_key="fixture-key",
        base_url="https://gemini-fixture.invalid/v1",
        reasoning_level=requested,
        max_retries=0,
        streaming=False,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        bound = llm.bind_tools(
            [
                {
                    "type": "function",
                    "function": {
                        "name": "read_note",
                        "description": "Read a workspace note.",
                        "parameters": {"type": "object", "properties": {}},
                    },
                },
            ],
            parallel_tool_calls=config.llm.parallel_tool_calls,
        )
        messages = [
            HumanMessage(
                content=[
                    {"type": "text", "text": "Check the supplied input."},
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="},
                    },
                ]
            )
        ]
        response = (
            await bound.ainvoke(messages) if mode == "async" else bound.invoke(messages)
        )
        assert response.content == "Verified result."
        assert len(captured) == 1
        body = captured[0]
        assert body["model"] == wire_model
        assert body["temperature"] == 1.0
        assert "top_p" not in body
        assert "top_k" not in body
        # 3.8 Flash returns an error for either penalty.
        assert "frequency_penalty" not in body
        assert "presence_penalty" not in body
        assert body["tools"][0]["function"]["name"] == "read_note"
        assert body["parallel_tool_calls"] is True
        assert body.get("max_tokens", body.get("max_completion_tokens")) == 65536
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == expected
            assert "reasoning" not in body
        assert body["messages"][0]["content"][1]["type"] == "image_url"
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
