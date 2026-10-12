"""Kimi K3 config, prompts, and offline provider SDK requests."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize("prefix", ["", "moonshotai/", "openrouter/moonshotai/"])
@pytest.mark.parametrize(
    "model,family",
    [
        ("kimi-k3", "kimi-k3"),
        ("Kimi-K3", "kimi-k3"),
        ("kimi-k3:batch", "kimi-k3"),
        ("kimi-k3[1m]", "kimi-k3"),
        ("kimi-k3-20260716", "kimi-k3"),
        ("kimi-k2.6", "default"),
        ("kimi-k2.7-code", "default"),
        ("kimi-k30", "default"),
        ("kimi-k3.5", "default"),
    ],
)
def test_detectors_agree(prefix, model, family):
    assert family_of(prefix + model) == family
    assert detect_family(prefix + model).family == family


def test_router_alias_is_not_claimed():
    """OpenRouter's rolling alias may not be K3; keep it on default."""
    assert family_of("~moonshotai/kimi-latest") == "default"
    assert detect_family("~moonshotai/kimi-latest").family == "default"


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "kimi-test",
                "display_name": "Kimi Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["kimi-k3", "openrouter/moonshotai/kimi-k3"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults_and_prompts(tmp_path, model, role):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "max"
    assert config.llm.temperature == 1.0
    assert config.llm.top_p is None
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is False
    settings = loader.resolve_model_settings(config.llm.model)
    # function_calling forces a named tool, which K3 rejects with thinking on.
    assert settings["structured_output_method"] == "json_schema"
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.llm.max_output_tokens == 131072
    assert config.limits.model_max_context_tokens == 1048576
    assert config.limits.context_threshold_tokens == int(1048576 * 0.8)
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "high", "max"]
    assert cap["default"] == config.llm.reasoning_level

    prompt = loader.get_phase_system_prompt(
        config,
        is_strategic=False,
        prompt_type="interactive" if role == "session_base" else "systemprompt",
        model=config.llm.model,
        tool_names=[],
    )
    assert "<execution_contract>" in prompt
    assert "later calls see that line but not your reasoning" in prompt.lower()
    assert "take the narrower reading" in prompt
    assert "Kimi Test" in prompt
    assert "{%" not in prompt
    assert "{expert_identity}" not in prompt
    assert "{available_skills}" not in prompt
    resolver = loader.PromptMatrixResolver(model_family="kimi-k3")
    assert resolver.resolve_filename("persona") == "persona_kimi_k3.txt"
    assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"


def test_explicit_settings_override_family_defaults(tmp_path):
    config = _load(
        tmp_path,
        "kimi-k3",
        reasoning_level="low",
        model_max_context_tokens=262144,
        max_output_tokens=32768,
    )
    assert config.llm.reasoning_level == "low"
    assert config.llm.max_output_tokens == 32768
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("medium", "low"),
        ("high", "high"),
        ("xhigh", "high"),
        ("max", "max"),
        ("none", None),
    ],
)
@pytest.mark.asyncio
async def test_serialized_request(
    tmp_path, monkeypatch, mode, provider, requested, expected
):
    """Exercise the real SDK through offline HTTP; no provider acceptance claim."""
    captured = []
    wire_model = "moonshotai/kimi-k3" if provider == "openrouter" else "kimi-k3"

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "kimi-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": wire_model,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "Verified result.",
                            "reasoning_content": "fixture reasoning",
                        },
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
        "openrouter/moonshotai/kimi-k3" if provider == "openrouter" else "kimi-k3",
        provider=provider,
        api_key="fixture-key",
        base_url="https://kimi-fixture.invalid/v1",
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
        assert response.additional_kwargs["reasoning_content"] == "fixture reasoning"
        assert len(captured) == 1
        body = captured[0]
        assert body["model"] == wire_model
        # Native K3 rejects any temperature but 1.0 and fixes top_p at 0.95.
        assert body["temperature"] == 1.0
        assert "top_p" not in body
        assert "frequency_penalty" not in body
        assert "presence_penalty" not in body
        assert body["tools"][0]["function"]["name"] == "read_note"
        assert "tool_choice" not in body
        assert body["parallel_tool_calls"] is False
        assert body.get("max_tokens", body.get("max_completion_tokens")) == 131072
        # K3 has no `thinking` toggle; a stale 'none' omits effort entirely.
        assert "thinking" not in body
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
