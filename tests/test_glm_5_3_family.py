"""GLM-5.3 config resolution and real SDK serialization (offline HTTP)."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize("prefix", ["", "z-ai/", "openrouter/z-ai/", "zai-org/"])
@pytest.mark.parametrize(
    "model,family",
    [
        ("glm-5.3", "glm-5.3"),
        ("GLM-5.3-Flash", "glm-5.3-flash"),
        ("glm-5.3-flash:exacto", "glm-5.3-flash"),
        ("glm-5.3-20260816", "glm-5.3"),
        ("glm-5.2", "glm"),
        ("glm-4.7-flash", "glm"),
    ],
)
def test_detectors_agree(prefix, model, family):
    assert family_of(prefix + model) == family
    assert detect_family(prefix + model).family == family


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "glm-test",
                "display_name": "GLM Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["glm-5.3", "glm-5.3-flash"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults_and_prompts(tmp_path, model, role):
    config = _load(tmp_path, "openrouter/z-ai/" + model, role=role)
    assert config.llm.reasoning_level == "max"
    assert config.llm.temperature == 1.0
    assert config.llm.top_p == 0.95
    assert config.llm.multimodal is model.endswith("flash")
    assert config.llm.parallel_tool_calls is False
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.llm.max_output_tokens == 131072
    assert config.limits.model_max_context_tokens == 1000000
    assert config.limits.context_threshold_tokens == 800000
    cap = loader.reasoning_capability(model)
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
    assert "GLM Test" in prompt
    assert "{%" not in prompt
    assert "{expert_identity}" not in prompt
    resolver = loader.PromptMatrixResolver(model_family=model)
    assert resolver.resolve_filename("persona") == "persona_glm_5_3.txt"
    assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"


def test_explicit_settings_override_family_defaults(tmp_path):
    config = _load(
        tmp_path,
        "glm-5.3-flash",
        reasoning_level="low",
        temperature=0.7,
        top_p=0.9,
        model_max_context_tokens=262144,
        max_output_tokens=32768,
    )
    assert config.llm.reasoning_level == "low"
    assert config.llm.temperature == 0.7
    assert config.llm.top_p == 0.9
    assert config.llm.max_output_tokens == 32768
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)


@pytest.mark.parametrize("model", ["glm-5.3", "glm-5.3-flash"])
@pytest.mark.parametrize("provider", ["openrouter", "openai"])
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("max", "max"),
        ("high", "high"),
        ("low", "low"),
        ("medium", "low"),
        ("xhigh", "high"),
        ("none", None),
    ],
)
async def test_serialized_request(
    tmp_path, monkeypatch, model, provider, mode, requested, expected
):
    """Exercise create_llm → LangChain → OpenAI SDK → JSON HTTP request.

    The response is a fixture, so this proves wire shape, not provider acceptance.
    """
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "glm-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": model,
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

    def offline_client(**kwargs):
        return original_client(**kwargs, transport=httpx.MockTransport(respond))

    original_async_client = reasoning_chat.AsyncReasoningCapturingClient

    def offline_async_client(**kwargs):
        return original_async_client(**kwargs, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(reasoning_chat, "ReasoningCapturingClient", offline_client)
    monkeypatch.setattr(
        reasoning_chat, "AsyncReasoningCapturingClient", offline_async_client
    )
    monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **kw: 10)
    config = _load(
        tmp_path,
        "openrouter/z-ai/" + model if provider == "openrouter" else model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://glm-fixture.invalid/v1",
        reasoning_level=requested,
        max_retries=0,
        streaming=False,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        content = [{"type": "text", "text": "Check the supplied input."}]
        if model.endswith("flash"):
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": "https://fixture.invalid/reference.png"},
                }
            )
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
        messages = [HumanMessage(content=content)]
        response = (
            await bound.ainvoke(messages) if mode == "async" else bound.invoke(messages)
        )
        assert response.content == "Verified result."
        assert response.additional_kwargs["reasoning_content"] == "fixture reasoning"
        assert len(captured) == 1
        body = captured[0]
        assert body["model"] == ("z-ai/" + model if provider == "openrouter" else model)
        assert body["temperature"] == 1.0
        assert body["top_p"] == 0.95
        assert body["tools"][0]["function"]["name"] == "read_note"
        assert body["parallel_tool_calls"] is False
        assert body.get("max_tokens", body.get("max_completion_tokens")) == 131072
        if provider == "openrouter":
            assert body.get("reasoning") == ({"effort": expected} if expected else None)
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == expected
            assert "reasoning" not in body
        assert "thinking" not in body
        if model.endswith("flash"):
            assert body["messages"][0]["content"][1]["type"] == "image_url"
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
