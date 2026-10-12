"""GPT-6.1 Sol family resolution, rendered prompts, and offline SDK transport."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize("prefix", ["", "openai/", "codex/", "openrouter/openai/"])
@pytest.mark.parametrize(
    "model", ["gpt-6.1-sol", "GPT-6.1-SOL", "gpt-6.1-sol-20260929", "gpt-6.1-sol:batch"]
)
def test_detectors_agree(prefix, model):
    assert family_of(prefix + model) == "gpt-6.1-sol"
    assert detect_family(prefix + model).family == "gpt-6.1-sol"


@pytest.mark.parametrize(
    "model,family",
    [
        ("gpt-6-astra", "gpt-6"),
        ("gpt-6-sol", "gpt-6"),
        ("gpt-6-luna", "gpt-6"),
        ("gpt-6.10-sol", "gpt-6"),
        ("gpt-6.1-solstice", "gpt-6"),
        ("gpt-6.1-luna", "gpt-6"),
        ("gpt-5.6-sol", "gpt-5.6"),
        ("gpt-6.1-sol-codex", "codex"),
        ("codex/gpt-6.1-sol-codex", "codex"),
        ("gpt-6.1-sol-codex-spark", "codex-spark"),
    ],
)
def test_other_families_keep_their_settings(model, family):
    assert family_of(model) == family
    assert detect_family(model).family == family


def _load(tmp_path, model="gpt-6.1-sol", *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "gpt61-test",
                "display_name": "Sol Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("role", ["worker_base", "session_base"])
@pytest.mark.parametrize(
    "model", ["gpt-6.1-sol", "codex/gpt-6.1-sol", "openrouter/openai/gpt-6.1-sol"]
)
def test_loaded_config_and_rendered_prompts(tmp_path, role, model):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "medium"
    assert config.llm.temperature == 1.0  # Config value only; omitted on wire.
    assert config.llm.top_p is None
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is True
    assert config.llm.max_output_tokens == 128000
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.limits.model_max_context_tokens == 1050000
    assert config.limits.context_threshold_tokens == 840000
    assert config.limits.context_threshold_tokens < 922000  # Actual input ceiling.
    assert (
        loader.resolve_model_settings(model)["structured_output_method"]
        == "json_schema"
    )
    cap = loader.reasoning_capability(model)
    assert cap["default"] == "medium"
    assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]
    prompt = loader.get_phase_system_prompt(
        config,
        is_strategic=False,
        model=model,
        tool_names=[],
        prompt_type="interactive" if role == "session_base" else "systemprompt",
    )
    assert "<execution_contract>" in prompt
    assert "Sol Test" in prompt
    assert "{%" not in prompt
    assert "{expert_identity}" not in prompt
    assert "Delegate independent" not in prompt
    resolver = loader.PromptMatrixResolver(model_family="gpt-6.1-sol")
    assert resolver.resolve_filename("persona") == "persona_gpt_6_1_sol.txt"
    assert (
        resolver.resolve_filename("summarization") == "summarization_prompt_gpt_5.txt"
    )


def test_explicit_settings_still_win(tmp_path):
    config = _load(
        tmp_path,
        reasoning_level="xhigh",
        model_max_context_tokens=262144,
        max_output_tokens=32768,
    )
    assert config.llm.reasoning_level == "xhigh"
    assert config.llm.max_output_tokens == 32768
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("provider", ["openai", "codex", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("medium", "medium"),
        ("max", "max"),
        ("xhigh", "xhigh"),
        ("minimal", "low"),
        ("none", None),
    ],
)
@pytest.mark.asyncio
async def test_serialized_tool_request(
    tmp_path, monkeypatch, mode, provider, requested, expected
):
    """Real SDK through mock HTTP: proves serialization, not provider acceptance."""
    captured = []
    is_responses = provider != "openrouter"
    wire_model = "gpt-6.1-sol" if is_responses else "openai/gpt-6.1-sol"

    def respond(request):
        captured.append((request.url.path, json.loads(request.content)))
        if is_responses:
            body = {
                "id": "resp_fixture",
                "object": "response",
                "created_at": 0,
                "model": wire_model,
                "status": "completed",
                "output": [
                    {
                        "id": "msg_fixture",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "Verified result.",
                                "annotations": [],
                            }
                        ],
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
            }
        else:
            body = {
                "id": "chat_fixture",
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
            }
        return httpx.Response(200, json=body)

    original = reasoning_chat.ReasoningCapturingClient
    original_async = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original_async(**kw, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **kw: 10)
    config = _load(
        tmp_path,
        model=f"codex/{wire_model}"
        if provider == "codex"
        else f"openrouter/{wire_model}"
        if provider == "openrouter"
        else wire_model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://sol-fixture.invalid/v1",
        reasoning_level=requested,
        temperature=0.2,
        top_p=0.8,
        top_k=64,
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
                        "description": "Read a note.",
                        "parameters": {"type": "object", "properties": {}},
                    },
                }
            ],
            parallel_tool_calls=config.llm.parallel_tool_calls,
        )
        messages = [HumanMessage(content="Check the supplied input.")]
        result = (
            await bound.ainvoke(messages) if mode == "async" else bound.invoke(messages)
        )
        assert "Verified result." in str(result.content)
        assert len(captured) == 1
        path, body = captured[0]
        assert path == ("/v1/responses" if is_responses else "/v1/chat/completions")
        assert body["model"] == wire_model
        assert "temperature" not in body
        assert "top_p" not in body
        assert "top_k" not in body
        assert "reasoning_effort" not in body
        assert body["parallel_tool_calls"] is True
        if is_responses:
            assert body["max_output_tokens"] == 128000
            assert body["tools"][0]["name"] == "read_note"
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected, "summary": "auto"}
            )
        else:
            assert body.get("max_tokens", body.get("max_completion_tokens")) == 128000
            assert body["tools"][0]["function"]["name"] == "read_note"
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
