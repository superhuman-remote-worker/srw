"""Grok 4.7 config and offline provider requests."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize(
    "model",
    [
        "grok-4.7",
        "Grok-4.7",
        "grok-4.7-fast",
        "x-ai/grok-4.7",
        "x-ai/grok-4.7-20260916",
        "openrouter/x-ai/grok-4.7",
    ],
)
def test_detectors_claim_grok_4_7(model):
    assert family_of(model) == "grok-4.7"
    assert detect_family(model).family == "grok-4.7"


@pytest.mark.parametrize(
    "model",
    [
        "grok-4.6",
        "x-ai/grok-4.5",
        "grok-4.3",
        "grok-4.20-0309-reasoning",
        "grok-build-0.1",
        "grok-4.70",
        "grok-4.7.1",
    ],
)
def test_detectors_leave_other_grok_rows_on_default(model):
    assert family_of(model) == "default"
    assert detect_family(model).family == "default"


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "grok-test",
                "display_name": "Grok Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["grok-4.7", "openrouter/x-ai/grok-4.7"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults(tmp_path, model, role):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "high"
    assert config.llm.temperature == 0.7
    assert config.llm.top_p == 0.95
    assert config.llm.top_k is None
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is False
    settings = loader.resolve_model_settings(config.llm.model)
    assert settings["structured_output_method"] == "json_schema"
    assert "single_system_message" not in settings
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.limits.model_max_context_tokens == 500000
    assert config.limits.context_threshold_tokens == int(500000 * 0.8)
    assert config.limits.image_tokens == {"mode": "flat", "flat": 1792}
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "medium", "high", "xhigh"]
    assert cap["default"] == config.llm.reasoning_level


def test_output_cap_is_clamped_to_the_window_backstop(tmp_path):
    config = _load(tmp_path, "grok-4.7")
    assert config.llm.max_output_tokens == 128000
    resolved = loader._resolve_max_output_tokens(config.llm, config.limits)
    assert resolved < 128000
    assert resolved + config.limits.context_threshold_tokens <= 500000


def _capture(monkeypatch, model_name):
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "grok-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": model_name,
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
    return captured


_HISTORY = [
    SystemMessage(content="System prompt."),
    SystemMessage(content="[Summary of prior work]\nEarlier steps."),
    HumanMessage(content="Continue the task."),
    AIMessage(content="Working on it."),
    HumanMessage(content="Todo list."),
]

# xAI returns an error for these on reasoning models.
_REJECTED = ("stop", "presence_penalty", "frequency_penalty")


@pytest.mark.parametrize("provider", ["openai", "openrouter", "codex"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("medium", "medium"),
        ("high", "high"),
        ("xhigh", "xhigh"),
        ("max", "xhigh"),
        ("none", None),
    ],
)
def test_serialized_request(tmp_path, monkeypatch, provider, requested, expected):
    """xAI direct (openai wire), OpenRouter, and the Grok Build proxy lane."""
    wire_model = "x-ai/grok-4.7" if provider == "openrouter" else "grok-4.7"
    captured = _capture(monkeypatch, wire_model)
    model = {
        "openai": "grok-4.7",
        "openrouter": "openrouter/x-ai/grok-4.7",
        # A Grok Build row: bare id on the proxy's Responses-protocol lane.
        "codex": "grok-4.7",
    }[provider]
    config = _load(
        tmp_path,
        model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://grok-fixture.invalid/v1",
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
        bound.invoke(_HISTORY)
        body = captured[0]
        assert body["model"] == wire_model
        assert body["temperature"] == 0.7
        assert body["top_p"] == 0.95
        assert "top_k" not in body
        assert body["parallel_tool_calls"] is False
        for key in _REJECTED:
            assert key not in body
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == expected
        # No fold: xAI takes system messages in any position.
        roles = [m["role"] for m in body["messages"]]
        assert roles == ["system", "system", "user", "assistant", "user"]
    finally:
        llm.http_client.close()
