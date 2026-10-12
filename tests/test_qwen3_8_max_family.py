"""Qwen3.8 Max / Max Prime config and offline provider requests."""

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
        "qwen3.8-max",
        "qwen3.8-max-2026-09-02",
        "qwen/qwen3.8-max-0902",
        "qwen/qwen3.8-max-prime",
        "openrouter/qwen/qwen3.8-max-prime",
        "Qwen3.8-Max",
    ],
)
def test_detectors_claim_max(model):
    assert family_of(model) == "qwen3.8-max"
    assert detect_family(model).family == "qwen3.8-max"


@pytest.mark.parametrize(
    "model",
    [
        "qwen/qwen3.8-2.4t-a95b",  # open weights, text-only
        "qwen/qwen3.8-flash",
        "qwen/qwen3.7-max",
        "qwen3.8-maximal",
    ],
)
def test_detectors_leave_other_qwen_rows_alone(model):
    assert family_of(model) != "qwen3.8-max"
    assert detect_family(model).family != "qwen3.8-max"


def test_the_27b_keeps_its_own_family():
    assert family_of("qwen/qwen3.8-27b") == "qwen3.8-27b"
    assert detect_family("qwen/qwen3.8-27b").family == "qwen3.8-27b"


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "qwen-max-test",
                "display_name": "Qwen Max Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["qwen3.8-max", "openrouter/qwen/qwen3.8-max-prime"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults(tmp_path, model, role):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "xhigh"
    assert config.llm.temperature == 1.0
    assert config.llm.top_p == 0.95
    assert config.llm.top_k == 20
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is False
    settings = loader.resolve_model_settings(config.llm.model)
    assert settings["structured_output_method"] == "json_schema"
    assert settings["single_system_message"] is True
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.limits.model_max_context_tokens == 1000000
    assert config.limits.image_tokens["budget"] == 2560
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "medium", "xhigh"]
    assert cap["default"] == config.llm.reasoning_level


def test_output_cap_fits_the_window_backstop(tmp_path):
    config = _load(tmp_path, "qwen3.8-max")
    assert config.llm.max_output_tokens == 131072
    resolved = loader._resolve_max_output_tokens(config.llm, config.limits)
    assert resolved <= 131072
    assert resolved + config.limits.context_threshold_tokens <= 1000000


def _capture(monkeypatch, model_name):
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "qwen-max-fixture",
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


_SRW_SHAPED_HISTORY = [
    SystemMessage(content="System prompt."),
    SystemMessage(content="[Summary of prior work]\nEarlier steps."),
    HumanMessage(content="Continue the task."),
    AIMessage(content="Working on it."),
    SystemMessage(content="OBSERVATION: no progress."),
    HumanMessage(content="Todo list."),
]


@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("medium", "medium"),
        ("high", "medium"),  # clamps down, like every family ladder
        ("xhigh", "xhigh"),
        ("max", "xhigh"),
        ("none", None),
    ],
)
def test_serialized_request(tmp_path, monkeypatch, provider, requested, expected):
    """DashScope (openai wire) and OpenRouter; no provider acceptance claim."""
    wire_model = "qwen/qwen3.8-max-0902" if provider == "openrouter" else "qwen3.8-max"
    captured = _capture(monkeypatch, wire_model)
    config = _load(
        tmp_path,
        "openrouter/qwen/qwen3.8-max-0902" if provider == "openrouter" else wire_model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://qwen-max-fixture.invalid/v1",
        reasoning_level=requested,
        max_retries=0,
        streaming=False,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        llm.invoke(_SRW_SHAPED_HISTORY)
        body = captured[0]
        assert body["model"] == wire_model
        assert body["temperature"] == 1.0
        assert body["top_p"] == 0.95
        assert body["top_k"] == 20
        assert "thinking_budget" not in body
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == expected
        roles = [m["role"] for m in body["messages"]]
        assert roles == ["system", "user", "assistant", "user", "user"]
        assert body["messages"][0]["content"] == (
            "System prompt.\n\n[Summary of prior work]\nEarlier steps."
        )
    finally:
        llm.http_client.close()
