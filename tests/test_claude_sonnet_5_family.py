"""Claude Sonnet 5 / 5.5 config and offline provider requests."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage, SystemMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize(
    "model",
    [
        "claude-sonnet-5",
        "claude-sonnet-5-20260630",
        "claude-sonnet-5-5",
        "claude-sonnet-5-5-20260928",
        "openrouter/anthropic/claude-sonnet-5.5",
        "openrouter/anthropic/claude-sonnet-5",
    ],
)
def test_detectors_claim_sonnet_5(model):
    assert family_of(model) == "claude-sonnet-5"
    assert detect_family(model).family == "claude-sonnet-5"


@pytest.mark.parametrize(
    "model",
    [
        "claude-sonnet-4-6",
        "claude-sonnet-4-5-20250929",
        "openrouter/anthropic/claude-sonnet-4.6",
    ],
)
def test_detectors_keep_sonnet_4_generic(model):
    assert family_of(model) == "claude-sonnet"
    assert detect_family(model).family == "claude-sonnet"


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "sonnet-test",
                "display_name": "Sonnet Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["claude-sonnet-5-5", "claude-sonnet-5"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults(tmp_path, model, role):
    config = _load(tmp_path, model, role=role)
    assert config.llm.reasoning_level == "high"
    # 1.0 is Anthropic's default; anything else 400s, and a missing key would
    # fall through to `default`'s 0.0.
    assert config.llm.temperature == 1.0
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is True
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.limits.model_max_context_tokens == 1000000
    assert config.limits.image_tokens["max_edge"] == 2576
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "medium", "high", "xhigh", "max"]
    assert cap["default"] == config.llm.reasoning_level
    # Forced tool use is a 400 on 5.5; nothing may switch this family off
    # json_schema.
    settings = loader.resolve_model_settings(config.llm.model)
    assert settings.get("structured_output_method", "json_schema") == "json_schema"


def test_sonnet_4_keeps_its_narrow_ladder():
    cap = loader.reasoning_capability("claude-sonnet-4-6")
    assert cap["options"] == ["low", "medium", "high"]


def _capture(monkeypatch, model_name):
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "sonnet-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": model_name,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "Done."},
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


@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("high", "high"),
        ("xhigh", "xhigh"),
        ("max", "max"),
        ("minimal", "low"),
        ("none", None),
    ],
)
def test_serialized_request(tmp_path, monkeypatch, provider, requested, expected):
    """The proxy (openai wire) and OpenRouter both get the full ladder."""
    wire_model = (
        "anthropic/claude-sonnet-5.5"
        if provider == "openrouter"
        else "claude-sonnet-5-5"
    )
    captured = _capture(monkeypatch, wire_model)
    config = _load(
        tmp_path,
        "openrouter/anthropic/claude-sonnet-5.5"
        if provider == "openrouter"
        else wire_model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://sonnet-fixture.invalid/v1",
        reasoning_level=requested,
        max_retries=0,
        streaming=False,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        llm.invoke(
            [SystemMessage(content="System prompt."), HumanMessage(content="Go.")]
        )
        body = captured[0]
        assert body["model"] == wire_model
        assert body["temperature"] == 1.0
        assert "top_p" not in body and "top_k" not in body
        assert "tool_choice" not in body
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
        else:
            assert body.get("reasoning_effort") == expected
    finally:
        llm.http_client.close()
