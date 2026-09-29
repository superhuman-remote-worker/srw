"""Qwen3.8-27B config, system-message folding, and offline provider requests."""

import copy
import json

import httpx
import pytest
import yaml
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat
from shared.runtime.llm.reasoning_chat import fold_system_messages


@pytest.mark.parametrize(
    "model",
    [
        "Qwen/Qwen3.8-27B",
        "Qwen/Qwen3.8-27B-FP8",
        "Inferact/Qwen3.8-27B-NVFP4",
        "qwen3.8-27b",
        "qwen/qwen3.8-27b",
        "qwen/qwen3.8-27b:free",
        "openrouter/qwen/qwen3.8-27b",
    ],
)
def test_detectors_claim_the_27b(model):
    assert family_of(model) == "qwen3.8-27b"
    assert detect_family(model).family == "qwen3.8-27b"


@pytest.mark.parametrize(
    "model",
    [
        "qwen/qwen3-8b",  # Qwen3 8B, not Qwen3.8
        "qwen/qwen3.8-max-0902",
        "qwen/qwen3.8-flash",
        "qwen/qwen3.8-2.4t-a95b",
        "qwen3.8-270b",
        "Qwen/Qwen3.6-27B",
    ],
)
def test_detectors_leave_other_qwen_rows_alone(model):
    assert family_of(model) != "qwen3.8-27b"
    assert detect_family(model).family != "qwen3.8-27b"


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "qwen-test",
                "display_name": "Qwen Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["Qwen/Qwen3.8-27B", "openrouter/qwen/qwen3.8-27b"])
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
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)
    assert config.limits.image_tokens["mode"] == "openai_patches"
    cap = loader.reasoning_capability(config.llm.model)
    assert cap["options"] == ["low", "medium", "xhigh"]
    assert cap["default"] == config.llm.reasoning_level

    prompt = loader.get_phase_system_prompt(
        config,
        is_strategic=False,
        prompt_type="interactive" if role == "session_base" else "systemprompt",
        model=config.llm.model,
        tool_names=[],
    )
    assert "Qwen Test" in prompt
    assert "{%" not in prompt
    # Effort travels as a request field, never as a system-prompt prefix.
    assert not prompt.startswith("Reasoning:")


def test_output_cap_is_clamped_to_the_window_backstop(tmp_path):
    config = _load(tmp_path, "Qwen/Qwen3.8-27B")
    assert config.llm.max_output_tokens == 65536
    resolved = loader._resolve_max_output_tokens(config.llm, config.limits)
    assert resolved < 65536
    assert resolved + config.limits.context_threshold_tokens <= 262144


class TestFoldSystemMessages:
    def test_merges_the_leading_run_and_demotes_later_system_turns(self):
        messages = [
            {"role": "system", "content": "S"},
            {"role": "system", "content": "[Summary of prior work]\nX"},
            {"role": "user", "content": "U"},
            {"role": "assistant", "content": "A"},
            {"role": "system", "content": "nudge"},
            {"role": "user", "content": "U2"},
        ]
        original = copy.deepcopy(messages)
        out = fold_system_messages(messages)
        assert [m["role"] for m in out] == [
            "system",
            "user",
            "assistant",
            "user",
            "user",
        ]
        assert out[0]["content"] == "S\n\n[Summary of prior work]\nX"
        assert out[3]["content"] == "nudge"
        assert messages == original

    def test_list_content_is_concatenated(self):
        out = fold_system_messages(
            [
                {"role": "system", "content": [{"type": "text", "text": "S"}]},
                {"role": "system", "content": "T"},
                {"role": "user", "content": "U"},
            ]
        )
        assert out[0]["content"] == [
            {"type": "text", "text": "S"},
            {"type": "text", "text": "\n\n"},
            {"type": "text", "text": "T"},
        ]

    def test_no_system_message_is_a_no_op(self):
        messages = [{"role": "user", "content": "U"}]
        assert fold_system_messages(messages) == messages
        assert fold_system_messages([]) == []


def _capture(monkeypatch, model_name):
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "qwen-fixture",
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


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested,expected",
    [
        ("low", "low"),
        ("medium", "medium"),
        ("high", "medium"),  # the template raises on `high`
        ("xhigh", "xhigh"),
        ("max", "xhigh"),
        ("none", None),
    ],
)
@pytest.mark.asyncio
async def test_serialized_request(
    tmp_path, monkeypatch, mode, provider, requested, expected
):
    """Exercise the real SDK through offline HTTP; no provider acceptance claim."""
    wire_model = "qwen/qwen3.8-27b" if provider == "openrouter" else "Qwen/Qwen3.8-27B"
    captured = _capture(monkeypatch, wire_model)
    config = _load(
        tmp_path,
        "openrouter/qwen/qwen3.8-27b" if provider == "openrouter" else wire_model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://qwen-fixture.invalid/v1",
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
        response = (
            await bound.ainvoke(_SRW_SHAPED_HISTORY)
            if mode == "async"
            else bound.invoke(_SRW_SHAPED_HISTORY)
        )
        assert response.content == "Verified result."
        assert response.additional_kwargs["reasoning_content"] == "fixture reasoning"
        assert len(captured) == 1
        body = captured[0]
        assert body["model"] == wire_model
        assert body["temperature"] == 1.0
        assert body["top_p"] == 0.95
        assert body["top_k"] == 20
        assert body["parallel_tool_calls"] is False
        assert "thinking" not in body
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if expected is None else {"effort": expected}
            )
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == expected
            assert "reasoning" not in body
        roles = [m["role"] for m in body["messages"]]
        assert roles == ["system", "user", "assistant", "user", "user"]
        assert body["messages"][0]["content"] == (
            "System prompt.\n\n[Summary of prior work]\nEarlier steps."
        )
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()


def test_other_families_keep_their_system_messages(tmp_path, monkeypatch):
    """The fold is family-gated: a kimi-k3 row still sends both system turns."""
    captured = _capture(monkeypatch, "kimi-k3")
    config = _load(
        tmp_path,
        "kimi-k3",
        provider="openai",
        api_key="fixture-key",
        base_url="https://kimi-fixture.invalid/v1",
        max_retries=0,
        streaming=False,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        assert llm.single_system_message is False
        llm.invoke(_SRW_SHAPED_HISTORY)
        roles = [m["role"] for m in captured[0]["messages"]]
        assert roles == ["system", "system", "user", "assistant", "system", "user"]
    finally:
        llm.http_client.close()
