"""Muse Spark 1.3 config, prompts, and offline provider SDK requests."""

import json

import httpx
import pytest
import yaml
from langchain_core.messages import HumanMessage

from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat


@pytest.mark.parametrize("prefix", ["", "meta/", "openrouter/meta/"])
@pytest.mark.parametrize(
    "model,family",
    [
        ("Muse-Spark-1.3", "muse-spark-1.3"),
        ("muse-spark-1.3-contributor", "muse-spark-1.3"),
        ("muse-spark-1.3-20260902", "muse-spark-1.3"),
        ("muse-spark-1.3:exacto", "muse-spark-1.3"),
        ("muse-spark-1.2", "default"),
        ("muse-spark-1.30", "default"),
    ],
)
def test_detectors_agree(prefix, model, family):
    assert family_of(prefix + model) == family
    assert detect_family(prefix + model).family == family


@pytest.mark.parametrize("prefix", ["", "meta/", "openrouter/meta/"])
def test_contributor_capability_caps_at_xhigh(prefix):
    """Contributor advertises xhigh max; Standard keeps max (all ID prefixes)."""
    std_cap = loader.reasoning_capability(prefix + "muse-spark-1.3")
    contrib_cap = loader.reasoning_capability(prefix + "muse-spark-1.3-contributor")
    assert std_cap["options"] == ["minimal", "low", "medium", "high", "xhigh", "max"]
    assert contrib_cap["options"] == ["minimal", "low", "medium", "high", "xhigh"]
    assert std_cap["default"] == "medium"
    assert contrib_cap["default"] == "medium"
    # Existing runtime clamp: Contributor legacy max -> xhigh; Standard unchanged.
    assert (
        loader._clamp_reasoning_level("max", loader._supported_efforts(contrib_cap))
        == "xhigh"
    )
    assert (
        loader._clamp_reasoning_level("max", loader._supported_efforts(std_cap))
        == "max"
    )


@pytest.mark.parametrize(
    "model_id,expected",
    [
        ("muse-spark-1.3-contributor", True),
        ("meta/muse-spark-1.3-contributor", True),
        ("openrouter/meta/muse-spark-1.3-contributor", True),
        ("muse-spark-1.3-contributor:exacto", True),
        ("muse-spark-1.3-contributor-20260902", True),
        ("MUSE-SPARK-1.3-CONTRIBUTOR", True),
        ("muse-spark-1.3-contributorish", False),
        ("contributor-org/muse-spark-1.3", False),
        ("openrouter/contributor-org/muse-spark-1.3", False),
        ("muse-spark-1.3-contributor-team/muse-spark-1.3", False),
        ("muse-spark-1.3", False),
        ("meta/muse-spark-1.3", False),
        ("openrouter/meta/muse-spark-1.3", False),
        ("muse-spark-1.3-20260902", False),
    ],
)
def test_contributor_tier_matcher_negatives(model_id, expected):
    """Tier suffix only: org-prefix or missing boundary keeps Standard options."""
    assert loader._is_muse_contributor_tier(model_id) is expected
    cap = loader.reasoning_capability(model_id)
    if expected:
        assert cap["options"] == ["minimal", "low", "medium", "high", "xhigh"]
    else:
        assert "max" in [str(o).lower() for o in cap["options"]]


@pytest.mark.parametrize("order", ["std-first", "contrib-first"])
def test_capability_lookup_order_independent(order):
    """Both lookup orders agree; Contributor narrowing never mutates the cache."""
    std_id = "openrouter/meta/muse-spark-1.3"
    contrib_id = "openrouter/meta/muse-spark-1.3-contributor"
    first, second = (
        (std_id, contrib_id) if order == "std-first" else (contrib_id, std_id)
    )
    loader.reasoning_capability(first)
    loader.reasoning_capability(second)
    std_cap = loader.reasoning_capability(std_id)
    contrib_cap = loader.reasoning_capability(contrib_id)
    assert std_cap["options"] == ["minimal", "low", "medium", "high", "xhigh", "max"]
    assert contrib_cap["options"] == ["minimal", "low", "medium", "high", "xhigh"]
    assert std_cap is not contrib_cap
    # Mutating the Contributor copy must not leak into Standard (cached block).
    contrib_cap["options"].append("max")
    assert loader.reasoning_capability(std_id)["options"] == [
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]
    assert loader.reasoning_capability(contrib_id)["options"] == [
        "minimal",
        "low",
        "medium",
        "high",
        "xhigh",
    ]


def _load(tmp_path, model, *, role="worker_base", **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "muse-test",
                "display_name": "Muse Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("model", ["muse-spark-1.3", "muse-spark-1.3-contributor"])
@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_loaded_defaults_and_prompts(tmp_path, model, role):
    config = _load(tmp_path, "openrouter/meta/" + model, role=role)
    assert config.llm.reasoning_level == "medium"
    assert config.llm.temperature == 1.0
    assert config.llm.top_p == 1.0
    assert config.llm.multimodal is True
    assert config.llm.parallel_tool_calls is False
    settings = loader.resolve_model_settings(config.llm.model)
    assert settings["structured_output_method"] == "function_calling"
    assert config.extra["shell"]["mode"] == "persistent"
    assert config.llm.max_output_tokens == 131072
    assert config.limits.model_max_context_tokens == 1048576
    assert config.limits.context_threshold_tokens == int(1048576 * 0.8)
    cap = loader.reasoning_capability(config.llm.model)
    if "contributor" in config.llm.model.lower():
        assert cap["options"] == ["minimal", "low", "medium", "high", "xhigh"]
    else:
        assert cap["options"] == ["minimal", "low", "medium", "high", "xhigh", "max"]
    assert cap["default"] == config.llm.reasoning_level

    prompt = loader.get_phase_system_prompt(
        config,
        is_strategic=False,
        prompt_type="interactive" if role == "session_base" else "systemprompt",
        model=config.llm.model,
        tool_names=[],
    )
    assert "<execution_contract>" in prompt
    assert "When the user corrects a detail" in prompt
    assert "Muse Test" in prompt
    assert "{%" not in prompt
    assert "{expert_identity}" not in prompt
    resolver = loader.PromptMatrixResolver(model_family="muse-spark-1.3")
    assert resolver.resolve_filename("persona") == "persona_muse_spark_1_3.txt"
    assert resolver.resolve_filename("summarization") == "summarization_prompt.txt"


def test_explicit_settings_override_family_defaults(tmp_path):
    config = _load(
        tmp_path,
        "meta/muse-spark-1.3",
        reasoning_level="max",
        temperature=0.7,
        top_p=0.9,
        model_max_context_tokens=262144,
        max_output_tokens=32768,
    )
    assert config.llm.reasoning_level == "max"
    assert config.llm.temperature == 0.7
    assert config.llm.top_p == 0.9
    assert config.llm.max_output_tokens == 32768
    assert config.limits.model_max_context_tokens == 262144
    assert config.limits.context_threshold_tokens == int(262144 * 0.8)


@pytest.mark.parametrize("model", ["muse-spark-1.3", "muse-spark-1.3-contributor"])
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("provider", ["openai", "openrouter"])
@pytest.mark.parametrize(
    "requested", ["minimal", "low", "medium", "high", "xhigh", "max", "none"]
)
@pytest.mark.asyncio
async def test_serialized_request(
    tmp_path, monkeypatch, model, mode, provider, requested
):
    """Exercise the real SDK through offline HTTP; no provider acceptance claim."""
    captured = []

    def respond(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "muse-fixture",
                "object": "chat.completion",
                "created": 0,
                "model": "meta/" + model,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "Verified result.",
                            "reasoning": "fixture reasoning",
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
        ("openrouter/meta/" if provider == "openrouter" else "meta/") + model,
        provider=provider,
        api_key="fixture-key",
        base_url="https://muse-fixture.invalid/v1",
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
                        "image_url": {"url": "https://fixture.invalid/reference.png"},
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
        assert body["model"] == "meta/" + model
        assert body["temperature"] == 1.0
        assert body["top_p"] == 1.0
        assert body["tools"][0]["function"]["name"] == "read_note"
        assert body["parallel_tool_calls"] is False
        assert body.get("max_tokens", body.get("max_completion_tokens")) == 131072
        # A stale 'none' setting omits control; it never sends a disable request.
        # Contributor tier caps at xhigh: legacy max clamps to xhigh (Meta docs;
        # pilot 9792db96 Contributor/max HTTP400, xhigh succeeds). Standard keeps max.
        expected = requested
        if requested == "max" and "contributor" in model.lower():
            expected = "xhigh"
        if provider == "openrouter":
            assert body.get("reasoning") == (
                None if requested == "none" else {"effort": expected}
            )
            assert "reasoning_effort" not in body
        else:
            assert body.get("reasoning_effort") == (
                None if requested == "none" else expected
            )
            assert "reasoning" not in body
        assert "thinking" not in body
        assert body["messages"][0]["content"][1]["type"] == "image_url"
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
