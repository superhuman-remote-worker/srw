"""A family with ``replay_reasoning_content`` gets its earlier reasoning back.

DeepSeek's thinking-mode guide: on a request that carries ``tools``, "the
reasoning_content must be fully passed back to the API in all subsequent
requests ... If your code does not correctly pass back reasoning_content, the
API will return a 400 error." langchain-openai builds an assistant message
from content and tool calls only, so ``ReasoningChatOpenAI`` puts the captured
reasoning back (``attach_reasoning_content``) for a family whose settings ask
for it (model_config_matrix.yaml: ``deepseek``).

These tests pin:

- the payload shape with the setting on and off, through the real SDK behind
  an offline transport;
- that the setting is the model family's, that a job can turn it off and that
  nothing can turn it on for a family without it;
- that the request-body and message token counts include the text that is
  sent;
- that two calls running at once on one client each keep their own reasoning.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

import httpx
import pytest
import yaml
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.context import (
    count_tokens_approximate,
    count_tokens_tiktoken,
    get_token_counter,
)
from shared.runtime.core import loader
from shared.runtime.core.loader import LLMConfig, PhaseLLMOverride
from shared.runtime.core.model_registry import family_of
from shared.runtime.llm import reasoning_chat
from shared.runtime.llm.reasoning_chat import (
    ReasoningChatOpenAI,
    attach_reasoning_content,
    count_request_tokens,
)

# langchain-openai warns about SRW's `reasoning_effort` in model_kwargs on
# every Chat Completions client this module builds; it is not under test here.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)

DEEPSEEK_URL = "https://deepseek-fixture.invalid/v1"

_TOOL = {
    "type": "function",
    "function": {
        "name": "read_note",
        "description": "Read a workspace note.",
        "parameters": {"type": "object", "properties": {}},
    },
}


def _tool_turn(reasoning: str | None = "step one reasoning") -> AIMessage:
    kwargs = {} if reasoning is None else {"reasoning_content": reasoning}
    return AIMessage(
        content="",
        tool_calls=[{"name": "read_note", "args": {}, "id": "call_1"}],
        additional_kwargs=kwargs,
    )


def _history() -> list:
    return [
        SystemMessage(content="System prompt."),
        HumanMessage(content="Read the note."),
        _tool_turn(),
        ToolMessage(content="note text", tool_call_id="call_1"),
        # An assistant turn SRW wrote itself: it has no reasoning.
        AIMessage(content="Phase complete."),
        HumanMessage(content="Continue."),
    ]


def _completion(model: str, message: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": "fixture",
        "object": "chat.completion",
        "created": 0,
        "model": model,
        "choices": [{"index": 0, "finish_reason": "stop", "message": message}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


def _capture(monkeypatch, respond) -> List[Dict[str, Any]]:
    """Route both HTTP clients to ``respond(body)``; return the bodies sent."""
    captured: List[Dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured.append(body)
        return httpx.Response(200, json=respond(body))

    original_client = reasoning_chat.ReasoningCapturingClient
    original_async_client = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: original_client(**kw, transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original_async_client(
            **kw, transport=httpx.MockTransport(handler)
        ),
    )
    return captured


def _load(tmp_path, model, **overrides):
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": "worker_base",
                "agent_id": "replay-test",
                "display_name": "Replay Test",
                "llm": {"model": model, **overrides},
            }
        )
    )
    return loader.load_agent_config(str(path))


def _deepseek_config(tmp_path, **overrides):
    return _load(
        tmp_path,
        "deepseek-flash",
        provider="openai",
        api_key="fixture-key",
        base_url=DEEPSEEK_URL,
        max_retries=0,
        **overrides,
    )


# ---------------------------------------------------------------------------
# attach_reasoning_content
# ---------------------------------------------------------------------------


class TestAttachReasoningContent:
    def test_every_assistant_turn_carries_the_field(self):
        messages = _history()
        payload = [
            {"role": "system", "content": "System prompt."},
            {"role": "user", "content": "Read the note."},
            {"role": "assistant", "content": None, "tool_calls": [{"id": "call_1"}]},
            {"role": "tool", "content": "note text", "tool_call_id": "call_1"},
            {"role": "assistant", "content": "Phase complete."},
            {"role": "user", "content": "Continue."},
        ]
        before = json.dumps(payload)

        out = attach_reasoning_content(messages, payload)

        assert [m.get("reasoning_content") for m in out] == [
            None,
            None,
            "step one reasoning",
            None,
            "",
            None,
        ]
        # Nothing else on the wire message changes.
        assert out[2]["tool_calls"] == [{"id": "call_1"}]
        assert out[2]["content"] is None
        # Only assistant messages gain a key, and the input is not mutated.
        assert all(
            "reasoning_content" not in m for m in out if m["role"] != "assistant"
        )
        assert json.dumps(payload) == before

    def test_reasoning_is_sent_unchanged(self):
        reasoning = "  line one\n\nline two — ünïcödé  \n"
        out = attach_reasoning_content(
            [_tool_turn(reasoning)], [{"role": "assistant", "content": None}]
        )
        assert out[0]["reasoning_content"] == reasoning

    def test_non_string_reasoning_becomes_the_empty_string(self):
        message = AIMessage(content="x", additional_kwargs={"reasoning_content": None})
        out = attach_reasoning_content([message], [{"role": "assistant"}])
        assert out[0]["reasoning_content"] == ""

    def test_length_mismatch_returns_the_payload_unchanged(self):
        payload = [{"role": "assistant", "content": "x"}]
        assert attach_reasoning_content(_history(), payload) is payload


# ---------------------------------------------------------------------------
# The request the SDK sends
# ---------------------------------------------------------------------------


def _reply(body: Dict[str, Any]) -> Dict[str, Any]:
    return _completion(
        body["model"],
        {
            "role": "assistant",
            "content": "Verified result.",
            "reasoning_content": "fixture reasoning",
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_deepseek_request_carries_earlier_reasoning(tmp_path, monkeypatch, mode):
    captured = _capture(monkeypatch, _reply)
    config = _deepseek_config(tmp_path, streaming=False)
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        assert llm.replay_reasoning_content is True
        bound = llm.bind_tools([_TOOL])
        response = (
            await bound.ainvoke(_history())
            if mode == "async"
            else bound.invoke(_history())
        )
        assert response.additional_kwargs["reasoning_content"] == "fixture reasoning"

        assert len(captured) == 1
        body = captured[0]
        assert body["tools"], "the rule applies to requests that carry tools"
        wire = body["messages"]
        assert [m["role"] for m in wire] == [
            "system",
            "user",
            "assistant",
            "tool",
            "assistant",
            "user",
        ]
        assert wire[2]["reasoning_content"] == "step one reasoning"
        assert wire[2]["tool_calls"][0]["id"] == "call_1"
        assert wire[4]["reasoning_content"] == ""
        assert all(
            "reasoning_content" not in m for m in wire if m["role"] != "assistant"
        )
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()


@pytest.mark.asyncio
async def test_two_step_tool_loop_sends_each_response_back(tmp_path, monkeypatch):
    """What one response returned is what the next request carries for it."""

    def respond(body: Dict[str, Any]) -> Dict[str, Any]:
        step = sum(1 for m in body["messages"] if m["role"] == "assistant") + 1
        message: Dict[str, Any] = {
            "role": "assistant",
            "content": "" if step < 3 else "Done.",
            "reasoning_content": f"reasoning of step {step}",
        }
        if step < 3:
            message["tool_calls"] = [
                {
                    "id": f"call_{step}",
                    "type": "function",
                    "function": {"name": "read_note", "arguments": "{}"},
                }
            ]
        return _completion(body["model"], message)

    captured = _capture(monkeypatch, respond)
    config = _deepseek_config(tmp_path, streaming=False)
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        bound = llm.bind_tools([_TOOL])
        messages: list = [SystemMessage(content="S"), HumanMessage(content="Go.")]
        for _ in range(3):
            response = await bound.ainvoke(messages)
            messages.append(response)
            for call in response.tool_calls:
                messages.append(ToolMessage(content="note", tool_call_id=call["id"]))

        assert len(captured) == 3
        assert [
            m["reasoning_content"]
            for m in captured[2]["messages"]
            if m["role"] == "assistant"
        ] == ["reasoning of step 1", "reasoning of step 2"]
        assert [
            m["reasoning_content"]
            for m in captured[1]["messages"]
            if m["role"] == "assistant"
        ] == ["reasoning of step 1"]
        assert not [m for m in captured[0]["messages"] if m["role"] == "assistant"]
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()


def test_a_job_can_turn_the_replay_off(tmp_path, monkeypatch):
    captured = _capture(monkeypatch, _reply)
    config = _deepseek_config(tmp_path, streaming=False, replay_reasoning_content=False)
    assert config.llm.replay_reasoning_content is False
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        assert llm.replay_reasoning_content is False
        llm.bind_tools([_TOOL]).invoke(_history())
        assert all("reasoning_content" not in m for m in captured[0]["messages"])
    finally:
        llm.http_client.close()


@pytest.mark.parametrize(
    "model", ["gpt-oss-120b", "kimi-k3", "qwen/qwen3.8-max", "grok-4.7", "glm-5.3"]
)
def test_other_families_send_no_reasoning(tmp_path, monkeypatch, model):
    """Family-gated, and an explicit ``true`` cannot arm a family without it."""
    captured = _capture(monkeypatch, _reply)
    config = _load(
        tmp_path,
        model,
        provider="openai",
        api_key="fixture-key",
        base_url="https://other-fixture.invalid/v1",
        max_retries=0,
        streaming=False,
        replay_reasoning_content=True,
    )
    llm = loader.create_llm(config.llm, limits=config.limits)
    try:
        assert llm.replay_reasoning_content is False
        llm.invoke(_history())
        assert all("reasoning_content" not in m for m in captured[0]["messages"])
    finally:
        llm.http_client.close()


def test_folded_system_messages_keep_the_mapping():
    """The replay runs before the system fold, which changes the count."""
    llm = ReasoningChatOpenAI(
        model="fixture",
        api_key="k",
        base_url=DEEPSEEK_URL,
        replay_reasoning_content=True,
        single_system_message=True,
    )
    try:
        payload = llm._get_request_payload(
            [SystemMessage(content="A"), SystemMessage(content="B"), *_history()[1:]]
        )
        wire = payload["messages"]
        assert [m["role"] for m in wire][:2] == ["system", "user"]
        assert [m["reasoning_content"] for m in wire if m["role"] == "assistant"] == [
            "step one reasoning",
            "",
        ]
    finally:
        llm.http_client.close()


def test_responses_api_payload_is_untouched():
    llm = ReasoningChatOpenAI(
        model="fixture",
        api_key="k",
        base_url=DEEPSEEK_URL,
        replay_reasoning_content=True,
        use_responses_api=True,
    )
    try:
        payload = llm._get_request_payload(_history())
        assert "messages" not in payload
        assert "reasoning_content" not in json.dumps(payload)
    finally:
        llm.http_client.close()


# ---------------------------------------------------------------------------
# The setting
# ---------------------------------------------------------------------------


class TestFamilySetting:
    @pytest.mark.parametrize(
        "model,family",
        [
            # V4.1 Flash is the deepseek entry with vision.
            ("deepseek-flash", "deepseek-v4.1-flash"),
            ("deepseek-v4-pro", "deepseek"),
            ("deepseek/deepseek-v4-flash", "deepseek"),
        ],
    )
    def test_deepseek_has_it(self, model, family):
        assert family_of(model) == family
        assert loader.resolve_model_settings(model)["replay_reasoning_content"] is True
        assert loader.replays_reasoning_content(model) is True

    @pytest.mark.parametrize(
        "model",
        ["gpt-oss-120b", "kimi-k3", "qwen/qwen3.8-max", "grok-4.7", "gpt-5.5", ""],
    )
    def test_no_other_family_has_it(self, model):
        assert loader.replays_reasoning_content(model) is False

    def test_loaded_config_carries_the_family_value(self, tmp_path):
        assert _load(tmp_path, "deepseek-flash").llm.replay_reasoning_content is True
        assert _load(tmp_path, "kimi-k3").llm.replay_reasoning_content is None

    def test_an_override_keeps_the_off_switch(self):
        base = LLMConfig(model="deepseek-flash", replay_reasoning_content=False)
        child = base.with_override(PhaseLLMOverride(temperature=0.3))
        assert child.replay_reasoning_content is False

    def test_a_summarization_model_of_another_family_is_not_armed(
        self, tmp_path, monkeypatch
    ):
        """The factory asks the family of the model the client is built for."""
        _capture(monkeypatch, _reply)
        main = _deepseek_config(tmp_path).llm
        assert main.replay_reasoning_content is True
        other = main.with_override(
            PhaseLLMOverride(model="gpt-oss-120b", base_url="https://o.invalid/v1")
        )
        llm = loader.create_llm(other)
        try:
            assert llm.replay_reasoning_content is False
        finally:
            llm.http_client.close()


# ---------------------------------------------------------------------------
# Token counts
# ---------------------------------------------------------------------------


class TestTokenCounts:
    REASONING = "considering the note and what to do next " * 200

    def test_request_body_count_includes_reasoning(self):
        message = {"role": "assistant", "content": "ok"}
        without = count_request_tokens({"messages": [message]})
        with_reasoning = count_request_tokens(
            {"messages": [{**message, "reasoning_content": self.REASONING}]}
        )
        assert with_reasoning - without > 1000
        # An empty field costs nothing.
        assert (
            count_request_tokens({"messages": [{**message, "reasoning_content": ""}]})
            == without
        )

    def test_message_counters_count_it_only_when_asked(self):
        messages = [HumanMessage(content="hi"), _tool_turn(self.REASONING)]
        plain = count_tokens_tiktoken(messages)
        assert count_tokens_tiktoken(messages, count_reasoning=True) - plain > 1000
        approx = count_tokens_approximate(messages)
        assert (
            count_tokens_approximate(messages, count_reasoning=True) - approx
            == len(self.REASONING) // 4
        )

    def test_the_family_counter_counts_it(self):
        messages = [HumanMessage(content="hi"), _tool_turn(self.REASONING)]
        deepseek = get_token_counter("deepseek-flash")(messages)
        other = get_token_counter("gpt-oss-120b")(messages)
        assert deepseek - other > 1000


# ---------------------------------------------------------------------------
# One capture per call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_calls_keep_their_own_reasoning(monkeypatch):
    """One client, two calls in flight: neither gets the other's reasoning.

    The slow call's response arrives last, so a single slot on the client
    would give the fast call nothing or the slow call's text.
    """
    gate = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        tag = body["messages"][-1]["content"]
        if tag == "slow":
            await gate.wait()
        return httpx.Response(
            200,
            json=_completion(
                body["model"],
                {
                    "role": "assistant",
                    "content": f"answer {tag}",
                    "reasoning_content": f"reasoning {tag}",
                },
            ),
        )

    original = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(handler)),
    )
    llm = ReasoningChatOpenAI(
        model="fixture", api_key="k", base_url=DEEPSEEK_URL, max_retries=0
    )
    try:
        slow = asyncio.create_task(llm.ainvoke([HumanMessage(content="slow")]))
        await asyncio.sleep(0)
        fast = await llm.ainvoke([HumanMessage(content="fast")])
        gate.set()
        slow_result = await slow

        assert fast.content == "answer fast"
        assert fast.additional_kwargs["reasoning_content"] == "reasoning fast"
        assert slow_result.additional_kwargs["reasoning_content"] == "reasoning slow"
        # Nothing was left behind on the shared client.
        assert llm._async_reasoning_client._last_reasoning_content is None
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()


@pytest.mark.asyncio
async def test_a_response_without_reasoning_gets_none(monkeypatch):
    """A call never inherits reasoning an earlier call left on the client."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json=_completion(body["model"], {"role": "assistant", "content": "plain"}),
        )

    original = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(handler)),
    )
    llm = ReasoningChatOpenAI(
        model="fixture", api_key="k", base_url=DEEPSEEK_URL, max_retries=0
    )
    try:
        llm._async_reasoning_client._last_reasoning_content = "stale"
        result = await llm.ainvoke([HumanMessage(content="hi")])
        assert "reasoning_content" not in result.additional_kwargs
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()


@pytest.mark.asyncio
async def test_streamed_reasoning_reaches_the_merged_message(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        def chunk(delta: Dict[str, Any], finish=None) -> bytes:
            data = {
                "id": "s",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "fixture",
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            return f"data: {json.dumps(data)}\n\n".encode()

        stream = (
            chunk({"role": "assistant", "reasoning_content": "think "})
            + chunk({"reasoning_content": "more"})
            + chunk({"content": "Answer."})
            + chunk({}, "stop")
            + b"data: [DONE]\n\n"
        )
        return httpx.Response(
            200, content=stream, headers={"content-type": "text/event-stream"}
        )

    original = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(handler)),
    )
    llm = ReasoningChatOpenAI(
        model="fixture", api_key="k", base_url=DEEPSEEK_URL, max_retries=0
    )
    try:
        merged = None
        async for part in llm.astream([HumanMessage(content="hi")]):
            merged = part if merged is None else merged + part
        assert merged.content == "Answer."
        assert merged.additional_kwargs["reasoning_content"] == "think more"
        # The call's own capture held the tap; the client's slot stayed empty.
        assert llm._async_reasoning_client._active_stream_tap is None
    finally:
        llm.http_client.close()
        await llm.http_async_client.aclose()
