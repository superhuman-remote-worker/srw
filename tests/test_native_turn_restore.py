"""A restored assistant turn goes on the wire as the live one did.

A session rebuilds its history from ``thread_messages`` rows on every
stateless turn and after a pod restart. ``content`` holds the readable text,
so the provider-native turn rides ``provider_raw``: without it the next
request diverged from the previous one at the first assistant turn (a prompt
cache miss from there on) and the model lost its earlier reasoning:

- OpenAI Responses: the reasoning items, the message and function-call item
  ids, annotations and the message phase;
- Gemini: the thought parts and the thought signatures (on a thought part, a
  text part, and a function call, which langchain-google-genai keeps in
  ``additional_kwargs``);
- Anthropic: thinking, redacted thinking, server-tool blocks, citations.

Chat Completions families carry reasoning in
``additional_kwargs.reasoning_content``, which the live next request does not
send either, so their plain row restores byte-identically.

Each live turn here is what the provider SDK parses from a wire response.
It goes through the real serializer, a jsonb round trip (object keys come
back shortest first), the session restore or the child-transcript restore,
and the provider boundary sanitizer the restore applies; the SDK's own
request builder must then produce the same history, key order included. The
streamed variants of these paths run end to end in
``tests/test_session_restore_round_trip.py``.
"""

from __future__ import annotations

import base64
import dataclasses
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.core.context import sanitize_history_for_provider_boundary
from agent.core.thread_messages import (
    RESPONSES_ITEM_RETENTION,
    _serialize_message_row,
)
from agent.subagents.persistence import restore_subagent_messages
from shared.runtime.core import loader
from tests.test_session_restore_byte_exact import _stored

PROXY_URL = "http://srw-codex-proxy:8317/v1"
SYSTEM = "You are the SRW session assistant under test."


def _config(model: str, provider: str, **fields: Any) -> loader.LLMConfig:
    return dataclasses.replace(
        loader.LLMConfig(),
        model=model,
        provider=provider,
        api_key="fixture-key",
        max_retries=0,
        **fields,
    )


def _history(turn: AIMessage, results: Dict[str, str]) -> List[BaseMessage]:
    """The input, the turn, its tool results, and the next input."""
    turn.id = "ai-1"
    tools = [
        ToolMessage(content=output, tool_call_id=call_id, id=f"tool-{index}")
        for index, (call_id, output) in enumerate(results.items())
    ]
    return [
        HumanMessage(content="Go.", id="human-1"),
        turn,
        *tools,
        HumanMessage(content="And now?", id="human-2"),
    ]


def _rows(history: List[BaseMessage]) -> List[Dict[str, Any]]:
    """Serialized rows as the history reader hands them back (jsonb round trip)."""
    return _stored(
        json.loads(json.dumps([_serialize_message_row(m, 1) for m in history]))
    )


def _session_restore(rows: List[Dict[str, Any]]) -> List[BaseMessage]:
    return _db_rows_to_lc_messages(rows)


def _child_restore(rows: List[Dict[str, Any]]) -> List[BaseMessage]:
    return restore_subagent_messages(rows)


RESTORES = pytest.mark.parametrize(
    "restore", [_session_restore, _child_restore], ids=["session", "child"]
)


def _restored(
    history: List[BaseMessage],
    restore: Callable[[List[Dict[str, Any]]], List[BaseMessage]],
    model: str,
) -> List[BaseMessage]:
    return sanitize_history_for_provider_boundary(restore(_rows(history)), model)


def _turn(messages: List[BaseMessage]) -> AIMessage:
    return next(m for m in messages if isinstance(m, AIMessage))


# ---------------------------------------------------------------------------
# OpenAI Responses (gpt-6.1-sol on any endpoint, the codex lane)
# ---------------------------------------------------------------------------


def _responses_turn(output: List[dict], model: str) -> AIMessage:
    """The turn langchain-openai parses from a Responses API response.

    The session streams, so ReasoningChatOpenAI's non-streaming flattening
    never touches it: the reasoning and call items stay in ``content``.
    """
    from langchain_openai.chat_models.base import (
        _construct_lc_result_from_responses_api,
    )
    from openai.types.responses import Response

    response = Response.model_validate(
        {
            "id": "resp_1",
            "object": "response",
            "created_at": 0,
            "status": "completed",
            "model": model,
            "output": output,
            "error": None,
            "incomplete_details": None,
            "instructions": None,
            "metadata": {},
            "parallel_tool_calls": True,
            "temperature": None,
            "tool_choice": "auto",
            "tools": [],
            "top_p": None,
        }
    )
    return _construct_lc_result_from_responses_api(response).generations[0].message


_REASONING = {
    "type": "reasoning",
    "id": "rs_1",
    "summary": [{"type": "summary_text", "text": "Read both files first."}],
}


def _call(index: int) -> dict:
    return {
        "type": "function_call",
        "id": f"fc_{index}",
        "call_id": f"call_{index}",
        "name": "read_file",
        "arguments": json.dumps({"path": f"{index}.md"}),
        "status": "completed",
    }


def _message(text: str, **fields: Any) -> dict:
    return {
        "type": "message",
        "id": "msg_1",
        "role": "assistant",
        "status": "completed",
        "content": [
            {"type": "output_text", "text": text, "annotations": fields.pop("ann", [])}
        ],
        **fields,
    }


RESPONSES_TURNS = {
    "reasoning-and-parallel-calls": (
        [_REASONING, _call(1), _call(2)],
        {"call_1": "one", "call_2": "two"},
    ),
    "reasoning-commentary-and-call": (
        [_REASONING, _message("Reading it now.", phase="commentary"), _call(1)],
        {"call_1": "one"},
    ),
    "cited-answer": (
        [
            {**_REASONING, "encrypted_content": "gAAAA-opaque"},
            _message(
                "The release is on Friday.",
                phase="final_answer",
                ann=[
                    {
                        "type": "url_citation",
                        "url": "https://docs.example.invalid/release",
                        "title": "Release",
                        "start_index": 0,
                        "end_index": 11,
                    }
                ],
            ),
        ],
        {},
    ),
    "text-only": ([_message("Done.")], {}),
}


class TestResponses:
    @RESTORES
    @pytest.mark.parametrize("case", sorted(RESPONSES_TURNS))
    @pytest.mark.parametrize(
        "config",
        [
            _config("gpt-6.1-sol", "openai", reasoning_level="high"),
            _config(
                "codex/gpt-5.6-sol", "codex", base_url=PROXY_URL, reasoning_level="high"
            ),
        ],
        ids=["openai", "codex"],
    )
    def test_the_restored_input_is_the_live_input(self, config, case, restore):
        llm = loader.create_llm(config)
        output, results = RESPONSES_TURNS[case]
        live = _history(_responses_turn(output, config.model), results)

        restored = _restored(live, restore, config.model)

        def wire(history):
            return llm._get_request_payload([SystemMessage(SYSTEM), *history])["input"]

        assert json.dumps(wire(restored)) == json.dumps(wire(live))
        # Non-vacuous: the items a flattened row lost are on the wire.
        sent = json.dumps(wire(live))
        assert '"id": "msg_1"' in sent or '"id": "fc_1"' in sent
        if case != "text-only":
            assert '"type": "reasoning"' in sent

    @RESTORES
    @pytest.mark.parametrize(
        "output, kept",
        [
            ([_REASONING, _call(1), _call(2)], ["reasoning", "function_call"]),
            (
                [_REASONING, _message("Reading."), _call(1), _call(2)],
                ["reasoning", "message", "function_call"],
            ),
        ],
        ids=["calls", "commentary-and-calls"],
    )
    def test_a_call_without_its_result_leaves_with_its_item(
        self, output, kept, restore
    ):
        # The pod died before the second call's result was stored. The
        # restore strips that call; its function_call item must go too (the
        # Responses API rejects an item without its output), and the
        # reasoning stays with the call it led to.
        llm = loader.create_llm(_config("gpt-6.1-sol", "openai"))
        live = _history(_responses_turn(output, "gpt-6.1-sol"), {"call_1": "one"})

        restored = _restored(live, restore, "gpt-6.1-sol")

        sent = llm._get_request_payload(restored)["input"]
        calls = [i["call_id"] for i in sent if i.get("type") == "function_call"]
        outputs = [
            i["call_id"] for i in sent if i.get("type") == "function_call_output"
        ]
        assert calls == outputs == ["call_1"]
        assert [i["type"] for i in sent[1 : 1 + len(kept)]] == kept

    def test_the_row_keeps_the_text_and_the_reasoning_readable(self):
        output, _ = RESPONSES_TURNS["reasoning-commentary-and-call"]
        row = _serialize_message_row(_responses_turn(output, "gpt-6.1-sol"), 1)

        assert row["content"] == "Reading it now."
        assert row["thinking"] == "Read both files first."
        assert row["provider_raw"]["provider"] == "openai"
        assert row["provider_raw"]["model"] == "gpt-6.1-sol"


# ---------------------------------------------------------------------------
# Gemini (langchain-google-genai)
# ---------------------------------------------------------------------------

_SIGNATURE = base64.b64encode(b"thought-signature").decode()


def _gemini_turn(parts: List[dict], model: str) -> AIMessage:
    """The turn langchain-google-genai parses from a generateContent response."""
    from google.genai.types import GenerateContentResponse
    from langchain_google_genai.chat_models import _response_to_result

    response = GenerateContentResponse.model_validate(
        {
            "candidates": [
                {
                    "content": {"role": "model", "parts": parts},
                    "finishReason": "STOP",
                    "index": 0,
                }
            ],
            "modelVersion": model,
            "responseId": "resp-1",
        }
    )
    return _response_to_result(response).generations[0].message


def _function_call(path: str, *, signed: bool) -> dict:
    part: Dict[str, Any] = {
        "functionCall": {"name": "read_file", "args": {"path": path}}
    }
    if signed:
        part["thoughtSignature"] = _SIGNATURE
    return part


GEMINI_TURNS = {
    # Gemini 3: the signature rides the first function call of the step.
    "thought-and-parallel-calls": (
        "gemini-3.5-flash",
        [
            {
                "text": "Read both files.",
                "thought": True,
                "thoughtSignature": _SIGNATURE,
            },
            _function_call("a.md", signed=True),
            _function_call("b.md", signed=False),
        ],
    ),
    "thought-and-signed-text": (
        "gemini-3.5-flash",
        [
            {"text": "Answer directly.", "thought": True},
            {"text": "Both files list the release.", "thoughtSignature": _SIGNATURE},
        ],
    ),
    # Gemini 2.5 without a thought part: the content is a string and the
    # signature lives in additional_kwargs alone.
    "signed-call-without-thought": (
        "gemini-2.5-pro",
        [_function_call("a.md", signed=True)],
    ),
}


class TestGemini:
    @RESTORES
    @pytest.mark.parametrize("case", sorted(GEMINI_TURNS))
    def test_the_restored_contents_are_the_live_contents(self, case, restore):
        model, parts = GEMINI_TURNS[case]
        llm = loader.create_llm(_config(model, "google"))
        turn = _gemini_turn(parts, model)
        live = _history(turn, {call["id"]: "file text" for call in turn.tool_calls})

        restored = _restored(live, restore, model)

        def wire(history):
            request = llm._prepare_request([SystemMessage(SYSTEM), *history])
            return [
                content.model_dump(mode="json", exclude_none=True)
                for content in request["contents"]
            ]

        assert json.dumps(wire(restored)) == json.dumps(wire(live))
        model_turn = json.dumps(wire(live)[1])
        assert '"thought_signature"' in model_turn


# ---------------------------------------------------------------------------
# Anthropic (native Messages API, directly or behind the subscription proxy)
# ---------------------------------------------------------------------------


def _anthropic_turn(content: List[dict]) -> AIMessage:
    """The turn langchain-anthropic parses from a Messages API response."""
    from anthropic.types.beta import BetaMessage
    from langchain_anthropic import ChatAnthropic

    llm = ChatAnthropic(model="claude-opus-5-5", api_key="fixture-key")
    message = BetaMessage.model_validate(
        {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-5-5",
            "content": content,
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
    )
    turn = llm._format_output(message).generations[0].message
    # langchain-core merges the response's model into the metadata.
    turn.response_metadata["model_name"] = "claude-opus-5-5"
    return turn


_THINKING = {"type": "thinking", "thinking": "Plan the reads.", "signature": "sig-1"}
_TOOL_USE = {
    "type": "tool_use",
    "id": "toolu_1",
    "name": "read_file",
    "input": {"path": "a.md"},
}
_CITED_TEXT = {
    "type": "text",
    "text": "SRW runs agents.",
    "citations": [
        {
            "type": "web_search_result_location",
            "url": "https://example.invalid/srw",
            "title": "SRW",
            "encrypted_index": "idx",
            "cited_text": "runs agents",
        }
    ],
}

ANTHROPIC_TURNS = {
    "thinking-and-call-without-text": [_THINKING, _TOOL_USE],
    "redacted-thinking-cited-text-and-call": [
        _THINKING,
        {"type": "redacted_thinking", "data": "opaque"},
        _CITED_TEXT,
        _TOOL_USE,
    ],
    "server-tool-search": [
        _THINKING,
        {
            "type": "server_tool_use",
            "id": "srvtoolu_1",
            "name": "web_search",
            "input": {"query": "srw agents"},
        },
        {
            "type": "web_search_tool_result",
            "tool_use_id": "srvtoolu_1",
            "content": [
                {
                    "type": "web_search_result",
                    "url": "https://example.invalid/srw",
                    "title": "SRW",
                    "encrypted_content": "opaque",
                    "page_age": None,
                }
            ],
        },
        _CITED_TEXT,
    ],
    "text-only-list": [_THINKING, {"type": "text", "text": "Done."}],
}


class TestAnthropic:
    @RESTORES
    @pytest.mark.parametrize("case", sorted(ANTHROPIC_TURNS))
    def test_the_restored_messages_are_the_live_messages(self, case, restore):
        llm = loader.create_llm(
            _config("claude-opus-5-5", "anthropic", reasoning_level="high")
        )
        turn = _anthropic_turn(ANTHROPIC_TURNS[case])
        live = _history(turn, {call["id"]: "file text" for call in turn.tool_calls})

        restored = _restored(live, restore, "claude-opus-5-5")

        def wire(history):
            payload = llm._get_request_payload([SystemMessage(SYSTEM), *history])
            return payload["messages"]

        assert json.dumps(wire(restored)) == json.dumps(wire(live))
        assert wire(live)[1]["content"][0]["type"] == "thinking"


# ---------------------------------------------------------------------------
# Chat Completions families with a reasoning_content channel
# ---------------------------------------------------------------------------


class TestChatCompletionsReasoning:
    @RESTORES
    @pytest.mark.parametrize(
        "config",
        [
            _config("deepseek/deepseek-v3.2", "openrouter", reasoning_level="high"),
            _config(
                "MiniMax-M2",
                "openai",
                base_url="http://vllm-fixture.invalid/v1",
                extra_body={"reasoning_split": True},
            ),
        ],
        ids=["deepseek-openrouter", "minimax-vllm"],
    )
    def test_the_live_request_does_not_replay_reasoning_and_neither_does_a_restore(
        self, config, restore
    ):
        llm = loader.create_llm(config)
        turn = AIMessage(
            content="",
            tool_calls=[{"name": "read_file", "args": {"path": "a.md"}, "id": "c1"}],
            additional_kwargs={"reasoning_content": "Open a.md first."},
            response_metadata={"model_name": config.model, "model_provider": "openai"},
        )
        live = _history(turn, {"c1": "file text"})
        rows = _rows(live)

        restored = _restored(live, restore, config.model)

        def wire(history):
            payload = llm._get_request_payload([SystemMessage(SYSTEM), *history])
            return payload["messages"]

        assert json.dumps(wire(restored)) == json.dumps(wire(live))
        assert "Open a.md first." not in json.dumps(wire(live))
        ai_row = rows[1]
        assert ai_row["thinking"] == "Open a.md first."
        assert "provider_raw" not in ai_row


# ---------------------------------------------------------------------------
# Another family: the boundary still strips the foreign reasoning
# ---------------------------------------------------------------------------


class TestForeignFamily:
    @pytest.mark.parametrize(
        "turn, foreign",
        [
            (
                lambda: _responses_turn(
                    RESPONSES_TURNS["reasoning-and-parallel-calls"][0], "gpt-6.1-sol"
                ),
                "claude-opus-5-5",
            ),
            (
                lambda: _gemini_turn(
                    GEMINI_TURNS["thought-and-parallel-calls"][1], "gemini-3.5-flash"
                ),
                "gpt-6.1-sol",
            ),
            (
                lambda: _anthropic_turn(
                    ANTHROPIC_TURNS["thinking-and-call-without-text"]
                ),
                "gemini-3.5-flash",
            ),
        ],
        ids=["responses-to-claude", "gemini-to-gpt", "claude-to-gemini"],
    )
    def test_a_restored_turn_loses_its_reasoning_to_another_family(self, turn, foreign):
        live = turn()
        history = _history(live, {call["id"]: "x" for call in live.tool_calls})

        restored = _turn(_restored(history, _session_restore, foreign))

        assert isinstance(restored.content, (str, list))
        if isinstance(restored.content, list):
            assert not any(
                isinstance(b, dict)
                and b.get("type") in ("thinking", "redacted_thinking", "reasoning")
                for b in restored.content
            )
        assert [c["id"] for c in restored.tool_calls] == [
            c["id"] for c in live.tool_calls
        ]


# ---------------------------------------------------------------------------
# A Responses turn past OpenAI's retention of its items
# ---------------------------------------------------------------------------

YOUNG = RESPONSES_ITEM_RETENTION - timedelta(days=1)
OLD = RESPONSES_ITEM_RETENTION + timedelta(days=1)


def _written(rows: List[Dict[str, Any]], age: timedelta) -> List[Dict[str, Any]]:
    """The rows with the ``created_at`` the history reader hands back."""
    created_at = datetime.now(timezone.utc) - age
    return [{**row, "created_at": created_at} for row in rows]


class TestResponsesItemRetention:
    """A Responses turn's item ids point at a response OpenAI stores for 30
    days; replayed after that, every request fails ("Item with id ... not
    found"). A restore keeps the turn's blocks only while its row is younger
    than ``RESPONSES_ITEM_RETENTION`` and otherwise rebuilds it from the row's
    text and calls, as every restore did before the blocks were kept."""

    MODEL = "gpt-6.1-sol"

    def _live(self) -> List[BaseMessage]:
        output, results = RESPONSES_TURNS["reasoning-commentary-and-call"]
        return _history(_responses_turn(output, self.MODEL), results)

    def _input(self, history: List[BaseMessage]) -> List[Any]:
        llm = loader.create_llm(_config(self.MODEL, "openai", reasoning_level="high"))
        return llm._get_request_payload([SystemMessage(SYSTEM), *history])["input"]

    def _restore(self, rows, restore) -> List[BaseMessage]:
        return sanitize_history_for_provider_boundary(restore(rows), self.MODEL)

    @RESTORES
    def test_a_young_row_restores_natively(self, restore):
        live = self._live()

        restored = self._restore(_written(_rows(live), YOUNG), restore)

        assert json.dumps(self._input(restored)) == json.dumps(self._input(live))

    @RESTORES
    def test_an_old_row_restores_flat_and_its_request_is_valid(self, restore):
        live = self._live()
        rows = _rows(live)

        restored = self._restore(_written(rows, OLD), restore)

        # Exactly the restore of rows without the envelope.
        flat = [{k: v for k, v in row.items() if k != "provider_raw"} for row in rows]
        sent = self._input(restored)
        assert json.dumps(sent) == json.dumps(self._input(self._restore(flat, restore)))
        turn = _turn(restored)
        assert (turn.content, turn.response_metadata) == ("Reading it now.", {})
        # No item points at a stored response, and every call has its output.
        assert [i for i in sent if i.get("id") or i.get("type") == "reasoning"] == []
        calls = [i["call_id"] for i in sent if i.get("type") == "function_call"]
        outputs = [
            i["call_id"] for i in sent if i.get("type") == "function_call_output"
        ]
        assert calls == outputs == ["call_1"]

    @RESTORES
    @pytest.mark.parametrize(
        "turn, model",
        [
            (
                lambda: _anthropic_turn(
                    ANTHROPIC_TURNS["thinking-and-call-without-text"]
                ),
                "claude-opus-5-5",
            ),
            (
                lambda: _gemini_turn(
                    GEMINI_TURNS["thought-and-parallel-calls"][1], "gemini-3.5-flash"
                ),
                "gemini-3.5-flash",
            ),
        ],
        ids=["claude", "gemini"],
    )
    def test_an_old_claude_or_gemini_row_still_restores_natively(
        self, turn, model, restore
    ):
        live = turn()
        history = _history(live, {call["id"]: "x" for call in live.tool_calls})

        (restored,) = [
            m
            for m in restore(_written(_rows(history), OLD))
            if isinstance(m, AIMessage)
        ]

        assert json.dumps(restored.content) == json.dumps(live.content)
        assert restored.response_metadata["model_name"] == model

    @RESTORES
    @pytest.mark.parametrize("created_at", ["missing", None], ids=["missing", "null"])
    def test_a_row_without_its_time_counts_as_young(self, created_at, restore):
        live = self._live()
        rows = _rows(live)
        if created_at != "missing":
            rows = [{**row, "created_at": created_at} for row in rows]

        restored = self._restore(rows, restore)

        assert json.dumps(self._input(restored)) == json.dumps(self._input(live))
