"""Claude runs on the native Messages API, its thinking replayed between steps.

Through the subscription proxy's OpenAI-compatible endpoints every step lost the
previous reasoning and rebuilt it (an A/B replay of the same turn: 235.6k output
tokens and no mockup, against 56.0k and a mockup with thinking replayed). See
knowledge-base/knowledge/issues/website_demo_findings_2026_10_11.md, F1.
"""

import dataclasses
import json

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.core.context import (
    repair_tool_pairing,
    sanitize_history_for_provider_boundary,
    strip_unreplayable_blocks,
)
from agent.core.thread_messages import (
    ANTHROPIC_CONTENT_KEY,
    NATIVE_CONTENT_KEY,
    _serialize_message_row,
)
from shared.runtime.core import loader

PROXY_URL = "http://srw-codex-proxy:8317/v1"


def _config(model="claude-opus-5-5", **overrides) -> loader.LLMConfig:
    fields = dict(
        model=model,
        provider="codex",
        base_url=PROXY_URL,
        api_key="not-needed",
        reasoning_level="xhigh",
        temperature=0.0,
        top_p=0.9,
        max_retries=0,
    )
    fields.update(overrides)
    return dataclasses.replace(loader.LLMConfig(), **fields)


def _payload(llm: ChatAnthropic) -> dict:
    return llm._get_request_payload([HumanMessage("hi")])


def _sampling(payload: dict, name: str):
    """A sampling parameter wherever the beta client carries it."""
    return payload.get(name, (payload.get("extra_body") or {}).get(name))


class TestRouting:
    @pytest.mark.parametrize("provider", ["codex", "openai"])
    def test_claude_behind_the_proxy_runs_native(self, provider):
        llm = loader.create_llm(_config(provider=provider))

        assert isinstance(llm, ChatAnthropic)
        assert llm.anthropic_api_url == "http://srw-codex-proxy:8317"

    def test_the_kill_switch_restores_the_openai_route(self, monkeypatch):
        monkeypatch.setenv("SRW_CLAUDE_NATIVE_TRANSPORT", "0")

        assert not isinstance(loader.create_llm(_config()), ChatAnthropic)

    def test_other_models_behind_the_proxy_keep_their_route(self):
        assert not isinstance(
            loader.create_llm(_config(model="gpt-6.1-sol")), ChatAnthropic
        )

    def test_claude_on_another_openai_compatible_endpoint_keeps_its_route(self):
        llm = loader.create_llm(
            _config(provider="openai", base_url="https://claude-fixture.invalid/v1")
        )

        assert not isinstance(llm, ChatAnthropic)


class TestRequest:
    def test_opus_5_5_thinks_adaptively_and_replays_through_edits(self):
        llm = loader.create_llm(
            _config(
                prompt_cache_key="srw-thread-t1",
                extra_headers={"Anthropic-Beta": "oauth-2025-04-20"},
            )
        )
        payload = _payload(llm)

        assert payload["thinking"] == {
            "type": "adaptive",
            "display": "summarized",
            "block_binding": {"prefix_mismatch_behavior": "drop_block"},
        }
        assert payload["output_config"] == {"effort": "xhigh"}
        for sampling in ("temperature", "top_p", "top_k"):
            assert _sampling(payload, sampling) is None
        assert payload["betas"] == [
            "thinking-binding-controls-2026-08-01",
            "oauth-2025-04-20",
        ]
        assert llm.default_headers == {"X-Session-ID": "srw-thread-t1"}

    def test_opus_4_6_thinks_adaptively_without_display_or_binding(self):
        payload = _payload(loader.create_llm(_config(model="claude-opus-4-6")))

        assert payload["thinking"] == {"type": "adaptive"}
        assert payload["betas"] == ["context-management-2025-06-27"]

    def test_a_model_without_adaptive_thinking_keeps_its_sampling(self):
        payload = _payload(
            loader.create_llm(_config(model="claude-haiku-4-5-20251001"))
        )

        assert "thinking" not in payload
        assert _sampling(payload, "temperature") == 0.0
        assert _sampling(payload, "top_p") == 0.9


class TestReplayHygiene:
    def test_a_stripped_call_takes_its_tool_use_block_along(self):
        thinking = {"type": "thinking", "thinking": "t", "signature": "sig"}
        ai = AIMessage(
            content=[
                thinking,
                {"type": "text", "text": "Reading both."},
                {"type": "tool_use", "id": "a", "name": "read_file", "input": {}},
                {"type": "tool_use", "id": "b", "name": "read_file", "input": {}},
            ],
            tool_calls=[
                {"id": "a", "name": "read_file", "args": {}},
                {"id": "b", "name": "read_file", "args": {}},
            ],
        )
        repaired = repair_tool_pairing(
            [HumanMessage("go"), ai, ToolMessage(content="ok", tool_call_id="a")]
        )

        content = repaired[1].content
        assert [b.get("id") for b in content if b["type"] == "tool_use"] == ["a"]
        assert content[0] == thinking

    def test_thinking_alone_is_no_turn_to_replay(self):
        assert (
            strip_unreplayable_blocks(
                [
                    {"type": "thinking", "thinking": "t", "signature": "sig"},
                    {"type": "tool_use", "id": "x", "name": "n", "input": {}},
                ],
                set(),
            )
            == ""
        )

    def test_an_unsigned_partial_thinking_block_is_dropped(self):
        kept = strip_unreplayable_blocks(
            [
                {"type": "thinking", "thinking": "half"},
                {"type": "text", "text": "partial answer"},
            ],
            set(),
        )

        assert kept == [{"type": "text", "text": "partial answer"}]


class TestPersistence:
    @staticmethod
    def _claude_turn():
        return AIMessage(
            content=[
                {"type": "thinking", "thinking": "plan", "signature": "sig"},
                {"type": "tool_use", "id": "a", "name": "read_file", "input": {}},
            ],
            tool_calls=[{"id": "a", "name": "read_file", "args": {}}],
            response_metadata={
                "model_name": "claude-opus-5-5",
                "model_provider": "anthropic",
            },
        )

    def test_a_claude_turn_keeps_its_blocks_and_no_whitespace_text(self):
        row = _serialize_message_row(self._claude_turn(), 1)

        assert row["content"] == ""
        # As JSON text: jsonb would hand the tool_use block back "id" first.
        assert row["provider_raw"] == {
            NATIVE_CONTENT_KEY: json.dumps(self._claude_turn().content),
            "model": "claude-opus-5-5",
            "provider": "anthropic",
        }

    def test_a_plain_text_turn_keeps_no_envelope(self):
        message = AIMessage(content="hello")

        assert "provider_raw" not in _serialize_message_row(message, 1)

    def test_a_row_stored_under_the_anthropic_key_still_restores(self):
        from agent.api.persistent_app import _db_rows_to_lc_messages

        row = {
            **_serialize_message_row(self._claude_turn(), 1),
            "id": "ai-1",
            "provider_raw": {
                ANTHROPIC_CONTENT_KEY: self._claude_turn().content,
                "model": "claude-opus-5-5",
            },
        }
        (ai,) = _db_rows_to_lc_messages([row])

        assert ai.content == self._claude_turn().content
        assert ai.response_metadata == {"model_name": "claude-opus-5-5"}

    def test_restore_rebuilds_the_blocks_and_keeps_own_thinking(self):
        from agent.api.persistent_app import _db_rows_to_lc_messages

        row = _serialize_message_row(self._claude_turn(), 1)
        restored = _db_rows_to_lc_messages(
            [
                {"id": "h", "role": "human", "content": "go", "turn_number": 1},
                {**row, "id": "ai-1"},
                {
                    "id": "t",
                    "role": "tool",
                    "content": "ok",
                    "tool_call_id": "a",
                    "turn_number": 1,
                },
            ]
        )
        ai = restored[1]

        assert ai.content == self._claude_turn().content
        same = sanitize_history_for_provider_boundary(restored, "claude-opus-5-5")
        assert same[1].content[0]["type"] == "thinking"
        other = sanitize_history_for_provider_boundary(restored, "gpt-6.1-sol")
        assert all(b["type"] != "thinking" for b in other[1].content)
