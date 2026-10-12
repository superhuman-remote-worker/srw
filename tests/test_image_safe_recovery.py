"""Tests for image-safe context recovery (context_token_accounting.md S3).

Covers:
- src.core.image_tokens.content_to_summary_text / has_image_content — flatten
  multimodal content to a base64-free string and detect image messages
- ContextManager._format_messages_for_summary no longer leaks base64 into the
  summarizer input (the 5dbb5770 wedge: list content slipped past ``[:500]``)
- ContextManager._shed_image_messages / _elide_largest_tool_results /
  _emergency_truncate_tool_results shed image HumanMessages at compaction time,
  before tool results, while leaving plain user turns and tool pairing intact
- src.core.summarizer._describe_exc names arg-less exceptions (the ``failed ()``
  timeout log)
"""

import asyncio

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
)

from agent.core.context import ContextConfig, ContextManager
from shared.runtime.core.image_tokens import (
    DEFAULT_IMAGE_TOKENS,
    content_to_summary_text,
    has_image_content,
)
from agent.core.summarizer import _describe_exc

_B64 = "Z" * 400_000  # a base64 blob big enough to dominate if stringified


def _openai_image_block(mime: str = "image/png") -> dict:
    return {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{_B64}"}}


def _image_message(text: str = "Image content from tool call") -> HumanMessage:
    return HumanMessage(content=[{"type": "text", "text": text}, _openai_image_block()])


# =============================================================================
# content_to_summary_text / has_image_content
# =============================================================================


class TestContentToSummaryText:
    def test_plain_string_passthrough(self):
        assert content_to_summary_text("hello") == "hello"

    def test_non_list_stringifies(self):
        assert content_to_summary_text(None) == "None"

    def test_image_block_becomes_marker_not_base64(self):
        out = content_to_summary_text(
            [{"type": "text", "text": "look"}, _openai_image_block()]
        )
        assert "look" in out
        assert "[image: image/png" in out
        assert "Z" * 50 not in out  # base64 never stringified
        assert "base64" not in out

    def test_anthropic_source_media_type(self):
        out = content_to_summary_text(
            [
                {
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg"},
                }
            ]
        )
        assert "[image: image/jpeg" in out

    def test_responses_api_input_image_data_url(self):
        out = content_to_summary_text(
            [{"type": "input_image", "image_url": "data:image/webp;base64,AAAA"}]
        )
        assert "[image: image/webp" in out

    def test_non_data_url_falls_back_to_image(self):
        out = content_to_summary_text(
            [{"type": "image_url", "image_url": {"url": "https://x/y.png"}}]
        )
        assert "[image: image," in out

    def test_text_only_list_has_no_marker(self):
        out = content_to_summary_text([{"type": "text", "text": "just text"}])
        assert out == "just text"
        assert "[image:" not in out

    def test_marker_reports_default_estimate(self):
        out = content_to_summary_text([_openai_image_block()])
        assert f"~{DEFAULT_IMAGE_TOKENS} tok" in out


class TestHasImageContent:
    def test_string_is_false(self):
        assert has_image_content("hello") is False

    def test_text_only_list_is_false(self):
        assert has_image_content([{"type": "text", "text": "x"}]) is False

    def test_string_list_is_false(self):
        assert has_image_content(["a", "b"]) is False

    def test_image_list_is_true(self):
        assert has_image_content([_openai_image_block()]) is True


# =============================================================================
# ContextManager wiring
# =============================================================================


@pytest.fixture
def config():
    return ContextConfig(
        compaction_threshold_tokens=500,
        summarization_threshold_tokens=800,
        message_count_threshold=5,
        message_count_min_tokens=200,
        keep_recent_messages=3,
    )


def _char_counter(messages):
    """Deterministic counter: chars for text, flat per image block.

    Mirrors the post-S1 behavior (base64 never counted as text) but with exact,
    predictable numbers so the elision budgets in these tests are stable.
    """
    total = 0
    for m in messages:
        content = m.content
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") in (
                    "image_url",
                    "image",
                    "input_image",
                ):
                    total += DEFAULT_IMAGE_TOKENS
                elif isinstance(item, dict) and isinstance(item.get("text"), str):
                    total += len(item["text"])
                elif isinstance(item, str):
                    total += len(item)
        else:
            total += len(str(content))
    return total


@pytest.fixture
def mgr(config):
    m = ContextManager(config=config)
    m.token_counter = _char_counter  # deterministic budgets
    return m


class TestFormatMessagesForSummaryImageSafe:
    def test_image_human_message_does_not_leak_base64(self, mgr):
        msgs = [
            _image_message(),
            ToolMessage(content="tool said ok", tool_call_id="t1", name="read_file"),
            AIMessage(content="here is the answer"),
        ]
        parts = mgr._format_messages_for_summary(msgs)
        blob = "\n".join(parts)
        assert "Z" * 50 not in blob  # the 400k base64 is gone
        assert "base64" not in blob
        assert "[image:" in blob  # replaced with a marker
        assert "Image content from tool call" in blob  # text part survived

    def test_ai_list_content_is_flattened(self, mgr):
        msgs = [AIMessage(content=[{"type": "text", "text": "reasoning"}])]
        parts = mgr._format_messages_for_summary(msgs)
        assert any("reasoning" in p for p in parts)


class TestShedImageMessages:
    def test_replaces_image_human_message(self, mgr):
        msgs = [_image_message()]
        result, shed = mgr._shed_image_messages(msgs)
        assert shed == 1
        assert isinstance(result[0].content, str)
        assert result[0].content.startswith("[image content elided")

    def test_leaves_plain_human_and_tool_untouched(self, mgr):
        msgs = [
            HumanMessage(content="real user question"),
            ToolMessage(content="tool result", tool_call_id="t1"),
        ]
        result, shed = mgr._shed_image_messages(msgs)
        assert shed == 0
        assert result[0].content == "real user question"
        assert result[1].content == "tool result"

    def test_preserves_message_id(self, mgr):
        msg = _image_message()
        msg.id = "abc-123"
        result, _ = mgr._shed_image_messages([msg])
        assert result[0].id == "abc-123"


class TestElideShedsImagesFirst:
    def test_image_shed_alone_keeps_tool_result(self, mgr):
        # image(1600+28) + tiny tool fits under target once the image is gone.
        msgs = [
            HumanMessage(content="hi"),
            _image_message(),
            ToolMessage(content="ok", tool_call_id="t1", name="read_file"),
        ]
        result = mgr._elide_largest_tool_results(msgs, target_tokens=300)
        assert result[1].content.startswith("[image content elided")
        assert result[2].content == "ok"  # tool result preserved
        assert result[0].content == "hi"  # plain user turn preserved

    def test_image_and_tool_both_shed_when_needed(self, mgr):
        big_tool = ToolMessage(content="X" * 8000, tool_call_id="t2", name="read_file")
        msgs = [_image_message(), big_tool]
        result = mgr._elide_largest_tool_results(msgs, target_tokens=500)
        assert result[0].content.startswith("[image content elided")
        assert result[1].content.startswith("[tool result elided")
        assert result[1].tool_call_id == "t2"  # pairing preserved
        assert result[1].name == "read_file"

    def test_plain_human_never_elided(self, mgr):
        msgs = [
            HumanMessage(content="important instruction " * 500),
            ToolMessage(content="X" * 8000, tool_call_id="t3"),
        ]
        result = mgr._elide_largest_tool_results(msgs, target_tokens=100)
        # The user turn survives; only the tool result is elided.
        assert result[0].content.startswith("important instruction")
        assert result[1].content.startswith("[tool result elided")


class TestEmergencyTruncateShedsImages:
    def test_image_message_shed_in_emergency_path(self, mgr):
        msgs = [_image_message(), ToolMessage(content="X" * 8000, tool_call_id="t1")]
        result = mgr._emergency_truncate_tool_results(msgs)
        image_msgs = [m for m in result if isinstance(m, HumanMessage)]
        assert image_msgs[0].content.startswith("[image content elided")


# =============================================================================
# summarizer._describe_exc
# =============================================================================


class TestDescribeExc:
    def test_timeout_error_named_not_empty(self):
        # asyncio.TimeoutError stringifies to "" -> used to log as "failed ()".
        assert str(asyncio.TimeoutError()) == ""
        assert _describe_exc(asyncio.TimeoutError()) == "TimeoutError"

    def test_exception_with_message(self):
        assert _describe_exc(ValueError("boom")) == "ValueError: boom"

    def test_none_is_handled(self):
        assert _describe_exc(None) == "unknown error"
