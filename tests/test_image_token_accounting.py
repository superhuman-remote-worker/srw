"""Tests for image-aware token accounting (context_token_accounting.md S1).

Covers:
- src.core.image_tokens.split_text_and_images / estimate_image_tokens
- ContextManager.get_token_count no longer tokenizes base64 image blocks as
  text (the dev-session 5dbb5770 phantom: 12 images → ~9.2M "tokens")
- ContextManager.record_provider_usage anchors the compaction trigger
"""

import pytest
from langchain_core.messages import HumanMessage

from agent.core.context import ContextConfig, ContextManager
from shared.runtime.core.image_tokens import (
    DEFAULT_IMAGE_TOKENS,
    estimate_image_tokens,
    split_text_and_images,
)


# =============================================================================
# split_text_and_images / estimate_image_tokens
# =============================================================================


class TestSplitTextAndImages:
    def test_plain_string(self):
        assert split_text_and_images("hello world") == ("hello world", 0)

    def test_non_list_non_str_stringifies(self):
        assert split_text_and_images(None) == ("None", 0)

    def test_openai_image_url_block(self):
        content = [
            {"type": "text", "text": "look at this"},
            {
                "type": "image_url",
                "image_url": {"url": "data:image/png;base64," + "A" * 100_000},
            },
        ]
        text, n_images = split_text_and_images(content)
        assert text == "look at this"
        assert n_images == 1

    def test_anthropic_image_block(self):
        content = [
            {"type": "text", "text": "hi"},
            {"type": "image", "source": {"type": "base64", "data": "A" * 100_000}},
        ]
        text, n_images = split_text_and_images(content)
        assert text == "hi"
        assert n_images == 1

    def test_responses_api_input_image(self):
        content = [{"type": "input_image", "image_url": "data:image/png;base64,AAAA"}]
        _, n_images = split_text_and_images(content)
        assert n_images == 1

    def test_multiple_images(self):
        content = [
            {"type": "image_url", "image_url": {"url": "x" * 1000}},
            {"type": "image_url", "image_url": {"url": "y" * 1000}},
        ]
        _, n_images = split_text_and_images(content)
        assert n_images == 2

    def test_string_items_in_list(self):
        text, n_images = split_text_and_images(["a", "b"])
        assert text == "a\nb"
        assert n_images == 0

    def test_estimate(self):
        assert estimate_image_tokens(0) == 0
        assert estimate_image_tokens(-3) == 0
        assert estimate_image_tokens(3) == 3 * DEFAULT_IMAGE_TOKENS


# =============================================================================
# ContextManager counting + trigger
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


@pytest.fixture
def mgr(config):
    return ContextManager(config=config)


def _image_message(b64_chars: int) -> HumanMessage:
    return HumanMessage(
        content=[
            {"type": "text", "text": "Image content from tool call"},
            {
                "type": "image_url",
                "image_url": {"url": "data:image/png;base64," + "Z" * b64_chars},
            },
        ]
    )


class TestImageAwareCounting:
    def test_single_image_not_counted_as_base64(self, mgr):
        """A 500k-char base64 block must count as flat image tokens, not text."""
        count = mgr.get_token_count([_image_message(500_000)])
        # Honest: a few text tokens + one flat image + per-msg overhead.
        assert count < 3_000
        assert count >= DEFAULT_IMAGE_TOKENS

    def test_image_heavy_context_is_honest(self, mgr):
        """The 5dbb5770 shape: 12 image messages, ~4.8M chars of base64 total.

        Old counting tokenized the base64 as text (~millions of phantom
        tokens). New counting is ~12×1600 + text ≈ tens of thousands.
        """
        msgs = [_image_message(400_000) for _ in range(12)]
        count = mgr.get_token_count(msgs)
        assert count < 50_000  # not the ~9.2M / ~1.2M phantom
        assert count >= 12 * DEFAULT_IMAGE_TOKENS  # images are still accounted for


class TestProviderUsageAnchor:
    def test_anchor_floors_the_trigger(self, mgr):
        """A tiny local context still trips summarization once the real provider
        input_tokens exceeds the threshold (server-side context is larger than
        the bare message list)."""
        msgs = [HumanMessage(content="hi")]
        assert mgr.should_summarize(msgs) is False

        mgr.record_provider_usage(900)  # > summarization_threshold_tokens (800)
        assert mgr.should_summarize(msgs) is True
        assert mgr.state.last_provider_input_tokens == 900

    def test_none_and_zero_ignored(self, mgr):
        mgr.record_provider_usage(900)
        mgr.record_provider_usage(None)
        mgr.record_provider_usage(0)
        mgr.record_provider_usage(-5)
        assert mgr.state.last_provider_input_tokens == 900

    def test_local_count_wins_when_larger(self, mgr):
        """The trigger takes max(local, anchor): a large local context still
        triggers even if the last anchor was small."""
        mgr.record_provider_usage(10)
        big = [HumanMessage(content="word " * 1000)]  # ~1000+ tokens > 800
        assert mgr.should_summarize(big) is True
