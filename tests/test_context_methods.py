"""Tests for ContextManager methods not covered by test_context_safety.py.

Tests: set_current_phase, get_token_count, should_compact, should_summarize,
and sanitize_message_history.
"""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from shared.runtime.core.loader import (
    CONTEXT_THRESHOLD_FRACTION,
    MESSAGE_COUNT_MIN_FRACTION,
)
from agent.core.context import (
    ContextManager,
    ContextConfig,
    repair_tool_call_arguments,
    repair_tool_pairing,
    sanitize_message_history,
    scrub_history_tool_call_arguments,
)
from shared.runtime.core.message_markers import PROTECTED_KEY
from shared.runtime.core.workspace_injection import create_phase_instruction_message


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config():
    """Low thresholds for testing."""
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


# =============================================================================
# sanitize_message_history (module-level function)
# =============================================================================


class TestSanitizeMessageHistory:
    """Tests for orphaned ToolMessage removal."""

    def test_empty_list(self):
        assert sanitize_message_history([]) == []

    def test_no_orphans(self):
        """Messages with matching AIMessage tool_calls should be kept."""
        messages = [
            AIMessage(
                content="", tool_calls=[{"name": "read_file", "id": "tc1", "args": {}}]
            ),
            ToolMessage(content="file content", tool_call_id="tc1"),
        ]
        result = sanitize_message_history(messages)
        assert len(result) == 2

    def test_removes_orphaned_tool_message(self):
        """ToolMessage without matching AIMessage should be removed."""
        messages = [
            HumanMessage(content="hi"),
            ToolMessage(content="orphaned result", tool_call_id="no_parent"),
        ]
        result = sanitize_message_history(messages)
        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)

    def test_preserves_non_tool_messages(self):
        """Human, AI, System messages should always be kept."""
        messages = [
            SystemMessage(content="system"),
            HumanMessage(content="user"),
            AIMessage(content="assistant"),
        ]
        result = sanitize_message_history(messages)
        assert len(result) == 3

    def test_mixed_orphaned_and_valid(self):
        """Should only remove orphaned, keep valid."""
        messages = [
            AIMessage(
                content="", tool_calls=[{"name": "read", "id": "valid", "args": {}}]
            ),
            ToolMessage(content="ok", tool_call_id="valid"),
            ToolMessage(content="orphaned", tool_call_id="missing_parent"),
        ]
        result = sanitize_message_history(messages)
        assert len(result) == 2


# =============================================================================
# repair_tool_pairing (module-level function, shared by persistent loop + resume)
# =============================================================================


class TestRepairToolPairing:
    """Bidirectional tool-call pairing repair.

    Regression coverage for the persistent gpt-5.5 sessions that 400'd with
    "No tool call found for function call output" — an orphaned ToolMessage
    (result without its call) reaching the Responses API.
    """

    def test_empty_list(self):
        assert repair_tool_pairing([]) == []

    def test_valid_pair_preserved(self):
        messages = [
            AIMessage(
                content="", tool_calls=[{"name": "read", "id": "tc1", "args": {}}]
            ),
            ToolMessage(content="ok", tool_call_id="tc1"),
        ]
        result = repair_tool_pairing(messages)
        assert len(result) == 2

    def test_drops_orphaned_result(self):
        """Result without a matching call — the exact 400 case — is dropped."""
        messages = [
            HumanMessage(content="hi"),
            ToolMessage(content="orphaned", tool_call_id="call_d27X"),
        ]
        result = repair_tool_pairing(messages)
        assert len(result) == 1
        assert isinstance(result[0], HumanMessage)

    def test_strips_orphaned_call_drops_empty_message(self):
        """Call without a result and no text — the AIMessage carries nothing."""
        messages = [
            AIMessage(
                content="", tool_calls=[{"name": "read", "id": "tc1", "args": {}}]
            ),
        ]
        result = repair_tool_pairing(messages)
        assert result == []

    def test_strips_orphaned_call_keeps_text(self):
        """Call without a result but with text — keep the text, drop the call."""
        messages = [
            AIMessage(
                content="here you go",
                tool_calls=[{"name": "read", "id": "tc1", "args": {}}],
            ),
        ]
        result = repair_tool_pairing(messages)
        assert len(result) == 1
        assert result[0].content == "here you go"
        assert result[0].tool_calls == []

    def test_partial_parallel_batch(self):
        """Parallel batch where only one result survived: keep the matched
        call+result, strip the dangling call."""
        messages = [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "a", "id": "t1", "args": {}},
                    {"name": "b", "id": "t2", "args": {}},
                ],
            ),
            ToolMessage(content="r1", tool_call_id="t1"),
        ]
        result = repair_tool_pairing(messages)
        assert len(result) == 2
        assert [tc["id"] for tc in result[0].tool_calls] == ["t1"]
        assert result[1].tool_call_id == "t1"

    def test_preserves_plain_messages(self):
        messages = [
            SystemMessage(content="system"),
            HumanMessage(content="user"),
            AIMessage(content="assistant"),
        ]
        result = repair_tool_pairing(messages)
        assert len(result) == 3


# =============================================================================
# get_token_count, should_compact, should_summarize
# =============================================================================


class TestTokenCounting:
    """Tests for threshold-based decision methods."""

    def test_get_token_count_returns_int(self, mgr):
        messages = [HumanMessage(content="Hello world")]
        count = mgr.get_token_count(messages)
        assert isinstance(count, int)
        assert count > 0

    def test_get_token_count_updates_state(self, mgr):
        messages = [HumanMessage(content="test")]
        count = mgr.get_token_count(messages)
        assert mgr.state.current_token_count == count

    def test_should_compact_below_threshold(self, mgr):
        """Small messages should not trigger compaction."""
        messages = [HumanMessage(content="Hi")]
        assert mgr.should_compact(messages) is False

    def test_should_compact_above_threshold(self, mgr):
        """Large messages should trigger compaction."""
        # config threshold is 500 tokens (~2000 chars)
        messages = [HumanMessage(content="x" * 5000)]
        assert mgr.should_compact(messages) is True

    def test_should_summarize_below_threshold(self, mgr):
        """Small messages should not trigger summarization."""
        messages = [HumanMessage(content="Hi")]
        assert mgr.should_summarize(messages) is False

    def test_should_summarize_high_tokens(self, mgr):
        """Token count above summarization_threshold triggers summarization."""
        # threshold is 800 tokens (~3200 chars)
        messages = [HumanMessage(content="y" * 10000)]
        assert mgr.should_summarize(messages) is True

    def test_should_summarize_high_message_count(self, mgr):
        """Many messages with moderate tokens should trigger."""
        # threshold is 5 messages with 200 min tokens
        messages = [
            HumanMessage(content=f"Message {i} with some content to add tokens " * 10)
            for i in range(10)
        ]
        assert mgr.should_summarize(messages) is True


class TestMessageCountGateNeverBinds:
    """Regression: a 400k-window session compacted at 162k (40% of window).

    ``should_summarize`` ORs a token gate (``CONTEXT_THRESHOLD_FRACTION``,
    0.80 of the window) with a message-count gate floored at
    ``MESSAGE_COUNT_MIN_FRACTION``. While that floor was 0.40, every session
    past ``message_count_threshold`` compacted at 40% of the window — half the
    intended headroom, on a lossy summarize. The floor must never sit below the
    token gate, so the message-count branch can never be the binding
    constraint. See knowledge-history/done/ + session 1930dec9 (328 msgs, 162.0k/400.0k).
    """

    WINDOW = 400_000

    def _mgr(self) -> ContextManager:
        """A ContextManager wired the way the loader derives a 400k model."""
        return ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=int(
                    self.WINDOW * CONTEXT_THRESHOLD_FRACTION
                ),
                summarization_threshold_tokens=int(
                    self.WINDOW * CONTEXT_THRESHOLD_FRACTION
                ),
                # config/session_base.yaml + config/worker_base.yaml
                message_count_threshold=300,
                message_count_min_tokens=int(self.WINDOW * MESSAGE_COUNT_MIN_FRACTION),
                model_max_context_tokens=self.WINDOW,
            )
        )

    def test_floor_is_not_below_the_token_gate(self):
        """The derived floor must not undercut the token gate at any window."""
        assert MESSAGE_COUNT_MIN_FRACTION >= CONTEXT_THRESHOLD_FRACTION

    def test_long_session_below_token_gate_does_not_summarize(self):
        """328 messages at 162k on a 400k model: 238k of window still free."""
        mgr = self._mgr()
        messages = [HumanMessage(content=f"turn {i}") for i in range(328)]
        # The live trigger anchors on the provider's real input_tokens.
        mgr.record_provider_usage(162_000)
        assert mgr.should_summarize(messages) is False

    def test_token_gate_still_fires_on_a_long_session(self):
        """Neutering the message gate must not disarm the token gate."""
        mgr = self._mgr()
        messages = [HumanMessage(content=f"turn {i}") for i in range(328)]
        mgr.record_provider_usage(int(self.WINDOW * CONTEXT_THRESHOLD_FRACTION) + 1)
        assert mgr.should_summarize(messages) is True


# =============================================================================
# set_current_phase
# =============================================================================


class TestSetCurrentPhase:
    """Tests for phase-switching token counter."""

    def test_set_phase_switches_counter(self, config):
        """Setting phase should use phase-specific counter if available."""
        mgr = ContextManager(config=config)
        # Default counter should work
        count = mgr.get_token_count([HumanMessage(content="test")])
        assert count > 0

        # Setting a phase that doesn't have a special counter should
        # fall back to default
        mgr.set_current_phase("strategic")
        count2 = mgr.get_token_count([HumanMessage(content="test")])
        assert count2 > 0


class TestContextConfigDefaults:
    """ContextConfig defaults include keep-window tool result truncation."""

    def test_keep_window_max_tool_result_chars_default(self):
        assert ContextConfig().keep_window_max_tool_result_chars == 16000


# =============================================================================
# update_limits — in-place threshold rebind on model hot-swap
# =============================================================================


class TestUpdateLimits:
    """Model hot-swap rebinds thresholds on the EXISTING manager, preserving
    accumulated state (knowledge-base/knowledge/issues/
    session_model_switch_stale_context_manager_empty_response.md)."""

    def test_thresholds_swap_and_state_survives(self):
        mgr = ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=840_000,
                summarization_threshold_tokens=840_000,
                model_max_context_tokens=1_050_000,
            ),
            model="gpt-5.5",
        )
        # The repro: ~125.7k history anchored from real provider usage on the
        # big-window model — under its 840k threshold, so no compaction.
        mgr.record_provider_usage(125_700)
        mgr.compaction_runs = 3
        assert mgr.should_summarize([]) is False

        new_cfg = ContextConfig(
            compaction_threshold_tokens=102_400,
            summarization_threshold_tokens=102_400,
            model_max_context_tokens=128_000,
        )
        mgr.update_limits(new_cfg, "gpt-5.3-codex-spark")

        assert mgr.config is new_cfg
        # State survives the swap: the provider anchor makes the very next
        # should_summarize see the real context size against the new window.
        assert mgr._state.last_provider_input_tokens == 125_700
        assert mgr.compaction_runs == 3
        assert mgr.should_summarize([]) is True

    def test_counter_rebound_to_new_model(self):
        mgr = ContextManager(config=ContextConfig(), model="gpt-4")
        old_counter = mgr.token_counter
        mgr.update_limits(ContextConfig(), "gpt-5.3-codex-spark")
        assert mgr.token_counter is mgr._default_counter
        assert mgr.token_counter is not old_counter


# =============================================================================
# repair_tool_call_arguments / scrub_history_tool_call_arguments
# (knowledge-base/knowledge/features/outbound_message_hygiene.md — the 2026-07-11 `6a186c76`
#  poisoned-checkpoint incident)
# =============================================================================


def _poisoned_ai_message(
    raw_args: str, *, call_id: str = "call_bad1", name: str = "file_exists"
):
    """AIMessage shaped like incident A: malformed arguments in BOTH
    invalid_tool_calls (LangChain's parse failure) and the raw
    additional_kwargs entry that gets re-serialized to the provider."""
    return AIMessage(
        content="",
        invalid_tool_calls=[
            {
                "name": name,
                "args": raw_args,
                "id": call_id,
                "error": "Function call arguments were not valid JSON",
                "type": "invalid_tool_call",
            }
        ],
        additional_kwargs={
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": raw_args},
                    "index": 0,
                }
            ]
        },
    )


class TestRepairToolCallArguments:
    def test_wellformed_message_untouched(self):
        msg = AIMessage(
            content="hi",
            tool_calls=[
                {
                    "name": "read_file",
                    "args": {"path": "a.md"},
                    "id": "c1",
                    "type": "tool_call",
                }
            ],
            additional_kwargs={
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": '{"path": "a.md"}',
                        },
                    }
                ]
            },
        )
        out = repair_tool_call_arguments(msg)
        assert out.tool_calls[0]["args"] == {"path": "a.md"}
        assert out.content == "hi"
        assert not out.invalid_tool_calls

    def test_repairable_truncation_promoted_to_tool_call(self):
        # Mid-string truncation — the closer-unwinding repair path.
        msg = _poisoned_ai_message('{"path": "archive/phase_1_retro')
        out = repair_tool_call_arguments(msg)
        assert out.tool_calls and out.tool_calls[0]["name"] == "file_exists"
        assert out.tool_calls[0]["args"] == {"path": "archive/phase_1_retro"}
        assert not out.invalid_tool_calls
        # Raw entry rewritten to valid JSON — nothing malformed goes back out.
        import json as _json

        raw = out.additional_kwargs["tool_calls"][0]["function"]["arguments"]
        assert _json.loads(raw) == {"path": "archive/phase_1_retro"}

    def test_trailing_comma_repaired(self):
        msg = _poisoned_ai_message('{"path": "a.md",}')
        out = repair_tool_call_arguments(msg)
        assert out.tool_calls[0]["args"] == {"path": "a.md"}

    def test_unrepairable_dropped_everywhere_with_note(self):
        msg = _poisoned_ai_message("not json at all — no braces")
        out = repair_tool_call_arguments(msg)
        assert not out.tool_calls
        assert not out.invalid_tool_calls
        assert out.additional_kwargs["tool_calls"] == []
        assert "discarded" in out.content
        assert "file_exists" in out.content

    def test_raw_only_poison_without_invalid_list(self):
        # Checkpoint round-trips can lose invalid_tool_calls while the raw
        # kwargs entry survives — the sweep must still catch it.
        msg = AIMessage(
            content="thinking...",
            additional_kwargs={
                "tool_calls": [
                    {
                        "id": "call_raw1",
                        "type": "function",
                        "function": {
                            "name": "write_file",
                            "arguments": "###garbage###",
                        },
                    }
                ]
            },
        )
        out = repair_tool_call_arguments(msg)
        assert out.additional_kwargs["tool_calls"] == []
        assert "discarded" in out.content

    def test_good_call_kept_when_sibling_dropped(self):
        msg = _poisoned_ai_message("no braces here")
        msg.tool_calls = [
            {
                "name": "read_file",
                "args": {"path": "b.md"},
                "id": "c_good",
                "type": "tool_call",
            }
        ]
        msg.additional_kwargs["tool_calls"].append(
            {
                "id": "c_good",
                "type": "function",
                "function": {"name": "read_file", "arguments": '{"path": "b.md"}'},
            }
        )
        out = repair_tool_call_arguments(msg)
        assert [tc["id"] for tc in out.tool_calls] == ["c_good"]
        assert [e["id"] for e in out.additional_kwargs["tool_calls"]] == ["c_good"]

    def test_history_scrub_no_note_on_nonempty_content(self):
        poisoned = _poisoned_ai_message("no braces")
        poisoned.content = "some prior visible answer"
        history = [
            HumanMessage(content="q"),
            poisoned,
            AIMessage(content="later"),
        ]
        out = scrub_history_tool_call_arguments(history)
        assert out[1].additional_kwargs["tool_calls"] == []
        # note=False: historical content untouched when non-empty
        assert out[1].content == "some prior visible answer"

    def test_history_scrub_stubs_empty_message(self):
        history = [_poisoned_ai_message("no braces")]
        out = scrub_history_tool_call_arguments(history)
        # Now-empty assistant turn gets a stub so strict providers don't 400.
        assert out[0].content
        assert not out[0].tool_calls

    def test_non_ai_messages_untouched(self):
        history = [
            HumanMessage(content="q"),
            ToolMessage(content="r", tool_call_id="x"),
        ]
        out = scrub_history_tool_call_arguments(history)
        assert out[0].content == "q" and out[1].content == "r"


# =============================================================================
# Protected messages (phase instruction blocks) — U2 WP1
# =============================================================================


class TestProtectedMessages:
    """Protected messages survive elision and emergency truncation;
    summarisation re-seats them (see
    tests/test_context_safety.py::TestPinnedAfterSummary)."""

    @staticmethod
    def _block(phase_key="2:tactical"):
        return create_phase_instruction_message(
            "skills/tactical-phase/SKILL.md", "PHASE BODY", "tactical", phase_key
        )

    def test_set_current_phase_records_phase_key(self, mgr):
        assert mgr.current_phase_key is None
        mgr.set_current_phase("tactical", phase_key="2:tactical")
        assert mgr.current_phase_key == "2:tactical"
        # Sessions call it without a key: the key is cleared, never stale.
        mgr.set_current_phase("strategic")
        assert mgr.current_phase_key is None

    def test_elide_skips_protected(self, mgr):
        block = self._block()
        ai = AIMessage(
            content="",
            id="a1",
            tool_calls=[
                {"id": "t1", "name": "read", "args": {}},
                {"id": "tp", "name": "read", "args": {}},
            ],
        )
        big = ToolMessage(content="regulation " * 2000, tool_call_id="t1", id="t1")
        protected_big = ToolMessage(
            content="protected " * 3000,
            tool_call_id="tp",
            id="tp",
            additional_kwargs={PROTECTED_KEY: True},
        )
        messages = [HumanMessage(content="go", id="h1"), block, ai, protected_big, big]
        result = mgr._elide_largest_tool_results(messages, 500)
        tools = {m.tool_call_id: m for m in result if isinstance(m, ToolMessage)}
        # The larger protected result is never shed; the unprotected one is.
        assert tools["tp"].content.startswith("protected ")
        assert tools["t1"].content.startswith("[tool result elided")
        assert result[1] is block

    def test_emergency_truncate_skips_protected(self, mgr):
        protected_big = ToolMessage(
            content="p" * 5000,
            tool_call_id="tp",
            additional_kwargs={PROTECTED_KEY: True},
        )
        big = ToolMessage(content="q" * 5000, tool_call_id="t1")
        result = mgr._emergency_truncate_tool_results([protected_big, big])
        assert result[0].content == "p" * 5000
        assert "[EMERGENCY TRUNCATED" in result[1].content
