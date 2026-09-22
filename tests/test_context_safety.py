"""Tests for the context safety system.

Covers the aux-budgeted rolling-fold summarization engine
(src/core/summarizer.py), its integration through ContextManager
(summarize_conversation / summarize_and_compact / ensure_within_limits),
and the pre-request formatting/observation-masking helpers.

Design: knowledge-base/knowledge/features/context_summarization_rework.md.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.context import (
    ContextManager,
    ContextConfig,
    ConversationSummary,
    place_pinned_after_summary,
)
from shared.runtime.core.message_markers import (
    PROTECTED_KEY,
    is_protected_message,
    protected_phase_key,
)
from shared.runtime.core.workspace_injection import create_phase_instruction_message
from agent.core.summarizer import (
    SummarizationEngine,
    SummarizationFailed,
    is_overflow_error,
)


# =============================================================================
# Test Fixtures
# =============================================================================


# Deterministic counter for planner tests: 4 chars per token, no tiktoken.
def char_counter(text: str) -> int:
    return len(text) // 4


@pytest.fixture
def context_config():
    """Create a test context config with low limits for testing."""
    return ContextConfig(
        compaction_threshold_tokens=1000,
        summarization_threshold_tokens=1000,
        message_count_threshold=10,
        message_count_min_tokens=500,
        keep_recent_messages=3,
        keep_recent_tool_results=2,
        model_max_context_tokens=2000,
    )


@pytest.fixture
def context_manager(context_config):
    """Create a context manager with test config."""
    return ContextManager(config=context_config, model="gpt-4")


def make_mock_aux(max_context_tokens=15_000):
    """Create a mock AuxiliaryLLM that returns structured summaries."""
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    llm = MagicMock()

    parsed_value = ConversationSummary(
        summary="Test summary of the conversation.",
        tasks_completed="- Task 1 completed\n- Task 2 completed",
        key_decisions="Decision to use approach A",
        current_state="Ready for next phase",
        blockers="",
    )
    raw_response = AIMessage(content="structured output")
    structured_llm = AsyncMock()
    structured_llm.ainvoke = AsyncMock(
        return_value={
            "raw": raw_response,
            "parsed": parsed_value,
            "parsing_error": None,
        }
    )
    llm.with_structured_output = MagicMock(return_value=structured_llm)

    return AuxiliaryLLM(llm=llm, max_context_tokens=max_context_tokens)


@pytest.fixture
def mock_llm():
    """AuxiliaryLLM-compatible mock with a 15k-token window."""
    return make_mock_aux()


def make_failing_aux(error: Exception, max_context_tokens=15_000):
    """Create a mock AuxiliaryLLM whose every call raises ``error``."""
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    llm = MagicMock()
    structured_llm = AsyncMock()
    structured_llm.ainvoke = AsyncMock(side_effect=error)
    llm.with_structured_output = MagicMock(return_value=structured_llm)
    llm.ainvoke = AsyncMock(side_effect=error)
    return AuxiliaryLLM(llm=llm, max_context_tokens=max_context_tokens)


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    """No real sleeps between summarization retries in tests."""
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


def create_large_message_history(
    num_messages: int, chars_per_message: int = 500
) -> list:
    """Create a list of messages with specified total size."""
    messages = []
    for i in range(num_messages):
        if i % 2 == 0:
            content = f"User message {i}: " + "x" * chars_per_message
            messages.append(HumanMessage(content=content))
        else:
            content = f"Assistant response {i}: " + "y" * chars_per_message
            messages.append(AIMessage(content=content))
    return messages


def make_engine(aux, *, max_summary_length=3000, **kwargs):
    """Engine with deterministic char-based counting for planner tests."""
    return SummarizationEngine(
        aux,
        max_summary_length=max_summary_length,
        token_counter=char_counter,
        **kwargs,
    )


# =============================================================================
# Tests for SummarizationEngine.plan
# =============================================================================


class TestSummarizationPlanner:
    """The plan is pure and deterministic: no chunk may exceed the budget
    derived from the AUXILIARY model's window (never the main model's)."""

    def test_single_chunk_when_small(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan(["Short message 1", "Short message 2"])

        assert plan.n_passes == 1
        assert plan.chunks[0].first_part == 1
        assert plan.chunks[0].last_part == 2

    def test_multiple_chunks_when_large(self, mock_llm):
        # window 15000 → budget = floor(15000*0.85) - overhead(2000) - 2*1000
        engine = make_engine(mock_llm)
        budget = engine.chunk_budget
        assert budget > 0

        # 30 parts of ~1000 tokens each (4000 chars / 4)
        parts = [f"part {i}: " + "x" * 4000 for i in range(30)]
        plan = engine.plan(parts)

        assert plan.n_passes > 1
        # Every chunk within budget
        for chunk in plan.chunks:
            assert chunk.tokens <= budget
        # All parts covered, in order, no gaps
        assert plan.chunks[0].first_part == 1
        assert plan.chunks[-1].last_part == 30
        for prev, nxt in zip(plan.chunks, plan.chunks[1:]):
            assert nxt.first_part == prev.last_part + 1

    def test_empty_input_yields_empty_plan(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan([])
        assert plan.n_passes == 0

    def test_oversized_single_part_is_split(self, mock_llm):
        """One giant tool result must not produce an over-budget chunk."""
        engine = make_engine(mock_llm)
        budget = engine.chunk_budget

        # Single part at ~3x the budget
        parts = ["x" * (budget * 4 * 3)]
        plan = engine.plan(parts)

        assert plan.n_passes >= 3
        for chunk in plan.chunks:
            assert chunk.tokens <= budget
            # All belong to the same original part ordinal
            assert chunk.first_part == 1
            assert chunk.last_part == 1
        assert "[part 1/" in plan.chunks[0].text

    def test_window_too_small_raises(self):
        aux = make_mock_aux(max_context_tokens=3000)
        engine = make_engine(aux, max_summary_length=10000)

        with pytest.raises(SummarizationFailed) as exc_info:
            engine.plan(["hello"])
        assert exc_info.value.reason == "aux_window_too_small"

    def test_budget_derives_from_aux_window_not_main(self, mock_llm):
        """The regression this rework exists for: a giant main-model window
        must not inflate the summarizer's chunk budget."""
        engine = make_engine(mock_llm)
        # Budget must be bounded by the aux window regardless of any main
        # model configuration (the engine never sees the main config at all).
        assert engine.chunk_budget < mock_llm.max_context_tokens

    def test_unknown_window_falls_back_conservative(self):
        aux = make_mock_aux(max_context_tokens=None)
        engine = make_engine(aux)
        from agent.core.summarizer import DEFAULT_AUX_WINDOW

        assert engine.aux_window == DEFAULT_AUX_WINDOW


# =============================================================================
# Tests for the extracted ChunkPlanner (Slice 1)
# =============================================================================


class TestChunkPlannerParity:
    """SummarizationEngine.plan now delegates to the shared ChunkPlanner. The
    extraction refactor must not move a single chunk boundary (byte parity),
    and the load-bearing double-subtraction of the output budget must survive.
    """

    def test_engine_delegates_byte_identically(self, mock_llm):
        import math

        from shared.runtime.core.chunk_planner import ChunkPlanner

        engine = make_engine(mock_llm)
        # A standalone planner built from the same authority as the engine's
        # internal one (two reserves = output_budget, no overlap).
        planner = ChunkPlanner(
            engine.aux_window,
            overhead_tokens=engine._overhead,
            output_reserve=engine.output_budget,
            carry_reserve=engine.output_budget,
            overlap_ratio=0.0,
            token_counter=char_counter,
        )
        budget = engine.chunk_budget
        cases = [
            [],
            ["short message 1", "short message 2"],
            [f"part {i}: " + "x" * 4000 for i in range(30)],  # multi-chunk
            ["x" * (budget * 4 * 3)],  # oversized single part → hard split
        ]
        for parts in cases:
            assert planner.chunk_budget == engine.chunk_budget
            assert planner.plan(parts).describe() == engine.plan(parts).describe()
            assert math.isclose(
                planner.plan(parts).total_tokens, engine.plan(parts).total_tokens
            )

    def test_budget_subtracts_output_budget_twice(self, mock_llm):
        """The fold carries the rolling summary, so output_budget is reserved
        twice. Pin it — a regression that drops the second subtraction would
        silently oversize every fold chunk (the 5dbb5770-class failure)."""
        import math

        from shared.runtime.core.chunk_planner import SAFETY_MARGIN

        engine = make_engine(mock_llm)
        expected = (
            math.floor(engine.aux_window * SAFETY_MARGIN)
            - engine._overhead
            - 2 * engine.output_budget
        )
        assert engine.chunk_budget == expected


class TestChunkPlannerOverlap:
    """overlap_ratio > 0 (extraction-only): adjacent chunks share a whole-part
    seed so a fact straddling a boundary lands in two chunks, yet no chunk ever
    exceeds the budget and every part is still covered."""

    def _planner(self, overlap_ratio):
        from shared.runtime.core.chunk_planner import ChunkPlanner

        # Window comfortably above the 1_000-token planning floor.
        return ChunkPlanner(
            12_000,
            overhead_tokens=0,
            output_reserve=0,
            carry_reserve=0,
            overlap_ratio=overlap_ratio,
            token_counter=char_counter,
        )

    def _parts(self, planner, n=12, frac=5):
        # Each part ~ budget/frac tokens (char_counter = len // 4).
        part_tokens = planner.chunk_budget // frac
        return ["x" * (part_tokens * 4) for _ in range(n)]

    def test_no_overlap_has_contiguous_disjoint_ranges(self):
        planner = self._planner(0.0)
        plan = planner.plan(self._parts(planner))
        assert plan.n_passes >= 2
        for cur, nxt in zip(plan.chunks, plan.chunks[1:]):
            assert nxt.first_part == cur.last_part + 1  # hard boundary

    def test_overlap_ranges_intersect_and_stay_in_budget(self):
        planner = self._planner(0.3)
        budget = planner.chunk_budget
        parts = self._parts(planner)
        plan = planner.plan(parts)

        assert plan.n_passes >= 2
        # No chunk exceeds the real budget despite the prepended seed.
        assert all(c.tokens <= budget for c in plan.chunks)
        # At least one adjacent pair shares a part (ranges intersect).
        assert any(
            nxt.first_part <= cur.last_part
            for cur, nxt in zip(plan.chunks, plan.chunks[1:])
        )
        # Every part is still covered — overlap adds, never drops.
        covered = set()
        for c in plan.chunks:
            covered.update(range(c.first_part, c.last_part + 1))
        assert covered == set(range(1, len(parts) + 1))

    def test_overlap_terminates_with_parts_larger_than_seed(self):
        # Parts bigger than the overlap allowance must not loop or drop content
        # (the seed is empty for those, packing just proceeds normally).
        planner = self._planner(0.3)
        parts = self._parts(planner, n=6, frac=2)  # ~budget/2 each > overlap
        plan = planner.plan(parts)
        assert plan.n_passes >= 2
        covered = set()
        for c in plan.chunks:
            covered.update(range(c.first_part, c.last_part + 1))
        assert covered == set(range(1, len(parts) + 1))


# =============================================================================
# Tests for the rolling fold (SummarizationEngine.run)
# =============================================================================


class TestRollingFold:
    """summary_i = summarize(summary_{i-1} + chunk_i), with bounded retries
    on the SAME structured call — no unstructured fallback exists anymore."""

    @pytest.mark.asyncio
    async def test_single_pass_returns_formatted_summary(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan(["User: Hello", "Assistant: Hi"])
        result = await engine.run(plan)

        assert "**Summary:**" in result
        assert "Test summary" in result

    @pytest.mark.asyncio
    async def test_fold_threads_running_summary(self, mock_llm):
        """Pass 2 must see pass 1's summary as 'Prior Summary:'."""
        engine = make_engine(mock_llm)
        parts = [f"part {i}: " + "x" * 4000 for i in range(30)]
        plan = engine.plan(parts)
        assert plan.n_passes > 1

        await engine.run(plan)

        structured = mock_llm.llm.with_structured_output.return_value
        assert structured.ainvoke.call_count == plan.n_passes
        # Second call's human message carries the running summary
        second_call_messages = structured.ainvoke.call_args_list[1][0][0]
        human_content = second_call_messages[1].content
        assert "Prior Summary:" in human_content
        assert "Test summary" in human_content

    @pytest.mark.asyncio
    async def test_seed_summary_reaches_first_pass(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan(["User: continue please"])
        await engine.run(plan, seed_summary="Previously: built the parser.")

        structured = mock_llm.llm.with_structured_output.return_value
        first_call_messages = structured.ainvoke.call_args_list[0][0][0]
        assert "Prior Summary: Previously: built the parser." in (
            first_call_messages[1].content
        )

    @pytest.mark.asyncio
    async def test_focus_included_in_fold(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan(["User: lots of stuff"])
        await engine.run(plan, focus="keep the SQL schema details")

        structured = mock_llm.llm.with_structured_output.return_value
        first_call_messages = structured.ainvoke.call_args_list[0][0][0]
        assert "keep the SQL schema details" in first_call_messages[1].content

    @pytest.mark.asyncio
    async def test_transient_failure_retried_then_succeeds(self, mock_llm):
        parsed_value = ConversationSummary(
            summary="Recovered summary.",
            tasks_completed="",
            key_decisions="",
            current_state="",
        )
        structured = mock_llm.llm.with_structured_output.return_value
        structured.ainvoke = AsyncMock(
            side_effect=[
                Exception("503 all backends unavailable"),
                {
                    "raw": AIMessage(content="ok"),
                    "parsed": parsed_value,
                    "parsing_error": None,
                },
            ]
        )

        engine = make_engine(mock_llm)
        plan = engine.plan(["User: hello"])
        result = await engine.run(plan)

        assert "Recovered summary" in result
        assert structured.ainvoke.call_count == 2

    @pytest.mark.asyncio
    async def test_permanent_failure_raises_after_max_attempts(self):
        aux = make_failing_aux(Exception("503 all backends unavailable"))
        engine = make_engine(aux)
        plan = engine.plan(["User: hello"])

        with pytest.raises(SummarizationFailed) as exc_info:
            await engine.run(plan)

        assert exc_info.value.reason == "aux_unavailable"
        from agent.core.summarizer import MAX_ATTEMPTS

        structured = aux.llm.with_structured_output.return_value
        assert structured.ainvoke.call_count == MAX_ATTEMPTS

    @pytest.mark.asyncio
    async def test_overflow_error_never_retried(self):
        """A planned call that overflows is a plan bug — deterministic, so
        retrying it just hammers the endpoint."""

        class ContextOverflowError(Exception):
            pass

        aux = make_failing_aux(ContextOverflowError("951682 exceeds limit of 131072"))
        engine = make_engine(aux)
        plan = engine.plan(["User: hello"])

        with pytest.raises(SummarizationFailed) as exc_info:
            await engine.run(plan)

        assert exc_info.value.reason == "aux_overflow"
        structured = aux.llm.with_structured_output.return_value
        assert structured.ainvoke.call_count == 1  # no retry

    @pytest.mark.asyncio
    async def test_empty_plan_raises(self, mock_llm):
        engine = make_engine(mock_llm)
        plan = engine.plan([])
        with pytest.raises(SummarizationFailed) as exc_info:
            await engine.run(plan)
        assert exc_info.value.reason == "empty_plan"


class TestOverflowDetection:
    def test_detects_typed_overflow_in_cause_chain(self):
        class ContextOverflowError(Exception):
            pass

        inner = ContextOverflowError("too big")
        outer = Exception("wrapped")
        outer.__cause__ = inner
        assert is_overflow_error(outer)

    def test_detects_synthetic_413(self):
        e = Exception("api error")
        e.status_code = 413
        assert is_overflow_error(e)

    def test_detects_aux_preflight_guard(self):
        from shared.runtime.services.auxiliary import AuxInputTooLarge

        assert is_overflow_error(AuxInputTooLarge(951_682, 131_072, "SummarizeTask"))

    def test_ignores_transient_errors(self):
        assert not is_overflow_error(Exception("503 backend unavailable"))
        assert not is_overflow_error(TimeoutError("read timeout"))


# =============================================================================
# Tests for the aux pre-flight guard
# =============================================================================


class TestAuxPreflightGuard:
    @pytest.mark.asyncio
    async def test_chain_rejects_oversized_input(self):
        """Non-summarization aux tasks fail fast instead of overflowing at
        the transport (951k memory-extraction payloads to a 131k model)."""
        from shared.runtime.services.auxiliary import AuxInputTooLarge, SummarizeTask

        aux = make_mock_aux(max_context_tokens=100)
        task = SummarizeTask(
            conversation_text="word " * 2000,
            summarization_prompt="",
            max_summary_length=1000,
        )

        with pytest.raises(AuxInputTooLarge):
            await aux.chain(task)

        # The LLM was never invoked
        structured = aux.llm.with_structured_output.return_value
        structured.ainvoke.assert_not_called()

    @pytest.mark.asyncio
    async def test_chain_allows_fitting_input(self, mock_llm):
        from shared.runtime.services.auxiliary import SummarizeTask

        task = SummarizeTask(
            conversation_text="User: short conversation",
            summarization_prompt="",
            max_summary_length=1000,
        )
        result = await mock_llm.chain(task)
        assert result.summary == "Test summary of the conversation."

    @pytest.mark.asyncio
    async def test_no_guard_when_window_unknown(self):
        from shared.runtime.services.auxiliary import SummarizeTask

        aux = make_mock_aux(max_context_tokens=None)
        task = SummarizeTask(
            conversation_text="word " * 2000,
            summarization_prompt="",
            max_summary_length=1000,
        )
        result = await aux.chain(task)  # no raise
        assert result is not None


# =============================================================================
# Tests for _format_messages_for_summary
# =============================================================================


class TestFormatMessagesForSummary:
    """Tests for the _format_messages_for_summary helper method."""

    def test_formats_human_messages(self, context_manager):
        """Human messages should be formatted with User prefix."""
        messages = [HumanMessage(content="Hello, world!")]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert parts[0].startswith("User:")
        assert "Hello, world!" in parts[0]

    def test_formats_ai_messages(self, context_manager):
        """AI messages should be formatted with Assistant prefix."""
        messages = [AIMessage(content="Hello back!")]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert parts[0].startswith("Assistant:")

    def test_formats_tool_calls(self, context_manager):
        """AI messages with tool calls should show tool names."""
        messages = [
            AIMessage(
                content="", tool_calls=[{"name": "read_file", "id": "1", "args": {}}]
            )
        ]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert "read_file" in parts[0]

    def test_formats_tool_messages_recent(self, context_manager):
        """Recent tool messages should include truncated content (observation masking)."""
        messages = [ToolMessage(content="x" * 100, tool_call_id="1")]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        # Single tool message falls within the recent-10 window, so content is included
        assert "[Tool 'unknown' result:" in parts[0]
        assert "xxx" in parts[0]  # content preserved (under 300 char truncation)

    def test_formats_tool_messages_old_masked(self, context_manager):
        """Old tool messages beyond the 10-message window should be observation-masked."""
        # Create 12 tool messages — first 2 should be masked, last 10 should have content
        messages = [
            ToolMessage(content=f"result_{i}" * 20, tool_call_id=str(i))
            for i in range(12)
        ]
        parts = context_manager._format_messages_for_summary(messages)

        # 12 messages + 1 recency marker = 13 parts
        assert len(parts) == 13
        # First 2 are beyond the recent-10 window — should be masked (placeholder only)
        assert "omitted" in parts[0]
        assert "omitted" in parts[1]
        # Recency marker separates old from recent
        assert "RECENT CONTEXT" in parts[2]
        # Last 10 should have content
        assert "result_2" in parts[3]
        assert "result_11" in parts[12]

    def test_recency_marker_inserted_when_enough_tool_messages(self, context_manager):
        """Recency marker should appear when there are >10 tool messages."""
        messages = [
            ToolMessage(content=f"result_{i}", tool_call_id=str(i)) for i in range(15)
        ]
        parts = context_manager._format_messages_for_summary(messages)

        marker_parts = [p for p in parts if "RECENT CONTEXT" in p]
        assert len(marker_parts) == 1
        assert "PRESERVE WITH HIGHEST PRIORITY" in marker_parts[0]

    def test_no_recency_marker_when_few_tool_messages(self, context_manager):
        """No recency marker when all tool messages fit in the recent window."""
        messages = [
            ToolMessage(content=f"result_{i}", tool_call_id=str(i)) for i in range(5)
        ]
        parts = context_manager._format_messages_for_summary(messages)

        marker_parts = [p for p in parts if "RECENT CONTEXT" in p]
        assert len(marker_parts) == 0

    def test_atomic_grouping_preserves_sibling_results(self, context_manager):
        """When one tool result in a group is recent, all siblings should be recent too."""
        # 9 old standalone tool messages (fill most of the window)
        old_msgs = [
            ToolMessage(content=f"old_{i}" * 20, tool_call_id=f"old_{i}")
            for i in range(9)
        ]

        # 1 AIMessage calling 3 tools — will straddle the boundary
        ai_msg = AIMessage(
            content="",
            tool_calls=[
                {"name": "read_file", "id": "group_a", "args": {}},
                {"name": "web_search", "id": "group_b", "args": {}},
                {"name": "sql_query", "id": "group_c", "args": {}},
            ],
        )
        group_results = [
            ToolMessage(content="file content here", tool_call_id="group_a"),
            ToolMessage(content="search results here", tool_call_id="group_b"),
            ToolMessage(content="query output here", tool_call_id="group_c"),
        ]

        # Total: 12 tool messages. Flat last-10 keeps old_2..old_8 + all 3 grouped.
        # All 3 grouped are in the flat window here, but atomic grouping ensures
        # that even at different boundary positions, they stay together.
        messages = old_msgs + [ai_msg] + group_results
        parts = context_manager._format_messages_for_summary(messages)

        # All 3 grouped results should show content, not "omitted"
        for label in ["file content", "search results", "query output"]:
            matching = [p for p in parts if label in p]
            assert len(matching) == 1
            assert "omitted" not in matching[0], f"'{label}' should not be masked"

    def test_atomic_grouping_boundary_case(self, context_manager):
        """Group straddling the flat-10 boundary: all siblings should be preserved."""
        # 10 standalone old tool messages
        old_msgs = [
            ToolMessage(content=f"standalone_{i}" * 20, tool_call_id=f"s_{i}")
            for i in range(10)
        ]

        # AIMessage with 3 tool calls — group will straddle the flat-10 boundary
        ai_msg = AIMessage(
            content="",
            tool_calls=[
                {"name": "tool_a", "id": "g_a", "args": {}},
                {"name": "tool_b", "id": "g_b", "args": {}},
                {"name": "tool_c", "id": "g_c", "args": {}},
            ],
        )
        group_results = [
            ToolMessage(content="result_a data", tool_call_id="g_a"),
            ToolMessage(content="result_b data", tool_call_id="g_b"),
            ToolMessage(content="result_c data", tool_call_id="g_c"),
        ]

        # Total: 13 tool messages. Flat last-10 = s_3..s_9 + g_a + g_b + g_c
        # g_a is the 11th tool message — in the flat window.
        # But with only 7 standalone old in the window, if we add more old messages
        # to push g_a out, atomic grouping pulls it back in.
        # Here all 3 are already in the flat window, so just verify they stay.
        messages = old_msgs + [ai_msg] + group_results
        parts = context_manager._format_messages_for_summary(messages)

        for label in ["result_a", "result_b", "result_c"]:
            matching = [p for p in parts if label in p]
            assert len(matching) == 1
            assert "omitted" not in matching[0], f"{label} should not be masked"

    def test_atomic_grouping_pulls_in_old_siblings(self, context_manager):
        """A group member outside the flat-10 window is pulled in by a recent sibling."""
        # AIMessage with 2 tool calls — results will be split across the boundary
        ai_msg = AIMessage(
            content="",
            tool_calls=[
                {"name": "tool_x", "id": "pair_x", "args": {}},
                {"name": "tool_y", "id": "pair_y", "args": {}},
            ],
        )

        # Place pair_x early (outside flat window), pair_y late (inside flat window),
        # with 10 filler tool messages in between.
        # Total: 12 tool messages. Flat last-10 = indices 2..11.
        # pair_x (tool index 0) is OUTSIDE flat window.
        # pair_y (tool index 11) is INSIDE flat window.
        # Atomic grouping should pull pair_x into the recent set.
        messages = (
            [ai_msg]
            + [ToolMessage(content="x_data value", tool_call_id="pair_x")]
            + [
                ToolMessage(content=f"fill_{i}" * 20, tool_call_id=f"fl_{i}")
                for i in range(10)
            ]
            + [ToolMessage(content="y_data value", tool_call_id="pair_y")]
        )
        parts = context_manager._format_messages_for_summary(messages)

        # pair_x should show content (pulled in by pair_y), not be masked
        x_parts = [p for p in parts if "x_data" in p]
        assert len(x_parts) == 1
        assert "omitted" not in x_parts[0], (
            "pair_x should be pulled into recent by sibling pair_y"
        )

        # pair_y should also show content (it's naturally in the window)
        y_parts = [p for p in parts if "y_data" in p]
        assert len(y_parts) == 1
        assert "omitted" not in y_parts[0]

    def test_includes_prior_summaries(self, context_manager):
        """System messages with prior summaries should be included."""
        messages = [
            SystemMessage(content="[Summary of prior work]\nPrevious work summary."),
            HumanMessage(content="Continue"),
        ]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 2
        assert "Prior Summary:" in parts[0]
        assert "User:" in parts[1]

    def test_excludes_regular_system_messages(self, context_manager):
        """Regular system messages should be excluded."""
        messages = [
            SystemMessage(content="You are a helpful assistant."),
            HumanMessage(content="Hello"),
        ]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert parts[0].startswith("User:")

    def test_truncates_long_messages(self, context_manager):
        """Long messages should be truncated."""
        long_content = "x" * 1000
        messages = [HumanMessage(content=long_content)]
        parts = context_manager._format_messages_for_summary(messages)

        # Human messages truncated to 500 chars
        assert len(parts[0]) < 600

        # AI messages truncated to 800 chars
        ai_messages = [AIMessage(content="y" * 1000)]
        ai_parts = context_manager._format_messages_for_summary(ai_messages)
        assert "y" * 800 in ai_parts[0]
        assert "y" * 801 not in ai_parts[0]

    def test_ai_reasoning_preserved_at_800_chars(self, context_manager):
        """AI reasoning should be preserved up to 800 chars, not 300."""
        # Content that's 500 chars — would be cut at 300 before, now preserved
        content = "A" * 500
        messages = [AIMessage(content=content)]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert "A" * 500 in parts[0]  # Full 500 chars preserved

    def test_ai_reasoning_with_tool_calls_preserved(self, context_manager):
        """AIMessage with both reasoning content and tool_calls should show both."""
        messages = [
            AIMessage(
                content="I need to check the auth module because the JWT validation is failing",
                tool_calls=[
                    {"name": "read_file", "id": "tc1", "args": {"path": "auth.py"}},
                    {"name": "web_search", "id": "tc2", "args": {"query": "JWT"}},
                ],
            )
        ]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        # Both reasoning and tool names should be present
        assert "JWT validation" in parts[0]
        assert "read_file" in parts[0]
        assert "web_search" in parts[0]

    def test_ai_reasoning_with_tool_calls_empty_content(self, context_manager):
        """AIMessage with tool_calls but empty content should only show tool names."""
        messages = [
            AIMessage(
                content="",
                tool_calls=[{"name": "read_file", "id": "tc1", "args": {}}],
            )
        ]
        parts = context_manager._format_messages_for_summary(messages)

        assert len(parts) == 1
        assert "read_file" in parts[0]
        assert parts[0] == "Assistant: [Called tools: read_file]"


# =============================================================================
# Tests for summarize_conversation (main entry point)
# =============================================================================


class TestSummarizeConversation:
    """Tests for the main summarize_conversation method."""

    @pytest.mark.asyncio
    async def test_small_input_single_pass(self, context_manager, mock_llm):
        messages = [
            HumanMessage(content="Hello"),
            AIMessage(content="Hi there!"),
        ]

        result = await context_manager.summarize_conversation(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert "**Summary:**" in result
        assert context_manager.state.total_summarizations == 1
        structured = mock_llm.llm.with_structured_output.return_value
        assert structured.ainvoke.call_count == 1

    @pytest.mark.asyncio
    async def test_large_input_folds_in_multiple_passes(
        self, context_manager, mock_llm
    ):
        """Input beyond the aux chunk budget folds across several calls.

        Note the formatter truncates each message (User 500 / Assistant 800
        chars), so multi-pass needs a genuinely LONG history — which is
        exactly the production shape (very long sessions), not four giant
        PDFs (those are bounded per-message by the formatter).
        """
        # 80 messages of varied words ≈ well past the ~4k-token chunk budget
        # for a 15k aux window even after per-message truncation.
        messages = []
        for i in range(80):
            content = " ".join(f"w{i}x{j}" for j in range(150))
            messages.append(
                HumanMessage(content=content)
                if i % 2 == 0
                else AIMessage(content=content)
            )

        result = await context_manager.summarize_conversation(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert result
        structured = mock_llm.llm.with_structured_output.return_value
        assert structured.ainvoke.call_count > 1

    @pytest.mark.asyncio
    async def test_failure_returns_none(self, context_manager):
        """Total summarizer failure returns None — callers keep history
        uncompacted, never a placeholder (audit #4)."""
        aux = make_failing_aux(Exception("LLM error"))

        result = await context_manager.summarize_conversation(
            messages=[HumanMessage(content="test")],
            auxiliary=aux,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_messages_return_none(self, context_manager, mock_llm):
        result = await context_manager.summarize_conversation(
            messages=[],
            auxiliary=mock_llm,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_tracks_summarization_state(self, context_manager, mock_llm):
        """Should track summarization in state."""
        messages = [HumanMessage(content="Test")]

        await context_manager.summarize_conversation(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert context_manager.state.total_summarizations == 1
        assert len(context_manager.state.summaries) == 1


# =============================================================================
# Tests for compaction progress events
# =============================================================================


class TestProgressEvents:
    """compaction.started / compaction.progress / compaction.failed flow
    through the ContextManager progress callback (S3 transport contract)."""

    @pytest.mark.asyncio
    async def test_success_emits_started_and_progress(self, context_manager, mock_llm):
        events = []

        async def recorder(event, params):
            events.append((event, params))

        context_manager.set_progress_callback(recorder)
        await context_manager.summarize_conversation(
            messages=[HumanMessage(content="Hello"), AIMessage(content="Hi")],
            auxiliary=mock_llm,
            trigger="manual",
        )

        names = [e[0] for e in events]
        assert names[0] == "compaction.started"
        started = events[0][1]
        assert started["trigger"] == "manual"
        assert started["n_passes"] == 1
        assert started["aux_limit_tokens"] == 15_000
        assert started["plan"][0]["pass"] == 1
        assert "compaction.progress" in names
        assert "compaction.failed" not in names
        # Stats stashed for the context.compacted completion event
        assert context_manager._last_summarization_stats["n_passes"] == 1

    @pytest.mark.asyncio
    async def test_failure_emits_failed_event(self, context_manager):
        events = []

        async def recorder(event, params):
            events.append((event, params))

        context_manager.set_progress_callback(recorder)
        aux = make_failing_aux(Exception("503"))
        result = await context_manager.summarize_conversation(
            messages=[HumanMessage(content="Hello")],
            auxiliary=aux,
        )

        assert result is None
        names = [e[0] for e in events]
        assert "compaction.failed" in names
        failed = [p for n, p in events if n == "compaction.failed"][0]
        assert failed["reason"] == "aux_unavailable"
        assert failed["kept_messages"] is True

    @pytest.mark.asyncio
    async def test_retry_bumps_attempt_in_progress(self, context_manager, mock_llm):
        parsed_value = ConversationSummary(
            summary="ok",
            tasks_completed="",
            key_decisions="",
            current_state="",
        )
        structured = mock_llm.llm.with_structured_output.return_value
        structured.ainvoke = AsyncMock(
            side_effect=[
                Exception("503"),
                {
                    "raw": AIMessage(content="ok"),
                    "parsed": parsed_value,
                    "parsing_error": None,
                },
            ]
        )
        events = []

        async def recorder(event, params):
            events.append((event, params))

        context_manager.set_progress_callback(recorder)
        result = await context_manager.summarize_conversation(
            messages=[HumanMessage(content="Hello")],
            auxiliary=mock_llm,
        )

        assert result
        attempts = [p["attempt"] for n, p in events if n == "compaction.progress"]
        assert 2 in attempts  # the retry was visible

    @pytest.mark.asyncio
    async def test_progress_frames_carry_trigger(self, context_manager, mock_llm):
        """Every engine progress frame is stamped with the trigger so a
        replayed progress frame (reload mid-compaction, started frame behind
        the cursor) reconstructs the UI without guessing 'auto'."""
        events = []

        async def recorder(event, params):
            events.append((event, params))

        context_manager.set_progress_callback(recorder)
        await context_manager.summarize_conversation(
            messages=[HumanMessage(content="Hello"), AIMessage(content="Hi")],
            auxiliary=mock_llm,
            trigger="manual",
        )

        progress = [p for (e, p) in events if e == "compaction.progress"]
        assert progress, "expected at least one progress frame"
        assert all(p["trigger"] == "manual" for p in progress)


# =============================================================================
# Tests for the compaction run counter (transport did-it-compact signal)
# =============================================================================


class TestCompactionRunCounter:
    """``compaction_runs`` increments only on a *successful* summarization —
    the transports' authoritative did-it-compact signal, replacing length
    heuristics that false-fired on stray RemoveMessage markers (the
    duplicate-banner bug, 2026-06-12)."""

    @pytest.mark.asyncio
    async def test_increments_on_successful_compaction(self, context_manager, mock_llm):
        messages = create_large_message_history(12)
        assert context_manager.compaction_runs == 0
        result = await context_manager.summarize_and_compact(messages, mock_llm)
        assert context_manager.compaction_runs == 1
        assert any(
            isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
            for m in result
        )

    @pytest.mark.asyncio
    async def test_no_increment_when_nothing_to_summarize(
        self, context_manager, mock_llm
    ):
        # len(conversation) <= keep_recent_messages (3) → early no-op return.
        messages = [HumanMessage(content="hi"), AIMessage(content="hello")]
        result = await context_manager.summarize_and_compact(messages, mock_llm)
        assert context_manager.compaction_runs == 0
        assert [m.content for m in result] == [m.content for m in messages]

    @pytest.mark.asyncio
    async def test_no_increment_on_summarizer_failure(self, context_manager):
        failing = make_failing_aux(RuntimeError("aux endpoint down"))
        messages = create_large_message_history(12)
        result = await context_manager.summarize_and_compact(messages, failing)
        assert context_manager.compaction_runs == 0
        # History kept intact — never compact behind a placeholder.
        assert [m.content for m in result] == [m.content for m in messages]

    @pytest.mark.asyncio
    async def test_size_guard_skip_emits_compaction_skipped(
        self, context_manager, mock_llm
    ):
        """When the summary comes out larger than what it would replace, the
        guard keeps the originals — and must emit a journaled terminal frame
        (compaction.skipped) so a replayed progress UI can clear. Counter
        stays untouched (no adoption, no banner row)."""
        events = []

        async def recorder(event, params):
            events.append((event, params))

        context_manager.set_progress_callback(recorder)
        # One tiny message beyond keep_recent (3): original ≈ a few tokens,
        # the structured summary is always bigger → guard fires.
        messages = [
            HumanMessage(content="hi"),
            AIMessage(content="yo"),
            HumanMessage(content="ok"),
            AIMessage(content="k"),
        ]
        result = await context_manager.summarize_and_compact(messages, mock_llm)

        assert context_manager.compaction_runs == 0
        assert [m.content for m in result] == [m.content for m in messages]
        names = [e for e, _ in events]
        assert "compaction.started" in names
        assert "compaction.skipped" in names
        skipped = next(p for e, p in events if e == "compaction.skipped")
        assert skipped["reason"] == "summary_not_smaller"
        assert skipped["trigger"] == "auto"


# =============================================================================
# Tests for ensure_within_limits with force parameter
# =============================================================================


class TestEnsureWithinLimitsForce:
    """Tests for ensure_within_limits with force=True (used by Layer 1)."""

    @pytest.mark.asyncio
    async def test_force_triggers_summarization(self, context_manager, mock_llm):
        """force=True should trigger summarization even below threshold.

        Note: Summarization still requires enough messages to summarize.
        If len(messages) <= keep_recent_messages, there's nothing to summarize.
        """
        # Need more messages than keep_recent_messages (which is 3 in test config)
        messages = [
            HumanMessage(content="Message 1"),
            AIMessage(content="Response 1"),
            HumanMessage(content="Message 2"),
            AIMessage(content="Response 2"),
            HumanMessage(content="Message 3"),
            AIMessage(content="Response 3"),
        ]

        await context_manager.ensure_within_limits(
            messages=messages,
            auxiliary=mock_llm,
            force=True,
        )

        # Should have triggered summarization
        assert context_manager.state.total_summarizations == 1

    @pytest.mark.asyncio
    async def test_no_force_respects_threshold(self, context_manager, mock_llm):
        """Without force, should respect threshold."""
        # Small messages below threshold
        messages = [
            HumanMessage(content="Hello"),
            AIMessage(content="Hi"),
        ]

        result = await context_manager.ensure_within_limits(
            messages=messages,
            auxiliary=mock_llm,
            force=False,
        )

        # Should NOT have triggered summarization
        assert context_manager.state.total_summarizations == 0
        assert result == messages  # Messages unchanged


# =============================================================================
# Keep-window elision (audit #6): pairing-protected giants must not wedge
# =============================================================================


class TestKeepWindowElision:
    def test_elide_largest_first_until_under_target(self, context_manager):
        ai = AIMessage(
            content="",
            id="a1",
            tool_calls=[
                {"id": "t1", "name": "read_pdf", "args": {}},
                {"id": "t2", "name": "read_pdf", "args": {}},
            ],
        )
        big = ToolMessage(content="regulation " * 4000, tool_call_id="t1", id="t1")
        small = ToolMessage(content="tiny result", tool_call_id="t2", id="t2")
        messages = [HumanMessage(content="read these", id="h1"), ai, big, small]

        result = context_manager._elide_largest_tool_results(messages, 500)

        tool_msgs = [m for m in result if isinstance(m, ToolMessage)]
        assert tool_msgs[0].content.startswith("[tool result elided")
        # Pairing preserved
        assert tool_msgs[0].tool_call_id == "t1"
        # The small result survived (target reached after the largest)
        assert tool_msgs[1].content == "tiny result"
        assert context_manager.get_token_count(result) <= 500

    @pytest.mark.asyncio
    async def test_per_turn_path_elides_when_summary_cannot_shrink(
        self, context_manager, mock_llm
    ):
        """The b60166ee shape: compaction succeeds but keep-window tool
        results alone exceed the model limit — previously the request was
        resent at 183% forever. Now the giants get elided."""
        # Old, summarizable conversation
        messages = []
        for i in range(10):
            messages.append(
                HumanMessage(content=f"question {i} " + "x" * 400, id=f"h{i}")
            )
            messages.append(AIMessage(content=f"answer {i} " + "y" * 400, id=f"a{i}"))
        # Recent keep-window: a tool pair with results far beyond the
        # 2000-token model_max_context_tokens of the test config.
        messages.append(
            AIMessage(
                content="",
                id="ai-tools",
                tool_calls=[
                    {"id": "t1", "name": "read_pdf", "args": {}},
                    {"id": "t2", "name": "read_pdf", "args": {}},
                ],
            )
        )
        messages.append(
            ToolMessage(content="cfr part " * 2000, tool_call_id="t1", id="t1")
        )
        messages.append(
            ToolMessage(content="eu reg " * 2000, tool_call_id="t2", id="t2")
        )

        result = await context_manager.ensure_within_limits(
            messages=messages,
            auxiliary=mock_llm,
        )

        from langchain_core.messages import RemoveMessage

        non_remove = [m for m in result if not isinstance(m, RemoveMessage)]
        # Under the model limit now…
        assert context_manager.get_token_count(non_remove) <= 2000
        # …because the giant results were elided, with pairing intact.
        elided = [
            m
            for m in non_remove
            if isinstance(m, ToolMessage)
            and m.content.startswith("[tool result elided")
        ]
        assert len(elided) == 2
        assert {m.tool_call_id for m in elided} == {"t1", "t2"}

    @pytest.mark.asyncio
    async def test_run_counter_increments_for_tail_tool_truncation_no_summary(
        self, context_manager, mock_llm
    ):
        """When the conversation stays under keep_recent, keep-window truncation
        is still an actual compaction event and must bump compaction_runs."""
        context_manager.config.keep_window_max_tool_result_chars = 32

        messages = [
            HumanMessage(content="query", id="h1"),
            ToolMessage(
                content="x" * 20000,
                tool_call_id="t1",
                name="read_file",
                id="t1",
            ),
            AIMessage(content="ack", id="a1"),
        ]
        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert context_manager.compaction_runs == 1
        assert not any(
            isinstance(m, SystemMessage)
            and "[Summary of prior work]" in (m.content or "")
            for m in result
        )
        assert any(
            isinstance(m, ToolMessage)
            and "[tool result truncated by compaction" in (m.content or "")
            for m in result
            if not isinstance(m, RemoveMessage)
        )

    @pytest.mark.asyncio
    async def test_already_truncated_tail_is_skipped(self, context_manager, mock_llm):
        """Already-truncated tool results are recognized and left untouched.

        Uses the REAL truncation shape — kept head first, marker appended at
        the end — so a regression to a startswith-style check fails here.
        """
        context_manager.config.keep_window_max_tool_result_chars = 20
        already_truncated = (
            "y"
            * 20
            + "\n\n[tool result truncated by compaction: kept 20 of 20,000 chars "
            "(~5,000 tokens). Full content was saved to the workspace / is "
            "re-fetchable — re-run the tool or read the saved file if the rest "
            "is needed.]"
        )
        messages = [
            HumanMessage(content="short", id="h1"),
            ToolMessage(
                content=already_truncated,
                tool_call_id="t1",
                name="read_file",
                id="t1",
            ),
            AIMessage(content="done", id="a1"),
        ]
        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert context_manager.compaction_runs == 0
        assert result == messages

    @pytest.mark.asyncio
    async def test_fresh_tool_message_preserves_name(self, context_manager, mock_llm):
        """ToolMessage rebuild in the keep-window slice preserves `name`."""
        context_manager.config.keep_window_max_tool_result_chars = 40
        context_manager.summarize_conversation = AsyncMock(
            return_value="manual summary"
        )

        messages = [
            HumanMessage(content="seed", id="h0"),
            AIMessage(content="response", id="a0"),
            HumanMessage(content="next", id="h1"),
            AIMessage(content="response", id="a1"),
            HumanMessage(content="probe", id="h2"),
            HumanMessage(content="status", id="h3"),
            AIMessage(
                content="",
                id="a2",
                tool_calls=[{"id": "t1", "name": "read_pdf", "args": {}}],
            ),
            ToolMessage(
                content="x" * 120,
                tool_call_id="t1",
                name="read_pdf",
                id="t1",
            ),
        ]

        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        assert context_manager.compaction_runs == 1
        tool_msgs = [
            m
            for m in result
            if isinstance(m, ToolMessage) and not isinstance(m, RemoveMessage)
        ]
        assert any(
            m.tool_call_id == "t1" and m.name == "read_pdf" for m in tool_msgs
        ), "re-built keep-window tool results must preserve `name`"

    @pytest.mark.asyncio
    async def test_summarize_receives_full_conversation(
        self, context_manager, mock_llm
    ):
        """Manual cap lives inside summarize_and_compact; the summarizer sees
        the whole conversation, not only the pre-summarized slice."""
        observed = []

        async def _fake_summarize(messages, *args, **kwargs):
            observed.extend(messages)
            return "manual summary"

        context_manager.summarize_conversation = AsyncMock(side_effect=_fake_summarize)

        messages = [
            HumanMessage(content=f"hi {i}", id=f"h{i}")
            if i % 2 == 0
            else AIMessage(
                content=f"reply {i}",
                id=f"a{i}",
            )
            for i in range(6)
        ]
        result = await context_manager.summarize_and_compact(messages, mock_llm)

        assert [m.content for m in observed] == [m.content for m in messages]
        assert any(
            isinstance(m, SystemMessage)
            and "[Summary of prior work]" in (m.content or "")
            for m in result
        )


# =============================================================================
# Failure semantics (audit #4): never destroy history
# =============================================================================


class TestFailureKeepsHistory:
    @pytest.mark.asyncio
    async def test_compaction_keeps_history_when_summarization_fails(
        self, context_manager
    ):
        """summarize_and_compact must not replace history with a placeholder
        when the summarizer is fully unavailable."""
        aux = make_failing_aux(Exception("aux 503"))

        messages = []
        for i in range(12):
            messages.append(HumanMessage(content=f"question {i}", id=f"h{i}"))
            messages.append(AIMessage(content=f"answer {i}", id=f"a{i}"))

        result = await context_manager.summarize_and_compact(messages, aux)

        # Unchanged: same objects, no RemoveMessage markers, no placeholder.
        assert result == messages
        assert not any("[Summarization failed" in str(m.content) for m in result)


# =============================================================================
# Integration-style tests
# =============================================================================


class TestContextSafetyIntegration:
    """Integration tests for the safety system."""

    @pytest.mark.asyncio
    async def test_handles_very_large_input(self):
        """System should handle arbitrarily large inputs without error."""
        config = ContextConfig(
            compaction_threshold_tokens=1000,
            summarization_threshold_tokens=1000,
        )
        mgr = ContextManager(config=config)
        aux = make_mock_aux(max_context_tokens=15_000)

        # ~100 messages of varied content (compressible x-runs would
        # undercount; words behave like real conversations)
        messages = []
        for i in range(100):
            content = " ".join(f"w{i}_{j}" for j in range(150))
            messages.append(
                HumanMessage(content=content)
                if i % 2 == 0
                else AIMessage(content=content)
            )

        # Should complete without error
        result = await mgr.summarize_conversation(messages=messages, auxiliary=aux)

        assert result
        assert "[Summarization failed:" not in result

    def test_config_defaults_are_safe(self):
        """Default config keeps the conservative 100k working-window base.

        Summarization budgets are no longer config leaves — they derive from
        the auxiliary model's window at call time (src/core/summarizer.py).
        """
        config = ContextConfig()
        assert config.model_max_context_tokens == 100_000
        assert not hasattr(config, "summarization_safe_limit")
        assert not hasattr(config, "summarization_chunk_size")


# =============================================================================
# Tests for oversized-message backstop in summarize_and_compact
# =============================================================================


class TestOversizedMessageCompaction:
    """Backstop that catches runaway-generation poisoning.

    When a single AIMessage exceeds half the context window, it's almost
    certainly a repetition-loop artifact (the loader-side max_tokens
    fallback prevents new ones, but a session resumed from a poisoned
    state needs this rule to recover). Verify the substitution shape.
    """

    @pytest.mark.asyncio
    async def test_oversized_aimessage_replaced_with_stub(
        self, context_manager, mock_llm
    ):
        """A single oversized AIMessage should be substituted by a stub."""
        # context_manager has model_max_context_tokens=2000, threshold=1000.
        # Build an AIMessage exceeding the threshold (~6000 chars => ~1500 tokens).
        oversized_content = "loop " * 1500
        messages = [
            HumanMessage(content="hi", id="h1"),
            AIMessage(content=oversized_content, id="a1"),  # poisonous
            HumanMessage(content="still there?", id="h2"),
            AIMessage(content="yes", id="a2"),
            HumanMessage(content="ok", id="h3"),
            AIMessage(content="done", id="a3"),
        ]

        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        # The original oversized content must not survive into the new history.
        assert not any(
            isinstance(m, AIMessage) and oversized_content in (m.content or "")
            for m in result
        )
        # A stub describing the elided message must be present.
        stubs = [
            m
            for m in result
            if isinstance(m, AIMessage) and "elided by compaction" in (m.content or "")
        ]
        assert len(stubs) == 1, "expected exactly one elision stub"
        # The original ID must be present in removal markers so state reducer
        # actually evicts it from LangGraph state.
        from langchain_core.messages import RemoveMessage

        removed_ids = {m.id for m in result if isinstance(m, RemoveMessage)}
        assert "a1" in removed_ids

    @pytest.mark.asyncio
    async def test_aimessage_with_tool_calls_not_substituted(
        self, context_manager, mock_llm
    ):
        """AIMessages with tool_calls must NOT be substituted — would orphan ToolMessages."""
        oversized = "loop " * 1500  # over 1000-token threshold
        messages = [
            HumanMessage(content="hi", id="h1"),
            AIMessage(
                content=oversized,
                id="a1",
                tool_calls=[{"id": "t1", "name": "tool", "args": {}}],
            ),
            ToolMessage(content="result", tool_call_id="t1", id="t1"),
            HumanMessage(content="next", id="h2"),
            AIMessage(content="ok", id="a2"),
            HumanMessage(content="ok2", id="h3"),
        ]

        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        # No elision stub for the tool-bearing AIMessage.
        stubs = [
            m
            for m in result
            if isinstance(m, AIMessage) and "elided by compaction" in (m.content or "")
        ]
        assert stubs == [], "tool-call AIMessages must not be substituted"

    @pytest.mark.asyncio
    async def test_normal_messages_pass_through_unchanged(
        self, context_manager, mock_llm
    ):
        """Normal-sized messages should not trigger substitution."""
        messages = [
            HumanMessage(content="short", id="h1"),
            AIMessage(content="short reply", id="a1"),
            HumanMessage(content="another", id="h2"),
            AIMessage(content="another reply", id="a2"),
            HumanMessage(content="third", id="h3"),
            AIMessage(content="third reply", id="a3"),
        ]

        result = await context_manager.summarize_and_compact(
            messages=messages,
            auxiliary=mock_llm,
        )

        stubs = [
            m
            for m in result
            if isinstance(m, AIMessage) and "elided by compaction" in (m.content or "")
        ]
        assert stubs == [], "no substitution should occur for normal-sized messages"


# Tests for the compaction boundary marker that drives message-granular resume
# (knowledge-base/knowledge/issues/persistent_session_midturn_message_loss.md, Phase 3).
class TestCompactionBoundaryId:
    """summarize_and_compact records the id of the last message its summary
    covers, so the persistent transport can store a message-level boundary_seq."""

    @pytest.mark.asyncio
    async def test_records_boundary_at_last_summarized_message(
        self, context_manager, mock_llm
    ):
        # keep_recent_messages=3, so 8 conversation messages → summarize the
        # first 5 (no tool pairs to shift the safe slice), keep the last 3.
        # The boundary is the newest summarized message: index 4 → "m4".
        msgs = [
            HumanMessage(content=f"u{i} " + "x" * 500, id=f"m{i}")
            if i % 2 == 0
            else AIMessage(content=f"a{i} " + "y" * 500, id=f"m{i}")
            for i in range(8)
        ]
        result = await context_manager.summarize_and_compact(
            messages=msgs, auxiliary=mock_llm
        )
        # Compaction actually happened (summary present).
        assert any(
            isinstance(m, SystemMessage)
            and "[Summary of prior work]" in (m.content or "")
            for m in result
        )
        assert context_manager._last_compaction_boundary_id == "m4"

    @pytest.mark.asyncio
    async def test_boundary_is_none_when_no_compaction(self, context_manager, mock_llm):
        # Fewer messages than keep_recent → no-op compaction, boundary stays None
        # so the transport falls back to boundary_turn rather than a stale seq.
        msgs = [HumanMessage(content="hi", id="m0"), AIMessage(content="yo", id="m1")]
        await context_manager.summarize_and_compact(messages=msgs, auxiliary=mock_llm)
        assert context_manager._last_compaction_boundary_id is None

    @pytest.mark.asyncio
    async def test_boundary_reset_across_calls(self, context_manager, mock_llm):
        # A real compaction sets the boundary; a subsequent no-op resets it to
        # None (not left stale from the previous call).
        big = [
            HumanMessage(content=f"u{i} " + "x" * 500, id=f"m{i}")
            if i % 2 == 0
            else AIMessage(content=f"a{i} " + "y" * 500, id=f"m{i}")
            for i in range(8)
        ]
        await context_manager.summarize_and_compact(messages=big, auxiliary=mock_llm)
        assert context_manager._last_compaction_boundary_id == "m4"
        await context_manager.summarize_and_compact(
            messages=[HumanMessage(content="hi", id="x0")], auxiliary=mock_llm
        )
        assert context_manager._last_compaction_boundary_id is None


# =============================================================================
# Protected phase blocks across summarisation — U2 WP1
# =============================================================================


class TestPinnedAfterSummary:
    """A protected phase block that fell into the summarised region is
    re-seated right after the ``[Summary of prior work]`` message and before
    the kept window — for the CURRENT phase only. The context_manager
    fixture keeps 3 recent messages."""

    BODY = "PROTECTED PHASE BODY " * 20

    @classmethod
    def _block(cls, phase_key, msg_id="blk", path="skills/tactical-phase/SKILL.md"):
        block = create_phase_instruction_message(path, cls.BODY, "tactical", phase_key)
        block.id = msg_id  # what the reducer assigned when it was delivered
        return block

    @staticmethod
    def _base():
        msgs = []
        for i in range(8):
            msgs.append(HumanMessage(content=f"question {i} " + "x" * 200, id=f"h{i}"))
            msgs.append(AIMessage(content=f"answer {i} " + "y" * 200, id=f"a{i}"))
        return msgs

    @classmethod
    def _history(cls, block, block_at):
        msgs = cls._base()
        msgs.insert(block_at, block)
        return msgs

    @staticmethod
    def _split(result):
        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        removed = {m.id for m in result if isinstance(m, RemoveMessage)}
        summary_idx = next(
            i
            for i, m in enumerate(kept)
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        )
        return kept, removed, summary_idx

    @pytest.mark.asyncio
    async def test_current_phase_block_reinjected_after_summary_before_keep_window(
        self, context_manager, mock_llm
    ):
        block = self._block("2:tactical")
        messages = self._history(block, block_at=2)
        context_manager.set_current_phase("tactical", phase_key="2:tactical")

        result = await context_manager.summarize_and_compact(
            messages=messages, auxiliary=mock_llm
        )

        kept, removed, summary_idx = self._split(result)
        protected = [m for m in kept if is_protected_message(m)]
        assert len(protected) == 1
        reseated = protected[0]
        assert kept.index(reseated) == summary_idx + 1
        assert reseated.content == block.content
        assert reseated.additional_kwargs == block.additional_kwargs
        # Appended fresh (no id); the original is evicted by its id.
        assert reseated.id is None
        assert "blk" in removed
        # The kept window follows the block.
        assert [m.content for m in kept[summary_idx + 2 :]] == [
            m.content for m in messages[-3:]
        ]

    @pytest.mark.asyncio
    async def test_other_phase_blocks_are_summarised_away(
        self, context_manager, mock_llm
    ):
        stale = self._block("1:strategic", msg_id="stale")
        messages = self._history(stale, block_at=2)
        context_manager.set_current_phase("tactical", phase_key="2:tactical")

        result = await context_manager.summarize_and_compact(
            messages=messages, auxiliary=mock_llm
        )

        kept, removed, _ = self._split(result)
        assert not any(is_protected_message(m) for m in kept)
        assert "stale" in removed
        assert not any(self.BODY in str(m.content) for m in kept)

    @pytest.mark.asyncio
    async def test_block_in_keep_window_keeps_marker_and_is_not_duplicated(
        self, context_manager, mock_llm
    ):
        block = self._block("2:tactical")
        base = self._base()
        messages = base[:-1] + [block, base[-1]]  # window = [h7, block, a7]
        context_manager.set_current_phase("tactical", phase_key="2:tactical")

        result = await context_manager.summarize_and_compact(
            messages=messages, auxiliary=mock_llm
        )

        kept, removed, summary_idx = self._split(result)
        protected = [m for m in kept if is_protected_message(m)]
        assert len(protected) == 1
        assert protected[0].additional_kwargs == block.additional_kwargs
        # It stayed at its window position (after h7) — not pinned a second time.
        assert kept.index(protected[0]) == summary_idx + 2
        assert kept[summary_idx + 1].content == messages[-3].content
        assert "blk" in removed  # original evicted, fresh copy appended

    @pytest.mark.asyncio
    async def test_protected_text_absent_from_summary_input(
        self, context_manager, mock_llm
    ):
        block = self._block("2:tactical")
        messages = self._history(block, block_at=2)
        context_manager.set_current_phase("tactical", phase_key="2:tactical")

        parts = context_manager._format_messages_for_summary(messages)
        assert parts
        assert not any(self.BODY in p for p in parts)
        assert any("question 0" in p for p in parts)

        await context_manager.summarize_and_compact(
            messages=messages, auxiliary=mock_llm
        )
        structured = mock_llm.llm.with_structured_output.return_value
        assert structured.ainvoke.await_count >= 1
        sent = str(structured.ainvoke.call_args_list)
        assert self.BODY not in sent
        assert "question 0" in sent

    @pytest.mark.asyncio
    async def test_generic_pin_survives_any_phase(self, context_manager, mock_llm):
        pin = HumanMessage(
            content="GENERIC PIN", id="pin", additional_kwargs={PROTECTED_KEY: True}
        )
        messages = self._history(pin, block_at=2)
        context_manager.set_current_phase("tactical", phase_key="9:tactical")

        result = await context_manager.summarize_and_compact(
            messages=messages, auxiliary=mock_llm
        )

        kept, removed, summary_idx = self._split(result)
        assert kept[summary_idx + 1].content == "GENERIC PIN"
        assert is_protected_message(kept[summary_idx + 1])
        assert "pin" in removed

    def test_place_pinned_after_summary_dedupes_and_filters(self):
        summary = SystemMessage(content="[Summary of prior work]\nS")
        current = self._block("2:tactical", msg_id="c")
        duplicate = self._block("2:tactical", msg_id="d")
        stale = self._block("1:strategic", msg_id="s")
        summarized = [stale, current, duplicate, HumanMessage(content="old")]

        # The window already holds the identity: nothing is re-seated.
        window_block = self._block("2:tactical", msg_id="k")
        kept = [window_block, HumanMessage(content="w")]
        assert place_pinned_after_summary(summary, summarized, kept, "2:tactical") == [
            summary,
            *kept,
        ]

        # Otherwise exactly one fresh copy of the current block: id-less,
        # markers intact, the stale phase filtered out.
        kept = [HumanMessage(content="w")]
        out = place_pinned_after_summary(summary, summarized, kept, "2:tactical")
        assert len(out) == 3
        assert out[0] is summary
        assert out[2] is kept[0]
        assert is_protected_message(out[1])
        assert out[1].id is None
        assert protected_phase_key(out[1]) == "2:tactical"
        assert out[1].content == current.content


# =============================================================================
# preserve_message_identity — the persistent loop's compaction contract
# =============================================================================


class TestPreserveMessageIdentity:
    """``preserve_message_identity``: the persistent loop replaces its history
    wholesale (no ``add_messages`` reducer), so the kept window must keep its
    objects — ids and ``additional_kwargs`` such as the turn stamp — and a
    pinned message is re-seated as the original. The default mode keeps the
    id-less fresh copies the worker graph's reducer relies on.
    knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md
    """

    @staticmethod
    def _manager(context_config, preserve: bool) -> ContextManager:
        return ContextManager(
            config=context_config,
            model="gpt-4",
            preserve_message_identity=preserve,
        )

    @staticmethod
    def _history(n: int = 12):
        from shared.runtime.core.message_markers import stamp_turn_membership

        body = "The quick brown fox jumps over the lazy dog. " * 8
        messages = []
        for i in range(n):
            msg = (
                HumanMessage(content=f"question {i}: {body}", id=f"msg-{i}")
                if i % 2 == 0
                else AIMessage(content=f"answer {i}: {body}", id=f"msg-{i}")
            )
            messages.append(stamp_turn_membership(msg, 4))
        return messages

    @pytest.mark.asyncio
    async def test_kept_window_keeps_objects_ids_and_stamps(
        self, context_config, mock_llm
    ):
        from langchain_core.messages import RemoveMessage
        from shared.runtime.core.message_markers import turn_membership

        manager = self._manager(context_config, preserve=True)
        messages = self._history()
        result = await manager.summarize_and_compact(messages, mock_llm)

        assert manager.compaction_runs == 1
        tail = [m for m in result if not isinstance(m, (SystemMessage, RemoveMessage))]
        assert tail
        assert all(any(m is original for original in messages) for m in tail)
        assert all(m.id and turn_membership(m) == 4 for m in tail)
        # A marker for a surviving id would delete the message it means to keep.
        evicted = {m.id for m in result if isinstance(m, RemoveMessage)}
        assert not evicted & {m.id for m in tail}
        assert evicted  # the summarised region is still evicted

    @pytest.mark.asyncio
    async def test_default_mode_recreates_idless_fresh_copies(
        self, context_config, mock_llm
    ):
        from langchain_core.messages import RemoveMessage

        manager = self._manager(context_config, preserve=False)
        messages = self._history()
        result = await manager.summarize_and_compact(messages, mock_llm)

        tail = [m for m in result if not isinstance(m, (SystemMessage, RemoveMessage))]
        assert tail
        assert all(m.id is None for m in tail)
        assert not any(any(m is original for original in messages) for m in tail)

    @pytest.mark.asyncio
    async def test_pinned_turn_input_is_reseated_as_the_original(
        self, context_config, mock_llm
    ):
        from langchain_core.messages import RemoveMessage
        from shared.runtime.core.message_markers import (
            PROTECTED_KEY,
            pin_turn_input,
            unpin_turn_input,
        )

        manager = self._manager(context_config, preserve=True)
        messages = self._history()
        turn_input = messages[0]
        pin_turn_input(turn_input)

        result = await manager.summarize_and_compact(messages, mock_llm)

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        summary_at = next(
            i
            for i, m in enumerate(kept)
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        )
        assert kept[summary_at + 1] is turn_input
        assert turn_input.id == "msg-0"
        # The pin is the loop's, removed when the turn ends; other pins stay.
        unpin_turn_input(turn_input)
        assert PROTECTED_KEY not in turn_input.additional_kwargs
        other = HumanMessage(
            content="phase block", additional_kwargs={PROTECTED_KEY: True}
        )
        unpin_turn_input(other)
        assert other.additional_kwargs[PROTECTED_KEY] is True

    @staticmethod
    def _tool_turn(result: str):
        from shared.runtime.core.message_markers import stamp_turn_membership

        messages = [HumanMessage(content="read the rows", id="msg-in")]
        for i in range(3):
            messages.append(
                AIMessage(
                    content="",
                    id=f"msg-call-{i}",
                    tool_calls=[{"name": "read", "args": {}, "id": f"tc{i}"}],
                )
            )
            messages.append(
                ToolMessage(content=result, tool_call_id=f"tc{i}", id=f"msg-res-{i}")
            )
        return [stamp_turn_membership(m, 4) for m in messages]

    @pytest.mark.asyncio
    async def test_capped_keep_window_result_is_a_stamped_compaction_view(
        self, mock_llm
    ):
        """A capped copy is lossy: it keeps its id AND its turn stamp (the
        reconcile still owes the row if its incremental write was lost) and is
        marked a compaction view (so the reconcile writes it insert-if-absent,
        never over the durable full result)."""
        from shared.runtime.core.message_markers import (
            is_compaction_view,
            turn_membership,
        )

        config = ContextConfig(
            keep_recent_messages=2, keep_window_max_tool_result_chars=200
        )
        big = "row-data " * 80
        for preserve in (True, False):
            manager = self._manager(config, preserve=preserve)
            result = await manager.summarize_and_compact(self._tool_turn(big), mock_llm)
            tools = [m for m in result if isinstance(m, ToolMessage)]
            assert tools and all(m.content != big for m in tools)
            if preserve:
                assert all(m.id and turn_membership(m) == 4 for m in tools)
                assert all(is_compaction_view(m) for m in tools)
            else:
                # The worker graph's id-less fresh copies carry no marks.
                assert not any(is_compaction_view(m) for m in tools)

    def test_elided_and_shed_copies_are_stamped_compaction_views(self):
        from shared.runtime.core.message_markers import (
            is_compaction_view,
            stamp_turn_membership,
            turn_membership,
        )

        image = stamp_turn_membership(
            HumanMessage(
                content=[
                    {"type": "text", "text": "Image content from tool call tc0:"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64," + "Z" * 400},
                    },
                ],
                id="msg-image",
            ),
            4,
        )
        for preserve in (True, False):
            manager = self._manager(ContextConfig(), preserve=preserve)
            messages = self._tool_turn("row-data " * 4_000) + [image]
            result = manager._elide_largest_tool_results(messages, target_tokens=500)
            rewritten = [m for m in result if not any(m is o for o in messages)]
            assert {m.id for m in rewritten} >= {"msg-image", "msg-res-0"}
            if preserve:
                assert all(turn_membership(m) == 4 for m in rewritten)
                assert all(is_compaction_view(m) for m in rewritten)
            else:
                assert not any(is_compaction_view(m) for m in rewritten)
