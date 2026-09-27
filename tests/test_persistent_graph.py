"""Tests for src/persistent_graph.py — persistent interactive agent loop.

Covers: sentinel constants, TurnResult dataclass, run_persistent_loop (system
prompt insertion, input handling, cancellation, error handling, config extraction,
memory extraction trigger, auto-commit git), and _execute_turn (interrupt handling,
memory/knowledge retrieval, transient injection, context compaction, LLM streaming
with fallback, timeout, tool execution loop, VM upgrade detection).
"""

import asyncio
import logging
from types import SimpleNamespace
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.core.workspace_backend import WorkspaceUnavailableError

from agent.persistent_graph import (
    APPROVE_SENTINEL,
    DENY_SENTINEL,
    INTERRUPT_SENTINEL,
    PermissionOutcome,
    PersistentLoopCallbacks,
    TurnResult,
    _execute_turn,
    _inject_context_pairs,
    _injection_anchor_index,
    _maybe_estimate_reasoning_tokens,
    run_persistent_loop,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _prompt_body(content: str) -> str:
    """The system prompt minus its trailing current-date line.

    run_persistent_loop stamps every system message with a date (see
    ``src/core/loader.with_current_date``), so prompt-identity assertions
    compare bodies rather than whole strings.
    """
    return "\n".join(
        line for line in content.split("\n") if not line.startswith("Current date:")
    ).rstrip()


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    """Build a PersistentLoopCallbacks with sane defaults (all no-op)."""
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_execution_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    """Preserve retry attempts while removing production wall-clock delays.

    Retry timing itself is covered in test_session_transient_llm_retry.py; this
    module tests turn outcomes, fallback routing, and callback ordering.
    """
    monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)


def _make_config(**overrides):
    """Minimal config mock."""
    cfg = MagicMock()
    cfg.llm.timeout = overrides.get("llm_timeout", 600)
    cfg.memory.enabled = overrides.get("memory_enabled", False)
    cfg.memory.observer_interval = overrides.get("observer_interval", 5)
    cfg.context_management.max_summary_length = 10000
    return cfg


def _make_llm_response(content="Hi!", tool_calls=None):
    """Build an AIMessage suitable as an LLM response."""
    msg = AIMessage(content=content)
    if tool_calls is not None:
        msg.tool_calls = tool_calls
    return msg


def _make_streaming_llm(response: AIMessage):
    """Return a mock LLM whose astream yields the response as a single chunk."""
    llm = AsyncMock()
    llm.reasoning = None

    async def _astream(messages, **kw):
        yield response

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=response)
    return llm


def _make_tool(name="test_tool", result="tool result"):
    """Build a mock tool."""
    tool = MagicMock()
    tool.name = name
    tool.ainvoke = AsyncMock(return_value=result)
    return tool


# ---------------------------------------------------------------------------
# 1.1 Sentinel constants
# ---------------------------------------------------------------------------


class TestSentinelConstants:
    def test_sentinels_are_distinct(self):
        assert INTERRUPT_SENTINEL != APPROVE_SENTINEL
        assert APPROVE_SENTINEL != DENY_SENTINEL
        assert INTERRUPT_SENTINEL != DENY_SENTINEL

    def test_sentinels_are_strings(self):
        assert isinstance(INTERRUPT_SENTINEL, str)
        assert isinstance(APPROVE_SENTINEL, str)
        assert isinstance(DENY_SENTINEL, str)

    def test_sentinels_unlikely_user_input(self):
        """Sentinel values should not look like normal user input."""
        for s in (INTERRUPT_SENTINEL, APPROVE_SENTINEL, DENY_SENTINEL):
            assert s.startswith("__") and s.endswith("__")


# ---------------------------------------------------------------------------
# 1.2 TurnResult dataclass
# ---------------------------------------------------------------------------


class TestTurnResult:
    def test_required_fields(self):
        tr = TurnResult(turn_id=1, messages_added=2, tool_calls_made=3)
        assert tr.turn_id == 1
        assert tr.messages_added == 2
        assert tr.tool_calls_made == 3

    def test_interrupted_defaults_false(self):
        tr = TurnResult(turn_id=1, messages_added=0, tool_calls_made=0)
        assert tr.interrupted is False

    def test_error_defaults_none(self):
        tr = TurnResult(turn_id=1, messages_added=0, tool_calls_made=0)
        assert tr.error is None

    def test_custom_interrupted_and_error(self):
        tr = TurnResult(
            turn_id=5,
            messages_added=1,
            tool_calls_made=0,
            interrupted=True,
            error="timeout",
        )
        assert tr.interrupted is True
        assert tr.error == "timeout"


# ---------------------------------------------------------------------------
# 1.3 run_persistent_loop — system prompt insertion
# ---------------------------------------------------------------------------


class TestRunPersistentLoopSystemPrompt:
    @pytest.mark.asyncio
    async def test_inserts_system_message_when_messages_empty(self):
        """System prompt inserted at index 0 when messages list is empty."""
        messages: List[BaseMessage] = []
        response = _make_llm_response("Hello!")

        # get_user_input returns one message then cancels
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(response)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="You are helpful.",
            callbacks=callbacks,
            messages=messages,
        )

        assert isinstance(messages[0], SystemMessage)
        assert _prompt_body(messages[0].content) == "You are helpful."

    @pytest.mark.asyncio
    async def test_inserts_system_message_when_first_is_not_system(self):
        """Inserts system prompt when first message is HumanMessage."""
        messages: List[BaseMessage] = [HumanMessage(content="existing")]
        response = _make_llm_response("Hi!")

        async def _input():
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(response)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="system",
            callbacks=callbacks,
            messages=messages,
        )

        assert isinstance(messages[0], SystemMessage)
        assert _prompt_body(messages[0].content) == "system"

    @pytest.mark.asyncio
    async def test_does_not_insert_when_system_message_present(self):
        """No duplicate system message when one is already present."""
        existing_sys = SystemMessage(content="original system")
        messages: List[BaseMessage] = [existing_sys]

        async def _input():
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(_make_llm_response())

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="new system",
            callbacks=callbacks,
            messages=messages,
        )

        assert messages[0] is existing_sys
        assert messages[0].content == "original system"

    @pytest.mark.asyncio
    async def test_live_rebuild_refreshes_messages0_in_place(self):
        """get_current_system_prompt re-read reaches messages[0] each turn.

        Live tool-group toggles rebuild session.system_prompt; without the
        per-turn re-read the model keeps seeing the attach-time prompt until
        the next detach/attach (live_session_settings.md P0.1).
        """
        existing_sys = SystemMessage(content="attach-time prompt")
        messages: List[BaseMessage] = [existing_sys]

        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(_make_llm_response())

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="attach-time prompt",
            callbacks=callbacks,
            messages=messages,
            get_current_system_prompt=lambda: "rebuilt prompt",
        )

        # Mutated in place — same object (persistence identity), new content.
        assert messages[0] is existing_sys
        assert _prompt_body(messages[0].content) == "rebuilt prompt"

    @pytest.mark.asyncio
    async def test_empty_rebuilt_prompt_does_not_clobber_messages0(self):
        """An empty re-read result must never blank the live prompt."""
        existing_sys = SystemMessage(content="attach-time prompt")
        messages: List[BaseMessage] = [existing_sys]

        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(_make_llm_response())

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="attach-time prompt",
            callbacks=callbacks,
            messages=messages,
            get_current_system_prompt=lambda: "",
        )

        assert _prompt_body(messages[0].content) == "attach-time prompt"

    @pytest.mark.asyncio
    async def test_system_prompt_carries_current_date(self):
        """Every turn's system message states today's date and weekday.

        Without it the agent cannot resolve "today" and reaches for a shell to
        run `date` — which the lite workspace tiers do not have.
        """
        from datetime import datetime, timezone

        messages: List[BaseMessage] = []
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="You are helpful.",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=messages,
        )

        now = datetime.now(timezone.utc)
        assert f"Current date: {now:%Y-%m-%d}" in messages[0].content
        # The weekday is the load-bearing part — "open on Sundays" is
        # unanswerable from the calendar date alone.
        assert f"{now:%A}" in messages[0].content

    @pytest.mark.asyncio
    async def test_stale_date_refreshed_on_next_turn(self):
        """A session outliving its creation day gets the date re-stamped.

        Threads here run for weeks; a date baked in at setup would have the
        agent answering "today" questions against session-creation day.
        """
        from datetime import datetime, timezone

        stale = "Current date: 1999-01-01 (Friday, UTC)"
        existing_sys = SystemMessage(content=f"attach-time prompt\n\n{stale}")
        messages: List[BaseMessage] = [existing_sys]

        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="attach-time prompt",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=messages,
        )

        # Same object (persistence identity), stale date gone, body intact.
        assert messages[0] is existing_sys
        assert stale not in messages[0].content
        assert f"Current date: {datetime.now(timezone.utc):%Y-%m-%d}" in (
            messages[0].content
        )
        assert _prompt_body(messages[0].content) == "attach-time prompt"

    @pytest.mark.asyncio
    async def test_date_refresh_does_not_displace_trailing_floor(self):
        """The managed product guide stays the tail of the system message.

        The date line is rewritten in place, not moved to the end — the guide
        is deliberately last so it sits closest to the current turn.
        """
        floor = "</managed_product_guide>"
        existing_sys = SystemMessage(
            content=f"body\n\nCurrent date: 1999-01-01 (Friday, UTC)\n\n{floor}"
        )
        messages: List[BaseMessage] = [existing_sys]

        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "hi"
            raise asyncio.CancelledError

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="body",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=messages,
        )

        assert messages[0].content.rstrip().endswith(floor)
        assert "1999-01-01" not in messages[0].content


# ---------------------------------------------------------------------------
# 1.3 run_persistent_loop — input handling
# ---------------------------------------------------------------------------


class TestRunPersistentLoopInputHandling:
    @pytest.mark.asyncio
    async def test_interrupt_sentinel_skips_turn(self):
        """INTERRUPT_SENTINEL should not increment turn_count or fire on_turn_start."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return INTERRUPT_SENTINEL
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

        callbacks.on_turn_start.assert_not_called()
        callbacks.on_turn_complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_valid_input_appends_human_message(self):
        """Valid input appends a HumanMessage before calling _execute_turn."""
        messages: List[BaseMessage] = []
        response = _make_llm_response("reply")
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "user question"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        llm = _make_streaming_llm(response)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=messages,
        )

        human_msgs = [m for m in messages if isinstance(m, HumanMessage)]
        assert len(human_msgs) == 1
        assert human_msgs[0].content == "user question"

    @pytest.mark.asyncio
    async def test_on_turn_start_called_before_execution(self):
        """on_turn_start fires with turn_id = 1 on first valid input."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "go"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

        callbacks.on_turn_start.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_on_turn_complete_called_after_execution(self):
        """on_turn_complete fires with the turn_id."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "go"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

        callbacks.on_turn_complete.assert_called_once()
        assert callbacks.on_turn_complete.call_args[0][0] == 1


# ---------------------------------------------------------------------------
# 1.3 run_persistent_loop — cancellation
# ---------------------------------------------------------------------------


class TestRunPersistentLoopCancellation:
    @pytest.mark.asyncio
    async def test_cancelled_during_get_user_input_exits_cleanly(self):
        """CancelledError during get_user_input exits without raising."""
        callbacks = _make_callbacks(
            get_user_input=AsyncMock(side_effect=asyncio.CancelledError),
        )

        # Should return without raising
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

    @pytest.mark.asyncio
    async def test_cancelled_during_execute_turn_exits_cleanly(self):
        """CancelledError during _execute_turn exits loop."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "go"
            raise asyncio.CancelledError

        # LLM that raises CancelledError
        llm = AsyncMock()
        llm.reasoning = None

        async def _astream(messages, **kw):
            raise asyncio.CancelledError
            yield  # make it a generator

        llm.astream = _astream

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )


# ---------------------------------------------------------------------------
# 1.3 run_persistent_loop — error handling
# ---------------------------------------------------------------------------


class TestRunPersistentLoopErrorHandling:
    @pytest.mark.asyncio
    async def test_execute_turn_error_calls_on_error_and_continues(self):
        """Non-cancellation error in _execute_turn calls on_error, loop continues."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return "go"
            raise asyncio.CancelledError

        # LLM that raises a runtime error on first call
        llm = AsyncMock()
        llm.reasoning = None
        invocation = 0

        async def _astream(messages, **kw):
            nonlocal invocation
            invocation += 1
            raise RuntimeError("LLM broke")
            yield  # make it a generator

        llm.astream = _astream

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

        callbacks.on_error.assert_called_once()
        assert "LLM broke" in callbacks.on_error.call_args[0][0]
        # on_turn_complete should still fire after error
        callbacks.on_turn_complete.assert_called_once()
        assert callbacks.on_turn_complete.call_args[0][0] == 1

    @pytest.mark.asyncio
    async def test_on_turn_complete_fires_after_error(self):
        """on_turn_complete fires even when _execute_turn raises."""
        call_count = 0

        async def _input():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "go"
            raise asyncio.CancelledError

        llm = AsyncMock()
        llm.reasoning = None

        async def _astream(messages, **kw):
            raise ValueError("bad")
            yield

        llm.astream = _astream
        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

        callbacks.on_turn_complete.assert_called_once()
        assert callbacks.on_turn_complete.call_args[0][0] == 1


# ---------------------------------------------------------------------------
# 1.3 run_persistent_loop — config extraction
# ---------------------------------------------------------------------------


class TestConfigExtraction:
    @pytest.mark.asyncio
    async def test_llm_timeout_defaults_to_600_when_missing(self):
        """When config.llm.timeout is missing, defaults to 600."""
        config = MagicMock()
        del config.llm.timeout  # force getattr fallback

        callbacks = _make_callbacks(
            get_user_input=AsyncMock(side_effect=asyncio.CancelledError),
        )

        # Should not raise
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=config,
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

    @pytest.mark.asyncio
    async def test_llm_timeout_defaults_to_600_when_none(self):
        """When config.llm.timeout is None, defaults to 600."""
        config = _make_config(llm_timeout=None)

        callbacks = _make_callbacks(
            get_user_input=AsyncMock(side_effect=asyncio.CancelledError),
        )

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=config,
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )

    @pytest.mark.asyncio
    async def test_extraction_interval_defaults_when_memory_none(self):
        """When config.memory is None, extraction_interval defaults to 5."""
        config = MagicMock()
        config.llm.timeout = 600
        config.memory = None
        config.context_management.max_summary_length = 10000

        callbacks = _make_callbacks(
            get_user_input=AsyncMock(side_effect=asyncio.CancelledError),
        )

        # Should not raise
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=config,
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
        )


# ---------------------------------------------------------------------------
# 1.4 Memory extraction trigger
# ---------------------------------------------------------------------------


class TestMemoryExtractionTrigger:
    @pytest.mark.asyncio
    async def test_fires_when_all_conditions_met(self):
        """Memory extraction fires at the right interval."""
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _make_config(observer_interval=5)
        llm = _make_streaming_llm(_make_llm_response("ok"))

        mock_recall = MagicMock()
        mock_recall.decrement_ttl = AsyncMock()
        mock_recall.retrieve = AsyncMock(return_value=[])
        mock_aux = MagicMock()

        with patch(
            "shared.runtime.services.auxiliary.extract_and_store_memories",
            new_callable=AsyncMock,
        ) as extract:
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=config,
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                recall_store=mock_recall,
                auxiliary_llm=mock_aux,
            )

            # It fires once (at turn 5) and is awaited before turn settlement;
            # a detached task could outlive and inherit a repointed lease.
            extract.assert_awaited_once()
            assert extract.await_args.kwargs["source_turn_start"] == 0
            assert extract.await_args.kwargs["source_turn_end"] == 5

    @pytest.mark.asyncio
    async def test_turn_settled_waits_for_workspace_push_and_mapping(self):
        """Physical detach cannot cancel between Git commit and mapping."""

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")
        stream_count = 0

        async def _astream(messages, **_kwargs):
            nonlocal stream_count
            stream_count += 1
            yield response_with_tool if stream_count == 1 else final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        tool = _make_tool("test_tool", "result")
        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.return_value = True
        git_mgr.has_unpushed_commits.return_value = True
        git_mgr.push.return_value = True
        git_mgr.get_current_commit.return_value = "a" * 40
        tool_ctx = MagicMock()
        tool_ctx.workspace_manager.git_manager = git_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "make a file"
            raise asyncio.CancelledError

        mapping_entered = asyncio.Event()
        release_mapping = asyncio.Event()
        settled = AsyncMock()

        async def _map(_sha):
            mapping_entered.set()
            await release_mapping.wait()

        callbacks = _make_callbacks(
            get_user_input=_input,
            on_workspace_commit=AsyncMock(side_effect=_map),
            on_turn_settled=settled,
        )
        loop_task = asyncio.create_task(
            run_persistent_loop(
                llm_with_tools=llm,
                tools=[tool],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_make_config(),
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                tool_context=tool_ctx,
            )
        )

        await asyncio.wait_for(mapping_entered.wait(), timeout=2)
        callbacks.on_turn_complete.assert_awaited_once()
        git_mgr.push.assert_called_once()
        settled.assert_not_awaited()

        release_mapping.set()
        await asyncio.wait_for(loop_task, timeout=2)
        settled.assert_awaited_once_with(1)

    @pytest.mark.asyncio
    async def test_does_not_fire_when_recall_store_none(self):
        """No extraction when recall_store is None."""
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _make_config(observer_interval=5)
        llm = _make_streaming_llm(_make_llm_response("ok"))

        with patch("agent.persistent_graph.asyncio.create_task") as mock_task:
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=config,
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                recall_store=None,
                auxiliary_llm=MagicMock(),
            )

            mock_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_fire_when_auxiliary_llm_none(self):
        """No extraction when auxiliary_llm is None."""
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _make_config(observer_interval=5)
        llm = _make_streaming_llm(_make_llm_response("ok"))

        with patch("agent.persistent_graph.asyncio.create_task") as mock_task:
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=config,
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                recall_store=MagicMock(),
                auxiliary_llm=None,
            )

            mock_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_fire_when_interval_zero(self):
        """No extraction when extraction_interval is 0."""
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _make_config(observer_interval=0)
        llm = _make_streaming_llm(_make_llm_response("ok"))

        with patch("agent.persistent_graph.asyncio.create_task") as mock_task:
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=config,
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                recall_store=MagicMock(),
                auxiliary_llm=MagicMock(),
            )

            mock_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_extraction_failure_is_non_fatal(self):
        """Import error during memory extraction doesn't crash the loop."""
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _make_config(observer_interval=5)
        llm = _make_streaming_llm(_make_llm_response("ok"))

        mock_recall = MagicMock()
        mock_recall.decrement_ttl = AsyncMock()
        mock_recall.retrieve = AsyncMock(return_value=[])

        with patch(
            "agent.persistent_graph.asyncio.create_task",
            side_effect=RuntimeError("task creation failed"),
        ):
            # Should not raise
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=config,
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                recall_store=mock_recall,
                auxiliary_llm=MagicMock(),
            )


# ---------------------------------------------------------------------------
# 1.5 Auto-commit git logic
# ---------------------------------------------------------------------------


class TestAutoCommitGit:
    @pytest.mark.asyncio
    async def test_commit_fires_when_tool_calls_made(self):
        """Git commit fires after a turn with tool calls."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("Done!")

        call_count = 0

        async def _astream(messages, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "result")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "do something"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[tool],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

        git_mgr.commit.assert_called_once()
        assert "turn 1" in git_mgr.commit.call_args[0][0]

    @pytest.mark.asyncio
    async def test_no_commit_when_no_tool_calls(self):
        """Git commit does NOT fire when turn had zero tool calls."""
        response = _make_llm_response("Just text, no tools")
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "hello"
            raise asyncio.CancelledError

        git_mgr = MagicMock()
        git_mgr.is_active = True

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(response),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

        git_mgr.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_fires_every_committing_turn_not_throttled(self):
        """Push fires on a normal turn even when turn_count is not a multiple
        of 5 — the old every-5th-turn throttle is gone."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        stream_count = 0

        async def _astream(messages, **kw):
            nonlocal stream_count
            stream_count += 1
            if stream_count % 2 == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "r")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.has_unpushed_commits.return_value = True
        git_mgr.push.return_value = True

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "turn 1"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[tool],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

        # Turn 1 is not a multiple of 5, but the commit must still be pushed.
        git_mgr.push.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_fires_on_no_tool_turn_when_commits_unpushed(self):
        """Regression: a session ending on a no-tool turn must still flush
        previously-committed-but-unpushed work to the remote."""
        response = _make_llm_response("Just text, no tools")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_unpushed_commits.return_value = True
        git_mgr.push.return_value = True

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "thanks!"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(response),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

        # No tools this turn, but unpushed commits exist → must still push.
        git_mgr.push.assert_called_once()
        git_mgr.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_push_when_nothing_unpushed(self):
        """Push is skipped (no needless network round-trip) when the branch has
        no unpushed commits — even across a 5th turn the old throttle pushed on."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        stream_count = 0

        async def _astream(messages, **kw):
            nonlocal stream_count
            stream_count += 1
            if stream_count % 2 == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "r")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = False
        git_mgr.has_unpushed_commits.return_value = False

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns <= 5:
                return f"turn {turns}"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[tool],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

        git_mgr.push.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_failure_is_logged_as_warning(self, caplog):
        """A failed push is surfaced (warning), not silently swallowed — the
        commits remain only on the workspace pod until a later push succeeds."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        stream_count = 0

        async def _astream(messages, **kw):
            nonlocal stream_count
            stream_count += 1
            if stream_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "r")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = False
        git_mgr.has_unpushed_commits.return_value = True
        git_mgr.push.return_value = False  # push fails

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "go"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        with caplog.at_level(logging.WARNING):
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[tool],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_make_config(),
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                tool_context=tool_ctx,
            )

        assert "push failed" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_git_exception_is_non_fatal(self):
        """Git failure doesn't crash the loop."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        stream_count = 0

        async def _astream(messages, **kw):
            nonlocal stream_count
            stream_count += 1
            if stream_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "r")

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.side_effect = RuntimeError("git broke")

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        ws_mgr.read_file.return_value = ""

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr
        tool_ctx.consume_freeze_request.return_value = None

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "go"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        # Should not raise
        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[tool],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=tool_ctx,
        )

    @pytest.mark.asyncio
    async def test_no_crash_when_tool_context_none(self):
        """Auto-commit section doesn't crash when tool_context is None."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        stream_count = 0

        async def _astream(messages, **kw):
            nonlocal stream_count
            stream_count += 1
            if stream_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "r")
        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "go"
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)

        # tool_context=None — should not raise
        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[tool],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            tool_context=None,
        )


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — interrupt handling
# ---------------------------------------------------------------------------


class TestExecuteTurnInterrupt:
    @pytest.mark.asyncio
    async def test_interrupt_returns_interrupted_result(self):
        """check_interrupt() returning True at top of loop returns interrupted."""
        callbacks = _make_callbacks(check_interrupt=MagicMock(return_value=True))
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.interrupted is True

    @pytest.mark.asyncio
    async def test_interrupt_preserves_accumulated_counts(self):
        """Interrupted result preserves messages_added and tool_calls_made."""
        # First LLM call returns tool call, then interrupt fires
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )

        interrupt_after_tool = [False]

        def _check_interrupt():
            return interrupt_after_tool[0]

        call_count = [0]

        async def _astream(messages, **kw):
            call_count[0] += 1
            yield response_with_tool

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("test_tool", "result")
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        callbacks = _make_callbacks(
            check_interrupt=_check_interrupt,
        )

        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        # After first tool execution, set interrupt
        original_on_tool_result = callbacks.on_tool_result

        async def _on_tool_result(name, result, tcid, is_error=False):
            interrupt_after_tool[0] = True
            await original_on_tool_result(name, result, tcid, is_error=is_error)

        callbacks.on_tool_result = _on_tool_result

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"test_tool": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.interrupted is True
        assert result.tool_calls_made >= 1
        assert result.messages_added >= 1


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — incremental persistence (Phase 2)
# ---------------------------------------------------------------------------


class TestExecuteTurnIncrementalPersistence:
    """persist_message must fire for each message the instant it's produced, so
    a mid-turn crash keeps the tail (knowledge-base/knowledge/issues/...midturn_message_loss.md)."""

    def _sequenced_llm(self, responses):
        """LLM whose astream yields a different response on each call."""
        idx = [0]

        async def _astream(messages, **kw):
            r = responses[min(idx[0], len(responses) - 1)]
            idx[0] += 1
            yield r

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        return llm

    @pytest.mark.asyncio
    async def test_persists_ai_and_tool_messages_as_produced(self):
        ai_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "test_tool", "args": {}, "id": "tc1"}],
        )
        ai_final = AIMessage(content="done")

        persisted = []
        callbacks = _make_callbacks(
            persist_message=AsyncMock(side_effect=lambda m: persisted.append(m))
        )
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=self._sequenced_llm([ai_with_tool, ai_final]),
            tool_map={"test_tool": _make_tool("test_tool", "result")},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        # AIMessage(tool call) → ToolMessage(result) → AIMessage(final),
        # each persisted the moment it entered history.
        assert [getattr(m, "type", None) for m in persisted] == ["ai", "tool", "ai"]
        assert persisted[0].tool_calls, "the AI step's tool calls must be persisted"
        assert persisted[1].tool_call_id == "tc1", "tool result keeps its link"
        # Every persisted message carries a stable id → reconciliation upserts.
        assert all(getattr(m, "id", None) for m in persisted)

    @pytest.mark.asyncio
    async def test_no_persist_callback_is_a_noop(self):
        """Back-compat: a turn runs fine when the transport wires no
        persist_message (callback defaults to None)."""
        callbacks = _make_callbacks()  # no persist_message
        assert callbacks.persist_message is None
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]
        result = await _execute_turn(
            llm_with_tools=self._sequenced_llm([AIMessage(content="hi")]),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )
        assert result.messages_added >= 1


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — memory retrieval
# ---------------------------------------------------------------------------


class TestExecuteTurnMemoryRetrieval:
    @pytest.mark.asyncio
    async def test_memory_retrieval_uses_last_human_message(self):
        """Context text is extracted from the last HumanMessage."""
        recall = MagicMock()
        recall.decrement_ttl = AsyncMock()
        recall.retrieve = AsyncMock(return_value=[])

        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="first"),
            HumanMessage(content="second question"),
        ]

        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            recall_store=recall,
        )

        recall.retrieve.assert_called_once_with("second question")

    @pytest.mark.asyncio
    async def test_memory_retrieval_timeout_is_non_fatal(self):
        """Timeout on recall_store.retrieve doesn't crash the turn."""
        recall = MagicMock()
        recall.decrement_ttl = AsyncMock()
        recall.retrieve = AsyncMock(side_effect=asyncio.TimeoutError)

        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            recall_store=recall,
        )

        assert result.error is None

    @pytest.mark.asyncio
    async def test_memory_retrieval_exception_is_non_fatal(self):
        """Exception on recall_store.retrieve doesn't crash the turn."""
        recall = MagicMock()
        recall.decrement_ttl = AsyncMock()
        recall.retrieve = AsyncMock(side_effect=RuntimeError("db down"))

        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            recall_store=recall,
        )

        assert result.error is None

    @pytest.mark.asyncio
    async def test_decrement_ttl_timeout_does_not_block_retrieval(self):
        """TTL decrement timeout doesn't prevent retrieval from running."""
        recall = MagicMock()
        recall.decrement_ttl = AsyncMock(side_effect=asyncio.TimeoutError)
        recall.retrieve = AsyncMock(return_value=[])

        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            recall_store=recall,
        )

        # retrieve should still be called even after TTL timeout
        recall.retrieve.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_context_when_no_human_message(self):
        """Empty string used as context if no HumanMessage found."""
        recall = MagicMock()
        recall.decrement_ttl = AsyncMock()
        recall.retrieve = AsyncMock(return_value=[])

        messages = [SystemMessage(content="sys")]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            recall_store=recall,
        )

        recall.retrieve.assert_called_once_with("")

    @pytest.mark.asyncio
    async def test_managed_guide_boundary_reaches_tail_injected_memory(self):
        """The trusted catalog/reader pair reaches the actual LLM request.

        Regression for a resumed, compacted session whose recalled workspace
        memory followed the current product question and caused the model to
        reuse an earlier answer without calling the current guide.
        """
        from shared.runtime.core.skill_resolution import (
            APP_GUIDE_LOADER_TOOL,
            APP_GUIDE_SKILL,
            add_persistent_system_skills,
        )

        catalog = add_persistent_system_skills({})
        digest = next(
            item["bundle_digest"]
            for item in catalog["menu"]
            if item["name"] == APP_GUIDE_SKILL
        )
        config = _make_config()
        config.extra = {"_resolved_skills": catalog}

        recall = MagicMock()
        recall.decrement_ttl = AsyncMock()
        recall.retrieve = AsyncMock(return_value=[MagicMock()])
        captured = []

        with patch(
            "shared.runtime.services.recall_store.RecallStore.assemble_memory_block",
            return_value="HISTORICAL WORKSPACE MEMORY",
        ):
            await _execute_turn(
                llm_with_tools=_capturing_llm(
                    captured,
                    _make_llm_response("done"),
                ),
                tool_map={APP_GUIDE_LOADER_TOOL: MagicMock()},
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(
                        side_effect=lambda messages, *args, **kwargs: messages
                    )
                ),
                messages=[
                    SystemMessage(content="current system prompt"),
                    HumanMessage(content="What can this SRW session do?"),
                ],
                callbacks=_make_callbacks(),
                llm_timeout=600,
                auxiliary_llm=None,
                config=config,
                recall_store=recall,
            )

        memory_result = next(
            message
            for message in captured[0]
            if isinstance(message, ToolMessage)
            and str(message.tool_call_id).startswith("memory_inject_")
        )
        assert "HISTORICAL WORKSPACE MEMORY" in memory_result.content
        assert "<managed_product_guide_turn_boundary" not in memory_result.content
        boundary = captured[0][-1]
        assert isinstance(boundary, HumanMessage)
        assert "<managed_product_guide_turn_boundary" in boundary.content
        assert digest in boundary.content
        assert "must call `read_product_guide` now" in boundary.content


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — knowledge retrieval
# ---------------------------------------------------------------------------


class TestExecuteTurnKnowledgeRetrieval:
    @pytest.mark.asyncio
    async def test_bound_kbs_use_chunk_search_and_source_aware_injection(self):
        """Datasource bindings take the chunk-only multi-KB path."""
        import uuid
        from types import SimpleNamespace

        from agent.services.knowledge.bindings import KnowledgeBinding
        from shared.runtime.services.knowledge_store import KnowledgeRecord

        native = KnowledgeBinding(
            kb_id=uuid.uuid4(),
            alias="project",
            name="Project Knowledge",
            kind="native",
            writable=True,
        )
        docs = KnowledgeBinding(
            kb_id=uuid.uuid4(),
            alias="docs",
            name="Docs",
            kind="datasource",
            writable=False,
            indexed_commit="dispatch-head",
        )
        ks = MagicMock()
        ks.embedding_service.model = "test-embed"
        ks.embedding_service.expected_dimensions = 16

        async def _search_chunks(**kwargs):
            binding = native if kwargs["kb_ids"] == [native.kb_id] else docs
            prefix = "native" if binding.is_native else "external"
            count = 3 if binding.is_native else 2
            return [
                KnowledgeRecord(
                    note_id=f"{prefix}-{index}",
                    kb_id=binding.kb_id,
                    title=f"{prefix.title()} {index}",
                    note_type="learning",
                    content="context",
                )
                for index in range(count)
            ]

        ks.search_chunks = AsyncMock(side_effect=_search_chunks)
        ks.get_watermark = AsyncMock(
            return_value=SimpleNamespace(indexed_commit="indexed-head")
        )
        ks.hybrid_search = AsyncMock()

        captured = []

        async def _astream(messages, **kwargs):
            captured.append(list(messages))
            yield AIMessage(content="ok")

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        context = MagicMock()
        context.knowledge_bindings = [native, docs]
        context.citation_engine = None
        context.consume_freeze_request.return_value = None
        config = _make_config()
        config.llm.model = None

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=[SystemMessage(content="sys"), HumanMessage(content="question")],
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
            knowledge_store=ks,
            tool_context=context,
        )

        assert ks.search_chunks.await_count == 2
        ks.hybrid_search.assert_not_awaited()
        injected = "\n".join(
            str(message.content)
            for message in captured[0]
            if isinstance(message, ToolMessage)
        )
        assert "[project] project:native-0" in injected
        assert "[docs] docs:external-0" in injected
        assert "External snapshots (as of): [docs] indexed-head" in injected

    @pytest.mark.asyncio
    async def test_knowledge_search_called_with_project_ids(self):
        """hybrid_search called with project_ids as UUIDs."""
        ks = MagicMock()
        ks.hybrid_search = AsyncMock(return_value=[])

        messages = [SystemMessage(content="sys"), HumanMessage(content="what auth?")]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            knowledge_store=ks,
            project_id="550e8400-e29b-41d4-a716-446655440000",
        )

        ks.hybrid_search.assert_called_once()
        call_kwargs = ks.hybrid_search.call_args[1]
        assert call_kwargs["match_count"] == 5
        assert call_kwargs["query"] == "what auth?"

    @pytest.mark.asyncio
    async def test_knowledge_not_called_without_project_id(self):
        """No knowledge search when project_id is None and project_ids empty."""
        ks = MagicMock()
        ks.hybrid_search = AsyncMock(return_value=[])

        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            knowledge_store=ks,
            project_id=None,
        )

        ks.hybrid_search.assert_not_called()

    @pytest.mark.asyncio
    async def test_knowledge_not_called_without_store(self):
        """No knowledge search when knowledge_store is None."""
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        # Should not raise even with project_id set
        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            knowledge_store=None,
            project_id="550e8400-e29b-41d4-a716-446655440000",
        )

        assert result.error is None

    @pytest.mark.asyncio
    async def test_knowledge_timeout_is_non_fatal(self):
        """Timeout on knowledge retrieval is non-fatal."""
        ks = MagicMock()
        ks.hybrid_search = AsyncMock(side_effect=asyncio.TimeoutError)

        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            knowledge_store=ks,
            project_id="550e8400-e29b-41d4-a716-446655440000",
        )

        assert result.error is None

    @pytest.mark.asyncio
    async def test_knowledge_uses_project_ids_list(self):
        """project_ids takes priority over project_id."""
        ks = MagicMock()
        ks.hybrid_search = AsyncMock(return_value=[])

        messages = [SystemMessage(content="sys"), HumanMessage(content="q")]
        callbacks = _make_callbacks()

        pid1 = "550e8400-e29b-41d4-a716-446655440000"
        pid2 = "660e8400-e29b-41d4-a716-446655440001"

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            knowledge_store=ks,
            project_ids=[pid1, pid2],
        )

        ks.hybrid_search.assert_called_once()
        call_kwargs = ks.hybrid_search.call_args[1]
        assert len(call_kwargs["project_ids"]) == 2


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — transient injection
# ---------------------------------------------------------------------------


class TestTransientInjection:
    @pytest.mark.asyncio
    async def test_prepared_is_copy_not_original(self):
        """Transient injections go into a copy, not the original messages list."""
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        # Original messages should have gained the AI response but NOT workspace injection
        ws_messages = [
            m
            for m in messages
            if isinstance(m, SystemMessage) and "<workspace_memory>" in m.content
        ]
        assert len(ws_messages) == 0


# ---------------------------------------------------------------------------
# 1.5b _execute_turn — citation-verification feedback (Phase 2b / D4, persistent
# parity with the worker's _inject_transient_messages)
# ---------------------------------------------------------------------------


def _failed_cit(cid=1, claim="The sky is green", notes="quote not found", score=0.1):
    """Fake failed-verification Citation (the attrs format_failed_citations reads)."""
    from types import SimpleNamespace

    return SimpleNamespace(
        id=cid, claim=claim, verification_notes=notes, similarity_score=score
    )


def _capturing_llm(captured, response):
    """LLM mock that records the messages list passed to each astream call."""
    llm = AsyncMock()
    llm.reasoning = None

    async def _astream(messages, **kw):
        captured.append(list(messages))
        yield response

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=response)
    return llm


class TestCitationFeedbackInjectionUnit:
    """Pure _inject_context_pairs coverage for the citation-feedback block."""

    def test_injects_citation_feedback_pair_after_other_context(self):
        from agent.core.citation_feedback_injection import (
            is_citation_feedback_injection_message,
        )

        prepared = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        added = _inject_context_pairs(
            prepared, [], "", "", "Automatic verification FAILED for [1]"
        )

        assert added == 2
        pair = [m for m in prepared if is_citation_feedback_injection_message(m)]
        assert len(pair) == 2  # synthetic AI tool-call + ToolMessage report
        tool_msgs = [m for m in prepared if isinstance(m, ToolMessage)]
        assert any("verification FAILED" in m.content for m in tool_msgs)
        # Anchored after the first user turn (provider turn-ordering safety).
        assert isinstance(prepared[0], SystemMessage)
        assert isinstance(prepared[1], HumanMessage)

    def test_empty_citation_block_is_noop(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        added = _inject_context_pairs(prepared, [], "", "", "")
        assert added == 0
        assert len(prepared) == 2


class TestExecuteTurnCitationFeedback:
    @pytest.mark.asyncio
    async def test_failed_citations_surfaced_to_llm(self):
        """A still-failed citation is queried once per turn and injected into the
        LLM input as a synthetic check_citation_verification pair."""
        from agent.core.citation_feedback_injection import (
            is_citation_feedback_injection_message,
        )

        engine = MagicMock()
        engine.list_citations = AsyncMock(return_value=[_failed_cit()])
        tool_ctx = MagicMock()
        tool_ctx.citation_engine = engine

        captured: List[List[BaseMessage]] = []
        messages = [SystemMessage(content="sys"), HumanMessage(content="cite this")]

        await _execute_turn(
            llm_with_tools=_capturing_llm(captured, _make_llm_response("done")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        engine.list_citations.assert_called_once_with(verification_status="failed")
        assert captured, "LLM was never called"
        pair = [m for m in captured[0] if is_citation_feedback_injection_message(m)]
        assert len(pair) == 2
        # Durable history must NOT carry the ephemeral injection.
        assert not any(is_citation_feedback_injection_message(m) for m in messages)

    @pytest.mark.asyncio
    async def test_no_engine_skips_query_and_injection(self):
        """No citation engine on the context → no query, no injection, no error."""
        from agent.core.citation_feedback_injection import (
            is_citation_feedback_injection_message,
        )

        tool_ctx = MagicMock()
        tool_ctx.citation_engine = None  # not yet created (no citation activity)

        captured: List[List[BaseMessage]] = []
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=_capturing_llm(captured, _make_llm_response("done")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.error is None
        assert captured
        assert not any(is_citation_feedback_injection_message(m) for m in captured[0])

    @pytest.mark.asyncio
    async def test_no_failed_citations_no_injection(self):
        """Engine present but nothing failed → no feedback pair."""
        from agent.core.citation_feedback_injection import (
            is_citation_feedback_injection_message,
        )

        engine = MagicMock()
        engine.list_citations = AsyncMock(return_value=[])
        tool_ctx = MagicMock()
        tool_ctx.citation_engine = engine

        captured: List[List[BaseMessage]] = []
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=_capturing_llm(captured, _make_llm_response("done")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        engine.list_citations.assert_called_once_with(verification_status="failed")
        assert not any(is_citation_feedback_injection_message(m) for m in captured[0])

    @pytest.mark.asyncio
    async def test_retrieval_timeout_is_non_fatal(self):
        """A timeout/error fetching failed citations must not kill the turn."""
        engine = MagicMock()
        engine.list_citations = AsyncMock(side_effect=asyncio.TimeoutError)
        tool_ctx = MagicMock()
        tool_ctx.citation_engine = engine

        captured: List[List[BaseMessage]] = []
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=_capturing_llm(captured, _make_llm_response("done")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.error is None


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — context compaction
# ---------------------------------------------------------------------------


def _make_compacting_ctx_mgr(result_fn):
    """Context-manager mock whose ensure_within_limits simulates a
    *successful* summarization: returns ``result_fn(msgs)`` and bumps
    ``compaction_runs`` — the loop's authoritative did-it-compact gate
    (length deltas alone no longer count as compaction)."""
    ctx_mgr = AsyncMock()
    ctx_mgr.compaction_runs = 0

    async def _compactor(msgs, *a, **kw):
        ctx_mgr.compaction_runs += 1
        return result_fn(msgs)

    ctx_mgr.ensure_within_limits = _compactor
    return ctx_mgr


class TestContextCompaction:
    @pytest.mark.asyncio
    async def test_turn_messages_are_stamped_and_survive_input_eviction(self):
        """Every message the turn appends carries its membership stamp, so a
        compaction that summarises the turn's own input away still leaves the
        turn's rows selectable for the turn-end reconcile (the crash in
        stateless_turn_settlement_crashes_after_midturn_compaction.md)."""
        from shared.runtime.core.message_markers import turn_membership

        def _drop_input(msgs):
            return [msgs[0], SystemMessage(content="[Summary of prior work]\nrecap")]

        ctx_mgr = _make_compacting_ctx_mgr(_drop_input)
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="question", id="input-4"),
        ]
        callbacks = _make_callbacks()

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            turn_id=4,
        )

        assert result.error is None
        assert not any(getattr(m, "id", None) == "input-4" for m in messages)
        ai = [m for m in messages if isinstance(m, AIMessage)]
        assert ai and all(turn_membership(m) == 4 for m in ai)
        assert turn_membership(messages[0]) is None  # system prompt is not a turn row

    @pytest.mark.asyncio
    async def test_compaction_triggers_git_commit_and_push(self):
        """When compaction actually ran (run counter), git commit + push fires."""

        ctx_mgr = _make_compacting_ctx_mgr(lambda msgs: msgs[:2])

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr

        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q1"),
            AIMessage(content="a1"),
            HumanMessage(content="q2"),
        ]
        callbacks = _make_callbacks()

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        git_mgr.commit.assert_called_once()
        git_mgr.push.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("push_ok", [False, True])
    async def test_compaction_maps_only_after_durable_push(self, push_ok):
        ctx_mgr = _make_compacting_ctx_mgr(lambda msgs: msgs[:2])
        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.return_value = True
        git_mgr.get_current_commit.return_value = "c" * 40
        git_mgr.push.return_value = push_ok
        tool_ctx = MagicMock()
        tool_ctx.workspace_manager.git_manager = git_mgr
        mapping = AsyncMock()
        callbacks = _make_callbacks(on_workspace_commit=mapping)

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=[
                SystemMessage(content="sys"),
                HumanMessage(content="q1"),
                AIMessage(content="a1"),
                HumanMessage(content="q2"),
            ],
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        git_mgr.push.assert_called_once()
        if push_ok:
            mapping.assert_awaited_once_with("c" * 40)
        else:
            mapping.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_compaction_adopts_result_into_session_list(self):
        """A real compaction is adopted into the durable ``messages`` list
        (summary + tail replace the old history in place), so the next LLM
        call doesn't re-summarize from scratch. The full log stays in
        thread_messages; only the in-memory working set shrinks."""

        summary_msg = SystemMessage(content="[Summary of prior work]\nrecap")
        ctx_mgr = _make_compacting_ctx_mgr(
            lambda msgs: [msgs[0], summary_msg, msgs[-1]]
        )

        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q1"),
            AIMessage(content="a1"),
            HumanMessage(content="q2"),
        ]
        callbacks = _make_callbacks(on_context_compacted=AsyncMock())

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        contents = [getattr(m, "content", "") for m in messages]
        assert any("[Summary of prior work]" in c for c in contents), (
            "compacted history must be adopted into the session list"
        )
        assert "q1" not in contents, "summarized-away messages must be gone"
        # on_context_compacted surfaced the compaction exactly once
        callbacks.on_context_compacted.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passthrough_does_not_fire_compaction_side_effects(self):
        """A pass-through bound (no summarization) must not adopt, must not
        emit on_context_compacted, and must not git-push — even if stray
        list-length changes occur (the old length heuristic false-fired on
        leaked RemoveMessage markers; the run counter cannot)."""
        from langchain_core.messages import RemoveMessage

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr
        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr

        # Stray markers in the list (the historical leak shape): the strip
        # shrinks the working copy, which used to read as "compaction".
        messages = [
            RemoveMessage(id="dead-1"),
            RemoveMessage(id="dead-2"),
            SystemMessage(content="sys"),
            SystemMessage(content="[Summary of prior work]\nold recap"),
            HumanMessage(content="q"),
        ]
        ctx_mgr = AsyncMock()
        ctx_mgr.compaction_runs = 0  # never bumped → pass-through
        ctx_mgr.ensure_within_limits = AsyncMock(side_effect=lambda m, *a, **kw: m)
        callbacks = _make_callbacks(on_context_compacted=AsyncMock())

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        callbacks.on_context_compacted.assert_not_awaited()
        git_mgr.commit.assert_not_called()
        git_mgr.push.assert_not_called()

    @pytest.mark.asyncio
    async def test_compaction_git_failure_non_fatal(self):
        """Git failure during compaction doesn't crash."""

        ctx_mgr = _make_compacting_ctx_mgr(lambda msgs: msgs[:1])

        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.side_effect = RuntimeError("git failed")

        ws_mgr = MagicMock()
        ws_mgr.git_manager = git_mgr

        tool_ctx = MagicMock()
        tool_ctx.workspace_manager = ws_mgr

        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q"),
            AIMessage(content="a"),
        ]
        callbacks = _make_callbacks()

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response("ok")),
            tool_map={},
            context_manager=ctx_mgr,
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.error is None


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — LLM streaming
# ---------------------------------------------------------------------------


class TestLLMStreaming:
    @pytest.mark.asyncio
    async def test_string_content_streamed_token_by_token(self):
        """String content from chunks is streamed via on_token."""
        chunk1 = AIMessage(content="Hello ")
        chunk2 = AIMessage(content="world")

        async def _astream(msgs, **kw):
            yield chunk1
            yield chunk2

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert callbacks.on_token.call_count == 2
        callbacks.on_token.assert_any_call("Hello ")
        callbacks.on_token.assert_any_call("world")

    @pytest.mark.asyncio
    async def test_anthropic_list_content_streamed(self):
        """Anthropic list-of-dicts content blocks are streamed."""
        chunk = AIMessage(
            content=[
                {"type": "text", "text": "Hello"},
                {"type": "tool_use", "id": "x"},  # non-text block
            ]
        )

        async def _astream(msgs, **kw):
            yield chunk

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        callbacks.on_token.assert_called_once_with("Hello")

    @pytest.mark.asyncio
    async def test_empty_text_blocks_produce_warning(self):
        """Empty text blocks generate a user-visible warning (not silently dropped)."""
        chunk = AIMessage(content=[{"type": "text", "text": ""}])

        async def _astream(msgs, **kw):
            yield chunk

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        callbacks.on_token.assert_called_once_with(
            "⚠ The model returned an empty response. Please try again or switch models."
        )

    @pytest.mark.asyncio
    async def test_empty_chunks_return_error(self):
        """No chunks → TurnResult with 'Empty LLM response' error."""

        async def _astream(msgs, **kw):
            return
            yield  # empty generator

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.error == "Empty LLM response"

    @pytest.mark.asyncio
    async def test_empty_streaming_response_generates_warning(self):
        """Streaming response with empty content and no tool calls sends warning."""
        chunk = AIMessage(content="")

        async def _astream(msgs, **kw):
            yield chunk

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        callbacks.on_token.assert_called_once_with(
            "⚠ The model returned an empty response. Please try again or switch models."
        )
        assert messages[-1].content == (
            "⚠ The model returned an empty response. Please try again or switch models."
        )
        assert result.error is None

    @pytest.mark.asyncio
    async def test_empty_streaming_response_with_refusal(self):
        """Streaming response with refusal in additional_kwargs sends refusal msg."""
        chunk = AIMessage(
            content="",
            additional_kwargs={"refusal": "I cannot help with that"},
        )

        async def _astream(msgs, **kw):
            yield chunk

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        callbacks.on_token.assert_called_once_with(
            "⚠ The model declined to respond: I cannot help with that"
        )

    @pytest.mark.asyncio
    async def test_empty_text_with_tool_calls_no_warning(self):
        """Streaming response with tool calls but no text should not warn."""
        chunk = AIMessage(
            content="",
            tool_calls=[{"id": "tc1", "name": "test_tool", "args": {"x": 1}}],
        )

        async def _astream(msgs, **kw):
            yield chunk

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        mock_tool = AsyncMock()
        mock_tool.name = "test_tool"
        mock_tool.ainvoke = AsyncMock(return_value="done")

        # Need a second LLM call that returns no tool calls to end the turn
        call_count = 0

        async def _astream_second(msgs, **kw):
            yield AIMessage(content="Tool result processed.")

        original_astream = llm.astream

        async def _astream_dispatch(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                async for c in original_astream(msgs, **kw):
                    yield c
            else:
                async for c in _astream_second(msgs, **kw):
                    yield c

        llm.astream = _astream_dispatch

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"test_tool": mock_tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        # on_token should NOT have been called with the warning message
        for call in callbacks.on_token.call_args_list:
            assert "empty response" not in call.args[0].lower()


# ---------------------------------------------------------------------------
# 1.5b _execute_turn — empty-response bounded retry (gpt-5.x / codex)
# ---------------------------------------------------------------------------

_PLACEHOLDER = (
    "⚠ The model returned an empty response. Please try again or switch models."
)


def _drive_reasoning_then_empty(reasoning_text="dead-end reasoning"):
    """An astream that surfaces a reasoning delta (sets reasoning_streamed=True
    via the live sink) and then yields an empty answer — the slow-empty mode."""
    from shared.runtime.llm.reasoning_chat import _STREAM_REASONING_SINK

    async def _astream(messages, **kw):
        sink = _STREAM_REASONING_SINK.get()
        assert sink is not None, "sink must be installed during the stream"
        sink(reasoning_text)
        yield AIMessage(content="")

    return _astream


async def _run_turn(llm, callbacks):
    messages = [SystemMessage(content="sys"), HumanMessage(content="go")]
    result = await _execute_turn(
        llm_with_tools=llm,
        tool_map={},
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        messages=messages,
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=_make_config(),
    )
    await asyncio.sleep(0)  # let fire-and-forget reasoning broadcasts settle
    return result, messages


class TestEmptyResponseRetry:
    @pytest.mark.asyncio
    async def test_slow_empty_replaces_reasoning_and_recovers(self):
        """Reasoning streamed then empty → ainvoke retry, reset frame fired, the
        retry's reasoning + answer re-streamed under the same message id."""
        thinking_calls = []

        async def _on_thinking(content, message_id=None):
            thinking_calls.append((content, message_id))

        retry = AIMessage(
            content=[
                {"type": "reasoning", "summary": [{"text": "retry reasoning"}]},
                {"type": "text", "text": "recovered answer"},
            ]
        )
        llm = AsyncMock()
        llm.reasoning = True  # gate ON
        llm.astream = _drive_reasoning_then_empty()
        llm.ainvoke = AsyncMock(return_value=retry)

        reset = AsyncMock()
        callbacks = _make_callbacks(on_thinking=_on_thinking, on_thinking_reset=reset)
        _, messages = await _run_turn(llm, callbacks)

        # Retried exactly once.
        llm.ainvoke.assert_awaited_once()
        # Reset fired once, keyed to the message id the appended row carries.
        reset.assert_awaited_once()
        assert reset.await_args.kwargs["message_id"] == messages[-1].id
        # The retry's reasoning was re-emitted under that same id...
        assert ("retry reasoning", messages[-1].id) in thinking_calls
        # ...and its answer streamed, with no placeholder shown.
        callbacks.on_token.assert_any_call("recovered answer")
        for call in callbacks.on_token.call_args_list:
            assert "empty response" not in call.args[0].lower()

    @pytest.mark.asyncio
    async def test_fast_empty_recovers_without_reset(self):
        """Empty with NO prior streamed reasoning → retry recovers, but no reset
        frame (there is no live bubble to clear)."""
        llm = _make_streaming_llm(AIMessage(content=""))
        llm.reasoning = True
        llm.ainvoke = AsyncMock(return_value=AIMessage(content="recovered"))

        reset = AsyncMock()
        callbacks = _make_callbacks(on_thinking_reset=reset)
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_awaited_once()
        reset.assert_not_awaited()
        callbacks.on_token.assert_any_call("recovered")
        assert messages[-1].content == "recovered"

    @pytest.mark.asyncio
    async def test_retry_also_empty_shows_placeholder_and_no_reset(self):
        """Reasoning streamed then empty, and the retry is ALSO empty → keep the
        dead-end reasoning (no reset) and fall back to the placeholder."""
        llm = AsyncMock()
        llm.reasoning = True
        llm.astream = _drive_reasoning_then_empty()
        llm.ainvoke = AsyncMock(return_value=AIMessage(content=""))

        reset = AsyncMock()
        callbacks = _make_callbacks(on_thinking_reset=reset)
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_awaited_once()
        reset.assert_not_awaited()  # reset only on a successful retry
        callbacks.on_token.assert_any_call(_PLACEHOLDER)
        assert messages[-1].content == _PLACEHOLDER

    @pytest.mark.asyncio
    async def test_non_reasoning_model_does_not_retry(self):
        """Gate is reasoning-only: a non-reasoning model keeps today's
        placeholder behavior and never retries."""
        llm = _make_streaming_llm(AIMessage(content=""))
        llm.reasoning = None  # gate OFF
        llm.ainvoke = AsyncMock()

        callbacks = _make_callbacks(on_thinking_reset=AsyncMock())
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_not_awaited()
        callbacks.on_token.assert_called_once_with(_PLACEHOLDER)
        assert messages[-1].content == _PLACEHOLDER

    @pytest.mark.asyncio
    async def test_refusal_short_circuits_before_retry(self):
        """An explicit refusal is a real answer, not the empty bug — show it and
        never retry, even on a reasoning model."""
        llm = _make_streaming_llm(
            AIMessage(content="", additional_kwargs={"refusal": "I cannot"})
        )
        llm.reasoning = True
        llm.ainvoke = AsyncMock()

        callbacks = _make_callbacks(on_thinking_reset=AsyncMock())
        _, _ = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_not_awaited()
        callbacks.on_token.assert_called_once_with(
            "⚠ The model declined to respond: I cannot"
        )

    @pytest.mark.asyncio
    async def test_retry_works_when_reset_callback_absent(self):
        """Back-compat: on_thinking_reset=None (older transport) → the retry still
        recovers, the stale bubble just isn't cleared live."""
        llm = AsyncMock()
        llm.reasoning = True
        llm.astream = _drive_reasoning_then_empty()
        llm.ainvoke = AsyncMock(return_value=AIMessage(content="recovered"))

        callbacks = _make_callbacks()  # no on_thinking_reset ⇒ None
        assert callbacks.on_thinking_reset is None
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_awaited_once()
        callbacks.on_token.assert_any_call("recovered")
        assert messages[-1].content == "recovered"

    @pytest.mark.asyncio
    async def test_codex_in_stream_reasoning_then_empty_resets_unkeyed_and_reorders(
        self,
    ):
        """The dev-journal scenario (thread b1758f38): a Responses-API stream
        surfaces reasoning as a typed content block (UNKEYED frame, sink never
        fires) then dead-ends empty. The retry must clear the stale bubble via
        an UNKEYED reset and emit its own reasoning BEFORE the answer — never
        as a trailing post-stream frame."""
        events = []

        async def _on_thinking(content, message_id=None):
            events.append(("thinking", content, message_id))

        async def _on_token(text):
            events.append(("token", text))

        async def _on_reset(message_id=None):
            events.append(("reset", message_id))

        async def _astream(messages, **kw):
            yield AIMessage(
                content=[{"type": "reasoning", "summary": [{"text": "dead-end"}]}]
            )

        retry = AIMessage(
            content="recovered answer",
            additional_kwargs={"reasoning_content": "retry reasoning"},
        )
        llm = AsyncMock()
        llm.reasoning = True
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=retry)

        callbacks = _make_callbacks(
            on_thinking=_on_thinking,
            on_token=_on_token,
            on_thinking_reset=_on_reset,
        )
        _, messages = await _run_turn(llm, callbacks)

        # The full broadcast order: attempt-1's unkeyed block frame, an
        # unkeyed reset that can actually clear it, the retry's reasoning
        # keyed to the persisted row, then the answer. Nothing after it.
        assert events == [
            ("thinking", "dead-end", None),
            ("reset", None),
            ("thinking", "retry reasoning", messages[-1].id),
            ("token", "recovered answer"),
        ]
        # reasoning_content ⇒ the row id is pinned to the turn's minted id.
        assert messages[-1].id.startswith("msg_")


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — LLM streaming fallback
# ---------------------------------------------------------------------------


class TestLLMStreamingFallback:
    @pytest.mark.asyncio
    async def test_response_not_read_triggers_fallback(self):
        """ResponseNotRead error triggers ainvoke fallback."""

        class ResponseNotReadError(Exception):
            pass

        async def _astream(msgs, **kw):
            raise ResponseNotReadError("stream failed")
            yield

        fallback_response = _make_llm_response("fallback response")
        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=fallback_response)

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        llm.ainvoke.assert_called_once()
        assert result.error is None

    @pytest.mark.asyncio
    async def test_api_connection_error_triggers_fallback(self):
        """APIConnectionError triggers ainvoke fallback."""

        class APIConnectionError(Exception):
            pass

        async def _astream(msgs, **kw):
            raise APIConnectionError("connection lost")
            yield

        fallback_response = _make_llm_response("ok")
        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=fallback_response)

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        llm.ainvoke.assert_called_once()
        assert result.error is None

    @pytest.mark.asyncio
    async def test_other_stream_errors_re_raised(self):
        """Non-fallback errors propagate up."""

        async def _astream(msgs, **kw):
            raise ValueError("unexpected")
            yield

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        with pytest.raises(ValueError, match="unexpected"):
            await _execute_turn(
                llm_with_tools=llm,
                tool_map={},
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                messages=messages,
                callbacks=callbacks,
                llm_timeout=600,
                auxiliary_llm=None,
                config=_make_config(),
            )

    @pytest.mark.asyncio
    async def test_fallback_streams_content_via_on_token(self):
        """Fallback ainvoke response is streamed via on_token."""

        class ResponseNotReadError(Exception):
            pass

        async def _astream(msgs, **kw):
            raise ResponseNotReadError("no stream")
            yield

        fallback_response = _make_llm_response("complete answer")
        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=fallback_response)

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        callbacks.on_token.assert_called_with("complete answer")


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — LLM timeout
# ---------------------------------------------------------------------------


class TestLLMTimeout:
    @pytest.mark.asyncio
    async def test_timeout_returns_error_result(self):
        """asyncio.TimeoutError at outer level returns TurnResult with error."""

        async def _astream(msgs, **kw):
            raise asyncio.TimeoutError()
            yield

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=10,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.error is not None
        assert "timed out" in result.error
        callbacks.on_error.assert_called_once()


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — tool execution loop
# ---------------------------------------------------------------------------


class TestToolExecutionLoop:
    @pytest.mark.asyncio
    async def test_successful_tool_execution(self):
        """Tool is invoked, result appended as ToolMessage."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "search", "args": {"q": "test"}, "id": "tc1"}],
        )
        final_response = _make_llm_response("Here is the answer")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        execution_order = []
        tool = _make_tool("search", "search results")

        async def _invoke(args):
            execution_order.append(("ainvoke", args))
            return "search results"

        tool.ainvoke = AsyncMock(side_effect=_invoke)
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        async def _execution_start(name, tool_call_id):
            execution_order.append(("execution_start", name, tool_call_id))

        callbacks = _make_callbacks(
            on_tool_execution_start=AsyncMock(side_effect=_execution_start)
        )
        messages = [SystemMessage(content="sys"), HumanMessage(content="find stuff")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"search": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.tool_calls_made == 1
        assert result.messages_added >= 2  # tool result + final AI

        tool.ainvoke.assert_called_once_with({"q": "test"})
        callbacks.on_tool_start.assert_called_once_with("search", {"q": "test"}, "tc1")
        callbacks.on_tool_execution_start.assert_awaited_once_with("search", "tc1")
        assert execution_order == [
            ("execution_start", "search", "tc1"),
            ("ainvoke", {"q": "test"}),
        ]
        callbacks.on_tool_result.assert_called_once_with(
            "search", "search results", "tc1", is_error=False
        )

    @pytest.mark.asyncio
    async def test_remote_retirement_between_provider_calls_blocks_second_effect(self):
        """A Force-End need not wait for the lifecycle watchdog to be seen."""

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "read", "args": {}, "id": "tc1"}],
        )
        provider_calls = 0

        async def _astream(_messages, **_kwargs):
            nonlocal provider_calls
            provider_calls += 1
            yield response_with_tool

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        tool = _make_tool("read", "ok")
        authority = AsyncMock(side_effect=[True, False])
        callbacks = _make_callbacks(before_provider_execution=authority)

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"read": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(
                    side_effect=lambda messages, *args, **kwargs: messages
                )
            ),
            messages=[SystemMessage(content="sys"), HumanMessage(content="go")],
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.admission_closed is True
        assert provider_calls == 1
        assert authority.await_count == 2
        tool.ainvoke.assert_awaited_once_with({})

    @pytest.mark.asyncio
    async def test_remote_retirement_after_first_provider_blocks_real_tool_effect(self):
        """The durable authority fence wins before a watchdog can poll End.

        Provider one returns a real tool call while the exact pinned life is
        live.  Owner End then installs its retirement token at the visible
        tool-start boundary, but the process-local retirement latch deliberately
        remains open (the lifecycle watchdog has not observed it).  The actual
        production provider/tool callbacks must consult the same remote row and
        refuse before ``ainvoke``; no second provider or fabricated tool result
        may follow.
        """

        from agent.api import persistent_app as pa

        generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        attach_token = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        agent_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        state = {"retirement_authorized": False}

        async def verify_authority(**_kwargs):
            return not state["retirement_authorized"]

        db = SimpleNamespace(
            verify_pinned_runtime_effect_authority=AsyncMock(
                side_effect=verify_authority
            )
        )
        session = SimpleNamespace(
            postgres_conn=db,
            protected_cloud_required=False,
        )
        provider_calls = 0
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "write", "args": {}, "id": "tc1"}],
        )

        async def stream(_messages, **_kwargs):
            nonlocal provider_calls
            provider_calls += 1
            yield response_with_tool

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = stream
        tool = _make_tool("write", "must not run")

        async def authorize_end_after_provider(*_args):
            state["retirement_authorized"] = True
            # The delayed lifecycle watchdog has not installed the local latch.
            assert pa._retirement_admission_identity is None

        callbacks = _make_callbacks(
            before_provider_execution=pa._loop_runtime_effect_authority_current,
            on_tool_start=authorize_end_after_provider,
            on_tool_execution_start=pa._loop_on_tool_execution_start,
        )

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_thread_id", "thread-a"),
            patch.object(
                pa, "_orchestrator_client", SimpleNamespace(agent_id=agent_id)
            ),
            patch.object(pa, "_pinned_runtime_generation_enabled", True),
            patch.object(pa, "_input_runtime_generation", generation),
            patch.object(pa, "_session_runtime_generation", generation),
            patch.object(pa, "_session_runtime_attach_token", attach_token),
            patch.object(pa, "_retirement_admission_identity", None),
            patch.object(pa, "_termination_admission_fenced", False),
            patch.object(pa, "_tool_inflight", False),
            patch.dict(pa.os.environ, {"POD_UID": "pod-uid-a"}),
        ):
            with pytest.raises(pa.TerminationAdmissionClosed):
                await _execute_turn(
                    llm_with_tools=llm,
                    tool_map={"write": tool},
                    context_manager=AsyncMock(
                        ensure_within_limits=AsyncMock(
                            side_effect=lambda messages, *args, **kwargs: messages
                        )
                    ),
                    messages=[
                        SystemMessage(content="sys"),
                        HumanMessage(content="go"),
                    ],
                    callbacks=callbacks,
                    llm_timeout=600,
                    auxiliary_llm=None,
                    config=_make_config(),
                )

            assert provider_calls == 1
            assert db.verify_pinned_runtime_effect_authority.await_count == 2
            tool.ainvoke.assert_not_awaited()
            callbacks.on_tool_result.assert_not_awaited()
            assert pa._tool_inflight is False

    @pytest.mark.asyncio
    async def test_execution_identity_failure_aborts_before_tool_invoke(self):
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "write", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("write", "must not run")
        callbacks = _make_callbacks(
            on_tool_execution_start=AsyncMock(
                side_effect=RuntimeError("stateless identity missing")
            )
        )

        with pytest.raises(RuntimeError, match="stateless identity missing"):
            await _execute_turn(
                llm_with_tools=llm,
                tool_map={"write": tool},
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(
                        side_effect=lambda messages, *args, **kwargs: messages
                    )
                ),
                messages=[SystemMessage(content="sys"), HumanMessage(content="go")],
                callbacks=callbacks,
                llm_timeout=600,
                auxiliary_llm=None,
                config=_make_config(),
            )

        callbacks.on_tool_execution_start.assert_awaited_once_with("write", "tc1")
        tool.ainvoke.assert_not_awaited()
        callbacks.on_tool_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_protected_loss_at_execution_gate_never_latches_tool_inflight(self):
        from agent.api import persistent_app as pa

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "write", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("write", "must not run")
        state = {"ready": True}

        async def _tool_start(name, args, tool_call_id):
            assert state["ready"] is True
            await pa._loop_on_tool_start(name, args, tool_call_id)
            # Mount health drops after the visible/permission phase but before
            # the exact external-effect boundary.
            state["ready"] = False

        callbacks = _make_callbacks(
            on_tool_start=_tool_start,
            on_tool_execution_start=pa._loop_on_tool_execution_start,
        )
        session = SimpleNamespace(
            protected_cloud_required=True,
            protected_cloud_ready=lambda: state["ready"],
        )

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_tool_inflight", False),
            patch.object(pa._session_input, "schedule_protected_reclaim"),
            patch.object(pa, "_broadcast"),
        ):
            with pytest.raises(WorkspaceUnavailableError, match="protected cloud"):
                await _execute_turn(
                    llm_with_tools=llm,
                    tool_map={"write": tool},
                    context_manager=AsyncMock(
                        ensure_within_limits=AsyncMock(
                            side_effect=lambda messages, *args, **kwargs: messages
                        )
                    ),
                    messages=[
                        SystemMessage(content="sys"),
                        HumanMessage(content="go"),
                    ],
                    callbacks=callbacks,
                    llm_timeout=600,
                    auxiliary_llm=None,
                    config=_make_config(),
                )

            tool.ainvoke.assert_not_awaited()
            callbacks.on_tool_result.assert_not_called()
            assert pa._tool_inflight is False

    @pytest.mark.asyncio
    async def test_malformed_stateless_identity_never_invokes_tool(self):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import LeaseHandle, current_lease

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "write", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("write", "must not run")
        callbacks = _make_callbacks(
            on_tool_execution_start=pa._loop_on_tool_execution_start
        )
        lease = LeaseHandle()
        lease.update("claimed-thread", 23)
        lease_context = current_lease.set(lease)
        try:
            with (
                patch.object(pa, "_session", SimpleNamespace(turn_count=1)),
                patch.object(pa, "_thread_id", "wrong-thread"),
                patch.object(pa, "_turn_tool_execution_identity", None),
                patch.object(
                    pa,
                    "_turn_tool_execution_external_hook",
                    MagicMock(),
                ),
            ):
                with pytest.raises(RuntimeError, match="exact claimant identity"):
                    await _execute_turn(
                        llm_with_tools=llm,
                        tool_map={"write": tool},
                        context_manager=AsyncMock(
                            ensure_within_limits=AsyncMock(
                                side_effect=lambda messages, *args, **kwargs: messages
                            )
                        ),
                        messages=[
                            SystemMessage(content="sys"),
                            HumanMessage(content="go"),
                        ],
                        callbacks=callbacks,
                        llm_timeout=600,
                        auxiliary_llm=None,
                        config=_make_config(),
                    )

                assert lease.lost.is_set()
                tool.ainvoke.assert_not_awaited()
                callbacks.on_tool_result.assert_not_called()
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_stateless_identity_without_executor_handoff_never_invokes_tool(self):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import LeaseHandle, current_lease

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "write", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("write", "must not run")
        callbacks = _make_callbacks(
            on_tool_execution_start=pa._loop_on_tool_execution_start
        )
        lease = LeaseHandle()
        lease.update("claimed-thread", 24)
        lease_context = current_lease.set(lease)
        try:
            with (
                patch.object(pa, "_session", SimpleNamespace(turn_count=1)),
                patch.object(pa, "_thread_id", "claimed-thread"),
                patch.object(pa, "_turn_tool_execution_identity", None),
                patch.object(pa, "_turn_tool_execution_external_hook", None),
            ):
                with pytest.raises(RuntimeError, match="exact claimant identity"):
                    await _execute_turn(
                        llm_with_tools=llm,
                        tool_map={"write": tool},
                        context_manager=AsyncMock(
                            ensure_within_limits=AsyncMock(
                                side_effect=lambda messages, *args, **kwargs: messages
                            )
                        ),
                        messages=[
                            SystemMessage(content="sys"),
                            HumanMessage(content="go"),
                        ],
                        callbacks=callbacks,
                        llm_timeout=600,
                        auxiliary_llm=None,
                        config=_make_config(),
                    )

                assert lease.lost.is_set()
                assert pa._turn_tool_execution_identity == (
                    "claimed-thread",
                    24,
                    1,
                )
                tool.ainvoke.assert_not_awaited()
                callbacks.on_tool_result.assert_not_called()
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_pinned_tool_executes_without_queue_identity(self):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import current_lease

        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "read", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")
        call_count = 0

        async def _astream(messages, **kwargs):
            nonlocal call_count
            del messages, kwargs
            call_count += 1
            yield response_with_tool if call_count == 1 else final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        tool = _make_tool("read", "ok")
        callbacks = _make_callbacks(
            on_tool_execution_start=pa._loop_on_tool_execution_start
        )
        lease_context = current_lease.set(None)
        try:
            with (
                patch.object(
                    pa,
                    "_session",
                    SimpleNamespace(protected_cloud_required=False),
                ),
                patch.object(pa, "_turn_tool_execution_identity", None),
                patch.object(pa, "_turn_tool_execution_external_hook", None),
            ):
                result = await _execute_turn(
                    llm_with_tools=llm,
                    tool_map={"read": tool},
                    context_manager=AsyncMock(
                        ensure_within_limits=AsyncMock(
                            side_effect=lambda messages, *args, **kwargs: messages
                        )
                    ),
                    messages=[
                        SystemMessage(content="sys"),
                        HumanMessage(content="go"),
                    ],
                    callbacks=callbacks,
                    llm_timeout=600,
                    auxiliary_llm=None,
                    config=_make_config(),
                )

                assert result.tool_calls_made == 1
                tool.ainvoke.assert_awaited_once_with({})
                assert pa._turn_tool_execution_identity is None
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_permission_denied_tool(self):
        """Denied tool call: ToolMessage added but tool NOT invoked."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "dangerous", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("dangerous", "should not run")

        callbacks = _make_callbacks(permission_check=AsyncMock(return_value=False))
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"dangerous": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        tool.ainvoke.assert_not_called()
        callbacks.on_tool_start.assert_not_called()
        callbacks.on_tool_execution_start.assert_not_awaited()
        assert result.tool_calls_made == 0

        # Check the refusal was reported to the model. An *explicit* deny
        # (legacy bool False == PermissionOutcome.DECLINED) says "declined";
        # only a real user decision may claim the user refused — an
        # unanswered gate must not (see
        # knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md).
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert any("declined" in m.content.lower() for m in tool_msgs)

    @pytest.mark.asyncio
    async def test_unanswered_permission_never_crosses_execution_boundary(self):
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "waiting", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("waiting", "must not run")
        callbacks = _make_callbacks(
            permission_check=AsyncMock(return_value=PermissionOutcome.NO_ANSWER)
        )

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"waiting": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=[SystemMessage(content="sys"), HumanMessage(content="go")],
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.tool_calls_made == 0
        tool.ainvoke.assert_not_awaited()
        callbacks.on_tool_execution_start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pre_tool_interrupt_never_crosses_execution_boundary(self):
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "waiting", "args": {}, "id": "tc1"}],
        )
        llm = _make_streaming_llm(response_with_tool)
        tool = _make_tool("waiting", "must not run")
        callbacks = _make_callbacks(
            after_assistant_tool_calls_persisted=AsyncMock(return_value=True)
        )

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"waiting": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=[SystemMessage(content="sys"), HumanMessage(content="go")],
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.interrupted is True
        tool.ainvoke.assert_not_awaited()
        callbacks.on_tool_execution_start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_tool(self):
        """Unknown tool name: error ToolMessage added, on_tool_result called."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "nonexistent", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},  # no tools
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.tool_calls_made == 0
        callbacks.on_tool_result.assert_called_once()
        callbacks.on_tool_execution_start.assert_not_awaited()
        # The message names the cause rather than only the symptom, so the
        # model can re-plan instead of retrying — see
        # _unavailable_tool_message and TestUnboundToolSkipsThePermissionGate.
        body = callbacks.on_tool_result.call_args[0][1]
        assert "No tool named 'nonexistent' exists" in body
        assert "Do not retry" in body

    @pytest.mark.asyncio
    async def test_tool_exception_handled(self):
        """Tool exception caught, error result stored, counters still increment."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "broken", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("broken")
        tool.ainvoke = AsyncMock(side_effect=RuntimeError("tool crashed"))
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={"broken": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        assert result.tool_calls_made == 1
        callbacks.on_tool_result.assert_called_once()
        assert "error" in callbacks.on_tool_result.call_args[0][1].lower()

    @pytest.mark.asyncio
    async def test_tool_none_result_becomes_empty_string(self):
        """Tool returning None produces empty string ToolMessage."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "noop", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("ok")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("noop", None)
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"noop": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        callbacks.on_tool_result.assert_called_once_with(
            "noop", "", "tc1", is_error=False
        )

    @pytest.mark.asyncio
    async def test_tool_args_default_to_empty_dict(self):
        """Missing 'args' in tool_call defaults to {} via .get('args', {})."""
        # LangChain AIMessage validator requires 'args', so we build a response
        # that has args=None and verify the code handles it via .get() default.
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "simple", "args": {}, "id": "tc1"}],
        )
        # Simulate missing 'args' at the dict level after construction
        response_with_tool.tool_calls[0].pop("args", None)

        final_response = _make_llm_response("ok")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("simple", "done")
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"simple": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        tool.ainvoke.assert_called_once_with({})

    @pytest.mark.asyncio
    async def test_turn_continues_after_tool_calls(self):
        """After tool results, LLM called again to see results."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "search", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("final answer")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("search", "data")
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"search": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        # LLM called twice: once got tool call, once got final response
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_turn_ends_when_no_tool_calls(self):
        """Turn ends when response has no tool_calls."""
        response = _make_llm_response("just text")
        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="hi")]

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(response),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.tool_calls_made == 0
        assert result.messages_added == 1  # just the AI response


# ---------------------------------------------------------------------------
# 1.6 _execute_turn — VM upgrade detection
# ---------------------------------------------------------------------------


class TestVMUpgradeDetection:
    @pytest.mark.asyncio
    async def test_vm_upgrade_fires_on_matching_freeze_type(self):
        """on_vm_upgrade_needed fires when freeze_type == 'vm_upgrade_required'."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")

        freeze_req = {"freeze_type": "vm_upgrade_required", "reason": "sudo"}
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = freeze_req

        on_upgrade = AsyncMock()
        callbacks = _make_callbacks(on_vm_upgrade_needed=on_upgrade)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        on_upgrade.assert_called_once_with(freeze_req)

    @pytest.mark.asyncio
    async def test_workspace_upgrade_fires_on_matching_freeze_type(self):
        """The generalized callback (workspace_tier_upgrade.md §4.2 S5) fires on a
        request_workspace_upgrade freeze (freeze_type=workspace_upgrade_required),
        and the new on_workspace_upgrade_needed kwarg is honored directly."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")

        freeze_req = {
            "freeze_type": "workspace_upgrade_required",
            "target_tier": "sandbox",
            "reason": "need a shell",
        }
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = freeze_req

        on_upgrade = AsyncMock()
        # Construct via the NEW kwarg directly (no alias).
        callbacks = _make_callbacks(on_workspace_upgrade_needed=on_upgrade)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        on_upgrade.assert_called_once_with(freeze_req)

    def test_vm_upgrade_kwarg_aliases_to_workspace_upgrade(self):
        """The deprecated on_vm_upgrade_needed kwarg is promoted to the
        generalized on_workspace_upgrade_needed the loop actually reads."""
        cb = AsyncMock()
        callbacks = _make_callbacks(on_vm_upgrade_needed=cb)
        assert callbacks.on_workspace_upgrade_needed is cb
        # An explicit new-kwarg value is NOT overwritten by the alias.
        new_cb = AsyncMock()
        old_cb = AsyncMock()
        both = _make_callbacks(
            on_workspace_upgrade_needed=new_cb, on_vm_upgrade_needed=old_cb
        )
        assert both.on_workspace_upgrade_needed is new_cb

    @pytest.mark.asyncio
    async def test_vm_upgrade_not_fired_on_other_freeze_type(self):
        """on_vm_upgrade_needed NOT fired for non-vm_upgrade freeze types."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")

        freeze_req = {"freeze_type": "human_review", "reason": "checkpoint"}
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = freeze_req

        on_upgrade = AsyncMock()
        callbacks = _make_callbacks(on_vm_upgrade_needed=on_upgrade)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        on_upgrade.assert_not_called()

    @pytest.mark.asyncio
    async def test_vm_upgrade_not_checked_without_callback(self):
        """No freeze check when on_vm_upgrade_needed is None."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        # on_vm_upgrade_needed is None
        callbacks = _make_callbacks(on_vm_upgrade_needed=None)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        # consume_freeze_request should not be called when callback is None
        # Actually, looking at the code: it checks `tool_context and callbacks.on_vm_upgrade_needed`
        # With on_vm_upgrade_needed=None, the condition is False, so no call

    @pytest.mark.asyncio
    async def test_vm_upgrade_not_checked_without_tool_context(self):
        """No freeze check when tool_context is None."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")
        on_upgrade = AsyncMock()
        callbacks = _make_callbacks(on_vm_upgrade_needed=on_upgrade)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=None,
        )

        on_upgrade.assert_not_called()

    @pytest.mark.asyncio
    async def test_vm_upgrade_not_fired_when_consume_returns_none(self):
        """No upgrade callback when consume_freeze_request returns None."""
        response_with_tool = AIMessage(
            content="",
            tool_calls=[{"name": "run_cmd", "args": {}, "id": "tc1"}],
        )
        final_response = _make_llm_response("done")

        call_count = 0

        async def _astream(msgs, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                yield response_with_tool
            else:
                yield final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        tool = _make_tool("run_cmd", "ok")
        tool_ctx = MagicMock()
        tool_ctx.consume_freeze_request.return_value = None

        on_upgrade = AsyncMock()
        callbacks = _make_callbacks(on_vm_upgrade_needed=on_upgrade)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={"run_cmd": tool},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
            tool_context=tool_ctx,
        )

        on_upgrade.assert_not_called()


# ---------------------------------------------------------------------------
# Streamed-response normalization (regression:
# persistent_session_empty_chunk_history_corruption)
# ---------------------------------------------------------------------------


class TestStreamedResponseNormalization:
    """A streamed AIMessageChunk must never land in history as a raw chunk,
    and an empty interrupted partial must be dropped rather than appended —
    otherwise the next turn's request serialization raises
    'TypeError: Got unknown type ...' ("malformed response")."""

    @pytest.mark.asyncio
    async def test_normal_streamed_chunk_appended_as_concrete_ai_message(self):
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(AIMessageChunk(content="answer")),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        ai = [m for m in messages if isinstance(m, AIMessage)]
        assert len(ai) == 1
        assert type(ai[0]) is AIMessage  # concrete, not AIMessageChunk
        assert ai[0].content == "answer"

    @pytest.mark.asyncio
    async def test_empty_interrupted_partial_is_not_appended(self):
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]
        # check_interrupt: False at top-of-loop (line 434), then "graceful"
        # mid-stream (line 574) so the partial-handling path runs.
        check = MagicMock(side_effect=[False, "graceful"])

        result = await _execute_turn(
            llm_with_tools=_make_streaming_llm(
                AIMessageChunk(content="", id="lc_run--019e5adc-0945-7b73")
            ),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=_make_callbacks(check_interrupt=check),
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert result.interrupted is True
        # The empty partial must NOT be in history (neither chunk nor concrete).
        assert len(messages) == 2
        assert not any(isinstance(m, AIMessage) for m in messages)


class TestHardInterruptHelpers:
    """Phase-3 immediate hard-interrupt cancellation primitives.

    These race an in-flight LLM / auxiliary await against a hard-interrupt
    event so a hung network read is torn down at once, instead of waiting for
    the cooperative check_interrupt poll (which can't fire while parked).
    """

    @pytest.mark.asyncio
    async def test_await_passthrough_when_no_event(self):
        from agent.persistent_graph import _await_or_hard_interrupt

        async def quick():
            return "done"

        result, interrupted = await _await_or_hard_interrupt(quick(), None)
        assert result == "done"
        assert interrupted is False

    @pytest.mark.asyncio
    async def test_await_completes_when_event_idle(self):
        from agent.persistent_graph import _await_or_hard_interrupt

        async def quick():
            return 42

        result, interrupted = await _await_or_hard_interrupt(quick(), asyncio.Event())
        assert result == 42
        assert interrupted is False

    @pytest.mark.asyncio
    async def test_await_cancels_blocked_coro_on_event(self):
        from agent.persistent_graph import _await_or_hard_interrupt

        event = asyncio.Event()
        cancelled = {"v": False}

        async def hang():
            try:
                await asyncio.sleep(30)
                return "never"
            except asyncio.CancelledError:
                cancelled["v"] = True
                raise

        async def fire():
            await asyncio.sleep(0.01)
            event.set()

        fire_task = asyncio.create_task(fire())
        result, interrupted = await _await_or_hard_interrupt(hang(), event)
        await fire_task

        assert interrupted is True
        assert result is None
        # The blocked await must actually be torn down, not just abandoned.
        assert cancelled["v"] is True

    @pytest.mark.asyncio
    async def test_await_propagates_coro_exception(self):
        from agent.persistent_graph import _await_or_hard_interrupt

        async def boom():
            raise ValueError("kaboom")

        with pytest.raises(ValueError, match="kaboom"):
            await _await_or_hard_interrupt(boom(), asyncio.Event())

    @pytest.mark.asyncio
    async def test_stream_yields_chunks_then_stop(self):
        from agent.persistent_graph import _stream_next_or_hard_interrupt

        async def gen():
            yield "a"
            yield "b"

        aiter = gen().__aiter__()
        event = asyncio.Event()
        assert await _stream_next_or_hard_interrupt(aiter, event) == ("a", "chunk")
        assert await _stream_next_or_hard_interrupt(aiter, event) == ("b", "chunk")
        assert await _stream_next_or_hard_interrupt(aiter, event) == (None, "stop")

    @pytest.mark.asyncio
    async def test_stream_passthrough_when_no_event(self):
        from agent.persistent_graph import _stream_next_or_hard_interrupt

        async def gen():
            yield "only"

        aiter = gen().__aiter__()
        assert await _stream_next_or_hard_interrupt(aiter, None) == ("only", "chunk")
        assert await _stream_next_or_hard_interrupt(aiter, None) == (None, "stop")

    @pytest.mark.asyncio
    async def test_stream_interrupts_hung_read(self):
        from agent.persistent_graph import _stream_next_or_hard_interrupt

        started = asyncio.Event()

        async def hanging_gen():
            started.set()
            await asyncio.sleep(30)  # never yields — simulates a hung read
            yield "never"

        aiter = hanging_gen().__aiter__()
        event = asyncio.Event()

        async def fire():
            await started.wait()
            event.set()

        fire_task = asyncio.create_task(fire())
        chunk, status = await _stream_next_or_hard_interrupt(aiter, event)
        await fire_task

        assert chunk is None
        assert status == "interrupt"


# ---------------------------------------------------------------------------
# 1.7 _execute_turn — reasoning broadcast (dedupe key + ordering)
# ---------------------------------------------------------------------------


class TestExecuteTurnReasoning:
    """Reasoning frames carry the AI message id and are emitted exactly once.

    knowledge-base/knowledge/issues/persistent_chat_reasoning_after_answer_and_replay_duplication.md
    """

    @pytest.mark.asyncio
    async def test_reasoning_content_emitted_once_with_pinned_message_id(self):
        """A Chat-Completions reasoning blob (additional_kwargs.reasoning_content)
        is broadcast once via the post-stream fallback, keyed to the AI message
        id, and the persisted row id is pinned to that same id."""
        response = AIMessage(content="The answer.")
        response.additional_kwargs = {"reasoning_content": "Let me think."}

        captured = []

        async def _on_thinking(content, message_id=None):
            captured.append((content, message_id))

        callbacks = _make_callbacks(on_thinking=_on_thinking)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(response),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert len(captured) == 1
        content, message_id = captured[0]
        assert content == "Let me think."
        assert message_id and message_id.startswith("msg_")
        # The appended AI row shares the broadcast id — the dedupe key.
        assert messages[-1].id == message_id

    @pytest.mark.asyncio
    async def test_live_streamed_reasoning_skips_post_stream_fallback(self):
        """When the SSE tap surfaces reasoning live (sink fired), the post-stream
        fallback does not re-broadcast it — exactly one emission."""
        from shared.runtime.llm.reasoning_chat import _STREAM_REASONING_SINK

        response = AIMessage(content="Answer.")
        response.additional_kwargs = {"reasoning_content": "live reasoning"}

        captured = []

        async def _on_thinking(content, message_id=None):
            captured.append((content, message_id))

        async def _astream(messages, **kw):
            # Simulate the tap surfacing a reasoning delta mid-stream, before
            # the answer chunk — exactly what the live ordering fix relies on.
            sink = _STREAM_REASONING_SINK.get()
            assert sink is not None, "sink must be installed during the stream"
            sink("live reasoning")
            yield response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=response)

        callbacks = _make_callbacks(on_thinking=_on_thinking)
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )
        # Let the fire-and-forget broadcast task run.
        await asyncio.sleep(0)

        assert len(captured) == 1
        assert captured[0][0] == "live reasoning"
        # Pinned so the live frame's id matches the persisted row.
        assert captured[0][1] == messages[-1].id

    @pytest.mark.asyncio
    async def test_sink_is_cleared_after_turn(self):
        """The reasoning sink contextvar must not leak past the turn."""
        from shared.runtime.llm.reasoning_chat import _STREAM_REASONING_SINK

        callbacks = _make_callbacks()
        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]

        await _execute_turn(
            llm_with_tools=_make_streaming_llm(_make_llm_response()),
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=_make_config(),
        )

        assert _STREAM_REASONING_SINK.get() is None

    @pytest.mark.asyncio
    async def test_responses_in_stream_reasoning_unkeyed_no_fallback_no_pin(self):
        """A Responses-API stream delivers reasoning as a typed content block:
        it is emitted once (unkeyed, before the answer), the post-stream
        fallback stays quiet, and the provider message id is NOT pinned (the
        id round-trips into subsequent Responses requests)."""
        events = []

        async def _on_thinking(content, message_id=None):
            events.append(("thinking", content, message_id))

        async def _on_token(text):
            events.append(("token", text))

        response = AIMessage(
            id="resp_test",
            content=[
                {"type": "reasoning", "summary": [{"text": "codex thoughts"}]},
                {"type": "text", "text": "Answer."},
            ],
        )
        llm = _make_streaming_llm(response)
        llm.reasoning = True

        callbacks = _make_callbacks(on_thinking=_on_thinking, on_token=_on_token)
        _, messages = await _run_turn(llm, callbacks)

        assert events == [
            ("thinking", "codex thoughts", None),
            ("token", "Answer."),
        ]
        assert messages[-1].id == "resp_test"

    @pytest.mark.asyncio
    async def test_empty_args_retry_emits_reasoning_before_answer(self):
        """The empty-tool-args ainvoke retry emits the retry's reasoning
        (parked in additional_kwargs by the non-streaming capture) BEFORE its
        answer text, keyed to the pinned row id — not as a trailing frame."""
        events = []

        async def _on_thinking(content, message_id=None):
            events.append(("thinking", content, message_id))

        async def _on_token(text):
            events.append(("token", text))

        streamed = AIMessage(content="")
        streamed.tool_calls = [
            {"name": "some_tool", "args": {}, "id": "call_1", "type": "tool_call"}
        ]
        retry = AIMessage(
            content="Answer.",
            additional_kwargs={"reasoning_content": "retry thoughts"},
        )
        llm = _make_streaming_llm(streamed)
        llm.reasoning = {"effort": "high", "summary": "auto"}
        llm.ainvoke = AsyncMock(return_value=retry)

        callbacks = _make_callbacks(on_thinking=_on_thinking, on_token=_on_token)
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_awaited_once()
        assert events == [
            ("thinking", "retry thoughts", messages[-1].id),
            ("token", "Answer."),
        ]
        assert messages[-1].id.startswith("msg_")

    @pytest.mark.asyncio
    async def test_streaming_failed_fallback_emits_reasoning_before_answer(self):
        """The streaming-not-supported ainvoke fallback emits reasoning before
        the answer text too (fixes the pre-existing trailing-frame ordering
        for Chat-Completions models on this path)."""
        events = []

        async def _on_thinking(content, message_id=None):
            events.append(("thinking", content, message_id))

        async def _on_token(text):
            events.append(("token", text))

        class APIConnectionError(Exception):
            pass

        async def _astream(messages, **kw):
            raise APIConnectionError("stream unsupported")
            yield  # pragma: no cover — makes this an async generator

        fallback = AIMessage(
            content="Answer.",
            additional_kwargs={"reasoning_content": "fallback thoughts"},
        )
        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream
        llm.ainvoke = AsyncMock(return_value=fallback)

        callbacks = _make_callbacks(on_thinking=_on_thinking, on_token=_on_token)
        _, messages = await _run_turn(llm, callbacks)

        llm.ainvoke.assert_awaited_once()
        assert events == [
            ("thinking", "fallback thoughts", messages[-1].id),
            ("token", "Answer."),
        ]
        assert messages[-1].id.startswith("msg_")


# ---------------------------------------------------------------------------
# Transient context injection placement (Gemini function-call turn ordering)
# ---------------------------------------------------------------------------


def _first_index(messages, predicate):
    return next((i for i, m in enumerate(messages) if predicate(m)), None)


def _is_tool_call_ai(m) -> bool:
    return isinstance(m, AIMessage) and bool(getattr(m, "tool_calls", None))


def _memory_pair(content: str):
    """A real (AIMessage(tool_call), ToolMessage) pair, as the seam produces."""
    from agent.core.memory_injection import create_memory_injection_messages

    return list(create_memory_injection_messages(content))


class TestInjectionAnchorIndex:
    """`_injection_anchor_index` anchors injected pairs at the tail.

    Tail placement is the prompt-cache optimization: the injected block
    changes every turn, so it must sit AFTER the stable conversation prefix
    or it invalidates the provider cache for the whole history each request.
    The anchor is the position after the LAST Human/Tool message (normally
    the very end), which also keeps the synthetic function-call pairs valid
    for Gemini's turn-ordering rule.
    """

    def test_tail_after_newest_human(self):
        msgs = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        assert _injection_anchor_index(msgs) == 2

    def test_tail_after_last_human_of_many(self):
        msgs = [
            SystemMessage(content="sys"),
            HumanMessage(content="h1"),
            AIMessage(content="a1"),
            HumanMessage(content="h2"),
        ]
        assert _injection_anchor_index(msgs) == 4

    def test_tail_after_trailing_tool_message(self):
        msgs = [
            HumanMessage(content="hi"),
            AIMessage(
                content="",
                tool_calls=[{"name": "t", "args": {}, "id": "call_1"}],
            ),
            ToolMessage(content="result", tool_call_id="call_1"),
        ]
        assert _injection_anchor_index(msgs) == 3

    def test_walks_back_past_trailing_bare_model_turn(self):
        # Degenerate/restored history ending in a plain assistant message:
        # the pairs must not follow a model turn (Gemini 400), so the anchor
        # steps back to just after the last Human/Tool message.
        msgs = [HumanMessage(content="hi"), AIMessage(content="yo")]
        assert _injection_anchor_index(msgs) == 1

    def test_no_human_or_tool_falls_back_to_end(self):
        assert _injection_anchor_index([]) == 0
        assert _injection_anchor_index([SystemMessage(content="sys")]) == 1
        assert _injection_anchor_index([AIMessage(content="a")]) == 1


class TestInjectContextPairs:
    """Injected memory/knowledge pairs must never precede the first user turn.

    Regression for the Gemini 400 "Please ensure that function call turn comes
    immediately after a user turn or after a function response turn": the
    injected pairs are synthetic AIMessage(tool_call)+ToolMessage, and placing
    them right after the SystemMessage (before the first HumanMessage) made the
    function call the first entry of Gemini's `contents` array — failing the
    very first turn of a session. The pairs now anchor at the TAIL (after the
    last Human/Tool message) — prompt-cache-friendly AND trivially after the
    first user turn, so the Gemini invariant holds by construction.
    """

    def _assert_gemini_ordering(self, messages):
        """No tool-call AIMessage or ToolMessage may precede the first user turn."""
        first_human = _first_index(messages, lambda m: isinstance(m, HumanMessage))
        first_tool = _first_index(
            messages, lambda m: _is_tool_call_ai(m) or isinstance(m, ToolMessage)
        )
        assert first_human is not None, "expected a user turn in the history"
        assert first_tool is None or first_human < first_tool, (
            f"tool turn at {first_tool} precedes first user turn at {first_human}"
        )

    def _assert_pairing(self, messages):
        call_ids = [
            tc["id"] for m in messages if _is_tool_call_ai(m) for tc in m.tool_calls
        ]
        result_ids = [m.tool_call_id for m in messages if isinstance(m, ToolMessage)]
        assert sorted(call_ids) == sorted(result_ids)
        # Each injected result immediately follows its call (AI then Tool).
        for i, m in enumerate(messages):
            if _is_tool_call_ai(m):
                nxt = messages[i + 1]
                assert isinstance(nxt, ToolMessage)
                assert nxt.tool_call_id in {tc["id"] for tc in m.tool_calls}

    def test_memory_injected_after_first_human(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        count = _inject_context_pairs(prepared, [], "MEMORIES", "")
        assert count == 2
        self._assert_gemini_ordering(prepared)
        # The fixed shape: System, Human, AI(tool_call), Tool — NOT the old
        # System, AI(tool_call), Tool, Human that Gemini rejects.
        assert isinstance(prepared[0], SystemMessage)
        assert isinstance(prepared[1], HumanMessage)
        assert _is_tool_call_ai(prepared[2])
        assert isinstance(prepared[3], ToolMessage)

    def test_product_guide_boundary_is_tail_user_after_legacy_memory(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="product?")]
        boundary = "<managed_product_guide_turn_boundary>fresh</managed>"

        count = _inject_context_pairs(
            prepared,
            [],
            "HISTORICAL MEMORY",
            "",
            product_guide_turn_boundary=boundary,
        )

        assert count == 3
        assert isinstance(prepared[-2], ToolMessage)
        assert prepared[-2].content == "HISTORICAL MEMORY"
        assert isinstance(prepared[-1], HumanMessage)
        assert prepared[-1].content == boundary

    def test_product_guide_boundary_is_tail_user_after_manager_memory(self):
        manager = _memory_pair("MANAGER MEMORY")
        original_content = manager[1].content
        prepared = [SystemMessage(content="sys"), HumanMessage(content="product?")]
        boundary = "<managed_product_guide_turn_boundary>fresh</managed>"

        count = _inject_context_pairs(
            prepared,
            manager,
            "",
            "",
            product_guide_turn_boundary=boundary,
        )

        assert count == 3
        assert manager[1].content == original_content
        injected = next(
            message for message in prepared if isinstance(message, ToolMessage)
        )
        assert injected.content == original_content
        assert isinstance(prepared[-1], HumanMessage)
        assert prepared[-1].content == boundary

    def test_product_guide_boundary_follows_tail_knowledge_too(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="product?")]
        boundary = "<managed_product_guide_turn_boundary>fresh</managed>"

        count = _inject_context_pairs(
            prepared,
            [],
            "",
            "KNOWLEDGE",
            product_guide_turn_boundary=boundary,
        )

        assert count == 3
        assert isinstance(prepared[-2], ToolMessage)
        assert isinstance(prepared[-1], HumanMessage)
        assert prepared[-1].content == boundary

    def test_product_guide_boundary_is_added_without_transient_context(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="product?")]
        boundary = "<managed_product_guide_turn_boundary>fresh</managed>"

        count = _inject_context_pairs(
            prepared,
            [],
            "",
            "",
            product_guide_turn_boundary=boundary,
        )

        assert count == 1
        assert isinstance(prepared[-1], HumanMessage)
        assert prepared[-1].content == boundary

    def test_knowledge_injected_after_first_human(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        count = _inject_context_pairs(prepared, [], "", "KNOWLEDGE")
        assert count == 2
        self._assert_gemini_ordering(prepared)

    def test_all_three_sources_ordering_and_pairing(self):
        manager = _memory_pair("MANAGER")  # seam payload is itself tool-call pairs
        prepared = [
            SystemMessage(content="sys"),
            HumanMessage(content="h1"),
            AIMessage(content="a1"),
            HumanMessage(content="h2"),
        ]
        count = _inject_context_pairs(prepared, manager, "MEM", "KB")
        assert count == len(manager) + 4
        self._assert_gemini_ordering(prepared)
        self._assert_pairing(prepared)

    def test_no_injection_when_all_empty(self):
        prepared = [SystemMessage(content="sys"), HumanMessage(content="hi")]
        before = list(prepared)
        count = _inject_context_pairs(prepared, [], "", "")
        assert count == 0
        assert prepared == before


class TestMaybeEstimateReasoningTokens:
    """_maybe_estimate_reasoning_tokens — backfill reasoning tokens from text
    when the provider reports none (e.g. gemma via vLLM)."""

    def test_estimates_and_flags_when_no_provider_count(self):
        tm = {"input_tokens": 10_000, "output_tokens": 400, "model": "gemma-4-moe"}
        _maybe_estimate_reasoning_tokens(tm, "Let me think: 17*23 = 391. " * 5)
        assert tm["reasoning_tokens"] > 0
        assert tm["reasoning_estimated"] is True
        # output is the provider's number, untouched; the estimate sits within it.
        assert tm["output_tokens"] == 400
        assert tm["reasoning_tokens"] <= tm["output_tokens"]

    def test_clamps_estimate_to_output_tokens(self):
        # A long reasoning blob whose tiktoken estimate would exceed the tiny
        # provider output count must clamp — reasoning can't exceed its output.
        tm = {"output_tokens": 5, "model": "gemma-4-moe"}
        _maybe_estimate_reasoning_tokens(tm, "reasoning " * 200)
        assert tm["reasoning_tokens"] == 5
        assert tm["reasoning_estimated"] is True

    def test_noop_when_provider_reported_reasoning(self):
        tm = {"output_tokens": 500, "reasoning_tokens": 200, "model": "o1"}
        _maybe_estimate_reasoning_tokens(tm, "some reasoning text")
        assert tm["reasoning_tokens"] == 200
        assert "reasoning_estimated" not in tm

    def test_noop_when_no_reasoning_text(self):
        tm = {"output_tokens": 50, "model": "gemma-4-moe"}
        _maybe_estimate_reasoning_tokens(tm, "")
        assert "reasoning_tokens" not in tm
        assert "reasoning_estimated" not in tm


# =============================================================================
# PERSIST_ROLE_KEY import path (re-exported from src.core.message_markers)
# =============================================================================


class TestPersistRoleKeyReexport:
    def test_persist_role_key_is_the_message_markers_constant(self):
        from shared.runtime.core.message_markers import PERSIST_ROLE_KEY as canonical
        from agent.persistent_graph import PERSIST_ROLE_KEY

        assert PERSIST_ROLE_KEY is canonical
        assert PERSIST_ROLE_KEY == "_srw_persist_role"
