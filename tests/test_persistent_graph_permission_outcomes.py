"""Supervised gate outcomes in the src/persistent_graph.py tool loop.

Regression tests for
knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md.

A supervised gate is a *question to the user*, so the loop must
distinguish three outcomes and never fabricate a decision the user did
not make:

* ``approved``  -> run the tool.
* ``declined``  -> the user really said no; tell the model so.
* no answer     -> the gate was never answered (timed out / the card
  never reached the browser). The loop must NOT tell the model the user
  denied it, and must not run the tool.

The live repro that motivated this: a model emitted four parallel
``web_search`` calls, the user approved the first, and each remaining
gate hit the 300 s TTL and was written into the conversation as the
literal "User denied this tool call." — a refusal the user never made.
"""

from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from agent.persistent_graph import (
    USER_INTERRUPT_CAUSE,
    PermissionOutcome,
    PersistentLoopCallbacks,
    run_persistent_loop,
)
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE
from agent.unrun_tool_calls import (
    NOT_RUN_APPROVAL_EXPIRED,
    NOT_RUN_APPROVAL_UNANSWERED,
    NOT_RUN_STOPPED_AT_APPROVAL,
    NOT_RUN_STOPPED_BEFORE_START,
    SKIPPED_TURN_ENDED,
)
from shared.tool_result_state import RESULT_NOT_RUN, TOOL_RESULT_STATE_KEY


# =============================================================================
# Helpers
# =============================================================================


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
        announce_permission_batch=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _make_config():
    cfg = MagicMock()
    cfg.llm.timeout = 600
    cfg.memory.enabled = False
    cfg.memory.observer_interval = 5
    cfg.context_management.max_summary_length = 10000
    return cfg


def _make_tool(name: str, result: str):
    tool = MagicMock()
    tool.name = name
    tool.ainvoke = AsyncMock(return_value=result)
    return tool


async def _run_turn_with_parallel_tools(
    *,
    permission_check,
    messages: list[BaseMessage],
    n_calls: int = 2,
    announce_permission_batch=None,
    tool=None,
    **callback_overrides,
):
    """Drive one turn whose single AIMessage carries ``n_calls`` parallel
    tool calls, then a final tool-free AIMessage ends the turn.

    ``tool`` replaces the bound ``web_search`` double; ``callback_overrides``
    set further ``PersistentLoopCallbacks`` fields."""
    response_with_tools = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "web_search",
                "args": {"query": f"capital of {country}"},
                "id": f"tc_{i}",
            }
            for i, country in enumerate(
                ["France", "Japan", "Brazil", "Egypt"][:n_calls]
            )
        ],
    )
    final_response = AIMessage(content="Done.")

    call_count = 0

    async def _astream(_messages, **kw):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield response_with_tools
        else:
            yield final_response

    llm = AsyncMock()
    llm.reasoning = None
    llm.astream = _astream

    if tool is None:
        tool = _make_tool("web_search", "Results: Paris.")

    turns = 0

    async def _input():
        nonlocal turns
        turns += 1
        if turns == 1:
            return "look up four capitals"
        raise asyncio.CancelledError

    callbacks = _make_callbacks(
        get_user_input=_input,
        permission_check=permission_check,
        announce_permission_batch=announce_permission_batch,
        **callback_overrides,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_make_config(),
        system_prompt="sys",
        callbacks=callbacks,
        messages=messages,
    )
    return tool


# =============================================================================
# Tests
# =============================================================================


class TestUnansweredGateIsNotADenial:
    @pytest.mark.asyncio
    async def test_unanswered_gate_does_not_tell_model_user_denied(self):
        """The live bug: approve #1, leave #2 unanswered -> the model was
        told "User denied this tool call." for a call the user never saw.

        Runs with the default ``close_unrun_calls=False``, the subagent path,
        which is why it still expects no ToolMessage for the unanswered call."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            if tool_call_id == "tc_0":
                return PermissionOutcome.APPROVED
            return PermissionOutcome.NO_ANSWER

        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages
        )

        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        bodies = [str(m.content) for m in tool_msgs]

        # The whole point: no fabricated refusal anywhere in the transcript.
        assert not any("User denied" in b for b in bodies), (
            f"unanswered gate was reported to the model as a denial: {bodies}"
        )

        # The approved call still ran and produced its real result.
        assert any("Results: Paris." in b for b in bodies)

        # The unanswered call contributed no ToolMessage at all — it is
        # still pending, not resolved.
        assert not any(m.tool_call_id == "tc_1" for m in tool_msgs)

    @pytest.mark.asyncio
    async def test_unanswered_gate_does_not_execute_the_tool(self):
        """No answer must never be read as consent."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.NO_ANSWER

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=1
        )

        tool.ainvoke.assert_not_called()


def _gates(outcomes: dict[str, PermissionOutcome]):
    """A permission check answering each call id from ``outcomes``."""

    async def _permission_check(tool_name, tool_args, tool_call_id):
        return outcomes.get(tool_call_id, PermissionOutcome.APPROVED)

    return _permission_check


def _recorder():
    """A ``persist_message`` double and the list it writes to."""
    persisted: list[BaseMessage] = []

    async def _persist(message):
        persisted.append(message)
        return True

    return persisted, _persist


def _tool_results(messages) -> dict[str, list[ToolMessage]]:
    by_id: dict[str, list[ToolMessage]] = {}
    for message in messages:
        if isinstance(message, ToolMessage):
            by_id.setdefault(message.tool_call_id, []).append(message)
    return by_id


class TestSessionTurnClosesUnrunCalls:
    """A Session turn (``close_unrun_calls``) leaves no call without a result.

    A call left without a ``role='tool'`` row is tool debt, and the next
    message's admission check stops the stateless executor over it
    (knowledge-base/knowledge/issues/stateless_unanswered_approval_stops_chat_executors.md).
    The turn writes a truthful ``not_run`` result instead, never a refusal.
    """

    @pytest.mark.asyncio
    async def test_session_no_answer_writes_not_run_for_the_gated_call(self):
        on_tool_result = AsyncMock()
        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_gates({"tc_1": PermissionOutcome.NO_ANSWER}),
            messages=messages,
            close_unrun_calls=True,
            permission_no_answer_status={"tc_1": "expired"}.get,
            on_tool_result=on_tool_result,
        )

        results = _tool_results(messages)
        assert len(results["tc_1"]) == 1
        row = results["tc_1"][0]
        assert row.content == NOT_RUN_APPROVAL_EXPIRED
        assert row.additional_kwargs[TOOL_RESULT_STATE_KEY] == RESULT_NOT_RUN
        assert not any(
            "User denied" in str(m.content) for rows in results.values() for m in rows
        )
        # A call that never started sends no result frame.
        assert all(call.args[2] != "tc_1" for call in on_tool_result.await_args_list)
        assert tool.ainvoke.await_count == 1

    @pytest.mark.asyncio
    async def test_no_answer_park_closes_later_calls_as_skipped(self):
        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_gates({"tc_1": PermissionOutcome.NO_ANSWER}),
            messages=messages,
            n_calls=3,
            close_unrun_calls=True,
            permission_no_answer_status={"tc_1": "expired"}.get,
        )

        tool_rows = [m for m in messages if isinstance(m, ToolMessage)]
        assert [m.tool_call_id for m in tool_rows] == ["tc_0", "tc_1", "tc_2"]
        assert tool_rows[0].content == "Results: Paris."
        assert tool_rows[2].content == SKIPPED_TURN_ENDED
        assert tool_rows[2].additional_kwargs[TOOL_RESULT_STATE_KEY] == RESULT_NOT_RUN

    @pytest.mark.asyncio
    async def test_platform_interrupt_during_approval_uses_neutral_text(self):
        """A lease loss or an owner-fence rejection returns 'interrupted' with
        no person's Stop pending: the text must not say the user stopped."""
        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_gates({"tc_1": PermissionOutcome.NO_ANSWER}),
            messages=messages,
            close_unrun_calls=True,
            permission_no_answer_status={"tc_1": "interrupted"}.get,
            peek_interrupt_cause=lambda: None,
        )

        assert _tool_results(messages)["tc_1"][0].content == (
            NOT_RUN_APPROVAL_UNANSWERED
        )

    @pytest.mark.asyncio
    async def test_user_stop_during_approval(self):
        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_gates({"tc_1": PermissionOutcome.NO_ANSWER}),
            messages=messages,
            close_unrun_calls=True,
            permission_no_answer_status={"tc_1": "interrupted"}.get,
            peek_interrupt_cause=lambda: USER_INTERRUPT_CAUSE,
        )

        assert _tool_results(messages)["tc_1"][0].content == (
            NOT_RUN_STOPPED_AT_APPROVAL
        )

    @staticmethod
    async def _stop_after_first_tool(
        *,
        close_unrun_calls: bool,
        cause: Optional[str] = USER_INTERRUPT_CAUSE,
        n_calls: int = 2,
    ):
        """Approved calls; an interrupt lands while ``tc_0`` runs.

        ``cause`` is what ``peek_interrupt_cause`` reports for it: a person's
        Stop by default, or the platform's own abort.
        """
        pending: dict[str, str] = {}

        async def _run_tool(_args):
            pending["mode"] = "graceful"
            return "Results: Paris."

        tool = _make_tool("web_search", "")
        tool.ainvoke = AsyncMock(side_effect=_run_tool)

        def _check_interrupt():
            return pending.pop("mode", None)

        def _peek_cause():
            return cause if "mode" in pending else None

        persisted, persist = _recorder()
        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_gates({}),
            messages=messages,
            n_calls=n_calls,
            tool=tool,
            close_unrun_calls=close_unrun_calls,
            check_interrupt=_check_interrupt,
            peek_interrupt_cause=_peek_cause,
            persist_message=persist,
        )
        assert tool.ainvoke.await_count == 1
        return messages, persisted

    @pytest.mark.asyncio
    async def test_stop_between_tools_closes_remaining_calls(self):
        messages, persisted = await self._stop_after_first_tool(close_unrun_calls=True)

        row = _tool_results(messages)["tc_1"]
        assert len(row) == 1
        assert row[0].content == NOT_RUN_STOPPED_BEFORE_START
        assert row[0].additional_kwargs[TOOL_RESULT_STATE_KEY] == RESULT_NOT_RUN
        assert row[0] in persisted
        assert not any("[tool-call interruption]" in str(m.content) for m in persisted)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cause", ["shutdown", None])
    async def test_platform_interrupt_between_tools_never_says_the_user_stopped(
        self, cause
    ):
        """A platform abort (an executor shutting down, a lost lease) before a
        tool is not a person's Stop: the calls left are skipped, not stopped."""
        messages, persisted = await self._stop_after_first_tool(
            close_unrun_calls=True, cause=cause, n_calls=3
        )

        results = _tool_results(messages)
        for call_id in ("tc_1", "tc_2"):
            assert len(results[call_id]) == 1
            row = results[call_id][0]
            assert row.content == SKIPPED_TURN_ENDED
            assert row.content != NOT_RUN_STOPPED_BEFORE_START
            assert row.additional_kwargs[TOOL_RESULT_STATE_KEY] == RESULT_NOT_RUN
            assert row in persisted

    @pytest.mark.asyncio
    async def test_subagent_default_keeps_the_interruption_event(self):
        messages, persisted = await self._stop_after_first_tool(close_unrun_calls=False)

        assert any("[tool-call interruption]" in str(m.content) for m in persisted)
        assert "tc_1" not in _tool_results(messages)

    @pytest.mark.asyncio
    async def test_unrun_results_are_persisted_before_flushed_images(self):
        """Tool results stay contiguous after the assistant message; the
        image of ``tc_0`` follows every result of the batch."""
        image = "[IMAGE: a.png]\n" + IMAGE_DATA_TAG_TEMPLATE.format(
            mime="image/png", b64="Zmlyc3QtcG5n"
        )
        persisted, persist = _recorder()
        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_gates({"tc_1": PermissionOutcome.NO_ANSWER}),
            messages=messages,
            tool=_make_tool("web_search", image),
            close_unrun_calls=True,
            permission_no_answer_status={"tc_1": "expired"}.get,
            persist_message=persist,
        )

        def _index(predicate) -> int:
            return next(i for i, m in enumerate(persisted) if predicate(m))

        tc_0 = _index(lambda m: getattr(m, "tool_call_id", None) == "tc_0")
        tc_1 = _index(lambda m: getattr(m, "tool_call_id", None) == "tc_1")
        flushed_image = _index(
            lambda m: isinstance(m, HumanMessage) and isinstance(m.content, list)
        )
        assert tc_0 < tc_1 < flushed_image


class TestExplicitDenyStillReported:
    @pytest.mark.asyncio
    async def test_explicit_deny_tells_model_the_user_declined(self):
        """A real "no" must still reach the model — and read as the user's
        decision, not as a timeout."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.DECLINED

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=1
        )

        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1
        assert "declined" in str(tool_msgs[0].content).lower()
        tool.ainvoke.assert_not_called()


class TestApprovedPathUnchanged:
    @pytest.mark.asyncio
    async def test_approved_calls_all_run(self):
        """Regression guard: approving every gate runs every tool."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=2
        )

        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 2
        assert all("Results: Paris." in str(m.content) for m in tool_msgs)
        assert tool.ainvoke.await_count == 2

    @pytest.mark.asyncio
    async def test_legacy_bool_true_still_approves(self):
        """Back-compat: callbacks that still return a plain bool keep working."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return True

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=1
        )

        assert tool.ainvoke.await_count == 1
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert "Results: Paris." in str(tool_msgs[0].content)

    @pytest.mark.asyncio
    async def test_legacy_bool_false_reads_as_explicit_deny(self):
        """Back-compat: a plain False keeps its old "user said no" meaning."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return False

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=1
        )

        tool.ainvoke.assert_not_called()
        tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
        assert len(tool_msgs) == 1


class TestBatchAnnounce:
    @pytest.mark.asyncio
    async def test_announce_called_once_with_all_calls_before_gating(self):
        """The whole batch must be announced before the first gate blocks,
        so the user sees every card at once instead of one per finished tool."""
        timeline = []  # Shared list: ("announce", None) or ("gate", tool_call_id)

        async def announce(calls):
            timeline.append(("announce", None))
            assert [c["id"] for c in calls] == ["tc_0", "tc_1", "tc_2", "tc_3"]

        async def _permission_check(tool_name, tool_args, tool_call_id):
            timeline.append(("gate", tool_call_id))
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_parallel_tools(
            permission_check=_permission_check,
            messages=messages,
            n_calls=4,
            announce_permission_batch=announce,
        )

        # Announce must precede all gates: timeline is [("announce", None), ("gate", "tc_0"), ...]
        assert timeline[0] == ("announce", None), (
            f"announce must fire first, got: {timeline}"
        )
        assert timeline[1:] == [("gate", f"tc_{i}") for i in range(4)], (
            f"all gates must follow announce, got: {timeline}"
        )

    @pytest.mark.asyncio
    async def test_no_announce_callback_still_works(self):
        """Back-compat: callers that never set the callback are unaffected."""

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check, messages=messages, n_calls=2
        )
        assert tool.ainvoke.await_count == 2

    @pytest.mark.asyncio
    async def test_announce_exception_does_not_break_turn(self):
        """Soft-fail: if announce raises, gates and tools still execute normally."""
        timeline = []

        async def announce_breaks(calls):
            timeline.append("announce_called")
            raise RuntimeError("boom")

        async def _permission_check(tool_name, tool_args, tool_call_id):
            timeline.append(("gate", tool_call_id))
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_parallel_tools(
            permission_check=_permission_check,
            messages=messages,
            n_calls=2,
            announce_permission_batch=announce_breaks,
        )

        # Announce was attempted but raised; gates and tools still executed
        assert "announce_called" in timeline
        assert ("gate", "tc_0") in timeline
        assert ("gate", "tc_1") in timeline
        # Both tools ran despite announce failure
        assert tool.ainvoke.await_count == 2


async def _run_turn_with_named_calls(
    *,
    calls: list[tuple[str, str]],
    messages: list[BaseMessage],
    permission_check,
    announce_permission_batch=None,
):
    """Drive one turn whose AIMessage carries ``calls`` as (tool_name, id).

    Only ``web_search`` is bound, so any other name models a tool the agent
    asked for that the capability gate never bound.
    """
    response_with_tools = AIMessage(
        content="",
        tool_calls=[
            {"name": name, "args": {"query": "x"}, "id": call_id}
            for name, call_id in calls
        ],
    )
    final_response = AIMessage(content="Done.")

    call_count = 0

    async def _astream(_messages, **kw):
        nonlocal call_count
        call_count += 1
        yield response_with_tools if call_count == 1 else final_response

    llm = AsyncMock()
    llm.reasoning = None
    llm.astream = _astream

    tool = _make_tool("web_search", "Results: Paris.")

    turns = 0

    async def _input():
        nonlocal turns
        turns += 1
        if turns == 1:
            return "go"
        raise asyncio.CancelledError

    callbacks = _make_callbacks(
        get_user_input=_input,
        permission_check=permission_check,
        announce_permission_batch=announce_permission_batch,
    )

    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_make_config(),
        system_prompt="sys",
        callbacks=callbacks,
        messages=messages,
    )
    return tool


class TestUnboundToolSkipsThePermissionGate:
    """A tool that binds to nothing must be rejected before it is gated.

    The live bug: a supervised session called `shell_execute` on a workspace
    tier with no shell. The existence check sat *after* the gate, so the user
    was shown an approval card for a tool that could not run either way and the
    turn blocked ~53s on that round-trip before reporting "not found".
    """

    @pytest.mark.asyncio
    async def test_unbound_tool_is_never_gated(self):
        gated = []

        async def _permission_check(tool_name, tool_args, tool_call_id):
            gated.append(tool_name)
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_named_calls(
            calls=[("shell_execute", "tc_0")],
            messages=messages,
            permission_check=_permission_check,
        )

        assert gated == [], f"phantom tool reached the permission gate: {gated}"

    @pytest.mark.asyncio
    async def test_unbound_tool_is_never_announced(self):
        announced = []

        async def announce(calls):
            announced.extend(c["name"] for c in calls)

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_named_calls(
            calls=[("shell_execute", "tc_0"), ("web_search", "tc_1")],
            messages=messages,
            permission_check=_permission_check,
            announce_permission_batch=announce,
        )

        # The real tool is still announced; the phantom raises no card.
        assert announced == ["web_search"], announced

    @pytest.mark.asyncio
    async def test_announce_still_called_when_every_call_is_unbound(self):
        """The hook also retires the previous batch's rows — never skip it."""
        announced_batches = []

        async def announce(calls):
            announced_batches.append([c["name"] for c in calls])

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_named_calls(
            calls=[("shell_execute", "tc_0")],
            messages=messages,
            permission_check=_permission_check,
            announce_permission_batch=announce,
        )

        assert announced_batches == [[]], announced_batches

    @pytest.mark.asyncio
    async def test_error_names_the_reason_not_just_not_found(self):
        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_named_calls(
            calls=[("shell_execute", "tc_0")],
            messages=messages,
            permission_check=_permission_check,
        )

        body = str(next(m for m in messages if isinstance(m, ToolMessage)).content)
        # Names the capability and that it is absent here — enough for the
        # model to re-plan rather than retry.
        assert "shell" in body
        assert "not available in this session" in body
        assert "Do not retry" in body

    @pytest.mark.asyncio
    async def test_unknown_name_reported_as_nonexistent(self):
        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        await _run_turn_with_named_calls(
            calls=[("totally_made_up_tool", "tc_0")],
            messages=messages,
            permission_check=_permission_check,
        )

        body = str(next(m for m in messages if isinstance(m, ToolMessage)).content)
        assert "No tool named 'totally_made_up_tool' exists" in body

    @pytest.mark.asyncio
    async def test_bound_sibling_in_same_batch_still_runs(self):
        """Rejecting a phantom must not disturb the real calls beside it."""
        gated = []

        async def _permission_check(tool_name, tool_args, tool_call_id):
            gated.append(tool_name)
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        tool = await _run_turn_with_named_calls(
            calls=[("shell_execute", "tc_0"), ("web_search", "tc_1")],
            messages=messages,
            permission_check=_permission_check,
        )

        assert gated == ["web_search"]
        tool.ainvoke.assert_awaited_once()
        bodies = {
            m.tool_call_id: str(m.content)
            for m in messages
            if isinstance(m, ToolMessage)
        }
        assert "not available in this session" in bodies["tc_0"]
        assert "Results: Paris." in bodies["tc_1"]

    @pytest.mark.asyncio
    async def test_failed_call_still_surfaces_to_the_client(self):
        """on_tool_start/on_tool_result stay paired so the UI renders it."""
        started, results = [], []

        async def _on_tool_start(name, args, call_id):
            started.append(name)

        async def _on_tool_result(name, result, call_id, is_error=False):
            results.append((name, is_error))

        async def _permission_check(tool_name, tool_args, tool_call_id):
            return PermissionOutcome.APPROVED

        messages: list[BaseMessage] = []
        response_calls = [("shell_execute", "tc_0")]
        callbacks_seen = {}

        async def announce(calls):
            callbacks_seen["announced"] = [c["name"] for c in calls]

        # Re-drive with custom start/result callbacks.
        final_response = AIMessage(content="Done.")
        response_with_tools = AIMessage(
            content="",
            tool_calls=[{"name": n, "args": {}, "id": i} for n, i in response_calls],
        )
        call_count = 0

        async def _astream(_messages, **kw):
            nonlocal call_count
            call_count += 1
            yield response_with_tools if call_count == 1 else final_response

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        turns = 0

        async def _input():
            nonlocal turns
            turns += 1
            if turns == 1:
                return "go"
            raise asyncio.CancelledError

        await run_persistent_loop(
            llm_with_tools=llm,
            tools=[_make_tool("web_search", "r")],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_make_config(),
            system_prompt="sys",
            callbacks=_make_callbacks(
                get_user_input=_input,
                permission_check=_permission_check,
                on_tool_start=_on_tool_start,
                on_tool_result=_on_tool_result,
                announce_permission_batch=announce,
            ),
            messages=messages,
        )

        assert started == ["shell_execute"]
        assert results == [("shell_execute", True)]
