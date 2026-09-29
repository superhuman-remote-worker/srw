"""Restore hands the model its history in conversation order.

Design: knowledge-base/knowledge/features/parallel_subagents.md §10.2 (WP0b,
finding F19). Rows arrive in ``seq`` order, which is write order. Input is
persisted when it is accepted, so text typed while a tool runs sits between
the tool call and its result, and a recovery row for an abandoned turn is
written after input that arrived later. Restore sorts rows by the turn that
consumed them and then places every tool result directly behind its call.
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.api.persistent_app import _conversation_order, _db_rows_to_lc_messages
from agent.core.context import repair_tool_pairing, repair_tool_result_adjacency


def _row(
    row_id: str,
    role: str,
    turn: int | None,
    *,
    calls: tuple[str, ...] = (),
    call_id: str | None = None,
    admitted: int | None = None,
) -> dict:
    return {
        "id": row_id,
        "role": role,
        "content": row_id if role != "ai" or not calls else "",
        "tool_calls": (
            [{"id": call, "name": "delegate_agent", "args": {}} for call in calls]
            if calls
            else None
        ),
        "tool_call_id": call_id,
        "turn_number": turn,
        "admitted_turn_number": admitted,
    }


def _ids(messages: list) -> list[str]:
    return [str(message.id) for message in messages]


def _assert_results_follow_calls(messages: list) -> None:
    """The provider rule: an assistant message with tool calls is followed
    directly by one result per call, before anything else."""

    for index, message in enumerate(messages):
        if isinstance(message, AIMessage) and message.tool_calls:
            wanted = {call["id"] for call in message.tool_calls}
            following = messages[index + 1 : index + 1 + len(wanted)]
            assert all(isinstance(m, ToolMessage) for m in following), _ids(messages)
            assert {m.tool_call_id for m in following} == wanted, _ids(messages)


class TestConversationOrder:
    def test_rows_already_in_turn_order_are_unchanged(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("result", "tool", 1, call_id="c1"),
            _row("a1", "ai", 1),
            _row("q2", "human", 2),
            _row("a2", "ai", 2),
        ]

        assert _ids(_db_rows_to_lc_messages(rows)) == [row["id"] for row in rows]

    def test_input_typed_during_a_tool_call_moves_behind_the_turn(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("typed", "human", 2),
            _row("result", "tool", 1, call_id="c1"),
            _row("a1", "ai", 1),
        ]

        restored = _db_rows_to_lc_messages(rows)

        assert _ids(restored) == ["q1", "call", "result", "a1", "typed"]
        _assert_results_follow_calls(restored)

    def test_an_event_between_a_call_and_its_result_moves_behind_the_turn(
        self,
    ) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("notice", "event", 2),
            _row("result", "tool", 1, call_id="c1"),
        ]

        restored = _db_rows_to_lc_messages(rows)

        assert _ids(restored) == ["q1", "call", "result", "notice"]
        assert isinstance(restored[-1], HumanMessage)

    def test_two_batches_in_one_turn(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("batch-1", "ai", 1, calls=("a1", "a2")),
            _row("typed", "human", 2),
            _row("r-a1", "tool", 1, call_id="a1"),
            _row("r-a2", "tool", 1, call_id="a2"),
            _row("batch-2", "ai", 1, calls=("b1",)),
            _row("r-b1", "tool", 1, call_id="b1"),
            _row("a1", "ai", 1),
        ]

        restored = _db_rows_to_lc_messages(rows)

        assert _ids(restored) == [
            "q1",
            "batch-1",
            "r-a1",
            "r-a2",
            "batch-2",
            "r-b1",
            "a1",
            "typed",
        ]
        _assert_results_follow_calls(restored)

    def test_two_inputs_typed_during_one_turn_keep_their_order(self) -> None:
        # The stateless lane numbers each accepted input total_turns + 1.
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("typed-1", "human", 2),
            _row("typed-2", "human", 3),
            _row("result", "tool", 1, call_id="c1"),
            _row("a1", "ai", 1),
        ]

        assert _ids(_db_rows_to_lc_messages(rows)) == [
            "q1",
            "call",
            "result",
            "a1",
            "typed-1",
            "typed-2",
        ]

    def test_pinned_input_follows_the_turn_that_admitted_it(self) -> None:
        # The pinned lane numbers both inputs typed during turn 1 as turn 2;
        # the delivery records that the second one ran as turn 3.
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("typed-1", "human", 2, admitted=2),
            _row("typed-2", "human", 2, admitted=3),
            _row("result", "tool", 1, call_id="c1"),
            _row("a1", "ai", 1),
            _row("a2", "ai", 2),
            _row("a3", "ai", 3),
        ]

        assert _ids(_db_rows_to_lc_messages(rows)) == [
            "q1",
            "call",
            "result",
            "a1",
            "typed-1",
            "a2",
            "typed-2",
            "a3",
        ]

    def test_a_recovery_row_of_an_abandoned_turn_moves_ahead_of_later_input(
        self,
    ) -> None:
        # The executor died during turn 1. The recovery writes turn 1's result
        # and continuation after the user's next message, and the executor
        # serves the recovery first.
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("typed", "human", 2),
            _row("recovered-result", "tool", 1, call_id="c1"),
            _row("recovery-event", "event", 1, admitted=1),
            _row("recovery-answer", "ai", 1),
        ]

        restored = _db_rows_to_lc_messages(rows)

        assert _ids(restored) == [
            "q1",
            "call",
            "recovered-result",
            "recovery-event",
            "recovery-answer",
            "typed",
        ]
        _assert_results_follow_calls(restored)

    def test_a_row_without_a_turn_keeps_its_place(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("a1", "ai", 1),
            _row("notice", "event", None),
            _row("q2", "human", 2),
            _row("a2", "ai", 2),
        ]

        assert _ids(_db_rows_to_lc_messages(rows)) == [row["id"] for row in rows]

    def test_rows_without_any_turn_keep_seq_order(self) -> None:
        rows = [
            {"id": "q1", "role": "human", "content": "q1"},
            {"id": "a1", "role": "ai", "content": "a1"},
            {"id": "q2", "role": "human", "content": "q2"},
        ]

        assert [row["id"] for row in _conversation_order(rows)] == ["q1", "a1", "q2"]

    def test_a_result_whose_call_is_missing_is_dropped_by_the_pairing(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("typed", "human", 2),
            _row("stray", "tool", 1, call_id="gone"),
            _row("a1", "ai", 1),
        ]

        restored = repair_tool_pairing(_db_rows_to_lc_messages(rows))

        assert _ids(restored) == ["q1", "a1", "typed"]

    def test_a_call_whose_result_is_missing_is_stripped_by_the_pairing(
        self,
    ) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("batch", "ai", 1, calls=("c1", "c2")),
            _row("typed", "human", 2),
            _row("r-c1", "tool", 1, call_id="c1"),
        ]

        restored = repair_tool_pairing(_db_rows_to_lc_messages(rows))

        assert _ids(restored) == ["q1", "batch", "r-c1", "typed"]
        assert [call["id"] for call in restored[1].tool_calls] == ["c1"]
        _assert_results_follow_calls(restored)

    def test_an_image_after_the_last_result_stays_behind_it(self) -> None:
        rows = [
            _row("q1", "human", 1),
            _row("call", "ai", 1, calls=("c1",)),
            _row("result", "tool", 1, call_id="c1"),
            _row("image", "event", 1),
            _row("a1", "ai", 1),
        ]

        assert _ids(_db_rows_to_lc_messages(rows)) == [row["id"] for row in rows]

    def test_an_image_between_two_results_of_a_batch_moves_behind_them(
        self,
    ) -> None:
        # The live loop appends a tool's image right behind that tool's result,
        # which puts it between the results of a parallel batch.
        rows = [
            _row("q1", "human", 1),
            _row("batch", "ai", 1, calls=("c1", "c2")),
            _row("r-c1", "tool", 1, call_id="c1"),
            _row("image-c1", "event", 1),
            _row("r-c2", "tool", 1, call_id="c2"),
            _row("a1", "ai", 1),
        ]

        restored = _db_rows_to_lc_messages(rows)

        assert _ids(restored) == ["q1", "batch", "r-c1", "r-c2", "image-c1", "a1"]
        _assert_results_follow_calls(restored)


class TestRepairToolResultAdjacency:
    def _history(self) -> list:
        return [
            HumanMessage(content="q1", id="q1"),
            AIMessage(
                content="",
                id="batch",
                tool_calls=[
                    {"id": "c1", "name": "t", "args": {}},
                    {"id": "c2", "name": "t", "args": {}},
                ],
            ),
            ToolMessage(content="r1", tool_call_id="c1", id="r-c1"),
            HumanMessage(content="typed", id="typed"),
            ToolMessage(content="r2", tool_call_id="c2", id="r-c2"),
        ]

    def test_results_move_directly_behind_their_call(self) -> None:
        repaired = repair_tool_result_adjacency(self._history())

        assert _ids(repaired) == ["q1", "batch", "r-c1", "r-c2", "typed"]

    def test_input_is_not_mutated_and_a_second_pass_is_a_no_op(self) -> None:
        history = self._history()
        before = list(history)

        once = repair_tool_result_adjacency(history)
        twice = repair_tool_result_adjacency(once)

        assert history == before
        assert _ids(twice) == _ids(once)

    def test_a_result_before_its_call_moves_behind_it(self) -> None:
        history = [
            HumanMessage(content="q1", id="q1"),
            ToolMessage(content="r1", tool_call_id="c1", id="r-c1"),
            AIMessage(
                content="",
                id="call",
                tool_calls=[{"id": "c1", "name": "t", "args": {}}],
            ),
        ]

        assert _ids(repair_tool_result_adjacency(history)) == ["q1", "call", "r-c1"]

    def test_a_result_without_a_call_stays_in_place(self) -> None:
        history = [
            HumanMessage(content="q1", id="q1"),
            ToolMessage(content="r", tool_call_id="gone", id="stray"),
            AIMessage(content="a1", id="a1"),
        ]

        assert _ids(repair_tool_result_adjacency(history)) == ["q1", "stray", "a1"]

    def test_a_valid_history_is_returned_unchanged(self) -> None:
        history = [
            HumanMessage(content="q1", id="q1"),
            AIMessage(
                content="",
                id="call",
                tool_calls=[{"id": "c1", "name": "t", "args": {}}],
            ),
            ToolMessage(content="r1", tool_call_id="c1", id="r-c1"),
            AIMessage(content="a1", id="a1"),
        ]

        assert _ids(repair_tool_result_adjacency(history)) == _ids(history)
