"""The owner's decision on a parked Session's unknown tool results.

A stateless Session parks when its transcript holds a tool call without a
result (``shared.session_tool_debt``): the call started, its process went away,
and nothing can prove whether it finished. Replaying the turn automatically
could repeat a side effect, so the owner decides, from the same kind of card as
a tool approval: **continue** with the result unknown, or **stop here**.

Either way every unanswered call gets a truthful result row and the interrupted
turn's input counts as consumed, so the request is never replayed after its own
partial work. Input sent afterwards still runs. Continue with nothing else
waiting asks the router to admit a ``Continue.`` input, the model-natural
resume. Design: knowledge-base/knowledge/issues/
session_unknown_tool_result_needs_admin.md (F4).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from orchestrator.services.stateless_queue_state import unknown_result_refusal
from shared.run_queue import (
    PARK_REASON_CLAIM_LOSS_HOLD,
    STATE_DONE,
    STATE_PARKED,
    settle_parked_unit,
)
from shared.session_tool_debt import has_tool_debt, unanswered_tool_calls
from shared.tool_result_state import RESULT_UNKNOWN, tool_result_state_metrics

DECISION_CONTINUE = "continue"
DECISION_STOP = "stop"

CONTINUE_INPUT = "Continue."

_RESULT_UNKNOWN = (
    "Result unknown: this call was interrupted before its result was recorded "
    "(the process running it was replaced or lost the workspace), so it may "
    "or may not have finished."
)
_DECISION_NOTE = {
    DECISION_CONTINUE: " The user chose to continue.",
    DECISION_STOP: (
        " The user chose to stop here: do not resume this work unless they ask."
    ),
}


class UnknownToolResultRefused(Exception):
    """The decision cannot be applied; ``status`` and ``code`` for the API."""

    def __init__(self, status: int, code: str) -> None:
        super().__init__(code)
        self.status = status
        self.code = code


@dataclass(frozen=True, slots=True)
class UnknownToolResultOutcome:
    state: str
    call_ids: list[str]
    needs_continue_input: bool


def result_text(decision: str) -> str:
    """The tool result recorded for every unanswered call under ``decision``."""
    return _RESULT_UNKNOWN + _DECISION_NOTE[decision]


async def decide_unknown_tool_results(
    conn: Any, *, thread_id: str, decision: str
) -> UnknownToolResultOutcome:
    """Answer every unanswered call and lift the park, in one transaction.

    Lock order threads → run_queue, as every other queue transition. Refused
    (and rolled back) when the thread is not a parked stateless Session, a
    stop marker or claim-loss hold is present, there is nothing to answer, or
    the history stays ambiguous after answering (malformed or duplicate call
    ids, which remain an operator's case).
    """
    if decision not in _DECISION_NOTE:
        raise UnknownToolResultRefused(400, "invalid_decision")
    async with conn.transaction():
        thread = await conn.fetchrow(
            "SELECT execution_lane, metadata FROM threads "
            "WHERE id = $1::uuid FOR UPDATE",
            thread_id,
        )
        if thread is None:
            raise UnknownToolResultRefused(404, "thread_not_found")
        if str(thread["execution_lane"] or "") != "stateless":
            raise UnknownToolResultRefused(409, "not_stateless")
        refusal = unknown_result_refusal(thread["metadata"])
        if refusal is not None:
            raise UnknownToolResultRefused(409, refusal)
        queue = await conn.fetchrow(
            "SELECT state, park_reason FROM run_queue "
            "WHERE unit_id = $1::uuid FOR UPDATE",
            thread_id,
        )
        if queue is None or queue["state"] != STATE_PARKED:
            raise UnknownToolResultRefused(404, "not_parked")
        if queue["park_reason"] == PARK_REASON_CLAIM_LOSS_HOLD:
            raise UnknownToolResultRefused(409, "claim_loss_hold")
        calls = await unanswered_tool_calls(conn, thread_id)
        if not calls:
            raise UnknownToolResultRefused(409, "no_unknown_results")
        content = result_text(decision)
        for call in calls:
            await conn.execute(
                "INSERT INTO thread_messages "
                "(thread_id, role, content, turn_number, tool_call_id, metrics) "
                "VALUES ($1::uuid, 'tool', $2, $3, $4, $5::jsonb)",
                thread_id,
                content,
                call["turn_number"],
                call["call_id"],
                json.dumps(tool_result_state_metrics(RESULT_UNKNOWN, call["call_id"])),
            )
        if await has_tool_debt(conn, thread_id):
            raise UnknownToolResultRefused(409, "tool_history_ambiguous")
        interrupted_input_seq = await conn.fetchval(
            "SELECT max(seq) FROM thread_messages "
            "WHERE thread_id = $1::uuid AND role = 'human' "
            "AND rewound_at IS NULL AND seq < $2::bigint",
            thread_id,
            max(int(call["call_seq"]) for call in calls),
        )
        state = await settle_parked_unit(
            conn, unit_id=thread_id, consumed_seq=interrupted_input_seq
        )
        if state is None:
            # A VM workspace recovery still owns the unit.
            raise UnknownToolResultRefused(409, "workspace_recovery_pending")
    return UnknownToolResultOutcome(
        state=state,
        call_ids=[str(call["call_id"]) for call in calls],
        needs_continue_input=decision == DECISION_CONTINUE and state == STATE_DONE,
    )


__all__ = [
    "CONTINUE_INPUT",
    "DECISION_CONTINUE",
    "DECISION_STOP",
    "UnknownToolResultOutcome",
    "UnknownToolResultRefused",
    "decide_unknown_tool_results",
    "result_text",
]
