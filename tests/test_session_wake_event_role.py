"""The agent half of the session wake: role='event' plumbing.

An injected job-completion notice has to be two things at once — a *durable*
message in the model's context, and *not a user bubble* in the transcript. The
split is: in memory it stays a ``HumanMessage``; on disk it persists as
``thread_messages.role = 'event'``, joining the shipped non-conversational roles
``summary`` and ``error``.

Every test here guards a way that split silently breaks. "Silently" is the
operative word — none of these failures raise, they just make the session answer
a question it can no longer see, or make the transcript claim the user said
something they did not.

Design: knowledge-base/knowledge/features/session_wake_on_job_completion.md.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import HumanMessage

from agent.api.persistent_app import (
    _db_rows_to_lc_messages,
    _serialize_message_row,
)
from agent.api.session_contract import ACCEPTED_INPUT_ROLES
from shared.runtime.core.loader import scheduled_work_system_floor
from agent.persistent_graph import PERSIST_ROLE_KEY

NOTICE = "[JOB_FINISHED] A worker job you created has reached a terminal state."


def _row(role: str, content: str = "x", **over) -> dict:
    row = {
        "role": role,
        "content": content,
        "tool_calls": None,
        "tool_call_id": None,
        "turn_number": 1,
    }
    row.update(over)
    return row


# --------------------------------------------------------------------------
# Restore (the silent one)
# --------------------------------------------------------------------------


def test_event_rows_survive_a_pod_recycle():
    """_db_rows_to_lc_messages is an if/elif chain with NO else, so an
    unhandled role is dropped without a trace: the notice would keep existing in
    the DB and in the UI while vanishing from the model's context on the next
    restore."""
    restored = _db_rows_to_lc_messages(
        [
            _row("human", "do the thing"),
            _row("event", NOTICE),
            _row("ai", "on it"),
        ]
    )

    assert len(restored) == 3, "the event row was silently dropped on restore"
    assert restored[1].content == NOTICE
    # Restored as a HumanMessage because that is what the model saw the first
    # time — 'event' is a transcript distinction, not a context one.
    assert isinstance(restored[1], HumanMessage)


def test_restored_event_messages_get_ids_so_compaction_can_remove_them():
    restored = _db_rows_to_lc_messages([_row("event", NOTICE)])
    assert restored[0].id, "RemoveMessage(id=...) is a no-op without an id"


def test_genuinely_unknown_roles_are_still_skipped():
    """The fix is a branch for 'event', not an else that resurrects system rows
    (the loop adds a fresh system prompt from current config)."""
    assert _db_rows_to_lc_messages([_row("system", "old prompt")]) == []


# --------------------------------------------------------------------------
# Persist
# --------------------------------------------------------------------------


def test_persist_role_override_wins_over_the_langchain_type():
    msg = HumanMessage(content=NOTICE)
    msg.additional_kwargs[PERSIST_ROLE_KEY] = "event"

    assert _serialize_message_row(msg, 1)["role"] == "event"


def test_a_normal_human_message_is_unaffected():
    assert _serialize_message_row(HumanMessage(content="hi"), 1)["role"] == "human"


def test_the_override_is_read_at_the_single_serialization_point():
    """Both writers — the accept-time persist and the loop's turn-start
    reconcile — go through _serialize_message_row and upsert the SAME row by id.
    If only the first honored the role, the second would flip the row back to
    'human' the instant the turn started."""
    msg = HumanMessage(content=NOTICE)
    msg.id = "msg_abc"
    msg.additional_kwargs[PERSIST_ROLE_KEY] = "event"

    first = _serialize_message_row(msg, 1)
    reconciled = _serialize_message_row(msg, 7, metrics={"tokens": 10})

    assert first["role"] == reconciled["role"] == "event"
    assert first["id"] == reconciled["id"]
    # metrics ride only on 'ai' rows, so the reconcile can't smuggle them in
    assert reconciled["metrics"] is None


# --------------------------------------------------------------------------
# Transport contract
# --------------------------------------------------------------------------


def test_only_human_and_event_are_accepted_over_api_input():
    """Ordinary input has no session token, so role remains allow-listed.

    The narrower retry-stable event identity is independently internal-key
    protected by the HTTP input transport; an arbitrary role must still never
    reach transcript persistence.
    """
    assert ACCEPTED_INPUT_ROLES == frozenset({"human", "event"})
    for forged in ("ai", "system", "tool", "summary", "error"):
        assert forged not in ACCEPTED_INPUT_ROLES


# --------------------------------------------------------------------------
# The operating rule
# --------------------------------------------------------------------------


def test_scheduled_work_rule_only_ships_to_sessions_that_can_create_jobs():
    assert scheduled_work_system_floor(["read_file", "run_command"]) == ""
    assert scheduled_work_system_floor([]) == ""
    assert scheduled_work_system_floor(None) == ""


def test_scheduled_work_rule_carries_the_cancel_lever():
    """The cancel clause is the design's own cost argument for per-job wake over
    a fan-in barrier, expressed as behavior: a woken agent holding
    cancel_job is the only thing that can stop a wrong-direction batch
    before the siblings spend their share of 10k+ requests."""
    rule = scheduled_work_system_floor(["create_job", "cancel_job"])

    assert rule.startswith("<scheduled_work>") and rule.endswith("</scheduled_work>")
    assert "cancel_job" in rule
    assert "do not narrate every completion" in rule


@pytest.mark.parametrize("names", [{"create_job"}, ("create_job",)])
def test_scheduled_work_rule_accepts_any_tool_name_collection(names):
    assert scheduled_work_system_floor(names) != ""
