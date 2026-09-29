"""The contract of a session delegation turn settled as one batch.

When the executor running a session dies during a turn that delegated work,
its successor settles that turn once: one tool result for every
``delegate_agent`` call that has none yet, in provider order, plus one
``role=event`` continuation that supersedes the abandoned input. The
orchestrator writes all of it in one transaction; this module holds what both
sides of that call must agree on and what the cockpit reads back.

* The identities: the continuation is keyed on the parent input, so a turn
  has at most one; each written result row is keyed on its call.
* The classes of a call and of a written result.
* The texts the server renders itself. Each is a pure function of durable
  facts, so a retried settle would produce the same bytes (the first write
  wins anyway: the turn is the idempotency key).
* The request bounds and the capability the orchestrator advertises.

Design: knowledge-base/knowledge/features/parallel_subagents.md §5 and §6.1.
The single-child recovery path keeps its own per-child identity and text
(``session_subagent_authority.session_subagent_delivery_id``).
"""

from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

# Advertised in the session attach payload (pinned attach and the stateless
# claim bundle) as ``{KEY: 1}``. An agent may create a delegation batch wider
# than one child only against an orchestrator that advertises it (§12).
SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY = "session_subagent_batch_settle_contract"
SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT = 1

# The class of one delegate_agent call of the turn, decided from durable facts.
CALL_DELIVERED = "delivered"  # the parent transcript already has its result
CALL_ENDED = "ended"  # the child finished; its result never reached the parent
CALL_LIVE = "live"  # the child was still queued or running
CALL_NOT_STARTED = "not_started"  # no child was ever recorded for the call
CALL_DECLINED = "declined"  # no child; the user declined the permission request
CALL_RETIRED = "retired"  # the child was cancelled when the session was stopped
CALL_CLASSES = frozenset(
    {
        CALL_DELIVERED,
        CALL_ENDED,
        CALL_LIVE,
        CALL_NOT_STARTED,
        CALL_DECLINED,
        CALL_RETIRED,
    }
)

# The class a written result row carries (``metrics.subagent_recovery.class``).
RESULT_COMPLETED = "completed"
RESULT_INTERRUPTED = "interrupted"
RESULT_NOT_STARTED = "not_started"
RESULT_DECLINED = "declined"
RESULT_RETIRED = "retired"
RESULT_CLASS_BY_CALL_CLASS = {
    CALL_ENDED: RESULT_COMPLETED,
    CALL_LIVE: RESULT_INTERRUPTED,
    CALL_NOT_STARTED: RESULT_NOT_STARTED,
    CALL_DECLINED: RESULT_DECLINED,
    CALL_RETIRED: RESULT_RETIRED,
}

# The key under ``thread_messages.metrics`` of every row the settle writes. The
# cockpit history projection returns ``metrics``; the agent's restore query
# does not select it, so it never reaches a provider.
RECOVERY_METRICS_KEY = "subagent_recovery"
RECOVERY_METRICS_VERSION = 1

# Request bounds. One member's text matches the single-child terminal request;
# the total bounds one transaction and one HTTP body.
MEMBER_MESSAGE_MAX_CHARS = 200_000
BATCH_MESSAGE_MAX_CHARS = 1_000_000
BATCH_MAX_MEMBERS = 64

# The text the live loop writes for a call the user declined, byte for byte
# (``agent.persistent_graph``), so a recovered decline reads like a live one.
DECLINED_RESULT_TEXT = "User declined this tool call."


def session_subagent_batch_delivery_id(
    parent_thread_id: UUID | str, parent_input_message_id: UUID | str
) -> UUID:
    """The one continuation a settled delegation turn may have.

    Keyed on the superseded input, not on the member set: a member set that
    changed between two attempts must not produce a second continuation.
    """

    parent = str(UUID(str(parent_thread_id)))
    source = str(UUID(str(parent_input_message_id)))
    return uuid5(NAMESPACE_URL, f"srw:subagent-batch-delivery:v1:{parent}:{source}")


def session_subagent_batch_result_id(
    parent_thread_id: UUID | str,
    parent_input_message_id: UUID | str,
    tool_call_id: str,
) -> UUID:
    """Row id of the tool result a settle writes for one call."""

    parent = str(UUID(str(parent_thread_id)))
    source = str(UUID(str(parent_input_message_id)))
    call = str(tool_call_id)
    return uuid5(
        NAMESPACE_URL, f"srw:subagent-batch-result:v1:{parent}:{source}:{call}"
    )


def not_started_result_text() -> str:
    """Result of a call for which no child was ever recorded."""

    return (
        "[delegate_agent: NOT STARTED]\n"
        "This subagent never started: the process running this conversation "
        "was replaced before it could begin. It did no work. This was an "
        "infrastructure interruption, not a failure of the task.\n"
        "If the task is still needed, call delegate_agent again with the same "
        "task."
    )


def retired_result_text(
    *, handle: str, subagent_type: str, turns: int, tokens: int
) -> str:
    """Result of a child cancelled when its parent session was stopped."""

    return (
        "[delegate_agent: CANCELLED - session stopped]\n"
        f"handle: {handle}   type: {subagent_type}\n"
        "This subagent was cancelled when this session was stopped, before it "
        "produced a final report.\n"
        f"Progress before it was cancelled: {int(turns)} turns, "
        f"{int(tokens)} tokens.\n"
        "Anything it changed in the workspace is still there and may be "
        "incomplete.\n"
        "If this work is still needed, first check the workspace for what it "
        "already produced. Then either finish the remainder yourself or call "
        "delegate_agent again with a task limited to what is still missing."
    )


def _were(count: int) -> str:
    return "was" if count == 1 else "were"


def _join(clauses: list[str]) -> str:
    if len(clauses) == 1:
        return clauses[0]
    return ", ".join(clauses[:-1]) + " and " + clauses[-1]


def batch_continuation_text(
    *,
    calls: int,
    interrupted: int,
    not_started: int,
    declined: int,
    retired: int,
) -> str:
    """The continuation that resumes a settled turn, from its call counts.

    ``calls`` counts every delegate_agent call of the turn, including the ones
    whose result the parent already had; every call not counted by another
    argument finished and has its result above.
    """

    unfinished = interrupted + not_started + declined + retired
    finished = int(calls) - unfinished
    if finished < 0:
        raise ValueError("a settled turn cannot have fewer calls than outcomes")
    parts = [
        "[subagent recovery] This turn was resumed after the process running "
        "it was replaced."
    ]
    if finished == 0:
        parts.append("No delegated task of this turn finished.")
    elif finished == calls == 1:
        parts.append("The delegated task finished and its result is above.")
    elif finished == calls:
        parts.append(
            f"All {calls} delegated tasks finished and their results are above."
        )
    elif finished == 1:
        parts.append(f"1 of {calls} delegated tasks finished and its result is above.")
    else:
        parts.append(
            f"{finished} of {calls} delegated tasks finished and their results "
            "are above."
        )
    clauses = []
    if interrupted:
        clauses.append(f"{interrupted} {_were(interrupted)} interrupted")
    if not_started:
        clauses.append(f"{not_started} never started")
    if declined:
        clauses.append(f"{declined} {_were(declined)} declined by the user")
    if retired:
        clauses.append(
            f"{retired} {_were(retired)} cancelled when the session was stopped"
        )
    if clauses:
        parts.append(_join(clauses) + "; each is marked.")
    if finished == 1:
        parts.append(
            "The finished result is complete and does not need to be repeated."
        )
    elif finished:
        parts.append(
            "The finished results are complete and do not need to be repeated."
        )
    parts.append("Continue with the original request.")
    return " ".join(parts)


def result_metrics(
    *,
    result_class: str,
    tool_call_id: str,
    delivery_id: UUID | str,
    thread_id: UUID | str | None = None,
    handle: str | None = None,
    subagent_type: str | None = None,
    subagent_status: str | None = None,
    report_path: str | None = None,
) -> dict[str, Any]:
    """``thread_messages.metrics`` of one tool result a settle writes."""

    if result_class not in RESULT_CLASS_BY_CALL_CLASS.values():
        raise ValueError(f"unknown recovered result class {result_class!r}")
    return {
        RECOVERY_METRICS_KEY: {
            "version": RECOVERY_METRICS_VERSION,
            "kind": "result",
            "class": result_class,
            "tool_call_id": str(tool_call_id),
            "thread_id": str(thread_id) if thread_id is not None else None,
            "handle": handle,
            "subagent_type": subagent_type,
            "subagent_status": subagent_status,
            "report_path": report_path,
            "delivery_id": str(delivery_id),
        }
    }


def continuation_metrics(
    *,
    supersedes_input_seq: int,
    calls: int,
    interrupted: int,
    not_started: int,
    declined: int,
    retired: int,
) -> dict[str, Any]:
    """``thread_messages.metrics`` of the continuation a settle writes."""

    return {
        RECOVERY_METRICS_KEY: {
            "version": RECOVERY_METRICS_VERSION,
            "kind": "continuation",
            "supersedes_input_seq": int(supersedes_input_seq),
            "calls": int(calls),
            "finished": int(calls) - (interrupted + not_started + declined + retired),
            "interrupted": int(interrupted),
            "not_started": int(not_started),
            "declined": int(declined),
            "retired": int(retired),
        }
    }


__all__ = [
    "BATCH_MAX_MEMBERS",
    "BATCH_MESSAGE_MAX_CHARS",
    "CALL_CLASSES",
    "CALL_DECLINED",
    "CALL_DELIVERED",
    "CALL_ENDED",
    "CALL_LIVE",
    "CALL_NOT_STARTED",
    "CALL_RETIRED",
    "DECLINED_RESULT_TEXT",
    "MEMBER_MESSAGE_MAX_CHARS",
    "RECOVERY_METRICS_KEY",
    "RECOVERY_METRICS_VERSION",
    "RESULT_CLASS_BY_CALL_CLASS",
    "RESULT_COMPLETED",
    "RESULT_DECLINED",
    "RESULT_INTERRUPTED",
    "RESULT_NOT_STARTED",
    "RESULT_RETIRED",
    "SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT",
    "SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY",
    "batch_continuation_text",
    "continuation_metrics",
    "not_started_result_text",
    "result_metrics",
    "retired_result_text",
    "session_subagent_batch_delivery_id",
    "session_subagent_batch_result_id",
]
