"""Durable operator-pause boundary shared by the pinned and stateless lanes.

A public pause parks the job as ``paused``, unassigned and freeze-free. That
is also the runnable shape every system-initiated pause (dispatcher
preemption, agent release, orphan and lease recovery, backoff redispatch)
relies on to be picked up again, so without a marker the dispatcher resumed an
operator-paused job on its next pass (job 65e8729d, 2026-09-20).

The public pause therefore stamps ``context._operator_pause_hold`` in the same
jobs-row write that parks the job. Everything that re-admits a paused job
refuses a row carrying it: the pinned dispatcher's candidate query and claim
CAS, the stateless admission query and enqueue CAS, and the stateless worker
claim's jobs-row CAS. A pass that selected the row before the pause landed
therefore still cannot run it. Internal resume writes (queued feedback, urgent
replies, completion bounces) merge around the marker and leave the job held;
on the stateless lane they expose no runnable queue unit. Only an explicit
authorized resume (or an admin assignment) removes it, in the same write that
re-queues or claims the job, and only while the marker is still the one that
request observed: the caller passes :func:`operator_pause_lift_token` of the
row it authorized, and a newer pause makes the write lose its CAS instead of
being crossed.

Presence alone holds, whatever the value's shape (fail-closed). The marker is
jobs-row state, so it survives orchestrator and worker restarts. Stdlib-only:
both the orchestrator and the stateless worker (agent image) import it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

OPERATOR_PAUSE_HOLD_CONTEXT_KEY = "_operator_pause_hold"
OPERATOR_PAUSE_HOLD_VERSION = 1
LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY = "last_operator_pause_hold"


def operator_pause_hold_present_sql(context_expression: str = "context") -> str:
    """SQL predicate: the row carries an operator pause hold of any shape."""

    return (
        f"(COALESCE({context_expression}, '{{}}'::jsonb) "
        f"? '{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}')"
    )


def operator_pause_held_ancestor_sql(job_id_expression: str) -> str:
    """SQL predicate: some ancestor of the job is paused under an operator hold.

    The children a public pause cascades to (or that were waiting when it
    landed) carry no hold of their own; they wait behind the held parent. The
    dispatcher's ancestor guard covers admission; this covers every claim and
    wake that reaches a child directly.
    """

    return f"""EXISTS (
    WITH RECURSIVE held_lineage(ancestor_id) AS (
        SELECT parent_job_id FROM jobs
         WHERE id = {job_id_expression} AND parent_job_id IS NOT NULL
        UNION
        SELECT lineage_job.parent_job_id
          FROM jobs AS lineage_job
          JOIN held_lineage ON lineage_job.id = held_lineage.ancestor_id
         WHERE lineage_job.parent_job_id IS NOT NULL
    )
    SELECT 1 FROM held_lineage
      JOIN jobs AS held_ancestor ON held_ancestor.id = held_lineage.ancestor_id
     WHERE held_ancestor.status = 'paused'
       AND {operator_pause_hold_present_sql("held_ancestor.context")}
)"""


def operator_pause_hold_matches_sql(context_expression: str, parameter: str) -> str:
    """SQL predicate: the row's hold is exactly the one a lift token names.

    ``''`` names "no hold" (or one without a readable id), matching
    :func:`operator_pause_lift_token`.
    """

    hold_id = f"({context_expression})->'{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}'->'hold_id'"
    return (
        f"CASE WHEN jsonb_typeof({hold_id}) = 'string' "
        f"THEN {hold_id}#>>'{{}}' ELSE '' END = {parameter}::text"
    )


def operator_pause_hold_lift_sql(context_expression: str) -> str:
    """SQL context value with the hold removed and stashed for provenance."""

    return (
        f"((COALESCE({context_expression}, '{{}}'::jsonb) "
        f"- '{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}') || CASE "
        f"WHEN COALESCE({context_expression}, '{{}}'::jsonb) "
        f"? '{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}' "
        f"THEN jsonb_build_object('{LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY}', "
        f"({context_expression})->'{OPERATOR_PAUSE_HOLD_CONTEXT_KEY}' "
        "|| jsonb_build_object('lifted_at', to_jsonb(now()))) "
        "ELSE '{}'::jsonb END)"
    )


HELD_FEEDBACK_REASON = (
    "This job was held by an operator pause. Every message that arrived while "
    "it was held is included below, oldest first, followed by any feedback "
    "given with the resume."
)


def operator_pause_hold_merged_feedback_sql(
    context_expression: str, merge_expression: str, reason_parameter: str
) -> str:
    """SQL ``jsonb`` overlay that appends held feedback instead of replacing it.

    A resume write merges ``queued_feedback`` wholesale. While the row being
    updated (``context_expression`` is the pre-update value) carries a hold,
    each internal resume and the final explicit resume append to the feedback
    already queued, oldest first, so nothing queued behind the hold is lost.
    Returns ``'{}'`` (no overlay) otherwise.
    """

    held = operator_pause_hold_present_sql(context_expression)
    return (
        f"CASE WHEN {held} "
        f"AND jsonb_typeof(({context_expression})->'queued_feedback') = 'string' "
        f"AND jsonb_typeof(({merge_expression})->'queued_feedback') = 'string' "
        "THEN jsonb_build_object("
        "'queued_feedback', "
        f"(({context_expression})->>'queued_feedback') || E'\\n\\n---\\n\\n' "
        f"|| (({merge_expression})->>'queued_feedback'), "
        f"'queued_feedback_reason', {reason_parameter}::text) "
        "ELSE '{}'::jsonb END"
    )


def operator_pause_hold_jsonb_sql(
    *, hold_id_parameter: str, source_parameter: str, paused_by_parameter: str
) -> str:
    """SQL ``jsonb`` value for a new hold written by a public pause."""

    return (
        "jsonb_build_object("
        f"'version', {OPERATOR_PAUSE_HOLD_VERSION}, "
        f"'hold_id', {hold_id_parameter}::text, "
        f"'source', {source_parameter}::text, "
        f"'paused_by', {paused_by_parameter}::text, "
        "'paused_at', to_jsonb(now()))"
    )


def _context(value: Any) -> Mapping[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, Mapping) else {}


def operator_pause_hold_present(context: Any) -> bool:
    """Python mirror of :func:`operator_pause_hold_present_sql` for a context."""

    return OPERATOR_PAUSE_HOLD_CONTEXT_KEY in _context(context)


def operator_pause_lift_already_consumed(
    job: Mapping[str, Any] | None, token: str, *, feedback: str | None
) -> bool:
    """Whether a losing explicit resume duplicated the one that lifted ``token``.

    True only when that exact hold was lifted (and no newer one set) and this
    request adds nothing the winner did not queue: no feedback, or feedback
    still present in the queued text. Otherwise the loss is a real conflict.
    """

    if not token or job is None:
        return False
    context = _context(job.get("context"))
    if OPERATOR_PAUSE_HOLD_CONTEXT_KEY in context:
        return False
    lifted = context.get(LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY)
    if not isinstance(lifted, Mapping) or lifted.get("hold_id") != token:
        return False
    if not feedback:
        return True
    queued = context.get("queued_feedback")
    return isinstance(queued, str) and feedback in queued


def operator_pause_lift_token(job: Mapping[str, Any] | None) -> str:
    """Hold id an explicit resume observed on ``job``; ``''`` when unheld."""

    marker = _context(job.get("context") if job else None).get(
        OPERATOR_PAUSE_HOLD_CONTEXT_KEY
    )
    hold_id = marker.get("hold_id") if isinstance(marker, Mapping) else None
    return hold_id if isinstance(hold_id, str) else ""


__all__ = [
    "HELD_FEEDBACK_REASON",
    "LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY",
    "OPERATOR_PAUSE_HOLD_CONTEXT_KEY",
    "OPERATOR_PAUSE_HOLD_VERSION",
    "operator_pause_held_ancestor_sql",
    "operator_pause_hold_jsonb_sql",
    "operator_pause_hold_lift_sql",
    "operator_pause_hold_matches_sql",
    "operator_pause_hold_merged_feedback_sql",
    "operator_pause_hold_present",
    "operator_pause_hold_present_sql",
    "operator_pause_lift_already_consumed",
    "operator_pause_lift_token",
]
