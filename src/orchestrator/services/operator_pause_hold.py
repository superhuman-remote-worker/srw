"""Durable operator-pause boundary for pinned jobs.

A public pause parks the job as ``paused``, unassigned and freeze-free. That
is also the dispatchable shape every system-initiated pause (dispatcher
preemption, agent release, orphan and lease recovery, backoff redispatch)
relies on to be picked up again, so without a marker the dispatcher resumed an
operator-paused job on its next pass (job 65e8729d, 2026-09-20).

The public pause therefore stamps ``context._operator_pause_hold`` in the same
jobs-row write that parks the job. The dispatcher's candidate query and its
claim CAS refuse a row carrying it, so neither a later poll nor a dispatcher
pass that selected the row before the pause landed can run it. Internal resume
writes (queued feedback, urgent replies, completion bounces) merge around the
marker and leave the job held. Only an explicit authorized resume (or an admin
assignment) removes it, in the same write that re-queues or claims the job, and
only while the marker is still the one that request observed: the caller
passes :func:`operator_pause_lift_token` of the row it authorized, and a newer
pause makes the write lose its CAS instead of being crossed.

Presence alone holds, whatever the value's shape (fail-closed). The marker is
jobs-row state, so it survives orchestrator restarts.
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


def operator_pause_lift_token(job: Mapping[str, Any] | None) -> str:
    """Hold id an explicit resume observed on ``job``; ``''`` when unheld."""

    context: Any = job.get("context") if job else None
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return ""
    if not isinstance(context, Mapping):
        return ""
    marker = context.get(OPERATOR_PAUSE_HOLD_CONTEXT_KEY)
    hold_id = marker.get("hold_id") if isinstance(marker, Mapping) else None
    return hold_id if isinstance(hold_id, str) else ""


__all__ = [
    "LAST_OPERATOR_PAUSE_HOLD_CONTEXT_KEY",
    "OPERATOR_PAUSE_HOLD_CONTEXT_KEY",
    "OPERATOR_PAUSE_HOLD_VERSION",
    "operator_pause_hold_jsonb_sql",
    "operator_pause_hold_lift_sql",
    "operator_pause_hold_matches_sql",
    "operator_pause_hold_present_sql",
    "operator_pause_lift_token",
]
