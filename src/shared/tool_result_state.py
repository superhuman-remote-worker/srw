"""The marker on a tool result row that does not report what the call did.

A Session writes such a row when it cannot know the outcome: the workspace
was lost while the call ran (its result is unknown) or before a later call of
the batch started (it never ran), and the owner's Continue / Stop decision
answers a call that a lost process left without a result. The row's text is
written for the model; this marker, in ``thread_messages.metrics``, is what
the cockpit reads to label the call instead of "completed"
(knowledge-base/knowledge/issues/session_unknown_tool_result_needs_admin.md).

Contract: ``cockpit/src/app/core/models/tool-result-state.model.ts``.
"""

from __future__ import annotations

TOOL_RESULT_STATE_KEY = "tool_result_state"

RESULT_UNKNOWN = "unknown"
RESULT_NOT_RUN = "not_run"
TOOL_RESULT_STATES = frozenset({RESULT_UNKNOWN, RESULT_NOT_RUN})


def tool_result_state_metrics(state: str, tool_call_id: str) -> dict:
    """The ``metrics`` value of a result row that carries ``state``."""
    if state not in TOOL_RESULT_STATES:
        raise ValueError(f"unknown tool result state: {state!r}")
    return {TOOL_RESULT_STATE_KEY: {"state": state, "tool_call_id": tool_call_id}}


__all__ = [
    "RESULT_NOT_RUN",
    "RESULT_UNKNOWN",
    "TOOL_RESULT_STATES",
    "TOOL_RESULT_STATE_KEY",
    "tool_result_state_metrics",
]
