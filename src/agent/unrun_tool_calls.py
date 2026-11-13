"""The result a Session turn writes for each tool call it leaves un-run.

A call without a ``role='tool'`` row is tool debt: the next message's
admission check refuses the claim and stops the stateless executor. So a
Session turn that ends before a call runs (an unanswered approval, or a Stop
between tools) writes a ``not_run`` result for it instead. Each text says only
what happened. An unanswered approval is never a refusal, and only a person's
Stop is told as "the user stopped"
(knowledge-base/knowledge/issues/stateless_unanswered_approval_stops_chat_executors.md).
"""

from __future__ import annotations

NOT_RUN_STOPPED_AT_APPROVAL = (
    "Not run: the user stopped the turn before approving this call. No decision "
    "was made about it. Do not run it again unless the user asks."
)
NOT_RUN_APPROVAL_EXPIRED = (
    "Not run: the approval request expired without an answer. No decision was "
    "made. If it is still needed, ask the user again."
)
NOT_RUN_APPROVAL_UNAVAILABLE = (
    "Not run: approval could not be requested (approval service unavailable). "
    "No decision was made. If it is still needed, ask again."
)
#: Any other unanswered status, a platform abort among them.
NOT_RUN_APPROVAL_UNANSWERED = (
    "Not run: the approval was not answered. No decision was made. If it is "
    "still needed, ask the user again."
)
SKIPPED_TURN_ENDED = "Skipped: not run because the turn ended before this call."
NOT_RUN_STOPPED_BEFORE_START = (
    "Not run: the user stopped the turn before this call started. Do not run it "
    "again unless the user asks."
)


def gated_call_text(status: str | None, *, user_stop: bool) -> str:
    """The result of a call whose approval was not answered.

    ``status`` is the approval row's final status. ``user_stop`` is true only
    when a person's Stop ended the wait; a platform abort also reports
    ``interrupted`` and gets the neutral text.
    """
    if status == "interrupted" and user_stop:
        return NOT_RUN_STOPPED_AT_APPROVAL
    if status == "expired":
        return NOT_RUN_APPROVAL_EXPIRED
    if status == "unavailable":
        return NOT_RUN_APPROVAL_UNAVAILABLE
    return NOT_RUN_APPROVAL_UNANSWERED


def interrupted_call_text(*, user_stop: bool) -> str:
    """The result of a call an interrupt reached before it started."""
    return NOT_RUN_STOPPED_BEFORE_START if user_stop else SKIPPED_TURN_ENDED


__all__ = [
    "NOT_RUN_APPROVAL_EXPIRED",
    "NOT_RUN_APPROVAL_UNANSWERED",
    "NOT_RUN_APPROVAL_UNAVAILABLE",
    "NOT_RUN_STOPPED_AT_APPROVAL",
    "NOT_RUN_STOPPED_BEFORE_START",
    "SKIPPED_TURN_ENDED",
    "gated_call_text",
    "interrupted_call_text",
]
