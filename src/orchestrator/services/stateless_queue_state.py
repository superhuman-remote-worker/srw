"""Owner-facing view of a stateless unit's queue lifecycle.

Design: knowledge-base/knowledge/features/stateless_turn_resilience.md step 2.
One helper builds the ``queue`` block that ``POST …/input``,
``GET …/connection`` and ``GET …/queue`` all return, so the cockpit sees the
same shape from every path; another decides whether a parked unit may be
revived by its owner (``POST …/queue/retry``) or only by an operator.

``retryable`` is computed here, not in SQL, because it needs the thread's
stop markers (``shared.session_retirement``): a park under a claim-loss hold or
a pending retirement is a correctness hold that an owner must not lift.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from shared.run_queue import (
    PARK_REASON_CLAIM_LOSS_HOLD,
    PARK_REASON_MODEL_UNAVAILABLE,
    RETRYABLE_PARK_REASONS,
    STATE_PARKED,
    queue_state_for,
)
from shared.session_retirement import CLAIM_LOSS_HOLD_KEY, stateless_stop_markers

RETRY_REFUSAL_STOP_MARKERS = "stop_markers"
RETRY_REFUSAL_CLAIM_LOSS_HOLD = "claim_loss_hold"
RETRY_REFUSAL_NOT_RETRYABLE = "not_retryable"


def _metadata_object(metadata: Any) -> Any:
    """SQL NULL means "no markers"; unparseable text stays malformed.

    ``stateless_stop_markers`` refuses a malformed root (RuntimeError), which
    :func:`park_retry_refusal` maps to a fail-closed refusal — the right call
    for corrupt JSON, the wrong call for a thread that simply has no metadata.
    """
    if metadata is None:
        return {}
    if isinstance(metadata, str):
        try:
            return json.loads(metadata)
        except (TypeError, ValueError):
            return metadata  # left malformed on purpose: refused downstream
    return metadata


def park_retry_refusal(park_reason: str | None, metadata: Any) -> str | None:
    """Why an owner may NOT retry a parked unit; ``None`` when they may.

    Order matters: a claim-loss hold and any other stop marker win over the
    reason (they are correctness holds), then the reason must be in the
    retryable set. A malformed metadata root fails closed (refused).
    """
    try:
        markers = stateless_stop_markers(_metadata_object(metadata))
    except RuntimeError:
        return RETRY_REFUSAL_STOP_MARKERS
    if CLAIM_LOSS_HOLD_KEY in markers or park_reason == PARK_REASON_CLAIM_LOSS_HOLD:
        return RETRY_REFUSAL_CLAIM_LOSS_HOLD
    if markers:
        return RETRY_REFUSAL_STOP_MARKERS
    if park_reason not in RETRYABLE_PARK_REASONS:
        return RETRY_REFUSAL_NOT_RETRYABLE
    return None


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def queue_block(state: dict[str, Any] | None, metadata: Any) -> dict[str, Any]:
    """The API ``queue`` block for one unit (contract: step 2).

    ``state`` is :func:`shared.run_queue.queue_state_for`'s dict (or ``None``
    for a thread that never enqueued — reported as ``state='none'``).
    ``retryable`` is only meaningful while parked and is ``False`` otherwise.
    """
    if state is None:
        return {
            "state": "none",
            "park_reason": None,
            "park_message": None,
            "parked_at": None,
            "retryable": False,
            "attempts": 0,
            "pending_input": False,
        }
    parked = state.get("state") == STATE_PARKED
    refusal = park_retry_refusal(state.get("park_reason"), metadata) if parked else None
    # Only a reason whose ``last_error`` was written for the owner is shown:
    # other parks record internal error text.
    park_message = (
        state.get("last_error")
        if parked and state.get("park_reason") == PARK_REASON_MODEL_UNAVAILABLE
        else None
    )
    return {
        "state": state.get("state"),
        "park_reason": state.get("park_reason") if parked else None,
        "park_message": park_message,
        "parked_at": _iso(state.get("parked_at")) if parked else None,
        "retryable": bool(parked and refusal is None),
        "attempts": int(state.get("attempts") or 0),
        "pending_input": bool(state.get("pending_input")),
    }


async def cloud_push_block(conn: Any, thread_id: str) -> dict[str, Any] | None:
    """The ``cloud_push`` view of a thread's pending turn-end push.

    Commit-then-effects (stateless_turn_resilience.md step 4a): after the
    unit completes, the push continues off-slot and this is the only place a
    client can see it — no journal frames are written after release.
    ``None`` when nothing is pending or the read fails (diagnostics only).
    """
    from shared.cloud_sync_generations import pending_push_state

    try:
        state = await pending_push_state(conn, thread_id=thread_id)
    except Exception:
        return None
    if not state.get("pending"):
        return None
    return {
        "pending": int(state.get("pending") or 0),
        "uploaded": int(state.get("uploaded") or 0),
        "total": state.get("total"),
        "owner_alive": bool(state.get("owner_alive")),
        "failed": bool(state.get("failed")),
    }


async def queue_block_for_thread(conn: Any, thread: dict[str, Any]) -> dict[str, Any]:
    """Read + shape the ``queue`` block for a thread row on ``conn``.

    Carries ``cloud_push`` (or ``None``) so ``/input``, ``/connection`` and
    ``GET …/queue`` all expose the off-slot push the same way.
    """
    state = await queue_state_for(conn, unit_id=str(thread["id"]))
    block = queue_block(state, thread.get("metadata"))
    block["cloud_push"] = await cloud_push_block(conn, str(thread["id"]))
    return block


__all__ = [
    "cloud_push_block",
    "RETRY_REFUSAL_CLAIM_LOSS_HOLD",
    "RETRY_REFUSAL_NOT_RETRYABLE",
    "RETRY_REFUSAL_STOP_MARKERS",
    "park_retry_refusal",
    "queue_block",
    "queue_block_for_thread",
]
