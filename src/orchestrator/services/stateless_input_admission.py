"""Stateless-lane admission of one owner turn (R1.B10).

stateless_agents.md §5.3.1: persist the human message, advance the run-queue
input watermark and admit the unit in ONE transaction, so "message durable ⟺
watermark advanced" can never tear and a signal can never be lost.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from orchestrator.services.model_availability import (
    WHERE_SESSION,
    ModelUnavailable,
    unavailable_slots,
)
from orchestrator.services.session_class_policy import require_stateless_workspace

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StatelessInputDependencies:
    """Application-owned collaborators for stateless input admission.

    ``resolve_model`` is the registry resolver the model check asks
    (``shared.runtime.core.model_registry.resolve_model`` when unset).
    """

    store: Any
    schedule_stateless_workspace_ensure: Callable[[str], Any]
    resolve_model: Callable[..., Any] | None = None


async def refuse_unavailable_session_models(
    thread: dict, *, dependencies: StatelessInputDependencies
) -> None:
    """Refuse a turn whose session generation names a model that cannot run,
    before the message is stored (unavailable_model_handling.md §5).

    Reads the models of the thread's current execution generation and asks the
    registry about each slot that does not inherit its parent's model — no
    credential resolution. Raises 409 ``{code: "model.unavailable", message,
    entries}``; the chat banner shows ``message`` and the message stays in the
    outbox for a retry after a model switch. A failure of the check itself lets
    the turn through: the claim-time refusal is the backstop.
    """
    from orchestrator.services.manifest_execution_snapshot import (
        read_execution,
        srw_snapshot_config,
    )

    user_id = str(thread["user_id"]) if thread.get("user_id") else None
    try:
        execution = await read_execution(
            dependencies.store, "Session", str(thread["id"])
        )
        if execution is None:
            return
        resolved, _policy = srw_snapshot_config(execution)
        resolve_model = dependencies.resolve_model
        if resolve_model is None:
            from shared.runtime.core import model_registry

            resolve_model = model_registry.resolve_model
        found = await unavailable_slots(
            resolved.get("agent") if isinstance(resolved, dict) else None,
            user_id=user_id,
            store=dependencies.store,
            resolve_model=resolve_model,
        )
    except Exception:  # noqa: BLE001 — the claim-time refusal is the backstop
        logger.warning(
            "Stateless admission: model check failed for thread %s; admitting",
            thread.get("id"),
            exc_info=True,
        )
        return
    if not found:
        return
    is_admin = False
    if user_id:
        try:
            user = await dependencies.store.get_user(user_id)
            is_admin = bool((user or {}).get("is_admin"))
        except Exception:  # noqa: BLE001 — only selects the admin hint
            is_admin = False
    refusal = ModelUnavailable(found)
    logger.info(
        "Stateless admission refused for thread %s: %s",
        thread.get("id"),
        [entry.as_dict() for entry in found],
    )
    raise HTTPException(
        status_code=409,
        detail=refusal.detail(where=WHERE_SESSION, is_admin=is_admin),
    )


async def admit_stateless_input(
    thread: dict,
    content: str,
    expected_conversation_revision: int | None = None,
    *,
    dependencies: StatelessInputDependencies,
) -> dict[str, Any]:
    """Admit one user turn for a stateless-lane thread (stateless_agents.md
    §5.3.1): persist the message, advance the input watermark, and queue the
    unit — all in ONE transaction, so "message durable ⟺ watermark advanced"
    can never tear and a signal can never be lost.

    The message row is indistinguishable from the agent's accept-time persist
    of a plain-text human message (``agent.api.session_input.SessionInputRuntime.accept``
    → ``src/database/postgres_db.save_thread_message``): same ``msg_`` id mint
    with the agent's own uuid5 row-id coercion, ``role='human'``,
    ``turn_number = total_turns + 1``, all other columns at their NULL
    defaults, and the same ``threads`` last_activity/total_turns bump.

    Admission is ``record_input_seq`` — the input-during-anything path: it
    creates a fresh ``'queued'`` row, revives ``'done'``, merges the watermark
    into ``'queued'``, bumps ONLY the watermark on ``'leased'`` (the running
    turn's completion re-queues via ``input_seq > consumed_seq``), and records
    input on ``'parked'`` without reviving it (explicit unpark only). No
    separate ``enqueue_unit`` call is needed: every branch leaves the unit
    queued, leased-with-watermark, or deliberately parked.
    """
    from shared.row_identity import _coerce_row_id
    from orchestrator.services.stateless_queue_state import queue_block
    from shared.run_queue import (
        LANE_STATELESS,
        UNIT_KIND_SESSION_TURN,
        queue_depth_for,
        queue_state_for,
        record_input_seq,
    )

    # The unlocked preflight provides a fast refusal. The locked copy below is
    # authoritative against lane/tier/lifecycle changes before message commit.
    require_stateless_workspace(thread)
    await refuse_unavailable_session_models(thread, dependencies=dependencies)

    thread_id = str(thread["id"])
    # Mirror the agent's accept-time mint exactly; the row id is the same
    # deterministic uuid5 the agent-side coercion would derive from this raw
    # id, so a later executor re-persist upserts onto this row (ON CONFLICT
    # (id)) instead of duplicating the user bubble.
    raw_msg_id = f"msg_{uuid4().hex[:24]}"
    row_id = _coerce_row_id(raw_msg_id)

    async with dependencies.store.acquire() as conn:
        async with conn.transaction():
            locked_thread = await conn.fetchrow(
                "SELECT id, user_id, execution_lane, agent_id, status, "
                "       total_turns, metadata, conversation_revision "
                "FROM threads WHERE id = $1 FOR UPDATE",
                thread_id,
            )
            if (
                locked_thread is None
                or str(locked_thread["execution_lane"] or "") != LANE_STATELESS
                or locked_thread["agent_id"] is not None
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Thread is no longer eligible for stateless admission",
                )
            locked_thread_dict = dict(locked_thread)
            current_revision = int(locked_thread_dict.get("conversation_revision") or 0)
            if expected_conversation_revision is None:
                revision_matches = current_revision == 0
            else:
                revision_matches = (
                    int(expected_conversation_revision) == current_revision
                )
            if not revision_matches:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "session_view_stale",
                        "reason": "conversation_revision_changed",
                        "conversation_revision": current_revision,
                    },
                )
            locked_backend = require_stateless_workspace(locked_thread_dict)
            locked_status = str(locked_thread["status"] or "")
            if locked_status not in {
                "created",
                "active",
                "awaiting_user",
                "suspended",
            }:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Thread is not currently accepting stateless input "
                        f"(status={locked_status or 'unknown'})"
                    ),
                )
            needs_workspace_ensure = locked_backend == "sandbox"
            if locked_status == "suspended":
                # Wake and enqueue are one lifecycle transaction. Workspace
                # restore remains a post-commit side effect, but no claimant
                # can observe a runnable queue paired with a still-suspended
                # thread (the claim/credential boundary correctly refuses
                # suspended rows).
                woke = await conn.fetchval(
                    "UPDATE threads SET status = 'created', "
                    "agent_id = NULL, control_admission_agent_id = NULL, "
                    "awaiting_user_since = NULL, extend_count = 0 "
                    "WHERE id = $1::uuid AND execution_lane = 'stateless' "
                    "AND status = 'suspended' RETURNING id",
                    thread_id,
                )
                if woke is None:
                    raise RuntimeError(
                        "stateless suspended-input wake lost thread authority"
                    )
            turn_number = int(locked_thread["total_turns"] or 0) + 1
            fair_key = (
                str(locked_thread["user_id"])
                if locked_thread["user_id"] is not None
                else None
            )
            seq = await conn.fetchval(
                """
                INSERT INTO thread_messages (id, thread_id, role, content, turn_number)
                VALUES ($1, $2, 'human', $3, $4)
                RETURNING seq
                """,
                row_id,
                thread_id,
                content,
                turn_number,
            )
            # Same activity bump the agent's save_thread_message performs.
            await conn.execute(
                """
                UPDATE threads
                SET last_activity = CURRENT_TIMESTAMP,
                    total_turns   = GREATEST(total_turns, COALESCE($2, 0))
                WHERE id = $1
                """,
                thread_id,
                turn_number,
            )
            state = await record_input_seq(
                conn,
                unit_id=thread_id,
                unit_kind=UNIT_KIND_SESSION_TURN,
                input_seq=int(seq),
                fair_key=fair_key,
            )

        if needs_workspace_ensure:
            # Queue admission commits before this side effect. The claimant may
            # arrive first, but its internal workspace poll independently
            # suppresses cached Ready credentials until the exact Pod UID is
            # live. Always schedule sandbox reconciliation: a DB-Ready row can
            # be stale even though its lifecycle string looks terminally good.
            dependencies.schedule_stateless_workspace_ensure(thread_id)
        # Post-commit watermark read (same conn): §5.3.1 response parity —
        # queue_depth comes from unconsumed watermarks, not a process queue.
        wm = await queue_depth_for(conn, unit_id=thread_id)
        queue_state = await queue_state_for(conn, unit_id=thread_id)

    queue_depth = 1 if (wm is not None and wm.has_pending_input) else 0
    # Lifecycle block (stateless_turn_resilience.md step 2): the SAME shape
    # /connection and GET …/queue return, so a parked unit is never mistaken
    # for a busy pool by the client.
    lifecycle = queue_block(queue_state, thread.get("metadata"))
    logger.info(
        "run_queue enqueue: thread=%s turn=%d input_seq=%d state=%s",
        thread_id,
        turn_number,
        int(seq),
        state,
    )
    return {
        "accepted": True,
        "turn_id": turn_number,
        "conversation_revision": current_revision,
        "queue": {
            "state": state,
            "queue_depth": queue_depth,
            "message_id": raw_msg_id,
            "input_seq": int(seq),
            "park_reason": lifecycle["park_reason"],
            "parked_at": lifecycle["parked_at"],
            "retryable": lifecycle["retryable"],
            "attempts": lifecycle["attempts"],
            "pending_input": lifecycle["pending_input"],
        },
    }


__all__ = ["StatelessInputDependencies", "admit_stateless_input"]
