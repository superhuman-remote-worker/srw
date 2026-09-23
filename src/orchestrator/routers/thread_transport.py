"""Owner session transport: event stream, input, queue and interrupt (R1.B10).

Headless persistent sessions — SSE + REST transport. SSE replaces the WebSocket
as the primary server→client path; input and interrupt are plain POSTs. Neither
exposes the execution lane: a pinned session forwards to its exact bound
runtime, a stateless session admits durably into the run queue or the
exact-lease interrupt inbox and returns admission only.

Collaborators come from the requesting application's
``thread_transport_dependencies_factory``; the pinned turn-lock registry is one
application-owned :class:`ThreadTurnLocks`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from orchestrator.schemas.thread_transport import (
    ThreadInputRequest,
    ThreadInterruptRequest,
)
from orchestrator.security.access import log_security_event
from orchestrator.services import pinned_forwarding
from orchestrator.services.stateless_input_admission import (
    StatelessInputDependencies,
    admit_stateless_input,
)
from orchestrator.services.thread_event_stream import (
    ThreadEventStreamDependencies,
    open_thread_event_stream,
)
from orchestrator.services.thread_interrupt_inbox import (
    InterruptAdmissionError,
    admit_thread_interrupt,
    find_existing_thread_interrupt,
)
from orchestrator.services.thread_turn_locks import ThreadTurnLocks

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True, slots=True)
class ThreadTransportDependencies:
    """Application-owned collaborators for one transport request."""

    store: Any
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]
    require_approved_user: Callable[[Request, Any], Awaitable[dict[str, Any]]]
    forwarding: pinned_forwarding.PinnedForwardingDependencies
    stateless_input: StatelessInputDependencies
    turn_locks: ThreadTurnLocks


def get_thread_transport_dependencies(request: Request) -> ThreadTransportDependencies:
    return request.app.state.thread_transport_dependencies_factory()


@router.get("/api/persistent/threads/{thread_id}/stream")
async def thread_event_stream(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadTransportDependencies = Depends(
        get_thread_transport_dependencies
    ),
) -> StreamingResponse:
    """SSE: stream this thread's event log with replay-from-cursor.

    The client sends `Last-Event-ID: <epoch>:<seq>` to resume from a known
    point. If the cursor's epoch doesn't match the server, or its seq is
    older than retention, the server emits a single `gone_beyond_horizon`
    event and closes — the client must drop its cursor and re-sync.

    Otherwise: replay everything since the cursor, then switch to live
    mode (200ms poll, adaptive backoff to 1s after 5 empty polls).
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await open_thread_event_stream(
        thread_id,
        request,
        user=user,
        thread=thread,
        dependencies=ThreadEventStreamDependencies(
            store=dependencies.store,
            require_thread_owner=dependencies.require_thread_owner,
        ),
    )


@router.post("/api/persistent/threads/{thread_id}/input")
async def thread_input(
    thread_id: str,
    body: ThreadInputRequest,
    request: Request,
    *,
    dependencies: ThreadTransportDependencies = Depends(
        get_thread_transport_dependencies
    ),
) -> dict[str, Any]:
    """Submit user input to a thread. Per-turn lock returns 409 on dupes."""
    from shared.run_queue import LANE_STATELESS

    store = dependencies.store
    user = await dependencies.require_approved_user(request, store)

    # Stateless-lane admission (stateless_agents.md §5.3.1) resolves BEFORE
    # agent forwarding — queue-lane threads have no bound agent, so
    # resolve_thread_for_forwarding would 503 on them. Owner gate identical
    # to the resolver's; the pinned path below is untouched (its resolver
    # re-loads the thread and re-applies the same checks).
    lane_thread = await pinned_forwarding.load_thread_for_owner(
        thread_id, user, store=store
    )
    if lane_thread.get("execution_lane") == LANE_STATELESS:
        if not body.content or not isinstance(body.content, str):
            raise HTTPException(
                status_code=400, detail="content must be a non-empty string"
            )
        # The per-turn in-process lock below is deliberately SKIPPED on this
        # lane: the run_queue itself serializes turns (input during a leased
        # turn only advances the watermark; one row per unit dedups the
        # queue), and the lock dict is per-process state — replica-unsafe
        # under the 2-replica topology anyway. body.turn_id is ignored: the
        # queue lane derives the turn number from DB truth (total_turns + 1).
        return await admit_stateless_input(
            lane_thread,
            body.content,
            body.expected_conversation_revision,
            dependencies=dependencies.stateless_input,
        )

    thread, binding = await pinned_forwarding.resolve_thread_for_forwarding(
        thread_id, user, dependencies=dependencies.forwarding
    )

    if not body.content or not isinstance(body.content, str):
        raise HTTPException(
            status_code=400, detail="content must be a non-empty string"
        )

    # Turn id defaults to the thread's current total_turns + 1. Reject
    # arbitrarily-large values to bound the lock dict.
    total_turns = int(thread.get("total_turns") or 0)
    if body.turn_id is None:
        turn_id = total_turns + 1
    else:
        turn_id = body.turn_id
        if turn_id < 0 or turn_id > total_turns + 5:
            raise HTTPException(
                status_code=400,
                detail=f"turn_id out of range "
                f"(thread at turn {total_turns}, max accepted "
                f"{total_turns + 5})",
            )

    turn_locks = dependencies.turn_locks
    lock = turn_locks.ensure(thread_id, turn_id)
    if lock.locked():
        in_flight = turn_locks.inflight.get(thread_id, turn_id)
        return JSONResponse(
            status_code=409,
            content={
                "error": "turn_in_flight",
                "turn_id": in_flight,
                "thread_id": thread_id,
            },
        )
    async with lock:
        turn_locks.inflight[thread_id] = turn_id
        try:
            # Waiting for another tab's turn lock is an authority boundary.
            # Refuse a same-G Pod/attach/endpoint rotation before constructing
            # the HTTP client; forward_to_agent performs the final reread
            # after client entry as well.
            await pinned_forwarding.revalidate_pinned_forwarding_binding(
                binding, store=store
            )
            result = await pinned_forwarding.forward_to_agent(
                binding,
                "/api/input",
                {"content": body.content, "turn_id": turn_id},
                store=store,
            )
        finally:
            turn_locks.schedule_cleanup(thread_id, turn_id)
    return {
        "accepted": True,
        "turn_id": turn_id,
        "agent": result,
    }


@router.get("/api/persistent/threads/{thread_id}/queue")
async def thread_queue_state(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadTransportDependencies = Depends(
        get_thread_transport_dependencies
    ),
) -> dict[str, Any]:
    """Owner read of the unit's queue lifecycle
    (stateless_turn_resilience.md step 2) — the same ``queue`` block that
    ``/input`` and ``/connection`` carry, for polling while a turn is awaited.
    A thread that never enqueued (pinned lane, or no turn yet) reports
    ``state='none'``.
    """
    from orchestrator.services.stateless_queue_state import queue_block_for_thread

    try:
        UUID(str(thread_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Thread not found") from None
    store = dependencies.store
    _user, thread = await dependencies.require_thread_owner(request, store, thread_id)
    async with store.acquire() as conn:
        block = await queue_block_for_thread(conn, thread)
    return {"thread_id": thread_id, "queue": block}


@router.post("/api/persistent/threads/{thread_id}/queue/retry")
async def thread_queue_retry(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadTransportDependencies = Depends(
        get_thread_transport_dependencies
    ),
) -> dict[str, Any]:
    """Owner verb: revive a parked, retryable unit
    (stateless_turn_resilience.md step 2). ``parked`` + retryable →
    ``unpark_unit`` (attempts reset, park_reason cleared) → 200
    ``{state:'queued'}``; 409 ``{code}`` under stop markers / a claim-loss
    hold / a non-retryable reason; 404 when not parked. Audited. The admin
    verb ``POST /api/admin/run-queue/{unit_id}/unpark`` remains the
    operator path for the non-retryable reasons.
    """
    from orchestrator.services.stateless_queue_state import park_retry_refusal
    from shared.run_queue import STATE_PARKED, queue_state_for, unpark_unit

    try:
        UUID(str(thread_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Thread not found") from None
    store = dependencies.store
    user, _thread = await dependencies.require_thread_owner(request, store, thread_id)
    async with store.acquire() as conn:
        async with conn.transaction():
            authority = await conn.fetchrow(
                "SELECT execution_lane, metadata FROM threads "
                "WHERE id = $1::uuid FOR UPDATE",
                thread_id,
            )
            if authority is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            queue_state = await queue_state_for(conn, unit_id=thread_id)
            if queue_state is None or queue_state.get("state") != STATE_PARKED:
                raise HTTPException(status_code=404, detail="Unit is not parked")
            park_reason = queue_state.get("park_reason")
            refusal = park_retry_refusal(park_reason, authority["metadata"])
            if refusal is not None:
                raise HTTPException(
                    status_code=409,
                    detail={"code": refusal, "park_reason": park_reason},
                )
            ok = await unpark_unit(conn, unit_id=thread_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Unit is not parked")
    attempts = int(queue_state.get("attempts") or 0)
    logger.info(
        "run_queue retry (owner): unit=%s park_reason=%s attempts=%d",
        thread_id,
        park_reason,
        attempts,
    )
    await log_security_event(
        store,
        resource_type="thread",
        event_type="queue_retry",
        user=user,
        resource_id=thread_id,
        detail=f"owner unpark park_reason={park_reason} attempts={attempts}",
        request=request,
    )
    return {
        "thread_id": thread_id,
        "unit_id": thread_id,
        "state": "queued",
        "park_reason": park_reason,
    }


@router.post(
    "/api/persistent/threads/{thread_id}/interrupt",
    responses={202: {"description": "Stateless interrupt admitted"}},
)
async def thread_interrupt(
    thread_id: str,
    request: Request,
    body: ThreadInterruptRequest | None = None,
    *,
    dependencies: ThreadTransportDependencies = Depends(
        get_thread_transport_dependencies
    ),
) -> Any:
    """Interrupt one exact in-flight turn without exposing its execution lane.

    Pinned sessions retain their direct agent forward. Every forwarded body is
    bound to the exact runtime fingerprint; an otherwise-empty legacy command
    still targets the active turn observed by that runtime. A correlated
    client is forwarded intact so the agent can reject a retry aimed at an
    older turn. Stateless sessions commit an exact-lease request for the
    serving executor and return admission only — that owner applies the verb
    and journals the authoritative ack.
    """
    from shared.run_queue import LANE_STATELESS

    store = dependencies.store
    user, lane_thread = await dependencies.require_thread_owner(
        request, store, thread_id
    )
    correlated = body is not None and body.client_request_id is not None
    if correlated and body is not None and body.target_turn_id is not None:
        try:
            existing = await find_existing_thread_interrupt(
                store,
                thread_id=thread_id,
                owner_user_id=lane_thread.get("user_id"),
                client_request_id=body.client_request_id,
                target_turn_id=body.target_turn_id,
            )
        except InterruptAdmissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if existing is not None:
            return JSONResponse(
                status_code=202,
                content={
                    "accepted": True,
                    "request_id": str(existing.id),
                    "client_request_id": str(existing.client_request_id),
                    "target_turn_id": existing.target_turn_id,
                    "state": existing.state,
                    "duplicate": True,
                },
            )
    if lane_thread.get("execution_lane") == LANE_STATELESS:
        if not correlated or body is None or body.target_turn_id is None:
            # Stateless interrupt did not exist for legacy clients. Refuse an
            # uncorrelated command rather than letting it strike whichever
            # lease/turn happens to be current.
            raise HTTPException(
                status_code=422,
                detail=(
                    "client_request_id and target_turn_id are required for "
                    "stateless interrupt"
                ),
            )
        try:
            admitted = await admit_thread_interrupt(
                store,
                thread_id=thread_id,
                owner_user_id=lane_thread.get("user_id"),
                client_request_id=body.client_request_id,
                target_turn_id=body.target_turn_id,
                requested_by=str(user.get("id") or user.get("sub") or "rest_client"),
            )
        except InterruptAdmissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        logger.info(
            "session-interrupt admission: thread=%s turn=%d token=%d duplicate=%s",
            thread_id,
            admitted.target_turn_id,
            admitted.accepted_lease_token,
            admitted.duplicate,
        )
        return JSONResponse(
            status_code=202,
            content={
                "accepted": True,
                "request_id": str(admitted.id),
                "client_request_id": str(admitted.client_request_id),
                "target_turn_id": admitted.target_turn_id,
                "state": admitted.state,
                "duplicate": admitted.duplicate,
            },
        )

    _, binding = await pinned_forwarding.resolve_thread_for_forwarding(
        thread_id, user, dependencies=dependencies.forwarding
    )
    payload: dict[str, Any] = {}
    if correlated and body is not None and body.target_turn_id is not None:
        payload = {
            "client_request_id": str(body.client_request_id),
            "target_turn_id": body.target_turn_id,
        }
    result = await pinned_forwarding.forward_to_agent(
        binding, "/api/interrupt", payload, store=store
    )
    return {"accepted": True, "agent": result}


__all__ = [
    "ThreadTransportDependencies",
    "get_thread_transport_dependencies",
    "router",
]
