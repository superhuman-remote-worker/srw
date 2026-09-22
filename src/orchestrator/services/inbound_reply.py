"""Inbound replies: the one funnel every human answer to a worker passes.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane M, census group
``J_messaging``). Three callers share :func:`route_inbound_reply` — the cockpit
reply route, the IMAP poller and the officer reply lane — which is why the
resolver identity (``resolver_kind`` / ``resolver_id``) is a parameter rather
than three copies of the decision.

Ordering and fail-closed properties preserved literally:

* **The completion-control guard runs before anything is read or written**, and
  again before a conflict is reported, so a reply can never race a finalizing
  completion into a context mutation.
* **"Delivered" is recorded only after the selected context/resume mutation won
  its command-aware CAS.** A losing control stays a clean 409.
* **A blocking resume CAS carries the route generation** (OC-04), so a delayed
  actor for an OLD route cannot resume a job that has since refrozen on a new
  one. ``None`` (an unrouted freeze) keeps the status-only CAS.
* **Urgent is not resume** (P1-A): a live run gets the message as next-turn
  guidance through :func:`queue_supervisor_guidance`; only a job with no live
  run is resumed to deliver it.
* Every answer settles the thread's feed rows first (D6), so a deferred "nobody
  answered" mail cannot go out after someone answered.

Collaborators arrive on :class:`InboundReplyDependencies` per invocation.
``guard_completion_control``, ``completion_dispatch_guard_kwargs`` (B08) and
``internal_resume_job`` (B09) are other batches' authorities, consumed and
never re-implemented here; ``store``, ``notifier`` and
``kick_officer_event_drain`` are names suites rebind on ``orchestrator.main``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import uuid4

from fastapi import HTTPException

from shared.operator_pause_hold import operator_pause_hold_present

logger = logging.getLogger(__name__)


@dataclass
class InboundReplyDependencies:
    """Collaborators for one inbound-reply delivery, resolved per invocation."""

    store: Any
    notifier: Any
    guard_completion_control: Callable[..., Awaitable[None]]
    completion_dispatch_guard_kwargs: Callable[[], dict[str, Any]]
    internal_resume_job: Callable[..., Awaitable[bool]]
    kick_officer_event_drain: Callable[[Any], None]


URGENT_RESUME_REASON = (
    "An urgent operator message arrived while this job was not running; "
    "the job was resumed to deliver it."
)


async def queue_supervisor_guidance(
    job: dict[str, Any],
    thread_id: str,
    message: str,
    *,
    dependencies: InboundReplyDependencies,
) -> str | None:
    """Non-destructive urgent steer (P1-A): append to ``context.pending_guidance``.

    The entry rides the agent-heartbeat response into the worker's next LLM
    turn as a transient [SUPERVISOR GUIDANCE] block — no pause, no pod
    replacement, no context compaction, no forced re-plan (the old urgent arm
    was a hidden resume-with-feedback that destroyed the worker's in-flight
    tactical context). Worst-case delivery: one heartbeat interval (currently
    60s) + the time to the worker's next LLM turn. The agent acks via
    ``POST /api/jobs/{id}/guidance/ack``, which moves the entry to
    ``context.consumed_replies`` — senders confirm delivery there.

    Returns:
        The delivery strategy, or None when there is no live run to deliver
        into (job not ``processing``, or the row vanished) — callers fall
        back to the resume path.
    """
    if job.get("status") != "processing":
        return None
    entry = {
        "id": str(uuid4()),
        "text": message,
        "source": thread_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if not await dependencies.store.append_pending_guidance(
        str(job["id"]), entry, **dependencies.completion_dispatch_guard_kwargs()
    ):
        return None
    logger.info(
        "Queued supervisor guidance %s for job %s (thread %s)",
        entry["id"][:8],
        str(job["id"])[:8],
        thread_id,
    )
    return "guidance_next_turn"


async def find_thread_route(
    job_id: str, thread_id: str, *, dependencies: InboundReplyDependencies
) -> dict[str, Any] | None:
    """Best-effort newest route row for a (job, thread). Never raises."""
    try:
        return await dependencies.store.find_message_route_for_thread(job_id, thread_id)
    except Exception:
        logger.debug(
            "route lookup failed for job %s thread %s",
            job_id[:8],
            thread_id,
            exc_info=True,
        )
        return None


async def record_route_reply_resolution(
    job_id: str,
    thread_id: str,
    *,
    actor_kind: str,
    actor_id: str | None = None,
    note: str | None = None,
    dependencies: InboundReplyDependencies,
) -> None:
    """Best-effort CAS of the thread's open route to its resolved state.

    The job-status CAS is what unblocks exactly once; this records WHO
    answered on the route ledger. A lost CAS (reconciler deadline or the
    other audience won) is a silent no-op by design.
    """
    try:
        from orchestrator.services import message_routing as routing_svc

        await routing_svc.record_reply_resolution(
            dependencies.store,
            job_id,
            thread_id,
            actor_kind=actor_kind,
            actor_id=actor_id,
            note=note,
        )
    except Exception:
        logger.debug(
            "route resolution recording failed for job %s thread %s",
            job_id[:8],
            thread_id,
            exc_info=True,
        )


async def route_inbound_reply(
    job_id: str,
    thread_id: str,
    message: str,
    sender_email: str | None = None,
    email_message_id: str | None = None,
    urgent: bool = False,
    resolver_kind: str = "user",
    resolver_id: str | None = None,
    *,
    dependencies: InboundReplyDependencies,
) -> tuple[str, int]:
    """Route an inbound reply to the correct job/thread.

    Shared by the cockpit reply endpoint, the IMAP poller, and the officer
    reply lane (M3 — which passes ``resolver_kind='officer'`` so the route
    ledger records the right actor).

    Args:
        job_id: Target job UUID
        thread_id: Target thread ID
        message: Reply body
        sender_email: Sender's email (for user resolution, IMAP only)
        email_message_id: RFC822 Message-ID for dedup (IMAP only)
        urgent: Deliver into the worker's next LLM turn via the guidance
            lane (non-destructive). Only when the job has no live run does
            urgent fall back to a resume-with-feedback.
        resolver_kind: 'user' (default) or 'officer' — recorded on a
            matching route's resolution transition.
        resolver_id: Actor id for the route audit (user id / officer thread).

    Returns:
        Tuple of (delivery_strategy, sequence_number).

    Raises:
        ValueError: If the job is not found.
    """
    job = await dependencies.store.get_job(job_id)
    if not job:
        raise ValueError(f"Job '{job_id}' not found")
    await dependencies.guard_completion_control(job_id, source="inbound_reply")
    # Any answer — cockpit, mail, officer — settles the thread's feed rows
    # (D6): the deferred "nobody answered" mail must never go out after this.
    await dependencies.notifier.resolve_source(
        "message_thread",
        thread_id,
        resolved_by=(
            f"{resolver_kind}:{resolver_id}"
            if resolver_id
            else f"{resolver_kind}:reply"
        ),
    )

    async def _resume_reply_or_conflict(
        *, reason: str, route_id: str | None = None
    ) -> None:
        resumed = await dependencies.internal_resume_job(
            job_id,
            feedback=message,
            reason=reason,
            expected_status=str(job.get("status") or ""),
            # OC-04: reply and timeout race for the same freeze. Both now CAS
            # on the route generation, so exactly one wins and a delayed actor
            # for an OLD route cannot resume a job that has since refrozen on
            # a new one. None (an unrouted freeze) keeps the status-only CAS.
            expected_route_id=route_id,
        )
        if resumed:
            return
        # Distinguish a command winner when possible, but never report an
        # immediate delivery that did not win its status/queue mutation.
        await dependencies.guard_completion_control(job_id, source="inbound_reply")
        refreshed = await dependencies.store.get_job(job_id)
        raise HTTPException(
            status_code=409,
            detail=(
                "Job changed while the inbound reply was being delivered"
                + (f" (status: {refreshed.get('status')})" if refreshed else "")
            ),
        )

    async def _resumed_strategy(strategy: str) -> str:
        # An operator pause hold survives the internal resume: the message is
        # queued behind it and reaches the worker only after an explicit
        # resume, so report that instead of an immediate delivery.
        resumed = await dependencies.store.get_job(job_id)
        if resumed and operator_pause_hold_present(resumed.get("context")):
            return "queued_until_resume"
        return strategy

    # Resolve user_id from sender email or job owner
    user_id = None
    if sender_email:
        async with dependencies.store.acquire() as conn:
            user_row = await conn.fetchrow(
                "SELECT id FROM users WHERE email = $1",
                sender_email,
            )
        if user_row:
            user_id = str(user_row["id"])
    if not user_id:
        user_id = str(job.get("user_id", "")) if job.get("user_id") else None

    # Get sequence number
    sequence = await dependencies.store.get_message_sequence(job_id, thread_id)

    async def _delivered(strategy: str) -> tuple[str, int]:
        # Record "delivered" only after the selected context/resume mutation
        # won its command-aware CAS. A losing control remains a clean 409.
        await dependencies.store.log_message(
            job_id=job_id,
            user_id=user_id,
            thread_id=thread_id,
            direction="inbound",
            subject="(reply)",
            message=message,
            status="delivered",
            email_message_id=email_message_id,
        )
        return strategy, sequence

    # Check if job is waiting for a reply on this thread
    job_status = job.get("status", "")
    freeze_data = job.get("freeze_data")
    if isinstance(freeze_data, str):
        try:
            freeze_data = json.loads(freeze_data)
        except json.JSONDecodeError:
            freeze_data = None

    is_blocking_reply = (
        job_status == "waiting_for_reply"
        and freeze_data
        and freeze_data.get("thread_id") == thread_id
    )

    if is_blocking_reply:
        await _resume_reply_or_conflict(
            reason=(
                "This job froze waiting for a reply to its outbound message; "
                "the reply below answers it."
            ),
            route_id=(freeze_data or {}).get("route_id"),
        )
        # The resume CAS won — record who answered on the route ledger
        # (officer_message_routing.md §3). Best-effort: the worker is
        # already unblocked either way.
        await record_route_reply_resolution(
            job_id,
            thread_id,
            actor_kind=resolver_kind,
            actor_id=resolver_id or user_id,
            dependencies=dependencies,
        )
        return await _delivered(await _resumed_strategy("immediate_resume"))

    # Officer-aware follow-ups (officer_message_routing.md §5.3): consult the
    # thread's route ONCE. Threads without a route (all pre-officer traffic)
    # skip this entirely — behavior below stays byte-compatible for them.
    thread_route = await find_thread_route(job_id, thread_id, dependencies=dependencies)
    if thread_route is not None:
        route_state = str(thread_route.get("state") or "")
        if job_status in ("completed", "failed", "cancelled"):
            # After disposition: the reply is recorded and wakes the officer
            # rather than pretending a finished job can resume.
            if resolver_kind == "user" and thread_route.get("project_id"):
                try:
                    from orchestrator.services.session_wake import notify_officer

                    await notify_officer(
                        dependencies.store,
                        str(thread_route["project_id"]),
                        source="worker_message",
                        dedup_key=(
                            f"late-reply:{thread_route.get('route_id')}:{sequence}"
                        ),
                        payload={
                            "summary": (
                                f"The user replied on thread {thread_id} of job "
                                f"{job_id[:8]} after it reached {job_status}: "
                                f"{message[:200]}"
                            ),
                            "job_id": job_id,
                            "thread_id": thread_id,
                        },
                    )
                    dependencies.kick_officer_event_drain(dependencies.store)
                except Exception:
                    logger.warning(
                        "late-reply officer wake failed for job %s",
                        job_id[:8],
                        exc_info=True,
                    )
            return await _delivered("recorded_after_disposition")
        if (
            resolver_kind == "user"
            and route_state == "resolved_by_officer"
            and job_status == "processing"
        ):
            # §5.3: the officer answered first; the later user reply is
            # higher authority — deliver it as a sourced steer through the
            # P1-A guidance lane instead of parking it for a phase boundary.
            strategy = await queue_supervisor_guidance(
                job,
                thread_id,
                (
                    f"[Reply from the job owner on thread {thread_id} — it "
                    "supersedes the project officer's earlier answer on this "
                    f"thread]\n{message}"
                ),
                dependencies=dependencies,
            )
            if strategy:
                return await _delivered(strategy)
        elif resolver_kind == "user" and route_state in (
            "pending_officer",
            "pending_both",
            "escalated_to_user",
            "delivery_failed",
        ):
            # A user answer on a still-open (async) route closes it.
            await record_route_reply_resolution(
                job_id,
                thread_id,
                actor_kind="user",
                actor_id=resolver_id or user_id,
                note="user replied on open route",
                dependencies=dependencies,
            )

    # Look up user delivery preferences
    user_prefs = {}
    if user_id:
        try:
            user_settings = await dependencies.store.get_user_settings(user_id)
            user_prefs = (
                (user_settings or {}).get("communication", {}).get("delivery", {})
            )
        except Exception:
            pass  # Non-critical — fall back to defaults

    # Check urgent flag (explicit from cockpit, or user preference).
    # Urgent ≠ resume anymore (P1-A): a live run gets the message as
    # next-turn guidance; only a job with no live run is resumed to
    # deliver it.
    urgent_override = user_prefs.get("urgent_override", True)
    if urgent and urgent_override:
        strategy = await queue_supervisor_guidance(
            job, thread_id, message, dependencies=dependencies
        )
        if strategy:
            return await _delivered(strategy)
        await _resume_reply_or_conflict(reason=URGENT_RESUME_REASON)
        return await _delivered(await _resumed_strategy("immediate_interrupt"))

    # Check user's async reply preference (same semantics as urgent above)
    async_pref = user_prefs.get("async_reply", "next_strategic_phase")
    if async_pref == "immediate_interrupt":
        strategy = await queue_supervisor_guidance(
            job, thread_id, message, dependencies=dependencies
        )
        if strategy:
            return await _delivered(strategy)
        await _resume_reply_or_conflict(reason=URGENT_RESUME_REASON)
        return await _delivered(await _resumed_strategy("immediate_interrupt"))

    # LLM triage: let auxiliary model decide guidance-now vs queue
    if async_pref == "llm_triage" and job.get("status") == "processing":
        try:
            from orchestrator.services.message_triage import triage_message

            decision = await triage_message(
                message=message,
                job_status=job.get("status", ""),
                job_description=job.get("description", ""),
                phase_number=job.get("phase_number"),
                db=dependencies.store,
            )
            if decision.get("action") == "interrupt":
                strategy = await queue_supervisor_guidance(
                    job, thread_id, message, dependencies=dependencies
                )
                if strategy:
                    logger.info(
                        "LLM triage: next-turn guidance for job %s — %s",
                        job_id[:8],
                        decision.get("reason", ""),
                    )
                    return await _delivered("llm_triage_guidance")
                # No live run after all — fall through to the queued lane.
        except Exception as e:
            logger.warning("LLM triage failed, falling through to queue: %s", e)

    # Default: queue for next strategic phase. Atomic array append so two
    # concurrent inbound replies both land — the old read-modify-write full-dict
    # rewrite lost one of a racing pair (its RMW window spans the LLM triage
    # call above).
    queued_reply = await dependencies.store.append_queued_reply(
        job_id,
        {
            "id": str(uuid4()),
            "thread_id": thread_id,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        **dependencies.completion_dispatch_guard_kwargs(),
    )
    if not queued_reply:
        await dependencies.guard_completion_control(job_id, source="inbound_reply")
        raise HTTPException(
            status_code=409,
            detail="Job changed while the inbound reply was being queued",
        )

    # Broadcast reply_delivered to cockpit SSE
    try:
        from orchestrator.services.notification_feed import notification_feed

        job_owner_id = str(job.get("user_id", "")) if job.get("user_id") else None
        if job_owner_id:
            notification_feed.broadcast(
                user_id=job_owner_id,
                event_type="reply_delivered",
                data={"job_id": job_id, "thread_id": thread_id},
            )
    except Exception:
        pass  # Non-critical

    return await _delivered("next_strategic_phase")


__all__ = [
    "URGENT_RESUME_REASON",
    "InboundReplyDependencies",
    "find_thread_route",
    "queue_supervisor_guidance",
    "record_route_reply_resolution",
    "route_inbound_reply",
]
