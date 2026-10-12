"""Wake an interactive session when a worker job it created finishes.

A session can already launch worker jobs. The missing half was the *return
path*: the orchestrator flipped a status column and went quiet, so a session
that delegated work either sat in a polling loop or simply never learned the
result. This module is that return path.

Framing it as "a notification feature" undersells it. Every session-created job
dispatches immediately (``create_job`` ends in ``_trigger_dispatch()``) and
there is no "start after", so a session that lays out a multi-stage plan — three
designers, then a critic, then a human gate, then an implementer — cannot
execute it. Its only options are to fire everything at once or to launch each
stage when the previous one lands. **The wake is what makes the second possible;
the conversation becomes the scheduler.**

Design: knowledge-base/knowledge/features/session_wake_on_job_completion.md.

Shape
-----
``maybe_wake_session`` is a *decision function*, not a hook on one choke point,
because there is no terminal-state choke point in this codebase — a job also
reaches a terminal state via cascade-cancel, critic approval of its target
(which never calls ``/complete`` at all), diff accept/reject, a dozen
dispatch-time failures, and several sweepers. Missing one would mean a session
that waits forever, which is *indistinguishable from the bug this feature
fixes*, so the design does not rely on enumerating them: the covered paths call
the decision function for **latency**, and the claim query independently finds
terminal jobs that owe a wake **by status**, so an unhooked path costs one
sweeper tick rather than a lost completion. Adding a hook to a newly-discovered
terminal path is therefore an optimization, never a bug fix.

The durable claim on the jobs row is the MECHANISM; the post-commit send is a
latency optimization layered on top of it. Nothing in the completion path is
transactional (``postgres_db.acquire()`` yields a raw pooled connection; every
statement autocommits), so a crash between the status write and a direct POST
loses the wake permanently and silently. Because the fast path goes through the
same atomic claim, losing it costs latency, not correctness.

Two rules this module exists to enforce
---------------------------------------
1. **Claim, commit, then send.** Never hold a transaction open across the HTTP
   call. Every send carries a durable server-owned delivery id; the agent
   inserts its transcript row once. A retry is acknowledged only after the
   durable input ledger proves provider/turn admission, never from transcript
   persistence alone.
2. **Do NOT leader-gate any of this.** ``_trigger_dispatch``'s leader gate sits
   right next to where the completion hook goes and invites a copy-paste, but it
   exists because *dispatch is a singleton*, not for dedup. The agent's
   ``/complete`` POST is already load-balanced to exactly one replica, so
   leader-gating the wake would fire it only when the agent happened to hit the
   leader and would silently drop ~half of all wakes. The claim is what provides
   single-firing, and it works from every replica.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from orchestrator.services.session_lifecycle import probe_ready
from orchestrator.services.session_runtime_admission import (
    thread_requests_protected_cloud,
    thread_runtime_authority,
)
from shared.content_redaction import sanitize_text
from shared.job_outcome import effective_job_status
from shared.pinned_session_identity import PinnedSessionBinding
from orchestrator.services.usage_ledger import llm_tokens_from_rows

logger = logging.getLogger(__name__)


class DurableWakeOutboxError(RuntimeError):
    """The established wake route could not durably insert its outbox row."""


class WakeDeliveryResult(str, Enum):
    """Truthful boundary reached by one persistent-runtime delivery attempt."""

    EXECUTED = "executed"
    PERSISTED = "persisted"
    FAILED = "failed"


# Statuses that owe the creating session a wake. 'pending_review' is included on
# purpose: the job stopped and wants a decision, which is exactly the moment the
# session should hear about it. It is also why the dedup key carries the status
# — a later approve flips pending_review → completed, and that IS a second,
# legitimate wake.
TERMINAL_STATUSES = ("completed", "failed", "cancelled", "pending_review")

# Statuses that additionally wake the PROJECT's officer (centurion.md §4).
# 'paused' is officer-only on purpose: it must never enter TERMINAL_STATUSES —
# the jobs outbox would spam ordinary sessions and corrupt wake_notified_status
# for redispatchable pauses (implementation notes, risk 6) — but a paused job
# is exactly what an officer exists to notice.
OFFICER_NOTIFY_STATUSES = TERMINAL_STATUSES + ("paused",)

# Backstop cadence. The opportunistic post-commit drain handles the happy path
# in milliseconds; this only catches wakes whose sender died or whose terminal
# path has no hook. Env-tunable mainly so tests can drive ticks faster.
TICK_SECONDS = int(os.getenv("SESSION_WAKE_SWEEP_SECONDS", "20"))

# How long a claim is exclusively ours before another replica may re-claim it.
# Must comfortably exceed one delivery attempt (probe 2s + POST ≤ 10s) so a slow
# send is not double-delivered; short enough that a SIGKILL mid-rollout costs
# seconds, not minutes, of latency.
VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("SESSION_WAKE_VISIBILITY_SECONDS", "120"))

# Attempt budget before a wake is buried as 'dead'. Burying matters: without it
# one permanently-unreachable session re-claims forever and starves live wakes
# behind it in the claim's ORDER BY.
MAX_ATTEMPTS = int(os.getenv("SESSION_WAKE_MAX_ATTEMPTS", "8"))

# Claim batch size. Bounded because every row in a batch holds its claim until
# the batch finishes; see the visibility-window arithmetic in drain_pending_wakes
# before raising it.
CLAIM_BATCH = int(os.getenv("SESSION_WAKE_CLAIM_BATCH", "20"))

# In-flight deliveries per drain. Keeps the worst-case batch inside the
# visibility window and makes a fan-out land together instead of in sequence.
# Small enough that a burst cannot open 20 sockets to 20 dead pod IPs at once.
_DELIVER_CONCURRENCY = int(os.getenv("SESSION_WAKE_DELIVER_CONCURRENCY", "5"))

# Summary text is a pointer to the result, not the result. Anything longer
# belongs behind get_job.
_SUMMARY_CHARS = 500

_TERMINAL_FOR_COUNTS = ("completed", "failed", "cancelled")
_WAKE_DELIVERY_NAMESPACE = "ada612a0-95c7-5e7e-83c3-8c37613455de"


def _job_wake_delivery_id(row: dict[str, Any]) -> str:
    """Stable identity for one job/status wake across response loss."""

    # PostgreSQL must derive the same identity inside the atomic claim without
    # depending on an extension-specific uuid_generate_v5 installation. MD5 is
    # used only as a deterministic identifier transform, never for security.
    material = (
        f"{_WAKE_DELIVERY_NAMESPACE}:job:{row.get('id')}:{row.get('status') or ''}"
    )
    return str(UUID(hashlib.md5(material.encode(), usedforsecurity=False).hexdigest()))


# --------------------------------------------------------------------------
# Firing
# --------------------------------------------------------------------------


async def maybe_wake_session(db: Any, job_id: str, terminal_status: str) -> bool:
    """Record that ``job_id`` reaching ``terminal_status`` owes a session a wake.

    Returns True iff a wake was newly enqueued. Safe to call from any completion
    path, for any job, more than once: every filter that matters (was this job
    created by a session, did its owner opt out, has this exact terminal status
    already been delivered, is a wake already in flight) lives in the SQL guard,
    so the caller needs to know nothing about the job.

    This is a **latency optimization, not the mechanism**. The claim query finds
    terminal jobs owing a wake by status on its own, so a terminal path that
    forgets to call this still delivers — one sweeper tick later instead of
    immediately. Calling it moves that to milliseconds and makes "a wake is
    owed" an explicit, greppable row state instead of an inference.

    Deliberately takes an id rather than the job dict. Terminal paths hand
    around job dicts assembled by half a dozen different queries with different
    projections; a dict-based pre-filter would silently no-op wherever the
    projection lacks the column — the exact silent-drop failure mode this
    feature exists to remove. One indexed UPDATE by primary key is cheaper than
    that risk.

    Never raises: a completion path must not fail because a notification could
    not be enqueued.
    """
    # Route auto-close first: a genuinely terminal job (not pending_review or
    # paused — both can still resume and answer) closes its still-open
    # worker-message routes, so the officer wake enqueued just below renders a
    # sitrep that no longer lists the dead question as "open". Same choke-point
    # rationale as the officer leg; terminal paths that never call this are
    # swept by the message-route reconciler's backstop leg. Never raises.
    if terminal_status in _TERMINAL_FOR_COUNTS:
        try:
            from orchestrator.services.message_routing import (
                close_routes_for_terminal_job,
            )

            await close_routes_for_terminal_job(db, job_id, terminal_status)
        except Exception:
            logger.exception(
                "session wake: route auto-close failed for job %s (%s)",
                str(job_id)[:8],
                terminal_status,
            )

    # Officer leg: this function is already called from every covered
    # terminal path with the right status, which makes it the one choke point
    # that reaches the project's officer without touching eight call sites.
    # Independent of the jobs-outbox guard below — the officer hears about
    # every project job, not just session-created ones.
    if terminal_status in OFFICER_NOTIFY_STATUSES:
        await _notify_project_officer_of_job(db, job_id, terminal_status)

    if terminal_status not in TERMINAL_STATUSES:
        return False
    try:
        enqueued = await db.mark_job_wake_pending(str(job_id), terminal_status)
    except Exception:
        logger.exception(
            "session wake: failed to enqueue for job %s (%s)",
            str(job_id)[:8],
            terminal_status,
        )
        return False
    if enqueued:
        logger.info(
            "session wake: enqueued for job %s (%s)", str(job_id)[:8], terminal_status
        )
    return enqueued


def kick_drain(db: Any) -> None:
    """Fire-and-forget the claim-and-send right after a completion commits.

    Pure latency optimization. Losing this task — cancelled at shutdown, raised
    and swallowed, never scheduled because the process died — is harmless by
    construction: the row is already durably 'pending' and the sweeper re-claims
    it. That is the whole reason the claim comes first.
    """

    async def _run() -> None:
        try:
            await drain_pending_wakes(db)
        except Exception:
            logger.exception("session wake: opportunistic drain raised (non-fatal)")

    try:
        asyncio.create_task(_run(), name="session-wake-drain")
    except RuntimeError:
        # No running loop (sync context / shutdown). The sweeper covers it.
        pass


# --------------------------------------------------------------------------
# Claim + deliver
# --------------------------------------------------------------------------


async def drain_pending_wakes(db: Any, *, limit: int = CLAIM_BATCH) -> int:
    """Claim owed wakes and deliver them. Returns the number delivered.

    Claim first, COMMIT, then send — the claim call returns with its transaction
    closed, so no lock is held across the HTTP call. Holding one there is a
    documented way to melt a database (lock-acquisition degradation plus dead
    tuples from the long-lived snapshot), and it buys nothing: the visibility
    timeout already covers the crash case.

    **The batch must finish inside the visibility window.** A claim only belongs
    to this drain for ``VISIBILITY_TIMEOUT_SECONDS``; overrun it and another
    replica re-claims a row this one is still sending, which is the duplicate
    the whole claim exists to prevent. Delivering serially would blow that
    budget on the realistic bad case — a fan-out into dead pods costs ~12s each
    (2s probe + up to 10s POST), so 20 of them is ~240s against a 120s window.
    Hence bounded concurrency: at ``_DELIVER_CONCURRENCY`` in flight, worst-case
    batch time is ``ceil(limit / concurrency) * 12s``, comfortably inside the
    window at the shipped values. Anything that raises ``CLAIM_BATCH`` or lowers
    ``VISIBILITY_TIMEOUT_SECONDS`` has to re-check that arithmetic.

    Concurrency also happens to be right for the workload: these are independent
    HTTP calls to different pods, and the case the feature exists for — six jobs
    of a fan-out landing together — is precisely the one serial delivery would
    make slowest.
    """
    try:
        claimed = await db.claim_pending_job_wakes(
            limit=limit, visibility_timeout_seconds=VISIBILITY_TIMEOUT_SECONDS
        )
    except Exception:
        logger.exception("session wake: claim failed")
        return 0
    if not claimed:
        return 0

    gate = asyncio.Semaphore(_DELIVER_CONCURRENCY)

    async def _one(row: dict[str, Any]) -> bool:
        async with gate:
            return await _deliver_and_settle(db, row)

    results = await asyncio.gather(
        *(_one(row) for row in claimed), return_exceptions=True
    )
    return sum(1 for r in results if r is True)


async def _deliver_and_settle(db: Any, row: dict[str, Any]) -> bool:
    """Deliver one claimed wake and record the outcome. True iff delivered.

    Settling is in its own try: a delivery that succeeded but whose settle write
    failed must NOT be reported as delivered, and must leave the row in
    'sending' so the visibility timeout re-claims it. Re-delivering a notice is
    the lesser evil against marking one sent that never arrived.
    """
    job_id = str(row["id"])
    status = str(row.get("status") or "")
    try:
        outcome = await _deliver(db, row)
    except Exception:
        logger.exception("session wake: delivery raised for job %s", job_id[:8])
        outcome = WakeDeliveryResult.FAILED

    try:
        # Boolean True remains accepted for narrow test doubles and for the
        # no-recipient retirement branch. Production injectors return the
        # explicit enum so transcript persistence can never masquerade as
        # execution.
        if outcome is True or outcome == WakeDeliveryResult.EXECUTED:
            settled = await db.finish_job_wake(job_id, status)
            if settled is False:
                logger.info(
                    "session wake: claim for job %s was retired before settle",
                    job_id[:8],
                )
                return False
            return True
        if outcome == WakeDeliveryResult.PERSISTED:
            await db.defer_job_wake_for_input(job_id)
            return False
        state = await db.release_job_wake(job_id, max_attempts=MAX_ATTEMPTS)
        if state == "undeliverable":
            logger.info(
                "session wake: failed claim for job %s was retired by thread delete",
                job_id[:8],
            )
        elif state == "dead":
            logger.error(
                "session wake: job %s exhausted %d attempts — thread %s will "
                "never learn this job finished",
                job_id[:8],
                MAX_ATTEMPTS,
                str(row.get("created_by_thread_id"))[:8],
            )
    except Exception:
        # The row stays 'sending'; the visibility timeout re-claims it.
        logger.exception("session wake: failed to settle claim for job %s", job_id[:8])
    return False


async def _deliver(db: Any, row: dict[str, Any]) -> bool | WakeDeliveryResult:
    """Deliver one claim, distinguishing persistence from execution."""
    thread_id = row.get("created_by_thread_id")
    if not thread_id:
        # Forward hard deletes atomically retire the row before the FK is
        # nulled. This branch covers a stale claimed projection (or a legacy
        # orphan); finish_job_wake's CAS decides whether it was retired.
        return True
    thread_id = str(thread_id)

    # Re-read the thread INSIDE the attempt rather than trusting anything read
    # at claim time. 'awaiting_user' in particular races a 60s sweeper
    # (mark_orphaned_threads_suspended) that rewrites exactly that state to
    # 'suspended' with agent_id = NULL, so the binding can vanish between the
    # claim and the send.
    try:
        thread = await db.get_thread(thread_id)
    except Exception:
        logger.exception("session wake: thread lookup failed for %s", thread_id[:8])
        return False
    if thread is None:
        # A hard delete that began after claim retires the jobs row in the same
        # transaction. Returning success gets us to the guarded finish CAS;
        # it cannot overwrite the distinct undeliverable outcome.
        return True

    if _thread_is_officer(thread):
        # Double-wake suppression (implementation notes, risk 1): an
        # officer-created job would otherwise wake him through BOTH outboxes —
        # [JOB_FINISHED] here plus the officer event from the completion hook —
        # costing two paid turns per delegation. Convert this claim into an
        # officer event instead; the dedup key matches the hook's enqueue, so
        # whichever lands second coalesces away. The jobs outbox stays the
        # durable trigger (its by-status backstop still finds the job); the
        # event outbox is the single officer delivery channel.
        try:
            effective_status = effective_job_status(row)
            await db.enqueue_session_wake_event(
                thread_id,
                source="job_transition",
                dedup_key=_officer_job_dedup_key(row["id"], effective_status),
                payload={
                    "job_id": str(row["id"]),
                    "status": effective_status,
                    "description": _truncate(str(row.get("description") or ""), 200),
                },
                project_id=(str(row["project_id"]) if row.get("project_id") else None),
            )
        except Exception:
            logger.exception(
                "session wake: officer conversion failed for job %s — keeping "
                "the jobs-outbox claim for retry",
                str(row["id"])[:8],
            )
            return False
        kick_event_drain(db)
        return True

    text = await _format_wake_message(db, row, thread_id)

    delivery_id = _job_wake_delivery_id(row)
    if not await db.assign_job_wake_delivery(str(row["id"]), delivery_id):
        return WakeDeliveryResult.FAILED

    # Pod IP is a recyclable coordinate, never recipient authority. All K8s
    # wakes enter the durable inbox; the exact current runtime generation then
    # claims them with its reciprocal thread/agent/Pod identity.

    # Suspended / detached / ended, or the live inject bounced. Write the notice
    # durably so it lands on the next resume, and tell the user out-of-band.
    #
    # 'ended' is deliberately NOT a do-nothing case: an active thread whose pod
    # dies is marked 'ended', not 'suspended', and ended threads are
    # user-resumable — treating 'ended' as terminal would silently drop
    # completions for a supported case.
    return await _deliver_durable(
        db,
        thread,
        thread_id,
        row,
        text,
        delivery_id=delivery_id,
    )


async def _resolve_live_agent(
    db: Any, thread: dict[str, Any]
) -> Optional[PinnedSessionBinding]:
    """Return the exact ready binding serving one pinned thread, if any.

    Wakes themselves always enter the durable inbox. The Officer watchdog uses
    this read-only probe only to distinguish a healthy sleeping runtime from a
    missing one, so the network check is fenced by binding reads on both sides.
    """

    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:
        return None
    try:
        binding = await db.get_pinned_session_binding(
            runtime_authority.thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
    except Exception:
        return None
    if binding is None or binding.agent_status not in ("ready", "working", "session"):
        return None
    if not await probe_ready(
        binding.pod_ip,
        binding.pod_port,
        required_capability="durable_input_delivery",
        require_protected_cloud=thread_requests_protected_cloud(thread),
        expected_session_identity_fingerprint=binding.session_identity_fingerprint,
    ):
        return None
    try:
        current = await db.get_pinned_session_binding(
            runtime_authority.thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
    except Exception:
        return None
    if (
        current is None
        or current.target_key != binding.target_key
        or current.agent_status not in ("ready", "working", "session")
    ):
        return None
    return current


# --------------------------------------------------------------------------
# Durable branch
# --------------------------------------------------------------------------


async def _deliver_durable(
    db: Any,
    thread: dict[str, Any],
    thread_id: str,
    row: dict[str, Any],
    text: str,
    *,
    delivery_id: str,
) -> WakeDeliveryResult:
    """Persist the notice and notify the user out-of-band.

    The durable branch atomically writes ``thread_messages`` plus
    ``thread_input_deliveries``, NOT ``thread_events``. The transcript is the
    truthful conversation record but is not an executable inbox by itself; the
    ledger is what a current runtime generation claims and admits. The event log
    is unusable here because its ``seq`` is allocated by the *agent*, in process
    memory, and appending into a live epoch from the orchestrator would silently
    destroy a batch of the agent's frames.

    No pod restore happens here. On attach, the exact reciprocal runtime claims
    unadmitted ledger rows in transcript order; ordinary restore deliberately
    excludes them so persistence cannot become passive context.
    """
    try:
        result = await db.persist_thread_input_delivery(
            thread_id=thread_id,
            delivery_id=delivery_id,
            role="event",
            content=text,
            # Every server-injected persistent input uses one source contract.
            # Its job/event provenance remains in the authoritative outbox;
            # using another ledger source would make a later live retry of the
            # same delivery identity look like a conflict.
            source="officer_wake",
        )
    except Exception:
        logger.exception(
            "session wake: durable write failed for thread %s", thread_id[:8]
        )
        return WakeDeliveryResult.FAILED

    logger.info(
        "session wake: wrote durable notice to thread %s (status=%s, job %s)",
        thread_id[:8],
        thread.get("status"),
        str(row["id"])[:8],
    )

    state = _delivery_state_for_thread(result, thread_id)
    if state is None:
        logger.error(
            "session wake: durable delivery identity did not resolve to intended "
            "thread %s",
            thread_id[:8],
        )
        return WakeDeliveryResult.FAILED

    disposition = str(result.get("execution_disposition") or "current")
    if disposition in {"historical", "superseded"}:
        # The stable wake identity was already observed before a rewind. Close
        # the source outbox obligation without scheduling another model turn.
        return WakeDeliveryResult.EXECUTED

    # Best-effort user-facing ping. Only on the durable branch — a live session
    # already showed the user the wake, and emailing them about it would be
    # noise. Failure here must not un-deliver the notice above.
    if result.get("transcript_inserted"):
        try:
            await _notify_owner(db, thread, thread_id, row)
        except Exception:
            logger.warning(
                "session wake: owner notification failed for thread %s", thread_id[:8]
            )
    if state in {"admitted", "settled"}:
        return WakeDeliveryResult.EXECUTED
    if state in {"persisted", "owned", "queued", "deferred"}:
        return WakeDeliveryResult.PERSISTED
    return WakeDeliveryResult.FAILED


def _delivery_state_for_thread(result: Any, thread_id: str) -> str | None:
    """Return a delivery state only for the intended authoritative thread."""

    if not isinstance(result, dict):
        return None
    if str(result.get("thread_id") or "") != str(thread_id):
        return None
    state = str(result.get("state") or "")
    if state not in {
        "persisted",
        "owned",
        "queued",
        "deferred",
        "admitted",
        "settled",
    }:
        return None
    return state


async def _notify_owner(
    db: Any, thread: dict[str, Any], thread_id: str, row: dict[str, Any]
) -> None:
    """Tell the owner that a job their session launched finished while the
    tab was closed — a ``session_wake`` feed row (unified notification
    system). The row is the durable half that reaches a user who is gone:
    in-app now, mail after the escalation window unless they looked.
    """
    from orchestrator.services.notification_service import notification_service

    user_id = thread.get("user_id")
    if not user_id:
        return

    job_id = str(row["id"])
    short = job_id[:8]
    # A delegated job's description is written by the session's agent (OC-05);
    # redacted before the cut, for the body and the payload alike.
    description = sanitize_text(row.get("description") or "")[:100]
    status = effective_job_status(row, fallback="finished")
    title = sanitize_text(thread.get("title")) or None  # LLM-generated
    await notification_service.record(
        recipient_id=str(user_id),
        category="session_wake",
        dedup_key=f"session_wake:{thread_id}:{job_id}",
        subject=f"Job {short} {status} — your session is waiting",
        body=(
            f"**Job `{short}`** launched from your session "
            f"**{title or 'Untitled'}** is now `{status}`.\n\n"
            f"**Task:** {description}\n\n"
            "Reopen the session to pick the result up."
        ),
        source_kind="thread",
        source_id=str(thread_id),
        action_params={"thread_id": str(thread_id), "job_id": job_id},
        payload={
            "thread_id": str(thread_id),
            "job_id": job_id,
            "job_description": description,
            "config_name": str(row.get("config_name") or "worker_base"),
            "status": status,
            "title": title,
        },
    )


# --------------------------------------------------------------------------
# Payload
# --------------------------------------------------------------------------


async def _format_wake_message(db: Any, row: dict[str, Any], thread_id: str) -> str:
    """Render the injected notice.

    Pointers, not payloads. ``Outputs`` names the tools that read the result; it
    does not inline it. Inlining would make every wake expensive and defeat the
    entire point of delegating the work. ``Task`` is the one field a batch
    formatter could get away without (the old worker delegation formatter
    did) — a session may have fanned out
    three jobs twenty minutes and one compaction ago and cannot otherwise tell
    them apart.

    ``[JOB_FINISHED]`` joins the shipped bracket-tag family and gives the
    cockpit a cheap literal to match.
    """
    job_id = str(row["id"])
    status = effective_job_status(row, fallback="unknown")
    description = (row.get("description") or "").strip()

    lines = [
        "[JOB_FINISHED] A worker job you created has reached a terminal state.",
        "",
        f"- Job: {job_id[:8]} ({await _agent_label(db, row)})",
        f"- Status: {status}",
    ]
    if description:
        lines.append(f"- Task: {_excerpt(description, 300)}")

    freeze = _as_dict(row.get("freeze_data"))
    summary = str(freeze.get("summary") or "").strip()
    if summary:
        lines.append(f"- Summary: {_excerpt(summary, _SUMMARY_CHARS)}")
    confidence = freeze.get("confidence")
    if confidence is not None:
        lines.append(f"- Confidence: {confidence}")
    deliverables = freeze.get("deliverables")
    read_hint = (
        "get_job for the summary; list_job_files / "
        "get_job_file for the job repo's files (committed state as "
        "of the worker's last push — mid-phase work is not visible; pass ref "
        "for a phase tag)"
    )
    if isinstance(deliverables, list) and deliverables:
        lines.append(f"- Outputs: {len(deliverables)} deliverables — {read_hint}")
    else:
        lines.append(f"- Outputs: {read_hint}")

    error = (row.get("error_message") or "").strip()
    if error and status in ("failed", "cancelled"):
        lines.append(f"- Error: {_excerpt(error, 300)}")

    siblings = await _sibling_line(db, thread_id)
    if siblings:
        lines += ["", siblings]

    # Exactly one line of closing instruction. It is a reminder; the policy
    # lives in the <scheduled_work> system-prompt block.
    lines += ["", "Decide now: inspect this result, or note it and continue."]
    return "\n".join(lines)


async def _agent_label(db: Any, row: dict[str, Any]) -> str:
    """'expert: designer' when a DB expert drove the job, else the config name."""
    expert_id = row.get("expert_id")
    if expert_id:
        try:
            expert = await db.get_expert_by_id(str(expert_id))
        except Exception:
            expert = None
        if expert and expert.get("name"):
            return f"expert: {expert['name']}"
    return f"config: {row.get('config_name') or 'worker_base'}"


async def _sibling_line(db: Any, thread_id: str) -> str:
    """ "1 of 3 finished — 1 still running, 1 failed".

    Free once created_by_thread_id exists, and it saves the agent a
    list_jobs round-trip on EVERY wake just to decide whether this is the
    moment to act.
    """
    try:
        counts = await db.get_thread_job_counts(thread_id)
    except Exception:
        return ""
    total = counts.get("total") or 0
    if total <= 1:
        return ""
    parts = []
    if counts.get("running"):
        parts.append(f"{counts['running']} still running")
    if counts.get("failed"):
        parts.append(f"{counts['failed']} failed")
    if counts.get("cancelled"):
        parts.append(f"{counts['cancelled']} cancelled")
    tail = f" — {', '.join(parts)}" if parts else ""
    return (
        f"Your outstanding jobs: {counts.get('finished', 0)} of {total} finished{tail}."
    )


def _as_dict(value: Any) -> dict[str, Any]:
    """JSONB reads come back as raw JSON strings on this driver; parse defensively."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _truncate(text: str, limit: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _excerpt(text: str, limit: int) -> str:
    """:func:`_truncate` for worker-reachable text, redacted first (OC-05).

    Mirrors ``sitrep._excerpt``: a worker summary or a failed push's error can
    carry a credential into a model's wake, and redacting after the cut would
    leave a fragment no pattern recognizes.
    """
    return _truncate(sanitize_text(text), limit)


# --------------------------------------------------------------------------
# Backstop
# --------------------------------------------------------------------------


async def session_wake_sweeper_loop(db: Any, shutdown_event: asyncio.Event) -> None:
    """Deliver every wake the fast path missed, until shutdown.

    Two classes of miss, both handled by the same claim query: a send whose
    replica died holding the claim, and a terminal path that never called
    :func:`maybe_wake_session` at all (dispatch-time failures, the LLM-outage
    fail path, VM-upgrade approval expiry — all direct DB writes with no hook).
    The second is why this loop is a correctness component and not just a
    retrier.

    **Not leader-gated, on purpose** — see the module docstring. Single-firing
    comes from the claim, which works from every replica; leader-gating would
    add nothing and would make the loop a SPOF during a leader handover.

    No age-grace is needed here, unlike ``project_loop_sweeper``. That sweeper
    needed one because the state it heals ("both pointer columns cleared") is
    also the normal transient window of a healthy advance, so acting early
    double-spawned a turn. Here the equivalent window — a claim in flight — is
    represented by a distinct state ('sending') that the claim query only
    reconsiders after the visibility timeout. The grace is the timeout.
    """
    logger.info(
        "Session wake sweeper started (tick=%ds, visibility=%ds, max_attempts=%d)",
        TICK_SECONDS,
        VISIBILITY_TIMEOUT_SECONDS,
        MAX_ATTEMPTS,
    )
    gc_countdown = _OFFICER_GC_EVERY_TICKS
    while not shutdown_event.is_set():
        try:
            sent = await drain_pending_wakes(db)
            if sent:
                logger.info("Session wake sweeper delivered %d wake(s)", sent)
        except Exception:
            logger.exception("Session wake sweeper tick raised; will retry next tick")

        # Officer event outbox (centurion.md §4): same claim discipline,
        # separate table — timers become due here, events retry here.
        try:
            sent = await drain_pending_event_wakes(db)
            if sent:
                logger.info("Session wake sweeper delivered %d officer wake(s)", sent)
        except Exception:
            logger.exception("Officer wake sweeper tick raised; will retry next tick")

        gc_countdown -= 1
        if gc_countdown <= 0:
            gc_countdown = _OFFICER_GC_EVERY_TICKS
            try:
                pruned = await db.gc_session_wake_events(
                    sent_retention_seconds=_OFFICER_SENT_RETENTION_SECONDS,
                    dead_retention_seconds=_OFFICER_DEAD_RETENTION_SECONDS,
                )
                if pruned:
                    logger.info("Officer wake GC pruned %d row(s)", pruned)
            except Exception:
                logger.exception("Officer wake GC raised (non-fatal)")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=TICK_SECONDS)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Session wake sweeper stopped")


# --------------------------------------------------------------------------
# Officer event outbox (centurion) — enqueue, timers, drain
# --------------------------------------------------------------------------
#
# The jobs-row outbox above is keyed to jobs.created_by_thread_id and four
# terminal statuses; officer sessions need wakes for non-job events and for
# their own durable sleep timer, so those ride session_wake_events (migration
# 0074, knowledge-base/knowledge/features/centurion.md §4). Delivery reuses the exact same
# primitives (_resolve_live_agent / _inject_live) and the same
# claim-commit-send discipline. Rendering here is deliberately minimal — the
# S3 sitrep service (orchestrator/services/sitrep.py) replaces
# _format_officer_wake with the full computed delta.

_OFFICER_CLAIM_BATCH = 20
_OFFICER_MAX_ATTEMPTS = 60  # transient-retry budget; watchdog respawn can take a while
_OFFICER_GC_EVERY_TICKS = 20
_OFFICER_SENT_RETENTION_SECONDS = 3600  # MUST exceed the largest debounce window
_OFFICER_DEAD_RETENTION_SECONDS = 7 * 24 * 3600

# Per-source debounce (seconds). Timers/respawn/conference are scheduled or
# one-shot — never debounced. Everything else is a wake-rate guard: suppressed
# rows stay pending and coalesce into the next allowed wake, so nothing is
# lost (centurion.md §4).
OFFICER_DEBOUNCE_BY_SOURCE: dict[str, int] = {
    "timer": 0,
    "respawn": 0,
    "conference": 0,
    "commission": 0,  # the continuity brief is one-shot — never debounced
    # Blocking worker questions (officer_message_routing.md §5.1): a frozen
    # worker is waiting on this wake — it may coalesce with already-claimed
    # events into one turn but must never wait for the routine timer.
    "worker_message": 0,
    # A note is the Legate speaking. Two directives a minute apart are two
    # directives; the claim query's unlisted-source default is 0 today, and
    # this entry keeps the note out of a future default's reach.
    "legate": 0,
    "job_transition": 300,
    "sudo_request": 300,
    "loop": 300,
    "fleet": 600,
}


def _thread_is_officer(thread: dict[str, Any]) -> bool:
    """True when a thread dict carries the officer flag in its metadata.

    Mirrors the SQL predicate (postgres.OFFICER_ENABLED_SQL) for code that
    already holds the thread row. Strict: only boolean True / string 'true'
    count, so MagicMock-metadata test threads never trip it.
    """
    metadata = _as_dict(thread.get("metadata"))
    officer = _as_dict(_as_dict(metadata.get("config_override")).get("officer"))
    enabled = officer.get("enabled")
    return enabled is True or (isinstance(enabled, str) and enabled == "true")


def _officer_job_dedup_key(job_id: Any, status: Any) -> str:
    """One dedup key per (job, status) transition, shared by BOTH enqueue
    paths (the completion-hook notify and the jobs-outbox conversion) so they
    coalesce instead of double-waking."""
    return f"{str(job_id)[:8]}:{status}"


def _officer_daily_ceiling(thread: dict[str, Any]) -> int:
    """The thread's ``officer.daily_token_ceiling`` (0 = disabled)."""
    metadata = _as_dict(thread.get("metadata"))
    officer = _as_dict(_as_dict(metadata.get("config_override")).get("officer"))
    try:
        return max(0, int(officer.get("daily_token_ceiling") or 0))
    except (TypeError, ValueError):
        return 0


def _next_utc_midnight(now: datetime) -> datetime:
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


# Officer daily-ceiling metering, bound per store by application composition.
#
# The ceiling brake runs inside ``drain_pending_event_wakes``, which is reached
# from the lifespan sweeper and from ``kick_event_drain(db)`` at a dozen call
# sites that hold nothing but the store (completion hooks, routers, the sudo
# gate, the Officer watchdog). A defaulted parameter would silently disable the
# ceiling on every path that forgot it, and threading the ledger through all of
# them moves the coupling rather than removing it. The store every path already
# carries is the application's identity here, so the application binds its store
# to a provider of its ledger (R1.B10). The provider is read per check: startup
# builds the ledger after the store exists, and a missing ledger fails open
# exactly like an unavailable one. Two applications with two stores reach two
# ledgers.
_UNSET: Any = object()
_METERING_BY_STORE: dict[int, tuple[Any, Callable[[], Any]]] = {}


def bind_officer_wake_metering(store: Any, usage_ledger: Callable[[], Any]) -> None:
    """Bind ``store``'s officer wakes to its application's usage ledger.

    ``usage_ledger`` is a zero-argument provider evaluated at every ceiling
    check. Rebinding the same store replaces its provider.
    """
    _METERING_BY_STORE[id(store)] = (store, usage_ledger)


def unbind_officer_wake_metering(store: Any) -> None:
    """Forget ``store``'s binding (tests and application teardown)."""
    entry = _METERING_BY_STORE.get(id(store))
    if entry is not None and entry[0] is store:
        del _METERING_BY_STORE[id(store)]


def _bound_usage_ledger(store: Any) -> Any:
    """The ledger bound for exactly this store, or None when none is bound."""
    entry = _METERING_BY_STORE.get(id(store))
    if entry is None or entry[0] is not store:
        return None
    return entry[1]()


async def _officer_ceiling_deferral(
    db: Any, thread: dict[str, Any], *, usage_ledger: Any = _UNSET
) -> Optional[datetime]:
    """Daily-token-ceiling brake (centurion.md §4, the third loop-guard layer).

    Returns the UTC budget reset to defer the officer's autonomous wakes to
    when today's session tokens (``usage_events`` rows with
    ``ref_id=thread_id``, materialized from the audit DB) have reached
    ``officer.daily_token_ceiling`` — or None to deliver normally.

    Fail-OPEN on every error: metering being down must never brick wakes.
    The brake only touches the drain — direct Legate input bypasses it, so
    a ceilinged officer still answers his commander immediately. The ledger
    lags live usage by one materializer poll, which is fine for a daily cap.

    The ledger is the one the application bound for ``db``
    (:func:`bind_officer_wake_metering`); an explicit ``usage_ledger`` wins.
    """
    ceiling = _officer_daily_ceiling(thread)
    if ceiling <= 0:
        return None
    try:
        if usage_ledger is _UNSET:
            usage_ledger = _bound_usage_ledger(db)
        if usage_ledger is None or not getattr(usage_ledger, "is_available", False):
            return None
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        usage = await usage_ledger.query_usage(
            from_ts=day_start, to_ts=now, ref_id=str(thread["id"])
        )
        tokens = llm_tokens_from_rows(usage.get("by_category") or [])
        if tokens >= ceiling:
            return _next_utc_midnight(now)
    except Exception:
        logger.warning("officer wake: ceiling check failed (fail-open)", exc_info=True)
    return None


async def _note_ceiling_breach(
    db: Any, thread_id: str, thread: dict[str, Any], deferred_to: datetime
) -> None:
    """One day-stamped notice when the ceiling brake engages.

    The notify contract's 'force-sleep + digest notice': the Legate learns
    the officer went quiet from his feed, not from silence — a ``low``
    ``officer_runtime`` row (in-app only). Idempotent per UTC day via
    ``officer_state.ceiling_notice`` and the row's dedup key.
    """
    try:
        from orchestrator.services.notification_service import notification_service

        state = _as_dict(_as_dict(thread.get("metadata")).get("officer_state"))
        today = datetime.now(timezone.utc).date().isoformat()
        if state.get("ceiling_notice") == today:
            return
        owner_id = thread.get("user_id")
        if owner_id:
            await notification_service.record(
                recipient_id=str(owner_id),
                category="officer_runtime",
                severity="low",
                dedup_key=f"officer_ceiling:{thread_id}:{today}",
                subject="Daily token ceiling reached",
                body=(
                    "The officer's daily token ceiling was reached; his "
                    "autonomous wakes are deferred until "
                    f"{deferred_to.strftime('%Y-%m-%d %H:%M UTC')}. Your "
                    "messages still reach him immediately."
                ),
                source_kind="thread",
                source_id=str(thread_id),
                action_params={
                    "thread_id": str(thread_id),
                    "project_id": (
                        str(thread.get("project_id"))
                        if thread.get("project_id")
                        else None
                    ),
                },
                payload={
                    "thread_id": str(thread_id),
                    "deferred_to": deferred_to.isoformat(),
                },
            )
        await db.merge_thread_officer_state(thread_id, {"ceiling_notice": today})
        logger.warning(
            "officer %s: daily token ceiling reached — wakes deferred to %s",
            thread_id[:8],
            deferred_to.isoformat(),
        )
    except Exception:
        logger.exception("officer wake: ceiling notice failed (non-fatal)")


async def _notify_project_officer_of_job(db: Any, job_id: str, status: str) -> bool:
    """Enqueue an officer wake for a job transition, by project. Never raises.

    The database makes one post-locked decision: enqueue for the exact current
    live incarnation, or append to the while-vacant ledger. Commission cannot
    slip between an unlocked lookup and the write.
    """
    try:
        job = await db.get_job(str(job_id))
        if not job or not job.get("project_id"):
            return False
        project_id = str(job["project_id"])
        effective_status = effective_job_status(job, fallback=str(status))
        decision = await db.route_project_officer_job_transition(
            project_id,
            job_id=str(job_id),
            status=effective_status,
            description=_truncate(str(job.get("description") or ""), 200),
            dedup_key=_officer_job_dedup_key(job_id, effective_status),
        )
        enqueued = bool(decision.get("enqueued"))
        if enqueued:
            kick_event_drain(db)
        return enqueued
    except Exception:
        logger.exception(
            "officer wake: job-transition notify failed for job %s (%s)",
            str(job_id)[:8],
            status,
        )
        return False


async def file_officer_timer(
    db: Any, thread_id: str, minutes: int, reason: str
) -> bool:
    """File (or re-file) the officer's durable sleep timer.

    One pending timer per thread — a new sleep replaces it (upsert on the
    pending unique index). The drain claims it once ``fire_at`` is reached.
    Never raises.
    """
    try:
        fire_at = datetime.now(timezone.utc) + timedelta(minutes=int(minutes))
        return await db.enqueue_session_wake_event(
            str(thread_id),
            source="timer",
            dedup_key="timer",
            payload={"minutes": int(minutes), "reason": reason or ""},
            fire_at=fire_at,
        )
    except Exception:
        logger.exception(
            "officer wake: filing timer failed for thread %s", str(thread_id)[:8]
        )
        return False


async def notify_officer(
    db: Any,
    project_id: str,
    *,
    source: str,
    dedup_key: str,
    payload: Optional[dict[str, Any]] = None,
    _conn: Any = None,
    _thread_id: str | None = None,
) -> bool:
    """Enqueue a wake for the officer commanding ``project_id``.

    No-op (False) when the project has no enabled officer. Never raises — a
    transition path must not fail because a wake could not be enqueued.
    """
    # BP-10: the floor-wake policy owns a larger transaction containing both
    # its outcome ledger and this outbox insert. This internal seam lets that
    # caller reuse the established route without opening a second connection.
    # Errors intentionally escape so its savepoint can roll back and classify
    # the attempt; ordinary callers retain the historical never-raises API.
    if _conn is not None and _thread_id is not None:
        try:
            return await db._enqueue_session_wake_event_on_conn(
                _conn,
                UUID(str(_thread_id)),
                source=source,
                dedup_key=dedup_key,
                payload_json=json.dumps(payload or {}),
                project_uuid=UUID(str(project_id)),
            )
        except Exception as exc:
            raise DurableWakeOutboxError(str(exc)) from exc

    try:
        officer = await db.get_officer_thread_for_project(str(project_id))
        if not officer:
            return False
        return await db.enqueue_session_wake_event(
            str(officer["id"]),
            source=source,
            dedup_key=dedup_key,
            payload=payload or {},
            project_id=str(project_id),
        )
    except Exception:
        logger.exception(
            "officer wake: enqueue failed for project %s (%s)",
            str(project_id)[:8],
            source,
        )
        return False


async def notify_all_officers(
    db: Any,
    *,
    source: str,
    dedup_key: str,
    payload: Optional[dict[str, Any]] = None,
) -> int:
    """Enqueue a fleet-scoped wake for every enabled officer. Never raises."""
    enqueued = 0
    try:
        for officer in await db.list_officer_threads():
            ok = await db.enqueue_session_wake_event(
                str(officer["id"]),
                source=source,
                dedup_key=dedup_key,
                payload=payload or {},
                project_id=(
                    str(officer["project_id"]) if officer.get("project_id") else None
                ),
            )
            enqueued += 1 if ok else 0
    except Exception:
        logger.exception("officer wake: fleet enqueue failed (%s)", source)
    return enqueued


async def notify_owning_officers(
    db: Any,
    payload_by_project: dict[str, dict[str, Any]],
    *,
    source: str,
    dedup_key: str,
) -> int:
    """Enqueue a wake only on each owning project's officer. Never raises.

    Scoped counterpart of :func:`notify_all_officers` for job-derived fleet
    events: a recovery in one project is that project officer's news alone,
    not a fleet broadcast — one livelocked job must not wake every officer on
    the roster each sweep. Each project gets its own payload (its own jobs'
    ids, its own counts). Projects without a commissioned officer are dropped
    by :func:`notify_officer`'s no-op contract; nobody else is woken in their
    place.
    """
    enqueued = 0
    for project_id, payload in payload_by_project.items():
        if not project_id:
            continue
        ok = await notify_officer(
            db,
            str(project_id),
            source=source,
            dedup_key=dedup_key,
            payload=payload,
        )
        enqueued += 1 if ok else 0
    return enqueued


LEGATE_NOTE_SOURCE = "legate"
LEGATE_NOTE_MAX_CHARS = 4000


def _officer_hold(thread: dict[str, Any]) -> dict[str, Any]:
    """Whatever hold is stamped on an officer thread ({} when he is free)."""
    metadata = _as_dict(thread.get("metadata"))
    officer = _as_dict(_as_dict(metadata.get("config_override")).get("officer"))
    return _as_dict(officer.get("hold"))


async def deliver_officer_note(db: Any, thread: dict[str, Any], text: str) -> str:
    """Deliver one Legate note to an officer. Returns how it landed.

    ``'queued'`` means the durable ``legate`` outbox owns the note and its
    delivery identity. The drain persists it for the exact current runtime to
    claim; a recyclable Pod IP is never treated as recipient authority.
    ``'held'`` means the same durable note is fenced behind the current hold.

    The return value is the caller's honesty contract — durable acceptance is
    not reported as provider admission. Each note carries a fresh
    ``dedup_key`` so notes never coalesce with each other.
    """
    hold = _officer_hold(thread)
    delivery_id = str(uuid4())
    project_id = thread.get("project_id")
    await db.enqueue_session_wake_event(
        str(thread["id"]),
        source=LEGATE_NOTE_SOURCE,
        dedup_key=uuid4().hex,
        payload={"message": text, "_delivery_id": delivery_id},
        project_id=str(project_id) if project_id else None,
    )
    return "held" if hold else "queued"


def kick_event_drain(db: Any) -> None:
    """Fire-and-forget the officer event drain after an enqueue commits.

    Latency optimization only — the sweeper re-claims anything this misses.
    """

    async def _run() -> None:
        try:
            await drain_pending_event_wakes(db)
        except Exception:
            logger.exception("officer wake: opportunistic drain raised (non-fatal)")

    try:
        asyncio.create_task(_run(), name="officer-wake-drain")
    except RuntimeError:
        pass


def _format_officer_wake(rows: list[dict[str, Any]]) -> str:
    """Render one coalesced wake message for a batch of claimed rows.

    Minimal v1 rendering — S3's sitrep service replaces this with the full
    computed delta. Keeps the ``[SITREP]`` bracket so the cockpit match and
    the persona instructions stay stable across that upgrade.
    """
    lines = [f"[SITREP] Wake — {len(rows)} reason(s):"]
    for row in rows:
        payload = _as_dict(row.get("payload"))
        source = str(row.get("source") or "event")
        if source == "timer":
            minutes = payload.get("minutes")
            reason = payload.get("reason") or ""
            desc = f"timer: slept ~{minutes} min"
            if reason:
                desc += f" (reason: {_excerpt(str(reason), 160)})"
        elif source == LEGATE_NOTE_SOURCE:
            # The Legate's own words, verbatim — this renderer runs when the
            # sitrep build failed, and a truncated directive is a lost one.
            note = str(payload.get("message") or "").strip()
            desc = f"Legate note:\n{note[:LEGATE_NOTE_MAX_CHARS]}"
        else:
            detail = payload.get("summary") or payload.get("status") or ""
            desc = f"{source}: {row.get('dedup_key')}"
            if detail:
                desc += f" — {_excerpt(str(detail), 200)}"
        lines.append(f"- {desc}")
    lines.append(
        "Assess with your tools, act within your authority, then file a sleep."
    )
    return "\n".join(lines)


async def drain_pending_event_wakes(
    db: Any, *, limit: int = _OFFICER_CLAIM_BATCH
) -> int:
    """Claim due officer wake events and deliver one coalesced wake per thread.

    Same claim-commit-send discipline as :func:`drain_pending_wakes`. Delivery
    outcomes per thread batch:

    * live inject accepted → finish the rows.
    * agent live but inject refused/failed → RELEASE for retry — a running
      officer never re-reads thread_messages, so a durable write would be
      invisible to him until the next restart.
    * no live agent → RELEASE for retry. A pod lifecycle gap is not delivery;
      the replacement consumes the same durable delivery identity.
    """
    try:
        claimed = await db.claim_pending_session_wake_events(
            limit=limit,
            visibility_timeout_seconds=VISIBILITY_TIMEOUT_SECONDS,
            debounce_seconds_by_source=OFFICER_DEBOUNCE_BY_SOURCE,
        )
    except Exception:
        logger.exception("officer wake: claim failed")
        return 0
    if not claimed:
        return 0

    try:
        assigned = await db.assign_session_wake_delivery_groups(
            [int(row["id"]) for row in claimed]
        )
    except Exception:
        logger.exception("officer wake: durable delivery identity assignment failed")
        try:
            await db.release_session_wake_events(
                [int(row["id"]) for row in claimed],
                max_attempts=_OFFICER_MAX_ATTEMPTS,
            )
        except Exception:
            logger.exception("officer wake: release after identity failure failed")
        return 0

    by_delivery: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in assigned:
        key = (str(row["thread_id"]), str(row["delivery_id"]))
        by_delivery.setdefault(key, []).append(row)

    delivered = 0
    for (thread_id, delivery_id), claimed_rows in by_delivery.items():
        ids = [int(r["id"]) for r in claimed_rows]
        try:
            rows = await db.get_session_wake_delivery_group(thread_id, delivery_id)
            if not rows:
                raise RuntimeError("durable wake delivery group disappeared")
            thread = await db.get_thread(thread_id)
            if thread is None:
                await db.finish_session_wake_events(ids)
                continue
            # Runtime-grant incidents are durable Post state, not a transient
            # callback verdict. Permit one compatibility probe so a pre-P0
            # runtime can reach its credential-bearing recovery call, then
            # defer every further wake without provider spend until recovery
            # resolves the incident and re-arms the existing outbox rows.
            project_id = thread.get("project_id")
            if project_id:
                from orchestrator.services.runtime_actor import (
                    admit_officer_wake_for_runtime,
                )

                admitted, retry_at = await admit_officer_wake_for_runtime(
                    db,
                    project_id=str(project_id),
                    thread_id=thread_id,
                )
                if not admitted:
                    floor = datetime.now(timezone.utc) + timedelta(seconds=60)
                    await db.defer_session_wake_events(
                        ids,
                        fire_at=max(retry_at or floor, floor),
                    )
                    continue
            # Daily-token-ceiling brake: defer (not release — no attempts
            # burned) everything to the UTC budget reset and note it once in
            # the digest. Timers ride along, so the watchdog sees a pending
            # timer and files nothing new.
            deferred_to = await _officer_ceiling_deferral(db, thread)
            if deferred_to is not None:
                await db.defer_session_wake_events(ids, fire_at=deferred_to)
                await _note_ceiling_breach(db, thread_id, thread, deferred_to)
                continue
            # Full computed-delta sitrep (services/sitrep.py); the minimal
            # reason-list renderer is the fallback so a formatter failure can
            # never cost the wake itself.
            text = None
            state_patch = None
            try:
                from orchestrator.services import sitrep as sitrep_svc

                text, state_patch = await sitrep_svc.build_wake_message(
                    db, thread, rows
                )
            except Exception:
                logger.exception(
                    "officer wake: sitrep build raised — using minimal renderer"
                )
            if not text:
                text = _format_officer_wake(rows)
                state_patch = None
            # A Pod IP is not a recipient. Keep the outbox authoritative so
            # the exact current/replacement runtime claims this identity.
            persisted = await db.persist_thread_input_delivery(
                thread_id=thread_id,
                delivery_id=delivery_id,
                role="event",
                content=text,
                source="officer_wake",
            )
            delivery_state = _delivery_state_for_thread(persisted, thread_id)
            execution_disposition = str(
                persisted.get("execution_disposition") or "current"
            )
            if execution_disposition in {"historical", "superseded"}:
                await db.finish_session_wake_events(ids)
                delivered += 1
            elif delivery_state in {
                "admitted",
                "settled",
            }:
                await db.finish_session_wake_events(ids)
                delivered += 1
                if state_patch:
                    try:
                        await db.merge_thread_officer_state(thread_id, state_patch)
                    except Exception:
                        logger.exception(
                            "officer wake: sitrep state merge failed "
                            "(non-fatal; next sitrep re-diffs)"
                        )
            elif delivery_state in {"persisted", "owned", "queued", "deferred"}:
                await db.defer_session_wake_events_for_input(
                    ids,
                    fire_at=datetime.now(timezone.utc) + timedelta(seconds=5),
                )
            else:  # pragma: no cover - production method returns a row or raises
                await db.release_session_wake_events(
                    ids, max_attempts=_OFFICER_MAX_ATTEMPTS
                )
        except Exception:
            logger.exception(
                "officer wake: delivery failed for thread %s", thread_id[:8]
            )
            try:
                await db.release_session_wake_events(
                    ids, max_attempts=_OFFICER_MAX_ATTEMPTS
                )
            except Exception:
                logger.exception("officer wake: release failed (rows stay claimed)")
    return delivered
