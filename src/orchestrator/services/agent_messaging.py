"""Agent → human message delivery: the send funnel and its officer-routed leg.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane M, census group
``J_messaging``). ``POST /api/jobs/{job_id}/messages/send`` is the only path a
worker's message to a human takes, so every refusal, every quota reservation
and every write-before-provider-I/O ordering in it is a contract; none of them
is re-derived here.

Two operations, split exactly where they already were:

* :func:`send_agent_message` — recipient resolution, the M1 policy resolve, the
  OC-07 quota reservation, the §5.1 fallback to direct user delivery, and the
  direct blocking/async legs.
* :func:`send_officer_routed_message` — the ``officer_first`` /
  ``officer_and_user`` leg. It returns ``None`` for an infrastructure failure
  (the caller re-runs the plain path; the job was NOT frozen) and raises
  ``HTTPException(409)`` when the freeze guard/CAS lost.

Ordering preserved literally:

* **The durable intent is written before any provider call.** Quota intent,
  delivery attempt, route row and message-log row all commit first; the
  notifier runs afterwards and its outcome settles those rows.
* **The blocking route, the message and the ``waiting_for_reply`` flip are one
  transaction** (``create_routed_blocking_freeze``, audit OC-01), on both the
  officer-routed and the user-direct path.
* **One server-owned generation is both the route identity and the quota charge
  identity**, so a retry recovers the same intent instead of minting a second
  route or consuming a second bucket slot.

``notifier`` and ``store`` arrive on :class:`AgentMessagingDependencies` rather
than being imported, because suites rebind ``notification_service`` and
``postgres_db`` on ``orchestrator.main``; resolving either in this module's
namespace would make such a rebind green but inert. ``completion_commands_enabled``
and ``kick_officer_event_drain`` are callables for the same reason — the first
is a B08-owned import-time flag, the second a name tests patch on ``main``.
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.schemas.messaging import MessageSendRequest
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


@dataclass
class AgentMessagingDependencies:
    """Collaborators for one agent-message send, resolved per invocation."""

    store: Any
    notifier: Any
    require_internal: Callable[[Request], Awaitable[None]]
    completion_commands_enabled: Callable[[], bool]
    kick_officer_event_drain: Callable[[Any], None]


def mask_email(email: str) -> str:
    """Mask email for display: alice@example.com -> a***@example.com"""
    if not email or "@" not in email:
        return email or ""
    local, domain = email.rsplit("@", 1)
    return f"{local[0]}***@{domain}" if len(local) > 1 else f"*@{domain}"


VALID_MESSAGE_PURPOSES = ("question", "blocker", "update")


async def send_officer_routed_message(
    *,
    dependencies: AgentMessagingDependencies,
    job: dict[str, Any],
    job_id: str,
    request: MessageSendRequest,
    routing: dict[str, Any],
    applied_policy: str,
    thread_id: str,
    sequence: int,
    user_id: str,
    recipient_email: str,
    recipient_name: str,
    purpose: str | None,
    routing_generation: str,
    quota_intent: dict[str, Any],
) -> dict[str, Any] | None:
    """Deliver one worker message through the officer chain (M2/M3).

    Returns the endpoint response dict on success. Returns None when the
    officer leg failed for infrastructure reasons — §5.1's immediate
    fallback: the caller re-runs the plain user_direct path, the job was NOT
    frozen (the routed transaction is all-or-nothing). Raises
    HTTPException(409) when the job's freeze guard/CAS lost — the job is no
    longer freezable and no fallback may freeze it either.
    """
    from orchestrator.services import message_routing as routing_svc

    officer_tid = str(routing["officer_thread_id"])
    incarnation = routing.get("officer_incarnation")
    minutes = int(
        routing.get("officer_response_minutes")
        or routing_svc.DEFAULT_OFFICER_RESPONSE_MINUTES
    )
    project_id = routing.get("project_id")
    now = datetime.now(timezone.utc)
    state = "pending_officer" if applied_policy == "officer_first" else "pending_both"
    blocking = request.mode == "blocking"
    # One server-owned generation is both the route identity and the quota
    # charge identity.  Retries can therefore recover the same durable intent
    # without inventing a second route or consuming a second bucket slot.
    route_id = routing_generation
    snapshot = routing_svc.snapshot_for_route(
        routing, applied=applied_policy, purpose=purpose
    )
    transitions = [
        routing_svc.build_transition(
            None,
            state,
            actor_kind="system",
            actor_id="send",
            officer_incarnation=incarnation,
            note=f"created ({applied_policy}, {'blocking' if blocking else 'async'})",
        )
    ]

    def _response(
        *,
        recipient: str,
        to_name: str,
        dispatch: dict[str, Any] | None,
        route_state: str,
    ) -> dict[str, Any]:
        return {
            "status": "sent",
            "thread_id": thread_id,
            "sequence": sequence,
            "file_path": f"messages/{thread_id}/{sequence:03d}_sent.md",
            "recipient": recipient,
            "to_name": to_name,
            "email_delivered": bool((dispatch or {}).get("email", False)),
            "channels": dispatch or {},
            "routing": {
                "policy": routing.get("requested"),
                "applied": applied_policy,
                "reason": routing.get("reason"),
                "route_id": route_id,
                "state": route_state,
                "officer_response_minutes": minutes,
            },
        }

    async def _dispatch_user_leg(
        *, message_log_id: str | None, blocking: bool
    ) -> dict[str, Any]:
        # The feed row is the user leg (D1); the ledger row id rides in the
        # payload so the (possibly deferred) email's Message-ID lands on
        # message_log for In-Reply-To routing.
        result = await dependencies.notifier.record_agent_message(
            user_id=user_id,
            job={
                **job,
                "config_name": canonical_config_name(
                    job.get("config_name") or "worker_base"
                ),
            },
            job_id=job_id,
            thread_id=thread_id,
            sequence=sequence,
            subject=request.subject,
            message_md=request.message,
            blocking=blocking,
            message_log_id=message_log_id,
            deliver_to=(recipient_email, recipient_name),
        )
        return result.as_dispatch()

    # Claim the non-idempotent side of this generation before creating a
    # route. A concurrent retry can reserve the same quota row, but cannot
    # create a second route or call a provider while this short attempt lease
    # is live. Sticky acceptance makes later request retries read-only.
    attempt = await routing_svc.begin_delivery_attempt(dependencies.store, quota_intent)
    if not attempt.get("delivery_claimed"):
        if attempt.get("accepted"):
            existing = await dependencies.store.get_message_route(route_id)
            return _response(
                recipient=(
                    "project officer"
                    if applied_policy == "officer_first"
                    else mask_email(recipient_email)
                ),
                to_name=(
                    "Project officer"
                    if applied_policy == "officer_first"
                    else recipient_name
                ),
                dispatch=None,
                route_state=(str(existing.get("state")) if existing else state),
            )
        raise HTTPException(
            status_code=409,
            detail="This message generation already has a delivery attempt in progress",
        )

    if blocking:
        deadlines = routing_svc.route_deadlines(
            blocking=True,
            state=state,
            officer_response_minutes=minutes,
            timeout_hours=routing_svc.blocking_timeout_hours(job),
            now=now,
        )
        freeze_data = {
            "status": "waiting_for_reply",
            "freeze_type": "blocking_message",
            "thread_id": thread_id,
            "subject": request.subject,
            "timestamp": now.isoformat(),
            "job_id": job_id,
            # Route/freeze generation fence: the reconciler's resume matches
            # this against the route it timed out, so a job re-frozen by a
            # LATER message can never be resumed against an old route.
            "route_id": route_id,
            "routing": applied_policy,
        }
        label = purpose or "question"
        if state == "pending_officer":
            wake_summary = (
                f"BLOCKING worker {label} from job {job_id[:8]} (thread "
                f"{thread_id}): {request.subject!r} — the worker is frozen "
                f"waiting. Answer with reply_to_job_message or hand it to the "
                f"user with escalate_job_message; unanswered it escalates "
                f"automatically in {minutes} min."
            )
        else:
            wake_summary = (
                f"BLOCKING worker {label} from job {job_id[:8]} (thread "
                f"{thread_id}): {request.subject!r} — the user was notified "
                f"in parallel; the first valid answer resumes the worker."
            )
        route = {
            "route_id": route_id,
            "job_id": job_id,
            "project_id": project_id,
            "thread_id": thread_id,
            "policy_snapshot": snapshot,
            "state": state,
            "officer_thread_id": officer_tid,
            "officer_incarnation": incarnation,
            "officer_deadline": deadlines["officer_deadline"],
            "total_deadline": deadlines["total_deadline"],
            "transitions": transitions,
            "routing_generation": routing_generation,
            "effective_audience": (
                "officer" if state == "pending_officer" else "officer_and_user"
            ),
        }
        wake = {
            "thread_id": officer_tid,
            "source": "worker_message",
            "dedup_key": f"route:{route_id}",
            "payload": {
                "summary": wake_summary,
                "job_id": job_id,
                "thread_id": thread_id,
                "subject": request.subject,
                "blocking": True,
                "purpose": purpose,
                "route_id": route_id,
            },
        }
        message_entry = {
            "user_id": user_id,
            "recipient_email": recipient_email if state == "pending_both" else None,
            "subject": request.subject,
            "message": request.message,
            "status": "sent" if state == "pending_officer" else "pending",
        }
        lane = str(job.get("execution_lane") or "pinned")
        try:
            created = await dependencies.store.create_routed_blocking_freeze(
                job_id,
                freeze_data,
                route=route,
                message_entry=message_entry,
                wake=wake,
                expected_lane=lane,
                lease_token=request.lease_token,
                agent_id=request.agent_id,
                pinned_delivery_id=request.pinned_delivery_id,
                pinned_projection_digest=request.pinned_projection_digest,
                pinned_delivery_proof=request.pinned_delivery_proof,
                pinned_process_generation=request.pinned_process_generation,
                pinned_pod_uid=request.pinned_pod_uid,
                completion_commands_enabled=dependencies.completion_commands_enabled(),
            )
        except Exception:
            await routing_svc.settle_delivery_attempt(
                dependencies.store,
                quota_intent,
                attempt,
                accepted=False,
                failure_class="route_commit_failed",
            )
            logger.exception(
                "Officer-routed blocking send failed for job %s — falling "
                "back to direct user delivery (§5.1)",
                job_id[:8],
            )
            return None
        if created is None:
            await routing_svc.settle_delivery_attempt(
                dependencies.store,
                quota_intent,
                attempt,
                accepted=False,
                failure_class="route_guard_lost",
            )
            raise HTTPException(
                status_code=409,
                detail="Job changed before blocking message was committed",
            )
        # Latency: the wake row is durable; this just delivers it now.
        dependencies.kick_officer_event_drain(dependencies.store)

        if state == "pending_officer":
            await routing_svc.settle_delivery_attempt(
                dependencies.store,
                quota_intent,
                attempt,
                accepted=True,
                detail="durable officer route and wake queued",
            )

        dispatch: dict[str, Any] | None = None
        if state == "pending_both":
            try:
                dispatch = await _dispatch_user_leg(
                    message_log_id=str(created["originating_message_id"]),
                    blocking=True,
                )
            except Exception:
                await routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    quota_intent,
                    attempt,
                    accepted=False,
                    failure_class="notifier_exception",
                )
                await dependencies.store.settle_outbound_message_log(
                    created["originating_message_id"],
                    accepted=False,
                    error_message="notifier exception",
                )
                # user_delivery_at stays NULL — the reconciler redelivers.
                logger.warning(
                    "officer_and_user user leg failed for route %s "
                    "(reconciler will redeliver)",
                    route_id[:8],
                    exc_info=True,
                )
            else:
                outcome = routing_svc.classify_dispatch(dispatch)
                await routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    quota_intent,
                    attempt,
                    accepted=outcome.accepted,
                    failure_class=(None if outcome.accepted else "provider_rejected"),
                    detail=outcome.detail,
                )
                await dependencies.store.settle_outbound_message_log(
                    created["originating_message_id"],
                    accepted=outcome.accepted,
                    error_message=outcome.detail,
                    email_message_id=dispatch.get("email_message_id"),
                )
                if outcome.accepted:
                    await dependencies.store.mark_route_user_delivery(route_id)
        if state == "pending_officer":
            return _response(
                recipient="project officer",
                to_name="Project officer",
                dispatch=None,
                route_state=state,
            )
        return _response(
            recipient=mask_email(recipient_email),
            to_name=recipient_name,
            dispatch=dispatch,
            route_state=state,
        )

    # ---- async modes: no freeze; the route row IS the durable inbox item.
    if applied_policy == "officer_first":
        # Officer only initially (§2 policy table) — no user notification.
        # No wake either: async items coalesce into the officer's next
        # inbox/SITREP section instead of costing a paid wake each.
        try:
            # notification-ledger: the officer-only leg's outbound record (no user notification by policy)
            log_row = await dependencies.store.log_message(
                job_id=job_id,
                user_id=user_id,
                thread_id=thread_id,
                direction="outbound",
                recipient_email=None,
                subject=request.subject,
                message=request.message,
                mode="async",
                status="sent",
                routing_generation=routing_generation,
                effective_audience="officer",
            )
            created_route = await dependencies.store.create_message_route(
                {
                    "route_id": route_id,
                    "job_id": job_id,
                    "project_id": project_id,
                    "thread_id": thread_id,
                    "originating_message_id": (log_row or {}).get("id"),
                    "policy_snapshot": snapshot,
                    "state": "pending_officer",
                    "blocking": False,
                    "officer_thread_id": officer_tid,
                    "officer_incarnation": incarnation,
                    "transitions": transitions,
                    "routing_generation": routing_generation,
                    "effective_audience": "officer",
                }
            )
            if not created_route:
                await routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    quota_intent,
                    attempt,
                    accepted=False,
                    failure_class="route_commit_failed",
                )
                return None
        except Exception:
            await routing_svc.settle_delivery_attempt(
                dependencies.store,
                quota_intent,
                attempt,
                accepted=False,
                failure_class="route_commit_failed",
            )
            logger.exception(
                "Async officer_first route failed for job %s — falling back "
                "to direct user delivery",
                job_id[:8],
            )
            return None
        await routing_svc.settle_delivery_attempt(
            dependencies.store,
            quota_intent,
            attempt,
            accepted=True,
            detail="durable officer route queued",
        )
        return _response(
            recipient="project officer",
            to_name="Project officer",
            dispatch=None,
            route_state="pending_officer",
        )

    # officer_and_user, async: immediate delivery to both (ratified) — the
    # user leg is the unchanged notification path; the officer sees the open
    # route in his next inbox/SITREP.
    # Persist the Officer inbox route before invoking the user notifier. The
    # reconciler can therefore repair a crash at every later fault point.
    # notification-ledger: prelogged before the feed row so its Message-ID can be stamped
    log_row = await dependencies.store.log_message(
        job_id=job_id,
        user_id=user_id,
        thread_id=thread_id,
        direction="outbound",
        recipient_email=recipient_email,
        subject=request.subject,
        message=request.message,
        mode="async",
        status="pending",
        routing_generation=routing_generation,
        effective_audience="officer_and_user",
    )
    created_route = await dependencies.store.create_message_route(
        {
            "route_id": route_id,
            "job_id": job_id,
            "project_id": project_id,
            "thread_id": thread_id,
            "originating_message_id": (log_row or {}).get("id"),
            "policy_snapshot": snapshot,
            "state": "pending_both",
            "blocking": False,
            "officer_thread_id": officer_tid,
            "officer_incarnation": incarnation,
            "transitions": transitions,
            "routing_generation": routing_generation,
            "effective_audience": "officer_and_user",
        }
    )
    if not created_route:
        await routing_svc.settle_delivery_attempt(
            dependencies.store,
            quota_intent,
            attempt,
            accepted=False,
            failure_class="route_commit_failed",
        )
        return None
    try:
        dispatch = await _dispatch_user_leg(
            message_log_id=(str(log_row["id"]) if log_row else None), blocking=False
        )
    except Exception:
        await routing_svc.settle_delivery_attempt(
            dependencies.store,
            quota_intent,
            attempt,
            accepted=False,
            failure_class="notifier_exception",
        )
        raise
    outcome = routing_svc.classify_dispatch(dispatch)
    await routing_svc.settle_delivery_attempt(
        dependencies.store,
        quota_intent,
        attempt,
        accepted=outcome.accepted,
        failure_class=(None if outcome.accepted else "provider_rejected"),
        detail=outcome.detail,
    )
    if dispatch.get("email_message_id") and log_row:
        await dependencies.store.settle_outbound_message_log(
            str(log_row["id"]),
            accepted=outcome.accepted,
            error_message=outcome.detail,
            email_message_id=dispatch.get("email_message_id"),
        )
    elif log_row:
        await dependencies.store.settle_outbound_message_log(
            str(log_row["id"]),
            accepted=outcome.accepted,
            error_message=outcome.detail,
        )
    if outcome.accepted:
        await dependencies.store.mark_route_user_delivery(route_id)
    return _response(
        recipient=mask_email(recipient_email),
        to_name=recipient_name,
        dispatch=dispatch,
        route_state="pending_both",
    )


async def send_agent_message(
    req: Request,
    job_id: str,
    request: MessageSendRequest,
    *,
    dependencies: AgentMessagingDependencies,
) -> dict[str, Any]:
    """Send a message from an agent to a human. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    The Pydantic body keeps its historical name ``request`` to avoid
    churning the body of this long handler; the FastAPI Request handle
    is named ``req`` for the gate call only.

    Resolves recipient from job ownership, checks rate limits, sends
    email, and logs to message_log.
    """
    try:
        # Validate job exists and has an owner
        job = await dependencies.store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        user_id = str(job.get("user_id", "")) if job.get("user_id") else None
        if not user_id:
            raise HTTPException(
                status_code=404,
                detail="Job has no associated user. Cannot resolve recipient.",
            )

        # Resolve recipient
        if request.to == "user":
            # Job owner
            user = await dependencies.store.get_user(user_id)
            if not user or not user.get("email"):
                raise HTTPException(
                    status_code=404,
                    detail="Job owner has no email address.",
                )
            recipient_email = user["email"]
            recipient_name = user.get("display_name", "User")
        else:
            # Multi-recipient: resolve from project members
            project_id = request.project_id or (
                str(job["project_id"]) if job.get("project_id") else None
            )
            if not project_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Cannot resolve recipient '{request.to}': "
                        "job has no project_id. Use to='user' for the job owner."
                    ),
                )
            members = await dependencies.store.get_project_members(project_id)
            if not members:
                raise HTTPException(
                    status_code=404,
                    detail="No project members found.",
                )
            # Match by display_name or email (case-insensitive)
            to_lower = request.to.lower()
            match = None
            for m in members:
                if (
                    m.get("email", "").lower() == to_lower
                    or m.get("display_name", "").lower() == to_lower
                ):
                    match = m
                    break
            if not match:
                # Fallback: contacts registry (channel-aware; email path here)
                resolved = await dependencies.store.resolve_contact(
                    project_id, request.to, "email"
                )
                if resolved["status"] == "ok":
                    recipient_email = resolved["address"]
                    recipient_name = resolved["display_name"]
                    # Contacts don't have a user_id — keep job owner's
                elif resolved["status"] == "no_channel_address":
                    raise HTTPException(
                        status_code=404,
                        detail=(
                            f"{resolved['display_name']} has no email address "
                            f"({', '.join(resolved['channels']) or 'no addresses'} only)."
                        ),
                    )
                elif resolved["status"] == "ambiguous":
                    cands = "; ".join(
                        f"{c['display_name']} <{', '.join(c['addresses'])}>"
                        for c in resolved["candidates"]
                    )
                    raise HTTPException(
                        status_code=404,
                        detail=(
                            f"Recipient '{request.to}' is ambiguous — specify an address. "
                            f"Candidates: {cands}"
                        ),
                    )
                else:
                    available = ", ".join(m.get("display_name", "?") for m in members)
                    contact_rows = await dependencies.store.get_project_contacts(
                        project_id
                    )
                    if contact_rows:
                        names = ", ".join(
                            c.get("display_name", "?") for c in contact_rows
                        )
                        available += f" | Contacts: {names}"
                    raise HTTPException(
                        status_code=404,
                        detail=(
                            f"Recipient '{request.to}' not found among project members "
                            f"or contacts. Available: {available}"
                        ),
                    )
            else:
                recipient_email = match["email"]
                recipient_name = match.get("display_name", "User")
                user_id = str(match["user_id"])

        # Generate thread_id if not provided
        thread_id = request.thread_id or secrets.token_hex(3)

        # Get sequence number
        sequence = await dependencies.store.get_message_sequence(job_id, thread_id)

        # M1 — resolve the project's worker-message routing policy for
        # owner-directed messages (officer_message_routing.md §2). Explicit
        # named recipients stay direct; any resolver failure degrades to
        # user_direct — the officer layer must never break plain messaging.
        purpose = request.purpose if request.purpose in VALID_MESSAGE_PURPOSES else None
        routing: dict[str, Any] | None = None
        if request.to == "user":
            try:
                from orchestrator.services import message_routing as _routing_svc

                routing = await _routing_svc.resolve_effective_policy(
                    dependencies.store, job
                )
            except Exception:
                logger.exception(
                    "Worker-message policy resolution failed for job %s — "
                    "using user_direct",
                    job_id[:8],
                )
                routing = None

        applied_policy = routing["applied"] if routing else "user_direct"
        applied_reason = (
            routing["reason"]
            if routing
            else ("explicit_recipient" if request.to != "user" else "resolver_failed")
        )
        if (
            applied_policy == "officer_first"
            and request.mode == "blocking"
            and routing is not None
            and routing.get("officer_held")
        ):
            # §5.1: a held officer is unavailable for the blocking SLA —
            # the question goes to the user immediately. Async officer_first
            # keeps its route and queues behind the hold.
            applied_policy = "user_direct"
            applied_reason = "officer_held"

        # OC-07: quota follows the server-resolved durable audience.  The
        # generation is opaque idempotency only; it conveys no routing or
        # quota authority and all audience selection above is server-owned.
        from orchestrator.services import message_routing as _routing_svc

        routing_generation = str(request.routing_generation or uuid4())
        route_project_id = str(job["project_id"]) if job.get("project_id") else None

        async def _reserve_delivery(
            bucket: str,
            audience: str,
            reason: str,
        ) -> dict[str, Any] | JSONResponse:
            intent = await _routing_svc.reserve_quota_intent(
                dependencies.store,
                routing_generation=routing_generation,
                route_id=routing_generation,
                bucket=bucket,
                effective_audience=audience,
                job_id=job_id,
                project_id=route_project_id,
                user_id=user_id if bucket == "human" else None,
                reason=reason,
            )
            if intent.get("allowed"):
                return intent
            limit_name = str(intent.get("limit") or "message_quota")
            retry_after = int(intent.get("retry_after_seconds") or 3600)
            # notification-ledger: durable intent row logged before any provider I/O
            await dependencies.store.log_message(
                job_id=job_id,
                thread_id=thread_id,
                direction="outbound",
                subject=request.subject,
                message=request.message,
                status="rate_limited",
                user_id=user_id,
                mode=request.mode,
                error_message=f"Rate limit: {limit_name}",
                routing_generation=routing_generation,
                effective_audience=audience,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "status": "rate_limited",
                    "error": f"Rate limit exceeded: {limit_name}",
                    "bucket": bucket,
                    "retry_after_seconds": retry_after,
                },
            )

        if applied_policy in ("officer_first", "officer_and_user"):
            audience = (
                "officer" if applied_policy == "officer_first" else "officer_and_user"
            )
            quota_intent = await _reserve_delivery(
                "officer_internal" if applied_policy == "officer_first" else "human",
                audience,
                applied_reason,
            )
            if isinstance(quota_intent, JSONResponse):
                return quota_intent
            officer_result = await send_officer_routed_message(
                dependencies=dependencies,
                job=job,
                job_id=job_id,
                request=request,
                routing=routing,
                applied_policy=applied_policy,
                thread_id=thread_id,
                sequence=sequence,
                user_id=user_id,
                recipient_email=recipient_email,
                recipient_name=recipient_name,
                purpose=purpose,
                routing_generation=routing_generation,
                quota_intent=quota_intent,
            )
            if officer_result is not None:
                return officer_result
            # §5.1 immediate fallback: the officer leg failed without
            # freezing the job — deliver directly to the user instead.
            applied_policy = "user_direct"
            applied_reason = "officer_route_failed"

        direct_audience = "human" if request.to == "user" else "explicit_recipient"
        direct_intent = await _reserve_delivery(
            "human", direct_audience, applied_reason
        )
        if isinstance(direct_intent, JSONResponse):
            return direct_intent

        direct_attempt = await _routing_svc.begin_delivery_attempt(
            dependencies.store, direct_intent
        )
        if not direct_attempt.get("delivery_claimed"):
            if direct_attempt.get("accepted"):
                return {
                    "status": "sent",
                    "thread_id": thread_id,
                    "sequence": sequence,
                    "file_path": f"messages/{thread_id}/{sequence:03d}_sent.md",
                    "recipient": mask_email(recipient_email),
                    "to_name": recipient_name,
                    "email_delivered": False,
                    "channels": {},
                    "routing": {
                        "policy": (
                            routing.get("requested") if routing else "user_direct"
                        ),
                        "applied": "user_direct",
                        "reason": "idempotent_replay",
                        "route_id": (
                            routing_generation if request.mode == "blocking" else None
                        ),
                        "state": (
                            "user_direct" if request.mode == "blocking" else None
                        ),
                    },
                }
            raise HTTPException(
                status_code=409,
                detail=(
                    "This message generation already has a delivery attempt in progress"
                ),
            )

        route_id: str | None = None
        freeze_data: dict[str, Any] | None = None
        direct_message_id: str | None = None
        if request.mode == "blocking":
            route_id = routing_generation
            freeze_data = {
                "status": "waiting_for_reply",
                "freeze_type": "blocking_message",
                "thread_id": thread_id,
                "subject": request.subject,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "job_id": job_id,
                "route_id": route_id,
                "routing": "user_direct",
            }
            # ONE transaction: the logical message, the route, and the job's
            # waiting_for_reply flip on the same route generation (audit
            # OC-01). This used to be a freeze followed by best-effort route
            # bookkeeping, so a crash between them left a job waiting forever
            # with nothing for the total-timeout reconciler to claim —
            # and user_direct is the DEFAULT policy, so that was the common
            # path. Under backlog pools such a job also held its one-shot
            # ticket claim and pool capacity indefinitely.
            #
            # External delivery happens AFTER this commits. If the commit
            # fails, nothing is written and the job stays runnable; there is
            # no compensating "unfreeze" to get wrong.
            from orchestrator.services import message_routing as _routing_svc

            _snapshot = (
                _routing_svc.snapshot_for_route(
                    routing,
                    applied="user_direct",
                    reason=applied_reason,
                    purpose=purpose,
                )
                if routing
                else {
                    "worker_messages": "user_direct",
                    "applied": "user_direct",
                    "reason": applied_reason,
                    "resolved_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            _deadlines = _routing_svc.route_deadlines(
                blocking=True,
                state="user_direct",
                officer_response_minutes=0,
                timeout_hours=_routing_svc.blocking_timeout_hours(job),
            )
            _direct_route = {
                "route_id": route_id,
                "job_id": job_id,
                "project_id": (
                    str(job["project_id"]) if job.get("project_id") else None
                ),
                "thread_id": thread_id,
                "policy_snapshot": _snapshot,
                "state": "user_direct",
                "blocking": True,
                "total_deadline": _deadlines["total_deadline"],
                "transitions": [
                    _routing_svc.build_transition(
                        None,
                        "user_direct",
                        actor_kind="system",
                        actor_id="send",
                        note=f"created (user_direct: {applied_reason})",
                    )
                ],
                "routing_generation": routing_generation,
                "effective_audience": direct_audience,
            }
            try:
                direct_committed = await dependencies.store.create_routed_blocking_freeze(
                    job_id,
                    freeze_data,
                    route=_direct_route,
                    message_entry={
                        "user_id": user_id,
                        "recipient_email": recipient_email,
                        "subject": request.subject,
                        "message": request.message,
                        "status": "pending",
                    },
                    wake=None,
                    expected_lane=str(job.get("execution_lane") or "pinned"),
                    lease_token=request.lease_token,
                    agent_id=request.agent_id,
                    pinned_delivery_id=request.pinned_delivery_id,
                    pinned_projection_digest=request.pinned_projection_digest,
                    pinned_delivery_proof=request.pinned_delivery_proof,
                    pinned_process_generation=request.pinned_process_generation,
                    pinned_pod_uid=request.pinned_pod_uid,
                    completion_commands_enabled=dependencies.completion_commands_enabled(),
                )
            except Exception:
                await _routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    direct_intent,
                    direct_attempt,
                    accepted=False,
                    failure_class="route_commit_failed",
                )
                raise
            if direct_committed is None:
                await _routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    direct_intent,
                    direct_attempt,
                    accepted=False,
                    failure_class="route_guard_lost",
                )
                raise HTTPException(
                    status_code=409,
                    detail="Job changed before blocking message was committed",
                )
            direct_message_id = str(direct_committed["originating_message_id"])
        else:
            # Async direct delivery has the same write-before-side-effect law as
            # the blocking route transaction.  In particular, a provider that
            # accepts the notification immediately before this process dies
            # must not leave the durable ledger as the only operator-visible
            # account of what was sent.
            # notification-ledger: prelogged before the feed row so its Message-ID can be stamped
            direct_message = await dependencies.store.log_message(
                job_id=job_id,
                user_id=user_id,
                thread_id=thread_id,
                direction="outbound",
                recipient_email=recipient_email,
                subject=request.subject,
                message=request.message,
                mode=request.mode,
                status="pending",
                routing_generation=routing_generation,
                effective_audience=direct_audience,
            )
            if not direct_message or not direct_message.get("id"):
                await _routing_svc.settle_delivery_attempt(
                    dependencies.store,
                    direct_intent,
                    direct_attempt,
                    accepted=False,
                    failure_class="message_log_failed",
                )
                raise HTTPException(
                    status_code=503,
                    detail="Message delivery could not be durably recorded",
                )
            direct_message_id = str(direct_message["id"])

        # Dispatch only after the durable intent (and, for blocking sends,
        # the route/freeze unit) exists.  Attempt and settlement are separate
        # durable facts so provider failure never masquerades as acceptance.
        try:
            record_result = await dependencies.notifier.record_agent_message(
                user_id=user_id,
                job={
                    **job,
                    "config_name": canonical_config_name(
                        job.get("config_name") or "worker_base"
                    ),
                },
                job_id=job_id,
                thread_id=thread_id,
                sequence=sequence,
                subject=request.subject,
                message_md=request.message,
                blocking=request.mode == "blocking",
                message_log_id=direct_message_id,
                # A named contact (no user row) still gets the mail; the feed
                # row belongs to the owner, who is the party with the stake.
                deliver_to=(recipient_email, recipient_name),
            )
            dispatch_results = record_result.as_dispatch()
        except Exception:
            await _routing_svc.settle_delivery_attempt(
                dependencies.store,
                direct_intent,
                direct_attempt,
                accepted=False,
                failure_class="notifier_exception",
            )
            if direct_message_id is not None:
                await dependencies.store.settle_outbound_message_log(
                    direct_message_id,
                    accepted=False,
                    error_message="notification provider raised an exception",
                )
            raise

        dispatch_outcome = _routing_svc.classify_dispatch(dispatch_results)
        await _routing_svc.settle_delivery_attempt(
            dependencies.store,
            direct_intent,
            direct_attempt,
            accepted=dispatch_outcome.accepted,
            failure_class=(None if dispatch_outcome.accepted else "provider_rejected"),
            detail=dispatch_outcome.detail,
        )

        email_sent = dispatch_results.get("email", False)
        email_msg_id = dispatch_results.get("email_message_id")

        # Both modes prelogged exactly one row before provider I/O. Settle that
        # row instead of appending a second, outcome-only message.
        if direct_message_id is not None:
            await dependencies.store.settle_outbound_message_log(
                direct_message_id,
                accepted=dispatch_outcome.accepted,
                error_message=dispatch_outcome.detail,
                email_message_id=email_msg_id,
            )
        if request.mode == "blocking":
            if dispatch_outcome.accepted:
                await dependencies.store.mark_route_user_delivery(str(route_id))

        file_path = f"messages/{thread_id}/{sequence:03d}_sent.md"

        response: dict[str, Any] = {
            "status": "sent",
            "thread_id": thread_id,
            "sequence": sequence,
            "file_path": file_path,
            "recipient": mask_email(recipient_email),
            "to_name": recipient_name,
            "email_delivered": email_sent,
            "channels": dispatch_results,
        }
        if request.to == "user":
            response["routing"] = {
                "policy": routing.get("requested") if routing else "user_direct",
                "applied": "user_direct",
                "reason": applied_reason,
                "route_id": route_id,
                "state": "user_direct" if route_id else None,
            }
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to send agent message for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "VALID_MESSAGE_PURPOSES",
    "AgentMessagingDependencies",
    "mask_email",
    "send_agent_message",
    "send_officer_routed_message",
]
