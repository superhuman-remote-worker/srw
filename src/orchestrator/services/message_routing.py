"""Officer-aware routing for worker→human messages.

Implements the policy/route layer of ``knowledge-base/knowledge/features/officer_message_routing.md``:

* **M1** — :func:`resolve_effective_policy`: the server-side, per-message
  resolver from a job to its project's ``project_officers.communication_policy``.
  Applies only to owner-directed (``to="user"``) messages; explicit named
  recipients stay direct. The result is snapshotted onto the route row so a
  later project-setting change never retargets a waiting question.
* **M2/M3 helpers** — escalation of a route's thread to the user (shared by the
  officer's explicit ``escalate_job_message``, the SLA reconciler, and the
  hold/decommission drains), the hold/decommission drain itself, and the
  reply-lane resolution recorder.

Nothing here mutates job status. The blocking-send transaction and the
resume verbs live in ``main.py`` / ``database/postgres.py``; this module owns
policy resolution and route-state bookkeeping so every lane transitions the
ledger the same way.

Fail-open discipline: policy resolution and route bookkeeping must never make
worker messaging worse than the pre-officer behavior. Any internal failure
degrades to ``user_direct`` (resolution) or to a logged no-op (bookkeeping);
only the dedicated blocking-send transaction is allowed to fail a send.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from shared.content_redaction import sanitize

logger = logging.getLogger(__name__)

# Policy vocabulary (must match the PATCH validator in main.py and the
# project_officers.communication_policy default, officer_first since migration
# 0163).
POLICY_USER_DIRECT = "user_direct"
POLICY_OFFICER_AND_USER = "officer_and_user"
POLICY_OFFICER_FIRST = "officer_first"
_POLICIES = (POLICY_USER_DIRECT, POLICY_OFFICER_AND_USER, POLICY_OFFICER_FIRST)

# Ratified defaults (spec §2.1 / §5.2): 15-minute blocking officer SLA,
# 24-hour total blocking timeout. The SLA is clamped to the PATCH bounds so a
# hand-edited row cannot produce a nonsense deadline.
DEFAULT_OFFICER_RESPONSE_MINUTES = 15
OFFICER_RESPONSE_MINUTES_BOUNDS = (5, 120)
DEFAULT_BLOCKING_TIMEOUT_HOURS = 24.0


def _positive_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True, slots=True)
class MessageQuotaPolicy:
    """Human interruption and Officer-internal flood-control ceilings."""

    human_job_hourly: int
    human_job_daily: int
    human_user_daily: int
    internal_job_hourly: int
    internal_project_daily: int


def message_quota_policy() -> MessageQuotaPolicy:
    """Resolve conservative configurable defaults from deployment env.

    Internal Officer traffic may be more frequent than human paging, but 30
    messages/hour from one job or 300/day across a project is already far
    beyond normal coalesced triage and bounds a runaway without touching the
    human 5/hour, 15/job-day, 30/user-day counters.
    """
    return MessageQuotaPolicy(
        human_job_hourly=_positive_env("MESSAGE_HUMAN_JOB_HOURLY_LIMIT", 5),
        human_job_daily=_positive_env("MESSAGE_HUMAN_JOB_DAILY_LIMIT", 15),
        human_user_daily=_positive_env("MESSAGE_HUMAN_USER_DAILY_LIMIT", 30),
        internal_job_hourly=_positive_env("OFFICER_MESSAGE_JOB_HOURLY_LIMIT", 30),
        internal_project_daily=_positive_env(
            "OFFICER_MESSAGE_PROJECT_DAILY_LIMIT", 300
        ),
    )


async def reserve_quota_intent(
    db: Any,
    *,
    routing_generation: str,
    route_id: str | None,
    bucket: str,
    effective_audience: str,
    job_id: str,
    project_id: str | None,
    user_id: str | None,
    reason: str,
) -> dict[str, Any]:
    """Reserve one durable charge before any delivery side effect."""
    policy = message_quota_policy()
    return await db.reserve_message_delivery_intent(
        routing_generation=routing_generation,
        route_id=route_id,
        bucket=bucket,
        effective_audience=effective_audience,
        job_id=job_id,
        project_id=project_id,
        user_id=user_id,
        job_hourly_limit=policy.human_job_hourly,
        job_daily_limit=policy.human_job_daily,
        user_daily_limit=policy.human_user_daily,
        internal_job_hourly_limit=policy.internal_job_hourly,
        internal_project_daily_limit=policy.internal_project_daily,
        metadata={"reason": reason},
    )


async def begin_delivery_attempt(db: Any, intent: dict[str, Any]) -> dict[str, Any]:
    return await db.begin_message_delivery_attempt(
        str(intent["intent_id"]),
        attempt_timeout_seconds=_positive_env(
            "MESSAGE_DELIVERY_ATTEMPT_TIMEOUT_SECONDS", 60
        ),
    )


async def settle_delivery_attempt(
    db: Any,
    intent: dict[str, Any],
    attempt: dict[str, Any],
    *,
    accepted: bool,
    failure_class: str | None = None,
    detail: str | None = None,
) -> bool:
    return await db.settle_message_delivery_attempt(
        str(intent["intent_id"]),
        int(attempt["attempt_number"]),
        accepted=accepted,
        failure_class=failure_class,
        detail=detail,
    )


# Route states (mirror of migration 0159's CHECK).
ROUTE_OPEN_STATES = (
    "pending_officer",
    "pending_both",
    "user_direct",
    "escalated_to_user",
    "delivery_failed",
)
ROUTE_TERMINAL_STATES = (
    "resolved_by_officer",
    "resolved_by_user",
    "timed_out",
    "closed",
)

# Officer-addressed states an officer action may act on. ``escalated_to_user``
# is deliberately included for replies: an officer answering seconds after his
# SLA escalated is still answering the same waiting worker.
OFFICER_ACTIONABLE_STATES = (
    "pending_officer",
    "pending_both",
    "escalated_to_user",
    "delivery_failed",
)

_TERMINAL_JOB_STATUSES = ("completed", "failed", "cancelled")


def _as_dict(value: Any) -> Dict[str, Any]:
    """Parse a JSONB column that may arrive as a raw string."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _clamped_minutes(value: Any) -> int:
    lo, hi = OFFICER_RESPONSE_MINUTES_BOUNDS
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return DEFAULT_OFFICER_RESPONSE_MINUTES
    return max(lo, min(hi, minutes))


def officer_hold(officer_thread: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The thread's ``officer.hold`` stamp, or None when unheld/absent."""
    if not officer_thread:
        return None
    metadata = _as_dict(officer_thread.get("metadata"))
    officer_cfg = _as_dict(_as_dict(metadata.get("config_override")).get("officer"))
    hold = officer_cfg.get("hold")
    return hold if isinstance(hold, dict) else (None if not hold else {"kind": "?"})


def blocking_timeout_hours(job: Dict[str, Any]) -> float:
    """The job's effective ``communication.blocking_timeout_hours``.

    Resolution order: the job's frozen ``resolved_config.agent`` (what the
    worker actually runs on) → ``config_override`` → the worker_base default
    of 24h. Non-positive/garbage values fall back to the default — a zero
    here would time a question out instantly, which no config author means.
    """
    for source in (
        _as_dict(_as_dict(job.get("resolved_config")).get("agent")),
        _as_dict(job.get("config_override")),
    ):
        comm = _as_dict(source.get("communication"))
        if "blocking_timeout_hours" in comm:
            try:
                hours = float(comm["blocking_timeout_hours"])
            except (TypeError, ValueError):
                continue
            if hours > 0:
                return hours
    return DEFAULT_BLOCKING_TIMEOUT_HOURS


async def resolve_effective_policy(
    db: Any, job: Dict[str, Any], *, to: str = "user"
) -> Dict[str, Any]:
    """Resolve the routing policy for one new logical message from ``job``.

    M1 (spec §2). Returns::

        {
          "requested": <row policy or user_direct>,
          "applied":   <after availability rules — vacancy/no-post/no-project
                        collapse to user_direct; a HOLD does NOT collapse here
                        because the rule is mode-dependent (blocking falls to
                        the user, async queues) — the caller applies it>,
          "reason":    explicit_recipient | no_project | no_post |
                       policy_user_direct | vacant | commissioned,
          "officer_response_minutes": int,
          "officer_thread_id": str | None,
          "officer_incarnation": int | None,
          "officer_held": bool,
          "project_id": str | None,
        }

    Never raises: any lookup failure degrades to ``user_direct`` — the
    officer layer must not be able to break plain worker messaging.
    """

    def _direct(reason: str, *, project_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "requested": POLICY_USER_DIRECT,
            "applied": POLICY_USER_DIRECT,
            "reason": reason,
            "officer_response_minutes": DEFAULT_OFFICER_RESPONSE_MINUTES,
            "officer_thread_id": None,
            "officer_incarnation": None,
            "officer_held": False,
            "project_id": project_id,
        }

    # v1 policy applies only to owner-directed messages (§2.1). An explicit
    # named recipient stays direct and visible in audit.
    if to != "user":
        return _direct("explicit_recipient")

    project_id = job.get("project_id")
    project_id = str(project_id) if project_id else None
    if not project_id:
        # Jobs without exactly one project remain user_direct (§2.1).
        return _direct("no_project")

    try:
        post = await db.get_project_officer(project_id)
    except Exception:
        logger.exception(
            "message routing: post lookup failed for project %s — user_direct",
            project_id[:8],
        )
        return _direct("no_post", project_id=project_id)
    if not post:
        return _direct("no_post", project_id=project_id)

    policy = _as_dict(post.get("communication_policy"))
    requested = policy.get("worker_messages")
    if requested not in _POLICIES:
        # Deliberately user_direct rather than the officer_first column default
        # (0163). The column is NOT NULL and the PATCH validator rejects
        # anything outside _POLICIES, so reaching here means the row was
        # corrupted or hand-edited — and the fail-safe for a question with no
        # trustworthy policy is the human, not a routing hop that could stall.
        requested = POLICY_USER_DIRECT
    minutes = _clamped_minutes(
        policy.get("officer_response_minutes", DEFAULT_OFFICER_RESPONSE_MINUTES)
    )

    if requested == POLICY_USER_DIRECT:
        result = _direct("policy_user_direct", project_id=project_id)
        result["officer_response_minutes"] = minutes
        return result

    # The effective policy is user_direct whenever there is no commissioned,
    # live officer (§2.1) — thread_id NULL (vacant) or a stale link at an
    # ended thread (get_officer_thread_for_project filters those out).
    try:
        officer = await db.get_officer_thread_for_project(project_id)
    except Exception:
        logger.exception(
            "message routing: officer lookup failed for project %s — user_direct",
            project_id[:8],
        )
        officer = None
    if not officer:
        result = _direct("vacant", project_id=project_id)
        result["requested"] = requested
        result["officer_response_minutes"] = minutes
        return result

    incarnations = _as_list(post.get("incarnations"))
    return {
        "requested": requested,
        "applied": requested,
        "reason": "commissioned",
        "officer_response_minutes": minutes,
        "officer_thread_id": str(officer["id"]),
        "officer_incarnation": len(incarnations),
        "officer_held": officer_hold(officer) is not None,
        "project_id": project_id,
    }


def snapshot_for_route(
    routing: Dict[str, Any],
    *,
    applied: str,
    reason: Optional[str] = None,
    purpose: Optional[str] = None,
) -> Dict[str, Any]:
    """The immutable ``policy_snapshot`` JSONB frozen onto a route row."""
    snapshot: Dict[str, Any] = {
        "worker_messages": routing.get("requested", POLICY_USER_DIRECT),
        "applied": applied,
        "reason": reason or routing.get("reason"),
        "officer_response_minutes": routing.get(
            "officer_response_minutes", DEFAULT_OFFICER_RESPONSE_MINUTES
        ),
        "resolved_at": datetime.now(timezone.utc).isoformat(),
    }
    if purpose:
        snapshot["purpose"] = purpose
    return snapshot


def build_transition(
    from_state: Optional[str],
    to_state: str,
    *,
    actor_kind: str,
    actor_id: Optional[str] = None,
    officer_incarnation: Optional[int] = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    """One append-only ``transitions`` audit entry (spec §7)."""
    entry: Dict[str, Any] = {
        "at": datetime.now(timezone.utc).isoformat(),
        "from": from_state,
        "to": to_state,
        "actor_kind": actor_kind,
    }
    if actor_id is not None:
        entry["actor_id"] = str(actor_id)
    if officer_incarnation is not None:
        entry["officer_incarnation"] = int(officer_incarnation)
    if note:
        entry["note"] = note
    return entry


def route_deadlines(
    *,
    blocking: bool,
    state: str,
    officer_response_minutes: int,
    timeout_hours: float,
    now: Optional[datetime] = None,
) -> Dict[str, Optional[datetime]]:
    """Compute the two §5.2 deadlines for a new route.

    Only a blocking ``pending_officer`` route carries the officer SLA; every
    blocking route carries the total deadline.
    """
    now = now or datetime.now(timezone.utc)
    officer_deadline = None
    total_deadline = None
    if blocking:
        total_deadline = now + timedelta(hours=float(timeout_hours))
        if state == "pending_officer":
            officer_deadline = now + timedelta(minutes=int(officer_response_minutes))
    return {"officer_deadline": officer_deadline, "total_deadline": total_deadline}


# ---------------------------------------------------------------------------
# Delivery outcome (audit OC-06)
# ---------------------------------------------------------------------------

# ``user_delivery_at`` means "the notification provider ACCEPTED this", not
# "a human read it". Nothing downstream can observe the latter, and the
# reconciler retries precisely while this stamp is null — so stamping on
# anything weaker than acceptance silently ends the retry loop.
_ACCEPTED = "accepted"
_RETRYABLE = "retryable"


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    """Normalized result of one notification dispatch.

    ``NotificationService.dispatch`` reports failure by RETURNING an error dict
    rather than raising — an uninitialized service is the common case — so a
    try/except around it sees success. Everything that decides route state or
    message-log status derives from this one value instead.
    """

    kind: str
    detail: str = ""

    @property
    def accepted(self) -> bool:
        return self.kind == _ACCEPTED

    @property
    def log_status(self) -> str:
        """What the message log records, from the same normalized outcome."""
        return "sent" if self.accepted else "failed"


def classify_dispatch(result: Any) -> DeliveryOutcome:
    """Map a dispatch return value onto :class:`DeliveryOutcome`.

    Accepted means the provider took ownership: a channel reported success, or
    the message was queued for digest delivery after quiet hours (durable, and
    genuinely accepted). Everything else — an explicit error, an empty result,
    every channel false — stays retryable, because the honest thing to say
    about it is that we do not know it arrived.
    """
    if not isinstance(result, dict):
        return DeliveryOutcome(
            _RETRYABLE, f"non-dict dispatch result: {type(result).__name__}"
        )
    if result.get("error"):
        return DeliveryOutcome(_RETRYABLE, str(result["error"]))
    if not result:
        return DeliveryOutcome(_RETRYABLE, "empty dispatch result")
    if result.get("queued"):
        return DeliveryOutcome(_ACCEPTED, "queued for digest (quiet hours)")
    if any(result.get(channel) for channel in ("email", "ntfy", "in_app", "sse")):
        return DeliveryOutcome(_ACCEPTED, "provider accepted")
    return DeliveryOutcome(_RETRYABLE, "no channel reported success")


# ---------------------------------------------------------------------------
# Escalation — the one shared "hand this thread to the user" path
# ---------------------------------------------------------------------------


def _delimited_user_body(
    original_subject: str,
    original_body: str,
    *,
    reason_line: str,
    officer_context: Optional[str] = None,
) -> str:
    """Render the user-facing escalation body with the worker text and any
    officer context clearly delimited (§7 — neither is a trusted instruction).
    """
    parts = [reason_line]
    if officer_context and officer_context.strip():
        parts.append(
            "**Officer context** (as written by the officer):\n\n"
            + "\n".join(f"> {line}" for line in officer_context.strip().splitlines())
        )
    # "verbatim" means unedited by us, not unsanitized: a worker (or a page it
    # summarized) can put a credential in here, and this body goes to email
    # (audit OC-05). Redaction is visible so the reader knows text was removed.
    worker_text = sanitize(original_body.strip())
    body_block = worker_text.text
    if worker_text.redacted:
        body_block += (
            f"\n\n_({worker_text.count} secret-shaped value(s) redacted "
            "from the worker's text.)_"
        )
    parts.append("---\n**Original worker message** (verbatim):\n\n" + body_block)
    parts.append(
        "---\nReply to this thread to answer the worker; your reply resumes "
        "it directly."
    )
    return "\n\n".join(parts)


async def _load_original_message(
    db: Any, route: Dict[str, Any]
) -> Dict[str, Optional[str]]:
    """The originating worker message for a route (subject/body), best-effort."""
    subject: Optional[str] = None
    body: Optional[str] = None
    message_id = route.get("originating_message_id")
    try:
        if message_id is not None:
            row = await db.get_message_log_entry(str(message_id))
            if row:
                subject = row.get("subject")
                body = row.get("message")
        if body is None:
            thread = await db.get_thread_messages(
                str(route["job_id"]), str(route["thread_id"])
            )
            for message in (thread or {}).get("messages", []):
                if message.get("direction") == "outbound":
                    subject = subject or (thread or {}).get("subject")
                    body = message.get("message")
                    break
    except Exception:
        logger.exception(
            "message routing: original-message lookup failed for route %s",
            str(route.get("route_id"))[:8],
        )
    return {"subject": subject or "(worker message)", "body": body or "(unavailable)"}


async def deliver_route_to_user(
    db: Any,
    route: Dict[str, Any],
    *,
    reason: str,
    officer_context: Optional[str] = None,
    notifier: Any = None,
) -> bool:
    """Deliver a route's thread to the job owner through the existing
    notification path and stamp ``user_delivery_at``.

    Pure delivery — no state CAS here; callers transition first (CAS is the
    durable fact, delivery is retried by the reconciler when the stamp is
    missing). Returns True when a dispatch was attempted and the stamp landed.
    Never raises.
    """
    try:
        if notifier is None:
            from orchestrator.services.notification_service import (
                notification_service as notifier,
            )

        job = await db.get_job(str(route["job_id"]))
        if not job or not job.get("user_id"):
            logger.warning(
                "message routing: cannot deliver route %s — job/user missing",
                str(route.get("route_id"))[:8],
            )
            return False
        user = await db.get_user(str(job["user_id"]))
        if not user or not user.get("email"):
            logger.warning(
                "message routing: job owner %s has no email for route %s",
                str(job.get("user_id"))[:8],
                str(route.get("route_id"))[:8],
            )
            return False

        generation = str(route.get("routing_generation") or route.get("route_id") or "")
        intent = await reserve_quota_intent(
            db,
            routing_generation=generation,
            route_id=str(route.get("route_id") or "") or None,
            bucket="human",
            effective_audience="officer_and_user",
            job_id=str(route["job_id"]),
            project_id=(str(route["project_id"]) if route.get("project_id") else None),
            user_id=str(job["user_id"]),
            reason=reason,
        )
        if not intent.get("allowed"):
            logger.warning(
                "message routing: human quota refused route %s (%s)",
                str(route.get("route_id"))[:8],
                intent.get("limit"),
            )
            return False
        # Provider acceptance is durable independently of the route stamp. If
        # a prior call settled acceptance and crashed before user_delivery_at,
        # repair the stamp without spending or notifying again.
        if intent.get("accepted_at") is not None:
            return bool(await db.mark_route_user_delivery(str(route["route_id"])))

        original = await _load_original_message(db, route)
        reason_lines = {
            "officer_escalated": (
                "**The project officer escalated this worker question to you.**"
            ),
            "officer_sla_expired": (
                "**A worker question waited on the project officer past the "
                "response SLA and was escalated to you automatically.**"
            ),
            "officer_hold": (
                "**The project officer was placed on hold while this worker "
                "question was pending — it now comes directly to you.**"
            ),
            "officer_decommissioned": (
                "**The project officer was decommissioned while this worker "
                "question was pending — it now comes directly to you.**"
            ),
            "officer_delivery_failed": (
                "**Delivery of this worker question to the project officer "
                "failed — it now comes directly to you.**"
            ),
            "delivery_repair": (
                "**Earlier delivery of this worker message did not go "
                "through — retrying.**"
            ),
        }
        reason_line = reason_lines.get(
            reason, "**A worker question was routed to you.**"
        )
        body = _delimited_user_body(
            str(original["subject"]),
            str(original["body"]),
            reason_line=reason_line,
            officer_context=officer_context,
        )
        # A repair re-sends the ORIGINAL delivery; only genuine escalations
        # wear the [Escalated] badge.
        clean_subject = sanitize(str(original["subject"])).text
        if reason == "delivery_repair":
            subject = clean_subject
        else:
            subject = f"[Escalated] {clean_subject}"

        attempt = await begin_delivery_attempt(db, intent)
        if not attempt.get("delivery_claimed"):
            if attempt.get("accepted"):
                return bool(await db.mark_route_user_delivery(str(route["route_id"])))
            # Another replica owns the bounded delivery attempt. Its failure
            # or timeout leaves the route unstamped for the reconciler.
            return False
        # Thread continuity: the escalation is an outbound row on the SAME
        # thread, so the cockpit shows it and an emailed reply (In-Reply-To
        # of this message id) routes back to the same thread → same resume.
        # Logged BEFORE the notification so the (possibly deferred) mail can
        # stamp its Message-ID onto this row.
        log_row = None
        try:
            # notification-ledger: the thread's outbound record, not a feed write
            log_row = await db.log_message(
                job_id=str(route["job_id"]),
                user_id=str(job["user_id"]),
                thread_id=str(route["thread_id"]),
                direction="outbound",
                recipient_email=user.get("email"),
                subject=subject,
                message=body,
                mode="async",
                status="pending",
                routing_generation=generation,
                effective_audience="officer_and_user",
            )
        except Exception:
            logger.warning(
                "message routing: escalation log_message failed (non-fatal)",
                exc_info=True,
            )
        try:
            # An escalation means the automated tier already failed the
            # user: `high`, so the mail goes now.
            recorded = await notifier.record_agent_message(
                user_id=str(job["user_id"]),
                job=job,
                job_id=str(route["job_id"]),
                thread_id=str(route["thread_id"]),
                sequence=None,
                subject=subject,
                message_md=body,
                severity="high",
                dedup_key=f"route_escalation:{route.get('route_id')}:{reason}",
                message_log_id=(str(log_row["id"]) if log_row else None),
                reason_line=reason,
            )
            dispatch = recorded.as_dispatch()
        except Exception:
            await settle_delivery_attempt(
                db,
                intent,
                attempt,
                accepted=False,
                failure_class="notifier_exception",
            )
            raise
        outcome = classify_dispatch(dispatch)
        if log_row:
            try:
                await db.settle_outbound_message_log(
                    str(log_row["id"]),
                    accepted=outcome.accepted,
                    error_message=outcome.detail,
                    email_message_id=dispatch.get("email_message_id"),
                )
            except Exception:
                logger.warning(
                    "message routing: escalation ledger settle failed (non-fatal)",
                    exc_info=True,
                )
        # OC-06: only an ACCEPTED dispatch may stamp delivery. The
        # reconciler retries exactly while this stamp is null, so stamping a
        # failure is what stops the retry loop and strands the user's thread.
        if not outcome.accepted:
            await settle_delivery_attempt(
                db,
                intent,
                attempt,
                accepted=False,
                failure_class="provider_rejected",
                detail=outcome.detail,
            )
            logger.warning(
                "message routing: delivery not accepted for route %s (%s) — "
                "leaving retryable",
                str(route.get("route_id"))[:8],
                outcome.detail,
            )
            return False
        await settle_delivery_attempt(
            db,
            intent,
            attempt,
            accepted=True,
            detail=outcome.detail,
        )
        stamped = await db.mark_route_user_delivery(str(route["route_id"]))
        return bool(stamped)
    except Exception:
        logger.exception(
            "message routing: user delivery failed for route %s",
            str(route.get("route_id"))[:8],
        )
        return False


async def escalate_route(
    db: Any,
    route: Dict[str, Any],
    *,
    reason: str,
    actor_kind: str,
    actor_id: Optional[str] = None,
    officer_thread_id: Optional[str] = None,
    officer_incarnation: Optional[int] = None,
    officer_context: Optional[str] = None,
    expected_states: Optional[tuple] = None,
    notifier: Any = None,
) -> Dict[str, Any]:
    """CAS a route to ``escalated_to_user`` and deliver the thread to the user.

    The CAS is the durable fact; delivery failure leaves
    ``user_delivery_at IS NULL`` for the reconciler's redelivery leg. Returns
    ``{"escalated": bool, "delivered": bool}``. Never raises.
    """
    try:
        officer_source = {}
        if officer_thread_id is not None:
            officer_source["officer_thread_id"] = officer_thread_id
        if officer_incarnation is not None:
            officer_source["officer_incarnation"] = officer_incarnation
        updated = await db.transition_message_route(
            str(route["route_id"]),
            to_state="escalated_to_user",
            expected_states=list(
                expected_states or ("pending_officer", "delivery_failed")
            ),
            actor_kind=actor_kind,
            actor_id=actor_id,
            note=reason,
            **officer_source,
        )
    except Exception:
        logger.exception(
            "message routing: escalation CAS failed for route %s",
            str(route.get("route_id"))[:8],
        )
        return {"escalated": False, "delivered": False}
    if not updated:
        return {"escalated": False, "delivered": False}
    delivered = await deliver_route_to_user(
        db,
        updated,
        reason=reason,
        officer_context=officer_context,
        notifier=notifier,
    )
    return {"escalated": True, "delivered": delivered}


async def drain_officer_blocking_routes(
    db: Any, project_id: str, *, reason: str, notifier: Any = None
) -> int:
    """Hand every pending officer-first blocking route to the user (§5.1).

    Called when the post enters hold (maintenance or conference) or is
    decommissioned: a worker already waiting on the officer must not keep
    waiting on someone who cannot answer. Async officer-first routes stay
    queued (hold) — only *blocking* ``pending_officer`` routes drain.
    Returns the number escalated. Never raises.
    """
    escalated = 0
    try:
        routes = await db.list_pending_officer_blocking_routes(str(project_id))
    except Exception:
        logger.exception(
            "message routing: drain listing failed for project %s",
            str(project_id)[:8],
        )
        return 0
    for route in routes or []:
        outcome = await escalate_route(
            db,
            route,
            reason=reason,
            actor_kind="system",
            actor_id=f"drain:{reason}",
            notifier=notifier,
        )
        if outcome["escalated"]:
            escalated += 1
    if escalated:
        logger.info(
            "message routing: drained %d pending officer route(s) to the user "
            "(project %s, %s)",
            escalated,
            str(project_id)[:8],
            reason,
        )
    return escalated


async def record_reply_resolution(
    db: Any,
    job_id: str,
    thread_id: str,
    *,
    actor_kind: str,
    actor_id: Optional[str] = None,
    note: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """CAS the newest open route on ``(job, thread)`` to its resolved state.

    ``actor_kind='user'`` → ``resolved_by_user``; ``actor_kind='officer'`` →
    ``resolved_by_officer``. A lost CAS (already resolved / timed out) is a
    no-op returning None — resolution races are decided by the route CAS and
    the job-status CAS, never twice. Never raises.
    """
    to_state = "resolved_by_officer" if actor_kind == "officer" else "resolved_by_user"
    try:
        route = await db.find_message_route_for_thread(
            str(job_id), str(thread_id), open_only=True
        )
        if not route:
            return None
        return await db.transition_message_route(
            str(route["route_id"]),
            to_state=to_state,
            expected_states=list(ROUTE_OPEN_STATES),
            actor_kind=actor_kind,
            actor_id=actor_id,
            note=note,
        )
    except Exception:
        logger.exception(
            "message routing: reply-resolution recording failed for job %s thread %s",
            str(job_id)[:8],
            thread_id,
        )
        return None


def job_is_terminal(job: Dict[str, Any]) -> bool:
    return str(job.get("status") or "") in _TERMINAL_JOB_STATUSES


async def close_routes_for_terminal_job(
    db: Any, job_id: str, terminal_status: str
) -> int:
    """Auto-close a just-terminalized job's still-open routes. Never raises.

    The ruling behind it: an open route outlives its job as a permanently
    "open" worker message in every sitrep and pending count, and no closure
    verb is safe (officer ack refuses blocking routes; a human reply risks
    resuming the dead job). So terminal transitions close the ledger
    themselves — stamped ``closed automatically: job <status>`` on the
    route's transitions audit, still visible in history. Nothing is
    delivered and nothing resumes; a repeat call is a no-op (the CAS
    matches zero rows). Direct-write terminal paths with no hook are swept
    by the reconciler's backstop leg one tick later.

    Returns the number of routes closed.
    """
    if terminal_status not in _TERMINAL_JOB_STATUSES:
        return 0
    try:
        closed = await db.close_message_routes_for_terminal_jobs(str(job_id))
    except Exception:
        logger.exception(
            "message routing: terminal auto-close failed for job %s (%s) — "
            "the reconciler backstop will retry",
            str(job_id)[:8],
            terminal_status,
        )
        return 0
    for route in closed:
        logger.info(
            "message routing: closed route %s (thread %s) — job %s reached %s",
            str(route.get("route_id"))[:8],
            route.get("thread_id"),
            str(job_id)[:8],
            terminal_status,
        )
    return len(closed)
