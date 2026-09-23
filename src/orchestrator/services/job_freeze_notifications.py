"""The operator-facing notification a frozen job produces, and its settlement.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane M, census group
``J_messaging``). Three responsibilities, each already separate in the source:

* :func:`format_freeze_notification` — pure subject/body rendering per freeze
  type, with the unlisted-type fallback intact.
* :data:`FREEZE_CATEGORY` — freeze type → feed category. Anything unlisted is
  an ``incident``: it reached a human because something went wrong, not because
  a decision is queued.
* :func:`notify_operator_freeze` — the one feed write. ``dedup_key`` is the
  caller's idempotency key, so a journal replay lands on the same row and sends
  nothing twice. Delivery is the notification system's business.
* :func:`resolve_job_notifications` — the job left its frozen/pending state, so
  every feed row about it settles (D6), whoever it belongs to.

``notifier`` arrives on the dependency object because suites rebind
``notification_service`` on ``orchestrator.main``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from orchestrator.services.notification_service import RecordResult
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


@dataclass
class JobFreezeNotificationDependencies:
    """The notification authority, resolved per invocation."""

    notifier: Any


_DELIVERY_HOLD_CHARS = 600


def delivery_hold_reason(freeze_data: dict[str, Any] | None) -> str | None:
    """Why this completion's delivery is unproven, or None.

    ``delivery_hold`` is the deliverable gate's verdict (set by the completion
    authority); ``delivery_error`` is the agent's own report of a failed
    job-ending commit or push. Either one means the branch may be stale.
    """
    fd = freeze_data or {}
    reason = fd.get("delivery_hold")
    if not reason and fd.get("delivery_failed"):
        reason = fd.get("delivery_error") or "the job-ending push failed"
    if not isinstance(reason, str) or not reason.strip():
        return None
    reason = " ".join(reason.split())
    if len(reason) > _DELIVERY_HOLD_CHARS:
        reason = reason[: _DELIVERY_HOLD_CHARS - 1] + "…"
    return reason


def format_freeze_notification(
    freeze_type: str,
    freeze_data: dict[str, Any],
    job_id: str,
    config_name: str,
    description: str,
) -> tuple[str, str]:
    """Format notification subject and body for a freeze event."""
    short_id = job_id[:8]

    if freeze_type == "vm_upgrade_required":
        command = freeze_data.get("command", "unknown")
        subject = f"Job {short_id} needs VM upgrade (sudo detected)"
        message_md = (
            f"**Job `{short_id}`** (`{config_name}`) attempted a sudo command "
            f"and needs approval to continue.\n\n"
            f"**Command:** `{command}`\n\n"
            f"**Description:** {description}\n\n"
            f"Approve a VM upgrade or reject to keep the job paused."
        )

    elif freeze_type == "job_complete":
        summary = freeze_data.get("summary", "No summary provided")
        confidence = freeze_data.get("confidence", 0)
        deliverables = freeze_data.get("deliverables", [])
        confidence_str = (
            f"{confidence:.0%}"
            if isinstance(confidence, (int, float))
            else str(confidence)
        )
        deliverables_str = (
            "\n".join(f"- `{d}`" for d in deliverables)
            if deliverables
            else "*(none listed)*"
        )
        subject = f"Job {short_id} completed — review required"
        message_md = (
            f"**Job `{short_id}`** (`{config_name}`) has completed and is awaiting review.\n\n"
            f"**Summary:** {summary}\n\n"
            f"**Confidence:** {confidence_str}\n\n"
            f"**Deliverables:**\n{deliverables_str}"
        )
        hold = delivery_hold_reason(freeze_data)
        if hold:
            # The seal was held because the repository cannot be shown to
            # hold the work — the reviewer must know that before reading the
            # branch, which may be a stale revision.
            subject = f"Job {short_id} held — delivery unproven"
            message_md = (
                f"**Job `{short_id}`** (`{config_name}`) finished, but its "
                f"deliverables could not be shown to have reached the job "
                f"repository, so it was held for review instead of sealed.\n\n"
                f"**Delivery:** {hold}\n\n"
                f"The workspace may hold the only copy of the latest work. "
                f"Recover it before approving what the branch shows.\n\n"
                f"**Summary:** {summary}\n\n"
                f"**Deliverables:**\n{deliverables_str}"
            )

    elif freeze_type == "budget_exceeded":
        phase_number = freeze_data.get("phase_number", "?")
        reason = freeze_data.get("reason", "Tool call budget exceeded")
        tool_calls = freeze_data.get("tool_calls_this_phase", "?")
        subject = f"Job {short_id} frozen — budget exceeded (phase {phase_number})"
        message_md = (
            f"**Job `{short_id}`** (`{config_name}`) has been frozen because "
            f"the tool call budget was exceeded.\n\n"
            f"**Phase:** #{phase_number}\n"
            f"**Tool calls this phase:** {tool_calls}\n"
            f"**Reason:** {reason}\n\n"
            f"**Description:** {description}"
        )

    elif freeze_type == "llm_unavailable":
        classification = freeze_data.get("classification", "unknown")
        model = freeze_data.get("model", "?")
        summary = freeze_data.get("error_summary") or "LLM endpoint unavailable"
        attempt = freeze_data.get("attempt", "?")
        subject = f"Job {short_id} FAILED — LLM endpoint unavailable (gave up)"
        message_md = (
            f"**Job `{short_id}`** (`{config_name}`) was paused and retried on a "
            f"backoff while the LLM endpoint was unavailable, but hit the give-up "
            f"ceiling and has **failed**.\n\n"
            f"**Model:** `{model}`\n"
            f"**Classification:** `{classification}`\n"
            f"**Attempts:** {attempt}\n"
            f"**Last error:** {str(summary)[:300]}\n\n"
            f"**Description:** {description}\n\n"
            f"Check the model endpoint/provider (Admin → Models), then re-run."
        )

    else:
        subject = f"Job {short_id} frozen — {freeze_type}"
        message_md = (
            f"**Job `{short_id}`** (`{config_name}`) has frozen with type "
            f"`{freeze_type}` and requires attention.\n\n"
            f"**Description:** {description}"
        )

    return subject, message_md


# freeze_type → feed category. Anything unlisted is an incident: it reached a
# human because something went wrong, not because a decision is queued.
FREEZE_CATEGORY = {
    "job_complete": "review_queue",
    "vm_upgrade_required": "vm_upgrade",
    "budget_exceeded": "budget_exceeded",
    "llm_unavailable": "incident",
}


async def resolve_job_notifications(
    job_id: str,
    *,
    user: dict[str, Any] | None,
    hook: str,
    dependencies: JobFreezeNotificationDependencies,
) -> None:
    """The job left its frozen/pending state — settle every feed row about it,
    whoever it belongs to (unified notification system, D6). Best-effort."""
    resolved_by = f"user:{user['id']}" if user and user.get("id") else f"system:{hook}"
    await dependencies.notifier.resolve_source(
        "job", str(job_id), resolved_by=resolved_by
    )


async def notify_operator_freeze(
    job: dict[str, Any],
    job_id: str,
    freeze_type: str,
    freeze_data: dict[str, Any],
    sudo_request_id: str | None = None,
    *,
    dedup_key: str,
    dependencies: JobFreezeNotificationDependencies,
) -> RecordResult | None:
    """Record the operator-facing notification for a freeze event.

    ``dedup_key`` is the caller's idempotency key — inside a completion effect
    that is the command id, so a journal replay lands on the same feed row and
    sends nothing twice. Delivery (email, webhooks, later the escalation
    ladder) is the notification system's business, not this function's.
    """
    user_id = str(job["user_id"]) if job.get("user_id") else None
    if not user_id:
        logger.debug(f"Job {job_id} has no user_id — skipping freeze notification")
        return None

    config_name = canonical_config_name(job.get("config_name") or "worker_base")
    description = (job.get("description") or "")[:100]
    subject, message_md = format_freeze_notification(
        freeze_type=freeze_type,
        freeze_data=freeze_data,
        job_id=job_id,
        config_name=config_name,
        description=description,
    )

    category = FREEZE_CATEGORY.get(freeze_type, "incident")
    if category == "vm_upgrade" and sudo_request_id:
        source_kind, source_id = "sudo_request", str(sudo_request_id)
    else:
        source_kind, source_id = "job", str(job_id)
    action_params: dict[str, Any] = {"job_id": str(job_id)}
    if sudo_request_id:
        action_params["request_id"] = str(sudo_request_id)

    result = await dependencies.notifier.record(
        recipient_id=user_id,
        category=category,
        dedup_key=dedup_key,
        subject=subject,
        body=message_md,
        source_kind=source_kind,
        source_id=source_id,
        action_params=action_params,
        payload={
            "job_id": str(job_id),
            "config_name": config_name,
            "job_description": description,
            "freeze_type": freeze_type,
            "phase_number": (freeze_data or {}).get("phase_number"),
            "sudo_request_id": str(sudo_request_id) if sudo_request_id else None,
            "delivery_hold": delivery_hold_reason(freeze_data),
        },
    )
    logger.info(
        "Freeze notification %s for job %s (%s → %s)",
        "recorded" if result.inserted else "replayed",
        job_id,
        freeze_type,
        category,
    )
    return result


__all__ = [
    "FREEZE_CATEGORY",
    "JobFreezeNotificationDependencies",
    "delivery_hold_reason",
    "format_freeze_notification",
    "notify_operator_freeze",
    "resolve_job_notifications",
]
