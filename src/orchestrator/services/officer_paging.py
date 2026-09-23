"""Officer → Legate paging, and the Officer's own durable sleep timer.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; centurion.md §4/§6).

* :func:`agent_file_officer_wake` — the sleep tool's park path. The timer is a
  Postgres ``session_wake_events`` row, so pod or node death never loses the
  schedule, and the requested minutes are **clamped to the thread's own officer
  bounds here**: the tool's value is a request, not an order.
* :func:`dispatch_officer_page` — one feed row on the thread owner's
  notification centre, shared by the notify endpoint's page/digest urgencies,
  the recycler's respawn-failure alert and the runtime-authorization incident.
  ``severity`` is the officer's urgency, never a channel selector (D1).
  The session deep link is appended as a **labelled bare URL**, not markdown:
  the email leg renders markdown but ntfy/Slack get raw text.
* :func:`agent_officer_notify` — the three urgencies. ``log`` is a deliberate
  server-side no-op so the tool has an honest cheap tier; throttling is the
  platform's job (dedup per text per day, preferences, quiet hours), never a
  per-officer page budget.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, Request

from orchestrator.schemas.officer_post import OfficerNotifyRequest, OfficerWakeRequest
from orchestrator.services.officer_metadata import (
    officer_meta_enabled,
    thread_officer_meta,
)
from orchestrator.services.session_wake import file_officer_timer
from shared.content_redaction import sanitize_text

logger = logging.getLogger(__name__)


@dataclass
class OfficerPagingDependencies:
    """Collaborators for one page or timer filing, resolved per invocation."""

    store: Any
    notifier: Any


async def agent_file_officer_wake(
    request: Request,
    thread_id: str,
    body: OfficerWakeRequest,
    *,
    dependencies: OfficerPagingDependencies,
) -> dict[str, Any]:
    """File an officer session's durable sleep timer. **Internal** — requires
    ``X-Internal-Key``; ingress strips this path.

    Called by the sleep tool's park path (centurion.md §4, decision
    2026-07-29): the timer is a Postgres ``session_wake_events`` row
    (source='timer'), so pod or node death never loses the schedule — the
    drain fires it when due. Minutes are clamped to the thread's officer
    bounds HERE; the tool's value is a request, not an order.
    """
    thread = await dependencies.store.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    officer_meta = thread_officer_meta(thread)
    if not officer_meta_enabled(officer_meta):
        raise HTTPException(status_code=409, detail="Thread is not an officer session")
    try:
        sleep_min = int(officer_meta.get("sleep_min_minutes") or 5)
        sleep_max = int(officer_meta.get("sleep_max_minutes") or 60)
    except (TypeError, ValueError):
        sleep_min, sleep_max = 5, 60
    minutes = max(sleep_min, min(int(body.minutes), max(sleep_min, sleep_max)))
    filed = await file_officer_timer(
        dependencies.store, thread_id, minutes, body.reason
    )
    return {"filed": filed, "minutes": minutes}


def officer_session_link(thread_id: str) -> str | None:
    """Absolute cockpit deep link to an officer session, or None when the
    cockpit base URL is unknown.

    Reads ``COCKPIT_EXTERNAL_URL`` (chart configmap, from ``srw.cockpitUrl``
    — the same var the email/notification services use for their deep links)
    at call time. Unset → None: a page without a link beats a page with a
    broken one.
    """
    base = os.getenv("COCKPIT_EXTERNAL_URL", "").strip().rstrip("/")
    if not base:
        return None
    return f"{base}/sessions/{thread_id}"


async def dispatch_officer_page(
    thread: dict,
    thread_id: str,
    subject: str,
    message_md: str,
    *,
    category: str = "officer_question",
    severity: str = "high",
    dedup_key: str | None = None,
    dependencies: OfficerPagingDependencies,
) -> str | None:
    """Record an officer → Legate notification on the thread owner's feed.

    Shared by the notify endpoint's page/digest urgencies, the recycler's
    respawn-failure alert and the runtime-authorization incident. Delivery
    (email per the owner's preferences, later the escalation ladder) is the
    notification system's business, not this function's: ``severity`` is the
    officer's urgency, never a channel selector (unified notification system,
    D1). A ``high`` row mails now; ``low`` is in-app only.

    Appends a deep link to the officer's session so every page carries a way
    back. Deliberately a labeled bare URL, not a markdown ``[label](url)``:
    the email leg renders markdown (services/email_markdown.py, which also
    auto-links a bare URL) but ntfy/Slack get the raw text — a bare URL is
    clickable-or-copyable in every leg, brackets-and-parens only in email.

    Returns the notification id when the row was recorded (new or replayed);
    ``None`` only when there is nobody to notify or the feed write itself
    failed.
    """
    user_id = thread.get("user_id")
    if not user_id:
        return None
    subject = subject or "Your centurion needs you"
    session_link = officer_session_link(thread_id)
    # The officer writes this from what it read, worker text included (audit
    # OC-05). Redacted here, before the server's own session link is added;
    # the dedup digest below keeps hashing the text as written.
    raw_subject, raw_message = subject, message_md
    subject = sanitize_text(subject)
    message_md = sanitize_text(message_md)
    page_body = message_md
    if session_link:
        page_body = f"{message_md}\n\nOpen his log to reply: {session_link}"
    if not dedup_key:
        # Identical text on one day collapses onto one row — the anti-spam
        # role the per-day page budget used to play.
        text_digest = hashlib.sha1(
            f"{raw_subject}\n{raw_message}".encode("utf-8")
        ).hexdigest()[:16]
        today = datetime.now(timezone.utc).date().isoformat()
        dedup_key = f"officer_notify:{thread_id}:{text_digest}:{today}"
    project_id = thread.get("project_id")
    try:
        result = await dependencies.notifier.record(
            recipient_id=str(user_id),
            category=category,
            severity=severity,
            dedup_key=dedup_key,
            subject=subject,
            body=page_body,
            source_kind="thread",
            source_id=str(thread_id),
            action_params={
                "thread_id": str(thread_id),
                "project_id": str(project_id) if project_id else None,
            },
            payload={
                "thread_id": str(thread_id),
                "project_id": str(project_id) if project_id else None,
                "config_name": str(thread.get("config_name") or "session_base"),
                "title": sanitize_text(thread.get("title")) or None,
            },
        )
    except Exception:
        logger.warning(
            "officer notification for thread %s failed",
            str(thread_id)[:8],
            exc_info=True,
        )
        return None
    return result.notification_id


async def agent_officer_notify(
    request: Request,
    thread_id: str,
    body: OfficerNotifyRequest,
    *,
    dependencies: OfficerPagingDependencies,
) -> dict[str, Any]:
    """The officer's notify_user contract (centurion.md §6). **Internal** —
    requires ``X-Internal-Key``; ingress strips this path.

    Three urgencies, each a feed row on the Legate's notification center
    (unified notification system):
      * ``log`` — no-op server-side: the officer's transcript already carries
        the line; this exists so the tool has an honest cheap tier.
      * ``digest`` — a ``low``-severity row: in-app only, read at the next
        look. The officer card lists these rows (feed filtered by source).
      * ``page`` — a ``high``-severity row: reaches the Legate now, through
        whatever channels their preferences allow. There is no per-officer
        page budget — the platform throttles (dedup per text per day,
        preferences, quiet hours), not the agent.
    """
    thread = await dependencies.store.get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    officer_meta = thread_officer_meta(thread)
    if not officer_meta_enabled(officer_meta):
        raise HTTPException(status_code=409, detail="Thread is not an officer session")

    urgency = (body.urgency or "log").strip().lower()
    if urgency not in ("log", "digest", "page"):
        raise HTTPException(
            status_code=400, detail="urgency must be log, digest, or page"
        )
    message = (body.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message must not be empty")

    if urgency == "log":
        return {"delivered": "log"}

    # A page is a `high` row (reaches the Legate now); a digest is a `low`
    # row (in-app, read at the next look). Throttling is the platform's job:
    # identical text on one day collapses onto one row (the dedup key), the
    # recipient's preferences and quiet hours apply per channel, and there is
    # no per-officer page budget any more.
    severity = "high" if urgency == "page" else "low"
    notification_id = await dispatch_officer_page(
        thread,
        thread_id,
        body.subject,
        message,
        category="officer_question",
        severity=severity,
        dependencies=dependencies,
    )
    if notification_id is None:
        raise HTTPException(
            status_code=503,
            detail="The notification could not be recorded — try again",
        )
    return {"delivered": urgency, "notification_id": notification_id}


__all__ = [
    "OfficerPagingDependencies",
    "agent_file_officer_wake",
    "agent_officer_notify",
    "dispatch_officer_page",
    "officer_session_link",
]
