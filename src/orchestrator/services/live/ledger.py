"""A live session's usage row; knowledge-base/knowledge/features/live_session_jobs.md §7.3.

When the connector's pod reports a session ended with its voice seconds,
one ``usage_events`` row is written: source ``live``, the session id as its
source id, the end time as its timestamp (deterministic, so a repeated
report collides on the ledger's dedupe key), billed to the job's project and
owner as an ``llm`` resource priced per ``live-voice-second``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from orchestrator.services.usage_ledger import UsageEvent

logger = logging.getLogger(__name__)

SOURCE = "live"
UNIT = "live-voice-second"


def usage_event(
    session: dict[str, Any], job: dict[str, Any] | None
) -> UsageEvent | None:
    """The row an ended session bills, or ``None`` without seconds."""
    seconds = session.get("usage_seconds")
    if seconds is None or float(seconds) <= 0:
        return None
    ended_at = session.get("ended_at") or datetime.now(timezone.utc)
    return UsageEvent(
        category="llm",
        resource=str(session.get("model") or "live-voice"),
        quantity=seconds,
        unit=UNIT,
        source=SOURCE,
        source_id=str(session["id"]),
        ts=ended_at,
        user_id=str(job["user_id"]) if job and job.get("user_id") else None,
        project_id=(
            str(job["project_id"])
            if job and job.get("project_id")
            else (str(session["project_id"]) if session.get("project_id") else None)
        ),
        ref_kind="job" if session.get("job_id") else None,
        ref_id=str(session["job_id"]) if session.get("job_id") else None,
        details={"connector_id": str(session.get("connector_id") or "")},
    )


async def record_session_usage(ledger: Any, db: Any, session: dict[str, Any]) -> int:
    """Write the session's usage row; non-fatal (the session record keeps
    the seconds either way)."""
    if ledger is None or not getattr(ledger, "is_available", False):
        return 0
    job = None
    if session.get("job_id"):
        try:
            job = await db.get_job(str(session["job_id"]))
        except Exception:
            logger.warning("Live usage: job of session %s not read", session["id"])
    event = usage_event(session, job)
    if event is None:
        return 0
    try:
        return await ledger.record_events([event])
    except Exception:
        logger.warning(
            "Live usage row of session %s not written (non-fatal)",
            session["id"],
            exc_info=True,
        )
        return 0
