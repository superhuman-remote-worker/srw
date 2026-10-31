"""A live session's usage row; knowledge-base/knowledge/features/live_session_jobs.md §7.3.

When the connector's pod reports a session ended with its voice seconds,
one ``usage_events`` row is written: source ``live``, the session id as its
source id, the end time as its timestamp (deterministic, so a repeated
report collides on the ledger's dedupe key), billed to the job's project and
owner as an ``llm`` resource priced per ``live-voice-second``.

``usage_rates`` ships empty (an admin or the price sync fills it), and no
public price list serves live voice seconds. While no rate is configured for
the model, the row is priced by the model's catalog row
(``params.rate_usd_per_minute``: an operator's own rate, 0 for a local
engine such as a voice gateway profile, which the chart seeds), else by an
engine's published list price (:data:`DEFAULT_RATES`); a model with neither
stays unpriced. A configured rate always wins.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from orchestrator.services.usage_ledger import UsageEvent

logger = logging.getLogger(__name__)

SOURCE = "live"
UNIT = "live-voice-second"
#: List prices per voice second (OpenAI: gpt-live-1 is $0.05 per minute,
#: billed per second).
DEFAULT_RATES: dict[str, Decimal] = {"gpt-live-1": Decimal("0.05") / Decimal(60)}
#: A ``live_voice`` catalog row's own price, in ``params`` (USD per minute).
RATE_PARAM = "rate_usd_per_minute"


def catalog_rate(row: Mapping[str, Any] | None) -> Decimal | None:
    """The per-second rate a ``live_voice`` catalog row names, or ``None``."""
    params = (row or {}).get("params_json")
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except ValueError:
            return None
    value = params.get(RATE_PARAM) if isinstance(params, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        per_minute = Decimal(str(value))
    except InvalidOperation:
        return None
    if not per_minute.is_finite() or per_minute < 0:
        return None
    return per_minute / Decimal(60)


async def default_rate(db: Any, model: str) -> Decimal | None:
    """What prices a session of ``model`` while no rate is configured: its
    catalog row's rate, else :data:`DEFAULT_RATES`."""
    try:
        row = await db.resolve_catalog_model(model, capability="live_voice")
    except Exception:
        logger.warning("Live usage: the catalog row of %s was not read", model)
        row = None
    rate = catalog_rate(row)
    return rate if rate is not None else DEFAULT_RATES.get(model)


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
    default = await default_rate(db, event.resource)
    if default is not None:
        try:
            configured = await ledger.resolve_rate(
                event.category, event.resource, event.unit, event.ts
            )
        except Exception:
            configured = None
        if configured is None:
            event.rate_usd = default
            event.cost_usd = Decimal(str(event.quantity)) * default
    try:
        return await ledger.record_events([event])
    except Exception:
        logger.warning(
            "Live usage row of session %s not written (non-fatal)",
            session["id"],
            exc_info=True,
        )
        return 0
