"""Usage and workforce reporting reads over the metering ledger.

Every operation here answers "what did this cost / how is the fleet doing"
from already-materialized rows. Nothing writes: the ledger is stamped by
``services/audit_usage.py`` and ``services/workspace_metering.py``, sealed by
the infrastructure-metering package, and only read back here.

Three shapes recur and are deliberately not collapsed into one:

* **The window.** ``usage_events`` is partitioned on ``ts``, so a read without
  a range is a confident zero rather than an error. Per-job reads derive their
  window from the job; the aggregate reads take it from the caller.
* **Unpriced is not free.** ``cost.usd`` stays ``None`` until some row carries
  a price, and ``cost.complete`` says whether every metered event had a rate.
* **Availability is not emptiness.** ``available=False`` / ``state`` values
  distinguish "the audit tier is off", "this predates the ledger anchor", and
  "this really spent nothing".

Collaborators arrive through :class:`UsageReportingDependencies`, resolved per
request by the application, because every metering singleton is assigned during
startup and is ``None`` at import.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services import job_queries
from orchestrator.services.infrastructure_metering import UsageSummaryV2
from orchestrator.services.infrastructure_metering.queries import (
    visibility_from_kwargs,
)
from orchestrator.services.infrastructure_metering.read_model import (
    UsageReadCutoverInactive,
)
from orchestrator.services.job_liveness import RUNNING_JOB_STATUSES
from orchestrator.services.usage_ledger import cache_hit_ratio_from_rows

JOB_USAGE_WINDOW_SLACK = timedelta(minutes=5)

# The materializer's poll interval (120s, llm_usage_poll_loop) plus its aging
# window (60s). A running job's figure is at worst this far behind the truth.
JOB_USAGE_LAG_SECONDS = 180

# usage_events.unit -> the token bucket it belongs to. Reasoning tokens are a
# subset of the completion total and ride in `details`, so they are deliberately
# absent here — counting them would double-count (see audit_usage.py).
JOB_USAGE_TOKEN_UNITS = {
    "prompt-token": "prompt_tokens",
    "cached-prompt-token": "cached_prompt_tokens",
    "completion-token": "completion_tokens",
}

USAGE_BREAKDOWN_GROUPS = ("user", "model", "project")


class UsageLabelStore(Protocol):
    """The app-DB reads that turn ledger keys into display labels."""

    async def fetch(self, query: str, *args: Any) -> Sequence[Any]: ...


@dataclass(frozen=True)
class UsageReportingDependencies:
    """Per-request collaborators; every metering singleton may still be ``None``.

    The application resolves this fresh on each invocation. Capturing any field
    at import time binds the pre-``lifespan`` ``None`` forever, which reads as
    "metering is off" rather than as the wiring bug it is.
    """

    store: Any
    audit_reader: Any
    logger: Any
    usage_ledger: Any | None = None
    usage_rollup: Any | None = None
    usage_cloud_estimator: Any | None = None
    infrastructure_usage_v2: Any | None = None
    infrastructure_usage_rollup: Any | None = None
    visible_project_ids: Callable[[dict[str, Any]], Awaitable[Any]] | None = None
    scope_project_id: Callable[[dict[str, Any]], Any] | None = None


# ---------------------------------------------------------------------------
# Per-job usage — pure folding over ledger rows
# ---------------------------------------------------------------------------


def job_usage_window(
    created_at: Any, now: datetime, floor: Optional[datetime]
) -> tuple[datetime, datetime]:
    """The [from, to) range to read a job's usage over.

    ``usage_events`` is partitioned on ``ts``, so a per-job read must carry a
    range — and a wrong range is indistinguishable from "this job was free".
    The tail is *now*, never ``completed_at``: workspace-pod intervals close at
    teardown and async auxiliary LLM calls land after the job seals, so a window
    ending at completion drops real spend. Slack at both ends absorbs clock skew
    between the orchestrator that stamps ``jobs.created_at`` and the audit
    writers that stamp ``ts``.
    """
    to_ts = now + JOB_USAGE_WINDOW_SLACK
    if isinstance(created_at, datetime):
        return created_at - JOB_USAGE_WINDOW_SLACK, to_ts
    # jobs.created_at defaults to CURRENT_TIMESTAMP so this is unreachable in
    # practice — widen to the whole ledger rather than invent a window and risk
    # the confident zero this endpoint exists to avoid.
    return floor or (now - timedelta(days=365)), to_ts


def job_usage_window_payload(from_ts: datetime, to_ts: datetime) -> dict[str, str]:
    """Serialize a window as Zulu.

    ``+00:00`` survives JSON but not a round trip through a query string, where
    the ``+`` decodes as a space and comes back a 422 — the jobs list shipped
    exactly that bug in its ``as_of`` watermark.
    """
    return {
        "from": from_ts.isoformat().replace("+00:00", "Z"),
        "to": to_ts.isoformat().replace("+00:00", "Z"),
    }


def job_usage_state(
    rows: list[dict[str, Any]], created_at: Any, floor: Optional[datetime]
) -> str:
    """Which kind of "nothing" an empty result is.

    A job created before the materializer's forward-only anchor has no rows and
    never will, which is not the same claim as "this job spent nothing" — and
    rendering both as $0.00 is how a cost feature loses its credibility. With no
    floor (an empty ledger) there is nothing to compare against, so the honest
    answer stays ``no_usage``.
    """
    if rows:
        return "measured"
    if floor is not None and isinstance(created_at, datetime) and created_at < floor:
        return "predates_ledger"
    return "no_usage"


def fold_job_usage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Fold ledger rows into totals that admit what they do not know.

    ``cost_usd`` stays None until some row carries a price, so a job whose models
    have no rate card reports "unknown", not "$0.00". ``complete`` is the flag a
    UI needs to decide between rendering the number plainly and qualifying it as
    a floor: a partially priced job has a real cost strictly above what is shown.
    """
    tokens = {v: 0 for v in JOB_USAGE_TOKEN_UNITS.values()}
    by_category: dict[str, dict[str, Any]] = {}
    cost: float | None = None
    priced_events = 0
    events = 0
    for row in rows:
        events += row["events"]
        priced_events += row["priced_events"]
        if row["cost_usd"] is not None:
            cost = (cost or 0.0) + row["cost_usd"]
        if row["category"] == "llm":
            bucket = JOB_USAGE_TOKEN_UNITS.get(row["unit"])
            if bucket:
                tokens[bucket] += int(row["quantity"])
        agg = by_category.setdefault(
            row["category"],
            {
                "category": row["category"],
                "cost_usd": None,
                "events": 0,
                "priced_events": 0,
            },
        )
        agg["events"] += row["events"]
        agg["priced_events"] += row["priced_events"]
        if row["cost_usd"] is not None:
            agg["cost_usd"] = (agg["cost_usd"] or 0.0) + row["cost_usd"]
    return {
        "llm": {
            **tokens,
            "total_tokens": sum(tokens.values()),
            "cache_hit_ratio": cache_hit_ratio_from_rows(rows),
        },
        "by_category": sorted(by_category.values(), key=lambda r: r["category"]),
        "cost": {
            "usd": cost,
            "complete": priced_events == events,
            "priced_events": priced_events,
            "events": events,
        },
    }


async def job_usage(
    *,
    job_id: str,
    job: dict[str, Any],
    include_subjobs: bool,
    dependencies: UsageReportingDependencies,
) -> dict[str, Any]:
    """Metered tokens, machine time and price for one already-authorized job."""
    ledger = dependencies.usage_ledger
    now = datetime.now(timezone.utc)
    scope = "subtree" if include_subjobs else "job"
    status = str(job.get("status") or "")
    freshness = {
        "as_of": now.isoformat().replace("+00:00", "Z"),
        # Only a job the system is still working adds usage. pending_review,
        # paused and waiting_for_reply are not terminal, but nothing runs.
        "live": status in RUNNING_JOB_STATUSES,
        "lag_seconds": JOB_USAGE_LAG_SECONDS,
    }

    created_at = job.get("created_at")

    if ledger is None or not ledger.is_available:
        # The window is derived from the job, not from the ledger, so it is still
        # answerable with metering off — and returning it keeps every response the
        # same shape. A null here reads as "no window" and invites the caller to
        # index into it anyway; this endpoint's own live check did exactly that.
        from_ts, to_ts = job_usage_window(created_at, now, None)
        return {
            "job_id": job_id,
            "scope": scope,
            "job_count": 1,
            "state": "unavailable",
            "window": job_usage_window_payload(from_ts, to_ts),
            "freshness": freshness,
            "rows": [],
            **fold_job_usage([]),
        }

    ref_ids = [job_id]
    if include_subjobs:
        ref_ids.extend(await dependencies.store.get_job_descendant_ids(job_id))

    floor = await ledger.earliest_event_ts()
    from_ts, to_ts = job_usage_window(created_at, now, floor)

    try:
        rows = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=ref_ids, from_ts=from_ts, to_ts=to_ts
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {
        "job_id": job_id,
        "scope": scope,
        "job_count": len(ref_ids),
        "state": job_usage_state(rows, created_at, floor),
        "window": job_usage_window_payload(from_ts, to_ts),
        "freshness": freshness,
        "rows": [
            {
                "category": r["category"],
                "resource": r["resource"],
                "unit": r["unit"],
                "quantity": r["quantity"],
                "cost_usd": r["cost_usd"],
                "events": r["events"],
                "priced_events": r["priced_events"],
            }
            for r in rows
        ],
        **fold_job_usage(rows),
    }


# ---------------------------------------------------------------------------
# Fleet and job statistics
# ---------------------------------------------------------------------------


async def visibility_kwargs_for_stats(
    user: dict[str, Any], *, dependencies: UsageReportingDependencies
) -> dict[str, Any]:
    """Shared visibility policy for the non-job statistics routes."""
    visible_project_ids = dependencies.visible_project_ids
    scope_project_id = dependencies.scope_project_id
    assert visible_project_ids is not None and scope_project_id is not None
    return await job_queries.visibility_kwargs_for_stats(
        user,
        visible_project_ids=visible_project_ids,
        scope_project_id=scope_project_id,
    )


async def session_wake_statistics(
    *, dependencies: UsageReportingDependencies
) -> dict[str, int]:
    """Health of the session-wake outbox."""
    try:
        return await dependencies.store.get_job_wake_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def daily_statistics(
    *, user: dict[str, Any], days: int, dependencies: UsageReportingDependencies
) -> list[dict[str, Any]]:
    """Daily job statistics scoped to the caller's visibility (G5)."""
    vis = await visibility_kwargs_for_stats(user, dependencies=dependencies)
    try:
        return await dependencies.store.get_daily_statistics(days=days, **vis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def agent_statistics(
    *, dependencies: UsageReportingDependencies
) -> dict[str, Any]:
    """Agent workforce summary counted by status."""
    try:
        agents = await dependencies.store.list_agents(limit=500)

        # Count by status
        status_counts = {
            "total": len(agents),
            "booting": 0,
            "ready": 0,
            "working": 0,
            "completed": 0,
            "failed": 0,
            "offline": 0,
        }

        for agent in agents:
            status = agent.get("status", "unknown")
            if status in status_counts:
                status_counts[status] += 1

        return status_counts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def stuck_jobs(
    *,
    user: dict[str, Any],
    threshold_minutes: int | None,
    dependencies: UsageReportingDependencies,
) -> dict[str, Any]:
    """Jobs suspected stuck, scoped to the caller's visibility (G5).

    Stuckness comes from the one shared liveness computation — audit movement
    first, then agent heartbeat — never from ``jobs.updated_at``.
    """
    vis = await visibility_kwargs_for_stats(user, dependencies=dependencies)
    from orchestrator.services.job_liveness import (
        compute_jobs_liveness,
        get_liveness_policy,
    )

    try:
        policy = get_liveness_policy(stall_override_minutes=threshold_minutes)
        processing = await dependencies.store.get_processing_jobs(**vis)
        liveness_by_id = await compute_jobs_liveness(
            processing,
            audit_reader=dependencies.audit_reader,
            db=dependencies.store,
            policy=policy,
        )
        stuck: list[dict[str, Any]] = []
        for job in processing:
            liveness = liveness_by_id.get(str(job["id"]))
            if not liveness or liveness["state"] not in (
                "suspected_stuck",
                "unavailable",
            ):
                continue
            row = dict(job)
            row.update(liveness)
            # Legacy shape kept for existing consumers.
            row["stuck_reason"] = "; ".join(liveness.get("reasons") or []) or (
                "No recent activity"
            )
            row["stuck_component"] = "liveness"
            stuck.append(row)
        return {
            "jobs": stuck,
            **policy.stall.as_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# Aggregate usage reads
# ---------------------------------------------------------------------------


def parse_utc_date(s: str) -> datetime:
    """Parse an ISO date/datetime as tz-aware UTC (naive → assumed UTC)."""
    d = datetime.fromisoformat(s)
    return (
        d.replace(tzinfo=timezone.utc)
        if d.tzinfo is None
        else d.astimezone(timezone.utc)
    )


def fold_breakdown(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Fold flat (key, unit) aggregate rows into one object per key.

    Each key carries a ``units`` map plus key-level ``events``/``cost_usd`` totals.
    Order-preserving on first appearance of a key.
    """
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        k = r["key"]
        o = out.setdefault(k, {"key": k, "units": {}, "events": 0, "cost_usd": 0.0})
        o["units"][r["unit"]] = {
            "quantity": r["quantity"],
            "cost_usd": r["cost_usd"],
            "events": r["events"],
        }
        o["events"] += r["events"]
        o["cost_usd"] += r["cost_usd"]
    for o in out.values():
        unit_rows = [
            {"category": "llm", "unit": unit, "quantity": agg["quantity"]}
            for unit, agg in o["units"].items()
        ]
        o["cache_hit_ratio"] = cache_hit_ratio_from_rows(unit_rows)
    return out


def merge_labels(
    folded: dict[str, dict[str, Any]], labels: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Attach a display label (+ optional is_admin) to each folded key.

    Unknown keys (deleted/aged-out referents — the ledger has no FKs) fall back to
    the raw key as the label. Sorted by total events desc (the leaderboard order).
    """
    out: list[dict[str, Any]] = []
    for k, o in folded.items():
        meta = labels.get(k, {})
        out.append(
            {**o, "label": meta.get("label", k), "is_admin": meta.get("is_admin")}
        )
    out.sort(key=lambda r: r["events"], reverse=True)
    return out


async def usage_labels(
    group_by: str, keys: list[str], *, store: UsageLabelStore
) -> dict[str, dict[str, Any]]:
    """Resolve display labels for breakdown keys via an app-DB lookup (cross-DB).

    user → users.display_name (+ is_admin); project → projects.name; model → none
    (the key IS the label). Robust to the audit/app DB split — a separate query,
    merged in Python, NOT a SQL join.
    """
    if not keys or group_by == "model":
        return {}
    uids = [UUID(k) for k in keys]
    if group_by == "user":
        rows = await store.fetch(
            "SELECT id, display_name, is_admin FROM users WHERE id = ANY($1::uuid[])",
            uids,
        )
        return {
            str(r["id"]): {"label": r["display_name"], "is_admin": r["is_admin"]}
            for r in rows
        }
    rows = await store.fetch(
        "SELECT id, name FROM projects WHERE id = ANY($1::uuid[])", uids
    )
    return {str(r["id"]): {"label": r["name"]} for r in rows}


def build_timeseries(
    rows: list[dict[str, Any]], labels: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Pivot flat (day, key, metrics) rows into a day axis + per-key series.

    ``days`` is the sorted union of all buckets; each series carries only the days
    it has activity (sparse — the client zero-fills gaps). Series are ordered by
    total events desc so the busiest key stacks first and leads the legend, matching
    ``merge_labels``. Unknown keys fall back to the raw key as their label.
    """
    days = sorted({r["day"] for r in rows})
    series: dict[str, dict[str, Any]] = {}
    for r in rows:
        k = r["key"]
        s = series.setdefault(
            k,
            {
                "key": k,
                "label": labels.get(k, {}).get("label", k),
                "is_admin": labels.get(k, {}).get("is_admin"),
                "events": 0,
                "points": [],
            },
        )
        s["points"].append(
            {
                "day": r["day"],
                "tokens": r["tokens"],
                "cost_usd": r["cost_usd"],
                "events": r["events"],
            }
        )
        s["events"] += r["events"]
    ordered = sorted(series.values(), key=lambda s: s["events"], reverse=True)
    return {"days": days, "series": ordered}


def _usage_window(
    *, days: int, from_date: str | None, to_date: str | None, now: datetime
) -> tuple[datetime, datetime]:
    """Resolve the caller-supplied window, preserving the 400 on a bad date."""
    try:
        to_ts = parse_utc_date(to_date) if to_date else now
        from_ts = (
            parse_utc_date(from_date) if from_date else (to_ts - timedelta(days=days))
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"invalid date: {e}") from e
    return from_ts, to_ts


async def usage_summary_v2(
    *,
    user: dict[str, Any],
    days: int,
    from_date: str | None,
    to_date: str | None,
    ref_id: str | None,
    include_non_customer: bool,
    dependencies: UsageReportingDependencies,
) -> UsageSummaryV2:
    """Dimensionally typed usage summary with a gated source-aware handoff.

    The caller has already been authorized and any ``ref_id`` resolved against
    the caller's visibility; this owns the readiness gates, the window, and the
    typed read itself.
    """
    logger = dependencies.logger
    usage_v2 = dependencies.infrastructure_usage_v2
    rollup = dependencies.infrastructure_usage_rollup
    if usage_v2 is None or not usage_v2.is_available:
        raise HTTPException(status_code=503, detail="Usage API v2 schema unavailable")
    if rollup is None:
        raise HTTPException(
            status_code=503, detail="Usage API v2 bootstrap unavailable"
        )
    try:
        bootstrap = await rollup.bootstrap_state()
    except Exception as exc:
        logger.warning("Usage API v2 bootstrap readiness check failed", exc_info=True)
        raise HTTPException(
            status_code=503, detail="Usage API v2 bootstrap unavailable"
        ) from exc
    if not bootstrap.read_ready:
        raise HTTPException(status_code=503, detail="Usage API v2 bootstrap incomplete")
    now = datetime.now(timezone.utc)
    try:
        to_ts = parse_utc_date(to_date) if to_date else now
        from_ts = (
            parse_utc_date(from_date) if from_date else (to_ts - timedelta(days=days))
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid date: {exc}") from exc
    if to_ts <= from_ts:
        raise HTTPException(
            status_code=400, detail="usage window end must be after its start"
        )
    try:
        visibility = visibility_from_kwargs(
            await visibility_kwargs_for_stats(user, dependencies=dependencies),
            include_non_customer=include_non_customer,
        )
        return await usage_v2.summary(
            from_ts=from_ts,
            to_ts=to_ts,
            visibility=visibility,
            ref_id=ref_id,
            as_of=now,
        )
    except HTTPException:
        raise
    except UsageReadCutoverInactive as exc:
        raise HTTPException(
            status_code=503,
            detail="Infrastructure usage cutover is not active",
        ) from exc
    except Exception as exc:
        logger.exception("Usage API v2 query failed")
        raise HTTPException(
            status_code=500, detail="Usage API v2 query failed"
        ) from exc


async def usage_summary(
    *,
    user: dict[str, Any],
    days: int,
    from_date: str | None,
    to_date: str | None,
    ref_id: str | None,
    dependencies: UsageReportingDependencies,
) -> dict[str, Any]:
    """Aggregate usage (LLM tokens + workspace compute) for the caller (G5)."""
    logger = dependencies.logger
    ledger = dependencies.usage_ledger
    rollup = dependencies.usage_rollup
    if ledger is None or not ledger.is_available:
        return {
            "by_category": [],
            "total_cost_usd": 0.0,
            "cache_hit_ratio": 0.0,
            "cloud_estimates": [],
            "available": False,
        }
    now = datetime.now(timezone.utc)
    from_ts, to_ts = _usage_window(
        days=days, from_date=from_date, to_date=to_date, now=now
    )
    vis = await visibility_kwargs_for_stats(user, dependencies=dependencies)
    try:
        # Read through the rollup (closed days from usage_daily, raw for the open
        # tail); ref_id per-job cost is not a rollup dim → it goes straight to raw
        # inside usage_rollup.usage. Fall back to the raw ledger if the rollup
        # singleton is absent (test/degraded).
        if rollup is not None:
            result = await rollup.usage(
                from_ts=from_ts,
                to_ts=to_ts,
                owner_user_id=vis.get("owner_user_id"),
                visible_project_ids=vis.get("visible_project_ids"),
                scope_project_id=vis.get("scope_project_id"),
                ref_id=ref_id,
            )
        else:
            result = await ledger.query_usage(
                from_ts=from_ts,
                to_ts=to_ts,
                owner_user_id=vis.get("owner_user_id"),
                visible_project_ids=vis.get("visible_project_ids"),
                scope_project_id=vis.get("scope_project_id"),
                ref_id=ref_id,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if dependencies.usage_cloud_estimator is not None:
        try:
            result[
                "cloud_estimates"
            ] = await dependencies.usage_cloud_estimator.estimate(
                result.get("by_category") or []
            )
        except Exception:
            # Planning estimates are strictly non-load-bearing. The measured
            # quantities and canonical LLM cost remain useful on their own.
            logger.warning(
                "cloud-equivalent usage estimate failed (non-fatal)",
                exc_info=True,
            )
            result["cloud_estimates"] = []
    else:
        result["cloud_estimates"] = []
    result["available"] = True
    result["from"] = from_ts.isoformat()
    result["to"] = to_ts.isoformat()
    return result


async def usage_breakdown(
    *,
    user: dict[str, Any],
    group_by: str,
    days: int,
    from_date: str | None,
    to_date: str | None,
    dependencies: UsageReportingDependencies,
) -> dict[str, Any]:
    """Per-(user|model|project) usage breakdown over a window (G5-scoped)."""
    ledger = dependencies.usage_ledger
    rollup = dependencies.usage_rollup
    if group_by not in USAGE_BREAKDOWN_GROUPS:
        raise HTTPException(status_code=400, detail=f"bad group_by: {group_by}")
    if ledger is None or not ledger.is_available:
        return {"available": False, "group_by": group_by, "rows": []}
    now = datetime.now(timezone.utc)
    from_ts, to_ts = _usage_window(
        days=days, from_date=from_date, to_date=to_date, now=now
    )
    vis = await visibility_kwargs_for_stats(user, dependencies=dependencies)
    try:
        reader = rollup.breakdown if rollup is not None else (ledger.query_grouped)
        rows = await reader(
            from_ts=from_ts,
            to_ts=to_ts,
            group_by=group_by,
            owner_user_id=vis.get("owner_user_id"),
            visible_project_ids=vis.get("visible_project_ids"),
            scope_project_id=vis.get("scope_project_id"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    folded = fold_breakdown(rows)
    labels = await usage_labels(group_by, list(folded.keys()), store=dependencies.store)
    return {
        "available": True,
        "group_by": group_by,
        "from": from_ts.isoformat(),
        "to": to_ts.isoformat(),
        "rows": merge_labels(folded, labels),
    }


async def usage_timeseries(
    *,
    user: dict[str, Any],
    group_by: str,
    days: int,
    from_date: str | None,
    to_date: str | None,
    dependencies: UsageReportingDependencies,
) -> dict[str, Any]:
    """Daily-bucketed usage time series grouped by user|model|project (G5-scoped)."""
    ledger = dependencies.usage_ledger
    rollup = dependencies.usage_rollup
    if group_by not in USAGE_BREAKDOWN_GROUPS:
        raise HTTPException(status_code=400, detail=f"bad group_by: {group_by}")
    if ledger is None or not ledger.is_available:
        return {"available": False, "group_by": group_by, "days": [], "series": []}
    now = datetime.now(timezone.utc)
    from_ts, to_ts = _usage_window(
        days=days, from_date=from_date, to_date=to_date, now=now
    )
    vis = await visibility_kwargs_for_stats(user, dependencies=dependencies)
    try:
        reader = rollup.timeseries if rollup is not None else (ledger.query_timeseries)
        rows = await reader(
            from_ts=from_ts,
            to_ts=to_ts,
            group_by=group_by,
            owner_user_id=vis.get("owner_user_id"),
            visible_project_ids=vis.get("visible_project_ids"),
            scope_project_id=vis.get("scope_project_id"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    labels = await usage_labels(
        group_by, sorted({r["key"] for r in rows}), store=dependencies.store
    )
    return {
        "available": True,
        "group_by": group_by,
        "from": from_ts.isoformat(),
        "to": to_ts.isoformat(),
        **build_timeseries(rows, labels),
    }
