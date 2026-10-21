"""HTTP adapters for usage and fleet-statistics reads with per-app dependencies.

Every metering collaborator is assigned during application startup and is
``None`` at import, so the factory behind :func:`get_usage_reporting_dependencies`
must resolve them per invocation. Capturing one at module import binds the
pre-startup ``None`` forever, which the routes would report as "metering is
off" rather than as the wiring bug it is.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from orchestrator.security.access import (
    mcp_scope_project_id,
    require_job_access,
    user_can_access_job_or_thread,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import usage_reporting
from orchestrator.services.infrastructure_metering import UsageSummaryV2

router = APIRouter()


@dataclass(frozen=True)
class UsageReportingDependencies:
    """Per-app auth store, gates and reporting collaborators."""

    store: Any
    reports: usage_reporting.UsageReportingDependencies
    require_admin: Callable[..., Awaitable[Any]]
    metering_settings: Any
    scope_project_id: Callable[[dict[str, Any]], Any] = mcp_scope_project_id
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    user_can_access_job_or_thread: Callable[..., Awaitable[Any]] = (
        user_can_access_job_or_thread
    )


def get_usage_reporting_dependencies(request: Request) -> UsageReportingDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.usage_reporting_dependencies_factory()


@router.get("/api/jobs/{job_id}/usage")
async def get_job_usage(
    request: Request,
    job_id: str,
    include_subjobs: bool = Query(default=False),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Metered tokens, machine time and price for one job.

    Reads the ``usage_events`` ledger rather than re-aggregating ``llm_requests``:
    the materializer already normalizes tokens out of three different homes, and
    a fresh aggregator reproduces the bug where Responses-API rows (``token_usage
    = {}`` plus a NULL ``metadata``) meter as nothing — one job lost ~17.3M tokens
    that way before the fallbacks landed (``services/audit_usage.py``).

    Three things this endpoint owns that ``GET /api/usage?ref_id=`` does not:

    * **The window.** ``usage_events`` is partitioned on ``ts``, so every read
      needs a range and a caller who picks the wrong one gets a confident zero:
      ``/api/usage?ref_id=<job>&days=1`` reports ``total_cost_usd: 0`` with
      ``available: true`` for a job that cost $0.94 a week earlier. Here the
      window comes from the job itself, and its tail is *now* rather than
      ``completed_at`` — workspace-pod intervals close at teardown and async
      auxiliary calls (memory extraction, summarization) land after the job
      seals, so a window that ends at completion loses real spend.
    * **Unpriced is not free.** ``cost.usd`` is null until something is priced,
      and ``cost.complete`` says whether every metered event carried a rate. The
      shared ``query_usage`` COALESCEs the sum to 0.0, which is why unpriced
      workspace compute currently renders as costing nothing.
    * **Three empty states, not one.** ``no_usage`` (really spent nothing),
      ``predates_ledger`` (created before the materializer's forward-only anchor,
      so it has no rows and never will), ``unavailable`` (audit tier off). Only
      the first is a zero.

    ``include_subjobs=true`` sums the job and every descendant, terminal ones
    included — the honest total for a parent row in a list that pages over
    display roots, where children ride along with their parent. Default is the
    job's own spend.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await usage_reporting.job_usage(
        job_id=job_id,
        job=job,
        include_subjobs=include_subjobs,
        dependencies=dependencies.reports,
    )


@router.get("/api/stats/session-wakes")
async def get_session_wake_statistics(
    request: Request,
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, int]:
    """Health of the session-wake outbox. **Admin only** (fleet-wide).

    ``dead`` is retry exhaustion: each one is a retained session waiting on a
    job it will never be told about, so a non-zero value warrants an alert.
    ``undeliverable`` is the distinct terminal count for wakes whose exact
    creating thread was hard-deleted. ``pending``/``sending`` are the in-flight
    depth; a steadily growing ``sending`` means deliveries are timing out rather
    than failing fast.
    """
    await dependencies.require_admin(request)
    return await usage_reporting.session_wake_statistics(
        dependencies=dependencies.reports
    )


@router.get("/api/stats/daily")
async def get_daily_statistics(
    request: Request,
    days: int = Query(default=7, ge=1, le=90),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> list[dict[str, Any]]:
    """Get daily job statistics for the past N days, scoped to the caller's visibility (G5)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await usage_reporting.daily_statistics(
        user=user, days=days, dependencies=dependencies.reports
    )


@router.get("/api/stats/agents")
async def get_agent_statistics(
    request: Request,
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Get agent workforce summary. **Admin only** (G5) — counts the
    fleet by status, which is infra-level data tied to ``/api/admin/agents``."""
    await dependencies.require_admin(request)
    return await usage_reporting.agent_statistics(dependencies=dependencies.reports)


@router.get("/api/stats/stuck")
async def get_stuck_jobs(
    request: Request,
    threshold_minutes: int | None = Query(default=None, ge=1, le=1440),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Get jobs suspected stuck, scoped to the caller's visibility (G5).

    E3 (officer_supervision_surface §5): stuckness comes from the one shared
    liveness computation — audit movement first, then agent heartbeat —
    never from ``jobs.updated_at`` (display metadata poisoned by trigger
    cascades). Rows carry the liveness state and reasons. Jobs whose
    activity evidence is unreachable are returned with
    ``state='unavailable'`` — surfaced uncertainty, not a fabricated
    stuck fact. Admins see the full fleet; non-admins see only jobs they
    own or are project members of.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await usage_reporting.stuck_jobs(
        user=user,
        threshold_minutes=threshold_minutes,
        dependencies=dependencies.reports,
    )


@router.get("/api/usage/v2", response_model=UsageSummaryV2)
async def get_usage_v2(
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
    from_date: str | None = Query(
        default=None, description="ISO date/datetime (UTC); overrides `days`"
    ),
    to_date: str | None = Query(
        default=None, description="ISO date/datetime (UTC), exclusive"
    ),
    ref_id: str | None = Query(default=None, description="Filter to one job/thread id"),
    include_non_customer: bool = Query(
        default=False,
        description="Fleet admins only: include shared-platform and unknown rows",
    ),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> UsageSummaryV2:
    """Dimensionally typed usage summary with a gated source-aware handoff.

    The default path reads finalized ledger rows with Decimal-safe string
    quantities. Once the independent source-aware gate is enabled after the
    durable cutover, the same route combines sealed daily rows, verified audit
    fragments, and confirmed provisional interval tails.
    """
    if not dependencies.metering_settings.v2_reads_enabled:
        raise HTTPException(status_code=404, detail="Usage API v2 is not enabled")
    user = await dependencies.require_approved_user(request, dependencies.store)
    fleet_admin = (
        bool(user.get("is_admin")) and dependencies.scope_project_id(user) is None
    )
    if include_non_customer and not fleet_admin:
        raise HTTPException(status_code=403, detail="Fleet admin access required")
    if ref_id is not None:
        try:
            UUID(ref_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid ref_id") from exc
        if not await dependencies.user_can_access_job_or_thread(
            user, dependencies.store, ref_id
        ):
            # Do not turn a guessed ledger reference into a presence oracle.
            raise HTTPException(status_code=404, detail="Usage reference not found")
    return await usage_reporting.usage_summary_v2(
        user=user,
        days=days,
        from_date=from_date,
        to_date=to_date,
        ref_id=ref_id,
        include_non_customer=include_non_customer,
        dependencies=dependencies.reports,
    )


@router.get("/api/usage")
async def get_usage(
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
    from_date: str | None = Query(
        default=None, description="ISO date/datetime (UTC); overrides `days`"
    ),
    to_date: str | None = Query(
        default=None, description="ISO date/datetime (UTC), exclusive"
    ),
    ref_id: str | None = Query(default=None, description="Filter to one job/thread id"),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Aggregate usage (LLM tokens + workspace compute) for the caller (G5).

    Reads the ``usage_events`` ledger (Slice 4). Window defaults to the last
    ``days``; ``from_date``/``to_date`` override it. Admins see the full fleet
    (optionally narrowed by an MCP ``project:`` scope); non-admins see only rows
    they own or can see via project membership. Returns sums by (category, unit),
    the canonical ``total_cost_usd``, provider-list-price ``cloud_estimates``, and
    ``cache_hit_ratio`` for LLM prompt cache reads. Cloud estimates reprice the
    measured quantities at read time and never mutate the ledger. ``available=false``
    means the audit tier is off — metering disabled.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await usage_reporting.usage_summary(
        user=user,
        days=days,
        from_date=from_date,
        to_date=to_date,
        ref_id=ref_id,
        dependencies=dependencies.reports,
    )


@router.get("/api/usage/breakdown")
async def get_usage_breakdown(
    request: Request,
    group_by: str = Query(..., description="user | model | project"),
    days: int = Query(default=30, ge=1, le=365),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Per-(user|model|project) usage breakdown over a window (G5-scoped).

    Quantities by unit + key-level event/cost totals, labels enriched from the app
    DB. Non-admins are strictly self-scoped (see query_grouped). ``available=false``
    when the audit tier is off.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await usage_reporting.usage_breakdown(
        user=user,
        group_by=group_by,
        days=days,
        from_date=from_date,
        to_date=to_date,
        dependencies=dependencies.reports,
    )


@router.get("/api/usage/timeseries")
async def get_usage_timeseries(
    request: Request,
    group_by: str = Query(..., description="user | model | project"),
    days: int = Query(default=30, ge=1, le=365),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    *,
    dependencies: UsageReportingDependencies = Depends(
        get_usage_reporting_dependencies
    ),
) -> dict[str, Any]:
    """Daily-bucketed usage time series grouped by user|model|project (G5-scoped).

    Powers the dashboard's stacked usage-over-time chart. Returns a sorted ``days``
    axis and one ``series`` per key, each carrying per-day {day, tokens, cost_usd,
    events} points (days a key had no activity are omitted, not zero-filled — the
    client fills gaps). Labels are enriched from the app DB; non-admins are strictly
    self-scoped (see query_timeseries). ``available=false`` when metering is off.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await usage_reporting.usage_timeseries(
        user=user,
        group_by=group_by,
        days=days,
        from_date=from_date,
        to_date=to_date,
        dependencies=dependencies.reports,
    )
