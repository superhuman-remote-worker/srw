"""Usage rollup — aggregate the auditdb ``usage_events`` firehose into the app-DB
``usage_daily`` mirror, and serve reads from the rollup for closed days + the raw
ledger for the open tail.

Two halves:

- **The rollup pass** (:meth:`UsageRollup.run_pass`) — a cross-DB job. It reads
  and *aggregates* a trailing window of the auditdb ledger (``usage_events``,
  migrations/audit/0002) grouped per UTC day x (user, project, category,
  resource, unit), then **full-replace upserts** those daily totals into the
  app-DB ``usage_daily`` and advances the ``rollup_state`` watermark **in one
  app-DB transaction**. That single atomic app write is the cross-DB
  exactly-once trick: the audit rows are only read (never locked), so a crash
  mid-pass rolls back the upsert *and* the watermark together and the next pass
  re-closes the same window. Because the source is append-only and the upsert
  overwrites in place, re-runs are idempotent — the safety mechanisms
  (a ~15 min safety lag before a day is closeable, a trailing re-close window
  that re-aggregates the last few closed days every pass, and startup catch-up)
  all converge instead of double-counting.

- **The serving split** (:meth:`usage` / :meth:`breakdown` / :meth:`timeseries`)
  — ``/api/usage`` and friends read the pre-aggregated ``usage_daily`` for the
  whole UTC days that fall entirely inside the query window AND are ``<=`` the
  watermark, and fall back to the raw :class:`UsageLedger` for everything else
  (the partial low-boundary day, and the open tail incl. today). The two halves
  are disjoint and their union is the exact query window, so the merged result
  equals a pure-raw aggregation (that equality is the reconcile invariant the
  tests pin). ``ref_id`` (per-job/thread cost) is NOT a rollup dim, so those
  queries bypass the rollup entirely and stay on the raw ledger.

Non-load-bearing, like the rest of the audit/usage tier: when either pool is
absent the pass no-ops and the serving methods transparently fall back to the
raw ledger (or empty when that too is unavailable).

Design: ``knowledge-base/knowledge/features/database_roadmap.md`` Phase 6 (D-1).
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import asyncpg

from orchestrator.services.usage_ledger import (
    V1_USAGE_COMPAT_PREDICATE,
    UsageLedger,
    _uuid,
    cache_hit_ratio_from_rows,
)

logger = logging.getLogger(__name__)

# A UTC day D is "closeable" (safe to roll up and never re-open on the normal
# path) once we are this far past its end — absorbs the materializer's own
# aging window + assign-before-commit visibility so a straggler for 23:59 lands
# before D is frozen. The trailing re-close window below is the second net.
_SAFETY_LAG = timedelta(minutes=15)

# Every pass re-aggregates this many trailing closed days (not just the newest),
# so a late arrival for a recently-closed day is picked up without a separate
# weekly cadence. Cheap: partition pruning bounds the scan to the touched months.
_TRAILING_DAYS = 7

_WATERMARK = "usage_daily"

# Aggregate the raw ledger per UTC calendar day x dims. `ts AT TIME ZONE 'UTC'`
# matches the ledger's own day bucketing (usage_ledger.query_timeseries) so the
# rollup and the raw tail agree on day boundaries. ts is the partition key, so
# the range predicate prunes to the touched monthly partitions.
_AGG_SQL = """
SELECT (ts AT TIME ZONE 'UTC')::date AS day,
       user_id, project_id, category, resource, unit,
       SUM(quantity)               AS quantity,
       COALESCE(SUM(cost_usd), 0)  AS cost_usd,
       COUNT(*)                    AS events
FROM usage_events
WHERE ts >= $1 AND ts < $2
GROUP BY 1, user_id, project_id, category, resource, unit
"""

# Full-replace upsert (measures overwrite, never accumulate). NULLS NOT DISTINCT
# on usage_daily_dims_idx makes the NULL-dim (unattributed) bucket conflict.
_UPSERT_SQL = """
INSERT INTO usage_daily
    (day, user_id, project_id, category, resource, unit,
     quantity, cost_usd, events, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
ON CONFLICT (day, user_id, project_id, category, resource, unit)
DO UPDATE SET quantity   = EXCLUDED.quantity,
              cost_usd   = EXCLUDED.cost_usd,
              events     = EXCLUDED.events,
              updated_at = now()
"""

# Dimension -> the usage_daily column it groups on (mirrors usage_ledger._GROUP_COLS).
_GROUP_COLS = {"user": "user_id", "model": "resource", "project": "project_id"}


def _day_of(ts: datetime) -> date:
    """The UTC calendar day ``ts`` falls in."""
    return ts.astimezone(timezone.utc).date()


def _midnight(d: date) -> datetime:
    """UTC midnight at the start of date ``d``."""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _split_window(
    from_ts: datetime, to_ts: datetime, watermark: Optional[date]
) -> Tuple[Optional[Tuple[date, date]], List[Tuple[datetime, datetime]]]:
    """Partition ``[from_ts, to_ts)`` into a rollup day-range + raw intervals.

    Returns ``(rollup_days, raw_intervals)`` where ``rollup_days`` is an inclusive
    ``(lo, hi)`` of whole UTC days that both fall entirely inside the window and
    are ``<= watermark`` (read from usage_daily), or ``None`` if there are none;
    and ``raw_intervals`` is the list of ``[lo_ts, hi_ts)`` sub-windows to read
    from the raw ledger. The two cover ``[from_ts, to_ts)`` exactly and disjoint.
    """
    if to_ts <= from_ts:
        return None, []
    if watermark is None:
        return None, [(from_ts, to_ts)]

    # First whole day fully inside the window (round from_ts UP to a UTC midnight).
    fday = _day_of(from_ts)
    first_full = fday if _midnight(fday) == from_ts else fday + timedelta(days=1)
    # The day containing to_ts is never complete within the half-open window, so
    # the last whole day inside is the day before it.
    last_full = _day_of(to_ts) - timedelta(days=1)

    hi = min(last_full, watermark)
    lo = first_full
    if hi < lo:
        return None, [(from_ts, to_ts)]

    raw: List[Tuple[datetime, datetime]] = []
    if from_ts < _midnight(lo):  # partial low-boundary day
        raw.append((from_ts, _midnight(lo)))
    hi_next = _midnight(hi + timedelta(days=1))
    if hi_next < to_ts:  # open tail (incl. today) + any within-safety-lag days
        raw.append((hi_next, to_ts))
    return (lo, hi), raw


def _merge_sum(
    parts: Sequence[List[Dict[str, Any]]], key_fields: Sequence[str]
) -> List[Dict[str, Any]]:
    """Sum the numeric measures of rows across ``parts``, grouped by ``key_fields``.

    Non-key numeric fields (``quantity``/``cost_usd``/``events``/``tokens``) add;
    non-key non-numeric fields take the first value seen. Preserves the union of
    keys across all parts. Used to fold the rollup half + the raw half(s) of a
    split window into one result identical to a single-pass aggregation.
    """
    acc: Dict[Tuple, Dict[str, Any]] = {}
    for rows in parts:
        for r in rows:
            k = tuple(r.get(f) for f in key_fields)
            cur = acc.get(k)
            if cur is None:
                acc[k] = dict(r)
                continue
            for field, val in r.items():
                if field in key_fields:
                    continue
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    cur[field] = cur.get(field, 0) + val
    return list(acc.values())


class UsageRollup:
    """Cross-DB usage rollup writer + rollup-aware read surface.

    ``audit_pool`` reads the raw ``usage_events`` ledger; ``app_pool`` owns the
    ``usage_daily`` mirror + ``rollup_state`` watermark; ``ledger`` serves the raw
    tail (its visibility SQL is the contract the rollup queries mirror).
    """

    def __init__(
        self,
        audit_pool: Optional[asyncpg.Pool],
        app_pool: Optional[asyncpg.Pool],
        ledger: UsageLedger,
    ):
        self._audit = audit_pool
        self._app = app_pool
        self._ledger = ledger

    @property
    def is_available(self) -> bool:
        return self._audit is not None and self._app is not None

    # ---- watermark ---------------------------------------------------------

    async def _read_watermark(self) -> Optional[date]:
        if self._app is None:
            return None
        try:
            return await self._app.fetchval(
                "SELECT last_closed_day FROM rollup_state WHERE name = $1", _WATERMARK
            )
        except Exception:
            logger.warning("usage rollup: watermark read failed", exc_info=True)
            return None

    # ---- the pass ----------------------------------------------------------

    async def run_pass(
        self,
        *,
        now: Optional[datetime] = None,
        trailing_days: int = _TRAILING_DAYS,
    ) -> Dict[str, Any]:
        """Re-aggregate the trailing closed-day window and advance the watermark.

        Aggregates ``[start_day, newest_closeable]`` (UTC days) from the auditdb
        and full-replace upserts into ``usage_daily``; advances ``last_closed_day``
        to ``newest_closeable`` in the same app-DB transaction. ``start_day`` is
        the earliest usage day on the first run (watermark NULL), else
        ``watermark - (trailing_days - 1)`` so recently-closed days re-close and
        absorb late arrivals. No-op (watermark unchanged) when unavailable or
        nothing is closeable yet.
        """
        if not self.is_available:
            return {"available": False, "rolled_days": 0, "rows": 0}

        now = now or datetime.now(timezone.utc)
        newest_closeable = (now - _SAFETY_LAG).date() - timedelta(days=1)

        watermark = await self._read_watermark()
        if watermark is None:
            earliest = await self._earliest_day()
            if earliest is None:
                # No usage yet — mark everything up to now as (emptily) closed so
                # the first real pass starts from the trailing window, not epoch.
                await self._advance_watermark(newest_closeable)
                return {
                    "rolled_days": 0,
                    "rows": 0,
                    "last_closed_day": newest_closeable,
                }
            start_day = earliest
        else:
            start_day = watermark - timedelta(days=max(0, trailing_days - 1))

        if newest_closeable < start_day:
            # Clock-skew or a just-booted-before-first-full-day edge; nothing to do.
            return {"rolled_days": 0, "rows": 0, "last_closed_day": watermark}

        lo_ts = _midnight(start_day)
        hi_ts = _midnight(newest_closeable + timedelta(days=1))
        try:
            async with self._audit.acquire() as conn:
                agg = await conn.fetch(_AGG_SQL, lo_ts, hi_ts)
        except Exception:
            logger.warning("usage rollup: aggregation read failed", exc_info=True)
            return {"rolled_days": 0, "rows": 0, "last_closed_day": watermark}

        rows = [
            (
                r["day"],
                r["user_id"],
                r["project_id"],
                r["category"],
                r["resource"],
                r["unit"],
                r["quantity"],
                r["cost_usd"],
                r["events"],
            )
            for r in agg
        ]
        try:
            async with self._app.acquire() as conn:
                async with conn.transaction():
                    if rows:
                        await conn.executemany(_UPSERT_SQL, rows)
                    await conn.execute(
                        "UPDATE rollup_state SET last_closed_day = $1, "
                        "updated_at = now() WHERE name = $2",
                        newest_closeable,
                        _WATERMARK,
                    )
        except Exception:
            logger.warning("usage rollup: upsert/watermark failed", exc_info=True)
            return {"rolled_days": 0, "rows": 0, "last_closed_day": watermark}

        rolled = (newest_closeable - start_day).days + 1
        logger.info(
            "usage rollup: closed %d day(s) [%s..%s], %d daily row(s)",
            rolled,
            start_day,
            newest_closeable,
            len(rows),
        )
        return {
            "rolled_days": rolled,
            "rows": len(rows),
            "last_closed_day": newest_closeable,
            "start_day": start_day,
        }

    async def _earliest_day(self) -> Optional[date]:
        try:
            async with self._audit.acquire() as conn:
                ts = await conn.fetchval(
                    "SELECT min(ts) AT TIME ZONE 'UTC' FROM usage_events"
                )
            return ts.date() if ts is not None else None
        except Exception:
            logger.warning("usage rollup: earliest-day probe failed", exc_info=True)
            return None

    async def _advance_watermark(self, day: date) -> None:
        try:
            async with self._app.acquire() as conn:
                await conn.execute(
                    "UPDATE rollup_state SET last_closed_day = $1, updated_at = now() "
                    "WHERE name = $2",
                    day,
                    _WATERMARK,
                )
        except Exception:
            logger.warning("usage rollup: watermark advance failed", exc_info=True)

    # ---- serving (rollup for closed days, raw for the tail) ----------------

    async def usage(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
        ref_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """``/api/usage`` — sums by (category, unit) + headline cost.

        Rollup for the whole closed days inside the window, raw for the rest.
        ``ref_id`` (per-job/thread) is not a rollup dim → straight to raw.
        """
        if ref_id is not None or not self.is_available:
            return await self._ledger.query_usage(
                from_ts=from_ts,
                to_ts=to_ts,
                owner_user_id=owner_user_id,
                visible_project_ids=visible_project_ids,
                scope_project_id=scope_project_id,
                ref_id=ref_id,
            )
        watermark = await self._read_watermark()
        rollup_days, raw = _split_window(from_ts, to_ts, watermark)

        parts: List[List[Dict[str, Any]]] = []
        if rollup_days is not None:
            parts.append(
                await self._rollup_by_category(
                    rollup_days, owner_user_id, visible_project_ids, scope_project_id
                )
            )
        for lo, hi in raw:
            r = await self._ledger.query_usage(
                from_ts=lo,
                to_ts=hi,
                owner_user_id=owner_user_id,
                visible_project_ids=visible_project_ids,
                scope_project_id=scope_project_id,
            )
            parts.append(r["by_category"])

        by_category = _merge_sum(parts, ("category", "unit"))
        by_category.sort(key=lambda x: (x["category"], x["unit"]))
        total = sum(item["cost_usd"] for item in by_category)
        return {
            "by_category": by_category,
            "total_cost_usd": total,
            "cache_hit_ratio": cache_hit_ratio_from_rows(by_category),
        }

    async def breakdown(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        group_by: str,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """``/api/usage/breakdown`` — sums by (key, unit), key in {user,model,project}."""
        if not self.is_available:
            return await self._ledger.query_grouped(
                from_ts=from_ts,
                to_ts=to_ts,
                group_by=group_by,
                owner_user_id=owner_user_id,
                visible_project_ids=visible_project_ids,
                scope_project_id=scope_project_id,
            )
        watermark = await self._read_watermark()
        rollup_days, raw = _split_window(from_ts, to_ts, watermark)

        parts: List[List[Dict[str, Any]]] = []
        if rollup_days is not None:
            parts.append(
                await self._rollup_grouped(
                    rollup_days, group_by, owner_user_id, scope_project_id
                )
            )
        for lo, hi in raw:
            parts.append(
                await self._ledger.query_grouped(
                    from_ts=lo,
                    to_ts=hi,
                    group_by=group_by,
                    owner_user_id=owner_user_id,
                    visible_project_ids=visible_project_ids,
                    scope_project_id=scope_project_id,
                )
            )
        rows = _merge_sum(parts, ("key", "unit"))
        rows.sort(key=lambda x: x["key"])
        return rows

    async def timeseries(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        group_by: str,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """``/api/usage/timeseries`` — per (UTC day, key) buckets."""
        if not self.is_available:
            return await self._ledger.query_timeseries(
                from_ts=from_ts,
                to_ts=to_ts,
                group_by=group_by,
                owner_user_id=owner_user_id,
                visible_project_ids=visible_project_ids,
                scope_project_id=scope_project_id,
            )
        watermark = await self._read_watermark()
        rollup_days, raw = _split_window(from_ts, to_ts, watermark)

        parts: List[List[Dict[str, Any]]] = []
        if rollup_days is not None:
            parts.append(
                await self._rollup_timeseries(
                    rollup_days, group_by, owner_user_id, scope_project_id
                )
            )
        for lo, hi in raw:
            parts.append(
                await self._ledger.query_timeseries(
                    from_ts=lo,
                    to_ts=hi,
                    group_by=group_by,
                    owner_user_id=owner_user_id,
                    visible_project_ids=visible_project_ids,
                    scope_project_id=scope_project_id,
                )
            )
        # Rollup days and raw days are disjoint by construction; merge-by-(day,key)
        # is defensive and keeps the shape identical to a single-pass query.
        rows = _merge_sum(parts, ("day", "key"))
        rows.sort(key=lambda x: x["day"])
        return rows

    # ---- rollup-side aggregations over usage_daily -------------------------

    async def _rollup_by_category(
        self,
        days: Tuple[date, date],
        owner_user_id: Optional[str],
        visible_project_ids: Optional[Sequence[str]],
        scope_project_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """(category, unit) sums from usage_daily — visibility mirrors
        UsageLedger.query_usage (self OR visible-project for a non-admin)."""
        clauses = ["day >= $1", "day <= $2", V1_USAGE_COMPAT_PREDICATE]
        params: List[Any] = [days[0], days[1]]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            own = f"user_id = ${len(params)}"
            pids = [_uuid(p) for p in (visible_project_ids or [])]
            if pids:
                params.append(pids)
                clauses.append(f"({own} OR project_id = ANY(${len(params)}::uuid[]))")
            else:
                clauses.append(own)
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        sql = (
            "SELECT category, unit, SUM(quantity) AS quantity, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, SUM(events) AS events "
            f"FROM usage_daily WHERE {' AND '.join(clauses)} "
            "GROUP BY category, unit"
        )
        rows = await self._app.fetch(sql, *params)
        return [
            {
                "category": r["category"],
                "unit": r["unit"],
                "quantity": float(r["quantity"]) if r["quantity"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": int(r["events"]),
            }
            for r in rows
        ]

    async def _rollup_grouped(
        self,
        days: Tuple[date, date],
        group_by: str,
        owner_user_id: Optional[str],
        scope_project_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """(key, unit) sums — strict self-scope for a non-admin, NULL keys excluded
        (mirrors UsageLedger.query_grouped)."""
        col = _GROUP_COLS[group_by]
        clauses = [
            "day >= $1",
            "day <= $2",
            f"{col} IS NOT NULL",
            V1_USAGE_COMPAT_PREDICATE,
        ]
        if group_by == "model":
            clauses.append("category = 'llm'")
        params: List[Any] = [days[0], days[1]]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            clauses.append(f"user_id = ${len(params)}")
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        sql = (
            f"SELECT {col} AS key, unit, SUM(quantity) AS quantity, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, SUM(events) AS events "
            f"FROM usage_daily WHERE {' AND '.join(clauses)} GROUP BY {col}, unit"
        )
        rows = await self._app.fetch(sql, *params)
        return [
            {
                "key": str(r["key"]),
                "unit": r["unit"],
                "quantity": float(r["quantity"]) if r["quantity"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": int(r["events"]),
            }
            for r in rows
        ]

    async def _rollup_timeseries(
        self,
        days: Tuple[date, date],
        group_by: str,
        owner_user_id: Optional[str],
        scope_project_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """(day, key) buckets — tokens = prompt+cached+completion, mirrors
        UsageLedger.query_timeseries (already day-granular in usage_daily)."""
        col = _GROUP_COLS[group_by]
        clauses = [
            "day >= $1",
            "day <= $2",
            f"{col} IS NOT NULL",
            V1_USAGE_COMPAT_PREDICATE,
        ]
        if group_by == "model":
            clauses.append("category = 'llm'")
        params: List[Any] = [days[0], days[1]]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            clauses.append(f"user_id = ${len(params)}")
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        sql = (
            f"SELECT day, {col} AS key, "
            "COALESCE(SUM(quantity) FILTER "
            "(WHERE unit IN "
            "('prompt-token', 'cached-prompt-token', 'completion-token')), 0) "
            "AS tokens, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, SUM(events) AS events "
            f"FROM usage_daily WHERE {' AND '.join(clauses)} GROUP BY day, {col}"
        )
        rows = await self._app.fetch(sql, *params)
        return [
            {
                "day": r["day"].isoformat(),
                "key": str(r["key"]),
                "tokens": float(r["tokens"]) if r["tokens"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": int(r["events"]),
            }
            for r in rows
        ]


async def usage_rollup_loop(
    shutdown_event,
    rollup: Optional[UsageRollup],
    *,
    interval_s: int = 6 * 3600,
    jitter_s: int = 1800,
) -> None:
    """Lifespan task body: roll up at startup (catch up days missed while down),
    then every ~6h with 0-30min jitter. Every pass is wrapped so a failure
    ERROR-logs and the loop continues — the rollup must never crash the
    orchestrator. No-op when the rollup is unavailable: it logs once and waits
    for ``shutdown_event``.
    """
    if rollup is None or not rollup.is_available:
        logger.info("usage rollup loop disabled (rollup unavailable)")
        # Park until shutdown instead of returning: run_when_leader re-creates
        # a loop that returns on its next poll, which re-logged this line about
        # once a second for the whole leadership tenure (R1.B12). The rollup's
        # pools are fixed for the lifecycle, so nothing is lost.
        await shutdown_event.wait()
        return

    logger.info(
        "usage rollup loop started (interval=%ds, jitter<=%ds)", interval_s, jitter_s
    )
    while not shutdown_event.is_set():
        try:
            await rollup.run_pass()
        except Exception:
            logger.exception("usage rollup pass failed (non-fatal)")
        delay = interval_s + random.uniform(0, jitter_s)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
    logger.info("usage rollup loop stopped")


__all__ = ["UsageRollup", "usage_rollup_loop", "_split_window"]
