"""Workspace compute metering (Slice 4b) — open/close intervals → usage ledger.

A workspace pod consuming reserved CPU/memory from creation to deletion is a
*time-driven* cost (it bills with zero LLM calls), so the orchestrator — which
owns pod lifecycle — is the meter. The container provisioner records an OPEN
interval when it creates a pod and CLOSES it when it deletes one, into the app-DB
``workspace_intervals`` table; :func:`materialize_and_reconcile` (run on a loop)
turns each CLOSED interval into two immutable ``usage_events`` rows (vcpu-hour +
gib-hour, **requests × wall-clock** — observability_and_quotas decision 5) and
bounds leaked opens. Keeping the mutable open-interval bookkeeping in the app DB
and only the finalized rows in the auditdb ledger keeps the ledger append-only
and keeps the provisioner (no auditdb handle) decoupled.

Design: ``knowledge-base/knowledge/features/observability_and_quotas.md`` ("Compute cost" +
"open-interval reconciler"). **Non-load-bearing:** every emit is best-effort — a
metering failure must never break provisioning. v1 meters **container (sandbox)**
workspaces; VM-tier emits are an additive follow-up (the table + loop are
tier-agnostic — a VM open/close just calls the same two functions).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from orchestrator.services.usage_ledger import UsageEvent, UsageLedger

logger = logging.getLogger(__name__)

_GIB = 1024**3

# Memory suffixes (binary Ki/Mi/Gi/Ti + decimal k/K/M/G/T), longest-first so the
# 2-char binary units match before the 1-char decimal ones.
_MEM_UNITS: Dict[str, int] = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "k": 1000,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}

# Attribution resolver: (owner_kind, owner_id) → {"user_id", "project_id"} | None.
AttributionFn = Callable[[str, str], Awaitable[Optional[Dict[str, Any]]]]


async def _legacy_metering_allowed(db: Any) -> bool:
    """Fail closed after the durable v2 cutover barrier exists.

    Standalone/legacy test databases may contain ``workspace_intervals`` without
    the infrastructure-metering control table; that is the only case treated as
    the pre-cutover compatibility mode.  A real control lookup failure is not a
    reason to reopen the legacy writer.
    """

    try:
        relation = await db.fetchval(
            "SELECT to_regclass('public.infra_metering_control')::text"
        )
        if relation is None:
            return True
        state = await db.fetchval(
            "SELECT cutover_state FROM infra_metering_control WHERE singleton = TRUE"
        )
        return state == "disabled"
    except Exception:
        logger.warning(
            "workspace metering: cutover fence lookup failed; legacy writer "
            "remains disabled",
            exc_info=True,
        )
        return False


def parse_cpu_millicores(cpu: str) -> int:
    """K8s CPU quantity → millicores. '500m' → 500, '2' → 2000, '2000m' → 2000."""
    s = str(cpu).strip()
    if s.endswith("m"):
        return int(float(s[:-1]))
    return int(float(s) * 1000)


def parse_mem_bytes(mem: str) -> int:
    """K8s memory quantity → bytes. '1Gi' → 2**30, '512Mi' → ..., '1G' → 10**9."""
    s = str(mem).strip()
    for suffix, mult in sorted(_MEM_UNITS.items(), key=lambda kv: -len(kv[0])):
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * mult)
    return int(float(s))  # bare bytes


async def open_interval(
    db: Any, owner: Any, *, tier: str, cpu: str, memory: str
) -> None:
    """Record an OPEN compute interval for ``owner`` (best-effort, app DB).

    Idempotent via the ``workspace_intervals_open_uq`` partial unique index: a
    re-create of a still-open workspace is a no-op (keeps the original
    started_at). ``db`` is anything exposing asyncpg's ``execute`` (the
    orchestrator's PostgresDB or a raw pool).
    """
    if db is None:
        return
    try:
        if not await _legacy_metering_allowed(db):
            logger.debug(
                "workspace metering: legacy open suppressed after cutover barrier"
            )
            return
        cm = parse_cpu_millicores(cpu)
        mb = parse_mem_bytes(memory)
        await db.execute(
            "INSERT INTO workspace_intervals "
            "(owner_kind, owner_id, tier, cpu_millicores, mem_bytes) "
            "VALUES ($1, $2::uuid, $3, $4, $5) "
            "ON CONFLICT (owner_kind, owner_id) WHERE ended_at IS NULL DO NOTHING",
            owner.kind,
            str(owner.id),
            tier,
            cm,
            mb,
        )
    except Exception:
        logger.warning(
            "workspace metering: open failed for %s %s (non-fatal)",
            getattr(owner, "kind", "?"),
            getattr(owner, "id", "?"),
            exc_info=True,
        )


async def close_interval(db: Any, owner: Any) -> None:
    """Close ``owner``'s open interval (best-effort). No-op if none is open."""
    if db is None:
        return
    try:
        await db.execute(
            "UPDATE workspace_intervals SET ended_at = now() "
            "WHERE owner_kind = $1 AND owner_id = $2::uuid AND ended_at IS NULL",
            owner.kind,
            str(owner.id),
        )
    except Exception:
        logger.warning(
            "workspace metering: close failed for %s %s (non-fatal)",
            getattr(owner, "kind", "?"),
            getattr(owner, "id", "?"),
            exc_info=True,
        )


def _interval_to_events(
    r: Any, attribution: Optional[Dict[str, Any]]
) -> List[UsageEvent]:
    """One closed interval → a vcpu-hour row + a gib-hour row (requests × hours)."""
    started, ended = r["started_at"], r["ended_at"]
    dur_h = max(0.0, (ended - started).total_seconds() / 3600.0)
    vcpu_hours = (r["cpu_millicores"] / 1000.0) * dur_h
    gib_hours = (r["mem_bytes"] / _GIB) * dur_h
    attribution = attribution or {}
    ref_kind = "job" if r["owner_kind"] == "job" else "thread"
    ref_id = str(r["owner_id"])
    # Deterministic per interval (started_at epoch) → re-materialize dedupes.
    src_id = f"ws:{r['owner_kind']}:{ref_id}:{int(started.timestamp())}"
    details = {
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "cpu_millicores": r["cpu_millicores"],
        "mem_bytes": r["mem_bytes"],
        "tier": r["tier"],
        "duration_h": round(dur_h, 6),
    }
    common = dict(
        category="compute",
        resource="workspace_pod",
        source="orchestrator",
        source_id=src_id,
        ts=ended,
        user_id=attribution.get("user_id"),
        project_id=attribution.get("project_id"),
        ref_kind=ref_kind,
        ref_id=ref_id,
        details=details,
    )
    return [
        UsageEvent(quantity=vcpu_hours, unit="vcpu-hour", **common),
        UsageEvent(quantity=gib_hours, unit="gib-hour", **common),
    ]


async def materialize_and_reconcile(
    db: Any,
    ledger: Optional[UsageLedger],
    attribution: AttributionFn,
    *,
    batch: int = 200,
    orphan_after_h: int = 24,
) -> Dict[str, int]:
    """Close leaked opens, then write closed intervals into the usage ledger.

    1. **Reconcile** — close any interval still open past ``orphan_after_h`` (the
       normal close path covers the common case; this bounds a leak from a skipped
       delete / lost node so it can't bill forever). ended_at is capped at
       started_at + the window.
    2. **Materialize** — each closed, not-yet-materialized interval becomes a
       vcpu-hour + gib-hour ``usage_events`` pair, attributed via ``attribution``,
       then stamped ``materialized_at``. Idempotent: the ledger dedupes on
       re-emit, and the stamp prevents reprocessing.
    """
    if db is None or ledger is None or not ledger.is_available:
        return {"materialized": 0, "reconciled": 0}
    if not await _legacy_metering_allowed(db):
        # The cutover coordinator owns strict legacy integrity/drain from the
        # first preparing transaction onward.  In particular, never run the
        # old blind 24-hour close behind that barrier.
        return {"materialized": 0, "reconciled": 0}

    reconciled = 0
    try:
        status = await db.execute(
            "UPDATE workspace_intervals "
            "SET ended_at = started_at + make_interval(hours => $1) "
            "WHERE ended_at IS NULL "
            "AND started_at < now() - make_interval(hours => $1)",
            orphan_after_h,
        )
        reconciled = int(str(status).split()[-1])
        if reconciled:
            logger.warning(
                "workspace metering: reconciled %d leaked open interval(s) "
                "(capped at %dh)",
                reconciled,
                orphan_after_h,
            )
    except Exception:
        logger.warning(
            "workspace metering: reconcile failed (non-fatal)", exc_info=True
        )

    try:
        rows = await db.fetch(
            "SELECT id, owner_kind, owner_id, tier, cpu_millicores, mem_bytes, "
            "started_at, ended_at FROM workspace_intervals "
            "WHERE ended_at IS NOT NULL AND materialized_at IS NULL "
            "ORDER BY ended_at LIMIT $1",
            batch,
        )
    except Exception:
        logger.warning(
            "workspace metering: fetch pending failed (non-fatal)", exc_info=True
        )
        return {"materialized": 0, "reconciled": reconciled}

    materialized = 0
    for r in rows:
        try:
            attrib = await attribution(r["owner_kind"], str(r["owner_id"]))
            await ledger.record_events(_interval_to_events(r, attrib))
            await db.execute(
                "UPDATE workspace_intervals SET materialized_at = now() WHERE id = $1",
                r["id"],
            )
            materialized += 1
        except Exception:
            logger.warning(
                "workspace metering: materialize interval %s failed (non-fatal)",
                r["id"],
                exc_info=True,
            )

    if materialized:
        logger.info("workspace metering: materialized %d interval(s)", materialized)
    return {"materialized": materialized, "reconciled": reconciled}


async def workspace_metering_loop(
    shutdown_event: asyncio.Event,
    db: Any,
    ledger: Optional[UsageLedger],
    attribution: AttributionFn,
    *,
    interval: float = 120.0,
) -> None:
    """Lifespan task: periodically materialize closed intervals + reconcile leaks.

    No-op when the app DB or the ledger is absent (metering disabled): it logs
    once and waits for ``shutdown_event``. Never raises into the loop — a pass
    failure is logged and retried.
    """
    if db is None or ledger is None or not ledger.is_available:
        logger.info("workspace metering loop disabled (no db/ledger)")
        # Park until shutdown instead of returning: run_when_leader re-creates
        # a loop that returns on its next poll, which re-logged this line about
        # once a second for the whole leadership tenure (R1.B12). The ledger's
        # availability is fixed for the lifecycle, so nothing is lost.
        await shutdown_event.wait()
        return
    logger.info("workspace metering loop starting (interval=%ss)", interval)
    try:
        while not shutdown_event.is_set():
            try:
                await materialize_and_reconcile(db, ledger, attribution)
            except Exception:
                logger.exception("workspace metering pass failed (non-fatal)")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("workspace metering loop stopped")


__all__ = [
    "parse_cpu_millicores",
    "parse_mem_bytes",
    "open_interval",
    "close_interval",
    "materialize_and_reconcile",
    "workspace_metering_loop",
]
