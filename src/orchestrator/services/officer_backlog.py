"""Officer auto-pull tick — machine liveness for the century's backlog.

The officer decides WHAT gets worked; this decides only WHEN a ready ticket
becomes a job. Every judgment call stays his: classification, arming a ticket,
reviewing an outcome, re-arming it. The tick's whole job is to notice that a
pool has a free slot and an authorized ticket, and to close that gap within a
minute instead of whenever he next wakes (knowledge-base/knowledge/features/officer_backlog_pools.md
§5).

**Claims are one-shot.** A ticket is claimed the moment a job carries its id —
in any status, terminal included. Dispatch consumes readiness; only the officer
re-arming the ticket (a fresh ``ready_at``) makes it eligible again. The
original design released claims when a job went terminal, which meant every
completed ticket was re-dispatched a minute later and every failing one was
re-burned at breaker cadence, forever. It also gives the century a dead-letter
queue for free: a failed ticket parks until someone looks at it.

**Correctness does not rest on leadership.** The tick is mounted leader-gated
because N replicas scanning is waste, but dual-leader windows are real and
acknowledged in-tree, so the claim itself is what has to be safe: the ticket
check, the capacity count and the job INSERT happen in ONE transaction holding
the stable durable-post row lock, and a unique index on ``(project_id, ticket)``
over non-terminal jobs refuses a racing second claim outright.

**Failures and outages are different things.** The pool breaker counts job
failures only. An honest ``goal_achieved=false`` never trips it — punishing a
truthful negative report is how you train a fleet to stop filing them — and
neither does a KB outage, a usage-ledger hiccup, or an exception in this module.
Infra problems log and retry; they never open a breaker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Awaitable, Callable, Optional, Sequence

from fastapi import HTTPException

from orchestrator.security.access import project_is_archived
from orchestrator.services.datasource_policy import (
    DatasourceUnavailableError,
    default_datasource_selection,
)
from orchestrator.services.officer_admission import (
    SLOT_VACATING_STATUSES,
    OfficerAdmissionConflict,
    SlotAdmissionError,
    admit_and_create_job,
    apply_prepared_slot_config,
    officer_is_held,
    prepare_officer_admission,
)
from orchestrator.services.officer_slots import roster_from_meta
from orchestrator.services.officer_preflight import ensure_officer_job_activated
from orchestrator.services.job_liveness import JobLivenessPolicy, get_liveness_policy
from orchestrator.services.project_backlog import (
    BacklogCursor,
    fetch_backlog,
    fetch_ready_backlog_candidates,
    fetch_ticket_state,
)
from orchestrator.services.work_categories import (
    EXECUTOR,
    category_block,
    classify_ticket,
    resolve_expert,
)
from shared.backlog_tags import READY_TAG, category_tag

logger = logging.getLogger(__name__)

TICK_SECONDS = int(os.getenv("OFFICER_BACKLOG_TICK_SECONDS", "60"))

# A claim whose job has not moved in this long renders as "claimed-but-stalled".
# Never auto-released: a second job for a claimed ticket must not exist until
# the officer releases the first, which is the failure mode silent-redelivery
# queues are famous for.
# Compatibility/export only. Runtime decisions resolve the stale-claim arm of
# the shared typed liveness policy once per tick.
STALE_CLAIM_HOURS = get_liveness_policy().stale_claim.minutes / 60.0

# pending_review claims page rather than merely rendering — that lane has a
# known dead zone, and a silently stranded review is exactly the invisibility
# this feature exists to end.
PENDING_REVIEW_PAGE_HOURS = float(os.getenv("OFFICER_PENDING_REVIEW_PAGE_HOURS", "24"))

# Two consecutive job failures on DISTINCT tickets open a pool for 30 minutes.
# Distinct because two failures on one ticket are one problem, not a pattern.
BREAKER_FAILURES = 2
BREAKER_OPEN_MINUTES = float(os.getenv("OFFICER_POOL_BREAKER_MINUTES", "30"))


class WorkspaceRefused(Exception):
    """The workspace defaults chain refused a dispatch (Slice A2b, spec §7).

    Configuration the owner must fix, not an outage and not a job failure: the
    tick records it on the post as ``backlog_workspace_refusals`` (sitrep and
    post card) and retries the pool on the next tick.
    """


# A floor breach wakes the officer at most this often per pool. Event-driven
# replenishment beats a passive number on a card, but a queue that is short
# because there is genuinely nothing to do must not page him every minute.
FLOOR_WAKE_DEBOUNCE_HOURS = float(os.getenv("OFFICER_FLOOR_WAKE_HOURS", "6"))

# A transport page size, never a semantic ceiling. Cross-store eligibility
# cannot be expressed as one SQL join because ticket ordering lives in the KB
# database while durable claims live in the app database. The scanner below
# therefore keyset-pages until it has enough eligible rows for its decision or
# has proved exhaustion.
_BACKLOG_SCAN_PAGE_SIZE = 100

# Ready-depth is a control-plane observation, not a data export.  Keep the
# three-query batch bounded even if a project accidentally tags an enormous
# fraction of its vault ready.  The extra row distinguishes a truthful exact
# result from an unavailable over-ceiling result; it is never rendered as a
# truncated depth.
_READY_DEPTH_MAX_CANDIDATES = max(
    1, int(os.getenv("OFFICER_READY_DEPTH_MAX_CANDIDATES", "50000"))
)
_READY_DEPTH_BATCH_VERSION = 1

# Viewer polls commonly arrive together.  Share only a computation that is
# still running; completed observations are removed immediately, so this does
# not turn stale backlog state into a cache hit.
_READY_DEPTH_INFLIGHT: dict[
    tuple[int, int, str, tuple[str, ...], datetime | None],
    asyncio.Task["ReadyDepthObservation"],
] = {}

ProvisionFn = Callable[..., Awaitable[None]]
GrantsFn = Callable[..., Awaitable[None]]


@dataclass
class EligibilityScan:
    """Result of a cross-store backlog scan with explicit completeness."""

    tickets: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    exhausted: bool = False
    lower_bound: bool = False
    unavailable: bool = False
    pages: int = 0
    rows_scanned: int = 0
    error: str | None = None


@dataclass(frozen=True)
class ReadyDepthObservation:
    """One versioned, bounded cross-store ready-depth observation."""

    depths_by_category: dict[str, int]
    observed_at: datetime
    state: str
    rows_scanned: int
    query_count: int
    elapsed_ms: float
    error: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _slot_workspace_backend(config_override: dict[str, Any] | None) -> Optional[str]:
    """The WORKER's workspace tier, from the authoritative slot patch.

    ``officer_slots`` writes ``workspace.backend`` per roster slot, so this is
    the tier the dispatched job will actually run on. It is what connector
    resolution must be measured against: the policy service withholds
    clone-based repositories from lite tiers, and the officer's own post is
    lite. Passing his tier instead of the worker's would silently drop the
    repository and reproduce the very defect this resolution exists to fix.
    """
    if not isinstance(config_override, dict):
        return None
    workspace = config_override.get("workspace")
    if not isinstance(workspace, dict):
        return None
    backend = workspace.get("backend")
    return str(backend) if backend else None


def _uuid_or_none(value: Any) -> Any:
    """asyncpg wants a real UUID for a uuid column; tests hand in strings."""
    import uuid as _uuid

    try:
        return _uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None


def _aware(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _officer_meta(thread: dict[str, Any]) -> dict[str, Any]:
    """The officer block off a thread row, tolerant of JSON-as-text metadata."""
    import json

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    block = (metadata.get("config_override") or {}).get("officer") or {}
    return block if isinstance(block, dict) else {}


def _officer_state(thread: dict[str, Any]) -> dict[str, Any]:
    import json

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            metadata = {}
    state = metadata.get("officer_state") or {}
    return state if isinstance(state, dict) else {}


def auto_pull_enabled(officer_meta: dict[str, Any]) -> bool:
    """Ships OFF, flipped per century (§13.1).

    A century whose officer has not triaged a backlog yet must not start
    pulling whatever happens to be tagged.
    """
    return officer_meta.get("auto_pull") in (True, "true", "True", 1)


def pools_from_meta(officer_meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Roster slots that carry a category — the ones the tick may fill.

    A slot with no category is not a pool and is never touched here: it stays
    exactly what it was before this feature, officer-directed capacity.
    """
    roster = roster_from_meta(officer_meta) or {}
    return {
        name: spec
        for name, spec in roster.items()
        if isinstance(spec, dict) and spec.get("category")
    }


def breaker_is_open(state: dict[str, Any], pool: str, now: datetime) -> bool:
    """True while ``pool``'s breaker is still open. Per-pool, never global —
    a burning research pool must not stop the executor from shipping."""
    entry = (state.get("backlog_breakers") or {}).get(pool) or {}
    until = _aware(entry.get("until"))
    return bool(until and until > now)


def evaluate_breaker(
    recent: Sequence[dict[str, Any]], state: dict[str, Any], pool: str
) -> Optional[dict[str, Any]]:
    """Should ``pool``'s breaker trip on this history? Returns the new entry.

    ``recent`` is the pool's terminal ticket-claiming jobs, newest first. The
    breaker opens when the two most recent outcomes on DISTINCT tickets are both
    ``failed`` — a chain, not an incident. Returns None when it should not trip,
    including when it already tripped on this same newest failure: re-deriving
    from history alone would re-open the breaker forever once those two rows
    stop changing.
    """
    outcomes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for job in recent:
        ticket = job.get("ticket_note_id")
        if not ticket or ticket in seen:
            continue
        seen.add(ticket)
        outcomes.append(job)
        if len(outcomes) == BREAKER_FAILURES:
            break
    if len(outcomes) < BREAKER_FAILURES:
        return None
    if any(job.get("status") != "failed" for job in outcomes):
        return None
    newest = str(outcomes[0].get("id"))
    prior = (state.get("backlog_breakers") or {}).get(pool) or {}
    if str(prior.get("tripped_on_job") or "") == newest:
        return None
    return {
        "tripped_on_job": newest,
        "tickets": [str(job.get("ticket_note_id")) for job in outcomes],
        "cause": f"{BREAKER_FAILURES} consecutive job failures on distinct tickets",
    }


def stale_claims(
    claims: Sequence[dict[str, Any]],
    now: datetime,
    *,
    policy: JobLivenessPolicy | None = None,
) -> list[dict[str, Any]]:
    """Claims whose job has not moved in STALE_CLAIM_HOURS, oldest first.

    Surfaced, never auto-released (§5.3). ``updated_at`` is acceptable here and
    only here: this is a control-plane staleness question ("has anything touched
    this row"), not the liveness verdict — a job that looks stalled to the tick
    still gets its real reading from ``compute_jobs_liveness`` on the sitrep.
    """
    effective_policy = policy or get_liveness_policy()
    cutoff = now - timedelta(minutes=effective_policy.stale_claim.minutes)
    out = []
    for job in claims:
        moved = _aware(job.get("updated_at")) or _aware(job.get("created_at"))
        if moved and moved < cutoff:
            out.append(
                {
                    "job_id": str(job.get("id")),
                    "ticket_note_id": job.get("ticket_note_id"),
                    "slot": job.get("officer_slot"),
                    "status": job.get("status"),
                    "since": moved.isoformat(),
                    "age_hours": round((now - moved).total_seconds() / 3600.0, 1),
                }
            )
    out.sort(key=lambda item: item["since"])
    return out


def eligible_tickets(
    rows: Sequence[dict[str, Any]],
    claims: dict[str, Any],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Split ready tickets into (dispatchable, skip-reasons).

    Four ways a ready-tagged ticket is still not dispatchable, and each is
    reported rather than silently dropped:

    * **Ambiguous** — two ``category:`` tags, or an expert pin that names no
      real config. Guessing would dispatch into the wrong pool under the
      officer's name, and an unvalidated pin fails at agent boot where it looks
      like a job failure and chain-trips the breaker.
    * **Unauthorized** — the ``ready`` tag is present but ``ready_at`` is NULL.
      That happens after a vault rebuild lost the timestamp, and the fail-closed
      direction is the only defensible one for a dispatch authorization.
    * **Claimed** — a job already carries this ticket and the officer has not
      re-armed it since.
    * **Legacy barrier** — a pre-ledger job had no provable generation, so the
      Officer has not explicitly re-readied it after the migration cutover.
    """
    ready: list[dict[str, Any]] = []
    notes: list[str] = []
    for row in rows:
        note_id = str(row.get("note_id") or "")
        classification = classify_ticket(row.get("tags"))
        if classification.problems:
            notes.append(f"{note_id}: {'; '.join(classification.problems)}")
            continue
        authorized_at = _aware(row.get("ready_at"))
        if authorized_at is None:
            notes.append(f"{note_id}: ready tag with no ready_at (re-arm it)")
            continue
        claim_state = claims.get(note_id)
        if isinstance(claim_state, dict):
            claimed_generation = _aware(claim_state.get("ready_generation_at"))
            legacy_rearm_after = _aware(claim_state.get("legacy_rearm_after"))
            has_non_terminal = bool(claim_state.get("has_non_terminal"))
        else:
            # Compatibility for pure callers/tests using the former
            # ``note_id -> datetime`` shape. Production reads the durable
            # state mapping from PostgresDB.ticket_claim_states.
            claimed_generation = _aware(claim_state)
            legacy_rearm_after = None
            has_non_terminal = False
        if has_non_terminal:
            notes.append(f"{note_id}: prior job is still non-terminal")
            continue
        if claimed_generation is not None and claimed_generation >= authorized_at:
            notes.append(
                f"{note_id}: generation claimed at {claimed_generation.isoformat()}"
            )
            continue
        if legacy_rearm_after is not None and legacy_rearm_after >= authorized_at:
            notes.append(
                f"{note_id}: legacy claim requires re-ready after "
                f"{legacy_rearm_after.isoformat()}"
            )
            continue
        ready.append({**row, "classification": classification})
    return ready, notes


async def _scan_eligible_tickets(
    db: Any,
    vector_db: Any,
    project_id: str,
    category: str,
    now: datetime,
    *,
    minimum: int | None,
) -> EligibilityScan:
    """Keyset-page across KB order until eligibility is sufficient or exact.

    ``minimum=None`` requests an exact count and therefore scans to exhaustion.
    A positive minimum is enough for admission/floor decisions; reaching it is
    an explicitly lower-bound result. Any KB or app-database failure is marked
    unavailable, never reinterpreted as an empty pool.
    """

    result = EligibilityScan()
    cursor: BacklogCursor | None = None
    seen_cursors: set[BacklogCursor] = set()
    target = None if minimum is None else max(1, int(minimum))

    while True:
        try:
            rows, _counts = await fetch_backlog(
                vector_db,
                project_id,
                require_tags=[READY_TAG, category_tag(category)],
                limit=_BACKLOG_SCAN_PAGE_SIZE,
                after=cursor,
                include_counts=False,
            )
        except Exception as exc:
            result.unavailable = True
            result.error = f"KB backlog read failed: {type(exc).__name__}"
            return result

        result.pages += 1
        result.rows_scanned += len(rows)
        if not rows:
            result.exhausted = True
            return result

        try:
            note_ids = [str(row.get("note_id")) for row in rows]
            claims = await db.ticket_claim_states(project_id, note_ids)
            unresolved_reader = getattr(db, "unresolved_knowledge_note_ids", None)
            unresolved_result = (
                await unresolved_reader(project_id, note_ids)
                if callable(unresolved_reader)
                else set()
            )
            unresolved = (
                {str(note_id) for note_id in unresolved_result}
                if isinstance(unresolved_result, (set, list, tuple))
                else set()
            )
        except Exception as exc:
            result.unavailable = True
            result.error = f"app-state read failed: {type(exc).__name__}"
            return result

        eligible_rows = [
            row for row in rows if str(row.get("note_id")) not in unresolved
        ]
        result.notes.extend(
            f"{row.get('note_id')}: canonical knowledge sync unresolved"
            for row in rows
            if str(row.get("note_id")) in unresolved
        )
        ready, notes = eligible_tickets(eligible_rows, claims, now)
        result.tickets.extend(ready)
        result.notes.extend(notes)
        if target is not None and len(result.tickets) >= target:
            result.lower_bound = True
            return result

        if len(rows) < _BACKLOG_SCAN_PAGE_SIZE:
            result.exhausted = True
            return result

        next_cursor = BacklogCursor.from_row(rows[-1])
        if not next_cursor.note_id or next_cursor in seen_cursors:
            result.unavailable = True
            result.error = "KB backlog cursor did not advance"
            return result
        seen_cursors.add(next_cursor)
        cursor = next_cursor


async def _observe_ready_depth_categories(
    db: Any,
    vector_db: Any,
    project_id: str,
    categories: tuple[str, ...],
    observed_at: datetime,
) -> ReadyDepthObservation:
    """Measure all requested categories with three bounded SQL statements."""
    started = perf_counter()
    query_count = 1
    try:
        rows = await fetch_ready_backlog_candidates(
            vector_db,
            project_id,
            list(categories),
            limit=_READY_DEPTH_MAX_CANDIDATES + 1,
        )
    except Exception as exc:
        return ReadyDepthObservation(
            depths_by_category={},
            observed_at=observed_at,
            state="unavailable",
            rows_scanned=0,
            query_count=query_count,
            elapsed_ms=(perf_counter() - started) * 1000,
            error=f"KB backlog read failed: {type(exc).__name__}",
        )

    rows_scanned = len(rows)
    if rows_scanned > _READY_DEPTH_MAX_CANDIDATES:
        return ReadyDepthObservation(
            depths_by_category={},
            observed_at=observed_at,
            state="unavailable",
            rows_scanned=rows_scanned,
            query_count=query_count,
            elapsed_ms=(perf_counter() - started) * 1000,
            error="candidate ceiling exceeded",
        )

    note_ids = [str(row.get("note_id") or "") for row in rows]
    claims: dict[str, Any] = {}
    unresolved: set[str] = set()
    if note_ids:
        try:
            query_count += 1
            claims = await db.ticket_claim_states(project_id, note_ids)
            unresolved_reader = getattr(db, "unresolved_knowledge_note_ids", None)
            if callable(unresolved_reader):
                query_count += 1
                unresolved_result = await unresolved_reader(project_id, note_ids)
            else:
                unresolved_result = set()
            unresolved = (
                {str(note_id) for note_id in unresolved_result}
                if isinstance(unresolved_result, (set, list, tuple))
                else set()
            )
        except Exception as exc:
            return ReadyDepthObservation(
                depths_by_category={},
                observed_at=observed_at,
                state="unavailable",
                rows_scanned=rows_scanned,
                query_count=query_count,
                elapsed_ms=(perf_counter() - started) * 1000,
                error=f"app-state read failed: {type(exc).__name__}",
            )

    eligible_rows = [
        row for row in rows if str(row.get("note_id") or "") not in unresolved
    ]
    ready, _notes = eligible_tickets(eligible_rows, claims, observed_at)
    depths = {category: 0 for category in categories}
    for row in ready:
        classification = row.get("classification")
        category = getattr(classification, "category", None)
        if category in depths:
            depths[category] += 1
    return ReadyDepthObservation(
        depths_by_category=depths,
        observed_at=observed_at,
        state="exact",
        rows_scanned=rows_scanned,
        query_count=query_count,
        elapsed_ms=(perf_counter() - started) * 1000,
    )


def _forget_ready_depth_task(
    key: tuple[int, int, str, tuple[str, ...], datetime | None],
    task: asyncio.Task[ReadyDepthObservation],
) -> None:
    if _READY_DEPTH_INFLIGHT.get(key) is task:
        _READY_DEPTH_INFLIGHT.pop(key, None)


async def ready_depth_by_pool(
    db: Any,
    vector_db: Any,
    project_id: str,
    pools: dict[str, dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    caller: str = "unspecified",
) -> dict[str, int]:
    """How many tickets each pool could actually take right now.

    This deliberately preserves the tick's OWN eligibility functions rather
    than using a cheaper count of everything wearing a ``ready`` tag.  The
    vector candidates, durable claims and unresolved materialization intents
    are each read once for the whole roster.  Concurrent viewer polls share
    only that in-progress observation; no completed result is cached.

    Returns ``{}`` on any read failure or bounded-scan overflow — the caller
    renders capacity without depth rather than asserting a zero it did not
    measure.
    """
    pool_categories: dict[str, str] = {}
    for pool, spec in pools.items():
        category = str(spec.get("category") or "").strip().lower()
        if not category:
            continue
        pool_categories[str(pool)] = category
    categories = tuple(sorted(set(pool_categories.values())))
    if not categories:
        return {}

    observed_at = now or _now()
    # Explicit historical/test observations do not coalesce with a live poll;
    # live concurrent viewers intentionally share the creator's timestamp.
    observation_key = observed_at if now is not None else None
    key = (id(db), id(vector_db), str(project_id), categories, observation_key)
    task = _READY_DEPTH_INFLIGHT.get(key)
    shared = task is not None
    if task is None:
        task = asyncio.create_task(
            _observe_ready_depth_categories(
                db, vector_db, str(project_id), categories, observed_at
            )
        )
        _READY_DEPTH_INFLIGHT[key] = task
        task.add_done_callback(lambda done: _forget_ready_depth_task(key, done))

    try:
        observation = await asyncio.shield(task)
    except Exception:
        logger.warning(
            "officer backlog: ready-depth batch failed caller=%s version=%s",
            caller,
            _READY_DEPTH_BATCH_VERSION,
            exc_info=True,
        )
        return {}
    finally:
        if task.done():
            _forget_ready_depth_task(key, task)

    if not shared:
        log = logger.info if observation.state == "exact" else logger.warning
        log(
            "officer backlog: ready-depth caller=%s version=%s state=%s "
            "queries=%s rows=%s pools=%s categories=%s elapsed_ms=%.2f "
            "observed_at=%s%s",
            caller,
            _READY_DEPTH_BATCH_VERSION,
            observation.state,
            observation.query_count,
            observation.rows_scanned,
            len(pool_categories),
            len(categories),
            observation.elapsed_ms,
            observation.observed_at.isoformat(),
            f" error={observation.error}" if observation.error else "",
        )
    if observation.state != "exact":
        return {}
    return {
        pool: observation.depths_by_category.get(category, 0)
        for pool, category in pool_categories.items()
    }


def pool_status_lines(
    officer_meta: dict[str, Any],
    officer_state: dict[str, Any],
    now: Optional[datetime] = None,
) -> list[str]:
    """Sitrep lines for the policies the tick enforces on the officer's behalf.

    A policy that is enforced but invisible invites doctrine drift: the officer
    watches a pool sit idle and concludes the queue is fine, when in fact its
    breaker is open. Everything here is state the tick already wrote to
    ``officer_state``; this only renders it.
    """
    now = now or _now()
    lines: list[str] = []
    pools = pools_from_meta(officer_meta)
    if not pools:
        return lines

    if not auto_pull_enabled(officer_meta):
        lines.append(
            "Auto-pull: OFF for this century — categorized pools are not "
            "filled automatically; dispatch by hand or ask the Legate to "
            "enable it."
        )

    breakers = officer_state.get("backlog_breakers") or {}
    for pool in sorted(pools):
        entry = breakers.get(pool) or {}
        until = _aware(entry.get("until"))
        if until and until > now:
            minutes = max(1, int((until - now).total_seconds() // 60))
            tickets = ", ".join(entry.get("tickets") or []) or "unknown tickets"
            lines.append(
                f"Pool {pool}: BREAKER OPEN for {minutes}m — "
                f"{entry.get('cause') or 'repeated failures'} ({tickets}). "
                "Read those failures before re-readying anything in this pool."
            )

    refusals = officer_state.get("backlog_workspace_refusals") or {}
    for pool in sorted(pools):
        entry = refusals.get(pool)
        if isinstance(entry, dict) and entry.get("reason"):
            lines.append(
                f"Pool {pool}: CANNOT DISPATCH until the workspace defaults "
                f"are fixed: {entry['reason']}"
            )

    stalled = officer_state.get("backlog_stale_claims") or []
    if stalled:
        rendered = "; ".join(
            f"{item.get('ticket_note_id')} held by job "
            f"{str(item.get('job_id'))[:8]} ({item.get('status')}, "
            f"{item.get('age_hours')}h)"
            for item in stalled[:5]
        )
        more = f" (+{len(stalled) - 5} more)" if len(stalled) > 5 else ""
        lines.append(
            f"Claimed but stalled: {rendered}{more}. These tickets are NOT "
            "released automatically — resume or cancel the job, then close or "
            "re-ready the ticket."
        )
    return lines


async def _spend_exhausted(
    usage_ledger: Any,
    *,
    project_id: str,
    ceiling: float,
    ref_ids: Optional[Sequence[str]] = None,
) -> bool:
    """True when today's spend has reached an OPTIONAL ceiling.

    Fail-open, like the officer's own ceiling: a usage-ledger outage must not
    stop the century from working. Ceilings are unset by default (§13.3), so
    this is skipped entirely for most posts — with no ceiling and no per-ticket
    caps, the century has no mechanical spend brake at all, which is the
    deliberate design: the officer is the brake.
    """
    if (
        not ceiling
        or usage_ledger is None
        or not getattr(usage_ledger, "is_available", False)
    ):
        return False
    try:
        now = _now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if ref_ids is not None:
            if not ref_ids:
                return False
            usage = await usage_ledger.query_usage(
                from_ts=day_start, to_ts=now, ref_ids=list(ref_ids)
            )
        else:
            usage = await usage_ledger.query_usage(
                from_ts=day_start, to_ts=now, scope_project_id=project_id
            )
        return float(usage.get("total_cost_usd") or 0.0) >= float(ceiling)
    except Exception:
        logger.warning(
            "officer backlog: spend check failed for project %s (fail-open)",
            str(project_id)[:8],
            exc_info=True,
        )
        return False


async def _executor_blocked(
    db: Any,
    vector_db: Any,
    *,
    capacity_lineage: Sequence[str],
    project_id: str,
    ticket_is_parallel_safe: bool,
) -> Optional[str]:
    """Reason the executor lane is closed right now, or None.

    Two independent gates, both fail-safe toward NOT dispatching:

    1. **Singleton.** At most one executor claim in flight across all executor
       pools, unless the officer tagged this ticket ``parallel-safe``. Executors
       write to shared project state; two of them make conflicting implicit
       decisions about the same surface. A paused executor is NOT in flight
       (owner ruling 2026-08-18: paused occupies no slot — nothing is running);
       a later resume may briefly overlap a new dispatch, and that is accepted
       over a paused zombie holding the lane shut.
    2. **Disposition.** The previous executor's ticket must be *dispositioned*
       — closed, or explicitly re-readied — not merely terminal. The deliverable
       gate checks that files exist, never what is in them, so without this an
       executor chain builds on unreviewed work and review debt compounds
       straight into the deliverable. If the officer stops reviewing, executors
       stop. That is the intended direction.
    """
    live_executors = [
        claim
        for claim in await db.list_officer_slot_claims(
            list(capacity_lineage), work_category=EXECUTOR, limit=None
        )
        if str(claim.get("status")) not in SLOT_VACATING_STATUSES
    ]
    if live_executors and not ticket_is_parallel_safe:
        return f"executor singleton held by job {str(live_executors[0]['id'])[:8]}"

    recent = await db.list_officer_slot_claims(
        list(capacity_lineage),
        work_category=EXECUTOR,
        include_terminal=True,
        terminal_only=True,
        limit=1,
    )
    previous = recent[0] if recent else None
    if previous is None:
        return None
    ticket_id = previous.get("ticket_note_id")
    if not ticket_id:
        return None
    try:
        ticket = await fetch_ticket_state(vector_db, project_id, str(ticket_id))
    except Exception:
        # A KB outage is not a disposition signal. Skip cleanly rather than
        # assuming either answer; the next tick re-reads.
        logger.warning(
            "officer backlog: disposition read failed for ticket %s", ticket_id
        )
        return "disposition unreadable (KB unavailable)"
    if ticket is None or ticket.get("status") != "active":
        return None  # closed — dispositioned
    re_armed = _aware(ticket.get("ready_at"))
    consumed_generation = _aware(previous.get("ready_generation_at"))
    if consumed_generation is None:
        consumed_generation = _aware(previous.get("legacy_rearm_after"))
    if re_armed and consumed_generation and re_armed > consumed_generation:
        return None  # explicitly re-readied — dispositioned
    return (
        f"previous executor ticket {ticket_id} is undispositioned "
        f"(job {str(previous['id'])[:8]}) — close it or re-ready it"
    )


async def _dispatch_one(
    db: Any,
    *,
    officer_thread_id: str,
    project_id: str,
    owner_user_id: Optional[str],
    pool: str,
    category: str,
    ticket: dict[str, Any],
    provision_repo: Optional[ProvisionFn],
    trigger_dispatch: Optional[Callable[[], None]],
    enforce_grants: Optional[GrantsFn],
) -> Optional[dict[str, Any]]:
    """Claim + create in one transaction, then provision and nudge.

    Returns the created job row, or None when admission refused (a full pool, a
    racing claim). Raises only on genuinely unexpected faults, which the caller
    logs without touching the breaker — infra failures are not job failures.
    """
    classification = ticket["classification"]
    expert = resolve_expert(classification)
    note_id = str(ticket["note_id"])
    title = str(ticket.get("title") or note_id)

    context = {
        "ticket_note_id": note_id,
        "work_category": category,
        "officer_slot": pool,
        # The category contract rides the kickoff, never `instructions` —
        # instructions REPLACES the rendered instructions.md template wholesale.
        "kickoff_message": (
            f"{category_block(category)}\n\n"
            f"TICKET {note_id} — {title}\n\n"
            "Read the full ticket from the project knowledge base "
            f"(kb_read '{note_id}') before you start; the officer's triage "
            "notes live there."
        ),
    }
    # Tick jobs are officer-reviewed by construction. Without this a completion
    # on a non-full-autonomy project parks in pending_review — a lane with a
    # known dead zone — holding its claim forever and double-reviewing what the
    # officer already reviews. It lives in config_override (not context) because
    # that is what both grant PEPs read, and it only survives them because the
    # job is dispatched as `lifecycle`, whose grant class raises the autonomy
    # ceiling to full while keeping the owner's capability grants. Loop jobs
    # carry exactly this pair for exactly this reason.
    config_override: dict[str, Any] = {"autonomy": "full"}

    authorized_at = _aware(ticket.get("ready_at"))

    # Preparation is intentionally outside the final transaction. Slot-driven
    # grant checks may consult policy stores and must never keep the post lock
    # open across that work. The fingerprint makes a roster/config change a
    # deterministic retry instead of a stale dispatch.
    try:
        preparation = await prepare_officer_admission(
            db,
            project_id=project_id,
            thread_id=officer_thread_id,
            requested_slot=pool,
            require_auto_pull=True,
            expected_category=category,
        )
    except (OfficerAdmissionConflict, SlotAdmissionError) as exc:
        logger.info(
            "officer=%s pool=%s skip=admission-preflight:%s",
            officer_thread_id[:8],
            pool,
            exc,
        )
        return None

    prepared_config = apply_prepared_slot_config(config_override, preparation)
    effective_owner_user_id = preparation.owner_user_id or (
        str(owner_user_id) if owner_user_id else None
    )
    from orchestrator.services.manifest_workspace_selection import (
        WorkspaceSelectionRace,
        select_project_workspace_default,
    )

    try:
        (
            prepared_config,
            workspace_selection,
        ) = await select_project_workspace_default(
            db,
            effective_owner_user_id,
            project_id,
            prepared_config,
        )
    except WorkspaceSelectionRace:
        raise  # transient: logged as dispatch-failed, retried next tick
    except HTTPException as exc:
        raise WorkspaceRefused(str(exc.detail)) from exc
    if workspace_selection is not None:
        config_override = prepared_config
    if enforce_grants is not None:
        await enforce_grants(
            prepared_config,
            user_id=effective_owner_user_id,
            project_ids=[project_id] if project_id else [],
        )

    # Connectors. This path never touched them, so an auto-pulled job was
    # created with none at all — no repository checkout, no clone/commit/push
    # — and the worker could only report that it could not do the work. That
    # is exactly what a hand-dispatched officer produced on Better Resavio
    # 2026-08-15; `2afbf956` fixed the REST create path, which this one
    # deliberately bypasses for the claim-ledger transaction, so the fix did
    # not reach here. Under auto-pull the same failure repeats every tick with
    # no human in the loop to notice.
    #
    # Resolve against the WORKER's tier (the slot patch's workspace backend),
    # never the officer's. His own post is lite, where the policy service
    # correctly withholds clone-based repositories; resolving with his backend
    # would silently drop the repository again and look identical to the bug.
    #
    # Resolved per tick rather than cached on the post so a connector added to
    # the project reaches the next dispatch, not the next commission.
    target_project_ids = [project_id] if project_id else []
    if effective_owner_user_id:
        try:
            (
                datasource_ids,
                datasource_policy_revisions,
            ) = await default_datasource_selection(
                db,
                effective_owner_user_id,
                target_project_ids,
                _slot_workspace_backend(prepared_config),
            )
        except DatasourceUnavailableError as exc:
            # A revoked owner, membership or connector. Skip the tick rather
            # than dispatch a worker holding a partial credential contract;
            # the pool stays visibly below floor instead of filling with jobs
            # that cannot reach their sources.
            logger.warning(
                "officer=%s pool=%s skip=connectors-unavailable:%s",
                officer_thread_id[:8],
                pool,
                exc,
            )
            return None
        datasource_origin = "default"
    else:
        # No authoritative principal, so no ambient preferences may be
        # borrowed — the complete, explicit selection is empty. Mirrors the
        # project-loop tick.
        datasource_ids = []
        datasource_policy_revisions = {}
        datasource_origin = "explicit"

    try:
        job = await admit_and_create_job(
            db,
            preparation=preparation,
            ticket_note_id=note_id,
            ticket_ready_at=authorized_at,
            ticket_claim_source="tick",
            strict_provisioning=True,
            job_kwargs={
                **(
                    {"workspace_selection": workspace_selection}
                    if workspace_selection is not None
                    else {}
                ),
                "description": f"[{category}] {title}",
                "config_name": expert,
                # The final helper applies the authoritative slot patch again
                # after locking/revalidating the post.
                "config_override": config_override or None,
                "context": context,
                "user_id": effective_owner_user_id,
                "wake_on_complete": True,
                "authority_user_id": effective_owner_user_id,
                "authority_project_ids": [project_id]
                if effective_owner_user_id
                else None,
                "datasource_ids": datasource_ids,
                "datasource_selection_provenance": {
                    "origin": datasource_origin,
                    "creation_path": "officer_backlog_tick",
                    "effective_work_owner_id": effective_owner_user_id,
                    "target_project_ids": target_project_ids,
                    "datasource_ids": datasource_ids,
                    "policy_revisions": datasource_policy_revisions,
                    "resolved_at": _now().isoformat(),
                },
                "datasource_policy_revisions": datasource_policy_revisions,
                # `lifecycle` is the established system-subjob class with the
                # owner's grants and a full autonomy ceiling.
                "runner_kind": "lifecycle",
            },
        )
    except (OfficerAdmissionConflict, SlotAdmissionError) as exc:
        logger.info(
            "officer=%s pool=%s skip=admission:%s",
            officer_thread_id[:8],
            pool,
            exc,
        )
        return None

    outcome = await ensure_officer_job_activated(
        db,
        job,
        provision=provision_repo,
        category=category,
        trigger_dispatch=trigger_dispatch,
    )
    if not outcome.activated:
        logger.warning(
            "officer backlog: job %s preflight=%s phase=%s error=%s",
            str(job.get("id"))[:8],
            outcome.state,
            outcome.phase,
            outcome.error,
        )
        return None
    return await db.get_job(str(job["id"])) or job


async def _recover_officer_preflights(
    db: Any,
    *,
    project_id: str,
    officer_thread_id: str,
    provision_repo: Optional[ProvisionFn],
    trigger_dispatch: Optional[Callable[[], None]],
) -> int:
    """Retry due preflights even if auto-pull was subsequently disabled."""

    list_preflights = getattr(db, "list_officer_job_preflights", None)
    if list_preflights is None:
        return 0
    rows = await list_preflights(
        project_id=project_id,
        officer_thread_id=officer_thread_id,
    )
    if not isinstance(rows, (list, tuple)):
        return 0
    activated = 0
    for row in rows:
        context = row.get("context") or {}
        if isinstance(context, str):
            import json

            try:
                context = json.loads(context)
            except (TypeError, ValueError):
                context = {}
        preflight = context.get("provisioning_preflight") or {}
        outcome = await ensure_officer_job_activated(
            db,
            row,
            provision=provision_repo,
            category=preflight.get("category") or context.get("work_category"),
            trigger_dispatch=trigger_dispatch,
        )
        if outcome.activated and outcome.attempted:
            activated += 1
    return activated


async def tick_officer(
    db: Any,
    vector_db: Any,
    officer_row: dict[str, Any],
    *,
    provision_repo: Optional[ProvisionFn] = None,
    trigger_dispatch: Optional[Callable[[], None]] = None,
    enforce_grants: Optional[GrantsFn] = None,
    usage_ledger: Any = None,
    notify: Any = None,
    now: Optional[datetime] = None,
    release_enabled: bool = False,
) -> dict[str, int]:
    """One officer's pass. Never raises — a bad post must not stop the fleet."""
    now = now or _now()
    liveness_policy = get_liveness_policy()
    counts = {"dispatched": 0, "skipped": 0, "breakers_opened": 0, "wakes": 0}

    thread_id = str(officer_row.get("id") or "")
    project_id = str(officer_row.get("project_id") or "")
    if not thread_id or not project_id:
        return counts

    meta = _officer_meta(officer_row)
    if officer_is_held(meta):
        logger.debug("officer backlog: %s held — skipping", thread_id[:8])
        return counts

    # An archived project takes no new work (§4.3 of
    # knowledge-base/knowledge/features/project_and_job_list_filtering.md).
    # THIS is the path the 2026-08-15 incident ran on: an officer commissioned
    # onto "Better Resavio (pre-split archive)" ran a full watch and dispatched
    # three workers against a historical record. Placed beside the hold
    # early-return because that is the established idiom for "this post is
    # standing down this tick" — and, like a hold, it skips and logs rather
    # than raising: a tick has no HTTP caller and a bad post must never stop
    # the fleet. Archiving also holds the officer (§4.5); this is the backstop
    # for a post held by neither.
    project = await db.get_project(project_id)
    if project_is_archived(project):
        logger.info(
            "officer=%s skip=project-archived project=%s",
            thread_id[:8],
            project_id[:8],
        )
        counts["skipped"] += 1
        return counts

    try:
        counts["dispatched"] += await _recover_officer_preflights(
            db,
            project_id=project_id,
            officer_thread_id=thread_id,
            provision_repo=provision_repo,
            trigger_dispatch=trigger_dispatch,
        )
    except Exception:
        logger.exception("officer backlog: provisioning recovery failed")

    # BP-07 recovery belongs to already-admitted, claim-holding work and must
    # continue through a disable/rollback. The deployment release fence gates
    # only *new* ticket admission, immediately before the ordinary auto-pull
    # predicate and every queue read.
    if not release_enabled or not auto_pull_enabled(meta) or vector_db is None:
        return counts

    pools = pools_from_meta(meta)
    if not pools:
        return counts

    if await _spend_exhausted(
        usage_ledger,
        project_id=project_id,
        ceiling=float(meta.get("worker_spend_ceiling_daily") or 0.0),
    ):
        logger.info("officer=%s skip=century-spend-ceiling", thread_id[:8])
        counts["skipped"] += 1
        return counts

    state = _officer_state(officer_row)
    lineage = await db.get_officer_capacity_lineage(thread_id)
    owner_user_id = officer_row.get("user_id") or officer_row.get("owner_user_id")

    state_patch: dict[str, Any] = {}
    breakers = dict(state.get("backlog_breakers") or {})
    refusals = dict(state.get("backlog_workspace_refusals") or {})

    # Stale claims: computed once for the whole post, recorded for the sitrep.
    open_claims = await db.list_stale_officer_claims(
        lineage,
        stale_before=now - timedelta(minutes=liveness_policy.stale_claim.minutes),
    )
    stalled = stale_claims(open_claims, now, policy=liveness_policy)
    state_patch["backlog_stale_claims"] = stalled
    state_patch["backlog_stale_claim_policy"] = {
        "threshold_minutes": liveness_policy.stale_claim.minutes,
        "threshold_source": liveness_policy.stale_claim.source,
    }
    for claim in stalled:
        if (
            claim["status"] == "pending_review"
            and claim["age_hours"] >= PENDING_REVIEW_PAGE_HOURS
            and notify is not None
        ):
            try:
                await notify(
                    db,
                    project_id,
                    source="backlog_stale_claim",
                    dedup_key=f"stale-claim:{claim['job_id']}",
                    payload=claim,
                )
                counts["wakes"] += 1
            except Exception:
                logger.warning(
                    "officer backlog: stale-claim page failed", exc_info=True
                )

    for pool, spec in sorted(pools.items()):
        category = str(spec["category"])
        if breaker_is_open(state, pool, now):
            logger.info("officer=%s pool=%s skip=breaker-open", thread_id[:8], pool)
            counts["skipped"] += 1
            continue

        # Breaker evaluation runs BEFORE the pull, on the pool's own history.
        terminal_history = await db.list_officer_distinct_terminal_outcomes(
            lineage, slot=pool, limit=BREAKER_FAILURES
        )
        tripped = evaluate_breaker(terminal_history, state, pool)
        if tripped:
            tripped["until"] = (
                now + timedelta(minutes=BREAKER_OPEN_MINUTES)
            ).isoformat()
            breakers[pool] = tripped
            state_patch["backlog_breakers"] = breakers
            counts["breakers_opened"] += 1
            logger.warning(
                "officer=%s pool=%s breaker=opened cause=%s",
                thread_id[:8],
                pool,
                tripped["cause"],
            )
            continue

        slot_ceiling = float(spec.get("spend_ceiling_daily") or 0.0)
        if slot_ceiling:
            pool_job_ids = [
                str(job["id"])
                for job in await db.list_officer_slot_claims(
                    lineage, slot=pool, include_terminal=True, limit=None
                )
            ]
            if await _spend_exhausted(
                usage_ledger,
                project_id=project_id,
                ceiling=slot_ceiling,
                ref_ids=pool_job_ids,
            ):
                logger.info(
                    "officer=%s pool=%s skip=slot-spend-ceiling", thread_id[:8], pool
                )
                counts["skipped"] += 1
                continue

        floor = int(spec.get("count") or 0)
        scan = await _scan_eligible_tickets(
            db,
            vector_db,
            project_id,
            category,
            now,
            minimum=max(floor, 1),
        )
        if scan.unavailable:
            logger.warning(
                "officer=%s pool=%s skip=backlog-unavailable reason=%s",
                thread_id[:8],
                pool,
                scan.error,
            )
            counts["skipped"] += 1
            continue
        ready = scan.tickets
        for note in scan.notes:
            logger.info("officer=%s pool=%s skip=%s", thread_id[:8], pool, note)

        # Floor = the pool's own slot count (§13.2): if every agent in the pool
        # lands at once, each must find a ticket waiting. The floor therefore
        # scales with the kit rather than needing its own knob.
        if scan.exhausted and len(ready) < floor:
            try:
                outcome = await db.queue_officer_floor_wake(
                    project_id,
                    expected_thread_id=thread_id,
                    pool=pool,
                    payload={
                        "pool": pool,
                        "category": category,
                        "ready": len(ready),
                        "floor": floor,
                    },
                    policy_debounce_seconds=FLOOR_WAKE_DEBOUNCE_HOURS * 3600,
                    now=now,
                    notifier=notify,
                )
                if isinstance(outcome, dict) and outcome.get("queued"):
                    counts["wakes"] += 1
            except Exception:
                logger.warning("officer backlog: floor wake failed", exc_info=True)
        elif scan.exhausted:
            try:
                await db.resolve_officer_floor_wake_retry(
                    project_id,
                    expected_thread_id=thread_id,
                    pool=pool,
                )
            except Exception:
                logger.warning(
                    "officer backlog: floor wake recovery-state update failed",
                    exc_info=True,
                )

        if not ready:
            continue

        ticket = ready[0]
        if category == EXECUTOR:
            blocked = await _executor_blocked(
                db,
                vector_db,
                capacity_lineage=lineage,
                project_id=project_id,
                ticket_is_parallel_safe=ticket["classification"].parallel_safe,
            )
            if blocked:
                logger.info("officer=%s pool=%s skip=%s", thread_id[:8], pool, blocked)
                counts["skipped"] += 1
                continue

        try:
            job = await _dispatch_one(
                db,
                officer_thread_id=thread_id,
                project_id=project_id,
                owner_user_id=str(owner_user_id) if owner_user_id else None,
                pool=pool,
                category=category,
                ticket=ticket,
                provision_repo=provision_repo,
                trigger_dispatch=trigger_dispatch,
                enforce_grants=enforce_grants,
            )
        except WorkspaceRefused as exc:
            # The Project's or installation's workspace defaults can't be
            # resolved. Nothing ran; record why for the sitrep and the post.
            logger.warning(
                "officer=%s pool=%s skip=workspace-refused ticket=%s: %s",
                thread_id[:8],
                pool,
                ticket.get("note_id"),
                exc,
            )
            refusals[pool] = {
                "reason": str(exc),
                "ticket": ticket.get("note_id"),
                "at": now.isoformat(),
            }
            state_patch["backlog_workspace_refusals"] = refusals
            counts["skipped"] += 1
            continue
        except Exception:
            # Includes the unique-index refusal of a racing double-claim. Not a
            # job failure: nothing ran, so the breaker stays shut.
            logger.warning(
                "officer=%s pool=%s skip=dispatch-failed ticket=%s",
                thread_id[:8],
                pool,
                ticket.get("note_id"),
                exc_info=True,
            )
            counts["skipped"] += 1
            continue

        if job is None:
            counts["skipped"] += 1
            continue
        if refusals.pop(pool, None) is not None:
            state_patch["backlog_workspace_refusals"] = refusals
        counts["dispatched"] += 1
        logger.info(
            "officer=%s pool=%s dispatched=%s/%s",
            thread_id[:8],
            pool,
            ticket.get("note_id"),
            str(job.get("id"))[:8],
        )

    if state_patch:
        try:
            await db.merge_thread_officer_state(thread_id, state_patch)
        except Exception:
            logger.warning(
                "officer backlog: officer_state merge failed for %s",
                thread_id[:8],
                exc_info=True,
            )

    return counts


async def officer_backlog_tick_once(
    db: Any,
    vector_db: Any,
    *,
    provision_repo: Optional[ProvisionFn] = None,
    trigger_dispatch: Optional[Callable[[], None]] = None,
    enforce_grants: Optional[GrantsFn] = None,
    usage_ledger: Any = None,
    notify: Any = None,
    now: Optional[datetime] = None,
    release_enabled: bool = False,
) -> dict[str, int]:
    """One tick across every commissioned officer. Errors isolate per officer."""
    totals = {"dispatched": 0, "skipped": 0, "breakers_opened": 0, "wakes": 0}
    try:
        officers = await db.list_commissioned_officer_posts_for_backlog()
    except Exception:
        logger.exception("officer backlog: officer listing failed")
        return totals

    for officer_row in officers:
        try:
            counts = await tick_officer(
                db,
                vector_db,
                officer_row,
                provision_repo=provision_repo,
                trigger_dispatch=trigger_dispatch,
                enforce_grants=enforce_grants,
                usage_ledger=usage_ledger,
                notify=notify,
                now=now,
                release_enabled=release_enabled,
            )
        except Exception:
            logger.exception(
                "officer backlog: tick failed for officer %s (continuing)",
                str(officer_row.get("id"))[:8],
            )
            continue
        for key, value in counts.items():
            totals[key] += value
    return totals


async def officer_backlog_tick_loop(
    db: Any,
    vector_db: Any,
    shutdown_event: asyncio.Event,
    *,
    provision_repo: Optional[ProvisionFn] = None,
    trigger_dispatch: Optional[Callable[[], None]] = None,
    enforce_grants: Optional[GrantsFn] = None,
    usage_ledger: Any = None,
    notify: Any = None,
    release_enabled: bool = False,
) -> None:
    """The ~60s tick, mounted leader-gated from main.py's lifespan."""
    stale_policy = get_liveness_policy().stale_claim
    logger.info(
        "Officer backlog tick started (tick=%ds, stale_claim=%.1fh, breaker=%.0fm)",
        TICK_SECONDS,
        stale_policy.minutes / 60.0,
        BREAKER_OPEN_MINUTES,
    )
    while not shutdown_event.is_set():
        try:
            counts = await officer_backlog_tick_once(
                db,
                vector_db,
                provision_repo=provision_repo,
                trigger_dispatch=trigger_dispatch,
                enforce_grants=enforce_grants,
                usage_ledger=usage_ledger,
                notify=notify,
                release_enabled=release_enabled,
            )
            if any(counts.values()):
                logger.info("Officer backlog tick: %s", counts)
        except Exception:
            logger.exception("Officer backlog tick raised; will retry next tick")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=TICK_SECONDS)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Officer backlog tick stopped")


__all__ = [
    "BREAKER_FAILURES",
    "BREAKER_OPEN_MINUTES",
    "FLOOR_WAKE_DEBOUNCE_HOURS",
    "PENDING_REVIEW_PAGE_HOURS",
    "STALE_CLAIM_HOURS",
    "TICK_SECONDS",
    "auto_pull_enabled",
    "breaker_is_open",
    "eligible_tickets",
    "evaluate_breaker",
    "officer_backlog_tick_loop",
    "officer_backlog_tick_once",
    "pools_from_meta",
    "stale_claims",
    "tick_officer",
]
