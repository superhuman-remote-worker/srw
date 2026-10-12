"""Server-side Job Bench runs, scheduling, and report computation.

The v0 scripts under :mod:`bench` kept their manifest on the invoking laptop.
This module moves that manifest into ``bench_runs.state`` and performs one
small, restart-safe sweep at a time.  HTTP concerns live in
``routers/bench.py``; all database and measurement behavior stays here so it
can be exercised without a FastAPI app or a live cluster.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import os
import random
import statistics
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from orchestrator.services.config_overrides import (
    refuse_caller_transport_keys,
    refuse_execution_owned_workspace_keys,
)
from orchestrator.services.deliverable_gate import parse_required_deliverables
from orchestrator.services.job_todos import build_archive_listing
from shared.runtime.core.loader import canonical_config_name, deep_merge

logger = logging.getLogger(__name__)

TERMINAL_JOB_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "pending_review", "waiting_for_reply"}
)
TERMINAL_RUN_STATUSES = frozenset({"done", "cancelled"})
TAIL_ANOMALY_FRACTION = 0.15
SWEEP_SECONDS = int(os.getenv("BENCH_SWEEP_SECONDS", "30"))
# Cross-replica sweep claim, hashed server-side via hashtext(). Mirrored in
# orchestrator/database/lock_ids.py for the collision audit.
BENCH_SWEEP_LOCK = "bench_sweep"

CreateJobFn = Callable[
    [dict[str, Any], dict[str, Any], dict[str, Any], int],
    Awaitable[dict[str, Any]],
]
CancelJobFn = Callable[[str], Awaitable[Any]]
ResolveJobRepoFn = Callable[[str], Awaitable[tuple[str, str | None]]]


class BenchStateError(RuntimeError):
    """The requested run transition is not valid for its current state."""


def _decode_json(value: Any, fallback: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return copy.deepcopy(fallback)
    return value if value is not None else copy.deepcopy(fallback)


def _record_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for key in ("id", "created_by"):
        if result.get(key) is not None:
            result[key] = str(result[key])
    result["spec"] = _decode_json(result.get("spec"), {})
    result["state"] = _decode_json(result.get("state"), [])
    return result


class BenchStore:
    """Small SQL repository over the app database.

    Keeping these queries next to the service avoids widening the already
    large ``PostgresDB`` class for a self-contained component.  ``db`` only
    needs to expose the existing ``acquire()`` async context manager.
    """

    def __init__(self, db: Any):
        self.db = db

    async def create_run(
        self, *, name: str, created_by: str, spec: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO bench_runs (name, created_by, spec, state)
                VALUES ($1, $2, $3::jsonb, '[]'::jsonb)
                RETURNING id, name, status, created_at, created_by, spec, state
                """,
                name,
                UUID(created_by),
                json.dumps(spec),
            )
        return _record_dict(row) or {}

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        try:
            run_uuid = UUID(run_id)
        except (TypeError, ValueError, AttributeError):
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, name, status, created_at, created_by, spec, state
                  FROM bench_runs
                 WHERE id = $1
                """,
                run_uuid,
            )
        return _record_dict(row)

    async def list_runs(
        self, *, created_by: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if created_by is not None:
            params.append(UUID(created_by))
            where = "WHERE created_by = $1"
        params.append(limit)
        limit_param = len(params)
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, name, status, created_at, created_by, spec, state
                  FROM bench_runs
                  {where}
                 ORDER BY created_at DESC
                 LIMIT ${limit_param}
                """,
                *params,
            )
        return [_record_dict(row) or {} for row in rows]

    async def list_running_runs(self) -> list[dict[str, Any]]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, status, created_at, created_by, spec, state
                  FROM bench_runs
                 WHERE status = 'running'
                 ORDER BY created_at, id
                """
            )
        return [_record_dict(row) or {} for row in rows]

    @asynccontextmanager
    async def try_sweep_lock(self):
        """Claim the cross-replica sweeper tick; yields False when another
        replica holds it.

        Session-scoped (``pg_try_advisory_lock``), not xact-scoped: the tick
        spans many independent statements on other pool connections, so the
        claim must outlive any single transaction. Claim and release run on
        the SAME held connection — ``PostgresDB.fetchval`` acquires a fresh
        pooled connection per call, which would unlock a different session.
        asyncpg's pool reset backstops a lost release when the connection is
        returned. See knowledge-base/knowledge/issues/bench_sweeper_multi_replica_race.md.
        """
        async with self.db.acquire() as conn:
            claimed = bool(
                await conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtext($1))", BENCH_SWEEP_LOCK
                )
            )
            try:
                yield claimed
            finally:
                if claimed:
                    try:
                        await conn.fetchval(
                            "SELECT pg_advisory_unlock(hashtext($1))",
                            BENCH_SWEEP_LOCK,
                        )
                    except Exception:  # pragma: no cover — session already gone
                        logger.warning(
                            "Bench sweep lock release failed; the session's "
                            "end frees it"
                        )

    async def save_run(
        self,
        run_id: str,
        state: list[dict[str, Any]],
        *,
        status: str | None = None,
        require_status: str | None = None,
    ) -> dict[str, Any] | None:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE bench_runs
                   SET state = $2::jsonb,
                       status = COALESCE($3::text, status)
                 WHERE id = $1
                   AND ($4::text IS NULL OR status = $4::text)
                RETURNING id, name, status, created_at, created_by, spec, state
                """,
                UUID(run_id),
                json.dumps(state),
                status,
                require_status,
            )
        return _record_dict(row)

    async def set_status(
        self,
        run_id: str,
        status: str,
        *,
        require_status: str | None = None,
    ) -> dict[str, Any] | None:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE bench_runs
                   SET status = $2
                 WHERE id = $1
                   AND ($3::text IS NULL OR status = $3::text)
                RETURNING id, name, status, created_at, created_by, spec, state
                """,
                UUID(run_id),
                status,
                require_status,
            )
        return _record_dict(row)

    async def list_tagged_jobs(self, run_id: str) -> list[dict[str, Any]]:
        """Return every surviving job tagged for ``run_id``.

        This is more than a status fetch: it closes the create→ledger crash
        window.  If the orchestrator died after ``jobs`` committed but before
        ``bench_runs.state`` did, the next tick reconstructs the missing entry
        from ``context.bench`` instead of submitting a duplicate.
        """

        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, status, created_at, context
                  FROM jobs
                 WHERE context->'bench'->>'run_id' = $1
                 ORDER BY created_at, id
                """,
                run_id,
            )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["id"] = str(item["id"])
            item["context"] = _decode_json(item.get("context"), {})
            result.append(item)
        return result


def freeze_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Return the complete, JSON-safe run definition persisted at creation.

    Tasks are already inline at the API boundary.  This final normalization
    resolves defaults, legacy config aliases, and deliverable spelling so no
    later read of ``bench/tasks.yaml`` (or any other task registry) is needed.
    """

    tasks: list[dict[str, Any]] = []
    for raw in spec.get("tasks") or []:
        task = dict(raw)
        # Bench arms/tasks reach db.create_job through admit_job (main
        # ``_create_bench_job``), never through the POST /api/jobs adapter, so
        # the transport fence runs here at the run-creation write boundary.
        refuse_caller_transport_keys(task.get("config_override"))
        refuse_execution_owned_workspace_keys(task.get("config_override"))
        frozen_task: dict[str, Any] = {
            "id": str(task["id"]),
            "description": str(task["description"]),
            "required_deliverables": parse_required_deliverables(
                task.get("required_deliverables")
            ),
            "config_name": canonical_config_name(
                str(task.get("config_name") or "worker_base")
            ),
            "config_override": copy.deepcopy(task.get("config_override") or {}),
            "priority": int(task.get("priority", 5)),
        }
        if task.get("family") is not None:
            frozen_task["family"] = str(task["family"])
        tasks.append(frozen_task)

    arms: list[dict[str, Any]] = []
    for raw in spec.get("arms") or []:
        arm = dict(raw)
        refuse_caller_transport_keys(arm.get("config_override"))
        refuse_execution_owned_workspace_keys(arm.get("config_override"))
        frozen_arm: dict[str, Any] = {
            "name": str(arm["name"]),
            "config_override": copy.deepcopy(arm.get("config_override") or {}),
            "model": str(arm["model"]),
        }
        if arm.get("config_name"):
            frozen_arm["config_name"] = canonical_config_name(str(arm["config_name"]))
        if arm.get("expert_id"):
            frozen_arm["expert_id"] = str(arm["expert_id"])
        if arm.get("project_id"):
            frozen_arm["project_id"] = str(arm["project_id"])
        if arm.get("execution_lane"):
            frozen_arm["execution_lane"] = str(arm["execution_lane"])
        arms.append(frozen_arm)

    project_id = spec.get("project_id")
    return {
        "version": 1,
        "tasks": tasks,
        "replicates": int(spec.get("replicates", 1)),
        "max_in_flight": int(spec.get("max_in_flight", 1)),
        "arms": arms,
        "project_id": str(project_id) if project_id else None,
    }


async def create_bench_run(
    store: BenchStore,
    *,
    name: str,
    created_by: str,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze and persist a new running bench run."""

    return await store.create_run(
        name=name,
        created_by=created_by,
        spec=freeze_spec(spec),
    )


def _submission_key(task: str, arm: str, replicate: int) -> tuple[str, str, int]:
    return task, arm, replicate


def build_submission_queue(run: Mapping[str, Any]) -> list[tuple[dict, dict, int]]:
    """Build the deterministic not-yet-submitted queue for one run.

    Each replicate gets its own task shuffle using the exact v0 seed shape,
    ``f"{run_id}:{replicate}"``.  Arms are adjacent within the shuffled task
    wave, which interleaves A/B work instead of running one complete arm first.
    """

    run_id = str(run["id"])
    spec = _decode_json(run.get("spec"), {})
    state = _decode_json(run.get("state"), [])
    submitted = {
        _submission_key(
            str(entry.get("task")),
            str(entry.get("arm")),
            int(entry.get("replicate")),
        )
        for entry in state
        if entry.get("job_id")
        and entry.get("task") is not None
        and entry.get("arm") is not None
        and entry.get("replicate") is not None
    }

    queue: list[tuple[dict, dict, int]] = []
    tasks = [dict(task) for task in spec.get("tasks") or []]
    arms = [dict(arm) for arm in spec.get("arms") or []]
    for replicate in range(1, int(spec.get("replicates", 1)) + 1):
        wave = list(tasks)
        random.Random(f"{run_id}:{replicate}").shuffle(wave)
        for task in wave:
            for arm in arms:
                key = _submission_key(str(task["id"]), str(arm["name"]), replicate)
                if key not in submitted:
                    queue.append((task, arm, replicate))
    return queue


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif value:
        dt = parse_timestamp(value)
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


async def refresh_run_ledger(
    store: BenchStore, run: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Refresh statuses and reconstruct any create→ledger crash-window jobs."""

    run_id = str(run["id"])
    spec = _decode_json(run.get("spec"), {})
    state = [dict(entry) for entry in _decode_json(run.get("state"), [])]
    task_by_id = {str(task["id"]): task for task in (spec.get("tasks") or [])}
    by_job_id = {str(entry["job_id"]): entry for entry in state if entry.get("job_id")}

    for job in await store.list_tagged_jobs(run_id):
        context = _decode_json(job.get("context"), {})
        bench = context.get("bench") if isinstance(context, dict) else None
        if not isinstance(bench, dict) or str(bench.get("run_id")) != run_id:
            continue
        task_id = str(bench.get("task") or "")
        arm_name = str(bench.get("arm") or "")
        try:
            replicate = int(bench.get("replicate"))
        except (TypeError, ValueError):
            logger.warning("Bench job %s has an invalid replicate tag", job.get("id"))
            continue
        if not task_id or not arm_name or replicate < 1:
            logger.warning("Bench job %s has an incomplete bench tag", job.get("id"))
            continue

        job_id = str(job["id"])
        entry = by_job_id.get(job_id)
        if entry is None:
            task = task_by_id.get(task_id) or {}
            entry = {
                "task": task_id,
                "arm": arm_name,
                "replicate": replicate,
                "job_id": job_id,
                "submitted_at": _iso(job.get("created_at")),
                "final_status": None,
            }
            if task.get("family") is not None:
                entry["family"] = task["family"]
            state.append(entry)
            by_job_id[job_id] = entry

        status = str(job.get("status") or "unknown")
        entry["last_status"] = status
        if entry.get("final_status") is None and status in TERMINAL_JOB_STATUSES:
            entry["final_status"] = status
            entry["finished_at"] = datetime.now(timezone.utc).isoformat()

    return state


def _in_flight_count(state: Sequence[Mapping[str, Any]]) -> int:
    return sum(
        1
        for entry in state
        if entry.get("job_id") and entry.get("final_status") is None
    )


async def sweep_run(
    store: BenchStore,
    run: Mapping[str, Any],
    create_job_fn: CreateJobFn,
) -> int:
    """Refresh and top up one running run; return jobs created this tick."""

    if run.get("status") != "running":
        return 0

    run_id = str(run["id"])
    spec = _decode_json(run.get("spec"), {})
    state = await refresh_run_ledger(store, run)
    persisted = await store.save_run(run_id, state, require_status="running")
    if persisted is None:
        return 0

    queue = build_submission_queue({**persisted, "state": state})
    max_in_flight = int(spec.get("max_in_flight", 1))
    in_flight = _in_flight_count(state)
    created = 0

    while queue and in_flight < max_in_flight:
        task, arm, replicate = queue.pop(0)
        job = await create_job_fn(persisted, task, arm, replicate)
        job_id = job.get("id") or job.get("job_id")
        if not job_id:
            raise RuntimeError("Bench job creation returned no job id")
        status = str(job.get("status") or "created")
        final_status = status if status in TERMINAL_JOB_STATUSES else None
        entry: dict[str, Any] = {
            "task": str(task["id"]),
            "arm": str(arm["name"]),
            "replicate": replicate,
            "job_id": str(job_id),
            "submitted_at": _iso(job.get("created_at")),
            "last_status": status,
            "final_status": final_status,
        }
        if task.get("family") is not None:
            entry["family"] = task["family"]
        if final_status is not None:
            entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        state.append(entry)
        created += 1
        if final_status is None:
            in_flight += 1

        persisted = await store.save_run(run_id, state, require_status="running")
        if persisted is None:
            # Cancellation won the race after job creation.  Preserve the
            # breadcrumb without reviving the run; the cancellation path can
            # then see and stop this job.
            await store.save_run(run_id, state)
            return created

    if not queue and in_flight == 0:
        await store.save_run(
            run_id,
            state,
            status="done",
            require_status="running",
        )
    return created


@asynccontextmanager
async def _try_sweep_lock(store: Any):
    """Claim the tick, tolerating narrow test stores without the method."""
    claim = getattr(type(store), "try_sweep_lock", None)
    if claim is None:
        yield True
        return
    async with store.try_sweep_lock() as claimed:
        yield bool(claimed)


async def sweep_tick(store: BenchStore, create_job_fn: CreateJobFn) -> int:
    """Sweep every running run once, isolating failures per run.

    Cross-replica guard: only the replica that claims the advisory lock
    sweeps; the loser skips the whole tick and retries next tick. Two
    orchestrator replicas both ran this loop and double-submitted
    (task, arm, replicate) pairs 2-5 ms apart — see
    knowledge-base/knowledge/issues/bench_sweeper_multi_replica_race.md.
    """

    async with _try_sweep_lock(store) as claimed:
        if not claimed:
            logger.debug("Bench sweep tick skipped: another replica holds the lock")
            return 0

        created = 0
        for run in await store.list_running_runs():
            try:
                created += await sweep_run(store, run, create_job_fn)
            except Exception:
                logger.exception("Bench run %s sweep failed", run.get("id"))
        return created


async def bench_sweeper_loop(
    store: BenchStore,
    shutdown_event: asyncio.Event,
    *,
    create_job_fn: CreateJobFn,
) -> None:
    """Resume and maintain running bench runs until shutdown."""

    logger.info("Job Bench sweeper started (tick=%ds)", SWEEP_SECONDS)
    while not shutdown_event.is_set():
        try:
            created = await sweep_tick(store, create_job_fn)
            if created:
                logger.info("Job Bench sweeper submitted %d job(s)", created)
        except Exception:
            logger.exception("Job Bench sweeper tick raised; retrying next tick")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=SWEEP_SECONDS)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Job Bench sweeper stopped")


def build_bench_job_payload(
    run: Mapping[str, Any],
    task: Mapping[str, Any],
    arm: Mapping[str, Any],
    replicate: int,
) -> dict[str, Any]:
    """Build the regular ``JobCreate`` payload for one queue entry."""

    spec = _decode_json(run.get("spec"), {})
    override = deep_merge(
        dict(task.get("config_override") or {}),
        dict(arm.get("config_override") or {}),
    )
    override = deep_merge(
        override,
        {
            "llm": {"model": str(arm["model"])},
            "autonomy": "full",
        },
    )

    task_id = str(task["id"])
    arm_name = str(arm["name"])
    context = {
        "bench": {
            "run_id": str(run["id"]),
            "task": task_id,
            "arm": arm_name,
            "replicate": replicate,
        }
    }
    required_deliverables = parse_required_deliverables(
        task.get("required_deliverables")
    )

    expert_id = str(arm["expert_id"]) if arm.get("expert_id") else None
    config_name = canonical_config_name(
        str(arm.get("config_name") or task.get("config_name") or "worker_base")
    )
    if expert_id:
        # DB-backed experts use worker_base as their structural base, matching
        # the public create-job contract.
        config_name = "worker_base"
    project_id = arm.get("project_id") or spec.get("project_id")

    return {
        "description": str(task["description"]),
        "config_name": config_name,
        "expert_id": expert_id,
        "config_override": override,
        "context": context,
        "required_deliverables": required_deliverables or None,
        "user_id": str(run["created_by"]),
        "project_id": str(project_id) if project_id else None,
        # Bench replicas must be reproducible. Never let an omitted selection
        # resolve against ambient auto-attach preferences that can change
        # between arms or replicas. A future connector-aware bench contract
        # must freeze IDs/revisions once in the run spec instead.
        "datasource_ids": [],
        "priority": int(task.get("priority", 5)),
        # Frozen at run creation like every other arm field, so replicates of
        # one arm cannot drift across lanes mid-run. None keeps the JobCreate
        # default rather than asserting "pinned", so omitting the field leaves
        # historical single-lane specs byte-identical in behaviour.
        "execution_lane": arm.get("execution_lane"),
    }


async def cancel_bench_run(
    store: BenchStore,
    run: Mapping[str, Any],
    *,
    cancel_job_fn: CancelJobFn,
) -> dict[str, Any]:
    """Stop future submissions and best-effort cancel every live member job."""

    status = str(run.get("status"))
    if status == "done":
        raise BenchStateError("A completed bench run cannot be cancelled")
    if status == "cancelled":
        return dict(run)

    run_id = str(run["id"])
    claimed = await store.set_status(run_id, "cancelled", require_status=status)
    if claimed is None:
        current = await store.get_run(run_id)
        if current is None:
            raise BenchStateError("Bench run disappeared during cancellation")
        if current.get("status") == "done":
            raise BenchStateError("A completed bench run cannot be cancelled")
        claimed = current

    state = [dict(entry) for entry in _decode_json(claimed.get("state"), [])]
    for entry in state:
        if not entry.get("job_id") or entry.get("final_status") is not None:
            continue
        try:
            await cancel_job_fn(str(entry["job_id"]))
            entry["last_status"] = "cancelled"
            entry["final_status"] = "cancelled"
            entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        except Exception:
            logger.exception(
                "Bench run %s: failed to cancel job %s",
                run_id,
                entry.get("job_id"),
            )
    return await store.save_run(run_id, state, status="cancelled") or {
        **claimed,
        "state": state,
        "status": "cancelled",
    }


def parse_timestamp(value: Any) -> datetime:
    """Parse API/archive timestamps; archive filenames are UTC when naive."""

    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _first_int(*values: Any) -> int:
    for value in values:
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return 0


async def fetch_main_requests(audit_reader: Any, job_id: str) -> list[dict[str, Any]]:
    """Read all main-loop requests through the endpoint's audit-reader seam."""

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        data = await audit_reader.list_llm_requests(
            job_id,
            limit=100,
            offset=offset,
            call_type="main",
        )
        items = data.get("requests") or data.get("items") or data.get("entries") or []
        for item in items:
            usage = (
                item.get("token_usage")
                or item.get("usage")
                or (item.get("metrics") or {}).get("token_usage")
                or {}
            )
            timestamp = item.get("timestamp") or item.get("created_at")
            if not timestamp:
                continue
            rows.append(
                {
                    "ts": parse_timestamp(timestamp),
                    "input_tokens": _first_int(
                        usage.get("prompt_tokens"),
                        usage.get("input_tokens"),
                        item.get("input_tokens"),
                    ),
                    "output_tokens": _first_int(
                        usage.get("completion_tokens"),
                        usage.get("output_tokens"),
                        item.get("output_tokens"),
                    ),
                    "latency_ms": _first_int(item.get("latency_ms")),
                    # Provider-side prefix-cache reads. Two shapes carry the
                    # same number: LangChain's usage_metadata and the OpenAI
                    # token_usage detail block, so read whichever this
                    # provider filled. Absent (0) is indistinguishable from a
                    # genuine cold call here, which is why cold_calls below
                    # only counts requests that did report input tokens.
                    "cache_read_tokens": _first_int(
                        (
                            (
                                item.get("usage_metadata")
                                or (item.get("metrics") or {}).get("usage_metadata")
                                or {}
                            ).get("input_token_details")
                            or {}
                        ).get("cache_read"),
                        (usage.get("prompt_tokens_details") or {}).get("cached_tokens"),
                    ),
                }
            )
        has_more = bool(data.get("hasMore") or data.get("has_more"))
        if not has_more and len(items) < 100:
            break
        if not items:
            break
        offset += len(items)
    rows.sort(key=lambda row: row["ts"])
    return rows


async def fetch_claim_timings(audit_reader: Any, job_id: str) -> list[dict[str, Any]]:
    """Read stateless worker claim timing rows through the audit seam."""

    rows = await audit_reader.list_claim_timings(job_id)
    return [dict(row) for row in rows]


async def fetch_phase_events(
    gitea_client: Any,
    resolve_job_repo: ResolveJobRepoFn,
    job_id: str,
    *,
    not_before: datetime | None,
) -> list[tuple[str, datetime]]:
    """Read strategic/tactical archive closures through the Gitea seam."""

    if hasattr(gitea_client, "is_initialized") and not gitea_client.is_initialized:
        return []
    repo_name, branch = await resolve_job_repo(job_id)
    entries = await gitea_client.list_contents(repo_name, "archive", ref=branch)
    events: list[tuple[str, datetime]] = []
    for entry in build_archive_listing(entries):
        phase_name = str(entry.get("phase_name") or entry.get("filename") or "")
        if "strategic" in phase_name:
            phase_type = "S"
        elif "tactical" in phase_name:
            phase_type = "T"
        else:
            continue
        if not entry.get("timestamp"):
            continue
        timestamp = parse_timestamp(entry["timestamp"])
        if not_before is not None and timestamp < not_before:
            continue
        events.append((phase_type, timestamp))
    events.sort(key=lambda event: event[1])
    return events


def _share(strategic: int | float, tactical: int | float) -> float | None:
    total = strategic + tactical
    return round(100 * strategic / total, 1) if total else None


def _median_number(values: Sequence[int | float]) -> int | float:
    value = statistics.median(values)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _nonnegative_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, number) if math.isfinite(number) else 0.0


def analyze_claim_metrics(
    claim_timings: Sequence[Mapping[str, Any]],
    *,
    job_wall_seconds: float | None,
) -> dict[str, Any]:
    """Compute setup and cross-pod handoff overhead from append-only rows."""

    if not claim_timings:
        return {
            "claims": None,
            "setup_s": None,
            "handoff_dead_s": None,
            "overhead_pct": None,
            "mcp_attached": None,
        }

    rows = [dict(row) for row in claim_timings]

    def payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
        value = row.get("payload")
        return value if isinstance(value, Mapping) else {}

    def timestamp(row: Mapping[str, Any]) -> datetime:
        raw = row.get("timestamp") or payload(row).get("claimed_at")
        try:
            return parse_timestamp(raw)
        except (TypeError, ValueError):
            return datetime.max.replace(tzinfo=timezone.utc)

    rows.sort(key=timestamp)
    setup_seconds = sum(
        _nonnegative_float(claim_payload.get(segment))
        for row in rows
        for claim_payload in (payload(row),)
        for segment in ("bundle", "preflight", "agent_start")
    )
    handoff_seconds = 0.0
    for previous, successor in zip(rows, rows[1:]):
        previous_released = payload(previous).get("released_at")
        successor_claimed = payload(successor).get("claimed_at")
        if not previous_released or not successor_claimed:
            continue
        try:
            gap = (
                parse_timestamp(successor_claimed) - parse_timestamp(previous_released)
            ).total_seconds()
        except (TypeError, ValueError):
            continue
        # Different pods can have slightly skewed clocks. A negative handoff
        # is measurement noise, never negative overhead.
        handoff_seconds += max(0.0, gap)

    overhead_pct = None
    if job_wall_seconds is not None and job_wall_seconds > 0:
        overhead_pct = round(
            100.0 * (setup_seconds + handoff_seconds) / job_wall_seconds,
            3,
        )
    return {
        "claims": len(rows),
        "setup_s": round(setup_seconds, 3),
        "handoff_dead_s": round(handoff_seconds, 3),
        "overhead_pct": overhead_pct,
        "mcp_attached": any(payload(row).get("mcp_attached") is True for row in rows),
    }


def analyze_job_metrics(
    *,
    job_id: str,
    status: str | None,
    requests: Sequence[Mapping[str, Any]],
    phase_events: Sequence[tuple[str, datetime]],
    claim_timings: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Port the v0 request/archive segmentation into a pure calculation."""

    normalized_requests = sorted(
        (dict(request) for request in requests), key=lambda request: request["ts"]
    )
    result: dict[str, Any] = {
        "job_id": job_id,
        "status": status,
        "classification": "job" if normalized_requests else "infra",
        "request_count": len(normalized_requests),
        "n_req": len(normalized_requests),
        "tail_anomaly": False,
        "tail_requests": 0,
        "tail_req": 0,
    }
    if not normalized_requests:
        result.update(analyze_claim_metrics(claim_timings, job_wall_seconds=None))
        return result

    t0 = normalized_requests[0]["ts"]
    t1 = normalized_requests[-1]["ts"]
    job_wall_seconds = max(0.0, (t1 - t0).total_seconds())
    input_tokens = [
        int(request.get("input_tokens") or 0) for request in normalized_requests
    ]
    output_tokens = [
        int(request.get("output_tokens") or 0) for request in normalized_requests
    ]
    cache_reads = [
        int(request.get("cache_read_tokens") or 0) for request in normalized_requests
    ]
    result.update(
        {
            "wall_minutes": round((t1 - t0).total_seconds() / 60, 3),
            "wall_min": round((t1 - t0).total_seconds() / 60, 3),
            "input_tokens": sum(input_tokens),
            "output_tokens": sum(output_tokens),
            "median_prompt_tokens": _median_number(input_tokens),
            "med_in": _median_number(input_tokens),
            "cache_read_tokens": sum(cache_reads),
            "cache_hit_pct": (
                round(100.0 * sum(cache_reads) / sum(input_tokens), 1)
                if sum(input_tokens)
                else None
            ),
            # The load-bearing lane metric. A rotation that discarded the
            # provider prefix cache would re-pay full input on the first call
            # of every batch, so cold calls -- not the hit ratio -- is what
            # separates "the handoff is cheap" from "the handoff re-reads
            # everything". Counted only over requests that reported input
            # tokens so a provider that omits cache detail reads as unknown
            # rather than as universally cold.
            "cold_calls": sum(
                1
                for tokens, cached in zip(input_tokens, cache_reads)
                if tokens > 0 and cached == 0
            ),
            "total_latency_ms": sum(
                int(request.get("latency_ms") or 0) for request in normalized_requests
            ),
            "strategic_archives": sum(1 for typ, _ in phase_events if typ == "S"),
            "tactical_archives": sum(1 for typ, _ in phase_events if typ == "T"),
            "arch_S": sum(1 for typ, _ in phase_events if typ == "S"),
            "arch_T": sum(1 for typ, _ in phase_events if typ == "T"),
        }
    )
    result.update(
        analyze_claim_metrics(
            claim_timings,
            job_wall_seconds=job_wall_seconds,
        )
    )
    if not phase_events:
        result.update(
            {
                "strategic_share_latency_pct": None,
                "strategic_share_prompt_tokens_pct": None,
                "strategic_share_requests_pct": None,
                "strat_share_llm": None,
                "strat_share_intok": None,
                "strat_share_req": None,
                "strat_share": None,
            }
        )
        return result

    events = sorted(phase_events, key=lambda event: event[1])
    accum = {
        "S": {"latency": 0, "requests": 0, "tokens": 0},
        "T": {"latency": 0, "requests": 0, "tokens": 0},
    }
    request_index = 0
    for phase_type, closed_at in events:
        while (
            request_index < len(normalized_requests)
            and normalized_requests[request_index]["ts"] <= closed_at
        ):
            request = normalized_requests[request_index]
            accum[phase_type]["latency"] += int(request.get("latency_ms") or 0)
            accum[phase_type]["requests"] += 1
            accum[phase_type]["tokens"] += int(request.get("input_tokens") or 0)
            request_index += 1

    # Every request after the last archive is the final strategic wrap.  This
    # is the subtle v0 rule that prevents a long wrap from disappearing from
    # ceremony share simply because it never emits another closing archive.
    while request_index < len(normalized_requests):
        request = normalized_requests[request_index]
        accum["S"]["latency"] += int(request.get("latency_ms") or 0)
        accum["S"]["requests"] += 1
        accum["S"]["tokens"] += int(request.get("input_tokens") or 0)
        request_index += 1

    tail_requests = sum(
        1 for request in normalized_requests if request["ts"] > events[-1][1]
    )
    latency_share = _share(accum["S"]["latency"], accum["T"]["latency"])
    request_share = _share(accum["S"]["requests"], accum["T"]["requests"])
    token_share = _share(accum["S"]["tokens"], accum["T"]["tokens"])
    result.update(
        {
            "strategic_share_latency_pct": latency_share,
            "strategic_share_prompt_tokens_pct": token_share,
            "strategic_share_requests_pct": request_share,
            "strat_share_llm": latency_share,
            "strat_share_intok": token_share,
            "strat_share_req": request_share,
            "strat_share": latency_share if latency_share is not None else token_share,
            "tail_anomaly": tail_requests / len(normalized_requests)
            > TAIL_ANOMALY_FRACTION,
            "tail_requests": tail_requests,
            "tail_req": tail_requests,
        }
    )
    return result


def metric_summary(values: Sequence[int | float | None]) -> dict[str, Any] | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return {
        "median": _median_number(clean),
        "min": min(clean),
        "max": max(clean),
    }


def build_aggregates(
    spec: Mapping[str, Any], jobs: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Aggregate clean terminal jobs per task × arm across replicates."""

    aggregates: list[dict[str, Any]] = []
    for task in spec.get("tasks") or []:
        for arm in spec.get("arms") or []:
            rows = [
                row
                for row in jobs
                if row.get("task") == task.get("id")
                and row.get("arm") == arm.get("name")
            ]
            terminal = [
                row for row in rows if row.get("status") in TERMINAL_JOB_STATUSES
            ]
            clean = [
                row
                for row in terminal
                if row.get("classification") != "infra" and not row.get("tail_anomaly")
            ]
            metrics = {
                "wall_minutes": metric_summary(
                    [row.get("wall_minutes") for row in clean]
                ),
                "request_count": metric_summary(
                    [row.get("request_count") for row in clean]
                ),
                "median_prompt_tokens": metric_summary(
                    [row.get("median_prompt_tokens") for row in clean]
                ),
                "strategic_share_latency_pct": metric_summary(
                    [row.get("strategic_share_latency_pct") for row in clean]
                ),
                "strategic_share_prompt_tokens_pct": metric_summary(
                    [row.get("strategic_share_prompt_tokens_pct") for row in clean]
                ),
                "cache_hit_pct": metric_summary(
                    [row.get("cache_hit_pct") for row in clean]
                ),
                "cold_calls": metric_summary([row.get("cold_calls") for row in clean]),
                "overhead_pct": metric_summary(
                    [row.get("overhead_pct") for row in clean]
                ),
                "setup_s": metric_summary([row.get("setup_s") for row in clean]),
                "handoff_dead_s": metric_summary(
                    [row.get("handoff_dead_s") for row in clean]
                ),
            }
            aggregate: dict[str, Any] = {
                "task": task.get("id"),
                "arm": arm.get("name"),
                "expected_replicates": int(spec.get("replicates", 1)),
                "jobs": len(rows),
                "terminal_jobs": len(terminal),
                "completed_jobs": sum(
                    1 for row in terminal if row.get("status") == "completed"
                ),
                "infra_jobs": sum(
                    1 for row in terminal if row.get("classification") == "infra"
                ),
                "tail_anomaly_jobs": sum(
                    1 for row in terminal if row.get("tail_anomaly")
                ),
                "included_jobs": len(clean),
                "metrics": metrics,
            }
            if task.get("family") is not None:
                aggregate["family"] = task["family"]
            aggregates.append(aggregate)
    return aggregates


async def compute_bench_report(
    db: Any,
    run: Mapping[str, Any],
    *,
    audit_reader: Any,
    gitea_client: Any,
    resolve_job_repo: ResolveJobRepoFn,
) -> dict[str, Any]:
    """Compute per-job metrics and task × arm medians/ranges for a run."""

    spec = _decode_json(run.get("spec"), {})
    state = _decode_json(run.get("state"), [])
    jobs: list[dict[str, Any]] = []
    for entry in state:
        if not entry.get("job_id"):
            continue
        job_id = str(entry["job_id"])
        job = await db.get_job(job_id)
        status = (
            str(job.get("status"))
            if job and job.get("status") is not None
            else entry.get("final_status") or entry.get("last_status") or "missing"
        )
        requests = await fetch_main_requests(audit_reader, job_id)
        claim_timings = await fetch_claim_timings(audit_reader, job_id)
        events: list[tuple[str, datetime]] = []
        archive_error: str | None = None
        if requests:
            created_at = None
            if job and job.get("created_at"):
                created_at = parse_timestamp(job["created_at"])
            try:
                events = await fetch_phase_events(
                    gitea_client,
                    resolve_job_repo,
                    job_id,
                    not_before=created_at,
                )
            except Exception as exc:
                archive_error = str(exc)
                logger.warning(
                    "Bench report could not read archives for job %s: %s",
                    job_id,
                    exc,
                )
        row = analyze_job_metrics(
            job_id=job_id,
            status=str(status),
            requests=requests,
            phase_events=events,
            claim_timings=claim_timings,
        )
        row.update(
            {
                "task": entry.get("task"),
                "arm": entry.get("arm"),
                "replicate": entry.get("replicate"),
            }
        )
        if entry.get("family") is not None:
            row["family"] = entry["family"]
        if archive_error is not None:
            row["archive_error"] = archive_error
        jobs.append(row)

    return {
        "run_id": str(run["id"]),
        "name": run.get("name"),
        "status": run.get("status"),
        "created_at": run.get("created_at"),
        "created_by": str(run.get("created_by")),
        "spec": spec,
        "jobs": jobs,
        "aggregates": build_aggregates(spec, jobs),
    }


__all__ = [
    "BenchStateError",
    "BenchStore",
    "SWEEP_SECONDS",
    "TAIL_ANOMALY_FRACTION",
    "TERMINAL_JOB_STATUSES",
    "analyze_claim_metrics",
    "analyze_job_metrics",
    "bench_sweeper_loop",
    "build_aggregates",
    "build_bench_job_payload",
    "build_submission_queue",
    "cancel_bench_run",
    "compute_bench_report",
    "create_bench_run",
    "fetch_claim_timings",
    "fetch_main_requests",
    "fetch_phase_events",
    "freeze_spec",
    "metric_summary",
    "refresh_run_ledger",
    "sweep_run",
    "sweep_tick",
]
