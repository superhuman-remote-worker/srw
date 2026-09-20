"""Durable routing for class-1 job rescuers during completion finalization.

The jobs-side rescuers must not redispatch an agent once a completion command
exists.  This service turns the shared database routing view into one durable,
lease-claimed action per finalizer attempt.  The job row is the allocation
lock; the action owner is the execution fence.  External work deliberately
runs after the allocation transaction commits.

Feature-flag gating belongs to the caller.  Keeping this module free of flag
reads is what lets every rescuer share the same implementation without making
the default-off path touch the Gate-3 relations.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Literal, Mapping
from uuid import UUID, uuid4

from orchestrator.database.postgres import _completion_control_active_sql

logger = logging.getLogger(__name__)

ACTION_RESULT_LIMIT_BYTES = 8 * 1024
STATELESS_OWNER_GAP_CODE = "stateless_terminal_queue_unowned"
STATELESS_OWNER_GAP_MESSAGE = (
    "Stateless execution lost its queue owner after work was reported; "
    "operator review is required."
)

CompletionSweepRoute = Literal[
    "resume_finalizer",
    "park_alert",
    "alert_only",
    "stand_down",
]
CompletionSweepDisposition = Literal[
    "missing_job",
    "legacy",
    "stand_down",
    "busy",
    "queued",
    "already_done",
    "completed",
    "claim_lost",
]
AlertCallback = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class CompletionSweepResult:
    """Typed outcome of routing one jobs-side rescue candidate."""

    job_id: str
    disposition: CompletionSweepDisposition
    route: CompletionSweepRoute | None = None
    command_id: str | None = None
    command_attempt: int | None = None
    action_attempt: int | None = None
    action_result: dict[str, Any] | None = None

    @property
    def legacy(self) -> bool:
        """Whether the caller should continue its pre-Gate-3 rescue path."""

        return self.disposition == "legacy"


@dataclass(frozen=True, slots=True)
class CompletionSweepBatchResult:
    """Outcomes from one bounded background routing scan."""

    count: int
    results: tuple[CompletionSweepResult, ...]


@dataclass(frozen=True, slots=True)
class StatelessOwnerGapPark:
    """One ownerless stateless job moved to the operator worklist."""

    job_id: str
    queue_state: Literal["done", "absent"]


@dataclass(frozen=True, slots=True)
class _ClaimedAction:
    job_id: str
    attempt: int
    command_id: str
    command_attempt: int
    route: CompletionSweepRoute
    claimed_by: str


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    """Accept a raw asyncpg connection, pool, or ``PostgresDB`` wrapper."""

    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


def _canonical_uuid(value: Any, *, label: str) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{label} must be a UUID") from exc


def _source(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("completion sweep source must be nonempty")
    return value.strip()


def _small_text(value: Any, *, limit: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _finalizer_summary(value: Any) -> dict[str, Any]:
    """Keep only bounded routing diagnostics, never a potentially huge outcome."""

    if isinstance(value, Mapping):
        read = value.get
    else:

        def read(key: str, default: Any = None) -> Any:
            return getattr(value, key, default)

    summary = {
        key: compact
        for key in ("command_id", "state", "disposition", "error_code")
        if (compact := _small_text(read(key))) is not None
    }
    if not summary:
        summary["type"] = type(value).__name__[:128]
    return summary


def _bounded_action_result(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
        allow_nan=False,
    )
    if len(encoded.encode("utf-8")) > ACTION_RESULT_LIMIT_BYTES:
        raise ValueError("completion sweep result exceeds the 8 KiB limit")
    return encoded


class CompletionSweepRouter:
    """Route and durably deduplicate completion-aware rescue work."""

    def __init__(
        self,
        db: Any,
        finalizer: Any,
        alert: AlertCallback | None = None,
        safety_net: Any | None = None,
        claimant_id: str | None = None,
        action_lease_seconds: float = 120,
        poll_seconds: float = 1,
    ) -> None:
        if float(action_lease_seconds) <= 0:
            raise ValueError("action_lease_seconds must be positive")
        if float(poll_seconds) <= 0:
            raise ValueError("poll_seconds must be positive")
        if claimant_id is not None and not str(claimant_id).strip():
            raise ValueError("claimant_id must be nonempty")
        self.db = db
        self.finalizer = finalizer
        self.alert = alert
        self.safety_net = safety_net
        self.claimant_id = (
            str(claimant_id).strip()
            if claimant_id is not None
            else f"completion-router-{uuid4()}"
        )
        self.action_lease_seconds = float(action_lease_seconds)
        self.action_heartbeat_seconds = min(30.0, self.action_lease_seconds / 3.0)
        self.poll_seconds = float(poll_seconds)

    def _result(
        self,
        job_id: str,
        disposition: CompletionSweepDisposition,
        *,
        route_row: Any | None = None,
        action_attempt: int | None = None,
        action_result: dict[str, Any] | None = None,
    ) -> CompletionSweepResult:
        if route_row is None:
            return CompletionSweepResult(job_id=job_id, disposition=disposition)
        return CompletionSweepResult(
            job_id=job_id,
            disposition=disposition,
            route=str(route_row["route"]),
            command_id=str(route_row["command_id"]),
            command_attempt=int(route_row["command_attempts"]),
            action_attempt=action_attempt,
            action_result=action_result,
        )

    async def _allocate_and_claim(
        self, job_id: str, *, source: str, claim_action: bool = True
    ) -> tuple[CompletionSweepResult | None, _ClaimedAction | None]:
        """Classify, allocate, and claim while holding the jobs-row lock."""

        async with _connection(self.db) as conn:
            async with conn.transaction():
                locked_job_id = await conn.fetchval(
                    "SELECT id FROM jobs WHERE id=$1::uuid FOR UPDATE",
                    UUID(job_id),
                )
                if locked_job_id is None:
                    return self._result(job_id, "missing_job"), None

                recovery_held = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs "
                    "WHERE job_id=$1::uuid AND resolved_at IS NULL)",
                    UUID(job_id),
                )
                if recovery_held is True:
                    return self._result(job_id, "stand_down"), None

                route_row = await conn.fetchrow(
                    """
                    SELECT job_id, command_id, command_attempts, route
                    FROM job_completion_sweep_exclusions
                    WHERE job_id=$1::uuid
                    """,
                    UUID(job_id),
                )
                if route_row is None:
                    return self._result(job_id, "legacy"), None

                route = str(route_row["route"])
                if route not in {
                    "resume_finalizer",
                    "park_alert",
                    "alert_only",
                    "stand_down",
                }:
                    raise RuntimeError(f"unknown completion sweep route {route!r}")
                if route == "stand_down":
                    return self._result(job_id, "stand_down", route_row=route_row), None

                command_id = UUID(str(route_row["command_id"]))
                command_attempt = int(route_row["command_attempts"])
                action = await conn.fetchrow(
                    """
                    SELECT job_id, attempt, command_id, command_attempt,
                           route, state, claimed_by, claim_expires_at, result
                    FROM job_completion_sweep_actions
                    WHERE command_id=$1::uuid AND command_attempt=$2::int
                    FOR UPDATE
                    """,
                    command_id,
                    command_attempt,
                )
                if action is None:
                    attempt = await conn.fetchval(
                        """
                        UPDATE jobs
                        SET completion_sweep_attempt_hwm =
                            completion_sweep_attempt_hwm + 1
                        WHERE id=$1::uuid
                        RETURNING completion_sweep_attempt_hwm
                        """,
                        UUID(job_id),
                    )
                    action = await conn.fetchrow(
                        """
                        INSERT INTO job_completion_sweep_actions (
                            job_id, attempt, command_id, command_attempt,
                            route, source
                        ) VALUES ($1::uuid, $2::bigint, $3::uuid, $4::int,
                                  $5::text, $6::text)
                        RETURNING job_id, attempt, command_id, command_attempt,
                                  route, state, claimed_by, claim_expires_at,
                                  result
                        """,
                        UUID(job_id),
                        int(attempt),
                        command_id,
                        command_attempt,
                        route,
                        source,
                    )

                if str(action["job_id"]) != job_id:
                    raise RuntimeError(
                        "completion sweep action command belongs to another job"
                    )
                action_attempt = int(action["attempt"])
                state = str(action["state"])
                if state == "done":
                    stored_result = action["result"]
                    if isinstance(stored_result, str):
                        stored_result = json.loads(stored_result)
                    return self._result(
                        job_id,
                        "already_done",
                        route_row=route_row,
                        action_attempt=action_attempt,
                        action_result=(
                            dict(stored_result)
                            if isinstance(stored_result, Mapping)
                            else None
                        ),
                    ), None

                if not claim_action:
                    if state == "pending":
                        return self._result(
                            job_id,
                            "queued",
                            route_row=route_row,
                            action_attempt=action_attempt,
                        ), None
                    return self._result(
                        job_id,
                        "busy",
                        route_row=route_row,
                        action_attempt=action_attempt,
                    ), None

                exact_owner = f"{self.claimant_id}:{uuid4()}"
                claimed = await conn.fetchrow(
                    """
                    UPDATE job_completion_sweep_actions
                    SET state='claimed', route=$4::text,
                        claimed_by=$5::text, claimed_at=now(),
                        claim_expires_at=(
                            now() + make_interval(secs => $6::float8)
                        ),
                        updated_at=now()
                    WHERE job_id=$1::uuid AND attempt=$2::bigint
                      AND command_id=$3::uuid
                      AND (
                          state='pending'
                          OR (state='claimed' AND claim_expires_at <= now())
                      )
                    RETURNING job_id, attempt, command_id, command_attempt,
                              route, claimed_by
                    """,
                    UUID(job_id),
                    action_attempt,
                    command_id,
                    route,
                    exact_owner,
                    self.action_lease_seconds,
                )
                if claimed is None:
                    return self._result(
                        job_id,
                        "busy",
                        route_row=route_row,
                        action_attempt=action_attempt,
                    ), None

                return None, _ClaimedAction(
                    job_id=job_id,
                    attempt=action_attempt,
                    command_id=str(command_id),
                    command_attempt=command_attempt,
                    route=route,
                    claimed_by=exact_owner,
                )

    async def _alert(self, message: str) -> None:
        logger.error(message)
        if self.alert is None:
            return
        value = self.alert(message)
        if isinstance(value, Awaitable):
            await value

    async def _execute(
        self, action: _ClaimedAction
    ) -> tuple[CompletionSweepRoute, dict[str, Any]]:
        effective_route = action.route
        result: dict[str, Any] = {"route": effective_route}
        if effective_route in {"resume_finalizer", "park_alert"}:
            finalized = await self.finalizer.finalize_command(
                action.command_id, inline=False
            )
            result["finalizer"] = _finalizer_summary(finalized)
            finalizer_state = (
                finalized.get("state")
                if isinstance(finalized, Mapping)
                else getattr(finalized, "state", None)
            )
            if effective_route == "resume_finalizer" and finalizer_state == "parked":
                # The command can cross deadline/retry cap after the view read
                # but before the finalizer claims it.  Reuse the exact action
                # and promote it so UNIQUE(command_id, command_attempt) cannot
                # suppress the required operator alert.
                effective_route = "park_alert"
                result["route"] = effective_route
        if effective_route in {"park_alert", "alert_only"}:
            await self._alert(
                "completion sweep routed "
                f"job={action.job_id} command={action.command_id} "
                f"attempt={action.command_attempt} route={effective_route}"
            )
            result["alerted"] = True
        return effective_route, result

    async def _renew(self, action: _ClaimedAction) -> bool:
        """Extend only this exact, still-live action ownership term."""

        async with _connection(self.db) as conn:
            renewed = await conn.fetchval(
                """
                UPDATE job_completion_sweep_actions
                SET claim_expires_at=GREATEST(
                        claim_expires_at,
                        now() + make_interval(secs => $4::float8)
                    ),
                    updated_at=now()
                WHERE job_id=$1::uuid AND attempt=$2::bigint
                  AND state='claimed' AND claimed_by=$3::text
                  AND claim_expires_at > now()
                RETURNING 1
                """,
                UUID(action.job_id),
                action.attempt,
                action.claimed_by,
                self.action_lease_seconds,
            )
        return renewed is not None

    async def _heartbeat(
        self,
        action: _ClaimedAction,
        lost: asyncio.Event,
        stopped: asyncio.Event,
    ) -> None:
        while not lost.is_set() and not stopped.is_set():
            try:
                await asyncio.wait_for(
                    stopped.wait(), timeout=self.action_heartbeat_seconds
                )
                return
            except TimeoutError:
                pass
            try:
                renewed = await self._renew(action)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "completion sweep action heartbeat failed; abandoning "
                    "result authority job=%s attempt=%s owner=%s",
                    action.job_id,
                    action.attempt,
                    action.claimed_by,
                )
                lost.set()
                return
            if not renewed:
                logger.info(
                    "completion sweep action heartbeat lost its exact term "
                    "job=%s attempt=%s owner=%s",
                    action.job_id,
                    action.attempt,
                    action.claimed_by,
                )
                lost.set()
                return

    async def _complete(
        self,
        action: _ClaimedAction,
        route: CompletionSweepRoute,
        result: Mapping[str, Any],
    ) -> bool:
        encoded = _bounded_action_result(result)
        async with _connection(self.db) as conn:
            completed = await conn.fetchval(
                """
                UPDATE job_completion_sweep_actions
                SET state='done', route=$4::text,
                    claimed_by=NULL, claim_expires_at=NULL,
                    result=$5::jsonb, error_code=NULL,
                    completed_at=now(), updated_at=now()
                WHERE job_id=$1::uuid AND attempt=$2::bigint
                  AND state='claimed' AND claimed_by=$3::text
                  AND claim_expires_at > now()
                RETURNING 1
                """,
                UUID(action.job_id),
                action.attempt,
                action.claimed_by,
                route,
                encoded,
            )
        return completed is not None

    async def route_job(self, job_id: Any, *, source: str) -> CompletionSweepResult:
        """Route one class-1 rescue candidate without ever dispatching an agent."""

        canonical_job_id = _canonical_uuid(job_id, label="job_id")
        clean_source = _source(source)
        immediate, action = await self._allocate_and_claim(
            canonical_job_id, source=clean_source
        )
        if immediate is not None:
            return immediate
        assert action is not None

        lost = asyncio.Event()
        stopped = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(action, lost, stopped))
        try:
            effective_route, result = await self._execute(action)
            completed = (
                False
                if lost.is_set()
                else await self._complete(action, effective_route, result)
            )
        except asyncio.CancelledError:
            logger.warning(
                "completion sweep action cancelled; leaving claim for takeover "
                "job=%s attempt=%s owner=%s",
                action.job_id,
                action.attempt,
                action.claimed_by,
            )
            raise
        except Exception:
            logger.exception(
                "completion sweep action failed; leaving claim for takeover "
                "job=%s attempt=%s owner=%s route=%s",
                action.job_id,
                action.attempt,
                action.claimed_by,
                action.route,
            )
            raise
        finally:
            stopped.set()
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        route_row = {
            "route": effective_route,
            "command_id": action.command_id,
            "command_attempts": action.command_attempt,
        }
        if not completed:
            logger.info(
                "completion sweep action lost its exact claim "
                "job=%s attempt=%s owner=%s",
                action.job_id,
                action.attempt,
                action.claimed_by,
            )
            return self._result(
                action.job_id,
                "claim_lost",
                route_row=route_row,
                action_attempt=action.attempt,
            )
        return self._result(
            action.job_id,
            "completed",
            route_row=route_row,
            action_attempt=action.attempt,
            action_result=result,
        )

    async def enqueue_job(self, job_id: Any, *, source: str) -> CompletionSweepResult:
        """Durably nudge an actionable route without running the finalizer inline.

        Resume/control HTTP verbs use this bounded path before returning 409.
        The ordinary router loop claims the pending action, while the unique
        ``(command_id, command_attempt)`` key makes concurrent nudges benign.
        """

        canonical_job_id = _canonical_uuid(job_id, label="job_id")
        clean_source = _source(source)
        immediate, action = await self._allocate_and_claim(
            canonical_job_id,
            source=clean_source,
            claim_action=False,
        )
        assert action is None
        assert immediate is not None
        return immediate

    async def route_once(
        self, limit: int = 50, source: str = "completion_router"
    ) -> CompletionSweepBatchResult:
        """Route one bounded set of currently actionable command rows."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("completion sweep limit must be a positive integer")
        clean_source = _source(source)
        async with _connection(self.db) as conn:
            rows = await conn.fetch(
                """
                SELECT route.job_id
                FROM job_completion_sweep_exclusions AS route
                LEFT JOIN job_completion_sweep_actions AS action
                  ON action.command_id = route.command_id
                 AND action.command_attempt = route.command_attempts
                WHERE route.route <> 'stand_down'
                  AND (
                      route.route <> 'resume_finalizer'
                      OR route.run_after <= now()
                  )
                  AND (
                      action.command_id IS NULL
                      OR action.state = 'pending'
                      OR (action.state = 'claimed'
                          AND action.claim_expires_at <= now())
                  )
                ORDER BY route.run_after, route.job_id
                LIMIT $1::int
                """,
                limit,
            )

        results = tuple(
            [await self.route_job(row["job_id"], source=clean_source) for row in rows]
        )
        return CompletionSweepBatchResult(count=len(results), results=results)

    async def _park_stateless_owner_gap(
        self, job_id: Any
    ) -> StatelessOwnerGapPark | None:
        """Park one exact terminal/absent-queue owner gap, if it still exists.

        A terminal completion command cannot own this shape: its finalizer and
        sweep action are already finished.  Re-enqueueing is unsafe because the
        worker may have produced an answer before filing the malformed report.
        The only automatic action is therefore a jobs-row CAS into the existing
        human-review worklist.  Queue and completion-protocol rows stay immutable.

        The global worker lock order is queue then jobs.  An absent queue row
        cannot be locked, so that branch re-reads after taking the jobs lock and
        the final UPDATE repeats the queue predicate.  A concurrent inserter
        that was still invisible must subsequently acquire the jobs lock and
        fail its own admission CAS against ``pending_review``.
        """

        canonical = _canonical_uuid(job_id, label="job_id")
        control_active = _completion_control_active_sql("context")
        async with _connection(self.db) as conn:
            async with conn.transaction():
                queue = await conn.fetchrow(
                    """
                    SELECT unit_kind, state
                    FROM run_queue
                    WHERE unit_id=$1::uuid
                    FOR UPDATE
                    """,
                    UUID(canonical),
                )
                if queue is not None and (
                    str(queue["unit_kind"]) != "worker_batch"
                    or str(queue["state"]) != "done"
                ):
                    return None

                job = await conn.fetchrow(
                    f"""
                    SELECT status::text AS status, execution_lane,
                           assigned_agent_id, lease_expires_at,
                           (lease_expires_at > clock_timestamp()) AS lease_live,
                           ({control_active}) AS control_active
                    FROM jobs
                    WHERE id=$1::uuid
                    FOR UPDATE
                    """,
                    UUID(canonical),
                )
                if job is None:
                    return None

                # A missing row has no queue lock to serialize an insert.  Read
                # it again after the jobs lock so any committed enqueue is seen;
                # the UPDATE below repeats this predicate for the remaining
                # uncommitted-insert boundary.
                if queue is None:
                    queue = await conn.fetchrow(
                        """
                        SELECT unit_kind, state
                        FROM run_queue
                        WHERE unit_id=$1::uuid
                        """,
                        UUID(canonical),
                    )
                    if queue is not None and (
                        str(queue["unit_kind"]) != "worker_batch"
                        or str(queue["state"]) != "done"
                    ):
                        return None

                if (
                    str(job["status"]) != "processing"
                    or str(job["execution_lane"] or "pinned") != "stateless"
                    or job["assigned_agent_id"] is not None
                    or bool(job["control_active"])
                ):
                    return None
                if bool(job["lease_live"]):
                    return None

                unfinished = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM job_completion_commands AS command
                        WHERE command.job_id=$1::uuid
                          AND command.state IN ('pending','finalizing','parked')
                    )
                    """,
                    UUID(canonical),
                )
                if unfinished:
                    return None

                queue_state = "absent" if queue is None else "done"
                parked = await conn.fetchrow(
                    f"""
                    UPDATE jobs
                    SET status='pending_review',
                        assigned_agent_id=NULL,
                        lease_expires_at=NULL,
                        error_message=$2::text,
                        error_details=(
                            CASE WHEN jsonb_typeof(error_details)='object'
                                 THEN error_details
                                 ELSE '{{}}'::jsonb
                            END
                            || jsonb_build_object(
                                'code', $3::text,
                                'route', 'park_alert',
                                'queue_state', $4::text
                            )
                        ),
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND status='processing'
                      AND execution_lane='stateless'
                      AND assigned_agent_id IS NULL
                      AND (
                          lease_expires_at IS NULL
                          OR lease_expires_at <= clock_timestamp()
                      )
                      AND NOT ({control_active})
                      AND NOT EXISTS (
                          SELECT 1
                          FROM job_completion_commands AS command
                          WHERE command.job_id=jobs.id
                            AND command.state IN (
                                'pending','finalizing','parked'
                            )
                      )
                      AND (
                          (
                              $4::text='absent'
                              AND NOT EXISTS (
                                  SELECT 1 FROM run_queue AS queue
                                  WHERE queue.unit_id=jobs.id
                              )
                          )
                          OR EXISTS (
                              SELECT 1 FROM run_queue AS queue
                              WHERE $4::text='done'
                                AND queue.unit_id=jobs.id
                                AND queue.unit_kind='worker_batch'
                                AND queue.state='done'
                          )
                      )
                    RETURNING id
                    """,
                    UUID(canonical),
                    STATELESS_OWNER_GAP_MESSAGE,
                    STATELESS_OWNER_GAP_CODE,
                    queue_state,
                )
                if parked is None:
                    return None
        return StatelessOwnerGapPark(job_id=canonical, queue_state=queue_state)

    async def park_stateless_owner_gaps_once(
        self, limit: int = 50
    ) -> tuple[StatelessOwnerGapPark, ...]:
        """Move a bounded set of ownerless stateless jobs to operator review.

        ``pending_review`` plus the stable ``error_details.code`` is the
        durable operator owner.  The callback is an attention signal: `_alert`
        always emits at ERROR level even when a deployment has no officer
        threads to receive a wake.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("stateless owner-gap limit must be a positive integer")
        control_active = _completion_control_active_sql("job.context")
        async with _connection(self.db) as conn:
            rows = await conn.fetch(
                f"""
                SELECT job.id
                FROM jobs AS job
                LEFT JOIN run_queue AS queue ON queue.unit_id=job.id
                WHERE job.status='processing'
                  AND job.execution_lane='stateless'
                  AND job.assigned_agent_id IS NULL
                  AND (
                      job.lease_expires_at IS NULL
                      OR job.lease_expires_at <= clock_timestamp()
                  )
                  AND NOT ({control_active})
                  AND (
                      queue.unit_id IS NULL
                      OR (
                          queue.unit_kind='worker_batch'
                          AND queue.state='done'
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM job_completion_commands AS command
                      WHERE command.job_id=job.id
                        AND command.state IN ('pending','finalizing','parked')
                  )
                ORDER BY job.updated_at, job.id
                LIMIT $1::int
                """,
                limit,
            )

        parked: list[StatelessOwnerGapPark] = []
        for row in rows:
            result = await self._park_stateless_owner_gap(row["id"])
            if result is None:
                continue
            parked.append(result)
            # Only the winning jobs-row CAS alerts.  The durable status/error
            # marker remains the worklist if this best-effort wake has no sink.
            await self._alert(
                "completion sweep parked unowned stateless job="
                f"{result.job_id} route=park_alert "
                f"code={STATELESS_OWNER_GAP_CODE} queue={result.queue_state}"
            )
        return tuple(parked)

    async def maintenance_once(self) -> None:
        """Run non-executing reconciliation before independent alarm sampling.

        A safety-net database failure must not stop routed finalizer recovery.
        Liveness monitoring owns its independent 30-second task and is not
        coupled to this router's one-second recovery cadence.
        """

        try:
            await self.park_stateless_owner_gaps_once(limit=50)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("stateless owner-gap rescue tick failed")

        if self.safety_net is not None:
            try:
                await self.safety_net.reconcile_batch(limit=50)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("completion safety-net tick failed")

    async def run(self, shutdown_event: asyncio.Event) -> None:
        """Poll until shutdown; action leases make overlapping runners safe."""

        logger.info(
            "completion sweep router started claimant=%s poll_seconds=%s",
            self.claimant_id,
            self.poll_seconds,
        )
        while not shutdown_event.is_set():
            try:
                await self.maintenance_once()
                await self.route_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("completion sweep router tick failed; retrying")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
        logger.info("completion sweep router stopped claimant=%s", self.claimant_id)


__all__ = [
    "ACTION_RESULT_LIMIT_BYTES",
    "CompletionSweepBatchResult",
    "CompletionSweepResult",
    "CompletionSweepRouter",
    "STATELESS_OWNER_GAP_CODE",
    "STATELESS_OWNER_GAP_MESSAGE",
    "StatelessOwnerGapPark",
]
