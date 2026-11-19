"""Non-executing safety and operator resolution for completion commands.

The day-one safety net is deliberately less capable than the finalizer: it
locks durable state, proves that no actor can still be performing work, and
then marks old-mode residue superseded.  It never invokes an effect callback
or reconcile probe.  This distinction prevents an old S36 row from becoming a
workspace delete months after an image rollback.

Operator ``unpark`` and ``force_resolve`` share the same authority checks.  A
jobs-row lock serializes them with admission and status writers; command order
is FIFO; live command/effect/sweep/control owners hold; and an authorized S36
is an absolute hold independent of every lease clock.
"""

from __future__ import annotations

import inspect
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Literal, Mapping
from uuid import UUID

from orchestrator.services.completion_control import (
    completion_control_claim_active,
    completion_delivery_control_claim_owned_active,
)
from orchestrator.services.completion_effect_policy import COMPLETION_EFFECT_PLAN
from orchestrator.services.completion_teardown_authority import (
    WORKSPACE_TEARDOWN_EFFECT,
)

logger = logging.getLogger(__name__)

DEFAULT_COMMAND_DEADLINE_SECONDS = 24 * 60 * 60
DEFAULT_SAFETY_NET_GRACE_SECONDS = 120
MAX_BATCH_SIZE = 200
MAX_ACTOR_CHARS = 128
MAX_REASON_CHARS = 2_048
_S1_EFFECT = "late_callback_guard"
_UNFINISHED_COMMAND_STATES = frozenset({"pending", "finalizing", "parked"})
_TERMINAL_COMMAND_STATES = frozenset({"done", "superseded", "force_resolved"})
_TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})

SafetyNetDisposition = Literal[
    "missing",
    "terminal",
    "not_oldest",
    "held",
    "not_eligible",
    "superseded",
]
ResolutionAlert = Callable[["CompletionResolutionIncident"], Awaitable[None] | None]


class CompletionResolutionError(RuntimeError):
    """Base class for explicit completion-resolution failures."""


class CompletionResolutionNotFound(CompletionResolutionError):
    """The requested command or its job no longer exists."""


class CompletionResolutionConflict(CompletionResolutionError):
    """The requested operator transition lost an exact-state/authority check."""

    def __init__(self, reason: str) -> None:
        self.reason = str(reason)
        super().__init__(self.reason)


@dataclass(frozen=True, slots=True)
class CompletionResolutionIncident:
    """Bounded incident payload for an external operator-alert sink."""

    dedup_key: str
    kind: str
    command_id: str
    job_id: str
    actor: str
    reason: str
    terminal_status: str | None = None


@dataclass(frozen=True, slots=True)
class CompletionSafetyNetResult:
    command_id: str
    disposition: SafetyNetDisposition
    job_id: str | None = None
    report_seq: int | None = None
    reason: str | None = None
    superseded_effects: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompletionSafetyNetBatchResult:
    scanned: int
    results: tuple[CompletionSafetyNetResult, ...]


@dataclass(frozen=True, slots=True)
class CompletionUnparkResult:
    command_id: str
    job_id: str
    report_seq: int
    state: str
    reset_effects: tuple[str, ...]
    deadline_at: datetime


@dataclass(frozen=True, slots=True)
class CompletionForceResolveResult:
    command_id: str
    job_id: str
    report_seq: int
    state: str
    terminal_status: str
    prior_job_status: str
    abandoned_effects: tuple[str, ...]
    outcome: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _AuthorityState:
    live_command: bool
    live_effect: bool
    live_action: bool
    live_control: bool
    active_s36: bool
    live_pinned_executor: bool = False
    live_stateless_executor: bool = False

    @property
    def hold_reason(self) -> str | None:
        if self.active_s36:
            return "workspace_teardown_authorized"
        if self.live_command:
            return "command_owner_live"
        if self.live_effect:
            return "effect_owner_live"
        if self.live_action:
            return "sweep_action_owner_live"
        if self.live_control:
            return "control_owner_live"
        if self.live_pinned_executor:
            return "pinned_executor_live"
        if self.live_stateless_executor:
            return "stateless_executor_live"
        return None


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


def _bounded_text(value: str, *, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be nonempty")
    compact = value.strip()
    if len(compact) > maximum:
        raise ValueError(f"{label} must be at most {maximum} characters")
    return compact


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return dict(value) if isinstance(value, Mapping) else {}


def _json_text(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
        allow_nan=False,
    )


class CompletionCommandResolution:
    """Safety-net and explicit operator transitions for durable commands."""

    def __init__(
        self,
        db: Any,
        *,
        command_deadline_seconds: float = DEFAULT_COMMAND_DEADLINE_SECONDS,
        safety_net_grace_seconds: float = DEFAULT_SAFETY_NET_GRACE_SECONDS,
        alert: ResolutionAlert | None = None,
        settled: Callable[[str, str], Any] | None = None,
    ) -> None:
        if float(command_deadline_seconds) <= 0:
            raise ValueError("command_deadline_seconds must be positive")
        if float(safety_net_grace_seconds) < 0:
            raise ValueError("safety_net_grace_seconds cannot be negative")
        self.db = db
        self.command_deadline_seconds = float(command_deadline_seconds)
        self.safety_net_grace_seconds = float(safety_net_grace_seconds)
        self.alert = alert
        # Told (command id, actor) after an operator moved a parked command
        # on, so whoever was told it parked is told it no longer waits.
        self.settled = settled

    async def _emit(self, incident: CompletionResolutionIncident) -> None:
        logger.critical(
            "completion resolution incident kind=%s command=%s job=%s actor=%s "
            "reason=%s terminal_status=%s",
            incident.kind,
            incident.command_id,
            incident.job_id,
            incident.actor,
            incident.reason,
            incident.terminal_status,
        )
        if self.alert is None:
            return
        try:
            result = self.alert(incident)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # The durable transition already committed. Alert delivery is an
            # at-least-once operational side effect and cannot roll it back.
            logger.exception(
                "completion resolution incident delivery failed for %s",
                incident.command_id,
            )

    async def _settle_parked(self, command_id: str, actor: str) -> None:
        if self.settled is None:
            return
        try:
            result = self.settled(command_id, actor)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # The transition already committed; a stale feed row is the cost.
            logger.exception(
                "completion parked-settlement delivery failed for %s", command_id
            )

    @staticmethod
    async def _lock_command(
        conn: Any,
        command_id: UUID,
        *,
        lock_executor: bool = False,
    ) -> tuple[Any, Any, Any | None] | None:
        # Discover without a lock so an operator can establish the binding
        # run_queue -> jobs order. A concurrent delete is harmless: one of the
        # locked reads then misses.
        job_id = await conn.fetchval(
            "SELECT job_id FROM job_completion_commands WHERE id=$1::uuid",
            command_id,
        )
        if job_id is None:
            return None
        queue = None
        if lock_executor:
            queue = await conn.fetchrow(
                """
                SELECT unit_id, unit_kind, state, leased_until,
                       state='leased' AND leased_until > now() AS live
                FROM run_queue
                WHERE unit_id=$1::uuid
                FOR UPDATE
                """,
                job_id,
            )
        job = await conn.fetchrow(
            """
            SELECT id, status::text AS status, context, execution_lane,
                   assigned_agent_id, lease_expires_at,
                   lease_expires_at > now()
                       AND EXISTS (
                           SELECT 1
                           FROM agents AS assigned
                           WHERE assigned.id=jobs.assigned_agent_id
                             AND assigned.current_job_id=jobs.id
                             AND assigned.status::text IN ('working','draining')
                       ) AS pinned_executor_live,
                   extract(epoch FROM clock_timestamp())::float8 AS db_now_epoch
            FROM jobs
            WHERE id=$1::uuid
            FOR UPDATE
            """,
            job_id,
        )
        if job is None:
            return None
        command = await conn.fetchrow(
            """
            SELECT command.*,
                   command.lease_expires_at > now() AS command_live,
                   command.deadline_at <= now() AS deadline_expired,
                   GREATEST(
                       0.0,
                       extract(epoch FROM (clock_timestamp()-command.reported_at))
                   )::float8 AS reported_age_seconds
            FROM job_completion_commands AS command
            WHERE command.id=$1::uuid AND command.job_id=$2::uuid
            FOR UPDATE OF command
            """,
            command_id,
            job_id,
        )
        if command is None:
            return None
        return job, command, queue

    @staticmethod
    async def _is_oldest_unfinished(conn: Any, command: Any) -> bool:
        return not bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM job_completion_commands AS predecessor
                    WHERE predecessor.job_id=$1::uuid
                      AND predecessor.report_seq < $2::bigint
                      AND predecessor.state IN ('pending','finalizing','parked')
                )
                """,
                command["job_id"],
                int(command["report_seq"]),
            )
        )

    @staticmethod
    async def _effects(conn: Any, command_id: UUID, *, lock: bool = False) -> list[Any]:
        lock_clause = " FOR UPDATE" if lock else ""
        return list(
            await conn.fetch(
                f"""
                SELECT effect_name, state, complete_by, detail,
                       state='pending' AND complete_by > now() AS live
                FROM completion_effects
                WHERE producer_kind='job_completion'
                  AND producer_id=$1::uuid
                ORDER BY effect_name
                {lock_clause}
                """,
                command_id,
            )
        )

    @staticmethod
    async def _authority(
        conn: Any,
        *,
        job: Any,
        command: Any,
        effects: list[Any],
        queue: Any | None = None,
        include_executor: bool = False,
        allow_owned_delivery_control: bool = False,
    ) -> _AuthorityState:
        command_id = UUID(str(command["id"]))
        live_action = bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM job_completion_sweep_actions
                    WHERE command_id=$1::uuid
                      AND state='claimed'
                      AND claim_expires_at > now()
                )
                """,
                command_id,
            )
        )
        active_s36 = any(
            str(effect["effect_name"]) == WORKSPACE_TEARDOWN_EFFECT
            and str(effect["state"]) == "pending"
            and isinstance(
                _json_object(effect["detail"]).get("teardown_authorization"),
                Mapping,
            )
            and _json_object(effect["detail"])["teardown_authorization"].get("active")
            is True
            for effect in effects
        )
        now_epoch = float(job["db_now_epoch"])
        control_active = completion_control_claim_active(
            job["context"], now_epoch=now_epoch
        )
        owned_delivery_control = bool(
            allow_owned_delivery_control
            and completion_delivery_control_claim_owned_active(
                job["context"], str(command_id), now_epoch=now_epoch
            )
        )
        return _AuthorityState(
            live_command=bool(command["command_live"]),
            live_effect=any(bool(effect["live"]) for effect in effects),
            live_action=live_action,
            # The finalizer installs a crash-adoptable marker around delivery.
            # Once every executable term is dead, the exact owning command's
            # operator transition may retain (unpark) or exact-clear (force)
            # that marker. Human, malformed, or other-command markers hold.
            live_control=control_active and not owned_delivery_control,
            active_s36=active_s36,
            live_pinned_executor=bool(
                include_executor
                and str(job["execution_lane"] or "pinned") == "pinned"
                and bool(job["pinned_executor_live"])
            ),
            live_stateless_executor=bool(
                include_executor
                and str(job["execution_lane"] or "pinned") == "stateless"
                and queue is not None
                and str(queue["unit_kind"]) == "worker_batch"
                and bool(queue["live"])
            ),
        )

    @staticmethod
    async def _settle_nonlive_actions(
        conn: Any,
        *,
        command_id: UUID,
        result: Mapping[str, Any],
    ) -> None:
        await conn.execute(
            """
            UPDATE job_completion_sweep_actions
            SET state='done', claimed_by=NULL, claim_expires_at=NULL,
                claimed_at=COALESCE(claimed_at, now()),
                result=$2::jsonb, error_code=NULL,
                updated_at=now(), completed_at=now()
            WHERE command_id=$1::uuid
              AND (
                  state='pending'
                  OR (state='claimed' AND claim_expires_at <= now())
              )
            """,
            command_id,
            _json_text(result),
        )

    @staticmethod
    async def _supersede_pending_effects(
        conn: Any,
        *,
        command_id: UUID,
        actor: str,
        reason: str,
        disposition: str,
    ) -> tuple[str, ...]:
        rows = await conn.fetch(
            """
            UPDATE completion_effects
            SET state='superseded', completed_at=now(), complete_by=NULL,
                error_code=NULL,
                detail=jsonb_set(
                    jsonb_set(
                        COALESCE(detail, '{}'::jsonb),
                        '{output}',
                        jsonb_build_object(
                            'executed', false,
                            'callbacks', false,
                            'disposition', $4::text,
                            'reason', $3::text
                        ),
                        true
                    ),
                    '{resolution}',
                    jsonb_build_object('actor', $2::text, 'reason', $3::text),
                    true
                )
            WHERE producer_kind='job_completion'
              AND producer_id=$1::uuid
              AND state='pending'
            RETURNING effect_name
            """,
            command_id,
            actor,
            reason,
            disposition,
        )
        return tuple(sorted(str(row["effect_name"]) for row in rows))

    @staticmethod
    def _unstarted_effects(effects: list[Any]) -> tuple[str, ...]:
        journaled = {str(effect["effect_name"]) for effect in effects}
        return tuple(
            sorted(
                effect.name
                for effect in COMPLETION_EFFECT_PLAN
                if effect.name not in journaled
            )
        )

    async def preclaim_command(
        self, command_id: UUID | str
    ) -> CompletionSafetyNetResult:
        """Reconcile one eligible old-mode command without executing effects."""

        command_uuid = UUID(str(command_id))
        async with _connection(self.db) as conn:
            async with conn.transaction():
                locked = await self._lock_command(conn, command_uuid)
                if locked is None:
                    return CompletionSafetyNetResult(
                        str(command_uuid), disposition="missing"
                    )
                job, command, _queue = locked
                job_id = str(command["job_id"])
                report_seq = int(command["report_seq"])
                state = str(command["state"])
                base = {
                    "command_id": str(command_uuid),
                    "job_id": job_id,
                    "report_seq": report_seq,
                }
                if state in _TERMINAL_COMMAND_STATES:
                    return CompletionSafetyNetResult(
                        disposition="terminal", reason=state, **base
                    )
                if state not in _UNFINISHED_COMMAND_STATES:
                    return CompletionSafetyNetResult(
                        disposition="not_eligible",
                        reason="unknown_command_state",
                        **base,
                    )
                if not await self._is_oldest_unfinished(conn, command):
                    return CompletionSafetyNetResult(
                        disposition="not_oldest", reason="fifo_predecessor", **base
                    )

                # Do not lock an effect while holding its command: the normal
                # finalizer settlement order is effect -> command. A nonlocking
                # authority read is safe under the jobs+command locks because
                # new work must first renew/claim that command, and any already
                # live effect is an immediate hold. Only after every term is
                # provably dead do we acquire effect locks and recheck.
                effects = await self._effects(conn, command_uuid)
                authority = await self._authority(
                    conn, job=job, command=command, effects=effects
                )
                if authority.hold_reason is not None:
                    return CompletionSafetyNetResult(
                        disposition="held", reason=authority.hold_reason, **base
                    )

                if bool(command["status_reorder_enabled"]):
                    # A persisted reorder command may already have written its
                    # status while still owing Class C/D. It always resumes.
                    return CompletionSafetyNetResult(
                        disposition="not_eligible",
                        reason="persisted_reorder_tail",
                        **base,
                    )
                effects = await self._effects(conn, command_uuid, lock=True)
                authority = await self._authority(
                    conn, job=job, command=command, effects=effects
                )
                if authority.hold_reason is not None:
                    return CompletionSafetyNetResult(
                        disposition="held", reason=authority.hold_reason, **base
                    )

                observed_status = str(job["status"])
                terminal_job = observed_status in _TERMINAL_JOB_STATUSES
                first_effect_only = all(
                    str(effect["effect_name"]) == _S1_EFFECT for effect in effects
                )
                stale = (
                    state == "parked"
                    or bool(command["deadline_expired"])
                    or int(command["attempts"]) >= int(command["max_attempts"])
                )
                if (
                    terminal_job
                    and first_effect_only
                    and not stale
                    and float(command["reported_age_seconds"])
                    < self.safety_net_grace_seconds
                ):
                    return CompletionSafetyNetResult(
                        disposition="not_eligible",
                        reason="fresh_terminal_grace",
                        **base,
                    )
                if not terminal_job and not (stale and first_effect_only):
                    return CompletionSafetyNetResult(
                        disposition="not_eligible",
                        reason="old_mode_command_still_resumable",
                        **base,
                    )

                reason = (
                    "safety_net_legacy_terminal"
                    if terminal_job
                    else "safety_net_stale_entry_only"
                )
                superseded = await self._supersede_pending_effects(
                    conn,
                    command_id=command_uuid,
                    actor="completion-safety-net",
                    reason=reason,
                    disposition="safety_net_superseded",
                )
                unstarted = self._unstarted_effects(effects)
                abandoned = tuple(sorted(set(superseded) | set(unstarted)))
                outcome = {
                    "status": "superseded",
                    "job_id": job_id,
                    "report_seq": report_seq,
                    "reason": reason,
                    "accepted_job_status": command["accepted_job_status"],
                    "observed_job_status": observed_status,
                    "winning_report_seq": None,
                    "abandoned_effects": list(abandoned),
                    "superseded_effects": list(superseded),
                    "unstarted_effects": list(unstarted),
                    "effect_plan_complete": True,
                    "reconcile_predicate": {
                        "persisted_reorder": False,
                        "terminal_job": terminal_job,
                        "stale": stale,
                        "entry_only": first_effect_only,
                    },
                    "executed": False,
                    "callbacks": False,
                    "safety_net": True,
                }
                updated = await conn.fetchval(
                    """
                    UPDATE job_completion_commands
                    SET state='superseded', outcome=$2::jsonb,
                        finalized_at=now(), error_code=$3::text,
                        finalizing_by=NULL, lease_expires_at=NULL
                    WHERE id=$1::uuid
                      AND state=$4::text
                    RETURNING 1
                    """,
                    command_uuid,
                    _json_text(outcome),
                    reason,
                    state,
                )
                if updated is None:
                    raise CompletionResolutionConflict("command_state_changed")
                await self._settle_nonlive_actions(
                    conn,
                    command_id=command_uuid,
                    result={
                        "route": "safety_net_superseded",
                        "executed": False,
                        "reason": reason,
                    },
                )
                return CompletionSafetyNetResult(
                    disposition="superseded",
                    reason=reason,
                    superseded_effects=superseded,
                    **base,
                )

    async def reconcile_batch(
        self, *, limit: int = 50
    ) -> CompletionSafetyNetBatchResult:
        """Run a bounded candidate scan; every mutation rechecks under locks."""

        if isinstance(limit, bool) or not 1 <= int(limit) <= MAX_BATCH_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_BATCH_SIZE}")
        async with _connection(self.db) as conn:
            rows = await conn.fetch(
                """
                SELECT command.id
                FROM job_completion_commands AS command
                JOIN jobs AS job ON job.id=command.job_id
                WHERE command.state IN ('pending','finalizing','parked')
                  AND command.status_reorder_enabled=false
                  AND NOT EXISTS (
                      SELECT 1
                      FROM job_completion_commands AS predecessor
                      WHERE predecessor.job_id=command.job_id
                        AND predecessor.report_seq < command.report_seq
                        AND predecessor.state IN ('pending','finalizing','parked')
                  )
                  AND (
                      (
                          job.status::text IN ('completed','failed','cancelled')
                          AND (
                              command.reported_at <= now()-make_interval(
                                  secs => $2::float8
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM completion_effects AS progressed
                                  WHERE progressed.producer_kind='job_completion'
                                    AND progressed.producer_id=command.id
                                    AND progressed.effect_name <> $3::text
                              )
                          )
                      )
                      OR (
                          (
                              command.state='parked'
                              OR command.deadline_at <= now()
                              OR command.attempts >= command.max_attempts
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM completion_effects AS progressed
                              WHERE progressed.producer_kind='job_completion'
                                AND progressed.producer_id=command.id
                                AND progressed.effect_name <> $3::text
                          )
                      )
                  )
                ORDER BY command.reported_at, command.job_id, command.report_seq
                LIMIT $1::int
                """,
                int(limit),
                self.safety_net_grace_seconds,
                _S1_EFFECT,
            )
        results = tuple([await self.preclaim_command(row["id"]) for row in rows])
        return CompletionSafetyNetBatchResult(scanned=len(rows), results=results)

    async def unpark(
        self,
        command_id: UUID | str,
        *,
        actor: str,
    ) -> CompletionUnparkResult:
        """Rearm one exact parked FIFO command and all pending effect budgets."""

        command_uuid = UUID(str(command_id))
        bounded_actor = _bounded_text(actor, label="actor", maximum=MAX_ACTOR_CHARS)
        async with _connection(self.db) as conn:
            async with conn.transaction():
                locked = await self._lock_command(
                    conn, command_uuid, lock_executor=True
                )
                if locked is None:
                    raise CompletionResolutionNotFound(str(command_uuid))
                job, command, queue = locked
                if str(command["state"]) != "parked":
                    raise CompletionResolutionConflict("command_not_parked")
                if not await self._is_oldest_unfinished(conn, command):
                    raise CompletionResolutionConflict("fifo_predecessor")
                effects = await self._effects(conn, command_uuid)
                authority = await self._authority(
                    conn,
                    job=job,
                    command=command,
                    effects=effects,
                    queue=queue,
                    include_executor=True,
                    allow_owned_delivery_control=True,
                )
                if authority.hold_reason is not None:
                    raise CompletionResolutionConflict(authority.hold_reason)
                effects = await self._effects(conn, command_uuid, lock=True)
                authority = await self._authority(
                    conn,
                    job=job,
                    command=command,
                    effects=effects,
                    queue=queue,
                    include_executor=True,
                    allow_owned_delivery_control=True,
                )
                if authority.hold_reason is not None:
                    raise CompletionResolutionConflict(authority.hold_reason)

                reset_rows = await conn.fetch(
                    """
                    UPDATE completion_effects
                    SET attempts=0, run_after=now(), complete_by=NULL,
                        error_code=NULL
                    WHERE producer_kind='job_completion'
                      AND producer_id=$1::uuid
                      AND state='pending'
                    RETURNING effect_name
                    """,
                    command_uuid,
                )
                command_row = await conn.fetchrow(
                    """
                    UPDATE job_completion_commands
                    SET state='pending', attempts=0, run_after=now(),
                        deadline_at=now()+make_interval(secs => $2::float8),
                        finalizing_by=NULL, lease_expires_at=NULL,
                        outcome=NULL, finalized_at=NULL, error_code=NULL
                    WHERE id=$1::uuid AND state='parked'
                    RETURNING deadline_at
                    """,
                    command_uuid,
                    self.command_deadline_seconds,
                )
                if command_row is None:
                    raise CompletionResolutionConflict("command_state_changed")
                await self._settle_nonlive_actions(
                    conn,
                    command_id=command_uuid,
                    result={
                        "route": "operator_unpark",
                        "actor": bounded_actor,
                        "executed": False,
                    },
                )
                result = CompletionUnparkResult(
                    command_id=str(command_uuid),
                    job_id=str(command["job_id"]),
                    report_seq=int(command["report_seq"]),
                    state="pending",
                    reset_effects=tuple(
                        sorted(str(row["effect_name"]) for row in reset_rows)
                    ),
                    deadline_at=command_row["deadline_at"],
                )
        logger.warning(
            "completion command %s unparked by %s", command_uuid, bounded_actor
        )
        await self._settle_parked(str(command_uuid), bounded_actor)
        return result

    async def force_resolve(
        self,
        command_id: UUID | str,
        *,
        expected_state: str,
        terminal_status: str,
        actor: str,
        reason: str,
    ) -> CompletionForceResolveResult:
        """Atomically abandon a command tail and write an explicit job status."""

        command_uuid = UUID(str(command_id))
        expected = str(expected_state).strip()
        if expected not in _UNFINISHED_COMMAND_STATES:
            raise ValueError("expected_state must be pending, finalizing, or parked")
        terminal = str(terminal_status).strip()
        if terminal not in _TERMINAL_JOB_STATUSES:
            raise ValueError("terminal_status must be completed, failed, or cancelled")
        bounded_actor = _bounded_text(actor, label="actor", maximum=MAX_ACTOR_CHARS)
        bounded_reason = _bounded_text(reason, label="reason", maximum=MAX_REASON_CHARS)

        async with _connection(self.db) as conn:
            async with conn.transaction():
                locked = await self._lock_command(
                    conn, command_uuid, lock_executor=True
                )
                if locked is None:
                    raise CompletionResolutionNotFound(str(command_uuid))
                job, command, queue = locked
                if str(command["state"]) != expected:
                    raise CompletionResolutionConflict("command_state_changed")
                if not await self._is_oldest_unfinished(conn, command):
                    raise CompletionResolutionConflict("fifo_predecessor")
                effects = await self._effects(conn, command_uuid)
                authority = await self._authority(
                    conn,
                    job=job,
                    command=command,
                    effects=effects,
                    queue=queue,
                    include_executor=True,
                    allow_owned_delivery_control=True,
                )
                if authority.hold_reason is not None:
                    raise CompletionResolutionConflict(authority.hold_reason)
                effects = await self._effects(conn, command_uuid, lock=True)
                authority = await self._authority(
                    conn,
                    job=job,
                    command=command,
                    effects=effects,
                    queue=queue,
                    include_executor=True,
                    allow_owned_delivery_control=True,
                )
                if authority.hold_reason is not None:
                    raise CompletionResolutionConflict(authority.hold_reason)

                prior_status = str(job["status"])
                if prior_status in _TERMINAL_JOB_STATUSES and prior_status != terminal:
                    raise CompletionResolutionConflict("terminal_status_conflict")

                superseded = await self._supersede_pending_effects(
                    conn,
                    command_id=command_uuid,
                    actor=bounded_actor,
                    reason=bounded_reason,
                    disposition="operator_force_resolved",
                )
                unstarted = self._unstarted_effects(effects)
                abandoned = tuple(sorted(set(superseded) | set(unstarted)))
                job_updated = await conn.fetchval(
                    """
                    UPDATE jobs
                    SET status=$2::text,
                        assigned_agent_id=NULL,
                        context=CASE
                            WHEN context->'_completion_control_claim'->>'version'='1'
                             AND context->'_completion_control_claim'->>'claim_id'=$4::text
                             AND context->'_completion_control_claim'->>'source'='completion_delivery'
                             AND context->'_completion_control_claim'->>'fence_kind'='completion_command'
                             AND context->'_completion_control_claim'->>'fence_value'=$4::text
                            THEN COALESCE(context, '{}'::jsonb)-'_completion_control_claim'
                            ELSE context
                        END,
                        completed_at=CASE
                            WHEN $2::text='completed'
                            THEN COALESCE(completed_at, now())
                            ELSE completed_at
                        END,
                        failed_at=CASE
                            WHEN $2::text='failed'
                            THEN COALESCE(failed_at, now())
                            ELSE failed_at
                        END,
                        updated_at=now()
                    WHERE id=$1::uuid AND status::text=$3::text
                    RETURNING 1
                    """,
                    command["job_id"],
                    terminal,
                    prior_status,
                    str(command_uuid),
                )
                if job_updated is None:
                    raise CompletionResolutionConflict("job_status_changed")

                outcome = {
                    "status": "force_resolved",
                    "job_id": str(command["job_id"]),
                    "report_seq": int(command["report_seq"]),
                    "terminal_status": terminal,
                    "prior_job_status": prior_status,
                    "accepted_job_status": command["accepted_job_status"],
                    "actor": bounded_actor,
                    "reason": bounded_reason,
                    "abandoned_effects": list(abandoned),
                    "superseded_effects": list(superseded),
                    "unstarted_effects": list(unstarted),
                    "effect_plan_complete": True,
                    "reconcile_predicate": {
                        "oldest_unfinished": True,
                        "expected_state": expected,
                        "live_authority": False,
                        "active_s36": False,
                    },
                    "incident": True,
                    "executed": False,
                    "callbacks": False,
                }
                command_updated = await conn.fetchval(
                    """
                    UPDATE job_completion_commands
                    SET state='force_resolved', outcome=$3::jsonb,
                        finalized_at=now(), error_code='force_resolved',
                        finalizing_by=NULL, lease_expires_at=NULL
                    WHERE id=$1::uuid AND state=$2::text
                    RETURNING 1
                    """,
                    command_uuid,
                    expected,
                    _json_text(outcome),
                )
                if command_updated is None:
                    raise CompletionResolutionConflict("command_state_changed")
                await self._settle_nonlive_actions(
                    conn,
                    command_id=command_uuid,
                    result={
                        "route": "operator_force_resolved",
                        "actor": bounded_actor,
                        "terminal_status": terminal,
                        "executed": False,
                    },
                )
                result = CompletionForceResolveResult(
                    command_id=str(command_uuid),
                    job_id=str(command["job_id"]),
                    report_seq=int(command["report_seq"]),
                    state="force_resolved",
                    terminal_status=terminal,
                    prior_job_status=prior_status,
                    abandoned_effects=abandoned,
                    outcome=outcome,
                )

        await self._emit(
            CompletionResolutionIncident(
                dedup_key=f"completion.force_resolved:{command_uuid}",
                kind="force_resolved",
                command_id=str(command_uuid),
                job_id=result.job_id,
                actor=bounded_actor,
                reason=bounded_reason,
                terminal_status=terminal,
            )
        )
        await self._settle_parked(str(command_uuid), bounded_actor)
        return result


__all__ = [
    "CompletionCommandResolution",
    "CompletionForceResolveResult",
    "CompletionResolutionConflict",
    "CompletionResolutionError",
    "CompletionResolutionIncident",
    "CompletionResolutionNotFound",
    "CompletionSafetyNetBatchResult",
    "CompletionSafetyNetResult",
    "CompletionUnparkResult",
    "DEFAULT_COMMAND_DEADLINE_SECONDS",
    "DEFAULT_SAFETY_NET_GRACE_SECONDS",
]
