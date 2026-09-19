"""Worker-job composition over the shared :mod:`run_queue` substrate.

``shared.run_queue`` mutates only ``run_queue`` and reads recovery fences. Worker
claims need one additional invariant: the queue lease and the authoritative
``jobs`` row move together.  This module is the narrow composition layer used
by both the orchestrator (admission/control/fence reads) and the stateless
executor (claim/renew/rotation).

No completion protocol lives here.  A batch rotation is queue-only and an
actual terminal job report remains an orchestrator API call made by the
driver.  In particular, :func:`rotate_worker_batch` atomically composes the
existing queue ``complete`` and ``enqueue`` verbs so a successful batch resets
the poison-attempt counter before becoming runnable again.
"""

from __future__ import annotations

import json
import logging
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import UUID

from shared.run_queue import (
    AFFINITY_GRACE_SECONDS,
    LANE_STATELESS,
    LEASE_TTL_SECONDS,
    STATE_QUEUED,
    UNIT_KIND_WORKER_BATCH,
    ClaimedUnit,
    EnqueueResult,
    claim_unit,
    complete_unit,
    enqueue_unit,
)
from shared.workspace_contract import (
    WORKSPACE_CONTRACT_CONTEXT_KEY,
    WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
    WorkspaceContractError,
    resolve_workspace_contract,
    stateless_worker_backend_admissible,
    vm_mode_from_env,
)
from shared.workspace_recovery import RecoveryAttemptDisposition

logger = logging.getLogger(__name__)

_RUNNABLE_JOB_STATUSES = frozenset({"created", "paused", "processing"})
# Faithful to the registered-agent heartbeat backstop in ``dual_app``.  This
# is deliberately a deny-list: completion-owned states such as ``reviewing``
# and ``completed`` can become visible while the terminal HTTP handler is
# still returning and must not masquerade as an out-of-band control.
_PREEMPTED_JOB_STATUSES = frozenset({"failed", "cancelled", "paused"})

_CONTROL_CLAIM_ACTIVE_SQL = """
CASE
    WHEN NOT (COALESCE(job.context, '{}'::jsonb)
              ? '_completion_control_claim') THEN false
    WHEN jsonb_typeof(job.context->'_completion_control_claim')
         IS DISTINCT FROM 'object' THEN true
    WHEN job.context->'_completion_control_claim'->>'version'
         IS DISTINCT FROM '1' THEN true
    WHEN jsonb_typeof(
        job.context->'_completion_control_claim'->'expires_epoch'
    ) IS DISTINCT FROM 'number' THEN true
    ELSE (
        job.context->'_completion_control_claim'->'expires_epoch'#>>'{}'
    )::numeric > extract(epoch FROM now())
END
"""

_LOCK_JOB_SQL = """
SELECT job.status::text AS status,
       job.execution_lane,
       job.assigned_agent_id,
       job.freeze_data,
       job.context,
       job.config_override,
       queue.max_attempts AS queue_max_attempts
FROM jobs AS job
JOIN run_queue AS queue ON queue.unit_id = job.id
WHERE job.id = $1::uuid
FOR UPDATE OF job
"""

_CAS_JOB_SQL = f"""
UPDATE jobs
SET status = 'processing',
    assigned_agent_id = NULL,
    lease_expires_at = NULL,
    context = jsonb_set(
        COALESCE(context, '{{}}'::jsonb),
        '{{{WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY}}}',
        jsonb_build_object(
            'version', 1,
            'dispatch_kind', 'stateless',
            'contract_version', CASE
                WHEN context ? '{WORKSPACE_CONTRACT_CONTEXT_KEY}' THEN 1 ELSE 0
            END,
            'assigned_backend', COALESCE(
                context->'{WORKSPACE_CONTRACT_CONTEXT_KEY}'->>'assigned_backend',
                CASE lower(COALESCE(
                    config_override->'workspace'->>'backend', 'sandbox'
                ))
                    WHEN 'container' THEN 'sandbox'
                    WHEN 'remote' THEN 'vm'
                    ELSE lower(COALESCE(
                        config_override->'workspace'->>'backend', 'sandbox'
                    ))
                END
            ),
            'worker_pod', $3::text,
            'queue_lease_token', $4::bigint,
            'queue_leased_until', to_jsonb($5::timestamptz)
        ),
        true
    ),
    error_message = NULL,
    error_details = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1::uuid
  AND status::text = $2::text
  AND execution_lane = 'stateless'
  AND assigned_agent_id IS NULL
  AND ($2::text <> 'paused' OR freeze_data IS NULL)
RETURNING id
"""

# Worker-specific composition is allowed to join jobs/completion state; the
# generic run_queue substrate intentionally is not.  Selecting the eligible
# row before leasing means a blocked FIFO head is skipped without consuming a
# token or starving the next runnable worker.
_CLAIM_COMMAND_AWARE_WORKER_SQL = f"""
WITH candidate AS (
    SELECT queue.unit_id
    FROM run_queue AS queue
    JOIN jobs AS job ON job.id = queue.unit_id
    WHERE queue.state = 'queued'
      AND queue.unit_kind = 'worker_batch'
      AND queue.run_after <= now()
      AND (queue.last_leased_by IS NULL
           OR queue.last_leased_by = $1::text
           OR queue.queued_at <= now() - make_interval(secs => $3::float8))
      AND NOT EXISTS (
          SELECT 1
          FROM job_completion_sweep_exclusions AS completion_route
          WHERE completion_route.job_id = job.id
      )
      AND NOT ({_CONTROL_CLAIM_ACTIVE_SQL})
      AND NOT EXISTS (
          SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
          WHERE recovery_job.job_id = job.id
            AND recovery_job.resolved_at IS NULL
      )
    ORDER BY queue.priority DESC, queue.queued_at, queue.enqueue_ord
    LIMIT 1
    FOR UPDATE OF queue SKIP LOCKED
)
UPDATE run_queue AS queue
SET state = 'leased',
    lease_token = queue.lease_token + 1,
    leased_by = $1::text,
    last_leased_by = $1::text,
    leased_until = now() + make_interval(secs => $2::float8),
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    attempts_since_completion = queue.attempts_since_completion + 1
FROM candidate
WHERE queue.unit_id = candidate.unit_id
RETURNING queue.unit_id, queue.unit_kind, queue.fair_key, queue.lease_token,
          queue.input_seq, queue.consumed_seq, queue.control_input_seq,
          queue.control_consumed_seq, queue.attempts_since_completion,
          queue.leased_until
"""

_PARK_REJECTED_VM_JOB_SQL = """
UPDATE jobs
SET status = 'paused',
    assigned_agent_id = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1::uuid
  AND execution_lane = 'stateless'
  AND status = 'processing'
RETURNING id
"""

_RENEW_WORKER_SQL = """
UPDATE run_queue AS queue
SET leased_until = now() + make_interval(secs => $3::float8)
FROM jobs AS job
WHERE queue.unit_id = $1::uuid
  AND queue.unit_id = job.id
  AND queue.unit_kind = 'worker_batch'
  AND queue.state = 'leased'
  AND queue.lease_token = $2::bigint
  AND job.execution_lane = 'stateless'
  AND NOT EXISTS (
      SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
      WHERE recovery_job.job_id = job.id
        AND recovery_job.resolved_at IS NULL
  )
RETURNING queue.leased_until,
          job.status::text AS job_status,
          job.context AS job_context
"""

_CANCEL_QUEUED_WORKER_SQL = """
UPDATE run_queue
SET state = 'done',
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    run_after = now(),
    queued_at = now()
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
  AND state IN ('queued', 'parked')
RETURNING 1
"""

_CURRENT_WORKER_LEASE_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM run_queue
    WHERE unit_id = $1::uuid
      AND unit_kind = 'worker_batch'
      AND lease_token = $2::bigint
)
"""

_ACCEPTED_WORKER_COMPLETION_SQL = """
SELECT job.status::text AS job_status,
       queue.state AS queue_state,
       command.state AS command_state,
       command.id AS command_id,
       command.outcome AS command_outcome,
       command.deadline_at,
       command.lease_expires_at,
       command.run_after,
       command.deadline_at <= clock_timestamp() AS deadline_expired,
       GREATEST(
           0.0,
           extract(epoch FROM (command.deadline_at-clock_timestamp()))
       )::float8 AS deadline_remaining_seconds,
       CASE WHEN command.lease_expires_at IS NULL THEN NULL
            ELSE GREATEST(
                0.0,
                extract(epoch FROM (
                    command.lease_expires_at-clock_timestamp()
                ))
            )::float8
       END AS lease_remaining_seconds,
       GREATEST(
           0.0,
           extract(epoch FROM (command.run_after-clock_timestamp()))
       )::float8 AS run_after_remaining_seconds
FROM run_queue AS queue
JOIN jobs AS job ON job.id = queue.unit_id
JOIN LATERAL (
    SELECT id, state, outcome, deadline_at, lease_expires_at, run_after
    FROM job_completion_commands
    WHERE job_id = queue.unit_id
      AND accepted_lease_token = $2::bigint
      AND ($3::uuid IS NULL OR id = $3::uuid)
    ORDER BY report_seq DESC
    LIMIT 1
) AS command ON TRUE
WHERE queue.unit_id = $1::uuid
  AND queue.unit_kind = 'worker_batch'
  AND queue.state = 'done'
  AND queue.lease_token = $2::bigint
"""

_LOCK_WORKER_ROTATION_SQL = """
SELECT input_seq
FROM run_queue
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
  AND state = 'leased'
  AND lease_token = $2::bigint
FOR UPDATE
"""

_LOCK_WORKER_WAKE_SQL = """
SELECT unit_kind, input_seq
FROM run_queue
WHERE unit_id = $1::uuid
FOR UPDATE
"""

_LOCK_WORKER_PREFLIGHT_SQL = """
SELECT unit_kind, state
FROM run_queue
WHERE unit_id = $1::uuid
FOR UPDATE
"""

_RELEASE_WORKER_SQL = """
UPDATE run_queue
SET state = CASE
        WHEN $3::boolean AND attempts_since_completion >= max_attempts
        THEN 'parked'
        ELSE 'queued'
    END,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(
        secs => $4::float8 * attempts_since_completion
    )
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
  AND state = 'leased'
  AND lease_token = $2::bigint
RETURNING state
"""

_RESET_WORKER_ATTEMPTS_SQL = """
UPDATE run_queue
SET attempts_since_completion = 0
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
RETURNING state
"""

_CLOSE_WORKER_PREFLIGHT_SQL = """
UPDATE run_queue
SET state = 'done',
    lease_token = lease_token + CASE WHEN state = 'leased' THEN 1 ELSE 0 END,
    attempts_since_completion = CASE WHEN $2::boolean THEN attempts_since_completion ELSE 0 END,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    run_after = now(),
    queued_at = now()
WHERE unit_id = $1::uuid
  AND unit_kind = 'worker_batch'
RETURNING state, lease_token
"""


@dataclass(frozen=True, slots=True)
class WorkerClaim:
    """A queue lease whose jobs-row CAS committed in the same transaction."""

    unit: ClaimedUnit
    prior_job_status: str
    resume: bool
    resume_id: str | None = None
    max_attempts: int = 5

    @property
    def unit_id(self) -> UUID:
        return self.unit.unit_id

    @property
    def lease_token(self) -> int:
        return self.unit.lease_token


@dataclass(frozen=True, slots=True)
class WorkerRenewal:
    """Lease renewal plus the control-plane state observed on that heartbeat."""

    leased_until: datetime
    job_status: str
    job_context: dict[str, Any]
    pending_guidance: tuple[dict[str, Any], ...]
    queued_replies: tuple[dict[str, Any], ...]

    @property
    def preempted(self) -> bool:
        """Whether an out-of-band control status requires this claim to stop."""

        return self.job_status in _PREEMPTED_JOB_STATUSES


@dataclass(frozen=True, slots=True)
class WorkerCompletionAcceptance:
    """A queue lease closed by durable completion-command admission (B4).

    The timing fields are calculated by PostgreSQL's clock.  A worker whose
    queue row is already ``done`` can therefore wait for the exact command's
    stored outcome without trusting pod clock skew or inventing a second
    liveness deadline.
    """

    job_status: str
    queue_state: str
    command_state: str
    command_id: UUID
    command_outcome: dict[str, Any]
    deadline_at: datetime
    lease_expires_at: datetime | None
    run_after: datetime
    deadline_expired: bool
    deadline_remaining_seconds: float
    lease_remaining_seconds: float | None
    run_after_remaining_seconds: float


@dataclass(frozen=True, slots=True)
class WorkerRotation:
    """The committed queue-only disposition of one successful batch."""

    completed_state: str
    enqueue: EnqueueResult
    prior_input_seq: int | None
    next_input_seq: int

    @property
    def state(self) -> str:
        return self.completed_state


@dataclass(frozen=True, slots=True)
class WorkerRecoveryHold:
    hold_lease_token: int
    refunded: bool


async def lock_workspace_recovery_membership(
    conn: Any, *, owner_ids: list[UUID], job_id: UUID | None = None
) -> bool:
    """Lock old/new job workspace owners before any queue/job publication lock.

    Callers hold a transaction and recheck their membership snapshot after
    these locks. Ordinary claimers must never acquire this advisory lock.
    """
    owners = sorted(set(owner_ids))
    for owner_id in owners:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"workspace-recovery:job:{owner_id}",
        )
    return not await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM vm_workspace_recoveries "
        "WHERE owner_kind='job' AND owner_id=ANY($1::uuid[]) AND resolved_at IS NULL) "
        "OR EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs "
        "WHERE job_id=$2 AND resolved_at IS NULL)",
        owners,
        job_id,
    )


async def get_worker_attempt_disposition(
    conn: Any, *, job_id: UUID | str, lease_token: int
) -> RecoveryAttemptDisposition | None:
    """Read exact attempt evidence; absent historical rows remain unknown."""
    row = await conn.fetchrow(
        "SELECT * FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2",
        _uuid(job_id),
        lease_token,
    )
    if row is None:
        return None
    return RecoveryAttemptDisposition(
        job_id=row["job_id"],
        lease_token=int(row["lease_token"]),
        bundle_authorized=row["bundle_authorized_at"] is not None,
        authority_digest=row["authority_digest"],
        disposition=_json_object(row["disposition"])
        if row["disposition"] is not None
        else None,
        recovery_id=row["recovery_id"],
        refunded=row["refunded_at"] is not None,
    )


async def record_worker_bundle_authorized(
    conn: Any, *, job_id: UUID | str, lease_token: int, authority_digest: str
) -> bool:
    """Serialize the authority boundary with holds using queue-before-attempt locks."""
    async with conn.transaction():
        current = await conn.fetchval(
            "SELECT 1 FROM run_queue WHERE unit_id=$1 AND unit_kind='worker_batch' "
            "AND state='leased' AND lease_token=$2 AND NOT EXISTS ("
            "SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 "
            "AND resolved_at IS NULL) FOR UPDATE",
            _uuid(job_id),
            lease_token,
        )
        if current is None:
            return False
        return (
            await conn.fetchval(
                "UPDATE worker_batch_attempts SET bundle_authorized_at=clock_timestamp(), "
                "authority_digest=$3 WHERE job_id=$1 AND lease_token=$2 "
                "AND bundle_authorized_at IS NULL AND authority_digest IS NULL "
                "AND refunded_at IS NULL AND recovery_id IS NULL RETURNING 1",
                _uuid(job_id),
                lease_token,
                authority_digest,
            )
            is not None
        )


async def park_worker_batch_for_workspace_recovery(
    conn: Any, *, job_id: UUID | str, accepted_lease_token: int, recovery_id: UUID | str
) -> WorkerRecoveryHold | None:
    """Fence exactly one live attempt and refund only proven pre-bundle work once."""
    job_id, recovery_id = _uuid(job_id), _uuid(recovery_id)
    async with conn.transaction():
        queue = await conn.fetchrow(
            "SELECT attempts_since_completion FROM run_queue WHERE unit_id=$1 "
            "AND unit_kind='worker_batch' AND state='leased' AND lease_token=$2 FOR UPDATE",
            job_id,
            accepted_lease_token,
        )
        if queue is None:
            return None
        attempt = await conn.fetchrow(
            "SELECT * FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2 FOR UPDATE",
            job_id,
            accepted_lease_token,
        )
        refund = bool(
            attempt is not None
            and attempt["bundle_authorized_at"] is None
            and attempt["refunded_at"] is None
            and attempt["recovery_id"] is None
            and attempt["claimed_attempt"] == queue["attempts_since_completion"]
        )
        row = await conn.fetchrow(
            """
            UPDATE run_queue SET state='parked', lease_token=lease_token+1,
                attempts_since_completion=attempts_since_completion-$3::integer,
                leased_by=NULL, last_leased_by=NULL, leased_until=NULL,
                interrupt_admission_lease_token=NULL, interrupt_admission_turn_id=NULL,
                run_after='infinity'::timestamptz, park_reason='workspace_recovery',
                parked_at=clock_timestamp()
            WHERE unit_id=$1 AND unit_kind='worker_batch'
              AND state='leased' AND lease_token=$2
            RETURNING lease_token
            """,
            job_id,
            accepted_lease_token,
            int(refund),
        )
        if row is None:
            raise RuntimeError("workspace recovery lost locked queue lease")
        if attempt is not None:
            await conn.execute(
                "UPDATE worker_batch_attempts SET recovery_id=$3, "
                "refunded_at=CASE WHEN $4 THEN clock_timestamp() ELSE refunded_at END, "
                "refund_reason=CASE WHEN $4 THEN 'workspace_recovery_pre_bundle' "
                "ELSE refund_reason END WHERE job_id=$1 AND lease_token=$2",
                job_id,
                accepted_lease_token,
                recovery_id,
                refund,
            )
        return WorkerRecoveryHold(int(row["lease_token"]), refund)


async def release_worker_batch_from_workspace_recovery(
    conn: Any,
    *,
    job_id: UUID | str,
    recovery_id: UUID | str,
    hold_lease_token: int,
    version: int,
    claim_token: int,
    claimed_by: str,
    resume_receipt: dict[str, Any],
) -> bool:
    """Release a participant by exact recovery CAS and advance its wake watermark once.

    The store composes this with all participant queue/job locks, projection
    validation and operation settlement in the surrounding transaction.
    """
    job_id, recovery_id = _uuid(job_id), _uuid(recovery_id)
    async with conn.transaction():
        queue = await conn.fetchval(
            "SELECT 1 FROM run_queue WHERE unit_id=$1 AND unit_kind='worker_batch' "
            "AND state='parked' AND park_reason='workspace_recovery' AND lease_token=$2 FOR UPDATE",
            job_id,
            hold_lease_token,
        )
        if queue is None:
            return False
        operation = await conn.fetchval(
            "SELECT 1 FROM vm_workspace_recoveries WHERE id=$1 AND version=$2 "
            "AND claim_token=$3 AND phase IN ("
            "'recovering','observing','waiting_runtime','verifying_stop',"
            "'attesting','reconciling_outcome') AND resolved_at IS NULL "
            "AND deadline_at>clock_timestamp() AND claimed_by=$4 "
            "AND claimed_until>clock_timestamp() AND EXISTS ("
            "SELECT 1 FROM vm_workspace_recovery_probe_slots slot "
            "WHERE slot.recovery_id=vm_workspace_recoveries.id "
            "AND slot.claim_token=$3 AND slot.leased_until>clock_timestamp()) "
            "FOR UPDATE",
            recovery_id,
            version,
            claim_token,
            claimed_by,
        )
        if operation is None:
            return False
        participant = await conn.fetchval(
            "UPDATE vm_workspace_recovery_jobs SET participation='released', "
            "resolved_at=clock_timestamp(), resume_receipt=$4::jsonb "
            "WHERE recovery_id=$1 AND job_id=$2 AND hold_lease_token=$3 "
            "AND participation='held' AND resolved_at IS NULL RETURNING 1",
            recovery_id,
            job_id,
            hold_lease_token,
            json.dumps(resume_receipt),
        )
        if participant is None:
            return False
        await conn.execute(
            "UPDATE run_queue SET state='queued', run_after=clock_timestamp(), "
            "queued_at=clock_timestamp(), park_reason=NULL, parked_at=NULL, "
            "input_seq=GREATEST(COALESCE(input_seq,0),COALESCE(consumed_seq,0))+1 "
            "WHERE unit_id=$1",
            job_id,
        )
        return True


def _uuid(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


@asynccontextmanager
async def _connection(source: Any) -> AsyncIterator[Any]:
    """Yield an asyncpg connection from a DB wrapper/pool or a raw connection."""

    acquire = getattr(source, "acquire", None)
    if acquire is None:
        yield source
        return
    async with acquire() as conn:
        yield conn


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _completion_control_claim_active(context: Any) -> bool:
    parsed = _json_object(context)
    if "_completion_control_claim" not in parsed:
        return False
    marker = parsed["_completion_control_claim"]
    if not isinstance(marker, dict):
        return True
    if marker.get("version") != 1:
        return True
    raw_expiry = marker.get("expires_epoch")
    if not isinstance(raw_expiry, (int, float)) or isinstance(raw_expiry, bool):
        return True
    try:
        expiry = float(raw_expiry)
    except (OverflowError, TypeError, ValueError):
        return True
    if not math.isfinite(expiry):
        return True
    import time

    return expiry > time.time()


def _dict_entries(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, dict))


def _job_requests_vm(job: Any) -> bool:
    """Mirror the durable VM markers before a worker jobs-row CAS.

    Public admission pins VM requests before insertion, but this is the final
    defense for inherited/operator-created legacy rows racing the dispatcher's
    repair. It deliberately checks only explicit VM markers; parent-workspace
    inheritance and ordinary K8s attestation remain control-plane concerns.
    """

    try:
        # The lane serves the sandbox tier everywhere and the VM tier on the
        # pod network only (same predicate as the agent's workspace guard —
        # keep them from drifting apart). Malformed/ambiguous legacy state
        # and every other tier stay ineligible; a claimant must never infer
        # authority from a ready container alone.
        return not stateless_worker_backend_admissible(
            resolve_workspace_contract(job).assigned_backend,
            vm_mode=vm_mode_from_env(),
        )
    except WorkspaceContractError:
        return True


async def enqueue_worker_batch(
    conn: Any,
    *,
    job_id: UUID | str,
    fair_key: str | None = None,
    priority: int = 0,
    run_after: datetime | None = None,
    input_seq: int | None = None,
) -> EnqueueResult:
    """Admit one worker job through the existing durable queue semantics."""

    return await enqueue_unit(
        conn,
        unit_id=job_id,
        unit_kind=UNIT_KIND_WORKER_BATCH,
        fair_key=fair_key,
        priority=priority,
        run_after=run_after,
        input_seq=input_seq,
    )


async def enqueue_worker_batch_wake(
    conn: Any,
    *,
    job_id: UUID | str,
    fair_key: str | None = None,
    priority: int = 0,
    run_after: datetime | None = None,
) -> EnqueueResult:
    """Record a fresh synthetic wake watermark for a resume/steer verb.

    The old terminal holder may still be leased while its HTTP completion
    handler exposes a human-facing status. A tokenless enqueue with no input
    watermark would then be swallowed: the old holder's queue completion sees
    no newer work and lands ``done``. Serialize on the queue row and advance a
    monotonic synthetic input so that exact completion necessarily requeues.

    Callers compose this with their jobs-row resume CAS in one transaction;
    this helper therefore preserves the binding queue-before-jobs lock order.
    """

    job_uuid = _uuid(job_id)
    current = await conn.fetchrow(_LOCK_WORKER_WAKE_SQL, job_uuid)
    if current is not None and current["unit_kind"] != UNIT_KIND_WORKER_BATCH:
        raise RuntimeError(
            f"worker wake collided with {current['unit_kind']!r} queue unit {job_uuid}"
        )
    next_input_seq = int((current or {}).get("input_seq") or 0) + 1
    return await enqueue_worker_batch(
        conn,
        job_id=job_uuid,
        fair_key=fair_key,
        priority=priority,
        run_after=run_after,
        input_seq=next_input_seq,
    )


async def cancel_queued_worker_batch(conn: Any, *, job_id: UUID | str) -> bool:
    """Remove queued/parked work from service without disturbing a live lease.

    A leased holder must retain its exact queue token long enough to discover
    the jobs-row status through :func:`renew_worker_batch`; it performs its own
    fenced closure after local teardown.  Durable queue rows are changed to
    ``done`` rather than deleted so their monotonic fence token is preserved.
    """

    row = await conn.fetchrow(_CANCEL_QUEUED_WORKER_SQL, _uuid(job_id))
    return row is not None


async def hold_worker_batch_for_preflight(
    conn: Any,
    *,
    job_id: UUID | str,
    preserve_attempts: bool = False,
) -> None:
    """Make a workspace-reprovisioning job non-runnable under the queue lock.

    A stateless explicit resume must carry a durable resume generation, but a
    missing workspace cannot be exposed to claimers until the leader has
    rebuilt and attested the K8s pod. Every state is closed to ``done``; a live
    lease also advances its token so the predecessor's saver and completion
    report fail closed immediately. When no row exists, create and close one
    inside the same transaction so concurrent admission cannot pass between
    queue and jobs updates. ``preserve_attempts=True`` retains genuine worker
    failures while VM creation preflight waits; the legacy default resets them.
    """

    job_uuid = _uuid(job_id)
    current = await conn.fetchrow(_LOCK_WORKER_PREFLIGHT_SQL, job_uuid)
    if current is not None and current["unit_kind"] != UNIT_KIND_WORKER_BATCH:
        raise RuntimeError(
            f"worker preflight collided with {current['unit_kind']!r} "
            f"queue unit {job_uuid}"
        )
    if current is None:
        await enqueue_worker_batch(conn, job_id=job_uuid)
    closed = await conn.fetchrow(
        _CLOSE_WORKER_PREFLIGHT_SQL, job_uuid, preserve_attempts
    )
    if closed is None:
        raise RuntimeError(f"worker preflight lost queue unit {job_uuid}")


async def reset_worker_batch_attempts(
    conn: Any,
    *,
    job_id: UUID | str,
) -> str | None:
    """Reset poison accounting for an explicit human/operator resume.

    The caller already holds or created the worker queue row in the same
    transaction.  State and lease token are deliberately untouched so a live
    predecessor remains fenced and can observe the companion jobs-row change.
    """

    row = await conn.fetchrow(_RESET_WORKER_ATTEMPTS_SQL, _uuid(job_id))
    return str(row["state"]) if row is not None else None


async def worker_lease_is_current(
    conn: Any,
    *,
    job_id: UUID | str,
    lease_token: int,
) -> bool:
    """Thin `/complete` entry fence for a stateless worker report.

    State is deliberately irrelevant.  An exact-holder retry after queue
    completion must reach the completion route's benign status guard; only a
    later claim/steal (which increments the token) makes the report stale.
    """

    return bool(
        await conn.fetchval(
            _CURRENT_WORKER_LEASE_SQL,
            _uuid(job_id),
            int(lease_token),
        )
    )


class _VMCreationPending(Exception):
    """Roll back a raced claim without consuming a genuine worker attempt."""


async def claim_worker_batch(
    db: Any,
    *,
    pod_name: str,
    lease_ttl_seconds: float = LEASE_TTL_SECONDS,
    affinity_grace_seconds: float = AFFINITY_GRACE_SECONDS,
    completion_commands_enabled: bool = False,
) -> WorkerClaim | None:
    try:
        return await _claim_worker_batch(
            db,
            pod_name=pod_name,
            lease_ttl_seconds=lease_ttl_seconds,
            affinity_grace_seconds=affinity_grace_seconds,
            completion_commands_enabled=completion_commands_enabled,
        )
    except _VMCreationPending:
        return None


async def _claim_worker_batch(
    db: Any,
    *,
    pod_name: str,
    lease_ttl_seconds: float = LEASE_TTL_SECONDS,
    affinity_grace_seconds: float = AFFINITY_GRACE_SECONDS,
    completion_commands_enabled: bool = False,
) -> WorkerClaim | None:
    """Claim a worker queue row and CAS its job to ``processing`` atomically.

    ``processing -> processing`` is intentional: clean rotations never touch
    ``jobs.status``, so every successor after the first batch observes an
    already-processing job.  A stale/terminal/pinned/assigned queue row is
    consumed to ``done`` inside the same transaction instead of rolling back
    into a poison-head claim loop.
    """

    async with _connection(db) as conn:
        async with conn.transaction():
            if completion_commands_enabled:
                claimed_row = await conn.fetchrow(
                    _CLAIM_COMMAND_AWARE_WORKER_SQL,
                    pod_name,
                    lease_ttl_seconds,
                    affinity_grace_seconds,
                )
                unit = (
                    ClaimedUnit(**dict(claimed_row))
                    if claimed_row is not None
                    else None
                )
            else:
                # Preserve the pre-Gate-3 call path exactly when dark.  In
                # particular, it must not parse or name command relations.
                unit = await claim_unit(
                    conn,
                    unit_kind=UNIT_KIND_WORKER_BATCH,
                    pod_name=pod_name,
                    lease_ttl_seconds=lease_ttl_seconds,
                    affinity_grace_seconds=affinity_grace_seconds,
                )
            if unit is None:
                return None

            job = await conn.fetchrow(_LOCK_JOB_SQL, unit.unit_id)
            if completion_commands_enabled:
                completion_blocked = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM job_completion_sweep_exclusions AS completion_route
                        WHERE completion_route.job_id=$1::uuid
                    )
                    """,
                    unit.unit_id,
                )
                if completion_blocked:
                    # Accept and claim share queue->jobs lock order, so this
                    # should be unreachable after the candidate predicate.
                    # Roll back the lease rather than ever running through a
                    # command that appeared at the boundary.
                    raise RuntimeError(
                        "completion command appeared during worker claim "
                        f"(job={unit.unit_id})"
                    )
                if job is not None and _completion_control_claim_active(
                    job.get("context")
                ):
                    raise RuntimeError(
                        "completion control appeared during worker claim "
                        f"(job={unit.unit_id})"
                    )
            prior_status = str(job["status"]) if job is not None else None
            job_context = _json_object(job.get("context")) if job is not None else {}
            if "_vm_creation_pending" in job_context:
                # Even a stale enqueuer cannot publish execution while creation
                # or readiness remains pending. Roll back lease + attempt count.
                raise _VMCreationPending
            resume_id_value = job_context.get("worker_resume_id")
            resume_id = (
                str(resume_id_value)
                if isinstance(resume_id_value, str) and resume_id_value
                else None
            )
            requests_vm = bool(job is not None and _job_requests_vm(job))
            eligible = bool(
                job is not None
                and job["execution_lane"] == LANE_STATELESS
                and job["assigned_agent_id"] is None
                and prior_status in _RUNNABLE_JOB_STATUSES
                # A human-facing completion pause retains its freeze and is
                # not dispatcher-runnable. If its HTTP response was lost, the
                # queue tail is consumed here instead of changing paused back
                # to processing and replaying the long completion handler.
                # Explicit resume/auto-preempt paths shed or never set the
                # freeze before enqueueing, matching the existing partial
                # dispatch-index contract.
                and (prior_status != "paused" or job.get("freeze_data") is None)
                # A worker claimant may beat the defensive dispatcher repair
                # to the queue lock. Reject the explicit VM marker before the
                # jobs CAS so the row never becomes a processing worker loop.
                and not requests_vm
            )
            if eligible:
                updated = await conn.fetchrow(
                    _CAS_JOB_SQL,
                    unit.unit_id,
                    prior_status,
                    pod_name,
                    unit.lease_token,
                    unit.leased_until,
                )
                if updated is None:
                    # The row is locked, so this can only mean a violated
                    # predicate/trigger.  Roll back both sides rather than
                    # committing a queue lease without the jobs-row marker.
                    raise RuntimeError(
                        "worker jobs-row CAS failed after eligibility lock "
                        f"(job={unit.unit_id}, status={prior_status})"
                    )
                await conn.execute(
                    "INSERT INTO worker_batch_attempts (job_id, lease_token, claimed_attempt) "
                    "VALUES ($1, $2, $3)",
                    unit.unit_id,
                    unit.lease_token,
                    unit.attempts_since_completion,
                )
                return WorkerClaim(
                    unit=unit,
                    prior_job_status=prior_status,
                    resume=prior_status != "created",
                    resume_id=resume_id,
                    max_attempts=int(job.get("queue_max_attempts") or 5),
                )

            if requests_vm and prior_status == "processing":
                # Heal rows produced by an older claimant or an operator
                # mutation so the paused-row dispatcher scan can pin them on
                # its next pass. This write is serialized after queue then job.
                await conn.fetchrow(_PARK_REJECTED_VM_JOB_SQL, unit.unit_id)

            discarded = await complete_unit(
                conn,
                unit_id=unit.unit_id,
                lease_token=unit.lease_token,
                consumed_seq=unit.input_seq,
            )
            logger.error(
                "worker_batch claim rejected and reconciled: unit=%s token=%d "
                "job_status=%s lane=%s assigned=%s queue_state=%s",
                unit.unit_id,
                unit.lease_token,
                prior_status,
                job["execution_lane"] if job is not None else None,
                job["assigned_agent_id"] if job is not None else None,
                discarded,
            )
            return None


async def renew_worker_batch(
    conn: Any,
    *,
    unit_id: UUID | str,
    lease_token: int,
    lease_ttl_seconds: float = LEASE_TTL_SECONDS,
) -> WorkerRenewal | None:
    """Renew an exact worker lease and return cancel/preempt/steering state."""

    row = await conn.fetchrow(
        _RENEW_WORKER_SQL,
        _uuid(unit_id),
        int(lease_token),
        float(lease_ttl_seconds),
    )
    if row is None:
        return None
    context = _json_object(row["job_context"])
    return WorkerRenewal(
        leased_until=row["leased_until"],
        job_status=str(row["job_status"]),
        job_context=context,
        pending_guidance=_dict_entries(context.get("pending_guidance")),
        queued_replies=_dict_entries(context.get("queued_replies")),
    )


async def get_worker_completion_acceptance(
    conn: Any,
    *,
    unit_id: UUID | str,
    lease_token: int,
    command_id: UUID | str | None = None,
) -> WorkerCompletionAcceptance | None:
    """Recognize B4 queue closure without misclassifying it as a stolen lease.

    The completion-command accept transaction closes ``run_queue`` before the
    legacy/finalizer tail runs.  A concurrent executor heartbeat consequently
    receives no renewal even though it remains the accepted reporter.  This
    exact token-to-command join distinguishes that benign terminal handoff from
    a real reaper steal; callers must never treat a merely ``done`` queue row as
    proof on its own.  The first lookup may discover the accepted report by
    token.  A finalization-pending worker supplies ``command_id`` thereafter so
    a later report under the same token cannot switch the outcome being
    observed.
    """

    row = await conn.fetchrow(
        _ACCEPTED_WORKER_COMPLETION_SQL,
        _uuid(unit_id),
        int(lease_token),
        _uuid(command_id) if command_id is not None else None,
    )
    if row is None:
        return None
    return WorkerCompletionAcceptance(
        job_status=str(row["job_status"]),
        queue_state=str(row["queue_state"]),
        command_state=str(row["command_state"]),
        command_id=_uuid(row["command_id"]),
        command_outcome=_json_object(row["command_outcome"]),
        deadline_at=row["deadline_at"],
        lease_expires_at=row["lease_expires_at"],
        run_after=row["run_after"],
        deadline_expired=bool(row["deadline_expired"]),
        deadline_remaining_seconds=float(row["deadline_remaining_seconds"]),
        lease_remaining_seconds=(
            float(row["lease_remaining_seconds"])
            if row["lease_remaining_seconds"] is not None
            else None
        ),
        run_after_remaining_seconds=float(row["run_after_remaining_seconds"]),
    )


async def complete_worker_batch(
    conn: Any,
    *,
    unit_id: UUID | str,
    lease_token: int,
    consumed_seq: int | None,
) -> str | None:
    """Close an exact worker queue lease; never changes the jobs row."""

    return await complete_unit(
        conn,
        unit_id=unit_id,
        lease_token=lease_token,
        consumed_seq=consumed_seq,
    )


async def release_worker_batch(
    conn: Any,
    *,
    unit_id: UUID | str,
    lease_token: int,
    park_on_exhaustion: bool,
    backoff_base_seconds: float = 5.0,
) -> str | None:
    """Error-release one worker generation with explicit parking semantics.

    A claim already increments ``attempts_since_completion``.  Generic
    :func:`run_queue.release_unit` preserves that counter but always returns
    the row to ``queued``; a repeatedly recoverable worker would therefore
    never reach the reaper's parked branch.  Recoverable graph/driver failures
    set ``park_on_exhaustion=True`` so the fifth failed batch parks directly.

    A terminal HTTP report failure is deliberately different: its successor
    must keep retrying the already-ended checkpoint and may never park merely
    because the completion handler was unavailable.  That path passes
    ``False`` while retaining the same linear error backoff.
    """

    return await conn.fetchval(
        _RELEASE_WORKER_SQL,
        _uuid(unit_id),
        int(lease_token),
        bool(park_on_exhaustion),
        float(backoff_base_seconds),
    )


async def rotate_worker_batch(
    db: Any,
    *,
    unit_id: UUID | str,
    lease_token: int,
    input_seq: int | None,
    fair_key: str | None = None,
    priority: int = 0,
) -> WorkerRotation | None:
    """Atomically complete successful progress and requeue it runnable-now.

    ``release_unit`` is deliberately not used: release preserves the failed
    attempt count and a healthy, long-running job would eventually park merely
    for rotating.  While the row is still leased, enqueue a monotonically
    advanced synthetic batch watermark.  The existing completion CASE then
    observes newer input, atomically returns the row to ``queued``, and resets
    attempts without ever exposing a ``done`` state.
    """

    async with _connection(db) as conn:
        async with conn.transaction():
            # ``enqueue_unit`` deliberately has no lease-token parameter: it
            # is also the public input-admission primitive.  Fence and lock
            # this exact worker generation before composing it into rotation,
            # otherwise a late token N zombie could advance token N+1's input
            # watermark before its own completion is rejected.
            locked = await conn.fetchrow(
                _LOCK_WORKER_ROTATION_SQL,
                _uuid(unit_id),
                int(lease_token),
            )
            if locked is None:
                return None
            current_input_seq = locked["input_seq"]
            next_input_seq = (
                max(
                    int(input_seq or 0),
                    int(current_input_seq or 0),
                )
                + 1
            )
            enqueue = await enqueue_worker_batch(
                conn,
                job_id=unit_id,
                fair_key=fair_key,
                priority=priority,
                input_seq=next_input_seq,
            )
            # A leased enqueue records only the watermark; it must not release
            # the lease before completion performs the atomic requeue.
            if enqueue.state != "leased":
                raise RuntimeError(
                    "worker rotation lost its lease before completion "
                    f"(job={unit_id}, enqueue={enqueue.status}/{enqueue.state})"
                )
            completed_state = await complete_worker_batch(
                conn,
                unit_id=unit_id,
                lease_token=lease_token,
                consumed_seq=input_seq,
            )
            if completed_state is None:
                # The exact row is locked, so this is an invariant failure.
                # Raising is load-bearing: it rolls back the preceding
                # tokenless enqueue instead of committing a phantom watermark.
                raise RuntimeError(
                    "worker rotation completion lost its locked lease "
                    f"(job={unit_id}, token={lease_token})"
                )
            if completed_state != STATE_QUEUED:
                raise RuntimeError(
                    "worker rotation failed to leave a queued unit "
                    f"(job={unit_id}, complete={completed_state})"
                )
            return WorkerRotation(
                completed_state=completed_state,
                enqueue=enqueue,
                prior_input_seq=input_seq,
                next_input_seq=next_input_seq,
            )


__all__ = [
    "WorkerClaim",
    "WorkerCompletionAcceptance",
    "WorkerRenewal",
    "WorkerRotation",
    "WorkerRecoveryHold",
    "cancel_queued_worker_batch",
    "claim_worker_batch",
    "complete_worker_batch",
    "enqueue_worker_batch",
    "get_worker_completion_acceptance",
    "get_worker_attempt_disposition",
    "lock_workspace_recovery_membership",
    "park_worker_batch_for_workspace_recovery",
    "record_worker_bundle_authorized",
    "release_worker_batch_from_workspace_recovery",
    "renew_worker_batch",
    "release_worker_batch",
    "rotate_worker_batch",
    "worker_lease_is_current",
]
