"""Frozen SRW execution deadlines composed with existing cancellation locks."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


_VM_WAIT_STATES = (
    "waiting_capacity",
    "waiting_golden",
    "waiting_headscale",
    "waiting_preparation",
    "waiting_creation_configuration",
    "creation_attention",
    "provisioning",
    "created",
    "starting",
    "ssh_pending",
    "query_failed",
)
_CANDIDATE = """(
    j.status IN ('created','processing') OR (
        j.status='paused' AND (
            j.context ? '_vm_creation_pending'
            OR j.context->'vm'->>'status'=ANY($1::text[])
            OR j.context->'vm'->>'provisioning_attention_reason' IS NOT NULL
        )
    )
)"""
_DEADLINE = "s.created_at + (s.resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second'"


@dataclass(frozen=True, slots=True)
class ExecutionDeadline:
    execution_id: UUID
    revision: str
    generation: int
    deadline: datetime

    @classmethod
    def from_row(cls, row):
        return cls(
            row["execution_id"], row["revision"], row["generation"], row["deadline"]
        )


async def expired_srw_jobs(db):
    """Advance a bounded, replica-safe scan even when earlier cancels are blocked.

    The cursor is only a scheduling hint. No Job or queue locks are held here;
    each cancellation must separately validate its immutable execution deadline.
    A crash after advancing delays that batch until wrap, without granting any
    execution authority. Null resource links still retain frozen deadlines.
    """
    query = (
        "SELECT j.id,s.created_at AS scan_created_at,s.id AS execution_id,s.revision,s.generation,"
        + _DEADLINE
        + " AS deadline "
        "FROM srw_execution_specs s JOIN jobs j ON s.work_kind='Job' AND s.work_id=j.id "
        "WHERE s.harness_adapter='srw/v1' AND " + _CANDIDATE + " "
        "AND s.resolved->'spec'->>'timeoutSeconds' IS NOT NULL "
        "AND " + _DEADLINE + " <= clock_timestamp() "
        "AND ($2::timestamptz IS NULL OR (s.created_at,j.id)>($2,$3::uuid)) "
        "ORDER BY s.created_at,j.id LIMIT 50"
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO srw_execution_deadline_scan(singleton) VALUES(TRUE) "
                "ON CONFLICT(singleton) DO NOTHING"
            )
            cursor = await conn.fetchrow(
                "SELECT created_at,job_id FROM srw_execution_deadline_scan "
                "WHERE singleton FOR UPDATE"
            )
            rows = await conn.fetch(
                query, list(_VM_WAIT_STATES), cursor["created_at"], cursor["job_id"]
            )
            if not rows and cursor["created_at"] is not None:
                rows = await conn.fetch(query, list(_VM_WAIT_STATES), None, None)
            await conn.execute(
                "UPDATE srw_execution_deadline_scan SET created_at=$1,job_id=$2 "
                "WHERE singleton",
                rows[-1]["scan_created_at"] if rows else None,
                rows[-1]["id"] if rows else None,
            )
            return rows


async def lock_expired_execution(
    conn, job_id: UUID, expected: ExecutionDeadline
) -> bool:
    """Use only inside the ordinary queue/job cancellation transaction.

    Lock the current candidate before re-reading its frozen execution, following
    the established Job -> execution-share order used by VM creation authority.
    A rejected guard grants no signal, runtime cleanup, or checkpoint mutation.
    """
    if not isinstance(expected, ExecutionDeadline):
        return False
    if (
        not isinstance(expected.execution_id, UUID)
        or not isinstance(expected.revision, str)
        or type(expected.generation) is not int
    ):
        return False
    if not isinstance(expected.deadline, datetime) or expected.deadline.tzinfo is None:
        return False
    job = await conn.fetchval(
        "SELECT j.id FROM jobs j WHERE j.id=$2 AND " + _CANDIDATE + " FOR UPDATE",
        list(_VM_WAIT_STATES),
        job_id,
    )
    if job is None:
        return False
    row = await conn.fetchrow(
        "SELECT s.id AS execution_id,s.revision,s.generation,"
        + _DEADLINE
        + " AS deadline "
        "FROM srw_execution_specs s WHERE s.work_kind='Job' AND s.work_id=$1 "
        "AND s.harness_adapter='srw/v1' FOR SHARE",
        job_id,
    )
    if row is None or ExecutionDeadline.from_row(row) != expected:
        return False
    now = await conn.fetchval("SELECT clock_timestamp()")
    return expected.deadline <= now
