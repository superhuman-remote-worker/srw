"""Test the atomic CAS job claim (M1 HA dispatch).

The claim must be safe when two dispatchers (e.g. a transient dual-leader
window) race for the same job: exactly one wins, the job is never handed to
two agents. Uses a real Postgres (testcontainers) with a minimal ``jobs``
table carrying the columns the CAS touches (uuid id/agent, status, updated_at).
"""

import asyncio
from uuid import UUID

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from tests._container_recovery_schema import create_empty_container_recovery_ledger

JOB = "11111111-1111-1111-1111-111111111111"
AGENT_1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
AGENT_2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
AGENT_3 = "cccccccc-cccc-cccc-cccc-cccccccccccc"


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def db(pg_dsn):
    d = PostgresDB(connection_string=pg_dsn)
    await d.connect()
    async with d.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id uuid PRIMARY KEY,
                status text NOT NULL,
                execution_lane text NOT NULL DEFAULT 'pinned',
                assigned_agent_id uuid,
                lease_expires_at timestamptz,
                config_override jsonb,
                -- A tripped lease-recovery circuit fences the claim: the job
                -- stays parked until the trip is acknowledged.
                context jsonb,
                -- A frozen job is not dispatchable (idx_jobs_dispatchable's
                -- partial predicate), so the CAS fences on it too.
                freeze_data jsonb,
                -- claim_job_for_agent() clears the previous run's failure
                -- record on the CAS statement itself; see
                -- tests/test_claim_clears_stale_failure.py
                error_message text,
                error_details jsonb,
                updated_at timestamptz DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS srw_execution_specs (
                work_kind text, work_id uuid, harness_adapter text
            )
            """
        )
        await create_empty_container_recovery_ledger(conn)
        await conn.execute("TRUNCATE jobs")
        await conn.execute(
            "INSERT INTO jobs (id, status, assigned_agent_id) VALUES ($1, 'created', NULL)",
            UUID(JOB),
        )
    yield d
    await d.close()


@pytest.mark.asyncio
async def test_claim_is_atomic_exactly_one_wins(db):
    r1, r2 = await asyncio.gather(
        db.claim_job_for_agent(JOB, AGENT_1),
        db.claim_job_for_agent(JOB, AGENT_2),
    )
    assert sorted([r1, r2]) == [False, True], "two leaders must not both claim a job"
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, assigned_agent_id FROM jobs WHERE id = $1", UUID(JOB)
        )
    assert row["status"] == "processing"
    assert str(row["assigned_agent_id"]) in (AGENT_1, AGENT_2)
    # An already-claimed job cannot be re-claimed.
    assert await db.claim_job_for_agent(JOB, AGENT_3) is False


@pytest.mark.asyncio
async def test_claim_rejects_non_dispatchable_status(db):
    # 'processing'/terminal jobs are not claimable even if assigned_agent_id is NULL.
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed', assigned_agent_id=NULL")
    assert await db.claim_job_for_agent(JOB, AGENT_1) is False


@pytest.mark.asyncio
async def test_claim_rejects_a_frozen_job(db):
    """A frozen job is not dispatchable, so the CAS must refuse it.

    ``get_dispatchable_jobs`` rides ``idx_jobs_dispatchable``, whose partial
    predicate carries ``freeze_data IS NULL``. Without the same guard on the
    claim, a job frozen between listing and claiming (officer preflight, a
    completion freeze) would still be handed to an agent.
    """
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET freeze_data = $2::jsonb WHERE id = $1",
            UUID(JOB),
            '{"freeze_type": "officer_preflight"}',
        )
    assert await db.claim_job_for_agent(JOB, AGENT_1) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["stateless", "future-lane"])
async def test_claim_rejects_non_pinned_execution_lane(db, lane):
    """Gate 1: legacy claim is a pinned-lane allowlist, not a denylist."""
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET execution_lane=$2 WHERE id = $1", UUID(JOB), lane
        )
    assert await db.claim_job_for_agent(JOB, AGENT_1) is False


@pytest.mark.asyncio
async def test_claim_rejects_a_tripped_lease_recovery_circuit(db):
    """A job whose lease-recovery circuit tripped must not be re-dispatched.

    Repeated recovery of the same pinned lease trips the circuit and parks the
    job for a human/officer acknowledgement. Without the guard on the claim,
    the dispatcher would immediately hand the job back out and the containment
    would be a no-op.
    """
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            UUID(JOB),
            '{"_lease_recovery": {"state": "tripped", "generation": "1"}}',
        )
    assert await db.claim_job_for_agent(JOB, AGENT_1) is False

    # An untripped recovery record leaves the job claimable.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            UUID(JOB),
            '{"_lease_recovery": {"state": "recovering", "generation": "1"}}',
        )
    assert await db.claim_job_for_agent(JOB, AGENT_1) is True
