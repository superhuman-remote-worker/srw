"""Claiming a job must clear the failure record from its previous run.

``update_job_status`` only writes ``error_message`` when the argument is
non-None (it builds the SET list dynamically), so an error set on a failed run
is never unset. A job that is subsequently recovered — re-queued by Resume, or
moved back to 'created' by hand — therefore carries its old error forever:
job 4435994d sat at ``status='reviewing'`` with 181 audit entries while still
reporting

    workspace.backend='vm' but no workspace.remote config was provided.

from a failed resume two days earlier. Anything reading ``error_message`` (the
cockpit banner, the API, `get_job`) shows a job that is plainly running as
broken.

``claim_job_for_agent`` is the right place: it is the single atomic CAS every
dispatch goes through, so clearing there means no job that is actually running
can display a stale failure, whichever route it took back to dispatchable.

Related: ``clear_job_failure`` covers the different case of a late completion
report re-resolving an already-failed job
(knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md).

Mirrors tests/test_queue_job_for_resume.py.
"""

import json
from uuid import UUID

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from tests._container_recovery_schema import create_empty_container_recovery_ledger

JOB = "4435994d-b029-444d-8a3c-26c64abd456a"
OTHER_JOB = "c6dd288d-25d0-41f0-a66e-79a8624f06ab"
AGENT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
OTHER_AGENT = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

STALE_ERROR = (
    "workspace.backend='vm' but no workspace.remote config was provided. "
    "The orchestrator must inject SSH credentials pointing at a provisioned "
    "workspace container or VM."
)
STALE_DETAILS = {"type": "job_error", "message": STALE_ERROR, "recoverable": False}


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
            CREATE TABLE IF NOT EXISTS srw_execution_specs (
                work_kind text, work_id uuid, harness_adapter text
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id uuid PRIMARY KEY,
                description text,
                status text NOT NULL,
                execution_lane text NOT NULL DEFAULT 'pinned',
                context jsonb,
                config_override jsonb,
                assigned_agent_id uuid,
                -- The CAS fences on freeze_data too (a frozen job is not
                -- dispatchable); see tests/test_job_claim.py.
                freeze_data jsonb,
                error_message text,
                error_details jsonb,
                lease_expires_at timestamptz,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
            """
        )
        await create_empty_container_recovery_ledger(conn)
        await conn.execute("TRUNCATE jobs")
        # A recovered job: back to 'created', unassigned, but still carrying the
        # failure record from the run that died.
        await conn.execute(
            """
            INSERT INTO jobs (id, status, error_message, error_details)
            VALUES ($1, 'created', $2, $3::jsonb)
            """,
            UUID(JOB),
            STALE_ERROR,
            json.dumps(STALE_DETAILS),
        )
        # Already claimed by someone else — the CAS must refuse it.
        await conn.execute(
            """
            INSERT INTO jobs (id, status, assigned_agent_id, error_message)
            VALUES ($1, 'processing', $2, $3)
            """,
            UUID(OTHER_JOB),
            UUID(OTHER_AGENT),
            STALE_ERROR,
        )
    yield d
    await d.close()


async def _row(db, job_id=JOB) -> dict:
    async with db.acquire() as conn:
        return await conn.fetchrow(
            "SELECT status, assigned_agent_id, error_message, error_details "
            "FROM jobs WHERE id = $1",
            UUID(job_id),
        )


@pytest.mark.asyncio
async def test_claim_clears_the_stale_error_message(db):
    """A running job must not report an error from a previous run."""
    assert await db.claim_job_for_agent(JOB, AGENT)

    row = await _row(db)
    assert row["error_message"] is None


@pytest.mark.asyncio
async def test_claim_clears_the_stale_error_details(db):
    """error_details drives the structured failure UI — clear it in step."""
    assert await db.claim_job_for_agent(JOB, AGENT)

    row = await _row(db)
    assert row["error_details"] is None


@pytest.mark.asyncio
async def test_claim_still_assigns_the_job(db):
    """Regression: the CAS itself must be unaffected."""
    assert await db.claim_job_for_agent(JOB, AGENT)

    row = await _row(db)
    assert row["status"] == "processing"
    assert str(row["assigned_agent_id"]) == AGENT


@pytest.mark.asyncio
async def test_a_lost_claim_does_not_touch_the_other_job(db):
    """The CAS predicate still fences a job another dispatcher already holds.

    Clearing must ride on the same statement, so a refused claim leaves the row
    completely alone rather than wiping its error as a side effect.
    """
    assert not await db.claim_job_for_agent(OTHER_JOB, AGENT)

    row = await _row(db, OTHER_JOB)
    assert row["error_message"] == STALE_ERROR
    assert str(row["assigned_agent_id"]) == OTHER_AGENT
