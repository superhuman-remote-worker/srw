"""Test the workspace-context shed (``shed_workspace_context``).

Refusing to resume a job with no live workspace is only half the recovery: the
dispatcher decides what to provision from the SAME context, and a stale
``context.vm`` with ``status: 'failed'`` makes ``decide_vm_action`` return
VM_PARKED, which re-fails the job instead of re-provisioning it. So the parked
key has to go.

Dropping it outright (as ``delete_job_context_keys`` would) destroys the
diagnosis — ``context.vm.error`` is usually the only surviving record of why
provisioning failed, since a failed resume overwrites ``error_message`` with its
own downstream symptom. Hence the stash to ``last_<key>``, mirroring
``queue_job_for_resume``'s ``last_freeze_data``. These tests pin both halves.

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

# The job 4435994d shape: provisioning died before a VM ever existed.
PARKED_VM = {
    "status": "failed",
    "error": 'while scanning a simple key\n  in "<unicode string>", line 80',
    "provision_attempts": 3,
}


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
                description text,
                status text NOT NULL,
                context jsonb,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
            """
        )
        await create_empty_container_recovery_ledger(conn)
        await conn.execute("TRUNCATE jobs")
        await conn.execute(
            "INSERT INTO jobs (id, status, context) VALUES ($1, 'failed', $2::jsonb)",
            UUID(JOB),
            json.dumps(
                {
                    "vm": PARKED_VM,
                    "loop_id": "b15dc68f",  # a sibling key that must survive
                }
            ),
        )
    yield d
    await d.close()


async def _context(db) -> dict:
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT context FROM jobs WHERE id = $1", UUID(JOB))
    ctx = row["context"]
    return json.loads(ctx) if isinstance(ctx, str) else ctx


@pytest.mark.asyncio
async def test_shed_drops_the_key_so_the_dispatcher_reprovisions(db):
    """An absent context.vm is what makes decide_vm_action return VM_PROVISION."""
    assert await db.shed_workspace_context(JOB, "vm")

    ctx = await _context(db)
    assert "vm" not in ctx


@pytest.mark.asyncio
async def test_shed_stashes_the_old_value_for_diagnosis(db):
    """The provisioning error must survive the shed, not be destroyed by it."""
    assert await db.shed_workspace_context(JOB, "vm")

    ctx = await _context(db)
    assert ctx["last_vm"] == PARKED_VM
    assert "line 80" in ctx["last_vm"]["error"]


@pytest.mark.asyncio
async def test_shed_leaves_sibling_context_keys_alone(db):
    assert await db.shed_workspace_context(JOB, "vm")

    ctx = await _context(db)
    assert ctx["loop_id"] == "b15dc68f"


@pytest.mark.asyncio
async def test_shedding_an_absent_key_is_a_no_op(db):
    """Idempotent: the endpoint and the resume guard may both shed the same job."""
    assert await db.shed_workspace_context(JOB, "workspace_container")

    ctx = await _context(db)
    assert "last_workspace_container" not in ctx, (
        "stashing an absent key would invent an empty last_* entry"
    )
    assert ctx["vm"] == PARKED_VM  # untouched


@pytest.mark.asyncio
async def test_shed_is_repeatable(db):
    """A second shed must not overwrite the stash with nothing."""
    assert await db.shed_workspace_context(JOB, "vm")
    assert await db.shed_workspace_context(JOB, "vm")

    ctx = await _context(db)
    assert ctx["last_vm"] == PARKED_VM


@pytest.mark.asyncio
async def test_unknown_job_returns_false(db):
    assert not await db.shed_workspace_context(
        "00000000-0000-0000-0000-000000000000", "vm"
    )


@pytest.mark.asyncio
async def test_malformed_job_id_returns_false(db):
    assert not await db.shed_workspace_context("not-a-uuid", "vm")
