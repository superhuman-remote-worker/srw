"""agents.image and jobs.harness_image round-trip through a real Postgres."""

from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
IMAGE = "example.invalid/h@sha256:" + "a" * 64


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def database(pg_dsn, monkeypatch):
    conn = await asyncpg.connect(pg_dsn)
    try:
        exists = await conn.fetchval("SELECT to_regclass('public.jobs')")
        if exists is None:
            await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    db = PostgresDB(connection_string=pg_dsn)
    await db.connect()
    try:
        yield db
    finally:
        await db.disconnect()


@pytest.mark.asyncio
async def test_jobs_and_agents_carry_the_image_columns(database):
    job = await database.create_job(description="x", harness_image=IMAGE)
    assert (await database.get_job(job["id"]))["harness_image"] == IMAGE

    plain = await database.create_job(description="y")
    assert (await database.get_job(plain["id"]))["harness_image"] is None

    agent = await database.register_agent(
        config_name="worker_base", pod_ip="10.0.0.9", image=IMAGE
    )
    async with database.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET status = 'ready' WHERE id = $1::uuid",
            agent["agent_id"],
        )
    rows = await database.get_available_agents()
    row = next(r for r in rows if str(r["id"]) == str(agent["agent_id"]))
    assert row["image"] == IMAGE

    async with database.acquire() as conn:
        lifecycle = await conn.fetchval(
            "SELECT image FROM agents WHERE id = $1::uuid", agent["agent_id"]
        )
    assert lifecycle == IMAGE


@pytest.mark.asyncio
async def test_busy_agent_count_for_image_counts_agents_on_processing_jobs(database):
    from orchestrator.services.agent_provisioner import AgentProvisioner

    other = "example.invalid/other@sha256:" + "c" * 64
    busy = await database.register_agent(
        config_name="worker_base", pod_ip="10.0.0.11", image=IMAGE
    )
    idle = await database.register_agent(
        config_name="worker_base", pod_ip="10.0.0.12", image=IMAGE
    )
    paused = await database.register_agent(
        config_name="worker_base", pod_ip="10.0.0.13", image=IMAGE
    )
    elsewhere = await database.register_agent(
        config_name="worker_base", pod_ip="10.0.0.14", image=other
    )
    # The dispatch-claim trigger guards UPDATEs only: insert the rows as found.
    async with database.acquire() as conn:
        for agent, status in (
            (busy, "processing"),
            (busy, "processing"),
            (paused, "paused"),
            (elsewhere, "processing"),
        ):
            await conn.execute(
                "INSERT INTO jobs (description, status, execution_lane, "
                "assigned_agent_id, harness_image) "
                "VALUES ('z', $1, 'pinned', $2::uuid, $3)",
                status,
                agent["agent_id"],
                IMAGE,
            )
    assert idle["agent_id"]

    provisioner = AgentProvisioner()
    provisioner._db = database
    assert await provisioner.busy_agent_count_for_image(IMAGE) == 1
    assert await provisioner.busy_agent_count_for_image(other) == 1
    assert await provisioner.busy_agent_count_for_image("nothing:here") == 0
