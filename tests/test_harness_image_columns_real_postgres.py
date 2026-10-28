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
    db = PostgresDB()
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
