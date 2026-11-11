"""A job's title in SQL: the write, the reads that carry it, and search.

``jobs.title`` is the name a person gave a job after creating it. The task
text stays in ``description``; a job that was never named has ``title`` NULL
and readers fall back to the description. Everything asserted here lives in
SQL, so it runs against real PostgreSQL.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "P" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=5)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE jobs CASCADE")
    try:
        yield store
    finally:
        await store.close()


async def _job(db, *, description: str, parent: str | None = None) -> str:
    job_id = uuid.uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, description, status, origin, parent_job_id)
            VALUES ($1, $2, 'completed', $3, $4)
            """,
            job_id,
            description,
            "subjob" if parent else "user",
            uuid.UUID(parent) if parent else None,
        )
    return str(job_id)


class TestUpdateJobTitle:
    @pytest.mark.asyncio
    async def test_a_new_job_has_no_title(self, db):
        job = await _job(db, description="Summarise the attached contract")

        assert (await db.get_job(job))["title"] is None

    @pytest.mark.asyncio
    async def test_a_rename_sets_the_title_and_keeps_the_task_text(self, db):
        job = await _job(db, description="Summarise the attached contract")

        assert await db.update_job_title(job, "Contract summary") is True

        stored = await db.get_job(job)
        assert stored["title"] == "Contract summary"
        assert stored["description"] == "Summarise the attached contract"

    @pytest.mark.asyncio
    async def test_none_clears_the_title(self, db):
        job = await _job(db, description="Summarise the attached contract")
        await db.update_job_title(job, "Contract summary")

        assert await db.update_job_title(job, None) is True

        assert (await db.get_job(job))["title"] is None

    @pytest.mark.asyncio
    async def test_an_unknown_or_malformed_job_reports_not_found(self, db):
        assert await db.update_job_title(str(uuid.uuid4()), "Name") is False
        assert await db.update_job_title("not-a-uuid", "Name") is False


class TestReadsCarryTheTitle:
    @pytest.mark.asyncio
    async def test_the_list_row_has_the_title_beside_the_description(self, db):
        named = await _job(db, description="Summarise the attached contract")
        unnamed = await _job(db, description="Translate the handbook")
        await db.update_job_title(named, "Contract summary")

        rows = {str(r["id"]): r for r in (await db.query_jobs(limit=25)).jobs}

        assert rows[named]["title"] == "Contract summary"
        assert rows[named]["description"] == "Summarise the attached contract"
        assert rows[unnamed]["title"] is None

    @pytest.mark.asyncio
    async def test_the_subjob_roster_has_the_title(self, db):
        parent = await _job(db, description="Build a calculator")
        child = await _job(db, description="Research phase", parent=parent)
        await db.update_job_title(child, "Research")

        roster = await db.get_job_subjob_roster(parent)

        assert [(str(r["id"]), r["title"]) for r in roster] == [(child, "Research")]


class TestSearchMatchesTheTitle:
    @pytest.mark.asyncio
    async def test_a_job_is_found_by_the_name_a_person_gave_it(self, db):
        """Once a job is renamed, its name is what people type into search."""
        named = await _job(db, description="Summarise the attached contract")
        await _job(db, description="Translate the handbook")
        await db.update_job_title(named, "Lease review")

        found = await db.query_jobs(search="lease", limit=25)

        assert [str(r["id"]) for r in found.jobs] == [named]

    @pytest.mark.asyncio
    async def test_the_task_text_still_matches_after_a_rename(self, db):
        named = await _job(db, description="Summarise the attached contract")
        await db.update_job_title(named, "Lease review")

        found = await db.query_jobs(search="contract", limit=25)

        assert [str(r["id"]) for r in found.jobs] == [named]
