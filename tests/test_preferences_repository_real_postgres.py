"""Existing preference JSONB semantics through the real PostgreSQL facade.

Testcontainers owns the database; no ambient database URL or cluster is used.
The concurrency test observes both real UPDATE statements blocked on the row.
"""

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB

MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
)
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def migrated_dsn(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(pool, MIGRATIONS)
    return pg_dsn


@pytest_asyncio.fixture
async def db(migrated_dsn):
    database = PostgresDB(
        connection_string=migrated_dsn,
        min_connections=1,
        max_connections=3,
        command_timeout=10,
    )
    await database.connect()
    try:
        yield database
    finally:
        await database.close()


async def seed_user(db, settings):
    user_id = uuid4()
    async with db.acquire() as connection:
        await connection.execute(
            "INSERT INTO users (id, email, display_name, settings) VALUES ($1, $2, 'Preference boundary', $3::jsonb)",
            user_id,
            f"{user_id}@preferences.test",
            json.dumps(settings),
        )
    return str(user_id)


async def test_explicit_null_and_whole_subobject_replacement_keep_sql_semantics(db):
    user = await seed_user(
        db,
        {
            "language": "en",
            "unrelated": {"retained": True, "null": None},
            "persistent_agent": {"model": "old", "permission_mode": "read_only"},
        },
    )
    assert (
        await db.update_user_settings(
            user,
            {
                "language": None,
                "persistent_agent": {
                    "model": "new",
                    "nested": {"remove": None, "keep": True},
                },
            },
        )
        is True
    )
    assert await db.get_user_settings(user) == {
        "unrelated": {"retained": True},
        "persistent_agent": {"model": "new", "nested": {"keep": True}},
    }


async def test_concurrent_null_reset_and_subobject_patch_do_not_lose_sibling_updates(
    db, migrated_dsn
):
    user = await seed_user(
        db,
        {
            "language": "en",
            "retained": True,
            "persistent_agent": {"model": "old", "permission_mode": "read_only"},
        },
    )
    observer = await asyncpg.connect(migrated_dsn)
    started = asyncio.Queue()
    tasks = []
    patches = (
        {"language": None},
        {"persistent_agent": {"model": "new", "remove": None}},
    )

    async def writer(patch):
        async with db.transaction_scope() as connection:
            await started.put(await connection.fetchval("SELECT pg_backend_pid()"))
            return await db.update_user_settings(user, patch)

    try:
        async with db.acquire() as blocker:
            transaction = blocker.transaction()
            await transaction.start()
            try:
                await blocker.fetchrow(
                    "SELECT id FROM users WHERE id=$1 FOR UPDATE", UUID(user)
                )
                tasks = [asyncio.create_task(writer(patch)) for patch in patches]
                async with asyncio.timeout(5):
                    pids = [await started.get(), await started.get()]
                    assert len(set(pids)) == 2
                    while True:
                        rows = await observer.fetch(
                            "SELECT pid FROM pg_stat_activity WHERE pid=ANY($1::int[]) "
                            "AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid)) > 0 "
                            "AND query ILIKE '%UPDATE users%'",
                            pids,
                        )
                        if {row["pid"] for row in rows} == set(pids):
                            break
            finally:
                await transaction.rollback()
        async with asyncio.timeout(5):
            assert await asyncio.gather(*tasks) == [True, True]
        assert await db.get_user_settings(user) == {
            "retained": True,
            "persistent_agent": {"model": "new"},
        }
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await observer.close()
