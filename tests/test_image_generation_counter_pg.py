"""The paid-image counter on real Postgres (image generation WP4, decision D1).

One conditional UPDATE reserves against the cap on ``jobs.context`` or
``threads.metadata``; concurrent reservations cannot overrun it; a release
gives images back with a floor of 0; other keys in the JSON are untouched.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from agent.database.postgres_db import PostgresDB

JOB = str(uuid.uuid4())
THREAD = str(uuid.uuid4())


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def db(pg_dsn):
    connection = await asyncpg.connect(pg_dsn)
    try:
        await connection.execute(
            """
            DROP TABLE IF EXISTS jobs; DROP TABLE IF EXISTS threads;
            CREATE TABLE jobs (id uuid PRIMARY KEY, context jsonb);
            CREATE TABLE threads (id uuid PRIMARY KEY, metadata jsonb NOT NULL DEFAULT '{}');
            """
        )
        await connection.execute(
            "INSERT INTO jobs VALUES ($1::uuid, $2::jsonb)",
            JOB,
            json.dumps({"keep": "me"}),
        )
        await connection.execute("INSERT INTO threads (id) VALUES ($1::uuid)", THREAD)
    finally:
        await connection.close()
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_reserve_up_to_the_cap_then_refuse(db):
    assert await db.reserve_image_generation("job", JOB, 2, 3) == (True, 2)
    assert await db.reserve_image_generation("job", JOB, 2, 3) == (False, 2)
    assert await db.reserve_image_generation("job", JOB, 1, 3) == (True, 3)
    context = json.loads(
        await db.fetchval("SELECT context::text FROM jobs WHERE id = $1::uuid", JOB)
    )
    assert context == {"keep": "me", "image_generation": {"paid_images": 3}}


@pytest.mark.asyncio
async def test_release_floors_at_zero_and_zero_cap_means_no_cap(db):
    assert await db.reserve_image_generation("thread", THREAD, 4, 0) == (True, 4)
    await db.release_image_generation("thread", THREAD, 10)
    used = await db.fetchval(
        "SELECT (metadata->'image_generation'->>'paid_images')::int FROM threads "
        "WHERE id = $1::uuid",
        THREAD,
    )
    assert used == 0


@pytest.mark.asyncio
async def test_concurrent_reservations_cannot_overrun_the_cap(db):
    results = await asyncio.gather(
        *(db.reserve_image_generation("job", JOB, 1, 5) for _ in range(12))
    )
    assert sum(1 for ok, _ in results if ok) == 5


@pytest.mark.asyncio
async def test_a_missing_row_reserves_nothing(db):
    assert await db.reserve_image_generation("job", str(uuid.uuid4()), 1, 5) == (
        False,
        0,
    )
