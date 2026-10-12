"""A project-folder copy stuck in ``seeding`` is released on a real
PostgreSQL (main cloud slice 6.3, review P2-8).

The dispatcher holds a job whose ``context.cloud_baseline.state`` is
``seeding``. A seed lost with its pod, or a request cut off before it fired
one, would hold it in ``created`` forever; the leader's stale detector fails
such a copy past ``SEED_STALE_AFTER_SECONDS`` so the job dispatches without
it. Each copy counts from the seed's own ``since`` stamp, else from the
job's last write; a loop's copy is its spawn's to settle.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.job_cloud_baseline import (
    SEED_STALE_AFTER_SECONDS,
    SEED_STALE_REASON,
)

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA = _ROOT / "src/orchestrator/database/schema_current.sql"


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
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(_SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE jobs CASCADE")
    try:
        yield store
    finally:
        await store.close()


def _ago(seconds: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


_STALE = SEED_STALE_AFTER_SECONDS + 60
_FRESH = 60


async def _job(
    db,
    *,
    context: dict,
    status: str = "created",
    updated_ago: float = 0,
) -> str:
    async with db.acquire() as conn:
        job_id = await conn.fetchval(
            """
            INSERT INTO jobs (description, status, context, created_at, updated_at)
            VALUES ('copy', $1, $2::jsonb, $3, $3)
            RETURNING id
            """,
            status,
            json.dumps(context),
            _ago(updated_ago),
        )
    return str(job_id)


async def _baseline(db, job_id: str) -> dict:
    async with db.acquire() as conn:
        context = await conn.fetchval(
            "SELECT context FROM jobs WHERE id = $1::uuid", job_id
        )
    return json.loads(context)["cloud_baseline"]


async def _release(db, **kwargs) -> set[str]:
    return set(
        await db.release_stale_cloud_baseline_seeds(
            stale_after_seconds=SEED_STALE_AFTER_SECONDS,
            reason=SEED_STALE_REASON,
            **kwargs,
        )
    )


def _seeding(since_ago: float | None = None) -> dict:
    baseline: dict = {"state": "seeding", "project_slug": "team"}
    if since_ago is not None:
        baseline["since"] = _ago(since_ago).isoformat()
    return {"cloud_baseline": baseline}


@pytest.mark.asyncio
async def test_a_lost_seed_or_an_unfired_copy_is_released_past_the_bound(db):
    lost_seed = await _job(db, context=_seeding(since_ago=_STALE))
    # Admission's stamp, no seed ever fired: the job's last write counts.
    unfired = await _job(db, context=_seeding(), updated_ago=_STALE)
    paused = await _job(db, context=_seeding(since_ago=_STALE), status="paused")
    kept = {
        # A seed still walking a large folder.
        await _job(db, context=_seeding(since_ago=_FRESH), updated_ago=_STALE),
        await _job(db, context=_seeding(), updated_ago=_FRESH),
        # A loop's copy is its spawn's to settle.
        await _job(
            db,
            context={**_seeding(since_ago=_STALE), "loop_id": "loop-1"},
            updated_ago=_STALE,
        ),
        # Not waiting for dispatch.
        await _job(db, context=_seeding(since_ago=_STALE), status="processing"),
    }
    ready = await _job(
        db, context={"cloud_baseline": {"state": "ready"}}, updated_ago=_STALE
    )

    assert await _release(db) == {lost_seed, unfired, paused}

    released = await _baseline(db, lost_seed)
    assert released["state"] == "failed"
    assert released["error"] == SEED_STALE_REASON
    # What the seed recorded stays beside the verdict.
    assert released["project_slug"] == "team"
    assert "since" in released
    for job_id in kept:
        assert (await _baseline(db, job_id))["state"] == "seeding"
    assert (await _baseline(db, ready)) == {"state": "ready"}
    # Released once: a second sweep finds nothing.
    assert await _release(db) == set()


@pytest.mark.asyncio
async def test_each_sweep_is_bounded(db):
    stale = {await _job(db, context=_seeding(since_ago=_STALE)) for _ in range(3)}

    first = await _release(db, limit=2)
    assert len(first) == 2 and first <= stale
    assert await _release(db, limit=2) == stale - first


@pytest.mark.asyncio
async def test_an_unreadable_since_falls_back_to_the_jobs_last_write(db):
    garbled = await _job(
        db,
        context={"cloud_baseline": {"state": "seeding", "since": "yesterday"}},
        updated_ago=_STALE,
    )
    fresh = await _job(
        db,
        context={"cloud_baseline": {"state": "seeding", "since": "yesterday"}},
    )

    assert await _release(db) == {garbled}
    assert (await _baseline(db, fresh))["state"] == "seeding"
