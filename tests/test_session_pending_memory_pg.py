"""The session's durable pending memory set on a real PostgreSQL (WP4, D32).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D32: one pending set per conversation, stored in ``threads.metadata``,
updated atomically with ``jsonb_set``; B9: the agent writes it fenced like
the turn's other writes). Runs the agent's real SQL against
``schema_current.sql`` in a throwaway container; no migration is involved.

- A write sets the one key and leaves every other metadata key alone.
- A clear removes the set only when its id is the one the turn took in.
- A stateless claimant proves its exact lease first: a stale token raises
  ``LeaseLostError`` and writes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.database.postgres_db import PostgresDB
from agent.services.memory.pending_set import deserialize_pending_set
from shared.session_pending_memory import SESSION_PENDING_MEMORY_KEY

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    postgres = pytest.importorskip("testcontainers.postgres")
    try:
        container = postgres.PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
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
async def db(pg_dsn, _schema_applied):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE threads, users, run_queue CASCADE")
    store = PostgresDB.__new__(PostgresDB)
    store._pool = pool
    store._queries = {}
    try:
        yield store
    finally:
        await pool.close()


async def _thread(db: PostgresDB, *, lane: str = "pinned") -> str:
    owner, thread_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'pm', $2)",
            owner,
            f"{owner}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads (id, user_id, status, execution_lane, config_name, "
            "metadata) VALUES ($1, $2, 'active', $3, 'session_base', "
            """'{"config_override": {"llm": {"model": "m"}}, "datasource_ids": []}'::jsonb)""",
            thread_id,
            owner,
            lane,
        )
    return str(thread_id)


async def _metadata(db: PostgresDB, thread_id: str) -> dict:
    value = await db.fetchval(
        "SELECT metadata FROM threads WHERE id = $1::uuid", thread_id
    )
    return json.loads(value) if isinstance(value, str) else dict(value)


def _set(set_id: str) -> dict:
    return {
        "v": 1,
        "id": set_id,
        "turn": 3,
        "created_at": "2026-10-05T12:00:00+00:00",
        "memory": [
            {
                "id": str(uuid4()),
                "content": "The deploy window is Friday 18:00 CET.",
                "memory_type": "factual",
                "importance": 0.7,
                "source_phase": None,
                "token_count": 9,
            }
        ],
        "knowledge": [],
    }


@pytest.mark.asyncio
async def test_write_read_and_clear_touch_only_the_one_key(db):
    thread_id = await _thread(db)

    assert await db.get_thread_pending_memory(thread_id) is None
    assert await db.save_thread_pending_memory(thread_id, _set("set-1")) is True

    stored = await db.get_thread_pending_memory(thread_id)
    assert stored == _set("set-1") | {"memory": stored["memory"]}
    assert deserialize_pending_set(stored).pending_id == "set-1"
    metadata = await _metadata(db, thread_id)
    assert metadata["config_override"] == {"llm": {"model": "m"}}
    assert metadata["datasource_ids"] == []

    # A newer set overwrites the key in place.
    await db.save_thread_pending_memory(thread_id, _set("set-2"))
    assert (await db.get_thread_pending_memory(thread_id))["id"] == "set-2"

    # Clearing a set the turn did not take in leaves the newer one.
    assert await db.save_thread_pending_memory(thread_id, None, expected_id="set-1")
    assert (await db.get_thread_pending_memory(thread_id))["id"] == "set-2"

    assert await db.save_thread_pending_memory(thread_id, None, expected_id="set-2")
    metadata = await _metadata(db, thread_id)
    assert SESSION_PENDING_MEMORY_KEY not in metadata
    assert metadata["config_override"] == {"llm": {"model": "m"}}


@pytest.mark.asyncio
async def test_a_missing_thread_writes_nothing(db):
    assert await db.save_thread_pending_memory(str(uuid4()), _set("set-1")) is False


@pytest.mark.asyncio
async def test_a_stateless_claim_writes_only_under_its_exact_lease(db):
    thread_id = await _thread(db, lane="stateless")
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token, "
            "leased_by, input_seq, consumed_seq) "
            "VALUES ($1::uuid, 'session_turn', 'leased', 5, 'pod-a', 3, 2)",
            thread_id,
        )
    lease = LeaseHandle()
    token = current_lease.set(lease)
    try:
        lease.update(thread_id, 5)
        assert await db.save_thread_pending_memory(thread_id, _set("set-1"))
        assert (await db.get_thread_pending_memory(thread_id))["id"] == "set-1"

        lease.update(thread_id, 4)  # a zombie with the previous token
        with pytest.raises(LeaseLostError):
            await db.save_thread_pending_memory(thread_id, _set("stale"))
        assert lease.lost.is_set()
        with pytest.raises(LeaseLostError):
            await db.save_thread_pending_memory(thread_id, None, expected_id="set-1")
    finally:
        current_lease.reset(token)

    assert (await db.get_thread_pending_memory(thread_id))["id"] == "set-1"
