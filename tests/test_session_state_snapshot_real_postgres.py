"""The durable session snapshot's running-calls query on a real PostgreSQL.

parallel_subagents.md F17: a session delegation batch journals one
``tool.started`` per call, and the snapshot must name every call that has no
``tool.completed`` yet (``running_tools``), not only the latest. The unit suite
drives the statement through a scripted fake; this runs the real SQL (its
``LIMIT $4`` bind included) against ``schema_current.sql`` in a throwaway
PostgreSQL container.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.session_state_snapshot import build_session_state_snapshot

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
        container = postgres.PostgresContainer("postgres:15")
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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE thread_events, threads, users CASCADE")
    try:
        yield store
    finally:
        await store.close()


async def _thread_with_journal(db: PostgresDB, frames: list[tuple[str, dict]]) -> UUID:
    owner, thread_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'f17', $2)",
            owner,
            f"{owner}@example.test",
        )
        await conn.execute(
            "INSERT INTO threads (id, user_id, status, execution_lane, config_name, "
            "metadata) VALUES ($1, $2, 'active', 'pinned', 'session_base', "
            "'{}'::jsonb)",
            thread_id,
            owner,
        )
        for seq, (kind, payload) in enumerate(frames, 1):
            await conn.execute(
                "INSERT INTO thread_events (thread_id, epoch, seq, kind, payload) "
                "VALUES ($1, 0, $2, $3, $4::jsonb)",
                thread_id,
                seq,
                kind,
                json.dumps(payload),
            )
    return thread_id


def _started(call_id: str, brief: str) -> tuple[str, dict]:
    return (
        "tool.started",
        {"id": call_id, "tool": "delegate_agent", "args": {"brief": brief}},
    )


@pytest.mark.asyncio
async def test_every_unmatched_call_of_a_batch_is_running(db):
    thread_id = await _thread_with_journal(
        db,
        [
            ("turn.started", {"turn_id": 3}),
            _started("d1", "a"),
            _started("d2", "b"),
            _started("d3", "c"),
            ("tool.completed", {"id": "d2", "result": "report"}),
        ],
    )

    snapshot = await build_session_state_snapshot(db, str(thread_id))

    assert snapshot is not None
    assert snapshot["turn_in_flight"] is True
    assert snapshot["running_tools"] == [
        {"id": "d1", "tool": "delegate_agent", "args": {"brief": "a"}},
        {"id": "d3", "tool": "delegate_agent", "args": {"brief": "c"}},
    ]
    # Older Cockpits keep reading the latest unmatched call.
    assert snapshot["running_tool"] == {
        "id": "d3",
        "tool": "delegate_agent",
        "args": {"brief": "c"},
    }


@pytest.mark.asyncio
async def test_a_turn_boundary_clears_every_running_call(db):
    thread_id = await _thread_with_journal(
        db,
        [
            ("turn.started", {"turn_id": 3}),
            _started("d1", "a"),
            _started("d2", "b"),
            ("turn.completed", {"turn_id": 3}),
        ],
    )

    snapshot = await build_session_state_snapshot(db, str(thread_id))

    assert snapshot is not None
    assert snapshot["running_tools"] == []
    assert snapshot["running_tool"] is None
