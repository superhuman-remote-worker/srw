"""``role='context'`` rows on a real PostgreSQL: write, re-save, project, restore.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(B1, B2); spec:
knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md §F
1-4, sub-step 2.2. Runs the agent's real upsert and history SQL against
``schema_current.sql`` in a throwaway container (``thread_messages.role`` is
``varchar(20)`` with no CHECK, so no migration is involved).

- The upsert keeps a context row's ``additional_kwargs`` when the same id is
  re-saved without them (the reconcile pass, the batch writer), and takes new
  ones when a re-save brings them.
- The history projection reads ``additional_kwargs`` for context rows only.
- ``_db_rows_to_lc_messages`` rebuilds the entries in conversation order.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.api.persistent_app import _db_rows_to_lc_messages
from agent.core.thread_messages import _serialize_message_row
from agent.database.postgres_db import PostgresDB
from shared.runtime.core.context_entries import (
    SRW_INJECTION_KEY,
    entry_meta,
    is_context_entry,
    make_context_entry,
)

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
        await conn.execute("TRUNCATE threads, users CASCADE")
    store = PostgresDB.__new__(PostgresDB)
    store._pool = pool
    store._queries = {}
    try:
        yield store
    finally:
        await pool.close()


async def _thread(db: PostgresDB) -> str:
    owner, thread_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'ctx', $2)",
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
    return str(thread_id)


def _entry(kind: str, body: str, *, turn: int = 1) -> HumanMessage:
    entry = make_context_entry(
        kind,
        body,
        section=kind,
        items=[{"key": f"{kind}-1", "hash": "b" * 16, "handle": None}]
        if kind in ("memory", "knowledge")
        else (),
        turn=turn,
    )
    entry.id = str(uuid4())
    return entry


async def _stored_kwargs(db: PostgresDB, message_id: str):
    async with db.acquire() as conn:
        value = await conn.fetchval(
            "SELECT additional_kwargs FROM thread_messages WHERE id = $1",
            UUID(message_id),
        )
    return json.loads(value) if isinstance(value, str) else value


async def _save(db: PostgresDB, thread_id: str, message, turn: int = 1) -> int:
    saved = await db.save_thread_message(
        thread_id=thread_id, **_serialize_message_row(message, turn)
    )
    return int(saved["seq"])


@pytest.mark.asyncio
async def test_a_resave_without_kwargs_keeps_the_schema(db):
    thread_id = await _thread(db)
    entry = _entry("memory", "[m:3f9a2c] The brief lives in docs/brief.md.")
    schema = {SRW_INJECTION_KEY: entry.additional_kwargs[SRW_INJECTION_KEY]}

    await _save(db, thread_id, entry)
    assert await _stored_kwargs(db, entry.id) == schema

    # The single writer re-saves the same id without kwargs ...
    bare = {k: v for k, v in _serialize_message_row(entry, 1).items()}
    bare.pop("additional_kwargs")
    await db.save_thread_message(thread_id=thread_id, **bare)
    assert await _stored_kwargs(db, entry.id) == schema

    # ... and so does the turn-end reconcile batch.
    await db.save_thread_messages(thread_id, [bare])
    assert await _stored_kwargs(db, entry.id) == schema

    # A re-save that brings kwargs still takes them.
    newer = {SRW_INJECTION_KEY: {**schema[SRW_INJECTION_KEY], "hash": "c" * 16}}
    await db.save_thread_messages(thread_id, [{**bare, "additional_kwargs": newer}])
    assert await _stored_kwargs(db, entry.id) == newer


@pytest.mark.asyncio
async def test_the_projection_reads_kwargs_for_context_rows_only(db):
    thread_id = await _thread(db)
    human = HumanMessage(content="go", id=str(uuid4()))
    entry = _entry("knowledge", "Note: summaries are capped at 300 words.")
    await _save(db, thread_id, human)
    await _save(db, thread_id, entry)
    # Another writer left kwargs on a non-context row: the diet still holds.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE thread_messages SET additional_kwargs = '{\"x\": 1}'::jsonb "
            "WHERE id = $1",
            UUID(human.id),
        )

    rows = await db.get_thread_messages_history(thread_id, order_by_seq=True)

    assert [row["role"] for row in rows] == ["human", "context"]
    assert "additional_kwargs" not in rows[0]
    assert rows[1]["additional_kwargs"] == {
        SRW_INJECTION_KEY: entry.additional_kwargs[SRW_INJECTION_KEY]
    }


@pytest.mark.asyncio
async def test_restore_rebuilds_the_entries_in_conversation_order(db):
    """Write order is the loop's; an entry stored between one batch's
    results comes back behind the batch's last result, and an unreadable
    context row is dropped."""
    thread_id = await _thread(db)
    question = HumanMessage(content="Summarize the brief.", id=str(uuid4()))
    boundary = _entry("turn_boundary", "App Guide: turn 1.")
    memory = _entry("memory", "[m:3f9a2c] The brief lives in docs/brief.md.")
    call = AIMessage(
        content="",
        tool_calls=[
            {"name": "read_file", "args": {"path": "a"}, "id": "c1"},
            {"name": "read_file", "args": {"path": "b"}, "id": "c2"},
        ],
        id=str(uuid4()),
    )
    first = ToolMessage(content="brief", tool_call_id="c1", id=str(uuid4()))
    between = _entry("knowledge", "Note: summaries are capped at 300 words.")
    second = ToolMessage(content="style", tool_call_id="c2", id=str(uuid4()))
    answer = AIMessage(content="Here is the summary.", id=str(uuid4()))
    for message in (question, memory, boundary, call, first, between, second, answer):
        await _save(db, thread_id, message)
    lost = _entry("citation", "stale")
    await _save(db, thread_id, lost)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE thread_messages SET additional_kwargs = NULL WHERE id = $1",
            UUID(lost.id),
        )

    rows = await db.get_thread_messages_history(
        thread_id=thread_id, limit=1000, newest_first=True
    )
    restored = _db_rows_to_lc_messages(rows)

    expected = [question, memory, boundary, call, first, second, between, answer]
    assert [m.id for m in restored] == [m.id for m in expected]
    for got, want in zip(restored, expected):
        assert got.content == want.content
        assert is_context_entry(got) == is_context_entry(want)
        if is_context_entry(want):
            assert entry_meta(got) == entry_meta(want)
