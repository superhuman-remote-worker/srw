"""Phase 1 of knowledge-base/knowledge/issues/persistent_session_midturn_message_loss.md.

The persistent agent now writes thread_messages straight through its own pool
(``PostgresDB.save_thread_message``) instead of hopping through the orchestrator
REST endpoint. These tests pin the mechanics that the resume fix keys off:

  * the row carries a caller-supplied, idempotent ``id`` (``ON CONFLICT (id)``);
  * non-UUID provider/minted ids are coerced deterministically to the UUID PK;
  * ``seq`` is returned in the same round-trip;
  * the ``threads`` activity/turn bump still happens (no regression vs the
    orchestrator path).
"""

import uuid

import pytest
from unittest.mock import AsyncMock, MagicMock

from agent.database.postgres_db import (
    PostgresDB,
    _coerce_row_id,
    _THREAD_MSG_ID_NS,
)


def _fake_db(returning):
    """A PostgresDB whose ``acquire()`` yields a fake connection."""
    db = PostgresDB.__new__(PostgresDB)  # bypass __init__/real pool
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=returning)
    conn.execute = AsyncMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=cm)
    return db, conn


@pytest.mark.asyncio
async def test_returns_id_and_seq_and_upserts():
    db, conn = _fake_db({"id": "11111111-1111-1111-1111-111111111111", "seq": 42})
    result = await db.save_thread_message(
        thread_id="t1",
        role="ai",
        content="hi",
        turn_number=3,
        id="11111111-1111-1111-1111-111111111111",
    )
    # seq is handed back in the same call (the resume cursor source).
    assert result == {"id": "11111111-1111-1111-1111-111111111111", "seq": 42}

    sql = " ".join(conn.fetchrow.call_args[0][0].split())
    assert "ON CONFLICT (id) DO UPDATE" in sql, "must upsert, not plain insert"
    assert "RETURNING id, seq" in sql, "must return the seq cursor"
    # id is the first bound param (a valid UUID passes through unchanged).
    assert conn.fetchrow.call_args[0][1] == "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_provider_id_coerced_to_deterministic_uuid():
    """A non-UUID provider id (``chatcmpl-…``) must become a valid, stable UUID
    so the incremental write + turn-complete reconciliation hit one row."""
    db, conn = _fake_db({"id": "x", "seq": 1})
    await db.save_thread_message(
        thread_id="t1", role="ai", content="hi", turn_number=1, id="chatcmpl-abc123"
    )
    bound_id = conn.fetchrow.call_args[0][1]
    # Parses as a UUID (would have raised in asyncpg otherwise).
    uuid.UUID(bound_id)
    # Deterministic: same message id → same row id (idempotent upsert).
    assert bound_id == str(uuid.uuid5(_THREAD_MSG_ID_NS, "chatcmpl-abc123"))


@pytest.mark.asyncio
async def test_none_id_mints_fresh_uuid():
    db, conn = _fake_db({"id": "x", "seq": 1})
    await db.save_thread_message(
        thread_id="t1", role="user", content="hello", turn_number=1
    )
    bound_id = conn.fetchrow.call_args[0][1]
    uuid.UUID(bound_id)  # a valid, freshly-minted UUID


@pytest.mark.asyncio
async def test_bumps_thread_activity():
    db, conn = _fake_db({"id": "x", "seq": 1})
    await db.save_thread_message(thread_id="t1", role="ai", content="hi", turn_number=5)
    # The threads activity/turn update still runs (parity with orchestrator path).
    assert conn.execute.await_count == 1
    upd = " ".join(conn.execute.call_args[0][0].split())
    assert "UPDATE threads" in upd
    assert "last_activity" in upd and "total_turns" in upd


@pytest.mark.asyncio
async def test_component_columns_present():
    db, conn = _fake_db({"id": "x", "seq": 1})
    await db.save_thread_message(
        thread_id="t1",
        role="ai",
        content="hi",
        turn_number=1,
        provider="openai-chat",
        provider_raw={"object": "chat.completion", "id": "cc_1"},
        reasoning="thinking",
        id="11111111-1111-1111-1111-111111111111",
    )
    sql = conn.fetchrow.call_args[0][0]
    assert "provider" in sql and "provider_raw" in sql and "reasoning" in sql
    # JSON-encoded provider_raw reached the positional args.
    assert any(isinstance(a, str) and "cc_1" in a for a in conn.fetchrow.call_args[0])


def test_coerce_row_id_passthrough_and_stability():
    valid = "22222222-2222-2222-2222-222222222222"
    assert _coerce_row_id(valid) == valid  # already a UUID → unchanged
    # Non-UUID → deterministic across calls.
    assert _coerce_row_id("msg_xyz") == _coerce_row_id("msg_xyz")
    # None → fresh + valid, and distinct from the derived ones.
    minted = _coerce_row_id(None)
    uuid.UUID(minted)
    assert minted != _coerce_row_id(None)


# ---------------------------------------------------------------------------
# HF-2: save_thread_messages — the turn-complete reconcile batches into one
# pipelined executemany + a single threads bump (was N serial upserts).
# ---------------------------------------------------------------------------


def _fake_db_batch():
    """A PostgresDB whose ``acquire()`` yields a fake conn with a transaction()."""
    db = PostgresDB.__new__(PostgresDB)
    conn = AsyncMock()
    conn.executemany = AsyncMock()
    conn.execute = AsyncMock()
    tx = MagicMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=cm)
    return db, conn


@pytest.mark.asyncio
async def test_batch_upserts_all_rows_in_one_executemany():
    db, conn = _fake_db_batch()
    rows = [
        {
            "id": "m1",
            "role": "ai",
            "content": "a",
            "tool_calls": None,
            "turn_number": 3,
            "metrics": {"tokens": 1},
            "tool_call_id": None,
            "thinking": None,
        },
        {
            "id": "22222222-2222-2222-2222-222222222222",
            "role": "tool",
            "content": "b",
            "tool_calls": None,
            "turn_number": 3,
            "metrics": None,
            "tool_call_id": "tc1",
            "thinking": None,
        },
    ]
    await db.save_thread_messages("t1", rows)

    # ONE pipelined executemany for the whole turn (not N fetchrow calls).
    conn.executemany.assert_awaited_once()
    conn.fetchrow.assert_not_called()
    sql = " ".join(conn.executemany.call_args[0][0].split())
    assert "ON CONFLICT (id) DO UPDATE" in sql, "must upsert onto the incremental row"
    assert "RETURNING" not in sql, "batch upsert needs no RETURNING (seq unread)"

    argslist = conn.executemany.call_args[0][1]
    assert len(argslist) == 2
    uuid.UUID(argslist[0][0])  # non-UUID 'm1' coerced to a valid UUID PK
    assert argslist[1][0] == "22222222-2222-2222-2222-222222222222"  # passthrough
    # metrics JSON-encoded into the args (7th positional, index 6).
    assert argslist[0][6] == '{"tokens": 1}'
    assert argslist[1][6] is None  # tool row carries no metrics

    # ONE threads bump for the whole batch, with max(turn_number) bound.
    conn.execute.assert_awaited_once()
    upd = " ".join(conn.execute.call_args[0][0].split())
    assert "UPDATE threads" in upd and "total_turns" in upd
    assert conn.execute.call_args[0][2] == 3


@pytest.mark.asyncio
async def test_batch_empty_is_noop():
    db, conn = _fake_db_batch()
    await db.save_thread_messages("t1", [])
    conn.executemany.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_batch_insert_if_absent_rows_never_update_and_keep_row_order():
    """A compaction view (``insert_if_absent``) fills a missing row but never
    rewrites a landed one; runs execute in row order so a row either kind has
    to mint keeps its place in seq."""
    db, conn = _fake_db_batch()
    rows = [
        {"id": f"m{i}", "role": role, "content": role, "turn_number": 3}
        for i, role in enumerate(("ai", "tool", "tool", "ai"))
    ]
    rows[1]["insert_if_absent"] = True
    rows[2]["insert_if_absent"] = True
    await db.save_thread_messages("t1", rows)

    calls = conn.executemany.call_args_list
    sqls = [" ".join(call.args[0].split()) for call in calls]
    assert [len(call.args[1]) for call in calls] == [1, 2, 1]
    assert "ON CONFLICT (id) DO UPDATE" in sqls[0]
    # A view only inserts: on conflict the tool link alone follows the AI
    # row's (possibly remapped) id; content and every other column never do.
    conflict_clause = sqls[1].partition("ON CONFLICT (id)")[2]
    assert conflict_clause.strip() == (
        "DO UPDATE SET tool_call_id = EXCLUDED.tool_call_id"
    )
    assert "ON CONFLICT (id) DO UPDATE" in sqls[2]
    ids = [args[0] for call in calls for args in call.args[1]]
    assert ids == [_coerce_row_id(f"m{i}") for i in range(4)]
    # The flag selects the statement; it is not a column.
    assert all(
        len(args) == len(calls[0].args[1][0]) for c in calls for args in c.args[1]
    )
