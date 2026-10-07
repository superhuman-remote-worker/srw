"""Retirement-owned terminalization of persistent-session children."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

import orchestrator.database.postgres as postgres_module
from orchestrator.database.postgres import PostgresDB
from shared.persistent_input_delivery import message_row_id
from shared.session_subagent_authority import session_subagent_delivery_id


PARENT = UUID("aaaaaaaa-1111-4222-8333-444444444444")
CHILD = UUID("bbbbbbbb-1111-4222-8333-444444444444")
GENERATION = UUID("cccccccc-1111-4222-8333-444444444444")
RETIREMENT = UUID("dddddddd-1111-4222-8333-444444444444")


def _compact(sql: str) -> str:
    return " ".join(sql.split())


def _conn() -> AsyncMock:
    conn = AsyncMock()
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=transaction)
    transaction.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction)
    return conn


def _db(conn: AsyncMock) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    return db


def _child(*, background: bool) -> dict:
    return {
        "id": CHILD,
        "runtime_generation": GENERATION,
        "subagent_handle": "tester-abcd",
        "subagent_type": "tester",
        "total_turns": 2,
        "total_tokens": 17,
        "report_path": ".subagents/tester-abcd/report.md",
        "metadata": {"subagent": {"run_in_background": background}},
    }


@pytest.mark.asyncio
async def test_foreground_retirement_locks_parent_then_children_without_lane_b_event():
    conn = _conn()
    conn.fetchrow.return_value = {
        "id": PARENT,
        "kind": "session",
        "execution_lane": "pinned",
    }
    conn.fetch.return_value = [_child(background=False)]
    conn.execute.return_value = "UPDATE 1"
    db = _db(conn)

    result = await db._terminalize_live_session_subagents_for_retirement(
        conn,
        parent_thread_id=PARENT,
        execution_lane="pinned",
        disposition="ended",
    )

    assert result == {"terminalized": 1, "deliveries": 0}
    parent_lock = _compact(conn.fetchrow.await_args_list[0].args[0])
    child_lock = _compact(conn.fetch.await_args.args[0])
    child_update = _compact(conn.execute.await_args_list[0].args[0])
    assert parent_lock.endswith("WHERE id=$1::uuid FOR UPDATE")
    assert "ORDER BY created_at, id FOR UPDATE" in child_lock
    assert "parent_job_id IS NULL" in child_lock
    assert "parent_thread_id = $1::uuid" in child_lock
    # An End, not a lost runtime: the child is cancelled, not interrupted.
    assert "subagent_status = $5" in child_update
    assert "subagent_outcome = $6" in child_update
    assert conn.execute.await_args_list[0].args[1:] == (
        CHILD,
        PARENT,
        GENERATION,
        "the parent session retired before child completion",
        "cancelled",
        "cancelled:parent_retired",
    )
    assert all(
        "thread_messages" not in _compact(call.args[0])
        and "thread_input_deliveries" not in _compact(call.args[0])
        for call in conn.fetchrow.await_args_list
    )


@pytest.mark.asyncio
async def test_pinned_background_retirement_persists_generation_stable_event():
    delivery_id = session_subagent_delivery_id(CHILD, GENERATION)
    stable_message_id = message_row_id(delivery_id)
    parent = {"id": PARENT, "kind": "session", "execution_lane": "pinned"}
    message = {
        "id": stable_message_id,
        "thread_id": PARENT,
        "role": "event",
    }
    delivery = {
        "delivery_id": delivery_id,
        "thread_id": PARENT,
        "message_id": stable_message_id,
        "source": "subagent",
        "execution_lane": "pinned",
        "state": "persisted",
    }
    conn = _conn()

    async def fetchrow(sql, *args):
        compact = _compact(sql)
        if compact.startswith("SELECT id, kind, execution_lane FROM threads"):
            return parent
        if compact.startswith("INSERT INTO thread_messages"):
            return {**message, "content": args[2]}
        if compact.startswith("INSERT INTO thread_input_deliveries"):
            return delivery
        raise AssertionError(compact)

    conn.fetchrow.side_effect = fetchrow
    conn.fetch.return_value = [_child(background=True)]
    conn.execute.return_value = "UPDATE 1"
    db = _db(conn)

    assert await db._terminalize_live_session_subagents_for_retirement(
        conn,
        parent_thread_id=PARENT,
        execution_lane="pinned",
        disposition="suspended",
    ) == {"terminalized": 1, "deliveries": 1}

    message_call = next(
        call
        for call in conn.fetchrow.await_args_list
        if _compact(call.args[0]).startswith("INSERT INTO thread_messages")
    )
    assert message_call.args[1:3] == (stable_message_id, PARENT)
    assert "cancelled:parent_retired" in message_call.args[3]
    assert "was suspended before this child could finish" in message_call.args[3]
    delivery_call = next(
        call
        for call in conn.fetchrow.await_args_list
        if _compact(call.args[0]).startswith("INSERT INTO thread_input_deliveries")
    )
    assert delivery_call.args[1:] == (delivery_id, PARENT, stable_message_id)
    delivery_sql = _compact(delivery_call.args[0])
    assert "'subagent', 'pinned'" in delivery_sql
    assert "state" not in delivery_sql.split("VALUES", 1)[0]


@pytest.mark.asyncio
async def test_matching_background_evidence_replay_is_idempotent():
    delivery_id = session_subagent_delivery_id(CHILD, GENERATION)
    stable_message_id = message_row_id(delivery_id)
    conn = _conn()
    envelope: str | None = None

    async def fetchrow(sql, *args):
        nonlocal envelope
        compact = _compact(sql)
        if compact.startswith("SELECT id, kind, execution_lane FROM threads"):
            return {"id": PARENT, "kind": "session", "execution_lane": "pinned"}
        if compact.startswith("INSERT INTO thread_messages"):
            envelope = args[2]
            return None
        if compact.startswith(
            "SELECT id, thread_id, role, content FROM thread_messages"
        ):
            return {
                "id": stable_message_id,
                "thread_id": PARENT,
                "role": "event",
                "content": envelope,
            }
        if compact.startswith("INSERT INTO thread_input_deliveries"):
            return None
        if compact.startswith("SELECT delivery_id, thread_id, message_id"):
            return {
                "delivery_id": delivery_id,
                "thread_id": PARENT,
                "message_id": stable_message_id,
                "source": "subagent",
                "execution_lane": "pinned",
                "state": "owned",
            }
        raise AssertionError(compact)

    conn.fetchrow.side_effect = fetchrow
    conn.fetch.return_value = [_child(background=True)]
    conn.execute.return_value = "UPDATE 1"
    db = _db(conn)

    assert await db._terminalize_live_session_subagents_for_retirement(
        conn,
        parent_thread_id=PARENT,
        execution_lane="pinned",
        disposition="ended",
    ) == {"terminalized": 1, "deliveries": 1}


@pytest.mark.asyncio
async def test_conflicting_background_event_aborts_retirement():
    conn = _conn()

    async def fetchrow(sql, *args):
        compact = _compact(sql)
        if compact.startswith("SELECT id, kind, execution_lane FROM threads"):
            return {"id": PARENT, "kind": "session", "execution_lane": "pinned"}
        if compact.startswith("INSERT INTO thread_messages"):
            return None
        if compact.startswith(
            "SELECT id, thread_id, role, content FROM thread_messages"
        ):
            return {
                "id": message_row_id(session_subagent_delivery_id(CHILD, GENERATION)),
                "thread_id": PARENT,
                "role": "event",
                "content": "conflicting evidence",
            }
        raise AssertionError(compact)

    conn.fetchrow.side_effect = fetchrow
    conn.fetch.return_value = [_child(background=True)]
    conn.execute.return_value = "UPDATE 1"
    db = _db(conn)

    with pytest.raises(RuntimeError, match="conflicts with transcript"):
        await db._terminalize_live_session_subagents_for_retirement(
            conn,
            parent_thread_id=PARENT,
            execution_lane="pinned",
            disposition="ended",
        )


@pytest.mark.asyncio
async def test_stateless_retirement_tombstones_even_malformed_background_without_event():
    conn = _conn()
    conn.fetchrow.return_value = {
        "id": PARENT,
        "kind": "session",
        "execution_lane": "stateless",
    }
    conn.fetch.return_value = [_child(background=True)]
    conn.execute.return_value = "UPDATE 1"
    db = _db(conn)

    assert await db._terminalize_live_session_subagents_for_retirement(
        conn,
        parent_thread_id=PARENT,
        execution_lane="stateless",
        disposition="ended",
    ) == {"terminalized": 1, "deliveries": 0}
    assert conn.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_pinned_soft_settlement_invokes_child_tombstone_in_same_transaction(
    monkeypatch,
):
    conn = _conn()
    conn.fetchrow.return_value = {
        "runtime_retirement_context": {"settle_status": "ended"},
        "runtime_retirement_stage_receipt": None,
        "runtime_retirement_local_quiescence": {},
        "metadata": {},
    }
    conn.fetchval.side_effect = [False, False, PARENT]
    conn.fetch.return_value = []
    conn.execute.return_value = "INSERT 0 1"
    db = _db(conn)
    tombstone = AsyncMock(return_value={"terminalized": 0, "deliveries": 0})
    db._terminalize_live_session_subagents_for_retirement = tombstone
    monkeypatch.setattr(
        postgres_module,
        "_pinned_retirement_local_quiescence_matches",
        lambda *args, **kwargs: True,
    )
    append = AsyncMock()
    monkeypatch.setattr("shared.event_journal.append_system_frame", append)

    assert await db.settle_pinned_thread_retirement(
        str(PARENT),
        token=str(RETIREMENT),
        generation=str(GENERATION),
        final_status="ended",
    )
    # The parent row is still bound to its agent while children retire, so
    # the tombstone must not write it (deferred reciprocity fence).
    tombstone.assert_awaited_once_with(
        conn,
        parent_thread_id=PARENT,
        execution_lane="pinned",
        disposition="ended",
        touch_parent_activity=False,
        runtime_lost=False,
    )
    append.assert_awaited_once()


@pytest.mark.asyncio
async def test_stateless_soft_finish_invokes_child_tombstone_before_parent_update(
    monkeypatch,
):
    marker = {
        "terminal_token": 7,
        "permanent": False,
        "runtime_incarnation": None,
        "workspace_absence_proven": False,
    }
    metadata = {"workspace_container": {}, "_workspace_binding": {}}
    conn = _conn()
    conn.fetchrow.side_effect = [
        {"status": "ended", "execution_lane": "stateless", "metadata": metadata},
        {"unit_kind": "session_turn", "state": "done", "lease_token": 7},
    ]
    conn.fetchval.return_value = PARENT
    db = _db(conn)
    tombstone = AsyncMock(return_value={"terminalized": 0, "deliveries": 0})
    db._terminalize_live_session_subagents_for_retirement = tombstone
    monkeypatch.setattr(
        "shared.session_retirement.stateless_retirement_release_authorized",
        lambda value: marker,
    )

    assert await db.finish_stateless_thread_workspace_retirement(str(PARENT))
    tombstone.assert_awaited_once_with(
        conn,
        parent_thread_id=str(PARENT),
        execution_lane="stateless",
        disposition="ended",
    )
    update_sql = _compact(conn.fetchval.await_args.args[0])
    assert update_sql.startswith("UPDATE threads SET metadata")
