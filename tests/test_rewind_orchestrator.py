"""Detached-rewind REST endpoint + orchestrator-side rewind SQL."""

from tests import _b09_control_seams as control_seams

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest
from orchestrator.schemas import thread_lifecycle as thread_lifecycle_module
from orchestrator.security import access as access_module


def test_orchestrator_live_readers_filter_tombstones():
    from orchestrator.database import postgres as mod

    for meth in (
        "get_thread_messages_history",
        "get_thread_messages_page",
        "get_thread_message_count",
        "get_officer_last_engagement",
    ):
        src = inspect.getsource(getattr(mod.PostgresDB, meth))
        assert "rewound_at IS NULL" in src, f"{meth} must filter tombstones"


def test_get_live_thread_message_malformed_id_returns_none_not_500():
    """Fix 8: the orchestrator twin has no _coerce_row_id (that's agent-side
    only) — a malformed id must resolve to None, which the endpoint already
    turns into 404, instead of an unhandled 500 from the uuid codec."""
    import asyncpg

    from orchestrator.database.postgres import PostgresDB

    class _FakeConn:
        async def fetchrow(self, q, *a):
            raise asyncpg.exceptions.InvalidTextRepresentationError(
                "invalid input syntax for type uuid"
            )

    class _FakeAcquire:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *exc):
            return False

    db = PostgresDB.__new__(PostgresDB)
    db.acquire = lambda: _FakeAcquire()

    out = asyncio.run(db.get_live_thread_message("tid-1", "msg_not_a_real_uuid"))
    assert out is None


def test_get_live_thread_message_client_side_value_error_returns_none():
    """Some asyncpg versions/paths raise the plain stdlib ValueError from
    uuid.UUID() client-side, before ever reaching the server — must be
    caught the same as the server-side DataError path above."""
    from orchestrator.database.postgres import PostgresDB

    class _FakeConn:
        async def fetchrow(self, q, *a):
            raise ValueError("badly formed hexadecimal UUID string")

    class _FakeAcquire:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *exc):
            return False

    db = PostgresDB.__new__(PostgresDB)
    db.acquire = lambda: _FakeAcquire()

    out = asyncio.run(db.get_live_thread_message("tid-1", "msg_not_a_real_uuid"))
    assert out is None


def test_get_live_thread_message_happy_path_returns_row():
    from orchestrator.database.postgres import PostgresDB

    class _FakeConn:
        async def fetchrow(self, q, *a):
            return {"seq": 8, "role": "human", "content": "the prompt"}

    class _FakeAcquire:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, *exc):
            return False

    db = PostgresDB.__new__(PostgresDB)
    db.acquire = lambda: _FakeAcquire()

    out = asyncio.run(
        db.get_live_thread_message("tid-1", "11111111-1111-1111-1111-111111111111")
    )
    assert out == {"seq": 8, "role": "human", "content": "the prompt"}


def test_apply_thread_rewind_locks_sweeps_bumps_and_journals():
    """apply_thread_rewind now routes the epoch bump + rewind.done frame
    through the shared src.shared.event_journal helpers (M4 port): the bump
    must also reset events_seq_hwm (the pre-0116 inline SQL didn't), and the
    frame is allocated from the reset high-water mark ((new_epoch, 1))."""
    from orchestrator.database.postgres import PostgresDB

    class _FakeTxn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _FakeConn:
        def __init__(self):
            self.calls = []
            self.memory_cursor = 10

        def transaction(self):
            return _FakeTxn()

        async def execute(self, q, *a):
            self.calls.append(q)
            if "UPDATE thread_session_runtime_state" in q:
                self.memory_cursor = min(self.memory_cursor, int(a[1]))

        async def fetchrow(self, q, *a):
            self.calls.append(q)
            # Branch on statement: thread_rewinds INSERT, the shared
            # bump_epoch UPDATE, and the shared append_system_frame CTE all
            # arrive here with different row shapes.
            if "thread_rewinds" in q:
                return {"id": "33333333-3333-3333-3333-333333333333"}
            if "events_epoch = events_epoch + 1" in q:
                return {"events_epoch": 9}
            if "INSERT INTO thread_events" in q:
                return {"epoch": 9, "seq": 1}
            return {"id": "33333333-3333-3333-3333-333333333333"}

        async def fetchval(self, q, *a):
            self.calls.append(q)
            if "COUNT" in q:
                return 5
            return 2

    conn = _FakeConn()

    class _FakeAcquire:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *exc):
            return False

    db = PostgresDB.__new__(PostgresDB)
    db.acquire = lambda: _FakeAcquire()

    out = asyncio.run(
        db.apply_thread_rewind(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", from_seq=10, actor="user-1"
        )
    )
    assert out["swept"] == 5
    assert out["rewind_id"] == "33333333-3333-3333-3333-333333333333"
    assert out["surviving_turn"] == 2
    assert conn.memory_cursor == 2
    blob = " ".join(conn.calls)
    assert "pg_advisory_xact_lock" in blob
    assert "SET rewound_at = now()" in blob
    assert "INSERT INTO thread_rewinds" in blob
    assert "UPDATE thread_session_runtime_state" in blob
    assert "memory_extraction_turn = LEAST(" in blob
    assert "events_epoch = events_epoch + 1" in blob
    # The port's fix: the bump resets the seq high-water mark atomically.
    assert "events_seq_hwm = 0" in blob
    assert "INSERT INTO thread_events" in blob


@pytest.mark.asyncio
async def test_rewind_endpoint_rejects_live_agent(monkeypatch):
    async def _fake_owner(request, db, thread_id):
        return ({"id": "user-1"}, {"id": thread_id, "agent_id": "agent-9"})

    monkeypatch.setattr(access_module, "require_thread_owner", _fake_owner)
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await control_seams.rewind_thread_detached(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            MagicMock(),
            thread_lifecycle_module.ThreadRewindRequest(
                message_id="m1", mode="conversation"
            ),
        )
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_detached_rewind_rejects_stateless_thread_without_agent_id(monkeypatch):
    """agent_id=NULL is not proof that a stateless queue owner is idle."""

    from fastapi import HTTPException
    from orchestrator import main as orch_main

    fake_db = MagicMock()

    async def _fake_owner(request, db, thread_id):
        return (
            {"id": "user-1"},
            {
                "id": thread_id,
                "agent_id": None,
                "execution_lane": "stateless",
                "status": "active",
            },
        )

    monkeypatch.setattr(access_module, "require_thread_owner", _fake_owner)
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", fake_db)

    with pytest.raises(HTTPException) as exc:
        await control_seams.rewind_thread_detached(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            MagicMock(),
            thread_lifecycle_module.ThreadRewindRequest(
                message_id="m1", mode="conversation"
            ),
        )

    assert exc.value.status_code == 409
    fake_db.get_live_thread_message.assert_not_called()


@pytest.mark.asyncio
async def test_rewind_endpoint_allows_ended_thread_with_stale_agent_id(monkeypatch):
    """mark_orphaned_threads_ended / agent_update_thread_status's ended branch

    both leave ``agent_id`` populated on real ended threads — only a LIVE
    binding (status not suspended/ended) justifies the 409.
    """
    from orchestrator import main as orch_main

    async def _fake_owner(request, db, thread_id):
        return (
            {"id": "user-1"},
            {"id": thread_id, "agent_id": "agent-9", "status": "ended"},
        )

    monkeypatch.setattr(access_module, "require_thread_owner", _fake_owner)
    fake_db = MagicMock()
    fake_db.get_live_thread_message = AsyncMock(
        return_value={"seq": 8, "role": "human", "content": "the prompt"}
    )
    fake_db.apply_thread_rewind = AsyncMock(
        return_value={"rewind_id": "r1", "swept": 3, "surviving_turn": 1}
    )
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", fake_db)

    out = await control_seams.rewind_thread_detached(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        MagicMock(),
        thread_lifecycle_module.ThreadRewindRequest(
            message_id="m1", mode="conversation"
        ),
    )
    assert out == {"rewind_id": "r1", "swept": 3, "prompt": "the prompt"}
    fake_db.apply_thread_rewind.assert_awaited_once_with(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", from_seq=8, actor="user-1"
    )


@pytest.mark.asyncio
async def test_rewind_endpoint_rejects_code_mode(monkeypatch):
    async def _fake_owner(request, db, thread_id):
        return ({"id": "user-1"}, {"id": thread_id, "agent_id": None})

    monkeypatch.setattr(access_module, "require_thread_owner", _fake_owner)
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await control_seams.rewind_thread_detached(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            MagicMock(),
            thread_lifecycle_module.ThreadRewindRequest(message_id="m1", mode="both"),
        )
    assert exc.value.status_code == 400
    assert "resume" in str(exc.value.detail).lower()


@pytest.mark.asyncio
async def test_rewind_endpoint_happy_path(monkeypatch):
    from orchestrator import main as orch_main

    async def _fake_owner(request, db, thread_id):
        return ({"id": "user-1"}, {"id": thread_id, "agent_id": None})

    monkeypatch.setattr(access_module, "require_thread_owner", _fake_owner)
    fake_db = MagicMock()
    fake_db.get_live_thread_message = AsyncMock(
        return_value={"seq": 8, "role": "human", "content": "the prompt"}
    )
    fake_db.apply_thread_rewind = AsyncMock(
        return_value={"rewind_id": "r1", "swept": 3, "surviving_turn": 1}
    )
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", fake_db)

    out = await control_seams.rewind_thread_detached(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        MagicMock(),
        thread_lifecycle_module.ThreadRewindRequest(
            message_id="m1", mode="conversation"
        ),
    )
    assert out == {"rewind_id": "r1", "swept": 3, "prompt": "the prompt"}
    fake_db.apply_thread_rewind.assert_awaited_once_with(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", from_seq=8, actor="user-1"
    )
