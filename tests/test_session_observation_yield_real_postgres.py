"""Cross-replica readiness scheduling signals grant no lifecycle authority."""

import asyncio
from uuid import uuid4

import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock

from orchestrator.database.postgres import PostgresDB
from tests import test_workspace_pull_failure_real_postgres as pull

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied


@pytest.mark.asyncio
async def test_signals_are_shared_generation_scoped_and_cross_replica(db, pg_dsn):
    other = PostgresDB(pg_dsn, min_connections=1, max_connections=1)
    await other.connect()
    thread_id, generation = str(uuid4()), str(uuid4())
    try:
        assert not await other.session_workspace_observation_yield_requested(
            thread_id, runtime_generation=generation
        )
        async with db.session_workspace_observation_yield_request(
            thread_id, runtime_generation=generation
        ):
            async with other.session_workspace_observation_yield_request(
                "{" + thread_id.upper() + "}", runtime_generation=generation.upper()
            ):
                assert await db.session_workspace_observation_yield_requested(
                    thread_id, runtime_generation=generation
                )
            assert await other.session_workspace_observation_yield_requested(
                thread_id, runtime_generation=generation
            )
            assert not await other.session_workspace_observation_yield_requested(
                thread_id, runtime_generation=str(uuid4())
            )
            assert not await other.session_workspace_observation_yield_requested(
                str(uuid4()), runtime_generation=generation
            )
        assert not await other.session_workspace_observation_yield_requested(
            thread_id, runtime_generation=generation
        )
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_signal_query_does_not_hold_a_lock_in_callers_transaction(db):
    tid, generation = str(uuid4()), str(uuid4())
    async with db.transaction_scope():
        assert not await db.session_workspace_observation_yield_requested(
            tid, runtime_generation=generation
        )
        async with db.session_workspace_observation_yield_request(
            tid, runtime_generation=generation, wait_timeout_s=0.5
        ):
            assert await db.session_workspace_observation_yield_requested(
                tid, runtime_generation=generation
            )


@pytest.mark.asyncio
async def test_signal_cancellation_releases_owned_connection_and_slot(db):
    tid, generation = str(uuid4()), str(uuid4())
    entered = asyncio.Event()

    async def request():
        async with db.session_workspace_observation_yield_request(
            tid, runtime_generation=generation
        ):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(request())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not await db.session_workspace_observation_yield_requested(
        tid, runtime_generation=generation
    )
    async with db.session_workspace_observation_yield_request(
        tid, runtime_generation=generation, wait_timeout_s=0.5
    ):
        assert await db.session_workspace_observation_yield_requested(
            tid, runtime_generation=generation
        )


@pytest.mark.asyncio
async def test_signal_budget_wait_is_bounded_and_separate_from_lifecycle(db):
    db._dedicated_advisory_lock_slot_groups["workspace_observation_yield"] = (
        asyncio.Semaphore(1)
    )
    db._dedicated_advisory_lock_slot_groups["lifecycle"] = asyncio.Semaphore(1)
    tid, generation, other = str(uuid4()), str(uuid4()), str(uuid4())
    async with db.thread_advisory_lock(tid):
        async with db.session_workspace_observation_yield_request(
            tid, runtime_generation=generation
        ):
            with pytest.raises(TimeoutError):
                async with db.session_workspace_observation_yield_request(
                    other, runtime_generation=generation, wait_timeout_s=0.05
                ):
                    pytest.fail("exhausted signal budget admitted another connection")
            assert not await db.session_workspace_observation_yield_requested(
                other, runtime_generation=generation
            )


@pytest.mark.asyncio
async def test_signal_connection_loss_cannot_leave_a_durable_request(db):
    from orchestrator.database.postgres import _session_workspace_yield_lock_key

    tid, generation = str(uuid4()), str(uuid4())
    key = _session_workspace_yield_lock_key(tid, generation)
    async with db.session_workspace_observation_yield_request(
        tid, runtime_generation=generation
    ):
        pid = await db.fetchval(
            "SELECT pid FROM pg_locks WHERE locktype='advisory' "
            "AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) "
            "AND classid::bigint=$1 AND objid::bigint=$2 AND objsubid=1 "
            "AND mode='ShareLock' AND granted",
            (key >> 32) & 0xFFFFFFFF,
            key & 0xFFFFFFFF,
        )
        assert pid is not None
        assert await db.fetchval("SELECT pg_terminate_backend($1)", pid)
        for _ in range(50):
            if not await db.session_workspace_observation_yield_requested(
                tid, runtime_generation=generation
            ):
                break
            await asyncio.sleep(0.01)
        assert not await db.session_workspace_observation_yield_requested(
            tid, runtime_generation=generation
        )


@pytest.mark.asyncio
async def test_signal_is_scoped_to_its_actual_database(db, pg_dsn):
    tid, generation = str(uuid4()), str(uuid4())
    other = await asyncpg.connect(pg_dsn, database="template1")
    try:
        async with db.session_workspace_observation_yield_request(
            tid, runtime_generation=generation
        ):
            assert await db.session_workspace_observation_yield_requested(
                tid, runtime_generation=generation
            )
            async with db.using_connection(other):
                assert not await db.session_workspace_observation_yield_requested(
                    tid, runtime_generation=generation
                )
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_repeated_cancellation_joins_signal_connection_cleanup(monkeypatch):
    from orchestrator.database import postgres as postgres_module

    store = PostgresDB.__new__(PostgresDB)
    store._connection_string = "postgresql://owned/signal-test"
    conn = MagicMock()
    conn.is_closed.return_value = False
    conn.fetchval = AsyncMock(return_value=True)
    closing, finish_close, entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def close(*, timeout):
        assert timeout == 5
        closing.set()
        await finish_close.wait()

    conn.close = AsyncMock(side_effect=close)
    monkeypatch.setattr(
        postgres_module.asyncpg, "connect", AsyncMock(return_value=conn)
    )

    async def owner():
        async with store.session_workspace_observation_yield_request(
            str(uuid4()), runtime_generation=str(uuid4())
        ):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(owner())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.wait_for(closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert (
            store._dedicated_advisory_slots("workspace_observation_yield")._value == 3
        )
        finish_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        conn.close.assert_awaited_once_with(timeout=5)
        assert (
            store._dedicated_advisory_slots("workspace_observation_yield")._value == 4
        )
    finally:
        finish_close.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_signal_connection_acquisition_deadline_returns_its_slot(monkeypatch):
    from orchestrator.database import postgres as postgres_module

    store = PostgresDB.__new__(PostgresDB)
    store._connection_string = "postgresql://owned/signal-test"
    connect_cancelled = asyncio.Event()

    async def unavailable(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            connect_cancelled.set()

    monkeypatch.setattr(postgres_module.asyncpg, "connect", unavailable)
    with pytest.raises(TimeoutError):
        async with store.session_workspace_observation_yield_request(
            str(uuid4()), runtime_generation=str(uuid4()), wait_timeout_s=0.05
        ):
            pytest.fail("connection acquisition deadline was bypassed")
    assert connect_cancelled.is_set()
    assert store._dedicated_advisory_slots("workspace_observation_yield")._value == 4
