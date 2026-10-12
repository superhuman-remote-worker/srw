"""The terminal checkpoint prune must survive bloat and must not fail silently.

Regression guard for Defect 6 of
knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md.

On 2026-07-27 the prune fired as one unbounded statement and was cancelled::

    ERROR: canceling statement due to user request
    STATEMENT: DELETE FROM checkpoint_blobs WHERE thread_id = $1

The failure was swallowed at DEBUG, so the one mechanism that reclaims
checkpoint space stopped working invisibly — while the volume it protects was
filling. Two properties matter: the delete is insensitive to row count, and a
real failure is loud.
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.database.postgres import (
    _CHECKPOINT_DELETE_BATCH,
    PostgresDB,
)

_TABLES = ("checkpoint_writes", "checkpoint_blobs", "checkpoints")


class UndefinedTableError(Exception):
    """Stands in for asyncpg's UndefinedTableError.

    The production code matches on ``type(e).__name__`` so it does not have to
    import asyncpg's exception tree, so this stub must carry the real name.
    """


def _db(monkeypatch, conn):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    db = PostgresDB.__new__(PostgresDB)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=ctx)
    return db


def _conn_with_rows(per_table: int):
    """Connection that reports `per_table` rows, draining a batch per call."""
    remaining = {t: per_table for t in _TABLES}

    async def execute(sql, *args):
        table = next(t for t in _TABLES if f"FROM {t} " in sql or f"FROM {t}\n" in sql)
        take = min(_CHECKPOINT_DELETE_BATCH, remaining[table])
        remaining[table] -= take
        return f"DELETE {take}"

    conn = MagicMock()
    conn.execute = AsyncMock(side_effect=execute)
    return conn


class TestBatching:
    @pytest.mark.asyncio
    async def test_deletes_every_row_when_count_exceeds_one_batch(self, monkeypatch):
        """Row count must not determine whether the prune completes."""
        rows = _CHECKPOINT_DELETE_BATCH * 2 + 7
        conn = _conn_with_rows(rows)
        db = _db(monkeypatch, conn)

        total = await db.delete_checkpoint_thread("thread-1")

        assert total == rows * len(_TABLES)

    @pytest.mark.asyncio
    async def test_every_statement_is_bounded_by_a_limit(self, monkeypatch):
        """The unbounded form is the one that got cancelled — it must be gone."""
        conn = _conn_with_rows(_CHECKPOINT_DELETE_BATCH * 2)
        db = _db(monkeypatch, conn)

        await db.delete_checkpoint_thread("thread-1")

        assert conn.execute.await_count > len(_TABLES), "should have taken batches"
        for call in conn.execute.await_args_list:
            assert "LIMIT" in call.args[0]

    @pytest.mark.asyncio
    async def test_stops_after_one_batch_when_thread_is_small(self, monkeypatch):
        """The common case stays a single statement per table."""
        conn = _conn_with_rows(3)
        db = _db(monkeypatch, conn)

        total = await db.delete_checkpoint_thread("thread-1")

        assert total == 9
        assert conn.execute.await_count == len(_TABLES)


class TestFailureIsLoud:
    @pytest.mark.asyncio
    async def test_real_failure_logs_at_warning(self, monkeypatch, caplog):
        """A cancelled/timed-out prune is an incident signal, not DEBUG noise."""
        conn = MagicMock()
        conn.execute = AsyncMock(
            side_effect=Exception("canceling statement due to user request")
        )
        db = _db(monkeypatch, conn)

        with caplog.at_level(logging.WARNING):
            total = await db.delete_checkpoint_thread("thread-1")

        assert total == 0
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "a silently-skipped prune is how this hid for four days"
        assert "thread-1" in warnings[0].getMessage()

    @pytest.mark.asyncio
    async def test_missing_table_stays_quiet(self, monkeypatch, caplog):
        """sqlite-backed / pre-migration deploys must not spam warnings."""
        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=UndefinedTableError("no such table"))
        db = _db(monkeypatch, conn)

        with caplog.at_level(logging.WARNING):
            await db.delete_checkpoint_thread("thread-1")

        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    @pytest.mark.asyncio
    async def test_strict_prune_propagates_real_failure(self, monkeypatch):
        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=RuntimeError("delete timed out"))
        db = _db(monkeypatch, conn)

        with pytest.raises(RuntimeError, match="delete timed out"):
            await db.delete_checkpoint_thread("thread-1", strict=True)

    @pytest.mark.asyncio
    async def test_strict_prune_rejects_surviving_rows(self, monkeypatch):
        conn = _conn_with_rows(0)
        conn.fetchval = AsyncMock(return_value=True)
        db = _db(monkeypatch, conn)

        with pytest.raises(RuntimeError, match="left rows behind"):
            await db.delete_checkpoint_thread("thread-1", strict=True)

    @pytest.mark.asyncio
    async def test_no_op_when_backend_is_not_postgres(self, monkeypatch):
        conn = MagicMock()
        conn.execute = AsyncMock()
        db = _db(monkeypatch, conn)
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "sqlite")

        assert await db.delete_checkpoint_thread("thread-1") == 0
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_strict_prune_checks_tables_despite_backend_rollback(
        self, monkeypatch
    ):
        conn = _conn_with_rows(0)
        conn.fetchval = AsyncMock(return_value=False)
        db = _db(monkeypatch, conn)
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "sqlite")

        assert await db.delete_checkpoint_thread("thread-1", strict=True) == 0
        assert conn.execute.await_count == len(_TABLES)
        # Strict delete checks each table within the prune transaction, then
        # again after the authority settles to catch a late checkpoint write.
        checked = [call.args for call in conn.fetchval.await_args_list]
        assert checked == [
            (f"SELECT EXISTS (SELECT 1 FROM {table} WHERE thread_id = $1)", "thread-1")
            for _phase in range(2)
            for table in _TABLES
        ]
