"""Unit tests for checkpoint retention (D3).

``delete_checkpoint_thread`` prunes the 3 LangGraph tables for a terminal job,
gated on ``CHECKPOINTER_BACKEND=postgres``; ``update_job_status`` triggers it on
terminal status only. Mocked connection (no real DB), mirroring
tests/test_admin_providers_db.py. See
knowledge-history/done/cross_pod_resume_cold_starts_checkpoint_not_replicated.md.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.database.postgres import PostgresDB

_JOB_ID = "11111111-1111-1111-1111-111111111111"


def _make_db(mock_conn):
    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def _acquire():
        yield mock_conn

    db.acquire = _acquire
    return db


class TestDeleteCheckpointThread:
    @pytest.mark.asyncio
    async def test_noop_when_backend_not_postgres(self, monkeypatch):
        monkeypatch.delenv("CHECKPOINTER_BACKEND", raising=False)  # default sqlite
        conn = AsyncMock()
        db = _make_db(conn)

        assert await db.delete_checkpoint_thread("job-1") == 0
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deletes_three_tables_when_postgres(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 2")
        conn.fetchval = AsyncMock(return_value=False)
        db = _make_db(conn)

        deleted = await db.delete_checkpoint_thread("job-1")

        assert deleted == 6  # 3 tables × 2 rows
        assert conn.execute.await_count == 3
        tables = {
            call.args[0].split("FROM ")[1].split()[0]
            for call in conn.execute.await_args_list
        }
        assert tables == {"checkpoint_writes", "checkpoint_blobs", "checkpoints"}
        for call in conn.execute.await_args_list:
            assert call.args[1] == "job-1"  # thread_id bound as $1

    @pytest.mark.asyncio
    async def test_missing_table_is_nonfatal(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(side_effect=Exception("relation does not exist"))
        conn.fetchval = AsyncMock(return_value=False)
        db = _make_db(conn)

        assert await db.delete_checkpoint_thread("job-1") == 0  # swallowed

    @pytest.mark.asyncio
    async def test_unresolved_workspace_recovery_preserves_checkpoint_thread(
        self, monkeypatch
    ):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=True)
        db = _make_db(conn)

        assert await db.delete_checkpoint_thread(_JOB_ID) == 0

        conn.execute.assert_not_awaited()


class TestUpdateJobStatusPrunes:
    @pytest.mark.asyncio
    async def test_terminal_status_triggers_prune(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        # first execute = the UPDATE, next three = the checkpoint DELETEs
        conn.execute = AsyncMock(
            side_effect=["UPDATE 1", "DELETE 0", "DELETE 0", "DELETE 0"]
        )
        db = _make_db(conn)

        ok = await db.update_job_status(_JOB_ID, status="completed")

        assert ok is True
        assert conn.execute.await_count == 4  # UPDATE + 3 DELETEs
        assert conn.execute.await_args_list[0].args[0].startswith("UPDATE jobs")

    @pytest.mark.asyncio
    async def test_non_terminal_status_does_not_prune(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _make_db(conn)

        ok = await db.update_job_status(_JOB_ID, status="paused")

        assert ok is True
        assert conn.execute.await_count == 1  # only the UPDATE — no prune

    @pytest.mark.asyncio
    async def test_resumable_stateless_failure_keeps_checkpoint(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        conn.fetchval = AsyncMock(return_value="stateless")
        db = _make_db(conn)
        db.delete_checkpoint_thread = AsyncMock()

        assert await db.update_job_status(_JOB_ID, status="failed")

        db.delete_checkpoint_thread.assert_not_awaited()


class TestPruneCheckpointsKeepLast:
    """In-flight keep-last-N retention across ALL threads (bounds long-running
    jobs that never terminate, so the checkpointer can't fill the PVC)."""

    @pytest.mark.asyncio
    async def test_noop_when_backend_not_postgres(self, monkeypatch):
        monkeypatch.delenv("CHECKPOINTER_BACKEND", raising=False)  # default sqlite
        conn = AsyncMock()
        db = _make_db(conn)

        assert await db.prune_checkpoints_keep_last(3) == 0
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prunes_three_tables_keeping_last_n(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 5")
        db = _make_db(conn)

        deleted = await db.prune_checkpoints_keep_last(3)

        assert deleted == 15  # 3 tables × 5 rows
        assert conn.execute.await_count == 3
        sql = [c.args[0] for c in conn.execute.await_args_list]
        # checkpoints must be pruned BEFORE checkpoint_writes — the writes
        # cleanup deletes rows orphaned by the checkpoints delete.
        assert "DELETE FROM checkpoints c" in sql[0]
        assert "DELETE FROM checkpoint_writes" in sql[1]
        assert "DELETE FROM checkpoint_blobs cb" in sql[2]
        # the windowed deletes keep the newest N (bound as $1)
        assert conn.execute.await_args_list[0].args[1] == 3
        assert conn.execute.await_args_list[2].args[1] == 3
        assert all("vm_workspace_recovery_jobs" in statement for statement in sql)

    @pytest.mark.asyncio
    async def test_keep_zero_is_rejected(self, monkeypatch):
        """keep_n < 1 would wipe live resume state — refuse, don't delete."""
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        db = _make_db(conn)

        assert await db.prune_checkpoints_keep_last(0) == 0
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nonfatal_on_error(self, monkeypatch):
        monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
        conn = AsyncMock()
        conn.execute = AsyncMock(side_effect=Exception("relation does not exist"))
        db = _make_db(conn)

        assert await db.prune_checkpoints_keep_last(3) == 0  # swallowed


class TestRetentionTick:
    """The periodic sweeper's per-tick logic: prune only on the leader (so two
    HA replicas don't run the global prune concurrently), with keep_n from env."""

    @pytest.mark.asyncio
    async def test_prunes_when_leader(self):
        from orchestrator.services.checkpoint_retention import retention_tick

        db = MagicMock()
        db.prune_checkpoints_keep_last = AsyncMock(return_value=7)

        deleted = await retention_tick(db, leader=True)

        assert deleted == 7
        db.prune_checkpoints_keep_last.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_when_not_leader(self):
        from orchestrator.services.checkpoint_retention import retention_tick

        db = MagicMock()
        db.prune_checkpoints_keep_last = AsyncMock(return_value=7)

        deleted = await retention_tick(db, leader=False)

        assert deleted == 0
        db.prune_checkpoints_keep_last.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_keep_n_from_env_default_3(self, monkeypatch):
        from orchestrator.services.checkpoint_retention import retention_tick

        monkeypatch.delenv("CHECKPOINT_RETENTION_KEEP", raising=False)
        db = MagicMock()
        db.prune_checkpoints_keep_last = AsyncMock(return_value=0)

        await retention_tick(db, leader=True)

        db.prune_checkpoints_keep_last.assert_awaited_once_with(3)

    @pytest.mark.asyncio
    async def test_keep_n_from_env_override(self, monkeypatch):
        from orchestrator.services.checkpoint_retention import retention_tick

        monkeypatch.setenv("CHECKPOINT_RETENTION_KEEP", "5")
        db = MagicMock()
        db.prune_checkpoints_keep_last = AsyncMock(return_value=0)

        await retention_tick(db, leader=True)

        db.prune_checkpoints_keep_last.assert_awaited_once_with(5)
