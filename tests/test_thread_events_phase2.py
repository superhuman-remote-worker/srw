"""Tests for Phase 2 of headless persistent sessions: event log, interrupt
semantics, per-turn lock. DB-integration tests (migration apply, full SSE
replay, prune sweep) live separately and are gated on a running Postgres.

Covers:
  - _broadcast stamps (epoch, seq) cursors and queues the ordered writer.
  - _loop_check_interrupt tri-state mode (also exercised in test_persistent_app).
  - WS interrupt handler picks hard vs graceful based on _tool_inflight.
  - Interrupt mid-stream in persistent_graph: "hard" drops partial AIMessage,
    other modes (graceful, legacy bool) preserve it.
  - Orchestrator per-turn lock: same (thread, turn) shares one Lock,
    different (thread, turn) get distinct Locks, cleanup after timeout.
  - Agent REST endpoints (/api/input, /api/interrupt, /api/approve) routing
    and 503-when-detached behavior.
"""

import asyncio
import inspect
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def test_pruner_preserves_receipts_until_owner_request_is_terminal():
    import orchestrator.main as om

    source = inspect.getsource(om.thread_events_prune_sweeper)
    assert source.count("request.id = thread_events.control_request_id") == 2
    assert source.count("request.id = thread_events.interrupt_request_id") == 2
    assert source.count("request.outcome IS NULL") == 4
    assert source.count("request.outcome = 'applied'") == 2
    assert source.count("'consumed_input_seq'") == 2


def test_applied_session_memory_migration_checksum_is_immutable():
    """0145 was live-applied before later milestones; its bytes are an API.

    The authority for "its bytes" is ``schema_migrations.checksum`` — what the
    runner recorded when it applied the file — NOT whatever the file happens
    to say today. Those diverged: cf7891e8 deleted a COMMENT ON TABLE block
    from 0145 after it had been applied, and this guard was then written
    against the post-edit bytes. The result pinned a snapshot that no database
    ever applied, so every orchestrator boot failed on

        checksum changed: 0145_session_turn_memory_effects.sql

    for 41 hours while this test stayed green. A guard asserting the wrong
    snapshot is worse than no guard: it makes the broken state the enforced
    one.

    The hash below is the applied one, verified against dev's
    schema_migrations row (650ad97e). If this test ever fails again, read the
    checksum out of the database before touching either side — the file is the
    thing that may be wrong.
    """

    import hashlib
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0145_session_turn_memory_effects.sql"
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        "2cfd047791f6530f7640571cdc4108e16e1012447899628faa341deca38e80f9"
    )


# ---------------------------------------------------------------------------
# Section 1 — _broadcast cursor + ordered journal writer
# ---------------------------------------------------------------------------


class TestEventJournalEpochAllocation:
    @staticmethod
    def _pool(conn):
        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, exc_type, exc, tb):
                return None

        pool = MagicMock()
        pool.acquire = lambda: _Acquire()
        return pool

    @pytest.mark.asyncio
    async def test_reuses_live_epoch_and_seeds_seq_on_clean_reattach(self):
        """New contract (doc §5.3.2): a live epoch with surviving rows is
        REUSED on reattach — seq seeds at GREATEST(hwm, MAX(seq)) so cached
        client cursors stay valid and no gone_beyond_horizon cascade fires.
        The full bump/reuse matrix lives in test_event_journal_epoch.py; this
        asserts the attach-path default is reuse."""
        import agent.api.persistent_app as mod

        conn = MagicMock()
        conn.fetchrow = AsyncMock(
            return_value={
                "events_epoch": 8,
                "events_seq_hwm": 41,
                "status": "active",
            }
        )
        # Probe order: terminal-frame EXISTS → False, MAX(seq) → 37 (< hwm).
        conn.fetchval = AsyncMock(side_effect=[False, 37])
        conn.execute = AsyncMock()

        epoch, seq_seed = await mod._resolve_event_journal_epoch(
            self._pool(conn), "thread-live"
        )

        assert (epoch, seq_seed) == (8, 41)
        # hwm already covers max_seq → no correction UPDATE.
        conn.execute.assert_not_awaited()
        sql, thread_id = conn.fetchrow.await_args.args
        assert "events_seq_hwm" in sql
        assert "events_epoch + 1" not in sql
        assert thread_id == "thread-live"

    @pytest.mark.asyncio
    async def test_missing_thread_fails_closed(self):
        import agent.api.persistent_app as mod

        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value=None)

        with pytest.raises(mod.EventJournalUnavailable, match="does not exist"):
            await mod._resolve_event_journal_epoch(self._pool(conn), "thread-missing")

    @pytest.mark.asyncio
    async def test_database_error_is_wrapped_as_journal_unavailable(self):
        import agent.api.persistent_app as mod

        conn = MagicMock()
        conn.fetchrow = AsyncMock(side_effect=RuntimeError("database offline"))

        with pytest.raises(mod.EventJournalUnavailable, match="initialization failed"):
            await mod._resolve_event_journal_epoch(self._pool(conn), "thread-db")


class TestBroadcastCursor:
    """Phase 2 changes: _broadcast pre-increments seq, stamps (epoch, seq)
    into the frame's params, and queues one ordered DB writer."""

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._subscribers.clear()
        mod._events_epoch = 0
        mod._next_seq = 0
        mod._event_writer = None
        mod._session = None  # disables the DB write scheduling

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._subscribers.clear()
        mod._events_epoch = 0
        mod._next_seq = 0
        mod._event_writer = None
        mod._session = None

    def test_broadcast_increments_seq_monotonically(self):
        import agent.api.persistent_app as mod

        q = mod._subscribe("c1")
        mod._broadcast("token", {"content": "a"})
        mod._broadcast("token", {"content": "b"})
        mod._broadcast("token", {"content": "c"})

        f1 = q.get_nowait()
        f2 = q.get_nowait()
        f3 = q.get_nowait()
        assert f1["params"]["_seq"] == [0, 1]
        assert f2["params"]["_seq"] == [0, 2]
        assert f3["params"]["_seq"] == [0, 3]
        assert mod._next_seq == 3

    def test_broadcast_uses_current_epoch(self):
        import agent.api.persistent_app as mod

        mod._events_epoch = 7
        mod._next_seq = 0
        q = mod._subscribe("c1")
        mod._broadcast("token", {"content": "x"})

        frame = q.get_nowait()
        assert frame["params"]["_seq"] == [7, 1]

    def test_broadcast_keeps_original_params(self):
        import agent.api.persistent_app as mod

        q = mod._subscribe("c1")
        mod._broadcast("tool.started", {"tool": "read_file", "id": "tc1"})

        frame = q.get_nowait()
        # _seq added, original keys preserved.
        assert frame["params"]["tool"] == "read_file"
        assert frame["params"]["id"] == "tc1"
        assert "_seq" in frame["params"]

    @pytest.mark.asyncio
    async def test_broadcast_queues_ordered_writer_when_session_present(
        self,
    ):
        """A DB-backed session routes broadcast persistence through its writer."""
        import agent.api.persistent_app as mod

        # Fake session with a no-op acquire that records the fenced flush
        # (fetchval returning the inserted count — the epoch-guard contract).
        recorded = []

        class _FakeConn:
            async def fetchval(self, *args, **kwargs):
                recorded.append(args)
                return len(json.loads(args[2]))

        class _Acquire:
            async def __aenter__(self):
                return _FakeConn()

            async def __aexit__(self, exc_type, exc, tb):
                return None

        fake_conn = MagicMock()
        fake_conn.acquire = lambda: _Acquire()
        fake_session = MagicMock()
        fake_session.postgres_conn = fake_conn

        mod._session = fake_session
        mod._thread_id = "thread-xyz"
        mod._subscribe("c1")
        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=fake_conn,
            thread_id="thread-xyz",
            epoch=0,
            on_terminal_failure=mod._event_persistence_failed,
        )
        writer.start()
        mod._event_writer = writer

        mod._broadcast("token", {"content": "hi"})
        await writer.close()
        mod._event_writer = None

        assert recorded, "expected the ordered writer to call fetchval()"
        # Args: (SQL, thread_id, rows_json, writer_epoch).
        sql = recorded[0][0]
        assert "INSERT INTO thread_events" in sql
        assert "ON CONFLICT" not in sql
        assert "events_epoch = $3" in sql  # fenced on the writer's epoch
        rows = json.loads(recorded[0][2])
        assert [(row["seq"], row["kind"]) for row in rows] == [(1, "token")]
        assert recorded[0][3] == 0

    @pytest.mark.asyncio
    async def test_durable_broadcast_waits_for_commit_and_links_request(self):
        import agent.api.persistent_app as mod

        write_started = asyncio.Event()
        allow_commit = asyncio.Event()
        recorded = []

        class _FakeConn:
            def transaction(self):
                return self

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            async def fetchval(self, *args):
                recorded.append(args)
                if "INSERT INTO thread_events" in args[0]:
                    write_started.set()
                    await allow_commit.wait()
                    return len(json.loads(args[2]))
                return 1

        class _Acquire:
            async def __aenter__(self):
                return _FakeConn()

            async def __aexit__(self, exc_type, exc, tb):
                return None

        pool = MagicMock()
        pool.acquire = lambda: _Acquire()
        session = MagicMock()
        session.postgres_conn = pool
        request_id = "77777777-7777-4777-8777-777777777777"
        agent_id = "88888888-8888-4888-8888-888888888888"
        mod._session = session
        mod._thread_id = "99999999-9999-4999-8999-999999999999"
        mod._events_epoch = 3
        mod._next_seq = 10
        live = mod._subscribe("durable-control")
        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=pool,
            thread_id=mod._thread_id,
            epoch=3,
            on_terminal_failure=lambda _events, _reason: None,
            pinned_agent_id=agent_id,
            pinned_runtime_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            pinned_runtime_attach_token="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        )
        writer.start()
        mod._event_writer = writer

        durable = asyncio.create_task(
            mod._broadcast_durable(
                "mode.changed",
                {
                    "mode": "autonomous",
                    "request_id": request_id,
                    "request_seq": 1,
                    "method": "mode.set",
                },
                control_request_id=request_id,
                lease_token=None,
                agent_id=agent_id,
            )
        )
        await asyncio.wait_for(write_started.wait(), timeout=1)
        assert not durable.done()
        assert live.empty()
        allow_commit.set()
        assert await durable == (3, 11)
        await asyncio.sleep(0)
        assert live.get_nowait()["params"]["_seq"] == [3, 11]
        await writer.close()
        mod._event_writer = None

        assert "execution_lane = 'pinned'" in recorded[0][0]
        assert "FROM agents" in recorded[1][0]
        assert "thread_control_requests" in recorded[2][0]
        insert = next(
            call for call in recorded if "INSERT INTO thread_events" in call[0]
        )
        rows = json.loads(insert[2])
        assert rows[0]["control_request_id"] == request_id
        assert recorded[0][2] == agent_id


class TestOrderedPersistentEventWriter:
    @staticmethod
    def _pool(conn):
        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, exc_type, exc, tb):
                return None

        pool = MagicMock()
        pool.acquire = lambda: _Acquire()
        return pool

    @pytest.mark.asyncio
    async def test_never_starts_later_write_while_earlier_batch_is_blocked(self):
        """The regression: seq 2 cannot race past a slow seq 1 insert."""
        import agent.api.persistent_app as mod

        first_started = asyncio.Event()
        release_first = asyncio.Event()
        started: list[list[int]] = []
        completed: list[list[int]] = []
        active = 0
        max_active = 0

        class _Conn:
            async def fetchval(self, _sql, _thread_id, rows_json, _epoch):
                nonlocal active, max_active
                rows = json.loads(rows_json)
                seqs = [row["seq"] for row in rows]
                started.append(seqs)
                active += 1
                max_active = max(max_active, active)
                if seqs == [1]:
                    first_started.set()
                    await release_first.wait()
                completed.append(seqs)
                active -= 1
                return len(rows)

        failures = []
        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id="thread-ordered",
            epoch=0,
            on_terminal_failure=lambda events, reason: failures.append(
                (events, reason)
            ),
            batch_size=1,
        )
        writer.start()
        assert writer.enqueue(
            mod._QueuedPersistentEvent(0, 1, "token", {"content": "one"})
        )
        await asyncio.wait_for(first_started.wait(), timeout=1)

        assert writer.enqueue(
            mod._QueuedPersistentEvent(0, 2, "token", {"content": "two"})
        )
        assert writer.enqueue(
            mod._QueuedPersistentEvent(0, 3, "token", {"content": "three"})
        )
        await asyncio.sleep(0)
        assert started == [[1]]

        release_first.set()
        await writer.close()

        assert started == [[1], [2], [3]]
        assert completed == [[1], [2], [3]]
        assert max_active == 1
        assert failures == []

    @pytest.mark.asyncio
    async def test_persists_a_fifo_burst_in_one_batch(self):
        import agent.api.persistent_app as mod

        calls = []

        class _Conn:
            async def fetchval(self, _sql, _thread_id, rows_json, _epoch):
                rows = json.loads(rows_json)
                calls.append(rows)
                return len(rows)

        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id="thread-batch",
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            batch_size=10,
        )
        writer.start()
        for seq in range(1, 4):
            assert writer.enqueue(
                mod._QueuedPersistentEvent(4, seq, "token", {"content": str(seq)})
            )
        await writer.close()

        assert len(calls) == 1
        assert [row["seq"] for row in calls[0]] == [1, 2, 3]
        assert all(row["epoch"] == 4 for row in calls[0])

    @pytest.mark.asyncio
    async def test_stateless_flush_locks_epoch_thread_then_bound_queue(self):
        import agent.api.persistent_app as mod
        from agent.api.lease_context import LeaseHandle

        thread_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        calls = []

        class _Txn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, _exc_type, _exc, _tb):
                return False

        class _Conn:
            def transaction(self):
                return _Txn()

            async def fetchval(self, sql, *args):
                calls.append((sql, args))
                if "INSERT INTO thread_events" in sql:
                    return 1
                return 1

        lease = LeaseHandle()
        lease.update(thread_id, 17)
        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id=thread_id,
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            lease=lease,
        )

        inserted = await writer._write_batch(
            [mod._QueuedPersistentEvent(4, 1, "token", {"content": "one"})]
        )

        assert inserted == 1
        assert len(calls) == 3
        assert "FROM threads" in calls[0][0]
        assert "events_epoch" in calls[0][0]
        assert "FOR NO KEY UPDATE" in calls[0][0]
        assert calls[0][1] == (thread_id, 4)
        assert "FROM run_queue" in calls[1][0]
        assert "lease_token" in calls[1][0]
        assert calls[1][1] == (thread_id, 17)
        assert "INSERT INTO thread_events" in calls[2][0]
        assert calls[2][1][3:] == (thread_id, 17)

    @pytest.mark.asyncio
    async def test_stateless_flush_rejects_repointed_unit_before_sql(self):
        import agent.api.persistent_app as mod
        from agent.api.lease_context import LeaseHandle

        lease = LeaseHandle()
        lease.update("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", 18)
        pool = MagicMock()
        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=pool,
            thread_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            lease=lease,
        )

        inserted = await writer._write_batch(
            [mod._QueuedPersistentEvent(4, 1, "token", {"content": "one"})]
        )

        assert inserted == 0
        pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_pinned_flush_locks_exact_live_runtime_then_reciprocal_agent(self):
        import agent.api.persistent_app as mod

        thread_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        agent_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        generation = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        attach_token = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        calls = []

        class _Txn:
            async def __aenter__(self):
                calls.append(("begin", ()))
                return self

            async def __aexit__(self, _exc_type, _exc, _tb):
                calls.append(("commit", ()))
                return False

        class _Conn:
            def transaction(self):
                return _Txn()

            async def fetchval(self, sql, *args):
                calls.append((sql, args))
                return 1

        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id=thread_id,
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            pinned_agent_id=agent_id,
            pinned_runtime_generation=generation,
            pinned_runtime_attach_token=attach_token,
        )

        inserted = await writer._write_batch(
            [mod._QueuedPersistentEvent(4, 1, "token", {"content": "one"})]
        )

        assert inserted == 1
        assert calls[0][0] == "begin"
        assert "FROM threads" in calls[1][0]
        assert "runtime_retirement_token IS NULL" in calls[1][0]
        assert (
            "status IN ('created', 'active', 'awaiting_user', 'suspended')"
            in calls[1][0]
        )
        assert "events_epoch = $5" in calls[1][0]
        assert "FOR NO KEY UPDATE" in calls[1][0]
        assert calls[1][1] == (thread_id, agent_id, generation, attach_token, 4)
        assert "FROM agents" in calls[2][0]
        assert calls[2][1] == (agent_id, thread_id)
        assert "INSERT INTO thread_events" in calls[3][0]
        assert calls[4][0] == "commit"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failed_fence", ["thread", "agent"])
    async def test_pinned_flush_rejects_retired_or_nonreciprocal_runtime(
        self, failed_fence
    ):
        import agent.api.persistent_app as mod

        calls = []

        class _Txn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, _exc_type, _exc, _tb):
                return False

        class _Conn:
            def transaction(self):
                return _Txn()

            async def fetchval(self, sql, *args):
                calls.append((sql, args))
                if failed_fence == "thread" and "FROM threads" in sql:
                    return None
                if failed_fence == "agent" and "FROM agents" in sql:
                    return None
                if "INSERT INTO thread_events" in sql:
                    raise AssertionError("INSERT must not execute after a failed fence")
                return 1

        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            pinned_agent_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            pinned_runtime_generation="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            pinned_runtime_attach_token="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )

        assert (
            await writer._write_batch(
                [mod._QueuedPersistentEvent(4, 1, "token", {"content": "late"})]
            )
            == 0
        )
        assert not any("INSERT INTO thread_events" in sql for sql, _args in calls)

    @pytest.mark.asyncio
    async def test_pinned_flush_allows_exact_suspended_attach(self):
        """A resumable suspended row may journal before its active CAS.

        Attach starts the ordered writer before the final ``active`` status
        transition and can emit protected workspace diagnostics in that
        interval.  The retirement token and exact reciprocal runtime identity
        remain the authority fence; excluding ``suspended`` would terminally
        close the writer on its first valid frame.
        """
        import agent.api.persistent_app as mod

        seen_sql: list[str] = []

        class _Txn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, _exc_type, _exc, _tb):
                return False

        class _Conn:
            def transaction(self):
                return _Txn()

            async def fetchval(self, sql, *_args):
                seen_sql.append(sql)
                return 1

        writer = mod._OrderedPersistentEventWriter(
            postgres_conn=self._pool(_Conn()),
            thread_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            epoch=4,
            on_terminal_failure=lambda _events, _reason: None,
            pinned_agent_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            pinned_runtime_generation="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            pinned_runtime_attach_token="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )

        assert (
            await writer._write_batch(
                [mod._QueuedPersistentEvent(4, 1, "status", {"state": "creating"})]
            )
            == 1
        )
        assert (
            "status IN ('created', 'active', 'awaiting_user', 'suspended')"
            in seen_sql[0]
        )
        assert any("INSERT INTO thread_events" in sql for sql in seen_sql)

    @pytest.mark.asyncio
    async def test_canvas_failure_retries_then_sends_unjournaled_reconcile(self):
        import agent.api.persistent_app as mod

        attempts = 0

        class _Conn:
            async def fetchval(self, _sql, _thread_id, _rows_json, _epoch):
                nonlocal attempts
                attempts += 1
                raise RuntimeError("database unavailable")

        mod._subscribers.clear()
        with patch.object(mod, "_orchestrator_client", None):
            queue = mod._subscribe("canvas-client")
            writer = mod._OrderedPersistentEventWriter(
                postgres_conn=self._pool(_Conn()),
                thread_id="thread-canvas",
                epoch=2,
                on_terminal_failure=mod._event_persistence_failed,
                state_max_attempts=3,
                retry_base_s=0,
            )
            writer.start()
            assert writer.enqueue(
                mod._QueuedPersistentEvent(
                    2,
                    9,
                    "canvas.updated",
                    {"canvas_id": "main", "presentation_revision": 7},
                )
            )
            await writer.close()

        assert attempts == 3
        assert queue.get_nowait() == {
            "method": "canvas.reconcile_required",
            "params": {"canvas_id": "main", "reason": "write_failed"},
        }
        assert queue.empty()
        mod._subscribers.clear()

    @pytest.mark.asyncio
    async def test_full_queue_does_not_silently_drop_canvas_invalidation(self):
        import agent.api.persistent_app as mod

        class _Conn:
            async def fetchval(self, _sql, _thread_id, rows_json, _epoch):
                return len(json.loads(rows_json))

        mod._subscribers.clear()
        with patch.object(mod, "_orchestrator_client", None):
            queue = mod._subscribe("canvas-client")
            writer = mod._OrderedPersistentEventWriter(
                postgres_conn=self._pool(_Conn()),
                thread_id="thread-overflow",
                epoch=1,
                on_terminal_failure=mod._event_persistence_failed,
                queue_maxsize=1,
            )
            writer.start()
            assert writer.enqueue(
                mod._QueuedPersistentEvent(1, 1, "token", {"content": "full"})
            )
            assert not writer.enqueue(
                mod._QueuedPersistentEvent(
                    1, 2, "canvas.updated", {"canvas_id": "main"}
                )
            )
            await writer.close()

        assert queue.get_nowait() == {
            "method": "canvas.reconcile_required",
            "params": {"canvas_id": "main", "reason": "queue_overflow"},
        }
        mod._subscribers.clear()


# ---------------------------------------------------------------------------
# Section 2 — WS interrupt handler picks hard vs graceful based on tool state
# ---------------------------------------------------------------------------


class TestInterruptModeSelection:
    """The /api/interrupt REST handler (and the WS interrupt branch) picks
    mode = 'graceful' if a tool is currently mid-`ainvoke`, else 'hard'.
    The mode picks the right behavior at the persistent_graph check sites."""

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._loop_interrupt_flag = None
        mod._tool_inflight = False

    def test_loop_on_tool_start_does_not_claim_execution_inflight(self):
        import agent.api.persistent_app as mod

        async def _run():
            await mod._loop_on_tool_start("read_file", {"path": "/x"}, "tc1")

        asyncio.run(_run())
        assert mod._tool_inflight is False

    def test_loop_on_tool_execution_start_sets_inflight_at_effect_boundary(self):
        import agent.api.persistent_app as mod

        async def _run():
            await mod._loop_on_tool_execution_start("read_file", "tc1")

        with patch.object(mod, "_protected_cloud_runtime_ready", return_value=True):
            asyncio.run(_run())
        assert mod._tool_inflight is True

    def test_loop_on_tool_result_clears_inflight(self):
        import agent.api.persistent_app as mod

        mod._tool_inflight = True

        async def _run():
            await mod._loop_on_tool_result("read_file", "ok", "tc1")

        asyncio.run(_run())
        assert mod._tool_inflight is False


# ---------------------------------------------------------------------------
# Section 3 — persistent_graph interrupt semantics: hard drops partial,
#             graceful (and legacy bool) preserves it
# ---------------------------------------------------------------------------


class TestPersistentGraphInterruptModes:
    """Mid-stream interrupt mode controls whether the partial AIMessage is
    appended. The legacy bool API ('check_interrupt returns True') falls
    into the graceful branch — partial is preserved."""

    @pytest.mark.asyncio
    async def test_hard_interrupt_drops_partial_aimessage(self):
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            SystemMessage,
        )

        from agent.persistent_graph import PersistentLoopCallbacks, _execute_turn

        # First chunk arrives, then interrupt fires before the second.
        chunk1 = AIMessage(content="Half-typed ")
        chunk2 = AIMessage(content="response")

        async def _astream(msgs, **kw):
            yield chunk1
            yield chunk2

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        # Site 1 (pre-LLM): None → continue into the LLM call.
        # Site 2 (mid-astream after chunk1): "hard" → break, drop partial.
        interrupt_seq = [None, "hard", None, None]

        def _check_interrupt():
            return interrupt_seq.pop(0) if interrupt_seq else None

        callbacks = PersistentLoopCallbacks(
            get_user_input=AsyncMock(return_value="hello"),
            on_token=AsyncMock(),
            on_thinking=AsyncMock(),
            on_tool_start=AsyncMock(),
            on_tool_result=AsyncMock(),
            permission_check=AsyncMock(return_value=True),
            on_turn_start=AsyncMock(),
            on_turn_complete=AsyncMock(),
            on_error=AsyncMock(),
            check_interrupt=_check_interrupt,
        )

        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]
        config = MagicMock()
        config.llm.timeout = 600
        config.context_management.max_summary_length = 10000

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
        )

        assert result.interrupted is True
        # Hard interrupt: only system + human message; no partial AI message.
        assert len(messages) == 2
        assert isinstance(messages[0], SystemMessage)
        assert isinstance(messages[1], HumanMessage)

    @pytest.mark.asyncio
    async def test_graceful_interrupt_preserves_partial_aimessage(self):
        from langchain_core.messages import (
            AIMessage,
            AIMessageChunk,
            HumanMessage,
            SystemMessage,
        )

        from agent.persistent_graph import PersistentLoopCallbacks, _execute_turn

        chunk1 = AIMessageChunk(content="Tool calling now")

        async def _astream(msgs, **kw):
            yield chunk1

        llm = AsyncMock()
        llm.reasoning = None
        llm.astream = _astream

        # Arm only after the first streamed token. Provider-admission gained
        # additional pre-stream interrupt probes, so a positional call-count
        # fixture would test the old topology rather than the semantic seam.
        token_streamed = False
        interrupt_consumed = False

        async def _on_token(_text):
            nonlocal token_streamed
            token_streamed = True

        def _check_interrupt():
            nonlocal interrupt_consumed
            if token_streamed and not interrupt_consumed:
                interrupt_consumed = True
                return "graceful"
            return None

        callbacks = PersistentLoopCallbacks(
            get_user_input=AsyncMock(return_value="hello"),
            on_token=AsyncMock(side_effect=_on_token),
            on_thinking=AsyncMock(),
            on_tool_start=AsyncMock(),
            on_tool_result=AsyncMock(),
            permission_check=AsyncMock(return_value=True),
            on_turn_start=AsyncMock(),
            on_turn_complete=AsyncMock(),
            on_error=AsyncMock(),
            check_interrupt=_check_interrupt,
        )

        messages = [SystemMessage(content="sys"), HumanMessage(content="go")]
        config = MagicMock()
        config.llm.timeout = 600
        config.context_management.max_summary_length = 10000

        result = await _execute_turn(
            llm_with_tools=llm,
            tool_map={},
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            messages=messages,
            callbacks=callbacks,
            llm_timeout=600,
            auxiliary_llm=None,
            config=config,
        )

        assert result.interrupted is True
        # Graceful interrupt: partial AI message kept in history.
        assert len(messages) == 3
        assert isinstance(messages[2], AIMessage)


# ---------------------------------------------------------------------------
# Section 4 — Orchestrator per-turn lock semantics
# ---------------------------------------------------------------------------


class TestPerTurnLock:
    """The per-turn lock dedupes concurrent POSTs from multi-tab cockpits.
    Two POSTs racing on the same (thread, turn) share one asyncio.Lock; the
    second sees lock.locked() and returns 409."""

    def setup_method(self):
        import orchestrator.main as om

        om._thread_turn_locks.clear()
        om._thread_turn_inflight.clear()

    def teardown_method(self):
        import orchestrator.main as om

        om._thread_turn_locks.clear()
        om._thread_turn_inflight.clear()

    def test_same_key_shares_one_lock(self):
        import orchestrator.main as om

        lock_a = om._ensure_thread_turn_lock("thread-x", 5)
        lock_b = om._ensure_thread_turn_lock("thread-x", 5)
        assert lock_a is lock_b

    def test_different_keys_distinct_locks(self):
        import orchestrator.main as om

        l_thread = om._ensure_thread_turn_lock("thread-x", 5)
        l_other_thread = om._ensure_thread_turn_lock("thread-y", 5)
        l_other_turn = om._ensure_thread_turn_lock("thread-x", 6)
        assert l_thread is not l_other_thread
        assert l_thread is not l_other_turn

    @pytest.mark.asyncio
    async def test_concurrent_acquire_returns_locked_for_second(self):
        import orchestrator.main as om

        lock = om._ensure_thread_turn_lock("thread-x", 1)
        await lock.acquire()
        try:
            # Second caller sees the lock held; the HTTP handler returns
            # 409 based on this signal.
            assert lock.locked() is True
        finally:
            lock.release()
        # After release the next caller can acquire.
        assert lock.locked() is False


# ---------------------------------------------------------------------------
# Section 5 — Agent REST input endpoints reject when no session
# ---------------------------------------------------------------------------


class TestAgentRestInputEndpointsNoSession:
    """All three new REST endpoints must 503 when _session is None — same
    contract as POST /session/detach."""

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._loop_user_queue = None

    def test_api_input_503_without_session(self):
        from fastapi.testclient import TestClient

        from agent.api.persistent_app import create_persistent_app

        app = create_persistent_app("interactive")
        client = TestClient(app)
        resp = client.post("/api/input", json={"content": "hi"})
        assert resp.status_code == 503

    def test_api_interrupt_503_without_session(self):
        from fastapi.testclient import TestClient

        from agent.api.persistent_app import create_persistent_app

        app = create_persistent_app("interactive")
        client = TestClient(app)
        resp = client.post("/api/interrupt")
        assert resp.status_code == 503

    def test_api_approve_503_without_session(self):
        from fastapi.testclient import TestClient

        from agent.api.persistent_app import create_persistent_app

        app = create_persistent_app("interactive")
        client = TestClient(app)
        resp = client.post("/api/approve", json={"decision": "approve"})
        assert resp.status_code == 503

    def test_api_input_rejects_empty_content(self):
        import asyncio as _asyncio

        from fastapi.testclient import TestClient

        import agent.api.persistent_app as mod
        from agent.api.persistent_app import create_persistent_app

        # Stand up a minimal session so we get past the 503 gate.
        mod._session = MagicMock()
        mod._session.protected_cloud_required = False
        mod._loop_user_queue = _asyncio.Queue()
        mod._loop_last_user_content = [""]
        mod._retirement_admission_identity = None
        mod._termination_admission_fenced = False

        try:
            app = create_persistent_app("interactive")
            client = TestClient(app)
            resp = client.post("/api/input", json={"content": ""})
            assert resp.status_code == 400
        finally:
            mod._session = None
            mod._loop_user_queue = None

    @pytest.mark.asyncio
    async def test_api_input_starts_loop_without_websocket(self):
        """Direct/SSE sessions must not depend on a control WebSocket attach.

        The REST endpoint used by Cockpit should both accept the message and
        ensure the persistent loop is running, otherwise the input sits in
        _loop_user_queue forever.
        """
        import json
        from types import SimpleNamespace
        from uuid import uuid4

        from starlette.requests import Request

        import agent.api.persistent_app as mod

        exact_session_fingerprint = "sha256:" + ("a" * 64)

        async def receive():
            return {
                "type": "http.request",
                "body": json.dumps(
                    {
                        "content": "hello from REST",
                        "session_identity_fingerprint": exact_session_fingerprint,
                    }
                ).encode(),
                "more_body": False,
            }

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-type", b"application/json")],
            },
            receive,
        )

        seen_inputs: list[str] = []

        async def fake_run_persistent_loop(**kwargs):
            seen_inputs.append(await kwargs["callbacks"].get_user_input())

        class _DeliveryDB:
            def __init__(self):
                self.row = None

            async def persist_pinned_input_delivery(self, **kwargs):
                self.row = {
                    **kwargs,
                    "state": "owned",
                    "claim_generation": 1,
                    "message_id": str(uuid4()),
                    "transcript_inserted": True,
                }
                return dict(self.row)

            async def claim_pending_pinned_input_deliveries(self, **_kwargs):
                return [dict(self.row)] if self.row is not None else []

            async def mark_pinned_input_delivery_queued(self, **_kwargs):
                self.row["state"] = "queued"
                return True

        mod._thread_id = "thread-rest"
        mod._loop_task = None
        mod._loop_user_queue = asyncio.Queue()
        mod._loop_last_user_content = [""]
        mod._hard_interrupt_event = asyncio.Event()
        mod._input_runtime_generation = str(uuid4())
        mod._session_runtime_attach_token = str(uuid4())
        mod._queued_input_claims.clear()
        mod._session = SimpleNamespace(
            llm_with_tools=object(),
            tools=[],
            context_manager=object(),
            config=SimpleNamespace(
                llm=SimpleNamespace(timeout=1),
                interactive=SimpleNamespace(idle_timeout_minutes=0),
            ),
            system_prompt="system",
            messages=[],
            auxiliary_llm=None,
            recall_store=None,
            knowledge_store=None,
            # PersistentSession.thread_id is a real field; the final-memory
            # outbox reads it at loop start (memory_thread_id=...).
            thread_id="thread-events-phase2",
            project_id=None,
            project_ids=[],
            tool_context=None,
            turn_count=0,
            postgres_conn=_DeliveryDB(),
            memory_extraction_prompt="",
            memory_service=None,  # legacy path (manager flag off)
        )

        try:
            with (
                patch.object(
                    mod,
                    "run_persistent_loop",
                    new=fake_run_persistent_loop,
                ),
                patch.object(mod, "_loop_completion_handler", new=AsyncMock()),
                patch.object(mod, "_early_title_from_prompt", new=AsyncMock()),
                patch.object(
                    mod,
                    "_current_pinned_session_identity_fingerprint",
                    return_value=exact_session_fingerprint,
                ),
                patch.object(
                    mod,
                    "_orchestrator_client",
                    new=SimpleNamespace(agent_id=str(uuid4())),
                ),
                patch.dict("os.environ", {"POD_UID": "rest-input-pod"}),
            ):
                response = await mod.handle_api_input(request)
                # Persistence/queue ownership is not execution admission.
                assert response.status_code == 202
                await asyncio.wait_for(mod._loop_task, timeout=1)

            # Accept-time persistence wraps queue items as {content, id} so
            # the loop reuses the already-persisted row id
            # (session_silent_failure_audit.md #1).
            assert [i["content"] for i in seen_inputs] == ["hello from REST"]
            assert all(i["delivery_id"] for i in seen_inputs)
            assert all(i["claim_generation"] == 1 for i in seen_inputs)
        finally:
            mod._session = None
            mod._thread_id = None
            mod._loop_task = None
            mod._loop_user_queue = None
            mod._hard_interrupt_event = None
            mod._input_runtime_generation = None
            mod._session_runtime_attach_token = None
            mod._queued_input_claims.clear()


# ---------------------------------------------------------------------------
# Section 6 — No-cursor SSE replay anchors past the last completed turn
# ---------------------------------------------------------------------------


class TestNoCursorReplayStart:
    """A fresh SSE attach (no cached cursor — e.g. opening the session on a
    second device) must NOT replay the whole epoch from seq 0. The client has
    already loaded the thread's completed turns from REST history; replaying
    them as live frames re-renders the last assistant turn twice, split by a
    spurious "SESSION RESUMED" divider (the cold-attach twin of the
    gone_beyond_horizon duplicate render). The replay must start just past the
    last turn-terminal event so only the in-flight turn is replayed."""

    @pytest.mark.asyncio
    async def test_anchors_past_last_terminal_event(self):
        import orchestrator.main as om

        captured = {}

        class _Conn:
            async def fetchval(self, sql, *args):
                captured["sql"] = sql
                captured["args"] = args
                # Simulate MAX(seq) of the epoch's terminal events.
                return 7

        start = await om._no_cursor_replay_start(_Conn(), "thread-x", 3)

        # Replay only seq > 7 (the in-flight turn), NOT the whole epoch.
        assert start == 7
        # The anchor must filter on turn-terminal kinds, not every event —
        # otherwise it would stop at the last token and still re-replay the
        # completed turn's text.
        assert "turn.completed" in captured["sql"]
        assert "turn.error" in captured["sql"]
        assert captured["args"] == ("thread-x", 3)

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_turn_has_finished(self):
        """First turn still in flight (no terminal event yet) → replay from 0
        so the in-flight first turn, absent from REST history, is delivered."""
        import orchestrator.main as om

        class _Conn:
            async def fetchval(self, sql, *args):
                return 0  # COALESCE(MAX(seq), 0) with no terminal rows

        start = await om._no_cursor_replay_start(_Conn(), "thread-x", 0)
        assert start == 0

    @pytest.mark.asyncio
    async def test_coerces_null_max_to_zero(self):
        import orchestrator.main as om

        class _Conn:
            async def fetchval(self, sql, *args):
                return None

        start = await om._no_cursor_replay_start(_Conn(), "thread-x", 0)
        assert start == 0


# ---------------------------------------------------------------------------
# Section 7 — thread_event_stream kills the zombie epoch (Phase 1)
# ---------------------------------------------------------------------------


class _ScriptedConn:
    """A fake asyncpg connection for driving the SSE generator's poll loop.

    Dispatches on the SQL text rather than call order, so the open-path
    MIN(seq) probe, the mid-loop events_epoch re-read, and the
    _no_cursor_replay_start anchor query stay independently controllable.
    """

    def __init__(self, *, epochs, anchor=0, min_seq=0, rows_script=None):
        # Successive values returned by the events_epoch re-read.
        self._epochs = list(epochs)
        self._anchor = anchor
        self._min_seq = min_seq
        # A list of row-batches; each fetch() pops one, [] once exhausted.
        self._rows_script = list(rows_script or [])
        self.epoch_reads = 0

    async def fetch(self, sql, *args):
        if self._rows_script:
            return self._rows_script.pop(0)
        return []

    async def fetchval(self, sql, *args):
        if "events_epoch" in sql:
            self.epoch_reads += 1
            return self._epochs.pop(0) if self._epochs else None
        if "MIN(seq)" in sql:
            return self._min_seq
        if "turn.completed" in sql:  # _no_cursor_replay_start anchor
            return self._anchor
        return 0  # generic MAX(seq) tail (unused on the matching-cursor path)


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeRequest:
    """Minimal stand-in for starlette Request used by the SSE endpoint."""

    def __init__(self, last_event_id=None):
        self.headers = {}
        self.query_params = {"last_event_id": last_event_id} if last_event_id else {}

    async def is_disconnected(self):
        return False


class TestThreadEventStreamEpochRecheck:
    """Phase 1: a live SSE generator opened before an agent re-attach must
    detect the events_epoch bump on its own poll loop and terminate with a
    re-anchoring gone_beyond_horizon frame, instead of polling the dead epoch
    forever while its keepalive pings fool the client watchdog."""

    def _patch(self, monkeypatch, conn, *, server_epoch=3, recheck_s=0.0):
        import orchestrator.main as om

        monkeypatch.setattr(om, "THREAD_EVENTS_EPOCH_RECHECK_S", recheck_s)
        monkeypatch.setattr(
            om,
            "require_thread_owner",
            AsyncMock(return_value=(MagicMock(), {"events_epoch": server_epoch})),
        )
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire(conn)
        monkeypatch.setattr(om, "postgres_db", fake_db)
        # Neutralize the backoff sleeps so the loop spins instantly.
        monkeypatch.setattr(asyncio, "sleep", AsyncMock(return_value=None))

    async def _drain(self, resp, cap=25):
        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
            if len(chunks) > cap:
                await resp.body_iterator.aclose()
                break
        return chunks

    @pytest.mark.asyncio
    async def test_midstream_bump_emits_single_horizon(self, monkeypatch):
        """Criterion 1: epoch reads 3, 3, then 4; anchor 17 → exactly one
        gone_beyond_horizon with id: 4:17 and the epoch_bumped params, then
        the generator terminates."""
        import json

        import orchestrator.main as om

        conn = _ScriptedConn(epochs=[3, 3, 4], anchor=17, min_seq=0)
        self._patch(monkeypatch, conn, server_epoch=3)

        # Matching cursor (epoch 3) so the open path skips the mismatch/
        # retention branches and the loop starts clean.
        req = _FakeRequest(last_event_id="3:100")
        resp = await om.thread_event_stream("thread-x", req)
        chunks = await self._drain(resp)

        assert chunks[0] == ": open\n\n"
        horizon = [c for c in chunks if "gone_beyond_horizon" in c]
        assert len(horizon) == 1
        h = horizon[0]
        assert "id: 4:17\n" in h
        assert "event: gone_beyond_horizon\n" in h
        data_line = next(ln for ln in h.split("\n") if ln.startswith("data: "))
        payload = json.loads(data_line[len("data: ") :])
        assert payload["method"] == "gone_beyond_horizon"
        assert payload["params"] == {
            "epoch": 4,
            "server_seq": 17,
            "reason": "epoch_bumped_mid_stream",
        }
        # Three re-reads (matched, matched, changed); no more after terminate.
        assert conn.epoch_reads == 3
        # No frames after the horizon — the generator returned.
        assert chunks[-1] is h

    @pytest.mark.asyncio
    async def test_bump_with_empty_new_epoch_anchors_zero(self, monkeypatch):
        """Criterion 2: new epoch has no terminal turn yet → anchor 0 →
        id: 4:0, server_seq 0."""
        import json

        import orchestrator.main as om

        conn = _ScriptedConn(epochs=[4], anchor=0, min_seq=0)
        self._patch(monkeypatch, conn, server_epoch=3)

        req = _FakeRequest(last_event_id="3:100")
        resp = await om.thread_event_stream("thread-x", req)
        chunks = await self._drain(resp)

        horizon = [c for c in chunks if "gone_beyond_horizon" in c]
        assert len(horizon) == 1
        assert "id: 4:0\n" in horizon[0]
        data_line = next(ln for ln in horizon[0].split("\n") if ln.startswith("data: "))
        payload = json.loads(data_line[len("data: ") :])
        assert payload["params"]["server_seq"] == 0
        assert payload["params"]["epoch"] == 4

    @pytest.mark.asyncio
    async def test_thread_deleted_terminates_without_horizon(self, monkeypatch):
        """Criterion 3: events_epoch re-read returns None (row gone) → the
        generator terminates silently, no horizon frame."""
        import orchestrator.main as om

        conn = _ScriptedConn(epochs=[None], min_seq=0)
        self._patch(monkeypatch, conn, server_epoch=3)

        req = _FakeRequest(last_event_id="3:100")
        resp = await om.thread_event_stream("thread-x", req)
        chunks = await self._drain(resp)

        assert chunks == [": open\n\n"]
        assert conn.epoch_reads == 1

    @pytest.mark.asyncio
    async def test_steady_state_streams_without_epoch_query(self, monkeypatch):
        """Criterion 4: while rows flow the epoch is never re-read and no
        horizon frame appears — the guard is idle-only. id: lines carry the
        unchanged server epoch."""
        import orchestrator.main as om

        rows = [
            {"seq": 1, "kind": "token", "payload": {"content": "a"}},
            {"seq": 2, "kind": "token", "payload": {"content": "b"}},
        ]
        # High recheck window so the brief idle after the batch never trips it.
        conn = _ScriptedConn(epochs=[9], rows_script=[rows], min_seq=0)
        self._patch(monkeypatch, conn, server_epoch=3, recheck_s=999.0)

        req = _FakeRequest(last_event_id="3:0")
        resp = await om.thread_event_stream("thread-x", req)

        # Pull exactly the open comment + the two row frames, then close —
        # the post-batch idle loop spins without yielding, so don't ask for a
        # fourth chunk.
        it = resp.body_iterator
        c0 = await it.__anext__()
        c1 = await it.__anext__()
        c2 = await it.__anext__()
        await it.aclose()

        assert c0 == ": open\n\n"
        assert "id: 3:1\n" in c1
        assert "id: 3:2\n" in c2
        for c in (c1, c2):
            assert "gone_beyond_horizon" not in c
        assert conn.epoch_reads == 0


class TestThreadEventStreamPresence:
    @staticmethod
    def _owner_row(lane: str) -> tuple[dict, dict]:
        return ({"id": "user-x"}, {"events_epoch": 3, "execution_lane": lane})

    @pytest.mark.asyncio
    async def test_stateless_establishes_after_owner_gate(self, monkeypatch):
        import orchestrator.main as om
        from shared.thread_presence import PresenceRefresh

        auth = AsyncMock(return_value=self._owner_row("stateless"))
        refresh = AsyncMock(return_value=PresenceRefresh(True, True))
        monkeypatch.setattr(om, "require_thread_owner", auth)
        monkeypatch.setattr(om, "refresh_thread_presence", refresh)

        response = await om.thread_event_stream("thread-x", _FakeRequest())
        assert auth.await_count == 1
        refresh.assert_awaited_once_with(
            om.postgres_db,
            thread_id="thread-x",
            ttl_seconds=om.THREAD_CLIENT_PRESENCE_TTL_S,
            establish=True,
        )
        iterator = response.body_iterator
        assert await iterator.__anext__() == ": open\n\n"
        await iterator.aclose()
        # Closing is TTL grace, not a destructive presence delete.
        assert refresh.await_count == 1

    @pytest.mark.asyncio
    async def test_periodic_renewal_reauthorizes_before_touch(self, monkeypatch):
        import orchestrator.main as om
        from shared.thread_presence import PresenceRefresh

        auth = AsyncMock(
            side_effect=[
                self._owner_row("stateless"),
                self._owner_row("stateless"),
            ]
        )
        refresh = AsyncMock(
            side_effect=[PresenceRefresh(True, True), RuntimeError("renew failed")]
        )
        monkeypatch.setattr(om, "require_thread_owner", auth)
        monkeypatch.setattr(om, "refresh_thread_presence", refresh)
        monkeypatch.setattr(om, "THREAD_CLIENT_PRESENCE_RENEW_S", 0.0)
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire(_ScriptedConn(epochs=[], min_seq=0))
        monkeypatch.setattr(om, "postgres_db", fake_db)

        response = await om.thread_event_stream("thread-x", _FakeRequest())
        iterator = response.body_iterator
        assert await iterator.__anext__() == ": open\n\n"
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()

        assert auth.await_count == 2
        assert refresh.await_count == 2
        assert refresh.await_args_list[1].kwargs["establish"] is False

    @pytest.mark.asyncio
    async def test_renewal_auth_failure_closes_without_refresh(self, monkeypatch):
        import orchestrator.main as om
        from fastapi import HTTPException
        from shared.thread_presence import PresenceRefresh

        auth = AsyncMock(
            side_effect=[
                self._owner_row("stateless"),
                HTTPException(status_code=401, detail="session expired"),
            ]
        )
        refresh = AsyncMock(return_value=PresenceRefresh(True, True))
        monkeypatch.setattr(om, "require_thread_owner", auth)
        monkeypatch.setattr(om, "refresh_thread_presence", refresh)
        monkeypatch.setattr(om, "THREAD_CLIENT_PRESENCE_RENEW_S", 0.0)
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire(_ScriptedConn(epochs=[], min_seq=0))
        monkeypatch.setattr(om, "postgres_db", fake_db)

        response = await om.thread_event_stream("thread-x", _FakeRequest())
        iterator = response.body_iterator
        assert await iterator.__anext__() == ": open\n\n"
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()

        assert auth.await_count == 2
        assert refresh.await_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("caller", "tethers"),
        [
            ({"id": "user-x"}, True),  # cookie / OIDC session
            ({"id": "user-x", "auth_method": "mcp", "scopes": ["user"]}, True),
            ({"id": "user-x", "auth_method": "pat", "scopes": ["chat:write"]}, True),
            ({"id": "user-x", "auth_method": "pat", "scopes": ["admin"]}, True),
            (
                {"id": "user-x", "auth_method": "pat", "scopes": ["chat:read"]},
                False,
            ),
            (
                {
                    "id": "user-x",
                    "auth_method": "pat",
                    "scopes": ["chat:read", "jobs:write"],
                },
                False,
            ),
        ],
    )
    async def test_only_a_caller_who_could_answer_records_presence(
        self, monkeypatch, caller, tethers
    ):
        """Presence keeps permission prompts open (and the turn's executor
        slot leased) and blocks the natural pause, so it is a claim that
        someone attached can answer. A read-only token still streams — it
        is re-authorized on the renewal cadence — but never tethers."""
        import orchestrator.main as om
        from shared.thread_presence import PresenceRefresh

        thread = {"events_epoch": 3, "execution_lane": "stateless"}
        auth = AsyncMock(return_value=(caller, thread))
        refresh = AsyncMock(return_value=PresenceRefresh(True, True))
        monkeypatch.setattr(om, "require_thread_owner", auth)
        monkeypatch.setattr(om, "refresh_thread_presence", refresh)
        monkeypatch.setattr(om, "THREAD_CLIENT_PRESENCE_RENEW_S", 0.0)
        monkeypatch.setattr(om, "THREAD_EVENTS_EPOCH_RECHECK_S", 999.0)
        monkeypatch.setattr(asyncio, "sleep", AsyncMock(return_value=None))
        rows = [{"seq": 1, "kind": "token", "payload": {"content": "a"}}]
        conn = _ScriptedConn(epochs=[3], rows_script=[rows], min_seq=0)
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire(conn)
        monkeypatch.setattr(om, "postgres_db", fake_db)

        response = await om.thread_event_stream(
            "thread-x", _FakeRequest(last_event_id="3:0")
        )
        iterator = response.body_iterator
        assert await iterator.__anext__() == ": open\n\n"
        # The first loop pass runs the renewal before it streams the row:
        # every stateless stream is re-authorized, only a tethering caller
        # writes presence.
        assert "id: 3:1\n" in await iterator.__anext__()
        await iterator.aclose()

        assert auth.await_count >= 2
        if tethers:
            assert refresh.await_args_list[0].kwargs["establish"] is True
            assert refresh.await_count >= 2
        else:
            refresh.assert_not_called()

    @pytest.mark.asyncio
    async def test_pinned_stream_never_touches_presence(self, monkeypatch):
        import orchestrator.main as om

        monkeypatch.setattr(
            om,
            "require_thread_owner",
            AsyncMock(return_value=self._owner_row("pinned")),
        )
        refresh = AsyncMock()
        monkeypatch.setattr(om, "refresh_thread_presence", refresh)

        response = await om.thread_event_stream("thread-x", _FakeRequest())
        iterator = response.body_iterator
        assert await iterator.__anext__() == ": open\n\n"
        await iterator.aclose()
        refresh.assert_not_called()
