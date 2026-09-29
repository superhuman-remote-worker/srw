"""Real-Postgres proofs for exact-lease permission retirement."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import agent.api.persistent_app as persistent_app
from orchestrator.services.run_queue_reaper import retry_stale_permission_requests
from agent.api.lease_context import LeaseHandle, current_lease
from shared.session_permission_retirement import (
    retire_stale_stateless_permissions,
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def pg_pool(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            CREATE TABLE threads (
                id uuid PRIMARY KEY,
                execution_lane text NOT NULL,
                agent_id uuid,
                status text NOT NULL DEFAULT 'active',
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                events_epoch integer NOT NULL DEFAULT 3,
                events_seq_hwm bigint NOT NULL DEFAULT 0
            );
            CREATE TABLE run_queue (
                unit_id uuid PRIMARY KEY REFERENCES threads(id) ON DELETE CASCADE,
                unit_kind text NOT NULL,
                state text NOT NULL,
                lease_token bigint NOT NULL,
                leased_by text,
                last_leased_by text,
                leased_until timestamptz,
                max_attempts integer NOT NULL DEFAULT 5,
                attempts_since_completion integer NOT NULL DEFAULT 0,
                queued_at timestamptz,
                run_after timestamptz,
                input_seq bigint,
                consumed_seq bigint,
                control_input_seq bigint NOT NULL DEFAULT 0,
                control_consumed_seq bigint NOT NULL DEFAULT 0,
                interrupt_admission_lease_token bigint,
                interrupt_admission_turn_id integer
            );
            CREATE TABLE thread_permission_requests (
                id uuid PRIMARY KEY,
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                tool_call_id text NOT NULL,
                tool_name text NOT NULL DEFAULT 'run_command',
                tool_args jsonb NOT NULL DEFAULT '{}'::jsonb,
                status text NOT NULL DEFAULT 'pending',
                requested_at timestamptz NOT NULL DEFAULT now(),
                decided_at timestamptz,
                decided_by text,
                accepted_lease_token bigint,
                UNIQUE (id, thread_id),
                CHECK (accepted_lease_token IS NULL OR accepted_lease_token > 0)
            );
            CREATE TABLE thread_interrupt_requests (
                id uuid PRIMARY KEY,
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                client_request_id uuid NOT NULL,
                target_turn_id integer NOT NULL,
                accepted_lease_token bigint NOT NULL,
                accepted_leased_by text NOT NULL,
                outcome text,
                result jsonb,
                applied_mode text,
                applied_at timestamptz,
                applied_lease_token bigint,
                journal_epoch integer,
                journal_seq bigint,
                acknowledged_at timestamptz,
                error_code text,
                requested_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE TABLE thread_events (
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                epoch integer NOT NULL,
                seq bigint NOT NULL,
                kind text NOT NULL,
                payload jsonb NOT NULL,
                control_request_id uuid,
                interrupt_request_id uuid,
                permission_request_id uuid,
                PRIMARY KEY (thread_id, epoch, seq),
                FOREIGN KEY (permission_request_id, thread_id)
                    REFERENCES thread_permission_requests(id, thread_id)
            );
            CREATE UNIQUE INDEX idx_thread_events_permission_request
                ON thread_events(permission_request_id)
                WHERE permission_request_id IS NOT NULL;
            """
        )
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8, timeout=10)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def pg(pg_pool):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE thread_events, thread_interrupt_requests, "
            "thread_permission_requests, run_queue, threads CASCADE"
        )
    yield pg_pool


async def _seed_thread(
    pg,
    *,
    queue_token: int,
    queue_state: str,
    leased_by: str | None = None,
) -> UUID:
    thread_id = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, execution_lane, events_epoch) "
            "VALUES ($1, 'stateless', 3)",
            thread_id,
        )
        await conn.execute(
            "INSERT INTO run_queue "
            "(unit_id, unit_kind, state, lease_token, leased_by, consumed_seq) "
            "VALUES ($1, 'session_turn', $2, $3, $4, 5)",
            thread_id,
            queue_state,
            queue_token,
            leased_by,
        )
    return thread_id


async def _insert_permission(pg, thread_id: UUID, token: int | None) -> UUID:
    request_id = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_permission_requests "
            "(id, thread_id, tool_call_id, accepted_lease_token) "
            "VALUES ($1, $2, $3, $4)",
            request_id,
            thread_id,
            f"tool-{request_id}",
            token,
        )
    return request_id


@pytest.mark.asyncio
async def test_force_end_token_zero_expires_legacy_null_with_linked_receipt(pg):
    thread_id = await _seed_thread(pg, queue_token=1, queue_state="queued")
    request_id = await _insert_permission(pg, thread_id, None)

    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.fetchrow(
                "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE", thread_id
            )
            await conn.fetchrow(
                "SELECT 1 FROM run_queue WHERE unit_id = $1 FOR UPDATE", thread_id
            )
            result = await retire_stale_stateless_permissions(
                conn,
                thread_id=str(thread_id),
                retired_lease_token=0,
                successor_lease_token=1,
                reason="force_end",
                epoch_already_bumped=False,
            )

        row = await conn.fetchrow(
            "SELECT request.status, request.decided_by, event.kind, event.payload, "
            "event.permission_request_id, event.epoch, event.seq "
            "FROM thread_permission_requests request "
            "JOIN thread_events event ON event.permission_request_id = request.id "
            "WHERE request.id = $1",
            request_id,
        )

    assert result.count == 1
    assert row["status"] == "expired"
    assert row["decided_by"] == "system/force_end"
    assert row["kind"] == "permission.resolved"
    assert row["permission_request_id"] == request_id
    payload = json.loads(row["payload"])
    assert payload["legacy_unbound"] is True
    assert payload["accepted_lease_token"] is None
    assert (row["epoch"], row["seq"]) == (4, 1)


@pytest.mark.asyncio
async def test_done_repair_requires_and_uses_exact_interrupt_recovery_receipt(pg):
    thread_id = await _seed_thread(pg, queue_token=10, queue_state="done")
    stamped_permission_id = await _insert_permission(pg, thread_id, 9)
    null_permission_id = await _insert_permission(pg, thread_id, None)
    interrupt_id = uuid4()
    client_request_id = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET events_seq_hwm = 1 WHERE id = $1", thread_id
        )
        await conn.execute(
            "INSERT INTO thread_interrupt_requests "
            "(id, thread_id, client_request_id, target_turn_id, "
            " accepted_lease_token, accepted_leased_by, outcome, result, "
            " applied_mode, applied_at, applied_lease_token, journal_epoch, "
            " journal_seq, acknowledged_at) "
            "VALUES ($1, $2, $3, 4, 9, 'old-owner', 'applied', "
            "        jsonb_build_object("
            "          'consumed_input_seq', 5, "
            "          'input_settled_by_lease_token', 10, "
            "          'input_settlement', 'lease_recovery'), "
            "        'hard', now(), 9, 3, 1, now())",
            interrupt_id,
            thread_id,
            client_request_id,
        )
        await conn.execute(
            "INSERT INTO thread_events "
            "(thread_id, epoch, seq, kind, payload, interrupt_request_id) "
            "VALUES ($1, 3, 1, 'interrupt.ack', "
            "        jsonb_build_object("
            "          'request_id', $2::uuid::text, "
            "          'client_request_id', $3::uuid::text, "
            "          'target_turn_id', 4, 'applied', true, 'mode', 'hard'), "
            "        $2)",
            thread_id,
            interrupt_id,
            client_request_id,
        )

        assert await retry_stale_permission_requests(conn) == 2
        permissions = await conn.fetch(
            "SELECT id, status, decided_by FROM thread_permission_requests "
            "WHERE thread_id = $1 ORDER BY accepted_lease_token NULLS FIRST",
            thread_id,
        )
        receipts = await conn.fetch(
            "SELECT epoch, seq, payload, permission_request_id "
            "FROM thread_events WHERE permission_request_id IS NOT NULL "
            "AND thread_id = $1 ORDER BY seq",
            thread_id,
        )
        thread = await conn.fetchrow(
            "SELECT events_epoch, events_seq_hwm FROM threads WHERE id = $1",
            thread_id,
        )

    assert {row["id"]: (row["status"], row["decided_by"]) for row in permissions} == {
        stamped_permission_id: ("expired", "system/lease_expired"),
        null_permission_id: ("expired", "system/lease_expired"),
    }
    assert [(receipt["epoch"], receipt["seq"]) for receipt in receipts] == [
        (4, 1),
        (4, 2),
    ]
    assert {receipt["permission_request_id"] for receipt in receipts} == {
        stamped_permission_id,
        null_permission_id,
    }
    assert {
        json.loads(receipt["payload"])["accepted_lease_token"] for receipt in receipts
    } == {9, None}
    assert (thread["events_epoch"], thread["events_seq_hwm"]) == (4, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("settled_token", [None, 9, 11])
async def test_done_repair_excludes_missing_or_mismatched_recovery_marker(
    pg, settled_token
):
    thread_id = await _seed_thread(pg, queue_token=10, queue_state="done")
    permission_id = await _insert_permission(pg, thread_id, 9)
    if settled_token is not None:
        interrupt_id = uuid4()
        client_request_id = uuid4()
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE threads SET events_seq_hwm = 1 WHERE id = $1", thread_id
            )
            await conn.execute(
                "INSERT INTO thread_interrupt_requests "
                "(id, thread_id, client_request_id, target_turn_id, "
                " accepted_lease_token, accepted_leased_by, outcome, result, "
                " applied_mode, applied_at, applied_lease_token, journal_epoch, "
                " journal_seq, acknowledged_at) "
                "VALUES ($1, $2, $3, 4, 9, 'old-owner', 'applied', "
                "        jsonb_build_object("
                "          'consumed_input_seq', 5, "
                "          'input_settled_by_lease_token', $4::bigint, "
                "          'input_settlement', 'lease_recovery'), "
                "        'hard', now(), 9, 3, 1, now())",
                interrupt_id,
                thread_id,
                client_request_id,
                settled_token,
            )
            await conn.execute(
                "INSERT INTO thread_events "
                "(thread_id, epoch, seq, kind, payload, interrupt_request_id) "
                "VALUES ($1, 3, 1, 'interrupt.ack', "
                "        jsonb_build_object("
                "          'request_id', $2::uuid::text, "
                "          'client_request_id', $3::uuid::text, "
                "          'target_turn_id', 4, 'applied', true, "
                "          'mode', 'hard'), $2)",
                thread_id,
                interrupt_id,
                client_request_id,
            )

    async with pg.acquire() as conn:
        assert await retry_stale_permission_requests(conn) == 0
        status = await conn.fetchval(
            "SELECT status FROM thread_permission_requests WHERE id = $1",
            permission_id,
        )
        receipt_count = await conn.fetchval(
            "SELECT count(*) FROM thread_events WHERE permission_request_id = $1",
            permission_id,
        )
        epoch = await conn.fetchval(
            "SELECT events_epoch FROM threads WHERE id = $1", thread_id
        )

    assert status == "pending"
    assert receipt_count == 0
    assert epoch == 3


@pytest.mark.asyncio
async def test_successor_recovery_releases_discovery_locks_and_seeds_next_seq(
    pg, monkeypatch
):
    thread_id = await _seed_thread(
        pg, queue_token=10, queue_state="leased", leased_by="successor"
    )
    old_request = await _insert_permission(pg, thread_id, 9)
    null_request = await _insert_permission(pg, thread_id, None)

    enqueued = []
    writers = []

    class NewWriter:
        def __init__(self, *, thread_id, epoch, **_kwargs):
            self.thread_id = thread_id
            self.epoch = epoch
            self.started = False
            writers.append(self)

        def start(self):
            self.started = True

        def enqueue(self, event):
            enqueued.append(event)
            return True

    class OldWriter:
        def __init__(self):
            self.thread_id = str(thread_id)
            self.closed = False

        async def close(self):
            self.closed = True

    old_writer = OldWriter()
    handle = LeaseHandle()
    handle.update(str(thread_id), 10)
    lease_context = current_lease.set(handle)
    monkeypatch.setattr(
        persistent_app,
        "_session",
        SimpleNamespace(postgres_conn=pg, turn_count=4),
    )
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", str(thread_id))
    monkeypatch.setattr(persistent_app, "_event_writer", old_writer)
    monkeypatch.setattr(persistent_app, "_events_epoch", 3)
    monkeypatch.setattr(persistent_app, "_next_seq", 99)
    monkeypatch.setattr(persistent_app, "_OrderedPersistentEventWriter", NewWriter)
    try:
        result = await asyncio.wait_for(
            persistent_app._reconcile_stale_thread_interrupts(lease_token=10),
            timeout=5,
        )
        persistent_app._broadcast_frame("turn.started", {}, durable_receipt=False)
    finally:
        current_lease.reset(lease_context)

    assert result == (0, 5)
    assert old_writer.closed is True
    assert len(writers) == 1 and writers[0].started is True
    assert persistent_app._events_epoch == 4
    assert persistent_app._next_seq == 3
    assert [(event.epoch, event.seq) for event in enqueued] == [(4, 3)]

    async with pg.acquire() as conn:
        statuses = dict(
            await conn.fetch(
                "SELECT id, status FROM thread_permission_requests "
                "WHERE thread_id = $1",
                thread_id,
            )
        )
        events = await conn.fetch(
            "SELECT permission_request_id, epoch, seq, payload "
            "FROM thread_events WHERE thread_id = $1 ORDER BY seq",
            thread_id,
        )
        thread = await conn.fetchrow(
            "SELECT events_epoch, events_seq_hwm FROM threads WHERE id = $1",
            thread_id,
        )

    assert statuses == {old_request: "expired", null_request: "expired"}
    assert [(event["epoch"], event["seq"]) for event in events] == [(4, 1), (4, 2)]
    assert {event["permission_request_id"] for event in events} == {
        old_request,
        null_request,
    }
    assert {
        json.loads(event["payload"])["accepted_lease_token"] for event in events
    } == {
        9,
        None,
    }
    assert (thread["events_epoch"], thread["events_seq_hwm"]) == (4, 2)


@pytest.mark.asyncio
async def test_approval_wins_row_lock_race_without_expiry_receipt(pg):
    thread_id = await _seed_thread(pg, queue_token=10, queue_state="parked")
    request_id = await _insert_permission(pg, thread_id, 9)

    approver = await pg.acquire()
    retire_conn = await pg.acquire()
    approval_tx = approver.transaction()
    await approval_tx.start()
    approval_committed = False
    try:
        assert (
            await approver.execute(
                "UPDATE thread_permission_requests SET status = 'approved' "
                "WHERE id = $1 AND status = 'pending'",
                request_id,
            )
            == "UPDATE 1"
        )

        async def retire():
            async with retire_conn.transaction():
                await retire_conn.fetchrow(
                    "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE", thread_id
                )
                await retire_conn.fetchrow(
                    "SELECT 1 FROM run_queue WHERE unit_id = $1 FOR UPDATE",
                    thread_id,
                )
                return await retire_stale_stateless_permissions(
                    retire_conn,
                    thread_id=str(thread_id),
                    retired_lease_token=9,
                    successor_lease_token=10,
                    reason="lease_expired",
                    epoch_already_bumped=False,
                )

        retire_task = asyncio.create_task(retire())
        await asyncio.sleep(0.05)
        assert not retire_task.done(), "retirement must wait for the row-CAS winner"
        await approval_tx.commit()
        approval_committed = True
        result = await asyncio.wait_for(retire_task, timeout=5)
    finally:
        if not approval_committed:
            await approval_tx.rollback()
        await pg.release(retire_conn)
        await pg.release(approver)

    async with pg.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM thread_permission_requests WHERE id = $1", request_id
        )
        receipt_count = await conn.fetchval(
            "SELECT count(*) FROM thread_events WHERE permission_request_id = $1",
            request_id,
        )
        epoch = await conn.fetchval(
            "SELECT events_epoch FROM threads WHERE id = $1", thread_id
        )

    assert result.count == 0
    assert status == "approved"
    assert receipt_count == 0
    assert epoch == 3
