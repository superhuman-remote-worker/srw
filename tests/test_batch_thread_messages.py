"""HF-2 — the turn-complete reconcile batch against a real Postgres.

The reconcile switched from a per-message save_thread_message loop to one
pipelined save_thread_messages. These tests pin the resume-critical invariants
the batch must preserve, verified on a real pg (testcontainers):

  * ON CONFLICT (id) dedup — reconciling the incrementally-written rows updates
    them in place, never duplicates;
  * seq stability — seq is assigned once on first insert and preserved across
    the reconcile update (it is the resume cursor);
  * idempotency — running the batch twice is a no-op on the row set + seqs;
  * the threads activity/turn bump still happens, once.
"""

import asyncio
import json
import uuid

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.database.postgres_db import PostgresDB, _coerce_row_id

_TID = "aaaaaaaa-0000-0000-0000-000000000001"
_PROJECT_ID = "aaaaaaaa-0000-0000-0000-000000000099"
_CONTROL_IDS = (
    "bbbbbbbb-0000-0000-0000-000000000001",
    "bbbbbbbb-0000-0000-0000-000000000002",
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def db(pg_dsn):
    d = PostgresDB(connection_string=pg_dsn)
    await d.connect()
    async with d.acquire() as conn:
        await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS threads (
                id uuid PRIMARY KEY,
                project_id uuid,
                metadata jsonb DEFAULT '{}'::jsonb,
                last_activity timestamptz,
                total_turns int DEFAULT 0,
                status text DEFAULT 'active',
                execution_lane text DEFAULT 'stateless',
                agent_id uuid,
                events_epoch int NOT NULL DEFAULT 0,
                events_seq_hwm bigint NOT NULL DEFAULT 0
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_mounts (
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                mount_kind text NOT NULL,
                source_ref uuid
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_messages (
                id uuid PRIMARY KEY,
                thread_id uuid REFERENCES threads(id) ON DELETE CASCADE,
                role text,
                content text,
                tool_calls jsonb,
                turn_number int,
                metrics jsonb,
                tool_call_id text,
                thinking text,
                reasoning jsonb,
                tool_results jsonb,
                provider text,
                provider_raw jsonb,
                additional_kwargs jsonb,
                response_metadata jsonb,
                seq bigserial,
                created_at timestamptz DEFAULT now(),
                rewound_at timestamptz,
                turn_execution_id uuid
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS completion_effects (
                producer_kind text NOT NULL,
                producer_id uuid NOT NULL,
                scope_id uuid,
                effect_name text NOT NULL,
                effect_group text NOT NULL,
                state text NOT NULL DEFAULT 'pending',
                attempts int NOT NULL DEFAULT 0,
                max_attempts int NOT NULL DEFAULT 5,
                run_after timestamptz NOT NULL DEFAULT now(),
                created_at timestamptz NOT NULL DEFAULT now(),
                intent_at timestamptz,
                complete_by timestamptz,
                completed_at timestamptz,
                detail jsonb NOT NULL DEFAULT '{}'::jsonb,
                error_code text,
                claimed_by uuid,
                PRIMARY KEY (producer_kind, producer_id, effect_name)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_queue (
                unit_id uuid PRIMARY KEY REFERENCES threads(id) ON DELETE CASCADE,
                unit_kind text NOT NULL,
                state text NOT NULL,
                lease_token bigint NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_control_requests (
                id uuid PRIMARY KEY,
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                accepted_agent_id uuid,
                runtime_generation uuid,
                outcome text
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_events (
                id bigserial PRIMARY KEY,
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                epoch int NOT NULL,
                seq bigint NOT NULL,
                kind text NOT NULL,
                payload jsonb NOT NULL,
                control_request_id uuid,
                interrupt_request_id uuid,
                UNIQUE (thread_id, epoch, seq)
            )
            """
        )
        await conn.execute(
            "TRUNCATE thread_events, thread_control_requests, thread_mounts, "
            "thread_messages, completion_effects, run_queue RESTART IDENTITY"
        )
        await conn.execute(
            "INSERT INTO threads "
            "(id, total_turns, status, execution_lane, agent_id, "
            "events_epoch, events_seq_hwm) "
            "VALUES ($1, 0, 'active', 'stateless', NULL, 4, 0) "
            "ON CONFLICT (id) DO UPDATE SET total_turns = 0, "
            "project_id = NULL, metadata = '{}'::jsonb, last_activity = NULL, "
            "status = 'active', execution_lane = 'stateless', "
            "agent_id = NULL, events_epoch = 4, events_seq_hwm = 0",
            uuid.UUID(_TID),
        )
        await conn.executemany(
            "INSERT INTO thread_control_requests "
            "(id, thread_id, accepted_agent_id, outcome) "
            "VALUES ($1::uuid, $2::uuid, NULL, NULL)",
            [(request_id, _TID) for request_id in _CONTROL_IDS],
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'leased', 17)",
            uuid.UUID(_TID),
        )
    yield d
    await d.close()


def _row(mid, role, content, turn, **extra):
    base = {
        "id": mid,
        "role": role,
        "content": content,
        "tool_calls": None,
        "turn_number": turn,
        "metrics": None,
        "tool_call_id": None,
        "thinking": None,
    }
    base.update(extra)
    return base


async def _seqs_by_id(db):
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, seq, content, metrics FROM thread_messages "
            "WHERE thread_id = $1 ORDER BY seq",
            uuid.UUID(_TID),
        )
    return {str(r["id"]): r for r in rows}


async def _seed_turn_boundary(db, raw_id: str, turn_number: int, role="event"):
    """Model the orchestrator's already-durable accepted input row."""

    row_id = _coerce_row_id(raw_id)
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_messages "
            "(id, thread_id, role, content, turn_number) "
            "VALUES ($1::uuid, $2::uuid, $3, 'accepted input', $4)",
            row_id,
            _TID,
            role,
            turn_number,
        )
    return row_id


async def _terminal_end_after_release(db, thread_locked, release_queue):
    """Model public End's load-bearing threads -> run_queue transaction."""

    async with db.acquire() as conn:
        async with conn.transaction():
            locked = await conn.fetchval(
                "SELECT 1 FROM threads WHERE id = $1::uuid FOR UPDATE",
                _TID,
            )
            assert locked == 1
            thread_locked.set()
            await release_queue.wait()
            token = await conn.fetchval(
                "SELECT lease_token FROM run_queue WHERE unit_id = $1::uuid FOR UPDATE",
                _TID,
            )
            assert token == 17
            await conn.execute(
                "UPDATE run_queue SET state = 'done', lease_token = 18 "
                "WHERE unit_id = $1::uuid",
                _TID,
            )
            await conn.execute(
                "UPDATE threads SET status = 'ended' WHERE id = $1::uuid",
                _TID,
            )


async def _wait_for_blocked_query(db, *fragments):
    """Wait until the competing writer is blocked on its first thread lock."""

    await _wait_for_blocked_query_count(db, 1, *fragments)


async def _wait_for_blocked_query_count(db, expected, *fragments):
    """Wait until ``expected`` active queries are blocked on matching locks."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while loop.time() < deadline:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT query, wait_event_type FROM pg_stat_activity "
                "WHERE datname = current_database() "
                "AND pid <> pg_backend_pid() AND state = 'active'"
            )
        matching = 0
        for row in rows:
            query = str(row["query"] or "")
            if row["wait_event_type"] == "Lock" and all(
                fragment in query for fragment in fragments
            ):
                matching += 1
        if matching >= expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"expected {expected} blocked writers on query fragments: {fragments!r}"
    )


@pytest.mark.asyncio
async def test_reconcile_dedups_and_preserves_seq(db):
    # Incremental mid-turn writes land 3 rows (each returns its assigned seq).
    ids = ["msg_a", "msg_b", "msg_c"]
    first = {}
    for i, mid in enumerate(ids):
        r = await db.save_thread_message(
            thread_id=_TID, role="ai", content=f"draft-{mid}", turn_number=2, id=mid
        )
        first[mid] = r["seq"]

    # Turn-complete reconcile: same ids, updated content + metrics, batched.
    rows = [
        _row(mid, "ai", f"final-{mid}", 2, metrics={"tokens": i})
        for i, mid in enumerate(ids)
    ]
    await db.save_thread_messages(_TID, rows)

    after = await _seqs_by_id(db)
    # No duplicates — one row per id.
    assert len(after) == 3
    for mid in ids:
        row_id = _coerce_row_id(mid)
        assert row_id in after, f"{mid} row missing after reconcile"
        # seq preserved across the upsert (the resume cursor stays stable).
        assert after[row_id]["seq"] == first[mid], f"seq moved for {mid}"
        # content updated to the reconciled value.
        assert after[row_id]["content"] == f"final-{mid}"


@pytest.mark.asyncio
async def test_reconcile_is_idempotent(db):
    rows = [_row("only", "ai", "hello", 1)]
    await db.save_thread_messages(_TID, rows)
    once = await _seqs_by_id(db)
    await db.save_thread_messages(_TID, rows)
    twice = await _seqs_by_id(db)
    assert list(once) == list(twice), "row set changed on re-run"
    for k in once:
        assert once[k]["seq"] == twice[k]["seq"], "seq changed on re-run"


@pytest.mark.asyncio
async def test_compaction_view_fills_a_lost_row_but_never_rewrites_a_landed_one(db):
    """``insert_if_absent`` rows (lossy compaction views): the landed full
    result keeps its content and seq, a result whose incremental write was
    lost is filled with the view (no tool call left without its result), rows
    the batch mints keep their order, and the effect range still ends at the
    final answer."""
    await _seed_turn_boundary(db, "view-boundary", 9)
    landed = await db.save_thread_message(
        thread_id=_TID,
        role="tool",
        content="full result",
        turn_number=9,
        tool_call_id="tc-landed",
        id="view-landed",
    )
    rows = [
        _row(
            "view-landed",
            "tool",
            "capped view",
            9,
            tool_call_id="tc-landed",
            insert_if_absent=True,
        ),
        _row(
            "view-lost",
            "tool",
            "capped view",
            9,
            tool_call_id="tc-lost",
            insert_if_absent=True,
        ),
        _row("view-final", "ai", "answer", 9),
    ]
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        for _attempt in range(2):  # the reconcile is retryable: idempotent
            await db.save_thread_messages(
                _TID,
                rows,
                turn_input_message_id="view-boundary",
                turn_number=9,
                memory_scope_kind="thread",
                memory_scope_id=_TID,
            )
    finally:
        current_lease.reset(context_token)

    after = await _seqs_by_id(db)
    landed_row = after[_coerce_row_id("view-landed")]
    lost_row = after[_coerce_row_id("view-lost")]
    final_row = after[_coerce_row_id("view-final")]
    assert landed_row["content"] == "full result"
    assert landed_row["seq"] == landed["seq"]
    assert lost_row["content"] == "capped view"
    assert lost_row["seq"] < final_row["seq"]
    assert len(after) == 4  # boundary + three rows, nothing duplicated
    async with db.acquire() as conn:
        detail = await conn.fetchval(
            "SELECT detail FROM completion_effects WHERE producer_kind = 'session_turn'"
        )
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail["end_seq"] == final_row["seq"]


@pytest.mark.asyncio
async def test_compaction_view_keeps_its_tool_link_in_step_with_the_ai_row(db):
    """A mid-turn cross-family model switch remaps tool-call ids in memory
    (sanitize_history_for_provider_boundary) on the AI message AND its tool
    result. The AI row upserts the new id; the view row must follow it, or
    the stored pair splits and restore prunes both halves as orphans. The
    link moves, the landed full content still never does."""
    await _seed_turn_boundary(db, "link-boundary", 9)
    await db.save_thread_message(
        thread_id=_TID,
        role="tool",
        content="full result",
        turn_number=9,
        tool_call_id="toolu_01ABCDEF",
        id="link-landed",
    )
    rows = [
        _row(
            "link-landed",
            "tool",
            "capped view",
            9,
            tool_call_id="7aec07fb2",
            insert_if_absent=True,
        ),
        _row("link-final", "ai", "answer", 9),
    ]
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        await db.save_thread_messages(
            _TID,
            rows,
            turn_input_message_id="link-boundary",
            turn_number=9,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        landed = await conn.fetchrow(
            "SELECT content, tool_call_id FROM thread_messages WHERE id = $1",
            _coerce_row_id("link-landed"),
        )
    assert landed["content"] == "full result"
    assert landed["tool_call_id"] == "7aec07fb2"


@pytest.mark.asyncio
async def test_reconcile_bumps_thread_turn_count(db):
    await db.save_thread_messages(
        _TID,
        [_row("t_hi", "ai", "x", 7), _row("t_lo", "tool", "y", 5)],
    )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT total_turns, last_activity FROM threads WHERE id = $1",
            uuid.UUID(_TID),
        )
    # GREATEST(existing, max turn_number in the batch) => 7.
    assert row["total_turns"] == 7
    assert row["last_activity"] is not None


@pytest.mark.asyncio
async def test_stateless_reconcile_mints_one_stable_memory_effect_per_turn(db):
    input_id = await _seed_turn_boundary(db, "effect-boundary", 8)
    output_id = _coerce_row_id("effect-output")
    output_rows = [_row("effect-output", "ai", "durable answer", 8)]
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        first = await db.save_thread_messages(
            _TID,
            output_rows,
            turn_input_message_id="effect-boundary",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
        second = await db.save_thread_messages(
            _TID,
            output_rows,
            turn_input_message_id="effect-boundary",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
    finally:
        current_lease.reset(context_token)

    assert first == second
    uuid.UUID(first)
    async with db.acquire() as conn:
        message_execution_id = await conn.fetchval(
            "SELECT turn_execution_id FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )
        effects = await conn.fetch(
            "SELECT producer_id, scope_id, effect_name, effect_group, state, detail "
            "FROM completion_effects WHERE producer_kind = 'session_turn'"
        )
        total_turns = await conn.fetchval(
            "SELECT total_turns FROM threads WHERE id = $1::uuid", _TID
        )
        seqs = await conn.fetch(
            "SELECT id, seq FROM thread_messages WHERE id = ANY($1::uuid[])",
            [input_id, output_id],
        )

    assert str(message_execution_id) == first
    assert len(effects) == 1
    effect = effects[0]
    assert str(effect["producer_id"]) == first
    assert str(effect["scope_id"]) == _TID
    assert effect["effect_name"] == "final_memory_extraction"
    assert effect["effect_group"] == "memory_extraction"
    assert effect["state"] == "pending"
    detail = effect["detail"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    seq_by_id = {str(row["id"]): int(row["seq"]) for row in seqs}
    assert detail == {
        "input_message_id": input_id,
        "turn_number": 8,
        "memory_scope_kind": "thread",
        "memory_scope_id": _TID,
        "boundary_seq": seq_by_id[input_id],
        "end_seq": seq_by_id[output_id],
    }
    assert total_turns == 8


@pytest.mark.asyncio
async def test_distinct_boundaries_do_not_collapse_when_turn_number_is_reused(db):
    await _seed_turn_boundary(db, "first-reused-turn", 8)
    await _seed_turn_boundary(db, "second-reused-turn", 8)
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        first = await db.save_thread_messages(
            _TID,
            [],
            turn_input_message_id="first-reused-turn",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
        second = await db.save_thread_messages(
            _TID,
            [],
            turn_input_message_id="second-reused-turn",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
    finally:
        current_lease.reset(context_token)

    assert first != second
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM completion_effects "
                "WHERE producer_kind = 'session_turn'"
            )
            == 2
        )


@pytest.mark.asyncio
async def test_delayed_effect_range_does_not_absorb_reused_turn_number(db):
    first_input = await _seed_turn_boundary(db, "first-range-boundary", 8)
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        first = await db.save_thread_messages(
            _TID,
            [_row("first-range-output", "ai", "first answer", 8)],
            turn_input_message_id="first-range-boundary",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
        await _seed_turn_boundary(db, "second-range-boundary", 8)
        await db.save_thread_messages(
            _TID,
            [_row("second-range-output", "ai", "second answer", 8)],
            turn_input_message_id="second-range-boundary",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        detail = await conn.fetchval(
            "SELECT detail FROM completion_effects "
            "WHERE producer_kind = 'session_turn' AND producer_id = $1::uuid",
            first,
        )
        if isinstance(detail, str):
            detail = json.loads(detail)
        rows = await conn.fetch(
            "SELECT id FROM thread_messages WHERE thread_id = $1::uuid "
            "AND seq BETWEEN $2 AND $3 ORDER BY seq",
            _TID,
            int(detail["boundary_seq"]),
            int(detail["end_seq"]),
        )

    assert [str(row["id"]) for row in rows] == [
        first_input,
        _coerce_row_id("first-range-output"),
    ]


@pytest.mark.asyncio
async def test_project_destination_is_captured_only_when_attached(db):
    input_id = await _seed_turn_boundary(db, "project-effect-boundary", 11)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET project_id = $2::uuid WHERE id = $1::uuid",
            _TID,
            _PROJECT_ID,
        )
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        producer_id = await db.save_thread_messages(
            _TID,
            [_row("project-effect-output", "ai", "project fact", 11)],
            turn_input_message_id="project-effect-boundary",
            turn_number=11,
            memory_scope_kind="project",
            memory_scope_id=_PROJECT_ID,
        )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        detail = await conn.fetchval(
            "SELECT detail FROM completion_effects WHERE producer_id = $1::uuid",
            producer_id,
        )
        if isinstance(detail, str):
            detail = json.loads(detail)
        execution_id = await conn.fetchval(
            "SELECT turn_execution_id FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )
    assert str(execution_id) == producer_id
    assert detail["memory_scope_kind"] == "project"
    assert detail["memory_scope_id"] == _PROJECT_ID


@pytest.mark.asyncio
async def test_unattached_project_destination_rolls_back_transcript_and_effect(db):
    input_id = await _seed_turn_boundary(db, "unattached-effect-boundary", 12)
    output_id = _coerce_row_id("unattached-effect-output")
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        with pytest.raises(ValueError, match="not attached"):
            await db.save_thread_messages(
                _TID,
                [_row("unattached-effect-output", "ai", "must roll back", 12)],
                turn_input_message_id="unattached-effect-boundary",
                turn_number=12,
                memory_scope_kind="project",
                memory_scope_id=_PROJECT_ID,
            )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT turn_execution_id IS NULL FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM thread_messages WHERE id = $1::uuid)",
            output_id,
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM completion_effects "
                "WHERE producer_kind = 'session_turn'"
            )
            == 0
        )


@pytest.mark.asyncio
async def test_effect_retry_rejects_destination_identity_drift(db):
    await _seed_turn_boundary(db, "scope-drift-boundary", 13)
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        producer_id = await db.save_thread_messages(
            _TID,
            [_row("scope-drift-output", "ai", "same output", 13)],
            turn_input_message_id="scope-drift-boundary",
            turn_number=13,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE threads SET project_id = $2::uuid WHERE id = $1::uuid",
                _TID,
                _PROJECT_ID,
            )
        with pytest.raises(ValueError, match="conflicting identity"):
            await db.save_thread_messages(
                _TID,
                [_row("scope-drift-output", "ai", "same output", 13)],
                turn_input_message_id="scope-drift-boundary",
                turn_number=13,
                memory_scope_kind="project",
                memory_scope_id=_PROJECT_ID,
            )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        detail = await conn.fetchval(
            "SELECT detail FROM completion_effects WHERE producer_id = $1::uuid",
            producer_id,
        )
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail["memory_scope_kind"] == "thread"
    assert detail["memory_scope_id"] == _TID


@pytest.mark.asyncio
async def test_rewind_cannot_tombstone_an_unfinished_effect_source(db):
    input_id = await _seed_turn_boundary(db, "rewind-effect-boundary", 14)
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        await db.save_thread_messages(
            _TID,
            [_row("rewind-effect-output", "ai", "retain source", 14)],
            turn_input_message_id="rewind-effect-boundary",
            turn_number=14,
            memory_scope_kind="thread",
            memory_scope_id=_TID,
        )
        async with db.acquire() as conn:
            from_seq = await conn.fetchval(
                "SELECT seq FROM thread_messages WHERE id = $1::uuid",
                input_id,
            )
        with pytest.raises(RuntimeError, match="final-memory extraction"):
            await db.apply_rewind(
                _TID,
                from_seq=int(from_seq),
                mode="conversation",
                actor="test",
            )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT rewound_at IS NULL FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )


@pytest.mark.asyncio
async def test_fenced_out_reconcile_mints_neither_transcript_nor_effect(db):
    input_id = await _seed_turn_boundary(db, "stolen-effect-boundary", 9)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET lease_token = 18 WHERE unit_id = $1::uuid",
            _TID,
        )

    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        with pytest.raises(LeaseLostError):
            await db.save_thread_messages(
                _TID,
                [_row("stolen-output", "ai", "must roll back", 9)],
                turn_input_message_id="stolen-effect-boundary",
                turn_number=9,
                memory_scope_kind="thread",
                memory_scope_id=_TID,
            )
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        execution_id = await conn.fetchval(
            "SELECT turn_execution_id FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )
        effect_count = await conn.fetchval(
            "SELECT count(*) FROM completion_effects "
            "WHERE producer_kind = 'session_turn'"
        )
        output_count = await conn.fetchval(
            "SELECT count(*) FROM thread_messages WHERE id = $1::uuid",
            _coerce_row_id("stolen-output"),
        )

    assert execution_id is None
    assert effect_count == 0
    assert output_count == 0


@pytest.mark.asyncio
async def test_pinned_reconcile_keeps_transcript_only_compatibility(db):
    input_id = await _seed_turn_boundary(db, "pinned-effect-boundary", 10)
    producer_id = await db.save_thread_messages(
        _TID,
        [_row("pinned-output", "ai", "pinned answer", 10)],
        turn_input_message_id="pinned-effect-boundary",
        turn_number=10,
    )

    assert producer_id is None
    async with db.acquire() as conn:
        execution_id = await conn.fetchval(
            "SELECT turn_execution_id FROM thread_messages WHERE id = $1::uuid",
            input_id,
        )
        effect_count = await conn.fetchval(
            "SELECT count(*) FROM completion_effects "
            "WHERE producer_kind = 'session_turn'"
        )
        output = await conn.fetchval(
            "SELECT content FROM thread_messages WHERE id = $1::uuid",
            _coerce_row_id("pinned-output"),
        )

    assert execution_id is None
    assert effect_count == 0
    assert output == "pinned answer"


@pytest.mark.asyncio
async def test_stateless_message_flush_commits_under_ordered_locks(db):
    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        result = await db.save_thread_message(
            _TID,
            role="ai",
            content="ordered durable message",
            turn_number=2,
            id="ordered-message",
        )
    finally:
        current_lease.reset(context_token)

    assert result["seq"] == 1
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT content FROM thread_messages WHERE id = $1::uuid",
                uuid.UUID(_coerce_row_id("ordered-message")),
            )
            == "ordered durable message"
        )


@pytest.mark.asyncio
async def test_stateless_event_flush_commits_under_ordered_locks(db):
    import agent.api.persistent_app as persistent_app

    lease = LeaseHandle()
    lease.update(_TID, 17)
    writer = persistent_app._OrderedPersistentEventWriter(
        postgres_conn=db,
        thread_id=_TID,
        epoch=4,
        on_terminal_failure=lambda _events, _reason: None,
        lease=lease,
    )
    inserted = await writer._write_batch(
        [
            persistent_app._QueuedPersistentEvent(
                4,
                1,
                "token",
                {"content": "ordered durable event"},
            )
        ]
    )

    assert inserted == 1
    async with db.acquire() as conn:
        event_count, hwm = await conn.fetchrow(
            "SELECT (SELECT COUNT(*) FROM thread_events "
            "WHERE thread_id = $1::uuid), events_seq_hwm "
            "FROM threads WHERE id = $1::uuid",
            _TID,
        )
    assert event_count == 1
    assert hwm == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("event_mode", ["ordinary", "stateless_control"])
async def test_two_stateless_event_writers_serialize_before_hwm_update(db, event_mode):
    import agent.api.persistent_app as persistent_app

    advisory_key = 742_013_342
    function_name = "test_gate_thread_event_insert"
    trigger_name = "test_gate_thread_event_insert"
    tasks = []
    async with db.acquire() as gate_conn:
        await gate_conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION {function_name}()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock_shared({advisory_key});
                RETURN NEW;
            END
            $$
            """
        )
        await gate_conn.execute(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON thread_events "
            f"FOR EACH ROW EXECUTE FUNCTION {function_name}()"
        )
        await gate_conn.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        gate_released = False
        try:

            def start_writer(seq):
                lease = LeaseHandle()
                lease.update(_TID, 17)
                writer = persistent_app._OrderedPersistentEventWriter(
                    postgres_conn=db,
                    thread_id=_TID,
                    epoch=4,
                    on_terminal_failure=lambda _events, _reason: None,
                    lease=lease,
                )
                event_kwargs = {}
                if event_mode == "stateless_control":
                    event_kwargs = {
                        "control_request_id": _CONTROL_IDS[seq - 1],
                        "control_lease_token": 17,
                    }
                event = persistent_app._QueuedPersistentEvent(
                    4,
                    seq,
                    "token",
                    {"content": f"concurrent-{seq}"},
                    **event_kwargs,
                )
                return asyncio.create_task(writer._write_batch([event]))

            tasks.append(start_writer(1))
            # Writer 1 holds the up-front parent lock and is gated inside its
            # INSERT. Writer 2 must serialize on that same mutation-strength
            # lock, rather than reaching the HWM UPDATE with a weaker prelock.
            await _wait_for_blocked_query(db, "INSERT INTO thread_events")
            tasks.append(start_writer(2))
            await _wait_for_blocked_query(db, "FROM threads", "FOR NO KEY UPDATE")
            await gate_conn.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
            gate_released = True

            assert await asyncio.wait_for(asyncio.gather(*tasks), timeout=5) == [1, 1]
        finally:
            if not gate_released:
                await gate_conn.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await gate_conn.execute(
                f"DROP TRIGGER IF EXISTS {trigger_name} ON thread_events"
            )
            await gate_conn.execute(f"DROP FUNCTION IF EXISTS {function_name}()")

    async with db.acquire() as conn:
        event_count, hwm, seqs = await conn.fetchrow(
            "SELECT (SELECT COUNT(*) FROM thread_events "
            "WHERE thread_id = $1::uuid), events_seq_hwm, "
            "(SELECT array_agg(seq ORDER BY seq) FROM thread_events "
            "WHERE thread_id = $1::uuid) "
            "FROM threads WHERE id = $1::uuid",
            _TID,
        )
    assert event_count == 2
    assert hwm == 2
    assert seqs == [1, 2]


@pytest.mark.asyncio
async def test_terminal_end_cannot_deadlock_with_message_flush(db):
    thread_locked = asyncio.Event()
    release_queue = asyncio.Event()
    terminal = asyncio.create_task(
        _terminal_end_after_release(db, thread_locked, release_queue)
    )
    await asyncio.wait_for(thread_locked.wait(), timeout=2)

    lease = LeaseHandle()
    lease.update(_TID, 17)
    context_token = current_lease.set(lease)
    try:
        message = asyncio.create_task(
            db.save_thread_message(
                _TID,
                role="ai",
                content="must be fenced after End",
                turn_number=1,
                id="terminal-race-message",
            )
        )
        await _wait_for_blocked_query(db, "FROM threads", "FOR KEY SHARE")
        release_queue.set()
        await asyncio.wait_for(terminal, timeout=2)
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(message, timeout=2)
    finally:
        current_lease.reset(context_token)

    async with db.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM thread_messages WHERE thread_id = $1::uuid",
            _TID,
        )
    assert count == 0


@pytest.mark.asyncio
async def test_terminal_end_cannot_deadlock_with_event_flush(db):
    import agent.api.persistent_app as persistent_app

    thread_locked = asyncio.Event()
    release_queue = asyncio.Event()
    terminal = asyncio.create_task(
        _terminal_end_after_release(db, thread_locked, release_queue)
    )
    await asyncio.wait_for(thread_locked.wait(), timeout=2)

    lease = LeaseHandle()
    lease.update(_TID, 17)
    writer = persistent_app._OrderedPersistentEventWriter(
        postgres_conn=db,
        thread_id=_TID,
        epoch=4,
        on_terminal_failure=lambda _events, _reason: None,
        lease=lease,
    )
    event = asyncio.create_task(
        writer._write_batch(
            [
                persistent_app._QueuedPersistentEvent(
                    4,
                    1,
                    "token",
                    {"content": "must be fenced after End"},
                )
            ]
        )
    )
    await _wait_for_blocked_query(db, "events_epoch", "FOR NO KEY UPDATE")
    release_queue.set()
    await asyncio.wait_for(terminal, timeout=2)
    assert await asyncio.wait_for(event, timeout=2) == 0

    async with db.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM thread_events WHERE thread_id = $1::uuid",
            _TID,
        )
    assert count == 0
