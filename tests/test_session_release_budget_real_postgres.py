"""Real-Postgres proofs: a voluntary session release honours the retry budget.

knowledge-base/knowledge/issues/voluntary_session_release_bypasses_retry_budget.md

Every claim counts an attempt, but only the expired-lease reaper compared the
count with ``max_attempts`` — and a voluntarily released row is never an
expired lease. A deterministic pre-effect failure (``bundle_409``) was
therefore claimed and released forever (observed at 16 attempts, budget 5)
while Cockpit generated indefinitely.

These tests drive the executor's real release paths against the real queue
SQL and prove the exhausted disposition: a DETERMINISTIC failure parks at the
row's OWN ``max_attempts``, and the queue state, the epoch bump, the settled
claimant authority and the ``turn.parked`` frame commit as one fact — a failed
journal write leaves the lease untouched instead of splitting them. TRANSIENT
failures (an orchestrator deploy blackout) never park and back off
exponentially, and neither they nor shutdown hand-backs spend the budget.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from agent.api import turn_executor as te
from shared.run_queue import (
    PARK_REASON_ATTACH_FAILED,
    PARK_REASON_RETRY_EXHAUSTED,
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    enqueue_unit,
    record_input_seq,
    release_unit,
)

POD = "stateless-pod-a"
POD_UID = "11111111-2222-3333-4444-555555555555"


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def pool(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            CREATE TABLE threads (
                id uuid PRIMARY KEY,
                execution_lane text NOT NULL DEFAULT 'stateless',
                agent_id uuid,
                status text NOT NULL DEFAULT 'active',
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                events_epoch integer NOT NULL DEFAULT 3,
                events_seq_hwm bigint NOT NULL DEFAULT 0
            );
            CREATE TABLE vm_workspace_recovery_jobs (
                job_id uuid NOT NULL,
                resolved_at timestamptz
            );
            -- Column-for-column the live run_queue (schema_current.sql).
            CREATE TABLE run_queue (
                unit_id uuid PRIMARY KEY,
                unit_kind text NOT NULL,
                dedup_key text,
                state text NOT NULL DEFAULT 'queued',
                priority integer NOT NULL DEFAULT 0,
                fair_key text,
                run_after timestamptz NOT NULL DEFAULT now(),
                attempts_since_completion integer NOT NULL DEFAULT 0,
                max_attempts integer NOT NULL DEFAULT 5,
                lease_token bigint NOT NULL DEFAULT 0,
                leased_by text,
                leased_until timestamptz,
                input_seq bigint,
                consumed_seq bigint,
                queued_at timestamptz NOT NULL DEFAULT now(),
                enqueue_ord bigserial NOT NULL,
                last_leased_by text,
                control_input_seq bigint NOT NULL DEFAULT 0,
                control_consumed_seq bigint NOT NULL DEFAULT 0,
                interrupt_admission_lease_token bigint,
                interrupt_admission_turn_id integer,
                input_delivery_capable_lease_token bigint,
                park_reason text,
                parked_at timestamptz,
                last_error text,
                last_error_signature text,
                attach_failures integer NOT NULL DEFAULT 0
            );
            CREATE TABLE thread_permission_requests (
                id uuid PRIMARY KEY,
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                tool_call_id text NOT NULL,
                status text NOT NULL DEFAULT 'pending',
                requested_at timestamptz NOT NULL DEFAULT now(),
                decided_at timestamptz,
                decided_by text,
                accepted_lease_token bigint,
                UNIQUE (id, thread_id),
                CHECK (accepted_lease_token IS NULL OR accepted_lease_token > 0)
            );
            CREATE TABLE thread_events (
                thread_id uuid NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
                epoch integer NOT NULL,
                seq bigint NOT NULL,
                kind text NOT NULL,
                payload jsonb NOT NULL,
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

    created = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4, timeout=10)
    try:
        yield created
    finally:
        await created.close()


async def _seed(pool, *, max_attempts: int = 5) -> UUID:
    thread_id = uuid4()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO threads (id) VALUES ($1)", thread_id)
        await enqueue_unit(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=1
        )
        await conn.execute(
            "UPDATE run_queue SET max_attempts = $2 WHERE unit_id = $1",
            thread_id,
            max_attempts,
        )
    return thread_id


async def _claim(pool, thread_id: UUID, *, pod: str = POD):
    """Claim the unit and stamp its credential-bound active claimant, as the
    claim bundle does before a pre-effect failure can release it."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET run_after = now() WHERE unit_id = $1", thread_id
        )
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=pod,
            affinity_grace_seconds=0.0,
        )
        if claim is None:
            return None
        await conn.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{_stateless_active_claim}', $2::jsonb, true) WHERE id = $1",
            thread_id,
            json.dumps(
                {"lease_token": claim.lease_token, "pod": pod, "pod_uid": POD_UID}
            ),
        )
        return claim


async def _queue(pool, thread_id: UUID):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id = $1", thread_id
        )


async def _thread(pool, thread_id: UUID):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM threads WHERE id = $1", thread_id)


async def _backoff(pool, thread_id: UUID) -> float:
    """Seconds between the release and the next claimable instant."""
    async with pool.acquire() as conn:
        return float(
            await conn.fetchval(
                "SELECT EXTRACT(EPOCH FROM run_after - queued_at) FROM run_queue "
                "WHERE unit_id = $1",
                thread_id,
            )
        )


async def _frames(pool, thread_id: UUID):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT epoch, seq, kind, payload FROM thread_events "
            "WHERE thread_id = $1 ORDER BY epoch, seq",
            thread_id,
        )
    return [
        {
            "epoch": row["epoch"],
            "seq": row["seq"],
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
        }
        for row in rows
    ]


@pytest.fixture
def executor(monkeypatch, pool):
    ex = te.StatelessTurnExecutor(pod_name=POD, pod_uid=POD_UID)
    monkeypatch.setattr(te.StatelessTurnExecutor, "_db", property(lambda self: pool))
    monkeypatch.setattr(te, "_pa", lambda: MagicMock())
    # The local drain is exercised by test_turn_executor; here the claimant is
    # already quiescent and only the durable disposition is under test.
    monkeypatch.setattr(ex, "_quiesce_claim_before_transition", AsyncMock())
    monkeypatch.setattr(ex, "_ack_terminal_claim_loss", AsyncMock())
    monkeypatch.setattr(ex, "_clear_claim_tool_effect", MagicMock())
    return ex


@pytest.mark.asyncio
async def test_release_loop_parks_at_the_rows_own_budget(executor, pool):
    thread_id = await _seed(pool, max_attempts=3)
    epoch_before = (await _thread(pool, thread_id))["events_epoch"]

    claims = []
    delays = []
    for _ in range(8):
        claim = await _claim(pool, thread_id)
        if claim is None:
            break
        claims.append(claim)
        await executor._release(claim, reason="bundle_409")
        delays.append(round(await _backoff(pool, thread_id)))

    # Three claims (a non-default budget), then nothing more to claim; the
    # budgeted retries are spaced on the attach-failure schedule.
    assert [claim.attempts_since_completion for claim in claims] == [1, 2, 3]
    assert delays[:2] == [5, 15]
    row = await _queue(pool, thread_id)
    assert row["state"] == "parked"
    assert row["park_reason"] == PARK_REASON_RETRY_EXHAUSTED
    assert row["last_error"] == "bundle_409"
    assert row["lease_token"] == claims[-1].lease_token
    assert row["leased_by"] is None and row["last_leased_by"] is None
    assert row["attempts_since_completion"] == 3

    # The visible outcome is in the same commit: one epoch edge, one frame.
    thread = await _thread(pool, thread_id)
    assert thread["events_epoch"] == epoch_before + 1
    assert "_stateless_active_claim" not in json.loads(thread["metadata"])
    frames = await _frames(pool, thread_id)
    assert [frame["kind"] for frame in frames] == ["turn.parked"]
    assert frames[0]["epoch"] == epoch_before + 1
    assert frames[0]["payload"] == {
        "reason": PARK_REASON_RETRY_EXHAUSTED,
        "release_reason": "bundle_409",
        "attempts": 3,
        "failures": 3,
        "max_attempts": 3,
        "retryable": True,
        "parked_by": POD,
        "lease_token": claims[-1].lease_token,
    }

    # No successor may claim it, and new input does not unpark the poison unit.
    assert await _claim(pool, thread_id, pod="stateless-pod-b") is None
    async with pool.acquire() as conn:
        state = await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=9
        )
    assert state == "parked"
    assert await _claim(pool, thread_id, pod="stateless-pod-b") is None


@pytest.mark.asyncio
async def test_deterministic_release_under_budget_requeues_without_a_frame(
    executor, pool
):
    thread_id = await _seed(pool, max_attempts=5)
    epoch_before = (await _thread(pool, thread_id))["events_epoch"]

    claim = await _claim(pool, thread_id)
    await executor._release(claim, reason="loop_died")

    row = await _queue(pool, thread_id)
    assert row["state"] == "queued"
    assert row["park_reason"] is None
    assert row["last_error"] == "loop_died"
    assert row["attach_failures"] == 1
    # Linear error backoff (5 s x failures) for a deterministic retry.
    assert round(await _backoff(pool, thread_id)) == 5
    assert (await _thread(pool, thread_id))["events_epoch"] == epoch_before
    assert await _frames(pool, thread_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["bundle_503", "bundle_error"])
async def test_orchestrator_outage_never_parks_and_backs_off(executor, pool, reason):
    """A deploy blackout (single-replica orchestrator, Recreate) fails every
    claim bundle with a 5xx or a connection error for a minute or more. The
    old unbounded release healed by itself afterwards; a bounded one would
    have parked every such session for a manual Retry."""
    thread_id = await _seed(pool, max_attempts=5)
    epoch_before = (await _thread(pool, thread_id))["events_epoch"]

    delays = []
    for _ in range(8):  # 5+10+20+40+60+60+60 s of backoff: well past 90 s
        claim = await _claim(pool, thread_id)
        assert claim is not None
        await executor._release(claim, reason=reason)
        row = await _queue(pool, thread_id)
        assert row["state"] == "queued" and row["park_reason"] is None
        delays.append(round(await _backoff(pool, thread_id)))

    assert delays == [5, 10, 20, 40, 60, 60, 60, 60]
    row = await _queue(pool, thread_id)
    assert row["attempts_since_completion"] == 8  # every claim still counted
    assert row["attach_failures"] == 0  # none of them against the budget
    assert (await _thread(pool, thread_id))["events_epoch"] == epoch_before
    assert await _frames(pool, thread_id) == []

    # The orchestrator is back: the unit is claimed again, and a genuinely
    # deterministic failure now starts its own budget from one.
    recovered = await _claim(pool, thread_id, pod="stateless-pod-b")
    assert recovered is not None
    await executor._release(recovered, reason="bundle_409")
    row = await _queue(pool, thread_id)
    assert row["state"] == "queued" and row["attach_failures"] == 1


@pytest.mark.asyncio
async def test_hand_backs_and_transients_neither_consume_nor_reset_the_budget(
    executor, pool
):
    thread_id = await _seed(pool, max_attempts=3)

    async def hand_back(**kwargs):
        claim = await _claim(pool, thread_id)
        async with pool.acquire() as conn:
            await release_unit(
                conn, unit_id=thread_id, lease_token=claim.lease_token, **kwargs
            )

    for step in (
        "bundle_409",
        "stop_boundary",  # claimed on the stop boundary: plain, no backoff
        "bundle_error",
        "bundle_409",
        "shutdown_cancelled",  # the shutdown path's plain error release
        "serve_crash",
    ):
        if step == "stop_boundary":
            await hand_back(backoff_seconds=0.0)
        elif step == "shutdown_cancelled":
            await hand_back(error=True)
        else:
            await executor._release(await _claim(pool, thread_id), reason=step)
        assert (await _queue(pool, thread_id))["state"] == "queued", step

    row = await _queue(pool, thread_id)
    assert row["attempts_since_completion"] == 6
    assert row["attach_failures"] == 2

    await executor._release(await _claim(pool, thread_id), reason="bundle_409")
    row = await _queue(pool, thread_id)
    assert row["state"] == "parked" and row["attach_failures"] == 3
    payload = (await _frames(pool, thread_id))[-1]["payload"]
    assert payload["failures"] == 3 and payload["attempts"] == 7


@pytest.mark.asyncio
async def test_failed_journal_write_leaves_the_lease_unsplit(
    executor, pool, monkeypatch
):
    thread_id = await _seed(pool, max_attempts=1)
    epoch_before = (await _thread(pool, thread_id))["events_epoch"]
    claim = await _claim(pool, thread_id)
    monkeypatch.setattr(
        te, "append_system_frame", AsyncMock(side_effect=RuntimeError("journal down"))
    )

    await executor._release(claim, reason="bundle_409")

    # Park, epoch bump, claimant settlement and frame are one commit: the
    # rolled-back park leaves the exact lease for the reaper, which journals
    # its own bounded park — never a silent parked spinner.
    row = await _queue(pool, thread_id)
    assert row["state"] == "leased"
    assert row["lease_token"] == claim.lease_token
    assert row["park_reason"] is None
    thread = await _thread(pool, thread_id)
    assert thread["events_epoch"] == epoch_before
    assert "_stateless_active_claim" in json.loads(thread["metadata"])
    assert await _frames(pool, thread_id) == []


@pytest.mark.asyncio
async def test_attach_failure_park_is_atomic_with_its_frame(
    executor, pool, monkeypatch
):
    thread_id = await _seed(pool, max_attempts=2)
    first = await _claim(pool, thread_id)
    await executor._release_attach_failure(first, RuntimeError("first shape"))
    assert (await _queue(pool, thread_id))["state"] == "queued"
    assert await _frames(pool, thread_id) == []

    second = await _claim(pool, thread_id)
    real_append = te.append_system_frame
    journal_down = True

    async def append(*args, **kwargs):
        if journal_down:
            raise RuntimeError("journal down")
        return await real_append(*args, **kwargs)

    monkeypatch.setattr(te, "append_system_frame", append)
    await executor._release_attach_failure(second, RuntimeError("second shape"))

    # The budget park (2 of 2, distinct signatures) rolled back with its frame.
    row = await _queue(pool, thread_id)
    assert row["state"] == "leased" and row["lease_token"] == second.lease_token
    assert row["attach_failures"] == 1  # the failure record rolled back too
    assert await _frames(pool, thread_id) == []

    journal_down = False
    await executor._release_attach_failure(second, RuntimeError("second shape"))
    row = await _queue(pool, thread_id)
    assert row["state"] == "parked"
    assert row["park_reason"] == PARK_REASON_ATTACH_FAILED
    frames = await _frames(pool, thread_id)
    assert [frame["kind"] for frame in frames] == ["turn.parked"]
    assert frames[0]["payload"]["reason"] == PARK_REASON_ATTACH_FAILED
    assert frames[0]["payload"]["attempts"] == 2
    assert frames[0]["payload"]["max_attempts"] == 2
    assert frames[0]["payload"]["error"] == "second shape"


@pytest.mark.asyncio
async def test_exhausted_park_expires_the_dead_turns_permission_prompt(executor, pool):
    thread_id = await _seed(pool, max_attempts=1)
    claim = await _claim(pool, thread_id)
    exact, older, approved = uuid4(), uuid4(), uuid4()
    async with pool.acquire() as conn:
        # The loop died while a tool awaited approval under this very token.
        for request_id, token, status in (
            (exact, claim.lease_token, "pending"),
            (older, None, "pending"),  # legacy unbound row, same thread
            (approved, claim.lease_token, "approved"),
        ):
            await conn.execute(
                "INSERT INTO thread_permission_requests "
                "(id, thread_id, tool_call_id, status, accepted_lease_token) "
                "VALUES ($1, $2, $3, $4, $5)",
                request_id,
                thread_id,
                f"call-{request_id}",
                status,
                token,
            )

    await executor._release(claim, reason="loop_died")

    async with pool.acquire() as conn:
        statuses = {
            row["id"]: row["status"]
            for row in await conn.fetch(
                "SELECT id, status FROM thread_permission_requests "
                "WHERE thread_id = $1",
                thread_id,
            )
        }
    assert statuses == {exact: "expired", older: "expired", approved: "approved"}
    frames = await _frames(pool, thread_id)
    # Receipts and the park share the one new epoch, the park last.
    assert len({frame["epoch"] for frame in frames}) == 1
    assert [frame["kind"] for frame in frames] == [
        "permission.resolved",
        "permission.resolved",
        "turn.parked",
    ]
    assert {frame["payload"]["approval_id"] for frame in frames[:2]} == {
        str(exact),
        str(older),
    }


@pytest.mark.asyncio
async def test_lost_response_replays_without_a_second_frame(pool):
    thread_id = await _seed(pool, max_attempts=1)
    claim = await _claim(pool, thread_id)

    async def release(conn):
        return await release_unit(
            conn,
            unit_id=claim.unit_id,
            lease_token=claim.lease_token,
            error=True,
            park_reason=PARK_REASON_RETRY_EXHAUSTED,
            last_error="bundle_409",
        )

    kwargs = dict(
        cas=release,
        pod_name=POD,
        pod_uid=POD_UID,
        park_reason=PARK_REASON_RETRY_EXHAUSTED,
        release_reason="bundle_409",
    )
    first = await te._settle_session_release(pool, claim, **kwargs)
    # The commit landed but its response was lost: the retry must recognise
    # its own disposition, not journal it twice nor report a fenced-out loss.
    second = await te._settle_session_release(pool, claim, **kwargs)

    assert first.state == "parked" and first.journaled and not first.replayed
    assert second.state == "parked" and second.replayed and not second.journaled
    assert [frame["kind"] for frame in await _frames(pool, thread_id)] == [
        "turn.parked"
    ]


@pytest.mark.asyncio
async def test_a_successor_generation_is_fenced_not_replayed(pool):
    thread_id = await _seed(pool, max_attempts=5)
    stale = await _claim(pool, thread_id)
    async with pool.acquire() as conn:
        await release_unit(conn, unit_id=thread_id, lease_token=stale.lease_token)
    successor = await _claim(pool, thread_id, pod="stateless-pod-b")
    assert successor.lease_token == stale.lease_token + 1

    async def release(conn):
        return await release_unit(
            conn,
            unit_id=stale.unit_id,
            lease_token=stale.lease_token,
            error=True,
            park_reason=PARK_REASON_RETRY_EXHAUSTED,
        )

    outcome = await te._settle_session_release(
        pool,
        stale,
        cas=release,
        pod_name=POD,
        pod_uid=POD_UID,
        park_reason=PARK_REASON_RETRY_EXHAUSTED,
        release_reason="bundle_409",
    )

    assert outcome is None
    row = await _queue(pool, thread_id)
    assert row["state"] == "leased" and row["lease_token"] == successor.lease_token
    # The successor's credential-bound authority is not the stale caller's.
    active = json.loads((await _thread(pool, thread_id))["metadata"])[
        "_stateless_active_claim"
    ]
    assert active["lease_token"] == successor.lease_token


@pytest.mark.asyncio
async def test_foreign_active_claim_is_left_for_the_operator(executor, pool):
    thread_id = await _seed(pool, max_attempts=1)
    claim = await _claim(pool, thread_id)
    foreign = {"lease_token": claim.lease_token, "pod": "other", "pod_uid": "x"}
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{_stateless_active_claim}', $2::jsonb) WHERE id = $1",
            thread_id,
            json.dumps(foreign),
        )

    await executor._release(claim, reason="bundle_409")

    # The exact queue lease is this claimant's to park; an authority record it
    # cannot prove is its own is never cleared on its say-so.
    assert (await _queue(pool, thread_id))["state"] == "parked"
    metadata = json.loads((await _thread(pool, thread_id))["metadata"])
    assert metadata["_stateless_active_claim"] == foreign
    assert [frame["kind"] for frame in await _frames(pool, thread_id)] == [
        "turn.parked"
    ]
