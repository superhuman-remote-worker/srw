"""Real-Postgres ownership and dedup proofs for completion-sweep routing."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.services.completion_sweep_router import (
    STATELESS_OWNER_GAP_CODE,
    STATELESS_OWNER_GAP_MESSAGE,
    CompletionSweepRouter,
    _ClaimedAction,
)
from orchestrator.database.postgres import PostgresDB
from shared.worker_queue import claim_worker_batch


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def pg(pg_dsn, _schema_applied):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8, timeout=10)
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE job_completion_sweep_actions, completion_effects, "
            "completion_finalizer_leases, job_completion_commands, "
            "run_queue, jobs, agents CASCADE"
        )
    try:
        yield pool
    finally:
        await pool.close()


async def _job(pg) -> UUID:
    async with pg.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('completion router test', 'processing', 'pinned') "
            "RETURNING id"
        )


async def _stateless_owner_gap_job(
    pg,
    *,
    queue_state: str | None = "done",
    status: str = "processing",
    lane: str = "stateless",
    assigned_agent_id: UUID | None = None,
    lease_seconds: float | None = None,
    context: dict | None = None,
) -> UUID:
    async with pg.acquire() as conn:
        job_id = await conn.fetchval(
            """
            INSERT INTO jobs (
                description, status, execution_lane, assigned_agent_id,
                lease_expires_at, context
            ) VALUES (
                'stateless owner-gap test', $1, $2, $3,
                CASE WHEN $4::float8 IS NULL THEN NULL
                     ELSE now()+make_interval(secs => $4::float8) END,
                $5::jsonb
            )
            RETURNING id
            """,
            status,
            lane,
            assigned_agent_id,
            lease_seconds,
            json.dumps(context or {}),
        )
        if queue_state is not None:
            await conn.execute(
                """
                INSERT INTO run_queue (
                    unit_id, unit_kind, state, lease_token, leased_by,
                    leased_until, input_seq, consumed_seq,
                    attempts_since_completion, last_leased_by
                ) VALUES (
                    $1, 'worker_batch', $2, 7,
                    CASE WHEN $2='leased' THEN 'owner-gap-worker' END,
                    CASE WHEN $2='leased' THEN now()+interval '5 minutes' END,
                    3, 3, 2, 'prior-owner-gap-worker'
                )
                """,
                job_id,
                queue_state,
            )
        return job_id


async def _settled_owner_gap_command(pg, job_id: UUID) -> UUID:
    command_id = await _command(pg, job_id)
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE job_completion_commands
            SET state='done', attempts=1,
                outcome='{\"new_status\":\"processing\"}'::jsonb,
                finalized_at=now(), finalizing_by=NULL,
                lease_expires_at=NULL
            WHERE id=$1
            """,
            command_id,
        )
        await conn.execute(
            """
            INSERT INTO completion_effects (
                producer_kind, producer_id, scope_id, effect_name,
                effect_group, state, attempts, completed_at, detail
            ) VALUES (
                'job_completion', $1, $2, 'test_settled_effect',
                'notification', 'done', 1, now(),
                '{\"settled\":true}'::jsonb
            )
            """,
            command_id,
            job_id,
        )
        await conn.execute(
            "UPDATE jobs SET completion_sweep_attempt_hwm=1 WHERE id=$1",
            job_id,
        )
        # Match the preserved specimen exactly: its already-done historical
        # action records command_attempt=0 while the settled command is now at
        # attempts=1.  The rescue must preserve both rows byte-for-byte.
        await conn.execute(
            """
            INSERT INTO job_completion_sweep_actions (
                job_id, attempt, command_id, command_attempt, route,
                source, state, claimed_at, result, completed_at
            ) VALUES (
                $1, 1, $2, 0, 'resume_finalizer',
                'settled-owner-gap-fixture', 'done', now(),
                '{\"route\":\"resume_finalizer\"}'::jsonb, now()
            )
            """,
            job_id,
            command_id,
        )
    return command_id


async def _command(
    pg,
    job_id: UUID,
    *,
    state: str = "pending",
    attempts: int = 0,
    max_attempts: int = 5,
    lease_expires_at: datetime | None = None,
    deadline_at: datetime | None = None,
) -> UUID:
    error_code = "operator_required" if state == "parked" else None
    async with pg.acquire() as conn:
        return await conn.fetchval(
            """
            INSERT INTO job_completion_commands (
                job_id, report_seq, client_report_id, payload, payload_digest,
                origin, requested_by, state, attempts, max_attempts,
                run_after, lease_expires_at, deadline_at, code_version,
                error_code
            ) VALUES (
                $1, 1, $2, '{}'::jsonb, 'digest', 'operator',
                'completion-router-test', $3, $4, $5, now(), $6, $7,
                'test-version', $8
            )
            RETURNING id
            """,
            job_id,
            uuid4(),
            state,
            attempts,
            max_attempts,
            lease_expires_at,
            deadline_at or datetime.now(UTC) + timedelta(hours=1),
            error_code,
        )


def _db_from_pool(pool) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        async with pool.acquire() as conn:
            yield conn

    db.acquire = acquire
    return db


def _finalizer_result(command_id: str):
    return SimpleNamespace(
        command_id=command_id,
        state="done",
        disposition="done",
        outcome={"ignored": "x" * 20_000},
        error_code=None,
    )


@pytest.mark.asyncio
async def test_resume_bypass_rolls_back_and_claim_skips_blocked_fifo_head(pg):
    """The accepted-202 catastrophe dies at both mutation and claim layers."""

    blocked_job = uuid4()
    eligible_job = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, description, status, execution_lane)
            VALUES ($1, 'blocked', 'paused', 'stateless'),
                   ($2, 'eligible', 'paused', 'stateless')
            """,
            blocked_job,
            eligible_job,
        )
        await conn.execute(
            """
            INSERT INTO run_queue (
                unit_id, unit_kind, state, priority, queued_at, run_after
            ) VALUES
                ($1, 'worker_batch', 'done', 100, now() - interval '2 minutes', now()),
                ($2, 'worker_batch', 'queued', 0, now() - interval '1 minute', now())
            """,
            blocked_job,
            eligible_job,
        )
    await _command(pg, blocked_job, state="pending")

    db = _db_from_pool(pg)
    assert not await db.queue_stateless_job_for_resume(
        str(blocked_job),
        {"queued_feedback": "must not start round two"},
        expected_status="paused",
        completion_commands_enabled=True,
    )
    async with pg.acquire() as conn:
        blocked_after_resume = await conn.fetchrow(
            "SELECT state, lease_token FROM run_queue WHERE unit_id=$1",
            blocked_job,
        )
        blocked_status = await conn.fetchval(
            "SELECT status::text FROM jobs WHERE id=$1", blocked_job
        )
    assert blocked_after_resume["state"] == "done"
    assert blocked_after_resume["lease_token"] == 0
    assert blocked_status == "paused"

    # Bypass the public verb exactly as the catastrophe trace does.  The
    # worker-side predicate is an independent safety layer: a command-blocked
    # high-priority head must remain byte-for-byte queued while the claimant
    # skips it and leases the next eligible unit.
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET state='queued' WHERE unit_id=$1",
            blocked_job,
        )
        blocked_before_claim = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, leased_by, "
            "leased_until FROM run_queue WHERE unit_id=$1",
            blocked_job,
        )
    assert blocked_before_claim["state"] == "queued"

    claim = await claim_worker_batch(
        pg,
        pod_name="m2-claim-proof",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert claim is not None
    assert claim.unit_id == eligible_job

    async with pg.acquire() as conn:
        blocked_after_claim = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, leased_by, "
            "leased_until FROM run_queue WHERE unit_id=$1",
            blocked_job,
        )
    assert dict(blocked_after_claim) == dict(blocked_before_claim)


@pytest.mark.asyncio
async def test_accept_holding_job_lock_wins_against_concurrent_pinned_resume(pg):
    """The post-lock command read observes an accept that commits while waiting."""

    job_id = await _job(pg)
    db = _db_from_pool(pg)
    accept_conn = await pg.acquire()
    transaction = accept_conn.transaction()
    await transaction.start()
    committed = False
    resume_task = None
    try:
        await accept_conn.fetchval("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        resume_task = asyncio.create_task(
            db.queue_job_for_resume(
                str(job_id),
                {"queued_feedback": "round two"},
                expected_status="processing",
                completion_commands_enabled=True,
            )
        )
        await asyncio.sleep(0.05)
        assert not resume_task.done()
        await accept_conn.execute(
            """
            INSERT INTO job_completion_commands (
                job_id, report_seq, client_report_id, payload, payload_digest,
                origin, requested_by, state, run_after, deadline_at, code_version
            ) VALUES (
                $1, 1, $2, '{}'::jsonb, 'digest', 'operator', 'race-test',
                'pending', now(), now() + interval '1 hour', 'test-version'
            )
            """,
            job_id,
            uuid4(),
        )
        await transaction.commit()
        committed = True
        assert not await asyncio.wait_for(resume_task, timeout=2)
    finally:
        if not committed:
            await transaction.rollback()
        if resume_task is not None and not resume_task.done():
            resume_task.cancel()
            await asyncio.gather(resume_task, return_exceptions=True)
        await pg.release(accept_conn)

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text AS status, context FROM jobs WHERE id=$1", job_id
        )
    assert row["status"] == "processing"
    context = row["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert "queued_feedback" not in (context or {})


@pytest.mark.asyncio
async def test_pinned_resume_holding_job_lock_closes_late_accept_fence(pg):
    job_id = await _job(pg)
    resume_conn = await pg.acquire()
    transaction = resume_conn.transaction()
    await transaction.start()
    committed = False
    accept_task = None

    async def _late_accept() -> bool:
        async with pg.acquire() as conn:
            async with conn.transaction():
                status = await conn.fetchval(
                    "SELECT status::text FROM jobs WHERE id=$1 FOR UPDATE", job_id
                )
                if status != "processing":
                    return False
                await conn.execute(
                    """
                    INSERT INTO job_completion_commands (
                        job_id, report_seq, client_report_id, payload,
                        payload_digest, origin, requested_by, state,
                        run_after, deadline_at, code_version
                    ) VALUES (
                        $1, 1, $2, '{}'::jsonb, 'digest', 'operator',
                        'late-accept', 'pending', now(),
                        now() + interval '1 hour', 'test-version'
                    )
                    """,
                    job_id,
                    uuid4(),
                )
                return True

    try:
        await resume_conn.fetchval("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        accept_task = asyncio.create_task(_late_accept())
        await asyncio.sleep(0.05)
        assert not accept_task.done()
        await resume_conn.execute(
            "UPDATE jobs SET status='paused' WHERE id=$1 AND status='processing'",
            job_id,
        )
        await transaction.commit()
        committed = True
        assert not await asyncio.wait_for(accept_task, timeout=2)
    finally:
        if not committed:
            await transaction.rollback()
        if accept_task is not None and not accept_task.done():
            accept_task.cancel()
            await asyncio.gather(accept_task, return_exceptions=True)
        await pg.release(resume_conn)

    async with pg.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM job_completion_commands WHERE job_id=$1)",
            job_id,
        )


class _BlockingFinalizer:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[tuple[str, bool]] = []

    async def finalize_command(self, command_id: str, *, inline: bool):
        self.calls.append((command_id, inline))
        self.started.set()
        await self.release.wait()
        return _finalizer_result(command_id)


@pytest.mark.asyncio
async def test_missing_command_is_legacy_and_live_finalizer_stands_down(pg):
    legacy_job = await _job(pg)
    live_job = await _job(pg)
    await _command(
        pg,
        live_job,
        state="finalizing",
        attempts=1,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    finalizer = SimpleNamespace(finalize_command=AsyncMock())
    router = CompletionSweepRouter(pg, finalizer, claimant_id="route-proof")

    legacy = await router.route_job(legacy_job, source="orphan")
    stand_down = await router.route_job(live_job, source="expired-job-lease")

    assert legacy.disposition == "legacy"
    assert legacy.legacy
    assert stand_down.disposition == "stand_down"
    assert stand_down.route == "stand_down"
    finalizer.finalize_command.assert_not_awaited()
    async with pg.acquire() as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM job_completion_sweep_actions")
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT completion_sweep_attempt_hwm FROM jobs WHERE id=$1",
                live_job,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_stateless_owner_gap_two_routers_park_once_without_protocol_mutation(pg):
    """The preserved-specimen shape becomes inert operator-owned work once."""

    job_id = await _stateless_owner_gap_job(
        pg,
        context={
            # Project-loop jobs normally must never drift into pending_review.
            # A zero-owner terminal-queue residue is the exceptional case: an
            # operator must decide its already-executed turn, never an inferred
            # auto-complete or a re-executing enqueue.
            "loop_id": str(uuid4()),
            "loop_iteration": 4,
        },
    )
    command_id = await _settled_owner_gap_command(pg, job_id)
    async with pg.acquire() as conn:
        queue_before = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        )
        command_before = dict(
            await conn.fetchrow(
                "SELECT * FROM job_completion_commands WHERE id=$1", command_id
            )
        )
        effect_before = dict(
            await conn.fetchrow(
                "SELECT * FROM completion_effects WHERE producer_id=$1", command_id
            )
        )
        action_before = dict(
            await conn.fetchrow(
                "SELECT * FROM job_completion_sweep_actions WHERE job_id=$1", job_id
            )
        )

    alerts: list[str] = []
    finalizer = SimpleNamespace(finalize_command=AsyncMock())
    routers = [
        CompletionSweepRouter(
            pg,
            finalizer,
            alerts.append,
            claimant_id=f"owner-gap-router-{suffix}",
        )
        for suffix in ("a", "b")
    ]

    results = await asyncio.gather(
        *(router.park_stateless_owner_gaps_once() for router in routers)
    )

    parked = [result for batch in results for result in batch]
    assert [(result.job_id, result.queue_state) for result in parked] == [
        (str(job_id), "done")
    ]
    assert len(alerts) == 1
    assert str(job_id) in alerts[0]
    assert f"code={STATELESS_OWNER_GAP_CODE}" in alerts[0]
    finalizer.finalize_command.assert_not_awaited()

    async with pg.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT status::text AS status, lease_expires_at, error_message, "
            "error_details, context FROM jobs WHERE id=$1",
            job_id,
        )
        queue_after = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        )
        command_after = dict(
            await conn.fetchrow(
                "SELECT * FROM job_completion_commands WHERE id=$1", command_id
            )
        )
        effect_after = dict(
            await conn.fetchrow(
                "SELECT * FROM completion_effects WHERE producer_id=$1", command_id
            )
        )
        action_after = dict(
            await conn.fetchrow(
                "SELECT * FROM job_completion_sweep_actions WHERE job_id=$1", job_id
            )
        )
    error_details = job["error_details"]
    if isinstance(error_details, str):
        error_details = json.loads(error_details)
    context = job["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert job["status"] == "pending_review"
    assert job["lease_expires_at"] is None
    assert job["error_message"] == STATELESS_OWNER_GAP_MESSAGE
    assert error_details == {
        "code": STATELESS_OWNER_GAP_CODE,
        "route": "park_alert",
        "queue_state": "done",
    }
    assert context["loop_iteration"] == 4
    assert queue_after == queue_before
    assert command_after == command_before
    assert effect_after == effect_before
    assert action_after == action_before


@pytest.mark.asyncio
async def test_stateless_owner_gap_absent_parity_and_fail_closed_exclusions(pg):
    absent = await _stateless_owner_gap_job(pg, queue_state=None)
    expired_lease = await _stateless_owner_gap_job(pg, lease_seconds=-1)
    excluded: set[UUID] = set()
    for queue_state in ("queued", "leased", "parked"):
        excluded.add(await _stateless_owner_gap_job(pg, queue_state=queue_state))
    excluded.add(
        await _stateless_owner_gap_job(pg, status="paused", queue_state="done")
    )
    excluded.add(
        await _stateless_owner_gap_job(pg, status="completed", queue_state="done")
    )
    excluded.add(await _stateless_owner_gap_job(pg, lane="pinned", queue_state="done"))
    wrong_kind = await _stateless_owner_gap_job(pg)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET unit_kind='session_turn' WHERE unit_id=$1",
            wrong_kind,
        )
    excluded.add(wrong_kind)
    excluded.add(await _stateless_owner_gap_job(pg, lease_seconds=600))

    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('developer', $1, 'working') RETURNING id",
            f"owner-gap-agent-{uuid4()}",
        )
    excluded.add(await _stateless_owner_gap_job(pg, assigned_agent_id=agent_id))
    excluded.add(
        await _stateless_owner_gap_job(
            pg,
            context={
                "_completion_control_claim": {
                    "version": 1,
                    "claim_id": str(uuid4()),
                    "expires_epoch": 4_000_000_000,
                }
            },
        )
    )
    excluded.add(
        await _stateless_owner_gap_job(
            pg,
            context={"_completion_control_claim": "malformed-fails-closed"},
        )
    )
    for command_state in ("pending", "finalizing", "parked"):
        job_id = await _stateless_owner_gap_job(pg)
        await _command(pg, job_id, state=command_state)
        excluded.add(job_id)

    alerts: list[str] = []
    finalizer = SimpleNamespace(finalize_command=AsyncMock())
    router = CompletionSweepRouter(pg, finalizer, alerts.append)

    parked = await router.park_stateless_owner_gaps_once(limit=100)

    assert {(UUID(result.job_id), result.queue_state) for result in parked} == {
        (absent, "absent"),
        (expired_lease, "done"),
    }
    assert len(alerts) == 2
    finalizer.finalize_command.assert_not_awaited()
    async with pg.acquire() as conn:
        parked_rows = await conn.fetch(
            "SELECT id, status::text AS status, lease_expires_at, error_details "
            "FROM jobs WHERE id=ANY($1::uuid[]) ORDER BY id",
            [absent, expired_lease],
        )
        excluded_statuses = await conn.fetch(
            "SELECT id, status::text AS status FROM jobs WHERE id=ANY($1::uuid[])",
            list(excluded),
        )
    assert all(row["status"] == "pending_review" for row in parked_rows)
    assert all(row["lease_expires_at"] is None for row in parked_rows)
    assert {row["status"] for row in excluded_statuses} == {
        "processing",
        "paused",
        "completed",
    }


@pytest.mark.asyncio
async def test_stateless_owner_gap_loses_to_concurrent_queue_reactivation(pg):
    job_id = await _stateless_owner_gap_job(pg)
    alerts: list[str] = []
    router = CompletionSweepRouter(
        pg,
        SimpleNamespace(finalize_command=AsyncMock()),
        alerts.append,
    )
    blocker = await pg.acquire()
    transaction = blocker.transaction()
    await transaction.start()
    committed = False
    rescue_task = None
    try:
        await blocker.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
        )
        rescue_task = asyncio.create_task(router.park_stateless_owner_gaps_once())
        await asyncio.sleep(0.05)
        assert not rescue_task.done()
        await blocker.execute(
            "UPDATE run_queue SET state='queued', run_after=now() WHERE unit_id=$1",
            job_id,
        )
        await transaction.commit()
        committed = True
        assert await asyncio.wait_for(rescue_task, timeout=2) == ()
    finally:
        if not committed:
            await transaction.rollback()
        if rescue_task is not None and not rescue_task.done():
            rescue_task.cancel()
            await asyncio.gather(rescue_task, return_exceptions=True)
        await pg.release(blocker)

    async with pg.acquire() as conn:
        job_status = await conn.fetchval(
            "SELECT status::text FROM jobs WHERE id=$1", job_id
        )
        queue_state = await conn.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", job_id
        )
    assert job_status == "processing"
    assert queue_state == "queued"
    assert alerts == []


@pytest.mark.asyncio
async def test_concurrent_routes_share_one_action_and_heartbeat_keeps_term_live(pg):
    import time

    job_id = await _job(pg)
    command_id = await _command(pg, job_id)
    finalizer = _BlockingFinalizer()
    # A live-term success proof needs room for real database and scheduling
    # delays; expiry remains authoritative and has separate loss controls.
    first_router = CompletionSweepRouter(
        pg,
        finalizer,
        claimant_id="router-a",
        action_lease_seconds=3,
    )
    second_router = CompletionSweepRouter(
        pg,
        finalizer,
        claimant_id="router-b",
        action_lease_seconds=3,
    )

    first_task = asyncio.create_task(
        first_router.route_job(job_id, source="stale-agent")
    )
    await asyncio.wait_for(finalizer.started.wait(), timeout=2)
    async with pg.acquire() as conn:
        first_claim = await conn.fetchrow(
            "SELECT attempt, claimed_by, claim_expires_at "
            "FROM job_completion_sweep_actions WHERE job_id=$1",
            job_id,
        )
    initial_expiry = first_claim["claim_expires_at"]
    first_owner = str(first_claim["claimed_by"])
    assert first_owner.startswith("router-a:")

    stale_action = _ClaimedAction(
        job_id=str(job_id),
        attempt=int(first_claim["attempt"]),
        command_id=str(command_id),
        command_attempt=0,
        route="resume_finalizer",
        claimed_by=f"{first_owner}-stale",
    )
    assert not await first_router._renew(stale_action)

    renewal_deadline = asyncio.get_running_loop().time() + 10
    renewed_expiry = initial_expiry
    while renewed_expiry <= initial_expiry + timedelta(milliseconds=50):
        assert asyncio.get_running_loop().time() < renewal_deadline
        await asyncio.sleep(0.03)
        async with pg.acquire() as conn:
            renewed_expiry = await conn.fetchval(
                "SELECT claim_expires_at FROM job_completion_sweep_actions "
                "WHERE job_id=$1",
                job_id,
            )

    await asyncio.sleep(
        max(0.0, (initial_expiry - datetime.now(UTC)).total_seconds()) + 0.03
    )
    competing = await second_router.route_job(job_id, source="orphan")
    assert competing.disposition == "busy"
    assert finalizer.calls == [(str(command_id), False)]

    # Deliberately stall the event loop after the native busy response. This
    # exceeded the old 300ms lease and correctly caused claim_lost under load.
    time.sleep(0.35)
    finalizer.release.set()
    completed = await asyncio.wait_for(first_task, timeout=2)
    assert completed.disposition == "completed"
    replay = await second_router.route_job(job_id, source="orphan")
    assert replay.disposition == "already_done"

    async with pg.acquire() as conn:
        durable = await conn.fetchrow(
            "SELECT state, source, claimed_by, result "
            "FROM job_completion_sweep_actions WHERE job_id=$1",
            job_id,
        )
        action_count = await conn.fetchval(
            "SELECT count(*) FROM job_completion_sweep_actions WHERE job_id=$1",
            job_id,
        )
        hwm = await conn.fetchval(
            "SELECT completion_sweep_attempt_hwm FROM jobs WHERE id=$1", job_id
        )
    result = durable["result"]
    if isinstance(result, str):
        result = json.loads(result)
    assert durable["state"] == "done"
    assert durable["source"] == "stale-agent"
    assert durable["claimed_by"] is None
    assert result == {
        "finalizer": {
            "command_id": str(command_id),
            "disposition": "done",
            "state": "done",
        },
        "route": "resume_finalizer",
    }
    assert action_count == 1
    assert hwm == 1


@pytest.mark.asyncio
async def test_exception_claim_is_taken_over_without_allocating_another_action(pg):
    job_id = await _job(pg)
    command_id = await _command(pg, job_id)
    failing = SimpleNamespace(
        finalize_command=AsyncMock(side_effect=RuntimeError("finalizer unavailable"))
    )
    first = CompletionSweepRouter(
        pg, failing, claimant_id="failed-router", action_lease_seconds=30
    )
    with pytest.raises(RuntimeError, match="finalizer unavailable"):
        await first.route_job(job_id, source="orphan")

    async with pg.acquire() as conn:
        abandoned_owner = await conn.fetchval(
            "SELECT claimed_by FROM job_completion_sweep_actions WHERE job_id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE job_completion_sweep_actions "
            "SET claimed_at=now()-interval '2 seconds', "
            "claim_expires_at=now()-interval '1 second' WHERE job_id=$1",
            job_id,
        )

    succeeding = SimpleNamespace(
        finalize_command=AsyncMock(return_value=_finalizer_result(str(command_id)))
    )
    second = CompletionSweepRouter(pg, succeeding, claimant_id="takeover-router")
    takeover = await second.route_job(job_id, source="pause-redispatch")

    assert takeover.disposition == "completed"
    succeeding.finalize_command.assert_awaited_once_with(str(command_id), inline=False)
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempt, state, claimed_by FROM job_completion_sweep_actions "
            "WHERE job_id=$1",
            job_id,
        )
        count = await conn.fetchval(
            "SELECT count(*) FROM job_completion_sweep_actions WHERE job_id=$1",
            job_id,
        )
        hwm = await conn.fetchval(
            "SELECT completion_sweep_attempt_hwm FROM jobs WHERE id=$1", job_id
        )
    assert str(abandoned_owner).startswith("failed-router:")
    assert dict(row) == {"attempt": 1, "state": "done", "claimed_by": None}
    assert count == 1
    assert hwm == 1


@pytest.mark.asyncio
async def test_alert_only_never_finalizes_and_park_alert_finalizes_before_alert(pg):
    parked_job = await _job(pg)
    parked_command = await _command(pg, parked_job, state="parked")
    capped_job = await _job(pg)
    capped_command = await _command(
        pg,
        capped_job,
        state="finalizing",
        attempts=5,
        max_attempts=5,
        lease_expires_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    events: list[str] = []

    async def finalize(command_id: str, *, inline: bool):
        assert not inline
        events.append(f"finalize:{command_id}")
        return _finalizer_result(command_id)

    async def alert(message: str):
        events.append(f"alert:{message.rsplit('=', 1)[-1]}")

    finalizer = SimpleNamespace(finalize_command=AsyncMock(side_effect=finalize))
    router = CompletionSweepRouter(pg, finalizer, alert, claimant_id="alert-router")

    parked_result = await router.route_job(parked_job, source="stale-agent")
    assert parked_result.disposition == "completed"
    assert parked_result.route == "alert_only"
    finalizer.finalize_command.assert_not_awaited()
    assert events == ["alert:alert_only"]

    capped_result = await router.route_job(capped_job, source="job-lease")
    assert capped_result.disposition == "completed"
    assert capped_result.route == "park_alert"
    assert events == [
        "alert:alert_only",
        f"finalize:{capped_command}",
        "alert:park_alert",
    ]
    assert parked_result.command_id == str(parked_command)


@pytest.mark.asyncio
async def test_deadline_race_promotes_same_action_to_park_alert(pg):
    job_id = await _job(pg)
    command_id = await _command(pg, job_id)
    alerts: list[str] = []
    finalizer = SimpleNamespace(
        finalize_command=AsyncMock(
            return_value=SimpleNamespace(
                command_id=str(command_id),
                state="parked",
                disposition="busy",
                error_code="deadline_or_attempts_exhausted",
            )
        )
    )
    router = CompletionSweepRouter(
        pg,
        finalizer,
        alerts.append,
        claimant_id="deadline-race-router",
    )

    result = await router.route_job(job_id, source="expired-finalizer")

    assert result.disposition == "completed"
    assert result.route == "park_alert"
    assert len(alerts) == 1
    async with pg.acquire() as conn:
        action = await conn.fetchrow(
            "SELECT route, state, result FROM job_completion_sweep_actions "
            "WHERE job_id=$1",
            job_id,
        )
        hwm = await conn.fetchval(
            "SELECT completion_sweep_attempt_hwm FROM jobs WHERE id=$1", job_id
        )
    durable_result = action["result"]
    if isinstance(durable_result, str):
        durable_result = json.loads(durable_result)
    assert action["route"] == "park_alert"
    assert action["state"] == "done"
    assert durable_result["route"] == "park_alert"
    assert durable_result["alerted"] is True
    assert hwm == 1


@pytest.mark.asyncio
async def test_route_once_skips_live_and_already_completed_actions(pg):
    actionable_jobs = [await _job(pg), await _job(pg)]
    command_ids = [await _command(pg, job_id) for job_id in actionable_jobs]
    live_job = await _job(pg)
    await _command(
        pg,
        live_job,
        state="finalizing",
        attempts=1,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    grace_job = await _job(pg)
    grace_command = await _command(pg, grace_job)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands "
            "SET run_after=now()+interval '5 minutes' WHERE id=$1",
            grace_command,
        )
    finalizer = SimpleNamespace(
        finalize_command=AsyncMock(
            side_effect=lambda command_id, inline: _finalizer_result(command_id)
        )
    )
    router = CompletionSweepRouter(pg, finalizer, claimant_id="batch-router")

    first = await router.route_once(limit=10)
    second = await router.route_once(limit=10)

    assert first.count == 2
    assert {result.job_id for result in first.results} == {
        str(job_id) for job_id in actionable_jobs
    }
    assert all(result.disposition == "completed" for result in first.results)
    assert second.count == 0
    assert second.results == ()
    assert {call.args[0] for call in finalizer.finalize_command.await_args_list} == {
        str(command_id) for command_id in command_ids
    }
    assert str(grace_command) not in {
        call.args[0] for call in finalizer.finalize_command.await_args_list
    }
