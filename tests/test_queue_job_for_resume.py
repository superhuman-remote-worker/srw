"""Test the resume re-queue write (``queue_job_for_resume``).

Locks the dispatcher-visibility contract for the HUMAN-reply resume paths:
``_internal_resume_job`` (reply to a blocking agent message, urgent interrupt,
LLM-triage interrupt) and ``resume_job``'s ``_queue_for_dispatch`` (explicit
Resume when no agent is free). Both flip a frozen job to 'paused' — and
``get_dispatchable_jobs`` requires ``freeze_data IS NULL``, so keeping the
blob left the job paused-but-invisible: the dispatcher never selected it
again and nothing else was scheduled to change its state.

For the blocking-message arm the freeze is a PRECONDITION (the reply router
only resumes when ``freeze_data.thread_id`` matches), so every reply to a
blocking message wedged its job. Mirrors tests/test_delegation_resume_claim.py.
See knowledge-base/knowledge/issues/blocking_message_reply_keeps_freeze_data.md.
"""

import asyncio
import json
from uuid import UUID

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import CompletionDecisionBlocked, PostgresDB
from shared.subagent_parent_authority import (
    ParentExecutionAuthority,
    ParentExecutionAuthorityRefused,
)

JOB = "9b760af1-a693-4652-a50c-aca46542ab48"
TARGET = "7c1275d8-2f9f-4a2b-9b58-6a1f6f6b2f10"
AGENT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
POD_UID = "pod-completion-barrier"
PROCESS_GENERATION = "process-completion-barrier"

JOB_COMPLETE_FREEZE = {
    "job_id": TARGET,
    "summary": "done, ready for review",
    "freeze_type": "job_complete",
}

BLOCKING_FREEZE = {
    "job_id": JOB,
    "status": "waiting_for_reply",
    "subject": "BLOCKED: managed shell tab reset required",
    "thread_id": "d78144",
    "timestamp": "2026-07-18T23:24:06.901206+00:00",
    "freeze_type": "blocking_message",
}


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def db(pg_dsn):
    d = PostgresDB(connection_string=pg_dsn)
    await d.connect()
    async with d.acquire() as conn:
        # Minimal jobs table, but with every column get_dispatchable_jobs
        # touches so the dispatcher-visibility contract can be asserted here.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS srw_execution_specs (
                work_kind text, work_id uuid, harness_adapter text
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id uuid PRIMARY KEY,
                description text,
                status text NOT NULL,
                config_name text,
                config_override jsonb,
                assigned_agent_id uuid,
                user_id uuid,
                project_id uuid,
                parent_job_id uuid,
                priority int DEFAULT 5,
                runner_kind text,
                execution_lane text NOT NULL DEFAULT 'pinned',
                branch_name text,
                repo_name text,
                context jsonb,
                freeze_data jsonb,
                workspace_idle_revision bigint NOT NULL DEFAULT 0,
                workspace_idle_episode jsonb,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
            """
        )
        # ``set_completion_decision`` checks durable child state after locking
        # the parent. These minimal sibling tables let this module exercise the
        # real PostgreSQL lock/snapshot behavior without running migrations.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agents (
                id uuid PRIMARY KEY,
                status text NOT NULL,
                current_job_id uuid,
                pod_uid text,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS threads (
                id uuid PRIMARY KEY,
                kind text NOT NULL,
                user_id uuid,
                project_id uuid,
                title text,
                status text NOT NULL,
                permission_mode text,
                narration_mode text,
                execution_lane text,
                metadata jsonb,
                parent_job_id uuid,
                parent_thread_id uuid,
                parent_tool_call_id text,
                subagent_handle text,
                subagent_type text,
                subagent_status text,
                subagent_outcome text,
                subagent_error text,
                report_path text,
                total_turns integer NOT NULL DEFAULT 0,
                total_tokens bigint NOT NULL DEFAULT 0,
                ended_at timestamptz,
                last_activity timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
                runtime_generation uuid NOT NULL DEFAULT gen_random_uuid()
            )
            """
        )
        # The resume CAS refuses a pinned Job while an open idle operation
        # (migration 0270) owns it. Only the fence columns are modelled.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vm_idle_operations (
                owner_kind text NOT NULL,
                owner_id uuid NOT NULL,
                closed_at timestamptz
            )
            """
        )
        await conn.execute("TRUNCATE threads, agents, jobs, vm_idle_operations")
        await conn.execute(
            """
            INSERT INTO jobs (id, status, assigned_agent_id, context, freeze_data)
            VALUES ($1, 'waiting_for_reply', $2, $3::jsonb, $4::jsonb)
            """,
            UUID(JOB),
            UUID(AGENT),
            json.dumps({"loop_id": "b15dc68f", "loop_iteration": 3}),
            json.dumps(BLOCKING_FREEZE),
        )
    yield d
    await d.close()


@pytest.mark.asyncio
async def test_reply_resume_clears_freeze_so_dispatcher_sees_job(db):
    """The re-queue must clear freeze_data, or the reply resumes nothing."""
    assert await db.queue_job_for_resume(JOB, {"queued_feedback": "fixed, resume"})

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, assigned_agent_id, freeze_data, context "
            "FROM jobs WHERE id = $1",
            UUID(JOB),
        )
    assert row["status"] == "paused"
    assert row["assigned_agent_id"] is None  # agent cleared for re-dispatch
    assert row["freeze_data"] is None

    dispatchable = await db.get_dispatchable_jobs()
    assert [str(j["id"]) for j in dispatchable] == [JOB], (
        "a job resumed by a human reply must satisfy the dispatcher's "
        "freeze_data IS NULL contract"
    )


@pytest.mark.asyncio
async def test_reply_resume_merges_feedback_and_keeps_existing_context(db):
    """Feedback lands without clobbering sibling keys (loop bookkeeping)."""
    assert await db.queue_job_for_resume(JOB, {"queued_feedback": "fixed, resume"})

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert ctx["queued_feedback"] == "fixed, resume"
    assert ctx["loop_id"] == "b15dc68f"
    assert ctx["loop_iteration"] == 3


@pytest.mark.asyncio
async def test_reply_resume_stashes_freeze_for_observability(db):
    """The shed freeze survives in context — the row-level blob must not."""
    assert await db.queue_job_for_resume(JOB, {"queued_feedback": "fixed, resume"})

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert ctx["last_freeze_data"]["freeze_type"] == "blocking_message"
    assert ctx["last_freeze_data"]["thread_id"] == "d78144"


@pytest.mark.asyncio
async def test_resume_without_feedback_still_sheds_freeze(db):
    """``resume_job``'s no-feedback arm must clear the freeze too."""
    assert await db.queue_job_for_resume(JOB)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, freeze_data FROM jobs WHERE id = $1", UUID(JOB)
        )
    assert row["status"] == "paused"
    assert row["freeze_data"] is None
    assert len(await db.get_dispatchable_jobs()) == 1


@pytest.mark.asyncio
async def test_generic_resume_refuses_redispatch_trip_without_shedding_fences(db):
    """Automation cannot turn the circuit's diagnostic freeze into false success."""
    circuit = {
        "version": 1,
        "generation": "incident-1",
        "state": "tripped",
        "unchanged_recoveries": 3,
    }
    freeze = {
        "freeze_type": "redispatch_livelock",
        "automatic_redispatch": False,
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='paused', context=context || $2::jsonb, "
            "freeze_data=$3::jsonb WHERE id=$1",
            UUID(JOB),
            json.dumps({"_lease_recovery": circuit}),
            json.dumps(freeze),
        )

    assert not await db.queue_job_for_resume(
        JOB, {"queued_feedback": "automation tried to resume"}
    )

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, context, freeze_data FROM jobs WHERE id=$1", UUID(JOB)
        )
    context = json.loads(row["context"])
    assert row["status"] == "paused"
    assert context["_lease_recovery"] == circuit
    assert "queued_feedback" not in context
    assert json.loads(row["freeze_data"]) == freeze


@pytest.mark.asyncio
async def test_critic_returned_resume_clears_freeze_so_dispatcher_sees_job(db):
    """The critic-'returned' arm resumes the reviewing parent through this
    same write. The parent always arrives frozen (``job_complete`` freeze from
    its own completion), so keeping the blob would park it paused-but-invisible
    after every review round-trip on non-full autonomy.
    See knowledge-history/done/critic_feedback_resume_parent_freeze_data_wedge.md.
    """
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, status, assigned_agent_id, context, freeze_data)
            VALUES ($1, 'reviewing', $2, $3::jsonb, $4::jsonb)
            """,
            UUID(TARGET),
            UUID(AGENT),
            json.dumps(
                {
                    "verification_rounds": [
                        {"round": 1, "critic_job_id": "c-1", "verdict": "returned"}
                    ]
                }
            ),
            json.dumps(JOB_COMPLETE_FREEZE),
        )

    assert await db.queue_job_for_resume(
        TARGET,
        {
            "queued_feedback": "## Open findings\n- **F1** (high): missing tests",
            "queued_feedback_reason": (
                "The critic reviewed the completed work and returned it with "
                "open findings; address them."
            ),
        },
    )

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, assigned_agent_id, freeze_data FROM jobs WHERE id = $1",
            UUID(TARGET),
        )
        ctx = await db_context(conn, TARGET)
    assert row["status"] == "paused"
    assert row["assigned_agent_id"] is None
    assert row["freeze_data"] is None

    # The seeded blocking-message job is 'waiting_for_reply', so the returned
    # parent must be the one dispatchable row.
    dispatchable = await db.get_dispatchable_jobs()
    assert [str(j["id"]) for j in dispatchable] == [TARGET], (
        "a parent resumed by a critic 'returned' verdict must satisfy the "
        "dispatcher's freeze_data IS NULL contract"
    )

    # The review ledger must survive the context merge, and the shed freeze
    # stays observable.
    assert ctx["verification_rounds"][0]["critic_job_id"] == "c-1"
    assert ctx["last_freeze_data"]["freeze_type"] == "job_complete"


@pytest.mark.asyncio
async def test_unknown_job_is_a_no_op(db):
    """Caller distinguishes 'not found' from a real re-queue (warning path)."""
    assert (
        await db.queue_job_for_resume("11111111-1111-1111-1111-111111111111") is False
    )
    assert await db.queue_job_for_resume("not-a-uuid") is False


DECISION = {
    "tool_call_id": "call-round1",
    "summary": "Round 1 delivered.",
    "deliverables": ["output/report.md"],
    "confidence": 0.9,
    "recorded_at": "2026-08-06T20:00:00+00:00",
    "job_id": JOB,
}


@pytest.mark.asyncio
async def test_feedback_resume_voids_completion_decision(db):
    """A resume that demands NEW work must drop the journaled decision — the
    round-2 agent's hydration would otherwise re-seed round 1's decision and
    finalize the new round with the old report.
    See knowledge-base/knowledge/issues/job_finalization_decisions_held_only_in_process_memory.md.
    """
    assert await db.set_completion_decision(JOB, DECISION)

    assert await db.queue_job_for_resume(JOB, {"queued_feedback": "do more"})

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert "completion_decision" not in ctx
    # Sibling keys and the feedback merge must survive the subtraction.
    assert ctx["queued_feedback"] == "do more"
    assert ctx["loop_id"] == "b15dc68f"


@pytest.mark.asyncio
async def test_provisioning_requeue_preserves_completion_decision(db):
    """The missing-workspace requeue is a pure re-park, not a new round —
    losing the decision there is the exact defect the journal exists to fix."""
    assert await db.set_completion_decision(JOB, DECISION)

    assert await db.queue_job_for_resume(JOB, void_completion_decision=False)

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert ctx["completion_decision"]["tool_call_id"] == "call-round1"
    assert ctx["completion_decision"]["summary"] == "Round 1 delivered."


@pytest.mark.asyncio
async def test_set_completion_decision_round_trips(db):
    assert await db.set_completion_decision(JOB, DECISION)

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert ctx["completion_decision"] == DECISION
    # Sibling context keys untouched (atomic top-level merge).
    assert ctx["loop_id"] == "b15dc68f"


@pytest.mark.asyncio
async def test_set_completion_decision_refuses_terminal_job(db):
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'completed' WHERE id = $1", UUID(JOB)
        )

    assert await db.set_completion_decision(JOB, DECISION) is False

    async with db.acquire() as conn:
        ctx = await db_context(conn, JOB)
    assert "completion_decision" not in ctx, (
        "a stale agent must not rewrite the journal of a terminal job"
    )


@pytest.mark.asyncio
async def test_completion_waits_for_child_create_winner_then_sees_live_row(db):
    """A child transaction that owns the parent lock cannot be hidden by the
    completion statement's pre-wait snapshot.

    This is the race that requires a *second* statement after ``FOR UPDATE``:
    the child row does not exist when completion starts, but must block the
    decision once the winning create commits.
    """

    child_id = UUID("11111111-2222-4333-8444-555555555555")
    locked = asyncio.Event()
    release = asyncio.Event()

    async def child_create_winner() -> None:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchval(
                    "SELECT id FROM jobs WHERE id=$1::uuid FOR UPDATE", UUID(JOB)
                )
                await conn.execute(
                    """
                    INSERT INTO threads (
                        id, kind, status, parent_job_id, subagent_status
                    ) VALUES ($1, 'subagent', 'created', $2, 'queued')
                    """,
                    child_id,
                    UUID(JOB),
                )
                locked.set()
                await release.wait()

    writer = asyncio.create_task(child_create_winner())
    await locked.wait()
    decision = asyncio.create_task(db.set_completion_decision(JOB, DECISION))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(decision), timeout=0.1)
    finally:
        release.set()
    await writer

    with pytest.raises(CompletionDecisionBlocked) as excinfo:
        await decision
    assert excinfo.value.reason == "live_subagents"
    assert excinfo.value.live_subagents == 1
    assert excinfo.value.queued_subagent_replies == 0
    async with db.acquire() as conn:
        assert "completion_decision" not in await db_context(conn, JOB)


@pytest.mark.asyncio
async def test_completion_waits_for_terminal_winner_then_sees_queued_report(db):
    """Terminalizing a child does not open a completion gap: its atomically
    queued report remains a blocker after the transaction releases the parent.
    """

    child_id = UUID("22222222-3333-4444-8555-666666666666")
    reply = {
        "id": "33333333-4444-4555-8666-777777777777",
        "source": "subagent",
        "thread_id": str(child_id),
        "message": "independent verification finished",
    }
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (
                id, kind, status, parent_job_id, subagent_status
            ) VALUES ($1, 'subagent', 'active', $2, 'running')
            """,
            child_id,
            UUID(JOB),
        )

    locked = asyncio.Event()
    release = asyncio.Event()

    async def child_terminal_winner() -> None:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchval(
                    "SELECT id FROM jobs WHERE id=$1::uuid FOR UPDATE", UUID(JOB)
                )
                await conn.execute(
                    """
                    UPDATE threads
                       SET status='ended', subagent_status='completed'
                     WHERE id=$1
                    """,
                    child_id,
                )
                await conn.execute(
                    """
                    UPDATE jobs
                       SET context=jsonb_set(
                           COALESCE(context, '{}'::jsonb),
                           '{queued_replies}',
                           $2::jsonb
                       )
                     WHERE id=$1
                    """,
                    UUID(JOB),
                    json.dumps([reply]),
                )
                locked.set()
                await release.wait()

    writer = asyncio.create_task(child_terminal_winner())
    await locked.wait()
    decision = asyncio.create_task(db.set_completion_decision(JOB, DECISION))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(decision), timeout=0.1)
    finally:
        release.set()
    await writer

    with pytest.raises(CompletionDecisionBlocked) as excinfo:
        await decision
    assert excinfo.value.reason == "queued_subagent_replies"
    assert excinfo.value.live_subagents == 0
    assert excinfo.value.queued_subagent_replies == 1
    async with db.acquire() as conn:
        assert "completion_decision" not in await db_context(conn, JOB)


@pytest.mark.asyncio
async def test_committed_completion_decision_closes_child_create_admission(db):
    """The opposite lock winner is safe too: after completion commits, the
    production child-create funnel may not insert a new row under that parent.
    """

    authority = ParentExecutionAuthority(
        execution_lane="pinned",
        parent_job_id=UUID(JOB),
        agent_id=UUID(AGENT),
        pod_uid=POD_UID,
        dispatch_process_generation=PROCESS_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
               SET status='processing', execution_lane='pinned',
                   assigned_agent_id=$2
             WHERE id=$1
            """,
            UUID(JOB),
            UUID(AGENT),
        )
        await conn.execute(
            """
            INSERT INTO agents (
                id, status, current_job_id, pod_uid, metadata
            ) VALUES (
                $1, 'working', $2, $3,
                jsonb_build_object('dispatch_process_generation', $4::text)
            )
            """,
            UUID(AGENT),
            UUID(JOB),
            POD_UID,
            PROCESS_GENERATION,
        )

    assert await db.set_completion_decision(JOB, DECISION)
    child_id = UUID("44444444-5555-4666-8777-888888888888")
    with pytest.raises(ParentExecutionAuthorityRefused) as excinfo:
        await db.create_subagent_thread(
            parent_job_id=JOB,
            parent_authority=authority,
            thread_id=str(child_id),
            handle="verifier-1234",
            subagent_type="verifier",
            run_in_background=True,
            initial_status="queued",
        )
    assert excinfo.value.reason == "completion_decision_recorded"
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM threads WHERE id=$1)", child_id
        )


@pytest.mark.asyncio
async def test_committed_completion_decision_closes_child_reopen_admission(db):
    """An ended child cannot be revived behind an already-closed barrier."""

    authority = ParentExecutionAuthority(
        execution_lane="pinned",
        parent_job_id=UUID(JOB),
        agent_id=UUID(AGENT),
        pod_uid=POD_UID,
        dispatch_process_generation=PROCESS_GENERATION,
    )
    child_id = UUID("55555555-6666-4777-8888-999999999999")
    generation = UUID("66666666-7777-4888-8999-aaaaaaaaaaaa")
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
               SET status='processing', execution_lane='pinned',
                   assigned_agent_id=$2
             WHERE id=$1
            """,
            UUID(JOB),
            UUID(AGENT),
        )
        await conn.execute(
            """
            INSERT INTO agents (
                id, status, current_job_id, pod_uid, metadata
            ) VALUES (
                $1, 'working', $2, $3,
                jsonb_build_object('dispatch_process_generation', $4::text)
            )
            """,
            UUID(AGENT),
            UUID(JOB),
            POD_UID,
            PROCESS_GENERATION,
        )
        await conn.execute(
            """
            INSERT INTO threads (
                id, kind, status, parent_job_id, subagent_status,
                runtime_generation
            ) VALUES ($1, 'subagent', 'ended', $2, 'completed', $3)
            """,
            child_id,
            UUID(JOB),
            generation,
        )

    assert await db.set_completion_decision(JOB, DECISION)
    with pytest.raises(ParentExecutionAuthorityRefused) as excinfo:
        await db.reopen_subagent_thread(
            parent_job_id=JOB,
            thread_id=str(child_id),
            runtime_generation=str(generation),
            parent_authority=authority,
        )
    assert excinfo.value.reason == "completion_decision_recorded"
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, runtime_generation FROM threads WHERE id=$1", child_id
        )
    assert row["status"] == "ended"
    assert row["runtime_generation"] == generation


async def db_context(conn, job_id: str) -> dict:
    """Read a job's context as a dict (asyncpg returns JSONB as a str)."""
    raw = await conn.fetchval("SELECT context FROM jobs WHERE id = $1", UUID(job_id))
    return json.loads(raw) if isinstance(raw, str) else (raw or {})
