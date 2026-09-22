"""Real-PostgreSQL proofs for the operator pause boundary (pinned lane).

Incident (job 65e8729d, 2026-09-20): ``PUT /api/jobs/{id}/pause`` returned
200 + ``paused`` and the dispatcher resumed the job on another agent 4.6 s
later with no operator resume. A public pause parks the row as ``paused`` +
unassigned + freeze-free, which is exactly the shape
``get_dispatchable_jobs`` selects; once the pause's control claim was released
nothing distinguished it from a preemption or agent-release pause.

These tests drive the real pause operation and store SQL against the full
schema snapshot and read the dispatcher's own candidate query and claim CAS.
Every assertion is on durable jobs-row state, so a fresh ``PostgresDB``
(an orchestrator restart) sees the same boundary.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.completion_control import CompletionControl
from orchestrator.services.completion_runtime import CompletionControlBoundary
from orchestrator.services.job_mutation_controls import (
    JobControlDependencies,
    JobControlOperations,
)
from orchestrator.services.operator_pause_hold import (
    OPERATOR_PAUSE_HOLD_CONTEXT_KEY,
    operator_pause_lift_token,
)

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    database = PostgresDB(connection_string=pg_dsn)
    await database.connect()
    async with database.acquire() as conn:
        await conn.execute(
            "TRUNCATE completion_effects, job_completion_commands, "
            "run_queue, jobs, agents CASCADE"
        )
    try:
        yield database
    finally:
        await database.close()


class _AgentClient:
    """The agent's ``/job/pause`` answer: a positive, quiescent stop."""

    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.posts: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, url, json=None):
        self.posts.append(url)
        return SimpleNamespace(status_code=self.status_code)


def _operations(db: PostgresDB, *, commands_enabled: bool) -> JobControlOperations:
    runtime = SimpleNamespace(
        dependencies=SimpleNamespace(
            commands_enabled=lambda: commands_enabled,
            logger=logging.getLogger(__name__),
        ),
        control=lambda: CompletionControl(db, AsyncMock()),
    )
    target = SimpleNamespace(
        agent={"pod_ip": "10.0.0.9", "pod_port": 8080},
        recipient=SimpleNamespace(model_dump=lambda **_kwargs: {}),
    )
    client = _AgentClient()
    return JobControlOperations(
        JobControlDependencies(
            store=db,
            logger=logging.getLogger(__name__),
            completion_commands_enabled=lambda: commands_enabled,
            completion_control=CompletionControlBoundary(runtime),
            manifest_cancel=AsyncMock(return_value=True),
            prepare_pinned_mutation_target=AsyncMock(return_value=target),
            archive_and_cleanup_workspace=AsyncMock(),
            http_client_factory=lambda **_kwargs: client,
            handle_scholar_completion=AsyncMock(),
            maybe_wake_session=AsyncMock(),
            kick_session_wake_drain=MagicMock(),
            trigger_dispatch=MagicMock(),
            resolve_job_notifications=AsyncMock(),
            snapshot_service=SimpleNamespace(is_available=False),
            gitea_client=SimpleNamespace(is_initialized=False),
            revoke_and_delete_managed_repository=AsyncMock(return_value=True),
            vector_db=None,
        )
    )


def _guard(commands_enabled: bool) -> dict[str, bool]:
    return {"completion_commands_enabled": True} if commands_enabled else {}


async def _agent(db: PostgresDB) -> str:
    async with db.acquire() as conn:
        return str(
            await conn.fetchval(
                "INSERT INTO agents (config_name, hostname, status) "
                "VALUES ('developer', $1, 'ready') RETURNING id",
                f"pause-hold-{uuid4().hex[:10]}",
            )
        )


async def _dispatched_job(db: PostgresDB) -> tuple[str, str]:
    """A pinned job the dispatcher already claimed for a registered agent."""
    agent_id = await _agent(db)
    async with db.acquire() as conn:
        job_id = str(
            await conn.fetchval(
                "INSERT INTO jobs (description, status, execution_lane) "
                "VALUES ('operator pause', 'paused', 'pinned') RETURNING id"
            )
        )
    assert await db.claim_job_for_agent(job_id, agent_id)
    return job_id, agent_id


async def _row(db: PostgresDB, job_id: str) -> dict:
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text AS status, assigned_agent_id, context "
            "FROM jobs WHERE id=$1::uuid",
            job_id,
        )
    context = row["context"]
    if isinstance(context, str):
        context = json.loads(context)
    return {**dict(row), "context": context or {}}


async def _dispatchable(db: PostgresDB, job_id: str, *, commands_enabled: bool):
    jobs = await db.get_dispatchable_jobs(limit=50, **_guard(commands_enabled))
    return job_id in {str(job["id"]) for job in jobs}


async def _public_pause(db: PostgresDB, job_id: str, *, commands_enabled: bool):
    return await _operations(db, commands_enabled=commands_enabled).pause(
        job_id, job=await db.get_job(job_id)
    )


@pytest.mark.parametrize("commands_enabled", [True, False])
@pytest.mark.asyncio
async def test_public_pause_is_not_redispatched(db, commands_enabled):
    """The incident: a 200 pause must not be the dispatcher's next candidate."""
    job_id, _agent_id = await _dispatched_job(db)

    result = await _public_pause(db, job_id, commands_enabled=commands_enabled)

    assert result == {"status": "paused", "job_id": job_id}
    row = await _row(db, job_id)
    assert row["status"] == "paused"
    assert row["assigned_agent_id"] is None
    assert not await _dispatchable(db, job_id, commands_enabled=commands_enabled)
    # A dispatcher pass that selected the row before the pause landed still
    # cannot claim it for any ready agent.
    assert not await db.claim_job_for_agent(
        job_id, await _agent(db), **_guard(commands_enabled)
    )
    assert (await _row(db, job_id))["status"] == "paused"


@pytest.mark.asyncio
async def test_hold_survives_an_orchestrator_restart(db, pg_dsn):
    job_id, _agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=True)

    restarted = PostgresDB(connection_string=pg_dsn)
    await restarted.connect()
    try:
        assert not await _dispatchable(restarted, job_id, commands_enabled=True)
        assert not await restarted.claim_job_for_agent(
            job_id, await _agent(restarted), completion_commands_enabled=True
        )
    finally:
        await restarted.close()


@pytest.mark.parametrize(
    "context_merge",
    [
        # Urgent reply / steer with no live run, a deliverable-gate bounce of
        # a completion that arrived after the pause, a critic verdict: all
        # reach the jobs row through the internal resume write.
        {"queued_feedback": "urgent steer", "queued_feedback_reason": "urgent"},
        {"queued_feedback": "missing deliverable: report.md"},
    ],
)
@pytest.mark.asyncio
async def test_internal_resume_writes_queue_behind_the_hold(db, context_merge):
    job_id, _agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=True)

    assert await db.queue_job_for_resume(
        job_id,
        context_merge,
        expected_status="paused",
        completion_commands_enabled=True,
    )

    row = await _row(db, job_id)
    assert row["status"] == "paused"
    assert row["context"]["queued_feedback"] == context_merge["queued_feedback"]
    assert OPERATOR_PAUSE_HOLD_CONTEXT_KEY in row["context"]
    assert not await _dispatchable(db, job_id, commands_enabled=True)


@pytest.mark.parametrize("commands_enabled", [True, False])
@pytest.mark.asyncio
async def test_explicit_resume_lifts_the_hold_for_the_same_agent_path(
    db, commands_enabled
):
    job_id, agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=commands_enabled)
    token = operator_pause_lift_token(await db.get_job(job_id))
    assert token

    assert await db.queue_job_for_resume(
        job_id,
        {"queued_feedback": "focused feedback"},
        expected_status="paused",
        lift_operator_pause_hold=token,
        **_guard(commands_enabled),
    )

    row = await _row(db, job_id)
    assert OPERATOR_PAUSE_HOLD_CONTEXT_KEY not in row["context"]
    assert row["context"]["last_operator_pause_hold"]["hold_id"] == token
    assert row["context"]["queued_feedback"] == "focused feedback"
    assert await _dispatchable(db, job_id, commands_enabled=commands_enabled)
    # The workspace context is untouched, so the dispatcher's resume lane
    # reattaches the same VM/branch; the original agent may take it again.
    assert await db.claim_job_for_agent(job_id, agent_id, **_guard(commands_enabled))


@pytest.mark.asyncio
async def test_direct_claim_lift_is_atomic_with_the_assignment(db):
    """Flag-off direct resume and admin assign lift inside the claim CAS."""
    job_id, agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=False)
    token = operator_pause_lift_token(await db.get_job(job_id))

    assert not await db.claim_job_for_agent(
        job_id, agent_id, lift_operator_pause_hold="some-older-hold"
    )
    assert await db.claim_job_for_agent(
        job_id, agent_id, lift_operator_pause_hold=token
    )

    row = await _row(db, job_id)
    assert row["status"] == "processing"
    assert OPERATOR_PAUSE_HOLD_CONTEXT_KEY not in row["context"]


@pytest.mark.asyncio
async def test_stale_resume_cannot_lift_a_newer_pause(db):
    """A resume authorized against pause #1 must not cross pause #2."""
    job_id, agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=True)
    first = operator_pause_lift_token(await db.get_job(job_id))
    assert await db.queue_job_for_resume(
        job_id,
        expected_status="paused",
        lift_operator_pause_hold=first,
        completion_commands_enabled=True,
    )
    assert await db.claim_job_for_agent(
        job_id, agent_id, completion_commands_enabled=True
    )
    await _public_pause(db, job_id, commands_enabled=True)
    second = operator_pause_lift_token(await db.get_job(job_id))
    assert second and second != first

    # Delayed duplicate of the first resume, and a resume that observed an
    # unheld row before the second pause landed.
    for stale in (first, ""):
        assert not await db.queue_job_for_resume(
            job_id,
            {"queued_feedback": "stale"},
            expected_status="paused",
            lift_operator_pause_hold=stale,
            completion_commands_enabled=True,
        )
    row = await _row(db, job_id)
    assert row["context"][OPERATOR_PAUSE_HOLD_CONTEXT_KEY]["hold_id"] == second
    assert "queued_feedback" not in row["context"]
    assert not await _dispatchable(db, job_id, commands_enabled=True)

    assert await db.queue_job_for_resume(
        job_id,
        expected_status="paused",
        lift_operator_pause_hold=second,
        completion_commands_enabled=True,
    )
    assert await _dispatchable(db, job_id, commands_enabled=True)


@pytest.mark.asyncio
async def test_pause_racing_dispatch_delivery(db):
    """Pause wins between the dispatcher's claim and its post-POST confirm."""
    job_id, agent_id = await _dispatched_job(db)

    await _public_pause(db, job_id, commands_enabled=True)

    assert not await db.confirm_pinned_job_dispatch(job_id, agent_id)
    assert not await _dispatchable(db, job_id, commands_enabled=True)
    assert (await _row(db, job_id))["status"] == "paused"


@pytest.mark.asyncio
async def test_duplicate_pause_keeps_the_first_hold(db):
    job_id, _agent_id = await _dispatched_job(db)
    await _public_pause(db, job_id, commands_enabled=True)
    token = operator_pause_lift_token(await db.get_job(job_id))

    with pytest.raises(HTTPException) as raised:
        await _public_pause(db, job_id, commands_enabled=True)

    assert raised.value.status_code == 400
    assert operator_pause_lift_token(await db.get_job(job_id)) == token


@pytest.mark.asyncio
async def test_system_pauses_still_redispatch(db):
    """Preemption, agent release and a lifted-then-reclaimed job stay live."""
    preempted, preempted_agent = await _dispatched_job(db)
    control = CompletionControl(db, AsyncMock())
    claim = await control.claim_pause_job(
        preempted, source="dispatcher_preempt", expected_agent_id=preempted_agent
    )
    await control.abort_claim(claim)
    assert await _dispatchable(db, preempted, commands_enabled=True)

    released, released_agent = await _dispatched_job(db)
    assert await db.pause_job(
        released, completion_commands_enabled=True, expected_agent_id=released_agent
    )
    assert await _dispatchable(db, released, commands_enabled=True)

    resumed, resumed_agent = await _dispatched_job(db)
    await _public_pause(db, resumed, commands_enabled=True)
    assert await db.queue_job_for_resume(
        resumed,
        expected_status="paused",
        lift_operator_pause_hold=operator_pause_lift_token(await db.get_job(resumed)),
        completion_commands_enabled=True,
    )
    assert await db.claim_job_for_agent(
        resumed, resumed_agent, completion_commands_enabled=True
    )
    assert await db.pause_job(
        resumed, completion_commands_enabled=True, expected_agent_id=resumed_agent
    )
    assert await _dispatchable(db, resumed, commands_enabled=True)
