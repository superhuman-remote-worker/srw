"""Real-Postgres fencing proofs for the Gate-3 completion finalizer."""

from __future__ import annotations

from tests import b08_completion_helpers as b08_helpers

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import orchestrator.main
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.completion_finalizer import (
    CompletionDispositionSuperseded,
    CompletionEffectRunner,
    CompletionFinalizer,
    CompletionLeaseLost,
)
from orchestrator.services.job_completion_commands import accept_completion_command
from shared import worker_queue
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import verification_workflow as verification_workflow_module


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
            "TRUNCATE completion_effects, completion_finalizer_leases, "
            "job_completion_commands, run_queue, jobs, agents CASCADE"
        )
        # Seed after reset: TRUNCATE can reach grants through nullable FKs.
        await conn.execute("""
            INSERT INTO capability_grants(scope_kind,key,value_json) VALUES
                ('global','shell_tools','true'),
                ('global','delegation','true'),
                ('global','autonomy_ceiling','"full"')
            ON CONFLICT(scope_kind,scope_id,key) DO UPDATE SET value_json=EXCLUDED.value_json
        """)
    from orchestrator.services.manifest_experts import seed_bundled_expert_manifests

    await seed_bundled_expert_manifests(
        _pool_db(pool), Path(__file__).resolve().parents[1] / "config"
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _accepted_pinned(pg, *, report_id: UUID | None = None):
    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('developer', $1, 'working') RETURNING id",
            f"finalizer-{uuid4().hex[:10]}",
        )
        job_id = await conn.fetchval(
            "INSERT INTO jobs "
            "(description, status, execution_lane, assigned_agent_id) "
            "VALUES ('finalizer test', 'processing', 'pinned', $1) RETURNING id",
            agent_id,
        )
    accepted = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(report_id or uuid4()),
        requested_by="real-pg-finalizer-test",
    )
    return accepted, job_id, agent_id


async def _accepted_stateless(pg):
    lease_token = 71
    async with pg.acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('stateless finalizer test', 'processing', 'stateless') "
            "RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO run_queue (
                unit_id, unit_kind, state, attempts_since_completion,
                lease_token, leased_by, last_leased_by, leased_until,
                input_seq, consumed_seq
            ) VALUES (
                $1, 'worker_batch', 'leased', 1,
                $2, 'stateless-finalizer-pod', 'stateless-finalizer-pod',
                now() + interval '5 minutes', 4, 3
            )
            """,
            job_id,
            lease_token,
        )
    accepted = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": None,
        },
        lease_token=str(lease_token),
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="real-pg-stateless-finalizer-test",
    )
    return accepted, job_id


def _pool_db(pg) -> PostgresDB:
    database = PostgresDB.__new__(PostgresDB)
    database._pool = pg
    return database


async def _claimed_runner(db, command_id: str) -> CompletionEffectRunner:
    finalizer = CompletionFinalizer(db, leader_id="critic-synthesizer-test")
    command, owner = await finalizer._claim(command_id, inline=True)
    assert command is not None
    assert owner is not None
    return CompletionEffectRunner(db, command=command, owner=owner)


@pytest.mark.asyncio
async def test_two_claimers_execute_one_effect_and_replay_one_outcome(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    effect_calls = 0

    async def workflow(runner):
        async def effect():
            nonlocal effect_calls
            effect_calls += 1
            await asyncio.sleep(0.05)
            return {"external_id": "stable-result"}

        detail = await runner.run(name="one_effect", group="proof", callback=effect)
        return {"status": "handled", "detail": detail}

    first = CompletionFinalizer(pg, workflow=workflow, leader_id="first")
    second = CompletionFinalizer(pg, workflow=workflow, leader_id="second")
    results = await asyncio.gather(
        first.finalize_command(accepted.command_id),
        second.finalize_command(accepted.command_id),
    )

    assert effect_calls == 1
    assert any(result.disposition == "done" for result in results)
    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, attempts, outcome, finalizing_by, lease_expires_at "
            "FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        effect = await conn.fetchrow(
            "SELECT state, attempts, detail FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1",
            UUID(accepted.command_id),
        )
    assert command["state"] == "done"
    assert command["attempts"] == 1
    assert command["finalizing_by"] is None
    assert command["lease_expires_at"] is None
    outcome = json.loads(command["outcome"])
    effect_detail = json.loads(effect["detail"])
    assert outcome["detail"] == {"external_id": "stable-result"}
    assert effect["state"] == "done"
    assert effect["attempts"] == 1
    assert effect_detail["output"] == {"external_id": "stable-result"}

    replay = await second.finalize_command(accepted.command_id)
    assert replay.disposition == "terminal"
    assert replay.outcome == outcome
    assert effect_calls == 1


@pytest.mark.asyncio
async def test_due_stateless_command_is_finalized_once_by_background_claim(pg):
    accepted, job_id = await _accepted_stateless(pg)
    assert accepted.queue_terminalized is True

    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands "
            "SET run_after=clock_timestamp()-interval '1 second' WHERE id=$1",
            UUID(accepted.command_id),
        )
        queue_before = await conn.fetchrow(
            "SELECT state, leased_by, consumed_seq, input_seq "
            "FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert dict(queue_before) == {
        "state": "done",
        "leased_by": None,
        "consumed_seq": 4,
        "input_seq": 4,
    }

    workflow_calls = 0
    outcome = {
        "status": "handled",
        "job_id": str(job_id),
        "new_status": "completed",
        "actions": ["background finalizer proof"],
    }

    async def workflow(_runner):
        nonlocal workflow_calls
        workflow_calls += 1
        return outcome

    finalizer = CompletionFinalizer(
        pg,
        workflow=workflow,
        leader_id="stateless-background-proof",
    )
    finalized = await finalizer.finalize_command(accepted.command_id, inline=False)
    replay = await finalizer.finalize_command(accepted.command_id, inline=False)

    assert finalized.disposition == "done"
    assert finalized.state == "done"
    assert finalized.outcome == outcome
    assert replay.disposition == "terminal"
    assert replay.state == "done"
    assert replay.outcome == outcome
    assert workflow_calls == 1

    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, attempts, outcome, finalizing_by, lease_expires_at "
            "FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        queue_after = await conn.fetchrow(
            "SELECT state, leased_by, consumed_seq, input_seq "
            "FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert command["state"] == "done"
    assert command["attempts"] == 1
    assert json.loads(command["outcome"]) == outcome
    assert command["finalizing_by"] is None
    assert command["lease_expires_at"] is None
    assert dict(queue_after) == dict(queue_before)


@pytest.mark.asyncio
async def test_superseded_effect_is_replay_terminal_and_command_can_finish(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    effect_calls = 0

    async def workflow(runner):
        async def lose_world_state_cas():
            nonlocal effect_calls
            effect_calls += 1
            return {"won": False, "observed_status": "pending_review"}

        detail = await runner.run_transactional(
            name="critic_spawn_world_cas",
            group="verification",
            callback=lose_world_state_cas,
            supersede_if=lambda output: output["won"] is False,
        )
        return {"status": "handled", "detail": detail}

    finalizer = CompletionFinalizer(pg, workflow=workflow, leader_id="supersede")
    result = await finalizer.finalize_command(accepted.command_id)

    assert result.disposition == "done"
    assert effect_calls == 1
    async with pg.acquire() as conn:
        command_state = await conn.fetchval(
            "SELECT state FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        effect = await conn.fetchrow(
            "SELECT state, detail, completed_at, complete_by "
            "FROM completion_effects WHERE producer_kind='job_completion' "
            "AND producer_id=$1 AND effect_name='critic_spawn_world_cas'",
            UUID(accepted.command_id),
        )
    assert command_state == "done"
    assert effect["state"] == "superseded"
    assert effect["completed_at"] is not None
    assert effect["complete_by"] is None
    assert json.loads(effect["detail"])["output"] == {
        "won": False,
        "observed_status": "pending_review",
    }

    replay = await finalizer.finalize_command(accepted.command_id)
    assert replay.disposition == "terminal"
    assert effect_calls == 1


async def _critic_verdict_fixture(
    pg,
    *,
    target_status: str = "reviewing",
    target_lane: str = "pinned",
    finding_claim: str = "fix it",
):
    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('critic', $1, 'working') RETURNING id",
            f"critic-{uuid4().hex[:10]}",
        )
        target_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane, context, "
            "resolved_config) VALUES ('target', $1, $2, '{}'::jsonb, "
            '\'{"agent":{"autonomy":"review"}}\'::jsonb) RETURNING id',
            target_status,
            target_lane,
        )
        if target_lane == "stateless":
            await conn.execute(
                "INSERT INTO run_queue (unit_id, unit_kind, state) "
                "VALUES ($1, 'worker_batch', 'done')",
                target_id,
            )
        critic_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane, "
            "assigned_agent_id, parent_job_id, context) "
            "VALUES ('critic', 'processing', 'pinned', $1, $2, "
            "jsonb_build_object('verification_target', $2::uuid::text)) "
            "RETURNING id",
            agent_id,
            target_id,
        )
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object("
            "'verification_rounds', jsonb_build_array(jsonb_build_object("
            "'round', 1, 'critic_job_id', $2::uuid::text, "
            "'verdict', 'returned', 'opened', jsonb_build_array("
            "jsonb_build_object('id', 'F1', 'severity', 'high', "
            "'claim', $3::text, 'status', 'OPEN'))))) WHERE id=$1",
            target_id,
            critic_id,
            finding_claim,
        )
    accepted = await accept_completion_command(
        pg,
        job_id=str(critic_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="critic-synthesizer-test",
    )
    async with pg.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", critic_id)
    return accepted, target_id, critic_id


@pytest.mark.asyncio
@pytest.mark.parametrize("target_lane", ["pinned", "stateless"])
async def test_s27_reviewing_cas_and_effect_marker_commit_together(
    pg, monkeypatch, target_lane
):
    accepted, target_id, critic_id = await _critic_verdict_fixture(
        pg, target_lane=target_lane
    )
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    assert plan["outcome"] == "returned"
    async with pg.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT status::text, context, freeze_data FROM jobs WHERE id=$1",
            target_id,
        )
        effect = await conn.fetchrow(
            "SELECT state, detail, pg_column_size(detail) AS detail_bytes "
            "FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='critic_verdict'",
            UUID(accepted.command_id),
        )
        queue = await conn.fetchrow(
            "SELECT state, input_seq FROM run_queue WHERE unit_id=$1", target_id
        )
    assert target["status"] == "paused"
    assert target["freeze_data"] is None
    target_context = json.loads(target["context"])
    assert "F1" in target_context["queued_feedback"]
    assert effect["state"] == "done"
    assert effect["detail_bytes"] < 8 * 1024
    assert json.loads(effect["detail"])["output"]["world_cas_won"] is True
    if target_lane == "stateless":
        assert queue["state"] == "queued"
        assert queue["input_seq"] == 1
    else:
        assert queue is None


@pytest.mark.asyncio
async def test_s27_approval_consumes_the_reviewed_completion_decision(pg, monkeypatch):
    accepted, target_id, critic_id = await _critic_verdict_fixture(pg)
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET resolved_config='{"agent":{"autonomy":"full"}}'::jsonb,
                context=COALESCE(context, '{}'::jsonb) || jsonb_build_object(
                    'completion_decision',
                    jsonb_build_object('tool_call_id', 'round-2-reviewed-tool')
                )
            WHERE id=$1
            """,
            target_id,
        )
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        verification_workflow_module,
        "resolve_critic_outcome",
        lambda *_args: ("approved", "round 2 approved"),
    )
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    assert plan["outcome"] == "approved"
    assert plan["new_status"] == "completed"
    async with pg.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT status::text, context ? 'completion_decision' AS decision_live "
            "FROM jobs WHERE id=$1",
            target_id,
        )
    assert dict(target) == {"status": "completed", "decision_live": False}


@pytest.mark.asyncio
async def test_s27_oversized_findings_persist_in_domain_not_effect_detail(
    pg, monkeypatch
):
    large_claim = "oversized-finding-" + ("x" * 20_000)
    accepted, target_id, critic_id = await _critic_verdict_fixture(
        pg, finding_claim=large_claim
    )
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan == {
        "applicable": True,
        "world_cas_won": True,
        "outcome": "returned",
        "target_job_id": str(target_id),
        "critic_job_id": str(critic_id),
        "new_status": "paused",
        "open_finding_count": 1,
        "actions": [],
    }
    async with pg.acquire() as conn:
        target_feedback = await conn.fetchval(
            "SELECT context->>'queued_feedback' FROM jobs WHERE id=$1", target_id
        )
        detail_bytes = await conn.fetchval(
            "SELECT pg_column_size(detail) FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='critic_verdict'",
            UUID(accepted.command_id),
        )
    assert large_claim in target_feedback
    assert detail_bytes < 8 * 1024


@pytest.mark.asyncio
async def test_s27_stateless_return_uses_ledger_recomputed_after_queue_lock(
    pg, monkeypatch
):
    accepted, target_id, critic_id = await _critic_verdict_fixture(
        pg,
        target_lane="stateless",
        finding_claim="hint finding must not survive",
    )
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    original_enqueue = worker_queue.enqueue_worker_batch_wake

    async def enqueue_then_change_ledger(conn, **kwargs):
        admitted = await original_enqueue(conn, **kwargs)
        # The queue row is now locked by the synthesizer transaction, while a
        # concurrent verdict writer can still commit a newer target ledger.
        # S27 must derive feedback only after its subsequent jobs-row lock.
        locked_world = {
            "verification_rounds": [
                {
                    "round": 1,
                    "critic_job_id": str(critic_id),
                    "verdict": "returned",
                    "opened": [
                        {
                            "id": "F2",
                            "severity": "critical",
                            "claim": "locked finding wins",
                            "status": "OPEN",
                        }
                    ],
                }
            ]
        }
        async with pg.acquire() as rival:
            await rival.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                target_id,
                json.dumps(locked_world),
            )
        return admitted

    monkeypatch.setattr(
        worker_queue,
        "enqueue_worker_batch_wake",
        enqueue_then_change_ledger,
    )
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    async with pg.acquire() as conn:
        feedback = await conn.fetchval(
            "SELECT context->>'queued_feedback' FROM jobs WHERE id=$1",
            target_id,
        )
        queue = await conn.fetchrow(
            "SELECT state, input_seq FROM run_queue WHERE unit_id=$1", target_id
        )
    assert "locked finding wins" in feedback
    assert "hint finding must not survive" not in feedback
    assert queue["state"] == "queued"
    assert queue["input_seq"] == 1


@pytest.mark.asyncio
async def test_s27_multibyte_escalation_is_bounded_before_domain_write(pg, monkeypatch):
    accepted, target_id, critic_id = await _critic_verdict_fixture(pg)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    huge_reason = "誤" * 20_000
    monkeypatch.setattr(
        verification_workflow_module,
        "resolve_critic_outcome",
        lambda *_args: ("escalate", huge_reason),
    )
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    assert plan["outcome"] == "escalate"
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT error_message, status::text FROM jobs WHERE id=$1", target_id
        )
        detail_bytes = await conn.fetchval(
            "SELECT pg_column_size(detail) FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='critic_verdict'",
            UUID(accepted.command_id),
        )
    assert row["status"] == "pending_review"
    assert len(row["error_message"].encode("utf-8")) <= 1024
    assert row["error_message"].endswith("…")
    assert detail_bytes < 8 * 1024


@pytest.mark.asyncio
async def test_s27_human_decision_supersedes_effect_without_followup(pg, monkeypatch):
    accepted, target_id, critic_id = await _critic_verdict_fixture(
        pg, target_status="pending_review"
    )
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    critic = await db.get_job(str(critic_id))

    plan = await runner.run_transactional(
        name="critic_verdict",
        group="critic_verdict",
        callback=lambda: b08_helpers.materialize_critic_verdict_transactional(critic),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is False
    assert plan["observed_status"] == "pending_review"
    async with pg.acquire() as conn:
        assert (
            await conn.fetchval("SELECT status::text FROM jobs WHERE id=$1", target_id)
            == "pending_review"
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM completion_effects "
                "WHERE producer_kind='job_completion' AND producer_id=$1 "
                "AND effect_name='critic_verdict'",
                UUID(accepted.command_id),
            )
            == "superseded"
        )


async def _verification_parent_fixture(pg, *, with_workspace: bool = False):
    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('developer', $1, 'working') RETURNING id",
            f"verification-{uuid4().hex[:10]}",
        )
        parent_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane, "
            "assigned_agent_id, context, resolved_config, freeze_data) "
            "VALUES ('verification target', 'processing', 'pinned', $1, "
            '\'{"datasource_selection":{"datasource_ids":[],'
            '"policy_revisions":{}}}\'::jsonb, '
            '\'{"verification":{"enabled":true,'
            '"critic_config":"critic","max_rounds":3}}\'::jsonb, '
            '\'{"freeze_type":"job_complete","summary":"done",'
            '"deliverables":["output/result.md"]}\'::jsonb) RETURNING id',
            agent_id,
        )
    if with_workspace:
        db = _pool_db(pg)
        runtime_uid = str(uuid4())
        reservation = await db.reserve_managed_repository_workspace_creation(
            str(parent_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="verification-parent-fixture",
            desired_manifest_digest="0" * 64,
        )
        assert reservation is not None
        reservation = await db.mark_managed_repository_workspace_creation_started(
            str(parent_id),
            owner_kind="job",
            scope="workspace_container",
            reservation_generation=int(reservation["reservation_generation"]),
            claimant="verification-parent-fixture",
            claim_token=int(reservation["claim_token"]),
        )
        assert reservation is not None
        assert await db.authorize_managed_repository_workspace_creation_runtime(
            str(parent_id),
            owner_kind="job",
            scope="workspace_container",
            reservation_generation=int(reservation["reservation_generation"]),
            claimant="verification-parent-fixture",
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=runtime_uid,
        )
        workspace = {
            "provisioner": "k8s",
            "status": "ready",
            "_runtime_incarnation": runtime_uid,
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
        }
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET context=context || $2::jsonb WHERE id=$1",
                parent_id,
                json.dumps({"workspace_container": workspace}),
            )
        assert await db.settle_managed_repository_workspace_creation_reservation(
            str(parent_id),
            owner_kind="job",
            scope="workspace_container",
            reservation_generation=int(reservation["reservation_generation"]),
            claimant="verification-parent-fixture",
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=runtime_uid,
        )
    accepted = await accept_completion_command(
        pg,
        job_id=str(parent_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": {
                "freeze_type": "job_complete",
                "summary": "done",
                "deliverables": ["output/result.md"],
            },
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="verification-synthesizer-test",
    )
    async with pg.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='reviewing' WHERE id=$1", parent_id)
    return accepted, parent_id


@pytest.mark.asyncio
async def test_s30_materializes_one_critic_before_external_handoff(pg, monkeypatch):
    accepted, parent_id = await _verification_parent_fixture(pg)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    parent = await db.get_job(str(parent_id))

    plan = await runner.run_transactional(
        name="verification_critic_spawn",
        group="verification",
        callback=lambda: b08_helpers.materialize_verification_critic_transactional(
            parent,
            {"should_stop": True, "goal_achieved": True, "error": None},
            expected_round=0,
        ),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    assert plan["action"] == "handoff"
    async with pg.acquire() as conn:
        critics = await conn.fetch(
            "SELECT id, branch_name, context FROM jobs "
            "WHERE parent_job_id=$1 AND context->>'verification_target'=$1::text",
            parent_id,
        )
        effect = await conn.fetchrow(
            "SELECT state, pg_column_size(detail) AS detail_bytes "
            "FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='verification_critic_spawn'",
            UUID(accepted.command_id),
        )
    assert [str(row["id"]) for row in critics] == [plan["critic_job_id"]]
    assert critics[0]["branch_name"] is None
    assert effect["state"] == "done"
    assert effect["detail_bytes"] < 8 * 1024


@pytest.mark.asyncio
async def test_s30_inherits_parent_workspace_without_rebinding_runtime(pg, monkeypatch):
    accepted, parent_id = await _verification_parent_fixture(pg, with_workspace=True)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    parent = await db.get_job(str(parent_id))

    plan = await runner.run_transactional(
        name="verification_critic_spawn",
        group="verification",
        callback=lambda: b08_helpers.materialize_verification_critic_transactional(
            parent,
            {"should_stop": True, "goal_achieved": True, "error": None},
            expected_round=0,
        ),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["action"] == "handoff"
    critic = await db.get_job(plan["critic_job_id"])
    critic_context = critic["context"]
    if isinstance(critic_context, str):
        critic_context = json.loads(critic_context)
    assert critic_context["inherits_parent_workspace"] is True
    assert "workspace_container" not in critic_context
    assert "vm" not in critic_context
    assert critic_context["_workspace_contract"]["assignment_source"] == (
        "parent_inheritance"
    )


@pytest.mark.asyncio
async def test_s30_create_job_and_effect_marker_roll_back_as_one_unit(pg, monkeypatch):
    accepted, parent_id = await _verification_parent_fixture(pg)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    workspace_handoff = AsyncMock()
    dispatch = MagicMock()
    monkeypatch.setattr(
        verification_workflow_module,
        "setup_verification_critic_workspace",
        workspace_handoff,
    )
    monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", dispatch)
    runner = await _claimed_runner(db, accepted.command_id)
    parent = await db.get_job(str(parent_id))

    def fail_after_materialization(_output):
        raise RuntimeError("rollback after critic insert")

    with pytest.raises(RuntimeError, match="rollback after critic insert"):
        await runner.run_transactional(
            name="verification_critic_spawn",
            group="verification",
            callback=lambda: b08_helpers.materialize_verification_critic_transactional(
                parent,
                {"should_stop": True, "goal_achieved": True, "error": None},
                expected_round=0,
            ),
            supersede_if=fail_after_materialization,
        )

    async with pg.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM jobs WHERE parent_job_id=$1 "
            "AND context->>'verification_target'=$1::text)",
            parent_id,
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='verification_critic_spawn')",
            UUID(accepted.command_id),
        )
    workspace_handoff.assert_not_awaited()
    dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_s30_multibyte_delivery_error_is_bounded_before_domain_write(
    pg, monkeypatch
):
    accepted, parent_id = await _verification_parent_fixture(pg)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    parent = await db.get_job(str(parent_id))
    huge_error = "配" * 20_000
    parent["freeze_data"] = {
        "freeze_type": "job_complete",
        "delivery_failed": True,
        "delivery_error": huge_error,
    }

    plan = await runner.run_transactional(
        name="verification_critic_spawn",
        group="verification",
        callback=lambda: b08_helpers.materialize_verification_critic_transactional(
            parent,
            {"should_stop": True, "goal_achieved": True, "error": None},
            expected_round=0,
        ),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is True
    assert plan["action"] == "escalate"
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT error_message, status::text FROM jobs WHERE id=$1", parent_id
        )
        detail_bytes = await conn.fetchval(
            "SELECT pg_column_size(detail) FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='verification_critic_spawn'",
            UUID(accepted.command_id),
        )
    assert row["status"] == "pending_review"
    assert len(row["error_message"].encode("utf-8")) <= 1024
    assert row["error_message"].endswith("…")
    assert detail_bytes < 8 * 1024


@pytest.mark.asyncio
@pytest.mark.parametrize("miss", ["status", "round"])
async def test_s30_reviewing_or_round_miss_supersedes_without_spawn(
    pg, monkeypatch, miss
):
    accepted, parent_id = await _verification_parent_fixture(pg)
    db = _pool_db(pg)
    monkeypatch.setattr(orchestrator.main.app.state.resources, "postgres_db", db)
    runner = await _claimed_runner(db, accepted.command_id)
    parent = await db.get_job(str(parent_id))
    async with pg.acquire() as conn:
        if miss == "status":
            await conn.execute(
                "UPDATE jobs SET status='pending_review' WHERE id=$1", parent_id
            )
        else:
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context, "
                "'{verification_rounds}', '[{\"round\":1}]'::jsonb) WHERE id=$1",
                parent_id,
            )

    plan = await runner.run_transactional(
        name="verification_critic_spawn",
        group="verification",
        callback=lambda: b08_helpers.materialize_verification_critic_transactional(
            parent,
            {"should_stop": True, "goal_achieved": True, "error": None},
            expected_round=0,
        ),
        supersede_if=lambda output: output["world_cas_won"] is False,
    )

    assert plan["world_cas_won"] is False
    assert plan["observed_status"] == (
        "pending_review" if miss == "status" else "reviewing:round-1"
    )
    async with pg.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM jobs WHERE parent_job_id=$1 "
            "AND context->>'verification_target'=$1::text)",
            parent_id,
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM completion_effects "
                "WHERE producer_kind='job_completion' AND producer_id=$1 "
                "AND effect_name='verification_critic_spawn'",
                UUID(accepted.command_id),
            )
            == "superseded"
        )


@pytest.mark.asyncio
async def test_long_s36_budget_extends_exact_term_then_shrinks_before_finish(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    observed: dict[str, float] = {}

    async def workflow(runner):
        async def terminal_snapshot():
            async with pg.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT extract(epoch FROM command.lease_expires_at-now()) "
                    "AS command_remaining, "
                    "extract(epoch FROM effect.complete_by-now()) "
                    "AS effect_remaining "
                    "FROM job_completion_commands command "
                    "JOIN completion_effects effect ON effect.producer_id=command.id "
                    "WHERE command.id=$1 AND effect.effect_name='workspace_teardown'",
                    UUID(accepted.command_id),
                )
            observed["command"] = float(row["command_remaining"])
            observed["effect"] = float(row["effect_remaining"])
            return {"snapshot": "command-keyed"}

        output = await runner.run(
            name="workspace_teardown",
            group="teardown",
            callback=terminal_snapshot,
            effect_timeout_seconds=890,
            command_lease_seconds=900,
        )
        async with pg.acquire() as conn:
            observed["after"] = float(
                await conn.fetchval(
                    "SELECT extract(epoch FROM lease_expires_at-now()) "
                    "FROM job_completion_commands WHERE id=$1",
                    UUID(accepted.command_id),
                )
            )
        return {"status": "handled", "teardown": output}

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "done"
    assert observed["effect"] > 880
    assert observed["command"] > observed["effect"] + 5
    assert 115 < observed["after"] <= 120


@pytest.mark.asyncio
async def test_expired_owner_is_fenced_from_renew_and_finish_after_takeover(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands SET state='finalizing', attempts=1, "
            "finalizing_by='old-owner', lease_expires_at=now()-interval '1 second' "
            "WHERE id=$1",
            UUID(accepted.command_id),
        )

    finalizer = CompletionFinalizer(pg, workflow=lambda runner: _outcome("new"))
    assert not await finalizer._renew_command(accepted.command_id, "old-owner")

    result = await finalizer.finalize_command(accepted.command_id)
    assert result.disposition == "done"
    assert result.outcome == {"status": "new"}
    with pytest.raises(CompletionLeaseLost):
        await finalizer._finish(accepted.command_id, "old-owner", {"status": "stale"})


@pytest.mark.asyncio
async def test_cancel_after_accept_supersedes_before_workflow_or_effects(pg):
    accepted, job_id, _ = await _accepted_pinned(pg)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=$1",
            job_id,
        )
    workflow_calls = 0

    async def workflow(_runner):
        nonlocal workflow_calls
        workflow_calls += 1
        return {"status": "handled", "new_status": "completed"}

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "superseded"
    assert workflow_calls == 0
    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, attempts, accepted_job_status, outcome, finalized_at, "
            "error_code, finalizing_by, lease_expires_at FROM "
            "job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        effect_count = await conn.fetchval(
            "SELECT count(*) FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1",
            UUID(accepted.command_id),
        )
    outcome = json.loads(command["outcome"])
    assert command["state"] == "superseded"
    assert command["attempts"] == 1
    assert command["accepted_job_status"] == "processing"
    assert command["finalized_at"] is not None
    assert command["error_code"] == "entry_status_superseded"
    assert command["finalizing_by"] is None
    assert command["lease_expires_at"] is None
    assert outcome["observed_status"] == "cancelled"
    assert outcome["expected_entry_statuses"] == ["processing"]
    assert outcome["abandoned_effects"] == []
    assert effect_count == 0


@pytest.mark.asyncio
async def test_cancel_after_entry_resolution_fences_pre_s17_delivery(pg):
    accepted, job_id, _ = await _accepted_pinned(pg)
    delivery_calls = 0

    async def workflow(runner):
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1",
                job_id,
            )
        await runner.assert_entry_authority()

        async def must_not_deliver():
            nonlocal delivery_calls
            delivery_calls += 1
            return {"delivered": True}

        await runner.run(
            name="subjob_output_graft",
            group="subjob_graft",
            callback=must_not_deliver,
        )
        raise AssertionError("entry-authority loss did not stop delivery")

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "superseded"
    assert delivery_calls == 0
    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, outcome FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        effect_count = await conn.fetchval(
            "SELECT count(*) FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1",
            UUID(accepted.command_id),
        )
    assert command["state"] == "superseded"
    assert json.loads(command["outcome"])["observed_status"] == "cancelled"
    assert effect_count == 0


@pytest.mark.asyncio
async def test_pending_group_query_is_fenced_to_exact_live_command(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    runner = await _claimed_runner(_pool_db(pg), accepted.command_id)
    runner.command["resolved_entry_status"] = "processing"
    async with pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO completion_effects "
            "(producer_kind, producer_id, scope_id, effect_name, effect_group, "
            " state, attempts, run_after, intent_at, complete_by, detail) "
            "SELECT 'job_completion', id, job_id, 'graft', 'subjob_graft', "
            "       'pending', 1, now()+interval '5 seconds', now(), NULL, '{}' "
            "FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )

    assert await runner.has_pending_group("subjob_graft")
    assert not await runner.has_pending_group("terminal_delivery")

    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands SET lease_expires_at=now()-interval '1 second' "
            "WHERE id=$1",
            UUID(accepted.command_id),
        )
    # Once this workflow pass observed the durable block, it remains the
    # finalizer release signal even if the exact command term expires before
    # workflow return.
    assert await runner.has_pending_group("subjob_graft")


@pytest.mark.asyncio
async def test_typed_s17_race_supersedes_exact_term_and_abandons_pending_effect(pg):
    accepted, job_id, _ = await _accepted_pinned(pg)

    async def workflow(runner):
        async def s17_attempt():
            async with pg.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status='cancelled' WHERE id=$1",
                    job_id,
                )
            raise CompletionDispositionSuperseded(
                observed_status="cancelled",
                expected_statuses=("processing",),
            )

        await runner.run(
            name="main_status_write",
            group="job_disposition",
            callback=s17_attempt,
        )
        raise AssertionError("typed status race did not stop the workflow")

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "superseded"
    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, attempts, outcome, finalized_at, error_code, "
            "finalizing_by, lease_expires_at FROM job_completion_commands "
            "WHERE id=$1",
            UUID(accepted.command_id),
        )
        effect = await conn.fetchrow(
            "SELECT state, error_code FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='main_status_write'",
            UUID(accepted.command_id),
        )
    outcome = json.loads(command["outcome"])
    assert command["state"] == "superseded"
    assert command["attempts"] == 1
    assert command["finalized_at"] is not None
    assert command["error_code"] == "entry_status_superseded"
    assert command["finalizing_by"] is None
    assert command["lease_expires_at"] is None
    assert outcome["observed_status"] == "cancelled"
    assert outcome["abandoned_effects"] == ["main_status_write"]
    assert effect["state"] == "pending"
    assert effect["error_code"] == "CompletionDispositionSuperseded"


@pytest.mark.asyncio
async def test_post_s17_cancel_supersedes_before_class_c_effect(pg):
    accepted, job_id, _ = await _accepted_pinned(pg)
    class_c_calls = 0

    async def workflow(runner):
        async def write_s17():
            async with pg.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status='completed' WHERE id=$1",
                    job_id,
                )
            return {"new_status": "completed"}

        await runner.run(
            name="main_status_write",
            group="job_disposition",
            callback=write_s17,
        )
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1",
                job_id,
            )
        await runner.assert_disposition_authority()

        async def must_not_run():
            nonlocal class_c_calls
            class_c_calls += 1
            return {"grafted": True}

        await runner.run(
            name="subjob_output_graft",
            group="subjob_graft",
            callback=must_not_run,
        )
        raise AssertionError("post-S17 cancel did not stop Class C")

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "superseded"
    assert class_c_calls == 0
    async with pg.acquire() as conn:
        command = await conn.fetchrow(
            "SELECT state, outcome, error_code FROM job_completion_commands "
            "WHERE id=$1",
            UUID(accepted.command_id),
        )
        effects = await conn.fetch(
            "SELECT effect_name, state FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "ORDER BY effect_name",
            UUID(accepted.command_id),
        )
    assert command["state"] == "superseded"
    assert command["error_code"] == "entry_status_superseded"
    assert json.loads(command["outcome"])["observed_status"] == "cancelled"
    assert [(row["effect_name"], row["state"]) for row in effects] == [
        ("main_status_write", "done")
    ]


@pytest.mark.asyncio
async def test_completed_s23_pause_is_command_owned_disposition_authority(pg):
    accepted, job_id, _ = await _accepted_pinned(pg)

    async def workflow(runner):
        async def write_s17():
            async with pg.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status='pending_review' WHERE id=$1",
                    job_id,
                )
            return {"new_status": "pending_review"}

        await runner.run(
            name="main_status_write",
            group="job_disposition",
            callback=write_s17,
        )

        async def auto_deny_resume():
            async with pg.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status='paused' WHERE id=$1",
                    job_id,
                )
            return {"auto_denied": True}

        await runner.run(
            name="auto_deny_resume",
            group="auto_deny_resume",
            callback=auto_deny_resume,
        )
        await runner.assert_disposition_authority()
        return {"status": "handled", "new_status": "paused"}

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "done"
    assert result.outcome == {"status": "handled", "new_status": "paused"}
    async with pg.acquire() as conn:
        assert await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job_id) == (
            "paused"
        )


async def _outcome(status: str) -> dict[str, str]:
    return {"status": status}


@pytest.mark.asyncio
async def test_report_sequence_and_inline_grace_gate_background_claims(pg):
    first, job_id, agent_id = await _accepted_pinned(pg)
    second = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": None,
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="real-pg-finalizer-test",
    )

    async def ordered_noop(_runner):
        return {"status": "done", "new_status": "processing"}

    finalizer = CompletionFinalizer(pg, workflow=ordered_noop)

    grace = await finalizer.finalize_command(first.command_id, inline=False)
    assert grace.disposition == "busy"
    assert grace.state == "pending"
    blocked = await finalizer.finalize_command(second.command_id, inline=True)
    assert blocked.disposition == "busy"
    assert blocked.state == "pending"

    assert (await finalizer.finalize_command(first.command_id)).disposition == "done"
    assert (await finalizer.finalize_command(second.command_id)).disposition == "done"


@pytest.mark.asyncio
async def test_successor_adopts_done_predecessor_status_not_stale_accept_snapshot(pg):
    first, job_id, agent_id = await _accepted_pinned(pg)
    second = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": "late crash report",
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="real-pg-finalizer-test",
    )

    async def complete_first(_runner):
        async with pg.acquire() as conn:
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
        return {"status": "handled", "new_status": "completed"}

    first_result = await CompletionFinalizer(
        pg, workflow=complete_first
    ).finalize_command(first.command_id)
    observed_entry: str | None = None

    async def absorb_late_error(runner):
        nonlocal observed_entry
        observed_entry = str(runner.command["resolved_entry_status"])
        return {"status": "handled", "new_status": "completed"}

    second_result = await CompletionFinalizer(
        pg, workflow=absorb_late_error
    ).finalize_command(second.command_id)

    assert first_result.disposition == "done"
    assert second_result.disposition == "done"
    assert observed_entry == "completed"
    async with pg.acquire() as conn:
        states = await conn.fetch(
            "SELECT report_seq, state, outcome FROM job_completion_commands "
            "WHERE job_id=$1 ORDER BY report_seq",
            job_id,
        )
        status = await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job_id)
    assert [row["state"] for row in states] == ["done", "done"]
    assert json.loads(states[0]["outcome"])["new_status"] == "completed"
    assert json.loads(states[1]["outcome"])["new_status"] == "completed"
    assert status == "completed"


@pytest.mark.asyncio
async def test_feedback_round_accept_finishes_and_consumes_its_exact_decision(pg):
    """Round 1 reviewing -> feedback processing -> round 2 terminal."""

    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('developer', $1, 'working') RETURNING id",
            f"feedback-finalizer-{uuid4().hex[:10]}",
        )
        job_id = await conn.fetchval(
            """
            INSERT INTO jobs (
                description, status, execution_lane, assigned_agent_id, context
            ) VALUES (
                'feedback round finalizer', 'processing', 'pinned', $1,
                jsonb_build_object(
                    'completion_decision',
                    jsonb_build_object(
                        'tool_call_id', 'round-1-tool',
                        'recorded_at', '2026-08-18T10:00:00+00:00'
                    )
                )
            ) RETURNING id
            """,
            agent_id,
        )
    first = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="round-1-agent",
    )

    async def finalize_round_one(_runner):
        async with pg.acquire() as conn:
            assert (
                await conn.execute(
                    "UPDATE jobs SET status='reviewing' "
                    "WHERE id=$1 AND status='processing'",
                    job_id,
                )
                == "UPDATE 1"
            )
        return {"status": "handled", "new_status": "reviewing"}

    assert (
        await CompletionFinalizer(pg, workflow=finalize_round_one).finalize_command(
            first.command_id
        )
    ).disposition == "done"

    # The verification feedback transition voids round 1 and journals a fresh
    # round-2 decision before that report is accepted against processing.
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET status='processing', assigned_agent_id=$2,
                context=(COALESCE(context, '{}'::jsonb)-'completion_decision')
                    || jsonb_build_object(
                        'completion_decision',
                        jsonb_build_object(
                            'tool_call_id', 'round-2-tool',
                            'recorded_at', '2026-08-18T10:05:00+00:00'
                        )
                    )
            WHERE id=$1 AND status='reviewing'
            """,
            job_id,
            agent_id,
        )
    second = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="round-2-agent",
    )

    class _PoolDatabase:
        def acquire(self):
            return pg.acquire()

        async def delete_checkpoint_thread(self, _job_id):
            return None

    async def finalize_round_two(runner):
        accepted_decision = runner.command["payload"]["_accepted_completion_decision"][
            "tool_call_id"
        ]
        updated = await PostgresDB.update_job_status(
            _PoolDatabase(),
            str(job_id),
            status="completed",
            expected_status="processing",
            completion_command_id=runner.command_id,
            completion_finalizing_by=runner.owner,
            consume_completion_decision_tool_call_id=accepted_decision,
        )
        assert updated
        return {"status": "handled", "new_status": "completed"}

    second_result = await CompletionFinalizer(
        pg, workflow=finalize_round_two
    ).finalize_command(second.command_id)

    assert second_result.disposition == "done"
    async with pg.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT status, context ? 'completion_decision' AS decision_live "
            "FROM jobs WHERE id=$1",
            job_id,
        )
        commands = await conn.fetch(
            "SELECT report_seq, state, outcome->>'new_status' AS new_status "
            "FROM job_completion_commands WHERE job_id=$1 ORDER BY report_seq",
            job_id,
        )
    assert dict(job) == {"status": "completed", "decision_live": False}
    assert [dict(row) for row in commands] == [
        {"report_seq": 1, "state": "done", "new_status": "reviewing"},
        {"report_seq": 2, "state": "done", "new_status": "completed"},
    ]


@pytest.mark.asyncio
async def test_genuine_status_change_supersedes_and_cannot_leave_live_decision(pg):
    async with pg.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('developer', $1, 'working') RETURNING id",
            f"status-race-{uuid4().hex[:10]}",
        )
        job_id = await conn.fetchval(
            """
            INSERT INTO jobs (
                description, status, execution_lane, assigned_agent_id, context
            ) VALUES (
                'status race', 'processing', 'pinned', $1,
                jsonb_build_object(
                    'completion_decision',
                    jsonb_build_object('tool_call_id', 'status-race-tool')
                )
            ) RETURNING id
            """,
            agent_id,
        )
    accepted = await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={"should_stop": True, "goal_achieved": True},
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="status-race-agent",
    )
    async with pg.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='paused' WHERE id=$1", job_id)

    async def must_not_run(_runner):
        raise AssertionError("stale acceptance reached the completion workflow")

    result = await CompletionFinalizer(pg, workflow=must_not_run).finalize_command(
        accepted.command_id
    )

    assert result.disposition == "superseded"
    assert result.outcome["observed_status"] == "paused"
    assert result.outcome["completion_decision_disposition"] == (
        "voided_exact_acceptance"
    )
    async with pg.acquire() as conn:
        state = await conn.fetchrow(
            "SELECT status, context ? 'completion_decision' AS decision_live, "
            "(SELECT state FROM job_completion_commands WHERE id=$2) AS command_state "
            "FROM jobs WHERE id=$1",
            job_id,
            UUID(accepted.command_id),
        )
    assert dict(state) == {
        "status": "paused",
        "decision_live": False,
        "command_state": "superseded",
    }


@pytest.mark.asyncio
async def test_destructive_effect_intent_and_higher_sequence_are_durable(pg):
    first, job_id, agent_id = await _accepted_pinned(pg)
    await accept_completion_command(
        pg,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": "later report",
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(agent_id),
        client_report_id=str(uuid4()),
        requested_by="real-pg-finalizer-test",
    )
    identity = {
        "kind": "kubernetes",
        "pod_uid": "pod-uid-a",
        "pvc_uid": "pvc-uid-a",
        "service_uid": "service-uid-a",
    }

    async def workflow(runner):
        async def teardown_probe():
            assert await runner.capture_intent("workspace_teardown") is None
            assert (
                await runner.capture_intent("workspace_teardown", identity) == identity
            )
            assert await runner.capture_intent("workspace_teardown") == identity
            return {"captured": True}

        output = await runner.run(
            name="workspace_teardown",
            group="teardown",
            callback=teardown_probe,
        )
        return {"status": "handled", "teardown": output}

    result = await CompletionFinalizer(pg, workflow=workflow).finalize_command(
        first.command_id
    )

    assert result.disposition == "done"
    async with pg.acquire() as conn:
        detail = await conn.fetchval(
            "SELECT detail FROM completion_effects "
            "WHERE producer_kind='job_completion' AND producer_id=$1 "
            "AND effect_name='workspace_teardown'",
            UUID(first.command_id),
        )
    assert json.loads(detail) == {
        "intent": identity,
        "output": {"captured": True},
    }


@pytest.mark.asyncio
async def test_failure_retries_with_bounded_backoff_then_parks_at_cap(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE job_completion_commands SET max_attempts=2 WHERE id=$1",
            UUID(accepted.command_id),
        )

    async def failing(runner):
        async def fail_effect():
            raise RuntimeError("downstream unavailable")

        await runner.run(name="failing_effect", group="proof", callback=fail_effect)
        return {"status": "unreachable"}

    finalizer = CompletionFinalizer(pg, workflow=failing, random_source=lambda: 1.0)
    first = await finalizer.finalize_command(accepted.command_id)
    assert first.disposition == "retry"
    async with pg.acquire() as conn:
        delay = await conn.fetchval(
            "SELECT extract(epoch FROM run_after-now()) "
            "FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        await conn.execute(
            "UPDATE job_completion_commands SET run_after=now()-interval '1 second' "
            "WHERE id=$1",
            UUID(accepted.command_id),
        )
        await conn.execute(
            "UPDATE completion_effects SET complete_by=now()-interval '1 second' "
            "WHERE producer_kind='job_completion' AND producer_id=$1",
            UUID(accepted.command_id),
        )
    assert 4.8 <= float(delay) <= 6.1

    second = await finalizer.finalize_command(accepted.command_id, inline=False)
    assert second.disposition == "parked"
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, attempts, outcome, finalized_at, error_code "
            "FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
    assert dict(row) == {
        "state": "parked",
        "attempts": 2,
        "outcome": None,
        "finalized_at": None,
        "error_code": "RuntimeError",
    }


@pytest.mark.asyncio
async def test_code_version_mismatch_parks_and_alerts(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    alerts: list[str] = []
    finalizer = CompletionFinalizer(
        pg,
        workflow=lambda runner: _outcome("never"),
        code_version="job-completion-v2",
        alert=alerts.append,
    )

    result = await finalizer.finalize_command(accepted.command_id)

    assert result.state == "parked"
    assert result.error_code == "code_version_mismatch"
    assert len(alerts) == 1
    assert "code version" in alerts[0]


@pytest.mark.asyncio
async def test_leader_takeover_fences_old_elected_term(pg):
    first = CompletionFinalizer(pg, leader_id="pod-a")
    second = CompletionFinalizer(pg, leader_id="pod-b")
    first_term = await first.acquire_leader()
    assert first_term is not None
    assert await second.acquire_leader() is None

    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE completion_finalizer_leases "
            "SET elected_at=now()-interval '2 seconds', "
            "expires_at=now()-interval '1 second' "
            "WHERE lease_name='job_completion'"
        )
    second_term = await second.acquire_leader()
    assert second_term is not None
    assert second_term.elected_at != first_term.elected_at
    assert not await first.renew_leader(first_term)
    assert not await first.release_leader(first_term)
    assert await second.renew_leader(second_term)


@pytest.mark.asyncio
async def test_cancelled_inline_attempt_is_resumed_after_lease_expiry(pg):
    accepted, _, _ = await _accepted_pinned(pg)
    entered = asyncio.Event()

    async def stranded(runner):
        entered.set()
        await asyncio.Event().wait()
        return {"status": "unreachable"}

    first = CompletionFinalizer(pg, workflow=stranded)
    task = asyncio.create_task(first.finalize_command(accepted.command_id))
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with pg.acquire() as conn:
        stranded_row = await conn.fetchrow(
            "SELECT state, finalizing_by FROM job_completion_commands WHERE id=$1",
            UUID(accepted.command_id),
        )
        assert stranded_row["state"] == "finalizing"
        assert stranded_row["finalizing_by"] is not None
        await conn.execute(
            "UPDATE job_completion_commands "
            "SET lease_expires_at=now()-interval '1 second', "
            "run_after=now()-interval '1 second' WHERE id=$1",
            UUID(accepted.command_id),
        )
        # A callback with no named effect leaves no effect ambiguity window;
        # this assertion makes that precondition explicit.
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM completion_effects WHERE producer_id=$1",
                UUID(accepted.command_id),
            )
            == 0
        )

    resumed = CompletionFinalizer(pg, workflow=lambda runner: _outcome("resumed"))
    result = await resumed.finalize_command(accepted.command_id, inline=False)
    assert result.disposition == "done"
    assert result.outcome == {"status": "resumed"}
