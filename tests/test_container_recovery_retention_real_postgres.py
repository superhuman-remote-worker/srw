"""W4: real owner/creation/claim transitions must preserve recoverable work."""

import asyncio
import json
from uuid import UUID, uuid4
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.job_completion_commands import accept_completion_command
from orchestrator.services.completion_finalizer import CompletionFinalizer
from tests.test_completion_finalizer_real_postgres import _claimed_runner
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle
from tests.test_vm_workspace_recovery_real_postgres import (
    prepare_checkpoint_rows,
    checkpoint_row_counts,
)

pg_dsn = lifecycle.pg_dsn
_schema_applied = lifecycle._schema_applied
db = lifecycle.db


async def recovery_job(db, *, attempts, lane="pinned"):
    (
        job_id,
        runtime_uid,
        creation,
        state,
    ) = await lifecycle._create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET config_override='{"
            + '"workspace":{"backend":"sandbox"}'
            + "}'::jsonb, "
            "context=jsonb_set(context,'{workspace_container,recovery_attempts}',$2::jsonb) WHERE id=$1",
            job_id,
            str(attempts),
        )
        agent_id = await conn.fetchval(
            "INSERT INTO agents(config_name,hostname,status) VALUES('worker_base',$1,'ready') RETURNING id",
            "recovery-proof-" + uuid4().hex,
        )
    if lane == "stateless":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET execution_lane='stateless' WHERE id=$1", job_id
            )
        admitted, _ = await db.admit_stateless_worker_job(
            str(job_id),
            fair_key=None,
            priority=5,
            completion_commands_enabled=True,
        )
        assert admitted
        from shared.worker_queue import claim_worker_batch

        claim = await claim_worker_batch(
            db,
            pod_name="recovery-worker",
            affinity_grace_seconds=0,
            completion_commands_enabled=True,
        )
        assert claim is not None and claim.unit_id == job_id
    else:
        assert await db.claim_job_for_agent(str(job_id), str(agent_id))
    job = await db.get_job(str(job_id))
    if isinstance(job["context"], str):
        job["context"] = json.loads(job["context"])
    assert job["status"] == "processing"
    if lane == "pinned":
        assert str(job["assigned_agent_id"]) == str(agent_id)
    assert job["context"]["workspace_container"]["_runtime_incarnation"] == runtime_uid
    return job_id, job


@pytest.mark.asyncio
@pytest.mark.parametrize("pod_alive", [True])
async def test_container_recovery_pause_preserves_checkpoint_and_volume_policy(
    db, monkeypatch, pod_alive
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    monkeypatch.setenv("WORKSPACE_RECOVERY_MAX_ATTEMPTS", "3")
    job_id, job = await recovery_job(db, attempts=0)
    await prepare_checkpoint_rows(db._pool, job_id, checkpoint_count=1)
    deleted = AsyncMock(return_value=True)
    outcome = await handle_pod_workspace_recovery(
        job,
        str(job_id),
        {"type": "workspace_unavailable", "message": "fixture transport loss"},
        db=db,
        delete_workspace=deleted,
        trigger_dispatch=Mock(),
        probe=AsyncMock(return_value=pod_alive),
        expected_status="processing",
    )
    assert outcome["new_status"] == "paused"
    assert await checkpoint_row_counts(db._pool, job_id) == (1, 1, 1)
    async with db.acquire() as conn:
        policies = await conn.fetch(
            "SELECT resource_policy FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
            job_id,
        )
    assert all(r["resource_policy"] == "preserve" for r in policies)


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True, "stateless"])
async def test_container_recovery_exhaustion_preserves_checkpoint_and_volume_policy(
    db, monkeypatch, durable
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    monkeypatch.setenv("WORKSPACE_RECOVERY_MAX_ATTEMPTS", "3")
    job_id, job = await recovery_job(
        db, attempts=3, lane="stateless" if durable == "stateless" else "pinned"
    )
    authority = {"expected_status": "processing"}
    if durable:
        lease_token = (
            await db.fetchval(
                "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
            )
            if durable == "stateless"
            else None
        )
        accepted = await accept_completion_command(
            db._pool,
            job_id=str(job_id),
            payload={
                "should_stop": True,
                "goal_achieved": False,
                "error": {
                    "type": "workspace_unavailable",
                    "recoverable": True,
                    "message": "fixture transport loss",
                },
                "freeze_data": None,
            },
            lease_token=lease_token,
            agent_id=str(job["assigned_agent_id"])
            if job["assigned_agent_id"]
            else None,
            client_report_id=str(uuid4()),
            requested_by="recovery-retention-proof",
        )
        if durable == "stateless":
            assert accepted.queue_terminalized is True
            assert (
                await db.fetchval(
                    "SELECT state FROM run_queue WHERE unit_id=$1", job_id
                )
                == "done"
            )
        runner = await _claimed_runner(db, accepted.command_id)
        authority = {
            "completion_command_id": runner.command_id,
            "completion_finalizing_by": runner.owner,
        }
    await prepare_checkpoint_rows(db._pool, job_id, checkpoint_count=1)
    deleted = AsyncMock(return_value=True)
    dispatch = Mock()
    probe = AsyncMock(return_value=False)
    outcome = await handle_pod_workspace_recovery(
        job,
        str(job_id),
        {"type": "workspace_unavailable", "message": "fixture transport loss"},
        db=db,
        delete_workspace=deleted,
        trigger_dispatch=dispatch,
        probe=probe,
        **authority,
    )
    async with db.acquire() as conn:
        policies = await conn.fetch(
            "SELECT resource_policy FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
            job_id,
        )
    assert (
        outcome["new_status"],
        await checkpoint_row_counts(db._pool, job_id),
        [r["resource_policy"] for r in policies],
    ) == ("paused", (1, 1, 1), [])
    deleted.assert_not_awaited()
    dispatch.assert_not_called()
    probe.assert_not_awaited()
    held = await db.get_job(str(job_id))
    context = (
        json.loads(held["context"])
        if isinstance(held["context"], str)
        else held["context"]
    )
    assert held["assigned_agent_id"] is None
    assert (
        context["workspace_container"]["_runtime_incarnation"]
        == job["context"]["workspace_container"]["_runtime_incarnation"]
    )
    assert context["workspace_container"]["status"] == "ready"
    assert context["workspace_container"]["recovery_attempts"] == 4
    assert context["_operator_pause_hold"]["source"] == "workspace_recovery_exhausted"
    assert context["_operator_pause_hold"]["paused_by"] is None


async def accepted_recovery(db, job):
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job["id"]),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": {"type": "workspace_unavailable", "message": "transport lost"},
            "freeze_data": None,
        },
        lease_token=None,
        agent_id=str(job["assigned_agent_id"]),
        client_report_id=str(uuid4()),
        requested_by="recovery-retention-proof",
    )
    return await _claimed_runner(db, accepted.command_id)


async def hold(db, job, runner=None):
    return await db.hold_exhausted_workspace_recovery(
        str(job["id"]),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=job["assigned_agent_id"],
        recovery_cap=3,
        error_detail="transport lost",
        completion_command_id=runner.command_id if runner else None,
        completion_finalizing_by=runner.owner if runner else None,
    )


async def current_job(db, job_id):
    job = await db.get_job(str(job_id))
    if isinstance(job["context"], str):
        job["context"] = json.loads(job["context"])
    return job


@pytest.mark.asyncio
@pytest.mark.parametrize("after", ["held", "resume", "cancel"])
async def test_lost_response_replay_never_restamps_after_owner_action(db, after):
    job_id, original = await recovery_job(db, attempts=3)
    runner = await accepted_recovery(db, original)
    first = await hold(db, original, runner)
    assert first["held_for_resume"] is True
    row = await current_job(db, job_id)
    token = row["context"]["_operator_pause_hold"]["hold_id"]
    if after == "resume":
        # Close the completion before a deliberate new owner action. The
        # retained jobs-row receipt remains sufficient for response replay.
        await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, first)
        assert await db.queue_job_for_resume(
            str(job_id),
            expected_status="paused",
            lift_operator_pause_hold=token,
            completion_commands_enabled=True,
        )
        assert await db.claim_job_for_agent(
            str(job_id),
            str(original["assigned_agent_id"]),
            completion_commands_enabled=True,
        )
    elif after == "cancel":
        await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, first)
        assert await db.cancel_job(str(job_id))
    before = await current_job(db, job_id)
    # The original processing snapshot models a response lost before the
    # effect journal recorded completion. Reconstructed reader sees no change.
    assert await hold(db, original, runner) == first
    assert await current_job(db, job_id) == before
    assert before["context"]["workspace_container"]["recovery_attempts"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["actor", "runtime", "counter", "term", "deadline", "cancel"]
)
async def test_stale_recovery_cannot_hold_a_different_owner_or_term(db, changed):
    job_id, original = await recovery_job(db, attempts=3)
    runner = (
        await accepted_recovery(db, original)
        if changed in {"term", "deadline"}
        else None
    )
    if changed == "actor":
        original["assigned_agent_id"] = str(uuid4())
    elif changed == "runtime":
        original["context"]["workspace_container"]["_runtime_incarnation"] = str(
            uuid4()
        )
    elif changed == "counter":
        original["context"]["workspace_container"]["recovery_attempts"] = 4
    elif changed == "term":
        await db.execute(
            "UPDATE job_completion_commands SET finalizing_by='successor' WHERE id=$1",
            UUID(runner.command_id),
        )
    elif changed == "deadline":
        await db.execute(
            "UPDATE job_completion_commands SET lease_expires_at=now()-interval '1 second' WHERE id=$1",
            UUID(runner.command_id),
        )
    elif changed == "cancel":
        assert await db.cancel_job(str(job_id))
    before = await current_job(db, job_id)
    assert await hold(db, original, runner) is None
    assert await current_job(db, job_id) == before


@pytest.mark.asyncio
async def test_hold_fences_internal_resume_and_exact_lift_preserves_runtime(db):
    job_id, original = await recovery_job(db, attempts=3)
    assert await hold(db, original)
    held = await current_job(db, job_id)
    token = held["context"]["_operator_pause_hold"]["hold_id"]
    assert not await db.claim_job_for_agent(
        str(job_id), str(original["assigned_agent_id"])
    )
    assert await db.queue_job_for_resume(
        str(job_id), {"queued_feedback": "urgent reply"}, expected_status="paused"
    )
    assert (await current_job(db, job_id))["context"]["_operator_pause_hold"][
        "hold_id"
    ] == token
    assert not await db.claim_job_for_agent(
        str(job_id), str(original["assigned_agent_id"])
    )
    assert not await db.queue_job_for_resume(
        str(job_id), expected_status="paused", lift_operator_pause_hold="older-token"
    )
    assert await db.queue_job_for_resume(
        str(job_id), expected_status="paused", lift_operator_pause_hold=token
    )
    resumed = await current_job(db, job_id)
    assert "_operator_pause_hold" not in resumed["context"]
    assert (
        resumed["context"]["workspace_container"]
        == held["context"]["workspace_container"]
    )
    assert await db.claim_job_for_agent(str(job_id), str(original["assigned_agent_id"]))


@pytest.mark.asyncio
async def test_stateless_hold_requires_explicit_lift_before_new_queue_lease(db):
    from shared.worker_queue import claim_worker_batch

    job_id, original = await recovery_job(db, attempts=3, lane="stateless")
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": {"type": "workspace_unavailable", "message": "transport lost"},
            "freeze_data": None,
        },
        lease_token=await db.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
        ),
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="hold-fence-proof",
    )
    runner = await _claimed_runner(db, accepted.command_id)
    outcome = await hold(db, original, runner)
    assert outcome["held_for_resume"] is True
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, job_id)
    token = held["context"]["_operator_pause_hold"]["hold_id"]
    assert await db.queue_stateless_job_for_resume(
        str(job_id),
        {"queued_feedback": "reply"},
        expected_status="paused",
        completion_commands_enabled=True,
    )
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id)
        == "done"
    )
    assert not await claim_worker_batch(
        db,
        pod_name="new-worker",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert not await db.queue_stateless_job_for_resume(
        str(job_id),
        expected_status="paused",
        lift_operator_pause_hold="old-hold",
        completion_commands_enabled=True,
    )
    assert await db.queue_stateless_job_for_resume(
        str(job_id),
        expected_status="paused",
        lift_operator_pause_hold=token,
        completion_commands_enabled=True,
    )
    claim = await claim_worker_batch(
        db,
        pod_name="new-worker",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert claim and claim.unit_id == job_id
    assert (await current_job(db, job_id))["context"]["workspace_container"] == held[
        "context"
    ]["workspace_container"]


@pytest.mark.asyncio
async def test_cancel_wins_owner_lock_before_delayed_hold(db):
    job_id, original = await recovery_job(db, attempts=3)
    task = None
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.fetchval(
                    "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id
                )
                task = asyncio.create_task(hold(db, original))
                async with asyncio.timeout(10):
                    while not await db.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' "
                        "AND query LIKE 'SELECT status::text, execution_lane, assigned_agent_id, context,%')"
                    ):
                        await asyncio.sleep(0.01)
                # Actual terminal transition and cleanup admission under the
                # existing owner guard; no direct cleanup-resource mutation.
                await conn.execute(
                    "UPDATE jobs SET status='cancelled',assigned_agent_id=NULL WHERE id=$1",
                    job_id,
                )
        assert await task is None
        assert (await current_job(db, job_id))["status"] == "cancelled"
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_legacy_report_cannot_compete_with_accepted_completion(db):
    _job_id, original = await recovery_job(db, attempts=3)
    runner = await accepted_recovery(db, original)
    assert await hold(db, original) is None
    assert (await hold(db, original, runner))["held_for_resume"] is True


@pytest.mark.asyncio
async def test_original_checkpoint_freeze_is_stashed_and_detail_bounded(db):
    job_id, original = await recovery_job(db, attempts=3)
    await db.update_job_status(
        str(job_id),
        freeze_data={"freeze_type": "checkpoint", "checkpoint_id": "original"},
    )
    assert await db.hold_exhausted_workspace_recovery(
        str(job_id),
        expected_workspace=original["context"]["workspace_container"],
        expected_agent_id=original["assigned_agent_id"],
        recovery_cap=3,
        error_detail="x" * 10000,
    )
    row = await current_job(db, job_id)
    assert row["context"]["last_freeze_data"] == {
        "freeze_type": "checkpoint",
        "checkpoint_id": "original",
    }
    assert row["freeze_data"] is None
    attention = row["context"]["_operator_pause_hold"]["attention"]
    assert attention["freeze_type"] == "workspace_recovery_attention"
    assert len(attention["detail"]) == 1000
    token = row["context"]["_operator_pause_hold"]["hold_id"]
    assert await db.queue_job_for_resume(
        str(job_id), {"queued_feedback": "held reply"}, expected_status="paused"
    )
    assert (await current_job(db, job_id))["context"]["last_freeze_data"] == row[
        "context"
    ]["last_freeze_data"]
    assert await db.queue_job_for_resume(
        str(job_id), expected_status="paused", lift_operator_pause_hold=token
    )
    resumed = await current_job(db, job_id)
    assert resumed["context"]["last_freeze_data"] == row["context"]["last_freeze_data"]
    assert resumed["context"]["last_operator_pause_hold"]["attention"] == attention


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["vm", "lite", "malformed"])
async def test_current_workspace_contract_refuses_stale_sandbox_projection(db, changed):
    job_id, original = await recovery_job(db, attempts=3)
    await db.execute(
        "UPDATE jobs SET config_override=$2::jsonb WHERE id=$1",
        job_id,
        json.dumps({"workspace": {"backend": changed}}),
    )
    before = await current_job(db, job_id)
    assert await hold(db, original) is None
    assert await current_job(db, job_id) == before
