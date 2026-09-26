"""A worker budget does not grant terminal cleanup of a lost container workspace."""

import json
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.job_completion_commands import accept_completion_command
from orchestrator.services.completion_finalizer import CompletionFinalizer
from tests.test_completion_finalizer_real_postgres import _claimed_runner
from tests.test_container_recovery_retention_real_postgres import (
    recovery_job,
    current_job,
)
from tests.test_vm_workspace_recovery_real_postgres import (
    checkpoint_row_counts,
    prepare_checkpoint_rows,
)

from tests import test_container_recovery_retention_real_postgres as retention_helpers

pg_dsn = retention_helpers.pg_dsn
_schema_applied = retention_helpers._schema_applied
db = retention_helpers.db


async def accepted_exhaustion(db, *, cause="workspace_unavailable", attempts=0):
    job_id, job = await recovery_job(db, attempts=attempts, lane="stateless")
    payload = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {
            "type": "worker_retry_exhausted",
            "recoverable": False,
            "message": "worker queue budget exhausted",
            "cause": {"type": cause, "recoverable": True},
        },
        "freeze_data": {
            "freeze_type": "worker_retry_exhausted",
            "attempts": 5,
            "max_attempts": 5,
        },
    }
    lease_token = await db.fetchval(
        "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
    )
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        payload=payload,
        lease_token=lease_token,
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="worker-budget-retention-proof",
    )
    assert accepted.queue_terminalized
    return job_id, job, payload, await _claimed_runner(db, accepted.command_id)


@pytest.mark.asyncio
async def test_worker_budget_hold_retains_original_work_without_container_attempt(db):
    job_id, job, payload, runner = await accepted_exhaustion(db)
    await prepare_checkpoint_rows(db._pool, job_id, checkpoint_count=1)
    original_freeze = {"freeze_type": "checkpoint", "checkpoint_id": "retained"}
    await db.update_job_status(str(job_id), freeze_data=original_freeze)
    delete = AsyncMock(side_effect=AssertionError("budget grants no cleanup"))
    probe = AsyncMock(return_value=False)
    dispatch = Mock()

    outcome = await handle_pod_workspace_recovery(
        job,
        str(job_id),
        payload["error"],
        db=db,
        delete_workspace=delete,
        trigger_dispatch=dispatch,
        probe=probe,
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
    )

    assert outcome["held_for_resume"] is True
    current = await db.get_job(str(job_id))
    context = current["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert current["status"] == "paused"
    assert current["freeze_data"] is None
    assert context["last_freeze_data"] == original_freeze
    assert context["workspace_container"]["recovery_attempts"] == 0
    assert (
        context["workspace_container"]["_runtime_incarnation"]
        == job["context"]["workspace_container"]["_runtime_incarnation"]
    )
    assert await checkpoint_row_counts(db._pool, job_id) == (1, 1, 1)
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1)",
        job_id,
    )
    delete.assert_not_awaited()
    probe.assert_not_awaited()
    dispatch.assert_not_called()


async def hold_worker(db, job, runner):
    return await db.hold_exhausted_workspace_recovery(
        str(job["id"]),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=None,
        recovery_cap=3,
        error_detail="typed workspace failure",
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
        worker_queue_exhausted=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    ["cause", "term", "lease", "queue_token", "runtime", "counter", "contract"],
)
async def test_worker_mode_cannot_bypass_accepted_payload_or_current_authority(
    db, changed
):
    job_id, job, _, runner = await accepted_exhaustion(
        db, cause="llm_unavailable" if changed == "cause" else "workspace_unavailable"
    )
    if changed == "term":
        await db.execute(
            "UPDATE job_completion_commands SET finalizing_by='successor' WHERE id=$1",
            UUID(runner.command_id),
        )
    elif changed == "lease":
        await db.execute(
            "UPDATE job_completion_commands SET lease_expires_at=now()-interval '1 second' WHERE id=$1",
            UUID(runner.command_id),
        )
    elif changed == "queue_token":
        await db.execute(
            "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1", job_id
        )
    elif changed == "runtime":
        job["context"]["workspace_container"]["_runtime_incarnation"] = str(uuid4())
    elif changed == "counter":
        job["context"]["workspace_container"]["recovery_attempts"] = 3
    elif changed == "contract":
        await db.execute(
            'UPDATE jobs SET config_override=\'{"workspace":{"backend":"vm"}}\'::jsonb WHERE id=$1',
            job_id,
        )
    before = await current_job(db, job_id)
    assert await hold_worker(db, job, runner) is None
    assert await current_job(db, job_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("after", ["resume", "cancel"])
async def test_worker_hold_lost_reply_is_pure_after_new_owner_disposition(db, after):
    job_id, job, _, runner = await accepted_exhaustion(db)
    first = await hold_worker(db, job, runner)
    assert first["held_for_resume"]
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, first)
    if after == "resume":
        held = await current_job(db, job_id)
        assert await db.queue_stateless_job_for_resume(
            str(job_id),
            expected_status="paused",
            completion_commands_enabled=True,
            lift_operator_pause_hold=held["context"]["_operator_pause_hold"]["hold_id"],
        )
        from shared.worker_queue import claim_worker_batch

        successor = await claim_worker_batch(
            db,
            pod_name="explicit-resume-worker",
            affinity_grace_seconds=0,
            completion_commands_enabled=True,
        )
        assert successor and successor.unit_id == job_id
    else:
        assert await db.cancel_job(str(job_id))
    before = await current_job(db, job_id)
    assert await hold_worker(db, job, runner) == first
    assert await current_job(db, job_id) == before
