"""A full workspace disk holds with its usage, or resumes on a grown volume.

knowledge-base/knowledge/issues/workspace_disk_full_pauses_job_as_transport_lost.md:
a worker's ``workspace_disk_full`` report is not a lost transport. Against a
real jobs row, run queue and completion command it must never say "transport
lost", must not count as a recovery attempt, and resumes through ordinary
stateless admission only when the volume grew and no command was running.
"""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_finalizer import CompletionFinalizer
from orchestrator.services.job_completion_commands import accept_completion_command
from shared.workspace_disk import WorkspaceVolumeGrowth
from tests.test_completion_finalizer_real_postgres import _claimed_runner
from tests import test_container_recovery_retention_real_postgres as retention

pg_dsn = retention.pg_dsn
_schema_applied = retention._schema_applied
db = retention.db

GIB = 1024**3


def _error(*, command_in_flight=False):
    return {
        "type": "workspace_unavailable",
        "recoverable": True,
        "message": "Remote tmux create or observe incarnation-fenced stateless "
        "session failed with exit code 82: workspace disk is full",
        "reason": "workspace_disk_full",
        "disk": {
            "size_bytes": 10_501_771_264,
            "used_bytes": 9_984_000_000,
            "available_bytes": 0,
        },
        "command_in_flight": command_in_flight,
    }


async def _report(db, error, *, attempts=2):
    job_id, job = await retention.recovery_job(db, attempts=attempts, lane="stateless")
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": error,
            "freeze_data": None,
        },
        lease_token=await db.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
        ),
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="disk-full-proof",
    )
    runner = await _claimed_runner(db, accepted.command_id)
    return job_id, job, runner


async def _settle(db, job, job_id, error, runner, growth):
    dispatch = Mock()
    outcome = await handle_pod_workspace_recovery(
        job,
        str(job_id),
        error,
        db=db,
        delete_workspace=AsyncMock(side_effect=AssertionError("no cleanup")),
        trigger_dispatch=dispatch,
        probe=AsyncMock(side_effect=AssertionError("no probe")),
        grow_workspace_volume=AsyncMock(return_value=growth),
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
    )
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    return outcome, dispatch


async def _admitted(db, job_id) -> bool:
    admitted, _ = await db.admit_stateless_worker_job(
        str(job_id), fair_key=None, priority=5, completion_commands_enabled=True
    )
    return bool(admitted)


@pytest.mark.asyncio
async def test_full_disk_without_growth_holds_with_its_usage(db):
    error = _error()
    job_id, job, runner = await _report(db, error)
    refused = WorkspaceVolumeGrowth("refused", 20 * GIB, "the cluster refused it")

    outcome, dispatch = await _settle(db, job, job_id, error, runner, refused)

    assert outcome["held_for_resume"] is True
    action = outcome["actions"][0]
    assert "transport" not in action and "outcome is unknown" not in action
    assert action.startswith(
        "workspace disk is full (9.3 GiB of 9.8 GiB used, 0 B free); "
        "growing the volume failed: the cluster refused it"
    )
    held = await retention.current_job(db, job_id)
    assert held["status"] == "paused"
    assert held["error_message"] == action
    hold = held["context"]["_operator_pause_hold"]
    assert hold["source"] == "workspace_disk_full"
    assert hold["attention"]["reason"] == "workspace_disk_full"
    assert hold["attention"]["disk"]["available_bytes"] == 0
    workspace = held["context"]["workspace_container"]
    assert workspace["recovery_attempts"] == 2  # not a recovery attempt
    assert "disk_growth" not in workspace  # nothing grew, so nothing is spent
    assert workspace["status"] == "ready"
    dispatch.assert_not_called()
    assert not await _admitted(db, job_id)


@pytest.mark.asyncio
async def test_grown_volume_resumes_through_ordinary_admission(db):
    from shared.worker_queue import claim_worker_batch

    error = _error()
    job_id, job, runner = await _report(db, error)
    resized = WorkspaceVolumeGrowth("resized", 20 * GIB)

    outcome, dispatch = await _settle(db, job, job_id, error, runner, resized)

    assert outcome["held_for_resume"] is False
    assert outcome["actions"][0].endswith(
        "grew the volume to 20 GiB; workspace and checkpoints retained; the job resumes"
    )
    dispatch.assert_called_once_with()
    resumed = await retention.current_job(db, job_id)
    assert resumed["status"] == "paused"
    assert resumed["assigned_agent_id"] is None
    assert "_operator_pause_hold" not in resumed["context"]
    assert resumed["error_message"] == job.get("error_message")
    workspace = resumed["context"]["workspace_container"]
    assert workspace["disk_growth"] == {"outcome": "resized", "size_bytes": 20 * GIB}
    assert workspace["recovery_attempts"] == 2
    assert (
        workspace["_runtime_incarnation"]
        == job["context"]["workspace_container"]["_runtime_incarnation"]
    )

    assert await _admitted(db, job_id)
    claim = await claim_worker_batch(
        db,
        pod_name="after-growth",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert claim and claim.unit_id == job_id


@pytest.mark.asyncio
async def test_grown_volume_with_a_command_in_flight_keeps_the_hold(db):
    error = _error(command_in_flight=True)
    job_id, job, runner = await _report(db, error)

    outcome, dispatch = await _settle(
        db, job, job_id, error, runner, WorkspaceVolumeGrowth("resized", 20 * GIB)
    )

    assert outcome["held_for_resume"] is True
    assert "explicit Resume required" in outcome["actions"][0]
    held = await retention.current_job(db, job_id)
    assert held["context"]["_operator_pause_hold"]["source"] == "workspace_disk_full"
    # Grown is grown: the next full disk on this job holds instead.
    assert held["context"]["workspace_container"]["disk_growth"]["outcome"] == "resized"
    dispatch.assert_not_called()
    assert not await _admitted(db, job_id)


@pytest.mark.asyncio
async def test_lost_response_replays_the_resume_without_another_growth(db):
    error = _error()
    job_id, job, runner = await _report(db, error)
    first, _ = await _settle(
        db, job, job_id, error, runner, WorkspaceVolumeGrowth("resized", 20 * GIB)
    )
    after = await retention.current_job(db, job_id)
    grow = AsyncMock(side_effect=AssertionError("a replay must not grow again"))
    dispatch = Mock()

    replay = await handle_pod_workspace_recovery(
        after,
        str(job_id),
        error,
        db=db,
        delete_workspace=AsyncMock(side_effect=AssertionError("no cleanup")),
        trigger_dispatch=dispatch,
        probe=AsyncMock(side_effect=AssertionError("no probe")),
        grow_workspace_volume=grow,
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
    )

    assert replay == first
    dispatch.assert_called_once_with()
