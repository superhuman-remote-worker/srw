"""A transport error and a live TCP socket do not prove command replay is safe."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.job_completion_commands import accept_completion_command
from orchestrator.services.completion_finalizer import CompletionFinalizer
from tests.test_completion_finalizer_real_postgres import _claimed_runner
from tests import test_container_recovery_retention_real_postgres as retention
from tests import test_stateless_worker_runtime as worker

pg_dsn = retention.pg_dsn
_schema_applied = retention._schema_applied
db = retention.db
worker_runtime = worker.worker_runtime


def configure_bundle(client, *, provisioner="k8s"):
    original = client.get_claim_bundle

    async def bundle(unit_id, lease_token):
        value = await original(unit_id, lease_token)
        value["job"]["workspace_provisioner"] = provisioner
        return value

    client.get_claim_bundle = AsyncMock(side_effect=bundle)


@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [1, 5])
async def test_first_container_workspace_failure_is_reported_without_another_graph_attempt(
    worker_runtime, monkeypatch, attempts
):
    claim = worker._claim(
        input_seq=4, prior="processing", attempts=attempts, max_attempts=5
    )
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, agent, client, _, rotate, _complete, release = worker._install(
        monkeypatch, claim, final
    )
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    client.backend = "sandbox"
    configure_bundle(client)
    executor._completion_commands_enabled = True
    await executor._serve_worker_claim(claim)

    client.report_completion.assert_awaited_once()
    assert client.report_completion.await_args.args[1] == final
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    assert len(agent.process_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,provisioner,commands",
    [
        ("vm", "k8s", True),
        ("sandbox", "docker", True),
        ("sandbox", None, True),
        ("sandbox", "k8s", False),
    ],
)
async def test_other_workspace_authorities_keep_existing_retry_path(
    worker_runtime, monkeypatch, backend, provisioner, commands
):
    claim = worker._claim(attempts=1, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, agent, client, _, rotate, _, release = worker._install(
        monkeypatch, claim, final
    )
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    client.backend = backend
    executor._completion_commands_enabled = commands
    configure_bundle(client, provisioner=provisioner)
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()
    release.assert_awaited_once()
    rotate.assert_not_awaited()
    assert len(agent.process_calls) == 1


@pytest.mark.asyncio
async def test_workspace_provisioner_is_reset_before_fetching_another_bundle(
    worker_runtime, monkeypatch
):
    claim = worker._claim(attempts=1, max_attempts=5)
    executor, _, client, _, _, _, _ = worker._install(monkeypatch, claim, {})
    executor._worker_workspace_backend = "sandbox"
    executor._worker_workspace_provisioner = "k8s"

    async def failed_bundle(*args):
        assert executor._worker_workspace_backend is None
        assert executor._worker_workspace_provisioner is None
        raise RuntimeError("bundle failed before authorization")

    client.get_claim_bundle = AsyncMock(side_effect=failed_bundle)
    await executor._serve_worker_claim(claim)
    client.report_completion.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("freeze_type", ["user_question", "batch_boundary"])
async def test_human_and_batch_boundaries_keep_existing_dispositions(
    worker_runtime, monkeypatch, freeze_type
):
    claim = worker._claim(attempts=1, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "freeze_data": {"freeze_type": freeze_type},
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, _, client, _, rotate, _, release = worker._install(
        monkeypatch, claim, final
    )
    configure_bundle(client)
    await executor._serve_worker_claim(claim)
    release.assert_not_awaited()
    if freeze_type == "batch_boundary":
        rotate.assert_awaited_once()
        client.report_completion.assert_not_awaited()
    else:
        rotate.assert_not_awaited()
        client.report_completion.assert_awaited_once()
        assert client.report_completion.await_args.args[1] == final


@pytest.mark.asyncio
async def test_known_failure_report_waits_for_local_stream_join(
    worker_runtime, monkeypatch
):
    claim = worker._claim(attempts=1, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, agent, client, _, _, _, release = worker._install(
        monkeypatch, claim, final
    )
    configure_bundle(client)
    closing, closed = asyncio.Event(), asyncio.Event()
    executor._completion_commands_enabled = True

    async def stream():
        try:
            yield final
        finally:
            closing.set()
            await closed.wait()

    agent.process_job = AsyncMock(return_value=stream())
    task = asyncio.create_task(executor._serve_worker_claim(claim))
    try:
        await asyncio.wait_for(closing.wait(), 2)
        client.report_completion.assert_not_awaited()
        release.assert_not_awaited()
        closed.set()
        await asyncio.wait_for(task, 2)
        client.report_completion.assert_awaited_once()
    finally:
        closed.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_lost_accepted_report_response_uses_exact_receipt_without_replay(
    worker_runtime, monkeypatch
):
    claim = worker._claim(attempts=1, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": False,
        "error": {"type": "workspace_unavailable", "recoverable": True},
    }
    executor, agent, client, renew, rotate, complete, release = worker._install(
        monkeypatch, claim, final, report_result=False
    )
    configure_bundle(client)
    renew.side_effect = [worker._renewal(), None]
    executor._completion_commands_enabled = True
    accepted = worker._acceptance(job_status="paused", outcome={"new_status": "paused"})
    lookup = AsyncMock(return_value=accepted)
    monkeypatch.setattr(
        worker.turn_executor, "get_worker_completion_acceptance", lookup
    )
    await executor._serve_worker_claim(claim)
    assert len(agent.process_calls) == 1
    client.report_completion.assert_awaited_once()
    lookup.assert_awaited()
    release.assert_not_awaited()
    rotate.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lane,durable", [("pinned", False), ("pinned", True), ("stateless", True)]
)
@pytest.mark.parametrize("attempts", [0, 2])
async def test_alive_tcp_without_command_outcome_evidence_holds_instead_of_redispatch(
    db, lane, durable, attempts
):
    job_id, job = await retention.recovery_job(db, attempts=attempts, lane=lane)
    error = {
        "type": "workspace_unavailable",
        "recoverable": True,
        "message": "SSH connection was lost after tool admission",
    }
    authority = {"expected_status": "processing"}
    if durable:
        lease = (
            await db.fetchval(
                "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
            )
            if lane == "stateless"
            else None
        )
        accepted = await accept_completion_command(
            db._pool,
            job_id=str(job_id),
            payload={
                "should_stop": True,
                "goal_achieved": False,
                "error": error,
                "freeze_data": None,
            },
            lease_token=lease,
            agent_id=str(job["assigned_agent_id"]) if lane == "pinned" else None,
            client_report_id=str(uuid4()),
            requested_by="unknown-outcome-proof",
        )
        runner = await _claimed_runner(db, accepted.command_id)
        authority = {
            "completion_command_id": runner.command_id,
            "completion_finalizing_by": runner.owner,
        }
    dispatch = Mock()
    delete = AsyncMock(
        side_effect=AssertionError("TCP health does not authorize deletion")
    )
    outcome = await handle_pod_workspace_recovery(
        job,
        str(job_id),
        error,
        db=db,
        delete_workspace=delete,
        trigger_dispatch=dispatch,
        probe=AsyncMock(return_value=True),
        **authority,
    )
    assert outcome["held_for_resume"] is True
    held = await retention.current_job(db, job_id)
    assert held["context"]["_operator_pause_hold"]
    assert held["context"]["workspace_container"]["recovery_attempts"] == attempts + 1
    assert (
        held["context"]["workspace_container"]["_runtime_incarnation"]
        == job["context"]["workspace_container"]["_runtime_incarnation"]
    )
    dispatch.assert_not_called()
    delete.assert_not_awaited()


async def hold_unknown(db, job, runner=None):
    return await db.hold_unavailable_workspace_recovery(
        str(job["id"]),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=str(job["assigned_agent_id"])
        if job["assigned_agent_id"]
        else None,
        error_detail="SSH reply lost after command admission",
        completion_command_id=runner.command_id if runner else None,
        completion_finalizing_by=runner.owner if runner else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["actor", "runtime", "counter", "source", "term", "deadline", "cancel"]
)
async def test_unknown_outcome_hold_refuses_stale_authority(db, changed):
    job_id, original = await retention.recovery_job(db, attempts=0)
    runner = (
        await retention.accepted_recovery(db, original)
        if changed in {"term", "deadline"}
        else None
    )
    expected = deepcopy(original)
    if changed == "actor":
        expected["assigned_agent_id"] = str(uuid4())
    elif changed == "runtime":
        expected["context"]["workspace_container"]["_runtime_incarnation"] = str(
            uuid4()
        )
    elif changed == "counter":
        expected["context"]["workspace_container"]["recovery_attempts"] = 1
    elif changed == "source":
        expected["context"]["workspace_container"]["_creation_reservation_id"] = str(
            uuid4()
        )
    elif changed == "term":
        await db.execute(
            "UPDATE job_completion_commands SET finalizing_by='new-owner' WHERE id=$1::uuid",
            runner.command_id,
        )
    elif changed == "deadline":
        await db.execute(
            "UPDATE job_completion_commands SET lease_expires_at=now()-interval '1 second' WHERE id=$1::uuid",
            runner.command_id,
        )
    else:
        assert await db.cancel_job(str(job_id))
    before = await retention.current_job(db, job_id)
    assert await hold_unknown(db, expected, runner) is None
    assert await retention.current_job(db, job_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("after", ["held", "resume", "cancel"])
async def test_unknown_outcome_lost_reply_is_pure_after_owner_action(db, after):
    job_id, job = await retention.recovery_job(db, attempts=0)
    original_freeze = {"freeze_type": "user_question", "message": "original question"}
    import json

    await db.execute(
        "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
        job_id,
        json.dumps(original_freeze),
    )
    runner = await retention.accepted_recovery(db, job)
    first = await hold_unknown(db, job, runner)
    assert first["held_for_resume"] is True and first["cleanup_pending"] is False
    current = await retention.current_job(db, job_id)
    assert current["freeze_data"] is None
    assert current["context"]["last_freeze_data"] == original_freeze
    assert "recovery_cleanup" not in current["context"]["workspace_container"]
    hold_id = current["context"]["_operator_pause_hold"]["hold_id"]
    assert (
        current["context"]["_operator_pause_hold"]["source"]
        == "workspace_recovery_unavailable"
    )
    assert not await db.claim_job_for_agent(
        str(job_id), str(job["assigned_agent_id"]), completion_commands_enabled=True
    )
    if after != "held":
        await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, first)
        if after == "resume":
            assert await db.queue_job_for_resume(
                str(job_id),
                expected_status="paused",
                lift_operator_pause_hold=hold_id,
                completion_commands_enabled=True,
            )
            assert await db.claim_job_for_agent(
                str(job_id),
                str(job["assigned_agent_id"]),
                completion_commands_enabled=True,
            )
        else:
            assert await db.cancel_job(str(job_id))
    before = await retention.current_job(db, job_id)
    assert await hold_unknown(db, job, runner) == first
    assert await retention.current_job(db, job_id) == before
    assert before["context"]["workspace_container"]["recovery_attempts"] == 1


@pytest.mark.asyncio
async def test_pending_cleanup_cannot_be_acknowledged_as_a_pure_hold(db):
    job_id, job = await retention.recovery_job(db, attempts=0)
    runner = await retention.accepted_recovery(db, job)
    pending = await db.prepare_dead_workspace_recovery(
        str(job_id),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=str(job["assigned_agent_id"]),
        error_detail="transport lost",
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
    )
    assert pending["cleanup_pending"] is True
    before = await retention.current_job(db, job_id)
    assert await hold_unknown(db, job, runner) is None
    assert await retention.current_job(db, job_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"should_stop": True, "goal_achieved": False, "error": {"type": "job_error"}},
        {
            "should_stop": True,
            "goal_achieved": True,
            "error": {"type": "workspace_unavailable"},
        },
    ],
)
async def test_attention_mode_requires_the_actual_accepted_typed_failure(db, payload):
    job_id, job = await retention.recovery_job(db, attempts=0)
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        payload={**payload, "freeze_data": None},
        agent_id=str(job["assigned_agent_id"]),
        lease_token=None,
        client_report_id=str(uuid4()),
        requested_by="typed-payload-negative",
    )
    runner = await _claimed_runner(db, accepted.command_id)
    before = await retention.current_job(db, job_id)
    assert await hold_unknown(db, job, runner) is None
    assert await retention.current_job(db, job_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["live", "checkpoint"])
@pytest.mark.parametrize(
    "goal,reported",
    [(False, True), (None, False), (True, False), ("false", False), (0, False)],
)
async def test_container_typed_stop_preserves_explicit_goal_wire_values(
    worker_runtime, monkeypatch, source, goal, reported
):
    claim = worker._claim(attempts=1, max_attempts=5)
    final = {
        "should_stop": True,
        "goal_achieved": goal,
        "error": {"type": "workspace_unavailable", "recoverable": True},
        "freeze_data": None,
    }
    if source == "checkpoint":
        # The exact checkpoint envelope is wire authority over live fields.
        final = {
            **final,
            "goal_achieved": False,
            "completion_report_payload": dict(final),
        }
    executor, _, client, _, rotate, _, release = worker._install(
        monkeypatch, claim, final
    )
    configure_bundle(client)
    executor._completion_commands_enabled = True
    await executor._serve_worker_claim(claim)
    assert client.report_completion.await_count == int(reported)
    assert release.await_count == int(not reported)
    rotate.assert_not_awaited()
