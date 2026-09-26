"""Dead transport admits exact preserve cleanup without permitting command replay."""

import asyncio
import copy
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_finalizer import CompletionFinalizer
from orchestrator.services.completion_control import CompletionControl
from shared.operator_pause_hold import operator_pause_lift_token
from tests.test_container_recovery_retention_real_postgres import (
    _schema_applied,  # noqa: F401
    accepted_recovery,
    current_job,
    db as recovery_db,
    pg_dsn,  # noqa: F401
    recovery_job,
)


# Reuse the same real PostgreSQL fixture and migrations as the retention suite.
db = recovery_db


async def prepare(db, job, runner=None):
    return await db.prepare_dead_workspace_recovery(
        str(job["id"]),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=job["assigned_agent_id"],
        error_detail="transport lost during an interrupted command",
        completion_command_id=runner.command_id if runner else None,
        completion_finalizing_by=runner.owner if runner else None,
    )


async def intent_for(db, job):
    return await db.get_managed_repository_workspace_cleanup_intent(
        str(job["id"]),
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=job["context"]["workspace_container"][
            "_runtime_incarnation"
        ],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_actual_dead_probe_holds_instead_of_speculative_deleted(db, durable):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original) if durable else None
    dispatch = Mock()
    delete = AsyncMock(return_value=False)
    kwargs = (
        {
            "completion_command_id": runner.command_id,
            "completion_finalizing_by": runner.owner,
        }
        if runner
        else {"expected_status": "processing"}
    )
    if durable:
        # Without the typed cleanup service, retain the committed pending
        # obligation instead of accepting the old arbitrary delete callback.
        with pytest.raises(RuntimeError, match="cleanup.*pending"):
            await handle_pod_workspace_recovery(
                original,
                str(job_id),
                {"type": "workspace_unavailable"},
                db=db,
                delete_workspace=delete,
                trigger_dispatch=dispatch,
                probe=AsyncMock(return_value=False),
                **kwargs,
            )
    else:
        outcome = await handle_pod_workspace_recovery(
            original,
            str(job_id),
            {"type": "workspace_unavailable"},
            db=db,
            delete_workspace=delete,
            trigger_dispatch=dispatch,
            probe=AsyncMock(return_value=False),
            **kwargs,
        )
        assert outcome["held_for_resume"] is True
    current = await current_job(db, job_id)
    assert current["status"] == "paused"
    assert (
        current["context"]["_operator_pause_hold"]["source"]
        == "workspace_recovery_unavailable"
    )
    assert current["context"]["workspace_container"]["status"] == (
        "retiring_process_zero" if durable else "ready"
    )
    assert bool(await intent_for(db, original)) is durable
    dispatch.assert_not_called()
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_admission_is_atomic_and_exact_replay_does_not_increment(db, monkeypatch):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original)
    before_admission = await current_job(db, job_id)
    original_prepare = db.prepare_managed_repository_workspace_cleanup_intent

    async def interrupted(*args, **kwargs):
        intent = await original_prepare(*args, **kwargs)
        assert intent["resource_policy"] == "preserve"
        assert (await current_job(db, job_id))["context"]["workspace_container"][
            "status"
        ] == "retiring_process_zero"
        raise RuntimeError("fault between intent and pause")

    monkeypatch.setattr(
        db, "prepare_managed_repository_workspace_cleanup_intent", interrupted
    )
    with pytest.raises(RuntimeError, match="fault between"):
        await prepare(db, original, runner)
    assert await intent_for(db, original) is None
    assert await current_job(db, job_id) == before_admission
    monkeypatch.setattr(
        db, "prepare_managed_repository_workspace_cleanup_intent", original_prepare
    )
    first = await prepare(db, original, runner)
    assert first["cleanup_pending"] is True
    current = await current_job(db, job_id)
    assert await prepare(db, original, runner) == first
    assert await current_job(db, job_id) == current
    assert current["context"]["workspace_container"]["recovery_attempts"] == 1
    intent = await intent_for(db, original)
    assert intent["resource_policy"] == "preserve" and intent["settled_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        "actor",
        "counter",
        "contract",
        "term",
        "deadline",
        "cancel",
        "runtime_source",
        "generation",
        "claim",
        "missing_uid",
    ],
)
async def test_stale_admission_has_no_hold_or_cleanup(db, changed):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original)
    if changed in {"runtime_source", "generation", "claim", "missing_uid"}:
        original = copy.deepcopy(original)
        key = {
            "runtime_source": "_runtime_incarnation",
            "generation": "_canvas_workspace_generation",
            "claim": "_creation_claim_token",
            "missing_uid": "_runtime_incarnation",
        }[changed]
        if changed == "missing_uid":
            original["context"]["workspace_container"].pop(key)
        else:
            original["context"]["workspace_container"][key] = str(uuid4())
    elif changed == "actor":
        await db.execute("UPDATE jobs SET assigned_agent_id=NULL WHERE id=$1", job_id)
    elif changed == "counter":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{workspace_container,recovery_attempts}','2') WHERE id=$1",
            job_id,
        )
    elif changed == "contract":
        await db.execute(
            'UPDATE jobs SET config_override=\'{"workspace":{"backend":"vm"}}\' WHERE id=$1',
            job_id,
        )
    elif changed == "term":
        runner.owner = "stale-owner"
    elif changed == "deadline":
        await db.execute(
            "UPDATE job_completion_commands SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=$1",
            UUID(runner.command_id),
        )
    else:
        assert await db.cancel_job(str(job_id))
    before = await current_job(db, job_id)
    intent_before = await intent_for(db, before)
    assert await prepare(db, original, runner) is None
    assert await current_job(db, job_id) == before
    assert await intent_for(db, before) == intent_before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["queue", "shed", "claim", "stateless_shed", "pinned_shed"]
)
async def test_pending_cleanup_refuses_resume_before_hold_or_context_changes(db, path):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original)
    outcome = await prepare(db, original, runner)
    assert outcome["cleanup_pending"]
    # Completion's lease is not the pending-cleanup fence: prove all paths
    # still refuse after its finalizer is no longer excluding controls.
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    if path == "stateless_shed":
        await db.execute(
            "UPDATE jobs SET execution_lane='stateless' WHERE id=$1", job_id
        )
    control_claim = None
    if path == "pinned_shed":
        control_claim = await CompletionControl(db, AsyncMock()).claim_job(
            str(job_id),
            source="missing_workspace_resume",
            expected_status="paused",
            expected_lane="pinned",
        )
    held = await current_job(db, job_id)
    token = operator_pause_lift_token(held)
    if path == "queue":
        changed = await db.queue_job_for_resume(
            str(job_id), expected_status="paused", lift_operator_pause_hold=token
        )
    elif path == "shed":
        changed = await db.shed_workspace_context(str(job_id), "workspace_container")
    elif path == "claim":
        changed = await db.claim_job_for_agent(
            str(job_id),
            str(original["assigned_agent_id"]),
            lift_operator_pause_hold=token,
        )
    elif path == "stateless_shed":
        changed = await db.prepare_stateless_job_for_workspace_resume(
            str(job_id),
            "workspace_container",
            expected_status="paused",
            lift_operator_pause_hold=token,
        )
    else:
        changed = await db.prepare_pinned_job_for_workspace_resume(
            str(job_id),
            "workspace_container",
            expected_status="paused",
            completion_control_claim_id=str(control_claim.claim_id),
            lift_operator_pause_hold=token,
        )
    assert changed is False
    assert await current_job(db, job_id) == held


@pytest.mark.asyncio
async def test_cancel_promotes_policy_but_old_recovery_receipt_has_no_effect_authority(
    db,
):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original)
    await prepare(db, original, runner)
    before = await intent_for(db, original)
    held = await current_job(db, job_id)
    receipt = held["context"]["workspace_container"]["recovery_cleanup"]
    assert await db.cancel_job(str(job_id))
    promoted = await intent_for(db, original)
    assert promoted["id"] == before["id"]
    assert promoted["resource_policy"] == "terminal_reclaim"
    assert await db.workspace_recovery_cleanup_is_current(receipt) is False
    cancelled = await current_job(db, job_id)
    assert await prepare(db, original, runner) is None
    assert await current_job(db, job_id) == cancelled


@pytest.mark.asyncio
async def test_stateless_exact_accepted_queue_term_stays_closed_under_pending_hold(db):
    from orchestrator.services.job_completion_commands import accept_completion_command
    from tests.test_completion_finalizer_real_postgres import _claimed_runner

    job_id, original = await recovery_job(db, attempts=0, lane="stateless")
    token = await db.fetchval(
        "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
    )
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        lease_token=token,
        agent_id=None,
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": {"type": "workspace_unavailable"},
            "freeze_data": None,
        },
        client_report_id=str(uuid4()),
        requested_by="dead-probe-proof",
    )
    runner = await _claimed_runner(db, accepted.command_id)
    outcome = await prepare(db, original, runner)
    assert outcome["cleanup_pending"] is True
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id)
        == "done"
    )
    held = await current_job(db, job_id)
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    assert not await db.queue_stateless_job_for_resume(
        str(job_id),
        expected_status="paused",
        lift_operator_pause_hold=operator_pause_lift_token(held),
    )
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id)
        == "done"
    )
    assert await current_job(db, job_id) == held


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["cancel", "recovery"])
async def test_cancel_and_recovery_serialize_under_real_owner_lock(
    db, monkeypatch, winner
):
    job_id, original = await recovery_job(db, attempts=0)
    runner = await accepted_recovery(db, original)
    task = None
    release = asyncio.Event()
    try:
        if winner == "cancel":
            async with db.transaction_scope():
                async with db.acquire() as conn:
                    await conn.fetchval(
                        "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id
                    )
                    task = asyncio.create_task(prepare(db, original, runner))
                    async with asyncio.timeout(10):
                        while True:
                            await conn.execute("SELECT pg_stat_clear_snapshot()")
                            if await conn.fetchval(
                                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT status::text,execution_lane,assigned_agent_id,context,%')"
                            ):
                                break
                            await asyncio.sleep(0.01)
                    assert await db.cancel_job(str(job_id))
            assert await task is None
        else:
            prepared = asyncio.Event()
            original_prepare = db.prepare_managed_repository_workspace_cleanup_intent

            async def wait_before_owner_pause(*args, **kwargs):
                result = await original_prepare(*args, **kwargs)
                prepared.set()
                await release.wait()
                return result

            monkeypatch.setattr(
                db,
                "prepare_managed_repository_workspace_cleanup_intent",
                wait_before_owner_pause,
            )
            task = asyncio.create_task(prepare(db, original, runner))
            await asyncio.wait_for(prepared.wait(), 10)
            cancel = asyncio.create_task(db.cancel_job(str(job_id)))
            try:
                async with asyncio.timeout(10):
                    while not await db.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%jobs%')"
                    ):
                        await asyncio.sleep(0.01)
                release.set()
                assert (await task)["cleanup_pending"] is True
                assert await cancel
            finally:
                if not cancel.done():
                    cancel.cancel()
                    await asyncio.gather(cancel, return_exceptions=True)
        assert (await current_job(db, job_id))["status"] == "cancelled"
        assert (await intent_for(db, original))["resource_policy"] == "terminal_reclaim"
    finally:
        release.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stateless_report_without_durable_command_refuses_explicitly(db):
    job_id, original = await recovery_job(db, attempts=0, lane="stateless")
    before_queue = await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
    with pytest.raises(RuntimeError, match="disposition term"):
        await handle_pod_workspace_recovery(
            original,
            str(job_id),
            {"type": "workspace_unavailable"},
            db=db,
            delete_workspace=AsyncMock(side_effect=AssertionError("untyped cleanup")),
            trigger_dispatch=Mock(side_effect=AssertionError("automatic redispatch")),
            probe=AsyncMock(return_value=False),
            expected_status="processing",
        )
    assert await current_job(db, job_id) == original
    assert (
        await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        == before_queue
    )
    assert await intent_for(db, original) is None
