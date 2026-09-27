"""Real PG/checkpoint: node interruption differs from outer task cancellation."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from agent.api.lease_context import current_lease
from agent.core.fenced_checkpointer import close_fenced_checkpointer_pool
from shared import worker_queue
from shared.subagent_lifecycle import SubagentLifecycleError
from tests import test_container_report_loss_real_postgres as stream
from tests import test_container_recovery_retention_real_postgres as retention
from tests import test_stateless_worker_runtime as worker
from tests.test_container_workspace_unknown_outcome import configure_bundle

pg_dsn = stream.pg_dsn
_schema_applied = stream._schema_applied
db = stream.db
worker_runtime = stream.worker_runtime


@pytest.mark.asyncio
@pytest.mark.parametrize("vm_recovery_flag", ["false", "true"])
@pytest.mark.parametrize("report_result", [False, True])
async def test_internal_node_cancellation_cannot_replay_or_report_pending_effect(
    db, pg_dsn, worker_runtime, monkeypatch, vm_recovery_flag, report_result
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", vm_recovery_flag)
    effects = []
    claim = None
    previous_lease = current_lease.get()
    try:
        claim = await stream.exact_claim(db)
        before = await retention.current_job(db, claim.unit_id)
        executor, client, graph, config, release = await stream.install_actual_stream(
            db, pg_dsn, claim, monkeypatch, effects, die=True
        )
        client.report_completion.return_value = report_result
        await executor._serve_worker_claim(claim)
        snapshot = await graph.aget_state(config)
        queue = await db.fetchrow(
            "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", claim.unit_id
        )
        assert snapshot.next == ("uncertain_tool",)
        assert not snapshot.values.get("error")
        assert not snapshot.values.get("completion_report_payload")
        assert "_worker_node_cancelled" not in snapshot.values
        assert (
            await db.fetchval(
                "SELECT count(*) FROM job_completion_commands WHERE job_id=$1",
                claim.unit_id,
            )
            == 0
        )
        successor = await worker_queue.claim_worker_batch(
            db,
            pod_name="node-cancel-successor",
            affinity_grace_seconds=0,
            completion_commands_enabled=True,
        )
        # This native successor actually re-executed the pending node before
        # the correction. Keep the causal assertion, not just an error type.
        if successor is not None:
            second, _, _, _, _ = await stream.install_actual_stream(
                db, pg_dsn, successor, monkeypatch, effects, die=True
            )
            await second._serve_worker_claim(successor)
        assert effects == [claim.lease_token], (
            "unfinished external-effect node replayed"
        )
        assert successor is None
        assert (
            queue["state"] == "parked" and queue["lease_token"] == claim.lease_token + 1
        )
        row = await retention.current_job(db, claim.unit_id)
        marker = row["context"]["_worker_execution_hold"]
        assert marker["reason"] == "node_execution_interrupted"
        assert marker["phase"] == "pending" and marker["executor_pod_uid"] is None
        assert (
            row["context"]["workspace_container"]
            == before["context"]["workspace_container"]
        )
        assert not await db.queue_stateless_job_for_resume(
            str(claim.unit_id),
            expected_status="paused",
            lift_operator_pause_hold=row["context"]["_operator_pause_hold"]["hold_id"],
            completion_commands_enabled=True,
        )
        client.report_completion.assert_not_awaited()
        release.assert_not_awaited()
    finally:
        current_lease.set(previous_lease)
        await close_fenced_checkpointer_pool()
        if claim is not None:
            await db.cancel_job(str(claim.unit_id))
            await worker_queue.cancel_queued_worker_batch(
                db._pool, job_id=claim.unit_id
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["finally_generic", "finally_child", "hold", "retirement"]
)
async def test_interruption_survives_finally_hold_and_retirement_failure(
    db, pg_dsn, worker_runtime, monkeypatch, failure
):
    effects = []
    claim = None
    previous_lease = current_lease.get()
    try:
        claim = await stream.exact_claim(db)
        final_error = None
        if failure == "finally_generic":
            final_error = RuntimeError("modeled stream finalizer fault")
        elif failure == "finally_child":
            final_error = SubagentLifecycleError("modeled child join failure")
        executor, client, graph, config, release = await stream.install_actual_stream(
            db,
            pg_dsn,
            claim,
            monkeypatch,
            effects,
            die=True,
            stream_finally_error=final_error,
        )
        if failure == "hold":
            monkeypatch.setattr(
                worker.turn_executor,
                "hold_interrupted_container_worker",
                AsyncMock(side_effect=RuntimeError("modeled DB outage")),
            )
        if failure == "retirement":
            worker.pa._agent.quiesce_worker_workspace_recovery = AsyncMock(
                side_effect=RuntimeError("modeled retirement failure")
            )
        await executor._serve_worker_claim(claim)
        client.report_completion.assert_not_awaited()
        release.assert_not_awaited()
        assert effects == [claim.lease_token]
        assert (await graph.aget_state(config)).next == ("uncertain_tool",)
        state = await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", claim.unit_id
        )
        assert state == ("leased" if failure == "hold" else "parked")
        if failure == "hold":
            async with db.acquire() as conn:
                assert await worker_queue.renew_worker_batch(
                    conn,
                    unit_id=claim.unit_id,
                    lease_token=claim.lease_token,
                    lease_ttl_seconds=0.01,
                )
            await asyncio.sleep(0.03)
            async with db.acquire() as conn:
                await stream.run_queue_reaper.reap_cycle(conn, grace_seconds=0)
            assert (
                await db.fetchval(
                    "SELECT state FROM run_queue WHERE unit_id=$1", claim.unit_id
                )
                == "parked"
            )
    finally:
        current_lease.set(previous_lease)
        await close_fenced_checkpointer_pool()
        if claim is not None:
            await db.cancel_job(str(claim.unit_id))
            await worker_queue.cancel_queued_worker_batch(
                db._pool, job_id=claim.unit_id
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("control", ["vm", "virtual", "docker", "legacy", "generic"])
async def test_other_authorities_and_untyped_errors_keep_generic_report(
    db, pg_dsn, worker_runtime, monkeypatch, control
):
    effects = []
    claim = None
    previous_lease = current_lease.get()
    try:
        monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
        claim = await stream.exact_claim(db)
        executor, client, _, _, release = await stream.install_actual_stream(
            db,
            pg_dsn,
            claim,
            monkeypatch,
            effects,
            die=True,
            node_error=RuntimeError("NodeCancelledError: cancelled node")
            if control == "generic"
            else None,
        )
        if control in {"vm", "virtual"}:
            client.backend = control
        if control == "docker":
            configure_bundle(client, provisioner="docker")
        if control == "legacy":
            executor._completion_commands_enabled = False
        hold = AsyncMock(return_value="held")
        monkeypatch.setattr(
            worker.turn_executor, "hold_interrupted_container_worker", hold
        )
        await executor._serve_worker_claim(claim)
        hold.assert_not_awaited()
        client.report_completion.assert_awaited_once()
        sent = client.report_completion.await_args.args[1]
        assert sent["error"]["type"] == "job_error"
        assert "_worker_node_cancelled" not in sent
        release.assert_awaited_once()
    finally:
        current_lease.set(previous_lease)
        await close_fenced_checkpointer_pool()
        if claim is not None:
            await db.cancel_job(str(claim.unit_id))
            await worker_queue.cancel_queued_worker_batch(
                db._pool, job_id=claim.unit_id
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary", ["prebundle", "wrong_token", "successor", "cancelled", "accepted"]
)
async def test_interruption_hold_keeps_exact_native_authority_and_disposition(
    db, boundary
):
    from tests.test_worker_execution_hold_real_postgres import accept

    job_id = None
    try:
        if boundary == "prebundle":
            job_id, _ = await retention.recovery_job(db, attempts=0, lane="stateless")
            token = await db.fetchval(
                "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
            )
        else:
            claim = await stream.exact_claim(db)
            job_id, token = claim.unit_id, claim.lease_token
        if boundary == "successor":
            await worker_queue.release_worker_batch(
                db._pool,
                unit_id=job_id,
                lease_token=token,
                park_on_exhaustion=True,
                backoff_base_seconds=0,
            )
            successor = await worker_queue.claim_worker_batch(
                db,
                pod_name="native-successor",
                affinity_grace_seconds=0,
                completion_commands_enabled=True,
            )
            assert successor is not None and successor.unit_id == job_id
        elif boundary == "cancelled":
            assert await db.cancel_job(str(job_id))
        elif boundary == "accepted":
            await accept(db._pool, claim)
        before_job = await retention.current_job(db, job_id)
        before_queue = dict(
            await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        )
        decision = await worker_queue.hold_interrupted_container_worker(
            db,
            unit_id=job_id,
            lease_token=token + (boundary == "wrong_token"),
        )
        assert decision == (
            "not_applicable" if boundary == "prebundle" else "superseded"
        )
        assert await retention.current_job(db, job_id) == before_job
        assert (
            dict(await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id))
            == before_queue
        )
        assert "_worker_execution_hold" not in before_job["context"]
    finally:
        if job_id is not None:
            await db.cancel_job(str(job_id))
            await worker_queue.cancel_queued_worker_batch(db._pool, job_id=job_id)


@pytest.mark.asyncio
async def test_same_tick_pause_does_not_discard_ready_interruption_signal(
    worker_runtime, monkeypatch
):
    claim = worker._claim(prior="processing")
    executor, agent, client, _, _, complete, release = worker._install(
        monkeypatch, claim, {}
    )
    configure_bundle(client)
    executor._completion_commands_enabled = True
    hold = AsyncMock(return_value="held")
    monkeypatch.setattr(worker.turn_executor, "hold_interrupted_container_worker", hold)

    async def process_job(*args, **kwargs):
        async def states():
            executor._worker_preempt_status = "paused"
            executor._worker_preempted.set()
            yield {
                "error": {"type": "job_error"},
                "should_stop": True,
                "_worker_node_cancelled": True,
            }

        return states()

    agent.process_job = process_job
    await executor._serve_worker_claim(claim)
    hold.assert_awaited_once()
    client.report_completion.assert_not_awaited()
    complete.assert_not_awaited()
    release.assert_not_awaited()
