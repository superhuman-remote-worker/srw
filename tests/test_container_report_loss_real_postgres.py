"""Real PG + fenced checkpoint, modeled tool/network, actual stream driver."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from langgraph.graph import END, START, StateGraph

from agent.agent import UniversalAgent
from agent.api.lease_context import current_lease
from agent.core.fenced_checkpointer import (
    make_fenced_checkpointer,
    close_fenced_checkpointer_pool,
)
from shared.workspace_contract import workspace_runtime_authority_digest
from orchestrator.services import run_queue_reaper
from shared import worker_queue
from shared.run_queue import ClaimedUnit
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from tests import test_container_recovery_retention_real_postgres as retention
from tests import test_stateless_worker_runtime as worker
from tests.test_container_workspace_unknown_outcome import configure_bundle

pg_dsn = retention.pg_dsn
_schema_applied = retention._schema_applied
db = retention.db
worker_runtime = worker.worker_runtime


async def exact_claim(db):
    job_id, row = await retention.recovery_job(db, attempts=0, lane="stateless")
    q = await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
    claim = worker_queue.WorkerClaim(
        unit=ClaimedUnit(
            **{
                k: q[k]
                for k in (
                    "unit_id",
                    "unit_kind",
                    "fair_key",
                    "lease_token",
                    "input_seq",
                    "consumed_seq",
                    "attempts_since_completion",
                    "leased_until",
                    "control_input_seq",
                    "control_consumed_seq",
                )
            }
        ),
        prior_job_status="created",
        resume=False,
        max_attempts=q["max_attempts"],
    )
    digest = workspace_runtime_authority_digest(row, vm_mode="external")
    assert digest is not None
    async with db.acquire() as conn:
        assert await worker_queue.record_worker_bundle_authorized(
            conn,
            job_id=job_id,
            lease_token=claim.lease_token,
            authority_digest=digest,
        )
    return claim


async def install_actual_stream(
    db,
    pg_dsn,
    claim,
    monkeypatch,
    effects,
    *,
    die=False,
    explicit_goal=False,
    effect_entered=None,
    park_in_tool=False,
    node_error=None,
    stream_finally_error=None,
):
    executor, fixture_agent, client, _, _, _, release = worker._install(
        monkeypatch,
        claim,
        {},
        report_result=False,
    )
    configure_bundle(client)
    monkeypatch.setenv("LANGGRAPH_STRICT_MSGPACK", "true")
    executor._completion_commands_enabled = True
    fixture_agent.postgres_conn = db._pool
    current_lease.set(executor._lease)
    monkeypatch.setattr(
        worker.turn_executor, "renew_worker_batch", worker_queue.renew_worker_batch
    )
    monkeypatch.setattr(
        worker.turn_executor,
        "complete_worker_batch",
        worker_queue.complete_worker_batch,
    )

    async def actual_release(connection, **kwargs):
        return await worker_queue.release_worker_batch(
            connection, **kwargs, backoff_base_seconds=0
        )

    release.side_effect = actual_release
    saver = await make_fenced_checkpointer(
        pg_dsn, unit_id=str(claim.unit_id), lease_token=claim.lease_token
    )

    async def uncertain_tool(state):
        effects.append(claim.lease_token)
        if effect_entered is not None:
            effect_entered.set()
        if park_in_tool:
            await asyncio.Event().wait()
        if node_error is not None:
            raise node_error
        if die:
            raise asyncio.CancelledError(
                "modeled process loss after remote tool admission"
            )
        raise WorkspaceUnavailableError(
            "modeled response loss after external side effect"
        )

    graph = StateGraph(worker._WorkerFrontierState)
    graph.add_node("uncertain_tool", uncertain_tool)
    graph.add_edge(START, "uncertain_tool")
    graph.add_edge("uncertain_tool", END)
    graph = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": str(claim.unit_id)}}
    stream_agent = UniversalAgent.__new__(UniversalAgent)
    stream_agent._graph = graph
    stream_agent._current_job_id = str(claim.unit_id)
    stream_agent._jobs_processed = 0
    stream_agent._workspace_manager = None
    stream_agent._defer_job_cleanup = True
    stream_agent._recover_subagent_orphans = AsyncMock()
    stream_agent._quiesce_subagent_runtime = AsyncMock(side_effect=stream_finally_error)

    async def process_job(*args, **kwargs):
        fixture_agent.process_calls.append((args, kwargs))
        graph_input = (
            None
            if claim.resume
            else {"initialized": True, "iteration": 0, "should_stop": False}
        )
        if claim.resume:
            assert (
                await stream_agent._arm_worker_batch(
                    job_id=str(claim.unit_id),
                    graph_input=None,
                    thread_config=config,
                    target_wall_seconds=60,
                    min_wall_seconds=0,
                    iteration_cap=10,
                )
                is None
            )
        stream = stream_agent._process_job_streaming(graph_input, config)
        if not explicit_goal:
            return stream

        async def explicit_false_stop():
            async for value in stream:
                if value.get("should_stop") is True:
                    value = {**value, "goal_achieved": False}
                yield value

        return explicit_false_stop()

    fixture_agent.process_job = process_job
    return executor, client, graph, config, release


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "false",
        "report_exception",
        "retirement_exception",
        "delayed_retirement",
        "paused_before_false",
    ],
)
async def test_unaccepted_typed_error_does_not_replay_uncheckpointed_tool(
    db,
    pg_dsn,
    worker_runtime,
    monkeypatch,
    failure,
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    effects = []
    claim = None
    previous_lease = current_lease.get()
    try:
        claim = await exact_claim(db)
        executor, client, graph, config, release = await install_actual_stream(
            db,
            pg_dsn,
            claim,
            monkeypatch,
            effects,
        )
        if failure == "report_exception":
            client.report_completion.side_effect = RuntimeError("report transport lost")
        if failure == "retirement_exception":
            worker.pa._agent.quiesce_worker_workspace_recovery = AsyncMock(
                side_effect=RuntimeError("local retirement unknown")
            )
        if failure == "paused_before_false":

            async def pause_before_reply(*args, **kwargs):
                assert await db.pause_stateless_job(
                    str(claim.unit_id),
                    expected_lease_token=claim.lease_token,
                    completion_commands_enabled=True,
                )
                return False

            client.report_completion.side_effect = pause_before_reply
        if failure == "delayed_retirement":
            retiring = asyncio.Event()
            retire_allowed = asyncio.Event()

            async def delayed_retirement():
                retiring.set()
                await retire_allowed.wait()

            worker.pa._agent.quiesce_worker_workspace_recovery = AsyncMock(
                side_effect=delayed_retirement
            )
            serving = asyncio.create_task(executor._serve_worker_claim(claim))
            try:
                await asyncio.wait_for(retiring.wait(), 5)
                assert not serving.done()
                assert (await retention.current_job(db, claim.unit_id))["context"][
                    "_worker_execution_hold"
                ]["phase"] == "pending"
                release.assert_not_awaited()
            finally:
                retire_allowed.set()
                await asyncio.wait_for(serving, 5)
        else:
            await executor._serve_worker_claim(claim)
        client.report_completion.assert_awaited_once()
        assert (
            client.report_completion.await_args.args[1]["error"]["type"]
            == "workspace_unavailable"
        )
        before = await graph.aget_state(config)
        assert before.next == ("uncertain_tool",)
        assert not before.values.get("error")
        assert not before.values.get("completion_report_payload")
        assert (
            await db.fetchval(
                "SELECT count(*) FROM job_completion_commands WHERE job_id=$1",
                claim.unit_id,
            )
            == 0
        )
        q = await db.fetchrow(
            "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", claim.unit_id
        )
        print(
            "PRE_ACCEPTANCE_RELEASE",
            dict(q),
            "frontier",
            before.next,
            "effects",
            effects,
        )
        successor = await worker_queue.claim_worker_batch(
            db,
            pod_name="successor",
            affinity_grace_seconds=0,
            completion_commands_enabled=True,
        )
        if successor:
            second, _, _, _, _ = await install_actual_stream(
                db, pg_dsn, successor, monkeypatch, effects
            )
            await second._serve_worker_claim(successor)
        print(
            "AFTER_SUCCESSOR",
            "effects",
            effects,
            "first_release_calls",
            release.await_count,
        )
        assert effects == [claim.lease_token], (
            "same pending external-effect node executed again without acceptance or explicit Resume"
        )
        assert q["state"] == "parked" and q["lease_token"] == claim.lease_token + 1
        assert (await retention.current_job(db, claim.unit_id))["context"][
            "_worker_execution_hold"
        ]["phase"] == "pending"
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
@pytest.mark.parametrize("vm_recovery_flag", ["false", "true"])
async def test_authorized_container_process_loss_cannot_publish_fresh_graph_claim(
    db,
    pg_dsn,
    worker_runtime,
    monkeypatch,
    vm_recovery_flag,
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", vm_recovery_flag)
    effects = []
    claim = serving = None
    previous_lease = current_lease.get()
    try:
        claim = await exact_claim(db)
        entered = asyncio.Event()
        executor, client, graph, config, release = await install_actual_stream(
            db,
            pg_dsn,
            claim,
            monkeypatch,
            effects,
            effect_entered=entered,
            park_in_tool=True,
        )
        # A task cancellation models worker loss. Raising CancelledError from
        # inside a node is a distinct LangGraph NodeCancelledError failure.
        serving = asyncio.create_task(executor._serve_worker_claim(claim))
        await asyncio.wait_for(entered.wait(), 5)
        serving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await serving
        client.report_completion.assert_not_awaited()
        release.assert_not_awaited()
        before = await graph.aget_state(config)
        assert before.next == ("uncertain_tool",) and not before.values.get("error")
        async with db.acquire() as conn:
            evidence = await worker_queue.get_worker_attempt_disposition(
                conn, job_id=claim.unit_id, lease_token=claim.lease_token
            )
            assert evidence.bundle_authorized and evidence.authority_digest
            assert await worker_queue.renew_worker_batch(
                conn,
                unit_id=claim.unit_id,
                lease_token=claim.lease_token,
                lease_ttl_seconds=0.01,
            )
        await asyncio.sleep(0.03)
        async with db.acquire() as conn:
            await run_queue_reaper.reap_cycle(conn, grace_seconds=0)
        q = await db.fetchrow(
            "SELECT state,lease_token,park_reason,run_after FROM run_queue WHERE unit_id=$1",
            claim.unit_id,
        )
        row = await retention.current_job(db, claim.unit_id)
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_workspace_recoveries WHERE owner_id=$1",
                claim.unit_id,
            )
            == 0
        )
        print(
            "POST_BUNDLE_LOSS",
            vm_recovery_flag,
            dict(q),
            "job",
            row["status"],
            "hold",
            row["context"].get("_operator_pause_hold"),
        )
        assert q["state"] == "parked" and "_operator_pause_hold" in row["context"], (
            "authorized container attempt was queued after loss with no no-replay hold"
        )
    finally:
        if serving is not None and not serving.done():
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)
        current_lease.set(previous_lease)
        await close_fenced_checkpointer_pool()
        if claim is not None:
            await db.cancel_job(str(claim.unit_id))
            await worker_queue.cancel_queued_worker_batch(
                db._pool, job_id=claim.unit_id
            )
