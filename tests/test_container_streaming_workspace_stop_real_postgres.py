"""Production typed streaming stop crosses the wire-default completion boundary."""

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


async def install_actual_stream(db, pg_dsn, claim, monkeypatch):
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

    async def actual_release(connection, **kwargs):
        return await worker_queue.release_worker_batch(
            connection, **kwargs, backoff_base_seconds=0
        )

    release.side_effect = actual_release
    saver = await make_fenced_checkpointer(
        pg_dsn, unit_id=str(claim.unit_id), lease_token=claim.lease_token
    )

    async def uncertain_tool(state):
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
    stream_agent._quiesce_subagent_runtime = AsyncMock()

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
        return stream_agent._process_job_streaming(graph_input, config)

    fixture_agent.process_job = process_job
    return executor, client, graph, config, release


@pytest.mark.asyncio
async def test_actual_streaming_workspace_error_is_reported_without_required_false_goal(
    db,
    pg_dsn,
    worker_runtime,
    monkeypatch,
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    claim = None
    previous_lease = current_lease.get()
    try:
        claim = await exact_claim(db)
        executor, client, graph, config, release = await install_actual_stream(
            db, pg_dsn, claim, monkeypatch
        )
        await executor._serve_worker_claim(claim)
        state = await graph.aget_state(config)
        assert state.next == ("uncertain_tool",) and not state.values.get("error")
        client.report_completion.assert_awaited_once()
        reported = client.report_completion.await_args.args[1]
        assert reported["should_stop"] is True
        assert reported["error"]["type"] == "workspace_unavailable"
        assert "goal_achieved" not in reported
        # This test covers routing into the exact report protocol. The
        # separate report-loss module verifies the unaccepted-response hold.
    finally:
        current_lease.set(previous_lease)
        await close_fenced_checkpointer_pool()
        if claim is not None:
            await db.cancel_job(str(claim.unit_id))
            await worker_queue.cancel_queued_worker_batch(
                db._pool, job_id=claim.unit_id
            )
