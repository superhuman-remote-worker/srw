"""Full-schema PG fence and canonical END replay for native Worker give-up."""

from copy import deepcopy
from uuid import uuid4

import pytest

from agent.agent import UniversalAgent
from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.api.turn_executor import StatelessTurnExecutor
from agent.core.fenced_checkpointer import (
    close_fenced_checkpointer_pool,
    make_fenced_checkpointer,
)
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _schema_applied as _schema_fixture,
    db as _db_fixture,
    pg_dsn as _dsn_fixture,
)
from tests.test_worker_retry_exhaustion_checkpoint import graph, outage

_schema_applied = _schema_fixture
db = _db_fixture
pg_dsn = _dsn_fixture


async def setup(db, pg_dsn, monkeypatch):
    monkeypatch.setenv("LANGGRAPH_STRICT_MSGPACK", "true")
    job = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id, description, status, execution_lane) "
            "VALUES($1, 'native give-up checkpoint', 'created', 'stateless')",
            job,
        )
        await conn.execute(
            "INSERT INTO run_queue(unit_id, unit_kind, state, lease_token, "
            "attempts_since_completion, max_attempts, leased_by, leased_until) "
            "VALUES($1, 'worker_batch', 'leased', 7, 5, 5, 'owned-test', "
            "now() + interval '5 minutes')",
            job,
        )
    handle = LeaseHandle()
    handle.update(str(job), 7)
    reset = current_lease.set(handle)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._current_job_id = str(job)
    agent._worker_lease_token = 7
    agent._checkpointer = await make_fenced_checkpointer(
        pg_dsn, unit_id=str(job), lease_token=7
    )
    agent._graph = graph(agent._checkpointer)
    agent._retain_compiled_worker_checkpointer()
    agent._worker_thread_config = {"configurable": {"thread_id": str(job)}}
    initial = outage()
    await agent._graph.ainvoke(initial, agent._worker_thread_config)
    exhausted = StatelessTurnExecutor._worker_retry_exhausted_state(
        initial, attempts=5, max_attempts=5
    )
    return agent, exhausted, handle, reset


@pytest.mark.asyncio
async def test_pg_committed_then_error_successor_replays_same_end_without_work(
    db,
    pg_dsn,
    monkeypatch,
):
    agent, exhausted, handle, reset = await setup(db, pg_dsn, monkeypatch)
    update = agent._graph.aupdate_state
    committed = None

    async def save_then_error(*args, **kwargs):
        nonlocal committed
        await update(*args, **kwargs)
        committed = dict(
            (await agent._graph.aget_state(agent._worker_thread_config)).values
        )
        raise TimeoutError("modeled disconnect after committed checkpoint")

    try:
        agent._graph.aupdate_state = save_then_error
        with pytest.raises(TimeoutError):
            await agent.checkpoint_worker_retry_exhaustion(
                job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
            )
        assert committed["client_report_id"] != exhausted["client_report_id"]
        async with db.acquire() as conn:
            before = await conn.fetchval(
                "SELECT count(*) FROM checkpoints WHERE thread_id=$1", handle.unit_id
            )
            await conn.execute(
                "UPDATE run_queue SET lease_token=8, attempts_since_completion=6 "
                "WHERE unit_id=$1::uuid",
                handle.unit_id,
            )
        handle.update(handle.unit_id, 8)
        agent._worker_lease_token = 8
        agent._checkpointer = await make_fenced_checkpointer(
            pg_dsn, unit_id=handle.unit_id, lease_token=8
        )
        agent._graph = graph(agent._checkpointer)
        agent._retain_compiled_worker_checkpointer()
        terminal = await agent._arm_worker_batch(
            job_id=handle.unit_id,
            graph_input=None,
            thread_config=agent._worker_thread_config,
            target_wall_seconds=10,
            min_wall_seconds=None,
            iteration_cap=None,
            retry_exhausted=True,
        )
        replay = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=8, terminal_state=terminal
        )
        assert replay["client_report_id"] == committed["client_report_id"]
        assert (
            replay["completion_report_payload"]
            == committed["completion_report_payload"]
        )
        assert (
            replay["completion_report_payload"]["error"]["type"]
            == "worker_retry_exhausted"
        )
        async with db.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM checkpoints WHERE thread_id=$1",
                    handle.unit_id,
                )
                == before
            )
    finally:
        current_lease.reset(reset)
        await close_fenced_checkpointer_pool()


@pytest.mark.asyncio
async def test_pg_lease_rotation_before_giveup_save_leaves_original_end_unchanged(
    db,
    pg_dsn,
    monkeypatch,
):
    agent, exhausted, handle, reset = await setup(db, pg_dsn, monkeypatch)
    before = deepcopy(
        dict((await agent._graph.aget_state(agent._worker_thread_config)).values)
    )
    try:
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE run_queue SET lease_token=8 WHERE unit_id=$1::uuid",
                handle.unit_id,
            )
        with pytest.raises(LeaseLostError, match="run_queue lease rejected"):
            await agent.checkpoint_worker_retry_exhaustion(
                job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
            )
        assert handle.lost.is_set()
        assert (
            dict((await agent._graph.aget_state(agent._worker_thread_config)).values)
            == before
        )
    finally:
        current_lease.reset(reset)
        await close_fenced_checkpointer_pool()
