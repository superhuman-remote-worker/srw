"""Native worker give-up uses a distinct durable, exact-lease END envelope."""

from copy import deepcopy
import json
from types import MethodType
from uuid import uuid4

import httpx
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from agent.agent import UniversalAgent
from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.api.turn_executor import StatelessTurnExecutor
from agent.api.orchestrator_client import OrchestratorClient
from agent.core.fenced_checkpointer import FencedAsyncPostgresSaver
from agent.graph import checkpoint_completion_report


class State(TypedDict, total=False):
    should_stop: bool
    goal_achieved: bool
    error: dict | None
    freeze_data: dict | None
    client_report_id: str | None
    completion_report_payload: dict | None
    iteration: int
    worker_batch_started_at: float
    worker_batch_start_iteration: int
    worker_batch_target_wall_seconds: float
    worker_batch_min_wall_seconds: float
    worker_batch_iteration_cap: int | None
    worker_resume_id: str | None


class MemoryFencedSaver(FencedAsyncPostgresSaver):
    """Use real local lease binding with memory storage; SQL fence is tested on PG."""

    def __init__(self, job, token):
        self.unit_id = job
        self.lease_token = token
        self.memory = InMemorySaver()
        self.serde = self.memory.serde
        self.writes = 0

    async def aget_tuple(self, config):
        return await self.memory.aget_tuple(config)

    async def alist(self, *args, **kwargs):
        async for value in self.memory.alist(*args, **kwargs):
            yield value

    async def aput(self, *args, **kwargs):
        self._bound_handle()
        self.writes += 1
        return await self.memory.aput(*args, **kwargs)

    async def aput_writes(self, *args, **kwargs):
        self._bound_handle()
        return await self.memory.aput_writes(*args, **kwargs)

    def get_next_version(self, current, channel):
        return self.memory.get_next_version(current, channel)


def outage():
    state = dict(
        should_stop=True,
        goal_achieved=False,
        error={"type": "llm_unavailable", "recoverable": True},
        freeze_data={"freeze_type": "llm_unavailable", "reason": "endpoint offline"},
    )
    state.update(checkpoint_completion_report(state))
    return state


def graph(saver):
    workflow = StateGraph(State)
    workflow.add_node("checkpoint_completion_report", checkpoint_completion_report)
    workflow.set_entry_point("checkpoint_completion_report")
    workflow.add_edge("checkpoint_completion_report", END)
    return workflow.compile(checkpointer=saver)


async def bind_checkpoint_agent(agent, claim, final, executor):
    job = str(claim.unit_id)
    executor._lease.update(job, claim.lease_token)
    reset = current_lease.set(executor._lease)
    saver = MemoryFencedSaver(job, claim.lease_token)
    agent._current_job_id = job
    agent._worker_lease_token = claim.lease_token
    agent._checkpointer = saver
    agent._graph = graph(saver)
    agent._worker_thread_config = {"configurable": {"thread_id": job}}
    agent.checkpoint_worker_retry_exhaustion = MethodType(
        UniversalAgent.checkpoint_worker_retry_exhaustion, agent
    )
    await agent._graph.ainvoke(final, agent._worker_thread_config)
    return reset


async def setup_agent(state=None):
    job = str(uuid4())
    token = 7
    handle = LeaseHandle()
    handle.update(job, token)
    reset = current_lease.set(handle)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._current_job_id = job
    agent._worker_lease_token = token
    agent._checkpointer = MemoryFencedSaver(job, token)
    agent._graph = graph(agent._checkpointer)
    agent._worker_thread_config = {"configurable": {"thread_id": job}}
    state = state or outage()
    await agent._graph.ainvoke(state, agent._worker_thread_config)
    exhausted = StatelessTurnExecutor._worker_retry_exhausted_state(
        state, attempts=5, max_attempts=5
    )
    return agent, exhausted, handle, reset


@pytest.mark.asyncio
async def test_new_giveup_is_distinct_durable_and_replays_byte_identically():
    agent, exhausted, handle, reset = await setup_agent()
    old = deepcopy(exhausted["completion_report_payload"])
    old_id = exhausted["client_report_id"]
    try:
        saved = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
        )
        payload, source = StatelessTurnExecutor._worker_completion_wire_payload(saved)
        assert (
            source == "checkpoint_envelope"
            and payload["error"]["type"] == "worker_retry_exhausted"
        )
        assert saved["client_report_id"] != old_id
        assert payload["freeze_data"]["prior_client_report_id"] == old_id
        assert len(payload["freeze_data"]["prior_report_payload_sha256"]) == 64
        assert "prior_completion_report_payload" not in payload["freeze_data"]
        assert exhausted["completion_report_payload"] == old
        writes = agent._checkpointer.writes
        # Another claim binds a new saver to the same durable canonical graph.
        successor = MemoryFencedSaver(handle.unit_id, 8)
        successor.memory = agent._checkpointer.memory
        handle.update(handle.unit_id, 8)
        agent._worker_lease_token = 8
        agent._checkpointer = successor
        agent._graph = graph(successor)
        reloaded = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=8, terminal_state=exhausted
        )
        assert reloaded["client_report_id"] == saved["client_report_id"]
        assert (
            reloaded["completion_report_payload"] == payload and successor.writes == 0
        )
        terminal = await agent._arm_worker_batch(
            job_id=handle.unit_id,
            graph_input=None,
            thread_config=agent._worker_thread_config,
            target_wall_seconds=10,
            min_wall_seconds=None,
            iteration_cap=None,
            retry_exhausted=True,
        )
        assert (
            terminal["client_report_id"] == saved["client_report_id"]
            and successor.writes == 0
        )
        assert (
            writes > 0
            and not (await agent._graph.aget_state(agent._worker_thread_config)).next
        )
    finally:
        current_lease.reset(reset)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["before_commit", "after_commit", "readback"])
async def test_ambiguous_save_reentry_keeps_or_creates_only_one_durable_giveup(stage):
    agent, exhausted, handle, reset = await setup_agent()
    update = agent._graph.aupdate_state
    read = agent._graph.aget_state
    committed = None
    reads = 0

    async def fail_update(*args, **kwargs):
        nonlocal committed
        if stage == "after_commit":
            await update(*args, **kwargs)
            committed = dict((await read(agent._worker_thread_config)).values)
        raise TimeoutError("injected checkpoint ambiguity")

    async def fail_read(*args, **kwargs):
        nonlocal reads, committed
        reads += 1
        if reads == 2:
            committed = dict((await read(agent._worker_thread_config)).values)
            raise TimeoutError("readback unavailable")
        return await read(*args, **kwargs)

    try:
        if stage == "readback":
            agent._graph.aget_state = fail_read
        else:
            agent._graph.aupdate_state = fail_update
        with pytest.raises(TimeoutError):
            await agent.checkpoint_worker_retry_exhaustion(
                job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
            )
        agent._graph.aupdate_state = update
        agent._graph.aget_state = read
        budget = await agent._arm_worker_batch(
            job_id=handle.unit_id,
            graph_input=None,
            thread_config=agent._worker_thread_config,
            target_wall_seconds=10,
            min_wall_seconds=None,
            iteration_cap=None,
            retry_exhausted=True,
        )
        next_state = (
            StatelessTurnExecutor._worker_retry_exhausted_state(
                budget, attempts=6, max_attempts=5
            )
            if committed is None
            else budget
        )
        result = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=7, terminal_state=next_state
        )
        if committed is not None:
            assert (
                result["client_report_id"] == committed["client_report_id"]
                and result["completion_report_payload"]
                == committed["completion_report_payload"]
            )
        assert (
            result["completion_report_payload"]["error"]["type"]
            == "worker_retry_exhausted"
        )
    finally:
        current_lease.reset(reset)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["job", "token", "saver", "config", "handle", "lost"]
)
async def test_foreign_or_lost_claim_cannot_write_giveup(changed):
    agent, exhausted, handle, reset = await setup_agent()
    writes = agent._checkpointer.writes
    job = handle.unit_id
    token = 7
    try:
        if changed == "job":
            job = str(uuid4())
        elif changed == "token":
            token = 8
        elif changed == "saver":
            agent._checkpointer = InMemorySaver()
        elif changed == "config":
            agent._worker_thread_config = {"configurable": {"thread_id": str(uuid4())}}
        elif changed == "handle":
            handle.update(str(uuid4()), 7)
        else:
            handle.mark_lost()
        with pytest.raises(LeaseLostError):
            await agent.checkpoint_worker_retry_exhaustion(
                job_id=job, lease_token=token, terminal_state=exhausted
            )
        assert agent._graph.checkpointer.writes == writes
    finally:
        current_lease.reset(reset)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["success", "human"])
async def test_genuine_canonical_end_cannot_be_overwritten_by_giveup(kind):
    original = outage()
    original.update(
        goal_achieved=kind == "success",
        error=None,
        freeze_data={
            "freeze_type": "job_complete" if kind == "success" else "blocking_message"
        },
    )
    original.update(client_report_id=None, completion_report_payload=None)
    original.update(checkpoint_completion_report(original))
    agent, exhausted, handle, reset = await setup_agent(original)
    writes = agent._checkpointer.writes
    try:
        saved = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
        )
        assert (
            saved["client_report_id"] == original["client_report_id"]
            and saved["completion_report_payload"]
            == original["completion_report_payload"]
        )
        assert agent._checkpointer.writes == writes
    finally:
        current_lease.reset(reset)


@pytest.mark.asyncio
async def test_actual_http_client_retries_exact_durable_giveup_json():
    agent, exhausted, handle, reset = await setup_agent()
    requests = []

    async def receive(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            raise httpx.ReadError("modeled response loss", request=request)
        return httpx.Response(200, json={"new_status": "paused"})

    client = OrchestratorClient(
        orchestrator_url="https://completion.invalid",
        pod_ip="127.0.0.1",
        pod_port=8001,
        hostname="owned-test",
        config_name="worker_base",
        pid=1,
    )
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    try:
        saved = await agent.checkpoint_worker_retry_exhaustion(
            job_id=handle.unit_id, lease_token=7, terminal_state=exhausted
        )
        assert (
            await client.report_completion(handle.unit_id, saved, lease_token=7)
            is False
        )
        # Outer state cannot change the frozen operation after a lost reply.
        retry = deepcopy(saved)
        retry.update(
            error={"type": "llm_unavailable", "recoverable": True}, freeze_data=None
        )
        assert (
            await client.report_completion(handle.unit_id, retry, lease_token=7) is True
        )
        assert (
            requests[0]
            == requests[1]
            == {
                **saved["completion_report_payload"],
                "client_report_id": saved["client_report_id"],
                "lease_token": 7,
            }
        )
        assert requests[0]["error"]["type"] == "worker_retry_exhausted"
        assert requests[0]["error"]["recoverable"] is False
    finally:
        await client.close()
        current_lease.reset(reset)
