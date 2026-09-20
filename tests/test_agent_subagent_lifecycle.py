"""Agent/worker lifecycle fences around durable background subagents."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import agent.agent as agent_module
import agent.api.persistent_app as persistent_app
from agent.agent import UniversalAgent
from agent.api.lease_context import LeaseHandle
from agent.api.turn_executor import StatelessTurnExecutor
from shared.subagent_lifecycle import (
    SubagentAbandonError,
    SubagentQuiescenceError,
    SubagentRecoveryError,
)


class _DurableLedger:
    async def list_live(self, parent_job_id):  # pragma: no cover - capability seam
        del parent_job_id
        return []


class _RecordingRuntime:
    def __init__(
        self,
        events: list[str],
        *,
        recover_error: Exception | None = None,
        quiesce_error: Exception | None = None,
        abandon_error: Exception | None = None,
    ):
        self.events = events
        self.ledger = _DurableLedger()
        self.recover_calls = 0
        self.quiesce_calls = 0
        self.abandon_calls = 0
        self.recover_error = recover_error
        self.quiesce_error = quiesce_error
        self.abandon_error = abandon_error

    async def recover_orphans(self):
        self.recover_calls += 1
        self.events.append("recover")
        if self.recover_error is not None:
            raise self.recover_error
        return []

    async def quiesce(self, reason):
        self.quiesce_calls += 1
        self.events.append(f"quiesce:{reason}")
        if self.quiesce_error is not None:
            raise self.quiesce_error

    async def abandon(self, reason):
        self.abandon_calls += 1
        self.events.append(f"abandon:{reason}")
        if self.abandon_error is not None:
            raise self.abandon_error


def _streaming_agent(runtime: _RecordingRuntime) -> UniversalAgent:
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._tool_context = SimpleNamespace(subagent_runtime=runtime)
    agent._graph = SimpleNamespace(_srw_memory_service=None)
    agent._jobs_processed = 0
    agent._defer_job_cleanup = True
    agent._current_job_id = "job-1"
    return agent


@pytest.mark.asyncio
async def test_stream_recovers_once_before_graph_and_quiesces_before_return(
    monkeypatch,
):
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)

    async def graph_stream(graph, graph_input, config):
        del graph, graph_input, config
        events.append("graph")
        yield {"should_stop": True}

    monkeypatch.setattr(agent_module, "run_graph_with_streaming", graph_stream)
    stream = agent._process_job_streaming({}, {"configurable": {}})

    assert await anext(stream) == {"should_stop": True}
    assert events == ["recover", "graph"]
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["recover", "graph", "quiesce:parent stream ended"]
    await agent._recover_subagent_orphans()
    await agent._quiesce_subagent_runtime("defensive cleanup")
    assert runtime.recover_calls == 1
    assert runtime.quiesce_calls == 1


@pytest.mark.asyncio
async def test_generator_close_joins_graph_provider_before_quiescing():
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)

    class _Graph:
        async def astream(self, graph_input, *, config, stream_mode):
            del graph_input, config, stream_mode
            try:
                events.append("graph")
                yield {"iteration": 1}
                await asyncio.Future()
            finally:
                events.append("graph_closed")

    agent._graph = _Graph()
    stream = agent._process_job_streaming({}, {"configurable": {}})
    assert await anext(stream) == {"iteration": 1}

    await stream.aclose()

    assert events[-2:] == ["graph_closed", "quiesce:parent stream ended"]
    assert runtime.quiesce_calls == 1


@pytest.mark.asyncio
async def test_graph_error_is_typed_then_quiesced_before_stream_end(monkeypatch):
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)

    async def broken_graph(graph, graph_input, config):
        del graph, graph_input, config
        events.append("graph_error")
        raise RuntimeError("provider exploded")
        yield  # pragma: no cover - declares an async generator

    monkeypatch.setattr(agent_module, "run_graph_with_streaming", broken_graph)
    stream = agent._process_job_streaming({}, {"configurable": {}})

    error_state = await anext(stream)
    assert error_state["should_stop"] is True
    assert error_state["error"]["message"] == "provider exploded"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == [
        "recover",
        "graph_error",
        "quiesce:parent stream ended",
    ]


@pytest.mark.asyncio
async def test_terminal_mirror_recovers_orphans_without_starting_graph():
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)
    stream = agent._yield_error_state({"should_stop": True})

    assert await anext(stream) == {"should_stop": True}
    assert events == ["recover"]
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["recover", "quiesce:parent stream ended"]


@pytest.mark.asyncio
async def test_authority_loss_abandon_is_idempotent_and_supersedes_quiesce():
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)

    await agent.abandon_worker_subagents("lease stolen")
    await agent.abandon_worker_subagents("duplicate cleanup")
    await agent._quiesce_subagent_runtime("must not write")

    assert events == ["abandon:lease stolen"]
    assert runtime.abandon_calls == 1
    assert runtime.quiesce_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        ("recover", SubagentRecoveryError),
        ("quiesce", SubagentQuiescenceError),
        ("abandon", SubagentAbandonError),
    ],
)
async def test_lifecycle_failures_are_typed_and_remain_retryable(operation, expected):
    events: list[str] = []
    runtime = _RecordingRuntime(
        events,
        recover_error=RuntimeError("recover failed")
        if operation == "recover"
        else None,
        quiesce_error=(
            RuntimeError("quiesce failed") if operation == "quiesce" else None
        ),
        abandon_error=(
            RuntimeError("abandon failed") if operation == "abandon" else None
        ),
    )
    agent = _streaming_agent(runtime)

    with pytest.raises(expected):
        if operation == "recover":
            await agent._recover_subagent_orphans()
        elif operation == "quiesce":
            await agent._quiesce_subagent_runtime("first")
        else:
            await agent.abandon_worker_subagents("first")

    # A failed transition never sets the object-identity guard, so the driver
    # can retry the exact same runtime instead of silently treating it settled.
    if operation == "recover":
        runtime.recover_error = None
        await agent._recover_subagent_orphans()
        assert runtime.recover_calls == 2
    elif operation == "quiesce":
        runtime.quiesce_error = None
        await agent._quiesce_subagent_runtime("retry")
        assert runtime.quiesce_calls == 2
    else:
        runtime.abandon_error = None
        await agent.abandon_worker_subagents("retry")
        assert runtime.abandon_calls == 2


@pytest.mark.asyncio
async def test_worker_cleanup_quiesces_before_retiring_shell_admission():
    events: list[str] = []
    runtime = _RecordingRuntime(events)
    agent = _streaming_agent(runtime)
    backend = object()
    agent._worker_workspace_backend = lambda: backend
    agent._retire_worker_shell_admission = lambda value: events.append(
        f"retire:{value is backend}"
    )
    agent._scrub_worker_claim_locals = AsyncMock(
        side_effect=lambda: events.append("scrub")
    )
    agent._worker_finalization_held = False
    agent._worker_finalization_backend = None
    agent._worker_terminal_shell_cleanup = None
    agent._shell_manager = None

    await agent.cleanup_worker_claim(preserve_shell=True)

    assert events[:3] == [
        "quiesce:worker claim cleanup",
        "retire:True",
        "scrub",
    ]


@pytest.mark.asyncio
async def test_lease_loss_abandons_before_worker_cleanup(monkeypatch):
    events: list[str] = []
    agent = SimpleNamespace()
    agent.abandon_worker_subagents = AsyncMock(
        side_effect=lambda reason: events.append(f"abandon:{reason}")
    )
    agent.cleanup_worker_claim = AsyncMock(
        side_effect=lambda *, preserve_shell: events.append(f"cleanup:{preserve_shell}")
    )
    monkeypatch.setattr(persistent_app, "_agent", agent)

    executor = StatelessTurnExecutor.__new__(StatelessTurnExecutor)
    executor._worker_retirement_lock = asyncio.Lock()
    executor._worker_workspace_backend = "sandbox"
    executor._lease = LeaseHandle()
    executor._lease.update("job-1", 7)
    executor._lease.mark_lost()

    await executor._cleanup_worker_runtime(preserve_shell=True)

    assert events == [
        "abandon:worker lease authority lost",
        "cleanup:True",
    ]
    agent.abandon_worker_subagents.assert_awaited_once_with(
        "worker lease authority lost"
    )
