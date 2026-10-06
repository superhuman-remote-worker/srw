"""Worker retains the actual strict-msgpack compiled, lease-fenced saver."""

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from agent.agent import UniversalAgent
from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.core.fenced_checkpointer import FencedAsyncPostgresSaver


def test_nonstateless_graph_binding_keeps_existing_saver():
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._worker_lease_token = None
    source = object()
    agent._checkpointer = source
    agent._graph = SimpleNamespace(checkpointer=object())
    agent._retain_compiled_worker_checkpointer()
    assert agent._checkpointer is source


async def binding_agent():
    job = str(uuid4())
    source = FencedAsyncPostgresSaver(object(), unit_id=job, lease_token=5)
    source.serde = JsonPlusSerializer(allowed_msgpack_modules=None)
    compiled = source.with_allowlist({("binding_fixture", "State")})
    assert compiled is not source
    handle = LeaseHandle()
    handle.update(job, 5)
    reset = current_lease.set(handle)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._current_job_id = job
    agent._worker_lease_token = 5
    agent._checkpointer = source
    agent._graph = SimpleNamespace(checkpointer=compiled)
    return agent, source, compiled, handle, reset


@pytest.mark.asyncio
async def test_rebuilt_compiled_saver_keeps_same_live_claim_and_write_binding():
    agent, source, compiled, handle, reset = await binding_agent()
    try:
        agent._retain_compiled_worker_checkpointer()
        assert agent._checkpointer is compiled
        rebuilt = compiled.with_allowlist({("binding_fixture", "UpgradeState")})
        assert rebuilt is not compiled
        agent._graph = SimpleNamespace(checkpointer=rebuilt)
        agent._retain_compiled_worker_checkpointer()
        assert agent._checkpointer is rebuilt
        assert rebuilt.conn is source.conn and rebuilt.lock is source.lock
        assert rebuilt._bound_handle() is handle
    finally:
        current_lease.reset(reset)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    [
        "source_job",
        "source_token",
        "compiled_job",
        "compiled_token",
        "connection",
        "lock",
        "callback",
        "retry_attempts",
        "retry_delay",
        "missing_connection",
        "missing_source_job",
        "not_fenced",
        "lease_lost",
        "successor_claim",
    ],
)
async def test_compiled_saver_binding_refuses_mismatch_without_assignment(mismatch):
    agent, source, compiled, handle, reset = await binding_agent()
    try:
        if mismatch == "source_job":
            source.unit_id = str(uuid4())
        elif mismatch == "source_token":
            source.lease_token = 4
        elif mismatch == "compiled_job":
            compiled.unit_id = str(uuid4())
        elif mismatch == "compiled_token":
            compiled.lease_token = 4
        elif mismatch == "connection":
            compiled.conn = object()
        elif mismatch == "lock":
            compiled.lock = object()
        elif mismatch == "callback":
            compiled.post_commit = lambda *_args: None
        elif mismatch == "retry_attempts":
            compiled.retry_attempts += 1
        elif mismatch == "retry_delay":
            compiled.retry_base_seconds += 1
        elif mismatch == "missing_connection":
            del compiled.conn
        elif mismatch == "missing_source_job":
            del source.unit_id
        elif mismatch == "not_fenced":
            agent._graph.checkpointer = object()
        elif mismatch == "lease_lost":
            handle.mark_lost()
        elif mismatch == "successor_claim":
            handle.update(handle.unit_id, 6)
        with pytest.raises(LeaseLostError):
            agent._retain_compiled_worker_checkpointer()
        assert agent._checkpointer is source
        assert handle.lost.is_set() is (mismatch == "lease_lost")
    finally:
        current_lease.reset(reset)


def test_strict_worker_process_job_retains_real_compiled_saver(tmp_path):
    # The strict serializer flag is read when LangGraph imports. Setting it
    # after pytest collection misses the shallow clone made by real compile.
    script = r"""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from langgraph._internal import _serde

from agent.agent import UniversalAgent
from agent.api.lease_context import LeaseHandle, current_lease
from agent.api.turn_executor import StatelessTurnExecutor
from agent.core.fenced_checkpointer import FencedAsyncPostgresSaver
from agent.graph import checkpoint_completion_report
from shared.runtime.core.loader import AgentConfig


async def main():
    assert _serde.STRICT_MSGPACK_ENABLED is True
    job = str(uuid4())
    handle = LeaseHandle()
    handle.update(job, 5)
    reset = current_lease.set(handle)
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._initialized = True
    agent._base_config = AgentConfig(agent_id="binding-test", display_name="binding-test")
    agent._base_config.memory.required = False
    agent._auxiliary_llm = None
    agent._llm_with_tools = Mock()
    agent._tools = []
    agent._todo_manager = Mock()
    agent._tool_context = None
    agent._orchestrator_client = None
    agent.postgres_conn = None
    agent._jobs_processed = 0
    agent._workspace_manager = SimpleNamespace(
        path=Path(__import__("sys").argv[1]),
        backend=SimpleNamespace(supports_shell=True),
    )
    agent._setup_job_workspace = AsyncMock(return_value={})
    agent._setup_job_tools = AsyncMock()
    agent._remove_legacy_manifest_status = Mock()
    agent._commit_workspace_seed = Mock()
    initial = dict(
        should_stop=True, goal_achieved=False,
        error={"type": "llm_unavailable", "recoverable": True},
        freeze_data={"freeze_type": "llm_unavailable"},
    )
    initial.update(checkpoint_completion_report(initial))
    agent._arm_worker_batch = AsyncMock(return_value=initial)
    source = None

    async def factory(_url, **kwargs):
        nonlocal source
        # Real constructor, real graph builder, real StateGraph schema and
        # real strict allowlist clone; no database, model or workspace IO.
        source = FencedAsyncPostgresSaver(object(), **kwargs)
        source.aget_tuple = AsyncMock(return_value=None)
        return source

    try:
        with (
            patch("agent.agent.checkpointer_backend", return_value="postgres"),
            patch("agent.agent.resolve_fenced_checkpoint_url", return_value="offline"),
            patch("agent.core.fenced_checkpointer.make_fenced_checkpointer", factory),
            patch("agent.agent.PhaseSnapshotManager", return_value=None),
        ):
            stream = await agent.process_job(
                job, stream=True, worker_lease_token=5,
                worker_batch_target_wall_seconds=10, defer_cleanup=True,
            )
            assert [state async for state in stream] == [initial]
        compiled = agent._graph.checkpointer
        assert compiled is not source
        assert type(compiled) is FencedAsyncPostgresSaver
        assert compiled.conn is source.conn and compiled.lock is source.lock
        assert compiled.serde is not source.serde
        assert agent._worker_thread_config["configurable"]["thread_id"] == job

        terminal = StatelessTurnExecutor._worker_retry_exhausted_state(
            initial, attempts=5, max_attempts=5
        )
        terminal.update(client_report_id=None, completion_report_payload=None)
        terminal.update(checkpoint_completion_report(terminal))
        agent._graph.aget_state = AsyncMock(
            return_value=SimpleNamespace(next=(), values=terminal)
        )
        agent._graph.aupdate_state = AsyncMock()
        saved = await agent.checkpoint_worker_retry_exhaustion(
            job_id=job, lease_token=5, terminal_state=terminal
        )
        assert agent._checkpointer is compiled
        assert saved == terminal
        agent._graph.aupdate_state.assert_not_awaited()
    finally:
        current_lease.reset(reset)


asyncio.run(main())
"""
    env = dict(os.environ, LANGGRAPH_STRICT_MSGPACK="true")
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
