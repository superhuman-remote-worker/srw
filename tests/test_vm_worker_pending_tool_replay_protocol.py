"""Source reproduction: a pending graph call is not remote-effect deduplication.

This positive mechanism test is paired with the native no-successor RED. It
does not propose changing the reusable shell's completed-tab semantics.
"""

from types import SimpleNamespace
import shlex
from unittest.mock import patch
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from agent.agent import UniversalAgent
from agent.core.state import UniversalAgentState
from agent.graph import create_audited_tool_node
from tests.test_phase_gate import FakeConfig
from tests.test_workspace_backends import _tmux_window_row, remote_backend  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.parametrize("remote_finished", [False, True])
async def test_pending_tools_frontier_can_send_again_after_remote_completion(
    remote_backend,  # noqa: F811
    remote_finished,
):
    backend, _, _ = remote_backend
    backend._shell_protocol_current = True
    backend._shell_initialized = True
    previous = "__DONE_0123456789ab__"
    remote_sends = []
    original = "printf 'one-effect\\n' >> effect-count"

    def remote_mutation(command, **_kwargs):
        if "send-keys" in command:
            remote_sends.append(command)
        return ""

    def remote_capture(_tab):
        if remote_sends:
            pending = backend._tabs["default"].pending_sentinel
            return [f"{pending} 0 /home/agent-host/workspace"]
        if remote_finished:
            return [f"{previous} 0 /home/agent-host/workspace"]
        return ["original command still running"]

    @tool
    def run_command(command: str) -> str:
        """Run the exact pending command through the actual remote backend."""
        return backend.shell_run(command, timeout=1)

    owner = str(uuid4())
    initial = {
        "job_id": owner,
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "original-call",
                        "name": "run_command",
                        "args": {"command": original},
                    }
                ],
            )
        ],
        "iteration": 1,
        "is_strategic_phase": False,
        "phase_number": 2,
        "metadata": {},
    }
    graph = StateGraph(UniversalAgentState)
    graph.add_node("prepare", lambda state: state)
    graph.add_node("tools", create_audited_tool_node([run_command], FakeConfig()))
    graph.add_edge(START, "prepare")
    graph.add_edge("prepare", "tools")
    graph.add_edge("tools", END)
    saver = InMemorySaver()
    config = {"configurable": {"thread_id": owner}}
    paused = graph.compile(checkpointer=saver, interrupt_before=["tools"])
    await paused.ainvoke(initial, config)
    checkpoint = await paused.aget_state(config)
    assert checkpoint.next == ("tools",)
    assert checkpoint.metadata["source"] == "loop"
    assert not any(isinstance(m, ToolMessage) for m in checkpoint.values["messages"])
    successor = graph.compile(checkpointer=saver)
    with (
        patch.object(
            backend,
            "_tmux_exec_checked",
            return_value=_tmux_window_row("default", "shell", previous),
        ),
        patch.object(backend, "_tmux_mutate_checked", side_effect=remote_mutation),
        patch.object(backend, "_tmux_capture", side_effect=remote_capture),
        patch("shared.runtime.core.backends.remote.time.sleep"),
        patch("agent.graph.get_archiver", return_value=None),
    ):
        backend._rehydrate_tabs()
        result = await UniversalAgent._arm_worker_batch(
            SimpleNamespace(_graph=successor),
            job_id=owner,
            graph_input=None,
            thread_config=config,
            target_wall_seconds=300,
            min_wall_seconds=300,
            iteration_cap=None,
        )
        assert result is None
        assert (await successor.aget_state(config)).next == ("tools",)
        final = await successor.ainvoke(None, config)
    assert len(remote_sends) == int(remote_finished)
    if remote_finished:
        line = next(x for x in remote_sends[0].splitlines() if "send-keys" in x)
        assert original in shlex.split(line)[-1]
    results = [m for m in final["messages"] if isinstance(m, ToolMessage)]
    assert len(results) == 1 and results[0].tool_call_id == "original-call"
    assert (
        "previous command still running" in results[0].content
    ) is not remote_finished
