"""The no-tool-call and empty-response breakers end the run.

Both breakers in the execute node return a non-recoverable error. Before
2026-11-04 neither set ``should_stop``, so ``route_after_check_todos`` sent the
run straight back to ``execute``: on local k3d a model that only answered "ok"
was called 5,080 times in 142 s until the job was cancelled (vault issue
agent_no_tool_call_breaker_never_ends_the_job).

The graph here wires the real execute node to the real routers; the other
nodes are stand-ins, as a pending todo list leaves ``check_todos``.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage
from langgraph.graph import END, StateGraph

from agent.core.state import UniversalAgentState
from agent.graph import route_after_check_todos, route_after_execute
from orchestrator.services.completion import determine_job_status
from tests.test_execute_prepared_layout import (
    CapturingLLM,
    _make_node,
    _run,
    _state,
    env as env,
)


def _unreachable(name):
    def node(state):
        raise AssertionError(f"the run reached {name}")

    return node


def _graph(execute):
    async def run_execute(state):
        return await _run(execute, state)

    workflow = StateGraph(UniversalAgentState)
    workflow.add_node("execute", run_execute)
    workflow.add_node("tools", _unreachable("tools"))
    workflow.add_node("check_todos", lambda state: {"phase_complete": False})
    workflow.add_node("archive_phase", _unreachable("archive_phase"))
    workflow.add_node("check_goal", lambda state: {})
    workflow.set_entry_point("execute")
    workflow.add_conditional_edges(
        "execute",
        route_after_execute,
        {"tools": "tools", "check_todos": "check_todos"},
    )
    workflow.add_conditional_edges(
        "check_todos",
        route_after_check_todos,
        {
            "execute": "execute",
            "archive_phase": "archive_phase",
            "check_goal": "check_goal",
        },
    )
    workflow.add_edge("check_goal", END)
    return workflow.compile()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "error_type"),
    [
        pytest.param("ok", "parser_failure", id="text-without-tool-calls"),
        pytest.param("", "empty_response", id="empty"),
    ],
)
async def test_a_model_that_never_calls_a_tool_fails_the_job(
    request, answer, error_type
):
    llm = CapturingLLM([AIMessage(content=answer) for _ in range(50)])
    execute = _make_node(request.getfixturevalue("env"), llm)

    final = await _graph(execute).ainvoke(_state(), {"recursion_limit": 40})

    # Threshold 3: the fourth such answer trips the breaker, and nothing
    # calls the model after it.
    assert len(llm.requests) == 4
    assert final["should_stop"] is True
    assert final["error"]["type"] == error_type
    assert final["error"]["recoverable"] is False
    status, _ = determine_job_status(
        {"id": "job", "status": "processing", "context": {}}, final
    )
    assert status == "failed"
