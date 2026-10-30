"""A feedback resume never sends a tool call without its result.

Dev jobs 19837f4b and 1dc0fac1 (2026-10-30) paused while ``shell_execute``
ran (its shell had exited). The checkpoint held the assistant's call with no
result; the operator resumed with feedback, ``restore_from_feedback`` ran in
place of the pending tools node, and OpenAI rejected the first request:
``400 No tool output found for function call call_...``. The job failed for
good, on whichever model it was retried with.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.core.context import repair_tool_pairing
from agent.core.workspace import WorkspaceManager
from agent.graph import (
    INTERRUPTED_TOOL_RESULT,
    create_execute_node,
    create_restore_from_feedback_node,
    settle_interrupted_tool_calls,
)
from agent.managers.todo import TodoManager
from tests._fs_backend import FilesystemTestBackend


def _call(call_id: str, name: str = "shell_execute") -> dict:
    return {"id": call_id, "name": name, "args": {"command": "pytest; exit 0"}}


def _unanswered(messages) -> set:
    calls = {
        tc["id"]
        for m in messages
        if isinstance(m, AIMessage)
        for tc in (m.tool_calls or [])
    }
    results = {m.tool_call_id for m in messages if isinstance(m, ToolMessage)}
    return calls - results


@pytest.fixture
def workspace(tmp_path):
    ws = WorkspaceManager(
        job_id="feedback-resume-job",
        base_path=tmp_path,
        backend=FilesystemTestBackend(tmp_path),
    )
    ws.initialize()
    return ws


@pytest.fixture
def config():
    config = MagicMock()
    config.agent_id = "test-agent"
    config.llm.model = "test-model"
    config.extra = {}
    config._deployment_dir = None
    config.context_management.max_summary_length = 10_000
    return config


class TestSettleInterruptedToolCalls:
    def test_unanswered_call_gets_an_unknown_result_right_behind_it(self):
        history = [
            HumanMessage(content="run the tests"),
            AIMessage(content="", tool_calls=[_call("call_QAzZ")]),
        ]

        settled, results = settle_interrupted_tool_calls(history)

        assert [m.tool_call_id for m in results] == ["call_QAzZ"]
        assert settled[-1].tool_call_id == "call_QAzZ"
        assert settled[-1].content == INTERRUPTED_TOOL_RESULT
        assert settled[-1].name == "shell_execute"
        assert _unanswered(settled) == set()

    def test_only_the_missing_calls_of_a_parallel_batch_are_settled(self):
        history = [
            HumanMessage(content="look around"),
            AIMessage(content="", tool_calls=[_call("a", "read_file"), _call("b")]),
            ToolMessage(content="file text", tool_call_id="a"),
        ]

        settled, results = settle_interrupted_tool_calls(history)

        assert [m.tool_call_id for m in results] == ["b"]
        assert [getattr(m, "tool_call_id", None) for m in settled[1:]] == [
            None,
            "a",
            "b",
        ]

    def test_message_after_the_call_moves_behind_its_result(self):
        history = [
            AIMessage(content="", tool_calls=[_call("c")]),
            HumanMessage(content="a note appended after the call"),
        ]

        settled, _ = settle_interrupted_tool_calls(history)

        assert isinstance(settled[1], ToolMessage)
        assert settled[2].content == "a note appended after the call"

    def test_finished_history_is_left_alone(self):
        history = [
            AIMessage(content="", tool_calls=[_call("d")]),
            ToolMessage(content="Exit code: 0", tool_call_id="d"),
            AIMessage(content="done"),
        ]

        settled, results = settle_interrupted_tool_calls(history)

        assert results == []
        assert settled == history


class TestFeedbackResumeSettlesTheStoppedCall:
    @pytest.mark.asyncio
    async def test_resumed_history_pairs_every_call(self, workspace, config):
        context_mgr = MagicMock()
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        node = create_restore_from_feedback_node(
            workspace,
            TodoManager(workspace),
            config,
            context_mgr,
            auxiliary_llm=None,
            summarization_prompt="",
        )
        stopped = AIMessage(
            content="", tool_calls=[_call("call_JSow")], id="ai-stopped"
        )

        with patch("agent.graph.get_archiver", return_value=None):
            result = await node(
                {
                    "job_id": "j",
                    "resume_feedback": "do not use exit in shell commands",
                    "messages": [HumanMessage(content="task", id="h1"), stopped],
                    "phase_number": 5,
                    "is_strategic_phase": False,
                }
            )

        compacted = context_mgr.ensure_within_limits.call_args.args[0]
        assert _unanswered(compacted) == set()
        messages = result["messages"]
        assert _unanswered(messages) == set()
        position = {
            getattr(m, "tool_call_id", None) or m.id: i
            for i, m in enumerate(messages)
            if getattr(m, "tool_call_id", None) or m.id
        }
        assert position["ai-stopped"] < position["call_JSow"]
        assert messages[-1].content.startswith("[FEEDBACK_RESUME]")
        assert position["call_JSow"] < len(messages) - 1
        # The provider-facing repair has nothing left to strip.
        assert repair_tool_pairing(messages) == messages


class TestExecuteNeverSendsAnUnansweredCall:
    """Backstop for histories no resume settled (compaction or damage)."""

    @pytest.mark.asyncio
    async def test_unanswered_call_is_dropped_from_the_request(self, workspace):
        captured = []

        async def fake_ainvoke(prepared, **_kwargs):
            captured.append(list(prepared))
            return AIMessage(content="ok")

        llm = MagicMock()
        llm.ainvoke = AsyncMock(side_effect=fake_ainvoke)
        config = MagicMock()
        config.agent_id = "test-agent"
        config.extra = {}
        config.llm.model = "test-model"
        config.llm.timeout = 10.0
        config.llm.model_max_context_tokens = 100000
        config.limits.model_max_context_tokens = 100000
        config.limits.response_validation.enabled = False
        config.context_management.max_summary_length = 500
        context_mgr = MagicMock()
        context_mgr.get_token_count.return_value = 50
        context_mgr.config.compaction_threshold_tokens = 100000
        context_mgr.config.summarization_threshold_tokens = 100000
        context_mgr.config.keep_recent_messages = 10
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        execute = create_execute_node(
            llm_with_tools=llm,
            todo_manager=TodoManager(workspace),
            memory_manager=MagicMock(),
            workspace_manager=workspace,
            config=config,
            context_mgr=context_mgr,
            retry_manager=MagicMock(),
            auxiliary_llm=None,
            summarization_prompt="",
            tool_names=["shell_execute"],
        )
        state = {
            "job_id": "job-under-test",
            "iteration": 1,
            "messages": [
                HumanMessage(content="run the tests"),
                AIMessage(content="", tool_calls=[_call("call_orphan")]),
                HumanMessage(content="[FEEDBACK_RESUME] carry on"),
            ],
            "is_strategic_phase": False,
            "phase_number": 2,
            "metadata": {},
        }

        await execute(state)

        (request,) = captured
        sent_calls = {
            tc["id"]
            for m in request
            if isinstance(m, AIMessage)
            for tc in (m.tool_calls or [])
        }
        assert "call_orphan" not in sent_calls
        assert _unanswered(request) == set()
