"""The worker's todo list lives in the history, not in a per-request tail.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D17-D19); plan:
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md
(WP1).

- D17: the per-turn ``<active_tasks>`` message is gone
  (tests/test_execute_prepared_layout.py pins the tail without it; the
  ``todos-only`` cases of tests/test_prompt_cache_prefix_invariance.py are the
  prompt-cache gate).
- D18: ``todo_complete`` and ``next_phase_todos`` return the full current list,
  rendered by ``TodoManager.format_for_injection``, which carries no per-turn
  data: rendered again later, or after a resume, it is byte-identical.
- Phase start: the message that opens a phase carries the new list once.
- D19: after a summary evicts the list, compaction restates it once, right
  after the summary; the restatement stays put until the next compaction.
"""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)

from agent.core.context import ContextConfig, ContextManager
from agent.core.phase import on_strategic_phase_complete, on_tactical_phase_complete
from agent.core.workspace import WorkspaceManager
from agent.graph import (
    create_init_strategic_todos_node,
    create_restore_from_feedback_node,
    hydrate_todo_manager_from_state,
)
from agent.managers.todo import (
    TODO_HEADLINE_MAX_CHARS,
    TODO_LIST_RESTATEMENT_LEAD,
    TodoManager,
    todo_headline,
)
from agent.tools.context import ToolContext
from agent.tools.core.todo import create_todo_tools
from tests._fs_backend import FilesystemTestBackend
from shared.runtime.core.message_markers import is_compaction_summary


def _offline_workspace() -> MagicMock:
    ws = MagicMock()
    ws.git_manager = None  # no commit per completed todo
    return ws


def _tactical_manager() -> TodoManager:
    mgr = TodoManager(_offline_workspace(), min_todos=2)
    mgr.is_strategic_phase = False
    mgr.phase_number = 2
    mgr.add("Read the brief")
    mgr.add("Write the summary")
    mgr.add("Check the summary against the brief")
    return mgr


def _tools(mgr: TodoManager) -> dict:
    ctx = ToolContext(workspace_manager=_offline_workspace(), todo_manager=mgr)
    ctx.record_file_read("skills/todo-guide/SKILL.md")  # next_phase_todos gate
    return {tool.name: tool for tool in create_todo_tools(ctx)}


def _text(message: BaseMessage) -> str:
    return str(message.content)


# ---------------------------------------------------------------------------
# D18: tools that change the list return all of it
# ---------------------------------------------------------------------------


class TestTodoToolResults:
    def test_completion_returns_the_full_list(self):
        mgr = _tactical_manager()
        tools = _tools(mgr)

        result = tools["todo_complete"].invoke(
            {"todo_id": "todo_1", "completion_note": "PASS: brief read"}
        )

        rendering = mgr.format_for_injection()
        assert result == (
            "Completed: Read the brief\n"
            "Next: todo_2: Write the summary\n\n"
            f"{rendering}\n\n"
            "Recorded completion note: PASS: brief read"
        )
        assert "Current Tasks — Phase 2 (Tactical)" in rendering
        assert "  - [x] todo_1: Read the brief" in rendering
        assert "      Outcome: PASS: brief read" in rendering
        assert "  - [ ] todo_2: Write the summary" in rendering
        assert "  - [ ] todo_3: Check the summary against the brief" in rendering
        assert "Remaining:" not in result

    def test_completion_without_an_id_returns_the_full_list(self):
        mgr = _tactical_manager()
        tools = _tools(mgr)

        result = tools["todo_complete"].invoke({})

        assert result == (
            "Completed: Read the brief\n"
            "Next: todo_2: Write the summary\n\n"
            f"{mgr.format_for_injection()}"
        )

    def test_last_completion_returns_the_list_then_the_phase_signal(self):
        mgr = _tactical_manager()
        tools = _tools(mgr)
        for todo_id in ("todo_1", "todo_2"):
            tools["todo_complete"].invoke({"todo_id": todo_id})

        result = tools["todo_complete"].invoke({"todo_id": "todo_3"})

        assert result == (
            "Completed: Check the summary against the brief\n"
            "All tasks complete! Ready for phase transition.\n\n"
            f"{mgr.format_for_injection()}\n\n"
            "[PHASE_COMPLETE] All tasks in this phase are done."
        )
        assert "Pending:" not in mgr.format_for_injection()

    def test_finished_todos_show_only_their_headline(self):
        """A long multi-line body (the predefined strategic todos) appears in
        full while pending; once completed, the list and the result head show
        only its first line, so each completion does not repeat it."""
        mgr = TodoManager(_offline_workspace())
        body = "EXPLORE: understand the task.\n" + "Read every line. " * 200
        mgr.add(body)
        mgr.add("PLAN: write plan.md.\n" + "Phase table. " * 100)
        assert body in mgr.format_for_injection()

        result = _tools(mgr)["todo_complete"].invoke({"todo_id": "todo_1"})

        rendering = mgr.format_for_injection()
        assert "  - [x] todo_1: EXPLORE: understand the task." in rendering
        assert "Read every line." not in rendering
        assert result.startswith(
            "Completed: EXPLORE: understand the task.\n"
            "Next: todo_2: PLAN: write plan.md.\n\n"
        )
        assert result.count("Read every line.") == 0
        assert result.count("Phase table.") == 100  # the pending body, once

    def test_a_long_first_line_is_capped(self):
        assert todo_headline("x" * 500) == "x" * (TODO_HEADLINE_MAX_CHARS - 1) + "…"
        assert todo_headline("\n\n  a   b \nrest") == "a b"

    def test_the_rendering_carries_no_per_turn_data(self):
        """Rendered later, on another turn, after unrelated tool calls or after
        a resume from the checkpoint, the list is byte-identical to the one in
        the tool result — so it can sit in the history and be found there."""
        mgr = _tactical_manager()
        tools = _tools(mgr)
        result = tools["todo_complete"].invoke(
            {"todo_id": "todo_1", "completion_note": "PASS: brief read"}
        )
        first = mgr.format_for_injection()

        # Later turns: other tools run, the clock moves on.
        tools["todo_list"].invoke({})
        with patch("agent.managers.todo.datetime") as clock:
            clock.now.side_effect = AssertionError("the rendering read the clock")
            later = mgr.format_for_injection()

        # A resume rebuilds the manager from the checkpointed state only.
        exported = mgr.export_state()
        resumed = TodoManager(_offline_workspace())
        hydrate_todo_manager_from_state(
            resumed,
            {
                "todos": exported["todos"],
                "staged_todos": exported["staged_todos"],
                "todo_next_id": exported["next_id"],
                "phase_number": 2,
                "is_strategic_phase": False,
            },
        )

        assert later == first
        assert resumed.format_for_injection() == first
        assert first in result
        assert not re.search(r"\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2}", first)
        assert (
            mgr.list_restatement([ToolMessage(content=result, tool_call_id="c1")])
            is None
        )

    def test_next_phase_todos_returns_the_staged_batch_and_the_current_list(self):
        mgr = TodoManager(_offline_workspace(), min_todos=2)
        mgr.is_strategic_phase = True
        mgr.phase_number = 1
        mgr.add("Review the brief and write plan.md")
        mgr.add("Stage the todos for the next phase")
        tools = _tools(mgr)
        tools["todo_complete"].invoke({"todo_id": "todo_1"})

        result = tools["next_phase_todos"].invoke(
            {
                "todos": ["Implement the parser module", "Write the parser tests"],
                "phase_name": "Build",
            }
        )

        rendering = mgr.format_for_injection()
        assert result == (
            "Staged 2 todos for the next tactical phase (Build).\n"
            "  - Implement the parser module\n"
            "  - Write the parser tests\n\n"
            f"{rendering}\n\n"
            "All strategic todos complete. Invoke the `todo_complete` tool to "
            "transition to tactical phase."
        )
        # The current (strategic) list; the staged phase name is not part of
        # its header — it is not checkpointed, so a resume would lose it.
        assert rendering.startswith("Current Tasks — Phase 1 (Strategic)\n")
        assert "Build" not in rendering

    def test_next_phase_todos_with_strategic_work_left(self):
        mgr = TodoManager(_offline_workspace(), min_todos=2)
        mgr.add("Review the brief and write plan.md")
        mgr.add("Record the key decisions in the KB")
        mgr.add("Stage the todos for the next phase")
        tools = _tools(mgr)

        result = tools["next_phase_todos"].invoke(
            {"todos": ["Implement the parser module", "Write the parser tests"]}
        )

        assert mgr.format_for_injection() in result
        assert result.endswith(
            "Remaining strategic todos before transition: the pending ones "
            "above. Complete each with the `todo_complete` tool; completing "
            "the last one starts the tactical phase."
        )

    def test_error_results_are_unchanged(self):
        mgr = _tactical_manager()
        tools = _tools(mgr)

        assert tools["todo_complete"].invoke({"todo_id": "todo_9"}) == (
            "Error: Todo 'todo_9' not found. Available todos: todo_1, todo_2, todo_3"
        )
        assert tools["todo_complete"].invoke({"todo_id": "todo_1,todo_2"}) == (
            "Error: Complete one todo at a time. Each todo_complete call should "
            "reflect verified work for that specific task.\n"
            "Invoke `todo_complete` with todo_id set to `todo_1` first, "
            "then invoke it again for each subsequent task."
        )
        assert tools["next_phase_todos"].invoke({"todos": ["Too few todos here"]}) == (
            "Error: Too few todos: 1 < 2. Create more detailed, actionable tasks."
        )


# ---------------------------------------------------------------------------
# Phase start: the opening message carries the new list once
# ---------------------------------------------------------------------------


@pytest.fixture
def workspace(tmp_path):
    ws = WorkspaceManager(
        job_id="todo-history-job",
        base_path=tmp_path,
        backend=FilesystemTestBackend(tmp_path),
    )
    ws.initialize()
    return ws


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.agent_id = "test-agent"
    config.llm.model = "test-model"
    config.extra = {}
    config._deployment_dir = None
    config.context_management.max_summary_length = 10_000
    return config


def _in_history_once(mgr: TodoManager, messages) -> None:
    rendering = mgr.format_for_injection()
    assert sum(_text(m).count(rendering) for m in messages) == 1
    assert mgr.list_restatement(messages) is None


class TestPhaseStartCarriesTheList:
    def test_strategic_to_tactical_marker_carries_the_tactical_list(self, workspace):
        mgr = TodoManager(workspace, min_todos=2)
        mgr.phase_number = 1
        mgr.add("Stage the todos for the next phase")
        mgr.complete("todo_1")
        mgr.stage_tactical_todos(
            ["Implement the parser module", "Write the parser tests"], "Build"
        )
        mgr.archive("strategic")

        result = on_strategic_phase_complete(
            {"job_id": "j", "phase_number": 1, "is_strategic_phase": True},
            workspace,
            mgr,
            min_todos=2,
        )

        (marker,) = result.state_updates["messages"]
        assert result.success
        assert marker.content.startswith("[PHASE_TRANSITION] Strategic phase complete")
        assert "Current Tasks — Phase 2 (Tactical)" in marker.content
        assert "  - [ ] todo_1: Implement the parser module" in marker.content
        # handle_transition syncs the same phase fields after success; the
        # rendering the rest of the phase sees is the one in the marker.
        mgr.is_strategic_phase = result.state_updates["is_strategic_phase"]
        mgr.phase_number = result.state_updates["phase_number"]
        _in_history_once(mgr, [marker])

    def test_tactical_to_strategic_marker_carries_the_strategic_list(self, workspace):
        mgr = TodoManager(workspace)
        mgr.is_strategic_phase = False
        mgr.phase_number = 2
        mgr.add("Implement the parser module")
        mgr.complete("todo_1")
        mgr.archive("tactical")

        result = on_tactical_phase_complete(
            {"job_id": "j", "phase_number": 2, "is_strategic_phase": False},
            workspace,
            mgr,
        )

        (marker,) = result.state_updates["messages"]
        assert marker.content.startswith("[PHASE_TRANSITION] Tactical phase complete")
        assert "Current Tasks — Phase 3 (Strategic)" in marker.content
        assert mgr.list_all(), "the predefined strategic todos were loaded"
        _in_history_once(mgr, [marker])

    def test_first_message_carries_the_initial_strategic_list(
        self, workspace, mock_config
    ):
        workspace.write_file("instructions.md", "# Task\n\nDo the thing.")
        mgr = TodoManager(workspace)
        node = create_init_strategic_todos_node(workspace, mgr, mock_config)

        result = node({"job_id": "j", "phase_number": 1})

        (first,) = result["messages"]
        assert "Do the thing." in first.content
        assert "Current Tasks — Phase 1 (Strategic)" in first.content
        _in_history_once(mgr, [first])

    @pytest.mark.asyncio
    async def test_feedback_resume_message_carries_the_resume_list(
        self, workspace, mock_config
    ):
        mgr = TodoManager(workspace)
        context_mgr = MagicMock()
        context_mgr.ensure_within_limits = AsyncMock(
            side_effect=lambda msgs, *a, **k: msgs
        )
        node = create_restore_from_feedback_node(
            workspace,
            mgr,
            mock_config,
            context_mgr,
            auxiliary_llm=None,
            summarization_prompt="",
        )

        with patch("agent.graph.get_archiver", return_value=None):
            result = await node(
                {
                    "job_id": "j",
                    "resume_feedback": "fix finding F1",
                    "messages": [HumanMessage(content="old context")],
                    "phase_number": 3,
                    "is_strategic_phase": False,
                }
            )

        banner = result["messages"][-1].content
        assert banner.startswith("[FEEDBACK_RESUME]")
        below = banner.index("strategic todos below")
        assert banner.index(mgr.format_for_injection()) > below
        _in_history_once(mgr, result["messages"])


# ---------------------------------------------------------------------------
# D19: compaction restates the list once, right after the summary
# ---------------------------------------------------------------------------


def _summarizer():
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=AIMessage(
            content="## Objective\n- Summarize the brief.",
            response_metadata={"finish_reason": "stop"},
        )
    )
    return AuxiliaryLLM(llm=llm, max_context_tokens=15_000)


def _restate(mgr: TodoManager):
    """The execute node's hook (graph.py ``_restate_todo_list``)."""

    def hook(retained):
        text = mgr.list_restatement(retained)
        return [HumanMessage(content=text)] if text else []

    return hook


def _tool_round(n: int, name: str, content: str) -> list:
    return [
        AIMessage(
            content="",
            tool_calls=[{"name": name, "args": {"n": n}, "id": f"c{n}"}],
            id=f"ai{n}",
        ),
        ToolMessage(content=content, tool_call_id=f"c{n}", name=name, id=f"t{n}"),
    ]


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


class TestCompactionRestatesTheList:
    def _history(self, mgr: TodoManager, *, completion_round: int) -> list:
        """A tactical history whose todo_complete result sits in round
        ``completion_round``; the other rounds are large reads."""
        tools = _tools(mgr)
        history: list = [HumanMessage(content="Start the task.", id="h0")]
        for n in range(1, 6):
            if n == completion_round:
                result = tools["todo_complete"].invoke({"todo_id": "todo_1"})
                history += _tool_round(n, "todo_complete", result)
            else:
                history += _tool_round(n, "read_file", f"line {n}\n" * 600)
        return history

    def _manager(self) -> ContextManager:
        return ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=1_000,
                summarization_threshold_tokens=1_000,
                keep_recent_messages=2,
                model_max_context_tokens=200_000,
            ),
            model="gpt-4",
        )

    @pytest.mark.asyncio
    async def test_evicted_list_is_restated_once_after_the_summary(self):
        mgr = _tactical_manager()
        history = self._history(mgr, completion_round=1)

        result = await self._manager().ensure_within_limits(
            history, _summarizer(), force=True, restate_after_summary=_restate(mgr)
        )

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert is_compaction_summary(kept[0])
        restated = kept[1]
        assert isinstance(restated, HumanMessage) and restated.id is None
        assert restated.content == (
            f"{TODO_LIST_RESTATEMENT_LEAD}\n\n{mgr.format_for_injection()}"
        )
        # The kept window follows; the list is in the history exactly once.
        assert [type(m) for m in kept[2:]] == [AIMessage, ToolMessage]
        _in_history_once(mgr, kept)

    @pytest.mark.asyncio
    async def test_kept_window_showing_the_list_needs_no_restatement(self):
        mgr = _tactical_manager()
        history = self._history(mgr, completion_round=5)

        result = await self._manager().ensure_within_limits(
            history, _summarizer(), force=True, restate_after_summary=_restate(mgr)
        )

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert not any(TODO_LIST_RESTATEMENT_LEAD in _text(m) for m in kept)
        assert kept[-1].name == "todo_complete"
        _in_history_once(mgr, kept)

    @pytest.mark.asyncio
    async def test_restatement_survives_a_later_compaction_exactly_once(self):
        """The next summary evicts the restatement with its region; the hook
        then restates the (unchanged) list once more — never twice."""
        mgr = _tactical_manager()
        history = self._history(mgr, completion_round=1)
        cm = self._manager()
        first = await cm.ensure_within_limits(
            history, _summarizer(), force=True, restate_after_summary=_restate(mgr)
        )
        state = [m for m in first if not isinstance(m, RemoveMessage)]
        for n in range(6, 9):
            state += _tool_round(n, "read_file", f"more {n}\n" * 600)

        second = await cm.ensure_within_limits(
            state, _summarizer(), force=True, restate_after_summary=_restate(mgr)
        )

        kept = [m for m in second if not isinstance(m, RemoveMessage)]
        assert sum(TODO_LIST_RESTATEMENT_LEAD in _text(m) for m in kept) == 1
        _in_history_once(mgr, kept)

    @pytest.mark.asyncio
    async def test_no_hook_no_restatement(self):
        """Sessions and the boundary/feedback compactions pass no hook."""
        mgr = _tactical_manager()
        history = self._history(mgr, completion_round=1)

        result = await self._manager().ensure_within_limits(
            history, _summarizer(), force=True
        )

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert mgr.list_restatement(kept) is not None  # evicted, not restated


def test_empty_list_needs_no_restatement():
    assert TodoManager(SimpleNamespace(git_manager=None)).list_restatement([]) is None
