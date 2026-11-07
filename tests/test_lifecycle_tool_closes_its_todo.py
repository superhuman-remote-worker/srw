"""A successful lifecycle tool completes the one todo still pending.

``next_phase_todos`` and ``job_complete`` are the last act of the todo that
asks for them. When that todo is the only one pending, the tool completes it,
so the model does not spend a request on a ``todo_complete`` that only records
the call it just made (SRW vs Claude Code benchmark, 2026-11-04: every job
ended ``job_complete`` -> lone ``todo_complete``, every strategic phase
``next_phase_todos`` -> lone ``todo_complete``). With more than one todo
pending nothing is completed. The ``next_phase_todos`` results are pinned in
tests/test_todo_list_in_history.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from agent.managers.todo import TodoManager
from agent.tools.core.job import _final_phase_data, create_job_tools
from agent.tools.core.todo import close_last_pending_todo
from tests._tool_invoke import invoke_tool

JOB_ID = "job-lifecycle-closes-todo"


@pytest.fixture(autouse=True)
def _clean_caches():
    _final_phase_data.clear()
    yield
    _final_phase_data.clear()


def _manager(*contents: str) -> TodoManager:
    ws = MagicMock()
    ws.git_manager = None
    mgr = TodoManager(ws, min_todos=2)
    mgr.is_strategic_phase = True
    mgr.phase_number = 2
    for content in contents:
        mgr.add(content)
    return mgr


def _context(mgr: TodoManager) -> MagicMock:
    context = MagicMock()
    context.job_id = JOB_ID
    context.has_workspace.return_value = True
    context.workspace_manager = MagicMock()
    context.has_todo.return_value = True
    context.todo_manager = mgr
    context.orchestrator_client = None
    context.subagent_runtime = None
    return context


async def _job_complete(context: MagicMock) -> str:
    _, job_complete = create_job_tools(context)
    return await invoke_tool(
        job_complete,
        {"summary": "Done.", "deliverables": [], "confidence": 0.9},
    )


class TestJobCompleteClosesItsTodo:
    @pytest.mark.asyncio
    async def test_the_last_pending_todo_is_completed(self):
        mgr = _manager("VERIFY THE RESULT", "CLOSE OR CONTINUE")
        mgr.complete("todo_1")
        context = _context(mgr)

        result = await _job_complete(context)

        assert result.startswith(
            "Phase marked as final. Completed the last todo (CLOSE OR CONTINUE) "
            "- job will complete now."
        )
        assert mgr.all_complete()
        assert mgr.list_all()[1].notes == ["Marked the job final with job_complete."]
        context.progress_committer.on_todo_complete.assert_called_once_with(
            "todo_2", "CLOSE OR CONTINUE"
        )
        context.request_reply_drain.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_strategic_work_left_completes_nothing(self):
        mgr = _manager("VERIFY THE RESULT", "CLOSE OR CONTINUE")
        context = _context(mgr)

        result = await _job_complete(context)

        assert result.startswith(
            "Phase marked as final. Complete your 2 remaining todo(s) to finish "
            "the job."
        )
        assert len(mgr.list_pending()) == 2
        context.progress_committer.on_todo_complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_pending_todo_keeps_the_plain_result(self):
        mgr = _manager("VERIFY THE RESULT", "CLOSE OR CONTINUE")
        mgr.complete("todo_1")
        mgr.complete("todo_2")
        context = _context(mgr)

        result = await _job_complete(context)

        assert result.startswith(
            "Phase marked as final. No remaining todos - job will complete now."
        )
        context.progress_committer.on_todo_complete.assert_not_called()


class TestCloseLastPendingTodo:
    def test_a_redundant_todo_complete_after_the_close_is_harmless(self):
        """A model that still batches todo_complete after the lifecycle call
        finds nothing pending; the list stays complete."""
        mgr = _manager("Write plan.md", "Stage the execution phase")
        mgr.complete("todo_1")
        context = _context(mgr)

        closed = close_last_pending_todo(context, "staged")
        again = mgr.complete_first_pending_sync()

        assert closed is not None and closed.id == "todo_2"
        assert again["completed_id"] is None
        assert again["is_last_task"] is True
        assert mgr.all_complete()
