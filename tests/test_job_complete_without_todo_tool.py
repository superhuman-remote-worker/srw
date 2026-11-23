"""``job_complete`` for a job whose loaded tools do not include ``todo_complete``.

Such a job closes its one task with ``job_complete``. The tool reads the
loaded tool list (``ToolContext._resolved_tool_names``) at call time and the
agent rewrites its description at bind time, so:

* its description and results never speak of a strategic phase, remaining
  todos or ``todo_complete``;
* its file check is relaxed: a listed file under the 50-character floor is
  accepted and recorded as a warning (in the result and the decision record);
  a listed path that does not exist gets one corrective refusal per job, and
  the next call is accepted with the warning recorded.

Jobs that bind ``todo_complete`` keep today's description, results and
refusals exactly.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agent.core.workspace import WorkspaceManager
from agent.managers.todo import TodoManager
from agent.tools.core.job import (
    JOB_COMPLETE_DESCRIPTION_WITHOUT_TODO_TOOL,
    _final_phase_data,
    _missing_deliverable_refused,
    create_job_tools,
)
from agent.tools.description_manager import apply_job_complete_description
from tests._fs_backend import FilesystemTestBackend
from tests._tool_invoke import invoke_tool

JOB_ID = "job-without-todo-tool"
#: The tools of a job without phases, and of the same job planned in phases.
DIRECT_TASK_TOOLS = ["job_complete", "write_file", "read_file"]
PHASED_TOOLS = DIRECT_TASK_TOOLS + ["todo_complete", "next_phase_todos"]
PHASE_WORDS = ("strategic phase", "remaining todo", "todo_complete")


@pytest.fixture(autouse=True)
def _clean_caches():
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()
    yield
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()


@pytest.fixture
def workspace(tmp_path: Path) -> WorkspaceManager:
    ws = WorkspaceManager(
        job_id=JOB_ID, base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ws.initialize()
    return ws


def _job_complete(workspace, tool_names, *, todo: str = "Do the task in the brief."):
    mgr = TodoManager(workspace)
    mgr.is_strategic_phase = True
    mgr.phase_number = 0
    mgr.add(todo)
    context = MagicMock()
    context.job_id = JOB_ID
    context.has_workspace.return_value = True
    context.workspace_manager = workspace
    context.has_todo.return_value = True
    context.todo_manager = mgr
    context.orchestrator_client = None
    context.subagent_runtime = None
    context._resolved_tool_names = list(tool_names)
    _, job_complete = create_job_tools(context)
    return job_complete, mgr


async def _call(job_complete, deliverables, confidence=0.9):
    return await invoke_tool(
        job_complete,
        {
            "summary": "Wrote the note.",
            "deliverables": deliverables,
            "confidence": confidence,
        },
    )


class TestShortFile:
    @pytest.mark.asyncio
    async def test_accepted_and_recorded_without_todo_complete(self, workspace):
        workspace.write_file("output/note.md", "short")
        job_complete, mgr = _job_complete(workspace, DIRECT_TASK_TOOLS)

        result = await _call(job_complete, ["output/note.md"])

        warning = "Deliverable 'output/note.md' appears empty or trivial (5 bytes)"
        assert result.startswith("Job marked as complete. It ends now.")
        assert warning in result
        assert _final_phase_data[JOB_ID]["deliverable_warnings"] == [warning]
        assert mgr.all_complete()
        for word in PHASE_WORDS:
            assert word not in result

    @pytest.mark.asyncio
    async def test_still_refused_with_todo_complete(self, workspace):
        workspace.write_file("output/note.md", "short")
        job_complete, mgr = _job_complete(workspace, PHASED_TOOLS)

        result = await _call(job_complete, ["output/note.md"])

        assert result == (
            "ERROR: Cannot accept confidence 90% with deliverable issues:\n"
            "  - Deliverable 'output/note.md' appears empty or trivial (5 bytes)\n\n"
            "Either fix the deliverables, or lower confidence below 0.5 to "
            "acknowledge the issues. Re-call job_complete after addressing these "
            "problems."
        )
        assert JOB_ID not in _final_phase_data
        assert len(mgr.list_pending()) == 1


class TestMissingPath:
    @pytest.mark.asyncio
    async def test_one_corrective_refusal_then_accepted(self, workspace):
        job_complete, mgr = _job_complete(workspace, DIRECT_TASK_TOOLS)

        first = await _call(job_complete, ["output/never_written.md"])

        assert first == (
            "ERROR: These listed deliverables do not exist in the workspace:\n"
            "  - output/never_written.md\n\n"
            "List only files that exist; an empty list is valid. "
            "Re-call job_complete with the corrected list."
        )
        assert JOB_ID not in _final_phase_data
        assert len(mgr.list_pending()) == 1

        second = await _call(job_complete, ["output/never_written.md"])

        warning = "Deliverable 'output/never_written.md' does not exist"
        assert second.startswith("Job marked as complete. It ends now.")
        assert warning in second
        assert _final_phase_data[JOB_ID]["deliverable_warnings"] == [warning]
        assert mgr.all_complete()

    @pytest.mark.asyncio
    async def test_low_confidence_gets_the_same_one_refusal(self, workspace):
        job_complete, _ = _job_complete(workspace, DIRECT_TASK_TOOLS)

        first = await _call(job_complete, ["output/gone.md"], confidence=0.3)
        second = await _call(job_complete, ["output/gone.md"], confidence=0.3)

        assert first.startswith("ERROR: These listed deliverables do not exist")
        assert second.startswith("Job marked as complete.")

    @pytest.mark.asyncio
    async def test_an_empty_list_is_accepted_at_once(self, workspace):
        job_complete, mgr = _job_complete(workspace, DIRECT_TASK_TOOLS)

        result = await _call(job_complete, [])

        assert result == (
            "Job marked as complete. It ends now.\n\n"
            "Summary: Wrote the note.\n"
            "Deliverables: 0 files\n"
            "Confidence: 90%"
        )
        assert "deliverable_warnings" not in _final_phase_data[JOB_ID]
        assert mgr.all_complete()

    @pytest.mark.asyncio
    async def test_still_refused_every_time_with_todo_complete(self, workspace):
        job_complete, _ = _job_complete(workspace, PHASED_TOOLS)

        for _ in range(2):
            result = await _call(job_complete, ["output/never_written.md"])
            assert result.startswith("ERROR: Cannot accept confidence 90%")
            assert "does not exist" in result

    @pytest.mark.asyncio
    async def test_low_confidence_is_still_accepted_with_todo_complete(self, workspace):
        job_complete, _ = _job_complete(workspace, PHASED_TOOLS)

        result = await _call(job_complete, ["output/gone.md"], confidence=0.3)

        assert result.startswith(
            "Phase marked as final. Completed the last todo (Do the task in the "
            "brief.) - job will complete now."
        )
        assert "deliverable_warnings" not in _final_phase_data[JOB_ID]


class TestDescription:
    def _tool(self):
        context = MagicMock()
        context.has_workspace.return_value = True
        return create_job_tools(context)[1]

    def test_rewritten_without_todo_complete(self):
        job_complete = self._tool()

        (bound,) = apply_job_complete_description([job_complete], DIRECT_TASK_TOOLS)

        assert bound.description == JOB_COMPLETE_DESCRIPTION_WITHOUT_TODO_TOOL
        assert bound is not job_complete
        assert "strategic phase" in job_complete.description  # source untouched
        text = bound.description
        assert "ends the job" in text or "End the job" in text
        assert "summary:" in text and "deliverables:" in text
        assert "an empty list is valid" in text
        for word in ("strategic", "remaining", "todo_complete"):
            assert word not in text

    def test_kept_with_todo_complete(self):
        job_complete = self._tool()
        other = MagicMock()
        other.name = "write_file"

        bound = apply_job_complete_description([job_complete, other], PHASED_TOOLS)

        assert bound[0] is job_complete
        assert bound[1] is other

    def test_only_job_complete_is_rewritten(self):
        job_complete = self._tool()
        other = MagicMock()
        other.name = "write_file"

        bound = apply_job_complete_description([other, job_complete], DIRECT_TASK_TOOLS)

        assert bound[0] is other
        assert bound[1].description == JOB_COMPLETE_DESCRIPTION_WITHOUT_TODO_TOOL
