"""Tests for request_replan — the in-flight adaptation path.

This replaced `todo_rewind`, which was a genuine rewind: it archived every
todo (including the completed ones) into `archive/failed_<time>.md` and
emptied the list, so the strategic phase that followed inherited no record of
what had actually been achieved and had to reconstruct it. It also only
reached the strategic phase *indirectly*, by leaving the todo list empty until
`check_todos` hit its "no todos in tactical phase — forcing phase complete to
recover" branch, which exists for resume bugs.

The replacement keeps every todo with its real status and asks for the phase
boundary explicitly. That matters more as tactical phases grow: with roughly
three phases per job it is the only way to adapt mid-job.
"""

from unittest.mock import MagicMock

import pytest

from agent.graph import create_check_todos_node, create_handle_transition_node
from agent.managers.todo import TodoManager
from agent.tools.context import ToolContext


def make_config():
    config = MagicMock()
    config.extra = {}  # must be a real dict — MagicMock loops yaml.safe_load
    config.phase_settings = MagicMock(min_todos=5, max_todos=20)
    return config


def make_todo_manager(completed=0, total=5):
    mgr = TodoManager(workspace=MagicMock())
    mgr.stage_tactical_todos(
        [f"Do the thing number {i} properly" for i in range(total)]
    )
    mgr.apply_staged_todos()
    for todo in mgr.list_all()[:completed]:
        mgr.complete(todo.id)
    return mgr


def make_context(todo_mgr):
    ws = MagicMock()
    ws.exists.return_value = True
    ws.read_file.return_value = "x" * 100
    return ToolContext(workspace_manager=ws, todo_manager=todo_mgr, _job_id="job-1")


def get_tool(context):
    from agent.tools.core.todo import create_todo_tools

    return next(t for t in create_todo_tools(context) if t.name == "request_replan")


class TestNothingIsLost:
    """The whole point: a replan is not an undo."""

    def test_active_todos_survive(self):
        mgr = make_todo_manager(completed=2, total=5)
        ctx = make_context(mgr)

        get_tool(ctx).invoke({"reason": "The API caps batches at 100 items"})

        assert len(mgr.list_all()) == 5

    def test_completed_todos_stay_completed(self):
        """The old tool archived these as 'failed' along with everything else."""
        mgr = make_todo_manager(completed=3, total=5)
        ctx = make_context(mgr)

        get_tool(ctx).invoke({"reason": "found a blocker"})

        statuses = [t.status.value for t in mgr.list_all()]
        assert statuses.count("completed") == 3

    def test_pending_todos_stay_pending(self):
        mgr = make_todo_manager(completed=1, total=5)
        ctx = make_context(mgr)

        get_tool(ctx).invoke({"reason": "found a blocker"})

        statuses = [t.status.value for t in mgr.list_all()]
        assert statuses.count("completed") == 1
        assert len(statuses) == 5

    def test_no_failure_archive_is_written(self):
        """archive_phase records the real statuses at the boundary instead."""
        mgr = make_todo_manager(completed=2, total=5)
        mgr.archive_with_failure_note = MagicMock()
        ctx = make_context(mgr)

        get_tool(ctx).invoke({"reason": "found a blocker"})

        mgr.archive_with_failure_note.assert_not_called()

    def test_progress_is_reported_back(self):
        mgr = make_todo_manager(completed=3, total=5)
        ctx = make_context(mgr)

        result = get_tool(ctx).invoke({"reason": "found a blocker"})

        assert "3/5" in result
        assert "nothing was" in result.lower()


class TestRequestSignal:
    def test_reason_is_parked_on_the_context(self):
        mgr = make_todo_manager()
        ctx = make_context(mgr)

        get_tool(ctx).invoke({"reason": "the schema changed underneath us"})

        assert ctx.consume_replan_request() == "the schema changed underneath us"

    def test_reason_is_consumed_once(self):
        mgr = make_todo_manager()
        ctx = make_context(mgr)
        get_tool(ctx).invoke({"reason": "x marks the spot"})

        assert ctx.consume_replan_request() == "x marks the spot"
        assert ctx.consume_replan_request() is None

    def test_empty_reason_is_rejected_and_sets_nothing(self):
        mgr = make_todo_manager()
        ctx = make_context(mgr)

        result = get_tool(ctx).invoke({"reason": "   "})

        assert "Error" in result
        assert ctx.consume_replan_request() is None


class TestCheckTodosEndsThePhase:
    def test_replan_completes_the_phase_with_todos_outstanding(self):
        """Without a replan, incomplete todos keep the phase running."""
        mgr = make_todo_manager(completed=1, total=5)
        ctx = make_context(mgr)
        node = create_check_todos_node(mgr, make_config(), tool_context=ctx)

        # Baseline: still work to do, phase continues.
        assert (
            node({"job_id": "job-1", "is_strategic_phase": False})["phase_complete"]
            is False
        )

        get_tool(ctx).invoke({"reason": "approach is wrong"})
        result = node({"job_id": "job-1", "is_strategic_phase": False})

        assert result["phase_complete"] is True
        assert result["replan_reason"] == "approach is wrong"

    def test_replan_exports_todo_state_for_the_checkpoint(self):
        mgr = make_todo_manager(completed=2, total=5)
        ctx = make_context(mgr)
        node = create_check_todos_node(mgr, make_config(), tool_context=ctx)
        get_tool(ctx).invoke({"reason": "approach is wrong"})

        result = node({"job_id": "job-1", "is_strategic_phase": False})

        assert len(result["todos"]) == 5

    def test_no_tool_context_is_harmless(self):
        mgr = make_todo_manager(completed=5, total=5)
        node = create_check_todos_node(mgr, make_config(), tool_context=None)

        assert (
            node({"job_id": "job-1", "is_strategic_phase": False})["phase_complete"]
            is True
        )


class TestTransitionCarriesTheReason:
    def _node(self):
        return create_handle_transition_node(
            MagicMock(),
            make_todo_manager(),
            make_config(),
            min_todos=5,
            max_todos=20,
        )

    @pytest.mark.asyncio
    async def test_reason_reaches_the_strategic_phase(self):
        result = await self._node()(
            {
                "job_id": "job-1",
                "is_strategic_phase": False,
                "phase_number": 2,
                "iteration": 5,
                "replan_reason": "the API caps batches at 100 items",
            }
        )

        bodies = [getattr(m, "content", "") for m in result.get("messages", [])]
        replan = [b for b in bodies if "[REPLAN REQUESTED]" in b]
        assert replan, "the incoming strategic phase was told nothing"
        assert "the API caps batches at 100 items" in replan[0]

    @pytest.mark.asyncio
    async def test_reason_is_cleared_so_it_cannot_steer_a_later_phase(self):
        result = await self._node()(
            {
                "job_id": "job-1",
                "is_strategic_phase": False,
                "phase_number": 2,
                "iteration": 5,
                "replan_reason": "one-time reason",
            }
        )

        assert result.get("replan_reason") is None

    @pytest.mark.asyncio
    async def test_ordinary_transition_says_nothing_about_replanning(self):
        result = await self._node()(
            {
                "job_id": "job-1",
                "is_strategic_phase": False,
                "phase_number": 2,
                "iteration": 5,
            }
        )

        bodies = [getattr(m, "content", "") for m in result.get("messages", [])]
        assert not any("[REPLAN REQUESTED]" in b for b in bodies)


class TestRegistration:
    def test_tool_is_tactical_only(self):
        from agent.tools.core.todo import TODO_TOOLS_METADATA

        assert TODO_TOOLS_METADATA["request_replan"]["phases"] == ["tactical"]

    def test_old_name_is_gone(self):
        from agent.tools.core.todo import TODO_TOOLS_METADATA

        assert "todo_rewind" not in TODO_TOOLS_METADATA


class TestLiveSessionEnd:
    """The end of a live-session job's session ends its tactical phase like a
    replan, and the boundary loads the post-session todos (live_session_jobs
    §6.1)."""

    def _live_tactical(self):
        mgr = TodoManager(workspace=MagicMock())
        mgr.set_todos_from_list(
            [
                {
                    "id": "todo_1",
                    "content": "Handle the live session until it ends.",
                    "status": "pending",
                    "priority": "medium",
                }
            ]
        )
        return mgr

    def test_the_end_parks_a_replan_and_the_reason_together(self):
        ctx = make_context(make_todo_manager())
        ctx.end_live_session("remote_hangup")

        assert ctx._replan_request == "live session ended: remote_hangup"
        assert ctx.consume_live_session_end() == "remote_hangup"
        # Taking the end takes its replan with it, never an unrelated one.
        assert ctx.consume_replan_request() is None
        ctx.request_replan("the agent's own reason")
        ctx.end_live_session("expired")
        ctx.request_replan("the agent's own reason")
        assert ctx.consume_live_session_end() == "expired"
        assert ctx.consume_replan_request() == "the agent's own reason"

    def test_check_todos_ends_the_tactical_phase_and_sets_the_flag(self):
        mgr = self._live_tactical()
        ctx = make_context(mgr)
        node = create_check_todos_node(mgr, make_config(), tool_context=ctx)
        state = {"job_id": "job-1", "is_strategic_phase": False, "phase_number": 0}

        assert node(state)["phase_complete"] is False
        ctx.end_live_session("remote_hangup")
        result = node(state)

        assert result["phase_complete"] is True
        assert result["replan_reason"] == "live session ended: remote_hangup"
        assert result["live_session_ended"] == {
            "reason": "remote_hangup",
            "phase_number": 0,
        }
        assert len(result["todos"]) == 1  # the live todo stays as it was
        assert ctx.consume_replan_request() is None

    def test_a_replan_read_before_the_end_still_carries_the_flag(self):
        """The runtime parks both at once; check_todos may see the replan
        first, and must not leave the end behind for a later phase."""
        mgr = self._live_tactical()
        ctx = make_context(mgr)
        node = create_check_todos_node(mgr, make_config(), tool_context=ctx)
        consume = ctx.consume_live_session_end
        calls = []

        def late_consume():
            calls.append(1)
            if len(calls) == 1:
                ctx.end_live_session("expired")  # parked between the reads
                return None
            return consume()

        ctx.consume_live_session_end = late_consume
        result = node(
            {"job_id": "job-1", "is_strategic_phase": False, "phase_number": 0}
        )

        assert result["phase_complete"] is True
        assert result["live_session_ended"]["reason"] == "expired"

    def test_a_strategic_phase_only_learns_the_news(self):
        mgr = make_todo_manager(completed=1, total=5)
        ctx = make_context(mgr)
        node = create_check_todos_node(mgr, make_config(), tool_context=ctx)
        ctx.end_live_session("remote_hangup")

        result = node(
            {"job_id": "job-1", "is_strategic_phase": True, "phase_number": 2}
        )

        assert result["phase_complete"] is False
        assert "replan_reason" not in result
        assert result["live_session_ended"]["phase_number"] == 2
        (banner,) = result["messages"]
        assert banner.content.startswith("[LIVE SESSION ENDED: remote_hangup]")
        assert banner.additional_kwargs.get("_srw_persist_role") == "event"
        assert ctx.consume_replan_request() is None

    @pytest.mark.asyncio
    async def test_the_boundary_loads_the_post_session_todos(self):
        mgr = TodoManager(workspace=MagicMock())
        node = create_handle_transition_node(
            MagicMock(), mgr, make_config(), min_todos=5, max_todos=20
        )
        result = await node(
            {
                "job_id": "job-1",
                "is_strategic_phase": False,
                "phase_number": 0,
                "iteration": 5,
                "replan_reason": "live session ended: remote_hangup",
                "live_session_ended": {"reason": "remote_hangup", "phase_number": 0},
            }
        )

        assert result["is_strategic_phase"] is True
        assert result["phase_number"] == 1
        bodies = [getattr(m, "content", "") for m in result.get("messages", [])]
        assert bodies[0].startswith("[LIVE SESSION ENDED: remote_hangup]")
        assert "Current Tasks — Phase 1 (Strategic)" in bodies[0]
        assert not any("[REPLAN REQUESTED]" in b for b in bodies)
        assert result.get("replan_reason") is None
        (todo,) = mgr.list_all()
        assert todo.content.startswith(
            "Review the session (transcript, questions, promises)."
        )

    @pytest.mark.asyncio
    async def test_a_later_tactical_phase_transitions_as_usual(self):
        mgr = TodoManager(workspace=MagicMock())
        node = create_handle_transition_node(
            MagicMock(), mgr, make_config(), min_todos=5, max_todos=20
        )
        result = await node(
            {
                "job_id": "job-1",
                "is_strategic_phase": False,
                "phase_number": 2,
                "iteration": 9,
                "live_session_ended": {"reason": "remote_hangup", "phase_number": 0},
            }
        )

        bodies = [getattr(m, "content", "") for m in result.get("messages", [])]
        assert bodies[0].startswith("[PHASE_TRANSITION] Tactical phase complete")
        assert not any("LIVE SESSION ENDED" in b for b in bodies)

    def test_a_lost_post_session_list_is_reloaded(self):
        """A resume that lost the post-session phase's todos reloads them,
        not the transition template."""
        mgr = TodoManager(workspace=MagicMock())
        node = create_check_todos_node(mgr, make_config(), tool_context=None)

        result = node(
            {
                "job_id": "job-1",
                "is_strategic_phase": True,
                "phase_number": 1,
                "live_session_ended": {"reason": "expired", "phase_number": 0},
            }
        )

        assert result["phase_complete"] is False
        (todo,) = mgr.list_all()
        assert todo.content.startswith("Review the session")
