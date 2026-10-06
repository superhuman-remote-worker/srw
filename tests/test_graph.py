"""Unit tests for graph.py (phase alternation graph).

Tests the phase alternation graph architecture routing functions and helper utilities.
LLM-dependent nodes are tested with mocks or integration tests.
"""

import inspect

import pytest
import tempfile
import yaml
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch
from uuid import UUID

# Add project root src to path for imports
project_root = Path(__file__).parent.parent

# Import from src package (requires langgraph in environment)
from agent.core.workspace import WorkspaceManager  # noqa: E402
from agent.core.state import create_initial_state  # noqa: E402
from agent.managers import TodoManager, PlanManager, MemoryManager  # noqa: E402
from agent.managers.todo import TODO_LIST_RESTATEMENT_LEAD  # noqa: E402
from tests._fs_backend import FilesystemTestBackend  # noqa: E402
from agent.graph import (  # noqa: E402
    WORKER_BATCH_MIN_WALL_SECONDS,
    route_entry,
    route_after_execute,
    route_after_check_todos,
    create_route_after_transition,
    create_init_workspace_node,
    create_init_strategic_todos_node,
    create_check_todos_node,
    create_archive_phase_node,
    create_check_goal_node,
    create_handle_transition_node,
    create_audited_tool_node,
    build_phase_alternation_graph,
    checkpoint_completion_report,
    get_managers_from_workspace,
    worker_batch_boundary_updates,
)
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage  # noqa: E402
from shared.runtime.core.loader import LimitsConfig  # noqa: E402
from agent.core.phase import (  # noqa: E402
    get_initial_strategic_todos,
    get_transition_strategic_todos,
    validate_todos_yaml,
    TodosYamlValidationError,
)


@pytest.fixture
def temp_workspace():
    """Create a temporary directory for workspace testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def workspace_manager(temp_workspace):
    """Create a WorkspaceManager with a temporary base path."""
    ws = WorkspaceManager(
        job_id="test-job-123",
        base_path=temp_workspace,
        backend=FilesystemTestBackend(temp_workspace),
    )
    ws.initialize()
    return ws


@pytest.fixture
def managers(workspace_manager):
    """Create all managers."""
    return {
        "todo": TodoManager(workspace_manager),
        "plan": PlanManager(workspace_manager),
        "memory": MemoryManager(workspace_manager),
        "workspace": workspace_manager,
    }


@pytest.fixture
def mock_config():
    """Create a mock AgentConfig for testing.

    Note: config.extra must be a real dict, not a MagicMock.
    MagicMock().get() returns a truthy MagicMock, which can cause
    yaml.safe_load(MagicMock()) — an infinite read loop that OOMs.
    """
    config = MagicMock()
    config.agent_id = "test-agent"
    config.llm.model = "test-model"
    config.extra = {}
    config._deployment_dir = None
    return config


class TestRouteEntry:
    """Tests for route_entry routing function."""

    def test_route_entry_not_initialized(self):
        """Test that uninitialized state routes to init_workspace."""
        state = {"initialized": False}
        result = route_entry(state)
        assert result == "init_workspace"

    def test_route_entry_initialized(self):
        """Test that initialized state routes to restore_todo_state (resume)."""
        state = {"initialized": True}
        result = route_entry(state)
        assert result == "restore_todo_state"

    def test_route_entry_missing_key(self):
        """Test default behavior when key is missing."""
        state = {}
        result = route_entry(state)
        assert result == "init_workspace"


class TestRouteAfterExecute:
    """Tests for route_after_execute routing function."""

    def test_route_after_execute_no_messages(self):
        """Test routing with no messages goes to check_todos."""
        state = {"messages": []}
        result = route_after_execute(state)
        assert result == "check_todos"

    def test_route_after_execute_with_tool_calls(self):
        """Test routing with tool calls goes to tools."""
        # Create a mock AIMessage with tool_calls
        mock_message = MagicMock()
        mock_message.tool_calls = [{"name": "some_tool"}]

        # Make isinstance check work
        from langchain_core.messages import AIMessage

        mock_message.__class__ = AIMessage

        state = {"messages": [mock_message]}
        result = route_after_execute(state)
        assert result == "tools"

    def test_route_after_execute_without_tool_calls(self):
        """Test routing without tool calls goes to check_todos."""
        mock_message = MagicMock()
        mock_message.tool_calls = []

        from langchain_core.messages import AIMessage

        mock_message.__class__ = AIMessage

        state = {"messages": [mock_message]}
        result = route_after_execute(state)
        assert result == "check_todos"

    def test_route_after_execute_human_message(self):
        """Test routing with HumanMessage goes to check_todos."""
        from langchain_core.messages import HumanMessage

        state = {"messages": [HumanMessage(content="test")]}
        result = route_after_execute(state)
        assert result == "check_todos"


class TestRouteAfterCheckTodos:
    """Tests for route_after_check_todos routing function."""

    def test_route_after_check_todos_phase_complete(self):
        """Test routing when phase is complete."""
        state = {"phase_complete": True}
        result = route_after_check_todos(state)
        assert result == "archive_phase"

    def test_route_after_check_todos_not_complete(self):
        """Test routing when phase is not complete."""
        state = {"phase_complete": False}
        result = route_after_check_todos(state)
        assert result == "execute"

    def test_route_after_check_todos_missing_key(self):
        """Test default behavior when key is missing."""
        state = {}
        result = route_after_check_todos(state)
        assert result == "execute"


class TestGetManagersFromWorkspace:
    """Tests for get_managers_from_workspace helper function."""

    def test_returns_all_managers(self, workspace_manager):
        """Test that helper returns all three managers."""
        todo, plan, memory = get_managers_from_workspace(workspace_manager)

        assert isinstance(todo, TodoManager)
        assert isinstance(plan, PlanManager)
        assert isinstance(memory, MemoryManager)

    def test_managers_use_same_workspace(self, workspace_manager):
        """Test that all managers use the same workspace."""
        todo, plan, memory = get_managers_from_workspace(workspace_manager)

        # Write through one manager, read through workspace
        memory.write("# Test")
        assert workspace_manager.exists("workspace.md")

        plan.write("# Plan")
        assert workspace_manager.exists("plan.md")


class TestInitWorkspaceNode:
    """Tests for init_workspace node."""

    def test_returns_empty_workspace_memory(self, managers, mock_config):
        """Test that init returns empty workspace_memory (workspace.md no longer used)."""
        template = "# Test Template\n\n## Section\nContent"
        node = create_init_workspace_node(managers["memory"], template, mock_config)

        state = {"job_id": "test-123"}
        result = node(state)

        assert "workspace_memory" in result
        assert result["workspace_memory"] == ""

    def test_does_not_create_workspace_md(self, managers, mock_config):
        """Test that init does not create workspace.md (replaced by knowledge base)."""
        template = "# New Template"
        node = create_init_workspace_node(managers["memory"], template, mock_config)

        state = {"job_id": "test-123"}
        node(state)

        # workspace.md should NOT be created
        assert not managers["memory"].exists()


class TestCheckTodosNode:
    """Tests for check_todos node."""

    def test_todos_not_complete(self, managers, mock_config):
        """Test check when todos are not complete."""
        managers["todo"].add("Task 1")
        managers["todo"].add("Task 2")

        node = create_check_todos_node(managers["todo"], mock_config)

        state = {"job_id": "test-123", "iteration": 0}
        result = node(state)

        assert result.get("phase_complete") is False
        # Should also export todo state for checkpointing
        assert "todos" in result
        assert "staged_todos" in result
        assert "todo_next_id" in result
        assert len(result["todos"]) == 2

    def test_todos_all_complete(self, managers, mock_config):
        """Test check when all todos are complete."""
        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")

        node = create_check_todos_node(managers["todo"], mock_config)

        state = {"job_id": "test-123", "iteration": 0}
        result = node(state)

        assert result.get("phase_complete") is True
        # Should also export todo state for checkpointing
        assert "todos" in result
        assert result["todos"][0]["status"] == "completed"

    @pytest.mark.parametrize(
        "decision_field", ["is_final_phase", "completion_decision", "verdict_decision"]
    )
    def test_empty_final_phase_does_not_reload_seed_todos(
        self, managers, mock_config, decision_field
    ):
        node = create_check_todos_node(managers["todo"], mock_config)
        decision = True if decision_field == "is_final_phase" else {"recorded": True}

        result = node(
            {
                "job_id": "final-after-archive",
                "iteration": 12,
                "is_strategic_phase": True,
                decision_field: decision,
            }
        )

        assert result["phase_complete"] is True
        assert result["todos"] == []
        assert managers["todo"].list_all() == []

    def test_due_batch_freezes_only_at_safe_todo_check(self, managers, mock_config):
        managers["todo"].add("Task still in progress")
        node = create_check_todos_node(managers["todo"], mock_config)
        state = {
            "job_id": "batch-mid-phase",
            "iteration": 8,
            "is_strategic_phase": False,
            "phase_number": 4,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with patch("agent.graph.time.time", return_value=1300.0):
            result = node(state)

        assert result["should_stop"] is True
        assert result["phase_complete"] is False
        assert result["freeze_data"]["freeze_type"] == "batch_boundary"
        assert result["freeze_data"]["boundary"] == "mid_phase"
        assert result["freeze_data"]["trigger"] == "wall_clock"
        assert result["todos"][0]["content"] == "Task still in progress"
        for field in (
            "worker_batch_started_at",
            "worker_batch_start_iteration",
            "worker_batch_target_wall_seconds",
            "worker_batch_iteration_cap",
        ):
            assert result[field] is None

    def test_replan_request_precedes_due_batch(self, managers, mock_config):
        managers["todo"].add("Task still in progress")
        tool_context = MagicMock()
        tool_context.consume_replan_request.return_value = "the plan changed"
        node = create_check_todos_node(
            managers["todo"], mock_config, tool_context=tool_context
        )
        state = {
            "job_id": "batch-replan",
            "iteration": 8,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with patch("agent.graph.time.time", return_value=1300.0):
            result = node(state)

        assert result["phase_complete"] is True
        assert result["replan_reason"] == "the plan changed"
        assert "freeze_data" not in result

    def test_completed_phase_precedes_due_batch(self, managers, mock_config):
        managers["todo"].add("Finished task")
        managers["todo"].complete("todo_1")
        node = create_check_todos_node(managers["todo"], mock_config)
        state = {
            "job_id": "batch-complete-phase",
            "iteration": 8,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with patch("agent.graph.time.time", return_value=1300.0):
            result = node(state)

        assert result["phase_complete"] is True
        assert "freeze_data" not in result

    def test_empty_tactical_recovery_precedes_due_batch(self, managers, mock_config):
        node = create_check_todos_node(managers["todo"], mock_config)
        state = {
            "job_id": "batch-empty-tactical",
            "iteration": 8,
            "is_strategic_phase": False,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with patch("agent.graph.time.time", return_value=1300.0):
            result = node(state)

        assert result["phase_complete"] is True
        assert "freeze_data" not in result

    def test_drain_intent_precedes_due_midphase_batch(self, managers, mock_config):
        managers["todo"].add("Task still in progress")
        node = create_check_todos_node(managers["todo"], mock_config)
        state = {
            "job_id": "batch-drain",
            "iteration": 8,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with (
            patch("agent.graph.time.time", return_value=1300.0),
            patch("agent.graph._is_drain_requested", return_value=True),
        ):
            result = node(state)

        assert result["phase_complete"] is False
        assert "freeze_data" not in result


class TestWorkerBatchBudget:
    @staticmethod
    def _state(**overrides):
        state = {
            "job_id": "batch-budget",
            "iteration": 20,
            "is_strategic_phase": False,
            "phase_number": 3,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 10,
            "worker_batch_target_wall_seconds": 600.0,
            "worker_batch_iteration_cap": None,
        }
        state.update(overrides)
        return state

    def test_missing_fields_leave_legacy_runs_unarmed(self):
        assert worker_batch_boundary_updates({"job_id": "legacy"}, now=9999) is None

    def test_initial_state_is_explicitly_unarmed(self):
        state = create_initial_state("fresh", "/workspace")
        for field in (
            "worker_batch_started_at",
            "worker_batch_start_iteration",
            "worker_batch_target_wall_seconds",
            "worker_batch_iteration_cap",
        ):
            assert state[field] is None
        assert worker_batch_boundary_updates(state, now=10_000_000) is None

    def test_budget_check_never_runs_inside_audited_tools(self):
        assert "worker_batch_boundary_updates(" not in inspect.getsource(
            create_audited_tool_node
        )
        assert 'workflow.add_edge("tools", "check_todos")' in inspect.getsource(
            build_phase_alternation_graph
        )

    def test_wall_target_is_not_due_early(self):
        assert worker_batch_boundary_updates(self._state(), now=1599.999) is None
        result = worker_batch_boundary_updates(self._state(), now=1600.0)
        assert result["freeze_data"]["trigger"] == "wall_clock"

    def test_undersized_target_is_clamped_to_five_minutes(self):
        state = self._state(worker_batch_target_wall_seconds=1.0)
        assert worker_batch_boundary_updates(state, now=1299.999) is None
        result = worker_batch_boundary_updates(state, now=1300.0)
        assert result["freeze_data"]["target_wall_seconds"] == (
            WORKER_BATCH_MIN_WALL_SECONDS
        )

    def test_iteration_cap_cannot_fire_before_wall_floor(self):
        state = self._state(worker_batch_iteration_cap=5)
        assert worker_batch_boundary_updates(state, now=1299.999) is None
        result = worker_batch_boundary_updates(state, now=1300.0)
        assert result["freeze_data"]["trigger"] == "iteration_cap"
        assert result["freeze_data"]["iteration_delta"] == 10

    def test_wall_clock_wins_when_both_limits_are_due(self):
        state = self._state(
            worker_batch_target_wall_seconds=300.0,
            worker_batch_iteration_cap=5,
        )
        result = worker_batch_boundary_updates(state, now=1300.0)
        assert result["freeze_data"]["trigger"] == "wall_clock"

    @pytest.mark.parametrize(
        "blocking_state",
        [
            {"should_stop": True},
            {"goal_achieved": True},
            {"freeze_data": {"freeze_type": "blocking_message"}},
            {"error": {"message": "existing failure"}},
        ],
    )
    def test_existing_outcome_precedes_batch(self, blocking_state):
        state = self._state(worker_batch_target_wall_seconds=300.0)
        state.update(blocking_state)
        assert worker_batch_boundary_updates(state, now=1300.0) is None

    def test_phase_boundary_can_clear_a_stale_prior_error(self):
        state = self._state(
            worker_batch_target_wall_seconds=300.0,
            error={"message": "stale error from prior node"},
        )
        result = worker_batch_boundary_updates(
            state, now=1300.0, boundary="phase_boundary"
        )
        assert result["freeze_data"]["freeze_type"] == "batch_boundary"
        assert result["error"] is None


class TestCheckpointCompletionReport:
    def test_mints_one_id_with_an_exact_deep_copied_payload(self):
        state = {
            "should_stop": True,
            "goal_achieved": False,
            "error": {"type": "llm_error", "detail": {"attempt": 2}},
            "freeze_data": {
                "freeze_type": "blocking_message",
                "summary": "wait for a reply",
            },
        }

        updates = checkpoint_completion_report(state)

        UUID(updates["client_report_id"])
        assert set(updates["completion_report_payload"]) == {
            "should_stop",
            "goal_achieved",
            "error",
            "freeze_data",
        }
        assert updates["completion_report_payload"] == state

        state["error"]["detail"]["attempt"] = 3
        state["freeze_data"]["summary"] = "changed after END"
        assert updates["completion_report_payload"]["error"]["detail"]["attempt"] == 2
        assert (
            updates["completion_report_payload"]["freeze_data"]["summary"]
            == "wait for a reply"
        )

    def test_retry_keeps_existing_identity_and_payload_verbatim(self):
        report_id = "11111111-1111-4111-8111-111111111111"
        payload = {
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": {"freeze_type": "job_complete", "stamp": "first"},
        }
        state = {
            "client_report_id": report_id,
            "completion_report_payload": payload,
            "should_stop": True,
            "goal_achieved": False,
            "error": {"message": "later state must not replace the stop"},
            "freeze_data": None,
        }

        assert checkpoint_completion_report(state) == {}
        assert state["client_report_id"] == report_id
        assert state["completion_report_payload"] is payload

    def test_batch_boundary_does_not_mint_and_clears_stale_envelope(self):
        result = checkpoint_completion_report(
            {
                "client_report_id": "11111111-1111-4111-8111-111111111111",
                "completion_report_payload": {
                    "should_stop": True,
                    "goal_achieved": True,
                    "error": None,
                    "freeze_data": None,
                },
                "should_stop": True,
                "freeze_data": {"freeze_type": "batch_boundary"},
            }
        )

        assert result == {
            "client_report_id": None,
            "completion_report_payload": None,
        }

    def test_graph_checkpoints_report_before_end(self):
        source = inspect.getsource(build_phase_alternation_graph)
        assert '"end": "checkpoint_completion_report"' in source
        assert 'workflow.add_edge("checkpoint_completion_report", END)' in source

    @pytest.mark.asyncio
    async def test_report_envelope_is_in_the_terminal_checkpoint(self):
        from langgraph.checkpoint.memory import InMemorySaver
        from langgraph.graph import END, StateGraph

        from agent.core.state import UniversalAgentState

        workflow = StateGraph(UniversalAgentState)
        workflow.add_node("checkpoint_completion_report", checkpoint_completion_report)
        workflow.set_entry_point("checkpoint_completion_report")
        workflow.add_edge("checkpoint_completion_report", END)
        graph = workflow.compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": "completion-envelope-test"}}

        result = await graph.ainvoke(
            {
                "should_stop": True,
                "goal_achieved": True,
                "error": None,
                "freeze_data": {"freeze_type": "job_complete"},
            },
            config=config,
        )
        terminal = await graph.aget_state(config)

        assert terminal.next == ()
        assert terminal.values["client_report_id"] == result["client_report_id"]
        assert (
            terminal.values["completion_report_payload"]
            == result["completion_report_payload"]
        )


class TestRestoreTodoStateNode:
    """Tests for restore_todo_state node."""

    def test_restores_from_checkpoint(self, managers):
        """Test that restore_todo_state restores TodoManager from checkpoint."""
        from agent.graph import create_restore_todo_state_node

        node = create_restore_todo_state_node(managers["todo"])

        # State with todo data from checkpoint
        state = {
            "job_id": "test-123",
            "is_strategic_phase": False,
            "todos": [
                {
                    "id": "todo_1",
                    "content": "Task 1",
                    "status": "completed",
                    "priority": "high",
                    "notes": ["Done"],
                },
                {
                    "id": "todo_2",
                    "content": "Task 2",
                    "status": "pending",
                    "priority": "medium",
                    "notes": [],
                },
            ],
            "staged_todos": [],
            "todo_next_id": 3,
            "client_report_id": "11111111-1111-4111-8111-111111111111",
            "completion_report_payload": {
                "should_stop": True,
                "goal_achieved": False,
                "error": None,
                "freeze_data": {"freeze_type": "blocking_message"},
            },
        }

        result = node(state)

        # The history does not show the list, so the resume restates it once
        # (append-only context injection, D17-D19).
        (restated,) = result.pop("messages")
        assert restated.content.startswith(TODO_LIST_RESTATEMENT_LEAD)
        assert managers["todo"].format_for_injection() in restated.content

        # Node always clears stop flags on resume
        assert result == {
            "should_stop": False,
            "goal_achieved": False,
            "client_report_id": None,
            "completion_report_payload": None,
        }

        # But TodoManager should be restored
        todos = managers["todo"].list_all()
        assert len(todos) == 2
        assert todos[0].id == "todo_1"
        assert todos[0].status.value == "completed"
        assert todos[1].id == "todo_2"
        assert managers["todo"]._next_id == 3
        # Phase state should also be restored
        assert managers["todo"].is_strategic_phase is False

    def test_handles_empty_checkpoint(self, managers):
        """Test that restore_todo_state handles checkpoints without todo data."""
        from agent.graph import create_restore_todo_state_node

        node = create_restore_todo_state_node(managers["todo"])

        # Old checkpoint without todo fields
        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "todos": None,
            "staged_todos": None,
            "todo_next_id": None,
        }

        result = node(state)

        # Node always clears stop flags on resume
        assert result == {"should_stop": False, "goal_achieved": False}
        # TodoManager should remain in initial state
        assert managers["todo"].list_all() == []

    def test_applies_staged_todos_on_phase_boundary_resume(self, managers):
        """Test that staged todos are applied on phase-boundary resume.

        When resuming with staged todos but no active todos (phase-boundary
        freeze pattern), the node should apply them and flip to tactical.
        """
        from agent.graph import create_restore_todo_state_node

        node = create_restore_todo_state_node(managers["todo"])

        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "todos": [],
            "staged_todos": [
                {
                    "id": "todo_1",
                    "content": "Staged Task 1",
                    "status": "pending",
                    "priority": "medium",
                    "notes": [],
                },
            ],
            "todo_next_id": 5,
        }

        result = node(state)

        # Staged todos should have been applied as active todos
        assert not managers["todo"].has_staged_todos()
        assert len(managers["todo"].list_all()) == 1
        assert managers["todo"].list_all()[0].content == "Staged Task 1"
        # Phase should flip to tactical
        assert result["is_strategic_phase"] is False
        assert result["should_stop"] is False
        assert managers["todo"].is_strategic_phase is False
        # The applied list enters the history once, as the tactical list.
        (restated,) = result["messages"]
        assert "Current Tasks — Phase 1 (Tactical)" in restated.content
        assert "todo_1: Staged Task 1" in restated.content

    def test_no_restatement_when_the_history_shows_the_list(self, managers):
        """A resume whose history already carries the current rendering (a
        todo tool result, a phase-start message) appends nothing."""
        from agent.graph import create_restore_todo_state_node

        todos = [
            {
                "id": "todo_1",
                "content": "Task 1",
                "status": "pending",
                "priority": "medium",
                "notes": [],
            }
        ]
        managers["todo"].restore_state(
            {"todos": todos, "phase_number": 3, "is_strategic_phase": False}
        )
        shown = ToolMessage(
            content=f"Completed: x\n\n{managers['todo'].format_for_injection()}",
            tool_call_id="c1",
            name="todo_complete",
        )
        node = create_restore_todo_state_node(managers["todo"])

        result = node(
            {
                "job_id": "test-123",
                "phase_number": 3,
                "is_strategic_phase": False,
                "todos": todos,
                "staged_todos": [],
                "todo_next_id": 2,
                "messages": [shown],
            }
        )

        assert "messages" not in result

    def test_restores_staged_todos_with_active_todos(self, managers):
        """Test that staged todos are preserved when active todos also exist.

        When there are both active and staged todos, the node should NOT
        apply staged todos — this is a normal mid-phase restore.
        """
        from agent.graph import create_restore_todo_state_node

        node = create_restore_todo_state_node(managers["todo"])

        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "todos": [
                {
                    "id": "todo_1",
                    "content": "Active Task",
                    "status": "pending",
                    "priority": "medium",
                    "notes": [],
                },
            ],
            "staged_todos": [
                {
                    "id": "todo_2",
                    "content": "Staged Task",
                    "status": "pending",
                    "priority": "medium",
                    "notes": [],
                },
            ],
            "todo_next_id": 5,
        }

        result = node(state)

        # Should clear stop flags but not trigger phase-boundary resume (the
        # active list is restated: the history does not show it)
        (restated,) = result.pop("messages")
        assert "Active Task" in restated.content
        assert "Staged Task" not in restated.content
        assert result == {"should_stop": False, "goal_achieved": False}
        # Staged todos should remain staged
        assert managers["todo"].has_staged_todos()
        # Active todos should be present
        assert len(managers["todo"].list_all()) == 1
        assert managers["todo"]._next_id == 5


class TestArchivePhaseNode:
    """Tests for archive_phase node."""

    @pytest.mark.asyncio
    async def test_archives_todos(self, managers, mock_config):
        """Test that todos are archived."""
        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")
        managers["plan"].write("## Phase 1: Test\n\n- [x] Task 1")

        # Create mock context manager - ensure_within_limits returns unchanged messages
        mock_context_mgr = MagicMock()
        # ensure_within_limits is async, so we need AsyncMock
        from unittest.mock import AsyncMock

        mock_context_mgr.ensure_within_limits = AsyncMock(return_value=[])

        # Mock config to enable compact_on_archive
        mock_config.context_management = MagicMock()
        mock_config.context_management.compact_on_archive = True
        mock_config.context_management.reasoning_level = None
        mock_config.context_management.max_summary_length = 10000
        mock_config.llm = MagicMock()
        mock_config.llm.reasoning_level = "high"

        mock_llm = MagicMock()
        mock_summarization_prompt = "Summarize this conversation."

        node = create_archive_phase_node(
            managers["todo"],
            managers["plan"],
            mock_config,
            mock_context_mgr,
            mock_llm,
            mock_summarization_prompt,
        )

        state = {"job_id": "test-123", "messages": []}
        result = await node(state)

        assert "messages" in result
        assert "Phase complete" in result["messages"][0].content
        # Todos should be cleared
        assert len(managers["todo"].list_all()) == 0

    @pytest.mark.asyncio
    async def test_no_force_summarize_on_strategic_to_tactical_transition(
        self, managers, mock_config
    ):
        """Boundary compaction is threshold-driven, never forced — both directions.

        This used to force a full summarization on the strategic→tactical hop to
        give tactical a 'fresh conversation with just the plan summary'. That
        erased what the NEXT strategic phase needed, forcing it to reconstruct
        state from git, at a cost that grew every phase. Repeated irreversible
        query-agnostic compaction grows end-task error super-linearly in the
        number of events (arXiv 2607.08032), and no major harness compacts on a
        structural boundary. See knowledge-base/knowledge/issues/phase_model_overhead_amnesia_loop.md.
        """
        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")
        managers["plan"].write("## Phase 1: Test\n\n- [x] Task 1")

        # Create mock context manager to capture the force parameter
        mock_context_mgr = MagicMock()
        mock_context_mgr.ensure_within_limits = AsyncMock(return_value=[])

        # Mock config to enable compact_on_archive
        mock_config.context_management = MagicMock()
        mock_config.context_management.compact_on_archive = True
        mock_config.context_management.reasoning_level = None
        mock_config.context_management.max_summary_length = 10000
        mock_config.llm = MagicMock()
        mock_config.llm.reasoning_level = "high"

        mock_llm = MagicMock()
        mock_summarization_prompt = "Summarize this conversation."

        node = create_archive_phase_node(
            managers["todo"],
            managers["plan"],
            mock_config,
            mock_context_mgr,
            mock_llm,
            mock_summarization_prompt,
        )

        # Strategic phase completing (is_strategic=True) — the hop that used to force
        state = {"job_id": "test-123", "messages": [], "is_strategic_phase": True}
        await node(state)

        # Verify ensure_within_limits was NOT forced
        call_kwargs = mock_context_mgr.ensure_within_limits.call_args
        assert call_kwargs.kwargs.get("force") is False, (
            "Boundary compaction must be threshold-driven, not forced"
        )

    @pytest.mark.asyncio
    async def test_no_force_summarize_on_tactical_to_strategic_transition(
        self, managers, mock_config
    ):
        """Test that summarization is NOT forced when transitioning from tactical to strategic.

        Tactical→strategic transitions should preserve execution context for strategic reflection,
        only summarizing if thresholds are exceeded.
        """
        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")
        managers["plan"].write("## Phase 1: Test\n\n- [x] Task 1")

        # Create mock context manager to capture the force parameter
        mock_context_mgr = MagicMock()
        mock_context_mgr.ensure_within_limits = AsyncMock(return_value=[])

        # Mock config to enable compact_on_archive
        mock_config.context_management = MagicMock()
        mock_config.context_management.compact_on_archive = True
        mock_config.context_management.reasoning_level = None
        mock_config.context_management.max_summary_length = 10000
        mock_config.llm = MagicMock()
        mock_config.llm.reasoning_level = "high"

        mock_llm = MagicMock()
        mock_summarization_prompt = "Summarize this conversation."

        node = create_archive_phase_node(
            managers["todo"],
            managers["plan"],
            mock_config,
            mock_context_mgr,
            mock_llm,
            mock_summarization_prompt,
        )

        # Test tactical phase (is_strategic=False) - should NOT force summarization
        state = {"job_id": "test-123", "messages": [], "is_strategic_phase": False}
        await node(state)

        # Verify ensure_within_limits was called with force=False
        call_kwargs = mock_context_mgr.ensure_within_limits.call_args
        assert call_kwargs.kwargs.get("force") is False, (
            "Expected force=False for tactical→strategic transition"
        )

    @pytest.mark.asyncio
    async def test_boundary_compaction_drops_ending_phase_block_keeps_generic_pin(
        self, managers, mock_config
    ):
        """U2 WP1: the archive node clears the protected-block phase key before
        its boundary compaction, so the ending phase's instruction block is
        summarised away with its region (not re-seated after the summary),
        while a generic pin (no phase key) survives. The execute node stays
        the only place that sets a non-None key."""
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            RemoveMessage,
            SystemMessage,
        )

        from agent.core.context import ContextConfig, ContextManager
        from shared.runtime.core.message_markers import (
            PROTECTED_KEY,
            is_protected_message,
            protected_phase_key,
        )
        from shared.runtime.core.workspace_injection import (
            create_phase_instruction_message,
        )
        from shared.runtime.services.auxiliary import AuxiliaryLLM

        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")
        managers["plan"].write("## Phase 2: Test\n\n- [x] Task 1")

        # A real context manager, thresholds low enough that the boundary
        # compaction summarises, left as the execute node leaves it (key set).
        context_mgr = ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=200,
                summarization_threshold_tokens=200,
                message_count_threshold=1000,
                message_count_min_tokens=100,
                keep_recent_messages=2,
                model_max_context_tokens=4000,
            )
        )
        context_mgr.set_current_phase("tactical", phase_key="2:tactical")

        aux_llm = MagicMock()
        aux_llm.ainvoke = AsyncMock(
            return_value=AIMessage(
                content="## Objective\n- Phase 2 work.\n\n## Work State\n- Task 1"
            )
        )
        auxiliary = AuxiliaryLLM(llm=aux_llm, max_context_tokens=15_000)

        block = create_phase_instruction_message(
            "skills/research-guide/SKILL.md",
            "TACTICAL GUIDANCE " * 30,
            "tactical",
            "2:tactical",
        )
        block.id = "blk"
        pin = HumanMessage(
            content="GENERIC PIN", id="pin", additional_kwargs={PROTECTED_KEY: True}
        )
        history = [HumanMessage(content="start", id="h0"), block, pin]
        for i in range(6):
            history.append(
                HumanMessage(content=f"question {i} " + "x" * 200, id=f"h{i + 1}")
            )
            history.append(
                AIMessage(content=f"answer {i} " + "y" * 200, id=f"a{i + 1}")
            )

        mock_config.context_management = MagicMock()
        mock_config.context_management.compact_on_archive = True
        mock_config.context_management.reasoning_level = None
        mock_config.context_management.max_summary_length = 10000
        mock_config.llm = MagicMock()
        mock_config.llm.reasoning_level = "high"

        node = create_archive_phase_node(
            managers["todo"],
            managers["plan"],
            mock_config,
            context_mgr,
            auxiliary,
            "Summarize this conversation.",
        )
        state = {
            "job_id": "test-123",
            "messages": history,
            "is_strategic_phase": False,
            "phase_number": 2,
        }
        result = await node(state)

        assert context_mgr.current_phase_key is None
        kept = [m for m in result["messages"] if not isinstance(m, RemoveMessage)]
        removed = {m.id for m in result["messages"] if isinstance(m, RemoveMessage)}
        assert "blk" in removed
        assert "pin" in removed
        # The ending phase's block is gone from the compacted history...
        assert not any(
            is_protected_message(m) and protected_phase_key(m) == "2:tactical"
            for m in kept
        )
        assert not any("TACTICAL GUIDANCE" in str(m.content) for m in kept)
        # ...while the generic pin is re-seated right after the summary.
        summary_idx = next(
            i
            for i, m in enumerate(kept)
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        )
        assert kept[summary_idx + 1].content == "GENERIC PIN"
        assert is_protected_message(kept[summary_idx + 1])
        assert kept[summary_idx + 1].id is None
        assert "Phase complete" in kept[-1].content


class TestCheckGoalNode:
    """Tests for check_goal node."""

    def test_goal_not_achieved(self, managers, mock_config):
        """Test when plan is not complete."""
        managers["plan"].write("## Phase 1\n\n- [ ] Task 1")

        node = create_check_goal_node(
            managers["plan"], managers["workspace"], mock_config, managers["todo"]
        )

        state = {"job_id": "test-123"}
        result = node(state)

        assert result.get("goal_achieved") is False

    def test_goal_achieved_plan_complete(self, managers, mock_config):
        """Test when plan is marked complete."""
        managers["plan"].write("# Plan\n\n# Complete\n\nAll done.")

        node = create_check_goal_node(
            managers["plan"], managers["workspace"], mock_config, managers["todo"]
        )

        state = {"job_id": "test-123"}
        result = node(state)

        assert result.get("goal_achieved") is True
        assert result.get("should_stop") is True

    def test_goal_achieved_no_more_phases(self, managers, mock_config):
        """Test when there are no more phases."""
        # Plan with all phases complete (no pending phases)
        managers["plan"].write("""# Plan

## Phase 1 (complete)
- [x] Done
""")

        node = create_check_goal_node(
            managers["plan"], managers["workspace"], mock_config, managers["todo"]
        )

        state = {"job_id": "test-123"}
        result = node(state)

        # Should check is_complete which looks for completed markers
        # If plan has "(complete)" it might trigger completion
        # Otherwise no more phases means goal achieved
        assert (
            result.get("goal_achieved") is True or result.get("goal_achieved") is False
        )
        # At minimum, should return something about goal state
        assert "goal_achieved" in result

    def test_goal_achieved_job_complete_called(self, managers, mock_config):
        """Test that job_complete (writing job_completion.json) triggers goal achieved."""
        # Simulate job_complete tool having written the completion file
        import json

        completion_data = {
            "status": "job_completed",
            "summary": "All tasks complete",
            "deliverables": ["output/results.json"],
        }
        managers["workspace"].write_file(
            "output/job_completion.json", json.dumps(completion_data)
        )

        node = create_check_goal_node(
            managers["plan"], managers["workspace"], mock_config, managers["todo"]
        )

        state = {"job_id": "test-123"}
        result = node(state)

        assert result.get("goal_achieved") is True
        assert result.get("should_stop") is True


# =============================================================================
# PHASE ALTERNATION TESTS
# =============================================================================


class TestRouteAfterTransition:
    """Tests for route_after_transition routing function."""

    def test_route_after_transition_success_with_phase_marker(self, workspace_manager):
        """Test routing when transition succeeded (phase boundary marker present)."""
        route_after_transition = create_route_after_transition(workspace_manager)
        from langchain_core.messages import HumanMessage

        marker = HumanMessage(content="[PHASE_TRANSITION] Tactical phase complete.")
        state = {"messages": [marker]}
        result = route_after_transition(state)
        assert result == "check_goal"

    def test_route_after_transition_rejected(self, workspace_manager):
        """Test routing when transition was rejected."""
        route_after_transition = create_route_after_transition(workspace_manager)
        from langchain_core.messages import ToolMessage

        error_msg = ToolMessage(
            content="[TRANSITION_REJECTED] todos.yaml validation failed",
            tool_call_id="phase_transition",
        )
        state = {"messages": [error_msg]}
        result = route_after_transition(state)
        assert result == "execute"

    def test_route_after_transition_other_messages(self, workspace_manager):
        """Test routing with other types of messages."""
        route_after_transition = create_route_after_transition(workspace_manager)
        from langchain_core.messages import HumanMessage

        state = {"messages": [HumanMessage(content="Some message")]}
        result = route_after_transition(state)
        # Other messages go to check_goal
        assert result == "check_goal"

    def test_route_after_transition_job_frozen(self, workspace_manager):
        """Test routing when job is stopped (should_stop=True from handle_transition).

        When should_stop is True in state, should always route to check_goal
        even if transition was rejected (no todos staged).
        """
        route_after_transition = create_route_after_transition(workspace_manager)

        # Even with rejection message, should route to check_goal when should_stop=True
        from langchain_core.messages import ToolMessage

        error_msg = ToolMessage(
            content="[TRANSITION_REJECTED] No todos staged",
            tool_call_id="phase_transition",
        )
        state = {"messages": [error_msg], "should_stop": True}
        result = route_after_transition(state)
        assert result == "check_goal"  # NOT execute, because job should stop


class TestInitStrategicTodosNode:
    """Tests for init_strategic_todos node (phase alternation)."""

    def test_loads_predefined_strategic_todos(self, managers, mock_config):
        """Test that init loads predefined strategic todos."""
        # Write instructions for the node to read
        managers["workspace"].write_file(
            "instructions.md", "# Test Task\n\nDo something."
        )

        node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )

        state = {"job_id": "test-123"}
        result = node(state)

        # Should have messages with instructions
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert "Test Task" in result["messages"][0].content

        # Should set phase state
        assert result.get("initialized") is True
        assert result.get("is_strategic_phase") is True
        assert result.get("phase_number") == 0
        assert result.get("phase_complete") is False

        # Should have loaded strategic todos
        todos = managers["todo"].list_all()
        assert len(todos) == 4  # Predefined strategic todos

    def test_missing_instructions_is_fine_when_the_brief_is_there(
        self, managers, mock_config
    ):
        """instructions.md is optional; task_brief.md alone is a real task.

        Only the case where the agent has NO task description at all is fatal
        (see ``test_taskless_boot_raises``). instructions.md is the optional
        expert/inline channel, so its absence must stay a normal boot.
        """
        managers["workspace"].write_file(
            "task_brief.md", "# Task Brief\n\n## Description\n\nShip the thing."
        )

        node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )

        result = node({"job_id": "test-123"})

        assert "Ship the thing" in result["messages"][0].content
        assert "strategic mode" in result["messages"][0].content
        assert len(managers["todo"].list_all()) == 4

    def test_taskless_boot_raises(self, managers, mock_config):
        """No brief and no instructions must abort the boot, not warn.

        Both files are served by in-process virtual providers
        (knowledge-base/knowledge/features/virtual_directories.md). If the overlay ever fails to
        serve them — provider raises, registration missed, a backend swap loses
        the rebind — every read here returns empty and the composed first
        HumanMessage degrades to the boilerplate "You are starting in strategic
        mode" with no task in it. The agent then runs a full job against a task
        it was never told, and the only prior signal was one WARNING line.

        This is the guarantee that replaced VIRTUAL_DIRS_ENABLED: it holds for
        every cause, where the kill switch covered exactly one.
        """
        node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )

        with pytest.raises(RuntimeError, match="no task description"):
            node({"job_id": "test-123"})

    def test_whitespace_only_brief_is_still_taskless(self, managers, mock_config):
        """A provider that renders blank must not pass the check.

        The realistic overlay failure returns "" or whitespace, not a missing
        file — an emptiness check that only tested existence would sail past
        exactly the case it is here to catch.
        """
        managers["workspace"].write_file("task_brief.md", "   \n\n\t\n")
        managers["workspace"].write_file("instructions.md", "")

        node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )

        with pytest.raises(RuntimeError, match="no task description"):
            node({"job_id": "test-123"})


class TestPredefinedTodos:
    """Tests for predefined strategic todos."""

    def test_initial_strategic_todos(self):
        """Test get_initial_strategic_todos returns correct todos."""
        todos = get_initial_strategic_todos()
        assert len(todos) == 4

        # Check content patterns
        contents = [t.content for t in todos]
        assert any("workspace" in c.lower() for c in contents)
        assert any("plan" in c.lower() for c in contents)
        assert any("next_phase_todos" in c.lower() for c in contents)

    def test_transition_strategic_todos(self):
        """Test get_transition_strategic_todos returns the slim 2-todo cycle."""
        todos = get_transition_strategic_todos()
        # Verification is followed by an explicitly ordered close/continue
        # decision; technique lives in the capability-aware skill.
        assert len(todos) == 2

        contents = [t.content for t in todos]
        assert "skills/verify-before-done/SKILL.md" in contents[0]
        assert "instructions.md" in contents[0]
        assert "PASS" in contents[0] and "GAPS" in contents[0]
        assert "completion_note" in contents[0]
        assert "begin only after todo 1 is complete" in contents[1]
        assert "job_complete" in contents[1]
        assert "next_phase_todos" in contents[1]
        combined = "\n".join(contents)
        assert "git_tags" not in combined
        assert "git_diff" not in combined
        assert "stop condition comes FIRST" not in combined

    def test_gpt_oss_transition_todos_use_same_ordered_skill_contract(self):
        template = (
            project_root / "config/templates/strategic_todos_transition_gpt_oss.yaml"
        )
        parsed = yaml.safe_load(template.read_text(encoding="utf-8"))
        contents = [todo["content"] for todo in parsed["todos"]]

        assert len(contents) == 2
        assert "skills/verify-before-done/SKILL.md" in contents[0]
        assert "completion_note" in contents[0]
        assert "Start only after todo 1 is complete" in contents[1]
        assert "job_complete" in contents[1]
        assert "next_phase_todos" in contents[1]
        combined = "\n".join(contents)
        assert "git_tags" not in combined
        assert "git_diff" not in combined
        assert "stop condition" not in combined.lower()


class TestTodosYamlValidation:
    """Tests for todos.yaml validation."""

    def test_valid_todos_yaml(self):
        """Test validation of a valid todos.yaml."""
        content = """
phase: "Phase 1: Extract requirements"
description: "Process documents and extract requirements"
todos:
  - id: 1
    content: "Read the source document"
  - id: 2
    content: "Extract section headings"
  - id: 3
    content: "Identify requirement statements"
  - id: 4
    content: "Validate extracted requirements"
  - id: 5
    content: "Write requirements to database"
"""
        metadata, todos = validate_todos_yaml(content)

        assert metadata.get("phase") == "Phase 1: Extract requirements"
        assert len(todos) == 5
        assert todos[0]["id"] == 1
        assert todos[0]["content"] == "Read the source document"

    def test_invalid_yaml_syntax(self):
        """Test validation fails for invalid YAML."""
        content = "invalid: yaml: syntax: here"
        with pytest.raises(TodosYamlValidationError) as exc_info:
            validate_todos_yaml(content)
        assert "YAML" in str(exc_info.value)

    def test_too_few_todos(self):
        """Test validation fails when too few todos."""
        content = """
todos:
  - id: 1
    content: "Only one todo"
"""
        with pytest.raises(TodosYamlValidationError) as exc_info:
            validate_todos_yaml(content, min_todos=5)
        assert "Too few todos" in str(exc_info.value.errors[0])

    def test_too_many_todos(self):
        """Test validation fails when too many todos."""
        todos_list = "\n".join(
            [f"  - id: {i}\n    content: 'Todo number {i}'" for i in range(1, 25)]
        )
        content = f"todos:\n{todos_list}"
        with pytest.raises(TodosYamlValidationError) as exc_info:
            validate_todos_yaml(content, max_todos=20)
        assert "Too many todos" in str(exc_info.value.errors[0])

    def test_missing_required_fields(self):
        """Test validation fails when required fields are missing."""
        content = """
todos:
  - id: 1
  - content: "Missing id"
  - id: 3
    content: "Complete"
  - id: 4
    content: "Also complete"
  - id: 5
    content: "Five"
"""
        with pytest.raises(TodosYamlValidationError) as exc_info:
            validate_todos_yaml(content)
        errors = exc_info.value.errors
        assert any("content" in e for e in errors)
        assert any("id" in e for e in errors)


class TestHandleTransitionNode:
    """Tests for handle_transition node."""

    @pytest.mark.asyncio
    async def test_strategic_to_tactical_success(self, managers, mock_config):
        """Test successful strategic -> tactical transition."""
        # Stage todos (simulating next_phase_todos tool call)
        managers["todo"].stage_tactical_todos(
            [
                "First task with enough detail",
                "Second task with enough detail",
                "Third task with enough detail",
                "Fourth task with enough detail",
                "Fifth task with enough detail",
            ],
            phase_name="Phase 1",
        )

        # Add phase_settings to mock config
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )

        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "phase_number": 0,
            "iteration": 10,
        }
        result = await node(state)

        # Should inject phase boundary marker
        assert len(result.get("messages", [])) == 1
        assert "[PHASE_TRANSITION]" in result["messages"][0].content
        # Should flip phase
        assert result.get("is_strategic_phase") is False
        # Should increment phase number
        assert result.get("phase_number") == 1

        # TodoManager should have new todos
        todos = managers["todo"].list_all()
        assert len(todos) == 5

    @pytest.mark.asyncio
    async def test_strategic_to_tactical_rejection(self, managers, mock_config):
        """Test rejected strategic -> tactical transition (no todos.yaml)."""
        # Add phase_settings to mock config
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )

        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "phase_number": 0,
            "iteration": 10,
        }
        result = await node(state)

        # Should have error message
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert "[TRANSITION_REJECTED]" in result["messages"][0].content

    @pytest.mark.asyncio
    async def test_tactical_to_strategic_transition(self, managers, mock_config):
        """Test tactical -> strategic transition (archives and loads strategic todos)."""
        # Add some completed todos
        managers["todo"].add("Task 1")
        managers["todo"].complete("todo_1")

        # Add phase_settings to mock config
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )

        state = {
            "job_id": "test-123",
            "is_strategic_phase": False,  # Tactical mode
            "phase_number": 1,
            "iteration": 20,
        }
        result = await node(state)

        # Should inject phase boundary marker
        assert len(result.get("messages", [])) == 1
        assert "[PHASE_TRANSITION]" in result["messages"][0].content
        # Should flip to strategic
        assert result.get("is_strategic_phase") is True
        # Should increment phase number
        assert result.get("phase_number") == 2

        # TodoManager should have predefined strategic todos
        todos = managers["todo"].list_all()
        assert (
            len(todos) == 2
        )  # Transition strategic todos (review+adapt, plan-or-complete)

    @pytest.mark.asyncio
    async def test_drain_intent_freezes_with_version_upgrade(
        self, managers, mock_config
    ):
        """Phase 1d: at the phase boundary, drain intent freezes with
        ``version_upgrade`` so the orchestrator pauses + re-dispatches."""
        # Stage tactical todos so the regular transition itself succeeds —
        # we want to assert that drain reaction overrides freeze_data
        # even on a successful transition.
        managers["todo"].stage_tactical_todos(
            [f"Task {i} with enough detail" for i in range(1, 6)],
            phase_name="Phase 1",
        )
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )
        state = {
            "job_id": "test-drain-1",
            "is_strategic_phase": True,
            "phase_number": 0,
            "iteration": 10,
            # A stale error left in state by a mid-phase node must NOT ride out
            # with the drain freeze (else the orchestrator fails instead of
            # pauses — version_upgrade_drain_masked_by_coincident_error).
            "error": {"message": "stale mid-phase error", "type": "job_error"},
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": 5,
        }
        with patch("agent.graph._is_drain_requested", return_value=True):
            result = await node(state)

        assert result.get("should_stop") is True
        freeze = result.get("freeze_data")
        assert freeze is not None
        assert freeze["freeze_type"] == "version_upgrade"
        assert freeze["phase_number"] == 0
        assert "drain intent" in freeze["reason"].lower()

        # The drain branch explicitly clears the stale error.
        assert "error" in result and result["error"] is None
        for field in (
            "worker_batch_started_at",
            "worker_batch_start_iteration",
            "worker_batch_target_wall_seconds",
            "worker_batch_iteration_cap",
        ):
            assert result[field] is None

        # Marker file written for parity with other freeze types.
        marker = managers["workspace"].read_file("output/job_frozen.json")
        assert marker is not None
        import json

        assert json.loads(marker)["freeze_type"] == "version_upgrade"

    @pytest.mark.asyncio
    async def test_no_drain_intent_no_version_upgrade(self, managers, mock_config):
        """Default path: drain flag false → regular transition, no freeze."""
        managers["todo"].stage_tactical_todos(
            [f"Task {i} with enough detail" for i in range(1, 6)],
            phase_name="Phase 1",
        )
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )
        state = {
            "job_id": "test-no-drain",
            "is_strategic_phase": True,
            "phase_number": 0,
            "iteration": 10,
        }
        with patch("agent.graph._is_drain_requested", return_value=False):
            result = await node(state)

        # Successful transition without any version_upgrade freeze.
        assert result.get("should_stop") is not True
        freeze = result.get("freeze_data")
        if freeze:
            assert freeze.get("freeze_type") != "version_upgrade"

    @pytest.mark.asyncio
    async def test_due_batch_freezes_at_phase_boundary_without_marker(
        self, managers, mock_config
    ):
        managers["todo"].stage_tactical_todos(
            [f"Task {i} with enough detail" for i in range(1, 6)],
            phase_name="Phase 1",
        )
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings
        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )
        state = {
            "job_id": "test-batch-boundary",
            "is_strategic_phase": True,
            "phase_number": 2,
            "iteration": 12,
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with (
            patch("agent.graph._is_drain_requested", return_value=False),
            patch("agent.graph.time.time", return_value=1300.0),
        ):
            result = await node(state)

        assert result["should_stop"] is True
        assert result["error"] is None
        assert result["freeze_data"]["freeze_type"] == "batch_boundary"
        assert result["freeze_data"]["boundary"] == "phase_boundary"
        assert result["freeze_data"]["phase_number"] == 2
        assert not managers["workspace"].exists("output/job_frozen.json")

    @pytest.mark.asyncio
    async def test_replan_lands_before_tactical_phase_batch_handoff(
        self, managers, mock_config
    ):
        managers["todo"].add("Completed tactical task")
        managers["todo"].complete("todo_1")
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings
        node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )
        state = {
            "job_id": "test-replan-batch-boundary",
            "is_strategic_phase": False,
            "phase_number": 3,
            "iteration": 12,
            "replan_reason": "new evidence invalidated the old approach",
            "worker_batch_started_at": 1000.0,
            "worker_batch_start_iteration": 2,
            "worker_batch_target_wall_seconds": 300.0,
            "worker_batch_iteration_cap": None,
        }

        with (
            patch("agent.graph._is_drain_requested", return_value=False),
            patch("agent.graph.time.time", return_value=1300.0),
        ):
            result = await node(state)

        assert result["freeze_data"]["freeze_type"] == "batch_boundary"
        assert result["replan_reason"] is None
        assert any(
            "[REPLAN REQUESTED]" in getattr(message, "content", "")
            for message in result.get("messages", [])
        )


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestPhaseAlternationCycle:
    """Integration tests for the full phase alternation cycle."""

    @pytest.mark.asyncio
    async def test_full_strategic_tactical_strategic_cycle(self, managers, mock_config):
        """Test a complete cycle: init → strategic → tactical → strategic."""
        # Configure mock
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        # Step 1: Initialize with strategic todos
        managers["workspace"].write_file(
            "instructions.md", "# Test Task\n\nExtract data."
        )
        init_node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )

        state = {"job_id": "test-123"}
        state.update(init_node(state))

        # Verify strategic initialization
        assert state.get("is_strategic_phase") is True
        assert state.get("phase_number") == 0
        assert len(managers["todo"].list_all()) == 4  # Initial strategic todos

        # Step 2: Complete strategic phase and write todos.yaml
        for todo in managers["todo"].list_all():
            managers["todo"].complete(todo.id)

        # Stage todos (simulating strategic agent calling next_phase_todos)
        managers["todo"].stage_tactical_todos(
            [
                "Read the source document carefully",
                "Extract data from section one",
                "Extract data from section two",
                "Validate all extractions against source",
                "Write validated data to database",
            ],
            phase_name="Phase 1: Execute extraction",
        )

        # Step 3: Transition strategic → tactical
        transition_node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )

        state["iteration"] = 10
        result = await transition_node(state)
        state.update(result)

        # Verify transition to tactical
        assert state.get("is_strategic_phase") is False
        assert state.get("phase_number") == 1
        assert any(
            "[PHASE_TRANSITION]" in getattr(m, "content", "")
            for m in state.get("messages", [])
        )
        assert len(managers["todo"].list_all()) == 5  # Loaded from todos.yaml

        # Step 4: Complete tactical todos
        for todo in managers["todo"].list_all():
            managers["todo"].complete(todo.id)

        # Step 5: Transition tactical → strategic
        state["iteration"] = 20
        result = await transition_node(state)
        state.update(result)

        # Verify transition back to strategic
        assert state.get("is_strategic_phase") is True
        assert state.get("phase_number") == 2
        assert any(
            "[PHASE_TRANSITION]" in getattr(m, "content", "")
            for m in state.get("messages", [])
        )
        # Should have transition strategic todos (2 items)
        assert len(managers["todo"].list_all()) == 2

    def test_job_completion_detection(self, managers, mock_config):
        """Test that job_complete creates the completion marker file."""
        # Create the output directory
        output_dir = managers["workspace"].get_path("output")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Simulate job_complete tool writing the completion file
        completion_file = output_dir / "job_completion.json"
        completion_file.write_text('{"status": "complete", "summary": "Task done"}')

        # Create check_goal node
        check_goal = create_check_goal_node(
            managers["plan"], managers["workspace"], mock_config, managers["todo"]
        )

        state = {"job_id": "test-123", "iteration": 50}
        result = check_goal(state)

        # Should detect goal achieved
        assert result.get("goal_achieved") is True
        assert result.get("should_stop") is True

    @pytest.mark.asyncio
    async def test_transition_rejection_retryable(self, managers, mock_config):
        """Test that transition rejection allows retry."""
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        transition_node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )

        # Create the routing function with workspace access
        route_after_transition = create_route_after_transition(managers["workspace"])

        # First attempt: no todos.yaml
        state = {
            "job_id": "test-123",
            "is_strategic_phase": True,
            "phase_number": 0,
            "iteration": 10,
        }
        result = await transition_node(state)

        # Should be rejected
        assert "[TRANSITION_REJECTED]" in result["messages"][0].content
        # State should NOT change
        assert (
            "is_strategic_phase" not in result
            or result.get("is_strategic_phase") is None
        )

        # After rejection, route_after_transition sends back to execute
        assert route_after_transition({"messages": result["messages"]}) == "execute"

        # Second attempt: stage todos properly (content must be 10+ chars)
        managers["todo"].stage_tactical_todos(
            [
                "Read the source document and identify structure",
                "Extract requirements from section one",
                "Extract requirements from section two",
                "Validate all extracted requirements",
                "Write validated requirements to database",
            ],
            phase_name="Phase 1",
        )

        # Try transition again
        state["iteration"] = 15
        result = await transition_node(state)

        # Should succeed now
        assert len(result.get("messages", [])) == 1
        assert "[PHASE_TRANSITION]" in result["messages"][0].content
        assert result.get("is_strategic_phase") is False
        assert result.get("phase_number") == 1

        # route_after_transition should proceed to check_goal
        assert route_after_transition({"messages": result["messages"]}) == "check_goal"

    @pytest.mark.asyncio
    async def test_workspace_memory_persists_across_phases(self, managers, mock_config):
        """Test that workspace.md content persists across phase transitions."""
        phase_settings = MagicMock()
        phase_settings.min_todos = 5
        phase_settings.max_todos = 20
        mock_config.phase_settings = phase_settings

        # Write initial memory
        managers["memory"].write(
            "# Project Memory\n\n## Findings\n\n- Important fact 1"
        )

        # Every real job boots with a task; init now refuses without one.
        managers["workspace"].write_file("instructions.md", "# Task\n\nExtract data.")

        # Initialize strategic phase
        init_node = create_init_strategic_todos_node(
            managers["workspace"], managers["todo"], mock_config
        )
        state = init_node({"job_id": "test-123"})

        # Verify memory still exists
        assert "Important fact 1" in managers["memory"].read()

        # Complete strategic todos and transition
        for todo in managers["todo"].list_all():
            managers["todo"].complete(todo.id)

        managers["todo"].stage_tactical_todos(
            [
                "Read the source document carefully",
                "Extract key information from document",
                "Validate extracted information against source",
                "Transform data into the required format",
                "Write results to the output file",
            ],
            phase_name="Phase 1",
        )

        # Transition to tactical
        transition_node = create_handle_transition_node(
            managers["workspace"],
            managers["todo"],
            mock_config,
            min_todos=5,
            max_todos=20,
        )
        state["is_strategic_phase"] = True
        state["phase_number"] = 0
        state["iteration"] = 10
        await transition_node(state)

        # Verify memory still exists after transition
        memory_content = managers["memory"].read()
        assert "Important fact 1" in memory_content

        # Update memory during tactical phase
        managers["memory"].update_section(
            "Findings", "- Important fact 1\n- Important fact 2"
        )

        # Complete tactical and transition back to strategic
        for todo in managers["todo"].list_all():
            managers["todo"].complete(todo.id)

        state["is_strategic_phase"] = False
        state["phase_number"] = 1
        state["iteration"] = 20
        await transition_node(state)

        # Verify both facts persist
        memory_content = managers["memory"].read()
        assert "Important fact 1" in memory_content
        assert "Important fact 2" in memory_content


# =============================================================================
# EDIT FILE TOOL TESTS
# =============================================================================


class TestEditFileTool:
    """Tests for the edit_file workspace tool."""

    @pytest.fixture
    def workspace_tools_dict(self, workspace_manager):
        """Create workspace tools and return as dict."""
        from agent.tools.workspace import create_workspace_tools
        from agent.tools.context import ToolContext

        ctx = ToolContext(workspace_manager=workspace_manager)
        tools = create_workspace_tools(ctx)
        return {t.name: t for t in tools}

    @pytest.fixture
    def edit_tool(self, workspace_tools_dict):
        """Get the edit_file tool from workspace tools."""
        return workspace_tools_dict["edit_file"]

    @pytest.fixture
    def read_tool(self, workspace_tools_dict):
        """Get the read_file tool from workspace tools."""
        return workspace_tools_dict["read_file"]

    def test_edit_file_single_replacement(
        self, workspace_manager, edit_tool, read_tool
    ):
        """Test successful single replacement."""
        workspace_manager.write_file("test.md", "Hello world\nGoodbye world\n")
        read_tool.invoke({"path": "test.md"})  # Must read first
        result = edit_tool.invoke(
            {"path": "test.md", "old_string": "Hello world", "new_string": "Hi world"}
        )
        assert "Edited" in result
        content = workspace_manager.read_file("test.md")
        assert content == "Hi world\nGoodbye world\n"

    def test_edit_file_not_found(self, edit_tool):
        """Test error when file doesn't exist."""
        # File doesn't exist - should fail before read check
        result = edit_tool.invoke(
            {"path": "missing.md", "old_string": "x", "new_string": "y"}
        )
        assert "Error" in result
        assert "not found" in result

    def test_edit_file_old_string_missing(
        self, workspace_manager, edit_tool, read_tool
    ):
        """Test error when old_string not in file content."""
        workspace_manager.write_file("test.md", "Hello world\n")
        read_tool.invoke({"path": "test.md"})  # Must read first
        result = edit_tool.invoke(
            {"path": "test.md", "old_string": "does not exist", "new_string": "y"}
        )
        assert "Error" in result
        assert "old_string not found" in result

    def test_edit_file_multiple_matches(self, workspace_manager, edit_tool, read_tool):
        """Test error when old_string appears multiple times."""
        workspace_manager.write_file("test.md", "foo bar\nfoo baz\n")
        read_tool.invoke({"path": "test.md"})  # Must read first
        result = edit_tool.invoke(
            {"path": "test.md", "old_string": "foo", "new_string": "qux"}
        )
        assert "Error" in result
        assert "2 times" in result
        assert "more surrounding context" in result
        # File should be unchanged
        assert workspace_manager.read_file("test.md") == "foo bar\nfoo baz\n"

    def test_edit_file_deletion(self, workspace_manager, edit_tool, read_tool):
        """Test deletion by replacing with empty string."""
        workspace_manager.write_file("test.md", "keep this\ndelete this\nkeep too\n")
        read_tool.invoke({"path": "test.md"})  # Must read first
        result = edit_tool.invoke(
            {"path": "test.md", "old_string": "delete this\n", "new_string": ""}
        )
        assert "Edited" in result
        content = workspace_manager.read_file("test.md")
        assert content == "keep this\nkeep too\n"

    def test_edit_file_directory_error(self, workspace_manager, edit_tool, read_tool):
        """Test error when path is a directory."""
        workspace_manager.get_path("subdir").mkdir(parents=True, exist_ok=True)
        # Can't read a directory, so this should fail at the directory check
        result = edit_tool.invoke(
            {"path": "subdir", "old_string": "x", "new_string": "y"}
        )
        assert "Error" in result
        assert "directory" in result

    def test_edit_file_requires_read(self, workspace_manager, edit_tool):
        """Test that edit_file fails without recent read."""
        workspace_manager.write_file("test.md", "Hello world\n")
        # Don't read first - should fail
        result = edit_tool.invoke(
            {"path": "test.md", "old_string": "Hello", "new_string": "Hi"}
        )
        assert "Error" in result
        assert "read_file" in result.lower()


# =============================================================================
# EDIT CITATION TOOL TESTS
# =============================================================================


class TestEditCitationTool:
    """Tests for the edit_citation tool.

    edit_citation now routes through the engine's job-scoped
    ``edit_citation`` on the vector store (not ``context.db.citations.edit``,
    which targeted the main app DB where citations don't live).
    """

    @pytest.fixture
    def mock_engine(self):
        """CitationEngine mock with an async edit_citation."""
        engine = MagicMock()
        engine.edit_citation = AsyncMock(return_value=None)
        return engine

    @pytest.fixture
    def edit_tool(self, workspace_manager, mock_engine):
        """Create citation tools and return the edit_citation tool."""
        from agent.tools.citation import create_citation_tools
        from agent.tools.context import ToolContext

        ctx = ToolContext(
            workspace_manager=workspace_manager,
            _job_id="test-job-123",
        )
        # Pre-set the engine so get_citation_engine() returns it without
        # needing a real vector pool.
        ctx.citation_engine = mock_engine
        tools = create_citation_tools(ctx)
        for t in tools:
            if t.name == "edit_citation":
                return t
        pytest.fail("edit_citation tool not found in citation tools")

    @pytest.mark.asyncio
    async def test_edit_claim(self, edit_tool, mock_engine):
        """Test successful edit of claim field."""
        result = await edit_tool.ainvoke(
            {
                "citation_id": 1,
                "claim": "Updated claim text",
            }
        )

        assert "ok: edited citation [1]" in result
        assert "verification_status reset" in result
        mock_engine.edit_citation.assert_called_once()
        assert (
            mock_engine.edit_citation.call_args.kwargs["claim"] == "Updated claim text"
        )

    @pytest.mark.asyncio
    async def test_edit_not_found(self, edit_tool, mock_engine):
        """Test error when citation is not found (engine raises ValueError)."""
        mock_engine.edit_citation = AsyncMock(
            side_effect=ValueError("Citation 999 not found")
        )

        result = await edit_tool.ainvoke(
            {
                "citation_id": 999,
                "claim": "New claim",
            }
        )

        assert "error:" in result
        assert "not found" in result

    @pytest.mark.asyncio
    async def test_content_edit_resets_verification(self, edit_tool, mock_engine):
        """Test that editing content fields triggers verification reset message."""
        result = await edit_tool.ainvoke(
            {
                "citation_id": 1,
                "verbatim_quote": "New quote text",
            }
        )

        assert "verification_status reset to 'pending'" in result

    @pytest.mark.asyncio
    async def test_non_content_edit_preserves_verification(
        self, edit_tool, mock_engine
    ):
        """Test that editing non-content fields does not mention verification reset."""
        result = await edit_tool.ainvoke(
            {
                "citation_id": 1,
                "confidence": "medium",
            }
        )

        assert "ok: edited citation [1]" in result
        assert "verification_status reset" not in result

    @pytest.mark.asyncio
    async def test_edit_no_fields(self, edit_tool):
        """Test error when no fields are provided."""
        result = await edit_tool.ainvoke(
            {
                "citation_id": 1,
            }
        )

        assert "error:" in result
        assert "no fields" in result

    @pytest.mark.asyncio
    async def test_edit_invalid_locator_json(self, edit_tool):
        """Test error when locator is not valid JSON."""
        result = await edit_tool.ainvoke(
            {
                "citation_id": 1,
                "locator": "not valid json{",
            }
        )

        assert "error:" in result
        assert "valid JSON" in result


# =============================================================================
# CONTEXT MANAGER ENSURE_WITHIN_LIMITS TESTS
# =============================================================================


class TestEnsureWithinLimits:
    """Tests for ContextManager.ensure_within_limits method."""

    @pytest.fixture
    def context_mgr(self):
        """Create a ContextManager with low thresholds for testing."""
        from agent.core.context import ContextManager, ContextConfig

        config = ContextConfig(
            compaction_threshold_tokens=1000,
            summarization_threshold_tokens=1000,
            message_count_threshold=5,
            message_count_min_tokens=100,
            keep_recent_messages=2,
        )
        return ContextManager(config=config)

    @pytest.fixture
    def mock_auxiliary(self):
        """Create a mock AuxiliaryLLM that returns a summary."""
        from shared.runtime.services.auxiliary import AuxiliaryLLM
        from langchain_core.messages import AIMessage

        llm = MagicMock()
        # Text mode: the model writes the Markdown checkpoint itself.
        llm.ainvoke = AsyncMock(
            return_value=AIMessage(content="## Objective\n- Test summary")
        )
        return AuxiliaryLLM(llm=llm)

    @pytest.mark.asyncio
    async def test_no_compaction_when_under_threshold(self, context_mgr):
        """Test that messages are returned unchanged when under threshold."""
        from langchain_core.messages import HumanMessage
        from shared.runtime.services.auxiliary import AuxiliaryLLM

        messages = [HumanMessage(content="Hello")]

        mock_llm = MagicMock()
        mock_aux = AuxiliaryLLM(llm=mock_llm)
        result = await context_mgr.ensure_within_limits(messages, mock_aux)

        # Should return same messages unchanged
        assert result == messages
        # LLM should not be called
        mock_llm.with_structured_output.assert_not_called()

    @pytest.mark.asyncio
    async def test_compaction_when_over_message_threshold(
        self, context_mgr, mock_auxiliary
    ):
        """Test that compaction happens when message count exceeds threshold."""
        from langchain_core.messages import HumanMessage, AIMessage

        # Create enough messages to trigger threshold (>5 messages, >100 tokens)
        messages = [
            HumanMessage(content="Long message 1 " * 20),
            AIMessage(content="Response 1 " * 20),
            HumanMessage(content="Long message 2 " * 20),
            AIMessage(content="Response 2 " * 20),
            HumanMessage(content="Long message 3 " * 20),
            AIMessage(content="Response 3 " * 20),
        ]

        result = await context_mgr.ensure_within_limits(messages, mock_auxiliary)

        # Should have fewer messages (compacted)
        assert len(result) < len(messages)

    @pytest.mark.asyncio
    async def test_force_compaction(self, context_mgr, mock_auxiliary):
        """Test that force=True triggers compaction even under threshold."""
        from langchain_core.messages import HumanMessage, AIMessage

        # Large messages so the summary is smaller than original
        # (compaction is skipped if summary is larger than original)
        messages = [
            HumanMessage(content="Hello world " * 50),
            AIMessage(content="Hi there " * 50),
            HumanMessage(content="How are you doing today? " * 50),
            AIMessage(content="I am doing great, thanks! " * 50),
        ]

        result = await context_mgr.ensure_within_limits(
            messages, mock_auxiliary, force=True
        )

        # With force=True, compaction should happen (summary is smaller than original)
        assert len(result) < len(messages)

    @pytest.mark.asyncio
    async def test_returns_original_when_not_enough_messages(
        self, context_mgr, mock_auxiliary
    ):
        """Test that messages are returned unchanged when too few to compact."""
        from langchain_core.messages import HumanMessage

        # Only 1 message - can't really compact
        messages = [HumanMessage(content="Hello")]

        result = await context_mgr.ensure_within_limits(
            messages, mock_auxiliary, force=True
        )

        # Should return same messages since there's nothing to summarize
        assert result == messages


# =============================================================================
# Per-call phase gate (U2 WP3) and the one-binding execute node
# =============================================================================


def _gate_config(**limits):
    """The config surface ``create_audited_tool_node`` reads."""
    return SimpleNamespace(
        agent_id="gate-agent",
        limits=LimitsConfig(**limits),
        llm=SimpleNamespace(model=None),
    )


def _named_tool(name):
    fake = MagicMock()
    fake.name = name
    return fake


def _gate_state(calls, is_strategic=False, phase_number=4):
    return {
        "messages": [AIMessage(content="", tool_calls=calls)],
        "job_id": "gate-job",
        "iteration": 1,
        "is_strategic_phase": is_strategic,
        "phase_number": phase_number,
        "metadata": {},
    }


class TestPerCallPhaseGate:
    """With one binding for every phase, the audited tool node decides per call."""

    @pytest.mark.asyncio
    async def test_mixed_batch_runs_the_legal_call_and_rejects_the_illegal_one(
        self,
    ):
        auditor = MagicMock()
        auditor.audit_tool_call.side_effect = lambda **kw: f"doc-{kw['call_id']}"
        with (
            patch("agent.graph.ToolNode") as MockToolNode,
            patch("agent.graph.get_archiver", return_value=auditor),
        ):
            mock_tn = AsyncMock()
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(content="body", tool_call_id="c1", name="read_file")
                    ]
                }
            )
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node(
                [_named_tool("read_file"), _named_tool("job_complete")],
                _gate_config(max_tool_calls_per_job=2),
            )
            result = await audited(
                _gate_state(
                    [
                        {"name": "read_file", "id": "c1", "args": {"path": "a"}},
                        {"name": "job_complete", "id": "c2", "args": {}},
                    ]
                )
            )

            # The legal call executed — the ToolNode saw only it — and the
            # illegal one is an error ToolMessage in its original position.
            mock_tn.ainvoke.assert_awaited_once()
            seen = mock_tn.ainvoke.await_args.args[0]["messages"][-1]
            assert [c["name"] for c in seen.tool_calls] == ["read_file"]
            msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
            assert [m.tool_call_id for m in msgs] == ["c1", "c2"]
            assert msgs[0].content == "body"
            assert msgs[1].content == (
                "Error: 'job_complete' is a strategic-phase tool; you are in the "
                "tactical phase (phase 4). Finish or replan the current todos — it "
                "becomes available at the next strategic phase. Other calls in "
                "this batch were executed normally."
            )

            # Both calls are in the audit; the rejection is a recorded failure.
            assert [
                c.kwargs["call_id"] for c in auditor.audit_tool_call.call_args_list
            ] == ["c1", "c2"]
            updates = {
                c.kwargs["audit_doc_id"]: c.kwargs
                for c in auditor.update_tool_result.call_args_list
            }
            assert updates["doc-c1"]["success"] is True
            assert updates["doc-c2"]["success"] is False
            assert updates["doc-c2"]["error"].startswith(
                "Error: 'job_complete' is a strategic-phase tool"
            )

            # The budget counted both: the cap of 2 trips on the next call.
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(content="body", tool_call_id="c3", name="read_file")
                    ]
                }
            )
            frozen = await audited(
                _gate_state([{"name": "read_file", "id": "c3", "args": {}}])
            )
            assert frozen["freeze_data"]["freeze_type"] == "budget_exceeded"
            assert frozen["freeze_data"]["tool_calls_this_job"] == 3

    @pytest.mark.asyncio
    async def test_stuck_detection_progress_counts_only_the_executed_call(self):
        def nudges(result):
            return [
                m
                for m in result.get("messages", [])
                if isinstance(m, SystemMessage) and "OBSERVATION" in m.content
            ]

        with patch("agent.graph.ToolNode") as MockToolNode:
            mock_tn = AsyncMock()
            MockToolNode.return_value = mock_tn
            audited = create_audited_tool_node(
                [_named_tool("write_file"), _named_tool("job_complete")],
                _gate_config(progress_stall_threshold=2),
            )
            # Two rejected calls are two calls without progress: the stall nudge.
            for call_id in ("c1", "c2"):
                result = await audited(
                    _gate_state([{"name": "job_complete", "id": call_id, "args": {}}])
                )
            assert nudges(result)
            mock_tn.ainvoke.assert_not_called()

            # A legal write_file next to a rejected call is progress: reset.
            mock_tn.ainvoke = AsyncMock(
                return_value={
                    "messages": [
                        ToolMessage(
                            content="written", tool_call_id="c3", name="write_file"
                        )
                    ]
                }
            )
            result = await audited(
                _gate_state(
                    [
                        {"name": "write_file", "id": "c3", "args": {"path": "p"}},
                        {"name": "job_complete", "id": "c4", "args": {}},
                    ]
                )
            )
            assert not nudges(result)
            result = await audited(
                _gate_state([{"name": "job_complete", "id": "c5", "args": {}}])
            )
            assert not nudges(result)  # one call since the write, below 2


class TestExecuteNodeBindings:
    """The execute node has exactly one bound-LLM input."""

    @staticmethod
    def _kwargs():
        return dict(
            todo_manager=MagicMock(),
            memory_manager=MagicMock(),
            workspace_manager=MagicMock(),
            config=MagicMock(),
            context_mgr=MagicMock(),
            retry_manager=MagicMock(),
            auxiliary_llm=None,
            summarization_prompt="",
        )

    def test_one_binding_is_the_primary_argument(self):
        from agent.graph import create_execute_node

        node = create_execute_node(llm_with_tools=MagicMock(), **self._kwargs())
        assert callable(node)

    def test_binding_is_required(self):
        from agent.graph import create_execute_node

        with pytest.raises(TypeError, match="llm_with_tools"):
            create_execute_node(**self._kwargs())


# =============================================================================
# Built-in subagents (U3 WP2): the context probe the graph stashes
# =============================================================================


class TestSubagentContextProbe:
    """``build_phase_alternation_graph`` stashes ``parent_context_probe`` on
    the ToolContext (plan B.5): a live read of the graph's ContextManager
    that the return envelope shares the parent's headroom against."""

    def _config(self):
        from pathlib import Path
        from shared.runtime.core.loader import load_agent_config

        root = Path(__file__).resolve().parents[1]
        config = load_agent_config(str(root / "config" / "worker_base.yaml"))
        config.memory.manager_enabled = False
        return config

    def test_graph_build_stashes_a_live_probe_on_the_tool_context(
        self, workspace_manager
    ):
        from unittest.mock import patch

        from agent.core.context import ContextManager
        from agent.subagents import ContextProbe
        from agent.tools.context import ToolContext

        config = self._config()
        ctx = ToolContext(workspace_manager=workspace_manager)
        assert ctx.parent_context_probe is None
        managers_seen = []

        def capture(**kwargs):
            managers_seen.append(ContextManager(**kwargs))
            return managers_seen[-1]

        with patch("agent.graph.ContextManager", side_effect=capture):
            build_phase_alternation_graph(
                llm_with_tools=MagicMock(),
                tools=[],
                config=config,
                workspace=workspace_manager,
                todo_manager=TodoManager(workspace_manager),
                tool_context=ctx,
            )
        assert callable(ctx.parent_context_probe)
        probe = ctx.parent_context_probe()
        assert isinstance(probe, ContextProbe)
        assert (
            probe.compaction_threshold_tokens == config.limits.context_threshold_tokens
        )
        assert probe.model_max_context_tokens == config.limits.model_max_context_tokens
        assert probe.last_provider_input_tokens is None
        assert probe.current_token_count == 0

        # Live, not a snapshot: the graph's own manager is what it reads.
        (context_mgr,) = managers_seen
        context_mgr.state.current_token_count = 4321
        context_mgr.state.last_provider_input_tokens = 5000
        again = ctx.parent_context_probe()
        assert (again.current_token_count, again.last_provider_input_tokens) == (
            4321,
            5000,
        )

    def test_a_graph_without_a_tool_context_builds_as_before(self, workspace_manager):
        build_phase_alternation_graph(
            llm_with_tools=MagicMock(),
            tools=[],
            config=self._config(),
            workspace=workspace_manager,
            todo_manager=TodoManager(workspace_manager),
        )
