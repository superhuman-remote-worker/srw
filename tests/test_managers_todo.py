"""Unit tests for TodoManager (managers package).

Tests the stateful todo management for the nested loop graph architecture.
"""

import pytest
import tempfile
from datetime import datetime
from pathlib import Path

from agent.core.workspace import WorkspaceManager
from agent.managers.todo import TodoManager, TodoItem, TodoStatus
from tests._fs_backend import FilesystemTestBackend


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
def todo_manager(workspace_manager):
    """Create a TodoManager for testing."""
    return TodoManager(workspace_manager)


class TestTodoStatus:
    """Tests for TodoStatus enum."""

    def test_status_values(self):
        """Test that all expected status values exist."""
        assert TodoStatus.PENDING.value == "pending"
        assert TodoStatus.IN_PROGRESS.value == "in_progress"
        assert TodoStatus.COMPLETED.value == "completed"


class TestTodoItem:
    """Tests for TodoItem dataclass."""

    def test_create_basic(self):
        """Test creating a basic todo item."""
        item = TodoItem(id="todo_1", content="Test task")
        assert item.id == "todo_1"
        assert item.content == "Test task"
        assert item.status == TodoStatus.PENDING
        assert item.priority == "medium"
        assert item.notes == []
        assert isinstance(item.created_at, datetime)

    def test_create_with_options(self):
        """Test creating a todo with all options."""
        item = TodoItem(
            id="todo_2",
            content="High priority task",
            status=TodoStatus.IN_PROGRESS,
            priority="high",
            notes=["note 1", "note 2"],
        )
        assert item.status == TodoStatus.IN_PROGRESS
        assert item.priority == "high"
        assert item.notes == ["note 1", "note 2"]

    def test_to_dict(self):
        """Test serializing to dictionary."""
        item = TodoItem(
            id="todo_3",
            content="Serialize me",
            status=TodoStatus.COMPLETED,
            priority="low",
            notes=["done"],
        )
        data = item.to_dict()

        assert data["id"] == "todo_3"
        assert data["content"] == "Serialize me"
        assert data["status"] == "completed"
        assert data["priority"] == "low"
        assert data["notes"] == ["done"]
        assert "created_at" in data

    def test_from_dict(self):
        """Test deserializing from dictionary."""
        data = {
            "id": "todo_4",
            "content": "Deserialize me",
            "status": "in_progress",
            "priority": "high",
            "notes": ["started"],
            "created_at": "2024-01-15T10:30:00+00:00",
        }
        item = TodoItem.from_dict(data)

        assert item.id == "todo_4"
        assert item.content == "Deserialize me"
        assert item.status == TodoStatus.IN_PROGRESS
        assert item.priority == "high"
        assert item.notes == ["started"]

    def test_from_dict_defaults(self):
        """Test deserializing with minimal data uses defaults."""
        data = {"id": "todo_5", "content": "Minimal"}
        item = TodoItem.from_dict(data)

        assert item.status == TodoStatus.PENDING
        assert item.priority == "medium"
        assert item.notes == []


class TestTodoManagerBasics:
    """Tests for basic TodoManager operations."""

    def test_add_todo(self, todo_manager):
        """Test adding a todo item."""
        item = todo_manager.add("Test task")

        assert item.id == "todo_1"
        assert item.content == "Test task"
        assert item.priority == "medium"
        assert item.status == TodoStatus.PENDING

    def test_add_multiple_todos(self, todo_manager):
        """Test adding multiple todos generates unique IDs."""
        item1 = todo_manager.add("Task 1")
        item2 = todo_manager.add("Task 2")
        item3 = todo_manager.add("Task 3")

        assert item1.id == "todo_1"
        assert item2.id == "todo_2"
        assert item3.id == "todo_3"

    def test_add_with_priority(self, todo_manager):
        """Test adding todos with different priorities."""
        high = todo_manager.add("High priority", priority="high")
        low = todo_manager.add("Low priority", priority="low")

        assert high.priority == "high"
        assert low.priority == "low"

    def test_get_todo(self, todo_manager):
        """Test getting a todo by ID."""
        todo_manager.add("Task 1")
        item2 = todo_manager.add("Task 2")
        todo_manager.add("Task 3")

        found = todo_manager.get("todo_2")
        assert found is not None
        assert found.content == "Task 2"
        assert found == item2

    def test_get_nonexistent(self, todo_manager):
        """Test getting a todo that doesn't exist."""
        found = todo_manager.get("nonexistent")
        assert found is None


class TestTodoManagerCompletion:
    """Tests for todo completion operations."""

    def test_complete_todo(self, todo_manager):
        """Test completing a todo."""
        todo_manager.add("Task to complete")

        result = todo_manager.complete("todo_1")

        assert result is not None
        assert result.status == TodoStatus.COMPLETED

    def test_complete_with_notes(self, todo_manager):
        """Test completing with notes."""
        todo_manager.add("Task with notes")

        result = todo_manager.complete(
            "todo_1", notes=["Done successfully", "No issues"]
        )

        assert result.notes == ["Done successfully", "No issues"]

    def test_complete_nonexistent(self, todo_manager):
        """Test completing a todo that doesn't exist."""
        result = todo_manager.complete("nonexistent")
        assert result is None

    def test_start_todo(self, todo_manager):
        """Test marking a todo as in progress."""
        todo_manager.add("Task to start")

        result = todo_manager.start("todo_1")

        assert result is not None
        assert result.status == TodoStatus.IN_PROGRESS

    def test_start_nonexistent(self, todo_manager):
        """Test starting a todo that doesn't exist."""
        result = todo_manager.start("nonexistent")
        assert result is None


class TestTodoManagerListing:
    """Tests for listing todos."""

    def test_list_all(self, todo_manager):
        """Test listing all todos."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.add("Task 3")

        all_todos = todo_manager.list_all()
        assert len(all_todos) == 3

    def test_list_all_returns_copy(self, todo_manager):
        """Test that list_all returns a copy, not the internal list."""
        todo_manager.add("Task 1")

        list1 = todo_manager.list_all()
        list1.clear()  # Modify the returned list

        list2 = todo_manager.list_all()
        assert len(list2) == 1  # Internal list unchanged

    def test_list_pending(self, todo_manager):
        """Test listing only pending todos."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.add("Task 3")
        todo_manager.complete("todo_1")

        pending = todo_manager.list_pending()
        assert len(pending) == 2
        assert all(t.status != TodoStatus.COMPLETED for t in pending)

    def test_list_pending_includes_in_progress(self, todo_manager):
        """Test that list_pending includes in_progress items."""
        todo_manager.add("Pending task")
        todo_manager.add("In progress task")
        todo_manager.start("todo_2")

        pending = todo_manager.list_pending()
        assert len(pending) == 2

    def test_list_pending_sorted_by_priority(self, todo_manager):
        """Test that list_pending sorts by priority."""
        todo_manager.add("Low priority", priority="low")
        todo_manager.add("High priority", priority="high")
        todo_manager.add("Medium priority", priority="medium")

        pending = todo_manager.list_pending()
        priorities = [t.priority for t in pending]
        assert priorities == ["high", "medium", "low"]


class TestTodoManagerStatus:
    """Tests for todo status checks."""

    def test_all_complete_empty(self, todo_manager):
        """Test all_complete with no todos returns False (empty != complete)."""
        assert todo_manager.all_complete() is False

    def test_all_complete_none_done(self, todo_manager):
        """Test all_complete when no todos are done."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")

        assert todo_manager.all_complete() is False

    def test_all_complete_some_done(self, todo_manager):
        """Test all_complete when some todos are done."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.complete("todo_1")

        assert todo_manager.all_complete() is False

    def test_all_complete_all_done(self, todo_manager):
        """Test all_complete when all todos are done."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.complete("todo_1")
        todo_manager.complete("todo_2")

        assert todo_manager.all_complete() is True

    def test_get_progress(self, todo_manager):
        """Test getting progress statistics."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.add("Task 3")
        todo_manager.complete("todo_1")

        progress = todo_manager.get_progress()
        assert progress["total"] == 3
        assert progress["completed"] == 1
        assert progress["pending"] == 2
        assert progress["percentage"] == pytest.approx(33.3, rel=0.1)

    def test_get_progress_empty(self, todo_manager):
        """Test progress with no todos."""
        progress = todo_manager.get_progress()
        assert progress["total"] == 0
        assert progress["completed"] == 0
        assert progress["percentage"] == 0


class TestTodoManagerDisplay:
    """Tests for display formatting."""

    def test_format_empty(self, todo_manager):
        """Test formatting empty todo list."""
        result = todo_manager.format_for_display()
        assert result == "No active todos."

    def test_format_with_pending(self, todo_manager):
        """Test formatting with pending todos."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")

        result = todo_manager.format_for_display()
        assert "## Phase 1 (Strategic)" in result
        assert "**Pending:**" in result
        assert "Task 1" in result
        assert "Task 2" in result

    def test_format_includes_phase_info(self, todo_manager):
        """Test that format_for_display includes phase number, type, and name."""
        todo_manager.phase_number = 3
        todo_manager.is_strategic_phase = False
        todo_manager.set_phase_name("Data Extraction")
        todo_manager.add("Task 1")

        result = todo_manager.format_for_display()
        assert "## Phase 3 (Tactical): Data Extraction" in result

    def test_format_with_in_progress(self, todo_manager):
        """Test formatting with in progress todos."""
        todo_manager.add("Working on this")
        todo_manager.start("todo_1")

        result = todo_manager.format_for_display()
        assert "**In Progress:**" in result
        assert "Working on this" in result

    def test_format_with_completed(self, todo_manager):
        """Test formatting shows completed count."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.complete("todo_1")

        result = todo_manager.format_for_display()
        assert "**Completed:** 1/2" in result

    def test_injection_shows_only_latest_completion_handoff(self, todo_manager):
        """Completion outcomes survive compaction without accumulating per turn."""
        todo_manager.add("First task")
        todo_manager.add("Second task")
        todo_manager.add("Decision task")
        todo_manager.complete("todo_1", notes=["OLD: superseded result"])
        todo_manager.complete("todo_2", notes=["PASS: current verified result"])

        result = todo_manager.format_for_injection()

        assert "Outcome: PASS: current verified result" in result
        assert "OLD: superseded result" not in result

    def test_format_high_priority_marker(self, todo_manager):
        """Test that high priority items get marked."""
        todo_manager.add("Urgent task", priority="high")

        result = todo_manager.format_for_display()
        assert "[!] Urgent task" in result


class TestTodoManagerArchive:
    """Tests for archiving todos."""

    def test_archive_empty(self, todo_manager):
        """Test archiving with no todos returns empty string."""
        result = todo_manager.archive()
        assert result == ""

    def test_archive_creates_file(self, todo_manager, workspace_manager):
        """Test that archive creates a file in workspace."""
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive()

        assert archive_path.startswith("archive/")
        assert workspace_manager.exists(archive_path)

    def test_archive_with_phase_name(self, todo_manager, workspace_manager):
        """Test archiving with a phase name uses phase-aware naming."""
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive(phase_name="extraction")

        # New format: todos_phase_{N}_{type}_{ts}.md
        # phase_name is now used in content header, not filename
        assert "todos_phase_1_strategic_" in archive_path
        assert workspace_manager.exists(archive_path)

        # Verify phase_name appears in content
        content = workspace_manager.read_file(archive_path)
        assert "extraction" in content

    def test_archive_content(self, todo_manager, workspace_manager):
        """Test archive file content."""
        todo_manager.add("Completed task")
        todo_manager.add("Not completed task")
        todo_manager.complete("todo_1", notes=["Successfully completed"])

        archive_path = todo_manager.archive(phase_name="test_phase")
        content = workspace_manager.read_file(archive_path)

        assert "# Archived Todos: test_phase" in content
        assert "## Completed (1)" in content
        assert "[x] Completed task" in content
        assert "Successfully completed" in content
        assert "## Not Completed (1)" in content
        assert "Not completed task" in content
        assert "## Summary" in content

    def test_archive_clears_list(self, todo_manager):
        """Test that archive clears the todo list."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")

        todo_manager.archive()

        assert len(todo_manager.list_all()) == 0

    def test_archive_resets_id_counter(self, todo_manager):
        """Test that archive resets the ID counter."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.archive()

        # New todos should start from todo_1 again
        new_item = todo_manager.add("New task")
        assert new_item.id == "todo_1"


class TestTodoManagerClear:
    """Tests for clearing todos without archiving."""

    def test_clear(self, todo_manager):
        """Test clearing todos without archiving."""
        todo_manager.add("Task 1")
        todo_manager.add("Task 2")

        todo_manager.clear()

        assert len(todo_manager.list_all()) == 0

    def test_clear_resets_id(self, todo_manager):
        """Test that clear resets the ID counter."""
        todo_manager.add("Task 1")
        todo_manager.clear()

        new_item = todo_manager.add("New task")
        assert new_item.id == "todo_1"


class TestTodoManagerLogging:
    """Tests for logging functionality."""

    def test_log_state(self, todo_manager, caplog):
        """Test that log_state logs todo statistics."""
        import logging

        caplog.set_level(logging.INFO)

        todo_manager.add("Task 1")
        todo_manager.add("Task 2")
        todo_manager.start("todo_1")
        todo_manager.complete("todo_2")

        todo_manager.log_state()

        assert "total=2" in caplog.text
        assert "completed=1" in caplog.text
        assert "in_progress=1" in caplog.text
        assert "pending=0" in caplog.text


class TestTodoManagerStatePersistence:
    """Tests for export_state and restore_state methods."""

    def test_export_state_empty(self, todo_manager):
        """Test export_state with empty TodoManager."""
        state = todo_manager.export_state()

        assert state["todos"] == []
        assert state["staged_todos"] == []
        assert state["next_id"] == 1
        assert state["staged_phase_name"] == ""

    def test_export_state_with_todos(self, todo_manager):
        """Test export_state captures all todo state."""
        todo_manager.add("Task 1", priority="high")
        todo_manager.add("Task 2")
        todo_manager.complete("todo_1", notes=["Done"])

        state = todo_manager.export_state()

        assert len(state["todos"]) == 2
        assert state["todos"][0]["id"] == "todo_1"
        assert state["todos"][0]["status"] == "completed"
        assert state["todos"][0]["priority"] == "high"
        assert state["todos"][0]["notes"] == ["Done"]
        assert state["todos"][1]["id"] == "todo_2"
        assert state["todos"][1]["status"] == "pending"
        assert state["next_id"] == 3

    def test_export_state_with_staged_todos(self, todo_manager):
        """Test export_state captures staged todos."""
        # Stage some todos
        todos = [f"Task {i}: do something meaningful here" for i in range(1, 6)]
        todo_manager.stage_tactical_todos(todos, "Test Phase")

        state = todo_manager.export_state()

        assert len(state["staged_todos"]) == 5
        assert state["staged_phase_name"] == "Test Phase"

    def test_restore_state_basic(self, todo_manager):
        """Test restore_state restores todos correctly."""
        state = {
            "todos": [
                {
                    "id": "todo_1",
                    "content": "Task 1",
                    "status": "pending",
                    "priority": "high",
                    "notes": [],
                },
                {
                    "id": "todo_2",
                    "content": "Task 2",
                    "status": "completed",
                    "priority": "medium",
                    "notes": ["Done"],
                },
            ],
            "staged_todos": [],
            "next_id": 3,
            "staged_phase_name": "",
        }

        todo_manager.restore_state(state)

        todos = todo_manager.list_all()
        assert len(todos) == 2
        assert todos[0].id == "todo_1"
        assert todos[0].content == "Task 1"
        assert todos[0].priority == "high"
        assert todos[1].id == "todo_2"
        assert todos[1].status.value == "completed"
        assert todos[1].notes == ["Done"]

    def test_restore_state_with_staged_todos(self, todo_manager):
        """Test restore_state restores staged todos."""
        state = {
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
            "next_id": 5,
            "staged_phase_name": "Restored Phase",
        }

        todo_manager.restore_state(state)

        assert todo_manager.has_staged_todos()
        assert todo_manager.get_staged_phase_name() == "Restored Phase"
        assert todo_manager._next_id == 5

    def test_restore_state_with_todo_next_id_key(self, todo_manager):
        """Test restore_state handles todo_next_id key from checkpoint state."""
        # Checkpoint state uses todo_next_id instead of next_id
        state = {
            "todos": [],
            "staged_todos": [],
            "todo_next_id": 10,  # Key used in UniversalAgentState
        }

        todo_manager.restore_state(state)

        assert todo_manager._next_id == 10

    def test_restore_state_handles_none_values(self, todo_manager):
        """Test restore_state handles None/missing values gracefully."""
        state = {
            "todos": None,
            "staged_todos": None,
            "next_id": None,
        }

        todo_manager.restore_state(state)

        assert todo_manager.list_all() == []
        assert not todo_manager.has_staged_todos()
        assert todo_manager._next_id == 1

    def test_export_restore_roundtrip(self, todo_manager):
        """Test that export/restore is a perfect roundtrip."""
        # Set up state
        todo_manager.add("Task 1", priority="high")
        todo_manager.add("Task 2")
        todo_manager.complete("todo_1", notes=["Completed"])
        todo_manager.start("todo_2")

        # Stage some todos
        todos = [f"Staged task {i}: meaningful description" for i in range(1, 6)]
        todo_manager.stage_tactical_todos(todos, "Next Phase")

        # Export
        state = todo_manager.export_state()

        # Create a new manager and restore
        from agent.managers.todo import TodoManager

        new_manager = TodoManager(todo_manager._workspace)
        new_manager.restore_state(state)

        # Verify
        new_state = new_manager.export_state()
        assert state["todos"] == new_state["todos"]
        assert state["staged_todos"] == new_state["staged_todos"]
        assert state["next_id"] == new_state["next_id"]
        assert state["staged_phase_name"] == new_state["staged_phase_name"]


class TestTodoManagerPhaseTracking:
    """Tests for phase number tracking functionality."""

    def test_initial_phase_number(self, todo_manager):
        """Test that phase number starts at 1."""
        assert todo_manager.phase_number == 1

    def test_get_phase_info_strategic(self, todo_manager):
        """Test get_phase_info in strategic mode."""
        todo_manager.is_strategic_phase = True
        todo_manager.set_phase_name("Planning Phase")

        info = todo_manager.get_phase_info()

        assert info["phase_number"] == 1
        assert info["phase_type"] == "strategic"
        assert info["phase_name"] == "Planning Phase"

    def test_get_phase_info_tactical(self, todo_manager):
        """Test get_phase_info in tactical mode."""
        todo_manager.is_strategic_phase = False
        todo_manager.set_phase_name("Execution Phase")

        info = todo_manager.get_phase_info()

        assert info["phase_number"] == 1
        assert info["phase_type"] == "tactical"
        assert info["phase_name"] == "Execution Phase"

    def test_increment_phase_number(self, todo_manager):
        """Test incrementing phase number."""
        assert todo_manager.phase_number == 1

        new_num = todo_manager.increment_phase_number()

        assert new_num == 2
        assert todo_manager.phase_number == 2

    def test_set_phase_name(self, todo_manager):
        """Test setting phase name."""
        todo_manager.set_phase_name("Test Phase")
        assert todo_manager.current_phase_name == "Test Phase"

    def test_current_phase_name_falls_back_to_staged(self, todo_manager):
        """Test that current_phase_name falls back to staged_phase_name."""
        # Stage some todos with a phase name
        todos = [f"Task {i}: meaningful description here" for i in range(1, 6)]
        todo_manager.stage_tactical_todos(todos, "Staged Phase Name")

        # current_phase_name should fall back to staged_phase_name
        assert todo_manager.current_phase_name == "Staged Phase Name"


class TestTodoManagerPhaseAwareArchive:
    """Tests for phase-aware archive naming."""

    def test_archive_filename_format(self, todo_manager, workspace_manager):
        """Test that archive uses phase-aware filename format."""
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive()

        # Should be: todos_phase_{N}_{type}_{ts}.md
        assert "todos_phase_1_strategic_" in archive_path
        assert archive_path.endswith(".md")

    def test_archive_filename_tactical(self, todo_manager, workspace_manager):
        """Test archive filename in tactical phase."""
        todo_manager.is_strategic_phase = False
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive()

        assert "todos_phase_1_tactical_" in archive_path

    def test_archive_filename_with_phase_number(self, todo_manager, workspace_manager):
        """Test archive filename includes correct phase number."""
        todo_manager._phase_number = 3
        todo_manager.is_strategic_phase = False
        todo_manager.add("Task 1")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive()

        assert "todos_phase_3_tactical_" in archive_path

    def test_archive_content_includes_phase_info(self, todo_manager, workspace_manager):
        """Test that archive content includes phase information."""
        todo_manager._phase_number = 2
        todo_manager.is_strategic_phase = False
        todo_manager.set_phase_name("Extraction Phase")
        todo_manager.add("Extract data")
        todo_manager.complete("todo_1")

        archive_path = todo_manager.archive()
        content = workspace_manager.read_file(archive_path)

        assert "Phase: 2 (tactical)" in content
        assert "# Archived Todos: Extraction Phase" in content


class TestTodoManagerBuildCommitMessage:
    """Tests for commit message building."""

    def test_build_commit_message_strategic(self, todo_manager):
        """Test commit message format for strategic phase."""
        todo_manager.is_strategic_phase = True
        todo_manager._phase_number = 1

        todo = TodoItem(id="todo_1", content="Review and plan next steps")
        message = todo_manager._build_commit_message(todo)

        assert "[Phase 1 Strategic]" in message
        assert "todo_1:" in message
        assert "Review and plan next steps" in message
        assert "Completed:" in message

    def test_build_commit_message_tactical(self, todo_manager):
        """Test commit message format for tactical phase."""
        todo_manager.is_strategic_phase = False
        todo_manager._phase_number = 2

        todo = TodoItem(id="todo_3", content="Extract requirements from document")
        message = todo_manager._build_commit_message(todo)

        assert "[Phase 2 Tactical]" in message
        assert "todo_3:" in message
        assert "Extract requirements from document" in message

    def test_build_commit_message_without_todo_steps(self, todo_manager):
        """A job without todo_complete works through one task: no phase label."""
        todo_manager.is_strategic_phase = True
        todo_manager._phase_number = 0
        todo_manager.todo_tool_bound = False

        todo = TodoItem(id="todo_1", content="Do the task in the brief.")
        message = todo_manager._build_commit_message(todo)

        assert message.splitlines()[0] == "[Task] Do the task in the brief."
        assert "Phase" not in message
        assert "Strategic" not in message
        assert "Completed:" in message

    def test_build_commit_message_with_notes(self, todo_manager):
        """Test commit message includes notes."""
        todo_manager.is_strategic_phase = True
        todo_manager._phase_number = 1

        todo = TodoItem(id="todo_1", content="Task description")
        todo.notes = ["Found 5 items", "All validated"]
        message = todo_manager._build_commit_message(todo)

        assert "Notes:" in message
        assert "Found 5 items; All validated" in message


class TestTodoManagerPhaseStatePersistence:
    """Tests for phase state in export/restore."""

    def test_export_state_includes_phase_info(self, todo_manager):
        """Test that export_state includes phase tracking fields."""
        todo_manager._phase_number = 3
        todo_manager.is_strategic_phase = False
        todo_manager.set_phase_name("Test Phase")

        state = todo_manager.export_state()

        assert state["phase_number"] == 3
        assert state["is_strategic_phase"] is False
        assert state["current_phase_name"] == "Test Phase"

    def test_restore_state_restores_phase_info(self, todo_manager):
        """Test that restore_state restores phase tracking fields."""
        state = {
            "todos": [],
            "staged_todos": [],
            "next_id": 1,
            "staged_phase_name": "",
            "phase_number": 5,
            "is_strategic_phase": False,
            "current_phase_name": "Restored Phase",
        }

        todo_manager.restore_state(state)

        assert todo_manager.phase_number == 5
        assert todo_manager.is_strategic_phase is False
        assert todo_manager.current_phase_name == "Restored Phase"

    def test_restore_state_defaults_phase_number(self, todo_manager):
        """Test that restore_state defaults phase_number to 1."""
        state = {
            "todos": [],
            "staged_todos": [],
            "next_id": 1,
        }

        todo_manager.restore_state(state)

        assert todo_manager.phase_number == 1

    def test_export_restore_roundtrip_with_phase(self, todo_manager):
        """Test export/restore roundtrip preserves phase info."""
        todo_manager._phase_number = 4
        todo_manager.is_strategic_phase = False
        todo_manager.set_phase_name("Phase 4 Tactical")
        todo_manager.add("Task 1")

        state = todo_manager.export_state()

        # Create new manager and restore
        from agent.managers.todo import TodoManager

        new_manager = TodoManager(todo_manager._workspace)
        new_manager.restore_state(state)

        assert new_manager.phase_number == 4
        assert new_manager.is_strategic_phase is False
        assert new_manager.current_phase_name == "Phase 4 Tactical"


class TestTodoManagerAutoCommit:
    """Tests for auto-commit on todo completion."""

    def test_commit_todo_completion_without_git(self, todo_manager):
        """Test that _commit_todo_completion works when git is not available."""
        # By default, workspace doesn't have git_manager set up in these tests
        # (WorkspaceManagerConfig defaults to git_versioning=True but the
        # direct module import tests don't trigger full initialization)
        todo = TodoItem(id="todo_1", content="Test task")

        # Should return True (success) when git not active
        result = todo_manager._commit_todo_completion(todo)
        assert result is True

    def test_complete_calls_commit(self, todo_manager, workspace_manager):
        """Test that complete() calls _commit_todo_completion."""
        from unittest.mock import patch

        todo_manager.add("Task 1")

        # Mock the _commit_todo_completion method
        with patch.object(todo_manager, "_commit_todo_completion") as mock_commit:
            mock_commit.return_value = True
            todo_manager.complete("todo_1")

            # Verify commit was called
            mock_commit.assert_called_once()
            # Verify it was called with the todo
            call_args = mock_commit.call_args[0]
            assert call_args[0].id == "todo_1"

    def test_complete_succeeds_even_if_commit_fails(self, todo_manager):
        """Test that complete() succeeds even if commit fails."""
        from unittest.mock import patch

        todo_manager.add("Task 1")

        # Mock commit to fail
        with patch.object(todo_manager, "_commit_todo_completion", return_value=False):
            result = todo_manager.complete("todo_1")

            # Todo should still be marked complete
            assert result is not None
            assert result.status.value == "completed"

    def test_commit_message_used_in_commit(self, todo_manager, workspace_manager):
        """Test that the correct commit message is used."""
        from unittest.mock import MagicMock

        # Create a mock git manager
        mock_git = MagicMock()
        mock_git.is_active = True
        mock_git.commit.return_value = True

        # Set the mock on the workspace's private attribute
        workspace_manager._git_manager = mock_git

        todo_manager.add("Extract requirements")
        todo_manager._phase_number = 2
        todo_manager.is_strategic_phase = False

        todo_manager.complete("todo_1")

        # Verify commit was called with correct message format
        mock_git.commit.assert_called_once()
        call_args = mock_git.commit.call_args
        message = call_args[0][0]

        assert "[Phase 2 Tactical]" in message
        assert "todo_1:" in message
        assert "Extract requirements" in message

        # Clean up
        workspace_manager._git_manager = None

    def test_commit_with_notes(self, todo_manager, workspace_manager):
        """Test that notes are included in commit message."""
        from unittest.mock import MagicMock

        mock_git = MagicMock()
        mock_git.is_active = True
        mock_git.commit.return_value = True

        workspace_manager._git_manager = mock_git

        todo_manager.add("Process data")
        todo_manager.complete("todo_1", notes=["Found 10 items"])

        message = mock_git.commit.call_args[0][0]
        assert "Notes:" in message
        assert "Found 10 items" in message

        workspace_manager._git_manager = None

    def test_commit_allow_empty(self, todo_manager, workspace_manager):
        """Test that commits allow empty (for read-only todos)."""
        from unittest.mock import MagicMock

        mock_git = MagicMock()
        mock_git.is_active = True
        mock_git.commit.return_value = True

        workspace_manager._git_manager = mock_git

        todo_manager.add("Review document")
        todo_manager.complete("todo_1")

        # Verify allow_empty=True was passed
        call_kwargs = mock_git.commit.call_args[1]
        assert call_kwargs.get("allow_empty") is True

        workspace_manager._git_manager = None


class TestTodoManagerFloor:
    """The min_todos floor is the live gate and is config-wired.

    Production sites (src/agent.py) pass config.phase_settings.min_todos into
    the constructor; worker_base.yaml sets 2. When nothing is passed, the
    constructor default (5) applies.
    """

    def test_configured_floor_accepts_two_todos(self, workspace_manager):
        mgr = TodoManager(workspace_manager, min_todos=2)
        mgr.stage_tactical_todos(
            [
                "Write output/report.md section 1 from findings",
                "Verify output/report.md covers both requirements",
            ],
            "Small Phase",
        )
        assert mgr.has_staged_todos()
        assert len(mgr._staged_todos) == 2

    def test_configured_floor_rejects_one_todo(self, workspace_manager):
        mgr = TodoManager(workspace_manager, min_todos=2)
        with pytest.raises(ValueError, match="Too few todos: 1 < 2"):
            mgr.stage_tactical_todos(
                ["Write output/report.md section 1 from findings"],
                "Tiny Phase",
            )
        assert not mgr.has_staged_todos()

    def test_default_floor_when_unset(self, workspace_manager):
        """Without config wiring the constructor default (5) still applies."""
        mgr = TodoManager(workspace_manager)
        with pytest.raises(ValueError, match="Too few todos: 4 < 5"):
            mgr.stage_tactical_todos(
                [f"Meaningful task number {i} for the phase" for i in range(4)],
                "Default Phase",
            )
        mgr.stage_tactical_todos(
            [f"Meaningful task number {i} for the phase" for i in range(5)],
            "Default Phase",
        )
        assert mgr.has_staged_todos()

    def test_configured_ceiling_still_enforced(self, workspace_manager):
        mgr = TodoManager(workspace_manager, min_todos=2, max_todos=3)
        with pytest.raises(ValueError, match="Too many todos: 4 > 3"):
            mgr.stage_tactical_todos(
                [f"Meaningful task number {i} for the phase" for i in range(4)],
                "Over Phase",
            )
