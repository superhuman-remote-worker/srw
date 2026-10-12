"""Todo manager for nested loop graph architecture.

This module provides the TodoManager for tracking task execution
within the graph's inner loop. The manager is stateful - it holds
todos in memory until explicitly archived.

The TodoManager is used by graph nodes to:
- Create todos from plan phases
- Track completion during execution
- Archive completed todos at phase transitions
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional


if TYPE_CHECKING:
    from agent.core.workspace import WorkspaceManager

logger = logging.getLogger(__name__)

#: Lead line of the message that restates the todo list when the conversation
#: no longer contains its current rendering (after compaction, on resume).
TODO_LIST_RESTATEMENT_LEAD = (
    "[TODO_LIST] Your current todo list, restated because the conversation "
    "no longer shows it:"
)


#: Longest headline shown for a todo that is no longer the current work.
TODO_HEADLINE_MAX_CHARS = 240


def todo_headline(content: str) -> str:
    """First line of a todo, whitespace-collapsed and capped.

    Completed todos, and the "Completed:"/"Next:" lines of a todo tool
    result, show only this: the full body of a finished todo is already in
    the history (the phase-start list) and the phase archive, and repeating
    long strategic bodies in every todo result would grow the history by
    several thousand tokens per completion.
    """
    first = next((line for line in content.splitlines() if line.strip()), "")
    line = " ".join(first.split())
    if len(line) > TODO_HEADLINE_MAX_CHARS:
        line = f"{line[: TODO_HEADLINE_MAX_CHARS - 1].rstrip()}…"
    return line


def _message_text(message: Any) -> str:
    """Plain text of a message's content (str, or the text parts of a list)."""
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return str(content or "")


class TodoStatus(Enum):
    """Status values for todo items."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass
class TodoItem:
    """A single todo item.

    Attributes:
        id: Unique identifier (e.g., "todo_1")
        content: Task description
        status: Current status (pending, in_progress, completed)
        priority: Priority level ("high", "medium", "low")
        notes: Completion notes or comments
        created_at: When the todo was created
    """

    id: str
    content: str
    status: TodoStatus = TodoStatus.PENDING
    priority: str = "medium"
    notes: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize todo item to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "status": self.status.value,
            "priority": self.priority,
            "notes": self.notes,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TodoItem":
        """Deserialize todo item from dictionary."""
        return cls(
            id=data["id"],
            content=data["content"],
            status=TodoStatus(data.get("status", "pending")),
            priority=data.get("priority", "medium"),
            notes=data.get("notes", []),
            created_at=(
                datetime.fromisoformat(data["created_at"])
                if data.get("created_at")
                else datetime.now(timezone.utc)
            ),
        )


class TodoManager:
    """Stateful manager for todo list operations.

    The TodoManager holds todos in memory and provides operations for
    the graph's inner execution loop. Todos are created when entering
    a plan phase and archived when the phase completes.

    Key design principles:
    - Stateful: Todos live in memory until archived
    - Graph-focused: API matches what graph nodes need
    - Workspace-backed: Archives write to workspace filesystem

    Example:
        ```python
        # Create manager with workspace
        todo_mgr = TodoManager(workspace)

        # Add todos for current phase
        todo_mgr.add("Extract document chunks")
        todo_mgr.add("Identify requirements", priority="high")

        # Track progress
        pending = todo_mgr.list_pending()
        todo_mgr.complete("todo_1", notes=["Processed 15 chunks"])

        # Archive at phase end
        todo_mgr.archive("extraction")
        ```
    """

    def __init__(
        self,
        workspace: "WorkspaceManager",
        min_todos: int = 5,
        max_todos: int = 20,
        model_name: Optional[str] = None,
    ):
        """Initialize todo manager.

        Args:
            workspace: WorkspaceManager for archive operations
            min_todos: Minimum todos required for tactical phases (default: 5).
                This is the LIVE floor enforced by stage_tactical_todos;
                production sites pass config.phase_settings.min_todos
                (the worker overlay, config/overlays/worker.yaml, sets 2).
            max_todos: Maximum todos allowed for tactical phases (default: 20)
            model_name: Optional model id used to resolve family-specific
                runtime nudges (e.g. the todo-list footer rendered into
                injected context). Falls through to the default family
                when not supplied.
        """
        self._workspace = workspace
        self._todos: List[TodoItem] = []
        self._next_id = 1
        self._min_todos = min_todos
        self._max_todos = max_todos
        self._model_name = model_name
        # Staging for next phase todos
        self._staged_todos: List[TodoItem] = []
        self._staged_phase_name: str = ""
        # Last archive counts (populated by archive(), read by phase transitions)
        self._last_archived_total: int = 0
        self._last_archived_completed: int = 0
        # Phase state tracking (for job_complete validation)
        self._is_strategic_phase: bool = True
        # Phase number tracking (for git versioning and archive naming)
        # Sequential numbering: phase 1, 2, 3, 4... (increments every transition)
        self._phase_number: int = 1
        self._current_phase_name: str = ""  # Human-readable name for current phase

    @property
    def is_strategic_phase(self) -> bool:
        """Check if currently in strategic phase."""
        return self._is_strategic_phase

    @is_strategic_phase.setter
    def is_strategic_phase(self, value: bool) -> None:
        """Set the current phase state."""
        self._is_strategic_phase = value
        logger.debug(f"Phase state updated: is_strategic={value}")

    @property
    def phase_number(self) -> int:
        """Get the current phase number."""
        return self._phase_number

    @phase_number.setter
    def phase_number(self, value: int) -> None:
        """Set the current phase number (synced from state on transitions)."""
        self._phase_number = value
        logger.debug(f"Phase number updated to {value}")

    @property
    def current_phase_name(self) -> str:
        """Get the current phase name."""
        return self._current_phase_name or self._staged_phase_name

    def get_phase_info(self) -> Dict[str, Any]:
        """Get current phase information for commits and state files.

        Returns:
            Dictionary containing:
            - phase_number: Current phase number (sequential)
            - phase_type: "strategic" or "tactical"
            - phase_name: Human-readable phase name
        """
        return {
            "phase_number": self._phase_number,
            "phase_type": "strategic" if self._is_strategic_phase else "tactical",
            "phase_name": self._current_phase_name or self._staged_phase_name,
        }

    def increment_phase_number(self) -> int:
        """Increment phase number (called on phase transitions).

        Sequential numbering: increments at every phase transition.

        Returns:
            The new phase number
        """
        self._phase_number += 1
        logger.info(f"Phase number incremented to {self._phase_number}")
        return self._phase_number

    def set_phase_name(self, name: str) -> None:
        """Set the current phase name.

        Args:
            name: Human-readable phase name
        """
        self._current_phase_name = name
        logger.debug(f"Phase name set to: {name}")

    def add(self, content: str, priority: str = "medium") -> TodoItem:
        """Add a new todo item.

        Args:
            content: Task description
            priority: Priority level ("high", "medium", "low")

        Returns:
            Created TodoItem
        """
        item = TodoItem(
            id=f"todo_{self._next_id}",
            content=content,
            priority=priority,
        )
        self._todos.append(item)
        self._next_id += 1

        logger.info(f"Added todo: {item.id} - {content} [{priority}]")
        return item

    def complete(
        self, todo_id: str, notes: Optional[List[str]] = None
    ) -> Optional[TodoItem]:
        """Mark a todo as completed and commit changes.

        Marks the todo as completed, then auto-commits workspace changes
        if git versioning is active. Commit failures are logged but don't
        prevent the todo from being marked complete.

        Args:
            todo_id: Todo ID to complete
            notes: Optional completion notes

        Returns:
            Updated TodoItem or None if not found
        """
        for todo in self._todos:
            if todo.id == todo_id:
                todo.status = TodoStatus.COMPLETED
                if notes:
                    todo.notes.extend(notes)
                logger.info(f"Completed todo: {todo_id}")

                # Auto-commit if git versioning is active
                self._commit_todo_completion(todo)

                return todo

        logger.warning(f"Todo not found: {todo_id}")
        return None

    def complete_multiple(
        self, todo_ids: List[str], notes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Mark multiple todos as completed.

        Args:
            todo_ids: List of todo IDs to complete
            notes: Optional notes to add to all completed todos

        Returns:
            Dictionary with:
                - completed: List of completed TodoItems
                - not_found: List of IDs that weren't found
                - is_last: Whether all todos are now complete
        """
        completed = []
        not_found = []

        for todo_id in todo_ids:
            todo = self.complete(todo_id.strip(), notes)
            if todo:
                completed.append(todo)
            else:
                not_found.append(todo_id)

        return {
            "completed": completed,
            "not_found": not_found,
            "is_last": self.all_complete(),
        }

    def start(self, todo_id: str) -> Optional[TodoItem]:
        """Mark a todo as in progress.

        Args:
            todo_id: Todo ID to start

        Returns:
            Updated TodoItem or None if not found
        """
        for todo in self._todos:
            if todo.id == todo_id:
                todo.status = TodoStatus.IN_PROGRESS
                logger.info(f"Started todo: {todo_id}")
                return todo

        logger.warning(f"Todo not found: {todo_id}")
        return None

    def get(self, todo_id: str) -> Optional[TodoItem]:
        """Get a todo by ID.

        Args:
            todo_id: Todo ID

        Returns:
            TodoItem or None if not found
        """
        for todo in self._todos:
            if todo.id == todo_id:
                return todo
        return None

    def list_all(self) -> List[TodoItem]:
        """List all todo items.

        Returns:
            List of all TodoItems
        """
        return self._todos.copy()

    def list_pending(self) -> List[TodoItem]:
        """List pending todo items (not completed).

        Includes both PENDING and IN_PROGRESS items, sorted by:
        1. Priority (high > medium > low)
        2. Creation time (earlier first)

        Returns:
            List of pending TodoItems
        """
        priority_order = {"high": 0, "medium": 1, "low": 2}
        pending = [t for t in self._todos if t.status != TodoStatus.COMPLETED]
        return sorted(
            pending, key=lambda t: (priority_order.get(t.priority, 1), t.created_at)
        )

    def all_complete(self) -> bool:
        """Check if all todos are complete.

        Returns:
            True if all todos are completed, False if empty or incomplete
        """
        if not self._todos:
            return False
        return all(t.status == TodoStatus.COMPLETED for t in self._todos)

    def format_for_injection(self) -> str:
        """Render the full current todo list — the one rendering the model sees.

        Returns all todos grouped by status (completed, in progress, pending)
        with a brief tool usage guide. Completed items are shown as crossed
        off so the agent retains awareness of finished work.

        The list enters the conversation history once per change and is never
        re-rendered per request (append-only context injection, D17-D19):
        the ``todo_complete`` and ``next_phase_todos`` results, the phase-start
        messages and the restatement after compaction or on resume all embed
        this exact text, and ``list_restatement`` checks for it by substring.
        So it must be a pure function of the durable todo state: no counters
        that move without a list change, no timestamps, no turn numbers, and
        nothing that is lost on a checkpoint resume (the staged phase name is
        not checkpointed, so it is not part of the header).

        Returns:
            Formatted todo list, or "No active todos." for an empty list
        """
        if not self._todos:
            return "No active todos."

        phase_type = "Strategic" if self._is_strategic_phase else "Tactical"
        phase_name = self._current_phase_name
        if phase_name:
            lines = [
                f"Current Tasks — Phase {self._phase_number} ({phase_type}): {phase_name}"
            ]
        else:
            lines = [f"Current Tasks — Phase {self._phase_number} ({phase_type})"]

        # Completed
        completed = [t for t in self._todos if t.status == TodoStatus.COMPLETED]
        if completed:
            # A concise completion note is the handoff from the last completed
            # task to the current one (for example final-review PASS/GAPS). Show
            # only the latest noted todo so phase-long note history does not
            # become another per-turn injection tax. Full notes remain in the
            # checkpoint, Git commit, and phase archive.
            latest_noted_todo = next(
                (todo for todo in reversed(completed) if todo.notes), None
            )
            lines.append("")
            lines.append("Completed:")
            for todo in completed:
                lines.append(f"  - [x] {todo.id}: {todo_headline(todo.content)}")
                if todo is latest_noted_todo:
                    note = " | ".join(" ".join(item.split()) for item in todo.notes)
                    if len(note) > 1000:
                        note = f"{note[:997]}..."
                    lines.append(f"      Outcome: {note}")

        # In progress
        in_progress = [t for t in self._todos if t.status == TodoStatus.IN_PROGRESS]
        if in_progress:
            lines.append("")
            lines.append("In Progress:")
            for todo in in_progress:
                lines.append(f"  - [>] {todo.id}: {todo.content}")

        # Pending
        pending = [t for t in self._todos if t.status == TodoStatus.PENDING]
        if pending:
            lines.append("")
            lines.append("Pending:")
            priority_order = {"high": 0, "medium": 1, "low": 2}
            pending_sorted = sorted(
                pending, key=lambda t: priority_order.get(t.priority, 1)
            )
            for todo in pending_sorted:
                lines.append(f"  - [ ] {todo.id}: {todo.content}")

        # Tool usage guide — resolved per-family via the guardrails matrix
        from shared.runtime.services.guardrails import format_nudge

        lines.append("")
        lines.append(format_nudge("todo_list_footer", model=self._model_name))

        return "\n".join(lines)

    def list_restatement(self, messages: List[Any]) -> Optional[str]:
        """Text that restates the current list, or None when none is needed.

        None when the list is empty or its current rendering
        (``format_for_injection``) already appears in ``messages``: a todo
        tool result, a phase-start message or an earlier restatement. The
        caller appends the text once as a HumanMessage (world-state rule:
        append when absent, never re-render per request).
        """
        if not self._todos:
            return None
        rendering = self.format_for_injection()
        for message in reversed(messages):
            if rendering in _message_text(message):
                return None
        return f"{TODO_LIST_RESTATEMENT_LEAD}\n\n{rendering}"

    def format_for_display(self) -> str:
        """Format todos for Layer 2 injection.

        Creates a compact, readable format suitable for injecting
        into the agent's context as a status reminder.

        Returns:
            Formatted string for display
        """
        if not self._todos:
            return "No active todos."

        phase_type = "Strategic" if self._is_strategic_phase else "Tactical"
        phase_name = self._current_phase_name or self._staged_phase_name
        if phase_name:
            lines = [f"## Phase {self._phase_number} ({phase_type}): {phase_name}"]
        else:
            lines = [f"## Phase {self._phase_number} ({phase_type})"]

        # In progress
        in_progress = [t for t in self._todos if t.status == TodoStatus.IN_PROGRESS]
        if in_progress:
            lines.append("")
            lines.append("**In Progress:**")
            for todo in in_progress:
                lines.append(f"  - [{todo.id}] {todo.content}")

        # Pending
        pending = [t for t in self._todos if t.status == TodoStatus.PENDING]
        if pending:
            lines.append("")
            lines.append("**Pending:**")
            # Sort by priority
            priority_order = {"high": 0, "medium": 1, "low": 2}
            pending_sorted = sorted(
                pending, key=lambda t: priority_order.get(t.priority, 1)
            )
            for todo in pending_sorted:
                marker = "[!] " if todo.priority == "high" else ""
                lines.append(f"  - [{todo.id}] {marker}{todo.content}")

        # Completed count
        completed = [t for t in self._todos if t.status == TodoStatus.COMPLETED]
        if completed:
            lines.append("")
            lines.append(f"**Completed:** {len(completed)}/{len(self._todos)}")

        return "\n".join(lines)

    def _build_commit_message(self, todo: TodoItem) -> str:
        """Build structured commit message for completed todo.

        Format:
            [Phase N Strategic/Tactical] todo_X: Task description

            Completed: 2026-02-01T14:23:00Z
            Notes: Note 1; Note 2

        Args:
            todo: The completed TodoItem

        Returns:
            Formatted commit message
        """
        phase_type = "Strategic" if self._is_strategic_phase else "Tactical"
        header = f"[Phase {self._phase_number} {phase_type}] {todo.id}: {todo.content}"

        body_lines = [
            f"Completed: {datetime.now(timezone.utc).isoformat()}",
        ]

        if todo.notes:
            body_lines.append(f"Notes: {'; '.join(todo.notes)}")

        return header + "\n\n" + "\n".join(body_lines)

    def _commit_todo_completion(self, todo: TodoItem) -> bool:
        """Commit workspace changes for completed todo.

        Auto-commits when git versioning is active. Uses the commit message
        format defined by _build_commit_message().

        Args:
            todo: The completed TodoItem

        Returns:
            True if commit succeeded or git not active, False if commit failed

        Note:
            Empty commits are allowed (allow_empty=True) because a todo might
            involve read-only analysis. This maintains the audit trail.
            Commit failures are logged but don't fail the todo completion.
        """
        git_mgr = self._workspace.git_manager
        if git_mgr is None or not git_mgr.is_active:
            # Git not available - this is fine, just skip
            return True

        message = self._build_commit_message(todo)
        success = git_mgr.commit(message, allow_empty=True)

        if not success:
            logger.warning(f"Git commit failed for {todo.id}")
            return False

        logger.debug(f"Committed changes for {todo.id}")
        return True

    def archive(self, phase_name: str = "") -> str:
        """Archive todos to workspace and clear the list.

        Writes completed todos as markdown to archive/ directory,
        then clears the internal list for the next phase.

        Uses phase-aware naming: todos_phase_{N}_{type}_{ts}.md
        Example: todos_phase_2_tactical_20260201_140000.md

        Args:
            phase_name: Optional name for the archived phase (used in header)

        Returns:
            Path to archive file
        """
        if not self._todos:
            logger.info("No todos to archive")
            return ""

        # Get phase info for naming
        phase_info = self.get_phase_info()
        phase_type = phase_info["phase_type"]
        phase_num = phase_info["phase_number"]

        # Generate archive content
        lines = []
        timestamp = datetime.now(timezone.utc)

        # Header with phase info
        header_name = (
            phase_name
            or phase_info["phase_name"]
            or f"Phase {phase_num} {phase_type.title()}"
        )
        lines.append(f"# Archived Todos: {header_name}")
        lines.append(f"Phase: {phase_num} ({phase_type})")
        lines.append(f"Archived: {timestamp.isoformat()}")
        lines.append("")

        # Completed
        completed = [t for t in self._todos if t.status == TodoStatus.COMPLETED]
        if completed:
            lines.append(f"## Completed ({len(completed)})")
            for todo in completed:
                lines.append(f"- [x] {todo.content}")
                for note in todo.notes:
                    lines.append(f"  - {note}")
            lines.append("")

        # Not completed
        not_completed = [t for t in self._todos if t.status != TodoStatus.COMPLETED]
        if not_completed:
            lines.append(f"## Not Completed ({len(not_completed)})")
            for todo in not_completed:
                status_mark = "~" if todo.status == TodoStatus.IN_PROGRESS else " "
                lines.append(f"- [{status_mark}] {todo.content}")
            lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append(f"- Total: {len(self._todos)}")
        lines.append(f"- Completed: {len(completed)}")
        lines.append(f"- Not completed: {len(not_completed)}")

        content = "\n".join(lines)

        # Generate filename with phase-aware naming
        # Format: todos_phase_{N}_{type}_{ts}.md
        ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"todos_phase_{phase_num}_{phase_type}_{ts_str}.md"

        archive_path = f"archive/{filename}"

        # Write to workspace
        self._workspace.write_file(archive_path, content)
        logger.info(f"Archived {len(self._todos)} todos to {archive_path}")

        # Store count before clearing (used by phase transition git commits)
        self._last_archived_total = len(self._todos)
        self._last_archived_completed = len(completed)

        # Clear the list
        self._todos = []
        self._next_id = 1

        return archive_path

    def clear(self) -> None:
        """Clear all todos without archiving."""
        count = len(self._todos)
        self._todos = []
        self._next_id = 1
        logger.info(f"Cleared {count} todos without archiving")

    # =========================================================================
    # Staging methods for next phase todos
    # =========================================================================

    def stage_tactical_todos(
        self,
        todos: List[str],
        phase_name: str = "",
    ) -> str:
        """Stage todos for the next tactical phase.

        This method validates the todos and stores them in a staging area.
        The staged todos will be applied when transitioning to tactical phase.

        Args:
            todos: List of task descriptions (min 10 chars each)
            phase_name: Optional name for the phase

        Returns:
            Success message or error message if validation fails

        Raises:
            ValueError: If validation fails
        """
        # Validate count
        if len(todos) < self._min_todos:
            raise ValueError(
                f"Too few todos: {len(todos)} < {self._min_todos}. "
                f"Create more detailed, actionable tasks."
            )
        if len(todos) > self._max_todos:
            raise ValueError(
                f"Too many todos: {len(todos)} > {self._max_todos}. "
                f"Split into multiple phases."
            )

        # Validate each todo content
        for i, content in enumerate(todos):
            if not isinstance(content, str):
                raise ValueError(f"Todo #{i + 1}: content must be a string")
            if len(content.strip()) < 10:
                raise ValueError(
                    f"Todo #{i + 1}: content too short ({len(content.strip())} chars). "
                    f"Provide a meaningful task description."
                )

        # Create staged todo items
        self._staged_todos = []
        for i, content in enumerate(todos):
            item = TodoItem(
                id=f"todo_{i + 1}",
                content=content.strip(),
                priority="medium",
                status=TodoStatus.PENDING,
            )
            self._staged_todos.append(item)

        self._staged_phase_name = phase_name

        logger.info(f"Staged {len(self._staged_todos)} todos for next phase")
        return (
            f"Staged {len(self._staged_todos)} todos for the next tactical phase"
            + (f" ({phase_name})" if phase_name else "")
            + "."
        )

    def has_staged_todos(self) -> bool:
        """Check if there are staged todos for the next phase.

        Returns:
            True if there are staged todos waiting to be applied
        """
        return len(self._staged_todos) > 0

    def get_staged_phase_name(self) -> str:
        """Get the name of the staged phase.

        Returns:
            Phase name or empty string if not set
        """
        return self._staged_phase_name

    def list_staged(self) -> List[TodoItem]:
        """List the todos staged for the next tactical phase."""
        return self._staged_todos.copy()

    def apply_staged_todos(self) -> None:
        """Apply staged todos to the active todo list.

        Moves todos from the staging area to the active list,
        clearing the current todos and staging area.
        """
        if not self._staged_todos:
            logger.warning("No staged todos to apply")
            return

        # Clear current todos and apply staged
        self._todos = self._staged_todos.copy()
        self._next_id = len(self._todos) + 1

        count = len(self._todos)
        phase_name = self._staged_phase_name

        # Clear staging
        self._staged_todos = []
        self._staged_phase_name = ""

        logger.info(
            f"Applied {count} staged todos" + (f" ({phase_name})" if phase_name else "")
        )

    def clear_staged_todos(self) -> None:
        """Clear staged todos without applying them."""
        count = len(self._staged_todos)
        self._staged_todos = []
        self._staged_phase_name = ""
        if count > 0:
            logger.info(f"Cleared {count} staged todos")

    def log_state(self) -> None:
        """Log current todo state for monitoring."""
        total = len(self._todos)
        completed = len([t for t in self._todos if t.status == TodoStatus.COMPLETED])
        in_progress = len(
            [t for t in self._todos if t.status == TodoStatus.IN_PROGRESS]
        )
        pending = len([t for t in self._todos if t.status == TodoStatus.PENDING])

        logger.info(
            f"Todo state: total={total}, completed={completed}, "
            f"in_progress={in_progress}, pending={pending}"
        )

    def get_progress(self) -> Dict[str, Any]:
        """Get progress statistics.

        Returns:
            Dictionary with progress metrics
        """
        total = len(self._todos)
        completed = len([t for t in self._todos if t.status == TodoStatus.COMPLETED])

        return {
            "total": total,
            "completed": completed,
            "pending": total - completed,
            "percentage": round((completed / total * 100) if total > 0 else 0, 1),
        }

    # =========================================================================
    # Compatibility methods for todo_tools.py
    # =========================================================================

    def set_todos_from_list(self, todo_list: List[Dict[str, Any]]) -> str:
        """Replace all todos with items from a list of dictionaries.

        This is used by the next_phase_todos tool to atomically replace the
        entire todo list.

        Args:
            todo_list: List of todo dictionaries with keys:
                - content (str): Task description (required)
                - status (str): "pending", "in_progress", or "completed"
                - priority (str): "high", "medium", or "low" (optional)
                - id (str): Todo ID (optional, auto-generated if missing)

        Returns:
            Formatted summary of the updated todo list
        """
        # Clear existing todos
        self._todos = []
        self._next_id = 1

        # Add each todo from the list
        for item in todo_list:
            content = item.get("content", "")
            status_str = item.get("status", "pending")
            priority = item.get("priority", "medium")
            todo_id = item.get("id")

            # Create todo item
            if todo_id:
                todo = TodoItem(
                    id=todo_id,
                    content=content,
                    priority=priority,
                    status=TodoStatus(status_str),
                )
            else:
                todo = TodoItem(
                    id=f"todo_{self._next_id}",
                    content=content,
                    priority=priority,
                    status=TodoStatus(status_str),
                )
                self._next_id += 1

            self._todos.append(todo)

        logger.info(f"Set {len(self._todos)} todos from list")
        return self._format_todo_summary()

    def archive_and_reset(self, phase_name: str = "") -> str:
        """Archive todos and reset for next phase.

        Convenience method that calls archive() and returns a
        user-friendly message.

        Args:
            phase_name: Optional name for the archived phase

        Returns:
            Confirmation message with archive path
        """
        if not self._todos:
            return "No todos to archive. The todo list is already empty."

        count = len(self._todos)
        archive_path = self.archive(phase_name)

        return (
            f"Archived {count} todos to {archive_path}.\n"
            f"Todo list cleared. Ready for new phase.\n"
            f"Use next_phase_todos to add new tasks."
        )

    def complete_first_pending_sync(
        self, notes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Find and complete the first pending or in-progress task.

        Looks for tasks in this order:
        1. First in_progress task
        2. First pending task (by priority)

        Args:
            notes: Optional completion notes to persist with the selected task.

        Returns:
            Dictionary with:
                - message (str): Status message
                - completed_id (str): ID of completed task (if any)
                - is_last_task (bool): True if this was the last task
        """
        # Find first in_progress task
        in_progress = [t for t in self._todos if t.status == TodoStatus.IN_PROGRESS]
        if in_progress:
            target = in_progress[0]
        else:
            # Find first pending task
            pending = self.list_pending()
            if not pending:
                return {
                    "message": "No pending tasks to complete.",
                    "completed_id": None,
                    "is_last_task": self.all_complete(),  # False if empty, True only if todos exist and all complete
                }
            target = pending[0]

        # Complete it
        self.complete(target.id, notes=notes)

        # Check if all complete now
        is_last = self.all_complete()

        # Build message
        remaining = len(self.list_pending())
        if is_last:
            message = (
                f"Completed: {target.content}\n"
                f"All tasks complete! Ready for phase transition."
            )
        else:
            next_task = self.list_pending()[0] if remaining > 0 else None
            next_str = f"\nNext: {next_task.content}" if next_task else ""
            message = (
                f"Completed: {target.content}\nRemaining: {remaining} tasks{next_str}"
            )

        return {
            "message": message,
            "completed_id": target.id,
            "is_last_task": is_last,
        }

    def _format_todo_summary(self) -> str:
        """Format a summary of the current todo list.

        Returns:
            Formatted summary string
        """
        if not self._todos:
            return "Todo list is empty."

        lines = []
        progress = self.get_progress()
        bar_len = 20
        filled = int(bar_len * progress["percentage"] / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        lines.append(f"Progress: [{bar}] {progress['percentage']}%")
        lines.append(
            f"Total: {progress['total']} | Completed: {progress['completed']} | Pending: {progress['pending']}"
        )
        lines.append("")

        # Group by status
        in_progress = [t for t in self._todos if t.status == TodoStatus.IN_PROGRESS]
        pending = [t for t in self._todos if t.status == TodoStatus.PENDING]
        completed = [t for t in self._todos if t.status == TodoStatus.COMPLETED]

        if in_progress:
            lines.append("IN PROGRESS:")
            for t in in_progress:
                lines.append(f"  → {t.content}")

        if pending:
            lines.append("PENDING:")
            priority_order = {"high": 0, "medium": 1, "low": 2}
            for t in sorted(pending, key=lambda x: priority_order.get(x.priority, 1)):
                marker = "[!]" if t.priority == "high" else "[ ]"
                lines.append(f"  {marker} {t.content}")

        if completed:
            lines.append(f"COMPLETED: ({len(completed)} tasks)")
            for t in completed[:3]:  # Show last 3
                lines.append(f"  ✓ {t.content}")
            if len(completed) > 3:
                lines.append(f"  ... and {len(completed) - 3} more")

        return "\n".join(lines)

    def set_phase_info(
        self,
        phase_number: int = 0,
        total_phases: int = 0,
        phase_name: str = "",
    ) -> None:
        """Legacy compatibility stub - phase tracking is no longer used.

        In the new nested loop architecture, phase transitions are handled
        structurally by graph nodes, not via TodoManager state.

        Args:
            phase_number: Ignored
            total_phases: Ignored
            phase_name: Ignored
        """
        logger.debug(
            f"set_phase_info called (ignored): phase={phase_number}/{total_phases}, name={phase_name}"
        )

    def archive_with_failure_note(
        self,
        issue: str,
        *,
        phase_label: str = "failed",
        heading: str = "Failure Note",
    ) -> str:
        """Archive todos with an explanatory note.

        Used by restore_from_feedback to archive in-flight todos a feedback
        resume preempts (with an honest label instead of "failed"), and by the
        ``max_tool_calls_per_phase`` budget rewind in graph.py.

        NOT used by ``request_replan`` any more. That tool used to call this
        with the "failed" defaults, which wrote *every* todo — including the
        completed ones — into a failure archive and emptied the list, so the
        next strategic phase inherited no record of what had actually been
        achieved. A replan now leaves the todos alone and lets the normal
        phase archive record their real statuses.

        Args:
            issue: Description of why the todos are being archived
            phase_label: Prefix for the archive header name (default "failed")
            heading: Section heading for the appended note

        Returns:
            Confirmation message
        """
        if not self._todos:
            return "No todos to archive."

        # Add explanatory note to archive content
        count = len(self._todos)
        phase_name = f"{phase_label}_{datetime.now(timezone.utc).strftime('%H%M%S')}"

        # Archive with phase name indicating why the phase ended early
        archive_path = self.archive(phase_name)

        # Append the note to the archive file
        note_content = f"\n\n## {heading}\n\n{issue}\n"
        try:
            existing = self._workspace.read_file(archive_path)
            self._workspace.write_file(archive_path, existing + note_content)
        except Exception as e:
            logger.warning(f"Could not append failure note: {e}")

        return (
            f"Archived {count} todos with {heading.lower()} to {archive_path}.\n"
            f"Issue: {issue}\n"
            f"Todo list cleared for re-planning."
        )

    # =========================================================================
    # State persistence methods (for checkpoint/resume)
    # =========================================================================

    def export_state(self) -> Dict[str, Any]:
        """Export full TodoManager state for persistence in LangGraph checkpoints.

        Returns:
            Dictionary containing all state needed to restore the TodoManager:
            - todos: List of todo dicts
            - staged_todos: List of staged todo dicts
            - next_id: Next todo ID counter
            - staged_phase_name: Name of the staged phase
            - phase_number: Current phase number (sequential)
            - current_phase_name: Human-readable phase name
            - is_strategic_phase: Current phase type
        """
        return {
            "todos": [t.to_dict() for t in self._todos],
            "staged_todos": [t.to_dict() for t in self._staged_todos],
            "next_id": self._next_id,
            "staged_phase_name": self._staged_phase_name,
            "phase_number": self._phase_number,
            "current_phase_name": self._current_phase_name,
            "is_strategic_phase": self._is_strategic_phase,
        }

    def restore_state(self, state: Dict[str, Any]) -> None:
        """Restore TodoManager state from persisted checkpoint data.

        Args:
            state: Dictionary containing:
                - todos: List of todo dicts (optional)
                - staged_todos: List of staged todo dicts (optional)
                - next_id or todo_next_id: Next todo ID counter (optional)
                - staged_phase_name: Name of the staged phase (optional)
                - phase_number: Current phase number (optional, default: 1)
                - current_phase_name: Human-readable phase name (optional)
                - is_strategic_phase: Current phase type (optional)
        """
        todos_data = state.get("todos") or []
        self._todos = [TodoItem.from_dict(t) for t in todos_data]

        staged_data = state.get("staged_todos") or []
        self._staged_todos = [TodoItem.from_dict(t) for t in staged_data]

        # Support both "next_id" (from export_state) and "todo_next_id" (from state)
        self._next_id = state.get("next_id") or state.get("todo_next_id") or 1

        self._staged_phase_name = state.get("staged_phase_name") or ""

        # Restore phase tracking fields
        self._phase_number = state.get("phase_number", 1)
        self._current_phase_name = state.get("current_phase_name", "")
        if "is_strategic_phase" in state:
            self._is_strategic_phase = state["is_strategic_phase"]

        logger.info(
            f"Restored TodoManager: {len(self._todos)} todos, "
            f"{len(self._staged_todos)} staged, next_id={self._next_id}, "
            f"phase={self._phase_number} ({('strategic' if self._is_strategic_phase else 'tactical')})"
        )
