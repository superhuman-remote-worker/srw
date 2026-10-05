"""Todo management tools for the Universal Agent.

Provides task tracking and phase control tools that enable
the strategic/tactical phase alternation pattern.

In the phase alternation architecture:
- `next_phase_todos` is strategic-only (stages todos for tactical phases)
- `todo_complete` is shared (marks tasks done, triggers phase transitions)
- `todo_list` is shared (helps see current state)
- `request_replan` is tactical-only (the in-flight adaptation path)

The todo list is not re-sent with every request. `todo_complete` and
`next_phase_todos` return the full current list
(``TodoManager.format_for_injection``), so the list enters the history exactly
where it changes; `todo_list` lets the model pull it on demand. See
knowledge-base/knowledge/features/append_only_context_injection.md (D17-D19).
"""

import logging
from typing import Any, List

from langchain_core.tools import tool

from agent.managers.todo import todo_headline
from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    TODO_TOOLS_METADATA as TODO_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

MAX_COMPLETION_NOTE_CHARS = 1000

# Tool metadata for registry
# Phase availability:
#   - "strategic": Available only in strategic mode (planning)
#   - "tactical": Available only in tactical mode (execution)
#   - Both: Available in both modes


def create_todo_tools(context: ToolContext) -> List[Any]:
    """Create todo management tools with injected context.

    Args:
        context: ToolContext with todo_manager

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If context doesn't have a todo_manager
    """
    if not context.has_todo():
        raise ValueError("ToolContext must have a todo_manager for todo tools")

    todo_mgr = context.todo_manager

    @tool
    def next_phase_todos(todos: List[str], phase_name: str = "") -> str:
        """Create todos for the next tactical execution phase.

        This tool stages todos for the upcoming tactical phase. It takes a simple
        list of task strings and stages them directly in the TodoManager.

        Use this to:
        - Create actionable tasks for the next execution phase
        - Prepare work items after strategic planning is complete

        The tool validates:
        - Todo count against the configured bounds (workers: 2-20)
        - Each todo content (minimum 10 characters)

        After calling this tool, complete your current strategic todo by
        invoking the `todo_complete` tool. The phase transition will then
        apply the staged todos.

        Args:
            todos: List of task descriptions. Each must be at least 10 characters.
                   Example: ["Extract document text from PDF", "Chunk document into segments"]
            phase_name: Optional name for the phase (e.g., "Phase 1: Document Processing")

        Returns:
            Confirmation with the staged todos and the full current todo list,
            or an error if validation fails.
        """
        try:
            # Enforcement of the todo guide read is now config-driven via
            # instruction_files triggers (apply_instruction_enforcement wrapper);
            # the guide is the bundled "todo-guide" skill (Slice 3). Fallback check
            # for backward compat when no instruction_files are configured:
            if not context._instruction_files and not context.was_recently_read(
                "skills/todo-guide/SKILL.md"
            ):
                from shared.runtime.services.guardrails import format_nudge

                model = (
                    context._llm_config.model
                    if context._llm_config is not None
                    else None
                )
                return format_nudge(
                    "read_file_required_error",
                    model=model,
                    file_path="skills/todo-guide/SKILL.md",
                    tool_name="next_phase_todos",
                )

            # Convert to list if it's a string (JSON)
            if isinstance(todos, str):
                import json

                todos = json.loads(todos)

            if not isinstance(todos, list):
                return "Error: todos must be a list of task descriptions"

            # Validate each item is a string
            for i, item in enumerate(todos):
                if not isinstance(item, str):
                    return f"Error: todo at index {i} must be a string, got {type(item).__name__}"

            # Use TodoManager's staging method which handles validation
            result = todo_mgr.stage_tactical_todos(todos, phase_name)

            # The staged batch (ids are assigned when the next phase starts),
            # then the full current strategic list (D18).
            staged = "\n".join(f"  - {t.content}" for t in todo_mgr.list_staged())
            message = f"{result}\n{staged}\n\n{todo_mgr.format_for_injection()}"

            # Check if all other strategic todos are complete
            remaining = [t for t in todo_mgr.list_pending()]
            if len(remaining) <= 1:
                # Only the current todo (create todos) remains
                return f"{message}\n\nAll strategic todos complete. Invoke the `todo_complete` tool to transition to tactical phase."
            else:
                return (
                    f"{message}\n\nRemaining strategic todos before transition: "
                    "the pending ones above. Complete each with the "
                    "`todo_complete` tool; completing the last one starts the "
                    "tactical phase."
                )

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"next_phase_todos error: {e}")
            return f"Error staging todos: {str(e)}"

    @tool
    def todo_complete(todo_id: str = "", completion_note: str = "") -> str:
        """Mark a single task as complete.

        Call this tool AFTER you have finished working on a task.
        Complete one todo at a time — each call should reflect verified work
        for that specific task.

        Args:
            todo_id: Optional. The ID of the todo to complete (e.g., "todo_1").
                     If not provided, completes the first incomplete task
                     (in_progress first, then highest-priority pending).
            completion_note: Optional concise outcome that later todos must retain,
                             such as a verification verdict beginning with
                             ``PASS:`` or ``GAPS:``. Notes longer than 1000
                             characters are truncated to fit.

        This is the primary rhythm of work:
        1. Work on the current task
        2. Invoke this tool when done — either with no arguments (completes
           the first pending task) or with todo_id set to a specific id
        3. Read the response to see what's next
        4. Repeat

        Returns:
            Status message including:
            - Which task was completed
            - What the next task is (if any)
            - The full current todo list
            - Phase transition signal when all tasks complete

        Phase Transitions:
            When the last task is completed, this tool signals a phase transition.
            - In tactical mode: System will transition to strategic mode
            - In strategic mode: System will validate todos.yaml and transition to tactical

        Note:
            The response includes "PHASE_COMPLETE" when all tasks are done.
            This is detected by the graph to trigger phase transitions.
        """
        try:
            completion_note = completion_note.strip()
            note_truncated_from = 0
            if len(completion_note) > MAX_COMPLETION_NOTE_CHARS:
                note_truncated_from = len(completion_note)
                marker = f" …[truncated from {note_truncated_from} chars]"
                keep = MAX_COMPLETION_NOTE_CHARS - len(marker)
                completion_note = completion_note[:keep].rstrip() + marker
            completion_notes = [completion_note] if completion_note else None

            # Set by whichever completion path runs below; drives the progress
            # commit at the end of the tool.
            completed_id = ""
            completed_content = ""

            if todo_id and todo_id.strip():
                # Reject comma-separated IDs — complete one todo at a time
                ids = [id.strip() for id in todo_id.split(",") if id.strip()]

                if len(ids) > 1:
                    return (
                        "Error: Complete one todo at a time. Each todo_complete call should "
                        "reflect verified work for that specific task.\n"
                        f"Invoke `todo_complete` with todo_id set to `{ids[0]}` first, "
                        "then invoke it again for each subsequent task."
                    )

                if len(ids) == 1:
                    # Single ID - use existing logic
                    todo = todo_mgr.complete(ids[0], notes=completion_notes)
                    if not todo:
                        # List available todos to help the agent
                        available = todo_mgr.list_all()
                        if available:
                            todo_list_str = ", ".join(t.id for t in available)
                            return f"Error: Todo '{ids[0]}' not found. Available todos: {todo_list_str}"
                        return (
                            f"Error: Todo '{ids[0]}' not found. No todos in the list."
                        )

                    completed_id = todo.id
                    completed_content = todo.content

                    # Queue memory for completed todo with notes (Memory Light)
                    # Only store if notes contain substantive content (>50 chars)
                    if context.recall_store and todo.notes:
                        combined_notes = "; ".join(todo.notes)
                        if len(combined_notes) > 50:
                            from shared.runtime.services.recall_store import (
                                extract_keywords,
                            )

                            context.queue_memory(
                                content=f"Completed: {todo.content}\nOutcome: {combined_notes}",
                                keywords=extract_keywords(todo.content),
                                importance=0.7,
                                source="todo",
                                memory_type="procedural",
                                source_phase=todo_mgr.phase_number,
                            )

                    is_last = todo_mgr.all_complete()
            else:
                # Original behavior: complete first pending task
                result = todo_mgr.complete_first_pending_sync(notes=completion_notes)
                message = result["message"]
                is_last = result.get("is_last_task", False)
                completed_id = result.get("completed_id") or ""
                if completed_id:
                    _done = next(
                        (t for t in todo_mgr.list_all() if t.id == completed_id), None
                    )
                    completed_content = _done.content if _done else ""

            # A completion returns the full current list (D18): the list
            # enters the history where it changed, never per request.
            if completed_id:
                # Headlines only: the list below carries the pending bodies.
                if is_last:
                    head = (
                        f"Completed: {todo_headline(completed_content)}\n"
                        "All tasks complete! Ready for phase transition."
                    )
                else:
                    remaining = todo_mgr.list_pending()
                    head = f"Completed: {todo_headline(completed_content)}" + (
                        f"\nNext: {remaining[0].id}: {todo_headline(remaining[0].content)}"
                        if remaining
                        else ""
                    )
                message = f"{head}\n\n{todo_mgr.format_for_injection()}"

            # Add phase transition signal if this was the last task
            if is_last:
                message += "\n\n[PHASE_COMPLETE] All tasks in this phase are done."
                logger.info("Last task completed - phase transition signal sent")

            if completion_note and completed_id:
                message += f"\n\nRecorded completion note: {completion_note}"
                if note_truncated_from:
                    message += (
                        f"\nNote: completion_note was truncated from "
                        f"{note_truncated_from} chars to fit the "
                        f"{MAX_COMPLETION_NOTE_CHARS}-char limit. Record only the "
                        "concise outcome needed by the next todo."
                    )

            # Make the finished work durable. A completed todo is a real unit of
            # progress, so it is the honest place to commit — and the commit is
            # what every external observer (orchestrator, cockpit, MCP tools,
            # officer) actually reads. Pushing is throttled inside the committer;
            # git failures are swallowed there and must never fail the tool.
            committer = getattr(context, "progress_committer", None)
            if committer is not None and completed_id:
                committer.on_todo_complete(completed_id, completed_content)

            # Same moment, the other half: a finished todo is also the natural
            # break at which to deliver queued (non-urgent) replies. Urgent
            # guidance does not wait for this — it renders every turn.
            if completed_id:
                request_drain = getattr(context, "request_reply_drain", None)
                if callable(request_drain):
                    request_drain()

            return message

        except Exception as e:
            logger.error(f"todo_complete error: {e}")
            return f"Error completing task: {str(e)}"

    @tool
    def todo_list() -> str:
        """List all current todos with their IDs and status.

        Use this tool to see:
        - All todo IDs (for use with todo_complete)
        - Current status of each todo
        - Overall progress

        Returns:
            Formatted list of all todos with IDs and status.

        Example output:
            Current Todos (2/5 complete):
            - [x] todo_1: Extract document text (completed)
            - [x] todo_2: Chunk document (completed)
            - [ ] todo_3: Identify requirements (pending)
            - [ ] todo_4: Validate requirements (pending)
            - [ ] todo_5: Write to database (pending)
        """
        try:
            todos = todo_mgr.list_all()
            if not todos:
                return "No todos in the current list."

            completed = sum(1 for t in todos if t.status.value == "completed")
            total = len(todos)

            lines = [f"Current Todos ({completed}/{total} complete):"]
            for todo in todos:
                status_mark = "x" if todo.status.value == "completed" else " "
                status_text = f"({todo.status.value})"
                priority_mark = " [!]" if todo.priority == "high" else ""
                lines.append(
                    f"- [{status_mark}] {todo.id}: {todo.content}{priority_mark} {status_text}"
                )

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"todo_list error: {e}")
            return f"Error listing todos: {str(e)}"

    @tool
    def request_replan(reason: str) -> str:
        """End this phase early and go back to planning, keeping all your work.

        Use this when you have learned something that changes the approach and
        it would be wrong to keep working through the remaining todos. This is
        NOT for normal task completion — use `todo_complete` for that.

        When to use request_replan:
        - You discovered the planned approach will not work
        - What you found makes some remaining todos pointless or wrong
        - An external constraint invalidates part of the plan
        - The work turned out much larger or smaller than the plan assumed

        Nothing is lost or undone. Your files, commits and completed todos all
        stay exactly as they are; todos you finished stay finished and todos
        you did not start stay pending. The phase simply ends here instead of
        after the last todo, and the planning phase begins with the full record
        of what happened plus the reason you give below.

        In the planning phase you will review plan.md against what actually
        happened, adjust it, and stage the next batch with `next_phase_todos`.

        Args:
            reason: What you learned and why it changes the plan. Be specific —
                    this is what the planning phase reads first.
                    Example: "The API caps batch operations at 100 items, so
                    the bulk-import todos need to become paged imports."

        Returns:
            Confirmation that the phase will end after this turn.
        """
        if not reason or not reason.strip():
            return (
                "Error: You must provide a 'reason' explaining what changed. "
                "The planning phase reads it to decide what to adjust."
            )

        try:
            # Deliberately does NOT archive-as-failed or clear the todo list.
            # The old behaviour wrote every todo — including the completed ones
            # — into archive/failed_<time>.md and emptied the list, so the
            # planning phase inherited no record of what had actually been
            # achieved and had to reconstruct it. Leaving the todos alone means
            # archive_phase records them at the boundary with their real
            # statuses, which is exactly what the planning phase needs to
            # decide what to keep.
            done = sum(1 for t in todo_mgr.list_all() if t.status.value == "completed")
            total = len(todo_mgr.list_all())

            note = ""
            # Staged todos are a bet on the plan that is now being revised —
            # drop them so the planning phase re-decides rather than inheriting
            # a stale batch it never sees.
            if todo_mgr.has_staged_todos():
                todo_mgr.clear_staged_todos()
                note = "\nPreviously staged todos were cleared — stage a fresh batch."

            context.request_replan(reason.strip())

            return (
                f"Replan requested. This phase ends after the current turn.\n"
                f"Progress kept: {done}/{total} todos complete; nothing was "
                f"undone or discarded.\n"
                f"Reason recorded: {reason.strip()}{note}\n\n"
                "The planning phase will start with the full record of this "
                "phase. There, review plan.md against what actually happened, "
                "update it, then stage the next batch with next_phase_todos()."
            )

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"request_replan error: {e}")
            return f"Error requesting replan: {str(e)}"

    # Return all todo tools
    return [
        next_phase_todos,
        todo_complete,
        todo_list,
        request_replan,
    ]
