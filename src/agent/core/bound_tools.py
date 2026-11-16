"""Which procedure tools a job has, read off its loaded tool list.

The worker composes a few texts that name the phase tools: the reminder after
a reply without a tool call, the to-do list footer, the kickoff message, the
feedback message and ``job_complete``'s own description. A job that is not
offered those tools must not be told to call them. The worker does not read
how the job was configured; it looks only at the tools it actually loaded:

* ``todo_complete`` decides every text about to-dos;
* ``next_phase_todos`` decides every text about phases and plans.

An unknown tool list (``None``, empty, or not a list) counts as "bound", so a
caller that has no list at hand keeps today's text.
"""

from __future__ import annotations

from typing import Any

TODO_TOOL = "todo_complete"
PHASE_TOOL = "next_phase_todos"


def binds(tool_names: Any, name: str) -> bool:
    """True unless ``tool_names`` is a known, non-empty list that lacks ``name``."""
    if not isinstance(tool_names, (list, tuple, set, frozenset)) or not tool_names:
        return True
    return name in tool_names


def binds_todo_tool(tool_names: Any) -> bool:
    """True when the job can close its to-dos with ``todo_complete``."""
    return binds(tool_names, TODO_TOOL)


def binds_phase_tool(tool_names: Any) -> bool:
    """True when the job plans phases with ``next_phase_todos``."""
    return binds(tool_names, PHASE_TOOL)
