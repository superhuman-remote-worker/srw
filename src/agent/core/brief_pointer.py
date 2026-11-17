"""Send a job back to its brief after a compaction.

A job's brief reaches the model once, in the kickoff message. A compaction
replaces that message by a summary, and the summary does not hold the brief
word for word: exact inputs, wording and numbers of work still to do are
gone. The brief itself is unchanged in ``task_brief.md``. Measured on k3d
(2026-11-17): on a job of 16 tasks, the tasks done after a compaction failed
their checks while the ones before it passed, with and without phases, and no
job ever opened the file again.

The compaction hook appends :func:`brief_pointer_after_compaction` once, after
the summary, when the kickoff message is no longer in the kept history.
"""

from __future__ import annotations

from typing import Any, List, Optional

#: The sentence every kickoff message ends its task part with. Its presence in
#: the kept history means the brief is still in front of the model.
BRIEF_SAVED_MARK = "Your task brief is saved to `task_brief.md` in your workspace"

BRIEF_POINTER_LEAD = "[TASK_BRIEF]"

BRIEF_POINTER = (
    f"{BRIEF_POINTER_LEAD} Earlier turns were compacted, and the summary does "
    "not hold your task brief word for word. The brief is unchanged in "
    "`task_brief.md`. Read it again now, and go back to it for the exact "
    "wording, inputs and numbers of each part you work on."
)


def _text(message: Any) -> str:
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return str(content or "")


def brief_pointer_after_compaction(
    retained: List[Any], tool_names: Any
) -> Optional[str]:
    """The pointer to append after a summary, or ``None`` when none is needed.

    ``None`` when the kickoff message is still in ``retained``, when an
    earlier pointer is, or when the job cannot read a file (no ``read_file``
    among its tools: a job without a workspace has nothing to open).
    """
    if isinstance(tool_names, (list, tuple, set, frozenset)) and tool_names:
        if "read_file" not in tool_names:
            return None
    for message in retained:
        text = _text(message)
        if BRIEF_SAVED_MARK in text or BRIEF_POINTER_LEAD in text:
            return None
    return BRIEF_POINTER
