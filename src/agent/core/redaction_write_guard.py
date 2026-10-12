"""The write-side half of tool-output redaction: never write the marker back.

Tool results the model reads are credential-redacted
(``agent.core.tool_output_redaction``), so a token in a file comes back as
``[REDACTED]``. The agent edits from what it read: content copied from that
view carries the marker where the real value stood, and writing it replaces
that value — silently, since every later read or diff is redacted the same
way. Every tool that writes agent-supplied content over existing content
(``write_file``, ``edit_file``, ``kb_write``, ``kb_update``) refuses instead,
so a false positive in the redaction can stall an edit but never corrupt what
is stored. Shell heredocs are out of reach of this guard.
"""

from __future__ import annotations

from shared.content_redaction import REDACTED


def adds_redaction_marker(existing: str | None, content: str | None) -> bool:
    """Writing ``content`` over ``existing`` would add a redaction marker.

    Counted, not merely looked for: a test fixture that already holds the
    literal marker must not license writing a SECOND one over a real token
    beside it.
    """
    return (content or "").count(REDACTED) > (existing or "").count(REDACTED)


def redaction_marker_refusal(target: str, tool_name: str) -> str:
    """The error a write tool returns instead of adding the marker."""
    return (
        f"Error: {tool_name} refused — the content carries more {REDACTED} "
        f"markers than {target} holds. The view you read was redacted where a "
        "credential-shaped value stands, and writing the marker would replace "
        "that value. Leave the redacted span untouched: edit only the text "
        "beside it (with an old_string/new_string that avoids the line holding "
        "it). If you are quoting redacted output in new text, write it without "
        "the exact marker (e.g. <redacted>)."
    )


__all__ = ["adds_redaction_marker", "redaction_marker_refusal"]
