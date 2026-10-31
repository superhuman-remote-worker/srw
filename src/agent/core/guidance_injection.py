"""Supervisor-guidance rendering for context injection.

Renders pending supervisor guidance (the non-destructive steer lane, P1-A of
knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md)
as the body of a guidance context entry, so the worker sees mid-run steering
on its very next LLM turn — without a kill, a compaction, or a re-plan. The
entry is appended once and stays in the history (append-only context
injection, D21).
"""

from typing import Any, Dict, List


def format_supervisor_guidance(entries: List[Dict[str, Any]]) -> str:
    """Render all pending guidance entries as one [SUPERVISOR GUIDANCE] block.

    Args:
        entries: Pending guidance entries from the heartbeat inbox, each
            ``{id, text, source, created_at}``.

    Returns:
        Formatted block, or "" when there is nothing to render.
    """
    lines: List[str] = []
    for entry in entries:
        text = str(entry.get("text", "")).strip()
        if not text:
            continue
        created_at = entry.get("created_at", "")
        source = entry.get("source", "")
        meta = ", ".join(part for part in (str(source), str(created_at)) if part)
        suffix = f" ({meta})" if meta else ""
        lines.append(f"- {text}{suffix}")

    if not lines:
        return ""

    header = (
        "[SUPERVISOR GUIDANCE] Mid-run guidance from your supervisor. "
        "Your current plan and todos remain in force — fold this guidance "
        "into the work in progress instead of re-planning."
    )
    return header + "\n\n" + "\n".join(lines)
