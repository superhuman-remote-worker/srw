"""Helper functions for supervisor-guidance injection as synthetic tool calls.

Mirrors memory_injection.py. Renders pending supervisor guidance (the
non-destructive steer lane, P1-A of
knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md) as a fake
``supervisor_guidance`` tool-call result so the worker sees mid-run steering
on its very next LLM turn — without a kill, a compaction, or a re-plan.

The messages are transient — re-derived fresh every execute() call from the
heartbeat-fed inbox (src/agent/api/dual_app.py), so they are immune to context
compaction. They keep rendering until the ack round-trip moves the entries
from ``context.pending_guidance`` to ``context.consumed_replies`` on the
orchestrator (at-least-once delivery, ~1-2 turns of overlap).
"""

from typing import Any, Dict, List, Tuple

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

from shared.runtime.core.injection_markers import (
    GUIDANCE_TOOL_CALL_ID_PREFIX as GUIDANCE_TOOL_CALL_ID_PREFIX,
)
from shared.runtime.core.workspace_injection import content_hash_id


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
        "into the work in progress instead of re-planning. It may repeat "
        "for a turn or two until delivery is confirmed; act on it once."
    )
    return header + "\n\n" + "\n".join(lines)


def create_guidance_injection_messages(
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    """Create synthetic AIMessage + ToolMessage pair for guidance injection.

    Creates a fake tool call that makes it appear as if the agent already
    called ``supervisor_guidance`` and received the block.

    Args:
        content: Formatted block from format_supervisor_guidance()

    Returns:
        Tuple of (AIMessage with tool_call, ToolMessage with guidance content)
    """
    tool_call_id = f"{GUIDANCE_TOOL_CALL_ID_PREFIX}{content_hash_id(content)}"

    ai_message = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "supervisor_guidance",
                "args": {"scope": "current_run"},
                "id": tool_call_id,
            }
        ],
    )

    tool_message = ToolMessage(
        content=content,
        tool_call_id=tool_call_id,
    )

    return ai_message, tool_message


def is_guidance_injection_message(message: BaseMessage) -> bool:
    """Check if a message is part of a guidance injection pair."""
    if isinstance(message, ToolMessage):
        tool_call_id = getattr(message, "tool_call_id", "")
        return tool_call_id.startswith(GUIDANCE_TOOL_CALL_ID_PREFIX)

    if isinstance(message, AIMessage):
        if hasattr(message, "tool_calls") and message.tool_calls:
            for tc in message.tool_calls:
                if tc.get("id", "").startswith(GUIDANCE_TOOL_CALL_ID_PREFIX):
                    return True

    return False
