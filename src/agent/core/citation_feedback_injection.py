"""Helper functions for citation-verification feedback injection (Phase 2b / D4).

Citation verification runs asynchronously on the auxiliary model: ``cite_*``
returns ``pending`` and a background task writes ``verification_status`` back.
This module surfaces still-**failed** citations to the agent so it can correct
them — injected as a synthetic ``check_citation_verification`` tool-call result,
mirroring ``memory_injection.py``.

It is **DB-driven and transient**: the execute node re-computes the failed set
from ``verification_status`` every turn and re-injects it (excluded from
summarization via ``is_workspace_injection_message``). So it self-resolves — when
the agent edits a citation (which resets it to ``pending`` → re-verifies) or
removes it, it drops out of the injected block on the next turn.
"""

from typing import Any, List, Optional, Tuple

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

from shared.runtime.core.injection_markers import (
    CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX as CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
)
from shared.runtime.core.workspace_injection import content_hash_id

#: Cap how many failed citations are listed in one injection (the rest are
#: summarized as a count) to bound the injected token cost.
MAX_FAILED_CITATIONS_SHOWN = 5


def format_failed_citations(
    citations: List[Any], limit: int = MAX_FAILED_CITATIONS_SHOWN
) -> str:
    """Format failed-verification citations into an injectable feedback block.

    Args:
        citations: ``Citation`` objects with ``verification_status == 'failed'``.
        limit: Max citations listed before collapsing the rest into a count.

    Returns:
        A human-readable block instructing the agent to fix/remove each one.
    """
    n = len(citations)
    lines = [
        "Automatic verification FAILED for the citation(s) below — the quoted "
        "text could not be confirmed against the source, or it does not support "
        "the claim. Fix each one with the edit_citation tool (correct the quote, "
        "quote_context, or claim), or remove the citation if it cannot be "
        "supported. Edited citations are automatically re-verified.",
        "",
    ]
    for c in citations[:limit]:
        claim = c.claim if len(c.claim) <= 140 else c.claim[:140].rstrip() + "…"
        reason = c.verification_notes or "no reason recorded"
        if len(reason) > 200:
            reason = reason[:200].rstrip() + "…"
        score = (
            f" (similarity {c.similarity_score:.2f})"
            if c.similarity_score is not None
            else ""
        )
        lines.append(f'- [{c.id}]{score} claim: "{claim}"')
        lines.append(f"    reason: {reason}")
    if n > limit:
        lines.append(f"- … and {n - limit} more failed citation(s).")
    return "\n".join(lines)


def format_citation_feedback_resolved(model: Optional[str] = None) -> str:
    """The cleared rendering: no citation is failing verification any more.

    Append-only injection (D21, O6) appends this once when the failed set
    that an earlier entry reported becomes empty, so the model knows the
    report above no longer applies.
    """
    from shared.runtime.services.guardrails import format_nudge

    return format_nudge("citation_feedback_resolved", model=model)


def create_citation_feedback_injection_messages(
    content: str,
) -> Tuple[AIMessage, ToolMessage]:
    """Create a synthetic AIMessage + ToolMessage pair for citation feedback.

    Makes it appear as if the agent called ``check_citation_verification`` and
    received the failed-citation report. The ai+tool pairing keeps providers
    that require a function-call to precede its response (Gemini) happy.

    Args:
        content: Formatted failed-citation block from ``format_failed_citations``.

    Returns:
        Tuple of (AIMessage with tool_call, ToolMessage with the report).
    """
    tool_call_id = f"{CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX}{content_hash_id(content)}"

    ai_message = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "check_citation_verification",
                "args": {},
                "id": tool_call_id,
            }
        ],
    )
    tool_message = ToolMessage(content=content, tool_call_id=tool_call_id)
    return ai_message, tool_message


def is_citation_feedback_injection_message(message: BaseMessage) -> bool:
    """Check if a message is part of a citation-feedback injection pair."""
    if isinstance(message, ToolMessage):
        tool_call_id = getattr(message, "tool_call_id", "")
        return tool_call_id.startswith(CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX)
    if isinstance(message, AIMessage):
        if hasattr(message, "tool_calls") and message.tool_calls:
            for tc in message.tool_calls:
                if tc.get("id", "").startswith(CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX):
                    return True
    return False
