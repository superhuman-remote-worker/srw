"""Helper functions for citation-verification feedback injection (Phase 2b / D4).

Citation verification runs asynchronously on the auxiliary model: ``cite_*``
returns ``pending`` and a background task writes ``verification_status`` back.
This module renders still-**failed** citations so the agent can correct them.

It is **DB-driven**: the execute node re-computes the failed set from
``verification_status`` every turn, and the set is appended as a context
entry when it changes (append-only context injection, D21). So it
self-resolves — when the agent edits a citation (which resets it to
``pending`` → re-verifies) or removes it, the next entry no longer lists it,
and an emptied set appends the resolved notice (O6).
"""

from typing import Any, List, Optional

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
