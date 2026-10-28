"""Helper functions for citation-verification feedback injection (Phase 2b / D4).

Citation verification runs asynchronously on the auxiliary model: ``cite_*``
returns at once and a background task writes ``verification_status`` back; a
verbatim quote that is not in the source fails at once. This module renders
still-**failed** citations, with the reason, so the agent can correct them.
A failed citation is flagged, never deleted (owner ruling D7): the agent
re-points it with ``edit_citation`` or withdraws it with ``withdraw_citation``.

It is **DB-driven**: the execute node re-computes the failed set from
``verification_status`` every turn (withdrawn citations left out), and the set
is appended as a context entry when it changes (append-only context injection,
D21). So it self-resolves — when the agent edits a citation (which checks it
again) or withdraws it, the next entry no longer lists it, and an emptied set
appends the resolved notice (O6).
"""

from typing import Any, List, Optional

from shared.citation_handles import format_handle

#: Cap how many failed citations are listed in one injection (the rest are
#: summarized as a count) to bound the injected token cost.
MAX_FAILED_CITATIONS_SHOWN = 5

#: ``citations.verification_reason`` codes as the agent reads them.
_REASON_TEXT = {
    "quote_not_found": "the quoted words are not in the source",
    "not_supported": "the passage does not support the claim",
}


def format_failed_citations(
    citations: List[Any], limit: int = MAX_FAILED_CITATIONS_SHOWN
) -> str:
    """Format failed-verification citations into an injectable feedback block.

    Args:
        citations: ``Citation`` objects with ``verification_status == 'failed'``.
        limit: Max citations listed before collapsing the rest into a count.

    Returns:
        A human-readable block asking the agent to fix or withdraw each one.
    """
    n = len(citations)
    lines = [
        "These citations are UNVERIFIED: the check found the quoted text is not "
        "in the source, or that it does not support the claim. Readers see them "
        "flagged. Fix each one with edit_citation (quote the source exactly, "
        "point it at a better passage, or correct the claim; it is checked "
        "again), or withdraw it with withdraw_citation and take its handle out "
        "of your text.",
        "",
    ]
    for c in citations[:limit]:
        claim = c.claim if len(c.claim) <= 140 else c.claim[:140].rstrip() + "…"
        reason = _REASON_TEXT.get(getattr(c, "verification_reason", None) or "")
        detail = c.verification_notes or "no reason recorded"
        if len(detail) > 200:
            detail = detail[:200].rstrip() + "…"
        score = (
            f" (similarity {c.similarity_score:.2f})"
            if c.similarity_score is not None
            else ""
        )
        lines.append(f'- {format_handle(c.id)}{score} claim: "{claim}"')
        lines.append(
            f"    reason: {reason}: {detail}" if reason else f"    reason: {detail}"
        )
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
