"""Footnotes for delivered documents (citation engine overhaul §6.5, WP4).

The agent writes handles (``[^c1241]``) while it writes. When a document is
delivered, this pass appends one Markdown footnote definition per handle the
document uses, in first-mention order and only for what it cites (as STORM
and PaperQA2 do), so the file reads as numbered footnotes in GitHub, Obsidian
or Pandoc without SRW. A citation that is unverified, not checked or
withdrawn says so in its footnote rather than disappearing (owner ruling D7).

Deterministic and idempotent: the definitions live in one marked block that is
regenerated on each delivery. It edits workspace files only, never the
model's context.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from shared.citation_handles import (
    DEFINITIONS_START,
    format_handle,
    handles_in,
    replace_definitions,
)

log = logging.getLogger(__name__)

#: Files the pass rewrites. Other deliverables are left alone.
MARKDOWN_SUFFIXES = (".md", ".markdown")

_QUOTE_MAX = 160

_REASONS = {
    "quote_not_found": "the quoted words are not in the source",
    "not_supported": "the passage does not support the claim",
    "source_unavailable": "the source could not be found to check against",
    "not_checked": "no check ran",
}


def _clip(text: str, limit: int = _QUOTE_MAX) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[:limit].rstrip() + "…"


def _locator(locator: dict[str, Any] | None) -> str:
    parts = []
    for key, value in (locator or {}).items():
        if value in (None, "") or key in ("title",):
            continue
        if key == "page":
            parts.append(f"p. {value}")
        elif key == "section":
            parts.append(f"§ {value}")
        elif key == "accessed_at":
            parts.append(f"accessed {value}")
        else:
            parts.append(f"{key} {value}")
    return ", ".join(parts)


def definition(citation_id: int, citation: Any | None, source: Any | None) -> str:
    """One footnote definition line for a handle."""
    handle = format_handle(citation_id)
    if citation is None:
        return f"{handle}: **Citation not found** for this job."
    parts = []
    if source is not None:
        name = source.name or source.identifier
        identifier = source.identifier or ""
        if identifier.startswith(("http://", "https://")) and identifier != name:
            parts.append(f"{name}, <{identifier}>")
        else:
            parts.append(name)
    else:
        parts.append("Source no longer available")
    where = _locator(citation.locator)
    if where:
        parts.append(where)
    line = f"{handle}: " + ", ".join(p for p in parts if p) + "."
    quote = citation.verbatim_quote or citation.quote_context
    if quote:
        line += f" “{_clip(quote)}”"
    status = citation.verification_status.value
    reason = _REASONS.get(citation.verification_reason or "")
    if citation.withdrawn_at is not None:
        line += f" **Withdrawn:** {citation.withdrawn_reason or 'no reason given'}."
    elif status == "failed":
        line += f" **Unverified:** {reason or 'the check did not confirm it'}."
    elif status == "unverified":
        line += " (Not checked.)"
    elif status == "pending":
        line += " (Check still running at delivery.)"
    return line


async def add_footnotes(
    engine: Any, text: str, citations: dict[int, Any] | None = None
) -> tuple[str, list[int]]:
    """Return ``text`` with its footnote block regenerated, and the handles used.

    ``citations`` maps id → Citation for the job; read from the engine when
    not given. Handles of other jobs or made-up ones get a "not found"
    definition, so no footnote dangles.
    """
    ids = handles_in(text)
    if citations is None:
        citations = {c.id: c for c in await engine.list_citations()}
    sources: dict[int, Any] = {}
    lines = []
    for cid in ids:
        citation = citations.get(cid)
        source = None
        if citation is not None:
            if citation.source_id not in sources:
                sources[citation.source_id] = await engine.get_source(
                    citation.source_id
                )
            source = sources[citation.source_id]
        lines.append(definition(cid, citation, source))
    return replace_definitions(text, lines), ids


def citation_report(citations: Iterable[Any]) -> dict[str, Any]:
    """What the completion report says about a job's citations.

    The reviewer sees the unverified ones before approving, as CoCounsel's
    Verify report shows them (overhaul §6.3).
    """
    active = [c for c in citations if c.withdrawn_at is None]
    unverified = [c for c in active if c.verification_status.value == "failed"]
    return {
        "total": len(active),
        "verified": sum(1 for c in active if c.verification_status.value == "verified"),
        "unverified": [
            {
                "handle": format_handle(c.id),
                "claim": _clip(c.claim, 200),
                "reason": c.verification_reason,
            }
            for c in unverified[:20]
        ],
        "unverified_count": len(unverified),
        "not_checked": sum(
            1 for c in active if c.verification_status.value == "unverified"
        ),
        "pending": sum(1 for c in active if c.verification_status.value == "pending"),
        "withdrawn": sum(1 for c in citations if c.withdrawn_at is not None),
    }


async def deliver_documents(
    engine: Any, workspace: Any, paths: Iterable[str], *, verify_timeout: float = 10.0
) -> dict[str, Any]:
    """Run the pass over the Markdown deliverables of a job.

    Waits briefly for running checks first, so the footnotes and the report
    show verdicts rather than "still running". Returns ``{"files": {path:
    handles_used}, "citations": citation_report(...)}``. A file that cannot
    be read or written is skipped and logged; delivery never fails a job.
    """
    try:
        await engine.await_pending_verifications(timeout=verify_timeout)
    except Exception as e:
        log.debug("waiting for citation checks before delivery failed: %s", e)
    all_citations = await engine.list_citations()
    by_id = {c.id: c for c in all_citations}
    files: dict[str, int] = {}
    for path in paths:
        if not str(path).lower().endswith(MARKDOWN_SUFFIXES):
            continue
        try:
            text = workspace.read_file(path)
            if not handles_in(text) and DEFINITIONS_START not in text:
                continue
            updated, ids = await add_footnotes(engine, text, by_id)
            if updated != text:
                workspace.write_file(path, updated)
            files[path] = len(ids)
        except Exception as e:
            if type(e).__name__ == "WorkspaceUnavailableError":
                raise
            log.warning("Citation footnotes for %s skipped: %s", path, e)
    return {"files": files, "citations": citation_report(all_citations)}
