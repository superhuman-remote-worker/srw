"""Citation tools for the Universal Agent.

Provides document and web citation capabilities for requirement traceability.
Integrates with CitationEngine for verified, persistent citations.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union, get_args

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.citation_handles import (
    format_handle,
    format_source_ref,
    parse_citation_ref,
    parse_source_ref,
)
from shared.tool_catalog.definitions import (
    CITATION_TOOLS_METADATA as CITATION_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Closed vocabularies, mirrored from src/citation_engine/models.py and the
# Postgres enums in orchestrator/database/migrations/vector/0001_initial.sql.
#
# These MUST be Literal, not str: every citation tool is registered
# defer_to_workspace=True, so apply_description_overrides() replaces the
# docstring naming these values with a one-line short_description before the
# tool is bound. The docstring does not reach the model; the args_schema does
# (and is serialized on every call regardless). A vocabulary documented only in
# prose is therefore invisible — see
# knowledge-base/knowledge/issues/agent_tool_fixed_vocabularies_invisible_to_model.md.
#
# Spelled literally rather than derived from the enums so these signatures do
# not import the engine at load time (the @tool decorator evaluates
# annotations). tests/test_tool_vocabularies.py asserts they stay in sync.
ExtractionMethodValue = Literal[
    "direct_quote", "paraphrase", "inference", "aggregation", "negative"
]
ConfidenceValue = Literal["high", "medium", "low"]
VerificationStatusValue = Literal["pending", "verified", "failed", "unverified"]
SourceTypeValue = Literal["document", "website", "database", "custom"]
AnnotationTypeValue = Literal["note", "highlight", "summary", "question", "critique"]
SearchModeValue = Literal["hybrid", "keyword", "semantic"]
SearchScopeValue = Literal["content", "annotations", "all"]
BibliographyStyleValue = Literal["bibtex", "harvard", "ieee", "apa", "inline"]
TagActionValue = Literal["add", "remove"]

#: A citation as the agent names it: the handle the cite tool returned
#: (``[^c1241]``) or its number. Parsed by ``parse_citation_ref``.
CitationRef = Union[str, int]
#: A source as the tools print it (``S12``) or its number.
SourceRef = Union[str, int]


#: Postgres enum type name -> the vocabulary the caller should have been offered.
#: Only the vector-DB enums reachable from these tools; ``AnnotationType`` is a
#: Python-side enum and already raises a ValueError with its own message.
_DB_ENUM_VOCABULARIES: Dict[str, tuple] = {
    "extraction_method": get_args(ExtractionMethodValue),
    "confidence_level": get_args(ConfidenceValue),
    "verification_status": get_args(VerificationStatusValue),
    "source_type": get_args(SourceTypeValue),
}


def _humanize_db_enum_error(exc: Exception) -> Optional[str]:
    """Translate a Postgres enum rejection into something an agent can act on.

    asyncpg raises ``InvalidTextRepresentationError`` for a bad enum label. It
    is a ``DataError``, **not** a ``ValueError``, so it slips past the handlers
    that return curated messages — and its text names no valid values, leaving
    an agent to brute-force the vocabulary. Last-resort net: the tool schemas
    and the engine's validation should both reject a bad value long before it
    reaches Postgres.

    Returns None when ``exc`` is not a recognised enum error, so callers can
    fall through to their normal error handling.
    """
    match = re.search(r'invalid input value for enum (\w+): "(.*)"', str(exc))
    if not match:
        return None
    enum_name, bad_value = match.group(1), match.group(2)
    allowed = _DB_ENUM_VOCABULARIES.get(enum_name)
    if not allowed:
        return None
    return (
        f"invalid value '{bad_value}' for {enum_name}. Use one of: "
        f"{', '.join(allowed)}."
    )


def _verbatim_or_none(text: str, extraction_method: str) -> Optional[str]:
    """Return ``text`` as a verbatim quote only if the agent claims it is one.

    The verifier's word-for-word check is triggered by the *presence* of
    ``verbatim_quote`` — ``VerifyCitationTask.build_context`` emits a
    "## Verbatim Quote" section only when it is set, and the prompt then
    requires the text to appear in the source. Filing a paraphrase under that
    field is what makes an otherwise-sound citation fail verification, and the
    create tools used to do exactly that: they hardcoded
    ``extraction_method="direct_quote"`` no matter what ``text`` really was.

    For any non-quote method the field stays unset, so the citation is checked
    for meaning against ``quote_context`` instead — which the verification
    prompt already supports ("a close, meaning-preserving match", ~0.7 score).

    The ``< 1000`` guard is pre-existing behaviour, kept as-is: an overlong
    "quote" is treated as context rather than a quote.
    """
    if extraction_method != "direct_quote":
        return None
    return text[:500] if len(text) < 1000 else None


#: What each status means to the agent, in the cite tools' first line.
_STATUS_SENTENCES = {
    "pending": "verification pending",
    "verified": "verified",
    "failed": "UNVERIFIED",
    "unverified": "not checked",
}


def _status_phrase(status: str, withdrawn: bool = False) -> str:
    phrase = _STATUS_SENTENCES.get(status, status)
    return f"{phrase}, WITHDRAWN" if withdrawn else phrase


def _locator_text(locator: Optional[Dict[str, Any]]) -> str:
    if not locator:
        return ""
    parts = [
        f"{k} {v}"
        for k, v in locator.items()
        if v and k not in ("accessed_at", "title")
    ]
    return f" ({', '.join(parts)})" if parts else ""


def _created_output(
    result: Any, source_line: str, extra_lines: Optional[List[str]] = None
) -> str:
    """The cite tools' result: the handle first, then what to do with it.

    Kept short on purpose (E4): the agent needs the handle, the state and the
    next step, not "Similarity Score: N/A".
    """
    handle = format_handle(result.citation_id)
    status = result.verification_status.value
    if status == "failed":
        head = f"Citation {handle} created but UNVERIFIED: {result.verification_notes}"
        tail = (
            f"Fix it with edit_citation (it is checked again), or withdraw it "
            f"with withdraw_citation and leave {handle} out of your text."
        )
    else:
        head = f"Citation {handle} created; {_status_phrase(status)}."
        if status == "unverified" and result.verification_notes:
            head += f" {result.verification_notes}"
        tail = (
            f"Write {handle} right after the claim it supports, copied exactly. "
            "Readers see it as a numbered footnote; never write a number yourself."
        )
    lines = [head, f"Source: {source_line}"]
    lines.extend(extra_lines or [])
    lines += ["", tail]
    return "\n".join(lines)


def create_source_tools(context: ToolContext) -> List[Any]:
    """Create citation tools with injected context.

    Args:
        context: ToolContext with dependencies

    Returns:
        List of LangChain tool functions
    """
    # Get workspace manager for path resolution
    workspace = context.workspace_manager if context.has_workspace() else None

    @tool
    async def cite_document(
        text: str,
        document_path: str,
        page: Optional[int] = None,
        section: Optional[str] = None,
        claim: Optional[str] = None,
        extraction_method: ExtractionMethodValue = "direct_quote",
        confidence: ConfidenceValue = "high",
    ) -> str:
        """Create a verified citation for document content.

        Registers the document as a source (if not already registered) and creates
        a citation linking your claim to the quoted text. A direct quote is
        checked against the document at once; whether it supports the claim is
        checked in the background.

        Tip: Use search_library first to find relevant evidence across all sources,
        then cite the specific passage with this tool.

        Set extraction_method honestly — it decides how the citation is verified.
        'direct_quote' means `text` appears verbatim in the source and will be
        checked word-for-word; anything else is checked for meaning instead. If
        you reworded the source, say 'paraphrase' rather than letting a reworded
        passage be checked as a quote (it will fail).

        Args:
            text: The evidence from the document — verbatim if extraction_method
                is 'direct_quote', otherwise your rewording/synthesis of it
            document_path: Path to the source document
            page: Page number if applicable
            section: Section reference if applicable
            claim: The assertion being supported (defaults to summary of text)
            extraction_method: How you got `text` from the source
            confidence: Your self-assessment of how well the source backs the claim

        Returns:
            The citation handle, e.g. [^c1241]. Write it right after the claim,
            copied exactly; readers see a numbered footnote.
        """
        try:
            # Register source and create citation
            cloud_anchor = None
            try:
                # Phase 3 (D7): if this file was read from a user's cloud, a
                # snapshot-anchor (drift fingerprint + live pointer) was stashed
                # at read time — persist it onto the source's metadata.cloud.
                # The identity is workspace-relative. Resolving it first is a
                # backend-path bypass: virtual paths are object keys and remote
                # paths name files on another pod, not local agent files.
                cloud_anchor = context.get_cloud_anchor(document_path)
                # Phase 3b: snapshot the original bytes to the blob store (via the
                # orchestrator — the agent has no S3 creds) so the citation has a
                # "view original" backup. Mutates cloud_anchor with the blob key
                # before registration; best-effort, never blocks the citation.
                if cloud_anchor:
                    await context.snapshot_cloud_source_bytes(
                        document_path, cloud_anchor
                    )
                # Registration also materializes through local_copy in
                # get_or_register_doc_source; no backend path reaches local I/O.
                source_id = await context.get_or_register_doc_source(
                    document_path,
                    name=Path(document_path).name,
                    cloud_metadata=cloud_anchor,
                )
            except FileNotFoundError:
                return f"Error: Document not found at {document_path}"
            except Exception as e:
                # Registration failed for a real reason (extraction, embedding,
                # remote fetch, …): report the actual error.
                logger.warning(f"Could not register document source: {e}")
                return (
                    f"Error: could not register document source '{document_path}': {e}"
                )

            # Build locator dict
            locator = {}
            if page is not None:
                locator["page"] = page
            if section:
                locator["section"] = section

            # Create the citation
            engine = context.get_citation_engine()
            effective_claim = claim or f"Evidence from document: {text[:100]}..."
            result = await engine.cite_doc(
                claim=effective_claim,
                source_id=source_id,
                quote_context=text,
                locator=locator,
                verbatim_quote=_verbatim_or_none(text, extraction_method),
                confidence=confidence,
                extraction_method=extraction_method,
            )

            extra = []
            if cloud_anchor:
                if cloud_anchor.get("snapshot_blob_key"):
                    extra.append(
                        "Cloud-anchored: yes (original snapshotted for drift detection)"
                    )
                else:
                    extra.append("Cloud-anchored: yes (live pointer recorded)")
            return _created_output(
                result,
                f"{format_source_ref(source_id)} {document_path}{_locator_text(locator)}",
                extra,
            )

        except Exception as e:
            logger.error(f"Citation creation error: {e}")
            return f"Error creating citation: {str(e)}"

    @tool
    async def cite_web(
        text: str,
        url: str,
        title: Optional[str] = None,
        accessed_date: Optional[str] = None,
        claim: Optional[str] = None,
        extraction_method: ExtractionMethodValue = "direct_quote",
        confidence: ConfidenceValue = "high",
    ) -> str:
        """Create a verified citation for web content.

        Registers the URL as a source (fetching and archiving its content) and creates
        a citation linking your claim to the quoted text. A direct quote is
        checked against the archived page at once; whether it supports the
        claim is checked in the background.

        Tip: Use search_library first to find relevant evidence across all sources,
        then cite the specific passage with this tool.

        Set extraction_method honestly — it decides how the citation is verified.
        'direct_quote' means `text` appears verbatim in the source and will be
        checked word-for-word; anything else is checked for meaning instead. If
        you reworded the source, say 'paraphrase' rather than letting a reworded
        passage be checked as a quote (it will fail).

        Args:
            text: The evidence from the web — verbatim if extraction_method is
                'direct_quote', otherwise your rewording/synthesis of it
            url: Source URL
            title: Page title (auto-detected if not provided)
            accessed_date: Date accessed in ISO format (defaults to today)
            claim: The assertion being supported (defaults to summary of text)
            extraction_method: How you got `text` from the source
            confidence: Your self-assessment of how well the source backs the claim

        Returns:
            The citation handle, e.g. [^c1241]. Write it right after the claim,
            copied exactly; readers see a numbered footnote.
        """
        try:
            # Use today's date if not provided
            if not accessed_date:
                accessed_date = datetime.utcnow().strftime("%Y-%m-%d")

            # Register source and create citation
            try:
                source_id, fetch_error = await context.get_or_register_web_source(
                    url, name=title
                )
            except Exception as e:
                # Registration failed for a real reason: report the actual error.
                logger.warning(f"Could not register web source: {e}")
                return f"Error: could not register web source '{url}': {e}"

            # Persist web content to disk (idempotent — no-op if already saved by research tools)
            if text:
                context.save_web_content_to_disk(
                    url, text, title=title, source_id=source_id
                )

            # Build locator dict
            locator = {"accessed_at": accessed_date}
            if title:
                locator["title"] = title

            # Create the citation
            engine = context.get_citation_engine()
            effective_claim = claim or f"Information from web source: {text[:100]}..."
            result = await engine.cite_web(
                claim=effective_claim,
                source_id=source_id,
                quote_context=text,
                locator=locator,
                verbatim_quote=_verbatim_or_none(text, extraction_method),
                confidence=confidence,
                extraction_method=extraction_method,
            )

            label = f"{title} — {url}" if title else url
            return _created_output(
                result,
                f"{format_source_ref(source_id)} {label} (accessed {accessed_date})",
            )

        except Exception as e:
            logger.error(f"Web citation error: {e}")
            return f"Error creating web citation: {str(e)}"

    @tool
    async def list_sources() -> str:
        """List citation sources registered by this job.

        Shows document, web, database, and custom sources that have been
        registered for citations in the current job. Sources from other jobs
        are not visible. Source ids are printed as S12; they are not citation
        handles and never go in your text.

        Returns:
            Formatted list of sources with IDs and types
        """
        try:
            engine = context.get_citation_engine()
            # list_sources() now filters by job_id from context automatically
            sources = await engine.list_sources()

            if not sources:
                return "No sources registered yet. Use cite_document or cite_web to register sources."

            lines = [f"Registered Sources ({len(sources)} total):", ""]
            for source in sources:
                lines.append(
                    f"  {format_source_ref(source.id)} {source.type.value}: {source.name}"
                )

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Error listing sources: {e}")
            return f"Error listing sources: {str(e)}"

    @tool
    async def get_citation(citation_id: CitationRef) -> str:
        """Get details about a specific citation from this job.

        Retrieves the full citation record including claim, source, verification
        status and, when it is not verified, why. Only citations belonging to
        the current job are accessible.

        Args:
            citation_id: The citation handle, e.g. [^c1241]

        Returns:
            Detailed citation information, or not found if citation doesn't exist
            or belongs to another job
        """
        try:
            cid = parse_citation_ref(citation_id)
            engine = context.get_citation_engine()
            # get_citation() now filters by job_id from context automatically
            citation = await engine.get_citation(cid)

            handle = format_handle(cid)
            if not citation:
                return f"Citation {handle} not found."

            source = await engine.get_source(citation.source_id)
            source_ref = format_source_ref(citation.source_id)
            if source is not None:
                source_line = f"{source_ref} {source.name} ({source.type.value})"
            else:
                source_line = f"{source_ref} (no longer available)"

            output = f"Citation {handle}\n"
            output += f"\nSource: {source_line}"

            if citation.locator:
                loc_parts = [
                    f"{k.capitalize()} {v}" for k, v in citation.locator.items() if v
                ]
                if loc_parts:
                    output += f"\nLocation: {', '.join(loc_parts)}"

            output += f"\nClaim: {citation.claim}"

            if citation.verbatim_quote:
                quote = (
                    citation.verbatim_quote[:500] + "..."
                    if len(citation.verbatim_quote) > 500
                    else citation.verbatim_quote
                )
                output += f'\n\nQuote: "{quote}"'

            if citation.quote_context:
                ctx = (
                    citation.quote_context[:500] + "..."
                    if len(citation.quote_context) > 500
                    else citation.quote_context
                )
                output += f"\nContext: {ctx}"

            if citation.quote_language:
                output += f"\nLanguage: {citation.quote_language}"

            if citation.extraction_method:
                output += f"\nExtraction: {citation.extraction_method.value}"

            if citation.relevance_reasoning:
                output += f"\nReasoning: {citation.relevance_reasoning}"

            status = _status_phrase(
                citation.verification_status.value, citation.withdrawn_at is not None
            )
            output += f"\n\nStatus: {status} | Confidence: {citation.confidence.value}"
            if citation.similarity_score is not None:
                output += f" | Similarity: {citation.similarity_score:.2f}"
            if citation.verification_reason:
                output += f"\nReason: {citation.verification_reason}"
            if citation.verification_notes:
                output += f"\nNotes: {citation.verification_notes}"
            if citation.withdrawn_at is not None:
                output += (
                    f"\nWithdrawn: {citation.withdrawn_reason or 'no reason given'}"
                )

            return output

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error getting citation: {e}")
            return f"Error getting citation: {str(e)}"

    @tool
    async def list_citations(
        source_id: Optional[SourceRef] = None,
        status: Optional[VerificationStatusValue] = None,
    ) -> str:
        """List citations created by this job.

        Shows citation handles, claims (truncated), verification status, and
        source. Optionally filter by source or verification status ('failed'
        is shown as UNVERIFIED, 'unverified' as not checked). Citations from
        other jobs are not visible.

        Args:
            source_id: Filter by source, e.g. S12 (optional)
            status: Filter by verification status: pending, verified, failed,
                unverified (optional)

        Returns:
            Formatted list of citations for the current job
        """
        try:
            engine = context.get_citation_engine()
            # list_citations() now filters by job_id from context automatically
            # Pass filters directly to engine for efficiency
            citations = await engine.list_citations(
                source_id=parse_source_ref(source_id)
                if source_id is not None
                else None,
                verification_status=status,
            )

            if not citations:
                if source_id is not None or status is not None:
                    return "No citations match the given filters."
                return "No citations created yet. Use cite_document or cite_web to create citations."

            lines = [f"Citations ({len(citations)} total):", ""]
            for c in citations:
                claim_preview = c.claim[:50] + "..." if len(c.claim) > 50 else c.claim
                confidence = f" ({c.confidence.value})" if c.confidence else ""
                status_str = _status_phrase(
                    c.verification_status.value, c.withdrawn_at is not None
                )
                lines.append(
                    f"  {format_handle(c.id)} {status_str}{confidence} — "
                    f'{format_source_ref(c.source_id)} "{claim_preview}"'
                )

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error listing citations: {e}")
            return f"Error listing citations: {str(e)}"

    @tool
    async def edit_citation(
        citation_id: CitationRef,
        claim: Optional[str] = None,
        verbatim_quote: Optional[str] = None,
        quote_context: Optional[str] = None,
        relevance_reasoning: Optional[str] = None,
        confidence: Optional[ConfidenceValue] = None,
        extraction_method: Optional[ExtractionMethodValue] = None,
        locator: Optional[str] = None,
    ) -> str:
        """Edit fields of an existing citation belonging to this job.

        Use it to fix a citation that failed verification: point it at a
        better passage, quote the source exactly, or correct the claim. When
        claim, verbatim_quote or quote_context change, the old verdict is
        cleared and the citation is checked again. The handle stays the same.

        Only citations belonging to the current job can be edited, and a
        withdrawn citation cannot be.

        Args:
            citation_id: The citation handle, e.g. [^c1241]
            claim: The assertion being supported
            verbatim_quote: Exact quote from source
            quote_context: Context around the quote
            relevance_reasoning: Why this citation is relevant
            confidence: Confidence level (high, medium, low)
            extraction_method: How extracted (direct_quote, paraphrase, inference, aggregation, negative)
            locator: Location reference as JSON string (e.g., '{"page": 5, "section": "3.2"}')

        Returns:
            "ok: edited citation [^c1241]" on success, "error: {reason}" on failure
        """
        try:
            cid = parse_citation_ref(citation_id)
            engine = context.get_citation_engine()

            # Build kwargs for edit, only including non-None values
            kwargs = {}
            if claim is not None:
                kwargs["claim"] = claim
            if verbatim_quote is not None:
                kwargs["verbatim_quote"] = verbatim_quote
            if quote_context is not None:
                kwargs["quote_context"] = quote_context
            if relevance_reasoning is not None:
                kwargs["relevance_reasoning"] = relevance_reasoning
            if confidence is not None:
                kwargs["confidence"] = confidence
            if extraction_method is not None:
                kwargs["extraction_method"] = extraction_method
            if locator is not None:
                try:
                    kwargs["locator"] = json.loads(locator)
                except json.JSONDecodeError:
                    return "error: locator must be valid JSON"

            if not kwargs:
                return "error: no fields provided to edit"

            content_changed = any(
                v is not None for v in [claim, verbatim_quote, quote_context]
            )

            # Route through the engine (vector store). Job-scoped ownership and
            # the new check on a content change are handled there.
            await engine.edit_citation(citation_id=cid, **kwargs)

            result = f"ok: edited citation {format_handle(cid)}"
            if content_changed:
                edited = await engine.get_citation(cid)
                status = edited.verification_status.value if edited else "pending"
                if status == "failed" and edited is not None:
                    result += f" — still UNVERIFIED: {edited.verification_notes}"
                else:
                    result += f" ({_status_phrase(status)})"
            return result

        except ValueError as e:
            return f"error: {str(e)}"
        except Exception as e:
            hint = _humanize_db_enum_error(e)
            if hint:
                return f"error: {hint}"
            logger.error(f"Error editing citation: {e}")
            return f"error: {str(e)}"

    @tool
    async def withdraw_citation(citation_id: CitationRef, reason: str) -> str:
        """Withdraw a citation you no longer use, keeping its record.

        For a citation that failed verification and cannot be fixed, or one
        you dropped from your text. Nothing is deleted: the record and its
        verdict stay, and readers see it struck through. Remove its handle from
        your text as well. To fix a citation instead, use edit_citation.

        Args:
            citation_id: The citation handle, e.g. [^c1241]
            reason: Why it is withdrawn (one sentence)

        Returns:
            "ok: withdrew citation [^c1241]" on success, "error: {reason}" on failure
        """
        try:
            cid = parse_citation_ref(citation_id)
            engine = context.get_citation_engine()
            await engine.withdraw_citation(cid, reason)
            handle = format_handle(cid)
            return f"ok: withdrew citation {handle}. Remove {handle} from your text."
        except ValueError as e:
            return f"error: {str(e)}"
        except Exception as e:
            logger.error(f"Error withdrawing citation: {e}")
            return f"error: {str(e)}"

    @tool
    async def annotate_source(
        source_id: SourceRef,
        content: str,
        type: AnnotationTypeValue = "note",
        page: Optional[str] = None,
    ) -> str:
        """Add a note, highlight, summary, question, or critique to a source.

        Build understanding of sources by annotating them with notes, highlights,
        summaries, questions, or critiques. Annotations are per-job.

        Args:
            source_id: The source, e.g. S12
            content: The annotation text
            type: Annotation type: note, highlight, summary, question, critique (default: note)
            page: Optional page/section reference (e.g., "p.12", "§ 3.1")

        Returns:
            Confirmation with annotation ID, or error message
        """
        try:
            sid = parse_source_ref(source_id)
            engine = context.get_citation_engine()
            annotation = await engine.annotate_source(
                source_id=sid,
                content=content,
                annotation_type=type or "note",
                page_reference=page,
            )

            return (
                f"Annotation {annotation.id} created\n"
                f"Type: {annotation.annotation_type.value}\n"
                f"Source: {format_source_ref(sid)}"
                + (f"\nPage: {page}" if page else "")
            )

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error creating annotation: {e}")
            return f"Error creating annotation: {str(e)}"

    @tool
    async def get_annotations(
        source_id: SourceRef,
        type: Optional[AnnotationTypeValue] = None,
    ) -> str:
        """Get annotations for a source in the current job.

        Retrieve all notes, highlights, summaries, questions, and critiques
        attached to a source. Optionally filter by annotation type.

        Args:
            source_id: The source, e.g. S12
            type: Optional filter: note, highlight, summary, question, critique

        Returns:
            Formatted list of annotations, or message if none found
        """
        try:
            sid = parse_source_ref(source_id)
            engine = context.get_citation_engine()
            annotations = await engine.get_annotations(
                source_id=sid,
                annotation_type=type,
            )

            source_ref = format_source_ref(sid)
            if not annotations:
                filter_msg = f" of type '{type}'" if type else ""
                return f"No annotations{filter_msg} found for source {source_ref}."

            lines = [
                f"Annotations for source {source_ref} ({len(annotations)} total):",
                "",
            ]
            for ann in annotations:
                preview = (
                    ann.content[:200] + "..." if len(ann.content) > 200 else ann.content
                )
                page_str = f" (p.{ann.page_reference})" if ann.page_reference else ""
                lines.append(
                    f"  {ann.id}. {ann.annotation_type.value}{page_str}: {preview}"
                )

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error getting annotations: {e}")
            return f"Error getting annotations: {str(e)}"

    @tool
    async def tag_source(
        source_id: SourceRef,
        tags: str,
        action: TagActionValue = "add",
    ) -> str:
        """Add or remove tags on a citation source.

        Tags help organize sources for later retrieval. Tags are per-job.
        Provide tags as a comma-separated string.

        Args:
            source_id: The source, e.g. S12
            tags: Comma-separated tag strings (e.g., "compliance, GoBD, retention")
            action: "add" (default) or "remove"

        Returns:
            Current list of tags for the source, or error message
        """
        try:
            sid = parse_source_ref(source_id)
            engine = context.get_citation_engine()
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]

            if not tag_list:
                return "Error: no tags provided"

            # Match both actions explicitly. An unrecognised value must never
            # fall through to "add" — that silently did the *opposite* of a
            # remove request and still reported "Added".
            if action == "remove":
                current_tags = await engine.remove_tags(source_id=sid, tags=tag_list)
                verb = "Removed"
            elif action == "add":
                current_tags = await engine.tag_source(source_id=sid, tags=tag_list)
                verb = "Added"
            else:
                return f"error: invalid action '{action}'. Use 'add' or 'remove'."

            return (
                f"{verb} tags on source {format_source_ref(sid)}\n"
                f"Current tags: {', '.join(current_tags) if current_tags else '(none)'}"
            )

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error tagging source: {e}")
            return f"Error tagging source: {str(e)}"

    @tool
    async def search_library(
        query: str,
        mode: SearchModeValue = "hybrid",
        tags: Optional[str] = None,
        source_type: Optional[SourceTypeValue] = None,
        scope: SearchScopeValue = "content",
        top_k: Optional[int] = 10,
    ) -> str:
        """Search the source library using keyword, semantic, or hybrid retrieval.

        Find evidence across all registered sources. Returns results with
        explainable evidence labels (HIGH/MEDIUM/LOW). Use this to find
        supporting evidence before creating citations. Results name their
        source as S12; a source id is not a citation, so cite the passage with
        cite_document or cite_web and write the handle that returns.

        Args:
            query: Natural language query or keywords to search for
            mode: Search mode: "hybrid" (default, recommended), "keyword", or "semantic"
            tags: Optional comma-separated tags to filter by (AND logic)
            source_type: Optional filter: "document", "website", "database", "custom"
            scope: What to search: "content" (default), "annotations", or "all"
            top_k: Maximum results to return (default: 10)

        Returns:
            Formatted search results with evidence labels and source references
        """
        try:
            engine = context.get_citation_engine()

            # Parse tags from comma-separated string
            tag_list = None
            if tags:
                tag_list = [t.strip() for t in tags.split(",") if t.strip()]

            results = await engine.search_library(
                query=query,
                mode=mode or "hybrid",
                tags=tag_list,
                source_type=source_type,
                scope=scope or "content",
                top_k=top_k or 10,
            )

            if not results.results:
                return f"No results found for: {query}\nMode: {results.mode}"

            lines = [
                f"Search Results ({len(results.results)} found)",
                f"Query: {query}",
                f"Mode: {results.mode}",
                f"Evidence: {results.overall_label}",
                "",
            ]

            for i, r in enumerate(results.results, 1):
                source_ref = f"{format_source_ref(r.source_id)} {r.source_name}"
                page_str = f", {r.page_reference}" if r.page_reference else ""
                preview = (
                    r.chunk_text[:300] + "..."
                    if len(r.chunk_text) > 300
                    else r.chunk_text
                )

                lines.append(f"  {i}. {r.evidence_label} ({r.evidence_reason})")
                lines.append(f"     Source: {source_ref}{page_str}")
                lines.append(f'     "{preview}"')
                lines.append("")

            return "\n".join(lines)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error searching library: {e}")
            return f"Error searching library: {str(e)}"

    @tool
    async def generate_bibliography(
        style: BibliographyStyleValue = "bibtex",
        citation_ids: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> str:
        """Generate a formatted bibliography/references section from citations.

        Produces a bibliography from all citations in the current job, or from
        a specific subset. Can write directly to a file (e.g., references.bib)
        or return the formatted text. When writing to an existing file, only
        new entries are appended — duplicates are skipped. A delivered Markdown
        document gets its footnote definitions appended automatically; use
        this for a separate references file.

        Args:
            style: Format style: "bibtex" (default), "harvard", "ieee", "apa", "inline"
            citation_ids: Comma-separated citation handles to include
                (e.g., "[^c1241], [^c1242]"). If omitted, includes all citations
                for the current job.
            output_path: Workspace-relative path to write (e.g., "references.bib").
                         If omitted, returns the formatted text directly.

        Returns:
            Formatted bibliography text, or write confirmation with counts
        """
        try:
            engine = context.get_citation_engine()
            effective_style = style or "bibtex"

            # Get citations — filtered subset or all
            if citation_ids:
                ids = []
                for part in citation_ids.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        ids.append(parse_citation_ref(part))
                    except ValueError:
                        return f"Error: invalid citation handle '{part}' — use e.g. [^c1241]"

                entries = []
                for cid in ids:
                    try:
                        entry = await engine.format_citation(cid, effective_style)
                        entries.append(entry)
                    except ValueError as e:
                        entries.append(
                            f"% Error for citation {format_handle(cid)}: {e}"
                        )
            else:
                # All citations for this job
                all_citations = await engine.list_citations(include_withdrawn=False)
                if not all_citations:
                    return "No citations found. Use cite_document or cite_web first."
                entries = []
                for c in all_citations:
                    try:
                        entry = await engine.format_citation(c.id, effective_style)
                        entries.append(entry)
                    except ValueError as e:
                        entries.append(
                            f"% Error for citation {format_handle(c.id)}: {e}"
                        )

            if not entries:
                return "No entries generated."

            bibliography = "\n\n".join(entries)

            # Return text directly if no output path
            if not output_path:
                header = (
                    f"Bibliography ({len(entries)} entries, style: {effective_style})\n"
                )
                return header + "\n" + bibliography

            # Write to file in workspace (uses backend abstraction for remote support)
            if workspace is None:
                return "Error: no workspace available for file output"

            new_count = len(entries)
            skipped = 0

            if workspace.exists(output_path):
                existing_content = workspace.read_file(output_path)

                if effective_style == "bibtex":
                    # Extract existing BibTeX keys to avoid duplicates
                    existing_keys = set(re.findall(r"@\w+\{(\w+),", existing_content))
                    new_entries = []
                    for entry in entries:
                        match = re.search(r"@\w+\{(\w+),", entry)
                        if match and match.group(1) in existing_keys:
                            skipped += 1
                        else:
                            new_entries.append(entry)
                    new_count = len(new_entries)

                    if new_entries:
                        append_text = "\n\n" + "\n\n".join(new_entries)
                        workspace.append_file(output_path, append_text)
                else:
                    # For non-bibtex styles, use exact string matching
                    existing_entries = set(existing_content.strip().split("\n\n"))
                    new_entries = []
                    for entry in entries:
                        if entry.strip() in existing_entries:
                            skipped += 1
                        else:
                            new_entries.append(entry)
                    new_count = len(new_entries)

                    if new_entries:
                        append_text = "\n\n" + "\n\n".join(new_entries)
                        workspace.append_file(output_path, append_text)

                return (
                    f"Updated {output_path}: {new_count} new entries appended, "
                    f"{skipped} duplicates skipped"
                )
            else:
                workspace.write_file(output_path, bibliography + "\n")
                return f"Written {output_path}: {len(entries)} entries ({effective_style} style)"

        except Exception as e:
            logger.error(f"Error generating bibliography: {e}")
            return f"Error generating bibliography: {str(e)}"

    return [
        cite_document,
        cite_web,
        list_sources,
        get_citation,
        list_citations,
        edit_citation,
        withdraw_citation,
        annotate_source,
        get_annotations,
        tag_source,
        search_library,
        generate_bibliography,
    ]
