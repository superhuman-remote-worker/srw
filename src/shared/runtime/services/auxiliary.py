"""AuxiliaryLLM — Unified support task system.

Provides two execution modes for background/support LLM tasks:

- **Chain mode** (`chain()`): Single LLM call with structured output.
  System prompt + context → Pydantic model. No tools, no loop.
  For tasks that just need reasoning over provided context.

- **Text mode** (`complete()`): Single LLM call returning the model's own
  text, for tasks whose output is prose the caller validates itself
  (conversation summarization writes Markdown sections, not JSON).

- **Agent mode** (`agent()`): Short-lived tool loop with structured output.
  The LLM can make tool calls (search KB, read files, write notes),
  then a final structured-output call produces the result.
  Capped iterations. Not a full job — no workspace, no todos, no phases.

Chain and agent tasks use `with_structured_output()` for reliable structured
returns.

See knowledge-base/knowledge/features/auxiliary.md for the full design document.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, Type

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from pydantic import BaseModel, Field, ValidationError

from shared.runtime.core.llm_retry import RetryPolicy, invoke_with_retry
from shared.runtime.llm.exceptions import ContextOverflowError
from shared.runtime.llm.structured_recovery import recover_structured

logger = logging.getLogger(__name__)


# =============================================================================
# Output schemas (Pydantic models for with_structured_output)
# =============================================================================


class AuxiliaryArchiver(Protocol):
    """Archive sink needed by support tasks, independent of agent lifecycle."""

    def archive(
        self,
        *,
        job_id: str,
        agent_type: str,
        messages: List[BaseMessage],
        response: AIMessage,
        model: str,
        latency_ms: int,
        call_type: str,
        auxiliary_metadata: dict,
    ) -> Optional[str]: ...

    def archive_error(
        self,
        *,
        job_id: str,
        agent_type: str,
        messages: List[BaseMessage],
        model: str,
        error: str,
        error_type: str,
        latency_ms: int,
        call_type: str,
        auxiliary_metadata: dict,
    ) -> Optional[str]: ...


class ExtractedMemory(BaseModel):
    """A single extracted memory from conversation."""

    content: str = Field(description="The insight (1-3 sentences, self-contained)")
    summary: str = Field(description="One-line summary (under 100 chars)")
    keywords: List[str] = Field(description="Relevant terms for search (3-8 keywords)")
    importance: float = Field(
        description="How useful is this for future work? (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )
    type: str = Field(
        description="One of: factual, procedural, error_solution, vocabulary, relational"
    )
    retrieval_messages: List[str] = Field(
        default_factory=list,
        description=(
            "3-5 synthetic trigger phrases representing situations where this "
            "memory should be retrieved. Phrased as what the agent would be "
            "saying or encountering when it needs this information."
        ),
    )


class ExtractedMemories(BaseModel):
    """Structured output for memory extraction."""

    memories: List[ExtractedMemory] = Field(
        description="List of extracted memories. Empty list if nothing noteworthy."
    )


class CurationResult(BaseModel):
    """Structured output for knowledge curation (agent mode)."""

    notes_created: int = Field(description="Number of new knowledge notes created")
    notes_updated: int = Field(description="Number of existing notes updated")
    summary: str = Field(description="Brief summary of what was curated")


class KnowledgeAssemblyResult(BaseModel):
    """Structured output for knowledge assembly / convergence (agent mode)."""

    notes_refreshed: int = Field(
        description="Stale notes confirmed still valid (kept as-is)"
    )
    notes_superseded: int = Field(
        description="Stale notes retired because a newer note replaces them"
    )
    notes_merged: int = Field(
        description="Duplicate/overlapping notes consolidated into one"
    )
    notes_archived: int = Field(
        description="Stale notes retired because they are no longer relevant"
    )
    summary: str = Field(description="Brief summary of what was converged")


class AssemblyAction(BaseModel):
    """A single TTL adjustment made by the assembler."""

    memory_id: str = Field(description="UUID of the memory acted on")
    action: str = Field(description="'boost' or 'deprecate'")
    turns: int = Field(description="Number of turns to adjust TTL by")
    reason: str = Field(description="Why this adjustment was made")


class AssemblyResult(BaseModel):
    """Structured output for memory assembly (agent mode)."""

    actions_taken: List[AssemblyAction] = Field(
        default_factory=list,
        description="List of TTL adjustments made. Empty if no changes needed.",
    )
    gaps_identified: List[str] = Field(
        default_factory=list,
        description="Missing knowledge areas where no relevant memory exists",
    )
    summary: str = Field(description="Brief summary of assembly review")


class IngestionVerdict(BaseModel):
    """Structured output for the ingestion verdict (overhaul Phase 4).

    The adjudicator compares a newly-extracted candidate memory against its
    nearest currently-valid neighbours and decides what to do with it.
    ``target_indices`` are 1-based positions in the numbered neighbour list
    shown to the model (NOT database ids — echoing UUIDs is error-prone); the
    store maps them back to rows.
    """

    action: str = Field(
        description=(
            "One of: ADD (genuinely new information — keep the candidate, "
            "touch nothing), NOOP (already captured by a neighbour — discard "
            "the candidate), UPDATE (the candidate is the SAME fact with a "
            "changed/corrected value — keep it and retire the stale "
            "neighbours in target_indices), MERGE (the candidate and "
            "neighbours are complementary parts of one fact — store "
            "merged_content and retire the neighbours in target_indices)."
        )
    )
    target_indices: List[int] = Field(
        default_factory=list,
        description=(
            "1-based indices of the neighbour memories this verdict acts on. "
            "Required for UPDATE/MERGE (the rows to retire) and NOOP (the row "
            "the candidate duplicates). Empty for ADD."
        ),
    )
    merged_content: Optional[str] = Field(
        default=None,
        description=(
            "For MERGE only: the single combined fact (1-3 self-contained "
            "sentences) that replaces the candidate and the retired neighbours."
        ),
    )
    reason: str = Field(description="One-line justification for the verdict")


class KnowledgeVerdict(BaseModel):
    """Structured output for the knowledge ingestion verdict (OKF KB slice 2 PR2).

    The KB analog of :class:`IngestionVerdict`. The adjudicator compares a
    curation candidate note against its nearest currently-active KB neighbours
    and decides what to do with it *before* any ``kb_write``/``kb_update`` —
    this is the gate that stops the F33/F38 curator noise (reconstructed
    deliverables, double-written proposals, immortal learning/retro notes).
    ``target_indices`` are 1-based positions in the numbered neighbour list
    shown to the model (NOT note ids — echoing slugs is error-prone); the
    curator maps them back to notes.
    """

    action: str = Field(
        description=(
            "One of: ADD (genuinely new knowledge — write the candidate, touch "
            "nothing), DISCARD (already captured by a neighbour, or not worth "
            "keeping — drop the candidate, write nothing), UPDATE (the candidate "
            "is the SAME note with a changed/corrected value — edit the single "
            "neighbour in target_indices in place, keeping its id), SUPERSEDE "
            "(the candidate replaces one or more stale neighbours — write the "
            "candidate as a new note and retire the neighbours in target_indices)."
        )
    )
    target_indices: List[int] = Field(
        default_factory=list,
        description=(
            "1-based indices of the neighbour notes this verdict acts on. "
            "Exactly one for UPDATE (the note to edit) and DISCARD (the note the "
            "candidate duplicates); one or more for SUPERSEDE (the notes to "
            "retire). Empty for ADD."
        ),
    )
    reason: str = Field(description="One-line justification for the verdict")


class CitationVerdict(BaseModel):
    """Structured verdict for citation verification (citation engine Phase 2).

    The verifier checks a citation's quote against its source content and
    decides whether the quote exists in the source AND supports the claim.
    Relocated from the citation engine's own LLM stack onto the auxiliary model.
    """

    verified: bool = Field(
        description=(
            "True only if the quoted text (or closely matching text) exists in "
            "the source AND supports the claim. False otherwise."
        )
    )
    similarity_score: float = Field(
        description="How closely the quote matches the source content (0.0–1.0).",
        ge=0.0,
        le=1.0,
    )
    matched_text: Optional[str] = Field(
        default=None,
        description="The actual text found in the source that matches the quote.",
    )
    reasoning: str = Field(description="One or two sentences explaining the verdict.")


# =============================================================================
# Task base classes
# =============================================================================


class AuxTask(ABC):
    """Base class for chain-mode tasks.

    Subclasses define the system prompt, context assembly, and output schema.
    The AuxiliaryLLM handles execution via with_structured_output().
    """

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """System prompt for the LLM call."""
        ...

    @abstractmethod
    def build_context(self) -> str:
        """Build the user message content from task inputs."""
        ...

    @property
    @abstractmethod
    def output_schema(self) -> Type[BaseModel]:
        """Pydantic model class for structured output."""
        ...


class AuxTextTask(ABC):
    """Base class for text-mode tasks: system prompt + context -> free text.

    The AuxiliaryLLM returns the raw ``AIMessage``; the caller parses and
    validates it (finish reason, required structure, repetition).
    """

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """System prompt for the LLM call."""
        ...

    @abstractmethod
    def build_context(self) -> str:
        """Build the user message content from task inputs."""
        ...


class AuxAgentTask(AuxTask):
    """Base class for agent-mode tasks (adds tool access).

    Agent tasks run a short tool loop, then a final structured-output call
    produces the result. The output_schema defines what the final call returns.
    """

    @abstractmethod
    def get_tools(self) -> list:
        """Return the list of LangChain tools available to this task."""
        ...


# =============================================================================
# Task implementations
# =============================================================================


class ExtractMemoriesTask(AuxTask):
    """Extract memories from a conversation segment.

    Chain mode task. Prompt loaded from config/prompts/ via the prompt matrix.
    """

    def __init__(self, messages: List[BaseMessage], prompt: str, phase: int = 0):
        self.messages = messages
        self._prompt = prompt
        self.phase = phase

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        return _format_messages_for_extraction(self.messages)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return ExtractedMemories


class TextExtractMemoriesTask(AuxTask):
    """Extract memories from a *pre-formatted* conversation chunk.

    Text-input twin of :class:`ExtractMemoriesTask` (mirrors how
    :class:`SummarizeTask` takes ``conversation_text`` rather than raw
    messages). ``build_context()`` passes the already chunk-planned text
    straight through, so ``MemoryExtractionEngine`` feeds planner chunks
    directly — no 40-message cap, no re-formatting. Same prompt and
    ``ExtractedMemories`` schema as the message-input task.
    """

    def __init__(self, conversation_text: str, prompt: str):
        self.conversation_text = conversation_text
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        return self.conversation_text

    @property
    def output_schema(self) -> Type[BaseModel]:
        return ExtractedMemories


class IngestionVerdictTask(AuxTask):
    """Adjudicate a candidate memory against its nearest neighbours.

    Chain-mode task (overhaul Phase 4). Prompt loaded from config/prompts/.
    ``neighbours`` is the ordered list shown to the model — each item a dict
    with ``content`` and an optional ``age`` / ``similarity`` annotation; the
    1-based display index is the position in this list.
    """

    def __init__(
        self,
        candidate_content: str,
        neighbours: List[Dict[str, Any]],
        prompt: str,
    ):
        self.candidate_content = candidate_content
        self.neighbours = neighbours
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        lines = ["NEW candidate memory:", self.candidate_content, ""]
        lines.append("Existing similar memories (currently valid):")
        for i, n in enumerate(self.neighbours, start=1):
            meta_parts = []
            if n.get("similarity") is not None:
                meta_parts.append(f"similarity {float(n['similarity']):.2f}")
            if n.get("age"):
                meta_parts.append(str(n["age"]))
            meta = f" ({', '.join(meta_parts)})" if meta_parts else ""
            lines.append(f"[{i}]{meta} {n.get('content', '')}")
        lines.append("")
        lines.append(
            "Decide ADD / UPDATE / MERGE / NOOP. For UPDATE/MERGE/NOOP put the "
            "relevant [n] numbers in target_indices."
        )
        return "\n".join(lines)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return IngestionVerdict


class KnowledgeVerdictTask(AuxTask):
    """Adjudicate a curation candidate note against its nearest KB neighbours.

    Chain-mode task (OKF KB slice 2 PR2) — one structured-output adjudication
    like :class:`IngestionVerdictTask`, NOT another agent loop. ``neighbours``
    is the ordered list shown to the model — each item a dict with ``content``
    and optional ``similarity`` / ``title`` / ``age`` annotations; the 1-based
    display index is the position in this list.

    Unlike the memory verdict (whose prompt is loaded once at attach time), the
    ``prompt`` is passed in at **event time** — the curator resolves it from the
    prompt matrix so ``config.update`` is honoured in persistent sessions.
    """

    def __init__(
        self,
        candidate_content: str,
        neighbours: List[Dict[str, Any]],
        prompt: str,
    ):
        self.candidate_content = candidate_content
        self.neighbours = neighbours
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        lines = ["NEW candidate knowledge note:", self.candidate_content, ""]
        lines.append("Existing similar notes (currently active):")
        for i, n in enumerate(self.neighbours, start=1):
            meta_parts = []
            if n.get("similarity") is not None:
                meta_parts.append(f"similarity {float(n['similarity']):.2f}")
            if n.get("title"):
                meta_parts.append(str(n["title"]))
            if n.get("age"):
                meta_parts.append(str(n["age"]))
            meta = f" ({', '.join(meta_parts)})" if meta_parts else ""
            lines.append(f"[{i}]{meta} {n.get('content', '')}")
        lines.append("")
        lines.append(
            "Decide ADD / UPDATE / SUPERSEDE / DISCARD. For UPDATE/SUPERSEDE/"
            "DISCARD put the relevant [n] numbers in target_indices."
        )
        return "\n".join(lines)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return KnowledgeVerdict


class VerifyCitationTask(AuxTask):
    """Verify a citation's quote against its source content.

    Chain-mode task (citation engine Phase 2). Replaces the engine's own
    synchronous verification LLM call. ``source_content`` is the relevant
    portion of the source (page-scoped by the engine when a page locator
    exists). Prompt loaded from config/prompts/ via the prompt matrix.
    """

    #: Cap the source excerpt so a large document can't overflow the aux window.
    MAX_SOURCE_CHARS = 50000

    def __init__(
        self,
        claim: str,
        quote_context: str,
        verbatim_quote: Optional[str],
        source_content: str,
        prompt: str,
    ):
        self.claim = claim
        self.quote_context = quote_context
        self.verbatim_quote = verbatim_quote
        self.source_content = source_content
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        src = self.source_content or ""
        if len(src) > self.MAX_SOURCE_CHARS:
            dropped = len(src) - self.MAX_SOURCE_CHARS
            src = (
                src[: self.MAX_SOURCE_CHARS]
                + f"\n\n[... truncated {dropped} chars ...]"
            )

        parts = [
            "## Claim",
            self.claim,
            "",
            "## Quoted Context",
            self.quote_context,
        ]
        if self.verbatim_quote:
            parts += ["", "## Verbatim Quote", self.verbatim_quote]
        parts += [
            "",
            "## Source Content",
            src,
            "",
            "Verify that the quoted text exists in the source and supports the claim.",
        ]
        return "\n".join(parts)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return CitationVerdict


#: Merge contract for a fold pass that carries a prior summary. Adapted from
#: OpenCode's ``SUMMARY_UPDATE_INSTRUCTIONS`` (sst/opencode @ 7945de20,
#: packages/core/src/session/compaction.ts, MIT): every harness that merges
#: says the prior summary is discarded after the call, so the model must carry
#: forward what still matters (compaction refactor, finding F1).
SUMMARY_MERGE_INSTRUCTIONS = """\
The <prior-summary> covers everything that happened before the <conversation>. \
Write one new checkpoint that combines both. The <prior-summary> is discarded \
after this: anything you do not carry into the new checkpoint is lost.
- Carry forward objectives, user requests, constraints, decisions and open work \
from the <prior-summary>, even when the <conversation> does not mention them. \
Drop only what is finished and no longer needed.
- The <conversation> is more recent. Where the two conflict, the conversation \
wins: state the corrected fact and drop the old claim.
- Move work that has since finished from Active to Completed, and mark resolved \
blockers as resolved.
- Keep every quote under User Requests (verbatim) unless the user withdrew it."""


class SummarizeTask(AuxTextTask):
    """Write one fold pass of a conversation checkpoint (text mode).

    Invoked per fold pass by ``agent.core.summarizer.SummarizationEngine``. The
    system prompt (``config/prompts/summarization_prompt*.txt``, resolved per
    summarizer family) holds the role, the rules and the section template.
    The user message puts the transcript first and the instruction last
    (finding F12): conversation, then the prior summary with the merge
    contract, then the optional user focus, then the closing instruction that
    repeats the no-continuation rule.
    """

    def __init__(
        self,
        conversation_text: str,
        summarization_prompt: str,
        max_summary_length: int = 10000,
        *,
        prior_summary: Optional[str] = None,
        focus: Optional[str] = None,
    ):
        self.conversation_text = conversation_text
        self._summarization_prompt = summarization_prompt
        self.max_summary_length = max_summary_length
        self.prior_summary = prior_summary
        self.focus = focus

    @property
    def max_summary_tokens(self) -> int:
        """The length asked of the model. ``max_summary_length`` is characters."""
        return max(250, self.max_summary_length // 4)

    @property
    def system_prompt(self) -> str:
        # Bundled prompts carry no placeholders. A DB-authored or older prompt
        # may still hold {conversation} / {max_summary_length}: render them so
        # the text stays valid (the conversation always travels in
        # build_context()).
        from collections import defaultdict

        rendered = self._summarization_prompt.format_map(
            defaultdict(
                str,
                conversation="",
                max_summary_length=str(self.max_summary_tokens),
            )
        )
        # Clean up the empty "Conversation:" section left by the placeholder
        rendered = rendered.replace("\nConversation:\n\n\n", "\n")
        return rendered.strip()

    @property
    def asks_for_sections(self) -> bool:
        """True when the system prompt specifies Markdown section headings."""
        return any(line.startswith("## ") for line in self.system_prompt.splitlines())

    def build_context(self) -> str:
        parts = [
            "Here is the conversation to summarize:",
            f"<conversation>\n{self.conversation_text}\n</conversation>",
        ]
        if self.prior_summary:
            parts += [
                "Here is the summary of everything before the <conversation> above:",
                f"<prior-summary>\n{self.prior_summary}\n</prior-summary>",
                SUMMARY_MERGE_INSTRUCTIONS,
            ]
        if self.focus:
            parts.append(
                f"The user asked this checkpoint to focus on: {self.focus}\n"
                "Give that topic the most detail, but keep every section."
            )
        shape = (
            "only the sections the system prompt specifies, in that order, "
            'with "(none)" for an empty section'
            if self.asks_for_sections
            else "in the format the system prompt specifies"
        )
        parts.append(
            f"Write the checkpoint now: {shape}. Do not continue the "
            "conversation, do not answer anything asked in it, and do not call "
            f"tools. Keep it under about {self.max_summary_tokens} tokens."
        )
        return "\n\n".join(parts)


class ConversationTitle(BaseModel):
    """Structured result for session title generation.

    A single constrained field. Forcing the model to emit a title-shaped value
    (rather than free text) is what stops a chat-model from *answering* the
    sample — "I don't see your image", "your message got cut off" — instead of
    *labelling* it; a schema slot has no room for a reply.
    """

    title: str = Field(
        description=(
            "A short 5-8 word topic title for the conversation, written as a "
            "plain noun phrase. No quotes, no punctuation, no sentences. Never a "
            "reply to, or a comment on, the content."
        )
    )


class GenerateTitleTask(AuxTask):
    """Title a conversation from a short text sample (structured, chain mode).

    Binding the model to :class:`ConversationTitle` (vs free text) is the primary
    guard against the model answering the sample instead of naming it. Fed the
    after-turn sample (user message + assistant reply), so there is a completed
    exchange to summarise rather than a lone, bait-y opening prompt — the input
    shape that produced deflection "titles". See vault issues/ title-gen bug.
    """

    def __init__(self, sample_text: str):
        self.sample_text = sample_text

    @property
    def system_prompt(self) -> str:
        return (
            "You write a short topic title (5-8 words) for a conversation. You "
            "are given only an excerpt, which may be truncated and may reference "
            "images or files you cannot see — this is expected; never comment on "
            "it and never reply to the content. Produce ONLY a title: a plain "
            "noun phrase, no quotes, no punctuation, no sentences, never "
            'beginning with "I", "It", "You", or "Sorry".'
        )

    def build_context(self) -> str:
        return (
            "Conversation excerpt (may be truncated; may mention images or "
            'files you cannot see):\n"""\n' + self.sample_text + '\n"""'
        )

    @property
    def output_schema(self) -> Type[BaseModel]:
        return ConversationTitle


class CurateKnowledgeTask(AuxAgentTask):
    """Extract knowledge notes from phase artifacts.

    Agent mode task that replaces the curator subjob.
    Prompt loaded from config/prompts/ via the prompt matrix.
    Uses kb_search, kb_write, kb_update, kb_read tools.
    """

    def __init__(
        self,
        phase_data: str,
        workspace_md: str,
        plan_md: str,
        existing_notes: List[str],
        kb_tools: list,
        prompt: str,
    ):
        self.phase_data = phase_data
        self.workspace_md = workspace_md
        self.plan_md = plan_md
        self.existing_notes = existing_notes
        self._kb_tools = kb_tools
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        parts = [
            "## Phase Artifacts",
            self.phase_data,
            "",
            "## Current Workspace",
            self.workspace_md,
            "",
            "## Current Plan",
            self.plan_md,
        ]
        if self.existing_notes:
            parts.extend(
                [
                    "",
                    "## Existing Knowledge (check before writing duplicates)",
                    "\n".join(self.existing_notes),
                ]
            )
        return "\n".join(parts)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return CurationResult

    def get_tools(self) -> list:
        return self._kb_tools


class AssembleKnowledgeTask(AuxAgentTask):
    """Re-verify stale knowledge notes and converge the KB.

    Agent-mode counterpart to CurateKnowledgeTask (which only *populates*). Given
    the stale queue — notes whose cycle TTL ran out — decide per note: keep it
    (still valid), supersede it (a newer note replaces it), merge it (duplicate
    of another), or archive it (no longer relevant). Convergence is applied via
    kb_update (status=superseded/archived, or merge-then-supersede); the runner
    resets the TTL of the survivors afterwards. Uses kb_search/kb_read/kb_update.

    See knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md.
    """

    def __init__(
        self,
        stale_notes: List[str],
        related_notes: List[str],
        kb_tools: list,
        prompt: str,
    ):
        self.stale_notes = stale_notes
        self.related_notes = related_notes
        self._kb_tools = kb_tools
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        parts = [
            "## Stale notes to re-verify",
            "Each note below has a TTL that ran out. For EACH, take exactly one "
            "action with kb_update: keep it (still accurate — do nothing), "
            "supersede it (a newer note replaces it → status=superseded), merge "
            "it (duplicate/overlap → fold into one note, supersede the rest), or "
            "archive it (no longer relevant → status=archived). Do NOT touch "
            "notes that are not listed here.",
            "",
            "\n".join(self.stale_notes) if self.stale_notes else "(none)",
        ]
        if self.related_notes:
            parts.extend(
                [
                    "",
                    "## Other active notes (context for dedup / supersede decisions)",
                    "\n".join(self.related_notes),
                ]
            )
        return "\n".join(parts)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return KnowledgeAssemblyResult

    def get_tools(self) -> list:
        return self._kb_tools


class AssembleMemoriesTask(AuxAgentTask):
    """Review recent conversation and curate memory TTLs.

    Agent mode task (counterpart to ExtractMemoriesTask). Searches
    the memory DB for relevant missing memories and adjusts TTLs:
    boost relevant ones, deprecate stale ones.

    Prompt loaded from config/prompts/ via the prompt matrix.
    Uses memory_search, memory_boost, memory_deprecate tools.
    """

    def __init__(
        self,
        recent_context: str,
        current_injection: str,
        assembler_tools: list,
        prompt: str,
    ):
        self.recent_context = recent_context
        self.current_injection = current_injection
        self._tools = assembler_tools
        self._prompt = prompt

    @property
    def system_prompt(self) -> str:
        return self._prompt

    def build_context(self) -> str:
        parts = [
            "## Recent Conversation Context",
            self.recent_context,
            "",
            "## Currently Injected Memories",
            self.current_injection if self.current_injection else "(none)",
            "",
            "Review whether the right memories are being surfaced. "
            "Search for missing relevant memories and boost them. "
            "Deprecate pinned memories that are no longer relevant to the current work.",
        ]
        return "\n".join(parts)

    @property
    def output_schema(self) -> Type[BaseModel]:
        return AssemblyResult

    def get_tools(self) -> list:
        return self._tools


# =============================================================================
# AuxiliaryLLM — the unified executor
# =============================================================================

# Maps task class names to call_type values for archiving
_TASK_CALL_TYPES = {
    "SummarizeTask": "summarization",
    "ExtractMemoriesTask": "memory_extraction",
    "AssembleMemoriesTask": "memory_assembly",
    "CurateKnowledgeTask": "knowledge_curation",
    "AssembleKnowledgeTask": "knowledge_assembly",
    "VerifyCitationTask": "citation_verification",
}


def _get_model_name(llm: BaseChatModel) -> str:
    """Extract model name from a LangChain chat model."""
    for attr in ("model_name", "model"):
        if hasattr(llm, attr):
            return getattr(llm, attr)
    return "unknown"


@dataclass
class _TaskHealth:
    """Outcome counters for one auxiliary task family (e.g. memory_extraction)."""

    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    last_error_type: Optional[str] = None
    last_failure_at: Optional[float] = None  # epoch seconds
    last_success_at: Optional[float] = None


class AuxHealth:
    """Tracks auxiliary-task call outcomes so silent failures become visible.

    Auxiliary tasks (memory extraction/curation/assembly, session title
    generation) are deliberately *non-fatal*: their callers swallow exceptions
    so a degraded auxiliary model never fails a job or a chat turn. The cost is
    invisibility — on 2026-06-03 the auxiliary backend was unreachable for ~3
    days and produced no user- or operator-facing signal (no memories, no
    titles), because every failure logged at WARNING and was dropped.

    This tracker turns *sustained* failure into a single, alertable ERROR and
    exposes a snapshot via the agent status endpoint. It never changes control
    flow — callers still swallow and continue — and is itself best-effort.

    See knowledge-base/knowledge/issues/surface_silent_aux_failures.md.
    """

    #: Consecutive failures (across any task) before the degraded ERROR fires.
    ESCALATE_AFTER = 3
    #: While degraded, re-emit the ERROR every Nth further failure so the alert
    #: stays live without flooding the log.
    REPEAT_EVERY = 20

    def __init__(self, model: str = "unknown") -> None:
        self.model = model
        self._tasks: Dict[str, _TaskHealth] = {}
        self._consecutive_failures = 0
        self._degraded = False
        #: Reachability of the *dedicated* aux model, tracked by the fallback
        #: wrapper independently of the caller-driven per-task health. A
        #: fallback call succeeds (so the caller records success and clears
        #: ``_degraded``), yet the aux model itself is still down — this flag
        #: stays False so the heartbeat keeps ``aux_degraded`` lit until an
        #: actual aux-model call succeeds.
        self._aux_reachable = True
        self._last_fallback_task: Optional[str] = None
        self._last_fallback_error: Optional[str] = None

    def _task(self, task: str) -> _TaskHealth:
        t = self._tasks.get(task)
        if t is None:
            t = _TaskHealth()
            self._tasks[task] = t
        return t

    def record_success(self, task: str) -> None:
        """Record a successful auxiliary call; clears any degraded state."""
        t = self._task(task)
        t.successes += 1
        t.consecutive_failures = 0
        t.last_success_at = time.time()
        self._consecutive_failures = 0
        if self._degraded:
            self._degraded = False
            logger.error(
                "AUXILIARY MODEL RECOVERED: model=%s — task '%s' succeeded; "
                "memory/curation/titles resume.",
                self.model,
                task,
            )

    def record_failure(self, task: str, exc: BaseException) -> None:
        """Record a failed auxiliary call; escalates once it becomes sustained."""
        t = self._task(task)
        t.failures += 1
        t.consecutive_failures += 1
        t.last_error = str(exc)[:300]
        t.last_error_type = type(exc).__name__
        t.last_failure_at = time.time()
        self._consecutive_failures += 1

        n = self._consecutive_failures
        should_log = False
        if not self._degraded and n >= self.ESCALATE_AFTER:
            self._degraded = True
            should_log = True
        elif self._degraded and (n - self.ESCALATE_AFTER) % self.REPEAT_EVERY == 0:
            should_log = True
        if should_log:
            logger.error(
                "AUXILIARY MODEL DEGRADED: model=%s — %d consecutive auxiliary "
                "failures (latest task '%s': %s: %s). Memory extraction, "
                "knowledge curation and session titles are silently disabled "
                "until the auxiliary model is reachable again.",
                self.model,
                n,
                task,
                t.last_error_type,
                t.last_error,
            )

    def mark_aux_unreachable(self, task: str, exc: BaseException) -> None:
        """The dedicated aux model failed a call; the caller is falling back to
        the main model. Tracks aux-model reachability separately from the
        per-task success/failure counters so a fallback *success* can't mask
        that the aux model is down. Logs loud on the reachable→unreachable edge
        (the per-call LOUD log lives in ``_ainvoke_fallback``)."""
        self._last_fallback_task = task
        self._last_fallback_error = f"{type(exc).__name__}: {str(exc)[:200]}"
        if self._aux_reachable:
            self._aux_reachable = False
            logger.error(
                "AUXILIARY MODEL UNREACHABLE: model=%s — running background + "
                "compaction tasks on the main-model fallback until it recovers "
                "(latest failing task '%s': %s).",
                self.model,
                task,
                self._last_fallback_error,
            )

    def mark_aux_reachable(self) -> None:
        """A call on the dedicated aux model succeeded — clear fallback state.
        Logs loud only on the unreachable→reachable edge."""
        if not self._aux_reachable:
            self._aux_reachable = True
            self._last_fallback_task = None
            self._last_fallback_error = None
            logger.error(
                "AUXILIARY MODEL REACHABLE AGAIN: model=%s — resuming aux tasks "
                "on the dedicated model (was on main-model fallback).",
                self.model,
            )

    @property
    def aux_reachable(self) -> bool:
        return self._aux_reachable

    @property
    def degraded(self) -> bool:
        return self._degraded or not self._aux_reachable

    def snapshot(self) -> Dict[str, Any]:
        """JSON-serializable health summary for status endpoints."""
        return {
            "model": self.model,
            "degraded": self.degraded,
            "on_fallback": not self._aux_reachable,
            "last_fallback_error": self._last_fallback_error,
            "consecutive_failures": self._consecutive_failures,
            "tasks": {
                name: {
                    "successes": t.successes,
                    "failures": t.failures,
                    "consecutive_failures": t.consecutive_failures,
                    "last_error_type": t.last_error_type,
                    "last_error": t.last_error,
                    "last_failure_at": t.last_failure_at,
                    "last_success_at": t.last_success_at,
                }
                for name, t in self._tasks.items()
            },
        }

    def heartbeat_summary(self) -> Dict[str, Any]:
        """Compact health projection carried on the agent → orchestrator heartbeat.

        Smaller than :meth:`snapshot`: just the degraded flag, the aggregate
        failure count, the model, and which tasks are currently failing (with
        their last error type). The orchestrator persists ``degraded`` to
        ``agents.aux_degraded`` (drives the admin badge) and stashes the rest in
        ``agents.metadata.aux`` for the badge tooltip. Per-task success counters
        and timestamps stay in :meth:`snapshot` / the ``/status`` endpoint.

        Always includes ``degraded`` (even when False) so a recovered agent's
        heartbeat clears the persisted flag.
        """
        failing = {
            name: {
                "consecutive_failures": t.consecutive_failures,
                "last_error_type": t.last_error_type,
            }
            for name, t in self._tasks.items()
            if t.consecutive_failures > 0
        }
        return {
            "degraded": self.degraded,
            "on_fallback": not self._aux_reachable,
            "last_fallback_error": self._last_fallback_error,
            "consecutive_failures": self._consecutive_failures,
            "model": self.model,
            "failing_tasks": failing,
        }


class AuxInputTooLarge(Exception):
    """Task input exceeds the auxiliary model's context window.

    Typed and deterministic — callers must NOT retry (the input doesn't
    shrink). Summarization avoids this via the SummarizationEngine's
    chunk planning; other aux tasks (memory extraction, titles) fail fast
    here instead of overflowing at the HTTP transport.
    See knowledge-base/knowledge/features/context_summarization_rework.md (S1).
    """

    def __init__(self, tokens: int, limit: int, task_name: str = "unknown"):
        self.tokens = tokens
        self.limit = limit
        self.task_name = task_name
        super().__init__(
            f"Auxiliary task {task_name} input is ~{tokens:,} tokens, exceeding "
            f"the auxiliary model's {limit:,}-token context window"
        )


# Aux calls are background/compaction work behind a per-call timeout, so the
# retry budget is deliberately tight: one extra attempt, ~1 s apart. That clears
# the common transient (a dropped stream fails fast, it does not sit out the
# timeout) without materially widening worst-case latency on the compaction
# path, which is the only aux caller a user waits on.
#
# Three deliberate exclusions, all cases where a second identical attempt is not
# merely useless but actively wasteful:
#   * ContextOverflowError / AuxInputTooLarge — deterministic by construction.
#     A payload that exceeds the window exceeds it every time; AuxInputTooLarge's
#     own docstring says callers must NOT retry. These reach the classifier as
#     internal typed exceptions, not provider errors, so its catch-all would
#     otherwise call them `transient` and burn a duplicate oversized request.
#   * asyncio.TimeoutError — a hung aux model should ESCALATE, not burn a second
#     full timeout. Falling back answers immediately; retrying does not.
#   * rate_limit (via `retryable`) — a throttled aux model should escalate too.
#     Sitting out a provider's 90 s Retry-After is strictly worse than asking the
#     main model, which is exactly the choice the fallback exists to make.
class AuxiliaryProviderAdmissionClosed(RuntimeError):
    """A lifecycle fence refused a not-yet-started auxiliary provider call."""


_AUX_RETRY = RetryPolicy(
    max_attempts=2,
    base_delay=1.0,
    max_delay=5.0,
    retryable=frozenset({"transient", "auth_unavailable"}),
    never_retry=(
        asyncio.TimeoutError,
        ContextOverflowError,
        AuxInputTooLarge,
        AuxiliaryProviderAdmissionClosed,
    ),
    respect_retry_after=False,
)

# The escalated call has nowhere left to go, so give it the same one retry
# rather than letting a blip on the main model raise straight out.
_AUX_FALLBACK_RETRY = _AUX_RETRY


class AuxiliaryLLM:
    """Unified support task execution with chain and agent modes.

    All tasks use with_structured_output() for reliable structured returns.

    Args:
        llm: The support model (e.g. gpt-oss-120b, or main LLM as fallback)
        max_context_tokens: The support model's own context window, resolved
            from its settings at construction. Budgeting authority for the
            SummarizationEngine and the chain() pre-flight guard. None
            disables the guard (window unknown).
        summarization_llm: The same support model at the summarization
            reasoning level (``auxiliary.summarization_reasoning_level``).
            Serves :class:`SummarizeTask`; None = ``llm`` serves it too.
    """

    def __init__(
        self,
        llm: BaseChatModel,
        *,
        structured_output_method: str = "json_schema",
        fallback_structured_output_method: Optional[str] = None,
        max_iterations: int = 15,
        timeout: float = 120.0,
        archiver: Optional["AuxiliaryArchiver"] = None,
        job_id: Optional[str] = None,
        agent_type: Optional[str] = None,
        max_context_tokens: Optional[int] = None,
        fallback_llm: Optional[BaseChatModel] = None,
        summarization_llm: Optional[BaseChatModel] = None,
    ):
        self.llm = llm
        self.summarization_llm = summarization_llm
        self.structured_output_method = structured_output_method or "json_schema"
        self.fallback_structured_output_method = (
            fallback_structured_output_method or self.structured_output_method
        )
        #: Drop-in fallback for a dead/unreachable *dedicated* aux model —
        #: normally the main session/worker model, which is always present and
        #: working (it serves the turns). When the aux model fails a call, the
        #: task retries here so the session keeps running instead of crashing on
        #: the next compaction. None when the aux model already IS the main model
        #: (no separate fallback to fall back to). See
        #: knowledge-base/knowledge/issues/openrouter_auxiliary_misrouted_to_openai.md.
        primary_name = _get_model_name(llm)
        fallback_name = (
            _get_model_name(fallback_llm) if fallback_llm is not None else None
        )
        self.fallback_llm = (
            fallback_llm if fallback_name and fallback_name != primary_name else None
        )
        self._fallback_model_name = fallback_name if self.fallback_llm else None
        self.max_iterations = max_iterations
        self.timeout = timeout
        self.max_context_tokens = max_context_tokens
        self._archiver = archiver
        self._job_id = job_id
        self._agent_type = agent_type or "unknown"
        self._provider_admission_gate: Optional[Callable[[], bool]] = None
        self._provider_calls_inflight = 0
        #: Observability for silent non-fatal failures (see AuxHealth).
        self.health = AuxHealth(model=_get_model_name(llm))

    def set_provider_admission_gate(self, gate: Optional[Callable[[], bool]]) -> None:
        """Install the process-local lifecycle gate on every aux call shape."""

        self._provider_admission_gate = gate

    @property
    def provider_calls_inflight(self) -> int:
        return self._provider_calls_inflight

    async def _invoke_provider(self, runnable: Any, invoke_arg: Any, timeout: float):
        gate = self._provider_admission_gate
        if gate is not None and not gate():
            raise AuxiliaryProviderAdmissionClosed(
                "auxiliary provider admission is closed"
            )
        self._provider_calls_inflight += 1
        try:
            return await asyncio.wait_for(runnable.ainvoke(invoke_arg), timeout=timeout)
        finally:
            self._provider_calls_inflight -= 1

    def set_job_context(
        self,
        archiver: Optional["AuxiliaryArchiver"],
        job_id: str,
        agent_type: str,
    ) -> None:
        """Set archiver and job context for logging. Called at job start."""
        self._archiver = archiver
        self._job_id = job_id
        self._agent_type = agent_type

    async def _ainvoke_fallback(
        self,
        build_runnable,
        invoke_arg,
        *,
        task_name: str,
        timeout: Optional[float] = None,
        structured_schema: Optional[Type[BaseModel]] = None,
        method: Optional[str] = None,
        fallback_method: Optional[str] = None,
        retry_policy: Optional[RetryPolicy] = None,
        primary_llm: Optional[BaseChatModel] = None,
    ):
        """Invoke ``build_runnable(llm).ainvoke(invoke_arg)`` on the dedicated aux
        model; on failure, retry the aux model, then fall back to the main model.

        ``primary_llm`` replaces ``self.llm`` as the aux client for this call
        (the summarization client); the fallback is unchanged.

        ``build_runnable`` maps an LLM to the runnable to invoke (identity for a
        raw ``ainvoke``, ``.with_structured_output(...)`` for chain mode,
        ``.bind_tools(...)`` for a single agent turn) — so the exact same
        fallback wraps every aux call shape.

        Retry and fallback are DIFFERENT AXES and compose in that order. Before
        :class:`RetryPolicy` existed, this method had no retry at all, so the
        fallback was doing retry's job: a single transient blip on the cheap aux
        model instantly rerouted memory extraction, title generation and
        compaction onto the expensive main model, and lit the heartbeat
        ``aux_degraded`` flag while doing it. Now a transient failure gets a
        second attempt on the aux model first, and only a genuinely stuck or
        permanently-broken aux model escalates.

        Semantics (the calibrated "fail loud" contract):
          - Aux model succeeds → normal return; aux marked reachable.
          - Aux model fails transiently → bounded retry on the aux model
            (``_AUX_RETRY``); a success on attempt 2 is a normal return and
            never touches the fallback or the health flag.
          - Aux retries exhausted, or a permanent/timeout failure + a fallback
            exists → LOUD error, escalate to the main model, return its result.
            Never silent: ``mark_aux_unreachable`` lights the heartbeat
            ``aux_degraded`` flag — and now only fires once the aux model is
            genuinely unreachable rather than momentarily rude.
          - Aux fails + no fallback (aux IS the main model), OR the fallback
            ALSO fails (itself retried under ``_AUX_FALLBACK_RETRY``) → raise.
            The caller then fails the turn/session (compaction →
            ``SummarizationFailed('aux_unavailable')``) rather than limping
            with half a context.
        """
        _timeout = timeout if timeout is not None else self.timeout
        _policy = retry_policy if retry_policy is not None else _AUX_RETRY
        primary = primary_llm if primary_llm is not None else self.llm
        method = method or self.structured_output_method
        fallback_method = fallback_method or self.fallback_structured_output_method
        try:
            # The per-attempt wait_for lives INSIDE the retried callable so each
            # attempt gets its own timeout budget rather than sharing a spent one.
            result = await invoke_with_retry(
                lambda: self._invoke_provider(
                    build_runnable(primary, method), invoke_arg, _timeout
                ),
                policy=_policy,
                description=f"aux '{task_name}' on model '{self.health.model}'",
            )
            self.health.mark_aux_reachable()
            if structured_schema is not None:
                try:
                    return self._recover_structured_output(
                        result, structured_schema, task_name
                    )
                except Exception:
                    parsed = await self._recover_via_raw_invoke(
                        primary,
                        invoke_arg,
                        structured_schema,
                        timeout=_timeout,
                    )
                    if parsed is not None:
                        self.health.mark_aux_reachable()
                        return parsed
            else:
                return result
            # parsed shape/validation failed and raw recovery did not recover
            raise ValueError(
                f"Structured-output validation failed for {task_name} "
                "after raw fallback recovery"
            )
        except AuxiliaryProviderAdmissionClosed:
            raise
        except ValidationError as primary_exc:
            parsed = await self._recover_via_raw_invoke(
                primary,
                invoke_arg,
                structured_schema,
                timeout=_timeout,
            )
            if parsed is not None:
                self.health.mark_aux_reachable()
                return parsed
            if self.fallback_llm is None:
                raise primary_exc
            # fall through to fallback path if available
        except Exception as primary_exc:
            parsed = None
            if structured_schema is not None and self.fallback_llm is None:
                parsed = await self._recover_via_raw_invoke(
                    primary,
                    invoke_arg,
                    structured_schema,
                    timeout=_timeout,
                )
            if parsed is not None:
                self.health.mark_aux_reachable()
                return parsed
            if self.fallback_llm is None:
                raise
            logger.error(
                "AUXILIARY OUTPUT PARSE/transport failure on aux model '%s' for "
                "task '%s' (%s: %s) — retrying on main model '%s'.",
                self.health.model,
                task_name,
                type(primary_exc).__name__,
                str(primary_exc)[:200],
                self._fallback_model_name,
            )
            self.health.mark_aux_unreachable(task_name, primary_exc)
            # The escalated call used to be the one path with no protection at
            # all: a transient blip on the MAIN model raised straight out of
            # here, and for compaction that surfaces as
            # SummarizationFailed('aux_unavailable') — failing the turn on a
            # blip, at the exact moment we had already given up on aux.
            fallback_raw = await invoke_with_retry(
                lambda: self._invoke_provider(
                    build_runnable(self.fallback_llm, fallback_method),
                    invoke_arg,
                    _timeout,
                ),
                policy=_policy,
                description=(
                    f"aux '{task_name}' fallback on main model "
                    f"'{self._fallback_model_name}'"
                ),
            )
            if structured_schema is not None:
                try:
                    return self._recover_structured_output(
                        fallback_raw, structured_schema, task_name
                    )
                except Exception:
                    logger.debug(
                        "Structured-output fallback parse failed on raw response; "
                        "trying raw invoke recovery on fallback model."
                    )
                    fallback_raw = await self._recover_via_raw_invoke(
                        self.fallback_llm,
                        invoke_arg,
                        structured_schema,
                        timeout=_timeout,
                    )
                    if fallback_raw is not None:
                        return fallback_raw
                    raise
            return fallback_raw

    async def ainvoke(
        self,
        messages: List[BaseMessage],
        *,
        task_name: str = "raw_invoke",
        timeout: Optional[float] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ):
        """Raw (unstructured) aux call with main-model fallback.

        For call sites that used to reach past the wrapper into
        ``auxiliary_llm.llm.ainvoke(...)`` directly (e.g. session title
        generation). Routes through :meth:`_ainvoke_fallback` so those calls get
        the same fallback + loud-degrade behaviour as chain/agent.
        """
        return await self._ainvoke_fallback(
            lambda llm, method: llm,
            messages,
            task_name=task_name,
            timeout=timeout,
            retry_policy=retry_policy,
        )

    def _recover_structured_output(
        self, result: Any, schema: Type[BaseModel], task_name: str
    ) -> dict:
        if not isinstance(result, dict):
            raise TypeError(
                f"Unexpected structured-output result for {task_name}: {type(result)}"
            )
        parsed = result.get("parsed")
        if parsed is None:
            raw_text = None
            raw_result = result.get("raw")
            if hasattr(raw_result, "content"):
                raw_text = raw_result.content
            recovery = recover_structured(raw_text, schema)
            if recovery is None:
                parsing_error = result.get("parsing_error")
                raise ValueError(
                    f"Structured-output validation failed for {task_name}: {parsing_error}"
                )
            return {"raw": raw_result, "parsed": recovery, "parsing_error": None}
        return {"raw": result.get("raw"), "parsed": parsed}

    def _build_with_structured_output(
        self, llm: BaseChatModel, schema: Type[BaseModel], method: Optional[str]
    ):
        return llm.with_structured_output(
            schema,
            method=method or self.structured_output_method,
            include_raw=True,
        )

    async def _recover_via_raw_invoke(
        self,
        llm: BaseChatModel,
        invoke_arg,
        schema: Optional[Type[BaseModel]],
        *,
        timeout: float,
    ) -> Optional[dict]:
        if schema is None:
            return None
        try:
            raw_result = await self._invoke_provider(llm, invoke_arg, timeout)
        except AuxiliaryProviderAdmissionClosed:
            raise
        except Exception as recovery_exc:
            logger.debug("Raw fallback parse recovery failed: %s", recovery_exc)
            return None
        raw_text = (
            raw_result.content if hasattr(raw_result, "content") else str(raw_result)
        )
        parsed = recover_structured(raw_text, schema)
        if parsed is None:
            return None
        return {"raw": raw_result, "parsed": parsed, "parsing_error": None}

    async def chain(
        self,
        task: AuxTask,
        timeout: Optional[float] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> BaseModel:
        """Single LLM call: system prompt + context -> structured output.

        For tasks that need reasoning but no tool access.

        Args:
            task: AuxTask instance with system_prompt, build_context(), output_schema
            timeout: Per-call timeout override (seconds). Defaults to
                ``self.timeout``. Lets a long task (conversation summarization)
                request a larger budget without widening the short interactive
                default that protects every other aux task.

        Returns:
            Pydantic model instance matching task.output_schema

        Raises:
            asyncio.TimeoutError: If the LLM call exceeds timeout
        """
        messages = [
            SystemMessage(content=task.system_prompt),
            HumanMessage(content=task.build_context()),
        ]

        # Pre-flight: fail fast (typed, non-retryable) when the input cannot
        # fit the auxiliary model's own window, instead of overflowing at the
        # HTTP transport. Memory-extraction calls were shipping 951k-token
        # payloads to a 131k model (session_silent_failure_audit.md #7).
        if self.max_context_tokens:
            from shared.runtime.core.chunk_planner import count_text_tokens

            input_tokens = count_text_tokens(messages[0].content) + count_text_tokens(
                messages[1].content
            )
            if input_tokens > self.max_context_tokens:
                task_name = task.__class__.__name__
                error = AuxInputTooLarge(
                    input_tokens, self.max_context_tokens, task_name
                )
                self.health.record_failure(task_name, error)
                raise error

        start = time.monotonic()
        raw_result = await self._invoke_aux(
            self._ainvoke_fallback(
                lambda llm, method: self._build_with_structured_output(
                    llm, task.output_schema, method
                ),
                messages,
                structured_schema=task.output_schema,
                method=self.structured_output_method,
                task_name=task.__class__.__name__,
                timeout=timeout if timeout is not None else self.timeout,
                retry_policy=retry_policy,
            ),
            task=task,
            messages=messages,
            start=start,
        )
        latency_ms = int((time.monotonic() - start) * 1000)

        parsed = raw_result["parsed"]
        raw_response = raw_result["raw"]

        logger.debug(
            f"AuxiliaryLLM.chain completed: {task.__class__.__name__} -> "
            f"{type(parsed).__name__}"
        )

        self._archive_call(task, messages, raw_response, latency_ms)

        return parsed

    async def complete(
        self,
        task: AuxTextTask,
        timeout: Optional[float] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> AIMessage:
        """Single LLM call: system prompt + context -> the model's own text.

        Same pre-flight guard, main-model fallback and archiving as
        :meth:`chain`, without structured output. Returns the raw message so
        the caller can check ``finish_reason`` and validate the text.
        """
        messages = [
            SystemMessage(content=task.system_prompt),
            HumanMessage(content=task.build_context()),
        ]

        if self.max_context_tokens:
            from shared.runtime.core.chunk_planner import count_text_tokens

            input_tokens = count_text_tokens(messages[0].content) + count_text_tokens(
                messages[1].content
            )
            if input_tokens > self.max_context_tokens:
                task_name = task.__class__.__name__
                error = AuxInputTooLarge(
                    input_tokens, self.max_context_tokens, task_name
                )
                self.health.record_failure(task_name, error)
                raise error

        start = time.monotonic()
        response = await self._invoke_aux(
            self._ainvoke_fallback(
                lambda llm, method: llm,
                messages,
                task_name=task.__class__.__name__,
                timeout=timeout if timeout is not None else self.timeout,
                retry_policy=retry_policy,
                primary_llm=(
                    self.summarization_llm if isinstance(task, SummarizeTask) else None
                ),
            ),
            task=task,
            messages=messages,
            start=start,
        )
        latency_ms = int((time.monotonic() - start) * 1000)
        self._archive_call(task, messages, response, latency_ms)
        return response

    async def agent(self, task: AuxAgentTask) -> BaseModel:
        """Short-lived agent loop: system prompt + tools -> structured result.

        Runs a tool loop capped at max_iterations, then makes one final
        structured-output call to produce the result.

        Args:
            task: AuxAgentTask with system_prompt, build_context(),
                  output_schema, get_tools()

        Returns:
            Pydantic model instance matching task.output_schema

        Raises:
            asyncio.TimeoutError: If any individual LLM call exceeds timeout
        """
        tools = task.get_tools()
        from shared.runtime.services.guardrails import apply_guardrails_to_tools

        tools = apply_guardrails_to_tools(tools, model=_get_model_name(self.llm))
        tool_map = {t.name: t for t in tools}

        messages: List[BaseMessage] = [
            SystemMessage(content=task.system_prompt),
            HumanMessage(content=task.build_context()),
        ]

        start = time.monotonic()
        tool_calls_made = 0
        for iteration in range(self.max_iterations):
            response = await self._invoke_aux(
                self._ainvoke_fallback(
                    lambda llm, method: llm.bind_tools(tools),
                    messages,
                    method=self.structured_output_method,
                    task_name=task.__class__.__name__,
                    timeout=self.timeout,
                ),
                task=task,
                messages=messages,
                start=start,
            )
            messages.append(response)

            if not response.tool_calls:
                # LLM is done with tool calls
                break

            # Execute tool calls
            for tool_call in response.tool_calls:
                tool_name = tool_call.get("name", "unknown")
                tool_args = tool_call.get("args", {})
                tool_id = tool_call.get("id", "")

                try:
                    tool_fn = tool_map.get(tool_name)
                    if tool_fn is None:
                        result = f"Error: Unknown tool '{tool_name}'"
                    else:
                        result = await tool_fn.ainvoke(tool_args)
                        if not isinstance(result, str):
                            result = str(result)
                    tool_calls_made += 1
                except Exception as e:
                    result = f"Error executing {tool_name}: {e}"
                    logger.warning(f"AuxiliaryLLM.agent: tool {tool_name} failed: {e}")

                messages.append(
                    ToolMessage(
                        content=result,
                        tool_call_id=tool_id,
                    )
                )

        iterations_used = iteration + 1

        logger.info(
            f"AuxiliaryLLM.agent completed: {task.__class__.__name__}, "
            f"{tool_calls_made} tool calls in {iterations_used} iterations"
        )

        # Final structured-output call to get the result
        messages.append(
            HumanMessage(
                content="Summarize what you accomplished in the required output format."
            )
        )

        raw_result = await self._invoke_aux(
            self._ainvoke_fallback(
                lambda llm, method: self._build_with_structured_output(
                    llm, task.output_schema, method
                ),
                messages,
                structured_schema=task.output_schema,
                method=self.structured_output_method,
                task_name=task.__class__.__name__,
                timeout=self.timeout,
            ),
            task=task,
            messages=messages,
            start=start,
        )
        latency_ms = int((time.monotonic() - start) * 1000)

        parsed = raw_result["parsed"]
        raw_response = raw_result["raw"]

        self._archive_call(
            task,
            messages,
            raw_response,
            latency_ms,
            auxiliary_metadata={
                "iterations": iterations_used,
                "tool_calls_made": tool_calls_made,
            },
        )

        return parsed

    def _archive_call(
        self,
        task: AuxTask,
        messages: List[BaseMessage],
        response: AIMessage,
        latency_ms: int,
        auxiliary_metadata: Optional[dict] = None,
    ) -> None:
        """Archive an auxiliary LLM call. Fire-and-forget — never raises."""
        if not self._archiver or not self._job_id:
            return

        try:
            task_class = task.__class__.__name__
            call_type = _TASK_CALL_TYPES.get(task_class, "auxiliary")

            meta = {"task_class": task_class}
            if auxiliary_metadata:
                meta.update(auxiliary_metadata)

            self._archiver.archive(
                job_id=self._job_id,
                agent_type=self._agent_type,
                messages=messages,
                response=response,
                model=_get_model_name(self.llm),
                latency_ms=latency_ms,
                call_type=call_type,
                auxiliary_metadata=meta,
            )
        except Exception as e:
            logger.warning(
                f"Failed to archive auxiliary call ({task.__class__.__name__}): {e}"
            )

    def _archive_error(
        self,
        task: AuxTask,
        messages: List[BaseMessage],
        exc: BaseException,
        latency_ms: int,
    ) -> None:
        """Archive a FAILED auxiliary call so it surfaces in the debug view.

        Auxiliary failures are swallowed as non-fatal by their callers, so
        without this they leave no llm_requests row at all (unlike main-loop
        failures, which surface via the job's error state). Fire-and-forget.
        """
        if not self._archiver or not self._job_id:
            return
        try:
            task_class = task.__class__.__name__
            call_type = _TASK_CALL_TYPES.get(task_class, "auxiliary")
            self._archiver.archive_error(
                job_id=self._job_id,
                agent_type=self._agent_type,
                messages=messages,
                model=_get_model_name(self.llm),
                error=str(exc),
                error_type=type(exc).__name__,
                latency_ms=latency_ms,
                call_type=call_type,
                auxiliary_metadata={"task_class": task_class},
            )
        except Exception as e:
            logger.warning(
                f"Failed to archive auxiliary error ({task.__class__.__name__}): {e}"
            )

    async def _invoke_aux(
        self,
        awaitable,
        *,
        task: AuxTask,
        messages: List[BaseMessage],
        start: float,
    ):
        """Await an auxiliary LLM call; archive an error row + re-raise on failure.

        Centralizes the failure path for chain() and agent() so a failed
        auxiliary call is recorded to llm_requests before the exception
        propagates to the (swallowing) caller.
        """
        try:
            return await awaitable
        except Exception as exc:
            self._archive_error(
                task, messages, exc, int((time.monotonic() - start) * 1000)
            )
            raise


# =============================================================================
# Memory extraction helper
# =============================================================================

# Max messages to include in a single observation window
_MAX_OBSERVATION_WINDOW = 40


def _observation_window(messages: List[BaseMessage]) -> List[BaseMessage]:
    """The newest ``_MAX_OBSERVATION_WINDOW`` conversation messages.

    Injected context is filtered BEFORE the count cap: it carries no new
    fact, and persisted context entries would otherwise use up window slots.
    """
    from shared.runtime.core.context_entries import is_context_injection

    messages = [m for m in messages if not is_context_injection(m)]
    if len(messages) > _MAX_OBSERVATION_WINDOW:
        messages = messages[-_MAX_OBSERVATION_WINDOW:]
    return messages


async def extract_and_store_memories(
    auxiliary_llm: "AuxiliaryLLM",
    recall_store,
    messages: List[BaseMessage],
    memory_extraction_prompt: str,
    phase: int = 0,
    source_turn_start: Optional[int] = None,
    source_turn_end: Optional[int] = None,
) -> int:
    """Extract memories via AuxiliaryLLM and store them in RecallStore.

    Runs the ExtractMemoriesTask in chain mode and stores each result.

    Args:
        auxiliary_llm: AuxiliaryLLM instance for extraction
        recall_store: RecallStore instance for storage
        messages: Conversation messages to extract from
        memory_extraction_prompt: System prompt for memory extraction
        phase: Current phase number
        source_turn_start: Start turn for windowed extraction (optional)
        source_turn_end: End turn for windowed extraction (optional)

    Returns:
        Number of memories successfully stored
    """
    try:
        messages = _observation_window(messages)

        if not messages:
            return 0

        task = ExtractMemoriesTask(
            messages=messages, prompt=memory_extraction_prompt, phase=phase
        )
        result = await auxiliary_llm.chain(task)

        stored_count = 0
        for mem in result.memories:
            try:
                mem_id = await recall_store.store(
                    content=mem.content,
                    summary=mem.summary,
                    keywords=mem.keywords,
                    importance=mem.importance,
                    memory_type=mem.type,
                    source="observer",
                    source_turn_start=source_turn_start,
                    source_turn_end=source_turn_end,
                    source_phase=phase,
                    retrieval_messages=mem.retrieval_messages or None,
                )
                if mem_id:
                    stored_count += 1
            except Exception as e:
                logger.warning(
                    "Memory extraction: failed to store memory: %s: %s",
                    type(e).__name__,
                    e,
                )

        logger.info(
            f"Memory extraction: extracted {len(result.memories)}, "
            f"stored {stored_count} (phase {phase})"
        )
        auxiliary_llm.health.record_success("memory_extraction")
        return stored_count

    except Exception as e:
        auxiliary_llm.health.record_failure("memory_extraction", e)
        # Include the type — bare openai exceptions can format as "" (B1
        # follow-up; same fix as persistent_graph retrieval logging).
        logger.warning(
            "Memory extraction failed (non-fatal): %s: %s", type(e).__name__, e
        )
        return 0


async def verify_and_store_citation(
    verify_aux: "AuxiliaryLLM",
    engine: Any,
    citation_id: int,
    prompt: str,
) -> Optional["CitationVerdict"]:
    """Verify one pending citation via the auxiliary model and write the verdict back.

    Counterpart to ``extract_and_store_memories`` for the citation engine: runs
    ``VerifyCitationTask`` in chain mode on ``verify_aux``, records ``AuxHealth``,
    and persists the verdict via ``engine._update_verification_status``.

    Eventually-consistent (D2): the citation was already written ``pending`` by
    ``cite_*``; this runs in the background and flips it to ``verified`` /
    ``failed``. **An aux *outage* leaves the row ``pending``** (so the Phase-2b
    boundary reconcile / a retry can pick it up) — only a real negative verdict
    sets ``failed``. Non-fatal: never raises into the (fire-and-forget) caller.

    Args:
        verify_aux: AuxiliaryLLM wrapping the citation-verification model
            (the dedicated CITATION_LLM model, or the auxiliary model fallback).
        engine: The async CitationEngine (data access + status write-back).
        citation_id: The citation to verify.
        prompt: The matrix-resolved citation-verification system prompt.

    Returns:
        The CitationVerdict on a successful verdict, else None.
    """
    try:
        citation = await engine.get_citation(citation_id)
        if citation is None:
            return None
        source = await engine.get_source(citation.source_id)
        if source is None:
            logger.warning(
                "Citation verification: source [%s] for citation [%s] not found",
                citation.source_id,
                citation_id,
            )
            return None

        source_content = engine._extract_relevant_content(
            source.content, citation.locator
        )
        task = VerifyCitationTask(
            claim=citation.claim,
            quote_context=citation.quote_context,
            verbatim_quote=citation.verbatim_quote,
            source_content=source_content,
            prompt=prompt,
        )

        verdict = await verify_aux.chain(task)

        # Lazy import — the citation_engine package owns this model.
        from shared.runtime.citation_engine.models import VerificationResult

        await engine._update_verification_status(
            citation_id,
            VerificationResult(
                is_verified=verdict.verified,
                similarity_score=verdict.similarity_score,
                matched_text=verdict.matched_text,
                matched_location=(
                    {"matched_text": verdict.matched_text}
                    if verdict.matched_text
                    else None
                ),
                reasoning=verdict.reasoning,
            ),
        )
        verify_aux.health.record_success("citation_verification")
        logger.info(
            "Citation [%s] verified=%s (score %.2f)",
            citation_id,
            verdict.verified,
            verdict.similarity_score,
        )
        return verdict

    except Exception as e:
        verify_aux.health.record_failure("citation_verification", e)
        # Leave verification_status='pending' — an aux outage is transient; the
        # Phase-2b boundary reconcile (or a re-read) can retry. Non-fatal.
        logger.warning(
            "Citation verification failed for [%s] (non-fatal): %s: %s",
            citation_id,
            type(e).__name__,
            e,
        )
        return None


def _should_extract_memories(
    turn_count: int,
    interval: int,
    last_observed_turn: int,
) -> bool:
    """Check if memory extraction should run on this turn.

    Args:
        turn_count: Current turn count
        interval: Extraction interval (every N turns)
        last_observed_turn: Last turn when extraction ran

    Returns:
        True if extraction should run
    """
    if turn_count <= 0:
        return False
    if turn_count <= last_observed_turn:
        return False
    return turn_count % interval == 0


def _should_assemble_memories(
    turn_count: int,
    interval: int,
    last_assembled_turn: int,
) -> bool:
    """Check if memory assembler should run on this turn.

    Args:
        turn_count: Current turn count
        interval: Assembly interval (every N turns)
        last_assembled_turn: Last turn when assembler ran

    Returns:
        True if assembler should run
    """
    if turn_count <= 0:
        return False
    if turn_count <= last_assembled_turn:
        return False
    return turn_count % interval == 0


async def assemble_memories(
    auxiliary_llm: "AuxiliaryLLM",
    recall_store,
    messages: List[BaseMessage],
    current_injection_text: str,
    memory_assembler_prompt: str,
) -> Optional["AssemblyResult"]:
    """Run the memory assembler to review and adjust memory TTLs.

    Counterpart to extract_and_store_memories. While the extractor
    creates new memories, the assembler curates existing ones by
    adjusting their TTLs (boost relevant, deprecate stale).

    Args:
        auxiliary_llm: AuxiliaryLLM instance for agent-mode execution
        recall_store: RecallStore instance (passed to assembler tools)
        messages: Recent conversation messages for context
        current_injection_text: Currently injected memory block text
        memory_assembler_prompt: System prompt for memory assembly

    Returns:
        AssemblyResult on success, None on failure
    """
    try:
        messages = _observation_window(messages)

        if not messages:
            return None

        recent_context = _format_messages_for_extraction(messages)

        from shared.runtime.services.assembler_tools import create_assembler_tools

        assembler_tools = create_assembler_tools(recall_store)

        task = AssembleMemoriesTask(
            recent_context=recent_context,
            current_injection=current_injection_text,
            assembler_tools=assembler_tools,
            prompt=memory_assembler_prompt,
        )

        result = await auxiliary_llm.agent(task)

        actions_count = len(result.actions_taken) if result.actions_taken else 0
        gaps_count = len(result.gaps_identified) if result.gaps_identified else 0
        logger.info(
            f"Memory assembly: {actions_count} TTL adjustments, "
            f"{gaps_count} gaps identified — {result.summary}"
        )
        auxiliary_llm.health.record_success("memory_assembly")
        return result

    except Exception as e:
        auxiliary_llm.health.record_failure("memory_assembly", e)
        logger.warning(
            "Memory assembly failed (non-fatal): %s: %s", type(e).__name__, e
        )
        return None


# =============================================================================
# Helpers
# =============================================================================


def _format_messages_for_extraction(messages: List[BaseMessage]) -> str:
    """Format messages into readable text for the extraction LLM.

    Filters out injected context (typed entries and the legacy tail) and the
    compaction summary to focus on actual conversation content.
    """
    from shared.runtime.core.context_entries import is_context_injection
    from shared.runtime.core.message_markers import (
        is_compaction_summary,
        is_protected_message,
    )

    lines = []
    for msg in messages:
        if is_context_injection(msg):
            continue
        if is_compaction_summary(msg):
            continue  # derived from turns already extracted, not a user turn
        if is_protected_message(msg):
            continue  # phase instruction block — guidance, not conversation

        role = _get_message_role(msg)
        content = msg.content if hasattr(msg, "content") else ""
        # Responses-API models return list-of-blocks content; .strip() on it
        # killed extraction every turn (AttributeError, contained). Flatten
        # with the summarizer helper, which also keeps base64 image payloads
        # out of the extraction prompt.
        if not isinstance(content, str):
            from shared.runtime.core.image_tokens import content_to_summary_text

            content = content_to_summary_text(content)

        if not content or not content.strip():
            continue

        # Truncate very long tool results
        if isinstance(msg, ToolMessage) and len(content) > 1000:
            content = content[:1000] + "... [truncated]"

        lines.append(f"[{role}] {content}")

    return "\n\n".join(lines)


def _get_message_role(msg: BaseMessage) -> str:
    """Get a human-readable role label for a message."""
    if isinstance(msg, HumanMessage):
        return "User"
    elif isinstance(msg, AIMessage):
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            tool_names = ", ".join(tc.get("name", "?") for tc in msg.tool_calls)
            return f"Agent (calls: {tool_names})"
        return "Agent"
    elif isinstance(msg, ToolMessage):
        return "Tool Result"
    return "System"
