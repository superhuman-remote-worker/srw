"""Chunked, full-coverage memory extraction at the compaction boundary.

Design: knowledge-history/done/memory_extraction_before_compaction.md (Slice 2).

The async interval extractor (``extract_and_store_memories``) tail-caps its
input to the newest 40 messages — exactly the ones a compaction *keeps*. When a
mid-phase token-threshold compaction fires, the durable facts at risk are the
*oldest* (evicted) messages, and a single-shot extraction over a large history
raises the non-retryable ``AuxInputTooLarge`` and silently extracts nothing.

``MemoryExtractionEngine`` is the mirror of ``SummarizationEngine`` but
**maps instead of folds**: it chunk-plans the evicted slice with the shared
:class:`~shared.runtime.core.chunk_planner.ChunkPlanner` (with overlap so a fact straddling
a chunk edge isn't lost), extracts each chunk independently, unions the results,
dedups them in memory, and stores them serially (write-time dedup compares
against already-committed rows, so concurrent stores would race). A failing
chunk is skipped, not fatal — chunks are independent, so a partial result still
beats the zero the single-shot path returns.
"""

import asyncio
import logging
from typing import Any, Callable, List, Optional

from langchain_core.messages import BaseMessage

from shared.runtime.core.chunk_planner import (
    DEFAULT_AUX_WINDOW,
    SCHEMA_OVERHEAD_TOKENS,
    ChunkPlanner,
    SummarizationFailed,
    count_text_tokens,
)
from shared.runtime.core.llm_retry import NO_RETRY
from agent.core.summarizer import _describe_exc, is_overflow_error

logger = logging.getLogger(__name__)

# Fixed reserve for the extraction JSON output (a list of ExtractedMemory).
# Bounds how much of the aux window is held back for the model's answer;
# generous enough for a couple dozen memories per chunk.
EXTRACTION_OUTPUT_RESERVE_TOKENS = 4_000

# ~15% overlap between adjacent chunks (chat reads as narrative; consensus
# 10–20%). Duplicate facts from the overlap are absorbed by dedup, so it's
# near-free insurance against a fact straddling a chunk boundary.
DEFAULT_OVERLAP_RATIO = 0.15

# Per-chunk retry policy. Unlike the fold, exhaustion skips the chunk (chunks
# are independent) rather than aborting the whole run.
MAX_EXTRACTION_ATTEMPTS = 3
EXTRACTION_BACKOFF_SECONDS = (5.0, 15.0)  # sleep after attempt 1, attempt 2

# Per-chunk call timeout (seconds). Matches the summarizer's per-fold budget.
DEFAULT_CALL_TIMEOUT = 240.0


def _dedup_in_memory(memories: List[Any]) -> List[Any]:
    """Collapse cross-chunk duplicates before persisting.

    Keys on whitespace-normalized, lowercased content so the overlap region's
    re-extracted facts don't each become a store() call (which would race the
    write-time dedup). On a collision the higher-importance variant wins; order
    is otherwise preserved. Semantic (non-identical) dedup stays the store's job.
    """
    seen: dict = {}
    for mem in memories:
        content = getattr(mem, "content", "") or ""
        key = " ".join(content.lower().split())
        if not key:
            continue
        existing = seen.get(key)
        if existing is None or (getattr(mem, "importance", 0) or 0) > (
            getattr(existing, "importance", 0) or 0
        ):
            seen[key] = mem
    return list(seen.values())


class MemoryExtractionEngine:
    """Map-not-fold memory extraction sized for the auxiliary model's window."""

    def __init__(
        self,
        auxiliary,
        recall_store,
        extraction_prompt: Optional[str] = None,
        *,
        token_counter: Optional[Callable[[str], int]] = None,
        counting_model: Optional[str] = None,
        output_reserve: int = EXTRACTION_OUTPUT_RESERVE_TOKENS,
        overlap_ratio: float = DEFAULT_OVERLAP_RATIO,
        call_timeout: float = DEFAULT_CALL_TIMEOUT,
    ):
        """
        Args:
            auxiliary: AuxiliaryLLM instance. Its ``max_context_tokens`` is the
                budgeting authority (same as the summarizer).
            recall_store: RecallStore for persistence (writers gate on
                non-None, but a None store here just yields zero stored).
            extraction_prompt: Pre-rendered extraction system prompt.
            token_counter: Override for text token counting (tests).
            counting_model: Model name for tokenizer selection (best effort).
            output_reserve: Tokens reserved for the extraction JSON output.
            overlap_ratio: Whole-part overlap between adjacent chunks.
            call_timeout: Per-chunk aux call timeout (seconds).
        """
        self.auxiliary = auxiliary
        self.recall_store = recall_store
        self.extraction_prompt = extraction_prompt or ""
        self.call_timeout = call_timeout
        self._token_counter = token_counter
        # MemoryRuntime carries no counter, so best-effort a tokenizer from the
        # aux model name when the caller doesn't pass one.
        self.counting_model = counting_model or getattr(
            auxiliary, "counting_model", None
        )

        window = getattr(auxiliary, "max_context_tokens", None)
        if not window or window <= 0:
            logger.warning(
                "MemoryExtractionEngine: auxiliary model window unknown — "
                f"falling back to conservative {DEFAULT_AUX_WINDOW} tokens"
            )
            window = DEFAULT_AUX_WINDOW
        self.aux_window = int(window)

        self.planner = ChunkPlanner(
            self.aux_window,
            overhead_tokens=self._measure_overhead(),
            output_reserve=output_reserve,
            carry_reserve=0,  # map, no rolling carry
            overlap_ratio=overlap_ratio,
            token_counter=token_counter,
            counting_model=self.counting_model,
        )

    def _count(self, text: str) -> int:
        if self._token_counter is not None:
            return self._token_counter(text)
        return count_text_tokens(text, self.counting_model)

    def _measure_overhead(self) -> int:
        """Measure the extraction prompt's token cost (+ schema allowance)."""
        try:
            from shared.runtime.services.auxiliary import TextExtractMemoriesTask

            probe = TextExtractMemoriesTask("", self.extraction_prompt)
            prompt_tokens = self._count(probe.system_prompt)
        except Exception as e:  # pragma: no cover - defensive
            logger.debug(f"Extraction overhead probe failed, using fixed value: {e}")
            prompt_tokens = 3_000
        return prompt_tokens + SCHEMA_OVERHEAD_TOKENS

    def _format(self, messages: List[BaseMessage]) -> List[str]:
        """Low-loss ``messages -> List[str]`` formatter (§3.3).

        Filters injected context (typed entries and the legacy tail; it
        carries no durable fact) and empties, but — unlike the summary and
        interval-extraction formatters — does **not** truncate tool results.
        The planner's oversized-part hard-split handles a giant tool result
        instead; truncating here would drop exactly the durable detail this
        feature exists to capture.
        """
        from shared.runtime.core.context_entries import is_context_injection
        from shared.runtime.core.message_markers import is_protected_message
        from shared.runtime.services.auxiliary import _get_message_role

        parts: List[str] = []
        for msg in messages:
            if is_context_injection(msg):
                continue
            if is_protected_message(msg):
                continue  # phase instruction block — guidance, not a fact
            content = getattr(msg, "content", "")
            if not isinstance(content, str):
                content = str(content)
            if not content.strip():
                continue
            parts.append(f"[{_get_message_role(msg)}] {content}")
        return parts

    async def run(self, messages: List[BaseMessage], *, phase: int = 0) -> int:
        """Extract + store memories from ``messages``. Returns the store count.

        Never raises for content reasons — a planning failure or a bad chunk is
        logged and skipped so it can never break the compaction that scheduled
        it. ``CancelledError`` (hard interrupt) is passed through untouched.
        """
        if not messages or self.recall_store is None:
            return 0

        parts = self._format(messages)
        if not parts:
            return 0

        try:
            plan = self.planner.plan(parts)
        except SummarizationFailed as e:
            logger.warning(
                "pre-compaction extraction: planning failed (%s); skipped", e.reason
            )
            return 0

        collected: List[Any] = []
        for chunk in plan.chunks:
            collected.extend(await self._extract_chunk(chunk))

        deduped = _dedup_in_memory(collected)

        stored = 0
        for mem in deduped:  # SEQUENTIAL — write-time dedup compares committed rows
            try:
                mem_id = await self.recall_store.store(
                    content=mem.content,
                    summary=mem.summary,
                    keywords=mem.keywords,
                    importance=mem.importance,
                    memory_type=mem.type,
                    source="observer",
                    source_phase=phase,
                    retrieval_messages=mem.retrieval_messages or None,
                )
                stored += bool(mem_id)
            except Exception as e:
                logger.warning(
                    "pre-compaction extraction: store failed: %s: %s",
                    type(e).__name__,
                    e,
                )

        logger.info(
            "pre-compaction extraction: %d chunk(s), %d unique, %d stored (phase %s)",
            plan.n_passes,
            len(deduped),
            stored,
            phase,
        )
        return stored

    async def _extract_chunk(self, chunk) -> List[Any]:
        """Extract one chunk with bounded retries; skip (not abort) on failure.

        Overflow is deterministic (a planned chunk should never overflow), so
        it is not retried. Any other error retries with backoff, then the chunk
        is skipped — chunks are independent, so a lost chunk costs coverage, not
        correctness (unlike a fold, where a lost pass corrupts the summary).
        """
        from shared.runtime.services.auxiliary import TextExtractMemoriesTask

        last_error: Optional[BaseException] = None
        for attempt in range(1, MAX_EXTRACTION_ATTEMPTS + 1):
            try:
                task = TextExtractMemoriesTask(chunk.text, self.extraction_prompt)
                # NO_RETRY: this loop already owns the retry for this call.
                # Retry belongs at exactly one layer per path.
                result = await self.auxiliary.chain(
                    task, timeout=self.call_timeout, retry_policy=NO_RETRY
                )
                return list(getattr(result, "memories", []) or [])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if is_overflow_error(e):
                    logger.warning(
                        "pre-compaction chunk %s overflowed despite planning "
                        "(%s tokens, skipped): %s",
                        chunk.index,
                        chunk.tokens,
                        e,
                    )
                    return []
                last_error = e
                if attempt == MAX_EXTRACTION_ATTEMPTS:
                    break
                backoff = EXTRACTION_BACKOFF_SECONDS[
                    min(attempt - 1, len(EXTRACTION_BACKOFF_SECONDS) - 1)
                ]
                logger.warning(
                    "pre-compaction chunk %s attempt %d/%d failed (%s); retrying "
                    "in %ss",
                    chunk.index,
                    attempt,
                    MAX_EXTRACTION_ATTEMPTS,
                    _describe_exc(e),
                    backoff,
                )
                await asyncio.sleep(backoff)

        logger.warning(
            "pre-compaction chunk %s failed after %d attempts (skipped): %s",
            chunk.index,
            MAX_EXTRACTION_ATTEMPTS,
            _describe_exc(last_error),
        )
        return []
