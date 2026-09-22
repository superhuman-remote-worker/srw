"""Pure, deterministic chunk planner shared by summarization and extraction.

Factored out of ``SummarizationEngine``
(knowledge-history/done/memory_extraction_before_compaction.md, Slice 1). It sizes
formatted conversation parts into within-budget chunks for an auxiliary model's
window — no LLM calls, no I/O. Two callers, two budget shapes, one algorithm:

- **Summarization** folds a rolling summary, so the summary rides along every
  chunk. It passes ``output_reserve = carry_reserve = output_budget`` (the
  budget is reserved *twice*) and ``overlap_ratio = 0`` — a fold's sequential
  coherence covers chunk seams, so no overlap is needed. With these arguments
  the planner is byte-for-byte identical to the pre-extraction code path (the
  parity test in ``tests/test_context_safety.py`` pins that).
- **Extraction** maps each chunk independently with no rolling carry:
  ``carry_reserve = 0``, ``output_reserve`` = a fixed extraction-JSON reserve,
  and ``overlap_ratio ~= 0.15`` so a fact straddling a chunk edge isn't lost.
  Overlap is packed *natively* — chunks are packed against a reduced budget and
  a whole-part overlap seed is prepended — so no chunk ever exceeds ``budget``
  and the pack loop always makes forward progress.
"""

import logging
import math
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:  # pragma: no cover - environment dependent
    TIKTOKEN_AVAILABLE = False

# Fraction of the auxiliary window usable after safety margin. Covers tokenizer
# disagreement between our counter and the endpoint's, message framing
# overhead, and provider-side additions.
SAFETY_MARGIN = 0.85

# Fixed allowance for the structured-output schema the SDK appends to the
# request (tool/JSON-schema definition). Callers fold this into the
# ``overhead_tokens`` they pass in.
SCHEMA_OVERHEAD_TOKENS = 2_000

# Conservative chars-per-token used only when tiktoken is unavailable.
# 3.5 overestimates token counts for typical English/code, which errs toward
# smaller (safer) chunks.
_FALLBACK_CHARS_PER_TOKEN = 3.5

# Reserved tokens per split piece: the "[part i/j …]" marker plus
# tokenizer-boundary inflation from cutting mid-token.
_SPLIT_MARKER_RESERVE_TOKENS = 64

# Floor when the auxiliary window is unknown: matches the historical
# conservative default base (see LimitsConfig.model_max_context_tokens).
DEFAULT_AUX_WINDOW = 100_000


class SummarizationFailed(Exception):
    """A planned aux call could not proceed; callers keep raw history.

    Named for its original (and still primary) raiser, the summarizer, but the
    planner raises the ``aux_window_too_small`` variant and both the summarizer
    and the extraction engine import it from here, so the class stays a single
    shared object that ``context.py`` catches by ``reason``.

    ``reason`` is machine-readable and lands in the ``compaction.failed``
    event: ``aux_unavailable`` (retries exhausted), ``aux_overflow`` (a planned
    call still overflowed — a plan bug, never retried), ``aux_window_too_small``
    (window can't fit prompt + output budget), ``empty_plan`` (nothing to do).
    """

    def __init__(
        self,
        reason: str,
        message: str = "",
        pass_index: int = 0,
        n_passes: int = 0,
    ):
        self.reason = reason
        self.pass_index = pass_index
        self.n_passes = n_passes
        super().__init__(message or reason)


@dataclass
class Chunk:
    """One planned pass: a contiguous slice of formatted conversation parts.

    ``first_part``/``last_part`` are 1-based part ordinals; with overlap they
    can span into the previous chunk's tail (the seed), so adjacent chunks'
    ranges intersect by design.
    """

    index: int  # 1-based pass number
    first_part: int  # 1-based ordinal of the first conversation part
    last_part: int  # 1-based ordinal of the last conversation part
    text: str
    tokens: int


@dataclass
class ChunkPlan:
    """Deterministic chunking plan computed before any LLM call."""

    chunks: List[Chunk] = field(default_factory=list)
    total_tokens: int = 0
    chunk_budget: int = 0
    aux_window: int = 0

    @property
    def n_passes(self) -> int:
        return len(self.chunks)

    def describe(self) -> List[Dict[str, int]]:
        """Compact plan representation for the ``compaction.started`` event."""
        return [
            {
                "pass": c.index,
                "first_msg": c.first_part,
                "last_msg": c.last_part,
                "tokens": c.tokens,
            }
            for c in self.chunks
        ]


_ENCODING_CACHE: Dict[str, Any] = {}
_ENCODING_LOCK = threading.Lock()

# How long a failed encoding load is remembered before it is tried again. On a
# cold cache tiktoken fetches its vocab with synchronous HTTP; where that host
# does not resolve, each attempt blocks its thread for seconds of DNS retries.
# One failure predicts the next, so counts use the fallback for this long
# instead of paying the retry once per call (once per KB note, on the live
# 2026-08-07 stall).
_ENCODING_RETRY_SECONDS = 300.0


@dataclass(frozen=True)
class _EncodingUnavailable:
    """Cache marker for a failed load; ``retry_at`` is a ``time.monotonic`` value."""

    retry_at: float


def _cached_encoding(key: str) -> Tuple[bool, Any]:
    """``(True, encoding)`` when the cache answers for ``key``, else ``(False, None)``.

    A failure still inside its cooldown answers with ``None`` (use the fallback).
    """
    cached = _ENCODING_CACHE.get(key)
    if isinstance(cached, _EncodingUnavailable):
        return time.monotonic() < cached.retry_at, None
    return cached is not None, cached


def _load_encoding(key: str, model: Optional[str]) -> Any:
    """Return the encoding for ``key``, or None while it is unavailable."""
    answered, encoding = _cached_encoding(key)
    if answered:
        return encoding
    with _ENCODING_LOCK:
        # Re-read under the lock: a caller that waited on another thread's
        # failed load must see that failure, not repeat the download.
        answered, encoding = _cached_encoding(key)
        if answered:
            return encoding
        try:
            try:
                encoding = tiktoken.encoding_for_model(model) if model else None
            except (KeyError, ValueError):
                encoding = None
            if encoding is None:
                encoding = tiktoken.get_encoding("cl100k_base")
        except Exception as exc:
            _ENCODING_CACHE[key] = _EncodingUnavailable(
                time.monotonic() + _ENCODING_RETRY_SECONDS
            )
            logger.warning(
                "tiktoken encoding for %s unavailable (%r); using the "
                "conservative estimate for %ds",
                model or "the default model",
                exc,
                int(_ENCODING_RETRY_SECONDS),
            )
            return None
        _ENCODING_CACHE[key] = encoding
        return encoding


def count_text_tokens(text: str, model: Optional[str] = None) -> int:
    """Count tokens in a plain string with tiktoken, conservative fallback.

    The planner must use real token counts: the historical ``len(text) // 4``
    estimate let a 951k-token conversation slip past a 943k gate (it
    under-counts), straight into a 131k summarizer.

    A failed encoding load (no vocab in the cache and no route to its host)
    uses the fallback too, and is not retried until the cooldown passes.
    Images bake the vocab, so this is the offline-dev and outage path.
    """
    if not text:
        return 0
    if TIKTOKEN_AVAILABLE:
        encoding = _load_encoding(model or "_default", model)
        if encoding is not None:
            try:
                return len(encoding.encode(text, disallowed_special=()))
            except Exception:  # pragma: no cover - defensive
                pass
    return math.ceil(len(text) / _FALLBACK_CHARS_PER_TOKEN)


class ChunkPlanner:
    """Pack formatted conversation parts into within-budget chunks.

    Pure/deterministic — no LLM calls. Construct once per planning authority
    (an aux window + its overhead), then call :meth:`plan` on any part list.
    """

    def __init__(
        self,
        aux_window: int,
        *,
        overhead_tokens: int,
        output_reserve: int,
        carry_reserve: int = 0,
        overlap_ratio: float = 0.0,
        token_counter: Optional[Callable[[str], int]] = None,
        counting_model: Optional[str] = None,
    ):
        """
        Args:
            aux_window: The auxiliary model's context window (budgeting
                authority — never the main model's).
            overhead_tokens: System prompt + schema tokens the call carries
                regardless of input (measured by the caller).
            output_reserve: Tokens reserved for the model's output.
            carry_reserve: Additional reserve for content that rides along
                every chunk (the summarizer's rolling summary). 0 for map-style
                extraction.
            overlap_ratio: Fraction of the budget to overlap between adjacent
                chunks (0 = hard, non-overlapping boundaries).
            token_counter: Override for text token counting (tests).
            counting_model: Model name for tokenizer selection (best effort).
        """
        self.aux_window = int(aux_window)
        self.overhead_tokens = overhead_tokens
        self.output_reserve = output_reserve
        self.carry_reserve = carry_reserve
        self.overlap_ratio = overlap_ratio
        self._token_counter = token_counter
        self.counting_model = counting_model

    def _count(self, text: str) -> int:
        if self._token_counter is not None:
            return self._token_counter(text)
        return count_text_tokens(text, self.counting_model)

    @property
    def chunk_budget(self) -> int:
        """Max input tokens of conversation text per chunk.

        Leaves room for prompt overhead, the model's output, and any per-chunk
        carry (the rolling summary, when a caller sets ``carry_reserve``).
        """
        return (
            math.floor(self.aux_window * SAFETY_MARGIN)
            - self.overhead_tokens
            - self.output_reserve
            - self.carry_reserve
        )

    def plan(self, formatted_parts: List[str]) -> ChunkPlan:
        """Pack formatted conversation parts into within-budget chunks.

        Oversized single parts (one giant tool result) are hard-split so no
        chunk can exceed the budget. When ``overlap_ratio`` > 0 each chunk after
        the first is seeded with the previous chunk's trailing whole parts (up
        to the overlap allowance); parts are packed against a correspondingly
        reduced budget so the seeded chunk still fits ``budget``.

        Raises:
            SummarizationFailed: ``aux_window_too_small`` when the window cannot
                fit even the prompt + reserves.
        """
        budget = self.chunk_budget
        if budget < 1_000:
            raise SummarizationFailed(
                "aux_window_too_small",
                f"Auxiliary window {self.aux_window} cannot fit prompt overhead "
                f"({self.overhead_tokens}) + reserves "
                f"({self.output_reserve}+{self.carry_reserve})",
            )

        # Room reserved for the prepended overlap seed. ``pack_budget == budget``
        # when overlap is off, so the whole algorithm reduces to the original
        # non-overlapping packer (parity).
        overlap_tokens = (
            math.floor(budget * self.overlap_ratio) if self.overlap_ratio > 0 else 0
        )
        pack_budget = budget - overlap_tokens

        # Expand oversized parts into sub-parts that each fit pack_budget.
        expanded: List[tuple] = []  # (part_ordinal, text, tokens)
        for ordinal, part in enumerate(formatted_parts, start=1):
            tokens = self._count(part)
            if tokens <= pack_budget:
                expanded.append((ordinal, part, tokens))
                continue
            # Size pieces against a reduced budget so the marker prefix and
            # mid-token cut inflation can't push a piece over the real budget.
            effective = max(pack_budget - _SPLIT_MARKER_RESERVE_TOKENS, 1)
            n_pieces = math.ceil(tokens / effective)
            # Split by chars proportionally; re-measure each piece.
            piece_chars = math.ceil(len(part) / n_pieces)
            for i in range(n_pieces):
                piece = part[i * piece_chars : (i + 1) * piece_chars]
                if not piece:
                    continue
                marked = f"[part {i + 1}/{n_pieces} of an oversized message]\n{piece}"
                expanded.append((ordinal, marked, self._count(marked)))

        chunks: List[Chunk] = []
        total_tokens = 0
        carry: List[tuple] = []  # overlap seed carried from the previous chunk
        idx = 0
        n = len(expanded)

        while idx < n:
            current: List[tuple] = list(carry)  # seed (empty when overlap off)
            current_tokens = sum(t for _, _, t in current)
            first: Optional[int] = current[0][0] if current else None
            last: Optional[int] = current[-1][0] if current else None
            new_count = 0

            while idx < n:
                ordinal, text, tokens = expanded[idx]
                # Never split before the chunk holds a new part (a seed-only
                # chunk would make no forward progress); after that, respect the
                # full budget. A single new part always fits after the seed
                # because pack_budget + overlap_tokens == budget.
                if new_count > 0 and current_tokens + tokens > budget:
                    break
                current.append((ordinal, text, tokens))
                current_tokens += tokens
                total_tokens += tokens
                if first is None:
                    first = ordinal
                last = ordinal
                new_count += 1
                idx += 1

            chunks.append(
                Chunk(
                    index=len(chunks) + 1,
                    first_part=first or 1,
                    last_part=last or (first or 1),
                    text="\n".join(t for _, t, _ in current),
                    tokens=current_tokens,
                )
            )

            # Seed the next chunk with this chunk's trailing whole parts, up to
            # the overlap allowance. Only when overlap is on and parts remain.
            if idx < n and overlap_tokens > 0:
                carry = []
                acc = 0
                for entry in reversed(current):
                    if acc + entry[2] > overlap_tokens:
                        break
                    carry.insert(0, entry)
                    acc += entry[2]
            else:
                carry = []

        return ChunkPlan(
            chunks=chunks,
            total_tokens=total_tokens,
            chunk_budget=budget,
            aux_window=self.aux_window,
        )
