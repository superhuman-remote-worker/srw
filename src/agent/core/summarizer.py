"""Aux-budgeted rolling-fold summarization engine.

Design: knowledge-base/knowledge/features/context_summarization_rework.md (slices S1+S2).

Replaces the recursive map-reduce path (``_recursive_summarize``) and the
unstructured fallback that used to live in
``ContextManager._single_pass_summarize``. One algorithm:

1. ``plan()``   — measure the formatted conversation with a real tokenizer and
   pack it into chunks sized for the *summarizer's own* context window
   (``AuxiliaryLLM.max_context_tokens``), never the main model's.
2. ``run()``    — sequentially fold: ``summary_i = summarize(summary_{i-1} +
   chunk_i)``. Every call is within-budget by construction; passes are linear
   (``ceil(input/chunk_budget)``), so the engine scales to arbitrarily large
   inputs without recursion depth limits.

Each pass asks the auxiliary model for a Markdown checkpoint in plain text
(``SummarizeTask``, text mode) and validates what comes back: truncated,
section-less or looping output is rejected and the pass retried. Robustness
comes from bounded retries with backoff on the *same* call — there is no
fallback to a second, differently-shaped summarizer. On exhaustion the engine
raises :class:`SummarizationFailed` and the caller keeps the original history
(never a placeholder).

Design of the prompt contract and transcript: knowledge-base/knowledge/features/
compaction_refactor_fidelity_and_fork_strategy.md (WP1, WP2).

Progress is emitted through an optional async callback so the transport layer
decides what to do with it (persistent sessions broadcast SSE frames; worker
agents log).
"""

import asyncio
import logging
import math
import re
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from openai import BadRequestError
from pydantic import ValidationError

from shared.runtime.core.chunk_planner import (
    DEFAULT_AUX_WINDOW,
    SCHEMA_OVERHEAD_TOKENS,
    Chunk,
    ChunkPlan,
    ChunkPlanner,
    SummarizationFailed,
    count_text_tokens,
)
from agent.core.response_validator import (
    _detect_line_repetition,
    _detect_token_repetition,
)
from shared.runtime.core.llm_retry import NO_RETRY

logger = logging.getLogger(__name__)

# Retry policy per fold call — rides out transient aux-endpoint failures
# (503 flap, ReadTimeout) without falling back to a different algorithm.
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (5.0, 15.0)  # sleep after attempt 1, attempt 2

ProgressCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]


#: Heading of the file list the engine's caller appends from the tool calls.
#: The model never writes it; it is stripped from model output and from the
#: prior summary before a fold, then rebuilt deterministically.
FILES_SECTION_HEADING = "## Files Touched (recorded from tool calls)"

#: Tools whose ``path`` argument names a file the agent read.
_READ_TOOLS = frozenset({"read_file", "get_document_info"})
#: Tools that change files, and the arguments that name them.
_MODIFY_TOOL_ARGS: Dict[str, Tuple[str, ...]] = {
    "write_file": ("path",),
    "edit_file": ("path",),
    "delete_file": ("path",),
    "rename_file": ("path",),
    "move_file": ("source", "dest"),
    "copy_file": ("dest",),
}
#: Bound on each list so a long job cannot grow the section without limit.
_MAX_FILES_PER_LIST = 60

# Repetition thresholds for a checkpoint. Higher than the main-model response
# validator's: every empty section legitimately repeats "- (none)".
_MAX_SUMMARY_LINE_REPEATS = 20
_MAX_SUMMARY_TOKEN_REPEATS = 30


class SummaryRejected(Exception):
    """A fold pass returned output that is not a usable checkpoint.

    Retryable: sampling differs between attempts, and a loop or a truncation
    is not a property of the input alone.
    """

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        super().__init__(f"{reason}: {detail}" if detail else reason)


def _strip_wrapping(text: str) -> str:
    """Drop ``<think>`` blocks and a code fence wrapped around the whole text."""
    text = re.sub(r"(?is)<think>.*?</think>\s*", "", text).strip()
    fenced = re.match(r"(?s)^```[a-zA-Z]*\n(.*)\n```$", text)
    return fenced.group(1).strip() if fenced else text


def validate_summary_message(message: Any, *, expect_sections: bool) -> str:
    """Return the checkpoint text of an aux response, or raise SummaryRejected.

    Rejects a response the provider cut off at its output limit, one with no
    text (a tool call or an empty reply), one without any section heading
    when the prompt asked for sections (a refusal, or an answer to the
    transcript instead of a summary), and a repetition loop.
    """
    metadata = getattr(message, "response_metadata", None) or {}
    finish = str(
        metadata.get("finish_reason") or metadata.get("stop_reason") or ""
    ).lower()
    if finish in ("length", "max_tokens"):
        raise SummaryRejected("truncated", f"finish_reason={finish}")

    content = getattr(message, "content", message)
    if isinstance(content, list):
        content = "\n".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
            if not (
                isinstance(part, dict) and part.get("type") in ("thinking", "reasoning")
            )
        )
    text = _strip_wrapping(str(content or ""))
    if not text:
        raise SummaryRejected("empty")
    if expect_sections and not re.search(r"(?m)^#{1,3} \S", text):
        raise SummaryRejected("no_sections", text[:120])
    loop = _detect_line_repetition(
        text, max_line_repetitions=_MAX_SUMMARY_LINE_REPEATS
    ) or _detect_token_repetition(
        text, max_token_repetitions=_MAX_SUMMARY_TOKEN_REPEATS
    )
    if loop:
        raise SummaryRejected("repetition", loop)
    return strip_files_section(text)


def strip_files_section(summary: str) -> str:
    """Remove the recorded-files section (heading to the next ``## ``/end)."""
    idx = summary.find(FILES_SECTION_HEADING)
    if idx < 0:
        return summary
    rest = summary[idx + len(FILES_SECTION_HEADING) :]
    nxt = re.search(r"(?m)^## ", rest)
    tail = rest[nxt.start() :] if nxt else ""
    return (summary[:idx].rstrip() + ("\n\n" + tail if tail else "")).strip()


def _parse_files_section(summary: str) -> Tuple[List[str], List[str]]:
    """Read the Read/Modified lists back out of an earlier checkpoint."""
    idx = summary.find(FILES_SECTION_HEADING)
    if idx < 0:
        return [], []
    body = summary[idx + len(FILES_SECTION_HEADING) :]
    nxt = re.search(r"(?m)^## ", body)
    body = body[: nxt.start()] if nxt else body
    lists: Dict[str, List[str]] = {"read": [], "modified": []}
    for line in body.splitlines():
        match = re.match(r"^- (Read|Modified): (.*)$", line.strip())
        if match and match.group(2).strip() != "(none)":
            lists[match.group(1).lower()] = [
                p.strip() for p in match.group(2).split(", ") if p.strip()
            ]
    return lists["read"], lists["modified"]


def _collect_tool_call_files(messages: Iterable[Any]) -> Tuple[List[str], List[str]]:
    """Files read and modified, in first-seen order, from the tool calls."""
    read: List[str] = []
    modified: List[str] = []
    for msg in messages:
        for call in getattr(msg, "tool_calls", None) or []:
            name = call.get("name")
            args = call.get("args") or {}
            if not isinstance(args, dict):
                continue
            if name in _READ_TOOLS:
                targets, bucket = ("path",), read
            elif name in _MODIFY_TOOL_ARGS:
                targets, bucket = _MODIFY_TOOL_ARGS[name], modified
            else:
                continue
            for key in targets:
                value = args.get(key)
                if isinstance(value, str) and value.strip():
                    bucket.append(value.strip())
    return read, modified


def _merge_recent(older: List[str], newer: List[str], limit: int) -> List[str]:
    """Union keeping first-seen order, but the most recent ``limit`` entries."""
    merged: Dict[str, None] = {}
    for path in older + newer:
        merged.pop(path, None)
        merged[path] = None
    return list(merged)[-limit:]


def files_section(prior_summary: Optional[str], messages: Iterable[Any]) -> str:
    """Build the deterministic file list for a new checkpoint.

    Unions the list recorded in the prior checkpoint with the files named by
    this span's tool calls. A file that was modified is listed only under
    Modified. Research: file state tracked from tool calls, not left to the
    summarizer ("artifact trail" was the weakest dimension in Factory's
    evaluation; pi records read and modified files the same way).
    """
    prior_read, prior_modified = _parse_files_section(prior_summary or "")
    new_read, new_modified = _collect_tool_call_files(messages)
    modified = _merge_recent(prior_modified, new_modified, _MAX_FILES_PER_LIST)
    modified_set = set(modified)
    read = [
        p
        for p in _merge_recent(prior_read, new_read, _MAX_FILES_PER_LIST * 2)
        if p not in modified_set
    ][-_MAX_FILES_PER_LIST:]
    if not read and not modified:
        return ""
    return "\n".join(
        [
            FILES_SECTION_HEADING,
            f"- Read: {', '.join(read) if read else '(none)'}",
            f"- Modified: {', '.join(modified) if modified else '(none)'}",
        ]
    )


def is_overflow_error(exc: BaseException) -> bool:
    """True when the error chain indicates a context-window overflow.

    These are deterministic (a planned call that overflows will overflow on
    every retry), so the fold loop must NOT retry them. Recognizes the typed
    ``ContextOverflowError`` anywhere in the cause chain, the synthetic
    HTTP 413 the capture client returns (``code: context_overflow`` — see
    ``src/shared/runtime/llm/reasoning_chat.py``), and the AuxiliaryLLM pre-flight guard.
    """
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__
        if name in ("ContextOverflowError", "AuxInputTooLarge"):
            return True
        if getattr(current, "status_code", None) == 413:
            return True
        if (
            isinstance(current, BadRequestError)
            and str(getattr(current, "code", "") or "").lower() == "invalid_json_schema"
        ):
            return True
        if isinstance(current, ValidationError):
            return True
        text = str(current)
        if "context_overflow" in text or "exceeds limit of" in text:
            return True
        current = current.__cause__ or current.__context__
    return False


def _describe_exc(exc: Optional[BaseException]) -> str:
    """Readable exception text — names the type when ``str()`` is empty.

    ``asyncio.TimeoutError`` (and other arg-less exceptions) stringify to "",
    which logged as the infamous ``failed ()`` during the 5dbb5770 incident,
    hiding that the aux model was timing out on base64-laden folds.
    """
    if exc is None:
        return "unknown error"
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def load_summarizer_prompt(config: Any) -> Optional[str]:
    """The summarization prompt variant for the model that writes the summary.

    That is the dedicated auxiliary model when one is configured and enabled,
    otherwise the summarization model. (The worker used to resolve the variant
    from ``llm.summarization`` even when a different auxiliary model wrote the
    summary, and sessions and subagents loaded none at all.) Returns None when
    the config cannot resolve a prompt, so a caller never fails to start over
    it; the summarizer then runs with the task's own instructions only.
    """
    try:
        from shared.runtime.core.loader import load_summarization_prompt

        aux = getattr(config, "auxiliary", None)
        aux_model = (
            getattr(aux, "model", None) if getattr(aux, "enabled", False) else None
        )
        model = (
            aux_model
            or config.llm.get_phase_config("summarization").model
            or config.llm.model
        )
        prompt = load_summarization_prompt(config, model=model or "")
        return prompt if isinstance(prompt, str) and prompt.strip() else None
    except Exception as e:
        logger.warning(f"Summarization prompt could not be loaded: {e}")
        return None


class SummarizationEngine:
    """Plan-then-fold summarization sized for the auxiliary model's window."""

    def __init__(
        self,
        auxiliary,
        *,
        summarization_prompt: Optional[str] = None,
        max_summary_length: int = 10000,
        call_timeout: float = 240.0,
        progress_cb: Optional[ProgressCallback] = None,
        counting_model: Optional[str] = None,
        token_counter: Optional[Callable[[str], int]] = None,
    ):
        """
        Args:
            auxiliary: AuxiliaryLLM instance. Its ``max_context_tokens``
                (resolved from the aux model's settings at construction) is
                the budgeting authority.
            summarization_prompt: Pre-rendered prompt template (may be None).
            max_summary_length: Max summary length in characters; the task
                asks for about a quarter of it in tokens, and it bounds the
                running summary.
            call_timeout: Per fold-call timeout in seconds. Replaces the old
                single 600s blob — N passes get N bounded calls.
            progress_cb: Optional async ``(event_name, params)`` callback.
            counting_model: Model name for tokenizer selection (best effort).
            token_counter: Override for text token counting (tests).
        """
        self.auxiliary = auxiliary
        self.summarization_prompt = summarization_prompt
        self.max_summary_length = max_summary_length
        self.call_timeout = call_timeout
        self.progress_cb = progress_cb
        self.counting_model = counting_model
        self._token_counter = token_counter

        window = getattr(auxiliary, "max_context_tokens", None)
        if not window or window <= 0:
            logger.warning(
                "SummarizationEngine: auxiliary model window unknown — "
                f"falling back to conservative {DEFAULT_AUX_WINDOW} tokens"
            )
            window = DEFAULT_AUX_WINDOW
        self.aux_window = int(window)

        # Output budget: the summary is bounded by max_summary_length chars.
        # ~3 chars/token is deliberately conservative (reserves more room).
        self.output_budget = max(1_000, math.ceil(max_summary_length / 3))
        self._overhead = self._measure_overhead()

        # The fold subtracts the output budget twice — once for the model's
        # output, once because the rolling summary rides along every chunk — so
        # both reserves are ``output_budget``. No overlap: sequential coherence
        # covers chunk seams. These arguments make the planner byte-for-byte
        # identical to the pre-extraction packer.
        self._planner = ChunkPlanner(
            self.aux_window,
            overhead_tokens=self._overhead,
            output_reserve=self.output_budget,
            carry_reserve=self.output_budget,
            overlap_ratio=0.0,
            token_counter=self._token_counter,
            counting_model=self.counting_model,
        )

    # ------------------------------------------------------------------
    # Budget + plan
    # ------------------------------------------------------------------

    def _count(self, text: str) -> int:
        if self._token_counter is not None:
            return self._token_counter(text)
        return count_text_tokens(text, self.counting_model)

    def _measure_overhead(self) -> int:
        """Measure the fixed prompt cost of a fold call, plus an allowance.

        Counts the system prompt and the user-message scaffolding with the
        merge contract present (the worst case). The prior summary itself is
        covered by the planner's carry reserve; the allowance covers a
        ``/compact`` focus and tokenizer differences.
        """
        try:
            from shared.runtime.services.auxiliary import SummarizeTask

            probe = SummarizeTask(
                conversation_text="",
                summarization_prompt=self.summarization_prompt or "",
                max_summary_length=self.max_summary_length,
                prior_summary=" ",
            )
            prompt_tokens = self._count(probe.system_prompt) + self._count(
                probe.build_context()
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.debug(f"Prompt overhead probe failed, using fixed value: {e}")
            prompt_tokens = 3_000
        return prompt_tokens + SCHEMA_OVERHEAD_TOKENS

    @property
    def chunk_budget(self) -> int:
        """Max input tokens of conversation text per fold call (delegated)."""
        return self._planner.chunk_budget

    def plan(self, formatted_parts: List[str]) -> ChunkPlan:
        """Pack formatted conversation parts into within-budget fold chunks.

        Thin delegator to the shared :class:`ChunkPlanner` (pure, deterministic,
        no LLM calls). Oversized single parts are hard-split so no chunk exceeds
        the budget.

        Raises:
            SummarizationFailed: ``aux_window_too_small`` when the window
                cannot fit even the prompt + reserves.
        """
        return self._planner.plan(formatted_parts)

    # ------------------------------------------------------------------
    # Fold loop
    # ------------------------------------------------------------------

    async def run(
        self,
        plan: ChunkPlan,
        *,
        seed_summary: Optional[str] = None,
        focus: Optional[str] = None,
    ) -> str:
        """Execute the fold loop over a plan; returns the final summary.

        Args:
            plan: Output of :meth:`plan`.
            seed_summary: Prior summary text to merge into the first pass
                (rolling-summary continuation). Later passes carry the
                running summary the same way, under the merge contract.
            focus: Optional user-provided compaction focus (``/compact
                <focus>``), honored in every fold call.

        Raises:
            SummarizationFailed: when any pass exhausts retries (the caller
                must keep the original messages).
            asyncio.CancelledError: passed through untouched so hard
                interrupts keep working.
        """
        if not plan.chunks:
            raise SummarizationFailed("empty_plan", "Nothing to summarize")
        if self.auxiliary is None:
            # Fast-fail: no auxiliary model means no fold call can ever
            # succeed, so the retry burn (5 s + 15 s per pass, observed as a
            # 20 s stall per turn in the U0 subagent spike, scenario E) would
            # only delay the same "keep the raw history" outcome.
            raise SummarizationFailed(
                "aux_unavailable",
                "no auxiliary LLM configured for summarization",
                pass_index=1,
                n_passes=plan.n_passes,
            )

        summary = strip_files_section(seed_summary) if seed_summary else None
        for chunk in plan.chunks:
            await self._emit_progress(plan, chunk, attempt=1, out_tokens=None)
            summary = await self._call_with_retries(
                chunk.text, plan, chunk, prior_summary=summary, focus=focus
            )
            await self._emit_progress(
                plan, chunk, attempt=1, out_tokens=self._count(summary)
            )

        return summary or ""

    async def _call_with_retries(
        self,
        conversation_text: str,
        plan: ChunkPlan,
        chunk: Chunk,
        *,
        prior_summary: Optional[str] = None,
        focus: Optional[str] = None,
    ) -> str:
        last_error: Optional[BaseException] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return await self._call_once(
                    conversation_text, prior_summary=prior_summary, focus=focus
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if is_overflow_error(e):
                    # Deterministic — a planned call must never overflow.
                    logger.error(
                        f"Summarization pass {chunk.index}/{plan.n_passes} "
                        f"overflowed despite planning ({chunk.tokens} tokens, "
                        f"budget {plan.chunk_budget}, window {plan.aux_window}): {e}"
                    )
                    raise SummarizationFailed(
                        "aux_overflow",
                        str(e),
                        pass_index=chunk.index,
                        n_passes=plan.n_passes,
                    ) from e
                last_error = e
                if attempt == MAX_ATTEMPTS:
                    break
                backoff = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
                logger.warning(
                    f"Summarization pass {chunk.index}/{plan.n_passes} attempt "
                    f"{attempt}/{MAX_ATTEMPTS} failed ({_describe_exc(e)}); "
                    f"retrying in {backoff}s"
                )
                await self._emit_progress(
                    plan, chunk, attempt=attempt + 1, out_tokens=None
                )
                await asyncio.sleep(backoff)

        logger.error(
            f"Summarization pass {chunk.index}/{plan.n_passes} failed after "
            f"{MAX_ATTEMPTS} attempts: {_describe_exc(last_error)}"
        )
        raise SummarizationFailed(
            "summary_rejected"
            if isinstance(last_error, SummaryRejected)
            else "aux_unavailable",
            str(last_error),
            pass_index=chunk.index,
            n_passes=plan.n_passes,
        ) from last_error

    async def _call_once(
        self,
        conversation_text: str,
        *,
        prior_summary: Optional[str] = None,
        focus: Optional[str] = None,
    ) -> str:
        """One summarization call, validated. No fallback variants."""
        from shared.runtime.services.auxiliary import SummarizeTask

        task = SummarizeTask(
            conversation_text=conversation_text,
            summarization_prompt=self.summarization_prompt or "",
            max_summary_length=self.max_summary_length,
            prior_summary=prior_summary,
            focus=focus,
        )
        # NO_RETRY: the fold loop above already owns the retry for this call
        # (MAX_ATTEMPTS + BACKOFF_SECONDS + the is_overflow_error gate). Letting
        # AuxiliaryLLM add its own would double the provider calls per fold and
        # hide the first failure from `_emit_progress`, so the cockpit would show
        # attempt 1 twice instead of 1 then 2.
        response = await self.auxiliary.complete(
            task, timeout=self.call_timeout, retry_policy=NO_RETRY
        )
        return validate_summary_message(
            response, expect_sections=task.asks_for_sections
        )

    async def _emit_progress(
        self,
        plan: ChunkPlan,
        chunk: Chunk,
        *,
        attempt: int,
        out_tokens: Optional[int],
    ) -> None:
        if self.progress_cb is None:
            return
        try:
            await self.progress_cb(
                "compaction.progress",
                {
                    "pass": chunk.index,
                    "n_passes": plan.n_passes,
                    "first_msg": chunk.first_part,
                    "last_msg": chunk.last_part,
                    "in_tokens": chunk.tokens,
                    "out_tokens": out_tokens,
                    "stage": "summarizing",
                    "attempt": attempt,
                },
            )
        except Exception as e:  # progress must never break the fold
            logger.debug(f"Compaction progress emit failed (non-fatal): {e}")
