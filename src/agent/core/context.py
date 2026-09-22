"""Context management for Universal Agent.

Implements context compaction and summarization strategies to keep
the conversation lean while preserving agent effectiveness.

Key strategies (in order of preference):
1. Tool result clearing - Replace old tool results with placeholders
2. Message trimming - Keep recent messages, trim older ones
3. Summarization - Use LLM to compress history when needed

References:
- Anthropic: "one of the safest, lightest touch forms of compaction"
- Phil Schmid: Context Engineering Part 2
- LangGraph: Manage Conversation History
"""

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Callable, Dict, List, Optional, Tuple

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)
from shared.runtime.core.summary_models import (
    ConversationSummary as ConversationSummary,
    IdentityAnchor as IdentityAnchor,
)

from shared.runtime.core.image_tokens import (
    content_to_summary_text,
    estimate_image_block_tokens,
    has_image_content,
    split_text_and_image_blocks,
    split_text_and_images,
)
from shared.runtime.core.message_markers import (
    is_pinned_for_phase,
    is_protected_message,
    mark_compaction_view,
    protected_identity,
)

logger = logging.getLogger(__name__)


def extract_summary_text(messages: List[BaseMessage]) -> Optional[str]:
    """Return the most recent '[Summary of prior work]' summary content with the
    prefix stripped, or None when no summary is present.

    The summary is written by ``summarize_and_compact`` as a
    ``SystemMessage(content=f"[Summary of prior work]\\n{summary}")``. Callers use
    this to surface the summary for the compaction display banner after
    ``ensure_within_limits`` / ``summarize_and_compact`` runs.
    """
    prefix = "[Summary of prior work]"
    for m in reversed(messages):
        content = getattr(m, "content", None)
        if (
            isinstance(m, SystemMessage)
            and isinstance(content, str)
            and prefix in content
        ):
            return content.split(prefix, 1)[1].strip()
    return None


def fresh_protected_copy(
    message: BaseMessage, *, preserve_identity: bool = False
) -> BaseMessage:
    """Copy of a protected message without an id, markers intact.

    A copy without an id is *appended* by LangGraph's ``add_messages``
    reducer (the original is evicted by its ``RemoveMessage`` marker), which
    is how a re-seated block moves from the summarised region to its new
    position right after the summary.

    ``preserve_identity`` returns the original object instead: a caller that
    replaces its history wholesale (the persistent loop) has no reducer to
    satisfy, and a re-seated message must keep the id its persisted row and
    its turn stamp are keyed by.
    """
    if preserve_identity:
        return message
    if isinstance(message, HumanMessage):
        return HumanMessage(
            content=message.content,
            additional_kwargs=dict(message.additional_kwargs or {}),
        )
    return message.model_copy(update={"id": None})


def select_pinned_for_reseat(
    summarized: List[BaseMessage],
    kept: List[BaseMessage],
    current_phase_key: Optional[str],
    *,
    preserve_identity: bool = False,
) -> List[BaseMessage]:
    """Fresh copies of the protected messages in ``summarized`` that must survive.

    A protected message survives a summary when it is a generic pin (no
    phase key) or bound to ``current_phase_key``; earlier phases' blocks are
    summarised away with their region. Copies are de-duplicated by
    ``protected_identity`` against ``kept`` (the window that follows the
    summary) and against each other, in original order. With
    ``preserve_identity`` the originals are re-seated instead of copies.
    """
    present = {protected_identity(m) for m in kept if is_protected_message(m)}
    pinned: List[BaseMessage] = []
    for msg in summarized:
        if not is_pinned_for_phase(msg, current_phase_key):
            continue
        identity = protected_identity(msg)
        if identity in present:
            continue
        present.add(identity)
        pinned.append(fresh_protected_copy(msg, preserve_identity=preserve_identity))
    return pinned


def place_pinned_after_summary(
    summary_msg: BaseMessage,
    summarized: List[BaseMessage],
    kept: List[BaseMessage],
    current_phase_key: Optional[str],
    *,
    preserve_identity: bool = False,
) -> List[BaseMessage]:
    """``[summary, *re-seated protected blocks, *kept]`` — the compacted tail.

    The current phase's instruction block goes immediately after the
    ``[Summary of prior work]`` message and before the kept window, so the
    model reads "what happened" and then "how this phase works" ahead of
    the live turns. See :func:`select_pinned_for_reseat` for the rule.
    """
    pinned = select_pinned_for_reseat(
        summarized, kept, current_phase_key, preserve_identity=preserve_identity
    )
    if pinned:
        logger.info(
            f"Re-seated {len(pinned)} protected message(s) after the summary "
            f"(phase key {current_phase_key!r})"
        )
    return [summary_msg, *pinned, *kept]


def find_safe_slice_start(messages: List[BaseMessage], target_start: int) -> int:
    """Find a safe starting index that doesn't orphan ToolMessages.

    When slicing messages, we must ensure that if we include a ToolMessage,
    we also include its corresponding AIMessage with the tool_call.
    This function adjusts the start index backwards if needed.

    Args:
        messages: Full message list
        target_start: Desired start index

    Returns:
        Adjusted start index that won't orphan ToolMessages
    """
    if target_start <= 0:
        return 0

    if target_start >= len(messages):
        return len(messages)

    # If the message at target_start is a ToolMessage, we need to find
    # the preceding AIMessage that contains the tool_call
    adjusted_start = target_start

    # Check if we're starting at or near ToolMessages that would be orphaned
    # Walk backwards to find a safe boundary
    while adjusted_start > 0:
        msg = messages[adjusted_start]

        if isinstance(msg, ToolMessage):
            # This ToolMessage needs its parent AIMessage
            # Look backwards for the AIMessage with matching tool_call
            tool_call_id = getattr(msg, "tool_call_id", None)

            if tool_call_id:
                for i in range(adjusted_start - 1, -1, -1):
                    prev_msg = messages[i]
                    if isinstance(prev_msg, AIMessage):
                        if hasattr(prev_msg, "tool_calls") and prev_msg.tool_calls:
                            # Check if this AIMessage has the matching tool_call
                            for tc in prev_msg.tool_calls:
                                if tc.get("id") == tool_call_id:
                                    # Found the parent - start from here
                                    adjusted_start = i
                                    break
                            else:
                                continue
                            break
                        # AIMessage without tool_calls - safe boundary
                        break
                    elif isinstance(prev_msg, HumanMessage):
                        # Human message - safe boundary
                        break
                else:
                    # Couldn't find parent, start from beginning
                    adjusted_start = 0
            break
        elif isinstance(msg, AIMessage):
            # Starting at an AIMessage is safe
            break
        elif isinstance(msg, HumanMessage):
            # Starting at a HumanMessage is safe
            break
        else:
            # Other message types - check the previous one
            adjusted_start -= 1

    if adjusted_start != target_start:
        logger.debug(
            f"Adjusted slice start from {target_start} to {adjusted_start} "
            "to preserve tool call pairs"
        )

    return adjusted_start


def sanitize_message_history(messages: List[BaseMessage]) -> List[BaseMessage]:
    """Remove orphaned ToolMessages that lack a parent AIMessage with tool_calls.

    This function repairs corrupted message histories where a ToolMessage
    appears without a preceding AIMessage that made the corresponding tool call.
    Such corruption can occur from improper message slicing during context compaction.

    Args:
        messages: Message list that may contain orphaned ToolMessages

    Returns:
        Sanitized message list with orphaned ToolMessages removed
    """
    if not messages:
        return messages

    # Build a set of valid tool_call_ids from AIMessages
    valid_tool_call_ids = set()
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                tc_id = tc.get("id")
                if tc_id:
                    valid_tool_call_ids.add(tc_id)

    # Filter out orphaned ToolMessages
    result = []
    orphaned_count = 0
    for msg in messages:
        if isinstance(msg, ToolMessage):
            tool_call_id = getattr(msg, "tool_call_id", None)
            if tool_call_id and tool_call_id not in valid_tool_call_ids:
                # This ToolMessage is orphaned - skip it
                orphaned_count += 1
                continue
        result.append(msg)

    if orphaned_count > 0:
        logger.warning(
            f"Removed {orphaned_count} orphaned ToolMessages from message history"
        )

    # Fix consecutive AIMessages at the end of the list.
    # Some LLM APIs (e.g., vLLM) reject requests with 2+ assistant messages
    # at the end. This can happen when orphaned ToolMessages that separated
    # AIMessages are removed above, or from synthetic AIMessages injected by
    # archive_phase/handle_transition nodes.
    if len(result) >= 2:
        trailing_ai_count = 0
        for msg in reversed(result):
            if isinstance(msg, AIMessage):
                trailing_ai_count += 1
            else:
                break
        if trailing_ai_count >= 2:
            # Insert a separator HumanMessage before the last AIMessage
            insert_pos = len(result) - 1  # before the final AIMessage
            separator = HumanMessage(content="Continue.")
            result.insert(insert_pos, separator)
            logger.warning(
                f"Inserted separator between {trailing_ai_count} consecutive "
                f"trailing AIMessages to satisfy LLM API constraints"
            )

    return result


def repair_tool_pairing(messages: List[BaseMessage]) -> List[BaseMessage]:
    """Drop tool calls/results that lost their partner.

    Both the OpenAI Responses API and Anthropic reject a history where an
    assistant tool/function call has no matching result, or a result has no
    matching call, with a 400 ("No tool call found for function call output
    ..." / "no tool output found for function call ..."). Unlike
    ``sanitize_message_history`` (which only drops orphaned results), this is
    bidirectional and also strips orphaned calls off assistant messages.

    Orphans arise from context-compaction thrash, an interrupted turn (a tool
    result that was never produced or persisted), or streamed parallel-tool
    corruption (langchain #34660). Keep only calls and results whose ids match
    on both sides; assistant messages left with neither text nor calls are
    dropped.

    Shared by the live persistent turn loop (``persistent_graph``) and the
    session resume path (``persistent_app``) so both enforce the same
    invariant before a strict-pairing API call.
    """
    call_ids = {
        tc.get("id")
        for m in messages
        if isinstance(m, AIMessage)
        for tc in (getattr(m, "tool_calls", None) or [])
        if tc.get("id")
    }
    result_ids = {
        m.tool_call_id
        for m in messages
        if isinstance(m, ToolMessage) and getattr(m, "tool_call_id", "")
    }
    valid_ids = call_ids & result_ids

    repaired: List[BaseMessage] = []
    dropped_results = 0
    stripped_calls = 0
    for m in messages:
        if isinstance(m, AIMessage):
            tool_calls = getattr(m, "tool_calls", None) or []
            kept = [tc for tc in tool_calls if tc.get("id") in valid_ids]
            if len(kept) != len(tool_calls):
                stripped_calls += len(tool_calls) - len(kept)
                # Preserve provider-native metadata, reasoning blocks, usage,
                # name and response ids. Reconstructing a bare AIMessage here
                # fixed pairing while silently discarding everything else on
                # the turn; model_copy changes only the poisoned field.
                m = m.model_copy(update={"tool_calls": kept})
            if not kept and not m.content:
                continue  # empty assistant turn carries no information
            repaired.append(m)
        elif isinstance(m, ToolMessage):
            if getattr(m, "tool_call_id", "") in valid_ids:
                repaired.append(m)
            else:
                dropped_results += 1  # orphaned result — drop
        else:
            repaired.append(m)

    if dropped_results or stripped_calls:
        logger.warning(
            f"repair_tool_pairing: dropped {dropped_results} orphaned tool "
            f"result(s), stripped {stripped_calls} orphaned tool call(s)"
        )
    return repaired


# --- Provider-boundary history sanitizer (live_session_settings.md Slice D) ---

# Reasoning/thinking content-block types that must never be replayed to a
# provider that didn't produce them. Anthropic rejects thinking blocks whose
# signature it can't validate (any cross-model replay); OpenAI-compatible
# servers 400 on unknown content-part types the langchain serializer passes
# through ("redacted_thinking", Responses-style "reasoning" blocks).
_REASONING_BLOCK_TYPES = frozenset(
    {"thinking", "redacted_thinking", "reasoning", "reasoning_content"}
)

# additional_kwargs keys that carry a captured reasoning channel. Stock
# langchain-openai never serializes them outbound, but custom transports have
# no such guarantee — and a reasoning_content + tool_calls combo crashes
# gpt-oss chat templates server-side. Foreign reasoning carries no replay
# value, so it is dropped at the boundary.
_REASONING_KWARGS_KEYS = ("reasoning_content", "reasoning", "reasoning_details")

# Mistral validates tool-call ids as exactly 9 alphanumerics; everything else
# we route to accepts [a-zA-Z0-9_-] (OpenAI mints ~29-30 chars, Anthropic
# toolu_ + 24). 40 is a conservative common bound observed across providers.
_MISTRAL_TOOL_ID_RE = re.compile(r"^[a-zA-Z0-9]{9}$")
_GENERIC_TOOL_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,40}$")


def _conforming_tool_call_id(raw_id: str, target_family: str) -> str:
    """Return ``raw_id`` unchanged when the target accepts it, else a
    deterministic replacement derived from its hash.

    Deterministic (pure function of the id) so the assistant tool_calls side
    and the ToolMessage result side always map to the same value, and so a
    repeated sanitizer pass is a no-op.
    """
    if target_family == "mistral":
        if _MISTRAL_TOOL_ID_RE.match(raw_id):
            return raw_id
        return hashlib.blake2b(raw_id.encode(), digest_size=16).hexdigest()[:9]
    if _GENERIC_TOOL_ID_RE.match(raw_id):
        return raw_id
    return "call_" + hashlib.blake2b(raw_id.encode(), digest_size=16).hexdigest()[:24]


def sanitize_history_for_provider_boundary(
    messages: List[BaseMessage], target_model: str
) -> List[BaseMessage]:
    """Make a history produced under one provider/family safe to replay under
    another (live_session_settings.md Slice D — the #1 session killer).

    Three transforms, then a pairing verification:

    1. **Strip foreign reasoning**: thinking/reasoning content blocks are
       removed from list-form AIMessage content (signed Anthropic thinking
       blocks fail signature validation on any cross-model replay; unknown
       block types 400 on OpenAI-compatible servers), and captured reasoning
       keys are dropped from ``additional_kwargs``. ``invalid_tool_calls``
       (failed parses) are dropped too — they serialize outbound with no
       matching result and are pure replay risk.
    2. **Remap tool-call ids** to the target's accepted format, consistently
       on the assistant and tool-result sides (LiteLLM's
       ``_sanitize_anthropic_tool_use_id`` is the reference for the class of
       bug; Mistral's strict 9-alphanumeric format is the worst case).
    3. **Verify pairing** via :func:`repair_tool_pairing` so a strict-pairing
       API never sees an orphaned call or result.

    Operates on the in-memory working set only — persisted ``thread_messages``
    rows stay provider-native. Returns a new list; input messages are never
    mutated. Idempotent: a second pass over sanitized output is a no-op.
    """
    from shared.runtime.core.model_registry import family_of

    target_family = family_of(target_model or "")
    id_map: Dict[str, str] = {}

    def _map_id(raw_id: str) -> str:
        if not raw_id:
            return raw_id
        if raw_id not in id_map:
            id_map[raw_id] = _conforming_tool_call_id(raw_id, target_family)
        return id_map[raw_id]

    out: List[BaseMessage] = []
    stripped_blocks = 0
    stripped_kwargs = 0
    dropped_invalid = 0
    for m in messages:
        if isinstance(m, AIMessage):
            update: Dict[str, Any] = {}

            content = m.content
            if isinstance(content, list):
                kept = [
                    b
                    for b in content
                    if not (
                        isinstance(b, dict) and b.get("type") in _REASONING_BLOCK_TYPES
                    )
                ]
                if len(kept) != len(content):
                    stripped_blocks += len(content) - len(kept)
                    if all(
                        isinstance(b, dict) and b.get("type") == "text" for b in kept
                    ):
                        # Only plain text remains — collapse to the string form
                        # every provider accepts.
                        update["content"] = "".join(b.get("text", "") for b in kept)
                    else:
                        update["content"] = kept

            ak = m.additional_kwargs or {}
            removed_keys = [k for k in _REASONING_KWARGS_KEYS if k in ak]
            if removed_keys:
                stripped_kwargs += len(removed_keys)
                update["additional_kwargs"] = {
                    k: v for k, v in ak.items() if k not in _REASONING_KWARGS_KEYS
                }

            tool_calls = getattr(m, "tool_calls", None) or []
            new_calls = []
            calls_changed = False
            for tc in tool_calls:
                new_id = _map_id(tc.get("id") or "")
                if new_id != (tc.get("id") or ""):
                    calls_changed = True
                    tc = {**tc, "id": new_id}
                new_calls.append(tc)
            if calls_changed:
                update["tool_calls"] = new_calls

            if getattr(m, "invalid_tool_calls", None):
                dropped_invalid += len(m.invalid_tool_calls)
                update["invalid_tool_calls"] = []

            # A message whose content was reasoning-only ends up empty; with
            # no tool calls left it carries nothing and some providers 400 on
            # an empty assistant turn — drop it.
            final_content = update.get("content", m.content)
            if not final_content and not new_calls:
                continue

            out.append(m.model_copy(update=update) if update else m)

        elif isinstance(m, ToolMessage):
            raw_id = getattr(m, "tool_call_id", "") or ""
            new_id = _map_id(raw_id)
            if new_id != raw_id:
                m = m.model_copy(update={"tool_call_id": new_id})
            out.append(m)

        else:
            out.append(m)

    remapped = sum(1 for k, v in id_map.items() if k != v)
    if stripped_blocks or stripped_kwargs or remapped or dropped_invalid:
        logger.info(
            "Provider-boundary sanitize (target family=%s): stripped %d "
            "reasoning block(s) + %d reasoning kwarg(s), remapped %d "
            "tool-call id(s), dropped %d invalid tool call(s)",
            target_family,
            stripped_blocks,
            stripped_kwargs,
            remapped,
            dropped_invalid,
        )
    return repair_tool_pairing(out)


def _try_repair_json_object(raw: Any) -> Optional[Dict[str, Any]]:
    """Best-effort repair of a malformed JSON-object string.

    Handles the shapes models actually emit when a generation degrades:
    prose/markdown fences around the object, trailing commas, and mid-string
    truncation. Returns the parsed dict, or None if the string can't be
    coaxed into a JSON object (non-object JSON also returns None — tool
    arguments must be objects).
    """
    if not isinstance(raw, str) or not raw.strip():
        return None
    s = raw.strip()

    def _loads(text: str) -> Optional[Dict[str, Any]]:
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None

    parsed = _loads(s)
    if parsed is not None:
        return parsed

    # Trim prose/fences around the outermost object.
    start = s.find("{")
    if start == -1:
        return None
    s = s[start:]
    end = s.rfind("}")
    if end != -1:
        parsed = _loads(s[: end + 1])
        if parsed is not None:
            return parsed

    # Trailing commas: {"a": 1,} / [1, 2,].
    s = re.sub(r",\s*([}\]])", r"\1", s)
    parsed = _loads(s)
    if parsed is not None:
        return parsed

    # Truncation: close an open string, then unwind the bracket stack.
    stack: List[str] = []
    in_string = False
    escaped = False
    for ch in s:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "{[":
            stack.append("}" if ch == "{" else "]")
        elif ch in "}]" and stack:
            stack.pop()
    candidate = s + ('"' if in_string else "") + "".join(reversed(stack))
    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)
    return _loads(candidate)


def repair_tool_call_arguments(msg: BaseMessage, *, note: bool = True) -> BaseMessage:
    """Repair or scrub tool calls whose arguments are not valid JSON.

    Models occasionally emit a tool call whose ``arguments`` string is broken
    JSON (observed from MiniMax-M3 after a 441s generation — the 2026-07-11
    job `6a186c76` incident). LangChain parks the parse failure in
    ``invalid_tool_calls``, but the RAW malformed string survives in
    ``additional_kwargs["tool_calls"]`` — and once checkpointed it is
    replayed to the provider on every subsequent request. MiniMax validates
    history tool calls on input and rejects its own prior output with a
    deterministic ``400 bad_request_error: invalid function arguments json
    string``, permanently poisoning the job/session
    (knowledge-base/knowledge/features/outbound_message_hygiene.md).

    Repair-first: a call whose arguments ``_try_repair_json_object`` can fix
    is promoted into ``tool_calls`` (raw entry rewritten) so it executes
    normally. Unrepairable calls are removed everywhere; with ``note=True``
    (ingestion) a short note is appended to the message content so the model
    knows the call was discarded. Pairing stays intact by construction — an
    invalid call never had a result message.

    Mutates ``msg`` in place (AIMessage and AIMessageChunk) and returns it.
    No-ops on non-AI messages and well-formed messages.
    """
    if not isinstance(msg, AIMessage):
        return msg

    kwargs = getattr(msg, "additional_kwargs", None)
    raw_entries = kwargs.get("tool_calls") if isinstance(kwargs, dict) else None
    if not isinstance(raw_entries, list):
        raw_entries = None

    repaired_names: List[str] = []
    dropped_names: List[str] = []

    def _raw_rewrite(call_id: Optional[str], fixed: Dict[str, Any]) -> None:
        for entry in raw_entries or []:
            fn = entry.get("function") if isinstance(entry, dict) else None
            if isinstance(fn, dict) and (call_id is None or entry.get("id") == call_id):
                if (
                    call_id is not None
                    or _try_repair_json_object(fn.get("arguments")) == fixed
                ):
                    fn["arguments"] = json.dumps(fixed)

    def _raw_remove(call_id: Optional[str]) -> None:
        if raw_entries is None:
            return
        if call_id is not None:
            raw_entries[:] = [
                e
                for e in raw_entries
                if not (isinstance(e, dict) and e.get("id") == call_id)
            ]

    # 1. LangChain-detected parse failures.
    for itc in list(getattr(msg, "invalid_tool_calls", None) or []):
        name = itc.get("name") or "unknown_tool"
        call_id = itc.get("id")
        fixed = _try_repair_json_object(itc.get("args"))
        if fixed is not None:
            msg.tool_calls = list(msg.tool_calls or []) + [
                {"name": name, "args": fixed, "id": call_id, "type": "tool_call"}
            ]
            _raw_rewrite(call_id, fixed)
            repaired_names.append(name)
        else:
            _raw_remove(call_id)
            dropped_names.append(name)
    if getattr(msg, "invalid_tool_calls", None):
        msg.invalid_tool_calls = []

    # 2. Raw-entry sweep — catches malformed arguments that bypassed
    #    LangChain's parser (checkpoint round-trips, provider quirks).
    for entry in list(raw_entries or []):
        fn = entry.get("function") if isinstance(entry, dict) else None
        raw_args = fn.get("arguments") if isinstance(fn, dict) else None
        if not isinstance(raw_args, str):
            continue
        try:
            if isinstance(json.loads(raw_args), dict):
                continue
        except (ValueError, TypeError):
            pass
        name = fn.get("name") or "unknown_tool"
        fixed = _try_repair_json_object(raw_args)
        if fixed is not None:
            fn["arguments"] = json.dumps(fixed)
            repaired_names.append(name)
        else:
            raw_entries.remove(entry)
            call_id = entry.get("id")
            if call_id and msg.tool_calls:
                msg.tool_calls = [
                    tc for tc in msg.tool_calls if tc.get("id") != call_id
                ]
            dropped_names.append(name)

    if not repaired_names and not dropped_names:
        return msg

    logger.warning(
        f"repair_tool_call_arguments: repaired {repaired_names or 'none'}, "
        f"discarded {dropped_names or 'none'} (malformed JSON arguments)"
    )
    if dropped_names:
        notice = (
            f"[system note: {len(dropped_names)} tool call(s) "
            f"({', '.join(dropped_names)}) had unparseable JSON arguments and "
            f"were discarded — re-issue with valid JSON if still needed]"
        )
        if note and isinstance(msg.content, str):
            msg.content = f"{msg.content}\n\n{notice}" if msg.content else notice
        elif not msg.content and not msg.tool_calls:
            # History scrub of a now-empty assistant message: keep a stub so
            # providers that reject empty assistant turns don't 400.
            msg.content = notice
    return msg


def scrub_history_tool_call_arguments(
    messages: List[BaseMessage],
) -> List[BaseMessage]:
    """Send-time backstop: run the argument repair over every AIMessage.

    Catches poison already sitting in old checkpoints (jobs/sessions frozen
    before the ingestion repair shipped) and anything a future ingestion-path
    bug lets through. Cost: one ``json.loads`` per historical raw tool call —
    negligible against an LLM round-trip. Mutations self-heal the checkpoint
    at its next write.
    """
    for m in messages:
        if isinstance(m, AIMessage):
            repair_tool_call_arguments(m, note=False)
    return messages


# Try to import tiktoken for accurate token counting
try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    logger.warning("tiktoken not available, using approximate token counting")


@dataclass
class ContextManagementState:
    """State for context management tracking.

    Stored in agent metadata to track context management operations.
    """

    total_tool_results_cleared: int = 0
    total_messages_trimmed: int = 0
    total_summarizations: int = 0
    current_token_count: int = 0
    # Real input_tokens of the last provider call — the compaction-trigger
    # anchor (context_token_accounting.md S1). None until the first response.
    last_provider_input_tokens: Optional[int] = None
    summaries: List[str] = field(default_factory=list)
    last_compaction_iteration: int = 0


@dataclass
class ContextConfig:
    """Configuration for context management.

    Attributes:
        compaction_threshold_tokens: Trigger compaction when context exceeds this
        summarization_threshold_tokens: Trigger summarization when exceeds this
        message_count_threshold: Message count threshold for alternate summarization trigger
        message_count_min_tokens: Minimum tokens required when using message count threshold
        keep_recent_tool_results: Number of recent tool results to keep in full
        keep_recent_messages: Number of recent messages to preserve
        max_tool_result_length: Max chars for truncated tool results
        placeholder_text: Text to use when replacing cleared tool results
        tool_retry_count: Number of retries for failed tool calls
        tool_retry_delay_seconds: Delay between retries
        model_max_context_tokens: Hard limit for model context window
        preserve_tool_names: Tool names whose results are kept verbatim even when
            older than keep_recent_tool_results — evidence of side effects the
            strategic phase audit needs to cite.
        preserve_content_patterns: Case-insensitive substrings that, if present in
            a tool result, protect it from recency-based clearing/truncation —
            errors, exceptions, missing-file markers.
    """

    compaction_threshold_tokens: int = 80_000
    summarization_threshold_tokens: int = 100_000
    message_count_threshold: int = 200
    # Floored at the compaction threshold so the message-count branch of
    # should_summarize() can never fire before the token gate. Mirrors
    # loader.MESSAGE_COUNT_MIN_FRACTION (a base=100_000 instance).
    message_count_min_tokens: int = 80_000
    keep_recent_tool_results: int = 15
    keep_recent_messages: int = 10
    max_tool_result_length: int = 5000
    keep_window_max_tool_result_chars: int = 16000
    placeholder_text: str = "[Result processed - see workspace if needed]"
    tool_retry_count: int = 3
    tool_retry_delay_seconds: float = 1.0
    # Safety layer constant — a base=100_000 instance of the limit fractions in
    # src/core/loader.py; fallback-only, the real value comes from the matrix
    # derivation. Summarization budgets are deliberately NOT config leaves:
    # they are computed at call time from the auxiliary model's own window
    # (src/core/summarizer.py) — deriving them from the MAIN model's window
    # sent 951k-token payloads to a 131k summarizer
    # (knowledge-base/knowledge/features/context_summarization_rework.md).
    model_max_context_tokens: int = 100_000
    # Per-family image-token estimator config (matrix settings.image_tokens via
    # LimitsConfig). None -> flat DEFAULT_IMAGE_TOKENS per image. S4.
    image_tokens: Optional[Dict[str, Any]] = None
    # Evidence-preservation filter: side effects and failures survive compaction
    # so the strategic-phase audit protocol can cite verbatim tool output.
    preserve_tool_names: Tuple[str, ...] = (
        "write_file",
        "edit_file",
        "patch_file",
        "patch_tool",
    )
    # Failure-content preservation is OFF. It existed to feed the strategic
    # <phase_audit_protocol>, which was deleted in 4eba5d47 — the consumer is
    # gone but the unbounded retention stayed, and it is actively harmful:
    # keeping an agent's own old error traces in context is the measured
    # "self-conditioning" effect (arXiv 2509.09677 — accuracy falls from ~70%
    # to ~15% as induced past errors rise, and it does not diminish with model
    # scale). Recent failures are still visible: keep_recent_tool_results
    # retains the last N results verbatim regardless of content. This carve-out
    # only ever governed results OLDER than that window, which are exactly the
    # ones that should decay to a placeholder. Restore by re-adding patterns.
    preserve_content_patterns: Tuple[str, ...] = ()


def count_tokens_tiktoken(
    messages: List[BaseMessage],
    model: str = "gpt-4",
    image_config: Optional[Dict[str, Any]] = None,
) -> int:
    """Count tokens using tiktoken for accurate counting.

    Args:
        messages: List of messages to count
        model: Model name for tokenizer selection
        image_config: Resolved per-family image-token estimator config; image
            blocks are billed by their pixel dimensions, never their base64.

    Returns:
        Token count
    """
    if not TIKTOKEN_AVAILABLE:
        return count_tokens_approximate(messages, image_config)

    try:
        # Try to get encoding for specific model
        # Strip provider prefix (e.g., "openai/gpt-oss-120b" -> "gpt-oss-120b")
        model_name = model.split("/")[-1] if "/" in model else model
        try:
            enc = tiktoken.encoding_for_model(model_name)
            logger.debug(f"Using tiktoken encoding for model {model_name}: {enc.name}")
        except KeyError:
            # Fall back to cl100k_base (used by GPT-4)
            enc = tiktoken.get_encoding("cl100k_base")
            logger.debug(
                f"Model {model_name} not found in tiktoken, using cl100k_base. "
                "Token counts may over/undercount for non-OpenAI models."
            )

        total = 0
        debug_details = []
        for i, msg in enumerate(messages):
            msg_tokens = 0
            # Count message content. Multimodal content is a list of blocks;
            # split out image blocks and count them as flat image tokens
            # instead of tokenizing their base64 data URL as text (which
            # inflated session 5dbb5770 to ~9.2M phantom tokens). See
            # src/core/image_tokens.py + knowledge-history/done/context_token_accounting.md.
            text, image_blocks = split_text_and_image_blocks(msg.content)
            content_tokens = len(enc.encode(text, disallowed_special=()))
            image_tokens = sum(
                estimate_image_block_tokens(b, image_config) for b in image_blocks
            )
            n_images = len(image_blocks)
            msg_tokens += content_tokens + image_tokens

            # Count tool calls if present
            tool_call_tokens = 0
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                tool_call_tokens = len(
                    enc.encode(str(msg.tool_calls), disallowed_special=())
                )
                msg_tokens += tool_call_tokens

            # Add overhead for message structure (role, etc.)
            msg_tokens += 4  # Approximate overhead per message
            total += msg_tokens

            # Log large messages
            if msg_tokens > 1000:
                msg_type = type(msg).__name__
                debug_details.append(
                    f"[{i}] {msg_type}: {content_tokens}t content, "
                    f"{image_tokens}t images ({n_images}), {tool_call_tokens}t tools, "
                    f"{len(text)} chars"
                )

        if (
            debug_details
            and total > 50000
            and os.getenv("DEBUG_TOKEN_BREAKDOWN", "").strip() in ("1", "true")
        ):
            logger.debug(
                f"Token count breakdown ({len(messages)} msgs, {total} total tokens):\n  "
                + "\n  ".join(debug_details)
            )

        return total

    except Exception as e:
        logger.warning(f"tiktoken error, falling back to approximate: {e}")
        return count_tokens_approximate(messages, image_config)


def count_tokens_approximate(
    messages: List[BaseMessage],
    image_config: Optional[Dict[str, Any]] = None,
) -> int:
    """Approximate token count using character-based estimation.

    Uses ~4 characters per token as a rough estimate.
    This is a fallback when tiktoken is not available.

    Args:
        messages: List of messages to count
        image_config: Resolved per-family image-token estimator config.

    Returns:
        Approximate token count
    """
    total_chars = 0
    total_image_tokens = 0
    for msg in messages:
        text, image_blocks = split_text_and_image_blocks(msg.content)
        total_chars += len(text)
        total_image_tokens += sum(
            estimate_image_block_tokens(b, image_config) for b in image_blocks
        )

        # Add tool calls if present
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            total_chars += len(str(msg.tool_calls))

    # ~4 chars per token on average, plus flat per-image tokens
    return total_chars // 4 + total_image_tokens


def get_token_counter(
    model: str = "gpt-4",
    image_config: Optional[Dict[str, Any]] = None,
) -> Callable[[List[BaseMessage]], int]:
    """Get the appropriate token counter function.

    Args:
        model: Model name for tokenizer selection
        image_config: Resolved per-family image-token estimator config, bound
            into the returned counter so multimodal blocks are billed by their
            pixel dimensions rather than their base64 length.

    Returns:
        Token counter function
    """
    if TIKTOKEN_AVAILABLE:
        return lambda msgs: count_tokens_tiktoken(msgs, model, image_config)
    return lambda msgs: count_tokens_approximate(msgs, image_config)


class ContextManager:
    """Manages context window for the Universal Agent.

    Implements a multi-tier context management strategy:
    1. Tool result clearing (lowest impact, highest benefit)
    2. Message trimming (moderate impact)
    3. LLM summarization (highest impact, preserves meaning)

    Example:
        ```python
        config = ContextConfig(
            compaction_threshold_tokens=80000,
            keep_recent_tool_results=5,
        )
        context_mgr = ContextManager(config=config)

        # In graph process node
        prepared = context_mgr.prepare_messages_for_llm(messages)
        response = llm.invoke(prepared)

        # Check if summarization needed
        if context_mgr.should_summarize(messages):
            messages = await context_mgr.summarize_and_compact(messages, llm)
        ```
    """

    def __init__(
        self,
        config: Optional[ContextConfig] = None,
        model: str = "gpt-4",
        strategic_model: Optional[str] = None,
        tactical_model: Optional[str] = None,
        summarization_call_timeout: float = 240.0,
        preserve_message_identity: bool = False,
    ):
        """Initialize context manager.

        Args:
            config: Context management configuration
            model: Model name for token counting (default/fallback)
            strategic_model: Model name for strategic phase token counting
            tactical_model: Model name for tactical phase token counting
            summarization_call_timeout: Per fold-call timeout in seconds for
                summarization LLM calls (N passes get N bounded calls, not
                one shared blob — see src/agent/core/summarizer.py)
            preserve_message_identity: Keep the kept window's message
                objects (ids, ``additional_kwargs``) through a summary
                instead of re-creating them as id-less fresh copies. The
                fresh copies exist for LangGraph's ``add_messages`` reducer
                (evict by ``RemoveMessage``, append the copy after the
                summary); a caller that replaces its history wholesale — the
                persistent session loop — has no reducer and needs the ids
                so its per-row upsert (``ON CONFLICT (id)``) and its turn
                stamps survive compaction. See
                knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md.
        """
        self.config = config or ContextConfig()
        self.preserve_message_identity = bool(preserve_message_identity)
        # getattr-guarded: some callers pass a non-ContextConfig (e.g. tests
        # hand the whole AgentConfig). image_tokens is an optional new field —
        # absent -> flat estimation, never a constructor crash.
        image_config = getattr(self.config, "image_tokens", None)
        self._default_counter = get_token_counter(model, image_config)
        self._phase_counters: Dict[str, Callable[[List[BaseMessage]], int]] = {}
        if strategic_model:
            self._phase_counters["strategic"] = get_token_counter(
                strategic_model, image_config
            )
        if tactical_model:
            self._phase_counters["tactical"] = get_token_counter(
                tactical_model, image_config
            )
        self.token_counter = self._default_counter
        self._state = ContextManagementState()
        self._summarization_call_timeout = summarization_call_timeout
        # Optional async (event_name, params) callback for compaction
        # progress (knowledge-base/knowledge/features/context_summarization_rework.md). The
        # transport decides rendering: persistent sessions broadcast SSE
        # frames, the worker graph logs. None = silent.
        self.progress_cb: Optional[Callable[[str, Dict[str, Any]], Any]] = None
        # Stats of the most recent successful summarization, read by the
        # transport into the context.compacted completion event.
        self._last_summarization_stats: Optional[Dict[str, Any]] = None
        # "<n>:<phase>" of the running phase (set_current_phase): the key a
        # protected phase block must carry to be re-seated after a summary.
        # None (sessions, resume nodes) re-seats generic pins only.
        self._current_phase_key: Optional[str] = None
        # Set by summarize_and_compact to the id of the last message the newest
        # summary covers (the summarized/kept boundary). The persistent-session
        # transport reads it to record a message-granular `boundary_seq` on the
        # summary row so resume loads `summary + messages after the boundary`
        # instead of whole post-boundary turns. None when the last call did not
        # actually compact. See knowledge-base/knowledge/issues/persistent_session_midturn_message_loss.md.
        self._last_compaction_boundary_id: Optional[str] = None
        # Monotonic count of *successful* summarizations. Transports snapshot
        # it around a compaction call to know whether a summary was actually
        # produced this call — the authoritative signal, replacing length-delta
        # heuristics that false-fire on stray RemoveMessage markers (the
        # duplicate-banner bug, 2026-06-12).
        self.compaction_runs: int = 0

    def _note_compaction_success(self) -> None:
        """Bump the run counter and invalidate the provider-usage anchor.

        The anchor (``last_provider_input_tokens``) describes a context that
        no longer exists once a compaction lands — left in place it keeps the
        trigger (``max(local, anchor)``) above threshold and re-fires a
        needless second compaction on the next check until an LLM call heals
        it (worst on a model hot-swap, where the pre-swap fit compaction is
        followed immediately by the first post-swap turn's check).
        """
        self.compaction_runs += 1
        self._state.last_provider_input_tokens = None

    def _as_compaction_view(
        self, original: BaseMessage, view: BaseMessage
    ) -> BaseMessage:
        """Tag a lossy rewrite of ``original`` for the session's reconcile.

        Identity-preserving callers (the persistent session) keep the
        original's markers — its turn stamp included — on the rewrite and
        mark it a compaction view (``message_markers.COMPACTION_VIEW_KEY``).
        The turn-end reconcile still owes the row when its incremental write
        was lost, and writes a view insert-if-absent: it fills a missing row
        (a tool call without its result wedges the session) and never
        overwrites the durable full row. Every caller keeps the id itself.
        The worker graph's rewrites are left exactly as they were.
        """
        if self.preserve_message_identity:
            view.additional_kwargs = dict(
                getattr(original, "additional_kwargs", None) or {}
            )
            mark_compaction_view(view)
        return view

    def update_limits(self, config: ContextConfig, model: str) -> None:
        """Rebind thresholds + token counter to a new model's window, in place.

        Called on a session model hot-swap. Mutating (rather than rebuilding)
        keeps every reference the running loop captured valid and preserves
        accumulated state — critically ``_state.last_provider_input_tokens``,
        so a downswitch to a smaller-window model sees the real context size
        and compacts on the very next turn instead of resending an oversized
        history until the model returns empty responses. See
        knowledge-history/done/session_model_switch_stale_context_manager_empty_response.md.
        """
        self.config = config
        image_config = getattr(config, "image_tokens", None)
        self._default_counter = get_token_counter(model, image_config)
        # Sessions use only the default counter (no phase counters); rebind the
        # active pointer unless a worker phase counter is currently selected.
        if self.token_counter not in self._phase_counters.values():
            self.token_counter = self._default_counter

    def set_current_phase(self, phase: str, phase_key: Optional[str] = None) -> None:
        """Record the running phase: token counter + the protected-block key.

        Falls back to the default counter if no phase-specific one exists.

        Args:
            phase: Phase name ("strategic" or "tactical")
            phase_key: ``"<phase_number>:<phase>"`` of the concrete phase
                instance. Protected messages bound to this key are re-seated
                after a summary; other phases' blocks are summarised away.
                ``None`` clears the key (sessions never set one).
        """
        self.token_counter = self._phase_counters.get(phase, self._default_counter)
        self._current_phase_key = phase_key

    @property
    def current_phase_key(self) -> Optional[str]:
        """The phase key protected blocks must carry to survive a summary."""
        return self._current_phase_key

    def set_progress_callback(
        self, callback: Optional[Callable[[str, Dict[str, Any]], Any]]
    ) -> None:
        """Install the compaction progress callback (async ``(event, params)``)."""
        self.progress_cb = callback

    async def _emit_compaction_event(self, event: str, params: Dict[str, Any]) -> None:
        """Emit a compaction lifecycle event; failures never break compaction."""
        if self.progress_cb is None:
            return
        try:
            await self.progress_cb(event, params)
        except Exception as e:
            logger.debug(f"Compaction event emit failed (non-fatal): {e}")

    @property
    def state(self) -> ContextManagementState:
        """Get current context management state."""
        return self._state

    def get_token_count(self, messages: List[BaseMessage]) -> int:
        """Get current token count for messages.

        Args:
            messages: Messages to count

        Returns:
            Token count
        """
        count = self.token_counter(messages)
        self._state.current_token_count = count
        return count

    def record_provider_usage(self, input_tokens: Optional[int]) -> None:
        """Anchor the compaction trigger on the provider's real input_tokens.

        Called after each main-model response that carries usage. The provider
        count is ground truth for the current context size (it includes the
        system prompt, tool schemas, and real image-token cost) and self-heals
        the trigger every turn. None / non-positive values are ignored so an
        empty-usage turn never clobbers the anchor.
        """
        if input_tokens and input_tokens > 0:
            self._state.last_provider_input_tokens = int(input_tokens)

    def _trigger_token_count(self, messages: List[BaseMessage]) -> int:
        """Token count for compaction-*trigger* decisions.

        The image-aware local count, floored by the last real provider
        ``input_tokens``. Both numbers are honest now that image blocks are no
        longer stringified; the floor is biased-high (it carries the
        system-prompt / tool-schema / injection overhead the bare message list
        lacks) and guards against local under-counting. The raw local count
        still drives the post-compaction elision checks against the model max —
        only the trigger uses the floor.
        """
        local = self.get_token_count(messages)
        anchor = self._state.last_provider_input_tokens or 0
        return max(local, anchor)

    def should_compact(self, messages: List[BaseMessage]) -> bool:
        """Check if context needs compaction.

        Args:
            messages: Current message history

        Returns:
            True if compaction threshold exceeded
        """
        return (
            self._trigger_token_count(messages)
            > self.config.compaction_threshold_tokens
        )

    def should_summarize(self, messages: List[BaseMessage]) -> bool:
        """Check if summarization is needed.

        Summarization is triggered when:
        1. Token count exceeds summarization_threshold_tokens, OR
        2. Message count exceeds message_count_threshold AND
           token count exceeds message_count_min_tokens

        Args:
            messages: Current message history

        Returns:
            True if summarization threshold exceeded
        """
        token_count = self._trigger_token_count(messages)
        message_count = len(messages)

        # Original threshold: high token count
        if token_count > self.config.summarization_threshold_tokens:
            return True

        # New: message count threshold with minimum token requirement
        if (
            message_count > self.config.message_count_threshold
            and token_count > self.config.message_count_min_tokens
        ):
            return True

        return False

    def _is_evidence_tool_message(self, msg: ToolMessage) -> bool:
        """Check if a tool result should survive recency-based compaction.

        Tool results that carry evidence of side effects (file writes, edits)
        or failures (errors, exceptions, missing files) are preserved so the
        strategic phase audit protocol can cite verbatim tool output after
        long tactical phases. See knowledge-base/knowledge/features/phase_audit_protocol.md.

        Args:
            msg: The ToolMessage to inspect

        Returns:
            True if the message should be preserved regardless of recency
        """
        tool_name = getattr(msg, "name", None)
        if tool_name and tool_name in self.config.preserve_tool_names:
            return True

        content = msg.content if isinstance(msg.content, str) else str(msg.content)
        lowered = content.lower()
        for pattern in self.config.preserve_content_patterns:
            if pattern in lowered:
                return True
        return False

    def clear_old_tool_results(
        self,
        messages: List[BaseMessage],
        keep_recent: Optional[int] = None,
    ) -> List[BaseMessage]:
        """Replace old tool results with placeholder text.

        This is the "safest, lightest touch form of compaction" per Anthropic.
        The agent can always re-read files from workspace if needed.

        Tool results matching the evidence filter (write-type tools, error
        content) are preserved verbatim regardless of recency so the strategic
        phase can audit the previous tactical phase's actual output.

        Args:
            messages: Message list to process
            keep_recent: Number of recent tool results to keep (default from config)

        Returns:
            Processed message list with old tool results replaced
        """
        keep_recent = keep_recent or self.config.keep_recent_tool_results

        # Count tool messages from the end (protected messages are neither
        # cleared nor counted against the window)
        tool_indices = [
            i
            for i, m in enumerate(messages)
            if isinstance(m, ToolMessage) and not is_protected_message(m)
        ]

        if not tool_indices:
            return messages

        # Determine which tool messages to clear
        num_to_clear = max(0, len(tool_indices) - keep_recent)
        indices_to_clear = set(tool_indices[:num_to_clear])

        result = []
        cleared_count = 0
        preserved_count = 0

        for i, msg in enumerate(messages):
            if i in indices_to_clear and not self._is_evidence_tool_message(msg):
                # Replace with placeholder, preserving the tool name so the
                # audit protocol can still see which tool produced it.
                result.append(
                    ToolMessage(
                        content=self.config.placeholder_text,
                        tool_call_id=msg.tool_call_id,
                        name=getattr(msg, "name", None),
                    )
                )
                cleared_count += 1
            else:
                if i in indices_to_clear:
                    preserved_count += 1
                result.append(msg)

        if cleared_count > 0:
            self._state.total_tool_results_cleared += cleared_count
            logger.debug(
                f"Cleared {cleared_count} old tool results "
                f"(preserved {preserved_count} evidence-bearing results)"
            )

        return result

    def truncate_long_tool_results(
        self,
        messages: List[BaseMessage],
        max_length: Optional[int] = None,
        keep_recent: Optional[int] = None,
    ) -> List[BaseMessage]:
        """Truncate tool results that exceed max length.

        Only truncates older results; recent ones are kept in full.

        Args:
            messages: Message list to process
            max_length: Max chars for tool results (default from config)
            keep_recent: Number of recent results to keep in full

        Returns:
            Processed message list with truncated results
        """
        max_length = max_length or self.config.max_tool_result_length
        keep_recent = keep_recent or self.config.keep_recent_tool_results

        # Count tool messages from the end (protected messages are neither
        # truncated nor counted against the window)
        tool_indices = [
            i
            for i, m in enumerate(messages)
            if isinstance(m, ToolMessage) and not is_protected_message(m)
        ]

        if not tool_indices:
            return messages

        # Recent tool messages (don't truncate)
        recent_indices = set(tool_indices[-keep_recent:]) if keep_recent else set()

        result = []
        for i, msg in enumerate(messages):
            if (
                isinstance(msg, ToolMessage)
                and i not in recent_indices
                and not is_protected_message(msg)
            ):
                if len(msg.content) > max_length and not self._is_evidence_tool_message(
                    msg
                ):
                    truncated = (
                        msg.content[:max_length]
                        + f"\n\n[TRUNCATED - {len(msg.content) - max_length} chars omitted, see workspace]"
                    )
                    result.append(
                        ToolMessage(
                            content=truncated,
                            tool_call_id=msg.tool_call_id,
                            name=getattr(msg, "name", None),
                        )
                    )
                else:
                    result.append(msg)
            else:
                result.append(msg)

        return result

    def prepare_messages_for_llm(
        self,
        messages: List[BaseMessage],
        aggressive: bool = False,
    ) -> List[BaseMessage]:
        """Prepare messages for LLM by applying context management.

        Applies the following in order:
        1. Clear old tool results (if aggressive or above threshold)
        2. Truncate long tool results
        3. Trim messages if still over threshold

        Args:
            messages: Original message list
            aggressive: If True, clear more aggressively

        Returns:
            Processed message list ready for LLM
        """
        if not messages:
            return messages

        token_count = self.get_token_count(messages)
        should_be_aggressive = (
            aggressive or token_count > self.config.compaction_threshold_tokens
        )

        # Step 1: Clear old tool results
        if should_be_aggressive:
            messages = self.clear_old_tool_results(messages)

        # Step 2: Truncate long results in remaining messages
        messages = self.truncate_long_tool_results(messages)

        # Step 3: If STILL above threshold, trim messages
        new_token_count = self.get_token_count(messages)
        if new_token_count > self.config.compaction_threshold_tokens:
            logger.warning(
                f"Context still at {new_token_count} tokens after tool compaction, "
                f"trimming messages (threshold: {self.config.compaction_threshold_tokens})"
            )
            messages = self.trim_messages(messages)

        return messages

    def trim_messages(
        self,
        messages: List[BaseMessage],
        keep_recent: Optional[int] = None,
    ) -> List[BaseMessage]:
        """Trim messages to keep only recent ones.

        Preserves (never trimmed - implements Layers 1-3 protection):
        - All system messages (Layer 1: system prompt, Layer 2: todo list)
        - The first human message (original task)
        - Protected messages (phase instruction blocks), wherever they sit
        - Recent conversation messages

        Note: Layer 2 (todo list with visual separators) is injected fresh
        AFTER this method is called in graph.py, so it's never subject to
        trimming anyway. This method preserves any SystemMessages that might
        be in the message history.

        Args:
            messages: Message list to trim
            keep_recent: Number of recent messages to keep

        Returns:
            Trimmed message list
        """
        keep_recent = keep_recent or self.config.keep_recent_messages

        # Separate system messages
        system_msgs = [m for m in messages if isinstance(m, SystemMessage)]
        conversation = [m for m in messages if not isinstance(m, SystemMessage)]

        if len(conversation) <= keep_recent:
            return messages

        # Keep first human message (original task) and recent messages
        first_human_idx = next(
            (i for i, m in enumerate(conversation) if isinstance(m, HumanMessage)), None
        )

        # Recent messages start at a boundary that doesn't orphan ToolMessages
        target_start = len(conversation) - keep_recent
        safe_start = find_safe_slice_start(conversation, target_start)

        trimmed_conversation = []
        if first_human_idx is not None and first_human_idx < safe_start:
            trimmed_conversation = [conversation[first_human_idx]]
        # Protected messages outside the window are kept in their original
        # relative order, after the original task and before the window.
        trimmed_conversation.extend(
            m
            for i, m in enumerate(conversation[:safe_start])
            if i != first_human_idx and is_protected_message(m)
        )
        trimmed_conversation.extend(conversation[safe_start:])

        trimmed_count = len(conversation) - len(trimmed_conversation)
        if trimmed_count > 0:
            self._state.total_messages_trimmed += trimmed_count
            logger.info(f"Trimmed {trimmed_count} old messages")

        return system_msgs + trimmed_conversation

    async def ensure_within_limits(
        self,
        messages: List[BaseMessage],
        auxiliary,
        summarization_prompt: Optional[str] = None,
        max_summary_length: int = 10000,
        force: bool = False,
        trigger: str = "auto",
        focus: Optional[str] = None,
    ) -> List[BaseMessage]:
        """Ensure messages are within configured limits, summarizing if needed.

        This is the single entry point for context compaction. Call this before
        LLM requests to guarantee context stays within bounds.

        Args:
            messages: Current message history
            auxiliary: AuxiliaryLLM instance for summarization
            summarization_prompt: Optional custom prompt (reasoning level pre-rendered)
            max_summary_length: Max length for summary
            force: If True, summarize even if thresholds not exceeded
            trigger: ``auto`` | ``manual`` | ``resume`` — compaction event metadata
            focus: Optional user compaction focus (``/compact <focus>``)

        Returns:
            Messages (possibly compacted) guaranteed to be within limits
        """
        if force or self.should_summarize(messages):
            logger.info(
                f"Context compaction triggered: {len(messages)} messages, "
                f"{self.get_token_count(messages)} tokens"
            )
            result = await self.summarize_and_compact(
                messages,
                auxiliary,
                summarization_prompt,
                max_summary_length,
                trigger=trigger,
                focus=focus,
            )

            # Keep-window elision (session_silent_failure_audit.md #6): tool
            # results inside the keep window are protected from summarization
            # by tool-call pairing, so a few giant reads can hold the context
            # above the model limit even after a successful summary — every
            # retry then resends the same overflowing request (thread
            # b60166ee: four PDF results = 234k tokens on a 128k model).
            # Eliding only the *content* preserves pairing. Runs on both the
            # per-turn and force paths; the force-path emergency truncation
            # below stays as the last resort.
            non_remove = [m for m in result if not isinstance(m, RemoveMessage)]
            if self.get_token_count(non_remove) > self.config.model_max_context_tokens:
                result = self._elide_largest_tool_results(
                    result, self.config.model_max_context_tokens
                )

            # Progressive compaction: if force=True and still too many messages,
            # retry with progressively smaller keep_recent windows
            if force:
                keep_recent = self.config.keep_recent_messages
                # Count conversation messages (non-system, non-RemoveMessage)
                prev_conv_count = sum(
                    1
                    for m in messages
                    if not isinstance(m, (SystemMessage, RemoveMessage))
                )
                conv_count = sum(
                    1
                    for m in result
                    if not isinstance(m, (SystemMessage, RemoveMessage))
                )
                # If first compaction didn't reduce, skip progressive loop
                # (e.g., summary was larger than original — further retries won't help)
                if conv_count >= prev_conv_count:
                    logger.debug(
                        f"Compaction did not reduce messages ({prev_conv_count} -> {conv_count}), "
                        "skipping progressive loop"
                    )
                else:
                    # Try progressively smaller windows: half → quarter → 2 → 1
                    for divisor in [2, 4, None, None]:
                        if conv_count <= keep_recent:
                            break
                        if divisor:
                            next_keep = max(2, keep_recent // divisor)
                        else:
                            # Final attempts with absolute minimums
                            next_keep = 2 if conv_count > 2 else 1
                        if next_keep >= keep_recent:
                            break  # No further reduction possible
                        prev_conv_count = conv_count
                        logger.warning(
                            f"Progressive compaction: still {conv_count} conversation messages, "
                            f"retrying with keep_recent={next_keep}"
                        )
                        # Re-compact from ORIGINAL messages to avoid RemoveMessage accumulation
                        result = await self.summarize_and_compact(
                            messages,
                            auxiliary,
                            summarization_prompt,
                            max_summary_length,
                            keep_recent_override=next_keep,
                            trigger=trigger,
                            focus=focus,
                        )
                        conv_count = sum(
                            1
                            for m in result
                            if not isinstance(m, (SystemMessage, RemoveMessage))
                        )
                        keep_recent = next_keep
                        # Break if compaction didn't reduce messages further
                        if conv_count >= prev_conv_count:
                            logger.debug(
                                "Progressive compaction stalled, stopping retries"
                            )
                            break

                # Emergency: if token count still exceeds model max, truncate tool results
                non_remove = [m for m in result if not isinstance(m, RemoveMessage)]
                token_count = self.get_token_count(non_remove)
                if token_count > self.config.model_max_context_tokens:
                    logger.warning(
                        f"Emergency truncation: {token_count} tokens still exceeds "
                        f"model max {self.config.model_max_context_tokens}"
                    )
                    result = self._emergency_truncate_tool_results(result)

            return result
        return messages

    def _shed_image_messages(
        self, messages: List[BaseMessage]
    ) -> Tuple[List[BaseMessage], int]:
        """Replace every multimodal image ``HumanMessage`` with a text marker.

        Shared by both compaction-time elision tiers (S3). Image re-deliveries
        (the synthetic "image content from tool call …" messages, or a user's
        own uploaded image) are lossless to shed — the model already processed
        the image — and carry no tool-call pairing contract, so this is
        unconditionally safe. Returns a new list (same length, order, and ids
        preserved) and the number of messages shed.
        """
        result = list(messages)
        shed = 0
        for i, msg in enumerate(result):
            if is_protected_message(msg):
                continue
            if isinstance(msg, HumanMessage) and has_image_content(msg.content):
                tokens = self.token_counter([msg])
                _text, n_images = split_text_and_images(msg.content)
                replacement = HumanMessage(
                    content=(
                        f"[image content elided by compaction: {n_images} image(s), "
                        f"~{tokens:,} tokens. The model already processed it; "
                        f"re-read the source if the visual is needed again.]"
                    )
                )
                if getattr(msg, "id", None):
                    replacement.id = msg.id
                result[i] = self._as_compaction_view(msg, replacement)
                shed += 1
        return result, shed

    def _elide_largest_tool_results(
        self,
        messages: List[BaseMessage],
        target_tokens: int,
    ) -> List[BaseMessage]:
        """Shed the largest sheddable messages until under target.

        Two candidate classes, shed in order:
        1. Multimodal image ``HumanMessage``s — lossless (the model already saw
           the image) and free of any tool-pairing contract, so they go first
           and free the most room with the least cost.
        2. ``ToolMessage`` results — content elided, ``tool_call_id`` kept so
           the parent AIMessage's tool calls stay answered. Largest-first, so
           the minimum number is sacrificed.

        Evidence preservation is deliberately ignored here: this stage only runs
        when the request cannot otherwise fit the model at all — an elided
        result beats a permanently dead session. Mutating the live list is why
        elision is **compaction-time only**: adding/removing an image
        invalidates the provider prompt cache (context_token_accounting.md §4.4).

        Args:
            messages: Message list (may contain RemoveMessage markers)
            target_tokens: Stop once the non-marker total fits this budget
        """
        # Images first — lossless and the dominant bloat for multimodal sessions.
        result, images_shed = self._shed_image_messages(messages)
        non_remove = [m for m in result if not isinstance(m, RemoveMessage)]
        if self.get_token_count(non_remove) <= target_tokens:
            if images_shed:
                logger.warning(
                    f"Keep-window elision: shed {images_shed} image message(s) to "
                    f"fit the {target_tokens:,}-token model limit"
                )
            return result

        sized = []
        for i, msg in enumerate(result):
            if is_protected_message(msg):
                continue  # phase instruction blocks are never shed
            if isinstance(msg, ToolMessage) and isinstance(msg.content, str):
                if msg.content.startswith("[tool result elided"):
                    continue  # already elided
                sized.append((self.token_counter([msg]), i))
        sized.sort(reverse=True)

        elided = 0
        for msg_tokens, idx in sized:
            non_remove = [m for m in result if not isinstance(m, RemoveMessage)]
            if self.get_token_count(non_remove) <= target_tokens:
                break
            msg = result[idx]
            replacement = ToolMessage(
                content=(
                    f"[tool result elided by compaction: ~{msg_tokens:,} tokens. "
                    f"Re-run the tool if this data is needed again.]"
                ),
                tool_call_id=msg.tool_call_id,
            )
            if getattr(msg, "name", None):
                replacement.name = msg.name
            if getattr(msg, "id", None):
                replacement.id = msg.id
            result[idx] = self._as_compaction_view(msg, replacement)
            elided += 1

        if images_shed or elided:
            logger.warning(
                f"Keep-window elision: shed {images_shed} image message(s) and "
                f"replaced {elided} tool result(s) to fit the "
                f"{target_tokens:,}-token model limit"
            )
        return result

    def _emergency_truncate_tool_results(
        self,
        messages: List[BaseMessage],
        initial_limit: int = 2000,
        final_limit: int = 500,
    ) -> List[BaseMessage]:
        """Truncate ALL tool results as a last resort when context is still too large.

        This handles the case where individual tool results (e.g., a 875KB read_file)
        are larger than the entire context budget.

        Args:
            messages: Message list (may contain RemoveMessage markers)
            initial_limit: First truncation pass limit in chars
            final_limit: Second truncation pass limit if still over

        Returns:
            Messages with truncated tool results
        """
        # Last-resort path: shed image re-deliveries too. The largest-first
        # elision tier handles them first, but a re-summarized keep-window can
        # reintroduce them — and char-truncation can't touch list content.
        # Lossless and pairing-free, so unconditional here.
        messages, images_shed = self._shed_image_messages(messages)
        if images_shed:
            logger.warning(f"Emergency truncation: shed {images_shed} image message(s)")

        for limit in [initial_limit, final_limit]:
            # Build list of (index, content_length) for ToolMessages, sorted largest first
            tool_sizes = []
            for i, msg in enumerate(messages):
                if (
                    isinstance(msg, ToolMessage)
                    and len(msg.content) > limit
                    and not is_protected_message(msg)
                ):
                    tool_sizes.append((i, len(msg.content)))
            tool_sizes.sort(key=lambda x: x[1], reverse=True)

            if not tool_sizes:
                break

            truncated_count = 0
            result = list(messages)
            for idx, orig_len in tool_sizes:
                msg = result[idx]
                result[idx] = ToolMessage(
                    content=msg.content[:limit]
                    + f"\n\n[EMERGENCY TRUNCATED - {orig_len - limit} chars removed]",
                    tool_call_id=msg.tool_call_id,
                )
                truncated_count += 1

            logger.warning(
                f"Emergency truncated {truncated_count} tool results to {limit} chars"
            )
            messages = result

            # Check if we're under the limit now
            non_remove = [m for m in messages if not isinstance(m, RemoveMessage)]
            token_count = self.get_token_count(non_remove)
            if token_count <= self.config.model_max_context_tokens:
                break

        return messages

    def _format_messages_for_summary(self, messages: List[BaseMessage]) -> List[str]:
        """Format messages into text parts for summarization.

        Uses observation masking (JetBrains "Complexity Trap" pattern):
        - Recent tool results: include truncated content (first 300 chars)
        - AI reasoning: include up to 800 chars (reasoning traces > tool output per ACON)
        - Old tool results: replace with placeholder noting tool name + size
        - Reasoning/action history: always preserved in full

        Args:
            messages: Messages to format

        Returns:
            List of formatted text parts
        """
        from shared.runtime.core.workspace_injection import (
            is_workspace_injection_message,
        )

        # Count tool messages and build parent mapping for atomic grouping
        tool_msg_indices = []
        tool_call_parent = {}  # tool_call_id → parent AIMessage index
        for i, msg in enumerate(messages):
            if (
                isinstance(msg, AIMessage)
                and hasattr(msg, "tool_calls")
                and msg.tool_calls
            ):
                for tc in msg.tool_calls:
                    tc_id = tc.get("id")
                    if tc_id:
                        tool_call_parent[tc_id] = i
            elif isinstance(msg, ToolMessage):
                tool_msg_indices.append(i)

        # Keep last 10 tool results with content (observation masking window)
        recent_tool_indices = set(tool_msg_indices[-10:]) if tool_msg_indices else set()

        # Atomic grouping: if any tool result sharing the same parent AIMessage
        # is recent, include all sibling results in the recent set.
        # (ForgeCode pattern: "never split tool call/result pairs")
        if recent_tool_indices and tool_call_parent:
            recent_parents = set()
            for idx in recent_tool_indices:
                tc_id = getattr(messages[idx], "tool_call_id", None)
                parent_idx = tool_call_parent.get(tc_id)
                if parent_idx is not None:
                    recent_parents.add(parent_idx)

            for idx in tool_msg_indices:
                if idx not in recent_tool_indices:
                    tc_id = getattr(messages[idx], "tool_call_id", None)
                    parent_idx = tool_call_parent.get(tc_id)
                    if parent_idx in recent_parents:
                        recent_tool_indices.add(idx)

        # Determine recency boundary for the visual marker.
        # The marker is inserted before the earliest message in the recent tool window,
        # but only if there are enough tool messages to have an "old" section.
        recency_boundary = None
        if len(tool_msg_indices) > len(recent_tool_indices):
            recency_boundary = min(recent_tool_indices)

        formatted_parts = []
        marker_inserted = False
        for i, msg in enumerate(messages):
            # Skip workspace injection messages - they're re-injected fresh after summarization
            if is_workspace_injection_message(msg):
                continue
            # Skip protected messages (phase instruction blocks): they are
            # re-seated verbatim after the summary, and their text would
            # otherwise dominate the summary of the work.
            if is_protected_message(msg):
                continue

            # Insert recency marker before the first message in the recent window
            if (
                recency_boundary is not None
                and i >= recency_boundary
                and not marker_inserted
            ):
                if formatted_parts:  # Only if there are older messages to separate from
                    formatted_parts.append(
                        "\n════════════════════════════════════════\n"
                        "RECENT CONTEXT — PRESERVE WITH HIGHEST PRIORITY\n"
                        "════════════════════════════════════════"
                    )
                marker_inserted = True

            if isinstance(msg, SystemMessage):
                # Include prior summaries in the new summarization so context is preserved
                # Skip other system messages (like the main system prompt)
                if "[Summary of prior work]" in msg.content:
                    formatted_parts.append(f"Prior Summary: {msg.content}")
                continue
            elif isinstance(msg, HumanMessage):
                # Image-safe: list content (a multimodal re-delivery) becomes
                # text + "[image: ...]" markers, never stringified base64.
                text = content_to_summary_text(msg.content)
                formatted_parts.append(f"User: {text[:500]}")
            elif isinstance(msg, AIMessage):
                content = content_to_summary_text(msg.content)
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    tool_names = [tc.get("name", "unknown") for tc in msg.tool_calls]
                    if content:
                        # Preserve reasoning alongside tool calls
                        # (ACON: reasoning traces > tool output)
                        formatted_parts.append(
                            f"Assistant: {content[:800]}... [Called tools: {', '.join(tool_names)}]"
                        )
                    else:
                        formatted_parts.append(
                            f"Assistant: [Called tools: {', '.join(tool_names)}]"
                        )
                elif content:
                    formatted_parts.append(f"Assistant: {content[:800]}...")
            elif isinstance(msg, ToolMessage):
                tool_name = getattr(msg, "name", None) or "unknown"
                content = content_to_summary_text(msg.content)
                if i in recent_tool_indices:
                    # Recent: include truncated content for summarization
                    truncated = content[:300]
                    suffix = "..." if len(content) > 300 else ""
                    formatted_parts.append(
                        f"[Tool '{tool_name}' result: {truncated}{suffix}]"
                    )
                else:
                    # Old: observation masking — placeholder only
                    formatted_parts.append(
                        f"[Tool '{tool_name}' result omitted ({len(content)} chars)]"
                    )

        return formatted_parts

    async def summarize_conversation(
        self,
        messages: List[BaseMessage],
        auxiliary,
        summarization_prompt: Optional[str] = None,
        max_summary_length: int = 10000,
        seed_summary: Optional[str] = None,
        focus: Optional[str] = None,
        trigger: str = "auto",
        context_tokens: Optional[int] = None,
    ) -> Optional[str]:
        """Generate a summary of the conversation via the rolling-fold engine.

        Arbitrarily large inputs are planned into chunks sized for the
        *auxiliary model's own* context window and folded sequentially
        (``src/agent/core/summarizer.py``) — never sized against the main model's
        window. Emits ``compaction.started`` / ``compaction.failed`` through
        the progress callback; per-pass ``compaction.progress`` comes from the
        engine.

        Args:
            messages: Messages to summarize
            auxiliary: AuxiliaryLLM instance for summarization
            summarization_prompt: Optional custom prompt (reasoning level pre-rendered)
            max_summary_length: Maximum length for the final summary
            seed_summary: Prior summary text seeding the rolling fold
            focus: Optional user compaction focus (``/compact <focus>``)
            trigger: ``auto`` | ``manual`` | ``resume`` (event metadata)
            context_tokens: Full-context token count for event display
                (defaults to the planned input size)

        Returns:
            Summary string, or None on total summarizer failure — callers
            must keep the original messages (never compact behind a
            placeholder).
        """
        from agent.core.summarizer import SummarizationEngine, SummarizationFailed

        formatted_parts = self._format_messages_for_summary(messages)
        if not formatted_parts:
            logger.info("Nothing to summarize (no formattable messages)")
            return None

        # Stamp the trigger onto every engine frame: a reload can replay a
        # compaction.progress without its compaction.started, and the cockpit
        # should not have to guess the trigger when it synthesizes state.
        engine_cb = None
        if self.progress_cb is not None:
            outer_cb = self.progress_cb

            async def engine_cb(event: str, params: Dict[str, Any]) -> None:
                await outer_cb(event, {"trigger": trigger, **params})

        engine = SummarizationEngine(
            auxiliary,
            summarization_prompt=summarization_prompt,
            max_summary_length=max_summary_length,
            call_timeout=self._summarization_call_timeout,
            progress_cb=engine_cb,
        )

        try:
            plan = engine.plan(formatted_parts)
        except SummarizationFailed as e:
            logger.error(f"Summarization planning failed ({e.reason}): {e}")
            await self._emit_compaction_event(
                "compaction.failed",
                {
                    "trigger": trigger,
                    "reason": e.reason,
                    "pass": 0,
                    "n_passes": 0,
                    "kept_messages": True,
                },
            )
            return None

        ctx_used = context_tokens if context_tokens is not None else plan.total_tokens
        ctx_limit = max(self.config.model_max_context_tokens, 1)
        logger.info(
            f"Summarization plan: {plan.total_tokens} tokens in "
            f"{plan.n_passes} pass(es), chunk budget {plan.chunk_budget}, "
            f"aux window {plan.aux_window}"
        )
        await self._emit_compaction_event(
            "compaction.started",
            {
                "trigger": trigger,
                "total_tokens": plan.total_tokens,
                "ctx_used_tokens": ctx_used,
                "ctx_limit_tokens": ctx_limit,
                "ctx_used_pct": round(100 * ctx_used / ctx_limit),
                "aux_limit_tokens": plan.aux_window,
                "n_passes": plan.n_passes,
                "plan": plan.describe(),
            },
        )

        start_time = time.monotonic()
        try:
            summary = await engine.run(plan, seed_summary=seed_summary, focus=focus)
        except SummarizationFailed as e:
            logger.error(f"Summarization failed ({e.reason}): {e}")
            await self._emit_compaction_event(
                "compaction.failed",
                {
                    "trigger": trigger,
                    "reason": e.reason,
                    "pass": e.pass_index,
                    "n_passes": e.n_passes or plan.n_passes,
                    "kept_messages": True,
                },
            )
            return None

        if not summary:
            logger.error("Summarization produced no summary")
            await self._emit_compaction_event(
                "compaction.failed",
                {
                    "trigger": trigger,
                    "reason": "empty_summary",
                    "pass": plan.n_passes,
                    "n_passes": plan.n_passes,
                    "kept_messages": True,
                },
            )
            return None

        self._last_summarization_stats = {
            "n_passes": plan.n_passes,
            "duration_ms": int((time.monotonic() - start_time) * 1000),
            "before_tokens": plan.total_tokens,
        }

        logger.info(
            f"Generated summary ({len(summary)} chars, {plan.n_passes} pass(es))"
        )
        # Debug: log tail
        tail = summary[-500:] if len(summary) > 500 else summary
        logger.debug(f"Summary tail:\n{tail}")

        self._state.total_summarizations += 1
        self._state.summaries.append(summary)
        return summary

    async def summarize_and_compact(
        self,
        messages: List[BaseMessage],
        auxiliary,
        summarization_prompt: Optional[str] = None,
        max_summary_length: int = 10000,
        keep_recent_override: Optional[int] = None,
        trigger: str = "auto",
        focus: Optional[str] = None,
    ) -> List[BaseMessage]:
        """Summarize older messages and compact the conversation.

        This is the most aggressive context management strategy.
        Used when other strategies aren't sufficient.

        Args:
            messages: Full message history
            auxiliary: AuxiliaryLLM instance for summarization
            summarization_prompt: Optional custom prompt (reasoning level pre-rendered)
            max_summary_length: Max length for summary
            keep_recent_override: Override keep_recent_messages (for progressive compaction)
            trigger: ``auto`` | ``manual`` | ``resume`` — compaction event metadata
            focus: Optional user compaction focus (``/compact <focus>``)

        Returns:
            Compacted message list with summary prepended
        """
        from shared.runtime.core.workspace_injection import (
            is_workspace_injection_message,
        )

        # Reset the boundary marker; only a real compaction (final return below)
        # sets it. A no-op / skipped compaction leaves it None so the transport
        # falls back to boundary_turn rather than recording a stale boundary_seq.
        self._last_compaction_boundary_id = None

        # Filter out workspace injection messages BEFORE processing
        # They are transient and will be re-injected fresh after summarization
        messages = [m for m in messages if not is_workspace_injection_message(m)]

        # Determine effective keep_recent value
        effective_keep_recent = (
            keep_recent_override
            if keep_recent_override is not None
            else self.config.keep_recent_messages
        )

        # Separate system messages into:
        # 1. Regular system messages (keep in output)
        # 2. Old summary messages (incorporate into new summary, then discard)
        # Old summaries are identified by the "[Summary of prior work]" prefix
        system_msgs = [
            m
            for m in messages
            if isinstance(m, SystemMessage)
            and "[Summary of prior work]" not in m.content
        ]
        old_summaries = [
            m
            for m in messages
            if isinstance(m, SystemMessage) and "[Summary of prior work]" in m.content
        ]
        original_conversation = [
            m for m in messages if not isinstance(m, SystemMessage)
        ]

        # Backstop for runaway-generation poisoning: any single AIMessage
        # that exceeds half the context window is almost certainly a
        # repetition-loop artifact (the loader-side max_tokens fallback
        # prevents new ones, but a session resumed from a poisoned state
        # — or one that hit an endpoint ignoring max_tokens — needs this
        # to recover). Replace with a stub before slicing; the original
        # is removed via the existing removal_markers loop. We skip
        # AIMessages with tool_calls (substituting one orphans the
        # paired ToolMessages and breaks the turn) and ToolMessages
        # (legitimate large reads should be handled by `truncate_long_tool_results`).
        oversized_threshold = self.config.model_max_context_tokens // 2
        sanitized_conversation: List[BaseMessage] = []
        oversized_count = 0
        oversized_total = 0
        for msg in original_conversation:
            msg_tokens = self.token_counter([msg])
            replaceable = (
                msg_tokens > oversized_threshold
                and isinstance(msg, AIMessage)
                and not getattr(msg, "tool_calls", None)
                and not is_protected_message(msg)
            )
            if replaceable:
                oversized_count += 1
                oversized_total += msg_tokens
                sanitized_conversation.append(
                    AIMessage(
                        content=(
                            f"[Previous response of ~{msg_tokens:,} tokens elided "
                            f"by compaction — likely runaway generation. "
                            f"See workspace logs for details.]"
                        )
                    )
                )
            else:
                sanitized_conversation.append(msg)

        if oversized_count > 0:
            logger.warning(
                f"Compaction: replaced {oversized_count} oversized AIMessage(s) "
                f"({oversized_total:,} tokens total, threshold {oversized_threshold:,}) "
                f"with stubs"
            )

        # `conversation` (sanitized) drives slicing and recent-message
        # reconstruction; `original_conversation` is retained for the
        # removal_markers loop so the originals' IDs are evicted from state.
        conversation = sanitized_conversation

        def _tool_result_is_already_truncated(msg: ToolMessage) -> bool:
            content = msg.content if isinstance(msg.content, str) else ""
            if not content:
                return False
            lower = content.lower()
            if lower.startswith("[tool result elided by compaction"):
                return True
            # The truncation marker is appended AFTER the kept head, so it
            # must be detected in the content tail — otherwise every later
            # compaction re-truncates the message and rewrites the marker's
            # original-size provenance.
            return "[tool result truncated by compaction" in lower[-600:]

        def _cap_keep_window_tool_results(
            tool_messages: List[BaseMessage],
        ) -> Tuple[List[BaseMessage], int, int]:
            """Head-truncate oversized tail tool messages.

            Returns:
                capped_messages, before_tokens, after_tokens
            """
            before_tokens = self.get_token_count(
                [m for m in tool_messages if not isinstance(m, RemoveMessage)]
            )
            capped_messages: List[BaseMessage] = []
            for msg in tool_messages:
                if not isinstance(msg, ToolMessage) or is_protected_message(msg):
                    capped_messages.append(msg)
                    continue

                content = msg.content if isinstance(msg.content, str) else ""
                if len(content) <= self.config.keep_window_max_tool_result_chars:
                    capped_messages.append(msg)
                    continue
                if _tool_result_is_already_truncated(msg):
                    capped_messages.append(msg)
                    continue

                omitted_chars = (
                    len(content) - self.config.keep_window_max_tool_result_chars
                )
                omitted_tokens = max(1, omitted_chars // 4)
                capped_messages.append(
                    ToolMessage(
                        content=(
                            f"{content[: self.config.keep_window_max_tool_result_chars]}\n\n"
                            f"[tool result truncated by compaction: kept "
                            f"{self.config.keep_window_max_tool_result_chars:,} of "
                            f"{len(content):,} chars (~{omitted_tokens:,} tokens). "
                            "Full content was saved to the workspace / is re-fetchable — "
                            "re-run the tool or read the saved file if the rest is needed.]"
                        ),
                        tool_call_id=msg.tool_call_id,
                        name=getattr(msg, "name", None),
                        # Identity rides along: the worker path re-copies this
                        # id-less below anyway, the identity-preserving path
                        # needs the capped copy to stay the same row.
                        id=getattr(msg, "id", None),
                        additional_kwargs=dict(
                            getattr(msg, "additional_kwargs", None) or {}
                        ),
                    )
                )
                # ...as a compaction view: the reconcile must never write the
                # truncation over the durable full result.
                self._as_compaction_view(msg, capped_messages[-1])

            after_tokens = self.get_token_count(
                [m for m in capped_messages if not isinstance(m, RemoveMessage)]
            )
            return capped_messages, before_tokens, after_tokens

        # Helper: when normal compaction can't proceed (too few messages,
        # summary larger than original, etc.) but we *did* substitute
        # oversized messages, return the substitution as a standalone
        # result so the backstop still wins. Without this, the runaway
        # AIMessage would survive the early returns and re-poison the
        # next turn.
        def _substitution_only_result(
            fresh_messages: Optional[List[BaseMessage]] = None,
        ) -> List[BaseMessage]:
            markers = [
                RemoveMessage(id=m.id)
                for m in original_conversation
                if hasattr(m, "id") and m.id
            ]
            replacement = fresh_messages or sanitized_conversation
            return markers + system_msgs + replacement

        if len(conversation) <= effective_keep_recent:
            if oversized_count > 0:
                capped_conversation, _, _ = _cap_keep_window_tool_results(conversation)
                return _substitution_only_result(
                    fresh_messages=capped_conversation
                    if capped_conversation != conversation
                    else None
                )

            capped_conversation, before_tokens, after_tokens = (
                _cap_keep_window_tool_results(conversation)
            )
            if after_tokens >= before_tokens:
                return messages

            self._note_compaction_success()
            markers = [
                RemoveMessage(id=m.id)
                for m in original_conversation
                if hasattr(m, "id") and m.id
            ]
            return markers + system_msgs + capped_conversation

        # Find safe slice point that doesn't orphan ToolMessages
        target_start = len(conversation) - effective_keep_recent
        safe_start = find_safe_slice_start(conversation, target_start)

        # Messages to summarize (older ones) and recent messages to keep
        messages_to_summarize = conversation[:safe_start]
        recent_messages = conversation[safe_start:]
        capped_recent_messages, recent_before_tokens, recent_after_tokens = (
            _cap_keep_window_tool_results(recent_messages)
        )
        keep_window_token_savings = max(0, recent_before_tokens - recent_after_tokens)

        # Rolling-summary continuation: prior summaries seed the fold (the
        # engine prepends them as "Prior Summary:" to the first pass) instead
        # of being re-formatted as messages.
        seed_summary: Optional[str] = None
        if old_summaries:
            seed_summary = "\n\n".join(
                m.content.replace("[Summary of prior work]\n", "", 1)
                for m in old_summaries
            )

        # Generate summary
        summary = await self.summarize_conversation(
            conversation,
            auxiliary,
            summarization_prompt,
            max_summary_length,
            seed_summary=seed_summary,
            focus=focus,
            trigger=trigger,
            context_tokens=self.get_token_count(
                [m for m in messages if not isinstance(m, RemoveMessage)]
            ),
        )

        # Stopgap (knowledge-base/knowledge/issues/session_silent_failure_audit.md #4): when the
        # summarizer is fully unavailable, keep the original history rather
        # than compacting it away behind a placeholder. The turn may then
        # fail on context overflow — visibly — instead of the agent being
        # silently lobotomized.
        if summary is None:
            logger.error(
                "Compaction aborted: summarization unavailable (aux LLM "
                f"failure) — keeping {len(messages)} messages uncompacted"
            )
            if oversized_count > 0:
                return _substitution_only_result()
            return messages

        # Guard: if summary is larger than what we're replacing, skip compaction
        summary_tokens = self.get_token_count([SystemMessage(content=summary)])
        original_tokens = (
            self.get_token_count(old_summaries + messages_to_summarize)
            + keep_window_token_savings
        )
        if summary_tokens > original_tokens:
            logger.error(
                f"Summary ({summary_tokens} tokens) larger than original ({original_tokens} tokens) — skipping compaction"
            )
            # The engine already journaled compaction.started/progress; without
            # a journaled terminal frame an SSE replay resurrects the progress
            # UI with nothing to clear it. compaction.skipped closes the
            # lifecycle (the cockpit clears the block, silently).
            await self._emit_compaction_event(
                "compaction.skipped",
                {
                    "trigger": trigger,
                    "reason": "summary_not_smaller",
                    "summary_tokens": summary_tokens,
                    "original_tokens": original_tokens,
                },
            )
            if oversized_count > 0:
                return _substitution_only_result()
            return messages

        # Create summary as SystemMessage (best practice per OpenAI/LangChain)
        # SystemMessage signals "background context" rather than user dialogue
        summary_msg = SystemMessage(content=f"[Summary of prior work]\n{summary}")

        # Generate removal markers for:
        # 1. ALL conversation messages (summarized + recent) - recent are re-added as fresh copies
        # 2. Old summary messages - they've been incorporated into the new summary
        removal_markers = []
        messages_without_ids = 0

        # Remove old summaries (they've been merged into the new summary)
        for msg in old_summaries:
            if hasattr(msg, "id") and msg.id:
                removal_markers.append(RemoveMessage(id=msg.id))
            else:
                messages_without_ids += 1

        # Remove conversation messages (recent ones will be re-added as fresh copies).
        # Iterate the ORIGINAL conversation so we evict the right state IDs even
        # when oversized messages were substituted with stubs above. When the
        # kept window keeps its identity it is not re-added, so only the
        # summarised region is evicted (a marker for a surviving id would
        # delete the message it means to keep).
        evicted_conversation = (
            original_conversation[:safe_start]
            if self.preserve_message_identity
            else original_conversation
        )
        for msg in evicted_conversation:
            if hasattr(msg, "id") and msg.id:
                removal_markers.append(RemoveMessage(id=msg.id))
            else:
                messages_without_ids += 1

        if messages_without_ids > 0:
            logger.warning(
                f"{messages_without_ids} messages without IDs cannot be removed"
            )

        # Create fresh copies of recent messages without IDs so they get appended
        # after the summary instead of staying in their original positions.
        # preserve_message_identity keeps the objects themselves (no reducer
        # to satisfy; ids + turn stamps must survive — see __init__).
        fresh_recent = []
        for msg in capped_recent_messages:
            if self.preserve_message_identity:
                fresh_recent.append(msg)
            elif isinstance(msg, AIMessage):
                fresh_recent.append(
                    AIMessage(
                        content=msg.content,
                        tool_calls=getattr(msg, "tool_calls", None) or [],
                        additional_kwargs=msg.additional_kwargs,
                    )
                )
            elif isinstance(msg, ToolMessage):
                fresh_recent.append(
                    ToolMessage(
                        content=msg.content,
                        tool_call_id=msg.tool_call_id,
                        name=getattr(msg, "name", None),
                    )
                )
            elif isinstance(msg, HumanMessage):
                # additional_kwargs ride along: a protected phase block in
                # the keep window must keep its markers (and a session event
                # notice its persist role) through the fresh copy.
                fresh_recent.append(
                    HumanMessage(
                        content=msg.content,
                        additional_kwargs=dict(msg.additional_kwargs or {}),
                    )
                )
            else:
                # For any other message type, try to preserve it
                fresh_recent.append(msg)

        # Summary, then the current phase's protected blocks that fell into
        # the summarised region (fresh copies, deduped against the window),
        # then the kept window. Other phases' blocks go with their region.
        compacted_tail = place_pinned_after_summary(
            summary_msg,
            messages_to_summarize,
            fresh_recent,
            self._current_phase_key,
            preserve_identity=self.preserve_message_identity,
        )

        merged_summaries_info = (
            f", merged {len(old_summaries)} prior summaries" if old_summaries else ""
        )
        logger.info(
            f"Compacted {len(messages)} messages to {len(system_msgs) + len(compacted_tail)} "
            f"(summarized {len(messages_to_summarize)} messages{merged_summaries_info}, "
            f"removing {len(removal_markers)}, {messages_without_ids} without IDs)"
        )

        # A summary was produced and the compacted result is being returned —
        # bump the run counter (the transports' "did it actually compact this
        # call" signal; see __init__) and drop the now-stale provider anchor.
        self._note_compaction_success()

        # Record the summarized/kept boundary for the persistent transport: the
        # newest message the summary covers is original_conversation[safe_start-1]
        # (original, not sanitized, so the id matches the persisted row). Its seq
        # becomes boundary_seq, so resume loads exactly the messages after it
        # (seq > boundary_seq), never whole post-boundary turns.
        self._last_compaction_boundary_id = (
            getattr(original_conversation[safe_start - 1], "id", None)
            if safe_start >= 1
            else None
        )

        # Completion stats for the context.compacted event (read by the
        # persistent transport's _record_compaction).
        if self._last_summarization_stats is not None:
            self._last_summarization_stats["after_tokens"] = self.get_token_count(
                system_msgs + compacted_tail
            )

        # Return: removal markers + system messages + summary + re-seated
        # protected blocks + fresh recent. Order matters: the summary comes
        # BEFORE the phase block, which comes BEFORE the recent messages.
        return removal_markers + system_msgs + compacted_tail

    def create_pre_model_hook(self) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        """Create a pre-model hook for LangGraph integration.

        The hook intercepts state before each LLM call and applies
        context management as needed.

        Returns:
            Callable compatible with LangGraph pre_model_hook
        """

        def pre_model_hook(state: Dict[str, Any]) -> Dict[str, Any]:
            messages = state.get("messages", [])

            # Apply context management
            prepared = self.prepare_messages_for_llm(messages)

            # Log if significant compaction occurred
            if len(prepared) < len(messages):
                logger.debug(
                    f"Pre-model hook: {len(messages)} -> {len(prepared)} messages"
                )

            return {"llm_input_messages": prepared}

        return pre_model_hook


class ToolRetryManager:
    """Manages retry logic for tool execution.

    Implements exponential backoff with configurable retry count.
    Tracks failures per tool for monitoring.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
    ):
        """Initialize retry manager.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._failure_counts: Dict[str, int] = {}
        self._total_retries = 0

    def get_retry_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt.

        Uses exponential backoff with jitter.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        import random

        delay = self.base_delay * (2**attempt)
        delay = min(delay, self.max_delay)
        # Add 10% jitter
        jitter = delay * 0.1 * random.random()
        return delay + jitter

    def should_retry(self, tool_name: str, attempt: int) -> bool:
        """Check if a tool call should be retried.

        Args:
            tool_name: Name of the tool that failed
            attempt: Current attempt number

        Returns:
            True if retry should be attempted
        """
        return attempt < self.max_retries

    def record_failure(self, tool_name: str) -> int:
        """Record a tool failure.

        Args:
            tool_name: Name of the tool that failed

        Returns:
            Total failures for this tool
        """
        self._failure_counts[tool_name] = self._failure_counts.get(tool_name, 0) + 1
        return self._failure_counts[tool_name]

    def record_retry(self) -> None:
        """Record that a retry was attempted."""
        self._total_retries += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics.

        Returns:
            Dict with failure counts and total retries
        """
        return {
            "failure_counts": self._failure_counts.copy(),
            "total_retries": self._total_retries,
        }


async def write_error_to_workspace(
    workspace_manager: Any,
    error: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Write error details to workspace for debugging.

    Creates an error report in the workspace output folder.

    Args:
        workspace_manager: Workspace manager instance
        error: Error details dict
        context: Optional additional context

    Returns:
        Path to error file
    """
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    error_path = f"output/error_{timestamp}.md"

    error_content = f"""# Error Report

**Timestamp:** {datetime.now(UTC).isoformat()}
**Error Type:** {error.get("type", "unknown")}
**Recoverable:** {error.get("recoverable", False)}

## Error Message

{error.get("message", "No message provided")}

## Stack Trace

```
{error.get("traceback", "No traceback available")}
```

## Context

"""

    if context:
        for key, value in context.items():
            error_content += f"- **{key}:** {value}\n"
    else:
        error_content += "No additional context available.\n"

    error_content += """

## Recovery Suggestions

1. Check the workspace files for partial results
2. Review the todo list for completed vs pending items
3. Check the archive folder for completed phase summaries
4. Review the error message for actionable information
"""

    try:
        await workspace_manager.write_file(error_path, error_content)
        logger.info(f"Error report written to {error_path}")
        return error_path
    except Exception as e:
        logger.error(f"Failed to write error report: {e}")
        return ""
