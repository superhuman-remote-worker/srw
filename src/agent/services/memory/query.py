"""Unified retrieval-query formation (memory overhaul §4, Phase 3 slice 4).

The legacy query texts are mode-forked and thin: the worker retrieves on
"top todo + phase descriptor" (no conversational signal at all), the
persistent loop on the last user message alone (no task signal, and a
bare "yes, do that" retrieves on three words). The request digest
unifies both: a recent window of conversational turns plus the task
frame, so retrieval sees what the agent is actually about to do.

Behaviour change, therefore flagged: ``memory.query.digest`` (default
off). Both graphs and the eval harness consult the flag at their
AssembleRequest build sites, so the digest is measurable per-arm via
the harness requery mode before any production default flips.

Shape: chronological window (oldest → newest, the current message last —
"context then focus", the order embedders and rerankers read best),
task-frame descriptor appended as the final focus line. Human/AI
messages only — tool results are payload, not intent.
"""

from typing import List, Optional

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from agent.services.memory.types import TaskFrame
from shared.runtime.core.context_entries import is_context_injection

#: Defaults mirror QueryConfig (src/core/loader.py) — callers normally
#: pass the configured values; these keep the function usable standalone.
DEFAULT_WINDOW = 4
DEFAULT_MAX_CHARS_PER_MESSAGE = 500


def _message_text(msg: BaseMessage) -> str:
    return msg.content if isinstance(msg.content, str) else str(msg.content)


def build_digest_query_text(
    messages: List[BaseMessage],
    frame: Optional[TaskFrame] = None,
    *,
    window: int = DEFAULT_WINDOW,
    max_chars_per_message: int = DEFAULT_MAX_CHARS_PER_MESSAGE,
) -> str:
    """Digest of the upcoming call: recent window + task frame.

    Empty messages and a missing frame yield "" — same contract as the
    legacy builders (the read path still retrieves rather than skipping).
    Injected context never feeds the query: retrieval would otherwise
    search with its own earlier results.
    """
    recent = [
        m
        for m in messages
        if isinstance(m, (HumanMessage, AIMessage)) and not is_context_injection(m)
    ]
    if window > 0:
        recent = recent[-window:]
    else:
        recent = []

    parts: List[str] = []
    for msg in recent:
        text = _message_text(msg).strip()
        if not text:
            continue
        if max_chars_per_message > 0 and len(text) > max_chars_per_message:
            text = text[:max_chars_per_message]
        parts.append(text)

    if frame is not None:
        if frame.top_todo:
            parts.append(frame.top_todo)
        parts.append(
            f"phase {frame.phase_number} "
            f"{'strategic' if frame.is_strategic else 'tactical'}"
        )

    return "\n".join(parts)


#: Per-part cap of the exchange query (the user message and the answer).
EXCHANGE_MAX_CHARS = 2000


def _answer_text(msg: AIMessage) -> str:
    """The text of an assistant message (text blocks of list content only)."""
    content = msg.content
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        return " ".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    return ""


def build_exchange_query_text(
    messages: List[BaseMessage],
    *,
    max_chars_per_message: int = EXCHANGE_MAX_CHARS,
) -> str:
    """Query of a session's idle-time prefetch: the latest exchange (D24).

    The newest user message (not injected context) and the assistant's
    final answer after it, each capped at ``max_chars_per_message``,
    joined by a blank line ("" when the history holds neither). The answer
    is the newest AIMessage after that user message with text content;
    tool results are payload, not intent, and stay out.
    """
    user_index: Optional[int] = None
    for index in range(len(messages) - 1, -1, -1):
        msg = messages[index]
        if isinstance(msg, HumanMessage) and not is_context_injection(msg):
            user_index = index
            break
    parts: List[str] = []
    if user_index is not None:
        parts.append(_message_text(messages[user_index]).strip())
        for msg in reversed(messages[user_index + 1 :]):
            if not isinstance(msg, AIMessage) or is_context_injection(msg):
                continue
            text = _answer_text(msg)
            if text:
                parts.append(text)
                break
    if max_chars_per_message > 0:
        parts = [part[:max_chars_per_message] for part in parts]
    return "\n\n".join(part for part in parts if part)
