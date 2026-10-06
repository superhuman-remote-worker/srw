"""Serialising LangChain messages into ``thread_messages`` rows.

Lifted out of ``src/agent/api/persistent_app.py`` (U3 WP1) so the session transport
and the subagent runtime (``src/subagents``) share ONE serialiser: a child
thread's transcript must land in the same columns, with the same role
normalisation and reasoning extraction, as a session's. ``persistent_app``
re-exports these names unchanged (its tests import them from there).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from shared.runtime.llm.reasoning_chat import extract_reasoning_text_from_block
from shared.runtime.core.context_entries import SRW_INJECTION_KEY
from shared.runtime.core.message_markers import PERSIST_ROLE_CONTEXT
from shared.runtime.core.message_markers import PERSIST_ROLE_KEY as _PERSIST_ROLE_KEY

logger = logging.getLogger(__name__)


def _extract_thinking(msg: Any) -> Optional[str]:
    """Pull reasoning content out of an AIMessage for persistence.

    Three sources, in order:
      - Anthropic: ``content`` is a list of blocks, thinking blocks carry
        ``{"type": "thinking", "thinking": "..."}``.
      - OpenAI Responses API (gpt-5, etc.): ``content`` is a list of blocks,
        reasoning blocks carry ``{"type": "reasoning", "summary": [...],
        "content": [...]}``. Streaming preserves these as-is, since
        ``_extract_responses_api_reasoning`` only runs on the non-streaming
        path. Persistent agent streams, so we extract here at save time.
      - DeepSeek / OpenRouter / non-streaming Responses API:
        ``additional_kwargs.reasoning_content`` carries a plain string,
        populated by the HTTP layer or by ``_post_process_result``.

    Returns None when the model didn't emit a visible reasoning channel.
    """
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        anthropic_parts = [
            b.get("thinking", "")
            for b in content
            if isinstance(b, dict) and b.get("type") == "thinking"
        ]
        joined = "".join(anthropic_parts).strip()
        if joined:
            return joined

        responses_parts = [
            extract_reasoning_text_from_block(b)
            for b in content
            if isinstance(b, dict) and b.get("type") == "reasoning"
        ]
        joined = "".join(responses_parts).strip()
        if joined:
            return joined

    rc = getattr(msg, "additional_kwargs", {}).get("reasoning_content")
    return rc or None


# Normalize LangChain chunk types to persisted role strings (AIMessageChunk → ai).
_ROLE_MAP = {
    "ai": "ai",
    "AIMessageChunk": "ai",
    "human": "human",
    "HumanMessageChunk": "human",
    "tool": "tool",
    "ToolMessageChunk": "tool",
    "system": "system",
    "SystemMessageChunk": "system",
}


def _serialize_message_row(
    msg: Any,
    turn_number: int,
    *,
    metrics: dict | None = None,
    tool_decisions: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Serialize one LangChain message to a ``thread_messages`` row dict.

    The single serialization point shared by the **incremental** path
    (:func:`_persist_one_message`, persist each message the instant the loop
    produces it) and the **turn-complete reconciliation** batch
    (:func:`_save_turn_ai_messages`). Both carry the message's stable id, so they
    converge on one row (``ON CONFLICT (id)``): the incremental write lands the
    content the moment it exists (crash durability); reconciliation re-runs with
    the turn-level ``metrics`` and approval ``tool_decisions`` and updates the
    same row. ``seq`` is assigned once on first insert and preserved across the
    update, so it stays a stable cursor.

    Only a ``role='context'`` row (a typed context entry) carries
    ``additional_kwargs``, and only its ``srw_injection`` schema; every other
    row leaves the column to the upsert, which keeps what is stored.
    """
    raw_type = getattr(msg, "type", "unknown")
    role = _ROLE_MAP.get(raw_type, raw_type)
    # A message may declare the row role it wants to be stored under when its
    # LangChain type is only a carrier — an injected 'event' notice travels as
    # HumanMessage so the graph and _save_turn_ai_messages need no changes, but
    # must not persist as a user bubble. Read here rather than at each call site
    # so the accept-time write and the turn-start reconcile (which re-serializes
    # the same row by id) cannot disagree.
    override = getattr(msg, "additional_kwargs", {}).get(_PERSIST_ROLE_KEY)
    if isinstance(override, str) and override:
        role = override
    content = msg.content if hasattr(msg, "content") else None
    tc = None
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        tc = []
        for t in msg.tool_calls:
            entry: Dict[str, Any] = {
                "name": t.get("name"),
                "args": t.get("args"),
                "id": t.get("id"),
            }
            decision = (tool_decisions or {}).get(t.get("id") or "")
            if decision:
                entry["decision"] = decision
            tc.append(entry)
    # Extract reasoning content + tool-call back-reference BEFORE we flatten
    # Anthropic's list-of-dicts content (which drops the thinking blocks).
    thinking = _extract_thinking(msg) if role == "ai" else None
    tool_call_id = getattr(msg, "tool_call_id", None) if role == "tool" else None
    # Normalize content for Anthropic list-of-dicts format
    if isinstance(content, list):
        content = " ".join(
            b.get("text", "") if isinstance(b, dict) else str(b) for b in content
        )
    # Attach metrics only to AI messages (not tool results)
    msg_metrics = metrics if role == "ai" else None
    row: Dict[str, Any] = {
        "id": getattr(msg, "id", None),
        "role": role,
        "content": content,
        "tool_calls": tc,
        "turn_number": turn_number,
        "metrics": msg_metrics,
        "tool_call_id": tool_call_id,
        "thinking": thinking,
    }
    if role == PERSIST_ROLE_CONTEXT:
        # A typed context entry: restore rebuilds it from its schema, so that
        # one key is persisted (and read back for 'context' rows only). The
        # rest of additional_kwargs (turn stamp, persist role) stays in memory.
        meta = getattr(msg, "additional_kwargs", {}).get(SRW_INJECTION_KEY)
        if isinstance(meta, dict):
            row["additional_kwargs"] = {SRW_INJECTION_KEY: meta}
    return row


async def _persist_one_message(
    client: Any,
    thread_id: str,
    msg: Any,
    turn_number: int,
    *,
    metrics: dict | None = None,
    tool_decisions: Optional[Dict[str, str]] = None,
) -> None:
    """Upsert one serialized message row (the incremental mid-turn durability
    path). Shares the serializer with the turn-complete reconcile."""
    row = _serialize_message_row(
        msg, turn_number, metrics=metrics, tool_decisions=tool_decisions
    )
    await client.save_thread_message(thread_id=thread_id, **row)


__all__ = [
    "_ROLE_MAP",
    "_extract_thinking",
    "_persist_one_message",
    "_serialize_message_row",
]
