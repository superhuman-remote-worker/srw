"""Serialising LangChain messages into ``thread_messages`` rows.

Lifted out of ``src/agent/api/persistent_app.py`` (U3 WP1) so the session transport
and the subagent runtime (``src/subagents``) share ONE serialiser: a child
thread's transcript must land in the same columns, with the same role
normalisation and reasoning extraction, as a session's. ``persistent_app``
re-exports these names unchanged (its tests import them from there).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional

from shared.runtime.llm.reasoning_chat import extract_reasoning_text_from_block
from shared.runtime.core.context_entries import SRW_INJECTION_KEY
from shared.runtime.core.message_markers import PERSIST_ROLE_CONTEXT, PERSIST_ROLE_EVENT
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


#: provider_raw key for a message's list content as the provider got it: an
#: assistant turn's native blocks, the parts of a tool's image row. JSON text
#: where jsonb would reorder the keys of a block (:func:`_jsonb_text`).
NATIVE_CONTENT_KEY = "content"
#: The key Anthropic turns were stored under before every provider's turn
#: was; restore still reads it.
ANTHROPIC_CONTENT_KEY = "anthropic_content"
#: additional_kwargs a provider SDK reads back when it formats the turn:
#: langchain-google-genai keeps each function call's thought signature here,
#: by tool-call id.
_REPLAYED_KWARGS = ("__gemini_function_call_thought_signatures__",)
#: Replay Responses item ids only this long: OpenAI stores responses 30 days.
RESPONSES_ITEM_RETENTION = timedelta(days=25)


def _native_provider_raw(msg: Any) -> Optional[Dict[str, Any]]:
    """The provider-native form of an assistant turn, for ``provider_raw``.

    ``content`` stores the flattened text, which a restore cannot replay as the
    turn the provider produced: Anthropic's thinking blocks (with their
    signatures), the Responses API's reasoning items and item ids, and
    Gemini's thought parts and signatures are gone, so the next request no
    longer starts with the previous one (a cache miss at that turn) and the
    model loses its earlier reasoning. The blocks (in their key order), the
    producing model and provider, and the kwargs the provider's SDK replays
    are kept here and restored by :func:`native_ai_fields`. A plain-text turn
    needs none of it.
    """
    content = getattr(msg, "content", None)
    kwargs = getattr(msg, "additional_kwargs", None) or {}
    replayed = {k: kwargs[k] for k in _REPLAYED_KWARGS if kwargs.get(k)}
    if not isinstance(content, list) and not replayed:
        return None
    metadata = getattr(msg, "response_metadata", None) or {}
    raw: Dict[str, Any] = {}
    model = metadata.get("model_name") or metadata.get("model")
    if model:
        raw["model"] = model
    if metadata.get("model_provider"):
        raw["provider"] = metadata["model_provider"]
    if isinstance(content, list):
        raw[NATIVE_CONTENT_KEY] = _stored_content(
            [b for b in content if isinstance(b, (dict, str))]
        )
    if replayed:
        raw["additional_kwargs"] = replayed
    return raw


def _aware(value: Any) -> Optional[datetime]:
    """A row's ``created_at`` as an aware datetime; ``None`` without one."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _item_ids_expired(
    provider_raw: Mapping[str, Any], content: list, created_at: Any
) -> bool:
    """Whether a Responses turn's item ids may point at a response OpenAI no
    longer stores.

    Replayed, every later request fails ("Item with id ... not found"), and
    after that long the prompt cache is gone too. Claude's and Gemini's blocks
    are self-contained and never expire; a row without its time counts as
    young.
    """
    if provider_raw.get("provider") != "openai":
        return False
    if not any(isinstance(block, dict) and block.get("id") for block in content):
        return False
    created = _aware(created_at)
    return (
        created is not None
        and datetime.now(timezone.utc) - created > RESPONSES_ITEM_RETENTION
    )


def native_ai_fields(provider_raw: Any, created_at: Any = None) -> Dict[str, Any]:
    """The ``AIMessage`` fields a row's ``provider_raw`` restores.

    ``content`` is the turn's native blocks, ``response_metadata`` names the
    model and provider that produced it (the provider boundary sanitizer and
    each SDK's formatting read them), and ``additional_kwargs`` holds what the
    SDK replays. Empty for a row without the envelope (a plain-text turn, or
    one stored before it existed): the row's text is the content. Empty as
    well for a Responses turn whose row (``created_at``) is older than
    :data:`RESPONSES_ITEM_RETENTION`: it comes back as its text and calls, as
    before the envelope. Shared by the session restore and the
    child-transcript restore.
    """
    if not isinstance(provider_raw, dict):
        return {}
    fields: Dict[str, Any] = {}
    content = _native_content(provider_raw)
    if content is not None and _item_ids_expired(provider_raw, content, created_at):
        return {}
    if content is not None:
        fields["content"] = content
    metadata: Dict[str, Any] = {}
    if provider_raw.get("model"):
        metadata["model_name"] = provider_raw["model"]
    if provider_raw.get("provider"):
        metadata["model_provider"] = provider_raw["provider"]
    if metadata:
        fields["response_metadata"] = metadata
    kwargs = provider_raw.get("additional_kwargs")
    if isinstance(kwargs, dict):
        replayed = {k: kwargs[k] for k in _REPLAYED_KWARGS if kwargs.get(k)}
        if replayed:
            fields["additional_kwargs"] = replayed
    return fields


#: tool_calls entry key: the call's arguments as JSON text.
ARGUMENTS_KEY = "arguments"


def content_text(content: Any) -> str:
    """The text the ``content`` column stores for a message's content.

    Only text-bearing parts of list content join. Joining every part stored a
    [thinking, tool_use] turn as " ", which a restore replays as a
    whitespace-only text block that Anthropic rejects, and a tool's image row
    as its text plus a trailing space.
    """
    if isinstance(content, list):
        parts = [b.get("text", "") if isinstance(b, dict) else str(b) for b in content]
        return " ".join(part for part in parts if part)
    return content if isinstance(content, str) else str(content or "")


def _jsonb_key_order(value: Any) -> Any:
    """``value`` with every object's keys in the order jsonb returns them:
    shorter keys first, keys of one length in byte order."""
    if isinstance(value, dict):
        return {
            key: _jsonb_key_order(value[key])
            for key in sorted(
                value, key=lambda k: (len(str(k).encode()), str(k).encode())
            )
        }
    if isinstance(value, list):
        return [_jsonb_key_order(item) for item in value]
    return value


def _jsonb_text(value: Any) -> Optional[str]:
    """``value`` as JSON text, when a jsonb column would reorder its keys.

    ``tool_calls`` and ``provider_raw`` are jsonb columns, which keep no key
    order, and the provider clients send what a restore hands them in its
    order: a tool call's ``args`` serialised with ``json.dumps``, a turn's
    native blocks (an Anthropic thinking block, a Responses item, a Gemini
    part) as they are. A restored value whose keys came back reordered sends
    other bytes than the live one: a cache miss from that message on. The
    text keeps the order; ``None`` when jsonb keeps it too.
    """
    text = json.dumps(value)
    if text == json.dumps(_jsonb_key_order(value)):
        return None
    return text


def _arguments_text(args: Any) -> Optional[str]:
    """A tool call's arguments as JSON text, when jsonb would reorder them
    (:func:`_jsonb_text`); ``args`` stays for every other reader."""
    if not isinstance(args, dict):
        return None
    return _jsonb_text(args)


def _stored_content(content: list) -> Any:
    """List content for ``provider_raw``: as it is, or as JSON text where
    jsonb would reorder it (read back by :func:`_native_content`)."""
    text = _jsonb_text(content)
    return content if text is None else text


def _native_content(provider_raw: Any) -> Optional[list]:
    """A row's list content from ``provider_raw``, in the live key order.

    Stored as JSON text or as the list (:func:`_stored_content`); an Anthropic
    turn stored before every provider's turn was has its blocks under
    ``anthropic_content``. ``None`` for a row without it, or with unreadable
    text: the row's text is the content then.
    """
    if not isinstance(provider_raw, Mapping):
        return None
    for key in (NATIVE_CONTENT_KEY, ANTHROPIC_CONTENT_KEY):
        value = provider_raw.get(key)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                logger.debug("Unreadable stored %s of a thread_messages row", key)
                value = None
        if isinstance(value, list):
            return value
    return None


def restored_tool_call_args(call: Mapping[str, Any]) -> Any:
    """A stored tool call's ``args``, in the key order the live call had."""
    text = call.get(ARGUMENTS_KEY)
    if isinstance(text, str):
        try:
            return json.loads(text)
        except ValueError:
            logger.debug("Unreadable stored arguments of tool call %s", call.get("id"))
    return call.get("args", {})


def restored_human_content(row: Mapping[str, Any]) -> Any:
    """The content of a human or event row as the live history held it.

    A tool's image row keeps its parts (text, image) in ``provider_raw``, as
    an assistant turn keeps its blocks; the ``content`` column holds their
    text only, for the transcript.
    """
    parts = _native_content(row.get("provider_raw"))
    if parts:
        return parts
    return row.get("content") or ""


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
            arguments = _arguments_text(t.get("args"))
            if arguments is not None:
                entry[ARGUMENTS_KEY] = arguments
            decision = (tool_decisions or {}).get(t.get("id") or "")
            if decision:
                entry["decision"] = decision
            tc.append(entry)
    # Extract reasoning content + tool-call back-reference BEFORE we flatten
    # Anthropic's list-of-dicts content (which drops the thinking blocks).
    thinking = _extract_thinking(msg) if role == "ai" else None
    provider_raw = _native_provider_raw(msg) if role == "ai" else None
    tool_call_id = getattr(msg, "tool_call_id", None) if role == "tool" else None
    if isinstance(content, list):
        if role in ("human", PERSIST_ROLE_EVENT):
            # A tool's image row: the image and the text parts as the live
            # history holds them, so a restore sends what the live loop sends
            # (the image included) and a context entry folds in as a part.
            provider_raw = {NATIVE_CONTENT_KEY: _stored_content(content)}
        content = content_text(content)
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
    if provider_raw is not None:
        row["provider_raw"] = provider_raw
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
    "ARGUMENTS_KEY",
    "NATIVE_CONTENT_KEY",
    "RESPONSES_ITEM_RETENTION",
    "_ROLE_MAP",
    "_extract_thinking",
    "_persist_one_message",
    "_serialize_message_row",
    "content_text",
    "native_ai_fields",
    "restored_human_content",
    "restored_tool_call_args",
]
