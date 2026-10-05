"""Typed context entries: the vocabulary of append-only context injection.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D3, D27, D28) and the WP2 spec
(knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md
§A, §B, §D).

An *entry* is harness context (memory, knowledge, guidance, ...) stored in
the history as its own message, right after the message it rides on (its
*carrier*): the last tool result of a batch, or the user's message. It is a
``HumanMessage`` whose ``additional_kwargs`` carry the schema below, so every
repair pass keeps it, the session store persists it under its own
``thread_messages.role`` and a builder that misses the fold still sends a
valid user message instead of a 400. It is never an AI, Tool or System
message.

At request build :func:`fold_context_entries` appends each entry's stored
text to its carrier, so the provider sees no extra turn and request N+1
starts with request N byte for byte. Without entries the fold is the
identity, which keeps today's ``legacy`` requests unchanged.

:func:`is_context_injection` is the one predicate for "harness context, not
conversation": typed entries plus every legacy transient shape (the
synthetic tool-call pairs and the transient HumanMessages of the tail).
Summarizer, extraction, archiver, query builders, fork seeding and title
generation all ask it.

This module depends only on ``langchain_core`` and the marker modules, so
any layer can import it without a cycle.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    ToolMessage,
)

from shared.runtime.core.injection_markers import (
    ACTIVE_SUBAGENTS_CONTENT_PREFIX,
    CHARTER_TOOL_CALL_ID_PREFIX,
    CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
    GUIDANCE_TOOL_CALL_ID_PREFIX,
    INSTRUCTION_TOOL_CALL_ID_PREFIX,
    KNOWLEDGE_TOOL_CALL_ID_PREFIX,
    MEMORY_TOOL_CALL_ID_PREFIX,
    PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX,
    TODOS_INJECTION_CONTENT_PREFIX,
)
from shared.runtime.core.message_markers import (
    PERSIST_ROLE_CONTEXT,
    PERSIST_ROLE_KEY,
)

logger = logging.getLogger(__name__)

# --- Schema v1 (spec §A.3) ---------------------------------------------------

SRW_INJECTION_KEY = "srw_injection"
SCHEMA_VERSION = 1
# On folded carrier COPIES only (the request view), never on stored messages.
# Provider converters drop custom kwargs, so it never reaches the wire.
FOLDED_KEY = "_srw_folded"
# Joins a carrier's text and its entries. Part of the prompt-cache contract:
# changing it costs one cache miss per conversation.
ENTRY_SEPARATOR = "\n\n"
# Every kind, in the order one request build appends them.
INJECTION_KINDS = (
    "charter",
    "memory",
    "knowledge",
    "citation",
    "guidance",
    "subagents",
    "turn_boundary",
)
# Kinds whose entries list per-item keys and hashes (presence by item); the
# others are tracked by one state hash per section.
ITEM_KINDS = ("memory", "knowledge", "guidance")

# --- The rollback flag (spec §I) ----------------------------------------------

INJECTION_MODE_LEGACY = "legacy"
INJECTION_MODE_APPEND_ONLY = "append_only"
INJECTION_MODES = (INJECTION_MODE_LEGACY, INJECTION_MODE_APPEND_ONLY)


def is_append_only(config: Any) -> bool:
    """Whether ``config.context_management.injection_mode`` is ``append_only``.

    Strict on purpose: anything but the exact string (a missing section, a
    ``MagicMock`` config in a test) reads as ``legacy``.
    """
    context_management = getattr(config, "context_management", None)
    mode = getattr(context_management, "injection_mode", None)
    return isinstance(mode, str) and mode == INJECTION_MODE_APPEND_ONLY


# --- Text helpers (spec §A.3-§A.5) --------------------------------------------

_WRAP_CLOSE = "\n</srw_context>"


def digest(text: str) -> str:
    """16-hex content hash used for entry and item hashes."""
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()[:16]


def wrap(kind: str, body: str) -> str:
    """The exact stored (and appended) text of an entry of ``kind``."""
    return f'<srw_context kind="{kind}">\n{body.strip()}{_WRAP_CLOSE}'


def memory_handle(record_id: Any) -> str:
    """Short display handle of a memory row (D30), e.g. ``m:3f9a2c``.

    The model sees it as ``[m:3f9a2c]``; the database id stays in kwargs.
    """
    return "m:" + hashlib.sha256(str(record_id).encode("utf-8")).hexdigest()[:6]


_HANDLE_ARGUMENT = re.compile(r"\[?(?:m:)?([0-9a-f]{6})\]?")


def normalize_memory_handle(value: Any) -> Optional[str]:
    """``m:3f9a2c`` from the ways a model writes a handle, else None.

    Accepts ``m:3f9a2c``, ``[m:3f9a2c]`` and the bare ``3f9a2c``, any case.
    """
    match = _HANDLE_ARGUMENT.fullmatch(str(value or "").strip().lower())
    return f"m:{match.group(1)}" if match else None


# A memory block as RecallStore.format_memory renders it with a handle label:
# "[m:3f9a2c]", an optional " (meta...)" (and the updated marker) on the same
# line, then the content. Blocks are joined by ENTRY_SEPARATOR, so a block
# starts the text or follows a blank line.
_MEMORY_BLOCK_HEADER = re.compile(r"(?:\A|\n\n)\[(m:[0-9a-f]{6})\](?: \([^\n]*\))?\n")


def memory_list_items(content: Any) -> List[Tuple[str, str]]:
    """``(handle, digest(content))`` of each handle-labelled memory in a text.

    The inverse of ``RecallStore.render_memory_list`` (the ``memory_search``
    result) and of the blocks of an appended memory entry: a block's content
    runs from the line after its header to the blank line before the next
    header, or to the end. The digest matches the presence hash of the row
    (``digest(record.content)``) as long as the text reached the history
    unchanged; a redacted or truncated block hashes differently and reads as
    a changed memory, which costs at most one extra push. Text without handle
    labels gives an empty list.
    """
    if isinstance(content, list):
        content = "".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    if not isinstance(content, str) or "[m:" not in content:
        return []
    matches = list(_MEMORY_BLOCK_HEADER.finditer(content))
    items: List[Tuple[str, str]] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        items.append((match.group(1), digest(content[match.end() : end])))
    return items


# Rendered after an item whose key is already in the history with another
# hash (D5): the new version is appended, the old one stays above it.
UPDATED_ITEM_MARKER = "(updated; replaces the earlier version above)"


def knowledge_item_key(record: Any) -> str:
    """Presence key of a knowledge note (spec §A.3): ``kb:<kb>:<note_id>``."""
    kb = getattr(record, "kb_id", None) or getattr(record, "project_id", None)
    return f"kb:{kb}:{getattr(record, 'note_id', '')}"


# --- Constructors and accessors (spec §A.8) -----------------------------------


def _normalize_items(items: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        handle = item.get("handle")
        normalized.append(
            {
                "key": str(item["key"]),
                "hash": str(item["hash"]),
                "handle": str(handle) if handle is not None else None,
            }
        )
    return normalized


def make_context_entry(
    kind: str,
    body: str,
    *,
    section: str,
    items: Iterable[Mapping[str, Any]] = (),
    state_hash: Optional[str] = None,
    turn: Optional[int] = None,
    visible: bool = False,
) -> HumanMessage:
    """Build one typed entry: ``wrap(kind, body)`` plus the v1 metadata.

    ``section`` is the presence scope (the kind, or ``turn_boundary:<turn>``);
    ``items`` are ``{key, hash, handle}`` mappings for item kinds; the entry
    hash is ``state_hash`` when given (state kinds), else ``digest(body)``.
    """
    if kind not in INJECTION_KINDS:
        raise ValueError(f"unknown context entry kind {kind!r}")
    meta = {
        "v": SCHEMA_VERSION,
        "kind": kind,
        "section": str(section),
        "items": _normalize_items(items),
        "hash": state_hash if state_hash is not None else digest(body),
        "turn": int(turn) if turn is not None else None,
        "visible": bool(visible),
    }
    return HumanMessage(
        content=wrap(kind, body),
        additional_kwargs={
            SRW_INJECTION_KEY: meta,
            PERSIST_ROLE_KEY: PERSIST_ROLE_CONTEXT,
        },
    )


def _meta_of(kwargs: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(kwargs, dict):
        return None
    meta = kwargs.get(SRW_INJECTION_KEY)
    if isinstance(meta, dict) and isinstance(meta.get("kind"), str):
        return meta
    return None


def entry_meta(msg: Any) -> Optional[Dict[str, Any]]:
    """The ``srw_injection`` metadata of an entry, or None for any other message."""
    if not isinstance(msg, HumanMessage):
        return None
    return _meta_of(getattr(msg, "additional_kwargs", None))


def is_context_entry(msg: Any) -> bool:
    """True for a typed context entry (a HumanMessage carrying the schema)."""
    return entry_meta(msg) is not None


def entry_kind(msg: Any) -> Optional[str]:
    """The kind of an entry, or None for any other message."""
    meta = entry_meta(msg)
    return meta["kind"] if meta is not None else None


def entry_body(msg: Any) -> str:
    """An entry's body without the ``<srw_context>`` wrapper."""
    content = getattr(msg, "content", "")
    text = content if isinstance(content, str) else str(content)
    if text.startswith("<srw_context ") and text.endswith(_WRAP_CLOSE):
        opening_end = text.find(">\n")
        if opening_end != -1:
            return text[opening_end + 2 : -len(_WRAP_CLOSE)]
    return text


def context_entry_from_row(
    content: Any,
    additional_kwargs: Any,
    *,
    id: Optional[str],
) -> Optional[HumanMessage]:
    """Rebuild an entry from a ``thread_messages`` row (``role='context'``).

    ``content`` is replayed exactly as stored (D28: never re-rendered).
    ``additional_kwargs`` may arrive as a JSON string. Returns None when the
    row is unreadable; the caller drops it and the planner re-injects what
    is still relevant (D3).
    """
    if isinstance(additional_kwargs, (str, bytes)):
        try:
            additional_kwargs = json.loads(additional_kwargs)
        except (TypeError, ValueError):
            return None
    meta = _meta_of(additional_kwargs)
    if meta is None or not isinstance(content, str) or not content:
        return None
    return HumanMessage(
        content=content,
        additional_kwargs={
            SRW_INJECTION_KEY: dict(meta),
            PERSIST_ROLE_KEY: PERSIST_ROLE_CONTEXT,
        },
        id=id,
    )


# --- One predicate (spec §D) --------------------------------------------------

_LEGACY_TOOL_CALL_ID_PREFIXES = (
    INSTRUCTION_TOOL_CALL_ID_PREFIX,
    MEMORY_TOOL_CALL_ID_PREFIX,
    KNOWLEDGE_TOOL_CALL_ID_PREFIX,
    CHARTER_TOOL_CALL_ID_PREFIX,
    CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
    GUIDANCE_TOOL_CALL_ID_PREFIX,
)
_LEGACY_HUMAN_CONTENT_PREFIXES = (
    TODOS_INJECTION_CONTENT_PREFIX,
    ACTIVE_SUBAGENTS_CONTENT_PREFIX,
    PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX,
)


def is_legacy_injection(msg: Any) -> bool:
    """True for a piece of the legacy per-request tail.

    The synthetic tool-call pairs (instruction, memory, knowledge, charter,
    citation feedback, supervisor guidance) and the transient HumanMessages
    (``<active_tasks>``, ``<active_subagents>``, the App Guide turn
    boundary). A protected phase block, WP1's ``[TODO_LIST]`` restatement,
    background subagent evidence and image or event messages are history,
    not injections.
    """
    if isinstance(msg, HumanMessage):
        content = msg.content
        return isinstance(content, str) and content.startswith(
            _LEGACY_HUMAN_CONTENT_PREFIXES
        )
    if isinstance(msg, ToolMessage):
        tool_call_id = getattr(msg, "tool_call_id", None) or ""
        return str(tool_call_id).startswith(_LEGACY_TOOL_CALL_ID_PREFIXES)
    if isinstance(msg, AIMessage):
        for call in getattr(msg, "tool_calls", None) or []:
            if str(call.get("id") or "").startswith(_LEGACY_TOOL_CALL_ID_PREFIXES):
                return True
    return False


def is_context_injection(msg: Any) -> bool:
    """True for harness context: a typed entry or a legacy injection."""
    return is_context_entry(msg) or is_legacy_injection(msg)


def last_user_text(messages: Sequence[BaseMessage]) -> str:
    """Text of the newest HumanMessage that is not injected context.

    List (multimodal) content is string-coerced, as the retrieval query
    builders always did; "" when there is none.
    """
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage) and not is_context_injection(msg):
            content = msg.content
            return content if isinstance(content, str) else str(content)
    return ""


# --- The carrier fold (spec §B, D27) ------------------------------------------


def _tool_batch_tails(messages: Sequence[BaseMessage]) -> Dict[int, int]:
    """ToolMessage index -> index of the last ToolMessage of its batch.

    A batch's results all come before the next AIMessage (P3 guard): an
    entry stored between the results of one batch folds into the last one,
    so no text ever sits between the results of one call batch.
    """
    tails: Dict[int, int] = {}
    tail: Optional[int] = None
    for index in range(len(messages) - 1, -1, -1):
        msg = messages[index]
        if isinstance(msg, AIMessage):
            tail = None
        elif isinstance(msg, ToolMessage):
            if tail is None:
                tail = index
            tails[index] = tail
    return tails


def _fold_into(carrier: BaseMessage, entries: List[BaseMessage]) -> BaseMessage:
    texts = [str(entry.content) for entry in entries]
    kinds = [entry_kind(entry) for entry in entries]
    content = carrier.content
    if isinstance(content, list):
        folded: Any = list(content) + [{"type": "text", "text": t} for t in texts]
    else:
        folded = ENTRY_SEPARATOR.join(([content] if content else []) + texts)
    kwargs = dict(getattr(carrier, "additional_kwargs", None) or {})
    kwargs[FOLDED_KEY] = kinds
    return carrier.model_copy(update={"content": folded, "additional_kwargs": kwargs})


def fold_context_entries(messages: Sequence[BaseMessage]) -> List[BaseMessage]:
    """The request view of ``messages``: every entry folded into its carrier.

    Pure: never mutates its input, deterministic, idempotent, and with no
    entries it returns a new list of the same objects. Rules (spec §B):

    - carrier = the nearest preceding non-entry message;
    - a HumanMessage carrier (user input, image or event message, todo
      restatement) takes the entry as is;
    - a ToolMessage carrier hands it to the last result of the same batch;
    - after an AIMessage with tool calls the entry is dropped with a
      warning: nothing may sit between a call and its results;
    - with no carrier, after a SystemMessage or after a text-only AIMessage
      the entry becomes a standalone HumanMessage at its own position.

    A carrier with entries is emitted as a copy with the entry texts
    appended (``ENTRY_SEPARATOR`` for string content, one text part each
    for list content) and ``FOLDED_KEY: [kinds]`` in its kwargs.
    """
    if not any(is_context_entry(msg) for msg in messages):
        return list(messages)

    tails = _tool_batch_tails(messages)
    attached: Dict[int, List[BaseMessage]] = {}
    standalone: Dict[int, BaseMessage] = {}
    carrier: Optional[int] = None
    for index, msg in enumerate(messages):
        if not is_context_entry(msg):
            carrier = index
            continue
        target = messages[carrier] if carrier is not None else None
        if isinstance(target, HumanMessage):
            attached.setdefault(carrier, []).append(msg)
        elif isinstance(target, ToolMessage):
            attached.setdefault(tails.get(carrier, carrier), []).append(msg)
        elif isinstance(target, AIMessage) and getattr(target, "tool_calls", None):
            logger.warning(
                "Dropping a %s context entry: it follows an assistant tool "
                "call, and nothing may sit between a call and its results",
                entry_kind(msg),
            )
        else:
            standalone[index] = HumanMessage(
                content=msg.content,
                additional_kwargs={FOLDED_KEY: [entry_kind(msg)]},
            )

    out: List[BaseMessage] = []
    for index, msg in enumerate(messages):
        if index in standalone:
            out.append(standalone[index])
        elif is_context_entry(msg):
            continue
        elif index in attached:
            out.append(_fold_into(msg, attached[index]))
        else:
            out.append(msg)
    return out


def has_folded_carrier(messages: Sequence[Any]) -> bool:
    """True when the request view holds a folded carrier copy."""
    for msg in messages:
        kwargs = getattr(msg, "additional_kwargs", None)
        if isinstance(kwargs, dict) and FOLDED_KEY in kwargs:
            return True
    return False


__all__ = [
    "ENTRY_SEPARATOR",
    "FOLDED_KEY",
    "INJECTION_KINDS",
    "INJECTION_MODES",
    "INJECTION_MODE_APPEND_ONLY",
    "INJECTION_MODE_LEGACY",
    "ITEM_KINDS",
    "PERSIST_ROLE_CONTEXT",
    "SCHEMA_VERSION",
    "SRW_INJECTION_KEY",
    "UPDATED_ITEM_MARKER",
    "context_entry_from_row",
    "digest",
    "entry_body",
    "entry_kind",
    "entry_meta",
    "fold_context_entries",
    "has_folded_carrier",
    "is_append_only",
    "is_context_entry",
    "is_context_injection",
    "is_legacy_injection",
    "knowledge_item_key",
    "last_user_text",
    "make_context_entry",
    "memory_handle",
    "memory_list_items",
    "normalize_memory_handle",
    "wrap",
]
