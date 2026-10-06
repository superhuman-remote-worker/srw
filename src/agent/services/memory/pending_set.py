"""The durable copy of a session's pending memory set (WP4, D32).

An idle-time prefetch (:meth:`MemoryManager.prefetch`) leaves a
:class:`RetrievalResult` as the conversation's pending set. The next turn may
run in another process, so the set is saved with the conversation, in
``threads.metadata`` (key and size bound in
:mod:`shared.session_pending_memory`). This module turns the set into that
JSON form and back.

Only what the append-only planner needs is stored: per memory the fields
:meth:`RecallStore.format_memory` renders and the presence item reads (id,
content, importance, phase, type); per knowledge note the fields
:meth:`KnowledgeStore.format_note` and ``knowledge_item_key`` read. A record
read back renders to the same bytes and has the same presence key and hash
as the row, so the planner treats it exactly like a fresh retrieval (D3: a
replay after a crash appends nothing). Records are never truncated; the
lowest-ranked ones are dropped until the set fits.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from agent.services.memory.types import InjectionBlock, MemoryPayload, RetrievalResult
from shared.session_pending_memory import (
    SESSION_PENDING_MEMORY_MAX_BYTES,
    SESSION_PENDING_MEMORY_VERSION,
    valid_pending_memory,
)

_MEMORY_FIELDS = ("content", "memory_type", "importance", "source_phase", "token_count")
_KNOWLEDGE_FIELDS = (
    "note_id",
    "title",
    "note_type",
    "status",
    "priority",
    "confidence",
    "tags",
    "phase",
    "content",
    "matched_arms",
)
_KNOWLEDGE_ID_FIELDS = ("id", "project_id", "kb_id")


def _id_text(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _id_value(value: Any) -> Any:
    """A stored id back as a UUID when it is one (the row's own type)."""
    if value is None:
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return str(value)


def _plain(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return value


def _memory_dict(record: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"id": _id_text(getattr(record, "id", None))}
    for name in _MEMORY_FIELDS:
        out[name] = _plain(getattr(record, name, None))
    return out


def _knowledge_dict(record: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        name: _id_text(getattr(record, name, None)) for name in _KNOWLEDGE_ID_FIELDS
    }
    for name in _KNOWLEDGE_FIELDS:
        out[name] = _plain(getattr(record, name, None))
    return out


def _size(payload: Mapping[str, Any]) -> int:
    return len(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )


def serialize_pending_set(
    result: RetrievalResult,
    *,
    turn: Optional[int] = None,
    max_bytes: int = SESSION_PENDING_MEMORY_MAX_BYTES,
) -> Optional[Dict[str, Any]]:
    """The JSON form of a pending set, or None when there is nothing to store.

    Shape (version 1)::

        {"v": 1, "id": "<pending_id>", "turn": 7,
         "created_at": "2026-10-05T12:00:00+00:00",
         "memory": [{"id", "content", "memory_type", "importance",
                     "source_phase", "token_count"}, ...],
         "knowledge": [{"id", "project_id", "kb_id", "note_id", "title",
                        "note_type", "status", "priority", "confidence",
                        "tags", "phase", "content", "matched_arms"}, ...]}

    Records keep their rank order. Past ``max_bytes`` the lowest-ranked
    knowledge notes go first, then the lowest-ranked memories.
    """
    if not result.pending_id:
        return None
    memory = [_memory_dict(r) for r in result.records("memory") if r is not None]
    knowledge = [
        _knowledge_dict(r) for r in result.records("knowledge") if r is not None
    ]
    payload: Dict[str, Any] = {
        "v": SESSION_PENDING_MEMORY_VERSION,
        "id": result.pending_id,
        "turn": turn,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "memory": memory,
        "knowledge": knowledge,
    }
    while (memory or knowledge) and _size(payload) > max_bytes:
        if knowledge:
            knowledge.pop()
        else:
            memory.pop()
    if not (memory or knowledge):
        return None
    return payload


def deserialize_pending_set(value: Any) -> Optional[RetrievalResult]:
    """A stored pending set as a prefetch :class:`RetrievalResult`, or None.

    Accepts the stored mapping or its JSON text; an unreadable value (another
    version, no id, no records) is None. Records come back as
    ``MemoryRecord`` / ``KnowledgeRecord`` with the stored fields.
    """
    from shared.runtime.services.knowledge_store import KnowledgeRecord
    from shared.runtime.services.recall_store import MemoryRecord

    stored = valid_pending_memory(value)
    if stored is None:
        return None
    memories: List[Any] = []
    for item in stored.get("memory") or []:
        if not isinstance(item, Mapping) or not isinstance(item.get("content"), str):
            continue
        fields = {name: item.get(name) for name in _MEMORY_FIELDS}
        fields = {name: v for name, v in fields.items() if v is not None}
        memories.append(MemoryRecord(id=_id_value(item.get("id")), **fields))
    notes: List[Any] = []
    for item in stored.get("knowledge") or []:
        if not isinstance(item, Mapping) or not isinstance(item.get("content"), str):
            continue
        fields = {name: item.get(name) for name in _KNOWLEDGE_FIELDS}
        fields = {name: v for name, v in fields.items() if v is not None}
        ids = {name: _id_value(item.get(name)) for name in _KNOWLEDGE_ID_FIELDS}
        notes.append(KnowledgeRecord(**ids, **fields))
    if not (memories or notes):
        return None
    blocks: List[InjectionBlock] = []
    if memories:
        blocks.append(InjectionBlock(kind="memory", records=memories))
    if notes:
        blocks.append(InjectionBlock(kind="knowledge", records=notes))
    return RetrievalResult(
        payload=MemoryPayload(blocks=blocks),
        seq=0,
        finished_at=time.monotonic(),
        source="prefetch",
        pending_id=str(stored["id"]),
    )


__all__ = ["deserialize_pending_set", "serialize_pending_set"]
