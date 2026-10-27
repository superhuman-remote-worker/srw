"""Memories from the conversation that retrieves them (append-only context
injection P4).

The observer extracts memories from a conversation while it still runs, so
other jobs in the project can use them. A retrieval can therefore find a
memory whose source is the current job or session and inject it while the
source messages are still in context. ``memory.retrieval.same_conversation``
decides what happens to such a memory; it is the switch of a deferred test
(vault knowledge-base/knowledge/tests/aoci_same_conversation_memory_reinjection_test.md):

- ``allow`` (arm A, the default): it stays eligible, as before;
- ``skip_in_context`` (arm B): it is skipped while its source turns are
  still in context, and eligible again once a compaction has removed them,
  so a multi-day job still gets its own earlier memories back.

The rule runs in one place, where a finished retrieval is taken in
(:meth:`MemoryManager.take_retrieval`): the request retrieval, the session's
idle prefetch and a pending set read back from the thread all pass there.
``memory_search`` is the model's explicit pull and is never filtered.

"Still in context" compares a memory's source turns with where the
conversation stands (:class:`ConversationTurns`), in the numbering its
memories record:

- **Workers** count one turn per answered main request (``turn_count``); the
  interval extractor records ``source_turn_end`` in it. Nothing records
  where a compaction cut, so the boundary is derived from the history: the
  turns so far minus the assistant responses still in it
  (:func:`worker_conversation_turns`). A synthetic assistant message (the
  "Phase complete" note, a recovery nudge) counts as a response, so the
  boundary can sit a few turns early: a memory just behind it still counts
  as in context.
- **Sessions** count one turn per accepted input; the pinned interval
  extractor and the stateless outbox executor record it. The boundary is the
  compaction checkpoint's ``boundary_turn`` (the compaction turn minus one),
  which the transport keeps on the context manager
  (:func:`session_conversation_turns`). It is turn-granular: the kept window
  can still hold the tail of the turn before the compaction.
- **Memories without turn stamps** (phase-boundary, pre-compaction, teardown,
  compaction-summary and todo memories) have no position. They count as in
  context until the conversation's first compaction and as removed after it:
  a pre-compaction memory exists only once its slice was evicted, while a
  phase-boundary memory written after a compaction can still duplicate
  history.
"""

from __future__ import annotations

import dataclasses
import uuid
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple

from langchain_core.messages import AIMessage, BaseMessage

from agent.services.memory.types import MemoryPayload, RetrievalResult
from shared.runtime.core.context_entries import is_context_injection
from shared.runtime.core.loader import (
    SAME_CONVERSATION_ALLOW,
    SAME_CONVERSATION_MODES,
    SAME_CONVERSATION_SKIP_IN_CONTEXT,
)

#: Key of the arm-B report in ``AssembleStats.extra`` (the ``stats`` of the
#: worker's ``memory_inject`` audit).
STATS_KEY = "same_conversation"


@dataclass(frozen=True)
class ConversationTurns:
    """Where the current conversation stands, in its memories' turn numbering.

    ``compacted_through``: the latest compaction removed the turns up to and
    including this one (0: nothing compacted). ``current``: the
    conversation's current turn, None when unknown. A memory numbered past it
    comes from an earlier run of the conversation (a rewind, a restart
    without the checkpoint) and is not in this history.
    """

    compacted_through: int = 0
    current: Optional[int] = None


def same_conversation_mode(memory_config: Any) -> str:
    """``memory.retrieval.same_conversation``; ``allow`` when unset or unknown."""
    retrieval = getattr(memory_config, "retrieval", None)
    mode = getattr(retrieval, "same_conversation", None)
    return mode if mode in SAME_CONVERSATION_MODES else SAME_CONVERSATION_ALLOW


def conversation_key(value: Any) -> Optional[str]:
    """Canonical text of a job or thread id, a UUID or its string."""
    if value is None:
        return None
    text = str(value).strip()
    try:
        return str(uuid.UUID(text))
    except ValueError:
        return text or None


def _turn(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def source_in_context(record: Any, turns: ConversationTurns) -> bool:
    """Whether the turns a same-conversation memory came from are in context."""
    end = _turn(getattr(record, "source_turn_end", None))
    if end is None:
        end = _turn(getattr(record, "source_turn_start", None))
    if end is None:
        return turns.compacted_through <= 0
    if turns.current is not None and end > turns.current:
        return False
    return end > turns.compacted_through


def worker_conversation_turns(
    messages: Sequence[BaseMessage], turn_count: Any
) -> ConversationTurns:
    """A worker's position: its turns and the responses its history still holds.

    Every answered main request adds one response and one turn; a
    compaction removes the oldest responses, so turns minus the responses
    left is the last turn it removed.
    """
    current = _turn(turn_count) or 0
    responses = sum(
        1
        for message in messages
        if isinstance(message, AIMessage) and not is_context_injection(message)
    )
    return ConversationTurns(
        compacted_through=max(0, current - responses), current=current
    )


def session_conversation_turns(context_manager: Any, turn: Any) -> ConversationTurns:
    """A session's position: the turn and the compaction checkpoint's boundary."""
    boundary = _turn(getattr(context_manager, "compaction_boundary_turn", None))
    current = _turn(turn)
    return ConversationTurns(
        compacted_through=max(0, boundary or 0),
        current=current if current is not None and current > 0 else None,
    )


def skip_in_context(
    result: RetrievalResult, *, conversation_id: Any, turns: ConversationTurns
) -> Tuple[RetrievalResult, int]:
    """Arm B on one retrieval result: ``(result, memories skipped)``.

    Drops the memories of ``conversation_id`` whose source is still in
    context, from both the records the planner renders and the block's
    provenance items. Records what it did under :data:`STATS_KEY` in the
    payload stats, a skip count of 0 included, so a bench run sees that
    arm B ran. Other kinds and other conversations' memories are untouched.
    """
    key = conversation_key(conversation_id)
    same = 0
    skipped = 0
    blocks = []
    for block in result.payload.blocks:
        if block.kind != "memory":
            blocks.append(block)
            continue
        kept = []
        dropped_ids = set()
        for record in block.records:
            if (
                key is not None
                and conversation_key(getattr(record, "job_id", None)) == key
            ):
                same += 1
                if source_in_context(record, turns):
                    record_id = getattr(record, "id", None)
                    if record_id is not None:
                        dropped_ids.add(str(record_id))
                    continue
            kept.append(record)
        if len(kept) == len(block.records):
            blocks.append(block)
            continue
        skipped += len(block.records) - len(kept)
        blocks.append(
            dataclasses.replace(
                block,
                records=kept,
                items=[
                    item
                    for item in block.items
                    if str(item.get("record_id")) not in dropped_ids
                ],
                token_count=sum(getattr(r, "token_count", 0) or 0 for r in kept),
            )
        )
    stats = result.payload.stats
    stats = dataclasses.replace(
        stats,
        extra={
            **stats.extra,
            STATS_KEY: {
                "mode": SAME_CONVERSATION_SKIP_IN_CONTEXT,
                "compacted_through": turns.compacted_through,
                "current_turn": turns.current,
                "same_conversation": same,
                "skipped": skipped,
            },
        },
    )
    return (
        dataclasses.replace(result, payload=MemoryPayload(blocks=blocks, stats=stats)),
        skipped,
    )


def skipped_count(stats: Any) -> int:
    """How many memories arm B skipped in a payload's stats (0: none, or arm A)."""
    extra = getattr(stats, "extra", None)
    report = extra.get(STATS_KEY) if isinstance(extra, dict) else None
    return int(report.get("skipped") or 0) if isinstance(report, dict) else 0


__all__ = [
    "STATS_KEY",
    "ConversationTurns",
    "conversation_key",
    "same_conversation_mode",
    "session_conversation_turns",
    "skip_in_context",
    "skipped_count",
    "source_in_context",
    "worker_conversation_turns",
]
