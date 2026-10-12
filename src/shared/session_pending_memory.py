"""The durable pending memory set of a session thread.

Append-only context injection, WP4 (D24, D32, D33; build notes B8, B9 of
knowledge-base/knowledge/features/append_only_context_injection.md). At the
end of a session turn, after the reply, the agent runs one memory retrieval
with the latest exchange as its query (the idle-time prefetch). Its result
waits for the first request of the next turn. That turn may run in another
process (a stateless pod hop, a pinned restart), so the result is also saved
with the conversation: in ``threads.metadata`` under
:data:`SESSION_PENDING_MEMORY_KEY`, written with an atomic ``jsonb_set``,
never by rewriting the blob. No column, no migration.

Readers:

- the stateless lane gets the set from the claim bundle, which carries it
  beside ``attach`` (B9): inside ``attach`` it would change the attach
  fingerprint and force a re-attach whenever it changes;
- a pinned session reads it at setup in a fresh process.

The set holds only what the context planner needs to render the entries:
memory and knowledge records, bounded in size. The agent writes it first
and clears it after the next turn drained it; a replay after a crash between
the two is harmless, because the planner skips what the history already
holds (D3).

Framework-free: the orchestrator and the agent both import it.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional

#: The ``threads.metadata`` key, and the claim bundle key beside ``attach``.
SESSION_PENDING_MEMORY_KEY = "aoci_pending_memory"

#: Shape version of the stored set; a reader ignores any other version.
SESSION_PENDING_MEMORY_VERSION = 1

#: Upper bound on the serialized set (UTF-8 bytes of the compact JSON). The
#: writer drops the lowest-ranked records until the set fits, it never
#: truncates a record: a truncated text would hash differently from the row
#: and read as a changed memory.
SESSION_PENDING_MEMORY_MAX_BYTES = 32 * 1024


def valid_pending_memory(value: Any) -> Optional[Dict[str, Any]]:
    """``value`` as a pending set when it is one this version can read.

    Accepts the decoded mapping or its JSON text (a ``jsonb`` value read
    without a codec). Anything else, another version, or a set without an
    id is ``None``.
    """
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    if not isinstance(value, Mapping):
        return None
    if value.get("v") != SESSION_PENDING_MEMORY_VERSION:
        return None
    set_id = value.get("id")
    if not isinstance(set_id, str) or not set_id:
        return None
    return dict(value)


def pending_memory_from_metadata(metadata: Any) -> Optional[Dict[str, Any]]:
    """The pending set stored in a ``threads.metadata`` value, if valid."""
    if isinstance(metadata, (str, bytes)):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            return None
    if not isinstance(metadata, Mapping):
        return None
    return valid_pending_memory(metadata.get(SESSION_PENDING_MEMORY_KEY))


__all__ = [
    "SESSION_PENDING_MEMORY_KEY",
    "SESSION_PENDING_MEMORY_MAX_BYTES",
    "SESSION_PENDING_MEMORY_VERSION",
    "pending_memory_from_metadata",
    "valid_pending_memory",
]
