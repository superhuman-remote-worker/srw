"""Marker vocabulary for messages whose LangChain type is only a carrier.

Some messages travel as ``HumanMessage`` because that is the one type every
provider accepts anywhere in a conversation, yet they are not user turns:
an injected system notice in a session, or a worker's phase instruction
block. They declare what they *are* through ``additional_kwargs`` — a
``BaseMessage`` field that survives the LangGraph checkpointer, the
archiver's ``_message_to_dict`` and every history sanitiser (none of them
rebuild HumanMessages).

Two concerns live here, in one tiny stdlib+langchain module so the graph,
the context manager, the archiver and the session transport cannot drift
apart on the spelling:

- ``PERSIST_ROLE_KEY`` — the ``thread_messages.role`` a message should be
  persisted under (sessions; ``"event"`` = system notice, not a user bubble).
- ``PROTECTED_KEY`` / ``PHASE_KEY`` / ``INSTRUCTION_PATH_KEY`` — a
  *protected* message: delivered once, kept out of every compaction
  strategy (tool-result clearing, trimming, keep-window elision, the
  summariser's input) and re-seated right after the summary when the
  region it sat in is summarised — provided its phase key is the current
  one. Today's only producer is the worker's ``phase_start`` binding
  (``src/core/workspace_injection.create_phase_instruction_message``).

Marker contract (``additional_kwargs`` of a phase instruction block)::

    {
        "_srw_persist_role": "event",
        "srw_protected": True,
        "srw_phase_key": "<phase_number>:<strategic|tactical>",
        "srw_instruction_path": "<workspace-relative path>",
    }

``is_protected_message`` tests ``srw_protected`` for truthiness, so a
string tag (``"phase_start"``) qualifies as well as ``True``. A protected
message without ``srw_phase_key`` is a *generic pin*: re-seated after every
summary regardless of the current phase.
"""

from __future__ import annotations

import hashlib
from typing import Any, Optional, Tuple

from langchain_core.messages import BaseMessage

# thread_messages.role override for session persistence (see
# src/persistent_graph.py, which re-exports this name unchanged).
PERSIST_ROLE_KEY = "_srw_persist_role"
PERSIST_ROLE_EVENT = "event"

# Protected-message markers (see module docstring).
PROTECTED_KEY = "srw_protected"
PHASE_KEY = "srw_phase_key"
INSTRUCTION_PATH_KEY = "srw_instruction_path"

# Turn-membership stamp (sessions). The persistent loop stamps every message
# it appends during a turn with that turn's id, so the turn-end reconcile
# selects "this turn's rows" by membership instead of walking back to an
# anchor message that a mid-turn compaction may have summarised away
# (knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md).
# In-memory only: ``_serialize_message_row`` does not persist
# ``additional_kwargs``, and the stamp is never sent to a provider.
TURN_MEMBERSHIP_KEY = "_srw_turn_id"

# Compaction-view mark (sessions). A compaction rewrite that leaves a lossy
# resident copy of a row (keep-window cap, elision, image shedding) keeps the
# row's id and turn stamp — the reconcile still owes the row when its
# incremental write was lost, and a tool call without its result wedges the
# session — and carries this mark, so the reconcile writes it
# insert-if-absent: it fills a missing row, and never overwrites the durable
# full row with the view. Same in-memory-only rule as the stamp.
COMPACTION_VIEW_KEY = "_srw_compaction_view"

# ``PROTECTED_KEY`` value the persistent loop pins the active turn's input
# with: the user's live request survives a mid-turn summary verbatim (generic
# pin, re-seated right after the summary) and is unpinned when the turn ends.
# Distinct from ``True`` so unpinning never strips a marker some other
# producer set on the same message (an injected event notice, say).
PROTECTED_TURN_INPUT = "turn_input"


def phase_key_for(phase_number: Any, phase_name: str) -> str:
    """The ``srw_phase_key`` of a concrete phase instance: ``"<n>:<phase>"``."""
    return f"{phase_number}:{phase_name}"


def _kwargs(message: Any) -> dict:
    kwargs = getattr(message, "additional_kwargs", None)
    return kwargs if isinstance(kwargs, dict) else {}


def is_protected_message(message: Any) -> bool:
    """True when ``message`` carries the protected marker."""
    return bool(_kwargs(message).get(PROTECTED_KEY))


def protected_phase_key(message: Any) -> Optional[str]:
    """The phase key a protected message is bound to, or None (generic pin)."""
    value = _kwargs(message).get(PHASE_KEY)
    return str(value) if value else None


def protected_path(message: Any) -> Optional[str]:
    """The instruction path a protected message delivers, or None."""
    value = _kwargs(message).get(INSTRUCTION_PATH_KEY)
    return str(value) if value else None


def protected_identity(message: BaseMessage) -> Tuple[Optional[str], str]:
    """Dedupe key of a protected message: ``(phase_key, path-or-content-hash)``.

    Two protected messages with the same identity deliver the same artifact
    to the same concrete phase instance; the context manager keeps one.
    """
    path = protected_path(message)
    if path is None:
        content = getattr(message, "content", "")
        text = content if isinstance(content, str) else str(content)
        path = "sha1:" + hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:12]
    return (protected_phase_key(message), path)


def is_pinned_for_phase(message: Any, current_phase_key: Optional[str]) -> bool:
    """Whether a protected message must survive a summary under ``current_phase_key``.

    Generic pins (no phase key) always survive; phase-bound blocks survive
    only while their phase is the current one — earlier phases' blocks are
    summarised away with the region they sat in.
    """
    if not is_protected_message(message):
        return False
    key = protected_phase_key(message)
    return key is None or key == current_phase_key


def stamp_turn_membership(message: Any, turn_id: int) -> Any:
    """Mark ``message`` as produced in turn ``turn_id``; returns the message."""
    kwargs = getattr(message, "additional_kwargs", None)
    if isinstance(kwargs, dict):
        kwargs[TURN_MEMBERSHIP_KEY] = int(turn_id)
    return message


def turn_membership(message: Any) -> Optional[int]:
    """The turn id a message was stamped with, or None (unstamped)."""
    value = _kwargs(message).get(TURN_MEMBERSHIP_KEY)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def mark_compaction_view(message: Any) -> Any:
    """Mark ``message`` as a lossy compaction view of its row; returns it."""
    kwargs = getattr(message, "additional_kwargs", None)
    if isinstance(kwargs, dict):
        kwargs[COMPACTION_VIEW_KEY] = True
    return message


def is_compaction_view(message: Any) -> bool:
    """True when ``message`` is a lossy compaction view of its durable row."""
    return _kwargs(message).get(COMPACTION_VIEW_KEY) is True


def pin_turn_input(message: Any) -> Any:
    """Pin the active turn's input so a mid-turn summary re-seats it verbatim.

    A no-op on a message some other producer already protects (its marker
    and phase binding stay authoritative).
    """
    kwargs = getattr(message, "additional_kwargs", None)
    if isinstance(kwargs, dict) and not kwargs.get(PROTECTED_KEY):
        kwargs[PROTECTED_KEY] = PROTECTED_TURN_INPUT
    return message


def unpin_turn_input(message: Any) -> Any:
    """Remove the pin :func:`pin_turn_input` set; leaves other pins alone."""
    kwargs = getattr(message, "additional_kwargs", None)
    if isinstance(kwargs, dict) and kwargs.get(PROTECTED_KEY) == PROTECTED_TURN_INPUT:
        kwargs.pop(PROTECTED_KEY, None)
    return message


__all__ = [
    "COMPACTION_VIEW_KEY",
    "INSTRUCTION_PATH_KEY",
    "PERSIST_ROLE_EVENT",
    "PERSIST_ROLE_KEY",
    "PHASE_KEY",
    "PROTECTED_KEY",
    "PROTECTED_TURN_INPUT",
    "TURN_MEMBERSHIP_KEY",
    "is_compaction_view",
    "is_pinned_for_phase",
    "is_protected_message",
    "mark_compaction_view",
    "phase_key_for",
    "pin_turn_input",
    "protected_identity",
    "protected_path",
    "protected_phase_key",
    "stamp_turn_membership",
    "turn_membership",
    "unpin_turn_input",
]
