"""contract — the events of a phone call, independent of the voice engine.

A voice engine (GPT-Live today, a local turn-based chain later) carries the
audio and the spoken conversation. The job's phone subagent sees the call only
through these events, and steers it only through :class:`AppendKind` appends
plus transfer and hang-up. The shapes mirror GPT-Live's delegation protocol
(design §4.4) so the GPT-Live adapter is a thin mapping and a later engine
implements the same events.

Pure data: no I/O and no imports beyond the standard library.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Optional, Union

# GPT-Live's limit per instructions/thinking/commentary append, adopted as the
# contract limit for every engine.
MAX_APPEND_TOKENS = 500
# A conservative character budget for that limit. German averages a little
# under four characters per token; three keeps a margin for numbers and names.
MAX_APPEND_CHARS = MAX_APPEND_TOKENS * 3


class CallDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class CallStatus(StrEnum):
    """Lifecycle of one call row."""

    RINGING = (
        "ringing"  # webhook received or outbound placed; not yet accepted/answered
    )
    ACTIVE = "active"  # accepted (inbound) or answered (outbound)
    ENDED = "ended"  # session.closed seen
    REJECTED = "rejected"  # declined before accept
    FAILED = "failed"  # accept/attach/transport failed


TERMINAL_CALL_STATUSES = frozenset(
    {CallStatus.ENDED, CallStatus.REJECTED, CallStatus.FAILED}
)


class Speaker(StrEnum):
    CALLER = "caller"
    ASSISTANT = "assistant"


class AppendKind(StrEnum):
    """How the job adds context to the live conversation.

    * ``INSTRUCTIONS`` — trusted steering; can interrupt speech; exact wording
      (the AI disclosure goes here).
    * ``THINKING`` — quiet facts the voice can use in later replies.
    * ``COMMENTARY`` — something to say aloud now; the voice paraphrases it.
    """

    INSTRUCTIONS = "instructions"
    THINKING = "thinking"
    COMMENTARY = "commentary"


class CloseReason(StrEnum):
    CLOSE_REQUESTED = "close_requested"
    EXPIRED = "expired"
    CONTENT = "content"
    REMOTE_HANGUP = "remote_hangup"
    CONNECTION_LOST = "connection_lost"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TranscriptDelta:
    """A fragment of what one side said. Not a whole turn: group by speaker."""

    speaker: Speaker
    text: str
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    event_id: Optional[str] = None


@dataclass(frozen=True)
class DelegationCreated:
    """The voice asks the backend for help.

    It carries no task text: the request is whatever the caller said since the
    previous delegation, read from the transcript.
    """

    delegation_id: str
    offset_ms: Optional[int] = None
    target: str = "client"
    event_id: Optional[str] = None


@dataclass(frozen=True)
class AppendAcknowledged:
    """An append reached the session timeline (context delivery, not speech)."""

    kind: AppendKind
    client_event_id: Optional[str]
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    event_id: Optional[str] = None


@dataclass(frozen=True)
class EngineError:
    """A rejected command or a session-level error that did not end the call."""

    code: Optional[str]
    message: str
    error_type: Optional[str] = None
    param: Optional[str] = None
    client_event_id: Optional[str] = None
    event_id: Optional[str] = None


@dataclass(frozen=True)
class DtmfReceived:
    digit: str
    event_id: Optional[str] = None


@dataclass(frozen=True)
class TransportProgress:
    """Outbound call progress: ``ringing``, ``answered`` or ``failed``."""

    state: str
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    event_id: Optional[str] = None


@dataclass(frozen=True)
class UsageUpdated:
    """Cumulative voice seconds so far (the latest value is the total)."""

    seconds: float
    context_usage_ratio: Optional[float] = None
    event_id: Optional[str] = None


@dataclass(frozen=True)
class SessionClosed:
    """The session finalised. The final usage is confirmed only by this event."""

    reason: CloseReason
    usage_seconds: Optional[float] = None
    event_id: Optional[str] = None
    raw_reason: Optional[str] = None


@dataclass(frozen=True)
class OtherEvent:
    """An engine event the contract does not model, kept for the record."""

    type: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    event_id: Optional[str] = None


CallEvent = Union[
    TranscriptDelta,
    DelegationCreated,
    AppendAcknowledged,
    EngineError,
    DtmfReceived,
    TransportProgress,
    UsageUpdated,
    SessionClosed,
    OtherEvent,
]


def clip_append_content(text: str, limit: int = MAX_APPEND_CHARS) -> str:
    """Trim ``text`` to the append budget, preferring a sentence boundary.

    An over-long append is rejected by the engine, which would leave a caller
    waiting for an answer that never comes. A trimmed answer is better.
    """

    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:limit]
    for mark in (". ", "! ", "? ", "\n"):
        idx = cut.rfind(mark)
        if idx >= limit // 2:
            return cut[: idx + 1].strip()
    space = cut.rfind(" ")
    if space >= limit // 2:
        cut = cut[:space]
    return cut.rstrip() + " …"
