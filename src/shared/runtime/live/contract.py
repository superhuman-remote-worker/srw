"""contract — the events of a live session, independent of the engine.

A voice engine (GPT-Live today, a local turn-based chain later) carries the
audio and the spoken conversation. The live connector's pod holds the
session; the job's voice subagent sees it only through these events (the
pod's event stream and the orchestrator's journal), and steers it only
through :class:`AppendKind` appends plus transfer and hang-up. The shapes
mirror GPT-Live's delegation protocol so an engine adapter is a thin mapping.

:func:`event_from_wire` reads one event of the pod's stream (the Go driver
``drivers/gpt-live-sip`` writes them); :data:`JOURNAL_KINDS` are the kinds of
``live_session_events`` rows.

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


class Direction(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class SessionStatus(StrEnum):
    """Lifecycle of one ``live_sessions`` row."""

    RINGING = "ringing"  # received; not yet accepted
    ACTIVE = "active"  # accepted by the connector's pod
    ENDED = "ended"  # session.closed seen (or the pod lost it)
    REJECTED = "rejected"  # declined before accept
    FAILED = "failed"  # admission, handoff or accept failed


TERMINAL_SESSION_STATUSES = frozenset(
    {SessionStatus.ENDED, SessionStatus.REJECTED, SessionStatus.FAILED}
)

#: The statuses each status may follow; every update is monotonic.
SESSION_STATUS_PREDECESSORS: dict[str, frozenset[str]] = {
    SessionStatus.ACTIVE: frozenset({SessionStatus.RINGING}),
    SessionStatus.ENDED: frozenset({SessionStatus.ACTIVE}),
    SessionStatus.REJECTED: frozenset({SessionStatus.RINGING}),
    SessionStatus.FAILED: frozenset({SessionStatus.RINGING, SessionStatus.ACTIVE}),
}

#: The kinds of a session's journal rows (``live_session_events.kind``).
JOURNAL_KINDS = frozenset(
    "transcript delegation answer instruction thinking fallback question "
    "follow_up transfer hangup dtmf closed error system".split()
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
    # The pod could not re-attach the sideband for 30 s (404/409).
    RECORDER_LOST = "recorder_lost"


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


LiveEvent = Union[
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


def _int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def _float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _str(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def _close_reason(value: Any) -> CloseReason:
    try:
        return CloseReason(str(value))
    except ValueError:
        return CloseReason.UNKNOWN


def event_from_wire(data: Mapping[str, Any]) -> Optional[LiveEvent]:
    """One event of a live connector pod's stream (its SSE ``data``), or
    ``None`` for a shape this contract does not read."""
    kind = data.get("type")
    event_id = _str(data.get("event_id"))
    if kind == "transcript":
        speaker = data.get("speaker")
        if speaker not in (Speaker.CALLER, Speaker.ASSISTANT):
            return None
        return TranscriptDelta(
            speaker=Speaker(speaker),
            text=str(data.get("text") or ""),
            start_ms=_int(data.get("start_ms")),
            end_ms=_int(data.get("end_ms")),
            event_id=event_id,
        )
    if kind == "delegation":
        delegation_id = _str(data.get("delegation_id"))
        if not delegation_id:
            return None
        return DelegationCreated(
            delegation_id=delegation_id,
            offset_ms=_int(data.get("offset_ms")),
            target=str(data.get("target") or "client"),
            event_id=event_id,
        )
    if kind == "append_ack":
        try:
            append_kind = AppendKind(str(data.get("kind")))
        except ValueError:
            return None
        return AppendAcknowledged(
            kind=append_kind,
            client_event_id=_str(data.get("client_event_id")),
            start_ms=_int(data.get("start_ms")),
            end_ms=_int(data.get("end_ms")),
            event_id=event_id,
        )
    if kind == "error":
        return EngineError(
            code=_str(data.get("code")),
            message=str(data.get("message") or ""),
            error_type=_str(data.get("error_type")),
            param=_str(data.get("param")),
            client_event_id=_str(data.get("client_event_id")),
            event_id=event_id,
        )
    if kind == "dtmf":
        return DtmfReceived(digit=str(data.get("digit") or ""), event_id=event_id)
    if kind == "transport":
        return TransportProgress(
            state=str(data.get("state") or ""),
            error_code=_str(data.get("error_code")),
            error_message=_str(data.get("error_message")),
            event_id=event_id,
        )
    if kind == "usage":
        seconds = _float(data.get("seconds"))
        if seconds is None:
            return None
        return UsageUpdated(
            seconds=seconds,
            context_usage_ratio=_float(data.get("context_usage_ratio")),
            event_id=event_id,
        )
    if kind == "closed":
        return SessionClosed(
            reason=_close_reason(data.get("reason")),
            usage_seconds=_float(data.get("usage_seconds")),
            event_id=event_id,
            raw_reason=_str(data.get("raw_reason")),
        )
    if kind == "other":
        payload = data.get("payload")
        return OtherEvent(
            type=str(data.get("wire_type") or ""),
            payload=payload if isinstance(payload, Mapping) else {},
            event_id=event_id,
        )
    return None
