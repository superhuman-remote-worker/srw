"""live — the voice subagent: a background child whose driver is a live session.

A live-session job (``config.live_session``, injected at dispatch) is born
with a session already running on one of its connectors: a full-duplex voice
engine (GPT-Live) talks to the caller, and the connector's pod
(``drivers/gpt-live-sip``) holds the session. The job sees that session as a
background subagent of its roster's live entry (``driver: live``), driven
with the ordinary subagent tools (design
``knowledge-base/knowledge/features/live_session_jobs.md`` Part I §6.2). This
driver:

* keeps the transcript (turns grouped from deltas) from the pod's event
  stream, backfilled from the orchestrator's journal of the session (the
  stream replays only the last 3 s): the stream is opened first, then the
  journal is read, and both are deduplicated by ``event_id``;
* turns every delegation into a **question** for the main agent — the
  request is what the caller said since the previous delegation — and hands
  it to the runtime, which wakes ``wait_agent``;
* speaks the main agent's answer (``message_agent(reply_to=…)`` → a
  ``commentary`` append with the delegation id) and its steering
  (``message_agent`` without ``reply_to`` → an ``instructions`` append);
* after ``answer_timeout_s`` without an answer, tells the caller the request
  is noted and marks the question a follow-up for after the session;
* ends with the session's ``closed`` event and returns the transcript, the
  questions and the follow-ups as its report.

It never ends a session because its job is going away: only an explicit
``stop_agent`` (reason :data:`PARENT_STOP_REASON`) says goodbye and hangs up.
Any other stop detaches, and the pod keeps covering the session (its fallback
answers, the line's maximum duration). The driver duck-types the parts of
``SubagentDriver`` the runtime uses.

The job's lease for the connector authenticates every pod call. It is read
from the job payload in process (:func:`live_lease_from_payload`) and lives
only in :class:`LiveSessionSettings` and the pod client's headers: never in
config, graph state, the workspace, a log line, a journal row or the report.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)
from urllib.parse import quote

import aiohttp

from agent.subagents.driver import SubagentResult
from shared.runtime.live.contract import (
    TERMINAL_SESSION_STATUSES,
    AppendKind,
    DelegationCreated,
    EngineError,
    SessionClosed,
    Speaker,
    TranscriptDelta,
    UsageUpdated,
    clip_append_content,
    event_from_wire,
)

logger = logging.getLogger(__name__)

LIVE_DRIVER = "live"
#: The reason ``SubagentRuntime.stop_agent`` passes; only this one hangs up.
PARENT_STOP_REASON = "parent requested stop"
#: The refusal ``delegate_agent`` returns for a live entry (single attach).
NOT_DELEGATED = "a live voice subagent is attached by its session, not delegated"

DEFAULT_ANSWER_TIMEOUT_S = 25.0
#: What the voice tells the caller when an answer is late (``live.texts``
#: overrides it). A ``commentary`` append: the voice says it in its own words,
#: in the caller's language.
DEFAULT_FOLLOW_UP = (
    "Tell the caller that the request is noted, that it will be handled after "
    "this conversation, and that they will hear back."
)
#: What ``stop_agent`` tells the voice before it hangs up (``live.texts``).
DEFAULT_GOODBYE = (
    "Say a short, polite goodbye to the caller now and tell them that the "
    "conversation is ending."
)
_TRANSCRIPT_TAIL_TURNS = 8
_GOODBYE_GRACE_S = 6.0
#: How long an append or a stop waits for the first connection to the pod.
_CONNECT_WAIT_S = 5.0
#: A job can be dispatched a moment before the orchestrator's handoff to the
#: pod completes, so the pod answers 404/409 for a session the journal still
#: shows ``ringing`` or ``active``: retry that long before the session counts
#: as gone.
_NOT_YET_WINDOW_S = 30.0
#: How long the pod may stay unreachable (connection errors, 5xx, a stream
#: that drops at once) before the session counts as lost to this job.
_UNREACHABLE_WINDOW_S = 120.0
_BACKOFF_FIRST_S = 0.25
_BACKOFF_MAX_S = 2.0
#: A stream open this long counts as a working connection again.
_STREAM_HELD_S = 5.0
_ATTACH_ATTEMPTS = 5
_APPEND_ATTEMPTS = 3
_SPEAKER_LABEL = {"caller": "Caller", "assistant": "Assistant"}
_TERMINAL_STATUSES = frozenset(str(status) for status in TERMINAL_SESSION_STATUSES)


def is_live_entry(entry: Optional[Mapping[str, Any]]) -> bool:
    """Whether a resolved roster entry is a live voice subagent (``driver: live``)."""

    if not isinstance(entry, Mapping):
        return False
    return str(entry.get("driver") or "").strip().lower() == LIVE_DRIVER


def live_entry_name(roster: Mapping[str, Any]) -> Optional[str]:
    """The roster's live voice entry (the first one), if it has one."""

    return next((str(n) for n, e in roster.items() if is_live_entry(e)), None)


def live_session_block(config: Any) -> Optional[Mapping[str, Any]]:
    """``config.live_session`` when this job serves a live session."""

    block = config.get("live_session") if isinstance(config, Mapping) else None
    if not isinstance(block, Mapping) or not block.get("session_id"):
        return None
    return block


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LiveLease:
    """The job's lease for its live connector, read from the job payload."""

    token: str = field(repr=False)
    connector_id: str = ""
    connection_url: str = ""


def live_lease_from_payload(
    datasources: Optional[Iterable[Any]], connector_id: str
) -> Optional[LiveLease]:
    """The lease of the payload entry for ``connector_id`` (a live driver's).

    Lease delivery puts it where an inline driver's secret would be
    (``credentials.lease``); for a live driver nothing is materialized in the
    workspace (``agent.connectors.legacy``), so the voice adapter reads it here.
    """

    from shared.connectors.builtin import spec_for_type

    for entry in datasources or []:
        if not isinstance(entry, Mapping):
            continue
        if connector_id and str(entry.get("datasource_id") or "") != connector_id:
            continue
        spec = spec_for_type(entry.get("type"))
        if spec is None or not getattr(spec, "live", False):
            continue
        credentials = entry.get("credentials")
        lease = credentials.get("lease") if isinstance(credentials, Mapping) else None
        if not isinstance(lease, Mapping):
            continue
        token = lease.get("token")
        lease_connector = str(lease.get("connector_id") or "")
        if not isinstance(token, str) or not token:
            continue
        if connector_id and lease_connector and lease_connector != connector_id:
            continue
        return LiveLease(
            token=token,
            connector_id=lease_connector or connector_id,
            connection_url=str(entry.get("connection_url") or ""),
        )
    return None


@dataclass(frozen=True)
class LiveSessionSettings:
    """What the job knows about its session: ``config.live_session``, the
    payload's lease and the roster entry's ``live`` block. No engine key."""

    session_id: str
    endpoint_url: str
    connector_id: str = ""
    provider_session_id: str = ""
    lease_token: str = field(default="", repr=False)
    answer_timeout_s: float = DEFAULT_ANSWER_TIMEOUT_S
    follow_up: str = DEFAULT_FOLLOW_UP
    goodbye: str = DEFAULT_GOODBYE

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        entry: Optional[Mapping[str, Any]] = None,
        lease: Optional[LiveLease] = None,
    ) -> "LiveSessionSettings":
        block = live_session_block(config)
        if block is None:
            raise ValueError(
                "this job has no live session (config.live_session is missing)"
            )
        # The payload entry's connection URL wins: it is the connector's
        # current Service, the injected one a copy from dispatch time.
        endpoint = str(
            (lease.connection_url if lease else "") or block.get("endpoint_url") or ""
        )
        if not endpoint:
            raise ValueError("config.live_session has no endpoint_url")
        live = (entry or {}).get("live")
        live = live if isinstance(live, Mapping) else {}
        texts = live.get("texts") if isinstance(live.get("texts"), Mapping) else {}
        try:
            timeout = float(live.get("answer_timeout_s") or DEFAULT_ANSWER_TIMEOUT_S)
        except (TypeError, ValueError):
            timeout = DEFAULT_ANSWER_TIMEOUT_S
        return cls(
            session_id=str(block["session_id"]),
            endpoint_url=endpoint,
            connector_id=str(block.get("connector_id") or ""),
            provider_session_id=str(block.get("provider_session_id") or ""),
            lease_token=lease.token if lease else "",
            answer_timeout_s=timeout if timeout > 0 else DEFAULT_ANSWER_TIMEOUT_S,
            follow_up=str(texts.get("follow_up") or DEFAULT_FOLLOW_UP),
            goodbye=str(texts.get("goodbye") or DEFAULT_GOODBYE),
        )


class PodError(Exception):
    """A pod call that failed: ``status`` 0 when the pod was not reached."""

    def __init__(self, status: int, code: str = "", message: str = "") -> None:
        super().__init__(f"HTTP {status}" if status else "unreachable")
        self.status = status
        self.code = code
        self.message = message

    def describe(self) -> str:
        head = f"HTTP {self.status}" if self.status else "pod unreachable"
        detail = ": ".join(part for part in (self.code, self.message) if part)
        return f"{head} ({detail})" if detail else head


async def _json_body(resp: aiohttp.ClientResponse) -> Dict[str, Any]:
    try:
        body = await resp.json(content_type=None)
    except (aiohttp.ClientError, ValueError, asyncio.TimeoutError):
        return {}
    return body if isinstance(body, dict) else {}


def _error_of(body: Mapping[str, Any]) -> Tuple[str, str]:
    error = body.get("error")
    if not isinstance(error, Mapping):
        return "", ""
    return str(error.get("code") or ""), str(error.get("message") or "")[:300]


class PodClient:
    """The live connector pod's session API and the orchestrator's journal.

    Pod calls carry the job's lease (``Authorization: Bearer scl_…``); journal
    calls the internal key (``X-Internal-Key``). Journal access is best
    effort: a failure is logged and the session goes on — the pod journals
    the transcript itself.
    """

    def __init__(
        self,
        endpoint_url: str,
        session_id: str,
        lease_token: str,
        *,
        journal_url: str,
        internal_key: str = "",
        timeout_s: float = 10.0,
    ) -> None:
        self.endpoint_url = endpoint_url.rstrip("/")
        self.session_id = session_id
        self.journal_url = journal_url.rstrip("/")
        self._pod_headers = (
            {"Authorization": f"Bearer {lease_token}"} if lease_token else {}
        )
        self._journal_headers = {"X-Internal-Key": internal_key} if internal_key else {}
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        # The event stream is long-lived: no total timeout, but a read gap
        # well past the pod's 15 s pings means the connection is dead.
        self._stream_timeout = aiohttp.ClientTimeout(
            total=None, sock_connect=timeout_s, sock_read=45.0
        )
        self._pod_http: Optional[aiohttp.ClientSession] = None
        self._stream_http: Optional[aiohttp.ClientSession] = None
        self._journal_http: Optional[aiohttp.ClientSession] = None

    @classmethod
    def for_settings(cls, settings: LiveSessionSettings) -> "PodClient":
        return cls(
            settings.endpoint_url,
            settings.session_id,
            settings.lease_token,
            journal_url=os.getenv("ORCHESTRATOR_URL", "http://localhost:8085"),
            internal_key=os.getenv("MCP_INTERNAL_KEY", ""),
        )

    def __repr__(self) -> str:
        return f"PodClient({self.endpoint_url!r}, session={self.session_id!r})"

    # -- HTTP sessions -----------------------------------------------------
    def _pod(self) -> aiohttp.ClientSession:
        if self._pod_http is None or self._pod_http.closed:
            self._pod_http = aiohttp.ClientSession(
                headers=self._pod_headers, timeout=self._timeout
            )
        return self._pod_http

    def _stream(self) -> aiohttp.ClientSession:
        if self._stream_http is None or self._stream_http.closed:
            self._stream_http = aiohttp.ClientSession(
                headers=self._pod_headers, timeout=self._stream_timeout
            )
        return self._stream_http

    def _journal(self) -> aiohttp.ClientSession:
        if self._journal_http is None or self._journal_http.closed:
            self._journal_http = aiohttp.ClientSession(
                headers=self._journal_headers, timeout=self._timeout
            )
        return self._journal_http

    def _session_url(self, suffix: str = "") -> str:
        sid = quote(self.session_id, safe="")
        return f"{self.endpoint_url}/v1/sessions/{sid}{suffix}"

    def _journal_events_url(self) -> str:
        sid = quote(self.session_id, safe="")
        return f"{self.journal_url}/api/internal/live/sessions/{sid}/events"

    # -- the pod -------------------------------------------------------------
    async def _call(
        self, method: str, suffix: str, body: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        try:
            async with self._pod().request(
                method, self._session_url(suffix), json=body
            ) as resp:
                return resp.status, await _json_body(resp)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PodError(0, "unreachable", type(exc).__name__) from None

    async def _control(
        self, suffix: str, body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        status, payload = await self._call("POST", suffix, body or {})
        if status >= 400:
            raise PodError(status, *_error_of(payload))
        return payload

    async def attach(self) -> Dict[str, Any]:
        """Mark the session attached by its job (ends the pod's no-job fallback)."""
        return await self._control("/attach")

    async def status(self) -> Dict[str, Any]:
        status, payload = await self._call("GET", "")
        if status >= 400:
            raise PodError(status, *_error_of(payload))
        return payload

    async def append(
        self,
        kind: AppendKind,
        text: str,
        delegation_id: Optional[str],
        event_id: str,
    ) -> Dict[str, Any]:
        """One append: ``{"ok": true}`` or ``{"ok": false, "error": …}``."""
        status, payload = await self._call(
            "POST",
            "/append",
            {
                "kind": kind.value,
                "text": text,
                "delegation_id": delegation_id,
                "event_id": event_id,
            },
        )
        if status >= 400:
            raise PodError(status, *_error_of(payload))
        return payload

    async def transfer(self, target: Optional[str] = None) -> Dict[str, Any]:
        return await self._control("/transfer", {"target": target} if target else {})

    async def hangup(self) -> Dict[str, Any]:
        return await self._control("/hangup")

    async def open_events(self) -> aiohttp.ClientResponse:
        """Open the session's event stream (the caller closes the response)."""
        try:
            resp = await self._stream().get(
                self._session_url("/events"),
                headers={"Accept": "text/event-stream"},
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PodError(0, "unreachable", type(exc).__name__) from None
        if resp.status != 200:
            payload = await _json_body(resp)
            resp.release()
            raise PodError(resp.status, *_error_of(payload))
        return resp

    @staticmethod
    async def read_events(
        resp: aiohttp.ClientResponse,
    ) -> AsyncIterator[Tuple[Optional[str], str, str]]:
        """``(id, event, data)`` per server-sent event; comments (pings) skipped."""
        event_id: Optional[str] = None
        event_type = ""
        data: List[str] = []
        async for raw in resp.content:
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if not line:
                if data:
                    yield event_id, event_type, "\n".join(data)
                event_id, event_type, data = None, "", []
                continue
            if line.startswith(":"):
                continue
            name, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if name == "id":
                event_id = value
            elif name == "event":
                event_type = value
            elif name == "data":
                data.append(value)
        if data:
            yield event_id, event_type, "\n".join(data)

    # -- the journal -----------------------------------------------------------
    async def journal(self, after_id: int = 0) -> Dict[str, Any]:
        """Journal rows after ``after_id`` and the session row.

        ``{"ok", "events", "session", "next_after_id"}``; ``ok`` is false when
        the journal could not be read (``session`` is then ``None``).
        """
        events: List[Dict[str, Any]] = []
        session: Optional[Dict[str, Any]] = None
        cursor = after_id
        ok = False
        try:
            for _ in range(50):
                async with self._journal().get(
                    self._journal_events_url(),
                    params={"after_id": str(cursor), "limit": "500"},
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(
                            "live session %s: journal read refused (HTTP %s)",
                            self.session_id,
                            resp.status,
                        )
                        break
                    body = await resp.json(content_type=None)
                if not isinstance(body, dict):
                    break
                ok = True
                if isinstance(body.get("session"), dict):
                    session = body["session"]
                page = [
                    row for row in body.get("events") or [] if isinstance(row, dict)
                ]
                events.extend(page)
                next_after = int(body.get("next_after_id") or cursor)
                if not page or next_after <= cursor:
                    break
                cursor = next_after
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            logger.warning(
                "live session %s: journal read failed: %s",
                self.session_id,
                type(exc).__name__,
            )
        return {
            "ok": ok,
            "events": events,
            "session": session if ok else None,
            "next_after_id": cursor,
        }

    async def write_journal(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        try:
            async with self._journal().post(
                self._journal_events_url(), json={"events": events}
            ) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "live session %s: journal write refused (HTTP %s)",
                        self.session_id,
                        resp.status,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "live session %s: journal write failed: %s",
                self.session_id,
                type(exc).__name__,
            )

    async def close(self) -> None:
        for http in (self._pod_http, self._stream_http, self._journal_http):
            if http is not None and not http.closed:
                await http.close()


@dataclass
class Turn:
    speaker: str
    text: str
    start_ms: Optional[int] = None


class Transcript:
    """Transcript deltas grouped into turns (a turn = consecutive deltas of one speaker)."""

    def __init__(self) -> None:
        self.turns: List[Turn] = []
        self._seen: Set[str] = set()

    def add(
        self,
        speaker: str,
        text: str,
        start_ms: Optional[int] = None,
        event_id: Optional[str] = None,
    ) -> bool:
        if event_id:
            if event_id in self._seen:
                return False
            self._seen.add(event_id)
        if not text:
            return False
        if self.turns and self.turns[-1].speaker == speaker:
            self.turns[-1].text += text
        else:
            self.turns.append(Turn(speaker=speaker, text=text, start_ms=start_ms))
        return True

    def caller_text_since(self, turn_index: int) -> str:
        parts = [
            t.text.strip()
            for t in self.turns[turn_index:]
            if t.speaker == "caller" and t.text.strip()
        ]
        return " ".join(parts)

    def last_caller_text(self) -> str:
        for turn in reversed(self.turns):
            if turn.speaker == "caller" and turn.text.strip():
                return turn.text.strip()
        return ""

    def render(self, last: Optional[int] = None) -> str:
        turns = self.turns[-last:] if last else self.turns
        return "\n".join(
            f"{_SPEAKER_LABEL.get(t.speaker, t.speaker)}: {t.text.strip()}"
            for t in turns
            if t.text.strip()
        )


class _Outage:
    """One stretch of failed pod calls: its start and the next backoff."""

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._since: Optional[float] = None
        self._attempt = 0

    def reset(self) -> None:
        self._since = None
        self._attempt = 0

    def elapsed(self) -> float:
        if self._since is None:
            self._since = self._clock()
        return self._clock() - self._since

    def next_delay(self) -> float:
        delay = min(_BACKOFF_MAX_S, _BACKOFF_FIRST_S * (2**self._attempt))
        self._attempt += 1
        return delay


QuestionCallback = Callable[[Dict[str, Any]], Awaitable[None]]


class LiveVoiceDriver:
    """Drives one live session as a background child (duck-types ``SubagentDriver``)."""

    def __init__(
        self,
        settings: LiveSessionSettings,
        *,
        handle: str,
        subagent_type: str,
        subagent_id: str,
        job_id: Optional[str] = None,
        on_question: Optional[QuestionCallback] = None,
        on_change: Optional[Callable[[], Awaitable[None]]] = None,
        client: Optional[PodClient] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.handle = handle
        self.subagent_type = subagent_type
        self.subagent_id = subagent_id
        self.job_id = job_id
        self.running = False
        self.connected = False
        self.provider_calls = 0
        self.tokens = 0
        self.transcript = Transcript()
        #: delegation_id → question dict (shared with ``BackgroundRun.questions``).
        self.questions: Dict[str, Dict[str, Any]] = {}
        self.close_reason: Optional[str] = None
        self.usage_seconds: Optional[float] = None
        self.attach_error: Optional[str] = None
        self._on_question = on_question
        self._on_change = on_change
        self._client = client or PodClient.for_settings(settings)
        self._clock = clock
        self._connect_settled = asyncio.Event()
        self._stop = asyncio.Event()
        self._stop_reason: Optional[str] = None
        self._loop_done = asyncio.Event()
        self._tasks: Set[asyncio.Task] = set()
        self._started_at: Optional[float] = None
        self._finished = False
        self._last_delegation_turn = 0
        self._seen_event_ids: Set[str] = set()
        self._journal_cursor = 0

    # -- the parts of SubagentDriver the runtime uses ----------------------
    @property
    def authority_lost(self) -> bool:
        return False

    @property
    def alive(self) -> bool:
        return self.running

    @property
    def end_reason(self) -> Optional[str]:
        """Why the session is over for this job; ``None`` for a detach."""

        if self.close_reason is not None:
            return self.close_reason
        if self._stop_reason == "stopped":
            return "stopped"
        return None

    async def run(self, brief: str, *, role: str = "human") -> SubagentResult:
        """Follow the session until it ends. A later call returns the result."""

        del brief, role
        if self._finished:
            return self.classify()
        self.running = True
        self._started_at = self._clock()
        try:
            await self._run_session()
        finally:
            self.running = False
            self._finished = True
            self._connect_settled.set()
            self._loop_done.set()
        return self.classify()

    def steer(self, text: str) -> None:
        """Steering from ``message_agent`` without ``reply_to``."""

        self._spawn(self.instruct(text))

    async def stop(self, *, timeout: float = 10.0) -> None:
        """Detach without ending the session."""

        self._request_stop("detached")
        await self._wait_loop(timeout)

    async def graceful_stop(
        self, reason: str = PARENT_STOP_REASON, *, timeout: float = 10.0
    ) -> SubagentResult:
        """``stop_agent``: say goodbye and hang up. Any other reason only detaches."""

        if reason == PARENT_STOP_REASON and self.running and not self.connected:
            try:
                await asyncio.wait_for(self._connect_settled.wait(), _CONNECT_WAIT_S)
            except asyncio.TimeoutError:
                pass
        if reason == PARENT_STOP_REASON and self.running and self._live:
            await self._append(
                AppendKind.INSTRUCTIONS,
                self.settings.goodbye,
                None,
                journal_kind="instruction",
            )
            await asyncio.sleep(min(_GOODBYE_GRACE_S, max(0.0, timeout / 2)))
            await self.hangup(record=True)
            if not await self._wait_loop(max(1.0, timeout / 2)):
                self._request_stop("stopped")
                await self._wait_loop(2.0)
        else:
            self._request_stop(
                "stopped" if reason == PARENT_STOP_REASON else "detached"
            )
            await self._wait_loop(timeout)
        return self.classify()

    async def close(self) -> None:
        self._request_stop(self._stop_reason or "detached")
        await self._wait_loop(5.0)
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._client.close()

    # -- session controls the runtime exposes ------------------------------
    async def answer(self, delegation_id: str, text: str) -> Dict[str, Any]:
        """Speak the main agent's answer to one question (``message_agent(reply_to=…)``)."""

        question = self.questions.get(delegation_id)
        if question is None:
            return {"result": "unknown_question", "reply_to": delegation_id}
        if self.close_reason is not None or self._finished:
            return {
                "result": "session_ended",
                "reply_to": delegation_id,
                "error": "the session has ended; handle it as a follow-up",
            }
        late = question["status"] in ("follow_up",)
        sent = await self._append(
            AppendKind.COMMENTARY, text, delegation_id, journal_kind="answer"
        )
        if not sent:
            return {
                "result": "error",
                "reply_to": delegation_id,
                "error": "the answer could not be sent",
            }
        question["status"] = "answered_late" if late else "answered"
        question["answer"] = text
        question["answered_at"] = _utcnow().isoformat()
        await self._changed()
        return {
            "result": "answered_late" if late else "answered",
            "reply_to": delegation_id,
        }

    async def instruct(self, text: str) -> bool:
        return await self._append(
            AppendKind.INSTRUCTIONS, text, None, journal_kind="instruction"
        )

    async def transfer(self, target_uri: Optional[str] = None) -> Dict[str, Any]:
        """Hand the caller to a person (the line's transfer target by default)."""

        try:
            await self._client.transfer(target_uri)
        except PodError as exc:  # reported to the parent, never retried
            if exc.status == 400:
                return {
                    "result": "refused",
                    "error": "this line has no transfer target",
                }
            return {"result": "error", "error": exc.describe()}
        await self._write(
            [{"kind": "transfer", "speaker": "job", "text": target_uri or ""}]
        )
        outcome: Dict[str, Any] = {"result": "transferring"}
        if target_uri:
            outcome["target"] = target_uri
        return outcome

    async def hangup(self, *, record: bool = True) -> bool:
        try:
            await self._client.hangup()
        except PodError as exc:
            logger.warning("live %s: hangup failed: %s", self.handle, exc.describe())
            return False
        if record:
            await self._write([{"kind": "hangup", "speaker": "job", "text": ""}])
        return True

    def open_questions(self) -> List[Dict[str, Any]]:
        return [q for q in self.questions.values() if q["status"] == "open"]

    # -- internals ---------------------------------------------------------
    @property
    def _live(self) -> bool:
        return self.connected and self.close_reason is None and not self._finished

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _request_stop(self, reason: str) -> None:
        if self._stop_reason is None:
            self._stop_reason = reason
        self._stop.set()

    async def _wait_loop(self, timeout: float) -> bool:
        if not self.running:
            return True
        try:
            await asyncio.wait_for(self._loop_done.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def _changed(self) -> None:
        if self._on_change is not None:
            try:
                await self._on_change()
            except Exception:  # noqa: BLE001
                logger.warning(
                    "live %s: change notification failed", self.handle, exc_info=True
                )

    async def _write(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            event.setdefault("event_id", f"job_{uuid.uuid4().hex}")
            self._seen_event_ids.add(str(event["event_id"]))
        await self._client.write_journal(events)

    async def _append(
        self,
        append_kind: AppendKind,
        text: str,
        delegation_id: Optional[str],
        *,
        journal_kind: str,
    ) -> bool:
        """Send one append to the session and journal it as a ``journal_kind`` row."""

        if not self.connected and self.running:
            # A steer or an answer can race the first connection.
            try:
                await asyncio.wait_for(self._connect_settled.wait(), _CONNECT_WAIT_S)
            except asyncio.TimeoutError:
                return False
        if not self._live:
            return False
        # The pod clips too; clip here so the journal holds what was sent.
        content = clip_append_content(text)
        if not content:
            return False
        event_id = f"job_{uuid.uuid4().hex}"
        outage = _Outage(self._clock)
        for attempt in range(_APPEND_ATTEMPTS):
            try:
                body = await self._client.append(
                    append_kind, content, delegation_id, event_id
                )
            except PodError as exc:
                # 404/409 can mean the handoff is still on its way.
                if (
                    exc.status in (404, 409)
                    and attempt + 1 < _APPEND_ATTEMPTS
                    and self._live
                ):
                    await asyncio.sleep(outage.next_delay())
                    continue
                logger.warning(
                    "live %s: %s append failed: %s",
                    self.handle,
                    append_kind.value,
                    exc.describe(),
                )
                return False
            if body.get("ok"):
                break
            code, message = _error_of(body)
            logger.warning(
                "live %s: %s append refused: %s %s",
                self.handle,
                append_kind.value,
                code,
                message,
            )
            return False
        else:  # pragma: no cover - every attempt returns or breaks
            return False
        self.provider_calls += 1
        self._spawn(
            self._write(
                [
                    {
                        "event_id": event_id,
                        "kind": journal_kind,
                        "speaker": "job",
                        "text": content,
                        "delegation_id": delegation_id,
                    }
                ]
            )
        )
        return True

    async def _catch_up(self) -> Optional[Dict[str, Any]]:
        """Take in the journal rows after the cursor; return the session row.

        ``None`` when the journal could not be read.
        """

        page = await self._client.journal(self._journal_cursor)
        if not page.get("ok"):
            return None
        self._journal_cursor = int(page.get("next_after_id") or self._journal_cursor)
        await self._ingest(page.get("events") or [])
        session = page.get("session")
        return session if isinstance(session, dict) else {}

    async def _ingest(self, rows: List[Dict[str, Any]]) -> None:
        """Journal rows into the transcript and the questions."""

        answered: Set[str] = set()
        noted: Set[str] = set()
        delegations: List[Dict[str, Any]] = []
        for row in rows:
            event_id = row.get("event_id")
            if event_id and str(event_id) in self._seen_event_ids:
                continue
            if event_id:
                self._seen_event_ids.add(str(event_id))
            kind = row.get("kind")
            if kind == "transcript":
                self.transcript.add(
                    str(row.get("speaker") or "caller"),
                    str(row.get("text") or ""),
                    row.get("start_ms"),
                    event_id,
                )
            elif kind == "delegation" and row.get("delegation_id"):
                delegations.append(
                    {
                        "delegation_id": str(row["delegation_id"]),
                        "turn": len(self.transcript.turns),
                    }
                )
            elif kind in ("answer", "follow_up") and row.get("delegation_id"):
                answered.add(str(row["delegation_id"]))
            elif kind == "fallback" and row.get("delegation_id"):
                noted.add(str(row["delegation_id"]))
        for delegation_id in noted:
            # The pod's fallback answered a question while the stream was down.
            question = self.questions.get(delegation_id)
            if question is not None and question["status"] == "open":
                question["status"] = "follow_up"
                question["surfaced"] = True
        for item in delegations:
            if item["delegation_id"] in answered:
                continue
            if item["delegation_id"] in noted:
                # The pod told the caller it is noted before the job got
                # there: nothing to answer live, but it is a follow-up.
                self._note_follow_up(item["delegation_id"], upto_turn=item["turn"])
                continue
            await self._raise_question(item["delegation_id"], upto_turn=item["turn"])

    def _request_upto(self, upto_turn: Optional[int]) -> str:
        """What the caller said since the previous delegation (advances the mark)."""

        end = len(self.transcript.turns) if upto_turn is None else upto_turn
        turns = self.transcript.turns[self._last_delegation_turn : end]
        # The caller's last stretch of speech before the delegation: skip the
        # voice's holding phrase, then take caller turns back to the voice's
        # previous reply (earlier questions the voice already answered are
        # not part of this request; the transcript tail still shows them).
        index = len(turns) - 1
        while index >= 0 and turns[index].speaker != "caller":
            index -= 1
        block: List[str] = []
        while index >= 0 and turns[index].speaker == "caller":
            block.insert(0, turns[index].text.strip())
            index -= 1
        request = " ".join(t for t in block if t) or self.transcript.last_caller_text()
        self._last_delegation_turn = end
        return request

    def _note_follow_up(self, delegation_id: str, *, upto_turn: Optional[int]) -> None:
        if delegation_id in self.questions:
            return
        now = _utcnow().isoformat()
        self.questions[delegation_id] = {
            "delegation_id": delegation_id,
            "request": self._request_upto(upto_turn),
            "transcript_tail": self.transcript.render(last=_TRANSCRIPT_TAIL_TURNS),
            "asked_at": now,
            "answer_by": now,
            "status": "follow_up",
            "surfaced": True,
        }

    async def _raise_question(
        self, delegation_id: str, *, upto_turn: Optional[int] = None
    ) -> None:
        if delegation_id in self.questions:
            return
        request = self._request_upto(upto_turn)
        now = _utcnow()
        question: Dict[str, Any] = {
            "delegation_id": delegation_id,
            "request": request,
            "transcript_tail": self.transcript.render(last=_TRANSCRIPT_TAIL_TURNS),
            "asked_at": now.isoformat(),
            "answer_by": (
                now + timedelta(seconds=self.settings.answer_timeout_s)
            ).isoformat(),
            "status": "open",
            "surfaced": False,
        }
        self.questions[delegation_id] = question
        self._spawn(
            self._write(
                [
                    {
                        "kind": "question",
                        "speaker": "job",
                        "text": request,
                        "delegation_id": delegation_id,
                        "payload": {"request": request},
                    }
                ]
            )
        )
        self._spawn(self._expire_question(delegation_id))
        if self._on_question is not None:
            await self._on_question(question)

    async def _expire_question(self, delegation_id: str) -> None:
        await asyncio.sleep(self.settings.answer_timeout_s)
        question = self.questions.get(delegation_id)
        if question is None or question["status"] != "open":
            return
        if await self._append(
            AppendKind.COMMENTARY,
            self.settings.follow_up,
            delegation_id,
            journal_kind="follow_up",
        ):
            question["status"] = "follow_up"
        else:
            question["status"] = "unanswered"
        await self._changed()

    def _settle_from_journal(
        self, session: Mapping[str, Any], fallback: str = "ended"
    ) -> None:
        self.close_reason = str(
            session.get("close_reason") or session.get("status") or fallback
        )
        usage = session.get("usage_seconds")
        if isinstance(usage, (int, float)) and not isinstance(usage, bool):
            self.usage_seconds = float(usage)

    async def _settle_unreachable(self, exc: PodError, outage: _Outage) -> bool:
        """After a failed pod call: ``True`` to try again, ``False`` when the
        session is over for this job (``close_reason`` is set then)."""

        self.attach_error = exc.describe()
        session = await self._catch_up()
        status = str((session or {}).get("status") or "")
        if status in _TERMINAL_STATUSES:
            # Ended, failed or rejected before the job got there: the
            # missed-session path settles at once from the journal.
            self._settle_from_journal(session or {})
            return False
        if exc.status in (401, 403):
            self.close_reason = "not_authorized"
            return False
        window = (
            _NOT_YET_WINDOW_S if exc.status in (404, 409) else _UNREACHABLE_WINDOW_S
        )
        if outage.elapsed() >= window:
            self.close_reason = (
                "session_gone" if exc.status in (404, 409) else "connection_lost"
            )
            return False
        await self._sleep_or_stop(outage.next_delay())
        return not self._stop.is_set()

    async def _attach(self) -> bool:
        """Tell the pod the job is here (ends its no-job fallback)."""

        outage = _Outage(self._clock)
        for attempt in range(_ATTACH_ATTEMPTS):
            try:
                await self._client.attach()
                return True
            except PodError as exc:
                retry = exc.status in (0, 404, 409) or exc.status >= 500
                if not retry or attempt + 1 == _ATTACH_ATTEMPTS:
                    logger.warning(
                        "live %s: attach refused: %s", self.handle, exc.describe()
                    )
                    return False
                await self._sleep_or_stop(outage.next_delay())
                if self._stop.is_set():
                    return False
        return False

    async def _handle_event(self, event: Any) -> bool:
        """One stream event; ``True`` when it closed the session."""

        event_id = getattr(event, "event_id", None)
        if event_id and event_id in self._seen_event_ids:
            return False
        if event_id:
            self._seen_event_ids.add(event_id)
        if isinstance(event, TranscriptDelta):
            speaker = "caller" if event.speaker is Speaker.CALLER else "assistant"
            self.transcript.add(speaker, event.text, event.start_ms, event.event_id)
        elif isinstance(event, DelegationCreated):
            await self._raise_question(event.delegation_id)
        elif isinstance(event, UsageUpdated):
            self.usage_seconds = event.seconds
        elif isinstance(event, EngineError):
            logger.info(
                "live %s: engine error %s: %s", self.handle, event.code, event.message
            )
        elif isinstance(event, SessionClosed):
            self.close_reason = event.reason.value
            if event.usage_seconds is not None:
                self.usage_seconds = event.usage_seconds
            return True
        return False

    async def _read(self, resp: aiohttp.ClientResponse) -> bool:
        """Consume the stream; ``True`` once the session closed."""

        try:
            async for _event_id, _type, data in PodClient.read_events(resp):
                try:
                    wire = json.loads(data)
                except ValueError:
                    continue
                event = event_from_wire(wire) if isinstance(wire, dict) else None
                if event is None:
                    continue
                if await self._handle_event(event):
                    return True
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            logger.info(
                "live %s: event stream dropped: %s", self.handle, type(exc).__name__
            )
        return False

    async def _consume(self, resp: aiohttp.ClientResponse) -> bool:
        """Read the stream until the session closes, it drops, or a stop."""

        reader = asyncio.ensure_future(self._read(resp))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({reader, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (reader, stopper):
                if not task.done():
                    task.cancel()
            await asyncio.gather(reader, stopper, return_exceptions=True)
        if reader.cancelled():
            return False
        return bool(reader.result())

    async def _run_session(self) -> None:
        attached = False
        outage = _Outage(self._clock)
        while not self._stop.is_set():
            try:
                resp = await self._client.open_events()
            except PodError as exc:
                if await self._settle_unreachable(exc, outage):
                    continue
                break
            self.connected = True
            self.attach_error = None
            self._connect_settled.set()
            opened_at = self._clock()
            seen_before = len(self._seen_event_ids)
            try:
                if not attached:
                    attached = await self._attach()
                # Stream first, then the journal: whatever the pod journaled
                # before the stream opened comes from here, and the stream's
                # replay overlaps it by event_id.
                await self._catch_up()
                closed = await self._consume(resp)
            finally:
                resp.close()
            if closed or self._stop.is_set():
                break
            # A stream that held, or brought news, ends an outage; one that
            # drops at once keeps counting against the window.
            if (
                self._clock() - opened_at >= _STREAM_HELD_S
                or len(self._seen_event_ids) > seen_before
            ):
                outage.reset()
            # The stream ended without a closed event: the pod may know the
            # session ended (its replay ends at once then), or it dropped.
            try:
                pod_session = await self._client.status()
            except PodError as exc:
                if await self._settle_unreachable(exc, outage):
                    continue
                break
            if str(pod_session.get("status") or "") in _TERMINAL_STATUSES:
                await self._catch_up()
                self._settle_from_journal(pod_session)
                break
            if outage.elapsed() >= _UNREACHABLE_WINDOW_S:
                await self._catch_up()
                self.close_reason = "connection_lost"
                break
            await self._sleep_or_stop(outage.next_delay())
        for question in self.open_questions():
            question["status"] = "unanswered"
        self._connect_settled.set()
        await self._changed()

    def _report(self) -> str:
        lines: List[str] = []
        duration = None
        if self._started_at is not None:
            duration = self._clock() - self._started_at
        lines.append(f"Live session {self.settings.session_id}.")
        state = self.close_reason or ("detached" if self._stop_reason else "unknown")
        lines.append(
            f"Ended: {state}."
            if self.close_reason
            else f"Status: {state} (the session may still be running; its "
            "connector keeps covering it)."
        )
        if self.usage_seconds is not None:
            lines.append(f"Voice duration: {self.usage_seconds:.0f} s.")
        elif duration is not None:
            lines.append(f"Attached for {duration:.0f} s.")
        if self.attach_error and not self.connected:
            lines.append(
                f"The job did not reach the live session ({self.attach_error}); "
                "the transcript comes from the session's journal."
            )
        lines.append("")
        lines.append("## Transcript")
        lines.append(self.transcript.render() or "(nothing was said)")
        if self.questions:
            lines.append("")
            lines.append("## Questions the voice handed to the back office")
            for index, question in enumerate(self.questions.values(), 1):
                entry = f"{index}. [{question['status']}] {question['request'] or '(no request text)'}"
                if question.get("answer"):
                    entry += f" → answered: {question['answer']}"
                lines.append(entry)
        follow_ups = [
            q
            for q in self.questions.values()
            if q["status"] in ("follow_up", "unanswered", "open")
        ]
        if follow_ups:
            lines.append("")
            lines.append(
                "## Follow-ups to handle now (the caller was told we will get back to them)"
            )
            for question in follow_ups:
                lines.append(f"- {question['request'] or '(see transcript)'}")
        return "\n".join(lines)

    def classify(self) -> SubagentResult:
        if self.close_reason is not None:
            status = "completed"
        elif self._stop_reason == "stopped":
            status = "interrupted:stopped"
        elif self._stop_reason == "detached":
            status = "interrupted:drain"
        else:
            status = "error"
        duration = (
            0.0
            if self._started_at is None
            else max(0.0, self._clock() - self._started_at)
        )
        return SubagentResult(
            status=status,
            text=self._report(),
            turns=len(self.transcript.turns),
            tokens=0,
            duration=duration,
            handle=self.handle,
            subagent_type=self.subagent_type,
            subagent_id=self.subagent_id,
            error=None
            if status != "error"
            else (self.attach_error or "the session ended without a close event"),
        )


def _end_unattached(context: Any, reason: str) -> None:
    """A live job whose voice subagent could not start still turns strategic."""

    end = getattr(context, "end_live_session", None)
    if callable(end):
        end(reason)


async def attach_live_session(context: Any) -> Optional[List[Any]]:
    """Start the job's live session as its voice subagent; return the history to seed.

    A live-session job (``config.live_session`` set at dispatch) is born with
    its session already running. The agent sees it as something it started
    itself: a ``delegate_agent`` call to the roster's live entry plus the
    receipt, appended after the task message (model-natural). Returns ``None``
    for every other job. When the voice subagent cannot start (no subagent
    runtime, no live roster entry, a refused admission) the pod's no-job
    fallback keeps covering the session, and the job is told its session is
    over for it so it turns strategic instead of waiting for questions.

    On a resume the runtime attaches a new run the same way (the predecessor
    run was superseded by orphan recovery); the history is not reseeded then.
    """

    config = getattr(context, "config", None) or {}
    block = live_session_block(config)
    if block is None:
        return None
    runtime = getattr(context, "subagent_runtime", None)
    if runtime is None:
        logger.error(
            "live-session job without a subagent runtime: the connector covers "
            "the session alone"
        )
        _end_unattached(context, "the voice subagent could not start")
        return None
    name = live_entry_name(runtime.roster)
    if name is None:
        logger.error(
            "live-session job whose expert has no live voice entry: the "
            "connector covers the session alone"
        )
        _end_unattached(context, "the voice subagent could not start")
        return None

    from langchain_core.messages import AIMessage, ToolMessage

    from agent.subagents.runtime import SubagentCall

    # Unique per attach: a resumed job attaches again, and the ledger replays
    # an earlier call id's stored report instead of starting a run.
    tool_call_id = f"call_live_{uuid.uuid4().hex[:24]}"
    args = {
        "description": "Live voice session",
        "prompt": (
            "Attach to the live session and hand me every question the voice "
            "delegates; report the transcript and follow-ups when it ends."
        ),
        "subagent_type": name,
        "run_in_background": True,
    }
    receipt = await runtime.run_background(
        SubagentCall(
            tool_call_id=tool_call_id,
            subagent_type=name,
            prompt=args["prompt"],
            description=args["description"],
            run_in_background=True,
            live_attach=True,
        )
    )
    if str(receipt).startswith("Error:"):
        logger.error("live-session job: the voice subagent was refused: %s", receipt)
        _end_unattached(context, "the voice subagent could not start")
    return [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "delegate_agent",
                    "args": args,
                    "id": tool_call_id,
                    "type": "tool_call",
                }
            ],
        ),
        ToolMessage(
            content=str(receipt), tool_call_id=tool_call_id, name="delegate_agent"
        ),
    ]
