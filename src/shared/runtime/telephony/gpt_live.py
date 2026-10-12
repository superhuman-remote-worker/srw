"""gpt_live — OpenAI Live API call control and the sideband WebSocket.

Two halves:

* :class:`GptLiveClient` — the REST call controls for a SIP session: accept,
  reject, refer (transfer), hang up, and create (outbound). Outbound create is
  **never retried**: every request dials again, so an ambiguous failure is
  reported, not repeated.
* :class:`Sideband` — a WebSocket attached to a running session
  (``/live/sessions/{id}/attach``). A reader task maps GPT-Live events onto
  :mod:`.contract` events, deduplicates on ``event_id`` (the attach replays the
  last 3 s), and resolves each append's future from its ``*.appended``
  acknowledgement or ``error``.

Several sidebands may observe one session; each action must have one owner
(design §4.1.1). Base URLs are injected so tests and k3d can point at the fake
server. Wire protocol: design §3.1.1.

Depends on ``aiohttp`` only (shipped in both the agent and orchestrator
images).
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Deque, Dict, Mapping, Optional, Set

import aiohttp

from .contract import (
    AppendAcknowledged,
    AppendKind,
    CallEvent,
    CloseReason,
    DelegationCreated,
    DtmfReceived,
    EngineError,
    OtherEvent,
    SessionClosed,
    Speaker,
    TranscriptDelta,
    TransportProgress,
    UsageUpdated,
    clip_append_content,
)

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://api.openai.com/v1"
DEFAULT_WS_BASE = "wss://api.openai.com/v1"
DEFAULT_MODEL = "gpt-live-1"
DEFAULT_VOICE = "marin"

# Events that copy audio onto the sideband. Large, timestamp-only, and never
# needed by a job: dropped before they reach the queue.
_REFLECTED_AUDIO_EVENTS = frozenset(
    {"session.input_audio.append", "session.output_audio.delta"}
)
_DEDUPE_WINDOW = 4096


class GptLiveError(Exception):
    """A Live API request failed with an HTTP error."""

    def __init__(
        self,
        status: int,
        message: str,
        *,
        code: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        super().__init__(f"GPT-Live HTTP {status}: {code or ''} {message}".strip())
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id


class GptLiveAmbiguousError(Exception):
    """An outbound create timed out or lost its connection.

    The call may or may not have been placed. Never retry automatically.
    """


class SidebandClosedError(Exception):
    """The sideband closed before an append was acknowledged."""


class AppendRejectedError(Exception):
    """GPT-Live refused an append (``error`` with our ``client_event_id``)."""

    def __init__(self, error: EngineError) -> None:
        super().__init__(f"{error.code or 'error'}: {error.message}")
        self.error = error


@dataclass(frozen=True)
class LiveEndpoints:
    """Where the Live API lives. Overridable so k3d can use the fake server."""

    api_base: str = DEFAULT_API_BASE
    ws_base: str = DEFAULT_WS_BASE
    extra_headers: Mapping[str, str] = field(default_factory=dict)

    def session_url(self, session_id: str, action: str = "") -> str:
        base = f"{self.api_base.rstrip('/')}/live/sessions/{session_id}"
        return f"{base}/{action}" if action else base

    def attach_url(self, session_id: str) -> str:
        return f"{self.ws_base.rstrip('/')}/live/sessions/{session_id}/attach"


def build_session_config(
    *,
    instructions: str,
    model: str = DEFAULT_MODEL,
    voice: str = DEFAULT_VOICE,
    delegation: str = "client",
    input_messages: Optional[list] = None,
    store: Optional[bool] = None,
) -> Dict[str, Any]:
    """The ``session`` object for accept or outbound create.

    SIP negotiates the audio format, so ``audio.format`` is never set.
    """

    config: Dict[str, Any] = {
        "model": model,
        "instructions": instructions,
        "audio": {"output": {"voice": voice}},
        "delegation": {"type": delegation},
    }
    if input_messages:
        config["input"] = list(input_messages)
    if store is not None:
        config["store"] = bool(store)
    return config


def _opt_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _opt_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def parse_event(raw: Mapping[str, Any]) -> Optional[CallEvent]:
    """Map one GPT-Live server event onto the contract.

    Returns ``None`` for reflected audio, which the contract never carries.
    """

    event_type = str(raw.get("type") or "")
    event_id = raw.get("event_id") if isinstance(raw.get("event_id"), str) else None
    if event_type in _REFLECTED_AUDIO_EVENTS:
        return None
    if event_type in (
        "session.input_transcript.delta",
        "session.output_transcript.delta",
    ):
        speaker = (
            Speaker.CALLER
            if event_type.startswith("session.input")
            else Speaker.ASSISTANT
        )
        return TranscriptDelta(
            speaker=speaker,
            text=str(raw.get("delta") or ""),
            start_ms=_opt_int(raw.get("start_ms")),
            end_ms=_opt_int(raw.get("end_ms")),
            event_id=event_id,
        )
    if event_type == "session.delegation.created":
        delegation = (
            raw.get("delegation") if isinstance(raw.get("delegation"), Mapping) else {}
        )
        delegation_id = delegation.get("id")
        if not isinstance(delegation_id, str) or not delegation_id:
            return OtherEvent(type=event_type, payload=dict(raw), event_id=event_id)
        return DelegationCreated(
            delegation_id=delegation_id,
            offset_ms=_opt_int(raw.get("offset_ms")),
            target=str(delegation.get("target") or "client"),
            event_id=event_id,
        )
    for kind in AppendKind:
        if event_type == f"session.{kind.value}.appended":
            client_event_id = raw.get("client_event_id")
            return AppendAcknowledged(
                kind=kind,
                client_event_id=client_event_id
                if isinstance(client_event_id, str)
                else None,
                start_ms=_opt_int(raw.get("start_ms")),
                end_ms=_opt_int(raw.get("end_ms")),
                event_id=event_id,
            )
    if event_type == "error":
        error = raw.get("error") if isinstance(raw.get("error"), Mapping) else {}
        client_event_id = error.get("client_event_id")
        return EngineError(
            code=error.get("code") if isinstance(error.get("code"), str) else None,
            message=str(error.get("message") or ""),
            error_type=error.get("type")
            if isinstance(error.get("type"), str)
            else None,
            param=error.get("param") if isinstance(error.get("param"), str) else None,
            client_event_id=client_event_id
            if isinstance(client_event_id, str)
            else None,
            event_id=event_id,
        )
    if event_type == "transport.dtmf.received":
        return DtmfReceived(digit=str(raw.get("event") or ""), event_id=event_id)
    if event_type in ("transport.ringing", "transport.answered", "transport.failed"):
        error = raw.get("error") if isinstance(raw.get("error"), Mapping) else {}
        return TransportProgress(
            state=event_type.split(".", 1)[1],
            error_code=error.get("code")
            if isinstance(error.get("code"), str)
            else None,
            error_message=error.get("message")
            if isinstance(error.get("message"), str)
            else None,
            event_id=event_id,
        )
    if event_type == "session.usage.updated":
        usage = raw.get("usage") if isinstance(raw.get("usage"), Mapping) else {}
        window = (
            raw.get("context_window")
            if isinstance(raw.get("context_window"), Mapping)
            else {}
        )
        return UsageUpdated(
            seconds=_opt_float(usage.get("seconds")) or 0.0,
            context_usage_ratio=_opt_float(window.get("usage_ratio")),
            event_id=event_id,
        )
    if event_type == "session.closed":
        raw_reason = raw.get("reason") if isinstance(raw.get("reason"), str) else None
        try:
            reason = CloseReason(raw_reason) if raw_reason else CloseReason.UNKNOWN
        except ValueError:
            reason = CloseReason.UNKNOWN
        usage = raw.get("usage") if isinstance(raw.get("usage"), Mapping) else {}
        return SessionClosed(
            reason=reason,
            usage_seconds=_opt_float(usage.get("seconds")),
            event_id=event_id,
            raw_reason=raw_reason,
        )
    return OtherEvent(type=event_type, payload=dict(raw), event_id=event_id)


_CLOSED = object()


class Sideband:
    """One attached sideband WebSocket. Iterate it for :mod:`.contract` events.

    Use :meth:`GptLiveClient.attach` to open one. The reader task starts at
    once, so acknowledgements resolve even while nobody iterates; events queue
    until read. Iteration ends after ``session.closed`` or when the socket
    drops (check :attr:`closed_event` to tell the two apart).
    """

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, session_id: str) -> None:
        self.session_id = session_id
        self.closed_event: Optional[SessionClosed] = None
        self._ws = ws
        self._queue: "asyncio.Queue[Any]" = asyncio.Queue()
        self._pending: Dict[str, "asyncio.Future[AppendAcknowledged]"] = {}
        self._seen: Set[str] = set()
        self._seen_order: Deque[str] = deque()
        self._closed = asyncio.Event()
        self._counter = itertools.count(1)
        self._prefix = uuid.uuid4().hex[:8]
        self._reader = asyncio.create_task(
            self._read(), name=f"gpt-live-sideband-{session_id}"
        )

    # -- reading -----------------------------------------------------------
    def _first_sight(self, event_id: Optional[str]) -> bool:
        if not event_id:
            return True
        if event_id in self._seen:
            return False
        self._seen.add(event_id)
        self._seen_order.append(event_id)
        if len(self._seen_order) > _DEDUPE_WINDOW:
            self._seen.discard(self._seen_order.popleft())
        return True

    def _resolve(self, event: CallEvent) -> None:
        if isinstance(event, AppendAcknowledged) and event.client_event_id:
            future = self._pending.pop(event.client_event_id, None)
            if future is not None and not future.done():
                future.set_result(event)
        elif isinstance(event, EngineError) and event.client_event_id:
            future = self._pending.pop(event.client_event_id, None)
            if future is not None and not future.done():
                future.set_exception(AppendRejectedError(event))

    async def _read(self) -> None:
        try:
            async for message in self._ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSE,
                    ):
                        break
                    continue
                try:
                    raw = json.loads(message.data)
                except json.JSONDecodeError:
                    logger.warning(
                        "sideband %s: non-JSON frame dropped", self.session_id
                    )
                    continue
                if not isinstance(raw, Mapping):
                    continue
                event = parse_event(raw)
                if event is None or not self._first_sight(event.event_id):
                    continue
                self._resolve(event)
                if isinstance(event, SessionClosed):
                    self.closed_event = event
                await self._queue.put(event)
                if isinstance(event, SessionClosed):
                    break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("sideband %s: connection lost: %s", self.session_id, exc)
        finally:
            self._closed.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(SidebandClosedError(self.session_id))
            self._pending.clear()
            await self._queue.put(_CLOSED)

    def __aiter__(self) -> AsyncIterator[CallEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[CallEvent]:
        while True:
            item = await self._queue.get()
            if item is _CLOSED:
                # Leave the sentinel for any other iterator.
                await self._queue.put(_CLOSED)
                return
            yield item

    async def next_event(self, timeout: Optional[float] = None) -> Optional[CallEvent]:
        """One event, or ``None`` when the sideband has ended."""

        item = await asyncio.wait_for(self._queue.get(), timeout)
        if item is _CLOSED:
            await self._queue.put(_CLOSED)
            return None
        return item

    @property
    def is_closed(self) -> bool:
        return self._closed.is_set()

    async def wait_closed(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._closed.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True

    # -- commands ----------------------------------------------------------
    def new_event_id(self, label: str = "cmd") -> str:
        return f"srw_{label}_{self._prefix}_{next(self._counter)}"

    async def send_raw(self, command: Mapping[str, Any]) -> None:
        if self.is_closed or self._ws.closed:
            raise SidebandClosedError(self.session_id)
        await self._ws.send_str(json.dumps(command))

    async def send_append(
        self,
        kind: AppendKind,
        content: str,
        *,
        delegation_id: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> "asyncio.Future[AppendAcknowledged]":
        """Send an append and return a future for its acknowledgement.

        The future fails with :class:`AppendRejectedError` or
        :class:`SidebandClosedError`. An acknowledgement confirms context
        delivery, not speech, and can stay pending while the timeline stalls,
        so callers wait on it with a timeout or not at all.
        """

        event_id = event_id or self.new_event_id(kind.value)
        future: "asyncio.Future[AppendAcknowledged]" = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[event_id] = future
        command = {
            "type": f"session.{kind.value}.append",
            "event_id": event_id,
            "delegation_id": delegation_id,
            "content": clip_append_content(content),
        }
        try:
            await self.send_raw(command)
        except Exception:
            self._pending.pop(event_id, None)
            raise
        return future

    async def append(
        self,
        kind: AppendKind,
        content: str,
        *,
        delegation_id: Optional[str] = None,
        event_id: Optional[str] = None,
        timeout: float = 10.0,
    ) -> AppendAcknowledged:
        future = await self.send_append(
            kind, content, delegation_id=delegation_id, event_id=event_id
        )
        return await asyncio.wait_for(future, timeout)

    async def close_session(self, timeout: float = 10.0) -> Optional[SessionClosed]:
        """End the whole session (``session.close``) and wait for ``session.closed``."""

        if not self.is_closed:
            try:
                await self.send_raw(
                    {"type": "session.close", "event_id": self.new_event_id("close")}
                )
            except SidebandClosedError:
                pass
        await self.wait_closed(timeout)
        await self.detach()
        return self.closed_event

    async def detach(self) -> None:
        """Close this WebSocket only. The call goes on."""

        if not self._ws.closed:
            await self._ws.close()
        if not self._reader.done():
            try:
                await asyncio.wait_for(asyncio.shield(self._reader), 2.0)
            except asyncio.TimeoutError:
                self._reader.cancel()


class GptLiveClient:
    """REST call control and sideband attach for one OpenAI project key."""

    def __init__(
        self,
        api_key: str,
        endpoints: Optional[LiveEndpoints] = None,
        *,
        http: Optional[aiohttp.ClientSession] = None,
        timeout_s: float = 10.0,
    ) -> None:
        if not api_key:
            raise ValueError("GptLiveClient needs an API key")
        self._api_key = api_key
        self.endpoints = endpoints or LiveEndpoints()
        self._http = http
        self._owns_http = http is None
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)

    def _headers(self) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self._api_key}"}
        headers.update(self.endpoints.extra_headers)
        return headers

    def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession()
            self._owns_http = True
        return self._http

    async def close(self) -> None:
        if self._owns_http and self._http is not None and not self._http.closed:
            await self._http.close()

    async def __aenter__(self) -> "GptLiveClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def _post(
        self, url: str, body: Optional[Mapping[str, Any]]
    ) -> Optional[Mapping[str, Any]]:
        kwargs: Dict[str, Any] = {"headers": self._headers(), "timeout": self._timeout}
        if body is not None:
            kwargs["json"] = body
        async with self._session().post(url, **kwargs) as response:
            text = await response.text()
            request_id = response.headers.get("x-request-id")
            if response.status >= 400:
                code: Optional[str] = None
                message = text[:500]
                try:
                    parsed = json.loads(text) if text else {}
                    error = parsed.get("error") if isinstance(parsed, dict) else None
                    if isinstance(error, dict):
                        code = (
                            error.get("code")
                            if isinstance(error.get("code"), str)
                            else None
                        )
                        message = str(error.get("message") or message)
                except json.JSONDecodeError:
                    pass
                raise GptLiveError(
                    response.status, message, code=code, request_id=request_id
                )
            if not text.strip():
                return None
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None

    async def accept(self, session_id: str, session_config: Mapping[str, Any]) -> None:
        """Accept a pending inbound call. ``200`` with an empty body on success."""

        config = dict(session_config)
        config.setdefault("type", "live")
        await self._post(
            self.endpoints.session_url(session_id, "accept"), {"session": config}
        )

    async def reject(self, session_id: str, status_code: int = 486) -> None:
        if not 300 <= int(status_code) <= 699:
            raise ValueError("SIP reject status must be 300..699")
        await self._post(
            self.endpoints.session_url(session_id, "reject"),
            {"status_code": int(status_code)},
        )

    async def refer(self, session_id: str, target_uri: str) -> None:
        """Transfer the call. Success means the request was taken, not answered."""

        await self._post(
            self.endpoints.session_url(session_id, "refer"), {"target_uri": target_uri}
        )

    async def hangup(self, session_id: str) -> None:
        await self._post(self.endpoints.session_url(session_id, "hangup"), None)

    async def create_outbound(
        self,
        session_config: Mapping[str, Any],
        *,
        destination: str,
        provider_url: str,
        username: str,
        password: str,
        caller_number: str,
    ) -> str:
        """Place an outbound SIP call and return the new session id.

        Never retried: a timeout raises :class:`GptLiveAmbiguousError` because
        the call may already be ringing.
        """

        body = {
            "session": dict(session_config),
            "transport": {
                "type": "sip",
                "destination": destination,
                "trunk": {
                    "provider_url": provider_url,
                    "auth": {
                        "type": "digest",
                        "username": username,
                        "password": password,
                    },
                    "caller_number": caller_number,
                },
            },
        }
        url = f"{self.endpoints.api_base.rstrip('/')}/live/sessions"
        try:
            parsed = await self._post(url, body)
        except (
            asyncio.TimeoutError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientOSError,
        ) as exc:
            raise GptLiveAmbiguousError(str(exc) or type(exc).__name__) from exc
        session = (parsed or {}).get("session") if parsed else None
        session_id = session.get("id") if isinstance(session, Mapping) else None
        if not isinstance(session_id, str) or not session_id:
            raise GptLiveAmbiguousError("create returned no session id")
        return session_id

    async def attach(self, session_id: str, *, heartbeat_s: float = 20.0) -> Sideband:
        """Open a sideband on a running session. Never send ``session.start``."""

        ws = await self._session().ws_connect(
            self.endpoints.attach_url(session_id),
            headers=self._headers(),
            heartbeat=heartbeat_s,
            max_msg_size=8 * 1024 * 1024,
        )
        return Sideband(ws, session_id)
