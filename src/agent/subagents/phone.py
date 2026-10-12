"""phone — the phone subagent: a background child whose driver is a live call.

A phone call is a read-only background subagent of a job (design
``knowledge-base/knowledge/features/phone_calls_as_subagents.md`` §4.2, §4.4.1).
The voice engine (GPT-Live) talks to the caller; this driver attaches the
job's **actor** sideband to the running session and:

* keeps the transcript (turns grouped from deltas, backfilled from the call
  record the orchestrator's recorder keeps, because the sideband replays only
  the last 3 s);
* turns every ``delegation.created`` into a **question** for the main agent —
  the request is what the caller said since the previous delegation — and
  hands it to the runtime, which wakes ``wait_agent``;
* speaks the main agent's answer (``message_agent(reply_to=…)`` →
  ``commentary.append`` with the delegation id) and its steering
  (``message_agent`` without ``reply_to`` → ``instructions.append``);
* after ``answer_timeout_s`` without an answer, tells the caller the request
  is noted and marks the question a follow-up for after the call;
* ends with ``session.closed`` and returns the transcript, the questions and
  the follow-ups as its report.

It never ends a call because its job is going away: only an explicit
``stop_agent`` (reason :data:`PARENT_STOP_REASON`) says goodbye and hangs up.
Any other stop detaches, and the orchestrator's recorder keeps covering the
call (fallback answers, max duration). The driver duck-types the parts of
``SubagentDriver`` the runtime uses.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Set

import aiohttp

from agent.subagents.driver import SubagentResult
from shared.runtime.telephony.contract import (
    AppendKind,
    DelegationCreated,
    EngineError,
    SessionClosed,
    Speaker,
    TranscriptDelta,
    UsageUpdated,
)
from shared.runtime.telephony.gpt_live import (
    GptLiveClient,
    LiveEndpoints,
    Sideband,
    SidebandClosedError,
)

logger = logging.getLogger(__name__)

PHONE_DRIVER = "phone"
#: The reason ``SubagentRuntime.stop_agent`` passes; only this one hangs up.
PARENT_STOP_REASON = "parent requested stop"

DEFAULT_ANSWER_TIMEOUT_S = 25.0
DEFAULT_GOODBYE = (
    "Verabschieden Sie sich jetzt kurz und höflich vom Anrufer. Sagen Sie, dass "
    "das Gespräch jetzt beendet wird."
)
FOLLOW_UP_TEXT = (
    "Sagen Sie dem Anrufer: Ich habe das notiert. Wir kümmern uns nach dem "
    "Gespräch darum und melden uns bei Ihnen."
)
_TRANSCRIPT_TAIL_TURNS = 8
_GOODBYE_GRACE_S = 6.0
_ATTACH_ATTEMPTS = 3
_ATTACH_WAIT_S = 5.0
_SPEAKER_LABEL = {"caller": "Caller", "assistant": "Assistant"}


def is_phone_entry(entry: Optional[Mapping[str, Any]]) -> bool:
    """Whether a resolved roster entry runs on the phone driver."""

    if not isinstance(entry, Mapping):
        return False
    return str(entry.get("driver") or "").strip().lower() == PHONE_DRIVER


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class PhoneCallSettings:
    """What the job knows about its call: injected at dispatch as ``config.telephony``."""

    call_id: str
    session_id: str
    api_key: str
    api_base: str
    ws_base: str
    extra_headers: Mapping[str, str] = field(default_factory=dict)
    line_name: str = ""
    answer_timeout_s: float = DEFAULT_ANSWER_TIMEOUT_S
    transfer_target: Optional[str] = None
    goodbye: Optional[str] = None
    max_duration_s: Optional[float] = None

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any], entry: Optional[Mapping[str, Any]] = None
    ) -> "PhoneCallSettings":
        block = config.get("telephony") if isinstance(config, Mapping) else None
        if not isinstance(block, Mapping):
            raise ValueError("this job has no phone call (config.telephony is missing)")
        missing = [k for k in ("call_id", "session_id", "api_key") if not block.get(k)]
        if missing:
            raise ValueError(f"config.telephony is missing {', '.join(missing)}")
        line = block.get("line") if isinstance(block.get("line"), Mapping) else {}
        phone = (
            (entry or {}).get("phone")
            if isinstance((entry or {}).get("phone"), Mapping)
            else {}
        )
        timeout = (
            line.get("answer_timeout_s")
            or phone.get("answer_timeout_s")
            or DEFAULT_ANSWER_TIMEOUT_S
        )
        headers = (
            block.get("extra_headers")
            if isinstance(block.get("extra_headers"), Mapping)
            else {}
        )
        return cls(
            call_id=str(block["call_id"]),
            session_id=str(block["session_id"]),
            api_key=str(block["api_key"]),
            api_base=str(block.get("api_base") or "https://api.openai.com/v1"),
            ws_base=str(block.get("ws_base") or "wss://api.openai.com/v1"),
            extra_headers={str(k): str(v) for k, v in headers.items()},
            line_name=str(line.get("name") or ""),
            answer_timeout_s=float(timeout),
            transfer_target=line.get("transfer_target") or None,
            goodbye=line.get("goodbye") or None,
            max_duration_s=float(line["max_duration_s"])
            if line.get("max_duration_s")
            else None,
        )


class CallRecordClient:
    """The orchestrator's internal call-record API (``/api/internal/telephony``).

    Best effort: a failure is logged and the call goes on — the recorder keeps
    the authoritative record.
    """

    def __init__(
        self, base_url: str, internal_key: str = "", *, timeout_s: float = 5.0
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._headers = {"X-Internal-Key": internal_key} if internal_key else {}
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._http: Optional[aiohttp.ClientSession] = None

    @classmethod
    def from_env(cls) -> "CallRecordClient":
        return cls(
            os.getenv("ORCHESTRATOR_URL", "http://localhost:8085"),
            os.getenv("MCP_INTERNAL_KEY", ""),
        )

    def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                headers=self._headers, timeout=self._timeout
            )
        return self._http

    def _url(self, call_id: str, suffix: str) -> str:
        return f"{self.base_url}/api/internal/telephony/calls/{call_id}/{suffix}"

    async def attach(
        self, call_id: str, job_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        try:
            async with self._session().post(
                self._url(call_id, "attach"), json={"job_id": job_id}
            ) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "call %s: attach report refused (HTTP %s)", call_id, resp.status
                    )
                    return None
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("call %s: attach report failed: %s", call_id, exc)
            return None

    async def events(self, call_id: str, after_id: int = 0) -> Dict[str, Any]:
        events: List[Dict[str, Any]] = []
        call: Optional[Dict[str, Any]] = None
        cursor = after_id
        try:
            for _ in range(50):
                async with self._session().get(
                    self._url(call_id, "events"), params={"after_id": str(cursor)}
                ) as resp:
                    if resp.status >= 400:
                        logger.warning(
                            "call %s: backlog read refused (HTTP %s)",
                            call_id,
                            resp.status,
                        )
                        break
                    body = await resp.json()
                call = body.get("call") or call
                page = body.get("events") or []
                events.extend(page)
                next_after = int(body.get("next_after_id") or cursor)
                if not page or next_after <= cursor:
                    break
                cursor = next_after
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            logger.warning("call %s: backlog read failed: %s", call_id, exc)
        return {"call": call, "events": events}

    async def post_events(self, call_id: str, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        try:
            async with self._session().post(
                self._url(call_id, "events"), json={"events": events}
            ) as resp:
                if resp.status >= 400:
                    logger.warning(
                        "call %s: event write-back refused (HTTP %s)",
                        call_id,
                        resp.status,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("call %s: event write-back failed: %s", call_id, exc)

    async def close(self) -> None:
        if self._http is not None and not self._http.closed:
            await self._http.close()


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


QuestionCallback = Callable[[Dict[str, Any]], Awaitable[None]]


class PhoneDriver:
    """Drives one live call as a background child (duck-types ``SubagentDriver``)."""

    def __init__(
        self,
        settings: PhoneCallSettings,
        *,
        handle: str,
        subagent_type: str,
        subagent_id: str,
        job_id: Optional[str] = None,
        on_question: Optional[QuestionCallback] = None,
        on_change: Optional[Callable[[], Awaitable[None]]] = None,
        record: Optional[CallRecordClient] = None,
        client: Optional[GptLiveClient] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.handle = handle
        self.subagent_type = subagent_type
        self.subagent_id = subagent_id
        self.job_id = job_id
        self.running = False
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
        self._record = record or CallRecordClient.from_env()
        self._client = client or GptLiveClient(
            settings.api_key,
            LiveEndpoints(
                settings.api_base, settings.ws_base, dict(settings.extra_headers)
            ),
        )
        self._clock = clock
        self._sideband: Optional[Sideband] = None
        self._attach_settled = asyncio.Event()
        self._stop = asyncio.Event()
        self._stop_reason: Optional[str] = None
        self._loop_done = asyncio.Event()
        self._tasks: Set[asyncio.Task] = set()
        self._started_at: Optional[float] = None
        self._finished = False
        self._last_delegation_turn = 0
        self._seen_event_ids: Set[str] = set()

    # -- the parts of SubagentDriver the runtime uses ----------------------
    @property
    def authority_lost(self) -> bool:
        return False

    @property
    def alive(self) -> bool:
        return self.running

    async def run(self, brief: str, *, role: str = "human") -> SubagentResult:
        """Attach and handle the call until it ends. A later call returns the result."""

        del brief, role
        if self._finished:
            return self.classify()
        self.running = True
        self._started_at = self._clock()
        try:
            await self._run_call()
        finally:
            self.running = False
            self._finished = True
            self._attach_settled.set()
            self._loop_done.set()
        return self.classify()

    def steer(self, text: str) -> None:
        """Steering from ``message_agent`` without ``reply_to``."""

        self._spawn(self.instruct(text))

    async def stop(self, *, timeout: float = 10.0) -> None:
        """Detach without ending the call."""

        self._request_stop("detached")
        await self._wait_loop(timeout)

    async def graceful_stop(
        self, reason: str = PARENT_STOP_REASON, *, timeout: float = 10.0
    ) -> SubagentResult:
        """``stop_agent``: say goodbye and hang up. Any other reason only detaches."""

        if reason == PARENT_STOP_REASON and self.running and self._sideband is None:
            try:
                await asyncio.wait_for(self._attach_settled.wait(), _ATTACH_WAIT_S)
            except asyncio.TimeoutError:
                pass
        if (
            reason == PARENT_STOP_REASON
            and self.running
            and self._sideband is not None
            and not self._sideband.is_closed
        ):
            goodbye = self.settings.goodbye or DEFAULT_GOODBYE
            await self._append(
                AppendKind.INSTRUCTIONS, goodbye, None, kind="instruction"
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
        if self._sideband is not None:
            await self._sideband.detach()
        await self._client.close()
        await self._record.close()

    # -- call controls the runtime exposes ---------------------------------
    async def answer(self, delegation_id: str, text: str) -> Dict[str, Any]:
        """Speak the main agent's answer to one question (``message_agent(reply_to=…)``)."""

        question = self.questions.get(delegation_id)
        if question is None:
            return {"result": "unknown_question", "reply_to": delegation_id}
        if self._sideband is None or self._sideband.is_closed:
            return {
                "result": "call_ended",
                "reply_to": delegation_id,
                "error": "the call has ended; handle it as a follow-up",
            }
        late = question["status"] in ("follow_up",)
        sent = await self._append(
            AppendKind.COMMENTARY, text, delegation_id, kind="answer"
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
            AppendKind.INSTRUCTIONS, text, None, kind="instruction"
        )

    async def transfer(self, target_uri: Optional[str] = None) -> Dict[str, Any]:
        target = target_uri or self.settings.transfer_target
        if not target:
            return {"result": "refused", "error": "this line has no transfer target"}
        try:
            await self._client.refer(self.settings.session_id, target)
        except Exception as exc:  # noqa: BLE001 - reported to the parent, never retried
            return {"result": "error", "error": f"{type(exc).__name__}: {exc}"}
        await self._write([{"kind": "transfer", "speaker": "job", "text": target}])
        return {"result": "transferring", "target": target}

    async def hangup(self, *, record: bool = True) -> bool:
        try:
            await self._client.hangup(self.settings.session_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("phone %s: hangup failed: %s", self.handle, exc)
            return False
        if record:
            await self._write([{"kind": "hangup", "speaker": "job", "text": ""}])
        return True

    def open_questions(self) -> List[Dict[str, Any]]:
        return [q for q in self.questions.values() if q["status"] == "open"]

    # -- internals ---------------------------------------------------------
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

    async def _changed(self) -> None:
        if self._on_change is not None:
            try:
                await self._on_change()
            except Exception:  # noqa: BLE001
                logger.warning(
                    "phone %s: change notification failed", self.handle, exc_info=True
                )

    async def _write(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            event.setdefault("event_id", f"job_{uuid.uuid4().hex}")
        await self._record.post_events(self.settings.call_id, events)

    async def _append(
        self,
        append_kind: AppendKind,
        text: str,
        delegation_id: Optional[str],
        *,
        kind: str,
    ) -> bool:
        """Send one append on the actor sideband and record it as a ``kind`` event."""

        if self._sideband is None and self.running:
            # A steer or answer can race the attach at the start of the call.
            try:
                await asyncio.wait_for(self._attach_settled.wait(), _ATTACH_WAIT_S)
            except asyncio.TimeoutError:
                return False
        sideband = self._sideband
        if sideband is None or sideband.is_closed:
            return False
        try:
            await sideband.send_append(append_kind, text, delegation_id=delegation_id)
        except SidebandClosedError:
            return False
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "phone %s: %s append failed: %s", self.handle, append_kind.value, exc
            )
            return False
        self.provider_calls += 1
        self._spawn(
            self._write(
                [
                    {
                        "kind": kind,
                        "speaker": "job",
                        "text": text,
                        "delegation_id": delegation_id,
                    }
                ]
            )
        )
        return True

    async def _backfill(self) -> None:
        """Read what the recorder saw before we attached."""

        backlog = await self._record.events(self.settings.call_id)
        answered: Set[str] = set()
        delegations: List[Dict[str, Any]] = []
        for row in backlog.get("events") or []:
            kind = row.get("kind")
            event_id = row.get("event_id")
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
                        "delegation_id": row["delegation_id"],
                        "turn": len(self.transcript.turns),
                        "event_id": event_id,
                    }
                )
            elif kind in ("answer", "fallback", "follow_up") and row.get(
                "delegation_id"
            ):
                answered.add(str(row["delegation_id"]))
            if event_id:
                self._seen_event_ids.add(str(event_id))
        for item in delegations:
            if item["delegation_id"] in answered:
                continue
            await self._raise_question(item["delegation_id"], upto_turn=item["turn"])

    async def _raise_question(
        self, delegation_id: str, *, upto_turn: Optional[int] = None
    ) -> None:
        if delegation_id in self.questions:
            return
        end = len(self.transcript.turns) if upto_turn is None else upto_turn
        request = (
            " ".join(
                t.text.strip()
                for t in self.transcript.turns[self._last_delegation_turn : end]
                if t.speaker == "caller" and t.text.strip()
            )
            or self.transcript.last_caller_text()
        )
        self._last_delegation_turn = end
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
            AppendKind.COMMENTARY, FOLLOW_UP_TEXT, delegation_id, kind="follow_up"
        ):
            question["status"] = "follow_up"
        else:
            question["status"] = "unanswered"
        await self._changed()

    async def _run_call(self) -> None:
        reported = await self._record.attach(self.settings.call_id, self.job_id)
        call = (reported or {}) if isinstance(reported, dict) else {}
        for attempt in range(_ATTACH_ATTEMPTS):
            try:
                self._sideband = await self._client.attach(self.settings.session_id)
                break
            except aiohttp.WSServerHandshakeError as exc:
                # 404/409: the session is gone or not active — the call ended
                # before the job got here. Anything else may be a blip.
                self.attach_error = f"HTTP {exc.status}: {exc.message}"
                if (
                    exc.status in (401, 403, 404, 409)
                    or attempt == _ATTACH_ATTEMPTS - 1
                ):
                    break
            except Exception as exc:  # noqa: BLE001
                self.attach_error = f"{type(exc).__name__}: {exc}"
                if attempt == _ATTACH_ATTEMPTS - 1:
                    break
            await asyncio.sleep(1.0 + attempt)
        self._attach_settled.set()
        if self._sideband is None:
            logger.info(
                "phone %s: attach refused (%s); reading the record only",
                self.handle,
                self.attach_error,
            )
            await self._backfill()
            self.close_reason = str(call.get("close_reason") or "ended_before_attach")
            return
        self.attach_error = None
        await self._backfill()
        sideband = self._sideband
        while not self._stop.is_set():
            try:
                event = await sideband.next_event(timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if event is None:
                if sideband.closed_event is None and self.close_reason is None:
                    self.close_reason = "connection_lost"
                break
            if event.event_id and event.event_id in self._seen_event_ids:
                continue
            if event.event_id:
                self._seen_event_ids.add(event.event_id)
            if isinstance(event, TranscriptDelta):
                speaker = "caller" if event.speaker is Speaker.CALLER else "assistant"
                self.transcript.add(speaker, event.text, event.start_ms, event.event_id)
            elif isinstance(event, DelegationCreated):
                await self._raise_question(event.delegation_id)
            elif isinstance(event, UsageUpdated):
                self.usage_seconds = event.seconds
            elif isinstance(event, EngineError):
                logger.info(
                    "phone %s: engine error %s: %s",
                    self.handle,
                    event.code,
                    event.message,
                )
            elif isinstance(event, SessionClosed):
                self.close_reason = event.reason.value
                if event.usage_seconds is not None:
                    self.usage_seconds = event.usage_seconds
                break
        for question in self.open_questions():
            question["status"] = "unanswered"
        await self._changed()

    def _report(self) -> str:
        lines: List[str] = []
        duration = None
        if self._started_at is not None:
            duration = self._clock() - self._started_at
        head = f"Phone call {self.settings.call_id}"
        if self.settings.line_name:
            head += f' on line "{self.settings.line_name}"'
        lines.append(head + ".")
        state = self.close_reason or ("detached" if self._stop_reason else "unknown")
        lines.append(
            f"Ended: {state}."
            if self.close_reason
            else f"Status: {state} (the call may still be running; the recorder keeps covering it)."
        )
        if self.usage_seconds is not None:
            lines.append(f"Voice duration: {self.usage_seconds:.0f} s.")
        elif duration is not None:
            lines.append(f"Attached for {duration:.0f} s.")
        if self.attach_error:
            lines.append(
                f"The job did not attach to the live call ({self.attach_error}); the transcript comes from the call record."
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
            else (self.attach_error or "the call ended without a close event"),
        )
