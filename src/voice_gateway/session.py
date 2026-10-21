"""One call's state and ordered timeline; knowledge-base/knowledge/features/local_voice_gateway.md §4.1."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from collections.abc import Callable, Mapping
from typing import Any

from voice_gateway.config import Line
from voice_gateway.interfaces import CallSource, Engine

# GPT-Live counts its append budget in tokens (500). The driver clips to 1500
# code points and may add " …", so the gateway must accept a little more than
# 1500 characters or a clipped answer would never reach the caller.
MAX_APPEND_CHARS = 2000
APPEND_KINDS = {"instructions", "commentary", "thinking"}


class ProtocolError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


class Session:
    """Lifecycle mutations are serialized; timeline publication never yields.

    An attach registers its queue and snapshots replay before yielding, so a
    live event cannot overtake replay. A slow subscriber is disconnected and
    can reattach without blocking the call or another subscriber.
    """

    def __init__(
        self,
        line: Line,
        headers: list[dict],
        source: CallSource,
        engine_factory: Mapping[str, Callable[[], Engine]],
        *,
        replay_window_s: float = 3.0,
        record: bool = False,
    ):
        self.id = "live_" + uuid.uuid4().hex
        self.line, self.sip_headers, self.source = line, headers, source
        self.engine_factory = engine_factory
        self.engine: Engine | None = None
        self.state = "ringing"
        self.decision: str | None = None
        self.accept_config: dict | None = None
        self.reject_status: int | None = None
        self.refer_target: str | None = None
        self.close_reason: str | None = None
        self.started_at: float | None = None
        self.ended_at: float | None = None
        self.history: deque[tuple[float, dict]] = deque()
        self.replay_window_s = replay_window_s
        self.subscribers: set[asyncio.Queue] = set()
        self.attach_count = 0
        # Commands and REST bodies carry what the caller hears; keep them only
        # for the test-control API.
        self.record = record
        self.commands: list[dict] = []
        self.rest_calls: list[tuple[str, dict]] = []
        self.delegations: set[str] = set()
        self.caller_since_delegation = False
        self.webhook_responses: list[int] = []
        self.lock = asyncio.Lock()
        self.changed = asyncio.Event()
        self.decided = asyncio.Event()
        self.closed = asyncio.Event()

    def notify(self) -> None:
        self.changed.set()
        self.changed = asyncio.Event()

    @property
    def seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        return round((self.ended_at or time.monotonic()) - self.started_at, 3)

    def require_active(self) -> None:
        if self.state != "active":
            raise ProtocolError(409, "session_not_active", "session is not active")

    def prune(self) -> None:
        cutoff = time.monotonic() - self.replay_window_s
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    def emit(
        self, event_type: str, *, target: asyncio.Queue | None = None, **payload
    ) -> dict:
        event = {
            **payload,
            "type": event_type,
            "event_id": "event_" + uuid.uuid4().hex,
            "session_id": self.id,
        }
        self.prune()
        self.history.append((time.monotonic(), event))
        queues = (target,) if target is not None else tuple(self.subscribers)
        for queue in queues:
            if queue.full():
                self.subscribers.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
            else:
                queue.put_nowait(event)
        self.notify()
        return event

    def subscribe(self) -> tuple[list[dict], asyncio.Queue]:
        self.require_active()
        self.prune()
        queue = asyncio.Queue(maxsize=1024)
        self.subscribers.add(queue)
        self.attach_count += 1
        self.notify()
        return [event for _, event in self.history], queue

    def transcript(
        self,
        speaker: str,
        text: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> None:
        self.require_active()
        offset = int(self.seconds * 1000)
        self.emit(
            f"session.{'input' if speaker == 'caller' else 'output'}_transcript.delta",
            delta=text,
            start_ms=offset if start_ms is None else start_ms,
            end_ms=offset if end_ms is None else end_ms,
        )
        if speaker == "caller":
            self.caller_since_delegation = True

    def delegate(self) -> str:
        self.require_active()
        if not self.caller_since_delegation:
            raise ProtocolError(
                409, "missing_transcript", "delegation requires a caller transcript"
            )
        delegation_id = "item_" + uuid.uuid4().hex
        self.delegations.add(delegation_id)
        self.caller_since_delegation = False
        self.emit(
            "session.delegation.created",
            offset_ms=int(self.seconds * 1000),
            delegation={"id": delegation_id, "type": "delegation", "target": "client"},
        )
        return delegation_id

    def dtmf(self, digit: str) -> None:
        self.require_active()
        self.emit("transport.dtmf.received", event=digit)

    def usage(self) -> None:
        self.require_active()
        self.emit("session.usage.updated", usage={"seconds": self.seconds})

    async def control(self, action: str, body: dict) -> None:
        async with self.lock:
            if self.record:
                self.rest_calls.append((action, body))
            if action in {"accept", "reject"}:
                if self.state != "ringing":
                    raise ProtocolError(409, "decision_already_made", "already decided")
                if action == "accept":
                    config = body.get("session")
                    if not isinstance(config, dict) or config.get("type") != "live":
                        raise ProtocolError(
                            400, "invalid_session", "session.type must be live"
                        )
                    model = config.get("model")
                    if not isinstance(model, str) or model not in self.engine_factory:
                        raise ProtocolError(
                            400, "model_not_found", "unknown model profile"
                        )
                    instructions = config.get("instructions", "")
                    audio, delegation = (
                        config.get("audio", {}),
                        config.get("delegation", {}),
                    )
                    if (
                        not isinstance(instructions, str)
                        or len(instructions) > 48000
                        or not isinstance(audio, dict)
                        or not isinstance(audio.get("output", {}), dict)
                        or not isinstance(audio.get("output", {}).get("voice", ""), str)
                        or not isinstance(delegation, dict)
                        or delegation.get("type") != "client"
                    ):
                        raise ProtocolError(
                            400,
                            "invalid_session",
                            "invalid instructions, audio or delegation",
                        )
                    self.engine = self.engine_factory[model]()
                    await self.source.answer(self)
                    self.started_at = time.monotonic()
                    self.accept_config = config
                    self.state, self.decision = "active", "accept"
                    self.decided.set()
                    self.emit("session.started")
                    await self.engine.start(self, body)
                else:
                    status = body.get("status_code")
                    if type(status) is not int or not 300 <= status <= 699:
                        raise ProtocolError(
                            400, "invalid_status_code", "status_code must be 300..699"
                        )
                    await self.source.reject(self, status)
                    self.reject_status, self.decision = status, "reject"
                    self.finish("close_requested")
            elif action == "refer":
                self.require_active()
                target = body.get("target_uri")
                if not isinstance(target, str) or not target.strip():
                    raise ProtocolError(
                        400, "invalid_target_uri", "target_uri is required"
                    )
                await self.source.refer(self, target)
                self.refer_target = target
            elif action == "hangup":
                self.require_active()
                await self.source.hangup(self)
                await self.close("close_requested")
            else:
                raise ProtocolError(404, "unknown_action", "unknown action")
            self.notify()

    def finish(self, reason: str) -> None:
        self.ended_at = time.monotonic()
        self.state, self.close_reason = "closed", reason
        self.decided.set()
        self.closed.set()
        self.emit("session.usage.updated", usage={"seconds": self.seconds})
        self.emit("session.closed", reason=reason, usage={"seconds": self.seconds})

    async def close(self, reason: str) -> None:
        if self.state == "closed":
            return
        self.finish(reason)
        if self.engine is not None:
            await self.engine.stop(self)

    async def expire(self) -> None:
        async with self.lock:
            if self.state == "ringing":
                await self.source.reject(self, 480)
                self.reject_status, self.decision = 480, "timeout"
                await self.close("expired")

    async def command(self, command: dict, target: asyncio.Queue) -> None:
        async with self.lock:
            if self.record:
                self.commands.append(command)
                self.notify()
            kind = (
                command.get("type", "").removeprefix("session.").removesuffix(".append")
            )
            code = None
            param = None
            delegation = command.get("delegation_id")
            content = command.get("content")
            if self.state != "active":
                code = "session_not_active"
            elif command.get("type") == "session.close":
                await self.source.hangup(self)
                await self.close("close_requested")
                return
            elif (
                kind not in APPEND_KINDS
                or command.get("type") != f"session.{kind}.append"
            ):
                code = "unknown_command"
            elif (
                not isinstance(command.get("event_id"), str) or not command["event_id"]
            ):
                code, param = "invalid_event_id", "event_id"
            elif delegation is not None and (
                not isinstance(delegation, str) or delegation not in self.delegations
            ):
                code, param = "unknown_delegation", "delegation_id"
            elif not isinstance(content, str) or not content.strip():
                code, param = "empty_content", "content"
            elif len(content) > MAX_APPEND_CHARS:
                code, param = "content_too_long", "content"
            if code:
                self.emit(
                    "error",
                    target=target,
                    error={
                        "type": "invalid_request_error",
                        "code": code,
                        "message": "command refused",
                        "param": param,
                        "client_event_id": command.get("event_id"),
                    },
                )
                return
            offset = int(self.seconds * 1000)
            self.emit(
                f"session.{kind}.appended",
                target=target,
                client_event_id=command["event_id"],
                start_ms=offset,
                end_ms=offset,
            )
            await self.engine.append(self, kind, content, delegation)

    def snapshot(self) -> dict[str, Any]:
        return {
            "session_id": self.id,
            "state": self.state,
            "decision": self.decision,
            "accept_config": self.accept_config,
            "reject_status": self.reject_status,
            "refer_target": self.refer_target,
            "close_reason": self.close_reason,
            "usage_seconds": self.seconds,
            "attach_count": self.attach_count,
            "sockets": len(self.subscribers),
            "commands": self.commands,
            "rest_calls": self.rest_calls,
            "webhook_responses": self.webhook_responses,
        }
