"""A fake OpenAI Live API for phone-call tests and the k3d call script.

Implements the slice of GPT-Live SIP that SRW uses (design §3.1.1):

* ``POST /v1/live/sessions/{id}/accept|reject|refer|hangup`` and
  ``POST /v1/live/sessions`` (outbound create);
* ``GET /v1/live/sessions/{id}/attach`` — the sideband WebSocket. Several
  sidebands may attach; each new one gets the events of the last
  ``replay_window_s`` seconds replayed with their original ``event_id``.

Appends (``session.{instructions,thinking,commentary}.append``) are answered
with ``*.appended`` on the sending socket, or an ``error`` carrying the
``client_event_id`` when the ``delegation_id`` is unknown or the content is
empty. ``session.close`` and ``/hangup`` emit ``session.closed``.

The test side drives a call with :meth:`FakeGptLive.incoming_call` (optionally
POSTing a signed ``live.transport.incoming`` webhook), :meth:`emit` /
:meth:`say` / :meth:`delegate` for server events, and
:meth:`wait_for_command` to await what the application sent. Everything the
application sent is kept in :attr:`FakeSession.commands` and
:attr:`FakeSession.rest_calls`.

Runnable standalone for the k3d gate: ``scripts/telephony_fake_call.py``.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import aiohttp
from aiohttp import web

from shared.runtime.live.webhook import INCOMING_CALL_EVENT, sign_webhook

DEFAULT_KEY = "sk-fake-gpt-live"


@dataclass
class FakeSession:
    session_id: str
    direction: str = "inbound"
    status: str = "pending"  # pending | accepted | rejected | active | closed
    accept_config: Optional[Dict[str, Any]] = None
    create_body: Optional[Dict[str, Any]] = None
    reject_status: Optional[int] = None
    refer_target: Optional[str] = None
    close_reason: Optional[str] = None
    sip_headers: List[Dict[str, str]] = field(default_factory=list)
    sockets: List[web.WebSocketResponse] = field(default_factory=list)
    history: List[tuple] = field(default_factory=list)  # (monotonic time, event dict)
    commands: List[Dict[str, Any]] = field(default_factory=list)
    rest_calls: List[tuple] = field(default_factory=list)  # (action, body)
    delegations: set = field(default_factory=set)
    attach_count: int = 0
    usage_seconds: float = 0.0
    changed: asyncio.Event = field(default_factory=asyncio.Event)

    def notify(self) -> None:
        self.changed.set()
        self.changed = asyncio.Event()


class FakeGptLive:
    """In-process fake GPT-Live server. ``await start()``, then use ``.api_base``."""

    def __init__(
        self,
        *,
        api_key: str = DEFAULT_KEY,
        replay_window_s: float = 3.0,
        host: str = "127.0.0.1",
        port: int = 0,
        ack_delay_s: float = 0.0,
    ) -> None:
        self.api_key = api_key
        self.replay_window_s = replay_window_s
        self.host = host
        self.port = port
        self.ack_delay_s = ack_delay_s
        self.sessions: Dict[str, FakeSession] = {}
        self.webhook_responses: List[int] = []
        self._ids = itertools.count(1)
        self._runner: Optional[web.AppRunner] = None
        self._app = web.Application()
        self._app.router.add_post("/v1/live/sessions", self._create)
        self._app.router.add_post("/v1/live/sessions/{sid}/{action}", self._control)
        self._app.router.add_get("/v1/live/sessions/{sid}/attach", self._attach)

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> "FakeGptLive":
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        sockets = site._server.sockets  # type: ignore[union-attr]
        self.port = sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        for session in self.sessions.values():
            for ws in list(session.sockets):
                await ws.close()
        if self._runner is not None:
            await self._runner.cleanup()

    async def __aenter__(self) -> "FakeGptLive":
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    @property
    def api_base(self) -> str:
        return f"http://{self.host}:{self.port}/v1"

    @property
    def ws_base(self) -> str:
        return f"ws://{self.host}:{self.port}/v1"

    # -- helpers -----------------------------------------------------------
    def _authorized(self, request: web.Request) -> bool:
        return request.headers.get("Authorization") == f"Bearer {self.api_key}"

    def _event_id(self) -> str:
        return f"event_fake_{next(self._ids)}"

    def session(self, session_id: str) -> FakeSession:
        return self.sessions[session_id]

    @staticmethod
    def _error(status: int, code: str, message: str) -> web.Response:
        return web.json_response(
            {
                "error": {
                    "type": "invalid_request_error",
                    "code": code,
                    "message": message,
                }
            },
            status=status,
        )

    # -- REST --------------------------------------------------------------
    async def _create(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return self._error(401, "invalid_api_key", "bad key")
        body = await request.json()
        session_id = f"live_fake_out_{next(self._ids)}"
        session = FakeSession(
            session_id=session_id,
            direction="outbound",
            status="active",
            create_body=body,
        )
        self.sessions[session_id] = session
        session.rest_calls.append(("create", body))
        session.notify()
        return web.json_response(
            {"session": {"id": session_id}, "transport": {"type": "sip"}}
        )

    async def _control(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return self._error(401, "invalid_api_key", "bad key")
        session_id = request.match_info["sid"]
        action = request.match_info["action"]
        session = self.sessions.get(session_id)
        if session is None:
            return self._error(404, "session_id_not_found", f"no session {session_id}")
        body = await request.json() if request.can_read_body else None
        session.rest_calls.append((action, body))
        if action == "accept":
            if session.status != "pending":
                return self._error(409, "decision_already_made", "already decided")
            config = (body or {}).get("session") or {}
            if config.get("type") != "live" or not config.get("model"):
                return self._error(
                    400, "invalid_session", "session.type live and model are required"
                )
            session.accept_config = config
            session.status = "accepted"
        elif action == "reject":
            if session.status != "pending":
                return self._error(409, "decision_already_made", "already decided")
            session.reject_status = int((body or {}).get("status_code") or 0)
            session.status = "rejected"
        elif action == "refer":
            session.refer_target = (body or {}).get("target_uri")
        elif action == "hangup":
            await self._close_session(session, "close_requested")
        else:
            return self._error(404, "unknown_action", action)
        session.notify()
        return web.Response(status=200)

    # -- sideband ----------------------------------------------------------
    async def _attach(self, request: web.Request) -> web.StreamResponse:
        if not self._authorized(request):
            return self._error(401, "invalid_api_key", "bad key")
        session = self.sessions.get(request.match_info["sid"])
        if session is None:
            return self._error(404, "session_id_not_found", "no such session")
        if session.status in ("pending", "rejected", "closed"):
            return self._error(409, "session_not_active", session.status)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        session.attach_count += 1
        cutoff = time.monotonic() - self.replay_window_s
        for stamp, event in list(session.history):
            if stamp >= cutoff:
                await ws.send_str(json.dumps(event))
        session.sockets.append(ws)
        session.notify()
        try:
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    continue
                command = json.loads(message.data)
                session.commands.append(command)
                session.notify()
                await self._handle_command(session, ws, command)
        finally:
            if ws in session.sockets:
                session.sockets.remove(ws)
            session.notify()
        return ws

    async def _handle_command(
        self, session: FakeSession, ws: web.WebSocketResponse, command: Dict[str, Any]
    ) -> None:
        ctype = str(command.get("type") or "")
        event_id = command.get("event_id")
        for kind in ("instructions", "thinking", "commentary"):
            if ctype == f"session.{kind}.append":
                delegation_id = command.get("delegation_id")
                if (
                    delegation_id is not None
                    and delegation_id not in session.delegations
                ):
                    await self._send(
                        ws,
                        {
                            "type": "error",
                            "event_id": self._event_id(),
                            "error": {
                                "type": "invalid_request_error",
                                "code": "unknown_delegation",
                                "message": f"unknown delegation {delegation_id}",
                                "param": "delegation_id",
                                "client_event_id": event_id,
                            },
                        },
                    )
                    return
                if not str(command.get("content") or "").strip():
                    await self._send(
                        ws,
                        {
                            "type": "error",
                            "event_id": self._event_id(),
                            "error": {
                                "type": "invalid_request_error",
                                "code": "empty_content",
                                "message": "empty",
                                "client_event_id": event_id,
                            },
                        },
                    )
                    return
                if self.ack_delay_s:
                    await asyncio.sleep(self.ack_delay_s)
                await self._send(
                    ws,
                    {
                        "type": f"session.{kind}.appended",
                        "event_id": self._event_id(),
                        "client_event_id": event_id,
                        "start_ms": 0,
                        "end_ms": 0,
                    },
                )
                return
        if ctype == "session.close":
            await self._close_session(session, "close_requested")

    async def _send(self, ws: web.WebSocketResponse, event: Dict[str, Any]) -> None:
        if not ws.closed:
            await ws.send_str(json.dumps(event))

    async def _close_session(self, session: FakeSession, reason: str) -> None:
        if session.status == "closed":
            return
        session.status = "closed"
        session.close_reason = reason
        await self.emit(
            session.session_id,
            {
                "type": "session.closed",
                "reason": reason,
                "usage": {"seconds": session.usage_seconds},
            },
        )
        for ws in list(session.sockets):
            await ws.close()

    # -- test driver API ---------------------------------------------------
    def new_session(self, *, sip_headers: Optional[List[Dict[str, str]]] = None) -> str:
        session_id = f"live_fake_{uuid.uuid4().hex[:12]}"
        self.sessions[session_id] = FakeSession(
            session_id=session_id, sip_headers=list(sip_headers or [])
        )
        return session_id

    async def incoming_call(
        self,
        *,
        webhook_url: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        to_number: str = "+4960521829990",
        from_number: str = "+4917600000000",
        sip_headers: Optional[List[Dict[str, str]]] = None,
        event_type: str = INCOMING_CALL_EVENT,
        verify_ssl: bool = True,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """Open a pending inbound session and optionally deliver its webhook."""

        headers = sip_headers or [
            {"name": "From", "value": f"sip:{from_number}@sip.telnyx.example"},
            {"name": "To", "value": f"sip:{to_number}@sip.api.openai.com"},
            {"name": "Call-ID", "value": uuid.uuid4().hex},
        ]
        session_id = self.new_session(sip_headers=headers)
        if webhook_url:
            await self.deliver_webhook(
                session_id,
                webhook_url=webhook_url,
                webhook_secret=webhook_secret or "",
                event_type=event_type,
                verify_ssl=verify_ssl,
                extra_headers=extra_headers,
            )
        return session_id

    def webhook_payload(
        self, session_id: str, event_type: str = INCOMING_CALL_EVENT
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "session_id": session_id,
            "sip_headers": self.sessions[session_id].sip_headers,
        }
        if event_type == INCOMING_CALL_EVENT:
            data["type"] = "sip"
        return {
            "object": "event",
            "id": f"evt_{uuid.uuid4().hex[:16]}",
            "type": event_type,
            "created_at": int(time.time()),
            "data": data,
        }

    async def deliver_webhook(
        self,
        session_id: str,
        *,
        webhook_url: str,
        webhook_secret: str,
        event_type: str = INCOMING_CALL_EVENT,
        webhook_id: Optional[str] = None,
        verify_ssl: bool = True,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> int:
        body = json.dumps(self.webhook_payload(session_id, event_type)).encode()
        webhook_id = webhook_id or f"wh_{uuid.uuid4().hex}"
        timestamp = int(time.time())
        headers = {
            "content-type": "application/json",
            "user-agent": "OpenAI/1.0 (fake)",
            "webhook-id": webhook_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": sign_webhook(
                webhook_secret, webhook_id, timestamp, body
            ),
        }
        headers.update(extra_headers or {})
        connector = aiohttp.TCPConnector(ssl=verify_ssl)
        async with aiohttp.ClientSession(connector=connector) as http:
            async with http.post(webhook_url, data=body, headers=headers) as response:
                self.webhook_responses.append(response.status)
                return response.status

    async def emit(self, session_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
        """Broadcast a server event to every attached sideband and keep it for replay."""

        session = self.sessions[session_id]
        event = dict(event)
        event.setdefault("event_id", self._event_id())
        event.setdefault("session_id", session_id)
        session.history.append((time.monotonic(), event))
        for ws in list(session.sockets):
            try:
                await self._send(ws, event)
            except ConnectionResetError:
                pass
        session.notify()
        return event

    async def say(
        self,
        session_id: str,
        speaker: str,
        text: str,
        start_ms: int = 0,
        end_ms: int = 0,
    ) -> None:
        kind = "input" if speaker == "caller" else "output"
        await self.emit(
            session_id,
            {
                "type": f"session.{kind}_transcript.delta",
                "delta": text,
                "start_ms": start_ms,
                "end_ms": end_ms,
            },
        )

    async def delegate(self, session_id: str, *, offset_ms: int = 0) -> str:
        delegation_id = f"item_fake_{uuid.uuid4().hex[:12]}"
        self.sessions[session_id].delegations.add(delegation_id)
        await self.emit(
            session_id,
            {
                "type": "session.delegation.created",
                "offset_ms": offset_ms,
                "delegation": {
                    "id": delegation_id,
                    "type": "delegation",
                    "target": "client",
                },
            },
        )
        return delegation_id

    async def remote_hangup(self, session_id: str) -> None:
        await self._close_session(self.sessions[session_id], "remote_hangup")

    async def wait_until(
        self,
        session_id: str,
        predicate: Callable[[FakeSession], bool],
        timeout: float = 5.0,
    ) -> FakeSession:
        session = self.sessions[session_id]
        deadline = time.monotonic() + timeout
        while not predicate(session):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError(
                    f"fake GPT-Live: condition not met for {session_id}"
                )
            waiter = session.changed
            try:
                await asyncio.wait_for(waiter.wait(), min(remaining, 0.25))
            except asyncio.TimeoutError:
                pass
        return session

    async def wait_for_command(
        self,
        session_id: str,
        command_type: str,
        predicate: Optional[Callable[[Dict[str, Any]], bool]] = None,
        timeout: float = 5.0,
        start_index: int = 0,
    ) -> Dict[str, Any]:
        found: Dict[str, Any] = {}

        def match(session: FakeSession) -> bool:
            for command in session.commands[start_index:]:
                if command.get("type") == command_type and (
                    predicate is None or predicate(command)
                ):
                    found["command"] = command
                    return True
            return False

        await self.wait_until(session_id, match, timeout)
        return found["command"]
