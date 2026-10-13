"""A fake live connector pod and the orchestrator's session journal.

One aiohttp app serves both sides the job's voice subagent talks to
(``agent.subagents.live.PodClient``):

* the pod's session API (``srw.gpt-live-sip/v1``, live_session_jobs §5.4):
  ``POST /v1/sessions/{id}/attach|append|transfer|hangup``,
  ``GET /v1/sessions/{id}/events`` (server-sent events, a 3 s replay then live
  events, ending after ``closed``) and ``GET /v1/sessions/{id}``, all behind
  ``Authorization: Bearer <lease>``;
* the journal: ``GET|POST /api/internal/live/sessions/{id}/events`` behind
  ``X-Internal-Key``.

Like the real pod, it journals what the engine says (transcript, delegations,
fallbacks, ``closed``) and never the job's appends; the job writes its own rows.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiohttp import web
from aiohttp.test_utils import TestServer

LEASE = "scl_fake_lease_token_0123456789"
INTERNAL_KEY = "fake-internal-key"
REPLAY_S = 3.0


@dataclass
class FakeSession:
    session_id: str
    status: str = "active"
    job_id: Optional[str] = None
    #: (monotonic time, event dict) of every engine event, for the replay.
    history: List[tuple] = field(default_factory=list)
    subscribers: List[asyncio.Queue] = field(default_factory=list)
    attached: int = 0
    appends: List[Dict[str, Any]] = field(default_factory=list)
    transfers: List[Dict[str, Any]] = field(default_factory=list)
    hangups: int = 0
    close_reason: Optional[str] = None
    transfer_target: Optional[str] = None
    #: How many more ``/events`` and ``/attach`` calls answer 404 (handoff lag).
    unknown_for: int = 0
    #: The pod forgot the session (restart): every call answers 404.
    forgotten: bool = False
    #: Journal status override (``ringing`` while the handoff is on its way).
    journal_status: Optional[str] = None
    stream_opens: int = 0


class FakeLivePod:
    def __init__(self) -> None:
        self.sessions: Dict[str, FakeSession] = {}
        self.journal: Dict[str, List[Dict[str, Any]]] = {}
        self._next_row = 0
        self.authorizations: List[str] = []
        self.internal_keys: List[str] = []
        self.server: Optional[TestServer] = None
        app = web.Application()
        app.router.add_post("/v1/sessions/{sid}/attach", self._attach)
        app.router.add_get("/v1/sessions/{sid}/events", self._events)
        app.router.add_post("/v1/sessions/{sid}/append", self._append)
        app.router.add_post("/v1/sessions/{sid}/transfer", self._transfer)
        app.router.add_post("/v1/sessions/{sid}/hangup", self._hangup)
        app.router.add_get("/v1/sessions/{sid}", self._status)
        app.router.add_get(
            "/api/internal/live/sessions/{sid}/events", self._journal_read
        )
        app.router.add_post(
            "/api/internal/live/sessions/{sid}/events", self._journal_write
        )
        self.app = app

    # -- lifecycle ------------------------------------------------------------
    async def start(self) -> None:
        self.server = TestServer(self.app)
        await self.server.start_server()

    async def stop(self) -> None:
        for session in self.sessions.values():
            for queue in session.subscribers:
                queue.put_nowait(None)
        if self.server is not None:
            await self.server.close()

    @property
    def url(self) -> str:
        assert self.server is not None
        return str(self.server.make_url("")).rstrip("/")

    def open_session(self, **kwargs: Any) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = FakeSession(session_id=sid, **kwargs)
        self.journal[sid] = []
        return sid

    def session(self, sid: str) -> FakeSession:
        return self.sessions[sid]

    # -- the engine side (what the test drives) --------------------------------
    def emit(self, sid: str, event: Dict[str, Any], *, journal: bool = True) -> str:
        """One engine event: to the stream subscribers, the replay, the journal."""
        session = self.sessions[sid]
        event = dict(event)
        event.setdefault("event_id", f"evt_{uuid.uuid4().hex[:12]}")
        session.history.append((time.monotonic(), event))
        for queue in list(session.subscribers):
            queue.put_nowait(event)
        if journal:
            self.journal_row(sid, _journal_row(event))
        return event["event_id"]

    def say(self, sid: str, speaker: str, text: str, **kw: Any) -> str:
        return self.emit(
            sid, {"type": "transcript", "speaker": speaker, "text": text}, **kw
        )

    def delegate(self, sid: str, **kw: Any) -> str:
        delegation_id = f"item_{uuid.uuid4().hex[:8]}"
        self.emit(sid, {"type": "delegation", "delegation_id": delegation_id}, **kw)
        return delegation_id

    def close(self, sid: str, reason: str = "remote_hangup") -> None:
        session = self.sessions[sid]
        session.status = "ended"
        session.close_reason = reason
        self.emit(sid, {"type": "closed", "reason": reason, "usage_seconds": 42.0})

    def journal_row(self, sid: str, row: Dict[str, Any]) -> None:
        self._next_row += 1
        self.journal[sid].append({"id": self._next_row, **row})

    def job_rows(self, sid: str) -> List[Dict[str, Any]]:
        return [row for row in self.journal[sid] if row.get("speaker") == "job"]

    async def wait_for(self, predicate, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError("condition not reached in time")
            await asyncio.sleep(0.01)

    # -- the pod API -------------------------------------------------------------
    def _auth(self, request: web.Request) -> Optional[web.Response]:
        header = request.headers.get("Authorization", "")
        self.authorizations.append(header)
        if header != f"Bearer {LEASE}":
            return web.json_response({"error": {"code": "unauthorized"}}, status=401)
        return None

    def _known(self, request: web.Request) -> Optional[FakeSession]:
        session = self.sessions.get(request.match_info["sid"])
        if session is None or session.forgotten:
            return None
        if session.unknown_for > 0:
            session.unknown_for -= 1
            return None
        return session

    @staticmethod
    def _unknown() -> web.Response:
        return web.json_response(
            {"ok": False, "error": {"code": "unknown_session"}}, status=404
        )

    def _view(self, session: FakeSession) -> Dict[str, Any]:
        return {
            "session_id": session.session_id,
            "provider_session_id": "rtc_fake",
            "job_id": session.job_id,
            "status": session.status,
            "close_reason": session.close_reason,
            "usage_seconds": 42.0 if session.status == "ended" else None,
        }

    async def _attach(self, request: web.Request) -> web.Response:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self._known(request)
        if session is None:
            return self._unknown()
        session.attached += 1
        return web.json_response(self._view(session))

    async def _status(self, request: web.Request) -> web.Response:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self.sessions.get(request.match_info["sid"])
        if session is None or session.forgotten:
            return self._unknown()
        return web.json_response(self._view(session))

    async def _events(self, request: web.Request) -> web.StreamResponse:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self._known(request)
        if session is None:
            return self._unknown()
        session.stream_opens += 1
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        queue: asyncio.Queue = asyncio.Queue()
        session.subscribers.append(queue)
        try:
            now = time.monotonic()
            replay = [e for t, e in session.history if now - t <= REPLAY_S]
            await resp.write(b": ping\n\n")
            for event in replay:
                await resp.write(_sse(event))
                if event.get("type") == "closed":
                    return resp
            if session.status != "active":
                return resp
            while True:
                event = await queue.get()
                if event is None:
                    break
                await resp.write(_sse(event))
                if event.get("type") == "closed":
                    break
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            if queue in session.subscribers:
                session.subscribers.remove(queue)
        return resp

    async def _append(self, request: web.Request) -> web.Response:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self._known(request)
        if session is None:
            return self._unknown()
        if session.status != "active":
            return web.json_response(
                {"ok": False, "error": {"code": "not_active"}}, status=409
            )
        body = await request.json()
        session.appends.append(body)
        self.emit(
            session.session_id,
            {
                "type": "append_ack",
                "kind": body["kind"],
                "client_event_id": body["event_id"],
            },
            journal=False,
        )
        return web.json_response({"ok": True})

    async def _transfer(self, request: web.Request) -> web.Response:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self._known(request)
        if session is None:
            return self._unknown()
        body = await request.json()
        target = body.get("target") or session.transfer_target
        if not target:
            return web.json_response(
                {"error": {"code": "no_target", "message": "no transfer target"}},
                status=400,
            )
        session.transfers.append({"target": target})
        return web.json_response({"ok": True})

    async def _hangup(self, request: web.Request) -> web.Response:
        if (refused := self._auth(request)) is not None:
            return refused
        session = self._known(request)
        if session is None:
            return self._unknown()
        session.hangups += 1
        if session.status == "active":
            self.close(session.session_id, "close_requested")
        return web.json_response({"ok": True})

    # -- the journal --------------------------------------------------------------
    def _internal(self, request: web.Request) -> Optional[web.Response]:
        key = request.headers.get("X-Internal-Key", "")
        self.internal_keys.append(key)
        if key != INTERNAL_KEY:
            return web.json_response({"detail": "forbidden"}, status=403)
        return None

    async def _journal_read(self, request: web.Request) -> web.Response:
        if (refused := self._internal(request)) is not None:
            return refused
        sid = request.match_info["sid"]
        if sid not in self.journal:
            return web.json_response({"detail": "not found"}, status=404)
        after = int(request.query.get("after_id", "0"))
        limit = int(request.query.get("limit", "500"))
        rows = [row for row in self.journal[sid] if row["id"] > after][:limit]
        session = self.sessions[sid]
        status = session.journal_status or session.status
        return web.json_response(
            {
                "events": rows,
                "next_after_id": rows[-1]["id"] if rows else after,
                "session": {
                    "id": sid,
                    "status": status,
                    "job_id": session.job_id,
                    "close_reason": session.close_reason,
                    "usage_seconds": 42.0 if status == "ended" else None,
                },
            }
        )

    async def _journal_write(self, request: web.Request) -> web.Response:
        if (refused := self._internal(request)) is not None:
            return refused
        sid = request.match_info["sid"]
        body = await request.json()
        for row in body.get("events") or []:
            self.journal_row(sid, row)
        return web.json_response({"inserted": len(body.get("events") or [])})


def _sse(event: Dict[str, Any]) -> bytes:
    return (
        f"id: {event['event_id']}\nevent: {event['type']}\n"
        f"data: {json.dumps(event)}\n\n"
    ).encode()


def _journal_row(event: Dict[str, Any]) -> Dict[str, Any]:
    kind = event["type"]
    if kind == "delegation":
        return {
            "event_id": event["event_id"],
            "kind": "delegation",
            "speaker": "assistant",
            "delegation_id": event["delegation_id"],
            "payload": {},
        }
    if kind == "transcript":
        return {
            "event_id": event["event_id"],
            "kind": "transcript",
            "speaker": event["speaker"],
            "text": event["text"],
            "payload": {},
        }
    if kind == "closed":
        return {
            "event_id": event["event_id"],
            "kind": "closed",
            "speaker": "system",
            "payload": {"reason": event["reason"]},
        }
    return {"event_id": event["event_id"], "kind": "system", "payload": event}
