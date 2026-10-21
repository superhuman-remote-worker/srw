"""HTTP, sideband and webhook transport; knowledge-base/knowledge/features/local_voice_gateway.md §4.1."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Callable, Mapping
from urllib.parse import quote

import aiohttp
from aiohttp import web

from voice_gateway.config import Config
from voice_gateway.interfaces import CallSource, Engine
from voice_gateway.scripted import ScriptedCallSource, ScriptedEngine
from voice_gateway.session import ProtocolError, Session


def sign_webhook(key: bytes, webhook_id: str, timestamp: int, body: bytes) -> str:
    """Standard Webhooks HMAC; independent of the agent/orchestrator runtime."""
    signed = f"{webhook_id}.{timestamp}.".encode() + body
    return "v1," + base64.b64encode(hmac.digest(key, signed, "sha256")).decode()


@web.middleware
async def errors(request: web.Request, handler):
    try:
        return await handler(request)
    except ProtocolError as exc:
        return web.json_response(
            {
                "error": {
                    "type": "invalid_request_error",
                    "code": exc.code,
                    "message": exc.message,
                }
            },
            status=exc.status,
        )
    except web.HTTPException as exc:
        return web.json_response(
            {"error": {"code": "invalid_request", "message": exc.reason}},
            status=exc.status,
        )


async def object_body(request: web.Request) -> dict:
    if not request.can_read_body:
        return {}
    try:
        body = await request.json()
    except (ValueError, UnicodeError):
        raise ProtocolError(400, "invalid_body", "body must be a JSON object") from None
    if not isinstance(body, dict):
        raise ProtocolError(400, "invalid_body", "body must be a JSON object")
    return body


class Gateway:
    """Sources create calls here; protocol handlers operate only on Sessions."""

    def __init__(
        self,
        config: Config,
        *,
        source: CallSource | None = None,
        engines: Mapping[str, Callable[[], Engine]] | None = None,
        ringing_timeout_s: float = 15.0,
        replay_window_s: float = 3.0,
        usage_interval_s: float = 5.0,
        retention_s: float | None = None,
    ):
        self.config = config
        # The test-control API reads a call's commands after the job has
        # finished, which can take minutes; a real call needs no history.
        if retention_s is None:
            retention_s = 600.0 if config.test_control else 60.0
        self.retention_s = retention_s
        self.source = source if source is not None else ScriptedCallSource()
        self.engines = (
            engines
            if engines is not None
            else {name: ScriptedEngine for name in config.profiles}
        )
        self.ringing_timeout_s, self.replay_window_s = (
            ringing_timeout_s,
            replay_window_s,
        )
        self.usage_interval_s = usage_interval_s
        self.sessions: dict[str, Session] = {}
        self.tasks: set[asyncio.Task] = set()
        self.sockets: set[web.WebSocketResponse] = set()
        self.http: aiohttp.ClientSession | None = None
        self.app = web.Application(
            middlewares=[errors, self.authorize], client_max_size=1024 * 1024
        )
        self.app.router.add_post("/v1/live/sessions/{sid}/{action}", self.control)
        self.app.router.add_get("/v1/live/sessions/{sid}/attach", self.attach)
        if config.test_control:
            self.app.router.add_post("/test/calls", self.incoming)
            self.app.router.add_get("/test/calls/{sid}", self.state)
            self.app.router.add_get("/test/calls/{sid}/commands", self.commands)
            self.app.router.add_post("/test/calls/{sid}/{action}", self.test_action)
        self.app.on_shutdown.append(self.shutdown)
        self.app.cleanup_ctx.append(self.lifecycle)

    @web.middleware
    async def authorize(self, request: web.Request, handler):
        candidate = request.headers.get("Authorization", "").encode()
        digest = hashlib.sha256(candidate).digest()
        # Compare every key, including during key rotation, without early exit.
        matched = False
        for key in self.config.api_keys:
            matched |= hmac.compare_digest(
                digest, hashlib.sha256(b"Bearer " + key).digest()
            )
        if not matched:
            raise ProtocolError(401, "invalid_api_key", "invalid API key")
        return await handler(request)

    async def lifecycle(self, app):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as http:
            self.http = http
            yield
            await self.shutdown(app)
            self.http = None

    async def shutdown(self, app) -> None:
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for session in self.sessions.values():
            async with session.lock:
                await session.close("connection_lost")
        await asyncio.gather(
            *(ws.close() for ws in tuple(self.sockets)), return_exceptions=True
        )

    def spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def session(self, request: web.Request) -> Session:
        session = self.sessions.get(request.match_info["sid"])
        if session is None:
            raise ProtocolError(404, "session_id_not_found", "no such session")
        return session

    def incoming_call(self, line_name: str, sip_headers: list[dict]) -> Session:
        """Source ingress: publish an incoming real or scripted call."""
        line = self.config.lines.get(line_name)
        if line is None:
            raise ProtocolError(404, "line_not_found", "no such line")
        session = Session(
            line,
            sip_headers,
            self.source,
            self.engines,
            replay_window_s=self.replay_window_s,
            record=self.config.test_control,
        )
        self.sessions[session.id] = session
        self.spawn(self.ringing_deadline(session))
        self.spawn(self.deliver_webhook(session))
        self.spawn(self.usage_loop(session))
        self.spawn(self.retire(session))
        return session

    async def retire(self, session: Session) -> None:
        """Forget a closed call after a grace period; later requests get 404."""
        await session.closed.wait()
        await asyncio.sleep(self.retention_s)
        self.sessions.pop(session.id, None)

    async def ringing_deadline(self, session: Session) -> None:
        try:
            await asyncio.wait_for(session.decided.wait(), self.ringing_timeout_s)
        except TimeoutError:
            await session.expire()

    async def usage_loop(self, session: Session) -> None:
        await session.decided.wait()
        while session.state == "active":
            await asyncio.sleep(self.usage_interval_s)
            if session.state == "active":
                session.usage()
                session.prune()

    async def deliver_webhook(self, session: Session) -> None:
        # Keep body and webhook-id stable across retries: the orchestrator
        # deduplicates deliveries by webhook-id. Never follow a redirect with
        # signed metadata, or log a response which might echo credentials.
        body = json.dumps(
            {
                "object": "event",
                "id": "evt_" + uuid.uuid4().hex,
                "type": "live.transport.incoming",
                "created_at": int(time.time()),
                "data": {
                    "type": "sip",
                    "session_id": session.id,
                    "sip_headers": session.sip_headers,
                },
            }
        ).encode()
        webhook_id = "wh_" + uuid.uuid4().hex
        url = (
            self.config.orchestrator_url
            + "/api/live/webhooks/"
            + quote(session.line.connector_id, safe="")
        )
        delay = 0.25
        deadline = time.monotonic() + self.ringing_timeout_s
        while session.state == "ringing":
            timestamp = int(time.time())
            headers = {
                "Content-Type": "application/json",
                "webhook-id": webhook_id,
                "webhook-timestamp": str(timestamp),
                "webhook-signature": sign_webhook(
                    session.line.webhook_key, webhook_id, timestamp, body
                ),
            }
            try:
                async with self.http.post(
                    url,
                    data=body,
                    headers=headers,
                    allow_redirects=False,
                    ssl=self.config.webhook_verify_ssl,
                ) as response:
                    status = response.status
            except (aiohttp.ClientError, TimeoutError):
                status = 0
            session.webhook_responses.append(status)
            session.notify()
            if 200 <= status < 300 or (
                status and status < 500 and status not in {408, 429}
            ):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0 or session.state != "ringing":
                return
            try:
                await asyncio.wait_for(session.decided.wait(), min(delay, remaining))
                return
            except TimeoutError:
                delay = min(delay * 2, 2.0)

    async def control(self, request: web.Request) -> web.Response:
        session = self.session(request)
        await session.control(request.match_info["action"], await object_body(request))
        return web.Response(status=200)

    async def attach(self, request: web.Request) -> web.StreamResponse:
        session = self.session(request)
        session.require_active()
        ws = web.WebSocketResponse(compress=False, max_msg_size=1024 * 1024)
        await ws.prepare(request)
        self.sockets.add(ws)
        # Accept/close can run while the handshake drains. If the call closed,
        # finish this socket; its next attach gets the normal HTTP 409.
        if session.state != "active":
            await ws.close()
            self.sockets.discard(ws)
            return ws
        replay, queue = session.subscribe()

        async def write():
            try:
                for event in replay:
                    await ws.send_json(event)
                while True:
                    event = await queue.get()
                    if event is None:
                        break
                    await asyncio.wait_for(ws.send_json(event), 5)
                    if event["type"] == "session.closed":
                        break
            except (ConnectionError, TimeoutError, RuntimeError):
                pass
            finally:
                await ws.close()

        writer = asyncio.create_task(write())
        try:
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type == aiohttp.WSMsgType.BINARY:
                        await ws.close(code=1003, message=b"text frames required")
                    continue
                try:
                    command = json.loads(message.data)
                    if not isinstance(command, dict) or not isinstance(
                        command.get("type"), str
                    ):
                        raise ValueError
                except (ValueError, UnicodeError):
                    session.emit(
                        "error",
                        target=queue,
                        error={
                            "type": "invalid_request_error",
                            "code": "invalid_command",
                            "message": "expected a JSON command object",
                        },
                    )
                    continue
                await session.command(command, queue)
        finally:
            session.subscribers.discard(queue)
            session.notify()
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
            self.sockets.discard(ws)
        return ws

    async def incoming(self, request: web.Request) -> web.Response:
        body = await object_body(request)
        line = body.get("line")
        if not isinstance(line, str):
            raise ProtocolError(400, "invalid_line", "line is required")
        headers = body.get("sip_headers")
        if headers is None:
            headers = []
            for field, name in (("from_number", "From"), ("to_number", "To")):
                value = body.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ProtocolError(
                        400, "invalid_number", "from_number and to_number are required"
                    )
                headers.append({"name": name, "value": f"sip:{value}@gateway.example"})
        if (
            not isinstance(headers, list)
            or not all(
                isinstance(h, dict)
                and isinstance(h.get("name"), str)
                and isinstance(h.get("value"), str)
                for h in headers
            )
            or not {"from", "to"} <= {h["name"].lower() for h in headers}
        ):
            raise ProtocolError(
                400,
                "invalid_sip_headers",
                "sip_headers needs From and To name/value pairs",
            )
        session = self.incoming_call(line, headers)
        return web.json_response({"session_id": session.id}, status=201)

    async def state(self, request: web.Request) -> web.Response:
        return web.json_response(self.session(request).snapshot())

    async def test_action(self, request: web.Request) -> web.Response:
        session = self.session(request)
        body = await object_body(request)
        async with session.lock:
            action = request.match_info["action"]
            if action == "remote_hangup":
                await session.close("remote_hangup")
                return web.json_response({"ok": True})
            session.require_active()
            if action == "say":
                speaker, text = body.get("speaker", "caller"), body.get("text")
                if (
                    speaker not in {"caller", "assistant"}
                    or not isinstance(text, str)
                    or not text.strip()
                ):
                    raise ProtocolError(
                        400,
                        "invalid_transcript",
                        "speaker and nonempty text are required",
                    )
                for field in ("start_ms", "end_ms"):
                    if field in body and (
                        type(body[field]) is not int or body[field] < 0
                    ):
                        raise ProtocolError(
                            400,
                            "invalid_offset",
                            "offsets must be nonnegative integers",
                        )
                session.transcript(
                    speaker, text, body.get("start_ms"), body.get("end_ms")
                )
            elif action == "delegate":
                return web.json_response({"delegation_id": session.delegate()})
            elif action == "dtmf":
                digit = body.get("digit")
                if (
                    not isinstance(digit, str)
                    or len(digit) != 1
                    or digit not in "0123456789*#ABCD"
                ):
                    raise ProtocolError(400, "invalid_digit", "invalid DTMF digit")
                session.dtmf(digit)
            elif action == "usage":
                session.usage()
            else:
                raise ProtocolError(404, "unknown_action", "unknown action")
        return web.json_response({"ok": True})

    async def commands(self, request: web.Request) -> web.Response:
        session = self.session(request)
        try:
            after = int(request.query.get("after", "0"))
            timeout = float(request.query.get("timeout", "20"))
            if after < 0 or not 0 <= timeout <= 30:
                raise ValueError
        except ValueError:
            raise ProtocolError(
                400, "invalid_cursor", "after >= 0 and timeout 0..30 required"
            ) from None
        deadline = time.monotonic() + timeout
        while len(session.commands) <= after:
            changed = session.changed
            remaining = deadline - time.monotonic()
            if remaining <= 0 or session.state == "closed":
                return web.Response(status=204)
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except TimeoutError:
                return web.Response(status=204)
        return web.json_response(
            {"command": session.commands[after], "next_index": after + 1}
        )
