"""Call audio over chan_websocket; knowledge-base/knowledge/features/local_voice_gateway.md §4.2–4.3.

Asterisk's externalMedia channel connects out to the gateway (the entry in
websocket_client.conf) for every call. Its first text frame is MEDIA_START
(JSON control messages, ``chan_websocket.conf``) naming the channel id the
gateway chose, which ties the socket to its call. Binary frames carry slin16:
16 kHz PCM16 little-endian, 640 bytes per 20 ms.

Playback is paced here, a few frames ahead of the caller, so a flush
(barge-in) drops almost everything locally, FLUSH_MEDIA drops the rest in
Asterisk, and ``played_ms`` is what the caller actually heard. Asterisk
re-times frames itself, but would queue up to 20 s; nothing is buffered on
disk on either side.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
from collections import deque
from collections.abc import AsyncIterator

import aiohttp
from aiohttp import web

from voice_gateway.interfaces import FRAME_BYTES

logger = logging.getLogger(__name__)

FRAME_S = 0.020
MEDIA_FORMAT = "slin16"
PROTOCOL = "media"
#: How far playback runs ahead of the caller: enough to ride out scheduling
#: jitter, small enough that a flush is nearly exact.
LEAD_FRAMES = 3
#: Caller audio kept for an engine that reads slowly or not at all (5 s);
#: the oldest frames go first.
INBOUND_FRAMES = 250
_BYTES_PER_MS = FRAME_BYTES // 20


class MediaPort:
    """The AudioPort of one call: caller frames in, paced playback out."""

    def __init__(
        self,
        channel_id: str,
        *,
        lead_frames: int = LEAD_FRAMES,
        inbound_frames: int = INBOUND_FRAMES,
        clock=None,
    ):
        self.channel_id = channel_id
        self.lead_s = lead_frames * FRAME_S
        self.clock = clock or asyncio.get_running_loop().time
        self.ws: web.WebSocketResponse | None = None
        self.started = asyncio.Event()
        self.ended = asyncio.Event()
        self._inbound: deque[bytes] = deque(maxlen=inbound_frames)
        self._partial = bytearray()
        self._arrived = asyncio.Event()
        self._pending = bytearray()
        self._in_flight: deque[tuple[float, float, int]] = deque()
        self._queue_end = 0.0
        self._played_bytes = 0
        self._wake = asyncio.Event()
        self._paused = False
        self._pacer: asyncio.Task | None = None

    # -- AudioPort ---------------------------------------------------------------

    async def frames(self) -> AsyncIterator[bytes]:
        while True:
            while self._inbound:
                yield self._inbound.popleft()
            if self.ended.is_set():
                return
            self._arrived.clear()
            await self._arrived.wait()

    async def play(self, pcm: bytes) -> None:
        if self.ended.is_set() or not pcm:
            return
        self._pending += pcm
        self._wake.set()
        if self._pacer is None and self.ws is not None:
            self._pacer = asyncio.create_task(self._pace())

    async def flush(self) -> None:
        now = self.clock()
        self._pending.clear()
        self._retire(now)
        # A frame the caller is hearing right now counts as heard.
        while self._in_flight and self._in_flight[-1][0] > now:
            self._in_flight.pop()
        self._queue_end = self._in_flight[-1][1] if self._in_flight else now
        if self.ws is not None and not self.ws.closed:
            try:
                await self.ws.send_str(json.dumps({"command": "FLUSH_MEDIA"}))
            except ConnectionError:
                pass

    def played_ms(self) -> int:
        self._retire(self.clock())
        return self._played_bytes // _BYTES_PER_MS

    # -- the socket side -----------------------------------------------------------

    def attach(self, ws: web.WebSocketResponse) -> None:
        self.ws = ws
        self.started.set()
        if self._pending and self._pacer is None:
            self._pacer = asyncio.create_task(self._pace())

    def feed(self, data: bytes) -> None:
        """Caller audio from Asterisk, re-cut into FRAME_BYTES frames."""
        self._partial += data
        while len(self._partial) >= FRAME_BYTES:
            self._inbound.append(bytes(self._partial[:FRAME_BYTES]))
            del self._partial[:FRAME_BYTES]
        self._arrived.set()

    def control(self, message: dict) -> None:
        event = message.get("event")
        if event == "MEDIA_XOFF":
            self._paused = True
        elif event == "MEDIA_XON":
            self._paused = False
            self._wake.set()
        elif event == "ERROR":
            logger.warning("media %s: Asterisk reported an error", self.channel_id)

    async def close(self) -> None:
        self.ended.set()
        self._arrived.set()
        self._pending.clear()
        self._wake.set()
        if self._pacer is not None:
            self._pacer.cancel()
            await asyncio.gather(self._pacer, return_exceptions=True)
            self._pacer = None

    # -- pacing ----------------------------------------------------------------------

    def _retire(self, now: float) -> None:
        while self._in_flight and self._in_flight[0][1] <= now:
            self._played_bytes += self._in_flight.popleft()[2]

    async def _pace(self) -> None:
        try:
            while not self.ended.is_set():
                now = self.clock()
                self._retire(now)
                ahead = self._queue_end - now
                if not self._pending or self._paused:
                    self._wake.clear()
                    await self._wake.wait()
                    continue
                if ahead >= self.lead_s:
                    await asyncio.sleep(ahead - self.lead_s + 0.001)
                    continue
                if len(self._pending) < FRAME_BYTES and ahead > 0.002:
                    # A partial frame waits for more audio until the caller
                    # would otherwise hear a gap, then goes out padded.
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), ahead)
                    except TimeoutError:
                        pass
                    continue
                real = min(len(self._pending), FRAME_BYTES)
                frame = bytes(self._pending[:real]).ljust(FRAME_BYTES, b"\0")
                del self._pending[:real]
                start = max(now, self._queue_end)
                self._queue_end = start + FRAME_S
                self._in_flight.append((start, self._queue_end, real))
                await self.ws.send_bytes(frame)
        except (ConnectionError, RuntimeError):
            # The socket closed under us; the call's teardown follows.
            pass


def _parse_listen(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host.strip("[]"), int(port)


class MediaServer:
    """Listener for Asterisk's per-call media connections."""

    def __init__(
        self,
        listen: str,
        user: str,
        password: str,
        *,
        start_timeout_s: float = 5.0,
    ):
        self.host, self.port = _parse_listen(listen)
        self._auth = hashlib.sha256(
            b"Basic " + base64.b64encode(f"{user}:{password}".encode())
        ).digest()
        self.start_timeout_s = start_timeout_s
        self.expected: dict[str, MediaPort] = {}
        self.app = web.Application()
        self.app.router.add_get("/media", self.handle)
        self.runner: web.AppRunner | None = None

    def expect(self, port: MediaPort) -> None:
        self.expected[port.channel_id] = port

    def forget(self, channel_id: str) -> None:
        self.expected.pop(channel_id, None)

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, self.host, self.port).start()

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

    async def handle(self, request: web.Request) -> web.StreamResponse:
        offered = hashlib.sha256(request.headers.get("Authorization", "").encode())
        if not hmac.compare_digest(offered.digest(), self._auth):
            return web.Response(status=401)
        ws = web.WebSocketResponse(
            protocols=(PROTOCOL,), max_msg_size=1024 * 1024, compress=False
        )
        await ws.prepare(request)
        port = None
        try:
            first = await ws.receive(timeout=self.start_timeout_s)
            start = (
                json.loads(first.data) if first.type == aiohttp.WSMsgType.TEXT else {}
            )
            if not isinstance(start, dict) or start.get("event") != "MEDIA_START":
                logger.warning("media connection without MEDIA_START; closing")
                return ws
            port = self.expected.pop(str(start.get("channel_id")), None)
            if port is None:
                logger.warning("media connection for an unknown channel; closing")
                return ws
            if (
                start.get("format") != MEDIA_FORMAT
                or start.get("optimal_frame_size") != FRAME_BYTES
            ):
                logger.error(
                    "media %s: expected %s in %d-byte frames, got %s/%s",
                    port.channel_id,
                    MEDIA_FORMAT,
                    FRAME_BYTES,
                    start.get("format"),
                    start.get("optimal_frame_size"),
                )
                return ws
            port.attach(ws)
            async for message in ws:
                if message.type == aiohttp.WSMsgType.BINARY:
                    port.feed(message.data)
                elif message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        control = json.loads(message.data)
                    except ValueError:
                        continue
                    if isinstance(control, dict):
                        port.control(control)
        except (TimeoutError, ValueError, ConnectionError):
            pass
        finally:
            if port is not None:
                await port.close()
            await ws.close()
        return ws
