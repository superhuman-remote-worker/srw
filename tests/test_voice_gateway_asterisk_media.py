"""Call audio over chan_websocket: framing, pacing, flush; local_voice_gateway.md §4.2–4.3."""

from __future__ import annotations

import asyncio
import base64
import json
import time

import aiohttp
import pytest
import pytest_asyncio

from voice_gateway.asterisk.media import MediaPort, MediaServer
from voice_gateway.interfaces import FRAME_BYTES


class FakeSocket:
    """Records what the port sends towards Asterisk."""

    def __init__(self):
        self.frames: list[tuple[float, bytes]] = []
        self.texts: list[dict] = []
        self.closed = False

    async def send_bytes(self, data: bytes) -> None:
        self.frames.append((time.monotonic(), data))

    async def send_str(self, data: str) -> None:
        self.texts.append(json.loads(data))


def tone(ms: int, value: int = 1) -> bytes:
    return bytes([value]) * (ms * FRAME_BYTES // 20)


@pytest.mark.asyncio
async def test_caller_audio_is_cut_into_frames():
    port = MediaPort("m")
    port.feed(b"\1" * 1000)
    port.feed(b"\2" * 600)
    port.feed(b"\3" * 300)  # 1900 bytes: two frames and 620 left over
    await port.close()
    frames = [frame async for frame in port.frames()]
    assert [len(f) for f in frames] == [FRAME_BYTES, FRAME_BYTES]
    assert frames[1] == b"\1" * 360 + b"\2" * 280


@pytest.mark.asyncio
async def test_an_engine_that_never_reads_keeps_only_recent_audio():
    port = MediaPort("m", inbound_frames=5)
    for value in range(10):
        port.feed(bytes([value]) * FRAME_BYTES)
    await port.close()
    frames = [frame async for frame in port.frames()]
    assert [f[0] for f in frames] == [5, 6, 7, 8, 9]


@pytest.mark.asyncio
async def test_frames_wait_for_audio_and_end_with_the_call():
    port = MediaPort("m")
    seen = []

    async def read():
        async for frame in port.frames():
            seen.append(frame)

    reader = asyncio.create_task(read())
    await asyncio.sleep(0.01)
    port.feed(b"\5" * FRAME_BYTES)
    await asyncio.sleep(0.01)
    assert seen == [b"\5" * FRAME_BYTES]
    await port.close()
    await asyncio.wait_for(reader, 1)


async def until(predicate, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


# Timing assertions only bound what a slow test machine cannot break: the
# port never runs ahead of the caller, and everything eventually plays.


@pytest.mark.asyncio
async def test_playback_is_paced_a_few_frames_ahead():
    port = MediaPort("m", lead_frames=3)
    ws = FakeSocket()
    port.attach(ws)
    started = time.monotonic()
    await port.play(tone(400))  # 20 frames
    await asyncio.sleep(0.1)
    elapsed_ms = (time.monotonic() - started) * 1000
    # Heard so far, plus at most the 3-frame lead and the frame just sent.
    assert 1 <= len(ws.frames) <= elapsed_ms / 20 + 5
    await until(lambda: len(ws.frames) == 20)
    assert all(len(frame) == FRAME_BYTES for _, frame in ws.frames)
    # Never faster than the caller hears it: 17 frames before the lead.
    assert ws.frames[-1][0] - started >= 0.32
    await until(lambda: port.played_ms() == 400)
    await port.close()


@pytest.mark.asyncio
async def test_flush_drops_unheard_audio_and_counts_only_what_played():
    port = MediaPort("m", lead_frames=3)
    ws = FakeSocket()
    port.attach(ws)
    started = time.monotonic()
    await port.play(tone(1000))
    await until(lambda: len(ws.frames) >= 8)
    await port.flush()
    elapsed_ms = (time.monotonic() - started) * 1000
    played = port.played_ms()
    sent = len(ws.frames)
    assert ws.texts == [{"command": "FLUSH_MEDIA"}]
    assert played <= min(sent * 20, elapsed_ms + 20)
    await asyncio.sleep(0.15)
    assert len(ws.frames) == sent  # nothing more after the flush
    # The frame playing at the flush counts; nothing queued behind it does.
    assert played <= port.played_ms() <= played + 20
    assert port.played_ms() < 1000
    # Playback continues normally afterwards.
    await port.play(tone(100))
    await until(lambda: len(ws.frames) == sent + 5)
    await port.close()


@pytest.mark.asyncio
async def test_a_short_tail_is_padded_and_counted_by_its_real_length():
    port = MediaPort("m")
    ws = FakeSocket()
    port.attach(ws)
    await port.play(b"\7" * (FRAME_BYTES + 96))
    await until(lambda: len(ws.frames) == 2)
    assert ws.frames[1][1] == b"\7" * 96 + b"\0" * (FRAME_BYTES - 96)
    await until(lambda: port.played_ms() == (FRAME_BYTES + 96) // 32)
    await asyncio.sleep(0.05)
    assert len(ws.frames) == 2
    await port.close()


@pytest.mark.asyncio
async def test_audio_before_the_socket_waits_for_it():
    port = MediaPort("m")
    await port.play(tone(40))
    ws = FakeSocket()
    port.attach(ws)
    await until(lambda: len(ws.frames) == 2)
    await port.close()


@pytest.mark.asyncio
async def test_xoff_pauses_playback_until_xon():
    port = MediaPort("m")
    ws = FakeSocket()
    port.attach(ws)
    port.control({"event": "MEDIA_XOFF"})
    await port.play(tone(100))
    await asyncio.sleep(0.05)
    assert ws.frames == []
    port.control({"event": "MEDIA_XON"})
    await until(lambda: len(ws.frames) == 5)
    await port.close()


# -- the listener Asterisk connects to ---------------------------------------------------


def auth(user="srw", password="pw"):
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def media_start(channel="chan-1", fmt="slin16", size=FRAME_BYTES):
    return json.dumps(
        {
            "event": "MEDIA_START",
            "connection_id": "srw-gateway",
            "channel": "WebSocket/srw-gateway/0x1",
            "channel_id": channel,
            "format": fmt,
            "optimal_frame_size": size,
            "ptime": 20,
        }
    )


@pytest_asyncio.fixture
async def server():
    media = MediaServer("127.0.0.1:0", "srw", "pw", start_timeout_s=1)
    await media.start()
    port = media.runner.addresses[0][1]
    async with aiohttp.ClientSession() as http:
        yield media, http, f"ws://127.0.0.1:{port}/media"
    await media.stop()


@pytest.mark.asyncio
async def test_listener_ties_a_connection_to_its_call(server):
    media, http, url = server
    port = MediaPort("chan-1")
    media.expect(port)
    async with http.ws_connect(url, headers=auth(), protocols=("media",)) as ws:
        assert ws.protocol == "media"
        await ws.send_str(media_start())
        await asyncio.wait_for(port.started.wait(), 1)
        await ws.send_bytes(b"\1" * FRAME_BYTES * 2)
        await port.play(b"\2" * FRAME_BYTES)
        message = await ws.receive(timeout=1)
        assert message.type == aiohttp.WSMsgType.BINARY
        assert message.data == b"\2" * FRAME_BYTES
        await port.flush()
        assert json.loads((await ws.receive(timeout=1)).data) == {
            "command": "FLUSH_MEDIA"
        }
    await asyncio.wait_for(port.ended.wait(), 1)
    frames = [frame async for frame in port.frames()]
    assert frames == [b"\1" * FRAME_BYTES] * 2
    assert "chan-1" not in media.expected


@pytest.mark.asyncio
async def test_listener_requires_the_shared_password(server):
    media, http, url = server
    with pytest.raises(aiohttp.WSServerHandshakeError) as error:
        await http.ws_connect(url, headers=auth(password="wrong"))
    assert error.value.status == 401


@pytest.mark.parametrize(
    "first",
    [
        media_start(channel="someone-else"),
        media_start(fmt="ulaw", size=160),
        json.dumps({"event": "STATUS"}),
        "not json",
    ],
    ids=["unknown-channel", "ulaw", "no-media-start", "not-json"],
)
@pytest.mark.asyncio
async def test_listener_closes_anything_but_an_expected_slin16_start(server, first):
    media, http, url = server
    port = MediaPort("chan-1")
    media.expect(port)
    async with http.ws_connect(url, headers=auth()) as ws:
        await ws.send_str(first)
        message = await ws.receive(timeout=2)
        assert message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED)
    assert not port.started.is_set()
