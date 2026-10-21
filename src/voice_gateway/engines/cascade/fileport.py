"""A phone line without a phone: an AudioPort over WAV clips; local_voice_gateway.md §4.3, §6.1 (WP3).

The try-it harness and the tests put caller clips on the line (``say``) and
the port sends them in real time as 20 ms frames, silence in between, like
Asterisk's chan_websocket. What the engine plays is paced the same way and
recorded with the silences, so the recording is what the caller heard.
Nothing here is used by a real call, and nothing is written to disk unless
the caller of ``write_wav`` asks for it.
"""

from __future__ import annotations

import asyncio
import audioop
import struct
import wave
from collections import deque
from pathlib import Path

from voice_gateway.interfaces import FRAME_BYTES, SAMPLE_RATE

FRAME_MS = 20


class FileAudioPort:
    """AudioPort with a clock: one 20 ms frame in and out per tick."""

    def __init__(self, *, realtime: bool = True):
        self.realtime = realtime
        self.clock_ms = 0
        self.heard = bytearray()  # the engine's audio as played, with silences
        self.said = bytearray()  # the caller's audio as sent, with silences
        #: Clock times (ms) at which the engine's audio started after silence.
        self.voice_starts: list[int] = []
        #: Clock times (ms) at which each caller clip ended.
        self.clip_ends: list[int] = []
        self._incoming: deque[bytes] = deque()
        self._outgoing = bytearray()
        self._played_ms = 0.0
        self._frames: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._ticker: asyncio.Task | None = None
        self._was_playing = False
        self._clip_open = False

    # -- AudioPort ---------------------------------------------------------

    async def frames(self):
        while (frame := await self._frames.get()) is not None:
            yield frame

    async def play(self, pcm: bytes) -> None:
        self._outgoing += pcm

    async def flush(self) -> None:
        self._outgoing.clear()

    def played_ms(self) -> int:
        return int(self._played_ms)

    # -- the line ----------------------------------------------------------

    def start(self) -> None:
        if self._ticker is None:
            self._ticker = asyncio.create_task(self._tick())

    async def close(self) -> None:
        if self._ticker is not None:
            self._ticker.cancel()
            await asyncio.gather(self._ticker, return_exceptions=True)
            self._ticker = None
        self._frames.put_nowait(None)

    def say(self, pcm: bytes) -> None:
        """Queue a caller clip (16 kHz PCM16) after anything still queued."""
        pcm = pcm + bytes(-len(pcm) % FRAME_BYTES)
        for at in range(0, len(pcm), FRAME_BYTES):
            self._incoming.append(pcm[at : at + FRAME_BYTES])
        self._incoming.append(b"")  # marks the clip's end

    @property
    def caller_speaking(self) -> bool:
        return bool(self._incoming)

    @property
    def voice_playing(self) -> bool:
        return bool(self._outgoing)

    async def _tick(self) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time()
        while True:
            self._step()
            if self.realtime:
                due += FRAME_MS / 1000
                await asyncio.sleep(max(0.0, due - loop.time()))
            else:
                await asyncio.sleep(0)

    def _step(self) -> None:
        frame = self._incoming.popleft() if self._incoming else bytes(FRAME_BYTES)
        if frame == b"":
            self.clip_ends.append(self.clock_ms)
            frame = self._incoming.popleft() if self._incoming else bytes(FRAME_BYTES)
        self.said += frame
        self._frames.put_nowait(frame)
        out = bytes(self._outgoing[:FRAME_BYTES])
        del self._outgoing[:FRAME_BYTES]
        if out and not self._was_playing:
            self.voice_starts.append(self.clock_ms)
        self._was_playing = bool(out)
        self._played_ms += len(out) / (SAMPLE_RATE * 2 / 1000)
        self.heard += out + bytes(FRAME_BYTES - len(out))
        self.clock_ms += FRAME_MS


_PCM, _ALAW, _ULAW = 1, 6, 7


def read_wav(path: str | Path) -> bytes:
    """A mono WAV as 16 kHz PCM16; 8 kHz G.711 A-law/µ-law (the phone) is
    decoded and upsampled, so a test clip sounds like a real caller. The
    stdlib ``wave`` module reads PCM only, hence the small RIFF walk."""
    raw = Path(path).read_bytes()
    if raw[:4] != b"RIFF" or raw[8:12] != b"WAVE":
        raise ValueError("not a WAV file")
    fmt = data = None
    at = 12
    while at + 8 <= len(raw):
        chunk, size = raw[at : at + 4], struct.unpack("<I", raw[at + 4 : at + 8])[0]
        body = raw[at + 8 : at + 8 + size]
        if chunk == b"fmt ":
            fmt = struct.unpack("<HHIIHH", body[:16])
        elif chunk == b"data":
            data = body
        at += 8 + size + (size & 1)
    if fmt is None or data is None:
        raise ValueError("WAV without fmt or data")
    tag, channels, rate, _, _, bits = fmt
    if channels != 1:
        raise ValueError("caller clips must be mono")
    if tag == _ALAW:
        pcm = audioop.alaw2lin(data, 2)
    elif tag == _ULAW:
        pcm = audioop.ulaw2lin(data, 2)
    elif tag == _PCM and bits in {8, 16, 24, 32}:
        pcm = data if bits == 16 else audioop.lin2lin(data, bits // 8, 2)
    else:
        raise ValueError("unsupported WAV encoding")
    if rate != SAMPLE_RATE:
        pcm, _ = audioop.ratecv(pcm, 2, 1, rate, SAMPLE_RATE, None)
    return pcm


def phone(pcm: bytes) -> bytes:
    """What 16 kHz audio sounds like after an 8 kHz G.711 A-law leg."""
    narrow, _ = audioop.ratecv(pcm, 2, 1, SAMPLE_RATE, 8000, None)
    narrow = audioop.alaw2lin(audioop.lin2alaw(narrow, 2), 2)
    wide, _ = audioop.ratecv(narrow, 2, 1, 8000, SAMPLE_RATE, None)
    return wide


def write_wav(path: str | Path, pcm: bytes, *, channels: int = 1) -> None:
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(channels)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(pcm)


def interleave(left: bytes, right: bytes) -> bytes:
    """Two mono tracks as one stereo track (caller left, voice right)."""
    size = max(len(left), len(right))
    left, right = left.ljust(size, b"\0"), right.ljust(size, b"\0")
    return audioop.add(
        audioop.tostereo(left, 2, 1, 0), audioop.tostereo(right, 2, 0, 1), 2
    )
