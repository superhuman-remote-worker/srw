"""Pipecat transport over the call's AudioPort; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

``CallInput`` feeds the port's 20 ms caller frames into the pipeline.
``CallOutput`` hands the reply to ``AudioPort.play`` but keeps at most
``lead_ms`` queued ahead of what ``played_ms`` says the caller heard, so
Pipecat's idea of "the bot is speaking" matches the phone and a barge-in
loses little. It also keeps the ledger of spoken sentences: each sentence
becomes the assistant transcript when its audio starts, and on barge-in the
part of the current sentence the caller heard is offered to the context.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from pipecat.frames.frames import (
    AggregatedTextFrame,
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSTextFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import TransportParams

from voice_gateway.interfaces import SAMPLE_RATE, AudioPort

BYTES_PER_MS = SAMPLE_RATE * 2 // 1000
#: Speaking rate used when a sentence's length is not known yet.
_MS_PER_CHAR = 65.0


def transport_params() -> TransportParams:
    return TransportParams(
        audio_in_enabled=True,
        audio_in_sample_rate=SAMPLE_RATE,
        audio_out_enabled=True,
        audio_out_sample_rate=SAMPLE_RATE,
        audio_out_10ms_chunks=2,
        audio_out_end_silence_secs=0,
    )


class CallInput(BaseInputTransport):
    """Caller audio from the port, 16 kHz PCM16 in 20 ms frames."""

    def __init__(self, port: AudioPort, params: TransportParams, **kwargs):
        super().__init__(params, **kwargs)
        self._port = port
        self._reader: asyncio.Task | None = None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.set_transport_ready(frame)
        if self._reader is None:
            self._reader = self.create_task(self._read(), "call-input")

    async def _read(self):
        async for pcm in self._port.frames():
            await self.push_audio_frame(
                InputAudioRawFrame(audio=pcm, sample_rate=SAMPLE_RATE, num_channels=1)
            )

    async def _stop_reader(self):
        if self._reader is not None:
            await self.cancel_task(self._reader)
            self._reader = None

    async def stop(self, frame):
        await self._stop_reader()
        await super().stop(frame)

    async def cancel(self, frame):
        await self._stop_reader()
        await super().cancel(frame)

    async def cleanup(self):
        await self._stop_reader()
        await super().cleanup()


@dataclass
class _Sentence:
    text: str
    context_id: str | None
    start_ms: float = 0.0


class CallOutput(BaseOutputTransport):
    """The reply to the port, paced to the caller's ear."""

    def __init__(
        self,
        port: AudioPort,
        params: TransportParams,
        *,
        lead_ms: int,
        on_sentence: Callable[[str], None],
        on_first_audio: Callable[[], None],
        sentence_ms: Callable[[str | None], float | None],
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._port = port
        self._lead_ms = lead_ms
        self._on_sentence = on_sentence
        self._on_first_audio = on_first_audio
        self._sentence_ms = sentence_ms
        self._queued_ms = 0.0
        self._announced: deque[_Sentence] = deque()
        self._open: _Sentence | None = None
        self._heard_tail: str | None = None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.set_transport_ready(frame)

    def pending_ms(self) -> float:
        """Audio queued at the port that the caller has not heard yet."""
        return max(0.0, self._queued_ms - self._port.played_ms())

    def audible(self) -> bool:
        return self.pending_ms() > 0

    def take_heard_tail(self) -> str | None:
        """What the caller heard of the sentence a barge-in cut, once."""
        tail, self._heard_tail = self._heard_tail, None
        return tail

    async def write_transport_frame(self, frame: Frame):
        # Text frames arrive here in order with the audio: a sentence is
        # announced before its audio and its TTSTextFrame follows its audio.
        if isinstance(frame, TTSTextFrame):
            self._open = None
        elif isinstance(frame, AggregatedTextFrame) and frame.will_be_spoken:
            self._announced.append(_Sentence(frame.text.strip(), frame.context_id))

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        if not self.audible() and not self._open:
            self._on_first_audio()
        if self._open is None and self._announced:
            self._open = self._announced.popleft()
            self._open.start_ms = self._queued_ms
            if self._open.text:
                self._on_sentence(self._open.text)
        await self._port.play(frame.audio)
        self._queued_ms += len(frame.audio) / BYTES_PER_MS
        while self._queued_ms - self._port.played_ms() > self._lead_ms:
            await asyncio.sleep(0.01)
        return True

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, InterruptionFrame):
            # Before the frame travels on: the assistant aggregator reads the
            # heard tail when it handles the same interruption.
            self._heard_tail = self._cut()
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            await self._port.flush()
            self._queued_ms = float(self._port.played_ms())

    def _cut(self) -> str | None:
        sentence, self._open = self._open, None
        self._announced.clear()
        if sentence is None or not self.audible():
            return None
        heard = self._port.played_ms() - sentence.start_ms
        total = (
            self._sentence_ms(sentence.context_id) or len(sentence.text) * _MS_PER_CHAR
        )
        return heard_prefix(sentence.text, heard / total if total > 0 else 0.0)


def heard_prefix(text: str, fraction: float) -> str | None:
    """The words of ``text`` a listener heard after ``fraction`` of its audio,
    weighting words by length; a word counts once its middle was played."""
    words = text.split()
    if not words or fraction <= 0:
        return None
    total = sum(len(w) + 1 for w in words)
    heard, position = [], 0.0
    for word in words:
        middle = (position + (len(word) + 1) / 2) / total
        if middle > fraction:
            break
        heard.append(word)
        position += len(word) + 1
    return " ".join(heard) or None
