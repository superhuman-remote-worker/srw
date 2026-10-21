"""Per-turn timings of a cascade call; knowledge-base/knowledge/features/local_voice_gateway.md §5 (A4).

One record per caller turn: when the caller stopped speaking, how long STT,
the voice LLM's first token and the TTS's first audio took, and when the
reply's first audio reached the call. Times only, never content, so the
records may be logged.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass


@dataclass
class TurnTiming:
    speech_end: float
    stt_ms: float | None = None
    llm_first_token_ms: float | None = None
    tts_first_audio_ms: float | None = None
    #: End of the caller's speech to the reply's first audio at the call.
    first_audio_ms: float | None = None
    delegated: bool = False

    def as_dict(self) -> dict:
        data = asdict(self)
        data.pop("speech_end")
        return data


class Timings:
    """Filled by the pipeline's services; read by the harness and the logs."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.turns: list[TurnTiming] = []
        self._llm_started: float | None = None

    @property
    def current(self) -> TurnTiming | None:
        return self.turns[-1] if self.turns else None

    def speech_ended(self, at: float) -> None:
        """The caller stopped speaking; a stretch that continues a turn which
        has no reply yet moves that turn's end instead of opening a new one."""
        turn = self.current
        if (
            turn is not None
            and turn.first_audio_ms is None
            and turn.llm_first_token_ms is None
        ):
            turn.speech_end = at
            return
        self.turns.append(TurnTiming(speech_end=at))

    def stt(self, ms: float) -> None:
        if self.current is not None:
            self.current.stt_ms = round(ms)

    def llm_started(self) -> None:
        self._llm_started = self.clock()

    def llm_first_token(self) -> None:
        turn = self.current
        if self._llm_started is None:
            return
        ms = (self.clock() - self._llm_started) * 1000
        self._llm_started = None
        if (
            turn is not None
            and turn.llm_first_token_ms is None
            and turn.first_audio_ms is None
        ):
            turn.llm_first_token_ms = round(ms)

    def tts_first_audio(self, ms: float) -> None:
        turn = self.current
        if (
            turn is not None
            and turn.tts_first_audio_ms is None
            and turn.first_audio_ms is None
        ):
            turn.tts_first_audio_ms = round(ms)

    def delegated(self) -> None:
        if self.current is not None:
            self.current.delegated = True

    def audio_out(self) -> None:
        turn = self.current
        if turn is not None and turn.first_audio_ms is None:
            turn.first_audio_ms = round((self.clock() - turn.speech_end) * 1000)
