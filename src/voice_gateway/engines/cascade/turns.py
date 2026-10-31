"""Turn taking for a phone caller; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

- ``CallVAD``: Pipecat's Silero analyzer, optionally with another ONNX file
  (Silero v6.2 instead of the bundled v6.0).
- ``CallerTranscripts`` sits after STT: it turns each transcription into the
  caller's transcript, or drops it when it was a backchannel ("ja", "mhm")
  or too short while the voice kept talking.
- ``BargeInStart`` is the only turn-start strategy: while the voice is
  silent any speech starts a turn; while it talks, only speech that lasts
  ``min_speech_secs`` or a transcript that survived the filter does, and
  that turn start is the interruption that flushes the port.
- ``HeardAssistant`` adds the part of a cut sentence the caller heard to the
  context, so the LLM's memory matches what was said on the line, and
  delivers a back-office answer as news when the voice has spoken since its
  request.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable

from pipecat.audio.vad.silero import SileroOnnxModel, SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    TranscriptionFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.aggregators.llm_context import LLMSpecificMessage
from pipecat.processors.aggregators.llm_response_universal import LLMAssistantAggregator
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.turns.types import ProcessFrameResult
from pipecat.turns.user_start import BaseUserTurnStartStrategy
from pipecat.utils.string import TextPartForConcatenation

from voice_gateway.engines.cascade.settings import BargeInSettings, TurnSettings
from voice_gateway.engines.cascade.timings import Timings

_WORD = re.compile(r"[\wäöüß]+", re.IGNORECASE)


class CallVAD(SileroVADAnalyzer):
    def __init__(self, turn: TurnSettings):
        super().__init__(
            params=VADParams(
                confidence=turn.vad_confidence,
                start_secs=turn.vad_start_secs,
                stop_secs=turn.vad_stop_secs,
                min_volume=turn.vad_min_volume,
            )
        )
        if turn.vad_model_file:
            self._model = SileroOnnxModel(turn.vad_model_file, force_onnx_cpu=True)


def words(text: str) -> list[str]:
    return _WORD.findall(text.lower())


def is_backchannel(text: str, backchannels: tuple[str, ...]) -> bool:
    """Only backchannel words, e.g. "Ja, genau." or "Mhm."."""
    said = words(text)
    if not said:
        return True
    singles = {w for phrase in backchannels for w in phrase.split()}
    return all(w in singles for w in said)


class CallerTranscripts(FrameProcessor):
    """Caller transcripts out to the session; backchannels over the voice dropped."""

    def __init__(
        self,
        *,
        barge_in: BargeInSettings,
        voice_audible: Callable[[], bool],
        emit: Callable[[str], None],
        timings: Timings,
        on_speech: Callable[[bool], None] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._barge_in = barge_in
        self._voice_audible = voice_audible
        self._emit = emit
        self._timings = timings
        self._on_speech = on_speech or (lambda speaking: None)
        self._over_voice = False

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        # The VAD frames come back upstream from the user aggregator.
        if (
            isinstance(frame, VADUserStartedSpeakingFrame)
            and direction == FrameDirection.UPSTREAM
        ):
            self._over_voice = self._voice_audible()
            self._on_speech(True)
        elif (
            isinstance(frame, VADUserStoppedSpeakingFrame)
            and direction == FrameDirection.UPSTREAM
        ):
            self._on_speech(False)
            self._timings.speech_ended(time.monotonic() - frame.stop_secs)
        elif (
            isinstance(frame, TranscriptionFrame)
            and direction == FrameDirection.DOWNSTREAM
        ):
            if (
                self._over_voice
                and self._voice_audible()
                and self._ignorable(frame.text)
            ):
                return
            self._emit(frame.text)
        await self.push_frame(frame, direction)

    def _ignorable(self, text: str) -> bool:
        return (
            is_backchannel(text, self._barge_in.backchannels)
            or len(words(text)) < self._barge_in.min_words
        )


class BargeInStart(BaseUserTurnStartStrategy):
    """Start a user turn; over the voice only after enough speech."""

    def __init__(
        self,
        *,
        barge_in: BargeInSettings,
        voice_audible: Callable[[], bool],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._min_speech = barge_in.min_speech_secs
        self._voice_audible = voice_audible
        self._speech_since: float | None = None

    async def handle_user_turn_started(self):
        self._speech_since = None

    async def process_frame(self, frame: Frame) -> ProcessFrameResult:
        if isinstance(frame, VADUserStartedSpeakingFrame):
            if not self._voice_audible():
                await self.trigger_user_turn_started()
                return ProcessFrameResult.STOP
            # VAD reports speech start_secs after it began.
            self._speech_since = time.monotonic() - frame.start_secs
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            self._speech_since = None
        elif isinstance(frame, InputAudioRawFrame) and self._speech_since is not None:
            if (
                not self._voice_audible()
                or time.monotonic() - self._speech_since >= self._min_speech
            ):
                await self.trigger_user_turn_started()
                return ProcessFrameResult.STOP
        elif isinstance(frame, TranscriptionFrame):
            # Only transcripts CallerTranscripts let through get here.
            await self.trigger_user_turn_started()
            return ProcessFrameResult.STOP
        return ProcessFrameResult.CONTINUE


class HeardAssistant(LLMAssistantAggregator):
    """The assistant aggregator, plus the heard part of a cut sentence."""

    def __init__(self, *args, heard_tail: Callable[[], str | None], **kwargs):
        super().__init__(*args, **kwargs)
        self._heard_tail = heard_tail

    def _is_deferred(self, in_progress_frame) -> bool:
        """Pipecat settles an async call's result in place of its
        placeholder unless a user or developer message follows it. When the
        voice has spoken since (an earlier piece's answer, a status line from
        the back office), the result in place reads as one the voice already
        answered, and the LLM said nothing more (bench, 2026-10-22): then it
        goes in as news, after what was said."""
        if super()._is_deferred(in_progress_frame):
            return True
        tool_call_id = in_progress_frame.tool_call_id
        after = False
        for message in self._context.messages:
            if isinstance(message, LLMSpecificMessage):
                continue
            if not after:
                after = (
                    message.get("role") == "tool"
                    and message.get("tool_call_id") == tool_call_id
                )
                continue
            content = message.get("content")
            if (
                message.get("role") == "assistant"
                and isinstance(content, str)
                and content.strip()
            ):
                return True
        return False

    async def _handle_interruptions(self, frame: InterruptionFrame):
        tail = self._heard_tail()
        if tail:
            self._aggregation.append(
                TextPartForConcatenation(tail + " …", includes_inter_part_spaces=False)
            )
        await super()._handle_interruptions(frame)
