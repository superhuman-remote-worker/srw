"""One call's cascade on Pipecat; knowledge-base/knowledge/features/local_voice_gateway.md §4.1, §4.3.

Pipeline: port → STT → caller transcripts → user turn (Silero VAD, barge-in
start, Smart Turn v3 stop) → voice LLM (delegate tool) → TTS → port →
assistant context. How the protocol's appends map onto it:

- ``instructions`` with quoted exact words (the greeting) are spoken as they
  are; other instructions become a note the LLM acts on now.
- ``commentary`` with an open delegation's id is that ``delegate`` call's
  result, which the LLM phrases for the caller; any other ``commentary`` is
  a note to say now in its own words.
- ``thinking`` is a silent note.

A ``delegate`` call emits ``session.delegation.created`` (after the caller's
transcript, which STT emitted first) and plays the filler unless the LLM
already said it was checking. Pipecat runs the call as an async tool, so
the conversation goes on until the answer arrives.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

import aiohttp
from loguru import logger as pipecat_logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.turn.smart_turn.base_smart_turn import SmartTurnParams
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import (
    LocalSmartTurnAnalyzerV3,
)
from pipecat.frames.frames import Frame, LLMMessagesAppendFrame, TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMAssistantAggregatorParams,
    LLMUserAggregator,
    LLMUserAggregatorParams,
)
from pipecat.services.llm_service import FunctionCallParams
from pipecat.turns.user_stop import (
    SpeechTimeoutUserTurnStopStrategy,
    TurnAnalyzerUserTurnStopStrategy,
)
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.workers.runner import WorkerRunner

from voice_gateway.engines.cascade.prompt import (
    DELEGATE_TOOL,
    delegate_schema,
    exact_words,
    note,
    system_prompt,
)
from voice_gateway.engines.cascade.services import (
    SpeechTTS,
    TranscriptionSTT,
    VoiceLLM,
)
from voice_gateway.engines.cascade.settings import CascadeSettings
from voice_gateway.engines.cascade.timings import Timings
from voice_gateway.engines.cascade.transport import (
    CallInput,
    CallOutput,
    transport_params,
)
from voice_gateway.engines.cascade.turns import (
    BargeInStart,
    CallerTranscripts,
    CallVAD,
    HeardAssistant,
)
from voice_gateway.interfaces import SAMPLE_RATE
from voice_gateway.session import ProtocolError, Session

# Session ids and counts only, like the rest of the gateway.
logger = logging.getLogger(__name__)
_quiet = False


def quiet_pipecat() -> None:
    """Pipecat logs transcripts and prompts at DEBUG; the gateway logs no
    content. ``VOICE_GATEWAY_PIPECAT_LOG=DEBUG`` brings them back locally."""
    global _quiet
    if not _quiet:
        _quiet = True
        pipecat_logger.remove()
        pipecat_logger.add(
            sys.stderr, level=os.environ.get("VOICE_GATEWAY_PIPECAT_LOG", "ERROR")
        )


class CascadeEngine:
    """The Engine port for one call. ``vad`` and ``turn_analyzer`` replace
    Silero and Smart Turn (tests feed synthetic audio)."""

    def __init__(self, settings: CascadeSettings, *, vad=None, turn_analyzer=None):
        quiet_pipecat()
        self.settings = settings
        self.timings = Timings()
        self._vad = vad
        self._turn_analyzer = turn_analyzer
        self._session: Session | None = None
        self._http: aiohttp.ClientSession | None = None
        self._worker: PipelineWorker | None = None
        self._runner: WorkerRunner | None = None
        self._task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self._early: list[Frame] = []
        self._pending: dict[str, object] = {}
        self._llm: VoiceLLM | None = None
        self._audioless = False
        self._stopped = False
        self._prewarm: asyncio.Task | None = None
        self.output: CallOutput | None = None
        self.context: LLMContext | None = None

    # -- Engine port ------------------------------------------------------

    async def start(self, session: Session, accept_body: dict) -> None:
        self._session = session
        if session.audio is None:
            # A call without media (the scripted source): speak appends as
            # transcript, like the scripted engine.
            self._audioless = True
            logger.warning(
                "call %s has no audio; cascade runs without media", session.id
            )
            return
        config = accept_body.get("session") or {}
        self._task = asyncio.create_task(self._run(config))

    async def append(
        self, session: Session, kind: str, content: str, delegation_id: str | None
    ) -> None:
        if self._audioless:
            if kind != "thinking":
                self._say("assistant", content)
            return
        if kind == "commentary" and delegation_id in self._pending:
            answer = self._pending.pop(delegation_id)
            await answer(content)
            return
        if kind == "instructions" and (words := exact_words(content)):
            await self._queue(TTSSpeakFrame(words))
            return
        message = {"role": "developer", "content": note(kind, content)}
        await self._queue(
            LLMMessagesAppendFrame(messages=[message], run_llm=kind != "thinking")
        )

    async def stop(self, session: Session) -> None:
        self._stopped = True
        self._pending.clear()
        if self._prewarm is not None:
            self._prewarm.cancel()
        if self._runner is not None:
            await self._runner.cancel()
        elif self._task is not None:
            self._task.cancel()  # still building the pipeline
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), 5)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            except Exception:  # noqa: BLE001 - a failed pipeline must not block the close
                logger.exception("call %s cascade ended with an error", session.id)
        if self._http is not None:
            await self._http.close()
        turns = [t.as_dict() for t in self.timings.turns]
        logger.info("call %s cascade turns: %s", session.id, turns)

    # -- pipeline ---------------------------------------------------------

    async def _run(self, config: dict) -> None:
        session = self._session
        try:
            pipeline = await self._build(config)
            if self._stopped:
                return
            self._worker = PipelineWorker(
                pipeline,
                params=PipelineParams(
                    audio_in_sample_rate=SAMPLE_RATE, audio_out_sample_rate=SAMPLE_RATE
                ),
                cancel_on_idle_timeout=False,
                idle_timeout_secs=None,
                enable_rtvi=False,
                enable_turn_tracking=False,
                check_dangling_tasks=False,
            )
            self._runner = WorkerRunner(handle_sigint=False, check_dangling_tasks=False)
            await self._runner.add_workers(self._worker)

            @self._worker.event_handler("on_pipeline_started")
            async def _started(worker, frame):
                for early in self._early:
                    await worker.queue_frame(early)
                self._early.clear()
                self._ready.set()
                if self.settings.llm.prewarm:
                    prefix = LLMContext(tools=self.context.tools)
                    self._prewarm = asyncio.create_task(self._llm.prewarm(prefix))

            await self._runner.run()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("call %s cascade failed", session.id)

    async def _build(self, config: dict) -> Pipeline:
        settings, audio = self.settings, self._session.audio
        voice_name = ((config.get("audio") or {}).get("output") or {}).get(
            "voice"
        ) or ""
        voice = settings.tts.voices.get(voice_name, settings.tts.voice)
        system = system_prompt(
            config.get("instructions") or "", settings.language, settings.prompt
        )
        # The ONNX sessions take a moment to load; keep it off the event loop.
        vad = self._vad or await asyncio.to_thread(CallVAD, settings.turn)
        if not settings.turn.smart_turn:
            stop = SpeechTimeoutUserTurnStopStrategy(
                user_speech_timeout=settings.turn.speech_timeout_secs
            )
        else:
            analyzer = self._turn_analyzer or await asyncio.to_thread(
                LocalSmartTurnAnalyzerV3,
                params=SmartTurnParams(stop_secs=settings.turn.smart_turn_stop_secs),
            )
            stop = TurnAnalyzerUserTurnStopStrategy(turn_analyzer=analyzer)
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60, sock_connect=5, sock_read=20)
        )
        params = transport_params()
        tts = SpeechTTS(
            settings.tts,
            voice=voice,
            language=settings.language,
            http=self._http,
            timings=self.timings,
        )
        self.output = output = CallOutput(
            audio,
            params,
            lead_ms=settings.playout_lead_ms,
            on_sentence=lambda text: self._say("assistant", text),
            on_first_audio=self.timings.audio_out,
            sentence_ms=tts.sentence_ms,
        )
        stt = TranscriptionSTT(
            settings.stt,
            language=settings.language,
            http=self._http,
            timings=self.timings,
        )
        caller = CallerTranscripts(
            barge_in=settings.barge_in,
            voice_audible=output.audible,
            emit=lambda text: self._say("caller", text),
            timings=self.timings,
        )
        self.context = context = LLMContext(
            tools=ToolsSchema(standard_tools=[FunctionSchema(**delegate_schema())])
        )
        user = LLMUserAggregator(
            context,
            params=LLMUserAggregatorParams(
                vad_analyzer=vad,
                user_turn_strategies=UserTurnStrategies(
                    start=[
                        BargeInStart(
                            barge_in=settings.barge_in, voice_audible=output.audible
                        )
                    ],
                    stop=[stop],
                ),
            ),
            _realtime_service_mode=False,
        )

        @user.event_handler("on_user_turn_stopped")
        async def _turn_stopped(aggregator, strategy, message):
            self.timings.turn_released()

        assistant = HeardAssistant(
            context,
            params=LLMAssistantAggregatorParams(),
            _realtime_service_mode=False,
            _paired_user_aggregator=user,
            heard_tail=output.take_heard_tail,
        )
        self._llm = llm = VoiceLLM(settings.llm, system=system, timings=self.timings)
        llm.register_function(
            DELEGATE_TOOL, self._delegate, cancel_on_interruption=False
        )
        return Pipeline(
            [CallInput(audio, params), stt, caller, user, llm, tts, output, assistant]
        )

    async def _queue(self, frame: Frame) -> None:
        if self._ready.is_set() and self._worker is not None:
            await self._worker.queue_frame(frame)
        else:
            self._early.append(frame)

    async def _delegate(self, params: FunctionCallParams) -> None:
        try:
            delegation_id = self._session.delegate()
        except ProtocolError:
            # No caller words since the last delegation: the back office
            # already has this request.
            await params.result_callback(
                "Not handed over again: the back office already has this request "
                "and its answer will follow."
            )
            return
        self.timings.delegated()
        self._pending[delegation_id] = params.result_callback
        if self.settings.filler and not (self._llm and self._llm.spoke):
            await params.llm.push_frame(TTSSpeakFrame(self.settings.filler))
        logger.info("call %s delegated", self._session.id)

    def _say(self, speaker: str, text: str) -> None:
        try:
            self._session.transcript(speaker, text)
        except ProtocolError:
            pass  # the call ended while the pipeline drained
