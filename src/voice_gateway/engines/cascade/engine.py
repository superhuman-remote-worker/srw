"""One call's cascade on Pipecat; knowledge-base/knowledge/features/local_voice_gateway.md §4.1, §4.3.

Pipeline: port → STT → caller transcripts → user turn (Silero VAD, barge-in
start, Smart Turn v3 stop) → voice LLM (delegate tool) → TTS → port →
assistant context. How the protocol's appends map onto it:

- ``instructions`` with quoted exact words (the greeting) are spoken as they
  are, sentence by sentence; while a request is open they are the back
  office's line about it and take the next hold sentence's place. Other
  instructions become a note the LLM acts on now.
- ``commentary`` with an open delegation's id is that ``delegate`` call's
  result, which the LLM phrases for the caller; any other ``commentary`` is
  a note to say now in its own words.
- ``thinking`` is a silent note.

Whatever the voice says on its own (the exact words, the notes it acts on
now) waits until the line is free: the caller neither speaking nor in a
turn, the voice silent and the LLM idle. It never cuts into the caller or a
sentence. A note that comes in while the caller speaks goes into the context
at once, so the reply to the caller takes it in; the LLM runs for it once
the line is free only if no request has.

A ``delegate`` call emits ``session.delegation.created`` (after the caller's
transcript, which STT emitted first) and plays the filler unless the LLM
already said it was checking, or an earlier request is still open (the
caller has heard a holding sentence for it). Pipecat runs the call as an
async tool, so the conversation goes on until the answer arrives. The filler
enters the LLM's context with the call, where the LLM's own holding sentence
would stand, before the LLM's next request; appended after the call's result
it made the LLM open the answer with a second holding sentence.

Several requests may be open at once: a caller who paused was cut off and
the voice hands on the rest as another request (the prompt's rules). An
answer to an earlier request while a later one is open goes to the LLM with
a note that it may be incomplete; an answer to a later one settles the
earlier ones still open (the back office answers the latest piece with
everything it knows). While a request is open, a caller turn of only
backchannel words ("Okay.", "Ja, gut.") gets no reply: the LLM tended to
invent the open answer for it. When the line has been quiet for
``hold_after_secs`` the voice says the back office's line, if one came, or
one of ``hold_phrases`` (never the same twice in a row), at most
``hold_max`` times, then ``hold_longer`` once. Once the call has had
``hold_max`` holding sentences, the filler and the back office's lines
included, each later one needs ``hold_later_secs`` of quiet: a caller who
has heard them knows the voice is still there.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
import time
from collections import deque
from dataclasses import dataclass

import aiohttp
from loguru import logger as pipecat_logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.turn.smart_turn.base_smart_turn import SmartTurnParams
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import (
    LocalSmartTurnAnalyzerV3,
)
from pipecat.frames.frames import (
    Frame,
    FunctionCallResultProperties,
    LLMMessagesAppendFrame,
    LLMRunFrame,
    TTSSpeakFrame,
)
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

from voice_gateway.engines.cascade.clauses import sentences
from voice_gateway.engines.cascade.prompt import (
    ADDITION_OPEN,
    COVERED,
    DELEGATE_TOOL,
    HOLDING_PHRASES,
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
    is_backchannel,
)
from voice_gateway.interfaces import SAMPLE_RATE
from voice_gateway.session import ProtocolError, Session

# Session ids and counts only, like the rest of the gateway.
logger = logging.getLogger(__name__)

#: How long the line must have been free before the voice speaks on its own.
SETTLE_SECS = 0.3
#: How often the engine looks at the line.
WATCH_SECS = 0.1
#: A holding sentence said this recently covers a new request too.
HELD_RECENTLY_SECS = 10.0


@dataclass
class _Delegation:
    """A ``delegate`` call waiting for the back office's answer."""

    answer: object  # the call's result callback
    tool_call_id: str
    #: The filler the engine spoke for it (not yet in the LLM's context).
    filler: str | None = None


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
        #: Open delegations, oldest first.
        self._pending: dict[str, _Delegation] = {}
        #: What the voice says on its own, waiting for the line to be free.
        self._held: deque[list[Frame]] = deque()
        self._watch: asyncio.Task | None = None
        #: The line: caller speech (VAD), a caller turn in progress, and when
        #: anything last happened on it.
        self._caller_speaking = False
        self._turn_open = False
        self._last_busy = float("-inf")
        #: Hold sentences in the current wait and holding sentences in the
        #: call; the last hold sentence said.
        self._holds = 0
        self._call_holds = 0
        #: The back office's line about the open request, for the next hold.
        self._status: str | None = None
        #: A note went into the context while the caller spoke and no LLM
        #: request has taken it in yet.
        self._owed = False
        self._longer_said = False
        self._last_hold: str | None = None
        self._random = random.Random()
        #: When the voice last started a sentence, and the sentence.
        self._last_said: tuple[float, str] | None = None
        self._llm: VoiceLLM | None = None
        self._audioless = False
        self._stopped = False
        self._prewarm: asyncio.Task | None = None
        self._tts: SpeechTTS | None = None
        #: Set once the voice first played audio (the greeting).
        self._greeted = asyncio.Event()
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
            await self._answered(delegation_id, content)
            return
        if kind == "instructions" and (words := exact_words(content)):
            if self._pending and self.settings.hold_after_secs:
                # Said at once, it followed the filler: two holding
                # sentences in a row (owner 2026-10-23).
                self._status = words
                return
            # One TTS request per sentence: a TTS that answers once a
            # request is synthesised starts with the first sentence.
            await self._say_when_free(
                [TTSSpeakFrame(sentence) for sentence in sentences(words)]
            )
            return
        message = {"role": "developer", "content": note(kind, content)}
        silent = kind == "thinking"
        if not self._held and (silent or self._caller_speaking or self._turn_open):
            # Held until the caller's turn was over, a late answer came
            # after the reply to it, which had guessed the answer: the
            # caller heard it twice (owner 2026-10-23).
            await self._queue(LLMMessagesAppendFrame(messages=[message], run_llm=False))
            self._owed = self._owed or not silent
        else:
            await self._say_when_free(
                [LLMMessagesAppendFrame(messages=[message], run_llm=not silent)]
            )

    async def stop(self, session: Session) -> None:
        self._stopped = True
        self._pending.clear()
        self._held.clear()
        if self._prewarm is not None:
            self._prewarm.cancel()
        if self._watch is not None:
            self._watch.cancel()
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
                self._watch = asyncio.create_task(self._watch_line())
                if self.settings.llm.prewarm:
                    prefix = LLMContext(tools=self.context.tools)
                    self._prewarm = asyncio.create_task(self._prewarm_all(prefix))

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
        self._tts = tts = SpeechTTS(
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
            on_first_audio=self._audio_out,
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
            on_speech=self._caller_speech,
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

        @user.event_handler("on_user_turn_started")
        async def _turn_started(aggregator, strategy):
            self._turn_open = True
            self._touch()

        @user.event_handler("on_user_turn_stopped")
        async def _turn_stopped(aggregator, strategy, message):
            self._turn_open = False
            self._touch()
            self.timings.turn_released()

        assistant = HeardAssistant(
            context,
            params=LLMAssistantAggregatorParams(),
            _realtime_service_mode=False,
            _paired_user_aggregator=user,
            heard_tail=output.take_heard_tail,
        )
        self._llm = llm = VoiceLLM(
            settings.llm, system=system, timings=self.timings, prepare=self._prepare
        )
        llm.register_function(
            DELEGATE_TOOL, self._delegate, cancel_on_interruption=False
        )
        return Pipeline(
            [CallInput(audio, params), stt, caller, user, llm, tts, output, assistant]
        )

    async def _prewarm_all(self, prefix: LLMContext) -> None:
        """The LLM's fixed prefix, then, once the greeting has been played
        (a TTS server with one slot would make it wait), the audio of the
        filler and the holding phrases that are not cached yet."""
        await self._llm.prewarm(prefix)
        if self._tts is None:
            return
        phrases = [self.settings.filler] if self.settings.filler else []
        holding = HOLDING_PHRASES.get(self.settings.language, ())
        phrases += [p for p in holding if p not in phrases]
        if not self._tts.uncached(phrases):
            return
        try:
            await asyncio.wait_for(self._greeted.wait(), timeout=5)
        except TimeoutError:
            pass
        for _ in range(150):  # the greeting's rest, at most 15 s
            if self.output is None or not self.output.audible():
                break
            await asyncio.sleep(0.1)
        await self._tts.prewarm(phrases)

    def _audio_out(self) -> None:
        self._greeted.set()
        self.timings.audio_out()

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
        self._touch()
        pending = _Delegation(params.result_callback, params.tool_call_id)
        # An addition to an open request: the caller has heard a holding
        # sentence for that one already, or has just heard one (a reply the
        # caller cut before its call went out).
        heard = bool(self._pending) or self._held_recently()
        self._pending[delegation_id] = pending
        if not heard and self._llm and self._llm.spoke:
            self._call_holds += 1  # the LLM's own holding sentence
        elif self.settings.filler and not heard:
            self._call_holds += 1
            pending.filler = self.settings.filler
            await params.llm.push_frame(
                TTSSpeakFrame(self.settings.filler, append_to_context=False)
            )
        logger.info("call %s delegated", self._session.id)

    async def _answered(self, delegation_id: str, content: str) -> None:
        """The back office's answer to one open delegation.

        Earlier requests still open are settled first, quietly: the back
        office answers the latest piece with everything it knows. While a
        later request is open, the LLM is told this answer may be
        incomplete."""
        order = list(self._pending)
        at = order.index(delegation_id)
        for older in order[:at]:
            await self._settle(older, COVERED, run_llm=False)
        later = order[at + 1 :]
        self._hold_reset()
        self._touch()
        await self._settle(
            delegation_id, f"{content}\n{ADDITION_OPEN}" if later else content
        )

    async def _settle(
        self, delegation_id: str, result: str, *, run_llm: bool | None = None
    ) -> None:
        pending = self._pending.pop(delegation_id)
        if pending.filler:
            self._said_before_call(pending.tool_call_id, pending.filler)
            pending.filler = None
        properties = (
            None if run_llm is None else FunctionCallResultProperties(run_llm=run_llm)
        )
        await pending.answer(result, properties=properties)

    def _prepare(self, context: LLMContext) -> bool:
        """Before each LLM request: the fillers go in with their calls, and
        a caller's mere backchannel while a request is open gets no reply."""
        for pending in self._pending.values():
            if pending.filler and self._said_before_call(
                pending.tool_call_id, pending.filler
            ):
                pending.filler = None
        messages = context.messages
        last = messages[-1] if messages else None
        before = messages[-2] if len(messages) > 1 else None
        run = not (
            self._pending
            and isinstance(last, dict)
            and last.get("role") == "user"
            and isinstance(last.get("content"), str)
            and is_backchannel(last["content"], self.settings.barge_in.backchannels)
            # A note or an answer that came in meanwhile is still owed.
            and not (isinstance(before, dict) and before.get("role") == "developer")
        )
        if run:
            self._owed = False
        return run

    def _said_before_call(self, tool_call_id: str, text: str) -> bool:
        """Put ``text`` into the context in front of the tool call, as an
        assistant message, where the LLM's own words would stand (the LLM's
        request makes them one message, ``services.note_messages``)."""
        if self.context is None:
            return False
        messages = list(self.context.messages)
        for index, message in enumerate(messages):
            calls = message.get("tool_calls") if isinstance(message, dict) else None
            if calls and any(call.get("id") == tool_call_id for call in calls):
                messages.insert(index, {"role": "assistant", "content": text})
                self.context.set_messages(messages)
                return True
        return False

    # -- the line ----------------------------------------------------------

    def _touch(self) -> None:
        """Something happened on the line: the quiet starts again."""
        self._last_busy = time.monotonic()

    def _caller_speech(self, speaking: bool) -> None:
        self._caller_speaking = speaking
        self._touch()

    def _line_busy(self) -> bool:
        return (
            self._caller_speaking
            or self._turn_open
            or bool(self._llm and self._llm.generating)
            or bool(self.output and self.output.speaking())
        )

    def _line_free(self) -> bool:
        if self._line_busy():
            self._touch()
            return False
        return time.monotonic() - self._last_busy >= SETTLE_SECS

    async def _say_when_free(self, frames: list[Frame]) -> None:
        if not self._held and self._line_free():
            await self._release(frames)
        else:
            self._held.append(frames)

    async def _release(self, frames: list[Frame]) -> None:
        self._touch()
        for frame in frames:
            await self._queue(frame)

    async def _watch_line(self) -> None:
        """Release held speech when the line is free; bridge a long wait."""
        try:
            while not self._stopped:
                await asyncio.sleep(WATCH_SECS)
                await self._look_at_the_line()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the call goes on without it
            logger.warning(
                "call %s: the line watch stopped (%s)",
                self._session.id,
                type(exc).__name__,
            )

    async def _look_at_the_line(self) -> None:
        if not self._line_free():
            return
        if self._held:
            await self._release(self._held.popleft())
            return
        if self._owed:
            self._owed = False
            await self._release([LLMRunFrame()])
            return
        settings = self.settings
        after = settings.hold_after_secs
        if self._call_holds >= settings.hold_max:
            after = after and max(after, settings.hold_later_secs)
        if not self._pending:
            self._hold_reset()
        elif after and time.monotonic() - self._last_busy >= after:
            phrase, self._status = self._status, None
            if phrase:
                self._holds += 1
            else:
                phrase = self._hold_phrase()
            if phrase:
                self._call_holds += 1
                await self._release(
                    [
                        TTSSpeakFrame(sentence, append_to_context=False)
                        for sentence in sentences(phrase)
                    ]
                )

    def _hold_phrase(self) -> str | None:
        """The next hold sentence of this wait, if any is left."""
        settings = self.settings
        if self._holds < settings.hold_max:
            fresh = [p for p in settings.hold_phrases if p != self._last_hold]
            if fresh:
                self._holds += 1
                self._last_hold = self._random.choice(fresh)
                return self._last_hold
        if not self._longer_said:
            self._longer_said = True
            return settings.hold_longer or None
        return None

    def _hold_reset(self) -> None:
        self._holds = 0
        self._longer_said = False
        self._status = None

    def _held_recently(self) -> bool:
        """Whether the voice's last sentence, started a moment ago, was a
        holding sentence (the filler or one the rules prescribe)."""
        if self._last_said is None:
            return False
        at, text = self._last_said
        if time.monotonic() - at > HELD_RECENTLY_SECS:
            return False
        holding = (
            self.settings.filler,
            *HOLDING_PHRASES.get(self.settings.language, ()),
        )
        return _plain(text) in {_plain(p) for p in holding if p}

    def _say(self, speaker: str, text: str) -> None:
        if speaker == "assistant":
            self._last_said = (time.monotonic(), text)
        try:
            self._session.transcript(speaker, text)
        except ProtocolError:
            pass  # the call ended while the pipeline drained


def _plain(text: str) -> str:
    return " ".join(text.lower().replace(",", " ").replace(".", " ").split())
