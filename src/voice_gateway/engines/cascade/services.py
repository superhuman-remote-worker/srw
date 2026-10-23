"""The three OpenAI-compatible stages as Pipecat services; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

- ``TranscriptionSTT``: one ``/audio/transcriptions`` request per stretch of
  caller speech (Pipecat segments it by VAD), JSON response.
- ``VoiceLLM``: Pipecat's OpenAI chat service with the profile's
  ``extra_body`` (e.g. thinking off) and note-role mapping.
- ``SpeechTTS``: one streamed ``/audio/speech`` request per sentence, PCM or
  WAV at any rate, resampled to 16 kHz. ``SpokenTextAggregator`` cuts the
  reply into those sentences; a long first sentence starts at a clause.
  Short texts that recur (the filler, the greeting, the holding phrases the
  rules prescribe) are kept in ``PHRASES``, in memory, and replayed at once.

Nothing here logs what was said; errors carry status codes only.
"""

from __future__ import annotations

import json
import re
import struct
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, AsyncIterator, Callable

import aiohttp
from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
)
from pipecat.processors.aggregators import async_tool_messages
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.settings import STTSettings, TTSSettings
from pipecat.services.stt_service import SegmentedSTTService
from pipecat.services.tts_service import TTSService
from pipecat.utils.text.base_text_aggregator import Aggregation, AggregationType
from pipecat.utils.text.simple_text_aggregator import (
    SimpleTextAggregator,
    _LookaheadState,
)
from pipecat.utils.time import time_now_iso8601

from voice_gateway.engines.cascade.clauses import clause_end
from voice_gateway.engines.cascade.prompt import HANDED_ON
from voice_gateway.engines.cascade.settings import LlmSettings, SttSettings, TtsSettings
from voice_gateway.engines.cascade.timings import Timings
from voice_gateway.interfaces import SAMPLE_RATE

#: Whisper's best-known German inventions on silence and noise. A stretch
#: that transcribes to exactly one of these was not speech.
WHISPER_PHANTOMS = frozenset(
    {
        "untertitel im auftrag des zdf, 2017",
        "untertitel im auftrag des zdf, 2018",
        "untertitel im auftrag des zdf, 2020",
        "untertitel im auftrag des zdf für funk, 2017",
        "untertitel der amara.org-community",
        "untertitelung aufgrund der amara.org-community",
        "untertitelung des zdf, 2020",
        "vielen dank fürs zuschauen",
        "vielen dank für's zuschauen",
        "bis zum nächsten mal",
        "copyright wdr 2021",
    }
)


def _headers(key: str | None) -> dict:
    return {"Authorization": f"Bearer {key}"} if key else {}


class TranscriptionSTT(SegmentedSTTService):
    """Per-stretch transcription over ``POST {base_url}/audio/transcriptions``."""

    def __init__(
        self,
        settings: SttSettings,
        *,
        language: str,
        http: aiohttp.ClientSession,
        timings: Timings,
        **kwargs,
    ):
        super().__init__(
            sample_rate=SAMPLE_RATE,
            ttfs_p99_latency=1.5,
            settings=STTSettings(model=settings.model, language=None),
            **kwargs,
        )
        self._stage = settings
        self._language = language
        self._http = http
        self._timings = timings

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        form = aiohttp.FormData()
        form.add_field("file", audio, filename="speech.wav", content_type="audio/wav")
        form.add_field("model", self._stage.model)
        form.add_field("language", self._language)
        form.add_field("response_format", "json")
        form.add_field("temperature", "0")
        if self._stage.prompt:
            form.add_field("prompt", self._stage.prompt)
        for key, value in self._stage.extra_body.items():
            form.add_field(key, str(value))
        started = time.monotonic()
        try:
            async with self._http.post(
                self._stage.base_url + "/audio/transcriptions",
                data=form,
                headers=_headers(self._stage.api_key),
            ) as response:
                if response.status != 200:
                    yield ErrorFrame(error=f"STT answered {response.status}")
                    return
                body = await response.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            yield ErrorFrame(error=f"STT request failed: {type(exc).__name__}")
            return
        self._timings.stt((time.monotonic() - started) * 1000)
        text = body.get("text", "") if isinstance(body, dict) else ""
        text = cut_loop(" ".join(str(text).split()))
        if not text or text.lower().rstrip(".!") in WHISPER_PHANTOMS:
            return
        yield TranscriptionFrame(text, "", time_now_iso8601())


#: Digits and number words. A phone number or a postcode says "fünf fünf
#: fünf", so a phrase of only these counts as a loop from this many repeats.
NUMBER_LOOP = 6
_NUMBER_WORDS = frozenset(
    "null eins ein zwei zwo drei vier fünf sechs sieben acht neun "
    "zero oh one two three four five six seven eight nine".split()
)


def _number(word: str) -> bool:
    return word.isdigit() or word in _NUMBER_WORDS


def cut_loop(text: str, longest: int = 6) -> str:
    """Whisper sometimes repeats a word or phrase until the segment ends
    ("das das das …"); keep the text up to the first repeat of a phrase
    said three times in a row (digits and number words: ``NUMBER_LOOP``
    times, the test set's phone numbers lost their "fünf fünf fünf")."""
    words = text.split()
    key = [w.lower().strip(",.!?;:") for w in words]
    for start in range(len(words)):
        for size in range(1, longest + 1):
            phrase = key[start : start + size]
            times = NUMBER_LOOP if all(map(_number, phrase)) else 3
            if start + times * size > len(words):
                continue
            if all(
                key[start + n * size : start + (n + 1) * size] == phrase
                for n in range(1, times)
            ):
                return " ".join(words[: start + size])
    return text


#: Gemma 4's special-token markup ("<|channel>thought<channel|>"), which now
#: and then lands in the content although thinking is off; spoken, it would
#: read "channel thought". A thought marker takes the word after it along.
_MARKUP = re.compile(r"<\|[a-z_]+>(?:thought)?|<[a-z_]+\|>")
_MARKUP_START = re.compile(r"<\|?[a-z_]*\|?$")


def strip_markup(text: str) -> tuple[str, str]:
    """``text`` without special-token markup, and a tail held back because
    it may be the start of a marker the next chunk completes."""
    text = _MARKUP.sub("", text)
    held = _MARKUP_START.search(text)
    if held is None:
        return text, ""
    return text[: held.start()], text[held.start() :]


#: Markup written since the reply's last speakable text after which the
#: reply counts as a loop and is cut off (Gemma 4 sometimes repeats empty
#: thought markers up to ``max_tokens`` when it would rather think, which
#: keeps the LLM busy for seconds and the caller in silence).
MARKUP_LOOP_CHARS = 200


class _MarkupLoop(Exception):
    """A reply that only repeats special-token markup."""


class VoiceLLM(OpenAILLMService):
    """Streaming chat completions with the delegate tool, thinking off.

    ``prepare`` sees the context before each request; returning False skips
    the request (the engine's bookkeeping, see ``engine.CascadeEngine``)."""

    def __init__(
        self,
        settings: LlmSettings,
        *,
        system: str,
        timings: Timings,
        prepare: Callable[[object], bool] | None = None,
        **kwargs,
    ):
        super().__init__(
            api_key=settings.api_key or "none",
            base_url=settings.base_url,
            settings=OpenAILLMService.Settings(
                model=settings.model,
                system_instruction=system,
                temperature=settings.temperature,
                max_tokens=settings.max_tokens,
            ),
            **kwargs,
        )
        self._stage = settings
        self._timings = timings
        self._prepare = prepare
        self._first_token = False
        #: Whether the response in flight said anything before a tool call.
        self.spoke = False
        #: Whether a request is in flight.
        self.generating = False
        self._held = ""
        self._markup = 0

    async def prewarm(self, context) -> None:
        """One 1-token completion over the call's fixed prefix; the server's
        prompt cache then holds it for the first real turn."""
        self._compose_system_instruction()
        params = self.get_llm_adapter().get_llm_invocation_params(
            context,
            system_instruction=self._settings.system_instruction,
            convert_developer_to_user=not self.supports_developer_role,
        )
        params = self.build_chat_completion_params(params)
        params.pop("stream_options", None)
        params.pop("max_completion_tokens", None)
        params.update(stream=False, max_tokens=1)
        try:
            await self._client.chat.completions.create(**params)
        except Exception:  # noqa: BLE001 - a cold cache only costs latency
            pass

    def build_chat_completion_params(self, params_from_context) -> dict:
        params = super().build_chat_completion_params(params_from_context)
        params["messages"] = note_messages(params["messages"], self._stage.note_role)
        if self._stage.extra_body:
            params["extra_body"] = {
                **params.get("extra_body", {}),
                **self._stage.extra_body,
            }
        return params

    async def _process_context(self, context):
        self.spoke = False
        self._first_token = False
        self._held = ""  # a lone "<" at a reply's end is no speech either
        self._markup = 0
        if self._prepare is not None and not self._prepare(context):
            return
        self.generating = True
        self._timings.llm_started()
        try:
            await super()._process_context(context)
        except _MarkupLoop:
            pass  # what was speakable went out already
        finally:
            self.generating = False

    def _token(self):
        if not self._first_token:
            self._first_token = True
            self._timings.llm_first_token()

    async def _push_llm_text(self, text: str):
        self._token()
        raw = self._held + text
        text, self._held = strip_markup(raw)
        self._markup += len(raw) - len(text) - len(self._held)
        if text.strip():
            self._markup = 0
        elif self._markup > MARKUP_LOOP_CHARS:
            raise _MarkupLoop
        if not text:
            return
        if text.strip():
            self.spoke = True
        await super()._push_llm_text(text)

    async def run_function_calls(self, function_calls):
        self._token()
        await super().run_function_calls(function_calls)


def note_messages(messages: list, role: str) -> list:
    """Map "developer" notes (Pipecat's and ours) to the profile's note role
    and merge user messages that would end up next to each other; the system
    prompt at the front is never touched, so its cached prefix holds.

    What the voice said with a tool call (its holding sentence, or the
    filler in its place) and the call become one assistant message, the
    shape chat models are trained on. Pipecat keeps them as two; Gemma 4
    then loops on empty thought markers, or repeats the holding sentence
    without calling the tool, when the caller speaks while the call runs.
    Not once the call's result is in place: Gemma 4's template puts a
    message's text after its tool responses and closes the turn, and the
    LLM then reasoned before relaying the answer (8 of 10, 2.3-5.2 s, once
    past ``max_tokens`` with nothing said; owner's call 2026-10-23). Two
    messages it continues its own turn after the response (0 of 10), and
    they stay two for the rest of the call, so the cached prefix holds.
    A call still running says it was handed on and that an addition goes
    out as a new call (``prompt.HANDED_ON``)."""
    out: list = []
    for index, message in enumerate(messages):
        if message.get("role") == "developer" and role != "developer":
            message = {**message, "role": role}
        message = _handed_on(message)
        previous = out[-1] if out else None
        if (
            previous is not None
            and message.get("role") == "assistant"
            and message.get("tool_calls")
            and not message.get("content")
            and previous.get("role") == "assistant"
            and not previous.get("tool_calls")
            and isinstance(previous.get("content"), str)
            and previous["content"].strip()
            and not _answered(messages, index)
        ):
            out[-1] = {**message, "content": previous["content"]}
            continue
        if (
            index > 0
            and previous is not None
            and len(out) > 1
            and message.get("role") == "user"
            and previous.get("role") == "user"
            and isinstance(message.get("content"), str)
            and isinstance(previous.get("content"), str)
        ):
            out[-1] = {
                **previous,
                "content": previous["content"] + "\n\n" + message["content"],
            }
            continue
        out.append(message)
    return out


def _answered(messages: list, index: int) -> bool:
    """Whether the call at ``index`` has its result in place of Pipecat's
    placeholder (a result deferred to a note leaves the placeholder)."""
    for message in messages[index + 1 :]:
        if message.get("role") != "tool":
            return False
        content = message.get("content")
        payload = (
            async_tool_messages.parse_message(message)
            if isinstance(content, str)
            else None
        )
        if payload is None or payload.kind != "started":
            return True
    return False


def _handed_on(message: dict) -> dict:
    """Pipecat's placeholder of a running async call, with SRW's wording."""
    content = message.get("content")
    if message.get("role") != "tool" or not isinstance(content, str):
        return message
    payload = async_tool_messages.parse_message(message)
    if payload is None or payload.kind != "started":
        return message
    body = json.loads(content)
    body["description"] = HANDED_ON
    return {**message, "content": json.dumps(body)}


class _WavStream:
    """Incremental RIFF/WAVE parser: header first, then PCM16 samples."""

    def __init__(self):
        self._buffer = b""
        self.rate: int | None = None
        self._in_data = False

    def feed(self, chunk: bytes) -> bytes:
        if self._in_data:
            return chunk
        self._buffer += chunk
        buf = self._buffer
        if len(buf) < 12:
            return b""
        if buf[:4] != b"RIFF" or buf[8:12] != b"WAVE":
            raise ValueError("not a WAV stream")
        at = 12
        while len(buf) >= at + 8:
            chunk_id, size = (
                buf[at : at + 4],
                struct.unpack("<I", buf[at + 4 : at + 8])[0],
            )
            if chunk_id == b"data":
                self._in_data = True
                self._buffer = b""
                return buf[at + 8 :]
            if len(buf) < at + 8 + size:
                return b""
            if chunk_id == b"fmt ":
                fmt, channels, rate = struct.unpack("<HHI", buf[at + 8 : at + 16])
                bits = struct.unpack("<H", buf[at + 22 : at + 24])[0]
                if fmt not in {1, 0xFFFE} or channels != 1 or bits != 16:
                    raise ValueError("TTS WAV must be mono 16-bit PCM")
                self.rate = rate
            at += 8 + size + (size & 1)
        return b""


#: A number before a period: in German mostly an ordinal ("am 12. März"),
#: which Pipecat's sentence splitter takes for a sentence end.
_NUMBER_END = re.compile(r"(?:^|\s)\d{1,4}$")
#: Stands in for that period while the splitter looks (it is no sentence
#: punctuation); the text goes out with the period back.
_ORDINAL_DOT = "\u2024"


def _spoken(aggregation: Aggregation) -> Aggregation:
    return Aggregation(
        text=aggregation.text.replace(_ORDINAL_DOT, "."), type=aggregation.type
    )


class SpokenTextAggregator(SimpleTextAggregator):
    """Pipecat's sentence aggregator for a German phone voice.

    - A number's period ("am 12. März", "Zimmer 12.") does not end a
      sentence; a reply that ends there still goes out at its end.
    - The first piece of a reply may end at a clause (``clauses.clause_end``)
      once ``clause_chars`` are written, so a long sentence starts sooner.
    """

    def __init__(self, *, clause_chars: int, **kwargs):
        super().__init__(aggregation_type=AggregationType.SENTENCE, **kwargs)
        self._clause_chars = clause_chars
        self._piece_out = False

    async def aggregate(self, text: str) -> AsyncIterator[Aggregation]:
        for char in text:
            if char == "." and _NUMBER_END.search(self._text):
                char = _ORDINAL_DOT
            self._text += char
            result = await self._check_sentence_with_lookahead(char)
            if result is None and not self._piece_out:
                result = self._clause()
            if result is not None:
                self._piece_out = True
                yield _spoken(result)

    def _clause(self) -> Aggregation | None:
        if self._lookahead_state != _LookaheadState.IDLE:
            return None  # a sentence end is still being decided
        end = clause_end(self._text, self._clause_chars)
        if end is None:
            return None
        piece, self._text = self._text[:end], self._text[end:].lstrip(" ")
        return Aggregation(text=piece.strip(" "), type=AggregationType.SENTENCE)

    async def flush(self) -> Aggregation | None:
        self._piece_out = False
        result = await super().flush()
        return None if result is None else _spoken(result)

    async def handle_interruption(self):
        self._piece_out = False
        await super().handle_interruption()

    async def reset(self):
        self._piece_out = False
        await super().reset()


class PhraseCache:
    """Synthesised audio of short texts, shared by the process's calls.

    A TTS that answers only once a request is synthesised (Qwen3-TTS on
    llama.cpp takes about a second for "Einen Moment, ich schaue nach.")
    makes the caller wait for every piece; the filler, the greeting and the
    holding phrases the rules prescribe recur in every call, so the second
    time they play at once. The voice's own speech, in memory only (A5), and
    never the caller's.
    """

    def __init__(self, max_entries: int = 64, max_chars: int = 80):
        self.max_entries = max_entries
        self.max_chars = max_chars
        self._entries: OrderedDict[tuple, bytes] = OrderedDict()

    def key(self, stage: TtsSettings, voice: str, text: str) -> tuple | None:
        if not stage.cache_phrases or len(text) > self.max_chars:
            return None
        extra = tuple(sorted((k, repr(v)) for k, v in stage.extra_body.items()))
        # The LLM's "Einen Moment, ich schaue nach.\n" is the filler too.
        return (
            stage.base_url,
            stage.model,
            voice,
            stage.speed,
            extra,
            " ".join(text.split()),
        )

    def get(self, key: tuple | None) -> bytes | None:
        if key is None or key not in self._entries:
            return None
        self._entries.move_to_end(key)
        return self._entries[key]

    def put(self, key: tuple | None, pcm: bytes) -> None:
        if key is None or not pcm:
            return
        self._entries[key] = pcm
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()


PHRASES = PhraseCache()
#: Replayed audio goes out in 100 ms frames, like a streamed answer.
_REPLAY_BYTES = SAMPLE_RATE * 2 // 10


class SpeechTTS(TTSService):
    """One streamed ``POST {base_url}/audio/speech`` per sentence."""

    def __init__(
        self,
        settings: TtsSettings,
        *,
        voice: str,
        language: str,
        http: aiohttp.ClientSession,
        timings: Timings,
        **kwargs,
    ):
        # One context per sentence, so each sentence's length is known for
        # the barge-in bookkeeping (Pipecat still plays them in order).
        super().__init__(
            sample_rate=SAMPLE_RATE,
            push_start_frame=True,
            push_stop_frames=True,
            reuse_context_id_within_turn=False,
            settings=TTSSettings(model=settings.model, voice=voice, language=None),
            **kwargs,
        )
        self._text_aggregator = SpokenTextAggregator(
            clause_chars=settings.clause_chars, language=language
        )
        self._stage = settings
        self._voice = voice
        self._http = http
        self._timings = timings
        #: Milliseconds of audio per TTS context, for "what was heard" maths.
        self.durations: dict[str, float] = {}
        self._sentence_context: str | None = None

    def sentence_ms(self, context_id: str | None) -> float | None:
        return self.durations.get(context_id) if context_id else None

    async def _push_tts_frames(self, src_frame, *args, **kwargs):
        """One sentence's request, then its context is closed at once.

        Each sentence has its own context, and Pipecat closes only the turn's
        (an HTTP service's sentence contexts end when no audio came for
        ``stop_frame_timeout_s``, 3 s, counted from the last chunk received,
        not played). Without this a sentence shorter than 3 s was followed
        by silence until then before the next one played.
        """
        self._sentence_context = None
        await super()._push_tts_frames(src_frame, *args, **kwargs)
        context_id, self._sentence_context = self._sentence_context, None
        if context_id and self.audio_context_available(context_id):
            if self._push_stop_frames:
                await self.append_to_audio_context(
                    context_id, TTSStoppedFrame(context_id=context_id)
                )
            await self.remove_audio_context(context_id)

    async def run_tts(
        self, text: str, context_id: str
    ) -> AsyncGenerator[Frame | None, None]:
        body = self._body(text)
        self._sentence_context = context_id
        self._timings.tts_requested()
        key = PHRASES.key(self._stage, self._voice, text)
        cached = PHRASES.get(key)
        if cached is not None:
            self._timings.tts_first_audio(0)
            await self.stop_ttfb_metrics()
            for at in range(0, len(cached), _REPLAY_BYTES):
                yield TTSAudioRawFrame(
                    cached[at : at + _REPLAY_BYTES],
                    SAMPLE_RATE,
                    1,
                    context_id=context_id,
                )
            self.durations[context_id] = len(cached) / (SAMPLE_RATE * 2 / 1000)
            return
        started = time.monotonic()
        resampler = create_stream_resampler()
        wav = _WavStream() if self._stage.response_format == "wav" else None
        rate = self._stage.sample_rate
        carry = b""
        produced = 0
        kept = bytearray() if key is not None else None
        try:
            async with self._http.post(
                self._stage.base_url + "/audio/speech",
                json=body,
                headers=_headers(self._stage.api_key),
            ) as response:
                if response.status != 200:
                    yield ErrorFrame(error=f"TTS answered {response.status}")
                    return
                async for chunk in response.content.iter_chunked(4096):
                    if wav is not None:
                        chunk = wav.feed(chunk)
                        rate = wav.rate or rate
                    chunk = carry + chunk
                    usable = len(chunk) - len(chunk) % 2
                    chunk, carry = chunk[:usable], chunk[usable:]
                    if not chunk:
                        continue
                    pcm = await resampler.resample(chunk, rate, SAMPLE_RATE)
                    if not pcm:
                        continue
                    if not produced:
                        self._timings.tts_first_audio(
                            (time.monotonic() - started) * 1000
                        )
                        await self.stop_ttfb_metrics()
                    produced += len(pcm)
                    if kept is not None:
                        kept += pcm
                    yield TTSAudioRawFrame(pcm, SAMPLE_RATE, 1, context_id=context_id)
                tail = await resampler.flush()
                if tail:
                    produced += len(tail)
                    if kept is not None:
                        kept += tail
                    yield TTSAudioRawFrame(tail, SAMPLE_RATE, 1, context_id=context_id)
                # Only a whole answer (a barge-in cancels this generator first).
                if kept is not None:
                    PHRASES.put(key, bytes(kept))
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            yield ErrorFrame(error=f"TTS request failed: {type(exc).__name__}")
        finally:
            self.durations[context_id] = produced / (SAMPLE_RATE * 2 / 1000)

    def uncached(self, texts) -> list[str]:
        """The texts the phrase cache would keep and does not have yet."""
        out = []
        for text in texts:
            key = PHRASES.key(self._stage, self._voice, text)
            if key is not None and PHRASES.get(key) is None:
                out.append(text)
        return out

    async def prewarm(self, texts) -> None:
        """Synthesise phrases into the cache without playing them."""
        for text in texts:
            key = PHRASES.key(self._stage, self._voice, text)
            if key is None or PHRASES.get(key) is not None:
                continue
            resampler = create_stream_resampler()
            wav = _WavStream() if self._stage.response_format == "wav" else None
            rate, carry, kept = self._stage.sample_rate, b"", bytearray()
            try:
                async with self._http.post(
                    self._stage.base_url + "/audio/speech",
                    json=self._body(text),
                    headers=_headers(self._stage.api_key),
                ) as response:
                    if response.status != 200:
                        continue
                    async for chunk in response.content.iter_chunked(4096):
                        if wav is not None:
                            chunk = wav.feed(chunk)
                            rate = wav.rate or rate
                        chunk = carry + chunk
                        usable = len(chunk) - len(chunk) % 2
                        chunk, carry = chunk[:usable], chunk[usable:]
                        if chunk:
                            kept += await resampler.resample(chunk, rate, SAMPLE_RATE)
                    kept += await resampler.flush()
            except (aiohttp.ClientError, TimeoutError, ValueError):
                continue  # a cold phrase only costs latency
            PHRASES.put(key, bytes(kept))

    def _body(self, text: str) -> dict:
        body = {
            "model": self._stage.model,
            "input": text,
            "voice": self._voice,
            "response_format": self._stage.response_format,
            **self._stage.extra_body,
        }
        if self._stage.speed is not None:
            body["speed"] = self._stage.speed
        return body
