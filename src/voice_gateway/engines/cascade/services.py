"""The three OpenAI-compatible stages as Pipecat services; knowledge-base/knowledge/features/local_voice_gateway.md §4.3.

- ``TranscriptionSTT``: one ``/audio/transcriptions`` request per stretch of
  caller speech (Pipecat segments it by VAD), JSON response.
- ``VoiceLLM``: Pipecat's OpenAI chat service with the profile's
  ``extra_body`` (e.g. thinking off) and note-role mapping.
- ``SpeechTTS``: one streamed ``/audio/speech`` request per sentence, PCM or
  WAV at any rate, resampled to 16 kHz.

Nothing here logs what was said; errors carry status codes only.
"""

from __future__ import annotations

import struct
import time
from collections.abc import AsyncGenerator

import aiohttp
from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    TranscriptionFrame,
    TTSAudioRawFrame,
)
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.settings import STTSettings, TTSSettings
from pipecat.services.stt_service import SegmentedSTTService
from pipecat.services.tts_service import TTSService
from pipecat.utils.time import time_now_iso8601

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


def cut_loop(text: str, longest: int = 6) -> str:
    """Whisper sometimes repeats a word or phrase until the segment ends
    ("das das das …"); keep the text up to the first repeat of a phrase
    said three times in a row."""
    words = text.split()
    key = [w.lower().strip(",.!?") for w in words]
    for start in range(len(words)):
        for size in range(1, longest + 1):
            end = start + 3 * size
            if end > len(words):
                break
            if (
                key[start : start + size]
                == key[start + size : start + 2 * size]
                == key[start + 2 * size : end]
            ):
                return " ".join(words[: start + size])
    return text


class VoiceLLM(OpenAILLMService):
    """Streaming chat completions with the delegate tool, thinking off."""

    def __init__(
        self, settings: LlmSettings, *, system: str, timings: Timings, **kwargs
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
        self._first_token = False
        #: Whether the response in flight said anything before a tool call.
        self.spoke = False

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
        self._timings.llm_started()
        await super()._process_context(context)

    def _token(self):
        if not self._first_token:
            self._first_token = True
            self._timings.llm_first_token()

    async def _push_llm_text(self, text: str):
        self._token()
        if text.strip():
            self.spoke = True
        await super()._push_llm_text(text)

    async def run_function_calls(self, function_calls):
        self._token()
        await super().run_function_calls(function_calls)


def note_messages(messages: list, role: str) -> list:
    """Map "developer" notes (Pipecat's and ours) to the profile's note role
    and merge user messages that would end up next to each other; the system
    prompt at the front is never touched, so its cached prefix holds."""
    out: list = []
    for index, message in enumerate(messages):
        if message.get("role") == "developer" and role != "developer":
            message = {**message, "role": role}
        previous = out[-1] if out else None
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


class SpeechTTS(TTSService):
    """One streamed ``POST {base_url}/audio/speech`` per sentence."""

    def __init__(
        self,
        settings: TtsSettings,
        *,
        voice: str,
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
        self._stage = settings
        self._voice = voice
        self._http = http
        self._timings = timings
        #: Milliseconds of audio per TTS context, for "what was heard" maths.
        self.durations: dict[str, float] = {}

    def sentence_ms(self, context_id: str | None) -> float | None:
        return self.durations.get(context_id) if context_id else None

    async def run_tts(
        self, text: str, context_id: str
    ) -> AsyncGenerator[Frame | None, None]:
        body = {
            "model": self._stage.model,
            "input": text,
            "voice": self._voice,
            "response_format": self._stage.response_format,
            **self._stage.extra_body,
        }
        if self._stage.speed is not None:
            body["speed"] = self._stage.speed
        started = time.monotonic()
        resampler = create_stream_resampler()
        wav = _WavStream() if self._stage.response_format == "wav" else None
        rate = self._stage.sample_rate
        carry = b""
        produced = 0
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
                    yield TTSAudioRawFrame(pcm, SAMPLE_RATE, 1, context_id=context_id)
                tail = await resampler.flush()
                if tail:
                    produced += len(tail)
                    yield TTSAudioRawFrame(tail, SAMPLE_RATE, 1, context_id=context_id)
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            yield ErrorFrame(error=f"TTS request failed: {type(exc).__name__}")
        finally:
            self.durations[context_id] = produced / (SAMPLE_RATE * 2 / 1000)
