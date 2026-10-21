"""Try the cascade engine without Asterisk; local_voice_gateway.md §4.3, §6.1 (WP3).

    PYTHONPATH=src .venv-voice/bin/python scripts/voice_cascade_try.py \\
        --profile-file docker/voice-models/cascade-cpu.yaml

One call on the real gateway ``Session`` with a ``FileAudioPort`` as the
phone: accept with the test set's instructions, the greeting as an
``instructions`` append 1.5 s later (as the driver sends it), then the
steps in ``--turns``. Each step plays a test-set clip (8 kHz A-law, upsampled,
so it sounds like a phone) and waits for the reply; a delegation gets the
step's canned answer as ``commentary`` after ``--answer-delay`` seconds.
``barge:A/B`` plays A and, once the voice has talked for ``--barge-after``
seconds, B over it.

Prints the transcript and per-turn timings (end of caller speech to first
audio out, STT, LLM first token, TTS first audio), and writes to ``--out``
(default ``.cache/voice-cascade-try/<UTC>/``, gitignored): ``heard.wav``
(the voice through an 8 kHz G.711 leg), ``call.wav`` (stereo: caller left,
voice right) and ``report.json``. ``--asr-check`` transcribes each voice
utterance from ``heard.wav`` with the profile's STT and reports the word
error rate: how well the German voice survives the phone.
"""

from __future__ import annotations

import argparse
import asyncio
import audioop
import io
import json
import re
import sys
import time
import wave
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import voice_cascade_testset as testset

from voice_gateway.config import Line
from voice_gateway.engines.cascade import parse_settings
from voice_gateway.engines.cascade.engine import CascadeEngine
from voice_gateway.engines.cascade.services import cut_loop
from voice_gateway.engines.cascade.fileport import (
    FRAME_MS,
    FileAudioPort,
    interleave,
    phone,
    read_wav,
    write_wav,
)
from voice_gateway.interfaces import FRAME_BYTES
from voice_gateway.session import Session

#: Below this a 20 ms frame is silence (G.711 turns digital silence into
#: a little noise).
SILENCE_RMS = 60

DEFAULT_TURNS = "a02-checkout,d03-invoice-copy,barge:a13-children/t03-complaint"


class PortSource:
    """The call source: answering attaches the file-backed line."""

    def __init__(self, port: FileAudioPort):
        self.port = port

    async def answer(self, session):
        session.audio = self.port
        self.port.start()

    async def reject(self, session, status_code):
        pass

    async def refer(self, session, target_uri):
        print(f"  refer to {target_uri}")

    async def hangup(self, session):
        pass


class Harness:
    def __init__(self, settings, *, answer_delay: float, barge_after: float):
        self.settings = settings
        self.answer_delay = answer_delay
        self.barge_after = barge_after
        self.port = FileAudioPort()
        self.engine: CascadeEngine | None = None
        self.events: list[dict] = []
        self.answers_due = 0
        self.awaiting_voice_after = -1
        self.current_step = ""
        self.steps: list[dict] = []
        self.started = time.monotonic()

        def factory():
            self.engine = CascadeEngine(settings)
            return self.engine

        headers = [
            {"name": "From", "value": "sip:+4930123456@pbx.example"},
            {"name": "To", "value": "sip:2000@pbx.example"},
        ]
        self.session = Session(
            Line("try", "try", b"key"),
            headers,
            PortSource(self.port),
            {"cascade": factory},
        )

    def log(self, text: str) -> None:
        print(f"[{self.port.clock_ms / 1000:6.2f}s] {text}", flush=True)

    async def command(self, kind: str, content: str, delegation_id: str | None = None):
        command = {
            "type": f"session.{kind}.append",
            "event_id": f"try_{len(self.events)}_{kind}",
            "content": content,
        }
        if delegation_id:
            command["delegation_id"] = delegation_id
        await self.session.command(command, self.queue)

    async def collect(self):
        while (event := await self.queue.get()) is not None:
            self.events.append({**event, "clock_ms": self.port.clock_ms})
            kind = event["type"]
            if kind == "session.input_transcript.delta":
                self.log(f"caller: {event['delta']}")
            elif kind == "session.output_transcript.delta":
                self.log(f"voice:  {event['delta']}")
            elif kind == "session.delegation.created":
                self.log("delegation created")
                self.answers_due += 1
                asyncio.create_task(
                    self.answer(event["delegation"]["id"], self.current_step)
                )
            elif kind == "error":
                self.log(f"error: {event['error'].get('code')}")

    async def answer(self, delegation_id: str, step: str):
        await asyncio.sleep(self.answer_delay)
        text = testset.CANNED_ANSWERS.get(step, testset.DEFAULT_ANSWER)
        self.log(f"back office answers ({self.answer_delay:.1f}s later)")
        self.awaiting_voice_after = self.port.clock_ms
        self.answers_due -= 1
        await self.command("commentary", text, delegation_id)

    def voice_started_after(self, clock_ms: int) -> bool:
        return any(start >= clock_ms for start in self.port.voice_starts)

    async def settle(
        self, after_ms: int, quiet_s: float = 2.0, timeout_s: float = 60.0
    ):
        """Wait for the reply to ``after_ms`` and anything a delegation adds."""
        deadline = time.monotonic() + timeout_s
        quiet_since = None
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            busy = (
                self.port.voice_playing
                or self.port.caller_speaking
                or self.answers_due > 0
                or not self.voice_started_after(after_ms)
                or (
                    self.awaiting_voice_after >= 0
                    and not self.voice_started_after(self.awaiting_voice_after)
                )
            )
            if busy:
                quiet_since = None
                continue
            quiet_since = quiet_since or time.monotonic()
            if time.monotonic() - quiet_since >= quiet_s:
                return
        self.log("step timed out waiting for the voice")

    async def play(self, utterance_id: str) -> int:
        """Play a clip; returns the line time its speech ended (the test-set
        clips carry 0.8 s of trailing silence, which is not speech)."""
        self.current_step = utterance_id
        self.log(f"caller clip {utterance_id}")
        clips = len(self.port.clip_ends)
        pcm = read_wav(testset.clip(utterance_id))
        self.port.say(pcm)
        while len(self.port.clip_ends) == clips:
            await asyncio.sleep(0.02)
        return self.port.clip_ends[-1] - trailing_silence_ms(pcm)

    async def run(self, turns: list[str], instructions: str):
        body = {
            "session": {
                "type": "live",
                "model": "cascade",
                "instructions": instructions,
                "audio": {"output": {"voice": "marin"}},
                "delegation": {"type": "client"},
            }
        }
        await self.session.control("accept", body)
        _, self.queue = self.session.subscribe()
        collector = asyncio.create_task(self.collect())
        await asyncio.sleep(1.5)
        self.log("greeting sent as an instructions append")
        mark = self.port.clock_ms
        await self.command("instructions", testset.greeting_instruction())
        await self.settle(mark, quiet_s=1.0)
        for turn in turns:
            if turn.startswith("barge:"):
                first, second = turn.removeprefix("barge:").split("/")
                end = await self.play(first)
                self.steps.append({"step": first, "speech_end_ms": end})
                while not self.voice_started_after(end):
                    await asyncio.sleep(0.02)
                start = next(s for s in self.port.voice_starts if s >= end)
                while self.port.clock_ms < start + self.barge_after * 1000:
                    await asyncio.sleep(0.02)
                heard = self.port.played_ms()
                self.log(f"caller talks over the voice ({heard} ms played so far)")
                end = await self.play(second)
                self.steps.append(
                    {"step": second, "speech_end_ms": end, "barge_in": True}
                )
                await self.settle(end)
            else:
                end = await self.play(turn)
                self.steps.append({"step": turn, "speech_end_ms": end})
                await self.settle(end)
        self.log("caller hangs up")
        await self.session.close("remote_hangup")
        await self.port.close()
        collector.cancel()


def trailing_silence_ms(pcm: bytes) -> int:
    end = len(pcm) - len(pcm) % FRAME_BYTES
    while end > 0 and audioop.rms(pcm[end - FRAME_BYTES : end], 2) <= SILENCE_RMS:
        end -= FRAME_BYTES
    return (len(pcm) - end) // FRAME_BYTES * FRAME_MS


def voice_runs(pcm: bytes, gap_ms: int = 400) -> list[tuple[int, int]]:
    """(start, end) ms of each stretch of voice audio in the recording."""
    runs, start, silent = [], None, 0
    for index in range(0, len(pcm), FRAME_BYTES):
        loud = audioop.rms(pcm[index : index + FRAME_BYTES], 2) > SILENCE_RMS
        at = index // FRAME_BYTES * FRAME_MS
        if loud:
            start = at if start is None else start
            silent = 0
        elif start is not None:
            silent += FRAME_MS
            if silent >= gap_ms:
                runs.append((start, at - silent + FRAME_MS))
                start, silent = None, 0
    if start is not None:
        runs.append((start, len(pcm) // FRAME_BYTES * FRAME_MS))
    return runs


def word_error_rate(reference: str, hypothesis: str) -> float:
    ref = re.findall(r"[\wäöüß]+", reference.lower())
    hyp = re.findall(r"[\wäöüß]+", hypothesis.lower())
    previous = list(range(len(hyp) + 1))
    for i, word in enumerate(ref, 1):
        current = [i]
        for j, other in enumerate(hyp, 1):
            current.append(
                min(
                    previous[j] + 1,
                    current[j - 1] + 1,
                    previous[j - 1] + (word != other),
                )
            )
        previous = current
    return previous[-1] / max(1, len(ref))


async def asr_check(settings, heard: bytes, events: list[dict]) -> list[dict]:
    said = [e for e in events if e["type"] == "session.output_transcript.delta"]
    results = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as http:
        for start, end in voice_runs(heard):
            text = " ".join(
                e["delta"] for e in said if start - 300 <= e["clock_ms"] <= end
            )
            if not text:
                continue
            audio = heard[start * 32 : end * 32]
            buffer = io.BytesIO()
            with wave.open(buffer, "wb") as wav:
                wav.setnchannels(1)
                wav.setsampwidth(2)
                wav.setframerate(16_000)
                wav.writeframes(audio)
            form = aiohttp.FormData()
            form.add_field(
                "file",
                buffer.getvalue(),
                filename="voice.wav",
                content_type="audio/wav",
            )
            for key, value in (
                ("model", settings.stt.model),
                ("language", settings.language),
                ("response_format", "json"),
                ("temperature", "0"),
            ):
                form.add_field(key, value)
            headers = (
                {"Authorization": f"Bearer {settings.stt.api_key}"}
                if settings.stt.api_key
                else {}
            )
            async with http.post(
                settings.stt.base_url + "/audio/transcriptions",
                data=form,
                headers=headers,
            ) as response:
                body = await response.json(content_type=None)
            heard_text = cut_loop(body.get("text", "").strip())
            results.append(
                {
                    "start_ms": start,
                    "said": text,
                    "heard": heard_text,
                    "wer": round(word_error_rate(text, heard_text), 2),
                }
            )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--profile-file", type=Path, required=True)
    parser.add_argument("--profile", default="cascade-cpu")
    parser.add_argument("--turns", default=DEFAULT_TURNS)
    parser.add_argument("--answer-delay", type=float, default=3.0)
    parser.add_argument("--barge-after", type=float, default=1.5)
    parser.add_argument("--asr-check", action="store_true")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    settings = parse_settings(
        args.profile, testset.load_profile(args.profile_file, args.profile)
    )
    out = args.out or testset.REPO_ROOT / ".cache" / "voice-cascade-try" / datetime.now(
        UTC
    ).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=True)
    harness = Harness(
        settings, answer_delay=args.answer_delay, barge_after=args.barge_after
    )
    turns = [t.strip() for t in args.turns.split(",") if t.strip()]
    asyncio.run(harness.run(turns, testset.instructions()))

    port = harness.port
    heard = phone(bytes(port.heard))
    write_wav(out / "heard.wav", heard)
    write_wav(out / "call.wav", interleave(bytes(port.said), heard), channels=2)
    timings = [t.as_dict() for t in harness.engine.timings.turns]
    rows = []
    for step in harness.steps:
        end = step["speech_end_ms"]
        starts = [s for s in port.voice_starts if s >= end]
        rows.append(
            {**step, "line_first_audio_ms": (starts[0] - end) if starts else None}
        )
    print("\nWhat the voice LLM's context holds at the end:")
    for message in harness.engine.context.get_messages():
        if message.get("role") in {"user", "assistant"} and message.get("content"):
            print(f"  {message['role']:9s} {message['content']}")
    print(
        "\nTurn timings (ms): line = end of the clip's speech to first voice audio on the port;"
    )
    print("engine = VAD end of speech to first audio, plus stage times")
    print(
        f"{'step':24s} {'line':>6s} {'engine':>6s} {'stt':>6s} {'llm1st':>6s} {'tts1st':>6s} deleg"
    )
    for row, timing in zip(
        rows, timings + [{}] * (len(rows) - len(timings)), strict=False
    ):
        print(
            f"{row['step']:24s} {row['line_first_audio_ms'] or '-':>6} "
            f"{timing.get('first_audio_ms') or '-':>6} {timing.get('stt_ms') or '-':>6} "
            f"{timing.get('llm_first_token_ms') or '-':>6} {timing.get('tts_first_audio_ms') or '-':>6} "
            f"{'yes' if timing.get('delegated') else ''}"
        )
    report = {
        "profile": args.profile,
        "turns": rows,
        "engine_timings": timings,
        "context": [
            {k: m.get(k) for k in ("role", "content")}
            for m in harness.engine.context.get_messages()
        ],
        "events": [
            {k: e[k] for k in ("type", "clock_ms", "delta") if k in e}
            for e in harness.events
        ],
    }
    if args.asr_check:
        report["asr_check"] = asyncio.run(asr_check(settings, heard, harness.events))
        print("\nThe voice as STT hears it after G.711:")
        for item in report["asr_check"]:
            print(
                f"  WER {item['wer']:.2f} | said: {item['said']}\n           | heard: {item['heard']}"
            )
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nrecordings and report in {out}")


if __name__ == "__main__":
    sys.exit(main())
