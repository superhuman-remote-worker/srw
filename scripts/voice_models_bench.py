"""German TTS and STT on their own, for the cascade's model servers; local_voice_gateway.md §5 (A4), §6.2 (WP4).

    PYTHONPATH=src .venv-voice/bin/python scripts/voice_models_bench.py tts \\
        --tts-url http://127.0.0.1:18454/v1 --model qwen3-tts --voice de-1 \\
        --judge-url http://127.0.0.1:18455/v1 --label qwen3-tts
    PYTHONPATH=src .venv-voice/bin/python scripts/voice_models_bench.py stt \\
        --stt-url http://127.0.0.1:18453/v1 --model whisper-large-v3-turbo --label turbo

``tts``: renders every test-set line (``utterances.yaml``) and the A1 call's
answers through ``/audio/speech`` (streamed), measures time to first audio
and the real-time factor (synthesis time / audio length), resamples to the
gateway's 16 kHz as the engine does, sends it through an 8 kHz G.711 A-law
leg, and has the judge (another server's ``/audio/transcriptions``) write
down what a caller would understand: word error rate per line and overall.

``stt``: transcribes the test set's phone clips (8 kHz A-law, as Asterisk
hands them over, upsampled to 16 kHz) with the STT under test: WER against
the text (after the engine's own cut of whisper's repetition loops) and the
time per utterance. The bench's clips are Piper's ``mls``
voice, which garbles some lines itself; ``--clips DIR`` takes another voice's
``<id>.phone.wav`` files instead (what ``tts`` wrote).

Word error rates compare numbers as words: the judge writes "neunzehn"
where the text says "19" (digits become German number words, a phone
number digit by digit, "12." an ordinal, and inflected ordinals count as
the same word); hyphens do not split a word ("Check-out", "Checkout").

Writes ``report.json`` (and, for ``tts``, the rendered and phone WAVs, which
are synthetic speech) to ``--out`` (default ``.cache/voice-models-bench/
<label>-<UTC>/``, gitignored).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import statistics
import struct
import time
import wave
from datetime import UTC, datetime
from pathlib import Path

import re

import aiohttp
import voice_cascade_testset as testset
from num2words import num2words
from pipecat.audio.utils import create_stream_resampler
from voice_cascade_try import word_error_rate

from voice_gateway.engines.cascade.fileport import phone, read_wav, write_wav
from voice_gateway.engines.cascade.services import _WavStream, cut_loop
from voice_gateway.interfaces import SAMPLE_RATE

#: What the voice said in the A1 call (local_voice_gateway.md §9.6), the
#: filler, and the canned back-office answers it phrases.
A1_LINES = [
    ("a1-greeting", testset.GREETING),
    ("a1-fact", "Der Check-out ist bis 11 Uhr möglich."),
    ("a1-filler", "Einen Moment, ich schaue nach."),
    *((f"answer-{k}", v) for k, v in testset.CANNED_ANSWERS.items()),
]


_ORDINALS = {num2words(n, lang="de", to="ordinal") for n in range(1, 32)}
_DIGITS = "null eins zwei drei vier fünf sechs sieben acht neun".split()


def _number_words(match: re.Match) -> str:
    digits, ordinal = match.group(1), match.group(2)
    if digits.startswith("0") or len(digits) > 4:
        return " ".join(_DIGITS[int(d)] for d in digits)
    return num2words(int(digits), lang="de", to="ordinal" if ordinal else "cardinal")


def spoken(text: str) -> str:
    """Numbers as words, inflected ordinals as their base form."""
    text = re.sub(r"(?<!\w)(\d+)(\.(?=\s+\w))?", _number_words, text)
    # "Check-out" and "Checkout", "E-Mail" and "EMail" are the same word.
    words = re.findall(r"[\wäöüß]+", text.lower().replace("-", ""))
    return " ".join(
        w[:-1] if w[:-1] in _ORDINALS and w[-1] in "nmrs" else w for w in words
    )


def wer(reference: str, hypothesis: str) -> float:
    return word_error_rate(spoken(reference), spoken(hypothesis))


def wav_bytes(pcm: bytes, rate: int = SAMPLE_RATE) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        wav.writeframes(pcm)
    return buffer.getvalue()


async def transcribe(
    http, url: str, model: str, pcm: bytes, key: str | None
) -> tuple[str, float]:
    form = aiohttp.FormData()
    form.add_field(
        "file", wav_bytes(pcm), filename="speech.wav", content_type="audio/wav"
    )
    for name, value in (
        ("model", model),
        ("language", "de"),
        ("response_format", "json"),
        ("temperature", "0"),
    ):
        form.add_field(name, value)
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    started = time.monotonic()
    async with http.post(
        url + "/audio/transcriptions", data=form, headers=headers
    ) as response:
        response.raise_for_status()
        body = await response.json(content_type=None)
    return " ".join(str(body.get("text", "")).split()), time.monotonic() - started


async def speak(http, args, text: str) -> dict:
    """One streamed /audio/speech request: 16 kHz PCM plus timings."""
    body = {
        "model": args.model,
        "input": text,
        "voice": args.voice,
        "response_format": args.format,
    }
    resampler = create_stream_resampler()
    wav = _WavStream() if args.format == "wav" else None
    rate, carry, out, first = args.rate, b"", bytearray(), None
    raw = 0
    started = time.monotonic()
    async with http.post(args.tts_url + "/audio/speech", json=body) as response:
        response.raise_for_status()
        async for chunk in response.content.iter_chunked(4096):
            if wav is not None:
                chunk = wav.feed(chunk)
                rate = wav.rate or rate
            chunk = carry + chunk
            usable = len(chunk) - len(chunk) % 2
            chunk, carry = chunk[:usable], chunk[usable:]
            if not chunk:
                continue
            if first is None:
                first = time.monotonic() - started
            raw += len(chunk)
            out += await resampler.resample(chunk, rate, SAMPLE_RATE)
    out += await resampler.flush()
    seconds = time.monotonic() - started
    audio_s = raw / 2 / rate
    return {
        "pcm": bytes(out),
        "first_audio_ms": round((first or seconds) * 1000),
        "synth_s": round(seconds, 3),
        "audio_s": round(audio_s, 3),
        "rtf": round(seconds / audio_s, 3) if audio_s else None,
    }


def trim_silence(pcm: bytes, level: int = 200) -> bytes:
    """Leading and trailing silence off, so the judge hears speech only."""
    samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
    loud = [i for i, s in enumerate(samples) if abs(s) > level]
    if not loud:
        return pcm
    start, end = max(0, loud[0] - 1600), min(len(samples), loud[-1] + 1600)
    return pcm[start * 2 : end * 2]


async def run_tts(args, out: Path) -> dict:
    lines = [(u["id"], u["text"]) for u in testset.utterances()] + A1_LINES
    results = []
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        # One warm-up request: the first one loads kernels and caches.
        await speak(http, args, "Guten Tag.")
        for name, text in lines:
            synth = await speak(http, args, text)
            clean = trim_silence(synth.pop("pcm"))
            line = phone(clean)
            write_wav(out / f"{name}.wav", clean)
            write_wav(out / f"{name}.phone.wav", line)
            heard, judge_s = await transcribe(
                http, args.judge_url, args.judge_model, line, None
            )
            wer_ = wer(text, heard)
            results.append(
                {
                    "id": name,
                    "text": text,
                    "heard": heard,
                    "wer": round(wer_, 3),
                    **synth,
                }
            )
            print(
                f"{name:24s} WER {wer_:4.2f} first {synth['first_audio_ms']:5d} ms "
                f"RTF {synth['rtf']:.2f} | {heard}",
                flush=True,
            )
    return summarise(args, results)


def summarise(args, results: list[dict]) -> dict:
    testset_rows = [r for r in results if not r["id"].startswith(("a1-", "answer-"))]
    a1_rows = [r for r in results if r["id"].startswith(("a1-", "answer-"))]

    def mean(rows, key):
        values = [r[key] for r in rows if r.get(key) is not None]
        return round(statistics.mean(values), 3) if values else None

    def median(rows, key):
        values = [r[key] for r in rows if r.get(key) is not None]
        return round(statistics.median(values), 3) if values else None

    summary = {
        "label": args.label,
        "lines": len(results),
        "wer_testset_mean": mean(testset_rows, "wer"),
        "wer_a1_mean": mean(a1_rows, "wer"),
        "wer_all_mean": mean(results, "wer"),
        "lines_perfect": sum(r["wer"] == 0 for r in results),
    }
    if "first_audio_ms" in results[0]:
        summary.update(
            first_audio_ms_median=median(results, "first_audio_ms"),
            rtf_median=median(results, "rtf"),
        )
    if "seconds" in results[0]:
        summary.update(
            seconds_median=median(results, "seconds"),
            seconds_mean=mean(results, "seconds"),
        )
    print(json.dumps(summary, indent=2))
    return {"summary": summary, "results": results}


async def run_stt(args) -> dict:
    results = []
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        clips = [
            (
                u["id"],
                u["text"],
                read_wav(
                    args.clips / f"{u['id']}.phone.wav"
                    if args.clips
                    else testset.clip(u["id"])
                ),
            )
            for u in testset.utterances()
        ]
        await transcribe(http, args.stt_url, args.model, clips[0][2], None)  # warm-up
        for name, text, pcm in clips:
            heard, seconds = await transcribe(http, args.stt_url, args.model, pcm, None)
            heard = cut_loop(heard)  # as TranscriptionSTT does
            wer_ = wer(text, heard)
            results.append(
                {
                    "id": name,
                    "text": text,
                    "heard": heard,
                    "wer": round(wer_, 3),
                    "seconds": round(seconds, 3),
                    "audio_s": round(len(pcm) / 2 / SAMPLE_RATE, 2),
                }
            )
            print(
                f"{name:24s} WER {wer_:4.2f} {seconds * 1000:6.0f} ms | {heard}",
                flush=True,
            )
    return summarise(args, results)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    tts = sub.add_parser("tts")
    tts.add_argument("--tts-url", required=True)
    tts.add_argument("--model", required=True)
    tts.add_argument("--voice", default="")
    tts.add_argument("--format", choices=("pcm", "wav"), default="pcm")
    tts.add_argument("--rate", type=int, default=24_000, help="PCM sample rate")
    tts.add_argument("--judge-url", required=True)
    tts.add_argument("--judge-model", default="judge")
    stt = sub.add_parser("stt")
    stt.add_argument("--stt-url", required=True)
    stt.add_argument("--model", required=True)
    stt.add_argument(
        "--clips", type=Path, help="<id>.phone.wav files instead of the bench's"
    )
    for each in (tts, stt):
        each.add_argument("--label", required=True)
        each.add_argument("--out", type=Path)
    args = parser.parse_args()
    out = args.out or (
        testset.REPO_ROOT
        / ".cache"
        / "voice-models-bench"
        / f"{args.label}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    )
    out.mkdir(parents=True, exist_ok=True)
    report = asyncio.run(run_tts(args, out) if args.command == "tts" else run_stt(args))
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"report in {out}")


if __name__ == "__main__":
    main()
