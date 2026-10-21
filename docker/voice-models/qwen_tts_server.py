"""OpenAI-compatible ``/v1/audio/speech`` over Qwen3-TTS on PyTorch, streamed; local_voice_gateway.md §6.2 (WP4).

faster-qwen3-tts (MIT) runs Qwen3-TTS 12Hz Base (Apache-2.0) with captured
GPU graphs and yields audio every ``CHUNK_FRAMES`` codec frames (8 frames are
0.67 s of speech), so the first audio leaves while the sentence is still
being generated. The voice is cloned from ``VOICES_DIR/<voice>.wav`` and its
transcript ``<voice>.txt`` (read once; the clone prompt stays cached in
memory). Answers 16-bit ``pcm`` (24 kHz) or a streamed ``wav``.

One sentence at a time (a lock); a client that hangs up mid-sentence stops
the generation. Audio and text stay in memory (A5: no audio on disk);
nothing is logged but counts. Runs on ROCm or CUDA builds of PyTorch.
"""

from __future__ import annotations

import json
import os
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import numpy as np

MODEL_DIR = os.environ.get("MODEL_DIR", "/models/qwen3-tts-0.6b-base")
VOICES_DIR = Path(os.environ.get("VOICES_DIR", "/voices"))
DEFAULT_VOICE = os.environ.get("TTS_VOICE", "")
LANGUAGE = os.environ.get("TTS_LANGUAGE", "German")
CHUNK_FRAMES = int(os.environ.get("CHUNK_FRAMES", "8"))
COUNTS = {"speech": 0, "errors": 0, "cancelled": 0}


def load_voices(directory: Path) -> dict[str, tuple[str, str]]:
    """Reference clips with transcripts: name -> (wav path, text)."""
    voices = {}
    for wav in sorted(directory.glob("*.wav")):
        text = wav.with_suffix(".txt")
        if text.exists():
            voices[wav.stem] = (str(wav), text.read_text(encoding="utf-8").strip())
    return voices


def pcm16(audio) -> bytes:
    samples = np.clip(np.asarray(audio, dtype=np.float32).reshape(-1), -1.0, 1.0)
    return (samples * 32767).astype("<i2").tobytes()


def wav_header(rate: int) -> bytes:
    return (
        b"RIFF"
        + struct.pack("<I", 0xFFFFFFFF)
        + b"WAVEfmt "
        + struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16)
        + b"data"
        + struct.pack("<I", 0xFFFFFFFF)
    )


class Speaker:
    def __init__(self):
        from faster_qwen3_tts import FasterQwen3TTS

        self.voices = load_voices(VOICES_DIR)
        if not self.voices:
            raise SystemExit(f"no reference voices (<name>.wav + .txt) in {VOICES_DIR}")
        self.model = FasterQwen3TTS.from_pretrained(MODEL_DIR)
        self.rate = int(self.model.sample_rate)
        self.lock = threading.Lock()
        # First use compiles kernels and captures the graphs: do it now.
        for _ in range(3):
            for voice in self.voices:
                for _chunk in self.stream("Guten Tag, das ist ein Test.", voice):
                    pass

    def stream(self, text: str, voice: str):
        wav, ref_text = self.voices[voice]
        for audio, _rate, _timing in self.model.generate_voice_clone_streaming(
            text=text,
            language=LANGUAGE,
            ref_audio=wav,
            ref_text=ref_text,
            chunk_size=CHUNK_FRAMES,
        ):
            yield pcm16(audio)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # requests carry what is said; log nothing
        pass

    def reply(self, status: int, body: bytes, content_type: str = "application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, b"OK", "text/plain")
        elif self.path == "/v1/models":
            data = [{"id": v, "object": "voice"} for v in SPEAKER.voices]
            self.reply(200, json.dumps({"data": data, "counts": COUNTS}).encode())
        else:
            self.reply(404, b'{"error":{"code":"not_found"}}')

    def do_POST(self):
        if self.path != "/v1/audio/speech":
            self.reply(404, b'{"error":{"code":"not_found"}}')
            return
        try:
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
            )
            text = str(body["input"]).strip()
            voice = str(
                body.get("voice") or DEFAULT_VOICE or next(iter(SPEAKER.voices))
            )
            response_format = body.get("response_format", "pcm")
            if not text or response_format not in {"pcm", "wav"}:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            self.reply(400, b'{"error":{"code":"invalid_request"}}')
            return
        if voice not in SPEAKER.voices:
            self.reply(400, b'{"error":{"code":"unknown_voice"}}')
            return
        COUNTS["speech"] += 1
        with SPEAKER.lock:
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "audio/wav"
                if response_format == "wav"
                else f"audio/pcm;rate={SPEAKER.rate}",
            )
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            chunks = SPEAKER.stream(text, voice)
            try:
                if response_format == "wav":
                    self.chunk(wav_header(SPEAKER.rate))
                for data in chunks:
                    self.chunk(data)
                self.chunk(b"")
            except OSError:
                COUNTS["cancelled"] += 1  # the caller barged in; stop generating
                self.close_connection = True
            except Exception:  # noqa: BLE001 - the stream has started; end it
                COUNTS["errors"] += 1
                self.close_connection = True
            finally:
                chunks.close()

    def chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()


if __name__ == "__main__":
    started = time.monotonic()
    SPEAKER = Speaker()
    print(f"ready in {time.monotonic() - started:.0f} s", flush=True)
    ThreadingHTTPServer(
        ("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler
    ).serve_forever()
