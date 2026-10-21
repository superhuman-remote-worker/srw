"""OpenAI-compatible ``/v1/audio/speech`` over one Piper process; local_voice_gateway.md §6.2.

Speaches always speaks with a multi-speaker voice's first speaker, and the
first MLS German speaker reads at half speed. This server keeps one
``piper --json-input`` process (Piper 2023.11.14-2, MIT) with the model
loaded and picks the speaker per request: ``voice`` is a speaker id or a
name from the voice's ``speaker_id_map``; empty means ``PIPER_SPEAKER``.
Answers ``wav`` (the voice's rate in the header) or raw ``pcm`` at that
rate. Standard library only; nothing is logged but request counts.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import threading
import uuid
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

MODEL = os.environ.get("PIPER_MODEL", "/models/de_DE-mls-medium.onnx")
SPEAKER = os.environ.get("PIPER_SPEAKER", "0")
# Piper writes each utterance to a file, read back and deleted at once. It
# stays in memory: /dev/shm is a tmpfs in every container (A5: no audio on
# disk); /tmp would put the caller-facing speech on the container's disk.
WORK = Path(os.environ.get("PIPER_WORK", "/dev/shm/piper"))


class Piper:
    def __init__(self):
        WORK.mkdir(parents=True, exist_ok=True)
        config = json.loads(Path(MODEL + ".json").read_text())
        self.rate = config["audio"]["sample_rate"]
        self.speakers = config.get("speaker_id_map", {})
        self.lock = threading.Lock()
        self.process = subprocess.Popen(
            [
                "/opt/piper/piper",
                "--model",
                MODEL,
                "--json-input",
                "--quiet",
                "--sentence_silence",
                os.environ.get("PIPER_SENTENCE_SILENCE", "0.15"),
                "--length_scale",
                os.environ.get("PIPER_LENGTH_SCALE", "1.0"),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )

    def speaker(self, voice: str) -> int:
        voice = (voice or SPEAKER).strip()
        if voice in self.speakers:
            return self.speakers[voice]
        return (
            int(voice)
            if voice.isdigit() and int(voice) < max(1, len(self.speakers))
            else int(SPEAKER)
        )

    def synthesize(self, text: str, voice: str) -> bytes:
        path = WORK / f"{uuid.uuid4().hex}.wav"
        line = json.dumps(
            {"text": text, "speaker_id": self.speaker(voice), "output_file": str(path)}
        )
        with self.lock:
            self.process.stdin.write(line + "\n")
            self.process.stdin.flush()
            written = self.process.stdout.readline().strip()
        if not written:
            raise RuntimeError("piper stopped")
        try:
            with wave.open(written, "rb") as wav:
                return wav.readframes(wav.getnframes())
        finally:
            Path(written).unlink(missing_ok=True)


PIPER = Piper()


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
            model = {
                "id": Path(MODEL).stem,
                "object": "model",
                "sample_rate": PIPER.rate,
            }
            self.reply(200, json.dumps({"data": [model]}).encode())
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
            response_format = body.get("response_format", "wav")
            if not text or response_format not in {"wav", "pcm"}:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            self.reply(400, b'{"error":{"code":"invalid_request"}}')
            return
        pcm = PIPER.synthesize(text, str(body.get("voice", "")))
        if response_format == "pcm":
            self.reply(200, pcm, "audio/pcm")
            return
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(PIPER.rate)
            wav.writeframes(pcm)
        self.reply(200, buffer.getvalue(), "audio/wav")


if __name__ == "__main__":
    ThreadingHTTPServer(
        ("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler
    ).serve_forever()
