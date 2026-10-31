"""OpenAI audio routes over llama-server's speech models; local_voice_gateway.md §4.3, §6.2 (WP4).

llama.cpp serves Qwen3-TTS on ``POST /tts`` and Qwen3-ASR through chat
completions with an ``input_audio`` part; the cascade (and the bench's
judge) speak OpenAI's audio API. This adapter translates, nothing more:

- ``POST /v1/audio/speech`` (``TTS_UPSTREAM``): ``input`` and ``voice`` go
  to ``/tts`` with the voice's reference clip (``VOICES_DIR/<voice>.wav``,
  read once at start), ``TTS_LANG`` and a fixed ``TTS_SEED``, streamed. The
  float32 samples come back as 16-bit PCM (``pcm``, the default, or a
  streamed ``wav`` with an open length) at the model's rate, chunk by chunk
  as llama.cpp produces them, so the first audio does not wait for the
  whole sentence.
- ``POST /v1/audio/transcriptions`` (``ASR_UPSTREAM``): the uploaded WAV goes
  to ``/v1/chat/completions`` as ``input_audio`` with the language forced by
  an assistant prefill (``language German<asr_text>``); answers
  ``{"text": …}``.

Audio and text stay in memory (A5: no audio on disk); nothing is logged but
request counts and status codes. Standard library only.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import struct
import sys
import threading
from email.parser import BytesParser
from email.policy import HTTP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

TTS_UPSTREAM = os.environ.get("TTS_UPSTREAM", "")
ASR_UPSTREAM = os.environ.get("ASR_UPSTREAM", "")
VOICES_DIR = Path(os.environ.get("VOICES_DIR", "/voices"))
DEFAULT_VOICE = os.environ.get("TTS_VOICE", "")
TTS_LANG = os.environ.get("TTS_LANG", "de")
TTS_SEED = int(os.environ.get("TTS_SEED", "1234"))
#: Qwen3-ASR's language names for the OpenAI ISO codes the cascade sends.
ASR_LANGUAGES = {"de": "German", "en": "English"}
_MARKER = "<asr_text>"
#: Upstream reads may block while llama.cpp generates a long sentence.
TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "120"))

COUNTS = {"speech": 0, "transcriptions": 0, "errors": 0}
_LOCK = threading.Lock()


def _count(key: str) -> None:
    with _LOCK:
        COUNTS[key] += 1


def load_voices(directory: Path) -> dict[str, str]:
    """Reference clips as base64, keyed by file stem."""
    if not directory.is_dir():
        return {}
    return {
        path.stem: base64.b64encode(path.read_bytes()).decode()
        for path in sorted(directory.glob("*.wav"))
    }


VOICES = load_voices(VOICES_DIR)


def _connection(base: str) -> tuple[http.client.HTTPConnection, str]:
    parts = urlsplit(base)
    cls = (
        http.client.HTTPSConnection
        if parts.scheme == "https"
        else http.client.HTTPConnection
    )
    return cls(parts.hostname, parts.port, timeout=TIMEOUT), parts.path.rstrip("/")


def float_to_pcm16(data: bytes) -> bytes:
    """Little-endian float32 samples to 16-bit PCM, clipped."""
    count = len(data) // 4
    samples = struct.unpack(f"<{count}f", data[: count * 4])
    return struct.pack(
        f"<{count}h",
        *(int(max(-1.0, min(1.0, s)) * 32767) for s in samples),
    )


def wav_header(rate: int) -> bytes:
    """A streamed WAV's header: mono 16-bit, length unknown (0xFFFFFFFF)."""
    return (
        b"RIFF"
        + struct.pack("<I", 0xFFFFFFFF)
        + b"WAVEfmt "
        + struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16)
        + b"data"
        + struct.pack("<I", 0xFFFFFFFF)
    )


def upstream_rate(content_type: str, default: int = 24_000) -> int:
    for part in content_type.split(";"):
        key, _, value = part.strip().partition("=")
        if key == "rate" and value.isdigit():
            return int(value)
    return default


def parse_form(content_type: str, body: bytes) -> dict[str, bytes]:
    """multipart/form-data fields by name (files and plain values)."""
    message = BytesParser(policy=HTTP).parsebytes(
        b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body
    )
    fields = {}
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if name:
            fields[name] = part.get_payload(decode=True) or b""
    return fields


def asr_text(content: str) -> str:
    """Qwen3-ASR answers ``language German<asr_text>…``; keep the text."""
    _, marker, text = content.partition(_MARKER)
    return (text if marker else content).strip()


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

    def error(self, status: int, code: str) -> None:
        _count("errors")
        self.reply(status, json.dumps({"error": {"code": code}}).encode())

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, b"OK", "text/plain")
        elif self.path == "/v1/models":
            data = [{"id": voice, "object": "voice"} for voice in VOICES]
            self.reply(200, json.dumps({"data": data, "counts": COUNTS}).encode())
        else:
            self.error(404, "not_found")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        if self.path == "/v1/audio/speech" and TTS_UPSTREAM:
            self.speech(body)
        elif self.path == "/v1/audio/transcriptions" and ASR_UPSTREAM:
            self.transcription(body)
        else:
            self.error(404, "not_found")

    # -- TTS ---------------------------------------------------------------

    def speech(self, body: bytes) -> None:
        try:
            request = json.loads(body)
            text = str(request["input"]).strip()
            response_format = request.get("response_format", "pcm")
            voice = str(request.get("voice") or DEFAULT_VOICE)
            if not text or response_format not in {"pcm", "wav"}:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            self.error(400, "invalid_request")
            return
        if voice not in VOICES:
            self.error(400, "unknown_voice")
            return
        _count("speech")
        upstream = {
            "input": text,
            "lang": TTS_LANG,
            "speaker_ref_b64": VOICES[voice],
            "seed": TTS_SEED,
            "stream": True,
            "response_format": "pcm",
        }
        connection, base = _connection(TTS_UPSTREAM)
        try:
            connection.request(
                "POST",
                base + "/tts",
                body=json.dumps(upstream),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            if response.status != 200:
                response.read()
                self.error(502, f"upstream_{response.status}")
                return
            rate = upstream_rate(response.getheader("Content-Type", ""))
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "audio/wav" if response_format == "wav" else f"audio/pcm;rate={rate}",
            )
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            if response_format == "wav":
                self.chunk(wav_header(rate))
            carry = b""
            while True:
                data = response.read1(8192)
                if not data:
                    break
                data = carry + data
                usable = len(data) - len(data) % 4
                carry = data[usable:]
                if usable:
                    self.chunk(float_to_pcm16(data[:usable]))
            self.chunk(b"")
        except (OSError, http.client.HTTPException):
            # The caller hung up (barge-in) or llama.cpp went away.
            _count("errors")
            self.close_connection = True
        finally:
            connection.close()

    def chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()

    # -- ASR ---------------------------------------------------------------

    def transcription(self, body: bytes) -> None:
        try:
            fields = parse_form(self.headers.get("Content-Type", ""), body)
            audio = fields["file"]
            if not audio:
                raise ValueError
        except (KeyError, ValueError, TypeError):
            self.error(400, "invalid_request")
            return
        _count("transcriptions")
        language = fields.get("language", b"").decode().strip().lower()
        name = ASR_LANGUAGES.get(language, language.capitalize())
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": base64.b64encode(audio).decode(),
                            "format": "wav",
                        },
                    }
                ],
            }
        ]
        if name:
            # Force the language as the model's own output starts.
            messages.append(
                {"role": "assistant", "content": f"language {name}{_MARKER}"}
            )
        upstream = {
            "messages": messages,
            "temperature": 0,
            "max_tokens": int(os.environ.get("ASR_MAX_TOKENS", "400")),
        }
        connection, base = _connection(ASR_UPSTREAM)
        try:
            connection.request(
                "POST",
                base + "/v1/chat/completions",
                body=json.dumps(upstream),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            data = response.read()
            if response.status != 200:
                self.error(502, f"upstream_{response.status}")
                return
            content = json.loads(data)["choices"][0]["message"].get("content") or ""
        except (OSError, http.client.HTTPException, ValueError, KeyError, IndexError):
            self.error(502, "upstream_failed")
            return
        finally:
            connection.close()
        self.reply(200, json.dumps({"text": asr_text(content)}).encode())


def main() -> None:
    if not TTS_UPSTREAM and not ASR_UPSTREAM:
        sys.exit("set TTS_UPSTREAM and/or ASR_UPSTREAM")
    if TTS_UPSTREAM and not VOICES:
        sys.exit(f"no reference voices (*.wav) in {VOICES_DIR}")
    port = int(os.environ.get("PORT", "8000"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
