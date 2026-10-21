"""The OpenAI audio adapter over llama-server's speech models (WP4,
local_voice_gateway.md §4.3, §6.2): streamed TTS, ASR, and nothing on disk."""

from __future__ import annotations

import base64
import http.client
import importlib.util
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ADAPTER = (
    Path(__file__).resolve().parent.parent
    / "docker"
    / "voice-models"
    / "audio_adapter.py"
)


@pytest.fixture
def adapter(monkeypatch, tmp_path):
    monkeypatch.setenv("VOICES_DIR", str(tmp_path / "none"))
    spec = importlib.util.spec_from_file_location("audio_adapter_under_test", ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Upstream(BaseHTTPRequestHandler):
    """llama-server's /tts (float32 PCM, streamed) and chat completions."""

    protocol_version = "HTTP/1.1"
    seen: list = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        Upstream.seen.append((self.path, body))
        if self.path == "/tts":
            samples = struct.pack("<5f", 0.0, 0.5, -0.5, 1.5, -1.5)
            self.send_response(200)
            self.send_header(
                "Content-Type", "audio/pcm;rate=24000;encoding=float;bits=32"
            )
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            # Split mid-sample: the adapter must carry the remainder over.
            for piece in (samples[:6], samples[6:]):
                self.wfile.write(f"{len(piece):x}\r\n".encode() + piece + b"\r\n")
            self.wfile.write(b"0\r\n\r\n")
        else:
            message = {"role": "assistant", "content": "Guten Tag."}
            data = json.dumps({"choices": [{"message": message}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)


def serve(handler) -> tuple[ThreadingHTTPServer, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def test_helpers(adapter):
    assert adapter.float_to_pcm16(struct.pack("<3f", 0.0, 1.0, -2.0)) == struct.pack(
        "<3h", 0, 32767, -32767
    )
    header = adapter.wav_header(24_000)
    assert header[:4] == b"RIFF" and header[8:16] == b"WAVEfmt "
    assert struct.unpack("<I", header[24:28])[0] == 24_000
    assert adapter.upstream_rate("audio/pcm;rate=22050;encoding=float") == 22_050
    assert adapter.asr_text("language German<asr_text> Guten Tag.") == "Guten Tag."
    assert adapter.asr_text("Guten Tag.") == "Guten Tag."


def test_speech_streams_pcm16_from_the_voice_reference(adapter):
    upstream, up_port = serve(Upstream)
    adapter.TTS_UPSTREAM = f"http://127.0.0.1:{up_port}"
    adapter.VOICES = {"de-1": base64.b64encode(b"RIFFref").decode()}
    server, port = serve(adapter.Handler)
    try:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "POST",
            "/v1/audio/speech",
            body=json.dumps({"model": "qwen3-tts", "input": "Hallo.", "voice": "de-1"}),
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Type") == "audio/pcm;rate=24000"
        assert response.read() == struct.pack("<5h", 0, 16383, -16383, 32767, -32767)
        path, body = Upstream.seen[-1]
        assert path == "/tts" and body["stream"] is True
        assert (
            body["lang"] == "de" and body["speaker_ref_b64"] == adapter.VOICES["de-1"]
        )
        connection.request(
            "POST",
            "/v1/audio/speech",
            body=json.dumps({"input": "Hallo.", "voice": "unknown"}),
            headers={"Content-Type": "application/json"},
        )
        refused = connection.getresponse()
        assert refused.status == 400 and b"unknown_voice" in refused.read()
    finally:
        server.shutdown()
        upstream.shutdown()


def test_transcription_forces_the_language_and_keeps_the_text(adapter):
    upstream, up_port = serve(Upstream)
    adapter.ASR_UPSTREAM = f"http://127.0.0.1:{up_port}"
    server, port = serve(adapter.Handler)
    boundary = "b0undary"
    form = (
        (
            f'--{boundary}\r\nContent-Disposition: form-data; name="language"\r\n\r\nde\r\n'
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            'filename="x.wav"\r\nContent-Type: audio/wav\r\n\r\n'
        ).encode()
        + b"RIFF\x00\x01wav"
        + f"\r\n--{boundary}--\r\n".encode()
    )
    try:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "POST",
            "/v1/audio/transcriptions",
            body=form,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        response = connection.getresponse()
        assert response.status == 200
        assert json.loads(response.read()) == {"text": "Guten Tag."}
        path, body = Upstream.seen[-1]
        assert path == "/v1/chat/completions"
        audio = body["messages"][0]["content"][0]["input_audio"]["data"]
        assert base64.b64decode(audio) == b"RIFF\x00\x01wav"
        assert body["messages"][1] == {
            "role": "assistant",
            "content": "language German<asr_text>",
        }
    finally:
        server.shutdown()
        upstream.shutdown()


def test_nothing_touches_the_disk():
    source = ADAPTER.read_text(encoding="utf-8")
    assert "open(" not in source.replace("read_bytes()", "")
    assert "tempfile" not in source and "write_bytes" not in source
