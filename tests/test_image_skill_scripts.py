"""The image skill's scripts (image generation WP2) against a fake provider.

imagegen.py and batch.py run as subprocesses, as an agent runs them in a
workspace, against a stdlib HTTP server on 127.0.0.1 that speaks the OpenAI
Images API. The spend cap, retries, concurrency, sidecars and type detection
are the guarantees the shell lane has to keep on its own, since nothing in
SRW caps a user's own key.
"""

from __future__ import annotations

import base64
import io
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from PIL import Image

SCRIPTS = (
    Path(__file__).resolve().parents[1] / "config" / "skills" / "image" / "scripts"
)


def _image(fmt: str) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (32, 24), (200, 30, 30)).save(out, format=fmt)
    return out.getvalue()


PNG, JPEG = _image("PNG"), _image("JPEG")


class FakeProvider:
    """OpenAI Images API on 127.0.0.1: records requests and concurrency."""

    def __init__(self, *, delay=0.0, fail_first=0, image=PNG):
        self.delay = delay
        self.fail_first = fail_first
        self.image = image
        self.requests: list[dict] = []
        self.active = 0
        self.peak = 0
        self.lock = threading.Lock()
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with provider.lock:
                    provider.requests.append(
                        {
                            "path": self.path,
                            "auth": self.headers.get("Authorization"),
                            **body,
                        }
                    )
                    provider.active += 1
                    provider.peak = max(provider.peak, provider.active)
                    failing = provider.fail_first > 0
                    if failing:
                        provider.fail_first -= 1
                try:
                    time.sleep(provider.delay)
                    if failing:
                        self.send_response(503)
                        self.end_headers()
                        self.wfile.write(b'{"error":{"message":"busy"}}')
                        return
                    data = [
                        {"b64_json": base64.b64encode(provider.image).decode()}
                    ] * int(body.get("n", 1))
                    payload = json.dumps(
                        {"data": data, "usage": {"output_tokens": 100}}
                    ).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                finally:
                    with provider.lock:
                        provider.active -= 1

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def provider_factory():
    made = []

    def make(**kwargs):
        provider = FakeProvider(**kwargs)
        made.append(provider)
        return provider

    yield make
    for provider in made:
        provider.close()


def _run(script: str, args: list[str], cwd: Path, provider: FakeProvider):
    env = {
        **{
            k: v
            for k, v in os.environ.items()
            if k not in ("OPENAI_API_KEY", "GEMINI_API_KEY")
        },
        "OPENAI_API_KEY": "sk-test",
        "IMAGEGEN_BASE_URL": provider.url,
        "IMAGEGEN_RETRY_BASE": "0.01",
    }
    return subprocess.run(
        [sys.executable, "-I", "-B", str(SCRIPTS / script), *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _manifest(tmp_path: Path, count: int) -> Path:
    path = tmp_path / "manifest.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "prompt": f"shot {i}",
                    "out": f"output/set/{i:03d}.png",
                    "quality": "low",
                }
            )
            for i in range(count)
        )
    )
    return path


def test_imagegen_writes_the_image_and_its_sidecar(tmp_path, provider_factory):
    provider = provider_factory()
    done = _run(
        "imagegen.py",
        ["a red fox", "--out", "output/images/fox.png", "--n", "2", "--size", "16:9"],
        tmp_path,
        provider,
    )
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "output/images/fox-1.png").read_bytes() == PNG
    sidecar = json.loads(
        (tmp_path / ".provenance/output/images/fox-2.png.json").read_text()
    )
    assert sidecar["prompt"] == "a red fox" and sidecar["billing"] == "user key"
    request = provider.requests[0]
    assert request["path"] == "/v1/images/generations"
    assert request["auth"] == "Bearer sk-test"
    assert request["size"] == "1536x1024" and request["n"] == 2


def test_the_type_comes_from_the_bytes_not_the_name(tmp_path, provider_factory):
    provider = provider_factory(image=JPEG)
    done = _run("imagegen.py", ["x", "--out", "output/a.png"], tmp_path, provider)
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "output/a.jpg").read_bytes() == JPEG
    assert not (tmp_path / "output/a.png").exists()


def test_batch_refuses_without_a_spend_cap(tmp_path, provider_factory):
    provider = provider_factory()
    done = _run(
        "batch.py",
        [str(_manifest(tmp_path, 2)), "--price-per-image", "0.04"],
        tmp_path,
        provider,
    )
    assert done.returncode != 0 and "--max-spend" in done.stderr
    assert provider.requests == []


def test_the_spend_cap_stops_the_run(tmp_path, provider_factory):
    provider = provider_factory()
    done = _run(
        "batch.py",
        [
            str(_manifest(tmp_path, 5)),
            "--max-spend",
            "0.10",
            "--price-per-image",
            "0.04",
            "--concurrency",
            "1",
        ],
        tmp_path,
        provider,
    )
    assert done.returncode == 2, done.stderr
    summary = json.loads(done.stdout)
    assert summary["images"] == 2 and len(summary["not_run_spend_cap"]) == 3
    assert len(provider.requests) == 2
    ledger = [
        json.loads(line)
        for line in (tmp_path / ".provenance/batch-ledger.jsonl")
        .read_text()
        .splitlines()
    ]
    assert [entry["status"] for entry in ledger] == ["ok", "ok"]
    assert all(entry["estimated_usd"] == 0.04 for entry in ledger)
    assert len(list((tmp_path / ".provenance/output/set").glob("*.json"))) == 2


def test_a_second_run_continues_where_the_first_stopped(tmp_path, provider_factory):
    provider = provider_factory()
    manifest = str(_manifest(tmp_path, 3))
    _run(
        "batch.py",
        [manifest, "--max-spend", "0.04", "--price-per-image", "0.04"],
        tmp_path,
        provider,
    )
    again = _run(
        "batch.py",
        [manifest, "--max-spend", "1", "--price-per-image", "0.04"],
        tmp_path,
        provider,
    )
    assert again.returncode == 0, again.stderr
    summary = json.loads(again.stdout)
    assert summary["skipped_existing"] == 1 and summary["images"] == 2
    assert len(provider.requests) == 3  # nothing was generated twice


def test_busy_answers_are_retried_and_each_attempt_is_paid_for(
    tmp_path, provider_factory
):
    provider = provider_factory(fail_first=2)
    done = _run(
        "batch.py",
        [
            str(_manifest(tmp_path, 1)),
            "--max-spend",
            "1",
            "--price-per-image",
            "0.04",
            "--retries",
            "2",
        ],
        tmp_path,
        provider,
    )
    assert done.returncode == 0, done.stderr
    summary = json.loads(done.stdout)
    assert summary["images"] == 1
    assert summary["estimated_spend_usd"] == pytest.approx(0.12)  # three attempts sent
    statuses = [
        json.loads(line)["status"]
        for line in (tmp_path / ".provenance/batch-ledger.jsonl")
        .read_text()
        .splitlines()
    ]
    assert statuses == ["error", "error", "ok"]


def test_concurrency_is_capped(tmp_path, provider_factory):
    provider = provider_factory(delay=0.2)
    done = _run(
        "batch.py",
        [
            str(_manifest(tmp_path, 6)),
            "--max-spend",
            "1",
            "--price-per-image",
            "0.01",
            "--concurrency",
            "2",
        ],
        tmp_path,
        provider,
    )
    assert done.returncode == 0, done.stderr
    assert provider.peak == 2


def test_contact_sheet_numbers_and_splits_a_set(tmp_path):
    for i in range(5):
        Image.new("RGB", (300, 200), (40 * i, 80, 120)).save(tmp_path / f"{i}.png")
    done = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPTS / "contact_sheet.py"),
            *[str(tmp_path / f"{i}.png") for i in range(5)],
            "--out",
            str(tmp_path / "review/sheet.png"),
            "--per-sheet",
            "4",
            "--cell",
            "128",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    first, second = (Image.open(tmp_path / f"review/sheet-0{n}.png") for n in (1, 2))
    assert first.width > 2 * 128 and second.width < first.width
