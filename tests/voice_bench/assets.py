"""Pinned model downloads for the WP0 voice bench.

Every file comes from its official Hugging Face repository at a fixed commit
and must match the sha256 below. Files land in ``tests/voice_bench/.cache/
models`` (gitignored); a file that is already there and matches is reused.
"""

from __future__ import annotations

import hashlib
import os
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Asset:
    name: str
    url: str
    sha256: str
    licence: str


PIPER_VOICES_REVISION = "5041a7a49ae5b3a0f30e6837769eb03fdbb231bd"
WHISPER_CPP_MODELS_REVISION = "5359861c739e955e79d9a303bcbc70fb988958b1"

_PIPER_BASE = (
    f"https://huggingface.co/rhasspy/piper-voices/resolve/{PIPER_VOICES_REVISION}"
    "/de/de_DE/mls/medium"
)

ASSETS: tuple[Asset, ...] = (
    Asset(
        name="de_DE-mls-medium.onnx",
        url=f"{_PIPER_BASE}/de_DE-mls-medium.onnx",
        sha256="69cd1d2aa5a35839a518966fcc4924b5f93e5f8c948ed0752b1a616ad53f65bf",
        licence="MIT (rhasspy/piper-voices); trained from scratch on MLS, CC BY 4.0",
    ),
    Asset(
        name="de_DE-mls-medium.onnx.json",
        url=f"{_PIPER_BASE}/de_DE-mls-medium.onnx.json",
        sha256="b0af1c89ddfdc72d32e015729b0e89b99eec13c2c8caa1db7488d98e9e570b40",
        licence="MIT (rhasspy/piper-voices)",
    ),
    Asset(
        name="ggml-small.bin",
        url=(
            "https://huggingface.co/ggerganov/whisper.cpp/resolve/"
            f"{WHISPER_CPP_MODELS_REVISION}/ggml-small.bin"
        ),
        sha256="1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b",
        licence="MIT (OpenAI Whisper small, ggml conversion by whisper.cpp)",
    ),
)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(models_dir: Path, log=print) -> None:
    """Download every asset that is missing or does not match its checksum."""
    models_dir.mkdir(parents=True, exist_ok=True)
    for asset in ASSETS:
        target = models_dir / asset.name
        if target.exists() and sha256_of(target) == asset.sha256:
            continue
        log(f"downloading {asset.name}")
        part = target.with_suffix(target.suffix + ".part")
        request = urllib.request.Request(
            asset.url, headers={"User-Agent": "srw-voice-bench"}
        )
        with (
            urllib.request.urlopen(request, timeout=60) as response,
            part.open("wb") as out,
        ):
            while chunk := response.read(1 << 20):
                out.write(chunk)
        actual = sha256_of(part)
        if actual != asset.sha256:
            part.unlink()
            raise RuntimeError(
                f"{asset.name}: sha256 {actual} does not match the pinned {asset.sha256}"
            )
        os.replace(part, target)
