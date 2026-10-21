"""Fetch the pinned models of the `cascade-cpu` test profile; local_voice_gateway.md §6.2.

    python docker/voice-models/models.py fetch [--dir DIR] [--with-medium-stt]
    python docker/voice-models/models.py check [--dir DIR]

Every file comes from its official repository at a fixed revision and must
match the sha256 below. Files land in DIR (default ``.cache/voice-models`` at
the repo root, gitignored): ``llm/`` for llama.cpp, ``piper/`` for the TTS,
``vad/`` for Silero, ``hf/`` as a Hugging Face cache for Speaches' STT (downloaded by Speaches' own
``huggingface_hub`` in its pinned image, then marked as ``main`` so the
offline server finds it). Standard library only.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = REPO_ROOT / ".cache" / "voice-models"
SPEACHES_IMAGE = (
    "ghcr.io/speaches-ai/speaches:0.8.3-cpu"
    "@sha256:21e3df06d842fb7802ab470dd77c25f0e8c0d22950e8d8c6ae886e851af53ef8"
)


@dataclass(frozen=True)
class File:
    path: str  # relative to DIR
    url: str
    sha256: str
    licence: str


@dataclass(frozen=True)
class Snapshot:
    repo: str
    revision: str
    sha256: dict[str, str]  # large files and their hashes
    licence: str
    optional: bool = False


# The same pinned voice as the WP0 bench (tests/voice_bench/assets.py).
_PIPER = (
    "https://huggingface.co/rhasspy/piper-voices/resolve/"
    "5041a7a49ae5b3a0f30e6837769eb03fdbb231bd/de/de_DE/mls/medium"
)

FILES = (
    File(
        "llm/gemma-4-12b-it-qat-q4_0.gguf",
        "https://huggingface.co/google/gemma-4-12B-it-qat-q4_0-gguf/resolve/"
        "29d097773436b69ff9feafd636ab4cf873786537/gemma-4-12b-it-qat-q4_0.gguf",
        "93567e57a8fe10b23569b9d9ec38cd005deedf71e29477c421a4b83f418a538b",
        "Apache-2.0 (Gemma 4)",
    ),
    File(
        "piper/de_DE-mls-medium.onnx",
        f"{_PIPER}/de_DE-mls-medium.onnx",
        "69cd1d2aa5a35839a518966fcc4924b5f93e5f8c948ed0752b1a616ad53f65bf",
        "MIT (rhasspy/piper-voices); trained from scratch on MLS German, CC BY 4.0",
    ),
    File(
        "piper/de_DE-mls-medium.onnx.json",
        f"{_PIPER}/de_DE-mls-medium.onnx.json",
        "b0af1c89ddfdc72d32e015729b0e89b99eec13c2c8caa1db7488d98e9e570b40",
        "MIT (rhasspy/piper-voices)",
    ),
    File(
        "vad/silero_vad-v6.2.3.onnx",
        "https://raw.githubusercontent.com/snakers4/silero-vad/"
        "5cd7945676eb32225748052e2e6a0580e4686a08/src/silero_vad/data/silero_vad.onnx",
        "1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3",
        "MIT (Silero VAD v6.2.3)",
    ),
)

SNAPSHOTS = (
    Snapshot(
        "Systran/faster-whisper-small",
        "536b0662742c02347bc0e980a01041f333bce120",
        {
            "model.bin": "3e305921506d8872816023e4c273e75d2419fb89b24da97b4fe7bce14170d671"
        },
        "MIT (OpenAI Whisper small, CTranslate2 conversion by Systran)",
    ),
    Snapshot(
        "Systran/faster-whisper-medium",
        "08e178d48790749d25932bbc082711ddcfdfbc4f",
        {
            "model.bin": "9b45e1009dcc4ab601eff815b61d80e60ce3fd8c74c1a14f4a282258286b51ae"
        },
        "MIT (OpenAI Whisper medium, CTranslate2 conversion by Systran)",
        optional=True,
    ),
)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_file(root: Path, item: File) -> None:
    target = root / item.path
    if target.exists() and sha256_of(target) == item.sha256:
        print(f"ok   {item.path}")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(target.suffix + ".part")
    print(f"get  {item.path}")
    # curl resumes large downloads; urllib is the fallback.
    try:
        subprocess.run(
            ["curl", "-fsSL", "--retry", "20", "-C", "-", "-o", str(part), item.url],
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        urllib.request.urlretrieve(item.url, part)  # noqa: S310 - pinned https URL
    if sha256_of(part) != item.sha256:
        part.unlink()
        sys.exit(f"sha256 mismatch for {item.path}; download removed")
    part.rename(target)


def snapshot_dir(root: Path, snap: Snapshot) -> Path:
    return root / "hf" / ("models--" + snap.repo.replace("/", "--"))


def snapshot_ok(root: Path, snap: Snapshot) -> bool:
    files = snapshot_dir(root, snap) / "snapshots" / snap.revision
    return all(
        (files / name).exists() and sha256_of(files / name) == digest
        for name, digest in snap.sha256.items()
    )


def fetch_snapshot(root: Path, snap: Snapshot) -> None:
    if snapshot_ok(root, snap):
        print(f"ok   {snap.repo}@{snap.revision[:10]}")
    else:
        print(f"get  {snap.repo}@{snap.revision[:10]}")
        (root / "hf").mkdir(parents=True, exist_ok=True)
        code = (
            "from huggingface_hub import snapshot_download\n"
            f"snapshot_download(repo_id={snap.repo!r}, revision={snap.revision!r}, cache_dir='/hf')\n"
        )
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-e",
                "HOME=/tmp",
                "-e",
                "HF_HUB_DISABLE_TELEMETRY=1",
                "-v",
                f"{root / 'hf'}:/hf:z",
                "--entrypoint",
                "python",
                SPEACHES_IMAGE,
                "-c",
                code,
            ],
            check=True,
        )
        if not snapshot_ok(root, snap):
            sys.exit(
                f"sha256 mismatch for {snap.repo}; check {snapshot_dir(root, snap)}"
            )
    # Speaches resolves "main" offline; point it at the pinned revision.
    refs = snapshot_dir(root, snap) / "refs"
    refs.mkdir(exist_ok=True)
    (refs / "main").write_text(snap.revision)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("command", choices=["fetch", "check"])
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(os.environ.get("VOICE_MODELS_DIR", DEFAULT_DIR)),
    )
    parser.add_argument(
        "--with-medium-stt", action="store_true", help="also faster-whisper medium"
    )
    args = parser.parse_args()
    root = args.dir.resolve()
    snapshots = [s for s in SNAPSHOTS if not s.optional or args.with_medium_stt]
    if args.command == "check":
        bad = [
            f.path
            for f in FILES
            if not (root / f.path).exists() or sha256_of(root / f.path) != f.sha256
        ]
        bad += [s.repo for s in snapshots if not snapshot_ok(root, s)]
        print("\n".join(f"missing {b}" for b in bad) or f"all models present in {root}")
        sys.exit(1 if bad else 0)
    for item in FILES:
        fetch_file(root, item)
    for snap in snapshots:
        fetch_snapshot(root, snap)
    print(f"models in {root}")


if __name__ == "__main__":
    main()
