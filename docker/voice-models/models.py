"""Fetch the pinned models of the cascade test profiles; local_voice_gateway.md §6.2.

    python docker/voice-models/models.py fetch [--set cpu,gpu,judge] [--dir DIR] [--with-medium-stt]
    python docker/voice-models/models.py check [--set ...] [--dir DIR]

Every file comes from its official repository at a fixed revision and must
match the sha256 below. Files land in DIR (default ``.cache/voice-models`` at
the repo root, gitignored): ``llm/`` for llama.cpp, ``piper/`` for the TTS,
``vad/`` for Silero, ``hf/`` as a Hugging Face cache for Speaches' STT (downloaded by Speaches' own
``huggingface_hub`` in its pinned image, then marked as ``main`` so the
offline server finds it). Standard library only.

Sets: ``cpu`` (default) is the ``cascade-cpu`` profile with the voice LLM;
``gpu`` adds the ``cascade-gpu`` stages (``stt/`` whisper-large-v3-turbo for
whisper.cpp, ``tts/`` Qwen3-TTS for llama.cpp; WP4); ``rocm`` the streamed
Qwen3-TTS for AMD GPUs (``qwen-tts/``: the 0.6B Base model, the PyTorch ROCm
wheel, and ``runtime/``, the pinned Python packages of
``qwen-tts-rocm.requirements.txt`` installed there by pip in a pinned
Python image); ``judge`` the bench's transcription judge (``judge/``
Qwen3-ASR, another model family than the STT it judges).
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
    group: str = "cpu"  # the model set it belongs to


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

_WHISPER = (
    "https://huggingface.co/ggerganov/whisper.cpp/resolve/"
    "5359861c739e955e79d9a303bcbc70fb988958b1"
)
_QWEN_TTS = (
    "https://huggingface.co/ggml-org/Qwen3-TTS-12Hz-1.7B-Base-GGUF/resolve/"
    "ca27d74bc954b73dadab5b71ca265d87fc861a7c"
)
_QWEN_ASR = (
    "https://huggingface.co/ggml-org/Qwen3-ASR-0.6B-GGUF/resolve/"
    "928ab958557df9aa2ef1c93e0e83c7ad0933fae2"
)

FILES += (
    File(
        "stt/ggml-large-v3-turbo-q8_0.bin",
        f"{_WHISPER}/ggml-large-v3-turbo-q8_0.bin",
        "317eb69c11673c9de1e1f0d459b253999804ec71ac4c23c17ecf5fbe24e259a1",
        "MIT (OpenAI Whisper large-v3-turbo, ggml conversion by ggerganov)",
        "gpu",
    ),
    File(
        "tts/Qwen3-TTS-12Hz-1.7B-Base-Q8_0.gguf",
        f"{_QWEN_TTS}/Qwen3-TTS-12Hz-1.7B-Base-Q8_0.gguf",
        "ac7931aeb2e7aad1a6ed6602d353a5679c9d096b18ce8204ac730a8408d572e1",
        "Apache-2.0 (Qwen/Qwen3-TTS-12Hz-1.7B-Base, GGUF by ggml-org)",
        "gpu",
    ),
    File(
        "tts/mmproj-Qwen3-TTS-12Hz-1.7B-Base-Q8_0.gguf",
        f"{_QWEN_TTS}/mmproj-Qwen3-TTS-12Hz-1.7B-Base-Q8_0.gguf",
        "6fd65188839bcd6ecc91b277ad471e22a0edfada4699a0fe82f1165c18cfcce2",
        "Apache-2.0 (Qwen3-TTS 12 Hz speech tokenizer, GGUF by ggml-org)",
        "gpu",
    ),
    File(
        "judge/Qwen3-ASR-0.6B-Q8_0.gguf",
        f"{_QWEN_ASR}/Qwen3-ASR-0.6B-Q8_0.gguf",
        "bca259818b50ca7c4c05e9bdb35a5dc04fa039653a6d6f3f0f331f96f6aa1971",
        "Apache-2.0 (Qwen/Qwen3-ASR-0.6B, GGUF by ggml-org)",
        "judge",
    ),
    File(
        "judge/mmproj-Qwen3-ASR-0.6B-Q8_0.gguf",
        f"{_QWEN_ASR}/mmproj-Qwen3-ASR-0.6B-Q8_0.gguf",
        "41a342b5e4c514e968cb756de6cd1b7be39eff43c44c57a2ef5fc6522e36603d",
        "Apache-2.0 (Qwen3-ASR audio encoder, GGUF by ggml-org)",
        "judge",
    ),
)

_QWEN_TTS_06 = (
    "https://huggingface.co/Qwen/Qwen3-TTS-12Hz-0.6B-Base/resolve/"
    "5d83992436eae1d760afd27aff78a71d676296fc"
)
_QWEN_TTS_06_FILES = {
    "config.json": "2e714c787c8edb98b05432685cddb634add2de4d4e645f653d68251ef72ba011",
    "generation_config.json": "f1b90b4513f3b34c62851049e2492d7b4c5940daf1276f89c82b8ef04127f3aa",
    "merges.txt": "599bab54075088774b1733fde865d5bd747cbcc7a547c5bc12610e874e26f5e3",
    "preprocessor_config.json": "efdde1022ea9d76928bf7a9cd53139138f5ba2e466e837f08f6105ab1af1c119",
    "tokenizer_config.json": "dc3c31c3bdaedd5016382bb3cbe07323026775ad51f5a4fb564505992ae4a670",
    "vocab.json": "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910",
    "model.safetensors": "180b3b10eb1c9f1b4db7806d5475bae3071c0243c299d49926bab1da3b6946f6",
    "speech_tokenizer/config.json": "ee65bb901c876664ab8707c487157aa1a6ee57c65969b28fb5ec9dc211e68167",
    "speech_tokenizer/configuration.json": "6bc26d64eb5024b4d1dab5a52371958b429256d6c9d59787f1f5294a54e0cebd",
    "speech_tokenizer/preprocessor_config.json": "fcb3805e597e786d4067706e602f6688524640f8d3396790e2e09b5942fcbdfb",
    "speech_tokenizer/model.safetensors": "836b7b357f5ea43e889936a3709af68dfe3751881acefe4ecf0dbd30ba571258",
}
TORCH_ROCM_WHEEL = "torch-2.14.1+rocm7.2-cp312-cp312-manylinux_2_28_x86_64.whl"

FILES += tuple(
    File(
        f"qwen-tts/Qwen3-TTS-12Hz-0.6B-Base/{name}",
        f"{_QWEN_TTS_06}/{name}",
        digest,
        "Apache-2.0 (Qwen/Qwen3-TTS-12Hz-0.6B-Base)",
        "rocm",
    )
    for name, digest in _QWEN_TTS_06_FILES.items()
) + (
    File(
        f"qwen-tts/wheels/{TORCH_ROCM_WHEEL}",
        "https://download-r2.pytorch.org/whl/rocm7.2/torch-2.14.1%2Brocm7.2-cp312-cp312-manylinux_2_28_x86_64.whl",
        "fc35e48fd83329f5d60ab925951d138edb5a6fce84323600703637fbc693541c",
        "BSD-3-Clause and others (PyTorch 2.14.1, ROCm 7.2 build with ROCm libraries)",
        "rocm",
    ),
)
#: The Python image the server image builds on; pip runs in it, too.
PYTHON_IMAGE = (
    "python:3.12-slim"
    "@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea"
)
ROCM_REQUIREMENTS = Path(__file__).resolve().parent / "qwen-tts-rocm.requirements.txt"

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
    # curl resumes large downloads, also after a stalled connection (under
    # 10 kB/s for a minute counts as a timeout, which --retry retries);
    # urllib is the fallback.
    try:
        subprocess.run(
            [
                "curl",
                "-fsSL",
                "--retry",
                "20",
                "--speed-limit",
                "10000",
                "--speed-time",
                "60",
                "-C",
                "-",
                "-o",
                str(part),
                item.url,
            ],
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


def install_rocm_runtime(root: Path) -> None:
    """The Qwen3-TTS server's packages into ``qwen-tts/runtime``: torch from
    the fetched wheel, the rest pinned, all without dependency resolution."""
    runtime = root / "qwen-tts" / "runtime"
    stamp = runtime / ".requirements.sha256"
    wanted = sha256_of(ROCM_REQUIREMENTS) + " " + TORCH_ROCM_WHEEL
    if stamp.exists() and stamp.read_text().strip() == wanted:
        print("ok   qwen-tts/runtime")
        return
    print("get  qwen-tts/runtime (pip, a few minutes)")
    runtime.mkdir(parents=True, exist_ok=True)
    pip = "pip install -q --no-cache-dir --no-deps --target /runtime"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            "HOME=/tmp",
            "-v",
            f"{root / 'qwen-tts' / 'wheels'}:/wheels:ro,z",
            "-v",
            f"{ROCM_REQUIREMENTS}:/requirements.txt:ro,z",
            "-v",
            f"{runtime}:/runtime:z",
            PYTHON_IMAGE,
            "sh",
            "-c",
            f"{pip} /wheels/{TORCH_ROCM_WHEEL} && {pip} -r /requirements.txt",
        ],
        check=True,
    )
    stamp.write_text(wanted + "\n")


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
    parser.add_argument(
        "--set",
        default="cpu",
        help="comma-separated model sets: cpu (default), gpu, rocm, judge, or all",
    )
    args = parser.parse_args()
    root = args.dir.resolve()
    known = {"cpu", "gpu", "rocm", "judge"}
    sets = known if args.set == "all" else set(args.set.split(","))
    unknown = sets - known
    if unknown:
        sys.exit(f"unknown model set(s): {', '.join(sorted(unknown))}")
    files = [f for f in FILES if f.group in sets]
    # Speaches' snapshots serve the CPU profile's STT.
    snapshots = [
        s
        for s in SNAPSHOTS
        if "cpu" in sets and (not s.optional or args.with_medium_stt)
    ]
    if args.command == "check":
        bad = [
            f.path
            for f in files
            if not (root / f.path).exists() or sha256_of(root / f.path) != f.sha256
        ]
        bad += [s.repo for s in snapshots if not snapshot_ok(root, s)]
        print("\n".join(f"missing {b}" for b in bad) or f"all models present in {root}")
        sys.exit(1 if bad else 0)
    for item in files:
        fetch_file(root, item)
    for snap in snapshots:
        fetch_snapshot(root, snap)
    if "rocm" in sets:
        install_rocm_runtime(root)
    print(f"models in {root}")


if __name__ == "__main__":
    main()
