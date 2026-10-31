"""Render caller speech with Qwen3-TTS on the CPU (inside ``srw-voice-qwen-tts:1``).

Run by ``scripts/voice_eval.py render`` in the TTS server's image with the
models folder mounted (``/opt/qwen-tts``), never on the GPU the cascade's
servers use. Reads a JSON list of jobs and writes 24 kHz mono WAVs:

- ``{"kind": "design", "instruct", "text", "seed", "out"}``: a new voice from
  a description (Qwen3-TTS 12Hz 1.7B VoiceDesign, Apache-2.0); the take
  becomes that voice's reference clip.
- ``{"kind": "clone", "ref_wav", "ref_text", "text", "seed", "out"}``: the
  text in a reference clip's voice (Qwen3-TTS 12Hz 0.6B Base, Apache-2.0).

    python3 qwen_worker.py JOBS.json --worker 0 --workers 2 --threads 8

Every job seeds torch first, so a job renders the same take on the same
machine and thread count. Prints one JSON line per finished job.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

MODELS = Path("/opt/qwen-tts")
DESIGN_MODEL = MODELS / "Qwen3-TTS-12Hz-1.7B-VoiceDesign"
BASE_MODEL = MODELS / "Qwen3-TTS-12Hz-0.6B-Base"
LANGUAGE = "German"


def max_tokens(text: str) -> int:
    """A ceiling for the codec frames (12.5 per second): runaway takes stop."""
    return int(12.5 * (3 + 0.12 * len(text)) * 1.5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jobs", type=Path)
    parser.add_argument("--worker", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()

    import soundfile
    import torch
    from qwen_tts import Qwen3TTSModel

    torch.set_num_threads(args.threads)
    jobs = json.loads(args.jobs.read_text(encoding="utf-8"))
    mine = [
        job for index, job in enumerate(jobs) if index % args.workers == args.worker
    ]
    models: dict[str, object] = {}
    prompts: dict[str, object] = {}

    def model(path: Path):
        if str(path) not in models:
            models[str(path)] = Qwen3TTSModel.from_pretrained(
                str(path), device_map="cpu", dtype=torch.float32
            )
        return models[str(path)]

    for job in mine:
        started = time.monotonic()
        torch.manual_seed(int(job["seed"]))
        text = job["text"]
        if job["kind"] == "design":
            wavs, rate = model(DESIGN_MODEL).generate_voice_design(
                text=text,
                instruct=job["instruct"],
                language=LANGUAGE,
                max_new_tokens=max_tokens(text),
            )
        elif job["kind"] == "clone":
            base = model(BASE_MODEL)
            key = job["ref_wav"]
            if key not in prompts:
                prompts[key] = base.create_voice_clone_prompt(
                    ref_audio=job["ref_wav"], ref_text=job["ref_text"]
                )
            torch.manual_seed(int(job["seed"]))
            wavs, rate = base.generate_voice_clone(
                text=text,
                language=LANGUAGE,
                voice_clone_prompt=prompts[key],
                max_new_tokens=max_tokens(text),
            )
        else:
            print(
                json.dumps({"out": job.get("out"), "error": "unknown kind"}), flush=True
            )
            continue
        out = Path(job["out"])
        out.parent.mkdir(parents=True, exist_ok=True)
        soundfile.write(str(out), wavs[0], rate, subtype="PCM_16")
        seconds = len(wavs[0]) / rate
        took = time.monotonic() - started
        print(
            json.dumps(
                {
                    "out": job["out"],
                    "audio_s": round(seconds, 2),
                    "took_s": round(took, 2),
                    "rtf": round(took / seconds, 2) if seconds else None,
                }
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
