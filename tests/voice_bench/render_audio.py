"""Render the bench WAVs and the German test set with Piper (never committed).

Outputs, under ``tests/voice_bench/.cache/audio`` (gitignored):

- ``bench/{dut,human,caller}.16k.wav`` and ``.8k-alaw.wav``: the three bench
  parties (texts and speakers in ``testset/de/voices.yaml``)
- ``testset/<id>.16k.wav`` and ``.8k-alaw.wav``: every ``utterances.yaml`` entry
- ``call/<hash>.16k.wav`` and ``.8k-alaw.wav``: free text lines of a call
  script (``call.py``), rendered on demand

A manifest skips texts that are already rendered with the same voice, speaker
and wording.
"""

from __future__ import annotations

import hashlib
import json

import yaml

from tests.voice_bench.assets import ASSETS
from tests.voice_bench.settings import AUDIO_DIR, TESTSET_DIR, Compose

FORMATS = (".16k.wav", ".8k-alaw.wav")


def load_voices() -> dict:
    return yaml.safe_load((TESTSET_DIR / "voices.yaml").read_text(encoding="utf-8"))


def load_utterances() -> list[dict]:
    data = yaml.safe_load((TESTSET_DIR / "utterances.yaml").read_text(encoding="utf-8"))
    return list(data["utterances"])


def planned_jobs() -> list[dict]:
    """Every (name, text, speaker) to render, name relative to AUDIO_DIR."""
    voices = load_voices()
    jobs = [
        {"name": f"bench/{role}", "text": spec["text"], "speaker": int(spec["speaker"])}
        for role, spec in voices["bench"].items()
    ]
    pool = [int(s) for s in voices["testset_speakers"]]
    for index, utterance in enumerate(load_utterances()):
        speaker = int(utterance.get("speaker", pool[index % len(pool)]))
        jobs.append(
            {
                "name": f"testset/{utterance['id']}",
                "text": utterance["text"],
                "speaker": speaker,
            }
        )
    return jobs


def _fingerprint(job: dict) -> str:
    voice_sha = next(a.sha256 for a in ASSETS if a.name.endswith(".onnx"))
    payload = json.dumps([voice_sha, job["speaker"], job["text"]], ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def text_job(text: str, speaker: int) -> dict:
    """A free text line (a call script's) as a render job under ``call/``;
    the name follows from the wording and the speaker."""
    digest = hashlib.sha256(f"{speaker}\n{text}".encode()).hexdigest()[:16]
    return {"name": f"call/{digest}", "text": text, "speaker": int(speaker)}


def render(
    compose: Compose, force: bool = False, log=print, jobs: list[dict] | None = None
) -> int:
    """Render what is missing or changed (default: the bench parties and the
    test set); returns the number of files rendered."""
    manifest_path = AUDIO_DIR / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    todo = []
    for job in planned_jobs() if jobs is None else jobs:
        fp = _fingerprint(job)
        outputs = [AUDIO_DIR / f"{job['name']}{ext}" for ext in FORMATS]
        if (
            not force
            and manifest.get(job["name"]) == fp
            and all(p.exists() for p in outputs)
        ):
            continue
        todo.append((job, fp))
    if not todo:
        return 0
    for job, _ in todo:
        (AUDIO_DIR / job["name"]).parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "text": job["text"],
                "speaker_id": job["speaker"],
                "output_file": f"/audio/{job['name']}.raw.wav",
            },
            ensure_ascii=False,
        )
        for job, _ in todo
    ]
    (AUDIO_DIR / "jobs.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"rendering {len(todo)} utterance(s) with Piper")
    compose.run(
        "--profile", "tools", "run", "--rm", "piper", "/audio/jobs.jsonl", timeout=900
    )
    for job, fp in todo:
        missing = [
            ext for ext in FORMATS if not (AUDIO_DIR / f"{job['name']}{ext}").exists()
        ]
        if missing:
            raise RuntimeError(f"Piper did not produce {job['name']}{missing[0]}")
        manifest[job["name"]] = fp
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return len(todo)
