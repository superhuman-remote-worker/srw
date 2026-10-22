"""Render the caller corpus from its text files into the gitignored cache.

    scripts/voice-eval.sh render [--workers 2 --threads 8] [--judge-url URL]

1. **Voices**: every ``qwen-design`` voice in ``voices.yaml`` says the
   reference text once with Qwen3-TTS VoiceDesign (its description, its
   seed); the take is the voice's reference clip.
2. **Pieces**: every distinct (voice, piece of text) the corpus and the
   scenarios need: Qwen3-TTS Base clones the reference clip, Piper speaks the
   ``piper`` voice. A piece is a stretch of caller speech between pauses.
   With a judge, a piece the judge cannot read back (WER over
   ``MAX_PIECE_WER``) is rendered again with the next seed, at most
   ``ATTEMPTS`` times; the best take stays and the manifest says which.
3. **Clips**: pieces resampled to 16 kHz, trimmed, sped up or slowed down
   (the voice's ``tempo``), brought to one speech level and joined with the
   text's pauses, plus a little silence before and after.

Qwen3-TTS runs on the CPU in the TTS server's own image
(``srw-voice-qwen-tts:1``, PyTorch from the models folder), so the GPU
servers of a cascade profile are never touched; Piper runs in the bench's
image. Phone conditions are applied later, in memory (``conditions.py``).
Everything lands in ``tests/voice_bench/.cache/evalset/`` with a manifest of
seeds, takes and hashes; real recordings from ``human/`` are converted next
to them. Nothing here is committed.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import wave
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from tests.voice_bench.evalset import conditions as C
from tests.voice_bench.evalset.spec import EvalSet, Voice, segments
from tests.voice_bench.settings import AUDIO_DIR, CACHE_DIR, REPO_ROOT

EVAL_CACHE = CACHE_DIR / "evalset"
VOICES_DIR = EVAL_CACHE / "voices"
PIECES_DIR = EVAL_CACHE / "pieces"
CLIPS_DIR = EVAL_CACHE / "clips"
HUMAN_CACHE = EVAL_CACHE / "human"
MANIFEST = EVAL_CACHE / "manifest.json"

QWEN_IMAGE = "srw-voice-qwen-tts:1"
DESIGN_MODEL = "Qwen3-TTS-12Hz-1.7B-VoiceDesign"
BASE_MODEL = "Qwen3-TTS-12Hz-0.6B-Base"
WORKER = Path(__file__).resolve().parent / "qwen_worker.py"
#: Speech level every clip is brought to (active speech, dBFS).
SPEECH_DBFS = -20.0
LEAD_S, TAIL_S = 0.3, 0.7
MAX_PIECE_WER = 0.34
ATTEMPTS = 3
#: Hard memory limits of a render container, and what the host keeps free.
DESIGN_MEMORY_GB = 12
CLONE_MEMORY_GB = 7
HOST_RESERVE_GB = 6


def available_gb() -> float:
    """The host's available memory (``MemAvailable``)."""
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024 / 1024
    return 0.0


def models_dir() -> Path:
    return Path(
        os.environ.get("VOICE_MODELS_DIR", REPO_ROOT / ".cache" / "voice-models")
    ).expanduser()


def _sha(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode()).hexdigest()[
        :16
    ]


# -- what to render ---------------------------------------------------------------


@dataclass(frozen=True)
class Clip:
    """One caller line in one voice: its pieces and the pauses between them."""

    id: str
    voice: str
    text: str
    silence: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.voice}--{_sha(self.voice, self.text, self.silence)}"


def piece_key(voice: Voice, text: str) -> str:
    if voice.engine == "piper":
        return f"{voice.id}--{_sha('piper', voice.speaker, text)}"
    return f"{voice.id}--{_sha('qwen', voice.instruct, voice.seed, text)}"


def corpus_clips(evalset: EvalSet) -> list[tuple[str, Clip]]:
    """(utterance id, clip): each utterance in its own voice, the sweep ones
    in every voice."""
    out = []
    for index, utt in enumerate(evalset.corpus.utterances.values()):
        own = utt.voice or evalset.voices.rotation[index % len(evalset.voices.rotation)]
        voices = [own]
        if utt.sweep:
            voices += [v for v in evalset.voices.voices if v != own]
        for voice in voices:
            out.append(
                (utt.id, Clip(f"{utt.id}__{voice}", voice, utt.text, utt.silence))
            )
    return out


def scenario_clips(evalset: EvalSet) -> list[tuple[str, int, Clip]]:
    out = []
    for scenario in evalset.scenarios.values():
        for turn in scenario.turns:
            voice = turn.voice or scenario.voice
            out.append(
                (
                    scenario.id,
                    turn.index,
                    Clip(
                        f"{scenario.id}__{turn.label}", voice, turn.text, turn.silence
                    ),
                )
            )
    return out


def needed_pieces(evalset: EvalSet) -> dict[str, tuple[Voice, str]]:
    clips = [c for _, c in corpus_clips(evalset)] + [
        c for *_, c in scenario_clips(evalset)
    ]
    pieces: dict[str, tuple[Voice, str]] = {}
    for clip in clips:
        if clip.silence:
            continue
        voice = evalset.voices.voices[clip.voice]
        for seg in segments(clip.text):
            pieces[piece_key(voice, seg.text)] = (voice, seg.text)
    return pieces


def clone_seed(voice: Voice, text: str, attempt: int) -> int:
    return voice.seed * 100_003 + zlib.crc32(text.encode()) % 100_000 + attempt * 7919


# -- audio helpers ------------------------------------------------------------------


def read_pcm(path: Path) -> tuple[np.ndarray, int]:
    with wave.open(str(path), "rb") as wav:
        if wav.getsampwidth() != 2:
            raise ValueError(f"{path.name}: 16-bit PCM expected")
        rate, channels = wav.getframerate(), wav.getnchannels()
        data = np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2")
    if channels > 1:
        data = data.reshape(-1, channels).mean(axis=1).astype(np.int16)
    return data.astype(np.int16), rate


def write_pcm(path: Path, pcm: np.ndarray, rate: int = C.RATE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        wav.writeframes(np.asarray(pcm, dtype="<i2").tobytes())


def wav_bytes(pcm: np.ndarray, rate: int = C.RATE) -> bytes:
    import io

    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        wav.writeframes(np.asarray(pcm, dtype="<i2").tobytes())
    return buffer.getvalue()


def trim(
    signal: np.ndarray,
    threshold_db: float = -45.0,
    pad_s: float = 0.05,
    rate: int = C.RATE,
) -> np.ndarray:
    """Leading and trailing silence off (relative to the loudest 20 ms)."""
    size = rate // 50
    frames = len(signal) // size
    if frames == 0:
        return signal
    power = np.mean(signal[: frames * size].reshape(frames, size) ** 2, axis=1)
    if power.max() <= 0:
        return signal[:0]
    loud = np.nonzero(power >= power.max() * 10 ** (threshold_db / 10))[0]
    pad = int(pad_s * rate)
    start = max(0, loud[0] * size - pad)
    end = min(len(signal), (loud[-1] + 1) * size + pad)
    return signal[start:end]


def tempo(signal: np.ndarray, factor: float) -> np.ndarray:
    """Pitch-preserving speed change (ffmpeg's atempo)."""
    if abs(factor - 1.0) < 1e-3:
        return signal
    pcm = C.to_int16(signal).tobytes()
    raw = ["-f", "s16le", "-ar", str(C.RATE), "-ac", "1"]
    out = C._ffmpeg(
        [*raw, "-i", "pipe:0", "-filter:a", f"atempo={factor}", *raw, "pipe:1"], pcm
    )
    return C.to_float(C.from_bytes(out))


def normalise(signal: np.ndarray, dbfs: float = SPEECH_DBFS) -> np.ndarray:
    level = C.active_level(signal)
    if level is None:
        return signal
    out = signal * (10 ** (dbfs / 20) / level)
    peak = np.max(np.abs(out)) if len(out) else 0
    return out * (0.98 / peak) if peak > 0.98 else out


def assemble(pieces: list[tuple[np.ndarray, float]]) -> np.ndarray:
    """Pieces (float, 16 kHz) with the pause after each, plus lead and tail."""
    parts = [np.zeros(int(LEAD_S * C.RATE))]
    for signal, pause in pieces:
        parts.append(signal)
        if pause:
            parts.append(np.zeros(int(pause * C.RATE)))
    parts.append(np.zeros(int(TAIL_S * C.RATE)))
    return np.concatenate(parts)


# -- the renderer -------------------------------------------------------------------


class Renderer:
    def __init__(
        self,
        evalset: EvalSet,
        *,
        workers: int = 2,
        threads: int = 8,
        judge: Callable[[bytes], str] | None = None,
        wer: Callable[[str, str], float] | None = None,
        compose=None,
        log=print,
    ):
        self.evalset = evalset
        self.workers = workers
        self.threads = threads
        self.judge = judge
        self.wer = wer
        self.compose = compose
        self.log = log
        self.manifest = (
            json.loads(MANIFEST.read_text("utf-8")) if MANIFEST.exists() else {}
        )
        self.manifest.setdefault("voices", {})
        self.manifest.setdefault("pieces", {})
        self.manifest.setdefault("clips", {})

    def save(self) -> None:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(
            json.dumps(self.manifest, indent=1, ensure_ascii=False, sort_keys=True),
            "utf-8",
        )

    # -- Qwen3-TTS in its image on the CPU
    def run_qwen(self, jobs: list[dict]) -> None:
        if not jobs:
            return
        EVAL_CACHE.mkdir(parents=True, exist_ok=True)
        root = models_dir() / "qwen-tts"
        for model in {j["model"] for j in jobs}:
            if not (root / model / "model.safetensors").exists():
                raise SystemExit(
                    f"{root / model} is missing: python docker/voice-models/models.py fetch --set rocm,design"
                )
        work = EVAL_CACHE / "jobs"
        work.mkdir(exist_ok=True)
        for job in jobs:
            job["out"] = "/cache/" + str(Path(job["out"]).relative_to(EVAL_CACHE))
            if "ref_wav" in job:
                job["ref_wav"] = "/cache/" + str(
                    Path(job["ref_wav"]).relative_to(EVAL_CACHE)
                )
        (work / "jobs.json").write_text(json.dumps(jobs, ensure_ascii=False), "utf-8")
        design = any(j["kind"] == "design" for j in jobs)
        # The 1.7B design model in float32 takes ~9 GB, a 0.6B clone worker
        # ~4 GB; each container gets a hard limit, and no more workers start
        # than the host's free memory allows with room to spare, so a render
        # never pushes other programs into the kernel's OOM killer.
        limit_gb = DESIGN_MEMORY_GB if design else CLONE_MEMORY_GB
        count = min(1 if design else self.workers, len(jobs))
        count = min(count, int((available_gb() - HOST_RESERVE_GB) // limit_gb))
        if count < 1:
            raise SystemExit(
                f"not enough free memory for a Qwen3-TTS worker ({available_gb():.0f} GB "
                f"available, {limit_gb + HOST_RESERVE_GB} GB needed)"
            )
        procs = []
        for worker in range(count):
            cmd = [
                "docker", "run", "--rm", "--network", "none",
                "--user", f"{os.getuid()}:{os.getgid()}",
                "--cpus", str(self.threads),
                "--memory", f"{limit_gb}g", "--memory-swap", f"{limit_gb}g",
                "-e", "HOME=/tmp", "-e", f"OMP_NUM_THREADS={self.threads}",
                "-v", f"{root}:/opt/qwen-tts:ro",
                "-v", f"{WORKER}:/worker.py:ro,z",
                "-v", f"{EVAL_CACHE}:/cache:z",
                "--entrypoint", "python3", QWEN_IMAGE, "/worker.py", "/cache/jobs/jobs.json",
                "--worker", str(worker), "--workers", str(count), "--threads", str(self.threads),
            ]  # fmt: skip
            procs.append(
                subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
                )
            )
        self.log(f"  {count} worker(s), {self.threads} threads and {limit_gb} GB each")
        done = 0
        for proc in procs:
            for line in proc.stdout:
                if line.startswith("{"):
                    done += 1
                    item = json.loads(line)
                    self.log(
                        f"  [{done}/{len(jobs)}] {Path(item['out']).name} {item.get('audio_s')} s, rtf {item.get('rtf')}"
                    )
            proc.wait()
        failed = [p.returncode for p in procs if p.returncode]
        if failed:
            raise SystemExit(f"a Qwen3-TTS worker failed (exit {failed[0]})")

    # -- step 1: the designed voices
    def design_voices(self, force: bool = False) -> None:
        jobs, todo = [], []
        for voice in self.evalset.voices.voices.values():
            if voice.engine != "qwen-design":
                continue
            fingerprint = self.design_fingerprint(voice, voice.seed)
            ref = VOICES_DIR / f"{voice.id}.wav"
            known = self.manifest["voices"].get(voice.id, {})
            if not force and known.get("fingerprint") == fingerprint and ref.exists():
                continue
            take = self.design_take(voice, voice.seed)
            if force or not take.exists():
                jobs.append(
                    {
                        "kind": "design",
                        "model": DESIGN_MODEL,
                        "instruct": voice.instruct,
                        "text": self.evalset.voices.reference_text,
                        "seed": voice.seed,
                        "out": str(take),
                    }
                )
            todo.append((voice, fingerprint))
        if not todo:
            return
        if jobs:
            self.log(
                f"designing {len(jobs)} voice(s) with Qwen3-TTS VoiceDesign on the CPU"
            )
            self.run_qwen(jobs)
        for voice, fingerprint in todo:
            raw, rate = read_pcm(self.design_take(voice, voice.seed))
            ref = trim(C.to_float(raw), pad_s=0.1, rate=rate)
            ref_path = VOICES_DIR / f"{voice.id}.wav"
            write_pcm(ref_path, C.to_int16(ref), rate)
            (VOICES_DIR / f"{voice.id}.txt").write_text(
                self.evalset.voices.reference_text, "utf-8"
            )
            entry = {
                "fingerprint": fingerprint,
                "seed": voice.seed,
                "model": DESIGN_MODEL,
                "seconds": round(len(ref) / rate, 2),
                "sha256": hashlib.sha256(ref_path.read_bytes()).hexdigest(),
                "pitch_hz": pitch(C.resample(ref, rate, C.RATE)),
                "words_per_s": round(
                    len(self.evalset.voices.reference_text.split()) / (len(ref) / rate),
                    2,
                ),
            }
            if self.judge:
                heard = self.judge(wav_bytes(C.to_int16(C.resample(ref, rate, C.RATE))))
                entry["judge_text"] = heard
                if self.wer:
                    entry["judge_wer"] = round(
                        self.wer(self.evalset.voices.reference_text, heard), 3
                    )
            self.manifest["voices"][voice.id] = entry
            self.log(
                f"  {voice.id}: {entry['seconds']} s, {entry['pitch_hz']} Hz, {entry['words_per_s']} words/s"
            )
        self.save()

    def design_fingerprint(self, voice: Voice, seed: int) -> str:
        text = self.evalset.voices.reference_text
        return _sha(DESIGN_MODEL, voice.instruct, seed, text)

    def design_take(self, voice: Voice, seed: int) -> Path:
        """Where the design take of a voice with a seed lives (a candidate
        from ``try_seeds`` is reused when the description did not change)."""
        fp = self.design_fingerprint(voice, seed)[:8]
        return VOICES_DIR / "candidates" / f"{voice.id}-{seed}-{fp}.wav"

    def try_seeds(self, voice_ids: list[str], seeds: list[int]) -> list[dict]:
        """Candidate takes for choosing a voice's seed: each voice's
        description with each seed, with pitch, pace and what the judge
        understood. Takes land in ``voices/candidates/``; nothing changes in
        voices.yaml or the manifest."""
        jobs, rows = [], []
        text = self.evalset.voices.reference_text
        for vid in voice_ids:
            voice = self.evalset.voices.voices[vid]
            for seed in seeds:
                out = self.design_take(voice, seed)
                if not out.exists():
                    jobs.append(
                        {
                            "kind": "design",
                            "model": DESIGN_MODEL,
                            "instruct": voice.instruct,
                            "text": text,
                            "seed": seed,
                            "out": str(out),
                        }
                    )
                rows.append({"voice": vid, "seed": seed, "path": out})
        self.run_qwen(jobs)
        for row in rows:
            raw, rate = read_pcm(row["path"])
            take = C.resample(trim(C.to_float(raw), rate=rate), rate, C.RATE)
            row["seconds"] = round(len(take) / C.RATE, 2)
            row["pitch_hz"] = pitch(take)
            row["words_per_s"] = (
                round(len(text.split()) / row["seconds"], 2) if row["seconds"] else None
            )
            if self.judge and self.wer:
                row["judge_wer"] = round(
                    self.wer(text, self.judge(wav_bytes(C.to_int16(take)))), 3
                )
            row["path"] = str(row["path"])
            self.log(
                f"  {row['voice']:14s} seed {row['seed']:5d}: {row['seconds']} s, "
                f"{row['pitch_hz']} Hz, {row['words_per_s']} words/s, judge WER {row.get('judge_wer')}"
            )
        return rows

    # -- step 2: the pieces
    def render_pieces(self, force: bool = False) -> None:
        needed = needed_pieces(self.evalset)
        qwen = {k: v for k, v in needed.items() if v[0].engine == "qwen-design"}
        piper = {k: v for k, v in needed.items() if v[0].engine == "piper"}
        self._render_piper(piper, force)
        for attempt in range(ATTEMPTS):
            todo = []
            for key, (voice, text) in qwen.items():
                entry = self.manifest["pieces"].get(key)
                path = PIECES_DIR / f"{key}.wav"
                if not force and entry and path.exists():
                    if entry.get("accepted") or entry.get("attempts", 0) > attempt:
                        continue
                if attempt > 0 and entry is None:
                    continue
                todo.append((key, voice, text))
            if not todo:
                continue
            force = False
            label = (
                "rendering" if attempt == 0 else f"re-rendering (attempt {attempt + 1})"
            )
            self.log(f"{label} {len(todo)} piece(s) with Qwen3-TTS Base on the CPU")
            jobs = [
                {
                    "kind": "clone",
                    "model": BASE_MODEL,
                    "ref_wav": str(VOICES_DIR / f"{voice.id}.wav"),
                    "ref_text": self.evalset.voices.reference_text,
                    "text": text,
                    "seed": clone_seed(voice, text, attempt),
                    "out": str(PIECES_DIR / f"{key}.try{attempt}.wav"),
                }
                for key, voice, text in todo
            ]
            self.run_qwen(jobs)
            for (key, voice, text), job in zip(todo, jobs, strict=True):
                self._accept(key, voice, text, attempt, job["seed"])
            self.save()

    def _accept(
        self, key: str, voice: Voice, text: str, attempt: int, seed: int
    ) -> None:
        take = PIECES_DIR / f"{key}.try{attempt}.wav"
        raw, rate = read_pcm(take)
        signal = trim(C.resample(C.to_float(raw), rate, C.RATE))
        entry = self.manifest["pieces"].get(key, {}) if attempt else {}
        result = {"seed": seed, "seconds": round(len(signal) / C.RATE, 2)}
        if self.judge and self.wer:
            heard = self.judge(wav_bytes(C.to_int16(signal)))
            result["judge_text"] = heard
            result["judge_wer"] = round(self.wer(text, heard), 3)
        best = entry.get("best")
        better = best is None or result.get("judge_wer", 0) < best.get("judge_wer", 1)
        if better:
            write_pcm(PIECES_DIR / f"{key}.wav", C.to_int16(signal))
            best = result
        take.unlink()
        accepted = best.get("judge_wer") is None or best["judge_wer"] <= MAX_PIECE_WER
        self.manifest["pieces"][key] = {
            "voice": voice.id,
            "text": text,
            "engine": "qwen-clone",
            "model": BASE_MODEL,
            "attempts": attempt + 1,
            "accepted": accepted or attempt + 1 >= ATTEMPTS,
            "judge_ok": accepted,
            "best": best,
            "sha256": hashlib.sha256(
                (PIECES_DIR / f"{key}.wav").read_bytes()
            ).hexdigest(),
        }

    def _render_piper(self, piper: dict[str, tuple[Voice, str]], force: bool) -> None:
        if not piper:
            return
        from tests.voice_bench import render_audio

        jobs = [
            {"name": f"evalset/{key}", "text": text, "speaker": int(voice.speaker)}
            for key, (voice, text) in piper.items()
        ]
        rendered = render_audio.render(
            self.compose, force=force, log=self.log, jobs=jobs
        )
        if rendered:
            self.log(f"rendered {rendered} piece(s) with Piper")
        for key, (voice, text) in piper.items():
            raw, rate = read_pcm(AUDIO_DIR / "evalset" / f"{key}.16k.wav")
            signal = trim(C.to_float(raw))
            path = PIECES_DIR / f"{key}.wav"
            entry = self.manifest["pieces"].get(key)
            if entry and path.exists() and not rendered:
                continue
            write_pcm(path, C.to_int16(signal))
            result: dict = {"seconds": round(len(signal) / C.RATE, 2)}
            if self.judge and self.wer:
                heard = self.judge(wav_bytes(C.to_int16(signal)))
                result.update(
                    judge_text=heard, judge_wer=round(self.wer(text, heard), 3)
                )
            self.manifest["pieces"][key] = {
                "voice": voice.id,
                "text": text,
                "engine": "piper",
                "speaker": voice.speaker,
                "attempts": 1,
                "accepted": True,
                "judge_ok": result.get("judge_wer") is None
                or result["judge_wer"] <= MAX_PIECE_WER,
                "best": result,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        self.save()

    # -- step 3: the clips
    def build_clips(self) -> int:
        built, missing = 0, 0
        clips = {c.key: c for _, c in corpus_clips(self.evalset)}
        clips.update({c.key: c for *_, c in scenario_clips(self.evalset)})
        for key, clip in clips.items():
            path = CLIPS_DIR / f"{key}.wav"
            voice = self.evalset.voices.voices[clip.voice]
            if clip.silence:
                parts = []
                pieces_sha = ["silence", clip.silence]
            else:
                parts, pieces_sha = [], []
                for seg in segments(clip.text):
                    pkey = piece_key(voice, seg.text)
                    entry = self.manifest["pieces"].get(pkey)
                    if entry is None:
                        break
                    pieces_sha.append(entry["sha256"])
                    parts.append((pkey, seg.pause_after))
                else:
                    entry = True
                if entry is None:
                    missing += 1
                    continue
            fingerprint = _sha(pieces_sha, voice.tempo, SPEECH_DBFS, LEAD_S, TAIL_S)
            if (
                path.exists()
                and self.manifest["clips"].get(key, {}).get("fingerprint")
                == fingerprint
            ):
                continue
            if clip.silence:
                signal = np.zeros(int(clip.silence * C.RATE))
            else:
                pieces = []
                for pkey, pause in parts:
                    raw, _ = read_pcm(PIECES_DIR / f"{pkey}.wav")
                    pieces.append((tempo(C.to_float(raw), voice.tempo), pause))
                level = C.active_level(np.concatenate([p for p, _ in pieces]))
                gain = 10 ** (SPEECH_DBFS / 20) / level if level else 1.0
                signal = assemble([(p * gain, pause) for p, pause in pieces])
                peak = np.max(np.abs(signal))
                if peak > 0.98:
                    signal *= 0.98 / peak
            write_pcm(path, C.to_int16(signal))
            self.manifest["clips"][key] = {
                "fingerprint": fingerprint,
                "voice": clip.voice,
                "text": clip.text,
                "seconds": round(len(signal) / C.RATE, 2),
            }
            built += 1
        self.save()
        if missing:
            self.log(f"{missing} clip(s) wait for pieces that are not rendered yet")
        return built

    def human(self) -> int:
        """Real recordings (``human/manifest.yaml``) as 16 kHz mono clips."""
        count = 0
        for rec in self.evalset.human:
            out = HUMAN_CACHE / f"{rec.id}.wav"
            if out.exists() and out.stat().st_mtime >= rec.file.stat().st_mtime:
                continue
            pcm = C._ffmpeg(
                [
                    "-i",
                    str(rec.file),
                    "-f",
                    "s16le",
                    "-ar",
                    str(C.RATE),
                    "-ac",
                    "1",
                    "pipe:1",
                ],
                b"",
            )
            write_pcm(out, C.from_bytes(pcm))
            count += 1
        return count


def clip_path(clip: Clip) -> Path:
    return CLIPS_DIR / f"{clip.key}.wav"


def load_clip(clip: Clip) -> np.ndarray:
    path = clip_path(clip)
    if not path.exists():
        raise SystemExit(
            f"{path.name} is missing: run scripts/voice-eval.sh render first"
        )
    pcm, rate = read_pcm(path)
    if rate != C.RATE:
        raise SystemExit(f"{path.name} is {rate} Hz")
    return pcm


def babble_sources(limit: int = 24) -> list[np.ndarray]:
    """The bench's Piper sentences (``voice-bench.sh audio``) for café babble."""
    found = sorted((AUDIO_DIR / "testset").glob("*.16k.wav"))[:limit]
    return [read_pcm(p)[0] for p in found]


def pitch(signal: np.ndarray, rate: int = C.RATE) -> float | None:
    """Median fundamental frequency of the voiced 40 ms frames (autocorrelation)."""
    size = int(0.04 * rate)
    values = []
    lo, hi = int(rate / 400), int(rate / 60)
    for start in range(0, len(signal) - size, size):
        frame = signal[start : start + size] - np.mean(signal[start : start + size])
        if np.sqrt(np.mean(frame**2)) < 0.02:
            continue
        corr = np.correlate(frame, frame, mode="full")[size - 1 :]
        if corr[0] <= 0:
            continue
        lag = lo + int(np.argmax(corr[lo:hi]))
        if corr[lag] / corr[0] > 0.4:
            values.append(rate / lag)
    return round(float(np.median(values)), 1) if values else None
