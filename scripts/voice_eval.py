"""The voice system's end-to-end test set; tests/voice_bench/evalset/README.md.

    scripts/voice-eval.sh render                      # the caller corpus (CPU)
    scripts/voice-eval.sh run --profile-file docker/voice-models/cascade-gpu.yaml \\
        --profile cascade-gpu-rocm --levels stt,file,call --judge-url http://127.0.0.1:18495/v1
    scripts/voice-eval.sh diff RUN_DIR                # against the stored baseline
    scripts/voice-eval.sh compare RUN_A RUN_B ...     # profiles side by side

Levels:

- ``stt``: every corpus clip under every phone condition through the
  profile's STT, with the engine's own STT code (``TranscriptionSTT``): word
  error rate and exact names, numbers, dates per category, condition, voice.
- ``file``: every scenario as a call on the gateway's real ``Session`` and
  ``CascadeEngine`` with a file-backed phone line (``voice_cascade_try.py``'s
  harness), the caller's lines after the phone condition and its background
  noise between them, and a canned back office: decisions, values in the reply
  and the delegated request, turn taking, wording, stage timings, and what
  the caller heard (``--judge-url``).
- ``call``: every scenario through the bench PBX (``voice-bench.sh``'s addon
  mode), a gateway on the host with the profile and ``srw_driver.py`` as the
  canned back office: the same scores from the driver's log, the gateway's
  turn timings and the caller's recording.

Each run writes ``stt.json``, ``file.json``, ``call.json``, ``summary.json``
and ``summary.md`` into ``tests/voice_bench/results/eval-<UTC>-<profile>/``
(gitignored), and ``diff.md`` against ``tests/voice_bench/evalset/baselines/
<profile>.json`` when there is one. Needs the voice environment (Pipecat):
the wrapper picks ``.venv-voice`` or ``SRW_VOICE_PYTHON``.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import base64
import datetime as dt
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.voice_bench.evalset import conditions as C  # noqa: E402
from tests.voice_bench.evalset import render as R  # noqa: E402
from tests.voice_bench.evalset import report as Rep  # noqa: E402
from tests.voice_bench.evalset import scoring as S  # noqa: E402
from tests.voice_bench.evalset import spec  # noqa: E402

RESULTS = REPO_ROOT / "tests" / "voice_bench" / "results"
#: The bench run's own compose project and subnet, apart from any other bench.
COMPOSE_PROJECT = "srw-voice-eval"
SUBNET = "172.30.98.0/24"
GATEWAY_LISTEN = "127.0.0.1:18540"
DRIVER_LISTEN = "127.0.0.1:18590"
JUDGE_COMPOSE = REPO_ROOT / "tests" / "voice_bench" / "evalset" / "judge-cpu.yaml"
VOICE_MODELS_COMPOSE = REPO_ROOT / "docker" / "voice-models" / "compose.yaml"
JUDGE_PROJECT = "srw-voice-eval-judge"
NOISE_NOTE = (
    "timings come from model servers other clients may share; while they do, "
    "the latency numbers are noisy"
)


def log(text: str) -> None:
    print(f"[voice-eval] {text}", flush=True)


# -- shared helpers -------------------------------------------------------------------


def load_settings(args):
    import voice_cascade_testset as testset

    from voice_gateway.engines.cascade import parse_settings

    raw = testset.load_profile(args.profile_file, args.profile)
    return parse_settings(args.profile, raw), raw


def bench_wer(reference: str, hypothesis: str) -> float:
    """The model bench's number-aware word error rate (voice_models_bench.py)."""
    from voice_models_bench import wer

    return wer(reference, hypothesis)


def judge_fn(args):
    if not getattr(args, "judge_url", None):
        return None
    from tests.voice_bench.call import judge_text

    url, model = args.judge_url, args.judge_model
    return lambda wav: judge_text(url, model, wav)


def run_dir(args) -> Path:
    if getattr(args, "out", None):
        out = Path(args.out).resolve()
    else:
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        out = RESULTS / f"eval-{stamp}-{args.profile}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def git_state() -> str:
    def git(*a):
        return subprocess.run(
            ["git", *a], cwd=REPO_ROOT, capture_output=True, text=True, check=False
        )

    rev = git("rev-parse", "--short", "HEAD").stdout.strip() or "?"
    branch = git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    dirty = git(
        "diff",
        "--quiet",
        "HEAD",
        "--",
        "src/voice_gateway",
        "scripts",
        "tests/voice_bench",
    ).returncode
    return f"{rev} ({branch}{', with local changes' if dirty else ''})"


def gpu_memory() -> str | None:
    for used in Path("/sys/class/drm").glob("card*/device/mem_info_vram_used"):
        total = used.with_name("mem_info_vram_total")
        try:
            return f"{int(used.read_text()) / 1e9:.1f} of {int(total.read_text()) / 1e9:.1f} GB VRAM in use"
        except (OSError, ValueError):
            continue
    return None


def repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def judge_label(args) -> str | None:
    """What judged the caller's line: ``--judge-label`` or model and URL."""
    if not getattr(args, "judge_url", None):
        return None
    label = getattr(args, "judge_label", None) or args.judge_model
    return f"{label} at {args.judge_url}"


def write_meta(out: Path, args, settings, levels: list[str]) -> dict:
    path = out / "run.json"
    meta = json.loads(path.read_text("utf-8")) if path.exists() else {}
    meta.update(
        {
            "profile": args.profile,
            "profile_file": repo_relative(Path(args.profile_file)),
            "started": meta.get("started")
            or dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "git": git_state(),
            "today": dt.date.today().isoformat(),
            "stages": {
                "stt": f"{settings.stt.model} at {settings.stt.base_url}",
                "llm": f"{settings.llm.model} at {settings.llm.base_url}",
                "tts": f"{settings.tts.model} voice {settings.tts.voice} at {settings.tts.base_url}",
            },
            "judge": judge_label(args),
            "gpu": gpu_memory(),
        }
    )
    meta["levels"] = sorted(
        set(meta.get("levels", [])) | set(levels), key=Rep.LEVELS.index
    )
    notes = [NOISE_NOTE]
    if meta["judge"]:
        notes.append(f"what the caller heard was judged by {meta['judge']}")
    else:
        notes.append("no judge: what the caller heard was not transcribed")
    if getattr(args, "no_llm_judge", False):
        meta["llm_judge"] = None
    else:
        model = args.llm_judge_model or settings.llm.model
        url = args.llm_judge_url or settings.llm.base_url
        meta["llm_judge"] = f"{model} at {url}"
        notes.append(
            f"an LLM judge ({model}) decides only `meaning` values that exact rules "
            "and their keywords cannot"
        )
    meta["notes"] = notes
    path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), "utf-8")
    return meta


# -- render ------------------------------------------------------------------------------


def cmd_render(args) -> int:
    from tests.voice_bench.settings import Compose, Network

    os.environ.setdefault("COMPOSE_PROJECT_NAME", COMPOSE_PROJECT)
    evalset = spec.load()
    judge = judge_fn(args)
    renderer = R.Renderer(
        evalset,
        workers=args.workers,
        threads=args.threads,
        judge=judge,
        wer=bench_wer if judge else None,
        compose=Compose(Network(args.subnet)),
        log=log,
    )
    if args.try_seeds:
        voices = (
            args.voices.split(",")
            if args.voices
            else [
                v.id
                for v in evalset.voices.voices.values()
                if v.engine == "qwen-design"
            ]
        )
        rows = renderer.try_seeds(voices, [int(x) for x in args.try_seeds.split(",")])
        (R.VOICES_DIR / "candidates" / "candidates.json").write_text(
            json.dumps(rows, indent=1), "utf-8"
        )
        return 0
    renderer.design_voices(force=args.force_voices)
    if args.only == "voices":
        return 0
    renderer.render_pieces(force=args.force)
    built = renderer.build_clips()
    log(f"built {built} clip(s); {renderer.human()} real recording(s) converted")
    pieces = renderer.manifest["pieces"]
    bad = [k for k, v in pieces.items() if not v.get("judge_ok", True)]
    if bad:
        log(
            f"{len(bad)} piece(s) the judge could not read back even after {R.ATTEMPTS} takes:"
        )
        for key in bad:
            best = pieces[key]["best"]
            log(f"  {key}: {pieces[key]['text']!r} -> {best.get('judge_text')!r}")
    return 0


# -- level 1: STT over the corpus ------------------------------------------------------------


def stt_plan(evalset, args) -> list[tuple[spec.Utterance, R.Clip, str, Path]]:
    plan = []
    first_voice: dict[str, str] = {}
    for utt_id, clip in R.corpus_clips(evalset):
        utt = evalset.corpus.utterances[utt_id]
        own = first_voice.setdefault(utt_id, clip.voice) == clip.voice
        conds = evalset.corpus.conditions if own else evalset.corpus.sweep_conditions
        for cond in conds:
            plan.append((utt, clip, cond, R.clip_path(clip)))
    for rec in evalset.human:
        utt = spec.Utterance(rec.id, rec.category, rec.text, values=rec.values)
        clip = R.Clip(rec.id, f"human:{rec.speaker}", rec.text)
        for cond in ["as-recorded"] if rec.phone else evalset.corpus.conditions:
            plan.append((utt, clip, cond, R.HUMAN_CACHE / f"{rec.id}.wav"))
    if args.conditions:
        wanted = set(args.conditions.split(","))
        plan = [p for p in plan if p[2] in wanted or p[2] == "as-recorded"]
    if args.categories:
        wanted = set(args.categories.split(","))
        plan = [p for p in plan if p[0].category in wanted]
    if args.voices:
        wanted = set(args.voices.split(","))
        plan = [p for p in plan if p[1].voice in wanted]
    return plan[: args.limit] if args.limit else plan


async def stt_level(args, evalset, settings, out: Path) -> dict:
    import aiohttp

    from voice_gateway.engines.cascade.services import TranscriptionSTT
    from voice_gateway.engines.cascade.timings import Timings

    plan = stt_plan(evalset, args)
    sources = _babble()
    today = dt.date.today()
    log(f"STT level: {len(plan)} clip(s) through {settings.stt.model}")
    items: list[dict] = []
    semaphore = asyncio.Semaphore(args.parallel)
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as http:

        async def one(index, utt, clip, cond, path):
            pcm, _ = R.read_pcm(path)
            info: dict = {}
            if cond != "as-recorded":
                pcm = C.apply(
                    pcm, evalset.conditions[cond], clip.id, sources=sources, info=info
                )
            timings = Timings()
            stt = TranscriptionSTT(
                settings.stt, language=settings.language, http=http, timings=timings
            )
            async with semaphore:
                started = time.monotonic()
                text = ""
                async for frame in stt.run_stt(R.wav_bytes(pcm)):
                    text = getattr(frame, "text", "") or text
                ms = round((time.monotonic() - started) * 1000)
            scored = S.score_stt(utt, text, wer=bench_wer, today=today)
            item = {
                "id": utt.id,
                "clip": clip.id,
                "category": utt.category,
                "styles": list(utt.styles),
                "voice": clip.voice,
                "condition": cond,
                "silence": bool(utt.silence),
                "reference": utt.reference,
                "heard": text,
                "stt_ms": ms,
                **scored,
                **({"info": info} if info else {}),
            }
            items.append(item)
            if index % 50 == 0 or args.verbose:
                log(
                    f"  {index + 1}/{len(plan)} {clip.id} {cond}: WER {item['wer']} | {text}"
                )

        await asyncio.gather(*(one(i, *p) for i, p in enumerate(plan)))
    items.sort(key=lambda i: (i["id"], i["voice"], i["condition"]))
    summary = S.aggregate_stt(items)
    (out / "stt.json").write_text(
        json.dumps({"summary": summary, "items": items}, indent=1, ensure_ascii=False),
        "utf-8",
    )
    o = summary["overall"]
    log(f"STT: WER {o['wer_mean']}, values exact {o['values_ok']}/{o['values']}")
    return summary


# -- level 2: scenarios through the cascade harness ------------------------------------------


_BABBLE: list = []


def _babble() -> list:
    if not _BABBLE:
        _BABBLE.extend(R.babble_sources())
    return _BABBLE


def scenario_lines(
    evalset, scenario, stage: str | None
) -> list[tuple[spec.Turn, "object", int, int]]:
    """(turn, pcm under the scenario's condition, lead ms, tail ms) per turn."""
    clips = {(sid, ti): clip for sid, ti, clip in R.scenario_clips(evalset)}
    sources = _babble()
    out = []
    for turn in scenario.turns:
        clip = clips[(scenario.id, turn.index)]
        pcm = R.load_clip(clip)
        cond = evalset.conditions[turn.condition or scenario.condition]
        conditioned = C.apply(pcm, cond, clip.id, stage=stage, sources=sources)
        lead = 0 if turn.silence else int(R.LEAD_S * 1000)
        tail = 0 if turn.silence else int(R.TAIL_S * 1000)
        out.append((turn, conditioned, lead, tail))
    return out


def make_harness(settings, scenario, background):
    """The try-it harness of voice_cascade_try.py with the scenario's canned
    back office, a line that carries the condition's background between the
    caller's lines, and a record of every flush (barge-in)."""
    from tests.voice_bench.srw_driver import match_answer
    from voice_cascade_try import Harness

    from voice_gateway.engines.cascade.fileport import FRAME_MS
    from voice_gateway.interfaces import FRAME_BYTES

    class EvalHarness(Harness):
        def __init__(self):
            super().__init__(settings, answer_delay=0.0, barge_after=0.0)
            port = self.port
            self.flushes: list[tuple[int, bool]] = []
            self.delegations: list[dict] = []
            self.heard_since: list[str] = []
            self.used: set[int] = set()
            self.transfers: list[int] = []
            self.lines: list[dict] = []
            original_flush, original_step = port.flush, port._step
            bg = background
            cursor = [0]

            async def flush():
                self.flushes.append((port.clock_ms, port.voice_playing))
                await original_flush()

            def step():
                if bg is not None and not port._incoming:
                    at = cursor[0]
                    port._incoming.append(bg[at : at + FRAME_BYTES])
                    cursor[0] = (at + FRAME_BYTES) % (len(bg) - FRAME_BYTES)
                original_step()

            port.flush = flush
            port._step = step
            self.frame_ms = FRAME_MS

        async def collect(self):
            while (event := await self.queue.get()) is not None:
                self.events.append({**event, "clock_ms": self.port.clock_ms})
                kind = event["type"]
                if kind == "session.input_transcript.delta":
                    self.heard_since.append(event["delta"])
                    self.log(f"caller: {event['delta']}")
                elif kind == "session.output_transcript.delta":
                    self.log(f"voice:  {event['delta']}")
                elif kind == "session.delegation.created":
                    request = " ".join(t.strip() for t in self.heard_since)
                    self.heard_since = []
                    record = {
                        "id": event["delegation"]["id"],
                        "at_ms": self.port.clock_ms,
                        "request": request,
                    }
                    self.delegations.append(record)
                    self.log("delegation created")
                    self.answers_due += 1
                    asyncio.create_task(self.back_office(record))
                elif kind == "error":
                    self.log(f"error: {event['error'].get('code')}")

        async def back_office(self, record: dict):
            answers = [b.as_driver_entry() for b in scenario.back_office()]
            found = match_answer(
                answers, record["request"], self.used, scenario.default_answer
            )
            index, entry = found
            if index >= 0:
                self.used.add(index)
            record["answer_index"] = index
            delay = float(entry.get("delay", 4.0))
            waited = 0.0
            steps = []
            if entry.get("status"):
                line = f'Say exactly: "{entry["status"]}"'
                steps.append((entry["status_delay"], "instructions", line, None))
            if entry.get("follow_up"):
                steps.append(
                    (
                        entry["follow_up_delay"],
                        "commentary",
                        entry["follow_up"],
                        record["id"],
                    )
                )
            for at, kind, content, delegation_id in sorted(steps, key=lambda s: s[0]):
                await asyncio.sleep(at - waited)
                waited = at
                self.log(f"back office {kind} ({at} s later)")
                record[f"{kind}_at_ms"] = self.port.clock_ms
                await self.command(kind, content, delegation_id)
            await asyncio.sleep(delay - waited)
            self.log(f"back office answers ({delay} s later)")
            record["answered_at_ms"] = self.port.clock_ms
            self.awaiting_voice_after = self.port.clock_ms
            self.answers_due -= 1
            await self.command("commentary", entry["answer"], record["id"])
            if entry.get("transfer"):
                await asyncio.sleep(4.0)
                self.transfers.append(self.port.clock_ms)
                await self.session.control("refer", {"target_uri": "sip:3000@bench"})

        def voice_started_since(self, ms: int) -> bool:
            return any(s >= ms for s in self.port.voice_starts)

        async def wait_quiet(
            self, quiet_s: float, react_after: int | None, timeout_s: float = 75.0
        ):
            """Until the voice and the back office are done: nothing playing,
            no answer due, the voice reacted to ``react_after`` (or 10 s
            passed), then ``quiet_s`` of quiet."""
            started = time.monotonic()
            quiet_since = None
            while time.monotonic() - started < timeout_s:
                await asyncio.sleep(0.05)
                waiting_reaction = (
                    react_after is not None
                    and not self.voice_started_since(react_after)
                    and time.monotonic() - started < 10.0
                )
                waiting_relay = (
                    self.awaiting_voice_after >= 0
                    and not self.voice_started_since(self.awaiting_voice_after)
                    and self.port.clock_ms - self.awaiting_voice_after < 12_000
                )
                busy = (
                    self.port.voice_playing
                    or self.port.caller_speaking
                    or self.answers_due > 0
                    or waiting_reaction
                    or waiting_relay
                )
                if busy:
                    quiet_since = None
                    continue
                quiet_since = quiet_since or time.monotonic()
                if time.monotonic() - quiet_since >= quiet_s:
                    return
            self.log("timed out waiting for the voice")

        async def say(self, turn, pcm: bytes, lead: int, tail: int) -> dict:
            start = self.port.clock_ms
            clips = len(self.port.clip_ends)
            self.port.say(pcm)
            self.log(
                f"caller {turn.label}: {(turn.text or f'(silence {turn.silence} s)')[:70]}"
            )
            while len(self.port.clip_ends) == clips:
                await asyncio.sleep(0.02)
            end = self.port.clip_ends[-1]
            line = {
                "turn": turn.index,
                "clip_start_ms": start,
                "clip_end_ms": end,
                "caller_start_ms": start + lead,
                "caller_end_ms": end - tail,
                "voice_was_talking": False,
            }
            return line

        async def run_scenario(self, lines, instructions: str, greeting: str):
            body = {
                "session": {
                    "type": "live",
                    "model": "cascade",
                    "instructions": instructions,
                    "audio": {"output": {"voice": "marin"}},
                    "delegation": {"type": "client"},
                }
            }
            await self.session.control("accept", body)
            self.mono0 = time.monotonic() - self.port.clock_ms / 1000
            _, self.queue = self.session.subscribe()
            collector = asyncio.create_task(self.collect())
            await asyncio.sleep(1.5)
            mark = self.port.clock_ms
            await self.command("instructions", greeting)
            await self.wait_quiet(1.0, mark)
            previous: dict | None = None
            for turn, pcm, lead, tail in lines:
                expects_reaction = previous is not None and previous["expect"] in (
                    "answer",
                    "delegate",
                    "transfer",
                    "message",
                )
                react_after = previous["caller_end_ms"] if expects_reaction else None
                if turn.when == "during_voice":
                    since = previous["caller_end_ms"] if previous else 0
                    started = time.monotonic()
                    while (
                        not self.voice_started_since(since)
                        and time.monotonic() - started < 15
                    ):
                        await asyncio.sleep(0.02)
                    begin = next(
                        (s for s in self.port.voice_starts if s >= since), None
                    )
                    if begin is None:
                        self.log("the voice never started; the caller talks anyway")
                    else:
                        while self.port.clock_ms < begin + turn.offset * 1000:
                            await asyncio.sleep(0.02)
                elif turn.when == "after_caller":
                    target = (
                        previous["clip_end_ms"] if previous else self.port.clock_ms
                    ) + turn.offset * 1000
                    while self.port.clock_ms < target:
                        await asyncio.sleep(0.02)
                else:
                    await self.wait_quiet(1.2 + turn.offset, react_after)
                talking = self.port.voice_playing
                line = await self.say(turn, pcm, lead, tail)
                line["voice_was_talking"] = talking
                line["expect"] = turn.expect.outcome
                self.lines.append(line)
                previous = line
            last = (
                previous["caller_end_ms"]
                if previous and previous["expect"] not in ("ignore", "any")
                else None
            )
            await self.wait_quiet(2.5, last)
            self.log("caller hangs up")
            self.end_ms = self.port.clock_ms
            await self.session.close("remote_hangup")
            await self.port.close()
            collector.cancel()

    return EvalHarness()


def judge_voice(heard: bytes, events: list[dict], judge: tuple[str, str]) -> list[dict]:
    """What the caller understood of each stretch of the voice (the judge
    ASR on the voice after the phone line) against what the voice said.
    Stretches split only at pauses over 0.9 s, so a sentence stays whole."""
    from voice_cascade_try import voice_runs

    from tests.voice_bench.call import judge_text

    said = [e for e in events if e["type"] == "session.output_transcript.delta"]
    out = []
    for start, end in voice_runs(heard, gap_ms=900):
        text = " ".join(
            e["delta"].strip() for e in said if start - 400 <= e["clock_ms"] <= end
        )
        if not text:
            continue
        audio = np.frombuffer(heard[start * 32 : end * 32], dtype="<i2")
        understood = judge_text(judge[0], judge[1], R.wav_bytes(audio))
        out.append(
            {
                "start_ms": start,
                "said": text,
                "heard": understood,
                "wer": round(bench_wer(text, understood), 3),
            }
        )
    return out


def llm_requests(context) -> list[str]:
    """The voice LLM's own ``delegate`` arguments, in call order."""
    out = []
    for message in context.get_messages() if context is not None else []:
        for call in (
            (message.get("tool_calls") or []) if isinstance(message, dict) else []
        ):
            function = call.get("function") or {}
            if function.get("name") != "delegate":
                continue
            try:
                out.append(
                    json.loads(function.get("arguments") or "{}").get("request", "")
                )
            except (ValueError, AttributeError):
                out.append(str(function.get("arguments")))
    return out


def observe_file(harness, scenario) -> list[dict]:
    """Per turn, what happened in its window (scoring.score_turn's ``obs``)."""
    from voice_cascade_try import voice_runs

    from voice_gateway.engines.cascade.fileport import phone

    events = harness.events
    starts = list(harness.port.voice_starts)
    runs = voice_runs(phone(bytes(harness.port.heard)))
    llm = llm_requests(harness.engine.context if harness.engine else None)
    for index, delegation in enumerate(harness.delegations):
        delegation["request_llm"] = llm[index] if index < len(llm) else None
    timings = []
    if harness.engine is not None:
        for t in harness.engine.timings.turns:
            timings.append(
                {
                    **t.as_dict(),
                    "speech_end_ms": round((t.speech_end - harness.mono0) * 1000),
                }
            )
    lines = harness.lines
    reply_his = S.reply_windows(
        list(scenario.turns), [ln["clip_start_ms"] for ln in lines], harness.end_ms + 1
    )
    observed = []
    for i, line in enumerate(lines):
        lo = line["clip_start_ms"]
        hi = lines[i + 1]["clip_start_ms"] if i + 1 < len(lines) else harness.end_ms + 1
        rhi = reply_his[i]
        ignore = scenario.turns[i].expect.outcome == "ignore"
        caller = [
            (e["clock_ms"], e["delta"])
            for e in events
            if e["type"] == "session.input_transcript.delta"
            and lo <= e["clock_ms"] < hi
        ]
        voice = [
            (e["clock_ms"], e["delta"])
            for e in events
            if e["type"] == "session.output_transcript.delta"
            and lo <= e["clock_ms"] < rhi
        ]
        delegations = [
            d
            for d in harness.delegations
            if (line["caller_end_ms"] if ignore else lo)
            <= d["at_ms"]
            < (hi if ignore else rhi)
        ]
        answered = min(
            (d["answered_at_ms"] for d in delegations if d.get("answered_at_ms")),
            default=None,
        )
        holding = " ".join(t for at, t in voice if answered is None or at < answered)
        relay = " ".join(
            t for at, t in voice if answered is not None and at >= answered
        )
        end = line["caller_end_ms"]
        after = [s for s in starts if s >= end - 200 and s < rhi]
        obs = {
            **{
                k: line[k]
                for k in ("caller_start_ms", "caller_end_ms", "voice_was_talking")
            },
            "window_end_ms": hi,
            "stt": " ".join(t.strip() for _, t in caller),
            "stt_events": [at for at, _ in caller],
            "reply": " ".join(t.strip() for _, t in voice),
            "holding": holding,
            "relay": relay,
            "delegations": delegations,
            "voice_starts": [s for s in starts if lo <= s < rhi],
            "voice_sentences": [at for at, _ in voice],
            "flushes": [at for at, cut in harness.flushes if cut and lo <= at < hi],
            "first_audio_ms": (after[0] - end) if after else None,
            "transfer_requested": any(lo <= t < hi for t in harness.transfers),
            "timings": [t for t in timings if lo <= t["speech_end_ms"] < hi],
        }
        if delegations and answered is not None:
            # The longest stretch without the voice between the delegation
            # and the relayed answer's first audio.
            relay_start = next((s for s in starts if s >= answered), rhi)
            gaps, cursor = [], delegations[0]["at_ms"]
            for a, b in runs:
                if b <= cursor or a >= relay_start:
                    continue
                gaps.append(max(0, a - cursor))
                cursor = max(cursor, b)
            gaps.append(max(0, relay_start - cursor))
            obs["wait_silence_ms"] = max(gaps)
        observed.append(obs)
    return observed, timings


async def file_scenario(args, evalset, settings, scenario, judge, out: Path) -> dict:
    import voice_cascade_testset as testset

    from voice_gateway.engines.cascade.fileport import interleave, phone, write_wav

    cond = evalset.conditions[scenario.condition]
    background = None
    if any(op.op == "noise" for op in cond.ops):
        silence = np.zeros(20 * C.RATE, dtype=np.int16)
        background = C.apply(silence, cond, f"{scenario.id}/background").tobytes()
    lines = [
        (turn, pcm.tobytes(), lead, tail)
        for turn, pcm, lead, tail in scenario_lines(evalset, scenario, stage=None)
    ]
    harness = make_harness(settings, scenario, background)
    await harness.run_scenario(
        lines, testset.instructions(), testset.greeting_instruction()
    )
    observed, timings = observe_file(harness, scenario)
    heard = phone(bytes(harness.port.heard))
    sdir = out / "file" / scenario.id
    sdir.mkdir(parents=True, exist_ok=True)
    write_wav(
        sdir / "call.wav", interleave(bytes(harness.port.said), heard), channels=2
    )
    intelligibility = judge_voice(heard, harness.events, judge) if judge else []
    speech = [
        (ln["caller_start_ms"], ln["caller_end_ms"])
        for ln, (turn, *_rest) in zip(harness.lines, lines, strict=False)
        if not turn.silence
    ]
    transcripts = [
        (e["clock_ms"], e["delta"])
        for e in harness.events
        if e["type"] == "session.input_transcript.delta"
    ]
    phantoms = S.phantom_transcripts(transcripts, speech)
    return {
        "scenario": scenario.id,
        "observed": observed,
        "timings": timings,
        "intelligibility": intelligibility,
        "phantoms": phantoms,
        "context": [
            {k: m.get(k) for k in ("role", "content", "tool_calls")}
            for m in (harness.engine.context.get_messages() if harness.engine else [])
            if isinstance(m, dict)
        ],
        "events": [
            {k: e[k] for k in ("type", "clock_ms", "delta") if k in e}
            for e in harness.events
        ],
    }


def score_scenarios(
    evalset, runs: list[dict], judge_llm=None, today: dt.date | None = None
) -> tuple[list[dict], dict]:
    """Verdicts for every observed turn and the totals; ``today`` resolves
    relative dates (the day of the run)."""
    import voice_cascade_eval as cascade_eval

    today = today or dt.date.today()
    results = []
    timings, intel, phantoms = [], [], 0
    for run in runs:
        scenario = evalset.scenarios[run["scenario"]]
        verdicts = []
        for index, obs in enumerate(run["observed"]):
            turn = scenario.turns[index]
            verdict = S.score_turn(
                turn,
                obs,
                today=today,
                wording=cascade_eval.wording,
                relay_wording=cascade_eval.relay_wording,
                judge=judge_llm,
            )
            results.append({"scenario": scenario.id, "turn": index, "verdict": verdict})
            verdicts.append(verdict)
        run["verdicts"] = verdicts
        timings += run.get("timings") or []
        intel += run.get("intelligibility") or []
        phantoms += len(run.get("phantoms") or [])
    summary = S.aggregate_turns(results, evalset.scenarios)
    summary["timings"] = S.aggregate_timings(timings)
    summary["phantom_transcripts"] = phantoms
    if intel:
        summary["intelligibility"] = {
            "judge": runs[0].get("judge"),
            "lines": len(intel),
            "wer_mean": S._mean(i["wer"] for i in intel),
            "perfect": sum(i["wer"] == 0 for i in intel),
        }
    return results, summary


def pick_scenarios(evalset, args) -> list:
    if not args.scenarios:
        return list(evalset.scenarios.values())
    wanted = args.scenarios.split(",")
    picked = [
        s for s in evalset.scenarios.values() if any(s.id.startswith(w) for w in wanted)
    ]
    if not picked:
        raise SystemExit(f"no scenario matches {args.scenarios}")
    return picked


async def file_level(args, evalset, settings, out: Path) -> dict:
    judge = (args.judge_url, args.judge_model) if args.judge_url else None
    runs = []
    for scenario in pick_scenarios(evalset, args):
        log(f"file level: {scenario.id} ({scenario.title})")
        run = await file_scenario(args, evalset, settings, scenario, judge, out)
        run["judge"] = judge_label(args)
        runs.append(run)
    results, summary = score_scenarios(evalset, runs, llm_judge(args, settings))
    (out / "file.json").write_text(
        json.dumps(
            {"summary": summary, "scenarios": runs},
            indent=1,
            ensure_ascii=False,
            default=str,
        ),
        "utf-8",
    )
    o = summary["overall"]
    log(
        f"file level: {o['scenarios_ok']}/{o['scenarios']} scenarios, {o['turns_ok']}/{o['turns']} turns right"
    )
    return summary


# -- the LLM judge (only where exact rules cannot decide) ------------------------------------

JUDGE_RUBRIC = """You check a phone assistant's test call. You get a VALUE the \
caller gave (what it means) and a TEXT (what the assistant said, or the request \
it passed on). Answer PASS only if the TEXT carries the same value: the same \
fact, date (resolve relative dates against TODAY), number, name (case and \
spacing may differ, spelling may not) or request. Wording, order and politeness \
do not matter. Answer FAIL if the value is missing, changed, or only partly \
there. Reply with one JSON object: {"verdict": "PASS" or "FAIL", "reason": "<one short sentence>"}."""


def llm_judge(args, settings):
    """A judge for ``meaning`` values: the profile's LLM unless --llm-judge-url
    names another OpenAI-compatible server. Exact rules always go first."""
    if args.no_llm_judge:
        return None
    url = args.llm_judge_url or settings.llm.base_url
    model = args.llm_judge_model or settings.llm.model

    def judge(value, text: str, where: str) -> dict:
        import urllib.request

        body = {
            "model": model,
            "temperature": 0,
            "max_tokens": 120,
            "messages": [
                {"role": "system", "content": JUDGE_RUBRIC},
                {
                    "role": "user",
                    "content": f"TODAY: {dt.date.today().isoformat()}\nVALUE: {value.value}\nTEXT ({where}): {text}",
                },
            ],
        }
        request = urllib.request.Request(
            url.rstrip("/") + "/chat/completions",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - the operator's LLM
                answer = json.loads(response.read())["choices"][0]["message"]["content"]
        except (OSError, ValueError, KeyError) as exc:
            return {"pass": False, "model": model, "error": type(exc).__name__}
        match = re.search(r"\{.*\}", answer or "", re.S)
        try:
            parsed = json.loads(match.group(0)) if match else {}
        except ValueError:
            parsed = {}
        return {
            "pass": str(parsed.get("verdict", "")).upper() == "PASS",
            "reason": parsed.get("reason"),
            "model": model,
        }

    return judge


# -- level 3: scenarios through the bench PBX ------------------------------------------------


def _secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(value)


def wait_until(predicate, timeout: float, what: str) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise TimeoutError(f"timed out waiting for {what}")


#: Seconds the caller leaves the voice per expected outcome (call level: the
#: bench's caller cannot listen, so the script's times are fixed).
REPLY_ROOM = {"answer": 8.0, "any": 7.0, "ignore": 1.5}
DELEGATION_ROOM = 10.0
FIRST_LINE_AT = 14.0


def reply_room(turn) -> float:
    """Seconds the voice needs after a caller line, as far as the script can
    guess: the back office's delay plus room for a delegation."""
    if turn.back_office is not None:
        return turn.back_office.after + DELEGATION_ROOM
    return REPLY_ROOM.get(turn.expect.outcome, DELEGATION_ROOM)


def call_schedule(lines) -> tuple[list[float], float]:
    """Start time of each caller line (s after the answer) and the hangup.
    A line after the voice waits until every earlier line's reply had its
    room; a line over the voice starts 1.6 s (about when the voice begins)
    plus its offset after the previous line; ``after_caller`` follows on."""
    times, cursor, voice_done, prev_end = [], FIRST_LINE_AT, FIRST_LINE_AT, None
    for turn, pcm, *_ in lines:
        seconds = len(pcm) / 2 / C.RATE if isinstance(pcm, bytes) else len(pcm) / C.RATE
        if prev_end is not None:
            if turn.when == "during_voice":
                cursor = prev_end + 1.6 + turn.offset
            elif turn.when == "after_caller":
                cursor = prev_end + turn.offset
            else:
                cursor = voice_done + turn.offset
        times.append(round(cursor, 2))
        prev_end = cursor + seconds
        voice_done = max(voice_done, prev_end + reply_room(turn))
    return times, round(voice_done + 4, 1)


class CallBench:
    """The bench PBX, SRW's addon, a gateway on the host with the profile,
    and srw_driver.py as the canned back office, for one run."""

    def __init__(self, args, raw_profile: dict, out: Path):
        from tests.voice_bench.settings import RUN_DIR, Compose, Network

        os.environ["COMPOSE_PROJECT_NAME"] = COMPOSE_PROJECT
        self.args = args
        self.network = Network(args.subnet)
        self.compose = Compose(self.network, srw=True)
        self.raw_profile = raw_profile
        self.out = out / "call"
        self.out.mkdir(parents=True, exist_ok=True)
        self.secrets = RUN_DIR / "srw" / "eval"
        self.gateway: subprocess.Popen | None = None
        self.driver: subprocess.Popen | None = None

    def up(self) -> None:
        from tests.voice_bench import srw
        from tests.voice_bench.bench import ensure_images, setup_pbx

        images = ("srw-asterisk:22.11.0", "srw-voice-bench-ua:pjsua-2.17")
        missing = subprocess.run(
            ["docker", "image", "inspect", *images], capture_output=True, check=False
        ).returncode
        if missing:
            ensure_images(self.compose)
        args = argparse.Namespace(
            subnet=self.args.subnet, dut_peer=f"{self.network.addon}:5060", dut="srw"
        )
        setup_pbx(args, fresh=False)
        source = srw.prepare_addon(self.network, "eval", "2000")
        srw.addon_up(self.compose, "2000", timeout=60)
        log(f"bench PBX and addon up on {self.args.subnet} (project {COMPOSE_PROJECT})")
        api_key = secrets.token_hex(16)
        whsec = "whsec_" + base64.b64encode(secrets.token_bytes(24)).decode()
        _secret(self.secrets / "api-key", api_key)
        _secret(self.secrets / "webhook", whsec)
        profile = {"engine": "cascade", **self.raw_profile}
        config = {
            "listen": GATEWAY_LISTEN,
            "api_key_files": [str(self.secrets / "api-key")],
            "orchestrator_url": f"http://{DRIVER_LISTEN}",
            "lines": [
                {
                    "name": "eval",
                    "connector_id": "eval-connector",
                    "webhook_secret_file": str(self.secrets / "webhook"),
                }
            ],
            "profiles": {self.args.profile: profile},
            "source": source,
        }
        import yaml

        _secret(self.secrets / "gateway.yaml", yaml.safe_dump(config, sort_keys=False))
        env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
        self.gateway_log = (self.out / "gateway.log").open("w")
        self.gateway = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "voice_gateway",
                "--config",
                str(self.secrets / "gateway.yaml"),
            ],
            cwd=self.secrets,
            env=env,
            stdout=self.gateway_log,
            stderr=subprocess.STDOUT,
        )
        wait_until(
            lambda: srw.APP in self.compose.asterisk("ari show apps", service="addon"),
            60,
            "the gateway's ARI app",
        )
        log(f"gateway up on {GATEWAY_LISTEN} with profile {self.args.profile}")

    def start_driver(self, scenario, events: Path) -> None:
        import voice_cascade_testset as testset

        config = {
            "listen": DRIVER_LISTEN,
            "gateway": f"http://{GATEWAY_LISTEN}",
            "api_key_file": str(self.secrets / "api-key"),
            "connectors": {"eval-connector": str(self.secrets / "webhook")},
            "events": str(events),
            "model": self.args.profile,
            "instructions": testset.instructions(),
            "greeting": testset.greeting_instruction(),
            "transfer_target": f"sip:3000@{self.network.pbx}",
            "transfer_digit": "9",
            "answer_delay": 4.0,
            "transfer_delay": 4.0,
            "answers": [b.as_driver_entry() for b in scenario.back_office()],
            "answer_once": True,
            "default_answer": scenario.default_answer,
            "log_content": True,
        }
        path = events.parent / "driver.json"
        path.write_text(json.dumps(config, indent=1, ensure_ascii=False), "utf-8")
        events.write_text("", "utf-8")
        env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
        self.driver = subprocess.Popen(
            [
                sys.executable,
                str(REPO_ROOT / "tests" / "voice_bench" / "srw_driver.py"),
                "--config",
                str(path),
            ],
            env=env,
            stdout=(events.parent / "driver.log").open("w"),
            stderr=subprocess.STDOUT,
        )
        wait_until(lambda: '"ready"' in events.read_text("utf-8"), 20, "the driver")

    def stop_driver(self) -> None:
        if self.driver is not None:
            self.driver.terminate()
            try:
                self.driver.wait(10)
            except subprocess.TimeoutExpired:
                self.driver.kill()
            self.driver = None

    def down(self) -> None:
        from tests.voice_bench import srw

        self.stop_driver()
        if self.gateway is not None:
            self.gateway.terminate()
            try:
                self.gateway.wait(15)
            except subprocess.TimeoutExpired:
                self.gateway.kill()
            self.gateway_log.close()
        if self.args.keep_up:
            log(
                "bench left running (--keep-up); `scripts/voice-eval.sh bench-down` stops it"
            )
            return
        srw.down(self.compose)
        self.compose.run(
            "--profile", "tools", "down", "--remove-orphans", check=False, timeout=120
        )
        log("bench stopped")


def gateway_timings(log_text: str, session_id: str | None) -> list[dict]:
    """The engine's turn timings from the gateway's log line for a call."""
    for line in log_text.splitlines():
        if "cascade turns:" in line and (session_id is None or session_id in line):
            try:
                return ast.literal_eval(line.split("cascade turns:", 1)[1].strip())
            except (ValueError, SyntaxError):
                return []
    return []


def observe_call(
    scenario, times: list[float], lines, sdir: Path, call_report: dict, gateway_log: str
) -> tuple[list[dict], list[dict], list]:
    from tests.voice_bench.analysis import load_events

    caller_events = load_events(sdir / "caller.events.jsonl")
    t0 = next(
        (
            e["t"]
            for e in caller_events
            if e.get("type") == "call_state" and e.get("state") == "CONFIRMED"
        ),
        None,
    )
    driver = [
        json.loads(x)
        for x in (sdir / "driver.jsonl").read_text("utf-8").splitlines()
        if x.strip()
    ]
    if t0 is None:
        return [], [], []

    def ms(t: float) -> int:
        return round((t - t0) * 1000)

    session = next(
        (e.get("session_id") for e in driver if e.get("kind") == "attached"), None
    )
    transcripts = [
        (ms(e["t"]), e.get("text", ""), e["event"])
        for e in driver
        if e.get("kind") == "event"
        and str(e.get("event", "")).endswith("_transcript.delta")
    ]
    delegations = [
        {
            "at_ms": ms(e["t"]),
            "request": e.get("request", ""),
            "answer_index": e.get("answer"),
        }
        for e in driver
        if e.get("kind") == "delegation"
    ]
    answered = [ms(e["t"]) for e in driver if e.get("kind") == "answered"]
    for delegation, at in zip(delegations, answered, strict=False):
        delegation["answered_at_ms"] = at
    referred = [ms(e["t"]) for e in driver if e.get("kind") == "referred"]
    human_at = (call_report.get("transfer") or {}).get("human_answered_at_s")
    stretches = [
        (round(s["from_s"] * 1000), round(s["to_s"] * 1000), s.get("text", ""))
        for s in call_report.get("timeline", [])
        if human_at is None or s["from_s"] < human_at
    ]
    starts = [a for a, _, _ in stretches]
    # The last turn's window ends with the call: the last event either side
    # saw. (Unbounded, a back-office answer the voice never said counted as
    # a silence of 10**9 ms.)
    call_end = max(ms(e["t"]) for e in [*driver, *caller_events] if "t" in e) + 1
    reply_his = S.reply_windows(
        [t for t, *_ in lines], [round(t * 1000) for t in times], call_end
    )
    observed = []
    for i, (turn, pcm, lead, tail) in enumerate(lines):
        seconds = len(pcm) / 2 / C.RATE
        clip_start = round(times[i] * 1000)
        clip_end = clip_start + round(seconds * 1000)
        lo = clip_start
        hi = round(times[i + 1] * 1000) if i + 1 < len(times) else 10**9
        start, end = clip_start + lead, clip_end - tail
        rhi = reply_his[i]
        ignore = turn.expect.outcome == "ignore"
        caller = [
            (at, t)
            for at, t, kind in transcripts
            if kind.startswith("session.input") and lo <= at < hi
        ]
        voice = [
            (at, t)
            for at, t, kind in transcripts
            if kind.startswith("session.output") and lo <= at < rhi
        ]
        dels = [
            d
            for d in delegations
            if (end if ignore else lo) <= d["at_ms"] < (hi if ignore else rhi)
        ]
        answered_at = min(
            (d["answered_at_ms"] for d in dels if d.get("answered_at_ms")), default=None
        )
        after = [s for s in starts if s >= end - 200 and s < rhi]
        obs = {
            "caller_start_ms": start,
            "caller_end_ms": end,
            "window_end_ms": hi,
            "stt": " ".join(t.strip() for _, t in caller),
            "stt_events": [at for at, _ in caller],
            "reply": " ".join(t.strip() for _, t in voice),
            "holding": " ".join(
                t for at, t in voice if answered_at is None or at < answered_at
            ),
            "relay": " ".join(
                t for at, t in voice if answered_at is not None and at >= answered_at
            ),
            "delegations": dels,
            "voice_starts": [s for s in starts if lo <= s < rhi],
            "voice_sentences": [at for at, _ in voice],
            "flushes": None,
            "voice_was_talking": any(a <= lo < b for a, b, _ in stretches),
            "first_audio_ms": (after[0] - end) if after else None,
            "transfer_requested": any(lo <= r < hi for r in referred),
            "heard": " ".join(x for a, _, x in stretches if lo <= a < rhi),
        }
        if turn.expect.barge_in:
            ongoing = [b for a, b, _ in stretches if a < start < b]
            if ongoing:
                obs["voice_stopped_ms"] = ongoing[0]
            else:
                obs["voice_stopped_ms"] = start
        if dels and answered_at is not None:
            relay_start = next((s for s in starts if s >= answered_at), rhi)
            gaps, cursor = [], dels[0]["at_ms"]
            for a, b, _ in stretches:
                if b <= cursor or a >= relay_start:
                    continue
                gaps.append(max(0, a - cursor))
                cursor = max(cursor, b)
            gaps.append(max(0, relay_start - cursor))
            obs["wait_silence_ms"] = max(gaps)
        observed.append(obs)
    speech = [
        (o["caller_start_ms"], o["caller_end_ms"])
        for o, (turn, *_r) in zip(observed, lines, strict=False)
        if not turn.silence
    ]
    phantoms = S.phantom_transcripts(
        [(at, t) for at, t, kind in transcripts if kind.startswith("session.input")],
        speech,
    )
    timings = gateway_timings(gateway_log, session)
    return observed, timings, phantoms


def call_intelligibility(observed: list[dict]) -> list[dict]:
    """Per turn: what the judge heard on the caller's line against what the
    voice said (the driver's transcript)."""
    out = []
    for obs in observed:
        said, heard = obs.get("reply") or "", obs.get("heard") or ""
        if said and heard:
            out.append(
                {
                    "start_ms": obs["caller_end_ms"],
                    "said": said,
                    "heard": heard,
                    "wer": round(bench_wer(said, heard), 3),
                }
            )
    return out


def call_level(args, evalset, raw_profile, out: Path) -> dict:
    from tests.voice_bench import call as bench_call
    from tests.voice_bench import srw

    if not args.judge_url:
        log(
            "call level without --judge-url: what the caller heard is transcribed by whisper.cpp small"
        )
    judge = (args.judge_url, args.judge_model) if args.judge_url else None
    bench = CallBench(args, raw_profile, out)
    runs = []
    try:
        bench.up()
        for scenario in pick_scenarios(evalset, args):
            log(f"call level: {scenario.id} ({scenario.title})")
            sdir = bench.out / scenario.id
            if sdir.exists():
                shutil.rmtree(sdir)
            sdir.mkdir(parents=True)
            lines = [
                (turn, pcm.tobytes(), lead, tail)
                for turn, pcm, lead, tail in scenario_lines(
                    evalset, scenario, stage="acoustic"
                )
            ]
            times, hangup = call_schedule(lines)
            raw_lines = []
            for (turn, pcm, *_), at in zip(lines, times, strict=True):
                path = sdir / "lines" / f"{turn.label}.wav"
                R.write_pcm(path, C.from_bytes(pcm))
                raw_lines.append({"at": at, "audio": str(path)})
            raw_script = {
                "number": scenario.number,
                "hangup_after": hangup,
                "lines": raw_lines,
            }
            cond = evalset.conditions[scenario.condition]
            if any(op.op == "noise" for op in cond.ops):
                silence = np.zeros(20 * C.RATE, dtype=np.int16)
                noise = C.apply(
                    silence, cond, f"{scenario.id}/background", stage="acoustic"
                )
                R.write_pcm(sdir / "lines" / "background.wav", noise)
                raw_script["background"] = str(sdir / "lines" / "background.wav")
            script = bench_call.parse_script(raw_script)
            bench.start_driver(scenario, sdir / "driver.jsonl")
            offset = (bench.out / "gateway.log").stat().st_size
            try:
                report = bench_call.run(
                    bench.compose, script, sdir, args.threads, log=log, judge=judge
                )
            finally:
                time.sleep(1.0)
                bench.stop_driver()
            with (bench.out / "gateway.log").open() as handle:
                handle.seek(offset)
                gateway_log = handle.read()
            observed, timings, phantoms = observe_call(
                scenario, times, lines, sdir, report, gateway_log
            )
            runs.append(
                {
                    "scenario": scenario.id,
                    "schedule_s": times,
                    "hangup_s": hangup,
                    "call_status": report.get("status"),
                    "transfer": report.get("transfer"),
                    "observed": observed,
                    "timings": timings,
                    "phantoms": phantoms,
                    "intelligibility": call_intelligibility(observed),
                    "judge": judge_label(args) or report.get("judge"),
                }
            )
        audio = srw.audio_check(bench.compose, ("addon",))
    finally:
        bench.down()
    results, summary = score_scenarios(
        evalset, runs, llm_judge(args, load_settings(args)[0])
    )
    summary["no_audio_written"] = audio.get("ok")
    (out / "call.json").write_text(
        json.dumps(
            {"summary": summary, "scenarios": runs, "no_audio_written": audio},
            indent=1,
            ensure_ascii=False,
            default=str,
        ),
        "utf-8",
    )
    o = summary["overall"]
    log(
        f"call level: {o['scenarios_ok']}/{o['scenarios']} scenarios, {o['turns_ok']}/{o['turns']} turns right"
    )
    return summary


# -- commands ----------------------------------------------------------------------------


def cmd_run(args) -> int:
    evalset = spec.load()
    settings, raw = load_settings(args)
    out = run_dir(args)
    levels = [lvl for lvl in args.levels.split(",") if lvl]
    unknown = set(levels) - set(Rep.LEVELS)
    if unknown:
        raise SystemExit(f"unknown level(s): {', '.join(sorted(unknown))}")
    write_meta(out, args, settings, levels)
    log(f"results in {out}")
    if "stt" in levels:
        asyncio.run(stt_level(args, evalset, settings, out))
    if "file" in levels:
        asyncio.run(file_level(args, evalset, settings, out))
    if "call" in levels:
        call_level(args, evalset, raw, out)
    return finish(out, args)


def finish(out: Path, args) -> int:
    summary = Rep.summarise(out)
    baseline_file = (
        Path(args.baseline)
        if getattr(args, "baseline", None)
        else Rep.baseline_path(summary["meta"].get("profile", ""))
    )
    baseline = Rep.load_json(baseline_file)
    if baseline is not None:
        rows = Rep.diff(summary, baseline)
        (out / "diff.json").write_text(json.dumps(rows, indent=1), "utf-8")
        (out / "diff.md").write_text(Rep.diff_markdown(rows), "utf-8")
        worse = sum(r["verdict"] == "worse" for r in rows)
        log(f"against {baseline_file.name}: {worse} metric(s) worse; {out / 'diff.md'}")
    log(f"summary: {out / 'summary.md'}")
    for spot in Rep.weak_spots(summary)[:20]:
        log(f"  weak: {spot}")
    return 0


def rescore(out: Path, args) -> None:
    """Score a run's stored observations again with the current test set and
    matchers, without a call: STT values against the stored transcripts,
    scenario turns against the stored observations, relative dates against
    the day of the run. Word error rates, timings and judge results stay."""
    from tests.voice_bench.evalset import values as V

    evalset = spec.load()
    meta = Rep.load_json(out / "run.json") or {}
    today = dt.date.fromisoformat(meta["today"]) if meta.get("today") else None
    data = Rep.load_json(out / "stt.json")
    if data is not None:
        utterances = dict(evalset.corpus.utterances)
        for rec in evalset.human:
            utterances[rec.id] = spec.Utterance(
                rec.id, rec.category, rec.text, values=rec.values
            )
        for item in data["items"]:
            utt = utterances.get(item["id"])
            if utt is None:
                continue
            checks = []
            for value in utt.values:
                found = V.match(value, item["heard"], today)
                checks.append(
                    {
                        "value": value.label(),
                        "kind": value.kind,
                        "category": value.category,
                        "ok": found.ok,
                        "how": found.how,
                    }
                )
            item["values"] = checks
        data["summary"] = S.aggregate_stt(data["items"])
        (out / "stt.json").write_text(
            json.dumps(data, indent=1, ensure_ascii=False), "utf-8"
        )
    judge = None if args.no_llm_judge else llm_judge(args, load_settings(args)[0])
    for level in ("file", "call"):
        data = Rep.load_json(out / f"{level}.json")
        if data is None:
            continue
        keep = {k: v for k, v in data["summary"].items() if k in ("no_audio_written",)}
        runs = [r for r in data["scenarios"] if r["scenario"] in evalset.scenarios]
        for run in runs:
            if level == "call":
                reobserve_call(out, evalset.scenarios[run["scenario"]], run)
            else:
                add_voice_sentences(run)
        _, summary = score_scenarios(evalset, runs, judge, today)
        data["summary"] = {**summary, **keep}
        (out / f"{level}.json").write_text(
            json.dumps(data, indent=1, ensure_ascii=False, default=str), "utf-8"
        )
    log(f"rescored {out}")


def add_voice_sentences(run: dict) -> None:
    """File level: when the voice's sentences began, from the stored events
    (runs from before observations recorded them)."""
    said = [
        e["clock_ms"]
        for e in run.get("events", [])
        if e["type"] == "session.output_transcript.delta"
    ]
    observed = run["observed"]
    for i, obs in enumerate(observed):
        if "voice_sentences" in obs:
            continue
        lo = obs["caller_start_ms"] - 300
        hi = (
            observed[i + 1]["caller_start_ms"] - 300 if i + 1 < len(observed) else 10**9
        )
        obs["voice_sentences"] = [at for at in said if lo <= at < hi]


def reobserve_call(out: Path, scenario, run: dict) -> None:
    """Call level: observe a stored call again from its saved artifacts (the
    driver's log, the bench's report, the gateway's log, the played lines)."""
    sdir = out / "call" / scenario.id
    report = Rep.load_json(sdir / "call.json")
    if report is None or not (sdir / "driver.jsonl").exists():
        return
    lines = []
    for turn in scenario.turns:
        pcm, _ = R.read_pcm(sdir / "lines" / f"{turn.label}.wav")
        lead = 0 if turn.silence else int(R.LEAD_S * 1000)
        tail = 0 if turn.silence else int(R.TAIL_S * 1000)
        lines.append((turn, pcm.tobytes(), lead, tail))
    gateway = out / "call" / "gateway.log"
    log_text = gateway.read_text("utf-8") if gateway.exists() else ""
    observed, timings, phantoms = observe_call(
        scenario, run["schedule_s"], lines, sdir, report, log_text
    )
    if observed:
        run.update(
            observed=observed,
            timings=timings,
            phantoms=phantoms,
            intelligibility=call_intelligibility(observed),
        )


def cmd_report(args) -> int:
    out = Path(args.run).resolve()
    if args.rescore:
        rescore(out, args)
    return finish(out, args)


def cmd_diff(args) -> int:
    out = Path(args.run).resolve()
    summary = Rep.load_json(out / "summary.json") or Rep.summarise(out)
    baseline_file = (
        Path(args.baseline)
        if args.baseline
        else Rep.baseline_path(summary["meta"].get("profile", ""))
    )
    baseline = Rep.load_json(baseline_file)
    if baseline is None:
        raise SystemExit(f"no baseline at {baseline_file}")
    rows = Rep.diff(summary, baseline)
    print(Rep.diff_markdown(rows), end="")
    return 1 if any(r["verdict"] == "worse" for r in rows) and args.strict else 0


def cmd_baseline(args) -> int:
    out = Path(args.run).resolve()
    summary = Rep.load_json(out / "summary.json") or Rep.summarise(out)
    profile = summary["meta"].get("profile")
    target = Rep.baseline_path(profile)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=1, ensure_ascii=False) + "\n", "utf-8")
    log(f"baseline for {profile}: {target}")
    return 0


def cmd_compare(args) -> int:
    summaries = {}
    for item in args.runs:
        path = Path(item)
        data = Rep.load_json(path / "summary.json" if path.is_dir() else path)
        if data is None:
            raise SystemExit(f"no summary in {item}")
        name = data["meta"].get("profile", path.name)
        if name in summaries:
            name = f"{name} ({path.name})"
        summaries[name] = data
    print(Rep.compare(summaries), end="")
    return 0


def cmd_judge(args) -> int:
    env = {
        **os.environ,
        "VOICE_MODELS_DIR": os.environ.get("VOICE_MODELS_DIR", str(R.models_dir())),
    }
    base = [
        "docker", "compose", "-f", str(VOICE_MODELS_COMPOSE), "-f", str(JUDGE_COMPOSE),
        "-p", JUDGE_PROJECT, "--profile", "judge",
    ]  # fmt: skip
    if args.action == "up":
        subprocess.run([*base, "up", "-d", "judge"], env=env, check=True)
        log("judge (Qwen3-ASR 0.6B on the CPU) on http://127.0.0.1:18495/v1")
    else:
        subprocess.run([*base, "down"], env=env, check=True)
    return 0


def cmd_bench_down(args) -> int:
    from tests.voice_bench import srw
    from tests.voice_bench.settings import Compose, Network

    os.environ["COMPOSE_PROJECT_NAME"] = COMPOSE_PROJECT
    compose = Compose(Network(args.subnet), srw=True)
    srw.down(compose)
    compose.run(
        "--profile", "tools", "down", "--remove-orphans", check=False, timeout=120
    )
    return 0


def cmd_list(args) -> int:
    evalset = spec.load()
    from collections import Counter

    cats = Counter(u.category for u in evalset.corpus.utterances.values())
    clips = R.corpus_clips(evalset)
    print(f"voices: {', '.join(evalset.voices.voices)}")
    print(f"conditions: {', '.join(evalset.conditions)}")
    print(f"corpus: {len(evalset.corpus.utterances)} utterances, {len(clips)} clips")
    for cat, n in sorted(cats.items()):
        print(f"  {cat:15s} {n}")
    print(
        f"scenarios: {len(evalset.scenarios)}, {sum(len(s.turns) for s in evalset.scenarios.values())} turns"
    )
    for s in evalset.scenarios.values():
        print(
            f"  {s.id:28s} {len(s.turns)} turns  {s.voice:13s} {s.condition:8s} {s.title}"
        )
    print(f"real recordings: {len(evalset.human)}")
    print(f"pieces to render: {len(R.needed_pieces(evalset))}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--subnet", default=os.environ.get("VOICE_EVAL_SUBNET", SUBNET))
    sub = parser.add_subparsers(dest="command", required=True)

    def profile_opts(p):
        p.add_argument(
            "--profile-file",
            type=Path,
            default=REPO_ROOT / "docker" / "voice-models" / "cascade-gpu.yaml",
        )
        p.add_argument("--profile", default="cascade-gpu-rocm")
        p.add_argument(
            "--judge-url",
            help="OpenAI-compatible transcription server for what the caller heard",
        )
        p.add_argument("--judge-model", default="judge")
        p.add_argument(
            "--judge-label",
            help='what the judge is, for the report (e.g. "Qwen3-ASR 0.6B on the CPU")',
        )
        p.add_argument(
            "--llm-judge-url",
            help="LLM for meaning checks (default: the profile's LLM)",
        )
        p.add_argument("--llm-judge-model")
        p.add_argument("--no-llm-judge", action="store_true")
        p.add_argument(
            "--out",
            help="results directory (default: tests/voice_bench/results/eval-<UTC>-<profile>)",
        )
        p.add_argument(
            "--baseline",
            help="baseline summary to diff against (default: baselines/<profile>.json)",
        )

    render = sub.add_parser(
        "render", help="render the caller corpus (Qwen3-TTS and Piper, CPU)"
    )
    render.add_argument("--workers", type=int, default=2)
    render.add_argument("--threads", type=int, default=8)
    render.add_argument("--only", choices=("voices",))
    render.add_argument("--force", action="store_true", help="render every piece again")
    render.add_argument(
        "--force-voices", action="store_true", help="design every voice again"
    )
    render.add_argument(
        "--judge-url",
        help="re-render pieces this transcription server cannot read back",
    )
    render.add_argument("--judge-model", default="judge")
    render.add_argument(
        "--try-seeds", help="render candidate takes for these seeds (choosing a voice)"
    )
    render.add_argument(
        "--voices", help="with --try-seeds: these voices (default: every designed one)"
    )

    run = sub.add_parser(
        "run", help="run levels, write the summary, diff against the baseline"
    )
    profile_opts(run)
    run.add_argument("--levels", default="stt,file")
    run.add_argument("--conditions", help="STT level: only these conditions")
    run.add_argument("--categories", help="STT level: only these categories")
    run.add_argument("--voices", help="STT level: only these voices")
    run.add_argument("--limit", type=int, help="STT level: at most this many clips")
    run.add_argument(
        "--parallel", type=int, default=2, help="STT level: requests at once"
    )
    run.add_argument("--scenarios", help="scenario ids (or prefixes), comma-separated")
    run.add_argument(
        "--threads",
        type=int,
        default=8,
        help="whisper.cpp threads (call level without a judge)",
    )
    run.add_argument(
        "--keep-up", action="store_true", help="call level: leave the bench running"
    )
    run.add_argument("--verbose", action="store_true")

    report = sub.add_parser(
        "report", help="summary.md (and diff.md) of a run directory again"
    )
    report.add_argument("run")
    report.add_argument("--baseline")
    report.add_argument(
        "--rescore",
        action="store_true",
        help="score the stored observations again with the current matchers (no calls)",
    )
    report.add_argument(
        "--profile-file",
        type=Path,
        default=REPO_ROOT / "docker" / "voice-models" / "cascade-gpu.yaml",
    )
    report.add_argument("--profile", default="cascade-gpu-rocm")
    report.add_argument("--llm-judge-url")
    report.add_argument("--llm-judge-model")
    report.add_argument("--no-llm-judge", action="store_true")
    diff = sub.add_parser("diff", help="a run against the stored baseline")
    diff.add_argument("run")
    diff.add_argument("--baseline")
    diff.add_argument(
        "--strict", action="store_true", help="exit 1 when a metric got worse"
    )
    baseline = sub.add_parser(
        "baseline", help="store a run's summary as its profile's baseline"
    )
    baseline.add_argument("run")
    compare = sub.add_parser("compare", help="runs or baselines side by side")
    compare.add_argument("runs", nargs="+")
    judge = sub.add_parser("judge", help="the CPU judge (Qwen3-ASR 0.6B) on :18495")
    judge.add_argument("action", choices=("up", "down"))
    sub.add_parser(
        "bench-down", help="stop the call level's bench (project srw-voice-eval)"
    )
    sub.add_parser("list", help="what the test set holds")

    args = parser.parse_args()
    handlers = {
        "render": cmd_render,
        "run": cmd_run,
        "report": cmd_report,
        "diff": cmd_diff,
        "baseline": cmd_baseline,
        "compare": cmd_compare,
        "judge": cmd_judge,
        "bench-down": cmd_bench_down,
        "list": cmd_list,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
