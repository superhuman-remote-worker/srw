"""One scripted call through the bench PBX (A1, local_voice_gateway.md §5).

``scripts/voice-bench.sh call --script FILE`` places one call: the caller
dials the script's number on the bench PBX and says the script's lines at
fixed times after the call was answered, the human extension 3000 answers a
transfer and plays its line, and the caller hangs up at ``hangup_after``.
Whatever answers the number is not driven by the bench: for A1 it is SRW's
addon (``voice-bench.sh addon up``) with a gateway on the host and SRW's live
job on k3d behind it. Afterwards whisper transcribes what the caller heard,
as a timeline and per ``expect`` window, and the report adds the PBX's view
of a transfer (REFER, its answer, the NOTIFY, the human answering).
``--judge-url`` hands that transcription to an OpenAI-compatible server
instead (WP4: whisper small could not understand the cascade's voice; the
judge should be a stronger model than the STT stage under test uses).

A script (YAML; times in seconds after the call was answered)::

    number: "107"                 # bench number; 107 accepts a blind REFER
    hangup_after: 170
    lines:
      - at: 13
        utterance: a02-checkout   # an id from testset/de/utterances.yaml
      - at: 100
        text: Verbinden Sie mich bitte mit der Rezeption.
        speaker: 10               # Piper de_DE-mls-medium speaker (default 10)
    expect:                       # what the caller must hear
      - name: greeting
        to: 13
        keywords: [beispielhof]   # all must appear; "a|b" accepts either
      - name: human
        from: human               # from the moment 3000 answered
        keywords: [mitarbeiter, verbunden]

Every line is rendered with the bench's Piper voice (test-set lines are
reused) and placed in one caller WAV, which is synthetic test speech; the
caller's recording of the far end is test equipment, like every bench
recording.
"""

from __future__ import annotations

import array
import json
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from tests.voice_bench import render_audio
from tests.voice_bench.analysis import (
    final_status,
    load_events,
    missing_keywords,
    parse_asterisk_sip_log,
    read_wav,
    whisper_text,
    write_wav,
)
from tests.voice_bench.profiles import (
    HUMAN_EXTENSION,
    CallData,
    Check,
    Profile,
    Window,
)
from tests.voice_bench.settings import AUDIO_DIR, RUN_DIR, Compose, load_secrets

RATE = 16000
#: Silence after the hangup time, so the looping player never starts over.
TAIL_S = 30.0
DEFAULT_SPEAKER = 10
CALLER_AUDIO = "caller-script.16k.wav"


class ScriptError(ValueError):
    pass


@dataclass(frozen=True)
class Line:
    at: float
    utterance: str | None = None
    text: str | None = None
    speaker: int = DEFAULT_SPEAKER

    @property
    def label(self) -> str:
        return self.utterance or (self.text or "")[:40]


@dataclass(frozen=True)
class Expect:
    name: str
    keywords: list[str]
    start: float | str = 0.0  # seconds after answer, or "human"
    end: float | None = None  # seconds after answer; None = end of the call


@dataclass
class CallScript:
    number: str
    hangup_after: float
    lines: list[Line]
    expect: list[Expect] = field(default_factory=list)
    caller_ext: str = "1000"


def _seconds(value, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ScriptError(f"{label} must be a number of seconds")
    return float(value)


def parse_script(raw: dict) -> CallScript:
    if not isinstance(raw, dict):
        raise ScriptError("a call script is a mapping")
    unknown = set(raw) - {"number", "hangup_after", "lines", "expect", "caller_ext"}
    if unknown:
        raise ScriptError(f"unknown script fields: {', '.join(sorted(unknown))}")
    number = str(raw.get("number", ""))
    if not number.isdigit():
        raise ScriptError("number must be a bench dialplan number (digits)")
    hangup = _seconds(raw.get("hangup_after"), "hangup_after")
    lines = []
    for index, item in enumerate(raw.get("lines") or []):
        label = f"lines[{index}]"
        if not isinstance(item, dict) or set(item) - {
            "at",
            "utterance",
            "text",
            "speaker",
        }:
            raise ScriptError(f"{label} takes at, utterance or text, speaker")
        if bool(item.get("utterance")) == bool(item.get("text")):
            raise ScriptError(f"{label} needs either utterance or text")
        speaker = item.get("speaker", DEFAULT_SPEAKER)
        if type(speaker) is not int or speaker < 0:
            raise ScriptError(f"{label}.speaker must be a speaker id")
        lines.append(
            Line(
                at=_seconds(item.get("at"), f"{label}.at"),
                utterance=item.get("utterance"),
                text=item.get("text"),
                speaker=speaker,
            )
        )
    if not lines:
        raise ScriptError("a call script needs lines")
    if any(b.at <= a.at for a, b in zip(lines, lines[1:])):
        raise ScriptError("lines must be in time order")
    if lines[-1].at >= hangup:
        raise ScriptError("every line must start before hangup_after")
    expects = []
    for index, item in enumerate(raw.get("expect") or []):
        label = f"expect[{index}]"
        if not isinstance(item, dict) or set(item) - {"name", "from", "to", "keywords"}:
            raise ScriptError(f"{label} takes name, from, to, keywords")
        keywords = item.get("keywords")
        if not isinstance(keywords, list) or not keywords:
            raise ScriptError(f"{label}.keywords must be a nonempty list")
        start = item.get("from", 0.0)
        if start != "human":
            start = _seconds(start, f"{label}.from")
        end = item.get("to")
        expects.append(
            Expect(
                name=str(item.get("name") or f"expect{index}"),
                keywords=[str(k) for k in keywords],
                start=start,
                end=None if end is None else _seconds(end, f"{label}.to"),
            )
        )
    return CallScript(
        number=number,
        hangup_after=hangup,
        lines=lines,
        expect=expects,
        caller_ext=str(raw.get("caller_ext", "1000")),
    )


def load_script(path: Path) -> CallScript:
    return parse_script(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def render_jobs(script: CallScript) -> list[dict]:
    """Piper jobs for the script's free text lines (test-set lines exist)."""
    return [
        render_audio.text_job(line.text, line.speaker)
        for line in script.lines
        if line.text
    ]


def line_audio(line: Line) -> Path:
    if line.utterance:
        return AUDIO_DIR / "testset" / f"{line.utterance}.16k.wav"
    return (
        AUDIO_DIR / f"{render_audio.text_job(line.text, line.speaker)['name']}.16k.wav"
    )


def compose_audio(
    clips: list[tuple[float, array.array]], total_s: float, rate: int = RATE
) -> array.array:
    """One track with every clip at its start time, silence elsewhere."""
    out = array.array("h", bytes(2 * int(total_s * rate)))
    end_of_last = 0
    for at, samples in clips:
        first = int(at * rate)
        if first < end_of_last:
            raise ScriptError(
                f"the line at {at} s starts before the previous one ended"
            )
        last = first + len(samples)
        if last > len(out):
            raise ScriptError(f"the line at {at} s runs past the end of the call")
        out[first:last] = samples
        end_of_last = last
    return out


def write_caller_audio(script: CallScript, dst: Path) -> list[dict]:
    """Write the caller's WAV; returns where each line sits and how long it is."""
    clips, placed = [], []
    for line in script.lines:
        path = line_audio(line)
        if not path.exists():
            raise ScriptError(f"no audio for {line.label!r}: run voice-bench.sh audio")
        rate, samples = read_wav(path)
        if rate != RATE:
            raise ScriptError(f"{path.name} is {rate} Hz, expected {RATE}")
        clips.append((line.at, samples))
        placed.append(
            {
                "at_s": line.at,
                "seconds": round(len(samples) / rate, 2),
                "line": line.utterance or line.text,
            }
        )
    write_wav(dst, RATE, compose_audio(clips, script.hangup_after + TAIL_S))
    return placed


def scenarios(script: CallScript, pbx: str) -> dict[str, dict]:
    secrets = load_secrets()
    return {
        "human": {
            "role": "human",
            "account": {
                "id": f"sip:{HUMAN_EXTENSION}@{pbx}",
                "registrar": f"sip:{pbx}",
                "username": HUMAN_EXTENSION,
                "password": secrets["HUMAN_PASSWORD"],
            },
            "play": "/bench/audio/human.16k.wav",
            "auto_answer": 200,
            "max_seconds": script.hangup_after + 120,
        },
        "caller": {
            "role": "caller",
            "account": {
                "id": f"sip:{script.caller_ext}@{pbx}",
                "username": script.caller_ext,
                "password": secrets["CALLER_PASSWORD"],
            },
            "play": f"/bench/out/{CALLER_AUDIO}",
            "call": f"sip:{script.number}@{pbx}",
            "steps": [{"at": script.hangup_after, "do": "hangup"}],
            "exit_after_call": True,
            "max_seconds": script.hangup_after + 60,
        },
    }


def windows(script: CallScript, data: CallData) -> dict[str, Window]:
    """One window per ``expect`` entry, in wall-clock time."""
    t0 = data.t_confirmed("caller")
    out: dict[str, Window] = {}
    if t0 is None:
        return out
    t_human = data.t_confirmed("human")
    for exp in script.expect:
        if exp.start == "human":
            if t_human is None:
                continue
            start = t_human + 0.5
        else:
            start = t0 + exp.start
        end = None if exp.end is None else t0 + exp.end
        out[exp.name] = Window("caller", start, end, exp.keywords, exp.name)
    return out


def stretches(
    samples: array.array,
    rate: int,
    *,
    level: int = 500,
    gap_s: float = 0.5,
    min_s: float = 0.3,
    pad_s: float = 0.15,
) -> list[tuple[float, float]]:
    """Voiced stretches (start, end in seconds) of a recording: 20 ms frames
    above ``level``, joined across pauses shorter than ``gap_s``."""
    step = rate // 50
    voiced = [
        max(map(abs, samples[i : i + step]), default=0) > level
        for i in range(0, len(samples), step)
    ]
    found: list[list[int]] = []
    for index, on in enumerate(voiced):
        if not on:
            continue
        if found and index - found[-1][1] <= gap_s * 50:
            found[-1][1] = index + 1
        else:
            found.append([index, index + 1])
    total = len(samples) / rate
    return [
        (round(max(0.0, a / 50 - pad_s), 2), round(min(total, b / 50 + pad_s), 2))
        for a, b in found
        if (b - a) / 50 >= min_s
    ]


def judge_text(url: str, model: str, wav: bytes, language: str = "de") -> str:
    """What an OpenAI-compatible ``/audio/transcriptions`` server hears."""
    boundary = uuid.uuid4().hex
    parts = [
        f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
        f"{value}\r\n".encode()
        for name, value in (
            ("model", model),
            ("language", language),
            ("response_format", "json"),
            ("temperature", "0"),
        )
    ]
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
        'filename="heard.wav"\r\nContent-Type: audio/wav\r\n\r\n'.encode()
        + wav
        + f"\r\n--{boundary}--\r\n".encode()
    )
    request = urllib.request.Request(
        url.rstrip("/") + "/audio/transcriptions",
        data=b"".join(parts),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310 - the operator's judge URL
        body = json.loads(response.read())
    return " ".join(str(body.get("text", "")).split())


def heard_timeline(
    compose: Compose,
    data: CallData,
    out: Path,
    threads: int,
    judge: tuple[str, str] | None = None,
) -> list[dict]:
    """What the caller heard: each voiced stretch of its recording transcribed
    on its own (short clips keep whisper from looping over long silences),
    in seconds after the answer."""
    summary = data.summary.get("caller") or {}
    t0 = data.t_confirmed("caller")
    recording = out / "caller.wav"
    if t0 is None or not recording.exists() or not summary.get("recording_seconds"):
        return []
    # pjsua records only while the call's media runs, so the file ends when
    # the call ends.
    rec_end = data.t_disconnected("caller") or summary["t_exit"]
    rec_t0 = rec_end - summary["recording_seconds"]
    rate, samples = read_wav(recording)
    skip = max(0, int((t0 - rec_t0) * rate))
    heard = samples[skip:]
    clips = out / "heard"
    if clips.exists():
        for old in clips.iterdir():
            old.unlink()
    clips.mkdir(exist_ok=True)
    found = stretches(heard, rate)
    names = []
    for index, (start, end) in enumerate(found):
        name = f"{index:03d}.wav"
        write_wav(clips / name, rate, heard[int(start * rate) : int(end * rate)])
        names.append(name)
    if names and judge is not None:
        for name in names:
            text = judge_text(judge[0], judge[1], (clips / name).read_bytes())
            (clips / f"{name}.judge.json").write_text(
                json.dumps({"text": text}, ensure_ascii=False), "utf-8"
            )
    elif names:
        args = ["--profile", "tools", "run", "--rm", "--no-deps", "whisper"]
        args += ["-m", "/models/ggml-small.bin", "-l", "de", "-t", str(threads)]
        args += ["-np", "-mc", "0", "-oj"]
        for name in names:
            args += ["-f", f"/work/{name}"]
        proc = compose.run(*args, work_dir=clips, check=False, timeout=900)
        if proc.returncode != 0:
            (out / "whisper.log").write_text(proc.stdout + proc.stderr, "utf-8")
    timeline = []
    for name, (start, end) in zip(names, found):
        result = clips / f"{name}.json"
        judged = clips / f"{name}.judge.json"
        if judge is not None:
            text = (
                json.loads(judged.read_text("utf-8"))["text"] if judged.exists() else ""
            )
        else:
            text = whisper_text(result) if result.exists() else ""
        timeline.append({"from_s": start, "to_s": end, "text": text})
    return timeline


def window_text(timeline: list[dict], start_s: float, end_s: float | None) -> str:
    """The stretches that start inside a window, joined."""
    return " ".join(
        entry["text"]
        for entry in timeline
        if entry["text"]
        and entry["from_s"] >= start_s
        and (end_s is None or entry["from_s"] < end_s)
    )


def transfer_view(data: CallData) -> dict:
    """The transfer as the PBX saw it from the AI extension's side."""
    msgs = data.dut_msgs
    refer = next(
        (
            m
            for m in msgs
            if m.is_request and m.method == "REFER" and m.direction == "tx"
        ),
        None,
    )
    t0 = data.t_confirmed("caller")
    t_human = data.t_confirmed("human")
    view: dict = {
        "refer_to": refer.first("Refer-To") if refer else None,
        "refer_at_s": round(refer.t - t0, 2) if refer and t0 else None,
        "refer_response": final_status(msgs, refer) if refer else None,
        "notify_sipfrag": [],
        "human_answered_at_s": round(t_human - t0, 2) if t_human and t0 else None,
    }
    if refer:
        view["notify_sipfrag"] = [
            m.body.splitlines()[0]
            for m in msgs
            if m.is_request
            and m.method == "NOTIFY"
            and m.direction == "rx"
            and m.call_id == refer.call_id
            and m.body
        ]
    return view


def checks(script: CallScript, data: CallData, transfer: dict) -> list[Check]:
    t0, t_end = data.t_confirmed("caller"), data.t_disconnected("caller")
    out = [Check("call_answered", t0 is not None, f"caller confirmed={t0 is not None}")]
    if t0 and t_end:
        hung_up = data.first_event("caller", "command", do="hangup") is not None
        out.append(
            Check(
                "call_lasted",
                hung_up and t_end - t0 >= script.hangup_after - 1.0,
                f"{t_end - t0:.1f} s, ended by the caller's hangup={hung_up}",
            )
        )
    for exp in script.expect:
        window = data.windows.get(exp.name)
        if window is None:
            out.append(Check(f"heard_{exp.name}", False, "window not placed"))
            continue
        text = data.transcripts.get(exp.name, "")
        missing = missing_keywords(text, exp.keywords)
        out.append(
            Check(
                f"heard_{exp.name}",
                not missing,
                f"missing {missing}" if missing else "all keywords heard",
            )
        )
    if transfer["refer_to"]:
        out.append(
            Check(
                "transfer_accepted",
                transfer["refer_response"] == 202
                and any(
                    f.startswith("SIP/2.0 200") for f in transfer["notify_sipfrag"]
                ),
                f"REFER {transfer['refer_response']}, NOTIFY {transfer['notify_sipfrag']}",
            )
        )
    return out


def run(
    compose: Compose,
    script: CallScript,
    out: Path,
    threads: int,
    log=print,
    judge: tuple[str, str] | None = None,
) -> dict:
    """Place the call, then analyze it (``out/call.json``); returns the report."""
    # Imported here: bench.py imports this module for its CLI.
    from tests.voice_bench.bench import registered, wait_for

    network = compose.network
    out.mkdir(parents=True, exist_ok=True)
    jobs = render_jobs(script)
    if jobs:
        rendered = render_audio.render(compose, jobs=jobs, log=log)
        if rendered:
            log(f"rendered {rendered} call line(s)")
    placed = write_caller_audio(script, out / CALLER_AUDIO)
    (out / "lines.json").write_text(json.dumps(placed, ensure_ascii=False), "utf-8")
    for role, scenario in scenarios(script, network.pbx).items():
        scenario["out_dir"] = "/bench/out"
        (out / f"{role}.json").write_text(json.dumps(scenario, indent=2), "utf-8")

    compose.run(
        "up", "-d", "--force-recreate", "--no-deps", "human", out_dir=out, timeout=60
    )
    wait_for(lambda: registered(out, "human"), 20, "the human extension to register")
    contacts = compose.asterisk("pjsip show contacts")
    (out / "contacts_before.txt").write_text(contacts, encoding="utf-8")
    if "2000/" not in contacts:
        log(
            "warning: nothing registered as 2000; is the addon up (voice-bench.sh addon up)?"
        )
    pbx_log = RUN_DIR / "pbx" / "log" / "full"
    offset = pbx_log.stat().st_size if pbx_log.exists() else 0
    log(f"calling {script.number}; hanging up after {script.hangup_after:.0f} s")
    compose.run(
        "up", "-d", "--force-recreate", "--no-deps", "caller", out_dir=out, timeout=60
    )
    container = compose.run("ps", "-q", "caller", out_dir=out).stdout.strip()
    try:
        subprocess.run(
            ["docker", "wait", container],
            capture_output=True,
            text=True,
            timeout=script.hangup_after + 120,
            check=False,
        )
    finally:
        compose.run(
            "stop", "-t", "15", "caller", "human", out_dir=out, check=False, timeout=60
        )
    time.sleep(0.3)
    with pbx_log.open("rb") as fh:
        fh.seek(offset)
        pbx_text = fh.read().decode("utf-8", errors="replace")
    (out / "pbx.log").write_text(pbx_text, encoding="utf-8")
    return analyze(compose, script, out, threads, log=log, judge=judge)


def analyze(
    compose: Compose,
    script: CallScript,
    out: Path,
    threads: int,
    log=print,
    judge: tuple[str, str] | None = None,
) -> dict:
    """Read a placed call's artifacts in ``out`` and write ``out/call.json``;
    also runs on an earlier results directory (``call --analyze DIR``)."""
    network = compose.network
    roles = ("caller", "human")
    events = {role: load_events(out / f"{role}.events.jsonl") for role in roles}
    summary = {}
    for role in roles:
        path = out / f"{role}.summary.json"
        summary[role] = json.loads(path.read_text("utf-8")) if path.exists() else {}
    pbx_path = out / "pbx.log"
    pbx_text = pbx_path.read_text("utf-8") if pbx_path.exists() else ""
    data = CallData(
        profile=Profile(
            name="call",
            number=script.number,
            imitates="scripted call",
            hangup_after=script.hangup_after,
        ),
        out=out,
        standin=False,
        events=events,
        summary=summary,
        dut_msgs=parse_asterisk_sip_log(pbx_text, {network.caller, network.human}),
        keywords={},
        heard_from_dut=[],
        dut_mode="external",
    )
    data.windows = windows(script, data)
    log("transcribing what the caller heard")
    heard = heard_timeline(compose, data, out, threads, judge)
    t0 = data.t_confirmed("caller")
    report_windows = {}
    for name, w in data.windows.items():
        start = round(w.start - t0, 2)
        end = None if w.end is None else round(w.end - t0, 2)
        data.transcripts[name] = window_text(heard, start, end)
        report_windows[name] = {
            "from_s": start,
            "to_s": end,
            "keywords": w.keywords,
            "transcript": data.transcripts[name],
        }
    transfer = transfer_view(data)
    lines_path = out / "lines.json"
    report_path = out / "call.json"
    # A second analysis keeps what only the call itself could record.
    earlier = json.loads(report_path.read_text("utf-8")) if report_path.exists() else {}
    report = {
        "number": script.number,
        "hangup_after_s": script.hangup_after,
        "lines": json.loads(lines_path.read_text("utf-8"))
        if lines_path.exists()
        else earlier.get("lines", []),
        "judge": f"{judge[1]} at {judge[0]}" if judge else "whisper.cpp small",
        "timeline": heard,
        "windows": report_windows,
        "transfer": transfer,
    }
    if "no_audio_written" in earlier:
        report["no_audio_written"] = earlier["no_audio_written"]
    found = checks(script, data, transfer)
    report["checks"] = [c.as_dict() for c in found]
    report["status"] = "fail" if any(c.ok is False for c in found) else "pass"
    if not report.get("no_audio_written", {"ok": True})["ok"]:
        report["status"] = "fail"
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return report
