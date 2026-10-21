"""WP0 phone test bench runner (entry point: scripts/voice-bench.sh).

    scripts/voice-bench.sh run [--profile a,b] [--skip-slow] [--dut standin|external|srw ...]
    scripts/voice-bench.sh list | models | audio | build | standin up|down | down
    scripts/voice-bench.sh addon up|down [--line NAME] | call --script FILE   (A1)

``run`` downloads the pinned models, builds the images, renders the German
WAVs, starts the bench PBX and runs each selected profile as one phone call.
It writes ``tests/voice_bench/results/<timestamp>/report.json`` (plus logs,
recordings and transcripts per profile) and exits non-zero when a profile
fails. See tests/voice_bench/README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import yaml

from tests.voice_bench import assets, render_audio, srw
from tests.voice_bench.analysis import (
    load_events,
    messages_from_events,
    parse_asterisk_sip_log,
    read_wav,
    slice_wav,
    voiced_ratio,
    whisper_text,
)
from tests.voice_bench.profiles import (
    HUMAN_EXTENSION,
    PROFILES,
    TRANSFER_DIGIT,
    CallData,
    Check,
    Profile,
    common_checks,
    default_windows,
)
from tests.voice_bench.settings import (
    BENCH_DIR,
    MODELS_DIR,
    RESULTS_DIR,
    RUN_DIR,
    Compose,
    ExternalDut,
    Network,
    load_secrets,
)

TOOLS = {
    "asterisk": "22.11.0 (downloads.asterisk.org, sha256 + GPG)",
    "pjsua": "pjproject 2.17",
    "whisper.cpp": "v1.9.5, ggml-small.bin (multilingual)",
    "piper": "2023.11.14-2, de_DE-mls-medium",
}
PLACEHOLDER_CONFIGS = (
    "acl.conf",
    "ari.conf",
    "ccss.conf",
    "chan_websocket.conf",
    "features.conf",
    "pjproject.conf",
    "pjsip_notify.conf",
    "pjsip_wizard.conf",
    "udptl.conf",
    "websocket_client.conf",
)


def log(message: str) -> None:
    print(f"[voice-bench] {message}", flush=True)


# -- setup -------------------------------------------------------------------------------


def ensure_images(compose: Compose, rebuild: bool = False) -> None:
    """Build the bench images; Docker's layer cache makes this quick when nothing changed."""
    log("building images (cached layers are reused)")
    flags = ["--no-cache"] if rebuild else []
    compose.run("build", *flags, "pbx", "dut", capture=False, timeout=3600)
    compose.run(
        "--profile",
        "tools",
        "build",
        *flags,
        "whisper",
        "piper",
        capture=False,
        timeout=3600,
    )


def render_pbx_config(
    network: Network, external: ExternalDut | None, peer: tuple[str, int]
) -> None:
    """Write the PBX config into the run directory (in place: it may be mounted)."""
    secrets = load_secrets()
    etc = RUN_DIR / "pbx" / "etc"
    logs = RUN_DIR / "pbx" / "log"
    etc.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    # The PBX runs as uid 10001 and writes its log into this bind mount.
    logs.chmod(0o777)
    for src in (BENCH_DIR / "pbx").glob("*.conf"):
        shutil.copyfile(src, etc / src.name)
    for name in PLACEHOLDER_CONFIGS:
        (etc / name).write_text("; intentionally empty (bench)\n", encoding="utf-8")
    external_lines = ""
    if external:
        external_lines = (
            f"external_media_address = {external.advertise}\n"
            f"external_signaling_address = {external.advertise}\n"
            f"external_signaling_port = {external.sip_port}"
        )
    values = {
        "BENCH_SUBNET": network.subnet,
        "EXTERNAL_ADDRESS_LINES": external_lines,
        "DUT_PEER_HOST": peer[0],
        "DUT_PEER_PORT": str(peer[1]),
        **secrets,
    }
    text = (BENCH_DIR / "pbx" / "pjsip.conf.in").read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(f"@@{key}@@", value)
    if "@@" in text:
        raise RuntimeError("pjsip.conf.in has an unknown @@placeholder@@")
    (etc / "pjsip.conf").write_text(text, encoding="utf-8")
    for path in etc.iterdir():
        path.chmod(0o644)


def pbx_running(compose: Compose) -> bool:
    result = compose.run("ps", "--status", "running", "-q", "pbx", check=False)
    return bool(result.stdout.strip())


def start_pbx(compose: Compose, fresh: bool) -> None:
    """Start the PBX; ``fresh`` recreates it (empty registrations and log)."""
    if fresh:
        compose.run("rm", "-s", "-f", "pbx", check=False, timeout=60)
        for stale in (RUN_DIR / "pbx" / "log").iterdir():
            stale.unlink()
    elif pbx_running(compose):
        # Pick up a changed config; registrations survive a reload.
        compose.asterisk("core reload")
    compose.run("up", "-d", "--wait", "--wait-timeout", "60", "pbx", timeout=120)
    log("bench PBX is up")


def setup_pbx(args: argparse.Namespace, fresh: bool) -> tuple[Compose, tuple[str, int]]:
    """Render the PBX config for these options and start (or reuse) the PBX."""
    network = Network(args.subnet)
    peer = (network.dut, 5060)
    if getattr(args, "dut_peer", None):
        peer = parse_host_port(args.dut_peer)
    external = None
    if getattr(args, "external_address", None):
        external = ExternalDut(args.external_address, args.sip_port)
    compose = Compose(network, external, srw=getattr(args, "dut", None) == "srw")
    render_pbx_config(network, external, peer)
    start_pbx(compose, fresh=fresh or not pbx_running(compose))
    return compose, peer


def print_device_hints(compose: Compose, args: argparse.Namespace) -> None:
    ext = compose.external
    host = ext.advertise if ext else compose.network.pbx
    port = ext.sip_port if ext else 5060
    log(
        f"device under test: register extension 2000 at sip:{host}:{port} (UDP or TCP; "
        "password VOICE_BENCH_AI_PASSWORD in tests/voice_bench/.env), or answer the "
        f"peer profile's INVITE at {args.dut_peer or compose.network.dut + ':5060'}; on "
        f"DTMF {TRANSFER_DIGIT} blind-transfer (REFER) to sip:{HUMAN_EXTENSION}@{host}"
    )


# -- one profile ---------------------------------------------------------------------------


def scenarios(profile: Profile, network: Network, standin: bool) -> dict[str, dict]:
    secrets = load_secrets()
    pbx = network.pbx
    audio = "/bench/audio"
    out = {
        "human": {
            "role": "human",
            "account": {
                "id": f"sip:{HUMAN_EXTENSION}@{pbx}",
                "registrar": f"sip:{pbx}",
                "username": HUMAN_EXTENSION,
                "password": secrets["HUMAN_PASSWORD"],
            },
            "play": f"{audio}/human.16k.wav",
            "auto_answer": 200,
            "max_seconds": 900,
        },
        "caller": {
            "role": "caller",
            "account": {
                "id": f"sip:{profile.caller_ext}@{pbx}",
                "username": profile.caller_ext,
                "password": secrets["CALLER_PASSWORD"],
            },
            "play": f"{audio}/caller.16k.wav",
            "call": f"sip:{profile.number}@{pbx}",
            "steps": [
                *profile.caller_steps,
                {"at": profile.hangup_after, "do": "hangup"},
            ],
            "exit_after_call": True,
            "max_seconds": profile.hangup_after + 40,
        },
    }
    if standin:
        account: dict = {}
        if profile.dut_registers:
            account = {
                "id": f"sip:2000@{pbx}",
                "registrar": f"sip:{pbx}",
                "username": "2000",
                "password": secrets["AI_PASSWORD"],
            }
        out["dut"] = standin_scenario(network, account)
        if not profile.transfer_on_dtmf:
            out["dut"].pop("on_dtmf")
    return out


def standin_scenario(network: Network, account: dict) -> dict:
    return {
        "role": "dut",
        "account": account,
        "play": "/bench/audio/dut.16k.wav",
        "auto_answer": 200,
        "on_dtmf": {
            TRANSFER_DIGIT: {
                "do": "transfer",
                "uri": f"sip:{HUMAN_EXTENSION}@{network.pbx}",
            }
        },
        "max_seconds": 86400,
    }


def wait_for(predicate, timeout: float, what: str) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise TimeoutError(f"timed out waiting for {what}")


def registered(out: Path, role: str) -> bool:
    return any(
        e.get("type") == "registration" and e.get("ok")
        for e in load_events(out / f"{role}.events.jsonl")
    )


def started(out: Path, role: str) -> bool:
    return any(
        e.get("type") == "start" for e in load_events(out / f"{role}.events.jsonl")
    )


def run_call(compose: Compose, profile: Profile, out: Path, standin: bool) -> str:
    """Place the profile's call; returns the PBX log written during it."""
    network = compose.network
    for role, scenario in scenarios(profile, network, standin).items():
        scenario["out_dir"] = "/bench/out"
        (out / f"{role}.json").write_text(
            json.dumps(scenario, indent=2), encoding="utf-8"
        )
    parties = ["human", "dut"] if standin else ["human"]
    compose.run(
        "up", "-d", "--force-recreate", "--no-deps", *parties, out_dir=out, timeout=60
    )
    wait_for(lambda: registered(out, "human"), 20, "the human extension to register")
    if standin and profile.dut_registers:
        wait_for(lambda: registered(out, "dut"), 20, "the stand-in DUT to register")
    elif standin:
        wait_for(lambda: started(out, "dut"), 20, "the stand-in DUT to start")
        time.sleep(1.0)
    else:
        contacts = compose.asterisk("pjsip show contacts")
        if profile.dut_registers and "2000/" not in contacts:
            log(
                "warning: no contact registered for 2000; is the external device registered?"
            )
    pbx_log = RUN_DIR / "pbx" / "log" / "full"
    offset = pbx_log.stat().st_size if pbx_log.exists() else 0
    (out / "contacts_before.txt").write_text(
        compose.asterisk("pjsip show contacts"), encoding="utf-8"
    )
    compose.run(
        "up", "-d", "--force-recreate", "--no-deps", "caller", out_dir=out, timeout=60
    )
    container = compose.run("ps", "-q", "caller", out_dir=out).stdout.strip()
    try:
        subprocess.run(
            ["docker", "wait", container],
            capture_output=True,
            text=True,
            timeout=profile.hangup_after + 60,
            check=False,
        )
    finally:
        compose.run(
            "stop", "-t", "15", "caller", *parties, out_dir=out, check=False, timeout=60
        )
    time.sleep(0.3)
    with pbx_log.open("rb") as fh:
        fh.seek(offset)
        text = fh.read().decode("utf-8", errors="replace")
    (out / "pbx.log").write_text(text, encoding="utf-8")
    return text


def place_windows(data: CallData) -> None:
    p = data.profile
    data.windows = p.windows(data) if p.windows else default_windows(data)


def transcribe(compose: Compose, data: CallData, threads: int) -> None:
    """Cut every window out of its recording, measure it and run whisper once."""
    jobs = []
    for name, window in data.windows.items():
        summary = data.summary.get(window.role) or {}
        rec = data.out / f"{window.role}.wav"
        if not rec.exists() or not summary.get("recording_seconds"):
            data.audio[name] = {"error": "no recording"}
            continue
        # pjsua's recorder only receives frames while the call's media is
        # connected, so the file ends when the call ends.
        rec_end = data.t_disconnected(window.role) or summary["t_exit"]
        rec_t0 = rec_end - summary["recording_seconds"]
        start = (
            window.start if window.start is not None else data.t_confirmed(window.role)
        )
        end = window.end if window.end is not None else data.t_disconnected(window.role)
        if start is None:
            data.audio[name] = {"error": "call never confirmed"}
            continue
        end = end if end is not None else summary["t_exit"]
        clip = data.out / f"{name}.wav"
        seconds = slice_wav(rec, clip, start - rec_t0, end - rec_t0)
        rate, samples = read_wav(clip)
        data.audio[name] = {
            "seconds": round(seconds, 2),
            "from_s": round(start - rec_t0, 2),
            "voiced": round(voiced_ratio(rate, samples), 3),
        }
        if seconds >= 1.0:
            jobs.append(name)
    if not jobs:
        return
    args = ["--profile", "tools", "run", "--rm", "--no-deps", "whisper"]
    args += [
        "-m",
        "/models/ggml-small.bin",
        "-l",
        "de",
        "-t",
        str(threads),
        "-np",
        "-oj",
    ]
    for name in jobs:
        args += ["-f", f"/work/{name}.wav"]
    proc = compose.run(*args, work_dir=data.out, check=False, timeout=900)
    if proc.returncode != 0:
        (data.out / "whisper.log").write_text(
            proc.stdout + proc.stderr, encoding="utf-8"
        )
    for name in jobs:
        out_json = data.out / f"{name}.wav.json"
        data.transcripts[name] = whisper_text(out_json) if out_json.exists() else ""


def run_profile(
    compose: Compose,
    profile: Profile,
    out: Path,
    mode: str,
    heard_from_dut: list[str],
    threads: int,
) -> dict:
    standin = mode == "standin"
    started_at = time.time()
    result = {
        "name": profile.name,
        "number": profile.number,
        "imitates": profile.imitates,
        "slow": profile.slow,
        "approximation": profile.approximation,
        "artifacts": os.path.relpath(out, BENCH_DIR),
    }
    try:
        out.mkdir(parents=True, exist_ok=True)
        offset = srw.driver_offset() if mode == "srw" else 0
        pbx_text = run_call(compose, profile, out, standin)
        driver = srw.driver_events(offset) if mode == "srw" else []
        if driver:
            (out / "driver.events.jsonl").write_text(
                "".join(json.dumps(e) + "\n" for e in driver), encoding="utf-8"
            )
        roles = ("caller", "dut", "human") if standin else ("caller", "human")
        events = {role: load_events(out / f"{role}.events.jsonl") for role in roles}
        summary = {}
        for role in roles:
            path = out / f"{role}.summary.json"
            summary[role] = (
                json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            )
        network = compose.network
        if standin:
            dut_msgs = messages_from_events(events["dut"])
        else:
            dut_msgs = parse_asterisk_sip_log(pbx_text, {network.caller, network.human})
        voices = render_audio.load_voices()["bench"]
        data = CallData(
            profile=profile,
            out=out,
            standin=standin,
            events=events,
            summary=summary,
            dut_msgs=dut_msgs,
            keywords={role: spec["keywords"] for role, spec in voices.items()},
            heard_from_dut=heard_from_dut,
            contacts_before=(out / "contacts_before.txt").read_text(encoding="utf-8"),
            dut_mode=mode,
            driver=driver,
        )
        place_windows(data)
        transcribe(compose, data, threads)
        checks: list[Check] = []
        if profile.name == "refer-accept":
            checks += [
                c
                for c in common_checks(data)
                if c.name not in ("caller_hears_dut", "dut_hears_caller")
            ]
        else:
            checks += common_checks(data)
        if profile.checks:
            checks += profile.checks(data)
        failed = [c for c in checks if c.ok is False]
        result.update(
            {
                "status": "fail" if failed else "pass",
                "checks": [c.as_dict() for c in checks],
                "observed": data.observed,
                "transcripts": data.transcripts,
                "audio": data.audio,
            }
        )
    except Exception as exc:  # noqa: BLE001 - a broken profile must not stop the others
        result.update({"status": "error", "error": f"{type(exc).__name__}: {exc}"})
        (out / "error.txt").write_text(traceback.format_exc(), encoding="utf-8")
    result["duration_s"] = round(time.time() - started_at, 1)
    return result


# -- commands ---------------------------------------------------------------------------------


def parse_host_port(value: str, default_port: int = 5060) -> tuple[str, int]:
    host, _, port = value.rpartition(":") if value.count(":") == 1 else (value, "", "")
    return (host or value), int(port) if port else default_port


def cmd_run(args: argparse.Namespace) -> int:
    standin = args.dut == "standin"
    if args.dut == "srw":
        # SRW's own addon on the bench network (srw.py): registers as 2000,
        # takes the peer profile at its own address.
        args.dut_peer = args.dut_peer or f"{Network(args.subnet).addon}:5060"
        args.wait_registration = args.wait_registration or 60
    if not standin and not args.dut_peer:
        log(
            "--dut external needs --dut-peer HOST[:PORT] (where the peer profile sends its INVITE)"
        )
        return 2
    names = (
        list(PROFILES)
        if args.profile in (None, "all")
        else [n.strip() for n in args.profile.split(",")]
    )
    unknown = [n for n in names if n not in PROFILES]
    if unknown:
        log(f"unknown profile(s) {unknown}; choose from {list(PROFILES)}")
        return 2
    if args.skip_slow:
        names = [n for n in names if not PROFILES[n].slow]

    voices = render_audio.load_voices()["bench"]
    if args.dut_expect == "standin":
        heard = list(voices["dut"]["keywords"])
    elif args.dut_expect == "echo":
        heard = list(voices["caller"]["keywords"])
    else:
        heard = [k.strip() for k in args.dut_expect.split(",") if k.strip()]

    log("checking pinned models")
    assets.fetch(MODELS_DIR, log=log)
    ensure_images(Compose(Network(args.subnet)), args.build)
    rendered = render_audio.render(Compose(Network(args.subnet)), log=log)
    if rendered:
        log(f"rendered {rendered} WAV set(s)")
    # The stand-in run always starts from a clean PBX; an external device's
    # registration lives in the running PBX, so external runs reuse it. SRW's
    # stack starts fresh with it.
    compose, peer = setup_pbx(args, fresh=args.dut != "external")
    if args.dut == "srw":
        log("starting SRW's Asterisk addon, voice gateway and driver harness")
        srw.prepare(compose.network, f"sip:{HUMAN_EXTENSION}@{compose.network.pbx}")
        srw.up(compose, timeout=args.wait_registration)
    if not standin:
        print_device_hints(compose, args)
        if args.wait_registration:
            wait_for(
                lambda: "2000/" in compose.asterisk("pjsip show contacts"),
                args.wait_registration,
                "extension 2000 to register",
            )
    keep_up = args.keep_up or args.dut == "external"

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    results_dir = Path(args.results).resolve() if args.results else RESULTS_DIR / stamp
    results_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "bench": "SRW voice bench (WP0)",
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": platform.node(),
        "dut": {
            "mode": args.dut,
            "expects_from_dut": heard,
            "peer": f"{peer[0]}:{peer[1]}",
            "external_address": args.external_address,
        },
        "tools": TOOLS,
        "profiles": [],
    }
    exit_code = 0
    try:
        for name in names:
            profile = PROFILES[name]
            log(
                f"profile {name}: dialling {profile.number}"
                + (" (slow)" if profile.slow else "")
            )
            result = run_profile(
                compose, profile, results_dir / name, args.dut, heard, args.threads
            )
            report["profiles"].append(result)
            failed = [
                c["name"] for c in result.get("checks", []) if c["status"] == "fail"
            ]
            log(
                f"profile {name}: {result['status'].upper()} {result['duration_s']} s {failed or ''}"
            )
            if result["status"] != "pass":
                exit_code = 1
            (results_dir / "report.json").write_text(
                json.dumps(report, indent=2, ensure_ascii=False), "utf-8"
            )
        if args.dut == "srw":
            # A5: nothing in the addon, the gateway or the driver wrote audio.
            report["no_audio_written"] = srw.audio_check(compose)
            log(
                "no audio written by addon/gateway/driver: "
                + ("yes" if report["no_audio_written"]["ok"] else "NO")
            )
            if not report["no_audio_written"]["ok"]:
                exit_code = 1
    finally:
        if args.dut == "srw":
            srw.logs(compose, results_dir)
            if not keep_up:
                srw.down(compose)
        if not keep_up:
            compose.run("down", "--remove-orphans", check=False, timeout=120)
    statuses = [p["status"] for p in report["profiles"]]
    report["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    report["summary"] = {s: statuses.count(s) for s in ("pass", "fail", "error")}
    (results_dir / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print_table(report)
    log(f"report: {results_dir / 'report.json'}")
    return exit_code


def print_table(report: dict) -> None:
    print(f"\n{'profile':<15} {'result':<6} {'seconds':>7}  notes")
    for p in report["profiles"]:
        notes = [
            c["name"] for c in p.get("checks", []) if c["status"] == "fail"
        ] or p.get("error", "")
        skipped = [c["name"] for c in p.get("checks", []) if c["status"] == "skip"]
        text = ", ".join(notes) if isinstance(notes, list) else notes
        if skipped:
            text = (text + "; " if text else "") + f"skipped: {', '.join(skipped)}"
        print(f"{p['name']:<15} {p['status']:<6} {p['duration_s']:>7}  {text}")
    print()


def cmd_list(_: argparse.Namespace) -> int:
    for p in PROFILES.values():
        flag = " [slow]" if p.slow else ""
        print(f"{p.name:<15} dial {p.number:<4}{flag}  {p.imitates}")
    return 0


def cmd_up(args: argparse.Namespace) -> int:
    """Start (or keep) the bench PBX for an external device and print its settings."""
    ensure_images(Compose(Network(args.subnet)))
    compose, _ = setup_pbx(args, fresh=False)
    print_device_hints(compose, args)
    return 0


def cmd_standin(args: argparse.Namespace) -> int:
    """Run the stand-in DUT on its own, as an 'external' device (self-test)."""
    network = Network(args.subnet)
    out = RUN_DIR / "standin"
    if args.action == "down":
        Compose(network).run("stop", "-t", "15", "dut", out_dir=out, check=False)
        return 0
    ensure_images(Compose(network))
    assets.fetch(MODELS_DIR, log=log)
    render_audio.render(Compose(network), log=log)
    compose, _ = setup_pbx(args, fresh=False)
    out.mkdir(parents=True, exist_ok=True)
    secrets = load_secrets()
    account = {
        "id": f"sip:2000@{network.pbx}",
        "registrar": f"sip:{network.pbx}",
        "username": "2000",
        "password": secrets["AI_PASSWORD"],
    }
    scenario = standin_scenario(network, account)
    scenario["out_dir"] = "/bench/out"
    scenario["record"] = False
    (out / "dut.json").write_text(json.dumps(scenario, indent=2), encoding="utf-8")
    compose.run("up", "-d", "--force-recreate", "--no-deps", "dut", out_dir=out)
    wait_for(lambda: registered(out, "dut"), 20, "the stand-in DUT to register")
    log(f"stand-in DUT registered as 2000 from {network.dut}:5060 (events in {out})")
    return 0


def cmd_addon(args: argparse.Namespace) -> int:
    """SRW's addon alone on the bench network, for a gateway that runs
    elsewhere (A1: on the host, with SRW's live job on k3d behind it)."""
    network = Network(args.subnet)
    compose = Compose(network, srw=True)
    if args.action == "down":
        srw.down(compose)
        return 0
    ensure_images(Compose(network))
    args.dut_peer = args.dut_peer or f"{network.addon}:5060"
    _, _ = setup_pbx(args, fresh=False)
    source = srw.prepare_addon(network, args.line, args.extension, args.media_host)
    srw.addon_up(compose, args.extension, timeout=args.wait_registration or 60)
    log(
        f"addon registered line {args.line} as {args.extension} at {network.pbx}; "
        f"human extension {HUMAN_EXTENSION}"
    )
    log("the gateway's source settings (also in .run/srw/gateway/source.yaml):")
    print(yaml.safe_dump({"source": source}, sort_keys=False), end="", flush=True)
    return 0


def cmd_call(args: argparse.Namespace) -> int:
    """One scripted call (call.py) through the running PBX."""
    from tests.voice_bench import call

    network = Network(args.subnet)
    compose = Compose(network, srw=True)
    try:
        script = call.load_script(Path(args.script))
    except (OSError, call.ScriptError) as exc:
        log(f"bad call script: {exc}")
        return 2
    if args.analyze:
        # Only the analysis, again, on an earlier call's results directory.
        out = Path(args.analyze).resolve()
        report = call.analyze(compose, script, out, args.threads, log=log)
    else:
        if not pbx_running(compose):
            log("the bench PBX is not running: start it with `addon up` (or `up`)")
            return 2
        assets.fetch(MODELS_DIR, log=log)
        render_audio.render(compose, log=log)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        out = (
            Path(args.results).resolve()
            if args.results
            else RESULTS_DIR / f"call-{stamp}"
        )
        report = call.run(compose, script, out, args.threads, log=log)
    addon = compose.run("ps", "-q", "addon", check=False).stdout.strip()
    if addon and not args.analyze:
        # A5: the addon wrote no audio during the call.
        report["no_audio_written"] = srw.audio_check(compose, ("addon",))
        if not report["no_audio_written"]["ok"]:
            report["status"] = "fail"
        (out / "call.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    for entry in report["timeline"]:
        print(f"  {entry['from_s']:7.1f}  {entry['text']}")
    for check in report["checks"]:
        print(f"  {check['status']:<5} {check['name']}: {check['detail']}")
    log(f"call {report['status'].upper()}; report: {out / 'call.json'}")
    return 0 if report["status"] == "pass" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="voice-bench", description=__doc__.split("\n")[0]
    )
    parser.add_argument(
        "--subnet", default=os.environ.get("VOICE_BENCH_SUBNET", "172.30.86.0/24")
    )
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="run profiles and write a JSON report")
    run.add_argument("--profile", help="comma-separated profile names (default: all)")
    run.add_argument(
        "--skip-slow",
        action="store_true",
        help="leave out slow profiles (session-timer)",
    )
    run.add_argument(
        "--dut",
        choices=("standin", "external", "srw"),
        default="standin",
        help="standin (pjsua), external (a device you run) or srw (SRW's own "
        "Asterisk addon, voice gateway and echo engine; WP2)",
    )
    run.add_argument(
        "--dut-expect",
        default=None,
        help="what the caller must hear from the DUT: standin (default with the stand-in), "
        "echo (default for external and srw: the caller's own words) or comma-separated keywords",
    )

    def device_options(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--dut-peer", help="device address for the peer profile, HOST[:PORT]"
        )
        p.add_argument(
            "--external-address",
            help="publish the PBX on the host (compose.external.yaml) and advertise this address",
        )
        p.add_argument(
            "--sip-port",
            type=int,
            default=5060,
            help="host SIP port with --external-address",
        )

    device_options(run)
    run.add_argument(
        "--wait-registration",
        type=float,
        default=0,
        help="seconds to wait for 2000 to register",
    )
    run.add_argument(
        "--results",
        help="results directory (default: tests/voice_bench/results/<UTC time>)",
    )
    run.add_argument("--threads", type=int, default=8, help="whisper.cpp threads")
    run.add_argument(
        "--build",
        action="store_true",
        help="rebuild the images without the layer cache",
    )
    run.add_argument(
        "--keep-up",
        action="store_true",
        help="leave the PBX (and SRW's stack) running afterwards (always with --dut external)",
    )
    up = sub.add_parser("up", help="start the bench PBX for an external device")
    device_options(up)

    sub.add_parser("list", help="list the profiles")
    sub.add_parser("models", help="download the pinned models")
    audio = sub.add_parser(
        "audio", help="render the bench WAVs and the German test set"
    )
    audio.add_argument("--force", action="store_true")
    sub.add_parser("build", help="build all bench images")
    standin = sub.add_parser(
        "standin", help="run the stand-in DUT alone (external-mode self-test)"
    )
    standin.add_argument("action", choices=("up", "down"))
    device_options(standin)
    sub.add_parser("down", help="stop and remove the bench containers")
    addon = sub.add_parser(
        "addon",
        help="SRW's Asterisk addon alone, for a gateway outside the bench (A1)",
    )
    addon.add_argument("action", choices=("up", "down"))
    addon.add_argument("--line", default="bench", help="the gateway's line name")
    addon.add_argument("--extension", default="2000")
    addon.add_argument(
        "--media-host",
        default="",
        help="where the addon sends call audio (default: this host's bench address)",
    )
    addon.add_argument("--wait-registration", type=float, default=60)
    device_options(addon)
    call_parser = sub.add_parser(
        "call", help="one scripted call through the running PBX (call.py)"
    )
    call_parser.add_argument("--script", required=True, help="call script (YAML)")
    call_parser.add_argument(
        "--results", help="results directory (default: results/call-<UTC time>)"
    )
    call_parser.add_argument("--threads", type=int, default=8)
    call_parser.add_argument(
        "--analyze",
        metavar="DIR",
        help="only analyze an earlier call's results directory again",
    )

    args = parser.parse_args(argv)
    if args.command == "run" and args.dut_expect is None:
        args.dut_expect = "standin" if args.dut == "standin" else "echo"
    compose = Compose(Network(args.subnet))
    if args.command == "run":
        return cmd_run(args)
    if args.command == "list":
        return cmd_list(args)
    if args.command == "models":
        assets.fetch(MODELS_DIR, log=log)
        return 0
    if args.command == "audio":
        assets.fetch(MODELS_DIR, log=log)
        ensure_images(compose)
        log(
            f"rendered {render_audio.render(compose, force=args.force, log=log)} WAV set(s)"
        )
        return 0
    if args.command == "build":
        ensure_images(compose)
        return 0
    if args.command == "standin":
        return cmd_standin(args)
    if args.command == "up":
        return cmd_up(args)
    if args.command == "addon":
        return cmd_addon(args)
    if args.command == "call":
        return cmd_call(args)
    if args.command == "down":
        compose.run(
            "--profile", "tools", "down", "--remove-orphans", check=False, capture=False
        )
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
