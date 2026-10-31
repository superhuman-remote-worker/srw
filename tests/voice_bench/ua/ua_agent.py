#!/usr/bin/env python3
"""Drive one pjsua instance through a scripted phone call (WP0 voice bench).

Runs inside the ``srw-voice-bench-ua`` container (stdlib only). The scenario is
a JSON file (path in argv[1] or ``$UA_SCENARIO``)::

    {
      "role": "caller" | "dut" | "human",
      "out_dir": "/bench/out",
      "local_port": 5060, "rtp_port": 4000, "public_address": null,
      "account": {"id": "sip:2000@pbx", "registrar": "sip:pbx" | null,
                  "username": "2000", "password": "...", "realm": "*"},
      "codecs": ["PCMA", "PCMU", "G722"],          # preference order
      "session_timer": {"use": 1, "se": 1800, "min_se": 90},
      "play": "/bench/audio/dut.16k.wav", "record": true,
      "auto_answer": 200,                           # dut / human
      "call": "sip:100@pbx",                        # caller only
      "steps": [{"at": 4.0, "do": "dtmf", "method": "rfc2833", "digits": "1"},
                {"at": 12.0, "do": "hangup"}],      # seconds after CONFIRMED
      "on_dtmf": {"9": {"do": "transfer", "uri": "sip:3000@pbx"}},
      "max_seconds": 60, "exit_after_call": true
    }

Outputs in ``out_dir`` (``<role>`` prefix): ``.events.jsonl`` (every SIP
message with headers and body, call states, received DTMF with method,
transfer results, registration results, commands sent), ``.console.log``
(pjsua stdout), ``.pjsua.log`` (pjsua level-5 log), ``.wav`` (what the far end
sent, 16 kHz mono) and ``.summary.json``.
"""

from __future__ import annotations

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import wave

PJSUA = "/usr/local/bin/pjsua"
KNOWN_CODECS = ("PCMA", "PCMU", "G722")

SIP_START = re.compile(
    r"\b(?P<dir>RX|TX) (?P<size>\d+) bytes (?P<kind>Request|Response) msg "
    r"(?P<info>\S+).*? (?:from|to) (?P<transport>\S+) (?P<peer>[^\s]+):$"
)
CALL_STATE = re.compile(r"Call (?P<call>\d+) state changed to (?P<state>[A-Z]+)")
CALL_DISCONNECTED = re.compile(
    r"Call (?P<call>\d+) is DISCONNECTED \[reason=(?P<code>\d+) \((?P<reason>.*)\)\]"
)
DTMF = re.compile(
    r"Incoming DTMF on call (?P<call>\d+): (?P<digit>.)(?::duration\((?P<duration>\d+)\))?, "
    r"using (?P<method>.+) method"
)
TRANSFER = re.compile(
    r"Call (?P<call>\d+): transfer status=(?P<code>\d+) \((?P<text>.*?)\) ?(?P<final>\[final\])?"
)
REG_OK = re.compile(r"registration success, status=(?P<code>\d+)")
REG_FAIL = re.compile(r"registration failed, status=(?P<code>\d+)")
UNREG_OK = re.compile(r"unregistration success")


def parse_sip(text: str) -> dict:
    """Split one logged SIP message into start line, headers and body."""
    head, _, body = text.replace("\r\n", "\n").partition("\n\n")
    lines = head.split("\n")
    headers: list[list[str]] = []
    for line in lines[1:]:
        if line[:1] in (" ", "\t") and headers:
            headers[-1][1] += " " + line.strip()
        elif ":" in line:
            name, _, value = line.partition(":")
            headers.append([name.strip(), value.strip()])
    return {"start": lines[0].strip(), "headers": headers, "body": body.strip("\n")}


class Agent:
    def __init__(self, scenario: dict) -> None:
        self.sc = scenario
        self.role = scenario["role"]
        self.out = scenario.get("out_dir", "/bench/out")
        os.makedirs(self.out, exist_ok=True)
        self.prefix = os.path.join(self.out, self.role)
        self.events = queue.Queue()
        self.lock = threading.Lock()
        self.events_file = open(self.prefix + ".events.jsonl", "w", encoding="utf-8")
        self.console = open(self.prefix + ".console.log", "w", encoding="utf-8")
        self.proc: subprocess.Popen | None = None
        self.t_start = time.time()
        self.t_confirmed: float | None = None
        self.t_disconnected: float | None = None
        self.disconnect: dict | None = None
        self.stop_requested = False
        self.fired_dtmf: set[str] = set()

    # -- output ---------------------------------------------------------------

    def emit(self, event_type: str, /, **data) -> dict:
        event = {"t": round(time.time(), 3), "type": event_type, **data}
        with self.lock:
            self.events_file.write(json.dumps(event, ensure_ascii=False) + "\n")
            self.events_file.flush()
        self.events.put(event)
        return event

    # -- pjsua ----------------------------------------------------------------

    def argv(self) -> list[str]:
        sc = self.sc
        acc = sc.get("account") or {}
        # stdbuf: pjsua block-buffers stdout into a pipe, which would delay
        # every event (and a DTMF-triggered transfer) by seconds.
        args = [
            "stdbuf",
            "-oL",
            "-eL",
            PJSUA,
            "--null-audio",
            "--clock-rate=16000",
            "--snd-clock-rate=16000",
            "--no-vad",
            "--ec-tail=0",
            "--no-color",
            "--max-calls=4",
            f"--local-port={sc.get('local_port', 5060)}",
            f"--rtp-port={sc.get('rtp_port', 4000)}",
            f"--log-file={self.prefix}.pjsua.log",
            "--log-level=5",
            "--app-log-level=4",
        ]
        if sc.get("public_address"):
            args.append(f"--ip-addr={sc['public_address']}")
        if acc.get("id"):
            args.append(f"--id={acc['id']}")
        if acc.get("registrar"):
            args += [f"--registrar={acc['registrar']}", "--reg-timeout=300"]
        if acc.get("username"):
            args += [
                f"--realm={acc.get('realm', '*')}",
                f"--username={acc['username']}",
                f"--password={acc.get('password', '')}",
            ]
        codecs = sc.get("codecs") or list(KNOWN_CODECS)
        args += [f"--dis-codec={c}" for c in KNOWN_CODECS if c not in codecs]
        # pjsua gives later --add-codec arguments the higher priority.
        args += [f"--add-codec={c}" for c in reversed(codecs)]
        timer = sc.get("session_timer") or {}
        args += [
            f"--use-timer={timer.get('use', 1)}",
            f"--timer-se={timer.get('se', 1800)}",
            f"--timer-min-se={timer.get('min_se', 90)}",
        ]
        if sc.get("play"):
            args += [f"--play-file={sc['play']}", "--auto-play"]
        if sc.get("record", True):
            args += [f"--rec-file={self.prefix}.wav", "--auto-rec"]
        if sc.get("auto_answer"):
            args.append(f"--auto-answer={int(sc['auto_answer'])}")
        if sc.get("call"):
            args.append(sc["call"])
        return args

    def send(self, *lines: str) -> None:
        if not self.proc or self.proc.poll() is not None or not self.proc.stdin:
            return
        try:
            for line in lines:
                self.proc.stdin.write(line + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass

    def command(self, step: dict) -> None:
        action = step.get("do")
        redacted = {k: v for k, v in step.items() if k != "password"}
        self.emit("command", **redacted)
        if action == "dtmf":
            key = "*" if step.get("method") == "info" else "#"
            self.send(key, str(step["digits"]))
        elif action == "transfer":
            self.send("x", step["uri"])
        elif action == "hangup":
            self.send("h")
        elif action == "hold":
            self.send("H")
        elif action == "unhold":
            self.send("v")
        elif action == "update":
            self.send("U")
        elif action == "quit":
            self.send("q")

    def reader(self) -> None:
        assert self.proc and self.proc.stdout
        pending: dict | None = None
        body: list[str] = []
        for raw in self.proc.stdout:
            line = raw.rstrip("\n")
            self.console.write(line + "\n")
            if pending is not None:
                if line.strip() == "--end msg--":
                    msg = parse_sip("\n".join(body))
                    self.emit("sip", **pending, **msg)
                    pending, body = None, []
                else:
                    body.append(line)
                continue
            m = SIP_START.search(line)
            if m:
                pending = {
                    "dir": m["dir"].lower(),
                    "kind": m["kind"].lower(),
                    "info": m["info"],
                    "transport": m["transport"],
                    "peer": m["peer"],
                }
                body = []
                continue
            if m := CALL_DISCONNECTED.search(line):
                self.emit(
                    "call_state",
                    call=int(m["call"]),
                    state="DISCONNECTED",
                    code=int(m["code"]),
                    reason=m["reason"],
                )
            elif m := CALL_STATE.search(line):
                if m["state"] != "DISCONNECTED":
                    self.emit("call_state", call=int(m["call"]), state=m["state"])
            elif m := DTMF.search(line):
                self.emit(
                    "dtmf",
                    call=int(m["call"]),
                    digit=m["digit"],
                    method="info" if "INFO" in m["method"] else "rfc4733",
                    duration=int(m["duration"]) if m["duration"] else None,
                )
            elif m := TRANSFER.search(line):
                self.emit(
                    "transfer_status",
                    call=int(m["call"]),
                    code=int(m["code"]),
                    text=m["text"],
                    final=bool(m["final"]),
                )
            elif m := REG_OK.search(line):
                self.emit("registration", ok=True, code=int(m["code"]))
            elif m := REG_FAIL.search(line):
                self.emit("registration", ok=False, code=int(m["code"]))
            elif UNREG_OK.search(line):
                self.emit("unregistration", ok=True)
        self.console.flush()
        self.events.put({"type": "eof"})

    # -- main loop ------------------------------------------------------------

    def run(self) -> int:
        sc = self.sc
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        argv = self.argv()
        shown = [
            a if not a.startswith("--password=") else "--password=***" for a in argv
        ]
        self.emit("start", role=self.role, argv=shown)
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        threading.Thread(target=self.reader, daemon=True).start()
        steps = sorted(sc.get("steps") or [], key=lambda s: float(s["at"]))
        on_dtmf = sc.get("on_dtmf") or {}
        deadline = self.t_start + float(sc.get("max_seconds", 120))
        exit_after_call = bool(sc.get("exit_after_call", False))
        quit_at: float | None = None

        while True:
            now = time.time()
            if self.stop_requested and quit_at is None:
                quit_at = now
            if now >= deadline and quit_at is None:
                self.emit("watchdog", seconds=sc.get("max_seconds", 120))
                self.command({"do": "hangup"})
                quit_at = now + 1.0
            if self.t_confirmed is not None:
                while steps and now >= self.t_confirmed + float(steps[0]["at"]):
                    self.command(steps.pop(0))
            if quit_at is not None and now >= quit_at:
                break
            try:
                event = self.events.get(timeout=0.05)
            except queue.Empty:
                continue
            kind = event.get("type")
            if kind == "eof":
                break
            if kind == "call_state":
                if event["state"] == "CONFIRMED":
                    # A long-running stand-in takes many calls; re-arm the
                    # DTMF triggers for each one.
                    self.fired_dtmf.clear()
                    if self.t_confirmed is None:
                        self.t_confirmed = event["t"]
                elif event["state"] == "DISCONNECTED":
                    self.t_disconnected = event["t"]
                    self.disconnect = event
                    if exit_after_call and quit_at is None:
                        quit_at = time.time() + 1.0
            elif kind == "dtmf":
                action = on_dtmf.get(event["digit"])
                if action and event["digit"] not in self.fired_dtmf:
                    self.fired_dtmf.add(event["digit"])
                    self.command(action)

        return self.shutdown()

    def _on_signal(self, *_: object) -> None:
        self.stop_requested = True

    def shutdown(self) -> int:
        t_quit = time.time()
        if self.proc and self.proc.poll() is None:
            self.command({"do": "quit"})
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        # The recorder closes when pjsua exits, so its last sample is "now".
        t_exit = time.time()
        rec = self.prefix + ".wav"
        rec_seconds = None
        if os.path.exists(rec):
            try:
                with wave.open(rec) as w:
                    rec_seconds = round(w.getnframes() / float(w.getframerate()), 3)
            except (wave.Error, EOFError):
                rec_seconds = None
        summary = {
            "role": self.role,
            "t_start": round(self.t_start, 3),
            "t_confirmed": self.t_confirmed,
            "t_disconnected": self.t_disconnected,
            "t_quit": round(t_quit, 3),
            "t_exit": round(t_exit, 3),
            "disconnect": self.disconnect,
            "recording": os.path.basename(rec) if rec_seconds is not None else None,
            "recording_seconds": rec_seconds,
            "pjsua_exit": self.proc.returncode if self.proc else None,
        }
        with open(self.prefix + ".summary.json", "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        self.emit("exit", **{k: v for k, v in summary.items() if k != "disconnect"})
        self.events_file.close()
        self.console.close()
        if self.sc.get("call"):
            return 0 if self.t_confirmed is not None else 2
        return 0


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("UA_SCENARIO", "")
    if not path:
        print("usage: ua_agent.py SCENARIO.json (or set UA_SCENARIO)", file=sys.stderr)
        return 64
    with open(path, encoding="utf-8") as fh:
        scenario = json.load(fh)
    return Agent(scenario).run()


if __name__ == "__main__":
    sys.exit(main())
