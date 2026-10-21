"""SRW's own stack as the bench's device under test (WP2, ``run --dut srw``).

The Asterisk addon (srw-asterisk with the config ``voice_gateway.asterisk.
render`` writes from bench line values), the voice gateway with the Asterisk
source and the echo engine, and ``srw_driver.py`` in SRW's driver role, all
on the addon's bench address. Two lines: ``bench`` registers as extension
2000 at the bench PBX, ``bench-peer`` takes the peer profile's call. Every
secret is a fresh throwaway value under the gitignored run directory.
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml

from tests.voice_bench.settings import (
    REPO_ROOT,
    SRW_DIR,
    Compose,
    Network,
    load_secrets,
)

SERVICES = ("addon", "gateway", "driver")
APP = "srw-voice"
#: line name -> connector id the gateway posts its webhook for
CONNECTORS = {"bench": "bench-register", "bench-peer": "bench-peer"}
DRIVER_LOG = SRW_DIR / "driver" / "driver.jsonl"
AUDIO_SUFFIXES = (
    ".wav",
    ".gsm",
    ".sln",
    ".sln16",
    ".ulaw",
    ".alaw",
    ".g722",
    ".raw",
    ".pcm",
    ".mp3",
    ".ogg",
    ".au",
)


def _write(path: Path, text: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), mode)
        handle.write(text)


def prepare(network: Network, transfer_target: str) -> None:
    """Write the addon values, render /etc/asterisk, and configure the
    gateway and the driver."""
    if SRW_DIR.exists():
        shutil.rmtree(SRW_DIR)
    for part in ("addon", "gateway", "driver"):
        (SRW_DIR / part).mkdir(parents=True, mode=0o700)
    password = secrets.token_hex(16)
    api_key = secrets.token_hex(16)
    _write(SRW_DIR / "addon" / "asterisk-password", password)
    _write(SRW_DIR / "addon" / "line-bench-password", load_secrets()["AI_PASSWORD"])
    _write(SRW_DIR / "gateway" / "asterisk-password", password)
    for directory in ("gateway", "driver"):
        _write(SRW_DIR / directory / "api-key", api_key)
    for line, connector in CONNECTORS.items():
        whsec = "whsec_" + base64.b64encode(secrets.token_bytes(24)).decode()
        _write(SRW_DIR / "gateway" / f"webhook-{line}", whsec)
        _write(SRW_DIR / "driver" / f"webhook-{connector}", whsec)

    values = {
        "sip": {"port": 5060},
        "rtp": {"start": 10000, "end": 10099},
        "ari": {
            "bind": "127.0.0.1:8088",
            "user": "srw",
            "password_file": "asterisk-password",
            "app": APP,
        },
        "gateway": {"media_uri": "ws://127.0.0.1:18441/media"},
        "lines": [
            {
                "name": "bench",
                "mode": "register",
                "pbx": network.pbx,
                "extension": "2000",
                "password_file": "line-bench-password",
            },
            {"name": "bench-peer", "mode": "peer", "pbx": network.pbx},
        ],
    }
    _write(SRW_DIR / "addon" / "values.yaml", yaml.safe_dump(values, sort_keys=False))
    # The same renderer the Helm chart's init container will run (WP5).
    subprocess.run(
        [
            sys.executable,
            "-m",
            "voice_gateway.asterisk.render",
            "--values",
            str(SRW_DIR / "addon" / "values.yaml"),
            "--out",
            str(SRW_DIR / "addon" / "etc"),
        ],
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
        check=True,
        capture_output=True,
        text=True,
    )

    gateway = {
        "listen": "127.0.0.1:18440",
        "api_key_files": ["api-key"],
        "orchestrator_url": "http://127.0.0.1:18480",
        "lines": [
            {
                "name": line,
                "connector_id": connector,
                "webhook_secret_file": f"webhook-{line}",
            }
            for line, connector in CONNECTORS.items()
        ],
        "profiles": {"echo": {"engine": "echo"}},
        "source": {
            "kind": "asterisk",
            "ari_url": "http://127.0.0.1:8088",
            "ari_user": "srw",
            "password_file": "asterisk-password",
            "app": APP,
            "media_listen": "127.0.0.1:18441",
        },
    }
    _write(
        SRW_DIR / "gateway" / "gateway.yaml", yaml.safe_dump(gateway, sort_keys=False)
    )
    driver = {
        "listen": "127.0.0.1:18480",
        "gateway": "http://127.0.0.1:18440",
        "api_key_file": "/bench/driver/api-key",
        "connectors": {
            connector: f"/bench/driver/webhook-{connector}"
            for connector in CONNECTORS.values()
        },
        "model": "echo",
        "transfer_digit": "9",
        "transfer_target": transfer_target,
        "events": "/bench/driver/driver.jsonl",
    }
    _write(SRW_DIR / "driver" / "driver.json", json.dumps(driver, indent=2))
    _write(DRIVER_LOG, "")


def up(compose: Compose, timeout: float = 60.0) -> None:
    """Start addon, gateway and driver; wait for ARI and the registration."""
    compose.run("build", "gateway", capture=False, timeout=1800)
    compose.run("up", "-d", "--force-recreate", "--no-deps", *SERVICES, timeout=180)
    deadline = time.time() + timeout
    while time.time() < deadline:
        apps = compose.asterisk("ari show apps", service="addon")
        contacts = compose.asterisk("pjsip show contacts")
        if APP in apps and "2000/" in contacts:
            return
        time.sleep(0.5)
    raise TimeoutError(
        "the gateway did not register its ARI app, or 2000 did not register"
    )


def logs(compose: Compose, out: Path) -> None:
    for service in SERVICES:
        result = compose.run("logs", "--no-color", service, check=False, timeout=60)
        (out / f"srw-{service}.log").write_text(
            result.stdout + result.stderr, encoding="utf-8"
        )


def down(compose: Compose) -> None:
    compose.run("stop", "-t", "10", *SERVICES, check=False, timeout=120)
    compose.run("rm", "-f", *SERVICES, check=False, timeout=60)


def driver_offset() -> int:
    return DRIVER_LOG.stat().st_size if DRIVER_LOG.exists() else 0


def driver_events(offset: int, settle_s: float = 3.0) -> list[dict]:
    """The driver's log since ``offset``; waits until every call it saw closed."""
    deadline = time.time() + settle_s
    while True:
        with DRIVER_LOG.open("rb") as handle:
            handle.seek(offset)
            events = [
                json.loads(line)
                for line in handle.read().decode("utf-8").splitlines()
                if line.strip()
            ]
        sessions = {e["session_id"] for e in events if e["kind"] == "attached"}
        detached = {e["session_id"] for e in events if e["kind"] == "detached"}
        if sessions <= detached or time.time() > deadline:
            return events
        time.sleep(0.2)


#: What the addon may change in its container: Asterisk's key-value store
#: and its control socket. Everything else would be a finding.
ADDON_WRITES = (
    "/var/lib/asterisk/astdb.sqlite3",
    "/run/asterisk/asterisk.ctl",
    "/run/asterisk/asterisk.pid",
    "/var/run/asterisk/asterisk.ctl",
    "/var/run/asterisk/asterisk.pid",
)


def audio_check(compose: Compose) -> dict:
    """A5 for the bench: every file the addon, the gateway and the driver
    added to their containers (``docker diff``). The gateway and the driver
    run read-only; the addon may only keep its astdb and control socket. The
    run directory (the driver's log, configs, secrets) holds no audio."""
    report: dict = {"changed": {}, "unexpected": []}
    for service in SERVICES:
        container = compose.run("ps", "-q", service, check=False).stdout.strip()
        if not container:
            report["changed"][service] = None
            report["unexpected"].append(f"{service}: not running")
            continue
        entries = subprocess.run(
            ["docker", "diff", container], capture_output=True, text=True, check=False
        ).stdout.splitlines()
        report["changed"][service] = entries
        paths = [entry.split(" ", 1)[-1] for entry in entries]
        for entry, path in zip(entries, paths):
            # Parents of other entries are directories; mount points come
            # from compose (/app/src, /bench/...).
            is_dir = any(other.startswith(path + "/") for other in paths)
            if is_dir or path.startswith(("/app", "/bench")):
                continue
            allowed = service == "addon" and path.startswith(ADDON_WRITES)
            if not allowed or path.lower().endswith(AUDIO_SUFFIXES):
                report["unexpected"].append(f"{service}: {entry}")
    files = sorted(
        str(p.relative_to(SRW_DIR)) for p in SRW_DIR.rglob("*") if p.is_file()
    )
    report["run_dir_files"] = files
    report["unexpected"] += [p for p in files if p.lower().endswith(AUDIO_SUFFIXES)]
    report["ok"] = not report["unexpected"]
    return report
