"""Paths, addresses, secrets and the docker compose wrapper for the WP0 bench."""

from __future__ import annotations

import ipaddress
import os
import secrets
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
CACHE_DIR = BENCH_DIR / ".cache"
MODELS_DIR = CACHE_DIR / "models"
AUDIO_DIR = CACHE_DIR / "audio"
RUN_DIR = BENCH_DIR / ".run"
RESULTS_DIR = BENCH_DIR / "results"
ENV_FILE = BENCH_DIR / ".env"
TESTSET_DIR = BENCH_DIR / "testset" / "de"

SECRET_KEYS = ("AI_PASSWORD", "CALLER_PASSWORD", "HUMAN_PASSWORD")


def load_secrets() -> dict[str, str]:
    """Read the gitignored env file, creating throwaway passwords on first use."""
    values: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    missing = [key for key in SECRET_KEYS if not values.get(f"VOICE_BENCH_{key}")]
    if missing:
        for key in missing:
            values[f"VOICE_BENCH_{key}"] = secrets.token_hex(12)
        lines = ["# Throwaway credentials for the WP0 voice bench (gitignored)."]
        lines += [f"{key}={value}" for key, value in sorted(values.items())]
        ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
        ENV_FILE.chmod(0o600)
    return {key: values[f"VOICE_BENCH_{key}"] for key in SECRET_KEYS}


@dataclass
class Network:
    """Addresses on the bench's docker network (one /24 by default)."""

    subnet: str = "172.30.86.0/24"

    def host(self, index: int) -> str:
        net = ipaddress.ip_network(self.subnet)
        return str(net.network_address + index)

    @property
    def pbx(self) -> str:
        return self.host(10)

    @property
    def dut(self) -> str:
        return self.host(20)

    @property
    def human(self) -> str:
        return self.host(30)

    @property
    def caller(self) -> str:
        return self.host(40)


@dataclass
class ExternalDut:
    """Where the PBX finds a real device instead of the stand-in (WP2)."""

    advertise: str  # address the PBX advertises in Contact/SDP to the device
    sip_port: int = 5060  # host port published for the PBX's SIP


@dataclass
class Compose:
    network: Network = field(default_factory=Network)
    external: ExternalDut | None = None

    def env(
        self, out_dir: Path | None = None, work_dir: Path | None = None
    ) -> dict[str, str]:
        env = dict(os.environ)
        env.update(
            {
                "VOICE_BENCH_UID": str(os.getuid()),
                "VOICE_BENCH_GID": str(os.getgid()),
                "VOICE_BENCH_RUN": str(RUN_DIR),
                "VOICE_BENCH_OUT": str(out_dir or RUN_DIR / "current"),
                "VOICE_BENCH_WORK": str(work_dir or RUN_DIR),
                "VOICE_BENCH_AUDIO": str(AUDIO_DIR / "bench"),
                "VOICE_BENCH_AUDIO_WORK": str(AUDIO_DIR),
                "VOICE_BENCH_MODELS": str(MODELS_DIR),
                "VOICE_BENCH_SUBNET": self.network.subnet,
                "VOICE_BENCH_PBX_IP": self.network.pbx,
                "VOICE_BENCH_DUT_IP": self.network.dut,
                "VOICE_BENCH_HUMAN_IP": self.network.human,
                "VOICE_BENCH_CALLER_IP": self.network.caller,
            }
        )
        if self.external:
            env["VOICE_BENCH_SIP_PORT"] = str(self.external.sip_port)
        return env

    def files(self) -> list[str]:
        args = ["-f", str(BENCH_DIR / "compose.yaml")]
        if self.external:
            args += ["-f", str(BENCH_DIR / "compose.external.yaml")]
        return args

    def run(
        self,
        *args: str,
        out_dir: Path | None = None,
        work_dir: Path | None = None,
        check: bool = True,
        capture: bool = True,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess:
        cmd = ["docker", "compose", *self.files(), *args]
        return subprocess.run(
            cmd,
            cwd=BENCH_DIR,
            env=self.env(out_dir, work_dir),
            check=check,
            capture_output=capture,
            text=True,
            timeout=timeout,
        )

    def asterisk(self, command: str) -> str:
        result = self.run("exec", "-T", "pbx", "asterisk", "-rx", command, check=False)
        return result.stdout
