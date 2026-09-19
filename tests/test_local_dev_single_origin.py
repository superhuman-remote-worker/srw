from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def _write_executable(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env bash\nset -eu\n" + body)
    path.chmod(0o755)


def _fake_path(tmp_path: Path, *, include_mkcert: bool = False) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True)
    for name in ("bash", "dirname", "mktemp", "rm", "tr", "head", "cat", "date", "sed", "grep"):
        target = Path("/usr/bin") / name
        if not target.exists():
            target = Path("/bin") / name
        (bin_dir / name).symlink_to(target)

    _write_executable(
        bin_dir / "k3d",
        """
if [[ "$1 $2" == "cluster list" ]]; then
  [[ "${FAKE_CLUSTER_EXISTS:-0}" == 1 ]]
  exit
fi
printf 'k3d %s\n' "$*"
""",
    )
    _write_executable(
        bin_dir / "docker",
        """
if [[ "${1:-}" == port ]]; then
  case "${3:-}" in
    80/tcp) printf '%s\n' "${FAKE_DOCKER_PORT_80:-}" ;;
    443/tcp) printf '%s\n' "${FAKE_DOCKER_PORT_443:-}" ;;
    *) printf '%s\n' "${FAKE_DOCKER_PORT:-}" ;;
  esac
fi
""",
    )
    _write_executable(bin_dir / "kubectl", "exit 0\n")
    _write_executable(
        bin_dir / "helm",
        """
if [[ "${1:-}" == dependency && "${2:-}" == list ]]; then
  printf 'STATUS\tok\n'
fi
if [[ "${1:-} ${2:-}" == "upgrade --help" ]]; then
  printf '%s\n' '  --take-ownership'
fi
""",
    )
    for name in ("openssl", "ssh-keygen", "git", "curl"):
        _write_executable(bin_dir / name, "exit 0\n")
    if include_mkcert:
        _write_executable(bin_dir / "mkcert", "exit 0\n")
    _write_executable(bin_dir / "tilt", "printf 'tilt %s\\n' \"$*\"\n")
    return bin_dir


def _run_bootstrap(tmp_path: Path, **env_overrides: str) -> subprocess.CompletedProcess[str]:
    fake_path = _fake_path(tmp_path, include_mkcert=env_overrides.pop("include_mkcert", "0") == "1")
    env = {
        **os.environ,
        "PATH": str(fake_path),
        "SRW_IMAGE_PIN": "0",
        "MKCERT_CAROOT": str(tmp_path / "missing-ca"),
        **env_overrides,
    }
    return subprocess.run(
        [str(REPO / "scripts/local-dev-up.sh")],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_single_origin_skips_mkcert_and_creates_dedicated_nodeport_mapping(tmp_path: Path) -> None:
    result = _run_bootstrap(tmp_path, SRW_EXPOSURE_MODE="single-origin")

    assert result.returncode == 0, result.stderr
    assert '--port 127.0.0.1:8443:30443@server:0' in result.stdout
    assert "cert-manager" not in result.stdout
    assert "mkcert" not in result.stdout
    assert "https://localhost:8443/" in result.stdout
    assert "deployment/values-local-single-origin.yaml" in result.stdout


def test_single_origin_rejects_an_existing_cluster_with_the_wrong_mapping(tmp_path: Path) -> None:
    result = _run_bootstrap(
        tmp_path,
        SRW_EXPOSURE_MODE="single-origin",
        FAKE_CLUSTER_EXISTS="1",
        FAKE_DOCKER_PORT="0.0.0.0:8443",
    )

    assert result.returncode != 0
    assert "127.0.0.1:8443" in result.stderr
    assert "cannot be changed in place" in result.stderr.lower()
    assert "back up" in result.stderr.lower()
    assert "delete" in result.stderr.lower()


def test_multi_host_rejects_an_existing_single_origin_cluster(tmp_path: Path) -> None:
    ca_root = tmp_path / "missing-ca"
    ca_root.mkdir()
    (ca_root / "rootCA.pem").write_text("fixture certificate")
    (ca_root / "rootCA-key.pem").write_text("fixture key")

    result = _run_bootstrap(
        tmp_path,
        SRW_EXPOSURE_MODE="multi-host",
        FAKE_CLUSTER_EXISTS="1",
        FAKE_DOCKER_PORT="127.0.0.1:8443",
        include_mkcert="1",
    )

    assert result.returncode != 0
    assert "ports 80 and 443" in result.stderr
    assert "cannot be changed in place" in result.stderr.lower()
    assert "back up" in result.stderr.lower()
    assert "delete" in result.stderr.lower()


def test_legacy_mode_still_requires_mkcert(tmp_path: Path) -> None:
    result = _run_bootstrap(tmp_path, SRW_EXPOSURE_MODE="multi-host")

    assert result.returncode != 0
    assert "missing required binary: mkcert" in result.stderr


def test_tilt_wrapper_prints_the_single_origin_url(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "deployment").mkdir()
    for name in ("local-dev-up.sh", "local-dev-tilt-up.sh"):
        shutil.copy2(REPO / "scripts" / name, repo / "scripts" / name)
    (repo / "deployment" / "values-local.yaml").write_text("license:\n  acceptTerms: true\n")
    fake_path = _fake_path(tmp_path / "fake")
    env = {
        **os.environ,
        "PATH": str(fake_path),
        "SRW_EXPOSURE_MODE": "single-origin",
        "SRW_IMAGE_PIN": "0",
    }

    result = subprocess.run(
        [str(repo / "scripts" / "local-dev-tilt-up.sh")],
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Cockpit:      https://localhost:8443" in result.stdout
    assert "tilt up" in result.stdout
