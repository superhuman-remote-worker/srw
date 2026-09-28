"""The Python audit may retry a PyPI outage without weakening the CVE gate."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


WRAPPER = Path(__file__).resolve().parent.parent / "scripts" / "retry_pip_audit.py"
SERVICE_ERROR = """Traceback (most recent call last):
requests.exceptions.HTTPError: {status} Server Error: Backend is unhealthy for url: {url}

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
pip_audit._service.interface.ServiceError
"""
PYPI_URL = "https://pypi.org/pypi/webdavclient3/3.14.7/json"


def _run_audit(
    tmp_path: Path, steps: list[dict[str, object]]
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """Replace only the external audit executable; run the real wrapper CLI."""
    fake_audit = tmp_path / "pip-audit"
    fake_audit.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "state = pathlib.Path(os.environ['FAKE_AUDIT_STATE'])\n"
        "calls = json.loads(state.read_text()) if state.exists() else []\n"
        "calls.append(sys.argv[1:])\n"
        "state.write_text(json.dumps(calls))\n"
        "step = json.loads(os.environ['FAKE_AUDIT_STEPS'])[len(calls) - 1]\n"
        "sys.stdout.write(step.get('stdout', ''))\n"
        "sys.stderr.write(step.get('stderr', ''))\n"
        "sys.exit(step['exit_code'])\n",
        encoding="utf-8",
    )
    fake_audit.chmod(0o755)
    state = tmp_path / "calls.json"
    env = os.environ.copy()
    env.update(
        {
            "FAKE_AUDIT_STATE": str(state),
            "FAKE_AUDIT_STEPS": json.dumps(steps),
            "PATH": f"{tmp_path}{os.pathsep}{env['PATH']}",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            str(WRAPPER),
            "--",
            "pip-audit",
            "--desc",
            "--ignore-vuln",
            "CVE-2026-4539",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=30,
    )
    return result, json.loads(state.read_text()) if state.exists() else []


@pytest.mark.parametrize("status", [502, 503, 504])
def test_exact_pypi_service_error_retries_full_audit_then_passes(
    tmp_path: Path, status: int
):
    failure = SERVICE_ERROR.format(status=status, url=PYPI_URL)
    result, calls = _run_audit(
        tmp_path,
        [
            {"exit_code": 1, "stderr": failure},
            {"exit_code": 0, "stdout": "No known vulnerabilities found\n"},
        ],
    )
    assert result.returncode == 0
    assert result.stdout == "No known vulnerabilities found\n"
    assert failure in result.stderr
    assert calls == [
        ["--desc", "--ignore-vuln", "CVE-2026-4539"],
        ["--desc", "--ignore-vuln", "CVE-2026-4539"],
    ]


def test_persistent_outage_fails_after_three_full_attempts(tmp_path: Path):
    failure = SERVICE_ERROR.format(status=503, url=PYPI_URL)
    result, calls = _run_audit(tmp_path, [{"exit_code": 1, "stderr": failure}] * 3)
    assert result.returncode == 1
    assert len(calls) == 3
    assert result.stderr.count(failure) == 3


@pytest.mark.parametrize(
    ("stderr", "stdout", "exit_code"),
    [
        ("Found 1 known vulnerability\nCVE-2026-99999\n", "", 1),
        (SERVICE_ERROR.format(status=404, url=PYPI_URL), "", 1),
        (
            SERVICE_ERROR.format(status=503, url="https://example.org/pypi/pkg/1/json"),
            "",
            1,
        ),
        (
            SERVICE_ERROR.format(status=503, url=PYPI_URL).replace(
                "pip_audit._service.interface.ServiceError", "RuntimeError"
            ),
            "",
            1,
        ),
        (
            SERVICE_ERROR.format(status=503, url=PYPI_URL).replace(
                "The above exception was the direct cause of the following exception:",
                "An unrelated error followed",
            ),
            "",
            1,
        ),
        (
            SERVICE_ERROR.format(status=503, url=PYPI_URL).replace(
                "Traceback (most recent call last):\npip_audit",
                "CVE-2026-99999\nTraceback (most recent call last):\npip_audit",
            ),
            "",
            1,
        ),
        (
            SERVICE_ERROR.format(status=503, url=PYPI_URL),
            "Found 1 known vulnerability: CVE-2026-99999\n",
            1,
        ),
        ("unexpected audit failure\n", "", 7),
    ],
)
def test_unclassified_failure_or_vulnerability_fails_without_retry(
    tmp_path: Path, stderr: str, stdout: str, exit_code: int
):
    result, calls = _run_audit(
        tmp_path,
        [{"exit_code": exit_code, "stderr": stderr, "stdout": stdout}],
    )
    assert result.returncode == exit_code
    assert result.stdout == stdout
    assert result.stderr == stderr
    assert len(calls) == 1
