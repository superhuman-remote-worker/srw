"""Exercise both CI audit steps with accepted and blocking vulnerability reports."""

import copy
import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BRACES_ADVISORY = "GHSA-vfj7-8cjw-p6xm"


@pytest.fixture(params=["main", "develop"])
def audit_gate(request, tmp_path):
    workflow = yaml.safe_load(
        (ROOT / f".github/workflows/{request.param}.yml").read_text()
    )
    step = next(
        step
        for step in workflow["jobs"]["dependency-audit"]["steps"]
        if step.get("name") == "Audit npm dependencies"
    )
    command = shlex.split(step["run"])
    program = command[command.index("-c") + 1]

    def run(report, packages=None):
        if packages is None:
            packages = {"node_modules/braces": {"version": "3.0.3", "dev": True}}
        (tmp_path / "package-lock.json").write_text(json.dumps({"packages": packages}))
        return subprocess.run(
            [sys.executable, "-c", program],
            input=json.dumps(report),
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        )

    return run


@pytest.fixture
def braces_report():
    return {
        "vulnerabilities": {
            "stylelint": {"severity": "high", "via": ["micromatch"]},
            "micromatch": {"severity": "high", "via": ["braces"]},
            "braces": {
                "severity": "high",
                "via": [{"url": f"https://github.com/advisories/{BRACES_ADVISORY}"}],
            },
        }
    }


def test_dev_only_braces_exception_propagates_to_dependents(audit_gate, braces_report):
    result = audit_gate(braces_report)
    assert result.returncode == 0, result.stderr
    assert "npm audit passed" in result.stdout
    assert BRACES_ADVISORY in result.stdout


@pytest.mark.parametrize(
    "packages",
    [
        {},
        {"node_modules/braces": {"version": "3.0.3", "dev": False}},
        {"node_modules/braces": {"version": "3.0.3"}},
        {"node_modules/braces": {"version": "3.0.3", "devOptional": True}},
        {"node_modules/braces": {"version": "3.0.4", "dev": True}},
        {
            "node_modules/braces": {"version": "3.0.3", "dev": True},
            "node_modules/other/node_modules/braces": {"version": "3.0.3"},
        },
    ],
)
def test_exception_rejects_changed_version_or_production_use(
    audit_gate, braces_report, packages
):
    result = audit_gate(braces_report, packages)
    assert result.returncode == 1
    assert "3 unignored high/critical vulnerabilities" in result.stdout


@pytest.mark.parametrize("severity", ["high", "critical"])
def test_new_advisory_on_braces_still_blocks(audit_gate, braces_report, severity):
    report = copy.deepcopy(braces_report)
    report["vulnerabilities"]["braces"]["via"].append(
        {"url": "https://github.com/advisories/GHSA-new-advisory"}
    )
    report["vulnerabilities"]["braces"]["severity"] = severity
    result = audit_gate(report)
    assert result.returncode == 1
    assert "3 unignored high/critical vulnerabilities" in result.stdout


def test_http_cache_advisory_is_not_excepted(audit_gate, braces_report):
    braces_report["vulnerabilities"]["http-cache-semantics"] = {
        "severity": "high",
        "via": [{"url": "https://github.com/advisories/GHSA-ch52-4w7c-c8xp"}],
    }
    result = audit_gate(braces_report)
    assert result.returncode == 1
    assert "http-cache-semantics: high" in result.stdout
    assert "1 unignored high/critical vulnerabilities" in result.stdout


def test_high_finding_without_root_metadata_still_blocks(audit_gate):
    result = audit_gate(
        {"vulnerabilities": {"unknown": {"severity": "high", "via": []}}}
    )
    assert result.returncode == 1
    assert "unknown: high" in result.stdout


@pytest.mark.parametrize(
    "report",
    [
        {"error": {"code": "E503"}},
        {"error": {"code": "E503"}, "vulnerabilities": {}},
        {},
        {"vulnerabilities": None},
    ],
)
def test_audit_errors_cannot_pass_the_gate(audit_gate, report):
    result = audit_gate(report)
    assert result.returncode == 2
    assert "did not return a vulnerability report" in result.stderr


def test_empty_vulnerability_report_passes(audit_gate):
    assert audit_gate({"vulnerabilities": {}}).returncode == 0
