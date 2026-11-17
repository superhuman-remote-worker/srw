"""Exercise both CI audit steps with accepted and blocking vulnerability reports."""

import copy
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BRACES_ADVISORY = "GHSA-vfj7-8cjw-p6xm"
PICOMATCH_ADVISORY = "GHSA-fc5q-39xc-vjpf"
# The lockfile as reviewed: one picomatch, needed outside the dev-only packages
# by the Angular CLI's core and by the two glob helpers.
PICOMATCH_LOCK = {
    "": {"dependencies": {"@angular/pwa": "^21.0.0"}},
    "node_modules/picomatch": {"version": "4.0.5"},
    "node_modules/@angular-devkit/core": {"dependencies": {"picomatch": "4.0.4"}},
    "node_modules/tinyglobby": {
        "devOptional": True,
        "dependencies": {"fdir": "^6.5.0", "picomatch": "^4.0.3"},
    },
    "node_modules/fdir": {
        "devOptional": True,
        "peerDependencies": {"picomatch": "^3 || ^4"},
    },
    "node_modules/vite": {"dev": True, "dependencies": {"picomatch": "^4.0.3"}},
}


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

    def run(report, packages=None, picomatch_latest="4.0.7"):
        if packages is None:
            packages = {"node_modules/braces": {"version": "3.0.3", "dev": True}}
        (tmp_path / "package-lock.json").write_text(json.dumps({"packages": packages}))
        # The step asks the registry for the newest picomatch and hands it on in
        # this variable; None stands for a registry that did not answer.
        env = {k: v for k, v in os.environ.items() if k != "PICOMATCH_LATEST"}
        if picomatch_latest is not None:
            env["PICOMATCH_LATEST"] = picomatch_latest
        return subprocess.run(
            [sys.executable, "-c", program],
            input=json.dumps(report),
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
            env=env,
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


@pytest.fixture
def picomatch_report():
    """The shape npm reported on 2026-11-17: picomatch itself, tools that are
    vulnerable only through it, and one that also carries a moderate advisory."""
    return {
        "vulnerabilities": {
            "picomatch": {
                "severity": "high",
                "via": [
                    {
                        "url": f"https://github.com/advisories/{PICOMATCH_ADVISORY}",
                        "severity": "high",
                    }
                ],
            },
            "tinyglobby": {"severity": "high", "via": ["picomatch"]},
            "vite": {"severity": "high", "via": ["picomatch", "tinyglobby"]},
            "glob": {
                "severity": "moderate",
                "via": [
                    {
                        "url": "https://github.com/advisories/GHSA-3g3m-2fjr-6pw4",
                        "severity": "moderate",
                    }
                ],
            },
            "@angular/cli": {"severity": "high", "via": ["glob", "tinyglobby"]},
        }
    }


def test_picomatch_exception_covers_the_tools_that_depend_on_it(
    audit_gate, picomatch_report
):
    result = audit_gate(picomatch_report, PICOMATCH_LOCK)
    assert result.returncode == 0, result.stdout + result.stderr
    assert PICOMATCH_ADVISORY in result.stdout


@pytest.mark.parametrize("latest", ["4.0.8", "4.1.0", "5.0.0"])
def test_picomatch_exception_ends_when_a_newer_release_is_published(
    audit_gate, picomatch_report, latest
):
    result = audit_gate(picomatch_report, PICOMATCH_LOCK, picomatch_latest=latest)
    assert result.returncode == 1
    assert "4 unignored high/critical vulnerabilities" in result.stdout
    assert f"picomatch {latest} is published" in result.stdout


@pytest.mark.parametrize("latest", [None, "", "not a version", "5.0.0-beta.1"])
def test_picomatch_exception_is_off_when_the_newest_release_is_unknown(
    audit_gate, picomatch_report, latest
):
    result = audit_gate(picomatch_report, PICOMATCH_LOCK, picomatch_latest=latest)
    assert result.returncode == 1
    assert "newest published picomatch version is unknown" in result.stdout


@pytest.mark.parametrize(
    ("extra", "named"),
    [
        (
            {"node_modules/some-runtime-lib": {"dependencies": {"picomatch": "^4"}}},
            "some-runtime-lib",
        ),
        (
            {"": {"dependencies": {"picomatch": "^4.0.5"}}},
            "the Cockpit itself",
        ),
        (
            {"node_modules/vite": {"dependencies": {"picomatch": "^4.0.3"}}},
            "vite",
        ),
    ],
)
def test_picomatch_exception_is_off_for_a_dependent_that_was_not_reviewed(
    audit_gate, picomatch_report, extra, named
):
    result = audit_gate(picomatch_report, {**PICOMATCH_LOCK, **extra})
    assert result.returncode == 1
    assert "4 unignored high/critical vulnerabilities" in result.stdout
    assert (
        f"not reviewed, depends on picomatch outside the dev-only packages: {named}"
        in (result.stdout)
    )


def test_new_advisory_on_picomatch_still_blocks(audit_gate, picomatch_report):
    picomatch_report["vulnerabilities"]["picomatch"]["via"].append(
        {"url": "https://github.com/advisories/GHSA-new-advisory", "severity": "high"}
    )
    result = audit_gate(picomatch_report, PICOMATCH_LOCK)
    assert result.returncode == 1
    assert "4 unignored high/critical vulnerabilities" in result.stdout


def test_package_is_judged_by_its_advisories_that_are_not_accepted(
    audit_gate, picomatch_report
):
    """npm rates @angular/cli high through picomatch. What is left on it once
    that advisory is accepted is moderate, so it does not block; a high advisory
    that is not accepted still does."""
    assert audit_gate(picomatch_report, PICOMATCH_LOCK).returncode == 0

    picomatch_report["vulnerabilities"]["glob"]["severity"] = "high"
    picomatch_report["vulnerabilities"]["glob"]["via"][0]["severity"] = "high"
    result = audit_gate(picomatch_report, PICOMATCH_LOCK)
    assert result.returncode == 1
    assert "glob: high" in result.stdout
    assert "@angular/cli: high" in result.stdout
    assert "2 unignored high/critical vulnerabilities" in result.stdout


def test_packages_that_only_name_each_other_still_block(audit_gate):
    result = audit_gate(
        {
            "vulnerabilities": {
                "a": {"severity": "high", "via": ["b"]},
                "b": {"severity": "high", "via": ["a"]},
            }
        }
    )
    assert result.returncode == 1
    assert "2 unignored high/critical vulnerabilities" in result.stdout


def test_dependency_missing_from_the_report_still_blocks(audit_gate):
    result = audit_gate(
        {"vulnerabilities": {"a": {"severity": "critical", "via": ["gone"]}}}
    )
    assert result.returncode == 1
    assert "a: critical" in result.stdout


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
