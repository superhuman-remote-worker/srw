"""Safety contract and expectations of the local D2 matrix gate (never run here)."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from shared.connectors.builtin import BUILTIN_SPECS

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-connector-matrix-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_connector_matrix_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

MATRIX = json.loads(
    (ROOT / "cockpit/src/app/core/models/fixtures/connector-drivers.json").read_text()
)
NAMES = [spec.name for spec in BUILTIN_SPECS]


def _driver(name: str) -> dict:
    return next(d for d in MATRIX["drivers"] if d["name"] == name)


def test_a_type_variant_is_not_the_types_driver():
    """srw.mcp-remote/v1 serves some mcp rows; the form and the link rows
    still take srw.mcp/v1 as the mcp type's driver."""
    owners = [
        d["name"]
        for d in MATRIX["drivers"]
        if d["legacy_type"] == "mcp" and gate.owns_type(d)
    ]
    assert owners == ["srw.mcp/v1"]
    assert gate.owns_type(_driver("srw.mcp-remote/v1")) is False
    # A matrix from before D3a has no flag: every typed driver owned its type.
    assert gate.owns_type({"legacy_type": "mcp"}) is True


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--user", "Robert'); DROP"],
        ["--base-url", "https://cockpit.example.com"],
    ],
)
def test_refuses_anything_outside_the_local_cluster(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("preflight", "api", "page", "picker", "links"):
        assert f"- {phase}:" in out


def test_the_password_never_reaches_an_argument(monkeypatch):
    seen: list[list[str]] = []

    def fake_run(argv, **kwargs):
        seen.append(argv)
        body = json.dumps({"status": 200, "body": json.dumps(MATRIX)})
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    args = gate.build_parser().parse_args(["--run", "--password", "s3cret-pw"])
    gate.MatrixGate(args).api()
    assert seen and all("s3cret-pw" not in " ".join(argv) for argv in seen)
    assert gate._scrub("token s3cret-pw") == "token <redacted>"


class TestExpectations:
    def test_the_built_in_matrix_passes(self):
        assert gate.matrix_problems(MATRIX, NAMES) == []

    def test_offered_levels_mirror_the_cockpit(self):
        for kind, choices in gate.LITERAL_CHOICES.items():
            driver = next(d for d in MATRIX["drivers"] if d["legacy_type"] == kind)
            assert gate.picker_expectation(driver)[0] == choices
        assert gate.LITERAL_CHOICES == {
            "mcp": ["read_write"],
            "kb": ["read_only"],
            "postgresql": ["read_only", "read_write"],
        }
        assert gate.picker_expectation(_driver("srw.env/v1"))[0] == []

    def test_a_public_connector_gets_the_declared_only_hint(self):
        # Public read-only binds nothing, so no enforced_by line is expected:
        # only an always read-only driver (the KB) gets the other hint.
        for driver in MATRIX["drivers"]:
            if not driver["legacy_type"]:
                continue
            hint = gate.picker_expectation(driver)[1]
            expected = (
                "visibilityKbHint"
                if driver["forced_read_only"]
                else "visibilityCredentialHint"
            )
            assert hint == expected, driver["name"]

    def test_a_link_binds_the_level_its_read_only_says(self):
        shape, level = gate.link_expectation(_driver("srw.postgresql/v1"), True)
        assert shape == "switch" and "READ ONLY transaction" in level["enforced_by"]
        assert gate.link_expectation(_driver("srw.postgresql/v1"), None)[1]["id"] == (
            "ReadWrite"
        )
        assert gate.link_expectation(_driver("srw.mcp/v1"), True)[0] == "badge"
        assert gate.link_expectation(_driver("srw.mcp/v1"), True)[1]["tools"] == "*"
        assert gate.link_expectation(_driver("srw.kb/v1"), None)[1]["id"] == "ReadOnly"
        for kind, read_only, shape in gate.LINK_ROWS:
            driver = next(d for d in MATRIX["drivers"] if d["legacy_type"] == kind)
            assert gate.link_expectation(driver, read_only)[0] == shape

    def test_a_drifted_matrix_is_reported(self):
        drifted = copy.deepcopy(MATRIX)
        postgres = next(
            d for d in drifted["drivers"] if d["name"] == "srw.postgresql/v1"
        )
        postgres["access_levels"][0]["enforced_by"] = ""
        postgres["trust"]["tier"] = "custom"
        postgres["egress"]["enforced"]["status"] = "verified"
        postgres["credential_slots"] = [
            {"name": "x", "schema": {"properties": {"p": {"default": "pw"}}}}
        ]
        problems = gate.matrix_problems(drifted, NAMES)
        assert any("no enforced_by line" in p for p in problems)
        assert any("not built-in" in p for p in problems)
        assert any("egress enforced" in p for p in problems)
        assert any("carries a value" in p for p in problems)
        assert gate.matrix_problems(MATRIX, NAMES[1:])
