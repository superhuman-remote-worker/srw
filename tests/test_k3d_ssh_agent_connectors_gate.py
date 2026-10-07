"""Safety contract for the local C1 ssh-agent connectors gate (never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-ssh-agent-connectors-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_ssh_agent_connectors_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--turn-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(monkeypatch, capsys):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("validation", "detach", "snapshot", "checkpoint", "wrong pin"):
        assert phase in out


def test_embedded_programs_compile():
    for program in (
        gate._API_PROGRAM,
        gate._GITEA_PROGRAM,
        gate._SNAPSHOT_PROGRAM,
        gate._HASH_PROGRAM,
    ):
        compile(program, "<gate program>", "exec")


def test_key_needles_identify_one_key_and_are_scrubbed():
    first, second = gate.make_key("x"), gate.make_key("y")
    assert first.needles and second.needles
    assert not set(first.needles) & set(second.needles)
    # The shared OpenSSH header line identifies nothing.
    assert first.private_key.splitlines()[1] not in first.needles
    assert "PRIVATE" not in gate._scrub(first.private_key).replace(
        "-----BEGIN OPENSSH PRIVATE KEY-----", ""
    ).replace("-----END OPENSSH PRIVATE KEY-----", "")
    for needle in first.needles:
        assert needle not in gate._scrub(first.private_key)


def test_workspace_scripts_are_valid_bash_and_carry_no_key(monkeypatch):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    scripts: list[str] = []

    def record(pod, script, *, check=True):
        scripts.append(script)
        return 0, ""

    monkeypatch.setattr(runner, "ws", record)
    runner.workspace_checks("pod", labels="ABCD")

    assert scripts
    for script in scripts:
        assert "PRIVATE KEY-----" not in script
        assert subprocess.run(["bash", "-n"], input=script, text=True).returncode == 0
    assert runner.report.results[0][0] == "no-key (ABCD)"


def test_session_and_detach_scripts_are_valid_bash(monkeypatch):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    runner.connectors = {label: f"id-{label}" for label in "ABCD"}
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.gitea = {"owner": "srw", "ssh_host": "srw-gitea-ssh", "ssh_port": 2222}
    scripts: list[str] = []

    def record(pod, script, *, check=True):
        scripts.append(script)
        return 0, ""

    monkeypatch.setattr(runner, "ws", record)
    monkeypatch.setattr(runner, "workspace_pod", lambda selector: "pod")
    monkeypatch.setattr(runner, "turn", lambda text, step: None)
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: {})
    monkeypatch.setattr(gate, "sql", lambda query: "0")
    monkeypatch.setattr(gate, "in_orchestrator", lambda *a, **k: {})

    runner.session_checks()
    runner.detach()

    assert len(scripts) > 5
    for script in scripts:
        assert subprocess.run(["bash", "-n"], input=script, text=True).returncode == 0
    # Nothing was observed, so nothing may pass by default.
    assert not runner.report.passed
