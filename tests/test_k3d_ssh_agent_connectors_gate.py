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


def _runner():
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    return runner


def test_a_regressed_201_on_a_refused_connector_is_still_cleaned_up(monkeypatch):
    runner = _runner()
    created = iter(range(100))

    def call(method, path, body=None):
        if method == "POST":
            return 201, {"id": f"stray-{next(created)}"}
        return 204, {}

    deleted: list[str] = []

    def cleanup_call(method, path, body=None):
        deleted.append(path)
        return 204, {}

    monkeypatch.setattr(runner.api, "call", call)
    runner.validation()

    assert not runner.report.passed
    assert sorted(runner.connectors.values()) == ["stray-0", "stray-1", "stray-2"]
    monkeypatch.setattr(runner.api, "call", cleanup_call)
    runner.cleanup()
    assert sorted(deleted) == [
        "/api/datasources/stray-0",
        "/api/datasources/stray-1",
        "/api/datasources/stray-2",
    ]


def test_the_alias_host_is_part_of_validation(monkeypatch):
    runner = _runner()
    bodies: list[dict] = []

    def call(method, path, body=None):
        bodies.append(body)
        return 400, {"detail": "refused passphrase"}

    monkeypatch.setattr(runner.api, "call", call)
    runner.validation()

    assert runner.report.passed
    assert not runner.connectors
    assert any(
        (body.get("config") or {}).get("host", "").lower().startswith("srw-repo-")
        for body in bodies
    )


@pytest.mark.parametrize(
    ("rc", "output", "count"),
    [
        (0, "ssh-agents=0", 0),
        (0, "noise\nssh-agents=2", 2),
        (0, "", None),
        (0, "0", None),
        (1, "ssh-agents=0", None),
        (126, "", None),
    ],
)
def test_the_end_count_needs_a_clean_exit_and_an_answer(rc, output, count):
    assert gate.ssh_agent_count(rc, output) == count


def test_the_end_count_script_counts_from_proc():
    result = subprocess.run(
        ["bash", "-s"], input=gate.COUNT_SSH_AGENTS, text=True, capture_output=True
    )
    assert result.returncode == 0
    assert gate.ssh_agent_count(result.returncode, result.stdout) is not None


@pytest.mark.parametrize("pods_after", [[], [{"metadata": {"name": "pod"}}]])
def test_end_fails_when_the_workspace_exec_fails(monkeypatch, pods_after):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    listings = iter([[{"metadata": {"name": "pod"}}], pods_after])
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: {})
    monkeypatch.setattr(gate, "sql", lambda query: "none")
    monkeypatch.setattr(gate.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        gate, "command", lambda args, **k: gate.json.dumps({"items": next(listings)})
    )
    monkeypatch.setattr(runner, "ws", lambda pod, script, check=True: (1, ""))

    runner.end()

    # A vanished workspace is a pass; a live one that did not answer is not.
    assert runner.report.passed is (not pods_after)


@pytest.mark.parametrize(
    ("origin", "alias"),
    [
        ("ssh://srw-repo-" + "a" * 32 + "/srw/r.git", "srw-repo-" + "a" * 32),
        ("srw-repo-" + "b" * 32 + ":srw/r.git", "srw-repo-" + "b" * 32),
        ("ssh://git@gitea:2222/srw/r.git", None),
        ("ssh://srw-repo-" + "a" * 32 + "/srw/other.git", None),
    ],
)
def test_origin_alias_accepts_both_url_forms(origin, alias):
    assert gate.origin_alias(origin, owner="srw", repo="r") == alias
