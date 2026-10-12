"""Safety contract and evaluators of the local C2 lease gate (never run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-credential-lease-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_credential_lease_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {
    "api": gate._API_PROGRAM,
    "lease": gate._LEASE_PROGRAM,
    "hash": gate._HASH_PROGRAM,
}


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _args(*extra: str):
    return gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d1a-0123456789"],
        ["--gate-id", "c2-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--max-ttl", "3600"],
        ["--job-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "port",
        "job",
        "refusals",
        "pause",
        "resume",
        "cancel",
        "delete",
        "completion",
        "session",
        "cleanup",
    ):
        assert f"- {phase}:" in out


@pytest.mark.parametrize("name", sorted(PROGRAMS))
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = PROGRAMS[name]
    compile(program, name, "exec")
    assert "\ncap_memory()\n" in program


def test_the_memory_cap_is_real():
    probe = gate._POD_MEMORY_CAP + (
        "\ncap_memory(64 << 20)\nimport resource\n"
        "print(resource.getrlimit(resource.RLIMIT_DATA)[0])\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert int(out) != -1 and int(out) > 0


def test_the_served_sets_name_every_lease_module_and_exist():
    orchestrator, agent = gate.SERVED_SETS
    for path in (
        "src/orchestrator/services/connector_credential_leases.py",
        "src/orchestrator/services/connector_driver_identities.py",
        "src/orchestrator/services/connector_lease_exchange.py",
        "src/orchestrator/routers/connector_lease_exchange.py",
        "src/orchestrator/application/connectors.py",
        "src/orchestrator/database/migrations/app/0347_connector_credential_leases.sql",
    ):
        assert path in orchestrator.files
    assert "src/agent/connectors" in agent.dirs
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (ROOT / path).is_file(), path
        for directory in served.dirs:
            assert (ROOT / directory).is_dir(), directory
        assert gate.expected_bytes(served)


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch):
    seen: list[tuple[list[str], str | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input")))
        body = json.dumps({"status": 200, "body": "{}"})
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.CredentialLeaseGate(_args("--password", "s3cret-pw"))
    run.api.call("GET", "/api/health")
    run.port = 8088
    run.identities["a"] = {"id": "i", "token": gate.secret("sdi_" + "q" * 49)}
    run.exchange("a", "00000000-0000-4000-8000-000000000001")
    assert seen
    for argv, stdin in seen:
        joined = " ".join(argv)
        assert "s3cret-pw" not in joined and "sdi_" + "q" * 49 not in joined
        assert "c2-upstream" not in joined
    assert any("sdi_" + "q" * 49 in (stdin or "") for _argv, stdin in seen)
    assert gate._scrub("x s3cret-pw y") == "x <redacted> y"
    for value in run.secrets.values():
        assert gate._scrub(value) == "<redacted>"


class TestEvaluators:
    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("missing", None),
            ("", None),
            (
                "file=600 dir=700 sha256=" + "a" * 64,
                {"file": "600", "dir": "700", "sha256": "a" * 64},
            ),
            ("file=600 dir=700 sha256=scl_token", None),
        ],
    )
    def test_the_lease_file_line(self, line, expected):
        assert gate.parse_lease_file(line) == expected

    def test_the_network_probe_reads_its_last_round(self):
        log = "api=http\nexchange=http\napi=http\nexchange=blocked\n"
        assert gate.parse_netprobe(log) == (True, False)
        assert gate.parse_netprobe("api=fail\nexchange=http\n") == (False, True)
        with pytest.raises(gate.GateError):
            gate.parse_netprobe("")

    @pytest.mark.parametrize(
        ("body", "like"),
        [
            ('{"error": "unknown_driver_identity"}', True),
            ('{"active": false}', True),
            ("<html><div class='active'>cockpit</div></html>", False),
            ('{"detail": "Not Found"}', False),
            ("", False),
        ],
    )
    def test_what_counts_as_the_exchange_answering(self, body, like):
        assert gate.answers_like_the_exchange(body) is like

    def test_a_lease_that_kept_its_expiry_and_lapsed_passes(self):
        observations = [
            {"expires": 100, "status": 200, "error": None},
            {"expires": 100, "status": 200, "error": None},
            {"expires": 100, "status": 403, "error": "lease_expired"},
        ]
        assert gate.self_renewal_verdict(observations)[0]

    @pytest.mark.parametrize(
        "observations",
        [
            [
                {"expires": 100, "status": 200, "error": None},
                {"expires": 160, "status": 403, "error": "lease_expired"},
            ],
            [
                {"expires": 100, "status": 200, "error": None},
                {"expires": 100, "status": 200, "error": None},
            ],
            [
                {"expires": 100, "status": 403, "error": "lease_expired"},
                {"expires": 100, "status": 403, "error": "lease_expired"},
            ],
            [{"expires": 100, "status": 403, "error": "lease_expired"}],
        ],
    )
    def test_a_moved_expiry_or_no_lapse_fails(self, observations):
        assert not gate.self_renewal_verdict(observations)[0]


class TestRun:
    def _gate(self, monkeypatch, calls: list[str], *, fail: set[str]):
        run = gate.CredentialLeaseGate(_args())

        def phase(name):
            def step():
                calls.append(name)
                if name in fail:
                    raise gate.GateError(f"{name} broke")

            step.__name__ = name
            return step

        for name in (
            "preflight",
            "fixture",
            "port_checks",
            "job_checks",
            "refusals",
            "pause_and_resume",
            "cancel",
            "delete",
            "completion",
            "session",
        ):
            monkeypatch.setattr(run, name, phase(name))
        monkeypatch.setattr(run, "cleanup", lambda: calls.append("cleanup") or [])
        monkeypatch.setattr(run, "residue", lambda: calls.append("residue") or [])
        return run

    def test_a_broken_fixture_still_cleans_up_and_fails(self, monkeypatch):
        calls: list[str] = []
        run = self._gate(monkeypatch, calls, fail={"fixture"})
        assert run.run() == 1
        assert calls == ["preflight", "fixture", "cleanup", "residue"]

    def test_a_broken_group_does_not_hide_the_next(self, monkeypatch):
        calls: list[str] = []
        run = self._gate(monkeypatch, calls, fail={"refusals", "delete"})
        assert run.run() == 1
        assert calls == [
            "preflight",
            "fixture",
            "port_checks",
            "job_checks",
            "refusals",
            "delete",
            "completion",
            "session",
            "cleanup",
            "residue",
        ]
        failed = [name for name, ok, _ in run.report.results if not ok]
        assert failed == ["refusals: infrastructure", "delete: infrastructure"]

    def test_keep_skips_the_cleanup(self, monkeypatch, capsys):
        calls: list[str] = []
        run = self._gate(monkeypatch, calls, fail=set())
        run.args.keep = True
        run.run()
        assert "cleanup" not in calls and "kept:" in capsys.readouterr().out


def test_cleanup_reaches_everything_the_run_named(monkeypatch):
    run = gate.CredentialLeaseGate(_args())
    run.thread = "00000000-0000-4000-8000-0000000000aa"
    run.jobs = {"main": "00000000-0000-4000-8000-0000000000bb"}
    run.connectors = {
        "a": "00000000-0000-4000-8000-0000000000cc",
        "b": "00000000-0000-4000-8000-0000000000dd",
    }
    run.project = "00000000-0000-4000-8000-0000000000ee"
    run.probe_started = True
    leftover_job = "00000000-0000-4000-8000-0000000000ff"
    monkeypatch.setattr(run, "titled_threads", lambda: [run.thread])
    monkeypatch.setattr(run, "described_jobs", lambda: [leftover_job])
    api_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        run.api,
        "call",
        lambda method, path, body=None: api_calls.append((method, path)) or (404, {}),
    )
    commands: list[list[str]] = []
    monkeypatch.setattr(gate, "command", lambda argv, **k: commands.append(argv) or "")
    monkeypatch.setattr(gate, "sql", lambda query: "0")

    assert run.cleanup() == []

    paths = [path for _method, path in api_calls]
    assert any(run.thread in path and "permanent=true" in path for path in paths)
    for job in (run.jobs["main"], leftover_job):
        assert ("DELETE", f"/api/jobs/{job}") in api_calls
    for datasource_id in run.connectors.values():
        assert ("DELETE", f"/api/datasources/{datasource_id}") in api_calls
    assert ("DELETE", f"/api/projects/{run.project}") in api_calls
    assert any(
        f"{gate.GATE_LABEL}={run.gate_id}" in " ".join(argv) for argv in commands
    )


def test_residue_counts_leases_and_identities_of_the_runs_connectors(monkeypatch):
    run = gate.CredentialLeaseGate(_args())
    run.connectors = {"a": "00000000-0000-4000-8000-0000000000cc"}
    queries: list[str] = []

    def fake_sql(query):
        queries.append(query)
        return "1" if "connector_credential_leases" in query else "0"

    monkeypatch.setattr(gate, "sql", fake_sql)
    monkeypatch.setattr(run, "titled_threads", lambda: [])
    monkeypatch.setattr(run, "described_jobs", lambda: [])
    monkeypatch.setattr(gate, "wait_for", lambda *a, **k: True)
    left = run.residue()
    assert left == ["1 rows in connector_credential_leases"]
    assert any(run.connectors["a"] in query for query in queries)
    assert any(
        "connector_driver_identities" in query and run.gate_id in query
        for query in queries
    )


def _ready_pod(name: str) -> dict:
    return {
        "metadata": {"name": name},
        "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
    }


@pytest.mark.parametrize(
    ("env", "configured"),
    [
        ({"PROBE": "true", "PORT": "8088", "TTL": "120", "SWEEP": "15"}, True),
        ({"PROBE": "true", "PORT": "8088", "TTL": "900", "SWEEP": "60"}, False),
        ({"PROBE": "false", "PORT": "8088", "TTL": "120", "SWEEP": "15"}, False),
        ({"PROBE": "true", "PORT": "8085", "TTL": "120", "SWEEP": "15"}, False),
        ({"PROBE": "true", "PORT": "0", "TTL": "120", "SWEEP": "15"}, False),
    ],
)
def test_preflight_refuses_a_deployment_the_gate_cannot_wait_for(
    monkeypatch, env, configured
):
    run = gate.CredentialLeaseGate(_args("--max-ttl", "300"))
    monkeypatch.setattr(run, "pods", lambda component: [_ready_pod(component)])
    monkeypatch.setattr(run, "served_problems", lambda pod, served: [])
    names = {
        "CONNECTOR_LEASE_PROBE_ENABLED": env["PROBE"],
        "CONNECTOR_LEASE_EXCHANGE_PORT": env["PORT"],
        "CONNECTOR_LEASE_TTL_SECONDS": env["TTL"],
        "CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS": env["SWEEP"],
    }
    monkeypatch.setattr(run, "orchestrator_env", lambda name: names[name])
    answers = iter(["2", "00000000-0000-4000-8000-000000000001"])
    monkeypatch.setattr(gate, "sql", lambda query: next(answers))
    if configured:
        run.preflight()
        assert (run.port, run.ttl, run.sweep) == (8088, 120, 15)
    else:
        with pytest.raises(gate.GateError):
            run.preflight()
    assert [ok for _name, ok, _detail in run.report.results][1] is configured
