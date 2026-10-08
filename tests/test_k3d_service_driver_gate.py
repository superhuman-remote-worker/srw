"""Safety contract and evaluators of the local D5 service driver gate (never
run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-service-driver-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_service_driver_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {
    "api": gate._API_PROGRAM,
    "net": gate._NET_PROGRAM,
    "bind": gate._BIND_PROGRAM,
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
        ["--gate-id", "c2-0123456789"],
        ["--gate-id", "d5-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--egress-host", "a b"],
        ["--egress-port", "0"],
        ["--canary", "one.one.one.one:443"],
        ["--max-idle", "3600"],
        ["--start-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_the_values_keys(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "pod",
        "reach",
        "egress",
        "exchange",
        "sharing",
        "moved-tag",
        "idle",
        "cleanup",
    ):
        assert f"- {phase}:" in out
    for key in (
        "connectors.servicePods.enabled: true",
        "connectors.servicePods.idleSeconds: 60",
        'connectors.drivers.registry.insecureHosts: ["srw-registry:5000"]',
        "connectors.drivers.echo.enabled: true",
        "orchestrator.connectorLeases.exchangePort: 8088",
    ):
        assert key in out


def test_the_values_keys_are_the_k3d_profile():
    import yaml

    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    pods = example["connectors"]["servicePods"]
    assert pods["enabled"] is True and pods["idleSeconds"] <= 120
    assert (
        "srw-registry:5000"
        in example["connectors"]["drivers"]["registry"]["insecureHosts"]
    )
    assert example["connectors"]["drivers"]["echo"]["enabled"] is True
    assert example["orchestrator"]["connectorLeases"]["exchangePort"] == 8088


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


def test_the_served_set_names_every_d5_module_and_exists():
    orchestrator, agent = gate.SERVED_SETS
    assert agent.component == "agent-stateless"
    assert "src/shared/connectors" in agent.dirs
    for path in (
        "src/shared/oci_registry.py",
        "src/orchestrator/services/connector_service_hosting.py",
        "src/orchestrator/services/connector_service_launch.py",
        "src/orchestrator/services/connector_egress.py",
        "src/orchestrator/services/connector_service_images.py",
        "src/orchestrator/database/migrations/app/0363_connector_service_pod_key_idx.notx.sql",
    ):
        assert path in orchestrator.files
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (ROOT / path).is_file(), path
        for directory in served.dirs:
            assert (ROOT / directory).is_dir(), directory
        assert gate.expected_bytes(served)
    for name in gate.MIGRATIONS:
        assert (ROOT / "src/orchestrator/database/migrations/app" / name).is_file()


def _pod(**over):
    shim = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    pod = {
        "spec": {
            "automountServiceAccountToken": False,
            "initContainers": [
                {"name": "canary-wait", "securityContext": dict(shim)},
                {"name": "install-shim", "securityContext": dict(shim)},
            ],
            "containers": [{"name": "driver", "securityContext": dict(shim)}],
            "volumes": [{"name": "delivery", "secret": {"secretName": "x"}}],
        },
        "status": {
            "phase": "Running",
            "initContainerStatuses": [
                {"name": "canary-wait", "state": {"terminated": {"exitCode": 0}}}
            ],
            "containerStatuses": [{"name": "driver", "ready": True}],
        },
    }
    pod.update(over)
    return pod


def test_pod_evaluators():
    pod = _pod()
    assert gate.capabilities_dropped(pod) == []
    assert gate.token_mounted(pod) is False
    assert gate.canary_passed(pod) is True
    assert gate.pod_ready(pod) is True
    loose = _pod()
    loose["spec"]["containers"][0]["securityContext"] = {
        "capabilities": {"add": ["NET_ADMIN"]}
    }
    assert len(gate.capabilities_dropped(loose)) == 3
    mounted = _pod()
    mounted["spec"]["automountServiceAccountToken"] = None
    assert gate.token_mounted(mounted) is True
    projected = _pod()
    projected["spec"]["volumes"].append(
        {"name": "t", "projected": {"sources": [{"serviceAccountToken": {}}]}}
    )
    assert gate.token_mounted(projected) is True
    failed = _pod()
    failed["status"]["initContainerStatuses"][0]["state"] = {
        "terminated": {"exitCode": 1}
    }
    assert gate.canary_passed(failed) is False
    reordered = _pod()
    reordered["spec"]["initContainers"].reverse()
    assert gate.canary_passed(reordered) is False


def test_the_netprobe_and_moved_tag_verdicts():
    assert gate.parse_netprobe("driver=http\ndriver=blocked\n") is False
    assert gate.parse_netprobe("driver=blocked\ndriver=http\n") is True
    with pytest.raises(gate.GateError):
        gate.parse_netprobe("nothing")
    ok, _ = gate.moved_tag_verdict(
        {"digest": "sha256:" + "a" * 64},
        {
            "refused": "The image behind x changed its contract (protocol 2.0 is not supported)"
        },
    )
    assert ok
    ok, _ = gate.moved_tag_verdict({"digest": "sha256:" + "a" * 64}, {"digest": "x"})
    assert not ok


def test_the_incompatible_label_breaks_the_echo_contract():
    from orchestrator.services.connector_service_images import config_errors
    from shared.connectors.builtin import ECHO_SERVICE_SPEC
    from shared.connectors.images import SpecContract, compatibility_problems

    new = SpecContract.of_label(json.loads(gate.INCOMPATIBLE_SPEC))
    problems = compatibility_problems(
        SpecContract.of_driver(ECHO_SERVICE_SPEC),
        new,
        config_errors=config_errors(new.config_schema, {"host": "h", "port": 1}),
    )
    assert any("protocol 2.0" in problem for problem in problems)
    assert any("credential slots disappeared" in problem for problem in problems)


def test_cleanup_removes_only_the_gates_own_registry_tag(monkeypatch):
    seen: list[list[str]] = []

    def fake_run(argv, **kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(gate.ServiceDriverGate, "registry_tags", lambda self: ["dev"])
    run = gate.ServiceDriverGate(_args("--gate-id", "d5-0123456789"))
    assert run.delete_pushed_tag() is True
    (argv,) = seen
    assert argv[:5] == ["docker", "exec", gate.REGISTRY_CONTAINER, "rm", "-rf"]
    assert argv[5].endswith("/srw-driver-echo/_manifests/tags/d5-0123456789")
    # Never a manifest delete: a digest may be shared with another tag.
    assert all("DELETE" not in part for part in argv)

    run.images_pushed = True
    monkeypatch.setattr(
        gate.ServiceDriverGate, "registry_tags", lambda self: ["dev", "d5-0123456789"]
    )
    assert run.delete_pushed_tag() is False


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch):
    seen: list[tuple[list[str], str | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input")))
        body = json.dumps([{"status": 200, "body": {}}])
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.ServiceDriverGate(_args("--password", "s3cret-pw"))
    run.identity_tokens["a"] = gate.secret("sdi_" + "q" * 49)
    run.from_orchestrator(
        [
            {
                "kind": "http",
                "method": "POST",
                "url": "http://127.0.0.1:8088/v1/leases/exchange",
                "identity": run.identity_tokens["a"],
                "lease_id": "00000000-0000-4000-8000-000000000001",
                "body": {"operation": "read"},
            }
        ]
    )
    assert seen
    for argv, _stdin in seen:
        joined = " ".join(argv)
        assert "sdi_" + "q" * 49 not in joined and "s3cret-pw" not in joined
        assert "d5-upstream" not in joined
    assert any("sdi_" + "q" * 49 in (stdin or "") for _argv, stdin in seen)
    assert gate._scrub("x s3cret-pw y") == "x <redacted> y"
    for value in run.secrets.values():
        assert gate._scrub(value) == "<redacted>"


def test_the_default_deny_probe_verdict_is_its_last_rounds():
    raced = "\n".join(["canary=open"] * 3 + ["canary=closed"] * 9)
    assert gate.parse_denyprobe(raced) is True
    unenforced = "\n".join(["canary=closed"] * 2 + ["canary=open"] * 10)
    assert gate.parse_denyprobe(unenforced) is False
    flapping = "\n".join(["canary=closed"] * 10 + ["canary=open", "canary=closed"])
    assert gate.parse_denyprobe(flapping) is False
    with pytest.raises(gate.GateError):
        gate.parse_denyprobe("canary=closed\n" * 3)


def test_a_pod_that_never_gets_ready_is_diagnosed():
    pod = {
        "status": {
            "phase": "Pending",
            "initContainerStatuses": [
                {
                    "name": "canary-wait",
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {"terminated": {"exitCode": 1}},
                    "restartCount": 2,
                },
                {
                    "name": "install-shim",
                    "state": {"waiting": {"reason": "PodInitializing"}},
                },
            ],
        }
    }
    log = (
        "srw-driver-shim: canary 10.43.0.1:8085 is still reachable\n"
        "srw-driver-shim: canary-wait: no 3 rounds ...: the default deny is not "
        "enforced\n"
    )
    found = gate.pod_diagnosis(pod, log)
    assert "phase=Pending" in found
    assert "canary-wait=waiting/CrashLoopBackOff restarts=2 last-exit=1" in found
    assert found.endswith("the default deny is not enforced")
    assert gate.pod_diagnosis(None) == "no pod"
