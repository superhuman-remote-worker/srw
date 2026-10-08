"""Safety contract and evaluators of the local D6 custom driver gate (never
run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-custom-driver-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_custom_driver_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {
    "api": gate._API_PROGRAM,
    "keycloak": gate._KEYCLOAK_PROGRAM,
    "hash": gate._HASH_PROGRAM,
}
DIGEST = "sha256:" + "ab" * 32
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
OPERATION = "11111111-2222-4333-8444-555555555555"


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
        ["--gate-id", "d5a-0123456789"],
        ["--gate-id", "d6-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--turn-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_the_values_keys(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "accounts",
        "register",
        "bind",
        "moved-tag",
        "revoke",
        "cleanup",
    ):
        assert f"- {phase}:" in out
    for key in (
        "connectors.servicePods.enabled: true",
        "connectors.drivers.registry.insecureHosts",
        "connectors.drivers.registry.resolveCacheSeconds: 5",
        "orchestrator.connectorLeases.exchangePort: 8088",
        "connectors.customDrivers.privileged (false)",
    ):
        assert key in out


def test_the_values_keys_are_the_k3d_profile():
    import yaml

    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    connectors = example["connectors"]
    registry = connectors["drivers"]["registry"]
    assert connectors["servicePods"]["enabled"] is True
    assert gate.CLUSTER_REGISTRY in registry["insecureHosts"]
    assert gate.CLUSTER_REGISTRY in registry["privateHosts"]
    assert registry["resolveCacheSeconds"] <= 5
    # The example image must stay a custom driver on k3d.
    assert not connectors.get("customDrivers", {}).get("privileged")
    assert gate.CLUSTER_REGISTRY not in str(
        connectors["drivers"].get("trustedRepositories") or []
    )


@pytest.mark.parametrize("name", sorted(PROGRAMS))
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = PROGRAMS[name]
    compile(program, name, "exec")
    assert "\ncap_memory()\n" in program


def test_the_served_sets_name_every_d6_module_and_exist():
    orchestrator, agent = gate.SERVED_SETS
    assert agent.component == "agent-stateless"
    assert "src/shared/connectors" in agent.dirs
    for path in (
        "src/orchestrator/services/connector_bind_time.py",
        "src/orchestrator/services/connector_bind_time_launch.py",
        "src/orchestrator/services/connector_driver_registrations.py",
        "src/orchestrator/routers/connector_drivers.py",
        "src/orchestrator/database/migrations/app/0420_connector_driver_registrations.sql",
    ):
        assert path in orchestrator.files
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (ROOT / path).is_file(), path
    assert (ROOT / gate.DOCKERFILE).is_file()


def test_the_spec_hash_is_the_one_srw_records():
    from shared.connectors.images import spec_hash

    spec = json.loads(gate.SPEC_FILE.read_text())
    assert gate.spec_hash(spec) == spec_hash(spec)
    assert json.loads(gate.compact(spec)) == spec


def test_the_minted_value_is_the_example_driver_s():
    sys.path.insert(0, str(ROOT / "drivers/example"))
    try:
        import srw_example_driver
    finally:
        sys.path.pop(0)
    assert gate.minted("t", "b-1") == srw_example_driver.minted("t", "b-1")


def test_the_incompatible_label_is_refused_by_srw_s_own_check():
    from orchestrator.services.connector_service_images import config_errors
    from shared.connectors.images import (
        SpecContract,
        compatibility_problems,
        refusal_message,
    )

    spec = json.loads(gate.SPEC_FILE.read_text())
    moved = gate.incompatible(spec)
    problems = compatibility_problems(
        SpecContract.of_label(spec),
        SpecContract.of_label(moved),
        config_errors=config_errors(
            moved["config_schema"], {"variable": "X", "file": "~/.srw-files/x"}
        ),
    )
    message = refusal_message("srw-registry:5000/srw-driver-example:d6-x", problems)
    row = {"status": "failed", "error_message": message, "image_digest": DIGEST}
    assert gate.refusal_problems(row, digest=DIGEST) == []
    assert gate.refusal_problems({**row, "image_digest": None}, digest=DIGEST)
    assert gate.refusal_problems({**row, "status": "bound"}, digest=DIGEST)


def _launched_pod() -> dict:
    from orchestrator.services.connector_bind_time_launch import (
        BindTimePod,
        build_bind_time_launch,
    )
    from orchestrator.services.connector_egress import EgressPins
    from orchestrator.services.connector_service_launch import ServiceLaunchPolicy

    plan = build_bind_time_launch(
        BindTimePod(
            operation_id=OPERATION,
            operation="bind",
            driver=gate.DRIVER,
            digest=DIGEST,
            connector_id=CONNECTOR,
        ),
        request={"protocol_version": "1.0", "operation": "bind"},
        image=f"srw-registry:5000/srw-driver-example@{DIGEST}",
        entrypoint=["python3", "/driver/srw_example_driver.py"],
        cmd=[],
        identity_token="sdi_" + "A" * 49,
        pins=EgressPins(hosts=(), resolved_at=datetime.now(timezone.utc)),
        policy=ServiceLaunchPolicy(
            namespace="srw-superhuman-remote-worker-connectors",
            release_namespace="srw",
            shim_image="srw-registry:5000/srw-driver-shim@sha256:" + "cd" * 32,
            exchange_host="srw-orchestrator.srw.svc",
            exchange_address="10.43.0.20",
            exchange_port=8088,
            orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
        ),
        deadline_seconds=120,
    )
    return json.loads(json.dumps(plan.pod))


def test_the_pod_evaluator_accepts_what_the_launch_builder_makes():
    pod = _launched_pod()
    assert (
        gate.pod_problems(
            pod, digest=DIGEST, namespace="srw-superhuman-remote-worker-connectors"
        )
        == []
    )


@pytest.mark.parametrize(
    ("mutate", "needle"),
    [
        (lambda p: p["spec"].update(restartPolicy="Always"), "restartPolicy"),
        (lambda p: p["spec"].pop("activeDeadlineSeconds"), "activeDeadlineSeconds"),
        (lambda p: p["spec"].update(automountServiceAccountToken=True), "token"),
        (lambda p: p["spec"].update(hostNetwork=True), "hostNetwork"),
        (
            lambda p: p["spec"]["containers"][0]["securityContext"].update(
                privileged=True
            ),
            "escalate",
        ),
        (
            lambda p: p["spec"]["containers"][0]["securityContext"][
                "capabilities"
            ].update(add=["NET_ADMIN"]),
            "adds capabilities",
        ),
        (
            lambda p: p["spec"]["containers"][0].update(
                envFrom=[{"secretRef": {"name": "srw-secrets"}}]
            ),
            "envFrom",
        ),
        (
            lambda p: p["spec"]["volumes"].append(
                {"name": "app", "secret": {"secretName": "srw-secrets"}}
            ),
            "secrets mounted",
        ),
        (
            lambda p: p["spec"]["containers"][0].update(
                image="srw-registry:5000/srw-driver-example:latest"
            ),
            "digest",
        ),
        (lambda p: p["spec"].update(securityContext={}), "seccomp"),
    ],
)
def test_the_pod_evaluator_refuses_what_the_gate_must_not_accept(mutate, needle):
    pod = _launched_pod()
    mutate(pod)
    problems = gate.pod_problems(
        pod, digest=DIGEST, namespace="srw-superhuman-remote-worker-connectors"
    )
    assert any(needle in problem for problem in problems), problems


def test_the_process_verdict():
    good = gate.process_facts(
        "uid=10001 capeff=0000000000000000 capbnd=0000000000000000 "
        "capprm=0000000000000000 nonewprivs=1 seccomp=2"
    )
    assert gate.unprivileged_process(good) == []
    root = {**good, "uid": "0"}
    assert gate.unprivileged_process(root)
    capable = {**good, "capeff": "00000000a80425fb"}
    assert any("capeff" in p for p in gate.unprivileged_process(capable))
    assert gate.unprivileged_process({**good, "nonewprivs": "0"})
    assert gate.unprivileged_process({**good, "seccomp": "0"})
    assert gate.unprivileged_process({})


def test_the_example_driver_reports_its_process_facts():
    """What the gate reads in the workspace is what the driver reports."""
    sys.path.insert(0, str(ROOT / "drivers/example"))
    try:
        import srw_example_driver
    finally:
        sys.path.pop(0)
    facts = gate.process_facts(srw_example_driver.process_facts())
    assert set(facts) == {"uid", "capeff", "capbnd", "capprm", "nonewprivs", "seccomp"}


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch):
    seen: list[tuple[list[str], str | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input")))
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps({"status": 200, "body": "{}"}) + "\n", stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.CustomDriverGate(_args("--password", "s3cret-pw"))
    run.owner.client_id = "client"
    run.owner.call("GET", "/api/connector-drivers")
    assert seen
    for argv, _stdin in seen:
        joined = " ".join(argv)
        assert "s3cret-pw" not in joined and run.token not in joined
    assert any("s3cret-pw" in (stdin or "") for _argv, stdin in seen)
    assert gate._scrub(f"x {run.token} y") == "x <redacted> y"
    assert gate._scrub("x s3cret-pw y") == "x <redacted> y"


def test_the_default_deny_probe_verdict_is_its_last_rounds():
    raced = "\n".join(["canary=open"] * 3 + ["canary=closed"] * 9)
    assert gate.parse_denyprobe(raced) is True
    unenforced = "\n".join(["canary=closed"] * 2 + ["canary=open"] * 10)
    assert gate.parse_denyprobe(unenforced) is False
