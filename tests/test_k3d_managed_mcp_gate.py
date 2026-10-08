"""Safety contract and evaluators of the local D5a managed MCP gate (never
run here)."""

from __future__ import annotations

import base64
import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-managed-mcp-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_managed_mcp_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {
    "api": gate._API_PROGRAM,
    "lease": gate._LEASE_PROGRAM,
    "mcp": gate._MCP_PROGRAM,
    "replace": gate._REPLACE_PROGRAM,
    "net": gate._NET_PROGRAM,
    "scan": gate._SCAN_PROGRAM,
    "keycloak": gate._KEYCLOAK_PROGRAM,
    "hash": gate._HASH_PROGRAM,
}
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
DIGEST = "sha256:" + "ab" * 32
FRONT = "srw-registry:5000/srw-driver-mcp-front@sha256:" + "cd" * 32


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
        ["--gate-id", "d5-0123456789"],
        ["--gate-id", "d5a-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--gitea-url", "http://gitea.com"],
        ["--gitea-url", "https://gitea.com/path"],
        ["--gitea-url", "https://user:pw@gitea.com"],
        ["--start-timeout", "5"],
        ["--turn-timeout", "99999"],
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
        "startup",
        "serve",
        "credential",
        "denied",
        "workspace",
        "readonly",
        "replace",
        "cleanup",
    ):
        assert f"- {phase}:" in out
    for key in (
        "connectors.servicePods.enabled: true",
        "connectors.drivers.managedMcp.gitea.enabled: true",
        "connectors.drivers.mcpTest.enabled: true",
        "connectors.drivers.mcpFront.image",
        "orchestrator.connectorLeases.exchangePort: 8088",
    ):
        assert key in out


def test_the_values_keys_are_the_k3d_profile():
    import yaml

    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    drivers = example["connectors"]["drivers"]
    assert example["connectors"]["servicePods"]["enabled"] is True
    assert example["connectors"]["servicePods"]["maxInstallation"] >= 3
    assert drivers["managedMcp"]["gitea"]["enabled"] is True
    assert drivers["mcpTest"]["enabled"] is True
    assert drivers["mcpFront"]["image"]["repository"].endswith("srw-driver-mcp-front")
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


def test_the_served_sets_name_every_d5a_module_and_exist():
    orchestrator, agent = gate.SERVED_SETS
    assert agent.component == "agent-stateless"
    assert "src/agent/tools/mcp/manager.py" in agent.files
    assert "src/shared/connectors" in agent.dirs
    assert "src/orchestrator/services/connector_drivers" in orchestrator.dirs
    for path in (
        "src/orchestrator/services/connector_service_hosting.py",
        "src/orchestrator/services/connector_service_launch.py",
        "src/orchestrator/services/connector_credential_leases.py",
        "src/orchestrator/database/migrations/app/0392_drop_connector_service_pod_key_idx.notx.sql",
    ):
        assert path in orchestrator.files
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (ROOT / path).is_file(), path
    for name in gate.MIGRATIONS:
        assert (ROOT / "src/orchestrator/database/migrations/app" / name).is_file()


def test_the_endpoint_is_the_one_bindings_carry():
    from orchestrator.services.connector_service_launch import endpoint_service_name

    assert gate.endpoint_name(CONNECTOR, DIGEST) == endpoint_service_name(
        CONNECTOR, DIGEST
    )


def _managed_pod(**over) -> dict:
    """A managed MCP pod as the launch builder makes it, as the API shows it."""
    from orchestrator.services.connector_egress import EgressPins
    from orchestrator.services.connector_service_launch import (
        ServiceLaunchPolicy,
        ServicePodIdentity,
        build_service_launch,
    )
    from shared.connectors.builtin import MCP_TEST_SPEC

    plan = build_service_launch(
        ServicePodIdentity(
            identity_id="11111111-2222-4333-8444-555555555555",
            connector_id=CONNECTOR,
            driver=MCP_TEST_SPEC.name,
            digest=DIGEST,
            generation="g",
        ),
        spec=MCP_TEST_SPEC,
        image=f"srw-registry:5000/srw-driver-mcp-test@{DIGEST}",
        entrypoint=["/srw-mcp-test"],
        cmd=[],
        config={},
        credentials={"token": "t"},
        identity_token="sdi_" + "A" * 49,
        pins=EgressPins(hosts=(), resolved_at=datetime.now(timezone.utc)),
        policy=ServiceLaunchPolicy(
            namespace="srw-connectors",
            release_namespace="srw",
            shim_image="r/shim@sha256:" + "ef" * 32,
            exchange_host="srw-orchestrator.srw.svc",
            exchange_address="10.43.0.20",
            exchange_port=8088,
            orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
            front_image=FRONT,
        ),
    )
    pod = json.loads(json.dumps(plan.pod))
    pod["status"] = {
        "phase": "Running",
        "initContainerStatuses": [
            {"name": "canary-wait", "state": {"terminated": {"exitCode": 0}}}
        ],
        "containerStatuses": [
            {"name": "driver", "ready": True},
            {"name": "front", "ready": True},
        ],
    }
    pod.update(over)
    return pod, plan


def test_the_pod_evaluators_accept_what_the_launch_builder_makes():
    pod, plan = _managed_pod()
    assert gate.canary_passed(pod)
    assert gate.pod_ready(pod)
    assert gate.layout_problems(pod, FRONT) == []
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request["credentials"] == {}


def test_the_pod_evaluators_refuse_what_the_gate_must_not_accept():
    pod, _plan = _managed_pod()
    pod["status"]["containerStatuses"][1]["ready"] = False
    assert not gate.pod_ready(pod)
    pod, _plan = _managed_pod()
    pod["spec"]["initContainers"].append({"name": "install-shim"})
    assert not gate.canary_passed(pod)
    pod, _plan = _managed_pod()
    assert gate.layout_problems(pod, "srw-registry:5000/other@sha256:" + "0" * 64)
    server = pod["spec"]["containers"][0]
    server["env"].append({"name": "SRW_DRIVER_IDENTITY_FILE", "value": "/run/srw/x"})
    server["volumeMounts"].append({"name": "delivery", "mountPath": "/run/srw/x"})
    server["securityContext"] = {}
    problems = gate.layout_problems(pod, FRONT)
    assert any("SRW's environment" in p for p in problems)
    assert any("delivery Secret" in p for p in problems)
    assert any("capabilities" in p for p in problems)


def test_the_replace_verdict():
    sha = "f" * 64
    good = {
        "phase": "done",
        "first": "srw-drv-a",
        "answers": [
            {"seconds": 0.1, "pod": "srw-drv-a", "sha": sha, "error": None},
            {"seconds": 21.5, "pod": "srw-drv-b", "sha": sha, "error": None},
        ],
        "reconnects": 1,
        "status": "connected",
    }
    assert gate.replace_verdict(good, sha)[0]
    for change in (
        {"reconnects": 0},
        {"reconnects": 4},
        {"status": "unavailable: x"},
        {"phase": "failed"},
        {"answers": good["answers"][:1]},
        {
            "answers": [
                {"seconds": 1, "pod": None, "sha": None, "error": "MCP tool error"},
                *good["answers"][1:],
            ]
        },
    ):
        assert not gate.replace_verdict({**good, **change}, sha)[0], change
    assert not gate.replace_verdict(good, "0" * 64)[0]


def test_the_readonly_verdict():
    from shared.connectors.builtin import GITEA_MCP_READ_TOOLS

    read_write = sorted({*GITEA_MCP_READ_TOOLS, *gate.GITEA_WRITE_TOOLS})
    refused = {
        "tool": "create_repo",
        "error": "McpError: Unknown tool: create_repo",
    }
    good = {
        "status": "connected",
        "tools": sorted(GITEA_MCP_READ_TOOLS),
        "calls": [refused],
    }
    assert gate.readonly_verdict(good, read_write)[0]
    leaking = {**good, "tools": [*good["tools"], "create_repo"]}
    assert not gate.readonly_verdict(leaking, read_write)[0]
    forwarded = {**good, "calls": [{"tool": "create_repo", "text": "created"}]}
    assert not gate.readonly_verdict(forwarded, read_write)[0]
    assert not gate.readonly_verdict(good, sorted(GITEA_MCP_READ_TOOLS))[0]
    assert not gate.readonly_verdict({**good, "tools": []}, read_write)[0]


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch):
    seen: list[tuple[list[str], str | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input")))
        if "-c" in argv and gate._LEASE_PROGRAM in argv:
            body = json.dumps({"token": "scl_" + "L" * 49})
        elif "-tAc" in argv:
            body = json.dumps({"id": "00000000-0000-4000-8000-000000000001"})
        else:
            body = json.dumps({"found": [], "scanned": {"processes": 1}})
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.ManagedMcpGate(_args("--password", "s3cret-pw"))
    run.threads["one"] = "00000000-0000-4000-8000-0000000000aa"
    run.connectors["notes"] = CONNECTOR
    token = run.lease_token("notes", "one")
    assert token == "scl_" + "L" * 49
    run.scan("agent-0", gate.AGENT_CONTAINER, ["/app"])
    assert seen
    for argv, _stdin in seen:
        joined = " ".join(argv)
        assert "s3cret-pw" not in joined and token not in joined
        assert "d5a-upstream" not in joined
    # The scan's needles travel on stdin; the lease token is scrubbed.
    assert any("d5a-upstream" in (stdin or "") for _argv, stdin in seen)
    assert gate._scrub(f"x {token} y") == "x <redacted> y"
    assert gate._scrub("x s3cret-pw y") == "x <redacted> y"
    for value in run.tokens.values():
        assert gate._scrub(value) == "<redacted>"


def test_the_default_deny_probe_verdict_is_its_last_rounds():
    raced = "\n".join(["canary=open"] * 3 + ["canary=closed"] * 9)
    assert gate.parse_denyprobe(raced) is True
    unenforced = "\n".join(["canary=closed"] * 2 + ["canary=open"] * 10)
    assert gate.parse_denyprobe(unenforced) is False
    with pytest.raises(gate.GateError):
        gate.parse_denyprobe("canary=closed\n" * 3)


def test_the_node_must_lie_in_the_refused_ranges():
    assert gate.node_refused("172.18.0.2", "172.16.0.0/12,10.42.0.0/16")
    assert not gate.node_refused("172.18.0.2", "10.0.50.0/24")
    assert not gate.node_refused("", "172.16.0.0/12")


def test_the_scan_finds_a_secret_and_names_only_its_path(tmp_path):
    """The in-pod scan, run here on a scratch tree: it reports where a
    secret is, never the secret."""
    needle = "d5a-upstream-notes-" + "9" * 32
    (tmp_path / "clean.txt").write_text("nothing here")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "config.json").write_text(json.dumps({"t": needle}))
    out = subprocess.run(
        [sys.executable, "-c", gate._SCAN_PROGRAM],
        input=json.dumps({"secrets": [needle], "roots": [str(tmp_path)]}) + "\n",
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    result = json.loads(out.splitlines()[-1])
    assert result["found"] == [str(tmp_path / "nested" / "config.json")]
    assert needle not in out
