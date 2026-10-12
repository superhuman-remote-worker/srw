"""Safety contract and evaluators of the local D5b managed MCP stdio gate
(never run here)."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-managed-mcp-stdio-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_managed_mcp_stdio_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

FRONT = "srw-registry:5000/srw-driver-mcp-front@sha256:" + "cd" * 32


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
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d5a-0123456789"],
        ["--gate-id", "d5b-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
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
        "processes",
        "isolation",
        "credential",
        "denied",
        "workspace",
        "readonly",
        "ending",
        "cleanup",
    ):
        assert f"- {phase}:" in out
    for key in (
        "connectors.servicePods.enabled: true",
        "connectors.drivers.mcpStdioTest.enabled: true",
        "connectors.drivers.mcpStdioProbe.enabled: true",
        "connectors.drivers.mcpTest:",
        "connectors.drivers.mcpFront.image",
        "orchestrator.connectorLeases.exchangePort: 8088",
    ):
        assert key in out


def test_the_values_keys_are_the_k3d_profile_and_the_chart_pins_the_image():
    import yaml

    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    drivers = example["connectors"]["drivers"]
    assert example["connectors"]["servicePods"]["enabled"] is True
    assert example["connectors"]["servicePods"]["maxInstallation"] >= 3
    assert drivers["mcpStdioTest"]["enabled"] is True
    assert drivers["mcpStdioProbe"]["enabled"] is True
    assert drivers["mcpTest"]["enabled"] is True
    assert drivers["mcpFront"]["image"]["repository"].endswith("srw-driver-mcp-front")
    chart = yaml.safe_load((ROOT / "helm/values.yaml").read_text())
    image = chart["connectors"]["drivers"]["mcpStdioTest"]["image"]
    assert image["repository"] == gate.STOCK_IMAGE
    assert image["digest"] == gate.STOCK_DIGEST


def test_embedded_programs_compile_and_cap_their_memory():
    compile(gate._RAW_PROGRAM, "raw", "exec")
    assert "cap_memory()" in gate._RAW_PROGRAM
    assert gate._RAW_PROGRAM.index("def cap_memory") < gate._RAW_PROGRAM.index(
        "cap_memory()"
    )


def test_the_gate_builds_on_the_d5a_gate_and_its_spec():
    from shared.connectors.builtin import MCP_STDIO_PROBE_SPEC, MCP_STDIO_TEST_SPEC
    from shared.connectors.mcp import (
        BINDING_HOME_ROOT,
        BINDING_UID_BASE,
        BRIDGE_CAPABILITIES,
        BRIDGE_PATH,
        BRIDGE_SOCKET,
        managed_mcp,
    )

    assert issubclass(gate.StdioGate, gate.base.ManagedMcpGate)
    mcp = managed_mcp(MCP_STDIO_TEST_SPEC)
    assert gate.STDIO_DRIVER == MCP_STDIO_TEST_SPEC.name
    assert gate.STDIO_TYPE == MCP_STDIO_TEST_SPEC.legacy_type
    assert gate.TOKEN_ENV == mcp.credential_env
    assert gate.BRIDGE == BRIDGE_PATH
    assert gate.BRIDGE_SOCKET == BRIDGE_SOCKET
    assert (gate.UID_BASE, gate.HOME_ROOT) == (BINDING_UID_BASE, BINDING_HOME_ROOT)
    assert gate.BRIDGE_CAPABILITIES == list(BRIDGE_CAPABILITIES)
    assert set(gate.READ_TOOLS) == set(mcp.read_tools)
    probe = managed_mcp(MCP_STDIO_PROBE_SPEC)
    assert gate.PROBE_DRIVER == MCP_STDIO_PROBE_SPEC.name
    assert gate.PROBE_TYPE == MCP_STDIO_PROBE_SPEC.legacy_type
    assert gate.PROBE_PROGRAM == list(probe.command)
    assert gate.PROBE_TOKEN_ENV == probe.credential_env
    # The probe tools the gate calls are read tools of the probe server.
    for tool in ("self_status", "whoami", "probe_path", "probe_signal", "probe_socket"):
        assert probe.allowed(tool, "ReadWrite")


@pytest.mark.skipif(shutil.which("sh") is None, reason="no shell")
def test_the_process_script_reads_a_process_but_never_its_environment():
    child = subprocess.Popen(
        ["sleep", "30"],
        env={"PATH": os.environ.get("PATH", ""), "MCP_STDIO_TEST_TOKEN": "x"},
    )
    try:
        out = subprocess.run(
            ["sh", "-c", gate._PROCESS_SCRIPT, "process", str(child.pid)],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        found = dict(line.split("=", 1) for line in out.splitlines())
        assert found["program"].startswith("sleep 30")
        assert found["parent"] == str(os.getpid())
        assert found["uid"] == str(os.getuid())
        assert found["state"] in ("S", "R")
        assert "environ" not in gate._PROCESS_SCRIPT
    finally:
        child.kill()
        child.wait()
    out = subprocess.run(
        ["sh", "-c", gate._PROCESS_SCRIPT, "process", "999999999"],
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout
    assert out.strip() == "missing" and gate.process_gone(out)


def test_process_verdict():
    good = "state=S\nparent=1\nuid=20001\nprogram=node dist/index.js \n"
    assert gate.process_verdict(good, 20001)[0]
    for bad, uid in (
        (good, 20002),  # another user than the bridge reports
        (good.replace("uid=20001", "uid=0"), 0),  # root
        (good.replace("parent=1", "parent=7"), 20001),
        (good.replace("program=node", "program=sh"), 20001),
        (good.replace("state=S", "state=Z"), 20001),
        ("missing\n", 20001),
    ):
        assert not gate.process_verdict(bad, uid)[0], bad
    assert gate.process_gone("state=Z\nparent=1\n")
    assert not gate.process_gone(good)


def _isolated():
    one = {
        "pid": 41,
        "uid": 20001,
        "gid": 20001,
        "home": "/srw/home/20001",
        "tmpdir": "/srw/home/20001",
        "CapPrm": "0000000000000000",
        "CapEff": "0000000000000000",
        "CapAmb": "0000000000000000",
        "NoNewPrivs": "1",
        "max_processes": "256",
        "max_core": "0",
        "env_names": ["HOME", "MCP_TEST_TOKEN", "PATH", "TMPDIR"],
    }
    two = {"pid": 42, "uid": 20002, "home": "/srw/home/20002"}
    probes = {
        "session two's environment": "refused: open /proc/42/environ: permission denied",
        "session two's directory": "refused: open /srw/home/20002: permission denied",
        "the bridge's socket": "refused: dial unix /srw/bridge/bridge.sock: connect: permission denied",
        "session two's process": "refused: operation not permitted",
    }
    return one, two, probes


def test_isolation_verdict():
    one, two, probes = _isolated()
    digest = "ab" * 32
    whoami = {"credential_sha256": digest}
    assert gate.isolation_verdict(one, two, probes, token_sha256=digest, whoami=whoami)[
        0
    ]
    for change in (
        {"uid": 0},
        {"uid": 20002},  # session two's user
        {"uid": 1000},  # outside the pool
        {"gid": 0},
        {"CapEff": "00000000000000c0"},
        {"CapPrm": "0000000000000080"},
        {"CapAmb": "0000000000000001"},
        {"NoNewPrivs": "0"},
        {"max_processes": "unlimited"},
        {"max_core": "unlimited"},
        {"home": "/root"},
        {"tmpdir": "/tmp"},
        {"env_names": ["HOME", "MCP_TEST_TOKEN", "SRW_REQUEST_FILE"]},
        {"env_names": ["HOME"]},
    ):
        assert not gate.isolation_verdict(
            {**one, **change}, two, probes, token_sha256=digest, whoami=whoami
        )[0], change
    assert not gate.isolation_verdict(
        one, two, probes, token_sha256=digest, whoami={"credential_sha256": "x"}
    )[0]
    for name in probes:
        allowed = {**probes, name: "allowed"}
        assert not gate.isolation_verdict(
            one, two, allowed, token_sha256=digest, whoami=whoami
        )[0], name
    # Refused for another reason than permission (a path that is not there)
    # proves nothing.
    absent = {**probes, "session two's directory": "refused: no such file or directory"}
    assert not gate.isolation_verdict(
        one, two, absent, token_sha256=digest, whoami=whoami
    )[0]


def test_processes_verdict_needs_one_process_per_binding_with_its_credential():
    leases = {"one": "lease-1", "two": "lease-2"}
    status = {
        "processes": [
            {"binding": "", "probe": True, "pid": 5, "credential": False},
            {"binding": "lease-1", "pid": 11, "credential": True},
            {"binding": "lease-2", "pid": 12, "credential": True},
        ]
    }
    ok, pids, _ = gate.processes_verdict(status, leases)
    assert ok and pids == {"one": 11, "two": 12}
    one_process = json.loads(json.dumps(status))
    one_process["processes"][2]["pid"] = 11
    assert not gate.processes_verdict(one_process, leases)[0]
    missing = {"processes": status["processes"][:2]}
    assert not gate.processes_verdict(missing, leases)[0]
    no_credential = json.loads(json.dumps(status))
    no_credential["processes"][1]["credential"] = False
    assert not gate.processes_verdict(no_credential, leases)[0]
    stranger = json.loads(json.dumps(status))
    stranger["processes"].append({"binding": "lease-x", "pid": 13, "credential": True})
    assert not gate.processes_verdict(stranger, leases)[0]


def test_the_corpus_names_this_servers_tools():
    bodies = gate.corpus_bodies("create_entities", "read_graph")
    shared = json.loads(gate.CORPUS.read_text())
    assert len(bodies) == len(shared["bodies"]) + len(shared["bodies_base64"])
    assert not any(b"notes_write" in body or b"whoami" in body for body in bodies)
    assert all(b"create_entities" in body for body in bodies if _utf8(body))
    # The body that is not UTF-8 (a read tool's name with a stray byte)
    # stays bytes, renamed.
    (garbled,) = [body for body in bodies if not _utf8(body)]
    assert b'"read_graph\xff"' in garbled
    control = json.loads(gate.control_body("create_entities", "d5b-x-bypass"))
    assert control["params"]["name"] == "create_entities"
    assert control["params"]["arguments"]["entities"][0]["name"] == "d5b-x-bypass"


def _utf8(body: bytes) -> bool:
    try:
        body.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def _answers(*middle):
    reviewers = [{"status": 400, "type": "application/json", "body": "{}"}] * 6
    control = {
        "status": 200,
        "type": "application/json",
        "body": '{"error":{"code":-32602,"message":"Unknown tool: create_entities"}}',
    }
    return {"initialize": 200, "answers": [*reviewers, *middle, control]}


def test_corpus_verdict():
    read = {
        "status": 200,
        "type": "text/event-stream",
        "body": 'event: message\ndata: {"result":{"content":[{"text":"{\\"entities\\":[]}"}]}}',
    }
    refused = {
        "status": 200,
        "type": "application/json",
        "body": "Unknown tool: create_entities",
    }
    ok, detail = gate.corpus_verdict(
        _answers(read, refused), sent=9, write_tool="create_entities"
    )
    assert ok, detail
    for leaked in (
        # A write that reached the server: its error, or its name in a stream.
        {
            "status": 200,
            "type": "text/event-stream",
            "body": 'data: {"error":{"code":-32603}}',
        },
        {
            "status": 200,
            "type": "text/event-stream",
            "body": "data: create_entities done",
        },
    ):
        assert not gate.corpus_verdict(
            _answers(read, leaked), sent=9, write_tool="create_entities"
        )[0]
    # A reviewer body the front forwarded, a control from the server, or a
    # body never answered.
    forwarded = _answers(read, refused)
    forwarded["answers"][2] = read
    assert not gate.corpus_verdict(forwarded, sent=9, write_tool="create_entities")[0]
    streamed_control = _answers(read, refused)
    streamed_control["answers"][-1] = dict(read, body="Unknown tool")
    assert not gate.corpus_verdict(
        streamed_control, sent=9, write_tool="create_entities"
    )[0]
    assert not gate.corpus_verdict(
        _answers(read, refused), sent=10, write_tool="create_entities"
    )[0]


PROBE_DIGEST = "sha256:" + "12" * 32
PROBE_IMAGE = f"srw-registry:5000/srw-driver-mcp-test@{PROBE_DIGEST}"


def _stdio_pod(probe: bool = False) -> dict:
    """A real launch plan of the stdio test server (or the probe server),
    as the pod the gate reads, with every init container exited 0."""
    from orchestrator.services.connector_egress import EgressPins
    from orchestrator.services.connector_service_launch import (
        ServiceLaunchPolicy,
        ServicePodIdentity,
        build_service_launch,
    )
    from shared.connectors.builtin import MCP_STDIO_PROBE_SPEC, MCP_STDIO_TEST_SPEC

    spec = MCP_STDIO_PROBE_SPEC if probe else MCP_STDIO_TEST_SPEC
    plan = build_service_launch(
        ServicePodIdentity(
            identity_id="11111111-2222-4333-8444-555555555555",
            connector_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
            driver=spec.name,
            digest=PROBE_DIGEST if probe else gate.STOCK_DIGEST,
            generation="hmac-sha256:" + "cd" * 32,
        ),
        spec=spec,
        image=PROBE_IMAGE if probe else f"{gate.STOCK_IMAGE}@{gate.STOCK_DIGEST}",
        entrypoint=["/srw-mcp-test"] if probe else gate.STOCK_PROGRAM,
        cmd=[],
        config={"message": "d5b-0123456789"} if probe else {},
        credentials={"token": "t"},
        identity_token="sdi_" + "A" * 49,
        pins=EgressPins(hosts=(), resolved_at=datetime.now(timezone.utc)),
        policy=ServiceLaunchPolicy(
            namespace="srw-connectors",
            release_namespace="srw",
            shim_image="srw-registry:5000/srw-driver-shim@sha256:" + "ef" * 32,
            exchange_host="srw-orchestrator.srw.svc",
            exchange_address="10.43.0.20",
            exchange_port=8088,
            orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
            front_image=FRONT,
        ),
    )
    pod = json.loads(json.dumps(plan.pod))
    pod["status"] = {
        "initContainerStatuses": [
            {"name": name, "state": {"terminated": {"exitCode": 0}}}
            for name in ("canary-wait", "install-bridge")
        ]
    }
    return pod


def test_the_layout_checks_hold_for_the_probe_pod_srw_launches():
    pod = _stdio_pod(probe=True)
    assert gate.stdio_init_passed(pod)
    check = {
        "image": PROBE_IMAGE,
        "program": gate.PROBE_PROGRAM,
        "token_env": gate.PROBE_TOKEN_ENV,
    }
    assert gate.stdio_layout_problems(pod, FRONT, **check) == []
    # The probe pod is no memory pod.
    assert gate.stdio_layout_problems(pod, FRONT) != []


def test_the_layout_checks_name_a_bridge_that_isolates_nothing():
    pod = _stdio_pod()
    for mutate, fragment in (
        (lambda s, f: s["command"].__setitem__(3, "/tmp/b.sock"), "socket"),
        (lambda s, f: s["command"].__setitem__(7, "0"), "socket"),
        (
            lambda s, f: s["command"].__setitem__(
                s["command"].index("--process-limit") + 1, "4096"
            ),
            "caps no",
        ),
        (
            lambda s, f: s["securityContext"].__setitem__("runAsUser", 1000),
            "securityContext",
        ),
        (
            lambda s, f: s["securityContext"]["capabilities"]["add"].append(
                "SYS_ADMIN"
            ),
            "securityContext",
        ),
        (lambda s, f: s["volumeMounts"].pop(), "emptyDir"),
        (lambda s, f: f["volumeMounts"][-1].pop("readOnly"), "read-only"),
    ):
        changed = json.loads(json.dumps(pod))
        server, front = changed["spec"]["containers"]
        mutate(server, front)
        problems = gate.stdio_layout_problems(changed, FRONT)
        assert any(fragment in p for p in problems), (fragment, problems)


def test_the_layout_checks_hold_for_the_pod_srw_launches():
    pod = _stdio_pod()
    assert gate.stdio_init_passed(pod)
    assert gate.stdio_layout_problems(pod, FRONT) == []
    # Another front image, the bridge left out, or an unpinned server: each
    # is named.
    assert gate.stdio_layout_problems(pod, FRONT.replace("cd", "ab")) != []
    unbridged = json.loads(json.dumps(pod))
    unbridged["spec"]["containers"][0]["command"] = gate.STOCK_PROGRAM
    assert any("command" in p for p in gate.stdio_layout_problems(unbridged, FRONT))
    unpinned = json.loads(json.dumps(pod))
    unpinned["spec"]["containers"][0]["image"] = f"{gate.STOCK_IMAGE}:latest"
    assert gate.stdio_layout_problems(unpinned, FRONT) != []
    failed = json.loads(json.dumps(pod))
    failed["status"]["initContainerStatuses"][1]["state"] = {
        "terminated": {"exitCode": 1}
    }
    assert not gate.stdio_init_passed(failed)


def test_the_raw_program_sends_bytes_it_was_given(tmp_path):
    """The program decodes each body from base64 and sends those bytes
    (the corpus holds a body that is not UTF-8)."""
    bodies = gate.corpus_bodies("create_entities", "read_graph")
    encoded = [base64.b64encode(body).decode() for body in bodies]
    assert [base64.b64decode(e) for e in encoded] == bodies
    assert "base64.b64decode(encoded)" in gate._RAW_PROGRAM
