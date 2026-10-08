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
    assert example["connectors"]["servicePods"]["maxInstallation"] >= 2
    assert drivers["mcpStdioTest"]["enabled"] is True
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
    from shared.connectors.builtin import MCP_STDIO_TEST_SPEC
    from shared.connectors.mcp import BRIDGE_PATH, managed_mcp

    assert issubclass(gate.StdioGate, gate.base.ManagedMcpGate)
    mcp = managed_mcp(MCP_STDIO_TEST_SPEC)
    assert gate.STDIO_DRIVER == MCP_STDIO_TEST_SPEC.name
    assert gate.STDIO_TYPE == MCP_STDIO_TEST_SPEC.legacy_type
    assert gate.TOKEN_ENV == mcp.credential_env
    assert gate.BRIDGE == BRIDGE_PATH
    assert gate.BRIDGE_LISTEN == f"127.0.0.1:{mcp.port}"
    assert set(gate.READ_TOOLS) == set(mcp.read_tools)
    assert gate._ENVIRON_SCRIPT.count("MCP_STDIO_TEST_TOKEN") == 1
    assert "MCP_STDIO_TEST_TOKEN" == mcp.credential_env


@pytest.mark.skipif(shutil.which("sh") is None, reason="no shell")
def test_the_environ_script_reads_a_process_and_takes_the_token_on_stdin():
    token = "d5b-upstream-memory-" + "0" * 32
    child = subprocess.Popen(
        ["sleep", "30"],
        env={
            "PATH": os.environ.get("PATH", ""),
            "MCP_STDIO_TEST_TOKEN": token,
            "SRW_LEAK": "1",
        },
    )
    try:
        out = subprocess.run(
            ["sh", "-c", gate._ENVIRON_SCRIPT, "environ", str(child.pid)],
            input=token + "\n",
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        found = dict(line.split("=", 1) for line in out.splitlines())
        assert found["token"] == "1" and found["srw"] == "1"
        assert found["program"].startswith("sleep 30")
        assert found["parent"] == str(os.getpid())
        # A wrong token is not counted; a process that is gone is missing.
        out = subprocess.run(
            ["sh", "-c", gate._ENVIRON_SCRIPT, "environ", str(child.pid)],
            input="other\n",
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        assert "token=0" in out
    finally:
        child.kill()
        child.wait()
    out = subprocess.run(
        ["sh", "-c", gate._ENVIRON_SCRIPT, "environ", "999999999"],
        input=token + "\n",
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout
    assert out.strip() == "missing"


def test_environ_verdict():
    good = "token=1\nsrw=0\nparent=1\nprogram=node dist/index.js \n"
    assert gate.environ_verdict(good)[0]
    for bad in (
        good.replace("token=1", "token=0"),
        good.replace("srw=0", "srw=2"),
        good.replace("parent=1", "parent=7"),
        good.replace("program=node", "program=sh"),
        "missing\n",
    ):
        assert not gate.environ_verdict(bad)[0], bad


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


def _stdio_pod() -> dict:
    """A real launch plan of the stdio test server, as the pod the gate
    reads, with every init container exited 0."""
    from orchestrator.services.connector_egress import EgressPins
    from orchestrator.services.connector_service_launch import (
        ServiceLaunchPolicy,
        ServicePodIdentity,
        build_service_launch,
    )
    from shared.connectors.builtin import MCP_STDIO_TEST_SPEC

    plan = build_service_launch(
        ServicePodIdentity(
            identity_id="11111111-2222-4333-8444-555555555555",
            connector_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
            driver=MCP_STDIO_TEST_SPEC.name,
            digest=gate.STOCK_DIGEST,
            generation="hmac-sha256:" + "cd" * 32,
        ),
        spec=MCP_STDIO_TEST_SPEC,
        image=f"{gate.STOCK_IMAGE}@{gate.STOCK_DIGEST}",
        entrypoint=gate.STOCK_PROGRAM,
        cmd=[],
        config={},
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
