"""Safety contract for the local D1d kubeconfig connector gate (never run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from agent.connectors import deliveries_from_payload
from agent.connectors.files import plan_credential_files
from orchestrator.security.credential_files import (
    normalize_credential_files,
    slugify_datasource_name,
)

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts" / "k3d-kubeconfig-connector-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_kubeconfig_connector_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

GATE_ID = "d1d-0123456789"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _args(**overrides):
    args = gate.build_parser().parse_args(["--gate-id", GATE_ID])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d1a-0123456789"],
        ["--gate-id", "d1d-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--turn-timeout", "5"],
        ["--job-timeout", "7200"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "srw-gate-",
        "read-only ServiceAccount",
        "NetworkPolicy",
        "session",
        "job",
        "kubectl",
        "cleanup",
    ):
        assert phase in out


def test_every_command_targets_the_local_context():
    assert gate.KUBE == ["kubectl", "--context=k3d-srw"]
    assert gate.K[:2] == ["kubectl", "--context=k3d-srw"]


def test_the_hash_program_hashes_what_it_finds(tmp_path):
    (tmp_path / "a.py").write_bytes(b"print(1)\n")
    out = subprocess.run(
        [sys.executable, "-c", gate._HASH_PROGRAM],
        input=json.dumps({"root": str(tmp_path), "paths": ["a.py", "gone.py"]}),
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    import hashlib

    assert json.loads(out) == {
        "a.py": hashlib.sha256(b"print(1)\n").hexdigest(),
        "gone.py": None,
    }


def test_the_served_modules_exist_in_this_checkout():
    for path in (*gate.AGENT_FILES, *gate.ORCHESTRATOR_FILES):
        assert (_ROOT / path).is_file(), path


def test_a_pinned_pod_is_checked_for_the_workspace_programs_too():
    runner = gate.KubeconfigConnectorGate(_args())
    files = set(runner.pinned_served.files)
    assert {
        "src/shared/runtime/core/credential_env.py",
        "src/shared/runtime/core/backends/remote.py",
    } <= files
    assert set(gate.base.PINNED_AGENT.files) <= files
    assert runner.pinned_served.dirs == gate.base.PINNED_AGENT.dirs
    for path in gate.base.expected_bytes(runner.pinned_served):
        assert (_ROOT / path).is_file(), path


def test_the_gates_generic_file_lands_on_the_allowlist():
    """The connector the gate creates must pass the save-time rule."""
    from orchestrator.security.credential_files import normalize_credential_files

    runner = gate.KubeconfigConnectorGate(_args())
    out = normalize_credential_files(
        "generic_file",
        runner.name("file"),
        {
            "files": [
                {
                    "contents": "x",
                    "target_path": f"~/.srw-files/d1d/{runner.suffix}.json",
                    "env_var": runner.file_var,
                }
            ]
        },
    )
    assert out["files"][0]["env_var"] == runner.file_var


def test_the_scratch_account_may_only_read_in_its_namespace():
    namespace = f"srw-gate-{GATE_ID}"
    manifests = gate.scratch_manifests(namespace, GATE_ID, "marker")
    kinds = [item["kind"] for item in manifests]
    assert kinds == [
        "Namespace",
        "ServiceAccount",
        "Role",
        "RoleBinding",
        "ConfigMap",
    ]
    for item in manifests:
        assert item["metadata"]["labels"] == {gate.GATE_LABEL: GATE_ID}
        if item["kind"] != "Namespace":
            assert item["metadata"]["namespace"] == namespace
    (role,) = [item for item in manifests if item["kind"] == "Role"]
    assert [rule["verbs"] for rule in role["rules"]] == [["get", "list"]]
    assert "ClusterRole" not in json.dumps(manifests)
    (binding,) = [item for item in manifests if item["kind"] == "RoleBinding"]
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": "reader", "namespace": namespace}
    ]


def test_the_egress_hole_selects_one_units_workspace_only():
    policy = gate.egress_policy(
        "srw-gate-x-job",
        GATE_ID,
        {"srw/job-id": "job-1"},
        "10.43.0.1",
        [("172.18.0.2", 6443)],
    )
    assert policy["metadata"]["namespace"] == "srw"
    assert policy["spec"]["podSelector"] == {
        "matchLabels": {"srw.io/component": "agent-workspace", "srw/job-id": "job-1"}
    }
    assert policy["spec"]["policyTypes"] == ["Egress"]
    rules = policy["spec"]["egress"]
    assert [rule["to"][0]["ipBlock"]["cidr"] for rule in rules] == [
        "10.43.0.1/32",
        "172.18.0.2/32",
    ]
    assert [rule["ports"] for rule in rules] == [
        [{"protocol": "TCP", "port": 443}],
        [{"protocol": "TCP", "port": 6443}],
    ]


def test_the_kubeconfig_reaches_the_shell_under_the_names_the_gate_checks():
    """The gate's expected paths and context are the product's own."""
    runner = gate.KubeconfigConnectorGate(_args())
    assert runner.job is None and callable(runner.run_job)
    assert runner.namespace == f"srw-gate-{GATE_ID}"
    name = runner.name("kubeconfig")
    assert runner.kube_slug == slugify_datasource_name(name)
    text = gate.kubeconfig_yaml(
        gate.API_SERVER, "Q0E=", "token-value", runner.namespace
    )
    credentials = normalize_credential_files(
        "kubeconfig", name, {"files": [{"contents": text}]}
    )
    plan = plan_credential_files(
        deliveries_from_payload(
            [{"type": "kubeconfig", "name": name, "credentials": credentials}]
        )
    )
    assert plan.files[0]["link"] == f".kube/configs/{runner.kube_slug}.yaml"
    merged = yaml.safe_load(plan.files[-1]["content"])
    assert merged["current-context"] == f"{runner.kube_slug}-scratch"
    (context,) = merged["contexts"]
    assert context["context"]["namespace"] == runner.namespace
    assert merged["users"][0]["user"] == {"token": "token-value"}


# -- the live attach/detach on a pinned session ---------------------------------


def _live_runner(monkeypatch, *, attach=None, detach=None):
    runner = gate.KubeconfigConnectorGate(_args())
    runner.connectors = {"kubeconfig": "ds-kube", "file": "ds-file"}
    names = sorted(runner.name(label) for label in ("kubeconfig", "file"))
    order: list[str] = []
    monkeypatch.setattr(runner, "check_pinned_pool", lambda: order.append("pool"))

    def session():
        order.append("session")
        runner.live_thread = "00000000-0000-4000-8000-0000000000cc"

    monkeypatch.setattr(runner, "live_session", session)
    monkeypatch.setattr(
        runner,
        "open_egress",
        lambda unit, selector: order.append(f"egress {unit} {selector}"),
    )
    answers = {
        "attach": attach
        or {"outcome": "config.changed", "datasources": {"added": names}},
        "detach": detach
        or {"outcome": "config.changed", "datasources": {"removed": names}},
    }

    def update(ids, label):
        order.append(f"update {label} {ids}")
        return answers[label]

    monkeypatch.setattr(runner, "live_update", update)
    monkeypatch.setattr(runner, "live_workspace", lambda: "ws-thread-pod")
    monkeypatch.setattr(
        runner, "workspace_checks", lambda label, pod: order.append(f"{label} {pod}")
    )
    monkeypatch.setattr(
        runner, "live_detached_checks", lambda pod: order.append(f"detached {pod}")
    )
    monkeypatch.setattr(
        runner, "delete_thread", lambda thread: order.append("delete") or True
    )
    return runner, order


def test_the_live_phase_attaches_then_detaches_both_connectors(monkeypatch):
    runner, order = _live_runner(monkeypatch)
    runner.live_phase()
    assert order == [
        "pool",
        "session",
        "egress live {'srw/thread-id': '00000000-0000-4000-8000-0000000000cc'}",
        "update attach ['ds-kube', 'ds-file']",
        "live attach ws-thread-pod",
        "update detach []",
        "detached ws-thread-pod",
        "delete",
    ]
    assert runner.report.passed, runner.report.results


@pytest.mark.parametrize(
    "fault",
    [
        {"attach": {"outcome": "error", "message": "rejected"}},
        {"detach": {"outcome": "config.changed", "datasources": {"removed": []}}},
    ],
)
def test_a_live_update_that_did_not_land_fails(monkeypatch, fault):
    runner, _order = _live_runner(monkeypatch, **fault)
    runner.live_phase()
    assert not runner.report.passed


@pytest.mark.parametrize(
    ("out", "passed"),
    [
        (
            "kubelink=no\nfilelink=no\nstore=no\nkubeconfig=\nvar=\n"
            "marker=error: no configuration has been provided\n",
            True,
        ),
        (
            "kubelink=yes\nfilelink=no\nstore=no\nkubeconfig=\nvar=\nmarker=x\n",
            False,
        ),
        (
            "kubelink=no\nfilelink=no\nstore=yes\nkubeconfig=\nvar=\nmarker=x\n",
            False,
        ),
        (
            "kubelink=no\nfilelink=no\nstore=no\n"
            "kubeconfig=/home/agent-host/.srw-credentials/files-x/kubeconfig\n"
            "var=\nmarker=x\n",
            False,
        ),
    ],
)
def test_the_detached_workspace_is_read_from_one_script(monkeypatch, out, passed):
    runner = gate.KubeconfigConnectorGate(_args())
    monkeypatch.setattr(runner, "ws", lambda pod, script, check=True: (0, out))
    runner.live_detached_checks("ws-thread-pod")
    assert runner.report.passed is passed


def test_skip_live_skips_only_the_live_phase(monkeypatch):
    for argv, expected in (([], True), (["--skip-live"], False)):
        args = gate.build_parser().parse_args(["--gate-id", GATE_ID, *argv])
        runner = gate.KubeconfigConnectorGate(args)
        ran: list[str] = []
        for phase in ("preflight", "fixture", "session", "run_job"):
            monkeypatch.setattr(runner, phase, lambda: None)
        monkeypatch.setattr(runner, "live_phase", lambda: ran.append("live"))
        monkeypatch.setattr(runner, "cleanup", lambda: [])
        monkeypatch.setattr(runner, "residue", lambda: [])
        runner.run()
        assert (ran == ["live"]) is expected


def test_cleanup_deletes_the_live_session_too(monkeypatch):
    runner = gate.KubeconfigConnectorGate(_args())
    runner.live_thread = "00000000-0000-4000-8000-0000000000cc"
    deleted: list[str] = []
    monkeypatch.setattr(runner, "titled_threads", lambda: [runner.live_thread])
    monkeypatch.setattr(
        runner, "delete_thread", lambda thread: deleted.append(thread) or True
    )
    assert runner.cleanup() == []
    assert deleted == [runner.live_thread]
