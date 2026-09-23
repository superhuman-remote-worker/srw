"""The A1 host wrapper owns only its exact disposable quota and evidence."""

from __future__ import annotations

import argparse
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/vm-retained-resume-gate.py"
spec = importlib.util.spec_from_file_location("vm_retained_resume_gate", SCRIPT)
assert spec and spec.loader
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def args(tmp_path: Path, *, cleanup_only: bool = False) -> argparse.Namespace:
    token = tmp_path / "token"
    token.write_text("a" * 32)
    token.chmod(0o600)
    return argparse.Namespace(
        execute=not cleanup_only, cleanup_only=cleanup_only,
        run_id="srw-a1-owned-20260923", job_id=str(uuid4()),
        expected_owner_id=str(uuid4()),
        expected_pvc_uid=str(uuid4()), context="srw-a1-disposable",
        cluster_uid=str(uuid4()), namespace="srw", vm_namespace="srw",
        orchestrator_deploy="srw-orchestrator", orchestrator_pod="srw-orchestrator-abc",
        orchestrator_pod_uid=str(uuid4()), provider_namespace="srw-a1-provider",
        provider_pod="provider-a1", provider_pod_uid=str(uuid4()),
        provider_control_token_file=token, protocol_version=1,
        confirm=gate.CONFIRMATION, output=tmp_path / "private" / "result.json",
    )


def test_host_guard_requires_exclusive_context_private_output_and_exact_ids(tmp_path):
    value = args(tmp_path)
    gate.require_host_guard(value)
    for key, invalid in (
        ("context", "production"), ("vm_namespace", "shared-vms"),
        ("cluster_uid", "not-a-uuid"), ("orchestrator_pod_uid", "bad"),
        ("protocol_version", 2), ("confirm", "yes"),
    ):
        with pytest.raises(gate.GateFailure):
            gate.require_host_guard(argparse.Namespace(**{**vars(value), key: invalid}))
    value.provider_control_token_file.chmod(0o644)
    with pytest.raises(gate.GateFailure):
        gate.require_host_guard(value)


def test_quota_manifest_and_result_refuse_other_run_or_synthetic_provider(tmp_path):
    value = args(tmp_path)
    body = gate.quota_body(value)
    assert body["spec"]["hard"] == {"count/virtualmachines.kubevirt.io": "0"}
    assert gate._quota_matches(body, value)
    assert not gate._quota_matches({**body, "metadata": {**body["metadata"], "uid": str(uuid4()),
                                                 "labels": {"srw.io/a1-gate-run": "other"}}}, value)
    state = {"run_id": value.run_id, "scenario": "retained-sentinel-worker",
             "sentinel_sha256": "a" * 64, "worker_job_tool_steps": 11,
             "unexpected_count": 0, "pending_calls": 0}
    assert gate.validate_provider_result(state, run_id=value.run_id,
                                         sentinel_sha256="a" * 64)["worker_job_tool_steps"] == 11
    with pytest.raises(gate.GateFailure):
        gate.validate_provider_result({**state, "worker_job_tool_steps": 7},
                                      run_id=value.run_id, sentinel_sha256="a" * 64)
    with pytest.raises(gate.GateFailure):
        gate.validate_provider_result({**state, "sentinel_sha256": "b" * 64},
                                      run_id=value.run_id, sentinel_sha256="a" * 64)


def test_quota_deletion_uses_exact_uid_resource_version(tmp_path, monkeypatch):
    from kubernetes import client

    value = args(tmp_path)
    uid = str(uuid4())
    observed = SimpleNamespace(metadata=SimpleNamespace(uid=uid, resource_version="17"))
    observed.to_dict = lambda: {
        **gate.quota_body(value), "metadata": {**gate.quota_body(value)["metadata"],
                                                "uid": uid, "resource_version": "17"}}
    class Core:
        def __init__(self):
            self.deleted = None
        def read_namespaced_resource_quota(self, name, namespace):
            assert name == gate.quota_name(value.run_id) and namespace == value.vm_namespace
            return observed
        def delete_namespaced_resource_quota(self, name, namespace, *, body):
            self.deleted = body
    core = Core()
    assert gate.remove_quota(core, value, uid)
    assert isinstance(core.deleted, client.V1DeleteOptions)
    assert core.deleted.preconditions.uid == uid
    assert core.deleted.preconditions.resource_version == "17"
    with pytest.raises(gate.GateFailure):
        gate.remove_quota(core, value, str(uuid4()))


def test_quota_active_proof_requires_exact_status_and_uid(tmp_path, monkeypatch):
    value = args(tmp_path)
    uid = str(uuid4())
    class Quota:
        def __init__(self, quota_uid, *, active):
            self.metadata = SimpleNamespace(uid=quota_uid)
            self.active = active
        def to_dict(self):
            return {**gate.quota_body(value),
                    "status": {"hard": {"count/virtualmachines.kubevirt.io": "0"},
                               "used": {"count/virtualmachines.kubevirt.io":
                                        "0" if self.active else "1"}}}
    class Core:
        def __init__(self, quota):
            self.quota = quota
        def read_namespaced_resource_quota(self, *_):
            return self.quota
    gate.wait_quota_active(Core(Quota(uid, active=True)), value, uid)
    with pytest.raises(gate.GateFailure):
        gate.wait_quota_active(Core(Quota(str(uuid4()), active=True)), value, uid)
    ticks = iter([0.0, 31.0])
    monkeypatch.setattr(gate.time, "monotonic", lambda: next(ticks))
    with pytest.raises(gate.GateFailure):
        gate.wait_quota_active(Core(Quota(uid, active=False)), value, uid)


def test_host_phases_keep_quota_after_real_cleanup_and_remove_before_worker_proof(
    tmp_path, monkeypatch,
):
    from kubernetes import client, config

    value = args(tmp_path)
    events = []
    quota_uid = str(uuid4())

    class Process:
        def __init__(self, lines):
            self.stdout = io.StringIO(lines)
            self.stdin = io.StringIO()
            self.returncode = 0

        def poll(self):
            return 0 if self.stdout.tell() == len(self.stdout.getvalue()) else None

    process = Process(
        "SRW_A1_ARM:" + "a" * 64 + "\n"
        "SRW_A1_QUOTA_INSTALL:" + str(uuid4()) + "\n"
        "SRW_A1_QUOTA_RELEASE:" + str(uuid4()) + "\n"
        "SRW_A1_PROVIDER_VERIFY:" + "a" * 64 + "\n"
    )
    monkeypatch.setattr(config, "load_kube_config", lambda **_: None)
    monkeypatch.setattr(client, "CoreV1Api", lambda: object())
    monkeypatch.setattr(gate, "verify_context", lambda _: {"namespace_uid": str(uuid4())})
    monkeypatch.setattr(gate, "_read_json", lambda _, *parts: {
        "metadata": {"uid": value.orchestrator_pod_uid,
                     "labels": {"app.kubernetes.io/component": "orchestrator"},
                     "ownerReferences": [{"kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"}]},
    } if parts[-2:] == ("pod", value.orchestrator_pod) else {
        "metadata": {"uid": "rs-uid", "ownerReferences": [{"kind": "Deployment",
                      "name": value.orchestrator_deploy, "uid": "deploy-uid"}]},
    } if parts[-2:] == ("replicaset", "rs") else {
        "metadata": {"uid": "deploy-uid"}, "spec": {"selector": {"matchLabels": {}}},
    })
    monkeypatch.setattr(gate, "_port_forward", lambda *_: (None, 12345))
    monkeypatch.setattr(gate, "_provider_request", lambda _, __, method, path, body=None: (
        events.append((method, path)) or {
            "scenario": "retained-sentinel-worker", "sentinel_sha256": "a" * 64,
            "run_id": value.run_id, "worker_job_tool_steps": 11,
            "unexpected_count": 0, "pending_calls": 0,
        }
    ))
    monkeypatch.setattr(gate.subprocess, "Popen", lambda *_, **__: process)
    monkeypatch.setattr(gate.select, "select", lambda files, *_: (files, [], []))
    monkeypatch.setattr(gate, "install_quota", lambda *_: (
        events.append("install") or quota_uid, "1"
    ))
    monkeypatch.setattr(gate, "wait_quota_active", lambda *_: events.append("active"))
    monkeypatch.setattr(gate, "remove_quota", lambda *_: events.append("remove") or True)
    monkeypatch.setattr(gate, "wait_quota_absent", lambda *_: events.append("absent"))
    monkeypatch.setattr(gate, "_image_output", lambda _: {
        "run_id": value.run_id, "job_id": value.job_id,
        "owner_id": value.expected_owner_id,
        "cluster_uid": value.cluster_uid, "outcome": "passed",
        "sentinel_sha256": "a" * 64, "replacement_request_id": str(uuid4()),
    })
    result = gate.run_gate(value)
    assert result["host"]["quota_released"] is True
    assert events.index(("POST", f"/control/scenarios/{value.run_id}/arm")) < events.index("install")
    assert events.index("active") < events.index("remove") < events.index("absent")
    assert events.index("absent") < events.index(("GET", f"/control/scenarios/{value.run_id}"))
    assert process.stdin.getvalue().splitlines() == [
        "SRW_A1_ACK:ARM", "SRW_A1_ACK:QUOTA_INSTALL",
        "SRW_A1_ACK:QUOTA_RELEASE", "SRW_A1_ACK:PROVIDER_VERIFY",
    ]
    assert value.output.stat().st_mode & 0o077 == 0


def test_wrong_first_host_phase_cannot_install_quota(tmp_path, monkeypatch):
    value = args(tmp_path)
    installed = []
    monkeypatch.setattr(gate, "verify_context", lambda _: {})
    monkeypatch.setattr(gate, "_read_json", lambda _, *parts: {
        "metadata": {"uid": value.orchestrator_pod_uid,
                     "labels": {"app.kubernetes.io/component": "orchestrator"},
                     "ownerReferences": [{"kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"}]},
    } if parts[-2:] == ("pod", value.orchestrator_pod) else {
        "metadata": {"uid": "rs-uid", "ownerReferences": [{"kind": "Deployment",
                      "name": value.orchestrator_deploy, "uid": "deploy-uid"}]},
    } if parts[-2:] == ("replicaset", "rs") else {
        "metadata": {"uid": "deploy-uid"}, "spec": {"selector": {"matchLabels": {}}},
    })
    class Process:
        def __init__(self):
            self.stdout = io.StringIO("SRW_A1_QUOTA_INSTALL:" + str(uuid4()) + "\n")
            self.stdin = io.StringIO()
            self.terminated = False
        def poll(self):
            return 0 if self.terminated else None
        def terminate(self):
            self.terminated = True
        def wait(self, **_):
            return 0
    process = Process()
    from kubernetes import client, config
    monkeypatch.setattr(config, "load_kube_config", lambda **_: None)
    monkeypatch.setattr(client, "CoreV1Api", lambda: object())
    monkeypatch.setattr(gate, "_port_forward", lambda *_: (None, 12345))
    monkeypatch.setattr(gate.subprocess, "Popen", lambda *_, **__: process)
    monkeypatch.setattr(gate.select, "select", lambda files, *_: (files, [], []))
    monkeypatch.setattr(gate, "install_quota", lambda *_: installed.append(True))
    with pytest.raises(gate.GateFailure, match="phase order"):
        gate.run_gate(value)
    assert installed == []
    assert process.terminated


def test_cleanup_only_removes_exact_prior_quota_before_owner_route(tmp_path, monkeypatch):
    from kubernetes import client, config

    value = args(tmp_path, cleanup_only=True)
    uid = str(uuid4())
    events = []
    monkeypatch.setattr(config, "load_kube_config", lambda **_: None)
    monkeypatch.setattr(client, "CoreV1Api", lambda: object())
    monkeypatch.setattr(gate, "verify_context", lambda _: {})
    monkeypatch.setattr(gate, "_read_json", lambda _, *parts: {
        "metadata": {"uid": value.orchestrator_pod_uid,
                     "labels": {"app.kubernetes.io/component": "orchestrator"},
                     "ownerReferences": [{"kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"}]},
    } if parts[-2:] == ("pod", value.orchestrator_pod) else {
        "metadata": {"uid": "rs-uid", "ownerReferences": [{"kind": "Deployment",
                      "name": value.orchestrator_deploy, "uid": "deploy-uid"}]},
    } if parts[-2:] == ("replicaset", "rs") else {
        "metadata": {"uid": "deploy-uid"}, "spec": {"selector": {"matchLabels": {}}},
    })
    monkeypatch.setattr(gate, "existing_owned_quota_uid", lambda *_: uid)
    monkeypatch.setattr(gate, "remove_quota", lambda *_, **__: events.append("quota_delete") or True)
    monkeypatch.setattr(gate, "wait_quota_absent", lambda *_: events.append("quota_absent"))

    class Process:
        def __init__(self):
            self.stdout = io.StringIO("")
            self.returncode = 0
        def poll(self):
            return 0

    monkeypatch.setattr(gate.subprocess, "Popen", lambda *_, **__: events.append("owner_route") or Process())
    monkeypatch.setattr(gate, "_image_output", lambda _: {
        "run_id": value.run_id, "job_id": value.job_id,
        "owner_id": value.expected_owner_id,
        "cluster_uid": value.cluster_uid, "outcome": "cleanup_requested",
    })
    result = gate.run_gate(value)
    assert events == ["quota_delete", "quota_absent", "owner_route"]
    assert result["host"]["quota_uid"] == uid
    assert result["host"]["quota_released"] is True


def test_interrupted_quota_cleanup_refuses_changed_run_identity(tmp_path):
    value = args(tmp_path, cleanup_only=True)
    observed = SimpleNamespace(metadata=SimpleNamespace(uid=str(uuid4())))
    observed.to_dict = lambda: {
        **gate.quota_body(value),
        "metadata": {**gate.quota_body(value)["metadata"],
                     "labels": {"srw.io/a1-gate-run": "different"}},
    }
    core = SimpleNamespace(read_namespaced_resource_quota=lambda *_: observed)
    with pytest.raises(gate.GateFailure, match="changed object"):
        gate.existing_owned_quota_uid(core, value)
