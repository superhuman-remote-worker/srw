"""Safety and evidence contracts for the disposable VM recovery gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/vm-workspace-recovery-k3d-gate.py"
DRIVER = ROOT / "scripts/vm-workspace-recovery-scenario.py"
SPEC = importlib.util.spec_from_file_location("vm_workspace_recovery_gate", SCRIPT)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def _passing_evidence() -> dict:
    return {
        "authority_evidence": {
            "application_api": {"status_code": 200, "job_visible": True},
            "kubernetes_api": {
                "vm_uid": "98b88058-9492-4f08-8d51-98571324d11e",
                "pvc_uid": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
            },
            "postgresql": {"database": "app", "server_version_num": 150000},
        },
        "driver": {
            "protocol_version": 1,
            "live_sources": ["application_api", "kubernetes_api", "postgresql"],
            "scenarios": [
                "response_loss",
                "leader_overlap",
                "slow_boot",
                "deadline",
                "missing_stop_evidence",
                "forced_deletion",
                "replacement",
            ],
        },
        "substrate": {
            "cluster_created_by_gate": True,
            "kubernetes": True,
            "kubevirt": True,
            "cdi": True,
            "longhorn": True,
            "longhorn_workloads_ready": True,
            "longhorn_nodes_ready": True,
            "longhorn_rwo_retain_test": True,
        },
        "response_loss": {
            "fault_injection": "committed_response_replay",
            "recovery_rows": 1,
            "request_receipts": 1,
            "terminal_reports": 0,
            "queue_token_before": 27,
            "queue_token_after": 28,
        },
        "leader_overlap": {
            "fault_injection": "concurrent_store_callers",
            "active_operations": 1,
            "accepted_retry_results": 1,
            "max_global_probes": 4,
            "max_node_probes": 1,
            "configured_global_probe_limit": 4,
            "configured_node_probe_limit": 1,
            "deadline_preserved": True,
        },
        "slow_boot": {
            "state": "recovered",
            "age_seconds": 480,
            "injected_delay_seconds": 15,
            "executor_occupied_while_waiting": False,
            "attested": True,
        },
        "deadline": {
            "fault_injection": "expired_deadline_test_hook",
            "state": "paused_attention",
            "reason_code": "workspace_recovery_deadline_exceeded",
            "disk_retained": True,
            "checkpoint_retained": True,
            "late_probe_released": False,
        },
        "missing_stop_evidence": {
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "successor_dispatched": False,
        },
        "forced_deletion": {
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "successor_dispatched": False,
        },
        "replacement": {
            "state": "recovered",
            "pvc_uid_before": "pvc-1",
            "pvc_uid_after": "pvc-1",
            "marker_before": "marker-7",
            "marker_after": "marker-7",
            "checkpoint_before": "checkpoint-9",
            "checkpoint_after": "checkpoint-9",
            "pin_acknowledged": True,
            "trusted_stop_receipt": True,
            "stop_receipt": {
                "id": "51e295a8-c3cc-4573-acd0-b7dab3a8b2dd",
                "evidence_digest": "sha256:" + "a" * 64,
                "vm_uid": "98b88058-9492-4f08-8d51-98571324d11e",
                "vmi_uid": "126b3b28-81ca-4841-97bf-8806cc53e772",
                "launcher_uid": "e73090a6-9bc6-4c75-9548-b1cae1e9785b",
                "root_pvc_uid": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
            },
            "resume_receipt": {
                "kind": "vm_workspace_recovery",
                "claim_token": 1,
                "successor": {"ssh_registration_id": "registration-1"},
            },
            "retention_pin": {
                "controller_pin_uid": "pin-1",
                "controller_pin_resource_version": "42",
                "controller_state": "released",
            },
            "pinned_identity": True,
            "network_qualified": True,
            "ssh_host_fingerprint_pinned": True,
            "root_pv_csi_driver": "driver.longhorn.io",
            "longhorn_volume_healthy": True,
            "successor_dispatches": 1,
        },
        "revisions": {
            "application": "sha256:app",
            "controller": "sha256:controller",
            "guest_image": "sha256:guest",
        },
    }


def test_pass_requires_real_substrate_and_every_api_db_evidence_gate() -> None:
    gate.validate_acceptance_evidence(_passing_evidence())

    mutations = (
        ("substrate", "longhorn", False),
        ("substrate", "longhorn_workloads_ready", False),
        ("substrate", "longhorn_nodes_ready", False),
        ("substrate", "longhorn_rwo_retain_test", False),
        ("response_loss", "terminal_reports", 1),
        ("leader_overlap", "active_operations", 2),
        ("slow_boot", "executor_occupied_while_waiting", True),
        ("deadline", "late_probe_released", True),
        ("missing_stop_evidence", "successor_dispatched", True),
        ("forced_deletion", "state", "recovered"),
        ("replacement", "pvc_uid_after", "pvc-2"),
        ("replacement", "checkpoint_after", "checkpoint-10"),
        ("replacement", "trusted_stop_receipt", False),
        ("replacement", "stop_receipt", {}),
        ("replacement", "resume_receipt", {}),
        ("replacement", "retention_pin", {}),
        ("replacement", "network_qualified", False),
        ("replacement", "root_pv_csi_driver", "hostpath.csi.k8s.io"),
        ("replacement", "longhorn_volume_healthy", False),
    )
    for section, key, value in mutations:
        evidence = _passing_evidence()
        evidence[section][key] = value
        with pytest.raises(gate.GateFailure):
            gate.validate_acceptance_evidence(evidence)


def test_destructive_run_only_accepts_uniquely_owned_gate_cluster_names() -> None:
    for unsafe in ("srw", "production", "k3d-srw", "srw-vm-recovery-gate"):
        with pytest.raises(gate.GateFailure):
            gate.require_disposable_cluster_name(unsafe)

    gate.require_disposable_cluster_name("srw-vm-recovery-gate-a1b2c3")


def test_preflight_reports_missing_capability_without_running_commands(
    tmp_path,
) -> None:
    values = tmp_path / "values.yaml"
    values.write_text("license:\n  acceptTerms: true\n", encoding="utf-8")
    driver = tmp_path / "driver"
    driver.write_text("#!/bin/sh\n", encoding="utf-8")
    driver.chmod(0o700)
    calls: list[list[str]] = []

    def command(args: list[str], **_kwargs):
        calls.append(args)
        raise AssertionError("preflight must not mutate or query a cluster")

    report = gate.host_preflight(
        values_file=values,
        scenario_driver=driver,
        guest_image="registry.example/guest@sha256:" + "a" * 64,
        which=lambda name: None if name == "k3d" else f"/usr/bin/{name}",
        device_exists=lambda _path: True,
        command=command,
    )

    assert report.ready is False
    assert report.missing == ("binary:k3d",)
    assert calls == []


def test_default_scenario_driver_is_source_controlled_and_executable() -> None:
    assert gate.DEFAULT_SCENARIO_DRIVER == DRIVER
    assert DRIVER.is_file()
    assert DRIVER.stat().st_mode & 0o111
    assert 'evidence["driver"] =' not in DRIVER.read_text(encoding="utf-8")


def test_preflight_uses_the_in_repo_driver_by_default(tmp_path) -> None:
    values = tmp_path / "values.yaml"
    values.write_text("license:\n  acceptTerms: true\n", encoding="utf-8")

    report = gate.host_preflight(
        values_file=values,
        guest_image="registry.example/guest@sha256:" + "a" * 64,
        which=lambda name: f"/usr/bin/{name}",
        device_exists=lambda _path: True,
    )

    assert report.ready is True


def test_forced_deletion_and_missing_stop_evidence_are_pause_only() -> None:
    for section in ("forced_deletion", "missing_stop_evidence"):
        outcome = gate.classify_stop_evidence(
            forced_deletion=section == "forced_deletion",
            exact_termination_receipt=False,
        )
        assert outcome == {
            "state": "paused_attention",
            "reason_code": "prior_runtime_unfenced",
            "successor_dispatched": False,
        }

    assert gate.classify_stop_evidence(
        forced_deletion=False, exact_termination_receipt=True
    ) == {"state": "eligible", "reason_code": None, "successor_dispatched": False}


def test_cluster_cleanup_requires_the_original_runtime_labeled_containers() -> None:
    class FakeShell:
        def __init__(self) -> None:
            self.responses = ["node-a\nnode-b", "node-a\nnode-b", ""]
            self.calls: list[list[str]] = []

        def run(self, args, **_kwargs):
            self.calls.append(list(args))
            return self.responses.pop(0)

    shell = FakeShell()
    ownership = gate.capture_cluster_ownership(shell, "srw-vm-recovery-gate-a1b2c3")
    gate.delete_owned_cluster(shell, "srw-vm-recovery-gate-a1b2c3", ownership=ownership)
    assert shell.calls[-1][:3] == ["k3d", "cluster", "delete"]

    shell = FakeShell()
    ownership = gate.capture_cluster_ownership(shell, "srw-vm-recovery-gate-a1b2c3")
    shell.responses[0] = "node-c"
    with pytest.raises(gate.GateFailure, match="ownership"):
        gate.delete_owned_cluster(
            shell, "srw-vm-recovery-gate-a1b2c3", ownership=ownership
        )


def test_deployed_image_id_must_match_imported_config_digest() -> None:
    digest = "sha256:" + "a" * 64
    assert gate.image_id_matches_config_digest("containerd://" + digest, digest)
    assert not gate.image_id_matches_config_digest(
        "containerd://sha256:" + "b" * 64, digest
    )
