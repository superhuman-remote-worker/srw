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
DRIVER_SPEC = importlib.util.spec_from_file_location(
    "vm_workspace_recovery_scenario", DRIVER
)
assert DRIVER_SPEC and DRIVER_SPEC.loader
driver = importlib.util.module_from_spec(DRIVER_SPEC)
DRIVER_SPEC.loader.exec_module(driver)


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
            "fault_injection": "controlled_leader_handoff",
            "active_operations": 1,
            "leader_instances": 2,
            "leader_a_identity": "gate-leader-a:run-1",
            "leader_b_identity": "gate-leader-b:run-1",
            "leader_a_backend_pid": 4101,
            "leader_b_backend_pid": 4102,
            "leadership_transfer_succeeded": True,
            "max_global_probes": 4,
            "max_node_probes": 1,
            "configured_global_probe_limit": 4,
            "configured_node_probe_limit": 1,
            "deadline_preserved": True,
            "stale_probe_finished_after_handoff": True,
            "stale_store_boundary": "claim_is_current",
            "stale_store_boundary_rejected": True,
            "stale_stage_attempted": False,
            "stale_result_rejected": True,
            "successor_dispatches": 1,
        },
        "slow_boot": {
            "state": "recovered",
            "age_seconds": 480,
            "injected_delay_seconds": 15,
            "executor_occupied_while_waiting": False,
            "attested": True,
        },
        "deadline": {
            "fault_injection": "live_claim_deadline_barrier",
            "state": "paused_attention",
            "reason_code": "workspace_recovery_deadline_exceeded",
            "probe_started_before_deadline": True,
            "probe_finished_after_deadline": True,
            "precondition_check_rejected": True,
            "stage_observation_attempted": False,
            "release_attempted": False,
            "final_release_succeeded": False,
            "successor_dispatches": 0,
            "queue_still_parked": True,
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
            "profiled_fixture": True,
            "pvc_uid_before": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
            "pvc_uid_after": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
            "vmi_uid_before": "126b3b28-81ca-4841-97bf-8806cc53e772",
            "vmi_uid_after": "9cb0c155-e71b-4e35-9282-cbcb846b1c0e",
            "interface_mac_before": "02:00:00:00:00:41",
            "interface_mac_after": "02:00:00:00:00:42",
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
                "stop_receipt_digest": "sha256:" + "a" * 64,
                "successor": {
                    "ssh_registration_id": "registration-1",
                    "vmi_uid": "9cb0c155-e71b-4e35-9282-cbcb846b1c0e",
                    "launcher_uid": "f1fb1665-6b5d-4956-b662-ebd49f4d1267",
                    "interface_mac": "02:00:00:00:00:42",
                },
            },
            "network_profile_receipt_before": {
                "vm_uid": "98b88058-9492-4f08-8d51-98571324d11e",
                "pvc_uid": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
                "vmi_uid": "126b3b28-81ca-4841-97bf-8806cc53e772",
                "launcher_uid": "e73090a6-9bc6-4c75-9548-b1cae1e9785b",
            },
            "network_profile_receipt_after": {
                "vm_uid": "98b88058-9492-4f08-8d51-98571324d11e",
                "pvc_uid": "1535a95e-6dd8-468b-ad16-37e0d9a4aa88",
                "vmi_uid": "9cb0c155-e71b-4e35-9282-cbcb846b1c0e",
                "launcher_uid": "f1fb1665-6b5d-4956-b662-ebd49f4d1267",
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


def test_unprofiled_job_receipts_do_not_need_network_profile_fields() -> None:
    evidence = _passing_evidence()
    replacement = evidence["replacement"]
    replacement["profiled_fixture"] = False
    replacement["network_profile_receipt_before"] = None
    replacement["network_profile_receipt_after"] = None
    gate.validate_acceptance_evidence(evidence)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("vmi_uid_after", "126b3b28-81ca-4841-97bf-8806cc53e772"),
        ("vmi_uid_before", None),
        ("vmi_uid_after", None),
        ("vmi_uid_after", "not-a-uuid"),
        ("interface_mac_after", "02:00:00:00:00:41"),
        ("interface_mac_after", None),
        ("interface_mac_after", "02:00:00:00:00:gg"),
        ("interface_mac_before", None),
        ("interface_mac_before", "not-a-mac"),
    ],
)
def test_replacement_requires_observed_changed_vmi_and_mac(field, value) -> None:
    evidence = _passing_evidence()
    evidence["replacement"][field] = value
    with pytest.raises(gate.GateFailure):
        gate.validate_acceptance_evidence(evidence)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("stop_receipt", "vmi_uid", "9cb0c155-e71b-4e35-9282-cbcb846b1c0e"),
        ("resume_successor", "vmi_uid", "126b3b28-81ca-4841-97bf-8806cc53e772"),
        ("resume_successor", "interface_mac", "02:00:00:00:00:43"),
        ("resume_receipt", "stop_receipt_digest", "sha256:" + "b" * 64),
        ("network_profile_receipt_before", "vmi_uid", "9cb0c155-e71b-4e35-9282-cbcb846b1c0e"),
        ("network_profile_receipt_after", "launcher_uid", "e73090a6-9bc6-4c75-9548-b1cae1e9785b"),
        ("network_profile_receipt_after", "interface_mac", "02:00:00:00:00:43"),
    ],
)
def test_replacement_rejects_stale_stop_resume_or_network_receipt(
    section, field, value,
) -> None:
    evidence = _passing_evidence()
    replacement = evidence["replacement"]
    target = (
        replacement["resume_receipt"]["successor"]
        if section == "resume_successor" else replacement[section]
    )
    target[field] = value
    with pytest.raises(gate.GateFailure):
        gate.validate_acceptance_evidence(evidence)


def test_existing_evidence_gates_still_reject_missing_or_conflicting_values() -> None:
    mutations = (
        ("substrate", "longhorn", False),
        ("substrate", "longhorn_workloads_ready", False),
        ("substrate", "longhorn_nodes_ready", False),
        ("substrate", "longhorn_rwo_retain_test", False),
        ("response_loss", "terminal_reports", 1),
        ("leader_overlap", "active_operations", 2),
        ("leader_overlap", "leader_instances", 1),
        ("leader_overlap", "leader_b_identity", "gate-leader-a:run-1"),
        ("leader_overlap", "leader_b_backend_pid", 4101),
        ("leader_overlap", "leadership_transfer_succeeded", False),
        ("leader_overlap", "stale_probe_finished_after_handoff", False),
        ("leader_overlap", "stale_store_boundary", ""),
        ("leader_overlap", "stale_store_boundary", "recovery_preconditions"),
        ("leader_overlap", "stale_store_boundary_rejected", False),
        ("leader_overlap", "stale_stage_attempted", True),
        ("leader_overlap", "stale_result_rejected", False),
        ("leader_overlap", "successor_dispatches", 2),
        ("slow_boot", "executor_occupied_while_waiting", True),
        ("deadline", "probe_started_before_deadline", False),
        ("deadline", "probe_finished_after_deadline", False),
        ("deadline", "precondition_check_rejected", False),
        ("deadline", "stage_observation_attempted", True),
        ("deadline", "release_attempted", True),
        ("deadline", "final_release_succeeded", True),
        ("deadline", "successor_dispatches", 1),
        ("deadline", "queue_still_parked", False),
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
        ("replacement", "network_profile_receipt_before", None),
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


def test_scenario_driver_discovers_the_unique_rendered_adapter(monkeypatch) -> None:
    calls: list[list[str]] = []

    def run(args, **_kwargs):
        calls.append(list(args))
        return type(
            "Result",
            (),
            {
                "returncode": 0,
                "stdout": '{"items":[{"metadata":{"name":"custom-stack-vm-workspace-recovery-gate-adapter"},"data":{}}]}',
            },
        )()

    monkeypatch.setattr(driver, "_run", run)

    resource = driver.discover_adapter("k3d-srw-vm-recovery-gate-a1b2c3", "srw")

    assert resource["metadata"]["name"] == (
        "custom-stack-vm-workspace-recovery-gate-adapter"
    )
    assert "srw.io/vm-workspace-recovery-gate-adapter=true" in calls[0]
    assert "srw-vm-workspace-recovery-gate-adapter" not in calls[0]


def test_enabled_chart_missing_adapter_is_a_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        driver,
        "_run",
        lambda *_args, **_kwargs: type(
            "Result", (), {"returncode": 0, "stdout": '{"items":[]}'}
        )(),
    )

    with pytest.raises(driver.ScenarioFailure, match="exactly one"):
        driver.discover_adapter("k3d-srw-vm-recovery-gate-a1b2c3", "srw")


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


def test_live_deadline_gate_helm_install_keeps_claim_alive_past_probe_timeout(
    tmp_path,
) -> None:
    class FakeShell:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, args, **_kwargs):
            self.calls.append(list(args))
            if "create" in args and "secret" in args:
                return '{"apiVersion":"v1","kind":"Secret"}'
            return ""

    shell = FakeShell()
    guest_image = "registry.example/guest@sha256:" + "a" * 64
    gate.deploy_application(
        shell,
        context="k3d-gate",
        namespace="srw",
        values_file=tmp_path / "values.yaml",
        images={
            "orchestrator": "localhost/srw-orchestrator:gate",
            "agent": "localhost/srw-agent:gate",
            "vm_controller": "localhost/srw-vm-controller:gate",
        },
        guest_image=guest_image,
    )

    helm = next(call for call in shell.calls if call[:2] == ["helm", "upgrade"])
    assert "orchestrator.vmWorkspaceRecovery.claimTtlSeconds=90" in helm
    assert "orchestrator.vmWorkspaceRecovery.permitTtlSeconds=90" in helm
    assert "orchestrator.vmWorkspaceRecovery.externalCallTimeoutSeconds=60" in helm
    assert "orchestrator.vmProvisioning.creationRetryEnabled=true" in helm
    assert "vmController.networkProfile.enabled=true" in helm
    assert f"vmController.networkProfile.imageAllowlist[0]={guest_image}" in helm
    assert f"vmController.defaultVmImage={guest_image}" in helm
