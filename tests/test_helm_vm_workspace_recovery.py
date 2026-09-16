"""Helm rollout contracts for durable VM workspace recovery."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from tests.test_manifest_hosting_helm import render


pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")
ROOT = Path(__file__).resolve().parents[1]


def _render_release(release: str, *settings: str) -> list[dict]:
    command = [
        "helm",
        "template",
        release,
        str(ROOT / "helm"),
        "-n",
        "control-plane",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _orchestrator(documents: list[dict]) -> dict:
    return next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"].endswith("-orchestrator")
    )


def _env(documents: list[dict], deployment: dict) -> dict[str, str]:
    config_maps = {
        document["metadata"]["name"]: document.get("data", {})
        for document in documents
        if document.get("kind") == "ConfigMap"
    }
    entries = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    result: dict[str, str] = {}
    for entry in entries:
        if "value" in entry:
            result[entry["name"]] = entry["value"]
            continue
        reference = entry.get("valueFrom", {}).get("configMapKeyRef")
        if reference and reference["key"] in config_maps.get(reference["name"], {}):
            result[entry["name"]] = config_maps[reference["name"]][reference["key"]]
    return result


def test_workspace_recovery_defaults_render_safe_bounded_runtime() -> None:
    values = Path(__file__).resolve().parents[1] / "helm/values.yaml"
    defaults = yaml.safe_load(values.read_text(encoding="utf-8"))["orchestrator"][
        "vmWorkspaceRecovery"
    ]

    assert defaults == {
        "enabled": False,
        "replacementEnabled": False,
        "deadlineSeconds": 900,
        "maxGlobalProbes": 4,
        "maxProbesPerNode": 1,
        "claimTtlSeconds": 30,
        "permitTtlSeconds": 30,
        "externalCallTimeoutSeconds": 10,
    }

    documents = render()
    env = _env(documents, _orchestrator(documents))
    assert {
        key: env[key]
        for key in (
            "VM_WORKSPACE_RECOVERY_ENABLED",
            "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED",
            "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS",
            "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES",
            "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE",
            "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS",
            "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS",
            "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS",
        )
    } == {
        "VM_WORKSPACE_RECOVERY_ENABLED": "false",
        "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED": "false",
        "VM_WORKSPACE_RECOVERY_DEADLINE_SECONDS": "900",
        "VM_WORKSPACE_RECOVERY_MAX_GLOBAL_PROBES": "4",
        "VM_WORKSPACE_RECOVERY_MAX_PROBES_PER_NODE": "1",
        "VM_WORKSPACE_RECOVERY_CLAIM_TTL_SECONDS": "30",
        "VM_WORKSPACE_RECOVERY_PERMIT_TTL_SECONDS": "30",
        "VM_WORKSPACE_RECOVERY_EXTERNAL_CALL_TIMEOUT_SECONDS": "10",
    }


def test_workspace_recovery_settings_roll_orchestrator_and_preserve_separate_gates() -> (
    None
):
    default_documents = render()
    enabled_documents = render("orchestrator.vmWorkspaceRecovery.enabled=true")
    replacement_documents = render(
        "orchestrator.vmWorkspaceRecovery.enabled=true",
        "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
    )
    tuned_documents = render("orchestrator.vmWorkspaceRecovery.maxGlobalProbes=3")
    default = _orchestrator(default_documents)
    enabled = _orchestrator(enabled_documents)
    replacement = _orchestrator(replacement_documents)
    tuned = _orchestrator(tuned_documents)

    def checksum(deployment: dict) -> str:
        return deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/vm-workspace-recovery"
        ]

    assert (
        len(
            {
                checksum(default),
                checksum(enabled),
                checksum(replacement),
                checksum(tuned),
            }
        )
        == 4
    )
    assert (
        _env(enabled_documents, enabled)["VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED"]
        == "false"
    )
    assert (
        _env(replacement_documents, replacement)[
            "VM_WORKSPACE_REPLACEMENT_RECOVERY_ENABLED"
        ]
        == "true"
    )


@pytest.mark.parametrize(
    "setting",
    (
        "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
        "orchestrator.vmWorkspaceRecovery.deadlineSeconds=0",
        "orchestrator.vmWorkspaceRecovery.deadlineSeconds=901",
        "orchestrator.vmWorkspaceRecovery.maxGlobalProbes=0",
        "orchestrator.vmWorkspaceRecovery.maxProbesPerNode=0",
        "orchestrator.vmWorkspaceRecovery.maxProbesPerNode=2",
        "orchestrator.vmWorkspaceRecovery.claimTtlSeconds=10",
        "orchestrator.vmWorkspaceRecovery.permitTtlSeconds=9",
        "orchestrator.vmWorkspaceRecovery.externalCallTimeoutSeconds=30",
    ),
)
def test_workspace_recovery_rejects_unsafe_rollout_settings(setting: str) -> None:
    result = render(setting, check=False)
    assert result.returncode != 0


def test_disposable_acceptance_adapter_is_explicit_and_in_image() -> None:
    documents = _render_release(
        "srw",
        "orchestrator.vmWorkspaceRecovery.enabled=true",
        "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
        "orchestrator.vmWorkspaceRecoveryAcceptanceGate.enabled=true",
    )
    adapter = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document["metadata"]["name"].endswith("-vm-workspace-recovery-gate-adapter")
    )
    command = yaml.safe_load(adapter["data"]["commandJson"])

    assert command == [
        "python",
        "-m",
        "orchestrator.operator_cli.vm_workspace_recovery_acceptance",
    ]
    assert adapter["data"]["protocolVersion"] == "1"
    assert adapter["metadata"]["name"] == (
        "srw-superhuman-remote-worker-vm-workspace-recovery-gate-adapter"
    )
    assert (
        adapter["metadata"]["labels"]["srw.io/vm-workspace-recovery-gate-adapter"]
        == "true"
    )
    env = _env(documents, _orchestrator(documents))
    assert env["VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED"] == "true"
    assert any(
        document.get("kind") == "Role"
        and document["metadata"]["name"].endswith("-vm-workspace-recovery-gate")
        for document in documents
    )
    role = next(
        document
        for document in documents
        if document.get("kind") == "Role"
        and document["metadata"]["name"].endswith("-vm-workspace-recovery-gate")
    )
    assert any(
        rule.get("apiGroups") == ["cdi.kubevirt.io"]
        and "datavolumes" in rule.get("resources", [])
        and "get" in rule.get("verbs", [])
        for rule in role["rules"]
    )


def test_acceptance_adapter_name_follows_fullname_override() -> None:
    documents = _render_release(
        "srw",
        "fullnameOverride=custom-stack",
        "orchestrator.vmWorkspaceRecovery.enabled=true",
        "orchestrator.vmWorkspaceRecovery.replacementEnabled=true",
        "orchestrator.vmWorkspaceRecoveryAcceptanceGate.enabled=true",
    )

    adapters = [
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {})
        .get("labels", {})
        .get("srw.io/vm-workspace-recovery-gate-adapter")
        == "true"
    ]

    assert [item["metadata"]["name"] for item in adapters] == [
        "custom-stack-vm-workspace-recovery-gate-adapter"
    ]


def test_acceptance_adapter_requires_both_automatic_recovery_gates() -> None:
    result = render(
        "orchestrator.vmWorkspaceRecoveryAcceptanceGate.enabled=true",
        check=False,
    )
    assert result.returncode != 0
