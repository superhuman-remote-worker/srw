from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
LABEL = "srw.io/vm-remote-operation-protocol"


def _render(*extra: str) -> list[dict]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    raw = subprocess.run(
        [
            "helm",
            "template",
            "vm-effect-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [document for document in yaml.safe_load_all(raw) if document]


def _orchestrator(documents: list[dict]) -> dict:
    return next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"].endswith("-orchestrator")
    )


def test_vm_protocol_defaults_on_and_recreates() -> None:
    # Owner, 2026-11-22: VM files on the Canvas are editable out of the box,
    # so the protocol a VM write needs is on by default.
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["orchestrator"]["vmRemoteOperationProtocol"]["enabled"] is True
    deployment = _orchestrator(_render())
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert labels.get(LABEL) == "v1"


def test_vm_protocol_off_rolls_when_other_cutovers_are_dark() -> None:
    deployment = _orchestrator(
        _render(
            "--set",
            "orchestrator.vmRemoteOperationProtocol.enabled=false",
            "--set",
            "orchestrator.workspaceLifecycleProtocolCutoverEnabled=false",
            "--set",
            "orchestrator.workspaceCleanupReconciliationEnabled=false",
            "--set",
            "orchestrator.manifestContractCutoverEnabled=false",
        )
    )
    assert deployment["spec"]["strategy"]["type"] == "RollingUpdate"
    assert LABEL not in deployment["spec"]["template"]["metadata"]["labels"]


def test_vm_protocol_or_maintenance_forces_recreate() -> None:
    for flag in (
        "orchestrator.vmRemoteOperationProtocol.enabled=true",
        "orchestrator.runtimeAuthorityMigrationMaintenanceAck=true",
    ):
        deployment = _orchestrator(_render("--set", flag))
        assert deployment["spec"]["strategy"]["type"] == "Recreate"


@pytest.mark.parametrize("enabled", [False, True])
def test_same_cluster_vm_ingress_preserves_existing_ssh_transport(
    enabled: bool,
) -> None:
    args = [
        "--set",
        "vm.mode=same-cluster",
        "--set",
        "vm.lifecycleAuthSecretName=vm-lifecycle-test",
        "--set",
        "agent.tailscale.enabled=false",
    ]
    args += [
        "--set",
        f"orchestrator.vmRemoteOperationProtocol.enabled={str(enabled).lower()}",
    ]
    documents = _render(*args)
    deployment = _orchestrator(documents)
    labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert (labels.get(LABEL) == "v1") is enabled

    policies = [d for d in documents if d.get("kind") == "NetworkPolicy"]
    orchestrator_rules = [
        rule
        for policy in policies
        for rule in policy["spec"].get("ingress", [])
        if any(
            peer.get("podSelector", {})
            .get("matchLabels", {})
            .get("app.kubernetes.io/component")
            == "orchestrator"
            for peer in rule.get("from", [])
        )
    ]
    assert any(
        any(int(port["port"]) == 22 for port in rule.get("ports", []))
        for rule in orchestrator_rules
    )
