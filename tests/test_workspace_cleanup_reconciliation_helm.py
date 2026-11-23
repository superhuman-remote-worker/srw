"""Chart contract for the durable workspace-cleanup reconciliation fence."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"


def _render_documents(*extra: str) -> list[dict]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    output = subprocess.run(
        [
            "helm",
            "template",
            "workspace-cleanup-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [doc for doc in yaml.safe_load_all(output) if doc]


def _render(*extra: str) -> tuple[dict, dict, dict]:
    documents = _render_documents(*extra)
    configmap = next(
        doc
        for doc in documents
        if doc.get("kind") == "ConfigMap"
        and "WORKSPACE_CLEANUP_RECONCILIATION_ENABLED" in (doc.get("data") or {})
    )
    deployment = next(
        doc
        for doc in documents
        if doc.get("kind") == "Deployment"
        and any(
            container.get("name") == "orchestrator"
            for container in doc["spec"]["template"]["spec"]["containers"]
        )
    )
    orchestrator = next(
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container.get("name") == "orchestrator"
    )
    return (
        configmap,
        {entry["name"]: entry for entry in orchestrator.get("env") or []},
        deployment["spec"]["template"]["metadata"]["annotations"],
    )


def test_workspace_cleanup_reconciliation_defaults_false() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["orchestrator"]["workspaceCleanupReconciliationEnabled"] is False
    assert values["orchestrator"]["workspaceLifecycleProtocolCutoverEnabled"] is True
    assert (
        values["orchestrator"]["workspaceLifecycleServiceAccountGeneration"] == "0197"
    )
    assert values["workspace"]["freshFallback"] is False


def test_workspace_cleanup_reconciliation_renders_dark_and_can_be_enabled() -> None:
    dark, dark_env, dark_annotations = _render()
    assert dark["data"]["WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"] == "false"
    assert dark["data"]["WORKSPACE_REATTACH_FRESH_FALLBACK"] == "false"
    assert (
        dark_env["WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"]["valueFrom"][
            "configMapKeyRef"
        ]["key"]
        == "WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"
    )

    enabled, enabled_env, enabled_annotations = _render(
        "--set", "orchestrator.workspaceCleanupReconciliationEnabled=true"
    )
    assert enabled["data"]["WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"] == "true"
    assert (
        enabled_env["WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"]["valueFrom"][
            "configMapKeyRef"
        ]["key"]
        == "WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"
    )

    checksum_key = "checksum/workspace-cleanup-reconciliation"
    assert dark_annotations[checksum_key]
    assert enabled_annotations[checksum_key]
    assert dark_annotations[checksum_key] != enabled_annotations[checksum_key]


def test_workspace_cleanup_reconciliation_checksum_does_not_require_reloader() -> None:
    _, _, dark_annotations = _render()
    _, _, without_reloader = _render("--set", "reloader.enabled=false")
    checksum_key = "checksum/workspace-cleanup-reconciliation"
    assert without_reloader[checksum_key] == dark_annotations[checksum_key]


def test_workspace_lifecycle_epoch_revokes_predecessor_service_account() -> None:
    old_documents = _render_documents(
        "--set", "orchestrator.workspaceLifecycleServiceAccountGeneration=0196"
    )
    current_documents = _render_documents(
        "--set", "orchestrator.workspaceLifecycleServiceAccountGeneration=0197"
    )

    def authority(documents: list[dict]) -> tuple[str, list[str], dict]:
        deployment = next(
            item
            for item in documents
            if item.get("kind") == "Deployment"
            and item.get("metadata", {}).get("name", "").endswith("-orchestrator")
        )
        binding = next(
            item
            for item in documents
            if item.get("kind") == "RoleBinding"
            and item.get("metadata", {}).get("name", "").endswith("-orchestrator")
        )
        accounts = [
            item["metadata"]["name"]
            for item in documents
            if item.get("kind") == "ServiceAccount"
            and "-ows" in item.get("metadata", {}).get("name", "")
        ]
        assert accounts == [
            deployment["spec"]["template"]["spec"]["serviceAccountName"]
        ]
        return (
            deployment["spec"]["template"]["spec"]["serviceAccountName"],
            [subject["name"] for subject in binding["subjects"]],
            deployment,
        )

    old_account, old_subjects, _ = authority(old_documents)
    current_account, current_subjects, current_deployment = authority(current_documents)
    assert old_account != current_account
    assert old_subjects == [old_account]
    assert current_subjects == [current_account]
    assert old_account not in current_subjects
    # Cleanup discovery remains dark, but the one-release authority cutover is
    # Recreate: the old ServiceAccount loses RBAC only while its Pods are being
    # quiesced, and no new owner overlaps the predecessor binary.
    assert current_deployment["spec"]["strategy"]["type"] == "Recreate"

    enabled = _render_documents(
        "--set", "orchestrator.workspaceCleanupReconciliationEnabled=true"
    )
    _, enabled_subjects, enabled_deployment = authority(enabled)
    assert enabled_subjects == [current_account]
    assert enabled_deployment["spec"]["strategy"]["type"] == "Recreate"


def test_workspace_lifecycle_cutover_can_retire_only_after_convergence() -> None:
    documents = _render_documents(
        "--set",
        "orchestrator.workspaceLifecycleProtocolCutoverEnabled=false",
        "--set",
        "orchestrator.manifestContractCutoverEnabled=false",
        "--set",
        "orchestrator.vmRemoteOperationProtocol.enabled=false",
    )
    deployment = next(
        item
        for item in documents
        if item.get("kind") == "Deployment"
        and item.get("metadata", {}).get("name", "").endswith("-orchestrator")
    )
    assert deployment["spec"]["strategy"]["type"] == "RollingUpdate"

    # Either independently activated protocol still requires Recreate.
    cleanup = _render_documents(
        "--set",
        "orchestrator.workspaceLifecycleProtocolCutoverEnabled=false",
        "--set",
        "orchestrator.workspaceCleanupReconciliationEnabled=true",
        "--set",
        "orchestrator.vmRemoteOperationProtocol.enabled=false",
    )
    cleanup_deployment = next(
        item
        for item in cleanup
        if item.get("kind") == "Deployment"
        and item.get("metadata", {}).get("name", "").endswith("-orchestrator")
    )
    assert cleanup_deployment["spec"]["strategy"]["type"] == "Recreate"


def test_recreate_composes_with_vm_protocol() -> None:
    documents = _render_documents(
        "--set", "orchestrator.vmRemoteOperationProtocol.enabled=true"
    )
    deployment = next(
        item
        for item in documents
        if item.get("kind") == "Deployment"
        and item.get("metadata", {}).get("name", "").endswith("-orchestrator")
    )
    assert deployment["spec"]["strategy"]["type"] == "Recreate"


@pytest.mark.parametrize("invalid", ["bad epoch", "bad/epoch", ".0197", "x" * 17])
def test_workspace_lifecycle_epoch_rejects_invalid_service_account_values(
    invalid: str,
) -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "workspace-cleanup-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            "--set-string",
            f"orchestrator.workspaceLifecycleServiceAccountGeneration={invalid}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert rendered.returncode != 0
    assert "workspaceLifecycleServiceAccountGeneration" in rendered.stderr


def test_long_release_name_retains_epoch_suffix() -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    output = subprocess.run(
        [
            "helm",
            "template",
            "workspace-cleanup-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            "--set-string",
            "fullnameOverride=workspace-cleanup-proof-with-a-deliberately-long-name",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    documents = [doc for doc in yaml.safe_load_all(output) if doc]
    account = next(
        doc["metadata"]["name"]
        for doc in documents
        if doc.get("kind") == "ServiceAccount" and "-ows" in doc["metadata"]["name"]
    )
    assert account.endswith("-ows0197")
    assert len(account) <= 63
