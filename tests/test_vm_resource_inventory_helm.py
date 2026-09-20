import shutil

import pytest

from shared.vm_resource_inventory_settings import InventorySettings
from tests.test_manifest_hosting_helm import render
from tests.test_helm_vm_workspace_recovery import _env, _orchestrator

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")


def enabled(*overrides):
    return render(
        "vm.mode=same-cluster",
        "vm.lifecycleAuthSecretName=vm-lifecycle",
        "agent.tailscale.enabled=false",
        "vm.resourceAdmission.observerEnabled=true",
        "vm.resourceAdmission.clusterWidePodReadAcknowledged=true",
        "vm.resourceAdmission.stableClusterId=test-cluster",
        "vm.resourceAdmission.inventory.publishIntervalSeconds=10",
        "vm.resourceAdmission.inventory.staleAfterSeconds=60",
        "vm.resourceAdmission.inventory.requestTimeoutSeconds=5",
        "vm.resourceAdmission.inventory.collectionTimeoutSeconds=20",
        "vm.resourceAdmission.inventory.publicationTimeoutSeconds=5",
        "vm.resourceAdmission.inventory.maxItems=1000",
        "vm.resourceAdmission.inventory.maxBytes=100000",
        "vm.resourceAdmission.inventory.historyLimit=3",
        "vm.resourceAdmission.inventory.nodeLabelKeys[0]=kubernetes.io/hostname",
        *overrides,
        check=False,
    )


def test_default_off_has_no_broad_observer_role_or_policy_env():
    docs = render()
    assert not any(
        doc["metadata"]["name"].endswith("resource-observer") for doc in docs
    )
    assert "VM_RESOURCE_ADMISSION_CONFIG" not in _env(docs, _orchestrator(docs))


def test_enabled_policy_matches_both_processes_and_rbac_is_list_only():
    import yaml

    result = enabled()
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    orchestrator = _orchestrator(docs)
    controller = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("vm-controller")
    )
    left = _env(docs, orchestrator)["VM_RESOURCE_ADMISSION_CONFIG"]
    right = _env(docs, controller)["VM_RESOURCE_ADMISSION_CONFIG"]
    assert left == right
    settings = InventorySettings.from_environment(
        {"VM_RESOURCE_ADMISSION_CONFIG": left, "VM_LIFECYCLE_HMAC_SECRET": "s" * 32}
    )
    assert settings.namespace == "control-plane"
    assert settings.cluster_id == "test-cluster"
    for pod in (orchestrator, controller):
        assert pod["spec"]["template"]["metadata"]["annotations"][
            "checksum/vm-resource-policy"
        ] == settings.policy_digest.removeprefix("sha256:")
    role = next(
        doc
        for doc in docs
        if doc["kind"] == "ClusterRole"
        and doc["metadata"]["name"].endswith("resource-observer")
    )
    assert {r for rule in role["rules"] for r in rule["resources"]} == {
        "nodes",
        "pods",
        "persistentvolumes",
        "storageclasses",
    }
    assert all(rule["verbs"] == ["list"] for rule in role["rules"])
    old = next(
        doc
        for doc in docs
        if doc["kind"] == "ClusterRole"
        and doc["metadata"]["name"].endswith("node-observer")
    )
    assert old["rules"][0]["verbs"] == ["get"]


@pytest.mark.parametrize(
    "override",
    [
        "vm.resourceAdmission.clusterWidePodReadAcknowledged=false",
        "vm.resourceAdmission.stableClusterId=",
        "vm.resourceAdmission.inventory.maxItems=null",
        "vm.resourceAdmission.inventory.maxBytes=0",
        "vm.resourceAdmission.inventory.historyLimit=1.5",
        "vm.resourceAdmission.inventory.nodeLabelKeys[0]=other",
        "vm.lifecycleAuthSecretName=",
        "vm.mode=off",
        "vm.resourceAdmission.shadowEnabled=true",
        "vm.resourceAdmission.enforcementEnabled=true",
    ],
)
def test_incomplete_or_unimplemented_policy_cannot_be_enabled(override):
    assert enabled(override).returncode != 0
