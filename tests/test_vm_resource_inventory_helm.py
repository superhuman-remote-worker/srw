import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

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


def full_policy_values(tmp_path, *, mutate=None):
    from tests.test_vm_resource_policy import whole_launcher_policy

    policy = whole_launcher_policy()["policy"]
    policy.update(
        stableClusterId="test-cluster", shadowEnabled=True,
        enforcementEnabled=True,
    )
    policy["inventory"]["maxItems"] = 1000
    value = {
        "vm": {
            "mode": "same-cluster",
            "lifecycleAuthSecretName": "vm-lifecycle",
            "resourceAdmission": policy,
        },
        "orchestrator": {"vmProvisioning": {"creationRetryEnabled": True}},
        "agent": {"tailscale": {"enabled": False}},
    }
    if mutate is not None:
        mutate(value)
    path = tmp_path / "full-resource-values.yaml"
    path.write_text(yaml.safe_dump(value))
    return path


def render_full_policy(tmp_path, *, mutate=None):
    path = full_policy_values(tmp_path, mutate=mutate)
    return subprocess.run(
        [
            "helm", "template", "resource-test",
            str(Path(__file__).resolve().parents[1] / "helm"),
            "-n", "control-plane", "-f",
            str(Path(__file__).resolve().parents[1] / "helm/ci/test-values.yaml"),
            "-f", str(path),
        ],
        capture_output=True, text=True,
    )


@pytest.mark.parametrize("kubevirt_version", ["v1.6.6", "v1.8.4"])
def test_explicit_whole_resource_policy_renders_one_valid_shared_document(tmp_path, kubevirt_version):
    from shared.vm_resource_policy import validate_enforcement_resource_policy
    from tests.test_helm_vm_workspace_recovery import _env, _orchestrator

    rendered = render_full_policy(tmp_path, mutate=lambda values: values["vm"]["resourceAdmission"]["launcherProfile"].update(
        kubevirtVersion=kubevirt_version,
        costAlgorithm=f"kubevirt-{kubevirt_version}-amd64-ordinary-pvc-v1",
    ))
    assert rendered.returncode == 0, rendered.stderr
    docs = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]
    controller = next(
        doc for doc in docs if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("vm-controller")
    )
    orchestrator = _orchestrator(docs)
    left = _env(docs, orchestrator)["VM_RESOURCE_ADMISSION_CONFIG"]
    right = _env(docs, controller)["VM_RESOURCE_ADMISSION_CONFIG"]
    assert left == right
    policy = validate_enforcement_resource_policy(json.loads(left))
    assert policy.inventory.protocol == 2
    for deployment in (orchestrator, controller):
        assert deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/vm-resource-policy"
        ] == policy.policy_digest.removeprefix("sha256:")


@pytest.mark.parametrize(
    "case",
    [
        "shadow_only", "enforcement_only", "retry_disabled", "ack_missing",
        "budget_missing", "host_cost_zero", "profile_missing",
        "profile_unsupported", "profile_version_unknown", "profile_algorithm_mismatch", "arch_label_missing", "installation_missing",
    ],
)
def test_incomplete_whole_resource_policy_is_rejected(tmp_path, case):
    def mutate(value):
        policy = value["vm"]["resourceAdmission"]
        if case == "shadow_only":
            policy["enforcementEnabled"] = False
        elif case == "enforcement_only":
            policy["shadowEnabled"] = False
        elif case == "retry_disabled":
            value["orchestrator"]["vmProvisioning"]["creationRetryEnabled"] = False
        elif case == "ack_missing":
            policy["clusterWidePodReadAcknowledged"] = False
        elif case == "budget_missing":
            policy["installationBudget"]["memoryBytes"] = None
        elif case == "host_cost_zero":
            policy["hostCost"]["cpuMillicoresPerVcpuDenominator"] = 0
        elif case == "profile_missing":
            policy["launcherProfile"] = None
        elif case == "profile_unsupported":
            policy["launcherProfile"]["architecture"] = "arm64"
        elif case == "profile_version_unknown":
            policy["launcherProfile"]["kubevirtVersion"] = "v1.8.5"
            policy["launcherProfile"]["costAlgorithm"] = "kubevirt-v1.8.5-amd64-ordinary-pvc-v1"
        elif case == "profile_algorithm_mismatch":
            policy["launcherProfile"]["kubevirtVersion"] = "v1.8.4"
        elif case == "arch_label_missing":
            policy["inventory"]["nodeLabelKeys"].remove("kubernetes.io/arch")
        elif case == "installation_missing":
            policy["inventory"]["kubevirtName"] = None

    rendered = render_full_policy(tmp_path, mutate=mutate)
    assert rendered.returncode != 0, case


def test_reused_values_without_resource_admission_map_render_disabled(tmp_path):
    chart = tmp_path / "helm"
    shutil.copytree(Path(__file__).resolve().parents[1] / "helm", chart)
    values_path = chart / "values.yaml"
    values = values_path.read_text()
    start = values.index("  resourceAdmission:\n")
    end = values.index("  preflight:\n", start)
    values_path.write_text(values[:start] + values[end:])
    rendered = subprocess.run(
        [
            "helm", "template", "legacy-resource", str(chart),
            "-f", str(chart / "ci/test-values.yaml"),
        ],
        capture_output=True, text=True,
    )
    assert rendered.returncode == 0, rendered.stderr
    docs = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]
    assert "VM_RESOURCE_ADMISSION_CONFIG" not in _env(docs, _orchestrator(docs))


def test_default_off_has_no_broad_observer_role_or_policy_env():
    docs = render()
    assert not any(
        doc["metadata"]["name"].endswith("resource-observer") for doc in docs
    )
    assert "VM_RESOURCE_ADMISSION_CONFIG" not in _env(docs, _orchestrator(docs))
    assert not any(
        doc["kind"] == "Role" and doc["metadata"]["name"].endswith("resource-installation")
        for doc in docs
    )


def test_whole_launcher_policy_is_one_digest_and_exact_namespaced_rbac(tmp_path):
    import json
    import yaml

    from tests.test_vm_resource_policy import whole_launcher_policy

    policy = whole_launcher_policy()["policy"]
    policy["stableClusterId"] = "test-cluster"
    policy["inventory"]["maxItems"] = 1000
    value = {
        "vm": {
            "mode": "same-cluster",
            "lifecycleAuthSecretName": "vm-lifecycle",
            "resourceAdmission": policy,
        },
        "agent": {"tailscale": {"enabled": False}},
    }
    path = tmp_path / "whole-resource-values.yaml"
    path.write_text(yaml.safe_dump(value))
    result = subprocess.run(
        ["helm", "template", "native-test", str(Path(__file__).resolve().parents[1] / "helm"),
         "-n", "control-plane", "-f", str(Path(__file__).resolve().parents[1] / "helm/ci/test-values.yaml"),
         "-f", str(path)],
        capture_output=True, text=True, check=True,
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    controller = next(doc for doc in docs if doc["kind"] == "Deployment" and doc["metadata"]["name"].endswith("vm-controller"))
    left = _env(docs, _orchestrator(docs))["VM_RESOURCE_ADMISSION_CONFIG"]
    right = _env(docs, controller)["VM_RESOURCE_ADMISSION_CONFIG"]
    assert left == right
    canonical = json.loads(left)
    assert canonical["policy"] == policy
    settings = InventorySettings.from_environment({
        "VM_RESOURCE_ADMISSION_CONFIG": left, "VM_LIFECYCLE_HMAC_SECRET": "s" * 32,
    })
    assert settings.protocol == 2
    for deployment in (controller, _orchestrator(docs)):
        assert deployment["spec"]["template"]["metadata"]["annotations"]["checksum/vm-resource-policy"] == settings.policy_digest.removeprefix("sha256:")
    installation = next(doc for doc in docs if doc["kind"] == "Role" and doc["metadata"]["name"].endswith("resource-installation"))
    assert installation["metadata"]["namespace"] == "kubevirt"
    assert installation["rules"] == [{
        "apiGroups": ["kubevirt.io"], "resources": ["kubevirts"],
        "resourceNames": ["kubevirt"], "verbs": ["get"],
    }]


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
