"""Pinned, closed KubeVirt 1.6.6 whole-launcher prediction and CR proof."""

from copy import deepcopy

import pytest

from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector
from shared.vm_launcher_profile import (
    default_launcher_profile,
    normalize_installed_profile,
    predict_launcher,
)


def installed_cr():
    return {
        "metadata": {
            "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "name": "kubevirt",
            "namespace": "kubevirt",
            "generation": 3,
        },
        "spec": {"configuration": {}},
        "status": {
            "phase": "Deployed",
            "observedGeneration": 3,
            "targetKubeVirtVersion": "v1.6.6",
            "observedKubeVirtVersion": "v1.6.6",
            "targetDeploymentID": "sha256:deployed-a",
            "observedDeploymentID": "sha256:deployed-a",
        },
    }


def test_closed_default_profile_predicts_captured_six_field_launcher():
    profile = default_launcher_profile()
    assert predict_launcher(profile, guest_vcpus=2, guest_memory_bytes=512 * 1024**2) == ResourceVector(
        205, 861277888, 1, 50000000, 1, 1
    )
    assert predict_launcher(profile, guest_vcpus=4, guest_memory_bytes=1024 * 1024**2) == ResourceVector(
        405, 1414926016, 1, 50000000, 1, 1
    )
    without_log = deepcopy(profile)
    without_log["disableSerialConsoleLog"] = True
    assert predict_launcher(without_log, guest_vcpus=2, guest_memory_bytes=512 * 1024**2) == ResourceVector(
        200, 826277888, 1, 50000000, 1, 1
    )


def test_ratio_and_console_override_change_prediction_without_changing_reserve():
    profile = default_launcher_profile()
    profile["cpuAllocationRatio"] = 5
    profile["supportContainerResources"]["guest-console-log"]["requests"]["cpuMillicores"] = 10
    profile["supportContainerResources"]["guest-console-log"]["requests"]["memoryBytes"] = 40000000
    assert predict_launcher(profile, guest_vcpus=2, guest_memory_bytes=512 * 1024**2) == ResourceVector(
        410, 866277888, 1, 50000000, 1, 1
    )


def test_installed_cr_normalizes_only_closed_fields_and_requires_settled_version():
    cr = installed_cr()
    observation = normalize_installed_profile(cr)
    assert observation["profile"] == default_launcher_profile()
    assert observation["uid"] == cr["metadata"]["uid"]
    assert observation["generation"] == observation["observedGeneration"] == 3
    assert "spec" not in observation and "status" not in observation
    changed = deepcopy(cr)
    changed["status"]["observedGeneration"] = 2
    with pytest.raises(ResourceAdmissionError):
        normalize_installed_profile(changed)
    changed = deepcopy(cr)
    changed["spec"]["configuration"]["developerConfiguration"] = {"cpuAllocationRatio": 0}
    with pytest.raises(ResourceAdmissionError):
        normalize_installed_profile(changed)
    changed = deepcopy(cr)
    changed["spec"]["configuration"]["virtualMachineOptions"] = {"disableSerialConsoleLog": False}
    assert normalize_installed_profile(changed)["profile"] == default_launcher_profile()
    changed["spec"]["configuration"]["virtualMachineOptions"] = {"disableSerialConsoleLog": True}
    assert normalize_installed_profile(changed)["profile"]["disableSerialConsoleLog"] is True
    changed = deepcopy(cr)
    changed["spec"]["configuration"]["developerConfiguration"] = {"useEmulation": True}
    with pytest.raises(ResourceAdmissionError):
        normalize_installed_profile(changed)


def test_unmodeled_profile_features_and_ephemeral_request_refuse():
    profile = default_launcher_profile()
    profile["features"]["containerDisk"] = True
    with pytest.raises(ResourceAdmissionError):
        predict_launcher(profile, guest_vcpus=2, guest_memory_bytes=512 * 1024**2)
    profile = default_launcher_profile()
    profile["features"]["perVmEphemeralStorageBytes"] = 1
    with pytest.raises(ResourceAdmissionError):
        predict_launcher(profile, guest_vcpus=2, guest_memory_bytes=512 * 1024**2)


@pytest.mark.parametrize(
    "guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes",
    [(2, 4294967296, 505, 4619374272),
     (12, 25769803776, 3005, 26178096832),
     (16, 107374182400, 4005, 107816029888)],
)
def test_kubevirt_184_matches_main_dev_launcher_requests(
    guest_vcpus, guest_memory_bytes, cpu_millicores, memory_bytes,
):
    # Captured main-dev 1.8.4 launchers, 2026-09-24, including native console.
    profile = default_launcher_profile()
    profile.update(kubevirtVersion="v1.8.4",
                   costAlgorithm="kubevirt-v1.8.4-amd64-ordinary-pvc-v1",
                   cpuAllocationRatio=4)
    assert predict_launcher(
        profile, guest_vcpus=guest_vcpus, guest_memory_bytes=guest_memory_bytes,
    ) == ResourceVector(cpu_millicores, memory_bytes, 1, 50000000, 1, 1)


def test_installed_184_requires_matching_settled_versions_and_kvm():
    cr = installed_cr()
    cr["status"].update(targetKubeVirtVersion="v1.8.4", observedKubeVirtVersion="v1.8.4")
    cr["spec"]["configuration"]["developerConfiguration"] = {"cpuAllocationRatio": 4}
    observation = normalize_installed_profile(cr)
    assert observation["profile"]["kubevirtVersion"] == "v1.8.4"
    assert observation["profile"]["costAlgorithm"] == "kubevirt-v1.8.4-amd64-ordinary-pvc-v1"
    assert observation["profile"]["cpuAllocationRatio"] == 4
    for field, value in [("targetKubeVirtVersion", "v1.6.6"),
                         ("observedKubeVirtVersion", "v1.8.5")]:
        changed = deepcopy(cr)
        changed["status"][field] = value
        with pytest.raises(ResourceAdmissionError):
            normalize_installed_profile(changed)
    changed = deepcopy(cr)
    changed["spec"]["configuration"]["hypervisors"] = [{"name": "hyperv"}]
    with pytest.raises(ResourceAdmissionError):
        normalize_installed_profile(changed)


@pytest.mark.parametrize("version", ["v1.6.6", "v1.8.4", "v1.8.5"])
def test_profile_version_cannot_relabel_another_cost_algorithm(version):
    profile = default_launcher_profile()
    profile["kubevirtVersion"] = version
    profile["costAlgorithm"] = (
        "kubevirt-v1.8.4-amd64-ordinary-pvc-v1"
        if version != "v1.8.4" else "kubevirt-v1.6.6-amd64-ordinary-pvc-v1"
    )
    with pytest.raises(ResourceAdmissionError):
        predict_launcher(profile, guest_vcpus=2, guest_memory_bytes=4 * 1024**3)
