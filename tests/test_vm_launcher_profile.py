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
