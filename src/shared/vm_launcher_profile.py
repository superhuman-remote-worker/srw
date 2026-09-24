"""Closed KubeVirt v1.6.6 and v1.8.4 amd64 ordinary PVC launcher profiles.

This is a prediction from the pinned renderer, not installation evidence or a
reservation. The controller separately attests the installed KubeVirt CR and
the eventual exact admitted Pod. Unsupported feature branches remain held.
"""

from copy import deepcopy
from uuid import UUID

from shared.kubernetes_quantities import (
    QuantityNormalizationError,
    normalize_byte_quantity,
    normalize_cpu_millicores,
)
from shared.vm_resource_admission import (
    ResourceAdmissionError,
    ResourceVector,
    _ceil_div,
    _integer,
    effective_pod_request,
)


LAUNCHER_ALGORITHM = "kubevirt-v1.6.6-amd64-ordinary-pvc-v1"
_LAUNCHER_ALGORITHMS = {
    "v1.6.6": LAUNCHER_ALGORITHM,
    "v1.8.4": "kubevirt-v1.8.4-amd64-ordinary-pvc-v1",
}
_SUPPORT_TYPES = (
    "guest-console-log", "container-disk", "sidecar", "virtiofs",
    "hotplug-disk", "vmexport",
)
_FEATURES = {
    "graphicsAttached": True,
    "serialConsoleAttached": True,
    "containerDisk": False,
    "kernelBoot": False,
    "virtiofs": False,
    "hookSidecar": False,
    "dedicatedCpu": False,
    "guaranteedCpu": False,
    "hugepages": False,
    "supplementalIoThreads": False,
    "hostDevices": False,
    "networkBindingResources": False,
    "sev": False,
    "tpm": False,
    "downwardMetrics": False,
    "customProbes": False,
    "perVmEphemeralStorageBytes": 0,
}


def default_launcher_profile(kubevirt_version="v1.6.6"):
    if type(kubevirt_version) is not str or kubevirt_version not in _LAUNCHER_ALGORITHMS:
        raise ResourceAdmissionError("unsupported_launcher_profile")
    support = {kind: None for kind in _SUPPORT_TYPES}
    support["guest-console-log"] = {
        "requests": {"cpuMillicores": 5, "memoryBytes": 35000000},
        "limits": {"cpuMillicores": 15, "memoryBytes": 60000000},
    }
    return {
        "version": 1,
        "kubevirtVersion": kubevirt_version,
        "architecture": "amd64",
        "costAlgorithm": _LAUNCHER_ALGORITHMS[kubevirt_version],
        "network": "pod/masquerade",
        "cpuAllocationRatio": 10,
        "cpuRequestMillicores": 100,
        "memoryOvercommit": 100,
        "additionalGuestMemoryOverheadRatio": "1",
        "useEmulation": False,
        "disableSerialConsoleLog": False,
        "defaultRuntimeClass": None,
        "podOverhead": ResourceVector(0, 0, 0).to_six_dict(),
        "supportContainerResources": support,
        "features": deepcopy(_FEATURES),
    }


def validate_launcher_profile(profile):
    """Return an owned closed profile; allow measured ratio and console inputs."""
    try:
        if not isinstance(profile, dict):
            raise ValueError
        default = default_launcher_profile(profile.get("kubevirtVersion"))
        if not isinstance(profile, dict) or set(profile) != set(default):
            raise ValueError
        for name in (
            "version", "kubevirtVersion", "architecture", "costAlgorithm",
            "network", "additionalGuestMemoryOverheadRatio", "useEmulation",
            "defaultRuntimeClass",
        ):
            if type(profile[name]) is not type(default[name]) or profile[name] != default[name]:
                raise ValueError
        if type(profile["disableSerialConsoleLog"]) is not bool:
            raise ValueError
        for name in ("cpuAllocationRatio", "cpuRequestMillicores", "memoryOvercommit"):
            _integer(profile[name], positive=True)
        if profile["memoryOvercommit"] < 10:
            raise ValueError
        if profile["features"] != _FEATURES or any(
            type(profile["features"].get(key)) is not type(value)
            for key, value in _FEATURES.items()
        ):
            raise ValueError
        if ResourceVector.from_six_dict(profile["podOverhead"]) != ResourceVector(0, 0, 0):
            raise ValueError
        support = profile["supportContainerResources"]
        if not isinstance(support, dict) or set(support) != set(_SUPPORT_TYPES):
            raise ValueError
        if any(support[kind] is not None for kind in _SUPPORT_TYPES if kind != "guest-console-log"):
            # Other support containers belong to unmodeled feature branches.
            raise ValueError
        console = support["guest-console-log"]
        if not isinstance(console, dict) or set(console) != {"requests", "limits"}:
            raise ValueError
        for section in ("requests", "limits"):
            values = console[section]
            if not isinstance(values, dict) or set(values) != {"cpuMillicores", "memoryBytes"}:
                raise ValueError
            _integer(values["cpuMillicores"], positive=True)
            _integer(values["memoryBytes"], positive=True)
        if any(
            console["requests"][name] > console["limits"][name]
            for name in ("cpuMillicores", "memoryBytes")
        ):
            raise ValueError
        return deepcopy(profile)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise ResourceAdmissionError("unsupported_launcher_profile") from None


def predict_launcher(profile, *, guest_vcpus, guest_memory_bytes):
    """Recreate an explicitly supported renderer, then aggregate Pod requests."""
    profile = validate_launcher_profile(profile)
    _integer(guest_vcpus, positive=True)
    _integer(guest_memory_bytes, positive=True)
    # The static SRW VMI has guest memory but no pre-render resource request.
    # GetMemoryOverhead therefore has no page-table request component. Its
    # supported amd64 branch adds 220Mi fixed, 8Mi/vCPU, 8Mi IO and 32Mi video.
    # In v1.8.4 this branch moved to pkg/hypervisor/kvm/hypervisorbackend.go;
    # the ordinary PVC/KVM branch retains the same resource calculation.
    memory_overhead = (220 + 8 * guest_vcpus + 8 + 32) * 1024**2
    memory_request = (
        guest_memory_bytes * 100 // profile["memoryOvercommit"]
        + memory_overhead
    )
    # KubeVirt defaults.go inserts cpuRequest only when it differs from 100m.
    cpu_request = (
        profile["cpuRequestMillicores"]
        if profile["cpuRequestMillicores"] != 100
        else _ceil_div(guest_vcpus * 1000, profile["cpuAllocationRatio"])
    )
    compute = {
        "name": "compute",
        "resources": {
            "requests": {
                "cpu": f"{cpu_request}m",
                "memory": str(memory_request),
                "ephemeral-storage": "50M",
                "devices.kubevirt.io/kvm": "1",
                "devices.kubevirt.io/tun": "1",
                "devices.kubevirt.io/vhost-net": "1",
            }
        },
    }
    init = []
    if not profile["disableSerialConsoleLog"]:
        requests = profile["supportContainerResources"]["guest-console-log"]["requests"]
        init.append({
            "name": "guest-console-log",
            "restartPolicy": "Always",
            "resources": {"requests": {
                "cpu": f"{requests['cpuMillicores']}m",
                "memory": str(requests["memoryBytes"]),
            }},
        })
    overhead = profile["podOverhead"]
    pod = {
        "metadata": {
            "uid": "00000000-0000-4000-8000-000000000001",
            "name": "predicted-launcher",
            "namespace": "prediction",
        },
        "spec": {
            "containers": [compute],
            "initContainers": init,
            "overhead": {
                "cpu": f"{overhead['cpu_millicores']}m",
                "memory": str(overhead["memory_bytes"]),
                "ephemeral-storage": str(overhead["ephemeral_storage_bytes"]),
            },
        },
        "status": {"phase": "Pending"},
    }
    return effective_pod_request(pod, managed_launcher=True)


def normalize_installed_profile(raw, *, namespace="kubevirt", name="kubevirt"):
    """Project only cost-relevant KubeVirt CR fields and settled identity."""
    try:
        if not isinstance(raw, dict):
            raise ValueError
        meta, spec, status = raw["metadata"], raw["spec"], raw["status"]
        uid = meta["uid"]
        if (
            type(uid) is not str or str(UUID(uid)) != uid
            or meta["namespace"] != namespace or meta["name"] != name
            or type(meta["generation"]) is not int or meta["generation"] < 1
            or meta.get("deletionTimestamp") is not None
            or status["phase"] != "Deployed"
            or type(status["observedGeneration"]) is not int
            or status["observedGeneration"] != meta["generation"]
            or status["targetKubeVirtVersion"] != status["observedKubeVirtVersion"]
            or not isinstance(status["targetDeploymentID"], str)
            or not status["targetDeploymentID"]
            or status["targetDeploymentID"] != status["observedDeploymentID"]
        ):
            raise ValueError
        config = spec.get("configuration", {})
        if not isinstance(config, dict):
            raise ValueError
        developer = config.get("developerConfiguration", {})
        if (
            not isinstance(developer, dict)
            or developer.get("featureGates", []) != []
            or config.get("defaultRuntimeClass") is not None
            or config.get("autoCPULimitNamespaceLabelSelector") is not None
            or config.get("network") is not None
            or config.get("hypervisors", []) != []
        ):
            raise ValueError
        profile = default_launcher_profile(status["observedKubeVirtVersion"])
        profile["cpuAllocationRatio"] = developer.get("cpuAllocationRatio", 10)
        profile["memoryOvercommit"] = developer.get("memoryOvercommit", 100)
        profile["useEmulation"] = developer.get("useEmulation", False)
        if "cpuRequest" in config:
            profile["cpuRequestMillicores"] = normalize_cpu_millicores(config["cpuRequest"]).normalized_value
        ratio = config.get("additionalGuestMemoryOverheadRatio")
        if ratio is not None:
            profile["additionalGuestMemoryOverheadRatio"] = ratio
        options = config.get("virtualMachineOptions", {})
        if not isinstance(options, dict):
            raise ValueError
        disable_log = options.get("disableSerialConsoleLog", False)
        if type(disable_log) is not bool:
            raise ValueError
        profile["disableSerialConsoleLog"] = disable_log
        overrides = config.get("supportContainerResources", [])
        if not isinstance(overrides, list):
            raise ValueError
        seen = set()
        for item in overrides:
            kind = item["type"]
            if kind not in _SUPPORT_TYPES or kind in seen or set(item) != {"type", "resources"}:
                raise ValueError
            seen.add(kind)
            if kind != "guest-console-log":
                raise ValueError
            resources = item["resources"]
            if not isinstance(resources, dict) or set(resources) - {"requests", "limits"}:
                raise ValueError
            for section in ("requests", "limits"):
                if set(resources.get(section, {})) - {"cpu", "memory"}:
                    raise ValueError
                for source, target, normalizer in (
                    ("cpu", "cpuMillicores", normalize_cpu_millicores),
                    ("memory", "memoryBytes", normalize_byte_quantity),
                ):
                    if source in resources.get(section, {}):
                        profile["supportContainerResources"][kind][section][target] = normalizer(
                            resources[section][source]
                        ).normalized_value
        profile = validate_launcher_profile(profile)
        return {
            "uid": uid,
            "namespace": namespace,
            "name": name,
            "generation": meta["generation"],
            "observedGeneration": status["observedGeneration"],
            "targetVersion": status["targetKubeVirtVersion"],
            "observedVersion": status["observedKubeVirtVersion"],
            "targetDeploymentID": status["targetDeploymentID"],
            "observedDeploymentID": status["observedDeploymentID"],
            "profile": profile,
        }
    except (ValueError, TypeError, KeyError, AttributeError, QuantityNormalizationError):
        raise ResourceAdmissionError("installed_launcher_unproven") from None
