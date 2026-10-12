"""Regression from an actual local KubeVirt launcher, including native sidecars."""

from dataclasses import replace
import json
from pathlib import Path

from shared.vm_resource_admission import (
    ResourceVector,
    WholeLauncherHostCostPolicy,
    effective_pod_request,
    managed_charge,
)
from shared.vm_resource_policy import (
    parse_host_cost_policy,
    parse_whole_launcher_host_cost_policy,
)

# The measured compute container's non-compute requests: 50M ephemeral
# storage plus one each of the KubeVirt kvm, tun and vhost-net devices.
MEASURED_EPHEMERAL_STORAGE_BYTES = 50000000


def measurement():
    return json.loads(
        (
            Path(__file__).parent
            / "fixtures/vm_launcher_cost/kubevirt-1.6.6-amd64-default.json"
        ).read_text()
    )


def whole_launcher(base):
    """The versioned (v2) reserve for the measured launcher's other requests."""
    return WholeLauncherHostCostPolicy(
        base,
        ephemeral_storage_reserve_bytes=MEASURED_EPHEMERAL_STORAGE_BYTES,
        kvm_devices=1,
        tun_devices=1,
        vhost_net_devices=1,
    )


def test_measured_launcher_includes_console_log_restartable_init_request():
    data = measurement()
    actual = effective_pod_request(data["pod"])
    assert actual == ResourceVector(
        205, 861277888, 1, MEASURED_EPHEMERAL_STORAGE_BYTES, 1, 1
    )
    # Frozen v1 and inventory-protocol-1 payloads keep exactly three fields.
    assert actual.to_dict() == data["expected"]
    policy = parse_host_cost_policy(data["policy"])
    # The v1 estimate predicts the measured compute dimensions exactly ...
    assert policy.cost(2, "512Mi").to_dict() == actual.to_dict()
    # ... and the whole-launcher v2 policy covers the complete measured Pod.
    whole = parse_whole_launcher_host_cost_policy(
        {
            **data["policy"],
            "version": 2,
            "ephemeralStorageReserveBytes": MEASURED_EPHEMERAL_STORAGE_BYTES,
            "kvmDevices": 1,
            "tunDevices": 1,
            "vhostNetDevices": 1,
        }
    )
    assert whole == whole_launcher(policy)
    assert whole.cost(2, "512Mi") == actual


def test_compute_only_estimate_cannot_cover_whole_measured_launcher():
    data = measurement()
    actual = effective_pod_request(data["pod"])
    policy = parse_host_cost_policy(data["policy"])
    # Even with the storage/device reserve in place, a compute-only base
    # estimate misses exactly the console-log sidecar's CPU and memory.
    compute_only = whole_launcher(
        replace(
            policy,
            launcher_cpu_overhead_millicores=0,
            fixed_memory_overhead_bytes=260 * 1024**2,
        )
    ).cost(2, "512Mi")
    assert actual - compute_only == ResourceVector(5, 35000000, 0)
    assert not actual.fits(compute_only)
    assert managed_charge(compute_only, actual) == actual
