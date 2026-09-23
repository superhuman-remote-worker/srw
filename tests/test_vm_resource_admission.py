"""Pure admission arithmetic uses the same Kubernetes requests as metering."""

from dataclasses import replace

import pytest

from shared.vm_resource_admission import (
    HostCostPolicy,
    ResourceVector,
    ResourceAdmissionError,
    effective_pod_request,
    scheduled_pod_charge,
    managed_charge,
)
from tests.test_infrastructure_metering_pod_normalization import _pod


def policy():
    # Explicit fixture values, not deployment defaults.
    return HostCostPolicy(1000, 3, 25, 100, 10, 25)


def test_host_cost_rounds_each_component_up_without_floating_point():
    assert policy().cost(2, "1001") == ResourceVector(692, 1124, 1)
    assert policy().cost(3, "1Gi") == ResourceVector(1025, 1076426309, 1)


@pytest.mark.parametrize("value", [True, -1, 1.5, "1", 2**63])
def test_resource_storage_rejects_invalid_integer_values(value):
    with pytest.raises(ResourceAdmissionError):
        ResourceVector(value, 1, 1)
    with pytest.raises(ResourceAdmissionError):
        replace(policy(), fixed_memory_overhead_bytes=value)


@pytest.mark.parametrize("value", [True, -1, 0, 1.5, "1", 2**63])
def test_guest_vcpu_is_a_positive_integer(value):
    with pytest.raises(ResourceAdmissionError):
        policy().cost(value, "1Gi")


@pytest.mark.parametrize("memory", [True, 0, "0", -1, "NaN", "Inf", "1e40", 1.2])
def test_invalid_guest_memory_is_refused(memory):
    with pytest.raises(ResourceAdmissionError):
        policy().cost(1, memory)


def test_policy_digest_is_stable_and_covers_every_cost_input():
    assert policy().digest == HostCostPolicy(1000, 3, 25, 100, 10, 25).digest
    for field in policy().__dataclass_fields__:
        assert (
            replace(policy(), **{field: getattr(policy(), field) + 1}).digest
            != policy().digest
        )
    with pytest.raises(ResourceAdmissionError):
        replace(policy(), cpu_millicores_per_vcpu_denominator=0)


def test_vector_arithmetic_and_independent_dimensions():
    a, b = ResourceVector(100, 1000, 1), ResourceVector(20, 2000, 2)
    assert a + b == ResourceVector(120, 3000, 3)
    assert (a + b) - a == b
    assert a.maximum(b) == ResourceVector(100, 2000, 2)
    assert a.fits(a + b)
    assert not a.fits(ResourceVector(99, 1000, 1))
    assert not a.fits(ResourceVector(100, 999, 1))
    assert not a.fits(ResourceVector(100, 1000, 0))
    with pytest.raises(ResourceAdmissionError):
        a - b
    with pytest.raises(ResourceAdmissionError):
        ResourceVector(2**63 - 1, 0, 0) + a


def test_six_dimensional_vector_charges_each_independent_resource():
    demand = ResourceVector(205, 861277888, 1, 50000000, 1, 1)
    assert demand.to_six_dict() == {
        "cpu_millicores": 205,
        "memory_bytes": 861277888,
        "ephemeral_storage_bytes": 50000000,
        "kvm_devices": 1,
        "tun_devices": 1,
        "vhost_net_devices": 1,
    }
    assert not demand.fits(ResourceVector(205, 861277888, 1, 49999999, 1, 1))
    assert not demand.fits(ResourceVector(205, 861277888, 1, 50000000, 0, 1))
    assert not demand.fits(ResourceVector(205, 861277888, 1, 50000000, 1, 0))
    assert demand + ResourceVector(1, 2, 0, 3, 0, 1) == ResourceVector(
        206, 861277890, 1, 50000003, 1, 2
    )


def test_captured_ordinary_launcher_has_all_six_requests():
    import json
    from pathlib import Path

    fixture = json.loads(
        (
            Path(__file__).parent
            / "fixtures/vm_launcher_cost/kubevirt-1.6.6-amd64-default.json"
        ).read_text()
    )
    assert effective_pod_request(fixture["pod"]) == ResourceVector(
        205, 861277888, 1, 50000000, 1, 1
    )


def test_ephemeral_and_device_peaks_follow_restartable_init_and_overhead():
    pod = _pod(
        containers=[{
            "name": "main",
            "resources": {"requests": {
                "cpu": "100m", "memory": "100",
                "ephemeral-storage": "10M",
                "devices.kubevirt.io/kvm": "1",
                "devices.kubevirt.io/tun": "1",
            }},
        }],
        init_containers=[
            {"name": "sidecar", "restartPolicy": "Always", "resources": {
                "requests": {"ephemeral-storage": "2M", "devices.kubevirt.io/tun": "1"}
            }},
            {"name": "init", "resources": {
                "requests": {"ephemeral-storage": "20M", "devices.kubevirt.io/vhost-net": "2"}
            }},
        ],
        overhead={"ephemeral-storage": "1M"},
    )
    assert effective_pod_request(pod) == ResourceVector(
        100, 100, 1, 23000000, 2, 2
    )


def test_managed_launcher_refuses_unknown_device_even_in_status_allocation():
    pod = _pod()
    pod["status"]["containerStatuses"] = [{
        "name": pod["spec"]["containers"][0]["name"],
        "allocatedResources": {"vendor.example/gpu": "1"},
    }]
    with pytest.raises(ResourceAdmissionError, match="unsupported_launcher_resource"):
        effective_pod_request(pod, managed_launcher=True)


def test_managed_charge_holds_reservation_and_uses_component_maximum():
    reservation = ResourceVector(500, 1000, 1)
    assert managed_charge(reservation, None) == reservation
    assert managed_charge(reservation, ResourceVector(100, 2000, 1)) == ResourceVector(
        500, 2000, 1
    )


def test_shared_normalizer_retains_the_original_public_api():
    from shared.kubernetes_pod_requests import normalize_pod as shared
    from orchestrator.services.infrastructure_metering.collectors.pod_normalization import (
        normalize_pod as metering,
    )

    assert shared is metering


def test_pod_uses_shared_init_sidecar_overhead_cpu_and_memory_and_kvm():
    pod = _pod(
        containers=[
            {
                "name": "main",
                "resources": {
                    "requests": {
                        "cpu": "100m",
                        "memory": "100",
                        "devices.kubevirt.io/kvm": "1",
                    }
                },
            }
        ],
        init_containers=[
            {
                "name": "sidecar",
                "restartPolicy": "Always",
                "resources": {
                    "requests": {
                        "cpu": "20m",
                        "memory": "20",
                        "devices.kubevirt.io/kvm": "1",
                    }
                },
            },
            {
                "name": "init",
                "resources": {
                    "requests": {
                        "cpu": "500m",
                        "memory": "500",
                        "devices.kubevirt.io/kvm": "3",
                    }
                },
            },
        ],
        overhead={"cpu": "1m", "memory": "1"},
    )
    assert effective_pod_request(pod) == ResourceVector(521, 521, 4)
    assert scheduled_pod_charge(pod) == ResourceVector(521, 521, 4)
    pod["metadata"]["deletionTimestamp"] = "2026-08-05T09:00:00Z"
    assert scheduled_pod_charge(pod) == ResourceVector(521, 521, 4)
    pod["status"]["phase"] = "Succeeded"
    assert scheduled_pod_charge(pod) == ResourceVector(0, 0, 0)
    pod["status"]["phase"] = "Pending"
    del pod["spec"]["nodeName"]
    pod["status"]["conditions"][0]["status"] = "False"
    assert scheduled_pod_charge(pod) == ResourceVector(0, 0, 0)


@pytest.mark.parametrize("value", ["0.5", "-1", True, "NaN", "1e30"])
def test_kvm_extended_resource_requires_bounded_whole_devices(value):
    pod = _pod()
    pod["spec"]["containers"][0]["resources"]["requests"]["devices.kubevirt.io/kvm"] = (
        value
    )
    with pytest.raises(ResourceAdmissionError):
        effective_pod_request(pod)


def test_invalid_or_uncertain_pod_request_cannot_grant_capacity():
    pod = _pod()
    pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "broken"
    with pytest.raises(ResourceAdmissionError):
        effective_pod_request(pod)


def test_scheduled_condition_without_node_does_not_become_free_capacity():
    pod = _pod()
    del pod["spec"]["nodeName"]
    with pytest.raises(ResourceAdmissionError, match="pod_node_unproven"):
        scheduled_pod_charge(pod)


def test_status_aware_resize_and_pod_level_cpu_requests_share_metering_semantics():
    pod = _pod()
    pod["status"]["containerStatuses"] = [
        {
            "name": "app",
            "resources": {"requests": {"cpu": "200m", "memory": "1Gi"}},
            "allocatedResources": {"cpu": "300m", "memory": "1Gi"},
        }
    ]
    assert effective_pod_request(pod) == ResourceVector(300, 1024**3, 0)
    pod["spec"]["resources"] = {"requests": {"cpu": "2"}}
    with pytest.raises(ResourceAdmissionError, match="pod_request_unproven"):
        effective_pod_request(pod)
    pod["status"]["resources"] = {"requests": {"cpu": "2"}}
    pod["status"]["allocatedResources"] = {"cpu": "2"}
    assert effective_pod_request(pod) == ResourceVector(2000, 1024**3, 0)
    del pod["spec"]["resources"]
    del pod["status"]["containerStatuses"]
    del pod["status"]["resources"]
    del pod["status"]["allocatedResources"]
    pod["status"]["conditions"].append(
        {"type": "PodResizeInProgress", "status": "True"}
    )
    with pytest.raises(ResourceAdmissionError, match="pod_request_unproven"):
        effective_pod_request(pod)
