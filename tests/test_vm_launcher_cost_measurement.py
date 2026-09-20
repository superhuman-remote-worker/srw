"""Regression from an actual local KubeVirt launcher, including native sidecars."""

from dataclasses import replace
import json
from pathlib import Path

from shared.vm_resource_admission import (
    ResourceVector,
    effective_pod_request,
    managed_charge,
)
from shared.vm_resource_policy import parse_host_cost_policy


def measurement():
    return json.loads(
        (
            Path(__file__).parent
            / "fixtures/vm_launcher_cost/kubevirt-1.6.6-amd64-default.json"
        ).read_text()
    )


def test_measured_launcher_includes_console_log_restartable_init_request():
    data = measurement()
    actual = effective_pod_request(data["pod"])
    assert actual == ResourceVector(205, 861277888, 1)
    assert actual.to_dict() == data["expected"]
    policy = parse_host_cost_policy(data["policy"])
    assert policy.cost(2, "512Mi") == actual


def test_compute_only_estimate_cannot_cover_whole_measured_launcher():
    data = measurement()
    actual = effective_pod_request(data["pod"])
    policy = parse_host_cost_policy(data["policy"])
    compute_only = replace(
        policy,
        launcher_cpu_overhead_millicores=0,
        fixed_memory_overhead_bytes=260 * 1024**2,
    ).cost(2, "512Mi")
    assert actual - compute_only == ResourceVector(5, 35000000, 0)
    assert not actual.fits(compute_only)
    assert managed_charge(compute_only, actual) == actual
