"""Exact VM host cost and Pod occupancy, without I/O or admission authority.

All policy values are explicit operator input. Kubernetes CPU/memory requests
reuse the metering normalizer; keeping that implementation shared also lets the
controller image normalize inventory without importing the orchestrator.
"""

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import hashlib
import json

from shared.kubernetes_pod_requests import normalize_pod, PodNormalizationError
from shared.kubernetes_quantities import (
    SIGNED_BIGINT_MAX,
    QuantityNormalizationError,
    normalize_byte_quantity,
    parse_kubernetes_quantity,
)


KVM_RESOURCE = "devices.kubevirt.io/kvm"
HOST_COST_ALGORITHM = "srw-vm-host-cost-v1"


class ResourceAdmissionError(ValueError):
    """Bounded reason code; never include raw Kubernetes or operator input."""


def _integer(value, *, positive=False):
    if type(value) is not int or not (int(positive) <= value <= SIGNED_BIGINT_MAX):
        raise ResourceAdmissionError("invalid_resource_integer")
    return value


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


@dataclass(frozen=True, slots=True)
class ResourceVector:
    cpu_millicores: int
    memory_bytes: int
    kvm_devices: int

    def __post_init__(self):
        for value in self.components:
            _integer(value)

    @property
    def components(self):
        return self.cpu_millicores, self.memory_bytes, self.kvm_devices

    def __add__(self, other):
        return ResourceVector(
            *(a + b for a, b in zip(self.components, other.components))
        )

    def __sub__(self, other):
        # Underflow must not wrap or silently authorize another reservation.
        return ResourceVector(
            *(a - b for a, b in zip(self.components, other.components))
        )

    def maximum(self, other):
        return ResourceVector(
            *(max(a, b) for a, b in zip(self.components, other.components))
        )

    def fits(self, available):
        return all(a <= b for a, b in zip(self.components, available.components))

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HostCostPolicy:
    cpu_millicores_per_vcpu_numerator: int
    cpu_millicores_per_vcpu_denominator: int
    launcher_cpu_overhead_millicores: int
    fixed_memory_overhead_bytes: int
    per_vcpu_memory_overhead_bytes: int
    memory_overhead_basis_points: int

    def __post_init__(self):
        for name, value in asdict(self).items():
            _integer(value, positive=name.startswith("cpu_millicores_per_vcpu_"))

    @property
    def digest(self):
        encoded = json.dumps(
            {"algorithm": HOST_COST_ALGORITHM, **asdict(self)},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def cost(self, guest_vcpus: int, guest_memory) -> ResourceVector:
        _integer(guest_vcpus, positive=True)
        try:
            memory = normalize_byte_quantity(guest_memory).normalized_value
        except QuantityNormalizationError:
            raise ResourceAdmissionError("invalid_guest_memory") from None
        _integer(memory, positive=True)
        return ResourceVector(
            _ceil_div(
                guest_vcpus * self.cpu_millicores_per_vcpu_numerator,
                self.cpu_millicores_per_vcpu_denominator,
            )
            + self.launcher_cpu_overhead_millicores,
            memory
            + self.fixed_memory_overhead_bytes
            + guest_vcpus * self.per_vcpu_memory_overhead_bytes
            + _ceil_div(memory * self.memory_overhead_basis_points, 10000),
            1,
        )


def managed_charge(reserved: ResourceVector, observed: ResourceVector | None):
    """For an already authenticated exact launcher, charge once, conservatively.

    The caller must prove the Pod UID/VMI owner and reservation/generation binding
    before removing that Pod from external occupancy. Absence holds the reserve.
    """
    return reserved if observed is None else reserved.maximum(observed)


def _mapping(value):
    if not isinstance(value, Mapping):
        raise ResourceAdmissionError("invalid_pod_resources")
    return value


def _devices(resources):
    requests = _mapping(_mapping(resources).get("requests", {}))
    return normalize_kvm_devices(requests.get(KVM_RESOURCE, 0))


def normalize_kvm_devices(value):
    try:
        value = parse_kubernetes_quantity(value, resource=KVM_RESOURCE).value
    except QuantityNormalizationError:
        raise ResourceAdmissionError("invalid_kvm_devices") from None
    if value > SIGNED_BIGINT_MAX or value != value.to_integral_value():
        raise ResourceAdmissionError("invalid_kvm_devices")
    return _integer(int(value))


def _kvm_request(raw):
    """Extended devices use the scheduler's app/sidecar/init maximum.

    See component-helpers v0.35.0 resource/helpers.go AggregateContainerRequests.
    Extended resources cannot be Pod-level resources or resized; observed status
    disagreement is incomplete evidence, never permission to undercount devices.
    """
    spec = _mapping(raw["spec"])
    status = _mapping(raw.get("status", {}))
    if _devices(spec.get("resources", {})):
        raise ResourceAdmissionError("unsupported_pod_level_kvm")
    statuses = {}
    for name in ("containerStatuses", "initContainerStatuses"):
        for item in status.get(name, []):
            if not isinstance(item, Mapping) or item.get("name") in statuses:
                raise ResourceAdmissionError("invalid_pod_resources")
            statuses[item.get("name")] = item

    def demand(container):
        value = _devices(container.get("resources", {}))
        current = statuses.get(container.get("name"), {})
        if "resources" in current and _devices(current["resources"]) != value:
            raise ResourceAdmissionError("unsupported_kvm_resize")
        allocated = _mapping(current.get("allocatedResources", {}))
        if KVM_RESOURCE in allocated and _devices({"requests": allocated}) != value:
            raise ResourceAdmissionError("unsupported_kvm_resize")
        return value

    total = sum(demand(container) for container in spec["containers"])
    sidecars = peak_init = 0
    for container in spec.get("initContainers", []):
        current = demand(container)
        if container.get("restartPolicy") == "Always":
            sidecars += current
            total += current
            current = sidecars
        else:
            current += sidecars
        peak_init = max(peak_init, current)
    return _integer(
        max(total, peak_init) + _devices({"requests": spec.get("overhead", {})})
    )


def _pod_resources(raw):
    try:
        pod = normalize_pod(raw)
    except PodNormalizationError:
        raise ResourceAdmissionError("invalid_pod_identity") from None
    request = pod.effective_request
    if (
        not pod.valid_for_metering
        or request is None
        or request.capacity_quality != "exact"
    ):
        raise ResourceAdmissionError("pod_request_unproven")
    return pod, ResourceVector(
        request.cpu_millicores, request.memory_bytes, _kvm_request(raw)
    )


def effective_pod_request(raw) -> ResourceVector:
    return _pod_resources(raw)[1]


def scheduled_pod_charge(raw) -> ResourceVector:
    pod, request = _pod_resources(raw)
    if pod.lifecycle.terminal or not pod.lifecycle.scheduled:
        return ResourceVector(0, 0, 0)
    if not pod.lifecycle.node_name:
        raise ResourceAdmissionError("pod_node_unproven")
    # deletionTimestamp does not mean that compute is physically absent.
    return request
