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
TUN_RESOURCE = "devices.kubevirt.io/tun"
VHOST_NET_RESOURCE = "devices.kubevirt.io/vhost-net"
EPHEMERAL_RESOURCE = "ephemeral-storage"
_SUPPORTED_LAUNCHER_RESOURCES = frozenset(
    {"cpu", "memory", EPHEMERAL_RESOURCE, KVM_RESOURCE, TUN_RESOURCE, VHOST_NET_RESOURCE}
)
HOST_COST_ALGORITHM = "srw-vm-host-cost-v1"
WHOLE_LAUNCHER_HOST_COST_ALGORITHM = "srw-vm-host-cost-v2"


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
    ephemeral_storage_bytes: int = 0
    tun_devices: int = 0
    vhost_net_devices: int = 0

    def __post_init__(self):
        for value in self.components:
            _integer(value)

    @property
    def components(self):
        return (
            self.cpu_millicores,
            self.memory_bytes,
            self.kvm_devices,
            self.ephemeral_storage_bytes,
            self.tun_devices,
            self.vhost_net_devices,
        )

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
        # Frozen v1/v2 and inventory-protocol-1 payloads have exactly three
        # fields. New versioned callers must explicitly ask for all six.
        return {
            "cpu_millicores": self.cpu_millicores,
            "memory_bytes": self.memory_bytes,
            "kvm_devices": self.kvm_devices,
        }

    def to_six_dict(self):
        return asdict(self)

    @classmethod
    def from_six_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {
            "cpu_millicores", "memory_bytes", "ephemeral_storage_bytes",
            "kvm_devices", "tun_devices", "vhost_net_devices",
        }:
            raise ResourceAdmissionError("invalid_resource_vector")
        return cls(**value)


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


@dataclass(frozen=True, slots=True)
class WholeLauncherHostCostPolicy:
    """Operator reserve, independent of the installed launcher prediction."""

    base: HostCostPolicy
    ephemeral_storage_reserve_bytes: int
    kvm_devices: int
    tun_devices: int
    vhost_net_devices: int

    def __post_init__(self):
        for value in (
            self.ephemeral_storage_reserve_bytes,
            self.kvm_devices,
            self.tun_devices,
            self.vhost_net_devices,
        ):
            _integer(value, positive=True)

    @property
    def digest(self):
        encoded = json.dumps(
            {
                "algorithm": WHOLE_LAUNCHER_HOST_COST_ALGORITHM,
                "base": asdict(self.base),
                "ephemeral_storage_reserve_bytes": self.ephemeral_storage_reserve_bytes,
                "kvm_devices": self.kvm_devices,
                "tun_devices": self.tun_devices,
                "vhost_net_devices": self.vhost_net_devices,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def cost(self, guest_vcpus, guest_memory):
        base = self.base.cost(guest_vcpus, guest_memory)
        return ResourceVector(
            base.cpu_millicores,
            base.memory_bytes,
            self.kvm_devices,
            self.ephemeral_storage_reserve_bytes,
            self.tun_devices,
            self.vhost_net_devices,
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


def _resource_value(value, resource):
    if resource == EPHEMERAL_RESOURCE:
        try:
            return _integer(normalize_byte_quantity(value).normalized_value)
        except QuantityNormalizationError:
            raise ResourceAdmissionError("invalid_ephemeral_storage") from None
    try:
        parsed = parse_kubernetes_quantity(value, resource=resource).value
    except QuantityNormalizationError:
        raise ResourceAdmissionError("invalid_extended_device") from None
    if parsed > SIGNED_BIGINT_MAX or parsed != parsed.to_integral_value():
        raise ResourceAdmissionError("invalid_extended_device")
    return _integer(int(parsed))


def _request_value(resources, resource):
    requests = _mapping(_mapping(resources).get("requests", {}))
    return _resource_value(requests.get(resource, 0), resource)


def _scalar_request(raw, resource):
    """Extended devices use the scheduler's app/sidecar/init maximum.

    See component-helpers v0.35.0 resource/helpers.go AggregateContainerRequests.
    Extended resources cannot be Pod-level resources or resized; observed status
    disagreement is incomplete evidence, never permission to undercount devices.
    """
    spec = _mapping(raw["spec"])
    status = _mapping(raw.get("status", {}))
    pod_requests = _mapping(_mapping(spec.get("resources", {})).get("requests", {}))
    if resource != EPHEMERAL_RESOURCE and resource in pod_requests:
        raise ResourceAdmissionError("unsupported_pod_level_device")
    statuses = {}
    for name in ("containerStatuses", "initContainerStatuses"):
        for item in status.get(name, []):
            if not isinstance(item, Mapping) or item.get("name") in statuses:
                raise ResourceAdmissionError("invalid_pod_resources")
            statuses[item.get("name")] = item

    def demand(container):
        value = _request_value(container.get("resources", {}), resource)
        current = statuses.get(container.get("name"), {})
        if "resources" in current and _request_value(current["resources"], resource) != value:
            raise ResourceAdmissionError("unsupported_resource_resize")
        allocated = _mapping(current.get("allocatedResources", {}))
        if resource in allocated and _resource_value(allocated[resource], resource) != value:
            raise ResourceAdmissionError("unsupported_resource_resize")
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
    total = max(total, peak_init)
    if resource in pod_requests:
        total = _resource_value(pod_requests[resource], resource)
        pod_status = _mapping(status.get("resources", {}))
        status_requests = _mapping(pod_status.get("requests", {}))
        if resource in status_requests and _resource_value(status_requests[resource], resource) != total:
            raise ResourceAdmissionError("unsupported_resource_resize")
        allocated = _mapping(status.get("allocatedResources", {}))
        if resource in allocated and _resource_value(allocated[resource], resource) != total:
            raise ResourceAdmissionError("unsupported_resource_resize")
    return _integer(total + _resource_value(_mapping(spec.get("overhead", {})).get(resource, 0), resource))


def _launcher_resources_supported(raw):
    spec = _mapping(raw["spec"])
    status = _mapping(raw.get("status", {}))
    def supported(resources):
        resources = _mapping(resources)
        for source in ("requests", "limits"):
            if set(_mapping(resources.get(source, {}))) - _SUPPORTED_LAUNCHER_RESOURCES:
                raise ResourceAdmissionError("unsupported_launcher_resource")
    resources = [spec.get("resources", {}), *(
        container.get("resources", {})
        for kind in ("containers", "initContainers")
        for container in spec.get(kind, [])
    )]
    for entry in resources:
        supported(entry)
    supported(status.get("resources", {}))
    if set(_mapping(status.get("allocatedResources", {}))) - _SUPPORTED_LAUNCHER_RESOURCES:
        raise ResourceAdmissionError("unsupported_launcher_resource")
    for kind in ("containerStatuses", "initContainerStatuses"):
        for item in status.get(kind, []):
            item = _mapping(item)
            supported(item.get("resources", {}))
            if set(_mapping(item.get("allocatedResources", {}))) - _SUPPORTED_LAUNCHER_RESOURCES:
                raise ResourceAdmissionError("unsupported_launcher_resource")
    if set(_mapping(spec.get("overhead", {}))) - _SUPPORTED_LAUNCHER_RESOURCES:
        raise ResourceAdmissionError("unsupported_launcher_resource")


def _pod_resources(raw, *, managed_launcher=False):
    try:
        pod = normalize_pod(raw)
    except PodNormalizationError:
        raise ResourceAdmissionError("invalid_pod_identity") from None
    if managed_launcher:
        _launcher_resources_supported(raw)
    request = pod.effective_request
    if (
        not pod.valid_for_metering
        or request is None
        or request.capacity_quality != "exact"
    ):
        raise ResourceAdmissionError("pod_request_unproven")
    return pod, ResourceVector(
        request.cpu_millicores,
        request.memory_bytes,
        _scalar_request(raw, KVM_RESOURCE),
        _scalar_request(raw, EPHEMERAL_RESOURCE),
        _scalar_request(raw, TUN_RESOURCE),
        _scalar_request(raw, VHOST_NET_RESOURCE),
    )


def effective_pod_request(raw, *, managed_launcher=False) -> ResourceVector:
    return _pod_resources(raw, managed_launcher=managed_launcher)[1]


def scheduled_pod_charge(raw) -> ResourceVector:
    pod, request = _pod_resources(raw)
    if pod.lifecycle.terminal or not pod.lifecycle.scheduled:
        return ResourceVector(0, 0, 0)
    if not pod.lifecycle.node_name:
        raise ResourceAdmissionError("pod_node_unproven")
    # deletionTimestamp does not mean that compute is physically absent.
    return request
