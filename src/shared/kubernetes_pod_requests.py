"""Pure normalization of admitted Kubernetes Pods for allocation metering.

Only request-related fields and a deliberately small attribution/lifecycle
allowlist leave this module. Raw Pods can contain environment variables,
Secret references, commands, images, annotations, and status messages; none of
those are copied into the normalized inventory item.

The request algorithm is pinned to Kubernetes component-helpers v0.35.0
(``PodRequests`` at tag commit ``71c97d70a960``), including restartable init
containers, per-resource Pod-level fallback, overhead, and status-aware
in-place resize behavior.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
from typing import Any, Literal

from shared.kubernetes_quantities import (
    SIGNED_BIGINT_MAX,
    NormalizedQuantity,
    QuantityNormalizationError,
    normalize_byte_quantity,
    normalize_cpu_millicores,
)


POD_REQUESTS_ALGORITHM = "pod-requests-k8s-v1.35-component-helpers-71c97d70a960"

ALLOWED_POD_LABELS = frozenset(
    {
        "srw/job-id",
        "srw/thread-id",
        "srw.io/thread-id",
        "srw/component",
        "srw.io/component",
        "srw/purpose",
        "srw/managed-by",
        "app",
        "app.kubernetes.io/name",
        "app.kubernetes.io/instance",
        "app.kubernetes.io/component",
    }
)

_MISSING = object()
_TERMINAL_PHASES = frozenset({"Succeeded", "Failed"})
_RESOURCES = ("cpu", "memory")


class PodNormalizationError(ValueError):
    """A fatal error for which exact Pod identity cannot be trusted."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class PodLifecycleState(StrEnum):
    UNSCHEDULED = "unscheduled"
    ACTIVE = "active"
    TERMINAL = "terminal"


@dataclass(frozen=True, slots=True)
class PodDiagnostic:
    """Sanitized normalization evidence safe for durable JSONB storage."""

    code: str
    path: str
    resource: Literal["cpu", "memory"] | None = None
    original_value: str | None = None
    covered_by_pod_level: bool | None = None

    def to_dict(self, *, include_original: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "path": self.path}
        if self.resource is not None:
            result["resource"] = self.resource
        if include_original and self.original_value is not None:
            result["original_value"] = self.original_value
        if self.covered_by_pod_level is not None:
            result["covered_by_pod_level"] = self.covered_by_pod_level
        return result


@dataclass(frozen=True, slots=True)
class PodOwnerReference:
    kind: str
    uid: str
    name: str

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "uid": self.uid, "name": self.name}


@dataclass(frozen=True, slots=True)
class PodLifecycle:
    """Lifecycle classification; inventory remains authoritative for existence."""

    state: PodLifecycleState
    scheduled: bool
    terminal: bool
    accrues: bool
    phase: str | None
    node_name: str | None
    deletion_requested: bool
    creation_timestamp: str | None
    deletion_timestamp: str | None
    start_time: str | None
    pod_scheduled_status: str | None
    pod_scheduled_transition_time: str | None

    def to_dict(self) -> dict[str, Any]:
        condition: dict[str, str] | None = None
        if self.pod_scheduled_status is not None:
            condition = {"status": self.pod_scheduled_status}
            if self.pod_scheduled_transition_time is not None:
                condition["last_transition_time"] = self.pod_scheduled_transition_time
        return {
            "state": self.state.value,
            "scheduled": self.scheduled,
            "terminal": self.terminal,
            "accrues": self.accrues,
            "phase": self.phase,
            "node_name": self.node_name,
            "deletion_requested": self.deletion_requested,
            "creation_timestamp": self.creation_timestamp,
            "deletion_timestamp": self.deletion_timestamp,
            "start_time": self.start_time,
            "pod_scheduled_condition": condition,
        }


@dataclass(frozen=True, slots=True)
class PodEffectiveRequest:
    """Integer scheduler capacity ready for exact time integration."""

    cpu_millicores: int
    memory_bytes: int
    cpu_source: str
    memory_source: str
    overhead_cpu_millicores: int
    overhead_memory_bytes: int
    capacity_quality: Literal["exact", "resize-status-unavailable"]
    resize_status: str
    status_resources_used: bool
    measurement_algorithm: str = POD_REQUESTS_ALGORITHM

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_millicores": self.cpu_millicores,
            "memory_bytes": self.memory_bytes,
            "cpu_source": self.cpu_source,
            "memory_source": self.memory_source,
            "overhead_cpu_millicores": self.overhead_cpu_millicores,
            "overhead_memory_bytes": self.overhead_memory_bytes,
            "capacity_quality": self.capacity_quality,
            "resize_status": self.resize_status,
            "status_resources_used": self.status_resources_used,
            "measurement_algorithm": self.measurement_algorithm,
        }


@dataclass(frozen=True, slots=True)
class NormalizedPod:
    """Allowlisted Pod observation suitable for inventory JSONB staging."""

    uid: str
    namespace: str
    name: str
    resource_version: str | None
    api_version: str
    labels: dict[str, str]
    owner_references: tuple[PodOwnerReference, ...]
    lifecycle: PodLifecycle
    effective_request: PodEffectiveRequest | None
    request_evidence: dict[str, Any]
    diagnostics: tuple[PodDiagnostic, ...]
    valid_for_metering: bool
    kind: Literal["pod"] = "pod"
    revision_hash: str | None = field(init=False)

    def __post_init__(self) -> None:
        revision_hash = None
        if self.valid_for_metering:
            if self.effective_request is None:
                raise ValueError("a valid Pod requires effective capacity")
            encoded = json.dumps(
                self.revision_payload(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            revision_hash = hashlib.sha256(encoded).hexdigest()
        object.__setattr__(self, "revision_hash", revision_hash)

    def revision_payload(self) -> dict[str, Any]:
        """Return metering-significant state, excluding transport-only churn."""

        diagnostics = sorted(
            (
                diagnostic.to_dict(include_original=False)
                for diagnostic in self.diagnostics
            ),
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
        return {
            "kind": self.kind,
            "api_version": self.api_version,
            "identity": {
                "uid": self.uid,
                "namespace": self.namespace,
                "name": self.name,
                "creation_timestamp": self.lifecycle.creation_timestamp,
            },
            "attribution": {
                "labels": dict(sorted(self.labels.items())),
                "owner_references": [
                    reference.to_dict() for reference in self.owner_references
                ],
            },
            "lifecycle": {
                "scheduled": self.lifecycle.scheduled,
                "terminal": self.lifecycle.terminal,
                "node_name": self.lifecycle.node_name,
                "pod_scheduled_transition_time": (
                    self.lifecycle.pod_scheduled_transition_time
                ),
            },
            "capacity": (
                self.effective_request.to_dict()
                if self.effective_request is not None
                else None
            ),
            "diagnostics": diagnostics,
        }

    def to_db_item(self) -> dict[str, Any]:
        """Return a JSON-serializable, raw-object-free inventory item."""

        return {
            "source_kind": self.kind,
            "api_version": self.api_version,
            "namespace": self.namespace,
            "name": self.name,
            "uid": self.uid,
            "resource_version": self.resource_version,
            "labels": dict(self.labels),
            "owner_references": [
                reference.to_dict() for reference in self.owner_references
            ],
            "lifecycle": self.lifecycle.to_dict(),
            "capacity": (
                self.effective_request.to_dict()
                if self.effective_request is not None
                else None
            ),
            "request_evidence": self.request_evidence,
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "measurement_algorithm": POD_REQUESTS_ALGORITHM,
            "valid_for_metering": self.valid_for_metering,
            "revision_hash": self.revision_hash,
        }


@dataclass(frozen=True, slots=True)
class _Vector:
    cpu: int = 0
    memory: int = 0

    def add(self, other: _Vector) -> _Vector:
        return _Vector(self.cpu + other.cpu, self.memory + other.memory)

    def maximum(self, *others: _Vector) -> _Vector:
        return _Vector(
            max(self.cpu, *(other.cpu for other in others)),
            max(self.memory, *(other.memory for other in others)),
        )

    def resource(self, name: str) -> int:
        return self.cpu if name == "cpu" else self.memory


@dataclass(frozen=True, slots=True)
class _ContainerRequest:
    name: str
    request: _Vector
    restartable_init: bool
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _StatusRequest:
    actuated: _Vector | None
    allocated: _Vector | None
    evidence: dict[str, Any]


class _InvalidPodCapacity(Exception):
    def __init__(self, diagnostic: PodDiagnostic) -> None:
        super().__init__(diagnostic.code)
        self.diagnostic = diagnostic


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _InvalidPodCapacity(PodDiagnostic("invalid-object-shape", path))
    return value


def _sequence(value: Any, path: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise _InvalidPodCapacity(PodDiagnostic("invalid-object-shape", path))
    return value


def _required_identity_text(
    value: Any,
    path: str,
    *,
    maximum: int = 1024,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise PodNormalizationError(f"invalid-{path.replace('.', '-')}")
    return value


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _bounded_metadata_text(
    value: Any,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> bool:
    return (
        isinstance(value, str)
        and (allow_empty or bool(value))
        and len(value) <= maximum
        and not any(character.isspace() for character in value)
    )


def _canonical_timestamp(
    value: Any,
    *,
    path: str,
    diagnostics: list[PodDiagnostic],
) -> str | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise TypeError
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (OverflowError, TypeError, ValueError):
        diagnostics.append(PodDiagnostic("invalid-timestamp", path))
        return None


def _classify_pod_lifecycle(
    raw: Mapping[str, Any],
) -> tuple[PodLifecycle, list[PodDiagnostic]]:
    diagnostics: list[PodDiagnostic] = []
    metadata_value = raw.get("metadata", {})
    spec_value = raw.get("spec", {})
    status_value = raw.get("status", {})
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    spec = spec_value if isinstance(spec_value, Mapping) else {}
    status = status_value if isinstance(status_value, Mapping) else {}

    node_name = _optional_text(spec.get("nodeName"))
    if spec.get("nodeName") not in (None, "") and node_name is None:
        diagnostics.append(PodDiagnostic("invalid-lifecycle-field", "spec.nodeName"))

    phase = _optional_text(status.get("phase"))
    if status.get("phase") not in (None, "") and phase is None:
        diagnostics.append(PodDiagnostic("invalid-lifecycle-field", "status.phase"))

    scheduled_status: str | None = None
    true_transition_times: list[str] = []
    conditions_value = status.get("conditions", [])
    if conditions_value is None:
        conditions_value = []
    if isinstance(conditions_value, Sequence) and not isinstance(
        conditions_value, (str, bytes, bytearray)
    ):
        statuses: set[str] = set()
        for index, condition_value in enumerate(conditions_value):
            if not isinstance(condition_value, Mapping):
                diagnostics.append(
                    PodDiagnostic(
                        "invalid-lifecycle-field", f"status.conditions[{index}]"
                    )
                )
                continue
            if condition_value.get("type") != "PodScheduled":
                continue
            condition_status = condition_value.get("status")
            if condition_status in {"True", "False", "Unknown"}:
                statuses.add(condition_status)
            else:
                diagnostics.append(
                    PodDiagnostic(
                        "invalid-lifecycle-field",
                        f"status.conditions[{index}].status",
                    )
                )
                continue
            if condition_status == "True":
                transition = _canonical_timestamp(
                    condition_value.get("lastTransitionTime"),
                    path=f"status.conditions[{index}].lastTransitionTime",
                    diagnostics=diagnostics,
                )
                if transition is not None:
                    true_transition_times.append(transition)
        if "True" in statuses:
            scheduled_status = "True"
        elif "False" in statuses:
            scheduled_status = "False"
        elif "Unknown" in statuses:
            scheduled_status = "Unknown"
    else:
        diagnostics.append(
            PodDiagnostic("invalid-lifecycle-field", "status.conditions")
        )

    scheduled_by_condition = scheduled_status == "True"
    scheduled = node_name is not None or scheduled_by_condition
    terminal = phase in _TERMINAL_PHASES
    if terminal:
        state = PodLifecycleState.TERMINAL
    elif scheduled:
        state = PodLifecycleState.ACTIVE
    else:
        state = PodLifecycleState.UNSCHEDULED

    creation_timestamp = _canonical_timestamp(
        metadata.get("creationTimestamp"),
        path="metadata.creationTimestamp",
        diagnostics=diagnostics,
    )
    deletion_timestamp = _canonical_timestamp(
        metadata.get("deletionTimestamp"),
        path="metadata.deletionTimestamp",
        diagnostics=diagnostics,
    )
    start_time = _canonical_timestamp(
        status.get("startTime"),
        path="status.startTime",
        diagnostics=diagnostics,
    )
    scheduled_transition = max(true_transition_times) if true_transition_times else None
    if creation_timestamp is not None:
        if (
            scheduled_transition is not None
            and scheduled_transition < creation_timestamp
        ):
            diagnostics.append(
                PodDiagnostic(
                    "unsafe-timestamp-order",
                    "status.conditions.PodScheduled.lastTransitionTime",
                )
            )
            scheduled_transition = None
        if start_time is not None and start_time < creation_timestamp:
            diagnostics.append(
                PodDiagnostic("unsafe-timestamp-order", "status.startTime")
            )
            start_time = None
        if deletion_timestamp is not None and deletion_timestamp < creation_timestamp:
            diagnostics.append(
                PodDiagnostic("unsafe-timestamp-order", "metadata.deletionTimestamp")
            )
            deletion_timestamp = None

    lifecycle = PodLifecycle(
        state=state,
        scheduled=scheduled,
        terminal=terminal,
        accrues=scheduled and not terminal,
        phase=phase,
        node_name=node_name,
        deletion_requested=metadata.get("deletionTimestamp") not in (None, ""),
        creation_timestamp=creation_timestamp,
        deletion_timestamp=deletion_timestamp,
        start_time=start_time,
        pod_scheduled_status=scheduled_status,
        pod_scheduled_transition_time=scheduled_transition,
    )
    return lifecycle, diagnostics


def classify_pod_lifecycle(raw: Mapping[str, Any]) -> PodLifecycle:
    """Classify scheduling/accrual without retaining the raw Pod."""

    if not isinstance(raw, Mapping):
        raise PodNormalizationError("invalid-pod-object")
    lifecycle, _ = _classify_pod_lifecycle(raw)
    return lifecycle


def _normalize_resource(
    value: Any,
    *,
    resource: Literal["cpu", "memory"],
    path: str,
) -> NormalizedQuantity:
    try:
        if resource == "cpu":
            return normalize_cpu_millicores(value)
        return normalize_byte_quantity(value, resource="memory")
    except QuantityNormalizationError as exc:
        raise _InvalidPodCapacity(
            PodDiagnostic(
                exc.code,
                path,
                resource=resource,
                original_value=exc.original_value,
            )
        ) from exc


def _resource_list(
    value: Any,
    *,
    path: str,
    diagnostics: list[PodDiagnostic],
    report_missing: bool,
    covered_by_pod_level: frozenset[str] = frozenset(),
) -> tuple[_Vector, dict[str, Any], frozenset[str]]:
    if value is _MISSING or value is None:
        resource_map: Mapping[str, Any] = {}
    else:
        resource_map = _mapping(value, path)

    normalized: dict[str, int] = {"cpu": 0, "memory": 0}
    evidence: dict[str, Any] = {}
    present: set[str] = set()
    for resource in _RESOURCES:
        typed_resource: Literal["cpu", "memory"] = resource  # type: ignore[assignment]
        if resource not in resource_map:
            if report_missing:
                diagnostics.append(
                    PodDiagnostic(
                        "missing-request",
                        f"{path}.{resource}",
                        resource=typed_resource,
                        covered_by_pod_level=(resource in covered_by_pod_level),
                    )
                )
            continue
        quantity = _normalize_resource(
            resource_map[resource],
            resource=typed_resource,
            path=f"{path}.{resource}",
        )
        present.add(resource)
        normalized[resource] = quantity.normalized_value
        evidence[resource] = quantity.to_dict()
    return (
        _Vector(cpu=normalized["cpu"], memory=normalized["memory"]),
        evidence,
        frozenset(present),
    )


def _container_request(
    value: Any,
    *,
    path: str,
    diagnostics: list[PodDiagnostic],
    pod_level_present: frozenset[str],
    restartable_init: bool,
) -> _ContainerRequest:
    container = _mapping(value, path)
    name = container.get("name")
    if not _bounded_metadata_text(name, maximum=253):
        raise _InvalidPodCapacity(
            PodDiagnostic("invalid-container-name", f"{path}.name")
        )
    assert isinstance(name, str)
    resources_value = container.get("resources", _MISSING)
    if resources_value in (_MISSING, None):
        requests_value = _MISSING
    else:
        resources = _mapping(resources_value, f"{path}.resources")
        requests_value = resources.get("requests", _MISSING)
    request, evidence, _ = _resource_list(
        requests_value,
        path=f"{path}.resources.requests",
        diagnostics=diagnostics,
        report_missing=True,
        covered_by_pod_level=pod_level_present,
    )
    return _ContainerRequest(
        name=name,
        request=request,
        restartable_init=restartable_init,
        evidence={
            "name": name,
            "restart_policy": "Always" if restartable_init else None,
            "requests": evidence,
        },
    )


def _status_requests(
    values: Any,
    *,
    path: str,
    diagnostics: list[PodDiagnostic],
) -> tuple[dict[str, _StatusRequest], list[dict[str, Any]]]:
    if values in (_MISSING, None):
        return {}, []
    statuses = _sequence(values, path)
    by_name: dict[str, _StatusRequest] = {}
    evidence_rows: list[dict[str, Any]] = []
    for index, value in enumerate(statuses):
        row_path = f"{path}[{index}]"
        status = _mapping(value, row_path)
        name = status.get("name")
        if not _bounded_metadata_text(name, maximum=253):
            raise _InvalidPodCapacity(
                PodDiagnostic("invalid-container-name", f"{row_path}.name")
            )
        assert isinstance(name, str)
        if name in by_name:
            raise _InvalidPodCapacity(
                PodDiagnostic("duplicate-container-status", f"{row_path}.name")
            )

        actuated: _Vector | None = None
        allocated: _Vector | None = None
        evidence: dict[str, Any] = {"name": name}
        resources_value = status.get("resources", _MISSING)
        if resources_value not in (_MISSING, None):
            resources = _mapping(resources_value, f"{row_path}.resources")
            actuated, request_evidence, _ = _resource_list(
                resources.get("requests", _MISSING),
                path=f"{row_path}.resources.requests",
                diagnostics=diagnostics,
                report_missing=False,
            )
            evidence["resources"] = {"requests": request_evidence}

        allocated_value = status.get("allocatedResources", _MISSING)
        if allocated_value not in (_MISSING, None):
            allocated, allocated_evidence, _ = _resource_list(
                allocated_value,
                path=f"{row_path}.allocatedResources",
                diagnostics=diagnostics,
                report_missing=False,
            )
            evidence["allocated_resources"] = allocated_evidence

        by_name[name] = _StatusRequest(
            actuated=actuated,
            allocated=allocated,
            evidence=evidence,
        )
        if len(evidence) > 1:
            evidence_rows.append(evidence)
    return by_name, evidence_rows


def _effective_container(
    container: _ContainerRequest,
    status: _StatusRequest | None,
    *,
    infeasible: bool,
) -> tuple[_Vector, bool]:
    # Upstream status-aware init handling applies only to restartable init
    # containers; callers do not invoke this for ordinary init containers.
    if status is None or status.actuated is None:
        return container.request, False
    candidates = [status.actuated]
    if status.allocated is not None:
        candidates.append(status.allocated)
    if not infeasible:
        candidates.append(container.request)
    return candidates[0].maximum(*candidates[1:]), True


def _aggregate_container_requests(
    regular: Sequence[_ContainerRequest],
    init: Sequence[_ContainerRequest],
    regular_status: Mapping[str, _StatusRequest],
    init_status: Mapping[str, _StatusRequest],
    *,
    infeasible: bool,
) -> tuple[_Vector, bool]:
    regular_sum = _Vector()
    status_used = False
    for container in regular:
        effective, used = _effective_container(
            container,
            regular_status.get(container.name),
            infeasible=infeasible,
        )
        regular_sum = regular_sum.add(effective)
        status_used = status_used or used

    running_restartable = _Vector()
    init_peak = _Vector()
    for container in init:
        if container.restartable_init:
            effective, used = _effective_container(
                container,
                init_status.get(container.name),
                infeasible=infeasible,
            )
            status_used = status_used or used
            running_restartable = running_restartable.add(effective)
            init_peak = init_peak.maximum(running_restartable)
        else:
            init_peak = init_peak.maximum(running_restartable.add(container.request))
    return regular_sum.add(running_restartable).maximum(init_peak), status_used


def _resize_status(status: Mapping[str, Any]) -> tuple[str, bool]:
    conditions = status.get("conditions", [])
    if isinstance(conditions, Sequence) and not isinstance(
        conditions, (str, bytes, bytearray)
    ):
        for condition in conditions:
            if not isinstance(condition, Mapping):
                continue
            if condition.get("type") == "PodResizePending":
                if condition.get("reason") == "Infeasible":
                    return "infeasible", True
                if condition.get("reason") == "Deferred":
                    return "deferred", False
                return "pending", False
        for condition in conditions:
            if (
                isinstance(condition, Mapping)
                and condition.get("type") == "PodResizeInProgress"
            ):
                return "in-progress", False

    deprecated = status.get("resize")
    if isinstance(deprecated, str) and deprecated:
        lowered = deprecated.lower()
        return lowered, lowered == "infeasible"
    return "none", False


def _vectors_differ(left: _Vector, right: _Vector) -> bool:
    return left.cpu != right.cpu or left.memory != right.memory


def _resize_detected_from_status(
    containers: Sequence[_ContainerRequest],
    statuses: Mapping[str, _StatusRequest],
) -> bool:
    for container in containers:
        status = statuses.get(container.name)
        if status is None:
            continue
        if status.actuated is not None and _vectors_differ(
            container.request, status.actuated
        ):
            return True
        if status.allocated is not None and _vectors_differ(
            container.request, status.allocated
        ):
            return True
    return False


def _status_complete_for_effective_request(
    regular: Sequence[_ContainerRequest],
    init: Sequence[_ContainerRequest],
    regular_status: Mapping[str, _StatusRequest],
    init_status: Mapping[str, _StatusRequest],
    pod_level_present: frozenset[str],
    pod_status: _StatusRequest,
) -> bool:
    for resource in _RESOURCES:
        if resource in pod_level_present:
            if pod_status.actuated is None:
                return False
            continue
        for container in regular:
            status = regular_status.get(container.name)
            if status is None or status.actuated is None:
                return False
        for container in init:
            if not container.restartable_init:
                continue
            status = init_status.get(container.name)
            if status is None or status.actuated is None:
                return False
    return True


def _validate_total(vector: _Vector, path: str = "effective_request") -> None:
    for resource, value in (("cpu", vector.cpu), ("memory", vector.memory)):
        if value > SIGNED_BIGINT_MAX:
            raise _InvalidPodCapacity(
                PodDiagnostic(
                    "quantity-overflow",
                    path,
                    resource=resource,  # type: ignore[arg-type]
                )
            )


def _metadata(
    raw: Mapping[str, Any],
    diagnostics: list[PodDiagnostic],
) -> tuple[
    str,
    str,
    str,
    str | None,
    str,
    dict[str, str],
    tuple[PodOwnerReference, ...],
    bool,
]:
    kind = raw.get("kind")
    if kind not in (None, "Pod"):
        raise PodNormalizationError("not-a-pod")
    api_version_value = raw.get("apiVersion", "v1")
    api_version = _required_identity_text(api_version_value, "apiVersion", maximum=64)
    metadata = raw.get("metadata")
    if not isinstance(metadata, Mapping):
        raise PodNormalizationError("invalid-metadata")
    uid = _required_identity_text(metadata.get("uid"), "metadata.uid")
    namespace = _required_identity_text(
        metadata.get("namespace"), "metadata.namespace", maximum=253
    )
    name = _required_identity_text(metadata.get("name"), "metadata.name", maximum=253)

    metadata_valid = True
    resource_version_value = metadata.get("resourceVersion")
    resource_version = _optional_text(resource_version_value)
    if resource_version_value not in (None, "") and resource_version is None:
        diagnostics.append(
            PodDiagnostic("invalid-metadata-field", "metadata.resourceVersion")
        )
        metadata_valid = False

    labels: dict[str, str] = {}
    labels_value = metadata.get("labels", {})
    if labels_value is None:
        labels_value = {}
    if not isinstance(labels_value, Mapping):
        diagnostics.append(PodDiagnostic("invalid-metadata-field", "metadata.labels"))
        metadata_valid = False
    else:
        for key in sorted(ALLOWED_POD_LABELS):
            if key not in labels_value:
                continue
            value = labels_value[key]
            if _bounded_metadata_text(value, maximum=63, allow_empty=True):
                assert isinstance(value, str)
                labels[key] = value
            else:
                diagnostics.append(
                    PodDiagnostic("invalid-metadata-field", f"metadata.labels.{key}")
                )
                metadata_valid = False

    references: list[PodOwnerReference] = []
    owner_values = metadata.get("ownerReferences", [])
    if owner_values is None:
        owner_values = []
    if not isinstance(owner_values, Sequence) or isinstance(
        owner_values, (str, bytes, bytearray)
    ):
        diagnostics.append(
            PodDiagnostic("invalid-metadata-field", "metadata.ownerReferences")
        )
        metadata_valid = False
    else:
        for index, owner_value in enumerate(owner_values):
            path = f"metadata.ownerReferences[{index}]"
            if not isinstance(owner_value, Mapping):
                diagnostics.append(PodDiagnostic("invalid-metadata-field", path))
                metadata_valid = False
                continue
            owner_kind = owner_value.get("kind")
            owner_uid = owner_value.get("uid")
            owner_name = owner_value.get("name")
            if not (
                _bounded_metadata_text(owner_kind, maximum=128)
                and _bounded_metadata_text(owner_uid, maximum=256)
                and _bounded_metadata_text(owner_name, maximum=253)
            ):
                diagnostics.append(PodDiagnostic("invalid-metadata-field", path))
                metadata_valid = False
                continue
            assert isinstance(owner_kind, str)
            assert isinstance(owner_uid, str)
            assert isinstance(owner_name, str)
            references.append(
                PodOwnerReference(
                    kind=owner_kind,
                    uid=owner_uid,
                    name=owner_name,
                )
            )
    references.sort(key=lambda item: (item.kind, item.uid, item.name))
    return (
        uid,
        namespace,
        name,
        resource_version,
        api_version,
        labels,
        tuple(references),
        metadata_valid,
    )


def normalize_pod(
    raw: Mapping[str, Any],
    *,
    previous_request: PodEffectiveRequest | None = None,
) -> NormalizedPod:
    """Normalize one raw admitted Pod into an inventory-safe observation.

    ``previous_request`` is required only for the conservative fallback when a
    resize is visible but the API object lacks the actuated status needed by
    upstream ``UseStatusResources`` semantics. Identifiable invalid capacity
    returns an invalid item with a null revision hash so LIST reconciliation
    still treats the UID as present.
    """

    if not isinstance(raw, Mapping):
        raise PodNormalizationError("invalid-pod-object")

    diagnostics: list[PodDiagnostic] = []
    (
        uid,
        namespace,
        name,
        resource_version,
        api_version,
        labels,
        owner_references,
        metadata_valid,
    ) = _metadata(raw, diagnostics)
    lifecycle, lifecycle_diagnostics = _classify_pod_lifecycle(raw)
    diagnostics.extend(lifecycle_diagnostics)

    request_evidence: dict[str, Any] = {}
    effective_request: PodEffectiveRequest | None = None
    capacity_valid = True
    try:
        spec = _mapping(raw.get("spec"), "spec")
        status_value = raw.get("status", {})
        status = _mapping(status_value, "status") if status_value is not None else {}

        pod_resources_value = spec.get("resources", _MISSING)
        if pod_resources_value in (_MISSING, None):
            pod_requests_value = _MISSING
        else:
            pod_resources = _mapping(pod_resources_value, "spec.resources")
            pod_requests_value = pod_resources.get("requests", _MISSING)
        pod_request, pod_request_evidence, pod_level_present = _resource_list(
            pod_requests_value,
            path="spec.resources.requests",
            diagnostics=diagnostics,
            report_missing=False,
        )

        containers_value = spec.get("containers", _MISSING)
        containers = _sequence(containers_value, "spec.containers")
        if not containers:
            raise _InvalidPodCapacity(
                PodDiagnostic("missing-containers", "spec.containers")
            )
        regular: list[_ContainerRequest] = []
        names: set[str] = set()
        for index, container_value in enumerate(containers):
            container_mapping = _mapping(container_value, f"spec.containers[{index}]")
            container = _container_request(
                container_mapping,
                path=f"spec.containers[{index}]",
                diagnostics=diagnostics,
                pod_level_present=pod_level_present,
                restartable_init=False,
            )
            if container.name in names:
                raise _InvalidPodCapacity(
                    PodDiagnostic("duplicate-container-name", "spec.containers")
                )
            names.add(container.name)
            regular.append(container)

        init_values = spec.get("initContainers", [])
        if init_values is None:
            init_values = []
        init_sequence = _sequence(init_values, "spec.initContainers")
        init: list[_ContainerRequest] = []
        for index, container_value in enumerate(init_sequence):
            container_mapping = _mapping(
                container_value, f"spec.initContainers[{index}]"
            )
            restartable = container_mapping.get("restartPolicy") == "Always"
            container = _container_request(
                container_mapping,
                path=f"spec.initContainers[{index}]",
                diagnostics=diagnostics,
                pod_level_present=pod_level_present,
                restartable_init=restartable,
            )
            if container.name in names:
                raise _InvalidPodCapacity(
                    PodDiagnostic("duplicate-container-name", "spec.initContainers")
                )
            names.add(container.name)
            init.append(container)

        overhead, overhead_evidence, _ = _resource_list(
            spec.get("overhead", _MISSING),
            path="spec.overhead",
            diagnostics=diagnostics,
            report_missing=False,
        )

        regular_status, regular_status_evidence = _status_requests(
            status.get("containerStatuses", _MISSING),
            path="status.containerStatuses",
            diagnostics=diagnostics,
        )
        init_status, init_status_evidence = _status_requests(
            status.get("initContainerStatuses", _MISSING),
            path="status.initContainerStatuses",
            diagnostics=diagnostics,
        )

        pod_status_resources_value = status.get("resources", _MISSING)
        pod_actuated: _Vector | None = None
        pod_status_evidence: dict[str, Any] = {}
        if pod_status_resources_value not in (_MISSING, None):
            pod_status_resources = _mapping(
                pod_status_resources_value, "status.resources"
            )
            pod_actuated, actuated_evidence, _ = _resource_list(
                pod_status_resources.get("requests", _MISSING),
                path="status.resources.requests",
                diagnostics=diagnostics,
                report_missing=False,
            )
            pod_status_evidence["resources"] = {"requests": actuated_evidence}
        pod_allocated_value = status.get("allocatedResources", _MISSING)
        pod_allocated: _Vector | None = None
        if pod_allocated_value not in (_MISSING, None):
            pod_allocated, allocated_evidence, _ = _resource_list(
                pod_allocated_value,
                path="status.allocatedResources",
                diagnostics=diagnostics,
                report_missing=False,
            )
            pod_status_evidence["allocated_resources"] = allocated_evidence
        pod_status = _StatusRequest(
            actuated=pod_actuated,
            allocated=pod_allocated,
            evidence=pod_status_evidence,
        )

        resize_status, infeasible = _resize_status(status)
        container_spec, _ = _aggregate_container_requests(
            regular, regular_status={}, init=init, init_status={}, infeasible=False
        )
        spec_base = _Vector(
            cpu=(pod_request.cpu if "cpu" in pod_level_present else container_spec.cpu),
            memory=(
                pod_request.memory
                if "memory" in pod_level_present
                else container_spec.memory
            ),
        )
        spec_total = spec_base.add(overhead)

        status_container, container_status_used = _aggregate_container_requests(
            regular,
            init,
            regular_status,
            init_status,
            infeasible=infeasible,
        )
        pod_status_used = False
        status_values: dict[str, int] = {}
        sources: dict[str, str] = {}
        for resource in _RESOURCES:
            if resource in pod_level_present:
                candidates: list[_Vector] = []
                if pod_status.actuated is not None:
                    candidates.append(pod_status.actuated)
                if pod_status.allocated is not None:
                    candidates.append(pod_status.allocated)
                if not infeasible:
                    candidates.append(pod_request)
                if candidates and pod_status.actuated is not None:
                    value = candidates[0].maximum(*candidates[1:]).resource(resource)
                    pod_status_used = True
                    sources[resource] = "pod-level-status-aware"
                else:
                    value = pod_request.resource(resource)
                    sources[resource] = "pod-level-spec"
            else:
                value = status_container.resource(resource)
                sources[resource] = (
                    "container-derived-status-aware"
                    if container_status_used
                    else "container-derived-spec"
                )
            status_values[resource] = value
        status_total = _Vector(
            cpu=status_values["cpu"], memory=status_values["memory"]
        ).add(overhead)

        resize_detected = resize_status != "none"
        resize_detected = resize_detected or _resize_detected_from_status(
            regular, regular_status
        )
        resize_detected = resize_detected or _resize_detected_from_status(
            [item for item in init if item.restartable_init], init_status
        )
        if pod_status.actuated is not None and _vectors_differ(
            pod_request, pod_status.actuated
        ):
            resize_detected = True
        if pod_status.allocated is not None and _vectors_differ(
            pod_request, pod_status.allocated
        ):
            resize_detected = True
        if previous_request is not None and (
            previous_request.cpu_millicores != spec_total.cpu
            or previous_request.memory_bytes != spec_total.memory
        ):
            resize_detected = True

        status_complete = _status_complete_for_effective_request(
            regular,
            init,
            regular_status,
            init_status,
            pod_level_present,
            pod_status,
        )
        quality: Literal["exact", "resize-status-unavailable"] = "exact"
        if resize_detected and not status_complete:
            diagnostics.append(PodDiagnostic("resize-status-unavailable", "status"))
            if previous_request is None:
                raise _InvalidPodCapacity(diagnostics[-1])
            status_total = _Vector(
                cpu=max(
                    spec_total.cpu,
                    status_total.cpu,
                    previous_request.cpu_millicores,
                ),
                memory=max(
                    spec_total.memory,
                    status_total.memory,
                    previous_request.memory_bytes,
                ),
            )
            sources = {
                "cpu": "previous-or-current-conservative",
                "memory": "previous-or-current-conservative",
            }
            quality = "resize-status-unavailable"

        _validate_total(status_total)
        effective_request = PodEffectiveRequest(
            cpu_millicores=status_total.cpu,
            memory_bytes=status_total.memory,
            cpu_source=sources["cpu"],
            memory_source=sources["memory"],
            overhead_cpu_millicores=overhead.cpu,
            overhead_memory_bytes=overhead.memory,
            capacity_quality=quality,
            resize_status=resize_status,
            status_resources_used=container_status_used or pod_status_used,
        )

        request_evidence = {
            "admitted_requests": {
                "containers": [item.evidence for item in regular],
                "init_containers": [item.evidence for item in init],
                "pod_requests": pod_request_evidence,
                "overhead": overhead_evidence,
            },
            "resize_request_status": {
                "container_statuses": regular_status_evidence,
                "init_container_statuses": init_status_evidence,
                "pod_resources": pod_status_evidence,
            },
        }

        resource_claim_statuses = status.get("resourceClaimStatuses")
        if (
            isinstance(resource_claim_statuses, Sequence)
            and not isinstance(resource_claim_statuses, (str, bytes, bytearray))
            and resource_claim_statuses
        ):
            diagnostics.append(
                PodDiagnostic(
                    "dra-capacity-unsupported", "status.resourceClaimStatuses"
                )
            )
            capacity_valid = False
            effective_request = None
    except _InvalidPodCapacity as exc:
        if exc.diagnostic not in diagnostics:
            diagnostics.append(exc.diagnostic)
        capacity_valid = False
        effective_request = None

    valid = metadata_valid and capacity_valid
    return NormalizedPod(
        uid=uid,
        namespace=namespace,
        name=name,
        resource_version=resource_version,
        api_version=api_version,
        labels=labels,
        owner_references=owner_references,
        lifecycle=lifecycle,
        effective_request=effective_request,
        request_evidence=request_evidence,
        diagnostics=tuple(diagnostics),
        valid_for_metering=valid,
    )


__all__ = [
    "ALLOWED_POD_LABELS",
    "POD_REQUESTS_ALGORITHM",
    "NormalizedPod",
    "PodDiagnostic",
    "PodEffectiveRequest",
    "PodLifecycle",
    "PodLifecycleState",
    "PodNormalizationError",
    "PodOwnerReference",
    "classify_pod_lifecycle",
    "normalize_pod",
]
