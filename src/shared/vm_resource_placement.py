"""Pure, fail-closed node eligibility for resource admission.

Selector and toleration semantics follow Kubernetes v1.35 component helpers:
https://github.com/kubernetes/component-helpers/blob/v0.35.0/scheduling/corev1/nodeaffinity/nodeaffinity.go
https://github.com/kubernetes/api/blob/v0.35.0/core/v1/toleration.go

This checks static eligibility only. Inventory freshness, resource reservations,
storage binding and exact Node UID revalidation remain separate admission gates.
"""

from collections.abc import Mapping
import re
from uuid import UUID

from shared.kubernetes_quantities import (
    normalize_cpu_millicores,
    normalize_byte_quantity,
    QuantityNormalizationError,
)
from shared.vm_resource_admission import (
    EPHEMERAL_RESOURCE,
    KVM_RESOURCE,
    TUN_RESOURCE,
    VHOST_NET_RESOURCE,
    ResourceAdmissionError,
    ResourceVector,
    normalize_kvm_devices,
)


_EFFECTS = {"NoSchedule", "NoExecute", "PreferNoSchedule"}


def _mapping(value):
    if not isinstance(value, Mapping):
        raise ResourceAdmissionError("invalid_placement")
    return value


def _list(value):
    if not isinstance(value, (list, tuple)) or len(value) > 1024:
        raise ResourceAdmissionError("invalid_placement")
    return value


def _text(value, *, empty=False):
    if (
        not isinstance(value, str)
        or len(value) > 253
        or (not empty and not value)
        or value != value.strip()
    ):
        raise ResourceAdmissionError("invalid_placement")
    return value


def _labels(value):
    result = _mapping(value)
    if len(result) > 1024:
        raise ResourceAdmissionError("invalid_placement")
    for key, value in result.items():
        _text(key)
        _text(value, empty=True)
    return result


def node_allocatable(node, *, six=False):
    values = _mapping(_mapping(node.get("status", {})).get("allocatable", {}))
    try:
        if six and any(
            key not in values
            for key in (EPHEMERAL_RESOURCE, KVM_RESOURCE, TUN_RESOURCE, VHOST_NET_RESOURCE)
        ):
            raise ResourceAdmissionError("invalid_placement")
        return ResourceVector(
            normalize_cpu_millicores(values["cpu"]).normalized_value,
            normalize_byte_quantity(values["memory"]).normalized_value,
            normalize_kvm_devices(values.get(KVM_RESOURCE, 0)),
            normalize_byte_quantity(values[EPHEMERAL_RESOURCE]).normalized_value
            if six else 0,
            normalize_kvm_devices(values[TUN_RESOURCE]) if six else 0,
            normalize_kvm_devices(values[VHOST_NET_RESOURCE]) if six else 0,
        )
    except (KeyError, QuantityNormalizationError):
        raise ResourceAdmissionError("invalid_placement") from None


def _signed_integer(value):
    if not isinstance(value, str) or not re.fullmatch(r"[+-]?[0-9]{1,19}", value):
        raise ResourceAdmissionError("invalid_placement")
    number = int(value)
    if not -(2**63) <= number < 2**63:
        raise ResourceAdmissionError("invalid_placement")
    return number


def _requirement_matches(raw, labels, *, fields=False):
    req = _mapping(raw)
    if set(req) - {"key", "operator", "values"}:
        raise ResourceAdmissionError("invalid_placement")
    key, operator = _text(req.get("key")), _text(req.get("operator"))
    values = [_text(value, empty=True) for value in _list(req.get("values", []))]
    if fields and (
        key != "metadata.name" or operator not in {"In", "NotIn"} or len(values) != 1
    ):
        raise ResourceAdmissionError("invalid_placement")
    if operator in {"In", "NotIn"}:
        if not values:
            raise ResourceAdmissionError("invalid_placement")
        match = key in labels and labels[key] in values
        return match if operator == "In" else not match
    if operator in {"Exists", "DoesNotExist"}:
        if values:
            raise ResourceAdmissionError("invalid_placement")
        return (key in labels) == (operator == "Exists")
    if operator in {"Gt", "Lt"}:
        if len(values) != 1:
            raise ResourceAdmissionError("invalid_placement")
        threshold = _signed_integer(values[0])
        if key not in labels:
            return False
        try:
            actual = _signed_integer(labels[key])
        except ResourceAdmissionError:
            return False
        return actual > threshold if operator == "Gt" else actual < threshold
    raise ResourceAdmissionError("invalid_placement")


def _affinity_matches(affinity, labels, name):
    if affinity is None:
        return True
    raw = _mapping(affinity)
    if set(raw) != {"nodeSelectorTerms"}:
        raise ResourceAdmissionError("invalid_placement")
    matches = []
    for term in _list(raw["nodeSelectorTerms"]):
        term = _mapping(term)
        if set(term) - {"matchExpressions", "matchFields"}:
            raise ResourceAdmissionError("invalid_placement")
        terms = [
            _requirement_matches(req, labels)
            for req in _list(term.get("matchExpressions", []))
        ]
        terms.extend(
            _requirement_matches(req, {"metadata.name": name}, fields=True)
            for req in _list(term.get("matchFields", []))
        )
        # Empty terms select no nodes. Validate all OR terms before matching so
        # unknown future constraints cannot silently become an eligible node.
        matches.append(bool(terms) and all(terms))
    return any(matches)


def affinity_label_keys(affinity):
    """Validate a required selector and return its declared label dependencies.

    Inventory consumers must prove these keys were collected before treating a
    missing value as absent (especially for NotIn and DoesNotExist).
    """
    _affinity_matches(affinity, {}, "validation-node")
    if affinity is None:
        return frozenset()
    return frozenset(
        requirement["key"]
        for term in affinity["nodeSelectorTerms"]
        for requirement in term.get("matchExpressions", [])
    )


def _unmatched_taints(node, tolerations):
    validated = []
    for raw in _list(tolerations):
        tol = _mapping(raw)
        if set(tol) - {"key", "operator", "value", "effect", "tolerationSeconds"}:
            raise ResourceAdmissionError("invalid_placement")
        key = _text(tol.get("key", ""), empty=True)
        value = _text(tol.get("value", ""), empty=True)
        operator = tol.get("operator", "Equal")
        if operator == "":
            operator = "Equal"
        effect = tol.get("effect", "")
        if operator not in {"Equal", "Exists"} or effect not in _EFFECTS | {""}:
            raise ResourceAdmissionError("invalid_placement")
        if (not key and operator != "Exists") or (value and operator == "Exists"):
            raise ResourceAdmissionError("invalid_placement")
        seconds = tol.get("tolerationSeconds")
        if seconds is not None and (
            effect != "NoExecute"
            or type(seconds) is not int
            or not -(2**63) <= seconds < 2**63
        ):
            raise ResourceAdmissionError("invalid_placement")
        validated.append((key, value, operator, effect))
    unmatched = []
    for raw in _list(_mapping(node.get("spec", {})).get("taints", [])):
        taint = _mapping(raw)
        key = _text(taint.get("key"))
        value = _text(taint.get("value", ""), empty=True)
        effect = taint.get("effect")
        if effect not in _EFFECTS:
            raise ResourceAdmissionError("invalid_placement")
        if not any(
            (not tk or tk == key)
            and (not te or te == effect)
            and (op == "Exists" or tv == value)
            for tk, tv, op, te in validated
        ):
            unmatched.append(effect)
    return unmatched


def preferred_taint_count(node, tolerations):
    return _unmatched_taints(node, tolerations).count("PreferNoSchedule")


def node_exclusion(
    node, *, selector=None, tolerations=(), required_affinity=None, pv_affinity=None
):
    """Return one bounded exclusion reason, or None for static eligibility."""
    try:
        node = _mapping(node)
        metadata = _mapping(node.get("metadata", {}))
        uid, name = metadata.get("uid"), metadata.get("name")
        try:
            if not isinstance(uid, str) or str(UUID(uid)) != uid or not _text(name):
                return "node_identity"
        except (ValueError, ResourceAdmissionError):
            return "node_identity"
        labels = _labels(metadata.get("labels", {}))
        spec, status = _mapping(node.get("spec", {})), _mapping(node.get("status", {}))
        ready = [
            condition.get("status")
            for condition in _list(status.get("conditions", []))
            if _mapping(condition).get("type") == "Ready"
        ]
        if ready != ["True"]:
            return "node_not_ready"
        cordoned = spec.get("unschedulable", False)
        if type(cordoned) is not bool:
            raise ResourceAdmissionError("invalid_placement")
        if cordoned:
            return "node_cordoned"
        if node_allocatable(node).kvm_devices == 0:
            return "kvm_unavailable"
        if any(
            labels.get(key) != value
            for key, value in _labels({} if selector is None else selector).items()
        ):
            return "node_selector"
        if any(
            effect != "PreferNoSchedule"
            for effect in _unmatched_taints(node, tolerations)
        ):
            return "node_taints"
        if not _affinity_matches(required_affinity, labels, name):
            return "node_affinity"
        if not _affinity_matches(pv_affinity, labels, name):
            return "storage_topology"
        return None
    except (ResourceAdmissionError, TypeError):
        return "invalid_placement"
