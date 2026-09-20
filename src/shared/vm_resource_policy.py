"""Immutable complete policy identity for private resource configuration.

Snapshots describe an operator cost estimate. They grant no Kubernetes access,
create/reservation authority, or permission to enable shadow/enforcement modes.
"""

from dataclasses import dataclass, fields, is_dataclass
import json

from shared.vm_resource_admission import (
    HostCostPolicy,
    ResourceAdmissionError,
    ResourceVector,
)
from shared.vm_resource_inventory_settings import InventorySettings


HOST_COST_FIELDS = (
    "cpuMillicoresPerVcpuNumerator",
    "cpuMillicoresPerVcpuDenominator",
    "launcherCpuOverheadMillicores",
    "fixedMemoryOverheadBytes",
    "perVcpuMemoryOverheadBytes",
    "memoryOverheadBasisPoints",
)


def _fields(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ResourceAdmissionError("invalid_resource_policy")
    return [value[field] for field in fields]


def parse_host_cost_policy(value):
    """Validate the closed six-field operator estimate used by both services."""
    try:
        return HostCostPolicy(*_fields(value, HOST_COST_FIELDS))
    except (ValueError, TypeError, KeyError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


def parse_resource_policy_values(policy):
    """The same exact value vocabulary serves config projection and admission."""
    try:
        cost = parse_host_cost_policy(policy["hostCost"])
        headroom = ResourceVector(
            *_fields(
                policy["nodeHeadroom"], ("cpuMillicores", "memoryBytes", "kvmDevices")
            )
        )
        bypasses, aging = _fields(
            policy["fairness"], ("maxBypasses", "priorityAgingSeconds")
        )
        if (
            type(bypasses) is not int
            or not 0 <= bypasses < 2**63
            or type(aging) is not int
            or not 1 <= aging < 2**63
        ):
            raise ValueError
        return cost, headroom, bypasses, aging
    except (ValueError, TypeError, KeyError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


@dataclass(frozen=True, slots=True)
class CompleteResourcePolicySnapshot:
    canonical_document: bytes
    policy_digest: str
    inventory: InventorySettings
    host_cost: HostCostPolicy
    headroom: ResourceVector
    max_bypasses: int
    priority_aging_seconds: int


@dataclass(frozen=True, slots=True)
class EnforcementResourcePolicySnapshot:
    canonical_document: bytes
    policy_digest: str
    inventory: InventorySettings
    host_cost: HostCostPolicy
    headroom: ResourceVector
    max_bypasses: int
    priority_aging_seconds: int


def _validate_resource_policy(document, *, inventory_loader, snapshot_type):
    try:
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        if len(canonical) > 16384:
            raise ValueError
        # Decode into an owned tree: no mutable input survives validation.
        value = json.loads(canonical)
        inventory = inventory_loader(value)
        if inventory is None:
            raise ValueError
        cost, headroom, bypasses, aging = parse_resource_policy_values(value["policy"])
        return snapshot_type(
            canonical,
            inventory.policy_digest,
            inventory,
            cost,
            headroom,
            bypasses,
            aging,
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


def validate_complete_resource_policy(document):
    """Freeze a complete explicit observer policy without environment/secret I/O.

    Unfinished shadow/enforcement modes remain refused by the existing parser.
    Their eventual activation needs a separate runtime and launcher contract.
    """
    return _validate_resource_policy(
        document,
        inventory_loader=InventorySettings.from_document,
        snapshot_type=CompleteResourcePolicySnapshot,
    )


def validate_enforcement_resource_policy(document):
    """Freeze the complete all-capabilities policy without granting enablement."""

    return _validate_resource_policy(
        document,
        inventory_loader=lambda value: InventorySettings._from_document_capabilities(
            value, expected=(True, True, True, True)
        ),
        snapshot_type=EnforcementResourcePolicySnapshot,
    )


def _same_typed_value(actual, expected):
    """Dataclass equality alone accepts float/int and bool/int substitutions."""
    if type(actual) is not type(expected):
        return False
    if is_dataclass(expected):
        return all(
            _same_typed_value(
                getattr(actual, field.name), getattr(expected, field.name)
            )
            for field in fields(expected)
        )
    if isinstance(expected, tuple):
        return len(actual) == len(expected) and all(
            _same_typed_value(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def validate_resource_policy_snapshot(snapshot):
    """Reject substituted types or inconsistent derived fields at resolver entry."""
    try:
        if (
            type(snapshot) is not CompleteResourcePolicySnapshot
            or type(snapshot.canonical_document) is not bytes
        ):
            raise ValueError
        if len(snapshot.canonical_document) > 16384:
            raise ValueError
        rebuilt = validate_complete_resource_policy(
            json.loads(snapshot.canonical_document)
        )
        if not _same_typed_value(snapshot, rebuilt):
            raise ValueError
        return snapshot
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


def validate_enforcement_resource_policy_snapshot(snapshot):
    """Rebuild and compare every enforcement snapshot field with exact types."""
    try:
        if (
            type(snapshot) is not EnforcementResourcePolicySnapshot
            or type(snapshot.canonical_document) is not bytes
        ):
            raise ValueError
        if len(snapshot.canonical_document) > 16384:
            raise ValueError
        rebuilt = validate_enforcement_resource_policy(
            json.loads(snapshot.canonical_document)
        )
        if not _same_typed_value(snapshot, rebuilt):
            raise ValueError
        return snapshot
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None
