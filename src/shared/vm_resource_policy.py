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
    WholeLauncherHostCostPolicy,
)
from shared.vm_resource_inventory_settings import InventorySettings
from shared.vm_launcher_profile import validate_launcher_profile


HOST_COST_FIELDS = (
    "cpuMillicoresPerVcpuNumerator",
    "cpuMillicoresPerVcpuDenominator",
    "launcherCpuOverheadMillicores",
    "fixedMemoryOverheadBytes",
    "perVcpuMemoryOverheadBytes",
    "memoryOverheadBasisPoints",
)
WHOLE_HOST_COST_FIELDS = HOST_COST_FIELDS + (
    "version", "ephemeralStorageReserveBytes", "kvmDevices", "tunDevices",
    "vhostNetDevices",
)
SIX_BUDGET_FIELDS = (
    "cpuMillicores", "memoryBytes", "ephemeralStorageBytes", "kvmDevices",
    "tunDevices", "vhostNetDevices",
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


def parse_whole_launcher_host_cost_policy(value):
    try:
        _fields(value, WHOLE_HOST_COST_FIELDS)
        if type(value["version"]) is not int or value["version"] != 2:
            raise ValueError
        return WholeLauncherHostCostPolicy(
            parse_host_cost_policy({key: value[key] for key in HOST_COST_FIELDS}),
            value["ephemeralStorageReserveBytes"],
            value["kvmDevices"],
            value["tunDevices"],
            value["vhostNetDevices"],
        )
    except (ValueError, TypeError, KeyError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


def parse_six_budget(value):
    try:
        values = _fields(value, SIX_BUDGET_FIELDS)
        return ResourceVector(values[0], values[1], values[3], values[2], values[4], values[5])
    except (ValueError, TypeError, KeyError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


def parse_whole_launcher_policy_values(policy):
    try:
        cost = parse_whole_launcher_host_cost_policy(policy["hostCost"])
        headroom = parse_six_budget(policy["nodeHeadroom"])
        installation = parse_six_budget(policy["installationBudget"])
        owner = parse_six_budget(policy["ownerBudget"])
        profile = validate_launcher_profile(policy["launcherProfile"])
        bypasses, aging = _fields(policy["fairness"], ("maxBypasses", "priorityAgingSeconds"))
        if (
            type(bypasses) is not int or not 0 <= bypasses < 2**63
            or type(aging) is not int or not 1 <= aging < 2**63
        ):
            raise ValueError
        return cost, headroom, installation, owner, bypasses, aging, profile
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
    launcher_profile: dict | None = None
    installation_budget: ResourceVector | None = None
    owner_budget: ResourceVector | None = None


@dataclass(frozen=True, slots=True)
class EnforcementResourcePolicySnapshot:
    canonical_document: bytes
    policy_digest: str
    inventory: InventorySettings
    host_cost: HostCostPolicy
    headroom: ResourceVector
    max_bypasses: int
    priority_aging_seconds: int
    launcher_profile: dict | None = None
    installation_budget: ResourceVector | None = None
    owner_budget: ResourceVector | None = None


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
        if inventory.protocol == 2:
            cost, headroom, installation, owner, bypasses, aging, profile = (
                parse_whole_launcher_policy_values(value["policy"])
            )
        else:
            cost, headroom, bypasses, aging = parse_resource_policy_values(value["policy"])
            profile = installation = owner = None
        return snapshot_type(
            canonical,
            inventory.policy_digest,
            inventory,
            cost,
            headroom,
            bypasses,
            aging,
            profile,
            installation,
            owner,
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


def configured_enforcement_required(source=None):
    """An operator-installed controller flag cannot be disabled by a create body."""
    import os

    env = os.environ if source is None else source
    raw = env.get("VM_RESOURCE_ADMISSION_CONFIG", "")
    if not raw:
        return False
    try:
        document = json.loads(raw)
        enabled = document["policy"]["enforcementEnabled"]
        if type(enabled) is not bool:
            raise ValueError
        if enabled:
            validate_enforcement_resource_policy(document)
        return enabled
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None


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
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _same_typed_value(actual[key], value)
            for key, value in expected.items()
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
