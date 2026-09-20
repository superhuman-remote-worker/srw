"""Complete resource policy snapshots are immutable identity, never enablement."""

from dataclasses import replace
import hashlib
import json

import pytest

from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_resource_inventory_settings import configuration


def complete_policy():
    value = configuration()
    value["policy"]["hostCost"] = {
        "cpuMillicoresPerVcpuNumerator": 1000,
        "cpuMillicoresPerVcpuDenominator": 10,
        "launcherCpuOverheadMillicores": 0,
        "fixedMemoryOverheadBytes": 260 * 1024**2,
        "perVcpuMemoryOverheadBytes": 8 * 1024**2,
        "memoryOverheadBasisPoints": 0,
    }
    value["policy"]["nodeHeadroom"] = {
        "cpuMillicores": 0,
        "memoryBytes": 0,
        "kvmDevices": 0,
    }
    value["policy"]["fairness"] = {"maxBypasses": 2, "priorityAgingSeconds": 60}
    return value


def snapshot(value=None):
    from shared.vm_resource_policy import validate_complete_resource_policy

    return validate_complete_resource_policy(
        complete_policy() if value is None else value
    )


def enforcement_policy():
    value = complete_policy()
    value["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    return value


def enforcement_snapshot(value=None):
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    return validate_enforcement_resource_policy(
        enforcement_policy() if value is None else value
    )


def test_snapshot_freezes_canonical_whole_policy_without_secret(monkeypatch):
    monkeypatch.delenv("VM_LIFECYCLE_HMAC_SECRET", raising=False)
    value = complete_policy()
    canonical = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    result = snapshot(value)
    assert result.canonical_document == canonical
    assert result.policy_digest == "sha256:" + hashlib.sha256(canonical).hexdigest()
    assert result.inventory.policy_digest == result.policy_digest
    assert result.host_cost.cost(2, "4Gi").to_dict() == {
        "cpu_millicores": 200,
        "memory_bytes": 4 * 1024**3 + 276 * 1024**2,
        "kvm_devices": 1,
    }
    value["policy"]["hostCost"]["fixedMemoryOverheadBytes"] = 0
    value["policy"]["inventory"]["nodeLabelKeys"].append("new-key")
    assert result.canonical_document == canonical
    assert result.host_cost.fixed_memory_overhead_bytes == 260 * 1024**2
    assert result.inventory.label_keys == ("kubernetes.io/hostname", "zone")


@pytest.mark.parametrize(
    "section,key,value",
    [
        (None, "namespace", "other"),
        ("policy", "stableClusterId", "other"),
        ("inventory", "maxItems", 1001),
        ("inventory", "nodeLabelKeys", ["kubernetes.io/hostname", "other"]),
        ("hostCost", "fixedMemoryOverheadBytes", 0),
        ("nodeHeadroom", "cpuMillicores", 10),
        ("fairness", "maxBypasses", 3),
    ],
)
def test_full_policy_changes_cannot_keep_old_digest(section, key, value):
    doc = complete_policy()
    target = (
        doc
        if section is None
        else doc["policy"]
        if section == "policy"
        else doc["policy"][section]
    )
    target[key] = value
    assert snapshot(doc).policy_digest != snapshot().policy_digest


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("hostCost", "cpuMillicoresPerVcpuDenominator", 0),
        ("hostCost", "cpuMillicoresPerVcpuNumerator", True),
        ("hostCost", "memoryOverheadBasisPoints", -1),
        ("hostCost", "fixedMemoryOverheadBytes", 2**63),
        ("hostCost", "launcherCpuOverheadMillicores", "0"),
        ("nodeHeadroom", "memoryBytes", False),
        ("fairness", "maxBypasses", -1),
        ("fairness", "priorityAgingSeconds", 0),
        ("inventory", "maxItems", True),
        ("inventory", "nodeLabelKeys", ["zone"]),
        ("policy", "observerEnabled", False),
        ("policy", "shadowEnabled", True),
        ("policy", "enforcementEnabled", True),
    ],
)
def test_partial_invalid_or_unimplemented_policy_is_refused(section, key, value):
    doc = complete_policy()
    (doc["policy"] if section == "policy" else doc["policy"][section])[key] = value
    with pytest.raises(ResourceAdmissionError, match="invalid_resource_policy"):
        snapshot(doc)


@pytest.mark.parametrize("section", ["hostCost", "nodeHeadroom", "fairness"])
def test_complete_projection_rejects_observer_only_empty_resource_sections(section):
    doc = complete_policy()
    doc["policy"][section] = {}
    with pytest.raises(ResourceAdmissionError):
        snapshot(doc)


def test_snapshot_revalidation_refuses_mixed_or_fabricated_fields():
    from shared.vm_resource_policy import validate_resource_policy_snapshot

    value = snapshot()
    assert validate_resource_policy_snapshot(value) is value
    for changed in (
        replace(value, policy_digest="sha256:" + "a" * 64),
        replace(value, max_bypasses=99),
        {},
    ):
        with pytest.raises(ResourceAdmissionError):
            validate_resource_policy_snapshot(changed)


def test_environment_observer_still_requires_secret_and_allows_empty_cost_sections():
    from shared.vm_resource_inventory_settings import InventorySettings
    from shared.vm_resource_inventory import InventoryError

    doc = configuration()
    assert InventorySettings.from_document(doc) is not None
    with pytest.raises(InventoryError):
        InventorySettings.from_environment(
            {"VM_RESOURCE_ADMISSION_CONFIG": json.dumps(doc)}
        )
    assert (
        InventorySettings.from_environment(
            {
                "VM_RESOURCE_ADMISSION_CONFIG": json.dumps(doc),
                "VM_LIFECYCLE_HMAC_SECRET": "fixture-lifecycle-secret-at-least-32-bytes",
            }
        )
        is not None
    )


@pytest.mark.parametrize(
    "field", ["max_bypasses", "priority_aging_seconds", "inventory"]
)
def test_snapshot_revalidation_rejects_equal_valued_wrong_types(field):
    from shared.vm_resource_policy import validate_resource_policy_snapshot

    value = snapshot()
    changed = (
        replace(value.inventory, max_items=float(value.inventory.max_items))
        if field == "inventory"
        else float(getattr(value, field))
    )
    with pytest.raises(ResourceAdmissionError, match="invalid_resource_policy"):
        validate_resource_policy_snapshot(replace(value, **{field: changed}))


def test_enforcement_snapshot_requires_all_capabilities_without_broadening_observer():
    from shared.vm_resource_inventory import InventoryError
    from shared.vm_resource_inventory_settings import InventorySettings

    value = enforcement_policy()
    with pytest.raises(InventoryError, match="invalid_inventory_configuration"):
        InventorySettings.from_document(value)

    result = enforcement_snapshot(value)
    canonical = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    assert result.canonical_document == canonical
    assert result.policy_digest == "sha256:" + hashlib.sha256(canonical).hexdigest()
    assert result.inventory.policy_digest == result.policy_digest
    assert result.inventory.cluster_id == "test-cluster"
    assert result.host_cost.cost(2, "4Gi").to_dict() == {
        "cpu_millicores": 200,
        "memory_bytes": 4 * 1024**3 + 276 * 1024**2,
        "kvm_devices": 1,
    }


@pytest.mark.parametrize(
    "flag,replacement",
    [
        ("observerEnabled", False),
        ("shadowEnabled", False),
        ("enforcementEnabled", False),
        ("clusterWidePodReadAcknowledged", False),
        ("shadowEnabled", 1),
    ],
)
def test_enforcement_snapshot_refuses_missing_or_untyped_capability(flag, replacement):
    value = enforcement_policy()
    value["policy"][flag] = replacement
    with pytest.raises(ResourceAdmissionError, match="invalid_resource_policy"):
        enforcement_snapshot(value)


def test_enforcement_snapshot_is_owned_and_exactly_revalidated():
    from shared.vm_resource_policy import validate_enforcement_resource_policy_snapshot

    value = enforcement_policy()
    result = enforcement_snapshot(value)
    value["policy"]["fairness"]["maxBypasses"] = 99
    assert result.max_bypasses == 2
    assert validate_enforcement_resource_policy_snapshot(result) is result
    for changed in (
        replace(result, max_bypasses=2.0),
        replace(result, policy_digest="sha256:" + "a" * 64),
        replace(result, inventory=replace(result.inventory, max_items=1000.0)),
        {},
    ):
        with pytest.raises(ResourceAdmissionError, match="invalid_resource_policy"):
            validate_enforcement_resource_policy_snapshot(changed)
