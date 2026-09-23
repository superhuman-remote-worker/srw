from copy import deepcopy
from uuid import uuid4

import pytest

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_placement import node_allocatable, node_exclusion, preferred_taint_count


def node():
    return {
        "metadata": {
            "uid": str(uuid4()),
            "name": "node-a",
            "labels": {
                "kubernetes.io/hostname": "node-a",
                "zone": "a",
                "generation": "10",
            },
        },
        "spec": {},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "allocatable": {
                "cpu": "8",
                "memory": "16Gi",
                "devices.kubevirt.io/kvm": "100",
            },
        },
    }


def test_six_field_node_allocatable_requires_real_ephemeral_and_devices():
    raw = node()
    raw["status"]["allocatable"].update({
        "ephemeral-storage": "100G",
        "devices.kubevirt.io/tun": "8",
        "devices.kubevirt.io/vhost-net": "8",
    })
    assert node_allocatable(raw, six=True).to_six_dict() == {
        "cpu_millicores": 8000,
        "memory_bytes": 16 * 1024**3,
        "ephemeral_storage_bytes": 100000000000,
        "kvm_devices": 100,
        "tun_devices": 8,
        "vhost_net_devices": 8,
    }
    del raw["status"]["allocatable"]["devices.kubevirt.io/tun"]
    with pytest.raises(ResourceAdmissionError):
        node_allocatable(raw, six=True)


def affinity(*requirements, fields=None):
    return {
        "nodeSelectorTerms": [
            {"matchExpressions": list(requirements), "matchFields": fields or []}
        ]
    }


def requirement(key, op, *values):
    return {"key": key, "operator": op, "values": list(values)}


def test_ready_uncordoned_node_with_kvm_and_matching_selector_is_eligible():
    assert node_exclusion(node(), selector={"zone": "a"}) is None
    assert node_exclusion(node(), selector={"zone": "b"}) == "node_selector"


@pytest.mark.parametrize(
    "mutation,reason",
    [
        (lambda n: n["metadata"].pop("uid"), "node_identity"),
        (
            lambda n: n["status"]["conditions"][0].update(status="False"),
            "node_not_ready",
        ),
        (lambda n: n["spec"].update(unschedulable=True), "node_cordoned"),
        (
            lambda n: n["status"]["allocatable"].update(
                {"devices.kubevirt.io/kvm": "0"}
            ),
            "kvm_unavailable",
        ),
        (
            lambda n: n["status"]["allocatable"].update(
                {"devices.kubevirt.io/kvm": "0.5"}
            ),
            "invalid_placement",
        ),
    ],
)
def test_ineligible_node_conditions(mutation, reason):
    current = node()
    mutation(current)
    assert node_exclusion(current) == reason


@pytest.mark.parametrize("effect", ["NoSchedule", "NoExecute"])
def test_hard_taint_needs_exact_effect_key_and_value_or_exists(effect):
    current = node()
    current["spec"]["taints"] = [{"key": "dedicated", "value": "vm", "effect": effect}]
    assert node_exclusion(current) == "node_taints"
    assert (
        node_exclusion(
            current, tolerations=[{"key": "dedicated", "value": "vm", "effect": effect}]
        )
        is None
    )
    assert (
        node_exclusion(current, tolerations=[{"key": "dedicated", "value": "other"}])
        == "node_taints"
    )
    assert (
        node_exclusion(
            current, tolerations=[{"key": "dedicated", "operator": "Exists"}]
        )
        is None
    )
    assert node_exclusion(current, tolerations=[{"operator": "Exists"}]) is None
    assert (
        node_exclusion(
            current,
            tolerations=[
                {"key": "dedicated", "operator": "Exists", "effect": "PreferNoSchedule"}
            ],
        )
        == "node_taints"
    )


def test_soft_taint_only_changes_preference():
    current = node()
    current["spec"]["taints"] = [{"key": "dedicated", "effect": "PreferNoSchedule"}]
    assert node_exclusion(current) is None
    assert preferred_taint_count(current, []) == 1
    assert preferred_taint_count(current, [{"operator": "Exists"}]) == 0


def test_empty_toleration_operator_is_kubernetes_equal_default():
    current = node()
    current["spec"]["taints"] = [
        {"key": "dedicated", "value": "vm", "effect": "NoSchedule"}
    ]
    matching = {"key": "dedicated", "value": "vm", "operator": ""}
    assert node_exclusion(current, tolerations=[matching]) is None
    matching["value"] = "other"
    assert node_exclusion(current, tolerations=[matching]) == "node_taints"
    matching["operator"] = "unknown"
    assert node_exclusion(current, tolerations=[matching]) == "invalid_placement"


@pytest.mark.parametrize(
    "req,match",
    [
        (requirement("zone", "In", "a"), True),
        (requirement("zone", "NotIn", "a"), False),
        (requirement("missing", "NotIn", "a"), True),
        (requirement("zone", "Exists"), True),
        (requirement("missing", "DoesNotExist"), True),
        (requirement("generation", "Gt", "9"), True),
        (requirement("generation", "Lt", "9"), False),
        (requirement("zone", "Gt", "9"), False),
    ],
)
def test_affinity_selector_operators(req, match):
    assert node_exclusion(node(), pv_affinity=affinity(req)) == (
        None if match else "storage_topology"
    )


def test_affinity_or_terms_and_requirements_and_separate_vm_and_pv_constraints():
    first = affinity(requirement("zone", "In", "b"))
    first["nodeSelectorTerms"].extend(
        affinity(requirement("zone", "In", "a"), requirement("generation", "Gt", "9"))[
            "nodeSelectorTerms"
        ]
    )
    assert node_exclusion(node(), required_affinity=first) is None
    assert (
        node_exclusion(
            node(),
            required_affinity=first,
            pv_affinity=affinity(requirement("zone", "In", "b")),
        )
        == "storage_topology"
    )
    assert (
        node_exclusion(node(), required_affinity={"nodeSelectorTerms": []})
        == "node_affinity"
    )
    assert (
        node_exclusion(node(), required_affinity={"nodeSelectorTerms": [{}]})
        == "node_affinity"
    )
    assert (
        node_exclusion(
            node(),
            pv_affinity=affinity(fields=[requirement("metadata.name", "In", "node-a")]),
        )
        is None
    )


@pytest.mark.parametrize(
    "bad",
    [
        affinity(requirement("zone", "In")),
        affinity(requirement("zone", "Exists", "a")),
        affinity(requirement("generation", "Gt", "1.5")),
        affinity(requirement("zone", "Unknown", "a")),
        affinity(fields=[requirement("metadata.name", "In", "node-a", "node-b")]),
        affinity(fields=[requirement("unknown", "In", "x")]),
        {"nodeSelectorTerms": None},
    ],
)
def test_invalid_affinity_is_not_eligible_even_with_another_matching_or_term(bad):
    current = deepcopy(bad)
    if isinstance(current.get("nodeSelectorTerms"), list):
        current["nodeSelectorTerms"].extend(
            affinity(requirement("zone", "In", "a"))["nodeSelectorTerms"]
        )
    assert node_exclusion(node(), pv_affinity=current) == "invalid_placement"
