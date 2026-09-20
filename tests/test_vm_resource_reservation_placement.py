"""Exact retained topology and strict projection inputs at the SQL fit seam."""

from copy import deepcopy
from uuid import uuid4

import pytest

from orchestrator.services.vm_resource_reservation_store import _fit
from shared.vm_resource_accounting import account_inventory
from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector
from tests.test_vm_resource_inventory_contract import snapshot


def fixture():
    observation = snapshot()
    node = observation["nodes"][0]
    node["labels"]["kubernetes.io/hostname"] = "node-a"
    sc, pvc, pv = str(uuid4()), str(uuid4()), str(uuid4())
    observation["storage_classes"] = [
        {
            "uid": sc,
            "name": "local",
            "binding_mode": "WaitForFirstConsumer",
            "allowed_topology": None,
        }
    ]
    observation["pvcs"] = [
        {
            "uid": pvc,
            "name": "root",
            "pv_uid": pv,
            "pv_name": "retained",
            "storage_class_uid": sc,
            "phase": "Bound",
        }
    ]
    observation["pvs"] = [
        {
            "uid": pv,
            "name": "retained",
            "claim_uid": pvc,
            "required_affinity": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {"key": "zone", "operator": "In", "values": ["a"]}
                        ]
                    }
                ]
            },
        }
    ]
    waiter = {
        "cpu_millicores": 1000,
        "memory_bytes": 1024**3,
        "kvm_devices": 1,
        "placement": {
            "version": 1,
            "selector": {},
            "tolerations": [],
            "required_affinity": None,
            "storage_class": "local",
            "retained_pvc_uid": pvc,
        },
    }
    return observation, waiter


def fit(observation, waiter):
    headroom = ResourceVector(0, 0, 0)
    accounting = account_inventory(observation, [], headroom=headroom)
    return _fit(observation, waiter, accounting, headroom)


def test_retained_pvc_requires_both_pv_and_storage_class_topology():
    observation, waiter = fixture()
    assert fit(observation, waiter) == ([observation["nodes"][0]["uid"]], False)
    topology = deepcopy(observation["pvs"][0]["required_affinity"])
    topology["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] = ["b"]
    observation["storage_classes"][0]["allowed_topology"] = topology
    assert fit(observation, waiter) == ([], False)


@pytest.mark.parametrize(
    "change",
    ["missing", "unbound", "pv_uid", "pv_name", "claim_uid", "storage_class_uid"],
)
def test_retained_missing_or_conflicting_exact_chain_stays_waiting(change):
    observation, waiter = fixture()
    if change == "missing":
        observation["pvcs"] = []
    elif change == "unbound":
        observation["pvcs"][0]["phase"] = "Pending"
    elif change == "claim_uid":
        observation["pvs"][0]["claim_uid"] = str(uuid4())
    else:
        observation["pvcs"][0][change] = str(uuid4())
    assert fit(observation, waiter) == ([], False)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("selector", []),
        ("tolerations", {}),
        ("storage_class", ""),
        ("retained_pvc_uid", "invalid"),
    ],
)
def test_malformed_placement_has_bounded_failure(field, value):
    observation, waiter = fixture()
    waiter["placement"][field] = value
    with pytest.raises(ResourceAdmissionError, match="invalid_resource_placement"):
        fit(observation, waiter)
