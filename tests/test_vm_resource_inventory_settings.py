from copy import deepcopy
import json

import pytest

from shared.vm_resource_inventory import InventoryError
from shared.vm_resource_inventory_settings import InventorySettings


def configuration():
    return {
        "mode": "same-cluster",
        "namespace": "workers",
        "policy": {
            "observerEnabled": True,
            "shadowEnabled": False,
            "enforcementEnabled": False,
            "clusterWidePodReadAcknowledged": True,
            "stableClusterId": "test-cluster",
            "inventory": {
                "publishIntervalSeconds": 10,
                "staleAfterSeconds": 60,
                "maxItems": 1000,
                "maxBytes": 100000,
                "requestTimeoutSeconds": 5,
                "collectionTimeoutSeconds": 20,
                "publicationTimeoutSeconds": 5,
                "historyLimit": 3,
                "nodeLabelKeys": ["kubernetes.io/hostname", "zone"],
            },
            "hostCost": {},
            "nodeHeadroom": {},
            "fairness": {},
        },
    }


def load(value=None, secret="inventory-settings-secret-at-least-32-bytes"):
    return InventorySettings.from_environment(
        {
            "VM_RESOURCE_ADMISSION_CONFIG": json.dumps(
                configuration() if value is None else value
            ),
            "VM_LIFECYCLE_HMAC_SECRET": secret,
        }
    )


def test_inventory_off_requires_no_policy_or_hmac():
    assert InventorySettings.from_environment({}) is None
    value = configuration()
    value["policy"]["observerEnabled"] = False
    value["policy"]["inventory"] = {}
    assert load(value, secret="") is None
    value.update(mode="external", namespace="")
    value["policy"].update(
        clusterWidePodReadAcknowledged=False,
        stableClusterId="",
        inventory=None,
        hostCost=None,
        nodeHeadroom=None,
        fairness=None,
    )
    assert load(value, secret="") is None


def test_enforced_whole_launcher_inventory_uses_the_installed_policy_identity():
    from tests.test_vm_resource_policy import whole_launcher_policy

    value = whole_launcher_policy()
    value["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    settings = load(value)
    assert settings.protocol == 2
    assert settings.kubevirt_namespace == "kubevirt"
    assert settings.kubevirt_name == "kubevirt"
    assert settings.policy_digest.startswith("sha256:")


def test_policy_digest_is_canonical_and_covers_scope_and_label_coverage():
    value = configuration()
    first = load(value)
    assert (
        first.policy_digest == load(dict(reversed(list(value.items())))).policy_digest
    )
    for change in ("scope", "label", "limit"):
        other = deepcopy(value)
        if change == "scope":
            other["namespace"] = "other"
        elif change == "label":
            other["policy"]["inventory"]["nodeLabelKeys"].append("other")
        else:
            other["policy"]["inventory"]["maxItems"] += 1
        assert first.policy_digest != load(other).policy_digest


@pytest.mark.parametrize(
    "change",
    [
        "ack",
        "cluster",
        "hmac",
        "mode",
        "shadow",
        "enforce",
        "unknown",
        "bool",
        "fraction",
        "zero",
        "missing",
        "duplicate-label",
        "hostname",
    ],
)
def test_enabled_inventory_requires_explicit_complete_and_supported_policy(change):
    value = configuration()
    policy, inv = value["policy"], value["policy"]["inventory"]
    if change == "ack":
        policy["clusterWidePodReadAcknowledged"] = False
    elif change == "cluster":
        policy["stableClusterId"] = ""
    elif change == "mode":
        value["mode"] = "external"
    elif change == "shadow":
        policy["shadowEnabled"] = True
    elif change == "enforce":
        policy["enforcementEnabled"] = True
    elif change == "unknown":
        inv["unknown"] = 1
    elif change == "bool":
        inv["maxItems"] = True
    elif change == "fraction":
        inv["maxItems"] = 1.5
    elif change == "zero":
        inv["historyLimit"] = 0
    elif change == "missing":
        inv.pop("collectionTimeoutSeconds")
    elif change == "duplicate-label":
        inv["nodeLabelKeys"].append("zone")
    elif change == "hostname":
        inv["nodeLabelKeys"] = ["zone"]
    with pytest.raises(InventoryError):
        load(value, secret="" if change == "hmac" else "s" * 32)
