"""Sanitized inventory is bounded evidence, never a resource reservation."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

import pytest

from shared.vm_resource_inventory import (
    InventoryError,
    canonical_snapshot,
    snapshot_digest,
    snapshot_is_fresh,
)


def snapshot():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "protocol": 1,
        "snapshot_id": str(uuid4()),
        "cluster_id": "test-cluster",
        "controller_id": str(uuid4()),
        "sequence": 1,
        "namespace": "workers",
        "policy_digest": "sha256:" + "a" * 64,
        "started_at": now,
        "finished_at": now,
        "complete": True,
        "reason": None,
        "label_keys": ["kubernetes.io/hostname", "zone"],
        "resource_versions": {
            kind: "10"
            for kind in (
                "nodes",
                "pods",
                "vms",
                "vmis",
                "pvcs",
                "pvs",
                "storage_classes",
                "dvs",
            )
        },
        "nodes": [
            {
                "uid": str(uuid4()),
                "name": "node-a",
                "labels": {"zone": "a"},
                "ready": True,
                "unschedulable": False,
                "taints": [],
                "allocatable": {
                    "cpu_millicores": 4000,
                    "memory_bytes": 8 * 1024**3,
                    "kvm_devices": 10,
                },
            }
        ],
        "pods": [],
        "vms": [],
        "vmis": [],
        "pvcs": [],
        "pvs": [],
        "storage_classes": [],
        "dvs": [],
    }


def validate(value, *, max_items=100, max_bytes=100000):
    return canonical_snapshot(value, max_items=max_items, max_bytes=max_bytes)


def test_inventory_digest_is_canonical_without_changing_input():
    raw = snapshot()
    before = deepcopy(raw)
    result = validate(raw)
    assert raw == before
    assert snapshot_digest(result) == snapshot_digest(
        dict(reversed(list(result.items())))
    )
    raw["nodes"][0]["allocatable"]["cpu_millicores"] += 1
    assert snapshot_digest(validate(raw)) != snapshot_digest(result)


@pytest.mark.parametrize("kubevirt_version", ["v1.6.6", "v1.8.4"])
def test_protocol_two_requires_all_six_dimensions_and_installed_profile_proof(kubevirt_version):
    from shared.vm_launcher_profile import default_launcher_profile

    raw = snapshot()
    raw["protocol"] = 2
    raw["label_keys"].append("kubernetes.io/arch")
    raw["nodes"][0]["labels"]["kubernetes.io/arch"] = "amd64"
    raw["nodes"][0]["allocatable"].update(
        ephemeral_storage_bytes=1000000000, tun_devices=8, vhost_net_devices=8,
    )
    raw["resource_versions"].update(kubevirt="10", limitranges="10")
    raw["installed_profile"] = {
        "uid": str(uuid4()),
        "namespace": "kubevirt",
        "name": "kubevirt",
        "generation": 2,
        "observedGeneration": 2,
        "targetVersion": kubevirt_version,
        "observedVersion": kubevirt_version,
        "targetDeploymentID": "settled",
        "observedDeploymentID": "settled",
        "profile": default_launcher_profile(),
    }
    raw["installed_profile"]["profile"].update(
        kubevirtVersion=kubevirt_version,
        costAlgorithm=f"kubevirt-{kubevirt_version}-amd64-ordinary-pvc-v1",
    )
    assert validate(raw)["protocol"] == 2
    broken = deepcopy(raw)
    broken["installed_profile"]["profile"]["kubevirtVersion"] = "v1.8.4" if kubevirt_version == "v1.6.6" else "v1.6.6"
    broken["installed_profile"]["profile"]["costAlgorithm"] = f"kubevirt-{broken['installed_profile']['profile']['kubevirtVersion']}-amd64-ordinary-pvc-v1"
    with pytest.raises(InventoryError):
        validate(broken)

    for key in ("ephemeral_storage_bytes", "tun_devices", "vhost_net_devices"):
        broken = deepcopy(raw)
        del broken["nodes"][0]["allocatable"][key]
        with pytest.raises(InventoryError):
            validate(broken)
    broken = deepcopy(raw)
    del broken["installed_profile"]
    with pytest.raises(InventoryError):
        validate(broken)
    broken = deepcopy(raw)
    broken["installed_profile"]["profile"]["cpuAllocationRatio"] += 1
    assert snapshot_digest(validate(broken)) != snapshot_digest(validate(raw))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(protocol=True),
        lambda s: s.update(snapshot_id="missing"),
        lambda s: s.update(sequence=0),
        lambda s: s.update(started_at="2026-09-20T10:00:00"),
        lambda s: s["nodes"][0]["allocatable"].update(cpu_millicores=True),
        lambda s: s["nodes"][0]["allocatable"].update(memory_bytes=-1),
        lambda s: s["nodes"][0]["allocatable"].update(kvm_devices=0.5),
        lambda s: s["nodes"][0].update(environment={"PRIVATE": "do-not-emit"}),
        lambda s: s["nodes"][0]["labels"].update(uncollected="do-not-emit"),
        lambda s: s["resource_versions"].pop("pods"),
        lambda s: s["nodes"].append(deepcopy(s["nodes"][0])),
        lambda s: s.update(raw_annotations={"secret": "do-not-emit"}),
    ],
)
def test_malformed_or_unsanitized_inventory_is_refused_without_raw_details(mutate):
    raw = snapshot()
    mutate(raw)
    with pytest.raises(InventoryError) as exc:
        validate(raw)
    assert "do-not-emit" not in str(exc.value)


def test_item_and_encoded_byte_limits_include_single_large_items():
    with pytest.raises(InventoryError, match="item_limit"):
        validate(snapshot(), max_items=0)
    with pytest.raises(InventoryError, match="byte_limit"):
        validate(snapshot(), max_bytes=10)


def test_incomplete_snapshot_contains_no_partial_capacity_to_reuse():
    raw = snapshot()
    raw.update(complete=False, reason="collection_failed")
    with pytest.raises(InventoryError):
        validate(raw)
    raw["nodes"] = []
    raw["resource_versions"] = {}
    assert validate(raw)["complete"] is False
    raw["reason"] = "raw Kubernetes exception containing a secret"
    with pytest.raises(InventoryError):
        validate(raw)


def test_unknown_topology_label_cannot_be_treated_as_absent():
    raw = snapshot()
    raw["pvs"] = [
        {
            "uid": str(uuid4()),
            "name": "pv-a",
            "claim_uid": None,
            "required_affinity": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "uncollected",
                                "operator": "DoesNotExist",
                                "values": [],
                            }
                        ],
                        "matchFields": [],
                    }
                ]
            },
        }
    ]
    with pytest.raises(InventoryError, match="label_coverage"):
        validate(raw)


def test_freshness_uses_collection_start_and_receipt_not_only_latest_receipt():
    raw = snapshot()
    now = datetime.now(timezone.utc)
    raw["started_at"] = (now - timedelta(seconds=120)).isoformat()
    raw["finished_at"] = now.isoformat()
    assert not snapshot_is_fresh(
        validate(raw), received_at=now, now=now, stale_after_seconds=60
    )
    raw["started_at"] = raw["finished_at"]
    assert snapshot_is_fresh(
        validate(raw), received_at=now, now=now, stale_after_seconds=60
    )
    assert not snapshot_is_fresh(
        validate(raw),
        received_at=now,
        now=now + timedelta(seconds=61),
        stale_after_seconds=60,
    )
    raw["started_at"] = raw["finished_at"] = (now + timedelta(seconds=1)).isoformat()
    assert not snapshot_is_fresh(
        validate(raw), received_at=now, now=now, stale_after_seconds=60
    )


def test_pod_wire_does_not_allow_raw_metadata_or_ambiguous_owner_reference():
    raw = snapshot()
    raw["pods"] = [
        {
            "uid": str(uuid4()),
            "namespace": "workers",
            "name": "launcher",
            "node_uid": raw["nodes"][0]["uid"],
            "node_name": "node-a",
            "terminal": False,
            "deleting": True,
            "requests": {"cpu_millicores": 100, "memory_bytes": 100, "kvm_devices": 1},
            "vmi_uid": None,
            "reservation_id": None,
            "provision_generation": None,
        }
    ]
    assert validate(raw)["pods"][0]["deleting"] is True
    raw["pods"][0]["node_uid"] = str(uuid4())
    with pytest.raises(InventoryError, match="identity_unproven"):
        validate(raw)


def test_digest_does_not_accept_nonfinite_or_nonjson_values():
    with pytest.raises(InventoryError):
        snapshot_digest({"invalid": float("nan")})
    assert len(json.dumps(validate(snapshot()))) < 100000


@pytest.mark.parametrize("pod_placement", ["other-node", "unscheduled"])
def test_running_vmi_and_its_launcher_require_consistent_node_identity(pod_placement):
    raw = snapshot()
    first = raw["nodes"][0]
    second = {**deepcopy(first), "uid": str(uuid4()), "name": "node-b"}
    raw["nodes"].append(second)
    vmi = {
        "uid": str(uuid4()),
        "name": "vmi-a",
        "vm_uid": None,
        "node_uid": first["uid"],
        "node_name": first["name"],
        "phase": "Running",
        "deleting": False,
    }
    raw["vmis"] = [vmi]
    raw["pods"] = [
        {
            "uid": str(uuid4()),
            "namespace": "workers",
            "name": "launcher",
            "node_uid": second["uid"] if pod_placement == "other-node" else None,
            "node_name": second["name"] if pod_placement == "other-node" else None,
            "terminal": False,
            "deleting": False,
            "requests": {"cpu_millicores": 100, "memory_bytes": 100, "kvm_devices": 1},
            "vmi_uid": vmi["uid"],
            "reservation_id": None,
            "provision_generation": None,
        }
    ]
    with pytest.raises(InventoryError, match="identity_unproven"):
        validate(raw)


def test_complete_inventory_requires_exact_datavolume_target_coverage():
    raw = snapshot()
    raw["dvs"] = [
        {
            "uid": str(uuid4()),
            "name": "root",
            "pvc_uid": str(uuid4()),
            "pvc_name": "root",
            "succeeded": True,
            "deleting": False,
        }
    ]
    with pytest.raises(InventoryError, match="identity_unproven"):
        validate(raw)
