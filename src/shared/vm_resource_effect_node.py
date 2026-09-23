"""Fresh bounded Kubernetes node/topology proof before a resource-bound POST."""

from collections.abc import Mapping
import time

from shared.vm_resource_admission import ResourceAdmissionError, ResourceVector
from shared.vm_resource_placement import node_exclusion


def _node_document(node):
    allocatable = node["allocatable"]
    return {
        "metadata": {
            "uid": node["uid"], "name": node["name"], "labels": node["labels"],
        },
        "spec": {"unschedulable": node["unschedulable"], "taints": node["taints"]},
        "status": {
            "conditions": [{
                "type": "Ready", "status": "True" if node["ready"] else "False",
            }],
            "allocatable": {
                "cpu": str(allocatable["cpu_millicores"]) + "m",
                "memory": str(allocatable["memory_bytes"]),
                "ephemeral-storage": str(allocatable["ephemeral_storage_bytes"]),
                "devices.kubevirt.io/kvm": str(allocatable["kvm_devices"]),
                "devices.kubevirt.io/tun": str(allocatable["tun_devices"]),
                "devices.kubevirt.io/vhost-net": str(allocatable["vhost_net_devices"]),
            },
        },
    }


def validate_resource_effect_node(snapshot, *, grant, resource, expected_pvc_uid):
    """Require selected Node and storage topology from one complete fresh read."""
    try:
        if (
            not isinstance(snapshot, Mapping)
            or snapshot["protocol"] != 2
            or snapshot["complete"] is not True
            or snapshot["cluster_id"] != grant["cluster_id"]
            or snapshot["policy_digest"] != grant["policy_digest"]
            or snapshot["installed_profile"]["profile"] != resource["launcher_profile"]
        ):
            raise ValueError
        matches = [
            node for node in snapshot["nodes"]
            if node["uid"] == grant["node_uid"] and node["name"] == grant["node_name"]
        ]
        if len(matches) != 1:
            raise ValueError
        node = matches[0]
        if (
            node["labels"].get("kubernetes.io/hostname") != node["name"]
            or node["labels"].get("kubernetes.io/arch") != "amd64"
        ):
            raise ValueError
        demand = ResourceVector.from_six_dict(grant["vector"])
        headroom = ResourceVector.from_six_dict(grant["headroom"])
        available = ResourceVector.from_six_dict(node["allocatable"])
        if not (demand + headroom).fits(available):
            raise ValueError
        profile = resource["template_profile"]
        classes = [
            item for item in snapshot["storage_classes"]
            if item["name"] == profile["storage_class"]
        ]
        if len(classes) != 1:
            raise ValueError
        selected = _node_document(node)
        if node_exclusion(
            selected, selector=profile["selector"],
            tolerations=profile["tolerations"],
            required_affinity=profile["required_affinity"],
            pv_affinity=classes[0]["allowed_topology"],
        ) is not None:
            raise ValueError
        if expected_pvc_uid is not None:
            pvcs = [
                item for item in snapshot["pvcs"]
                if item["uid"] == expected_pvc_uid
            ]
            if len(pvcs) != 1 or pvcs[0]["phase"] != "Bound" or (
                pvcs[0]["storage_class_uid"] != classes[0]["uid"]
            ):
                raise ValueError
            pvs = [
                item for item in snapshot["pvs"]
                if item["uid"] == pvcs[0]["pv_uid"]
                and item["name"] == pvcs[0]["pv_name"]
                and item["claim_uid"] == expected_pvc_uid
            ]
            if len(pvs) != 1 or node_exclusion(
                selected, pv_affinity=pvs[0]["required_affinity"],
            ) is not None:
                raise ValueError
    except (ValueError, KeyError, TypeError, ResourceAdmissionError):
        raise ResourceAdmissionError("resource_node_changed") from None


async def fresh_resource_effect_node(controller, row, grant):
    collector = getattr(controller, "resource_inventory_collector", None)
    if collector is None:
        raise ResourceAdmissionError("resource_inventory_unavailable")
    try:
        snapshot = await collector.collect(time.time_ns())
        validate_resource_effect_node(
            snapshot,
            grant=grant,
            resource=row["controller_configuration"]["resource_admission"],
            expected_pvc_uid=row["expected_pvc_uid"],
        )
    except ResourceAdmissionError:
        raise
    except Exception:
        raise ResourceAdmissionError("resource_inventory_unavailable") from None
