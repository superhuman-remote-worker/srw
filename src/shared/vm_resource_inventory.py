"""Strict sanitized inventory wire contract; no scheduling or effect authority."""

from datetime import datetime, timezone
import hashlib
import json
import re
from uuid import UUID

from shared.vm_resource_admission import ResourceVector, ResourceAdmissionError
from shared.vm_resource_placement import affinity_label_keys, preferred_taint_count


INVENTORY_KINDS = (
    "nodes",
    "pods",
    "vms",
    "vmis",
    "pvcs",
    "pvs",
    "storage_classes",
    "dvs",
)
INCOMPLETE_REASONS = frozenset(
    {
        "collection_failed",
        "collection_incomplete",
        "item_limit",
        "byte_limit",
        "normalization_failed",
        "identity_unproven",
        "label_coverage",
        "collection_stale",
    }
)
_FIELDS = frozenset(
    {
        "protocol",
        "snapshot_id",
        "cluster_id",
        "controller_id",
        "sequence",
        "namespace",
        "policy_digest",
        "started_at",
        "finished_at",
        "complete",
        "reason",
        "label_keys",
        "resource_versions",
        *INVENTORY_KINDS,
    }
)
_V2_FIELDS = _FIELDS | {"installed_profile"}
_V2_VERSIONS = frozenset(INVENTORY_KINDS) | {"kubevirt", "limitranges"}


class InventoryError(ValueError):
    """A fixed safe reason; never return validation details from a raw object."""


def _record(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise InventoryError("invalid_inventory")
    return value


def _text(value, *, empty=False):
    if (
        not isinstance(value, str)
        or len(value) > 253
        or value != value.strip()
        or (not empty and not value)
    ):
        raise InventoryError("invalid_inventory")
    return value


def _uuid(value, *, nullable=False):
    if value is None and nullable:
        return None
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise InventoryError("invalid_inventory") from None
    return value


def _boolean(value):
    if type(value) is not bool:
        raise InventoryError("invalid_inventory")


def _integer(value, *, positive=False):
    if type(value) is not int or not int(positive) <= value < 2**63:
        raise InventoryError("invalid_inventory")


def _list(value):
    if not isinstance(value, list):
        raise InventoryError("invalid_inventory")
    return value


def inventory_time(value):
    try:
        if not isinstance(value, str) or len(value) > 40:
            raise ValueError
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise InventoryError("invalid_inventory_time") from None


def _encoded(value):
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise InventoryError("invalid_inventory") from None


def snapshot_digest(value):
    return "sha256:" + hashlib.sha256(_encoded(value)).hexdigest()


def _resources(value, *, protocol):
    if protocol == 1:
        _record(value, ("cpu_millicores", "memory_bytes", "kvm_devices"))
        ResourceVector(**value)
    else:
        ResourceVector.from_six_dict(value)


def _installed_profile(value):
    from shared.vm_launcher_profile import validate_launcher_profile

    _record(
        value,
        (
            "uid", "namespace", "name", "generation", "observedGeneration",
            "targetVersion", "observedVersion", "targetDeploymentID",
            "observedDeploymentID", "profile",
        ),
    )
    _uuid(value["uid"])
    _text(value["namespace"])
    _text(value["name"])
    _text(value["targetDeploymentID"])
    _text(value["observedDeploymentID"])
    _integer(value["generation"], positive=True)
    _integer(value["observedGeneration"], positive=True)
    if (
        value["generation"] != value["observedGeneration"]
        or value["observedVersion"] != value["targetVersion"]
        or value["observedDeploymentID"] != value["targetDeploymentID"]
    ):
        raise InventoryError("invalid_inventory")
    profile = validate_launcher_profile(value["profile"])
    if value["targetVersion"] != profile["kubevirtVersion"]:
        raise InventoryError("invalid_inventory")


def _affinity(value, label_keys):
    if not affinity_label_keys(value) <= label_keys:
        raise InventoryError("label_coverage")


def _node(value, label_keys, *, protocol):
    _record(
        value,
        ("uid", "name", "labels", "ready", "unschedulable", "taints", "allocatable"),
    )
    labels = value["labels"]
    if not isinstance(labels, dict) or not set(labels) <= label_keys:
        raise InventoryError("label_coverage")
    for key, item in labels.items():
        _text(key)
        _text(item, empty=True)
    _boolean(value["ready"])
    _boolean(value["unschedulable"])
    for taint in _list(value["taints"]):
        _record(taint, ("key", "value", "effect"))
    preferred_taint_count({"spec": {"taints": value["taints"]}}, [])
    if protocol == 2 and labels.get("kubernetes.io/arch") not in {"amd64", "arm64", "ppc64le", "s390x"}:
        raise InventoryError("identity_unproven")
    _resources(value["allocatable"], protocol=protocol)


def _pod(value, *, protocol):
    _record(
        value,
        (
            "uid",
            "namespace",
            "name",
            "node_uid",
            "node_name",
            "terminal",
            "deleting",
            "requests",
            "vmi_uid",
            "reservation_id",
            "provision_generation",
        ),
    )
    _text(value["namespace"])
    _boolean(value["terminal"])
    _boolean(value["deleting"])
    _resources(value["requests"], protocol=protocol)
    for key in ("node_uid", "vmi_uid", "reservation_id", "provision_generation"):
        _uuid(value[key], nullable=True)
    if value["node_name"] is not None:
        _text(value["node_name"])


def _vm(value):
    _record(
        value,
        ("uid", "name", "owner_kind", "owner_id", "provision_generation", "deleting"),
    )
    if value["owner_kind"] not in (None, "job", "thread"):
        raise InventoryError("invalid_inventory")
    _uuid(value["owner_id"], nullable=True)
    _uuid(value["provision_generation"], nullable=True)
    if (value["owner_id"] is None) != (value["owner_kind"] is None):
        raise InventoryError("identity_unproven")
    _boolean(value["deleting"])


def _vmi(value):
    _record(
        value, ("uid", "name", "vm_uid", "node_uid", "node_name", "phase", "deleting")
    )
    for key in ("vm_uid", "node_uid"):
        _uuid(value[key], nullable=True)
    if value["node_name"] is not None:
        _text(value["node_name"])
    if value["phase"] not in {
        "Pending",
        "Scheduling",
        "Scheduled",
        "Running",
        "Succeeded",
        "Failed",
        "Unknown",
    }:
        raise InventoryError("invalid_inventory")
    _boolean(value["deleting"])


def _pvc(value):
    _record(value, ("uid", "name", "pv_uid", "pv_name", "storage_class_uid", "phase"))
    _uuid(value["pv_uid"], nullable=True)
    _uuid(value["storage_class_uid"], nullable=True)
    if value["pv_name"] is not None:
        _text(value["pv_name"])
    if value["phase"] not in {"Pending", "Bound", "Lost"}:
        raise InventoryError("invalid_inventory")
    if (value["pv_uid"] is None) != (value["pv_name"] is None):
        raise InventoryError("identity_unproven")
    if value["phase"] == "Bound" and value["pv_uid"] is None:
        raise InventoryError("identity_unproven")


def _pv(value, label_keys):
    _record(value, ("uid", "name", "claim_uid", "required_affinity"))
    _uuid(value["claim_uid"], nullable=True)
    _affinity(value["required_affinity"], label_keys)


def _dv(value):
    _record(value, ("uid", "name", "pvc_uid", "pvc_name", "succeeded", "deleting"))
    _uuid(value["pvc_uid"], nullable=True)
    _boolean(value["succeeded"])
    _boolean(value["deleting"])
    if value["pvc_name"] is not None:
        _text(value["pvc_name"])
    if (value["pvc_uid"] is None) != (value["pvc_name"] is None):
        raise InventoryError("identity_unproven")
    if value["succeeded"] and value["pvc_uid"] is None:
        raise InventoryError("identity_unproven")


def _storage_class(value, label_keys):
    _record(value, ("uid", "name", "binding_mode", "allowed_topology"))
    if value["binding_mode"] not in {"Immediate", "WaitForFirstConsumer"}:
        raise InventoryError("invalid_inventory")
    _affinity(value["allowed_topology"], label_keys)


def _identities(snapshot):
    nodes = {node["uid"]: node["name"] for node in snapshot["nodes"]}
    vms = {vm["uid"] for vm in snapshot["vms"]}
    vmis = {vmi["uid"]: vmi for vmi in snapshot["vmis"]}
    pvcs = {pvc["uid"]: pvc for pvc in snapshot["pvcs"]}
    pvs = {pv["uid"]: pv for pv in snapshot["pvs"]}
    classes = {item["uid"] for item in snapshot["storage_classes"]}
    for pod in snapshot["pods"]:
        if pod["node_uid"] is not None:
            if nodes.get(pod["node_uid"]) != pod["node_name"]:
                raise InventoryError("identity_unproven")
        elif pod["node_name"] is not None and not pod["terminal"]:
            raise InventoryError("identity_unproven")
        if pod["vmi_uid"] is not None and (
            pod["namespace"] != snapshot["namespace"] or pod["vmi_uid"] not in vmis
        ):
            raise InventoryError("identity_unproven")
        if pod["vmi_uid"] is not None and not pod["terminal"]:
            vmi = vmis[pod["vmi_uid"]]
            if (vmi["node_uid"] is not None and pod["node_uid"] != vmi["node_uid"]) or (
                vmi["phase"] == "Running" and pod["node_uid"] is None
            ):
                raise InventoryError("identity_unproven")
    for vmi in snapshot["vmis"]:
        if vmi["vm_uid"] is not None and vmi["vm_uid"] not in vms:
            raise InventoryError("identity_unproven")
        if (vmi["node_uid"] is None) != (vmi["node_name"] is None) or (
            vmi["node_uid"] is not None
            and nodes.get(vmi["node_uid"]) != vmi["node_name"]
        ):
            raise InventoryError("identity_unproven")
    for pvc in snapshot["pvcs"]:
        if (
            pvc["storage_class_uid"] is not None
            and pvc["storage_class_uid"] not in classes
        ):
            raise InventoryError("identity_unproven")
        if pvc["pv_uid"] is not None:
            pv = pvs.get(pvc["pv_uid"])
            if (
                pv is None
                or pv["name"] != pvc["pv_name"]
                or pv["claim_uid"] != pvc["uid"]
            ):
                raise InventoryError("identity_unproven")
    for dv in snapshot["dvs"]:
        if dv["pvc_uid"] is not None and (
            dv["pvc_uid"] not in pvcs or pvcs[dv["pvc_uid"]]["name"] != dv["pvc_name"]
        ):
            raise InventoryError("identity_unproven")


def canonical_snapshot(value, *, max_items: int, max_bytes: int):
    """Validate the entire strict document and return detached canonical JSON."""
    _integer(max_items)
    _integer(max_bytes)
    encoded = _encoded(value)
    if len(encoded) > max_bytes:
        raise InventoryError("byte_limit")
    result = json.loads(encoded)
    try:
        protocol = result["protocol"]
        if type(protocol) is not int or protocol not in (1, 2):
            raise InventoryError("invalid_inventory")
        _record(result, _FIELDS if protocol == 1 else _V2_FIELDS)
        _uuid(result["snapshot_id"])
        _uuid(result["controller_id"])
        _integer(result["sequence"], positive=True)
        _text(result["cluster_id"])
        _text(result["namespace"])
        if not isinstance(result["policy_digest"], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", result["policy_digest"]
        ):
            raise InventoryError("invalid_inventory")
        started, finished = (
            inventory_time(result["started_at"]),
            inventory_time(result["finished_at"]),
        )
        if finished < started:
            raise InventoryError("invalid_inventory_time")
        result["started_at"], result["finished_at"] = (
            started.isoformat(),
            finished.isoformat(),
        )
        _boolean(result["complete"])
        keys = _list(result["label_keys"])
        if len(keys) > 128 or len(set(keys)) != len(keys):
            raise InventoryError("invalid_inventory")
        for key in keys:
            _text(key)
        label_keys = frozenset(keys)
        if protocol == 2:
            if "kubernetes.io/arch" not in label_keys:
                raise InventoryError("label_coverage")
            if result["complete"]:
                _installed_profile(result["installed_profile"])
            elif result["installed_profile"] is not None:
                raise InventoryError("invalid_inventory")
        result["label_keys"] = sorted(keys)
        count = sum(len(_list(result[kind])) for kind in INVENTORY_KINDS)
        if count > max_items:
            raise InventoryError("item_limit")
        versions = result["resource_versions"]
        if result["complete"]:
            _record(versions, INVENTORY_KINDS if protocol == 1 else _V2_VERSIONS)
            for resource_version in versions.values():
                _text(resource_version)
            if result["reason"] is not None:
                raise InventoryError("invalid_inventory")
        elif result["reason"] not in INCOMPLETE_REASONS or count or versions != {}:
            raise InventoryError("invalid_inventory")
        validators = {
            "nodes": lambda item: _node(item, label_keys, protocol=protocol),
            "pods": lambda item: _pod(item, protocol=protocol),
            "vms": _vm,
            "vmis": _vmi,
            "pvcs": _pvc,
            "pvs": lambda item: _pv(item, label_keys),
            "dvs": _dv,
            "storage_classes": lambda item: _storage_class(item, label_keys),
        }
        for kind in INVENTORY_KINDS:
            uids, names = set(), set()
            for item in result[kind]:
                validators[kind](item)
                uid, name = _uuid(item["uid"]), _text(item["name"])
                key = (item.get("namespace"), name)
                if uid in uids or key in names:
                    raise InventoryError("identity_unproven")
                uids.add(uid)
                names.add(key)
            result[kind].sort(key=lambda item: item["uid"])
        _identities(result)
        return result
    except (KeyError, TypeError, ResourceAdmissionError):
        raise InventoryError("invalid_inventory") from None


def snapshot_is_fresh(snapshot, *, received_at, now, stale_after_seconds):
    _integer(stale_after_seconds, positive=True)
    if any(
        not isinstance(value, datetime) or value.tzinfo is None
        for value in (received_at, now)
    ):
        return False
    started = inventory_time(snapshot["started_at"])
    finished = inventory_time(snapshot["finished_at"])
    return (
        snapshot["complete"] is True
        and started <= finished <= received_at <= now
        and max((now - started).total_seconds(), (now - received_at).total_seconds())
        <= stale_after_seconds
    )
