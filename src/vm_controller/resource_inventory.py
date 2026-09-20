"""Bounded LIST inventory for VM admission; no create or reservation effects."""

import asyncio
from datetime import datetime, timezone
import time
from uuid import uuid4

from kubernetes.client import ApiClient

from shared.vm_resource_admission import effective_pod_request, scheduled_pod_charge
from shared.vm_resource_inventory import (
    INVENTORY_KINDS,
    INCOMPLETE_REASONS,
    InventoryError,
    canonical_snapshot,
)
from shared.vm_resource_placement import node_allocatable


_GENERATION = "srw.io/provision-generation"
_RESERVATION = "srw.io/vm-resource-reservation"


def _document(value):
    result = (
        value
        if isinstance(value, dict)
        else ApiClient().sanitize_for_serialization(value)
    )
    if not isinstance(result, dict):
        raise InventoryError("normalization_failed")
    return result


def _map(value):
    if not isinstance(value, dict):
        raise InventoryError("normalization_failed")
    return value


def _meta(value):
    return _map(value.get("metadata", {}))


def _owner(value, kind, owners, group):
    refs = _meta(value).get("ownerReferences", [])
    if not isinstance(refs, list):
        raise InventoryError("identity_unproven")
    controllers = [ref for ref in refs if _map(ref).get("controller") is True]
    if len(controllers) > 1:
        raise InventoryError("identity_unproven")
    if not controllers:
        return None
    ref = controllers[0]
    api_version = ref.get("apiVersion")
    if (
        ref.get("kind") != kind
        or not isinstance(api_version, str)
        or api_version.split("/")[:1] != [group]
    ):
        return None  # Unrelated controllers remain external Pod occupancy.
    if len(api_version.split("/")) != 2 or not api_version.split("/")[1]:
        raise InventoryError("identity_unproven")
    owner = owners.get(ref.get("name"))
    if owner is None or _meta(owner).get("uid") != ref.get("uid"):
        raise InventoryError("identity_unproven")
    return ref["uid"]


def _index(items, *, namespace=None):
    result, uids = {}, set()
    for item in items:
        meta = _meta(item)
        name, uid = meta.get("name"), meta.get("uid")
        if (
            not isinstance(name, str)
            or not isinstance(uid, str)
            or name in result
            or uid in uids
            or meta.get("namespace") != namespace
        ):
            raise InventoryError("identity_unproven")
        result[name] = item
        uids.add(uid)
    return result


def _required_affinity(pv):
    affinity = _map(pv.get("spec", {})).get("nodeAffinity")
    if affinity is None:
        return None
    if set(_map(affinity)) - {"required"}:
        raise InventoryError("normalization_failed")
    return affinity.get("required")


def _allowed_topology(storage_class):
    terms = storage_class.get("allowedTopologies", [])
    if not isinstance(terms, list):
        raise InventoryError("normalization_failed")
    if not terms:
        return None
    result = []
    for term in terms:
        if set(_map(term)) != {"matchLabelExpressions"}:
            raise InventoryError("normalization_failed")
        expressions = []
        for requirement in term["matchLabelExpressions"]:
            if set(_map(requirement)) != {"key", "values"}:
                raise InventoryError("normalization_failed")
            expressions.append(
                {
                    "key": requirement["key"],
                    "operator": "In",
                    "values": requirement["values"],
                }
            )
        result.append({"matchExpressions": expressions, "matchFields": []})
    return {"nodeSelectorTerms": result}


def normalize_inventory(raw, *, namespace, label_keys):
    """Build each public field explicitly. Raw object dictionaries stay local."""
    nodes, vms, vmis, pvcs, pvs, classes, dvs = (
        _index(
            raw[kind],
            namespace=namespace if kind in {"vms", "vmis", "pvcs", "dvs"} else None,
        )
        for kind in ("nodes", "vms", "vmis", "pvcs", "pvs", "storage_classes", "dvs")
    )
    result = {kind: [] for kind in INVENTORY_KINDS}
    for name, value in nodes.items():
        meta, spec, status = (
            _meta(value),
            _map(value.get("spec", {})),
            _map(value.get("status", {})),
        )
        ready = [
            condition.get("status")
            for condition in status.get("conditions", [])
            if _map(condition).get("type") == "Ready"
        ]
        if len(ready) > 1:
            raise InventoryError("identity_unproven")
        labels = _map(meta.get("labels", {}))
        result["nodes"].append(
            {
                "uid": meta.get("uid"),
                "name": name,
                "labels": {key: labels[key] for key in label_keys if key in labels},
                "ready": ready == ["True"],
                "unschedulable": spec.get("unschedulable", False),
                "taints": [
                    {
                        "key": _map(taint).get("key"),
                        "value": taint.get("value", ""),
                        "effect": taint.get("effect"),
                    }
                    for taint in spec.get("taints", [])
                ],
                "allocatable": node_allocatable(value).to_dict(),
            }
        )
    for name, value in vms.items():
        meta = _meta(value)
        labels = _map(meta.get("labels", {}))
        owner_kind = labels.get("srw.io/owner-kind")
        owner_id = labels.get("srw.io/owner-id")
        result["vms"].append(
            {
                "uid": meta["uid"],
                "name": name,
                "owner_kind": owner_kind,
                "owner_id": owner_id,
                "provision_generation": _map(meta.get("annotations", {})).get(
                    _GENERATION
                ),
                "deleting": meta.get("deletionTimestamp") is not None,
            }
        )
    for name, value in vmis.items():
        meta, status = _meta(value), _map(value.get("status", {}))
        node_name = status.get("nodeName") or None
        node_uid = _meta(nodes[node_name]).get("uid") if node_name in nodes else None
        result["vmis"].append(
            {
                "uid": meta["uid"],
                "name": name,
                "vm_uid": _owner(value, "VirtualMachine", vms, "kubevirt.io"),
                "node_name": node_name,
                "node_uid": node_uid,
                "phase": status.get("phase") or "Unknown",
                "deleting": meta.get("deletionTimestamp") is not None,
            }
        )
    for value in raw["pods"]:
        meta, spec, status = (
            _meta(value),
            _map(value.get("spec", {})),
            _map(value.get("status", {})),
        )
        # This validates lifecycle/resize evidence, including scheduled-without-node.
        scheduled_pod_charge(value)
        request = effective_pod_request(value)
        node_name = spec.get("nodeName") or None
        node_uid = _meta(nodes[node_name]).get("uid") if node_name in nodes else None
        annotations = _map(meta.get("annotations", {}))
        result["pods"].append(
            {
                "uid": meta.get("uid"),
                "name": meta.get("name"),
                "namespace": meta.get("namespace"),
                "node_uid": node_uid,
                "node_name": node_name,
                "requests": request.to_dict(),
                "terminal": status.get("phase") in {"Succeeded", "Failed"},
                "deleting": meta.get("deletionTimestamp") is not None,
                "vmi_uid": _owner(value, "VirtualMachineInstance", vmis, "kubevirt.io")
                if meta.get("namespace") == namespace
                else None,
                "reservation_id": annotations.get(_RESERVATION),
                "provision_generation": annotations.get(_GENERATION),
            }
        )
    required_pvs = set()
    for name, value in pvcs.items():
        meta, spec, status = (
            _meta(value),
            _map(value.get("spec", {})),
            _map(value.get("status", {})),
        )
        pv_name, class_name = (
            spec.get("volumeName") or None,
            spec.get("storageClassName") or None,
        )
        if pv_name is not None:
            required_pvs.add(pv_name)
        if class_name is not None and class_name not in classes:
            raise InventoryError("identity_unproven")
        result["pvcs"].append(
            {
                "uid": meta["uid"],
                "name": name,
                "pv_uid": _meta(pvs[pv_name]).get("uid") if pv_name in pvs else None,
                "pv_name": pv_name,
                "storage_class_uid": _meta(classes[class_name]).get("uid")
                if class_name in classes
                else None,
                "phase": status.get("phase") or "Pending",
            }
        )
    for name in sorted(required_pvs):
        if name not in pvs:
            raise InventoryError("identity_unproven")
        value = pvs[name]
        claim = _map(_map(value.get("spec", {})).get("claimRef", {}))
        if claim.get("namespace") != namespace:
            raise InventoryError("identity_unproven")
        result["pvs"].append(
            {
                "uid": _meta(value)["uid"],
                "name": name,
                "claim_uid": claim.get("uid"),
                "required_affinity": _required_affinity(value),
            }
        )
    for name, value in classes.items():
        result["storage_classes"].append(
            {
                "uid": _meta(value)["uid"],
                "name": name,
                "binding_mode": value.get("volumeBindingMode") or "Immediate",
                "allowed_topology": _allowed_topology(value),
            }
        )
    for name, value in dvs.items():
        meta, status = _meta(value), _map(value.get("status", {}))
        target_name = status.get("claimName") or name
        target = pvcs.get(target_name)
        if (
            target is not None
            and _owner(target, "DataVolume", dvs, "cdi.kubevirt.io") != meta["uid"]
        ):
            raise InventoryError("identity_unproven")
        result["dvs"].append(
            {
                "uid": meta["uid"],
                "name": name,
                "pvc_uid": _meta(target).get("uid") if target is not None else None,
                "pvc_name": target_name if target is not None else None,
                "succeeded": status.get("phase") == "Succeeded",
                "deleting": meta.get("deletionTimestamp") is not None,
            }
        )
    return result


class ResourceInventoryCollector:
    def __init__(
        self,
        *,
        core,
        custom,
        storage,
        namespace,
        cluster_id,
        controller_id,
        policy_digest,
        label_keys,
        max_items,
        max_bytes,
        request_timeout_seconds,
        collection_timeout_seconds,
    ):
        self.core, self.custom, self.storage = core, custom, storage
        self.namespace, self.cluster_id, self.controller_id = (
            namespace,
            cluster_id,
            controller_id,
        )
        self.policy_digest, self.label_keys = policy_digest, list(label_keys)
        self.max_items, self.max_bytes = max_items, max_bytes
        for limit in (
            max_items,
            max_bytes,
            request_timeout_seconds,
            collection_timeout_seconds,
        ):
            if type(limit) is not int or not 1 <= limit < 2**63:
                raise InventoryError("invalid_inventory_configuration")
        self.request_timeout_seconds, self.collection_timeout_seconds = (
            request_timeout_seconds,
            collection_timeout_seconds,
        )

    async def _call(self, method, **kwargs):
        task = asyncio.create_task(asyncio.to_thread(method, **kwargs))
        try:
            return _document(await asyncio.shield(task))
        except asyncio.CancelledError:
            # The caller must use an observer-owned client with retries=0. Its
            # bounded HTTP timeout terminates the thread; cancellation alone cannot.
            try:
                await task
            except Exception:
                pass
            raise

    async def _list(self, method, *, deadline, remaining, **kwargs):
        items, tokens, version, token = [], set(), None, None
        while True:
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise InventoryError("collection_stale")
            options = {
                **kwargs,
                "limit": min(500, max(1, remaining)),
                "_request_timeout": min(self.request_timeout_seconds, budget),
            }
            if token is not None:
                options["_continue"] = token
            response = await self._call(method, **options)
            if time.monotonic() > deadline:
                raise InventoryError("collection_stale")
            metadata = _map(response.get("metadata", {}))
            rv, following = (
                metadata.get("resourceVersion"),
                metadata.get("continue", ""),
            )
            batch = response.get("items")
            if (
                not isinstance(batch, list)
                or not isinstance(rv, str)
                or not rv
                or not isinstance(following, str)
                or (version is not None and rv != version)
            ):
                raise InventoryError("collection_incomplete")
            version = rv
            items.extend(batch)
            if len(items) > remaining:
                raise InventoryError("item_limit")
            if not following:
                return items, version
            if following in tokens:
                raise InventoryError("collection_incomplete")
            tokens.add(following)
            token = following

    async def collect(self, sequence):
        started = datetime.now(timezone.utc).isoformat()
        deadline = time.monotonic() + self.collection_timeout_seconds
        snapshot = {
            "protocol": 1,
            "snapshot_id": str(uuid4()),
            "cluster_id": self.cluster_id,
            "controller_id": self.controller_id,
            "sequence": sequence,
            "namespace": self.namespace,
            "policy_digest": self.policy_digest,
            "started_at": started,
            "finished_at": started,
            "label_keys": self.label_keys,
            "complete": False,
            "reason": "collection_incomplete",
            "resource_versions": {},
            **{kind: [] for kind in INVENTORY_KINDS},
        }
        try:
            raw, versions, count = {}, {}, 0
            calls = {
                "nodes": (self.core.list_node, {}),
                "pods": (self.core.list_pod_for_all_namespaces, {}),
                "pvcs": (
                    self.core.list_namespaced_persistent_volume_claim,
                    {"namespace": self.namespace},
                ),
                "pvs": (self.core.list_persistent_volume, {}),
                "storage_classes": (self.storage.list_storage_class, {}),
                **{
                    kind: (
                        self.custom.list_namespaced_custom_object,
                        {
                            "namespace": self.namespace,
                            "group": "cdi.kubevirt.io"
                            if kind == "dvs"
                            else "kubevirt.io",
                            "version": "v1beta1" if kind == "dvs" else "v1",
                            "plural": plural,
                        },
                    )
                    for kind, plural in (
                        ("vms", "virtualmachines"),
                        ("vmis", "virtualmachineinstances"),
                        ("dvs", "datavolumes"),
                    )
                },
            }
            for kind, (method, kwargs) in calls.items():
                raw[kind], versions[kind] = await self._list(
                    method,
                    deadline=deadline,
                    remaining=self.max_items - count,
                    **kwargs,
                )
                count += len(raw[kind])
            snapshot.update(
                normalize_inventory(
                    raw, namespace=self.namespace, label_keys=self.label_keys
                )
            )
            snapshot.update(
                complete=True,
                reason=None,
                resource_versions=versions,
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            return canonical_snapshot(
                snapshot, max_items=self.max_items, max_bytes=self.max_bytes
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, InventoryError) and str(exc) in INCOMPLETE_REASONS
                else "collection_failed"
            )
            snapshot.update({kind: [] for kind in INVENTORY_KINDS})
            snapshot.update(
                complete=False,
                reason=reason,
                resource_versions={},
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            return canonical_snapshot(
                snapshot, max_items=self.max_items, max_bytes=self.max_bytes
            )
