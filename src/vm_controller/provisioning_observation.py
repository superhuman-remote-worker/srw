"""Build bounded phase evidence from exact Kubernetes object associations.

No names-only inference, API calls, clock changes, or readiness promotion live
here. None means an observed 404, never a failed Kubernetes request. The caller
signs this observation with the ordinary lifecycle status envelope.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from uuid import UUID

_TRANSFER = {
    "ImportInProgress",
    "CloneInProgress",
    "UploadInProgress",
    "SnapshotForSmartClone",
    "SmartClonePVCInProgress",
    "CSICloneInProgress",
    "CloneFromSnapshotSourceInProgress",
}
_PREPARING = {"PVCBound", "ImportScheduled", "CloneScheduled", "UploadScheduled"}
_DEPENDENCY = {"WaitForFirstConsumer", "PendingPopulation", "Paused"}
_VMI_PHASES = {"Pending", "Scheduling", "Scheduled", "Running", "Succeeded", "Failed"}
_PERCENT = re.compile(r"(?:\d+(?:\.\d+)?|\.\d+)%\Z")


def _field(value: object, name: str, default=None):
    if isinstance(value, Mapping):
        return value.get(name, default)
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return getattr(value, snake, default)


def _uid(value: object) -> str:
    try:
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("phase object has no exact UID") from exc
    return value


def _metadata(value: object, *, name: str, namespace: str) -> tuple[object, str]:
    metadata = _field(value, "metadata")
    if (
        metadata is None
        or _field(metadata, "name") != name
        or _field(metadata, "namespace") != namespace
        or _field(metadata, "deletionTimestamp") is not None
    ):
        raise ValueError("phase object identity is not current")
    return metadata, _uid(_field(metadata, "uid"))


def _owner(metadata: object, kind: str, owner_id: str) -> None:
    labels = _field(metadata, "labels")
    if not isinstance(labels, Mapping) or (
        labels.get("srw.io/owner-kind") != kind
        or labels.get("srw.io/owner-id") != owner_id
    ):
        raise ValueError("phase object owner does not match")


def _reference(metadata: object, kind: str, uid: str) -> None:
    refs = _field(metadata, "ownerReferences", [])
    if not isinstance(refs, list) or not any(
        _field(ref, "kind") == kind
        and _field(ref, "uid") == uid
        and _field(ref, "controller") is True
        for ref in refs
    ):
        raise ValueError("phase object parent UID does not match")


def _volume_mode(spec: object, name: str) -> str:
    volumes = _field(spec, "volumes")
    if not isinstance(volumes, list):
        raise ValueError("phase runtime rootdisk association is missing")
    matches = []
    for volume in volumes:
        dv = _field(volume, "dataVolume")
        pvc = _field(volume, "persistentVolumeClaim")
        if dv is not None and pvc is not None:
            raise ValueError("phase runtime disk source is ambiguous")
        if _field(dv, "name") == name:
            matches.append("clone")
        if _field(pvc, "claimName") == name:
            matches.append("retained")
    if len(matches) != 1:
        raise ValueError("phase runtime rootdisk association is not exact")
    return matches[0]


def _progress(value: object) -> float | None:
    if type(value) is not str or len(value) > 20 or not _PERCENT.fullmatch(value):
        return None
    progress = float(value[:-1])
    return progress if math.isfinite(progress) and 0 <= progress <= 100 else None


def build_provisioning_observation(
    *,
    vm: object,
    vmi: object | None,
    datavolume: object | None,
    pvc: object | None,
    namespace: str,
    owner_kind: str,
    owner_id: str,
    generation: str,
    rootdisk_name: str,
    rootdisk_owner_kind: str,
    rootdisk_owner_id: str,
) -> dict:
    """Validate live ownership and publish normalized, non-secret phase facts."""
    if owner_kind not in ("job", "thread") or rootdisk_owner_kind not in (
        "job",
        "thread",
    ):
        raise ValueError("phase owner kind is invalid")
    _uid(owner_id)
    _uid(rootdisk_owner_id)
    _uid(generation)
    vm_name = f"agent-vm-{owner_id}"
    vm_meta, vm_uid = _metadata(vm, name=vm_name, namespace=namespace)
    _owner(vm_meta, owner_kind, owner_id)
    annotations = _field(vm_meta, "annotations")
    if (
        not isinstance(annotations, Mapping)
        or annotations.get("srw.io/provision-generation") != generation
    ):
        raise ValueError("phase VM generation does not match")
    mode = _volume_mode(
        _field(_field(_field(vm, "spec"), "template"), "spec"), rootdisk_name
    )
    vmi_uid, vmi_phase = None, "absent"
    if vmi is not None:
        vmi_meta, vmi_uid = _metadata(vmi, name=vm_name, namespace=namespace)
        _reference(vmi_meta, "VirtualMachine", vm_uid)
        _volume_mode(_field(vmi, "spec"), rootdisk_name)
        phase = _field(_field(vmi, "status"), "phase")
        vmi_phase = (
            phase.lower() if type(phase) is str and phase in _VMI_PHASES else "unknown"
        )
    dv_uid, disk_phase, disk_progress = None, "unknown", None
    if mode == "clone" and datavolume is not None:
        dv_meta, dv_uid = _metadata(datavolume, name=rootdisk_name, namespace=namespace)
        _owner(dv_meta, rootdisk_owner_kind, rootdisk_owner_id)
        status = _field(datavolume, "status")
        phase = _field(status, "phase")
        if type(phase) is str:
            if phase in _DEPENDENCY:
                disk_phase = "waiting_for_consumer"
            elif phase in _TRANSFER:
                disk_phase = "transferring"
            elif phase in _PREPARING:
                disk_phase = "preparing"
            elif phase == "Pending":
                disk_phase = "pending"
            elif phase == "Succeeded":
                disk_phase = "ready"
            elif phase == "Failed":
                disk_phase = "failed"
        disk_progress = _progress(_field(status, "progress"))
    pvc_uid = None
    if pvc is not None:
        pvc_meta, pvc_uid = _metadata(pvc, name=rootdisk_name, namespace=namespace)
        _owner(pvc_meta, rootdisk_owner_kind, rootdisk_owner_id)
        if mode == "clone" and dv_uid is not None:
            _reference(pvc_meta, "DataVolume", dv_uid)
        bound = _field(_field(pvc, "status"), "phase") == "Bound"
        if mode == "retained":
            disk_phase = "ready" if bound else "waiting_for_consumer"
        elif disk_phase == "ready" and not bound:
            disk_phase = "unknown"
    elif disk_phase == "ready":
        disk_phase = "unknown"
    return {
        "version": 1,
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "namespace": namespace,
        "provision_generation": generation,
        "vm_uid": vm_uid,
        "vmi_uid": vmi_uid,
        "rootdisk_dv_name": rootdisk_name if mode == "clone" else None,
        "rootdisk_dv_uid": dv_uid,
        "rootdisk_pvc_name": rootdisk_name,
        "rootdisk_pvc_uid": pvc_uid,
        "disk_mode": mode,
        "disk_phase": disk_phase,
        "disk_progress": disk_progress,
        "vmi_phase": vmi_phase,
    }
