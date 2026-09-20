"""Durable preparation records and exact Kubernetes storage identities."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json

from kubernetes.client.exceptions import ApiException

from shared.workspace_preparation import canonical, revision

RECORD_LABEL = "srw.io/preparation-record"
DISK_LABEL = "srw.io/preparation-disk"
SCOPE_LABEL = "srw.io/preparation-scope"
ARTIFACT_LABEL = "srw.io/preparation-artifact"


class PreparationConflict(RuntimeError):
    pass


@dataclass
class Record:
    name: str
    uid: str
    version: str
    created: float
    kind: str
    request: dict
    state: dict


def scope_key(scope):
    return revision(scope)[:32]


def record_name(kind, identity):
    return f"srw-prep-{kind}-{revision(identity)[:32]}"


class PreparationStore:
    def __init__(self, core, custom, namespace, storage_class):
        self.core, self.custom = core, custom
        self.namespace, self.storage_class = namespace, storage_class

    async def call(self, method, **kwargs):
        try:
            return await asyncio.to_thread(
                method, namespace=self.namespace, _request_timeout=(5, 20), **kwargs
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def record(self, raw):
        if raw is None:
            return None
        metadata = raw.metadata
        if metadata.deletion_timestamp or not metadata.uid:
            raise PreparationConflict("Preparation record is being removed.")
        kind = (metadata.labels or {}).get(RECORD_LABEL)
        if kind not in {"allocation", "artifact", "base", "tag", "input"}:
            raise PreparationConflict("Preparation record ownership is invalid.")
        return Record(
            metadata.name,
            metadata.uid,
            metadata.resource_version,
            metadata.creation_timestamp.timestamp(),
            kind,
            json.loads(raw.data["request.json"]),
            json.loads(raw.data.get("state.json", "{}")),
        )

    async def get(self, name):
        return self.record(
            await self.call(self.core.read_namespaced_config_map, name=name)
        )

    async def records(self, kind, *, artifact_uid=None):
        selector = f"{RECORD_LABEL}={kind}"
        if artifact_uid:
            selector += f",{ARTIFACT_LABEL}={artifact_uid}"
        response = await self.call(
            self.core.list_namespaced_config_map,
            label_selector=selector,
        )
        if response is None:
            raise PreparationConflict("Preparation inventory is unavailable.")
        return [self.record(item) for item in response.items]

    async def ensure(self, name, kind, request, state):
        existing = await self.get(name)
        if existing is None:
            body = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {
                        RECORD_LABEL: kind,
                        SCOPE_LABEL: scope_key(request.get("scope", {})),
                    },
                },
                "data": {
                    "request.json": canonical(request),
                    "state.json": canonical(state),
                },
            }
            if kind == "input":
                body["immutable"] = True
            try:
                existing = self.record(
                    await self.call(self.core.create_namespaced_config_map, body=body)
                )
            except ApiException as exc:
                if exc.status != 409:
                    raise
                existing = await self.get(name)
        if existing is None or existing.kind != kind or existing.request != request:
            raise PreparationConflict(
                "Preparation request conflicts with its durable identity."
            )
        return existing

    async def save(self, record, state):
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": record.name,
                "namespace": self.namespace,
                "uid": record.uid,
                "resourceVersion": record.version,
                "labels": {
                    RECORD_LABEL: record.kind,
                    SCOPE_LABEL: scope_key(record.request.get("scope", {})),
                    **(
                        {ARTIFACT_LABEL: state["artifact_uid"]}
                        if state.get("artifact_uid")
                        else {}
                    ),
                },
            },
            "data": {
                "request.json": canonical(record.request),
                "state.json": canonical(state),
            },
        }
        raw = await self.call(
            self.core.replace_namespaced_config_map, name=record.name, body=body
        )
        if raw is None or raw.metadata.uid != record.uid:
            raise PreparationConflict("Preparation record identity changed.")
        updated = self.record(raw)
        record.state, record.version = updated.state, updated.version
        return record

    async def delete_record(self, record):
        await self.call(
            self.core.delete_namespaced_config_map,
            name=record.name,
            body={
                "preconditions": {"uid": record.uid, "resourceVersion": record.version}
            },
        )

    async def dv(self, name):
        return await self.call(
            self.custom.get_namespaced_custom_object,
            group="cdi.kubevirt.io",
            version="v1beta1",
            plural="datavolumes",
            name=name,
        )

    async def pvc(self, name):
        return await self.call(
            self.core.read_namespaced_persistent_volume_claim, name=name
        )

    async def ensure_disk(self, name, *, owner_uid, scope, source, size):
        volume = await self.dv(name)
        if volume is None:
            body = {
                "apiVersion": "cdi.kubevirt.io/v1beta1",
                "kind": "DataVolume",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {DISK_LABEL: owner_uid, SCOPE_LABEL: scope_key(scope)},
                    "annotations": {
                        "cdi.kubevirt.io/storage.bind.immediate.requested": "true",
                        "cdi.kubevirt.io/storage.deleteAfterCompletion": "false",
                    },
                },
                "spec": {
                    "source": source,
                    "storage": {
                        "storageClassName": self.storage_class,
                        "accessModes": ["ReadWriteOnce"],
                        "volumeMode": "Filesystem",
                        "resources": {"requests": {"storage": size}},
                    },
                },
            }
            try:
                volume = await self.call(
                    self.custom.create_namespaced_custom_object,
                    group="cdi.kubevirt.io",
                    version="v1beta1",
                    plural="datavolumes",
                    body=body,
                )
            except ApiException as exc:
                if exc.status != 409:
                    raise
                volume = await self.dv(name)
        if (
            not volume
            or volume["metadata"].get("labels", {}).get(DISK_LABEL) != owner_uid
            or volume["spec"].get("source") != source
            or volume["metadata"].get("deletionTimestamp")
        ):
            raise PreparationConflict("Preparation disk ownership or source changed.")
        return volume

    async def disk_identity(self, name, *, owner_uid, expected=None):
        pvc = await self.pvc(name)
        if pvc is None:
            if expected:
                raise PreparationConflict("Captured preparation PVC is missing.")
            return None
        if (
            pvc.metadata.deletion_timestamp
            or (pvc.metadata.labels or {}).get(DISK_LABEL) != owner_uid
            or (expected and pvc.metadata.uid != expected)
        ):
            raise PreparationConflict("Preparation PVC identity changed.")
        return pvc.metadata.uid

    async def unused(self, name, *, ignore_cdi_owner=None):
        pods = await self.call(self.core.list_namespaced_pod)
        vms = await self.call(
            self.custom.list_namespaced_custom_object,
            group="kubevirt.io",
            version="v1",
            plural="virtualmachines",
        )
        vmis = await self.call(
            self.custom.list_namespaced_custom_object,
            group="kubevirt.io",
            version="v1",
            plural="virtualmachineinstances",
        )
        dvs = await self.call(
            self.custom.list_namespaced_custom_object,
            group="cdi.kubevirt.io",
            version="v1beta1",
            plural="datavolumes",
        )
        if any(result is None for result in (pods, vms, vmis, dvs)):
            raise PreparationConflict(
                "Preparation storage-use inventory is unavailable."
            )
        if any(
            v.persistent_volume_claim and v.persistent_volume_claim.claim_name == name
            for pod in pods.items
            if not ignore_cdi_owner
            or not any(
                r.kind == "PersistentVolumeClaim" and r.uid == ignore_cdi_owner
                for r in pod.metadata.owner_references or []
            )
            for v in (pod.spec.volumes or [])
        ):
            return False
        for obj in vms.get("items", []) + vmis.get("items", []):
            spec = obj.get("spec", {})
            spec = spec.get("template", {}).get("spec", spec)
            if any(
                v.get("dataVolume", {}).get("name") == name
                or v.get("persistentVolumeClaim", {}).get("claimName") == name
                for v in spec.get("volumes", [])
            ):
                return False
        for obj in dvs.get("items", []):
            source = obj.get("spec", {}).get("source", {}).get("pvc", {})
            if (
                source.get("name") == name
                and source.get("namespace", self.namespace) == self.namespace
                and obj.get("status", {}).get("phase") != "Succeeded"
            ):
                return False
        return True

    async def reap_disk_pods(self, name, pvc_uid):
        """Remove only terminated CDI consumers owned by this exact disk.

        CDI retains failed/importer Pods for diagnostics. Their PVC references
        otherwise prevent garbage collection even after every process exited.
        """
        pods = await self.call(self.core.list_namespaced_pod)
        if pods is None:
            raise PreparationConflict("Preparation Pod inventory is unavailable.")
        for pod in pods.items:
            if not any(
                v.persistent_volume_claim
                and v.persistent_volume_claim.claim_name == name
                for v in pod.spec.volumes or []
            ):
                continue
            if not any(
                r.kind == "PersistentVolumeClaim" and r.uid == pvc_uid
                for r in pod.metadata.owner_references or []
            ):
                continue
            states = (pod.status.container_statuses or []) + (
                pod.status.init_container_statuses or []
            )
            if (
                pod.status.phase not in {"Succeeded", "Failed"}
                or not states
                or any(s.state.terminated is None for s in states)
            ):
                continue
            await self.call(
                self.core.delete_namespaced_pod,
                name=pod.metadata.name,
                body={"preconditions": {"uid": pod.metadata.uid}},
            )

    async def delete_disk(
        self, name, *, owner_uid, pvc_uid, dv_uid, retire_import=False
    ):
        if not await self.unused(
            name, ignore_cdi_owner=pvc_uid if retire_import else None
        ):
            return False
        dv, pvc = await self.dv(name), await self.pvc(name)
        # Validate both captured identities before the first deletion. A
        # replaced PVC must not cause even the original DV to be removed.
        if pvc is not None and (
            pvc.metadata.uid != pvc_uid
            or (pvc.metadata.labels or {}).get(DISK_LABEL) != owner_uid
        ):
            raise PreparationConflict(
                "Preparation PVC identity changed before deletion."
            )
        if dv is not None:
            if (
                dv["metadata"].get("uid") != dv_uid
                or dv["metadata"].get("labels", {}).get(DISK_LABEL) != owner_uid
            ):
                raise PreparationConflict(
                    "Preparation DataVolume identity changed before deletion."
                )
            from vm_controller.creation_sources import pins

            if not dv["metadata"].get("resourceVersion"):
                raise PreparationConflict("Preparation source revision is unproven.")
            if any(pin["state"] != "released" for pin in pins(dv).values()):
                return False
            await self.call(
                self.custom.delete_namespaced_custom_object,
                group="cdi.kubevirt.io",
                version="v1beta1",
                plural="datavolumes",
                name=name,
                body={
                    "preconditions": {
                        "uid": dv_uid,
                        "resourceVersion": dv["metadata"]["resourceVersion"],
                    }
                },
            )
        if pvc is not None:
            await self.call(
                self.core.delete_namespaced_persistent_volume_claim,
                name=name,
                body={"preconditions": {"uid": pvc_uid}},
            )
        if retire_import:
            # PVC deletion stops CDI's importer reconciliation. Remove only
            # its exact dependent Pods, gracefully; storage protection then
            # waits for their departure. Never recycle this failed disk.
            pods = await self.call(self.core.list_namespaced_pod)
            if pods is None:
                raise PreparationConflict("Importer inventory is unavailable.")
            for pod in pods.items:
                if any(
                    r.kind == "PersistentVolumeClaim" and r.uid == pvc_uid
                    for r in pod.metadata.owner_references or []
                ):
                    await self.call(
                        self.core.delete_namespaced_pod,
                        name=pod.metadata.name,
                        body={"preconditions": {"uid": pod.metadata.uid}},
                    )
        return await self.dv(name) is None and await self.pvc(name) is None


def now():
    return datetime.now(timezone.utc).timestamp()
