"""Exclusive retained rootdisks for the singleton same-cluster VM controller.

The chart uses Recreate with one controller. Its workspace lock spans admission,
VM creation and disk deletion. A durable Lease records the generation floor and
release tombstone across restarts; it is never an expiring ownership grant.
"""

import asyncio
from copy import deepcopy

from kubernetes.client.exceptions import ApiException

from shared.vm_workspace_storage import (
    EXECUTION_LABEL,
    GENERATION_LABEL,
    WORKSPACE_LABEL,
    storage_binding,
    storage_labels,
    storage_name,
)


class RetainedStorage:
    def __init__(self, controller, namespace):
        self.controller = controller
        self.namespace = namespace
        self.lock = asyncio.Lock()

    async def _assert_not_recovery_pinned(self, binding) -> None:
        """Require authoritative absence of an exact controller retention pin."""

        pvc_uid = binding.get("pvc_uid")
        if not pvc_uid:
            return
        read_pins = getattr(self.controller, "_active_recovery_pins", None)
        if not callable(read_pins):
            raise RuntimeError("Workspace recovery pin authority is unavailable.")
        pins = await read_pins()
        if any(pin.get("pvc_uid") == pvc_uid for pin in pins):
            raise RuntimeError("Retained workspace is pinned for recovery.")

    @staticmethod
    def verify_vm(vm, binding, job_id):
        labels = vm.get("metadata", {}).get("labels", {})
        if any(
            labels.get(key) != value
            for key, value in storage_labels(binding, job_id).items()
        ):
            raise RuntimeError("VM belongs to another retained workspace attachment.")
        volumes = (
            vm.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", [])
        )
        if not any(
            volume.get("dataVolume", {}).get("name") == storage_name(binding)
            for volume in volumes
        ):
            raise RuntimeError("VM does not reference its retained workspace rootdisk.")

    async def unused(self, binding, *, allow_job=None):
        """Require absence of VM, VMI and launcher references, including termination."""
        from vm_controller.controller import KUBEVIRT_GROUP, KUBEVIRT_VERSION

        name = storage_name(binding)
        for plural in ("virtualmachines", "virtualmachineinstances"):
            result = await asyncio.to_thread(
                self.controller.k8s_client.list_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=self.namespace,
                plural=plural,
            )
            for item in result["items"]:
                metadata = item.get("metadata", {})
                if allow_job and metadata.get("name") == f"agent-vm-{allow_job}":
                    continue
                spec = item.get("spec", {})
                if plural == "virtualmachines":
                    spec = spec.get("template", {}).get("spec", {})
                if any(
                    volume.get("dataVolume", {}).get("name") == name
                    or volume.get("persistentVolumeClaim", {}).get("claimName") == name
                    for volume in spec.get("volumes", [])
                ):
                    return False
        pods = await asyncio.to_thread(
            self.controller.core_api.list_namespaced_pod,
            namespace=self.namespace,
        )
        for pod in pods.items:
            if (
                allow_job
                and (pod.metadata.labels or {}).get("vm.kubevirt.io/name")
                == f"agent-vm-{allow_job}"
            ):
                continue
            if any(
                volume.persistent_volume_claim
                and volume.persistent_volume_claim.claim_name == name
                for volume in (pod.spec.volumes or [])
            ):
                return False
        return True

    async def _lease(self, binding):
        try:
            return await asyncio.to_thread(
                self.controller.coordination_api.read_namespaced_lease,
                name=storage_name(binding),
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            return None

    async def claim(self, binding, job_id):
        binding = storage_binding(binding)
        await self._assert_not_recovery_pinned(binding)
        lease = await self._lease(binding)
        labels = storage_labels(binding, job_id)
        if lease is not None:
            current = lease.metadata.labels or {}
            if current.get(WORKSPACE_LABEL) != binding["uid"]:
                raise RuntimeError("Retained workspace ownership is unknown.")
            annotations = lease.metadata.annotations or {}
            if annotations.get("srw.io/released") == "true":
                raise RuntimeError("Retained workspace has been released.")
            generation = int(current.get(GENERATION_LABEL, "0"))
            if generation == binding["generation"]:
                if annotations.get("srw.io/detached") == "true":
                    raise RuntimeError("Retained workspace attachment has been fenced.")
                if current.get(EXECUTION_LABEL) != job_id:
                    raise RuntimeError(
                        "Retained workspace belongs to another execution."
                    )
                return
            if annotations.get("srw.io/detached") != "true":
                raise RuntimeError("Previous workspace attachment has not been fenced.")
            if generation + 1 != binding["generation"]:
                raise RuntimeError("Retained workspace attachment generation is stale.")
            if not await self.unused(binding):
                raise RuntimeError(
                    "Previous workspace VM or launcher is still present."
                )
            if not binding["pvc_uid"]:
                raise RuntimeError("Retained workspace PVC identity is missing.")
            await self.probe(binding, check_generation=False)
        elif binding["generation"] != 1 or binding["pvc_uid"] is not None:
            raise RuntimeError("Retained workspace ownership record is missing.")
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": storage_name(binding),
                "namespace": self.namespace,
                "labels": labels,
                **(
                    {"resourceVersion": lease.metadata.resource_version}
                    if lease
                    else {}
                ),
            },
            "spec": {},
        }
        if lease:
            await asyncio.to_thread(
                self.controller.coordination_api.replace_namespaced_lease,
                name=storage_name(binding),
                namespace=self.namespace,
                body=body,
            )
        else:
            await asyncio.to_thread(
                self.controller.coordination_api.create_namespaced_lease,
                namespace=self.namespace,
                body=body,
            )

    async def probe(self, binding, *, check_generation=True):
        """An absent first PVC is pending; an absent captured PVC is a conflict."""
        binding = storage_binding(binding)
        if check_generation:
            lease = await self._lease(binding)
            labels = lease.metadata.labels if lease else {}
            if not labels or labels.get(GENERATION_LABEL) != str(binding["generation"]):
                raise RuntimeError("Retained workspace attachment generation changed.")
        try:
            pvc = await asyncio.to_thread(
                self.controller.core_api.read_namespaced_persistent_volume_claim,
                name=storage_name(binding),
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            if binding["pvc_uid"]:
                raise RuntimeError(
                    "Captured retained workspace PVC is absent."
                ) from None
            return None
        labels = pvc.metadata.labels or {}
        if (
            labels.get(WORKSPACE_LABEL) != binding["uid"]
            or labels.get("srw.io/owner-id") != binding["owner_id"]
            or labels.get("srw.io/owner-kind") != binding["owner_kind"]
            or (binding["pvc_uid"] and pvc.metadata.uid != binding["pvc_uid"])
            or pvc.metadata.deletion_timestamp
        ):
            raise RuntimeError("Captured retained workspace PVC identity changed.")
        return pvc.metadata.uid

    async def ensure(self, manifest, binding, job_id):
        from vm_controller.controller import CDI_GROUP, CDI_VERSION, CDI_PLURAL

        await self._assert_not_recovery_pinned(binding)
        await self.claim(binding, job_id)
        name = storage_name(binding)
        templates = manifest["spec"].pop("dataVolumeTemplates", [])
        if len(templates) != 1:
            raise ValueError("Retained VM workspaces require exactly one rootdisk.")
        template = deepcopy(templates[0])
        old_name = template["metadata"]["name"]
        for volume in manifest["spec"]["template"]["spec"].get("volumes", []):
            if volume.get("dataVolume", {}).get("name") == old_name:
                volume["dataVolume"]["name"] = name
        labels = storage_labels(binding, job_id)
        for metadata in (
            manifest["metadata"],
            manifest["spec"]["template"].setdefault("metadata", {}),
        ):
            metadata.setdefault("labels", {}).update(labels)
        dv = await self.controller._get_dv(name)
        if dv is not None:
            metadata = dv.get("metadata", {})
            if (
                metadata.get("labels", {}).get(WORKSPACE_LABEL) != binding["uid"]
                or metadata.get("deletionTimestamp")
                or dv.get("status", {}).get("phase") == "Failed"
            ):
                raise RuntimeError("Retained rootdisk cannot be safely adopted.")
            await self.probe(binding)
            return name
        if binding["pvc_uid"] or binding["generation"] > 1:
            raise RuntimeError("Captured retained rootdisk DataVolume is absent.")
        source = template.get("spec", {}).get("source", {}).get("pvc")
        if source is not None:
            source.setdefault("namespace", self.namespace)
        labels.update(
            {
                "srw.io/rootdisk": "true",
                "job-id": binding["owner_id"],
                "srw.io/owner-kind": binding["owner_kind"],
                "srw.io/owner-id": binding["owner_id"],
            }
        )
        await asyncio.to_thread(
            self.controller.k8s_client.create_namespaced_custom_object,
            group=CDI_GROUP,
            version=CDI_VERSION,
            plural=CDI_PLURAL,
            namespace=self.namespace,
            body={
                "apiVersion": f"{CDI_GROUP}/{CDI_VERSION}",
                "kind": "DataVolume",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": labels,
                },
                "spec": template["spec"],
            },
        )
        return name

    async def detach(self, binding):
        """Durably fence late creates before publishing a reusable instance."""
        binding = storage_binding(binding)
        await self._assert_not_recovery_pinned(binding)
        async with self.lock:
            lease = await self._lease(binding)
            if not lease or (lease.metadata.labels or {}).get(GENERATION_LABEL) != str(
                binding["generation"]
            ):
                raise RuntimeError("Retained workspace generation changed.")
            await self.probe(binding)
            if not await self.unused(binding):
                return False
            if (lease.metadata.annotations or {}).get("srw.io/released") == "true":
                raise RuntimeError("Retained workspace has been released.")
            lease.metadata.annotations = {
                **(lease.metadata.annotations or {}),
                "srw.io/detached": "true",
            }
            await asyncio.to_thread(
                self.controller.coordination_api.replace_namespaced_lease,
                name=storage_name(binding),
                namespace=self.namespace,
                body=lease,
            )
            return True

    async def _finished_release(self, binding, lease=None):
        headscale = getattr(self.controller, "headscale", None)
        if headscale is not None and headscale.is_available:
            owners = {binding["owner_id"]}
            if lease and (lease.metadata.labels or {}).get(EXECUTION_LABEL):
                owners.add(lease.metadata.labels[EXECUTION_LABEL])
            for owner in owners:
                await headscale.delete_node(owner)
        return True

    async def delete(self, binding):
        """Tombstone before deleting exact, unattached storage; replay is safe."""
        binding = storage_binding(binding)
        await self._assert_not_recovery_pinned(binding)
        async with self.lock:
            lease = await self._lease(binding)
            if not await self.unused(binding):
                raise RuntimeError("Retained workspace is still in use.")
            if lease is None:
                if binding["generation"] != 1 or binding["pvc_uid"] is not None:
                    raise RuntimeError(
                        "Retained workspace generation record is missing."
                    )
                # Cancelled before its first controller create. Absence must be
                # proved before closing the never-used identity permanently.
                if (
                    await self.probe(binding, check_generation=False) is not None
                    or await self.controller._get_dv(storage_name(binding)) is not None
                ):
                    raise RuntimeError("Unrecorded retained workspace storage exists.")
                await asyncio.to_thread(
                    self.controller.coordination_api.create_namespaced_lease,
                    namespace=self.namespace,
                    body={
                        "apiVersion": "coordination.k8s.io/v1",
                        "kind": "Lease",
                        "metadata": {
                            "name": storage_name(binding),
                            "namespace": self.namespace,
                            "labels": storage_labels(binding, binding["owner_id"]),
                            "annotations": {"srw.io/released": "true"},
                        },
                        "spec": {},
                    },
                )
                return await self._finished_release(binding, lease)
            if (lease.metadata.labels or {}).get(GENERATION_LABEL) != str(
                binding["generation"]
            ):
                raise RuntimeError("Retained workspace generation changed.")
            annotations = lease.metadata.annotations or {}
            if annotations.get("srw.io/released") != "true":
                pvc_uid = await self.probe(binding)
                lease.metadata.annotations = {
                    **annotations,
                    "srw.io/released": "true",
                    "srw.io/released-pvc-uid": pvc_uid or "",
                }
                await asyncio.to_thread(
                    self.controller.coordination_api.replace_namespaced_lease,
                    name=storage_name(binding),
                    namespace=self.namespace,
                    body=lease,
                )
            expected_uid = binding["pvc_uid"] or (lease.metadata.annotations or {}).get(
                "srw.io/released-pvc-uid"
            )
            try:
                pvc = await asyncio.to_thread(
                    self.controller.core_api.read_namespaced_persistent_volume_claim,
                    name=storage_name(binding),
                    namespace=self.namespace,
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise
                dv = await self.controller._get_dv(storage_name(binding))
                if dv is None:
                    return await self._finished_release(binding, lease)
                if expected_uid:
                    return False
                if (
                    dv.get("metadata", {}).get("labels", {}).get(WORKSPACE_LABEL)
                    != binding["uid"]
                ):
                    raise RuntimeError("Retained DataVolume identity changed.")
                await self.controller._delete_dv(
                    storage_name(binding), expected_uid=dv["metadata"]["uid"]
                )
                return False
            if not expected_uid or pvc.metadata.uid != expected_uid:
                raise RuntimeError("Retained workspace PVC was replaced.")
            await self.controller._delete_captured_rootdisk(
                storage_name(binding),
                owner_id=binding["owner_id"],
                expected_pvc_uid=expected_uid,
            )
            return False
