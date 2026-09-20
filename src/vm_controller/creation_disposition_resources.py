"""Fixed-UID cancellation effects; source and retained attachment authority stay held."""

import asyncio
from collections.abc import Mapping

from kubernetes.client.exceptions import ApiException

from shared.vm_creation_issuance import EFFECT_NONCE_ANNOTATION, REQUEST_ANNOTATION
from vm_controller.creation_actuation import CreationUnproven, document


def _secret_reference(value, name):
    if isinstance(value, Mapping):
        for key, child in value.items():
            if "secret" in key.lower() and (
                child == name
                or isinstance(child, Mapping)
                and child.get("name") == name
                or isinstance(child, list)
                and any(
                    isinstance(item, Mapping) and item.get("name") == name
                    for item in child
                )
            ):
                return True
            if _secret_reference(child, name):
                return True
    elif isinstance(value, list):
        return any(_secret_reference(child, name) for child in value)
    return False


def _items(value):
    value = document(value)
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise CreationUnproven("creation_consumers_unproven")
    metadata = value.get("metadata")
    if (
        not isinstance(metadata, dict)
        or not isinstance(metadata.get("resourceVersion"), str)
        or not metadata["resourceVersion"]
        or metadata.get("continue")
        or metadata.get("remainingItemCount") not in (None, 0)
    ):
        raise CreationUnproven("creation_consumers_incomplete")
    return value["items"]


async def require_no_consumers(actuator, disposition):
    """Complete namespace scans include deleting VM/VMI/Pods and Secret users."""
    ctrl = actuator.controller
    job_name = "agent-vm-" + disposition["job_id"]
    root = disposition["objects"].get("rootdisk", {}).get("name")
    secret = disposition["objects"].get("cloud_init", {}).get("name")
    if await actuator.read("vm", job_name) is not None:
        raise CreationUnproven("creation_vm_requires_observation")
    documents = []
    for plural in ("virtualmachines", "virtualmachineinstances"):
        result = await asyncio.to_thread(
            ctrl.k8s_client.list_namespaced_custom_object,
            group="kubevirt.io",
            version="v1",
            namespace=actuator.namespace,
            plural=plural,
        )
        documents.extend((plural, item) for item in _items(result))
    result = await asyncio.to_thread(
        ctrl.core_api.list_namespaced_pod, namespace=actuator.namespace
    )
    documents.extend(("pods", item) for item in _items(result))
    for kind, item in documents:
        metadata, spec = item.get("metadata", {}), item.get("spec", {})
        if (
            metadata.get("name") == job_name
            or (metadata.get("labels") or {}).get("vm.kubevirt.io/name") == job_name
        ):
            raise CreationUnproven("creation_resource_in_use")
        if kind == "virtualmachines":
            spec = spec.get("template", {}).get("spec", {})
        if (
            root
            and any(
                volume.get("dataVolume", {}).get("name") == root
                or volume.get("persistentVolumeClaim", {}).get("claimName") == root
                for volume in spec.get("volumes", []) or []
            )
            or secret
            and _secret_reference(spec, secret)
        ):
            raise CreationUnproven("creation_resource_in_use")


class DispositionResources:
    def __init__(self, actuator, row, lease, disposition):
        self.actuator, self.row, self.lease, self.disposition = (
            actuator,
            row,
            lease,
            disposition,
        )

    async def check(self):
        """Read every captured identity before the first/final destructive seam."""
        disposition = self.disposition
        for stage in ("rootdisk", "cloud_init"):
            expected = disposition["objects"].get(stage)
            if expected is None:
                continue
            current = await self.actuator.read(stage, expected["name"])
            if current is not None:
                metadata = current["metadata"]
                intent = next(
                    effect["carrier_intent"]
                    for effect in disposition["effects"]
                    if effect["effect_kind"] == stage and effect["state"] == "observed"
                )
                labels, annotations = (
                    metadata.get("labels") or {},
                    metadata.get("annotations") or {},
                )
                if (
                    any(
                        metadata.get(key) != expected[key]
                        for key in ("name", "namespace", "uid")
                    )
                    or labels.get("srw.io/owner-kind") != "job"
                    or labels.get("srw.io/owner-id") != disposition["job_id"]
                    or annotations.get(EFFECT_NONCE_ANNOTATION)
                    != intent["effect_nonce"]
                    or annotations.get(REQUEST_ANNOTATION) != disposition["request_id"]
                    or annotations.get("srw.io/provision-generation")
                    != disposition["provision_generation"]
                    or stage == "cloud_init"
                    and annotations.get("srw.io/ssh-host-key-fingerprint")
                    != expected["ssh_host_key_fingerprint"]
                ):
                    raise CreationUnproven("creation_resource_identity_changed")
            if stage == "rootdisk":
                pvc = await self.actuator.read("pvc", expected["name"])
                if pvc is not None:
                    if pvc["metadata"].get("namespace") != expected["namespace"]:
                        raise CreationUnproven("creation_resource_identity_changed")
                    try:
                        self.actuator.controller._validate_cleanup_carrier_pvc(
                            pvc,
                            {
                                "name": expected["name"],
                                "old_pvc_uid": expected["pvc_uid"],
                                "old_dv_uid": expected["uid"],
                                "owner_kind": "job",
                                "owner_id": disposition["job_id"],
                            },
                        )
                    except RuntimeError as exc:
                        raise CreationUnproven(
                            "creation_resource_identity_changed"
                        ) from exc
        await require_no_consumers(self.actuator, disposition)
        pins = await self.actuator.controller._active_recovery_pins()
        root = disposition["objects"].get("rootdisk")
        if root and any(pin.get("pvc_uid") == root["pvc_uid"] for pin in pins):
            raise CreationUnproven("creation_resource_recovery_pinned")

    async def run(self):
        if (
            self.disposition["disk_policy"] != "purge_new_job_disk"
            or self.disposition["workspace_storage"] is not None
        ):
            return
        if not self.disposition["objects"]:
            return
        await self.check()
        for stage in ("cloud_init", "rootdisk"):
            if (
                stage not in self.disposition["objects"]
                or stage in self.row["cancellation_progress"]
            ):
                continue
            grant = await self.actuator.authority(
                "authorize-disposition",
                request_id=self.row["request_id"],
                carrier=self.lease,
                stage=stage,
            )
            if grant.get("resource") != self.disposition["objects"][stage]:
                raise CreationUnproven("creation_disposition_grant_changed")
            if stage == "cloud_init":
                if grant.get("operation") != "delete_secret":
                    raise CreationUnproven("creation_disposition_grant_changed")
                await self.check()
                resource = grant["resource"]
                try:
                    await asyncio.to_thread(
                        self.actuator.controller.core_api.delete_namespaced_secret,
                        name=resource["name"],
                        namespace=self.actuator.namespace,
                        body={
                            "apiVersion": "v1",
                            "kind": "DeleteOptions",
                            "preconditions": {"uid": resource["uid"]},
                        },
                    )
                except ApiException as exc:
                    if exc.status != 404:
                        raise
                except Exception:
                    # Only an exact follow-up read can resolve a lost DELETE.
                    if await self.actuator.read(stage, resource["name"]) is not None:
                        raise
                if await self.actuator.read(stage, resource["name"]) is not None:
                    return
            else:
                if grant.get("operation") != "purge_rootdisk":
                    raise CreationUnproven("creation_disposition_grant_changed")
                cleanup = grant["cleanup"]
                ctrl = self.actuator.controller
                carrier = await ctrl._ensure_workspace_cleanup_carrier(
                    cleanup,
                    **{
                        key: cleanup[key]
                        for key in (
                            "source",
                            "owner_kind",
                            "owner_id",
                            "pvc_uid",
                            "dv_uid",
                            "provision_generation",
                        )
                    },
                )
                resumed = await ctrl._resume_workspace_cleanup_reservation(carrier)
                from shared.vm_creation_disposition import (
                    disposition_identity,
                    validate_disposition_request,
                )

                if validate_disposition_request(
                    resumed.get("creation_disposition")
                ) != disposition_identity(self.row):
                    raise CreationUnproven("creation_disposition_child_changed")
                if resumed.get("allowed") is not False or (
                    resumed.get("reason") != "creation_disposition_required"
                    and resumed.get("completed_outcome") != "deleted"
                ):
                    raise CreationUnproven("creation_disposition_child_changed")
                if not await ctrl._reconcile_cleanup_carrier_old_identity(
                    carrier, require_failed_dv=False, before_delete=self.check
                ):
                    return
                await self.check()
                await ctrl._complete_workspace_cleanup_reservation(
                    carrier, outcome="deleted"
                )
            await self.check()
            recorded = await self.actuator.authority(
                "record-disposition",
                request_id=self.row["request_id"],
                carrier=self.lease,
                stage=stage,
                evidence=grant["completion"],
            )
            if (
                recorded.get("recorded") is not True
                or recorded.get("evidence") != grant["completion"]
            ):
                raise CreationUnproven("creation_disposition_progress_unproven")
