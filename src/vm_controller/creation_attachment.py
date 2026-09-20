"""One exact retained attachment Lease effect under the creation admission."""

import asyncio

from shared.vm_creation_attachment import validate_attachment_intent
from shared.vm_creation_issuance import EFFECT_NONCE_ANNOTATION, REQUEST_ANNOTATION
from shared.vm_workspace_storage import (
    storage_binding,
    storage_name,
    storage_labels,
    WORKSPACE_LABEL,
    GENERATION_LABEL,
    EXECUTION_LABEL,
)


class CreationAttachment:
    def __init__(self, actuator):
        self.actuator = actuator
        self.controller = actuator.controller

    def prior(self, lease, binding):
        metadata = lease["metadata"]
        labels, annotations = (
            metadata.get("labels") or {},
            metadata.get("annotations") or {},
        )
        generation = labels.get(GENERATION_LABEL)
        if (
            lease.get("kind") != "Lease"
            or lease.get("apiVersion") != "coordination.k8s.io/v1"
            or metadata.get("namespace") != self.actuator.namespace
            or metadata.get("name") != storage_name(binding)
            or metadata.get("deletionTimestamp") is not None
            or not isinstance(generation, str)
            or str(int(generation)) != generation
            or any(
                annotations.get("srw.io/" + key) not in (None, "true", "false")
                for key in ("detached", "released")
            )
        ):
            raise ValueError("Attachment prior state is unproven")
        return {
            "uid": metadata["uid"],
            "resource_version": metadata["resourceVersion"],
            "workspace_uid": labels[WORKSPACE_LABEL],
            "generation": int(generation),
            "execution_id": labels[EXECUTION_LABEL],
            "detached": annotations.get("srw.io/detached") == "true",
            "released": annotations.get("srw.io/released") == "true",
        }

    async def prepare(self, row, frozen=None):
        binding = storage_binding(row["request"]["workspace_storage"])
        if row["request"].get("preparation") is not None:
            raise ValueError("Prepared attachment target proof is not integrated")
        lease = await self.actuator.read("lease", storage_name(binding))
        prior = self.prior(lease, binding) if lease is not None else None
        intent = {
            "binding": binding,
            "execution_id": row["job_id"],
            "action": "create"
            if prior is None
            else "claim"
            if prior["generation"] == binding["generation"]
            else "replace",
            "prior": prior,
        }
        validate_attachment_intent(
            intent, request=row["request"], expected_pvc_uid=row["expected_pvc_uid"]
        )
        if frozen is not None and intent != frozen:
            raise ValueError("Frozen attachment prior changed")
        await self.validate(row, intent)
        return intent

    async def validate(self, row, intent):
        binding = intent["binding"]
        lease = await self.actuator.read("lease", storage_name(binding))
        prior = self.prior(lease, binding) if lease is not None else None
        if prior != intent["prior"]:
            raise ValueError("Attachment prior identity changed")
        if lease is not None:
            await require_legacy_attachment_idle(self.controller, lease)
        retained = self.controller._retained_storage()
        await retained._assert_not_recovery_pinned(binding)
        if not await retained.unused(binding):
            raise ValueError("Attachment has an existing VM or launcher consumer")
        await self.actuator.disk(row, require_attachment=False)

    def body(self, row, values):
        intent = values["workspace_attachment"]
        prior = intent["prior"]
        metadata = {
            "name": values["object_name"],
            "namespace": self.actuator.namespace,
            "labels": storage_labels(intent["binding"], row["job_id"]),
            "annotations": {
                REQUEST_ANNOTATION: row["request_id"],
                EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                "srw.io/provision-generation": row["provision_generation"],
            },
        }
        if prior is not None:
            metadata.update(uid=prior["uid"], resourceVersion=prior["resource_version"])
        return {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": metadata,
            "spec": {},
        }

    async def create(self, body, intent):
        kwargs = {"namespace": self.actuator.namespace, "body": body}
        if intent["action"] == "create":
            method = self.controller.coordination_api.create_namespaced_lease
        else:
            kwargs["name"] = body["metadata"]["name"]
            method = self.controller.coordination_api.replace_namespaced_lease
        return await asyncio.to_thread(method, **kwargs)


async def require_legacy_attachment_idle(controller, lease, *, allow_completed=True):
    """Every legacy writer must preserve a pending protocol attachment marker.

    A completed adoption permits ordinary legacy retirement to proceed; its
    existing unused, generation, recovery and cleanup checks still apply.
    Unknown/partial/cancelled issuance requires its source-specific disposition.
    """
    from vm_controller.creation_actuation import CreationActuator, document

    lease = document(lease)
    metadata = lease["metadata"]
    annotations = metadata.get("annotations") or {}
    request_id = annotations.get(REQUEST_ANNOTATION)
    if request_id is None:
        return
    if not allow_completed:
        raise RuntimeError("Attachment creation remains held for protocol claims")
    row = await CreationActuator(controller).authority("inspect", request_id=request_id)
    if (
        row.get("request_id") != request_id
        or row.get("state") not in {"succeeded", "settled"}
        or row.get("reason") != "creation_adopted"
        or not row.get("effects")
        or row["effects"][-1]["state"] != "observed"
        or row["effects"][-1]["carrier_intent"]["effect_kind"] != "vm"
    ):
        raise RuntimeError("Attachment creation remains held")
    effect = next(
        (
            effect
            for effect in row["effects"]
            if effect["carrier_intent"]["effect_kind"] == "workspace_attach"
            and effect["state"] == "observed"
        ),
        None,
    )
    if effect is None:
        raise RuntimeError("Attachment creation identity is unproven")
    values = effect["carrier_intent"]
    binding = values["workspace_attachment"]["binding"]
    if (
        effect["evidence"]["uid"] != metadata.get("uid")
        or metadata.get("name") != storage_name(binding)
        or metadata.get("namespace") != effect["evidence"]["namespace"]
        or annotations.get(EFFECT_NONCE_ANNOTATION) != values["effect_nonce"]
        or annotations.get("srw.io/provision-generation") != row["provision_generation"]
        or any(
            metadata.get("labels", {}).get(key) != value
            for key, value in storage_labels(binding, row["job_id"]).items()
        )
    ):
        raise RuntimeError("Attachment creation identity changed")
