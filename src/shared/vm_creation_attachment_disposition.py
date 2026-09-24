"""Typed cancellation-only Lease plans and readbacks; SQL supplies authority."""

from copy import deepcopy
import json
from uuid import UUID

from shared.vm_creation_issuance import EFFECT_NONCE_ANNOTATION, REQUEST_ANNOTATION
from shared.vm_workspace_storage import storage_binding, storage_labels, storage_name

DISPOSITION_ANNOTATION = "srw.io/vm-create-disposition"
IDENTITY_ANNOTATIONS = (
    REQUEST_ANNOTATION,
    EFFECT_NONCE_ANNOTATION,
    "srw.io/provision-generation",
    "srw.io/detached",
    "srw.io/released",
    DISPOSITION_ANNOTATION,
)


def lease_identity(obj, *, name, namespace):
    if not isinstance(obj, dict):
        raise ValueError("Attachment readback is missing")
    metadata = obj.get("metadata", {})
    uid, rv = metadata.get("uid"), metadata.get("resourceVersion")
    labels, annotations = metadata.get("labels", {}), metadata.get("annotations", {})
    if (
        obj.get("apiVersion") != "coordination.k8s.io/v1"
        or obj.get("kind") != "Lease"
        or metadata.get("name") != name
        or metadata.get("namespace") != namespace
        or metadata.get("deletionTimestamp") is not None
        or not isinstance(uid, str)
        or str(UUID(uid)) != uid
        or not isinstance(rv, str)
        or not 1 <= len(rv) <= 256
        or not isinstance(labels, dict)
        or not isinstance(annotations, dict)
    ):
        raise ValueError("Attachment readback identity is unproven")
    return {
        "uid": uid,
        "resource_version": rv,
        "labels": {
            key: labels.get(key)
            for key in storage_labels({"uid": "", "generation": 1}, "")
        },
        "annotations": {key: annotations.get(key) for key in IDENTITY_ANNOTATIONS},
    }


def attachment_plan(row, disposition, completion, obj):
    """Current observed or never-issued first attachment only; no absence shortcut."""
    if not all(key in completion for key in ("source", "rootdisk", "cloud_init")):
        raise ValueError("Resources and source remain held")
    binding = row["canonical_request"].get("workspace_storage")
    plan = {
        "version": 1,
        "kind": "attachment_disposition_planned",
        "disposition_id": disposition["disposition_id"],
        "request_id": str(row["request_id"]),
        "job_id": str(row["thread_id"] if row.get("owner_kind") == "thread" else row["job_id"]),
        "provision_generation": str(row["provision_generation"]),
        "binding": deepcopy(binding),
        "name": storage_name(binding) if binding else None,
        "namespace": disposition["namespace"],
        "outcome": "not_applicable",
        "prior": None,
    }
    if binding is None:
        if obj is not None:
            raise ValueError("Unexpected attachment")
        return plan
    binding = storage_binding(binding)
    root_kind = completion["rootdisk"]["kind"]
    if root_kind == "rootdisk_never_issued" and binding["pvc_uid"] is None:
        plan["outcome"] = "released"
    elif root_kind == "rootdisk_retained":
        plan["outcome"] = "detached"
    else:
        raise ValueError("Attachment disk disposition is unproven")
    observed = disposition["objects"].get("workspace_attach")
    if observed is None:
        if (
            obj is not None
            or binding["generation"] != 1
            or binding["pvc_uid"] is not None
            or plan["outcome"] != "released"
            or any(
                e["effect_kind"] == "workspace_attach" and e["state"] != "rejected"
                for e in disposition["effects"]
            )
        ):
            raise ValueError("Attachment non-issuance is unproven")
        return plan
    prior = lease_identity(obj, name=plan["name"], namespace=plan["namespace"])
    effect = next(
        e
        for e in disposition["effects"]
        if e["effect_kind"] == "workspace_attach" and e["state"] == "observed"
    )
    annotations = prior["annotations"]
    if (
        any(prior[key] != observed[key] for key in ("uid", "resource_version"))
        or prior["labels"] != storage_labels(binding, plan["job_id"])
        or annotations[REQUEST_ANNOTATION] != plan["request_id"]
        or annotations[EFFECT_NONCE_ANNOTATION] != effect["effect_nonce"]
        or annotations["srw.io/provision-generation"] != plan["provision_generation"]
        or any(
            annotations[key] not in (None, "false")
            for key in ("srw.io/detached", "srw.io/released")
        )
        or annotations[DISPOSITION_ANNOTATION] is not None
    ):
        raise ValueError("Observed attachment changed")
    plan["prior"] = prior
    return plan


def marker(plan):
    return json.dumps(plan, sort_keys=True, separators=(",", ":"))


def attachment_completion(plan, obj):
    identity = None
    if plan["binding"] is None:
        if (
            obj not in (None, {"outcome": "not_applicable"})
            or plan["outcome"] != "not_applicable"
        ):
            raise ValueError("Unexpected attachment completion")
    else:
        identity = lease_identity(obj, name=plan["name"], namespace=plan["namespace"])
        expected_annotations = {
            REQUEST_ANNOTATION: plan["request_id"],
            EFFECT_NONCE_ANNOTATION: (plan["prior"] or {})
            .get("annotations", {})
            .get(EFFECT_NONCE_ANNOTATION),
            "srw.io/provision-generation": plan["provision_generation"],
            "srw.io/detached": "true" if plan["outcome"] == "detached" else "false",
            "srw.io/released": "true" if plan["outcome"] == "released" else "false",
            DISPOSITION_ANNOTATION: marker(plan),
        }
        if (
            identity["labels"] != storage_labels(plan["binding"], plan["job_id"])
            or identity["annotations"] != expected_annotations
            or plan["prior"] is not None
            and (
                identity["uid"] != plan["prior"]["uid"]
                or identity["resource_version"] == plan["prior"]["resource_version"]
            )
        ):
            raise ValueError("Attachment cancellation readback changed")
    return {
        "version": 1,
        "kind": "attachment_disposition_completed",
        "plan": deepcopy(plan),
        "outcome": plan["outcome"],
        "lease": identity,
    }
