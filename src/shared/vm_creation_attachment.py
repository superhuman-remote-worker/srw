"""Exact facts for retained attachment effects, not stand-alone authorization.

The orchestrator must separately prove the instance/execution and predecessor
relationship under its existing control, owner/PVC and row locks before a grant.
"""

from collections.abc import Mapping
from uuid import UUID

from shared.vm_workspace_storage import storage_binding, storage_labels, storage_name


def _uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Attachment UUID is unproven")


def attachment_effect_kinds(request):
    if request.get("workspace_storage") is not None:
        storage_binding(request["workspace_storage"])
        return ("workspace_attach", "rootdisk", "cloud_init", "vm")
    return ("rootdisk", "cloud_init", "vm")


def validate_attachment_intent(value, *, request, expected_pvc_uid):
    if not isinstance(value, Mapping) or set(value) != {
        "binding",
        "execution_id",
        "action",
        "prior",
    }:
        raise ValueError("Attachment intent is incomplete")
    binding = storage_binding(request["workspace_storage"])
    if (
        storage_binding(value["binding"]) != binding
        or binding["pvc_uid"] != expected_pvc_uid
        or value["execution_id"] != request["job_id"]
        or binding["owner_kind"] != "job"
    ):
        raise ValueError("Attachment binding changed")
    _uuid(value["execution_id"])
    action, prior = value["action"], value["prior"]
    if action == "create":
        if (
            prior is not None
            or binding["generation"] != 1
            or expected_pvc_uid is not None
        ):
            raise ValueError("Initial attachment absence is unproven")
        return dict(value)
    if (
        action not in {"replace", "observe"}
        or not isinstance(prior, Mapping)
        or set(prior)
        != {
            "uid",
            "resource_version",
            "workspace_uid",
            "generation",
            "execution_id",
            "detached",
            "released",
        }
    ):
        raise ValueError("Prior attachment identity is incomplete")
    for field in ("uid", "workspace_uid", "execution_id"):
        _uuid(prior[field])
    if (
        not isinstance(prior["resource_version"], str)
        or not prior["resource_version"]
        or prior["workspace_uid"] != binding["uid"]
        or type(prior["generation"]) is not int
        or not 1 <= prior["generation"] <= 9223372036854775807
        or type(prior["detached"]) is not bool
        or prior["released"] is not False
        or expected_pvc_uid is None
    ):
        raise ValueError("Prior attachment state is unproven")
    if action == "replace":
        if (
            prior["generation"] + 1 != binding["generation"]
            or prior["detached"] is not True
        ):
            raise ValueError("Prior attachment is not fenced for advancement")
    elif (
        prior["generation"] != binding["generation"]
        or prior["execution_id"] != request["job_id"]
        or prior["detached"] is not False
    ):
        raise ValueError("Current attachment identity changed")
    return dict(value)


def attachment_observation(
    intent, obj, *, namespace, request_id, effect_nonce, provision_generation
):
    """Normalize a raw Lease only after validating its full selected transition."""
    for identifier in (request_id, effect_nonce, provision_generation):
        _uuid(identifier)
    binding = storage_binding(intent["binding"])
    validate_attachment_intent(
        intent,
        request={"workspace_storage": binding, "job_id": intent["execution_id"]},
        expected_pvc_uid=binding["pvc_uid"],
    )
    metadata = obj["metadata"]
    uid, rv = metadata["uid"], metadata["resourceVersion"]
    _uuid(uid)
    annotations = metadata.get("annotations") or {}
    if (
        obj.get("apiVersion") != "coordination.k8s.io/v1"
        or obj.get("kind") != "Lease"
        or metadata.get("name") != storage_name(binding)
        or metadata.get("namespace") != namespace
        or metadata.get("deletionTimestamp") is not None
        or not isinstance(rv, str)
        or not rv
        or any(
            metadata.get("labels", {}).get(k) != v
            for k, v in storage_labels(binding, intent["execution_id"]).items()
        )
        or any(
            annotations.get("srw.io/" + key, "false") != "false"
            for key in ("released", "detached")
        )
    ):
        raise ValueError("Observed attachment identity changed")
    if intent["prior"] is not None and uid != intent["prior"]["uid"]:
        raise ValueError("Observed attachment Lease was replaced")
    if intent["action"] == "observe":
        if rv != intent["prior"]["resource_version"]:
            raise ValueError("Observed attachment revision changed")
    elif (
        any(
            annotations.get(key) != value
            for key, value in {
                "srw.io/vm-create-request-id": request_id,
                "srw.io/vm-create-effect-nonce": effect_nonce,
                "srw.io/provision-generation": provision_generation,
            }.items()
        )
        or intent["prior"] is not None
        and rv == intent["prior"]["resource_version"]
    ):
        raise ValueError("Observed attachment issuance is unproven")
    return {
        "outcome": "observed",
        "kind": "workspace_attach",
        "uid": uid,
        "resource_version": rv,
        "name": metadata["name"],
        "namespace": namespace,
        "workspace_uid": binding["uid"],
        "generation": binding["generation"],
        "execution_id": intent["execution_id"],
    }
