"""Strict attachment facts; these documents never themselves grant permission."""

from copy import deepcopy
from uuid import uuid4

import pytest

from shared.vm_creation_attachment import (
    attachment_effect_kinds,
    validate_attachment_intent,
    attachment_observation,
)
from shared.vm_workspace_storage import storage_labels, storage_name


def case(action="create"):
    job = str(uuid4())
    binding = {
        "uid": str(uuid4()),
        "owner_id": job,
        "owner_kind": "job",
        "generation": 1 if action != "replace" else 2,
        "pvc_uid": None if action == "create" else str(uuid4()),
    }
    prior = (
        None
        if action == "create"
        else {
            "uid": str(uuid4()),
            "resource_version": "17",
            "workspace_uid": binding["uid"],
            "generation": binding["generation"] - (action == "replace"),
            "execution_id": str(uuid4()) if action == "replace" else job,
            "detached": action == "replace",
            "released": False,
        }
    )
    request = {"job_id": job, "workspace_storage": binding}
    intent = {"binding": binding, "execution_id": job, "action": action, "prior": prior}
    return request, intent


@pytest.mark.parametrize("action", ["create", "replace", "observe", "claim"])
def test_exact_attachment_transition_facts(action):
    request, intent = case(action)
    assert (
        validate_attachment_intent(
            intent,
            request=request,
            expected_pvc_uid=request["workspace_storage"]["pvc_uid"],
        )
        == intent
    )
    assert attachment_effect_kinds(request) == (
        "workspace_attach",
        "rootdisk",
        "cloud_init",
        "vm",
    )
    assert attachment_effect_kinds({"job_id": request["job_id"]}) == (
        "rootdisk",
        "cloud_init",
        "vm",
    )


@pytest.mark.parametrize(
    "change",
    ["uid", "execution", "pvc", "detached", "released", "generation", "rv", "extra"],
)
def test_advanced_attachment_requires_complete_exact_prior(change):
    request, intent = case("replace")
    intent = deepcopy(intent)
    if change == "uid":
        intent["prior"]["workspace_uid"] = str(uuid4())
    elif change == "execution":
        intent["execution_id"] = str(uuid4())
    elif change == "pvc":
        intent["binding"]["pvc_uid"] = str(uuid4())
    elif change == "detached":
        intent["prior"]["detached"] = False
    elif change == "released":
        intent["prior"]["released"] = True
    elif change == "generation":
        intent["prior"]["generation"] = 0
    elif change == "rv":
        intent["prior"]["resource_version"] = ""
    else:
        intent["allowed"] = True
    with pytest.raises(ValueError):
        validate_attachment_intent(
            intent,
            request=request,
            expected_pvc_uid=request["workspace_storage"]["pvc_uid"],
        )


def test_same_generation_observation_without_captured_disk_is_not_initial_authority():
    request, intent = case("observe")
    request["workspace_storage"]["pvc_uid"] = None
    with pytest.raises(ValueError):
        validate_attachment_intent(intent, request=request, expected_pvc_uid=None)


def lease_case(action="create"):
    request, intent = case(action)
    request_id, nonce, generation = (str(uuid4()) for _ in range(3))
    obj = {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "namespace": "agent-vms",
            "name": storage_name(intent["binding"]),
            "uid": intent["prior"]["uid"] if intent["prior"] else str(uuid4()),
            "resourceVersion": "17" if action == "observe" else "18",
            "labels": storage_labels(intent["binding"], request["job_id"]),
            "annotations": {}
            if action == "observe"
            else {
                "srw.io/vm-create-request-id": request_id,
                "srw.io/vm-create-effect-nonce": nonce,
                "srw.io/provision-generation": generation,
            },
        },
    }
    args = dict(
        namespace="agent-vms",
        request_id=request_id,
        effect_nonce=nonce,
        provision_generation=generation,
    )
    return intent, obj, args


@pytest.mark.parametrize("action", ["create", "replace", "observe", "claim"])
def test_attachment_observation_binds_actual_lease_identity(action):
    intent, obj, args = lease_case(action)
    observed = attachment_observation(intent, obj, **args)
    assert observed["uid"] == obj["metadata"]["uid"]
    assert observed["resource_version"] == obj["metadata"]["resourceVersion"]
    assert observed["workspace_uid"] == intent["binding"]["uid"]


@pytest.mark.parametrize(
    "mutation",
    [
        "uid",
        "generation",
        "execution",
        "nonce",
        "request",
        "released",
        "detached",
        "deleting",
    ],
)
def test_attachment_observation_refuses_replaced_fenced_or_foreign_lease(mutation):
    intent, obj, args = lease_case("replace")
    meta = obj["metadata"]
    if mutation == "uid":
        meta["uid"] = str(uuid4())
    elif mutation == "generation":
        meta["labels"]["srw.io/workspace-generation"] = "1"
    elif mutation == "execution":
        meta["labels"]["srw.io/workspace-execution"] = str(uuid4())
    elif mutation == "nonce":
        meta["annotations"]["srw.io/vm-create-effect-nonce"] = str(uuid4())
    elif mutation == "request":
        meta["annotations"]["srw.io/vm-create-request-id"] = str(uuid4())
    elif mutation == "deleting":
        meta["deletionTimestamp"] = "2026-09-20T12:00:00Z"
    else:
        meta["annotations"]["srw.io/" + mutation] = "true"
    with pytest.raises(ValueError):
        attachment_observation(intent, obj, **args)


def test_v3_carrier_binds_attachment_before_any_root_source():
    from shared.vm_creation_issuance import (
        seal_creation_carrier,
        verify_creation_carrier,
        public_effect_observation,
    )
    from tests.test_vm_creation_issuance import carrier_values, SECRET

    intent, obj, identity = lease_case()
    values = carrier_values(
        version=3,
        job_id=intent["execution_id"],
        rootdisk_source=None,
        workspace_attachment=intent,
        current_attachment_uid=None,
        effect_kind="workspace_attach",
        object_name=storage_name(intent["binding"]),
        retry_request_id=identity["request_id"],
        effect_nonce=identity["effect_nonce"],
        provision_generation=identity["provision_generation"],
    )
    carrier = seal_creation_carrier(
        values,
        namespace=identity["namespace"],
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    assert verify_creation_carrier(carrier, secret=SECRET) == values
    assert public_effect_observation(
        values, carrier, {"outcome": "observed", "object": obj}
    ) == attachment_observation(intent, obj, **identity)
    values.update(
        effect_kind="rootdisk", rootdisk_source={"kind": "registry", "image": "image"}
    )
    with pytest.raises(ValueError):
        seal_creation_carrier(
            values,
            namespace=identity["namespace"],
            uid=str(uuid4()),
            resource_version="1",
            secret=SECRET,
        )


@pytest.mark.parametrize(
    "field,value",
    [("effect_nonce", None), ("request_id", ""), ("provision_generation", "not-uuid")],
)
def test_attachment_observation_requires_complete_issuance_identity(field, value):
    intent, obj, args = lease_case()
    args[field] = value
    annotation = {
        "effect_nonce": "srw.io/vm-create-effect-nonce",
        "request_id": "srw.io/vm-create-request-id",
        "provision_generation": "srw.io/provision-generation",
    }[field]
    obj["metadata"]["annotations"][annotation] = value
    with pytest.raises(ValueError):
        attachment_observation(intent, obj, **args)
