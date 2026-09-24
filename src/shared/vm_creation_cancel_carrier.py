"""Signed, cancellation-only Lease for a retired typed thread creation source.

This protocol has no effect kind, creation nonce, or actuation grant. Creation
endpoints continue to accept only vm_creation_issuance creation carriers.
"""

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from uuid import UUID


INTENT_ANNOTATION = "srw.io/vm-thread-creation-cancel-intent"
SIGNATURE_ANNOTATION = "srw.io/vm-thread-creation-cancel-signature"
LABEL = "srw.io/vm-thread-creation-cancel-carrier"
_FIELDS = {
    "version", "kind", "source", "admission_id", "reservation_request_id",
    "intent_digest", "retry_request_id", "thread_id",
    "thread_runtime_generation", "thread_agent_id", "thread_attach_token",
    "thread_wake_operation_id", "retirement_token", "provision_generation",
    "request_digest", "controller_configuration_digest", "reservation_id",
    "reservation_revision", "reservation_cluster_id", "reservation_policy_digest",
    "source_pin_key",
}


def _uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Thread cancellation UUID is invalid")


def validate_intent(value):
    if not isinstance(value, Mapping) or set(value) != _FIELDS:
        raise ValueError("Thread cancellation intent is incomplete")
    value = dict(value)
    if (
        type(value["version"]) is not int or value["version"] != 1
        or value["kind"] != "thread_creation_cancel"
        or value["source"] != "controller_vm_create_cancel"
        or type(value["reservation_revision"]) is not int
        or value["reservation_revision"] <= 0
        or not isinstance(value["reservation_cluster_id"], str)
        or not value["reservation_cluster_id"]
        or value["source_pin_key"] != value["retry_request_id"]
        or (value["thread_agent_id"] is None)
        != (value["thread_attach_token"] is None)
    ):
        raise ValueError("Thread cancellation intent has no authority")
    for key in (
        "admission_id", "reservation_request_id", "retry_request_id",
        "thread_id", "thread_runtime_generation", "retirement_token",
        "provision_generation", "reservation_id", "source_pin_key",
    ):
        _uuid(value[key])
    for key in ("thread_agent_id", "thread_attach_token", "thread_wake_operation_id"):
        if value[key] is not None:
            _uuid(value[key])
    for key in (
        "intent_digest", "request_digest", "controller_configuration_digest",
        "reservation_policy_digest",
    ):
        if not isinstance(value[key], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", value[key]
        ):
            raise ValueError("Thread cancellation digest is invalid")
    return value


def carrier_name(admission_id):
    _uuid(admission_id)
    return "srw-thread-cancel-" + UUID(admission_id).hex


def _signature(values, *, namespace, name, uid, secret):
    if not isinstance(secret, bytes) or not secret:
        raise ValueError("Thread cancellation signing key is missing")
    payload = json.dumps(
        ["srw-thread-creation-cancel-v1", namespace, name, uid, values],
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def seal_cancel_carrier(values, *, namespace, uid, resource_version, secret):
    values = validate_intent(values)
    _uuid(uid)
    if not isinstance(namespace, str) or not namespace or not isinstance(
        resource_version, str
    ) or not resource_version:
        raise ValueError("Thread cancellation Lease identity is incomplete")
    name = carrier_name(values["admission_id"])
    return {
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {
            "name": name, "namespace": namespace, "uid": uid,
            "resourceVersion": resource_version, "labels": {LABEL: "true"},
            "annotations": {
                INTENT_ANNOTATION: json.dumps(values, sort_keys=True, separators=(",", ":")),
                SIGNATURE_ANNOTATION: _signature(
                    values, namespace=namespace, name=name, uid=uid, secret=secret,
                ),
            },
        },
        "spec": {"holderIdentity": values["admission_id"]},
    }


def verify_cancel_carrier(carrier, *, secret):
    try:
        if not isinstance(carrier, Mapping) or carrier["apiVersion"] != "coordination.k8s.io/v1" or carrier["kind"] != "Lease":
            raise ValueError("Thread cancellation carrier kind changed")
        metadata = carrier["metadata"]
        values = validate_intent(json.loads(metadata["annotations"][INTENT_ANNOTATION]))
        expected = seal_cancel_carrier(
            values, namespace=metadata["namespace"], uid=metadata["uid"],
            resource_version=metadata["resourceVersion"], secret=secret,
        )
        if (
            metadata.get("deletionTimestamp") is not None
            or metadata["name"] != expected["metadata"]["name"]
            or metadata["labels"].get(LABEL) != "true"
            or carrier["spec"].get("holderIdentity") != values["admission_id"]
            or not hmac.compare_digest(
                metadata["annotations"][SIGNATURE_ANNOTATION],
                expected["metadata"]["annotations"][SIGNATURE_ANNOTATION],
            )
        ):
            raise ValueError("Thread cancellation carrier authentication failed")
        return values
    except (KeyError, TypeError, AttributeError, json.JSONDecodeError) as exc:
        raise ValueError("Thread cancellation carrier evidence incomplete") from exc
