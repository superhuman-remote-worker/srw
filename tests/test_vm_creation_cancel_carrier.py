"""Cancellation-only Lease cannot be replayed as a creation effect."""

from copy import deepcopy
import json
from uuid import uuid4

import pytest

from shared.vm_creation_cancel_carrier import (
    INTENT_ANNOTATION, seal_cancel_carrier, verify_cancel_carrier,
)
from shared.vm_creation_issuance import verify_creation_carrier

SECRET = b"thread-cancel-carrier-test-secret-at-least-32-bytes"


def _intent():
    def uid():
        return str(uuid4())

    digest = "sha256:" + "a" * 64
    request = uid()
    return {
        "version": 1, "kind": "thread_creation_cancel",
        "source": "controller_vm_create_cancel",
        "admission_id": uid(), "reservation_request_id": uid(),
        "intent_digest": digest, "retry_request_id": request,
        "thread_id": uid(), "thread_runtime_generation": uid(),
        "thread_agent_id": None, "thread_attach_token": None,
        "thread_wake_operation_id": None, "retirement_token": uid(),
        "provision_generation": uid(), "request_digest": digest,
        "controller_configuration_digest": digest, "reservation_id": uid(),
        "reservation_revision": 1, "reservation_cluster_id": "cluster-one",
        "reservation_policy_digest": digest, "source_pin_key": request,
    }


def test_signed_cancel_carrier_binds_uid_end_reservation_and_source_pin():
    values = _intent()
    lease = seal_cancel_carrier(
        values, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    assert verify_cancel_carrier(lease, secret=SECRET) == values
    with pytest.raises(ValueError):
        verify_creation_carrier(lease, secret=SECRET)
    for change in (
        lambda item: item["metadata"].update(uid=str(uuid4())),
        lambda item: item["metadata"].update(namespace="foreign"),
        lambda item: item["metadata"]["annotations"].update({
            INTENT_ANNOTATION: json.dumps({**values, "retirement_token": str(uuid4())}),
        }),
        lambda item: item["metadata"]["annotations"].update({
            INTENT_ANNOTATION: json.dumps({**values, "reservation_id": str(uuid4())}),
        }),
        lambda item: item["metadata"]["annotations"].update({
            INTENT_ANNOTATION: json.dumps({**values, "source_pin_key": str(uuid4())}),
        }),
    ):
        tampered = deepcopy(lease)
        change(tampered)
        with pytest.raises(ValueError):
            verify_cancel_carrier(tampered, secret=SECRET)
