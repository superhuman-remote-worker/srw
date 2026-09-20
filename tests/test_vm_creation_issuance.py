"""Source-bound VM create carrier identity, independent of observer leases."""

import copy
from uuid import uuid4
import pytest

from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier

SECRET = b"creation-issuance-test-secret-at-least-32-bytes"


def carrier_values(**changes):
    job = str(uuid4())
    value = dict(
        version=1,
        source="controller_vm_create",
        admission_id=str(uuid4()),
        reservation_request_id=str(uuid4()),
        intent_digest="sha256:" + "a" * 64,
        retry_request_id=str(uuid4()),
        job_id=job,
        provision_generation=str(uuid4()),
        request_digest="sha256:" + "b" * 64,
        controller_configuration_digest="sha256:" + "c" * 64,
        expected_pvc_uid=None,
        retained_dv_uid=None,
        current_dv_uid=None,
        current_pvc_uid=None,
        current_secret_uid=None,
        effect_kind="rootdisk",
        effect_nonce=str(uuid4()),
        object_name=f"agent-vm-{job}-rootdisk",
    )
    return {**value, **changes}


def test_carrier_seal_binds_actual_uid_namespace_and_full_source_intent():
    values = carrier_values()
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=str(uuid4()),
        resource_version="17",
        secret=SECRET,
    )
    assert verify_creation_carrier(carrier, secret=SECRET) == values
    for field in ["uid", "namespace", "name"]:
        changed = copy.deepcopy(carrier)
        changed["metadata"][field] = "changed"
        with pytest.raises(ValueError):
            verify_creation_carrier(changed, secret=SECRET)
    changed = copy.deepcopy(carrier)
    changed["metadata"]["annotations"]["srw.io/vm-create-intent"] = "{}"
    with pytest.raises(ValueError):
        verify_creation_carrier(changed, secret=SECRET)


@pytest.mark.parametrize(
    "changes",
    [
        {"source": "controller_rootdisk_delete"},
        {"expected_pvc_uid": str(uuid4())},
        {"effect_kind": "vm", "object_name": "foreign"},
        {"effect_nonce": "not-a-uuid"},
    ],
)
def test_carrier_does_not_relax_existing_source_or_identity_rules(changes):
    with pytest.raises(ValueError):
        seal_creation_carrier(
            carrier_values(**changes),
            namespace="agent-vms",
            uid=str(uuid4()),
            resource_version="1",
            secret=SECRET,
        )
