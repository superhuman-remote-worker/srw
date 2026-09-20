"""Closed source completion facts never promote plans or partial identities."""

from copy import deepcopy
import json
from uuid import uuid4

import pytest

from shared.vm_creation_source_completion import (
    source_completion,
    SOURCE_PINS_ANNOTATION,
)
from tests.test_vm_creation_source_disposition_contract import disposed


def completion_inputs():
    request_id, dv_uid, pvc_uid = (str(uuid4()) for _ in range(3))
    pin = disposed(dv_uid, pvc_uid)
    source = {
        "kind": "golden",
        "namespace": "agent-vms",
        "name": "source",
        "dv_uid": dv_uid,
        "pvc_uid": pvc_uid,
    }
    plan = {
        "version": 1,
        "kind": "source_disposition_planned",
        "disposition_id": pin["disposition_id"],
        "request_id": request_id,
        "job_id": pin["job_id"],
        "provision_generation": pin["provision_generation"],
        "request_digest": "sha256:" + "1" * 64,
        "controller_configuration_digest": "sha256:" + "2" * 64,
        "source": source,
        "target": pin["target"],
        "tombstone": pin,
    }

    def obj(uid):
        return {
            "metadata": {
                "name": source["name"],
                "namespace": source["namespace"],
                "uid": uid,
                "resourceVersion": "1",
            }
        }

    dv, pvc = obj(dv_uid), obj(pvc_uid)
    dv["metadata"]["annotations"] = {
        SOURCE_PINS_ANNOTATION: json.dumps({request_id: pin})
    }
    return plan, dv, pvc


@pytest.mark.parametrize(
    "fault",
    ["plan_version", "pin_target", "source_uid", "extra_plan_field", "changed_digest"],
)
def test_incomplete_or_changed_plan_cannot_become_actual_source_completion(fault):
    plan, dv, pvc = completion_inputs()
    if fault == "plan_version":
        plan["version"] = True
    elif fault == "pin_target":
        plan["target"] = {**plan["target"], "name": "unrelated"}
    elif fault == "source_uid":
        plan["source"]["dv_uid"] = str(uuid4())
    elif fault == "extra_plan_field":
        plan["credentials"] = "never persist this"
    else:
        plan["request_digest"] = "unproven"
    with pytest.raises(ValueError):
        source_completion(plan, dv=dv, pvc=pvc)


@pytest.mark.parametrize(
    "fault",
    ["missing_uid", "missing_rv", "wrong_name", "wrong_namespace", "not_object"],
)
def test_unreadable_source_member_is_not_absence(fault):
    plan, dv, pvc = completion_inputs()
    if fault == "missing_uid":
        pvc["metadata"].pop("uid")
    elif fault == "missing_rv":
        pvc["metadata"].pop("resourceVersion")
    elif fault == "wrong_name":
        pvc["metadata"]["name"] = "another"
    elif fault == "wrong_namespace":
        pvc["metadata"]["namespace"] = "another"
    else:
        pvc = []
    with pytest.raises(ValueError):
        source_completion(plan, dv=dv, pvc=pvc)


def test_readback_completion_has_a_strict_replay_validator():
    from shared import vm_creation_source_completion as module

    validate = getattr(module, "validate_source_completion", None)
    assert callable(validate), "SQL replay needs the same complete typed validator"
    plan, dv, pvc = completion_inputs()
    evidence = source_completion(plan, dv=dv, pvc=pvc)
    assert validate(plan, evidence) == evidence
    for key, value in (
        ("version", True),
        ("outcome", "source_identity_gone"),
        ("allocation", {}),
        ("extra", True),
    ):
        changed = deepcopy(evidence)
        changed[key] = value
        with pytest.raises(ValueError):
            validate(plan, changed)


@pytest.mark.parametrize("path", ["plan", "pin"])
def test_receipt_cannot_replace_nested_integer_version_with_boolean(path):
    from shared.vm_creation_source_completion import validate_source_completion

    plan, dv, pvc = completion_inputs()
    evidence = source_completion(plan, dv=dv, pvc=pvc)
    if path == "plan":
        evidence["plan"]["version"] = True
    else:
        evidence["source_observation"]["pin"]["version"] = True
    with pytest.raises(ValueError):
        validate_source_completion(plan, evidence)


def test_completion_json_size_is_bounded_and_unrelated_metadata_is_not_persisted():
    from shared.vm_creation_source_completion import MAX_COMPLETION_BYTES

    plan, dv, pvc = completion_inputs()
    dv["metadata"]["annotations"]["unrelated-credential"] = "not-for-the-ledger"
    result = source_completion(plan, dv=dv, pvc=pvc)
    assert "not-for-the-ledger" not in json.dumps(result)
    plan["source"]["image"] = "x" * MAX_COMPLETION_BYTES
    with pytest.raises(ValueError, match="bound"):
        source_completion(plan, dv=dv, pvc=pvc)
