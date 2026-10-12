"""Policy 1 held-stop wire refuses stale authority and borrowed proof."""

from copy import deepcopy

import pytest

from shared.vm_cancel_retention import (
    never_app_ready_retention_authority_matches,
    valid_never_app_ready_retention_authority,
    valid_retention_preflight,
)
from shared.vm_pre_ssh_stop import (
    valid_frozen_stop_candidate,
    valid_positive_stop_proof,
)
from tests.test_vm_pre_ssh_stop_protocol import candidate, proof, uid


def held_pair():
    frozen = candidate()
    frozen.update(
        kind="vm_job_never_app_ready_retained_stop_candidate_v1",
        cleanup_admission_id=uid(10),
        cleanup_request_id=uid(11),
        cleanup_intent_digest="sha256:" + "a" * 64,
        kube_vm_ready_at_inspection=True,
    )
    authority = {
        "version": 1,
        "kind": "vm_job_cancel_retention_held_stop_authority_v1",
        "policy_version": 1,
        "owner_kind": "job",
        "job_id": frozen["job_id"],
        "provision_generation": frozen["provision_generation"],
        "namespace": frozen["namespace"],
        "cluster_id": "cluster-a",
        "cleanup_admission_id": frozen["cleanup_admission_id"],
        "cleanup_request_id": frozen["cleanup_request_id"],
        "cleanup_intent_digest": frozen["cleanup_intent_digest"],
        "creation_request_id": uid(12),
        "reservation_id": uid(13),
        "reservation_revision": 1,
        "vm_uid": frozen["vm_uid"],
        "vmi_uid": frozen["vmi_uid"],
        "launcher_uid": frozen["launcher_uid"],
        "pvc_uid": frozen["pvc_uid"],
        "node_uid": frozen["node_uid"],
    }
    parent = {
        "admission_id": authority["cleanup_admission_id"],
        "request_id": authority["cleanup_request_id"],
        "intent_digest": authority["cleanup_intent_digest"],
        "intent": {
            "owner_id": frozen["job_id"],
            "owner_kind": "job",
            "provision_generation": frozen["provision_generation"],
            "purge_disk": False,
            "pvc_uid": frozen["pvc_uid"],
            "resource": "vm_workspace",
            "source": "job_terminal_vm_release",
            "vm_uid": frozen["vm_uid"],
        },
    }
    preflight = {
        "version": 1,
        "kind": "vm_cancel_retention_preflight_v1",
        "stop_policy": "cancel_retention_v1",
        "frozen": frozen,
        "namespace": frozen["namespace"],
        "owner_id": frozen["job_id"],
        "pvc_name": f"agent-vm-{frozen['job_id']}-rootdisk",
        "pvc_uid": frozen["pvc_uid"],
        "dv_uid": uid(14),
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }
    return authority, parent, frozen, preflight


def test_new_wire_accepts_both_observed_ready_values_and_exact_proof():
    authority, parent, frozen, preflight = held_pair()
    assert valid_never_app_ready_retention_authority(authority)
    for ready in (True, False):
        frozen["kube_vm_ready_at_inspection"] = ready
        assert valid_frozen_stop_candidate(frozen)
        assert never_app_ready_retention_authority_matches(authority, parent, frozen)
        assert valid_retention_preflight(preflight, frozen)
        observed = proof(frozen)
        observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
        assert valid_positive_stop_proof(
            frozen, observed, frozen_digest="sha256:" + "a" * 64
        )


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("version", True),
        ("policy_version", True),
        ("reservation_revision", True),
        ("reservation_revision", 0),
        ("namespace", "bad.namespace"),
        ("cluster_id", "bad cluster"),
        ("cleanup_intent_digest", "sha256:ABC"),
        ("vm_uid", "NOT-A-UUID"),
        ("owner_kind", "thread"),
    ],
)
def test_authority_refuses_invalid_exact_fields(field, invalid):
    authority, _, _, _ = held_pair()
    authority[field] = invalid
    assert not valid_never_app_ready_retention_authority(authority)


def test_authority_refuses_extra_or_missing_keys():
    authority, _, _, _ = held_pair()
    assert not valid_never_app_ready_retention_authority({**authority, "extra": 1})
    authority.pop("creation_request_id")
    assert not valid_never_app_ready_retention_authority(authority)


@pytest.mark.parametrize(
    "field",
    [
        "job_id",
        "provision_generation",
        "namespace",
        "vm_uid",
        "vmi_uid",
        "launcher_uid",
        "pvc_uid",
        "node_uid",
        "cleanup_admission_id",
        "cleanup_request_id",
        "cleanup_intent_digest",
    ],
)
def test_authority_refuses_foreign_frozen_identity(field):
    authority, parent, frozen, _ = held_pair()
    changed = deepcopy(frozen)
    changed[field] = (
        uid(40) if field != "cleanup_intent_digest" else "sha256:" + "b" * 64
    )
    assert not never_app_ready_retention_authority_matches(authority, parent, changed)


@pytest.mark.parametrize(
    "field", ["admission_id", "request_id", "intent_digest", "intent"]
)
def test_authority_refuses_foreign_parent(field):
    authority, parent, frozen, _ = held_pair()
    parent = deepcopy(parent)
    parent[field] = (
        {**parent["intent"], "purge_disk": True} if field == "intent" else uid(41)
    )
    assert not never_app_ready_retention_authority_matches(authority, parent, frozen)


def test_authority_refuses_parent_with_extra_capability():
    authority, parent, frozen, _ = held_pair()
    assert not never_app_ready_retention_authority_matches(
        authority, {**parent, "retention_preflight": {"kind": "foreign"}}, frozen
    )


def test_new_frozen_and_proof_refuse_incomplete_or_borrowed_shapes():
    _, _, frozen, _ = held_pair()
    assert not valid_frozen_stop_candidate({**frozen, "extra": 1})
    assert not valid_frozen_stop_candidate(
        {k: v for k, v in frozen.items() if k != "kube_vm_ready_at_inspection"}
    )
    assert not valid_frozen_stop_candidate({**frozen, "kube_vm_ready_at_inspection": 1})
    observed = proof(frozen)
    assert not valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "a" * 64
    )
