"""Exact, metadata-only pre-SSH stop envelope; no Kubernetes or DB mocks."""

from copy import deepcopy
from uuid import UUID

import pytest

from shared.vm_pre_ssh_stop import (
    PRE_SSH_STOP_FINALIZER,
    valid_frozen_stop_candidate,
    valid_positive_stop_proof,
)


def uid(n: int) -> str:
    return str(UUID(int=n, version=4))


def candidate() -> dict:
    job = uid(1)
    return {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": job,
        "provision_generation": uid(2),
        "namespace": "owned-vms",
        "vm_name": f"agent-vm-{job}",
        "vm_uid": uid(3),
        "vmi_uid": uid(4),
        "launcher_name": "virt-launcher-owned",
        "launcher_uid": uid(5),
        "pvc_uid": uid(6),
        "node_name": "node-a",
        "node_uid": uid(7),
        "vm_resource_version": "42",
        "vm_generation": 7,
        "launcher_resource_version": "43",
        "containers": [
            {
                "kind": "regular",
                "name": "compute",
                "container_id": "containerd://compute",
            },
            {
                "kind": "init",
                "name": "guest-console-log",
                "container_id": "containerd://console",
            },
        ],
    }


def proof(frozen: dict) -> dict:
    return {
        "kind": "vm_pre_ssh_positive_stop_v1",
        "frozen_digest": "sha256:" + "a" * 64,
        "vm_uid": frozen["vm_uid"],
        "vmi_uid": frozen["vmi_uid"],
        "launcher_uid": frozen["launcher_uid"],
        "node_uid": frozen["node_uid"],
        "vm_run_strategy": "Halted",
        "vm_generation": frozen["vm_generation"] + 1,
        "node_ready": True,
        "vmi_disposition": "absent",
        "same_generation_replacement": False,
        "pod_finalizer": PRE_SSH_STOP_FINALIZER,
        "pod_intent_digest": "sha256:" + "a" * 64,
        "pod_terminal": {"phase": "Failed", "restart_policy": "Never"},
        "containers": [
            {
                **item,
                "terminated_container_id": item["container_id"],
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": "2026-10-07T12:00:00Z",
                "reason": "Completed",
            }
            for item in frozen["containers"]
        ],
        "controller_authenticated": True,
    }


def test_exact_frozen_candidate_and_positive_proof():
    frozen = candidate()
    assert valid_frozen_stop_candidate(frozen)
    assert valid_positive_stop_proof(
        frozen, proof(frozen), frozen_digest="sha256:" + "a" * 64
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("launcher_uid", None),
        ("node_uid", "reused-node"),
        ("launcher_resource_version", ""),
        ("containers", []),
        (
            "containers",
            [
                {
                    "kind": "regular",
                    "name": "compute",
                    "container_id": "containerd://a",
                },
                {
                    "kind": "regular",
                    "name": "compute",
                    "container_id": "containerd://b",
                },
            ],
        ),
        (
            "containers",
            [
                {
                    "kind": "ephemeral",
                    "name": "debug",
                    "container_id": "containerd://debug",
                }
            ],
        ),
    ],
)
def test_bad_frozen_candidate_refuses(field, value):
    frozen = candidate()
    frozen[field] = value
    assert not valid_frozen_stop_candidate(frozen)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("vm_run_strategy", "RerunOnFailure"),
        ("node_ready", False),
        ("same_generation_replacement", True),
        ("pod_finalizer", "other.io/borrowed"),
        ("pod_intent_digest", "sha256:" + "b" * 64),
        ("vmi_disposition", "running"),
        ("controller_authenticated", False),
    ],
)
def test_positive_proof_refuses_unstopped_or_unowned_state(field, value):
    frozen = candidate()
    observed = proof(frozen)
    observed[field] = value
    assert not valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "a" * 64
    )


@pytest.mark.parametrize(
    "changed", ["missing", "replacement", "restarted", "last_state"]
)
def test_positive_proof_refuses_incomplete_or_rebound_container(changed):
    frozen = candidate()
    observed = proof(frozen)
    container = observed["containers"][0]
    if changed == "missing":
        observed["containers"].pop()
    elif changed == "replacement":
        container["container_id"] = "containerd://replacement"
    elif changed == "restarted":
        container["restart_count"] = 1
    else:
        container["last_state"] = {"terminated": {}}
    assert not valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "a" * 64
    )


def test_positive_proof_refuses_foreign_digest_and_extra_fields():
    frozen = candidate()
    observed = proof(frozen)
    assert not valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "b" * 64
    )
    observed = deepcopy(observed)
    observed["unexpected"] = True
    assert not valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "a" * 64
    )
