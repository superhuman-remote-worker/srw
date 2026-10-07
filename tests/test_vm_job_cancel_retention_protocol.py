"""Strict wire evidence distinguishes pre-stop validation from disk retention."""

from copy import deepcopy
from uuid import uuid4

import pytest


def frozen_candidate():
    job = str(uuid4())
    return {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": job,
        "provision_generation": str(uuid4()),
        "namespace": "default",
        "vm_name": f"agent-vm-{job}",
        "vm_uid": str(uuid4()),
        "vmi_uid": str(uuid4()),
        "launcher_name": "virt-launcher-test",
        "launcher_uid": str(uuid4()),
        "pvc_uid": str(uuid4()),
        "node_name": "node8",
        "node_uid": str(uuid4()),
        "vm_resource_version": "42",
        "vm_generation": 7,
        "launcher_resource_version": "43",
        "containers": [
            {"kind": "regular", "name": "compute", "container_id": "containerd://old"}
        ],
    }


def preflight(frozen):
    return {
        "version": 1,
        "kind": "vm_cancel_retention_preflight_v1",
        "stop_policy": "cancel_retention_v1",
        "frozen": frozen,
        "namespace": frozen["namespace"],
        "owner_id": frozen["job_id"],
        "pvc_name": "rootdisk-test",
        "pvc_uid": frozen["pvc_uid"],
        "dv_uid": str(uuid4()),
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "extra",
        "owner",
        "pvc",
        "namespace",
        "dv",
        "bool",
        "frozen",
        "no_consumers",
    ],
)
def test_preflight_is_exact_and_never_claims_runtime_absence(fault):
    from shared.vm_cancel_retention import valid_retention_preflight

    frozen = frozen_candidate()
    proof = preflight(frozen)
    if fault == "extra":
        proof["unknown"] = True
    elif fault in {"owner", "pvc", "namespace", "dv"}:
        proof[
            {
                "owner": "owner_id",
                "pvc": "pvc_uid",
                "namespace": "namespace",
                "dv": "dv_uid",
            }[fault]
        ] = "foreign"
    elif fault == "bool":
        proof["version"] = True
    elif fault == "frozen":
        proof["frozen"] = {**frozen, "vm_uid": str(uuid4())}
    elif fault == "no_consumers":
        proof["no_consumers"] = True
    assert valid_retention_preflight(proof, frozen) is (fault is None)


@pytest.mark.parametrize(
    "fault",
    [None, "extra", "owner", "pvc", "namespace", "dv", "bool", "consumers", "deleting"],
)
def test_final_proof_binds_entire_preflight_identity(fault):
    from shared.vm_cancel_retention import (
        retained_rootdisk_from_preflight,
        valid_retained_rootdisk,
    )

    initial = preflight(frozen_candidate())
    expected = retained_rootdisk_from_preflight(initial)
    proof = deepcopy(expected)
    if fault == "extra":
        proof["unknown"] = True
    elif fault in {"owner", "pvc", "namespace", "dv"}:
        key = {
            "owner": "owner_id",
            "pvc": "pvc_uid",
            "namespace": "namespace",
            "dv": "dv_uid",
        }[fault]
        proof[key] = "foreign" if key == "namespace" else str(uuid4())
    elif fault == "bool":
        proof["no_consumers"] = 1
    elif fault == "consumers":
        proof["no_consumers"] = False
    elif fault == "deleting":
        proof["deleting"] = True
    assert valid_retained_rootdisk(proof, initial) is (fault is None)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "foreign", "unauthenticated"])
async def test_old_or_unproven_controller_cannot_start_retention_stop(
    monkeypatch, fault
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_provisioner import VMProvisioner, VMTeardownIdentity

    frozen = frozen_candidate()
    proof = preflight(frozen)
    inspected = {
        "status": "candidate",
        "frozen": frozen,
        "_identity_authenticated": True,
    }
    if fault != "missing":
        inspected["retention_preflight"] = proof
    if fault == "foreign":
        proof["owner_id"] = str(uuid4())
    if fault == "unauthenticated":
        inspected["_identity_authenticated"] = False
    store = SimpleNamespace(
        current_intent=AsyncMock(return_value=None),
        requires_retention_preflight=AsyncMock(return_value=True),
        admit_intent=AsyncMock(
            return_value={"frozen": frozen, "frozen_digest": "sha256:" + "a" * 64}
        ),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_pre_ssh_stop_store.VMPreSSHStopStore",
        lambda _db: store,
    )
    provisioner = VMProvisioner.__new__(VMProvisioner)
    provisioner._db = object()
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner._request_pre_ssh_stop = AsyncMock(side_effect=[inspected, None])
    identity = VMTeardownIdentity(
        frozen["provision_generation"], frozen["vm_uid"], frozen["pvc_uid"]
    )
    assert not await provisioner._attempt_pre_ssh_positive_stop(
        frozen["job_id"], identity, {}
    )
    store.admit_intent.assert_not_awaited()
    assert provisioner._request_pre_ssh_stop.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "missing", "foreign_dv", "foreign_owner", "extra", "consumers"]
)
async def test_compute_attestation_preserves_and_requires_full_retained_witness(fault):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )
    from shared.vm_cancel_retention import retained_rootdisk_from_preflight

    frozen = frozen_candidate()
    initial = preflight(frozen)
    retained = retained_rootdisk_from_preflight(initial)
    if fault == "missing":
        retained = None
    elif fault in {"foreign_dv", "foreign_owner"}:
        retained["dv_uid" if fault == "foreign_dv" else "owner_id"] = str(uuid4())
    elif fault == "extra":
        retained["unknown"] = True
    elif fault == "consumers":
        retained["no_consumers"] = False
    candidate = {
        key: frozen[key]
        for key in (
            "job_id",
            "provision_generation",
            "vm_uid",
            "vmi_uid",
            "launcher_uid",
            "pvc_uid",
        )
    }
    candidate.update(purge_disk=False, retention_preflight=initial)
    provisioner = VMProvisioner.__new__(VMProvisioner)
    provisioner._storage_context = AsyncMock(return_value=None)
    provisioner._current_provision_generation = AsyncMock(
        return_value=frozen["provision_generation"]
    )
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=_VMTeardownProbe(
            "absent",
            VMTeardownIdentity(frozen["provision_generation"], None, frozen["pvc_uid"]),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
            retained_rootdisk=retained,
        )
    )
    result = await provisioner.attest_vm_cleanup_stop(candidate)
    if fault is not None:
        assert result is None
    else:
        assert result["retained_rootdisk"] == retained
