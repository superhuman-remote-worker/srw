"""The Ready stop capability cannot be substituted for a legacy stop witness."""

from copy import deepcopy

import pytest

from shared.vm_cancel_retention import valid_retention_preflight
from tests.test_vm_pre_ssh_stop_protocol import candidate, uid


def ready_pair():
    frozen = candidate()
    frozen.update(
        kind="vm_initial_ready_positive_stop_candidate_v1",
        cleanup_admission_id=uid(10),
        cleanup_request_id=uid(11),
        cleanup_intent_digest="sha256:" + "a" * 64,
    )
    authority = {
        key: frozen[key]
        for key in (
            "job_id",
            "provision_generation",
            "namespace",
            "vm_uid",
            "vmi_uid",
            "launcher_uid",
            "pvc_uid",
            "node_uid",
            "cleanup_request_id",
            "cleanup_intent_digest",
        )
    }
    authority.update(
        version=1,
        kind="vm_job_initial_ready_stop_candidate_v1",
        owner_kind="job",
        request_id=uid(12),
        reservation_id=uid(13),
        reservation_revision=1,
        cluster_id="cluster-a",
    )
    preflight = dict(
        version=1,
        kind="vm_job_initial_ready_preflight_v1",
        stop_policy="initial_ready_cancel_v1",
        frozen=authority,
        namespace=frozen["namespace"],
        owner_id=frozen["job_id"],
        pvc_name=f"agent-vm-{frozen['job_id']}-rootdisk",
        pvc_uid=frozen["pvc_uid"],
        dv_uid=uid(14),
        ownership="standalone_dv",
        deleting=False,
        consumer_scope="exact_frozen_runtime_only",
    )
    return frozen, preflight


def test_ready_positive_stop_requires_exact_stored_authority_witness():
    frozen, preflight = ready_pair()
    assert valid_retention_preflight(preflight, frozen)


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
        "cleanup_request_id",
        "cleanup_intent_digest",
        "kind",
    ],
)
def test_ready_stop_refuses_foreign_frozen_identity(field):
    frozen, preflight = ready_pair()
    changed = deepcopy(frozen)
    changed[field] = "foreign"
    assert not valid_retention_preflight(preflight, changed)


def test_ready_stop_cannot_borrow_never_ready_kind():
    frozen, preflight = ready_pair()
    frozen["kind"] = "vm_pre_ssh_stop_candidate_v1"
    assert not valid_retention_preflight(preflight, frozen)


@pytest.mark.asyncio
async def test_initial_ready_qualifier_accepts_current_not_ready_without_weakening_continuation(
    monkeypatch,
):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_provisioner import VMProvisioner
    from tests.test_vm_job_retained_resume_protocol import (
        ready_candidate,
        ready_preflight,
    )

    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._controller_url = "http://controller.invalid"
    provisioner._lifecycle_hmac_secret = b"test-only"
    provisioner._storage_context = AsyncMock(return_value=None)
    _, preflight = ready_pair()
    candidate = preflight["frozen"]
    observation = {
        "_identity_authenticated": True,
        "job_id": candidate["job_id"],
        "provision_generation": candidate["provision_generation"],
        "vm_uid": candidate["vm_uid"],
        "vmi_uid": candidate["vmi_uid"],
        "active_pod_uid": candidate["launcher_uid"],
        "ready": False,
        "retention_preflight": preflight,
    }
    provisioner._query_http = AsyncMock(return_value=observation)
    assert await provisioner.qualify_retained_ready_stop(candidate) == preflight
    for invalid in (None, "false", 0):
        observation["ready"] = invalid
        assert await provisioner.qualify_retained_ready_stop(candidate) is None
    continuation = ready_candidate()
    provisioner._query_http.return_value = {
        "_identity_authenticated": True,
        "job_id": continuation["job_id"],
        "provision_generation": continuation["provision_generation"],
        "vm_uid": continuation["vm_uid"],
        "vmi_uid": continuation["vmi_uid"],
        "active_pod_uid": continuation["launcher_uid"],
        "ready": False,
        "retention_preflight": ready_preflight(continuation),
    }
    assert await provisioner.qualify_retained_ready_stop(continuation) is None
