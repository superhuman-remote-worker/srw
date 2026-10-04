"""Controlled authenticated teardown observations with real cleanup admissions."""

from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_pinned_thread_retirement_cleanup_permit,
    cleanup_intent_digest,
    complete_vm_cleanup_permit,
    completed_cleanup_outcome,
    vm_cleanup_kwargs,
)
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)


def observation(identity, *, vm=True, disk=True, writers=False):
    return _VMTeardownProbe(
        "present" if vm else "absent",
        replace(
            identity,
            vm_uid=identity.vm_uid if vm else None,
            rootdisk_pvc_uid=identity.rootdisk_pvc_uid if disk else None,
        ),
        rootdisk_identity_known=True,
        runtime_absence_known=not vm and not writers,
        vmi_absent=not vm and not writers,
        launcher_absent=not vm and not writers,
    )


@pytest.mark.parametrize(
    "vm,disk,writers,expected",
    [
        (True, False, False, "unknown"),
        (False, True, False, "matched"),
        (False, False, False, "completed"),
        (False, False, True, "unknown"),
    ],
)
def test_disk_absence_is_incomplete_not_replacement(vm, disk, writers, expected):
    identity = VMTeardownIdentity(str(uuid4()), str(uuid4()), str(uuid4()))
    assert (
        VMProvisioner._classify_captured_probe(
            observation(identity, vm=vm, disk=disk, writers=writers), identity
        )
        == expected
    )


async def admitted_teardown(db, setup, monkeypatch, *, disk_first):
    current, source = await adopted_source(db, setup, monkeypatch)
    owner = str(current["id"])
    retirement = await db.begin_pinned_thread_retirement(owner, permanent=True)
    assert await db.authorize_pinned_thread_retirement(
        owner,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    identity = VMTeardownIdentity(
        str(source["provision_generation"]),
        str(source["observed_vm_uid"]),
        str(source["observed_pvc_uid"]),
    )
    assert await db.record_managed_repository_workspace_process_zero(
        owner,
        owner_kind="thread",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=identity.provision_generation,
    )
    store = VMWorkspaceRecoveryStore(db)
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        store, thread_id=owner, identity=identity, purge_disk=True
    )
    assert permit.allowed and completed_cleanup_outcome(permit) is None
    physical = dict(vm=True, disk=True)
    child = None

    async def delete(_owner, **kwargs):
        nonlocal child
        assert _owner == owner
        assert kwargs["expected_vm_uid"] == identity.vm_uid
        assert kwargs["expected_rootdisk_pvc_uid"] == identity.rootdisk_pvc_uid
        assert kwargs["provision_generation"] == identity.provision_generation
        # Model the authenticated controller boundary, including its durable
        # delegated child. Production admission/completion perform all SQL.
        proof = kwargs["parent_cleanup"]
        child = await store.acquire_cleanup_permit(
            owner_kind="thread",
            owner_id=current["id"],
            pvc_uid=UUID(identity.rootdisk_pvc_uid),
            request_id=uuid4(),
            source="controller_rootdisk_delete",
            intent_digest=cleanup_intent_digest(
                {"parent": proof, "disk": identity.rootdisk_pvc_uid}
            ),
            parent_cleanup=proof,
            parent_provision_generation=identity.provision_generation,
            expected_vm_uid=identity.vm_uid,
        )
        assert child.allowed
        physical.update(vm=disk_first, disk=not disk_first)

    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._probe_vm_teardown_identity = AsyncMock(
        side_effect=lambda *_: observation(identity, **physical)
    )
    provisioner._delete_vm_with_identity = AsyncMock(side_effect=delete)
    result = await provisioner.release_vm_captured(
        owner,
        identity,
        entity_type="thread",
        capture_snapshot=False,
        **vm_cleanup_kwargs(permit),
    )
    if result.disposition in {"completed", "identity_superseded"}:
        await complete_vm_cleanup_permit(store, permit, outcome=result.disposition)
    return (
        current,
        source,
        retirement,
        identity,
        store,
        permit,
        provisioner,
        physical,
        child,
        result,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "disk_first", [True, False], ids=["disk-before-vm", "vm-before-disk"]
)
async def test_authorized_partial_teardown_remains_retryable_until_exact_completion(
    db,
    setup,
    monkeypatch,
    disk_first,
):
    (
        current,
        _,
        _,
        identity,
        store,
        permit,
        provisioner,
        physical,
        child,
        result,
    ) = await admitted_teardown(db, setup, monkeypatch, disk_first=disk_first)
    assert result.disposition == "retry_pending", (
        "authorized partial disappearance became a terminal refusal"
    )
    row = await db.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", permit.admission_id
    )
    assert row["completed_at"] is None and row["outcome"] is None
    physical.update(vm=False, disk=False)
    assert await store.complete_cleanup_permit(child.admission_id, outcome="deleted")
    retry = await acquire_pinned_thread_retirement_cleanup_permit(
        store,
        thread_id=current["id"],
        identity=identity,
        purge_disk=True,
    )
    assert retry.admission_id == permit.admission_id
    assert completed_cleanup_outcome(retry) is None
    result = await provisioner.release_vm_captured(
        str(current["id"]),
        identity,
        entity_type="thread",
        capture_snapshot=False,
        **vm_cleanup_kwargs(retry),
    )
    assert result.deleted and result.disposition == "completed"
    await complete_vm_cleanup_permit(store, retry, outcome=result.disposition)
    replay = await acquire_pinned_thread_retirement_cleanup_permit(
        store,
        thread_id=current["id"],
        identity=identity,
        purge_disk=True,
    )
    assert replay.admission_id == permit.admission_id
    assert completed_cleanup_outcome(replay) == "completed"
    provisioner._delete_vm_with_identity.assert_awaited_once()
