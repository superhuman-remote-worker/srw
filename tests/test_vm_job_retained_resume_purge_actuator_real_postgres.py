"""Typed physical purge never bypasses committed/current logical Delete authority."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_job_retained_disk_purge import (
    acquire_job_retained_disk_purge,
    read_current_retained_purge,
    read_job_retained_disk_purge_candidate,
)
from orchestrator.services.vm_job_retained_resume import settle_retained_no_compute
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_job_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    _pre_ssh_db,  # noqa: F401
    _retention_db,  # noqa: F401
    db as _resume_db,
    enabled,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    resume_schema,  # noqa: F401
    retention_schema,  # noqa: F401
    whole_schema,  # noqa: F401
    accepted_resume,
)

db = _resume_db


async def terminal_tail(db):
    state = await accepted_resume(db)
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    assert await settle_retained_no_compute(db, state["job_id"])
    assert await db.complete_stateless_cancel_cleanup(state["job_id"])
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    return state


def provisioner_for(db, monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"
    provisioner._probe_vm_teardown_identity = AsyncMock(
        side_effect=AssertionError("held authority must not probe")
    )
    provisioner._delete_http = AsyncMock(
        side_effect=AssertionError("held authority must not delete")
    )
    return provisioner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_parent",
        "foreign_parent",
        "foreign_vm",
        "foreign_generation",
        "candidate_digest",
        "no_delete",
        "wrong_current",
    ],
)
async def test_actual_purge_and_attestation_refuse_changed_authority(
    db, monkeypatch, fault
):
    state = await terminal_tail(db)
    identity = state["identity"]
    permit = await acquire_job_retained_disk_purge(
        state["recovery"], job_id=state["job_id"], identity=identity
    )
    candidate = await read_job_retained_disk_purge_candidate(state["recovery"], permit)
    parent = permit.parent_cleanup
    if fault == "missing_parent":
        parent = None
    elif fault == "foreign_parent":
        parent = {**parent, "admission_id": str(uuid4())}
    elif fault in {"foreign_vm", "foreign_generation"}:
        identity = VMTeardownIdentity(
            str(uuid4())
            if fault == "foreign_generation"
            else identity.provision_generation,
            str(uuid4()) if fault == "foreign_vm" else identity.vm_uid,
            identity.rootdisk_pvc_uid,
        )
        candidate = {
            **candidate,
            "vm_uid": identity.vm_uid,
            "provision_generation": identity.provision_generation,
        }
    elif fault == "candidate_digest":
        candidate = {**candidate, "chain_digest": "sha256:" + "0" * 64}
    elif fault == "no_delete":
        await db.execute(
            "UPDATE jobs SET context=context-'_stateless_delete_pending' WHERE id=$1",
            UUID(state["job_id"]),
        )
    elif fault == "wrong_current":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{_vm_job_retained_resume}',to_jsonb($2::text)) WHERE id=$1",
            UUID(state["job_id"]),
            str(uuid4()),
        )
    provisioner = provisioner_for(db, monkeypatch)
    if fault != "candidate_digest":
        result = await provisioner.delete_vm_captured(
            state["job_id"], identity, purge_disk=True, parent_cleanup=parent
        )
        assert result.disposition == "retained_purge_unproven"
    if fault not in {"missing_parent", "foreign_parent"}:
        assert await provisioner.attest_vm_cleanup_stop(candidate) is None
    provisioner._probe_vm_teardown_identity.assert_not_awaited()
    provisioner._delete_http.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_purge_rechecks_current_delete_after_probe_before_transport(
    db, monkeypatch
):
    state = await terminal_tail(db)
    identity = state["identity"]
    permit = await acquire_job_retained_disk_purge(
        state["recovery"], job_id=state["job_id"], identity=identity
    )
    provisioner = provisioner_for(db, monkeypatch)

    async def probe(*args, **kwargs):
        await db.execute(
            "UPDATE jobs SET context=context-'_stateless_delete_pending' WHERE id=$1",
            UUID(state["job_id"]),
        )
        return _VMTeardownProbe(
            "absent",
            VMTeardownIdentity(
                identity.provision_generation, None, identity.rootdisk_pvc_uid
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )

    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=probe)
    result = await provisioner.delete_vm_captured(
        state["job_id"], identity, purge_disk=True, parent_cleanup=permit.parent_cleanup
    )
    assert result.disposition == "retry_pending"
    provisioner._delete_http.assert_not_awaited()
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_receipts WHERE cleanup_admission_id=$1)",
        permit.admission_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("release_savepoint", [False, True])
async def test_typed_purge_reader_refuses_uncommitted_authority_after_savepoint(
    db, release_savepoint
):
    state = await terminal_tail(db)
    identity = state["identity"]
    async with db.acquire() as conn:

        class BorrowedConnection:
            @asynccontextmanager
            async def acquire(self):
                yield conn

        outer = conn.transaction()
        await outer.start()
        savepoint = conn.transaction()
        await savepoint.start()
        try:
            borrowed = BorrowedConnection()
            permit = await acquire_job_retained_disk_purge(
                VMWorkspaceRecoveryStore(borrowed),
                job_id=state["job_id"],
                identity=identity,
            )
            assert permit.allowed
            if release_savepoint:
                await savepoint.commit()
            with pytest.raises(ResourceAdmissionError, match="committed_authority"):
                await read_current_retained_purge(
                    borrowed,
                    job_id=state["job_id"],
                    generation=identity.provision_generation,
                    vm_uid=identity.vm_uid,
                    pvc_uid=identity.rootdisk_pvc_uid,
                    parent_cleanup=permit.parent_cleanup,
                )
        finally:
            await outer.rollback()
