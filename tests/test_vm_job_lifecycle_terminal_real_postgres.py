"""An exact completed lifecycle VM purge remains a Job terminal source."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
import pytest

from orchestrator.services.lifecycle.reconciler import InstanceLifecycleReconciler
from orchestrator.services.lifecycle.types import Instance
from orchestrator.services.lifecycle.vm_manager import VMInstanceManager
from orchestrator.services.vm_provisioner import VMTeardownIdentity
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from tests.test_vm_resource_job_runtime_real_postgres import (
    charged_idle_wait,
    db,  # noqa: F401
    runtime_schema,  # noqa: F401
    whole_schema,  # noqa: F401
    pg_dsn,  # noqa: F401
    _db_fixture,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
)


async def _completed_lifecycle_purge(store_db, monkeypatch, *, process_zero=True):
    _, retry, admitted, _, ids = await charged_idle_wait(
        store_db, monkeypatch, lane="pinned"
    )
    owner_id = retry["job_id"]
    await store_db.execute(
        "UPDATE jobs SET status='pending_review',"
        "context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"'::jsonb) WHERE id=$1",
        owner_id,
    )
    if process_zero:
        await store_db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            owner_id,
            ids["generation"],
        )
    identity = VMTeardownIdentity(ids["generation"], ids["vm_uid"], ids["pvc_uid"])
    proof = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": str(owner_id),
        "provision_generation": ids["generation"],
        "vm_uid": ids["vm_uid"],
        "vmi_uid": ids["vmi_uid"],
        "launcher_uid": ids["launcher_uid"],
        "pvc_uid": ids["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "purged",
        "controller_authenticated": True,
    }
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        attest_vm_cleanup_stop=AsyncMock(return_value=proof),
        release_vm_captured=AsyncMock(),
    )
    store = VMWorkspaceRecoveryStore(store_db)
    manager = VMInstanceManager(
        provisioner, None, None, store_db, workspace_recovery_store=store
    )
    parent = await manager._admit_destructive_cleanup(
        owner_kind="job",
        owner_id=str(owner_id),
        identity=identity,
        source="delete",
        purge_disk=True,
    )
    assert parent is not None and parent.allowed
    child = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=UUID(ids["pvc_uid"]),
        request_id=uuid4(),
        source="controller_rootdisk_delete",
        intent_digest="sha256:controller-child-deleted",
        parent_cleanup=parent.parent_cleanup,
        parent_provision_generation=ids["generation"],
        expected_vm_uid=ids["vm_uid"],
    )
    assert child.allowed
    assert await store.complete_cleanup_permit(child.admission_id, outcome="deleted")
    inst = Instance(
        kind="vm",
        id=f"agent-vm-{owner_id}",
        bound_to=str(owner_id),
        metadata={
            "scope": "job",
            "execution_lane": "pinned",
            "job_status": "pending_review",
            "vm_status": "retiring_process_zero",
        },
    )
    manager.is_dirty = AsyncMock(return_value=True)
    manager.give_up = AsyncMock()
    stats = {"reaped": 0, "reap_forced": 0, "reap_attempts": 0}
    await InstanceLifecycleReconciler([manager])._reap(manager, inst, stats)
    assert stats["reaped"] == int(process_zero)
    assert stats["reap_forced"] == 0
    manager.is_dirty.assert_not_awaited()
    manager.give_up.assert_not_awaited()
    row = await store_db.fetchrow(
        "SELECT id,request_id,source,intent_digest,completed_at,outcome "
        "FROM vm_workspace_cleanup_admissions WHERE id=$1",
        parent.admission_id,
    )
    assert row["source"] == "lifecycle_vm_delete"
    assert (row["completed_at"] is not None) is process_zero
    if process_zero:
        assert row["outcome"] == "completed"
    assert await store_db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == ("released" if process_zero else "teardown")
    if process_zero:
        await store_db.execute(
            "UPDATE jobs SET status='completed',"
            "context=jsonb_set(context,'{vm,status}','\"deleted\"'::jsonb) "
            "WHERE id=$1",
            owner_id,
        )
    current_retry = await store_db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", retry["request_id"]
    )
    return current_retry, parent, row


@pytest.mark.asyncio
async def test_completed_lifecycle_vm_delete_yields_exact_physical_terminal_packet(
    db,  # noqa: F811
    monkeypatch,
):
    retry, parent, admission = await _completed_lifecycle_purge(db, monkeypatch)
    assert admission["request_id"] == uuid5(
        NAMESPACE_URL,
        f"workspace-cleanup:delete:job:{retry['job_id']}:"
        f"{retry['provision_generation']}:{retry['observed_vm_uid']}:"
        f"{retry['observed_pvc_uid']}",
    )
    assert admission["intent_digest"].startswith("sha256:")
    prior = await db.fetchval(
        "SELECT vm_job_terminal_prior_packet_evidence($1)", retry["request_id"]
    )
    assert prior is not None
    if isinstance(prior, str):
        prior = json.loads(prior)
    assert prior["kind"] == "physical_stop"
    assert prior["job_id"] == str(retry["job_id"])
    assert prior["request_id"] == str(retry["request_id"])
    assert prior["provision_generation"] == str(retry["provision_generation"])
    assert prior["vm_uid"] == str(retry["observed_vm_uid"])
    assert prior["pvc_uid"] == str(retry["observed_pvc_uid"])
    assert prior["cleanup_admission_id"] == str(parent.admission_id)
    assert prior["cleanup_intent_digest"] == admission["intent_digest"]
    assert prior["reservation_count"] == 1
    assert await db.delete_job(str(retry["job_id"])) is True
    packet = await db.fetchrow(
        "SELECT terminal_kind,evidence FROM vm_job_creation_terminal_packets "
        "WHERE request_id=$1",
        retry["request_id"],
    )
    assert packet["terminal_kind"] == "physical_stop"
    evidence = packet["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    assert evidence == prior
    owner = await db.fetchrow(
        "SELECT live_job_id,deleted_at,deletion_receipt "
        "FROM vm_job_creation_owners WHERE job_id=$1",
        retry["job_id"],
    )
    assert owner["live_job_id"] is None and owner["deleted_at"] is not None
    receipt = owner["deletion_receipt"]
    if isinstance(receipt, str):
        receipt = json.loads(receipt)
    assert receipt["generations"] == [prior]


@pytest.mark.asyncio
async def test_lifecycle_terminal_requires_process_zero_receipt(db, monkeypatch):  # noqa: F811
    retry, _, _ = await _completed_lifecycle_purge(db, monkeypatch, process_zero=False)
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", retry["request_id"]
        )
        is None
    )


@pytest.mark.asyncio
async def test_lifecycle_terminal_refuses_changed_historical_parent(db, monkeypatch):  # noqa: F811
    retry, parent, admission = await _completed_lifecycle_purge(db, monkeypatch)
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", retry["request_id"]
        )
        is not None
    )
    obsolete_request = uuid5(
        NAMESPACE_URL,
        f"vm-workspace-cleanup:lifecycle_vm_delete:job:{retry['job_id']}:"
        f"{retry['provision_generation']}:{retry['observed_vm_uid']}:"
        f"{retry['observed_pvc_uid']}",
    )
    assert obsolete_request != admission["request_id"]
    changes = (
        ("request_id", uuid4()),
        ("request_id", obsolete_request),
        ("intent_digest", "sha256:" + "0" * 64),
        ("source", "lifecycle_vm_give_up"),
        ("completed_at", None),
    )
    async with db.acquire() as conn:
        for column, value in changes:
            tx = conn.transaction()
            await tx.start()
            try:
                # Exercise the projector itself against a corrupt legacy row;
                # immutable-row triggers ordinarily reject these mutations.
                await conn.execute("SET LOCAL session_replication_role=replica")
                if column == "completed_at":
                    await conn.execute(
                        "UPDATE vm_workspace_cleanup_admissions "
                        "SET completed_at=NULL,outcome=NULL WHERE id=$1",
                        parent.admission_id,
                    )
                else:
                    await conn.execute(
                        f"UPDATE vm_workspace_cleanup_admissions SET {column}=$2 "
                        "WHERE id=$1",
                        parent.admission_id,
                        value,
                    )
                assert (
                    await conn.fetchval(
                        "SELECT vm_job_terminal_prior_packet_evidence($1)",
                        retry["request_id"],
                    )
                    is None
                ), column
            finally:
                await tx.rollback()
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", retry["request_id"]
        )
        is not None
    )


@pytest.mark.asyncio
async def test_lifecycle_terminal_refuses_changed_generation_or_issued_effect(
    db,  # noqa: F811
    monkeypatch,
):
    retry, _, _ = await _completed_lifecycle_purge(db, monkeypatch)
    changes = (
        (
            "generation",
            "UPDATE vm_creation_retries SET provision_generation=$2 "
            "WHERE request_id=$1",
            (retry["request_id"], uuid4()),
        ),
        (
            "issued_effect",
            "INSERT INTO vm_creation_effects "
            "(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,"
            "carrier_namespace,carrier_intent,state) "
            "VALUES($1,$2,99,'vm',$3,'workers','{}'::jsonb,'issued')",
            (uuid4(), retry["request_id"], uuid4()),
        ),
    )
    async with db.acquire() as conn:
        for label, sql, args in changes:
            tx = conn.transaction()
            await tx.start()
            try:
                try:
                    await conn.execute(sql, *args)
                except asyncpg.CheckViolationError as exc:
                    assert exc.sqlstate == "23514"
                else:
                    assert (
                        await conn.fetchval(
                            "SELECT vm_job_terminal_prior_packet_evidence($1)",
                            retry["request_id"],
                        )
                        is None
                    ), label
            finally:
                await tx.rollback()
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", retry["request_id"]
        )
        is not None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        "missing_stop",
        "mismatched_stop",
        "unreleased_charge",
        "unreleased_waiter",
        "wrong_vm",
        "wrong_pvc",
    ),
)
async def test_lifecycle_terminal_retains_exact_physical_and_charge_gates(
    db,  # noqa: F811
    monkeypatch,
    mutation,
):
    retry, parent, _ = await _completed_lifecycle_purge(db, monkeypatch)
    request_id = retry["request_id"]
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", request_id
        )
        is not None
    )
    async with db.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            # Exercise the SQL projector against impossible/corrupt source
            # rows; immutable-row triggers normally reject these mutations.
            await conn.execute("SET LOCAL session_replication_role=replica")
            if mutation == "missing_stop":
                await conn.execute(
                    "DELETE FROM vm_resource_cleanup_stop_receipts "
                    "WHERE cleanup_admission_id=$1",
                    parent.admission_id,
                )
            elif mutation == "mismatched_stop":
                row = await conn.fetchrow(
                    "SELECT stop_evidence FROM vm_resource_cleanup_stop_receipts "
                    "WHERE cleanup_admission_id=$1",
                    parent.admission_id,
                )
                proof = row["stop_evidence"]
                if isinstance(proof, str):
                    proof = json.loads(proof)
                proof["launcher_absent"] = False
                await conn.execute(
                    "UPDATE vm_resource_cleanup_stop_receipts "
                    "SET stop_evidence=$2::jsonb WHERE cleanup_admission_id=$1",
                    parent.admission_id,
                    json.dumps(proof),
                )
            elif mutation == "unreleased_charge":
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='teardown',"
                    "released_at=NULL,release_evidence=NULL "
                    "WHERE request_id=$1",
                    request_id,
                )
            elif mutation == "unreleased_waiter":
                assert (
                    await conn.execute(
                        "UPDATE vm_resource_waiters SET state='waiting' "
                        "WHERE request_id=$1",
                        request_id,
                    )
                    == "UPDATE 1"
                )
            elif mutation == "wrong_vm":
                await conn.execute(
                    "UPDATE vm_creation_retries SET observed_vm_uid=$2 "
                    "WHERE request_id=$1",
                    request_id,
                    uuid4(),
                )
            elif mutation == "wrong_pvc":
                await conn.execute(
                    "UPDATE vm_creation_retries SET observed_pvc_uid=$2 "
                    "WHERE request_id=$1",
                    request_id,
                    uuid4(),
                )
            assert (
                await conn.fetchval(
                    "SELECT vm_job_terminal_prior_packet_evidence($1)", request_id
                )
                is None
            )
        finally:
            await tx.rollback()
    assert (
        await db.fetchval(
            "SELECT vm_job_terminal_prior_packet_evidence($1)", request_id
        )
        is not None
    )
