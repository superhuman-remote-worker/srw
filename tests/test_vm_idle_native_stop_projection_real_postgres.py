"""Native HTTP teardown projection must compose with exact idle settlement."""

import json
import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore, VMIdleLifecycleService
from orchestrator.services.vm_provisioner import VMProvisioner, VMTeardownIdentity
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore, acquire_vm_cleanup_permit, complete_vm_cleanup_permit,
    cleanup_intent_digest,
)
from shared.vm_lifecycle_auth import sign_payload
from tests.test_vm_idle_admission_handoff_real_postgres import (
    db as _runtime_db,
    runtime_schema,  # noqa: F401
    _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
)
from tests.test_vm_resource_job_runtime_real_postgres import charged_idle_wait

db = _runtime_db


async def native_deleted_idle(db, monkeypatch, *, outcome="completed", pinned=False):
    from tests.test_vm_resource_job_runtime_real_postgres import charged_pinned_idle_wait

    seed = charged_pinned_idle_wait if pinned else charged_idle_wait
    policy, retry, admitted, episode, identity = await seed(db, monkeypatch)
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document))
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(retry["job_id"]), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation is not None
    # Prior positive physical evidence is the fixture boundary. W9 separately
    # observes this exact path against a real KubeVirt guest and controller.
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", retry["job_id"], identity["generation"],
    )
    captured = VMTeardownIdentity(
        provision_generation=identity["generation"], vm_uid=identity["vm_uid"],
        rootdisk_pvc_uid=identity["pvc_uid"],
    )
    recovery = VMWorkspaceRecoveryStore(db)
    permit = await acquire_vm_cleanup_permit(
        recovery, owner_kind="job", owner_id=str(retry["job_id"]),
        identity=captured, source="vm_idle_release", purge_disk=False,
    )
    assert permit.allowed
    secret = b"native-idle-delete-test-secret-32"

    def controller_delete(request):
        assert request.method == "DELETE"
        assert request.url.params["purge_disk"] == "false"
        assert request.url.params["provision_generation"] == identity["generation"]
        assert request.url.params["expected_vm_uid"] == identity["vm_uid"]
        assert request.url.params["expected_rootdisk_pvc_uid"] == identity["pvc_uid"]
        return httpx.Response(200, json=sign_payload(
            {"provision_generation": identity["generation"], "deleted": True},
            direction="response", operation="delete", secret=secret,
            correlation_id=request.url.params["lifecycle_auth_request_id"],
        ))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(controller_delete), base_url="http://controller",
    ) as client:
        provisioner = VMProvisioner()
        provisioner._db = db
        provisioner._http_client = client
        provisioner._lifecycle_hmac_secret = secret
        assert await provisioner._delete_http(
            str(retry["job_id"]), purge_disk=False,
            provision_generation=identity["generation"],
            expected_vm_uid=identity["vm_uid"], expected_rootdisk_pvc_uid=identity["pvc_uid"],
            parent_cleanup=permit.parent_cleanup,
        )
    assert await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", retry["job_id"]
    ) == "deleted"
    if outcome is not None:
        await complete_vm_cleanup_permit(recovery, permit, outcome=outcome)
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "retained_pvc": True, "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    return retry, admitted, operation, permit, evidence


@pytest.mark.asyncio
async def test_native_deleted_projection_settles_exact_idle_stop(db, monkeypatch):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    assert await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval(
        "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", retry["job_id"]
    ) == "suspended"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]
    ) == "released"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "missing", "incomplete", "identity_superseded", "source", "owner", "pvc",
    "generation", "vm_uid", "purge_disk", "resource", "parent",
])
async def test_deleted_projection_needs_exact_completed_idle_permit(db, monkeypatch, change):
    retry, _, operation, permit, evidence = await native_deleted_idle(
        db, monkeypatch, outcome=None if change == "incomplete" else
        "identity_superseded" if change == "identity_superseded" else "completed",
    )
    if change == "missing":
        await db.execute("DELETE FROM vm_workspace_cleanup_admissions WHERE id=$1", permit.admission_id)
    elif change == "source":
        await db.execute("UPDATE vm_workspace_cleanup_admissions SET source='unrelated_cleanup' WHERE id=$1", permit.admission_id)
    elif change in {"owner", "pvc"}:
        column = "owner_id" if change == "owner" else "pvc_uid"
        await db.execute(f"UPDATE vm_workspace_cleanup_admissions SET {column}=$2 WHERE id=$1", permit.admission_id, uuid4())
    elif change == "parent":
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET parent_admission_id="
            "(SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$2) WHERE id=$1",
            permit.admission_id, retry["request_id"],
        )
    elif change in {"generation", "vm_uid", "purge_disk", "resource"}:
        intent = dict(permit.parent_cleanup["intent"])
        key = "provision_generation" if change == "generation" else change
        intent[key] = True if change == "purge_disk" else "other" if change == "resource" else str(uuid4())
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
            permit.admission_id, cleanup_intent_digest(intent),
        )
    before = await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])
    assert not await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"]) == before
    assert await db.fetchval("SELECT stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"]) is None
    assert await db.fetchval("SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]) == "teardown"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "zero", "generation", "vm_uid", "pvc", "marker", "closed", "replacement", "unauthenticated",
])
async def test_deleted_projection_keeps_exact_stop_and_owner_fences(db, monkeypatch, change):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"]))
    if change == "zero":
        await db.execute("DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1", retry["job_id"])
    elif change == "closed":
        await db.execute("UPDATE vm_idle_operations SET phase='superseded',closed_at=clock_timestamp() WHERE id=$1", operation["id"])
    elif change in {"replacement", "unauthenticated"}:
        evidence["same_generation_replacement" if change == "replacement" else "controller_authenticated"] = change == "replacement"
    else:
        field = {"generation": "provision_generation", "pvc": "rootdisk_pvc_uid", "marker": "_suspend_remote_io_closed"}.get(change, change)
        context["vm"][field] = str(uuid4())
        await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", retry["job_id"], json.dumps(context))
    before = await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])
    assert not await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"]) == before
    assert await db.fetchval("SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]) == "teardown"


@pytest.mark.asyncio
async def test_reconstructed_release_reuses_completed_permit_without_second_delete(db, monkeypatch):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner.release_vm_captured = AsyncMock(side_effect=AssertionError("must not repeat completed physical delete"))
    provisioner.attest_vm_idle_stop = AsyncMock(return_value=evidence)
    service = VMIdleLifecycleService(db, provisioner, VMWorkspaceRecoveryStore(db))
    claimed = await service.store.claim(str(operation["id"]), claimant=service.claimant)
    assert claimed is not None
    assert await service._release(claimed, current=lambda: True)
    receipt = await db.fetchrow("SELECT stop_evidence,stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"])
    charge = await db.fetchrow("SELECT state,release_evidence FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"])
    assert await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert dict(await db.fetchrow("SELECT stop_evidence,stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"])) == dict(receipt)
    assert dict(await db.fetchrow("SELECT state,release_evidence FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"])) == dict(charge)
    provisioner.release_vm_captured.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["cancelled", "completed", "failed"])
async def test_terminal_job_stop_settlement_does_not_create_wake_or_execution(db, monkeypatch, terminal):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    if terminal == "cancelled":
        assert await db.cancel_job(str(retry["job_id"]))
    else:
        await db.update_job_status(str(retry["job_id"]), terminal)
    assert await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", retry["job_id"]) == terminal
    settled = await db.fetchrow("SELECT wake_requested,wake_execution_requested,wake_id FROM vm_idle_operations WHERE id=$1", operation["id"])
    assert dict(settled) == {"wake_requested": False, "wake_execution_requested": False, "wake_id": None}
    assert await VMIdleLifecycleStore(db).request_wake(str(retry["job_id"]), execution_requested=True) is None
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", retry["job_id"]) == "done"
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["explicit", "episode_change"])
async def test_nonterminal_resume_intent_survives_proven_stop(db, monkeypatch, kind):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    store = VMIdleLifecycleStore(db)
    if kind == "explicit":
        wake = await store.request_wake(str(retry["job_id"]), execution_requested=True)
        assert wake is not None and wake["id"] == operation["id"]
    else:
        assert await db.queue_stateless_job_for_resume(
            str(retry["job_id"]), expected_status="waiting_for_reply",
        )
        assert await db.fetchval("SELECT workspace_idle_episode FROM jobs WHERE id=$1", retry["job_id"]) is None
    before = await db.fetchrow(
        "SELECT j.status,q.state FROM jobs j JOIN run_queue q ON q.unit_id=j.id WHERE j.id=$1",
        retry["job_id"],
    )
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    current = await store.get_operation(str(operation["id"]))
    assert current["wake_requested"] and current["wake_execution_requested"]
    assert current["wake_id"] == (wake["wake_id"] if kind == "explicit" else None)
    after = await db.fetchrow(
        "SELECT j.status,q.state FROM jobs j JOIN run_queue q ON q.unit_id=j.id WHERE j.id=$1",
        retry["job_id"],
    )
    assert dict(after) == dict(before)
    assert after["status"] == ("waiting_for_reply" if kind == "explicit" else "paused")
    assert after["state"] == ("done" if kind == "explicit" else "queued")


@pytest.mark.asyncio
async def test_deleted_vm_does_not_replace_pinned_actor_stop_authority(db, monkeypatch):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch, pinned=True)
    assert operation["release_kind"] == "pinned_job"
    assert not await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval("SELECT stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"]) is None
    assert await db.fetchval("SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]) == "teardown"


@pytest.mark.asyncio
async def test_stop_receipt_and_projection_roll_back_with_charge_release(db, monkeypatch):
    from orchestrator.services.vm_resource_reservation_store import VMResourceReservationStore

    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    async def fail(*args, **kwargs):
        raise RuntimeError("charge transaction failure")
    monkeypatch.setattr(VMResourceReservationStore, "release_idle_compute_on_conn", fail)
    with pytest.raises(RuntimeError, match="charge transaction failure"):
        await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)
    assert await db.fetchval("SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", retry["job_id"]) == "deleted"
    assert await db.fetchval("SELECT stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"]) is None
    assert await db.fetchval("SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]) == "teardown"


@pytest.mark.asyncio
async def test_incomplete_receipt_refuses_without_waiting_on_completion_row_lock(db, monkeypatch):
    _, _, operation, permit, evidence = await native_deleted_idle(db, monkeypatch, outcome=None)
    recovery = VMWorkspaceRecoveryStore(db)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR UPDATE", permit.admission_id)
        completion = asyncio.create_task(recovery.complete_cleanup_permit(permit.admission_id, outcome="completed"))
        await asyncio.sleep(.05)
        assert not completion.done()
        assert not await asyncio.wait_for(
            VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence), 2,
        )
    assert await completion
    assert await VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence)


@pytest.mark.asyncio
async def test_successor_winning_owner_lock_cannot_be_overwritten_by_stop_settlement(db, monkeypatch):
    retry, _, operation, _, evidence = await native_deleted_idle(db, monkeypatch)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", retry["job_id"])
        row = await conn.fetchrow("SELECT context FROM jobs WHERE id=$1 FOR UPDATE", retry["job_id"])
        successor = json.loads(row["context"])
        successor["vm"].update(status="ready", provision_generation=str(uuid4()), vm_uid=str(uuid4()))
        await conn.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", retry["job_id"], json.dumps(successor))
        completion = asyncio.create_task(VMIdleLifecycleStore(db).complete_release(str(operation["id"]), evidence=evidence))
        await asyncio.sleep(.05)
        assert not completion.done()
    assert not await completion
    assert json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])) == successor
    assert await db.fetchval("SELECT stop_verified_at FROM vm_idle_operations WHERE id=$1", operation["id"]) is None
    assert await db.fetchval("SELECT state FROM vm_resource_reservations WHERE request_id=$1", retry["request_id"]) == "teardown"
