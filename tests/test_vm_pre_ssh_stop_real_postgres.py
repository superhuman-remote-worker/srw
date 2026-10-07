"""Exact pre-SSH Job VM stop authority and atomic zero on real PostgreSQL."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.job_controls import JobControlOperations
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_pre_ssh_stop_store import (
    VMPreSSHStopConflict,
    VMPreSSHStopStore,
)
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
    vm_cleanup_request_identity,
)
from shared.vm_pre_ssh_stop import PRE_SSH_STOP_FINALIZER
from tests.test_vm_resource_whole_store_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db_fixture,  # noqa: F401
    environment,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    waiter,
    whole_schema,  # noqa: F401
)


@pytest_asyncio.fixture(scope="module")
async def pre_ssh_schema(pg_dsn, _schema_applied):  # noqa: F811
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0333_vm_pre_ssh_positive_stop.sql"
    )
    late_purge = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0334_vm_job_retained_disk_late_purge.sql"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_pre_ssh_stop_intents') IS NOT NULL"
        ):
            await conn.execute(migration.read_text())
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_disk_purge_authorities') "
            "IS NOT NULL"
        ):
            await conn.execute(late_purge.read_text())
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pre_ssh_schema, _db_fixture):  # noqa: F811
    yield _db_fixture


async def seeded_stop(db):
    policy, inventory, _, _ = await environment(db, installation_count=2)
    retry = await waiter(db, policy, inventory, lane="stateless", user_id=uuid4())
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    job_id, generation = str(claim["job_id"]), str(claim["provision_generation"])
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (str(uuid4()) for _ in range(4))
    await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": job_id,
            "provision_generation": generation,
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',reason='creation_adopted',"
        "boot_counted=TRUE,revision=revision+1,observed_vm_uid=$2,"
        "observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"],
        UUID(vm_uid),
        UUID(pvc_uid),
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=(SELECT creation_admission_id "
        "FROM vm_creation_retries WHERE request_id=$1)",
        retry["request_id"],
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]),
        UUID(vm_uid),
        UUID(vmi_uid),
        UUID(launcher_uid),
    )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", UUID(job_id))
    )
    context["vm"].update(
        status="created",
        provision_generation=generation,
        vm_uid=vm_uid,
        vmi_uid=vmi_uid,
        active_pod_uid=launcher_uid,
        rootdisk_pvc_uid=pvc_uid,
        identity_authenticated=True,
        identity_provision_generation=generation,
        creation_request_id=str(retry["request_id"]),
    )
    context.pop("_vm_creation_pending", None)
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        UUID(job_id),
        json.dumps(context),
    )
    permit = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=job_id,
        identity=VMTeardownIdentity(generation, vm_uid, pvc_uid),
        source="dispatcher_vm_recycle",
        purge_disk=False,
    )
    assert permit.allowed and permit.parent_cleanup is not None
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"') WHERE id=$1",
        UUID(job_id),
    )
    frozen = {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": job_id,
        "provision_generation": generation,
        "namespace": inventory.namespace,
        "vm_name": "agent-vm-" + job_id,
        "vm_uid": vm_uid,
        "vmi_uid": vmi_uid,
        "launcher_name": "virt-launcher-owned",
        "launcher_uid": launcher_uid,
        "pvc_uid": pvc_uid,
        "node_name": admitted["node_name"],
        "node_uid": admitted["node_uid"],
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
    return {
        "job_id": job_id,
        "generation": generation,
        "permit": permit.parent_cleanup,
        "cleanup_permit": permit,
        "frozen": frozen,
        "reservation_id": admitted["reservation_id"],
        "store": VMPreSSHStopStore(db),
    }


def terminal_proof(frozen, digest):
    return {
        "kind": "vm_pre_ssh_positive_stop_v1",
        "frozen_digest": digest,
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
        "pod_intent_digest": digest,
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


@pytest.mark.asyncio
async def test_exact_job_intent_requires_proof_before_atomic_zero(db):
    state = await seeded_stop(db)
    store = state["store"]
    intent = await store.admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    assert (
        await store.admit_intent(
            state["job_id"], state["generation"], state["permit"], state["frozen"]
        )
        == intent
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            UUID(state["job_id"]),
            state["generation"],
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    observed = terminal_proof(state["frozen"], intent["frozen_digest"])
    receipt = await store.commit_positive_proof(
        state["job_id"], state["generation"], state["permit"], observed
    )
    assert receipt["process_zero_receipt_id"]
    assert (
        await store.commit_positive_proof(
            state["job_id"], state["generation"], state["permit"], observed
        )
        == receipt
    )
    committed = await store.committed_proof(
        state["job_id"], state["generation"], state["permit"]
    )
    assert committed is not None
    assert committed["process_zero_receipt_id"] == receipt["process_zero_receipt_id"]
    assert committed["terminal_evidence"] == observed
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_proof", ["restarted_container", "foreign_halt_generation"]
)
async def test_stale_candidate_and_malformed_proof_never_write_zero(db, invalid_proof):
    state = await seeded_stop(db)
    stale = deepcopy(state["frozen"])
    stale["launcher_uid"] = str(uuid4())
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"], state["generation"], state["permit"], stale
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    intent = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    bad = terminal_proof(state["frozen"], intent["frozen_digest"])
    if invalid_proof == "restarted_container":
        bad["containers"][0]["restart_count"] = 1
    else:
        bad["vm_generation"] += 1
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].commit_positive_proof(
            state["job_id"], state["generation"], state["permit"], bad
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm'",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_old_intent_cannot_be_reused_by_new_current_cleanup_parent(db):
    state = await seeded_stop(db)
    store = state["store"]
    await store.admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions "
        "SET completed_at=clock_timestamp(),outcome='failed' WHERE id=$1",
        UUID(state["permit"]["admission_id"]),
    )
    source = "dispatcher_vm_recycle_followup"
    _, pvc_uid, request_id, intent_digest, resource_intent = (
        vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=state["job_id"],
            identity=VMTeardownIdentity(
                state["generation"],
                state["frozen"]["vm_uid"],
                state["frozen"]["pvc_uid"],
            ),
            source=source,
            purge_disk=False,
        )
    )
    admission_id = uuid4()
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
        "VALUES($1,'job',$2,$3,$4,$5,$6)",
        admission_id,
        UUID(state["job_id"]),
        pvc_uid,
        source,
        request_id,
        intent_digest,
    )
    new_parent = {
        "admission_id": str(admission_id),
        "request_id": str(request_id),
        "intent_digest": intent_digest,
        "intent": resource_intent,
    }
    with pytest.raises(VMPreSSHStopConflict, match="stop_intent_parent_changed"):
        await store.current_intent(state["job_id"], state["generation"], new_parent)
    stale_proof = terminal_proof(
        state["frozen"],
        (
            await db.fetchval(
                "SELECT frozen_digest FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
                UUID(state["job_id"]),
            )
        ),
    )
    with pytest.raises(VMPreSSHStopConflict, match="stop_intent_parent_changed"):
        await store.commit_positive_proof(
            state["job_id"], state["generation"], new_parent, stale_proof
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_captured_reservation_revision_and_node_are_immutable(db):
    state = await seeded_stop(db)
    intent = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    reservation = UUID(state["reservation_id"])
    for mutation in (
        "revision=revision+1",
        "node_uid=gen_random_uuid()",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                f"UPDATE vm_resource_reservations SET {mutation} WHERE id=$1",
                reservation,
            )
    assert (
        await state["store"].current_intent(
            state["job_id"], state["generation"], state["permit"]
        )
        == intent
    )


@pytest.mark.asyncio
async def test_new_stop_intent_cannot_borrow_preexisting_generic_zero(db):
    state = await seeded_stop(db)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        UUID(state["job_id"]),
        state["generation"],
    )
    with pytest.raises(VMPreSSHStopConflict, match="preexisting_zero_receipt"):
        await state["store"].admit_intent(
            state["job_id"], state["generation"], state["permit"], state["frozen"]
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_positive_proof_rolls_back_if_zero_insert_fails(db):
    state = await seeded_stop(db)
    intent = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    await db.execute(
        "CREATE FUNCTION public.test_pre_ssh_zero_refusal() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected zero refusal' "
        "USING ERRCODE='23514'; END $$"
    )
    await db.execute(
        "CREATE TRIGGER z_test_pre_ssh_zero_refusal BEFORE INSERT ON "
        "managed_repository_process_zero_receipts FOR EACH ROW "
        "EXECUTE FUNCTION public.test_pre_ssh_zero_refusal()"
    )
    observed = terminal_proof(state["frozen"], intent["frozen_digest"])
    try:
        with pytest.raises(asyncpg.CheckViolationError, match="injected zero refusal"):
            await state["store"].commit_positive_proof(
                state["job_id"], state["generation"], state["permit"], observed
            )
    finally:
        await db.execute(
            "DROP TRIGGER z_test_pre_ssh_zero_refusal ON "
            "managed_repository_process_zero_receipts"
        )
        await db.execute("DROP FUNCTION public.test_pre_ssh_zero_refusal()")
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm'",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await state["store"].commit_positive_proof(
            state["job_id"], state["generation"], state["permit"], observed
        )
    )["process_zero_receipt_id"]


@pytest.mark.asyncio
async def test_positive_stop_ledgers_survive_normal_job_deletion(db):
    state = await seeded_stop(db)
    job_id = UUID(state["job_id"])
    intent = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    terminal = terminal_proof(state["frozen"], intent["frozen_digest"])
    receipt = await state["store"].commit_positive_proof(
        state["job_id"], state["generation"], state["permit"], terminal
    )

    # The physical stop is external to PostgreSQL. Settle its ordinary exact
    # cleanup with a source-shaped absence attestation, not by rewriting the
    # admission or charge directly in this retention test.
    frozen = state["frozen"]
    absent = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": state["job_id"],
        "provision_generation": state["generation"],
        "vm_uid": frozen["vm_uid"],
        "vmi_uid": frozen["vmi_uid"],
        "launcher_uid": frozen["launcher_uid"],
        "pvc_uid": frozen["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
    }
    provisioner = SimpleNamespace(attest_vm_cleanup_stop=AsyncMock(return_value=absent))
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        state["cleanup_permit"],
        outcome="completed",
        provisioner=provisioner,
    )
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "released"
    )
    await db.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job_id)
    # Exercise the existing public VM purge control after retained cleanup.
    # Only its external KubeVirt operation is replaced with a completed probe.
    scope = await db.fetchrow(
        "SELECT controller_configuration->>'namespace' AS namespace,"
        "controller_configuration->'resource_admission'->>'cluster_id' AS cluster_id "
        "FROM vm_creation_retries WHERE owner_kind='job' AND job_id=$1 "
        "AND provision_generation=$2",
        job_id,
        UUID(state["generation"]),
    )
    assert scope is not None and scope["namespace"] and scope["cluster_id"]
    purged = {
        **absent,
        "pvc_disposition": "purged",
        "controller_scope": {"version": 1, **dict(scope)},
    }
    purge_provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(
                state["generation"], frozen["vm_uid"], frozen["pvc_uid"]
            )
        ),
        release_vm_captured=AsyncMock(),
        delete_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
        attest_vm_cleanup_stop=AsyncMock(return_value=purged),
    )
    controls = JobControlOperations(
        SimpleNamespace(
            vm_provisioner=purge_provisioner,
            recovery_store=VMWorkspaceRecoveryStore(db),
        )
    )
    assert await controls.delete_vm(state["job_id"]) == {
        "status": "deleting",
        "job_id": state["job_id"],
    }
    purge_provisioner.delete_vm_captured.assert_awaited_once()
    purge_provisioner.release_vm_captured.assert_not_awaited()
    assert await db.fetchval(
        "SELECT completed_at IS NOT NULL AND outcome='completed' "
        "FROM vm_workspace_cleanup_admissions "
        "WHERE owner_kind='job' AND owner_id=$1 AND source='public_vm_delete'",
        job_id,
    )
    await db.execute(
        "UPDATE jobs SET "
        "context=jsonb_set(context,'{vm,status}','\"deleted\"'::jsonb,true) "
        "WHERE id=$1",
        job_id,
    )
    assert await db.prepare_stateless_job_for_delete(state["job_id"]) is True
    assert await db.delete_job(state["job_id"], prepared_stateless=True) is True
    assert await db.get_job(state["job_id"]) is None

    retained = await db.fetchrow(
        "SELECT i.job_id,i.provision_generation,i.cleanup_admission_id,"
        "i.creation_request_id,i.reservation_id,i.frozen_digest,"
        "p.evidence_digest,p.terminal_evidence,a.owner_kind,a.owner_id,"
        "a.completed_at,a.outcome,r.job_id AS retry_job_id,"
        "r.provision_generation AS retry_generation,v.state AS charge_state,"
        "o.live_job_id,o.deleted_at "
        "FROM vm_pre_ssh_stop_intents i "
        "JOIN vm_pre_ssh_stop_proofs p USING(cleanup_admission_id) "
        "JOIN vm_workspace_cleanup_admissions a ON a.id=i.cleanup_admission_id "
        "JOIN vm_creation_retries r ON r.request_id=i.creation_request_id "
        "JOIN vm_resource_reservations v ON v.id=i.reservation_id "
        "JOIN vm_job_creation_owners o ON o.job_id=i.job_id "
        "WHERE i.job_id=$1 AND i.provision_generation=$2",
        job_id,
        UUID(state["generation"]),
    )
    assert retained is not None
    assert (
        retained["job_id"] == retained["owner_id"] == retained["retry_job_id"] == job_id
    )
    assert (
        retained["provision_generation"]
        == retained["retry_generation"]
        == UUID(state["generation"])
    )
    assert retained["cleanup_admission_id"] == UUID(state["permit"]["admission_id"])
    assert retained["reservation_id"] == UUID(state["reservation_id"])
    assert retained["frozen_digest"] == intent["frozen_digest"]
    assert retained["evidence_digest"] == receipt["evidence_digest"]
    assert json.loads(retained["terminal_evidence"]) == terminal
    assert retained["owner_kind"] == "job"
    assert retained["completed_at"] is not None
    assert retained["outcome"] == "completed"
    assert retained["charge_state"] == "released"
    assert retained["live_job_id"] is None and retained["deleted_at"] is not None
