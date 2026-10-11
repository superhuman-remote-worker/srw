"""A cancelled Job with committed non-issuance retires its exact VM generation."""

import asyncio
from functools import partial
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from orchestrator.database.migrate import discover, run_migrations
from orchestrator.database.postgres import JobVMAuditNotReady
from orchestrator.security import crypto
from orchestrator.services.managed_repository_authority import (
    create_managed_repository,
    ensure_managed_repository_authority,
    revoke_managed_repository_authority,
    revoke_and_delete_managed_repository,
)
from orchestrator.services import thread_retirement
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    VMTeardownResult,
    _VMTeardownProbe,
)
from orchestrator.services.job_controls import JobControlOperations
from orchestrator.services.vm_resource_waiter_maintenance import (
    VMResourceWaiterMaintenance,
)
from orchestrator.services.vm_workspace_policy import vm_needs_release
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    vm_cleanup_request_identity,
    WorkspaceRecoveryControlConflict,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from tests.test_vm_creation_preflight_real_postgres import candidate
from tests.test_vm_creation_preflight_real_postgres import initial_job
from tests.test_vm_creation_configuration import controller
from tests.test_vm_resource_template import shipped_template
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_creation_retry import canonical_request_digest
from shared.worker_queue import (
    claim_worker_batch,
    complete_worker_batch,
    enqueue_worker_batch,
)
from shared.workspace_contract import workspace_runtime_authority_digest
from tests.test_job_terminal_vm_cleanup import controls
from tests.test_vm_resource_whole_store_real_postgres import (
    db as _db_fixture,
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    environment,
    waiter,
)

db = _db_fixture


async def _bind_own_created_repository(
    db, job_id, *, key_generation=3, intent_generation=7
):
    """Install the same durable own pair that root provisioning binds before VM use."""
    repo_name = f"job-{str(job_id)[:8]}"
    remote = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn, conn.transaction():
        intent_id = await conn.fetchval(
            "INSERT INTO managed_repository_creation_intents "
            "(repository_owner,repo_name,authority_kind,authority_id,access_mode,"
            "generation,status,repository_created_at) "
            "VALUES('srw',$1,'job',$2,'write',$3,'created',now()) RETURNING id",
            repo_name,
            job_id,
            intent_generation,
        )
        authority_id = await conn.fetchval(
            "INSERT INTO managed_repository_authorities "
            "(repository_owner,repo_name,authority_kind,authority_id,access_mode,"
            "creation_intent_id,generation,clean_repo_url,public_key,"
            "public_key_fingerprint,private_key_ciphertext,forge_key_id,status,activated_at) "
            "VALUES('srw',$1,'job',$2,'write',$3,$4,$5,'ssh-ed25519 test',"
            "'SHA256:test','v1:test',91,'active',now()) RETURNING id",
            repo_name,
            job_id,
            intent_id,
            key_generation,
            remote,
        )
    assert await db.bind_job_managed_repository(
        str(job_id),
        repo_name=repo_name,
        clean_url=remote,
    )
    return authority_id, intent_id


@pytest.mark.asyncio
async def test_own_created_repository_never_issued_cancel_settles_and_keeps_pair(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    authority_id, intent_id = await _bind_own_created_repository(db, owner)
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    pair = await db.fetchrow(
        "SELECT a.id AS authority_id,a.generation AS key_generation,a.status AS key_status,"
        "i.id AS intent_id,i.generation AS intent_generation,i.status AS intent_status "
        "FROM managed_repository_authorities a JOIN managed_repository_creation_intents i "
        "ON i.id=a.creation_intent_id WHERE a.authority_id=$1",
        owner,
    )
    assert pair["authority_id"] == authority_id
    assert pair["intent_id"] == intent_id
    assert (pair["key_generation"], pair["intent_generation"]) == (3, 7)
    assert (pair["key_status"], pair["intent_status"]) == ("active", "created")


@pytest.mark.asyncio
async def test_normal_repository_provisioning_cancel_resume_cancel_keeps_own_pair(
    db,
    monkeypatch,
    tmp_path,
):
    from tests.test_job_control_operations import _operations

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "M" * 32)
    crypto.reset_cipher_cache()
    resolved = {
        "spec": {
            "timeoutSeconds": 3600,
            "execution": {
                "expert": {
                    "inline": {
                        "runtime": {
                            "config": {
                                "format": "srw/resolved-config-v1",
                                "resolved": {"agent": {}},
                                "policy": {"workspace": {"backend": "vm"}},
                            }
                        }
                    }
                }
            },
        }
    }
    retry, recovery, _ = await never_issued_cancel(
        db,
        monkeypatch,
        execution_resolved=resolved,
    )
    owner = retry["job_id"]
    repo_name = f"job-{str(owner)[:8]}"
    remote = f"http://gitea:3000/srw/{repo_name}.git"
    gitea = SimpleNamespace(
        repository_owner="srw",
        create_repo=AsyncMock(return_value=remote),
        clean_repo_url=lambda name: f"http://gitea:3000/srw/{name}.git",
        ensure_repo_deploy_key=AsyncMock(side_effect=[91, 92]),
        probe_repo_deploy_key=AsyncMock(return_value=True),
        delete_repo_deploy_key=AsyncMock(return_value=True),
        delete_repo=AsyncMock(return_value=True),
    )
    try:
        created_url, intent = await create_managed_repository(
            db,
            gitea,
            repo_name=repo_name,
            authority_kind="job",
            authority_id=str(owner),
            project_id=None,
            access_mode="write",
        )
        authority = await ensure_managed_repository_authority(
            db,
            gitea,
            repo_name=repo_name,
            authority_kind="job",
            authority_id=str(owner),
            access_mode="write",
            creation_intent_id=str(intent["id"]),
        )
        assert created_url == remote and authority["status"] == "active"
        assert await db.bind_job_managed_repository(
            str(owner),
            repo_name=repo_name,
            clean_url=remote,
        )
        assert await revoke_managed_repository_authority(db, gitea, repo_name)
        authority = await ensure_managed_repository_authority(
            db,
            gitea,
            repo_name=repo_name,
            authority_kind="job",
            authority_id=str(owner),
            access_mode="write",
            creation_intent_id=str(intent["id"]),
        )
        assert authority["generation"] == 2
        assert intent["generation"] == 1
        archive, _ = actual_archive(db, recovery, monkeypatch)
        assert await controls(
            store=db, archive=archive
        ).wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        receipt = await db.fetchrow(
            "SELECT * FROM vm_job_repository_settlement_receipts "
            "WHERE job_id=$1 AND provision_generation=$2",
            owner,
            retry["provision_generation"],
        )
        assert receipt["authority_id"] == authority["id"]
        assert receipt["creation_intent_id"] == intent["id"]
        assert (receipt["key_generation"], receipt["intent_generation"]) == (2, 1)
        assert (receipt["repository_owner"], receipt["repo_name"]) == ("srw", repo_name)
        assert receipt["forge_key_id"] == 92 and receipt["project_id"] is None
        # Exercise the public control path, including creation classification
        # and the queue-first stale-workspace CAS, rather than bypassing both.
        operations = _operations(tmp_path, store=db)
        operations.dependencies.resume_missing_workspace.return_value = "vm"
        before_retry = dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                retry["request_id"],
            )
        )
        result = await operations.resume_job_internal(
            str(owner),
            user=None,
            job=await db.get_job(str(owner)),
        )
        assert result["status"] == "queued"
        resumed = await db.get_job(str(owner))
        assert resumed["status"] == "paused"
        resumed_context = json.loads(resumed["context"])
        assert "vm" not in resumed_context
        assert "_vm_creation_pending" not in resumed_context
        assert resumed_context["last_vm"]["status"] == "deleted"
        assert (
            dict(
                await db.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                    retry["request_id"],
                )
            )
            == before_retry
        )
        assert (
            await db.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1",
                owner,
            )
            == "done"
        )
        policy, _, _, _ = await environment(db)
        second, recovery, _ = await normal_never_issued_generation(
            db,
            monkeypatch,
            owner=owner,
            policy=policy,
        )
        archive, _ = actual_archive(db, recovery, monkeypatch)
        assert await controls(
            store=db, archive=archive
        ).wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        assert second["provision_generation"] != retry["provision_generation"]
        assert (
            await db.fetchval(
                "SELECT count(*) FROM managed_repository_authorities WHERE id=$1 "
                "AND status='active' AND creation_intent_id=$2",
                authority["id"],
                intent["id"],
            )
            == 1
        )
        assert await revoke_and_delete_managed_repository(db, gitea, repo_name)
        assert await db.prepare_stateless_job_for_delete(str(owner))
        assert await db.delete_job(str(owner), prepared_stateless=True)
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
                owner,
            )
            == 2
        )
        retained = await db.fetchrow(
            "SELECT authority_id,creation_intent_id,key_generation,intent_generation "
            "FROM vm_job_repository_settlement_receipts WHERE job_id=$1 "
            "AND provision_generation=$2",
            owner,
            retry["provision_generation"],
        )
        assert tuple(retained) == (authority["id"], intent["id"], 2, 1)
        packet = await db.fetchval(
            "SELECT evidence FROM vm_job_creation_terminal_packets WHERE request_id=$1",
            retry["request_id"],
        )
        if isinstance(packet, str):
            packet = json.loads(packet)
        assert packet["repository_pair"]["authority_id"] == str(authority["id"])
        assert packet["repository_pair"]["creation_intent_id"] == str(intent["id"])
        assert (
            packet["repository_pair"]["key_generation"],
            packet["repository_pair"]["intent_generation"],
        ) == (2, 1)
        assert packet["repository_pair"]["forge_key_id"] == 92
    finally:
        crypto.reset_cipher_cache()


@pytest.mark.asyncio
async def test_final_delete_retains_never_issued_vm_source_and_terminal_receipt(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    result = await db.delete_job(
        str(owner),
        prepared_stateless=True,
        deletion_reason="authorized_api_delete",
        return_claim_state=True,
    )
    assert result["deleted"] is True
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1",
            owner,
        )
        == 1
    )
    audit = await db.fetchrow(
        "SELECT live_job_id,deleted_at,deletion_receipt FROM vm_job_creation_owners "
        "WHERE job_id=$1",
        owner,
    )
    assert audit["live_job_id"] is None and audit["deleted_at"]
    packet = json.loads(audit["deletion_receipt"])
    assert packet["job_id"] == str(owner)
    assert packet["generations"][0]["request_id"] == str(retry["request_id"])
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_creation_retries SET reason='changed' WHERE request_id=$1",
            retry["request_id"],
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO jobs(id,description,status) VALUES($1,'reused','created')",
            owner,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,source,request_id,intent_digest) "
            "VALUES($1,'job',$2,'late_cleanup',$3,'sha256:late')",
            uuid4(),
            owner,
            uuid4(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("late_delivery", ["worker_bundle", "execution_attempt"])
async def test_late_delivery_after_settlement_blocks_logical_final_delete(
    db,
    monkeypatch,
    late_delivery,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    if late_delivery == "worker_bundle":
        await db.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt,"
            "bundle_authorized_at,authority_digest) "
            "VALUES($1,992,1,now(),'sha256:late-delivery')",
            owner,
        )
    else:
        await db.execute(
            "INSERT INTO srw_execution_attempts(execution_id,attempt,pod_name,phase) "
            "VALUES($1,1,'late-execution-pod','Running')",
            retry["execution_id"],
        )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    with pytest.raises(JobVMAuditNotReady):
        await db.delete_job(str(owner), prepared_stateless=True)
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 1
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            owner,
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unattributed_delivery", [None, "worker_bundle", "execution_attempt"]
)
async def test_logical_cancel_then_charged_physical_successor_final_disposition(
    db,
    monkeypatch,
    unattributed_delivery,
):
    first, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = first["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.queue_stateless_job_for_resume(
        str(owner),
        expected_status="cancelled",
    )

    policy, inventory, _, _ = await environment(db)
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document)
    )
    from vm_controller import controller as controller_settings

    monkeypatch.setattr(controller_settings, "VM_NAMESPACE", "workers")
    monkeypatch.setattr(controller_settings, "VM_STORAGE_CLASS", "local")
    monkeypatch.setattr(controller_settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(controller_settings, "VM_TOLERATIONS", [])
    request, fresh = candidate(owner)
    preflight = VMCreationPreflightStore(db)
    await preflight.begin(job_id=str(owner), request=request, fresh_context=fresh)
    claim = (await preflight.claim_due(limit=1))[0]
    resolver = controller()
    resolver.template_text = shipped_template()
    second = await preflight.complete_resolution(
        claim,
        resolve_creation_configuration(resolver, claim["request"]),
    )
    assert second["provision_generation"] != first["provision_generation"]
    admitted = await policy.admit(request_id=str(second["request_id"]))
    assert admitted["action"] == "admitted"
    creation_claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(second["request_id"]),
        claim_token=str(creation_claim["claim_token"]),
        observed={
            "job_id": str(owner),
            "provision_generation": str(second["provision_generation"]),
            "request_digest": second["request_digest"],
            "controller_configuration_digest": second[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        second["request_id"],
        vm_uid,
        pvc_uid,
    )
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=$1",
        authorized["admission_id"],
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]),
        vm_uid,
        vmi_uid,
        launcher_uid,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb),"
        "config_override=jsonb_build_object('workspace',jsonb_build_object('backend','vm')) "
        "WHERE id=$1",
        owner,
        json.dumps(
            {
                "provision_generation": str(second["provision_generation"]),
                "vm_uid": str(vm_uid),
                "vmi_uid": str(vmi_uid),
                "active_pod_uid": str(launcher_uid),
                "rootdisk_pvc_uid": str(pvc_uid),
                "status": "ready",
                "provisioner": "vm",
                "identity_authenticated": True,
                "identity_provision_generation": str(second["provision_generation"]),
                "ssh_ready_source": "provisioner_probe",
                "ssh_host": "10.42.0.91",
                "ssh_port": 22,
                "ssh_host_key_fingerprint": "SHA256:test",
            }
        ),
    )
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=owner)
    worker = await claim_worker_batch(db, pod_name="normal-vm-worker")
    assert worker is not None and worker.unit_id == owner
    job = await db.fetchrow("SELECT * FROM jobs WHERE id=$1", owner)
    digest = workspace_runtime_authority_digest(dict(job), vm_mode="same-cluster")
    assert digest is not None
    async with db.acquire() as conn:
        assert not await VMWorkspaceRecoveryStore(db).record_bundle_authorized(
            conn,
            job_id=owner,
            lease_token=worker.lease_token,
            authority_digest="stale-runtime-digest",
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_worker_delivery_bindings WHERE job_id=$1",
            owner,
        )
        == 0
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_job_worker_delivery_bindings "
            "(job_id,lease_token,request_id,provision_generation,vm_uid,pvc_uid,authority_digest) "
            "VALUES($1,$2,$3,$4,$5,$6,$7)",
            owner,
            worker.lease_token,
            first["request_id"],
            first["provision_generation"],
            vm_uid,
            pvc_uid,
            digest,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_job_worker_delivery_bindings "
            "(job_id,lease_token,request_id,provision_generation,vm_uid,pvc_uid,authority_digest) "
            "VALUES($1,$2,$3,$4,$5,$6,$7)",
            owner,
            worker.lease_token,
            second["request_id"],
            second["provision_generation"],
            vm_uid,
            uuid4(),
            digest,
        )
    async with db.acquire() as conn:
        assert await VMWorkspaceRecoveryStore(db).record_bundle_authorized(
            conn,
            job_id=owner,
            lease_token=worker.lease_token,
            authority_digest=digest,
        )
        assert (
            await complete_worker_batch(
                conn,
                unit_id=owner,
                lease_token=worker.lease_token,
                consumed_seq=worker.unit.input_seq,
            )
            is not None
        )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        owner,
        str(second["provision_generation"]),
    )
    identity = VMTeardownIdentity(
        provision_generation=str(second["provision_generation"]),
        vm_uid=str(vm_uid),
        rootdisk_pvc_uid=str(pvc_uid),
    )
    proof = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": str(owner),
        "provision_generation": str(second["provision_generation"]),
        "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid),
        "pvc_uid": str(pvc_uid),
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
        release_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
        attest_vm_cleanup_stop=AsyncMock(return_value=proof),
    )
    vm_controls = JobControlOperations(
        SimpleNamespace(
            vm_provisioner=provisioner,
            recovery_store=VMWorkspaceRecoveryStore(db),
        )
    )
    assert (await vm_controls.delete_vm(str(owner)))["status"] == "deleting"
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'::jsonb,true) WHERE id=$1",
        owner,
    )
    assert (await db.cancel_stateless_job(str(owner)))[0]
    physical = await db.fetchval(
        "SELECT vm_job_terminal_packet_evidence($1)",
        second["request_id"],
    )
    assert physical is not None
    if isinstance(physical, str):
        physical = json.loads(physical)
    assert physical["kind"] == "physical_stop"
    assert (
        await db.fetchval(
            "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
            owner,
            str(first["provision_generation"]),
        )
        is True
    )
    if unattributed_delivery == "worker_bundle":
        await db.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt,"
            "bundle_authorized_at,authority_digest) "
            "VALUES($1,993,1,now(),'sha256:unattributed')",
            owner,
        )
    elif unattributed_delivery == "execution_attempt":
        await db.execute(
            "INSERT INTO srw_execution_attempts(execution_id,attempt,pod_name,phase) "
            "VALUES($1,2,'unattributed-pod','Running')",
            second["execution_id"],
        )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    lease_id = await db.fetchval(
        "INSERT INTO vm_idle_access_leases "
        "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
        "acquired_at,expires_at,max_expires_at) VALUES "
        "('job',$1,$2,$3,'ide',$4,clock_timestamp()-interval '5 minutes',"
        "clock_timestamp()-interval '3 minutes',"
        "clock_timestamp()-interval '1 minute') RETURNING id",
        owner,
        second["provision_generation"],
        vm_uid,
        f"{uuid4()}:{uuid4()}",
    )
    if unattributed_delivery is not None:
        with pytest.raises(JobVMAuditNotReady):
            await db.delete_job(str(owner), prepared_stateless=True)
        assert (
            await db.fetchval(
                "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1",
                lease_id,
            )
            is True
        )
        assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 1
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
                owner,
            )
            == 0
        )
        return
    assert await db.delete_job(str(owner), prepared_stateless=True)
    assert (
        await db.fetchval(
            "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        is True
    )
    packets = await db.fetch(
        "SELECT request_id,terminal_kind FROM vm_job_creation_terminal_packets "
        "WHERE job_id=$1",
        owner,
    )
    assert {row["request_id"]: row["terminal_kind"] for row in packets} == {
        first["request_id"]: "never_issued",
        second["request_id"]: "physical_stop",
    }
    owner_receipt = await db.fetchval(
        "SELECT deletion_receipt FROM vm_job_creation_owners WHERE job_id=$1",
        owner,
    )
    if isinstance(owner_receipt, str):
        owner_receipt = json.loads(owner_receipt)
    assert len(owner_receipt["worker_delivery_bindings"]) == 1
    assert any(
        attempt["lease_token"] == worker.lease_token
        and attempt["authority_digest"] == digest
        and attempt["bundle_authorized_at"] is not None
        for attempt in owner_receipt["worker_attempts"]
    )
    assert owner_receipt["worker_delivery_bindings"][0]["request_id"] == str(
        second["request_id"]
    )
    assert (
        owner_receipt["worker_delivery_bindings"][0]["lease_token"]
        == worker.lease_token
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_job_worker_delivery_bindings SET authority_digest='tampered' "
            "WHERE job_id=$1 AND lease_token=$2",
            owner,
            worker.lease_token,
        )
    physical_packet = await db.fetchval(
        "SELECT evidence FROM vm_job_creation_terminal_packets WHERE request_id=$1",
        second["request_id"],
    )
    if isinstance(physical_packet, str):
        physical_packet = json.loads(physical_packet)
    assert len(physical_packet["process_zero_receipt_ids"]) == 1
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE managed_repository_process_zero_receipts "
            "SET runtime_incarnation=$2 WHERE id=$1",
            UUID(physical_packet["process_zero_receipt_ids"][0]),
            str(uuid4()),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
            UUID(physical_packet["process_zero_receipt_ids"][0]),
        )


@pytest.mark.asyncio
async def test_retired_logical_cleanup_parent_cannot_be_reparented(db, monkeypatch):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    assert await db.delete_job(str(owner), prepared_stateless=True)
    other = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status) VALUES($1,'other','created')",
        other,
    )
    parent_id = await db.fetchval(
        "SELECT cleanup_admission_id FROM vm_job_creation_terminal_packets "
        "WHERE request_id=$1",
        retry["request_id"],
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET owner_id=$2 WHERE id=$1",
            parent_id,
            other,
        )
    assert (
        await db.fetchval(
            "SELECT owner_id FROM vm_workspace_cleanup_admissions WHERE id=$1",
            parent_id,
        )
        == owner
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_waiters SET job_id=$2 WHERE request_id=$1",
            retry["request_id"],
            other,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE srw_execution_specs SET work_id=$2 WHERE id=$1",
            retry["execution_id"],
            other,
        )


@pytest.mark.asyncio
async def test_final_delete_rollback_keeps_live_audit_link_and_no_packets(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    child = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,parent_job_id,description,status) "
        "VALUES($1,$2,'blocking child','created')",
        child,
        owner,
    )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await db.delete_job(str(owner), prepared_stateless=True)
    audit = await db.fetchrow(
        "SELECT live_job_id,deleted_at,deletion_receipt FROM vm_job_creation_owners "
        "WHERE job_id=$1",
        owner,
    )
    assert audit["live_job_id"] == owner
    assert audit["deleted_at"] is None and audit["deletion_receipt"] is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            owner,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_final_delete_serializes_losing_cleanup_writer_and_freezes_packet(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.prepare_stateless_job_for_delete(str(owner))

    # Pause the real Delete inside its final jobs DELETE, after it has taken
    # the Job row lock and captured the terminal packet. The competing child
    # INSERT must wait for that lock, then see the retired owner after commit.
    barrier_key = 927362875
    await db.execute(
        "CREATE FUNCTION public.m1_test_delete_barrier() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "PERFORM pg_advisory_xact_lock(927362875::bigint); RETURN OLD; END $$"
    )
    await db.execute(
        "CREATE TRIGGER zz_m1_test_delete_barrier BEFORE DELETE ON public.jobs "
        "FOR EACH ROW EXECUTE FUNCTION public.m1_test_delete_barrier()"
    )
    try:
        async with db.acquire() as holder:
            await holder.execute("SELECT pg_advisory_lock($1::bigint)", barrier_key)
            try:
                delete_task = asyncio.create_task(
                    db.delete_job(str(owner), prepared_stateless=True)
                )
                for _ in range(100):
                    if await db.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_locks "
                        "WHERE locktype='advisory' AND objid=$1 "
                        "AND granted=false)",
                        barrier_key,
                    ):
                        break
                    assert not delete_task.done()
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("final Delete never reached the Job-row-held barrier")
                async with db.acquire() as writer:
                    writer_pid = await writer.fetchval("SELECT pg_backend_pid()")
                    writer_task = asyncio.create_task(
                        writer.execute(
                            "INSERT INTO vm_workspace_cleanup_admissions "
                            "(id,owner_kind,owner_id,source,request_id,intent_digest) "
                            "VALUES($1,'job',$2,'late_cleanup',$3,'sha256:late')",
                            uuid4(),
                            owner,
                            uuid4(),
                        )
                    )
                    for _ in range(100):
                        if await db.fetchval(
                            "SELECT wait_event_type='Lock' FROM pg_stat_activity "
                            "WHERE pid=$1",
                            writer_pid,
                        ):
                            break
                        assert not writer_task.done()
                        await asyncio.sleep(0.05)
                    else:
                        pytest.fail("cleanup INSERT never waited on the locked Job")
                    await holder.execute(
                        "SELECT pg_advisory_unlock($1::bigint)",
                        barrier_key,
                    )
                    assert await asyncio.wait_for(delete_task, timeout=5) is True
                    with pytest.raises(asyncpg.CheckViolationError):
                        await asyncio.wait_for(writer_task, timeout=5)
            finally:
                await holder.execute(
                    "SELECT pg_advisory_unlock($1::bigint)", barrier_key
                )
    finally:
        await db.execute("DROP TRIGGER zz_m1_test_delete_barrier ON public.jobs")
        await db.execute("DROP FUNCTION public.m1_test_delete_barrier()")
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND source='late_cleanup'",
            owner,
        )
        == 0
    )
    packet = await db.fetchval(
        "SELECT evidence FROM vm_job_creation_terminal_packets WHERE request_id=$1",
        retry["request_id"],
    )
    if isinstance(packet, str):
        packet = json.loads(packet)
    assert packet["kind"] == "never_issued"
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_job_creation_terminal_packets "
            "SET evidence='{}'::jsonb WHERE request_id=$1",
            retry["request_id"],
        )
    after = await db.fetchval(
        "SELECT evidence FROM vm_job_creation_terminal_packets WHERE request_id=$1",
        retry["request_id"],
    )
    assert (json.loads(after) if isinstance(after, str) else after) == packet


@pytest.mark.asyncio
async def test_repository_writer_wins_job_lock_then_final_delete_holds(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.prepare_stateless_job_for_delete(str(owner))
    repo_name = f"job-{str(owner)[:8]}"
    async with db.acquire() as writer, writer.transaction():
        await writer.execute(
            "INSERT INTO managed_repository_creation_intents "
            "(repository_owner,repo_name,authority_kind,authority_id,access_mode,"
            "generation,status,repository_created_at) "
            "VALUES('srw',$1,'job',$2,'write',1,'created',now())",
            repo_name,
            owner,
        )
        delete_task = asyncio.create_task(
            db.delete_job(str(owner), prepared_stateless=True)
        )
        await asyncio.sleep(0.1)
        assert not delete_task.done()
    with pytest.raises(
        asyncpg.CheckViolationError,
        match="Managed repository authority must be contained first",
    ):
        await asyncio.wait_for(delete_task, timeout=5)
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 1
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            owner,
        )
        == 0
    )
    audit = await db.fetchrow(
        "SELECT live_job_id,deleted_at FROM vm_job_creation_owners WHERE job_id=$1",
        owner,
    )
    assert audit["live_job_id"] == owner and audit["deleted_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "contradiction", ["unpaired", "wrong_remote", "bundle_delivered"]
)
async def test_repository_pair_needs_exact_undelivered_history(
    db,
    monkeypatch,
    contradiction,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    authority_id, _ = await _bind_own_created_repository(db, owner)
    if contradiction == "unpaired":
        await db.execute(
            "UPDATE managed_repository_authorities SET creation_intent_id=NULL "
            "WHERE id=$1",
            authority_id,
        )
    elif contradiction == "wrong_remote":
        await db.execute(
            "UPDATE managed_repository_authorities SET clean_repo_url=$2 WHERE id=$1",
            authority_id,
            "http://gitea:3000/srw/other.git",
        )
    else:
        await db.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt,"
            "bundle_authorized_at,authority_digest) "
            "VALUES($1,991,1,now(),'sha256:delivered')",
            owner,
        )
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert (
        await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        is False
    )
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 1


@pytest.mark.asyncio
async def test_vm_job_without_source_history_stays_live_on_final_delete(db):
    owner = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,context) VALUES "
        "($1,'legacy unknown VM','cancelled',"
        "jsonb_build_object('vm',jsonb_build_object('status','deleted'))) ",
        owner,
    )
    with pytest.raises(JobVMAuditNotReady):
        await db.delete_job(str(owner))
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", owner) == 1


@pytest.mark.asyncio
async def test_0325_upgrade_preserves_applied_ledger_and_thread_predicate(
    pg_dsn,  # noqa: F811 - imported fixture
    tmp_path: Path,
):
    """A clean 0325 database gains only the forward Job source at 0326."""
    migration_dir = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    through_0325 = tmp_path / "through-0325"
    through_0325.mkdir()
    through_0326 = tmp_path / "through-0326"
    through_0326.mkdir()
    # Stop at 0326 so later heads do not change this upgrade's exact delta.
    for path in discover(migration_dir):
        version = path.name.split("_", 1)[0]
        if version > "0326":
            break
        if version != "0326":
            (through_0325 / path.name).write_bytes(path.read_bytes())
        (through_0326 / path.name).write_bytes(path.read_bytes())

    database = f"test_job_0326_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(pg_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()
    dsn = pg_dsn.rsplit("/", 1)[0] + "/" + database
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        await run_migrations(pool, through_0325)
        async with pool.acquire() as conn:
            applied = dict(
                await conn.fetch("SELECT filename,checksum FROM schema_migrations")
            )
            assert any(name.startswith("0325_") for name in applied)
            assert not any(name.startswith("0326_") for name in applied)
            thread_before = await conn.fetchval(
                "SELECT pg_get_functiondef("
                "'public.thread_vm_creation_never_issued_source(uuid,text)'::regprocedure)"
            )
            pinned_before = await conn.fetchval(
                "SELECT pg_get_functiondef("
                "'public.pinned_vm_actuator_request_valid(public.threads,jsonb,boolean)'::regprocedure)"
            )
            generation = str(uuid4())
            thread_id = uuid4()
            thread_result_before = await conn.fetchval(
                "SELECT public.managed_repository_process_zero_receipt_exists("
                "'thread',$1,'vm','vm',$2)",
                thread_id,
                generation,
            )
            assert thread_result_before is False

        await run_migrations(pool, through_0326)
        async with pool.acquire() as conn:
            after = dict(
                await conn.fetch("SELECT filename,checksum FROM schema_migrations")
            )
            assert {name: after[name] for name in applied} == applied
            assert len(after) == len(applied) + 1
            assert "0326_job_never_issued_vm_terminal.sql" in after
            assert (
                await conn.fetchval(
                    "SELECT pg_get_functiondef("
                    "'public.thread_vm_creation_never_issued_source(uuid,text)'::regprocedure)"
                )
                == thread_before
            )
            assert (
                await conn.fetchval(
                    "SELECT pg_get_functiondef("
                    "'public.pinned_vm_actuator_request_valid(public.threads,jsonb,boolean)'::regprocedure)"
                )
                == pinned_before
            )
            assert (
                await conn.fetchval(
                    "SELECT public.managed_repository_process_zero_receipt_exists("
                    "'thread',$1,'vm','vm',$2)",
                    thread_id,
                    generation,
                )
                == thread_result_before
            )
            for name in (
                "evidence",
                "terminal_source",
                "predecessors",
                "source",
            ):
                assert (
                    await conn.fetchval(
                        "SELECT to_regprocedure($1)",
                        f"public.job_vm_creation_never_issued_{name}(uuid,text)",
                    )
                    is not None
                )
    finally:
        await pool.close()
        admin = await asyncpg.connect(pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        finally:
            await admin.close()


async def never_issued_cancel(
    db,
    monkeypatch,
    *,
    existing_parent=False,
    execution_resolved=None,
):
    """Use real admission, Cancel, and retry settlement with no create effect."""
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(
        db,
        policy,
        inventory,
        lane="stateless",
        execution_resolved=execution_resolved,
    )
    owner = retry["job_id"]
    generation = retry["provision_generation"]
    row = await db.fetchrow(
        "SELECT context FROM jobs WHERE id=$1",
        owner,
    )
    context = json.loads(row["context"])
    assert context["_vm_creation_pending"] == str(retry["request_id"])
    vm = context["vm"]
    preflight_request = deepcopy(retry["canonical_request"])
    preflight_request["network_tier"] = " restricted "
    assert preflight_request != retry["canonical_request"]
    assert vm["creation_request"]["request"] == retry["canonical_request"]
    vm.update(
        {
            "status": "waiting_creation_configuration",
            "provision_attempts": 0,
            "identity_authenticated": False,
            "identity_provision_generation": None,
            "vm_uid": None,
            "rootdisk_pvc_uid": None,
            "preparation_request": None,
            "workspace_storage": None,
            "creation_preflight": {
                "version": 1,
                "request_id": str(retry["request_id"]),
                "job_id": str(owner),
                "request": preflight_request,
                "request_digest": canonical_request_digest(preflight_request),
                "state": "admitted",
                "revision": 1,
                "attempt": 0,
                "execution_id": str(retry["execution_id"]),
                "execution_revision": retry["execution_revision"],
                "execution_generation": retry["execution_generation"],
                "admission_deadline": retry["admission_deadline"].isoformat(),
                "expected_pvc_uid": None,
            },
        }
    )
    context["vm"] = vm
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(context),
    )
    await db.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state) "
        "VALUES($1,'worker_batch','queued') ON CONFLICT (unit_id) DO NOTHING",
        owner,
    )
    cancelled, _ = await db.cancel_stateless_job(str(owner))
    assert cancelled is True
    assert (
        await VMResourceWaiterMaintenance(policy).maintain(
            request_id=str(retry["request_id"]),
        )
    )["action"] == "cancelled"
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(retry["request_id"]),
        )
    )["settled"] is True
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            retry["request_id"],
        )
        == "cancelled"
    )
    assert (
        await db.fetchval(
            "SELECT reason FROM vm_resource_waiters WHERE request_id=$1",
            retry["request_id"],
        )
        == "job_cancelled"
    )
    recovery = VMWorkspaceRecoveryStore(db)
    parent_id = None
    if existing_parent:
        identity = SimpleNamespace(
            provision_generation=str(generation),
            vm_uid=None,
            rootdisk_pvc_uid=None,
        )
        _, _, request_id, digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
        )
        parent = await recovery.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner,
            pvc_uid=None,
            request_id=request_id,
            source="job_terminal_vm_release",
            intent_digest=digest,
        )
        assert parent.allowed
        parent_id = parent.admission_id
    return retry, recovery, parent_id


async def normal_never_issued_generation(
    db,
    monkeypatch,
    *,
    owner,
    policy,
    existing_parent=False,
):
    """Admit, resolve and cancel one generation through the ordinary stores."""
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document)
    )
    from vm_controller import controller as controller_settings

    monkeypatch.setattr(controller_settings, "VM_NAMESPACE", "workers")
    monkeypatch.setattr(controller_settings, "VM_STORAGE_CLASS", "local")
    monkeypatch.setattr(controller_settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(controller_settings, "VM_TOLERATIONS", [])
    preflight = VMCreationPreflightStore(db)
    request, fresh = candidate(owner)
    await preflight.begin(job_id=str(owner), request=request, fresh_context=fresh)
    claim = (await preflight.claim_due(limit=1))[0]
    resolver = controller()
    resolver.template_text = shipped_template()
    resolved = resolve_creation_configuration(resolver, claim["request"])
    retry = await preflight.complete_resolution(claim, resolved)
    assert retry["execution_id"] is not None
    assert retry["provision_generation"] == UUID(fresh["provision_generation"])
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            retry["request_id"],
        )
        == "waiting"
    )
    assert (await db.cancel_stateless_job(str(owner)))[0]
    assert (
        await VMResourceWaiterMaintenance(policy).maintain(
            request_id=str(retry["request_id"]),
        )
    )["action"] == "cancelled"
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(retry["request_id"]),
        )
    )["settled"]
    recovery = VMWorkspaceRecoveryStore(db)
    parent_id = None
    if existing_parent:
        identity = SimpleNamespace(
            provision_generation=fresh["provision_generation"],
            vm_uid=None,
            rootdisk_pvc_uid=None,
        )
        _, _, request_id, digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
        )
        parent = await recovery.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner,
            pvc_uid=None,
            request_id=request_id,
            source="job_terminal_vm_release",
            intent_digest=digest,
        )
        assert parent.allowed
        parent_id = parent.admission_id
    return retry, recovery, parent_id


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_parent", [False, True], ids=["fresh", "open-parent"])
async def test_repeated_never_issued_job_cancel_settles_exact_successor(
    db,
    monkeypatch,
    existing_parent,
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    policy, _, _, _ = await environment(db)
    owner = await initial_job(db)
    generations = []
    for index in range(2 if existing_parent else 3):
        retry, recovery, parent_id = await normal_never_issued_generation(
            db,
            monkeypatch,
            owner=owner,
            policy=policy,
            existing_parent=existing_parent and index == 1,
        )
        archive, vm = actual_archive(db, recovery, monkeypatch)
        assert await controls(
            store=db, archive=archive
        ).wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        state = json.loads(
            await db.fetchval(
                "SELECT context FROM jobs WHERE id=$1",
                owner,
            )
        )
        assert state["vm"]["status"] == "deleted"
        assert "_stateless_cancel_cleanup_pending" not in state
        for historical in [*generations, str(retry["provision_generation"])]:
            assert await db.fetchval(
                "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
                owner,
                historical,
            )
        parent = await db.fetchrow(
            "SELECT id,completed_at,outcome,pvc_uid FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND source='job_terminal_vm_release' "
            "AND request_id=$2",
            owner,
            vm_cleanup_request_identity(
                owner_kind="job",
                owner_id=owner,
                identity=SimpleNamespace(
                    provision_generation=str(retry["provision_generation"]),
                    vm_uid=None,
                    rootdisk_pvc_uid=None,
                ),
                source="job_terminal_vm_release",
                purge_disk=True,
            )[2],
        )
        assert parent["id"] == (parent_id or parent["id"])
        assert parent["completed_at"] and parent["outcome"] == "completed"
        assert parent["pvc_uid"] is None
        vm.release_vm_captured.assert_not_awaited()
        vm._probe_vm_teardown_identity.assert_not_awaited()
        generations.append(str(retry["provision_generation"]))
        if index < (1 if existing_parent else 2):
            assert await db.queue_stateless_job_for_resume(
                str(owner),
                expected_status="cancelled",
            )
    assert len(set(generations)) == len(generations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_last_vm",
        "wrong_last_generation",
        "last_vm_issued",
        "last_vm_prepared",
        "last_vm_pvc",
        "last_vm_snapshot",
        "prior_waiter_reason",
        "prior_parent_digest",
        "prior_retry_issued",
        "prior_effect",
        "prior_charge",
        "unknown_terminal_parent",
        "current_pending_marker",
        "current_preflight_digest",
        "current_waiter_reason",
    ],
)
async def test_repeated_cancel_refuses_unproven_predecessor_or_current_source(
    db,
    monkeypatch,
    mutation,
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    policy, _, _, _ = await environment(db)
    owner = await initial_job(db)
    first, recovery, _ = await normal_never_issued_generation(
        db,
        monkeypatch,
        owner=owner,
        policy=policy,
    )
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    assert await db.queue_stateless_job_for_resume(
        str(owner),
        expected_status="cancelled",
    )
    second, recovery, parent_id = await normal_never_issued_generation(
        db,
        monkeypatch,
        owner=owner,
        policy=policy,
        existing_parent=True,
    )
    assert parent_id is not None
    if mutation in {
        "missing_last_vm",
        "wrong_last_generation",
        "last_vm_issued",
        "last_vm_prepared",
        "last_vm_pvc",
        "last_vm_snapshot",
        "current_pending_marker",
        "current_preflight_digest",
    }:
        path, value = {
            "missing_last_vm": (["last_vm"], None),
            "wrong_last_generation": (
                ["last_vm", "provision_generation"],
                str(uuid4()),
            ),
            "last_vm_issued": (["last_vm", "vm_uid"], str(uuid4())),
            "last_vm_prepared": (
                ["last_vm", "preparation_request"],
                {"kind": "prepared"},
            ),
            "last_vm_pvc": (["last_vm", "rootdisk_pvc_uid"], str(uuid4())),
            "last_vm_snapshot": (
                ["last_vm", "creation_request", "request_digest"],
                "sha256:" + "a" * 64,
            ),
            "current_pending_marker": (["_vm_creation_pending"], str(uuid4())),
            "current_preflight_digest": (
                ["vm", "creation_preflight", "request_digest"],
                "sha256:" + "a" * 64,
            ),
        }[mutation]
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],$3::jsonb) WHERE id=$1",
            owner,
            path,
            json.dumps(value),
        )
    elif mutation in {"prior_waiter_reason", "current_waiter_reason"}:
        await db.execute(
            "UPDATE vm_resource_waiters SET reason='generation_changed' WHERE request_id=$1",
            first["request_id"]
            if mutation == "prior_waiter_reason"
            else second["request_id"],
        )
    elif mutation == "prior_parent_digest":
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 "
            "WHERE owner_kind='job' AND owner_id=$1 AND source='job_terminal_vm_release' "
            "AND completed_at IS NOT NULL",
            owner,
            "sha256:" + "b" * 64,
        )
    elif mutation == "unknown_terminal_parent":
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,"
            "completed_at,outcome) VALUES($1,'job',$2,NULL,'job_terminal_vm_release',"
            "$3,$4,clock_timestamp(),'completed')",
            uuid4(),
            owner,
            uuid4(),
            "sha256:" + "c" * 64,
        )
    else:
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role = replica")
            if mutation == "prior_retry_issued":
                await conn.execute(
                    "UPDATE vm_creation_retries SET observed_vm_uid=$2 WHERE request_id=$1",
                    first["request_id"],
                    uuid4(),
                )
            elif mutation == "prior_effect":
                await conn.execute(
                    "INSERT INTO vm_creation_effects "
                    "(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,"
                    "carrier_namespace,carrier_intent) "
                    "VALUES($1,$2,1,'rootdisk',$3,'agent-vms','{}'::jsonb)",
                    uuid4(),
                    first["request_id"],
                    uuid4(),
                )
            elif mutation == "prior_charge":
                await conn.execute(
                    "INSERT INTO vm_resource_reservations "
                    "(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,"
                    "cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest,"
                    "state,resource_version,ephemeral_storage_bytes,tun_devices,vhost_net_devices) "
                    "SELECT $2,request_id,1,cluster_id,policy_digest,$3,'node-a',"
                    "cpu_millicores,memory_bytes,kvm_devices,$4,$5,'reserved',2,"
                    "ephemeral_storage_bytes,tun_devices,vhost_net_devices "
                    "FROM vm_resource_waiters WHERE request_id=$1",
                    first["request_id"],
                    uuid4(),
                    uuid4(),
                    uuid4(),
                    "sha256:" + "a" * 64,
                )
    with pytest.raises((ResourceAdmissionError, VMCreationRetryConflict)):
        await recovery.settle_never_issued_job_terminal(
            str(owner),
            provision_generation=str(second["provision_generation"]),
        )
    parent = await db.fetchrow(
        "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
        parent_id,
    )
    assert parent["completed_at"] is None and parent["outcome"] is None
    state = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    assert state["vm"]["status"] != "deleted"
    assert state["_stateless_cancel_cleanup_pending"] is True


@pytest.mark.asyncio
async def test_third_never_issued_cancel_requires_every_older_logical_parent(
    db,
    monkeypatch,
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    policy, _, _, _ = await environment(db)
    owner = await initial_job(db)
    history = []
    for _ in range(2):
        retry, recovery, _ = await normal_never_issued_generation(
            db,
            monkeypatch,
            owner=owner,
            policy=policy,
        )
        archive, _ = actual_archive(db, recovery, monkeypatch)
        assert await controls(
            store=db, archive=archive
        ).wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        history.append(retry)
        assert await db.queue_stateless_job_for_resume(
            str(owner),
            expected_status="cancelled",
        )
    third, recovery, parent_id = await normal_never_issued_generation(
        db,
        monkeypatch,
        owner=owner,
        policy=policy,
        existing_parent=True,
    )
    state = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    assert state["last_vm"]["provision_generation"] == str(
        history[1]["provision_generation"]
    )
    assert await db.fetchval(
        "SELECT job_vm_creation_never_issued_source($1,$2)",
        owner,
        str(third["provision_generation"]),
    )
    await db.execute(
        "UPDATE vm_resource_waiters SET reason='generation_changed' WHERE request_id=$1",
        history[0]["request_id"],
    )
    assert not await db.fetchval(
        "SELECT job_vm_creation_never_issued_source($1,$2)",
        owner,
        str(third["provision_generation"]),
    )
    with pytest.raises(
        ResourceAdmissionError, match="job_vm_never_issued_source_unproven"
    ):
        await recovery.settle_never_issued_job_terminal(
            str(owner),
            provision_generation=str(third["provision_generation"]),
        )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            parent_id,
        )
        is None
    )


def actual_archive(db, recovery, monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    vm = VMProvisioner()
    vm._db = db
    vm._controller_url = "http://127.0.0.1:9"
    vm._probe_vm_teardown_identity = AsyncMock(
        side_effect=lambda _job_id, generation: _VMTeardownProbe(
            "absent",
            VMTeardownIdentity(generation, None, None),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        ),
    )
    vm.release_vm_captured = AsyncMock(
        side_effect=AssertionError("never-issued VM reached physical delete"),
    )

    def context_part(job, key):
        context = job.get("context") or {}
        if isinstance(context, str):
            context = json.loads(context)
        return context.get(key) or {}

    archive = partial(
        thread_retirement.archive_and_cleanup_workspace,
        dependencies=SimpleNamespace(
            store=db,
            vm_provisioner=vm,
            recovery_store=recovery,
            container_provisioner=SimpleNamespace(),
            docker_provisioner=SimpleNamespace(),
            get_container_context=lambda job: context_part(job, "workspace_container"),
            get_vm_context=lambda job: context_part(job, "vm"),
            vm_needs_release=vm_needs_release,
        ),
    )
    return archive, vm


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_parent", [False, True], ids=["fresh", "open-parent"])
async def test_cancelled_never_issued_job_finishes_normal_cleanup(
    db,
    monkeypatch,
    existing_parent,
):
    retry, recovery, parent_id = await never_issued_cancel(
        db,
        monkeypatch,
        existing_parent=existing_parent,
    )
    owner = retry["job_id"]
    archive, vm = actual_archive(db, recovery, monkeypatch)
    operation = controls(store=db, archive=archive)
    assert (
        await operation.wait_for_stateless_cancel_settle(
            str(owner),
            timeout_seconds=0,
        )
        is True
    )
    state = await db.fetchrow("SELECT context FROM jobs WHERE id=$1", owner)
    context = json.loads(state["context"])
    assert context["vm"]["status"] == "deleted"
    assert "_stateless_cancel_cleanup_pending" not in context
    rows = await db.fetch(
        "SELECT id,request_id,intent_digest,pvc_uid,parent_admission_id,outcome,completed_at "
        "FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' AND owner_id=$1 "
        "AND source='job_terminal_vm_release'",
        owner,
    )
    assert len(rows) == 1
    assert rows[0]["id"] == (parent_id or rows[0]["id"])
    assert rows[0]["pvc_uid"] is None and rows[0]["parent_admission_id"] is None
    assert rows[0]["outcome"] == "completed" and rows[0]["completed_at"]
    vm.release_vm_captured.assert_not_awaited()
    vm._probe_vm_teardown_identity.assert_not_awaited()
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            retry["request_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            retry["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_retired_never_issued_generation_survives_successor_resume(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    old_generation = str(retry["provision_generation"])
    assert await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        old_generation,
    )

    assert await db.queue_stateless_job_for_resume(
        str(owner),
        expected_status="cancelled",
    )
    request, fresh = candidate(owner)
    preflight = VMCreationPreflightStore(db)
    successor = await preflight.begin(
        job_id=str(owner),
        request=request,
        fresh_context=fresh,
    )
    assert successor["request"]["provision_generation"] != old_generation
    claim = (await preflight.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(controller(), claim["request"])
    resolved["creation_retry_protocol"] = 1
    admitted = await preflight.complete_resolution(claim, resolved)
    assert admitted["provision_generation"] == UUID(fresh["provision_generation"])
    assert admitted["execution_id"] == retry["execution_id"]
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO managed_repository_authorities "
            "(repository_owner,repo_name,authority_kind,authority_id,access_mode,"
            "clean_repo_url,public_key,public_key_fingerprint,private_key_ciphertext,status) "
            "VALUES('testowner',$2,'job',$1,'read',"
            "'https://example.invalid/testowner/testrepo','test-key','test-fingerprint',"
            "'v1:test-ciphertext','active')",
            owner,
            f"test-{owner.hex}",
        )
    assert await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        old_generation,
    )
    assert not await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        fresh["provision_generation"],
    )
    assert not await db.fetchval(
        "SELECT job_vm_creation_never_issued_source($1,$2)",
        owner,
        old_generation,
    )
    assert await db.fetchval(
        "SELECT managed_repository_process_zero_receipt_exists('job',$1,'vm','vm',$2)",
        owner,
        old_generation,
    )
    assert not await db.fetchval(
        "SELECT managed_repository_process_zero_receipt_exists('job',$1,'vm','vm',$2)",
        owner,
        fresh["provision_generation"],
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1",
            owner,
        )
        == 0
    )
    # A further lifecycle can replace last_vm. The retired source remains the
    # settled retry and exact completed parent, not that mutable context slot.
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{last_vm}',context->'vm') "
        "WHERE id=$1",
        owner,
    )
    assert await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        old_generation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["parent_digest", "issued", "prepared", "pvc"])
async def test_successor_preflight_requires_exact_never_issued_predecessor(
    db,
    monkeypatch,
    mutation,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    if mutation == "parent_digest":
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 "
            "WHERE owner_kind='job' AND owner_id=$1 AND source='job_terminal_vm_release'",
            owner,
            "sha256:" + "b" * 64,
        )
    else:
        key, value = {
            "issued": ("vm_uid", str(uuid4())),
            "prepared": ("preparation_request", {"owner_kind": "job"}),
            "pvc": ("rootdisk_pvc_uid", str(uuid4())),
        }[mutation]
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],$3::jsonb) WHERE id=$1",
            owner,
            ["vm", key],
            json.dumps(value),
        )
    assert await db.queue_stateless_job_for_resume(
        str(owner),
        expected_status="cancelled",
    )
    request, fresh = candidate(owner)
    with pytest.raises(VMCreationRetryConflict, match="creation_request_unproven"):
        await VMCreationPreflightStore(db).begin(
            job_id=str(owner),
            request=request,
            fresh_context=fresh,
        )


@pytest.mark.asyncio
async def test_retired_projection_refuses_conflicting_waiter_cancel_reason(
    db,
    monkeypatch,
):
    retry, recovery, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    generation = str(retry["provision_generation"])
    assert await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        generation,
    )
    await db.execute(
        "UPDATE vm_resource_waiters SET reason='generation_changed' "
        "WHERE request_id=$1",
        retry["request_id"],
    )
    assert not await db.fetchval(
        "SELECT job_vm_creation_never_issued_terminal_source($1,$2)",
        owner,
        generation,
    )
    assert not await db.fetchval(
        "SELECT managed_repository_process_zero_receipt_exists('job',$1,'vm','vm',$2)",
        owner,
        generation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "effect",
        "carrier",
        "charge",
        "preparation",
        "generation",
        "request",
        "digest",
        "parent_request",
        "conflict",
        "repository",
        "execution",
        "waiter",
        "preflight_digest",
        "issuance_bound",
        "snapshot_request",
        "snapshot_config",
        "snapshot_missing",
        "retry_digest",
        "configuration_digest",
        "preflight_expected_pvc",
        "preflight_predecessor_evidence",
        "preflight_predecessor_cleanup",
        "preflight_preparation",
        "preflight_storage",
        "pending_marker",
        "waiter_reason",
    ],
)
async def test_never_issued_terminal_refuses_changed_source_and_keeps_parent(
    db,
    monkeypatch,
    mutation,
):
    retry, recovery, parent_id = await never_issued_cancel(
        db,
        monkeypatch,
        existing_parent=True,
    )
    owner = retry["job_id"]
    async with db.acquire() as conn, conn.transaction():
        # These are deliberately inconsistent ledgers in a private disposable
        # database. The ordinary writers would usually refuse the mutation;
        # the final terminal source must refuse it independently too.
        await conn.execute("SET LOCAL session_replication_role = replica")
        if mutation == "effect":
            await conn.execute(
                "INSERT INTO vm_creation_effects "
                "(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,"
                "carrier_namespace,carrier_intent) "
                "VALUES($1,$2,1,'rootdisk',$3,'agent-vms','{}'::jsonb)",
                uuid4(),
                retry["request_id"],
                uuid4(),
            )
        elif mutation == "carrier":
            await conn.execute(
                "UPDATE vm_creation_retries SET creation_carrier_uid=$2,"
                "creation_carrier_namespace='agent-vms' WHERE request_id=$1",
                retry["request_id"],
                uuid4(),
            )
        elif mutation == "charge":
            await conn.execute(
                "INSERT INTO vm_resource_reservations "
                "(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,"
                "cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest,"
                "state,resource_version,ephemeral_storage_bytes,tun_devices,vhost_net_devices) "
                "SELECT $2,request_id,1,cluster_id,policy_digest,$3,'node-a',"
                "cpu_millicores,memory_bytes,kvm_devices,$4,$5,'reserved',2,"
                "ephemeral_storage_bytes,tun_devices,vhost_net_devices "
                "FROM vm_resource_waiters WHERE request_id=$1",
                retry["request_id"],
                uuid4(),
                uuid4(),
                uuid4(),
                "sha256:" + "a" * 64,
            )
        elif mutation in {
            "preparation",
            "generation",
            "request",
            "preflight_digest",
            "issuance_bound",
            "snapshot_request",
            "snapshot_config",
        }:
            path = {
                "preparation": "{vm,preparation_request}",
                "generation": "{vm,provision_generation}",
                "request": "{vm,creation_preflight,request_id}",
                "preflight_digest": "{vm,creation_preflight,request_digest}",
                "issuance_bound": "{vm,creation_request,issuance_authority_bound}",
                "snapshot_request": "{vm,creation_request,request,job_id}",
                "snapshot_config": "{vm,creation_request,controller_configuration_digest}",
            }[mutation]
            value = (
                {"ownerKind": "job"}
                if mutation == "preparation"
                else True
                if mutation == "issuance_bound"
                else "sha256:" + "a" * 64
                if mutation in {"preflight_digest", "snapshot_config"}
                else str(uuid4())
            )
            await conn.execute(
                f"UPDATE jobs SET context=jsonb_set(context,'{path}',$2::jsonb) WHERE id=$1",
                owner,
                json.dumps(value),
            )
        elif mutation in {"digest", "parent_request"}:
            if mutation == "digest":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 "
                    "WHERE id=$1",
                    parent_id,
                    "sha256:" + "b" * 64,
                )
            else:
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET request_id=$2 "
                    "WHERE id=$1",
                    parent_id,
                    uuid4(),
                )
        elif mutation == "snapshot_missing":
            await conn.execute(
                "UPDATE jobs SET context=context #- '{vm,creation_request}' WHERE id=$1",
                owner,
            )
        elif mutation in {
            "preflight_expected_pvc",
            "preflight_predecessor_evidence",
            "preflight_predecessor_cleanup",
            "preflight_preparation",
            "preflight_storage",
            "pending_marker",
        }:
            raw = await conn.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
            context = json.loads(raw)
            preflight = context["vm"]["creation_preflight"]
            if mutation == "preflight_expected_pvc":
                preflight["expected_pvc_uid"] = str(uuid4())
            elif mutation == "preflight_predecessor_evidence":
                preflight["predecessor_evidence"] = {
                    "provision_generation": str(uuid4())
                }
            elif mutation == "preflight_predecessor_cleanup":
                preflight["predecessor_cleanup_admission_id"] = str(uuid4())
            elif mutation in {"preflight_preparation", "preflight_storage"}:
                field = (
                    "preparation"
                    if mutation == "preflight_preparation"
                    else "workspace_storage"
                )
                preflight["request"][field] = {"pvc_uid": str(uuid4())}
                preflight["request_digest"] = canonical_request_digest(
                    preflight["request"]
                )
            else:
                context["_vm_creation_pending"] = str(uuid4())
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner,
                json.dumps(context),
            )
        elif mutation in {"retry_digest", "configuration_digest"}:
            field = (
                "request_digest"
                if mutation == "retry_digest"
                else "controller_configuration_digest"
            )
            await conn.execute(
                f"UPDATE vm_creation_retries SET {field}=$2 WHERE request_id=$1",
                retry["request_id"],
                "sha256:" + "c" * 64,
            )
        elif mutation == "conflict":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions "
                "SET source='completion_workspace_teardown' WHERE id=$1",
                parent_id,
            )
        elif mutation == "repository":
            await conn.execute(
                "INSERT INTO managed_repository_authorities "
                "(repository_owner,repo_name,authority_kind,authority_id,access_mode,"
                "clean_repo_url,public_key,public_key_fingerprint,private_key_ciphertext,status) "
                "VALUES('testowner',$2,'job',$1,'read',"
                "'https://example.invalid/testowner/testrepo','test-key','test-fingerprint',"
                "'v1:test-ciphertext','active')",
                owner,
                f"test-{owner.hex}",
            )
        elif mutation == "execution":
            await conn.execute(
                "UPDATE srw_execution_specs SET revision='changed' WHERE id=$1",
                retry["execution_id"],
            )
        elif mutation == "waiter":
            await conn.execute(
                "UPDATE vm_resource_waiters SET state='waiting' WHERE request_id=$1",
                retry["request_id"],
            )
        elif mutation == "waiter_reason":
            await conn.execute(
                "UPDATE vm_resource_waiters SET reason='generation_changed' "
                "WHERE request_id=$1",
                retry["request_id"],
            )
    with pytest.raises(
        (
            ResourceAdmissionError,
            WorkspaceRecoveryControlConflict,
            VMCreationRetryConflict,
        )
    ):
        await recovery.settle_never_issued_job_terminal(
            str(owner),
            provision_generation=str(retry["provision_generation"]),
        )
    parent = await db.fetchrow(
        "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
        parent_id,
    )
    assert parent["completed_at"] is None and parent["outcome"] is None
    state = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    assert state["vm"]["status"] != "deleted"
    assert state["_stateless_cancel_cleanup_pending"] is True
