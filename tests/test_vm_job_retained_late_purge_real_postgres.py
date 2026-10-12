"""A retained, already-released Job charge needs separate final disk proof."""

import asyncio
import json
import hashlib
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
import pytest_asyncio
import asyncpg
from fastapi import HTTPException

from orchestrator.database.postgres import JobVMAuditNotReady
from orchestrator.services.job_controls import JobControlOperations
from orchestrator.services.vm_idle_access import VMIdleAccessStore
from orchestrator.services.manifest_workspaces import ManifestWorkspaceService
from orchestrator.services.retained_vm_workspaces import record_detached
from orchestrator.services.vm_creation_request import (
    build_vm_creation_request,
    capture_vm_creation_request,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_pre_ssh_stop_store import VMPreSSHStopStore
from orchestrator.services.vm_job_retained_disk_purge import (
    acquire_job_retained_disk_purge,
    complete_job_retained_disk_purge,
)
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
)
from tests.test_vm_pre_ssh_stop_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _pre_ssh_db,  # noqa: F401
    pg_dsn,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    seeded_stop,
    terminal_proof,
    whole_schema,  # noqa: F401
)
from tests.test_vm_resource_whole_store_real_postgres import environment
from tests.test_vm_resource_inventory_real_postgres import publish
from tests.test_vm_resource_configuration import whole_launcher_configuration
from shared.vm_launcher_profile import predict_launcher
from shared.vm_creation_issuance import canonical_configuration_digest


@pytest_asyncio.fixture(scope="module")
async def late_purge_schema(pg_dsn, pre_ssh_schema):  # noqa: F811
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0334_vm_job_retained_disk_late_purge.sql"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_disk_purge_authorities') IS NOT NULL"
        ):
            await conn.execute(migration.read_text())
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(late_purge_schema, _pre_ssh_db):  # noqa: F811
    yield _pre_ssh_db


async def _controller_scope(db, request_id):
    scope = await db.fetchrow(
        "SELECT controller_configuration->>'namespace' AS namespace,"
        "controller_configuration->'resource_admission'->>'cluster_id' AS cluster_id "
        "FROM vm_creation_retries WHERE request_id=$1",
        UUID(str(request_id)),
    )
    assert scope is not None and scope["namespace"] and scope["cluster_id"]
    return {"version": 1, **dict(scope)}


async def _seed_bound_initial_stop(db, *, pre_ssh=True):
    """Use the ordinary request/admission/charge path with a real binding."""
    policy, inventory, inventory_value, _ = await environment(
        db, installation_count=2, owner_count=2
    )
    user_id, job_id, generation, execution_id, instance_id = (uuid4() for _ in range(5))
    binding = {
        "uid": str(instance_id),
        "generation": 1,
        "pvc_uid": None,
        "owner_id": str(job_id),
        "owner_kind": "job",
    }
    await db.execute(
        "INSERT INTO users(id,display_name) VALUES($1,'bound-owner')", user_id
    )
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,user_id,context) "
        "VALUES($1,'bound-stop','paused','stateless',$2,$3::jsonb)",
        job_id,
        user_id,
        json.dumps(
            {"vm": {"provision_generation": str(generation), "status": "failed"}}
        ),
    )
    await db.execute(
        "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,"
        "revision,harness_adapter) VALUES($1,'Job',$2,'{}','{}','revision-1','srw/v1')",
        execution_id,
        job_id,
    )
    await db.execute(
        "INSERT INTO srw_workspace_instances(id,owner_id,recipe,revision,pvc_name,"
        "execution_id,generation,backend_state) VALUES($1,$2,$3::jsonb,'revision-1',"
        "$4,$5,1,$6::jsonb)",
        instance_id,
        user_id,
        json.dumps({"backend": "vm"}),
        "srw-ws-" + instance_id.hex,
        execution_id,
        json.dumps({"storage": binding}),
    )
    await db.execute(
        "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) "
        "VALUES($1,$2)",
        execution_id,
        instance_id,
    )
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
    resource = config["resource_admission"]
    resource["cluster_id"] = inventory.cluster_id
    resource["policy_digest"] = inventory.policy_digest
    resource["template_profile"].update(
        storage_class="local", guest_vcpus=8, guest_memory_bytes=16 * 1024**3
    )
    resource["launcher_prediction"]["vector"] = predict_launcher(
        policy.launcher_profile, guest_vcpus=8, guest_memory_bytes=16 * 1024**3
    ).to_six_dict()
    resource["host_mapping"]["vector"] = policy.cost.cost(8, "16Gi").to_six_dict()
    request = build_vm_creation_request(
        job_id=str(job_id),
        provision_generation=str(generation),
        agent_config="worker_base",
        vm_image="pinned:image",
        cpu_cores=8,
        memory="16Gi",
        description="bound-stop",
        network_tier="restricted",
        workspace_storage=binding,
    )
    config_digest = canonical_configuration_digest(config)
    snapshot = await capture_vm_creation_request(
        db,
        job_id=str(job_id),
        generation=str(generation),
        request=request,
        controller_configuration_digest=config_digest,
        controller_configuration=config,
    )
    async with db.acquire() as conn, conn.transaction():
        retry = await VMCreationRetryStore(
            db, _resource_waiter_writer=policy
        ).admit_on_conn(
            conn,
            job_id=str(job_id),
            expected_generation=str(generation),
            request_id=str(uuid4()),
            proposal={
                "origin": "initial",
                "expected_status": "paused",
                "request_digest": snapshot["request_digest"],
                "controller_configuration_digest": config_digest,
                "expected_pvc_uid": None,
            },
        )
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (str(uuid4()) for _ in range(4))
    assert (
        await VMCreationRetryStore(db).authorize_controller(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]),
            observed={
                "job_id": str(job_id),
                "provision_generation": str(generation),
                "request_digest": claim["request_digest"],
                "controller_configuration_digest": claim[
                    "controller_configuration_digest"
                ],
                "expected_pvc_uid": None,
            },
        )
    )["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',reason='creation_adopted',"
        "boot_counted=TRUE,revision=revision+1,observed_vm_uid=$2,"
        "observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "ready_at=CASE WHEN $4 THEN NULL ELSE clock_timestamp() END,claim_token=NULL,"
        "claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"],
        UUID(vm_uid),
        UUID(pvc_uid),
        pre_ssh,
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
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
    )
    context["vm"].update(
        status="created" if pre_ssh else "ready",
        provision_generation=str(generation),
        vm_uid=vm_uid,
        vmi_uid=vmi_uid,
        active_pod_uid=launcher_uid,
        rootdisk_pvc_uid=pvc_uid,
        identity_authenticated=True,
        identity_provision_generation=str(generation),
        creation_request_id=str(retry["request_id"]),
        workspace_storage=binding,
    )
    context.pop("_vm_creation_pending", None)
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job_id, json.dumps(context)
    )
    await db.execute(
        "UPDATE srw_workspace_instances SET pvc_uid=$2,status='Attached' WHERE id=$1",
        instance_id,
        pvc_uid,
    )
    identity = VMTeardownIdentity(str(generation), vm_uid, pvc_uid)
    permit = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=str(job_id),
        identity=identity,
        source="dispatcher_vm_recycle",
        purge_disk=False,
    )
    assert permit.allowed and permit.parent_cleanup is not None
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"') WHERE id=$1",
        job_id,
    )
    frozen = {
        "kind": "vm_pre_ssh_stop_candidate_v1",
        "job_id": str(job_id),
        "provision_generation": str(generation),
        "namespace": inventory.namespace,
        "vm_name": "agent-vm-" + str(job_id),
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
            }
        ],
    }
    if pre_ssh:
        stop_store = VMPreSSHStopStore(db)
        admitted_intent = await stop_store.admit_intent(
            str(job_id), str(generation), permit.parent_cleanup, frozen
        )
        await stop_store.commit_positive_proof(
            str(job_id),
            str(generation),
            permit.parent_cleanup,
            terminal_proof(frozen, admitted_intent["frozen_digest"]),
        )
    else:
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            job_id,
            str(generation),
        )
    stop = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": str(job_id),
        "provision_generation": str(generation),
        "vm_uid": vm_uid,
        "vmi_uid": vmi_uid,
        "launcher_uid": launcher_uid,
        "pvc_uid": pvc_uid,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
    }
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        permit,
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=stop)
        ),
    )
    await db.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job_id)
    return {
        "job_id": job_id,
        "user_id": user_id,
        "generation": generation,
        "instance_id": instance_id,
        "execution_id": execution_id,
        "policy": policy,
        "inventory": inventory,
        "inventory_value": inventory_value,
        "config": config,
        "retry_id": retry["request_id"],
        "reservation_id": UUID(admitted["reservation_id"]),
        "identity": identity,
        "stop": stop,
        "binding": binding,
    }


async def _seed_bound_second_stop(db, first):
    """Use a fresh ordinary retry/charge against the exact retained PVC."""
    job_id = first["job_id"]
    generation = uuid4()
    pvc_uid = first["identity"].rootdisk_pvc_uid
    binding = {**first["binding"], "pvc_uid": pvc_uid}
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
    )
    context["last_vm"] = context["vm"]
    context["vm"] = {"provision_generation": str(generation), "status": "failed"}
    await db.execute(
        "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
        job_id,
        json.dumps(context),
    )
    request = build_vm_creation_request(
        job_id=str(job_id),
        provision_generation=str(generation),
        agent_config="worker_base",
        vm_image="pinned:image",
        cpu_cores=8,
        memory="16Gi",
        description="bound-stop-resume",
        network_tier="restricted",
        workspace_storage=binding,
    )
    config_digest = canonical_configuration_digest(first["config"])
    snapshot = await capture_vm_creation_request(
        db,
        job_id=str(job_id),
        generation=str(generation),
        request=request,
        controller_configuration_digest=config_digest,
        controller_configuration=first["config"],
    )
    old_parent = await db.fetchval(
        "SELECT cleanup_admission_id FROM vm_resource_cleanup_stop_receipts "
        "WHERE request_id=$1",
        first["retry_id"],
    )
    async with db.acquire() as conn, conn.transaction():
        retry = await VMCreationRetryStore(
            db, _resource_waiter_writer=first["policy"]
        ).admit_on_conn(
            conn,
            job_id=str(job_id),
            expected_generation=str(generation),
            request_id=str(uuid4()),
            proposal={
                "origin": "initial",
                "expected_status": "paused",
                "request_digest": snapshot["request_digest"],
                "controller_configuration_digest": config_digest,
                "expected_pvc_uid": pvc_uid,
                "predecessor_evidence": {
                    "provision_generation": str(first["generation"]),
                    "vm_uid": first["identity"].vm_uid,
                },
                "predecessor_cleanup_admission_id": str(old_parent),
            },
        )
    refreshed = deepcopy(first["inventory_value"])
    refreshed["snapshot_id"] = str(uuid4())
    refreshed["sequence"] += 1
    pv_uid = str(uuid4())
    refreshed["pvcs"] = [
        {
            "uid": pvc_uid,
            "name": "retained-root",
            "pv_uid": pv_uid,
            "pv_name": "retained-root-pv",
            "storage_class_uid": refreshed["storage_classes"][0]["uid"],
            "phase": "Bound",
        }
    ]
    refreshed["pvs"] = [
        {
            "uid": pv_uid,
            "name": "retained-root-pv",
            "claim_uid": pvc_uid,
            "required_affinity": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "kubernetes.io/hostname",
                                "operator": "In",
                                "values": ["node-a"],
                            }
                        ]
                    }
                ]
            },
        }
    ]
    refreshed["started_at"] = refreshed["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(first["inventory"], refreshed)
    admitted = await first["policy"].admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    vm_uid, vmi_uid, launcher_uid = (str(uuid4()) for _ in range(3))
    assert (
        await VMCreationRetryStore(db).authorize_controller(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]),
            observed={
                "job_id": str(job_id),
                "provision_generation": str(generation),
                "request_digest": claim["request_digest"],
                "controller_configuration_digest": claim[
                    "controller_configuration_digest"
                ],
                "expected_pvc_uid": pvc_uid,
            },
        )
    )["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',reason='creation_adopted',"
        "boot_counted=TRUE,revision=revision+1,observed_vm_uid=$2,"
        "observed_pvc_uid=$3,resolved_at=clock_timestamp(),ready_at=clock_timestamp(),"
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
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
    )
    context["vm"].update(
        status="ready",
        vm_uid=vm_uid,
        vmi_uid=vmi_uid,
        active_pod_uid=launcher_uid,
        rootdisk_pvc_uid=pvc_uid,
        identity_authenticated=True,
        identity_provision_generation=str(generation),
        creation_request_id=str(retry["request_id"]),
        workspace_storage=binding,
    )
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job_id, json.dumps(context)
    )
    await db.execute(
        "UPDATE srw_workspace_instances SET status='Attached' WHERE id=$1",
        first["instance_id"],
    )
    identity = VMTeardownIdentity(str(generation), vm_uid, pvc_uid)
    permit = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=str(job_id),
        identity=identity,
        source="dispatcher_vm_recycle",
        purge_disk=False,
    )
    assert permit.allowed and permit.parent_cleanup is not None
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"retiring_process_zero\"') WHERE id=$1",
        job_id,
    )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        job_id,
        str(generation),
    )
    stop = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": str(job_id),
        "provision_generation": str(generation),
        "vm_uid": vm_uid,
        "vmi_uid": vmi_uid,
        "launcher_uid": launcher_uid,
        "pvc_uid": pvc_uid,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
    }
    await complete_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        permit,
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=stop)
        ),
    )
    await db.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job_id)
    return {
        "generation": generation,
        "retry_id": retry["request_id"],
        "reservation_id": UUID(admitted["reservation_id"]),
        "identity": identity,
        "binding": binding,
        "stop": stop,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["positive", "generic_without_authority", "ack_only", "wrong_scope"]
)
async def test_unbound_retained_stop_requires_separate_purge_before_job_delete(
    db, mode
):
    """The later disk purge must not rewrite the original physical-stop charge."""
    state = await seeded_stop(db)
    old = await state["store"].admit_intent(
        state["job_id"], state["generation"], state["permit"], state["frozen"]
    )
    await state["store"].commit_positive_proof(
        state["job_id"],
        state["generation"],
        state["permit"],
        terminal_proof(state["frozen"], old["frozen_digest"]),
    )
    captured = state["frozen"]
    stop = {
        "version": 1,
        "kind": "vm_cleanup_physical_stop",
        "job_id": state["job_id"],
        "provision_generation": state["generation"],
        "vm_uid": captured["vm_uid"],
        "vmi_uid": captured["vmi_uid"],
        "launcher_uid": captured["launcher_uid"],
        "pvc_uid": captured["pvc_uid"],
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "pvc_disposition": "retained",
        "controller_authenticated": True,
    }
    store = VMWorkspaceRecoveryStore(db)
    await complete_vm_cleanup_permit(
        store,
        state["cleanup_permit"],
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=stop)
        ),
    )
    original = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(state["reservation_id"]),
    )
    assert original["state"] == "released"
    original_stop = await db.fetchrow(
        "SELECT cleanup_admission_id,reservation_id,request_id,job_id,"
        "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,intent_digest,"
        "stop_evidence,accepted_at FROM vm_resource_cleanup_stop_receipts "
        "WHERE reservation_id=$1",
        UUID(state["reservation_id"]),
    )
    assert original_stop is not None
    await db.execute(
        "UPDATE jobs SET status='cancelled' WHERE id=$1", UUID(state["job_id"])
    )
    if mode == "generic_without_authority":
        identity = VMTeardownIdentity(
            state["generation"], captured["vm_uid"], captured["pvc_uid"]
        )
        generic = await acquire_vm_cleanup_permit(
            store,
            owner_kind="job",
            owner_id=state["job_id"],
            identity=identity,
            source="public_vm_delete",
            purge_disk=True,
        )
        assert generic.allowed and generic.admission_id is not None
        with pytest.raises(asyncpg.CheckViolationError, match="retained disk purge"):
            await db.execute(
                "UPDATE vm_workspace_cleanup_admissions "
                "SET completed_at=clock_timestamp(),outcome='completed' "
                "WHERE id=$1",
                generic.admission_id,
            )
        assert (
            await db.fetchval(
                "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
                generic.admission_id,
            )
            is True
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_retained_disk_purge_receipts "
                "WHERE cleanup_admission_id=$1",
                generic.admission_id,
            )
            == 0
        )
        return
    purged = {
        **stop,
        "pvc_disposition": "purged",
        "controller_scope": await _controller_scope(db, original_stop["request_id"]),
    }
    if mode == "wrong_scope":
        purged["controller_scope"] = {
            **purged["controller_scope"],
            "namespace": "foreign-workers",
        }
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(
                state["generation"], captured["vm_uid"], captured["pvc_uid"]
            )
        ),
        release_vm_captured=AsyncMock(),
        delete_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
        attest_vm_cleanup_stop=AsyncMock(
            return_value=purged if mode != "ack_only" else None
        ),
    )
    controls = JobControlOperations(
        SimpleNamespace(vm_provisioner=provisioner, recovery_store=store)
    )
    if mode in {"ack_only", "wrong_scope"}:
        if mode == "ack_only":
            with pytest.raises(HTTPException) as refused:
                await controls.delete_vm(state["job_id"])
            assert refused.value.status_code == 409
        else:
            with pytest.raises(
                asyncpg.CheckViolationError, match="purge receipt lacks exact authority"
            ):
                await controls.delete_vm(state["job_id"])
        provisioner.delete_vm_captured.assert_awaited_once()
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_retained_disk_purge_receipts r "
                "JOIN vm_job_retained_disk_purge_authorities a USING(cleanup_admission_id) "
                "WHERE a.job_id=$1",
                UUID(state["job_id"]),
            )
            == 0
        )
        assert (
            await db.fetchrow(
                "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
                UUID(state["reservation_id"]),
            )
            == original
        )
        return
    assert (await controls.delete_vm(state["job_id"]))["status"] == "deleting"
    provisioner.delete_vm_captured.assert_awaited_once()
    provisioner.release_vm_captured.assert_not_awaited()
    purge = await db.fetchrow(
        "SELECT a.cleanup_admission_id,a.job_id,a.final_request_id,a.pvc_uid,"
        "p.source_request_id,p.old_cleanup_admission_id,p.reservation_id,"
        "r.purge_evidence,r.chain_digest FROM vm_job_retained_disk_purge_authorities a "
        "JOIN vm_job_retained_disk_purge_predecessors p USING(cleanup_admission_id) "
        "JOIN vm_job_retained_disk_purge_receipts r USING(cleanup_admission_id) "
        "WHERE a.job_id=$1",
        UUID(state["job_id"]),
    )
    assert purge is not None
    assert purge["old_cleanup_admission_id"] == original_stop["cleanup_admission_id"]
    assert purge["reservation_id"] == original_stop["reservation_id"]
    assert purge["purge_evidence"] is not None
    assert (
        await db.fetchrow(
            "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == original
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'::jsonb,true) WHERE id=$1",
        UUID(state["job_id"]),
    )
    assert await db.prepare_stateless_job_for_delete(state["job_id"]) is True
    assert await db.delete_job(state["job_id"], prepared_stateless=True) is True
    audit = await db.fetchrow(
        "SELECT deletion_receipt FROM vm_job_creation_owners WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    packet = json.loads(audit["deletion_receipt"])["generations"][0]
    assert packet["kind"] == "retained_late_purge"
    assert packet["old_cleanup_admission_id"] == str(
        original_stop["cleanup_admission_id"]
    )
    assert packet["old_reservation_id"] == str(original_stop["reservation_id"])
    assert packet["cleanup_admission_id"] == str(purge["cleanup_admission_id"])
    assert packet["chain_digest"] == purge["chain_digest"]
    assert (
        await db.fetchrow(
            "SELECT cleanup_admission_id,reservation_id,request_id,job_id,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,intent_digest,"
            "stop_evidence,accepted_at FROM vm_resource_cleanup_stop_receipts "
            "WHERE reservation_id=$1",
            UUID(state["reservation_id"]),
        )
        == original_stop
    )
    assert (
        await db.fetchrow(
            "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == original
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("pre_ssh", [True, False])
async def test_bound_late_purge_requires_actual_workspace_release(db, pre_ssh):
    state = await _seed_bound_initial_stop(db, pre_ssh=pre_ssh)
    job_id, instance_id = state["job_id"], state["instance_id"]
    captured = {**state["binding"], "pvc_uid": state["identity"].rootdisk_pvc_uid}
    purged = {
        **state["stop"],
        "pvc_disposition": "purged",
        "captured_workspace_storage": captured,
        "controller_scope": await _controller_scope(db, state["retry_id"]),
    }
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(),
        delete_vm_captured=AsyncMock(),
        attest_vm_cleanup_stop=AsyncMock(return_value=purged),
    )
    controls = JobControlOperations(
        SimpleNamespace(
            vm_provisioner=provisioner, recovery_store=VMWorkspaceRecoveryStore(db)
        )
    )
    with pytest.raises(HTTPException) as attached:
        await controls.delete_vm(str(job_id))
    assert attached.value.status_code == 409
    assert (
        await db.fetchval(
            "SELECT status FROM srw_workspace_instances WHERE id=$1", instance_id
        )
        == "Attached"
    )
    provisioner.attest_vm_cleanup_stop.assert_not_awaited()
    await record_detached(db, str(job_id), state["binding"])
    assert (
        await db.fetchval(
            "SELECT status FROM srw_workspace_instances WHERE id=$1", instance_id
        )
        == "Detached"
    )
    with pytest.raises(HTTPException) as detached:
        await controls.delete_vm(str(job_id))
    assert detached.value.status_code == 409
    workspace_service = ManifestWorkspaceService(
        db,
        None,
        namespace="workers",
        default_image="pinned:image",
        vm_provisioner=SimpleNamespace(
            release_workspace_storage=AsyncMock(return_value=True)
        ),
    )
    assert (
        await workspace_service._delete_vm(
            str(instance_id),
            {"id": str(state["user_id"]), "is_admin": True},
            expected_generation=1,
        )
    )["deleted"] is True
    assert (
        await db.fetchval(
            "SELECT status='Released' AND execution_id IS NULL "
            "FROM srw_workspace_instances WHERE id=$1",
            instance_id,
        )
        is True
    )
    assert (await controls.delete_vm(str(job_id)))["status"] == "deleting"
    provisioner.delete_vm_captured.assert_not_awaited()
    provisioner.release_vm_captured.assert_not_awaited()
    provisioner.attest_vm_cleanup_stop.assert_awaited_once()
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'::jsonb,true) WHERE id=$1",
        job_id,
    )
    assert await db.prepare_stateless_job_for_delete(str(job_id)) is True
    assert await db.delete_job(str(job_id), prepared_stateless=True) is True
    audit = await db.fetchval(
        "SELECT deletion_receipt FROM vm_job_creation_owners WHERE job_id=$1",
        job_id,
    )
    packet = json.loads(audit)["generations"][0]
    assert packet["kind"] == "retained_late_purge"
    assert packet["binding_kind"] == "bound"
    assert packet["workspace_instance_id"] == str(instance_id)
    assert await db.fetchval(
        "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1", job_id
    ) == int(pre_ssh)


@pytest.mark.asyncio
@pytest.mark.parametrize("first_pre_ssh", [False, True])
async def test_two_retained_generations_share_one_final_purge_after_workspace_release(
    db,
    first_pre_ssh,
):
    first = await _seed_bound_initial_stop(db, pre_ssh=first_pre_ssh)
    second = await _seed_bound_second_stop(db, first)
    job_id, instance_id = first["job_id"], first["instance_id"]
    await record_detached(db, str(job_id), second["binding"])
    workspace_service = ManifestWorkspaceService(
        db,
        None,
        namespace="workers",
        default_image="pinned:image",
        vm_provisioner=SimpleNamespace(
            release_workspace_storage=AsyncMock(return_value=True)
        ),
    )
    assert (
        await workspace_service._delete_vm(
            str(instance_id),
            {"id": str(first["user_id"]), "is_admin": True},
            expected_generation=1,
        )
    )["deleted"] is True
    captured = second["binding"]
    purged = {
        **second["stop"],
        "pvc_disposition": "purged",
        "captured_workspace_storage": captured,
        "controller_scope": await _controller_scope(db, second["retry_id"]),
    }
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=second["identity"]),
        release_vm_captured=AsyncMock(),
        delete_vm_captured=AsyncMock(),
        attest_vm_cleanup_stop=AsyncMock(return_value=purged),
    )
    controls = JobControlOperations(
        SimpleNamespace(
            vm_provisioner=provisioner, recovery_store=VMWorkspaceRecoveryStore(db)
        )
    )
    assert (await controls.delete_vm(str(job_id)))["status"] == "deleting"
    links = await db.fetch(
        "SELECT source_request_id FROM vm_job_retained_disk_purge_predecessors p "
        "JOIN vm_job_retained_disk_purge_authorities a USING(cleanup_admission_id) "
        "WHERE a.job_id=$1 ORDER BY source_request_id",
        job_id,
    )
    assert {r["source_request_id"] for r in links} == {
        first["retry_id"],
        second["retry_id"],
    }
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'::jsonb,true) WHERE id=$1",
        job_id,
    )
    assert await db.prepare_stateless_job_for_delete(str(job_id)) is True
    first_tab = await _expired_ide_tab(db, first)
    second_tab = await _expired_ide_tab(db, {**first, **second})
    assert await db.delete_job(str(job_id), prepared_stateless=True) is True
    closed_tabs = await db.fetch(
        "SELECT id FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) "
        "AND closed_at IS NOT NULL",
        [first_tab, second_tab],
    )
    assert {row["id"] for row in closed_tabs} == {first_tab, second_tab}
    receipt = json.loads(
        await db.fetchval(
            "SELECT deletion_receipt FROM vm_job_creation_owners WHERE job_id=$1",
            job_id,
        )
    )
    assert len(receipt["generations"]) == 2
    assert all(p["kind"] == "retained_late_purge" for p in receipt["generations"])
    assert len({p["cleanup_admission_id"] for p in receipt["generations"]}) == 1
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_cleanup_stop_receipts WHERE job_id=$1",
            job_id,
        )
        == 2
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations v "
            "JOIN vm_creation_retries r USING(request_id) "
            "WHERE r.job_id=$1 AND v.state='released'",
            job_id,
        )
        == 2
    )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_pre_ssh_stop_proofs WHERE job_id=$1", job_id
    ) == int(first_pre_ssh)


async def _prepared_retained_job_for_expired_ide_delete(db):
    """A real retained stop, workspace release and typed late purge."""
    state = await _seed_bound_initial_stop(db, pre_ssh=True)
    job_id, instance_id = state["job_id"], state["instance_id"]
    await record_detached(db, str(job_id), state["binding"])
    workspace_service = ManifestWorkspaceService(
        db,
        None,
        namespace="workers",
        default_image="pinned:image",
        vm_provisioner=SimpleNamespace(
            release_workspace_storage=AsyncMock(return_value=True)
        ),
    )
    assert (
        await workspace_service._delete_vm(
            str(instance_id),
            {"id": str(state["user_id"]), "is_admin": True},
            expected_generation=1,
        )
    )["deleted"] is True
    purged = {
        **state["stop"],
        "pvc_disposition": "purged",
        "captured_workspace_storage": {
            **state["binding"],
            "pvc_uid": state["identity"].rootdisk_pvc_uid,
        },
        "controller_scope": await _controller_scope(db, state["retry_id"]),
    }
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(),
        delete_vm_captured=AsyncMock(),
        attest_vm_cleanup_stop=AsyncMock(return_value=purged),
    )
    controls = JobControlOperations(
        SimpleNamespace(
            vm_provisioner=provisioner,
            recovery_store=VMWorkspaceRecoveryStore(db),
        )
    )
    assert (await controls.delete_vm(str(job_id)))["status"] == "deleting"
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}',"
        "'\"deleted\"'::jsonb,true) WHERE id=$1",
        job_id,
    )
    assert await db.prepare_stateless_job_for_delete(str(job_id)) is True
    return state


async def _expired_ide_tab(
    db,
    state,
    *,
    kind="ide",
    claimant=None,
    generation=None,
    vm_uid=None,
    owner_kind="job",
    owner_id=None,
):
    return await db.fetchval(
        "INSERT INTO vm_idle_access_leases "
        "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
        "acquired_at,expires_at,max_expires_at) VALUES "
        "($6,$1,$2,$3,$4,$5,clock_timestamp()-interval '5 minutes',"
        "clock_timestamp()-interval '3 minutes',"
        "clock_timestamp()-interval '1 minute') RETURNING id",
        owner_id or state["job_id"],
        generation or state["generation"],
        UUID(vm_uid or state["identity"].vm_uid),
        kind,
        claimant or f"{state['user_id']}:{uuid4()}",
        owner_kind,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["cancelled", "completed", "failed"])
async def test_prepared_job_delete_closes_expired_ordinary_ide_tab_after_late_purge(
    db, status
):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    await db.execute("UPDATE jobs SET status=$2 WHERE id=$1", state["job_id"], status)
    # A tab claimant need not be the Job owner (for example, a project member).
    claimant = f"{uuid4()}:{uuid4()}" if status == "completed" else None
    lease_id = await _expired_ide_tab(db, state, claimant=claimant)
    before = await db.fetchrow(
        "SELECT * FROM vm_idle_access_leases WHERE id=$1", lease_id
    )

    assert await db.delete_job(str(state["job_id"]), prepared_stateless=True) is True

    after = await db.fetchrow(
        "SELECT * FROM vm_idle_access_leases WHERE id=$1", lease_id
    )
    assert after["closed_at"] is not None
    assert {key: value for key, value in after.items() if key != "closed_at"} == {
        key: value for key, value in before.items() if key != "closed_at"
    }
    assert (
        await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", state["job_id"]) == 0
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM run_queue WHERE unit_id=$1", state["job_id"]
        )
        == 0
    )
    receipt = json.loads(
        await db.fetchval(
            "SELECT deletion_receipt FROM vm_job_creation_owners WHERE job_id=$1",
            state["job_id"],
        )
    )
    assert receipt["generations"][0]["kind"] == "retained_late_purge"
    assert receipt["generations"][0]["vm_uid"] == state["identity"].vm_uid


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blocker",
    [
        "active_ide",
        "ssh",
        "sftp",
        "operation",
        "unknown",
        "wrong_generation",
        "wrong_vm",
    ],
)
async def test_prepared_delete_leaves_all_access_unchanged_if_any_row_is_held(
    db, blocker
):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    candidate = await _expired_ide_tab(db, state)
    options = {}
    if blocker in {"ssh", "sftp"}:
        options["kind"] = blocker
    elif blocker == "operation":
        options["claimant"] = f"{state['user_id']}:operation:{uuid4()}:{uuid4()}"
    elif blocker == "unknown":
        options["claimant"] = "legacy-tab"
    elif blocker == "wrong_generation":
        options["generation"] = uuid4()
    elif blocker == "wrong_vm":
        options["vm_uid"] = str(uuid4())
    held = await _expired_ide_tab(db, state, **options)
    if blocker == "active_ide":
        await db.execute(
            "UPDATE vm_idle_access_leases SET "
            "expires_at=clock_timestamp()+interval '2 minutes',"
            "max_expires_at=clock_timestamp()+interval '3 minutes' WHERE id=$1",
            held,
        )
    before = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) ORDER BY id",
            [candidate, held],
        )
    ]

    with pytest.raises((JobVMAuditNotReady, asyncpg.CheckViolationError)):
        await db.delete_job(str(state["job_id"]), prepared_stateless=True)

    after = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) ORDER BY id",
            [candidate, held],
        )
    ]
    assert after == before
    assert (
        await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", state["job_id"]) == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            state["job_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_prepared_delete_does_not_touch_foreign_or_preclosed_tabs(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    candidate = await _expired_ide_tab(db, state)
    thread_same_uuid = await _expired_ide_tab(
        db, state, owner_kind="thread", owner_id=state["job_id"]
    )
    foreign_job = await _expired_ide_tab(db, state, owner_id=uuid4())
    preclosed = await _expired_ide_tab(db, state)
    await db.execute(
        "UPDATE vm_idle_access_leases SET closed_at=clock_timestamp() WHERE id=$1",
        preclosed,
    )
    before = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) ORDER BY id",
            [thread_same_uuid, foreign_job, preclosed],
        )
    ]

    assert await db.delete_job(str(state["job_id"]), prepared_stateless=True)

    assert (
        await db.fetchval(
            "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
            candidate,
        )
        is True
    )
    after = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_idle_access_leases WHERE id=ANY($1::uuid[]) ORDER BY id",
            [thread_same_uuid, foreign_job, preclosed],
        )
    ]
    assert after == before


@pytest.mark.asyncio
async def test_failed_final_job_delete_rolls_back_expired_tab_retirement(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    await db.execute(
        "INSERT INTO jobs(id,parent_job_id,description,status) "
        "VALUES($1,$2,'blocking child','created')",
        uuid4(),
        state["job_id"],
    )

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await db.delete_job(str(state["job_id"]), prepared_stateless=True)

    assert (
        await db.fetchval(
            "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1", lease_id
        )
        is True
    )
    assert (
        await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", state["job_id"]) == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM run_queue WHERE unit_id=$1", state["job_id"]
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            state["job_id"],
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT deleted_at IS NULL FROM vm_job_creation_owners WHERE job_id=$1",
            state["job_id"],
        )
        is True
    )


@pytest.mark.asyncio
async def test_unprepared_job_delete_cannot_close_expired_ide_tab(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    with pytest.raises(JobVMAuditNotReady, match="cleanup or access remains open"):
        await db.delete_job(str(state["job_id"]), prepared_stateless=False)
    assert (
        await db.fetchval(
            "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1", lease_id
        )
        is True
    )


@pytest.mark.asyncio
async def test_missing_typed_process_zero_proof_leaves_expired_tab_open(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    removed = await db.execute(
        "DELETE FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
        "AND runtime_incarnation=$2",
        state["job_id"],
        str(state["generation"]),
    )
    assert removed == "DELETE 1"

    with pytest.raises(asyncpg.CheckViolationError, match="predecessor changed"):
        await db.delete_job(str(state["job_id"]), prepared_stateless=True)

    assert (
        await db.fetchval(
            "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        is True
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_job_creation_terminal_packets WHERE job_id=$1",
            state["job_id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_missing_preparation_marker_leaves_expired_tab_open(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    await db.execute(
        "UPDATE jobs SET context=context-'_stateless_delete_pending' WHERE id=$1",
        state["job_id"],
    )
    with pytest.raises(RuntimeError, match="stateless deletion marker missing"):
        await db.delete_job(str(state["job_id"]), prepared_stateless=True)
    assert (
        await db.fetchval(
            "SELECT closed_at IS NULL FROM vm_idle_access_leases WHERE id=$1", lease_id
        )
        is True
    )


@pytest.mark.asyncio
async def test_delete_reclassifies_lease_row_after_lock_wait(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    async with db.acquire() as holder:
        async with holder.transaction():
            await holder.execute(
                "UPDATE vm_idle_access_leases SET claimed_by='legacy-tab' WHERE id=$1",
                lease_id,
            )
            deleting = asyncio.create_task(
                db.delete_job(str(state["job_id"]), prepared_stateless=True)
            )
            await asyncio.sleep(0.05)
            assert not deleting.done()
    with pytest.raises(JobVMAuditNotReady, match="cleanup or access remains open"):
        await deleting
    assert (
        await db.fetchval(
            "SELECT closed_at IS NULL AND claimed_by='legacy-tab' "
            "FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        is True
    )


@pytest.mark.asyncio
async def test_concurrent_exact_close_and_delete_never_reopen_tab(db):
    state = await _prepared_retained_job_for_expired_ide_delete(db)
    lease_id = await _expired_ide_tab(db, state)
    claimant = await db.fetchval(
        "SELECT claimed_by FROM vm_idle_access_leases WHERE id=$1", lease_id
    )
    async with db.acquire() as holder:
        async with holder.transaction():
            await holder.fetchrow(
                "SELECT id FROM vm_idle_access_leases WHERE id=$1 FOR UPDATE",
                lease_id,
            )
            deleting = asyncio.create_task(
                db.delete_job(str(state["job_id"]), prepared_stateless=True)
            )
            closing = asyncio.create_task(
                VMIdleAccessStore(db).close(
                    str(lease_id),
                    owner_kind="job",
                    owner_id=str(state["job_id"]),
                    kind="ide",
                    claimant=claimant,
                )
            )
            await asyncio.sleep(0.05)
            assert not deleting.done() and not closing.done()
    deleted, _ = await asyncio.gather(deleting, closing)
    assert deleted is True
    assert (
        await db.fetchval(
            "SELECT closed_at IS NOT NULL FROM vm_idle_access_leases WHERE id=$1",
            lease_id,
        )
        is True
    )


async def _released_bound_stop(db):
    state = await _seed_bound_initial_stop(db, pre_ssh=False)
    await record_detached(db, str(state["job_id"]), state["binding"])
    workspace_service = ManifestWorkspaceService(
        db,
        None,
        namespace="workers",
        default_image="pinned:image",
        vm_provisioner=SimpleNamespace(
            release_workspace_storage=AsyncMock(return_value=True)
        ),
    )
    assert (
        await workspace_service._delete_vm(
            str(state["instance_id"]),
            {"id": str(state["user_id"]), "is_admin": True},
            expected_generation=1,
        )
    )["deleted"] is True
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("child_case", ["exact", "foreign", "same_source_forged"])
async def test_exact_open_controller_child_can_resume_without_foreign_child_bypass(
    db, child_case
):
    state = await _released_bound_stop(db)
    store = VMWorkspaceRecoveryStore(db)
    parent = await acquire_job_retained_disk_purge(
        store, job_id=str(state["job_id"]), identity=state["identity"]
    )
    assert parent is not None and parent.allowed and parent.admission_id is not None
    owner = state["job_id"]
    pvc_uid = UUID(state["identity"].rootdisk_pvc_uid)
    if child_case != "exact":
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,"
            "parent_admission_id) VALUES($1,'job',$2,$3,$4,$5,$6,$7)",
            uuid4(),
            owner,
            pvc_uid,
            "foreign_rootdisk_delete"
            if child_case == "foreign"
            else "controller_rootdisk_delete",
            uuid4(),
            "sha256:" + "0" * 64,
            parent.admission_id,
        )
        if child_case == "foreign":
            with pytest.raises(asyncpg.CheckViolationError, match="current authority"):
                await db.fetchval(
                    "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
                    parent.admission_id,
                )
        else:
            # A directly forged same-source open row cannot complete the
            # parent, even if a signed-looking final absence is supplied.
            purged = {
                **state["stop"],
                "pvc_disposition": "purged",
                "controller_scope": await _controller_scope(db, state["retry_id"]),
                "captured_workspace_storage": {
                    **state["binding"],
                    "pvc_uid": state["identity"].rootdisk_pvc_uid,
                },
            }
            with pytest.raises(asyncpg.CheckViolationError, match="current authority"):
                await complete_job_retained_disk_purge(
                    store,
                    parent,
                    outcome="completed",
                    provisioner=SimpleNamespace(
                        attest_vm_cleanup_stop=AsyncMock(return_value=purged)
                    ),
                )
            assert (
                await db.fetchval(
                    "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions "
                    "WHERE id=$1",
                    parent.admission_id,
                )
                is True
            )
        return
    identity = {
        "source": "controller_rootdisk_delete",
        "owner_kind": "job",
        "owner_id": str(owner),
        "pvc_uid": str(pvc_uid),
        "dv_uid": str(uuid4()),
        "provision_generation": str(state["generation"]),
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    request_id = uuid5(
        NAMESPACE_URL,
        f"srw-controller-cleanup:{canonical}:parent:{parent.admission_id}",
    )
    digest = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
    child = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner,
        pvc_uid=pvc_uid,
        request_id=request_id,
        source="controller_rootdisk_delete",
        intent_digest=digest,
        parent_cleanup=parent.parent_cleanup,
        parent_provision_generation=str(state["generation"]),
        expected_vm_uid=state["identity"].vm_uid,
    )
    assert child.allowed and child.admission_id is not None
    resumed = await store.resume_cleanup_permit(
        child.admission_id,
        owner_kind="job",
        owner_id=owner,
        source="controller_rootdisk_delete",
        request_id=request_id,
        intent_digest=digest,
    )
    assert resumed.allowed and resumed.admission_id == child.admission_id
    assert (
        await store.complete_cleanup_permit(
            child.admission_id,
            outcome="deleted",
            request_id=request_id,
            intent_digest=digest,
        )
        is True
    )
    purged = {
        **state["stop"],
        "pvc_disposition": "purged",
        "controller_scope": await _controller_scope(db, state["retry_id"]),
        "captured_workspace_storage": {
            **state["binding"],
            "pvc_uid": state["identity"].rootdisk_pvc_uid,
        },
    }
    await complete_job_retained_disk_purge(
        store,
        parent,
        outcome="completed",
        provisioner=SimpleNamespace(
            attest_vm_cleanup_stop=AsyncMock(return_value=purged)
        ),
    )
    assert (
        await db.fetchval(
            "SELECT completed_at IS NOT NULL AND outcome='completed' "
            "FROM vm_workspace_cleanup_admissions WHERE id=$1",
            parent.admission_id,
        )
        is True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["final_runtime", "missing_generation"])
async def test_direct_sql_cannot_grant_forged_final_runtime_or_binding(db, forgery):
    state = await _released_bound_stop(db)
    parent = await acquire_vm_cleanup_permit(
        VMWorkspaceRecoveryStore(db),
        owner_kind="job",
        owner_id=str(state["job_id"]),
        identity=state["identity"],
        source="public_vm_delete",
        purge_disk=True,
    )
    assert parent.allowed and parent.admission_id is not None
    stop = await db.fetchrow(
        "SELECT * FROM vm_resource_cleanup_stop_receipts WHERE request_id=$1",
        state["retry_id"],
    )
    admission = await db.fetchrow(
        "SELECT request_id,intent_digest FROM vm_workspace_cleanup_admissions "
        "WHERE id=$1",
        parent.admission_id,
    )
    await db.execute(
        "INSERT INTO vm_job_retained_disk_purge_authorities "
        "(cleanup_admission_id,job_id,final_request_id,provision_generation,"
        "vm_uid,vmi_uid,launcher_uid,pvc_uid,binding_kind,workspace_instance_id,"
        "workspace_generation,cleanup_request_id,intent_digest) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,'bound',$9,1,$10,$11)",
        parent.admission_id,
        state["job_id"],
        state["retry_id"],
        state["generation"],
        UUID(state["identity"].vm_uid),
        uuid4() if forgery == "final_runtime" else stop["vmi_uid"],
        stop["launcher_uid"],
        UUID(state["identity"].rootdisk_pvc_uid),
        state["instance_id"],
        admission["request_id"],
        admission["intent_digest"],
    )
    await db.execute(
        "INSERT INTO vm_job_retained_disk_purge_predecessors "
        "(cleanup_admission_id,source_request_id,old_cleanup_admission_id,"
        "reservation_id,reservation_revision,stop_evidence_digest) "
        "SELECT $1,s.request_id,s.cleanup_admission_id,s.reservation_id,v.revision,"
        "'sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex') "
        "FROM vm_resource_cleanup_stop_receipts s JOIN vm_resource_reservations v "
        "ON v.id=s.reservation_id WHERE s.request_id=$2",
        parent.admission_id,
        state["retry_id"],
    )
    if forgery == "missing_generation":
        with pytest.raises(
            asyncpg.CheckViolationError, match="creation retry identity is immutable"
        ):
            await db.execute(
                "UPDATE vm_creation_retries SET canonical_request=jsonb_set("
                "canonical_request,'{workspace_storage,generation}','null'::jsonb) "
                "WHERE request_id=$1",
                state["retry_id"],
            )
        return
    with pytest.raises(asyncpg.CheckViolationError, match="generation|identity"):
        await db.fetchval(
            "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
            parent.admission_id,
        )
    assert (
        await db.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            parent.admission_id,
        )
        is True
    )
