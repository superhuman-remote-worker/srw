"""An unchanged S17 review remains approvable after access-only VM wake."""

import json
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import asyncpg
from fastapi import HTTPException

from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema, db as _db_fixture, pg_dsn, postgres_db_fixture, _schema_applied,  # noqa: F401
)
from tests.test_vm_idle_phase_approval_real_postgres import seed_phase_wait
from tests.test_vm_idle_terminal_review_real_postgres import seed_final_review, controls_for
from shared.workspace_idle_policy import read_episode


db = _db_fixture


@pytest.fixture(autouse=True)
def idle_env(monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)


async def access_only_cycle(db, owner, episode, identity, *, execution_requested=False):
    from orchestrator.services.vm_idle_lifecycle import (
        VMIdleLifecycleService, VMIdleLifecycleStore,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=episode.episode_id, revision=episode.revision,
        identity=identity,
    )
    assert operation is not None
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), **identity,
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    cleanup_intent = {
        "owner_kind": "job", "owner_id": str(owner),
        "provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"], "pvc_uid": identity["pvc_uid"],
        "purge_disk": False, "resource": "vm_workspace", "source": "vm_idle_release",
    }
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'vm_idle_release',$4,$5,clock_timestamp(),'completed')",
        uuid4(), owner, UUID(identity["pvc_uid"]), uuid4(),
        cleanup_intent_digest(cleanup_intent),
    )
    wake = await store.request_wake(str(owner), execution_requested=execution_requested)
    assert wake is not None and wake["wake_execution_requested"] is execution_requested
    provisioner = VMProvisioner()
    provisioner._db = db
    create_result = await provisioner.create_vm(
        str(owner), idle_wake_id=str(operation["id"]),
    )
    assert create_result["request_id"] == str(wake["wake_request_id"])
    successor = {
        "generation": str(wake["wake_generation"]),
        "vm_uid": str(uuid4()), "vmi_uid": str(uuid4()),
        "launcher_uid": str(uuid4()), "pvc_uid": identity["pvc_uid"],
    }
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    successor_preflight = context["vm"]["creation_preflight"]
    context["vm"].update(
        status="ready", provision_attempts=0,
        identity_authenticated=True,
        identity_provision_generation=successor["generation"],
        creation_request_id=successor_preflight["request_id"],
        vm_uid=successor["vm_uid"], vmi_uid=successor["vmi_uid"],
        active_pod_uid=successor["launcher_uid"],
        rootdisk_pvc_uid=successor["pvc_uid"],
        ssh_host="10.42.0.92", ssh_port=22,
        ssh_ready_source="provisioner_probe",
        ssh_host_key_fingerprint="SHA256:" + "B" * 43,
    )
    context["vm"]["creation_preflight"]["state"] = "admitted"
    context["vm"]["network_profile_evidence"] = {
        **context["last_vm"]["network_profile_evidence"],
        "provision_generation": successor["generation"],
        "vm_uid": successor["vm_uid"],
        "vmi_uid": successor["vmi_uid"],
        "launcher_uid": successor["launcher_uid"],
    }
    context.pop("_vm_creation_pending", None)
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    successor_admission = uuid4()
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'profiled-idle-fixture',$4,'test',clock_timestamp(),'completed')",
        successor_admission, owner, UUID(successor["pvc_uid"]), uuid4(),
    )
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
        "admission_deadline,expected_pvc_uid,creation_admission_id,state,observed_vm_uid,"
        "observed_pvc_uid,resolved_at) "
        "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,'succeeded',$13,$14,clock_timestamp())",
        UUID(successor_preflight["request_id"]), owner, UUID(successor["generation"]),
        successor_preflight["request_digest"], json.dumps(successor_preflight["request"]),
        "sha256:" + "a" * 64, UUID(successor_preflight["execution_id"]),
        successor_preflight["execution_revision"], successor_preflight["execution_generation"],
        datetime.fromisoformat(successor_preflight["admission_deadline"]),
        UUID(successor["pvc_uid"]), successor_admission,
        UUID(successor["vm_uid"]), UUID(successor["pvc_uid"]),
    )
    class ReadyProvisioner:
        async def attest_workspace_runtime(self, job_id):
            assert job_id == str(owner)
            return SimpleNamespace(
                workspace_generation=successor["generation"],
                vm_uid=successor["vm_uid"], vmi_uid=successor["vmi_uid"],
                launcher_pod_uid=successor["launcher_uid"],
                rootdisk_pvc_uid=successor["pvc_uid"],
            )

    service = VMIdleLifecycleService(db, ReadyProvisioner(), object())
    assert await service._wake(await store.get_operation(str(operation["id"])), current=lambda: True)
    assert await store.finish_wake(str(operation["id"]))
    row = await db.fetchrow(
        "SELECT status,workspace_idle_episode,workspace_idle_revision FROM jobs WHERE id=$1", owner,
    )
    if execution_requested:
        assert row["status"] == "paused" and row["workspace_idle_episode"] is None
        assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "queued"
        assert await db.fetchval(
            "SELECT access_rebind_proof FROM vm_idle_operations WHERE id=$1", operation["id"],
        ) is None
        return operation, None, successor
    rebound = read_episode(json.loads(row["workspace_idle_episode"]), revision=row["workspace_idle_revision"])
    assert row["status"] == "pending_review"
    assert rebound.episode_id == episode.episode_id
    assert rebound.wait_key == episode.wait_key
    assert rebound.entered_at == episode.entered_at
    assert rebound.revision == episode.revision + 1
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done"
    return operation, rebound, successor


@pytest.mark.asyncio
@pytest.mark.parametrize("cycles", [1, 2])
async def test_final_review_after_access_only_wake_approves_exact_successor(db, monkeypatch, tmp_path, cycles):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    owner, episode, identity, command_id = await seed_final_review(db)
    operation = None
    rebound, successor = episode, identity
    for _ in range(cycles):
        operation, rebound, successor = await access_only_cycle(db, owner, rebound, successor)
    from orchestrator.services.vm_idle_phase_approval import finalized_review_source
    async with db.acquire() as conn:
        job_row = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", owner)
        assert await finalized_review_source(
            conn, job=job_row, episode=rebound,
            generation=successor["generation"], vm_uid=successor["vm_uid"],
            launcher_uid=successor["launcher_uid"], pvc_uid=successor["pvc_uid"],
        ) is not None
    controls = await controls_for(db, monkeypatch, tmp_path)
    monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"}, job=await db.get_job(str(owner)), request=None,
    )
    assert result["status"] == "approved"
    terminal = await VMIdleLifecycleStore(db).get_open_for_owner(str(owner))
    assert terminal["terminal_source_command_id"] == command_id
    assert terminal["provision_generation"] == UUID(successor["generation"])
    assert terminal["pvc_uid"] == operation["pvc_uid"]
    assert terminal["storage_disposition"] == "retention_unknown"
    assert not terminal["wake_requested"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cycles", [1, 2])
async def test_phase_approval_after_closed_access_wake_queues_once(db, monkeypatch, tmp_path, cycles):
    await _schema(db)
    owner, episode, identity, command_id = await seed_phase_wait(db)
    operation = None
    rebound, successor = episode, identity
    for _ in range(cycles):
        operation, rebound, successor = await access_only_cycle(db, owner, rebound, successor)
    controls = await controls_for(db, monkeypatch, tmp_path)
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"}, job=await db.get_job(str(owner)), request=None,
    )
    assert result["status"] == "approved_continue"
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "queued"
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "paused"
    assert await db.fetchval("SELECT workspace_idle_episode FROM jobs WHERE id=$1", owner) is None
    assert await db.fetchval(
        "SELECT access_rebind_proof IS NOT NULL FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["phase", "final"])
async def test_approval_joins_next_open_release_after_access_rebind(
    db, monkeypatch, tmp_path, kind,
):
    from unittest.mock import AsyncMock
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    await _schema(db)
    if kind == "phase":
        owner, episode, identity, command_id = await seed_phase_wait(db)
    else:
        owner, episode, identity, command_id = await seed_final_review(db)
    _, rebound, successor = await access_only_cycle(db, owner, episode, identity)
    store = VMIdleLifecycleStore(db)
    opened = await store.admit_release(
        str(owner), episode_id=rebound.episode_id,
        revision=rebound.revision, identity=successor,
    )
    assert opened is not None and opened["phase"] == "releasing"
    controls = await controls_for(db, monkeypatch, tmp_path)
    if kind == "final":
        monkeypatch.setattr(type(controls), "publish_terminal_review", AsyncMock())
    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=await db.get_job(str(owner)), request=None,
    )
    operation = await store.get_operation(str(opened["id"]))
    if kind == "phase":
        assert result["status"] == "waking"
        assert operation["wake_execution_requested"]
        assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done"
    else:
        assert result["status"] == "approved"
        assert operation["terminal_source_command_id"] == command_id
        assert operation["storage_disposition"] == "retention_unknown"
        assert not operation["wake_requested"]


@pytest.mark.asyncio
async def test_native_access_lineage_receipt_is_immutable(db):
    await _schema(db)
    owner, episode, identity, _ = await seed_final_review(db)
    operation, rebound, successor = await access_only_cycle(db, owner, episode, identity)
    await access_only_cycle(db, owner, rebound, successor)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET access_rebind_proof=NULL WHERE id=$1",
            operation["id"],
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET vmi_uid=$2 WHERE id=$1",
            operation["id"], uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_idle_operations SET access_rebind_proof=jsonb_set("
            "access_rebind_proof,'{successor,vmi_uid}',to_jsonb($2::text)) "
            "WHERE id=$1", operation["id"], str(uuid4()),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("DELETE FROM vm_idle_operations WHERE id=$1", operation["id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["phase", "final"])
@pytest.mark.parametrize("drift", ["missing", "altered"])
async def test_approval_and_next_release_refuse_unproven_historical_receipt(
    db, monkeypatch, tmp_path, kind, drift,
):
    await _schema(db)
    if kind == "phase":
        owner, episode, identity, _ = await seed_phase_wait(db)
    else:
        owner, episode, identity, _ = await seed_final_review(db)
    first, rebound, successor = await access_only_cycle(db, owner, episode, identity)
    # Simulate a pre-0273 historical hole or a damaged retained record. The
    # native guard separately proves such edits are refused in normal use.
    async with db.acquire() as conn:
        await conn.execute("ALTER TABLE vm_idle_operations DISABLE TRIGGER vm_idle_access_rebind_guard")
        try:
            if drift == "missing":
                await conn.execute(
                    "UPDATE vm_idle_operations SET access_rebind_proof=NULL WHERE id=$1",
                    first["id"],
                )
            else:
                await conn.execute(
                    "UPDATE vm_idle_operations SET access_rebind_proof=jsonb_set("
                    "access_rebind_proof,'{successor,vmi_uid}',to_jsonb($2::text)) "
                    "WHERE id=$1", first["id"], str(uuid4()),
                )
        finally:
            await conn.execute("ALTER TABLE vm_idle_operations ENABLE TRIGGER vm_idle_access_rebind_guard")
    controls = await controls_for(db, monkeypatch, tmp_path)
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    assert await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=rebound.episode_id, revision=rebound.revision,
        identity=successor,
    ) is None
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
    assert raised.value.status_code == 409
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_idle_operations WHERE owner_id=$1 "
        "AND terminal_source_command_id IS NOT NULL", owner,
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["rootdisk_pvc_uid", "vmi_uid", "active_pod_uid"])
async def test_final_review_refuses_changed_current_successor_identity(
    db, monkeypatch, tmp_path, field,
):
    await _schema(db)
    owner, episode, identity, _ = await seed_final_review(db)
    await access_only_cycle(db, owner, episode, identity)
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    context["vm"][field] = str(uuid4())
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    controls = await controls_for(db, monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"},
            job=await db.get_job(str(owner)), request=None,
        )
    assert raised.value.status_code == 409
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["phase", "final"])
async def test_stale_caller_episode_cannot_approve_new_wait(db, monkeypatch, tmp_path, kind):
    from shared.workspace_idle_policy import IdleEpisode, episode_document

    await _schema(db)
    if kind == "phase":
        owner, episode, identity, _ = await seed_phase_wait(db)
    else:
        owner, episode, identity, _ = await seed_final_review(db)
    _, rebound, _ = await access_only_cycle(db, owner, episode, identity)
    caller_a = await db.get_job(str(owner))
    new_episode = IdleEpisode(
        str(uuid4()), rebound.revision + 1, rebound.wait_kind, str(uuid4()),
        rebound.entered_at, None, 0, rebound.runtime_identity,
    )
    await db.execute(
        "UPDATE jobs SET workspace_idle_revision=$2,workspace_idle_episode=$3::jsonb "
        "WHERE id=$1", owner, new_episode.revision,
        json.dumps(episode_document(new_episode)),
    )
    controls = await controls_for(db, monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"}, job=caller_a, request=None,
        )
    assert raised.value.status_code == 409
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "pending_review"


@pytest.mark.asyncio
async def test_execution_wake_cannot_be_used_as_access_lineage(db, monkeypatch, tmp_path):
    await _schema(db)
    owner, episode, identity, _ = await seed_phase_wait(db)
    caller = await db.get_job(str(owner))
    operation, _, _ = await access_only_cycle(
        db, owner, episode, identity, execution_requested=True,
    )
    assert not (await db.fetchval(
        "SELECT access_rebind_proof IS NOT NULL FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    ))
    controls = await controls_for(db, monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as raised:
        await controls.approve_job(
            str(owner), user={"id": "reviewer"}, job=caller, request=None,
        )
    assert raised.value.status_code == 409
