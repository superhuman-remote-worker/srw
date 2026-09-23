"""Idle release must serialize with the real owner and worker queue rows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import asyncio
from importlib import import_module
import json
from pathlib import Path
from uuid import UUID, uuid4
from types import SimpleNamespace

import pytest

from shared.workspace_idle_policy import IdleEpisode, RuntimeIdentity, episode_document
from tests.test_workspace_idle_store_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)


db = _db_fixture
MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src/orchestrator/database/migrations/app/0270_vm_idle_lifecycle.sql"
)


async def seed_wait(db):
    owner, generation, vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(6))
    route_id = uuid4()
    vm = {
        "status": "ready",
        "provision_generation": str(generation),
        "identity_authenticated": True,
        "identity_provision_generation": str(generation),
        "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid),
        "active_pod_uid": str(launcher_uid),
        "rootdisk_pvc_uid": str(pvc_uid),
        "ssh_host": "10.42.0.91",
        "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
    }
    runtime = RuntimeIdentity("job", str(owner), "vm", str(generation), str(vm_uid))
    idle = IdleEpisode(
        str(uuid4()),
        1,
        "human_message",
        str(route_id),
        datetime.now(timezone.utc) - timedelta(minutes=16),
        None,
        0,
        runtime,
    )
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,context,config_override,freeze_data) "
        "VALUES($1,'idle VM','waiting_for_reply','stateless',$2::jsonb,$3::jsonb,$4::jsonb)",
        owner,
        json.dumps({"vm": vm}),
        json.dumps({"workspace": {"backend": "vm"}}),
        json.dumps({"route_id": str(route_id)}),
    )
    await db.execute(
        "UPDATE jobs SET workspace_idle_revision=1,workspace_idle_episode=$2::jsonb WHERE id=$1",
        owner,
        json.dumps(episode_document(idle)),
    )
    await db.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state) VALUES($1,'worker_batch','done')",
        owner,
    )
    await db.execute(
        "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,revision,harness_adapter) "
        "VALUES($1,'Job',$2,'{}',$3::jsonb,'revision-1','srw/v1')",
        uuid4(), owner, json.dumps({"spec": {"timeoutSeconds": 7200}}),
    )
    return owner, idle, {
        "generation": str(generation),
        "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid),
        "pvc_uid": str(pvc_uid),
    }


async def _schema(db):
    if not await db.fetchval("SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"):
        await db.execute(MIGRATION.read_text())


@pytest.mark.asyncio
async def test_due_release_admission_is_durable_and_closes_remote_io(db, monkeypatch):
    """Removing the admission write would let capture race a fresh VM writer."""
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    assert operation is not None
    assert operation["phase"] == "releasing"
    assert operation["pvc_uid"] == UUID(identity["pvc_uid"])
    job = await db.fetchrow("SELECT context,workspace_idle_revision FROM jobs WHERE id=$1", owner)
    vm = json.loads(job["context"])["vm"]
    assert vm["status"] == "suspending"
    assert vm["_suspend_remote_io_closed"] == str(operation["id"])
    assert job["workspace_idle_revision"] == idle.revision
    replay = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    assert replay["id"] == operation["id"]


@pytest.mark.asyncio
async def test_release_requires_exact_process_zero_and_physical_absence(db, monkeypatch):
    """A generic teardown completion must not publish suspended or free compute."""
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    assert operation is not None
    evidence = {
        "version": 1,
        "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        **identity,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "same_generation_replacement": False,
        "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert not await store.complete_release(str(operation["id"]), evidence=evidence)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        owner,
        identity["generation"],
    )
    assert not await store.complete_release(
        str(operation["id"]), evidence={**evidence, "pvc_uid": str(uuid4())}
    )
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    row = await db.fetchrow(
        "SELECT phase,stop_evidence,stop_verified_at FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    )
    assert row["phase"] == "suspended"
    assert json.loads(row["stop_evidence"])["operation_id"] == str(operation["id"])
    assert row["stop_verified_at"] is not None
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    assert context["vm"]["status"] == "suspended"
    assert context["vm"]["rootdisk"] == "kept"


@pytest.mark.asyncio
async def test_release_claim_is_single_owner_renewable_and_replayable(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    op = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    winners = await asyncio.gather(
        store.claim(str(op["id"]), claimant="replica-a", seconds=30),
        store.claim(str(op["id"]), claimant="replica-b", seconds=30),
    )
    claimed = [item for item in winners if item is not None]
    assert len(claimed) == 1
    token = claimed[0]["claim_token"]
    winner = claimed[0]["claimed_by"]
    loser = "replica-b" if winner == "replica-a" else "replica-a"
    assert await store.renew_claim(str(op["id"]), token=token, claimant=winner, seconds=30)
    assert not await store.renew_claim(str(op["id"]), token=token, claimant=loser, seconds=30)
    assert not await store.claim(str(op["id"]), claimant=loser, seconds=30)
    await store.release_claim(str(op["id"]), token=token, claimant=winner)
    replay = await store.claim(str(op["id"]), claimant=loser, seconds=30)
    assert replay is not None and replay["claim_token"] > token


@pytest.mark.asyncio
async def test_reply_queue_cannot_claim_worker_through_releasing_vm(db, monkeypatch):
    from shared.worker_queue import claim_worker_batch

    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    op = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    assert await db.queue_stateless_job_for_resume(
        str(owner), expected_status="waiting_for_reply"
    )
    assert await claim_worker_batch(db, pod_name="idle-race-worker") is None
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done"
    assert await db.fetchval("SELECT phase FROM vm_idle_operations WHERE id=$1", op["id"]) == "releasing"
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop", "operation_id": str(op["id"]),
        **identity, "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(op["id"]), evidence=evidence)
    state = await store.get_operation(str(op["id"]))
    assert state["wake_requested"] and state["wake_execution_requested"]


@pytest.mark.asyncio
async def test_reconcile_replays_ambiguous_stop_without_publishing_early(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    module = import_module("orchestrator.services.vm_idle_lifecycle")
    operation = await module.VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    monkeypatch.setattr(
        module, "acquire_vm_cleanup_permit",
        lambda *args, **kwargs: asyncio.sleep(0, result=SimpleNamespace(allowed=True, completed_outcome=None, parent_cleanup=None)),
    )
    monkeypatch.setattr(module, "complete_vm_cleanup_permit", lambda *args, **kwargs: asyncio.sleep(0))

    class Provisioner:
        calls = 0
        mode = "same-cluster"
        lifecycle_available = True

        async def capture_vm_teardown_identity(self, job_id):
            from orchestrator.services.vm_provisioner import VMTeardownIdentity

            assert job_id == str(owner)
            return VMTeardownIdentity(
                provision_generation=identity["generation"],
                vm_uid=identity["vm_uid"],
                rootdisk_pvc_uid=identity["pvc_uid"],
                ssh_host="10.42.0.91", ssh_port=22,
            )

        async def release_vm_captured(self, job_id, captured, **kwargs):
            self.calls += 1
            assert job_id == str(owner) and kwargs["purge_disk"] is False
            assert captured.provision_generation == identity["generation"]
            if self.calls == 1:
                return SimpleNamespace(disposition="identity_unknown")
            await db.execute(
                "INSERT INTO managed_repository_process_zero_receipts "
                "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
                "VALUES('job',$1,'vm','vm',$2) ON CONFLICT DO NOTHING",
                owner, identity["generation"],
            )
            return SimpleNamespace(disposition="completed")

        async def attest_vm_idle_stop(self, captured):
            return {
                "version": 1, "kind": "vm_idle_physical_stop",
                "operation_id": str(captured["id"]), **identity,
                "vm_absent": True, "vmi_absent": True,
                "launcher_absent": True, "same_generation_replacement": False,
                "retained_pvc": True, "controller_authenticated": True,
            }

    provisioner = Provisioner()
    service = module.VMIdleLifecycleService(db, provisioner, object())
    assert await service.reconcile_once() == 0
    assert await db.fetchval("SELECT phase FROM vm_idle_operations WHERE id=$1", operation["id"]) == "release_held"
    await db.execute("UPDATE vm_idle_operations SET retry_after=clock_timestamp() WHERE id=$1", operation["id"])
    assert await service.reconcile_once() == 1
    assert provisioner.calls == 2
    assert await db.fetchval("SELECT phase FROM vm_idle_operations WHERE id=$1", operation["id"]) == "suspended"


@pytest.mark.asyncio
async def test_access_wake_shares_one_successor_with_execution_resume(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    op = await store.admit_release(str(owner), episode_id=idle.episode_id,
                                   revision=idle.revision, identity=identity)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop", "operation_id": str(op["id"]),
        **identity, "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(op["id"]), evidence=evidence)
    access = await store.request_wake(str(owner), execution_requested=False)
    assert access["phase"] == "waking"
    assert not access["wake_execution_requested"]
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "waiting_for_reply"
    execution = await store.request_wake(str(owner), execution_requested=True)
    assert execution["wake_id"] == access["wake_id"]
    assert execution["wake_generation"] == access["wake_generation"]
    assert execution["wake_request_id"] == access["wake_request_id"]
    assert execution["wake_execution_requested"]
    assert await db.fetchval("SELECT status FROM jobs WHERE id=$1", owner) == "waiting_for_reply"


@pytest.mark.asyncio
async def test_native_creation_requires_exact_idle_wake_operation(db, monkeypatch):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_request import build_vm_creation_request
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    op = await store.admit_release(str(owner), episode_id=idle.episode_id,
                                   revision=idle.revision, identity=identity)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop", "operation_id": str(op["id"]),
        **identity, "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(op["id"]), evidence=evidence)
    wake = await store.request_wake(str(owner), execution_requested=False)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    intent = {
        "owner_kind": "job", "owner_id": str(owner),
        "provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"], "pvc_uid": identity["pvc_uid"],
        "purge_disk": False, "resource": "vm_workspace", "source": "vm_idle_release",
    }
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'vm_idle_release',$4,$5,clock_timestamp(),'completed')",
        uuid4(), owner, UUID(identity["pvc_uid"]), uuid4(), cleanup_intent_digest(intent),
    )
    fresh = VMProvisioner._fresh_provision_ctx()
    fresh["provision_generation"] = str(wake["wake_generation"])
    request = build_vm_creation_request(
        job_id=str(owner), agent_config="worker_base", vm_image=None,
        cpu_cores=8, memory="16Gi", description="", network_tier="restricted",
        provision_generation=fresh["provision_generation"],
    )
    preflight = VMCreationPreflightStore(db)
    with pytest.raises(VMCreationRetryConflict):
        await preflight.begin(job_id=str(owner), request=request, fresh_context=fresh)
    with pytest.raises(VMCreationRetryConflict):
        await preflight.begin(job_id=str(owner), request=request, fresh_context=fresh,
                              idle_wake_id=str(uuid4()))
    admitted = await preflight.begin(job_id=str(owner), request=request,
                                     fresh_context=fresh, idle_wake_id=str(wake["id"]))
    assert admitted["request_id"] == str(wake["wake_request_id"])
    assert admitted["request"]["provision_generation"] == str(wake["wake_generation"])
    row = await db.fetchrow("SELECT status,context FROM jobs WHERE id=$1", owner)
    assert row["status"] == "waiting_for_reply"
    context = json.loads(row["context"])
    assert context["last_vm"]["vm_uid"] == identity["vm_uid"]
    assert context["vm"]["idle_wake_operation_id"] == str(wake["id"])
    assert context["vm"]["creation_preflight"]["expected_pvc_uid"] == identity["pvc_uid"]
    from tests.test_vm_creation_configuration import controller
    from vm_controller.creation_configuration import resolve_creation_configuration

    claim = (await preflight.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(controller(), claim["request"])
    resolved["creation_retry_protocol"] = 1
    ledger = await preflight.complete_resolution(claim, resolved)
    assert ledger["request_id"] == wake["wake_request_id"]
    row = await db.fetchrow("SELECT status,workspace_idle_episode,context FROM jobs WHERE id=$1", owner)
    assert row["status"] == "waiting_for_reply"
    assert json.loads(row["workspace_idle_episode"])["episode_id"] == idle.episode_id
    assert json.loads(row["context"])["vm"]["creation_preflight"]["state"] == "admitted"


@pytest.mark.asyncio
async def test_access_only_wake_waits_for_exact_ready_and_never_resumes_job(db, monkeypatch):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_request import build_vm_creation_request
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    module = import_module("orchestrator.services.vm_idle_lifecycle")
    store = module.VMIdleLifecycleStore(db)
    op = await store.admit_release(str(owner), episode_id=idle.episode_id,
                                   revision=idle.revision, identity=identity)
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop", "operation_id": str(op["id"]),
        **identity, "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "same_generation_replacement": False, "retained_pvc": True,
        "controller_authenticated": True,
    }
    assert await store.complete_release(str(op["id"]), evidence=evidence)
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    wake = await store.request_wake(
        str(owner), execution_requested=False,
        access_kind="ide", access_claimant="user-1",
    )
    intent = {
        "owner_kind": "job", "owner_id": str(owner),
        "provision_generation": identity["generation"], "vm_uid": identity["vm_uid"],
        "pvc_uid": identity["pvc_uid"], "purge_disk": False,
        "resource": "vm_workspace", "source": "vm_idle_release",
    }
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'vm_idle_release',$4,$5,clock_timestamp(),'completed')",
        uuid4(), owner, UUID(identity["pvc_uid"]), uuid4(), cleanup_intent_digest(intent),
    )
    successor_vm_uid = str(uuid4())
    successor_vmi_uid = str(uuid4())
    successor_launcher_uid = str(uuid4())

    class Provisioner:
        mode = "same-cluster"
        lifecycle_available = True
        starts = 0

        async def create_vm(self, job_id, *, idle_wake_id):
            self.starts += 1
            assert idle_wake_id == str(op["id"])
            fresh = VMProvisioner._fresh_provision_ctx()
            fresh["provision_generation"] = str(wake["wake_generation"])
            request = build_vm_creation_request(
                job_id=job_id, agent_config="worker_base", vm_image=None,
                cpu_cores=8, memory="16Gi", description="", network_tier="restricted",
                provision_generation=fresh["provision_generation"],
            )
            return await VMCreationPreflightStore(db).begin(
                job_id=job_id, request=request, fresh_context=fresh,
                idle_wake_id=idle_wake_id,
            )

        async def attest_workspace_runtime(self, job_id):
            return SimpleNamespace(
                workspace_generation=str(wake["wake_generation"]),
                vm_uid=successor_vm_uid,
                vmi_uid=successor_vmi_uid,
                launcher_pod_uid=successor_launcher_uid,
                rootdisk_pvc_uid=identity["pvc_uid"],
            )

    provisioner = Provisioner()
    reservations = []

    async def reserve(operation):
        reservations.append((operation["id"], operation["wake_generation"]))
        return True

    service = module.VMIdleLifecycleService(
        db, provisioner, object(), before_first_start=reserve,
    )
    assert await service.reconcile_once() == 1
    assert provisioner.starts == 1
    assert reservations == [(op["id"], wake["wake_generation"])]
    assert await service.reconcile_once() == 0
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    context["vm"].update(
        status="ready", vm_uid=successor_vm_uid,
        vmi_uid=successor_vmi_uid, active_pod_uid=successor_launcher_uid,
        rootdisk_pvc_uid=identity["pvc_uid"],
    )
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    assert await service.reconcile_once() == 1
    assert await service.reconcile_once() == 1
    assert provisioner.starts == 1
    row = await db.fetchrow(
        "SELECT status,workspace_idle_episode FROM jobs WHERE id=$1", owner
    )
    assert row["status"] == "waiting_for_reply"
    assert json.loads(row["workspace_idle_episode"])["episode_id"] == idle.episode_id
    done = await store.get_operation(str(op["id"]))
    assert done["phase"] == "ready" and done["closed_at"] is not None
    lease = await db.fetchrow(
        "SELECT provision_generation,vm_uid,expires_at,max_expires_at FROM vm_idle_access_leases "
        "WHERE owner_id=$1 AND claimed_by='user-1'", owner,
    )
    assert lease["provision_generation"] == wake["wake_generation"]
    assert lease["vm_uid"] == UUID(successor_vm_uid)
    assert lease["expires_at"] < lease["max_expires_at"]
    assert await store.renew_access(str(owner), kind="ide", claimant="user-1")
    assert await store.close_access(str(owner), kind="ide", claimant="user-1") == 1


@pytest.mark.asyncio
async def test_wake_ready_execution_enqueue_and_close_are_one_replayable_commit(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    op = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)", owner, identity["generation"],
    )
    generation, vm_uid = str(uuid4()), str(uuid4())
    await db.execute(
        "UPDATE vm_idle_operations SET phase='waking',stop_evidence='{}'::jsonb,"
        "stop_verified_at=clock_timestamp(),wake_id=$2,wake_generation=$3,"
        "wake_request_id=$4,wake_requested=true,wake_execution_requested=true,"
        "wake_ready_at=clock_timestamp() WHERE id=$1",
        op["id"], uuid4(), UUID(generation), uuid4(),
    )
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    context["last_vm"] = context["vm"]
    context["vm"] = {
        "status": "ready", "provision_generation": generation,
        "vm_uid": vm_uid, "rootdisk_pvc_uid": identity["pvc_uid"],
        "idle_wake_operation_id": str(op["id"]),
    }
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    results = await asyncio.gather(
        store.finish_wake(str(op["id"])),
        store.finish_wake(str(op["id"])),
    )
    assert results == [True, True]
    row = await db.fetchrow(
        "SELECT status,context,workspace_idle_episode FROM jobs WHERE id=$1", owner
    )
    assert row["status"] == "paused" and row["workspace_idle_episode"] is None
    resume_id = json.loads(row["context"])["worker_resume_id"]
    assert await store.finish_wake(str(op["id"]))
    assert json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))["worker_resume_id"] == resume_id
    assert await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "queued"
    assert await db.fetchval("SELECT phase FROM vm_idle_operations WHERE id=$1", op["id"]) == "ready"
