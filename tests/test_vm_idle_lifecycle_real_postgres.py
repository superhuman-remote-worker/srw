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

from shared.workspace_idle_policy import (
    IdleEpisode, RuntimeIdentity, episode_document, read_episode,
)
from shared.vm_network_profile import NETWORK_PROFILE
from tests.test_workspace_idle_store_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)


db = _db_fixture
PROFILE_IMAGE = "registry.example/srw-vm@sha256:" + "a" * 64


@pytest.fixture(autouse=True)
def profiled_idle_image_policy(monkeypatch):
    monkeypatch.setenv(
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST",
        PROFILE_IMAGE + ",registry.example/vm@sha256:" + "a" * 64,
    )

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src/orchestrator/database/migrations/app/0270_vm_idle_lifecycle.sql"
)


async def seed_wait(db, *, original_options=None, attempts=0, job_id=None,
                    retained_storage=False, proven_profile=True):
    from orchestrator.services.vm_creation_request import build_vm_creation_request
    from shared.vm_creation_retry import canonical_request_digest
    from shared.vm_network_profile import NETWORK_PROFILE

    owner, generation, vm_uid, vmi_uid, launcher_uid, pvc_uid = (
        job_id or uuid4(), *(uuid4() for _ in range(5))
    )
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
    execution = await db.fetchrow(
        "SELECT id,revision,generation,created_at+interval '2 hours' AS deadline "
        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1", owner,
    )
    options = {
        "agent_config": "worker_base", "vm_image": PROFILE_IMAGE if proven_profile else None, "cpu_cores": 8,
        "memory": "16Gi", "description": "", "network_tier": "restricted",
        **(original_options or {}),
    }
    if proven_profile:
        options["network_profile"] = dict(NETWORK_PROFILE)
    if retained_storage:
        binding = {
            "uid": str(uuid4()), "generation": 1, "pvc_uid": None,
            "owner_id": str(owner), "owner_kind": "job",
        }
        options["workspace_storage"] = binding
        user = await db.fetchval(
            "INSERT INTO users(display_name,is_approved) "
            "VALUES('Idle retained workspace',TRUE) RETURNING id"
        )
        await db.execute(
            "INSERT INTO srw_workspace_instances "
            "(id,owner_id,recipe,revision,pvc_name,pvc_uid,generation,execution_id,backend_state) "
            "VALUES($1,$2,$3::jsonb,'retained',$4,$5,1,$6,$7::jsonb)",
            UUID(binding["uid"]), user,
            json.dumps({"backend": "vm", "retention": "Retain"}),
            "srw-ws-" + UUID(binding["uid"]).hex,
            str(pvc_uid), execution["id"], json.dumps({
                "storage": binding,
                **({"network_profile": NETWORK_PROFILE} if proven_profile else {}),
            }),
        )
        await db.execute(
            "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) "
            "VALUES($1,$2)", execution["id"], UUID(binding["uid"]),
        )
    request = build_vm_creation_request(
        job_id=str(owner), provision_generation=str(generation), **options,
    )
    preflight = {
        "version": 1, "request_id": str(uuid4()), "job_id": str(owner),
        "request": request, "request_digest": canonical_request_digest(request),
        "revision": 1, "attempt": 0, "state": "admitted",
        "execution_id": str(execution["id"]),
        "execution_revision": execution["revision"],
        "execution_generation": execution["generation"],
        "admission_deadline": execution["deadline"].isoformat(),
        "expected_pvc_uid": None,
    }
    vm.update(
        creation_preflight=preflight,
        creation_request_id=preflight["request_id"],
        provision_attempts=attempts,
    )
    if proven_profile:
        vm["network_profile_evidence"] = {
            "profile": NETWORK_PROFILE,
            "provision_generation": str(generation),
            "vm_uid": str(vm_uid),
            "pvc_uid": str(pvc_uid),
            "vmi_uid": str(vmi_uid),
            "launcher_uid": str(launcher_uid),
            "guest_boot_id": str(uuid4()),
            "cloud_init_instance_id": "i-profiled-idle",
            "cloud_init_cached_instance_id": "i-profiled-idle",
            "network_file_sha256": "a" * 64,
            "name_only_dhcp": True,
        }
        admission = uuid4()
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
            "VALUES($1,'job',$2,$3,'profiled-idle-fixture',$4,'test',clock_timestamp(),'completed')",
            admission, owner, pvc_uid, uuid4(),
        )
        await db.execute(
            "INSERT INTO vm_creation_retries "
            "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
            "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
            "admission_deadline,creation_admission_id,state,observed_vm_uid,observed_pvc_uid,resolved_at) "
            "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,'succeeded',$12,$13,clock_timestamp())",
            UUID(preflight["request_id"]), owner, generation, preflight["request_digest"],
            json.dumps(request), "sha256:" + "a" * 64,
            execution["id"], execution["revision"], execution["generation"],
            execution["deadline"], admission, vm_uid, pvc_uid,
        )
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        owner, json.dumps({"vm": vm}),
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
    # The shared PG fixture truncates jobs between tests; idle operations have
    # deliberately no Job FK and need their own isolation reset.
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations")


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
async def test_exhausted_or_unproven_creation_budget_keeps_usable_vm_before_stop(db, monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_PROVISION_MAX_ATTEMPTS": "3",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    exhausted, episode, identity = await seed_wait(db, attempts=3)
    assert await store.admit_release(
        str(exhausted), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    ) is None
    unknown, episode2, identity2 = await seed_wait(db)
    await db.execute(
        "UPDATE jobs SET context=context #- '{vm,creation_preflight}' WHERE id=$1", unknown,
    )
    assert await store.admit_release(
        str(unknown), episode_id=episode2.episode_id,
        revision=episode2.revision, identity=identity2,
    ) is None
    for owner in (exhausted, unknown):
        assert json.loads(
            await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner)
        )["status"] == "ready"
        assert await store.get_open_for_owner(str(owner)) is None


@pytest.mark.asyncio
async def test_changed_retained_storage_authority_holds_before_stop(db, monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, idle, identity = await seed_wait(db, retained_storage=True)
    await db.execute(
        "UPDATE srw_workspace_instances SET status='Detached' "
        "WHERE execution_id=(SELECT id FROM srw_execution_specs "
        "WHERE work_kind='Job' AND work_id=$1)",
        owner,
    )
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    assert await store.admit_release(
        str(owner), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    assert json.loads(
        await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner)
    )["status"] == "ready"
    assert await store.get_open_for_owner(str(owner)) is None


@pytest.mark.asyncio
async def test_active_child_and_access_hold_release_until_finished_or_expired(db, monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    child = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,parent_job_id,context) "
        "VALUES($1,'inherited child','processing',$2,$3::jsonb)",
        child, owner, json.dumps({"inherits_parent_workspace": True}),
    )
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)

    async def admit():
        return await store.admit_release(
            str(owner), episode_id=idle.episode_id,
            revision=idle.revision, identity=identity,
        )

    assert await admit() is None
    await db.execute("UPDATE jobs SET status='completed' WHERE id=$1", child)
    lease = await db.fetchval(
        "INSERT INTO vm_idle_access_leases "
        "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
        "expires_at,max_expires_at) "
        "VALUES('job',$1,$2,$3,'ide','live-user',"
        "clock_timestamp()+interval '2 minutes',clock_timestamp()+interval '1 hour') "
        "RETURNING id",
        owner, UUID(identity["generation"]), UUID(identity["vm_uid"]),
    )
    assert await admit() is None
    await db.execute(
        "UPDATE vm_idle_access_leases SET "
        "acquired_at=clock_timestamp()-interval '3 minutes',"
        "expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1",
        lease,
    )
    assert await admit() is not None


@pytest.mark.asyncio
async def test_access_renewal_and_reply_serialize_with_release_admission(db, monkeypatch):
    from shared.worker_queue import claim_worker_batch

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    store = import_module("orchestrator.services.vm_idle_lifecycle").VMIdleLifecycleStore(db)
    owner, idle, identity = await seed_wait(db)
    lease = await db.fetchval(
        "INSERT INTO vm_idle_access_leases "
        "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,"
        "expires_at,max_expires_at) "
        "VALUES('job',$1,$2,$3,'ide','live-user',"
        "clock_timestamp()+interval '2 minutes',clock_timestamp()+interval '1 hour') "
        "RETURNING id",
        owner, UUID(identity["generation"]), UUID(identity["vm_uid"]),
    )

    async def release(job_id, episode, runtime):
        return await store.admit_release(
            str(job_id), episode_id=episode.episode_id,
            revision=episode.revision, identity=runtime,
        )

    renewed, refused = await asyncio.gather(
        store.renew_access(str(owner), kind="ide", claimant="live-user"),
        release(owner, idle, identity),
    )
    assert renewed and refused is None
    await db.execute(
        "UPDATE vm_idle_access_leases SET "
        "acquired_at=clock_timestamp()-interval '3 minutes',"
        "expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1",
        lease,
    )
    expired_renewal, admitted = await asyncio.gather(
        store.renew_access(str(owner), kind="ide", claimant="live-user"),
        release(owner, idle, identity),
    )
    assert not expired_renewal and admitted is not None
    assert await claim_worker_batch(db, pod_name="access-race-worker") is None

    reply_owner, reply_idle, reply_identity = await seed_wait(db)
    reply, raced = await asyncio.gather(
        db.queue_stateless_job_for_resume(
            str(reply_owner), expected_status="waiting_for_reply",
        ),
        release(reply_owner, reply_idle, reply_identity),
    )
    assert reply
    if raced is not None:
        assert await claim_worker_batch(db, pod_name="reply-race-worker") is None
        assert await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", reply_owner,
        ) == "done"
    else:
        assert await store.get_open_for_owner(str(reply_owner)) is None
        assert json.loads(
            await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", reply_owner)
        )["status"] == "ready"


@pytest.mark.asyncio
async def test_nomination_pages_past_blocked_head_and_wraps(db, monkeypatch):
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    lower = []
    for index in range(1, 17):
        owner, idle, _ = await seed_wait(db, job_id=UUID(int=index))
        lower.append((owner, idle))
        await db.execute(
            "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
            owner, json.dumps({"route_id": str(uuid4())}),
        )
    high, high_idle, _ = await seed_wait(db, job_id=UUID(int=1000))

    class Provisioner:
        mode = "same-cluster"
        lifecycle_available = True

        async def _vm(self, job_id):
            return json.loads(
                await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", UUID(job_id))
            )

        async def capture_vm_teardown_identity(self, job_id):
            vm = await self._vm(job_id)
            return VMTeardownIdentity(
                vm["provision_generation"], vm["vm_uid"], vm["rootdisk_pvc_uid"]
            )

        async def attest_workspace_runtime(self, job_id):
            vm = await self._vm(job_id)
            return SimpleNamespace(
                workspace_generation=vm["provision_generation"],
                vm_uid=vm["vm_uid"], vmi_uid=vm["vmi_uid"],
                launcher_pod_uid=vm["active_pod_uid"],
                rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
            )

    module = import_module("orchestrator.services.vm_idle_lifecycle")
    service = module.VMIdleLifecycleService(db, Provisioner(), object())
    assert await service.nominate(limit=16) == 0
    assert await service.nominate(limit=16) == 1
    assert await service.store.get_open_for_owner(str(high)) is not None
    first, first_idle = lower[0]
    await db.execute(
        "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
        first, json.dumps({"route_id": first_idle.wait_key}),
    )
    assert await service.nominate(limit=16) == 1
    assert await service.store.get_open_for_owner(str(first)) is not None
    assert high_idle.wait_kind == first_idle.wait_kind == "human_message"


@pytest.mark.asyncio
async def test_pending_operation_scan_pages_past_long_waking_head(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    await _schema(db)
    ids = [UUID(int=10000 + index) for index in range(17)]
    for index, operation_id in enumerate(ids):
        await db.execute(
            "INSERT INTO vm_idle_operations("
            "id,owner_kind,owner_id,phase,episode_id,episode_revision,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind,"
            "wake_id,wake_generation,wake_request_id,wake_requested) "
            "VALUES($1,'job',$2,'waking',$3,1,$4,$5,$6,$7,$8,'rootdisk',"
            "$9,$10,$11,true)",
            operation_id, UUID(int=20000 + index), uuid4(),
            uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(),
        )
    module = import_module("orchestrator.services.vm_idle_lifecycle")
    service = module.VMIdleLifecycleService(db, object(), object())
    with pytest.raises(ValueError, match="Invalid idle scan limit"):
        await service.store.pending_operations(limit=65)
    assert await service.reconcile_once(limit=16) == 0
    assert await db.fetchval(
        "SELECT claim_token FROM vm_idle_operations WHERE id=$1", ids[-1]
    ) == 0
    assert await service.reconcile_once(limit=16) == 0
    assert await db.fetchval(
        "SELECT claim_token FROM vm_idle_operations WHERE id=$1", ids[-1]
    ) > 0


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
    # The shared PG fixture can retain earlier orphan queue units after its
    # jobs-only teardown. Drain them until this owner's queued unit is the
    # one the real worker claimant rejects; every claim must stay non-runnable.
    for _ in range(32):
        assert await claim_worker_batch(db, pod_name="idle-race-worker") is None
        if await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", owner) == "done":
            break
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
        job_id=str(owner), agent_config="worker_base", vm_image=PROFILE_IMAGE,
        cpu_cores=8, memory="16Gi", description="", network_tier="restricted",
        provision_generation=fresh["provision_generation"], network_profile=NETWORK_PROFILE,
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
@pytest.mark.parametrize("retained_storage", [False, True])
@pytest.mark.parametrize("lost_response", [False, True])
async def test_real_provisioner_wake_preserves_predecessor_and_effective_options(
    db, monkeypatch, retained_storage, lost_response,
):
    from copy import deepcopy

    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
    from shared.workspace_initialization import initialization_request

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, idle, identity = await seed_wait(
        db,
        original_options={
            "agent_config": "worker_custom",
            "vm_image": "registry.example/vm@sha256:" + "a" * 64,
            "cpu_cores": 3, "memory": "6Gi", "disk_size": "80Gi",
            "network_tier": "unrestricted", "description": "captured purpose",
            "initialization": initialization_request(
                [{"command": ["/usr/bin/true"]}]
            ),
        },
        retained_storage=retained_storage,
    )
    module = import_module("orchestrator.services.vm_idle_lifecycle")
    store = module.VMIdleLifecycleStore(db)
    original = deepcopy(json.loads(
        await db.fetchval("SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1", owner)
    ))
    op = await store.admit_release(
        str(owner), episode_id=idle.episode_id, revision=idle.revision, identity=identity
    )
    assert op is not None
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
    wake = await store.request_wake(str(owner), execution_requested=False)
    provisioner = VMProvisioner()
    provisioner._db = db
    service = module.VMIdleLifecycleService(
        db, provisioner, object(), before_first_start=lambda _op: True,
    )
    if lost_response:
        begin = VMCreationPreflightStore.begin

        async def lose_after_commit(self, *args, **kwargs):
            await begin(self, *args, **kwargs)
            raise RuntimeError("preflight response lost after commit")

        monkeypatch.setattr(VMCreationPreflightStore, "begin", lose_after_commit)
        assert await service.reconcile_once() == 0
        assert (await store.get_operation(str(op["id"])))["phase"] == "wake_held"
        monkeypatch.setattr(VMCreationPreflightStore, "begin", begin)
    else:
        assert await service.reconcile_once() == 1
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    successor = context["vm"]["creation_preflight"]
    assert context["last_vm"]["creation_preflight"] == original
    assert successor["request_id"] == str(wake["wake_request_id"])
    assert successor["request"]["provision_generation"] == str(wake["wake_generation"])
    original_options = {
        key: value for key, value in original["request"].items()
        if key != "provision_generation"
    }
    successor_options = {
        key: value for key, value in successor["request"].items()
        if key != "provision_generation"
    }
    if retained_storage:
        assert original_options["workspace_storage"]["pvc_uid"] is None
        assert successor_options["workspace_storage"]["pvc_uid"] == identity["pvc_uid"]
        successor_options["workspace_storage"]["pvc_uid"] = None
    assert successor_options == original_options
    replay = await provisioner.create_vm(str(owner), idle_wake_id=str(op["id"]))
    assert replay["request_id"] == successor["request_id"]
    assert json.loads(
        await db.fetchval("SELECT context->'last_vm'->'creation_preflight' FROM jobs WHERE id=$1", owner)
    ) == original
    if not retained_storage and not lost_response:
        successor_identity = {
            "generation": str(wake["wake_generation"]),
            "vm_uid": str(uuid4()), "vmi_uid": str(uuid4()),
            "launcher_uid": str(uuid4()), "pvc_uid": identity["pvc_uid"],
        }
        context["vm"].update(
            status="ready", provision_attempts=0, identity_authenticated=True,
            identity_provision_generation=successor_identity["generation"],
            creation_request_id=successor["request_id"],
            vm_uid=successor_identity["vm_uid"],
            vmi_uid=successor_identity["vmi_uid"],
            active_pod_uid=successor_identity["launcher_uid"],
            rootdisk_pvc_uid=successor_identity["pvc_uid"],
            ssh_host="10.42.0.92", ssh_port=22,
            ssh_ready_source="provisioner_probe",
            ssh_host_key_fingerprint="SHA256:" + "B" * 43,
        )
        context["vm"]["creation_preflight"]["state"] = "admitted"
        context["vm"]["network_profile_evidence"] = {
            **context["last_vm"]["network_profile_evidence"],
            "provision_generation": successor_identity["generation"],
            "vm_uid": successor_identity["vm_uid"],
            "vmi_uid": successor_identity["vmi_uid"],
            "launcher_uid": successor_identity["launcher_uid"],
        }
        context.pop("_vm_creation_pending", None)
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context),
        )
        successor_admission = uuid4()
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
            "VALUES($1,'job',$2,$3,'profiled-idle-fixture',$4,'test',clock_timestamp(),'completed')",
            successor_admission, owner, UUID(successor_identity["pvc_uid"]), uuid4(),
        )
        await db.execute(
            "INSERT INTO vm_creation_retries "
            "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
            "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
            "admission_deadline,expected_pvc_uid,creation_admission_id,state,observed_vm_uid,observed_pvc_uid,resolved_at) "
            "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,'succeeded',$13,$14,clock_timestamp())",
            UUID(successor["request_id"]), owner, UUID(successor_identity["generation"]),
            successor["request_digest"], json.dumps(successor["request"]),
            "sha256:" + "a" * 64, UUID(successor["execution_id"]),
            successor["execution_revision"], successor["execution_generation"],
            datetime.fromisoformat(successor["admission_deadline"]),
            UUID(successor_identity["pvc_uid"]), successor_admission,
            UUID(successor_identity["vm_uid"]), UUID(successor_identity["pvc_uid"]),
        )
        assert await store.mark_wake_ready(str(op["id"]), **successor_identity)
        assert await store.finish_wake(str(op["id"]))
        rebound = await db.fetchrow(
            "SELECT workspace_idle_episode,workspace_idle_revision FROM jobs WHERE id=$1",
            owner,
        )
        next_idle = read_episode(
            json.loads(rebound["workspace_idle_episode"]),
            revision=rebound["workspace_idle_revision"],
        )
        second = await store.admit_release(
            str(owner), episode_id=next_idle.episode_id,
            revision=next_idle.revision, identity=successor_identity,
        )
        assert second is not None
        await db.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
            "VALUES('job',$1,'vm','vm',$2)",
            owner, successor_identity["generation"],
        )
        second_evidence = {
            **evidence, "operation_id": str(second["id"]), **successor_identity,
        }
        assert await store.complete_release(str(second["id"]), evidence=second_evidence)
        second_intent = {
            **intent, "provision_generation": successor_identity["generation"],
            "vm_uid": successor_identity["vm_uid"],
        }
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions "
            "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
            "VALUES($1,'job',$2,$3,'vm_idle_release',$4,$5,clock_timestamp(),'completed')",
            uuid4(), owner, UUID(identity["pvc_uid"]), uuid4(),
            cleanup_intent_digest(second_intent),
        )
        second_wake = await store.request_wake(str(owner), execution_requested=False)
        assert second_wake is not None
        second_create = await provisioner.create_vm(
            str(owner), idle_wake_id=str(second["id"]),
        )
        assert second_create["request_id"] == str(second_wake["wake_request_id"])
        second_context = json.loads(
            await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
        )
        assert second_context["vm"]["provision_generation"] == str(
            second_wake["wake_generation"]
        )
        assert second_context["last_vm"]["provision_generation"] == successor_identity[
            "generation"
        ]
        with pytest.raises(VMCreationRetryConflict, match="idle_wake_unproven"):
            await provisioner.create_vm(str(owner), idle_wake_id=str(op["id"]))


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
                job_id=job_id, agent_config="worker_base", vm_image=PROFILE_IMAGE,
                cpu_cores=8, memory="16Gi", description="", network_tier="restricted",
                provision_generation=fresh["provision_generation"], network_profile=NETWORK_PROFILE,
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
