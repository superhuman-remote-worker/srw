"""Profile selection and retained authority use the production PostgreSQL path."""

import json
from datetime import datetime
from uuid import uuid4

import asyncpg
import pytest

from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_network_profile import NETWORK_PROFILE
from tests.test_vm_creation_preflight_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    initial_job,
    candidate,
)


db = _db_fixture
IMAGE = "registry.example/srw-vm@sha256:" + "a" * 64


@pytest.mark.asyncio
async def test_new_ordinary_disk_freezes_selected_profile_and_lost_reply_replays(db, monkeypatch):
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    job = await initial_job(db)
    request, fresh = candidate(job)
    request["vm_image"] = IMAGE
    store = VMCreationPreflightStore(db)
    first = await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    assert first["expected_pvc_uid"] is None
    assert first["request"]["network_profile"] == NETWORK_PROFILE
    assert first["request_digest"] == canonical_request_digest(first["request"])
    assert "network_profile" not in request
    # A lost response reads the frozen preflight and cannot select a new image
    # or recompute a changed digest from today's operator settings.
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    replay = await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    assert replay == first


@pytest.mark.asyncio
async def test_default_off_and_mutable_image_leave_ordinary_first_create_unchanged(db, monkeypatch):
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    job = await initial_job(db)
    request, fresh = candidate(job)
    request["vm_image"] = IMAGE
    first = await VMCreationPreflightStore(db).begin(
        job_id=str(job), request=request, fresh_context=fresh
    )
    assert "network_profile" not in first["request"]
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    other = await initial_job(db)
    mutable, fresh = candidate(other)
    mutable["vm_image"] = "registry.example/srw-vm:latest"
    second = await VMCreationPreflightStore(db).begin(
        job_id=str(other), request=mutable, fresh_context=fresh
    )
    assert "network_profile" not in second["request"]


@pytest.mark.asyncio
async def test_native_instance_profile_is_closed_and_immutable(db):
    user = await db.fetchval(
        "INSERT INTO users(display_name,is_approved) VALUES('Profile owner',TRUE) RETURNING id"
    )
    uid = uuid4()
    state = {"storage": {"uid": str(uid), "generation": 1, "pvc_uid": None,
                         "owner_id": str(uuid4()), "owner_kind": "job"},
             "network_profile": NETWORK_PROFILE}
    await db.execute(
        "INSERT INTO srw_workspace_instances "
        "(id,owner_id,recipe,revision,pvc_name,generation,backend_state) "
        "VALUES($1,$2,$3::jsonb,'profile',$4,1,$5::jsonb)",
        uid, user, json.dumps({"backend": "vm", "retention": "Retain"}),
        "srw-ws-" + uid.hex, json.dumps(state),
    )
    before = await db.fetchval(
        "SELECT backend_state FROM srw_workspace_instances WHERE id=$1", uid
    )
    for altered in (
        {"storage": state["storage"]},
        {**state, "network_profile": {**NETWORK_PROFILE, "interface": "eth0"}},
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE srw_workspace_instances SET backend_state=$2::jsonb WHERE id=$1",
                uid, json.dumps(altered),
            )
    assert await db.fetchval(
        "SELECT backend_state FROM srw_workspace_instances WHERE id=$1", uid
    ) == before
    legacy_uid = uuid4()
    await db.execute(
        "INSERT INTO srw_workspace_instances "
        "(id,owner_id,recipe,revision,pvc_name,generation,backend_state) "
        "VALUES($1,$2,$3::jsonb,'legacy',$4,1,$5::jsonb)",
        legacy_uid, user, json.dumps({"backend": "vm", "retention": "Retain"}),
        "srw-ws-" + legacy_uid.hex,
        json.dumps({"storage": {**state["storage"], "uid": str(legacy_uid)}}),
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE srw_workspace_instances SET backend_state=backend_state || $2::jsonb WHERE id=$1",
            legacy_uid, json.dumps({"network_profile": NETWORK_PROFILE}),
        )


@pytest.mark.asyncio
async def test_idle_release_holds_legacy_and_unproven_profile_before_stop(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from tests.test_vm_idle_lifecycle_real_postgres import seed_wait, _schema

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_NETWORK_PROFILE_ENABLED": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": IMAGE,
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    store = VMIdleLifecycleStore(db)
    legacy, idle, identity = await seed_wait(db, proven_profile=False)
    assert await store.admit_release(
        str(legacy), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    assert await store.admit_release(
        str(legacy), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    assert json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", legacy))["vm"]["status"] == "ready"
    owner, idle, identity = await seed_wait(
        db, original_options={"vm_image": IMAGE, "network_profile": NETWORK_PROFILE},
        proven_profile=False,
    )
    context = json.loads(await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner))
    preflight = context["vm"]["creation_preflight"]
    admission = uuid4()
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'network-profile-test',$4,'test',clock_timestamp(),'completed')",
        admission, owner, identity["pvc_uid"], uuid4(),
    )
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
        "admission_deadline,creation_admission_id,state,observed_vm_uid,observed_pvc_uid,resolved_at) "
        "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,'succeeded',$12,$13,clock_timestamp())",
        preflight["request_id"], owner, identity["generation"], preflight["request_digest"],
        json.dumps(preflight["request"]), "sha256:" + "a" * 64,
        preflight["execution_id"], preflight["execution_revision"],
        preflight["execution_generation"], datetime.fromisoformat(preflight["admission_deadline"]),
        admission, identity["vm_uid"], identity["pvc_uid"],
    )
    assert await store.admit_release(
        str(owner), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    evidence = {
        "profile": NETWORK_PROFILE,
        "provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"],
        "pvc_uid": identity["pvc_uid"],
        "vmi_uid": identity["vmi_uid"],
        "launcher_uid": identity["launcher_uid"],
        "guest_boot_id": str(uuid4()),
        "cloud_init_instance_id": "i-profiled-idle",
        "cloud_init_cached_instance_id": "i-profiled-idle",
        "network_file_sha256": "a" * 64,
        "name_only_dhcp": True,
    }
    context["vm"]["network_profile_evidence"] = {
        key: value for key, value in evidence.items() if key != "network_file_sha256"
    }
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    assert await store.admit_release(
        str(owner), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    context["vm"]["network_profile_evidence"] = evidence
    await db.execute("UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context))
    operation = await store.admit_release(
        str(owner), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    )
    assert operation is not None and operation["phase"] == "releasing"


@pytest.mark.asyncio
async def test_idle_release_holds_forged_original_profile_lineage_before_stop(db, monkeypatch):
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
    from tests.test_vm_idle_lifecycle_real_postgres import seed_wait, _schema

    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_NETWORK_PROFILE_ENABLED": "false",
    }.items():
        monkeypatch.setenv(key, value)
    await _schema(db)
    owner, idle, identity = await seed_wait(db)
    vm = json.loads(await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner))
    prior = vm["creation_preflight"]
    legacy = {key: value for key, value in prior["request"].items() if key != "network_profile"}
    legacy["provision_generation"] = str(uuid4())
    admission = uuid4()
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'profile-lineage-test',$4,'test',clock_timestamp(),'completed')",
        admission, owner, identity["pvc_uid"], uuid4(),
    )
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,job_id,provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,execution_id,execution_revision,execution_generation,"
        "admission_deadline,creation_admission_id,state,observed_vm_uid,observed_pvc_uid,"
        "resolved_at,created_at) "
        "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,'succeeded',$12,$13,"
        "clock_timestamp(),clock_timestamp()-interval '1 hour')",
        uuid4(), owner, legacy["provision_generation"], canonical_request_digest(legacy),
        json.dumps(legacy), "sha256:" + "b" * 64, prior["execution_id"],
        prior["execution_revision"], prior["execution_generation"],
        datetime.fromisoformat(prior["admission_deadline"]), admission,
        uuid4(), identity["pvc_uid"],
    )
    assert await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=idle.episode_id,
        revision=idle.revision, identity=identity,
    ) is None
    assert json.loads(await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner))["status"] == "ready"
