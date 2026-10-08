"""Legacy pure-VM completion keeps one exact cleanup authority on real PG."""

from functools import partial
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.completion_effects import (
    CompletionEffectDependencies,
    run_completion_workspace_teardown,
)
from orchestrator.services.completion_teardown_replay import (
    legacy_completion_cleanup_identity,
)
from orchestrator.services.thread_retirement import archive_and_cleanup_workspace
from orchestrator.services.vm_provisioner import VMTeardownResult
from orchestrator.services.vm_workspace_policy import vm_needs_release
from orchestrator.services.vm_workspace_recovery_store import acquire_vm_cleanup_permit
from tests.test_vm_ready_purge_cancel_replay_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _schema_applied,
    _pre_ssh_db,
    _retention_db,
    _resume_db,
    db as _ready_db,
    enabled,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
    ready_source,
)


db = _ready_db


async def failed_ready(db, *, legacy=True):
    state = await ready_source(db)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE jobs SET status='failed',context=context-'_stateless_cancel_cleanup_pending' "
        "WHERE id=$1",
        owner,
    )
    await db.execute(
        "UPDATE run_queue SET state='done',leased_by=NULL,leased_until=NULL,"
        "attempts_since_completion=0,lease_token=5 WHERE unit_id=$1",
        owner,
    )
    if legacy:
        request, digest = legacy_completion_cleanup_identity(owner)
        state["legacy"] = await state["recovery"].acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner,
            pvc_uid=None,
            request_id=request,
            intent_digest=digest,
            source="completion_workspace_teardown",
        )
        assert state["legacy"].allowed
    return state


async def exact_cleanup(state):
    return await acquire_vm_cleanup_permit(
        state["recovery"],
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="job_terminal_vm_release",
        purge_disk=True,
    )


@pytest.mark.asyncio
async def test_stuck_legacy_parent_transfers_to_exact_cleanup_without_physical_completion(
    db,
):
    state = await failed_ready(db)
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["legacy"].admission_id,
        )
    )
    permit = await exact_cleanup(state)
    assert permit.allowed, permit.reason
    assert permit.admission_id != state["legacy"].admission_id
    old = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["legacy"].admission_id,
        )
    )
    assert (
        old.pop("outcome")
        == "superseded_before_issue:" + permit.parent_cleanup["request_id"]
    )
    assert old.pop("completed_at") is not None
    assert old == {
        key: value
        for key, value in before.items()
        if key not in {"outcome", "completed_at"}
    }
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_cleanup_stop_receipts") == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    assert await exact_cleanup(state) == permit


@pytest.mark.asyncio
async def test_stuck_legacy_parent_is_nominated_for_native_terminal_replay(db):
    state = await failed_ready(db)
    rows = await db.list_terminal_vm_cleanup_jobs()
    assert state["job_id"] in {row["id"] for row in rows}


@pytest.mark.asyncio
async def test_fresh_legacy_completion_uses_real_archive_exact_parent_without_self_block(
    db,
):
    state = await failed_ready(db, legacy=False)
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(
            return_value=VMTeardownResult("retry_pending", False)
        ),
    )
    archive_deps = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )
    result = await run_completion_workspace_teardown(
        state["job_id"],
        None,
        dependencies=CompletionEffectDependencies(
            store=db,
            container_provisioner=None,
            vm_provisioner=provisioner,
            get_container_context=archive_deps.get_container_context,
            get_vm_context=archive_deps.get_vm_context,
            archive_and_cleanup_workspace=partial(
                archive_and_cleanup_workspace, dependencies=archive_deps
            ),
            s36_exact_absence_timeout_seconds=lambda: 0,
            logger=logging.getLogger(__name__),
            recovery_store=state["recovery"],
        ),
    )
    assert provisioner.release_vm_captured.await_count == 1, result
    rows = await db.fetch(
        "SELECT source,pvc_uid FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 AND completed_at IS NULL",
        UUID(state["job_id"]),
    )
    assert [dict(row) for row in rows] == [
        {
            "source": "job_terminal_vm_release",
            "pvc_uid": UUID(state["frozen"]["pvc_uid"]),
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "cancelled",
        "paused",
        "generation",
        "container",
        "storage",
        "pending_resume",
        "ready_late",
        "child",
        "process_zero",
    ],
)
async def test_uncertain_or_nonordinary_legacy_authority_never_moves(db, fault):
    from uuid import uuid4

    state = await failed_ready(db, legacy=fault != "ready_late")
    owner = UUID(state["job_id"])
    if fault in {"cancelled", "paused"}:
        await db.execute("UPDATE jobs SET status=$2 WHERE id=$1", owner, fault)
    elif fault in {"generation", "container", "storage", "pending_resume"}:
        context = json.loads(
            await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
        )
        if fault == "generation":
            from dataclasses import replace

            state["identity"] = replace(
                state["identity"], provision_generation=str(uuid4())
            )
        elif fault == "container":
            context["workspace_container"] = {
                "status": "deleted",
                "provisioner": "docker",
            }
        elif fault == "storage":
            context["vm"]["workspace_storage"] = {}
        else:
            context["_stateless_resume_pending"] = True
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
        )
    elif fault == "ready_late":
        request, digest = legacy_completion_cleanup_identity(owner)
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,admitted_at) "
            "SELECT $1,'job',$2,NULL,'completion_workspace_teardown',$3,$4,ready_at-interval '1 second' "
            "FROM vm_creation_retries WHERE job_id=$2",
            uuid4(),
            owner,
            request,
            digest,
        )
        state["legacy"] = await state["recovery"].acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner,
            pvc_uid=None,
            request_id=request,
            intent_digest=digest,
            source="completion_workspace_teardown",
        )
    elif fault == "child":
        await db.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,parent_admission_id) "
            "VALUES($1,'job',$2,NULL,'uncertain_legacy_child',$3,$4,$5)",
            uuid4(),
            owner,
            uuid4(),
            "sha256:" + "f" * 64,
            state["legacy"].admission_id,
        )
    else:
        assert await db.record_managed_repository_workspace_process_zero(
            state["job_id"],
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=state["generation"],
        )
    before = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 ORDER BY id",
            owner,
        )
    ]
    assert not (await exact_cleanup(state)).allowed
    assert [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 ORDER BY id",
            owner,
        )
    ] == before
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_resource_prepare_refusal_rolls_back_old_close_new_parent_and_charge(
    db, monkeypatch
):
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services import vm_workspace_recovery_store as module

    state = await failed_ready(db)
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["legacy"].admission_id,
        )
    )
    original = module.prepare_vm_cleanup_resource

    async def fail_after_prepare(*args, **kwargs):
        await original(*args, **kwargs)
        raise ResourceAdmissionError("simulated_post_prepare_refusal")

    monkeypatch.setattr(module, "prepare_vm_cleanup_resource", fail_after_prepare)
    assert not (await exact_cleanup(state)).allowed
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                state["legacy"].admission_id,
            )
        )
        == before
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 AND source='job_terminal_vm_release'",
            UUID(state["job_id"]),
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "active"
    )


@pytest.mark.asyncio
async def test_duplicate_callers_converge_on_one_successor(db):
    import asyncio

    state = await failed_ready(db)
    results = await asyncio.gather(exact_cleanup(state), exact_cleanup(state))
    assert all(item.allowed for item in results)
    assert results[0].admission_id == results[1].admission_id
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 AND source='job_terminal_vm_release'",
            UUID(state["job_id"]),
        )
        == 1
    )


@pytest.mark.asyncio
async def test_owner_recheck_after_lock_wait_refuses_new_cancel(db):
    import asyncio

    state = await failed_ready(db)
    owner = UUID(state["job_id"])
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        pending = asyncio.create_task(exact_cleanup(state))
        await asyncio.sleep(0.05)
        assert not pending.done()
        await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", owner)
    assert not (await pending).allowed
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["legacy"].admission_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_delayed_supersession_caller_replays_existing_exact_successor(db):
    from orchestrator.services.vm_legacy_completion_cleanup import (
        supersede_unissued_legacy_completion,
    )

    state = await failed_ready(db)
    first = await exact_cleanup(state)
    # Both callers may have observed the original refusal before the first
    # transaction commits. The delayed transfer must replay, not move again.
    delayed = await supersede_unissued_legacy_completion(
        state["recovery"],
        owner_id=state["job_id"],
        identity=state["identity"],
    )
    assert delayed == first


@pytest.mark.asyncio
async def test_native_settlement_then_repeated_archive_and_public_delete(
    db, monkeypatch
):
    from contextlib import asynccontextmanager
    from orchestrator.services.job_mutation_controls import JobControlOperations
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )

    state = await failed_ready(db)
    permit = await exact_cleanup(state)
    assert permit.allowed
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["generation"],
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"

    async def absent(job_id, generation, **kwargs):
        assert (job_id, generation) == (state["job_id"], state["generation"])
        return _VMTeardownProbe(
            disposition="absent",
            identity=VMTeardownIdentity(generation, None, None),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )

    # Only external authenticated absence is simulated. Capture, completed
    # probe, attestation, release, archive and public Delete are production code.
    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=absent)
    deps = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )
    archive = partial(archive_and_cleanup_workspace, dependencies=deps)
    assert "vm released" in await archive(state["job_id"])
    owner = UUID(state["job_id"])
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "released"
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
            state["legacy"].admission_id,
        )
        == 0
    )
    assert (
        await db.fetchval(
            "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", owner
        )
        == "deleted"
    )
    await archive(state["job_id"])

    @asynccontextmanager
    async def vector_connection():
        yield SimpleNamespace(execute=AsyncMock())

    control = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=archive,
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vector_connection),
            resolve_job_notifications=AsyncMock(),
        )
    )
    job = await db.get_job(state["job_id"])
    result = await control.delete(
        state["job_id"], caller={"id": str(job["user_id"])}, job=job
    )
    assert result["status"] == "deleted"
    assert await db.get_job(state["job_id"]) is None
    assert (
        await db.fetchval(
            "SELECT outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["legacy"].admission_id,
        )
        == "superseded_before_issue:" + permit.parent_cleanup["request_id"]
    )
    assert (
        await db.fetchval(
            "SELECT outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
        == "completed"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [True, False])
async def test_repeated_legacy_s36_does_not_complete_a_logical_supersession(db, legacy):
    state = await failed_ready(db, legacy=legacy)
    assert (await exact_cleanup(state)).allowed
    assert await db.record_managed_repository_workspace_process_zero(
        state["job_id"],
        owner_kind="job",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=state["generation"],
    )
    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=state["identity"]),
        release_vm_captured=AsyncMock(
            return_value=VMTeardownResult("retry_pending", False)
        ),
    )
    archive_deps = SimpleNamespace(
        store=db,
        recovery_store=state["recovery"],
        vm_provisioner=provisioner,
        container_provisioner=None,
        docker_provisioner=None,
        get_container_context=lambda _: {},
        get_vm_context=lambda job: json.loads(job["context"])["vm"],
        vm_needs_release=vm_needs_release,
    )
    deps = CompletionEffectDependencies(
        store=db,
        container_provisioner=None,
        vm_provisioner=provisioner,
        get_container_context=archive_deps.get_container_context,
        get_vm_context=archive_deps.get_vm_context,
        archive_and_cleanup_workspace=partial(
            archive_and_cleanup_workspace, dependencies=archive_deps
        ),
        s36_exact_absence_timeout_seconds=lambda: 0,
        logger=logging.getLogger(__name__),
        recovery_store=state["recovery"],
    )
    for _ in range(2):
        result = await run_completion_workspace_teardown(
            state["job_id"], None, dependencies=deps
        )
        assert result["teardown_disposition"] == "retry_pending"
        assert "VM exact teardown remains retry_pending" in result["error"]
    assert provisioner.release_vm_captured.await_count == 2
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        == "teardown"
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_cleanup_stop_receipts") == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["request", "digest", "source", "access", "recovery", "charge_teardown"]
)
async def test_forged_or_held_legacy_admission_does_not_supersede(db, fault):
    from uuid import uuid4

    state = await failed_ready(db, legacy=False)
    owner = UUID(state["job_id"])
    request, digest = legacy_completion_cleanup_identity(owner)
    state["legacy"] = await state["recovery"].acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner,
        pvc_uid=None,
        request_id=uuid4() if fault == "request" else request,
        intent_digest="sha256:" + "a" * 64 if fault == "digest" else digest,
        source="foreign_completion"
        if fault == "source"
        else "completion_workspace_teardown",
    )
    assert state["legacy"].allowed
    if fault == "access":
        await db.execute(
            "INSERT INTO vm_idle_access_leases(id,owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,expires_at,max_expires_at) "
            "VALUES($1,'job',$2,$3,$4,'ide','test',clock_timestamp()+interval '1 minute',clock_timestamp()+interval '2 minutes')",
            uuid4(),
            owner,
            UUID(state["generation"]),
            UUID(state["frozen"]["vm_uid"]),
        )
    elif fault == "recovery":
        from tests.test_vm_workspace_recovery_real_postgres import insert_recovery

        await insert_recovery(db, owner_id=owner)
    elif fault == "charge_teardown":
        await db.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
            UUID(state["reservation_id"]),
        )
    before = [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 ORDER BY id",
            owner,
        )
    ]
    assert not (await exact_cleanup(state)).allowed
    assert [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 ORDER BY id",
            owner,
        )
    ] == before
    assert (
        await db.fetchval("SELECT count(*) FROM vm_resource_cleanup_stop_receipts") == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projection",
    ["retiring", "deleted", "last_vm", "retained_storage", "retained_resume"],
)
async def test_terminal_vm_wrapper_delegates_to_archive_without_broad_admission(
    projection,
):
    context = {"vm": {"status": "ready", "identity_authenticated": True}}
    if projection in {"retiring", "deleted"}:
        context["vm"]["status"] = (
            "retiring_process_zero" if projection == "retiring" else "deleted"
        )
    elif projection == "last_vm":
        context["last_vm"] = {"status": "deleted"}
    elif projection == "retained_storage":
        context["vm"]["workspace_storage"] = {"untrusted_hint": True}
    else:
        context["_vm_job_retained_resume"] = {"untrusted_hint": True}
    job = {
        "status": "failed",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "parent_job_id": None,
        "context": context,
    }
    archive = AsyncMock(
        side_effect=RuntimeError("exact archive authority remains held")
    )
    recovery = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(
            side_effect=AssertionError("must not create broad wrapper")
        )
    )
    result = await run_completion_workspace_teardown(
        "00000000-0000-0000-0000-000000000001",
        None,
        dependencies=CompletionEffectDependencies(
            store=SimpleNamespace(get_job=AsyncMock(return_value=job)),
            container_provisioner=None,
            vm_provisioner=None,
            get_container_context=lambda _: {},
            get_vm_context=lambda j: j["context"]["vm"],
            archive_and_cleanup_workspace=archive,
            s36_exact_absence_timeout_seconds=lambda: 0,
            logger=logging.getLogger(__name__),
            recovery_store=recovery,
        ),
    )
    archive.assert_awaited_once()
    recovery.acquire_cleanup_permit.assert_not_awaited()
    assert result["error"] == "exact archive authority remains held"
    assert result["teardown_disposition"] == "retry_pending"
