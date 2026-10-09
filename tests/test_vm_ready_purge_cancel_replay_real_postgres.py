"""Replay genuine Ready terminal purges without entering never-Ready retention.

The production selector, archive, cleanup store and charge SQL are real.
External VM stop/absence evidence is simulated and is not physical proof.
"""

import asyncio
from contextlib import asynccontextmanager
from functools import partial
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services import thread_retirement
from orchestrator.services.job_mutation_controls import JobControlOperations
from orchestrator.services.vm_workspace_policy import vm_needs_release
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    vm_cleanup_request_identity,
)
from tests._connector_lease_migrations import is_lease_table_migration
from tests.test_vm_job_cancel_retention_real_postgres import acquire, cancelled, child
from tests.test_vm_job_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _pre_ssh_db,  # noqa: F401
    _retention_db,  # noqa: F401
    db as _resume_db,
    enabled,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    retention_schema,  # noqa: F401
    resume_schema,  # noqa: F401
    whole_schema,  # noqa: F401
)

db = _resume_db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn, tmp_path_factory):  # noqa: F811
    """Seed genuinely historical True parents before the 0340 commit guard."""
    from orchestrator.database.migrate import run_migrations

    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path_factory.mktemp("ready-purge-pre0340")
    for path in migrations.glob("*.sql"):
        # Today's delete revokes credential leases (C2), so the old head
        # carries the lease tables the code it runs needs.
        if path.name.split("_", 1)[0] <= "0339" or is_lease_table_migration(path.name):
            (stage / path.name).write_bytes(path.read_bytes())
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
    try:
        await run_migrations(pool, stage)
        assert not await pool.fetchval(
            "SELECT to_regprocedure('public.vm_job_initial_ready_retention_candidate(uuid,uuid,uuid,uuid,uuid,uuid,boolean)') IS NOT NULL"
        )
    finally:
        await pool.close()


async def ready_source(db):
    state = await cancelled(db, old=False, retiring=False)
    owner = UUID(state["job_id"])
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        owner,
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm'||$2::jsonb) WHERE id=$1",
        owner,
        json.dumps(
            {"status": "ready", "active_pod_uid": state["frozen"]["launcher_uid"]}
        ),
    )
    # This fixture deliberately admits the historical True parent using the
    # pre-0340 schema. Fresh Ready Cancel selection now holds until the new
    # retained authority is available, even while that schema is absent.
    selected = await acquire(state)
    assert selected is not None and not selected.allowed
    return state


async def ready_purge(db, *, process_zero=True):
    state = await ready_source(db)
    owner = UUID(state["job_id"])
    state["cleanup_permit"] = await acquire_vm_cleanup_permit(
        state["recovery"],
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="job_terminal_vm_release",
        purge_disk=True,
    )
    assert state["cleanup_permit"].allowed
    state["parent_before"] = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["cleanup_permit"].admission_id,
        )
    )
    assert await db.fetchval(
        "SELECT r.ready_at < c.admitted_at FROM vm_creation_retries r "
        "JOIN vm_workspace_cleanup_admissions c ON c.id=$2 WHERE r.job_id=$1",
        owner,
        state["cleanup_permit"].admission_id,
    )
    if process_zero:
        assert await db.record_managed_repository_workspace_process_zero(
            state["job_id"],
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=state["generation"],
        )
    else:
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
            owner,
        )
    assert (
        await db.fetchval(
            "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", owner
        )
        == "retiring_process_zero"
    )
    assert (await child(db, state, completed=True)).allowed
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["request", "digest", "source", "pvc", "false_intent"]
)
async def test_ready_history_does_not_exempt_a_nonmatching_parent(db, fault):
    state = await ready_source(db)
    owner, pvc, request_id, digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=state["job_id"],
        identity=state["identity"],
        source="job_terminal_vm_release",
        purge_disk=fault != "false_intent",
    )
    permit = await state["recovery"].acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner,
        pvc_uid=uuid4() if fault == "pvc" else pvc,
        request_id=uuid4() if fault == "request" else request_id,
        intent_digest="sha256:" + "f" * 64 if fault == "digest" else digest,
        source="dispatcher_vm_recycle"
        if fault == "source"
        else "job_terminal_vm_release",
    )
    assert permit.allowed
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
        owner,
    )
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit.admission_id,
        )
    )
    result = await acquire(state)
    assert result is not None and not result.allowed
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                permit.admission_id,
            )
        )
        == before
    )
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "generation",
        "vm",
        "pvc",
        "context_request",
        "context_generation",
        "unauthenticated",
        "worker_lease",
        "late_ready",
        "missing_parent",
    ],
)
async def test_ready_purge_requires_original_history_and_current_exact_owner(db, fault):
    if fault == "late_ready":
        state = await cancelled(db)
        await db.execute(
            "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
            UUID(state["job_id"]),
        )
    elif fault == "missing_parent":
        state = await ready_source(db)
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"retiring_process_zero\"') WHERE id=$1",
            UUID(state["job_id"]),
        )
    else:
        state = await ready_purge(db)
        identity = state["identity"]
        if fault in {"generation", "vm", "pvc"}:
            state["identity"] = VMTeardownIdentity(
                str(uuid4())
                if fault == "generation"
                else identity.provision_generation,
                str(uuid4()) if fault == "vm" else identity.vm_uid,
                str(uuid4()) if fault == "pvc" else identity.rootdisk_pvc_uid,
            )
        elif fault == "worker_lease":
            await db.execute(
                "UPDATE run_queue SET state='leased',lease_token=lease_token+1,"
                "leased_by='stale-worker',leased_until=clock_timestamp()+interval '1 minute' WHERE unit_id=$1",
                UUID(state["job_id"]),
            )
        else:
            context = json.loads(
                await db.fetchval(
                    "SELECT context FROM jobs WHERE id=$1",
                    UUID(state["job_id"]),
                )
            )
            if fault == "context_request":
                context["vm"]["creation_request_id"] = str(uuid4())
            elif fault == "context_generation":
                context["vm"]["provision_generation"] = str(uuid4())
            else:
                context["vm"]["identity_authenticated"] = False
            await db.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                UUID(state["job_id"]),
                json.dumps(context),
            )
    result = await acquire(state)
    assert result is not None and not result.allowed
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )


@pytest.mark.asyncio
async def test_never_ready_and_existing_false_authority_still_require_retention(db):
    state = await cancelled(db)
    permit = await acquire(state)
    assert permit is not None and permit.allowed
    assert permit.parent_cleanup["intent"]["purge_disk"] is False
    assert await acquire(state) == permit


@pytest.mark.asyncio
async def test_ready_purge_rechecks_owner_after_job_lock_wait(db):
    state = await ready_purge(db)
    async with db.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        task = None
        try:
            await conn.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", UUID(state["job_id"])
            )
            task = asyncio.create_task(acquire(state))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.15)
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,identity_authenticated}','false') WHERE id=$1",
                UUID(state["job_id"]),
            )
            await transaction.commit()
            transaction = None
            result = await asyncio.wait_for(task, 5)
            assert result is not None and not result.allowed
        finally:
            if transaction is not None:
                await transaction.rollback()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("admission_enabled", [False, True])
@pytest.mark.parametrize(
    "proof_fault", [None, "missing_process_zero", "unauthenticated", "wrong_generation"]
)
async def test_ready_purge_replays_original_parent_then_releases_charge_and_cancel(
    db, monkeypatch, admission_enabled, proof_fault
):
    state = await ready_purge(db, process_zero=proof_fault != "missing_process_zero")
    monkeypatch.setenv(
        "VM_JOB_CANCEL_RETENTION_ENABLED", str(admission_enabled).lower()
    )
    owner = UUID(state["job_id"])
    assert await db.quiesce_cancelled_stateless_vm_parent(state["job_id"])
    assert not await db.quiesce_cancelled_stateless_vm_parent(
        state["job_id"], retention_only=True
    )
    events = []

    async def capture(job_id):
        assert job_id == state["job_id"]
        return state["identity"]

    async def release(job_id, identity, **kwargs):
        assert job_id == state["job_id"] and identity == state["identity"]
        assert kwargs["purge_disk"] is True
        assert kwargs["parent_cleanup"] == state["cleanup_permit"].parent_cleanup
        events.append("original_purge_replayed")
        return SimpleNamespace(disposition="completed")

    async def attest(candidate):
        assert candidate["purge_disk"] is True
        assert candidate["provision_generation"] == state["generation"]
        events.append("signed_absence_observed")
        if proof_fault == "wrong_generation":
            await db.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,provision_generation}',$2::jsonb) WHERE id=$1",
                owner,
                json.dumps(str(uuid4())),
            )
        return {
            "version": 1,
            "kind": "vm_cleanup_physical_stop",
            "job_id": state["job_id"],
            "provision_generation": state["generation"],
            **{
                key: state["frozen"][key]
                for key in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")
            },
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "pvc_disposition": "purged",
            "controller_authenticated": proof_fault != "unauthenticated",
        }

    provisioner = SimpleNamespace(
        lifecycle_available=True,
        capture_vm_teardown_identity=capture,
        release_vm_captured=release,
        attest_vm_cleanup_stop=attest,
    )
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
    control = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=partial(
                thread_retirement.archive_and_cleanup_workspace, dependencies=deps
            ),
        )
    )
    settled = await control.wait_for_stateless_cancel_settle(
        state["job_id"], timeout_seconds=0
    )
    assert events == ["original_purge_replayed", "signed_absence_observed"]
    if proof_fault:
        assert not settled
        assert (
            await db.fetchval(
                "SELECT state FROM vm_resource_reservations WHERE id=$1",
                UUID(state["reservation_id"]),
            )
            == "teardown"
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1",
                state["cleanup_permit"].admission_id,
            )
            == 0
        )
        assert await db.stateless_cancel_cleanup_pending(state["job_id"])
        vm = json.loads(
            await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner)
        )
        assert vm["status"] == "retiring_process_zero"
        assert vm.get("compute_released") is not True
        assert "disk_kept" not in vm
        assert (
            dict(
                await db.fetchrow(
                    "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                    state["cleanup_permit"].admission_id,
                )
            )
            == state["parent_before"]
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_job_cancel_retention_authorities"
            )
            == 0
        )
        return
    assert settled
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
            state["cleanup_permit"].admission_id,
        )
        == 1
    )

    assert not await db.stateless_cancel_cleanup_pending(state["job_id"])
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )
    current = dict(
        await db.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["cleanup_permit"].admission_id,
        )
    )
    assert current.pop("completed_at") is not None
    assert current.pop("outcome") == "completed"
    before = state["parent_before"]
    assert current == {
        k: v for k, v in before.items() if k not in {"completed_at", "outcome"}
    }
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 "
            "AND source='job_terminal_vm_release'",
            owner,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_ready_purge_actual_completed_probe_allows_repeated_archive_and_public_delete(
    db, monkeypatch
):
    state = await ready_purge(db)
    owner = UUID(state["job_id"])
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.invalid"

    async def absent(job_id, generation, **kwargs):
        assert job_id == state["job_id"] and generation == state["generation"]
        return _VMTeardownProbe(
            disposition="absent",
            identity=VMTeardownIdentity(generation, None, None),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )

    # Only the external authenticated absence observation is simulated. Native
    # capture, completed-probe release/delete, attestation and settlement run.
    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=absent)
    result = await provisioner.delete_vm_captured(
        state["job_id"],
        state["identity"],
        purge_disk=True,
        parent_cleanup=state["cleanup_permit"].parent_cleanup,
    )
    assert result.disposition == "completed"
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

    @asynccontextmanager
    async def vector_connection():
        yield SimpleNamespace(execute=AsyncMock())

    control = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=partial(
                thread_retirement.archive_and_cleanup_workspace,
                dependencies=deps,
            ),
            snapshot_service=SimpleNamespace(is_available=False),
            vector_db=SimpleNamespace(acquire=vector_connection),
            resolve_job_notifications=AsyncMock(),
        )
    )
    assert await control.wait_for_stateless_cancel_settle(
        state["job_id"], timeout_seconds=0
    )
    assert provisioner._probe_vm_teardown_identity.await_count == 3
    assert not await db.stateless_cancel_cleanup_pending(state["job_id"])
    await control.dependencies.archive_and_cleanup_workspace(state["job_id"])
    vm = json.loads(
        await db.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", owner)
    )
    assert vm["status"] == "deleted"
    assert vm["compute_released"] is True
    assert vm["disk_kept"] is False
    assert await db.prepare_stateless_job_for_delete(state["job_id"])
    await control.dependencies.archive_and_cleanup_workspace(state["job_id"])
    job = await db.get_job(state["job_id"])
    deleted = await control.delete(
        state["job_id"], caller={"id": str(job["user_id"])}, job=job
    )
    assert deleted["status"] == "deleted"
    assert await db.get_job(state["job_id"]) is None
    assert provisioner._probe_vm_teardown_identity.await_count == 3
    assert (
        await db.fetchval("SELECT count(*) FROM vm_job_cancel_retention_authorities")
        == 0
    )
