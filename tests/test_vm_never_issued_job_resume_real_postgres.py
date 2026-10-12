"""Public Resume retires only an exact completed never-issued creation marker."""

import asyncio
import json
from uuid import uuid4

import pytest
import asyncpg
from fastapi import HTTPException

from orchestrator.services.vm_creation_resume import settled_never_issued_resume_allowed
from tests.test_job_control_operations import _operations
from tests.test_vm_never_issued_job_terminal_real_postgres import (
    _bind_own_created_repository,
    actual_archive,
    controls,
    db as _db_fixture,
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    never_issued_cancel,
)

db = _db_fixture


async def settled(db, monkeypatch, *, timeout=3600):
    resolved = {
        "spec": {
            "timeoutSeconds": timeout,
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
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    retry, recovery, _ = await never_issued_cancel(
        db,
        monkeypatch,
        execution_resolved=resolved,
    )
    owner = retry["job_id"]
    await _bind_own_created_repository(db, owner)
    archive, _ = actual_archive(db, recovery, monkeypatch)
    assert await controls(store=db, archive=archive).wait_for_stateless_cancel_settle(
        str(owner),
        timeout_seconds=0,
    )
    return owner, retry


async def snapshot(db, owner):
    async with db.acquire() as conn:
        result = {}
        for table, column in (
            ("jobs", "id"),
            ("run_queue", "unit_id"),
            ("vm_creation_retries", "job_id"),
            ("vm_job_repository_settlement_receipts", "job_id"),
            ("vm_workspace_cleanup_admissions", "owner_id"),
            ("vm_idle_access_leases", "owner_id"),
            ("vm_idle_operations", "owner_id"),
            ("vm_workspace_recoveries", "owner_id"),
            ("vm_workspace_recovery_jobs", "job_id"),
        ):
            result[table] = [
                dict(r)
                for r in await conn.fetch(
                    f"SELECT * FROM {table} WHERE {column}=$1 ORDER BY 1",
                    owner,
                )
            ]
        return result


async def public_resume(db, owner, tmp_path):
    operations = _operations(tmp_path, store=db)
    operations.dependencies.resume_missing_workspace.return_value = "vm"
    return await operations.resume_job_internal(
        str(owner),
        user=None,
        job=await db.get_job(str(owner)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("marker_present", [True, False])
async def test_completed_never_issued_public_resume_preserves_audit(
    db,
    monkeypatch,
    tmp_path,
    marker_present,
):
    owner, _ = await settled(db, monkeypatch)
    if not marker_present:
        await db.execute(
            "UPDATE jobs SET context=context-'_vm_creation_pending' WHERE id=$1", owner
        )
    before = await snapshot(db, owner)
    assert (await public_resume(db, owner, tmp_path))["status"] == "queued"
    after = await snapshot(db, owner)
    for table in (
        "vm_creation_retries",
        "vm_job_repository_settlement_receipts",
        "vm_workspace_cleanup_admissions",
    ):
        assert after[table] == before[table]
    context = json.loads(after["jobs"][0]["context"])
    assert "vm" not in context and "_vm_creation_pending" not in context
    assert context["last_vm"]["status"] == "deleted"
    assert after["jobs"][0]["status"] == "paused"
    assert after["run_queue"][0]["state"] == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_marker",
        "null_marker",
        "generation",
        "creation_request_id",
        "preflight_digest",
        "captured_digest",
        "boolean_attempts",
        "runtime_uid",
        "retirement",
        "idle_wake",
        "suspended_io",
        "storage",
        "container",
        "ide",
        "cleanup_null",
        "delete_false",
        "completion_null",
        "preflight_version",
        "captured_version",
        "preflight_revision",
        "request_id_type",
        "captured_configuration_integer",
    ],
)
async def test_unproven_settled_creation_public_resume_is_unchanged(
    db,
    monkeypatch,
    tmp_path,
    mutation,
):
    owner, _ = await settled(db, monkeypatch)
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", owner)
    )
    vm = context["vm"]
    if mutation == "foreign_marker":
        context["_vm_creation_pending"] = str(uuid4())
    elif mutation == "null_marker":
        context["_vm_creation_pending"] = None
    elif mutation == "generation":
        vm["provision_generation"] = str(uuid4())
    elif mutation == "creation_request_id":
        vm["creation_request_id"] = str(uuid4())
    elif mutation == "preflight_digest":
        vm["creation_preflight"]["request_digest"] = "sha256:" + "f" * 64
    elif mutation == "captured_digest":
        vm["creation_request"]["request_digest"] = "sha256:" + "f" * 64
    elif mutation == "boolean_attempts":
        vm["provision_attempts"] = False
    elif mutation == "runtime_uid":
        vm["vm_uid"] = str(uuid4())
    elif mutation == "retirement":
        vm["retirement_cleanup_pending"] = True
    elif mutation == "idle_wake":
        vm["idle_wake_operation_id"] = str(uuid4())
    elif mutation == "suspended_io":
        vm["_suspend_remote_io_closed"] = True
    elif mutation == "storage":
        vm["workspace_storage"] = {"unknown": True}
    elif mutation == "container":
        context["workspace_container"] = {}
    elif mutation == "ide":
        context["ide_session"] = {}
    elif mutation == "cleanup_null":
        context["_stateless_cancel_cleanup_pending"] = None
    elif mutation == "delete_false":
        context["_stateless_delete_pending"] = False
    elif mutation == "completion_null":
        context["_completion_control_claim"] = None
    elif mutation == "preflight_version":
        vm["creation_preflight"]["version"] = True
    elif mutation == "captured_version":
        vm["creation_request"]["version"] = True
    elif mutation == "preflight_revision":
        vm["creation_preflight"]["revision"] = -1
    elif mutation == "request_id_type":
        vm["creation_preflight"]["request_id"] = {}
    elif mutation == "captured_configuration_integer":

        def corrupt_integer(value):
            for key, child in value.items():
                if type(child) is int and child == 1:
                    value[key] = True
                    return True
                if isinstance(child, dict) and corrupt_integer(child):
                    return True
            return False

        assert corrupt_integer(vm["creation_request"]["controller_configuration"])
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1", owner, json.dumps(context)
    )
    before = await snapshot(db, owner)
    with pytest.raises(HTTPException) as exc:
        await public_resume(db, owner, tmp_path)
    assert exc.value.status_code == 409
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
async def test_disabled_retry_cannot_retire_durable_creation_marker(
    db, monkeypatch, tmp_path
):
    owner, _ = await settled(db, monkeypatch)
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    before = await snapshot(db, owner)
    with pytest.raises(HTTPException) as exc:
        await public_resume(db, owner, tmp_path)
    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "vm_creation_retry_disabled"
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="cancelled",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
async def test_final_resume_status_cas_rolls_back_marker_retirement(db, monkeypatch):
    owner, _ = await settled(db, monkeypatch)
    before = await snapshot(db, owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="paused",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "obligation", ["cleanup", "access", "idle", "recovery", "participant"]
)
async def test_new_current_obligation_blocks_settled_marker_retirement(
    db,
    monkeypatch,
    obligation,
):
    owner, retry = await settled(db, monkeypatch)
    generation = retry["provision_generation"]
    async with db.acquire() as conn:
        assert await settled_never_issued_resume_allowed(conn, owner)
        if obligation == "cleanup":
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions "
                "(id,owner_kind,owner_id,source,request_id,intent_digest) "
                "VALUES($1,'job',$2,'held-test-obligation',$3,'sha256:held')",
                uuid4(),
                owner,
                uuid4(),
            )
        elif obligation == "access":
            await conn.execute(
                "INSERT INTO vm_idle_access_leases "
                "(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,expires_at,max_expires_at) "
                "VALUES('job',$1,$2,$3,'sftp','held-test',now()+interval '1 minute',now()+interval '2 minutes')",
                owner,
                generation,
                uuid4(),
            )
        elif obligation == "idle":
            await conn.execute(
                "INSERT INTO vm_idle_operations "
                "(owner_kind,owner_id,phase,episode_id,episode_revision,provision_generation,"
                "vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind) "
                "VALUES('job',$1,'release_held',$2,1,$3,$4,$5,$6,$7,'rootdisk')",
                owner,
                uuid4(),
                generation,
                uuid4(),
                uuid4(),
                uuid4(),
                uuid4(),
            )
        else:
            recovery = uuid4()
            await conn.execute(
                "INSERT INTO vm_workspace_recoveries "
                "(id,owner_kind,owner_id,workspace_contract_digest,cluster_name,phase,reason_code) "
                "VALUES($1,'job',$2,'sha256:held','test','paused_attention','tool_outcome_unknown')",
                recovery,
                owner if obligation == "recovery" else uuid4(),
            )
            if obligation == "participant":
                await conn.execute(
                    "INSERT INTO vm_workspace_recovery_jobs "
                    "(recovery_id,job_id,prior_queue_state,prior_job_status) "
                    "VALUES($1,$2,'non_worker','cancelled')",
                    recovery,
                    owner,
                )
        assert not await settled_never_issued_resume_allowed(conn, owner)
    before = await snapshot(db, owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="cancelled",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
async def test_missing_terminal_parent_cannot_forge_deleted_projection(db, monkeypatch):
    retry, _, _ = await never_issued_cancel(db, monkeypatch)
    owner = retry["job_id"]
    await _bind_own_created_repository(db, owner)
    # A deleted projection cannot manufacture the omitted logical settlement.
    with pytest.raises(asyncpg.CheckViolationError, match="process-zero authority"):
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context-'_stateless_cancel_cleanup_pending',"
            "'{vm,status}','\"deleted\"') WHERE id=$1",
            owner,
        )
    before = await snapshot(db, owner)
    async with db.acquire() as conn:
        assert not await settled_never_issued_resume_allowed(conn, owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="cancelled",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["revision", "generation", "deadline", "expired"])
async def test_changed_current_execution_cannot_retire_historical_marker(
    db,
    monkeypatch,
    mutation,
):
    owner, retry = await settled(
        db, monkeypatch, timeout=4 if mutation == "expired" else 3600
    )
    if mutation == "expired":
        database_now = await db.fetchval("SELECT clock_timestamp()")
        await asyncio.sleep(
            max(0, (retry["admission_deadline"] - database_now).total_seconds()) + 0.02
        )
    else:
        change = {
            "revision": "revision='changed'",
            "generation": "generation=generation+1",
            "deadline": "created_at=created_at+interval '1 hour'",
        }[mutation]
        await db.execute(
            f"UPDATE srw_execution_specs SET {change} WHERE work_id=$1", owner
        )
    before = await snapshot(db, owner)
    async with db.acquire() as conn:
        assert not await settled_never_issued_resume_allowed(conn, owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="cancelled",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
async def test_worker_hold_refusal_rolls_back_exact_marker_retirement(db, monkeypatch):
    owner, _ = await settled(db, monkeypatch)
    await db.execute(
        "UPDATE jobs SET context=context || '{\"_worker_execution_hold\":null}'::jsonb WHERE id=$1",
        owner,
    )
    async with db.acquire() as conn:
        assert await settled_never_issued_resume_allowed(conn, owner)
    before = await snapshot(db, owner)
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(owner),
        "vm",
        expected_status="cancelled",
    )
    assert await snapshot(db, owner) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_target", ["queue", "job"])
async def test_resume_rechecks_current_authority_after_lock_wait(
    db,
    monkeypatch,
    lock_target,
):
    owner, _ = await settled(db, monkeypatch)
    async with db.acquire() as conn:
        assert await settled_never_issued_resume_allowed(conn, owner)
        async with conn.transaction():
            blocker = await conn.fetchval("SELECT pg_backend_pid()")
            table, column = (
                ("run_queue", "unit_id") if lock_target == "queue" else ("jobs", "id")
            )
            await conn.fetchrow(
                f"SELECT * FROM {table} WHERE {column}=$1 FOR UPDATE", owner
            )
            task = asyncio.create_task(
                db.prepare_stateless_job_for_workspace_resume(
                    str(owner),
                    "vm",
                    expected_status="cancelled",
                )
            )
            # Require an actual blocked backend rather than timing the writer.
            for _ in range(100):
                await conn.execute("SELECT pg_stat_clear_snapshot()")
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                    blocker,
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                task.cancel()
                raise AssertionError("Resume never reached the owned row lock")
            await conn.execute(
                "UPDATE jobs SET context=context || '{\"_stateless_delete_pending\":false}'::jsonb WHERE id=$1",
                owner,
            )
        before = await snapshot(db, owner)
        assert await asyncio.wait_for(task, 5) is False
    assert await snapshot(db, owner) == before
