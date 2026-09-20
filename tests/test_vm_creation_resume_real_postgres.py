"""Public creation Resume preserves immutable intent and worker admission holds."""

import asyncio
import json
from uuid import uuid4

import pytest

from tests.test_vm_creation_preflight_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    resolving,
    initial_job,
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryStore,
    VMCreationRetryConflict,
)

db = _db_fixture


async def pending(db, stage):
    job, preflight, claim, resolved = await resolving(db)
    if stage == "preflight":
        assert await preflight.record_failure(
            claim, reason="creation_configuration_unproven"
        )
    else:
        await preflight.complete_resolution(claim, resolved)
        store = VMCreationRetryStore(db)
        retry = (await store.claim_due(limit=1))[0]
        assert await store.apply_observation(
            request_id=str(retry["request_id"]),
            claim_token=str(retry["claim_token"]),
            expected_revision=retry["revision"],
            observation={"outcome": "blocked"},
        )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1", job
        )
        await conn.execute(
            "UPDATE jobs SET status='failed',context=context || $2::jsonb WHERE id=$1",
            job,
            json.dumps(
                {"completion_decision": {"preserve": True}, "checkpoint": "keep"}
            ),
        )
    return job, claim


async def snapshot(db, job):
    async with db.acquire() as conn:
        return {
            "job": dict(await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job)),
            "queue": dict(
                await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job)
            ),
            "retry": [
                dict(r)
                for r in await conn.fetch(
                    "SELECT * FROM vm_creation_retries WHERE job_id=$1", job
                )
            ],
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
async def test_duplicate_resume_retains_one_request_and_pending_worker_budget(
    db, monkeypatch, stage
):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, original = await pending(db, stage)
    results = await asyncio.gather(
        *[
            resume_pending_creation(
                db,
                job_id=str(job),
                feedback="please continue",
                feedback_reason="Operator resumed",
            )
            for _ in range(2)
        ]
    )
    assert results[0] == results[1]
    assert results[0]["status"] == "queued"
    assert results[0]["vm_creation_retry_request_id"] == original["request_id"]
    state = await snapshot(db, job)
    context = json.loads(state["job"]["context"])
    assert state["job"]["status"] == "paused"
    assert context["_vm_creation_pending"] == original["request_id"]
    assert (
        context["vm"]["provision_generation"]
        == original["request"]["provision_generation"]
    )
    assert context["vm"]["creation_preflight"]["request"] == original["request"]
    assert (
        context["vm"]["creation_preflight"]["admission_deadline"]
        == original["admission_deadline"]
    )
    assert context["vm"]["provision_attempts"] == 0
    assert context["completion_decision"] == {"preserve": True}
    assert context["checkpoint"] == "keep"
    assert context["queued_feedback"] == "please continue"
    assert context["queued_feedback_reason"] == "Operator resumed"
    assert context["queued_feedback_delivery_id"]
    assert state["queue"]["state"] == "done"
    assert state["queue"]["attempts_since_completion"] == 3
    if stage == "preflight":
        assert context["vm"]["creation_preflight"]["state"] == "queued"
        assert state["retry"] == []
    else:
        assert len(state["retry"]) == 1
        assert state["retry"][0]["state"] == "queued"
        assert (
            state["retry"][0]["admission_deadline"].isoformat()
            == original["admission_deadline"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
@pytest.mark.parametrize(
    "reason",
    [
        "disabled",
        "deadline_drift",
        "revision_drift",
        "cancelled",
        "completion_control",
        "worker_lease",
        "reviewing",
    ],
)
async def test_resume_refusal_rolls_back_every_admission_write(
    db, monkeypatch, stage, reason
):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv(
        "VM_CREATION_RETRY_ENABLED", "false" if reason == "disabled" else "true"
    )
    job, _ = await pending(db, stage)
    async with db.acquire() as conn:
        if reason == "deadline_drift":
            await conn.execute(
                "UPDATE srw_execution_specs SET created_at=created_at+interval '1 hour' WHERE work_id=$1",
                job,
            )
        elif reason == "revision_drift":
            await conn.execute(
                "UPDATE srw_execution_specs SET revision='changed' WHERE work_id=$1",
                job,
            )
        elif reason in {"cancelled", "reviewing"}:
            await conn.execute("UPDATE jobs SET status=$2 WHERE id=$1", job, reason)
        elif reason == "completion_control":
            await conn.execute(
                "UPDATE jobs SET context=context || jsonb_build_object('_completion_control_claim',jsonb_build_object('version',1,'expires_epoch',extract(epoch FROM clock_timestamp())+3600)) WHERE id=$1",
                job,
            )
        elif reason == "worker_lease":
            await conn.execute(
                "UPDATE run_queue SET state='leased',leased_by='existing',leased_until=clock_timestamp()+interval '1 hour' WHERE unit_id=$1",
                job,
            )
    before = await snapshot(db, job)
    with pytest.raises(VMCreationRetryConflict):
        await resume_pending_creation(db, job_id=str(job), feedback="new feedback")
    assert await snapshot(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
async def test_duplicate_resume_does_not_revoke_live_observer_claim(
    db, monkeypatch, stage
):
    from orchestrator.services.vm_creation_resume import resume_pending_creation
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, _ = await pending(db, stage)
    await resume_pending_creation(db, job_id=str(job))
    store = (
        VMCreationPreflightStore(db)
        if stage == "preflight"
        else VMCreationRetryStore(db)
    )
    claim = (await store.claim_due(limit=1))[0]
    await resume_pending_creation(db, job_id=str(job))
    state = await snapshot(db, job)
    current = (
        json.loads(state["job"]["context"])["vm"]["creation_preflight"]
        if stage == "preflight"
        else state["retry"][0]
    )
    assert current["claim_token"] == claim["claim_token"]
    assert current["revision"] == claim["revision"]
    assert current["claim_expires_at"] == claim["claim_expires_at"]


@pytest.mark.asyncio
async def test_no_pending_intent_does_not_manufacture_creation_authority(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job = await initial_job(db)
    assert await resume_pending_creation(db, job_id=str(job)) is None
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            job,
            json.dumps(
                {"_vm_creation_pending": str(uuid4()), "vm": {"status": "failed"}}
            ),
        )
    with pytest.raises(VMCreationRetryConflict, match="creation_request_unproven"):
        await resume_pending_creation(db, job_id=str(job))


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["preflight", "ledger"])
async def test_public_resume_uses_fresh_intent_before_generic_workspace_preparation(
    db, monkeypatch, tmp_path, stage
):
    from tests.test_job_control_operations import _operations
    from orchestrator.schemas.job_controls import JobResumeRequest

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, original = await pending(db, stage)
    operations = _operations(tmp_path, store=db)
    result = await operations.resume_job_internal(
        str(job),
        user={"id": str(uuid4())},
        job={
            "id": str(job),
            "status": "failed",
            "execution_lane": "stateless",
            "context": {},
        },
        request=JobResumeRequest(feedback="try again"),
    )
    assert result["vm_creation_retry_request_id"] == original["request_id"]
    operations.dependencies.completion_control.guard.assert_awaited_once_with(
        str(job), source="public_resume"
    )
    operations.dependencies.prepare_job_workspace_runtime.assert_not_awaited()
    operations.dependencies.resume_job_on_agent.assert_not_awaited()
    operations.dependencies.trigger_dispatch.assert_called_once_with()


@pytest.mark.asyncio
async def test_public_resume_reports_bounded_conflict_without_generic_fallback(
    db, monkeypatch, tmp_path
):
    from tests.test_job_control_operations import _operations
    from fastapi import HTTPException

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    job, _ = await pending(db, "preflight")
    operations = _operations(tmp_path, store=db)
    before = await snapshot(db, job)
    with pytest.raises(HTTPException) as error:
        await operations.resume_job_internal(
            str(job),
            user={"id": str(uuid4())},
            job={
                "id": str(job),
                "status": "failed",
                "execution_lane": "stateless",
                "context": {},
            },
        )
    assert error.value.status_code == 409
    assert error.value.detail["code"] == "vm_creation_retry_disabled"
    assert await snapshot(db, job) == before
    operations.dependencies.prepare_job_workspace_runtime.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("helper", ["shed", "stateless", "pinned"])
async def test_legacy_resume_cannot_shed_preflight_even_before_ledger_exists(
    db, helper
):
    job, _, _, _ = await resolving(db)
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='paused' WHERE id=$1", job)
        if helper == "pinned":
            claim_id = uuid4()
            await conn.execute(
                "UPDATE jobs SET execution_lane='pinned',context=context || jsonb_build_object('_completion_control_claim',jsonb_build_object('version',1,'claim_id',$2::text,'expires_epoch',extract(epoch FROM clock_timestamp())+3600)) WHERE id=$1",
                job,
                str(claim_id),
            )
    before = await snapshot(db, job)
    if helper == "shed":
        result = await db.shed_workspace_context(str(job), "vm")
    elif helper == "stateless":
        result = await db.prepare_stateless_job_for_workspace_resume(
            str(job), "vm", expected_status="paused"
        )
    else:
        result = await db.prepare_pinned_job_for_workspace_resume(
            str(job),
            "vm",
            expected_status="paused",
            completion_control_claim_id=str(claim_id),
        )
    assert result is False
    assert await snapshot(db, job) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [None, "null", '{"vm": null}'])
async def test_ordinary_nullable_context_is_not_a_creation_resume(db, context):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    job = await initial_job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, context
        )
    assert await resume_pending_creation(db, job_id=str(job)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("authenticated_runtime", [False, True])
async def test_legacy_failed_successor_needs_original_creation_proof(
    db, monkeypatch, tmp_path, authenticated_runtime
):
    from tests.test_job_control_operations import _operations
    from fastapi import HTTPException
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    vm = {
        "status": "failed",
        "provision_generation": str(uuid4()),
        "provision_attempts": 0,
        "error": "create rejected while predecessor cleanup was open",
    }
    if authenticated_runtime:
        vm.update(
            vm_uid=str(uuid4()),
            identity_authenticated=True,
            identity_provision_generation=vm["provision_generation"],
        )
    context = {
        "vm": vm,
        "last_vm": {
            "status": "deleted",
            "provision_generation": str(uuid4()),
            "rootdisk_pvc_uid": str(uuid4()),
            "identity_authenticated": True,
        },
    }
    job = await initial_job(db, context=context)
    if authenticated_runtime:
        assert await resume_pending_creation(db, job_id=str(job)) is None
        return
    operations = _operations(tmp_path, store=db)
    async with db.acquire() as conn:
        before = await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
    with pytest.raises(HTTPException) as error:
        await operations.resume_job_internal(
            str(job),
            user={"id": str(uuid4())},
            job={
                "id": str(job),
                "status": "failed",
                "execution_lane": "stateless",
                "context": context,
            },
        )
    assert error.value.status_code == 409
    assert error.value.detail["code"] == "creation_request_unproven"
    operations.dependencies.prepare_job_workspace_runtime.assert_not_awaited()
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job) == before
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1)", job
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage,field",
    [
        ("preflight", "execution_id"),
        ("preflight", "request_id"),
        ("preflight", "expected_pvc_uid"),
        ("preflight", "request"),
        ("ledger", None),
    ],
)
async def test_malformed_frozen_authority_is_bounded_public_refusal(
    db, monkeypatch, tmp_path, stage, field
):
    from tests.test_job_control_operations import _operations
    from fastapi import HTTPException

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, _ = await pending(db, stage)
    async with db.acquire() as conn:
        path = (
            ["vm", "creation_request"]
            if stage == "ledger"
            else ["vm", "creation_preflight", field]
        )
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,$2::text[],'[\"bad\"]') WHERE id=$1",
            job,
            path,
        )
    operations = _operations(tmp_path, store=db)
    before = await snapshot(db, job)
    with pytest.raises(HTTPException) as error:
        await operations.resume_job_internal(
            str(job),
            user={"id": str(uuid4())},
            job={
                "id": str(job),
                "status": "failed",
                "execution_lane": "stateless",
                "context": {},
            },
        )
    assert error.value.status_code == 409
    assert error.value.detail["code"] == "creation_request_unproven"
    assert await snapshot(db, job) == before
