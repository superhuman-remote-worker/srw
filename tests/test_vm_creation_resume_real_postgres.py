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
async def test_public_creation_resume_lifts_exact_paused_fixture_hold(db, monkeypatch, stage):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, original = await pending(db, stage)
    owner = str(uuid4())
    await db.execute("UPDATE jobs SET status='paused' WHERE id=$1", job)
    assert await db.hold_paused_job(str(job), paused_by=owner)
    before = await snapshot(db, job)
    hold = json.loads(before["job"]["context"])["_operator_pause_hold"]

    result = await resume_pending_creation(
        db, job_id=str(job), lift_operator_pause_hold=hold["hold_id"],
    )

    assert result["vm_creation_retry_request_id"] == original["request_id"]
    after = await snapshot(db, job)
    context = json.loads(after["job"]["context"])
    assert "_operator_pause_hold" not in context
    assert context["last_operator_pause_hold"]["hold_id"] == hold["hold_id"]
    if stage == "ledger":
        assert (after["retry"][0]["canonical_request"],
                after["retry"][0]["request_digest"],
                after["retry"][0]["admission_deadline"]) == (
                    before["retry"][0]["canonical_request"],
                    before["retry"][0]["request_digest"],
                    before["retry"][0]["admission_deadline"],
                )
    else:
        assert context["vm"]["creation_preflight"]["request"] == original["request"]
    assert after["queue"]["state"] == "done"
    assert after["queue"]["lease_token"] == before["queue"]["lease_token"]


@pytest.mark.asyncio
async def test_background_creation_resume_preserves_owner_pause(db, monkeypatch):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, _ = await pending(db, "ledger")
    await db.execute("UPDATE jobs SET status='paused' WHERE id=$1", job)
    assert await db.hold_paused_job(str(job), paused_by=str(uuid4()))
    before = await snapshot(db, job)
    assert (await resume_pending_creation(db, job_id=str(job)))["status"] == "queued"
    after = await snapshot(db, job)
    assert json.loads(after["job"]["context"])["_operator_pause_hold"] == (
        json.loads(before["job"]["context"])["_operator_pause_hold"]
    )
    assert after["queue"]["state"] == "done"


@pytest.mark.asyncio
async def test_stale_public_creation_resume_cannot_cross_newer_pause(db, monkeypatch):
    from orchestrator.services.vm_creation_resume import resume_pending_creation

    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    job, _ = await pending(db, "ledger")
    await db.execute("UPDATE jobs SET status='paused' WHERE id=$1", job)
    assert await db.hold_paused_job(str(job), paused_by=str(uuid4()))
    first = json.loads((await snapshot(db, job))["job"]["context"])["_operator_pause_hold"]
    assert await db.queue_stateless_job_for_resume(
        str(job), expected_status="paused",
        lift_operator_pause_hold=first["hold_id"],
    )
    assert await db.hold_paused_job(str(job), paused_by=str(uuid4()))
    before = await snapshot(db, job)
    with pytest.raises(VMCreationRetryConflict, match="job_control_busy"):
        await resume_pending_creation(
            db, job_id=str(job), lift_operator_pause_hold=first["hold_id"],
        )
    assert await snapshot(db, job) == before


@pytest.mark.asyncio
async def test_owner_resume_route_merges_held_feedback_and_revokes_token(
    db, monkeypatch, tmp_path,
):
    import hashlib
    from datetime import datetime, timedelta, timezone

    import httpx
    from fastapi import FastAPI

    from orchestrator.routers import job_controls as routes
    from orchestrator.routers import job_lifecycle as mutation_routes
    from orchestrator.security.access import (
        require_internal, require_internal_or_job_access, require_job_access,
    )
    from orchestrator.operator_cli.vm_retained_resume_fixture import (
        prepare_fixture, seed_model,
    )
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_provisioner import VMProvisioner
    from tests.test_job_control_operations import _operations
    from tests.test_operator_pause_hold_real_postgres import (
        _operations as pause_operations,
    )

    image = "registry.example/a1-guest@sha256:" + "a" * 64
    for name, value in {
        "VM_MODE": "same-cluster", "VM_CREATION_RETRY_ENABLED": "true",
        "VM_NETWORK_PROFILE_ENABLED": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": image,
        "VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)
    run = "srw-a1-route-" + uuid4().hex[:8]
    model = "e2e-vm-a1-route-" + uuid4().hex[:8]
    await seed_model(
        db, run_id=run, namespace=run, model_id=model,
        inference_key="a1-owner-route-test-key-20260924",
    )
    provisioner = VMProvisioner()
    provisioner._db = db
    prepared = await prepare_fixture(
        db, provisioner, run_id=run, namespace=run,
        vm_image=image, model_id=model,
    )
    job, owner = prepared["job_id"], prepared["owner_id"]
    preflight = VMCreationPreflightStore(db)
    claim = (await preflight.claim_due(limit=1))[0]
    assert await preflight.record_failure(
        claim, reason="creation_configuration_unproven",
    )
    original = json.loads(await db.fetchval(
        "SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1",
        job,
    ))
    operations = _operations(tmp_path, store=db)
    dependencies = routes.JobControlRouteDependencies(
        operations=operations, store=db,
        require_internal_or_job_access=require_internal_or_job_access,
        require_job_access=require_job_access,
        require_admin=None, require_approved_user=None,
        require_sudo_request_authority=None, user_can_access_job_or_thread=None,
        mcp_scope_project_id=None,
    )
    app = FastAPI()
    app.include_router(routes.router)
    app.include_router(mutation_routes.router)
    app.state.job_control_dependencies_factory = lambda: dependencies
    pause_dependencies = mutation_routes.JobControlRouteDependencies(
        operations=pause_operations(db, commands_enabled=True), store=db,
        require_internal_or_job_access=require_internal_or_job_access,
        require_job_access=require_job_access, require_internal=require_internal,
    )
    app.state.job_control_route_dependencies_factory = lambda: pause_dependencies
    raw = "srw_" + uuid4().hex + uuid4().hex
    token = await db.create_mcp_token(
        user_id=str(owner), name="a1-owner-route",
        token_hash=hashlib.sha256(raw.encode()).hexdigest(),
        token_prefix=raw[:12], scope="user",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        origin="vm-retained-resume-fixture-test",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://owner.test",
    ) as client:
        pause = await client.put(
            f"/api/jobs/{job}/pause",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert pause.status_code == 200
        hold = json.loads(await db.fetchval(
            "SELECT context->'_operator_pause_hold' FROM jobs WHERE id=$1",
            job,
        ))
        assert hold["paused_by"] == owner and hold["source"] == "public_pause"
        await db.execute(
            "UPDATE jobs SET context=context || $2::jsonb WHERE id=$1",
            job, json.dumps({"queued_feedback": "held earlier"}),
        )
        response = await client.post(
            f"/api/jobs/{job}/resume",
            headers={"Authorization": f"Bearer {raw}"},
            json={"feedback": "new request"},
        )
        assert response.status_code == 200
        assert response.json()["vm_creation_retry_request_id"] == original["request_id"]
        assert await db.revoke_mcp_token(str(token["id"]), owner)
        refused = await client.post(
            f"/api/jobs/{job}/resume",
            headers={"Authorization": f"Bearer {raw}"}, json={},
        )
        assert refused.status_code == 401
    after = await snapshot(db, job)
    context = json.loads(after["job"]["context"])
    assert "_operator_pause_hold" not in context
    assert context["last_operator_pause_hold"]["hold_id"] == hold["hold_id"]
    assert context["queued_feedback"] == "held earlier\n\n---\n\nnew request"
    assert after["queue"]["state"] == "done"
    assert context["vm"]["creation_preflight"]["request"] == original["request"]
    assert context["vm"]["creation_preflight"]["admission_deadline"] == (
        original["admission_deadline"]
    )


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
