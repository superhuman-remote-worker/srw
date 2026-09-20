"""Initial creation provenance is frozen before read-only configuration I/O."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_retry_real_postgres import (
    db as _retry_db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from orchestrator.services.vm_creation_request import build_vm_creation_request
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

db = _retry_db_fixture


async def initial_job(db, *, timeout=3600, lane="stateless", context=None):
    job = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,context) VALUES($1,'initial','created',$2,$3::jsonb)",
            job,
            lane,
            json.dumps(context or {}),
        )
        await conn.execute(
            "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,revision,harness_adapter) "
            "VALUES($1,'Job',$2,'{}',$3::jsonb,'revision-1','srw/v1')",
            uuid4(),
            job,
            json.dumps({"spec": {"timeoutSeconds": timeout}}),
        )
    return job


def candidate(job, **changes):
    vm = VMProvisioner._fresh_provision_ctx()
    request = build_vm_creation_request(
        job_id=str(job),
        agent_config="worker_base",
        vm_image="image-original",
        cpu_cores=2,
        memory="2Gi",
        description="initial",
        network_tier="restricted",
        provision_generation=vm["provision_generation"],
        **changes,
    )
    return request, vm


@pytest.mark.asyncio
async def test_initial_preflight_first_writer_freezes_one_generation_and_queue_hold(db):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    job = await initial_job(db)
    store = VMCreationPreflightStore(db)
    one, two = candidate(job), candidate(job)
    results = await asyncio.gather(
        *[
            store.begin(job_id=str(job), request=request, fresh_context=context)
            for request, context in (one, two)
        ]
    )
    assert results[0] == results[1]
    assert results[0]["request"] in (one[0], two[0])
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["_vm_creation_pending"] == results[0]["request_id"]
        assert (
            context["vm"]["provision_generation"]
            == results[0]["request"]["provision_generation"]
        )
        assert context["vm"]["status"] == "waiting_creation_configuration"
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
            )
            == 0
        )
        assert (
            await conn.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job)
            == "done"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["legacy", "cancelled", "expired", "retirement"])
async def test_unproven_initial_creation_cannot_reset_existing_or_expired_authority(
    db, failure
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    original = (
        {"vm": {"status": "failed", "provision_generation": str(uuid4())}}
        if failure == "legacy"
        else {}
    )
    if failure == "retirement":
        original = {
            "vm": {
                "status": "deleted",
                "provision_generation": str(uuid4()),
                "retirement_cleanup_pending": True,
            }
        }
    job = await initial_job(
        db, context=original, timeout=-1 if failure == "expired" else 3600
    )
    if failure == "cancelled":
        async with db.acquire() as conn:
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
    request, fresh = candidate(job)
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=str(job), request=request, fresh_context=fresh
        )
    async with db.acquire() as conn:
        assert (
            json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job))
            == original
        )
        assert (
            await conn.fetchval("SELECT count(*) FROM run_queue WHERE unit_id=$1", job)
            == 0
        )


@pytest.mark.asyncio
async def test_resolution_claim_and_failure_backoff_survive_restart_and_ignore_late_reply(
    db,
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    job = await initial_job(db)
    request, fresh = candidate(job)
    store = VMCreationPreflightStore(db)
    await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    claim = (await store.claim_due(limit=1))[0]
    assert await VMCreationPreflightStore(db).claim_due(limit=1) == []
    assert await store.record_failure(claim, reason="controller_unavailable")
    assert not await store.record_failure(claim, reason="controller_unavailable")
    assert await VMCreationPreflightStore(db).claim_due(limit=1) == []
    async with db.acquire() as conn:
        vm = json.loads(
            await conn.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", job)
        )
        assert vm["creation_preflight"]["attempt"] == 1
        assert vm["creation_preflight"]["outage_started_at"] > 0
        assert vm.get("provision_attempts", 0) == 0


async def resolving(db):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from tests.test_vm_creation_configuration import controller
    from vm_controller.creation_configuration import resolve_creation_configuration

    job = await initial_job(db)
    request, fresh = candidate(job)
    store = VMCreationPreflightStore(db)
    await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    claim = (await store.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(controller(), claim["request"])
    resolved["creation_retry_protocol"] = 1
    return job, store, claim, resolved


@pytest.mark.asyncio
async def test_resolution_handoff_atomically_freezes_config_and_initial_ledger(db):
    job, store, claim, resolved = await resolving(db)
    admitted = await store.complete_resolution(claim, resolved)
    assert str(admitted["request_id"]) == claim["request_id"]
    assert admitted["canonical_request"] == resolved["request"]
    assert admitted["controller_configuration"] == resolved["controller_configuration"]
    assert admitted["state"] == "queued"
    assert await store.claim_due(limit=1) == []
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["vm"]["creation_preflight"]["state"] == "admitted"
        assert (
            context["vm"]["creation_request"]["controller_configuration_authenticated"]
            is True
        )
        assert context["_vm_creation_pending"] == claim["request_id"]
        assert (
            await conn.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job)
            == "done"
        )
    with pytest.raises(VMCreationRetryConflict):
        await store.complete_resolution(claim, resolved)


@pytest.mark.asyncio
async def test_failed_ledger_admission_rolls_back_full_snapshot_and_preflight_handoff(
    db, monkeypatch
):
    job, store, claim, resolved = await resolving(db)

    async def reject(*args, **kwargs):
        raise VMCreationRetryConflict("injected_admission_conflict")

    monkeypatch.setattr(store.retry, "admit_on_conn", reject)
    with pytest.raises(VMCreationRetryConflict):
        await store.complete_resolution(claim, resolved)
    async with db.acquire() as conn:
        vm = json.loads(
            await conn.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", job)
        )
        assert vm.get("creation_request") is None
        assert vm["creation_preflight"]["state"] == "resolving"
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
            )
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["cancel", "claim_expired", "options_changed"])
async def test_resolution_cannot_commit_after_authority_or_frozen_request_changes(
    db, winner
):
    from shared.vm_creation_retry import canonical_request_digest

    job, store, claim, resolved = await resolving(db)
    async with db.acquire() as conn:
        if winner == "cancel":
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
        elif winner == "claim_expired":
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight,claim_expires_at}','1'::jsonb) WHERE id=$1",
                job,
            )
        else:
            resolved["request"]["memory"] = "32Gi"
            resolved["request_digest"] = canonical_request_digest(resolved["request"])
    with pytest.raises((VMCreationRetryConflict, ValueError)):
        await store.complete_resolution(claim, resolved)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
            )
            == 0
        )
        assert await conn.fetchval(
            "SELECT context->'vm'->'creation_request' IS NULL OR context->'vm'->'creation_request'='null'::jsonb FROM jobs WHERE id=$1",
            job,
        )


@pytest.mark.asyncio
async def test_cancelled_preflight_settles_only_its_proven_zero_issuance_hold(db):
    job, store, claim, resolved = await resolving(db)
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job)
    assert await store.settle_cancelled(limit=10) == 1
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert "_vm_creation_pending" not in context
        assert context["vm"]["creation_preflight"]["state"] == "settled"
        assert (
            context["vm"]["provision_generation"]
            == claim["request"]["provision_generation"]
        )
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job)
            == "cancelled"
        )
    with pytest.raises(VMCreationRetryConflict):
        await store.complete_resolution(claim, resolved)
    assert await store.settle_cancelled(limit=10) == 0


@pytest.mark.asyncio
async def test_cancelled_preflight_cannot_settle_an_existing_creation_ledger(db):
    job, store, claim, resolved = await resolving(db)
    await store.complete_resolution(claim, resolved)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='cancelled', context=jsonb_set(context,'{vm,creation_preflight,state}','\"queued\"'::jsonb) WHERE id=$1",
            job,
        )
    assert await store.settle_cancelled(limit=10) == 0
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT context ? '_vm_creation_pending' FROM jobs WHERE id=$1", job
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE job_id=$1", job
            )
            == "cancel_requested"
        )


@pytest.mark.asyncio
async def test_preflight_execution_change_becomes_visible_attention_without_hot_polling(
    db,
):
    job, store, claim, _ = await resolving(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE srw_execution_specs SET created_at=clock_timestamp()-interval '2 hours' WHERE work_kind='Job' AND work_id=$1",
            job,
        )
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight,claim_expires_at}','1'::jsonb) WHERE id=$1",
            job,
        )
    assert await store.claim_due(limit=10) == []
    async with db.acquire() as conn:
        value = json.loads(
            await conn.fetchval(
                "SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1", job
            )
        )
        assert value["state"] == "attention"
        assert value["reason"] == "execution_manifest_changed"
        assert value["claim_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proof", ["valid", "missing_receipt", "purged_disk", "exhausted"]
)
async def test_retired_successor_preflight_keeps_disk_proof_and_boot_history(db, proof):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest

    generation, vm_uid, pvc_uid = (str(uuid4()) for _ in range(3))
    old = {
        "status": "deleted",
        "provision_generation": generation,
        "vm_uid": vm_uid,
        "rootdisk_pvc_uid": pvc_uid,
        "identity_authenticated": True,
        "identity_provision_generation": generation,
        "provision_attempts": 3 if proof == "exhausted" else 2,
    }
    job = await initial_job(db, context={"vm": old})
    async with db.acquire() as conn:
        if proof != "missing_receipt":
            await conn.execute(
                "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2)",
                job,
                generation,
            )
        intent = {
            "owner_kind": "job",
            "owner_id": str(job),
            "provision_generation": generation,
            "vm_uid": vm_uid,
            "pvc_uid": pvc_uid,
            "purge_disk": proof == "purged_disk",
            "resource": "vm_workspace",
            "source": "dispatcher_vm_recycle",
        }
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'dispatcher_vm_recycle',$4,$5,clock_timestamp(),'completed')",
            uuid4(),
            job,
            UUID(pvc_uid),
            uuid4(),
            cleanup_intent_digest(intent),
        )
    request, fresh = candidate(job)
    store = VMCreationPreflightStore(db)
    if proof != "valid":
        with pytest.raises(VMCreationRetryConflict):
            await store.begin(job_id=str(job), request=request, fresh_context=fresh)
        return
    value = await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    assert value["expected_pvc_uid"] == pvc_uid
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["last_vm"] == old
        assert context["vm"]["provision_attempts"] == 2
        assert context["vm"]["provision_generation"] != generation
        assert context["vm"]["status"] == "waiting_creation_configuration"


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [False, True])
@pytest.mark.parametrize("deadline_changed", [False, True])
async def test_retired_protocol_generation_can_begin_proven_successor(
    db, monkeypatch, archived, deadline_changed
):
    from tests.test_vm_creation_effects_real_postgres import observed_creation
    from shared.vm_creation_retry import canonical_request_digest

    retry, row, carrier, observations = await observed_creation(db, monkeypatch)
    await retry.settle_adopted(
        request_id=str(row["request_id"]), carrier=carrier, observations=observations
    )
    job = row["job_id"]
    store = VMCreationPreflightStore(db)
    generation = str(row["provision_generation"])
    vm_uid = observations["vm"]["object"]["metadata"]["uid"]
    pvc_uid = observations["rootdisk"]["pvc"]["metadata"]["uid"]
    prior = {
        "version": 1,
        "request": row["canonical_request"],
        "request_digest": canonical_request_digest(row["canonical_request"]),
        "request_id": str(row["request_id"]),
        "execution_id": str(row["execution_id"]),
        "execution_revision": row["execution_revision"],
        "execution_generation": row["execution_generation"],
        "admission_deadline": row["admission_deadline"].isoformat(),
        "revision": 2,
        "attempt": 0,
        "state": "admitted",
        "expected_pvc_uid": None,
    }
    async with db.acquire() as conn:
        old = json.loads(
            await conn.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", job)
        )
    old.update(status="deleted", creation_preflight=prior)
    intent = {
        "owner_kind": "job",
        "owner_id": str(job),
        "provision_generation": generation,
        "vm_uid": vm_uid,
        "pvc_uid": pvc_uid,
        "purge_disk": False,
        "resource": "vm_workspace",
        "source": "dispatcher_vm_recycle",
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2)",
            job,
            generation,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'dispatcher_vm_recycle',$4,$5,clock_timestamp(),'completed')",
            uuid4(),
            job,
            UUID(pvc_uid),
            uuid4(),
            cleanup_intent_digest(intent),
        )
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
            job,
            json.dumps({"vm": old}),
        )
        if deadline_changed:
            await conn.execute(
                "UPDATE srw_execution_specs SET created_at=created_at+interval '1 hour' WHERE work_id=$1",
                job,
            )
    if archived:
        assert await db.shed_workspace_context(str(job), "vm")
    newreq, newctx = candidate(job)
    if deadline_changed:
        with pytest.raises(VMCreationRetryConflict, match="execution_manifest_changed"):
            await store.begin(job_id=str(job), request=newreq, fresh_context=newctx)
        return
    value = await store.begin(job_id=str(job), request=newreq, fresh_context=newctx)
    assert value["request"]["provision_generation"] == newreq["provision_generation"]
    assert value["expected_pvc_uid"] == pvc_uid
    for key in (
        "execution_id",
        "execution_revision",
        "execution_generation",
        "admission_deadline",
    ):
        assert value[key] == prior[key]


@pytest.mark.asyncio
async def test_live_claim_does_not_hide_next_due_preflight(db):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    store = VMCreationPreflightStore(db)
    jobs = []
    for _ in range(2):
        job = await initial_job(db)
        req, ctx = candidate(job)
        await store.begin(job_id=str(job), request=req, fresh_context=ctx)
        jobs.append(str(job))
    one = await store.claim_due(limit=1)
    two = await store.claim_due(limit=1)
    assert len(one) == len(two) == 1
    assert one[0]["job_id"] != two[0]["job_id"]


@pytest.mark.asyncio
async def test_control_held_head_does_not_hide_next_due_preflight(db):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    store = VMCreationPreflightStore(db)
    jobs = []
    for _ in range(2):
        job = await initial_job(db)
        req, ctx = candidate(job)
        await store.begin(job_id=str(job), request=req, fresh_context=ctx)
        jobs.append(job)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context || jsonb_build_object('_completion_control_claim',jsonb_build_object('version',1,'expires_epoch',extract(epoch FROM clock_timestamp())+3600)) WHERE id=$1",
            jobs[0],
        )
    results = await store.claim_due(limit=1)
    assert len(results) == 1
    assert results[0]["job_id"] == str(jobs[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "generation", "deadline"])
async def test_resolution_handoff_rejects_execution_snapshot_drift(db, change):
    job, store, claim, resolved = await resolving(db)
    async with db.acquire() as conn:
        if change == "revision":
            await conn.execute(
                "UPDATE srw_execution_specs SET revision='revision-changed' WHERE work_id=$1",
                job,
            )
        elif change == "generation":
            await conn.execute(
                "UPDATE srw_execution_specs SET generation=generation+1 WHERE work_id=$1",
                job,
            )
        else:
            await conn.execute(
                "UPDATE srw_execution_specs SET created_at=created_at+interval '1 hour' WHERE work_id=$1",
                job,
            )
    with pytest.raises(VMCreationRetryConflict, match="execution_manifest_changed"):
        await store.complete_resolution(claim, resolved)
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1)", job
        )
        assert await conn.fetchval(
            "SELECT context->'vm'->'creation_request' FROM jobs WHERE id=$1", job
        ) in (None, "null")


@pytest.mark.asyncio
async def test_original_preflight_deadline_expires_without_manifest_change(db):
    job = await initial_job(db, timeout=1)
    request, fresh = candidate(job)
    store = VMCreationPreflightStore(db)
    value = await store.begin(job_id=str(job), request=request, fresh_context=fresh)
    await asyncio.sleep(1.1)
    assert await store.claim_due(limit=1) == []
    async with db.acquire() as conn:
        current = json.loads(
            await conn.fetchval(
                "SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1", job
            )
        )
    assert current["state"] == "attention"
    assert current["reason"] == "job_admission_expired"
    assert current["admission_deadline"] == value["admission_deadline"]
