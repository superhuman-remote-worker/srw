"""Retry admission and existing cleanup/job/queue authority on real PostgreSQL."""

import asyncio
import json
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_creation_request import (
    build_vm_creation_request,
    capture_vm_creation_request,
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryStore,
    VMCreationRetryConflict,
)
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.worker_queue import enqueue_worker_batch
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db as _postgres_db_fixture,
    pg_dsn,  # noqa: F401
)


postgres_db_fixture = _postgres_db_fixture


@pytest_asyncio.fixture
async def db(postgres_db_fixture):
    yield postgres_db_fixture
    async with postgres_db_fixture.acquire() as conn:
        await conn.execute("TRUNCATE jobs CASCADE")


CONFIG_DIGEST = "sha256:" + "a" * 64


async def admitted_job(db, *, lane="pinned", timeout=3600):
    job, generation, execution = uuid4(), uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,context) VALUES($1,'retry','paused',$2,$3::jsonb)",
            job,
            lane,
            json.dumps(
                {
                    "vm": {"provision_generation": str(generation), "status": "failed"},
                    "completion_decision": {"preserve": True},
                }
            ),
        )
        await conn.execute(
            "INSERT INTO srw_execution_specs(id,work_kind,work_id,document,resolved,revision,harness_adapter) "
            "VALUES($1,'Job',$2,'{}',$3::jsonb,'revision-1','srw/v1')",
            execution,
            job,
            json.dumps({"spec": {"timeoutSeconds": timeout}}),
        )
    request = build_vm_creation_request(
        job_id=str(job),
        agent_config="worker_base",
        vm_image="pinned:image",
        cpu_cores=8,
        memory="16Gi",
        description="retry",
        network_tier="restricted",
        provision_generation=str(generation),
    )
    snapshot = await capture_vm_creation_request(
        db,
        job_id=str(job),
        generation=str(generation),
        request=request,
        controller_configuration_digest=CONFIG_DIGEST,
    )
    proposal = {
        "origin": "initial",
        "expected_status": "paused",
        "request_digest": snapshot["request_digest"],
        "controller_configuration_digest": CONFIG_DIGEST,
        "expected_pvc_uid": None,
    }
    return job, generation, proposal


async def admit(db, job, generation, proposal, request_id=None):
    async with db.acquire() as conn:
        async with conn.transaction():
            return await VMCreationRetryStore(db).admit_on_conn(
                conn,
                job_id=str(job),
                expected_generation=str(generation),
                request_id=str(request_id or uuid4()),
                proposal=proposal,
            )


@pytest.mark.asyncio
async def test_duplicate_admission_preserves_generation_context_and_one_request(db):
    job, generation, proposal = await admitted_job(db)
    first, second = await asyncio.gather(
        admit(db, job, generation, proposal), admit(db, job, generation, proposal)
    )
    assert first["request_id"] == second["request_id"]
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        assert context["vm"]["provision_generation"] == str(generation)
        assert context["completion_decision"] == {"preserve": True}
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
            )
            == 1
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("UPDATE jobs SET context=context-'vm' WHERE id=$1", job)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "digest", "deadline", "unproven"])
async def test_invalid_admission_rolls_back_without_request_or_resume(db, change):
    job, generation, proposal = await admitted_job(
        db, timeout=-1 if change == "deadline" else 3600
    )
    if change == "generation":
        generation = uuid4()
    elif change == "digest":
        proposal["request_digest"] = "sha256:" + "b" * 64
    elif change == "unproven":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_request,controller_configuration_authenticated}','false') WHERE id=$1",
                job,
            )
    with pytest.raises(VMCreationRetryConflict):
        await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
            )
            == 0
        )


@pytest.mark.asyncio
async def test_schema_rejects_immutable_identity_mutation(db):
    job, generation, proposal = await admitted_job(db)
    row = await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET request_digest=$2 WHERE request_id=$1",
                row["request_id"],
                "sha256:" + "b" * 64,
            )


@pytest.mark.asyncio
async def test_expired_observer_claim_does_not_release_create_reservation(db):
    job, generation, proposal = await admitted_job(db)
    await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    old = (await store.claim_due(limit=1))[0]
    authorized = await store.authorize_controller(
        request_id=str(old["request_id"]),
        claim_token=str(old["claim_token"]),
        observed={
            "job_id": str(job),
            "provision_generation": str(generation),
            "request_digest": proposal["request_digest"],
            "controller_configuration_digest": CONFIG_DIGEST,
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"] is True
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            old["request_id"],
        )
    new = (await store.claim_due(limit=1))[0]
    assert new["claim_token"] != old["claim_token"]
    assert not await store.apply_observation(
        request_id=str(old["request_id"]),
        claim_token=str(old["claim_token"]),
        expected_revision=old["revision"],
        observation={"outcome": "transport_unknown"},
    )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            authorized["admission_id"],
        )


@pytest.mark.asyncio
async def test_existing_cleanup_blocks_retry_admission(db):
    job, generation, proposal = await admitted_job(db)
    cleanup = await VMWorkspaceRecoveryStore(db).acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job,
        pvc_uid=None,
        request_id=uuid4(),
        source="test_cleanup",
        intent_digest="test-digest",
    )
    assert cleanup.allowed
    with pytest.raises(VMCreationRetryConflict):
        await admit(db, job, generation, proposal)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_actual_cancel_revokes_claim_and_new_controller_authorization(db, lane):
    job, generation, proposal = await admitted_job(db, lane=lane)
    await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claimed = (await store.claim_due(limit=1))[0]
    if lane == "pinned":
        assert await db.linearize_pinned_cancel(str(job), expected_status="paused")
    else:
        assert (await db.cancel_stateless_job(str(job)))[0]
    async with db.acquire() as conn:
        cancelled = await conn.fetchrow(
            "SELECT state,claim_token FROM vm_creation_retries WHERE job_id=$1", job
        )
    assert cancelled["state"] == "cancel_requested"
    assert cancelled["claim_token"] is None
    denied = await store.authorize_controller(
        request_id=str(claimed["request_id"]),
        claim_token=str(claimed["claim_token"]),
        observed={},
    )
    assert denied["allowed"] is False


@pytest.mark.asyncio
async def test_stateless_admission_holds_claims_without_resetting_attempts(db):
    job, generation, proposal = await admitted_job(db, lane="stateless")
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=job)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1", job
        )
    await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            job,
        )
    assert queue["state"] == "done"
    assert queue["attempts_since_completion"] == 3


@pytest.mark.asyncio
async def test_pending_creation_rejects_a_reopened_worker_claim_without_spending_attempt(
    db,
):
    from shared.worker_queue import claim_worker_batch

    job, generation, proposal = await admitted_job(db, lane="stateless")
    await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=job)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1", job
        )
    assert (
        await claim_worker_batch(
            db, pod_name="must-not-run", completion_commands_enabled=False
        )
        is None
    )
    async with db.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            job,
        )
        assert queue["state"] != "leased"
        assert queue["attempts_since_completion"] == 3


def observed(job, generation, proposal):
    return {
        "job_id": str(job),
        "provision_generation": str(generation),
        "request_digest": proposal["request_digest"],
        "controller_configuration_digest": CONFIG_DIGEST,
        "expected_pvc_uid": proposal["expected_pvc_uid"],
    }


@pytest.mark.asyncio
async def test_controller_authorization_and_cleanup_compete_for_one_existing_authority(
    db,
):
    job, generation, proposal = await admitted_job(db)
    await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claimed = (await store.claim_due(limit=1))[0]
    allowed, cleanup = await asyncio.wait_for(
        asyncio.gather(
            store.authorize_controller(
                request_id=str(claimed["request_id"]),
                claim_token=str(claimed["claim_token"]),
                observed=observed(job, generation, proposal),
            ),
            VMWorkspaceRecoveryStore(db).acquire_cleanup_permit(
                owner_kind="job",
                owner_id=job,
                pvc_uid=None,
                request_id=uuid4(),
                source="competing-cleanup",
                intent_digest="test-digest",
            ),
        ),
        timeout=5,
    )
    assert allowed["allowed"] is not cleanup.allowed
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 AND completed_at IS NULL",
                job,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_cancellation_racing_authorization_retains_any_issued_reservation(db):
    job, generation, proposal = await admitted_job(db)
    await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claimed = (await store.claim_due(limit=1))[0]
    authorization, cancelled = await asyncio.wait_for(
        asyncio.gather(
            store.authorize_controller(
                request_id=str(claimed["request_id"]),
                claim_token=str(claimed["claim_token"]),
                observed=observed(job, generation, proposal),
            ),
            db.linearize_pinned_cancel(str(job), expected_status="paused"),
        ),
        timeout=5,
    )
    assert cancelled
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state,creation_admission_id FROM vm_creation_retries WHERE job_id=$1",
            job,
        )
        assert row["state"] == "cancel_requested"
        if authorization["allowed"]:
            assert row["creation_admission_id"] == authorization["admission_id"]
            assert await conn.fetchval(
                "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
                authorization["admission_id"],
            )


@pytest.mark.asyncio
async def test_completed_job_revokes_creation_without_waiting_for_a_reconciler(db):
    job, generation, proposal = await admitted_job(db)
    await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job)
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE job_id=$1", job
            )
            == "cancel_requested"
        )


@pytest.mark.asyncio
async def test_store_backoff_uses_db_time_and_capacity_clears_transport_outage(db):
    job, generation, proposal = await admitted_job(db)
    await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    assert await store.apply_observation(
        request_id=str(claim["request_id"]),
        claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"],
        observation={"outcome": "transport_unknown"},
    )
    assert await store.claim_due(limit=1) == []
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET next_probe_at=clock_timestamp(),transport_outage_started_at=clock_timestamp()-interval '20 minutes' WHERE job_id=$1",
            job,
        )
    claim = (await store.claim_due(limit=1))[0]
    assert await store.apply_observation(
        request_id=str(claim["request_id"]),
        claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"],
        observation={"outcome": "capacity_wait", "authenticated": True},
    )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state,transport_outage_started_at,boot_counted FROM vm_creation_retries WHERE job_id=$1",
            job,
        )
    assert row["state"] == "queued"
    assert row["transport_outage_started_at"] is None
    assert row["boot_counted"] is False


@pytest.mark.asyncio
async def test_admission_cannot_fence_an_active_worker_lease(db):
    job, generation, proposal = await admitted_job(db, lane="stateless")
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=job)
        await conn.execute(
            "UPDATE run_queue SET state='leased',leased_by='existing',leased_until=clock_timestamp()+interval '1 minute' WHERE unit_id=$1",
            job,
        )
    with pytest.raises(VMCreationRetryConflict, match="worker_lease_active"):
        await admit(db, job, generation, proposal)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job)
            == "leased"
        )


@pytest.mark.asyncio
async def test_retained_request_cannot_be_admitted_as_new_disk(db):
    job, generation, proposal = await admitted_job(db)
    async with db.acquire() as conn:
        raw = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        snapshot = raw["vm"]["creation_request"]
        snapshot["request"]["workspace_storage"] = {"pvc_uid": str(uuid4())}
        from shared.vm_creation_retry import canonical_request_digest

        snapshot["request_digest"] = canonical_request_digest(snapshot["request"])
        proposal["request_digest"] = snapshot["request_digest"]
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, json.dumps(raw)
        )
    with pytest.raises(VMCreationRetryConflict, match="retained_disk_changed"):
        await admit(db, job, generation, proposal)


@pytest.mark.asyncio
async def test_attention_resume_requeues_failed_job_without_new_request(db):
    job, generation, proposal = await admitted_job(db, lane="stateless")
    row = await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    await store.apply_observation(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        expected_revision=claim["revision"],
        observation={"outcome": "blocked", "authenticated": True},
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='failed' WHERE id=$1", job)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=4 WHERE unit_id=$1", job
        )
    resumed = await admit(db, job, generation, proposal)
    assert resumed["request_id"] == row["request_id"]
    assert resumed["state"] == "queued"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", job) == "paused"
        )
        queue = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            job,
        )
        assert dict(queue) == {"state": "done", "attempts_since_completion": 4}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proof", ["valid", "missing_receipt", "wrong_cleanup", "forgotten_retained_disk"]
)
@pytest.mark.parametrize("with_binding", [True, False])
async def test_retained_disk_requires_exact_predecessor_receipt_and_cleanup(
    db, proof, with_binding
):
    from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
    from shared.vm_creation_retry import canonical_request_digest

    job, generation, proposal = await admitted_job(db)
    pvc, predecessor_generation, predecessor_vm, cleanup = (uuid4() for _ in range(4))
    evidence = {
        "provision_generation": str(predecessor_generation),
        "vm_uid": str(predecessor_vm),
    }
    intent = {
        "owner_kind": "job",
        "owner_id": str(job),
        **evidence,
        "pvc_uid": str(pvc),
        "purge_disk": False,
        "resource": "vm_workspace",
        "source": "lifecycle_vm_reap",
    }
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        context["last_vm"] = {
            **evidence,
            "identity_authenticated": True,
            "identity_provision_generation": str(predecessor_generation),
            "rootdisk_pvc_uid": str(pvc),
        }
        snapshot = context["vm"]["creation_request"]
        if with_binding:
            snapshot["request"]["workspace_storage"] = {"pvc_uid": str(pvc)}
        snapshot["request_digest"] = canonical_request_digest(snapshot["request"])
        proposal.update(
            request_digest=snapshot["request_digest"],
            expected_pvc_uid=str(pvc),
            predecessor_evidence=evidence,
            predecessor_cleanup_admission_id=str(cleanup),
        )
        await conn.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job, json.dumps(context)
        )
        if proof != "missing_receipt":
            await conn.execute(
                "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2)",
                job,
                str(predecessor_generation),
            )
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'lifecycle_vm_reap',$4,$5,clock_timestamp(),'completed')",
            cleanup,
            job,
            pvc,
            uuid4(),
            cleanup_intent_digest(
                {**intent, "purge_disk": True} if proof == "wrong_cleanup" else intent
            ),
        )
    if proof == "forgotten_retained_disk":
        proposal.update(
            expected_pvc_uid=None,
            predecessor_evidence={},
            predecessor_cleanup_admission_id=None,
        )
    if proof != "valid":
        with pytest.raises(VMCreationRetryConflict):
            await admit(db, job, generation, proposal)
        return
    row = await admit(db, job, generation, proposal)
    assert row["expected_pvc_uid"] == pvc
    assert row["predecessor_cleanup_admission_id"] == cleanup
    assert row["predecessor_evidence"]["receipt_id"]
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    evidence = observed(job, generation, proposal)
    evidence["expected_pvc_uid"] = str(pvc)
    permit = await store.authorize_controller(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=evidence,
    )
    assert permit["allowed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("completion_commands_enabled", [False, True])
async def test_pending_creation_at_queue_head_does_not_starve_runnable_job(
    db, completion_commands_enabled
):
    from shared.worker_queue import claim_worker_batch

    pending_job, generation, proposal = await admitted_job(db, lane="stateless")
    await admit(db, pending_job, generation, proposal)
    runnable_job, _, _ = await admitted_job(db, lane="stateless")
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=pending_job, priority=100)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1",
            pending_job,
        )
        await enqueue_worker_batch(conn, job_id=runnable_job, priority=0)
    # A claimant may skip the held row or close it on its first pass. Neither
    # path may repeatedly select it instead of the next runnable worker.
    claim = None
    for _ in range(2):
        claim = await claim_worker_batch(
            db,
            pod_name="healthy-worker",
            completion_commands_enabled=completion_commands_enabled,
        )
        if claim is not None:
            break
    assert claim is not None
    assert claim.unit_id == runnable_job
    async with db.acquire() as conn:
        held = await conn.fetchrow(
            "SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1",
            pending_job,
        )
        assert held["state"] != "leased"
        assert held["attempts_since_completion"] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("expires", ["claim", "deadline"])
async def test_authorization_rechecks_expiry_after_waiting_for_job_lock(db, expires):
    job, generation, proposal = await admitted_job(
        db, timeout=2 if expires == "deadline" else 3600
    )
    row = await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    async with db.acquire() as conn:
        if expires == "claim":
            await conn.execute(
                "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '1 second' WHERE request_id=$1",
                row["request_id"],
            )
        async with conn.transaction():
            await conn.execute("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job)
            authorization = asyncio.create_task(
                store.authorize_controller(
                    request_id=str(row["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    observed=observed(job, generation, proposal),
                )
            )
            await asyncio.sleep(2.2 if expires == "deadline" else 1.3)
            assert not authorization.done()
        result = await asyncio.wait_for(authorization, timeout=5)
        assert result == {
            "allowed": False,
            "reason": "retry_claim_changed"
            if expires == "claim"
            else "job_admission_expired",
        }
        assert (
            await conn.fetchval(
                "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            is None
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_id=$1",
                job,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_observation_rechecks_claim_expiry_after_retry_row_lock_wait(db):
    job, generation, proposal = await admitted_job(db)
    row = await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '1 second' WHERE request_id=$1",
            row["request_id"],
        )
        async with conn.transaction():
            await conn.execute(
                "SELECT request_id FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                row["request_id"],
            )
            observation = asyncio.create_task(
                store.apply_observation(
                    request_id=str(row["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    expected_revision=claim["revision"],
                    observation={"outcome": "transport_unknown"},
                )
            )
            await asyncio.sleep(1.3)
            assert not observation.done()
        assert await asyncio.wait_for(observation, timeout=5) is False
        current = await conn.fetchrow(
            "SELECT revision,backoff_attempt FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
        assert current["revision"] == claim["revision"]
        assert current["backoff_attempt"] == 0
