"""A completed physical teardown must not strand the admitted S36 request."""

import logging
import json
import asyncio
from dataclasses import replace
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.database.migrate import run_migrations
from orchestrator.services.completion_effects import (
    CompletionEffectDependencies,
    replay_cancelled_container_completion_teardown,
    run_completion_workspace_teardown,
)
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    cleanup_intent_digest,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle
from tests.test_job_terminal_vm_cleanup import controls
from tests.test_vm_workspace_recovery_real_postgres import (
    checkpoint_row_counts,
    prepare_checkpoint_rows,
    insert_recovery,
)
from tests.test_workspace_cleanup_retry_real_postgres import (
    _absent_pod_provisioner,
    _capture,
)

pg_dsn = lifecycle.pg_dsn
db = lifecycle.db


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(
            pool,
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app",
        )


async def _stranded_completion_parent(db):
    (
        job,
        runtime,
        _creation,
        _state,
    ) = await lifecycle._create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='failed',execution_lane='stateless' WHERE id=$1",
            job,
        )
        await conn.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,max_attempts) "
            "VALUES($1,'worker_batch','done',5,5)",
            job,
        )
    await prepare_checkpoint_rows(db, job, checkpoint_count=2)
    assert await db.record_managed_repository_workspace_process_zero(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=runtime,
    )
    intent = await _capture(
        db, job, runtime, owner_kind="job", pvc=uuid4(), service=uuid4()
    )
    owner = WorkspaceOwner.job(str(job))
    provisioner, physical = _absent_pod_provisioner(db, owner, intent)
    calls = []

    async def archive(job_id):
        assert job_id == str(job)
        calls.append(job_id)
        result = await provisioner.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime,
            intent_generation=intent["intent_generation"],
        )
        assert result.settled
        if len(calls) == 1:
            # S36 catches an external failure before acknowledging its own
            # admission. Physical cleanup may already be durably complete.
            raise RuntimeError("external teardown acknowledgement unavailable")
        return ["k8s workspace released"]

    dependencies = CompletionEffectDependencies(
        store=db,
        container_provisioner=provisioner,
        vm_provisioner=None,
        get_container_context=lambda row: row.get("context", {}).get(
            "workspace_container", {}
        ),
        get_vm_context=lambda _row: {},
        archive_and_cleanup_workspace=archive,
        s36_exact_absence_timeout_seconds=lambda: 1.0,
        logger=logging.getLogger(__name__),
        recovery_store=VMWorkspaceRecoveryStore(db),
    )
    first = await run_completion_workspace_teardown(
        str(job), None, dependencies=dependencies
    )
    assert first["teardown_disposition"] == "retry_pending"
    assert physical == {}
    assert (
        provisioner._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    )
    assert provisioner._core_api.delete_namespaced_service.call_count == 1
    assert await db.cancel_stateless_job(str(job)) == (True, False)
    async with db.acquire() as conn:
        parent = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1",
            job,
        )
        assert await conn.fetchval(
            "SELECT settled_at IS NOT NULL FROM "
            "managed_repository_workspace_cleanup_intents WHERE id=$1",
            intent["id"],
        )
    assert parent["source"] == "completion_workspace_teardown"
    assert parent["pvc_uid"] is None
    assert parent["parent_admission_id"] is None
    assert parent["completed_at"] is None and parent["outcome"] is None
    assert parent["request_id"] == uuid5(
        uuid5(NAMESPACE_URL, f"completion-cleanup:{job}"), "legacy_workspace:none"
    )
    assert parent["intent_digest"] == cleanup_intent_digest(
        {
            "purge_workspace": True,
            "owner_kind": "job",
            "owner_id": str(job),
            "pvc_uid": "",
            "resource": "legacy_workspace",
            "source": "completion_workspace_teardown",
        }
    )
    return job, parent, dependencies, calls, provisioner


@pytest.mark.asyncio
async def test_exact_s36_parent_blocks_strict_prune_after_physical_settlement(db):
    job, parent, _dependencies, calls, _provisioner = await _stranded_completion_parent(
        db
    )
    assert await db.quiesce_cancelled_stateless_vm_parent(str(job)) is False
    with pytest.raises(RuntimeError, match="blocked by workspace recovery authority"):
        await db.finalize_cancelled_stateless_job(str(job))
    assert await checkpoint_row_counts(db, job) == (2, 2, 2)
    assert calls == [str(job)]
    async with db.acquire() as conn:
        assert (
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                parent["id"],
            )
            == parent
        )
    assert await db.stateless_cancel_cleanup_pending(str(job)) is True


@pytest.mark.asyncio
async def test_replaying_original_s36_completes_parent_then_prunes_without_delete_replay(
    db,
):
    job, parent, dependencies, calls, provisioner = await _stranded_completion_parent(
        db
    )
    second = await run_completion_workspace_teardown(
        str(job), None, dependencies=dependencies
    )
    assert second["teardown_disposition"] == "completed"
    async with db.acquire() as conn:
        replayed = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", parent["id"]
        )
        assert (
            replayed["completed_at"] is not None and replayed["outcome"] == "completed"
        )
        assert replayed["request_id"] == parent["request_id"]
        assert replayed["intent_digest"] == parent["intent_digest"]
    assert await db.finalize_cancelled_stateless_job(str(job)) is True
    assert await checkpoint_row_counts(db, job) == (0, 0, 0)
    assert await db.complete_stateless_cancel_cleanup(str(job)) is True
    assert calls == [str(job), str(job)]
    assert (
        provisioner._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    )
    assert provisioner._core_api.delete_namespaced_service.call_count == 1


@pytest.mark.asyncio
async def test_normal_cancel_settle_replays_exact_pending_completion_parent(db):
    (
        job,
        _parent,
        dependencies,
        _calls,
        _provisioner,
    ) = await _stranded_completion_parent(db)
    operation = controls(store=db, archive=dependencies.archive_and_cleanup_workspace)
    operation.dependencies.replay_completion_workspace_teardown = lambda replay: (
        replay_cancelled_container_completion_teardown(
            replay, dependencies=dependencies
        )
    )
    assert await operation.wait_for_stateless_cancel_settle(str(job), timeout_seconds=0)
    assert await checkpoint_row_counts(db, job) == (0, 0, 0)
    assert await db.stateless_cancel_cleanup_pending(str(job)) is False

    assert await db.prepare_stateless_job_for_delete(str(job)) is True
    assert await db.delete_job(str(job), prepared_stateless=True) is True
    assert await db.get_job(str(job)) is None


@pytest.mark.asyncio
async def test_completion_replay_refuses_changed_terminal_queue_token(db):
    (
        job,
        _parent,
        dependencies,
        calls,
        _provisioner,
    ) = await _stranded_completion_parent(db)
    replay = await db.cancelled_container_completion_replay(str(job))
    assert replay is not None
    async with db.acquire() as conn:
        await conn.execute("UPDATE run_queue SET lease_token=6 WHERE unit_id=$1", job)
    before = await _snapshot(db, job)
    assert not await replay_cancelled_container_completion_teardown(
        replay, dependencies=dependencies
    )
    assert await _snapshot(db, job) == before
    assert calls == [str(job)]


async def _snapshot(db, job):
    async with db.acquire() as conn:
        return (
            await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job),
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job),
            await conn.fetch(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 ORDER BY id",
                job,
            ),
            await conn.fetch(
                "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1 ORDER BY id",
                job,
            ),
            await conn.fetch(
                "SELECT * FROM managed_repository_workspace_creation_reservations WHERE owner_id=$1 ORDER BY id",
                job,
            ),
            await checkpoint_row_counts(db, job),
        )


async def _change_proof(db, job, parent, change):
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        if change.startswith("marker_"):
            context["_stateless_cancel_cleanup_pending"] = {
                "marker_false": False,
                "marker_null": None,
                "marker_string": "true",
            }[change]
        elif change.startswith("vm_"):
            context["vm"] = None if change == "vm_null" else {"status": "deleted"}
        elif change == "wrong_runtime":
            context["workspace_container"]["_runtime_incarnation"] = str(uuid4())
        elif change == "wrong_creation":
            context["workspace_container"]["_creation_reservation_id"] = str(uuid4())
        elif change == "wrong_creation_token":
            context["workspace_container"]["_creation_claim_token"] = "100000"
        elif change == "inherited":
            context["inherits_parent_workspace"] = True
        elif change == "context_array":
            context = []
        elif change == "worker_hold":
            context["_worker_execution_hold"] = None
        elif change == "resume_pending":
            context["_stateless_resume_pending"] = False
        elif change == "control_claim":
            context["_completion_control_claim"] = None
        else:
            context = None
        if context is not None:
            # Model a persisted malformed/stale previous-release envelope.
            # Production never disables these runtime authority triggers.
            await lifecycle._execute_pre_0195(
                conn,
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job,
                json.dumps(context),
            )
            return
        if change in {"status", "lane", "assignment", "parent"}:
            clause, value = {
                "status": ("status", "completed"),
                "lane": ("execution_lane", "pinned"),
                "assignment": ("assigned_agent_id", uuid4()),
                "parent": ("parent_job_id", uuid4()),
            }[change]
            await lifecycle._execute_pre_0195(
                conn, f"UPDATE jobs SET {clause}=$2 WHERE id=$1", job, value
            )
        elif change.startswith("queue_"):
            clause = {
                "queue_leased": "state='leased',leased_by='owner',leased_until=clock_timestamp()+interval '1 minute'",
                "queue_queued": "state='queued'",
                "queue_leftover_holder": "leased_by='owner'",
                "queue_leftover_expiry": "leased_until=clock_timestamp()+interval '1 minute'",
            }[change]
            await conn.execute(f"UPDATE run_queue SET {clause} WHERE unit_id=$1", job)
        elif change in {"source", "request", "digest", "pvc", "completed_wrong"}:
            clause, value = {
                "source": ("source", "other_teardown"),
                "request": ("request_id", uuid4()),
                "digest": ("intent_digest", "sha256:changed"),
                "pvc": ("pvc_uid", uuid4()),
                "completed_wrong": ("outcome", "identity_superseded"),
            }[change]
            suffix = (
                ",completed_at=clock_timestamp()" if change == "completed_wrong" else ""
            )
            await conn.execute(
                f"UPDATE vm_workspace_cleanup_admissions SET {clause}=$2{suffix} WHERE id=$1",
                parent["id"],
                value,
            )
        elif change in {"extra_admission", "admission_parent"}:
            other = uuid4()
            await conn.execute(
                "INSERT INTO vm_workspace_cleanup_admissions (id,owner_kind,owner_id,source,request_id,intent_digest,"
                "parent_admission_id,completed_at,outcome) "
                "VALUES($1,'job',$2,'other_teardown',$3,'sha256:other',$4,"
                "CASE WHEN $5 THEN clock_timestamp() END,CASE WHEN $5 THEN 'completed' END)",
                other,
                job,
                uuid4(),
                parent["id"] if change == "extra_admission" else None,
                change == "admission_parent",
            )
            if change == "admission_parent":
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET parent_admission_id=$2 WHERE id=$1",
                    parent["id"],
                    other,
                )
        elif change in {"receipt_missing", "receipt_wrong", "receipt_backend"}:
            if change == "receipt_missing":
                await conn.execute(
                    "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1",
                    job,
                )
            else:
                clause, value = (
                    ("runtime_incarnation", str(uuid4()))
                    if change == "receipt_wrong"
                    else ("scope", "stateless_workspace")
                )
                await conn.execute(
                    f"UPDATE managed_repository_process_zero_receipts SET {clause}=$2 WHERE owner_id=$1",
                    job,
                    value,
                )
        elif change == "creation_open":
            await lifecycle._execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET settled_at=NULL,result_kind=NULL,phase='runtime_bound' WHERE owner_id=$1",
                job,
            )
        elif change == "cleanup_open":
            await lifecycle._execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET settled_at=NULL,cleanup_completed_at=NULL,"
                "projection_transaction_id=NULL,result_kind=NULL,phase='process_zero' WHERE owner_id=$1",
                job,
            )
        elif change == "cleanup_superseded":
            await lifecycle._execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_cleanup_intents SET "
                "resource_policy='preserve',reclaim_shared_resources=FALSE,"
                "projection_transaction_id=NULL,result_kind='superseded',phase='superseded' WHERE owner_id=$1",
                job,
            )
        elif change == "creation_capture_mismatch":
            await lifecycle._execute_pre_0195(
                conn,
                "UPDATE managed_repository_workspace_creation_reservations SET pvc_uid=$2 WHERE owner_id=$1",
                job,
                uuid4(),
            )
        elif change == "successor_creation":
            await conn.execute(
                "INSERT INTO managed_repository_workspace_creation_reservations "
                "(owner_kind,owner_id,scope,claimed_by,desired_manifest_digest,expires_at,phase,settled_at,result_kind) "
                "VALUES('job',$1,'workspace_container','successor',repeat('1',64),clock_timestamp()+interval '1 hour',"
                "'aborted',clock_timestamp(),'aborted')",
                job,
            )
        elif change == "access":
            await conn.execute(
                "INSERT INTO vm_idle_access_leases(owner_kind,owner_id,provision_generation,vm_uid,kind,claimed_by,expires_at,max_expires_at) "
                "VALUES('job',$1,$2,$3,'ide','owner',clock_timestamp()+interval '1 minute',clock_timestamp()+interval '1 hour')",
                job,
                uuid4(),
                uuid4(),
            )
        elif change == "recovery":
            pass
        else:
            raise AssertionError(change)
    if change == "recovery":
        await insert_recovery(db, owner_id=job)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "marker_false",
        "marker_null",
        "marker_string",
        "vm_null",
        "vm_nonempty",
        "wrong_runtime",
        "wrong_creation",
        "wrong_creation_token",
        "inherited",
        "context_array",
        "worker_hold",
        "resume_pending",
        "control_claim",
        "status",
        "lane",
        "assignment",
        "parent",
        "queue_leased",
        "queue_queued",
        "queue_leftover_holder",
        "queue_leftover_expiry",
        "source",
        "request",
        "digest",
        "pvc",
        "extra_admission",
        "admission_parent",
        "completed_wrong",
        "receipt_missing",
        "receipt_wrong",
        "receipt_backend",
        "creation_open",
        "cleanup_open",
        "cleanup_superseded",
        "creation_capture_mismatch",
        "successor_creation",
        "access",
        "recovery",
    ],
)
async def test_refused_original_completion_replay_preserves_every_boundary(db, change):
    job, parent, dependencies, calls, provisioner = await _stranded_completion_parent(
        db
    )
    replay = await db.cancelled_container_completion_replay(str(job))
    assert replay is not None
    await _change_proof(db, job, parent, change)
    before = await _snapshot(db, job)
    assert await db.cancelled_container_completion_replay(str(job)) is None
    assert not await replay_cancelled_container_completion_teardown(
        replay, dependencies=dependencies
    )
    assert await _snapshot(db, job) == before
    assert calls == [str(job)]
    assert provisioner._core_api.delete_namespaced_service.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["queue_queued", "wrong_runtime", "extra_admission"])
async def test_context_or_queue_changes_during_external_archive_do_not_acknowledge(
    db, change
):
    job, parent, dependencies, calls, _provisioner = await _stranded_completion_parent(
        db
    )
    replay = await db.cancelled_container_completion_replay(str(job))
    entered, proceed = asyncio.Event(), asyncio.Event()
    archive = dependencies.archive_and_cleanup_workspace

    async def blocked_archive(job_id):
        entered.set()
        await proceed.wait()
        return await archive(job_id)

    raced = replace(dependencies, archive_and_cleanup_workspace=blocked_archive)
    task = asyncio.create_task(
        replay_cancelled_container_completion_teardown(replay, dependencies=raced)
    )
    await asyncio.wait_for(entered.wait(), 2)
    await _change_proof(db, job, parent, change)
    proceed.set()
    assert not await asyncio.wait_for(task, 2)
    async with db.acquire() as conn:
        assert (
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                parent["id"],
            )
            == parent
        )
    assert await checkpoint_row_counts(db, job) == (2, 2, 2)
    assert calls == [str(job), str(job)]


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_original_acknowledgement_ambiguity_replays_same_admission(
    db, monkeypatch, committed
):
    job, parent, dependencies, calls, provisioner = await _stranded_completion_parent(
        db
    )
    replay = await db.cancelled_container_completion_replay(str(job))
    complete = (
        dependencies.recovery_store.complete_cancelled_container_completion_replay
    )

    async def ambiguous_ack(proof):
        if committed:
            assert await complete(proof)
        raise ConnectionError("acknowledgement unavailable")

    monkeypatch.setattr(
        dependencies.recovery_store,
        "complete_cancelled_container_completion_replay",
        ambiguous_ack,
    )
    assert not await replay_cancelled_container_completion_teardown(
        replay, dependencies=dependencies
    )
    async with db.acquire() as conn:
        recorded = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", parent["id"]
        )
    assert (recorded["completed_at"] is not None) is committed
    assert await checkpoint_row_counts(db, job) == (2, 2, 2)
    assert await db.stateless_cancel_cleanup_pending(str(job)) is True
    monkeypatch.setattr(
        dependencies.recovery_store,
        "complete_cancelled_container_completion_replay",
        complete,
    )
    assert await replay_cancelled_container_completion_teardown(
        replay, dependencies=dependencies
    )
    async with db.acquire() as conn:
        admissions = await conn.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_id=$1", job
        )
    assert len(admissions) == 1 and admissions[0]["id"] == parent["id"]
    assert admissions[0]["request_id"] == parent["request_id"]
    assert admissions[0]["intent_digest"] == parent["intent_digest"]
    assert calls == [str(job)] * (2 if committed else 3)
    if committed:
        assert admissions[0] == recorded
    assert (
        provisioner._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    )
    assert provisioner._core_api.delete_namespaced_service.call_count == 1


@pytest.mark.asyncio
async def test_interrupted_archive_preserves_original_authority_for_restart(db):
    job, parent, dependencies, calls, _provisioner = await _stranded_completion_parent(
        db
    )
    replay = await db.cancelled_container_completion_replay(str(job))

    async def interrupted_archive(_job_id):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await replay_cancelled_container_completion_teardown(
            replay,
            dependencies=replace(
                dependencies, archive_and_cleanup_workspace=interrupted_archive
            ),
        )
    async with db.acquire() as conn:
        assert (
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                parent["id"],
            )
            == parent
        )
    assert await checkpoint_row_counts(db, job) == (2, 2, 2)
    assert await db.stateless_cancel_cleanup_pending(str(job)) is True
    assert await replay_cancelled_container_completion_teardown(
        replay, dependencies=dependencies
    )
    assert calls == [str(job), str(job)]
