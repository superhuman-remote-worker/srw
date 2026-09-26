"""Real PG: unknown worker effects stay held across every admission route."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from fastapi import HTTPException

from tests import test_container_recovery_retention_real_postgres as retention
from tests import test_container_streaming_workspace_stop_real_postgres as stream
from shared import worker_queue
from shared.worker_execution_hold import hold_container_worker_attempt
from orchestrator.services.job_completion_commands import (
    CompletionFenceRejected,
    accept_completion_command,
)
from orchestrator.services import run_queue_reaper
from orchestrator.services.job_controls import JobControlOperations

pg_dsn = retention.pg_dsn
_schema_applied = retention._schema_applied
db = retention.db

HOLD_KEY = "_worker_execution_hold"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, {}, {"phase": "pending", "hold_id": "test"}])
@pytest.mark.parametrize("change", ["delete", "null", "replace", "stale_context"])
async def test_native_execution_hold_cannot_be_removed_or_rewritten(db, value, change):
    job_id = uuid4()
    context = {HOLD_KEY: value, "ordinary": "kept"}
    await db.execute(
        "INSERT INTO jobs(id,description,status,context) VALUES($1,'held job','paused',$2::jsonb)",
        job_id,
        json.dumps(context),
    )
    changed = dict(context)
    if change == "delete":
        changed.pop(HOLD_KEY)
    elif change == "null":
        changed[HOLD_KEY] = None if value is not None else {"phase": "pending"}
    elif change == "replace":
        changed[HOLD_KEY] = {"phase": "settled", "hold_id": "other"}
    else:
        changed = {"ordinary": "stale snapshot"}
    with pytest.raises(asyncpg.CheckViolationError, match="worker execution hold"):
        await db.execute(
            "UPDATE jobs SET context=$2::jsonb WHERE id=$1", job_id, json.dumps(changed)
        )
    assert (
        json.loads(
            await db.fetchval("SELECT context::text FROM jobs WHERE id=$1", job_id)
        )
        == context
    )


@pytest.mark.asyncio
async def test_native_execution_hold_allows_unrelated_merge_and_cancel(db):
    job_id = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,context) VALUES($1,'held job','paused',$2::jsonb)",
        job_id,
        json.dumps({HOLD_KEY: None}),
    )
    assert await db.merge_job_context(str(job_id), {"ordinary": "new"})
    assert await db.cancel_job(str(job_id))
    row = await retention.current_job(db, job_id)
    assert row["status"] == "cancelled"
    assert row["context"] == {HOLD_KEY: None, "ordinary": "new"}


async def hold(db, claim, **options):
    async with db.acquire() as conn:
        return await hold_container_worker_attempt(
            conn,
            job_id=claim.unit_id,
            lease_token=claim.lease_token,
            reason="typed_report_unaccepted",
            **options,
        )


@pytest.mark.asyncio
async def test_exact_hold_preserves_runtime_counters_and_refuses_all_resume_paths(db):
    claim = await stream.exact_claim(db)
    before = await retention.current_job(db, claim.unit_id)
    queue_before = dict(
        await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", claim.unit_id)
    )
    assert await hold(db, claim) == "held"
    held = await retention.current_job(db, claim.unit_id)
    receipt = held["context"][HOLD_KEY]
    token = held["context"]["_operator_pause_hold"]["hold_id"]
    assert (
        held["context"]["workspace_container"]
        == before["context"]["workspace_container"]
    )
    assert receipt["phase"] == "pending" and receipt["executor_pod_uid"] is None
    queue = dict(
        await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", claim.unit_id)
    )
    assert queue["state"] == "parked" and queue["lease_token"] == claim.lease_token + 1
    for key in (
        "attempts_since_completion",
        "input_seq",
        "consumed_seq",
        "max_attempts",
    ):
        assert queue[key] == queue_before[key]
    assert not await db.queue_stateless_job_for_resume(
        str(claim.unit_id),
        expected_status="paused",
        lift_operator_pause_hold=token,
        completion_commands_enabled=True,
    )
    assert not await db.queue_job_for_resume(
        str(claim.unit_id),
        expected_status="paused",
        lift_operator_pause_hold=token,
    )
    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(claim.unit_id),
        "workspace_container",
        expected_status="paused",
        lift_operator_pause_hold=token,
    )
    assert not await db.shed_workspace_context(
        str(claim.unit_id), "workspace_container"
    )
    assert not await db.shed_workspace_context(str(claim.unit_id), "vm")
    assert await db.queue_stateless_job_for_resume(
        str(claim.unit_id),
        {"queued_feedback": "keep this feedback", HOLD_KEY: None},
        expected_status="paused",
        completion_commands_enabled=True,
    )
    assert not await worker_queue.claim_worker_batch(
        db,
        pod_name="next",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert not await db.delete_job_context_keys(str(claim.unit_id), [HOLD_KEY])
    assert await db.merge_job_context(
        str(claim.unit_id), {HOLD_KEY: None, "ordinary": 1}
    )
    current = await retention.current_job(db, claim.unit_id)
    assert current["context"][HOLD_KEY] == receipt
    assert current["context"]["queued_feedback"] == "keep this feedback"
    assert (
        await db.fetchval(
            "SELECT attempts_since_completion FROM run_queue WHERE unit_id=$1",
            claim.unit_id,
        )
        == queue_before["attempts_since_completion"]
    )
    assert not await db.reserve_managed_repository_workspace_creation(
        str(claim.unit_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="forbidden",
        desired_manifest_digest="a" * 64,
    )
    assert await hold(db, claim) == "superseded"
    assert await db.cancel_job(str(claim.unit_id))
    assert (await retention.current_job(db, claim.unit_id))["context"][
        HOLD_KEY
    ] == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker", [None, False, 1, "pending", {}, {"phase": "settled"}]
)
async def test_malformed_presence_refuses_admin_assignment_and_resume(db, marker):
    job_id, agent_id = uuid4(), uuid4()
    await db.execute(
        "INSERT INTO agents(id,config_name,hostname,status) VALUES($1,'worker_base','admin-test','ready')",
        agent_id,
    )
    await db.execute(
        "INSERT INTO jobs(id,description,status,context) VALUES($1,'held','paused',$2::jsonb)",
        job_id,
        json.dumps({HOLD_KEY: marker}),
    )
    assert not await db.claim_job_for_agent(
        str(job_id), str(agent_id), lift_operator_pause_hold=""
    )
    assert not await db.queue_job_for_resume(
        str(job_id), lift_operator_pause_hold="", expected_status="paused"
    )
    assert not await db.prepare_pinned_job_for_workspace_resume(
        str(job_id),
        "workspace_container",
        expected_status="paused",
        lift_operator_pause_hold="",
        completion_control_claim_id=str(uuid4()),
    )
    assert not await db.shed_workspace_context(str(job_id), "workspace_container")


@pytest.mark.asyncio
async def test_hold_rolls_back_queue_revocation_when_job_write_fails(db):
    claim = await stream.exact_claim(db)
    before_job = await retention.current_job(db, claim.unit_id)
    before_queue = dict(
        await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", claim.unit_id)
    )
    async with db.acquire() as conn:

        class FaultAtJobWrite:
            def __getattr__(self, name):
                return getattr(conn, name)

            async def execute(self, query, *args):
                if query.startswith("UPDATE jobs SET status='paused'"):
                    raise RuntimeError("between queue and owner writes")
                return await conn.execute(query, *args)

        with pytest.raises(RuntimeError, match="between queue"):
            await hold_container_worker_attempt(
                FaultAtJobWrite(),
                job_id=claim.unit_id,
                lease_token=claim.lease_token,
                reason="typed_report_unaccepted",
            )
    assert await retention.current_job(db, claim.unit_id) == before_job
    assert (
        dict(
            await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", claim.unit_id)
        )
        == before_queue
    )
    assert await hold(db, claim) == "held"
    await db.cancel_job(str(claim.unit_id))


async def accept(db, claim):
    return await accept_completion_command(
        db,
        job_id=str(claim.unit_id),
        lease_token=claim.lease_token,
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="worker-hold-test",
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": {"type": "workspace_unavailable", "recoverable": True},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["completion", "hold"])
async def test_completion_acceptance_and_hold_obey_queue_lock_commit_order(db, winner):
    claim = await stream.exact_claim(db)
    started = asyncio.Event()

    async def competing():
        started.set()
        return await (
            hold(db, claim) if winner == "completion" else accept(db._pool, claim)
        )

    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.fetchrow(
                "SELECT * FROM run_queue WHERE unit_id=$1 FOR UPDATE", claim.unit_id
            )
            pending = asyncio.create_task(competing())
            await started.wait()
            await asyncio.sleep(0)
            assert not pending.done()
            if winner == "completion":
                await accept(conn, claim)
            else:
                assert (
                    await hold_container_worker_attempt(
                        conn,
                        job_id=claim.unit_id,
                        lease_token=claim.lease_token,
                        reason="typed_report_unaccepted",
                    )
                    == "held"
                )
        if winner == "completion":
            assert await asyncio.wait_for(pending, 5) == "superseded"
        else:
            with pytest.raises(CompletionFenceRejected):
                await asyncio.wait_for(pending, 5)
    row = await retention.current_job(db, claim.unit_id)
    assert (HOLD_KEY in row["context"]) == (winner == "hold")
    if winner == "hold":
        await db.cancel_job(str(claim.unit_id))


@pytest.mark.asyncio
async def test_renewed_attempt_is_not_expired_and_cancel_supersedes_late_hold(db):
    claim = await stream.exact_claim(db)
    assert await hold(db, claim, grace_seconds=0) == "blocked"
    assert await db.cancel_job(str(claim.unit_id))
    assert await hold(db, claim) == "superseded"
    assert HOLD_KEY not in (await retention.current_job(db, claim.unit_id))["context"]


@pytest.mark.asyncio
async def test_issued_container_digest_drift_is_held_without_runtime_rewrite(db):
    claim = await stream.exact_claim(db)
    # Native authority correctly forbids direct mutation of Ready runtime
    # coordinates. Contract drift is representable and must not turn an
    # issued sandbox attempt into ordinary non-container requeue.
    await db.execute(
        "UPDATE jobs SET config_override=$2::jsonb WHERE id=$1",
        claim.unit_id,
        json.dumps({"workspace": {"backend": "vm"}}),
    )
    before = (await retention.current_job(db, claim.unit_id))["context"][
        "workspace_container"
    ]
    assert await hold(db, claim) == "held"
    context = (await retention.current_job(db, claim.unit_id))["context"]
    assert context[HOLD_KEY]["current_digest_matches"] is False
    assert context["workspace_container"] == before
    await db.cancel_job(str(claim.unit_id))


@pytest.mark.asyncio
async def test_old_attempt_cannot_hold_successor_and_positive_prebundle_is_retryable(
    db,
):
    job_id, _ = await retention.recovery_job(db, attempts=0, lane="stateless")
    token = await db.fetchval(
        "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
    )
    async with db.acquire() as conn:
        assert (
            await hold_container_worker_attempt(
                conn,
                job_id=job_id,
                lease_token=token,
                reason="typed_report_unaccepted",
            )
            == "not_applicable"
        )
        await worker_queue.release_worker_batch(
            conn,
            unit_id=job_id,
            lease_token=token,
            park_on_exhaustion=True,
            backoff_base_seconds=0,
        )
    successor = await worker_queue.claim_worker_batch(
        db,
        pod_name="successor",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert successor and successor.unit_id == job_id
    async with db.acquire() as conn:
        assert (
            await hold_container_worker_attempt(
                conn,
                job_id=job_id,
                lease_token=token,
                reason="typed_report_unaccepted",
            )
            == "superseded"
        )
    assert HOLD_KEY not in (await retention.current_job(db, job_id))["context"]
    await db.cancel_job(str(job_id))


@pytest.mark.asyncio
async def test_held_parent_blocks_descendant_claim_even_without_display_hold(db):
    parent = await stream.exact_claim(db)
    child = await stream.exact_claim(db)
    await db.execute(
        "UPDATE jobs SET parent_job_id=$2 WHERE id=$1", child.unit_id, parent.unit_id
    )
    assert await hold(db, parent) == "held"
    # A generic display-state change cannot erase the independent debt.
    await db.execute(
        "UPDATE jobs SET context=context-'_operator_pause_hold' WHERE id=$1",
        parent.unit_id,
    )
    await worker_queue.release_worker_batch(
        db._pool,
        unit_id=child.unit_id,
        lease_token=child.lease_token,
        park_on_exhaustion=True,
        backoff_base_seconds=0,
    )
    assert (
        await worker_queue.claim_worker_batch(
            db,
            pod_name="descendant-successor",
            affinity_grace_seconds=0,
            completion_commands_enabled=True,
        )
        is None
    )
    await db.cancel_job(str(child.unit_id))
    await worker_queue.cancel_queued_worker_batch(db._pool, job_id=child.unit_id)
    await db.cancel_job(str(parent.unit_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("vm_flag", ["false", "true"])
@pytest.mark.parametrize("kind", ["container_prebundle", "historical_vm_no_attempt"])
async def test_positive_ordinary_reaper_cases_keep_existing_retry(
    db, monkeypatch, vm_flag, kind
):
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", vm_flag)
    if kind == "container_prebundle":
        job_id, _ = await retention.recovery_job(db, attempts=0, lane="stateless")
        token = await db.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
        )
    else:
        # Historical queue/Job rows carry no claim-attempt ledger. No Ready
        # runtime or stop evidence is fabricated by this compatibility fixture.
        from tests.test_vm_workspace_recovery_real_postgres import insert_leased_job

        job_id, token = await insert_leased_job(db._pool, include_attempt=False)
        await db.execute(
            "UPDATE jobs SET config_override=$2::jsonb WHERE id=$1",
            job_id,
            json.dumps({"workspace": {"backend": "vm"}}),
        )
    if vm_flag == "false":

        def forbidden_vm_store(_conn):
            raise AssertionError("flag-off retry acquired optional VM recovery store")

        monkeypatch.setattr(
            run_queue_reaper, "VMWorkspaceRecoveryStore", forbidden_vm_store
        )
    await worker_queue.renew_worker_batch(
        db._pool, unit_id=job_id, lease_token=token, lease_ttl_seconds=0.01
    )
    await asyncio.sleep(0.03)
    async with db.acquire() as conn:
        await run_queue_reaper.reap_cycle(conn, grace_seconds=0)
    queue = await db.fetchrow(
        "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", job_id
    )
    if kind == "historical_vm_no_attempt" and vm_flag == "true":
        assert queue["state"] == "parked"
        assert (
            await db.fetchval(
                "SELECT count(*) FROM vm_workspace_recoveries WHERE owner_id=$1", job_id
            )
            == 1
        )
    else:
        assert queue["state"] == "queued" and queue["lease_token"] == token + 1
    assert HOLD_KEY not in (await retention.current_job(db, job_id))["context"]
    await db.cancel_job(str(job_id))
    await worker_queue.cancel_queued_worker_batch(db._pool, job_id=job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["ordinary", "without_vm", "create_vm", "upgrade_vm"])
async def test_resume_reports_pending_executor_evidence_before_external_work(db, entry):
    claim = await stream.exact_claim(db)
    assert await hold(db, claim) == "held"
    row = await retention.current_job(db, claim.unit_id)
    operations = JobControlOperations(
        SimpleNamespace(store=db, completion_control=SimpleNamespace(abort=AsyncMock()))
    )
    with pytest.raises(HTTPException) as raised:
        if entry == "ordinary":
            await operations._resume_job_internal(
                str(claim.unit_id), user=None, job=row
            )
        elif entry == "without_vm":
            await operations._resume_job_without_vm_internal(str(claim.unit_id))
        elif entry == "create_vm":
            await operations.create_vm(row, SimpleNamespace())
        else:
            await operations._upgrade_job_to_vm_internal(str(claim.unit_id))
    assert raised.value.status_code == 409
    assert "command outcome is unknown" in raised.value.detail
    assert "Resume is blocked" in raised.value.detail
    await db.cancel_job(str(claim.unit_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["tier_transition", "vm_preflight"])
async def test_malformed_hold_blocks_reprovision(db, entry):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_retry import VMCreationRetryConflict

    job_id = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,context,config_override) "
        "VALUES($1,'held tier','paused',$2::jsonb,$3::jsonb)",
        job_id,
        json.dumps({HOLD_KEY: None}),
        json.dumps({"workspace": {"backend": "virtual"}}),
    )
    if entry == "tier_transition":
        assert not await db.begin_job_workspace_tier_transition(
            str(job_id),
            expected_backend="virtual",
            target_backend="sandbox",
            requested_backend="sandbox",
            assignment_source="test",
            expected_status="paused",
        )
    else:
        async with db.acquire() as conn:
            async with conn.transaction():
                with pytest.raises(
                    VMCreationRetryConflict, match="worker_execution_pending"
                ):
                    await VMCreationPreflightStore(db)._lock(conn, job_id)
    await db.cancel_job(str(job_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["cancel", "hold"])
async def test_cancel_and_hold_serialize_without_reinstating_execution(db, winner):
    claim = await stream.exact_claim(db)
    started = asyncio.Event()

    async def competing():
        started.set()
        return await (
            hold(db, claim) if winner == "cancel" else db.cancel_job(str(claim.unit_id))
        )

    async with db.transaction_scope() as conn:
        await conn.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1 FOR UPDATE", claim.unit_id
        )
        pending = asyncio.create_task(competing())
        await started.wait()
        await asyncio.sleep(0)
        assert not pending.done()
        if winner == "cancel":
            assert await db.cancel_job(str(claim.unit_id))
        else:
            assert await hold(db, claim) == "held"
    result = await asyncio.wait_for(pending, 5)
    assert result == ("superseded" if winner == "cancel" else True)
    row = await retention.current_job(db, claim.unit_id)
    assert row["status"] == "cancelled"
    assert (HOLD_KEY in row["context"]) == (winner == "hold")


@pytest.mark.asyncio
async def test_bundle_authorized_after_first_reaper_classification_is_never_requeued(
    db, monkeypatch
):
    from shared import worker_execution_hold as holds
    from shared.workspace_contract import workspace_runtime_authority_digest

    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    job_id, row = await retention.recovery_job(db, attempts=0, lane="stateless")
    token = await db.fetchval(
        "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
    )
    digest = workspace_runtime_authority_digest(row, vm_mode="external")
    original = holds.hold_container_worker_attempt
    classified = False

    async def issue_between_checks(conn, **kwargs):
        nonlocal classified
        result = await original(conn, **kwargs)
        if result == "not_applicable" and not classified:
            classified = True
            assert await worker_queue.record_worker_bundle_authorized(
                conn,
                job_id=job_id,
                lease_token=token,
                authority_digest=digest,
            )
        return result

    monkeypatch.setattr(holds, "hold_container_worker_attempt", issue_between_checks)
    await worker_queue.renew_worker_batch(
        db._pool, unit_id=job_id, lease_token=token, lease_ttl_seconds=0.01
    )
    await asyncio.sleep(0.03)
    async with db.acquire() as conn:
        await run_queue_reaper.reap_cycle(conn, grace_seconds=0)
    assert classified
    queue = await db.fetchrow("SELECT state FROM run_queue WHERE unit_id=$1", job_id)
    assert queue["state"] == "parked", (
        "bundle was issued after pre-bundle classification and then requeued"
    )
    assert HOLD_KEY in (await retention.current_job(db, job_id))["context"]
    await db.cancel_job(str(job_id))
