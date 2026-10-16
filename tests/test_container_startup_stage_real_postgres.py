"""Exact container startup authority on native PostgreSQL owner receipts."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from orchestrator.database.container_startup_stage import (
    BoundPodObserved,
    ReadyObservedAt,
    ScheduledAt,
    StageBudgets,
    StartupAttention,
    Unknown,
    Unscheduled,
)
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from tests import test_workspace_pull_failure_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _bound_job(db, *, job_id=None):
    job_id = job_id or await fixtures._job(db)
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="startup-test",
        desired_manifest_digest="a" * 64,
    )
    assert reservation is not None
    gate = dict(
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="startup-test",
        claim_token=int(reservation["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate
    )
    pod_uid = str(uuid4())
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(COALESCE(context,'{}'::jsonb),"
            "'{workspace_container}',$2::jsonb) WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "provisioner": "k8s",
                    "status": "creating",
                    "_runtime_incarnation": pod_uid,
                    "_creation_reservation_id": str(reservation["id"]),
                    "_creation_claim_token": str(reservation["claim_token"]),
                }
            ),
        )
    return job_id, reservation, pod_uid, gate


async def _reservation(db, reservation):
    return await db.fetchrow(
        "SELECT * FROM managed_repository_workspace_creation_reservations WHERE id=$1",
        reservation["id"],
    )


def _observe_kwargs(job_id, reservation, pod_uid):
    return dict(
        owner_kind="job",
        owner_id=str(job_id),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]),
        pod_uid=pod_uid,
    )


@pytest.mark.asyncio
async def test_gate_off_preserves_legacy_row_and_generic_settlement(db):
    job_id, reservation, pod_uid, gate = await _bound_job(db)
    before = dict(await _reservation(db, reservation))
    assert not await db.observe_container_startup(
        **_observe_kwargs(job_id, reservation, pod_uid),
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=False,
    )
    assert dict(await _reservation(db, reservation)) == before
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )


@pytest.mark.asyncio
async def test_old_settled_creating_job_is_never_reopened_as_v1(db):
    job_id, reservation, pod_uid, gate = await _bound_job(db)
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )
    original = dict(await _reservation(db, reservation))
    assert original["phase"] == "settled"
    assert not await db.observe_container_startup(
        **_observe_kwargs(job_id, reservation, pod_uid),
        observation=BoundPodObserved(),
        adopt_if_unmarked=True,
    )
    assert dict(await _reservation(db, reservation)) == original
    assert (
        await db.complete_job_workspace_creation(
            **_job_ready_call(job_id, reservation, pod_uid)
        )
        is None
    )


@pytest.mark.asyncio
async def test_unscheduled_wait_preserves_budget_then_freezes_exact_schedule(db):
    job_id, reservation, pod_uid, gate = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert await db.observe_container_startup(
        **kwargs,
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    waiting = await _reservation(db, reservation)
    assert (waiting["startup_stage"], waiting["startup_state"]) == (
        "scheduling",
        "waiting_capacity",
    )
    assert waiting["scheduled_at"] is None
    assert waiting["ready_budget_seconds"] is None
    assert not await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )
    scheduled_at = datetime.now(timezone.utc)
    budgets = StageBudgets(ready_seconds=180, pull_seconds=300, ssh_seconds=30)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at),
        budgets=budgets,
        adopt_if_unmarked=False,
    )
    starting = await _reservation(db, reservation)
    assert (starting["startup_stage"], starting["startup_state"]) == (
        "readiness",
        "starting",
    )
    assert starting["scheduled_at"] == scheduled_at
    assert starting["ready_budget_seconds"] == 180
    assert starting["pull_budget_seconds"] == 300
    assert starting["ssh_budget_seconds"] == 30
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at),
        adopt_if_unmarked=False,
    )
    assert not await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at + timedelta(seconds=1)),
        budgets=budgets,
        adopt_if_unmarked=False,
    )
    assert (await _reservation(db, reservation))["scheduled_at"] == scheduled_at


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_whole_second_schedule_uses_receipt_second_without_changing_clock(
    db, owner_kind
):
    if owner_kind == "job":
        owner_id, reservation, pod_uid, _ = await _bound_job(db)
    else:
        owner_id, _, reservation, pod_uid = await _bound_thread(db)
    kwargs = _observe_kwargs(owner_id, reservation, pod_uid) | {
        "owner_kind": owner_kind
    }
    created_at = (datetime.now(timezone.utc) - timedelta(seconds=3)).replace(
        microsecond=618798
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=$2 WHERE id=$1",
            reservation["id"],
            created_at,
        )
    original = dict(await _reservation(db, reservation))
    assert original["created_at"] == created_at
    schedule = created_at.replace(microsecond=0)
    local_schedule = schedule.astimezone(timezone(timedelta(hours=2)))
    budgets = StageBudgets(180, None, 30)

    for refused in (
        schedule - timedelta(seconds=1),
        created_at - timedelta(microseconds=1),
        schedule.replace(tzinfo=None),
        datetime.now(timezone.utc) + timedelta(minutes=1),
    ):
        assert not await db.observe_container_startup(
            **kwargs,
            observation=ScheduledAt(refused),
            budgets=budgets,
            adopt_if_unmarked=True,
        )
        assert dict(await _reservation(db, reservation)) == original

    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(local_schedule),
        budgets=budgets,
        adopt_if_unmarked=True,
    )
    frozen = dict(await _reservation(db, reservation))
    assert frozen["scheduled_at"] == schedule
    assert frozen["ready_budget_seconds"] == 180
    assert frozen["pull_budget_seconds"] is None
    assert frozen["ssh_budget_seconds"] == 30
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(local_schedule),
        adopt_if_unmarked=False,
    )
    assert dict(await _reservation(db, reservation)) == frozen
    assert not await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(schedule + timedelta(microseconds=1)),
        adopt_if_unmarked=False,
    )
    assert dict(await _reservation(db, reservation)) == frozen


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
@pytest.mark.parametrize(
    ("ready_seconds", "ready_offset", "reason"),
    [
        (0.05, None, "readiness_deadline"),
        (5, 0.1, "ssh_deadline"),
    ],
)
async def test_whole_second_schedule_keeps_late_ready_attention_sticky(
    db, owner_kind, ready_seconds, ready_offset, reason
):
    if owner_kind == "job":
        owner_id, reservation, pod_uid, _ = await _bound_job(db)
    else:
        owner_id, _, reservation, pod_uid = await _bound_thread(db)
    kwargs = _observe_kwargs(owner_id, reservation, pod_uid) | {
        "owner_kind": owner_kind
    }
    created_at = (datetime.now(timezone.utc) - timedelta(seconds=3)).replace(
        microsecond=618798
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=$2 WHERE id=$1",
            reservation["id"],
            created_at,
        )
    schedule = created_at.replace(microsecond=0)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(schedule),
        budgets=StageBudgets(ready_seconds, None, 0.05),
        adopt_if_unmarked=True,
    )
    ready_at = (
        datetime.now(timezone.utc)
        if ready_offset is None
        else schedule + timedelta(seconds=ready_offset)
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(ready_at),
        adopt_if_unmarked=False,
    )
    attention = dict(await _reservation(db, reservation))
    assert attention["scheduled_at"] == schedule
    assert (attention["startup_state"], attention["startup_reason_code"]) == (
        "attention",
        reason,
    )
    assert not await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    assert dict(await _reservation(db, reservation)) == attention


@pytest.mark.asyncio
async def test_waiting_capacity_can_rotate_claim_without_resetting_clock(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert await db.observe_container_startup(
        **kwargs,
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=clock_timestamp()-interval '1 hour', "
            "expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
            reservation["id"],
        )
    next_claim = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="startup-next",
        desired_manifest_digest="a" * 64,
        expected_existing_reservation_id=str(reservation["id"]),
        expected_existing_claim_token=int(reservation["claim_token"]),
    )
    assert next_claim is not None
    assert next_claim["id"] == reservation["id"]
    assert next_claim["claim_token"] != reservation["claim_token"]
    assert next_claim["startup_stage"] == "scheduling"
    assert next_claim["scheduled_at"] is None


@pytest.mark.asyncio
async def test_waiting_capacity_keeps_native_cancellation_authority(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    assert await db.observe_container_startup(
        **_observe_kwargs(job_id, reservation, pod_uid),
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    cancelled = await db.request_managed_repository_workspace_creation_cancellation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="deleted",
        reclaim_shared_resources=False,
        claimant="startup-cleanup",
    )
    assert cancelled is not None
    assert cancelled["cancel_requested_at"] is not None
    assert str(cancelled["runtime_incarnation"]) == pod_uid


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
@pytest.mark.parametrize("startup_state", ["starting", "attention"])
async def test_native_cancel_cannot_be_rewritten_as_v1_ready_settlement(
    db, owner_kind, startup_state
):
    if owner_kind == "job":
        owner_id, reservation, pod_uid, _ = await _bound_job(db)
    else:
        owner_id, _, reservation, pod_uid = await _bound_thread(db)
    kwargs = dict(
        owner_kind=owner_kind,
        owner_id=str(owner_id),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]),
        pod_uid=pod_uid,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=30),
    )
    if startup_state == "attention":
        assert await db.observe_container_startup(
            **kwargs, observation=StartupAttention("invalid_image")
        )
    cancelled = await db.request_managed_repository_workspace_creation_cancellation(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        target_disposition="deleted",
        reclaim_shared_resources=False,
        claimant="startup-cleanup",
    )
    assert cancelled is not None and cancelled["cancel_requested_at"] is not None
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET phase='settled',result_kind='settled',"
                "settled_at=clock_timestamp() WHERE id=$1",
                reservation["id"],
            )
    after = await _reservation(db, reservation)
    assert after["cancel_requested_at"] is not None
    assert after["settled_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["partial", "sub_microsecond_budget"])
async def test_direct_writer_cannot_store_malformed_v1_shape(db, invalid):
    job_id, reservation, _, _ = await _bound_job(db)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            if invalid == "partial":
                await conn.execute(
                    "UPDATE managed_repository_workspace_creation_reservations "
                    "SET startup_protocol_version=1 WHERE id=$1",
                    reservation["id"],
                )
            else:
                await conn.execute(
                    "UPDATE managed_repository_workspace_creation_reservations "
                    "SET startup_protocol_version=1, startup_stage='readiness', "
                    "startup_state='starting', startup_reason_code='scheduled', "
                    "scheduled_at=clock_timestamp(), "
                    "ready_budget_seconds=0.0000001, ssh_budget_seconds=30 "
                    "WHERE id=$1",
                    reservation["id"],
                )
    assert (await _reservation(db, reservation))["startup_protocol_version"] is None


@pytest.mark.asyncio
async def test_exact_bound_pod_can_adopt_observing_without_guessing_clock(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert not await db.observe_container_startup(
        **kwargs, observation=Unknown(), adopt_if_unmarked=True
    )
    assert not await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=False
    )
    assert not await db.observe_container_startup(
        **(kwargs | {"pod_uid": str(uuid4())}),
        observation=BoundPodObserved(),
        adopt_if_unmarked=True,
    )
    assert (await _reservation(db, reservation))["startup_protocol_version"] is None
    assert await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=True
    )
    observed = await _reservation(db, reservation)
    assert (
        observed["startup_stage"],
        observed["startup_state"],
        observed["startup_reason_code"],
    ) == ("scheduling", "observing", "observation_pending")
    assert observed["scheduled_at"] is None
    scheduled_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(180, None, 30),
        adopt_if_unmarked=False,
    )
    starting = dict(await _reservation(db, reservation))
    assert await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=False
    )
    assert dict(await _reservation(db, reservation)) == starting


async def _starting_job(db, budgets=None, *, job_id=None):
    job_id, reservation, pod_uid, gate = await _bound_job(db, job_id=job_id)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    scheduled_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at),
        budgets=budgets
        or StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=30),
        adopt_if_unmarked=True,
    )
    return job_id, reservation, pod_uid, gate, kwargs, scheduled_at


def _job_ready_call(job_id, reservation, pod_uid):
    return dict(
        job_id=str(job_id),
        runtime_incarnation=pod_uid,
        backing_id=pod_uid,
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        pod_ip="10.42.0.42",
        port=30022,
        creation_reservation_id=str(reservation["id"]),
        creation_claim_token=int(reservation["claim_token"]),
        host="workspace.example.internal",
    )


@pytest.mark.asyncio
async def test_job_authenticated_ready_and_receipt_settle_in_one_commit(db):
    job_id, reservation, pod_uid, _, kwargs, scheduled_at = await _starting_job(db)
    ready_call = _job_ready_call(job_id, reservation, pod_uid)
    assert await db.complete_job_workspace_creation(**ready_call) is None
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    assert await db.complete_job_workspace_creation(**ready_call)
    job = await db.get_job(str(job_id))
    context = job["context"]
    if isinstance(context, str):
        context = json.loads(context)
    workspace = context["workspace_container"]
    assert workspace["status"] == "ready"
    assert workspace["_runtime_incarnation"] == pod_uid
    assert workspace["_creation_reservation_id"] == str(reservation["id"])
    assert workspace["_creation_claim_token"] == str(reservation["claim_token"])
    closed = await _reservation(db, reservation)
    assert (closed["phase"], closed["result_kind"]) == ("settled", "settled")
    assert closed["settled_at"] is not None
    assert closed["scheduled_at"] == scheduled_at
    assert closed["startup_first_ready_at"] is not None


@pytest.mark.asyncio
async def test_job_without_execution_timeout_can_publish_exact_ready(db):
    db.manifest_runtime_image = "test.invalid/srw:installed"
    created = await db.create_job(description="SRW Job without an execution timeout")
    job_id = created["id"]
    execution = await db.fetchrow(
        "SELECT harness_adapter,resolved #>> '{spec,timeoutSeconds}' AS timeout "
        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
        job_id,
    )
    assert execution is not None
    assert execution["harness_adapter"] == "srw/v1"
    assert execution["timeout"] is None
    job_id, reservation, pod_uid, _, kwargs, _ = await _starting_job(db, job_id=job_id)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    assert await db.complete_job_workspace_creation(
        **_job_ready_call(job_id, reservation, pod_uid)
    )
    ready = await db.get_job(str(job_id))
    context = (
        json.loads(ready["context"])
        if isinstance(ready["context"], str)
        else ready["context"]
    )
    assert context["workspace_container"]["status"] == "ready"
    settled = await _reservation(db, reservation)
    assert settled["phase"] == settled["result_kind"] == "settled"


@pytest.mark.asyncio
async def test_old_writer_cannot_split_v1_ready_from_receipt_after_first_ready(db):
    job_id, reservation, pod_uid, _, kwargs, _ = await _starting_job(db)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context, "
                "'{workspace_container,status}', '\"ready\"'::jsonb) WHERE id=$1",
                job_id,
            )
    assert (await _reservation(db, reservation))["phase"] == "runtime_bound"
    assert await db.complete_job_workspace_creation(
        **_job_ready_call(job_id, reservation, pod_uid)
    )


@pytest.mark.asyncio
async def test_old_writer_cannot_publish_ready_or_settle_v1_attention(db):
    job_id, reservation, pod_uid, gate, kwargs, _ = await _starting_job(db)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context, "
                "'{workspace_container,status}', '\"ready\"'::jsonb) WHERE id=$1",
                job_id,
            )
    assert await db.observe_container_startup(
        **kwargs,
        observation=StartupAttention("invalid_image"),
        adopt_if_unmarked=False,
    )
    assert (await _reservation(db, reservation))["startup_state"] == "attention"
    assert not await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )
    assert (
        await db.complete_job_workspace_creation(
            **_job_ready_call(job_id, reservation, pod_uid)
        )
        is None
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context, "
                "'{workspace_container,status}', '\"ready\"'::jsonb) WHERE id=$1",
                job_id,
            )


async def _bound_thread(db):
    thread_id, generation, pod_uid = uuid4(), uuid4(), str(uuid4())
    marker = {
        "generation": str(generation),
        "mode": "create",
        "attempted": False,
        "replaces_uid": None,
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads "
            "(id,status,execution_lane,runtime_generation,metadata) "
            "VALUES ($1,'created','stateless',$2,$3::jsonb)",
            thread_id,
            generation,
            json.dumps(
                {
                    "workspace_container": {
                        "status": "pending",
                        "provisioner": "k8s",
                        "_runtime_creation": marker,
                    }
                }
            ),
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        claimant="session-startup-test",
        desired_manifest_digest="b" * 64,
    )
    assert reservation is not None
    assert await db.claim_stateless_thread_workspace_creation_attempt(
        str(thread_id), generation=str(generation)
    )
    gate = dict(
        owner_kind="thread",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="session-startup-test",
        claim_token=int(reservation["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(thread_id), **gate
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(thread_id), **gate, runtime_incarnation=pod_uid
    )
    assert await db.publish_stateless_thread_workspace_runtime(
        str(thread_id),
        generation=str(generation),
        runtime_incarnation=pod_uid,
        pod_name=f"workspace-{str(thread_id)[:12]}",
        namespace="agent-workspaces",
        creation_reservation_id=str(reservation["id"]),
        creation_claim_token=int(reservation["claim_token"]),
    )
    return thread_id, generation, reservation, pod_uid


@pytest.mark.asyncio
async def test_session_v1_ready_requires_frozen_first_ready_and_settles_atomically(db):
    thread_id, generation, reservation, pod_uid = await _bound_thread(db)
    kwargs = _observe_kwargs(thread_id, reservation, pod_uid) | {"owner_kind": "thread"}
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(180, None, 30),
        adopt_if_unmarked=True,
    )
    ready_call = dict(
        thread_id=str(thread_id),
        generation=str(generation),
        runtime_incarnation=pod_uid,
        backing_id=f"k8s-pod:agent-workspaces:{pod_uid}",
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        pod_ip="10.42.0.43",
        port=30022,
        creation_reservation_id=str(reservation["id"]),
        creation_claim_token=int(reservation["claim_token"]),
    )
    assert await db.complete_stateless_thread_workspace_creation(**ready_call) is None
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    assert await db.complete_stateless_thread_workspace_creation(**ready_call)
    closed = await _reservation(db, reservation)
    assert (closed["phase"], closed["result_kind"]) == ("settled", "settled")
    thread = await db.get_thread(str(thread_id))
    state = thread["metadata"]
    if isinstance(state, str):
        state = json.loads(state)
    assert state["workspace_container"]["status"] == "ready"


@pytest.mark.asyncio
async def test_existing_generation_trigger_prevents_stale_thread_marker_before_v1(db):
    thread_id, _, reservation, _ = await _bound_thread(db)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_set(metadata, "
                "'{workspace_container,_runtime_creation,generation}', "
                "to_jsonb($2::text)) WHERE id=$1",
                thread_id,
                str(uuid4()),
            )
    assert (await _reservation(db, reservation))["startup_protocol_version"] is None


@pytest.mark.asyncio
async def test_protected_cloud_thread_hold_cannot_adopt_container_startup(db):
    thread_id, _, reservation, pod_uid = await _bound_thread(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata, "
            "'{protected_cloud}', 'true'::jsonb) WHERE id=$1",
            thread_id,
        )
    assert not await db.observe_container_startup(
        **(_observe_kwargs(thread_id, reservation, pod_uid) | {"owner_kind": "thread"}),
        observation=BoundPodObserved(),
        adopt_if_unmarked=True,
    )
    assert (await _reservation(db, reservation))["startup_protocol_version"] is None


@pytest.mark.asyncio
async def test_old_session_writer_cannot_split_ready_from_v1_settlement(db):
    thread_id, generation, reservation, pod_uid = await _bound_thread(db)
    kwargs = _observe_kwargs(thread_id, reservation, pod_uid) | {"owner_kind": "thread"}
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(180, None, 30),
        adopt_if_unmarked=True,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_set(metadata, "
                "'{workspace_container,status}', '\"ready\"'::jsonb) WHERE id=$1",
                thread_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET phase='settled', result_kind='settled', "
                "settled_at=clock_timestamp() WHERE id=$1",
                reservation["id"],
            )
    assert (await _reservation(db, reservation))["phase"] == "runtime_bound"


@pytest.mark.asyncio
async def test_first_ready_flap_cannot_extend_elapsed_ssh_window(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(0.05, None, 0.05),
        adopt_if_unmarked=True,
    )
    first_ready = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(first_ready),
        adopt_if_unmarked=False,
    )
    await asyncio.sleep(0.12)
    assert not await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    assert (
        await db.complete_job_workspace_creation(
            **_job_ready_call(job_id, reservation, pod_uid)
        )
        is None
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=StartupAttention("ssh_deadline"),
        adopt_if_unmarked=False,
    )
    attention = dict(await _reservation(db, reservation))
    assert attention["startup_first_ready_at"] == first_ready
    assert attention["startup_state"] == "attention"
    assert await db.observe_container_startup(
        **kwargs,
        observation=BoundPodObserved(),
        adopt_if_unmarked=False,
    )
    assert dict(await _reservation(db, reservation)) == attention


@pytest.mark.asyncio
async def test_delayed_observation_of_timely_ready_records_ssh_attention(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(0.05, None, 0.05),
        adopt_if_unmarked=True,
    )
    timely_ready = datetime.now(timezone.utc)
    await asyncio.sleep(0.12)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(timely_ready),
        adopt_if_unmarked=False,
    )
    receipt = await _reservation(db, reservation)
    assert receipt["startup_first_ready_at"] == timely_ready
    assert (receipt["startup_state"], receipt["startup_reason_code"]) == (
        "attention",
        "ssh_deadline",
    )


@pytest.mark.asyncio
async def test_job_ready_waiting_on_owner_lock_uses_fresh_deadline_clock(db):
    job_id, reservation, pod_uid, _, kwargs, _ = await _starting_job(
        db, StageBudgets(0.05, None, 0.05)
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ReadyObservedAt(datetime.now(timezone.utc)),
        adopt_if_unmarked=False,
    )
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT 1 FROM jobs WHERE id=$1 FOR UPDATE", job_id)
            task = asyncio.create_task(
                db.complete_job_workspace_creation(
                    **_job_ready_call(job_id, reservation, pod_uid)
                )
            )
            await asyncio.sleep(0.13)
            assert not task.done()
        assert await asyncio.wait_for(task, 5) is None
    assert (await _reservation(db, reservation))["phase"] == "runtime_bound"


@pytest.mark.asyncio
async def test_populated_pre_startup_upgrade_preserves_legacy_creation(tmp_path):
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in migrations.glob("*.sql"):
        if path.name.split("_", 1)[0] < "0309":
            (stage / path.name).write_bytes(path.read_bytes())
    with PostgresContainer("postgres:15") as container:
        dsn = container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        store = PostgresDB(dsn, min_connections=1, max_connections=4)
        try:
            await run_migrations(pool, stage)
            await store.connect()
            job_id, reservation, pod_uid, gate = await _bound_job(store)
            original = await store.fetchrow(
                "SELECT id,owner_kind,owner_id,scope,operation_kind,phase,"
                "claim_token,runtime_incarnation,pod_uid,settled_at "
                "FROM managed_repository_workspace_creation_reservations "
                "WHERE id=$1",
                reservation["id"],
            )
            checksums = await pool.fetch(
                "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
            )
            for name in (
                "0309_container_startup_stage_authority.sql",
                "0310_validate_container_startup_stage_authority.sql",
            ):
                migration = migrations / name
                (stage / migration.name).write_bytes(migration.read_bytes())
            await run_migrations(pool, stage)
            await run_migrations(pool, stage)
            assert (
                await pool.fetchval(
                    "SELECT convalidated FROM pg_constraint "
                    "WHERE conname='managed_workspace_startup_stage_shape_check'"
                )
                is True
            )
            assert (
                await pool.fetch(
                    "SELECT filename,checksum FROM schema_migrations "
                    "WHERE filename=ANY($1::text[]) ORDER BY filename",
                    [row["filename"] for row in checksums],
                )
                == checksums
            )
            after = await store.fetchrow(
                "SELECT id,owner_kind,owner_id,scope,operation_kind,phase,"
                "claim_token,runtime_incarnation,pod_uid,settled_at,"
                "startup_protocol_version,startup_stage,startup_state "
                "FROM managed_repository_workspace_creation_reservations "
                "WHERE id=$1",
                reservation["id"],
            )
            assert tuple(after.values())[: len(original)] == tuple(original.values())
            assert list(after.values())[-3:] == [None, None, None]
            assert await store.settle_managed_repository_workspace_creation_reservation(
                str(job_id), **gate, runtime_incarnation=pod_uid
            )
        finally:
            await store.close()
            await pool.close()


APP_MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
)
# The container_exited expansion and its validation, whatever numbers they got.
EXIT_CODE_MIGRATIONS = sorted(
    APP_MIGRATIONS.glob("*container_exited_startup_attention.sql")
)
V2_SHAPE = "managed_workspace_startup_stage_shape_check_v2"
STAGE_AUTHORITY = "managed_workspace_startup_stage_authority"
STARTUP_COLUMNS = (
    "id,startup_protocol_version,startup_stage,startup_state,startup_reason_code,"
    "scheduled_at,startup_first_ready_at,ready_budget_seconds,pull_budget_seconds,"
    "ssh_budget_seconds,startup_attention_at,phase,claim_token,settled_at"
)


async def _exited_by_sql(
    db, reservation, *, reason="container_exited", code=3, first_ready=False
):
    """Move a starting receipt into attention by direct SQL, as any writer could."""

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET startup_state='attention', startup_reason_code=$2, "
            "startup_attention_at=clock_timestamp(), startup_exit_code=$3, "
            "startup_first_ready_at=CASE WHEN $4 THEN scheduled_at ELSE NULL END "
            "WHERE id=$1",
            reservation["id"],
            reason,
            code,
            first_ready,
        )


def test_container_exited_migrations_are_one_expansion_and_one_validation():
    assert [path.name.split("_", 1)[1] for path in EXIT_CODE_MIGRATIONS] == [
        "container_exited_startup_attention.sql",
        "validate_container_exited_startup_attention.sql",
    ]
    first, second = (int(path.name[:4]) for path in EXIT_CODE_MIGRATIONS)
    assert second == first + 1
    expansion, validation = (path.read_text() for path in EXIT_CODE_MIGRATIONS)
    assert "NOT VALID" in expansion
    # The reliability stream's receipt function stays exactly as 0309 wrote it.
    assert "CREATE OR REPLACE" not in expansion
    assert "enforce_container_startup_stage_receipt" not in expansion
    assert f"VALIDATE CONSTRAINT {V2_SHAPE}" in validation


@pytest.mark.asyncio
async def test_v2_replaces_the_0309_shape_check_validated(db):
    rows = await db.fetch(
        "SELECT conname, convalidated FROM pg_constraint "
        "WHERE conname LIKE 'managed_workspace_startup_stage_shape_check%'"
    )
    assert {row["conname"]: row["convalidated"] for row in rows} == {V2_SHAPE: True}


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [3, 0])
async def test_v2_shape_accepts_container_exited_with_a_code(db, code):
    _job_id, reservation, *_ = await _starting_job(db)
    await _exited_by_sql(db, reservation, code=code)
    row = await _reservation(db, reservation)
    assert (row["startup_reason_code"], row["startup_exit_code"]) == (
        "container_exited",
        code,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "code", "first_ready"),
    [
        ("container_exited", None, False),
        ("container_exited", 3, True),
        ("invalid_image", 3, False),
    ],
    ids=["no-code", "after-first-ready", "code-on-another-reason"],
)
async def test_v2_shape_refuses_a_malformed_exit(db, reason, code, first_ready):
    _job_id, reservation, *_ = await _starting_job(db)
    with pytest.raises(asyncpg.CheckViolationError) as raised:
        await _exited_by_sql(
            db, reservation, reason=reason, code=code, first_ready=first_ready
        )
    assert raised.value.constraint_name == V2_SHAPE
    assert (await _reservation(db, reservation))["startup_state"] == "starting"


@pytest.mark.asyncio
async def test_v2_shape_refuses_a_code_outside_container_exited(db):
    _job_id, starting, *_ = await _starting_job(db)
    _other_id, unmarked, *_ = await _bound_job(db)
    async with db.acquire() as conn:
        for receipt in (starting, unmarked):
            with pytest.raises(asyncpg.CheckViolationError) as raised:
                await conn.execute(
                    "UPDATE managed_repository_workspace_creation_reservations "
                    "SET startup_exit_code=3 WHERE id=$1",
                    receipt["id"],
                )
            assert raised.value.constraint_name == V2_SHAPE


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [4, None], ids=["changed", "cleared"])
async def test_exit_code_is_frozen_once_written(db, changed):
    _job_id, reservation, *_ = await _starting_job(db)
    await _exited_by_sql(db, reservation, code=3)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as raised:
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET startup_exit_code=$2 WHERE id=$1",
                reservation["id"],
                changed,
            )
    assert raised.value.constraint_name == STAGE_AUTHORITY
    assert (await _reservation(db, reservation))["startup_exit_code"] == 3


@pytest.mark.asyncio
async def test_cancel_keeps_a_frozen_exit_code(db):
    job_id, reservation, *_ = await _starting_job(db)
    await _exited_by_sql(db, reservation, code=0)
    assert await db.cancel_job(str(job_id))
    row = await _reservation(db, reservation)
    assert row["cancel_requested_at"] is not None
    assert row["startup_exit_code"] == 0


@pytest.mark.asyncio
async def test_pre_exit_code_writer_updates_still_pass_v2(db):
    # The UPDATE an old orchestrator replica sends: it never names the column.
    job_id, reservation, *_ = await _starting_job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET startup_protocol_version=1, startup_stage='readiness', "
            "startup_state='attention', startup_reason_code='invalid_image', "
            "startup_attention_at=clock_timestamp() WHERE id=$1",
            reservation["id"],
        )
    assert await db.cancel_job(str(job_id))
    row = await _reservation(db, reservation)
    assert (row["startup_reason_code"], row["startup_exit_code"]) == (
        "invalid_image",
        None,
    )


@pytest.mark.asyncio
async def test_populated_upgrade_validates_v2_without_changing_receipts(tmp_path):
    first_new = EXIT_CODE_MIGRATIONS[0].name.split("_", 1)[0]
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in APP_MIGRATIONS.glob("*.sql"):
        if path.name.split("_", 1)[0] < first_new:
            (stage / path.name).write_bytes(path.read_bytes())
    with PostgresContainer("postgres:15") as container:
        dsn = container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        store = PostgresDB(dsn, min_connections=1, max_connections=4)
        try:
            await run_migrations(pool, stage)
            await store.connect()
            legacy_job, legacy, legacy_uid, legacy_gate = await _bound_job(store)
            _a_job, attention, _, _, kwargs, _ = await _starting_job(store)
            assert await store.observe_container_startup(
                **kwargs, observation=StartupAttention("invalid_image")
            )
            _s_job, starting, *_ = await _starting_job(store)
            ids = [legacy["id"], attention["id"], starting["id"]]
            before = await pool.fetch(
                f"SELECT {STARTUP_COLUMNS} FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE id=ANY($1::uuid[]) ORDER BY id",
                ids,
            )
            for path in EXIT_CODE_MIGRATIONS:
                (stage / path.name).write_bytes(path.read_bytes())
            await run_migrations(pool, stage)
            await run_migrations(pool, stage)
            after = await pool.fetch(
                f"SELECT {STARTUP_COLUMNS},startup_exit_code FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE id=ANY($1::uuid[]) ORDER BY id",
                ids,
            )
            assert [tuple(row.values())[:-1] for row in after] == [
                tuple(row.values()) for row in before
            ]
            assert [row["startup_exit_code"] for row in after] == [None, None, None]
            assert (
                await pool.fetchval(
                    "SELECT convalidated FROM pg_constraint WHERE conname=$1",
                    V2_SHAPE,
                )
                is True
            )
            assert await store.settle_managed_repository_workspace_creation_reservation(
                str(legacy_job), **legacy_gate, runtime_incarnation=legacy_uid
            )
        finally:
            await store.close()
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_code", [3, 0])
async def test_writer_records_container_exited_with_its_code(db, exit_code):
    _job_id, reservation, _pod_uid, _gate, kwargs, scheduled_at = await _starting_job(
        db
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=StartupAttention("container_exited", exit_code=exit_code),
    )
    row = await _reservation(db, reservation)
    assert (
        row["startup_stage"],
        row["startup_state"],
        row["startup_reason_code"],
        row["startup_exit_code"],
    ) == ("readiness", "attention", "container_exited", exit_code)
    assert row["startup_attention_at"] is not None
    assert row["startup_first_ready_at"] is None
    assert row["scheduled_at"] == scheduled_at


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation",
    [
        StartupAttention("container_exited"),
        StartupAttention("container_exited", exit_code=True),
        StartupAttention("container_exited", exit_code=2**31),
        StartupAttention("invalid_image", exit_code=3),
        StartupAttention("readiness_deadline", exit_code=3),
    ],
    ids=["no-code", "bool-code", "out-of-range", "code-on-invalid", "code-on-deadline"],
)
async def test_writer_refuses_a_malformed_exit_code(db, observation):
    _job_id, reservation, _pod_uid, _gate, kwargs, _ = await _starting_job(db)
    before = dict(await _reservation(db, reservation))
    assert not await db.observe_container_startup(**kwargs, observation=observation)
    assert dict(await _reservation(db, reservation)) == before


@pytest.mark.asyncio
async def test_writer_refuses_container_exited_after_first_ready(db):
    _job_id, reservation, _pod_uid, _gate, kwargs, scheduled_at = await _starting_job(
        db
    )
    assert await db.observe_container_startup(
        **kwargs, observation=ReadyObservedAt(scheduled_at)
    )
    assert (await _reservation(db, reservation))["startup_first_ready_at"] == (
        scheduled_at
    )
    assert not await db.observe_container_startup(
        **kwargs, observation=StartupAttention("container_exited", exit_code=3)
    )
    row = await _reservation(db, reservation)
    assert (row["startup_state"], row["startup_exit_code"]) == ("starting", None)


@pytest.mark.asyncio
async def test_writer_refuses_container_exited_before_a_schedule(db):
    job_id, reservation, pod_uid, _ = await _bound_job(db)
    kwargs = _observe_kwargs(job_id, reservation, pod_uid)
    assert await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=True
    )
    assert not await db.observe_container_startup(
        **kwargs, observation=StartupAttention("container_exited", exit_code=3)
    )
    row = await _reservation(db, reservation)
    assert (row["startup_stage"], row["startup_state"], row["startup_exit_code"]) == (
        "scheduling",
        "observing",
        None,
    )


@pytest.mark.asyncio
async def test_writer_refuses_container_exited_for_a_session(db):
    thread_id, _, receipt, pod_uid = await _bound_thread(db)
    kwargs = dict(
        owner_kind="thread",
        owner_id=str(thread_id),
        reservation_id=str(receipt["id"]),
        claim_token=receipt["claim_token"],
        pod_uid=pod_uid,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=Unscheduled("scheduler_unschedulable"),
        adopt_if_unmarked=True,
    )
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=30),
    )
    assert not await db.observe_container_startup(
        **kwargs, observation=StartupAttention("container_exited", exit_code=3)
    )
    row = await _reservation(db, receipt)
    assert (row["startup_state"], row["startup_exit_code"]) == ("starting", None)


@pytest.mark.asyncio
async def test_container_exited_attention_is_sticky_and_never_settles(db):
    job_id, reservation, pod_uid, gate, kwargs, scheduled_at = await _starting_job(db)
    assert await db.observe_container_startup(
        **kwargs, observation=StartupAttention("container_exited", exit_code=3)
    )
    for later in (
        StartupAttention("container_exited", exit_code=4),
        StartupAttention("invalid_image"),
        ReadyObservedAt(scheduled_at),
    ):
        assert not await db.observe_container_startup(**kwargs, observation=later)
    assert not await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=pod_uid
    )
    assert (
        await db.complete_job_workspace_creation(
            **_job_ready_call(job_id, reservation, pod_uid)
        )
        is None
    )
    row = await _reservation(db, reservation)
    assert (
        row["startup_reason_code"],
        row["startup_exit_code"],
        row["settled_at"],
    ) == (
        "container_exited",
        3,
        None,
    )
