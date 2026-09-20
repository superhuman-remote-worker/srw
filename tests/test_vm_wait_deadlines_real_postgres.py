"""Immutable deadlines cancel infrastructure waits through existing controls."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.execution_deadline import ExecutionDeadline, expired_srw_jobs
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_vm_creation_preflight_real_postgres import initial_job

db = _db_fixture


async def candidate(
    db,
    *,
    lane="stateless",
    timeout=-1,
    status="paused",
    vm_status="waiting_capacity",
    context=None,
):
    job = await initial_job(
        db, timeout=timeout, lane=lane, context=context or {"vm": {"status": vm_status}}
    )
    await db.update_job_status(str(job), status=status)
    candidates = await expired_srw_jobs(db)
    return job, next((value for value in candidates if value["id"] == job), None)


async def cancel(db, job, expected, lane):
    if lane == "stateless":
        return (
            await db.cancel_stateless_job(
                str(job), expected_execution_deadline=expected
            )
        )[0]
    return await db.linearize_pinned_cancel(
        str(job), expected_status="paused", expected_execution_deadline=expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_null_resource_paused_capacity_snapshot_cancels_without_new_deadline(
    db, lane
):
    job, row = await candidate(db, lane=lane)
    expected = ExecutionDeadline.from_row(row)
    assert await cancel(db, job, expected, lane)
    assert (await db.get_job(str(job)))["status"] == "cancelled"
    async with db.acquire() as conn:
        original = await conn.fetchrow(
            "SELECT created_at,resolved FROM srw_execution_specs WHERE work_id=$1", job
        )
        assert original["created_at"] + timedelta(seconds=-1) == expected.deadline


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["protocol", "legacy", "phase_attention"])
async def test_reconciler_sends_paused_vm_wait_to_guarded_cancel(db, kind):
    context = {
        "protocol": {"vm": {"status": "failed"}, "_vm_creation_pending": str(uuid4())},
        "legacy": {"vm": {"status": "waiting_capacity"}},
        "phase_attention": {
            "vm": {
                "status": "ssh_pending",
                "provisioning_attention_reason": "vm_rootdisk_stalled",
            }
        },
    }[kind]
    job, row = await candidate(db, context=context)
    calls = []

    async def guarded_cancel(current, *, expected_execution_deadline):
        calls.append((current["id"], expected_execution_deadline))
        await cancel(
            db, UUID(str(current["id"])), expected_execution_deadline, "stateless"
        )

    service = ManifestExecutionService(
        db, runtime=None, namespace="unused", cancel_srw=guarded_cancel
    )
    await service.reconcile()
    assert calls == [(job, ExecutionDeadline.from_row(row))]
    assert (await db.get_job(str(job)))["status"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"timeout": None},
        {"timeout": 3600},
        {"status": "completed"},
        {"context": {"vm": {"status": "ready"}}, "status": "paused"},
        {"context": {"note": "human pause"}, "status": "paused"},
    ],
)
async def test_no_new_deadline_or_human_pause_policy(db, changes):
    _, row = await candidate(db, **changes)
    assert row is None


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
@pytest.mark.parametrize(
    "change", ["execution_id", "revision", "generation", "deadline"]
)
async def test_changed_execution_cannot_be_cancelled_by_stale_candidate(
    db, lane, change
):
    job, row = await candidate(db, lane=lane)
    expected = ExecutionDeadline.from_row(row)
    bad = {
        "execution_id": uuid4(),
        "revision": "changed",
        "generation": expected.generation + 1,
        "deadline": expected.deadline - timedelta(seconds=1),
    }[change]
    assert not await cancel(db, job, replace(expected, **{change: bad}), lane)
    assert (await db.get_job(str(job)))["status"] == "paused"


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_terminal_winner_during_job_lock_wait_prevents_deadline_cancel(db, lane):
    job, row = await candidate(db, lane=lane)
    expected = ExecutionDeadline.from_row(row)
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job)
            task = asyncio.create_task(cancel(db, job, expected, lane))
            await asyncio.sleep(0.1)
            assert not task.done()
    assert not await task
    assert (await db.get_job(str(job)))["status"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_guard_loss_has_no_external_cancel_or_cleanup_effects(db, lane):
    from tests.test_b09_job_control_operations import _controls

    job, row = await candidate(db, lane=lane)
    operations = _controls(db)
    effects = operations.dependencies
    operations.cascade_cancel_to_children = AsyncMock(return_value=True)
    operations.wait_for_stateless_cancel_settle = AsyncMock(return_value=True)
    result = await operations.cancel(
        str(job),
        job=await db.get_job(str(job)),
        expected_execution_deadline=replace(
            ExecutionDeadline.from_row(row), revision="changed"
        ),
    )
    assert result == {"status": "unchanged"}
    effects.manifest_cancel.assert_not_awaited()
    effects.archive_and_cleanup_workspace.assert_not_awaited()
    operations.cascade_cancel_to_children.assert_not_awaited()
    assert (await db.get_job(str(job)))["status"] == "paused"


@pytest.mark.asyncio
async def test_stateless_guard_loss_rolls_back_queue_closure_and_attempts(db):
    from shared.worker_queue import enqueue_worker_batch

    job, row = await candidate(db)
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=job)
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1", job
        )
        before = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job)
        )
    assert not await cancel(
        db, job, replace(ExecutionDeadline.from_row(row), revision="stale"), "stateless"
    )
    async with db.acquire() as conn:
        after = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job)
        )
    assert before == after


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_nonexpired_execution_is_not_cancelled_by_an_early_guard(db, lane):
    job, _ = await candidate(db, lane=lane, timeout=3600)
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id AS execution_id,revision,generation,created_at+interval '3600 seconds' AS deadline FROM srw_execution_specs WHERE work_id=$1",
            job,
        )
    assert not await cancel(db, job, ExecutionDeadline.from_row(row), lane)
    assert (await db.get_job(str(job)))["status"] == "paused"


@pytest.mark.asyncio
async def test_blocked_deadline_cleanup_does_not_starve_next_candidate(db):
    from fastapi import HTTPException

    first, _ = await candidate(db)
    second, _ = await candidate(db)
    cancelled = []

    async def guarded(current, **guard):
        if current["id"] == first:
            raise HTTPException(503, "cleanup pending")
        cancelled.append(current["id"])
        await cancel(db, second, guard["expected_execution_deadline"], "stateless")

    service = ManifestExecutionService(
        db, runtime=None, namespace="unused", cancel_srw=guarded
    )
    await service.reconcile()
    assert cancelled == [second]
    assert (await db.get_job(str(first)))["status"] == "paused"
    assert (await db.get_job(str(second)))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_deadline_preserves_issued_creation_authority_for_exact_late_settlement(
    db, monkeypatch
):
    from tests.test_vm_creation_effects_real_postgres import observed_creation

    retry, row, carrier, observations = await observed_creation(
        db, monkeypatch, timeout=1
    )
    await asyncio.sleep(1.1)
    candidates = await expired_srw_jobs(db)
    expected = ExecutionDeadline.from_row(
        next(item for item in candidates if item["id"] == row["job_id"])
    )
    before = await db.fetchrow(
        "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    )
    assert await cancel(db, row["job_id"], expected, "pinned")
    async with db.acquire() as conn:
        pending = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
        )
        assert pending["state"] == "cancel_requested"
        assert pending["creation_admission_id"] == before["creation_admission_id"]
        permit = await conn.fetchrow(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            pending["creation_admission_id"],
        )
        assert permit["completed_at"] is None
    await retry.settle_adopted(
        request_id=str(row["request_id"]), carrier=carrier, observations=observations
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            == "settled"
        )
        assert (
            await conn.fetchval(
                "SELECT outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
                pending["creation_admission_id"],
            )
            == "adopted"
        )
    assert (await db.get_job(str(row["job_id"])))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_blocked_prefix_advances_across_recreated_reconcilers(db):
    for _ in range(50):
        job = await initial_job(
            db,
            timeout=-1,
            context={
                "vm": {"status": "waiting_capacity"},
                "_completion_control_claim": {},
            },
        )
        await db.update_job_status(str(job), status="paused")
    last = await initial_job(
        db, timeout=-1, context={"vm": {"status": "waiting_capacity"}}
    )
    await db.update_job_status(str(last), status="paused")

    async def guarded(job, **guard):
        await db.cancel_stateless_job(
            str(job["id"]), completion_commands_enabled=True, **guard
        )

    for _ in range(2):
        await ManifestExecutionService(
            db, runtime=None, namespace="unused", cancel_srw=guarded
        ).reconcile()
    assert (await db.get_job(str(last)))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_concurrent_scans_split_timestamp_ties_and_wrap_after_removed_boundary(
    db,
):
    jobs = []
    for _ in range(52):
        job = await initial_job(
            db, timeout=-1, context={"vm": {"status": "waiting_capacity"}}
        )
        await db.update_job_status(str(job), status="paused")
        jobs.append(job)
    async with db.acquire() as conn:
        await conn.execute("UPDATE srw_execution_specs SET created_at='2020-01-01Z'")
        await conn.execute("DELETE FROM srw_execution_deadline_scan")
    first, second = await asyncio.gather(expired_srw_jobs(db), expired_srw_jobs(db))
    assert sorted((len(first), len(second))) == [2, 50]
    assert {row["id"] for row in first}.isdisjoint(row["id"] for row in second)
    assert {row["id"] for row in first + second} == set(jobs)

    # Removing the execution from the joined candidate set cannot pin the scan.
    boundary = max(jobs)
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM srw_execution_specs WHERE work_id=$1", boundary)
    wrapped = await expired_srw_jobs(db)
    assert [row["id"] for row in wrapped] == sorted(set(jobs) - {boundary})[:50]
    final = await expired_srw_jobs(db)
    assert [row["id"] for row in final] == sorted(set(jobs) - {boundary})[50:]

    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed'")
    assert await expired_srw_jobs(db) == []
    position = await db.fetchrow(
        "SELECT created_at,job_id FROM srw_execution_deadline_scan"
    )
    assert dict(position) == {"created_at": None, "job_id": None}
