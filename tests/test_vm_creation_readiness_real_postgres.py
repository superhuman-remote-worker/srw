"""Creation adoption is separate from guarded final SSH/init Ready release."""

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    observed_creation,  # noqa: F401
)


db = _db_fixture


async def ready_creation(db, monkeypatch):
    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    await store.settle_adopted(
        request_id=str(row["request_id"]), carrier=carrier, observations=observations
    )
    updates = dict(
        status="ready",
        ssh_ready_source="provisioner_probe",
        ssh_verified_at=datetime.now(timezone.utc).isoformat(),
        ssh_registration_id=uuid4().hex,
        active_pod_uid=str(uuid4()),
        ssh_host="10.42.1.8",
        pod_ip="10.42.1.8",
        ssh_port=22,
    )
    await db.merge_vm_context(str(row["job_id"]), updates)
    return row


@pytest.mark.asyncio
async def test_ready_release_clears_only_pending_marker_and_preserves_queue_until_dispatch(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore
    from shared.worker_queue import (
        enqueue_worker_batch,
        hold_worker_batch_for_preflight,
    )

    row = await ready_creation(db, monkeypatch)
    async with db.acquire() as conn:
        await enqueue_worker_batch(conn, job_id=row["job_id"])
        await hold_worker_batch_for_preflight(
            conn, job_id=row["job_id"], preserve_attempts=True
        )
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=3 WHERE unit_id=$1",
            row["job_id"],
        )
        before = dict(
            await conn.fetchrow(
                "SELECT * FROM run_queue WHERE unit_id=$1", row["job_id"]
            )
        )
    store = VMCreationReadinessStore(db)
    assert await store.release(request_id=str(row["request_id"]))
    async with db.acquire() as conn:
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"]
        )
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", row["job_id"])
        )
        assert "_vm_creation_pending" not in context
        assert context["completion_decision"] == {"preserve": True}
        assert context["vm"]["provision_attempts"] == 0
        assert retry["ready_at"] is not None and retry["boot_counted"] is True
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT * FROM run_queue WHERE unit_id=$1", row["job_id"]
                )
            )
            == before
        )
        # Later dispatch owns queue admission; replay must leave its runnable row.
        await enqueue_worker_batch(conn, job_id=row["job_id"])
        queued = dict(
            await conn.fetchrow(
                "SELECT * FROM run_queue WHERE unit_id=$1", row["job_id"]
            )
        )
    assert await store.release(request_id=str(row["request_id"]))
    async with db.acquire() as conn:
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT * FROM run_queue WHERE unit_id=$1", row["job_id"]
                )
            )
            == queued
        )
        assert (
            await conn.fetchval(
                "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            == retry["ready_at"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"status": "created"},
        {"ssh_ready_source": "daemon"},
        {"ssh_registration_id": None},
        {"active_pod_uid": None},
        {"ssh_verified_at": float("inf")},
        {"ssh_port": True},
        {"ssh_verified_at": "2026-09-20T00:00:00"},
        {"ssh_verified_at": "2099-01-01T00:00:00Z"},
        {"ssh_verified_at": "not-a-time"},
        {"vm_uid": str(uuid4())},
        {"rootdisk_pvc_uid": str(uuid4())},
        {"ssh_host_key_fingerprint": "SHA256:changed"},
        {"retirement_cleanup_pending": True},
        {"provisioning_attention_reason": "vm_phase_identity_conflict"},
        {"provisioning": "{}"},
    ],
)
async def test_ready_release_requires_current_exact_prober_authority(
    db, monkeypatch, change
):
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore

    row = await ready_creation(db, monkeypatch)
    # PostgreSQL JSON cannot store Infinity; use an invalid string for this case.
    if change.get("ssh_verified_at") == float("inf"):
        change = {"ssh_verified_at": "Infinity"}
    await db.merge_vm_context(str(row["job_id"]), change)
    assert not await VMCreationReadinessStore(db).release(
        request_id=str(row["request_id"])
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            is None
        )
        assert await conn.fetchval(
            "SELECT context->>'_vm_creation_pending' FROM jobs WHERE id=$1",
            row["job_id"],
        ) == str(row["request_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "cancelled", "completed", "reviewing"])
async def test_ready_release_refuses_non_dispatchable_job(db, monkeypatch, status):
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore

    row = await ready_creation(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status=$2 WHERE id=$1", row["job_id"], status
        )
    assert not await VMCreationReadinessStore(db).release(
        request_id=str(row["request_id"])
    )


@pytest.mark.asyncio
async def test_creation_ready_timestamp_is_write_once_in_database(db, monkeypatch):
    import asyncpg
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore

    row = await ready_creation(db, monkeypatch)
    assert await VMCreationReadinessStore(db).release(request_id=str(row["request_id"]))
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="Ready release"):
            await conn.execute(
                "UPDATE vm_creation_retries SET ready_at=NULL WHERE request_id=$1",
                row["request_id"],
            )
        with pytest.raises(asyncpg.CheckViolationError, match="Ready release"):
            await conn.execute(
                "UPDATE vm_creation_retries SET ready_at=ready_at+interval '1 second' WHERE request_id=$1",
                row["request_id"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_case",
    [
        "valid",
        "missing_recipe",
        "missing_receipt",
        "wrong_owner",
        "partial",
        "wrong_revision",
    ],
)
async def test_ready_release_uses_frozen_initialization_requirement(
    db, monkeypatch, receipt_case
):
    from tests import test_vm_creation_retry_real_postgres as fixture_module
    from shared.workspace_initialization import initialization_request
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore

    original = fixture_module.build_vm_creation_request
    recipe = initialization_request([{"command": ["true"]}])
    monkeypatch.setattr(
        fixture_module,
        "build_vm_creation_request",
        lambda **kwargs: original(**kwargs, initialization=recipe),
    )
    row = await ready_creation(db, monkeypatch)
    receipt = dict(
        version=1,
        ownerId=str(row["job_id"]),
        revision=recipe["revision"],
        phase="Succeeded",
        step=1,
        exitCode=0,
    )
    if receipt_case == "wrong_owner":
        receipt["ownerId"] = str(uuid4())
    elif receipt_case == "partial":
        receipt["step"] = 0
    elif receipt_case == "wrong_revision":
        receipt["revision"] = "wrong"
    await db.merge_vm_context(
        str(row["job_id"]),
        {
            "initialization": None if receipt_case == "missing_recipe" else recipe,
            "initialization_receipt": None
            if receipt_case == "missing_receipt"
            else receipt,
        },
    )
    assert await VMCreationReadinessStore(db).release(
        request_id=str(row["request_id"])
    ) is (receipt_case == "valid")


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["cancel", "registration", "attention", "deadline"])
async def test_ready_release_rechecks_current_facts_after_job_lock_wait(
    db, monkeypatch, winner
):
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore

    row = await ready_creation(db, monkeypatch)
    async with db.acquire() as blocking:
        async with blocking.transaction():
            await blocking.execute(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", row["job_id"]
            )
            pending = asyncio.create_task(
                VMCreationReadinessStore(db).release(request_id=str(row["request_id"]))
            )
            await asyncio.sleep(0.15)
            assert not pending.done()
            if winner == "cancel":
                await blocking.execute(
                    "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
                )
            elif winner == "deadline":
                # The immutable execution snapshot must also remain the same.
                await blocking.execute(
                    "UPDATE srw_execution_specs SET created_at=clock_timestamp()-interval '2 hours' WHERE work_id=$1",
                    row["job_id"],
                )
            else:
                key = (
                    "ssh_registration_id"
                    if winner == "registration"
                    else "provisioning_attention_reason"
                )
                value = (
                    None if winner == "registration" else "vm_phase_identity_conflict"
                )
                await blocking.execute(
                    "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm' || $2::jsonb) WHERE id=$1",
                    row["job_id"],
                    json.dumps({key: value}),
                )
        assert not await asyncio.wait_for(pending, timeout=5)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            is None
        )
