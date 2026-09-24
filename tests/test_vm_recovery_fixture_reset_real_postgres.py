"""A recovery gate reset must not forge a stateless dispatch transition."""

import json
from uuid import UUID
from unittest.mock import AsyncMock

import pytest

from orchestrator.operator_cli.vm_workspace_recovery_acceptance import (
    AcceptanceFailure,
    LiveScenario,
)
from shared.worker_queue import _CAS_JOB_SQL
from tests.test_vm_recovery_gate_stop_store_real_postgres import seeded
from tests.test_vm_workspace_recovery_real_postgres import (  # noqa: F401
    app_pg as _app_pg,
    pg_dsn,
    _schema_applied,
)

app_pg = _app_pg


@pytest.mark.asyncio
async def test_resolve_then_reset_uses_fresh_workspace_claim_authority(app_pg):
    doc = await seeded(app_pg)
    job_id, operation_id = UUID(doc["job_id"]), UUID(doc["operation_id"])
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    async with app_pg.acquire() as conn:
        leased_until = await conn.fetchval(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,leased_by,leased_until) "
            "VALUES($1,'worker_batch','leased',27,$2,clock_timestamp()+interval '5 minutes') "
            "RETURNING leased_until",
            job_id,
            f"vm-recovery-gate:{doc['run_id']}",
        )
        assert (
            await conn.fetchval(
                _CAS_JOB_SQL,
                job_id,
                "processing",
                f"vm-recovery-gate:{doc['run_id']}",
                27,
                leased_until,
            )
            == job_id
        )
        await conn.execute(
            "UPDATE jobs SET status='paused',freeze_data='{}'::jsonb WHERE id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE run_queue SET state='parked',lease_token=28,leased_by=NULL,"
            "leased_until=NULL,park_reason='vm_workspace_recovery' WHERE unit_id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='paused_attention' WHERE id=$1",
            operation_id,
        )
    await scenario._resolve_fixture_recovery(operation_id, job_id)
    assert (
        await app_pg.fetchval("SELECT status FROM jobs WHERE id=$1", job_id) == "paused"
    )
    assert (
        await app_pg.fetchval("SELECT freeze_data FROM jobs WHERE id=$1", job_id)
        is None
    )
    token = await scenario._reset_lease(job_id)
    assert token == 29
    async with app_pg.acquire() as conn:
        job = await conn.fetchrow("SELECT status,context FROM jobs WHERE id=$1", job_id)
        queue = await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        marker = json.loads(job["context"])["_workspace_dispatch_authority"]
        assert job["status"] == "processing"
        assert marker["queue_lease_token"] == queue["lease_token"] == token
        assert (
            marker["worker_pod"]
            == queue["leased_by"]
            == f"vm-recovery-gate:{doc['run_id']}"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2",
                job_id,
                token,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL",
                job_id,
            )
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal", ["foreign_run", "open_recovery", "operator_pause", "terminal"]
)
async def test_fixture_claim_refusal_rolls_back_queue_reset(app_pg, refusal):
    doc = await seeded(app_pg)
    job_id = UUID(doc["job_id"])
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) VALUES($1,'worker_batch','parked',28)",
            job_id,
        )
        if refusal != "open_recovery":
            await conn.execute(
                "UPDATE vm_workspace_recovery_jobs SET resolved_at=clock_timestamp(),participation='cancelled' WHERE job_id=$1",
                job_id,
            )
        if refusal == "foreign_run":
            scenario.run_id = "another-run"
        elif refusal == "operator_pause":
            await conn.execute(
                "UPDATE jobs SET status='paused',context=context||'{\"_operator_pause_hold\":{}}'::jsonb WHERE id=$1",
                job_id,
            )
        elif refusal == "terminal":
            await conn.execute("UPDATE jobs SET status='cancelled' WHERE id=$1", job_id)
        before = dict(
            await conn.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        )
    with pytest.raises(AcceptanceFailure, match="current claim authority"):
        await scenario._reset_lease(job_id)
    assert (
        dict(await app_pg.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id))
        == before
    )
    assert (
        await app_pg.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1", job_id
        )
        == 0
    )


@pytest.mark.asyncio
async def test_cleanup_preserves_completed_recovery_evidence(app_pg):
    doc = await seeded(app_pg)
    operation_id = UUID(doc["operation_id"])
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='recovered',resolved_at=clock_timestamp() WHERE id=$1",
            operation_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='released',resolved_at=clock_timestamp() WHERE recovery_id=$1",
            operation_id,
        )
        before = dict(
            await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1", operation_id
            )
        )
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = app_pg, doc["run_id"]
    scenario._cleanup_stop_retention = AsyncMock()
    scenario._purge_fixture = AsyncMock()
    await scenario.cleanup()
    scenario._purge_fixture.assert_awaited_once_with(UUID(doc["job_id"]))
    assert (
        dict(
            await app_pg.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1", operation_id
            )
        )
        == before
    )
