"""Actual cancellation completion evidence is durable and natively monotonic."""

from copy import deepcopy
import json
from uuid import UUID

import asyncpg
import pytest

from tests.test_vm_creation_source_disposition_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    frozen_golden,
    source_runtime,
)
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_disposition_sources import DispositionSources

db, setup = _db_fixture, _setup_fixture


async def completed_source(db, monkeypatch, setup):
    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    evidence = await DispositionSources(
        CreationActuator(ctrl), row, carrier, disposition
    ).run()
    return service, row, carrier, disposition, evidence


@pytest.mark.asyncio
async def test_actual_source_completion_is_durable_separate_from_plan_and_parent(
    db, monkeypatch, setup
):
    service, row, carrier, disposition, evidence = await completed_source(
        db, monkeypatch, setup
    )
    result = await service.record(
        request_id=row["request_id"], carrier=carrier, stage="source", evidence=evidence
    )
    assert result == {"recorded": True, "stage": "source", "evidence": evidence}
    inspected = await service.retries.inspect(request_id=row["request_id"])
    assert inspected.get("cancellation_completion") == {"source": evidence}
    assert (
        await service.record(
            request_id=row["request_id"],
            carrier=carrier,
            stage="source",
            evidence=evidence,
        )
        == result
    )
    async with db.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT cancellation_progress,cancellation_completion,state FROM vm_creation_retries WHERE request_id=$1",
            UUID(row["request_id"]),
        )
        assert json.loads(record["cancellation_progress"])["source"] == evidence["plan"]
        assert json.loads(record["cancellation_completion"]) == {"source": evidence}
        assert record["state"] == "cancel_requested"
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(disposition["admission_id"]),
        )


@pytest.mark.asyncio
async def test_source_completion_native_shape_and_immutability_refuse_key_only_proof(
    db, monkeypatch, setup
):
    service, row, carrier, _, evidence = await completed_source(db, monkeypatch, setup)
    await service.record(
        request_id=row["request_id"], carrier=carrier, stage="source", evidence=evidence
    )
    altered = deepcopy(evidence)
    altered["source_observation"]["dv"]["resource_version"] = "999"
    for value in ({}, {"source": {"done": True}}, {"source": altered}):
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE vm_creation_retries SET cancellation_completion=$2::jsonb WHERE request_id=$1",
                    UUID(row["request_id"]),
                    json.dumps(value),
                )


@pytest.mark.asyncio
async def test_parent_controller_records_actual_source_receipt_and_replays_after_gc(
    db, monkeypatch, setup
):
    from vm_controller.creation_disposition import CreationDisposer
    from shared.vm_creation_disposition import disposition_identity

    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    api.objects["Lease", carrier["metadata"]["name"]] = carrier
    # The frozen helper originally returns a pre-cancellation inspect snapshot;
    # the actual controller must load the durable cancelled row itself.
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    current = await service.retries.inspect(request_id=row["request_id"])
    assert (
        current.get("cancellation_completion", {}).get("source", {}).get("outcome")
        == "pin_disposed"
    )
    accepted = deepcopy(current["cancellation_completion"])
    del api.objects["DataVolume", source["name"]]
    del api.objects["PersistentVolumeClaim", source["name"]]
    replacements = len(api.replacements)
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    current = await service.retries.inspect(request_id=row["request_id"])
    assert current["cancellation_completion"] == accepted
    for _ in range(4):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed"
    current = await service.retries.inspect(request_id=row["request_id"])
    assert current["state"] == "settled"
    assert len(api.replacements) == replacements


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "immediate"])
async def test_native_parent_completion_cannot_bypass_partial_disposition(
    db, monkeypatch, setup, boundary
):
    service, row, carrier, disposition, evidence = await completed_source(
        db, monkeypatch, setup
    )
    await service.record(
        request_id=row["request_id"], carrier=carrier, stage="source", evidence=evidence
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='creation_disposed' WHERE id=$1",
                    UUID(disposition["admission_id"]),
                )
                if boundary == "immediate":
                    await conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(disposition["admission_id"]),
        )
