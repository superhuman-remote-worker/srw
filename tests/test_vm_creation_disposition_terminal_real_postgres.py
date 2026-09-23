"""All fixed completion evidence and lifecycle releases commit together."""

import json
from uuid import UUID

import asyncpg
import pytest

from tests.test_vm_creation_attachment_disposition_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    attached as _attached_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    frozen_attachment,
    complete_scans,
)
from shared.vm_creation_disposition import disposition_identity
from vm_controller.creation_disposition import CreationDisposer

db, setup, attached = _db_fixture, _setup_fixture, _attached_fixture


async def completed_attachment(db, attached):
    ctrl, _, _, _ = attached
    service, row, carrier, disposition = await frozen_attachment(db, attached)
    complete_scans(ctrl)
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    return service, row, carrier, disposition


@pytest.mark.asyncio
async def test_empty_disposition_settles_parent_instance_and_retry_atomically(db, attached):
    service, row, carrier, disposition = await completed_attachment(db, attached)
    result = await service.settle(request_id=str(row["request_id"]), carrier=carrier)
    assert result == {"settled": True, "disposition": "creation_disposed"}
    assert await service.settle(request_id=str(row["request_id"]), carrier=carrier) == result
    async with db.acquire() as conn:
        retry = await conn.fetchrow("SELECT * FROM vm_creation_retries WHERE request_id=$1", row["request_id"])
        assert retry["state"] == "settled" and retry["reason"] == "creation_disposed"
        assert retry["observed_vm_uid"] is None and not retry["boot_counted"]
        parent = await conn.fetchrow("SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", retry["creation_admission_id"])
        assert parent["completed_at"] and parent["outcome"] == "creation_disposed"
        instance = await conn.fetchrow("SELECT * FROM srw_workspace_instances WHERE id=$1", UUID(disposition["workspace_instance_id"]))
        assert instance["status"] == "Released" and instance["execution_id"] is None
        assert instance["pvc_uid"] is None
        assert json.loads(instance["backend_state"])["retained_creation_disposition"]["request_id"] == str(row["request_id"])
        queue = await conn.fetchrow("SELECT state,attempts_since_completion FROM run_queue WHERE unit_id=$1", row["job_id"])
        assert dict(queue) == {"state": "done", "attempts_since_completion": 3}
        assert await conn.fetchval("SELECT status FROM jobs WHERE id=$1", row["job_id"]) == "cancelled"


@pytest.mark.asyncio
async def test_controller_completes_frozen_disposition_through_authority(db, attached):
    service, row, carrier, disposition = await completed_attachment(db, attached)
    ctrl, _, _, _ = attached
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposed"
    assert (await service.retries.inspect(request_id=str(row["request_id"])))['state'] == 'settled'


@pytest.mark.asyncio
async def test_terminal_parent_cannot_be_reopened_after_release(db, attached):
    service, row, carrier, _ = await completed_attachment(db, attached)
    await service.settle(request_id=str(row["request_id"]), carrier=carrier)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET completed_at=NULL,outcome=NULL "
                "WHERE id=(SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1)",
                row["request_id"],
            )


@pytest.mark.asyncio
async def test_terminal_parent_identity_cannot_change_after_release(db, attached):
    service, row, carrier, _ = await completed_attachment(db, attached)
    await service.settle(request_id=str(row["request_id"]), carrier=carrier)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET source='foreign_create' "
                "WHERE id=(SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1)",
                row["request_id"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "immediate"])
async def test_native_terminal_retry_without_corresponding_parent_and_instance_is_refused(db, attached, boundary):
    _, row, _, _ = await completed_attachment(db, attached)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await conn.execute("UPDATE vm_creation_retries SET state='settled',reason='creation_disposed',resolved_at=clock_timestamp(),claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1", row["request_id"])
                if boundary == "immediate":
                    await conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
