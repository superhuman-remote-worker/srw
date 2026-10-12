"""Pending creation precedes generic VM dispatch without hiding adopted boot policy."""

import json
import pytest

from tests.test_vm_creation_readiness_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    ready_creation,
)  # noqa: F401
from tests.test_vm_creation_preflight_real_postgres import resolving


db = _db_fixture


@pytest.mark.asyncio
async def test_preflight_wait_is_consumed_without_legacy_vm_poll(db):
    from orchestrator.services.vm_creation_dispatch import handle_creation_pending

    job, _, _, _ = await resolving(db)
    assert await handle_creation_pending(await db.get_job(str(job)), db=db)


@pytest.mark.asyncio
async def test_adopted_boot_remains_subject_to_existing_phase_policy(db, monkeypatch):
    from orchestrator.services.vm_creation_dispatch import handle_creation_pending

    row = await ready_creation(db, monkeypatch)
    await db.merge_vm_context(str(row["job_id"]), {"status": "created"})
    assert not await handle_creation_pending(
        await db.get_job(str(row["job_id"])), db=db
    )
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT context->>'_vm_creation_pending' FROM jobs WHERE id=$1",
            row["job_id"],
        ) == str(row["request_id"])


@pytest.mark.asyncio
async def test_ready_releases_hold_then_next_dispatch_runs_normal_preflight(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_dispatch import handle_creation_pending

    row = await ready_creation(db, monkeypatch)
    job = await db.get_job(str(row["job_id"]))
    assert await handle_creation_pending(job, db=db)
    current = await db.get_job(str(row["job_id"]))
    assert not await handle_creation_pending(current, db=db)
    context = (
        json.loads(current["context"])
        if isinstance(current["context"], str)
        else current["context"]
    )
    assert "_vm_creation_pending" not in context
