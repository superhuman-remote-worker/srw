"""Owner Delete must settle pinned Job checkpoints before removing the Job."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.job_mutation_controls import JobControlOperations
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle
from tests.test_vm_workspace_recovery_real_postgres import (
    checkpoint_row_counts,
    prepare_checkpoint_rows,
)

pg_dsn = lifecycle.pg_dsn
_schema_applied = lifecycle._schema_applied
db = lifecycle.db


def _controls(store):
    @asynccontextmanager
    async def vectors():
        yield SimpleNamespace(execute=AsyncMock())

    return JobControlOperations(
        SimpleNamespace(
            store=store,
            logger=logging.getLogger(__name__),
            archive_and_cleanup_workspace=AsyncMock(),
            snapshot_service=SimpleNamespace(is_available=False),
            gitea_client=SimpleNamespace(is_initialized=False),
            vector_db=SimpleNamespace(acquire=vectors),
            resolve_job_notifications=AsyncMock(),
        )
    )


async def _completed_pinned_job(store):
    job_id = uuid4()
    await store.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,context) "
        "VALUES($1,'pinned checkpoint Delete','completed','pinned','{}'::jsonb)",
        job_id,
    )
    await prepare_checkpoint_rows(store._pool, job_id, checkpoint_count=3)
    assert await checkpoint_row_counts(store._pool, job_id) == (3, 3, 3)
    return job_id


@pytest.mark.asyncio
async def test_pinned_owner_delete_with_sqlite_backend_and_absent_tables(
    db, monkeypatch
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "sqlite")
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT to_regclass('public.checkpoints')") is None
        assert (
            await conn.fetchval("SELECT to_regclass('public.checkpoint_blobs')") is None
        )
        assert (
            await conn.fetchval("SELECT to_regclass('public.checkpoint_writes')")
            is None
        )
    job_id = uuid4()
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,context) "
        "VALUES($1,'no checkpoint backend','completed','pinned','{}'::jsonb)",
        job_id,
    )

    result = await _controls(db).delete(
        str(job_id),
        caller={"id": str(uuid4()), "is_admin": True},
        job=await db.get_job(str(job_id)),
    )

    assert result["status"] == "deleted"
    assert await db.get_job(str(job_id)) is None


@pytest.mark.asyncio
async def test_pinned_owner_delete_drains_checkpoint_rows_before_job_delete(
    db, monkeypatch
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id = await _completed_pinned_job(db)

    result = await _controls(db).delete(
        str(job_id),
        caller={"id": str(uuid4()), "is_admin": True},
        job=await db.get_job(str(job_id)),
    )

    assert result["status"] == "deleted"
    assert await db.get_job(str(job_id)) is None
    assert await checkpoint_row_counts(db._pool, job_id) == (0, 0, 0)


@pytest.mark.asyncio
async def test_pinned_owner_delete_keeps_job_when_checkpoint_prune_is_held(
    db, monkeypatch
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id = await _completed_pinned_job(db)
    monkeypatch.setattr(
        db, "delete_checkpoint_thread", AsyncMock(side_effect=RuntimeError("held"))
    )

    with pytest.raises(HTTPException) as refused:
        await _controls(db).delete(
            str(job_id),
            caller={"id": str(uuid4()), "is_admin": True},
            job=await db.get_job(str(job_id)),
        )

    assert refused.value.status_code == 503
    assert await db.get_job(str(job_id)) is not None
    assert await checkpoint_row_counts(db._pool, job_id) == (3, 3, 3)


@pytest.mark.asyncio
async def test_delete_rechecks_rows_after_an_earlier_completed_prune(db, monkeypatch):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id = await _completed_pinned_job(db)
    assert await db.delete_checkpoint_thread(str(job_id), strict=True) == 9
    await prepare_checkpoint_rows(db._pool, job_id, checkpoint_count=1)

    result = await _controls(db).delete(
        str(job_id),
        caller={"id": str(uuid4()), "is_admin": True},
        job=await db.get_job(str(job_id)),
    )

    assert result["status"] == "deleted"
    assert await db.get_job(str(job_id)) is None
    assert await checkpoint_row_counts(db._pool, job_id) == (0, 0, 0)


@pytest.mark.asyncio
async def test_owner_delete_transaction_refuses_rows_written_after_strict_prune(
    db, monkeypatch
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id = await _completed_pinned_job(db)
    original_prune = db.delete_checkpoint_thread

    async def prune_then_late_write(thread_id, *, strict=False):
        result = await original_prune(thread_id, strict=strict)
        await prepare_checkpoint_rows(db._pool, job_id, checkpoint_count=1)
        return result

    monkeypatch.setattr(db, "delete_checkpoint_thread", prune_then_late_write)

    with pytest.raises(HTTPException) as refused:
        await _controls(db).delete(
            str(job_id),
            caller={"id": str(uuid4()), "is_admin": True},
            job=await db.get_job(str(job_id)),
        )

    assert refused.value.status_code == 503
    assert await db.get_job(str(job_id)) is not None
    assert await checkpoint_row_counts(db._pool, job_id) == (1, 1, 1)
