"""Durable terminal cleanup nomination never reinterprets historical retention."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tests.test_vm_idle_lifecycle_real_postgres import (
    db as _db_fixture,
    pg_dsn,  # noqa: F401 - imported pytest fixture dependency
    postgres_db_fixture,  # noqa: F401 - imported pytest fixture dependency
    _schema_applied,  # noqa: F401 - imported pytest fixture dependency
    seed_wait,
)
from tests.test_vm_idle_terminal_review_real_postgres import controls_for

db = _db_fixture


@pytest.mark.asyncio
async def test_pinned_cancel_marks_intent_without_purging_historical_terminal_vm(db):
    historical, cancelled, generation, pvc_uid = (uuid4() for _ in range(4))
    vm = {"status": "ready", "provision_generation": str(generation),
          "rootdisk_pvc_uid": str(pvc_uid)}
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,context) "
        "VALUES($1,'historical retained','completed','pinned',$3::jsonb),"
        "($2,'new cancel','paused','pinned',$3::jsonb)",
        historical, cancelled, json.dumps({"vm": vm}),
    )

    # A terminal status alone is not a new instruction to purge a disk.
    assert await db.list_terminal_vm_cleanup_jobs(limit=4) == []
    assert await db.linearize_pinned_cancel(
        str(cancelled), expected_status="paused", completion_commands_enabled=True,
    )
    nominated = await db.list_terminal_vm_cleanup_jobs(limit=4)
    assert [row["id"] for row in nominated] == [str(cancelled)]
    assert json.loads(nominated[0]["context"])["_job_terminal_vm_cleanup"] == {
        "version": 1, "source": "cancel", "provision_generation": str(generation),
    }
    assert not await db.complete_terminal_vm_cleanup_marker(
        str(cancelled), expected_generation=str(generation),
    )

    # An admitted parent is still selected for supported cleanup; this test
    # deliberately does not write any physical-absence/process-zero proof.
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
        "VALUES($1,'job',$2,$3,'job_terminal_vm_release',$4,'exact-intent')",
        uuid4(), cancelled, pvc_uid, uuid4(),
    )
    assert [row["id"] for row in await db.list_terminal_vm_cleanup_jobs(limit=4)] == [
        str(cancelled),
    ]
    assert not await db.complete_terminal_vm_cleanup_marker(
        str(cancelled), expected_generation=str(generation),
    )


@pytest.mark.asyncio
async def test_ordinary_vm_approval_atomically_selects_terminal_cleanup(
    db, monkeypatch, tmp_path,
):
    owner, _, identity = await seed_wait(db)
    await db.execute(
        "UPDATE jobs SET status='pending_review',workspace_idle_episode=NULL,"
        "workspace_idle_revision=workspace_idle_revision+1,"
        "freeze_data=$2::jsonb WHERE id=$1",
        owner, json.dumps({"freeze_type": "job_complete", "summary": "approved"}),
    )
    controls = await controls_for(db, monkeypatch, tmp_path)
    controls.dependencies.forge.is_initialized = False
    monkeypatch.setattr(
        "orchestrator.services.completion.apply_terminal_job_side_effects",
        AsyncMock(return_value={}),
    )

    result = await controls.approve_job(
        str(owner), user={"id": "reviewer"},
        job=await db.get_job(str(owner)), request=None,
    )

    assert result["status"] == "approved" and result["cleanup_pending"] is True
    row = await db.fetchrow("SELECT status,context FROM jobs WHERE id=$1", owner)
    assert row["status"] == "completed"
    assert json.loads(row["context"])["_job_terminal_vm_cleanup"] == {
        "version": 1, "source": "approve",
        "provision_generation": str(identity["generation"]),
    }
    assert [item["id"] for item in await db.list_terminal_vm_cleanup_jobs(limit=4)] == [
        str(owner),
    ]
