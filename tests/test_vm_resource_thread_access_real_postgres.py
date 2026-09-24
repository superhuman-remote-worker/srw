"""Active IDE transport and charged thread retirement share one owner fence."""

import pytest

from orchestrator.services.vm_idle_access import VMIdleAccessStore
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from tests.test_vm_resource_thread_source_real_postgres import (
    db as _db,
    thread_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    _ready_charged_thread,
)

db = _db


@pytest.mark.asyncio
async def test_charged_thread_end_waits_for_actual_ide_operation_after_tab_close(
    db, monkeypatch
):
    prepared = await _ready_charged_thread(db, monkeypatch)
    thread_id = prepared["thread_id"]
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        str(thread_id),
        str(prepared["generation"]),
        prepared["registration"],
        prepared["vm_uid"],
        prepared["updates"],
    )
    owner_id = str(
        await db.fetchval("SELECT user_id FROM threads WHERE id=$1", thread_id)
    )
    access = VMIdleAccessStore(db)
    tab = await access.acquire(
        owner_kind="thread",
        owner_id=str(thread_id),
        kind="ide",
        claimant=f"{owner_id}:tab",
    )
    assert tab
    writer = await access.begin_ide_operation(
        str(tab["id"]), owner_kind="thread", owner_id=str(thread_id), user_id=owner_id
    )
    assert writer and writer["id"] != tab["id"]
    assert await access.close_for_user(
        str(tab["id"]),
        owner_kind="thread",
        owner_id=str(thread_id),
        kind="ide",
        user_id=owner_id,
    )
    assert not await access.close_for_user(
        str(writer["id"]),
        owner_kind="thread",
        owner_id=str(thread_id),
        kind="ide",
        user_id=owner_id,
    )
    retirement = dict(
        permanent=True,
        expected_runtime_generation=str(prepared["runtime"]),
        expected_agent_id=None,
        expected_attach_token=None,
    )
    assert await db.begin_pinned_thread_retirement(str(thread_id), **retirement) == {
        "state": "conflict",
        "reason": "active_workspace_access",
    }
    assert (
        await db.fetchval(
            "SELECT runtime_retirement_token FROM threads WHERE id=$1", thread_id
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            prepared["request_id"],
        )
        == "active"
    )
    assert await access.close(
        str(writer["id"]),
        owner_kind="thread",
        owner_id=str(thread_id),
        kind="ide",
        claimant=writer["claimed_by"],
    )
    assert (await db.begin_pinned_thread_retirement(str(thread_id), **retirement))[
        "state"
    ] == "pending"
    # End admission fences new I/O; it cannot release the still-present guest's charge.
    assert (
        await access.acquire(
            owner_kind="thread",
            owner_id=str(thread_id),
            kind="ide",
            claimant=f"{owner_id}:new-tab",
        )
        is None
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
            prepared["request_id"],
        )
        == "active"
    )
