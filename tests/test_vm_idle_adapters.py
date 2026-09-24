"""Public controls preserve execution/access separation at idle wake."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from orchestrator.routers.ide import IdeDependencies, start_ide_session
from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    profiled_idle_image_policy,  # noqa: F401
    seed_wait,
)

db = _db


@pytest.mark.asyncio
async def test_authorized_ide_start_requests_access_only_idle_wake(db, monkeypatch):
    for key, value in {
        "WORKSPACE_IDLE_RELEASE_ENABLED": "true",
        "VM_CREATION_RETRY_ENABLED": "true",
        "VM_REMOTE_OPERATION_PROTOCOL_ENABLED": "true",
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
    }.items():
        monkeypatch.setenv(key, value)
    owner, episode, identity = await seed_wait(db)
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(owner), episode_id=episode.episode_id,
        revision=episode.revision, identity=identity,
    )
    assert operation
    # Disabling new idle releases must not strand an existing access-only wake.
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    user_id, job_id = str(uuid4()), str(owner)
    job = dict(await db.fetchrow("SELECT * FROM jobs WHERE id=$1", owner))
    ide_sessions = SimpleNamespace(start_session=AsyncMock())
    transport = SimpleNamespace(start_and_probe=AsyncMock())
    dependencies = IdeDependencies(
        store=db,
        ide_sessions=ide_sessions,
        ide_proxy=MagicMock(),
        require_job_access=AsyncMock(return_value=({"id": user_id}, job)),
        vm_ide_transport=transport,
    )
    request = MagicMock()
    result = await start_ide_session(request, job_id, dependencies=dependencies)

    assert result["status"] == "restoring"
    assert result["code_server_url"] is None
    dependencies.require_job_access.assert_awaited_once_with(request, db, job_id)
    lease = await db.fetchrow(
        "SELECT * FROM vm_idle_access_leases WHERE id=$1",
        UUID(result["access_lease_id"]),
    )
    assert lease["owner_kind"] == "job" and lease["owner_id"] == owner
    assert lease["kind"] == "ide" and lease["wake_id"] is not None
    assert lease["wake_id"] == await db.fetchval(
        "SELECT wake_id FROM vm_idle_operations WHERE id=$1", operation["id"],
    )
    assert lease["claimed_by"].startswith(user_id + ":")
    assert lease["closed_at"] is None
    assert await db.fetchval(
        "SELECT wake_execution_requested FROM vm_idle_operations WHERE id=$1",
        operation["id"],
    ) is False
    assert await db.fetchval(
        "SELECT state FROM run_queue WHERE unit_id=$1", owner,
    ) == "done"
    assert await db.fetchval(
        "SELECT status FROM jobs WHERE id=$1", owner,
    ) == "waiting_for_reply"
    transport.start_and_probe.assert_not_awaited()
    ide_sessions.start_session.assert_not_awaited()
