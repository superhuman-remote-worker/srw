"""Explicit IDE tab close keeps exact authority after a VM Job terminates."""
# ruff: noqa: F401, F811 -- imported pytest fixtures and their parameter names

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException

from orchestrator.routers.ide import IdeDependencies, router, stop_ide_session
from orchestrator.services.vm_idle_access import VMIdleAccessStore
from tests.test_vm_idle_lifecycle_real_postgres import (
    _schema_applied,
    db,
    pg_dsn,
    postgres_db_fixture,
    profiled_idle_image_policy,
    seed_wait,
)


def dependencies(store, user, job):
    return IdeDependencies(
        store=store,
        ide_sessions=SimpleNamespace(
            stop_session=AsyncMock(return_value={"status": "stopped"})
        ),
        ide_proxy=object(),
        require_job_access=AsyncMock(return_value=(user, job)),
    )


async def public_close(deps, owner, lease_id):
    app = FastAPI()
    app.state.ide_dependencies_factory = lambda: deps
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.delete(
            f"/api/jobs/{owner}/ide", params={"lease_id": lease_id}
        )


@pytest_asyncio.fixture
async def ready_owner(db):
    await db.execute("TRUNCATE vm_idle_access_leases, vm_idle_operations CASCADE")
    owner, _, _ = await seed_wait(db)
    return owner


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("cancelled", "completed", "failed"))
@pytest.mark.parametrize("expired", (False, True))
async def test_terminal_vm_job_closes_only_explicit_tab_without_renewing(
    db, ready_owner, status, expired
):
    owner = ready_owner
    user = {"id": uuid4()}
    access = VMIdleAccessStore(db)
    args = dict(
        owner_kind="job", owner_id=str(owner), kind="ide", user_id=str(user["id"])
    )
    target = await access.request(**args)
    adjacent = await access.request(**args)
    assert target and adjacent and target["id"] != adjacent["id"]
    if expired:
        await db.execute(
            "UPDATE vm_idle_access_leases SET "
            "acquired_at=clock_timestamp()-interval '61 minutes',"
            "expires_at=clock_timestamp()-interval '2 minutes',"
            "max_expires_at=clock_timestamp()-interval '1 minute' WHERE id=$1",
            target["id"],
        )
    await db.execute("UPDATE jobs SET status=$2 WHERE id=$1", owner, status)
    job_before = await db.get_job(str(owner))
    target_before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_idle_access_leases WHERE id=$1", target["id"]
        )
    )
    adjacent_before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_idle_access_leases WHERE id=$1", adjacent["id"]
        )
    )
    boundary = await db.fetchval("SELECT clock_timestamp()")
    deps = dependencies(db, user, job_before)

    response = await public_close(deps, str(owner), str(target["id"]))
    assert response.status_code == 200
    assert response.json() == {"status": "stopped"}

    target_after = dict(
        await db.fetchrow(
            "SELECT * FROM vm_idle_access_leases WHERE id=$1", target["id"]
        )
    )
    assert target_before.pop("closed_at") is None
    closed_at = target_after.pop("closed_at")
    assert closed_at is not None and closed_at >= boundary
    assert target_after == target_before
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_idle_access_leases WHERE id=$1", adjacent["id"]
            )
        )
        == adjacent_before
    )
    assert await db.get_job(str(owner)) == job_before
    deps.ide_sessions.stop_session.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("cancelled", "completed", "failed"))
@pytest.mark.parametrize(
    "case",
    ("foreign_user", "foreign_job", "operation", "wrong_kind", "closed", "unknown"),
)
async def test_terminal_vm_job_refuses_non_tab_authority_without_mutation(
    db, ready_owner, status, case
):
    owner = ready_owner
    user = {"id": uuid4()}
    access = VMIdleAccessStore(db)
    args = dict(
        owner_kind="job", owner_id=str(owner), kind="ide", user_id=str(user["id"])
    )
    tab = await access.request(**args)
    assert tab
    if case == "foreign_user":
        chosen = await access.request(**{**args, "user_id": str(uuid4())})
    elif case == "foreign_job":
        other_owner, _, _ = await seed_wait(db)
        chosen = await access.request(**{**args, "owner_id": str(other_owner)})
    elif case == "operation":
        chosen = await access.begin_ide_operation(
            str(tab["id"]),
            owner_kind="job",
            owner_id=str(owner),
            user_id=str(user["id"]),
        )
    elif case == "wrong_kind":
        chosen = await access.request(**{**args, "kind": "ssh"})
    elif case == "closed":
        assert await access.close_for_user(str(tab["id"]), **args)
        chosen = tab
    else:
        chosen = {"id": uuid4()}
    assert chosen
    await db.execute("UPDATE jobs SET status=$2 WHERE id=$1", owner, status)
    job_before = await db.get_job(str(owner))
    leases_before = await db.fetch("SELECT * FROM vm_idle_access_leases ORDER BY id")
    deps = dependencies(db, user, job_before)

    response = await public_close(deps, str(owner), str(chosen["id"]))
    assert response.status_code == 409
    assert response.json() == {"detail": "VM IDE access changed"}
    assert (
        await db.fetch("SELECT * FROM vm_idle_access_leases ORDER BY id")
        == leases_before
    )
    assert await db.get_job(str(owner)) == job_before
    deps.ide_sessions.stop_session.assert_not_awaited()


def no_database_store():
    return SimpleNamespace(
        acquire=MagicMock(side_effect=AssertionError("unexpected database access"))
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("cancelled", "completed", "failed"))
@pytest.mark.parametrize("lease_id", ("", "not-a-uuid"))
async def test_terminal_explicit_invalid_lease_refuses_before_database(
    status, lease_id
):
    owner = str(uuid4())
    store = no_database_store()
    deps = dependencies(
        store, {"id": uuid4()}, {"status": status, "context": {"vm": {}}}
    )

    with pytest.raises(HTTPException) as refused:
        await stop_ide_session(
            SimpleNamespace(), owner, lease_id=lease_id, dependencies=deps
        )

    assert refused.value.status_code == 409
    store.acquire.assert_not_called()
    deps.ide_sessions.stop_session.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("cancelled", "completed", "failed"))
async def test_terminal_without_lease_preserves_snapshot_stop(status):
    owner = str(uuid4())
    store = no_database_store()
    deps = dependencies(
        store,
        {"id": uuid4()},
        {"status": status, "context": {"vm": {"status": "deleted"}}},
    )
    result = {"status": "expired", "snapshot_preserved": True}
    deps.ide_sessions.stop_session.return_value = result

    assert await stop_ide_session(SimpleNamespace(), owner, dependencies=deps) == result

    store.acquire.assert_not_called()
    deps.ide_sessions.stop_session.assert_awaited_once_with(owner)


@pytest.mark.asyncio
async def test_live_vm_without_lease_still_refuses_before_database():
    owner = str(uuid4())
    store = no_database_store()
    deps = dependencies(
        store,
        {"id": uuid4()},
        {"status": "processing", "context": {"vm": {"status": "ready"}}},
    )

    with pytest.raises(HTTPException) as refused:
        await stop_ide_session(SimpleNamespace(), owner, dependencies=deps)

    assert refused.value.status_code == 409
    store.acquire.assert_not_called()
    deps.ide_sessions.stop_session.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", (401, 403, 404))
async def test_explicit_lease_close_requires_job_access_before_database(code):
    owner = str(uuid4())
    store = no_database_store()
    deps = dependencies(store, {"id": uuid4()}, {"status": "cancelled"})
    deps.require_job_access.side_effect = HTTPException(code, "access denied")
    request = SimpleNamespace()

    with pytest.raises(HTTPException) as refused:
        await stop_ide_session(request, owner, lease_id=str(uuid4()), dependencies=deps)

    assert refused.value.status_code == code
    deps.require_job_access.assert_awaited_once_with(request, store, owner)
    store.acquire.assert_not_called()
    deps.ide_sessions.stop_session.assert_not_awaited()
