"""Officer lifecycle drains publish human waits in the lifecycle transaction."""

from uuid import UUID

import pytest

from tests.test_workspace_idle_job_events_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    seeded,
    publish,
    episode,
)

db = _db_fixture


async def drain(db, seed, operation):
    if operation == "hold":
        return await db.set_project_officer_hold(
            seed["project_id"],
            expected_thread_id=seed["officer_thread_id"],
            hold={"kind": "maintenance"},
            route_reason="officer_held",
        )
    return await db.decommission_project_officer(
        seed["project_id"], seed["officer_thread_id"], reason="idle proof", force=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["hold", "decommission"])
async def test_officer_drain_records_human_wait_for_job_outside_officer_lineage(
    db, monkeypatch, operation
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    # Pinned idle entry needs an accepted exact delivery receipt.
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    _, route = await publish(db, seed, state="pending_officer")
    # The route Job is deliberately not created_by_thread_id. The drain must
    # lock all affected route Jobs, not just the officer's admitted lineage.
    assert (
        await db.fetchval(
            "SELECT created_by_thread_id FROM jobs WHERE id=$1", UUID(seed["job_id"])
        )
        is None
    )
    result = await drain(db, seed, operation)
    assert [r["route_id"] for r in result["routes"]] == [route["route_id"]]
    revision, stored = await episode(db, seed["job_id"])
    assert revision == 1 and stored["wait_key"] == route["route_id"]
    assert await db.mark_route_user_delivery(route["route_id"])
    assert await episode(db, seed["job_id"]) == (revision, stored)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["hold", "decommission"])
async def test_episode_failure_rolls_back_officer_lifecycle_and_route(
    db, monkeypatch, operation
):
    from orchestrator.services import workspace_idle_events

    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    # Pinned idle entry needs an accepted exact delivery receipt.
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    _, route = await publish(db, seed, state="pending_officer")
    before_post = await db.fetchrow(
        "SELECT * FROM project_officers WHERE project_id=$1", UUID(seed["project_id"])
    )
    before_thread = await db.fetchrow(
        "SELECT * FROM threads WHERE id=$1", UUID(seed["officer_thread_id"])
    )
    original = workspace_idle_events.apply_idle_transition_on_conn

    async def fail_after_write(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("lifecycle rollback")

    monkeypatch.setattr(
        workspace_idle_events, "apply_idle_transition_on_conn", fail_after_write
    )
    with pytest.raises(RuntimeError, match="lifecycle rollback"):
        await drain(db, seed, operation)
    assert (
        await db.fetchrow(
            "SELECT * FROM project_officers WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
        == before_post
    )
    assert (
        await db.fetchrow(
            "SELECT * FROM threads WHERE id=$1", UUID(seed["officer_thread_id"])
        )
        == before_thread
    )
    assert (await db.get_message_route(route["route_id"]))["state"] == "pending_officer"
    assert await episode(db, seed["job_id"]) == (0, None)
