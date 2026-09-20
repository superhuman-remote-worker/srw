"""Human handoff owns the idle clock, atomically with its exact route CAS."""

import asyncio
import json
from uuid import UUID, uuid4

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


async def escalate(db, seed, route, **overrides):
    args = dict(
        to_state="escalated_to_user",
        expected_states=["pending_officer", "pending_both"],
        actor_kind="officer",
        actor_id=seed["officer_thread_id"],
        officer_thread_id=seed["officer_thread_id"],
        officer_incarnation=0,
    )
    args.update(overrides)
    return await db.transition_message_route(route["route_id"], **args)


@pytest.fixture(autouse=True)
def tracking(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")


@pytest.mark.asyncio
@pytest.mark.parametrize("initial", ["pending_officer", "pending_both"])
async def test_human_handoff_enters_once_and_delivery_does_not_reset(db, initial):
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state=initial)
    before = await episode(db, seed["job_id"])
    assert before[0] == (1 if initial == "pending_both" else 0)
    assert await escalate(db, seed, route)
    after = await episode(db, seed["job_id"])
    assert after[0] == 1 and after[1]["wait_key"] == route["route_id"]
    if initial == "pending_both":
        assert after == before
    assert await db.mark_route_user_delivery(route["route_id"])
    assert not await escalate(db, seed, route)
    assert await episode(db, seed["job_id"]) == after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source", ["wrong_thread", "wrong_incarnation", "recommissioned"]
)
async def test_stale_officer_cannot_publish_handoff(db, source):
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    overrides = {}
    if source == "wrong_thread":
        overrides["officer_thread_id"] = str(uuid4())
    elif source == "wrong_incarnation":
        overrides["officer_incarnation"] = 1
    else:
        await db.execute(
            "UPDATE project_officers SET incarnations='[{}]'::jsonb WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
    before = await db.get_message_route(route["route_id"])
    assert await escalate(db, seed, route, **overrides) is None
    assert await db.get_message_route(route["route_id"]) == before
    assert await episode(db, seed["job_id"]) == (0, None)


@pytest.mark.asyncio
async def test_sla_two_replicas_publish_one_episode(db):
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    results = await asyncio.gather(
        db.claim_officer_sla_escalations(), db.claim_officer_sla_escalations()
    )
    assert sum(len(rows) for rows in results) == 1
    revision, stored = await episode(db, seed["job_id"])
    assert revision == 1 and stored["wait_key"] == route["route_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["question", "nonblocking", "terminal"])
async def test_historical_route_settles_without_entering_current_idle(db, changed):
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    if changed == "question":
        await db.execute(
            "UPDATE jobs SET freeze_data=jsonb_set(freeze_data,'{route_id}',$2::jsonb) WHERE id=$1",
            UUID(seed["job_id"]),
            json.dumps(str(uuid4())),
        )
    elif changed == "nonblocking":
        await db.execute(
            "UPDATE job_message_routes SET blocking=false WHERE route_id=$1",
            UUID(route["route_id"]),
        )
    else:
        await db.execute(
            "UPDATE jobs SET status='completed' WHERE id=$1", UUID(seed["job_id"])
        )
    assert await escalate(db, seed, route)
    assert await episode(db, seed["job_id"]) == (0, None)


@pytest.mark.asyncio
async def test_episode_failure_rolls_back_route_cas(db, monkeypatch):
    from orchestrator.services import workspace_idle_events

    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    original = workspace_idle_events.apply_idle_transition_on_conn

    async def fail_after_write(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("handoff rollback")

    monkeypatch.setattr(
        workspace_idle_events, "apply_idle_transition_on_conn", fail_after_write
    )
    with pytest.raises(RuntimeError, match="handoff rollback"):
        await escalate(db, seed, route)
    assert (await db.get_message_route(route["route_id"]))["state"] == "pending_officer"
    assert await episode(db, seed["job_id"]) == (0, None)


@pytest.mark.asyncio
async def test_escalation_rechecks_question_after_waiting_for_job_lock(db):
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", UUID(seed["job_id"])
            )
            task = asyncio.create_task(escalate(db, seed, route))
            try:
                async with asyncio.timeout(5):
                    while not await blocker.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE "
                        "query='SELECT id,project_id FROM jobs WHERE id=$1 FOR UPDATE' "
                        "AND wait_event_type='Lock' AND datname=current_database())"
                    ):
                        # PostgreSQL caches activity snapshots inside this
                        # deliberately open blocker transaction.
                        await blocker.execute("SELECT pg_stat_clear_snapshot()")
                        await asyncio.sleep(0.02)
                await blocker.execute(
                    "UPDATE jobs SET freeze_data=jsonb_set(freeze_data,'{route_id}',$2::jsonb) WHERE id=$1",
                    UUID(seed["job_id"]),
                    json.dumps(str(uuid4())),
                )
            except BaseException:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
    assert await asyncio.wait_for(task, timeout=5)
    assert await episode(db, seed["job_id"]) == (0, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_system_handoff_and_disabled_compatibility(db, monkeypatch, enabled):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", str(enabled).lower())
    seed, _ = await seeded(db)
    _, route = await publish(db, seed, state="pending_officer")
    assert await escalate(
        db,
        seed,
        route,
        actor_kind="system",
        officer_thread_id=None,
        officer_incarnation=None,
    )
    assert (await episode(db, seed["job_id"]))[0] == int(enabled)
