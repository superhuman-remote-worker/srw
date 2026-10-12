"""Only actual input execution admission closes a thread human-wait episode."""

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from shared.persistent_input_delivery import (
    claim_pending_input_deliveries,
    lock_runtime_authority,
    transition_input_delivery,
    transition_stateless_input_delivery,
)
from shared.run_queue import UNIT_KIND_SESSION_TURN, claim_unit
from shared.workspace_idle_policy import RuntimeIdentity
from shared.workspace_idle_store import apply_idle_transition_on_conn
from tests.test_stateless_input_delivery_real_postgres import (
    _claim_delivery,
    _persist_event,
    _schema_applied,  # noqa: F401
    _seed_pinned_thread,
    _seed_thread,
    db as _db_fixture,
    pg_dsn,  # noqa: F401
)

db = _db_fixture


async def seed(db, lane):
    delivery_id = uuid4()
    if lane == "stateless":
        _, thread_id = await _seed_thread(db)
        await _persist_event(db, thread_id, delivery_id)
        async with db.acquire() as conn:
            lease = await claim_unit(
                conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="idle-executor"
            )
        kwargs = dict(
            thread_id=thread_id,
            delivery_id=delivery_id,
            lease_token=lease.lease_token,
            executor_id="idle-executor",
            pod_uid="idle-executor-pod",
        )
        claimed = await _claim_delivery(db, **kwargs)
        kwargs["claim_generation"] = claimed["claim_generation"]
    else:
        _, thread_id, agent_id = await _seed_pinned_thread(db)
        await _persist_event(db, thread_id, delivery_id)
        async with db.acquire() as conn, conn.transaction():
            thread = await conn.fetchrow("SELECT * FROM threads WHERE id=$1", thread_id)
            identity = dict(
                agent_id=agent_id,
                pod_uid="pod-pinned",
                runtime_generation=uuid4(),
                session_runtime_generation=thread["runtime_generation"],
                runtime_attach_token=thread["runtime_attach_token"],
            )
            rows = await claim_pending_input_deliveries(
                conn, thread_id=thread_id, **identity
            )
            assert len(rows) == 1
        kwargs = dict(
            delivery_id=delivery_id,
            claim_generation=rows[0]["claim_generation"],
            **identity,
        )
    return thread_id, kwargs


async def enter(db, thread_id):
    async with db.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", thread_id
        )
        assert row["workspace_idle_episode"] is None
        return await apply_idle_transition_on_conn(
            conn,
            runtime=RuntimeIdentity(
                "thread", str(thread_id), "vm", str(uuid4()), str(uuid4())
            ),
            event="enter",
            expected_revision=row["workspace_idle_revision"],
            expected_episode_id=None,
            wait_kind="natural_pause",
            wait_key=str(uuid4()),
        )


async def state(db, thread_id):
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT workspace_idle_revision,workspace_idle_episode FROM threads WHERE id=$1",
            thread_id,
        )
        return tuple(row)


async def transition_on_conn(conn, lane, thread_id, kwargs, **changes):
    arguments = {**kwargs, "transition": "admitted", "turn_number": 1, **changes}
    if lane == "stateless":
        return await transition_stateless_input_delivery(conn, **arguments)
    await lock_runtime_authority(
        conn,
        thread_id=thread_id,
        **{
            key: kwargs[key]
            for key in (
                "agent_id",
                "pod_uid",
                "session_runtime_generation",
                "runtime_attach_token",
            )
        },
    )
    return await transition_input_delivery(conn, **arguments)


async def transition(db, lane, thread_id, kwargs, **changes):
    async with db.acquire() as conn, conn.transaction():
        return await transition_on_conn(conn, lane, thread_id, kwargs, **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_actual_admission_closes_once_with_tracking_disabled(
    db, monkeypatch, lane
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    thread_id, kwargs = await seed(db, lane)
    await enter(db, thread_id)
    before = await state(db, thread_id)
    assert not await transition(db, lane, thread_id, kwargs, claim_generation=99)
    assert await state(db, thread_id) == before
    assert await transition(db, lane, thread_id, kwargs)
    assert await state(db, thread_id) == (2, None)
    # A delayed replay or settlement cannot clear a subsequently published wait.
    await enter(db, thread_id)
    newer = await state(db, thread_id)
    assert not await transition(db, lane, thread_id, kwargs)
    assert await transition(db, lane, thread_id, kwargs, transition="settled")
    assert await state(db, thread_id) == newer


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_admission_without_episode_does_not_manufacture_revision(db, lane):
    thread_id, kwargs = await seed(db, lane)
    assert await transition(db, lane, thread_id, kwargs)
    assert await state(db, thread_id) == (0, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_episode_exit_failure_rolls_back_delivery_admission(db, lane):
    thread_id, kwargs = await seed(db, lane)
    await enter(db, thread_id)
    before = await state(db, thread_id)
    async with db.acquire() as conn:
        await conn.execute("""
            CREATE FUNCTION reject_idle_input_exit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF OLD.workspace_idle_episode IS NOT NULL AND NEW.workspace_idle_episode IS NULL
              THEN RAISE EXCEPTION 'idle exit fault'; END IF; RETURN NEW; END $$;
            CREATE TRIGGER reject_idle_input_exit BEFORE UPDATE ON threads
            FOR EACH ROW EXECUTE FUNCTION reject_idle_input_exit();
        """)
    try:
        with pytest.raises(asyncpg.RaiseError, match="idle exit fault"):
            await transition(db, lane, thread_id, kwargs)
    finally:
        async with db.acquire() as conn:
            await conn.execute("DROP TRIGGER reject_idle_input_exit ON threads")
            await conn.execute("DROP FUNCTION reject_idle_input_exit()")
    assert await state(db, thread_id) == before
    assert await transition(db, lane, thread_id, kwargs)
    assert await state(db, thread_id) == (2, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_competing_admissions_clear_exactly_once_without_lock_upgrade(db, lane):
    thread_id, kwargs = await seed(db, lane)
    await enter(db, thread_id)
    results = await asyncio.wait_for(
        asyncio.gather(
            transition(db, lane, thread_id, kwargs),
            transition(db, lane, thread_id, kwargs),
        ),
        timeout=10,
    )
    assert sorted(results) == [False, True]
    assert await state(db, thread_id) == (2, None)
