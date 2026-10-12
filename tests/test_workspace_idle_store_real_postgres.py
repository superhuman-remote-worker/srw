"""Episode CAS commits with existing owner workflow transactions, without I/O."""

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest

from shared.workspace_idle_policy import RuntimeIdentity, IdlePolicyError
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)

db = _db_fixture


async def owner(db, kind="job"):
    identity = uuid4()
    if kind == "job":
        await db.execute(
            "INSERT INTO jobs(id,description,status) VALUES($1,'idle fixture','waiting_for_reply')",
            identity,
        )
    else:
        await db.execute(
            "INSERT INTO threads(id,status) VALUES($1,'awaiting_user')", identity
        )
    return RuntimeIdentity(kind, str(identity), "vm", str(uuid4()), str(uuid4()))


async def apply(
    db, runtime, event="enter", *, revision=0, episode_id=None, wait_key=None
):
    from shared.workspace_idle_store import apply_idle_transition_on_conn

    async with db.acquire() as conn, conn.transaction():
        return await apply_idle_transition_on_conn(
            conn,
            runtime=runtime,
            event=event,
            expected_revision=revision,
            expected_episode_id=episode_id,
            wait_kind="human_message",
            wait_key=wait_key,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["job", "thread"])
async def test_episode_is_durable_idempotent_and_does_not_use_old_updated_time(
    db, kind
):
    runtime = await owner(db, kind)
    key = str(uuid4())
    before = await db.fetchval("SELECT clock_timestamp()")
    first = await apply(db, runtime, wait_key=key)
    second = await apply(
        db,
        runtime,
        revision=first.revision,
        episode_id=first.episode.episode_id,
        wait_key=key,
    )
    assert first == second and first.revision == 1
    assert first.episode.entered_at >= before
    table = "jobs" if kind == "job" else "threads"
    row = await db.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode FROM "
        + table
        + " WHERE id=$1",
        UUID(runtime.owner_id),
    )
    assert row["workspace_idle_revision"] == 1
    assert str(first.episode.episode_id) in row["workspace_idle_episode"]


@pytest.mark.asyncio
async def test_simultaneous_first_publish_has_one_episode_and_losing_cas_cannot_reset(
    db,
):
    runtime = await owner(db)
    key = str(uuid4())
    results = await asyncio.gather(
        apply(db, runtime, wait_key=key),
        apply(db, runtime, wait_key=key),
        return_exceptions=True,
    )
    winners = [result for result in results if not isinstance(result, Exception)]
    refusals = [result for result in results if isinstance(result, IdlePolicyError)]
    assert len(winners) == len(refusals) == 1 and str(refusals[0]) == "episode_changed"
    winner = winners[0]
    assert (
        await apply(
            db,
            runtime,
            revision=winner.revision,
            episode_id=winner.episode.episode_id,
            wait_key=key,
        )
        == winner
    )


@pytest.mark.asyncio
async def test_episode_and_source_workflow_roll_back_together(db):
    from shared.workspace_idle_store import apply_idle_transition_on_conn

    runtime = await owner(db)
    async with db.acquire() as conn:
        with pytest.raises(RuntimeError, match="source CAS lost"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE jobs SET status='paused' WHERE id=$1",
                    UUID(runtime.owner_id),
                )
                await apply_idle_transition_on_conn(
                    conn,
                    runtime=runtime,
                    event="enter",
                    expected_revision=0,
                    expected_episode_id=None,
                    wait_kind="human_message",
                    wait_key=str(uuid4()),
                )
                raise RuntimeError("source CAS lost")
    row = await db.fetchrow(
        "SELECT status,workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
        UUID(runtime.owner_id),
    )
    assert (
        row["status"] == "waiting_for_reply"
        and row["workspace_idle_revision"] == 0
        and row["workspace_idle_episode"] is None
    )


@pytest.mark.asyncio
async def test_clock_is_sampled_after_owner_row_wait(db):
    runtime = await owner(db)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", UUID(runtime.owner_id)
        )
        task = asyncio.create_task(apply(db, runtime, wait_key=str(uuid4())))
        for _ in range(100):
            if await db.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT%workspace_idle_revision%FOR UPDATE%')"
            ):
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("episode publisher did not wait for owner")
        boundary = await blocker.fetchval("SELECT clock_timestamp()")
    result = await asyncio.wait_for(task, 3)
    assert result.episode.entered_at >= boundary


@pytest.mark.asyncio
async def test_extend_reply_and_new_runtime_are_fenced_by_one_owner_revision(db):
    runtime = await owner(db)
    first = await apply(db, runtime, wait_key=str(uuid4()))
    fence = dict(revision=first.revision, episode_id=first.episode.episode_id)
    extended = await apply(db, runtime, "extend", **fence)
    assert extended.episode.entered_at == first.episode.entered_at
    with pytest.raises(IdlePolicyError, match="episode_changed"):
        await apply(db, runtime, "exit", **fence)
    successor = replace(
        runtime, runtime_uid=str(uuid4()), runtime_generation=str(uuid4())
    )
    rebound = await apply(
        db,
        successor,
        "rebind",
        revision=extended.revision,
        episode_id=extended.episode.episode_id,
    )
    assert (
        rebound.episode.entered_at == first.episode.entered_at
        and rebound.episode.extend_count == 1
    )
    assert rebound.revision == 3
    with pytest.raises(IdlePolicyError, match="runtime_changed"):
        await apply(
            db, runtime, "exit", revision=3, episode_id=rebound.episode.episode_id
        )


@pytest.mark.asyncio
async def test_presence_does_not_erase_semantic_episode_or_extension_count(db):
    runtime = await owner(db, "thread")
    first = await apply(db, runtime, wait_key=str(uuid4()))
    extended = await apply(
        db, runtime, "extend", revision=1, episode_id=first.episode.episode_id
    )
    # Legacy UX status/presence remains separate; no C adapter is enabled yet.
    await db.execute(
        "UPDATE threads SET status='active',awaiting_user_since=NULL,extend_count=0 WHERE id=$1",
        UUID(runtime.owner_id),
    )
    same = await apply(
        db, runtime, "presence", revision=2, episode_id=first.episode.episode_id
    )
    assert same == extended and same.episode.extend_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "assignment",
    [
        "workspace_idle_revision=0",
        "workspace_idle_episode=NULL",
        "workspace_idle_revision=2,workspace_idle_episode=jsonb_set(workspace_idle_episode,'{entered_at}',to_jsonb(clock_timestamp()))",
    ],
)
async def test_database_refuses_revision_rollback_unfenced_change_and_age_reset(
    db, assignment
):
    runtime = await owner(db)
    await apply(db, runtime, wait_key=str(uuid4()))
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE jobs SET " + assignment + " WHERE id=$1", UUID(runtime.owner_id)
        )


@pytest.mark.asyncio
async def test_helper_requires_existing_workflow_transaction(db):
    from shared.workspace_idle_store import apply_idle_transition_on_conn

    runtime = await owner(db)
    async with db.acquire() as conn:
        with pytest.raises(IdlePolicyError, match="idle_transaction_required"):
            await apply_idle_transition_on_conn(
                conn,
                runtime=runtime,
                event="enter",
                expected_revision=0,
                expected_episode_id=None,
                wait_kind="human_message",
                wait_key=str(uuid4()),
            )
