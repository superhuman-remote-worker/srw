"""Every committed terminal Job writer closes its native idle episode once."""

from uuid import UUID, uuid4

import asyncpg
import pytest

from tests.test_workspace_idle_store_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    owner,
    apply,
)

db = _db_fixture


async def state(db, identity, kind="jobs"):
    return dict(
        await db.fetchrow(
            "SELECT status,workspace_idle_revision,workspace_idle_episode FROM "
            + kind
            + " WHERE id=$1",
            UUID(identity.owner_id),
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
async def test_native_terminal_write_clears_once_with_tracking_off(
    db, monkeypatch, status
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    runtime = await owner(db)
    await apply(db, runtime, wait_key=str(uuid4()))
    # There is deliberately no currently-attested VM in this fixture. Clearing
    # terminal metadata must not require new runtime or physical authority.
    await db.execute(
        "UPDATE jobs SET status=$2 WHERE id=$1", UUID(runtime.owner_id), status
    )
    assert await state(db, runtime) == dict(
        status=status, workspace_idle_revision=2, workspace_idle_episode=None
    )
    await db.execute(
        "UPDATE jobs SET status=$2 WHERE id=$1", UUID(runtime.owner_id), status
    )
    assert (await state(db, runtime))["workspace_idle_revision"] == 2


@pytest.mark.asyncio
async def test_existing_explicit_exit_is_not_incremented_twice(db):
    runtime = await owner(db)
    await apply(db, runtime, wait_key=str(uuid4()))
    await db.execute(
        "UPDATE jobs SET status='completed',workspace_idle_episode=NULL,"
        "workspace_idle_revision=workspace_idle_revision+1 WHERE id=$1",
        UUID(runtime.owner_id),
    )
    assert (await state(db, runtime))["workspace_idle_revision"] == 2


@pytest.mark.asyncio
async def test_terminal_no_episode_does_not_create_revision(db):
    runtime = await owner(db)
    await db.execute(
        "UPDATE jobs SET status='completed' WHERE id=$1", UUID(runtime.owner_id)
    )
    assert (await state(db, runtime))["workspace_idle_revision"] == 0


@pytest.mark.asyncio
async def test_episode_cannot_be_introduced_on_terminal_job(db):
    runtime = await owner(db)
    await db.execute(
        "UPDATE jobs SET status='failed' WHERE id=$1", UUID(runtime.owner_id)
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await apply(db, runtime, wait_key=str(uuid4()))
    assert (await state(db, runtime))["workspace_idle_revision"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["revision", "document"])
async def test_terminal_transition_cannot_hide_explicit_bad_episode_mutation(
    db, mutation
):
    runtime = await owner(db)
    await apply(db, runtime, wait_key=str(uuid4()))
    before = await state(db, runtime)
    clause = (
        "workspace_idle_revision=workspace_idle_revision+2"
        if mutation == "revision"
        else "workspace_idle_episode=jsonb_set(workspace_idle_episode,'{wait_key}','\"replacement\"'::jsonb)"
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE jobs SET status='failed'," + clause + " WHERE id=$1",
            UUID(runtime.owner_id),
        )
    assert await state(db, runtime) == before


@pytest.mark.asyncio
async def test_terminal_source_transaction_rollback_preserves_episode(db):
    runtime = await owner(db)
    await apply(db, runtime, wait_key=str(uuid4()))
    before = await state(db, runtime)
    async with db.acquire() as conn:
        with pytest.raises(RuntimeError):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE jobs SET status='failed' WHERE id=$1",
                    UUID(runtime.owner_id),
                )
                assert (
                    await conn.fetchval(
                        "SELECT workspace_idle_episode FROM jobs WHERE id=$1",
                        UUID(runtime.owner_id),
                    )
                    is None
                )
                raise RuntimeError("roll back terminal source")
    assert await state(db, runtime) == before


@pytest.mark.asyncio
async def test_guarded_pinned_cancel_and_generic_status_writer_close_episode(db):
    runtime = await owner(db)
    await db.execute(
        "UPDATE jobs SET execution_lane='pinned' WHERE id=$1", UUID(runtime.owner_id)
    )
    await apply(db, runtime, wait_key=str(uuid4()))
    before = await state(db, runtime)
    assert not await db.linearize_pinned_cancel(
        runtime.owner_id, expected_status="processing"
    )
    assert await state(db, runtime) == before
    assert await db.linearize_pinned_cancel(
        runtime.owner_id, expected_status="waiting_for_reply"
    )
    assert (await state(db, runtime))["workspace_idle_episode"] is None
    second = await owner(db)
    await apply(db, second, wait_key=str(uuid4()))
    assert await db.update_job_status(
        second.owner_id, status="completed", expected_status="waiting_for_reply"
    )
    assert (await state(db, second))["workspace_idle_episode"] is None


@pytest.mark.asyncio
async def test_thread_episode_rules_remain_unchanged(db):
    runtime = await owner(db, kind="thread")
    await apply(db, runtime, wait_key=str(uuid4()))
    before = await state(db, runtime, kind="threads")
    await db.execute(
        "UPDATE threads SET status='ended' WHERE id=$1", UUID(runtime.owner_id)
    )
    after = await state(db, runtime, kind="threads")
    assert after["workspace_idle_episode"] == before["workspace_idle_episode"]
    assert after["workspace_idle_revision"] == before["workspace_idle_revision"]
