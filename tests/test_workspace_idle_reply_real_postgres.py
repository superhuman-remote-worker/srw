"""An old stateless reply must roll back queue and semantic wait changes."""

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


@pytest.mark.asyncio
async def test_stale_stateless_reply_preserves_new_question_and_queue(db, monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db)
    _, current_route = await publish(db, seed)
    job_id = UUID(seed["job_id"])
    await db.execute(
        "UPDATE jobs SET execution_lane='stateless',assigned_agent_id=NULL WHERE id=$1",
        job_id,
    )
    await db.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,attempts_since_completion,input_seq,consumed_seq,park_reason) "
        "VALUES($1,'worker_batch','parked',17,4,1,1,'human_wait')",
        job_id,
    )

    async def state():
        job = await db.fetchrow(
            "SELECT status,context,freeze_data,workspace_idle_revision,workspace_idle_episode FROM jobs WHERE id=$1",
            job_id,
        )
        queue = await db.fetchrow("SELECT * FROM run_queue WHERE unit_id=$1", job_id)
        return dict(job), dict(queue)

    original = await state()
    assert not await db.queue_stateless_job_for_resume(
        seed["job_id"],
        {"queued_feedback": "old question"},
        expected_status="waiting_for_reply",
        expected_route_id=str(uuid4()),
    )
    assert await state() == original
    assert await db.queue_stateless_job_for_resume(
        seed["job_id"],
        {"queued_feedback": "current answer"},
        expected_status="waiting_for_reply",
        expected_route_id=current_route["route_id"],
    )
    assert await episode(db, seed["job_id"]) == (2, None)
    job, queue = await state()
    assert job["status"] == "paused" and job["freeze_data"] is None
    assert (
        queue["state"] == "queued"
        and queue["attempts_since_completion"] == 0
        and queue["lease_token"] == 17
    )
    assert not await db.queue_stateless_job_for_resume(
        seed["job_id"],
        {"queued_feedback": "duplicate answer"},
        expected_status="waiting_for_reply",
        expected_route_id=current_route["route_id"],
    )
    assert await state() == (job, queue)
