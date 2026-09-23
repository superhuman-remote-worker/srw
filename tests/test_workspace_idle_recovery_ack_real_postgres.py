"""An exact recovery acknowledgement closes idle state with execution resume."""

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


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_exact_recovery_ack_closes_episode_even_after_tracking_disabled(
    db, monkeypatch, enabled
):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    monkeypatch.setenv("VM_MODE", "same-cluster")
    seed, _ = await seeded(db, delivered=True, monkeypatch=monkeypatch)
    result, route = await publish(db, seed)
    assert result is not None
    before = await episode(db, seed["job_id"])
    assert before[1]["wait_key"] == route["route_id"]
    generation = str(uuid4())
    await db.execute(
        "UPDATE jobs SET status='paused',context=context||$2::jsonb,"
        "freeze_data='{}'::jsonb WHERE id=$1",
        UUID(seed["job_id"]),
        json.dumps({"_lease_recovery": {"state": "tripped", "generation": generation}}),
    )
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", str(enabled).lower())
    snapshot = await db.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(seed["job_id"]))
    kwargs = dict(
        expected_status="paused",
        acknowledged_by={"kind": "user", "id": seed["user_id"]},
        completion_commands_enabled=True,
    )
    assert not await db.acknowledge_lease_recovery_circuit(
        seed["job_id"], expected_generation=str(uuid4()), **kwargs
    )
    assert (
        await db.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(seed["job_id"]))
        == snapshot
    )
    assert await episode(db, seed["job_id"]) == before
    assert await db.acknowledge_lease_recovery_circuit(
        seed["job_id"], expected_generation=generation, **kwargs
    )
    assert await episode(db, seed["job_id"]) == (before[0] + 1, None)
    accepted = await db.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(seed["job_id"]))
    assert not await db.acknowledge_lease_recovery_circuit(
        seed["job_id"], expected_generation=generation, **kwargs
    )
    assert (
        await db.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(seed["job_id"]))
        == accepted
    )
