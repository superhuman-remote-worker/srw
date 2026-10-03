"""Repeated confirmed pre-setup releases retain creation and purge authority."""

import pytest

from tests.test_pinned_vm_failed_initial_end_real_postgres import _release_binding
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)


async def released_chain(store, controller_setup, monkeypatch, count):
    current, source = await adopted_source(store, controller_setup, monkeypatch)
    for index in range(count):
        previous = current
        assert await _release_binding(store, previous) == "released"
        assert await store.delete_agent(str(previous["agent_id"]))
        current = await store.get_thread(str(previous["id"]))
        assert current["runtime_generation"] != previous["runtime_generation"]
        assert current["agent_id"] is None
        assert current["runtime_attach_token"] is None
        if index + 1 < count:
            current = await _bind_cold_agent(store, current["id"])
    outcomes = [
        dict(row)
        for row in await store.fetch(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 "
            "ORDER BY released_at",
            current["id"],
        )
    ]
    assert len(outcomes) == count
    assert all(
        row["quiescence_protocol"] == "agent_attach_not_started_v1" for row in outcomes
    )
    assert outcomes[0]["runtime_generation"] == source["thread_runtime_generation"]
    assert outcomes[-1]["successor_generation"] == current["runtime_generation"]
    return current, source, outcomes


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 3])
async def test_repeated_confirmed_pre_setup_release_then_normal_permanent_delete(
    db,  # noqa: F811 - imported full-schema PostgreSQL fixture
    setup,  # noqa: F811 - imported actuator fixture
    monkeypatch,
    count,
):
    current, source, outcomes = await released_chain(db, setup, monkeypatch, count)
    retirement = await cleaned_retirement(db, current, permanent=True)
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions WHERE owner_id=$1 "
        "AND source='pinned_thread_retirement' AND completed_at IS NOT NULL "
        "AND outcome='completed')",
        current["id"],
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    audit = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert audit["live_thread_id"] is None and audit["deleted_at"] is not None
    assert str(audit["deleted_retirement_token"]) == retirement["token"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1 ORDER BY released_at",
            current["id"],
        )
    ] == outcomes
