"""0298 accepts a populated published 0296 database without changing history."""

import subprocess
from pathlib import Path

import asyncpg
import pytest

from orchestrator.database.postgres import PostgresDB
from tests.test_vm_end_actuator_handoff_real_postgres import pg_dsn, scenario  # noqa: F401
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_b10_session_queries_real_postgres import _thread
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent


@pytest.mark.asyncio
async def test_populated_0296_to_0298_preserves_pending_begin_and_old_outcome(pg_dsn, monkeypatch):  # noqa: F811
    root = Path(__file__).resolve().parents[1]
    snapshot = subprocess.check_output(
        ["git", "show", "ea7676aa8:src/orchestrator/database/schema_current.sql"],
        cwd=root, text=True,
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(snapshot)
    finally:
        await conn.close()
    db = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=3)
    await db.connect()
    try:
        ids, retirement, request, events, _, _ = await scenario(db, monkeypatch)
        _, historical_id = await _thread(db, lane="pinned", status="created")
        bound = await _bind_protected_agent(db, historical_id)
        historical = {"thread": str(historical_id), "agent": str(bound["agent_id"]),
                      "attach_token": str(bound["runtime_attach_token"])}
        previous = await db.begin_pinned_thread_retirement(historical["thread"], permanent=False)
        assert previous["state"] == "pending", previous
        await fixtures._authorize_and_ack(db, historical, previous)
        assert await db.settle_pinned_thread_retirement(
            historical["thread"], token=previous["token"], generation=previous["generation"],
        )
        before = dict(await db.get_thread(ids["thread"]))
        await db.execute((root / "src/orchestrator/database/migrations/app/0298_pinned_vm_retirement_actuator_request.sql").read_text())
        after = dict(await db.get_thread(ids["thread"]))
        assert after.pop("runtime_retirement_actuator_request") is None
        assert after == before
        assert await db.fetchval(
            "SELECT actuator_request IS NULL FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
            historical["thread"],
        ) is True
        assert await db.request_pinned_thread_retirement_actuator(ids["thread"], **request)
        nominated = await db.list_retryable_pinned_retirements()
        assert [str(row["id"]) for row in nominated] == [ids["thread"]]
        assert events == []
    finally:
        await db.close()
