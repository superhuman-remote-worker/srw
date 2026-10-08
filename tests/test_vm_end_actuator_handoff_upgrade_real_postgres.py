"""0298 accepts a populated published 0296 database without changing history."""

from pathlib import Path

import asyncpg
import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.database.migrate import run_migrations
from tests.test_vm_end_actuator_handoff_real_postgres import pg_dsn as _pg_dsn, scenario
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_b10_session_queries_real_postgres import _thread
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_vm_thread_retained_disk_purge_real_postgres import (
    LEASE_TABLE_MIGRATIONS,
)


pg_dsn = _pg_dsn


@pytest.mark.asyncio
async def test_populated_0296_to_0298_preserves_pending_begin_and_old_outcome(
    pg_dsn, monkeypatch, tmp_path
):
    root = Path(__file__).resolve().parents[1]
    migrations = root / "src/orchestrator/database/migrations/app"
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in migrations.glob("*.sql"):
        # Today's Begin revokes credential leases (C2), so the old head
        # carries the lease tables the code it runs needs.
        if path.name.split("_", 1)[0] <= "0296" or path.name.startswith(
            LEASE_TABLE_MIGRATIONS
        ):
            (stage / path.name).write_bytes(path.read_bytes())
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
    db = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=3)
    try:
        await run_migrations(pool, stage)
        checksums = await pool.fetch(
            "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
        )
        await db.connect()
        # This test deliberately stops at published 0298, whose old marker
        # contract used VM UID. 0299 compatibility is qualified separately.
        ids, retirement, request, events, _, _ = await scenario(
            db, monkeypatch, legacy_vm_incarnation=True
        )
        _, historical_id = await _thread(db, lane="pinned", status="created")
        bound = await _bind_protected_agent(db, historical_id)
        historical = {
            "thread": str(historical_id),
            "agent": str(bound["agent_id"]),
            "attach_token": str(bound["runtime_attach_token"]),
        }
        previous = await db.begin_pinned_thread_retirement(
            historical["thread"], permanent=False
        )
        assert previous["state"] == "pending", previous
        await fixtures._authorize_and_ack(db, historical, previous)
        assert await db.settle_pinned_thread_retirement(
            historical["thread"],
            token=previous["token"],
            generation=previous["generation"],
        )
        before = dict(await db.get_thread(ids["thread"]))
        migration = migrations / "0298_pinned_vm_retirement_actuator_request.sql"
        (stage / migration.name).write_bytes(migration.read_bytes())
        await run_migrations(pool, stage)
        await run_migrations(pool, stage)
        assert (
            await pool.fetch(
                "SELECT filename,checksum FROM schema_migrations WHERE filename=ANY($1::text[]) ORDER BY filename",
                [row["filename"] for row in checksums],
            )
            == checksums
        )
        assert await pool.fetchval(
            "SELECT success FROM schema_migrations WHERE filename=$1", migration.name
        )
        after = dict(await db.get_thread(ids["thread"]))
        assert after.pop("runtime_retirement_actuator_request") is None
        assert after == before
        assert (
            await db.fetchval(
                "SELECT actuator_request IS NULL FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid",
                historical["thread"],
            )
            is True
        )
        assert await db.request_pinned_thread_retirement_actuator(
            ids["thread"], **request
        )
        nominated = await db.list_retryable_pinned_retirements()
        assert [str(row["id"]) for row in nominated] == [ids["thread"]]
        assert events == []
    finally:
        await db.close()
        await pool.close()
