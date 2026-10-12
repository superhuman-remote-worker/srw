"""Populated 0299→0300 IDE cancellation upgrade without history dependence."""

from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB


@pytest.mark.asyncio
async def test_populated_0299_ide_attempt_survives_0300_then_closes(tmp_path):
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in migrations.glob("*.sql"):
        if path.name.split("_", 1)[0] <= "0299":
            (stage / path.name).write_bytes(path.read_bytes())

    with PostgresContainer("postgres:15") as container:
        dsn = container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
        try:
            await run_migrations(pool, stage)
            await db.connect()
            job_id, attempt_id = uuid4(), uuid4()
            async with db.acquire() as conn:
                await conn.execute(
                    "INSERT INTO jobs (id, description, status, repo_name, context) "
                    "VALUES ($1, 'populated IDE upgrade', 'completed', 'owned-repo', '{}'::jsonb)",
                    job_id,
                )
            admitted = await db.begin_managed_ide_restore_attempt(
                str(job_id),
                attempt_id=str(attempt_id),
                proposed_context={
                    "status": "restoring",
                    "source": "gitea",
                    "snapshot_type": "gitea",
                    "restore_type": "k8s_container",
                    "started_at": "2026-09-28T00:00:00Z",
                },
                desired_manifest_digest="a" * 64,
                claimant="ide-issuer:upgrade",
            )
            assert admitted["disposition"] == "accepted"
            reservation = admitted["reservation"]

            async def snapshot():
                return await pool.fetchrow(
                    "SELECT j.context::text AS context, to_jsonb(r)::text AS reservation "
                    "FROM jobs j JOIN managed_repository_workspace_creation_reservations r "
                    "ON r.owner_id = j.id AND r.owner_kind = 'job' AND r.scope = 'ide' "
                    "WHERE j.id = $1",
                    job_id,
                )

            before = await snapshot()
            old_checksums = await pool.fetch(
                "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
            )
            migration = migrations / "0300_ide_restore_zero_effect_cancellation.sql"
            (stage / migration.name).write_bytes(migration.read_bytes())
            await run_migrations(pool, stage)
            await run_migrations(pool, stage)
            assert await snapshot() == before
            assert (
                await pool.fetch(
                    "SELECT filename,checksum FROM schema_migrations "
                    "WHERE filename=ANY($1::text[]) ORDER BY filename",
                    [row["filename"] for row in old_checksums],
                )
                == old_checksums
            )
            assert await pool.fetchval(
                "SELECT success FROM schema_migrations WHERE filename=$1",
                migration.name,
            )

            closed = await db.cancel_managed_ide_restore_attempt(
                str(job_id),
                attempt_id=str(attempt_id),
                reservation_id=str(reservation["id"]),
                claim_token=int(reservation["claim_token"]),
                claimant="ide-stop:upgrade",
            )
            assert closed is not None
            assert closed["result_kind"] == "aborted"
            assert closed["cancel_projection_transaction_id"] is not None
        finally:
            await db.close()
            await pool.close()
