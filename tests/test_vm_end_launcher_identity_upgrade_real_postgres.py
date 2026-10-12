"""0299 fixes new launcher admission without rewriting 0298 durable authority."""

import json
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest

from orchestrator import main
from orchestrator.application import controls
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import stale_agent_detector as detector
from tests._connector_lease_migrations import is_lease_table_migration
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_vm_end_actuator_handoff_real_postgres import pg_dsn as _pg_dsn, scenario


pg_dsn = _pg_dsn


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_launcher_present", [True, False])
async def test_populated_0298_to_0299_preserves_exact_legacy_continuation(
    pg_dsn, monkeypatch, tmp_path, legacy_launcher_present
):
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in migrations.glob("*.sql"):
        # Today's Begin revokes credential leases (C2), so the old head
        # carries the lease tables the code it runs needs.
        if path.name.split("_", 1)[0] <= "0298" or is_lease_table_migration(path.name):
            (stage / path.name).write_bytes(path.read_bytes())
    database = "launcher_upgrade_" + uuid4().hex
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'CREATE DATABASE "{database}"')
    dsn = urlunsplit(urlsplit(pg_dsn)._replace(path=f"/{database}"))
    pool = db = None
    try:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        await run_migrations(pool, stage)
        checksums = await pool.fetch(
            "SELECT filename,checksum FROM schema_migrations ORDER BY filename"
        )
        db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=8)
        await db.connect()
        historical, _, old_request, _, _, _ = await scenario(
            db, monkeypatch, legacy_vm_incarnation=True
        )
        assert await db.request_pinned_thread_retirement_actuator(
            historical["thread"], **old_request
        )
        dependencies = controls.stale_agent_detector_dependencies(
            main.app.state.resources
        )
        candidate = (await db.list_retryable_pinned_retirements())[0]
        assert await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )

        native, _, native_request, _, _, _ = await scenario(db, monkeypatch)
        legacy, retirement, legacy_request, events, _, _ = await scenario(
            db,
            monkeypatch,
            legacy_vm_incarnation=True,
            vm_updates={} if legacy_launcher_present else {"active_pod_uid": None},
        )
        accepted = await db.request_pinned_thread_retirement_actuator(
            legacy["thread"], **legacy_request
        )
        assert accepted is not None
        stored_marker = accepted["actuator_request"]
        candidate = (await db.list_retryable_pinned_retirements())[0]
        assert str(candidate["id"]) == legacy["thread"]

        async def snapshot():
            return {
                table: await db.fetch(
                    f"SELECT to_jsonb(r)::text AS row FROM {table} r ORDER BY row"
                )
                for table in ("threads", "agents", "thread_runtime_retirement_outcomes")
            }

        before = await snapshot()
        migration = migrations / "0299_pinned_vm_actuator_launcher_identity.sql"
        (stage / migration.name).write_bytes(migration.read_bytes())
        await run_migrations(pool, stage)
        await run_migrations(pool, stage)
        assert await snapshot() == before
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

        # The legacy arm is continuation only, never an alternative identity
        # accepted at the INSERT boundary or for a fabricated nonstored marker.
        assert not await db.fetchval(
            "SELECT pinned_vm_actuator_request_valid(t,$2::jsonb,true) FROM threads t WHERE id=$1::uuid",
            legacy["thread"],
            json.dumps(stored_marker),
        )
        assert (
            await db.request_pinned_thread_retirement_actuator(
                legacy["thread"], **legacy_request
            )
            == accepted
        )
        for field in (
            "runtime_generation",
            "retirement_token",
            "process_generation",
            "workspace_runtime_incarnation",
        ):
            assert (
                await db.request_pinned_thread_retirement_actuator(
                    legacy["thread"],
                    **{**legacy_request, field: str(uuid4())},
                )
                is None
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE threads SET runtime_retirement_actuator_request=$2::jsonb WHERE id=$1::uuid",
                legacy["thread"],
                json.dumps({**stored_marker, "process_generation": str(uuid4())}),
            )
        assert await snapshot() == before

        current = await db.get_thread(native["thread"])
        vm_uid = json.loads(current["runtime_retirement_context"])["vm"]["vm_uid"]
        fabricated = {
            "kind": "vm_local_drain_complete_v1",
            "thread_id": native["thread"],
            **native_request,
            "workspace_runtime_incarnation": vm_uid,
        }
        assert not await db.fetchval(
            "SELECT pinned_vm_actuator_request_valid(t,$2::jsonb,false) FROM threads t WHERE id=$1::uuid",
            native["thread"],
            json.dumps(fabricated),
        )
        assert (
            await db.request_pinned_thread_retirement_actuator(
                native["thread"],
                **{**native_request, "workspace_runtime_incarnation": vm_uid},
            )
            is None
        )
        assert await db.request_pinned_thread_retirement_actuator(
            native["thread"], **native_request
        )

        assert await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )
        assert events == ["pod-stop", "vm-stop"]
        archived = await db.fetchval(
            "SELECT actuator_request FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid AND retirement_token=$2::uuid",
            legacy["thread"],
            retirement["token"],
        )
        assert json.loads(archived) == stored_marker
        assert await db.resume_thread(legacy["thread"])
        await _bind_protected_agent(db, legacy["thread"])
        successor = dict(await db.get_thread(legacy["thread"]))
        assert (
            await db.request_pinned_thread_retirement_actuator(
                legacy["thread"], **legacy_request
            )
        )["status"] == "settled_or_superseded"
        assert not await detector.retry_pending_pinned_retirement(
            candidate, dependencies=dependencies
        )
        assert dict(await db.get_thread(legacy["thread"])) == successor
        assert (
            await db.request_pinned_thread_retirement_actuator(
                historical["thread"], **old_request
            )
        )["status"] == "settled_or_superseded"
        assert events == ["pod-stop", "vm-stop"]
    finally:
        if db is not None:
            await db.close()
        if pool is not None:
            await pool.close()
        await admin.execute(f'DROP DATABASE "{database}"')
        await admin.close()
