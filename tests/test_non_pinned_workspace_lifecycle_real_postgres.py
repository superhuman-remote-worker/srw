"""Real-PostgreSQL proof for exact stateless workspace process-zero receipts."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.ide_session import IdeSessionService
from shared.worker_execution_hold import WORKER_EXECUTION_HOLD_KEY


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
NON_PINNED_LIFECYCLE_MIGRATIONS = tuple(
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "app"
    / filename
    for filename in (
        "0197_non_pinned_workspace_process_zero.sql",
        "0198_non_pinned_workspace_lifecycle_authority.sql",
        "0210_thread_terminal_reclaim_projection.sql",
    )
)


async def _execute_pre_0195(conn, query: str, *args):
    """Insert a previous-release row without teaching production a bypass."""

    async with conn.transaction():
        await conn.execute("SET LOCAL session_replication_role = replica")
        return await conn.execute(query, *args)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
        # schema_current.sql is a replay of the whole migration chain, so it
        # already carries these files once they are regenerated into it.
        # Applying them a second time would fail on CREATE TABLE/TRIGGER.
        present = await conn.fetchval(
            "SELECT to_regclass("
            "'public.managed_repository_workspace_creation_reservations'"
            ") IS NOT NULL"
        )
        if not present:
            for migration in NON_PINNED_LIFECYCLE_MIGRATIONS:
                await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=4,
    )
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_ide_restore_attempt_commits_intent_and_creation_receipt_together(db):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'atomic IDE restore', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )

    result = await db.begin_managed_ide_restore_attempt(
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
        claimant=f"ide-issuer:{uuid4()}",
    )
    assert result["disposition"] == "accepted"
    reservation = result["reservation"]
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT context FROM jobs WHERE id = $1", job_id
        )
    context = row["context"]
    if isinstance(context, str):
        context = json.loads(context)
    ide = context["ide_session"]
    assert ide["status"] == "restoring"
    assert ide["restore_type"] == "k8s_container"
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert ide["_creation_reservation_id"] == str(reservation["id"])
    assert ide["_creation_claim_token"] == str(reservation["claim_token"])
    assert reservation["operation_kind"] == "restore"
    fingerprint = reservation["lifecycle_fingerprint"]
    if isinstance(fingerprint, str):
        fingerprint = json.loads(fingerprint)
    assert fingerprint["restore_attempt_id"] == str(attempt_id)


@pytest.mark.asyncio
async def test_ide_restore_attempt_zero_effect_delete_closes_exact_projection(db):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'IDE preeffect DELETE', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id),
        attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="e" * 64,
        claimant="ide-issuer:cancel",
    )
    assert admitted["disposition"] == "accepted"
    reservation = admitted["reservation"]
    exact = dict(
        attempt_id=str(attempt_id),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]),
        claimant="ide-stop:cancel",
    )
    assert await db.cancel_managed_ide_restore_attempt(str(job_id), **{
        **exact, "attempt_id": str(uuid4())
    }) is None
    assert await db.cancel_managed_ide_restore_attempt(str(job_id), **{
        **exact, "claim_token": exact["claim_token"] + 1
    }) is None
    closed = await db.cancel_managed_ide_restore_attempt(str(job_id), **exact)
    assert closed is not None
    assert closed["result_kind"] == "aborted"
    assert closed["cancel_cleanup_completed_at"] is not None
    assert closed["cancel_projection_transaction_id"] is not None
    async with db.acquire() as conn:
        raw = await conn.fetchval("SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id)
    ide = json.loads(raw) if isinstance(raw, str) else raw
    assert ide["status"] == "expired"
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert "_creation_reservation_id" not in ide
    assert "_creation_claim_token" not in ide
    assert ide["code_server_url"] is None
    assert ide["stopped_at"]
    assert await db.cancel_managed_ide_restore_attempt(str(job_id), **exact) is None

    successor_id = uuid4()
    successor = await db.begin_managed_ide_restore_attempt(
        str(job_id),
        attempt_id=str(successor_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:01:00Z",
        },
        desired_manifest_digest="f" * 64,
        claimant="ide-issuer:successor",
    )
    assert successor["disposition"] == "accepted"
    assert successor["reservation"]["id"] != reservation["id"]
    async with db.acquire() as conn:
        raw = await conn.fetchval("SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id)
    new_ide = json.loads(raw) if isinstance(raw, str) else raw
    assert new_ide["status"] == "restoring"
    assert new_ide["_restore_attempt_id"] == str(successor_id)


@pytest.mark.asyncio
async def test_ide_restore_attempt_started_effect_delete_stays_reconcilable(db):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'IDE started DELETE', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="a" * 64, claimant="ide-issuer:started",
    )
    assert admitted["disposition"] == "accepted"
    reservation = admitted["reservation"]
    gate = dict(
        owner_kind="job", scope="ide",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="ide-issuer:started", claim_token=int(reservation["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(str(job_id), **gate)
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate, resource_kind="seed"
    )
    cancelled = await db.cancel_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id), reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]), claimant="ide-stop:started",
    )
    assert cancelled is not None
    assert cancelled["result_kind"] is None
    assert cancelled["cancel_requested_at"] is not None
    assert cancelled["cancel_cleanup_completed_at"] is None
    assert cancelled["claim_token"] != reservation["claim_token"]
    async with db.acquire() as conn:
        raw = await conn.fetchval("SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id)
    ide = json.loads(raw) if isinstance(raw, str) else raw
    assert ide["status"] == "restoring"
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert ide["_creation_claim_token"] == str(cancelled["claim_token"])
    assert await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(uuid4()),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:01:00Z",
        },
        desired_manifest_digest="b" * 64, claimant="ide-issuer:blocked",
    ) == {"disposition": "held"}


@pytest.mark.asyncio
async def test_ide_restore_ambiguous_creation_refuses_zero_effect_close(db):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'ambiguous IDE', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="a" * 64, claimant="ide-issuer:ambiguous",
    )
    assert admitted["disposition"] == "accepted"
    reservation = admitted["reservation"]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET phase = 'ambiguous' WHERE id = $1", reservation["id"]
        )
    assert await db.cancel_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]), claimant="ide-stop:ambiguous",
    ) is None
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT j.context->'ide_session' AS ide, r.result_kind, r.cancel_requested_at "
            "FROM jobs j JOIN managed_repository_workspace_creation_reservations r "
            "ON r.owner_id = j.id WHERE j.id = $1 AND r.id = $2",
            job_id, reservation["id"],
        )
    ide = json.loads(row["ide"]) if isinstance(row["ide"], str) else row["ide"]
    assert ide["status"] == "restoring"
    assert row["result_kind"] is None
    assert row["cancel_requested_at"] is None


@pytest.mark.asyncio
async def test_ide_pod_preeffect_refusal_closes_exact_attempt_not_row_alone(
    db, monkeypatch
):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'IDE preeffect refusal', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="a" * 64, claimant="ide-issuer:preeffect",
    )
    assert admitted["disposition"] == "accepted"
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(
        return_value={"digest": "a" * 64, "network_tier": "standard"}
    )
    provisioner._start_workspace_creation_reservation = AsyncMock(return_value=False)

    @asynccontextmanager
    async def owned(*_args, **_kwargs):
        yield True

    provisioner._workspace_mutation_guard = owned
    assert await provisioner.create_ide_pod(
        str(job_id), creation_reservation=admitted["reservation"]
    ) is None
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT j.context->'ide_session' AS ide, r.result_kind, "
            "r.cancel_cleanup_completed_at, r.cancel_projection_transaction_id "
            "FROM jobs j JOIN managed_repository_workspace_creation_reservations r "
            "ON r.owner_id = j.id WHERE j.id = $1 AND r.id = $2",
            job_id, admitted["reservation"]["id"],
        )
    ide = json.loads(row["ide"]) if isinstance(row["ide"], str) else row["ide"]
    assert ide["status"] == "expired"
    assert "_creation_reservation_id" not in ide
    assert row["result_kind"] == "aborted"
    assert row["cancel_cleanup_completed_at"] is not None
    assert row["cancel_projection_transaction_id"] is not None

    # The public POST path must actually admit B after A's exact zero-effect
    # closure. The DB admission still proves the old receipt under owner lock.
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "available"})
    service._schedule_restore_task = MagicMock()
    response = await service.start_session(str(job_id))
    assert response["status"] == "restoring"
    service._schedule_restore_task.assert_called_once()
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, result_kind FROM "
            "managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1 AND scope = 'ide' "
            "ORDER BY reservation_generation",
            job_id,
        )
    assert len(rows) == 2
    assert rows[0]["result_kind"] == "aborted"
    assert rows[1]["result_kind"] is None
    assert rows[1]["id"] != rows[0]["id"]


@pytest.mark.asyncio
async def test_ide_restore_attempt_hold_rolls_back_intent_and_reservation(db):
    job_id = uuid4()
    initial = {WORKER_EXECUTION_HOLD_KEY: {"reason": "test-hold"}}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'held IDE restore', 'completed', 'owned-repo', $2::jsonb)",
            job_id,
            json.dumps(initial),
        )

    result = await db.begin_managed_ide_restore_attempt(
        str(job_id),
        attempt_id=str(uuid4()),
        proposed_context={
            "status": "restoring",
            "source": "gitea",
            "snapshot_type": "gitea",
            "restore_type": "k8s_container",
            "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="b" * 64,
        claimant=f"ide-issuer:{uuid4()}",
    )
    assert result == {"disposition": "held", "reason": "worker_execution_hold"}
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT context FROM jobs WHERE id = $1", job_id)
        count = await conn.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1 AND scope = 'ide'",
            job_id,
        )
    context = row["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert context == initial
    assert count == 0


@pytest.mark.asyncio
async def test_ide_attempt_post_reservation_failure_rolls_back_both_rows(
    db, monkeypatch
):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'IDE admission rollback', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    original = db.reserve_managed_repository_workspace_creation

    async def fail_after_insert(*args, **kwargs):
        result = await original(*args, **kwargs)
        assert result is not None
        raise RuntimeError("injected before final owner projection")

    monkeypatch.setattr(
        db, "reserve_managed_repository_workspace_creation", fail_after_insert
    )
    with pytest.raises(RuntimeError, match="injected before final owner projection"):
        await db.begin_managed_ide_restore_attempt(
            str(job_id), attempt_id=str(uuid4()),
            proposed_context={
                "status": "restoring", "source": "gitea",
                "snapshot_type": "gitea", "restore_type": "k8s_container",
                "started_at": "2026-09-28T00:00:00Z",
            },
            desired_manifest_digest="a" * 64, claimant="ide-issuer:rollback",
        )
    async with db.acquire() as conn:
        context = await conn.fetchval("SELECT context FROM jobs WHERE id = $1", job_id)
        count = await conn.fetchval(
            "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1 AND scope = 'ide'",
            job_id,
        )
    if isinstance(context, str):
        context = json.loads(context)
    assert context == {}
    assert count == 0


@pytest.mark.asyncio
async def test_ide_attempt_pod_snapshot_source_and_absent_source(db):
    snapshot_job, empty_job = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, context) VALUES "
            "($1, 'IDE Pod snapshot', 'completed', $3::jsonb), "
            "($2, 'IDE no source', 'completed', '{}'::jsonb)",
            snapshot_job, empty_job,
            json.dumps({"snapshot": {"status": "available", "source_type": "pod"}}),
        )
    proposed = {
        "status": "restoring", "source": "snapshot", "snapshot_type": "pod",
        "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
    }
    accepted = await db.begin_managed_ide_restore_attempt(
        str(snapshot_job), attempt_id=str(uuid4()),
        proposed_context=proposed, desired_manifest_digest="a" * 64,
        claimant="ide-issuer:snapshot",
    )
    assert accepted["disposition"] == "accepted"
    denied = await db.begin_managed_ide_restore_attempt(
        str(empty_job), attempt_id=str(uuid4()),
        proposed_context=proposed, desired_manifest_digest="a" * 64,
        claimant="ide-issuer:absent",
    )
    assert denied == {"disposition": "definitively_denied"}
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT context FROM jobs WHERE id = $1", empty_job
        ) in ({}, "{}")


@pytest.mark.asyncio
async def test_ide_restore_worklist_selects_only_expired_exact_attempts(db):
    matching_job, stale_job = uuid4(), uuid4()
    start_generation = None
    for job_id in (matching_job, stale_job):
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, description, status, repo_name, context) "
                "VALUES ($1, 'restore worklist', 'completed', 'owned-repo', '{}'::jsonb)",
                job_id,
            )
        admitted = await db.begin_managed_ide_restore_attempt(
            str(job_id),
            attempt_id=str(uuid4()),
            proposed_context={
                "status": "restoring",
                "source": "gitea",
                "snapshot_type": "gitea",
                "restore_type": "k8s_container",
                "started_at": "2026-09-28T00:00:00Z",
            },
            desired_manifest_digest="c" * 64,
            claimant=f"ide-issuer:{uuid4()}",
        )
        assert admitted["disposition"] == "accepted"
        if start_generation is None:
            start_generation = int(admitted["reservation"]["reservation_generation"])
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET created_at = now() - interval '2 minutes', "
                "expires_at = now() - interval '1 second' "
                "WHERE id = $1",
                admitted["reservation"]["id"],
            )
            if job_id == stale_job:
                await conn.execute(
                    "UPDATE jobs SET context = jsonb_set(context, "
                    "'{ide_session,_restore_attempt_id}', to_jsonb($2::text)) "
                    "WHERE id = $1",
                    job_id,
                    str(uuid4()),
                )

    rows = await db.list_pending_ide_restore_attempts(
        limit=10, after_generation=start_generation - 1
    )
    assert [str(row["owner_id"]) for row in rows] == [str(matching_job)]


@pytest.mark.asyncio
async def test_expired_ide_attempt_one_claimant_rotates_owner_receipt(db):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'IDE claimant race', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="a" * 64, claimant="ide-issuer:lost",
    )
    assert admitted["disposition"] == "accepted"
    first = admitted["reservation"]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            first["id"],
        )
    kwargs = dict(
        owner_kind="job", scope="ide", operation_kind="restore",
        desired_manifest_digest="a" * 64,
        expected_existing_reservation_id=str(first["id"]),
        expected_existing_claim_token=int(first["claim_token"]),
        expected_restore_attempt_id=str(attempt_id),
    )
    claims = await asyncio.gather(*(
        db.reserve_managed_repository_workspace_creation(
            str(job_id), claimant=f"ide-maintenance:{index}", **kwargs
        ) for index in (1, 2)
    ))
    accepted = [row for row in claims if row is not None]
    assert len(accepted) == 1
    winner = accepted[0]
    assert winner["id"] == first["id"]
    assert winner["claim_token"] != first["claim_token"]
    async with db.acquire() as conn:
        raw = await conn.fetchval("SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id)
    ide = json.loads(raw) if isinstance(raw, str) else raw
    assert ide["_creation_claim_token"] == str(winner["claim_token"])
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert await db.list_pending_ide_restore_attempts(
        limit=10, after_generation=int(first["reservation_generation"]) - 1
    ) == []


@pytest.mark.asyncio
async def test_0300_refuses_unreceipted_uidless_ide_retirement(db):
    job_id = uuid4()
    legacy = {"ide_session": {
        "status": "restoring", "restore_type": "k8s_container",
        "source": "gitea", "snapshot_type": "gitea",
        "started_at": "2026-09-28T00:00:00Z",
    }}
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'legacy UID-less IDE', 'completed', 'owned-repo', $2::jsonb)",
            job_id, json.dumps(legacy),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, '{ide_session,status}', "
                "'\"expired\"'::jsonb) WHERE id = $1",
                job_id,
            )
    assert await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(uuid4()),
        proposed_context={**legacy["ide_session"], "status": "restoring"},
        desired_manifest_digest="a" * 64, claimant="ide-issuer:legacy",
    ) == {"disposition": "held"}


@pytest.mark.asyncio
async def test_0300_refuses_live_runtime_ide_retirement_without_zero(db):
    job_id, attempt_id, runtime = uuid4(), uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'live IDE zero guard', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        proposed_context={
            "status": "restoring", "source": "gitea", "snapshot_type": "gitea",
            "restore_type": "k8s_container", "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="a" * 64, claimant="ide-issuer:live",
    )
    assert admitted["disposition"] == "accepted"
    reservation = admitted["reservation"]
    gate = dict(
        owner_kind="job", scope="ide",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="ide-issuer:live", claim_token=int(reservation["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(str(job_id), **gate)
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate, runtime_incarnation=str(runtime)
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = jsonb_set(context, "
            "'{ide_session,_runtime_incarnation}', to_jsonb($2::text)) WHERE id = $1",
            job_id, str(runtime),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, '{ide_session,status}', "
                "'\"expired\"'::jsonb) WHERE id = $1",
                job_id,
            )
    assert await db.cancel_managed_ide_restore_attempt(
        str(job_id), attempt_id=str(attempt_id),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]), claimant="ide-stop:live",
    ) is None


@pytest.mark.asyncio
async def test_ide_restore_worklist_finds_settled_runtime_with_unfinished_work(db):
    job_id, runtime = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'settled IDE work', 'completed', 'owned-repo', '{}'::jsonb)",
            job_id,
        )
    admitted = await db.begin_managed_ide_restore_attempt(
        str(job_id),
        attempt_id=str(uuid4()),
        proposed_context={
            "status": "restoring",
            "source": "gitea",
            "snapshot_type": "gitea",
            "restore_type": "k8s_container",
            "started_at": "2026-09-28T00:00:00Z",
        },
        desired_manifest_digest="d" * 64,
        claimant="ide-issuer:settled",
    )
    assert admitted["disposition"] == "accepted"
    reservation = admitted["reservation"]
    gate = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="ide-issuer:settled",
        claim_token=int(reservation["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate, runtime_incarnation=str(runtime)
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = jsonb_set(context, "
            "'{ide_session,_runtime_incarnation}', to_jsonb($2::text)) "
            "WHERE id = $1",
            job_id,
            str(runtime),
        )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate, runtime_incarnation=str(runtime)
    )

    rows = await db.list_pending_ide_restore_attempts(
        limit=10,
        after_generation=int(reservation["reservation_generation"]) - 1,
    )
    assert [str(row["owner_id"]) for row in rows] == [str(job_id)]
    assert rows[0]["result_kind"] == "settled"
    assert rows[0]["runtime_incarnation"] == runtime


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("completed", "failed", "cancelled"))
async def test_terminal_job_ide_restore_traverses_creation_and_work_gates(db, status):
    job_id, runtime = uuid4(), str(uuid4())
    restoring = {
        "ide_session": {
            "status": "restoring",
            "source": "gitea",
            "snapshot_type": "gitea",
            "started_at": "2026-09-28T00:00:00Z",
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'terminal IDE restore', $2, 'job-repo', $3::jsonb)",
            job_id,
            status,
            json.dumps(restoring),
        )

    common = dict(owner_kind="job", scope="ide")
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            **common,
            claimant="not-a-restore",
            operation_kind="create",
            desired_manifest_digest="0" * 64,
        )
        is None
    )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="execution",
            operation_kind="restore",
            desired_manifest_digest="0" * 64,
        )
        is None
    )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        **common,
        claimant="ide-restore",
        operation_kind="restore",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    generation = reservation["reservation_generation"]
    token = reservation["claim_token"]
    gate = dict(
        **common,
        reservation_generation=generation,
        claimant="ide-restore",
        claim_token=token,
    )
    assert (
        await db.mark_managed_repository_workspace_creation_started(
            str(job_id),
            **gate,
        )
        is not None
    )
    assert await db.managed_repository_workspace_creation_claim_is_current(
        str(job_id),
        **gate,
    )
    assert (
        await db.begin_managed_repository_workspace_creation_effect(
            str(job_id),
            **gate,
            resource_kind="pod",
        )
        is not None
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id),
        **gate,
        runtime_incarnation=runtime,
    )
    restoring["ide_session"].update(
        {
            "restore_type": "k8s_container",
            "_runtime_incarnation": runtime,
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(token),
        }
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(restoring),
        )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id),
        **gate,
        runtime_incarnation=runtime,
    )
    assert (
        await db.get_current_managed_repository_workspace_creation_result(
            str(job_id),
            **common,
            operation_kind="restore",
        )
        is not None
    )
    claimed = await db.claim_current_managed_repository_workspace_restore_work(
        str(job_id),
        **common,
        claimant="restore-work",
    )
    assert claimed is not None
    work = dict(
        **common,
        reservation_id=str(reservation["id"]),
        runtime_incarnation=runtime,
        claimant="restore-work",
        work_claim_token=int(claimed["restore_work_claim_token"]),
    )
    assert (
        await db.renew_managed_repository_workspace_restore_work(
            str(job_id),
            **work,
        )
        is not None
    )
    assert await db.release_managed_repository_workspace_restore_work(
        str(job_id),
        **work,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET restore_work_next_attempt_at = now() - interval '1 second' "
            "WHERE id = $1",
            reservation["id"],
        )
    claimed = await db.claim_current_managed_repository_workspace_restore_work(
        str(job_id),
        **common,
        claimant="restore-work",
    )
    assert claimed is not None
    work["work_claim_token"] = int(claimed["restore_work_claim_token"])
    assert await db.complete_managed_repository_workspace_restore_work(
        str(job_id),
        **work,
        result_kind="active",
        code_server_url="http://10.42.0.31:8080",
        last_activity="2026-09-28T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_terminal_job_ide_restore_requires_current_supported_intent(db):
    job_id = uuid4()
    state = {
        "ide_session": {
            "status": "restoring",
            "source": "snapshot",
            "snapshot_type": "vm",
        },
        "snapshot": {"status": "available", "source_type": "vm"},
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'IDE source guard', 'completed', $2::jsonb)",
            job_id,
            json.dumps(state),
        )
    reserve = dict(
        owner_kind="job",
        scope="ide",
        claimant="source-guard",
        operation_kind="restore",
        desired_manifest_digest="0" * 64,
    )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            **reserve,
        )
        is None
    )
    state["ide_session"]["snapshot_type"] = "pod"
    state["snapshot"]["source_type"] = "pod"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(state),
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        **reserve,
    )
    assert reservation is not None
    state["ide_session"]["source"] = "unsupported"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(state),
        )
    assert (
        await db.mark_managed_repository_workspace_creation_started(
            str(job_id),
            owner_kind="job",
            scope="ide",
            reservation_generation=reservation["reservation_generation"],
            claimant="source-guard",
            claim_token=reservation["claim_token"],
        )
        is None
    )


async def _vm_process_zero(conn, owner_kind, owner, generation):
    await conn.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation,observed_at) "
        "VALUES ($1,$2,'vm','vm',$3,now())",
        owner_kind,
        owner,
        generation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
async def test_vm_heartbeat_does_not_create_ide_runtime_authority(db, owner_kind):
    from orchestrator.security.vm_guest import VmGuestIdentity
    from orchestrator.services.vm_guest_events import record_heartbeat

    owner, generation = uuid4(), str(uuid4())
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    state = {"vm": {"provision_generation": generation, "status": "ready"}}
    async with db.acquire() as conn:
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs (id, description, status, context) VALUES ($1, 'VM heartbeat', 'failed', $2::jsonb)",
                owner,
                json.dumps(state),
            )
        else:
            await conn.execute(
                "INSERT INTO threads (id, status, execution_lane, metadata) VALUES ($1, 'ended', 'stateless', $2::jsonb)",
                owner,
                json.dumps(state),
            )
    assert await record_heartbeat(
        db,
        VmGuestIdentity(owner_kind, str(owner), generation),
        {"code_server_connections": 0},
    )
    async with db.acquire() as conn:
        value = await conn.fetchval(f"SELECT {column} FROM {table} WHERE id=$1", owner)
        current = json.loads(value) if isinstance(value, str) else value
        assert "ide_session" not in current
        assert current["vm"]["code_server_connections"] == 0
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)
        await _vm_process_zero(conn, owner_kind, owner, generation)
        await conn.execute(
            f"UPDATE {table} SET {column}=jsonb_set({column}, '{{vm,status}}', '\"deleted\"'::jsonb) WHERE id=$1",
            owner,
        )
        assert (
            await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner) == "DELETE 1"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
@pytest.mark.parametrize(
    "existing",
    [
        {
            "restore_type": "k8s_container",
            "status": "active",
            "_runtime_incarnation": "179da88f-1bf0-4b6e-8586-7ea94c545e52",
            "pod_ip": "10.42.0.9",
        },
        {"status": "idle", "pod_name": "legacy-ide"},
        {"restore_type": "vm", "status": "expired"},
        {"restore_type": "vm", "status": "restoring"},
    ],
)
async def test_vm_heartbeat_preserves_other_and_nonready_ide_authority(
    db, owner_kind, existing
):
    from orchestrator.security.vm_guest import VmGuestIdentity
    from orchestrator.services.vm_guest_events import record_heartbeat

    owner, generation = uuid4(), str(uuid4())
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    state = {
        "vm": {"provision_generation": generation, "status": "ready"},
        "ide_session": existing,
    }
    async with db.acquire() as conn:
        if owner_kind == "job":
            await _execute_pre_0195(
                conn,
                "INSERT INTO jobs (id,description,status,context) VALUES ($1,'VM heartbeat','failed',$2::jsonb)",
                owner,
                json.dumps(state),
            )
        else:
            await _execute_pre_0195(
                conn,
                "INSERT INTO threads (id,status,metadata) VALUES ($1,'ended',$2::jsonb)",
                owner,
                json.dumps(state),
            )
    assert await record_heartbeat(
        db,
        VmGuestIdentity(owner_kind, str(owner), generation),
        {"code_server_connections": 1},
    )
    async with db.acquire() as conn:
        value = await conn.fetchval(
            f"SELECT {column}->'ide_session' FROM {table} WHERE id=$1", owner
        )
        assert (json.loads(value) if isinstance(value, str) else value) == existing


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
async def test_stale_vm_heartbeat_cannot_update_ide_activity(db, owner_kind):
    from orchestrator.security.vm_guest import VmGuestIdentity
    from orchestrator.services.vm_guest_events import record_heartbeat

    owner, generation = uuid4(), str(uuid4())
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    state = {
        "vm": {"provision_generation": generation, "status": "ready"},
        "ide_session": {"restore_type": "vm", "status": "idle"},
    }
    async with db.acquire() as conn:
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs (id,description,status,context) VALUES ($1,'VM heartbeat','failed',$2::jsonb)",
                owner,
                json.dumps(state),
            )
        else:
            await conn.execute(
                "INSERT INTO threads (id,status,metadata) VALUES ($1,'ended',$2::jsonb)",
                owner,
                json.dumps(state),
            )
    assert not await record_heartbeat(
        db,
        VmGuestIdentity(owner_kind, str(owner), str(uuid4())),
        {"code_server_connections": 1},
    )
    merge_ide = (
        db.merge_ide_session_context
        if owner_kind == "job"
        else db.merge_thread_ide_session_context
    )
    # Also cover a generation change between the liveness merge and IDE merge.
    assert not await merge_ide(
        str(owner), {"status": "active"}, expected_vm_generation=str(uuid4())
    )
    async with db.acquire() as conn:
        value = await conn.fetchval(f"SELECT {column} FROM {table} WHERE id=$1", owner)
        assert (json.loads(value) if isinstance(value, str) else value) == state


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
@pytest.mark.parametrize(
    "variant", ("heartbeat", "endpoint", "unknown", "unverified", "stale-generation")
)
async def test_0246_repairs_only_authenticated_vm_heartbeat_placeholders(
    db, owner_kind, variant
):
    owner, generation = uuid4(), str(uuid4())
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    state = {
        "vm": {
            "provision_generation": generation,
            "identity_provision_generation": generation,
            "identity_authenticated": True,
            "vm_uid": str(uuid4()),
            "status": "deleted",
        },
        "ide_session": {"status": "idle", "code_server_connections": 0},
    }
    if variant == "endpoint":
        state["ide_session"]["pod_ip"] = "10.42.0.12"
    elif variant == "unknown":
        state["ide_session"]["future_authority"] = "opaque"
    elif variant == "unverified":
        state["vm"]["identity_authenticated"] = False
    elif variant == "stale-generation":
        state["vm"]["identity_provision_generation"] = str(uuid4())
    async with db.acquire() as conn:
        if owner_kind == "job":
            await _execute_pre_0195(
                conn,
                "INSERT INTO jobs (id,description,status,context) VALUES ($1,'old VM heartbeat','failed',$2::jsonb)",
                owner,
                json.dumps(state),
            )
        else:
            await _execute_pre_0195(
                conn,
                "INSERT INTO threads (id,status,execution_lane,metadata) VALUES ($1,'ended','stateless',$2::jsonb)",
                owner,
                json.dumps(state),
            )
        await _vm_process_zero(conn, owner_kind, owner, generation)
        if owner_kind == "job":
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)
        migration = (
            SCHEMA_FILE.parent / "migrations/app/0246_vm_ide_heartbeat_projection.sql"
        ).read_text()
        await conn.execute(migration)
        await conn.execute(migration)
        value = await conn.fetchval(f"SELECT {column} FROM {table} WHERE id=$1", owner)
        current = json.loads(value) if isinstance(value, str) else value
        if variant == "heartbeat":
            expected = json.loads(json.dumps(state))
            expected.pop("ide_session")
            assert current == expected
            assert (
                await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)
                == "DELETE 1"
            )
        else:
            assert current == state
            if owner_kind == "job":
                with pytest.raises(asyncpg.CheckViolationError):
                    await conn.execute(f"DELETE FROM {table} WHERE id=$1", owner)


@pytest.mark.asyncio
async def test_0246_cannot_hide_an_ide_creation_reservation(db):
    # Separate IDE runtime reservations are currently supported for Jobs.
    owner_kind = "job"
    (
        owner,
        _runtime,
        _reservation,
        _state,
    ) = await _create_inflight_authoritative_runtime(
        db, owner_kind=owner_kind, scope="ide"
    )
    generation = str(uuid4())
    state = {
        "vm": {
            "provision_generation": generation,
            "identity_provision_generation": generation,
            "identity_authenticated": True,
            "vm_uid": str(uuid4()),
            "status": "deleted",
        },
        "ide_session": {"status": "idle", "code_server_connections": 0},
    }
    table, column = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            f"UPDATE {table} SET {column}=$2::jsonb WHERE id=$1",
            owner,
            json.dumps(state),
        )
        migration = (
            SCHEMA_FILE.parent / "migrations/app/0246_vm_ide_heartbeat_projection.sql"
        ).read_text()
        await conn.execute(migration)
        value = await conn.fetchval(f"SELECT {column} FROM {table} WHERE id=$1", owner)
        assert (json.loads(value) if isinstance(value, str) else value) == state
        assert not await conn.fetchval(
            "SELECT vm_ide_heartbeat_cleanup_is_authorized($1,$2,$3::jsonb,$4::jsonb)",
            owner_kind,
            owner,
            json.dumps(state),
            json.dumps({"vm": state["vm"]}),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
async def test_0195_raw_runtime_insert_requires_creation_reservation(db, owner_kind):
    owner_id = uuid4()
    runtime_uid = uuid4()
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "created",
            "_runtime_incarnation": str(runtime_uid),
        }
    }
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            if owner_kind == "job":
                await conn.execute(
                    "INSERT INTO jobs (id, description, status, context) "
                    "VALUES ($1, 'old writer', 'paused', $2::jsonb)",
                    owner_id,
                    json.dumps(state),
                )
            else:
                await conn.execute(
                    "INSERT INTO threads (id, status, execution_lane, metadata) "
                    "VALUES ($1, 'active', 'stateless', $2::jsonb)",
                    owner_id,
                    json.dumps(state),
                )
    assert exc_info.value.constraint_name == (
        "managed_repository_workspace_creation_reservation_required"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_projection", (False, True))
async def test_0238_legacy_job_activity_is_metadata(db, remove_projection):
    job_id = uuid4()
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'legacy heartbeat', 'completed', $2::jsonb)",
            job_id,
            json.dumps(
                {"workspace_container": {"last_activity": "2026-09-10T00:00:00Z"}}
            ),
        )
        if remove_projection:
            assert (
                await conn.execute(
                    "UPDATE jobs SET context = context - 'workspace_container' WHERE id = $1",
                    job_id,
                )
                == "UPDATE 1"
            )
        assert (
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id) == "DELETE 1"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projection",
    (
        {"last_activity": "2026-09-10T00:00:00Z", "pod_name": "legacy-pod"},
        {"last_activity": "2026-09-10T00:00:00Z", "unknown": True},
        {"last_activity": 123},
        {"last_activity": None},
    ),
)
async def test_0238_activity_does_not_hide_unknown_or_runtime_authority(db, projection):
    job_id = uuid4()
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'legacy authority', 'completed', $2::jsonb)",
            job_id,
            json.dumps({"workspace_container": projection}),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context = context - 'workspace_container' WHERE id = $1",
                job_id,
            )


@pytest.mark.asyncio
async def test_0238_job_heartbeat_exception_does_not_apply_to_threads(db):
    thread_id = uuid4()
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(
                {"workspace_container": {"last_activity": "2026-09-10T00:00:00Z"}}
            ),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("DELETE FROM threads WHERE id = $1", thread_id)


@pytest.mark.asyncio
async def test_0238_activity_cannot_hide_pending_creation_authority(db):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'pending workspace', 'paused')",
            job_id,
        )
    assert await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="activity-regression",
        desired_manifest_digest="0" * 64,
    )
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(
                {"workspace_container": {"last_activity": "2026-09-10T00:00:00Z"}}
            ),
        )
        with pytest.raises(asyncpg.CheckViolationError) as refused:
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)
    assert refused.value.constraint_name == (
        "managed_repository_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
async def test_activity_still_updates_an_authoritative_workspace(db):
    job_id, _, _, state = await _create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    assert await db.merge_workspace_container_context(
        str(job_id),
        {"last_activity": "2026-09-10T00:00:00Z"},
        existing_only=True,
    )
    async with db.acquire() as conn:
        workspace = await conn.fetchval(
            "SELECT context->'workspace_container' FROM jobs WHERE id = $1", job_id
        )
    if isinstance(workspace, str):
        workspace = json.loads(workspace)
    assert workspace == {
        **state["workspace_container"],
        "last_activity": "2026-09-10T00:00:00Z",
    }


async def _create_settled_authoritative_runtime(
    db: PostgresDB, *, owner_kind: str, scope: str, settle: bool = True
) -> tuple[UUID, str, dict, dict]:
    owner_id = uuid4()
    runtime_uid = str(uuid4())
    if owner_kind == "job":
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, description, status) "
                "VALUES ($1, 'authoritative runtime', 'paused')",
                owner_id,
            )
    else:
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO threads (id, status, execution_lane) "
                "VALUES ($1, 'active', 'stateless')",
                owner_id,
            )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        claimant="authority-envelope-creator",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="authority-envelope-creator",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="authority-envelope-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=runtime_uid,
    )
    runtime = {
        "status": "active" if scope == "ide" else "ready",
        "_runtime_incarnation": runtime_uid,
        "_creation_reservation_id": str(reservation["id"]),
        "_creation_claim_token": str(reservation["claim_token"]),
        "pod_name": f"runtime-{str(owner_id)[:12]}",
        "namespace": "agent-workspaces",
        "pod_ip": "10.42.0.31",
        "host": "10.42.0.31",
        "port": 30022,
        "_canvas_workspace_generation": str(uuid4()),
        "host_key_fingerprint": "SHA256:authoritative-runtime",
    }
    state_key = "ide_session" if scope == "ide" else "workspace_container"
    if scope == "ide":
        runtime.update(
            {
                "restore_type": "k8s_container",
                "code_server_url": "http://10.42.0.31:8080",
            }
        )
    else:
        runtime["provisioner"] = "k8s"
    state = {state_key: runtime}
    if owner_kind == "thread":
        state["_workspace_binding"] = {
            "generation": str(uuid4()),
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:authoritative-runtime",
            "ssh_host_key_fingerprint": "SHA256:authoritative-binding",
        }
    table = "jobs" if owner_kind == "job" else "threads"
    column = "context" if owner_kind == "job" else "metadata"
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {column} = $2::jsonb WHERE id = $1",
            owner_id,
            json.dumps(state),
        )
    if settle:
        assert await db.settle_managed_repository_workspace_creation_reservation(
            str(owner_id),
            owner_kind=owner_kind,
            scope=scope,
            reservation_generation=int(reservation["reservation_generation"]),
            claimant="authority-envelope-creator",
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=runtime_uid,
        )
    return owner_id, runtime_uid, reservation, state


async def _create_inflight_authoritative_runtime(
    db: PostgresDB, *, owner_kind: str, scope: str
) -> tuple[UUID, str, dict, dict]:
    """Publish an exact runtime while its creation reservation is unsettled.

    This is the real window between ``authorize_..._runtime`` and
    ``settle_..._reservation``: the owner already carries a live UID-bearing
    Kubernetes projection, and the creator still holds the only authority that
    can reconcile it.
    """

    return await _create_settled_authoritative_runtime(
        db, owner_kind=owner_kind, scope=scope, settle=False
    )


@pytest.mark.asyncio
async def test_0195_same_runtime_old_writer_cannot_mutate_workspace_authority(db):
    (
        job_id,
        _runtime_uid,
        _reservation,
        state,
    ) = await _create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    candidates: list[dict] = []
    for key, value in (
        ("_creation_reservation_id", None),
        ("_creation_reservation_id", str(uuid4())),
        ("_creation_claim_token", None),
        ("_creation_claim_token", "999999"),
        ("status", "failed"),
        ("pod_name", "workspace-forged"),
        ("namespace", "foreign-namespace"),
        ("pod_ip", "10.42.99.99"),
        ("host", "foreign.internal"),
        ("port", 2222),
        ("_canvas_workspace_generation", str(uuid4())),
        ("host_key_fingerprint", "SHA256:forged"),
    ):
        candidate = json.loads(json.dumps(state))
        if value is None:
            candidate["workspace_container"].pop(key)
        else:
            candidate["workspace_container"][key] = value
        candidates.append(candidate)
    rolling_old = json.loads(json.dumps(state))
    rolling_old["workspace_container"].pop("_creation_reservation_id")
    rolling_old["workspace_container"].pop("_creation_claim_token")
    rolling_old["workspace_container"]["pod_ip"] = "10.42.99.98"
    candidates.append(rolling_old)

    async with db.acquire() as conn:
        for candidate in candidates:
            with pytest.raises(asyncpg.CheckViolationError) as exc_info:
                await conn.execute(
                    "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
                    job_id,
                    json.dumps(candidate),
                )
            assert exc_info.value.constraint_name == (
                "managed_repository_workspace_authority_envelope_immutable"
            )

        heartbeat = json.loads(json.dumps(state))
        heartbeat["workspace_container"]["last_heartbeat_at"] = (
            "2026-08-27T06:00:00+00:00"
        )
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb, status = 'processing' WHERE id = $1",
            job_id,
            json.dumps(heartbeat),
        )


@pytest.mark.asyncio
async def test_0195_same_runtime_old_writer_cannot_substitute_thread_binding(db):
    (
        thread_id,
        _runtime_uid,
        _reservation,
        state,
    ) = await _create_settled_authoritative_runtime(
        db, owner_kind="thread", scope="workspace_container"
    )
    for key, value in (
        ("generation", str(uuid4())),
        ("backing_id", "k8s-pvc:foreign:other"),
        ("ssh_host_key_fingerprint", "SHA256:foreign"),
    ):
        candidate = json.loads(json.dumps(state))
        candidate["_workspace_binding"][key] = value
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError) as exc_info:
                await conn.execute(
                    "UPDATE threads SET metadata = $2::jsonb WHERE id = $1",
                    thread_id,
                    json.dumps(candidate),
                )
        assert exc_info.value.constraint_name == (
            "managed_repository_workspace_authority_envelope_immutable"
        )


@pytest.mark.asyncio
async def test_0195_same_runtime_old_writer_cannot_substitute_ide_endpoint(db):
    (
        job_id,
        _runtime_uid,
        _reservation,
        state,
    ) = await _create_settled_authoritative_runtime(db, owner_kind="job", scope="ide")
    for key, value in (
        ("code_server_url", "http://foreign.internal:8080"),
        ("restore_type", "container"),
        ("pod_ip", "10.42.99.97"),
    ):
        candidate = json.loads(json.dumps(state))
        candidate["ide_session"][key] = value
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError) as exc_info:
                await conn.execute(
                    "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
                    job_id,
                    json.dumps(candidate),
                )
        assert exc_info.value.constraint_name == (
            "managed_repository_ide_authority_envelope_immutable"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("old_status", ("failed", "deleted", "retiring_process_zero"))
@pytest.mark.parametrize("with_provisioner", (True, False))
async def test_0195_uidless_terminal_old_writer_cannot_rearm_then_reserve(
    db, old_status, with_provisioner
):
    job_id = uuid4()
    runtime = {
        "status": old_status,
        "pod_name": f"workspace-{str(job_id)[:12]}",
        **({"provisioner": "k8s"} if with_provisioner else {}),
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'uidless old writer', 'paused', $2::jsonb)",
            job_id,
            json.dumps({"workspace_container": runtime}),
        )
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, "
                "'{workspace_container,status}', '\"pending\"'::jsonb) "
                "WHERE id = $1",
                job_id,
            )
        assert exc_info.value.constraint_name == (
            "managed_repository_uidless_workspace_runtime_transition_forbidden"
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context = jsonb_build_object("
                "'workspace_container', '{\"status\":\"pending\"}'::jsonb) "
                "WHERE id = $1",
                job_id,
            )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="uidless-old-writer",
            desired_manifest_digest="0" * 64,
        )
        is None
    )


@pytest.mark.asyncio
async def test_0195_genuine_uidless_precreate_progress_remains_compatible(db):
    job_id = uuid4()
    runtime = {
        "provisioner": "k8s",
        "status": "pending",
        "pod_name": f"workspace-{str(job_id)[:12]}",
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'initial uidless create', 'paused', $2::jsonb)",
            job_id,
            json.dumps({"workspace_container": runtime}),
        )
        await conn.execute(
            "UPDATE jobs SET context = jsonb_set(context, "
            "'{workspace_container,status}', '\"creating\"'::jsonb) "
            "WHERE id = $1",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="initial-uidless-create",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    assert await db.abort_managed_repository_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="initial-uidless-create",
        claim_token=int(reservation["claim_token"]),
    )


@pytest.mark.asyncio
async def test_0195_creation_reservation_authorizes_exact_bind_and_settlement(db):
    job_id = uuid4()
    runtime_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'reserved creation', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-a",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-a",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-a",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "created",
            "_runtime_incarnation": str(runtime_uid),
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(state),
        )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-a",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )


@pytest.mark.asyncio
async def test_0195_refuses_reservation_over_unretired_runtime_and_ended_thread(db):
    job_id = uuid4()
    thread_id = uuid4()
    runtime_uid = uuid4()
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "retiring_process_zero",
            "_runtime_incarnation": str(runtime_uid),
        }
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'unretired runtime', 'paused', $2::jsonb)",
            job_id,
            json.dumps(state),
        )
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane) "
            "VALUES ($1, 'ended', 'stateless')",
            thread_id,
        )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="creator-b",
            desired_manifest_digest="0" * 64,
        )
        is None
    )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(thread_id),
            owner_kind="thread",
            scope="workspace_container",
            claimant="creator-b",
            desired_manifest_digest="0" * 64,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runtime_status", ("failed", "deleted", "retiring_process_zero")
)
@pytest.mark.parametrize("with_provenance", (True, False))
async def test_0195_refuses_reservation_over_uidless_historical_runtime_state(
    db,
    runtime_status,
    with_provenance,
):
    job_id = uuid4()
    state = {
        "workspace_container": {
            **({"provisioner": "k8s"} if with_provenance else {}),
            "status": runtime_status,
            "pod_name": f"workspace-{str(job_id)[:12]}",
        }
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'uidless historical runtime', 'paused', $2::jsonb)",
            job_id,
            json.dumps(state),
        )

    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="creator-uidless",
            desired_manifest_digest="0" * 64,
        )
        is None
    )


@pytest.mark.asyncio
async def test_0195_runtime_bound_expired_claim_rotation_updates_owner_atomically(db):
    job_id = uuid4()
    runtime_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'creation handoff', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-before-loss",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-before-loss",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-before-loss",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )
    runtime = {
        "provisioner": "k8s",
        "status": "created",
        "_runtime_incarnation": str(runtime_uid),
        "_creation_reservation_id": str(reservation["id"]),
        "_creation_claim_token": str(reservation["claim_token"]),
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = jsonb_build_object("
            "'workspace_container', $2::jsonb) WHERE id = $1",
            job_id,
            json.dumps(runtime),
        )
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '1 hour', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            reservation["id"],
        )

    reclaimed = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-after-loss",
        desired_manifest_digest="0" * 64,
    )
    assert reclaimed is not None
    assert reclaimed["id"] == reservation["id"]
    assert reclaimed["runtime_incarnation"] == runtime_uid
    assert int(reclaimed["claim_token"]) != int(reservation["claim_token"])

    async with db.acquire() as conn:
        projected = await conn.fetchval(
            "SELECT context->'workspace_container' FROM jobs WHERE id = $1",
            job_id,
        )
    if isinstance(projected, str):
        projected = json.loads(projected)
    assert projected["_creation_reservation_id"] == str(reservation["id"])
    assert projected["_creation_claim_token"] == str(reclaimed["claim_token"])
    assert projected["_runtime_incarnation"] == str(runtime_uid)

    # A committed-but-lost reclaim response replays the same generation and
    # token instead of wedging on the predecessor token in owner context.
    replay = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-after-loss",
        desired_manifest_digest="0" * 64,
    )
    assert replay is not None
    assert replay["id"] == reclaimed["id"]
    assert replay["claim_token"] == reclaimed["claim_token"]
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(replay["reservation_generation"]),
        claimant="creator-after-loss",
        claim_token=int(replay["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement_kind", ("restore", "reattach", "adopt"))
async def test_0195_expired_creation_cannot_change_operation_kind(db, replacement_kind):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'creation operation fence', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="original-create",
        operation_kind="create",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '1 hour', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            reservation["id"],
        )

    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant=f"replacement-{replacement_kind}",
            operation_kind=replacement_kind,
            desired_manifest_digest="0" * 64,
        )
        is None
    )
    replay = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="replacement-create",
        operation_kind="create",
        desired_manifest_digest="0" * 64,
    )
    assert replay is not None
    assert replay["id"] == reservation["id"]
    assert replay["operation_kind"] == "create"


@pytest.mark.asyncio
async def test_0195_cancelled_pre_pod_creation_settles_without_shared_reclaim(db):
    job_id = uuid4()
    seed_uid = uuid4()
    pvc_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'cancelled partial create', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-c",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-c",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    for kind, uid in (("pvc", pvc_uid), ("seed", seed_uid)):
        assert await db.record_managed_repository_workspace_creation_resource(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            reservation_generation=int(reservation["reservation_generation"]),
            claimant="creator-c",
            claim_token=int(reservation["claim_token"]),
            resource_kind=kind,
            resource_uid=str(uid),
        )
    cancelled = await db.request_managed_repository_workspace_creation_cancellation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="suspended",
        reclaim_shared_resources=False,
        claimant="cleanup-c",
    )
    assert cancelled is not None
    assert cancelled["cancel_resource_policy"] == "preserve"
    assert cancelled["pvc_uid"] == pvc_uid
    assert await db.settle_cancelled_partial_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(cancelled["reservation_generation"]),
        claimant="cleanup-c",
        claim_token=int(cancelled["claim_token"]),
    )
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT context->'workspace_container' AS workspace, "
            "phase, result_kind, cancel_cleanup_completed_at "
            "FROM jobs CROSS JOIN "
            "managed_repository_workspace_creation_reservations "
            "WHERE jobs.id = $1 AND owner_id = $1",
            job_id,
        )
    workspace = row["workspace"]
    if isinstance(workspace, str):
        workspace = json.loads(workspace)
    assert workspace["status"] == "suspended"
    assert row["phase"] == "aborted"
    assert row["result_kind"] == "aborted"
    assert row["cancel_cleanup_completed_at"] is not None
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT cancel_projection_transaction_id IS NOT NULL "
            "FROM managed_repository_workspace_creation_reservations "
            "WHERE id = $1",
            reservation["id"],
        )


@pytest.mark.asyncio
async def test_0195_active_creation_blocks_raw_owner_delete_until_abort(db):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'delete fence', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="creator-d",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)
    assert exc_info.value.constraint_name == (
        "managed_repository_workspace_cleanup_required_before_owner_delete"
    )
    assert await db.abort_managed_repository_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="creator-d",
        claim_token=int(reservation["claim_token"]),
    )
    async with db.acquire() as conn:
        assert (
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id) == "DELETE 1"
        )


@pytest.mark.asyncio
async def test_workspace_settlement_is_idempotent_but_rejects_successor(db):
    thread_id = uuid4()
    retired_uid = str(uuid4())
    successor_uid = str(uuid4())
    initial = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "retiring_process_zero",
            "_runtime_incarnation": retired_uid,
            "pod_ip": "10.42.0.90",
        }
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(initial),
        )

    assert await db.record_managed_repository_workspace_process_zero(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=retired_uid,
    )
    assert await db.prepare_managed_repository_workspace_cleanup_intent(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=retired_uid,
        target_disposition="deleted",
        reclaim_shared_resources=False,
        pod_uid=retired_uid,
        resources_captured=True,
    )
    assert await db.settle_managed_repository_workspace_after_process_zero(
        str(thread_id),
        owner_kind="thread",
        runtime_incarnation=retired_uid,
    )
    # A response lost after the first commit can replay against the absorbing
    # cleared state and finish named-resource cleanup.
    assert await db.settle_managed_repository_workspace_after_process_zero(
        str(thread_id),
        owner_kind="thread",
        runtime_incarnation=retired_uid,
    )

    successor = {
        "provisioner": "k8s",
        "status": "created",
        "_runtime_incarnation": successor_uid,
        "pod_ip": "10.42.0.91",
    }
    async with db.acquire() as conn:
        # This models a successor published before 0197 installed its raw
        # writer fence.  New writers must use a reservation and an ended
        # thread cannot reserve one, but the old cleanup replay still needs to
        # reject a genuine historical successor without mutating it.
        await _execute_pre_0195(
            conn,
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{workspace_container}', $2::jsonb) WHERE id = $1",
            thread_id,
            json.dumps(successor),
        )

    assert not await db.settle_managed_repository_workspace_after_process_zero(
        str(thread_id),
        owner_kind="thread",
        runtime_incarnation=retired_uid,
    )
    async with db.acquire() as conn:
        observed = await conn.fetchval(
            "SELECT metadata->'workspace_container' FROM threads WHERE id = $1",
            thread_id,
        )
    if isinstance(observed, str):
        observed = json.loads(observed)
    assert observed == successor


async def _create_settled_restore_generation(
    db: PostgresDB,
    *,
    owner_kind: str,
    scope: str,
    runtime_updates: dict | None = None,
    state_updates: dict | None = None,
) -> tuple[str, str, dict]:
    owner_id = uuid4()
    runtime_uid = uuid4()
    async with db.acquire() as conn:
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs (id, description, status) "
                "VALUES ($1, 'restore work lease', 'paused')",
                owner_id,
            )
        else:
            await conn.execute(
                "INSERT INTO threads (id, status, execution_lane) "
                "VALUES ($1, 'active', 'stateless')",
                owner_id,
            )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        claimant="restore-creator",
        operation_kind="restore",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="restore-creator",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="restore-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )
    runtime = {
        "status": "restoring",
        "_runtime_incarnation": str(runtime_uid),
        "_creation_reservation_id": str(reservation["id"]),
        "_creation_claim_token": str(reservation["claim_token"]),
    }
    if scope == "ide":
        runtime["restore_type"] = "k8s_container"
        state_key = "ide_session"
    else:
        runtime["provisioner"] = "k8s"
        runtime["_snapshot_restore_required"] = True
        state_key = "workspace_container"
    runtime.update(runtime_updates or {})
    table = "jobs" if owner_kind == "job" else "threads"
    column = "context" if owner_kind == "job" else "metadata"
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {column} = jsonb_build_object($2::text, "
            "$3::jsonb) || $4::jsonb WHERE id = $1",
            owner_id,
            state_key,
            json.dumps(runtime),
            json.dumps(state_updates or {}),
        )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(owner_id),
        owner_kind=owner_kind,
        scope=scope,
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="restore-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )
    return str(owner_id), str(runtime_uid), reservation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_kind", "scope", "success_kind"),
    (
        ("job", "workspace_container", "ready"),
        ("thread", "workspace_container", "ready"),
        ("job", "ide", "active"),
    ),
)
async def test_0195_restore_work_lease_reclaims_and_settles_exact_current_runtime(
    db, owner_kind, scope, success_kind
):
    owner_id, runtime_uid, reservation = await _create_settled_restore_generation(
        db, owner_kind=owner_kind, scope=scope
    )
    first = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        claimant="restore-worker-a",
    )
    assert first is not None
    replay = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        claimant="restore-worker-a",
    )
    assert replay is not None
    assert replay["restore_work_claim_token"] == first["restore_work_claim_token"]
    assert (
        await db.claim_current_managed_repository_workspace_restore_work(
            owner_id,
            owner_kind=owner_kind,
            scope=scope,
            claimant="restore-worker-b",
        )
        is None
    )
    renewed = await db.renew_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        reservation_id=str(first["id"]),
        runtime_incarnation=runtime_uid,
        claimant="restore-worker-a",
        work_claim_token=int(first["restore_work_claim_token"]),
    )
    assert renewed is not None

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET restore_work_claim_expires_at = now() - interval '1 second' "
            "WHERE id = $1",
            reservation["id"],
        )
    second = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        claimant="restore-worker-b",
    )
    assert second is not None
    assert int(second["restore_work_claim_token"]) != int(
        first["restore_work_claim_token"]
    )
    complete_kwargs = (
        {
            "code_server_url": "http://ide.internal:8080",
            "last_activity": "2026-08-27T05:00:00+00:00",
        }
        if scope == "ide"
        else {}
    )
    assert not await db.complete_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        reservation_id=str(first["id"]),
        runtime_incarnation=runtime_uid,
        claimant="restore-worker-a",
        work_claim_token=int(first["restore_work_claim_token"]),
        result_kind=success_kind,
        **complete_kwargs,
    )
    assert await db.complete_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        reservation_id=str(second["id"]),
        runtime_incarnation=runtime_uid,
        claimant="restore-worker-b",
        work_claim_token=int(second["restore_work_claim_token"]),
        result_kind=success_kind,
        **complete_kwargs,
    )
    table = "jobs" if owner_kind == "job" else "threads"
    column = "context" if owner_kind == "job" else "metadata"
    state_key = "ide_session" if scope == "ide" else "workspace_container"
    endpoint_key = "code_server_url" if scope == "ide" else "pod_ip"
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT restore_work_projection_transaction_id IS NOT NULL "
            "FROM managed_repository_workspace_creation_reservations "
            "WHERE id = $1",
            reservation["id"],
        )
        # A committed projection marker is deliberately transaction-scoped.
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                f"UPDATE {table} SET {column} = jsonb_set({column}, "
                f"'{{{state_key},{endpoint_key}}}', to_jsonb($2::text)) "
                "WHERE id = $1",
                UUID(owner_id),
                "http://forged.internal" if scope == "ide" else "10.42.99.99",
            )
    # Lost successful response is an exact idempotent replay.
    assert await db.complete_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind=owner_kind,
        scope=scope,
        reservation_id=str(second["id"]),
        runtime_incarnation=runtime_uid,
        claimant="restore-worker-b",
        work_claim_token=int(second["restore_work_claim_token"]),
        result_kind=success_kind,
        **complete_kwargs,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_kind", "terminal_status"),
    (("job", "failed"), ("thread", "ended")),
)
async def test_0195_terminal_status_atomically_cancels_effectful_creation(
    db, owner_kind, terminal_status
):
    owner_id = uuid4()
    async with db.acquire() as conn:
        if owner_kind == "job":
            await conn.execute(
                "INSERT INTO jobs (id, description, status) "
                "VALUES ($1, 'terminal creation race', 'paused')",
                owner_id,
            )
        else:
            await conn.execute(
                "INSERT INTO threads (id, status, execution_lane) "
                "VALUES ($1, 'active', 'stateless')",
                owner_id,
            )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        claimant="terminal-race-creator",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    started = await db.mark_managed_repository_workspace_creation_started(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="terminal-race-creator",
        claim_token=int(reservation["claim_token"]),
    )
    assert started is not None
    runtime_uid = str(uuid4())
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="terminal-race-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=runtime_uid,
    )
    table = "jobs" if owner_kind == "job" else "threads"
    column = "context" if owner_kind == "job" else "metadata"
    runtime_state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "created",
            "_runtime_incarnation": runtime_uid,
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET {column} = $2::jsonb WHERE id = $1",
            owner_id,
            json.dumps(runtime_state),
        )
    if owner_kind == "job":
        suspended = await db.request_managed_repository_workspace_creation_cancellation(
            str(owner_id),
            owner_kind="job",
            scope="workspace_container",
            target_disposition="suspended",
            reclaim_shared_resources=False,
            claimant="suspension-before-terminal",
        )
        assert suspended is not None
        assert suspended["cancel_target_disposition"] == "suspended"
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET status = $2 WHERE id = $1",
            owner_id,
            terminal_status,
        )
        cancelled = await conn.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations "
            "WHERE id = $1",
            reservation["id"],
        )
        projected = await conn.fetchval(
            f"SELECT {column}->'workspace_container' FROM {table} WHERE id = $1",
            owner_id,
        )
        cleanup_intent = await conn.fetchrow(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind = $1 AND owner_id = $2 "
            "AND scope = 'workspace_container' AND settled_at IS NULL",
            owner_kind,
            owner_id,
        )
    if isinstance(projected, str):
        projected = json.loads(projected)
    assert cancelled["cancel_requested_at"] is not None
    assert cancelled["cancel_claim_projection_transaction_id"] is not None
    assert cancelled["claimed_by"] == "terminal-owner-transition"
    assert int(cancelled["claim_token"]) != int(reservation["claim_token"])
    assert cancelled["cancel_resource_policy"] == (
        "terminal_reclaim" if owner_kind == "job" else "preserve"
    )
    assert cancelled["cancel_target_disposition"] == "deleted"
    assert cleanup_intent is not None
    assert cleanup_intent["terminal_admission_transaction_id"] is not None
    assert projected["_runtime_incarnation"] == runtime_uid
    assert projected["_creation_reservation_id"] == str(reservation["id"])
    assert projected["_creation_claim_token"] == str(cancelled["claim_token"])
    async with db.acquire() as conn:
        # The terminal trigger's exact token rotation was valid only in its
        # own transaction; the cancelled creation no longer authorizes an old
        # writer to mutate that same runtime envelope.
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute(
                f"UPDATE {table} SET {column} = jsonb_set({column}, "
                "'{workspace_container,pod_ip}', to_jsonb($2::text)) "
                "WHERE id = $1",
                owner_id,
                "10.42.99.96",
            )
    assert exc_info.value.constraint_name == (
        "managed_repository_workspace_authority_envelope_immutable"
    )
    assert not await db.managed_repository_workspace_creation_claim_is_current(
        str(owner_id),
        owner_kind=owner_kind,
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="terminal-race-creator",
        claim_token=int(reservation["claim_token"]),
    )


@pytest.mark.asyncio
async def test_0195_terminal_owner_fences_active_restore_work(db):
    owner_id, runtime_uid, reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    claimed = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        claimant="restore-before-terminal",
    )
    assert claimed is not None
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'failed' WHERE id = $1", UUID(owner_id)
        )
    assert (
        await db.renew_managed_repository_workspace_restore_work(
            owner_id,
            owner_kind="job",
            scope="workspace_container",
            reservation_id=str(reservation["id"]),
            runtime_incarnation=runtime_uid,
            claimant="restore-before-terminal",
            work_claim_token=int(claimed["restore_work_claim_token"]),
        )
        is None
    )
    assert (
        await db.claim_current_managed_repository_workspace_restore_work(
            owner_id,
            owner_kind="job",
            scope="workspace_container",
            claimant="restore-after-terminal",
        )
        is None
    )


@pytest.mark.asyncio
async def test_0195_cleanup_fences_active_restore_work_lease(db):
    owner_id, runtime_uid, reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    claimed = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        claimant="restore-before-suspend",
    )
    assert claimed is not None
    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=runtime_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        snapshot_restore_required=True,
    )
    assert intent is not None
    assert (
        await db.renew_managed_repository_workspace_restore_work(
            owner_id,
            owner_kind="job",
            scope="workspace_container",
            reservation_id=str(reservation["id"]),
            runtime_incarnation=runtime_uid,
            claimant="restore-before-suspend",
            work_claim_token=int(claimed["restore_work_claim_token"]),
        )
        is None
    )
    assert (
        await db.claim_current_managed_repository_workspace_restore_work(
            owner_id,
            owner_kind="job",
            scope="workspace_container",
            claimant="restore-after-suspend",
        )
        is None
    )


@pytest.mark.asyncio
async def test_0195_strict_thread_restore_work_settles_full_authority_tuple(db):
    workspace_generation = str(uuid4())
    endpoint_generation = str(uuid4())
    backing_id = "k8s-pvc:agent-workspaces:strict-restore"
    fingerprint = "SHA256:strictrestoreauthority"
    owner_id, runtime_uid, reservation = await _create_settled_restore_generation(
        db,
        owner_kind="thread",
        scope="workspace_container",
        runtime_updates={
            "_canvas_workspace_generation": endpoint_generation,
            "pod_ip": "10.42.0.19",
            "port": 30022,
        },
        state_updates={
            "_workspace_binding": {
                "generation": workspace_generation,
                "kind": "remote",
                "backing_id": backing_id,
                "ssh_host_key_fingerprint": fingerprint,
            }
        },
    )
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE threads SET execution_lane = 'stateless' WHERE id = $1",
            UUID(owner_id),
        )
    claimed = await db.claim_current_managed_repository_workspace_restore_work(
        owner_id,
        owner_kind="thread",
        scope="workspace_container",
        claimant="strict-restore-worker",
    )
    assert claimed is not None
    async with db.acquire() as conn:
        debug_row = await conn.fetchrow(
            "SELECT metadata, status::text AS status FROM threads WHERE id = $1",
            UUID(owner_id),
        )
    debug_metadata = debug_row["metadata"]
    if isinstance(debug_metadata, str):
        debug_metadata = json.loads(debug_metadata)
    assert debug_row["status"] == "active"
    assert debug_metadata["workspace_container"]["status"] == "restoring"
    assert (
        debug_metadata["workspace_container"]["_canvas_workspace_generation"]
        == endpoint_generation
    )
    assert debug_metadata["_workspace_binding"]["generation"] == (workspace_generation)
    debug_workspace = debug_metadata["workspace_container"]
    assert debug_workspace["_snapshot_restore_required"] is True
    assert debug_workspace["_runtime_incarnation"] == runtime_uid
    assert debug_workspace["_creation_reservation_id"] == str(reservation["id"])
    assert debug_workspace["_creation_claim_token"] == str(reservation["claim_token"])
    assert debug_metadata["_workspace_binding"]["backing_id"] == backing_id
    assert (
        debug_metadata["_workspace_binding"]["ssh_host_key_fingerprint"] == fingerprint
    )
    assert claimed["operation_kind"] == "restore"
    assert claimed["result_kind"] == "settled"
    assert claimed["restore_work_claimed_by"] == "strict-restore-worker"
    kwargs = {
        "reservation_id": str(reservation["id"]),
        "runtime_incarnation": runtime_uid,
        "claimant": "strict-restore-worker",
        "work_claim_token": int(claimed["restore_work_claim_token"]),
        "workspace_generation": workspace_generation,
        "endpoint_generation": endpoint_generation,
        "backing_id": backing_id,
        "host_key_fingerprint": fingerprint,
        "pod_ip": "10.42.0.19",
        "port": 30022,
        "expected_workspace_status": "restoring",
    }
    assert not await db.complete_stateless_thread_workspace_restore_work(
        owner_id, **{**kwargs, "endpoint_generation": str(uuid4())}
    )
    assert await db.complete_stateless_thread_workspace_restore_work(owner_id, **kwargs)
    assert await db.complete_stateless_thread_workspace_restore_work(owner_id, **kwargs)


@pytest.mark.asyncio
async def test_0195_soft_settled_thread_promotes_to_exact_terminal_reclaim(db):
    thread_id = uuid4()
    runtime_uid = uuid4()
    metadata = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "deleted",
            "_runtime_incarnation": None,
            "_snapshot_restore_required": True,
        },
        "_stateless_workspace_retirement_settled": {
            "terminal_token": 8,
            "cleanup_complete": True,
            "permanent": True,
            "backing_id": "k8s-pvc:agent-workspaces:thread-workspace",
            "runtime_incarnation": str(runtime_uid),
            "snapshot_restore_required": True,
            "workspace_absence_proven": True,
        },
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(metadata),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', 8)",
            thread_id,
        )
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts "
            "(owner_kind, owner_id, scope, provisioner, runtime_incarnation) "
            "VALUES ('thread', $1, 'workspace_container', 'k8s', $2)",
            thread_id,
            str(runtime_uid),
        )
        thread_generation = await conn.fetchval(
            "SELECT runtime_generation FROM threads WHERE id = $1", thread_id
        )
        prior = await conn.fetchrow(
            "INSERT INTO managed_repository_workspace_cleanup_intents ("
            "owner_kind, owner_id, thread_runtime_generation, scope, "
            "runtime_incarnation, intent_source, "
            "target_disposition, resource_policy, reclaim_shared_resources, "
            "lifecycle_fingerprint, pod_uid, capture_complete, "
            "resources_captured_at, phase, cleanup_completed_at, settled_at, "
            "result_kind, projection_transaction_id) VALUES ("
            "'thread', $1, $3, 'workspace_container', $2, "
            "'current', 'deleted', 'preserve', FALSE, '{}'::jsonb, $2, TRUE, "
            "now(), 'settled', now(), now(), 'settled', txid_current()) "
            "RETURNING *",
            thread_id,
            runtime_uid,
            thread_generation,
        )

    promoted = await db.prepare_managed_repository_workspace_cleanup_intent(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=str(runtime_uid),
        target_disposition="deleted",
        reclaim_shared_resources=True,
    )
    assert promoted is not None
    assert promoted["id"] != prior["id"]
    assert promoted["resource_policy"] == "terminal_reclaim"
    assert promoted["terminal_queue_token"] == 8
    async with db.acquire() as conn:
        observed = await conn.fetchval(
            "SELECT metadata->'workspace_container' FROM threads WHERE id = $1",
            thread_id,
        )
    if isinstance(observed, str):
        observed = json.loads(observed)
    assert observed["status"] == "deleted"
    assert observed["_runtime_incarnation"] is None


@pytest.mark.asyncio
async def test_settled_none_workspace_outcome_upgrades_and_deletes(db):
    """A backend=none outcome has no provisioner authority to retire."""

    thread_id = uuid4()
    metadata = {
        "config_override": {"workspace": {"backend": "none"}},
        "workspace_container": {"volume_reclaimed": False},
        "_stateless_workspace_retirement_settled": {
            "terminal_token": 3,
            "cleanup_complete": True,
            "permanent": False,
            "backing_id": None,
            "runtime_incarnation": None,
            "snapshot_restore_required": False,
            "workspace_absence_proven": False,
        },
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(metadata),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', 3)",
            thread_id,
        )

    result = await db.begin_stateless_thread_workspace_retirement(
        str(thread_id), force=True, permanent=True
    )
    assert result["state"] == "settled"
    assert result["permanent"] is True

    await db.delete_thread(str(thread_id))
    assert await db.get_thread(str(thread_id)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workspace_patch", "settled_patch", "backend"),
    [
        ({"pod_name": "same-name-successor"}, {}, "none"),
        ({"volume_reclaimed": "false"}, {}, "none"),
        ({}, {"backing_id": "k8s-pvc:workspaces:claim"}, "none"),
        ({}, {}, "sandbox"),
    ],
)
async def test_settled_none_workspace_outcome_classifier_fails_closed(
    db, workspace_patch, settled_patch, backend
):
    thread_id = uuid4()
    workspace = {"volume_reclaimed": False, **workspace_patch}
    settled = {
        "terminal_token": 3,
        "cleanup_complete": True,
        "permanent": True,
        "backing_id": None,
        "runtime_incarnation": None,
        "snapshot_restore_required": False,
        "workspace_absence_proven": False,
        **settled_patch,
    }
    metadata = {
        "config_override": {"workspace": {"backend": backend}},
        "workspace_container": workspace,
        "_stateless_workspace_retirement_settled": settled,
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(metadata),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', 3)",
            thread_id,
        )

    with pytest.raises(asyncpg.CheckViolationError) as exc:
        await db.delete_thread(str(thread_id))
    assert (
        exc.value.constraint_name
        == "managed_repository_legacy_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
async def test_0195_permanent_thread_cleanup_retains_uid_and_repairs_legacy_projection(
    db,
):
    thread_id = uuid4()
    runtime_uid = uuid4()
    generation = uuid4()
    fingerprint = "SHA256:" + ("A" * 43)
    ack = {
        "kind": "protocol",
        "terminal_token": 8,
        "workspace_generation": str(generation),
        "endpoint_generation": str(generation),
        "runtime_incarnation": str(runtime_uid),
        "host_key_fingerprint": fingerprint,
    }
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "_stateless_workspace_retirement_pending": True,
        "_stateless_claim_retirement": {
            "terminal_token": 8,
            "claimant_quiesced": True,
            "shell_retirement_required": True,
            "resident_cleanup_required": True,
            "residents_retired": True,
            "residents_retired_by": "protocol",
            "remote_retired": True,
            "remote_retired_by": "protocol",
            "permanent": True,
            "workspace_absence_proven": False,
            "workspace_generation": str(generation),
            "endpoint_generation": str(generation),
            "runtime_incarnation": str(runtime_uid),
            "host_key_fingerprint": fingerprint,
        },
        "_stateless_resident_retirement_ack": dict(ack),
        "_stateless_shell_retirement_ack": dict(ack),
        "workspace_container": {
            "status": "retiring_process_zero",
            "provisioner": "k8s",
            "pod_name": f"ws-thread-{str(thread_id)[:12]}",
            "namespace": "agent-workspaces",
            "pod_ip": "10.42.0.8",
            "port": 30022,
            "_canvas_workspace_generation": str(generation),
            "_runtime_incarnation": str(runtime_uid),
            "_snapshot_restore_required": False,
        },
        "_workspace_binding": {
            "generation": str(generation),
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": fingerprint,
        },
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO threads (id, status, execution_lane, metadata) "
            "VALUES ($1, 'ended', 'stateless', $2::jsonb)",
            thread_id,
            json.dumps(metadata),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token) "
            "VALUES ($1, 'session_turn', 'done', 8)",
            thread_id,
        )

    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=str(runtime_uid),
        target_disposition="deleted",
        reclaim_shared_resources=True,
        pod_uid=str(runtime_uid),
        pvc_uid=str(uuid4()),
        service_uid=str(uuid4()),
        resources_captured=True,
    )
    assert intent is not None
    claimed = await db.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]), claimant="permanent-thread-cleanup"
    )
    assert claimed is not None
    assert await db.record_managed_repository_workspace_process_zero(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=str(runtime_uid),
    )
    assert await db.settle_managed_repository_workspace_cleanup_intent(
        str(thread_id),
        owner_kind="thread",
        scope="workspace_container",
        runtime_incarnation=str(runtime_uid),
        intent_generation=int(claimed["intent_generation"]),
        claimant=str(claimed["claimed_by"]),
        claim_token=int(claimed["claim_token"]),
    )

    thread = await db.get_thread(str(thread_id))
    assert thread is not None
    stored_metadata = thread["metadata"]
    if isinstance(stored_metadata, str):
        stored_metadata = json.loads(stored_metadata)
    workspace = stored_metadata["workspace_container"]
    assert workspace["status"] == "deleted"
    assert workspace["_runtime_incarnation"] == str(runtime_uid)

    # Recreate the short-lived 0198 shape, then prove the superseding
    # migration restores only the UID backed by the exact settled authorities.
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE threads SET metadata = jsonb_set(jsonb_set(metadata, "
            "'{workspace_container,_runtime_incarnation}', 'null'::jsonb), "
            "'{_stateless_claim_retirement,terminal_token}', '9'::jsonb) "
            "WHERE id = $1",
            thread_id,
        )

    # A deployment migration runs in its own advisory-locked session after
    # pre-existing writers commit; keep the proof faithful to that boundary.
    async with db.acquire() as conn:
        await conn.execute(NON_PINNED_LIFECYCLE_MIGRATIONS[-1].read_text())

    unmatched = await db.get_thread(str(thread_id))
    assert unmatched is not None
    unmatched_metadata = unmatched["metadata"]
    if isinstance(unmatched_metadata, str):
        unmatched_metadata = json.loads(unmatched_metadata)
    assert unmatched_metadata["workspace_container"]["_runtime_incarnation"] is None

    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{_stateless_claim_retirement,terminal_token}', '8'::jsonb) "
            "WHERE id = $1",
            thread_id,
        )
    async with db.acquire() as conn:
        await conn.execute(NON_PINNED_LIFECYCLE_MIGRATIONS[-1].read_text())

    repaired = await db.get_thread(str(thread_id))
    assert repaired is not None
    repaired_metadata = repaired["metadata"]
    if isinstance(repaired_metadata, str):
        repaired_metadata = json.loads(repaired_metadata)
    assert repaired_metadata["workspace_container"]["_runtime_incarnation"] == str(
        runtime_uid
    )
    await db.delete_thread(str(thread_id))
    assert await db.get_thread(str(thread_id)) is None


@pytest.mark.asyncio
async def test_0195_workspace_mutation_guard_serializes_two_database_sessions(db):
    owner_id = str(uuid4())

    async with db.workspace_runtime_mutation_lock(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
    ) as first:
        assert first is True
        async with db.workspace_runtime_mutation_lock(
            owner_id,
            owner_kind="job",
            scope="workspace_container",
            wait=False,
        ) as second:
            assert second is False

    async with db.workspace_runtime_mutation_lock(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        wait=False,
    ) as successor:
        assert successor is True


@pytest.mark.asyncio
async def test_0195_terminal_transition_cannot_rotate_token_during_external_effect(
    db,
):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'guard terminal transition', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="external-effect",
        desired_manifest_digest="0" * 64,
    )
    assert reservation is not None
    started = await db.mark_managed_repository_workspace_creation_started(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="external-effect",
        claim_token=int(reservation["claim_token"]),
    )
    assert started is not None

    async with db.workspace_runtime_mutation_lock(
        str(job_id), owner_kind="job", scope="workspace_container"
    ) as acquired:
        assert acquired
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.SerializationError):
                await conn.execute(
                    "UPDATE jobs SET status = 'failed' WHERE id = $1", job_id
                )
            unchanged = await conn.fetchrow(
                "SELECT j.status::text AS status, r.claim_token, "
                "r.cancel_requested_at FROM jobs j JOIN "
                "managed_repository_workspace_creation_reservations r "
                "ON r.owner_id = j.id WHERE j.id = $1",
                job_id,
            )
        assert unchanged["status"] == "paused"
        assert int(unchanged["claim_token"]) == int(reservation["claim_token"])
        assert unchanged["cancel_requested_at"] is None

    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status = 'failed' WHERE id = $1", job_id)
        cancelled = await conn.fetchrow(
            "SELECT j.status::text AS status, r.claim_token, "
            "r.cancel_requested_at FROM jobs j JOIN "
            "managed_repository_workspace_creation_reservations r "
            "ON r.owner_id = j.id WHERE j.id = $1",
            job_id,
        )
    assert cancelled["status"] == "failed"
    assert int(cancelled["claim_token"]) != int(reservation["claim_token"])
    assert cancelled["cancel_requested_at"] is not None


@pytest.mark.asyncio
async def test_0195_external_effect_ambiguity_blocks_absence_until_observed(db):
    job_id = uuid4()
    pvc_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'external effect ambiguity', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="effect-owner",
        desired_manifest_digest="1" * 64,
    )
    assert reservation is not None
    issued = await db.begin_managed_repository_workspace_creation_effect(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="effect-owner",
        claim_token=int(reservation["claim_token"]),
        resource_kind="pvc",
        ambiguity_seconds=90,
    )
    assert issued is not None
    assert not await db.managed_repository_workspace_creation_effects_are_quiescent(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
    )
    assert await db.record_managed_repository_workspace_creation_resource(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="effect-owner",
        claim_token=int(reservation["claim_token"]),
        resource_kind="pvc",
        resource_uid=str(pvc_uid),
    )
    assert await db.managed_repository_workspace_creation_effects_are_quiescent(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
    )
    async with db.acquire() as conn:
        effect = await conn.fetchval(
            "SELECT external_effects->'pvc' FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            reservation["id"],
        )
    if isinstance(effect, str):
        effect = json.loads(effect)
    assert effect["observed_uid"] == str(pvc_uid)
    assert effect["observed_at"] is not None


@pytest.mark.asyncio
async def test_0195_cancel_reconciliation_observes_issued_effect(db):
    job_id = uuid4()
    seed_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'cancelled effect observation', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="effect-before-cancel",
        desired_manifest_digest="6" * 64,
    )
    assert reservation is not None
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="effect-before-cancel",
        claim_token=int(reservation["claim_token"]),
        resource_kind="seed",
    )
    cancelled = await db.request_managed_repository_workspace_creation_cancellation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="suspended",
        reclaim_shared_resources=False,
        claimant="effect-after-cancel",
    )
    assert cancelled is not None
    assert await db.record_cancelled_workspace_creation_resource_for_reconciliation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(cancelled["reservation_generation"]),
        claimant="effect-after-cancel",
        claim_token=int(cancelled["claim_token"]),
        resource_kind="seed",
        resource_uid=str(seed_uid),
    )
    async with db.acquire() as conn:
        effect = await conn.fetchval(
            "SELECT external_effects->'seed' FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            reservation["id"],
        )
    if isinstance(effect, str):
        effect = json.loads(effect)
    assert effect["observed_uid"] == str(seed_uid)


@pytest.mark.asyncio
async def test_0195_default_dark_db_boundary_is_non_mutating_but_replays(db):
    job_id = uuid4()
    runtime_uid = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'dark cleanup admission', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="dark-creator",
        desired_manifest_digest="2" * 64,
    )
    assert reservation is not None
    reservation = await db.mark_managed_repository_workspace_creation_started(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="dark-creator",
        claim_token=int(reservation["claim_token"]),
    )
    assert reservation is not None
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="dark-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=str(runtime_uid),
    )
    projected = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "created",
            "_runtime_incarnation": str(runtime_uid),
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(projected),
        )

    assert (
        await db.prepare_managed_repository_workspace_cleanup_intent(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            runtime_incarnation=str(runtime_uid),
            target_disposition="deleted",
            reclaim_shared_resources=False,
            admission_source="automatic",
            automatic_admission_enabled=False,
        )
        is None
    )
    async with db.acquire() as conn:
        unchanged = await conn.fetchrow(
            "SELECT cancel_requested_at, claimed_by, claim_token FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            reservation["id"],
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind = 'job' AND owner_id = $1)",
            job_id,
        )
    assert unchanged["cancel_requested_at"] is None
    assert unchanged["claimed_by"] == "dark-creator"
    assert int(unchanged["claim_token"]) == int(reservation["claim_token"])

    # An explicit supported operation may commit the generation. Once it is
    # durable, the dark automatic caller may replay but not replace it.
    explicit = await db.prepare_managed_repository_workspace_cleanup_intent(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=str(runtime_uid),
        target_disposition="deleted",
        reclaim_shared_resources=False,
        admission_source="explicit",
    )
    # Creation cancellation is the required handoff first; it intentionally
    # does not fabricate a cleanup row while the Pod generation is unsettled.
    assert explicit is None


@pytest.mark.asyncio
async def test_0195_default_dark_replays_exact_committed_cleanup(db):
    owner_id, runtime_uid, _reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    explicit = await db.prepare_managed_repository_workspace_cleanup_intent(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=runtime_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        admission_source="explicit",
    )
    assert explicit is not None
    replay = await db.prepare_managed_repository_workspace_cleanup_intent(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=runtime_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        admission_source="automatic",
        automatic_admission_enabled=False,
    )
    assert replay is not None
    assert replay["id"] == explicit["id"]
    assert replay["admission_source"] == "explicit"


@pytest.mark.asyncio
async def test_0195_cancelled_uidless_generation_loses_projection_authority(db):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'uidless cancellation fence', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="uidless-cancel-owner",
        desired_manifest_digest="5" * 64,
    )
    assert reservation is not None
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": "creating",
            "_creation_reservation_id": str(reservation["id"]),
            "_creation_claim_token": str(reservation["claim_token"]),
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = $2::jsonb WHERE id = $1",
            job_id,
            json.dumps(state),
        )
    cancelled = await db.request_managed_repository_workspace_creation_cancellation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="suspended",
        reclaim_shared_resources=False,
        claimant="uidless-cancel-reconciler",
    )
    assert cancelled is not None
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT managed_repository_workspace_uidless_creation_is_authorized("
            "'job', $1, 'workspace_container', $2, $3)",
            job_id,
            str(reservation["id"]),
            str(cancelled["claim_token"]),
        )


@pytest.mark.asyncio
async def test_0195_manifest_digest_is_frozen_for_active_generation(db):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'manifest freeze', 'paused')",
            job_id,
        )
    reservation = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="manifest-a",
        desired_manifest_digest="3" * 64,
    )
    assert reservation is not None
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 minute' WHERE id = $1",
            reservation["id"],
        )
    assert (
        await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="manifest-b",
            desired_manifest_digest="4" * 64,
        )
        is None
    )
    replay = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        claimant="manifest-b",
        desired_manifest_digest="3" * 64,
    )
    assert replay is not None
    assert replay["id"] == reservation["id"]
    assert replay["desired_manifest_digest"] == "3" * 64


@pytest.mark.asyncio
async def test_0195_terminal_transition_admits_cleanup_for_settled_runtime(db):
    owner_id, runtime_uid, reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'failed' WHERE id = $1", UUID(owner_id)
        )
        intent = await conn.fetchrow(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind = 'job' AND owner_id = $1 "
            "AND scope = 'workspace_container' AND settled_at IS NULL",
            UUID(owner_id),
        )
        runtime = await conn.fetchval(
            "SELECT context->'workspace_container' FROM jobs WHERE id = $1",
            UUID(owner_id),
        )
        creation = await conn.fetchrow(
            "SELECT result_kind, settled_at FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            reservation["id"],
        )
    if isinstance(runtime, str):
        runtime = json.loads(runtime)
    assert intent is not None
    assert str(intent["runtime_incarnation"]) == runtime_uid
    assert intent["admission_source"] == "explicit"
    assert intent["resource_policy"] == "terminal_reclaim"
    assert intent["target_disposition"] == "deleted"
    assert runtime["status"] == "retiring_process_zero"
    assert creation["result_kind"] == "settled"
    assert creation["settled_at"] is not None


@pytest.mark.asyncio
async def test_0195_terminal_transition_promotes_pending_preserve_cleanup(db):
    owner_id, runtime_uid, _reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    preserve = await db.prepare_managed_repository_workspace_cleanup_intent(
        owner_id,
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=runtime_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        admission_source="explicit",
    )
    assert preserve is not None
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = 'failed' WHERE id = $1", UUID(owner_id)
        )
        promoted = await conn.fetchrow(
            "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE id = $1",
            preserve["id"],
        )
    assert promoted["target_disposition"] == "deleted"
    assert promoted["resource_policy"] == "terminal_reclaim"
    assert promoted["reclaim_shared_resources"] is True
    assert promoted["admission_source"] == "explicit"


@pytest.mark.asyncio
@pytest.mark.parametrize("result_kind", ("settled", "superseded"))
async def test_0195_raw_delete_rejects_preserve_only_runtime_cleanup(db, result_kind):
    owner_id, runtime_uid, _reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO managed_repository_workspace_cleanup_intents ("
            "owner_kind, owner_id, scope, runtime_incarnation, intent_source, "
            "target_disposition, resource_policy, reclaim_shared_resources, "
            "lifecycle_fingerprint, pod_uid, capture_complete, "
            "resources_captured_at, phase, cleanup_completed_at, settled_at, "
            "result_kind, projection_transaction_id) VALUES ("
            "'job', $1, 'workspace_container', $2, 'current', 'deleted', "
            "'preserve', FALSE, '{}'::jsonb, $2, TRUE, now(), $3, now(), "
            "now(), $4, CASE WHEN $4 = 'settled' THEN 1 ELSE NULL END)",
            UUID(owner_id),
            UUID(runtime_uid),
            result_kind,
            result_kind,
        )
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute("DELETE FROM jobs WHERE id = $1", UUID(owner_id))
    assert exc_info.value.constraint_name == (
        "managed_repository_terminal_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
async def test_0195_raw_delete_accepts_exact_terminal_reclaim_settlement(db):
    owner_id, runtime_uid, _reservation = await _create_settled_restore_generation(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE jobs SET status = 'failed' WHERE id = $1",
            UUID(owner_id),
        )
        await _execute_pre_0195(
            conn,
            "INSERT INTO managed_repository_workspace_cleanup_intents ("
            "owner_kind, owner_id, scope, runtime_incarnation, intent_source, "
            "target_disposition, resource_policy, reclaim_shared_resources, "
            "lifecycle_fingerprint, pod_uid, capture_complete, "
            "resources_captured_at, phase, cleanup_completed_at, settled_at, "
            "result_kind, projection_transaction_id) VALUES ("
            "'job', $1, 'workspace_container', $2, 'current', 'deleted', "
            "'terminal_reclaim', TRUE, '{}'::jsonb, $2, TRUE, now(), "
            "'settled', now(), now(), 'settled', 1)",
            UUID(owner_id),
            UUID(runtime_uid),
        )
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts ("
            "owner_kind, owner_id, scope, provisioner, runtime_incarnation, "
            "observed_at) VALUES ("
            "'job', $1, 'workspace_container', 'k8s', $2, now())",
            UUID(owner_id),
            runtime_uid,
        )
        projected_runtime = await conn.fetchval(
            "SELECT context->'workspace_container' FROM jobs WHERE id = $1",
            UUID(owner_id),
        )
        if isinstance(projected_runtime, str):
            projected_runtime = json.loads(projected_runtime)
        assert projected_runtime["_runtime_incarnation"] == runtime_uid
        assert (
            await conn.execute("DELETE FROM jobs WHERE id = $1", UUID(owner_id))
            == "DELETE 1"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projected_status", ("created", "failed", "deleted", "expired")
)
async def test_0195_raw_delete_rejects_uidless_legacy_live_projection(
    db, projected_status
):
    job_id = uuid4()
    state = {
        "workspace_container": {
            "provisioner": "k8s",
            "status": projected_status,
            "pod_name": f"workspace-{str(job_id)[:12]}",
        }
    }
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, context) "
            "VALUES ($1, 'legacy live deletion fence', 'paused', $2::jsonb)",
            job_id,
            json.dumps(state),
        )
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)
    assert exc_info.value.constraint_name == (
        "managed_repository_legacy_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ("job", "thread"))
async def test_0196_owner_delete_reports_the_active_creation_reservation(
    db, owner_kind
):
    """An in-flight creation must fail closed as an unsettled reservation.

    ``prevent_workspace_owner_delete_before_cleanup`` is a BEFORE DELETE
    trigger, so ``NEW`` is unassigned and every field reference silently reads
    NULL instead of raising.  Reading the owner identity from ``NEW`` therefore
    degraded the per-scope reservation fence into an always-false predicate and
    mis-attributed a live creation to terminal (or legacy) cleanup authority.
    """

    (
        owner_id,
        runtime_uid,
        reservation,
        _state,
    ) = await _create_inflight_authoritative_runtime(
        db, owner_kind=owner_kind, scope="workspace_container"
    )
    table = "jobs" if owner_kind == "job" else "threads"
    async with db.acquire() as conn:
        active = await conn.fetchrow(
            "SELECT settled_at, runtime_incarnation FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            reservation["id"],
        )
        assert active is not None
        assert active["settled_at"] is None
        assert str(active["runtime_incarnation"]) == runtime_uid
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute(f"DELETE FROM {table} WHERE id = $1", owner_id)
    assert exc_info.value.constraint_name == (
        "managed_repository_workspace_cleanup_required_before_owner_delete"
    )


@pytest.mark.asyncio
async def test_0196_settled_creation_still_requires_exact_terminal_cleanup(db):
    """The reservation fence must not survive its own settlement.

    Once the creation reservation settles, the same owner/scope has to satisfy
    the exact terminal-reclaim fence again, and the legitimate cleanup path
    must still admit the delete.
    """

    (
        owner_id,
        runtime_uid,
        reservation,
        _state,
    ) = await _create_inflight_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(owner_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="authority-envelope-creator",
        claim_token=int(reservation["claim_token"]),
        runtime_incarnation=runtime_uid,
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as exc_info:
            await conn.execute("DELETE FROM jobs WHERE id = $1", owner_id)
    assert exc_info.value.constraint_name == (
        "managed_repository_terminal_workspace_cleanup_required_before_owner_delete"
    )

    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE jobs SET status = 'failed' WHERE id = $1",
            owner_id,
        )
        await _execute_pre_0195(
            conn,
            "INSERT INTO managed_repository_workspace_cleanup_intents ("
            "owner_kind, owner_id, scope, runtime_incarnation, intent_source, "
            "target_disposition, resource_policy, reclaim_shared_resources, "
            "lifecycle_fingerprint, pod_uid, capture_complete, "
            "resources_captured_at, phase, cleanup_completed_at, settled_at, "
            "result_kind, projection_transaction_id) VALUES ("
            "'job', $1, 'workspace_container', $2, 'current', 'deleted', "
            "'terminal_reclaim', TRUE, '{}'::jsonb, $2, TRUE, now(), "
            "'settled', now(), now(), 'settled', 1)",
            owner_id,
            UUID(runtime_uid),
        )
        await conn.execute(
            "INSERT INTO managed_repository_process_zero_receipts ("
            "owner_kind, owner_id, scope, provisioner, runtime_incarnation, "
            "observed_at) VALUES ("
            "'job', $1, 'workspace_container', 'k8s', $2, now())",
            owner_id,
            runtime_uid,
        )
        assert (
            await conn.execute("DELETE FROM jobs WHERE id = $1", owner_id) == "DELETE 1"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("restore_type", (None, "vm"))
async def test_0300_stale_cancel_receipt_cannot_clear_malformed_ide(db, restore_type):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'malformed IDE guard', 'completed', 'owned-repo', '{}'::jsonb)",
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
        desired_manifest_digest="e" * 64,
        claimant="ide-issuer:malformed",
    )
    assert admitted["disposition"] == "accepted"
    row = admitted["reservation"]
    closed = await db.cancel_managed_ide_restore_attempt(
        str(job_id),
        attempt_id=str(attempt_id),
        reservation_id=str(row["id"]),
        claim_token=int(row["claim_token"]),
        claimant="ide-stop:malformed",
    )
    assert closed and closed["result_kind"] == "aborted"
    malformed = {
        "status": "restoring",
        "source": "gitea",
        "snapshot_type": "gitea",
        "_restore_attempt_id": str(attempt_id),
        "_creation_reservation_id": str(row["id"]),
        "_creation_claim_token": str(row["claim_token"]),
    }
    if restore_type is not None:
        malformed["restore_type"] = restore_type
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "UPDATE jobs SET context = jsonb_set(context, '{ide_session}', "
            "$2::jsonb) WHERE id = $1",
            job_id,
            json.dumps(malformed),
        )
        # The receipt exists, but its old transaction and malformed source
        # cannot authorize a second destructive projection.
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, "
                "'{ide_session,status}', '\"expired\"'::jsonb) WHERE id = $1",
                job_id,
            )



@pytest.mark.asyncio
async def test_post_reclaims_only_expired_exact_ide_attempt(db, monkeypatch):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'POST IDE reclaim', 'completed', 'owned-repo', '{}'::jsonb)",
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
        claimant="ide-issuer:lost-post",
    )
    assert admitted["disposition"] == "accepted"
    first = admitted["reservation"]
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(return_value={"digest": "a" * 64})
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    # An active claimant must remain held, even for the same exact attempt.
    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_not_called()
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            first["id"],
        )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_called_once()
    claimed = service._schedule_restore_task.call_args.kwargs["creation_reservation"]
    assert claimed["id"] == first["id"]
    assert claimed["reservation_generation"] == first["reservation_generation"]
    assert claimed["claim_token"] != first["claim_token"]
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, claim_token FROM "
            "managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1 AND scope = 'ide'",
            job_id,
        )
        ide = await conn.fetchval(
            "SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id
        )
    ide = json.loads(ide) if isinstance(ide, str) else ide
    assert len(rows) == 1 and rows[0]["id"] == first["id"]
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert ide["_creation_claim_token"] == str(claimed["claim_token"])



@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("stale_token", "stale_attempt", "plan_drift"))
async def test_post_refuses_stale_or_changed_ide_attempt(db, monkeypatch, mutation):
    job_id, attempt_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'POST IDE refusal', 'completed', 'owned-repo', '{}'::jsonb)",
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
        claimant="ide-issuer:refused-post",
    )
    assert admitted["disposition"] == "accepted"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            admitted["reservation"]["id"],
        )
        if mutation in {"stale_token", "stale_attempt"}:
            field = (
                "_creation_claim_token"
                if mutation == "stale_token"
                else "_restore_attempt_id"
            )
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, $2::text[], "
                "to_jsonb($3::text)) WHERE id = $1",
                job_id,
                ["ide_session", field],
                "99999" if mutation == "stale_token" else str(uuid4()),
            )
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(
        return_value={"digest": "b" * 64 if mutation == "plan_drift" else "a" * 64}
    )
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_not_called()
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, claim_token FROM "
            "managed_repository_workspace_creation_reservations WHERE id = $1",
            admitted["reservation"]["id"],
        )
    assert row["claim_token"] == admitted["reservation"]["claim_token"]



async def _legacy_runtime_bound_ide(db):
    job_id, runtime = uuid4(), uuid4()
    session = {
        "status": "restoring",
        "source": "gitea",
        "snapshot_type": "gitea",
        "restore_type": "k8s_container",
        "started_at": "2026-09-28T00:00:00Z",
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'legacy runtime-bound IDE', 'cancelled', 'owned-repo', "
            "$2::jsonb)",
            job_id,
            json.dumps({"ide_session": session}),
        )
    row = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="ide",
        operation_kind="restore",
        claimant="ide-restore:old-issuer",
        desired_manifest_digest="a" * 64,
    )
    assert row is not None
    gate = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=int(row["reservation_generation"]),
        claimant="ide-restore:old-issuer",
        claim_token=int(row["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate, runtime_incarnation=str(runtime)
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = jsonb_set(context, '{ide_session}', "
            "context->'ide_session' || $2::jsonb) WHERE id = $1",
            job_id,
            json.dumps(
                {
                    "_runtime_incarnation": str(runtime),
                    "_creation_reservation_id": str(row["id"]),
                    "_creation_claim_token": str(row["claim_token"]),
                    "container_name": f"ide-{str(job_id)[:12]}",
                }
            ),
        )
    return job_id, row, runtime



@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        "expired",
        "active_claim",
        "wrong_uid",
        "wrong_token",
        "wrong_scope",
        "cleanup_hold",
    ),
)
async def test_post_legacy_runtime_bound_replays_only_exact_receipt(
    db, monkeypatch, mutation
):
    job_id, first, runtime = await _legacy_runtime_bound_ide(db)
    async with db.acquire() as conn:
        if mutation != "active_claim":
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET created_at = now() - interval '2 minutes', "
                "expires_at = now() - interval '1 second' WHERE id = $1",
                first["id"],
            )
        if mutation in {"wrong_uid", "wrong_token"}:
            field = (
                "_runtime_incarnation"
                if mutation == "wrong_uid"
                else "_creation_claim_token"
            )
            await _execute_pre_0195(
                conn,
                "UPDATE jobs SET context = jsonb_set(context, $2::text[], "
                "to_jsonb($3::text)) WHERE id = $1",
                job_id,
                ["ide_session", field],
                str(uuid4()) if mutation == "wrong_uid" else "99999",
            )
        if mutation == "wrong_scope":
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET scope = 'workspace_container' WHERE id = $1",
                first["id"],
            )
        if mutation == "cleanup_hold":
            await conn.execute(
                "INSERT INTO managed_repository_workspace_cleanup_intents "
                "(owner_kind, owner_id, scope, runtime_incarnation, pod_uid, "
                "target_disposition) VALUES ('job', $1, 'ide', $2, $2, 'expired')",
                job_id,
                runtime,
            )
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(return_value={"digest": "a" * 64})
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, claim_token, runtime_incarnation FROM "
            "managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1",
            job_id,
        )
    assert len(rows) == 1 and rows[0]["id"] == first["id"]
    assert rows[0]["runtime_incarnation"] == runtime
    if mutation == "expired":
        service._schedule_restore_task.assert_called_once()
        claimed = service._schedule_restore_task.call_args.kwargs[
            "creation_reservation"
        ]
        assert claimed["id"] == first["id"]
        assert claimed["claim_token"] != first["claim_token"]
        assert rows[0]["claim_token"] == claimed["claim_token"]
    else:
        service._schedule_restore_task.assert_not_called()
        assert rows[0]["claim_token"] == first["claim_token"]



@pytest.mark.asyncio
async def test_post_keeps_uidless_legacy_projection_held(db, monkeypatch):
    job_id = uuid4()
    async with db.acquire() as conn:
        await _execute_pre_0195(
            conn,
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'unreceipted legacy IDE', 'cancelled', 'owned-repo', $2::jsonb)",
            job_id,
            json.dumps(
                {
                    "ide_session": {
                        "status": "restoring",
                        "source": "gitea",
                        "snapshot_type": "gitea",
                        "restore_type": "k8s_container",
                    }
                }
            ),
        )
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_not_called()
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_kind = 'job' AND owner_id = $1",
                job_id,
            )
            == 0
        )



@pytest.mark.asyncio
async def test_post_reclaims_new_attempt_after_exact_runtime_publication(
    db, monkeypatch
):
    job_id, attempt_id, runtime = uuid4(), uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'runtime-bound IDE attempt', 'cancelled', 'owned-repo', '{}'::jsonb)",
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
        claimant="ide-issuer:runtime-bound",
    )
    assert admitted["disposition"] == "accepted"
    first = admitted["reservation"]
    gate = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=int(first["reservation_generation"]),
        claimant="ide-issuer:runtime-bound",
        claim_token=int(first["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate, runtime_incarnation=str(runtime)
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context = jsonb_set(context, "
            "'{ide_session,_runtime_incarnation}', to_jsonb($2::text)) WHERE id = $1",
            job_id,
            str(runtime),
        )
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            first["id"],
        )
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(return_value={"digest": "a" * 64})
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_called_once()
    claimed = service._schedule_restore_task.call_args.kwargs["creation_reservation"]
    assert claimed["id"] == first["id"]
    assert claimed["runtime_incarnation"] == runtime
    assert claimed["claim_token"] != first["claim_token"]
    async with db.acquire() as conn:
        ide = await conn.fetchval(
            "SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_kind = 'job' AND owner_id = $1",
                job_id,
            )
            == 1
        )
    ide = json.loads(ide) if isinstance(ide, str) else ide
    assert ide["_runtime_incarnation"] == str(runtime)
    assert ide["_restore_attempt_id"] == str(attempt_id)
    assert ide["_creation_claim_token"] == str(claimed["claim_token"])



@pytest.mark.asyncio
async def test_atomic_a_retirement_b_publication_clears_a_attempt_lineage(
    db, monkeypatch
):
    job_id, attempt_id, runtime_a, runtime_b = uuid4(), uuid4(), uuid4(), uuid4()
    old_start = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    new_start = datetime.now(timezone.utc).isoformat()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, repo_name, context) "
            "VALUES ($1, 'atomic A then B IDE', 'cancelled', 'owned-repo', '{}'::jsonb)",
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
            "started_at": old_start,
            "max_lifetime_minutes": 240,
        },
        desired_manifest_digest="a" * 64,
        claimant="ide-issuer:cycle-a",
    )
    assert admitted["disposition"] == "accepted"
    first = admitted["reservation"]
    gate_a = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=int(first["reservation_generation"]),
        claimant="ide-issuer:cycle-a",
        claim_token=int(first["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate_a
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate_a, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate_a, runtime_incarnation=str(runtime_a)
    )
    assert await db.merge_ide_session_context(
        str(job_id),
        {
            "_runtime_incarnation": str(runtime_a),
            "_creation_reservation_id": str(first["id"]),
            "_creation_claim_token": str(first["claim_token"]),
            "container_name": f"ide-{str(job_id)[:12]}",
            "pod_ip": "10.42.0.31",
        },
    )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id), **gate_a, runtime_incarnation=str(runtime_a)
    )
    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        str(job_id),
        owner_kind="job",
        scope="ide",
        runtime_incarnation=str(runtime_a),
        target_disposition="expired",
        reclaim_shared_resources=False,
        pod_uid=str(runtime_a),
        resources_captured=True,
    )
    assert intent is not None
    claimed_cleanup = await db.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]), claimant="ide-stop:cycle-a"
    )
    assert claimed_cleanup is not None
    assert await db.record_managed_repository_workspace_process_zero(
        str(job_id),
        owner_kind="job",
        scope="ide",
        provisioner="k8s",
        runtime_incarnation=str(runtime_a),
    )
    assert await db.settle_managed_repository_workspace_cleanup_intent(
        str(job_id),
        owner_kind="job",
        scope="ide",
        runtime_incarnation=str(runtime_a),
        intent_generation=int(claimed_cleanup["intent_generation"]),
        claimant=str(claimed_cleanup["claimed_by"]),
        claim_token=int(claimed_cleanup["claim_token"]),
    )
    async with db.acquire() as conn:
        retired = await conn.fetchval(
            "SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id
        )
    retired = json.loads(retired) if isinstance(retired, str) else retired
    assert retired["status"] == "expired"
    assert retired["_restore_attempt_id"] == str(attempt_id)
    assert retired["started_at"] == old_start

    second = await db.reserve_managed_repository_workspace_creation(
        str(job_id),
        owner_kind="job",
        scope="ide",
        operation_kind="restore",
        claimant=f"ide-restore:{runtime_a}",
        desired_manifest_digest="a" * 64,
    )
    assert second is not None and second["id"] != first["id"]
    gate_b = dict(
        owner_kind="job",
        scope="ide",
        reservation_generation=int(second["reservation_generation"]),
        claimant=f"ide-restore:{runtime_a}",
        claim_token=int(second["claim_token"]),
    )
    assert await db.mark_managed_repository_workspace_creation_started(
        str(job_id), **gate_b
    )
    assert await db.begin_managed_repository_workspace_creation_effect(
        str(job_id), **gate_b, resource_kind="pod"
    )
    assert await db.authorize_managed_repository_workspace_creation_runtime(
        str(job_id), **gate_b, runtime_incarnation=str(runtime_b)
    )
    # This is the exact first-B publication shape emitted by the provisioner.
    # The previous A attempt remains in its historical reservation, while the
    # owner now names B alone even if the issuer crashes before settlement.
    assert await db.merge_ide_session_context(
        str(job_id),
        {
            "status": "restoring",
            "source": "gitea",
            "snapshot_type": "gitea",
            "_restore_attempt_id": None,
            "started_at": new_start,
            "last_activity": None,
            "code_server_url": None,
            "max_lifetime_minutes": 240,
            "_runtime_incarnation": str(runtime_b),
            "_creation_reservation_id": str(second["id"]),
            "_creation_claim_token": str(second["claim_token"]),
            "container_name": f"ide-{str(job_id)[:12]}",
        },
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at = now() - interval '2 minutes', "
            "expires_at = now() - interval '1 second' WHERE id = $1",
            second["id"],
        )
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._ide_creation_plan = AsyncMock(return_value={"digest": "a" * 64})
    service = IdeSessionService()
    service.connect(db, None, None, container_provisioner=provisioner)
    service.get_session_status = AsyncMock(return_value={"status": "restoring"})
    service._schedule_restore_task = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.ide_session.contained_ide_status", lambda: None
    )

    assert (await service.start_session(str(job_id)))["status"] == "restoring"
    service._schedule_restore_task.assert_called_once()
    replay = service._schedule_restore_task.call_args.kwargs["creation_reservation"]
    assert replay["id"] == second["id"]
    assert replay["runtime_incarnation"] == runtime_b
    assert replay["claim_token"] != second["claim_token"]
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, lifecycle_fingerprint FROM "
            "managed_repository_workspace_creation_reservations "
            "WHERE owner_kind = 'job' AND owner_id = $1 ORDER BY reservation_generation",
            job_id,
        )
        current = await conn.fetchval(
            "SELECT context->'ide_session' FROM jobs WHERE id = $1", job_id
        )
    current = json.loads(current) if isinstance(current, str) else current
    assert [row["id"] for row in rows] == [first["id"], second["id"]]
    history = rows[0]["lifecycle_fingerprint"]
    history = json.loads(history) if isinstance(history, str) else history
    assert history["restore_attempt_id"] == str(attempt_id)
    assert current.get("_restore_attempt_id") is None
    assert current["started_at"] == new_start
    assert current["started_at"] != old_start
    assert current["last_activity"] is None
    assert current["code_server_url"] is None
    assert current["_runtime_incarnation"] == str(runtime_b)
    assert current["_creation_reservation_id"] == str(second["id"])
