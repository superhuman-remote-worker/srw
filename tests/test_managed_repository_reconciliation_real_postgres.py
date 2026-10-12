"""Real-PostgreSQL proofs for durable legacy repository reconciliation."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import orchestrator.database.postgres as postgres_module
from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.services.managed_repository_authority import (
    ensure_job_primary_repository_authority,
)
from orchestrator.services.managed_repository_reconciliation import (
    _process_claim,
    reconcile_managed_repository_legacy_once,
    scan_managed_repository_legacy_sources,
    serialize_legacy_reconciliation_report,
)

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
INVENTORY_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "inventory-managed-repository-authority.py"
)
JOB_STATUSES = (
    "created",
    "processing",
    "completed",
    "failed",
    "cancelled",
    "pending_review",
    "paused",
    "reviewing",
    "waiting",
    "waiting_for_reply",
)
TERMINAL_JOB_STATUSES = frozenset({"completed"})


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "R" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=12,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE managed_repository_legacy_reconciliations, "
            "managed_repository_authorities, managed_repository_creation_intents, "
            "run_queue, project_repositories, jobs, threads, agents, "
            "project_members, projects, users RESTART IDENTITY CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()
        crypto.reset_cipher_cache()


def _gitea(*, key_results=None, probe_results=None) -> MagicMock:
    client = MagicMock()
    client.repository_owner = "srw"
    client.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://gitea:3000/srw/{name}.git"
    )
    client.ensure_repo_deploy_key = AsyncMock(
        side_effect=key_results,
        return_value=91,
    )
    client.probe_repo_deploy_key = AsyncMock(
        side_effect=probe_results,
        return_value=True,
    )
    client.delete_repo_deploy_key = AsyncMock(return_value=True)
    return client


def _legacy_url(repo_name: str) -> str:
    return f"http://legacy-admin:historical-secret@gitea:3000/srw/{repo_name}.git"


@asynccontextmanager
async def _legacy_writes(conn):
    triggers = (
        ("jobs", "trg_managed_job_repository_url_authority"),
        ("threads", "trg_managed_thread_repository_url_authority"),
        ("project_repositories", "trg_managed_project_repository_url_authority"),
        ("project_officers", "trg_officer_post_thread_repository_authority"),
    )
    for table, trigger in triggers:
        await conn.execute(f"ALTER TABLE {table} DISABLE TRIGGER {trigger}")
    try:
        yield
    finally:
        for table, trigger in reversed(triggers):
            await conn.execute(f"ALTER TABLE {table} ENABLE TRIGGER {trigger}")


@asynccontextmanager
async def _pre_0196_thread_workspace_insert(conn):
    """Seed the exact UID-less runtime shape accepted before migration 0198."""

    trigger = "trg_threads_require_workspace_creation_reservation_on_insert"
    await conn.execute(f"ALTER TABLE threads DISABLE TRIGGER {trigger}")
    try:
        yield
    finally:
        await conn.execute(f"ALTER TABLE threads ENABLE TRIGGER {trigger}")


async def _force_reconciliation_due(db: PostgresDB) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_legacy_reconciliations "
            "SET next_attempt_at=now(), claim_expires_at=CASE "
            "WHEN state='claimed' THEN now() - interval '1 second' "
            "ELSE claim_expires_at END"
        )


async def _credentialed_count(db: PostgresDB) -> int:
    async with db.acquire() as conn:
        return int(
            await conn.fetchval(
                """
                SELECT (
                    SELECT count(*) FROM jobs
                     WHERE public.managed_repository_url_has_userinfo(
                         context->>'git_remote_url'
                     )
                ) + (
                    SELECT count(*) FROM threads
                     WHERE public.managed_repository_url_has_userinfo(
                         metadata->'workspace_container'->>'git_remote_url'
                     )
                ) + (
                    SELECT count(*) FROM project_repositories
                     WHERE public.managed_repository_url_has_userinfo(repo_url)
                )
                """
            )
        )


def _load_inventory_script():
    spec = importlib.util.spec_from_file_location(
        "managed_repository_inventory_script", INVENTORY_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_operator_inventory_counts_are_secret_free_and_schema_valid(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'inventory fixture', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            _legacy_url(repo_name),
        )

    inventory = _load_inventory_script()
    counts = await inventory._safe_inventory_counts(db)

    assert counts["reconciliation_table_present"] is True
    assert counts["credentialed_legacy_rows"] == 1
    assert counts["managed_scopes_without_active_authority"] == 1
    assert counts["incomplete_creation_intents"] == 0
    rendered = json.dumps(counts, default=str)
    assert repo_name not in rendered
    assert "historical-secret" not in rendered


@pytest.mark.asyncio
async def test_every_job_status_adopts_or_scrubs_without_fixed_window(db):
    jobs = [(uuid4(), status) for status in JOB_STATUSES]
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.executemany(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, branch_name, context) VALUES "
            "($1, 'production-shaped legacy job', $2, 'pinned', $3, 'main', "
            "jsonb_build_object('git_remote_url', $4::text))",
            [
                (
                    job_id,
                    status,
                    f"job-{str(job_id)[:8]}",
                    _legacy_url(f"job-{str(job_id)[:8]}"),
                )
                for job_id, status in jobs
            ],
        )

    gitea = _gitea()
    stats, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, page_size=3, concurrency=4
    )

    assert stats.scanned == len(JOB_STATUSES)
    assert not details["progress"]["failure_reasons"], details["progress"][
        "failure_reasons"
    ]
    assert stats.adopted == len(JOB_STATUSES) - len(TERMINAL_JOB_STATUSES), details
    assert stats.scrubbed_terminal == len(TERMINAL_JOB_STATUSES)
    assert stats.ambiguous == 0
    assert await _credentialed_count(db) == 0
    assert gitea.ensure_repo_deploy_key.await_count == (
        len(JOB_STATUSES) - len(TERMINAL_JOB_STATUSES)
    )
    async with db.acquire() as conn:
        terminal_authorities = await conn.fetchval(
            "SELECT count(*) FROM managed_repository_authorities "
            "WHERE authority_id=ANY($1::uuid[])",
            [job_id for job_id, status in jobs if status in TERMINAL_JOB_STATUSES],
        )
        stored_statuses = await conn.fetch(
            "SELECT id, status::text AS status FROM jobs"
        )
    assert terminal_authorities == 0
    assert {str(row["id"]): row["status"] for row in stored_statuses} == {
        str(job_id): status for job_id, status in jobs
    }
    assert not details["progress"]["ambiguous"]

    calls = gitea.ensure_repo_deploy_key.await_count
    rerun, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, page_size=2
    )
    assert rerun.scanned == 0
    assert gitea.ensure_repo_deploy_key.await_count == calls


@pytest.mark.asyncio
async def test_blocked_undelivered_job_scrubs_without_runtime_authority(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, branch_name, context) VALUES "
            "($1, 'blocked undelivered history', 'processing', 'pinned', "
            "$2, 'main', jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            _legacy_url(repo_name),
        )
        await conn.execute(
            "UPDATE jobs SET status='cancelled', "
            "completion_outcome_kind='blocked_undelivered' WHERE id=$1",
            job_id,
        )

    gitea = _gitea()
    stats, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True
    )

    assert stats.scrubbed_terminal == 1
    assert stats.adopted == 0
    assert not details["progress"]["ambiguous"]
    assert await _credentialed_count(db) == 0
    gitea.ensure_repo_deploy_key.assert_not_awaited()
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1)",
            job_id,
        )


@pytest.mark.asyncio
async def test_threads_officer_and_project_modes_preserve_exact_lifecycle(db):
    project_id = uuid4()
    current_thread = uuid4()
    idle_ended_thread = uuid4()
    orphan_ended_thread = uuid4()
    retired_thread = uuid4()
    repositories = [
        (uuid4(), "source", False),
        (uuid4(), "reference", True),
        (uuid4(), "jobs", False),
        (uuid4(), "knowledge", False),
    ]
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'legacy scope modes')",
            project_id,
        )
        async with _legacy_writes(conn):
            for thread_id, status, lane, extra in (
                (current_thread, "active", "pinned", {}),
                (
                    idle_ended_thread,
                    "ended",
                    "pinned",
                    {
                        "last_memory_archive_at": "2026-08-25T00:00:00Z",
                        "workspace_status": "suspended",
                    },
                ),
                (
                    orphan_ended_thread,
                    "ended",
                    "pinned",
                    {"workspace_status": "ready"},
                ),
                (
                    retired_thread,
                    "ended",
                    "stateless",
                    {
                        "_stateless_workspace_retirement_settled": {
                            "terminal_token": 8,
                            "cleanup_complete": True,
                            "permanent": True,
                            "backing_id": None,
                            "runtime_incarnation": None,
                            "snapshot_restore_required": False,
                        }
                    },
                ),
            ):
                repo_name = f"thread-{str(thread_id)[:8]}"
                workspace_status = extra.pop("workspace_status", None)
                metadata = {
                    **extra,
                    "workspace_container": {
                        "repo_name": repo_name,
                        "git_remote_url": _legacy_url(repo_name),
                        **(
                            {"status": workspace_status}
                            if workspace_status is not None
                            else {}
                        ),
                    },
                }
                await conn.execute(
                    "INSERT INTO threads (id, title, project_id, status, "
                    "execution_lane, metadata) VALUES "
                    "($1, 'legacy thread', $2, $3, $4, $5::jsonb)",
                    thread_id,
                    project_id,
                    status,
                    lane,
                    json.dumps(metadata),
                )
            for repository_id, role, read_only in repositories:
                repo_name = f"project-{str(repository_id)[:8]}-{role}"
                await conn.execute(
                    "INSERT INTO project_repositories "
                    "(id, project_id, name, repo_url, role, read_only, is_managed) "
                    "VALUES ($1, $2, $3, $4, $5, $6, true)",
                    repository_id,
                    project_id,
                    repo_name,
                    _legacy_url(repo_name),
                    role,
                    read_only,
                )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_officers (project_id, thread_id) VALUES ($1, $2)",
                project_id,
                current_thread,
            )

    gitea = _gitea()
    stats, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, page_size=2, concurrency=3
    )
    assert stats.scanned == 8
    assert stats.adopted == 6
    assert stats.scrubbed_terminal == 2
    assert await _credentialed_count(db) == 0
    async with db.acquire() as conn:
        modes = await conn.fetch(
            "SELECT authority_kind, authority_id, access_mode "
            "FROM managed_repository_authorities ORDER BY authority_kind, authority_id"
        )
        classifications = await conn.fetch(
            "SELECT source_id, classification, result_kind "
            "FROM managed_repository_legacy_reconciliations"
        )
    mode_by_id = {str(row["authority_id"]): row["access_mode"] for row in modes}
    assert mode_by_id[str(current_thread)] == "write"
    assert mode_by_id[str(idle_ended_thread)] == "write"
    assert mode_by_id[str(orphan_ended_thread)] == "write"
    assert str(retired_thread) not in mode_by_id
    for repository_id, role, read_only in repositories:
        if role == "knowledge":
            assert str(repository_id) not in mode_by_id
        else:
            assert mode_by_id[str(repository_id)] == (
                "read" if role == "reference" or read_only else "write"
            )
    classification_by_id = {
        str(row["source_id"]): (row["classification"], row["result_kind"])
        for row in classifications
    }
    assert classification_by_id[str(current_thread)] == (
        "current_officer_thread",
        "adopted",
    )
    assert classification_by_id[str(idle_ended_thread)] == (
        "resumable_thread",
        "adopted",
    )
    assert classification_by_id[str(orphan_ended_thread)] == (
        "resumable_thread",
        "adopted",
    )
    assert classification_by_id[str(retired_thread)] == (
        "terminal_historical",
        "scrubbed_terminal",
    )
    rendered = json.dumps(
        serialize_legacy_reconciliation_report(stats, details), default=str
    )
    assert "historical-secret" not in rendered
    assert all(repo_id.hex not in rendered for repo_id, _, _ in repositories)


@pytest.mark.asyncio
async def test_shared_jobs_root_and_subjob_reuse_exact_write_authority(db):
    project_id = uuid4()
    repository_id = uuid4()
    parent_id = uuid4()
    child_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-jobs"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'shared jobs history')",
            project_id,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) "
                "VALUES ($1, $2, $3, $4, 'jobs', true)",
                repository_id,
                project_id,
                repo_name,
                _legacy_url(repo_name),
            )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, branch_name, context) VALUES "
            "($1, 'historical root', 'created', 'pinned', $2, 'main', '{}'), "
            "($3, 'historical child', 'paused', 'pinned', $2, 'child', '{}')",
            parent_id,
            project_id,
            child_id,
        )
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$1 WHERE id=$2", parent_id, child_id
        )

    gitea = _gitea()
    stats, _ = await reconcile_managed_repository_legacy_once(db, gitea, apply=True)
    assert stats.adopted == 1
    parent_authority = await ensure_job_primary_repository_authority(
        db, gitea, await db.get_job(str(parent_id))
    )
    child_authority = await ensure_job_primary_repository_authority(
        db, gitea, await db.get_job(str(child_id))
    )
    assert parent_authority is not None
    assert child_authority is not None
    assert child_authority["id"] == parent_authority["id"]
    assert child_authority["authority_id"] == repository_id
    assert child_authority["access_mode"] == "write"
    assert gitea.ensure_repo_deploy_key.await_count == 1


@pytest.mark.asyncio
async def test_recommission_race_never_transfers_thread_authority(db):
    project_id = uuid4()
    predecessor = uuid4()
    successor = uuid4()
    repo_name = f"thread-{str(predecessor)[:8]}"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'officer race')",
            project_id,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO threads (id, title, project_id, status, "
                "execution_lane, metadata) VALUES "
                "($1, 'old officer', $3, 'ended', 'pinned', "
                "jsonb_build_object('workspace_container', "
                "jsonb_build_object('repo_name', $4::text, "
                "'git_remote_url', $5::text))), "
                "($2, 'new officer', $3, 'active', 'pinned', '{}'::jsonb)",
                predecessor,
                successor,
                project_id,
                repo_name,
                _legacy_url(repo_name),
            )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_officers (project_id, thread_id) VALUES ($1, $2)",
                project_id,
                predecessor,
            )

    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE project_officers SET thread_id=$2, updated_at=now() "
            "WHERE project_id=$1",
            project_id,
            successor,
        )

    stats, _ = await reconcile_managed_repository_legacy_once(
        db, _gitea(), apply=True, concurrency=1
    )
    assert stats.adopted == 1
    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT classification, authority_kind, authority_id, state "
            "FROM managed_repository_legacy_reconciliations "
            "WHERE source_kind='thread' AND source_id=$1",
            predecessor,
        )
        authorities = await conn.fetch(
            "SELECT authority_id FROM managed_repository_authorities"
        )
        current = await conn.fetchval(
            "SELECT thread_id FROM project_officers WHERE project_id=$1",
            project_id,
        )
    assert dict(intent) == {
        "classification": "resumable_thread",
        "authority_kind": "thread",
        "authority_id": predecessor,
        "state": "completed",
    }
    assert [row["authority_id"] for row in authorities] == [predecessor]
    assert current == successor


@pytest.mark.asyncio
async def test_ambiguity_is_opaque_then_supported_resolution_converges(db):
    first_project = uuid4()
    second_project = uuid4()
    first_repository = uuid4()
    second_repository = uuid4()
    repo_name = "historical-collision"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'first scope'), "
            "($2, 'second scope')",
            first_project,
            second_project,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) VALUES "
                "($1, $2, $5, $6, 'source', true), "
                "($3, $4, $5, $6, 'source', true)",
                first_repository,
                first_project,
                second_repository,
                second_project,
                repo_name,
                _legacy_url(repo_name),
            )

    gitea = _gitea()
    populated, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True
    )
    assert populated.scanned == 2
    assert populated.ambiguous == 2
    assert gitea.ensure_repo_deploy_key.await_count == 0
    report = json.dumps(
        serialize_legacy_reconciliation_report(populated, details), default=str
    )
    assert repo_name not in report
    assert "historical-secret" not in report

    removed = await db.remove_project_repository(str(second_repository))
    assert removed is not None
    converged, progress = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True
    )
    assert converged.adopted == 1
    assert converged.ambiguous == 0
    assert await _credentialed_count(db) == 0
    assert not progress["progress"]["ambiguous"]
    async with db.acquire() as conn:
        absent = await conn.fetchval(
            "SELECT result_kind FROM managed_repository_legacy_reconciliations "
            "WHERE source_kind='project_repository' AND source_id=$1",
            second_repository,
        )
    assert absent == "source_absent"


@pytest.mark.asyncio
async def test_claim_lease_reclaim_has_one_winner_and_fences_predecessor(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    observed = _legacy_url(repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'terminal lease fixture', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            observed,
        )
    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)
    first, second = await asyncio.gather(
        db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        ),
        db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        ),
    )
    assert sorted((len(first), len(second))) == [0, 1]
    predecessor = (first or second)[0]
    await _force_reconciliation_due(db)
    reclaimed = await asyncio.gather(
        db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        ),
        db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        ),
    )
    assert sorted(map(len, reclaimed)) == [0, 1]
    winner = (reclaimed[0] or reclaimed[1])[0]
    assert winner["claim_token"] != predecessor["claim_token"]
    assert (
        await db.finish_managed_repository_legacy_reconciliation(
            str(predecessor["id"]),
            int(predecessor["claim_token"]),
            observed_url=observed,
            authority_id=None,
        )
        is None
    )
    assert (
        await db.finish_managed_repository_legacy_reconciliation(
            str(winner["id"]),
            int(winner["claim_token"]),
            observed_url=observed,
            authority_id=None,
        )
        == "scrubbed_terminal"
    )


@pytest.mark.asyncio
async def test_forge_response_loss_and_probe_failure_retry_one_generation(db):
    jobs = [uuid4(), uuid4()]
    async with db.acquire() as conn, _legacy_writes(conn):
        repo_name = f"job-{str(jobs[0])[:8]}"
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'forge fault fixture', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            jobs[0],
            repo_name,
            _legacy_url(repo_name),
        )

    lost_response = _gitea(key_results=[None, 91])
    first, _ = await reconcile_managed_repository_legacy_once(
        db, lost_response, apply=True, concurrency=1
    )
    assert first.deferred == 1
    await _force_reconciliation_due(db)
    second, _ = await reconcile_managed_repository_legacy_once(
        db, lost_response, apply=True, concurrency=1
    )
    assert second.adopted == 1

    async with db.acquire() as conn, _legacy_writes(conn):
        repo_name = f"job-{str(jobs[1])[:8]}"
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'probe fault fixture', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            jobs[1],
            repo_name,
            _legacy_url(repo_name),
        )
    probe_retry = _gitea(probe_results=[False, True])
    third, _ = await reconcile_managed_repository_legacy_once(
        db, probe_retry, apply=True, concurrency=1
    )
    assert third.deferred == 1
    await _force_reconciliation_due(db)
    fourth, _ = await reconcile_managed_repository_legacy_once(
        db, probe_retry, apply=True, concurrency=1
    )
    assert fourth.adopted == 1
    assert await _credentialed_count(db) == 0
    async with db.acquire() as conn:
        generations = await conn.fetch(
            "SELECT authority_id, count(*) AS count, max(generation) AS generation "
            "FROM managed_repository_authorities GROUP BY authority_id"
        )
    assert {row["authority_id"] for row in generations} == set(jobs)
    assert all(row["count"] == 1 and row["generation"] == 1 for row in generations)


@pytest.mark.asyncio
async def test_crash_after_proof_before_cas_restarts_without_second_key(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    legacy_url = _legacy_url(repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'crash after proof', 'paused', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            legacy_url,
        )

    gitea = _gitea()
    original_finish = db.finish_managed_repository_legacy_reconciliation
    db.finish_managed_repository_legacy_reconciliation = AsyncMock(
        side_effect=asyncio.CancelledError
    )
    with pytest.raises(asyncio.CancelledError):
        await reconcile_managed_repository_legacy_once(
            db, gitea, apply=True, concurrency=1, lease_seconds=30
        )
    db.finish_managed_repository_legacy_reconciliation = original_finish

    assert await _credentialed_count(db) == 1
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as refused:
            await conn.execute("DELETE FROM jobs WHERE id=$1", job_id)
        assert refused.value.constraint_name == "managed_repository_cleanup_required"
    await _force_reconciliation_due(db)
    restarted, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1, lease_seconds=30
    )
    assert restarted.adopted == 1
    assert await _credentialed_count(db) == 0
    assert gitea.ensure_repo_deploy_key.await_count == 1
    async with db.acquire() as conn:
        authority_count = await conn.fetchval(
            "SELECT count(*) FROM managed_repository_authorities "
            "WHERE authority_id=$1 AND status='active'",
            job_id,
        )
    assert authority_count == 1


@pytest.mark.asyncio
async def test_expired_predecessor_does_not_revoke_clean_successor_authority(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'stale predecessor', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            _legacy_url(repo_name),
        )
    gitea = _gitea()
    await scan_managed_repository_legacy_sources(db, gitea, apply=True)
    predecessor = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_legacy_reconciliations "
            "SET claim_expires_at=clock_timestamp() - interval '1 second' "
            "WHERE id=$1",
            predecessor["id"],
        )

    successor, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    assert successor.adopted == 1
    assert await _process_claim(db, gitea, predecessor, max_attempts=8) == "deferred"

    async with db.acquire() as conn:
        authority = await conn.fetchrow(
            "SELECT status, authority_kind, authority_id "
            "FROM managed_repository_authorities WHERE repo_name=$1",
            repo_name,
        )
        current_url = await conn.fetchval(
            "SELECT context->>'git_remote_url' FROM jobs WHERE id=$1", job_id
        )
    assert dict(authority) == {
        "status": "active",
        "authority_kind": "job",
        "authority_id": job_id,
    }
    assert current_url == f"http://gitea:3000/srw/{repo_name}.git"
    gitea.delete_repo_deploy_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_clean_authority_waits_for_resumable_lineage_then_is_revoked(db):
    root_id = uuid4()
    child_id = uuid4()
    repo_name = f"job-{str(root_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'later-terminal root', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            root_id,
            repo_name,
            _legacy_url(repo_name),
        )
    gitea = _gitea()
    adopted, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    assert adopted.adopted == 1

    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "parent_job_id, repo_name, context) VALUES "
            "($1, 'resumable failed child', 'failed', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            child_id,
            root_id,
            repo_name,
            clean_url,
        )
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", root_id)

    retained, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    assert retained.scanned == 0
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT status FROM managed_repository_authorities "
                "WHERE authority_kind='job' AND authority_id=$1",
                root_id,
            )
            == "active"
        )
    gitea.delete_repo_deploy_key.assert_not_awaited()

    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", child_id)
    contained, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert contained.scanned == 1
    assert contained.scrubbed_terminal == 1
    assert contained.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            root_id,
        )
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            root_id,
        )
    assert authority_status == "revoked"
    assert dict(intent) == {"state": "completed", "result_kind": "authority_revoked"}
    gitea.delete_repo_deploy_key.assert_awaited_once_with(repo_name, 91)


@pytest.mark.asyncio
async def test_terminal_credentialed_root_scrubs_before_lineage_key_is_disposable(db):
    root_id = uuid4()
    child_id = uuid4()
    repo_name = f"job-{str(root_id)[:8]}"
    observed_url = _legacy_url(repo_name)
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'terminal credentialed root', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            root_id,
            repo_name,
            observed_url,
        )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(root_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=clean_url,
        public_key="ssh-ed25519 AAAAlineageroot",
        public_key_fingerprint="SHA256:lineage-root",
        private_key="private-lineage-root",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(root_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:lineage-root",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=91, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=91, access_mode="write"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "parent_job_id, repo_name, context) VALUES "
            "($1, 'resumable failed child', 'failed', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            child_id,
            root_id,
            repo_name,
            clean_url,
        )

    gitea = _gitea()
    scrubbed, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert scrubbed.scrubbed_terminal == 1
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        root_url = await conn.fetchval(
            "SELECT context->>'git_remote_url' FROM jobs WHERE id=$1", root_id
        )
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities WHERE id=$1",
            authority["id"],
        )
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            root_id,
        )
    assert root_url == clean_url
    assert authority_status == "active"
    assert dict(intent) == {"state": "completed", "result_kind": "scrubbed_terminal"}
    gitea.delete_repo_deploy_key.assert_not_awaited()

    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", child_id)
    contained, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert contained.scanned == 1
    assert contained.scrubbed_terminal == 1
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities WHERE id=$1",
            authority["id"],
        )
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            root_id,
        )
    assert authority_status == "revoked"
    assert dict(intent) == {"state": "completed", "result_kind": "authority_revoked"}
    gitea.delete_repo_deploy_key.assert_awaited_once_with(repo_name, 91)


@pytest.mark.asyncio
async def test_terminal_transition_contains_unproven_recorded_forge_key(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'unproven then terminal', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            _legacy_url(repo_name),
        )
    gitea = _gitea(probe_results=[False])
    first, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    assert first.deferred == 1
    async with db.acquire() as conn:
        unproven = await conn.fetchrow(
            "SELECT status, forge_key_id FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            job_id,
        )
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
    assert dict(unproven) == {"status": "provisioning", "forge_key_id": 91}

    await _force_reconciliation_due(db)
    terminal, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert terminal.scrubbed_terminal == 1
    async with db.acquire() as conn:
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            job_id,
        )
    assert authority_status == "revoked"
    gitea.delete_repo_deploy_key.assert_awaited_once_with(repo_name, 91)


@pytest.mark.asyncio
async def test_source_deletion_and_lifecycle_change_win_before_cas(db):
    deleted_job = uuid4()
    changed_job = uuid4()
    async with db.acquire() as conn, _legacy_writes(conn):
        for job_id, status in ((deleted_job, "completed"), (changed_job, "created")):
            repo_name = f"job-{str(job_id)[:8]}"
            await conn.execute(
                "INSERT INTO jobs (id, description, status, execution_lane, "
                "repo_name, context) VALUES "
                "($1, 'source race', $2, 'pinned', $3, "
                "jsonb_build_object('git_remote_url', $4::text))",
                job_id,
                status,
                repo_name,
                _legacy_url(repo_name),
            )
    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)
    assert await db.delete_job(str(deleted_job))

    gitea = _gitea()
    original_finish = db.finish_managed_repository_legacy_reconciliation
    changed = False

    async def transition_before_finish(*args, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            async with db.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET status='completed' WHERE id=$1", changed_job
                )
        return await original_finish(*args, **kwargs)

    db.finish_managed_repository_legacy_reconciliation = transition_before_finish
    first, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    db.finish_managed_repository_legacy_reconciliation = original_finish
    assert first.deferred == 1
    await _force_reconciliation_due(db)
    settled, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    assert settled.scrubbed_terminal == 1
    assert settled.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        deleted_result = await conn.fetchval(
            "SELECT result_kind FROM managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            deleted_job,
        )
        changed_authorities = await conn.fetch(
            "SELECT status FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            changed_job,
        )
    assert deleted_result == "source_absent"
    assert [row["status"] for row in changed_authorities] == ["revoked"]
    gitea.delete_repo_deploy_key.assert_awaited_once()
    assert await _credentialed_count(db) == 0


@pytest.mark.asyncio
async def test_forced_source_loss_after_authority_activation_is_contained(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'forced source loss', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            _legacy_url(repo_name),
        )

    gitea = _gitea()
    original_finish = db.finish_managed_repository_legacy_reconciliation
    deleted = False

    async def delete_before_finish(*args, **kwargs):
        nonlocal deleted
        if not deleted and kwargs.get("authority_id") is not None:
            # Migration 0176 normally makes this ordering impossible: its
            # cleanup trigger refuses deletion while exact key authority is
            # active. Disable and restore only that trigger in one atomic test
            # transaction to exercise the reconciler's defense-in-depth path
            # for corruption or a historical database missing the fence.
            async with db.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "ALTER TABLE jobs DISABLE TRIGGER "
                        "trg_managed_job_repository_cleanup"
                    )
                    assert (
                        await conn.execute("DELETE FROM jobs WHERE id=$1", job_id)
                        == "DELETE 1"
                    )
                    await conn.execute(
                        "ALTER TABLE jobs ENABLE TRIGGER "
                        "trg_managed_job_repository_cleanup"
                    )
            deleted = True
        return await original_finish(*args, **kwargs)

    db.finish_managed_repository_legacy_reconciliation = delete_before_finish
    first, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    db.finish_managed_repository_legacy_reconciliation = original_finish
    assert first.deferred == 1
    assert deleted
    assert await db.get_job(str(job_id)) is None
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT tgenabled = 'O' FROM pg_trigger "
            "WHERE tgrelid='jobs'::regclass "
            "AND tgname='trg_managed_job_repository_cleanup'"
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_authorities "
                "WHERE authority_kind='job' AND authority_id=$1 AND status='active'",
                job_id,
            )
            == 1
        )

    await _force_reconciliation_due(db)
    settled, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert settled.scrubbed_terminal == 1
    assert settled.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            job_id,
        )
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            job_id,
        )
    assert dict(intent) == {"state": "completed", "result_kind": "authority_revoked"}
    assert authority_status == "revoked"
    gitea.delete_repo_deploy_key.assert_awaited_once()


@pytest.mark.asyncio
async def test_stored_repository_owner_drift_never_mutates_configured_namespace(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    # A newly reserved Job authority must have a live owner. The mismatch
    # check below still runs before any forge operation or lifecycle cleanup.
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,execution_lane) "
            "VALUES($1,'repository owner drift','completed','pinned')",
            job_id,
        )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="legacy-owner",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/legacy-owner/{repo_name}.git",
        public_key="ssh-ed25519 AAAAownerdrift",
        public_key_fingerprint="SHA256:owner-drift",
        private_key="private-owner-drift",
    )
    exact = {
        "repository_owner": "legacy-owner",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(job_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:owner-drift",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=91, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=91, access_mode="write"
    )
    await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="job",
        source_id=str(job_id),
        project_id=None,
        classification="terminal_historical",
        authority_kind="job",
        authority_id=str(job_id),
        repository_owner="legacy-owner",
        repo_name=repo_name,
        access_mode="write",
        reason_code="source_absent",
        authority_record_id=str(authority["id"]),
        authority_generation=1,
    )
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1
        )
    )[0]
    gitea = _gitea()

    assert await _process_claim(db, gitea, claim, max_attempts=8) == "ambiguous"

    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT state, reason_code FROM "
            "managed_repository_legacy_reconciliations WHERE id=$1",
            claim["id"],
        )
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities WHERE id=$1",
            authority["id"],
        )
    assert dict(intent) == {
        "state": "ambiguous",
        "reason_code": "repository_owner_mismatch",
    }
    assert authority_status == "active"
    gitea.delete_repo_deploy_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_permanent_thread_retirement_after_activation_revokes_key(db):
    project_id = uuid4()
    thread_id = uuid4()
    repo_name = f"thread-{str(thread_id)[:8]}"
    workspace = {
        "repo_name": repo_name,
        "git_remote_url": _legacy_url(repo_name),
        "status": "ready",
        "provisioner": "k8s",
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'thread retirement race')",
            project_id,
        )
        async with _legacy_writes(conn), _pre_0196_thread_workspace_insert(conn):
            await conn.execute(
                "INSERT INTO threads (id, title, project_id, status, "
                "execution_lane, metadata) VALUES "
                "($1, 'thread retirement race', $2, 'active', 'stateless', $3::jsonb)",
                thread_id,
                project_id,
                json.dumps({"workspace_container": workspace}),
            )

    gitea = _gitea()
    original_finish = db.finish_managed_repository_legacy_reconciliation
    retired = False

    async def retire_before_finish(*args, **kwargs):
        nonlocal retired
        if not retired and kwargs.get("authority_id") is not None:
            retired = True
            metadata = {
                "workspace_container": workspace,
                "_stateless_workspace_retirement_settled": {
                    "terminal_token": 8,
                    "cleanup_complete": True,
                    "permanent": True,
                    "backing_id": None,
                    "runtime_incarnation": None,
                    "snapshot_restore_required": False,
                },
            }
            async with db.acquire() as conn, _legacy_writes(conn):
                await conn.execute(
                    "UPDATE threads SET status='ended', metadata=$2::jsonb WHERE id=$1",
                    thread_id,
                    json.dumps(metadata),
                )
        return await original_finish(*args, **kwargs)

    db.finish_managed_repository_legacy_reconciliation = retire_before_finish
    first, _ = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )
    db.finish_managed_repository_legacy_reconciliation = original_finish
    assert first.deferred == 1

    await _force_reconciliation_due(db)
    settled, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert settled.scrubbed_terminal == 1
    assert settled.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        authority_status = await conn.fetchval(
            "SELECT status FROM managed_repository_authorities "
            "WHERE authority_kind='thread' AND authority_id=$1",
            thread_id,
        )
        stored_url = await conn.fetchval(
            "SELECT metadata->'workspace_container'->>'git_remote_url' "
            "FROM threads WHERE id=$1",
            thread_id,
        )
    assert authority_status == "revoked"
    assert stored_url == f"http://gitea:3000/srw/{repo_name}.git"
    gitea.delete_repo_deploy_key.assert_awaited_once()


@pytest.mark.asyncio
async def test_concurrent_officer_bind_and_thread_key_revoke_have_two_safe_outcomes(db):
    async def prepare(label: str):
        project_id = uuid4()
        thread_id = uuid4()
        repo_name = f"thread-{str(thread_id)[:8]}"
        metadata = {
            "workspace_container": {
                "repo_name": repo_name,
                "git_remote_url": f"http://gitea:3000/srw/{repo_name}.git",
                "status": "ready",
                "provisioner": "k8s",
            },
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 11,
                "cleanup_complete": True,
                "permanent": True,
                "backing_id": None,
                "runtime_incarnation": None,
                "snapshot_restore_required": False,
            },
        }
        async with db.acquire() as conn:
            await conn.execute(
                "INSERT INTO projects (id, name) VALUES ($1, $2)",
                project_id,
                f"officer bind race {label}",
            )
            async with _legacy_writes(conn), _pre_0196_thread_workspace_insert(conn):
                await conn.execute(
                    "INSERT INTO threads (id, title, project_id, status, "
                    "execution_lane, metadata) VALUES "
                    "($1, $2, $3, 'ended', 'stateless', $4::jsonb)",
                    thread_id,
                    f"officer bind race {label}",
                    project_id,
                    json.dumps(metadata),
                )
        authority = await db.reserve_managed_repository_authority(
            repository_owner="srw",
            repo_name=repo_name,
            authority_kind="thread",
            authority_id=str(thread_id),
            project_id=str(project_id),
            access_mode="write",
            creation_intent_id=None,
            clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
            public_key=f"ssh-ed25519 AAAA{label}",
            public_key_fingerprint=f"SHA256:{label}",
            private_key=f"private-{label}",
        )
        exact = {
            "repository_owner": "srw",
            "repo_name": repo_name,
            "authority_kind": "thread",
            "authority_scope_id": str(thread_id),
            "project_id": str(project_id),
            "generation": 1,
            "access_mode": "write",
            "public_key_fingerprint": f"SHA256:{label}",
        }
        await db.record_managed_repository_authority_forge_key(
            str(authority["id"]), forge_key_id=91, **exact
        )
        await db.activate_managed_repository_authority(
            str(authority["id"]), forge_key_id=91, access_mode="write"
        )
        intent = await db.upsert_managed_repository_legacy_reconciliation(
            source_kind="thread",
            source_id=str(thread_id),
            project_id=str(project_id),
            classification="terminal_historical",
            authority_kind="thread",
            authority_id=str(thread_id),
            repository_owner="srw",
            repo_name=repo_name,
            access_mode="write",
            reason_code="permanent_stateless_retirement",
            authority_record_id=str(authority["id"]),
            authority_generation=1,
        )
        claim = (
            await db.claim_managed_repository_legacy_reconciliations(
                claimant_id=str(uuid4()), limit=1, lease_seconds=30
            )
        )[0]
        assert claim["id"] == intent["id"]
        return project_id, thread_id, authority, exact, claim

    # Post binding holds a key-share authority lock before commit. The revoke
    # claimant may initially miss that uncommitted Post row, but its final
    # Post recheck after acquiring the authority lock must retain the key.
    project_id, thread_id, authority, exact, claim = await prepare("post-wins")
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO project_officers (project_id, thread_id) VALUES ($1, $2)",
                project_id,
                thread_id,
            )
            revoke_task = asyncio.create_task(
                db.claim_managed_repository_authority_revoke_exact(
                    str(claim["id"]),
                    int(claim["claim_token"]),
                    str(authority["id"]),
                    **exact,
                )
            )
            done, _ = await asyncio.wait({revoke_task}, timeout=0.2)
            assert not done
        assert await revoke_task is None
    current = await db.get_managed_repository_authority(
        exact["repo_name"], repository_owner="srw", active_only=False
    )
    assert current is not None and current["status"] == "active"
    await db.retry_managed_repository_legacy_reconciliation(
        str(claim["id"]),
        int(claim["claim_token"]),
        reason_code="officer_bind_won",
        delay_seconds=60,
    )

    # If revocation commits first, the migration trigger must reject a later
    # Officer bind rather than expose a commissioned thread without its key.
    project_id, thread_id, authority, exact, claim = await prepare("revoke-wins")
    revoked = await db.claim_managed_repository_authority_revoke_exact(
        str(claim["id"]),
        int(claim["claim_token"]),
        str(authority["id"]),
        **exact,
    )
    assert revoked is not None and revoked["status"] == "revoking"
    with pytest.raises(asyncpg.PostgresError):
        async with db.acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO project_officers (project_id, thread_id) VALUES ($1, $2)",
                project_id,
                thread_id,
            )
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM project_officers "
            "WHERE project_id=$1 AND thread_id=$2)",
            project_id,
            thread_id,
        )


@pytest.mark.asyncio
async def test_deleted_job_settles_without_revoking_shared_jobs_authority(db):
    project_id = uuid4()
    repository_id = uuid4()
    job_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-jobs"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'shared source deletion')",
            project_id,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) "
                "VALUES ($1, $2, $3, $4, 'jobs', true)",
                repository_id,
                project_id,
                repo_name,
                _legacy_url(repo_name),
            )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO jobs (id, description, status, execution_lane, "
                "project_id, repo_name, branch_name, context) VALUES "
                "($1, 'shared job deletion', 'created', 'pinned', $2, $3, "
                "'main', jsonb_build_object('git_remote_url', $4::text))",
                job_id,
                project_id,
                repo_name,
                _legacy_url(repo_name),
            )

    gitea = _gitea()
    original_finish = db.finish_managed_repository_legacy_reconciliation
    deleted = False

    async def delete_job_before_its_finish(*args, **kwargs):
        nonlocal deleted
        async with db.acquire() as conn:
            source = await conn.fetchrow(
                "SELECT source_kind, source_id FROM "
                "managed_repository_legacy_reconciliations WHERE id=$1",
                args[0],
            )
        if not deleted and source["source_kind"] == "job":
            deleted = True
            assert source["source_id"] == job_id
            assert await db.delete_job(str(job_id))
            assert await db.get_job(str(job_id)) is None
        return await original_finish(*args, **kwargs)

    db.finish_managed_repository_legacy_reconciliation = delete_job_before_its_finish
    # The job and its shared project-repository intent are discovered together.
    # Whichever claim creates the authority first may force the other to rearm
    # with that exact authority generation before its finish hook is reached.
    # Drive the bounded retry until the deletion race is actually exercised.
    for _ in range(3):
        await reconcile_managed_repository_legacy_once(
            db, gitea, apply=True, concurrency=1
        )
        if deleted:
            break
        await _force_reconciliation_due(db)
    db.finish_managed_repository_legacy_reconciliation = original_finish
    assert deleted
    assert await db.get_job(str(job_id)) is None
    async with db.acquire() as conn:
        deferred_intent = await conn.fetchrow(
            "SELECT state, result_kind, authority_kind, authority_id FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            job_id,
        )
    assert deferred_intent["state"] in {"retry", "completed"}
    assert deferred_intent["result_kind"] in {None, "source_absent"}
    assert deferred_intent["authority_kind"] == "project_repository"
    assert deferred_intent["authority_id"] == repository_id

    await _force_reconciliation_due(db)
    settled, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True
    )

    assert settled.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            job_id,
        )
        authority = await conn.fetchrow(
            "SELECT authority_kind, authority_id, status "
            "FROM managed_repository_authorities WHERE repo_name=$1",
            repo_name,
        )
    assert dict(intent) == {"state": "completed", "result_kind": "source_absent"}
    assert dict(authority) == {
        "authority_kind": "project_repository",
        "authority_id": repository_id,
        "status": "active",
    }
    gitea.delete_repo_deploy_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_keyset_scan_exhausts_observed_production_scale(db):
    total = 1_184
    job_ids = [uuid4() for _ in range(total)]
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.executemany(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'scale fixture', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            [
                (
                    job_id,
                    f"job-{str(job_id)[:8]}",
                    _legacy_url(f"job-{str(job_id)[:8]}"),
                )
                for job_id in job_ids
            ],
        )

    started = time.monotonic()
    stats, details = await reconcile_managed_repository_legacy_once(
        db, _gitea(), apply=False, page_size=37
    )
    elapsed = time.monotonic() - started
    assert stats.scanned == total
    assert stats.deferred == total
    assert stats.ambiguous == 0
    assert details["classifications"] == {"terminal_historical": total}
    assert elapsed < 30
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_legacy_reconciliations"
            )
            == 0
        )


@pytest.mark.asyncio
async def test_database_clock_expiry_blocks_settlement_before_reclaim(db, monkeypatch):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    observed = _legacy_url(repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'database clock lease', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            observed,
        )
    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)
    predecessor = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_legacy_reconciliations "
            "SET claim_expires_at=clock_timestamp() - interval '1 second' "
            "WHERE id=$1",
            predecessor["id"],
        )

    class _ExplodingHostClock:
        @classmethod
        def now(cls, *_args, **_kwargs):
            raise AssertionError("reconciliation lease consulted host clock")

    monkeypatch.setattr(postgres_module, "datetime", _ExplodingHostClock)
    assert not await db.retry_managed_repository_legacy_reconciliation(
        str(predecessor["id"]),
        int(predecessor["claim_token"]),
        reason_code="expired_worker_retry",
        delay_seconds=60,
    )
    assert not await db.mark_managed_repository_legacy_reconciliation_ambiguous(
        str(predecessor["id"]),
        int(predecessor["claim_token"]),
        reason_code="expired_worker_ambiguity",
    )
    assert (
        await db.finish_managed_repository_legacy_reconciliation(
            str(predecessor["id"]),
            int(predecessor["claim_token"]),
            observed_url=observed,
            authority_id=None,
        )
        is None
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT context->>'git_remote_url' FROM jobs WHERE id=$1", job_id
            )
            == observed
        )

    winner = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    assert winner["claim_token"] != predecessor["claim_token"]
    assert (
        await db.finish_managed_repository_legacy_reconciliation(
            str(winner["id"]),
            int(winner["claim_token"]),
            observed_url=observed,
            authority_id=None,
        )
        == "scrubbed_terminal"
    )


@pytest.mark.asyncio
async def test_failed_intent_exact_rearm_is_audited_idempotent_and_claimable(db):
    job_id = uuid4()
    actor_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    observed = _legacy_url(repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, branch_name, context) VALUES "
            "($1, 'rearm after forge outage', 'created', 'pinned', $2, 'main', "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            observed,
        )
    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)

    for attempt in range(2):
        claim = (
            await db.claim_managed_repository_legacy_reconciliations(
                claimant_id=str(uuid4()), limit=1, max_attempts=2
            )
        )[0]
        assert await db.retry_managed_repository_legacy_reconciliation(
            str(claim["id"]),
            int(claim["claim_token"]),
            reason_code="forge_temporarily_unavailable",
            delay_seconds=60,
            max_attempts=2,
        )
        if attempt == 0:
            await _force_reconciliation_due(db)

    progress = await db.get_managed_repository_legacy_reconciliation_progress()
    assert progress["rearm_required"] == [
        {
            "source_kind": "job",
            "source_id": job_id,
            "attempts": 2,
            "lifetime_attempts": 2,
            "rearm_generation": 0,
            "last_failure_reason_code": "forge_temporarily_unavailable",
        }
    ]

    rearm_one, rearm_two, raced_claim = await asyncio.gather(
        db.rearm_managed_repository_legacy_reconciliation(
            "job",
            str(job_id),
            actor_id=str(actor_id),
            reason="forge_outage_resolved",
        ),
        db.rearm_managed_repository_legacy_reconciliation(
            "job",
            str(job_id),
            actor_id=str(actor_id),
            reason="forge_outage_resolved",
        ),
        db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, max_attempts=2
        ),
    )
    assert {rearm_one["status"], rearm_two["status"]} == {
        "rearmed",
        "replayed",
    }
    claim_rows = (
        raced_claim
        or await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, max_attempts=2
        )
    )
    assert len(claim_rows) == 1
    claim = claim_rows[0]

    replay = await db.rearm_managed_repository_legacy_reconciliation(
        "job",
        str(job_id),
        actor_id=str(actor_id),
        reason="forge_outage_resolved",
    )
    assert replay["status"] == "replayed"
    conflict = await db.rearm_managed_repository_legacy_reconciliation(
        "job",
        str(job_id),
        actor_id=str(uuid4()),
        reason="different_operator_request",
    )
    assert conflict["status"] == "idempotency_conflict"

    assert await db.retry_managed_repository_legacy_reconciliation(
        str(claim["id"]),
        int(claim["claim_token"]),
        reason_code="second_window_unavailable",
        delay_seconds=60,
        max_attempts=1,
    )
    lost_response_replay = await db.rearm_managed_repository_legacy_reconciliation(
        "job",
        str(job_id),
        actor_id=str(actor_id),
        reason="forge_outage_resolved",
    )
    assert lost_response_replay["status"] == "replayed"
    assert lost_response_replay["state"] == "failed"
    second_window = await db.rearm_managed_repository_legacy_reconciliation(
        "job",
        str(job_id),
        actor_id=str(actor_id),
        reason="second_outage_resolved",
    )
    assert second_window["status"] == "rearmed"
    assert second_window["rearm_generation"] == 2
    delayed_first_window_replay = (
        await db.rearm_managed_repository_legacy_reconciliation(
            "job",
            str(job_id),
            actor_id=str(actor_id),
            reason="forge_outage_resolved",
        )
    )
    assert delayed_first_window_replay["status"] == "replayed"
    assert delayed_first_window_replay["rearm_generation"] == 1
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, max_attempts=2
        )
    )[0]

    with pytest.raises(ValueError, match="machine reason code"):
        await db.rearm_managed_repository_legacy_reconciliation(
            "job",
            str(job_id),
            actor_id=str(actor_id),
            reason="https://gitea/private/repository",
        )

    job = await db.get_job(str(job_id))
    assert job is not None
    authority = await ensure_job_primary_repository_authority(db, _gitea(), job)
    assert authority is not None
    assert (
        await db.finish_managed_repository_legacy_reconciliation(
            str(claim["id"]),
            int(claim["claim_token"]),
            observed_url=observed,
            authority_id=str(authority["id"]),
        )
        == "adopted"
    )
    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT state, attempts, lifetime_attempts, rearm_generation, "
            "claim_token FROM managed_repository_legacy_reconciliations "
            "WHERE source_kind='job' AND source_id=$1",
            job_id,
        )
        audits = await conn.fetch(
            "SELECT generation, actor_id, reason_code, attempts_in_generation, "
            "lifetime_attempts, failure_reason_code "
            "FROM managed_repository_legacy_reconciliation_rearms "
            "WHERE reconciliation_id=$1 ORDER BY generation",
            claim["id"],
        )
    assert dict(intent) == {
        "state": "completed",
        "attempts": 1,
        "lifetime_attempts": 4,
        "rearm_generation": 2,
        "claim_token": claim["claim_token"],
    }
    assert [dict(row) for row in audits] == [
        {
            "generation": 1,
            "actor_id": actor_id,
            "reason_code": "forge_outage_resolved",
            "attempts_in_generation": 2,
            "lifetime_attempts": 2,
            "failure_reason_code": "forge_temporarily_unavailable",
        },
        {
            "generation": 2,
            "actor_id": actor_id,
            "reason_code": "second_outage_resolved",
            "attempts_in_generation": 1,
            "lifetime_attempts": 3,
            "failure_reason_code": "second_window_unavailable",
        },
    ]
    with pytest.raises(asyncpg.PostgresError, match="re-arms are append-only"):
        async with db.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE managed_repository_legacy_reconciliation_rearms "
                "SET reason_code='tampered' WHERE reconciliation_id=$1",
                claim["id"],
            )


@pytest.mark.asyncio
async def test_claim_liveness_and_exact_forge_key_recovery_are_generation_fenced(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    observed = _legacy_url(repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'exact key recovery', 'created', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            observed,
        )
    await scan_managed_repository_legacy_sources(db, _gitea(), apply=True)
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    assert await db.managed_repository_legacy_reconciliation_claim_is_current(
        str(claim["id"]), int(claim["claim_token"])
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_legacy_reconciliations "
            "SET claim_expires_at=clock_timestamp() - interval '1 second' "
            "WHERE id=$1",
            claim["id"],
        )
    assert not await db.managed_repository_legacy_reconciliation_claim_is_current(
        str(claim["id"]), int(claim["claim_token"])
    )

    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
        public_key="ssh-ed25519 AAAAexactkey",
        public_key_fingerprint="SHA256:exact-key-generation-one",
        private_key="private-generation-one",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(job_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:exact-key-generation-one",
    }
    assert (
        await db.record_managed_repository_authority_forge_key(
            str(authority["id"]), forge_key_id=91, **exact
        )
    )["forge_key_id"] == 91
    assert (
        await db.record_managed_repository_authority_forge_key(
            str(authority["id"]), forge_key_id=91, **exact
        )
    )["forge_key_id"] == 91
    with pytest.raises(RuntimeError, match="deploy-key identity changed"):
        await db.record_managed_repository_authority_forge_key(
            str(authority["id"]), forge_key_id=92, **exact
        )
    assert (
        await db.record_managed_repository_authority_forge_key(
            str(authority["id"]),
            forge_key_id=91,
            **{**exact, "generation": 2},
        )
        is None
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
    cleanup_intent = await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="job",
        source_id=str(job_id),
        project_id=None,
        classification="terminal_historical",
        authority_kind="job",
        authority_id=str(job_id),
        repository_owner="srw",
        repo_name=repo_name,
        access_mode="write",
        reason_code="active_authority_terminal",
        authority_record_id=str(authority["id"]),
        authority_generation=1,
    )
    cleanup_claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    assert cleanup_claim["id"] == cleanup_intent["id"]
    cleanup_binding = {
        "reconciliation_id": str(cleanup_claim["id"]),
        "claim_token": int(cleanup_claim["claim_token"]),
    }
    child_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "parent_job_id, repo_name, context) VALUES "
            "($1, 'resumable descendant', 'failed', 'pinned', $2, $3, '{}'::jsonb)",
            child_id,
            job_id,
            repo_name,
        )
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding, authority_id=str(authority["id"]), **exact
        )
        is None
    )
    live_before_child_settlement = (
        await db.list_managed_repository_legacy_active_authority_candidates(limit=10)
    )
    root_candidate = next(
        item
        for item in live_before_child_settlement
        if item["authority_record_id"] == authority["id"]
    )
    assert root_candidate["containment_candidate"] is False
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", child_id)
        await conn.execute(
            "UPDATE jobs SET repo_name='corrupt-descendant' WHERE id=$1", child_id
        )
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding, authority_id=str(authority["id"]), **exact
        )
        is None
    )
    foreign_project = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'foreign descendant')",
            foreign_project,
        )
        await conn.execute(
            "UPDATE jobs SET repo_name=$2, project_id=$3 WHERE id=$1",
            child_id,
            repo_name,
            foreign_project,
        )
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding, authority_id=str(authority["id"]), **exact
        )
        is None
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET project_id=NULL WHERE id=$1", child_id)
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2 WHERE id=$1", job_id, child_id
        )
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding, authority_id=str(authority["id"]), **exact
        )
        is None
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET parent_job_id=NULL WHERE id=$1", job_id)
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding,
            authority_id=str(authority["id"]),
            **{**exact, "repository_owner": "foreign"},
        )
        is None
    )
    revoke = await db.claim_managed_repository_authority_revoke_exact(
        **cleanup_binding, authority_id=str(authority["id"]), **exact
    )
    assert revoke is not None
    assert revoke["status"] == "revoking"
    assert revoke["forge_key_id"] == 91
    assert await db.finish_managed_repository_authority_revoke(str(authority["id"]))

    successor = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
        public_key="ssh-ed25519 AAAArecoveredkey",
        public_key_fingerprint="SHA256:exact-key-generation-two",
        private_key="private-generation-two",
    )
    second_job_id = uuid4()
    second_repo_name = f"job-{str(second_job_id)[:8]}"
    second_observed = _legacy_url(second_repo_name)
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'lost registration response', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            second_job_id,
            second_repo_name,
            second_observed,
        )
    recovering_authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=second_repo_name,
        authority_kind="job",
        authority_id=str(second_job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{second_repo_name}.git",
        public_key="ssh-ed25519 AAAAlostresponse",
        public_key_fingerprint="SHA256:lost-registration-response",
        private_key="private-lost-response",
    )
    live_candidates = (
        await db.list_managed_repository_legacy_active_authority_candidates(limit=10)
    )
    assert {item["authority_record_id"] for item in live_candidates} == {
        successor["id"],
        recovering_authority["id"],
    }
    assert all(item["containment_candidate"] for item in live_candidates)
    assert all(
        item["containment_reason"] == "job_lineage_terminal" for item in live_candidates
    )
    assert all(
        "private_key_ciphertext" not in item
        and "private_key" not in item
        and "clean_repo_url" not in item
        for item in live_candidates
    )
    first_page = await db.list_managed_repository_legacy_active_authority_candidates(
        limit=1
    )
    second_page = await db.list_managed_repository_legacy_active_authority_candidates(
        after_kind=str(first_page[0]["source_kind"]),
        after_id=str(first_page[0]["authority_record_id"]),
        limit=1,
    )
    assert first_page[0]["authority_record_id"] != second_page[0]["authority_record_id"]
    recovering_exact = {
        "repository_owner": "srw",
        "repo_name": second_repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(second_job_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:lost-registration-response",
    }
    recovering_intent = await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="job",
        source_id=str(second_job_id),
        project_id=None,
        classification="terminal_historical",
        authority_kind="job",
        authority_id=str(second_job_id),
        repository_owner="srw",
        repo_name=second_repo_name,
        access_mode="write",
        reason_code="active_authority_terminal",
        authority_record_id=str(recovering_authority["id"]),
        authority_generation=1,
    )
    recovering_claim = await db.claim_managed_repository_legacy_reconciliations(
        claimant_id=str(uuid4()), limit=10, lease_seconds=30
    )
    recovering_claim = next(
        item for item in recovering_claim if item["id"] == recovering_intent["id"]
    )
    recovering = await db.claim_managed_repository_authority_revoke_exact(
        str(recovering_claim["id"]),
        int(recovering_claim["claim_token"]),
        str(recovering_authority["id"]),
        **recovering_exact,
    )
    assert recovering is not None
    assert recovering["status"] == "revoking"
    assert recovering["forge_key_id"] is None
    recovered = await db.record_managed_repository_authority_forge_key(
        str(recovering_authority["id"]), forge_key_id=93, **recovering_exact
    )
    assert recovered is not None
    assert recovered["status"] == "revoking"
    assert recovered["forge_key_id"] == 93
    assert (
        await db.claim_managed_repository_authority_revoke_exact(
            **cleanup_binding, authority_id=str(authority["id"]), **exact
        )
        is None
    )
    current = await db.get_managed_repository_authority(
        repo_name, repository_owner="srw", active_only=False
    )
    assert current is not None
    assert current["id"] == successor["id"]
    assert current["status"] == "provisioning"


@pytest.mark.asyncio
async def test_terminal_read_project_authority_can_be_exactly_contained(db):
    project_id = uuid4()
    repository_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-reference"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'read authority cleanup')",
            project_id,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, read_only, is_managed) "
                "VALUES ($1, $2, $3, $4, 'reference', true, true)",
                repository_id,
                project_id,
                repo_name,
                _legacy_url(repo_name),
            )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="project_repository",
        authority_id=str(repository_id),
        project_id=str(project_id),
        access_mode="read",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
        public_key="ssh-ed25519 AAAAreadcleanup",
        public_key_fingerprint="SHA256:read-cleanup",
        private_key="private-read-cleanup",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "project_repository",
        "authority_scope_id": str(repository_id),
        "project_id": str(project_id),
        "generation": 1,
        "access_mode": "read",
        "public_key_fingerprint": "SHA256:read-cleanup",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=94, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=94, access_mode="read"
    )
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "UPDATE project_repositories SET role='knowledge', read_only=false "
            "WHERE id=$1",
            repository_id,
        )
    candidate = next(
        item
        for item in await db.list_managed_repository_legacy_active_authority_candidates(
            limit=10
        )
        if item["authority_record_id"] == authority["id"]
    )
    assert candidate["containment_candidate"] is True
    intent = await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="project_repository",
        source_id=str(repository_id),
        project_id=str(project_id),
        classification="terminal_historical",
        authority_kind="project_repository",
        authority_id=str(repository_id),
        repository_owner="srw",
        repo_name=repo_name,
        access_mode="read",
        reason_code=str(candidate["containment_reason"]),
        authority_record_id=str(authority["id"]),
        authority_generation=1,
    )
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1
        )
    )[0]
    assert claim["id"] == intent["id"]
    revoked = await db.claim_managed_repository_authority_revoke_exact(
        str(claim["id"]),
        int(claim["claim_token"]),
        str(authority["id"]),
        **exact,
    )
    assert revoked is not None
    assert revoked["status"] == "revoking"
    assert await db.finish_managed_repository_authority_revoke(str(authority["id"]))
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    assert (
        await db.finish_missing_or_clean_managed_repository_legacy_reconciliation(
            str(claim["id"]), int(claim["claim_token"])
        )
        == "authority_revoked"
    )
    # A committed settlement response may be lost; the exact claim token
    # replays without another source or authority mutation.
    assert (
        await db.finish_missing_or_clean_managed_repository_legacy_reconciliation(
            str(claim["id"]), int(claim["claim_token"])
        )
        == "authority_revoked"
    )
    async with db.acquire() as conn:
        settled = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations WHERE id=$1",
            claim["id"],
        )
        stored_url = await conn.fetchval(
            "SELECT repo_url FROM project_repositories WHERE id=$1",
            repository_id,
        )
    assert dict(settled) == {
        "state": "completed",
        "result_kind": "authority_revoked",
    }
    assert stored_url == clean_url


@pytest.mark.asyncio
async def test_expired_revoke_claim_cannot_transition_after_authority_lock_wait(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'lease lock wait', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            job_id,
            repo_name,
            clean_url,
        )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=clean_url,
        public_key="ssh-ed25519 AAAAleasewait",
        public_key_fingerprint="SHA256:lease-wait",
        private_key="private-lease-wait",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(job_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:lease-wait",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=96, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=96, access_mode="write"
    )
    intent = await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="job",
        source_id=str(job_id),
        project_id=None,
        classification="terminal_historical",
        authority_kind="job",
        authority_id=str(job_id),
        repository_owner="srw",
        repo_name=repo_name,
        access_mode="write",
        reason_code="active_authority_terminal",
        authority_record_id=str(authority["id"]),
        authority_generation=1,
    )
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1, lease_seconds=30
        )
    )[0]
    assert claim["id"] == intent["id"]

    blocker = await db._pool.acquire()
    transaction = blocker.transaction()
    await transaction.start()
    transaction_finished = False
    try:
        await blocker.fetchrow(
            "SELECT id FROM managed_repository_authorities WHERE id=$1 FOR UPDATE",
            authority["id"],
        )
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE managed_repository_legacy_reconciliations "
                "SET claim_expires_at=clock_timestamp() + interval '250 milliseconds' "
                "WHERE id=$1",
                claim["id"],
            )
        pending = asyncio.create_task(
            db.claim_managed_repository_authority_revoke_exact(
                str(claim["id"]),
                int(claim["claim_token"]),
                str(authority["id"]),
                **exact,
            )
        )
        await asyncio.sleep(0.4)
        await transaction.commit()
        transaction_finished = True
        assert await asyncio.wait_for(pending, timeout=5) is None
    finally:
        if not transaction_finished:
            await transaction.rollback()
        await db._pool.release(blocker)
    current = await db.get_managed_repository_authority(
        repo_name, repository_owner="srw", active_only=False
    )
    assert current is not None
    assert current["status"] == "active"


@pytest.mark.asyncio
async def test_lineage_mutex_exposes_committed_grandchild_before_revoke(db):
    root_id = uuid4()
    child_id = uuid4()
    grandchild_id = uuid4()
    repo_name = f"job-{str(root_id)[:8]}"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "repo_name, context) VALUES "
            "($1, 'lineage root', 'completed', 'pinned', $2, "
            "jsonb_build_object('git_remote_url', $3::text))",
            root_id,
            repo_name,
            clean_url,
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "parent_job_id, repo_name, context) VALUES "
            "($1, 'lineage child', 'completed', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            child_id,
            root_id,
            repo_name,
            clean_url,
        )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(root_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=clean_url,
        public_key="ssh-ed25519 AAAAlineagemutex",
        public_key_fingerprint="SHA256:lineage-mutex",
        private_key="private-lineage-mutex",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_scope_id": str(root_id),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "public_key_fingerprint": "SHA256:lineage-mutex",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=97, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=97, access_mode="write"
    )
    intent = await db.upsert_managed_repository_legacy_reconciliation(
        source_kind="job",
        source_id=str(root_id),
        project_id=None,
        classification="terminal_historical",
        authority_kind="job",
        authority_id=str(root_id),
        repository_owner="srw",
        repo_name=repo_name,
        access_mode="write",
        reason_code="active_authority_terminal",
        authority_record_id=str(authority["id"]),
        authority_generation=1,
    )
    claim = (
        await db.claim_managed_repository_legacy_reconciliations(
            claimant_id=str(uuid4()), limit=1
        )
    )[0]
    assert claim["id"] == intent["id"]

    inserter = await db._pool.acquire()
    insert_tx = inserter.transaction()
    await insert_tx.start()
    insert_finished = False
    try:
        await inserter.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "parent_job_id, repo_name, context) VALUES "
            "($1, 'racing grandchild', 'failed', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            grandchild_id,
            child_id,
            repo_name,
            clean_url,
        )
        pending = asyncio.create_task(
            db.claim_managed_repository_authority_revoke_exact(
                str(claim["id"]),
                int(claim["claim_token"]),
                str(authority["id"]),
                **exact,
            )
        )
        await asyncio.sleep(0.1)
        assert not pending.done()
        await insert_tx.commit()
        insert_finished = True
        assert await asyncio.wait_for(pending, timeout=5) is None
    finally:
        if not insert_finished:
            await insert_tx.rollback()
        await db._pool.release(inserter)
    current = await db.get_managed_repository_authority(
        repo_name, repository_owner="srw", active_only=False
    )
    assert current is not None
    assert current["status"] == "active"


@pytest.mark.asyncio
async def test_changed_credentialed_project_authority_converges_end_to_end(db):
    project_id = uuid4()
    repository_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-reference"
    legacy_url = _legacy_url(repo_name)
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'mode drift cleanup')",
            project_id,
        )
        async with _legacy_writes(conn):
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, read_only, is_managed) "
                "VALUES ($1, $2, $3, $4, 'reference', true, true)",
                repository_id,
                project_id,
                repo_name,
                legacy_url,
            )
    authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="project_repository",
        authority_id=str(repository_id),
        project_id=str(project_id),
        access_mode="read",
        creation_intent_id=None,
        clean_repo_url=clean_url,
        public_key="ssh-ed25519 AAAAmodechange",
        public_key_fingerprint="SHA256:mode-change",
        private_key="private-mode-change",
    )
    exact = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "project_repository",
        "authority_scope_id": str(repository_id),
        "project_id": str(project_id),
        "generation": 1,
        "access_mode": "read",
        "public_key_fingerprint": "SHA256:mode-change",
    }
    await db.record_managed_repository_authority_forge_key(
        str(authority["id"]), forge_key_id=95, **exact
    )
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=95, access_mode="read"
    )
    # Model an old replica changing repository policy without first rotating
    # the scoped key. The credential-bearing coordinate intentionally remains.
    async with db.acquire() as conn, _legacy_writes(conn):
        await conn.execute(
            "UPDATE project_repositories SET role='knowledge', read_only=false "
            "WHERE id=$1",
            repository_id,
        )
    gitea = _gitea()

    stats, details = await reconcile_managed_repository_legacy_once(
        db, gitea, apply=True, concurrency=1
    )

    assert stats.scrubbed_terminal == 1
    assert stats.ambiguous == 0
    assert not details["progress"]["ambiguous"]
    async with db.acquire() as conn:
        repository = await conn.fetchrow(
            "SELECT repo_url, role, read_only FROM project_repositories WHERE id=$1",
            repository_id,
        )
        stored_authority = await conn.fetchrow(
            "SELECT status FROM managed_repository_authorities WHERE id=$1",
            authority["id"],
        )
        intent = await conn.fetchrow(
            "SELECT state, result_kind FROM "
            "managed_repository_legacy_reconciliations "
            "WHERE source_kind='project_repository' AND source_id=$1",
            repository_id,
        )
    assert dict(repository) == {
        "repo_url": clean_url,
        "role": "knowledge",
        "read_only": False,
    }
    assert dict(stored_authority) == {"status": "revoked"}
    assert dict(intent) == {
        "state": "completed",
        "result_kind": "authority_revoked",
    }
    gitea.delete_repo_deploy_key.assert_awaited_once_with(repo_name, 95)
