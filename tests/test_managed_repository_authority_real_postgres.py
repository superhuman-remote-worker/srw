"""Real-PostgreSQL proofs for migration 0176 and legacy URL adoption."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    _deploy_keypair,
    authorize_job_repository_transport,
    create_managed_repository,
    ensure_job_primary_repository_authority,
    ensure_project_repository_authority,
    prepare_job_primary_repository_authority,
    prepare_job_repository_authority,
    prepare_project_repository_authority,
    prepare_thread_repository_authority,
    revoke_and_delete_managed_repository,
    rotate_project_repository_authority,
)
from tests._previous_release_seed import (
    PINNED_BINDING_AUTHORITY_TRIGGERS,
    previous_release_writer,
)

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


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
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "G" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=8,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE managed_repository_authorities, "
            "managed_repository_creation_intents, run_queue, "
            "project_repositories, jobs, threads, agents, project_members, "
            "projects, users CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()
        crypto.reset_cipher_cache()


def _gitea(*, probe: bool = True) -> MagicMock:
    client = MagicMock()
    client.repository_owner = "srw"
    client.is_initialized = True
    client.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://gitea:3000/srw/{name}.git"
    )
    client.ensure_repo_deploy_key = AsyncMock(return_value=91)
    client.probe_repo_deploy_key = AsyncMock(return_value=probe)
    client.delete_repo_deploy_key = AsyncMock(return_value=True)
    client.delete_repo = AsyncMock(return_value=True)
    return client


async def _reserve(db: PostgresDB, *, repo_name: str, authority_id: UUID) -> dict:
    private_key, public_key, fingerprint = _deploy_keypair()
    return await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(authority_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
        public_key=public_key,
        public_key_fingerprint=fingerprint,
        private_key=private_key,
    )


@pytest.mark.asyncio
async def test_concurrent_reservation_has_one_encrypted_authority_generation(db):
    job_id = UUID(str((await db.create_job("concurrent repository reservation"))["id"]))
    repo_name = f"job-{str(job_id)[:8]}"

    first, second = await asyncio.gather(
        _reserve(db, repo_name=repo_name, authority_id=job_id),
        _reserve(db, repo_name=repo_name, authority_id=job_id),
    )

    assert first["id"] == second["id"]
    assert first["private_key"] == second["private_key"]
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT private_key_ciphertext, public_key, generation, status "
            "FROM managed_repository_authorities WHERE repo_name=$1",
            repo_name,
        )
    assert len(rows) == 1
    assert rows[0]["generation"] == 1
    assert rows[0]["status"] == "provisioning"
    assert rows[0]["private_key_ciphertext"].startswith("v1:")
    assert first["private_key"] not in rows[0]["private_key_ciphertext"]


@pytest.mark.asyncio
async def test_genuine_legacy_admin_url_scrubs_only_after_key_is_proven(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        # This reproduces the row shape written by the release immediately
        # before 0176. The trigger is disabled only while constructing history;
        # product adoption itself runs with every fence enabled.
        await conn.execute(
            "ALTER TABLE jobs DISABLE TRIGGER trg_managed_job_repository_url_authority"
        )
        try:
            await conn.execute(
                "INSERT INTO jobs (id, description, status, execution_lane, "
                "repo_name, context) VALUES ($1, 'legacy managed repo', "
                "'created', 'pinned', $2, jsonb_build_object("
                "'git_remote_url', $3::text))",
                job_id,
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE jobs ENABLE TRIGGER "
                "trg_managed_job_repository_url_authority"
            )

    refused = _gitea(probe=False)
    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        await prepare_job_repository_authority(
            db, refused, await db.get_job(str(job_id))
        )
    assert exc.value.code == "repository_key_unproven"
    unchanged = await db.get_job(str(job_id))
    assert _json(unchanged["context"])["git_remote_url"] == legacy_url
    async with db.acquire() as conn:
        unproven = await conn.fetchrow(
            "SELECT status, forge_key_id FROM managed_repository_authorities "
            "WHERE authority_kind='job' AND authority_id=$1",
            job_id,
        )
    assert dict(unproven) == {"status": "provisioning", "forge_key_id": 91}

    accepted = _gitea(probe=True)
    authority = await prepare_job_repository_authority(
        db, accepted, await db.get_job(str(job_id))
    )
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    adopted = await db.get_job(str(job_id))
    assert authority["status"] == "active"
    assert _json(adopted["context"])["git_remote_url"] == clean_url
    assert "shared-secret" not in _json(adopted["context"])["git_remote_url"]

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='processing' WHERE id=$1",
            job_id,
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as refused_update:
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context, "
                "'{git_remote_url}', to_jsonb($2::text)) WHERE id=$1",
                job_id,
                "http://gitea:3000/srw/foreign-project.git",
            )
        assert (
            refused_update.value.constraint_name
            == "job_dispatch_requires_repository_authority"
        )


@pytest.mark.asyncio
async def test_genuine_legacy_thread_url_scrubs_only_after_key_is_proven(db):
    thread_id = await db.create_thread()
    repo_name = f"thread-{thread_id[:8]}"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "ALTER TABLE threads DISABLE TRIGGER "
            "trg_managed_thread_repository_url_authority"
        )
        try:
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_build_object("
                "'workspace_container', jsonb_build_object("
                "'repo_name', $2::text, 'git_remote_url', $3::text)) "
                "WHERE id=$1",
                UUID(thread_id),
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE threads ENABLE TRIGGER "
                "trg_managed_thread_repository_url_authority"
            )

    refused = _gitea(probe=False)
    with pytest.raises(ManagedRepositoryAuthorityError):
        await prepare_thread_repository_authority(
            db, refused, await db.get_thread(thread_id)
        )
    unchanged = await db.get_thread(thread_id)
    assert (
        _json(unchanged["metadata"])["workspace_container"]["git_remote_url"]
        == legacy_url
    )

    accepted = _gitea(probe=True)
    await prepare_thread_repository_authority(
        db, accepted, await db.get_thread(thread_id)
    )
    adopted = await db.get_thread(thread_id)
    clean_url = _json(adopted["metadata"])["workspace_container"]["git_remote_url"]
    assert clean_url == f"http://gitea:3000/srw/{repo_name}.git"
    assert "shared-secret" not in clean_url


@pytest.mark.asyncio
async def test_legacy_thread_can_detach_but_cannot_reattach_before_adoption(db):
    """0177 removes runtime authority without weakening the attach fence."""

    thread_id = await db.create_thread()
    repo_name = f"thread-{thread_id[:8]}"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    old_agent_id = uuid4()
    replacement_agent_id = uuid4()
    old_attach_token = uuid4()
    replacement_attach_token = uuid4()

    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents "
            "(id, config_name, hostname, status, agent_mode) "
            "VALUES ($1, 'session_base', 'legacy-thread-pod', 'session', "
            "'persistent')",
            old_agent_id,
        )
        # A pinned thread that was already attached when 0200 landed. Its
        # planned -> protected bind edge did not exist when this row was
        # written, and the subject here is repository authority above it.
        async with previous_release_writer(
            conn, "threads", *PINNED_BINDING_AUTHORITY_TRIGGERS
        ):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE threads SET agent_id=$2, runtime_attach_token=$3 "
                    "WHERE id=$1",
                    UUID(thread_id),
                    old_agent_id,
                    old_attach_token,
                )
                await conn.execute(
                    "UPDATE agents SET thread_id=$2 WHERE id=$1",
                    old_agent_id,
                    UUID(thread_id),
                )
        # Exact pre-0176 durable shape: an already-attached persistent thread
        # held the administrator-bearing clone URL in workspace_container.
        await conn.execute(
            "ALTER TABLE threads DISABLE TRIGGER "
            "trg_managed_thread_repository_url_authority"
        )
        try:
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_build_object("
                "'workspace_container', jsonb_build_object("
                "'repo_name', $2::text, 'git_remote_url', $3::text)) "
                "WHERE id=$1",
                UUID(thread_id),
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE threads ENABLE TRIGGER "
                "trg_managed_thread_repository_url_authority"
            )

        # Detach is a reduction of authority and must stay available even
        # before Gitea/key adoption can run.
        async with conn.transaction():
            assert (
                await conn.execute(
                    "UPDATE threads SET agent_id=NULL, runtime_attach_token=NULL "
                    "WHERE id=$1 AND agent_id=$2",
                    UUID(thread_id),
                    old_agent_id,
                )
                == "UPDATE 1"
            )
            assert (
                await conn.execute(
                    "UPDATE agents SET thread_id=NULL WHERE id=$1 AND thread_id=$2",
                    old_agent_id,
                    UUID(thread_id),
                )
                == "UPDATE 1"
            )
        stored = await conn.fetchrow(
            "SELECT agent_id, metadata FROM threads WHERE id=$1", UUID(thread_id)
        )
        assert stored["agent_id"] is None
        assert (
            _json(stored["metadata"])["workspace_container"]["git_remote_url"]
            == legacy_url
        )

        await conn.execute(
            "INSERT INTO agents "
            "(id, config_name, hostname, status, agent_mode) "
            "VALUES ($1, 'session_base', 'replacement-thread-pod', 'session', "
            "'persistent')",
            replacement_agent_id,
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as attach:
            await conn.execute(
                "UPDATE threads SET agent_id=$2, runtime_attach_token=$3 WHERE id=$1",
                UUID(thread_id),
                replacement_agent_id,
                replacement_attach_token,
            )
        assert (
            attach.value.constraint_name
            == "managed_repository_url_must_be_credential_free"
        )

    await prepare_thread_repository_authority(
        db, _gitea(probe=True), await db.get_thread(thread_id)
    )
    async with db.acquire() as conn:
        # 0176's repository fence is the subject; 0200's separate pinned
        # protection edge has its own proofs and is not what this rebind is
        # demonstrating.
        async with previous_release_writer(
            conn, "threads", *PINNED_BINDING_AUTHORITY_TRIGGERS
        ):
            async with conn.transaction():
                assert (
                    await conn.execute(
                        "UPDATE threads SET agent_id=$2, runtime_attach_token=$3 "
                        "WHERE id=$1",
                        UUID(thread_id),
                        replacement_agent_id,
                        replacement_attach_token,
                    )
                    == "UPDATE 1"
                )
                assert (
                    await conn.execute(
                        "UPDATE agents SET thread_id=$2 WHERE id=$1",
                        replacement_agent_id,
                        UUID(thread_id),
                    )
                    == "UPDATE 1"
                )
        clean_url = await conn.fetchval(
            "SELECT metadata->'workspace_container'->>'git_remote_url' "
            "FROM threads WHERE id=$1",
            UUID(thread_id),
        )
    assert clean_url == f"http://gitea:3000/srw/{repo_name}.git"
    assert "shared-secret" not in clean_url


@pytest.mark.asyncio
async def test_genuine_legacy_project_repository_scrubs_after_proof(db, monkeypatch):
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    project_id = uuid4()
    repository_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-source"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'legacy repository')",
            project_id,
        )
        await conn.execute(
            "ALTER TABLE project_repositories DISABLE TRIGGER "
            "trg_managed_project_repository_url_authority"
        )
        try:
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) "
                "VALUES ($1, $2, $3, $4, 'source', true)",
                repository_id,
                project_id,
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE project_repositories ENABLE TRIGGER "
                "trg_managed_project_repository_url_authority"
            )
    repository = await db.get_project_repository(str(repository_id))
    job = {"id": str(uuid4()), "project_id": str(project_id), "context": {}}

    with pytest.raises(ManagedRepositoryAuthorityError):
        await authorize_job_repository_transport(
            db, _gitea(probe=False), job, [repository], backend="sandbox"
        )
    assert (await db.get_project_repository(str(repository_id)))[
        "repo_url"
    ] == legacy_url

    _primary, _repositories, payloads = await authorize_job_repository_transport(
        db, _gitea(probe=True), job, [repository], backend="sandbox"
    )
    adopted = await db.get_project_repository(str(repository_id))
    assert adopted["repo_url"] == f"http://gitea:3000/srw/{repo_name}.git"
    assert "shared-secret" not in adopted["repo_url"]
    assert len(payloads) == 1


@pytest.mark.asyncio
async def test_old_dispatcher_cannot_claim_historical_managed_source_before_adoption(
    db,
):
    project_id = uuid4()
    repository_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-source"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'rolling source project')",
            project_id,
        )
        await conn.execute(
            "ALTER TABLE project_repositories DISABLE TRIGGER "
            "trg_managed_project_repository_url_authority"
        )
        try:
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) "
                "VALUES ($1, $2, $3, $4, 'source', true)",
                repository_id,
                project_id,
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE project_repositories ENABLE TRIGGER "
                "trg_managed_project_repository_url_authority"
            )
    job = await db.create_job(
        "old dispatcher must fail closed",
        project_id=str(project_id),
    )

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as refused:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1",
                job["id"],
            )
    assert (
        refused.value.constraint_name
        == "job_dispatch_requires_project_repository_authority"
    )

    repository = await db.get_project_repository(str(repository_id))
    await prepare_project_repository_authority(db, _gitea(), repository)
    adopted = await db.get_project_repository(str(repository_id))
    assert adopted["repo_url"] == f"http://gitea:3000/srw/{repo_name}.git"
    assert "shared-secret" not in adopted["repo_url"]
    async with db.acquire() as conn:
        assert (
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1",
                job["id"],
            )
            == "UPDATE 1"
        )


@pytest.mark.asyncio
async def test_previous_release_shared_jobs_root_is_fenced_then_exactly_adopted(
    db, monkeypatch
):
    """Use only fields the pre-0176 release wrote for shared project jobs."""

    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    project_id = uuid4()
    repository_id = uuid4()
    job_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-jobs"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'historical jobs')",
            project_id,
        )
        await conn.execute(
            "ALTER TABLE project_repositories DISABLE TRIGGER "
            "trg_managed_project_repository_url_authority"
        )
        try:
            await conn.execute(
                "INSERT INTO project_repositories "
                "(id, project_id, name, repo_url, role, is_managed) "
                "VALUES ($1, $2, $3, $4, 'jobs', true)",
                repository_id,
                project_id,
                repo_name,
                legacy_url,
            )
        finally:
            await conn.execute(
                "ALTER TABLE project_repositories ENABLE TRIGGER "
                "trg_managed_project_repository_url_authority"
            )
        # Exact old row: project + branch, no repo_name, no job-level Git URL,
        # and no post-0176 server authority fields.
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, branch_name, context) VALUES "
            "($1, 'shared jobs root', 'created', 'pinned', $2, 'main', '{}')",
            job_id,
            project_id,
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as refused:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
        assert (
            refused.value.constraint_name
            == "job_dispatch_requires_project_repository_authority"
        )

    authority = await prepare_job_primary_repository_authority(
        db, _gitea(), await db.get_job(str(job_id))
    )
    assert authority is not None
    assert authority["authority_kind"] == "project_repository"
    assert authority["authority_id"] == repository_id
    assert authority["project_id"] == project_id
    assert authority["access_mode"] == "write"
    repository = await db.get_project_repository(str(repository_id))
    assert repository["repo_url"] == clean_url
    adopted_job = await db.get_job(str(job_id))
    assert adopted_job["repo_name"] is None
    assert "git_remote_url" not in _json(adopted_job["context"])

    primary_url, rendered, payloads = await authorize_job_repository_transport(
        db, _gitea(), adopted_job, [repository], backend="sandbox"
    )
    assert primary_url == payloads[0]["clone_url"]
    assert payloads[0]["authority_id"] == str(authority["id"])
    assert payloads[0]["access_mode"] == "write"
    assert rendered[0]["repo_url"] == primary_url
    assert "@" not in primary_url
    assert "shared-secret" not in json.dumps(rendered, default=str)
    async with db.acquire() as conn:
        assert (
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
            == "UPDATE 1"
        )


@pytest.mark.asyncio
async def test_historical_subjob_resolves_parent_shared_jobs_authority(db):
    project_id = uuid4()
    repository_id = uuid4()
    parent_id = uuid4()
    child_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-jobs"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'subjob history')",
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, is_managed) "
            "VALUES ($1, $2, $3, $4, 'jobs', true)",
            repository_id,
            project_id,
            repo_name,
            clean_url,
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, branch_name, context) VALUES "
            "($1, 'old parent', 'created', 'pinned', $2, 'main', '{}')",
            parent_id,
            project_id,
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, parent_job_id, branch_name, context) VALUES "
            "($1, 'old child', 'created', 'pinned', $2, $3, "
            "'subjob/old/tester', '{}')",
            child_id,
            project_id,
            parent_id,
        )

    parent = await db.get_job(str(parent_id))
    child = await db.get_job(str(child_id))
    parent_authority = await ensure_job_primary_repository_authority(
        db, _gitea(), parent
    )
    child_authority = await ensure_job_primary_repository_authority(db, _gitea(), child)
    assert parent_authority is not None
    assert child_authority is not None
    assert child_authority["id"] == parent_authority["id"]
    assert child_authority["authority_id"] == repository_id


@pytest.mark.asyncio
async def test_historical_subjob_cannot_select_a_foreign_project_jobs_authority(db):
    parent_project_id = uuid4()
    child_project_id = uuid4()
    parent_id = uuid4()
    child_id = uuid4()
    child_repository_id = uuid4()
    child_repo_name = f"project-{str(child_project_id)[:8]}-jobs"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'parent scope'), "
            "($2, 'foreign child scope')",
            parent_project_id,
            child_project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, is_managed) "
            "VALUES ($1, $2, $3, $4, 'jobs', true)",
            child_repository_id,
            child_project_id,
            child_repo_name,
            f"http://gitea:3000/srw/{child_repo_name}.git",
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, branch_name, context) VALUES "
            "($1, 'old parent', 'created', 'pinned', $2, 'main', '{}'), "
            "($3, 'mismatched child', 'created', 'pinned', $4, "
            "'subjob/foreign/tester', '{}')",
            parent_id,
            parent_project_id,
            child_id,
            child_project_id,
        )
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$1 WHERE id=$2", parent_id, child_id
        )

    with pytest.raises(ManagedRepositoryAuthorityError) as refused:
        await ensure_job_primary_repository_authority(
            db, _gitea(), await db.get_job(str(child_id))
        )
    assert refused.value.code == "job_repository_project_mismatch"
    assert (
        await db.get_managed_repository_authority(
            child_repo_name, repository_owner="srw"
        )
        is None
    )


@pytest.mark.asyncio
async def test_non_jobs_project_repository_cannot_become_job_primary(db):
    project_id = uuid4()
    repository_id = uuid4()
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'separate source scope')",
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, is_managed) "
            "VALUES ($1, $2, $3, $4, 'source', true)",
            repository_id,
            project_id,
            repo_name,
            clean_url,
        )
    source_authority = await ensure_project_repository_authority(
        db,
        _gitea(),
        await db.get_project_repository(str(repository_id)),
    )
    assert source_authority is not None
    assert source_authority["authority_kind"] == "project_repository"

    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, repo_name, context) VALUES "
            "($1, 'source name collision', 'created', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            job_id,
            project_id,
            repo_name,
            clean_url,
        )

    with pytest.raises(ManagedRepositoryAuthorityError) as refused:
        await ensure_job_primary_repository_authority(
            db, _gitea(), await db.get_job(str(job_id))
        )
    assert refused.value.code == "repository_scope_ambiguous"
    assert not await db.bind_job_managed_repository(
        str(job_id), repo_name=repo_name, clean_url=clean_url
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as transition:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
        assert (
            transition.value.constraint_name
            == "job_dispatch_requires_repository_authority"
        )


@pytest.mark.asyncio
async def test_read_only_jobs_repository_cannot_receive_or_substitute_write_authority(
    db,
):
    project_id = uuid4()
    repository_id = uuid4()
    job_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-jobs"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'read only jobs')",
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, read_only, is_managed) "
            "VALUES ($1, $2, $3, $4, 'jobs', true, true)",
            repository_id,
            project_id,
            repo_name,
            clean_url,
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, "
            "project_id, repo_name, context) VALUES "
            "($1, 'read only primary', 'created', 'pinned', $2, $3, "
            "jsonb_build_object('git_remote_url', $4::text))",
            job_id,
            project_id,
            repo_name,
            clean_url,
        )

    with pytest.raises(ManagedRepositoryAuthorityError) as refused:
        await ensure_job_primary_repository_authority(
            db, _gitea(), await db.get_job(str(job_id))
        )
    assert refused.value.code == "job_repository_requires_write_authority"
    read_authority = await db.get_managed_repository_authority(
        repo_name, repository_owner="srw"
    )
    assert read_authority is not None
    assert read_authority["access_mode"] == "read"
    assert not await db.bind_job_managed_repository(
        str(job_id), repo_name=repo_name, clean_url=clean_url
    )

    # Even a directly forged active write row cannot override the durable
    # read-only project-repository contract at the authoritative bind/dispatch
    # boundaries.
    claimed = await db.claim_managed_repository_authority_revoke(
        repo_name, repository_owner="srw"
    )
    assert claimed is not None
    assert await db.finish_managed_repository_authority_revoke(str(claimed["id"]))
    private_key, public_key, fingerprint = _deploy_keypair()
    forged_write = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="project_repository",
        authority_id=str(repository_id),
        project_id=str(project_id),
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=clean_url,
        public_key=public_key,
        public_key_fingerprint=fingerprint,
        private_key=private_key,
    )
    await db.activate_managed_repository_authority(
        str(forged_write["id"]), forge_key_id=92, access_mode="write"
    )
    assert not await db.bind_job_managed_repository(
        str(job_id), repo_name=repo_name, clean_url=clean_url
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as transition:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
        assert (
            transition.value.constraint_name
            == "job_dispatch_requires_repository_authority"
        )


@pytest.mark.asyncio
async def test_creation_intent_is_concurrent_exact_scope_and_mode_identity(db):
    job_id = UUID(str((await db.create_job("concurrent repository intent"))["id"]))
    other_job_id = UUID(str((await db.create_job("foreign repository intent"))["id"]))
    repo_name = f"job-{str(job_id)[:8]}"
    kwargs = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_id": str(job_id),
        "project_id": None,
        "access_mode": "write",
    }
    first, second = await asyncio.gather(
        db.reserve_managed_repository_creation_intent(**kwargs),
        db.reserve_managed_repository_creation_intent(**kwargs),
    )
    assert first["id"] == second["id"]
    assert first["intent_marker"] == second["intent_marker"]
    with pytest.raises(RuntimeError):
        await db.reserve_managed_repository_creation_intent(
            **{**kwargs, "authority_id": str(other_job_id)}
        )
    with pytest.raises(RuntimeError):
        await db.reserve_managed_repository_creation_intent(
            **{**kwargs, "access_mode": "read"}
        )

    private_key, public_key, fingerprint = _deploy_keypair()
    with pytest.raises(RuntimeError):
        await db.reserve_managed_repository_authority(
            repository_owner="srw",
            repo_name=repo_name,
            authority_kind="job",
            authority_id=str(job_id),
            project_id=None,
            access_mode="read",
            creation_intent_id=str(first["id"]),
            clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
            public_key=public_key,
            public_key_fingerprint=fingerprint,
            private_key=private_key,
        )


@pytest.mark.asyncio
async def test_process_restart_adopts_committed_create_with_same_durable_marker(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'lost forge response', 'created', 'pinned')",
            job_id,
        )
    intent = await db.reserve_managed_repository_creation_intent(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
    )
    # The repository has already committed with this marker, but the first
    # process died before mark_managed_repository_created. A restarted service
    # receives the exact 409/adoption result from Gitea and reuses the ledger.
    restarted_gitea = _gitea()
    restarted_gitea.create_repo = AsyncMock(
        return_value=f"http://gitea:3000/srw/{repo_name}.git"
    )
    clean_url, adopted = await create_managed_repository(
        db,
        restarted_gitea,
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
    )
    assert adopted["id"] == intent["id"]
    assert adopted["intent_marker"] == intent["intent_marker"]
    assert adopted["status"] == "created"
    assert clean_url == f"http://gitea:3000/srw/{repo_name}.git"
    restarted_gitea.create_repo.assert_awaited_once_with(
        repo_name, intent_marker=str(intent["intent_marker"])
    )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_creation_intents "
                "WHERE repository_owner='srw' AND repo_name=$1",
                repo_name,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_foreign_creation_collision_is_retryable_without_wedging_owner_cleanup(
    db,
):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'foreign creation collision', 'created', 'pinned')",
            job_id,
        )
    kwargs = {
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_id": str(job_id),
        "project_id": None,
        "access_mode": "write",
    }
    first = await db.reserve_managed_repository_creation_intent(**kwargs)
    assert await db.conflict_managed_repository_creation_intent(str(first["id"]))

    # An exact retry reuses the same unguessable marker, so a late response or
    # operator removal of the collision can converge without audit-row growth.
    retry = await db.reserve_managed_repository_creation_intent(**kwargs)
    assert retry["id"] == first["id"]
    assert retry["intent_marker"] == first["intent_marker"]
    assert retry["status"] == "pending"
    assert await db.conflict_managed_repository_creation_intent(str(retry["id"]))

    gitea = _gitea()
    gitea.delete_repo.reset_mock()
    assert await revoke_and_delete_managed_repository(db, gitea, repo_name)
    gitea.delete_repo.assert_not_awaited()
    async with db.acquire() as conn:
        assert await conn.execute("DELETE FROM jobs WHERE id=$1", job_id) == "DELETE 1"
        row = await conn.fetchrow(
            "SELECT status, failure_class FROM managed_repository_creation_intents "
            "WHERE id=$1",
            first["id"],
        )
    assert row["status"] == "conflicted"
    assert row["failure_class"] == "foreign_collision"


@pytest.mark.asyncio
async def test_reference_mode_rotates_revoke_first_and_cannot_substitute_write(
    db, monkeypatch
):
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    project_id = uuid4()
    repository_id = uuid4()
    repo_name = f"project-{str(project_id)[:8]}-reference"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'mode rotation')",
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, read_only, is_managed) "
            "VALUES ($1, $2, $3, $4, 'source', true, true)",
            repository_id,
            project_id,
            repo_name,
            clean_url,
        )
    gitea = _gitea()
    repository = await db.get_project_repository(str(repository_id))
    read_authority = await ensure_project_repository_authority(db, gitea, repository)
    assert read_authority is not None
    assert read_authority["access_mode"] == "read"
    assert gitea.ensure_repo_deploy_key.await_args.kwargs["access_mode"] == "read"
    assert await db.managed_repository_authorities_are_current(
        [
            {
                "authority_id": str(read_authority["id"]),
                "generation": read_authority["generation"],
                "repo_name": repo_name,
                "access_mode": "read",
            }
        ]
    )
    assert not await db.managed_repository_authorities_are_current(
        [
            {
                "authority_id": str(read_authority["id"]),
                "generation": read_authority["generation"],
                "repo_name": repo_name,
                "access_mode": "write",
            }
        ]
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as widened:
            await conn.execute(
                "UPDATE project_repositories SET read_only=false WHERE id=$1",
                repository_id,
            )
        assert (
            widened.value.constraint_name
            == "managed_repository_access_mode_requires_authority"
        )

    target = {**repository, "read_only": False}
    write_authority = await rotate_project_repository_authority(
        db, gitea, target, force=True
    )
    assert write_authority is not None
    assert write_authority["generation"] == 2
    assert write_authority["access_mode"] == "write"
    gitea.delete_repo_deploy_key.assert_awaited_once_with(
        repo_name, int(read_authority["forge_key_id"])
    )
    # Until the row CAS completes, the new write key is not deliverable under
    # the still-read-only durable contract.
    with pytest.raises(ManagedRepositoryAuthorityError):
        await ensure_project_repository_authority(
            db, gitea, await db.get_project_repository(str(repository_id))
        )
    async with db.acquire() as conn:
        assert (
            await conn.execute(
                "UPDATE project_repositories SET read_only=false WHERE id=$1",
                repository_id,
            )
            == "UPDATE 1"
        )
    # A committed authority rotation followed by a lost PATCH response must
    # not revoke and mint a third generation on retry. The durable row now
    # agrees with the active target mode, so the existing generation is the
    # idempotent result.
    replayed = await rotate_project_repository_authority(
        db,
        gitea,
        await db.get_project_repository(str(repository_id)),
    )
    assert replayed is not None
    assert replayed["id"] == write_authority["id"]
    assert replayed["generation"] == 2
    assert gitea.delete_repo_deploy_key.await_count == 1


@pytest.mark.asyncio
async def test_direct_creation_funnels_strip_forged_repository_authority(db):
    forged = {
        "git_remote_url": "http://admin:secret@gitea/repo.git",
        "managed_repository_credentials": [{"private_key": "secret"}],
        "nested": {
            "repository_auth": {"password": "secret"},
            "ordinary": "kept",
        },
    }
    job = await db.create_job(
        "strip forged repo authority",
        repo_name="caller-selected-foreign-repository",
        context=forged,
        config_override={
            "ordinary": "kept",
            "nested": {"repository_credentials": {"token": "secret"}},
        },
    )
    stored_job = await db.get_job(str(job["id"]))
    stored_context = _json(stored_job["context"])
    stored_config = _json(stored_job["config_override"])
    assert stored_context["nested"] == {"ordinary": "kept"}
    assert stored_job["repo_name"] is None
    assert "git_remote_url" not in stored_context
    assert "managed_repository_credentials" not in stored_context
    assert stored_config["ordinary"] == "kept"
    assert stored_config["nested"] == {}
    assert "repository_credentials" not in stored_config

    assert await db.merge_job_context(
        str(job["id"]),
        {
            "git_remote_url": "http://gitea/srw/foreign.git",
            "repo_name": "foreign",
            "nested": {"managed_repository_credentials": [{"private_key": "secret"}]},
            "ordinary_after_create": "kept",
        },
    )
    merged_job = await db.get_job(str(job["id"]))
    merged_context = _json(merged_job["context"])
    assert merged_context["ordinary_after_create"] == "kept"
    assert "git_remote_url" not in merged_context
    assert "repo_name" not in merged_context
    assert merged_context["nested"] == {}

    thread_id = await db.create_thread(
        initial_metadata={
            "managed_repository_authority": {"private_key": "secret"},
            "nested": {"repository_auth": {"password": "secret"}},
            "workspace_container": {
                "repo_name": "thread-forged",
                "git_remote_url": "http://admin:secret@gitea/repo.git",
                "status": "pending",
            },
        }
    )
    thread = await db.get_thread(thread_id)
    metadata = _json(thread["metadata"])
    assert "managed_repository_authority" not in metadata
    assert metadata["nested"] == {}
    assert metadata["workspace_container"] == {"status": "pending"}

    assert await db.merge_thread_workspace_context(
        thread_id,
        {
            "repo_name": "thread-forged",
            "git_remote_url": "http://gitea/srw/foreign.git",
            "managed_repository_credentials": [{"private_key": "secret"}],
            "status": "ready",
        },
    )
    merged_thread = await db.get_thread(thread_id)
    assert _json(merged_thread["metadata"])["workspace_container"] == {
        "status": "ready"
    }


@pytest.mark.asyncio
async def test_exact_active_authority_is_required_for_job_and_thread_binding(db):
    root = await db.create_job("root repository binding")
    root_id = str(root["id"])
    repo_name = f"job-{root_id[:8]}"
    authority = await _reserve(db, repo_name=repo_name, authority_id=UUID(root_id))
    await db.activate_managed_repository_authority(
        str(authority["id"]), forge_key_id=17, access_mode="write"
    )
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"

    assert await db.bind_job_managed_repository(
        root_id, repo_name=repo_name, clean_url=clean_url
    )
    bound_root = await db.get_job(root_id)
    assert bound_root["repo_name"] == repo_name
    assert _json(bound_root["context"])["git_remote_url"] == clean_url
    assert not await db.bind_job_managed_repository(
        root_id,
        repo_name=repo_name,
        clean_url="http://gitea:3000/srw/foreign.git",
    )

    child = await db.create_job("child repository binding", parent_job_id=root_id)
    assert await db.bind_job_managed_repository(
        str(child["id"]), repo_name=repo_name, clean_url=clean_url
    )

    thread_id = await db.create_thread()
    private_key, public_key, fingerprint = _deploy_keypair()
    thread_repo = f"thread-{thread_id[:8]}"
    thread_authority = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=thread_repo,
        authority_kind="thread",
        authority_id=thread_id,
        project_id=None,
        access_mode="write",
        creation_intent_id=None,
        clean_repo_url=f"http://gitea:3000/srw/{thread_repo}.git",
        public_key=public_key,
        public_key_fingerprint=fingerprint,
        private_key=private_key,
    )
    await db.activate_managed_repository_authority(
        str(thread_authority["id"]), forge_key_id=18, access_mode="write"
    )
    assert await db.bind_thread_managed_repository(
        thread_id,
        repo_name=thread_repo,
        clean_url=str(thread_authority["clean_repo_url"]),
    )
    bound_thread = await db.get_thread(thread_id)
    workspace = _json(bound_thread["metadata"])["workspace_container"]
    assert workspace == {
        "repo_name": thread_repo,
        "git_remote_url": thread_authority["clean_repo_url"],
    }


@pytest.mark.asyncio
async def test_trigger_refuses_old_writer_and_managed_repo_rename(db):
    project_id = uuid4()
    repository_id = uuid4()
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as forged_job:
            await conn.execute(
                "INSERT INTO jobs (description, context) VALUES ("
                "'forged authority', '{\"wrapper\": {"
                '"managed_repository_credentials": [{'
                '"private_key": "secret"}]}}\'::jsonb)'
            )
        assert (
            forged_job.value.constraint_name
            == "managed_repository_credentials_are_server_owned"
        )
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'authority project')",
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_repositories "
            "(id, project_id, name, repo_url, role, is_managed) "
            "VALUES ($1, $2, 'project-secret', "
            "'http://admin:secret@gitea/srw/project-secret.git', "
            "'source', true)",
            repository_id,
            project_id,
        )
        stored = await conn.fetchrow(
            "SELECT repo_url, credentials FROM project_repositories WHERE id=$1",
            repository_id,
        )
        assert stored["repo_url"] == "http://gitea/srw/project-secret.git"
        assert _json(stored["credentials"]) == {}
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "UPDATE project_repositories SET name='project-renamed' WHERE id=$1",
                repository_id,
            )


@pytest.mark.asyncio
async def test_previous_release_job_write_is_stripped_and_held_until_proof(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'rolling old writer', 'created', 'pinned')",
            job_id,
        )
        # Exact old-replica order: URL first, repo_name second.
        await conn.execute(
            "UPDATE jobs SET context = COALESCE(context, '{}') || "
            "jsonb_build_object('git_remote_url', $2::text) WHERE id=$1",
            job_id,
            legacy_url,
        )
        between = await conn.fetchrow("SELECT context FROM jobs WHERE id=$1", job_id)
        between_context = _json(between["context"])
        assert between_context["git_remote_url"] == clean_url
        assert between_context["_managed_repository_authority_pending"] is True
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as gap_claim:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
        assert (
            gap_claim.value.constraint_name
            == "job_dispatch_requires_repository_authority"
        )
        await conn.execute(
            "UPDATE jobs SET repo_name=$2 WHERE id=$1", job_id, repo_name
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as old_claim:
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
        assert (
            old_claim.value.constraint_name
            == "job_dispatch_requires_repository_authority"
        )

    await prepare_job_repository_authority(
        db, _gitea(probe=True), await db.get_job(str(job_id))
    )
    adopted = await db.get_job(str(job_id))
    adopted_context = _json(adopted["context"])
    assert adopted_context["git_remote_url"] == clean_url
    assert "_managed_repository_authority_pending" not in adopted_context
    async with db.acquire() as conn:
        assert (
            await conn.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1", job_id
            )
            == "UPDATE 1"
        )


@pytest.mark.asyncio
async def test_previous_release_thread_write_is_stripped_and_attach_fenced(db):
    thread_id = await db.create_thread()
    repo_name = f"thread-{thread_id[:8]}"
    legacy_url = f"http://admin:shared-secret@gitea:3000/srw/{repo_name}.git"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET metadata=jsonb_set(COALESCE(metadata, '{}'), "
            "'{workspace_container}', jsonb_build_object("
            "'repo_name', $2::text, 'git_remote_url', $3::text), true) "
            "WHERE id=$1",
            UUID(thread_id),
            repo_name,
            legacy_url,
        )
        written = await conn.fetchval(
            "SELECT metadata FROM threads WHERE id=$1", UUID(thread_id)
        )
        workspace = _json(written)["workspace_container"]
        assert workspace["git_remote_url"] == clean_url
        assert workspace["_managed_repository_authority_pending"] is True
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('session_base', 'rolling-old-agent', 'ready') RETURNING id"
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as old_attach:
            await conn.execute(
                "UPDATE threads SET agent_id=$2 WHERE id=$1",
                UUID(thread_id),
                agent_id,
            )
        assert (
            old_attach.value.constraint_name
            == "thread_attach_requires_repository_authority"
        )

    await prepare_thread_repository_authority(
        db, _gitea(probe=True), await db.get_thread(thread_id)
    )
    adopted = await db.get_thread(thread_id)
    adopted_workspace = _json(adopted["metadata"])["workspace_container"]
    assert adopted_workspace["git_remote_url"] == clean_url
    assert "_managed_repository_authority_pending" not in adopted_workspace
    async with db.acquire() as conn:
        # 0176's repository fence is the subject; 0200's separate pinned
        # protection edge has its own proofs and is not what this bind is
        # demonstrating.
        async with previous_release_writer(
            conn, "threads", *PINNED_BINDING_AUTHORITY_TRIGGERS
        ):
            async with conn.transaction():
                assert (
                    await conn.execute(
                        "UPDATE threads SET agent_id=$2 WHERE id=$1",
                        UUID(thread_id),
                        agent_id,
                    )
                    == "UPDATE 1"
                )
                assert (
                    await conn.execute(
                        "UPDATE agents SET thread_id=$2 WHERE id=$1",
                        agent_id,
                        UUID(thread_id),
                    )
                    == "UPDATE 1"
                )


@pytest.mark.asyncio
async def test_ambiguous_historical_repository_scope_is_not_guessed(db):
    first_project = uuid4()
    second_project = uuid4()
    first_repository = uuid4()
    second_repository = uuid4()
    repo_name = "historical-shared-name"
    clean_url = f"http://gitea:3000/srw/{repo_name}.git"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES "
            "($1, 'first project'), ($2, 'second project')",
            first_project,
            second_project,
        )
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
            clean_url,
        )
    repository = await db.get_project_repository(str(first_repository))
    gitea = _gitea()

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        await ensure_project_repository_authority(db, gitea, repository)

    assert exc.value.code == "repository_scope_ambiguous"
    gitea.ensure_repo_deploy_key.assert_not_awaited()
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_authorities "
                "WHERE repo_name=$1",
                repo_name,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_revocation_contains_key_before_owner_cleanup(db):
    job_id = UUID(str((await db.create_job("repository revocation"))["id"]))
    repo_name = f"job-{str(job_id)[:8]}"
    reserved = await _reserve(db, repo_name=repo_name, authority_id=job_id)
    await db.activate_managed_repository_authority(
        str(reserved["id"]), forge_key_id=91, access_mode="write"
    )
    gitea = _gitea()
    gitea.delete_repo.return_value = False

    assert await revoke_and_delete_managed_repository(db, gitea, repo_name)

    gitea.delete_repo_deploy_key.assert_awaited_once_with(repo_name, 91)
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, revoked_at FROM managed_repository_authorities WHERE id=$1",
            reserved["id"],
        )
    assert row["status"] == "revoked"
    assert row["revoked_at"] is not None


@pytest.mark.asyncio
async def test_direct_owner_delete_cannot_orphan_live_repository_authority(db):
    job_id = uuid4()
    repo_name = f"job-{str(job_id)[:8]}"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'cleanup fence', 'created', 'pinned')",
            job_id,
        )
    intent = await db.reserve_managed_repository_creation_intent(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
    )
    await db.mark_managed_repository_created(
        str(intent["id"]), intent_marker=str(intent["intent_marker"])
    )
    private_key, public_key, fingerprint = _deploy_keypair()
    reserved = await db.reserve_managed_repository_authority(
        repository_owner="srw",
        repo_name=repo_name,
        authority_kind="job",
        authority_id=str(job_id),
        project_id=None,
        access_mode="write",
        creation_intent_id=str(intent["id"]),
        clean_repo_url=f"http://gitea:3000/srw/{repo_name}.git",
        public_key=public_key,
        public_key_fingerprint=fingerprint,
        private_key=private_key,
    )
    await db.activate_managed_repository_authority(
        str(reserved["id"]), forge_key_id=91, access_mode="write"
    )
    assert await db.bind_job_managed_repository(
        str(job_id),
        repo_name=repo_name,
        clean_url=str(reserved["clean_repo_url"]),
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as refused:
            await conn.execute("DELETE FROM jobs WHERE id=$1", job_id)
        assert refused.value.constraint_name == "managed_repository_cleanup_required"

    gitea = _gitea()
    assert await revoke_and_delete_managed_repository(db, gitea, repo_name)
    gitea.delete_repo.assert_awaited_once_with(
        repo_name, intent_marker=str(intent["intent_marker"])
    )
    async with db.acquire() as conn:
        states = await conn.fetchrow(
            "SELECT authority.status AS authority_status, intent.status AS "
            "intent_status FROM managed_repository_authorities AS authority "
            "JOIN managed_repository_creation_intents AS intent "
            "ON intent.id=authority.creation_intent_id WHERE authority.id=$1",
            reserved["id"],
        )
        assert states["authority_status"] == "revoked"
        assert states["intent_status"] == "deleted"
        assert await conn.execute("DELETE FROM jobs WHERE id=$1", job_id) == "DELETE 1"
