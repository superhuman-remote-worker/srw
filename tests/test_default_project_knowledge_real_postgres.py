"""Real-Postgres proof for the default-project vault backfill and its retry.

Three properties only a real server can show:

* the backfill's candidate query: approved users' default projects without a
  ``knowledge``/``jobs`` repository, one row per project (its oldest user), and
  never a pending user's or an orphan default project nobody points at;
* the pathless-row gate runs against the real ``knowledge_index`` (a separate
  database, as in production): only an *active* pathless *user* note skips a
  project. The ``datasource`` notes the knowledge projection writes for every
  connector a project reaches are pathless by design and never block, at
  signup (where the user's cloud-root connector lands one in the default
  project) or in the backfill;
* a retry after Gitea refused a creation resumes that creation's durable
  intent. Without the resume, the retry's fresh authority id is refused as
  another scope's intent, so the project would stay vault-less for good.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("testcontainers.postgres")

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402

from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.database.postgres import PostgresDB  # noqa: E402
from orchestrator.services import (  # noqa: E402
    default_project_knowledge as dpk,
    project_provisioning,
)

PG_IMAGE = "pgvector/pgvector:pg15"
DATABASE = Path(__file__).resolve().parents[1] / "src" / "orchestrator" / "database"
APP_SCHEMA = DATABASE / "schema_current.sql"
VECTOR_MIGRATIONS = DATABASE / "migrations" / "vector"


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - environment without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def dsns(pg_dsn):
    """The app schema and the vector schema, each in its own database."""
    vector_dsn = re.sub(r"/[^/]+$", "/vector", pg_dsn)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(APP_SCHEMA.read_text())
        await conn.execute("CREATE DATABASE vector")
    finally:
        await conn.close()
    pool = await asyncpg.create_pool(vector_dsn, min_size=1, max_size=2)
    try:
        await run_migrations(pool, VECTOR_MIGRATIONS)
    finally:
        await pool.close()
    return pg_dsn, vector_dsn


@pytest_asyncio.fixture
async def stores(dsns):
    app_dsn, vector_dsn = dsns
    app = PostgresDB(connection_string=app_dsn, min_connections=1, max_connections=4)
    vector = PostgresDB(
        connection_string=vector_dsn, min_connections=1, max_connections=2
    )
    await app.connect()
    await vector.connect()
    async with app.acquire() as conn:
        await conn.execute(
            "TRUNCATE managed_repository_authorities, "
            "managed_repository_creation_intents, project_repositories, "
            "project_members, projects, users CASCADE"
        )
    await vector.execute("TRUNCATE knowledge_index CASCADE")
    try:
        yield SimpleNamespace(app=app, vector=vector)
    finally:
        await app.close()
        await vector.close()


_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


async def _project(app: PostgresDB, name: str) -> str:
    async with app.acquire() as conn:
        return str(
            await conn.fetchval(
                "INSERT INTO projects (name, is_default) VALUES ($1, true) "
                "RETURNING id",
                name,
            )
        )


async def _user(
    app: PostgresDB, project_id: str, *, approved: bool = True, age_days: int = 0
) -> str:
    async with app.acquire() as conn:
        return str(
            await conn.fetchval(
                "INSERT INTO users (display_name, email, is_approved, "
                "default_project_id, created_at) VALUES ($1, $2, $3, $4, $5) "
                "RETURNING id",
                f"user-{project_id[:4]}-{age_days}",
                f"{uuid.uuid4().hex[:8]}@example.test",
                approved,
                uuid.UUID(project_id),
                _EPOCH - timedelta(days=age_days),
            )
        )


async def _repository(app: PostgresDB, project_id: str, role: str) -> None:
    await app.add_project_repository(
        project_id=project_id,
        name=f"{role}-{project_id[:8]}",
        repo_url=f"https://git.example.test/{role}-{project_id[:8]}.git",
        role=role,
        read_only=role == "reference",
    )


async def _note(
    vector: PostgresDB, project_id: str, *, path, status, note_type="learning"
) -> None:
    await vector.execute(
        """
        INSERT INTO knowledge_index
            (id, project_id, kb_id, note_id, title, note_type, status, content, path)
        VALUES ($1, $2::uuid,
                CASE WHEN $3::text IS NULL THEN NULL ELSE $2::uuid END,
                $4, 'Title', $6, $5, 'body', $3::text)
        """,
        uuid.uuid4(),
        uuid.UUID(project_id),
        path,
        f"note-{uuid.uuid4().hex[:6]}",
        status,
        note_type,
    )


async def _connector_note(
    vector: PostgresDB, project_id: str, *, status: str = "active"
) -> None:
    """A ``ds-*`` row as ``knowledge_projection.sync_datasource_knowledge``
    writes it: no ``kb_id``, no ``path``, type ``datasource``."""
    await vector.execute(
        """
        INSERT INTO knowledge_index
            (note_id, project_id, title, note_type, status, confidence, tags,
             content)
        VALUES ($1, $2::uuid, 'Connector: My cloud (cloud_folder)', 'datasource',
                $3, 'high', ARRAY['datasource', 'cloud_folder'], 'body')
        """,
        f"ds-{uuid.uuid4().hex[:8]}",
        uuid.UUID(project_id),
        status,
    )


def _deps(stores, forge) -> project_provisioning.ProjectProvisioningDependencies:
    return project_provisioning.ProjectProvisioningDependencies(
        store=stores.app,
        forge=forge,
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(),
        logger=MagicMock(),
        repair=project_provisioning.ProjectRepairState(),
        knowledge_index=SimpleNamespace(vector_db=stores.vector),
    )


@pytest.mark.asyncio
async def test_the_backfill_selects_exactly_the_eligible_projects(stores, monkeypatch):
    app, vector = stores.app, stores.vector
    bare = await _project(app, "bare")
    bare_owner = await _user(app, bare)
    with_knowledge = await _project(app, "knowledge")
    await _user(app, with_knowledge)
    await _repository(app, with_knowledge, "knowledge")
    with_jobs = await _project(app, "jobs")
    await _user(app, with_jobs)
    await _repository(app, with_jobs, "jobs")
    source_only = await _project(app, "source only")  # a source repo is no vault
    await _user(app, source_only)
    await _repository(app, source_only, "source")
    pending = await _project(app, "pending")
    await _user(app, pending, approved=False)
    orphan = await _project(app, "orphan")  # its user is gone
    shared = await _project(app, "shared")
    await _user(app, shared, age_days=1)
    shared_oldest = await _user(app, shared, age_days=5)
    legacy = await _project(app, "legacy")
    await _user(app, legacy)
    await _note(vector, legacy, path=None, status="active")
    settled = await _project(app, "settled")
    await _user(app, settled)
    await _note(vector, settled, path=None, status="archived")
    await _note(vector, settled, path="knowledge/a.md", status="active")
    # Every project a connector reaches: pathless by design, not user notes.
    connectors_only = await _project(app, "connectors only")
    await _user(app, connectors_only)
    await _connector_note(vector, connectors_only)
    await _connector_note(vector, connectors_only)
    await _connector_note(vector, connectors_only, status="archived")
    legacy_and_connector = await _project(app, "legacy and connector")
    await _user(app, legacy_and_connector)
    await _connector_note(vector, legacy_and_connector)
    await _note(vector, legacy_and_connector, path=None, status="active")

    provisioned: dict[str, str] = {}

    async def _provision(project, owner_id, *, dependencies):
        provisioned[str(project["id"])] = owner_id
        await _repository(app, str(project["id"]), "knowledge")
        return {"id": "datasource"}

    monkeypatch.setattr(dpk, "provision_project_knowledge_repo", _provision)
    deps = _deps(stores, SimpleNamespace(is_initialized=True))

    counts = await dpk.backfill_default_project_knowledge(dependencies=deps)

    assert provisioned == {
        bare: bare_owner,
        source_only: provisioned[source_only],
        shared: shared_oldest,
        settled: provisioned[settled],
        connectors_only: provisioned[connectors_only],
    }
    assert {
        with_knowledge,
        with_jobs,
        pending,
        orphan,
        legacy,
        legacy_and_connector,
    }.isdisjoint(provisioned)
    assert counts["provisioned"] == 5 and counts["legacy_rows"] == 2

    # Idempotent: the second pass only meets the projects it must skip.
    again = await dpk.backfill_default_project_knowledge(dependencies=deps)
    assert again["provisioned"] == 0 and again["legacy_rows"] == 2
    assert sum(again.values()) == 2


@pytest.mark.asyncio
async def test_the_signup_ensure_provisions_past_connector_notes(stores, monkeypatch):
    # Admission creates the user's cloud-root connector, linked to the default
    # project, before the vault ensure runs; the datasource reconciler may
    # have projected its ds-* note by then.
    app, vector = stores.app, stores.vector
    fresh = await _project(app, "fresh")
    fresh_user = await app.get_user(await _user(app, fresh))
    await _connector_note(vector, fresh)
    returning = await _project(app, "returning")
    returning_user = await app.get_user(await _user(app, returning))
    await _connector_note(vector, returning)
    await _note(vector, returning, path=None, status="active")

    async def _provision(project, owner_id, *, dependencies):
        return {"id": "datasource"}

    monkeypatch.setattr(dpk, "provision_project_knowledge_repo", _provision)
    deps = _deps(stores, SimpleNamespace(is_initialized=True))

    async def _admit(user) -> str:
        before = set(deps.repair.bg_repair_tasks)
        assert dpk.schedule_default_project_knowledge(user, dependencies=deps)
        (task,) = set(deps.repair.bg_repair_tasks) - before
        return await asyncio.wait_for(task, timeout=10)

    assert await _admit(fresh_user) == "provisioned"
    # A real pathless user note still blocks, connector notes or not.
    assert await _admit(returning_user) == "legacy_rows"


@pytest.mark.asyncio
async def test_a_retry_after_a_refused_creation_resumes_its_intent(stores):
    app = stores.app
    project = await _project(app, "retry")
    owner = await _user(app, project)
    repo_name = f"project-{project[:8]}-knowledge"
    url = f"http://gitea:3000/srw/{repo_name}.git"
    forge = MagicMock()
    forge.is_initialized = True
    forge.repository_owner = "srw"
    forge.clean_repo_url = MagicMock(return_value=url)
    # Gitea is down for the first attempt, back for the second.
    forge.create_repo = AsyncMock(side_effect=[None, url])
    forge.repository_creation_intent_status = AsyncMock(return_value="missing")
    forge.grant_user_repo_access = AsyncMock(return_value=True)
    app.create_datasource = AsyncMock(return_value={"id": "datasource"})
    deps = _deps(stores, forge)
    user = await app.get_user(owner)

    first = await dpk.ensure_default_project_knowledge(user, dependencies=deps)
    assert first == "failed"
    intent = await app.get_managed_repository_creation_intent(
        repo_name, repository_owner="srw"
    )
    assert intent["status"] == "pending"
    # What a retry under a fresh id would meet: another scope's live intent.
    with pytest.raises(RuntimeError, match="another scope"):
        await app.reserve_managed_repository_creation_intent(
            repository_owner="srw",
            repo_name=repo_name,
            authority_kind="project_repository",
            authority_id=str(uuid.uuid4()),
            project_id=project,
            access_mode="none",
        )

    second = await dpk.ensure_default_project_knowledge(user, dependencies=deps)

    assert second == "provisioned"
    (repository,) = await app.get_project_repositories(project, role="knowledge")
    assert str(repository["id"]) == str(intent["authority_id"])
    markers = {
        call.kwargs["intent_marker"] for call in forge.create_repo.call_args_list
    }
    assert markers == {str(intent["intent_marker"])}
    created = await app.get_managed_repository_creation_intent(
        repo_name, repository_owner="srw"
    )
    assert created["status"] == "created" and created["id"] == intent["id"]
    # And from here on the ensure is a no-op.
    assert await dpk.ensure_default_project_knowledge(user, dependencies=deps) == (
        "present"
    )
