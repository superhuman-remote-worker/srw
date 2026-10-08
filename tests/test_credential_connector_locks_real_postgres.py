"""Real-PostgreSQL proofs for the credential connector locks (slice D1c).

The specs say which connector types a live session may not detach
(``live_detach == "refused"``) and which may not be deleted while attached
(``delete_while_attached``): today the credentials connector. The store
enforces both in SQL, the detach lock through a ``d.type = ANY($3::text[])``
parameter built from the specs; these tests run that SQL.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database import postgres as postgres_module
from orchestrator.database.postgres import PostgresDB
from shared.credential_connectors import CredentialConnectorAttachedError

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
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
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=5)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE job_datasources, datasources, jobs, threads CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _connector(db: PostgresDB, ds_type: str) -> tuple[str, int]:
    datasource_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision) "
            "VALUES ($1, $2, $3, 'all', 1)",
            datasource_id,
            f"{ds_type}-{str(datasource_id)[:8]}",
            ds_type,
        )
        revision = await conn.fetchval(
            "SELECT policy_revision FROM datasources WHERE id = $1", datasource_id
        )
    return str(datasource_id), int(revision)


async def _session(db: PostgresDB, datasource_ids: list[str], status="active") -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) "
            "VALUES ($1, $2, jsonb_build_object('datasource_ids', $3::jsonb))",
            thread_id,
            status,
            json.dumps(datasource_ids),
        )
    return str(thread_id)


async def _selection(db: PostgresDB, thread_id: str) -> list[str]:
    async with db.acquire() as conn:
        value = await conn.fetchval(
            "SELECT metadata->'datasource_ids' FROM threads WHERE id = $1",
            UUID(thread_id),
        )
    return json.loads(value)


@pytest.mark.asyncio
async def test_a_live_session_cannot_detach_a_credentials_connector(db):
    credentials, _ = await _connector(db, "credentials")
    database, database_revision = await _connector(db, "postgresql")
    thread_id = await _session(db, [credentials, database])

    with pytest.raises(CredentialConnectorAttachedError, match="lifetime"):
        await db.set_thread_datasource_ids(
            thread_id,
            [database],
            datasource_policy_revisions={database: database_revision},
        )
    with pytest.raises(CredentialConnectorAttachedError):
        await db.set_thread_datasource_ids(
            thread_id, [], datasource_policy_revisions={}
        )
    assert await _selection(db, thread_id) == [credentials, database]


@pytest.mark.asyncio
async def test_other_connectors_detach_and_the_credentials_one_stays(db):
    credentials, credentials_revision = await _connector(db, "credentials")
    database, _ = await _connector(db, "postgresql")
    thread_id = await _session(db, [credentials, database])

    assert await db.set_thread_datasource_ids(
        thread_id,
        [credentials],
        datasource_policy_revisions={credentials: credentials_revision},
    )
    assert await _selection(db, thread_id) == [credentials]


@pytest.mark.asyncio
async def test_an_empty_refused_set_detaches_anything(db, monkeypatch):
    """The SQL parameter is safe empty: ANY('{}') matches no row."""
    monkeypatch.setattr(postgres_module, "_LIVE_DETACH_REFUSED_TYPES", frozenset())
    credentials, _ = await _connector(db, "credentials")
    thread_id = await _session(db, [credentials])

    assert await db.set_thread_datasource_ids(
        thread_id, [], datasource_policy_revisions={}
    )
    assert await _selection(db, thread_id) == []


@pytest.mark.asyncio
async def test_the_refused_set_comes_from_the_specs(db, monkeypatch):
    """A type the specs refuse is locked by the same SQL."""
    monkeypatch.setattr(
        postgres_module, "_LIVE_DETACH_REFUSED_TYPES", frozenset({"postgresql"})
    )
    database, _ = await _connector(db, "postgresql")
    thread_id = await _session(db, [database])

    with pytest.raises(CredentialConnectorAttachedError):
        await db.set_thread_datasource_ids(
            thread_id, [], datasource_policy_revisions={}
        )


@pytest.mark.asyncio
async def test_a_credentials_connector_in_a_live_session_cannot_be_deleted(db):
    credentials, _ = await _connector(db, "credentials")
    thread_id = await _session(db, [credentials])

    with pytest.raises(CredentialConnectorAttachedError, match="End the sessions"):
        await db.delete_datasource(credentials)

    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET status = 'ended', ended_at = now() WHERE id = $1",
            UUID(thread_id),
        )
    assert await db.delete_datasource(credentials) is True


@pytest.mark.asyncio
async def test_a_credentials_connector_on_unfinished_work_cannot_be_deleted(db):
    credentials, _ = await _connector(db, "credentials")
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'uses a credential', 'paused')",
            job_id,
        )
        await conn.execute(
            "INSERT INTO job_datasources (job_id, datasource_id) VALUES ($1, $2)",
            job_id,
            UUID(credentials),
        )

    with pytest.raises(CredentialConnectorAttachedError):
        await db.delete_datasource(credentials)

    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status = 'completed' WHERE id = $1", job_id)
    assert await db.delete_datasource(credentials) is True


@pytest.mark.asyncio
async def test_other_connectors_delete_while_attached(db):
    database, _ = await _connector(db, "postgresql")
    await _session(db, [database])

    assert await db.delete_datasource(database) is True
