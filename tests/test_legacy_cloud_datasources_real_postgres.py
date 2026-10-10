"""The legacy cloud WebDAV rows retired on a real PostgreSQL (main cloud
slice 8): only the exact shapes SRW made go, with their links, a tombstone
and the threads' references; a second pass retires nothing."""

from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.legacy_cloud_datasources import (
    retire_legacy_cloud_datasources,
)
from orchestrator.services.manifest_connectors import migrate_stored_connectors

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA = _ROOT / "src/orchestrator/database/schema_current.sql"
_USER = "11111111-1111-4111-8111-111111111111"
_DEFAULT_PROJECT = "22222222-2222-4222-8222-222222222222"
_OTHER_PROJECT = "33333333-3333-4333-8333-333333333333"
_THREAD = "44444444-4444-4444-8444-444444444444"
_ADMIN_HOME = "http://srw-nextcloud/remote.php/dav/files/admin/"


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
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(_SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE datasource_tombstones, project_datasources, datasources, "
            "threads, projects, users CASCADE"
        )
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1::uuid, 'ada')", _USER
        )
        await conn.execute(
            """
            INSERT INTO projects (id, name, is_default)
            VALUES ($1::uuid, 'Personal', TRUE), ($2::uuid, 'Team', FALSE)
            """,
            _DEFAULT_PROJECT,
            _OTHER_PROJECT,
        )
    try:
        yield store
    finally:
        await store.close()


async def _row(
    db,
    *,
    name: str,
    description: str | None = None,
    connection_url: str = "https://cloud.example/remote.php/dav/files/ada/",
    created_by: str | None = None,
    is_global: bool = False,
    projects: tuple[str, ...] = (),
) -> str:
    async with db.acquire() as conn:
        datasource_id = await conn.fetchval(
            """
            INSERT INTO datasources (
                name, description, type, connection_url, credentials,
                created_by, is_global, scope_mode
            ) VALUES ($1, $2, 'webdav', $3, '{}'::jsonb, $4::uuid, $5, $6)
            RETURNING id
            """,
            name,
            description,
            connection_url,
            created_by,
            is_global,
            "all" if is_global else "projects",
        )
        for project_id in projects:
            await conn.execute(
                "INSERT INTO project_datasources (project_id, datasource_id, "
                "read_only) VALUES ($1::uuid, $2, FALSE)",
                project_id,
                datasource_id,
            )
    return str(datasource_id)


def _personal(db, **overrides):
    shape = {
        "name": "Cloud Storage (Personal)",
        "description": "Personal cloud storage",
        "projects": (_DEFAULT_PROJECT,),
    }
    return _row(db, **{**shape, **overrides})


async def _ids(db) -> set[str]:
    async with db.acquire() as conn:
        return {str(r["id"]) for r in await conn.fetch("SELECT id FROM datasources")}


@pytest.mark.asyncio
async def test_the_shapes_srw_made_go_with_their_links_once(db):
    # (name, type, owner) is unique: one row of each shape per installation.
    personal = await _personal(db)
    bundled = await _row(
        db, name="Nextcloud", connection_url=_ADMIN_HOME, is_global=True
    )
    kept = await _row(db, name="Team files", projects=(_OTHER_PROJECT,))
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (id, user_id, status, metadata)
            VALUES ($1::uuid, $2::uuid, 'created', $3::jsonb)
            """,
            _THREAD,
            _USER,
            json.dumps({"datasource_ids": [personal, kept]}),
        )

    counts = await retire_legacy_cloud_datasources(db)

    assert counts == {"personal": 1, "bundled_nextcloud": 1}
    assert await _ids(db) == {kept}
    async with db.acquire() as conn:
        links = await conn.fetchval(
            "SELECT count(*) FROM project_datasources WHERE datasource_id = "
            "ANY($1::uuid[])",
            [personal, bundled],
        )
        tombstones = await conn.fetch(
            "SELECT id, name, deleted_by FROM datasource_tombstones"
        )
        metadata = await conn.fetchval(
            "SELECT metadata FROM threads WHERE id = $1::uuid", _THREAD
        )
    assert links == 0
    assert {(str(t["id"]), t["name"], t["deleted_by"]) for t in tombstones} == {
        (personal, "Cloud Storage (Personal)", None),
        (bundled, "Nextcloud", None),
    }
    assert json.loads(metadata)["datasource_ids"] == [kept]

    assert await retire_legacy_cloud_datasources(db) == {
        "personal": 0,
        "bundled_nextcloud": 0,
    }
    assert await _ids(db) == {kept}


@pytest.mark.asyncio
async def test_the_first_weeks_description_is_the_same_shape(db):
    await _personal(db, description="Personal Nextcloud file storage")
    assert (await retire_legacy_cloud_datasources(db))["personal"] == 1
    assert await _ids(db) == set()


@pytest.mark.asyncio
async def test_the_rows_as_srw_wrote_them_go_after_the_startup_connector_pass(db):
    """The personal row as an administrator's create wrote it, through
    ``create_datasource`` (its Connector written through at birth), and the
    bundled row as ``init.py`` seeded it still go once
    ``migrate_stored_connectors`` has run, as it does at every start before
    this pass: the write-through stamp is not an administrator's edit."""
    personal = await db.create_datasource(
        name="Cloud Storage (Personal)",
        ds_type="webdav",
        connection_url="https://cloud.example/remote.php/dav/files/ada/",
        description="Personal cloud storage",
        credentials={"username": "agent-service", "password": "secret"},
        scope_mode="projects",
        auto_attach=False,
        project_ids=[_DEFAULT_PROJECT],
    )
    await db.upsert_default_datasource(
        name="Nextcloud",
        ds_type="webdav",
        connection_url=_ADMIN_HOME,
        credentials={"username": "admin", "password": "secret"},
    )
    await migrate_stored_connectors(db)
    async with db.acquire() as conn:
        stamped = await conn.fetchval(
            "SELECT manifest_resource_id IS NOT NULL FROM datasources "
            "WHERE id = $1::uuid",
            personal["id"],
        )
    assert stamped

    counts = await retire_legacy_cloud_datasources(db)

    assert counts == {"personal": 1, "bundled_nextcloud": 1}
    assert await _ids(db) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"description": "Ada's files"},
        {"created_by": _USER},
        {"is_global": True},
        {"projects": (_DEFAULT_PROJECT, _OTHER_PROJECT)},
        {"projects": (_OTHER_PROJECT,)},
        {"projects": ()},
    ],
    ids=["described", "owned", "global", "linked-twice", "team-project", "unlinked"],
)
async def test_a_personal_row_an_administrator_made_theirs_stays(db, overrides):
    kept = await _personal(db, **overrides)
    assert (await retire_legacy_cloud_datasources(db))["personal"] == 0
    assert await _ids(db) == {kept}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"name": "Admin files"},
        {"connection_url": "http://srw-nextcloud/remote.php/dav/files/ada/"},
        {"connection_url": "http://srw-nextcloud/remote.php/dav/files/admin/Team/"},
        {"created_by": _USER},
        {"is_global": False},
        {"description": "Admin's files"},
        {"projects": (_OTHER_PROJECT,)},
        {"projects": (_DEFAULT_PROJECT, _OTHER_PROJECT)},
    ],
    ids=[
        "renamed",
        "another-home",
        "a-subfolder",
        "owned",
        "not-global",
        "described",
        "linked",
        "linked-twice",
    ],
)
async def test_a_nextcloud_row_not_at_admins_home_or_not_ownerless_stays(db, overrides):
    shape = {"name": "Nextcloud", "connection_url": _ADMIN_HOME, "is_global": True}
    kept = await _row(db, **{**shape, **overrides})
    assert (await retire_legacy_cloud_datasources(db))["bundled_nextcloud"] == 0
    assert await _ids(db) == {kept}
