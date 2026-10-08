"""Datasources become Connectors, slice D3a, on a real PostgreSQL.

* the migrations (0350-0353) add the identity and marker columns to rows a
  pre-D3a database already holds, and validate their constraints;
* ``migrate_stored_connectors`` writes one Connector per eligible row with
  the right uid, name, scope, driver and access, touches no
  ``policy_revision``, link or reconcile entry, and is idempotent;
* the datasource store writes the row and its resource in one transaction
  (create, update, policy update, link, unlink, delete), and a failure in
  either half rolls both back;
* platform-owned and linked Connectors refuse the resource API.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import (
    DatasourcePolicyConflictError,
    PostgresDB,
    _encrypt_credentials_dict,
)
from orchestrator.services import manifest_connectors
from orchestrator.services.manifest_connectors import (
    DISPLAY_NAME,
    migrate_stored_connectors,
    persist_connector_resource,
)
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import (
    LINKED_CONNECTOR_MESSAGE,
    PLATFORM_MANAGED_MESSAGE,
    ManifestStore,
)
from shared.connectors.builtin import MCP_REMOTE_DRIVER
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
)
FIRST_D3A_MIGRATION = "0350"
SECRETS = ("s3cret-pass", "s3cret-header", "s3cret-arg", "s3cret-neo")


# =============================================================================
# Helpers
# =============================================================================


async def _user(db, name: str, *, admin: bool = False) -> str:
    return str(
        await db.fetchval(
            "INSERT INTO users(display_name,is_approved,is_admin) "
            "VALUES($1,TRUE,$2) RETURNING id",
            name,
            admin,
        )
    )


async def _project(db, name: str) -> str:
    return str(
        await db.fetchval("INSERT INTO projects(name) VALUES($1) RETURNING id", name)
    )


async def _raw_row(
    db,
    name: str,
    ds_type: str,
    *,
    owner: str | None,
    url: str | None = None,
    credentials: dict | None = None,
    config: dict | None = None,
    is_global: bool = False,
    read_only: bool | None = None,
    scope_mode: str = "all",
    links: tuple[str, ...] = (),
) -> str:
    """A row as an older orchestrator wrote it: no write-through."""
    datasource_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """INSERT INTO datasources(id,name,type,connection_url,credentials,
               config,created_by,is_global,read_only,scope_mode)
               VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10)""",
            datasource_id,
            name,
            ds_type,
            url,
            _encrypt_credentials_dict(credentials),
            json.dumps(config or {}),
            UUID(owner) if owner else None,
            is_global,
            read_only,
            scope_mode,
        )
        for project in links:
            await conn.execute(
                "INSERT INTO project_datasources(project_id,datasource_id,read_only) "
                "VALUES($1,$2,$3)",
                UUID(project),
                datasource_id,
                True if ds_type == "kb" else None,
            )
    return str(datasource_id)


async def _resource(db, datasource_id: str) -> dict | None:
    row = await db.fetchrow(
        "SELECT * FROM srw_resources WHERE id=$1", UUID(datasource_id)
    )
    if row is None:
        return None
    result = dict(row)
    result["document"] = json.loads(result["document"])
    return result


async def _row(db, datasource_id: str) -> dict | None:
    row = await db.fetchrow(
        "SELECT * FROM datasources WHERE id=$1", UUID(datasource_id)
    )
    return dict(row) if row else None


async def _policy_state(db) -> dict:
    """Everything the backfill must leave alone."""
    return {
        "revisions": {
            str(row["id"]): (row["policy_revision"], row["scope_mode"])
            for row in await db.fetch(
                "SELECT id,policy_revision,scope_mode FROM datasources"
            )
        },
        "links": sorted(
            (str(row["project_id"]), str(row["datasource_id"]), row["read_only"])
            for row in await db.fetch("SELECT * FROM project_datasources")
        ),
        "queue": [
            dict(row)
            for row in await db.fetch(
                "SELECT project_id,datasource_id,policy_revision,claim_token,"
                "updated_at FROM datasource_project_reconcile_queue "
                "ORDER BY project_id,datasource_id"
            )
        ],
    }


def _name_of(row_name: str, datasource_id: str) -> str:
    stem = re.sub(r"[^a-z0-9]+", "-", row_name.lower()).strip("-")
    return f"{stem}-{datasource_id.replace('-', '')[:12]}"


@pytest_asyncio.fixture
async def legacy_database(postgres_url, tmp_path):
    """A database migrated to just before D3a."""
    name = "d3a_legacy_" + uuid4().hex
    admin = await asyncpg.connect(postgres_url)
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    url = postgres_url.rsplit("/", 1)[0] + "/" + name
    before = tmp_path / "migrations"
    before.mkdir()
    for path in MIGRATIONS.glob("*.sql"):
        if path.name[:4] < FIRST_D3A_MIGRATION:
            (before / path.name).write_bytes(path.read_bytes())
    async with asyncpg.create_pool(url, min_size=1, max_size=2) as pool:
        await run_migrations(pool, before)
    db = PostgresDB(url, min_connections=1, max_connections=4)
    await db.connect()
    yield db
    await db.disconnect()


# =============================================================================
# The migrations and the startup backfill
# =============================================================================


@pytest.mark.asyncio
async def test_the_migrations_and_the_backfill_map_every_stored_row(legacy_database):
    db = legacy_database
    alice = await _user(db, "Alice")
    kb_project = await _project(db, "Alpha")
    other_project = await _project(db, "Beta")
    rows = {
        "pg": await _raw_row(
            db,
            "prod",
            "postgresql",
            owner=alice,
            url="postgresql://app:s3cret-pass@db.internal:5432/app",
            read_only=True,
        ),
        "neo4j": await _raw_row(
            db,
            "prod",
            "neo4j",
            owner=alice,
            url="bolt://graph.internal:7687",
            credentials={"username": "neo4j", "password": "s3cret-neo"},
        ),
        "mcp_remote": await _raw_row(
            db,
            "tickets",
            "mcp",
            owner=alice,
            url="https://mcp.example/mcp",
            credentials={
                "transport": "http",
                "auth": {"type": "headers", "headers": {"X-Key": "s3cret-header"}},
            },
        ),
        "mcp_stdio": await _raw_row(
            db,
            "local tool",
            "mcp",
            owner=alice,
            credentials={
                "transport": "stdio",
                "command": "npx",
                "args": ["--key", "s3cret-arg"],
            },
        ),
        "native_kb": await _raw_row(
            db,
            "Alpha Knowledge (abcd1234)",
            "kb",
            owner=alice,
            config={"root_path": "knowledge", "native_project_id": kb_project},
            read_only=True,
            scope_mode="projects",
            links=(kb_project,),
        ),
        "ownerless_linked": await _raw_row(
            db, "Shared DB", "postgresql", owner=None, links=(other_project,)
        ),
        "ownerless_global": await _raw_row(
            db,
            "Seeded DB",
            "postgresql",
            owner=None,
            is_global=True,
            read_only=True,
        ),
        "ownerless_two_links": await _raw_row(
            db,
            "Shared env",
            "generic",
            owner=None,
            scope_mode="projects",
            links=(kb_project, other_project),
        ),
    }
    before = await _policy_state(db)

    assert await db.apply_migrations()
    validated = {
        row["conname"]: row["convalidated"]
        for row in await db.fetch(
            "SELECT conname,convalidated FROM pg_constraint WHERE conname = ANY($1)",
            [
                "datasources_manifest_resource_id_fkey",
                "datasources_managed_key_shape",
            ],
        )
    }
    assert validated == {
        "datasources_manifest_resource_id_fkey": True,
        "datasources_managed_key_shape": True,
    }
    assert await db.fetchval(
        "SELECT indisvalid AND indisunique FROM pg_index "
        "WHERE indexrelid = 'uq_datasources_managed_key'::regclass"
    )
    assert not await db.fetchval(
        "SELECT count(*) FROM datasources "
        "WHERE manifest_resource_id IS NOT NULL OR managed_key IS NOT NULL"
    )

    counts = await migrate_stored_connectors(db)
    assert counts == {
        "created": 6,
        "updated": 0,
        "unchanged": 0,
        "legacy": 2,
        "deleted": 0,
        "deferred": 0,
    }
    expected = {
        "pg": (("Account", alice), "srw.postgresql/v1", "ReadOnly", None),
        "neo4j": (("Account", alice), "srw.neo4j/v1", None, None),
        "mcp_remote": (("Account", alice), MCP_REMOTE_DRIVER, None, None),
        "mcp_stdio": (("Account", alice), "srw.mcp/v1", None, None),
        "native_kb": (
            ("Project", kb_project),
            "srw.kb/v1",
            "ReadOnly",
            f"project-kb:{kb_project}",
        ),
        "ownerless_linked": (
            ("Project", other_project),
            "srw.postgresql/v1",
            None,
            None,
        ),
    }
    for label, (scope, driver, access, key) in expected.items():
        datasource_id = rows[label]
        row = await _row(db, datasource_id)
        resource = await _resource(db, datasource_id)
        document = resource["document"]
        assert resource["kind"] == "Connector", label
        assert str(resource["id"]) == datasource_id == str(resource["linked_id"])
        assert resource["deleted_at"] is None
        assert (resource["scope_kind"], resource["scope_name"]) == scope, label
        assert document["metadata"]["scope"] == {"kind": scope[0], "name": scope[1]}
        assert resource["name"] == _name_of(row["name"], datasource_id), label
        assert document["metadata"]["annotations"][DISPLAY_NAME] == row["name"]
        assert document["spec"]["driver"] == driver, label
        assert document["spec"].get("access") == access, label
        assert "credentials" not in document["spec"]
        assert resource["platform_managed"] == key, label
        assert row["managed_key"] == key, label
        assert str(row["manifest_resource_id"]) == datasource_id
        expected_owner = None if label == "ownerless_linked" else alice
        assert str(resource["owner_id"] or "") == (expected_owner or "")
        assert str(resource["project_id"] or "") == (
            scope[1] if scope[0] == "Project" else ""
        )
    # One owner's ``prod`` Postgres and ``prod`` Neo4j: one scope, two names.
    pg, neo4j = await _resource(db, rows["pg"]), await _resource(db, rows["neo4j"])
    assert pg["scope_name"] == neo4j["scope_name"] and pg["name"] != neo4j["name"]
    assert (await _resource(db, rows["mcp_remote"]))["document"]["spec"]["config"][
        "header_names"
    ] == ["X-Key"]
    for label in ("ownerless_global", "ownerless_two_links"):
        assert await _resource(db, rows[label]) is None
        assert (await _row(db, rows[label]))["manifest_resource_id"] is None
    stored = await db.fetchval(
        "SELECT string_agg(document::text, '') FROM srw_resource_revisions"
    )
    assert stored and not [secret for secret in SECRETS if secret in stored]
    assert await _policy_state(db) == before

    # A rerun is a no-op: same counts as unchanged, no new revision, no row write.
    versions = {
        str(row["id"]): row["resource_version"]
        for row in await db.fetch("SELECT id,resource_version FROM srw_resources")
    }
    revisions = await db.fetchval("SELECT count(*) FROM srw_resource_revisions")
    stamps = {
        str(row["id"]): row["updated_at"]
        for row in await db.fetch("SELECT id,updated_at FROM datasources")
    }
    again = await migrate_stored_connectors(db)
    assert again == {**counts, "created": 0, "unchanged": 6}
    assert {
        str(row["id"]): row["resource_version"]
        for row in await db.fetch("SELECT id,resource_version FROM srw_resources")
    } == versions
    assert await db.fetchval("SELECT count(*) FROM srw_resource_revisions") == (
        revisions
    )
    assert {
        str(row["id"]): row["updated_at"]
        for row in await db.fetch("SELECT id,updated_at FROM datasources")
    } == stamps
    assert await _policy_state(db) == before


@pytest.mark.asyncio
async def test_a_rerun_reconciles_what_an_older_orchestrator_wrote(database):
    owner = await _user(database, "Owner")
    created = await _raw_row(database, "orders", "postgresql", owner=owner)
    assert (await migrate_stored_connectors(database))["created"] == 1

    await database.execute(
        "UPDATE datasources SET name='orders v2', read_only=TRUE WHERE id=$1",
        UUID(created),
    )
    assert (await migrate_stored_connectors(database))["updated"] == 1
    document = (await _resource(database, created))["document"]
    assert document["metadata"]["annotations"][DISPLAY_NAME] == "orders v2"
    assert document["spec"]["access"] == "ReadOnly"

    await database.execute("DELETE FROM datasources WHERE id=$1", UUID(created))
    assert (await migrate_stored_connectors(database))["deleted"] == 1
    assert (await _resource(database, created))["deleted_at"] is not None


# =============================================================================
# The write-through
# =============================================================================


@pytest.mark.asyncio
async def test_writes_keep_the_row_and_its_connector_in_step(database):
    owner = await _user(database, "Owner")
    project = await _project(database, "Alpha")
    created = await database.create_datasource(
        name="warehouse",
        ds_type="postgresql",
        connection_url="postgresql://reader:s3cret-pass@wh.internal/wh",
        created_by=owner,
    )
    datasource_id = str(created["id"])
    resource = await _resource(database, datasource_id)
    assert resource["resource_version"] == 1
    assert resource["document"]["spec"]["config"]["connection_url"] == (
        "postgresql://wh.internal/wh"
    )
    assert (await _row(database, datasource_id))["manifest_resource_id"] == UUID(
        datasource_id
    )

    assert await database.update_datasource(
        datasource_id, name="warehouse eu", read_only=True
    )
    resource = await _resource(database, datasource_id)
    assert resource["resource_version"] == 2
    assert resource["document"]["metadata"]["annotations"][DISPLAY_NAME] == (
        "warehouse eu"
    )
    assert resource["document"]["spec"]["access"] == "ReadOnly"
    # The identity was fixed at creation; a rename does not move it.
    assert resource["name"] == _name_of("warehouse", datasource_id)

    # Sharing and policy are never authored: these leave the resource alone.
    revision = (await _row(database, datasource_id))["policy_revision"]
    updated = await database.update_datasource_with_policy(
        datasource_id,
        expected_policy_revision=revision,
        scope_mode="projects",
        project_ids=[project],
        auto_attach=True,
    )
    assert updated["project_ids"] == [project]
    assert await database.unlink_datasource_from_project(project, datasource_id)
    assert await database.link_datasource_to_project(project, datasource_id)
    assert (await _resource(database, datasource_id))["resource_version"] == 2

    assert await database.delete_datasource(datasource_id, deleted_by=owner)
    resource = await _resource(database, datasource_id)
    assert resource["deleted_at"] is not None
    assert await database.fetchval(
        "SELECT name FROM datasource_tombstones WHERE id=$1", UUID(datasource_id)
    ) == ("warehouse eu")


class _Refused(RuntimeError):
    pass


def _refuse(*_args, **_kwargs):
    raise _Refused("the resource half failed")


@pytest.mark.asyncio
async def test_a_failure_in_either_half_rolls_both_back(database, monkeypatch):
    owner = await _user(database, "Owner")
    project = await _project(database, "Alpha")
    kept = await database.create_datasource(
        name="kept", ds_type="postgresql", created_by=owner
    )
    kept_id = str(kept["id"])
    rows = await database.fetchval("SELECT count(*) FROM datasources")
    resources = await database.fetchval("SELECT count(*) FROM srw_resources")

    # The resource half fails: no row is created, updated, linked or deleted.
    monkeypatch.setattr(manifest_connectors.ManifestStore, "save", _refuse)
    with pytest.raises(_Refused):
        await database.create_datasource(
            name="lost", ds_type="postgresql", created_by=owner
        )
    with pytest.raises(_Refused):
        await database.update_datasource(kept_id, name="renamed")
    monkeypatch.undo()
    monkeypatch.setattr(manifest_connectors, "_retire", _refuse)
    with pytest.raises(_Refused):
        await database.delete_datasource(kept_id)
    monkeypatch.undo()
    monkeypatch.setattr(manifest_connectors, "persist_connector_resource", _refuse)
    with pytest.raises(_Refused):
        await database.link_datasource_to_project(project, kept_id)
    monkeypatch.undo()
    assert await database.fetchval("SELECT count(*) FROM datasources") == rows
    assert (await _row(database, kept_id))["name"] == "kept"
    assert not await database.fetchval(
        "SELECT count(*) FROM project_datasources WHERE datasource_id=$1",
        UUID(kept_id),
    )
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == resources
    assert (await _resource(database, kept_id))["deleted_at"] is None

    # The row half fails: nothing reaches the resource.
    with pytest.raises(asyncpg.UniqueViolationError):
        await database.create_datasource(
            name="kept", ds_type="postgresql", created_by=owner
        )
    with pytest.raises(DatasourcePolicyConflictError):
        await database.update_datasource_with_policy(
            kept_id, expected_policy_revision=999, name="stale"
        )
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == resources
    assert (await _resource(database, kept_id))["resource_version"] == 1


@pytest.mark.asyncio
async def test_an_ownerless_row_follows_its_one_link(database):
    first = await _project(database, "Alpha")
    second = await _project(database, "Beta")
    created = await database.create_datasource(
        name="shared", ds_type="postgresql", project_ids=[first], scope_mode="projects"
    )
    datasource_id = str(created["id"])
    resource = await _resource(database, datasource_id)
    assert (resource["scope_kind"], resource["scope_name"]) == ("Project", first)

    # A second link leaves no one project to own it: back to the legacy path.
    await database.link_datasource_to_project(second, datasource_id)
    assert (await _resource(database, datasource_id))["deleted_at"] is not None

    # One link again: the same uid is revived in that project.
    await database.unlink_datasource_from_project(first, datasource_id)
    resource = await _resource(database, datasource_id)
    assert resource["deleted_at"] is None
    assert (resource["scope_kind"], resource["scope_name"]) == ("Project", second)
    assert resource["document"]["metadata"]["scope"] == {
        "kind": "Project",
        "name": second,
    }


@pytest.mark.asyncio
async def test_an_adopted_kb_moves_to_its_project_and_is_platform_managed(
    database, actor
):
    owner = str(actor["id"])
    project = await _project(database, "Alpha")
    created = await database.create_datasource(
        name="team notes",
        ds_type="kb",
        connection_url="https://git.example/acme/kb.git",
        config={"root_path": "knowledge"},
        created_by=owner,
        read_only=True,
    )
    datasource_id = str(created["id"])
    assert (await _resource(database, datasource_id))["scope_kind"] == "Account"

    # project_provisioning.adopt_kb_connector_as_vault's write.
    await database.update_datasource_with_policy(
        datasource_id,
        expected_policy_revision=created["policy_revision"],
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project],
        config={"root_path": "knowledge", "native_project_id": project},
    )
    key = f"project-kb:{project}"
    resource = await _resource(database, datasource_id)
    assert (resource["scope_kind"], resource["scope_name"]) == ("Project", project)
    assert resource["platform_managed"] == key
    assert "native_project_id" not in resource["document"]["spec"]["config"]
    row = await _row(database, datasource_id)
    assert row["managed_key"] == key
    assert json.loads(row["config"])["native_project_id"] == project  # the mirror

    store = ManifestStore(database)
    current = await store.by_id(datasource_id)
    with pytest.raises(HTTPException) as deleted:
        await store.delete(current, expected_version=current["resource_version"])
    assert (deleted.value.status_code, deleted.value.detail) == (
        409,
        PLATFORM_MANAGED_MESSAGE,
    )
    edited = json.loads(json.dumps(current["document"]))
    edited["metadata"]["labels"] = {"edited": "yes"}
    with pytest.raises(HTTPException) as applied:
        await ManifestResourceService(database).apply(
            json.dumps(edited),
            actor,
            format="json",
            expected_versions={
                f"Connector/Project/{project}/{current['name']}": current[
                    "resource_version"
                ]
            },
        )
    assert (applied.value.status_code, applied.value.detail) == (
        409,
        PLATFORM_MANAGED_MESSAGE,
    )


@pytest.mark.asyncio
async def test_a_linked_connector_refuses_the_resource_api(database, actor):
    created = await database.create_datasource(
        name="orders", ds_type="postgresql", created_by=str(actor["id"])
    )
    store = ManifestStore(database)
    current = await store.by_id(str(created["id"]))
    with pytest.raises(HTTPException) as deleted:
        await store.delete(current, expected_version=current["resource_version"])
    assert (deleted.value.status_code, deleted.value.detail) == (
        409,
        LINKED_CONNECTOR_MESSAGE,
    )
    edited = json.loads(json.dumps(current["document"]))
    edited["spec"]["config"]["connection_url"] = "postgresql://elsewhere/db"
    with pytest.raises(HTTPException) as applied:
        await ManifestResourceService(database).apply(
            json.dumps(edited),
            actor,
            format="json",
            expected_versions={
                f"Connector/Account/{actor['id']}/{current['name']}": current[
                    "resource_version"
                ]
            },
        )
    assert (applied.value.status_code, applied.value.detail) == (
        409,
        LINKED_CONNECTOR_MESSAGE,
    )
    assert (await store.by_id(str(created["id"])))["resource_version"] == 1


@pytest.mark.asyncio
async def test_a_managed_key_names_one_row(database):
    owner = await _user(database, "Owner")
    project = await _project(database, "Alpha")
    marker = {"root_path": "knowledge", "native_project_id": project}
    first = await _raw_row(database, "KB one", "kb", owner=owner, config=marker)
    second = await _raw_row(database, "KB two", "kb", owner=owner, config=marker)
    counts = await migrate_stored_connectors(database)
    assert counts["created"] == 2 and counts["deferred"] == 0
    keys = [(await _row(database, first))["managed_key"]]
    keys.append((await _row(database, second))["managed_key"])
    assert set(keys) == {f"project-kb:{project}", None}
    # Both still count as platform-owned through the marker.
    for datasource_id in (first, second):
        resource = await _resource(database, datasource_id)
        assert resource["platform_managed"] == f"project-kb:{project}"


@pytest.mark.asyncio
async def test_persisting_inside_a_transaction_is_idempotent(database):
    owner = await _user(database, "Owner")
    created = await _raw_row(database, "orders", "postgresql", owner=owner)
    outcomes = []
    for _ in range(2):
        async with database.transaction_scope():
            outcomes.append(await persist_connector_resource(database, created))
    assert outcomes == ["created", "unchanged"]
