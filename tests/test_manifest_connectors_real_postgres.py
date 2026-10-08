"""Datasources become Connectors, slice D3a, on a real PostgreSQL.

* the migrations (0350-0352) add the identity and marker columns to rows a
  pre-D3a database already holds and validate their constraints; they retry
  past application transactions that lock the two tables in either order,
  leaving no dirty ledger row, and applying any of them again is a no-op;
* ``migrate_stored_connectors`` writes one Connector per eligible row with
  the right uid, name, scope, driver and access, touches no
  ``policy_revision``, link or reconcile entry, is idempotent, works in
  batches and skips rows already in step (timed at 1,500 and 6,000 rows),
  including a row an older replica renamed in a transaction that began before
  the last sync, and never looks at legacy job clones;
* the datasource store writes the row and its resource in one transaction
  (create, update, policy update, link, unlink, delete, default seeding), and
  a failure in either half rolls both back; nothing secret-looking reaches a
  stored revision;
* a session settings save and a connector write, in either order, never
  deadlock;
* platform-owned and linked Connectors refuse the resource API.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
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
from jsonschema import Draft202012Validator

from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.manifest_connectors import (
    _NEEDS_WORK,
    DISPLAY_NAME,
    migrate_stored_connectors,
    persist_connector_resource,
)
from orchestrator.services.manifest_execution_retirement import (
    lock_manifest_execution_catalog,
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
SECRETS = (
    "s3cret-pass",
    "s3cret-header",
    "s3cret-arg",
    "s3cret-neo",
    "s3cret-path",
    "s3cret-query",
    "s3cret-hint",
    "s3cret-cmd",
)
REGISTRY = builtin_connector_drivers()


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


async def _needing_work(db) -> set[str]:
    return {str(row["id"]) for row in await db.fetch(_NEEDS_WORK, UUID(int=0), 100_000)}


async def _schema_problems(db) -> list[str]:
    """Every live Connector's config against its driver's config_schema."""
    problems = []
    for row in await db.fetch(
        "SELECT name, document FROM srw_resources WHERE kind='Connector' "
        "AND deleted_at IS NULL"
    ):
        spec = json.loads(row["document"])["spec"]
        driver = REGISTRY.get(spec["driver"])
        if driver is None:
            problems.append(f"{row['name']}: driver {spec['driver']} not installed")
            continue
        validator = Draft202012Validator(dict(driver.spec.config_schema))
        problems += [
            f"{row['name']}: {error.message}"
            for error in validator.iter_errors(spec.get("config", {}))
        ]
    return problems


async def _stored_revisions(db) -> str:
    return (
        await db.fetchval(
            "SELECT string_agg(document::text, '') FROM srw_resource_revisions"
        )
        or ""
    )


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
        # Since D3b the spec names its secret's keys, never their values.
        secret_name = "connector-" + datasource_id.replace("-", "")
        assert {
            ref["secretRef"]["name"] for ref in document["spec"]["credentials"].values()
        } <= {secret_name}, label
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
    stored = await _stored_revisions(db)
    assert stored and not [secret for secret in SECRETS if secret in stored]
    assert await _schema_problems(db) == []
    assert await _policy_state(db) == before
    # Only the two legacy-path rows are looked at again.
    assert await _needing_work(db) == {
        rows["ownerless_global"],
        rows["ownerless_two_links"],
    }

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
    assert again == {**counts, "created": 0}
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
    assert resource["document"]["spec"]["config"] == {
        "endpoint": "postgresql://wh.internal"
    }
    assert datasource_id not in await _needing_work(database)
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
    assert datasource_id not in await _needing_work(database)
    assert await database.unlink_datasource_from_project(project, datasource_id)
    assert await database.link_datasource_to_project(project, datasource_id)
    assert (await _resource(database, datasource_id))["resource_version"] == 2
    # Every write left the resource in step: the startup backfill skips it.
    assert datasource_id not in await _needing_work(database)

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
async def test_an_ownerless_rows_connector_keeps_the_project_it_was_made_in(
    database,
):
    first = await _project(database, "Alpha")
    second = await _project(database, "Beta")
    created = await database.create_datasource(
        name="shared", ds_type="postgresql", project_ids=[first], scope_mode="projects"
    )
    datasource_id = str(created["id"])
    resource = await _resource(database, datasource_id)
    assert (resource["scope_kind"], resource["scope_name"]) == ("Project", first)

    # Links are sharing, never scope: neither a second link nor dropping the
    # first moves or retires the Connector, so a reference to it holds.
    await database.link_datasource_to_project(second, datasource_id)
    await database.unlink_datasource_from_project(first, datasource_id)
    after = await _resource(database, datasource_id)
    assert after["deleted_at"] is None
    assert (after["scope_kind"], after["scope_name"]) == ("Project", first)
    assert after["resource_version"] == resource["resource_version"]

    # Deleting that project retires it with the project's other definitions;
    # the row lives on, and its next write brings the same uid back in the
    # one project it is linked to.
    await database.delete_project(first)
    assert (await _resource(database, datasource_id))["deleted_at"] is not None
    assert datasource_id in await _needing_work(database)
    assert await database.update_datasource(datasource_id, description="moved")
    revived = await _resource(database, datasource_id)
    assert revived["deleted_at"] is None
    assert (revived["scope_kind"], revived["scope_name"]) == ("Project", second)
    assert revived["document"]["metadata"]["scope"] == {
        "kind": "Project",
        "name": second,
    }


@pytest.mark.asyncio
async def test_an_ownerless_row_without_one_project_has_no_connector(database):
    first = await _project(database, "Alpha")
    second = await _project(database, "Beta")
    created = await database.create_datasource(
        name="wide",
        ds_type="postgresql",
        project_ids=[first, second],
        scope_mode="projects",
    )
    assert await _resource(database, str(created["id"])) is None
    assert (await _row(database, str(created["id"])))["manifest_resource_id"] is None


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


# =============================================================================
# Driver changes, default seeding, secrets in stored revisions
# =============================================================================


@pytest.mark.asyncio
async def test_a_transport_edit_changes_the_connectors_driver(database):
    owner = await _user(database, "Owner")
    created = await database.create_datasource(
        name="tools",
        ds_type="mcp",
        connection_url="https://mcp.example/api/mcp/s/s3cret-path/mcp",
        credentials={
            "transport": "http",
            "auth": {"type": "bearer", "token": "s3cret-header"},
        },
        created_by=owner,
    )
    datasource_id = str(created["id"])
    remote = await _resource(database, datasource_id)
    assert remote["document"]["spec"]["driver"] == "srw.mcp-remote/v1"
    assert remote["document"]["spec"]["config"] == {
        "endpoint": "https://mcp.example",
        "transport": "http",
        "auth_type": "bearer",
    }

    assert await database.update_datasource(
        datasource_id,
        connection_url=None,
        connection_url_set=True,
        credentials={"transport": "stdio", "command": "npx s3cret-cmd"},
    )
    stdio = await _resource(database, datasource_id)
    assert (stdio["id"], stdio["name"]) == (remote["id"], remote["name"])
    assert stdio["resource_version"] == remote["resource_version"] + 1
    assert stdio["document"]["spec"]["driver"] == "srw.mcp/v1"
    assert stdio["document"]["spec"]["config"] == {"transport": "stdio"}
    assert await _schema_problems(database) == []
    stored = await _stored_revisions(database)
    assert not [secret for secret in SECRETS if secret in stored]


@pytest.mark.asyncio
async def test_default_seeding_writes_through_a_linked_seed(database):
    project = await _project(database, "Alpha")
    seeded = await database.upsert_default_datasource(
        "Seeded DB", "postgresql", "postgresql://seed:s3cret-pass@one.internal/db"
    )
    datasource_id = str(seeded["id"])
    # Ownerless, global and unlinked: the legacy path.
    assert await _resource(database, datasource_id) is None
    await database.link_datasource_to_project(project, datasource_id)
    first = await _resource(database, datasource_id)
    assert first["document"]["spec"]["config"]["endpoint"] == (
        "postgresql://one.internal"
    )
    # Reseeding it rewrites the Connector it now has.
    await database.upsert_default_datasource(
        "Seeded DB", "postgresql", "postgresql://seed:s3cret-pass@two.internal/db"
    )
    second = await _resource(database, datasource_id)
    assert second["document"]["spec"]["config"]["endpoint"] == (
        "postgresql://two.internal"
    )
    assert second["resource_version"] == first["resource_version"] + 1
    assert "s3cret-pass" not in await _stored_revisions(database)


@pytest.mark.asyncio
async def test_no_secret_looking_part_reaches_a_stored_revision(database):
    owner = await _user(database, "Owner")
    rows = [
        dict(
            name="zapier",
            ds_type="mcp",
            connection_url="https://mcp.zapier.com/api/mcp/s/s3cret-path/mcp",
            credentials={"transport": "http"},
        ),
        dict(
            name="api",
            ds_type="generic",
            connection_url="https://api.example.com/v1?code=s3cret-query&k=s3cret-query",
            cli_hint="curl -H 'Authorization: Bearer s3cret-hint' https://api.example.com",
        ),
        dict(
            name="warehouse",
            ds_type="postgresql",
            connection_url=(
                "jdbc:postgresql://db.example:5432/app?user=s3cret-query"
                "&password=s3cret-pass"
            ),
        ),
        dict(
            name="files",
            ds_type="webdav",
            connection_url="https://dav.example/remote.php/dav/files/s3cret-path/",
        ),
        dict(
            name="local",
            ds_type="mcp",
            credentials={
                "transport": "stdio",
                "command": "env TOKEN=s3cret-cmd npx server",
                "args": ["--key", "s3cret-arg"],
            },
        ),
    ]
    for body in rows:
        await database.create_datasource(created_by=owner, **body)
    stored = await _stored_revisions(database)
    assert stored.count('"kind": "Connector"') == len(rows)
    assert not [secret for secret in SECRETS if secret in stored]
    assert await _schema_problems(database) == []


# =============================================================================
# The startup backfill at scale
# =============================================================================


async def _bulk_rows(db, owner: str, count: int) -> None:
    """``count`` rows as an orchestrator without the write-through wrote them."""
    await db.execute(
        """INSERT INTO datasources (name, type, connection_url, created_by)
           SELECT 'bulk ' || n, 'postgresql',
                  'postgresql://app:s3cret-pass@db' || n || '.internal:5432/app', $1
           FROM generate_series(1, $2) AS n""",
        UUID(owner),
        count,
    )


async def _timed_backfill(db) -> tuple[dict, float, int]:
    """The backfill's counts, its wall time, and the most advisory locks any
    one backend held while it ran."""
    peak = 0
    done = asyncio.Event()

    async def sample() -> None:
        nonlocal peak
        connection = await asyncpg.connect(db._connection_string)
        try:
            while not done.is_set():
                held = await connection.fetchval(
                    "SELECT coalesce(max(n), 0) FROM (SELECT count(*) AS n "
                    "FROM pg_locks WHERE locktype = 'advisory' GROUP BY pid) h"
                )
                peak = max(peak, int(held))
                await asyncio.sleep(0.005)
        finally:
            await connection.close()

    sampler = asyncio.create_task(sample())
    started = time.perf_counter()
    try:
        counts = await migrate_stored_connectors(db)
    finally:
        elapsed = time.perf_counter() - started
        done.set()
        await sampler
    return counts, elapsed, peak


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1500, 6000])
async def test_the_backfill_batches_and_skips_rows_in_step(database, count):
    owner = await _user(database, "Owner")
    await _bulk_rows(database, owner, count)

    counts, first, peak = await _timed_backfill(database)
    assert counts["created"] == count and counts["deferred"] == 0
    # One batch at a time: the catalog lock plus an identity lock per row.
    assert peak <= 2 * 50 + 2, peak

    again, rerun, _ = await _timed_backfill(database)
    assert again == dict.fromkeys(again, 0)
    assert await _needing_work(database) == set()
    print(
        f"\nbackfill {count} rows: first {first:.2f}s, rerun {rerun:.3f}s, "
        f"peak advisory locks per backend {peak}"
    )


# =============================================================================
# A session settings save never deadlocks against a connector write
# =============================================================================


async def _blocked(db, *, timeout: float = 10.0) -> None:
    """Wait until some backend waits for a lock."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await db.fetchval("SELECT count(*) FROM pg_locks WHERE NOT granted"):
            return
        await asyncio.sleep(0.02)
    raise AssertionError("nothing ever waited for a lock")


async def _settings_save(db, thread_id, datasource_id, entered, release, *, fixed):
    """A session settings save's lock order (thread_config_update): the thread
    row, the selected connectors' rows, then the catalog lock that capturing
    the session revision takes. ``fixed=False`` is the order before this fix,
    without the catalog lock up front."""
    scope = (
        db.thread_configuration_transaction(thread_id)
        if fixed
        else db.transaction_scope()
    )
    async with scope as conn:
        await conn.fetchrow(
            "SELECT 1 FROM threads WHERE id=$1 FOR UPDATE", UUID(thread_id)
        )
        await conn.fetchrow(
            "SELECT 1 FROM datasources WHERE id=$1 FOR UPDATE", UUID(datasource_id)
        )
        entered.set()
        await release.wait()
        await lock_manifest_execution_catalog(conn)


def _connector_write(db, op, datasource_id, project):
    return {
        "delete": lambda: db.delete_datasource(datasource_id),
        "update": lambda: db.update_datasource(datasource_id, name="renamed"),
        "link": lambda: db.link_datasource_to_project(project, datasource_id),
        "unlink": lambda: db.unlink_datasource_from_project(project, datasource_id),
    }[op]


async def _deadlock_fixture(db):
    owner = await _user(db, "Owner")
    project = await _project(db, "Alpha")
    created = await db.create_datasource(
        name="selected", ds_type="postgresql", created_by=owner
    )
    datasource_id = str(created["id"])
    await db.execute(
        "INSERT INTO project_datasources (project_id, datasource_id) VALUES ($1,$2)",
        UUID(project),
        UUID(datasource_id),
    )
    thread_id = str(uuid4())
    await db.execute(
        "INSERT INTO threads (id, title, status, metadata, execution_lane) "
        "VALUES ($1, 'settings', 'active', $2::jsonb, 'stateless')",
        UUID(thread_id),
        json.dumps({"datasource_ids": [datasource_id]}),
    )
    return thread_id, datasource_id, project


async def _race(db, op, order, *, fixed, monkeypatch):
    thread_id, datasource_id, project = await _deadlock_fixture(db)
    write = _connector_write(db, op, datasource_id, project)
    entered, release = asyncio.Event(), asyncio.Event()
    if order == "save_first":
        save = asyncio.create_task(
            _settings_save(db, thread_id, datasource_id, entered, release, fixed=fixed)
        )
        await asyncio.wait_for(entered.wait(), 10)
        writer = asyncio.create_task(write())
        await _blocked(db)
        release.set()
    else:
        holding, resume = asyncio.Event(), asyncio.Event()
        catalog = db._lock_connector_catalog

        async def paused_catalog():
            await catalog()
            holding.set()
            await resume.wait()

        monkeypatch.setattr(db, "_lock_connector_catalog", paused_catalog)
        writer = asyncio.create_task(write())
        await asyncio.wait_for(holding.wait(), 10)
        release.set()
        save = asyncio.create_task(
            _settings_save(db, thread_id, datasource_id, entered, release, fixed=fixed)
        )
        if fixed:
            await _blocked(db)
        else:
            await asyncio.wait_for(entered.wait(), 10)
        resume.set()
    return await asyncio.wait_for(
        asyncio.gather(save, writer, return_exceptions=True), 30
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["save_first", "write_first"])
@pytest.mark.parametrize("op", ["delete", "update", "link", "unlink"])
async def test_a_settings_save_and_a_connector_write_never_deadlock(
    database, op, order, monkeypatch
):
    outcomes = await _race(database, op, order, fixed=True, monkeypatch=monkeypatch)
    assert [o for o in outcomes if isinstance(o, BaseException)] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["save_first", "write_first"])
async def test_control_the_old_lock_order_deadlocks(database, order, monkeypatch):
    """Without the catalog lock up front the same race is a 40P01."""
    outcomes = await _race(
        database, "delete", order, fixed=False, monkeypatch=monkeypatch
    )
    assert any(isinstance(o, asyncpg.DeadlockDetectedError) for o in outcomes), outcomes


# =============================================================================
# The migrations against live application transactions, and applied again
# =============================================================================

D3A_FILES = (
    "0350_datasource_manifest_identity.sql",
    "0351_validate_datasource_manifest_identity.sql",
    "0352_datasources_managed_key_idx.notx.sql",
)


async def _ledger(db) -> list[tuple[str, bool]]:
    return [
        (row["filename"], row["success"])
        for row in await db.fetch(
            "SELECT filename, success FROM schema_migrations "
            "WHERE filename >= '0350' ORDER BY filename"
        )
    ]


async def _waiting_on(db, table: str, *, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await db.fetchval(
            "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            "WHERE c.relname = $1 AND NOT l.granted",
            table,
        ):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"nothing waited for {table}")


@pytest.mark.asyncio
async def test_the_migration_waits_out_a_manifest_apply_without_deadlock(
    legacy_database,
):
    """The review's scenario: an application transaction has written
    srw_resources (a manifest apply's store.save) and then reads datasources
    (admission's policy snapshot) while 0350 runs."""
    db = legacy_database
    owner = await _user(db, "Owner")
    await _raw_row(db, "orders", "postgresql", owner=owner)
    app = await asyncpg.connect(db._connection_string)
    transaction = app.transaction()
    await transaction.start()
    await app.execute("LOCK TABLE srw_resources IN ROW EXCLUSIVE MODE")
    runner = asyncio.create_task(db.apply_migrations())
    await _waiting_on(db, "srw_resources")
    # 0350 holds nothing while it waits for srw_resources, so the read runs.
    assert await app.fetchval("SELECT count(*) FROM datasources") == 1
    await transaction.rollback()
    await app.close()
    await asyncio.wait_for(runner, 120)
    assert await _ledger(db) == [(name, True) for name in D3A_FILES]
    assert await db.apply_migrations()
    assert await _ledger(db) == [(name, True) for name in D3A_FILES]


@pytest.mark.asyncio
async def test_a_deadlock_with_the_reverse_order_is_retried_not_recorded(
    legacy_database,
):
    """The write-through's order: an application transaction reads
    datasources, then writes srw_resources, while 0350 holds srw_resources and
    waits for datasources. Whichever side PostgreSQL ends, the migration
    retries and completes with a clean ledger."""
    db = legacy_database
    app = await asyncpg.connect(db._connection_string)
    transaction = app.transaction()
    await transaction.start()
    await app.fetchval("SELECT count(*) FROM datasources")
    runner = asyncio.create_task(db.apply_migrations())
    await _waiting_on(db, "datasources")
    try:
        await app.execute("LOCK TABLE srw_resources IN ROW EXCLUSIVE MODE")
    except asyncpg.DeadlockDetectedError:
        pass  # the application was the victim; it would retry
    await transaction.rollback()
    await app.close()
    await asyncio.wait_for(runner, 120)
    assert await _ledger(db) == [(name, True) for name in D3A_FILES]
    assert not await db.fetchval(
        "SELECT count(*) FROM schema_migrations WHERE NOT success"
    )


@pytest.mark.asyncio
async def test_each_d3a_migration_applied_again_is_a_no_op(database):
    """As if someone deleted the ledger rows by hand: the schema already has
    every object (it is schema_current), and each file runs twice."""

    async def shape() -> tuple:
        return (
            await database.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = "
                "'public.datasources'::regclass AND conname IN "
                "('datasources_managed_key_shape', "
                "'datasources_manifest_resource_id_fkey') AND convalidated"
            ),
            await database.fetchval(
                "SELECT count(*) FROM information_schema.columns WHERE "
                "(table_name, column_name) IN (('datasources', 'managed_key'), "
                "('datasources', 'manifest_resource_id'), "
                "('srw_resources', 'platform_managed'), "
                "('srw_resources', 'linked_updated_at'))"
            ),
            await database.fetchval(
                "SELECT count(*) FROM pg_index WHERE indisvalid AND "
                "indexrelid = 'uq_datasources_managed_key'::regclass"
            ),
        )

    assert await shape() == (2, 4, 1)
    connection = await asyncpg.connect(database._connection_string)
    try:
        for name in D3A_FILES:
            for _ in range(2):
                await connection.execute((MIGRATIONS / name).read_text())
    finally:
        await connection.close()
    assert await shape() == (2, 4, 1)


# =============================================================================
# What the backfill looks at
# =============================================================================


@pytest.mark.asyncio
async def test_a_rename_an_older_replica_began_before_the_sync_is_found(database):
    """The review's interleaving: an older replica's transaction starts, a
    write-through for the same row commits, then the older transaction renames
    the row. Its updated_at is its start time, earlier than the sync's."""
    owner = await _user(database, "Owner")
    created = await database.create_datasource(
        name="orig", ds_type="postgresql", created_by=owner
    )
    datasource_id = str(created["id"])
    older = await asyncpg.connect(database._connection_string)
    transaction = older.transaction()
    await transaction.start()
    await older.fetchval("SELECT now()")
    await asyncio.sleep(0.05)
    assert await database.update_datasource(datasource_id, description="touch")
    synced = await database.fetchval(
        "SELECT linked_updated_at FROM srw_resources WHERE id=$1", UUID(datasource_id)
    )
    await older.execute(
        "UPDATE datasources SET name='renamed by the older replica' WHERE id=$1",
        UUID(datasource_id),
    )
    await transaction.commit()
    await older.close()
    stamp = await database.fetchval(
        "SELECT updated_at FROM datasources WHERE id=$1", UUID(datasource_id)
    )
    assert stamp < synced  # the case a newer-than test misses
    assert datasource_id in await _needing_work(database)
    assert (await migrate_stored_connectors(database))["updated"] == 1
    resource = await _resource(database, datasource_id)
    assert resource["document"]["metadata"]["annotations"][DISPLAY_NAME] == (
        "renamed by the older replica"
    )
    assert datasource_id not in await _needing_work(database)


@pytest.mark.asyncio
async def test_legacy_job_clones_are_never_looked_at(database):
    owner = await _user(database, "Owner")
    job = await database.fetchval(
        "INSERT INTO jobs (description) VALUES ('clone owner') RETURNING id"
    )
    clone = await database.fetchval(
        "INSERT INTO datasources (name, type, created_by, job_id) "
        "VALUES ('clone', 'postgresql', $1, $2) RETURNING id",
        UUID(owner),
        job,
    )
    assert str(clone) not in await _needing_work(database)
    assert await migrate_stored_connectors(database) == dict.fromkeys(
        ("created", "updated", "unchanged", "legacy", "deleted", "deferred"), 0
    )
    # A write to it never makes it a Connector either.
    assert await database.update_datasource(str(clone), description="still a clone")
    assert await _resource(database, str(clone)) is None
