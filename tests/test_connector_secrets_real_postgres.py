"""Connector drivers D3b on a real PostgreSQL: Connector secrets.

* ``migrate_stored_connectors`` gives every Connector one
  ``connector-<32 hex>`` secret in its scope with the slot keys its driver
  names, rebuilds a pre-D3b resource's references, drops the secret of a
  retired Connector, and is a no-op when rerun;
* the datasource store's create, update and delete keep the secret in step
  in their transaction; through the API's update rules a blank edit keeps the
  secret, a ``credentials`` edit merges and any other edit replaces; a
  Connector that moves into its project takes its secret with it;
* decision 11: the Connector's own secret is lent to the owner, to anyone's
  work for a public connector, and to a member's work in a linked project,
  never to an unauthorized user or from the Catalog; a project member resolves
  the project's knowledge base Connector with its secret;
* a session's delivery reads the secret (and falls back to the row when the
  resource is stale), for a non-owner too; an unauthorized attach is refused;
* no other resource may reference a Connector's secret, its owner included;
* the resource API refuses a ``connector-`` secret, and no secret value
  reaches a stored revision.
"""

from __future__ import annotations

import functools
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import _encrypt_credentials_dict
from orchestrator.schemas.datasources import DatasourceUpdate
from orchestrator.security.crypto import decrypt
from orchestrator.services import datasources as datasource_service
from orchestrator.services.agent_datasource_payload import (
    DatasourcePayloadDependencies,
    build_datasources_payload,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_secrets import (
    CATALOG_SECRET_DETAIL,
    CONNECTOR_SECRET_DETAIL,
    FOREIGN_CONNECTOR_SECRET_DETAIL,
    ROW_KEY,
    SHAPE_KEY,
    URL_KEY,
    connector_secret_name,
    read_connector_credentials,
    row_digest,
    stored_credentials,
    write_connector_secret,
)
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_connectors import (
    _NEEDS_WORK,
    migrate_stored_connectors,
    persist_connector_resource,
)
from orchestrator.services.manifest_resolution import LiveManifestResolver
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import LINKED_CONNECTOR_MESSAGE, ManifestStore
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    authorize_thread_datasource_selection,
    resolve_authorized_thread_datasources,
)
from orchestrator.services.datasource_policy import (
    DatasourceUnavailableError,
    authorize_datasource_selection,
)
from orchestrator.services.job_datasource_selection import (
    JobDatasourceSelectionDependencies,
    resolve_authorized_job_datasources,
    revalidate_job_datasource_selection,
)
from orchestrator.services.workspace_tier_policy import backend_from_override
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url

REGISTRY = builtin_connector_drivers()
SECRETS = (
    "s3cret-pass",
    "s3cret-neo",
    "s3cret-header",
    "s3cret-arg",
    "s3cret-env",
    "s3cret-file",
    "s3cret-token",
    "s3cret-rotated",
)


# =============================================================================
# Helpers
# =============================================================================


async def _user(db, name: str, *, admin: bool = False) -> dict:
    return dict(
        await db.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) "
            "VALUES($1,TRUE,$2) RETURNING *",
            name,
            admin,
        )
    )


async def _project(db, name: str, members: dict[str, str]) -> str:
    project = await db.fetchval(
        "INSERT INTO projects(name) VALUES($1) RETURNING id", name
    )
    for user_id, role in members.items():
        await db.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,$3)",
            project,
            UUID(user_id),
            role,
        )
    return str(project)


async def _raw_row(
    db,
    name: str,
    ds_type: str,
    *,
    owner: str | None,
    url: str | None = None,
    credentials: dict | None = None,
    config: dict | None = None,
    scope_mode: str = "all",
    links: tuple[str, ...] = (),
) -> str:
    """A row as an orchestrator without the write-through stored it."""
    datasource_id = uuid4()
    await db.execute(
        """INSERT INTO datasources(id,name,type,connection_url,credentials,config,
           created_by,scope_mode) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8)""",
        datasource_id,
        name,
        ds_type,
        url,
        _encrypt_credentials_dict(credentials),
        json.dumps(config or {}),
        UUID(owner) if owner else None,
        scope_mode,
    )
    for project in links:
        await db.execute(
            "INSERT INTO project_datasources(project_id,datasource_id,read_only) "
            "VALUES($1,$2,$3)",
            UUID(project),
            datasource_id,
            True if ds_type == "kb" else None,
        )
    return str(datasource_id)


async def _resource(db, datasource_id: str) -> dict | None:
    return await ManifestStore(db).by_id(datasource_id)


async def _secret(db, datasource_id: str, scope: tuple[str, str] | None = None):
    """The Connector's secret row (in ``scope``, or wherever it is) with its
    decrypted values."""
    name = connector_secret_name(datasource_id)
    rows = await db.fetch("SELECT * FROM srw_resource_secrets WHERE name=$1", name)
    if scope is not None:
        rows = [row for row in rows if (row["scope_kind"], row["scope_name"]) == scope]
    if not rows:
        return None
    [row] = rows
    return {**dict(row), "values": json.loads(decrypt(row["ciphertext"]))}


async def _stored_text(db) -> str:
    return "".join(
        [
            await db.fetchval(
                "SELECT coalesce(string_agg(document::text, ''), '') "
                "FROM srw_resource_revisions"
            ),
            await db.fetchval(
                "SELECT coalesce(string_agg(document::text || resolved::text, ''), '') "
                "FROM srw_resources"
            ),
        ]
    )


async def _needing_work(db) -> set[str]:
    return {str(row["id"]) for row in await db.fetch(_NEEDS_WORK, UUID(int=0), 10_000)}


async def _persist(db, datasource_id: str) -> str:
    async with db.transaction_scope():
        return await persist_connector_resource(db, datasource_id)


def _scope_of(resource) -> tuple[str, str]:
    return resource["scope_kind"], resource["scope_name"]


async def _assert_in_step(db, datasource_id: str, keys: set[str]) -> dict:
    """The resource names exactly the secret's keys (all but the row digest),
    and the secret rebuilds the row it holds the digest of."""
    resource = await _resource(db, datasource_id)
    secret = await _secret(db, datasource_id, _scope_of(resource))
    refs = resource["document"]["spec"]["credentials"]
    assert set(refs) == keys
    assert {ref["secretRef"]["name"] for ref in refs.values()} <= {
        connector_secret_name(datasource_id)
    }
    row = await db.get_datasource(datasource_id)
    if not keys:
        assert secret is None
        return {}
    assert set(secret["values"]) == keys | {ROW_KEY} == set(secret["keys"])
    assert stored_credentials(secret["values"]) == (
        row["credentials"],
        row["connection_url"],
    )
    assert secret["values"][ROW_KEY] == row_digest(row)
    return secret


# =============================================================================
# The startup backfill
# =============================================================================


@pytest.mark.asyncio
async def test_the_backfill_gives_every_connector_its_secret(database):
    db = database
    alice = str((await _user(db, "Alice"))["id"])
    project = await _project(db, "Alpha", {alice: "owner"})
    rows = {
        "pg": (
            await _raw_row(
                db,
                "orders",
                "postgresql",
                owner=alice,
                url="postgresql://app:s3cret-pass@db.internal:5432/app",
            ),
            {URL_KEY},
        ),
        "neo4j": (
            await _raw_row(
                db,
                "graph",
                "neo4j",
                owner=alice,
                url="bolt://graph.internal:7687",
                credentials={"username": "neo4j", "password": "s3cret-neo"},
            ),
            {"username", "password", SHAPE_KEY, URL_KEY},
        ),
        "mcp_remote": (
            await _raw_row(
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
            {"header.X-Key", SHAPE_KEY, URL_KEY},
        ),
        "mcp_stdio": (
            await _raw_row(
                db,
                "local tool",
                "mcp",
                owner=alice,
                credentials={
                    "transport": "stdio",
                    "command": "npx",
                    "args": ["--key", "s3cret-arg"],
                    "env": {},
                },
            ),
            {"command", "arg.0", "arg.1", SHAPE_KEY},
        ),
        "env": (
            await _raw_row(
                db,
                "vendor",
                "credentials",
                owner=alice,
                credentials={"env_vars": {"VENDOR_TOKEN": "s3cret-env"}},
            ),
            {"env.VENDOR_TOKEN", SHAPE_KEY},
        ),
        "kubeconfig": (
            await _raw_row(
                db,
                "cluster",
                "kubeconfig",
                owner=alice,
                credentials={
                    "files": [
                        {
                            "name": "kubeconfig",
                            "contents": "s3cret-file",
                            "target_path": "~/.kube/config",
                            "mode": "0600",
                            "env_var": "KUBECONFIG",
                        }
                    ]
                },
            ),
            {"file.0", SHAPE_KEY},
        ),
        "native_kb": (
            await _raw_row(
                db,
                "Alpha Knowledge",
                "kb",
                owner=alice,
                url="https://git.internal/alpha/vault.git",
                config={"root_path": "knowledge", "native_project_id": project},
                scope_mode="projects",
                links=(project,),
            ),
            {URL_KEY},
        ),
        "empty": (
            await _raw_row(db, "nothing", "generic", owner=alice),
            set(),
        ),
    }
    counts = await migrate_stored_connectors(db)
    assert counts["created"] == len(rows) and counts["deferred"] == 0
    for label, (datasource_id, keys) in rows.items():
        secret = await _assert_in_step(db, datasource_id, keys)
        resource = await _resource(db, datasource_id)
        if keys:
            # In the Connector's own scope, owned like its resource.
            assert (secret["scope_kind"], secret["scope_name"]) == _scope_of(
                resource
            ), label
            assert secret["owner_id"] == UUID(alice), label
    assert _scope_of(await _resource(db, rows["native_kb"][0])) == ("Project", project)
    stored = await _stored_text(db)
    assert stored and not [secret for secret in SECRETS if secret in stored]
    assert await _needing_work(db) == set()

    # A rerun writes nothing.
    versions = {
        row["name"]: row["version"]
        for row in await db.fetch("SELECT name,version FROM srw_resource_secrets")
    }
    again = await migrate_stored_connectors(db)
    assert again["created"] == again["updated"] == 0
    assert {
        row["name"]: row["version"]
        for row in await db.fetch("SELECT name,version FROM srw_resource_secrets")
    } == versions


@pytest.mark.asyncio
async def test_the_backfill_repairs_what_an_older_orchestrator_left(database):
    db = database
    owner = str((await _user(db, "Owner"))["id"])
    first = await _raw_row(
        db,
        "graph",
        "neo4j",
        owner=owner,
        url="bolt://graph.internal:7687",
        credentials={"username": "neo4j", "password": "s3cret-neo"},
    )
    second = await _raw_row(
        db, "orders", "postgresql", owner=owner, url="postgresql://db/app"
    )
    await migrate_stored_connectors(db)

    # A D3a orchestrator rewrote the first resource without its references
    # and in step with the row; the second lost its secret.
    await db.execute(
        "UPDATE srw_resources SET document = document #- '{spec,credentials}' "
        "WHERE id=$1",
        UUID(first),
    )
    await db.execute(
        "DELETE FROM srw_resource_secrets WHERE name=$1", connector_secret_name(second)
    )
    assert await _needing_work(db) == {first, second}
    counts = await migrate_stored_connectors(db)
    assert counts["updated"] == 2 and counts["deferred"] == 0
    await _assert_in_step(db, first, {"username", "password", SHAPE_KEY, URL_KEY})
    await _assert_in_step(db, second, {URL_KEY})

    # A D3a orchestrator deleted the row and retired the resource, keeping
    # the secret; a secret named like a connector that is not one stays.
    unrelated = "connector-" + uuid4().hex
    await db.execute(
        """INSERT INTO srw_resource_secrets(scope_kind,scope_name,name,owner_id,
           ciphertext,keys) VALUES('Account',$1,$2,$3,'x',ARRAY['k'])""",
        owner,
        unrelated,
        UUID(owner),
    )
    await db.execute(
        "UPDATE srw_resources SET deleted_at=now() WHERE id=$1", UUID(second)
    )
    await db.execute("DELETE FROM datasources WHERE id=$1", UUID(second))
    await migrate_stored_connectors(db)
    assert await _secret(db, second) is None
    assert await db.fetchval(
        "SELECT count(*) FROM srw_resource_secrets WHERE name=$1", unrelated
    )


# =============================================================================
# The write-through
# =============================================================================


def _service_dependencies(db):
    return datasource_service.DatasourceDependencies(
        store=db,
        vector_db=MagicMock(),
        knowledge_index=MagicMock(),
        mcp_datasources_enabled=lambda: True,
        mcp_stdio_enabled=lambda: True,
        validate_mcp_datasource=lambda _url, _creds: None,
        connector_drivers=REGISTRY,
    )


async def _api_update(db, user, datasource_id: str, **body) -> None:
    await datasource_service.update_datasource(
        request=MagicMock(),
        datasource_id=datasource_id,
        body=DatasourceUpdate(**body),
        user=user,
        existing_ds=await db.get_datasource(datasource_id),
        require_project_owner=AsyncMock(),
        dependencies=_service_dependencies(db),
    )


@pytest.mark.asyncio
async def test_create_update_and_delete_keep_the_secret_in_step(database):
    db = database
    user = await _user(db, "Owner")
    owner = str(user["id"])
    created = await db.create_datasource(
        name="graph",
        ds_type="neo4j",
        connection_url="bolt://graph.internal:7687",
        credentials={"username": "neo4j", "password": "s3cret-neo"},
        created_by=owner,
    )
    graph = str(created["id"])
    first = await _assert_in_step(
        db, graph, {"username", "password", SHAPE_KEY, URL_KEY}
    )

    # A blank edit (no credentials, or an empty object) keeps the secret.
    await _api_update(db, user, graph, description="Supplier graph")
    await _api_update(db, user, graph, credentials={}, name="graph v2")
    kept = await _assert_in_step(
        db, graph, {"username", "password", SHAPE_KEY, URL_KEY}
    )
    assert kept["version"] == first["version"]
    assert kept["values"]["password"] == "s3cret-neo"

    # Any other edit replaces the whole object: the username goes.
    await _api_update(db, user, graph, credentials={"password": "s3cret-rotated"})
    replaced = await _assert_in_step(db, graph, {"password", SHAPE_KEY, URL_KEY})
    assert replaced["version"] == first["version"] + 1
    assert replaced["values"]["password"] == "s3cret-rotated"

    # A URL edit reaches the secret too.
    await _api_update(db, user, graph, connection_url="bolt://graph2.internal:7687")
    assert (await _secret(db, graph))["values"][URL_KEY] == (
        "bolt://graph2.internal:7687"
    )

    # A credentials connector merges the variables an edit names.
    vendor = str(
        (
            await db.create_datasource(
                name="vendor",
                ds_type="credentials",
                credentials={
                    "env_vars": {"VENDOR_USER": "alice", "VENDOR_TOKEN": "s3cret-env"}
                },
                created_by=owner,
            )
        )["id"]
    )
    await _api_update(
        db,
        user,
        vendor,
        credentials={"env_vars": {"VENDOR_TOKEN": "s3cret-rotated", "VENDOR_MFA": "1"}},
    )
    merged = await _assert_in_step(
        db,
        vendor,
        {"env.VENDOR_USER", "env.VENDOR_TOKEN", "env.VENDOR_MFA", SHAPE_KEY},
    )
    assert merged["values"]["env.VENDOR_USER"] == "alice"
    assert merged["values"]["env.VENDOR_TOKEN"] == "s3cret-rotated"

    # Delete: the secret goes with the resource, in the row's transaction.
    assert await db.delete_datasource(graph)
    assert await db.fetchval(
        "SELECT deleted_at IS NOT NULL FROM srw_resources WHERE id=$1", UUID(graph)
    )
    assert await _secret(db, graph) is None

    stored = await _stored_text(db)
    assert not [secret for secret in SECRETS if secret in stored]


@pytest.mark.asyncio
async def test_a_failed_write_leaves_no_secret(database, monkeypatch):
    db = database
    owner = str((await _user(db, "Owner"))["id"])
    from orchestrator.services import manifest_connectors

    async def broken(*_args, **_kwargs):
        raise RuntimeError("resource store down")

    monkeypatch.setattr(manifest_connectors, "write_connector_secret", broken)
    with pytest.raises(RuntimeError):
        await db.create_datasource(
            name="orders",
            ds_type="postgresql",
            connection_url="postgresql://app:s3cret-pass@db/app",
            created_by=owner,
        )
    assert not await db.fetchval("SELECT count(*) FROM datasources WHERE name='orders'")
    assert not await db.fetchval(
        "SELECT count(*) FROM srw_resources WHERE kind='Connector'"
    )


@pytest.mark.asyncio
async def test_a_connector_moving_into_its_project_takes_its_secret(database):
    db = database
    owner = str((await _user(db, "Owner"))["id"])
    project = await _project(db, "Alpha", {owner: "owner"})
    kb = await _raw_row(
        db,
        "Vault",
        "kb",
        owner=owner,
        url="https://git.internal/alpha/vault.git",
        credentials={"auth_method": "token", "token": "s3cret-token"},
        config={"root_path": "knowledge"},
    )
    assert await _persist(db, kb) == "created"
    assert await _secret(db, kb, ("Account", owner))
    # Adopted as the project's own knowledge base.
    await db.execute(
        "UPDATE datasources SET config = config || $2::jsonb WHERE id=$1",
        UUID(kb),
        json.dumps({"native_project_id": project}),
    )
    assert await _persist(db, kb) == "updated"
    assert await _secret(db, kb, ("Account", owner)) is None
    secret = await _assert_in_step(db, kb, {"token", SHAPE_KEY, URL_KEY})
    assert (secret["scope_kind"], secret["scope_name"]) == ("Project", project)


# =============================================================================
# Decision 11: the connector policy lends a Connector's own secret
# =============================================================================


async def _shared_connectors(db):
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    stranger = await _user(db, "Stranger")
    owner_id, member_id = str(owner["id"]), str(member["id"])
    project = await _project(db, "Alpha", {owner_id: "owner", member_id: "viewer"})

    async def connector(name, **kwargs):
        row = await db.create_datasource(
            name=name,
            ds_type="postgresql",
            connection_url=f"postgresql://{name}:s3cret-pass@db.internal/{name}",
            created_by=owner_id,
            **kwargs,
        )
        return str(row["id"])

    ids = {
        "public": await connector("public", is_global=True, read_only=True),
        "linked": await connector(
            "linked", scope_mode="projects", project_ids=[project]
        ),
        "private": await connector("private"),
    }
    return SimpleNamespace(
        owner=owner, member=member, stranger=stranger, project=project, ids=ids
    )


async def _connector_secret(db, user, datasource_id, project_ids=(), *, scope=None):
    resource = await _resource(db, datasource_id)
    ref = {
        "name": connector_secret_name(datasource_id),
        "key": URL_KEY,
        "scope": scope or dict(resource["document"]["metadata"]["scope"]),
    }
    return await ManifestAuthority(db, user).connector_secret(
        ref, resource, project_ids=list(project_ids)
    )


@pytest.mark.asyncio
async def test_decision_11_the_connector_policy_lends_its_secret(database):
    db = database
    world = await _shared_connectors(db)
    admin = await _user(db, "Administrator", admin=True)
    owner_scope = {"kind": "Account", "name": str(world.owner["id"])}
    allowed = [
        (world.owner, "private", ()),
        (world.owner, "linked", (world.project,)),
        (world.member, "public", ()),
        (world.member, "public", (world.project,)),
        (world.member, "linked", (world.project,)),
        # An administrator who is not the owner: the override a session's or
        # a job's own selection gets, so its delivery would carry it too.
        (admin, "private", ()),
    ]
    for user, label, projects in allowed:
        scope = await _connector_secret(db, user, world.ids[label], projects)
        assert scope == owner_scope, (user["display_name"], label)

    refused = [
        # Not linked to work outside the project, the owner's included: write
        # access to the secret's scope lends nothing the policy refuses.
        (world.member, "linked", ()),
        (world.owner, "linked", ()),
        # Never shared.
        (world.member, "private", (world.project,)),
        # Not a member of the linked project.
        (world.stranger, "linked", (world.project,)),
        (world.stranger, "private", ()),
    ]
    for user, label, projects in refused:
        with pytest.raises(HTTPException) as denied:
            await _connector_secret(db, user, world.ids[label], projects)
        assert denied.value.status_code == 403, (user["display_name"], label)

    # A member removed after the connector was linked loses it.
    await db.execute(
        "DELETE FROM project_members WHERE project_id=$1 AND user_id=$2",
        UUID(world.project),
        world.member["id"],
    )
    with pytest.raises(HTTPException) as removed:
        await _connector_secret(db, world.member, world.ids["linked"], (world.project,))
    assert removed.value.status_code == 403

    with pytest.raises(HTTPException) as catalog:
        await _connector_secret(
            db,
            world.owner,
            world.ids["public"],
            scope={"kind": "Catalog", "name": "shared"},
        )
    assert catalog.value.detail == CATALOG_SECRET_DETAIL


@pytest.mark.asyncio
async def test_a_project_member_resolves_the_projects_knowledge_base(database):
    """A viewer of the project, who may not write its secrets, names its own
    knowledge base Connector by ref. Since D3c a ref to a datasource's
    Connector resolves to its datasource binding and never reads the secret
    (no srw_resource_secrets read, nothing in the resolver's secrets): the
    connector policy decides where the work may use it, at admission and
    delivery, which then read the secret."""
    db = database
    owner = await _user(db, "Owner")
    viewer = await _user(db, "Viewer")
    editor = await _user(db, "Editor")
    project = await _project(
        db,
        "Alpha",
        {
            str(owner["id"]): "owner",
            str(viewer["id"]): "viewer",
            str(editor["id"]): "editor",
        },
    )
    kb = await _raw_row(
        db,
        "Alpha Knowledge",
        "kb",
        owner=str(owner["id"]),
        url="https://git.internal/alpha/vault.git",
        config={"root_path": "knowledge", "native_project_id": project},
        scope_mode="projects",
        links=(project,),
    )
    await migrate_stored_connectors(db)
    resource = await _resource(db, kb)
    project_scope = {"kind": "Project", "name": project}
    selection = {"ref": {"name": resource["name"], "scope": project_scope}}

    assert URL_KEY in resource["document"]["spec"]["credentials"]

    resolver = LiveManifestResolver(ManifestStore(db), ManifestAuthority(db, viewer))
    resolved = await resolver.selection("Connector", selection, project_scope, [])
    assert resolved == {
        "inline": {"driver": "srw.datasource/v1", "config": {"datasourceId": kb}}
    }
    assert resolver.secrets == {}
    selected, _ = await authorize_datasource_selection(
        db, viewer, str(viewer["id"]), [kb], [project], "sandbox"
    )
    assert selected == [kb]

    # Work in a member's own Account is not work in the project, also for an
    # editor, who could write the project's secrets: the policy refuses it
    # where the work is admitted.
    for member in (viewer, editor):
        personal = {"kind": "Account", "name": str(member["id"])}
        resolver = LiveManifestResolver(
            ManifestStore(db), ManifestAuthority(db, member)
        )
        await resolver.selection("Connector", selection, personal, [])
        assert resolver.secrets == {}
        with pytest.raises(DatasourceUnavailableError):
            await authorize_datasource_selection(
                db, member, str(member["id"]), [kb], [], "sandbox"
            )


@pytest.mark.asyncio
async def test_no_other_resource_may_reference_a_connector_secret(database):
    """Not even its owner's: an Expert's environment or a generic-hosting
    connector naming ``connector-<hex>`` would bypass the connector policy."""
    db = database
    world = await _shared_connectors(db)
    owner = world.owner
    owner_scope = {"kind": "Account", "name": str(owner["id"])}
    ref = {"name": connector_secret_name(world.ids["private"]), "key": URL_KEY}
    resolver = LiveManifestResolver(ManifestStore(db), ManifestAuthority(db, owner))
    with pytest.raises(HTTPException) as refused:
        await resolver.secret_values({"DB": {"secretRef": dict(ref)}}, owner_scope)
    assert refused.value.detail == FOREIGN_CONNECTOR_SECRET_DETAIL

    from orchestrator.services.manifest_execution import ManifestExecutionService

    service = ManifestExecutionService.__new__(ManifestExecutionService)
    service.db = db
    with pytest.raises(HTTPException) as refused:
        await service.secret(
            {**ref, "scope": owner_scope},
            ManifestAuthority(db, owner),
            materialize=True,
        )
    assert refused.value.detail == FOREIGN_CONNECTOR_SECRET_DETAIL


@pytest.mark.asyncio
async def test_an_edit_of_a_linked_connector_is_refused_as_before(database):
    """Its own document names its own secret: the apply reaches the store's
    refusal, as in D3a. A new document borrowing that secret does not."""
    db = database
    user = await _user(db, "Owner")
    created = await db.create_datasource(
        name="orders",
        ds_type="postgresql",
        connection_url="postgresql://app:s3cret-pass@db/app",
        created_by=str(user["id"]),
    )
    current = await ManifestStore(db).by_id(str(created["id"]))
    assert current["document"]["spec"]["credentials"]
    edited = json.loads(json.dumps(current["document"]))
    edited["metadata"]["labels"] = {"edited": "yes"}
    with pytest.raises(HTTPException) as applied:
        await ManifestResourceService(db).apply(
            json.dumps(edited),
            user,
            format="json",
            expected_versions={
                f"Connector/Account/{user['id']}/{current['name']}": current[
                    "resource_version"
                ]
            },
        )
    assert (applied.value.status_code, applied.value.detail) == (
        409,
        LINKED_CONNECTOR_MESSAGE,
    )

    borrowed = json.loads(json.dumps(current["document"]))
    borrowed["metadata"] = {"name": "borrowed", "scope": borrowed["metadata"]["scope"]}
    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(db).apply(
            json.dumps(borrowed), user, format="json"
        )
    assert refused.value.detail == FOREIGN_CONNECTOR_SECRET_DETAIL
    assert (
        await ManifestStore(db).by_name(
            "Connector", borrowed["metadata"]["scope"], "borrowed"
        )
        is None
    )


# =============================================================================
# Delivery reads the secret
# =============================================================================


def _thread_dependencies(db, project_ids):
    return ThreadDatasourceAuthorizationDependencies(
        store=db,
        thread_project_ids=AsyncMock(return_value=list(project_ids)),
        connector_credentials=functools.partial(
            read_connector_credentials, dependencies=SimpleNamespace(store=db)
        ),
    )


def _payload(rows):
    return build_datasources_payload(
        rows,
        dependencies=DatasourcePayloadDependencies(
            logger=MagicMock(),
            mcp_datasources_enabled=lambda: True,
            mcp_stdio_enabled=lambda: True,
            connector_drivers=REGISTRY,
            workspace_ssh_known_hosts=lambda: "",
        ),
    )


async def _mark_secrets(db, world, selected) -> None:
    """Mark each secret's URL, keeping its resource in step with its row, so a
    delivery that carries the mark read the secret, not the row."""
    for datasource_id in selected:
        resource = await _resource(db, datasource_id)
        secret = await _secret(db, datasource_id)
        values = dict(secret["values"])
        values[URL_KEY] = values[URL_KEY] + "?from=secret"
        async with db.transaction_scope():
            await write_connector_secret(
                db,
                datasource_id,
                resource["document"]["metadata"]["scope"],
                owner_id=world.owner["id"],
                values=values,
            )


def _job_dependencies(db):
    """The job delivery's collaborators over the real store and policy, as
    the application composes them."""
    dependencies = None

    async def revalidate(job):
        return await revalidate_job_datasource_selection(job, dependencies=dependencies)

    dependencies = JobDatasourceSelectionDependencies(
        store=db,
        authorize_thread_datasource_selection=functools.partial(
            authorize_thread_datasource_selection,
            dependencies=_thread_dependencies(db, []),
        ),
        backend_from_override=backend_from_override,
        revalidate_selection=revalidate,
        connector_credentials=functools.partial(
            read_connector_credentials, dependencies=SimpleNamespace(store=db)
        ),
    )
    return dependencies


@pytest.mark.asyncio
async def test_a_non_owners_job_gets_the_shared_connectors_secrets(database):
    db = database
    world = await _shared_connectors(db)
    member_id = str(world.member["id"])
    selected, revisions = await authorize_datasource_selection(
        db,
        world.member,
        member_id,
        [world.ids["public"], world.ids["linked"]],
        [world.project],
        "sandbox",
    )
    created = await db.create_job(
        "D3b delivery",
        user_id=member_id,
        project_id=world.project,
        datasource_ids=selected,
        datasource_policy_revisions=revisions,
        datasource_selection_provenance={"origin": "user"},
        authority_user_id=member_id,
        authority_project_ids=[world.project],
    )
    await _mark_secrets(db, world, selected)
    job = await db.get_job(str(created["id"]))

    rows = await resolve_authorized_job_datasources(
        job, dependencies=_job_dependencies(db)
    )
    assert sorted(str(row["id"]) for row in rows) == sorted(selected)
    assert {row["connection_url"].endswith("?from=secret") for row in rows} == {True}
    payload = _payload(rows)
    assert len(payload) == 2
    assert all(entry["connection_url"].endswith("?from=secret") for entry in payload)

    # The member leaves the project: the job's next delivery is refused.
    await db.execute(
        "DELETE FROM project_members WHERE project_id=$1 AND user_id=$2",
        UUID(world.project),
        world.member["id"],
    )
    with pytest.raises(HTTPException) as refused:
        await resolve_authorized_job_datasources(
            job, dependencies=_job_dependencies(db)
        )
    assert refused.value.status_code == 403


@pytest.mark.asyncio
async def test_a_non_owners_session_gets_the_shared_connectors_secrets(database):
    db = database
    world = await _shared_connectors(db)
    selected = [world.ids["public"], world.ids["linked"]]
    await _mark_secrets(db, world, selected)

    thread = {"id": str(uuid4()), "user_id": str(world.member["id"]), "metadata": {}}
    rows = await resolve_authorized_thread_datasources(
        thread,
        selected,
        target_project_ids=[world.project],
        dependencies=_thread_dependencies(db, [world.project]),
    )
    assert {row["connection_url"].endswith("?from=secret") for row in rows} == {True}
    payload = _payload(rows)
    assert len(payload) == 2
    assert all(entry["connection_url"].endswith("?from=secret") for entry in payload)

    # A row an older orchestrator changed is newer than its resource: the
    # row is delivered until the next write or start reconciles them.
    await db.execute(
        "UPDATE datasources SET description='edited elsewhere' WHERE id=$1",
        UUID(world.ids["public"]),
    )
    rows = await resolve_authorized_thread_datasources(
        thread,
        [world.ids["public"]],
        target_project_ids=[world.project],
        dependencies=_thread_dependencies(db, [world.project]),
    )
    assert not rows[0]["connection_url"].endswith("?from=secret")


@pytest.mark.asyncio
async def test_a_same_named_secret_in_another_scope_is_never_read(database):
    """The reader joins the secret in its Connector's own scope: a
    ``connector-<hex>`` secret elsewhere (another project's, say), even one
    that would pass every other check, falls back to the row."""
    db = database
    world = await _shared_connectors(db)
    public = world.ids["public"]
    secret = await _secret(db, public)
    values = dict(secret["values"])
    values[URL_KEY] = values[URL_KEY] + "?from=secret"
    async with db.transaction_scope():
        await write_connector_secret(
            db,
            public,
            {"kind": "Project", "name": world.project},
            owner_id=None,
            values=values,
        )
    await db.execute(
        "DELETE FROM srw_resource_secrets WHERE scope_kind='Account' AND name=$1",
        connector_secret_name(public),
    )
    thread = {"id": str(uuid4()), "user_id": str(world.member["id"]), "metadata": {}}
    rows = await resolve_authorized_thread_datasources(
        thread,
        [public],
        target_project_ids=[world.project],
        dependencies=_thread_dependencies(db, [world.project]),
    )
    row = await db.get_datasource(public)
    assert rows[0]["connection_url"] == row["connection_url"]
    assert not rows[0]["connection_url"].endswith("?from=secret")


@pytest.mark.asyncio
async def test_an_unauthorized_attach_is_refused(database):
    db = database
    world = await _shared_connectors(db)
    thread = {"id": str(uuid4()), "user_id": str(world.stranger["id"]), "metadata": {}}
    for selected in ([world.ids["linked"]], [world.ids["private"]]):
        with pytest.raises(HTTPException) as refused:
            await resolve_authorized_thread_datasources(
                thread,
                selected,
                target_project_ids=[world.project],
                dependencies=_thread_dependencies(db, [world.project]),
            )
        assert refused.value.status_code == 403


# =============================================================================
# The resource API
# =============================================================================


@pytest.mark.asyncio
async def test_the_resource_api_cannot_write_a_connector_secret(database):
    db = database
    user = await _user(db, "Owner")
    created = await db.create_datasource(
        name="orders",
        ds_type="postgresql",
        connection_url="postgresql://app:s3cret-pass@db/app",
        created_by=str(user["id"]),
    )
    before = await _secret(db, str(created["id"]))
    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(db).put_secret(
            user,
            scope={"kind": "Account", "name": "me"},
            name=connector_secret_name(created["id"]),
            values={URL_KEY: "postgresql://evil@db/app"},
            expected_version=before["version"],
        )
    assert refused.value.status_code == 409
    assert refused.value.detail == CONNECTOR_SECRET_DETAIL
    after = await _secret(db, str(created["id"]))
    assert after["values"] == before["values"] and after["version"] == before["version"]
