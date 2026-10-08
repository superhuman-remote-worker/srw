"""Datasources become Connectors, slice D3c, on a real PostgreSQL.

* a Project manifest lists its links as Connector refs and follows every
  write that links or unlinks (link, unlink, a policy edit of a connector's
  projects, a connector created with links, a delete); a natively authored
  Project keeps its own entries and only gains or loses the changed links;
* a stored Project that still holds inline ``datasource-<hex>`` children is
  rebuilt by the startup heal and its children are retired;
* applying a Project manifest links what it names and unlinks what its
  previous revision named and it drops, under the link API's authority, and
  the project's own knowledge base always stays linked and listed;
* the Project connector defaults (0380): Settings rows hold linked
  connectors only, a manifest that sets ``defaults.connectors`` owns its row,
  and creation-time defaults add them to the owner's own and the knowledge
  base;
* ``execution.connectors`` refs resolve to the ids ``datasource_ids`` name,
  through the unchanged funnels: the same bindings for an owner, a public and
  a project-linked connector, and one refusal for a ref that names nothing
  and a ref the caller may not use.
"""

from __future__ import annotations

from functools import partial
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import project_connectors
from orchestrator.services.connector_refs import (
    resolve_connector_refs,
    resolve_execution_connectors,
)
from orchestrator.services.datasource_policy import (
    GENERIC_UNAVAILABLE_DETAIL,
    default_datasource_selection,
)
from orchestrator.services.job_admission_datasources import (
    JobAdmissionDatasourcesDependencies,
    prepare_job_admission_datasources,
)
from orchestrator.services.job_datasource_selection import (
    datasource_selection_provenance,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.manifest_projects import persist_project_resource
from orchestrator.services.manifest_resolution import (
    REFERENCE_MISSING,
    LiveManifestResolver,
)
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.project_connector_defaults import (
    MANAGED_BY_MANIFEST,
    read_project_connector_defaults,
    read_view,
    save_settings_connector_defaults,
)
from orchestrator.services.thread_admission import select_thread_datasources
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    authorize_thread_datasource_selection,
)
from shared.manifests import preview_documents
from shared.manifests.resolution import content_revision
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url


# =============================================================================
# Helpers
# =============================================================================


async def _user(db, name: str, *, admin: bool = False) -> dict:
    row = await db.fetchrow(
        "INSERT INTO users(display_name,is_approved,is_admin) "
        "VALUES($1,TRUE,$2) RETURNING id",
        name,
        admin,
    )
    return {"id": str(row["id"]), "is_approved": True, "is_admin": admin}


async def _project(db, name: str, owner: dict, **members: str) -> str:
    project_id = str(
        await db.fetchval("INSERT INTO projects(name) VALUES($1) RETURNING id", name)
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
        UUID(project_id),
        UUID(owner["id"]),
    )
    for user_id, role in members.items():
        await db.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,$3)",
            UUID(project_id),
            UUID(user_id),
            role,
        )
    async with db.transaction_scope():
        await persist_project_resource(db, project_id)
    return project_id


async def _connector(db, name: str, owner: dict | None, **fields) -> str:
    row = await db.create_datasource(
        name=name,
        ds_type=fields.pop("ds_type", "postgresql"),
        connection_url=fields.pop("connection_url", "postgresql://db.example:5432/app"),
        created_by=owner["id"] if owner else None,
        **fields,
    )
    return str(row["id"])


async def _knowledge_base(db, project_id: str, owner: dict) -> str:
    return await _connector(
        db,
        "Team knowledge",
        owner,
        ds_type="kb",
        connection_url=None,
        config={"root_path": "knowledge", "native_project_id": project_id},
        read_only=True,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project_id],
    )


async def _project_resource(db, project_id: str) -> dict:
    return await ManifestStore(db).by_link("Project", project_id)


async def _entries(db, project_id: str) -> dict:
    resource = await _project_resource(db, project_id)
    return resource["document"]["spec"]["resources"].get("connectors", {})


async def _ref_of(db, datasource_id: str) -> dict:
    """The Connector resource's ref: its name and scope."""
    resource = await ManifestStore(db).by_id(datasource_id)
    return {
        "name": resource["name"],
        "scope": {"kind": resource["scope_kind"], "name": resource["scope_name"]},
    }


async def _listed(db, project_id: str) -> set[str]:
    """The connector ids a Project's manifest lists."""
    named = await project_connectors.entry_datasource_ids(
        db, await _entries(db, project_id), project_id=project_id, account_id=None
    )
    return {value for value in named.values() if value}


async def _links(db, project_id: str) -> set[str]:
    return {
        str(row["datasource_id"])
        for row in await db.fetch(
            "SELECT datasource_id FROM project_datasources WHERE project_id=$1",
            UUID(project_id),
        )
    }


async def _live_children(db, project_id: str) -> list[str]:
    resource = await _project_resource(db, project_id)
    return [
        row["name"]
        for row in await db.fetch(
            "SELECT name FROM srw_resources WHERE managed_by=$1 AND kind='Connector' "
            "AND deleted_at IS NULL",
            resource["id"],
        )
    ]


def _project_document(name: str, owner: dict, connectors: dict, **spec) -> dict:
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {
            "name": name,
            "scope": {"kind": "Account", "name": owner["id"]},
        },
        "spec": {"resources": {"connectors": connectors}, **spec},
    }


async def _apply(db, document: dict, actor: dict, *, expected=None) -> dict:
    return await ManifestResourceService(db).apply(
        json.dumps([document]), actor, format="json", expected_versions=expected
    )


# =============================================================================
# Project manifests list their links
# =============================================================================


@pytest.mark.asyncio
async def test_project_manifest_lists_links_as_refs_through_every_link_write(
    database,
):
    db = database
    owner = await _user(db, "Owner")
    project = await _project(db, "Team", owner)
    assert await _entries(db, project) == {}

    first = await _connector(db, "Prod DB", owner)
    assert await _entries(db, project) == {}

    assert await db.link_datasource_to_project(project, first)
    ref = await _ref_of(db, first)
    assert ref["scope"] == {"kind": "Account", "name": owner["id"]}
    assert await _entries(db, project) == {ref["name"]: {"ref": ref}}

    second = await _connector(db, "Reports", owner, project_ids=[project])
    assert await _listed(db, project) == {first, second} == await _links(db, project)

    assert await db.unlink_datasource_from_project(project, first)
    assert await _listed(db, project) == {second}

    row = await db.get_datasource(second)
    await db.update_datasource_with_policy(
        second, expected_policy_revision=row["policy_revision"], project_ids=[]
    )
    assert await _listed(db, project) == set() == await _links(db, project)

    assert await db.link_datasource_to_project(project, second)
    assert await _listed(db, project) == {second}
    assert await db.delete_datasource(second)
    assert await _entries(db, project) == {}
    # No srw.datasource/v1 child is ever made for a connector with a resource.
    assert await _live_children(db, project) == []


@pytest.mark.asyncio
async def test_a_failed_refresh_never_fails_the_link_write(database, monkeypatch):
    db = database
    owner = await _user(db, "Owner")
    project = await _project(db, "Team", owner)
    connector = await _connector(db, "Prod DB", owner)

    async def broken(*_args, **_kwargs):
        raise RuntimeError("an unrelated Project problem")

    monkeypatch.setattr(project_connectors, "refresh_project", broken)
    assert await db.link_datasource_to_project(project, connector)
    assert await _links(db, project) == {connector}
    assert await _entries(db, project) == {}

    monkeypatch.undo()
    assert (await project_connectors.heal_project_connectors(db)) == {"rebuilt": 1}
    assert await _listed(db, project) == {connector}


@pytest.mark.asyncio
async def test_startup_heal_rebuilds_inline_children_with_refs_and_retires_them(
    database, monkeypatch
):
    db = database
    owner = await _user(db, "Owner")
    project = await _project(db, "Team", owner)
    connector = await _connector(db, "Prod DB", owner)

    # A Project manifest as an orchestrator before D3c wrote it: an inline
    # srw.datasource/v1 child per link, and no refresh on link.
    async def before_d3c(db, project_id):
        return [
            {**dict(row), "resource_name": None}
            for row in await db.fetch(
                "SELECT d.id, d.policy_revision FROM project_datasources pd "
                "JOIN datasources d ON d.id = pd.datasource_id "
                "WHERE pd.project_id = $1",
                UUID(project_id),
            )
        ]

    with project_connectors.project_refresh_suspended():
        assert await db.link_datasource_to_project(project, connector)
    monkeypatch.setattr(project_connectors, "project_link_rows", before_d3c)
    async with db.transaction_scope():
        await persist_project_resource(db, project)
    alias = project_connectors.legacy_alias(connector)
    old = await _entries(db, project)
    assert old[alias]["inline"]["driver"] == "srw.datasource/v1"
    assert await _live_children(db, project) == [alias]
    # The inline entry is still read as the link it names.
    assert await _listed(db, project) == {connector}

    monkeypatch.undo()
    counts = await project_connectors.heal_project_connectors(db)
    assert counts.get("rebuilt") == 1
    ref = await _ref_of(db, connector)
    assert await _entries(db, project) == {ref["name"]: {"ref": ref}}
    assert await _live_children(db, project) == []
    # In step now: a second start only compares.
    assert (await project_connectors.heal_project_connectors(db)).get("rebuilt") is None


@pytest.mark.asyncio
async def test_native_project_apply_links_and_unlinks_what_it_names(database):
    db = database
    owner = await _user(db, "Owner")
    stranger = await _user(db, "Stranger")
    first = await _connector(db, "Prod DB", owner)
    second = await _connector(db, "Reports", owner)
    first_ref, second_ref = await _ref_of(db, first), await _ref_of(db, second)

    document = _project_document(
        "native-team", owner, {"db": {"ref": first_ref}, "reports": {"ref": second_ref}}
    )
    result = await _apply(db, document, owner)
    project = str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )
    assert await _links(db, project) == {first, second}
    # The authored aliases stay as written.
    assert set(await _entries(db, project)) == {"db", "reports"}

    # The knowledge base SRW provisions later joins the list and stays.
    kb = await _knowledge_base(db, project, owner)
    entries = await _entries(db, project)
    assert set(entries) == {"db", "reports", (await _ref_of(db, kb))["name"]}

    resource = await _project_resource(db, project)
    dropped = _project_document("native-team", owner, {"db": {"ref": first_ref}})
    result = await _apply(
        db,
        dropped,
        owner,
        expected={
            f"Project/Account/{owner['id']}/native-team": resource["resource_version"]
        },
    )
    # The dropped link is gone; the knowledge base the document left out is
    # never unlinked, and is listed again.
    assert await _links(db, project) == {first, kb}
    assert await _listed(db, project) == {first, kb}
    assert (
        result["resources"][0]["resourceVersion"]
        == (await _project_resource(db, project))["resource_version"]
    )

    # Someone else's private connector cannot be linked by an apply.
    other = await _project(db, "Other", stranger)
    resource = await _project_resource(db, other)
    taken = _project_document(f"project-{other}", stranger, {"db": {"ref": first_ref}})
    taken["metadata"] = resource["document"]["metadata"]
    expected = {
        f"Project/Account/{stranger['id']}/project-{other}": resource[
            "resource_version"
        ]
    }
    # Unseen, it answers as a connector that does not exist...
    with pytest.raises(HTTPException) as refused:
        await _apply(db, taken, stranger, expected=expected)
    assert (refused.value.status_code, refused.value.detail) == (
        422,
        REFERENCE_MISSING,
    )
    # ...and seen through a project the stranger joined, it still needs the
    # link API's authority: the connector's owner (or a public connector).
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'viewer')",
        UUID(project),
        UUID(stranger["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await _apply(db, taken, stranger, expected=expected)
    assert refused.value.status_code == 403
    assert first not in await _links(db, other)


@pytest.mark.asyncio
async def test_native_project_refresh_keeps_authored_entries(database):
    db = database
    owner = await _user(db, "Owner")
    first = await _connector(db, "Prod DB", owner)
    second = await _connector(db, "Reports", owner)
    document = _project_document(
        "native-team",
        owner,
        {"db": {"ref": await _ref_of(db, first)}},
        defaults={"connectors": ["db"]},
    )
    result = await _apply(db, document, owner)
    project = str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )
    stored = await read_project_connector_defaults(db, project)
    assert stored.source == "manifest" and stored.connector_ids == [first]

    assert await db.link_datasource_to_project(project, second)
    entries = await _entries(db, project)
    assert entries["db"] == document["spec"]["resources"]["connectors"]["db"]
    assert set(entries) == {"db", (await _ref_of(db, second))["name"]}

    assert await db.unlink_datasource_from_project(project, first)
    resource = await _project_resource(db, project)
    assert "db" not in resource["document"]["spec"]["resources"]["connectors"]
    assert "db" not in resource["resolved"]["spec"]["resources"]["connectors"]
    assert resource["document"]["spec"]["defaults"]["connectors"] == []
    assert (await read_project_connector_defaults(db, project)).connector_ids == []


@pytest.mark.asyncio
async def test_a_member_reads_and_reapplies_refs_to_connectors_shared_with_them(
    database,
):
    """A linked Connector is visible the way its datasource is (public, or
    linked to a project the caller belongs to), not by its Account scope: a
    Project editor re-applies the manifest that names the owner's connector,
    and someone outside the project neither reads nor names it."""
    db = database
    owner = await _user(db, "Owner")
    editor = await _user(db, "Editor")
    stranger = await _user(db, "Stranger")
    publisher = await _user(db, "Publisher")
    project = await _project(db, "Team", owner, **{editor["id"]: "editor"})
    linked = await _connector(db, "Prod DB", owner, project_ids=[project])
    public = await _connector(
        db, "Public", publisher, is_global=True, read_only=True, scope_mode="all"
    )
    for user, datasource_id, visible in (
        (editor, linked, True),
        (stranger, linked, False),
        (stranger, public, True),
    ):
        row = await ManifestStore(db).by_id(datasource_id)
        authority = ManifestAuthority(db, user)
        if visible:
            await authority.resource(row)
        else:
            with pytest.raises(HTTPException) as denied:
                await authority.resource(row)
            assert denied.value.status_code == 403

    resource = await _project_resource(db, project)
    result = await _apply(
        db,
        resource["document"],
        editor,
        expected={
            f"Project/Account/{owner['id']}/project-{project}": resource[
                "resource_version"
            ]
        },
    )
    assert (
        result["resources"][0]["resource"]["spec"]["resources"]["connectors"]
        == (resource["document"]["spec"]["resources"]["connectors"])
    )
    assert await _links(db, project) == {linked}


async def _catalog_connector(db, name: str) -> dict:
    """A Connector in the shared Catalog: no datasource row behind it."""
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Connector",
        "metadata": {"name": name, "scope": {"kind": "Catalog", "name": "shared"}},
        "spec": {"driver": "srw.env/v1", "config": {"names": ["REGION"]}},
    }
    resolved = preview_documents([document])["resolved"][0]
    async with db.transaction_scope():
        store = ManifestStore(db)
        await store.lock_catalog()
        await store.lock_identity(document)
        row, _ = await store.save(
            document, resolved, content_revision(resolved["spec"]), [], owner_id=None
        )
    return row


@pytest.mark.asyncio
async def test_a_connector_ref_is_authorized_by_the_connector_policy_and_leaks_nothing(
    database,
):
    """``ManifestAuthority.resource`` and manifest ref resolution follow the
    connector policy for a datasource's Connector: its owner, everyone when
    public, members of a project it is linked to; a Catalog Connector is never
    one. A ref the caller may not use answers exactly as a ref to nothing."""
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    stranger = await _user(db, "Stranger")
    publisher = await _user(db, "Publisher")
    project = await _project(db, "Team", owner, **{member["id"]: "editor"})
    kb = await _knowledge_base(db, project, owner)
    private = await _connector(db, "Private", owner)
    linked = await _connector(
        db, "Linked", owner, scope_mode="projects", project_ids=[project]
    )
    public = await _connector(
        db, "Public", publisher, is_global=True, read_only=True, scope_mode="all"
    )
    catalog = await _catalog_connector(db, "shared-env")

    visible = {
        "owner": {private, linked, public, kb},
        "member": {linked, public, kb},
        "stranger": {public},
    }
    users = {"owner": owner, "member": member, "stranger": stranger}
    for label, user in users.items():
        for datasource_id in (private, linked, public, kb):
            row = await ManifestStore(db).by_id(datasource_id)
            if datasource_id in visible[label]:
                await ManifestAuthority(db, user).resource(row)
            else:
                with pytest.raises(HTTPException) as denied:
                    await ManifestAuthority(db, user).resource(row)
                assert denied.value.status_code == 403, (label, datasource_id)
        # A datasource's Connector is never a Catalog resource.
        claimed = {
            **await ManifestStore(db).by_id(public),
            "scope_kind": "Catalog",
            "scope_name": "shared",
        }
        with pytest.raises(HTTPException):
            await ManifestAuthority(db, user).resource(claimed)

    async def resolve(user, ref):
        resolver = LiveManifestResolver(ManifestStore(db), ManifestAuthority(db, user))
        return await resolver.selection(
            "Connector", {"ref": ref}, {"kind": "Project", "name": project}, []
        )

    refs = {
        private: await _ref_of(db, private),
        linked: await _ref_of(db, linked),
        public: await _ref_of(db, public),
        kb: await _ref_of(db, kb),
    }
    missing = [
        {**refs[private], "name": "absent-0123456789ab"},
        {"name": refs[kb]["name"], "scope": {"kind": "Project", "name": str(uuid4())}},
    ]
    for label, user in users.items():
        for datasource_id, ref in refs.items():
            if datasource_id in visible[label]:
                assert await resolve(user, ref) == (
                    project_connectors.datasource_binding(datasource_id)
                )
                continue
            with pytest.raises(HTTPException) as hidden:
                await resolve(user, ref)
            assert (hidden.value.status_code, hidden.value.detail) == (
                422,
                REFERENCE_MISSING,
            )
        for ref in missing:
            with pytest.raises(HTTPException) as absent:
                await resolve(user, ref)
            assert (absent.value.status_code, absent.value.detail) == (
                422,
                REFERENCE_MISSING,
            )
        # A Catalog Connector stays a plain Catalog definition: readable, never
        # a datasource binding...
        resolved = await resolve(user, {"name": "shared-env", "scope": catalog_scope()})
        assert resolved["inline"]["driver"] == "srw.env/v1"
        # ...and never a job's or session's connector.
        with pytest.raises(HTTPException) as refused:
            await resolve_connector_refs(
                db,
                {"env": {"name": "shared-env", "scope": catalog_scope()}},
                owner_id=user["id"],
                project_id=project,
            )
        assert (refused.value.status_code, refused.value.detail) == (
            403,
            GENERIC_UNAVAILABLE_DETAIL,
        )
    assert catalog["linked_id"] is None


def catalog_scope() -> dict:
    return {"kind": "Catalog", "name": "shared"}


@pytest.mark.asyncio
async def test_a_manifest_job_takes_the_project_default_connector_by_ref(database):
    """A linked Connector ref binds as its datasource: an SRW manifest Job in
    the Project inherits ``defaults.connectors`` and is admitted with it,
    under the connector policy, for a member who does not own it."""
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    connector = await _connector(db, "Prod DB", owner)
    document = _project_document(
        "native-team",
        owner,
        {"db": {"ref": await _ref_of(db, connector)}},
        defaults={"connectors": ["db"]},
    )
    result = await _apply(db, document, owner)
    project = str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )
    resource = await _project_resource(db, project)
    assert resource["resolved"]["spec"]["resources"]["connectors"]["db"] == (
        project_connectors.datasource_binding(connector)
    )
    # Not frozen as a dependency: deleting it never waits for the Project.
    assert connector not in json.dumps(resource["dependencies"])
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(project),
        UUID(member["id"]),
    )

    authorize = partial(
        authorize_thread_datasource_selection,
        dependencies=ThreadDatasourceAuthorizationDependencies(
            store=db, thread_project_ids=AsyncMock(return_value=[])
        ),
    )
    execution = ManifestExecutionService(
        db,
        runtime=full_schema.ProcessRuntime(),
        namespace="test",
        srw_image=db.manifest_runtime_image,
        authorize_datasources=authorize,
        connector_drivers=builtin_connector_drivers(),
    )
    job = full_schema.assignment(adapter="srw/v1", mode="Reported")
    job["metadata"]["scope"] = {"kind": "Project", "name": project}
    admitted = await ManifestResourceService(db, admit_job=execution.admit).apply(
        json.dumps([job]), member, format="json"
    )
    work_id = next(iter(admitted["executions"].values()))
    assert [
        str(row["datasource_id"])
        for row in await db.fetch(
            "SELECT datasource_id FROM job_datasources WHERE job_id=$1", UUID(work_id)
        )
    ] == [connector]


# =============================================================================
# Project connector defaults
# =============================================================================


@pytest.mark.asyncio
async def test_project_connector_defaults_settings_and_selection(database):
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    project = await _project(db, "Team", owner, **{member["id"]: "editor"})
    kb = await _knowledge_base(db, project, owner)
    shared = await _connector(db, "Shared DB", owner, project_ids=[project])
    unlinked = await _connector(db, "Elsewhere", owner)

    async def defaults_for(user):
        selected, _ = await default_datasource_selection(
            db, user["id"], [project], "sandbox"
        )
        return selected

    # Today's defaults, unchanged: the knowledge base only for a member; the
    # owner's connector does not auto-attach.
    assert await defaults_for(member) == [kb]

    for refused in ([unlinked], [kb]):
        with pytest.raises(HTTPException) as error:
            await save_settings_connector_defaults(
                db, project, refused, actor_id=owner["id"]
            )
        assert error.value.status_code == 422
    await save_settings_connector_defaults(db, project, [shared], actor_id=owner["id"])
    assert set(await defaults_for(member)) == {kb, shared}
    assert set(await defaults_for(owner)) == {kb, shared}
    project_row = await db.get_project(project)
    view = await read_view(db, project_row, member)
    assert [item["id"] for item in view["effective"]] == [kb, shared]
    assert view["effective"][0]["platform_owned"] is True
    assert view["effective"][1]["ref"] == await _ref_of(db, shared)
    assert view["can_edit"] is False
    assert (await read_view(db, project_row, owner))["can_edit"] is True

    # A projectless session never takes a project's defaults.
    selected, _ = await default_datasource_selection(db, member["id"], [], "sandbox")
    assert shared not in selected

    # Unlinked, it is no default any more, and the stored id is pruned.
    assert await db.unlink_datasource_from_project(project, shared)
    assert await defaults_for(member) == [kb]
    assert (await read_project_connector_defaults(db, project)).connector_ids == []


@pytest.mark.asyncio
async def test_manifest_owns_its_connector_defaults_until_it_goes(database):
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    connector = await _connector(db, "Prod DB", owner)
    document = _project_document(
        "native-team",
        owner,
        {"db": {"ref": await _ref_of(db, connector)}},
        defaults={"connectors": ["db"]},
    )
    result = await _apply(db, document, owner)
    project = str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(project),
        UUID(member["id"]),
    )
    selected, _ = await default_datasource_selection(
        db, member["id"], [project], "sandbox"
    )
    assert selected == [connector]
    with pytest.raises(HTTPException, match=MANAGED_BY_MANIFEST):
        await save_settings_connector_defaults(db, project, [], actor_id=owner["id"])

    resource = await _project_resource(db, project)
    await ManifestStore(db).delete(
        resource, expected_version=resource["resource_version"]
    )
    stored = await read_project_connector_defaults(db, project)
    assert stored.source == "settings" and stored.connector_ids == []


# =============================================================================
# execution.connectors: the same bindings as datasource_ids
# =============================================================================


def _job_dependencies(db) -> JobAdmissionDatasourcesDependencies:
    authorize = partial(
        authorize_thread_datasource_selection,
        dependencies=ThreadDatasourceAuthorizationDependencies(
            store=db, thread_project_ids=AsyncMock(return_value=[])
        ),
    )
    return JobAdmissionDatasourcesDependencies(
        backend_from_override=Mock(return_value="sandbox"),
        inherit_parent_ids=AsyncMock(return_value=[]),
        filter_implicit_lite_ids=AsyncMock(return_value=[]),
        authorize_selection=authorize,
        default_selection=partial(default_datasource_selection, db),
        defaults_on_omission=Mock(return_value=False),
        selection_provenance=datasource_selection_provenance,
        resolve_connector_refs=partial(resolve_execution_connectors, db),
    )


async def _job_selection(db, user, project, **fields):
    result = await prepare_job_admission_datasources(
        command=JobCreate(description="Selection", **fields),
        config_override={"workspace": {"backend": "sandbox"}},
        selection_actor=user,
        effective_user_id=user["id"],
        project_id=project,
        internal_call=False,
        internal_origin_bound=False,
        dependencies=_job_dependencies(db),
    )
    return result.datasource_ids, result.policy_revisions


async def _session_selection(db, user, project, **fields):
    authorize = partial(
        authorize_thread_datasource_selection,
        dependencies=ThreadDatasourceAuthorizationDependencies(
            store=db, thread_project_ids=AsyncMock(return_value=[])
        ),
    )
    ids, revisions, _ = await select_thread_datasources(
        ThreadCreateRequest(project_id=project, **fields),
        user,
        thread_backend="sandbox",
        effective_project_ids=[project],
        dependencies=SimpleNamespace(
            store=db,
            authorize_thread_datasource_selection=authorize,
            datasource_defaults_on_omission=lambda: False,
        ),
    )
    return ids, revisions


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [_job_selection, _session_selection])
async def test_refs_and_ids_bind_the_same_connectors(database, selection):
    db = database
    owner = await _user(db, "Owner")
    publisher = await _user(db, "Publisher")
    member = await _user(db, "Member")
    project = await _project(db, "Team", owner, **{member["id"]: "editor"})
    kb = await _knowledge_base(db, project, owner)
    own = await _connector(db, "Mine", member, scope_mode="all")
    public = await _connector(
        db, "Public", publisher, is_global=True, read_only=True, scope_mode="all"
    )
    linked = await _connector(
        db, "Linked", owner, scope_mode="projects", project_ids=[project]
    )
    ids = [own, public, linked, kb]
    kb_name = (await _ref_of(db, kb))["name"]

    by_ids = await selection(db, member, project, datasource_ids=ids)
    by_refs = await selection(
        db,
        member,
        project,
        execution={
            "connectors": {
                "mine": {
                    "ref": {
                        "name": (await _ref_of(db, own))["name"],
                        "scope": {"kind": "Account", "name": "me"},
                    }
                },
                "public": {"ref": {"uid": public}},
                "linked": {"ref": await _ref_of(db, linked)},
                # An omitted scope is the execution's: its Project.
                "knowledge": {"ref": {"name": kb_name}},
            }
        },
    )
    assert by_refs == by_ids
    assert by_ids[0] == ids
    # An empty map is an explicit empty selection, like datasource_ids: [].
    assert await selection(db, member, project, execution={"connectors": {}}) == (
        [],
        {},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [_job_selection, _session_selection])
async def test_an_unusable_ref_is_refused_like_an_unusable_id(database, selection):
    db = database
    owner = await _user(db, "Owner")
    stranger = await _user(db, "Stranger")
    project = await _project(db, "Team", owner)
    other = await _project(db, "Elsewhere", stranger)
    private = await _connector(db, "Private", owner, scope_mode="all")
    private_ref = await _ref_of(db, private)

    refusals = []
    for fields in (
        {"datasource_ids": [private]},
        {"execution": {"connectors": {"db": {"ref": private_ref}}}},
        {"execution": {"connectors": {"db": {"ref": {"uid": private}}}}},
        {"execution": {"connectors": {"db": {"ref": {"uid": str(uuid4())}}}}},
        {
            "execution": {
                "connectors": {
                    "db": {"ref": {**private_ref, "name": "absent-0123456789ab"}}
                }
            }
        },
        # Another kind's resource, by uid.
        {
            "execution": {
                "connectors": {
                    "db": {
                        "ref": {
                            "uid": str((await _project_resource(db, project))["id"])
                        }
                    }
                }
            }
        },
    ):
        with pytest.raises(HTTPException) as refused:
            await selection(db, stranger, other, **fields)
        refusals.append((refused.value.status_code, refused.value.detail))
    assert set(refusals) == {(403, GENERIC_UNAVAILABLE_DETAIL)}
