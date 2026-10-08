"""D3c review follow-ups, on a real PostgreSQL.

* a natively authored Project keeps what its author wrote: an entry naming a
  connector an apply never linked (every native Project before D3c), and its
  ``defaults.connectors`` alias, survive the startup heal, unrelated link
  writes and a re-apply; only the entry of the connector a write unlinked
  goes;
* changing ``defaults.connectors`` by manifest needs a Project owner or an
  administrator, as ``PUT /connector-defaults`` does;
* the picker's ``default_selected`` and the creation defaults take a project
  default only when every target project chose it and links it;
* a stale inline child a saved Job still names is kept;
* reading a Connector resource stays with its scope; naming it follows the
  connector policy;
* an apply that names a knowledge base the caller cannot see answers 403.
"""

from __future__ import annotations

import json
from functools import partial
from unittest.mock import AsyncMock
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator.services import project_connectors
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.datasource_policy import default_datasource_selection
from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.manifest_projects import persist_project_resource
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.project_connector_defaults import (
    read_project_connector_defaults,
    save_settings_connector_defaults,
)
from orchestrator.services.project_connectors import DEFAULTS_AUTHORITY_DETAIL
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    authorize_thread_datasource_selection,
)
from tests import test_manifest_native_full_schema as full_schema
from tests.test_project_connectors_real_postgres import (
    _apply,
    _connector,
    _entries,
    _knowledge_base,
    _links,
    _project,
    _project_document,
    _project_resource,
    _ref_of,
    _user,
)

database = full_schema.database
postgres_url = full_schema.postgres_url


def _inline(datasource_id: str) -> dict:
    return {
        "inline": {
            "driver": "srw.datasource/v1",
            "config": {"datasourceId": datasource_id},
        }
    }


async def _native_project(db, owner, connectors, monkeypatch, **spec) -> str:
    """A natively authored Project as an apply before D3c left it: its
    entries name connectors, nothing is linked."""

    async def link_nothing(*_args, **_kwargs):
        return None

    async def refresh_nothing(*_args, **_kwargs):
        return "none", None

    with monkeypatch.context() as patched:
        patched.setattr(
            project_connectors, "sync_project_connector_links", link_nothing
        )
        patched.setattr(project_connectors, "refresh_project", refresh_nothing)
        result = await _apply(
            db, _project_document("native-team", owner, connectors, **spec), owner
        )
    return str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )


def _expected(owner: dict, resource: dict) -> dict:
    return {f"Project/Account/{owner['id']}/native-team": resource["resource_version"]}


# =============================================================================
# A natively authored Project keeps what its author wrote
# =============================================================================


@pytest.mark.asyncio
async def test_an_authored_entry_for_an_unlinked_connector_survives(
    database, monkeypatch
):
    """The dev shape: an inline srw.datasource/v1 entry naming a connector
    that is authorized but not linked, plus defaults.connectors naming it."""
    db = database
    owner = await _user(db, "Owner")
    source = await _connector(db, "Source", owner)
    project = await _native_project(
        db,
        owner,
        {
            "source": _inline(source),
            "env": {"inline": {"driver": "srw.env/v1", "config": {"names": ["X"]}}},
        },
        monkeypatch,
        defaults={"connectors": ["source"]},
    )
    authored = await _project_resource(db, project)
    assert await _links(db, project) == set()

    def unchanged(resource):
        return (
            resource["document"] == authored["document"]
            and resource["resource_version"] == authored["resource_version"]
        )

    # The startup heal reports it and leaves it as written.
    counts = await project_connectors.heal_project_connectors(db)
    assert counts == {"native-drift": 1}
    assert unchanged(await _project_resource(db, project))
    child = await ManifestStore(db).by_name(
        "Connector", {"kind": "Project", "name": project}, "source"
    )
    assert child is not None

    # A link write elsewhere, of a connector this Project does not list.
    elsewhere = await _project(db, "Elsewhere", owner)
    other = await _connector(db, "Other", owner, project_ids=[elsewhere])
    assert unchanged(await _project_resource(db, project))

    # A link to this Project adds its entry and touches nothing else.
    assert await db.link_datasource_to_project(project, other)
    after_link = await _project_resource(db, project)
    entries = after_link["document"]["spec"]["resources"]["connectors"]
    assert entries["source"] == _inline(source)
    assert set(entries) == {"source", "env", (await _ref_of(db, other))["name"]}
    assert after_link["document"]["spec"]["defaults"] == {"connectors": ["source"]}

    # Unlinked from the other project only: this Project's list is unchanged.
    assert await db.unlink_datasource_from_project(elsewhere, other)
    assert (await _project_resource(db, project))["document"] == after_link["document"]

    # Unlinked from this Project: only that entry goes.
    assert await db.unlink_datasource_from_project(project, other)
    resource = await _project_resource(db, project)
    assert resource["document"]["spec"] == authored["document"]["spec"]
    assert (await read_project_connector_defaults(db, project)).connector_ids == [
        source
    ]

    # Re-applying it changes no link: carried-over entries stay as they are.
    result = await _apply(
        db, resource["document"], owner, expected=_expected(owner, resource)
    )
    assert await _links(db, project) == set()
    assert result["resources"][0]["resource"]["spec"] == authored["document"]["spec"]


@pytest.mark.asyncio
async def test_a_native_project_in_step_is_counted_native(database, monkeypatch):
    db = database
    owner = await _user(db, "Owner")
    linked = await _connector(db, "Linked", owner)
    result = await _apply(
        db,
        _project_document(
            "native-team", owner, {"db": {"ref": await _ref_of(db, linked)}}
        ),
        owner,
    )
    assert result["resources"][0]["resource"]["spec"]["resources"]["connectors"]
    assert await project_connectors.heal_project_connectors(db) == {"native": 1}


# =============================================================================
# Connector defaults by manifest need the owner
# =============================================================================


@pytest.mark.asyncio
async def test_only_an_owner_changes_connector_defaults_by_manifest(database):
    db = database
    owner = await _user(db, "Owner")
    editor = await _user(db, "Editor")
    admin = await _user(db, "Admin", admin=True)
    connector = await _connector(db, "Prod DB", owner)
    document = _project_document(
        "native-team", owner, {"db": {"ref": await _ref_of(db, connector)}}
    )
    result = await _apply(db, document, owner)
    project = str(
        (await ManifestStore(db).by_id(result["resources"][0]["uid"]))["linked_id"]
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(project),
        UUID(editor["id"]),
    )

    resource = await _project_resource(db, project)
    with_defaults = json.loads(json.dumps(resource["document"]))
    with_defaults["spec"]["defaults"] = {"connectors": ["db"]}
    with pytest.raises(HTTPException) as refused:
        await _apply(db, with_defaults, editor, expected=_expected(owner, resource))
    assert (refused.value.status_code, refused.value.detail) == (
        403,
        DEFAULTS_AUTHORITY_DETAIL,
    )
    assert await read_project_connector_defaults(db, project) is None

    # An editor's apply that leaves the defaults alone goes through.
    described = json.loads(json.dumps(resource["document"]))
    described["spec"]["description"] = "Edited by an editor"
    await _apply(db, described, editor, expected=_expected(owner, resource))

    # The owner sets them; an editor may then re-apply them unchanged, and an
    # administrator may change them.
    resource = await _project_resource(db, project)
    with_defaults["spec"]["description"] = "Edited by an editor"
    await _apply(db, with_defaults, owner, expected=_expected(owner, resource))
    assert (await read_project_connector_defaults(db, project)).connector_ids == [
        connector
    ]
    resource = await _project_resource(db, project)
    await _apply(db, resource["document"], editor, expected=_expected(owner, resource))
    resource = await _project_resource(db, project)
    cleared = json.loads(json.dumps(resource["document"]))
    cleared["spec"]["defaults"] = {"connectors": []}
    with pytest.raises(HTTPException):
        await _apply(db, cleared, editor, expected=_expected(owner, resource))
    await _apply(db, cleared, admin, expected=_expected(owner, resource))
    assert (await read_project_connector_defaults(db, project)).connector_ids == []


# =============================================================================
# Project defaults: the picker and every target project
# =============================================================================


@pytest.mark.asyncio
async def test_the_picker_preselects_a_project_default(database):
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    project = await _project(db, "Team", owner, **{member["id"]: "editor"})
    kb = await _knowledge_base(db, project, owner)
    linked = await _connector(
        db, "Linked", owner, scope_mode="projects", project_ids=[project]
    )

    async def preselected(user, projects):
        rows = await db.list_eligible_datasources(user["id"], projects)
        return {str(row["id"]) for row in rows if row["default_selected"]}

    assert await preselected(member, [project]) == {kb}
    await save_settings_connector_defaults(db, project, [linked], actor_id=owner["id"])
    assert await preselected(member, [project]) == {kb, linked}
    assert linked not in await preselected(member, [])
    # The picker and creation agree.
    selected, _ = await default_datasource_selection(
        db, member["id"], [project], "sandbox"
    )
    assert set(selected) == {kb, linked}


@pytest.mark.asyncio
async def test_one_projects_default_never_reaches_another_projects_work(database):
    db = database
    owner = await _user(db, "Owner")
    first = await _project(db, "First", owner)
    second = await _project(db, "Second", owner)
    both = await _connector(db, "Both", owner, project_ids=[first, second])
    await save_settings_connector_defaults(db, first, [both], actor_id=owner["id"])

    async def flagged(projects):
        rows = await db.list_default_datasource_candidates(owner["id"], projects)
        return {str(row["id"]) for row in rows if row.get("project_default")}

    async def picked(projects):
        rows = await db.list_eligible_datasources(owner["id"], projects)
        return {str(row["id"]) for row in rows if row["default_selected"]}

    assert await flagged([first]) == {both}
    assert await flagged([first, second]) == set()
    assert both not in await picked([first, second])
    await save_settings_connector_defaults(db, second, [both], actor_id=owner["id"])
    assert await flagged([first, second]) == {both}
    assert both in await picked([first, second])


# =============================================================================
# A stale child a saved Job still names is kept
# =============================================================================


@pytest.mark.asyncio
async def test_a_stale_child_a_saved_job_names_is_kept(database, monkeypatch):
    db = database
    owner = await _user(db, "Owner")
    project = await _project(db, "Team", owner)
    connector = await _connector(db, "Prod DB", owner)

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
    with monkeypatch.context() as patched:
        patched.setattr(project_connectors, "project_link_rows", before_d3c)
        async with db.transaction_scope():
            await persist_project_resource(db, project)
    alias = project_connectors.legacy_alias(connector)

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
    job["spec"]["execution"]["connectors"] = {
        "db": {"ref": {"name": alias, "scope": {"kind": "Project", "name": project}}}
    }
    service = ManifestResourceService(db, admit_job=execution.admit)
    admitted = await service.apply(json.dumps([job]), owner, format="json")
    work_id = next(iter(admitted["executions"].values()))
    await db.execute("UPDATE jobs SET status='completed' WHERE id=$1", UUID(work_id))

    # The heal rebuilds the Project with a ref, and keeps the child the Job
    # names; a second start finds nothing more to do.
    assert (await project_connectors.heal_project_connectors(db)) == {"rebuilt": 1}
    ref = await _ref_of(db, connector)
    assert await _entries(db, project) == {ref["name"]: {"ref": ref}}
    assert await ManifestStore(db).by_name(
        "Connector", {"kind": "Project", "name": project}, alias
    )
    assert (await project_connectors.heal_project_connectors(db)) == {"unchanged": 1}
    # The Job re-applies: its frozen dependency is still there.
    await service.apply(json.dumps([job]), owner, format="json")


# =============================================================================
# Reading a Connector resource; naming a hidden knowledge base
# =============================================================================


@pytest.mark.asyncio
async def test_reading_a_connector_resource_stays_with_its_scope(database):
    db = database
    owner = await _user(db, "Owner")
    member = await _user(db, "Member")
    stranger = await _user(db, "Stranger")
    publisher = await _user(db, "Publisher")
    project = await _project(db, "Team", owner, **{member["id"]: "viewer"})
    mail = str(
        (
            await db.create_datasource(
                name="Ops mailbox",
                ds_type="email",
                connection_url=None,
                created_by=owner["id"],
                credentials={
                    "backend": "imap",
                    "username": "ops-lead@corp.example",
                    "password": "hunter2",
                    "imap": {
                        "host": "imap.corp.example",
                        "port": 993,
                        "security": "ssl",
                    },
                    "smtp": {
                        "host": "smtp.corp.example",
                        "port": 465,
                        "security": "ssl",
                    },
                },
                project_ids=[project],
            )
        )["id"]
    )
    public = await _connector(
        db, "Public", publisher, is_global=True, read_only=True, scope_mode="all"
    )
    service = ManifestResourceService(db)
    assert (await service.get(mail, owner))["uid"] == mail
    for user, datasource_id in ((member, mail), (stranger, public)):
        with pytest.raises(HTTPException) as hidden:
            await service.get(datasource_id, user)
        assert hidden.value.status_code == 403
    # Naming it from the Project still works for the member.
    from orchestrator.services.manifest_authority import ManifestAuthority

    await ManifestAuthority(db, member).resource(
        await ManifestStore(db).by_id(mail), reference=True
    )


@pytest.mark.asyncio
async def test_an_apply_naming_a_hidden_knowledge_base_answers_403(
    database, monkeypatch
):
    db = database
    owner = await _user(db, "Owner")
    stranger = await _user(db, "Stranger")
    team = await _project(db, "Team", owner)
    kb = await _knowledge_base(db, team, owner)
    own = await _project(db, "Own", stranger)
    resource = await _project_resource(db, own)
    document = _project_document(f"project-{own}", stranger, {"kb": _inline(kb)})
    document["metadata"] = resource["document"]["metadata"]
    expected = {
        f"Project/Account/{stranger['id']}/project-{own}": resource["resource_version"]
    }
    with pytest.raises(HTTPException) as refused:
        await _apply(db, document, stranger, expected=expected)
    assert refused.value.status_code == 403
    # Seen (a member of its project), it is the knowledge base's own 409.
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'viewer')",
        UUID(team),
        UUID(stranger["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await _apply(db, document, stranger, expected=expected)
    assert refused.value.status_code == 409
    assert kb not in await _links(db, own)
