"""project_workspace_defaults: storage and ownership (Slice A2b)."""

import pytest
from fastapi import HTTPException

from shared.workspace_defaults import ProjectDefaults
from orchestrator.services.project_workspace_defaults import (
    MANAGED_BY_MANIFEST,
    read_project_defaults,
    release_manifest_defaults,
    save_settings_defaults,
    sync_manifest_defaults,
    write_manifest_defaults,
)
from tests import test_manifest_native_full_schema as full_schema

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor

CONTAINER = {
    "ref": {"name": "container-minimal", "scope": {"kind": "Catalog", "name": "shared"}}
}
PINNED = {"inline": {"backend": "vm", "resources": {"cpu": 4}}}


async def _project(db) -> str:
    return str(
        await db.fetchval("INSERT INTO projects(name) VALUES('Defaults') RETURNING id")
    )


@pytest.mark.asyncio
async def test_settings_rows_round_trip(database, actor):
    project_id = await _project(database)
    assert await read_project_defaults(database, project_id) is None
    saved = await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", sessions="virtual", container=CONTAINER),
        actor_id=str(actor["id"]),
    )
    assert saved == ProjectDefaults(
        jobs="container", sessions="virtual", container=CONTAINER
    )
    assert await read_project_defaults(database, project_id) == saved


@pytest.mark.asyncio
async def test_a_manifest_row_refuses_settings_writes(database, actor):
    project_id = await _project(database)
    await write_manifest_defaults(
        database,
        project_id,
        ProjectDefaults(
            jobs="vm", vm=PINNED, source="manifest", manifest_revision="sha256:1"
        ),
    )
    with pytest.raises(HTTPException) as refused:
        await save_settings_defaults(
            database,
            project_id,
            ProjectDefaults(jobs="none"),
            actor_id=str(actor["id"]),
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        MANAGED_BY_MANIFEST,
    )


@pytest.mark.asyncio
async def test_a_manifest_takes_over_a_settings_row(database, actor):
    project_id = await _project(database)
    await save_settings_defaults(
        database, project_id, ProjectDefaults(jobs="none"), actor_id=str(actor["id"])
    )
    await write_manifest_defaults(
        database,
        project_id,
        ProjectDefaults(
            sessions="container", source="manifest", manifest_revision="sha256:2"
        ),
    )
    row = await read_project_defaults(database, project_id)
    assert (row.jobs, row.sessions, row.source, row.manifest_revision) == (
        None,
        "container",
        "manifest",
        "sha256:2",
    )


@pytest.mark.asyncio
async def test_release_clears_only_manifest_rows(database, actor):
    kept, released = await _project(database), await _project(database)
    await save_settings_defaults(
        database, kept, ProjectDefaults(jobs="vm"), actor_id=str(actor["id"])
    )
    await write_manifest_defaults(
        database,
        released,
        ProjectDefaults(jobs="vm", source="manifest", manifest_revision="sha256:3"),
    )
    await release_manifest_defaults(database, kept)
    await release_manifest_defaults(database, released)
    assert (await read_project_defaults(database, kept)).jobs == "vm"
    assert await read_project_defaults(database, released) == ProjectDefaults()


@pytest.mark.asyncio
async def test_deleting_the_project_deletes_its_row(database, actor):
    project_id = await _project(database)
    await save_settings_defaults(
        database, project_id, ProjectDefaults(jobs="vm"), actor_id=str(actor["id"])
    )
    await database.execute("DELETE FROM projects WHERE id=$1::uuid", project_id)
    assert await read_project_defaults(database, project_id) is None


SITE = {"inline": {"backend": "sandbox", "environment": {"image": "r.example/site:1"}}}


def resource(project_id, workspace=..., revision="sha256:r1"):
    defaults = {} if workspace is ... else {"workspace": workspace}
    return {
        "kind": "Project",
        "linked_id": project_id,
        "revision": revision,
        "resolved": {
            "spec": {"resources": {"workspaces": {"site": SITE}}, "defaults": defaults}
        },
    }


@pytest.mark.asyncio
async def test_activation_writes_a_manifest_row(database):
    project_id = await _project(database)
    await sync_manifest_defaults(
        database, resource(project_id, {"jobs": "container", "container": "site"})
    )
    row = await read_project_defaults(database, project_id)
    assert (row.jobs, row.container, row.source, row.manifest_revision) == (
        "container",
        SITE,
        "manifest",
        "sha256:r1",
    )


@pytest.mark.asyncio
async def test_dropping_the_field_releases_the_row(database, actor):
    project_id = await _project(database)
    await sync_manifest_defaults(database, resource(project_id, "site"))
    await sync_manifest_defaults(database, resource(project_id, revision="sha256:r2"))
    assert await read_project_defaults(database, project_id) == ProjectDefaults()
    await save_settings_defaults(
        database, project_id, ProjectDefaults(jobs="vm"), actor_id=str(actor["id"])
    )


@pytest.mark.asyncio
async def test_a_manifest_without_the_field_leaves_settings_alone(database, actor):
    project_id = await _project(database)
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(sessions="container"),
        actor_id=str(actor["id"]),
    )
    await sync_manifest_defaults(database, resource(project_id))
    assert (await read_project_defaults(database, project_id)).sessions == "container"


@pytest.mark.asyncio
async def test_non_projects_and_unlinked_projects_are_ignored(database):
    await sync_manifest_defaults(database, {"kind": "Expert", "linked_id": None})
    await sync_manifest_defaults(
        database, {**resource(None, "site"), "linked_id": None}
    )
