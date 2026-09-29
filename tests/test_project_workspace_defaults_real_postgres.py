"""project_workspace_defaults: storage and ownership (Slice A2b)."""

import pytest
from fastapi import HTTPException

from shared.workspace_defaults import ProjectDefaults
from orchestrator.services.project_workspace_defaults import (
    MANAGED_BY_MANIFEST,
    read_project_defaults,
    release_manifest_defaults,
    save_settings_defaults,
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
