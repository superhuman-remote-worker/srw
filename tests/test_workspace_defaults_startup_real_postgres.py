"""The startup check and the backfill (Slice A2b)."""

import json

import pytest

from shared.workspace_defaults import ProjectDefaults
from orchestrator.services.manifest_projects import persist_project_resource
from orchestrator.services.project_workspace_defaults import (
    read_project_defaults,
    write_manifest_defaults,
)
from orchestrator.services.workspace_defaults_backfill import (
    backfill_workspace_defaults,
)
from orchestrator.services.workspace_defaults_resolution import (
    check_installation_workspace_defaults,
    installation_problems,
)
from tests import test_manifest_native_full_schema as full_schema

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor


@pytest.fixture(autouse=True)
def chart(monkeypatch):
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.setenv("VM_MODE", "same-cluster")


@pytest.mark.asyncio
async def test_shipped_values_have_no_problems(database):
    assert await check_installation_workspace_defaults(database) == []
    assert installation_problems() == []


@pytest.mark.asyncio
async def test_malformed_installation_defaults_never_stop_startup(
    database, monkeypatch
):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", "not json")
    problems = await check_installation_workspace_defaults(database)
    assert len(problems) == 1
    assert problems[0].startswith(
        "The installation's workspace defaults (Helm workspace.defaults) are invalid:"
    )


@pytest.mark.asyncio
async def test_a_named_template_must_exist(database, monkeypatch):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"container": "company-image"}))
    assert await check_installation_workspace_defaults(database) == [
        "The installation's container template 'company-image' (Helm workspace.defaults.container) no longer exists."
    ]


@pytest.mark.asyncio
async def test_vm_values_need_vms(database, monkeypatch):
    monkeypatch.setenv("VM_MODE", "off")
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"jobs": "vm"}))
    assert await check_installation_workspace_defaults(database) == [
        "VM workspaces are not available on this installation. (Helm workspace.defaults.jobs)"
    ]


async def _project(db, **columns) -> str:
    override = columns.get("default_config_override")
    return str(
        await db.fetchval(
            "INSERT INTO projects(name, default_config_override) VALUES('Backfill', $1::jsonb) RETURNING id",
            json.dumps(override) if override is not None else None,
        )
    )


@pytest.mark.asyncio
async def test_the_legacy_project_backend_moves_into_a_row(database):
    project_id = await _project(
        database, default_config_override={"workspace": {"backend": "sandbox"}}
    )
    counts = await backfill_workspace_defaults(database)
    assert counts["legacy_project"] == 1
    row = await read_project_defaults(database, project_id)
    assert (row.jobs, row.sessions, row.source) == (
        "container",
        "container",
        "settings",
    )


@pytest.mark.asyncio
async def test_the_preference_moves_to_the_personal_project(database, actor):
    project_id = await _project(database)
    await database.execute(
        "UPDATE users SET default_project_id=$1::uuid, settings=$2::jsonb WHERE id=$3",
        project_id,
        json.dumps({"persistent_agent": {"workspace_backend": "sandbox"}}),
        actor["id"],
    )
    counts = await backfill_workspace_defaults(database)
    assert counts["preference"] == 1
    assert (await read_project_defaults(database, project_id)).sessions == "container"
    stored = await database.fetchval(
        "SELECT settings FROM users WHERE id=$1", actor["id"]
    )
    stored = json.loads(stored) if isinstance(stored, str) else stored
    assert stored["persistent_agent"]["workspace_backend"] == "sandbox"


@pytest.mark.asyncio
async def test_the_backfill_never_overwrites_a_row(database, actor):
    manifest_project = await _project(database)
    await write_manifest_defaults(
        database,
        manifest_project,
        ProjectDefaults(sessions="vm", source="manifest", manifest_revision="sha256:m"),
    )
    await database.execute(
        "UPDATE users SET default_project_id=$1::uuid, settings=$2::jsonb WHERE id=$3",
        manifest_project,
        json.dumps({"persistent_agent": {"workspace_backend": "none"}}),
        actor["id"],
    )
    first = await backfill_workspace_defaults(database)
    second = await backfill_workspace_defaults(database)
    assert first["preference"] == 0 and second == {
        "manifest": 0,
        "legacy_project": 0,
        "preference": 0,
    }
    assert (await read_project_defaults(database, manifest_project)).sessions == "vm"


@pytest.mark.asyncio
async def test_a_legacy_project_migrated_into_a_manifest_resource_gets_a_settings_row(
    database, actor
):
    """Controller ruling R1 (revised).

    A legacy Project's ``default_config_override.workspace.backend`` survives
    production's migration into a manifest resource as
    ``sharedConfig.workspace.backend`` in the resource's source recipe
    (``manifest_projects.persist_project_resource`` copies it there and
    clears the legacy column). That is not an authored ``defaults.workspace``,
    so the backfill must record it as a *settings* row -- not a manifest row
    -- counted under ``legacy_project``.
    """
    project_id = await _project(
        database, default_config_override={"workspace": {"backend": "sandbox"}}
    )
    async with database.transaction_scope():
        resource = await persist_project_resource(
            database, project_id, owner_id=actor["id"]
        )
    assert resource is not None
    # Production's migration clears the legacy column and points the Project
    # at its new manifest resource.
    migrated = await database.fetchrow(
        "SELECT manifest_resource_id, default_config_override FROM projects WHERE id=$1",
        project_id,
    )
    assert migrated["manifest_resource_id"] is not None
    assert migrated["default_config_override"] is None
    # No row yet: persist_project_resource's own sync_manifest_defaults call
    # released a (non-existent) manifest row, because the resolved spec has
    # no defaults.workspace.
    assert await read_project_defaults(database, project_id) is None

    first = await backfill_workspace_defaults(database)
    assert first["legacy_project"] == 1
    assert first["manifest"] == 0
    row = await read_project_defaults(database, project_id)
    assert (row.jobs, row.sessions, row.source) == (
        "container",
        "container",
        "settings",
    )

    second = await backfill_workspace_defaults(database)
    assert second == {"manifest": 0, "legacy_project": 0, "preference": 0}
    unchanged = await read_project_defaults(database, project_id)
    assert (unchanged.jobs, unchanged.sessions, unchanged.source) == (
        "container",
        "container",
        "settings",
    )
