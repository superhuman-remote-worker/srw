"""The Project workspace defaults API logic (Slice A2b)."""

from uuid import UUID

import pytest
from fastapi import HTTPException

from shared.workspace_defaults import ProjectDefaults
from orchestrator.schemas.projects import ProjectWorkspaceDefaultsUpdate
from orchestrator.services.builtin_workspace_templates import (
    reconcile_builtin_workspace_templates,
)
from orchestrator.services.project_workspace_defaults import (
    MANAGED_BY_MANIFEST,
    read_project_defaults,
    save_settings_defaults,
)
from orchestrator.services.project_workspace_defaults_view import (
    PICK_A_VISIBLE_TEMPLATE,
    read_view,
    update_view,
)
from tests import test_manifest_native_full_schema as full_schema
from tests.test_workspace_defaults_resolution_real_postgres import (
    _active_revision,
    _manifest_project,
)

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor

SHARED = {"kind": "Catalog", "name": "shared"}


def template(name, backend):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": name, "scope": SHARED},
        "spec": {"backend": backend},
    }


@pytest.fixture(autouse=True)
def chart(monkeypatch):
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)
    monkeypatch.setenv("VM_MODE", "same-cluster")


async def _project(db, *, is_default=False) -> dict:
    row = await db.fetchrow(
        "INSERT INTO projects(name, is_default) VALUES('View', $1) RETURNING *",
        is_default,
    )
    return dict(row)


async def _catalog(db):
    await reconcile_builtin_workspace_templates(
        db, [template("container-minimal", "sandbox"), template("vm-full", "vm")]
    )


@pytest.mark.asyncio
async def test_an_empty_project_shows_the_installation(database):
    view = await read_view(database, await _project(database))
    assert view["stored"] == {
        "jobs": None,
        "sessions": None,
        "container": None,
        "vm": None,
    }
    assert view["effective"]["jobs"] == {"mode": "container", "source": "installation"}
    assert view["effective"]["sessions"] == {
        "mode": "virtual",
        "source": "installation",
    }
    assert view["effective"]["container"] == {
        "template_name": None,
        "source": "builtin",
    }
    assert view["managed_by_manifest"] is False
    assert view["vm_available"] is True
    assert view["installation"] == {
        "jobs": "container",
        "sessions": "virtual",
        "container": None,
        "vm": None,
    }


@pytest.mark.asyncio
async def test_put_saves_and_returns_the_view(database, actor):
    await _catalog(database)
    project = await _project(database)
    view = await update_view(
        database,
        project,
        actor,
        ProjectWorkspaceDefaultsUpdate(
            jobs="container", container={"name": "container-minimal", "scope": SHARED}
        ),
    )
    assert view["stored"]["jobs"] == "container"
    assert view["stored"]["container"] == {
        "ref": {"name": "container-minimal", "scope": SHARED}
    }
    assert view["effective"]["container"] == {
        "template_name": "container-minimal",
        "source": "project",
    }


@pytest.mark.asyncio
async def test_put_refuses_a_backend_mismatch(database, actor):
    await _catalog(database)
    with pytest.raises(HTTPException) as refused:
        await update_view(
            database,
            await _project(database),
            actor,
            ProjectWorkspaceDefaultsUpdate(
                vm={"name": "container-minimal", "scope": SHARED}
            ),
        )
    assert (refused.value.status_code, refused.value.detail) == (
        422,
        "The vm template must be a vm workspace.",
    )


@pytest.mark.asyncio
async def test_put_refuses_account_templates_outside_the_personal_project(
    database, actor
):
    account = {"kind": "Account", "name": str(actor["id"])}
    with pytest.raises(HTTPException) as refused:
        await update_view(
            database,
            await _project(database),
            actor,
            ProjectWorkspaceDefaultsUpdate(
                container={"name": "mine", "scope": account}
            ),
        )
    assert (refused.value.status_code, refused.value.detail) == (
        422,
        PICK_A_VISIBLE_TEMPLATE,
    )


@pytest.mark.asyncio
async def test_put_refuses_vm_values_when_vms_are_off(database, actor, monkeypatch):
    monkeypatch.setenv("VM_MODE", "off")
    with pytest.raises(HTTPException) as refused:
        await update_view(
            database,
            await _project(database),
            actor,
            ProjectWorkspaceDefaultsUpdate(jobs="vm"),
        )
    assert (refused.value.status_code, refused.value.detail) == (
        422,
        "VM workspaces are not available on this installation.",
    )


@pytest.mark.asyncio
async def test_put_on_a_manifest_row_is_409(database, actor):
    project_id = await _manifest_project(database, actor)
    project = dict(
        await database.fetchrow(
            "SELECT * FROM projects WHERE id=$1", UUID(str(project_id))
        )
    )
    with pytest.raises(HTTPException) as refused:
        await update_view(
            database, project, actor, ProjectWorkspaceDefaultsUpdate(jobs="none")
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        MANAGED_BY_MANIFEST,
    )
    assert (await read_view(database, project))["managed_by_manifest"] is True


@pytest.mark.asyncio
async def test_the_view_heals_a_manifest_row_left_behind(database, actor):
    project_id = await _manifest_project(database, actor)
    project = dict(
        await database.fetchrow(
            "SELECT * FROM projects WHERE id=$1", UUID(str(project_id))
        )
    )
    await database.execute(
        "UPDATE project_workspace_defaults SET manifest_revision='sha256:stale' "
        "WHERE project_id=$1",
        project["id"],
    )
    view = await read_view(database, project)
    assert view["managed_by_manifest"] is True
    assert view["stored"]["jobs"] == "container"
    assert (await read_project_defaults(database, project_id)).manifest_revision == (
        await _active_revision(database, project_id)
    )

    # Without its Project manifest the row is released, and editable again.
    await database.execute(
        "UPDATE srw_resources SET deleted_at=now() WHERE kind='Project' AND linked_id=$1",
        project["id"],
    )
    view = await read_view(database, project)
    assert view["managed_by_manifest"] is False
    assert view["stored"]["jobs"] is None


@pytest.mark.asyncio
async def test_a_deleted_template_is_flagged(database, actor):
    project = await _project(database)
    gone = {"ref": {"name": "gone", "scope": SHARED}}
    await save_settings_defaults(
        database,
        project["id"],
        ProjectDefaults(container=gone),
        actor_id=str(actor["id"]),
    )
    view = await read_view(database, project)
    assert view["template_problems"] == {
        "container": "This Project's container template 'gone' no longer exists."
    }
