"""A project's cloud folder in another project's session (main cloud slice 5).

A native ``project-cloud:<B>`` row is eligible in work of project A when the
owner is a member of B: an editor or owner gets every level, a viewer at
most read-only (in B's own work too); it is never preselected outside B, and
no administrator reaches it without being a member. No other kind or type
crosses projects: B's knowledge base and B's ordinary connectors stay B's.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from orchestrator.services import datasources as datasources_service
from orchestrator.services.cloud.nextcloud import NEXTCLOUD_CAPABILITIES
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.datasource_policy import (
    DatasourceUnavailableError,
    access_ceiling_for,
    authorize_datasource_selection,
    classify_datasource_selection,
    default_datasource_selection,
)
from tests import test_manifest_native_full_schema as full_schema
from tests.test_native_cloud_connectors_real_postgres import (
    _cloud_project,
    _ensure,
    _instance,
    _native_rows,
)
from tests.test_project_connectors_real_postgres import (
    _connector,
    _knowledge_base,
    _project,
    _user,
)

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _world(db):
    """Project B with a cloud folder, its KB and a linked connector; project A
    of every user; B's editor, viewer, an outsider and an admin."""
    owner = await _user(db, "B owner")
    editor = await _user(db, "B editor")
    viewer = await _user(db, "B viewer")
    outsider = await _user(db, "Outsider")
    admin = await _user(db, "Admin", admin=True)
    instance = await _instance(db)
    b = await _cloud_project(
        db,
        "Bravo",
        owner,
        instance,
        **{editor["id"]: "editor", viewer["id"]: "viewer"},
    )
    b_id = str(b["id"])
    assert await _ensure(db, b, instance) == "created"
    [folder] = await _native_rows(db, b_id)
    kb = await _knowledge_base(db, b_id, owner)
    linked = await _connector(
        db, "B warehouse", owner, scope_mode="projects", project_ids=[b_id]
    )
    a = await _project(
        db,
        "Alpha",
        owner,
        **{
            editor["id"]: "editor",
            viewer["id"]: "editor",
            outsider["id"]: "editor",
            admin["id"]: "editor",
        },
    )
    return SimpleNamespace(
        owner=owner,
        editor=editor,
        viewer=viewer,
        outsider=outsider,
        admin=admin,
        a=a,
        b=b_id,
        folder=str(folder["id"]),
        kb=kb,
        linked=linked,
    )


async def _eligible(db, user, projects, **kwargs):
    rows = await datasources_service.list_eligible_datasources(
        user=user,
        project_id=projects,
        require_project_member=_member_of_anything,
        dependencies=SimpleNamespace(
            store=db,
            connector_drivers=builtin_connector_drivers(),
            is_protected_cloud_mode_enabled=lambda: True,
        ),
        **kwargs,
    )
    return {str(row["id"]): row for row in rows}


async def _member_of_anything(_project_id):
    return None


def _levels(row) -> dict[str, dict]:
    return {level["id"]: level for level in row["access"]["levels"]}


@pytest.mark.asyncio
async def test_an_editor_of_b_sees_bs_folder_in_a_not_preselected(database):
    world = await _world(database)
    listed = await _eligible(
        database,
        world.editor,
        [world.a],
        execution="session",
        workspace_backend="sandbox",
    )
    row = listed[world.folder]
    assert row["default_selected"] is False
    assert "native_member_role" not in row
    assert row["access"]["ceiling"] is None
    levels = _levels(row)
    for level in ("read_only", "protected", "read_write"):
        offered = NEXTCLOUD_CAPABILITIES.offers(
            "cloud_folder", "project", level, workspace_backend="sandbox"
        )
        assert levels[level]["offered"] is offered
        if not offered:
            assert levels[level]["reason"] == "level_not_offered"
    assert row["access"]["default"] == "read_write"
    assert row["access"]["default_origin"] == "driver_default"
    # B's knowledge base and B's ordinary connectors stay B's.
    assert world.kb not in listed
    assert world.linked not in listed


@pytest.mark.asyncio
async def test_a_viewer_of_b_is_capped_at_read_only_in_both_projects(database):
    world = await _world(database)
    for projects in ([world.a], [world.b]):
        listed = await _eligible(
            database,
            world.viewer,
            projects,
            execution="session",
            workspace_backend="sandbox",
        )
        access = listed[world.folder]["access"]
        assert access["ceiling"] == "read_only"
        levels = _levels(listed[world.folder])
        assert levels["protected"]["reason"] == "above_ceiling"
        assert levels["read_write"]["reason"] == "above_ceiling"
        assert not levels["protected"]["offered"]
        assert not levels["read_write"]["offered"]
        if NEXTCLOUD_CAPABILITIES.offers(
            "cloud_folder", "project", "read_only", workspace_backend="sandbox"
        ):
            assert levels["read_only"]["offered"]
            assert (access["default"], access["default_origin"]) == (
                "read_only",
                "clamped",
            )
        else:
            # Never a write in its place: the viewer gets no level here.
            assert access["default"] is None
    # The policy's verdict carries the same ceiling.
    verdicts, _ = await classify_datasource_selection(
        database,
        world.viewer,
        world.viewer["id"],
        [world.folder],
        [world.a],
        "sandbox",
        execution_kind="session",
    )
    assert [(v.denied, v.access_ceiling) for v in verdicts] == [(False, "read_only")]


@pytest.mark.asyncio
async def test_a_non_member_never_sees_it_and_naming_it_is_refused(database):
    world = await _world(database)
    for user in (world.outsider, world.admin):
        listed = await _eligible(database, user, [world.a], execution="session")
        assert world.folder not in listed
        with pytest.raises(DatasourceUnavailableError):
            await authorize_datasource_selection(
                database,
                user,
                user["id"],
                [world.folder],
                [world.a],
                "sandbox",
                allow_admin_explicit_override=True,
                execution_kind="session",
            )
    # A member names it explicitly in A: allowed.
    selected, revisions = await authorize_datasource_selection(
        database,
        world.editor,
        world.editor["id"],
        [world.folder],
        [world.a],
        "sandbox",
        execution_kind="session",
    )
    assert selected == [world.folder] and world.folder in revisions


@pytest.mark.asyncio
async def test_b_kb_and_bs_connectors_stay_refused_in_a(database):
    world = await _world(database)
    for connector in (world.kb, world.linked):
        with pytest.raises(DatasourceUnavailableError):
            await authorize_datasource_selection(
                database,
                world.editor,
                world.editor["id"],
                [connector],
                [world.a],
                "sandbox",
                execution_kind="session",
            )


@pytest.mark.asyncio
async def test_auto_attach_in_a_never_includes_bs_folder(database):
    world = await _world(database)
    # B's owner created the native row, and is a member of A.
    for user in (world.owner, world.editor):
        selected, _ = await default_datasource_selection(
            database, user["id"], [world.a], "sandbox", execution_kind="session"
        )
        assert world.folder not in selected
        listed = await _eligible(database, user, [world.a], execution="session")
        assert listed[world.folder]["default_selected"] is False
    selected, _ = await default_datasource_selection(
        database, world.editor["id"], [world.b], "sandbox", execution_kind="session"
    )
    assert world.folder in selected


def test_only_a_viewers_project_cloud_folder_has_a_ceiling():
    folder = {
        "type": "cloud_folder",
        "managed_key": "project-cloud:22222222-2222-4222-8222-222222222222",
    }
    kb = {
        "type": "kb",
        "managed_key": "project-kb:22222222-2222-4222-8222-222222222222",
    }
    assert access_ceiling_for(folder, "viewer") == "read_only"
    for role in ("editor", "owner", None):
        assert access_ceiling_for(folder, role) is None
    assert access_ceiling_for(kb, "viewer") is None
    assert access_ceiling_for({"type": "postgresql"}, "viewer") is None
