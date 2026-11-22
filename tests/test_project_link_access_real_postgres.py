"""A project link's default access level on a real PostgreSQL (0522/0523).

The column takes a level id or NULL, never beside a ``read_only`` boolean;
the store writes it through link and update, reads it back as
``project_access``, and a change bumps the connector's policy revision, so a
creation that resolved against the old default is refused as stale.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg
import pytest

from tests import test_manifest_native_full_schema as full_schema
from tests.test_native_cloud_connectors_real_postgres import (
    _cloud_project,
    _ensure,
    _instance,
    _native_rows,
)
from tests.test_project_connectors_real_postgres import _user

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _folder(db):
    owner = await _user(db, "Link owner")
    instance = await _instance(db)
    project = await _cloud_project(db, "Linked", owner, instance)
    assert await _ensure(db, project, instance) == "created"
    [row] = await _native_rows(db, str(project["id"]))
    return owner, str(project["id"]), str(row["id"])


async def _link(db, project_id: str, row_id: str) -> dict:
    [linked] = [
        row
        for row in await db.list_project_datasources(project_id)
        if str(row["id"]) == row_id
    ]
    return linked


@pytest.mark.asyncio
async def test_both_checks_hold(database):
    _owner, project_id, row_id = await _folder(database)
    validated = {
        record["conname"]: record["convalidated"]
        for record in await database.fetch(
            "SELECT conname, convalidated FROM pg_constraint "
            "WHERE conname LIKE 'project_datasources_access%'"
        )
    }
    assert validated == {
        "project_datasources_access_check": True,
        "project_datasources_access_or_read_only_check": True,
    }
    where = (UUID(project_id), UUID(row_id))
    with pytest.raises(asyncpg.CheckViolationError):
        await database.execute(
            "UPDATE project_datasources SET access='read only' "
            "WHERE project_id=$1 AND datasource_id=$2",
            *where,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await database.execute(
            "UPDATE project_datasources SET access='protected', read_only=FALSE "
            "WHERE project_id=$1 AND datasource_id=$2",
            *where,
        )
    await database.execute(
        "UPDATE project_datasources SET access='protected', read_only=NULL "
        "WHERE project_id=$1 AND datasource_id=$2",
        *where,
    )


@pytest.mark.asyncio
async def test_the_store_writes_reads_and_bumps_the_revision(database):
    owner, project_id, row_id = await _folder(database)
    before = (await database.get_datasource(row_id))["policy_revision"]
    assert await database.update_project_datasource(
        project_id,
        row_id,
        read_only=None,
        access="protected",
        authority_user_id=owner["id"],
    )
    linked = await _link(database, project_id, row_id)
    assert linked["project_access"] == "protected"
    assert linked["project_read_only"] is None
    after = (await database.get_datasource(row_id))["policy_revision"]
    assert after > before

    # A description edit leaves the level alone; null clears it.
    assert await database.update_project_datasource(
        project_id,
        row_id,
        description="Drafts",
        authority_user_id=owner["id"],
    )
    linked = await _link(database, project_id, row_id)
    assert linked["project_access"] == "protected"
    assert await database.update_project_datasource(
        project_id, row_id, access=None, authority_user_id=owner["id"]
    )
    linked = await _link(database, project_id, row_id)
    assert linked["project_access"] is None


@pytest.mark.asyncio
async def test_a_link_stores_its_level(database):
    owner = await _user(database, "Linker", admin=True)
    project_id = str(
        await database.fetchval(
            "INSERT INTO projects(name) VALUES('Target') RETURNING id"
        )
    )
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
        UUID(project_id),
        UUID(owner["id"]),
    )
    created = await database.create_datasource(
        name="Warehouse",
        ds_type="postgresql",
        connection_url="postgresql://db/x",
        created_by=owner["id"],
    )
    row_id = str(created["id"])
    await database.link_datasource_to_project(
        project_id,
        row_id,
        read_only=None,
        access="read_only",
        authority_user_id=owner["id"],
        authority_is_admin=True,
    )
    [linked] = await database.list_project_datasources(project_id)
    assert linked["project_access"] == "read_only"
    # POST again is an override update of the same link.
    await database.link_datasource_to_project(
        project_id,
        row_id,
        read_only=True,
        authority_user_id=owner["id"],
        authority_is_admin=True,
    )
    [linked] = await database.list_project_datasources(project_id)
    assert (linked["project_access"], linked["project_read_only"]) == (None, True)


@pytest.mark.asyncio
async def test_an_agent_is_told_the_project_note_else_the_connector_description(
    database,
):
    """What a session's README says under each connector (decision 3 of the
    connectors UX pass): the project's note on the link wins; without one,
    the connector's own description; a connector SRW keeps says nothing of
    its own, since its README entry already explains it."""
    owner = await _user(database, "Note writer", admin=True)
    projects = []
    for name in ("Hotel", "Spa"):
        project_id = str(
            await database.fetchval(
                "INSERT INTO projects(name) VALUES($1) RETURNING id", name
            )
        )
        await database.execute(
            "INSERT INTO project_members(project_id,user_id,role) "
            "VALUES($1,$2,'owner')",
            UUID(project_id),
            UUID(owner["id"]),
        )
        projects.append(project_id)
    hotel, spa = projects
    created = await database.create_datasource(
        name="Bookings",
        ds_type="postgresql",
        connection_url="postgresql://db/bookings",
        description="Guests and their bookings",
        created_by=owner["id"],
    )
    row_id = str(created["id"])
    for project_id in projects:
        await database.link_datasource_to_project(
            project_id, row_id, authority_user_id=owner["id"], authority_is_admin=True
        )

    async def told(project_ids: list[str]) -> str | None:
        [row] = await database.resolve_datasources_for_thread([row_id], project_ids)
        return row["description"]

    # No note anywhere: the connector's own description.
    assert await told([hotel]) == "Guests and their bookings"
    # The project's note replaces it.
    assert await database.update_project_datasource(
        hotel, row_id, description="Read only here", authority_user_id=owner["id"]
    )
    assert await told([hotel]) == "Read only here"
    # Two projects: the note they agree on; a second project without a note
    # does not disagree.
    assert await told([hotel, spa]) == "Read only here"
    assert await database.update_project_datasource(
        spa, row_id, description="Never delete rows", authority_user_id=owner["id"]
    )
    assert await told([hotel, spa]) == "Guests and their bookings"
    # A blank note is no note.
    assert await database.update_project_datasource(
        hotel, row_id, description="   ", authority_user_id=owner["id"]
    )
    assert await told([hotel]) == "Guests and their bookings"

    # SRW's own folder: nothing of its own, but the project's note if set.
    _owner, folder_project, folder_id = await _folder(database)
    [folder] = await database.resolve_datasources_for_thread(
        [folder_id], [folder_project]
    )
    assert folder["description"] is None
    assert await database.update_project_datasource(
        folder_project,
        folder_id,
        description="Drafts go in Entwürfe",
        authority_user_id=_owner["id"],
    )
    [folder] = await database.resolve_datasources_for_thread(
        [folder_id], [folder_project]
    )
    assert folder["description"] == "Drafts go in Entwürfe"


@pytest.mark.asyncio
async def test_the_catalog_counts_types_and_names_a_kept_connector_s_project(
    database,
):
    """The Customize list's type strip and its "Kept by SRW" groups: the
    first page counts each type under every filter but the type, and a
    connector SRW keeps names its project. Both the administrator's query
    (no owner test) and a member's run, since they use different
    parameters."""
    owner, project_id, folder_id = await _folder(database)
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner') "
        "ON CONFLICT DO NOTHING",
        UUID(project_id),
        UUID(owner["id"]),
    )
    for name in ("Bookings", "Rooms"):
        await database.create_datasource(
            name=name,
            ds_type="postgresql",
            connection_url=f"postgresql://db/{name.lower()}",
            created_by=owner["id"],
        )

    for is_admin in (True, False):
        page = await database.list_datasource_catalog(
            owner["id"], [project_id], is_admin=is_admin, managed=False
        )
        assert page["type_counts"] == {"postgresql": 2}
        assert {row["native_project_name"] for row in page["items"]} == {None}

        # A type narrows the rows, not the counts.
        page = await database.list_datasource_catalog(
            owner["id"],
            [project_id],
            is_admin=is_admin,
            managed=False,
            ds_type="kb",
        )
        assert page["items"] == []
        assert page["type_counts"] == {"postgresql": 2}

        kept = await database.list_datasource_catalog(
            owner["id"], [project_id], is_admin=is_admin, managed=True
        )
        [folder] = [row for row in kept["items"] if str(row["id"]) == folder_id]
        assert folder["native_project_name"] == "Linked"
        assert kept["type_counts"].get("cloud_folder") == 1

    # A later page carries no counts.
    first = await database.list_datasource_catalog(
        owner["id"], [project_id], is_admin=True, managed=False, limit=1
    )
    assert first["next_cursor"]
    second = await database.list_datasource_catalog(
        owner["id"],
        [project_id],
        is_admin=True,
        managed=False,
        limit=1,
        cursor=first["next_cursor"],
    )
    assert "type_counts" not in second
