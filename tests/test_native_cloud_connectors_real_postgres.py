"""A project's own cloud folder connector on a real PostgreSQL (main cloud
slice 3, 3.3).

* provisioning creates exactly one row per project, under its managed key,
  and a second pass creates nothing; a repaired folder handle repoints it;
* its Connector resource lives in the project's scope, marked platform
  managed, and the Project manifest lists it;
* it is eligible, and selected by default, for every member of its project,
  and for no other project's work;
* deleting the project removes it, and deleting its creator only detaches
  the record;
* the startup pass gives every cloud project of the active installation its
  row, skips the rest, and a second pass finds them all present;
* a user's own connector (``user-cloud-root``) is eligible for that user
  only, an administrator included.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services import project_cloud_connectors
from orchestrator.services.datasource_policy import default_datasource_selection
from orchestrator.services.manifest_store import ManifestStore
from tests import test_manifest_native_full_schema as full_schema
from tests.test_project_connectors_real_postgres import (
    _connector,
    _job_selection,
    _knowledge_base,
    _links,
    _listed,
    _project,
    _session_selection,
    _user,
)

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _instance(db, backend: str = "nextcloud") -> str:
    instance_id = uuid4()
    await db.execute(
        "INSERT INTO main_cloud_backend_instances "
        "(id,backend_id,routing,routing_sha256,installation_proof_sha256,"
        "secret_refs) VALUES ($1,$2,'{}'::jsonb,$3,$4,'{}'::jsonb)",
        instance_id,
        backend,
        uuid4().hex * 2,
        uuid4().hex * 2,
    )
    return str(instance_id)


async def _cloud_project(
    db,
    name: str,
    owner: dict,
    instance: str | None,
    *,
    handle: str | None = ...,
    **members,
) -> dict:
    project_id = await _project(db, name, owner, **members)
    await db.execute(
        "UPDATE projects SET main_cloud_backend=$2, main_cloud_backend_instance_id=$3,"
        " main_cloud_folder_handle=$4 WHERE id=$1",
        UUID(project_id),
        "nextcloud" if instance else None,
        UUID(instance) if instance else None,
        f"nextcloud:{name}" if handle is ... else handle,
    )
    return await db.get_project(project_id)


async def _ensure(db, project: dict, instance: str) -> str:
    return await project_cloud_connectors.ensure_project_cloud_connector(
        db, project, active_instance_id=instance, log=MagicMock()
    )


async def _native_rows(db, project_id: str, kind: str = "project-cloud") -> list[dict]:
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM datasources WHERE managed_key=$1",
            f"{kind}:{project_id}",
        )
    ]


async def _copy_id(db, project_id: str) -> str:
    [copy] = await _native_rows(db, project_id, "project-cloud-checkout")
    return str(copy["id"])


@pytest.mark.asyncio
async def test_provisioning_creates_one_row_and_a_second_pass_none(database):
    owner = await _user(database, "Cloud owner")
    instance = await _instance(database)
    project = await _cloud_project(database, "Atlas", owner, instance)
    project_id = str(project["id"])

    assert await _ensure(database, project, instance) == "created"
    assert await _ensure(database, project, instance) == "present"

    [row] = await _native_rows(database, project_id)
    assert row["type"] == "cloud_folder"
    assert row["created_by"] == UUID(owner["id"])
    assert row["scope_mode"] == "projects"
    assert row["auto_attach"] is True
    assert row["read_only"] is None
    assert row["is_global"] is False
    stored = await database.get_datasource(str(row["id"]))
    assert stored["config"] == {
        "folder_kind": "project",
        "project_id": project_id,
        "backend_id": "nextcloud",
        "backend_instance_id": instance,
        "folder_handle": "nextcloud:Atlas",
    }
    assert stored["credentials"] == {}
    # The folder's copy into its jobs is created and linked beside it.
    [copy] = await _native_rows(database, project_id, "project-cloud-checkout")
    assert copy["type"] == "cloud_folder_checkout"
    assert (copy["scope_mode"], copy["auto_attach"]) == ("projects", True)
    assert (await database.get_datasource(str(copy["id"])))["config"] == {
        "project_id": project_id
    }
    assert await _links(database, project_id) == {str(row["id"]), str(copy["id"])}
    link = await database.fetchrow(
        "SELECT read_only FROM project_datasources WHERE datasource_id=$1", row["id"]
    )
    assert link["read_only"] is None

    # Its Connector lives in the project, marked as SRW's, and the Project
    # manifest lists it.
    resource = await ManifestStore(database).by_id(str(row["id"]))
    assert (resource["scope_kind"], resource["scope_name"]) == ("Project", project_id)
    assert resource["platform_managed"] == f"project-cloud:{project_id}"
    assert resource["document"]["spec"]["driver"] == "srw.cloud-folder/v1"
    assert str(row["id"]) in await _listed(database, project_id)


@pytest.mark.asyncio
async def test_a_repaired_handle_repoints_the_row_and_its_resource(database):
    owner = await _user(database, "Repair owner")
    instance = await _instance(database)
    project = await _cloud_project(database, "Moved", owner, instance)
    await _ensure(database, project, instance)
    [row] = await _native_rows(database, str(project["id"]))

    await database.execute(
        "UPDATE projects SET main_cloud_folder_handle='nextcloud:moved-2' WHERE id=$1",
        project["id"],
    )
    moved = await database.get_project(str(project["id"]))
    assert await _ensure(database, moved, instance) == "repointed"
    assert await _ensure(database, moved, instance) == "present"

    stored = await database.get_datasource(str(row["id"]))
    assert stored["config"]["folder_handle"] == "nextcloud:moved-2"
    resource = await ManifestStore(database).by_id(str(row["id"]))
    assert resource["document"]["spec"]["config"]["folder_handle"] == (
        "nextcloud:moved-2"
    )
    assert len(await _native_rows(database, str(project["id"]))) == 1


@pytest.mark.asyncio
async def test_every_member_may_use_it_and_no_other_project(database):
    owner = await _user(database, "Owner")
    member = await _user(database, "Editor")
    outsider = await _user(database, "Outsider")
    instance = await _instance(database)
    project = await _cloud_project(
        database, "Shared", owner, instance, **{member["id"]: "editor"}
    )
    other = await _project(database, "Elsewhere", member)
    await _ensure(database, project, instance)
    [row] = await _native_rows(database, str(project["id"]))
    row_id = str(row["id"])

    eligible = {
        str(entry["id"]): entry
        for entry in await database.list_eligible_datasources(
            member["id"], [str(project["id"])]
        )
    }
    assert eligible[row_id]["default_selected"] is True
    assert eligible[row_id]["managed_key"] == f"project-cloud:{project['id']}"
    # A member may attach it in another project's work and projectless work
    # too (slice 5), never preselected there; an outsider nowhere.
    for projects in ([other], []):
        listed = {
            str(entry["id"]): entry
            for entry in await database.list_eligible_datasources(
                member["id"], projects
            )
        }
        assert listed[row_id]["default_selected"] is False
    for projects in ([other], [], [str(project["id"])]):
        assert row_id not in {
            str(entry["id"])
            for entry in await database.list_eligible_datasources(
                outsider["id"], projects
            )
        }

    selected, _revisions = await default_datasource_selection(
        database,
        member["id"],
        [str(project["id"])],
        "sandbox",
        execution_kind="session",
    )
    assert row_id in selected
    selected, _revisions = await default_datasource_selection(
        database, member["id"], [other], "sandbox", execution_kind="session"
    )
    assert row_id not in selected
    # A cloud folder mounts into sessions: a job never defaults to it.
    selected, _revisions = await default_datasource_selection(
        database, member["id"], [str(project["id"])], "sandbox", execution_kind="job"
    )
    assert row_id not in selected


@pytest.mark.asyncio
async def test_an_omitted_list_attaches_the_works_own_and_an_empty_one_nothing(
    database,
):
    owner = await _user(database, "Owner")
    member = await _user(database, "Member")
    instance = await _instance(database)
    project = await _cloud_project(
        database, "Omission", owner, instance, **{member["id"]: "editor"}
    )
    project_id = str(project["id"])
    await _ensure(database, project, instance)
    [cloud] = [str(row["id"]) for row in await _native_rows(database, project_id)]
    kb = await _knowledge_base(database, project_id, owner)
    # The member's own auto-attach connector: a default only when every
    # default is asked for.
    own = await _connector(database, "Mine", member, scope_mode="all", auto_attach=True)

    async def session(**fields):
        ids, _ = await _session_selection(database, member, project_id, **fields)
        return set(ids)

    async def job(**fields):
        ids, _ = await _job_selection(database, member, project_id, **fields)
        return set(ids)

    copy = await _copy_id(database, project_id)
    assert await session() == {kb, cloud}
    assert await session(datasource_ids=[]) == set()
    assert await session(use_datasource_defaults=True) == {kb, cloud, own}
    # A job's own: the knowledge base and the folder's copy (slice 6.3).
    assert await job() == {kb, copy}
    assert await job(datasource_ids=[]) == set()
    assert await job(use_datasource_defaults=True) == {kb, copy, own}


@pytest.mark.asyncio
async def test_deleting_the_project_removes_it_and_its_creator_only_detaches(database):
    owner = await _user(database, "Departing owner")
    successor = await _user(database, "Successor")
    instance = await _instance(database)
    project = await _cloud_project(
        database, "Handover", owner, instance, **{successor["id"]: "owner"}
    )
    await _ensure(database, project, instance)
    [row] = await _native_rows(database, str(project["id"]))

    assert await database.delete_user(owner["id"]) is True
    assert (
        await database.fetchval(
            "SELECT created_by FROM datasources WHERE id=$1", row["id"]
        )
        is None
    )
    assert await database.delete_project(str(project["id"])) is True
    assert await _native_rows(database, str(project["id"])) == []


@pytest.mark.asyncio
async def test_a_folder_error_is_stored_read_back_and_cleared(database):
    """Why a project has no folder reaches the project read (0548)."""
    owner = await _user(database, "Folderless owner")
    project = await _cloud_project(database, "Marketing", owner, None, handle=None)
    project_id = str(project["id"])
    assert project["main_cloud_folder_error"] is None

    await database.set_project_main_cloud_folder_error(project_id, "already_exists")
    assert (await database.get_project(project_id))[
        "main_cloud_folder_error"
    ] == "already_exists"

    await database.set_project_main_cloud_folder_error(project_id, None)
    assert (await database.get_project(project_id))["main_cloud_folder_error"] is None


@pytest.mark.asyncio
async def test_the_startup_pass_counts_and_a_second_pass_changes_nothing(
    database, caplog
):
    owner = await _user(database, "Backfill owner")
    active = await _instance(database)
    retired = await _instance(database)
    first = await _cloud_project(database, "One", owner, active)
    second = await _cloud_project(database, "Two", owner, active)
    elsewhere = await _cloud_project(database, "Elsewhere", owner, retired)
    unrouted = await _cloud_project(database, "Legacy", owner, None, handle=None)
    await database.execute(
        "UPDATE projects SET nextcloud_folder_id=7 WHERE id=$1", unrouted["id"]
    )
    # A default project's cloud is its owner's, never the project's own.
    default = await _cloud_project(database, "Personal", owner, active)
    await database.execute(
        "UPDATE projects SET is_default=TRUE WHERE id=$1", default["id"]
    )

    with caplog.at_level(logging.INFO, logger=project_cloud_connectors.__name__):
        counts = await project_cloud_connectors.backfill_project_cloud_connectors(
            database, active_instance_id=active
        )
        rerun = await project_cloud_connectors.backfill_project_cloud_connectors(
            database, active_instance_id=active
        )

    assert counts == {
        "created": 2,
        "present": 0,
        "repointed": 0,
        "skipped_default": 0,
        "skipped_inactive": 1,
        "skipped_unrouted": 1,
        "deferred": 0,
    }
    assert rerun == {**counts, "created": 0, "present": 2}
    assert f"Project cloud folder connectors: {rerun}" in caplog.text
    for project in (first, second):
        assert len(await _native_rows(database, str(project["id"]))) == 1
    for project in (elsewhere, unrouted, default):
        assert await _native_rows(database, str(project["id"])) == []


@pytest.mark.asyncio
async def test_a_users_own_connector_is_eligible_for_that_user_only(database):
    owner = await _user(database, "Root owner")
    admin = await _user(database, "Administrator", admin=True)
    project = await _project(database, "Personal", owner, **{admin["id"]: "editor"})
    instance = await _instance(database)
    row = await database.create_datasource(
        name="My cloud",
        ds_type="cloud_folder",
        connection_url=None,
        config={
            "folder_kind": "user_root",
            "backend_id": "nextcloud",
            "backend_instance_id": instance,
            "folder_handle": "nextcloud:home",
        },
        created_by=owner["id"],
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project],
        managed_key=f"user-cloud-root:{owner['id']}",
    )
    row_id = str(row["id"])

    async def eligible(user, *, is_admin=False):
        return {
            str(entry["id"])
            for entry in await database.list_eligible_datasources(
                user["id"], [project], is_admin=is_admin
            )
        }

    assert row_id in await eligible(owner)
    assert row_id not in await eligible(admin, is_admin=True)
    candidates = {
        str(entry["id"])
        for entry in await database.list_default_datasource_candidates(
            admin["id"], [project]
        )
    }
    assert row_id not in candidates
    selected, _revisions = await default_datasource_selection(
        database, owner["id"], [project], "sandbox", execution_kind="session"
    )
    assert row_id in selected

    # A second row under the same key is refused, not duplicated.
    assert (
        await database.create_datasource(
            name="My cloud again",
            ds_type="cloud_folder",
            config=row["config"],
            created_by=owner["id"],
            scope_mode="projects",
            project_ids=[project],
            managed_key=f"user-cloud-root:{owner['id']}",
        )
        is None
    )
