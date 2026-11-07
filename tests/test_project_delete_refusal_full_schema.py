"""A project delete is decided once, then finished; a refusal changes nothing.

The project delete removed the vault repo and the project's search index
rows and detached its jobs and datasources, and only then asked the database,
which refuses a project with a resumable session or unfinished work (409).
The refused project stayed, without its vault (k3d, 11-05). Then the refusal
was asked first, but work admitted between that check and the row delete
still made the row delete refuse after the vault was gone, and whatever a
request did not finish after the row (Keycloak group, cloud folder, index)
stayed behind.

Now the delete is decided in one transaction (refusal checks, the Project
resource version, the mark), the project refuses new work from then on, and
a ``project_deletions`` row records what is left until a pass removes it.

Runs the full schema on a real PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi import HTTPException

from orchestrator.services import project_deletion, projects
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _project_with_history(database, name, *, folder=False):
    """A project with an owner, a finished job, a datasource and a vault repo."""
    user, _default = await database.create_user_with_default_project(name)
    await database.execute(
        "UPDATE users SET is_approved=TRUE,is_admin=TRUE WHERE id=$1", user["id"]
    )
    project = await database.create_project(f"{name} project")
    project_id = str(project["id"])
    await database.add_project_member(project_id, str(user["id"]), "owner")
    job_id = await database.fetchval(
        "INSERT INTO jobs(description,project_id,status) "
        "VALUES('finished work',$1,'completed') RETURNING id",
        project["id"],
    )
    datasource_id = await database.fetchval(
        "INSERT INTO datasources(name,type,project_id) "
        "VALUES($2,'postgresql',$1) RETURNING id",
        project["id"],
        f"{name} notes",
    )
    vault = f"project-{project_id[:8]}-knowledge"
    await database.execute(
        "INSERT INTO project_repositories(project_id,name,repo_url,is_managed) "
        "VALUES($1,$2,'https://git.invalid/vault.git',TRUE)",
        project["id"],
        vault,
    )
    if folder:
        instance_id = uuid4()
        await database.execute(
            "INSERT INTO main_cloud_backend_instances "
            "(id,backend_id,routing,routing_sha256,installation_proof_sha256,"
            "secret_refs) VALUES ($1,'nextcloud','{}'::jsonb,$2,$3,'{}'::jsonb)",
            instance_id,
            uuid4().hex * 2,
            uuid4().hex * 2,
        )
        await database.execute(
            "UPDATE projects SET main_cloud_backend='nextcloud',"
            "main_cloud_backend_instance_id=$2,"
            "main_cloud_folder_handle='355' WHERE id=$1",
            project["id"],
            instance_id,
        )
    resource = await database.fetchrow(
        "SELECT id,resource_version FROM srw_resources "
        "WHERE kind='Project' AND linked_id=$1 AND deleted_at IS NULL",
        project["id"],
    )
    return SimpleNamespace(
        user=user,
        id=project_id,
        job_id=job_id,
        datasource_id=datasource_id,
        vault=vault,
        resource_id=str(resource["id"]),
        resource_version=resource["resource_version"],
    )


async def _resumable_session(database, project):
    """An ended session whose owner still exists: it can be resumed."""
    thread_id = await database.create_thread(
        user_id=str(project.user["id"]),
        project_id=project.id,
        authority_user_id=str(project.user["id"]),
        authority_project_ids=[project.id],
        execution_lane="pinned",
        initial_metadata={"config_override": {"workspace": {"backend": "none"}}},
    )
    await database.execute(
        "UPDATE threads SET status='ended' WHERE id=$1::uuid", thread_id
    )


class Outside:
    """Every cleanup outside the store, recording itself in ``calls``.

    ``repo_ok``, ``group_ok`` and ``folder_ok`` make a step fail until set.
    """

    def __init__(self, monkeypatch):
        self.calls: list[str] = []
        self.repo_ok = True
        self.group_ok = True
        self.folder_ok = True
        self.repo_gate: asyncio.Event | None = None
        monkeypatch.setattr(
            project_deletion, "revoke_and_delete_managed_repository", self._repo
        )
        monkeypatch.setattr(
            project_deletion.folder_grants, "revoke_folder_grants", self._grants
        )

    async def _repo(self, _store, _forge, name):
        if self.repo_gate is not None:
            await self.repo_gate.wait()
        self.calls.append(f"repo:{name}" if self.repo_ok else f"repo-failed:{name}")
        return self.repo_ok

    async def _grants(self, *_args, **_kwargs):
        self.calls.append("grants")
        return 0

    def dependencies(self, store):
        calls = self

        @asynccontextmanager
        async def vector_acquire():
            async def execute(sql, *_args):
                calls.calls.append(
                    "watermark" if "kb_index_watermark" in sql else "index"
                )

            yield SimpleNamespace(execute=execute)

        async def delete_group(_project_id):
            calls.calls.append("group" if calls.group_ok else "group-failed")
            return calls.group_ok

        async def delete_folder(handle):
            if not calls.folder_ok:
                calls.calls.append("folder-failed")
                raise RuntimeError("cloud down")
            calls.calls.append(f"folder:{handle.native_id}")

        graph = MagicMock()
        graph.delete_project_knowledge.side_effect = lambda _pid: calls.calls.append(
            "graph"
        )
        backend = SimpleNamespace(
            is_initialized=True,
            backend_id="nextcloud",
            delete_project_folder=delete_folder,
        )
        return projects.ProjectDependencies(
            store=store,
            vector_db=SimpleNamespace(acquire=vector_acquire),
            forge=SimpleNamespace(is_initialized=False),
            keycloak_groups=SimpleNamespace(
                is_configured=True,
                is_initialized=True,
                delete_project_group=delete_group,
            ),
            main_cloud_router=SimpleNamespace(
                for_project=MagicMock(return_value=backend),
            ),
            logger=MagicMock(),
            provisioning=MagicMock(),
            graph=SimpleNamespace(get=lambda: graph),
            with_validated_tool_overrides=lambda value: value,
        )


async def _still_attached(database, project):
    assert await database.get_project(project.id) is not None
    assert [r["name"] for r in await database.get_project_repositories(project.id)] == [
        project.vault
    ]
    assert await database.fetchval(
        "SELECT project_id FROM jobs WHERE id=$1", project.job_id
    ) == UUID(project.id)
    assert await database.fetchval(
        "SELECT project_id FROM datasources WHERE id=$1", project.datasource_id
    ) == UUID(project.id)


async def _deletion(database, project):
    row = await database.fetchrow(
        "SELECT * FROM project_deletions WHERE project_id=$1", UUID(project.id)
    )
    return dict(row) if row else None


async def _make_due(database, project):
    await database.execute(
        "UPDATE project_deletions SET next_attempt_at=now()-interval '1 second' "
        "WHERE project_id=$1",
        UUID(project.id),
    )


async def _delete(database, project, outside, **kwargs):
    return await projects.delete_project(
        project.id,
        project=await database.get_project(project.id),
        dependencies=outside.dependencies(database),
        **kwargs,
    )


# =============================================================================
# The decision
# =============================================================================


@pytest.mark.asyncio
async def test_a_refused_delete_removes_nothing(database, monkeypatch):
    project = await _project_with_history(database, "Refused")
    await _resumable_session(database, project)
    outside = Outside(monkeypatch)

    with pytest.raises(HTTPException) as refused:
        await _delete(database, project, outside)

    assert refused.value.status_code == 409
    assert outside.calls == []  # no vault repo, grants, index or graph delete
    await _still_attached(database, project)
    assert (await database.get_project(project.id))["deletion_requested_at"] is None
    assert await _deletion(database, project) is None


@pytest.mark.asyncio
async def test_a_stale_resource_version_refuses_before_anything_outside(
    database, monkeypatch
):
    project = await _project_with_history(database, "Stale")
    outside = Outside(monkeypatch)

    with pytest.raises(HTTPException) as refused:
        await _delete(
            database,
            project,
            outside,
            expected_resource=(project.resource_id, project.resource_version - 1),
        )

    assert refused.value.status_code == 409
    assert "Resource version changed" in refused.value.detail
    assert outside.calls == []
    await _still_attached(database, project)
    assert (
        await database.fetchval(
            "SELECT resource_version FROM srw_resources WHERE id=$1",
            UUID(project.resource_id),
        )
        == project.resource_version
    )
    assert await _deletion(database, project) is None


@pytest.mark.asyncio
async def test_a_default_project_is_refused_by_the_decision(database):
    user, default = await database.create_user_with_default_project("Default owner")
    with pytest.raises(HTTPException) as refused:
        await database.begin_project_deletion(str(default["id"]))
    assert refused.value.status_code == 400
    assert await database.get_project(str(default["id"])) is not None


# =============================================================================
# The teardown
# =============================================================================


@pytest.mark.asyncio
async def test_a_delete_decides_first_and_records_until_every_step_is_done(
    database, monkeypatch
):
    project = await _project_with_history(database, "Deleted", folder=True)
    outside = Outside(monkeypatch)
    begin, delete = database.begin_project_deletion, database.delete_project

    async def recorded_begin(project_id, **kwargs):
        deletion = await begin(project_id, **kwargs)
        outside.calls.append("decide")
        return deletion

    async def recorded_delete(project_id):
        deleted = await delete(project_id)
        outside.calls.append("row")
        return deleted

    monkeypatch.setattr(database, "begin_project_deletion", recorded_begin)
    monkeypatch.setattr(database, "delete_project", recorded_delete)

    assert await _delete(
        database,
        project,
        outside,
        requested_by=str(project.user["id"]),
        expected_resource=(project.resource_id, project.resource_version),
    ) == {"status": "deleted"}

    assert outside.calls == [
        "decide",
        f"repo:{project.vault}",
        "grants",
        "row",
        "graph",
        "index",
        "watermark",
        "group",
        "folder:355",
    ]
    assert await database.get_project(project.id) is None
    # The job and the datasource outlive the project, detached.
    assert await database.fetchval(
        "SELECT project_id IS NULL FROM jobs WHERE id=$1", project.job_id
    )
    assert await database.fetchval(
        "SELECT project_id IS NULL FROM datasources WHERE id=$1", project.datasource_id
    )
    assert await database.fetchval(
        "SELECT deleted_at IS NOT NULL FROM srw_resources WHERE id=$1",
        UUID(project.resource_id),
    )
    deletion = await _deletion(database, project)
    assert deletion["requested_by"] == project.user["id"]
    assert deletion["row_deleted_at"] is not None
    assert deletion["completed_at"] is not None
    assert deletion["pending_steps"] == []
    assert deletion["last_error"] is None
    targets = deletion["targets"]
    if isinstance(targets, str):
        targets = json.loads(targets)
    assert targets["cloud"]["main_cloud_folder_handle"] == "355"


@pytest.mark.asyncio
async def test_a_step_after_the_row_that_fails_is_retried_until_done(
    database, monkeypatch
):
    project = await _project_with_history(database, "Group down", folder=True)
    outside = Outside(monkeypatch)
    outside.group_ok = False
    outside.folder_ok = False

    assert (await _delete(database, project, outside))["status"] == "deleted"
    deletion = await _deletion(database, project)
    assert deletion["completed_at"] is None
    assert deletion["pending_steps"] == ["keycloak_group", "cloud_folder"]
    assert "keycloak_group" in deletion["last_error"]
    assert deletion["next_attempt_at"] > deletion["row_deleted_at"]

    # Not due yet: a sweep leaves it alone.
    dependencies = outside.dependencies(database)
    assert (
        await project_deletion.sweep_project_deletions(dependencies=dependencies) == 0
    )

    outside.group_ok = True
    outside.folder_ok = True
    outside.calls.clear()
    await _make_due(database, project)
    assert (
        await project_deletion.sweep_project_deletions(dependencies=dependencies) == 1
    )

    assert outside.calls == ["group", "folder:355"]
    deletion = await _deletion(database, project)
    assert deletion["completed_at"] is not None
    assert deletion["pending_steps"] == []
    assert deletion["last_error"] is None


@pytest.mark.asyncio
async def test_a_repository_step_that_fails_leaves_a_marked_project_the_sweep_finishes(
    database, monkeypatch
):
    project = await _project_with_history(database, "Gitea down")
    outside = Outside(monkeypatch)
    outside.repo_ok = False

    result = await _delete(database, project, outside)

    assert result["status"] == "deleting"
    assert "Repository credential revocation is retryable" in result["detail"]
    assert outside.calls == [f"repo-failed:{project.vault}"]
    await _still_attached(database, project)
    assert (await database.get_project(project.id))["deletion_requested_at"]

    # A second DELETE while the backoff runs drives nothing and says so.
    outside.calls.clear()
    assert (await _delete(database, project, outside))["status"] == "deleting"
    assert outside.calls == []

    outside.repo_ok = True
    await _make_due(database, project)
    assert (
        await project_deletion.sweep_project_deletions(
            dependencies=outside.dependencies(database)
        )
        == 1
    )
    assert await database.get_project(project.id) is None
    assert (await _deletion(database, project))["completed_at"] is not None


@pytest.mark.asyncio
async def test_a_request_that_died_after_the_decision_is_finished_by_the_sweep(
    database, monkeypatch
):
    project = await _project_with_history(database, "Died")
    outside = Outside(monkeypatch)
    await database.begin_project_deletion(project.id)  # the request stops here

    # Its lease keeps the sweep off until it runs out.
    assert (
        await project_deletion.sweep_project_deletions(
            dependencies=outside.dependencies(database)
        )
        == 0
    )
    await _make_due(database, project)
    assert (
        await project_deletion.sweep_project_deletions(
            dependencies=outside.dependencies(database)
        )
        == 1
    )

    assert await database.get_project(project.id) is None
    assert outside.calls[:3] == [f"repo:{project.vault}", "grants", "graph"]


# =============================================================================
# The fence
# =============================================================================


@pytest.mark.asyncio
async def test_a_marked_project_refuses_new_work_and_resource_changes(
    database, monkeypatch
):
    project = await _project_with_history(database, "Fenced")
    outside = Outside(monkeypatch)
    outside.repo_ok = False
    assert (await _delete(database, project, outside))["status"] == "deleting"

    def refused(caught):
        assert caught.value.constraint_name == "project_deletion_in_progress"

    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "INSERT INTO jobs(description,project_id,status) VALUES('late',$1,'created')",
            UUID(project.id),
        )
    refused(caught)
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await _resumable_session(database, project)
    refused(caught)
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "INSERT INTO project_repositories(project_id,name,repo_url,is_managed) "
            "VALUES($1,'late','https://git.invalid/late.git',TRUE)",
            UUID(project.id),
        )
    refused(caught)
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "UPDATE srw_resources SET resource_version=resource_version+1 WHERE id=$1",
            UUID(project.resource_id),
        )
    refused(caught)
    elsewhere = await database.fetchval(
        "INSERT INTO jobs(description,status) VALUES('elsewhere','created') RETURNING id"
    )
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "UPDATE jobs SET project_id=$1 WHERE id=$2", UUID(project.id), elsewhere
        )
    refused(caught)
    # Resume, assign and VM-creation retry take a failed job back.
    await database.execute(
        "UPDATE jobs SET status='failed' WHERE id=$1", project.job_id
    )
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await database.execute(
            "UPDATE jobs SET status='paused' WHERE id=$1", project.job_id
        )
    refused(caught)

    # The row delete itself still runs: it clears project_id and retires.
    outside.repo_ok = True
    await _make_due(database, project)
    await project_deletion.sweep_project_deletions(
        dependencies=outside.dependencies(database)
    )
    assert await database.get_project(project.id) is None


@pytest.mark.asyncio
async def test_a_writer_racing_the_decision_waits_for_it_and_is_refused(database):
    """The decision holds the project row; a job insert waits for it, then
    reads the committed mark."""
    project = await _project_with_history(database, "Race")
    decider = await database.pool.acquire()
    writer = await database.pool.acquire()
    try:
        transaction = decider.transaction()
        await transaction.start()
        await project_deletion.begin_project_deletion(decider, UUID(project.id))

        insert = asyncio.create_task(
            writer.execute(
                "INSERT INTO jobs(description,project_id,status) "
                "VALUES('racing',$1,'created')",
                UUID(project.id),
            )
        )
        await asyncio.sleep(0.5)
        assert not insert.done()  # waiting on the decision's row lock

        await transaction.commit()
        with pytest.raises(asyncpg.CheckViolationError) as caught:
            await insert
        assert caught.value.constraint_name == "project_deletion_in_progress"
    finally:
        await database.pool.release(decider)
        await database.pool.release(writer)


@pytest.mark.asyncio
async def test_a_refused_decision_releases_the_writer(database):
    project = await _project_with_history(database, "Rolled back")
    decider = await database.pool.acquire()
    writer = await database.pool.acquire()
    try:
        transaction = decider.transaction()
        await transaction.start()
        await project_deletion.begin_project_deletion(decider, UUID(project.id))
        insert = asyncio.create_task(
            writer.fetchval(
                "INSERT INTO jobs(description,project_id,status) "
                "VALUES('after a rollback',$1,'created') RETURNING id",
                UUID(project.id),
            )
        )
        await asyncio.sleep(0.5)
        await transaction.rollback()
        assert await insert is not None
    finally:
        await database.pool.release(decider)
        await database.pool.release(writer)
    assert (await database.get_project(project.id))["deletion_requested_at"] is None


@pytest.mark.asyncio
async def test_the_teardown_never_holds_the_catalog_lock(database, monkeypatch):
    """Gitea, Keycloak and the cloud are called after the decision commits:
    an unrelated catalog writer goes ahead while a repository delete hangs."""
    project = await _project_with_history(database, "Slow Gitea")
    outside = Outside(monkeypatch)
    outside.repo_gate = asyncio.Event()

    delete = asyncio.create_task(_delete(database, project, outside))
    while await _deletion(database, project) is None:
        await asyncio.sleep(0.05)

    async with database.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL lock_timeout = '2s'")
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended('srw-resource-catalog', 0))"
            )

    outside.repo_gate.set()
    assert (await delete) == {"status": "deleted"}


@pytest.mark.asyncio
async def test_the_listings_leave_out_a_project_being_deleted(database, monkeypatch):
    kept = await _project_with_history(database, "Kept")
    going = await _project_with_history(database, "Going")
    outside = Outside(monkeypatch)
    outside.repo_ok = False
    assert (await _delete(database, going, outside))["status"] == "deleting"

    admin = {**going.user, "is_admin": True, "is_approved": True}
    listed = await projects.list_projects(
        caller=admin,
        user_id=None,
        status=None,
        dependencies=outside.dependencies(database),
    )
    ids = {str(row["id"]) for row in listed}
    assert kept.id in ids
    assert going.id not in ids

    mine = await database.get_projects_for_user(
        str(going.user["id"]), statuses=["active"]
    )
    assert going.id not in {str(row["id"]) for row in mine}
