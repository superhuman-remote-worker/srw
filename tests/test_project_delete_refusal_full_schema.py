"""A refused project delete leaves the project whole.

The project delete removed the vault repo and the project's search index
rows and detached its jobs and datasources, and only then asked the database,
which refuses a project with a resumable session or unfinished work (409).
The refused project stayed, without its vault (k3d, 11-05). Now the refusal
is asked first, the index goes after the row, and the jobs and datasources
are detached in the delete's own transaction.

Runs the full schema on a real PostgreSQL.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.services import projects
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _project_with_history(database, name):
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
        "VALUES('notes','postgresql',$1) RETURNING id",
        project["id"],
    )
    vault = f"project-{project_id[:8]}-knowledge"
    await database.execute(
        "INSERT INTO project_repositories(project_id,name,repo_url,is_managed) "
        "VALUES($1,$2,'https://git.invalid/vault.git',TRUE)",
        project["id"],
        vault,
    )
    return SimpleNamespace(
        user=user,
        id=project_id,
        job_id=job_id,
        datasource_id=datasource_id,
        vault=vault,
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


def _dependencies(database, calls, monkeypatch):
    """The real store; every cleanup outside it records itself in ``calls``."""

    async def delete_repo(_store, _forge, name):
        calls.append(f"repo:{name}")
        return True

    async def revoke_grants(*_args, **_kwargs):
        calls.append("grants")
        return 0

    monkeypatch.setattr(projects, "revoke_and_delete_managed_repository", delete_repo)
    monkeypatch.setattr(projects.folder_grants, "revoke_folder_grants", revoke_grants)

    @asynccontextmanager
    async def vector_acquire():
        async def execute(*_args):
            calls.append("index")

        yield SimpleNamespace(execute=execute)

    graph = MagicMock()
    graph.delete_project_knowledge.side_effect = lambda _pid: calls.append("graph")
    return projects.ProjectDependencies(
        store=database,
        vector_db=SimpleNamespace(acquire=vector_acquire),
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None)
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


@pytest.mark.asyncio
async def test_a_refused_delete_removes_nothing(database, monkeypatch):
    project = await _project_with_history(database, "Refused")
    await _resumable_session(database, project)
    calls: list[str] = []

    with pytest.raises(HTTPException) as refused:
        await projects.delete_project(
            project.id,
            project=await database.get_project(project.id),
            dependencies=_dependencies(database, calls, monkeypatch),
        )

    assert refused.value.status_code == 409
    assert calls == []  # no vault repo, grants, index or graph delete
    await _still_attached(database, project)


@pytest.mark.asyncio
async def test_work_admitted_after_the_check_keeps_the_database_whole(
    database, monkeypatch
):
    """The check and the delete are two transactions. A session started in
    between is refused by the delete itself, which then changes nothing in
    the database. The vault repo is gone by then: its delete needs the row
    for a retry, so it cannot wait for the row."""
    project = await _project_with_history(database, "Late session")
    await _resumable_session(database, project)
    monkeypatch.setattr(database, "assert_project_deletable", AsyncMock())
    calls: list[str] = []

    with pytest.raises(HTTPException) as refused:
        await projects.delete_project(
            project.id,
            project=await database.get_project(project.id),
            dependencies=_dependencies(database, calls, monkeypatch),
        )

    assert refused.value.status_code == 409
    assert calls == [f"repo:{project.vault}", "grants"]
    await _still_attached(database, project)  # the repo delete is a fake


@pytest.mark.asyncio
async def test_a_delete_checks_first_and_clears_the_index_after_the_row(
    database, monkeypatch
):
    project = await _project_with_history(database, "Deleted")
    calls: list[str] = []
    dependencies = _dependencies(database, calls, monkeypatch)
    check, delete = database.assert_project_deletable, database.delete_project

    async def recorded_check(project_id):
        await check(project_id)
        calls.append("check")

    async def recorded_delete(project_id):
        deleted = await delete(project_id)
        calls.append("row")
        return deleted

    monkeypatch.setattr(database, "assert_project_deletable", recorded_check)
    monkeypatch.setattr(database, "delete_project", recorded_delete)

    assert await projects.delete_project(
        project.id,
        project=await database.get_project(project.id),
        dependencies=dependencies,
    ) == {"status": "deleted"}

    assert calls == [
        "check",
        f"repo:{project.vault}",
        "grants",
        "row",
        "index",
        "graph",
    ]
    assert await database.get_project(project.id) is None
    # The job and the datasource outlive the project, detached.
    assert await database.fetchval(
        "SELECT project_id IS NULL FROM jobs WHERE id=$1", project.job_id
    )
    assert await database.fetchval(
        "SELECT project_id IS NULL FROM datasources WHERE id=$1", project.datasource_id
    )
