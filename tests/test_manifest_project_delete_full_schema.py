"""The manifest API deletes a linked Project through the project delete.

``DELETE /api/resources/{id}`` on a Project ran only the database delete: a
500 when the vault was fenced by the repository ledger, otherwise a 200 that
left the vault, the Keycloak group, the cloud folder and the search index
behind. It now hands the project to the project delete, which compares the
resource version in its decision transaction, before anything outside the
database is touched.

Runs the full schema on a real PostgreSQL.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.services import project_deletion
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.project_status import PROJECT_DELETING_DETAIL
from tests import test_manifest_native_full_schema as full_schema
from tests._project_delete_stubs import delete_project_hook

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _owned_project(database, name):
    user, default = await database.create_user_with_default_project(name)
    await database.execute(
        "UPDATE users SET is_approved=TRUE,is_admin=FALSE WHERE id=$1", user["id"]
    )
    user = dict(await database.fetchrow("SELECT * FROM users WHERE id=$1", user["id"]))
    project = await database.create_project(f"{name} project")
    project_id = str(project["id"])
    await database.add_project_member(project_id, str(user["id"]), "owner")
    vault = f"project-{project_id[:8]}-knowledge"
    await database.execute(
        "INSERT INTO project_repositories(project_id,name,repo_url,is_managed) "
        "VALUES($1,$2,'https://git.invalid/vault.git',TRUE)",
        project["id"],
        vault,
    )
    resource = await database.fetchrow(
        "SELECT id,resource_version FROM srw_resources "
        "WHERE kind='Project' AND linked_id=$1 AND deleted_at IS NULL",
        project["id"],
    )
    return SimpleNamespace(
        user=user,
        default_id=str(default["id"]),
        id=project_id,
        vault=vault,
        uid=str(resource["id"]),
        version=resource["resource_version"],
    )


@pytest.fixture
def repo_calls(monkeypatch):
    calls = SimpleNamespace(names=[], ok=True)

    async def delete_repo(_store, _forge, name):
        calls.names.append(name)
        return calls.ok

    async def revoke_grants(*_args, **_kwargs):
        return 0

    monkeypatch.setattr(
        project_deletion, "revoke_and_delete_managed_repository", delete_repo
    )
    monkeypatch.setattr(
        project_deletion.folder_grants, "revoke_folder_grants", revoke_grants
    )
    return calls


def _service(database):
    return ManifestResourceService(
        database, delete_project=delete_project_hook(database)
    )


@pytest.mark.asyncio
async def test_a_project_delete_runs_the_project_delete(database, repo_calls):
    project = await _owned_project(database, "Routed")

    result = await _service(database).delete(
        project.uid, project.user, expected_version=project.version
    )

    assert result == {"deleted": True, "uid": project.uid}
    assert repo_calls.names == [project.vault]
    assert await database.get_project(project.id) is None
    deletion = await database.get_project_deletion(project.id)
    assert deletion["requested_by"] == project.user["id"]
    assert deletion["completed_at"] is not None


@pytest.mark.asyncio
async def test_a_stale_version_is_refused_before_anything_outside(database, repo_calls):
    project = await _owned_project(database, "Stale")

    with pytest.raises(HTTPException) as refused:
        await _service(database).delete(
            project.uid, project.user, expected_version=project.version + 1
        )

    assert refused.value.status_code == 409
    assert repo_calls.names == []
    assert (await database.get_project(project.id))["deletion_requested_at"] is None


@pytest.mark.asyncio
async def test_a_default_project_is_refused_like_the_rest_delete(database, repo_calls):
    project = await _owned_project(database, "Default")
    uid, version = await database.fetchrow(
        "SELECT id,resource_version FROM srw_resources "
        "WHERE kind='Project' AND linked_id=$1 AND deleted_at IS NULL",
        UUID(project.default_id),
    )

    with pytest.raises(HTTPException) as refused:
        await _service(database).delete(
            str(uid), project.user, expected_version=version
        )

    assert refused.value.status_code == 400  # was a foreign-key 500
    assert await database.get_project(project.default_id) is not None


@pytest.mark.asyncio
async def test_without_the_project_delete_a_project_is_not_deleted(
    database, repo_calls
):
    project = await _owned_project(database, "No hook")

    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(database).delete(
            project.uid, project.user, expected_version=project.version
        )

    assert refused.value.status_code == 503
    assert await database.get_project(project.id) is not None


@pytest.mark.asyncio
async def test_an_unfinished_delete_says_so_and_a_later_delete_finishes_it(
    database, repo_calls
):
    project = await _owned_project(database, "Gitea down")
    repo_calls.ok = False
    service = _service(database)

    result = await service.delete(
        project.uid, project.user, expected_version=project.version
    )

    assert result["deleted"] is False
    assert result["deleting"] is True
    assert result["uid"] == project.uid
    assert "retried in the background" in result["detail"]

    # The delete itself may be sent again; inside the backoff it drives
    # nothing and says so.
    current = await service.get(project.uid, project.user)
    assert current["resourceVersion"] == project.version
    again = await service.delete(
        project.uid, project.user, expected_version=current["resourceVersion"]
    )
    assert again["deleting"] is True
    assert repo_calls.names == [project.vault]

    repo_calls.ok = True
    await database.execute(
        "UPDATE project_deletions SET next_attempt_at=now() WHERE project_id=$1",
        UUID(project.id),
    )
    result = await service.delete(
        project.uid, project.user, expected_version=current["resourceVersion"]
    )
    assert result == {"deleted": True, "uid": project.uid}
    assert await database.get_project(project.id) is None


@pytest.mark.asyncio
async def test_a_project_being_deleted_refuses_manifest_writes(database, repo_calls):
    from orchestrator.services.manifest_authority import ManifestAuthority

    project = await _owned_project(database, "Being deleted")
    repo_calls.ok = False
    await _service(database).delete(
        project.uid, project.user, expected_version=project.version
    )

    authority = ManifestAuthority(database, project.user)
    scope = {"kind": "Project", "name": project.id}
    with pytest.raises(HTTPException) as refused:
        await authority.scope(scope, write=True)
    assert refused.value.status_code == 409
    assert refused.value.detail == PROJECT_DELETING_DETAIL
    # Reading stays possible; only the delete may write.
    assert await authority.scope(scope) == scope
    assert await authority.scope(scope, write=True, allow_deleting=True) == scope
