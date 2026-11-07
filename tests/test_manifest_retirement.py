"""Project removal and read authority use the complete production schema."""

from copy import deepcopy
import json
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import resource_key
from tests import test_manifest_native_full_schema as full_schema
from tests._project_delete_stubs import delete_project_hook

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


def project_document():
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "retirement-team"},
        "spec": {
            "resources": {
                "experts": {
                    "worker": {
                        "inline": {"runtime": {"image": "test.invalid/plain:v1"}}
                    }
                }
            },
            "defaults": {"expert": "worker"},
        },
    }


async def apply(service, document, user, **kwargs):
    return await service.apply(json.dumps(document), user, format="json", **kwargs)


def project_row(result):
    return next(
        row for row in result["resources"] if row["resource"]["kind"] == "Project"
    )


def versions(result):
    row = project_row(result)
    return {resource_key(row["resource"]): row["resourceVersion"]}


@pytest.mark.asyncio
async def test_project_removal_retires_child_and_preserves_revision_history(
    database, actor
):
    service = ManifestResourceService(database)
    document = project_document()
    first = await apply(service, document, actor)
    child = next(
        row for row in first["resources"] if row["resource"]["kind"] == "Expert"
    )
    removed = deepcopy(document)
    removed["spec"] = {"resources": {}}
    second = await apply(service, removed, actor, expected_versions=versions(first))
    assert project_row(second)["resourceVersion"] == 2
    with pytest.raises(HTTPException) as caught:
        await service.get(child["uid"], actor)
    assert caught.value.status_code == 404
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_resource_revisions WHERE resource_id=$1",
            UUID(child["uid"]),
        )
        == 1
    )
    # Reusing the old alias creates a new identity; it cannot resurrect a retired one.
    recreated = await apply(service, child["resource"], actor)
    assert recreated["resources"][0]["uid"] != child["uid"]


@pytest.mark.asyncio
@pytest.mark.parametrize("whole_project", [False, True])
async def test_external_user_default_blocks_project_retirement_atomically(
    database, actor, whole_project
):
    service = ManifestResourceService(
        database, delete_project=delete_project_hook(database)
    )
    document = project_document()
    first = await apply(service, document, actor)
    parent = project_row(first)
    child = next(
        row for row in first["resources"] if row["resource"]["kind"] == "Expert"
    )
    expert_id = await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(child["uid"])
    )
    await database.execute(
        "INSERT INTO user_expert_defaults(user_id,expert_type,expert_id) VALUES($1,'worker',$2)",
        actor["id"],
        expert_id,
    )
    with pytest.raises(HTTPException) as caught:
        if whole_project:
            await service.delete(parent["uid"], actor, expected_version=1)
        else:
            removed = deepcopy(document)
            removed["spec"] = {"resources": {}}
            await apply(service, removed, actor, expected_versions=versions(first))
    assert caught.value.status_code == 409
    assert (await service.get(parent["uid"], actor))["resourceVersion"] == 1
    assert (await service.get(child["uid"], actor))["resourceVersion"] == 1
    await database.execute(
        "DELETE FROM user_expert_defaults WHERE user_id=$1", actor["id"]
    )
    await service.delete(parent["uid"], actor, expected_version=1)
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_resources WHERE deleted_at IS NULL"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_account_list_uses_current_project_membership(database, actor):
    service = ManifestResourceService(database)
    member = {**actor, "is_admin": False}
    first = await apply(service, project_document(), member)
    parent = project_row(first)
    assert len((await service.list(member))["resources"]) == 1
    await database.execute("DELETE FROM project_members WHERE user_id=$1", actor["id"])
    assert (await service.list(member))["resources"] == []
    with pytest.raises(HTTPException) as caught:
        await service.get(parent["uid"], member)
    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_admin_creates_project_for_the_scoped_account_owner(database, actor):
    owner = dict(
        await database.fetchrow(
            "INSERT INTO users(display_name,is_approved) VALUES('Target account',TRUE) RETURNING *"
        )
    )
    service = ManifestResourceService(database)
    document = project_document()
    document["metadata"]["scope"] = {"kind": "Account", "name": str(owner["id"])}
    first = await apply(service, document, actor)
    parent = project_row(first)
    project_id = await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(parent["uid"])
    )
    assert (
        await database.get_user_role_in_project(str(project_id), str(owner["id"]))
        == "owner"
    )
    assert (await service.get(parent["uid"], owner))["uid"] == parent["uid"]
