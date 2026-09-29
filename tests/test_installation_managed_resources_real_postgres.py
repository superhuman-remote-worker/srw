"""Installation-managed resources are read-only through the API."""

import json

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import (
    INSTALLATION_MANAGED_MESSAGE,
    ManifestStore,
    resource_view,
)
from shared.manifests import preview_documents
from shared.manifests.resolution import content_revision
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

CATALOG = {"kind": "Catalog", "name": "shared"}
KIND = "WorkspaceTemplate"


def template(name: str, scope: dict, **spec) -> dict:
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": KIND,
        "metadata": {"name": name, "scope": scope},
        "spec": {"backend": "sandbox", **spec},
    }


async def save(database, document, *, dependencies=(), **options):
    resolved = preview_documents([document])["resolved"][0]
    async with database.transaction_scope():
        store = ManifestStore(database)
        await store.lock_catalog()
        await store.lock_identity(document)
        return await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            list(dependencies),
            **options,
        )


async def builtin(database, name="container-full", **spec):
    row, _ = await save(
        database,
        template(name, CATALOG, **spec),
        owner_id=None,
        installation_managed=True,
    )
    return row


def key(document) -> str:
    scope = document["metadata"]["scope"]
    return f"{KIND}/{scope['kind']}/{scope['name']}/{document['metadata']['name']}"


@pytest.mark.asyncio
async def test_the_marker_is_stored_and_shown(database, actor):
    marked = await builtin(database)
    assert marked["installation_managed"] is True
    assert marked["owner_id"] is None
    assert resource_view(marked)["installationManaged"] is True

    plain, _ = await save(
        database, template("team", CATALOG), owner_id=str(actor["id"])
    )
    assert plain["installation_managed"] is False
    assert resource_view(plain)["installationManaged"] is False


@pytest.mark.asyncio
async def test_an_admin_cannot_edit_a_marked_row(database, actor):
    assert actor["is_admin"] is True
    marked = await builtin(database)
    changed = template("container-full", CATALOG, resources={"cpu": 8})
    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(database).apply(
            json.dumps(changed),
            actor,
            format="json",
            expected_versions={key(changed): marked["resource_version"]},
        )
    assert refused.value.status_code == 409
    assert refused.value.detail == INSTALLATION_MANAGED_MESSAGE
    assert INSTALLATION_MANAGED_MESSAGE == (
        "This template is managed by the installation. Duplicate it to change it."
    )
    current = await ManifestStore(database).by_id(marked["id"])
    assert current["resource_version"] == marked["resource_version"]
    assert current["revision"] == marked["revision"]


@pytest.mark.asyncio
async def test_applying_even_identical_content_is_refused(database, actor):
    # One rule for every API write keeps the answer independent of how the
    # live resolver spells a resolved spec.
    marked = await builtin(database)
    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(database).apply(
            json.dumps(template("container-full", CATALOG)), actor, format="json"
        )
    assert refused.value.status_code == 409
    assert refused.value.detail == INSTALLATION_MANAGED_MESSAGE
    current = await ManifestStore(database).by_id(marked["id"])
    assert current["resource_version"] == marked["resource_version"]


@pytest.mark.asyncio
async def test_the_installation_may_rewrite_its_own_row(database, actor):
    marked = await builtin(database)
    same, changed = await save(
        database,
        template("container-full", CATALOG),
        owner_id=None,
        installation_managed=True,
        expected_version=marked["resource_version"],
    )
    assert changed is False and same["resource_version"] == 1
    updated, changed = await save(
        database,
        template("container-full", CATALOG, resources={"cpu": 8}),
        owner_id=None,
        installation_managed=True,
        expected_version=marked["resource_version"],
    )
    assert changed is True
    assert updated["id"] == marked["id"]
    assert updated["resource_version"] == 2
    assert updated["installation_managed"] is True


@pytest.mark.asyncio
async def test_an_admin_cannot_delete_a_marked_row(database, actor):
    marked = await builtin(database)
    with pytest.raises(HTTPException) as refused:
        await ManifestResourceService(database).delete(
            marked["id"], actor, expected_version=marked["resource_version"]
        )
    assert refused.value.status_code == 409
    assert refused.value.detail == INSTALLATION_MANAGED_MESSAGE
    assert await ManifestStore(database).by_id(marked["id"])


@pytest.mark.asyncio
async def test_the_installation_never_takes_over_an_admins_row(database, actor):
    plain, _ = await save(
        database, template("container-full", CATALOG), owner_id=str(actor["id"])
    )
    with pytest.raises(HTTPException) as refused:
        await save(
            database,
            template("container-full", CATALOG, resources={"cpu": 8}),
            owner_id=None,
            installation_managed=True,
            expected_version=plain["resource_version"],
        )
    assert refused.value.status_code == 409
    current = await ManifestStore(database).by_id(plain["id"])
    assert current["installation_managed"] is False
    assert current["resource_version"] == plain["resource_version"]


@pytest.mark.asyncio
async def test_the_same_name_in_an_account_scope_stays_editable(database, actor):
    await builtin(database)
    account = {"kind": "Account", "name": str(actor["id"])}
    service = ManifestResourceService(database)
    copy = template("container-full", account)
    created = await service.apply(json.dumps(copy), actor, format="json")
    version = created["resources"][0]["resourceVersion"]
    copy["spec"]["resources"] = {"cpu": 8}
    updated = await service.apply(
        json.dumps(copy),
        actor,
        format="json",
        expected_versions={key(copy): version},
    )
    assert updated["resources"][0]["resourceVersion"] == version + 1
    assert updated["resources"][0]["installationManaged"] is False
    await service.delete(
        updated["resources"][0]["uid"], actor, expected_version=version + 1
    )


@pytest.mark.asyncio
async def test_retiring_and_restoring_keeps_the_identifier(database, actor):
    marked = await builtin(database, "vm-full")
    store = ManifestStore(database)
    assert [row["name"] for row in await store.installation_managed(KIND)] == [
        "vm-full"
    ]
    async with database.transaction_scope():
        await store.lock_catalog()
        assert await store.retire_installation_managed(marked) is True
    assert await store.by_name(KIND, CATALOG, "vm-full") is None
    assert await store.installation_managed(KIND) == []

    retired = await store.retired_installation_managed(KIND, CATALOG, "vm-full")
    assert retired["id"] == marked["id"]
    async with database.transaction_scope():
        await store.lock_catalog()
        restored = await store.restore_installation_managed(retired)
    assert restored["id"] == marked["id"]
    assert restored["deleted_at"] is None
    assert restored["resource_version"] > retired["resource_version"]
    assert (await store.by_name(KIND, CATALOG, "vm-full"))["id"] == marked["id"]


@pytest.mark.asyncio
async def test_a_referenced_row_is_not_retired(database, actor):
    marked = await builtin(database, "vm-full")
    await save(
        database,
        template("uses-vm-full", {"kind": "Account", "name": str(actor["id"])}),
        owner_id=str(actor["id"]),
        dependencies=[
            {
                "uid": str(marked["id"]),
                "resourceVersion": marked["resource_version"],
                "revision": marked["revision"],
            }
        ],
    )
    store = ManifestStore(database)
    async with database.transaction_scope():
        await store.lock_catalog()
        assert await store.retire_installation_managed(marked) is False
    assert (await store.by_name(KIND, CATALOG, "vm-full"))["id"] == marked["id"]


@pytest.mark.asyncio
async def test_only_marked_rows_can_be_retired_this_way(database, actor):
    plain, _ = await save(
        database, template("team", CATALOG), owner_id=str(actor["id"])
    )
    store = ManifestStore(database)
    async with database.transaction_scope():
        await store.lock_catalog()
        with pytest.raises(ValueError):
            await store.retire_installation_managed(plain)
    assert await store.by_id(plain["id"])
