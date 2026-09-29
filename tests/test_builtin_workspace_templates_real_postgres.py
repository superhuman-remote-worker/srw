"""The Catalog follows the chart's built-in workspace templates."""

import asyncio
from copy import deepcopy

import pytest

from orchestrator.services.builtin_workspace_templates import (
    reconcile_builtin_workspace_templates as reconcile,
)
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests import preview_documents
from shared.manifests.resolution import content_revision
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

CATALOG = {"kind": "Catalog", "name": "shared"}
KIND = "WorkspaceTemplate"
IMAGE = "ghcr.io/superhuman-remote-worker/srw-workspace:sha-"
EMPTY = {
    "created": [],
    "updated": [],
    "unchanged": [],
    "restored": [],
    "retired": [],
    "kept": [],
    "conflicts": [],
    "invalid": [],
}


def builtin(name: str, spec: dict, scope: dict | None = None, kind: str = KIND):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": kind,
        "metadata": {"name": name, "scope": scope or CATALOG},
        "spec": spec,
    }


def release(tag: str = "1111111", *, vm: bool = True) -> list[dict]:
    sizes = {"cpu": 2, "memory": "4Gi", "requests": {"cpu": 0.5, "memory": "1Gi"}}
    declared = [
        builtin("virtual", {"backend": "virtual"}),
        builtin(
            "container-full",
            {
                "backend": "sandbox",
                "environment": {"image": IMAGE + tag},
                "resources": sizes,
            },
        ),
    ]
    if vm:
        declared.append(
            builtin(
                "vm-full",
                {
                    "backend": "vm",
                    "environment": {"image": "registry.example/vm:" + tag},
                    "resources": {"cpu": 8, "memory": "16Gi", "storage": "30Gi"},
                },
            )
        )
    return declared


def summary(**changes) -> dict:
    return {**deepcopy(EMPTY), **changes}


async def live(database) -> dict[str, dict]:
    rows = await ManifestStore(database).list_scope(CATALOG, kind=KIND)
    return {row["name"]: row for row in rows}


async def refer_to(database, actor, row):
    """Save an Account template whose dependencies name ``row``."""
    document = builtin(
        "uses-" + row["name"],
        {"backend": "virtual"},
        scope={"kind": "Account", "name": str(actor["id"])},
    )
    resolved = preview_documents([document])["resolved"][0]
    async with database.transaction_scope():
        store = ManifestStore(database)
        await store.lock_catalog()
        await store.lock_identity(document)
        await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            [
                {
                    "uid": str(row["id"]),
                    "resourceVersion": row["resource_version"],
                    "revision": row["revision"],
                }
            ],
            owner_id=str(actor["id"]),
        )


@pytest.mark.asyncio
async def test_a_fresh_installation_gets_the_declared_builtins(database):
    result = await reconcile(database, release())
    assert result == summary(created=["virtual", "container-full", "vm-full"])
    rows = await live(database)
    assert set(rows) == {"virtual", "container-full", "vm-full"}
    for row in rows.values():
        assert row["installation_managed"] is True
        assert row["owner_id"] is None
        assert row["resource_version"] == 1
    assert rows["container-full"]["resolved"]["spec"]["resources"]["requests"] == {
        "cpu": 0.5,
        "memory": "1Gi",
    }


@pytest.mark.asyncio
async def test_a_restart_changes_nothing(database):
    await reconcile(database, release())
    before = await live(database)
    result = await reconcile(database, release())
    assert result == summary(unchanged=["virtual", "container-full", "vm-full"])
    after = await live(database)
    for name, row in before.items():
        assert after[name]["resource_version"] == row["resource_version"]
        assert after[name]["updated_at"] == row["updated_at"]


@pytest.mark.asyncio
async def test_a_release_updates_in_place_and_keeps_history(database):
    await reconcile(database, release("1111111"))
    before = await live(database)
    result = await reconcile(database, release("2222222"))
    assert result == summary(
        updated=["container-full", "vm-full"], unchanged=["virtual"]
    )
    after = await live(database)
    row = after["container-full"]
    assert row["id"] == before["container-full"]["id"]
    assert row["resource_version"] == 2
    assert row["resolved"]["spec"]["environment"]["image"] == IMAGE + "2222222"
    store = ManifestStore(database)
    old = await store.by_name(
        KIND, CATALOG, "container-full", revision=before["container-full"]["revision"]
    )
    assert old["resolved"]["spec"]["environment"]["image"] == IMAGE + "1111111"


@pytest.mark.asyncio
async def test_a_rollback_restores_the_older_content(database):
    await reconcile(database, release("1111111"))
    await reconcile(database, release("2222222"))
    await reconcile(database, release("1111111"))
    row = (await live(database))["container-full"]
    assert row["resolved"]["spec"]["environment"]["image"] == IMAGE + "1111111"
    assert row["resource_version"] == 3


@pytest.mark.asyncio
async def test_an_undeclared_builtin_is_retired_and_later_restored(database):
    await reconcile(database, release())
    original = (await live(database))["vm-full"]

    result = await reconcile(database, release(vm=False))
    assert result == summary(
        unchanged=["virtual", "container-full"], retired=["vm-full"]
    )
    assert "vm-full" not in await live(database)

    result = await reconcile(database, release("3333333"))
    assert result == summary(
        updated=["container-full"], unchanged=["virtual"], restored=["vm-full"]
    )
    row = (await live(database))["vm-full"]
    assert row["id"] == original["id"]
    assert row["resolved"]["spec"]["environment"]["image"] == (
        "registry.example/vm:3333333"
    )


@pytest.mark.asyncio
async def test_an_empty_declaration_retires_every_builtin(database):
    await reconcile(database, release())
    result = await reconcile(database, [])
    assert result == summary(retired=["container-full", "virtual", "vm-full"])
    assert await live(database) == {}


@pytest.mark.asyncio
async def test_a_referenced_builtin_is_kept_when_undeclared(database, actor):
    await reconcile(database, release())
    await refer_to(database, actor, (await live(database))["vm-full"])
    result = await reconcile(database, release(vm=False))
    assert result == summary(unchanged=["virtual", "container-full"], kept=["vm-full"])
    row = (await live(database))["vm-full"]
    assert row["installation_managed"] is True


@pytest.mark.asyncio
async def test_a_referenced_builtin_still_updates(database, actor):
    await reconcile(database, release("1111111"))
    await refer_to(database, actor, (await live(database))["container-full"])
    result = await reconcile(database, release("2222222"))
    assert "container-full" in result["updated"]
    row = (await live(database))["container-full"]
    assert row["resolved"]["spec"]["environment"]["image"] == IMAGE + "2222222"


@pytest.mark.asyncio
async def test_an_admins_row_with_a_builtin_name_is_left_alone(database, actor):
    document = builtin("container-full", {"backend": "sandbox"})
    resolved = preview_documents([document])["resolved"][0]
    async with database.transaction_scope():
        store = ManifestStore(database)
        await store.lock_catalog()
        await store.lock_identity(document)
        mine, _ = await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            [],
            owner_id=str(actor["id"]),
        )
    result = await reconcile(database, release())
    assert result == summary(
        created=["virtual", "vm-full"], conflicts=["container-full"]
    )
    row = (await live(database))["container-full"]
    assert row["id"] == mine["id"]
    assert row["installation_managed"] is False
    assert row["resource_version"] == mine["resource_version"]


@pytest.mark.asyncio
async def test_a_conflict_wins_over_a_retired_builtin(database, actor):
    await reconcile(database, release())
    await reconcile(database, release(vm=False))
    document = builtin("vm-full", {"backend": "vm"})
    resolved = preview_documents([document])["resolved"][0]
    async with database.transaction_scope():
        store = ManifestStore(database)
        await store.lock_catalog()
        await store.lock_identity(document)
        mine, _ = await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            [],
            owner_id=str(actor["id"]),
        )
    result = await reconcile(database, release())
    assert result["conflicts"] == ["vm-full"]
    assert result["restored"] == []
    assert (await live(database))["vm-full"]["id"] == mine["id"]


@pytest.mark.asyncio
async def test_invalid_declaration_keeps_the_existing_row(database):
    await reconcile(database, release("1111111"))
    before = (await live(database))["container-full"]
    declared = release("2222222")
    # A request above its maximum: the chart's schema can't catch this.
    declared[1]["spec"]["resources"]["requests"]["memory"] = "8Gi"
    result = await reconcile(database, declared)
    assert result == summary(
        updated=["vm-full"], unchanged=["virtual"], invalid=["container-full"]
    )
    after = (await live(database))["container-full"]
    assert after["resource_version"] == before["resource_version"]
    assert after["revision"] == before["revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "foreign",
    [
        builtin("sneaky", {"backend": "virtual"}, kind="Expert"),
        builtin(
            "sneaky",
            {"backend": "virtual"},
            scope={"kind": "Account", "name": "11111111-1111-1111-1111-111111111111"},
        ),
        builtin(
            "sneaky", {"backend": "virtual"}, scope={"kind": "Catalog", "name": "x"}
        ),
        builtin("sneaky", {"backend": "sandbox", "environment": {"prepare": []}}),
        {"kind": KIND},
    ],
)
async def test_foreign_documents_are_never_written(database, foreign):
    result = await reconcile(
        database, [builtin("virtual", {"backend": "virtual"}), foreign]
    )
    assert result["created"] == ["virtual"]
    assert len(result["invalid"]) == 1
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == 1


@pytest.mark.asyncio
async def test_two_replicas_starting_together_agree(database):
    first, second = await asyncio.gather(
        reconcile(database, release()), reconcile(database, release())
    )
    names = ["virtual", "container-full", "vm-full"]
    assert sorted([first["created"], second["created"]]) == [[], names]
    assert sorted([first["unchanged"], second["unchanged"]]) == [[], names]
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_resources WHERE kind='WorkspaceTemplate'"
        )
        == 3
    )
