"""A user's own cloud root on a real PostgreSQL (main cloud slice 4).

* the row SRW creates is the one the policy reads: eligible and selected by
  default for its owner in their personal project only, for nobody else (a
  member of the personal project, an administrator), and an explicit empty
  list attaches nothing;
* a connected root's credential is stored encrypted, decrypted only on the
  read the mount makes, and is in no config, connector resource document or
  security event;
* deleting the user removes the row, and the delete succeeds.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from orchestrator.services import cloud_user_root
from orchestrator.services.datasource_policy import (
    classify_datasource_selection,
    default_datasource_selection,
)
from tests import test_manifest_native_full_schema as full_schema
from tests.test_cloud_user_root import TOKEN, FakeCloud, FakeRouter
from tests.test_native_cloud_connectors_real_postgres import _instance
from tests.test_project_connectors_real_postgres import (
    _project,
    _session_selection,
    _user,
)

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _owner_with_personal_project(db, name: str, **members) -> tuple[dict, str]:
    owner = await _user(db, name)
    project = await _project(db, f"{name}'s Workspace", owner, **members)
    await db.execute("UPDATE projects SET is_default=TRUE WHERE id=$1", UUID(project))
    await db.execute(
        "UPDATE users SET default_project_id=$2, email=$3 WHERE id=$1",
        UUID(owner["id"]),
        UUID(project),
        "ann@example.com",
    )
    return owner, project


@pytest.mark.asyncio
async def test_the_root_is_its_owners_alone_and_a_default_only_at_home(database):
    member = await _user(database, "Member")
    admin = await _user(database, "Administrator", admin=True)
    owner, personal = await _owner_with_personal_project(
        database, "Ann", **{member["id"]: "editor", admin["id"]: "editor"}
    )
    elsewhere = await _project(database, "Elsewhere", owner)
    cloud = FakeCloud(await _instance(database))
    router = FakeRouter(cloud)

    assert (
        await cloud_user_root.ensure_user_root_connector(database, router, owner["id"])
        == "created"
    )
    assert (
        await cloud_user_root.ensure_user_root_connector(database, router, owner["id"])
        == "present"
    )
    row = await database.get_datasource_by_managed_key(f"user-cloud-root:{owner['id']}")
    root = str(row["id"])
    assert row["type"] == "cloud_folder"
    assert row["created_by"] == UUID(owner["id"])
    assert row["auto_attach"] is True and row["scope_mode"] == "projects"

    async def eligible(user, project, *, is_admin=False):
        return {
            str(entry["id"]): entry
            for entry in await database.list_eligible_datasources(
                user["id"], [project], is_admin=is_admin
            )
        }

    mine = await eligible(owner, personal)
    assert mine[root]["default_selected"] is True
    assert root not in await eligible(owner, elsewhere)
    assert root not in await eligible(member, personal)
    assert root not in await eligible(admin, personal, is_admin=True)
    for user in (member, admin):
        selected, _ = await default_datasource_selection(
            database, user["id"], [personal], "sandbox", execution_kind="session"
        )
        assert root not in selected

    # A member forcing the root's id is refused, as is an administrator's
    # override.
    for user, kwargs in (
        (member, {}),
        (admin, {"allow_admin_explicit_override": True}),
    ):
        verdicts, _ = await classify_datasource_selection(
            database,
            await database.get_user(user["id"]),
            user["id"],
            [root],
            [personal],
            "sandbox",
            **kwargs,
        )
        assert [v.denied for v in verdicts] == [True]

    ids, _ = await _session_selection(database, owner, personal)
    assert root in ids
    ids, _ = await _session_selection(database, owner, personal, datasource_ids=[])
    assert ids == []
    ids, _ = await _session_selection(database, owner, elsewhere)
    assert root not in ids


@pytest.mark.asyncio
async def test_the_credential_is_stored_encrypted_and_the_row_goes_with_its_user(
    database,
):
    owner, _personal = await _owner_with_personal_project(database, "Ann")
    cloud = FakeCloud(await _instance(database))
    router = FakeRouter(cloud)
    await cloud_user_root.ensure_user_root_connector(database, router, owner["id"])
    user = await database.get_user(owner["id"])

    assert (
        await cloud_user_root.complete_connect(
            database, cloud, user, cloud.issue(TOKEN, "uid-ann")
        )
        == "connected"
    )
    row = await database.get_datasource_by_managed_key(f"user-cloud-root:{owner['id']}")
    root = row["id"]
    raw = await database.fetchrow(
        "SELECT credentials::text AS credentials, config::text AS config "
        "FROM datasources WHERE id=$1",
        root,
    )
    assert TOKEN not in raw["credentials"] and TOKEN not in raw["config"]
    full = await database.get_datasource(str(root))
    assert full["credentials"] == {"username": "uid-ann", "password": TOKEN}
    documents = await database.fetch(
        "SELECT document::text AS document FROM srw_resources WHERE id=$1", root
    )
    assert documents and all(TOKEN not in d["document"] for d in documents)
    events = await database.fetch(
        "SELECT event_type, detail FROM security_events WHERE resource_id=$1",
        str(root),
    )
    assert [e["event_type"] for e in events] == ["cloud_root_granted"]
    assert all(TOKEN not in (e["detail"] or "") for e in events)
    built = await cloud_user_root.user_root_mount(
        full, owner_id=owner["id"], store=database, cloud_router=router
    )
    assert built[1].credential == TOKEN

    revoke = await cloud_user_root.retirement_revoke(database, router, owner["id"])
    assert await database.delete_user(owner["id"]) is True
    await revoke()
    assert cloud.revoked == [TOKEN]
    assert (
        await database.fetchval("SELECT count(*) FROM datasources WHERE id=$1", root)
        == 0
    )
