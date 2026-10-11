"""Per-folder grants of the main cloud on a real PostgreSQL (0520).

* the table's constraints: principals named after the row, one live grant per
  connector, member and kind, an active reader only once verified, no
  password once done;
* ``ensure_folder_grant``: mints a writer (no probe) and a reader (probe
  required), reuses the active one without a provider call, mints anew for a
  re-pointed folder and revokes the old one, records a failed mint as
  ``failed`` with a closed code after revoking it, refuses a folder outside
  the active installation;
* the hooks and the sweep: a member's removal and a project's delete revoke
  at once; the sweep revokes grants whose connector, project, membership or
  installation went away, finishes due revokes with backoff and gives up
  after its attempts; a user's delete requests the revoke in its own
  transaction.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from orchestrator.services.cloud import folder_grants
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.folder_grants import (
    FolderGrantUnavailable,
    ensure_folder_grant,
    folder_grant_target,
    lookup_active_folder_grant,
    principal_ids,
    revoke_folder_grants,
    sweep_folder_grants_once,
)
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


class FakeGrantBackend:
    """A provider with per-folder grants, recording its calls."""

    backend_id = "nextcloud"
    is_initialized = True

    def __init__(self, instance_id: str) -> None:
        self.backend_instance_id = instance_id
        self.principals: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail_ensure: Exception | None = None
        self.fail_revoke: Exception | None = None

    def folder_grant_webdav_url(self, handle, principal_id):
        return (
            f"http://nc/remote.php/dav/groupfolders/{principal_id}/{handle.native_id}/"
        )

    async def ensure_folder_principal(
        self, handle, *, principal_id, group_id, password, kind
    ):
        self.calls.append(("ensure", principal_id))
        if self.fail_ensure is not None:
            raise self.fail_ensure
        self.principals[principal_id] = {
            "group": group_id,
            "password": password,
            "kind": kind,
            "folder": handle.native_id,
        }

    async def revoke_folder_principal(self, *, principal_id, group_id):
        self.calls.append(("revoke", principal_id))
        if self.fail_revoke is not None:
            raise self.fail_revoke
        self.principals.pop(principal_id, None)

    def folder_principal_client(self, principal_id, password):  # pragma: no cover
        raise AssertionError("the probe is stubbed in these tests")

    async def seed_canary_fixture(self, handle):  # pragma: no cover
        raise AssertionError("the probe is stubbed in these tests")

    async def remove_canary_fixture(self, handle, fixture):  # pragma: no cover
        raise AssertionError("the probe is stubbed in these tests")


class FakeRouter:
    def __init__(self, backend: FakeGrantBackend) -> None:
        self.backend = backend
        self.active_instance_id = backend.backend_instance_id

    def for_backend_instance(self, instance_id, *, expected_backend_id):
        if instance_id != self.backend.backend_instance_id:
            raise LookupError(instance_id)
        return self.backend


@pytest.fixture
def probe(monkeypatch):
    stub = AsyncMock(return_value=None)
    monkeypatch.setattr(folder_grants, "_verify_read_only", stub)
    return stub


async def _world(db, *, members: dict[str, str] | None = None):
    owner = await _user(db, "Grant owner")
    instance = await _instance(db)
    project = await _cloud_project(db, "Grants", owner, instance, **(members or {}))
    assert await _ensure(db, project, instance) == "created"
    [row] = await _native_rows(db, str(project["id"]))
    connector = await db.get_datasource(str(row["id"]))
    target = await folder_grant_target(db, connector)
    assert target is not None
    backend = FakeGrantBackend(instance)
    return SimpleNamespace(
        owner=owner,
        instance=instance,
        project=project,
        connector=connector,
        target=target,
        backend=backend,
        router=FakeRouter(backend),
    )


async def _grants(db) -> list[dict]:
    return [
        dict(row)
        for row in await db.fetch(
            "SELECT * FROM cloud_folder_grants ORDER BY created_at, id"
        )
    ]


async def _insert(db, world, *, kind="writer", user_id=None, grant_id=None, **over):
    grant_id = grant_id or str(uuid4())
    principal, group = principal_ids(kind, grant_id)
    values = {
        "grant_id": grant_id,
        "kind": kind,
        "datasource_id": world.target.datasource_id,
        "project_id": world.target.project_id,
        "user_id": user_id or world.owner["id"],
        "backend_id": "nextcloud",
        "backend_instance_id": world.instance,
        "folder_handle_sha256": world.target.folder_handle_sha256,
        "principal_id": principal,
        "group_id": group,
        "credentials": "pw",
        "webdav_url": "http://nc/x/",
    }
    values.update(over)
    return await db.insert_cloud_folder_grant(**values)


# --------------------------------------------------------------------------- #
# The table
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_principals_are_named_after_their_row(database):
    world = await _world(database)
    other_principal, _ = principal_ids("writer", str(uuid4()))
    with pytest.raises(asyncpg.CheckViolationError):
        await _insert(database, world, principal_id=other_principal)
    reader_principal, reader_group = principal_ids("reader", str(uuid4()))
    with pytest.raises(asyncpg.CheckViolationError):
        await _insert(
            database,
            world,
            kind="writer",
            principal_id=reader_principal,
            group_id=reader_group,
        )


@pytest.mark.asyncio
async def test_one_live_grant_per_connector_member_and_kind(database):
    world = await _world(database)
    first = await _insert(database, world)
    assert first is not None and first["status"] == "minting"
    # The password is stored encrypted and read back decrypted.
    stored = await database.fetchval(
        "SELECT credentials FROM cloud_folder_grants WHERE id=$1", UUID(first["id"])
    )
    assert stored != "pw" and first["credentials"] == "pw"
    assert await _insert(database, world) is None
    # Another kind, or another member, is another slot.
    assert await _insert(database, world, kind="reader") is not None
    member = await _user(database, "Second member")
    assert await _insert(database, world, user_id=member["id"]) is not None
    # A revoking one no longer holds the slot.
    await database.request_cloud_folder_grant_revokes(
        reason="repointed", grant_id=first["id"]
    )
    assert await _insert(database, world) is not None


@pytest.mark.asyncio
async def test_an_active_reader_was_verified_and_a_done_grant_keeps_no_password(
    database,
):
    world = await _world(database)
    reader = await _insert(database, world, kind="reader")
    with pytest.raises(asyncpg.CheckViolationError):
        await database.activate_cloud_folder_grant(reader["id"], verified=False)
    active = await database.activate_cloud_folder_grant(reader["id"], verified=True)
    assert active["status"] == "active" and active["verified_at"] is not None
    with pytest.raises(asyncpg.CheckViolationError):
        await database.execute(
            "UPDATE cloud_folder_grants SET status='revoked', revoked_at=now(), "
            "revoke_requested_at=now(), revoke_reason='x' WHERE id=$1",
            UUID(reader["id"]),
        )
    [revoking] = await database.request_cloud_folder_grant_revokes(
        reason="member_removed", grant_id=reader["id"]
    )
    assert revoking["status"] == "revoking" and revoking["credentials"] is None
    assert await database.finish_cloud_folder_grant_revoke(reader["id"])
    [row] = [g for g in await _grants(database) if str(g["id"]) == reader["id"]]
    assert (row["status"], row["credentials"]) == ("revoked", None)
    assert row["revoked_at"] is not None


# --------------------------------------------------------------------------- #
# ensure / lookup
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_writer_is_minted_once_and_then_reused(database, probe):
    world = await _world(database)
    user_id = world.owner["id"]
    assert (
        await lookup_active_folder_grant(
            database, world.target, user_id=user_id, kind="writer"
        )
        is None
    )

    grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=user_id, kind="writer"
    )
    assert grant.principal_id.startswith("srw-writer-f-")
    assert "password" not in repr(grant) and grant.password not in repr(grant)
    assert world.backend.principals[grant.principal_id]["kind"] == "writer"
    assert world.backend.principals[grant.principal_id]["password"] == grant.password
    probe.assert_not_awaited()
    subject = grant.subject()
    assert (subject.username, subject.credential) == (
        grant.principal_id,
        grant.password,
    )

    again = await ensure_folder_grant(
        database, world.router, world.target, user_id=user_id, kind="writer"
    )
    assert again == grant
    assert world.backend.calls == [("ensure", grant.principal_id)]
    assert (
        await lookup_active_folder_grant(
            database, world.target, user_id=user_id, kind="writer"
        )
        == grant
    )
    [row] = await _grants(database)
    assert (row["status"], row["verified_at"]) == ("active", None)


@pytest.mark.asyncio
async def test_a_reader_is_active_only_after_its_probe(database, probe):
    world = await _world(database)
    grant = await ensure_folder_grant(
        database,
        world.router,
        world.target,
        user_id=world.owner["id"],
        kind="reader",
    )
    assert grant.principal_id.startswith("srw-reader-f-")
    probe.assert_awaited_once()
    assert probe.await_args.kwargs["principal_id"] == grant.principal_id
    [row] = await _grants(database)
    assert row["status"] == "active" and row["verified_at"] is not None


@pytest.mark.asyncio
async def test_a_failed_probe_revokes_and_records_failed(database, probe):
    world = await _world(database)
    probe.side_effect = folder_grants._ProbeFailed("PUT -> 201 (expected 403/405)")
    with pytest.raises(FolderGrantUnavailable) as caught:
        await ensure_folder_grant(
            database,
            world.router,
            world.target,
            user_id=world.owner["id"],
            kind="reader",
        )
    assert caught.value.code == "probe_failed"
    [row] = await _grants(database)
    assert (row["status"], row["failure_code"], row["credentials"]) == (
        "failed",
        "probe_failed",
        None,
    )
    assert row["revoke_reason"] == "mint_failed"
    assert world.backend.principals == {}
    assert world.backend.calls[-1] == ("revoke", row["principal_id"])


@pytest.mark.asyncio
async def test_a_refused_provision_stays_revoking_until_its_revoke_passes(
    database, probe
):
    world = await _world(database)
    world.backend.fail_ensure = CloudBackendError(
        CloudBackendErrorKind.UNKNOWN, "create failed", backend="nextcloud"
    )
    world.backend.fail_revoke = CloudBackendError(
        CloudBackendErrorKind.UNAVAILABLE, "down", backend="nextcloud"
    )
    with pytest.raises(FolderGrantUnavailable) as caught:
        await ensure_folder_grant(
            database,
            world.router,
            world.target,
            user_id=world.owner["id"],
            kind="writer",
        )
    assert caught.value.code == "provision_failed"
    [row] = await _grants(database)
    assert (row["status"], row["failure_code"], row["revoke_attempts"]) == (
        "revoking",
        "provision_failed",
        1,
    )
    # The sweep finishes it once the provider answers.
    world.backend.fail_revoke = None
    await database.execute(
        "UPDATE cloud_folder_grants SET revoke_next_at=now() WHERE id=$1", row["id"]
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert report.revoked == 1
    [row] = await _grants(database)
    assert row["status"] == "failed"


@pytest.mark.asyncio
async def test_a_repointed_folder_gets_a_new_grant_and_the_old_one_goes(
    database, probe
):
    world = await _world(database)
    user_id = world.owner["id"]
    old = await ensure_folder_grant(
        database, world.router, world.target, user_id=user_id, kind="writer"
    )
    await database.execute(
        "UPDATE projects SET main_cloud_folder_handle='nextcloud:Moved' WHERE id=$1",
        UUID(world.target.project_id),
    )
    moved = await folder_grant_target(database, world.connector)
    assert moved.folder_handle == "nextcloud:Moved"
    assert (
        await lookup_active_folder_grant(
            database, moved, user_id=user_id, kind="writer"
        )
        is None
    )

    new = await ensure_folder_grant(
        database, world.router, moved, user_id=user_id, kind="writer"
    )
    assert new.principal_id != old.principal_id
    statuses = {str(g["principal_id"]): g["status"] for g in await _grants(database)}
    assert statuses == {old.principal_id: "revoked", new.principal_id: "active"}
    assert old.principal_id not in world.backend.principals


@pytest.mark.asyncio
async def test_a_folder_outside_the_active_installation_gets_no_grant(database, probe):
    world = await _world(database)
    world.router.active_instance_id = str(uuid4())
    with pytest.raises(FolderGrantUnavailable) as caught:
        await ensure_folder_grant(
            database,
            world.router,
            world.target,
            user_id=world.owner["id"],
            kind="writer",
        )
    assert caught.value.code == "backend_unavailable"
    assert await _grants(database) == []


@pytest.mark.asyncio
async def test_a_provider_without_folder_grants_gets_none(database, probe):
    world = await _world(database)
    world.router.backend = SimpleNamespace(
        backend_id="nextcloud",
        is_initialized=True,
        backend_instance_id=world.instance,
    )
    with pytest.raises(FolderGrantUnavailable) as caught:
        await ensure_folder_grant(
            database,
            world.router,
            world.target,
            user_id=world.owner["id"],
            kind="writer",
        )
    assert caught.value.code == "not_supported"
    assert await _grants(database) == []


@pytest.mark.asyncio
async def test_a_lost_mint_is_completed_with_its_stored_password(database, probe):
    world = await _world(database)
    lost = await _insert(database, world, credentials="stored-pw")
    grant = await ensure_folder_grant(
        database,
        world.router,
        world.target,
        user_id=world.owner["id"],
        kind="writer",
    )
    assert grant.id == lost["id"] and grant.password == "stored-pw"


# --------------------------------------------------------------------------- #
# Hooks and the sweep
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_member_removal_revokes_that_members_grants_only(database, probe):
    member = None
    world = await _world(database)
    member = await _user(database, "Editor")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    mine = await ensure_folder_grant(
        database, world.router, world.target, user_id=world.owner["id"], kind="writer"
    )
    theirs = await ensure_folder_grant(
        database, world.router, world.target, user_id=member["id"], kind="writer"
    )
    await database.remove_project_member(world.target.project_id, member["id"])

    assert (
        await revoke_folder_grants(
            database,
            world.router,
            reason="member_removed",
            project_id=world.target.project_id,
            user_id=member["id"],
        )
        == 1
    )
    statuses = {str(g["principal_id"]): g["status"] for g in await _grants(database)}
    assert statuses == {mine.principal_id: "active", theirs.principal_id: "revoked"}
    assert set(world.backend.principals) == {mine.principal_id}


@pytest.mark.asyncio
async def test_the_sweep_revokes_what_no_longer_serves(database, probe):
    world = await _world(database)
    member = await _user(database, "Leaver")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    owner_grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=world.owner["id"], kind="writer"
    )
    leaver_grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=member["id"], kind="reader"
    )
    # Nothing is stale yet.
    assert (await sweep_folder_grants_once(database, world.router)).any() is False

    # The member leaves without the hook (another replica, a crash).
    await database.remove_project_member(world.target.project_id, member["id"])
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.stale, report.revoked) == (1, 1)
    rows = {str(g["principal_id"]): g for g in await _grants(database)}
    assert rows[leaver_grant.principal_id]["revoke_reason"] == "member_removed"
    assert rows[owner_grant.principal_id]["status"] == "active"

    # Another installation becomes the active one.
    world.router.active_instance_id = str(uuid4())
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.stale, report.revoked) == (1, 1)
    rows = {str(g["principal_id"]): g for g in await _grants(database)}
    assert rows[owner_grant.principal_id]["revoke_reason"] == "installation_inactive"
    assert world.backend.principals == {}


@pytest.mark.asyncio
async def test_project_delete_leaves_its_grants_to_be_revoked(database, probe):
    world = await _world(database)
    grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=world.owner["id"], kind="writer"
    )
    assert await database.delete_project(world.target.project_id)
    # The connector went with the project; the grant row outlives both.
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.stale, report.revoked) == (1, 1)
    [row] = await _grants(database)
    assert row["revoke_reason"] in {"connector_gone", "project_deleted"}
    assert row["status"] == "revoked"
    assert grant.principal_id not in world.backend.principals


@pytest.mark.asyncio
async def test_a_revoke_is_given_up_after_its_attempts(database, probe, monkeypatch):
    world = await _world(database)
    grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=world.owner["id"], kind="writer"
    )
    world.backend.fail_revoke = CloudBackendError(
        CloudBackendErrorKind.UNAVAILABLE, "down", backend="nextcloud"
    )
    monkeypatch.setattr(folder_grants, "MAX_REVOKE_ATTEMPTS", 2)
    await revoke_folder_grants(
        database,
        world.router,
        reason="member_removed",
        project_id=world.target.project_id,
    )
    [row] = await _grants(database)
    assert (row["status"], row["revoke_attempts"]) == ("revoking", 1)
    assert row["revoke_next_at"] > row["revoke_requested_at"]
    await database.execute(
        "UPDATE cloud_folder_grants SET revoke_next_at=now() WHERE id=$1", row["id"]
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert report.given_up == 1
    [row] = await _grants(database)
    assert row["status"] == "abandoned" and row["revoked_at"] is not None
    assert "down" in row["revoke_error"]
    assert grant.principal_id in world.backend.principals
    # Given up is recorded as a security event, once, naming the principal.
    [event] = await database.fetch(
        "SELECT * FROM security_events WHERE event_type=$1",
        folder_grants.REVOKE_ABANDONED_EVENT,
    )
    assert (event["resource_type"], event["resource_id"]) == (
        "cloud_folder_grant",
        grant.id,
    )
    assert str(event["user_id"]) == world.owner["id"]
    assert grant.principal_id in event["detail"] and "attempts=2" in event["detail"]

    # ...but never the end: its principal may be live, so it is tried again
    # at the backoff's cap.
    assert row["revoke_attempts"] == 2
    wait = (row["revoke_next_at"] - row["revoked_at"]).total_seconds()
    assert abs(wait - folder_grants.ABANDONED_RETRY_SECONDS) < 60
    assert (await sweep_folder_grants_once(database, world.router)).any() is False
    await database.execute(
        "UPDATE cloud_folder_grants SET revoke_next_at=now() WHERE id=$1", row["id"]
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.retried, report.given_up) == (1, 0)
    [row] = await _grants(database)
    assert (row["status"], row["revoke_attempts"]) == ("abandoned", 3)
    assert row["revoke_next_at"] > row["revoked_at"]
    assert (
        await database.fetchval(
            "SELECT count(*) FROM security_events WHERE event_type=$1",
            folder_grants.REVOKE_ABANDONED_EVENT,
        )
        == 1
    )

    # Once the provider answers, the principal goes.
    world.backend.fail_revoke = None
    await database.execute(
        "UPDATE cloud_folder_grants SET revoke_next_at=now() WHERE id=$1", row["id"]
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert report.revoked == 1
    [row] = await _grants(database)
    assert (row["status"], row["revoke_error"]) == ("revoked", None)
    assert grant.principal_id not in world.backend.principals


@pytest.mark.asyncio
async def test_due_revokes_come_before_abandoned_ones(database, probe):
    world = await _world(database)
    member = await _user(database, "Second")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    first = await _insert(database, world)
    second = await _insert(database, world, user_id=member["id"])
    for row in (first, second):
        await database.request_cloud_folder_grant_revokes(
            reason="member_removed", grant_id=row["id"]
        )
    # The first was given up long ago; the second is a fresh revoke.
    assert await database.finish_cloud_folder_grant_revoke(
        first["id"], abandoned=True, error="down", attempts=12
    )
    await database.execute(
        "UPDATE cloud_folder_grants SET revoke_next_at=now() - interval '1 day' "
        "WHERE id=$1",
        UUID(first["id"]),
    )
    due = await database.list_due_cloud_folder_grant_revokes(limit=10)
    assert [row["id"] for row in due] == [second["id"], first["id"]]
    assert [
        row["id"] for row in await database.list_due_cloud_folder_grant_revokes(limit=1)
    ] == [second["id"]]
    # An abandoned grant is never abandoned twice.
    assert not await database.finish_cloud_folder_grant_revoke(
        first["id"], abandoned=True, error="down"
    )
    assert await database.cloud_folder_grants_pending()


@pytest.mark.asyncio
async def test_a_lost_mint_is_revoked_by_the_sweep(database, probe, monkeypatch):
    world = await _world(database)
    lost = await _insert(database, world)
    monkeypatch.setattr(folder_grants, "MINT_ABANDON_SECONDS", 0.0)
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.abandoned_mints, report.revoked) == (1, 1)
    [row] = await _grants(database)
    assert (row["status"], row["failure_code"]) == ("failed", "mint_abandoned")
    assert world.backend.calls == [("revoke", lost["principal_id"])]


@pytest.mark.asyncio
async def test_every_move_to_revoking_drops_the_password(database, probe, monkeypatch):
    """0520 would let a revoking row keep its password; none of the three
    moves to ``revoking`` (a hook's request, the lost-mint sweep, a user's
    delete) does, even while the revoke itself keeps failing."""
    world = await _world(database)
    world.backend.fail_revoke = CloudBackendError(
        CloudBackendErrorKind.UNAVAILABLE, "down", backend="nextcloud"
    )
    requested = await _insert(database, world)
    await database.activate_cloud_folder_grant(requested["id"], verified=False)
    member = await _user(database, "Retiring")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    retired = await _insert(database, world, user_id=member["id"])
    lost = await _insert(database, world, kind="reader")

    await revoke_folder_grants(
        database,
        world.router,
        reason="connector_gone",
        datasource_id=world.target.datasource_id,
        user_id=world.owner["id"],
        kind="writer",
    )
    assert await database.delete_user(member["id"])
    monkeypatch.setattr(folder_grants, "MINT_ABANDON_SECONDS", 0.0)
    report = await sweep_folder_grants_once(database, world.router)
    # The hook's own attempt already failed (its next one is later); the
    # user's and the lost mint's fail now.
    assert report.abandoned_mints == 1 and report.retried == 2

    rows = {str(g["id"]): g for g in await _grants(database)}
    for grant in (requested, retired, lost):
        row = rows[grant["id"]]
        assert (row["status"], row["credentials"]) == ("revoking", None)


@pytest.mark.asyncio
async def test_a_users_delete_requests_its_revokes(database, probe):
    world = await _world(database)
    member = await _user(database, "Retiring")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    grant = await ensure_folder_grant(
        database, world.router, world.target, user_id=member["id"], kind="writer"
    )
    assert await database.delete_user(member["id"])
    [row] = await _grants(database)
    assert (row["status"], row["revoke_reason"], row["credentials"]) == (
        "revoking",
        "user_retired",
        None,
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert report.revoked == 1
    assert grant.principal_id not in world.backend.principals


@pytest.mark.asyncio
async def test_a_member_lowered_to_viewer_loses_the_writer_only(database, probe):
    """A viewer binds the folder read-only (slice 5's ceiling): its writer
    serves nothing any more; its reader stays."""
    world = await _world(database)
    member = await _user(database, "Lowered")
    await database.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        UUID(world.target.project_id),
        UUID(member["id"]),
    )
    writer = await ensure_folder_grant(
        database, world.router, world.target, user_id=member["id"], kind="writer"
    )
    reader = await ensure_folder_grant(
        database, world.router, world.target, user_id=member["id"], kind="reader"
    )
    assert await database.update_project_member_role(
        world.target.project_id, member["id"], "viewer"
    )
    report = await sweep_folder_grants_once(database, world.router)
    assert (report.stale, report.revoked) == (1, 1)
    rows = {str(g["principal_id"]): g for g in await _grants(database)}
    assert (
        rows[writer.principal_id]["status"],
        rows[writer.principal_id]["revoke_reason"],
    ) == ("revoked", "role_lowered")
    assert rows[reader.principal_id]["status"] == "active"
