"""A user's own cloud root as a connector (main cloud slice 4).

A fake store and a fake provider (``SupportsUserRootGrant``) pin:

* one row per approved user, under ``user-cloud-root:<user>``, linked to the
  personal project only, created once and re-pointed (credential cleared,
  old one revoked) when the active installation changes; nothing on a
  provider without user roots or with grants off;
* the connect flow: a finished flow's credential is stored only for the
  user's own account (same e-mail, not a cloud administrator, enabled,
  reading its files), any refusal revokes it, a reconnect revokes the
  previous one once the new one is stored; the poller writes its outcome
  (connected, expired) and can be cancelled;
* the binder's exclusions (not the owner, not connected, revoked, grants
  off) and the probe before a Pod (a refused credential revokes the row);
* the credential is in no status body, no security event, no config, no
  ``repr``.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from typing import Any

import pytest

from orchestrator.services import cloud_user_root
from orchestrator.services.cloud.capabilities import (
    CloudCapability,
    ProviderCapabilities,
)
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.handles import ProjectFolderHandle
from orchestrator.services.cloud.user_root_grant import (
    CloudAccount,
    LoginFlowGrant,
    LoginFlowStart,
    SupportsUserRootGrant,
)
from orchestrator.services.connector_drivers.cloud_folder import (
    CloudFolderUnavailable,
)

ANN = "00000000-0000-0000-0000-0000000000a1"
BOB = "00000000-0000-0000-0000-0000000000b2"
ANN_PROJECT = "00000000-0000-0000-0000-00000000aa01"
BOB_PROJECT = "00000000-0000-0000-0000-00000000bb01"
INSTANCE = "00000000-0000-0000-0000-0000000001c1"
OTHER_INSTANCE = "00000000-0000-0000-0000-0000000001c2"
TOKEN = "app-password-SECRET-1"
TOKEN_2 = "app-password-SECRET-2"
STRANGER_TOKEN = "app-password-SECRET-3"
POLL_TOKEN = "poll-token-SECRET"


def _offering(status: str = "offered") -> ProviderCapabilities:
    return ProviderCapabilities(
        backend_id="acme-cloud",
        title="Acme",
        capabilities=(
            CloudCapability(
                "cloud_folder",
                "user_root",
                "read_write",
                status,
                "the user's own app token",
                frozenset({"sandbox"}),
                slice=None if status == "offered" else 4,
            ),
        ),
    )


class FakeCloud:
    """A provider with user roots: accounts, issued credentials, revokes."""

    backend_id = "acme-cloud"
    is_initialized = True
    user_root_client_name = "Superhuman Remote Worker"

    def __init__(self, instance: str = INSTANCE) -> None:
        self.backend_instance_id = instance
        self.capabilities = _offering()
        self.grant_mode = "login_flow"
        self.accounts = {
            "uid-ann": CloudAccount("uid-ann", "Ann@Example.com", True, False),
            "uid-root": CloudAccount("uid-root", "ann@example.com", True, True),
            "uid-off": CloudAccount("uid-off", "ann@example.com", False, False),
            "uid-eve": CloudAccount("uid-eve", "eve@example.com", True, False),
        }
        # credential -> (login name, account id)
        self.issued: dict[str, tuple[str, str]] = {}
        self.revoked: list[str] = []
        self.polls: list[Any] = []
        self.verify_answer: str | None = None
        self.started = 0

    @property
    def user_root_grant_mode(self) -> str:
        return self.grant_mode

    def issue(self, credential: str, account_id: str, login: str | None = None):
        login = login or account_id
        self.issued[credential] = (login, account_id)
        return LoginFlowGrant(login_name=login, credential=credential)

    async def start_login_flow(self) -> LoginFlowStart:
        self.started += 1
        return LoginFlowStart("https://cloud.example/login/v2/flow/abc", POLL_TOKEN)

    async def poll_login_flow(self, poll_token: str):
        assert poll_token == POLL_TOKEN
        if not self.polls:
            return None
        answer = self.polls.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def user_root_account_id(self, login_name, credential):
        issued = self.issued.get(credential)
        return issued[1] if issued and issued[0] == login_name else None

    async def user_root_account(self, account_id):
        return self.accounts.get(account_id)

    def user_root_handle(self, account_id) -> ProjectFolderHandle:
        return ProjectFolderHandle(
            backend=self.backend_id,
            native_id=f"home:{account_id}",
            vendor_meta={"kind": "user_home", "username": account_id},
        )

    async def verify_user_root(self, account_id, login_name, credential):
        if self.verify_answer is not None:
            return self.verify_answer
        issued = self.issued.get(credential)
        return "ok" if issued == (login_name, account_id) else "rejected"

    async def revoke_user_root(self, login_name, credential):
        self.revoked.append(credential)
        self.issued.pop(credential, None)
        return True

    async def rotate_user_root(self, login_name, credential):  # pragma: no cover
        raise NotImplementedError


class NoRoots:
    """A provider without user roots (OpenCloud today)."""

    backend_id = "other-cloud"
    is_initialized = True
    backend_instance_id = INSTANCE
    capabilities = _offering("planned")


class FakeRouter:
    def __init__(self, *backends: Any) -> None:
        self.active = backends[0]
        self.instances = {backend.backend_instance_id: backend for backend in backends}

    @property
    def active_instance_id(self):
        return self.active.backend_instance_id

    def for_backend_instance(self, instance_id, *, expected_backend_id):
        backend = self.instances.get(str(instance_id))
        if backend is None or backend.backend_id != expected_backend_id:
            raise LookupError(instance_id)
        return backend


class FakeStore:
    def __init__(self) -> None:
        self.users = {
            ANN: {
                "id": ANN,
                "email": "ann@example.com",
                "is_approved": True,
                "default_project_id": ANN_PROJECT,
            },
            BOB: {
                "id": BOB,
                "email": "bob@example.com",
                "is_approved": False,
                "default_project_id": BOB_PROJECT,
            },
        }
        self.rows: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.identities: list[tuple[str, str, dict]] = []
        self.fail_update = False

    async def get_user(self, user_id):
        user = self.users.get(str(user_id))
        return dict(user) if user else None

    async def list_users(self, limit=100):
        return [dict(user) for user in self.users.values()]

    async def get_datasource_by_managed_key(self, key):
        for row in self.rows.values():
            if row["managed_key"] == key:
                found = copy.deepcopy(row)
                found.pop("credentials")
                return found
        return None

    async def get_datasource(self, datasource_id):
        row = self.rows.get(str(datasource_id))
        return copy.deepcopy(row) if row else None

    async def create_datasource(self, **fields):
        if any(r["managed_key"] == fields["managed_key"] for r in self.rows.values()):
            return None
        row_id = str(uuid.uuid4())
        row = {
            "id": row_id,
            "name": fields["name"],
            "type": fields["ds_type"],
            "description": fields.get("description"),
            "credentials": dict(fields.get("credentials") or {}),
            "config": copy.deepcopy(fields.get("config") or {}),
            "created_by": fields.get("created_by"),
            "scope_mode": fields.get("scope_mode"),
            "auto_attach": fields.get("auto_attach"),
            "project_ids": list(fields.get("project_ids") or []),
            "managed_key": fields["managed_key"],
            "read_only": fields.get("read_only"),
        }
        self.rows[row_id] = row
        return copy.deepcopy(row)

    async def update_datasource(self, datasource_id, credentials=None, config=None):
        if self.fail_update and credentials:
            raise RuntimeError("database down")
        row = self.rows.get(str(datasource_id))
        if row is None:
            return False
        if credentials is not None:
            row["credentials"] = dict(credentials)
        if config is not None:
            row["config"] = copy.deepcopy(config)
        return True

    async def record_security_event(self, **event):
        self.events.append(event)

    async def merge_user_cloud_identity(self, user_id, backend_id, entry):
        self.identities.append((user_id, backend_id, entry))
        return True

    def root(self, user_id: str = ANN) -> dict[str, Any]:
        [row] = [
            row
            for row in self.rows.values()
            if row["managed_key"] == f"user-cloud-root:{user_id}"
        ]
        return row


@pytest.fixture
def cloud() -> FakeCloud:
    return FakeCloud()


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


def _secrets_absent(*values: Any) -> None:
    text = repr(values)
    for secret in (TOKEN, TOKEN_2, STRANGER_TOKEN, POLL_TOKEN):
        assert secret not in text


async def _connected(store: FakeStore, cloud: FakeCloud, token: str = TOKEN) -> dict:
    router = FakeRouter(cloud)
    assert await cloud_user_root.ensure_user_root_connector(store, router, ANN) in (
        "created",
        "present",
    )
    outcome = await cloud_user_root.complete_connect(
        store, cloud, store.users[ANN], cloud.issue(token, "uid-ann")
    )
    assert outcome == "connected"
    return store.root()


# =============================================================================
# The row
# =============================================================================


class TestTheRow:
    @pytest.mark.asyncio
    async def test_one_row_per_user_linked_to_the_personal_project(self, store, cloud):
        router = FakeRouter(cloud)
        assert isinstance(cloud, SupportsUserRootGrant)

        assert await cloud_user_root.ensure_user_root_connector(store, router, ANN) == (
            "created"
        )
        assert await cloud_user_root.ensure_user_root_connector(
            store, router, store.users[ANN]
        ) == ("present")
        row = store.root()
        assert {
            key: row[key]
            for key in (
                "name",
                "type",
                "created_by",
                "scope_mode",
                "auto_attach",
                "project_ids",
                "read_only",
                "credentials",
            )
        } == {
            "name": "My cloud",
            "type": "cloud_folder",
            "created_by": ANN,
            "scope_mode": "projects",
            "auto_attach": True,
            "project_ids": [ANN_PROJECT],
            "read_only": None,
            "credentials": {},
        }
        assert row["config"] == {
            "folder_kind": "user_root",
            "project_id": ANN_PROJECT,
            "backend_id": "acme-cloud",
            "backend_instance_id": INSTANCE,
        }
        assert cloud_user_root.root_state(row) == "not_connected"

    @pytest.mark.asyncio
    async def test_nothing_without_a_provider_that_offers_roots(self, store, cloud):
        assert await cloud_user_root.ensure_user_root_connector(
            store, FakeRouter(NoRoots()), ANN
        ) == ("skipped_unsupported")
        cloud.grant_mode = "off"
        assert await cloud_user_root.ensure_user_root_connector(
            store, FakeRouter(cloud), ANN
        ) == ("skipped_unsupported")
        assert cloud_user_root.active_root_provider(FakeRouter(cloud)) == (
            None,
            "grant_mode_off",
        )
        assert store.rows == {}

    @pytest.mark.asyncio
    async def test_unapproved_and_projectless_users_get_none(self, store, cloud):
        router = FakeRouter(cloud)
        assert await cloud_user_root.ensure_user_root_connector(store, router, BOB) == (
            "skipped_unapproved"
        )
        store.users[ANN]["default_project_id"] = None
        assert await cloud_user_root.ensure_user_root_connector(store, router, ANN) == (
            "skipped_no_project"
        )

    @pytest.mark.asyncio
    async def test_the_backfill_counts_and_a_second_pass_changes_nothing(
        self, store, cloud, caplog
    ):
        router = FakeRouter(cloud)
        store.users[BOB]["is_approved"] = True
        with caplog.at_level(logging.INFO, logger=cloud_user_root.__name__):
            first = await cloud_user_root.backfill_user_root_connectors(store, router)
            second = await cloud_user_root.backfill_user_root_connectors(store, router)
        assert first["created"] == 2 and first["deferred"] == 0
        assert second == {**first, "created": 0, "present": 2}
        assert f"User cloud root connectors: {second}" in caplog.text

    @pytest.mark.asyncio
    async def test_the_backfill_on_a_provider_without_roots_does_nothing(self, store):
        counts = await cloud_user_root.backfill_user_root_connectors(
            store, FakeRouter(NoRoots())
        )
        assert set(counts.values()) == {0} and store.rows == {}

    @pytest.mark.asyncio
    async def test_a_new_installation_repoints_and_revokes_the_old_credential(
        self, store, cloud
    ):
        await _connected(store, cloud)
        replacement = FakeCloud(OTHER_INSTANCE)
        router = FakeRouter(replacement, cloud)

        assert await cloud_user_root.ensure_user_root_connector(store, router, ANN) == (
            "repointed"
        )
        row = store.root()
        assert row["credentials"] == {}
        assert row["config"] == {
            "folder_kind": "user_root",
            "project_id": ANN_PROJECT,
            "backend_id": "acme-cloud",
            "backend_instance_id": OTHER_INSTANCE,
        }
        assert cloud.revoked == [TOKEN]
        assert cloud_user_root.root_state(row) == "not_connected"
        assert await cloud_user_root.ensure_user_root_connector(store, router, ANN) == (
            "present"
        )


# =============================================================================
# Connect
# =============================================================================


class TestCompleteConnect:
    @pytest.mark.asyncio
    async def test_the_users_own_account_is_stored_and_named(self, store, cloud):
        row = await _connected(store, cloud)

        assert row["credentials"] == {"username": "uid-ann", "password": TOKEN}
        config = row["config"]
        assert config["account_id"] == "uid-ann"
        assert config["folder_handle"] == cloud.user_root_handle("uid-ann").to_db()
        assert config["grant"]["state"] == "connected"
        assert config["grant"]["source"] == "login_flow"
        assert config["grant"]["client_name"] == "Superhuman Remote Worker"
        assert cloud_user_root.root_state(row) == "connected"
        assert store.identities == [
            (ANN, "acme-cloud", store.identities[0][2]),
        ]
        assert store.identities[0][2]["user_id"] == "uid-ann"
        assert [event["event_type"] for event in store.events] == ["cloud_root_granted"]
        assert cloud.revoked == []
        status = cloud_user_root.public_status(
            await store.get_datasource_by_managed_key(f"user-cloud-root:{ANN}")
        )
        assert status["state"] == "connected" and status["account_id"] == "uid-ann"
        _secrets_absent(config, store.events, status)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("account", "reason"),
        [
            ("uid-eve", "wrong_account"),
            ("uid-root", "admin_account"),
            ("uid-off", "account_disabled"),
        ],
    )
    async def test_another_account_is_refused_and_its_credential_revoked(
        self, store, cloud, account, reason
    ):
        router = FakeRouter(cloud)
        await cloud_user_root.ensure_user_root_connector(store, router, ANN)
        grant = cloud.issue(STRANGER_TOKEN, account)

        assert (
            await cloud_user_root.complete_connect(
                store, cloud, store.users[ANN], grant
            )
            == reason
        )
        row = store.root()
        assert row["credentials"] == {}
        assert row["config"]["connect_failure"]["reason"] == reason
        assert "account_id" not in row["config"]
        assert cloud.revoked == [STRANGER_TOKEN]
        assert cloud_user_root.root_state(row) == "failed"
        [event] = store.events
        assert event["event_type"] == "cloud_root_rejected"
        assert reason in event["detail"]
        _secrets_absent(row["config"], store.events)

    @pytest.mark.asyncio
    async def test_a_user_without_an_email_matches_no_account(self, store, cloud):
        await cloud_user_root.ensure_user_root_connector(store, FakeRouter(cloud), ANN)
        store.users[ANN]["email"] = None
        grant = cloud.issue(TOKEN, "uid-ann")
        assert (
            await cloud_user_root.complete_connect(
                store, cloud, store.users[ANN], grant
            )
            == "wrong_account"
        )

    @pytest.mark.asyncio
    async def test_a_credential_that_cannot_read_the_files_is_refused(
        self, store, cloud
    ):
        await cloud_user_root.ensure_user_root_connector(store, FakeRouter(cloud), ANN)
        cloud.verify_answer = "missing"
        grant = cloud.issue(TOKEN, "uid-ann")
        assert (
            await cloud_user_root.complete_connect(
                store, cloud, store.users[ANN], grant
            )
            == "verify_failed"
        )
        assert cloud.revoked == [TOKEN]

    @pytest.mark.asyncio
    async def test_a_store_failure_revokes_the_new_credential(self, store, cloud):
        await cloud_user_root.ensure_user_root_connector(store, FakeRouter(cloud), ANN)
        store.fail_update = True
        grant = cloud.issue(TOKEN, "uid-ann")
        assert (
            await cloud_user_root.complete_connect(
                store, cloud, store.users[ANN], grant
            )
            == "error"
        )
        assert cloud.revoked == [TOKEN]
        assert store.root()["credentials"] == {}

    @pytest.mark.asyncio
    async def test_a_reconnect_revokes_the_previous_credential_after_storing(
        self, store, cloud
    ):
        await _connected(store, cloud)
        grant = cloud.issue(TOKEN_2, "uid-ann")
        assert (
            await cloud_user_root.complete_connect(
                store, cloud, store.users[ANN], grant
            )
            == "connected"
        )
        assert store.root()["credentials"]["password"] == TOKEN_2
        assert cloud.revoked == [TOKEN]

    @pytest.mark.asyncio
    async def test_a_failed_reconnect_keeps_the_working_connection(self, store, cloud):
        await _connected(store, cloud)
        grant = cloud.issue(STRANGER_TOKEN, "uid-eve")
        await cloud_user_root.complete_connect(store, cloud, store.users[ANN], grant)
        row = store.root()
        assert row["credentials"]["password"] == TOKEN
        status = cloud_user_root.public_status(row)
        assert status["state"] == "connected"
        assert status["failure"]["reason"] == "wrong_account"


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_a_disconnect_revokes_and_forgets(self, store, cloud):
        await _connected(store, cloud)
        flows = cloud_user_root.CloudRootConnectFlows()
        body = await cloud_user_root.disconnect(
            store, FakeRouter(cloud), flows, store.users[ANN]
        )
        assert body["state"] == "revoked"
        assert body["revoked_reason"] == "user"
        assert body["provider_revoked"] is True
        assert cloud.revoked == [TOKEN]
        assert store.root()["credentials"] == {}
        assert [event["event_type"] for event in store.events][-1] == (
            "cloud_root_revoked"
        )
        _secrets_absent(body, store.events)

    @pytest.mark.asyncio
    async def test_a_retirement_revokes_the_credential_first(self, store, cloud):
        await _connected(store, cloud)
        await cloud_user_root.revoke_for_retirement(store, FakeRouter(cloud), ANN)
        assert cloud.revoked == [TOKEN]


# =============================================================================
# The flow's poller
# =============================================================================


class TestConnectFlows:
    @pytest.mark.asyncio
    async def test_a_finished_flow_connects(self, store, cloud):
        router = FakeRouter(cloud)
        flows = cloud_user_root.CloudRootConnectFlows(poll_seconds=0, flow_seconds=5)
        cloud.polls = [
            None,
            CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE, "down", backend="acme-cloud"
            ),
            cloud.issue(TOKEN, "uid-ann"),
        ]
        flow = await flows.start(store, router, store.users[ANN])
        assert flow.login_url == "https://cloud.example/login/v2/flow/abc"
        status = await cloud_user_root.status(store, router, flows, store.users[ANN])
        assert status["state"] in ("connecting", "connected")
        _secrets_absent(flow, status)

        await asyncio.wait_for(flow.task, 5)
        status = await cloud_user_root.status(store, router, flows, store.users[ANN])
        assert status["state"] == "connected"
        assert flows.active(ANN) is None

    @pytest.mark.asyncio
    async def test_a_new_flow_cancels_the_last_and_cancel_stops_it(self, store, cloud):
        router = FakeRouter(cloud)
        flows = cloud_user_root.CloudRootConnectFlows(
            poll_seconds=0.01, flow_seconds=60
        )
        first = await flows.start(store, router, store.users[ANN])
        second = await flows.start(store, router, store.users[ANN])
        await asyncio.sleep(0)
        assert first.task.cancelled() or first.task.done()
        assert flows.active(ANN) is second
        status = await cloud_user_root.status(store, router, flows, store.users[ANN])
        assert status["state"] == "connecting"
        assert status["connecting"]["login_url"] == second.login_url
        assert await flows.cancel(ANN) is True
        assert flows.active(ANN) is None
        assert await flows.cancel(ANN) is False
        assert "connect_failure" not in store.root()["config"]

    @pytest.mark.asyncio
    async def test_an_unfinished_flow_expires(self, store, cloud):
        flows = cloud_user_root.CloudRootConnectFlows(
            poll_seconds=0.01, flow_seconds=0.05
        )
        flow = await flows.start(store, FakeRouter(cloud), store.users[ANN])
        await asyncio.wait_for(flow.task, 5)
        assert store.root()["config"]["connect_failure"]["reason"] == "expired"

    @pytest.mark.asyncio
    async def test_no_root_no_flow(self, store, cloud):
        flows = cloud_user_root.CloudRootConnectFlows()
        with pytest.raises(LookupError, match="provider_unsupported"):
            await flows.start(store, FakeRouter(NoRoots()), store.users[ANN])
        store.users[ANN]["default_project_id"] = None
        with pytest.raises(LookupError, match="no_personal_project"):
            await flows.start(store, FakeRouter(cloud), store.users[ANN])
        assert cloud.started == 0

    @pytest.mark.asyncio
    async def test_drain_cancels_every_flow(self, store, cloud):
        flows = cloud_user_root.CloudRootConnectFlows(poll_seconds=10, flow_seconds=60)
        flow = await flows.start(store, FakeRouter(cloud), store.users[ANN])
        await flows.drain()
        assert flow.task.cancelled()
        assert flows.active(ANN) is None


# =============================================================================
# Mounting
# =============================================================================


class TestUserRootMount:
    @pytest.mark.asyncio
    async def test_a_connected_root_mounts_with_its_own_credential(self, store, cloud):
        row = await _connected(store, cloud)
        built = await cloud_user_root.user_root_mount(
            row, owner_id=ANN, store=store, cloud_router=FakeRouter(cloud)
        )
        mount_row, credential = built
        assert mount_row == {
            "id": row["id"],
            "source_kind": "cloud_folder",
            "source_ref": row["id"],
            "mount_kind": "user_root",
            "target_path": "cloud/my_cloud",
            "backend_id": "acme-cloud",
            "backend_instance_id": INSTANCE,
            "cloud_handle": cloud.user_root_handle("uid-ann").to_db(),
        }
        assert (credential.login_name, credential.credential) == ("uid-ann", TOKEN)
        _secrets_absent(mount_row, credential)

    @pytest.mark.asyncio
    async def test_what_a_root_mounts_nothing_for(self, store, cloud):
        router = FakeRouter(cloud)
        await cloud_user_root.ensure_user_root_connector(store, router, ANN)
        row = store.root()

        async def refused(**kwargs):
            built = await cloud_user_root.user_root_mount(
                row, store=store, cloud_router=router, **kwargs
            )
            assert isinstance(built, CloudFolderUnavailable)
            return built.reason

        assert await refused(owner_id=BOB) == "not_eligible"
        assert await refused(owner_id=None) == "not_eligible"
        assert await refused(owner_id=ANN) == "cloud_not_connected"
        await cloud_user_root.complete_connect(
            store, cloud, store.users[ANN], cloud.issue(TOKEN, "uid-ann")
        )
        await cloud_user_root.revoke_root_credential(
            store, router, store.root(), reason="rejected_by_cloud", revoke_remote=False
        )
        row = store.root()
        assert await refused(owner_id=ANN) == "cloud_grant_revoked"
        cloud.grant_mode = "off"
        assert await refused(owner_id=ANN) == "access_unavailable"


class TestProbeBeforeAPod:
    @pytest.mark.asyncio
    async def test_a_refused_credential_revokes_the_row(self, store, cloud):
        row = await _connected(store, cloud)
        cloud.issued.clear()  # revoked in the cloud's device list
        thread = {"id": "t1", "user_id": ANN}
        outcome = await cloud_user_root.verify_selected_user_root(
            thread,
            selected_ids=[row["id"]],
            store=store,
            cloud_router=FakeRouter(cloud),
        )
        assert outcome == "rejected"
        row = store.root()
        assert row["credentials"] == {}
        assert row["config"]["grant"]["state"] == "revoked"
        assert row["config"]["grant"]["reason"] == "rejected_by_cloud"
        assert cloud.revoked == []  # never asked to revoke a dead credential
        assert cloud_user_root.root_state(row) == "revoked"
        assert store.events[-1]["event_type"] == "cloud_root_revoked"

    @pytest.mark.asyncio
    async def test_an_account_without_files_is_not_connected_any_more(
        self, store, cloud
    ):
        row = await _connected(store, cloud)
        cloud.verify_answer = "missing"
        thread = {"id": "t1", "user_id": ANN}
        assert (
            await cloud_user_root.verify_selected_user_root(
                thread,
                selected_ids=[row["id"]],
                store=store,
                cloud_router=FakeRouter(cloud),
            )
            == "missing"
        )
        row = store.root()
        assert row["credentials"] == {}
        assert row["config"]["connect_failure"]["reason"] == "account_missing"
        assert cloud_user_root.root_state(row) == "failed"

    @pytest.mark.asyncio
    async def test_only_a_selected_connected_root_is_probed(self, store, cloud):
        row = await _connected(store, cloud)
        thread = {"id": "t1", "user_id": ANN}
        router = FakeRouter(cloud)
        assert (
            await cloud_user_root.verify_selected_user_root(
                thread, selected_ids=[], store=store, cloud_router=router
            )
            is None
        )
        assert (
            await cloud_user_root.verify_selected_user_root(
                thread, selected_ids=[row["id"]], store=store, cloud_router=router
            )
            == "ok"
        )
        assert store.root()["credentials"]["password"] == TOKEN


class TestStatus:
    @pytest.mark.asyncio
    async def test_unsupported_says_why(self, store, cloud):
        flows = cloud_user_root.CloudRootConnectFlows()
        body = await cloud_user_root.status(
            store, FakeRouter(NoRoots()), flows, store.users[ANN]
        )
        assert (body["supported"], body["state"], body["reason"]) == (
            False,
            "unsupported",
            "provider_unsupported",
        )
        store.users[ANN]["default_project_id"] = None
        body = await cloud_user_root.status(
            store, FakeRouter(cloud), flows, store.users[ANN]
        )
        assert body["reason"] == "no_personal_project"

    @pytest.mark.asyncio
    async def test_the_status_read_creates_the_row_lazily(self, store, cloud):
        flows = cloud_user_root.CloudRootConnectFlows()
        body = await cloud_user_root.status(
            store, FakeRouter(cloud), flows, {"id": ANN}
        )
        assert body["supported"] is True
        assert body["state"] == "not_connected"
        assert body["connector_id"] == store.root()["id"]
