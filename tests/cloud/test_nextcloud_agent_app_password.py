"""agent-service signs in with an app password (bundled Nextcloud performance,
WP1b).

The adapter adopts the stored app password, or mints one with the login
password and stores it encrypted; a 401 swaps in another replica's or a new
one and re-sends the request once; anything that fails leaves the login
password in use. Against a fake Nextcloud behind ``httpx.MockTransport`` and
an in-memory store with the database's compare-and-set semantics.
"""

from __future__ import annotations

import base64
import logging
from types import SimpleNamespace
from typing import Any, Optional

import httpx
import pytest

from orchestrator.security import crypto
from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    SessionFolderHandle,
)
from orchestrator.services.cloud import service_credentials
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.instance_registry import (
    adopt_main_cloud_service_credentials,
)
from orchestrator.services.cloud.nextcloud import NextcloudBackend
from orchestrator.services.cloud.nextcloud_agent_credential import (
    AGENT_CLIENT_USER_AGENT,
    app_password_name,
)

BASE = "http://nextcloud.internal"
INSTANCE = "77777777-7777-4777-8777-777777777777"
AGENT_USER = "agent-service"
LOGIN_PASSWORD = "agent-login-password"
ADMIN_PASSWORD = "admin-login-password"
SESSION = SessionFolderHandle(backend="nextcloud", native_id="sessions/s-1")


def _settings() -> NextcloudSettings:
    return NextcloudSettings(
        base_url=BASE,
        public_url="https://cloud.example",
        admin_user="admin",
        admin_password=ADMIN_PASSWORD,
        agent_user=AGENT_USER,
        agent_password=LOGIN_PASSWORD,
    )


def _credentials(request: httpx.Request) -> tuple[Optional[str], Optional[str]]:
    header = request.headers.get("authorization", "")
    if not header.startswith("Basic "):
        return None, None
    user, _, secret = base64.b64decode(header[6:]).decode().partition(":")
    return user, secret


_COOKIES = [
    ("set-cookie", "cookie_test=test; path=/"),
    ("set-cookie", "oc_sessionPassphrase=x; path=/; secure"),
]


class FakeNextcloud:
    """agent-service's login password, its app passwords, and the admin.

    Every request is recorded as it arrived (``seen``), tagged with the
    client it came through: ``admin`` (``_client``), ``agent``
    (``_agent_client``) or ``secret`` (the mint and revoke clients).
    """

    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        self.live: dict[str, bool] = {}
        self.minted: list[str] = []
        self.revoked: list[str] = []
        self.mint_status = 200

    def transport(self, tag: str) -> httpx.MockTransport:
        return httpx.MockTransport(lambda request: self.handle(tag, request))

    def handle(self, tag: str, request: httpx.Request) -> httpx.Response:
        user, secret = _credentials(request)
        self.seen.append(
            {
                "tag": tag,
                "method": request.method,
                "path": request.url.path,
                "user": user,
                "secret": secret,
                "cookie": request.headers.get("cookie"),
                "user_agent": request.headers.get("user-agent"),
                "ocs": request.headers.get("ocs-apirequest"),
                "content": request.content,
            }
        )
        path = request.url.path
        if path == "/ocs/v2.php/core/getapppassword":
            if self.mint_status != 200:
                return httpx.Response(self.mint_status, headers=_COOKIES)
            if (user, secret) != (AGENT_USER, LOGIN_PASSWORD):
                return httpx.Response(401, headers=_COOKIES)
            token = f"app-password-{len(self.minted) + 1}"
            self.minted.append(token)
            self.live[token] = True
            return httpx.Response(
                200,
                json={
                    "ocs": {"meta": {"statuscode": 200}, "data": {"apppassword": token}}
                },
                headers=_COOKIES,
            )
        if path == "/ocs/v2.php/core/apppassword" and request.method == "DELETE":
            if user != AGENT_USER or not self.live.get(secret or ""):
                return httpx.Response(401)
            self.live[secret] = False
            self.revoked.append(secret)
            return httpx.Response(200, json={"ocs": {"data": []}})
        if user == "admin":
            ok = secret == ADMIN_PASSWORD
        else:
            ok = user == AGENT_USER and (
                secret == LOGIN_PASSWORD or self.live.get(secret or "", False)
            )
        if not ok:
            return httpx.Response(401, headers=_COOKIES)
        status = {"PUT": 201, "MKCOL": 201, "PROPFIND": 207, "DELETE": 204}.get(
            request.method, 200
        )
        return httpx.Response(
            status,
            json={"ocs": {"meta": {"statuscode": 100}, "data": {"users": {}}}},
            headers=_COOKIES,
        )

    def requests(self, tag: str, method: str | None = None) -> list[dict[str, Any]]:
        return [
            seen
            for seen in self.seen
            if seen["tag"] == tag and (method is None or seen["method"] == method)
        ]

    def mints(self) -> list[dict[str, Any]]:
        return [s for s in self.seen if s["path"] == "/ocs/v2.php/core/getapppassword"]


class MemoryStore:
    """``main_cloud_service_credentials`` with the database's semantics."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.before_put = None
        self.fail_reads = False

    def seed(self, secret: str, *, revision: int = 1) -> None:
        self.rows[(INSTANCE, AGENT_USER)] = {
            "credential_ciphertext": crypto.encrypt(secret),
            "token_name": app_password_name(INSTANCE),
            "revision": revision,
        }

    def row(self) -> Optional[dict[str, Any]]:
        return self.rows.get((INSTANCE, AGENT_USER))

    async def get_main_cloud_service_credential(
        self, backend_instance_id: str, login_name: str
    ) -> Optional[dict[str, Any]]:
        if self.fail_reads:
            raise ConnectionError("database unavailable")
        row = self.rows.get((backend_instance_id, login_name))
        return dict(row) if row is not None else None

    async def put_main_cloud_service_credential(
        self,
        backend_instance_id: str,
        login_name: str,
        *,
        ciphertext: str,
        token_name: str,
        expected_revision: int | None,
    ) -> bool:
        if self.before_put is not None:
            hook, self.before_put = self.before_put, None
            hook()
        key = (backend_instance_id, login_name)
        row = self.rows.get(key)
        if expected_revision is None:
            if row is not None:
                return False
            revision = 1
        elif row is None or row["revision"] != expected_revision:
            return False
        else:
            revision = expected_revision + 1
        self.rows[key] = {
            "credential_ciphertext": ciphertext,
            "token_name": token_name,
            "revision": revision,
        }
        return True


@pytest.fixture(autouse=True)
def _encryption_key(monkeypatch):
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "K" * 32)
    crypto.reset_cipher_cache()
    yield
    crypto.reset_cipher_cache()


@pytest.fixture
def store(monkeypatch) -> MemoryStore:
    memory = MemoryStore()
    monkeypatch.setitem(service_credentials._state, "store", memory)
    return memory


@pytest.fixture
def fake() -> FakeNextcloud:
    return FakeNextcloud()


def _backend(fake: FakeNextcloud, *, bind: bool = True) -> NextcloudBackend:
    backend = NextcloudBackend(_settings())
    backend._client = httpx.AsyncClient(
        base_url=BASE,
        auth=("admin", ADMIN_PASSWORD),
        transport=fake.transport("admin"),
    )
    backend._agent_client = backend._build_agent_client(fake.transport("agent"))
    backend._agent_secret_transport = fake.transport("secret")
    backend._initialized = True
    if bind:
        backend.bind_backend_instance(INSTANCE)
    return backend


async def _put(backend: NextcloudBackend, content: bytes = b"report") -> None:
    await backend.put_session_file(SESSION, path="report.md", content=content)


class TestAdoption:
    @pytest.mark.asyncio
    async def test_a_stored_app_password_is_adopted_without_minting(self, store, fake):
        store.seed("stored-app-password", revision=3)
        fake.live["stored-app-password"] = True
        backend = _backend(fake)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.mints() == []
        [put] = fake.requests("agent", "PUT")
        assert (put["user"], put["secret"]) == (AGENT_USER, "stored-app-password")
        assert backend._agent_secret_kind == "app_password"
        assert backend._agent_secret_revision == 3

    @pytest.mark.asyncio
    async def test_without_a_row_one_is_minted_with_the_login_password(
        self, store, fake
    ):
        backend = _backend(fake)
        try:
            await _put(backend)
            await _put(backend)
        finally:
            await backend.close()

        [mint] = fake.mints()
        assert mint["tag"] == "secret"
        assert (mint["user"], mint["secret"]) == (AGENT_USER, LOGIN_PASSWORD)
        # Nextcloud names the token after the User-Agent: one deterministic
        # name per installation.
        assert mint["user_agent"] == f"SuperhumanRemoteWorker agent-service {INSTANCE}"
        assert mint["ocs"] == "true"
        assert mint["cookie"] is None
        row = store.row()
        assert row is not None
        assert row["revision"] == 1
        assert row["token_name"] == app_password_name(INSTANCE)
        assert row["credential_ciphertext"] != "app-password-1"
        assert row["credential_ciphertext"].startswith("v1:")
        assert "app-password-1" not in row["credential_ciphertext"]
        assert crypto.decrypt(row["credential_ciphertext"]) == "app-password-1"
        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == ["app-password-1", "app-password-1"]

    @pytest.mark.asyncio
    async def test_a_lost_insert_adopts_the_winner_and_revokes_its_own(
        self, store, fake
    ):
        def other_replica_inserts_first() -> None:
            fake.live["winner-app-password"] = True
            store.seed("winner-app-password")

        store.before_put = other_replica_inserts_first
        backend = _backend(fake)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.minted == ["app-password-1"]
        assert fake.revoked == ["app-password-1"]
        [revoke] = [s for s in fake.seen if s["method"] == "DELETE"]
        assert revoke["path"] == "/ocs/v2.php/core/apppassword"
        assert (revoke["user"], revoke["secret"]) == (AGENT_USER, "app-password-1")
        assert revoke["ocs"] == "true"
        assert revoke["cookie"] is None
        [put] = fake.requests("agent", "PUT")
        assert put["secret"] == "winner-app-password"
        assert crypto.decrypt(store.row()["credential_ciphertext"]) == (
            "winner-app-password"
        )

    @pytest.mark.asyncio
    async def test_ensure_service_credentials_adopts_without_a_request(
        self, store, fake
    ):
        backend = _backend(fake)
        try:
            assert backend.webdav_credentials["password"] == LOGIN_PASSWORD
            await backend.ensure_service_credentials()
            await backend.ensure_service_credentials()
        finally:
            await backend.close()

        assert fake.minted == ["app-password-1"]
        assert fake.requests("agent") == []
        assert backend.webdav_credentials == {
            "username": AGENT_USER,
            "password": "app-password-1",
        }

    @pytest.mark.asyncio
    async def test_startup_adopts_for_the_active_adapter_only_when_it_has_one(
        self, store, fake
    ):
        backend = _backend(fake)
        try:
            await adopt_main_cloud_service_credentials(SimpleNamespace(active=backend))
            await adopt_main_cloud_service_credentials(SimpleNamespace(active=object()))
        finally:
            await backend.close()

        assert backend._agent_secret_kind == "app_password"


class TestAgentsReceiveTheAppPassword:
    @pytest.mark.asyncio
    async def test_sync_config_and_mount_spec_carry_the_adopted_app_password(
        self, store, fake
    ):
        backend = _backend(fake)
        try:
            await backend.ensure_service_credentials()
            url = backend.get_session_folder_webdav_url(SESSION)
            sync = backend.cloud_sync_config(url)
            spec = await backend.build_rclone_mount_spec(
                handle=SESSION,
                mount_kind="session_folder",
                target_path="/workspace/cloud",
                access="read_write",
            )
        finally:
            await backend.close()

        assert sync["auth"] == {
            "type": "basic",
            "username": AGENT_USER,
            "password": "app-password-1",
        }
        assert spec.source_config["user"] == AGENT_USER
        assert spec.auth == {"type": "basic", "password": "app-password-1"}


class TestRefreshOn401:
    @pytest.mark.asyncio
    async def test_a_refused_app_password_is_replaced_and_the_put_resent_once(
        self, store, fake
    ):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)
        try:
            await backend.ensure_service_credentials()
            # The password changed, or the token was revoked: Nextcloud
            # refuses it from now on.
            fake.live["old-app-password"] = False
            await _put(backend, b"the same body")
        finally:
            await backend.close()

        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == ["old-app-password", "app-password-1"]
        assert [p["content"] for p in puts] == [b"the same body", b"the same body"]
        assert [p["path"] for p in puts] == [puts[0]["path"]] * 2
        [mint] = fake.mints()
        assert mint["secret"] == LOGIN_PASSWORD
        row = store.row()
        assert row["revision"] == 2
        assert crypto.decrypt(row["credential_ciphertext"]) == "app-password-1"
        # The replaced token is not revoked (agents may still hold it).
        assert fake.revoked == []

    @pytest.mark.asyncio
    async def test_another_replicas_new_token_is_adopted_without_minting(
        self, store, fake
    ):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)
        try:
            await backend.ensure_service_credentials()
            fake.live["old-app-password"] = False
            fake.live["other-replicas-app-password"] = True
            store.seed("other-replicas-app-password", revision=2)
            await _put(backend)
        finally:
            await backend.close()

        assert fake.mints() == []
        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == [
            "old-app-password",
            "other-replicas-app-password",
        ]
        assert backend._agent_secret_revision == 2

    @pytest.mark.asyncio
    async def test_a_lost_compare_and_set_adopts_the_winner(self, store, fake):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)

        def other_replica_replaces_first() -> None:
            fake.live["winner-app-password"] = True
            store.seed("winner-app-password", revision=2)

        try:
            await backend.ensure_service_credentials()
            fake.live["old-app-password"] = False
            store.before_put = other_replica_replaces_first
            await _put(backend)
        finally:
            await backend.close()

        assert fake.revoked == ["app-password-1"]
        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == [
            "old-app-password",
            "winner-app-password",
        ]

    @pytest.mark.asyncio
    async def test_a_second_401_is_returned_not_looped(self, store, fake):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)
        refused = {"old-app-password", "app-password-1"}
        handle = fake.handle

        def refuse_dav(tag: str, request: httpx.Request) -> httpx.Response:
            response = handle(tag, request)
            if tag == "agent" and _credentials(request)[1] in refused:
                return httpx.Response(401)
            return response

        fake.handle = refuse_dav
        try:
            await backend.ensure_service_credentials()
            with pytest.raises(CloudBackendError) as ei:
                await _put(backend)
        finally:
            await backend.close()

        assert ei.value.kind is CloudBackendErrorKind.AUTHENTICATION_FAILED
        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == ["old-app-password", "app-password-1"]

    @pytest.mark.asyncio
    async def test_a_401_on_the_login_password_is_returned_at_once(
        self, monkeypatch, fake
    ):
        monkeypatch.setitem(service_credentials._state, "store", None)
        backend = _backend(fake)
        backend._agent_password = "a-wrong-login-password"
        try:
            with pytest.raises(CloudBackendError):
                await _put(backend)
        finally:
            await backend.close()

        assert len(fake.requests("agent", "PUT")) == 1
        assert fake.mints() == []

    @pytest.mark.asyncio
    async def test_a_failed_replacement_falls_back_to_the_login_password(
        self, store, fake
    ):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)
        try:
            await backend.ensure_service_credentials()
            fake.live["old-app-password"] = False
            fake.mint_status = 503
            await _put(backend)
            await _put(backend)
        finally:
            await backend.close()

        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == [
            "old-app-password",
            LOGIN_PASSWORD,
            LOGIN_PASSWORD,
        ]
        assert len(fake.mints()) == 1
        assert backend._agent_secret_kind == "password"
        assert store.row()["revision"] == 1

    @pytest.mark.asyncio
    async def test_the_refused_revision_is_replaced_at_the_next_adoption(
        self, store, fake
    ):
        store.seed("old-app-password")
        fake.live["old-app-password"] = True
        backend = _backend(fake)
        try:
            await backend.ensure_service_credentials()
            fake.live["old-app-password"] = False
            fake.mint_status = 503
            await _put(backend)
            fake.mint_status = 200
            # The retry window and the mint interval have passed.
            backend._agent_adopt_not_before = 0.0
            backend._agent_mint_not_before = 0.0
            await _put(backend)
        finally:
            await backend.close()

        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == [
            "old-app-password",
            LOGIN_PASSWORD,
            "app-password-1",
        ]
        assert store.row()["revision"] == 2


class TestLoginPasswordFallback:
    @pytest.mark.asyncio
    async def test_without_a_store_the_login_password_is_used(self, monkeypatch, fake):
        monkeypatch.setitem(service_credentials._state, "store", None)
        backend = _backend(fake)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.mints() == []
        [put] = fake.requests("agent", "PUT")
        assert put["secret"] == LOGIN_PASSWORD

    @pytest.mark.asyncio
    async def test_before_the_instance_is_bound_the_login_password_is_used(
        self, store, fake
    ):
        backend = _backend(fake, bind=False)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.mints() == []
        assert store.row() is None
        [put] = fake.requests("agent", "PUT")
        assert put["secret"] == LOGIN_PASSWORD

    @pytest.mark.parametrize("status", [401, 403, 500])
    @pytest.mark.asyncio
    async def test_a_failed_mint_keeps_the_login_password_and_waits(
        self, store, fake, status
    ):
        fake.mint_status = status
        backend = _backend(fake)
        try:
            await _put(backend)
            await _put(backend)
        finally:
            await backend.close()

        # One attempt; the second request does not try again so soon.
        assert len(fake.mints()) == 1
        assert store.row() is None
        puts = fake.requests("agent", "PUT")
        assert [p["secret"] for p in puts] == [LOGIN_PASSWORD, LOGIN_PASSWORD]
        assert backend.webdav_credentials["password"] == LOGIN_PASSWORD

    @pytest.mark.asyncio
    async def test_an_unavailable_store_keeps_the_login_password(self, store, fake):
        store.fail_reads = True
        backend = _backend(fake)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.mints() == []
        [put] = fake.requests("agent", "PUT")
        assert put["secret"] == LOGIN_PASSWORD

    @pytest.mark.asyncio
    async def test_an_undecryptable_row_is_replaced(self, store, fake):
        store.rows[(INSTANCE, AGENT_USER)] = {
            "credential_ciphertext": "v1:AAAAAAAAAAAAAAAA:AAAA",
            "token_name": app_password_name(INSTANCE),
            "revision": 4,
        }
        backend = _backend(fake)
        try:
            await _put(backend)
        finally:
            await backend.close()

        assert fake.minted == ["app-password-1"]
        assert store.row()["revision"] == 5
        [put] = fake.requests("agent", "PUT")
        assert put["secret"] == "app-password-1"


class TestClientSeparation:
    @pytest.mark.asyncio
    async def test_the_admin_client_never_carries_agent_service_credentials(
        self, store, fake
    ):
        backend = _backend(fake)
        try:
            await _put(backend)
            await backend.remove_user_from_group("someone", "grp1")
            await backend.delete_session_folder(SESSION)
            await backend.remove_user_from_group("someone", "grp2")
        finally:
            await backend.close()

        admin = fake.requests("admin")
        assert len(admin) == 2
        assert {(s["user"], s["secret"]) for s in admin} == {("admin", ADMIN_PASSWORD)}
        agent = fake.requests("agent")
        assert {s["user"] for s in agent} == {AGENT_USER}
        assert {s["secret"] for s in agent} == {"app-password-1"}
        assert {s["user_agent"] for s in agent} == {AGENT_CLIENT_USER_AGENT}
        assert {s["ocs"] for s in agent} == {"true"}

    @pytest.mark.asyncio
    async def test_the_agent_client_sends_no_cookie(self, store, fake):
        backend = _backend(fake)
        try:
            for _ in range(3):
                await _put(backend)
        finally:
            await backend.close()

        assert [s["cookie"] for s in fake.seen] == [None] * len(fake.seen)
        assert len(fake.requests("agent", "PUT")) == 3

    @pytest.mark.asyncio
    async def test_close_closes_the_agent_client(self, store, fake):
        backend = _backend(fake)
        agent_client = backend._agent_client
        await backend.close()
        assert agent_client is not None and agent_client.is_closed
        assert backend._agent_client is None


class TestNoSecretInLogs:
    @pytest.mark.asyncio
    async def test_mint_race_refresh_and_failures_log_no_secret(
        self, store, fake, caplog
    ):
        caplog.set_level(logging.DEBUG)
        secrets = {LOGIN_PASSWORD, ADMIN_PASSWORD, "old-app-password"}

        # A failed mint; then a lost insert; then a 401 whose replacement is
        # refused (minted too recently), which falls back to the login
        # password.
        fake.mint_status = 401
        first = _backend(fake)
        try:
            await _put(first)
        finally:
            await first.close()
        fake.mint_status = 200

        def other_replica_inserts_first() -> None:
            fake.live["old-app-password"] = True
            store.seed("old-app-password")

        store.before_put = other_replica_inserts_first
        second = _backend(fake)
        try:
            await _put(second)
            fake.live["old-app-password"] = False
            await _put(second)
        finally:
            await second.close()

        assert fake.revoked == ["app-password-1"]
        assert second._agent_secret_kind == "password"

        secrets.update(fake.minted)
        secrets.update(row["credential_ciphertext"] for row in store.rows.values())
        text = caplog.text
        assert "Basic " not in text
        assert "v1:" not in text
        for secret in secrets:
            assert secret not in text
        assert "no app password" in text  # the failures were logged
