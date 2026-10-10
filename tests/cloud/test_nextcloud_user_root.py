"""Nextcloud's half of the user-root grant (main cloud slice 4).

An httpx.MockTransport fake speaks Login Flow v2 (init, poll), the current
user and account endpoints, a PROPFIND of the account's root and the app
password's self-revoke. Pinned: the flow is started with the device name as
User-Agent and polled on the internal URL; the page the user opens is the
public one; a poll answers nothing until the user finished; the probe maps
207/401/404/errors; every call made with the user's credential uses its
login name, never a cookie of the administrator's session, and no
credential reaches an error or a ``repr``.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, unquote

import httpx
import pytest

from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.nextcloud_user_root import (
    CLIENT_NAME,
    GRANT_MODE_ENV,
    NextcloudUserRootGrant,
)
from orchestrator.services.cloud.user_root_grant import (
    CloudAccount,
    LoginFlowGrant,
    SupportsUserRootGrant,
)

NC_BASE = "http://srw-nextcloud"
NC_PUBLIC = "https://cloud.example.com"
UID = "a" * 64
LOGIN = "ann"
TOKEN = "app-password-SECRET"
POLL = "poll-token-SECRET"


class FakeNc:
    def __init__(self) -> None:
        self.log: list[httpx.Request] = []
        self.finished = False
        self.valid = {TOKEN}
        self.accounts: dict[str, dict[str, Any]] = {
            UID: {
                "id": UID,
                "email": "ann@example.com",
                "groups": ["staff"],
                "enabled": True,
            }
        }
        self.fail: dict[str, httpx.Response | Exception] = {}

    def _user_auth(self, request: httpx.Request) -> str | None:
        for token in self.valid:
            if (
                request.headers.get("authorization")
                == httpx.BasicAuth(LOGIN, token)._auth_header
            ):
                return token
        return None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.log.append(request)
        path = unquote(request.url.path)
        if path in self.fail:
            failure = self.fail[path]
            if isinstance(failure, Exception):
                raise failure
            return failure
        if path == "/index.php/login/v2" and request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "poll": {"token": POLL, "endpoint": f"{NC_PUBLIC}/login/v2/poll"},
                    # Nextcloud answers with the host it was asked on.
                    "login": "https://srw-nextcloud/login/v2/flow/LOGIN-TOKEN",
                },
            )
        if path == "/index.php/login/v2/poll" and request.method == "POST":
            form = parse_qs(request.content.decode())
            assert form == {"token": [POLL]}
            if not self.finished:
                return httpx.Response(404, json=[])
            return httpx.Response(
                200,
                json={"server": NC_PUBLIC, "loginName": LOGIN, "appPassword": TOKEN},
            )
        if path == "/ocs/v2.php/cloud/user":
            if self._user_auth(request) is None:
                return httpx.Response(401)
            return httpx.Response(200, json={"ocs": {"data": {"id": UID}}})
        if path.startswith("/ocs/v2.php/cloud/users/"):
            assert (
                request.headers["authorization"]
                == httpx.BasicAuth("admin", "admin")._auth_header
            )
            account = self.accounts.get(path.rsplit("/", 1)[-1])
            if account is None:
                return httpx.Response(404, json={"ocs": {"data": []}})
            return httpx.Response(200, json={"ocs": {"data": account}})
        if path == f"/remote.php/dav/files/{UID}/" and request.method == "PROPFIND":
            assert request.headers["depth"] == "0"
            if self._user_auth(request) is None:
                return httpx.Response(401)
            return httpx.Response(207, text="<d:multistatus/>")
        if path == "/ocs/v2.php/core/apppassword" and request.method == "DELETE":
            token = self._user_auth(request)
            if token is None:
                return httpx.Response(401)
            self.valid.discard(token)
            return httpx.Response(200, json={"ocs": {"data": []}})
        if path == "/ocs/v2.php/core/apppassword/rotate" and request.method == "POST":
            token = self._user_auth(request)
            if token is None:
                return httpx.Response(401)
            self.valid = {"rotated-SECRET"}
            return httpx.Response(
                200, json={"ocs": {"data": {"apppassword": "rotated-SECRET"}}}
            )
        return httpx.Response(500, text=f"unhandled {request.method} {path}")


class Host(NextcloudUserRootGrant):
    """The attributes NextcloudBackend gives the mixin."""

    backend_id = "nextcloud"

    def __init__(self, fake: FakeNc) -> None:
        self._base_url = NC_BASE
        self._public_url = NC_PUBLIC
        self._user_root_transport = httpx.MockTransport(fake.handler)
        self._client = httpx.AsyncClient(
            base_url=NC_BASE,
            auth=("admin", "admin"),
            headers={"OCS-APIRequest": "true"},
            transport=httpx.MockTransport(fake.handler),
        )
        # An administrator's session cookie the user's calls must not carry.
        self._client.cookies.set("nc_session_id", "ADMIN-SESSION")
        self.ready = True

    def _ensure_ready(self) -> None:
        if not self.ready:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE, "not ready", backend="nextcloud"
            )


@pytest.fixture
def fake() -> FakeNc:
    return FakeNc()


@pytest.fixture
def host(fake: FakeNc) -> Host:
    return Host(fake)


def test_it_is_a_user_root_grant(host: Host, monkeypatch) -> None:
    assert isinstance(host, SupportsUserRootGrant)
    assert host.user_root_client_name == "Superhuman Remote Worker"
    monkeypatch.delenv(GRANT_MODE_ENV, raising=False)
    assert host.user_root_grant_mode == "login_flow"
    monkeypatch.setenv(GRANT_MODE_ENV, "off")
    assert host.user_root_grant_mode == "off"
    # Silent minting is reserved: refused as off, never half-run.
    monkeypatch.setenv(GRANT_MODE_ENV, "silent")
    assert host.user_root_grant_mode == "off"


@pytest.mark.asyncio
async def test_the_flow_starts_as_the_device_and_opens_the_public_page(
    host: Host, fake: FakeNc
) -> None:
    started = await host.start_login_flow()
    assert started.login_url == f"{NC_PUBLIC}/login/v2/flow/LOGIN-TOKEN"
    assert started.poll_token == POLL
    assert POLL not in repr(started)
    [init] = fake.log
    assert init.headers["user-agent"] == CLIENT_NAME
    assert str(init.url) == f"{NC_BASE}/index.php/login/v2"


@pytest.mark.asyncio
async def test_a_poll_answers_nothing_until_the_user_finished(
    host: Host, fake: FakeNc
) -> None:
    assert await host.poll_login_flow(POLL) is None
    fake.finished = True
    grant = await host.poll_login_flow(POLL)
    assert grant == LoginFlowGrant(login_name=LOGIN, credential=TOKEN)
    assert TOKEN not in repr(grant)
    # Always the internal URL, never the public poll endpoint it named.
    assert {str(request.url) for request in fake.log} == {
        f"{NC_BASE}/index.php/login/v2/poll"
    }


@pytest.mark.asyncio
async def test_a_poll_that_fails_raises_without_the_answer(
    host: Host, fake: FakeNc
) -> None:
    fake.fail["/index.php/login/v2/poll"] = httpx.Response(503, text=TOKEN)
    with pytest.raises(CloudBackendError) as caught:
        await host.poll_login_flow(POLL)
    assert caught.value.kind is CloudBackendErrorKind.UNAVAILABLE
    assert caught.value.raw is None
    assert TOKEN not in str(caught.value) and POLL not in str(caught.value)
    fake.fail["/index.php/login/v2/poll"] = httpx.ConnectError("refused")
    with pytest.raises(CloudBackendError) as caught:
        await host.poll_login_flow(POLL)
    assert caught.value.kind is CloudBackendErrorKind.UNAVAILABLE


@pytest.mark.asyncio
async def test_whose_account_is_asked_with_the_credential_and_as_admin(
    host: Host, fake: FakeNc
) -> None:
    assert await host.user_root_account_id(LOGIN, TOKEN) == UID
    assert await host.user_root_account_id(LOGIN, "wrong") is None
    assert await host.user_root_account(UID) == CloudAccount(
        UID, "ann@example.com", True, False
    )
    fake.accounts[UID] = {**fake.accounts[UID], "groups": ["admin"], "enabled": False}
    assert await host.user_root_account(UID) == CloudAccount(
        UID, "ann@example.com", False, True
    )
    assert await host.user_root_account("nobody") is None
    # The user's calls carry their own credential and no session cookie.
    user_calls = [r for r in fake.log if r.url.path == "/ocs/v2.php/cloud/user"]
    assert user_calls and all("cookie" not in r.headers for r in user_calls)
    assert all(r.headers["ocs-apirequest"] == "true" for r in user_calls)


@pytest.mark.asyncio
async def test_the_probe_maps_what_the_cloud_answers(host: Host, fake: FakeNc) -> None:
    assert await host.verify_user_root(UID, LOGIN, TOKEN) == "ok"
    assert await host.verify_user_root(UID, LOGIN, "revoked") == "rejected"
    assert await host.verify_user_root("b" * 64, LOGIN, TOKEN) == "unreachable"
    fake.fail[f"/remote.php/dav/files/{UID}/"] = httpx.Response(404)
    assert await host.verify_user_root(UID, LOGIN, TOKEN) == "missing"
    fake.fail[f"/remote.php/dav/files/{UID}/"] = httpx.ReadTimeout("slow")
    assert await host.verify_user_root(UID, LOGIN, TOKEN) == "unreachable"
    probes = [r for r in fake.log if r.method == "PROPFIND"]
    assert probes and all("cookie" not in r.headers for r in probes)


@pytest.mark.asyncio
async def test_a_revoke_uses_the_credential_itself(host: Host, fake: FakeNc) -> None:
    assert await host.revoke_user_root(LOGIN, TOKEN) is True
    [revoke] = [r for r in fake.log if r.method == "DELETE"]
    assert revoke.headers["authorization"] == httpx.BasicAuth(LOGIN, TOKEN)._auth_header
    assert revoke.headers["ocs-apirequest"] == "true"
    assert "cookie" not in revoke.headers
    assert TOKEN not in fake.valid
    # Already gone (401) is revoked too; a cloud that cannot be asked is not.
    assert await host.revoke_user_root(LOGIN, TOKEN) is True
    fake.fail["/ocs/v2.php/core/apppassword"] = httpx.ConnectError("refused")
    assert await host.revoke_user_root(LOGIN, TOKEN) is False


@pytest.mark.asyncio
async def test_a_rotation_answers_the_new_credential(host: Host) -> None:
    assert await host.rotate_user_root(LOGIN, TOKEN) == "rotated-SECRET"
    with pytest.raises(CloudBackendError):
        await host.rotate_user_root(LOGIN, TOKEN)


def test_the_root_handle_is_the_accounts_home(host: Host) -> None:
    handle = host.user_root_handle(UID)
    assert handle.native_id == f"home:{UID}"
    assert handle.vendor_meta == {"kind": "user_home", "username": UID}


@pytest.mark.asyncio
async def test_nothing_starts_before_the_backend_is_ready(host: Host) -> None:
    host.ready = False
    with pytest.raises(CloudBackendError):
        await host.start_login_flow()
