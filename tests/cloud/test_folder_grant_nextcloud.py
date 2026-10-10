"""NextcloudBackend's per-folder grants (main cloud slices 5 and 7).

An httpx.MockTransport fake speaks the OCS user/group endpoints, the
groupfolders group endpoints (attaching a group twice fails, as on NC 31) and
a PROPFIND of the groupfolders DAV path as the principal. It asserts the
provisioning order (permissions set before the principal joins its group),
the permissions per kind, "already exists" acceptance, the 401 password
reset, the idempotent revoke, and that no other account name is accepted.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, quote, unquote

import httpx
import pytest

from orchestrator.services.cloud import (
    NextcloudBackend,
    ProjectFolderHandle,
    SupportsFolderGrant,
)
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.folder_grants import principal_ids

NC_BASE = "https://nc.example.com"
MOUNTPOINT = "Test Project"
FOLDER_ID = "7"
GRANT_ID = "11111111-1111-4111-8111-111111111111"
WRITER, WRITER_GROUP = principal_ids("writer", GRANT_ID)
READER, READER_GROUP = principal_ids("reader", GRANT_ID)


def _settings() -> NextcloudSettings:
    return NextcloudSettings(
        base_url=NC_BASE,
        public_url="https://cloud.example.com",
        admin_user="admin",
        admin_password="admin",
        agent_user="agent-service",
        agent_password="agent-service-dev",
    )


def _handle() -> ProjectFolderHandle:
    return ProjectFolderHandle(
        backend="nextcloud", native_id=FOLDER_ID, vendor_meta={"mountpoint": MOUNTPOINT}
    )


class FakeNc:
    def __init__(self) -> None:
        self.users: dict[str, dict] = {}
        self.groups: set[str] = set()
        self.folder_groups: dict[str, dict[str, int]] = {FOLDER_ID: {"srw-agents": 31}}
        self.log: list[tuple[str, str]] = []
        self.fail: dict[tuple[str, str], httpx.Response] = {}

    @staticmethod
    def _ocs(code: int, status: int = 200) -> httpx.Response:
        return httpx.Response(status, json={"ocs": {"meta": {"statuscode": code}}})

    @staticmethod
    def _form(request: httpx.Request) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(request.content.decode() or "").items()}

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = unquote(request.url.path)
        method = request.method
        self.log.append((method, path))
        if (method, path) in self.fail:
            return self.fail[(method, path)]
        form = self._form(request)
        if path == "/ocs/v2.php/cloud/users" and method == "POST":
            uid = form["userid"]
            if uid in self.users:
                return self._ocs(102, 400)
            self.users[uid] = {"password": form["password"], "groups": []}
            return self._ocs(200)
        m = re.fullmatch(r"/ocs/v2\.php/cloud/users/([^/]+)", path)
        if m and method == "PUT":
            assert form["key"] == "password"
            self.users[m.group(1)]["password"] = form["value"]
            return self._ocs(100)
        if m and method == "DELETE":
            if self.users.pop(m.group(1), None) is None:
                return self._ocs(998, 404)
            return self._ocs(200)
        m = re.fullmatch(r"/ocs/v2\.php/cloud/users/([^/]+)/groups", path)
        if m and method == "POST":
            user = self.users[m.group(1)]
            if form["groupid"] not in user["groups"]:
                user["groups"].append(form["groupid"])
            return self._ocs(200)
        if path == "/ocs/v2.php/cloud/groups" and method == "POST":
            if form["groupid"] in self.groups:
                return self._ocs(102, 400)
            self.groups.add(form["groupid"])
            return self._ocs(200)
        m = re.fullmatch(r"/ocs/v2\.php/cloud/groups/([^/]+)", path)
        if m and method == "DELETE":
            gid = m.group(1)
            if gid not in self.groups:
                return self._ocs(101, 400)
            self.groups.discard(gid)
            for grants in self.folder_groups.values():
                grants.pop(gid, None)
            for user in self.users.values():
                if gid in user["groups"]:
                    user["groups"].remove(gid)
            return self._ocs(200)
        m = re.fullmatch(r"/index\.php/apps/groupfolders/folders/([^/]+)", path)
        if m and method == "GET":
            return httpx.Response(
                200,
                json={
                    "ocs": {
                        "meta": {"statuscode": 100},
                        "data": {"groups": dict(self.folder_groups[m.group(1)])},
                    }
                },
            )
        m = re.fullmatch(r"/index\.php/apps/groupfolders/folders/([^/]+)/groups", path)
        if m and method == "POST":
            grants = self.folder_groups[m.group(1)]
            if form["group"] in grants:
                return httpx.Response(500, json={"message": "UNIQUE constraint failed"})
            grants[form["group"]] = 31
            return self._ocs(100)
        m = re.fullmatch(
            r"/index\.php/apps/groupfolders/folders/([^/]+)/groups/([^/]+)", path
        )
        if m and method == "POST":
            self.folder_groups[m.group(1)][m.group(2)] = int(form["permissions"])
            return self._ocs(100)
        m = re.fullmatch(r"/remote\.php/dav/groupfolders/([^/]+)/", path)
        if m and method == "PROPFIND":
            user = self.users.get(m.group(1))
            expected = httpx.BasicAuth(m.group(1), (user or {}).get("password", ""))
            if user is None or request.headers.get("authorization") != (
                expected._auth_header
            ):
                return httpx.Response(401)
            return httpx.Response(207, text="<d:multistatus/>")
        m = re.fullmatch(r"/remote\.php/dav/groupfolders/([^/]+)/([^/]+)/", path)
        if m and method == "PROPFIND":
            uid = m.group(1)
            user = self.users.get(uid)
            auth = request.headers.get("authorization", "")
            expected = httpx.BasicAuth(uid, (user or {}).get("password", ""))
            if user is None or auth != expected._auth_header:
                return httpx.Response(401)
            if (
                not any(gid in self.folder_groups[FOLDER_ID] for gid in user["groups"])
                or m.group(2) != MOUNTPOINT
            ):
                return httpx.Response(404)
            return httpx.Response(207, text="<d:multistatus/>")
        return httpx.Response(500, text=f"unhandled {method} {path}")


def _backend() -> tuple[NextcloudBackend, FakeNc]:
    backend = NextcloudBackend(_settings())
    fake = FakeNc()
    backend._client = httpx.AsyncClient(
        base_url=NC_BASE,
        auth=("admin", "admin"),
        transport=httpx.MockTransport(fake.handler),
    )
    backend._initialized = True
    return backend, fake


def test_nextcloud_offers_folder_grants() -> None:
    backend, _ = _backend()
    assert isinstance(backend, SupportsFolderGrant)


def test_webdav_url_is_the_groupfolders_path_as_the_principal() -> None:
    backend, _ = _backend()
    assert backend.folder_grant_webdav_url(_handle(), WRITER) == (
        f"{NC_BASE}/remote.php/dav/groupfolders/{WRITER}/{quote(MOUNTPOINT)}/"
    )
    assert (
        backend.folder_grant_webdav_url(
            ProjectFolderHandle(backend="nextcloud", native_id="7"), WRITER
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "principal", "group", "permissions"),
    [("writer", WRITER, WRITER_GROUP, 15), ("reader", READER, READER_GROUP, 1)],
)
async def test_ensure_sets_permissions_before_membership(
    kind: str, principal: str, group: str, permissions: int
) -> None:
    backend, fake = _backend()
    await backend.ensure_folder_principal(
        _handle(), principal_id=principal, group_id=group, password="pw-1", kind=kind
    )

    assert fake.users[principal] == {"password": "pw-1", "groups": [group]}
    assert fake.folder_groups[FOLDER_ID] == {"srw-agents": 31, group: permissions}
    steps = [
        (method, path)
        for method, path in fake.log
        if method != "GET" and method != "PROPFIND"
    ]
    assert steps == [
        ("POST", "/ocs/v2.php/cloud/users"),
        ("POST", "/ocs/v2.php/cloud/groups"),
        ("POST", f"/index.php/apps/groupfolders/folders/{FOLDER_ID}/groups"),
        ("POST", f"/index.php/apps/groupfolders/folders/{FOLDER_ID}/groups/{group}"),
        ("POST", f"/ocs/v2.php/cloud/users/{principal}/groups"),
    ]
    assert fake.log[-1] == (
        "PROPFIND",
        f"/remote.php/dav/groupfolders/{principal}/{MOUNTPOINT}/",
    )


@pytest.mark.asyncio
async def test_ensure_completes_a_crashed_attempt_without_a_second_attach() -> None:
    backend, fake = _backend()
    # A lost attempt created the user and attached the group (at Group
    # Folders' broad default) before it died.
    fake.users[WRITER] = {"password": "pw-1", "groups": []}
    fake.groups.add(WRITER_GROUP)
    fake.folder_groups[FOLDER_ID][WRITER_GROUP] = 31

    await backend.ensure_folder_principal(
        _handle(),
        principal_id=WRITER,
        group_id=WRITER_GROUP,
        password="pw-1",
        kind="writer",
    )

    assert fake.folder_groups[FOLDER_ID][WRITER_GROUP] == 15
    assert fake.users[WRITER]["groups"] == [WRITER_GROUP]
    assert (
        "POST",
        f"/index.php/apps/groupfolders/folders/{FOLDER_ID}/groups",
    ) not in fake.log


@pytest.mark.asyncio
async def test_ensure_resets_a_refused_password_once() -> None:
    backend, fake = _backend()
    fake.users[WRITER] = {"password": "another", "groups": []}

    await backend.ensure_folder_principal(
        _handle(),
        principal_id=WRITER,
        group_id=WRITER_GROUP,
        password="pw-1",
        kind="writer",
    )

    assert fake.users[WRITER]["password"] == "pw-1"
    assert ("PUT", f"/ocs/v2.php/cloud/users/{WRITER}") in fake.log


@pytest.mark.asyncio
async def test_ensure_raises_when_the_principal_still_cannot_read() -> None:
    backend, fake = _backend()
    fake.fail[("PROPFIND", f"/remote.php/dav/groupfolders/{WRITER}/{MOUNTPOINT}/")] = (
        httpx.Response(401)
    )

    with pytest.raises(CloudBackendError) as caught:
        await backend.ensure_folder_principal(
            _handle(),
            principal_id=WRITER,
            group_id=WRITER_GROUP,
            password="pw-1",
            kind="writer",
        )
    assert caught.value.kind == CloudBackendErrorKind.AUTHENTICATION_FAILED


@pytest.mark.asyncio
async def test_ensure_does_not_take_an_ocs_failure_for_success() -> None:
    backend, fake = _backend()
    fake.fail[("POST", "/ocs/v2.php/cloud/users")] = FakeNc._ocs(107, 400)

    with pytest.raises(CloudBackendError, match="create folder grant principal"):
        await backend.ensure_folder_principal(
            _handle(),
            principal_id=WRITER,
            group_id=WRITER_GROUP,
            password="pw-1",
            kind="writer",
        )
    assert WRITER_GROUP not in fake.groups


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("principal", "group", "kind"),
    [
        ("agent-service", WRITER_GROUP, "writer"),
        ("admin", "admin", "writer"),
        (WRITER, READER_GROUP, "writer"),
        (
            WRITER,
            principal_ids("writer", "22222222-2222-4222-8222-222222222222")[1],
            "writer",
        ),
        (f"srw-reader-a-{GRANT_ID.replace('-', '')}", READER_GROUP, "reader"),
    ],
)
async def test_only_per_folder_names_are_accepted(
    principal: str, group: str, kind: str
) -> None:
    backend, fake = _backend()
    with pytest.raises(CloudBackendError, match="malformed"):
        await backend.ensure_folder_principal(
            _handle(), principal_id=principal, group_id=group, password="pw", kind=kind
        )
    with pytest.raises(CloudBackendError, match="malformed"):
        await backend.revoke_folder_principal(principal_id=principal, group_id=group)
    assert fake.log == []


@pytest.mark.asyncio
async def test_a_reader_is_never_granted_a_writer_permission() -> None:
    backend, fake = _backend()
    with pytest.raises(CloudBackendError, match="malformed"):
        await backend.ensure_folder_principal(
            _handle(),
            principal_id=READER,
            group_id=READER_GROUP,
            password="pw",
            kind="writer",
        )
    assert fake.log == []


@pytest.mark.asyncio
async def test_revoke_removes_group_then_principal_and_is_idempotent() -> None:
    backend, fake = _backend()
    await backend.ensure_folder_principal(
        _handle(),
        principal_id=WRITER,
        group_id=WRITER_GROUP,
        password="pw-1",
        kind="writer",
    )
    fake.log.clear()

    await backend.revoke_folder_principal(principal_id=WRITER, group_id=WRITER_GROUP)
    await backend.revoke_folder_principal(principal_id=WRITER, group_id=WRITER_GROUP)

    assert WRITER not in fake.users
    assert WRITER_GROUP not in fake.groups
    assert fake.folder_groups[FOLDER_ID] == {"srw-agents": 31}
    assert fake.log[:2] == [
        ("DELETE", f"/ocs/v2.php/cloud/groups/{WRITER_GROUP}"),
        ("DELETE", f"/ocs/v2.php/cloud/users/{WRITER}"),
    ]


def test_principal_client_authenticates_as_the_principal() -> None:
    backend, _ = _backend()
    client = backend.folder_principal_client(READER, "pw-1")
    assert client.auth is not None
    with pytest.raises(CloudBackendError):
        backend.folder_principal_client("agent-service", "pw")


@pytest.mark.asyncio
async def test_a_folder_not_at_its_path_keeps_a_live_principals_grant() -> None:
    """Moved away (its mountpoint renamed): the principal's own root reads,
    so the grant stands and the mount says not_found, as for any folder
    that went missing."""
    backend, fake = _backend()
    moved = ProjectFolderHandle(
        backend="nextcloud", native_id=FOLDER_ID, vendor_meta={"mountpoint": "Gone"}
    )
    await backend.ensure_folder_principal(
        moved,
        principal_id=WRITER,
        group_id=WRITER_GROUP,
        password="pw-1",
        kind="writer",
    )
    assert fake.users[WRITER]["groups"] == [WRITER_GROUP]
    assert fake.log[-1] == ("PROPFIND", f"/remote.php/dav/groupfolders/{WRITER}/")


@pytest.mark.asyncio
async def test_a_principal_that_reads_nothing_fails_its_grant() -> None:
    backend, fake = _backend()
    fake.fail[("PROPFIND", f"/remote.php/dav/groupfolders/{WRITER}/{MOUNTPOINT}/")] = (
        httpx.Response(404)
    )
    fake.fail[("PROPFIND", f"/remote.php/dav/groupfolders/{WRITER}/")] = httpx.Response(
        404
    )
    with pytest.raises(CloudBackendError, match="cannot read its folder: HTTP 404"):
        await backend.ensure_folder_principal(
            _handle(),
            principal_id=WRITER,
            group_id=WRITER_GROUP,
            password="pw-1",
            kind="writer",
        )
