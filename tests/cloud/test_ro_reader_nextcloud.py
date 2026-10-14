"""Tests for NextcloudBackend RO-reader provisioning (protected cloud mode).

Stands the backend up against an httpx.MockTransport fake that speaks the OCS
user/group admin endpoints, the groupfolders group-ACL endpoints, and the
WebDAV groupfolders path (for the canary). Asserts the provisioning LOGIC —
a low-priv reader account, a per-mount READ-only (permission=1) grant, a
rotated credential, and clean revoke — not live NC status-code semantics
(those are the §11.4 live-validation concern).
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, quote, unquote

import httpx
import pytest

from orchestrator.services.cloud import NextcloudBackend, ProjectFolderHandle
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.errors import CloudBackendError
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)

NC_BASE = "https://nc.example.com"
AGENT_USER = "agent-service"
MOUNTPOINT = "Test Project"
FOLDER_ID = "7"
BACKEND_INSTANCE_ID = "99999999-9999-4999-8999-999999999999"
PROJECT_ID = "88888888-8888-4888-8888-888888888888"


def _settings() -> NextcloudSettings:
    return NextcloudSettings(
        base_url=NC_BASE,
        public_url=NC_BASE,
        admin_user="admin",
        admin_password="admin",
        agent_user=AGENT_USER,
        agent_password="agent-service-dev",
    )


def _handle() -> ProjectFolderHandle:
    return ProjectFolderHandle(
        backend="nextcloud",
        native_id=FOLDER_ID,
        vendor_meta={"mountpoint": MOUNTPOINT},
    )


def _plan(
    *,
    attempt: str = "77777777-7777-4777-8777-777777777777",
    folder_id: str = FOLDER_ID,
    mountpoint: str = MOUNTPOINT,
) -> ProtectedNextcloudReaderGrantPlan:
    return ProtectedNextcloudReaderGrantPlan(
        engage_attempt=attempt,
        backend_instance_id=BACKEND_INSTANCE_ID,
        source=ProtectedMountSourceIdentity(
            backend_instance_id=BACKEND_INSTANCE_ID,
            source_ref=PROJECT_ID,
            target_path="cloud",
            native_id=folder_id,
            mountpoint=mountpoint,
        ),
    )


class FakeNcOcs:
    """In-memory OCS + groupfolders + WebDAV stand-in for reader provisioning."""

    def __init__(self) -> None:
        self.users: dict[str, dict] = {}
        self.groups: set[str] = set()
        # folder_id -> {group_id: permissions}
        self.folder_group_perms: dict[str, dict[str, int]] = {}
        self.files: dict[str, bytes] = {}
        self.requests: list[httpx.Request] = []
        self.delete_group_response: httpx.Response | None = None
        self.delete_user_response: httpx.Response | None = None

    def _ocs(self, statuscode: int, extra: dict | None = None) -> httpx.Response:
        body = {"ocs": {"meta": {"status": "ok", "statuscode": statuscode}}}
        if extra:
            body["ocs"].update(extra)
        return httpx.Response(200, json=body)

    def _form(self, request: httpx.Request) -> dict[str, str]:
        parsed = parse_qs(request.content.decode() or "")
        return {k: v[0] for k, v in parsed.items()}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = unquote(request.url.path)
        method = request.method

        # --- OCS users ---
        if path == "/ocs/v2.php/cloud/users" and method == "POST":
            uid = self._form(request).get("userid", "")
            if uid in self.users:
                return self._ocs(102)  # already exists
            self.users[uid] = {
                "groups": [],
                "password": self._form(request).get("password"),
            }
            return self._ocs(100)

        m = re.fullmatch(r"/ocs/v2\.php/cloud/users/([^/]+)", path)
        if m and method == "DELETE":
            if self.delete_user_response is not None:
                return self.delete_user_response
            self.users.pop(m.group(1), None)
            return self._ocs(100)
        if m and method == "PUT":
            uid = m.group(1)
            form = self._form(request)
            if form.get("key") == "password":
                self.users.setdefault(uid, {"groups": []})["password"] = form.get(
                    "value"
                )
            return self._ocs(100)

        m = re.fullmatch(r"/ocs/v2\.php/cloud/users/([^/]+)/groups", path)
        if m and method == "POST":
            uid, gid = m.group(1), self._form(request).get("groupid", "")
            self.users.setdefault(uid, {"groups": []})["groups"].append(gid)
            return self._ocs(100)
        if m and method == "DELETE":
            uid, gid = m.group(1), self._form(request).get("groupid", "")
            groups = self.users.get(uid, {}).get("groups", [])
            if gid in groups:
                groups.remove(gid)
            return self._ocs(100)

        # --- OCS groups ---
        if path == "/ocs/v2.php/cloud/groups" and method == "POST":
            gid = self._form(request).get("groupid", "")
            self.groups.add(gid)
            return httpx.Response(200, json={"ocs": {"meta": {"statuscode": 200}}})

        m = re.fullmatch(r"/ocs/v2\.php/cloud/groups/([^/]+)", path)
        if m and method == "DELETE":
            if self.delete_group_response is not None:
                return self.delete_group_response
            self.groups.discard(m.group(1))
            self.folder_group_perms and [
                perms.pop(m.group(1), None)
                for perms in self.folder_group_perms.values()
            ]
            return self._ocs(100)

        # --- groupfolders ACL ---
        m = re.fullmatch(r"/index\.php/apps/groupfolders/folders/([^/]+)/groups", path)
        if m and method == "POST":
            self.folder_group_perms.setdefault(m.group(1), {}).setdefault(
                self._form(request).get("group", ""), 31
            )
            return self._ocs(100)

        m = re.fullmatch(
            r"/index\.php/apps/groupfolders/folders/([^/]+)/groups/([^/]+)", path
        )
        if m and method == "POST":
            fid, gid = m.group(1), m.group(2)
            self.folder_group_perms.setdefault(fid, {})[gid] = int(
                self._form(request).get("permissions", "0")
            )
            return self._ocs(100)

        # --- WebDAV groupfolders (canary) ---
        dav_prefix = f"/remote.php/dav/groupfolders/{AGENT_USER}/{MOUNTPOINT}"
        if path.startswith(dav_prefix):
            rel = path[len(dav_prefix) :].strip("/")
            if method == "PUT":
                # MKCOL parents are also PUT-preceded by MKCOL; accept both.
                self.files[rel] = bytes(request.content)
                return httpx.Response(201)
            if method == "MKCOL":
                return httpx.Response(201)
            if method == "DELETE":
                self.files.pop(rel, None)
                return httpx.Response(204)
            if method == "GET":
                if rel in self.files:
                    return httpx.Response(200, content=self.files[rel])
                return httpx.Response(404)

        return httpx.Response(500, content=f"unhandled {method} {path}".encode())


def _backend_with_ocs_fake():
    backend = NextcloudBackend(_settings())
    fake = FakeNcOcs()
    backend._client = httpx.AsyncClient(
        base_url=NC_BASE, transport=httpx.MockTransport(fake.handler)
    )
    backend._agent_client = backend._build_agent_client(
        httpx.MockTransport(fake.handler)
    )
    backend._initialized = True
    backend._agent_user = AGENT_USER
    backend._agent_password = "pw"
    backend.bind_backend_instance(BACKEND_INSTANCE_ID)
    return backend, fake


def _effect_dispatcher(backend: NextcloudBackend):
    async def _dispatch(method: str, path: str, body: bytes) -> httpx.Response:
        assert "?" not in path
        return await backend._client.request(
            method,
            path,
            content=body,
            headers={"content-type": "application/x-www-form-urlencoded"},
        )

    return _dispatch


@pytest.mark.asyncio
async def test_grant_creates_attempt_scoped_reader_with_final_password():
    backend, fake = _backend_with_ocs_fake()
    plan = _plan()
    grant = await backend.grant_protected_reader_attempt(
        plan,
        credentials="final-password-A",
        dispatch_effect=_effect_dispatcher(backend),
    )

    assert grant.reader_id == plan.reader_id
    assert grant.grant_handle == plan.grant_handle
    assert grant.credentials == "final-password-A"
    assert fake.users[plan.reader_id]["password"] == "final-password-A"
    assert fake.users[plan.reader_id]["groups"] == [plan.group_id]
    assert fake.folder_group_perms[FOLDER_ID][plan.group_id] == 1
    assert quote(MOUNTPOINT, safe="") in grant.webdav_url
    assert all(request.method != "PUT" for request in fake.requests)


@pytest.mark.asyncio
async def test_same_attempt_retry_is_idempotent_without_password_rotation():
    backend, fake = _backend_with_ocs_fake()
    plan = _plan()
    for _ in range(2):
        grant = await backend.grant_protected_reader_attempt(
            plan,
            credentials="final-password-A",
            dispatch_effect=_effect_dispatcher(backend),
        )

    assert grant.reader_id == plan.reader_id
    assert fake.users[plan.reader_id]["password"] == "final-password-A"
    assert fake.users[plan.reader_id]["groups"].count(plan.group_id) >= 1
    assert all(request.method != "PUT" for request in fake.requests)


@pytest.mark.asyncio
async def test_two_same_user_threads_have_disjoint_reader_and_group_authority():
    backend, fake = _backend_with_ocs_fake()
    plan_a = _plan(attempt="11111111-1111-4111-8111-111111111111")
    plan_b = _plan(
        attempt="22222222-2222-4222-8222-222222222222",
        folder_id="8",
        mountpoint="Other Project",
    )
    await backend.grant_protected_reader_attempt(
        plan_a,
        credentials="password-A",
        dispatch_effect=_effect_dispatcher(backend),
    )
    await backend.grant_protected_reader_attempt(
        plan_b,
        credentials="password-B",
        dispatch_effect=_effect_dispatcher(backend),
    )

    assert plan_a.reader_id != plan_b.reader_id
    assert plan_a.group_id != plan_b.group_id
    assert fake.users[plan_a.reader_id] == {
        "groups": [plan_a.group_id],
        "password": "password-A",
    }
    assert fake.users[plan_b.reader_id] == {
        "groups": [plan_b.group_id],
        "password": "password-B",
    }
    assert fake.folder_group_perms[plan_a.folder_id] == {plan_a.group_id: 1}
    assert fake.folder_group_perms[plan_b.folder_id] == {plan_b.group_id: 1}


@pytest.mark.asyncio
async def test_revoke_attempt_deletes_group_and_reader_without_touching_peer():
    backend, fake = _backend_with_ocs_fake()
    plan_a = _plan(attempt="11111111-1111-4111-8111-111111111111")
    plan_b = _plan(
        attempt="22222222-2222-4222-8222-222222222222",
        folder_id="8",
        mountpoint="Other Project",
    )
    for plan, password in ((plan_a, "password-A"), (plan_b, "password-B")):
        await backend.grant_protected_reader_attempt(
            plan,
            credentials=password,
            dispatch_effect=_effect_dispatcher(backend),
        )

    await backend.revoke_protected_reader_attempt(plan_a)

    assert plan_a.group_id not in fake.groups
    assert plan_a.reader_id not in fake.users
    assert plan_b.group_id in fake.groups
    assert fake.users[plan_b.reader_id]["password"] == "password-B"
    assert fake.folder_group_perms[plan_b.folder_id] == {plan_b.group_id: 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(
            200,
            json={
                "ocs": {
                    "meta": {
                        "status": "failure",
                        "statuscode": 997,
                        "message": "logical delete failure",
                    }
                }
            },
        ),
        httpx.Response(200, json={"unexpected": "shape"}),
        httpx.Response(200, json=[]),
        httpx.Response(
            200,
            json={"ocs": {"meta": {"status": "ok", "statuscode": "100"}}},
        ),
        httpx.Response(
            200,
            json={"ocs": {"meta": {"status": "ok", "statuscode": True}}},
        ),
    ],
)
async def test_revoke_does_not_treat_http_200_ocs_failure_as_deleted(
    response: httpx.Response,
) -> None:
    backend, fake = _backend_with_ocs_fake()
    plan = _plan()
    await backend.grant_protected_reader_attempt(
        plan,
        credentials="final-password-A",
        dispatch_effect=_effect_dispatcher(backend),
    )
    fake.delete_group_response = response

    with pytest.raises(CloudBackendError, match="delete RO grant group"):
        await backend.revoke_protected_reader_attempt(plan)

    assert plan.group_id in fake.groups
    assert plan.group_id in fake.folder_group_perms[FOLDER_ID]
    assert plan.reader_id in fake.users


@pytest.mark.asyncio
async def test_revoke_accepts_nextcloud_31_absent_group_and_user() -> None:
    backend, fake = _backend_with_ocs_fake()
    plan = _plan()
    absent = httpx.Response(
        400,
        json={
            "ocs": {
                "meta": {
                    "status": "failure",
                    "statuscode": 101,
                    "message": "not found",
                }
            }
        },
    )
    fake.delete_group_response = absent
    fake.delete_user_response = absent

    # The signed create may fail before either identity exists. Cleanup must
    # still settle the exact attempt instead of wedging it in revoking.
    await backend.revoke_protected_reader_attempt(plan)


@pytest.mark.asyncio
async def test_canary_seed_and_remove_roundtrip():
    backend, fake = _backend_with_ocs_fake()
    fixture = await backend.seed_canary_fixture(_handle())
    assert fixture.path in fake.files  # written with the write identity
    await backend.remove_canary_fixture(_handle(), fixture)
    assert fixture.path not in fake.files
