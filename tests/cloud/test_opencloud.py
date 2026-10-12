"""Unit tests for ``OpenCloudBackend``.

These tests stand up the adapter against an ``httpx.MockTransport``
that routes requests to deterministic handlers. They cover the shape
of the LibreGraph calls the adapter makes, the Keycloak token flow,
the disable-then-purge delete pattern, and the error-mapping layer.

Full-stack contract parity with ``test_backend_contract.py`` is
deferred to Phase 2.5 / Phase 3, when VCR cassettes recorded against a
real OpenCloud instance replace these hand-rolled fakes. The goal here
is to exercise the adapter's control flow end-to-end with zero external
dependencies.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

import httpx
import pytest
from pydantic import SecretStr

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    CloudMountSubject,
    GroupId,
    OpenCloudBackend,
    OpenCloudSettings,
    ProjectFolderHandle,
    SessionFolderHandle,
    ShareHandle,
    UserId,
)

KEYCLOAK_BASE = "https://kc.example.com/realms/srw"
OPENCLOUD_BASE = "https://cloud.example.com"


def _settings() -> OpenCloudSettings:
    return OpenCloudSettings(
        backend_id="opencloud",
        base_url=OPENCLOUD_BASE,
        public_url=OPENCLOUD_BASE,
        keycloak_issuer=KEYCLOAK_BASE,
        keycloak_client_id="opencloud-orchestrator",
        keycloak_client_secret=SecretStr("test-secret"),
        admin_role_claim_value="opencloudAdmin",
        default_quota_bytes=10 * 1024 * 1024 * 1024,
    )


# ----------------------------------------------------------------------- Fakes


class FakeOpenCloud:
    """Minimal in-memory stand-in for an OpenCloud + Keycloak pair.

    The real adapter interleaves Keycloak token requests with LibreGraph
    calls on the same ``httpx.AsyncClient``. ``httpx.MockTransport`` sees
    the absolute URL for Keycloak requests (because the token endpoint
    is a full URL, not a relative path) and the ``base_url``-relative
    paths for LibreGraph. We dispatch on host + path to cover both.
    """

    def __init__(self) -> None:
        self.groups: dict[str, dict[str, Any]] = {}
        self.drives: dict[str, dict[str, Any]] = {}
        self.users: dict[str, dict[str, Any]] = {}
        self.next_id = 1
        self.token_requests: list[httpx.Request] = []
        self.graph_requests: list[httpx.Request] = []
        self.webdav_requests: list[httpx.Request] = []
        self.disabled_drives: set[str] = set()
        # OpenCloud role definitions use duplicate displayName values
        # distinguished by @libre.graph.weight. Space-applicable roles have
        # weight 90 (editor) and 40 (viewer). See opencloud.py header.
        self.role_catalog: list[dict[str, Any]] = [
            {
                "id": "aa11-editor-role-id",
                "displayName": "Can edit",
                "@libre.graph.weight": 90,
            },
            {
                "id": "bb22-viewer-role-id",
                "displayName": "Can view",
                "@libre.graph.weight": 40,
            },
            {
                "id": "cc33-share-edit-id",
                "displayName": "Can edit",
                "@libre.graph.weight": 60,
            },
        ]
        # permissions[drive_id][item_id] -> list[{id, user/group}]
        self.permissions: dict[str, dict[str, list[dict[str, Any]]]] = {}
        # items[drive_id] -> {path: item_id}
        self.items: dict[str, dict[str, str]] = {}
        self.error_once: Optional[int] = None

    def _new_id(self, prefix: str = "") -> str:
        val = f"{prefix}{self.next_id}"
        self.next_id += 1
        return val

    def seed_user(self, *, email: str, display_name: str) -> str:
        uid = self._new_id("user-")
        self.users[uid] = {
            "id": uid,
            "mail": email,
            "displayName": display_name,
            "drive": {
                "id": f"drive-{uid}",
                "webUrl": f"{OPENCLOUD_BASE}/files/spaces/drive-{uid}",
                "root": {
                    "webDavUrl": f"{OPENCLOUD_BASE}/dav/spaces/drive-{uid}/",
                },
            },
        }
        return uid

    # ---------------------------------------------------------- Dispatcher

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        host = url.host
        if host == "kc.example.com":
            self.token_requests.append(request)
            return self._handle_token(request)
        if "/dav/spaces/" in str(url):
            self.webdav_requests.append(request)
            return self._handle_webdav(request)
        self.graph_requests.append(request)
        return self._handle_graph(request)

    def _handle_token(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        return httpx.Response(
            200,
            json={
                "access_token": "fake-access-token",
                "expires_in": 300,
                "token_type": "Bearer",
            },
        )

    def _handle_webdav(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "MKCOL":
            return httpx.Response(201)
        if request.method == "DELETE":
            return httpx.Response(204)
        if request.method == "PROPFIND":
            # `_resolve_item_id` parses <oc:fileid> out of the body; the real
            # server returns 207 Multi-Status. The item id OpenCloud hands back
            # is a compound ``{drive_id}!{item_uuid}``, so synthesize one that
            # looks the part.
            segment = path.rsplit("/", 1)[-1] or "root"
            drive_part = ""
            if "/dav/spaces/" in path:
                after = path.split("/dav/spaces/", 1)[-1]
                drive_part = after.split("/", 1)[0]
            file_id = (
                f"item-{drive_part}-{segment}" if drive_part else f"item-{segment}"
            )
            body = (
                '<?xml version="1.0"?>'
                '<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">'
                "<d:response>"
                f"<d:href>{path}</d:href>"
                "<d:propstat><d:prop>"
                f"<oc:fileid>{file_id}</oc:fileid>"
                "</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response></d:multistatus>"
            )
            return httpx.Response(
                207,
                content=body.encode(),
                headers={"Content-Type": "application/xml"},
            )
        return httpx.Response(500, content=f"unexpected webdav call: {path}".encode())

    def _handle_graph(self, request: httpx.Request) -> httpx.Response:
        method = request.method
        path = request.url.path
        query = dict(request.url.params)

        # --- Role catalog
        # OpenCloud returns this as a bare JSON array, not a wrapped
        # {"value": [...]} — confirmed against a live dev instance.
        if (
            method == "GET"
            and path == "/graph/v1beta1/roleManagement/permissions/roleDefinitions"
        ):
            return httpx.Response(200, json=list(self.role_catalog))

        # --- Users list/search
        if method == "GET" and path == "/graph/v1.0/users":
            return httpx.Response(200, json={"value": list(self.users.values())})

        # --- User by id with drive expansion
        if method == "GET" and path.startswith("/graph/v1.0/users/"):
            uid = path.split("/graph/v1.0/users/")[-1]
            if uid in self.users:
                return httpx.Response(200, json=self.users[uid])
            return httpx.Response(404, json={"error": "user not found"})

        # --- Groups
        if method == "GET" and path == "/graph/v1.0/groups":
            return httpx.Response(200, json={"value": list(self.groups.values())})
        if method == "POST" and path == "/graph/v1.0/groups":
            body = json.loads(request.content)
            display_name = body["displayName"]
            gid = self._new_id("group-")
            self.groups[gid] = {"id": gid, "displayName": display_name}
            return httpx.Response(201, json=self.groups[gid])
        if method == "POST" and "/members/$ref" in path:
            return httpx.Response(204)
        if method == "DELETE" and "/members/" in path and "/$ref" in path:
            return httpx.Response(204)

        # --- Drives (Spaces)
        if method == "GET" and path == "/graph/v1.0/drives":
            name_filter = query.get("$filter", "")
            value = list(self.drives.values())
            if "name eq" in name_filter:
                want = name_filter.split("'")[1]
                value = [d for d in value if d.get("name") == want]
            return httpx.Response(200, json={"value": value})
        if method == "POST" and path == "/graph/v1.0/drives":
            body = json.loads(request.content)
            did = self._new_id("drive-")
            drive = {
                "id": did,
                "name": body["name"],
                "webUrl": f"{OPENCLOUD_BASE}/files/spaces/{did}",
                "root": {
                    "id": f"root-{did}",
                    "webDavUrl": f"{OPENCLOUD_BASE}/dav/spaces/{did}/",
                },
            }
            self.drives[did] = drive
            self.permissions[did] = {f"root-{did}": []}
            return httpx.Response(201, json=drive)

        # --- Drive delete (disable-then-purge)
        if method == "DELETE" and path.startswith("/graph/v1.0/drives/"):
            did = path.split("/graph/v1.0/drives/")[-1]
            if did not in self.drives:
                return httpx.Response(404, json={"error": "not found"})
            if request.headers.get("Purge") == "T":
                if did not in self.disabled_drives:
                    return httpx.Response(
                        400, json={"error": "cannot purge enabled space"}
                    )
                del self.drives[did]
                self.disabled_drives.discard(did)
                return httpx.Response(204)
            self.disabled_drives.add(did)
            return httpx.Response(204)

        # --- Root invite
        if method == "POST" and path.endswith("/root/invite"):
            return httpx.Response(
                200,
                json={"value": [{"id": self._new_id("perm-")}]},
            )

        # --- Item lookup by path
        if method == "GET" and "/root:/" in path:
            return httpx.Response(
                200,
                json={
                    "id": self._new_id("item-"),
                    "name": path.rsplit("/", 1)[-1],
                },
            )

        # --- Item invite
        if method == "POST" and "/items/" in path and path.endswith("/invite"):
            return httpx.Response(
                200,
                json={"value": [{"id": self._new_id("perm-")}]},
            )

        # --- Permission delete
        if method == "DELETE" and "/permissions/" in path:
            return httpx.Response(204)

        return httpx.Response(
            500,
            json={"error": f"unexpected graph call {method} {path}"},
        )


def _install_fake(backend: OpenCloudBackend, fake: FakeOpenCloud) -> None:
    """Wire a pre-initialized adapter up to the fake server.

    Skips ``ensure_initialized`` (which would try to create an agent-home
    Space via the real code path) and manually plants a drive id so
    session-folder operations work.
    """
    transport = httpx.MockTransport(fake.handler)
    backend._client = httpx.AsyncClient(
        base_url=OPENCLOUD_BASE,
        transport=transport,
        headers={
            "User-Agent": "SuperhumanRemoteWorker/0.1 main-cloud-backend",
            "Accept": "application/json",
        },
    )
    backend._initialized = True
    backend._agent_home_drive_id = "drive-agent-home"
    backend._agent_home_webdav_base = f"{OPENCLOUD_BASE}/dav/spaces/drive-agent-home"
    fake.drives["drive-agent-home"] = {
        "id": "drive-agent-home",
        "name": "srw-agent-home",
        "webUrl": f"{OPENCLOUD_BASE}/files/spaces/drive-agent-home",
        "root": {
            "id": "root-drive-agent-home",
            "webDavUrl": f"{OPENCLOUD_BASE}/dav/spaces/drive-agent-home/",
        },
    }
    backend._role_cache = {
        (r["displayName"], int(r.get("@libre.graph.weight", 0))): r["id"]
        for r in fake.role_catalog
    }


class TestRoleId:
    @pytest.mark.asyncio
    async def test_resolves_on_cold_cache_refresh_path(self):
        # Regression: on a cache miss the refresh path indexed the tuple-keyed
        # _role_cache with a bare display_name string, raising KeyError. Force
        # the cold path by clearing the pre-warmed cache.
        backend = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(backend, fake)
        backend._role_cache = {}  # force the _load_role_catalog refresh

        role_id = await backend._role_id("Can view", 40)

        assert role_id == "bb22-viewer-role-id"


# ----------------------------------------------------------------- Settings tests


class TestSettings:
    def test_settings_require_secret(self):
        with pytest.raises(Exception):  # pydantic.ValidationError
            OpenCloudSettings.model_validate(
                {
                    "backend_id": "opencloud",
                    "base_url": OPENCLOUD_BASE,
                    "public_url": OPENCLOUD_BASE,
                    "keycloak_issuer": KEYCLOAK_BASE,
                    "keycloak_client_id": "oc",
                    # missing keycloak_client_secret
                }
            )

    def test_settings_default_quota_optional(self):
        s = OpenCloudSettings(
            backend_id="opencloud",
            base_url=OPENCLOUD_BASE,
            public_url=OPENCLOUD_BASE,
            keycloak_issuer=KEYCLOAK_BASE,
            keycloak_client_id="oc",
            keycloak_client_secret=SecretStr("x"),
        )
        assert s.default_quota_bytes is None


# ------------------------------------------------------------ Static helpers


class TestUrlConstructors:
    def test_project_folder_browser_url_encodes_delimiters(self):
        be = OpenCloudBackend(_settings())
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="storage!space$1",
            vendor_meta={},
        )
        url = be.get_project_folder_browser_url(handle)
        assert url is not None
        # Both '!' and '$' must be percent-encoded (opencloud-eu/web#1795).
        assert "%21" in url
        assert "%24" in url
        # `/f/<fileId>` is OpenCloud's private-link resolver — routes to the
        # exact Space for any member. The old `/files/spaces/<drive_id>`
        # fallback silently resolved to the viewer's *personal* Space.
        assert f"{OPENCLOUD_BASE}/f/" in url

    def test_project_folder_browser_url_ignores_stale_persisted_domain(self):
        # Regression: OpenCloud was rebranded (cloud.superhuman-remote-worker.com
        # -> cloud.srw.works). The webUrl OpenCloud returns at Space creation
        # bakes in whatever OC_URL was canonical then, and we persisted it in
        # vendor_meta.web_url. Returning that snapshot verbatim sends the
        # "Open folder" button to a now-foreign instance. The link must be
        # derived from the LIVE public_url instead.
        be = OpenCloudBackend(_settings())  # public_url == OPENCLOUD_BASE
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="a03eeabc$f3f282fc",
            vendor_meta={
                "web_url": "https://cloud.old-domain.example/f/a03eeabc$f3f282fc",
                "webdav_url": (
                    "https://cloud.old-domain.example/dav/spaces/a03eeabc$f3f282fc"
                ),
            },
        )
        url = be.get_project_folder_browser_url(handle)
        assert url == f"{OPENCLOUD_BASE}/f/a03eeabc%24f3f282fc"
        assert "old-domain" not in url

    def test_project_folder_browser_url_legacy_no_native_id_uses_persisted(self):
        # Handles without a native_id can't be reconstructed; fall back to the
        # persisted absolute link (best effort).
        be = OpenCloudBackend(_settings())
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="",
            vendor_meta={"web_url": "https://cloud.example.com/f/legacy"},
        )
        assert (
            be.get_project_folder_browser_url(handle)
            == "https://cloud.example.com/f/legacy"
        )

    def test_project_folder_webdav_url_ignores_stale_persisted_domain(self):
        # Same rebrand hazard as the browser link: the persisted webdav_url is
        # the PUBLIC graph-response URL frozen on the old domain. Reconstruct
        # from the live internal base + drive id, mirroring
        # build_rclone_mount_spec (which already does this for real mounts).
        be = OpenCloudBackend(_settings())  # base_url == OPENCLOUD_BASE
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="a03eeabc$f3f282fc",
            vendor_meta={
                "webdav_url": (
                    "https://cloud.old-domain.example/dav/spaces/a03eeabc$f3f282fc"
                )
            },
        )
        url = be.get_project_folder_webdav_url(handle)
        assert url == f"{OPENCLOUD_BASE}/dav/spaces/a03eeabc%24f3f282fc/"
        assert "old-domain" not in url

    def test_project_folder_webdav_url_falls_back_to_reconstructed(self):
        be = OpenCloudBackend(_settings())
        handle = ProjectFolderHandle(
            backend="opencloud", native_id="drive-42", vendor_meta={}
        )
        url = be.get_project_folder_webdav_url(handle)
        assert url == f"{OPENCLOUD_BASE}/dav/spaces/drive-42/"

    def test_default_home_browser_url(self):
        be = OpenCloudBackend(_settings())
        assert be.get_default_home_browser_url() == f"{OPENCLOUD_BASE}/files/"

    def test_webdav_credentials_empty(self):
        be = OpenCloudBackend(_settings())
        assert be.webdav_credentials == {}


# ----------------------------------------------------------- rclone mount spec


class TestRcloneMountSpec:
    @pytest.mark.asyncio
    async def test_session_folder_spec_uses_bearer_command_auth(self):
        be = OpenCloudBackend(_settings())
        _install_fake(be, FakeOpenCloud())
        handle = SessionFolderHandle(
            backend="opencloud",
            native_id="sessions/thread-1",
            vendor_meta={"drive_id": "drive-agent-home"},
        )
        spec = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="session_folder",
            target_path="/cloud/home",
            access="read_write",
        )
        assert spec.source_type == "webdav"
        assert (
            spec.source_config["url"]
            == f"{OPENCLOUD_BASE}/dav/spaces/drive-agent-home/sessions/thread-1/"
        )
        assert spec.source_config["vendor"] == "infinitescale"
        # Bearer flow: no static credential in the rclone source config.
        assert "pass" not in spec.source_config
        assert spec.auth["type"] == "keycloak_client_credentials"
        assert spec.auth["issuer"] == KEYCLOAK_BASE
        assert spec.auth["client_id"] == "opencloud-orchestrator"
        assert spec.auth["client_secret"] == "test-secret"
        assert "token_helper" in spec.required_capabilities
        assert spec.min_rclone_version == "1.70.0"
        # TLS verification stays on unless explicitly opted out.
        assert spec.provider_flags == []

    @pytest.mark.asyncio
    async def test_mount_insecure_tls_adds_no_check_certificate(self):
        # Local-dev knob (OPENCLOUD_MOUNT_INSECURE_TLS): the tus data-gateway
        # hop presents the mkcert edge cert on local k3d.
        settings = _settings().model_copy(update={"mount_insecure_tls": True})
        be = OpenCloudBackend(settings)
        _install_fake(be, FakeOpenCloud())
        handle = SessionFolderHandle(
            backend="opencloud",
            native_id="sessions/thread-1",
            vendor_meta={"drive_id": "drive-agent-home"},
        )
        spec = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="session_folder",
            target_path="/cloud/home",
            access="read_write",
        )
        assert spec.provider_flags == ["--no-check-certificate"]
        # The flag must survive into the agent payload.
        assert spec.to_payload()["provider_flags"] == ["--no-check-certificate"]

    @pytest.mark.asyncio
    async def test_project_folder_spec_reconstructs_space_url(self):
        be = OpenCloudBackend(_settings())
        _install_fake(be, FakeOpenCloud())
        handle = ProjectFolderHandle(
            backend="opencloud", native_id="drive-42", vendor_meta={}
        )
        spec = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="project",
            target_path="/cloud/proj",
            access="read_write",
        )
        assert spec.source_config["url"] == f"{OPENCLOUD_BASE}/dav/spaces/drive-42/"
        assert spec.auth["type"] == "keycloak_client_credentials"

    @pytest.mark.asyncio
    async def test_prefer_public_url_swaps_internal_to_public(self):
        # A cross-cluster VM mount must target the public edge; the internal
        # service URL isn't reachable from the vm cluster
        # (knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md). Same-cluster
        # pods (default, prefer_public_url=False) keep the internal URL.
        settings = _settings().model_copy(
            update={
                "base_url": "http://srw-opencloud:9200",
                "public_url": "https://cloud.example.com",
            }
        )
        be = OpenCloudBackend(settings)
        _install_fake(be, FakeOpenCloud())
        handle = ProjectFolderHandle(
            backend="opencloud", native_id="drive-42", vendor_meta={}
        )
        internal = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="project",
            target_path="/cloud/proj",
            access="read_write",
        )
        assert (
            internal.source_config["url"]
            == "http://srw-opencloud:9200/dav/spaces/drive-42/"
        )
        public = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="project",
            target_path="/cloud/proj",
            access="read_only",
            prefer_public_url=True,
        )
        assert (
            public.source_config["url"]
            == "https://cloud.example.com/dav/spaces/drive-42/"
        )

    @pytest.mark.asyncio
    async def test_user_home_with_sub_uses_impersonation_auth(self):
        be = OpenCloudBackend(_settings())
        _install_fake(be, FakeOpenCloud())
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="personal-drive-1",
            vendor_meta={
                "kind": "user_home",
                "username": "user-1",
                # Graph responses persist the PUBLIC URL — the spec builder
                # must ignore it and reconstruct from the internal base.
                "webdav_url": "https://cloud.public.example/dav/spaces/personal-drive-1",
            },
        )
        spec = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="project_default",
            target_path="/cloud/home",
            access="read_write",
            subject=CloudMountSubject(user_sub="kc-sub-1234", username="user-1"),
        )
        # Internal base URL reconstruction — NOT the persisted vendor_meta
        # webdav_url, which is the public URL from the graph response.
        assert (
            spec.source_config["url"]
            == f"{OPENCLOUD_BASE}/dav/spaces/personal-drive-1/"
        )
        assert spec.auth["type"] == "keycloak_user_impersonation"
        assert spec.auth["target_user_sub"] == "kc-sub-1234"
        assert spec.auth["client_secret"] == "test-secret"
        assert "token_helper" in spec.required_capabilities

    @pytest.mark.asyncio
    async def test_user_home_without_sub_raises_not_supported(self):
        be = OpenCloudBackend(_settings())
        _install_fake(be, FakeOpenCloud())
        handle = ProjectFolderHandle(
            backend="opencloud",
            native_id="personal-drive-1",
            vendor_meta={"kind": "user_home", "username": "user-1"},
        )
        with pytest.raises(CloudBackendError) as exc_info:
            await be.build_rclone_mount_spec(
                handle=handle,
                mount_kind="project_default",
                target_path="/cloud/home",
                access="read_write",
                subject=CloudMountSubject(username="user-1"),
            )
        assert exc_info.value.kind == CloudBackendErrorKind.NOT_SUPPORTED

    @pytest.mark.asyncio
    async def test_uninitialized_backend_raises_unavailable(self):
        be = OpenCloudBackend(_settings())
        handle = SessionFolderHandle(
            backend="opencloud", native_id="sessions/t", vendor_meta={}
        )
        with pytest.raises(CloudBackendError) as exc_info:
            await be.build_rclone_mount_spec(
                handle=handle,
                mount_kind="session_folder",
                target_path="/cloud/home",
                access="read_write",
            )
        assert exc_info.value.kind == CloudBackendErrorKind.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_payload_carries_min_rclone_version(self):
        be = OpenCloudBackend(_settings())
        _install_fake(be, FakeOpenCloud())
        handle = SessionFolderHandle(
            backend="opencloud",
            native_id="sessions/thread-1",
            vendor_meta={"drive_id": "drive-agent-home"},
        )
        spec = await be.build_rclone_mount_spec(
            handle=handle,
            mount_kind="session_folder",
            target_path="/cloud/home",
            access="read_write",
        )
        payload = spec.to_payload()
        assert payload["min_rclone_version"] == "1.70.0"
        assert payload["source"]["config"]["vendor"] == "infinitescale"
        assert payload["auth"]["type"] == "keycloak_client_credentials"


# -------------------------------------------------------- Token + graph flows


class TestTokenFlow:
    @pytest.mark.asyncio
    async def test_get_service_token_caches(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)

        t1 = await be._get_service_token()
        t2 = await be._get_service_token()
        assert t1 == t2 == "fake-access-token"
        # Only one token request despite two calls — cache is honored.
        assert len(fake.token_requests) == 1

    @pytest.mark.asyncio
    async def test_token_request_shape(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        await be._get_service_token()
        req = fake.token_requests[0]
        body = dict(p.split("=", 1) for p in req.content.decode().split("&"))
        assert body["grant_type"] == "client_credentials"
        assert body["client_id"] == "opencloud-orchestrator"
        assert body["client_secret"] == "test-secret"

    @pytest.mark.asyncio
    async def test_missing_token_raises_authentication_failed(self):
        be = OpenCloudBackend(_settings())

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"expires_in": 300})  # no access_token

        be._client = httpx.AsyncClient(
            base_url=OPENCLOUD_BASE,
            transport=httpx.MockTransport(handler),
        )
        be._initialized = True
        with pytest.raises(CloudBackendError) as exc_info:
            await be._get_service_token()
        assert exc_info.value.kind == CloudBackendErrorKind.AUTHENTICATION_FAILED


# ------------------------------------------------------------ Behavioral tests


class TestEnsureGroup:
    @pytest.mark.asyncio
    async def test_creates_missing_group(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        await be.ensure_group(GroupId("project-42"))
        created = [g for g in fake.groups.values() if g["displayName"] == "project-42"]
        assert len(created) == 1

    @pytest.mark.asyncio
    async def test_idempotent_when_present(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        fake.groups["g1"] = {"id": "g1", "displayName": "project-42"}
        _install_fake(be, fake)
        await be.ensure_group(GroupId("project-42"))
        assert len(fake.groups) == 1  # no duplicate created


class TestProjectFolder:
    @pytest.mark.asyncio
    async def test_ensure_creates_space_and_invites_group(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        fake.groups["g1"] = {"id": "g1", "displayName": "project-42"}
        _install_fake(be, fake)

        handle = await be.ensure_project_folder(
            project_name="TestSpace", group_id=GroupId("project-42")
        )
        assert handle.backend == "opencloud"
        assert handle.vendor_meta["mountpoint"] == "TestSpace"
        assert handle.vendor_meta["webdav_url"].startswith(
            f"{OPENCLOUD_BASE}/dav/spaces/"
        )
        # The drive should exist in the fake server.
        assert any(d["name"] == "TestSpace" for d in fake.drives.values())
        # An invite should have been posted under /root/invite.
        invites = [r for r in fake.graph_requests if "/root/invite" in r.url.path]
        assert len(invites) == 1
        invite_body = json.loads(invites[0].content)
        assert invite_body["recipients"][0]["@libre.graph.recipient.type"] == "group"
        assert invite_body["recipients"][0]["objectId"] == "g1"

    @pytest.mark.asyncio
    async def test_ensure_project_folder_raises_when_group_missing(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        with pytest.raises(CloudBackendError) as exc_info:
            await be.ensure_project_folder(project_name="X", group_id=GroupId("nope"))
        assert exc_info.value.kind == CloudBackendErrorKind.NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_two_step_disable_then_purge(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        fake.groups["g1"] = {"id": "g1", "displayName": "project-42"}
        _install_fake(be, fake)

        handle = await be.ensure_project_folder(
            project_name="TempSpace", group_id=GroupId("project-42")
        )
        await be.delete_project_folder(handle)

        # Both DELETE calls should have hit the same drive id.
        deletes = [
            r
            for r in fake.graph_requests
            if r.method == "DELETE" and r.url.path.startswith("/graph/v1.0/drives/")
        ]
        assert len(deletes) == 2
        # Second call carries Purge: T.
        assert deletes[0].headers.get("Purge") is None
        assert deletes[1].headers.get("Purge") == "T"
        # Drive should be gone from the fake.
        assert handle.native_id not in fake.drives

    @pytest.mark.asyncio
    async def test_delete_missing_with_if_exists_true(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        missing = ProjectFolderHandle(
            backend="opencloud", native_id="drive-nope", vendor_meta={}
        )
        # No raise.
        await be.delete_project_folder(missing)

    @pytest.mark.asyncio
    async def test_delete_missing_raises_when_if_exists_false(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        missing = ProjectFolderHandle(
            backend="opencloud", native_id="drive-nope", vendor_meta={}
        )
        with pytest.raises(CloudBackendError) as exc_info:
            await be.delete_project_folder(missing, if_exists=False)
        assert exc_info.value.kind == CloudBackendErrorKind.NOT_FOUND


class TestSessionFolder:
    @pytest.mark.asyncio
    async def test_ensure_creates_via_webdav_mkcol(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        handle = await be.ensure_session_folder(session_id="abc123")
        assert handle.native_id == "sessions/abc123"
        assert handle.vendor_meta["drive_id"] == "drive-agent-home"
        # Two MKCOLs: one for "sessions", one for "sessions/abc123".
        mkcols = [r for r in fake.webdav_requests if r.method == "MKCOL"]
        assert len(mkcols) == 2
        # Every MKCOL carries an Authorization header.
        for r in mkcols:
            assert r.headers.get("Authorization", "").startswith("Bearer ")

    @pytest.mark.asyncio
    async def test_an_exclusive_create_refuses_an_existing_folder(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        handle_webdav = fake._handle_webdav

        def existing(request: httpx.Request) -> httpx.Response:
            if request.method == "MKCOL":
                return httpx.Response(405)
            return handle_webdav(request)

        fake._handle_webdav = existing

        with pytest.raises(CloudBackendError) as exc_info:
            await be.ensure_session_folder(session_id="abc123", exclusive=True)
        assert exc_info.value.kind == CloudBackendErrorKind.ALREADY_EXISTS
        # A plain create reuses it, as a session's folder always has.
        handle = await be.ensure_session_folder(session_id="abc123")
        assert handle.native_id == "sessions/abc123"

    @pytest.mark.asyncio
    async def test_share_and_revoke_round_trip(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        handle = await be.ensure_session_folder(session_id="abc123")
        share = await be.share_session_folder(handle, UserId("user-7"))
        assert share.backend == "opencloud"
        assert share.native_id.startswith("perm-")
        assert share.vendor_meta["drive_id"] == "drive-agent-home"
        assert share.vendor_meta["item_id"].startswith("item-")
        # Revoke uses the stashed drive/item/permission ids.
        await be.revoke_session_share(share)
        revokes = [
            r
            for r in fake.graph_requests
            if r.method == "DELETE" and "/permissions/" in r.url.path
        ]
        assert len(revokes) == 1

    @pytest.mark.asyncio
    async def test_revoke_missing_vendor_meta_raises_invalid_request(self):
        be = OpenCloudBackend(_settings())
        fake = FakeOpenCloud()
        _install_fake(be, fake)
        bad_share = ShareHandle(backend="opencloud", native_id="perm-1", vendor_meta={})
        with pytest.raises(CloudBackendError) as exc_info:
            await be.revoke_session_share(bad_share)
        assert exc_info.value.kind == CloudBackendErrorKind.INVALID_REQUEST


class TestUninitialized:
    @pytest.mark.asyncio
    async def test_ensure_group_raises_when_uninitialized(self):
        be = OpenCloudBackend(_settings())
        with pytest.raises(CloudBackendError) as exc_info:
            await be.ensure_group(GroupId("x"))
        assert exc_info.value.kind == CloudBackendErrorKind.UNAVAILABLE


class TestErrorMapping:
    def _err(
        self,
        status: int,
        *,
        body: Optional[dict[str, Any]] = None,
    ) -> Callable[[], CloudBackendError]:
        def run() -> CloudBackendError:
            be = OpenCloudBackend(_settings())
            req = httpx.Request("GET", f"{OPENCLOUD_BASE}/graph/v1.0/me")
            resp = httpx.Response(status, json=body or {}, request=req)
            http_err = httpx.HTTPStatusError("boom", request=req, response=resp)
            return be._map_http_error(http_err)

        return run

    def test_404_maps_not_found(self):
        err = self._err(404)()
        assert err.kind == CloudBackendErrorKind.NOT_FOUND
        assert err.backend == "opencloud"
        assert err.retryable is False

    def test_401_maps_authentication_failed(self):
        assert self._err(401)().kind == CloudBackendErrorKind.AUTHENTICATION_FAILED

    def test_403_maps_permission_denied(self):
        assert self._err(403)().kind == CloudBackendErrorKind.PERMISSION_DENIED

    def test_429_maps_throttled_retryable(self):
        err = self._err(429)()
        assert err.kind == CloudBackendErrorKind.THROTTLED
        assert err.retryable is True

    def test_500_maps_unavailable_retryable(self):
        err = self._err(500)()
        assert err.kind == CloudBackendErrorKind.UNAVAILABLE
        assert err.retryable is True

    def test_unknown_status_maps_unknown(self):
        err = self._err(418)()
        assert err.kind == CloudBackendErrorKind.UNKNOWN
