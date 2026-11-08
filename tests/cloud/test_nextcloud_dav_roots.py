"""Nextcloud under a webroot it writes into its hrefs (single-origin ``/cloud``).

With ``overwritewebroot=/cloud`` Nextcloud answers every caller, also one that
reached it without the prefix, with ``/cloud/remote.php/dav/...`` hrefs and a
``/cloud/remote.php/dav/`` sabre base URI. ``NextcloudBackend`` learns that
root in ``ensure_initialized`` (``_discover_dav_roots``) and uses it for WebDAV
only: hrefs are compared against the href root, requests and the URLs it hands
rclone go through the request root when the chart's alias serves it.
Issue: knowledge-base/knowledge/issues/single_origin_cloud_folder_listings_come_back_empty.md
"""

from __future__ import annotations

import logging
from urllib.parse import quote, unquote

import httpx
import pytest

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudMountSubject,
    NextcloudBackend,
    SessionFolderHandle,
)
from orchestrator.services.cloud._propfind import (
    first_href_path,
    hrefs_outside_prefix,
    parse_propfind_entries,
)
from orchestrator.services.cloud.config import NextcloudSettings
from tests.cloud.test_nextcloud import (
    AGENT_USER,
    MOUNTPOINT,
    FakeNextcloud,
    _base_prefix,
    _handle,
)
from orchestrator.services.cloud.folder_grants import principal_ids

INTERNAL = "http://srw-nextcloud"
PUBLIC = "https://localhost:8443/cloud"
_WRITER = principal_ids("writer", "22222222-2222-4222-8222-222222222222")[0]
DAV = "/remote.php/dav/"


def _settings(base_url: str = INTERNAL, public_url: str = PUBLIC) -> NextcloudSettings:
    return NextcloudSettings(
        base_url=base_url,
        public_url=public_url,
        admin_user="admin",
        admin_password="admin",
        agent_user=AGENT_USER,
        agent_password="agent-service-dev",
    )


def _multistatus(*hrefs: str) -> str:
    return (
        '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">'
        + "".join(f"<d:response><d:href>{h}</d:href></d:response>" for h in hrefs)
        + "</d:multistatus>"
    )


class WebrootNextcloud(FakeNextcloud):
    """``FakeNextcloud`` that writes ``href_root`` into every href, like a
    Nextcloud with ``overwritewebroot``. ``serves_root`` / ``serves_prefixed``
    say whether requests without / with the prefix reach it (the chart's
    Apache alias adds the latter)."""

    def __init__(
        self,
        *,
        href_root: str = "/cloud",
        serves_root: bool = True,
        serves_prefixed: bool = True,
    ) -> None:
        super().__init__()
        self.href_root = href_root
        self.serves_root = serves_root
        self.serves_prefixed = serves_prefixed
        self.base = f"{href_root}{_base_prefix()}"
        self.decoded_base = unquote(self.base)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = unquote(request.url.path)
        if request.method == "PROPFIND" and path in (DAV, f"{self.href_root}{DAV}"):
            self.requests.append(request)
            prefixed = path != DAV
            if (prefixed and not self.serves_prefixed) or (
                not prefixed and not self.serves_root
            ):
                return httpx.Response(405)
            return httpx.Response(207, text=_multistatus(f"{self.href_root}{DAV}"))
        return super().handler(request)

    def _rel(self, request: httpx.Request) -> str | None:
        path = unquote(request.url.path)
        accepted = []
        if self.serves_prefixed:
            accepted.append(self.decoded_base)
        if self.serves_root:
            accepted.append(unquote(_base_prefix()))
        for prefix in accepted:
            if path.startswith(prefix):
                return path[len(prefix) :].strip("/")
        return None


def _wire(backend: NextcloudBackend, handler, base_url: str = INTERNAL) -> None:
    backend._client = httpx.AsyncClient(
        base_url=base_url, transport=httpx.MockTransport(handler)
    )
    backend._agent_client = backend._build_agent_client(httpx.MockTransport(handler))
    backend._initialized = True
    backend._agent_user = AGENT_USER
    backend._agent_password = "pw"


async def _discovered(fake, base_url: str = INTERNAL) -> NextcloudBackend:
    backend = NextcloudBackend(_settings(base_url=base_url))
    _wire(backend, fake.handler, base_url)
    await backend._discover_dav_roots()
    return backend


# ------------------------------------------------------------------ discovery


class TestDiscovery:
    @pytest.mark.asyncio
    async def test_an_install_without_a_webroot_keeps_unprefixed_paths(self):
        backend = await _discovered(WebrootNextcloud(href_root=""))
        assert (backend.dav_href_root, backend.dav_request_root) == ("", "")

    @pytest.mark.asyncio
    async def test_single_origin_with_the_alias_uses_cloud_for_both(self):
        fake = WebrootNextcloud()
        backend = await _discovered(fake)
        assert (backend.dav_href_root, backend.dav_request_root) == ("/cloud", "/cloud")
        # The request root was verified before it was trusted.
        assert [unquote(r.url.path) for r in fake.requests] == [DAV, f"/cloud{DAV}"]

    @pytest.mark.asyncio
    async def test_single_origin_without_the_alias_names_the_problem(self, caplog):
        caplog.set_level(logging.WARNING)
        backend = await _discovered(WebrootNextcloud(serves_prefixed=False))
        assert (backend.dav_href_root, backend.dav_request_root) == ("/cloud", "")
        assert "rclone mounts will list nothing" in caplog.text

    @pytest.mark.asyncio
    async def test_an_external_nextcloud_served_under_its_base_path(self):
        base = "https://ext.example/nextcloud"
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(207, text=_multistatus(f"/nextcloud{DAV}"))

        backend = NextcloudBackend(_settings(base_url=base, public_url=base))
        _wire(backend, handler, base)
        await backend._discover_dav_roots()
        assert (backend.dav_href_root, backend.dav_request_root) == ("/nextcloud", "")
        assert seen == [f"/nextcloud{DAV}"]  # no second probe

    @pytest.mark.asyncio
    async def test_a_stripping_proxy_without_overwritewebroot_is_unchanged(self):
        base = "https://ext.example/nextcloud"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(207, text=_multistatus(DAV))

        backend = NextcloudBackend(_settings(base_url=base, public_url=base))
        _wire(backend, handler, base)
        await backend._discover_dav_roots()
        assert (backend.dav_href_root, backend.dav_request_root) == ("", "")

    @pytest.mark.asyncio
    async def test_a_definitive_refusal_keeps_todays_behaviour(self, caplog):
        caplog.set_level(logging.WARNING)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401)

        backend = NextcloudBackend(_settings())
        _wire(backend, handler)
        await backend._discover_dav_roots()
        assert (backend.dav_href_root, backend.dav_request_root) == ("", "")
        assert "not discoverable" in caplog.text

    @pytest.mark.asyncio
    async def test_a_server_error_fails_initialization(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        backend = NextcloudBackend(_settings())
        _wire(backend, handler)
        with pytest.raises(httpx.HTTPStatusError):
            await backend._discover_dav_roots()


# ------------------------------------------------------------- folder walks


class TestWalkUnderAWebroot:
    @pytest.mark.asyncio
    async def test_the_walk_lists_the_folder_through_the_alias(self):
        fake = WebrootNextcloud()
        fake.add_file("Company/menu.csv", b"a,b")
        fake.add_file("notes.md", b"hi")
        backend = await _discovered(fake)
        fake.requests.clear()

        entries = await backend.list_project_folder(_handle())

        assert {e.path for e in entries} == {"Company", "Company/menu.csv", "notes.md"}
        assert all(
            unquote(r.url.path).startswith(f"/cloud{unquote(_base_prefix())}")
            for r in fake.requests
        )

    @pytest.mark.asyncio
    async def test_the_walk_lists_the_folder_even_without_the_alias(self):
        """The 0.1.1 wire (unprefixed request, prefixed hrefs) lists once the
        href root is known."""
        fake = WebrootNextcloud(serves_prefixed=False)
        fake.add_file("Company/menu.csv", b"a,b")
        backend = await _discovered(fake)

        entries = await backend.list_project_folder(_handle())

        assert {e.path for e in entries} == {"Company", "Company/menu.csv"}

    @pytest.mark.asyncio
    async def test_foreign_hrefs_fail_loudly_instead_of_reading_as_empty(self):
        """Without the discovered root (0.1.1's behaviour) every entry was
        dropped and the folder read as empty; now the walk refuses."""
        fake = WebrootNextcloud()
        fake.add_file("notes.md", b"hi")
        backend = NextcloudBackend(_settings())
        _wire(backend, fake.handler)  # no discovery: roots stay ""

        with pytest.raises(CloudBackendError) as caught:
            await backend.list_project_folder(_handle())
        assert "do not start with the expected WebDAV root" in str(caught.value)

    @pytest.mark.asyncio
    async def test_the_etag_baseline_sees_the_files(self):
        fake = WebrootNextcloud()
        fake.add_file("a.txt", b"1")
        fake.add_file("d/b.txt", b"2")
        backend = await _discovered(fake)

        baseline = await backend.capture_etag_baseline(_handle())

        assert set(baseline) == {"a.txt", "d/b.txt"}

    @pytest.mark.asyncio
    async def test_bytes_round_trip_under_the_request_root(self):
        fake = WebrootNextcloud()
        backend = await _discovered(fake)
        fake.requests.clear()

        await backend.put_project_folder_file_bytes(
            _handle(), path="x/y.txt", content=b"payload"
        )
        got = await backend.get_project_folder_file_bytes(_handle(), path="x/y.txt")

        assert got == b"payload"
        assert fake.requests
        assert all(unquote(r.url.path).startswith("/cloud/") for r in fake.requests)


# --------------------------------------------------------------- URLs handed out


class TestUrlsHandedOut:
    def _backend(self, *, request_root: str, href_root: str | None = None):
        backend = NextcloudBackend(_settings())
        backend._dav_request_root = request_root
        backend._dav_href_root = request_root if href_root is None else href_root
        backend._initialized = True
        return backend

    @pytest.mark.asyncio
    async def test_a_project_mount_url_carries_the_request_root(self):
        backend = self._backend(request_root="/cloud")
        spec = await backend.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
            subject=CloudMountSubject(username=_WRITER, credential="pw"),
        )
        assert spec.source_config["url"] == (
            f"{INTERNAL}/cloud/remote.php/dav/groupfolders/{_WRITER}/"
            f"{quote(MOUNTPOINT, safe='')}/"
        )

    @pytest.mark.asyncio
    async def test_the_public_swap_never_doubles_the_webroot(self):
        backend = self._backend(request_root="/cloud")
        spec = await backend.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
            subject=CloudMountSubject(username=_WRITER, credential="pw"),
            prefer_public_url=True,
        )
        assert spec.source_config["url"].startswith(
            f"{PUBLIC}/remote.php/dav/groupfolders/{_WRITER}/"
        )
        assert "/cloud/cloud/" not in spec.source_config["url"]

    @pytest.mark.asyncio
    async def test_the_public_swap_without_a_request_root_is_unchanged(self):
        backend = self._backend(request_root="", href_root="/cloud")
        spec = await backend.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
            subject=CloudMountSubject(username=_WRITER, credential="pw"),
            prefer_public_url=True,
        )
        assert spec.source_config["url"].startswith(f"{PUBLIC}/remote.php/dav/")

    def test_session_grant_reader_and_home_urls_carry_the_request_root(self):
        backend = self._backend(request_root="/cloud")
        session = backend.get_session_folder_webdav_url(
            SessionFolderHandle(backend="nextcloud", native_id="sessions/s1")
        )
        grant = backend.folder_grant_webdav_url(_handle(), _WRITER)
        legacy = backend.get_project_folder_webdav_url(_handle())
        assert (
            session
            == f"{INTERNAL}/cloud/remote.php/dav/files/{AGENT_USER}/sessions/s1/"
        )
        assert grant.startswith(f"{INTERNAL}/cloud/remote.php/dav/groupfolders/")
        assert legacy.startswith(f"{INTERNAL}/cloud/remote.php/dav/groupfolders/")

    @pytest.mark.asyncio
    async def test_the_user_home_url_carries_the_request_root(self):
        backend = self._backend(request_root="/cloud")
        home = await backend.get_user_home("alice")
        assert home is not None
        assert home.webdav_url == f"{INTERNAL}/cloud/remote.php/dav/files/alice/"

    def test_an_install_without_a_webroot_hands_out_todays_urls(self):
        backend = self._backend(request_root="")
        assert backend.folder_grant_webdav_url(_handle(), _WRITER).startswith(
            f"{INTERNAL}/remote.php/dav/groupfolders/"
        )


# ---------------------------------------------------------------- href helpers


class TestHrefHelpers:
    def test_first_href_path_decodes_and_drops_the_origin(self):
        body = _multistatus("https://h.example/cloud/remote.php/dav/a%20b/")
        assert first_href_path(body) == "/cloud/remote.php/dav/a b/"
        assert first_href_path("<d:multistatus/>") is None

    def test_hrefs_outside_prefix(self):
        prefix = "/cloud/remote.php/dav/groupfolders/u/M/"
        assert hrefs_outside_prefix(
            _multistatus("/remote.php/dav/groupfolders/u/M/"), href_prefix=prefix
        )
        assert not hrefs_outside_prefix(
            _multistatus("/cloud/remote.php/dav/groupfolders/u/M/"), href_prefix=prefix
        )
        # No responses at all is not a mismatch.
        assert not hrefs_outside_prefix(_multistatus(), href_prefix=prefix)

    def test_parse_accepts_absolute_uri_hrefs(self):
        """RFC 4918 §8.3 allows a full URI; only its path is compared."""
        body = (
            '<d:multistatus xmlns:d="DAV:"><d:response>'
            "<d:href>http://nc/remote.php/dav/groupfolders/u/M/f.txt</d:href>"
            "<d:propstat><d:prop><d:resourcetype/></d:prop></d:propstat>"
            "</d:response></d:multistatus>"
        )
        entries = parse_propfind_entries(
            body, href_prefix="/remote.php/dav/groupfolders/u/M/"
        )
        assert [e.path for e in entries] == ["f.txt"]
