"""Tests for NextcloudWorkspaceSync — transport delegation."""

from __future__ import annotations

import email.message
import io
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from agent.services.cloud_sync.nextcloud_sync import NextcloudWorkspaceSync
from shared.http_no_cookies import NoCookieJar


class _FakeWebDavClient:
    """Records calls webdav3 would otherwise make to a real server."""

    def __init__(self):
        self.uploads: list[dict] = []
        self.mkdirs: list[str] = []
        self.downloads: list[dict] = []
        self.deletes: list[str] = []
        self.delete_error: Exception | None = None
        self.list_returns: list = []

    def upload_sync(self, **kwargs):
        self.uploads.append(kwargs)

    # Commit-then-effects (step 4a): uploads/deletes go through
    # execute_request so RFC 4918 preconditions can be sent and the new ETag
    # read back. `requests` records (action, path, headers_ext).
    requests: list[tuple] = []
    request_error: Exception | None = None
    etag: str = '"etag-1"'

    def execute_request(self, action, path, data=None, headers_ext=None):
        self.requests.append((action, path, headers_ext))
        if action == "upload":
            self.uploads.append({"path": path, "bytes": data.read() if data else b""})
        if action == "clean":
            self.deletes.append(path)
            if self.delete_error is not None:
                raise self.delete_error
        if self.request_error is not None:
            raise self.request_error

        class _Response:
            headers = {"ETag": self.etag}

        return _Response()

    def info(self, path):
        return {"etag": self.etag}

    def mkdir(self, path):
        self.mkdirs.append(path)
        return True  # webdav3: True for 201 and for 405 (already exists)

    def download_sync(self, **kwargs):
        self.downloads.append(kwargs)

    def clean(self, path):
        self.deletes.append(path)
        if self.delete_error is not None:
            raise self.delete_error

    def list(self, _path, get_info=False):
        return self.list_returns


@pytest.fixture
def fake_client_factory(monkeypatch):
    fake = _FakeWebDavClient()
    monkeypatch.setattr(NextcloudWorkspaceSync, "_get_client", lambda _self: fake)
    return fake


@pytest.mark.asyncio
async def test_upload_delegates(tmp_path: Path, fake_client_factory):
    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    local = tmp_path / "local.txt"
    local.write_bytes(b"payload")
    etag = await sync._upload_file("foo/bar.txt", str(local))
    assert etag == '"etag-1"'
    assert fake_client_factory.uploads == [
        {"path": "/foo/bar.txt", "bytes": b"payload"}
    ]
    assert fake_client_factory.requests == [("upload", "/foo/bar.txt", None)]


@pytest.mark.asyncio
async def test_upload_sends_preconditions_and_maps_412_to_fence_lost(
    tmp_path: Path, fake_client_factory
):
    from agent.services.cloud_sync.base import CloudSyncFenceLost

    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    local = tmp_path / "local.txt"
    local.write_bytes(b"payload")
    await sync._upload_file("a.txt", str(local), if_match='"e0"')
    await sync._upload_file("b.txt", str(local), if_none_match=True)
    assert fake_client_factory.requests[-2:] == [
        ("upload", "/a.txt", ['If-Match: "e0"']),
        ("upload", "/b.txt", ["If-None-Match: *"]),
    ]

    class ResponseErrorCode(Exception):
        def __init__(self, code):
            self.code = code

    fake_client_factory.request_error = ResponseErrorCode(412)
    with pytest.raises(CloudSyncFenceLost):
        await sync._upload_file("c.txt", str(local), if_match='"stale"')
    with pytest.raises(CloudSyncFenceLost):
        await sync._delete_remote_file("c.txt", if_match='"stale"')
    fake_client_factory.request_error = ResponseErrorCode(500)
    with pytest.raises(ResponseErrorCode):
        await sync._upload_file("d.txt", str(local))
    assert await sync._remote_etag("a.txt") == '"etag-1"'


@pytest.mark.asyncio
async def test_mkdir_delegates(tmp_path: Path, fake_client_factory):
    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    await sync._ensure_remote_dir("a/b")
    assert fake_client_factory.mkdirs == ["a/b"]


@pytest.mark.asyncio
async def test_list_strips_base_path(tmp_path: Path, fake_client_factory):
    fake_client_factory.list_returns = [
        {
            "path": "/remote.php/dav/files/agent/sess/a.txt",
            "etag": "v1",
            "isdir": False,
        },
        {
            "path": "/remote.php/dav/files/agent/sess/dir/",
            "etag": "",
            "isdir": True,
        },
    ]
    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    out = await sync._list_remote_files()
    rels = [item["path"] for item in out]
    assert "a.txt" in rels
    assert "dir/" in rels or "dir" in rels


@pytest.mark.asyncio
async def test_download_delegates(tmp_path: Path, fake_client_factory):
    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    await sync._download_file("a.txt", "/tmp/out.txt")
    assert fake_client_factory.downloads == [
        {"remote_path": "a.txt", "local_path": "/tmp/out.txt"}
    ]


@pytest.mark.asyncio
async def test_generation_delete_is_idempotent_when_resource_already_missing(
    tmp_path: Path, fake_client_factory
):
    class RemoteResourceNotFound(Exception):
        pass

    fake_client_factory.delete_error = RemoteResourceNotFound("already gone")
    sync = NextcloudWorkspaceSync(
        tmp_path,
        webdav_url="http://nc/remote.php/dav/files/agent/sess/",
        webdav_user="agent",
        webdav_password="pw",
    )
    before_write_calls = 0

    async def before_write():
        nonlocal before_write_calls
        before_write_calls += 1

    await sync._delete_remote_file("deleted.txt", before_write=before_write)

    assert fake_client_factory.deletes == ["/deleted.txt"]
    assert before_write_calls == 1


# ---------------------------------------------------------------------------
# The real clients: _get_client()'s webdav3 Client and the infinity-PROPFIND
# httpx client, against a Nextcloud stand-in that sets session cookies on
# every answer the way Nextcloud does for a Basic-auth request.
# ---------------------------------------------------------------------------

_BASE = "/remote.php/dav/files/agent/sess"
_SET_COOKIES = [
    ("Set-Cookie", "cookie_test=test; path=/"),
    ("Set-Cookie", "oc_sessionPassphrase=secret; path=/; secure; HttpOnly"),
]


class _RawResponse(io.BytesIO):
    """Just enough of urllib3's response for requests to read body and cookies."""

    def __init__(self, body: bytes, headers: list[tuple[str, str]]) -> None:
        super().__init__(body)
        message = email.message.Message()
        for name, value in headers:
            message[name] = value
        self._original_response = SimpleNamespace(msg=message)


class _NextcloudAdapter(BaseAdapter):
    """Routes ``(method, path)`` to a canned answer; records what was sent.

    Anything unrouted is a 404, as Nextcloud answers for a missing resource.
    """

    def __init__(self, routes: dict[tuple[str, str], tuple[int, dict, bytes]]):
        super().__init__()
        self.routes = routes
        self.seen: list[tuple[str, str, str | None]] = []

    def send(self, request, **kwargs):  # noqa: ANN001, ANN003
        path = urlsplit(request.url).path
        self.seen.append((request.method, path, request.headers.get("Cookie")))
        status, headers, body = self.routes.get((request.method, path), (404, {}, b""))
        response = requests.Response()
        response.status_code = status
        response.headers = CaseInsensitiveDict(dict(headers))
        response.raw = _RawResponse(body, list(headers.items()) + _SET_COOKIES)
        response.url = request.url
        response.request = request
        return response

    def close(self) -> None:
        pass


def _real_sync(tmp_path: Path) -> NextcloudWorkspaceSync:
    return NextcloudWorkspaceSync(
        tmp_path,
        webdav_url=f"http://nc{_BASE}/",
        webdav_user="agent",
        webdav_password="pw",
    )


def _mount(sync: NextcloudWorkspaceSync, routes) -> _NextcloudAdapter:
    adapter = _NextcloudAdapter(routes)
    sync._get_client().session.mount("http://", adapter)
    return adapter


@pytest.mark.asyncio
async def test_webdav_client_never_sends_a_cookie(tmp_path: Path):
    sync = _real_sync(tmp_path)
    adapter = _mount(
        sync,
        {
            ("PUT", f"{_BASE}/a.txt"): (201, {"ETag": '"e1"'}, b""),
            ("PUT", f"{_BASE}/b.txt"): (201, {"ETag": '"e2"'}, b""),
        },
    )
    local = tmp_path / "local.txt"
    local.write_bytes(b"payload")
    assert await sync._upload_file("a.txt", str(local)) == '"e1"'
    assert await sync._upload_file("b.txt", str(local)) == '"e2"'
    assert [cookie for _method, _path, cookie in adapter.seen] == [None, None]
    assert isinstance(sync._get_client().session.cookies, NoCookieJar)
    assert len(sync._get_client().session.cookies) == 0


def _multistatus(*hrefs: str) -> str:
    responses = "".join(
        f"<d:response><d:href>{href}</d:href><d:propstat><d:prop>"
        '<d:getetag>"e"</d:getetag><d:getcontentlength>1</d:getcontentlength>'
        "<d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status>"
        "</d:propstat></d:response>"
        for href in hrefs
    )
    return f'<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">{responses}</d:multistatus>'


def _mock_http(monkeypatch, handler) -> dict:
    """Route the lazily built infinity-PROPFIND client through ``handler``."""

    built: dict = {}
    real = httpx.AsyncClient

    def build(**kwargs):
        built.update(kwargs)
        return real(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", build)
    return built


@pytest.mark.asyncio
async def test_depth_infinity_client_never_sends_a_cookie(tmp_path, monkeypatch):
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("cookie"))
        return httpx.Response(
            207, headers=_SET_COOKIES, text=_multistatus(f"{_BASE}/a.txt")
        )

    built = _mock_http(monkeypatch, handler)
    sync = _real_sync(tmp_path)
    try:
        for _ in range(2):
            listing = await sync._list_remote_tree_fast()
            assert [item["path"] for item in listing] == ["a.txt"]
        assert isinstance(built["cookies"], NoCookieJar)
        assert len(sync._http.cookies.jar) == 0
    finally:
        await sync.aclose()
    assert seen == [None, None]


_FINITE_DEPTH = (
    '<?xml version="1.0"?><d:error xmlns:d="DAV:"><d:propfind-finite-depth/></d:error>'
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, body, refused",
    [
        (400, "", True),
        (405, "", True),
        (501, "", True),
        (403, _FINITE_DEPTH, True),
        (403, "", False),  # a permission answer, not a capability verdict
        (401, "", False),
        (404, "", False),  # the folder is created by the first push
        (423, "", False),
        (429, "", False),
        (500, "", False),
        (503, "", False),
    ],
)
async def test_only_a_refusal_turns_the_depth_infinity_listing_off(
    tmp_path, monkeypatch, status, body, refused
):
    answers = [httpx.Response(status, text=body)]
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["depth"])
        if answers:
            return answers.pop(0)
        return httpx.Response(207, text=_multistatus(f"{_BASE}/a.txt"))

    _mock_http(monkeypatch, handler)
    sync = _real_sync(tmp_path)
    try:
        assert await sync._list_remote_tree_fast() is None  # this pull walks
        assert sync._depth_infinity_unsupported is refused
        again = await sync._list_remote_tree_fast()
    finally:
        await sync.aclose()
    if refused:
        assert again is None and seen == ["infinity"]  # no doomed probe again
    else:
        assert [item["path"] for item in again] == ["a.txt"]
        assert seen == ["infinity", "infinity"]


# webdav_disable_check: the HEAD probes are gone, the 404 semantics are not.


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [201, 405])
async def test_mkdir_sends_one_mkcol_and_no_parent_probe(tmp_path, status):
    sync = _real_sync(tmp_path)
    adapter = _mount(sync, {("MKCOL", f"{_BASE}/a/b/"): (status, {}, b"")})
    await sync._ensure_remote_dir("a/b")  # 405 = already there, not an error
    assert [(m, p) for m, p, _c in adapter.seen] == [("MKCOL", f"{_BASE}/a/b/")]


@pytest.mark.asyncio
async def test_mkdir_under_a_missing_parent_is_a_swallowed_409(tmp_path):
    sync = _real_sync(tmp_path)
    adapter = _mount(sync, {("MKCOL", f"{_BASE}/a/"): (409, {}, b"")})
    with pytest.raises(Exception) as excinfo:
        await sync._ensure_remote_dir("a")
    assert getattr(excinfo.value, "code", None) == 409
    await sync._ensure_remote_dirs("a")  # the algorithm swallows it, as before
    assert [m for m, _p, _c in adapter.seen] == ["MKCOL", "MKCOL"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, cached", [(201, True), (405, True), (409, False), (503, False)]
)
async def test_only_a_created_or_existing_directory_is_cached(tmp_path, status, cached):
    sync = _real_sync(tmp_path)
    routes = {("MKCOL", f"{_BASE}/a/"): (status, {}, b"")}
    adapter = _mount(sync, routes)
    await sync._ensure_remote_dirs("a")
    assert ("a" in sync._remote_dirs) is cached
    routes[("MKCOL", f"{_BASE}/a/")] = (201, {}, b"")
    await sync._ensure_remote_dirs("a")  # a failure is asked again, once
    assert "a" in sync._remote_dirs
    await sync._ensure_remote_dirs("a")
    assert len(adapter.seen) == (1 if cached else 2)


@pytest.mark.asyncio
async def test_a_404_still_reads_as_missing_without_a_head_probe(tmp_path):
    sync = _real_sync(tmp_path)
    adapter = _mount(sync, {})
    assert await sync._remote_etag("gone.txt") is None
    with pytest.raises(Exception) as listed:
        await sync._list_remote_files("gone")
    assert type(listed.value).__name__ == "RemoteResourceNotFound"
    assert sync._marker_missing(listed.value)
    marker = await sync.read_sync_generation_marker(
        thread_id="t", sync_scope_sha256="a" * 64
    )
    assert marker is None
    methods = [m for m, _p, _c in adapter.seen]
    assert methods == ["PROPFIND", "PROPFIND", "PROPFIND"]


@pytest.mark.asyncio
async def test_download_is_two_propfinds_and_a_get(tmp_path):
    sync = _real_sync(tmp_path)
    href = f"{_BASE}/a.txt"
    adapter = _mount(
        sync,
        {
            ("PROPFIND", href): (207, {}, _multistatus(href).encode()),
            ("GET", href): (200, {}, b"payload"),
        },
    )
    out = tmp_path / "out.txt"
    await sync._download_file("a.txt", str(out))
    assert out.read_bytes() == b"payload"
    assert [m for m, _p, _c in adapter.seen] == ["PROPFIND", "PROPFIND", "GET"]
