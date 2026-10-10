"""Unit tests for ``NextcloudBackend``'s byte-level project-folder methods.

Stands the adapter up against an ``httpx.MockTransport`` backed by a small
in-memory WebDAV server (``FakeNextcloud``) that speaks the Group Folders
DAV endpoint (``/remote.php/dav/groupfolders/<user>/<mountpoint>/``). Covers
the four methods that back Mode-A job cloud export: ``list_project_folder``
(recursive ``Depth: 1`` walk), ``get_project_folder_file_bytes``,
``put_project_folder_file_bytes`` (MKCOL parents then PUT), and
``delete_project_folder_file``.

The fake's PROPFIND bodies mirror the real sabre/dav multistatus shape
captured from a live Nextcloud (``xmlns:s="http://sabredav.org/ns"``,
entity-encoded etags, server-absolute URL-encoded hrefs) so the shared
``parse_propfind_entries`` is exercised against a realistic body.
"""

from __future__ import annotations

from urllib.parse import quote, unquote

import httpx
import pytest

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    NextcloudBackend,
    ProjectFolderHandle,
)
from orchestrator.services.cloud.config import NextcloudSettings

NEXTCLOUD_BASE = "https://nc.example.com"
AGENT_USER = "agent-service"
# Mountpoint with a space → exercises URL-encoding end-to-end (the real
# "NC Validation Project" group folder has one too).
MOUNTPOINT = "Test Project"


def _nc_test_settings() -> NextcloudSettings:
    """Minimal valid ``NextcloudSettings`` for constructing the backend.

    Only ``agent_user`` (→ the groupfolders DAV path) actually matters for
    routing; the fake transport is injected via ``_install_fake`` afterwards.
    """
    return NextcloudSettings(
        base_url=NEXTCLOUD_BASE,
        public_url=NEXTCLOUD_BASE,
        admin_user="admin",
        admin_password="admin",
        agent_user=AGENT_USER,
        agent_password="agent-service-dev",
    )


def _base_prefix() -> str:
    return f"/remote.php/dav/groupfolders/{AGENT_USER}/{quote(MOUNTPOINT, safe='')}"


def _handle(*, mountpoint: str | None = MOUNTPOINT) -> ProjectFolderHandle:
    meta = {"mountpoint": mountpoint} if mountpoint is not None else {}
    return ProjectFolderHandle(backend="nextcloud", native_id="2", vendor_meta=meta)


# ----------------------------------------------------------------------- Fake


class FakeNextcloud:
    """In-memory WebDAV stand-in for a Nextcloud Group Folder.

    Stores file bytes + directories relative to the folder root and answers
    PROPFIND (Depth: 1), GET, PUT, MKCOL, DELETE against the groupfolders
    DAV endpoint.
    """

    def __init__(self) -> None:
        self.base = _base_prefix()
        self.decoded_base = unquote(self.base)
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = set()
        self.etags: dict[str, str] = {}
        self.requests: list[httpx.Request] = []
        self._etag_seq = 0
        # When True, a ``Depth: infinity`` PROPFIND is answered 400 (mimics
        # sabre/dav with infinity disabled) so the etag baseline falls back to
        # the Depth:1 BFS. Default False: the fake honors infinity.
        self.reject_infinity = False

    # ---- seeding helper
    def add_file(self, relpath: str, content: bytes) -> None:
        relpath = relpath.strip("/")
        self.files[relpath] = content
        self.etags[relpath] = self._next_etag()
        parts = relpath.split("/")[:-1]
        for i in range(len(parts)):
            self.dirs.add("/".join(parts[: i + 1]))

    def _next_etag(self) -> str:
        self._etag_seq += 1
        return f"etag{self._etag_seq}"

    # ---- dispatch
    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        rel = self._rel(request)
        if rel is None:
            return httpx.Response(404, content=b"outside group folder")
        method = request.method
        if method == "PROPFIND":
            return self._propfind(rel, request.headers.get("Depth", "1"))
        if method == "GET":
            return self._get(rel)
        if method == "PUT":
            return self._put(rel, bytes(request.content))
        if method == "MKCOL":
            return self._mkcol(rel)
        if method == "DELETE":
            return self._delete(rel)
        return httpx.Response(405, content=f"unexpected {method}".encode())

    def _rel(self, request: httpx.Request) -> str | None:
        path = unquote(request.url.path)
        if not path.startswith(self.decoded_base):
            return None
        return path[len(self.decoded_base) :].strip("/")

    # ---- WebDAV verbs
    def _propfind(self, rel: str, depth: str = "1") -> httpx.Response:
        if rel and rel not in self.dirs:
            return httpx.Response(404, content=self._dav_error())
        if depth == "infinity":
            if self.reject_infinity:
                return httpx.Response(400, content=self._dav_error())
            members = self._descendants(rel)
        else:
            members = self._children(rel)
        # The collection itself + the selected members (children or all descendants).
        blocks = [self._block(rel, is_dir=True)]
        for child, is_dir in sorted(members.items()):
            blocks.append(self._block(child, is_dir=is_dir))
        body = (
            '<?xml version="1.0"?>'
            '<d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" '
            'xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">'
            + "".join(blocks)
            + "</d:multistatus>"
        )
        return httpx.Response(
            207,
            content=body.encode(),
            headers={"Content-Type": "application/xml; charset=utf-8"},
        )

    def _get(self, rel: str) -> httpx.Response:
        if rel in self.files:
            return httpx.Response(
                200,
                content=self.files[rel],
                headers={"Content-Type": self._ctype(rel)},
            )
        return httpx.Response(404, content=self._dav_error())

    def _put(self, rel: str, content: bytes) -> httpx.Response:
        if not rel:
            return httpx.Response(409, content=self._dav_error())
        parent = "/".join(rel.split("/")[:-1])
        if parent and parent not in self.dirs:
            # Real WebDAV: PUT into a missing collection → 409 Conflict.
            return httpx.Response(409, content=self._dav_error())
        self.files[rel] = content
        self.etags[rel] = self._next_etag()
        return httpx.Response(201)

    def _mkcol(self, rel: str) -> httpx.Response:
        if not rel or rel in self.dirs:
            return httpx.Response(405)  # exists / not allowed on root
        parent = "/".join(rel.split("/")[:-1])
        if parent and parent not in self.dirs:
            return httpx.Response(409, content=self._dav_error())
        self.dirs.add(rel)
        return httpx.Response(201)

    def _delete(self, rel: str) -> httpx.Response:
        if rel in self.files:
            del self.files[rel]
            self.etags.pop(rel, None)
            return httpx.Response(204)
        if rel in self.dirs:
            self.dirs.discard(rel)
            return httpx.Response(204)
        return httpx.Response(404, content=self._dav_error())

    # ---- helpers
    def _descendants(self, current: str) -> dict[str, bool]:
        """Every file + dir at any depth under ``current`` → {relpath: is_dir}
        (for a ``Depth: infinity`` PROPFIND)."""
        prefix = (current + "/") if current else ""
        members: dict[str, bool] = {}
        for f in self.files:
            if f.startswith(prefix) and f != current:
                members[f] = False
        for d in self.dirs:
            if d.startswith(prefix) and d != current:
                members[d] = True
        return members

    def _children(self, current: str) -> dict[str, bool]:
        """Immediate children of ``current`` → {relpath: is_dir}."""
        prefix = (current + "/") if current else ""
        children: dict[str, bool] = {}
        for f in self.files:
            if f.startswith(prefix):
                rest = f[len(prefix) :]
                if "/" not in rest:
                    children[f] = False
                else:
                    children[prefix + rest.split("/", 1)[0]] = True
        for d in self.dirs:
            if d.startswith(prefix):
                rest = d[len(prefix) :]
                if not rest:
                    continue
                if "/" not in rest:
                    children[d] = True
                else:
                    children[prefix + rest.split("/", 1)[0]] = True
        return children

    def _href(self, rel: str, is_dir: bool) -> str:
        if not rel:
            return f"{self.base}/"
        href = f"{self.base}/{quote(rel, safe='/')}"
        return href + "/" if is_dir else href

    def _block(self, rel: str, *, is_dir: bool) -> str:
        if is_dir:
            rtype = "<d:resourcetype><d:collection/></d:resourcetype>"
            size = ""
            ctype = ""
            etag_val = f"d-{rel or 'root'}"
        else:
            rtype = "<d:resourcetype/>"
            size = f"<d:getcontentlength>{len(self.files.get(rel, b''))}</d:getcontentlength>"
            ctype = f"<d:getcontenttype>{self._ctype(rel)}</d:getcontenttype>"
            etag_val = self.etags.get(rel, "0")
        return (
            "<d:response>"
            f"<d:href>{self._href(rel, is_dir)}</d:href>"
            "<d:propstat><d:prop>"
            f"{rtype}{size}{ctype}"
            f"<d:getetag>&quot;{etag_val}&quot;</d:getetag>"
            "</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
            "</d:response>"
        )

    @staticmethod
    def _ctype(rel: str) -> str:
        if rel.endswith((".txt", ".md")):
            return "text/plain"
        return "application/octet-stream"

    @staticmethod
    def _dav_error() -> bytes:
        return (
            b'<?xml version="1.0" encoding="utf-8"?>'
            b'<d:error xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns">'
            b"<s:exception>Sabre\\DAV\\Exception\\NotFound</s:exception></d:error>"
        )


class TestRemoveUserFromGroup:
    @pytest.mark.asyncio
    async def test_issues_delete_with_body_without_typeerror(self):
        # Regression: httpx's AsyncClient.delete() rejects a request body, so
        # the old `delete(..., data=...)` raised TypeError. It must send the
        # groupid via a DELETE request body.
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["method"] = request.method
            seen["path"] = request.url.path
            seen["content"] = request.content
            return httpx.Response(200, json={"ocs": {"meta": {"statuscode": 100}}})

        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(handler)
        )
        be._initialized = True

        await be.remove_user_from_group("srw-reader-a", "grp1")  # must not raise

        assert seen["method"] == "DELETE"
        assert seen["path"] == "/ocs/v2.php/cloud/users/srw-reader-a/groups"
        assert b"grp1" in seen["content"]  # groupid sent in the request body


def _install_fake(backend: NextcloudBackend, fake: FakeNextcloud) -> None:
    """Wire a pre-initialized adapter up to the fake server."""
    backend._client = httpx.AsyncClient(
        base_url=NEXTCLOUD_BASE,
        transport=httpx.MockTransport(fake.handler),
    )
    backend._initialized = True
    backend._agent_user = AGENT_USER
    backend._agent_password = "pw"


# --------------------------------------------------------------- list_project_folder


class TestListProjectFolder:
    @pytest.mark.asyncio
    async def test_lists_nested_files(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.txt", b"hello")
        fake.add_file("sub/b.txt", b"world!!")
        fake.add_file("sub/deep/c.txt", b"x")
        _install_fake(be, fake)

        entries = await be.list_project_folder(_handle())

        # Assert on the file-level view the consumer (job_cloud_baseline)
        # actually uses: dirs filtered out, keyed by path.
        files = {e.path: e for e in entries if not e.is_dir}
        assert set(files) == {"a.txt", "sub/b.txt", "sub/deep/c.txt"}
        assert files["a.txt"].size == 5
        assert files["sub/b.txt"].size == 7
        assert files["a.txt"].content_type == "text/plain"
        # Every file carries a captured etag, and each file appears once
        # (recursion found the nested ones without duplicating files).
        assert all(f.etag for f in files.values())
        file_paths = [e.path for e in entries if not e.is_dir]
        assert len(file_paths) == len(set(file_paths))

    @pytest.mark.asyncio
    async def test_does_not_double_count_subdirs(self):
        # Regression for the double-subdir bug (design §11.5): each Depth:1
        # PROPFIND of a subdir returns that subdir's own self-entry, which the
        # walker must drop (its parent already emitted it). Assert directory
        # entries — not just files — appear exactly once.
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("Documents/a.md", b"a")
        fake.add_file("Documents/Sub/b.md", b"b")
        _install_fake(be, fake)

        entries = await be.list_project_folder(_handle())
        paths = [e.path for e in entries]
        assert len(paths) == len(set(paths)), f"duplicate paths: {paths}"
        assert set(paths) == {
            "Documents",
            "Documents/a.md",
            "Documents/Sub",
            "Documents/Sub/b.md",
        }

    @pytest.mark.asyncio
    async def test_etag_is_stable_across_calls(self):
        # External-mod detection compares baseline vs. live etags, so a
        # re-list of an unchanged file must yield the same etag.
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.txt", b"hello")
        _install_fake(be, fake)

        first = {e.path: e.etag for e in await be.list_project_folder(_handle())}
        second = {e.path: e.etag for e in await be.list_project_folder(_handle())}
        assert first["a.txt"] and first == second

    @pytest.mark.asyncio
    async def test_empty_folder_has_no_files(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        entries = await be.list_project_folder(_handle())
        assert [e for e in entries if not e.is_dir] == []


class TestCaptureEtagBaseline:
    @pytest.mark.asyncio
    async def test_prefers_infinity_single_request(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.md", b"a")
        fake.add_file("knowledge-base/knowledge/b.md", b"b")
        _install_fake(be, fake)

        base = await be.capture_etag_baseline(_handle())

        assert set(base) == {
            "a.md",
            "knowledge-base/knowledge/b.md",
        }  # files only, no dirs
        assert all(v for v in base.values())  # etags populated
        # Exactly one PROPFIND — the infinity short-circuit, not a per-dir BFS.
        propfinds = [r for r in fake.requests if r.method == "PROPFIND"]
        assert len(propfinds) == 1
        assert propfinds[0].headers.get("Depth") == "infinity"

    @pytest.mark.asyncio
    async def test_falls_back_to_bfs_when_infinity_rejected(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.reject_infinity = True  # sabre with infinity disabled → 400
        fake.add_file("a.md", b"a")
        fake.add_file("knowledge-base/knowledge/b.md", b"b")
        _install_fake(be, fake)

        base = await be.capture_etag_baseline(_handle())

        assert set(base) == {
            "a.md",
            "knowledge-base/knowledge/b.md",
        }  # same result via BFS
        methods = [r for r in fake.requests if r.method == "PROPFIND"]
        # One rejected infinity attempt + at least the root + docs Depth:1 walks.
        assert any(r.headers.get("Depth") == "infinity" for r in methods)
        assert any(r.headers.get("Depth") == "1" for r in methods)

    @pytest.mark.asyncio
    async def test_missing_mountpoint_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.list_project_folder(_handle(mountpoint=None))
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- get_project_folder_file_bytes


class TestGetBytes:
    @pytest.mark.asyncio
    async def test_returns_bytes(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("knowledge-base/knowledge/readme.md", b"# hi\n")
        _install_fake(be, fake)
        blob = await be.get_project_folder_file_bytes(
            _handle(), path="knowledge-base/knowledge/readme.md"
        )
        assert blob == b"# hi\n"

    @pytest.mark.asyncio
    async def test_binary_survives(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        payload = bytes(range(256))
        fake.add_file("blob.bin", payload)
        _install_fake(be, fake)
        assert (
            await be.get_project_folder_file_bytes(_handle(), path="blob.bin")
            == payload
        )

    @pytest.mark.asyncio
    async def test_missing_raises_not_found(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.get_project_folder_file_bytes(_handle(), path="nope.txt")
        assert ei.value.kind == CloudBackendErrorKind.NOT_FOUND

    @pytest.mark.asyncio
    async def test_empty_path_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.get_project_folder_file_bytes(_handle(), path="")
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- put_project_folder_file_bytes


class TestPutBytes:
    @pytest.mark.asyncio
    async def test_creates_parents_and_round_trips(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(
            _handle(), path="x/y/z.txt", content=b"data", content_type="text/plain"
        )
        # Each parent collection was MKCOL'd on the way (in order).
        mkcols = [unquote(r.url.path) for r in fake.requests if r.method == "MKCOL"]
        assert any(p.endswith("/x") for p in mkcols)
        assert any(p.endswith("/x/y") for p in mkcols)
        # File is stored and reads back identically.
        assert (
            await be.get_project_folder_file_bytes(_handle(), path="x/y/z.txt")
            == b"data"
        )

    @pytest.mark.asyncio
    async def test_root_file_needs_no_parents(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(_handle(), path="top.txt", content=b"t")
        assert fake.files["top.txt"] == b"t"
        assert [r for r in fake.requests if r.method == "MKCOL"] == []

    @pytest.mark.asyncio
    async def test_empty_path_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.put_project_folder_file_bytes(_handle(), path="", content=b"x")
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- delete_project_folder_file


class TestDeleteFile:
    @pytest.mark.asyncio
    async def test_removes_file(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("gone.txt", b"x")
        _install_fake(be, fake)
        await be.delete_project_folder_file(_handle(), path="gone.txt")
        assert "gone.txt" not in fake.files

    @pytest.mark.asyncio
    async def test_missing_with_if_exists_true_is_noop(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        # No raise — the goal state ("gone") already holds.
        await be.delete_project_folder_file(_handle(), path="nope.txt")

    @pytest.mark.asyncio
    async def test_missing_with_if_exists_false_raises_not_found(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.delete_project_folder_file(
                _handle(), path="nope.txt", if_exists=False
            )
        assert ei.value.kind == CloudBackendErrorKind.NOT_FOUND


# ------------------------------------------------------------------- uninitialized


class TestUninitialized:
    @pytest.mark.asyncio
    async def test_list_uninitialized_raises_unavailable(self):
        be = NextcloudBackend(_nc_test_settings())  # never wired to a client
        with pytest.raises(CloudBackendError) as ei:
            await be.list_project_folder(_handle())
        assert ei.value.kind == CloudBackendErrorKind.UNAVAILABLE


class TestRcloneMountSpec:
    """rclone's nextcloud vendor builds chunked uploads from a
    /dav/files/<user>/ URL and refuses any other ("the remote url looks
    incorrect"): a Group Folder's /dav/groupfolders/ URL mounts with
    whole-file uploads, every other surface keeps chunking."""

    @pytest.mark.asyncio
    async def test_a_group_folder_mounts_without_chunked_uploads(self):
        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
        )
        assert spec.source_config["url"].startswith(
            f"{NEXTCLOUD_BASE}/remote.php/dav/groupfolders/{AGENT_USER}/"
        )
        assert spec.source_config["nextcloud_chunk_size"] == "0"
        assert spec.source_config["vendor"] == "nextcloud"

    @pytest.mark.asyncio
    async def test_a_session_folder_keeps_chunked_uploads(self):
        from orchestrator.services.cloud import SessionFolderHandle

        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=SessionFolderHandle(backend="nextcloud", native_id="sessions/ab"),
            mount_kind="session_folder",
            target_path="/cloud/home",
            access="read_write",
        )
        assert "/remote.php/dav/files/" in spec.source_config["url"]
        assert "nextcloud_chunk_size" not in spec.source_config
