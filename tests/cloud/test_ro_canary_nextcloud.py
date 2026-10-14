"""Tests for NextcloudBackend's canary fixture real version/trash id discovery
(protected cloud mode, design §11.4 / §11.6 amendment #5).

``seed_canary_fixture`` must discover REAL version and trashbin ids so the RO
probe's CVE side channels (versions-restore / trash-restore) target ids the
server actually knows — turning ``inconclusive`` into a verified ``403``
(rejected) on a correctly-RO reader. A ref that the server exposes no id for
must stay ``None`` (fail-closed — that side channel stays inconclusive).

Model + binding helper copied from ``test_ro_reader_nextcloud.py`` (its
``_backend_with_ocs_fake()`` at :165-174) rather than imported, since these
are module-private test shims.
"""

from __future__ import annotations

import httpx
import pytest

from orchestrator.services.cloud import NextcloudBackend, ProjectFolderHandle
from orchestrator.services.cloud.config import NextcloudSettings

NC_BASE = "https://nc.example.com"
AGENT_USER = "agent-service"
MOUNTPOINT = "Test Project"
FOLDER_ID = "7"


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


def _nc_backend_with_fake(fake):
    backend = NextcloudBackend(_settings())
    backend._client = httpx.AsyncClient(
        base_url=NC_BASE, transport=httpx.MockTransport(fake.handler)
    )
    backend._agent_client = backend._build_agent_client(
        httpx.MockTransport(fake.handler)
    )
    backend._initialized = True
    backend._agent_user = AGENT_USER
    backend._agent_password = "pw"
    return backend, fake


class FakeNcCanary:
    def __init__(self) -> None:
        self.put_path: str | None = None
        self.deleted: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        if method == "PUT" and path.endswith("/.srw-ro-canary/probe.txt"):
            self.put_path = path
            return httpx.Response(201, headers={"OC-FileId": "12345"})
        if method == "PROPFIND" and "/versions/" in path:
            body = (
                '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">'
                "<d:response><d:href>/remote.php/dav/versions/agent-service/"
                "versions/12345/1699999999</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response></d:multistatus>"
            )
            return httpx.Response(207, text=body)
        # Trash canary: PUT then DELETE a throwaway file so a real
        # groupfolder-trashed item exists for the trash-restore side channel.
        if method == "PUT" and path.endswith("/srw-ro-trash-canary.txt"):
            return httpx.Response(201)
        if method == "DELETE" and path.endswith("/srw-ro-trash-canary.txt"):
            self.deleted = path
            return httpx.Response(204)
        if method == "DELETE" and path.endswith("/.srw-ro-canary/probe.txt"):
            self.deleted = path
            return httpx.Response(204)
        if method == "PROPFIND" and "/trashbin/" in path:
            body = (
                '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">'
                "<d:response><d:href>/remote.php/dav/trashbin/agent-service/"
                "trash/srw-ro-trash-canary.txt.d1699999999</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response></d:multistatus>"
            )
            return httpx.Response(207, text=body)
        return httpx.Response(200, text="<d:multistatus xmlns:d='DAV:'/>")


@pytest.mark.asyncio
async def test_seed_canary_discovers_real_version_and_trash_refs():
    backend, fake = _nc_backend_with_fake(FakeNcCanary())  # local shim
    fixture = await backend.seed_canary_fixture(_handle())
    assert fixture.path == ".srw-ro-canary/probe.txt"
    assert fixture.version_ref == "12345/1699999999"
    assert fixture.trash_ref == "srw-ro-trash-canary.txt.d1699999999"


@pytest.mark.asyncio
async def test_seed_canary_leaves_refs_none_when_server_exposes_none():
    class Empty(FakeNcCanary):
        def handler(self, request):
            if request.method == "PUT":
                return httpx.Response(201, headers={"OC-FileId": "12345"})
            if request.method == "PROPFIND":
                return httpx.Response(207, text="<d:multistatus xmlns:d='DAV:'/>")
            return httpx.Response(204)

    backend, _ = _nc_backend_with_fake(Empty())
    fixture = await backend.seed_canary_fixture(_handle())
    assert fixture.version_ref is None  # no version href -> stays inconclusive
    assert fixture.trash_ref is None


# ---------------------------------------------------------------------------
# F1 (review fix) — OC-FileId leading-digit extraction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_seed_canary_takes_leading_digit_run_only_from_mixed_fileid():
    # Real NC OC-FileId headers are `{fileid}{instance-suffix}` and the
    # suffix can itself contain digits (e.g. "137occ7ab92kf"). Collecting
    # EVERY digit char (the pre-fix bug) reads this as "137792" instead of
    # the correct leading run "137". Only the PUT response is overridden;
    # the versions/trashbin PROPFIND bodies are the base fake's (their
    # content doesn't matter here — this test is only about the fileid
    # extraction feeding into the `{fileid}/...` version_ref prefix).
    class MixedFileId(FakeNcCanary):
        def handler(self, request: httpx.Request) -> httpx.Response:
            if request.method == "PUT" and request.url.path.endswith(
                "/.srw-ro-canary/probe.txt"
            ):
                self.put_path = request.url.path
                return httpx.Response(201, headers={"OC-FileId": "137occ7ab92kf"})
            return super().handler(request)

    backend, _ = _nc_backend_with_fake(MixedFileId())
    fixture = await backend.seed_canary_fixture(_handle())
    assert fixture.version_ref is not None
    assert fixture.version_ref.startswith("137/")
    assert not fixture.version_ref.startswith("137792")


# ---------------------------------------------------------------------------
# F2 (review fix) — collection self-href must be filtered, not just the
# first child. A Depth:1 PROPFIND always re-states the queried collection
# itself as its own first <d:response>; the two brief-specified fakes above
# never emit one (their bodies contain only the real child), so they don't
# exercise `_first_child_leaf`'s self-href skip at all. This is a realistic
# body shape: self-href FIRST, then the real item.
# ---------------------------------------------------------------------------


class FakeNcCanarySelfHrefFirst(FakeNcCanary):
    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        if method == "PUT" and path.endswith("/.srw-ro-canary/probe.txt"):
            self.put_path = path
            return httpx.Response(201, headers={"OC-FileId": "12345"})
        if method == "PROPFIND" and "/versions/" in path:
            body = (
                '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">'
                "<d:response><d:href>/remote.php/dav/versions/agent-service/"
                "versions/12345</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response>"
                "<d:response><d:href>/remote.php/dav/versions/agent-service/"
                "versions/12345/1699999999</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response></d:multistatus>"
            )
            return httpx.Response(207, text=body)
        if method == "PUT" and path.endswith("/srw-ro-trash-canary.txt"):
            return httpx.Response(201)
        if method == "DELETE" and path.endswith("/srw-ro-trash-canary.txt"):
            return httpx.Response(204)
        if method == "PROPFIND" and "/trashbin/" in path:
            body = (
                '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">'
                "<d:response><d:href>/remote.php/dav/trashbin/agent-service/"
                "trash/</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response>"
                "<d:response><d:href>/remote.php/dav/trashbin/agent-service/"
                "trash/srw-ro-trash-canary.txt.d1699999999</d:href>"
                "<d:propstat><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
                "</d:response></d:multistatus>"
            )
            return httpx.Response(207, text=body)
        return httpx.Response(200, text="<d:multistatus xmlns:d='DAV:'/>")


@pytest.mark.asyncio
async def test_seed_canary_skips_collection_self_href_before_real_item():
    backend, _ = _nc_backend_with_fake(FakeNcCanarySelfHrefFirst())
    fixture = await backend.seed_canary_fixture(_handle())
    # Versions namespace: self-href's trailing segment equals the fileid
    # ("12345") — must be skipped in favor of the real version id.
    assert fixture.version_ref == "12345/1699999999"
    # Trashbin namespace: the collection self-href (trailing segment "trash")
    # does not match the trash-canary name prefix, so it is skipped in favor
    # of the real seeded item — never returned as a bogus trash_ref="trash".
    assert fixture.trash_ref == "srw-ro-trash-canary.txt.d1699999999"
    assert fixture.trash_ref != "trash"
