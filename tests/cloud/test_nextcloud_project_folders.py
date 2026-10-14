"""``NextcloudBackend``'s project Group Folders on groupfolders 19 to 21.

Group Folders is reached through index.php, which speaks OCS v1: every answer
is HTTP 200, a refusal too, and only ``ocs.meta.statuscode`` says which it is.
Groupfolders 20+ (Nextcloud 32) refuses a second folder at a mount point with
OCS 400 and ``data: []``; SRW read that as a folder with no id and swallowed
it, so a second project with a taken name silently got no folder.

The fake below answers the way groupfolders does (``FolderController`` and
``OCSMiddleware``): a refusal is HTTP 200 with the code in the envelope, an
installation without folders lists ``data: []``, and the server trims the
mount point it stores (whitespace, then a leading slash: ``trimMountpoint``).
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs, quote, unquote

import httpx
import pytest

from orchestrator.services import project_provisioning, projects
from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    GroupId,
    NextcloudBackend,
    ProjectFolderHandle,
)
from orchestrator.services.cloud.config import NextcloudSettings

NEXTCLOUD_BASE = "https://nc.example.com"
AGENT_USER = "agent-service"
PROJECT_ID = "a1b2c3d4-0000-4000-8000-000000000001"
GROUP = GroupId(f"project-{PROJECT_ID}")


def _ocs(code: int, data: Any = None) -> httpx.Response:
    """An OCS v1 answer: HTTP 200 whatever the envelope says."""
    return httpx.Response(
        200,
        json={
            "ocs": {
                "meta": {
                    "status": "ok" if code == 100 else "failure",
                    "statuscode": code,
                },
                "data": [] if data is None else data,
            }
        },
    )


class FakeGroupFolders:
    """Group Folders as groupfolders 20+ answers it (``refuse_duplicates``),
    or as 19 does, which takes any mount point."""

    def __init__(
        self,
        folders: dict[int, str] | None = None,
        *,
        refuse_duplicates: bool = True,
    ) -> None:
        self.folders = dict(folders or {})
        self.groups: dict[int, dict[str, int]] = {fid: {} for fid in self.folders}
        self.refuse_duplicates = refuse_duplicates
        #: Every mount point a create asked for, and those it got.
        self.posted: list[str] = []
        self.created: list[str] = []
        self.deleted: list[int] = []
        #: ``(method, path)`` -> the answer it gets instead.
        self.fail: dict[tuple[str, str], httpx.Response] = {}
        #: Folders another create makes between SRW's listing and its POST.
        self.taken_meanwhile: list[str] = []

    def _next_id(self) -> int:
        return max(self.folders, default=0) + 1

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = unquote(request.url.path)
        method = request.method
        if (method, path) in self.fail:
            return self.fail[(method, path)]
        form = {
            k: v[0]
            for k, v in parse_qs(
                request.content.decode(), keep_blank_values=True
            ).items()
        }
        if path == "/index.php/apps/groupfolders/folders":
            if method == "GET":
                listing = {
                    str(fid): {"id": fid, "mount_point": mp, "groups": {}}
                    for fid, mp in self.folders.items()
                }
                # PHP encodes an empty array as [], not {}.
                return _ocs(100, listing or [])
            if method == "POST":
                for name in self.taken_meanwhile:
                    fid = self._next_id()
                    self.folders[fid] = name
                    self.groups[fid] = {}
                self.taken_meanwhile = []
                self.posted.append(form["mountpoint"])
                mountpoint = form["mountpoint"].strip().lstrip("/")
                if not mountpoint or ".." in mountpoint:
                    return _ocs(400)  # "Mount point cannot contain .."
                if self.refuse_duplicates and mountpoint in self.folders.values():
                    return _ocs(400)  # "Mount point already exists"
                fid = self._next_id()
                self.folders[fid] = mountpoint
                self.groups[fid] = {}
                self.created.append(mountpoint)
                return _ocs(
                    100,
                    {"id": fid, "mount_point": mountpoint, "groups": {}, "acl": False},
                )
        m = re.fullmatch(r"/index\.php/apps/groupfolders/folders/(\d+)", path)
        if m and method == "DELETE":
            fid = int(m.group(1))
            if self.folders.pop(fid, None) is None:
                return _ocs(404)
            self.deleted.append(fid)
            return _ocs(100, {"success": True})
        m = re.fullmatch(r"/index\.php/apps/groupfolders/folders/(\d+)/groups", path)
        if m and method == "POST":
            fid = int(m.group(1))
            if fid not in self.folders:
                return _ocs(404)
            self.groups[fid][form["group"]] = 31
            return _ocs(100, {"success": True})
        m = re.fullmatch(
            r"/index\.php/apps/groupfolders/folders/(\d+)/groups/(.+)", path
        )
        if m and method == "POST":
            fid = int(m.group(1))
            if fid not in self.folders:
                return _ocs(404)
            self.groups[fid][m.group(2)] = int(form["permissions"])
            return _ocs(100, {"success": True})
        raise AssertionError(f"unexpected {method} {path}")


def _backend(fake: FakeGroupFolders) -> NextcloudBackend:
    be = NextcloudBackend(
        NextcloudSettings(
            base_url=NEXTCLOUD_BASE,
            public_url=NEXTCLOUD_BASE,
            admin_user="admin",
            admin_password="admin",
            agent_user=AGENT_USER,
            agent_password="agent-service-dev",
        )
    )
    be._client = httpx.AsyncClient(
        base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(fake.handler)
    )
    be._initialized = True
    return be


async def _ensure(be: NextcloudBackend, name: str = "Marketing") -> ProjectFolderHandle:
    return await be.ensure_project_folder(
        project_name=name, group_id=GROUP, project_id=PROJECT_ID
    )


class TestMountPoint:
    @pytest.mark.asyncio
    async def test_a_free_name_is_kept_as_the_server_stores_it(self):
        fake = FakeGroupFolders(refuse_duplicates=False)

        handle = await _ensure(_backend(fake), "/Marketing ")

        # The server trims; the handle keeps what it stored.
        assert handle.vendor_meta == {"mountpoint": "Marketing"}
        assert fake.created == ["Marketing"]
        fid = int(handle.native_id)
        assert fake.groups[fid] == {str(GROUP): 31, "srw-agents": 31}

    @pytest.mark.asyncio
    async def test_a_taken_name_gets_the_projects_suffix(self):
        fake = FakeGroupFolders({7: "Marketing"})
        be = _backend(fake)

        handle = await _ensure(be)

        assert fake.posted == ["Marketing (a1b2)"]
        assert fake.created == ["Marketing (a1b2)"]
        # Never adopted: the existing folder is someone else's.
        assert handle.native_id != "7"
        assert fake.groups[7] == {}
        assert handle.vendor_meta == {"mountpoint": "Marketing (a1b2)"}
        # Every DAV path is built from the stored mount point.
        suffixed = quote("Marketing (a1b2)", safe="")
        assert be.get_project_folder_webdav_url(handle) == (
            f"{NEXTCLOUD_BASE}/remote.php/dav/groupfolders/{AGENT_USER}/"
            "Marketing (a1b2)/"
        )
        assert be._groupfolder_dav_base(handle) == (
            f"/remote.php/dav/groupfolders/{AGENT_USER}/{suffixed}"
        )
        assert be.folder_grant_webdav_url(handle, "srw-writer-f-x") == (
            f"{NEXTCLOUD_BASE}/remote.php/dav/groupfolders/srw-writer-f-x/{suffixed}/"
        )
        assert be.get_project_folder_browser_url(handle) == (
            f"{NEXTCLOUD_BASE}/apps/files/?dir=/{quote('Marketing (a1b2)')}"
        )

    @pytest.mark.asyncio
    async def test_the_suffix_is_the_same_on_every_retry(self):
        first = FakeGroupFolders({7: "Marketing"})
        again = FakeGroupFolders({3: "Marketing"})

        assert (await _ensure(_backend(first))).vendor_meta == (
            await _ensure(_backend(again))
        ).vendor_meta

    def test_an_identity_that_is_no_uuid_is_hashed(self):
        assert NextcloudBackend._mount_point_suffix(PROJECT_ID) == "a1b2"
        suffix = NextcloudBackend._mount_point_suffix("project-7")
        assert re.fullmatch(r"[0-9a-f]{4}", suffix)
        assert suffix == NextcloudBackend._mount_point_suffix("project-7")

    @pytest.mark.asyncio
    async def test_a_case_variant_counts_as_taken(self):
        fake = FakeGroupFolders({7: "marketing"})

        handle = await _ensure(_backend(fake))

        assert handle.vendor_meta == {"mountpoint": "Marketing (a1b2)"}

    @pytest.mark.asyncio
    async def test_nextcloud_31_never_gets_a_duplicate_either(self):
        """Groupfolders 19 takes any mount point; SRW checks first."""
        fake = FakeGroupFolders({7: "Marketing"}, refuse_duplicates=False)

        handle = await _ensure(_backend(fake))

        assert fake.created == ["Marketing (a1b2)"]
        assert handle.native_id != "7"

    @pytest.mark.asyncio
    async def test_both_names_taken_is_refused_and_nothing_is_adopted(self):
        fake = FakeGroupFolders({7: "Marketing", 8: "Marketing (a1b2)"})

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake))

        assert ei.value.kind is CloudBackendErrorKind.ALREADY_EXISTS
        assert fake.posted == []
        assert fake.groups == {7: {}, 8: {}}

    @pytest.mark.asyncio
    async def test_a_name_taken_meanwhile_moves_on_to_the_suffix(self):
        """The listing was empty (``data: []``); another create took the name
        before SRW's POST, which groupfolders 20+ refuses with OCS 400."""
        fake = FakeGroupFolders()
        fake.taken_meanwhile = ["Marketing"]

        handle = await _ensure(_backend(fake))

        assert fake.posted == ["Marketing", "Marketing (a1b2)"]
        assert fake.created == ["Marketing (a1b2)"]
        assert handle.vendor_meta == {"mountpoint": "Marketing (a1b2)"}


class TestRefusals:
    @pytest.mark.asyncio
    async def test_ocs_400_with_empty_data_raises_instead_of_being_swallowed(self):
        """HTTP 200 + OCS 400 + ``data: []`` for a name that is not taken
        is a refusal, not an AttributeError, and no other name is tried."""
        fake = FakeGroupFolders()

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake), "Q3 .. Q4")

        assert ei.value.kind is CloudBackendErrorKind.INVALID_REQUEST
        assert ei.value.vendor_code == "400"
        assert fake.posted == ["Q3 .. Q4"]

    @pytest.mark.asyncio
    async def test_an_unconfirmed_password_is_permission_denied(self):
        fake = FakeGroupFolders()
        fake.fail[("POST", "/index.php/apps/groupfolders/folders")] = _ocs(403)

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake))

        assert ei.value.kind is CloudBackendErrorKind.PERMISSION_DENIED

    @pytest.mark.asyncio
    async def test_a_refused_grant_raises_and_removes_the_new_folder(self):
        fake = FakeGroupFolders({7: "Other"})
        fake.fail[("POST", "/index.php/apps/groupfolders/folders/8/groups")] = _ocs(403)

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake))

        assert ei.value.kind is CloudBackendErrorKind.PERMISSION_DENIED
        assert fake.deleted == [8]
        # A retry gets the plain name again.
        assert fake.folders == {7: "Other"}

    @pytest.mark.asyncio
    async def test_a_refused_permission_change_raises(self):
        fake = FakeGroupFolders()
        fake.fail[
            ("POST", f"/index.php/apps/groupfolders/folders/1/groups/{GROUP}")
        ] = _ocs(400)

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake))

        assert ei.value.kind is CloudBackendErrorKind.INVALID_REQUEST
        assert fake.deleted == [1]

    @pytest.mark.asyncio
    async def test_a_refused_listing_raises(self):
        fake = FakeGroupFolders()
        fake.fail[("GET", "/index.php/apps/groupfolders/folders")] = _ocs(403)

        with pytest.raises(CloudBackendError) as ei:
            await _ensure(_backend(fake))

        assert ei.value.kind is CloudBackendErrorKind.PERMISSION_DENIED
        assert fake.created == []


class TestDeleteProjectFolder:
    @pytest.mark.asyncio
    async def test_deletes(self):
        fake = FakeGroupFolders({7: "Marketing"})
        handle = ProjectFolderHandle(backend="nextcloud", native_id="7")

        await _backend(fake).delete_project_folder(handle)

        assert fake.deleted == [7]

    @pytest.mark.asyncio
    async def test_ocs_404_is_already_gone(self):
        fake = FakeGroupFolders()
        be = _backend(fake)
        handle = ProjectFolderHandle(backend="nextcloud", native_id="7")

        await be.delete_project_folder(handle)
        with pytest.raises(CloudBackendError) as ei:
            await be.delete_project_folder(handle, if_exists=False)
        assert ei.value.kind is CloudBackendErrorKind.NOT_FOUND

    @pytest.mark.asyncio
    async def test_a_refusal_is_not_logged_as_deleted(self):
        fake = FakeGroupFolders({7: "Marketing"})
        fake.fail[("DELETE", "/index.php/apps/groupfolders/folders/7")] = _ocs(403)

        with pytest.raises(CloudBackendError) as ei:
            await _backend(fake).delete_project_folder(
                ProjectFolderHandle(backend="nextcloud", native_id="7")
            )
        assert ei.value.kind is CloudBackendErrorKind.PERMISSION_DENIED


class TestProjectPageFolderLink:
    """The project page's deep-link comes from the stored mount point; a row
    from before handles carried one (a bare folder id) still uses the name."""

    @staticmethod
    async def _cloud_storage_url(
        monkeypatch: pytest.MonkeyPatch, row: dict[str, Any]
    ) -> str | None:
        def no_repair(_key: str, coro: Any, **_kwargs: Any) -> bool:
            coro.close()
            return False

        monkeypatch.setattr(project_provisioning, "fire_background_repair", no_repair)
        backend = _backend(FakeGroupFolders())
        dependencies = SimpleNamespace(
            store=MagicMock(),
            logger=MagicMock(),
            provisioning=MagicMock(),
            main_cloud_router=SimpleNamespace(
                for_project_optional=lambda _project: backend
            ),
        )
        result = await projects.get_project(
            row["id"], project=dict(row), dependencies=dependencies
        )
        return result["cloud_storage_url"]

    @pytest.mark.asyncio
    async def test_a_suffixed_folder_links_to_its_stored_mount_point(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        handle = ProjectFolderHandle(
            backend="nextcloud",
            native_id="8",
            vendor_meta={"mountpoint": "Marketing (a1b2)"},
        )
        url = await self._cloud_storage_url(
            monkeypatch,
            {
                "id": PROJECT_ID,
                "name": "Marketing",
                "is_default": False,
                "main_cloud_backend": "nextcloud",
                "main_cloud_folder_handle": handle.to_db(),
            },
        )

        assert url == f"{NEXTCLOUD_BASE}/apps/files/?dir=/{quote('Marketing (a1b2)')}"

    @pytest.mark.asyncio
    async def test_an_old_row_without_a_mount_point_uses_the_name(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        url = await self._cloud_storage_url(
            monkeypatch,
            {
                "id": PROJECT_ID,
                "name": "Marketing",
                "is_default": False,
                "main_cloud_backend": "nextcloud",
                "main_cloud_folder_handle": "7",
                "nextcloud_folder_id": 7,
            },
        )

        assert url == f"{NEXTCLOUD_BASE}/apps/files/?dir=/Marketing"
