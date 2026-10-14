"""Tests for the cloud_sync base class — algorithm only, no transport."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Awaitable, Callable, Optional

import pytest

from agent.services.cloud_sync.base import WorkspaceSyncBase, _should_ignore


class FakeSync(WorkspaceSyncBase):
    """Records calls so tests can assert on the algorithm's choices."""

    def __init__(self, *args, remote_listing: Optional[list[dict]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.uploads: list[tuple[str, str]] = []
        self.mkdirs: list[str] = []
        self.downloads: list[tuple[str, str]] = []
        self.deletes: list[str] = []
        self.ready_calls = 0
        self.list_calls: list[str] = []
        self._listing = remote_listing or []

    async def _ensure_ready(self) -> None:
        self.ready_calls += 1

    async def _ensure_remote_dir(
        self,
        rel_dir: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        self.mkdirs.append(rel_dir)

    async def _upload_file(
        self,
        rel_path: str,
        local_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> Optional[str]:
        if before_write is not None:
            await before_write()
        self.uploads.append((rel_path, local_path))

    async def _delete_remote_file(
        self,
        rel_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        self.deletes.append(rel_path)

    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        self.list_calls.append(rel_dir)
        return list(self._listing)

    async def _download_file(self, rel_path: str, local_path: str) -> None:
        self.downloads.append((rel_path, local_path))
        Path(local_path).write_text("remote-content")


@pytest.mark.parametrize(
    "path,expected",
    [
        (".git/HEAD", True),
        ("src/.git/foo", True),
        ("repos/x", True),
        ("todos.yaml", True),
        ("some/todos.yaml", True),
        ("archive/phase_1.yaml", True),
        ("workspace.md", True),
        ("plan.md", True),
        ("output/result.txt", False),
        ("notes.md", False),
        ("deep/nested/file.py", False),
    ],
)
def test_should_ignore(path, expected):
    assert _should_ignore(path) is expected


@pytest.mark.asyncio
async def test_push_local_dedup_by_mtime(tmp_path: Path):
    (tmp_path / "a.txt").write_text("hi")
    (tmp_path / "b.txt").write_text("ho")
    sync = FakeSync(tmp_path)

    first = await sync.push()
    assert set(first) == {"a.txt", "b.txt"}
    assert {u[0] for u in sync.uploads} == {"a.txt", "b.txt"}

    # Second push without changes — nothing uploaded
    sync.uploads.clear()
    second = await sync.push()
    assert second == []
    assert sync.uploads == []


@pytest.mark.asyncio
async def test_push_skips_ignored_files(tmp_path: Path):
    (tmp_path / "keep.txt").write_text("keep")
    (tmp_path / "todos.yaml").write_text("ignored")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("ignored")

    sync = FakeSync(tmp_path)
    pushed = await sync.push()
    assert pushed == ["keep.txt"]


@pytest.mark.asyncio
async def test_pull_etag_dedup(tmp_path: Path):
    listing = [
        {"path": "a.txt", "etag": "v1", "isdir": False},
        {"path": "dir/", "etag": "", "isdir": True},
        {"path": "todos.yaml", "etag": "v1", "isdir": False},
    ]
    sync = FakeSync(tmp_path, remote_listing=listing)

    first = await sync.pull()
    assert first == ["a.txt"]  # dir skipped, todos.yaml skipped

    # Same etag → no re-download
    sync.downloads.clear()
    second = await sync.pull()
    assert second == []


@pytest.mark.asyncio
async def test_pull_refetch_on_etag_change(tmp_path: Path):
    sync = FakeSync(
        tmp_path,
        remote_listing=[
            {"path": "a.txt", "etag": "v1", "isdir": False},
        ],
    )
    await sync.pull()
    sync._listing = [{"path": "a.txt", "etag": "v2", "isdir": False}]
    sync.downloads.clear()
    pulled = await sync.pull()
    assert pulled == ["a.txt"]


@pytest.mark.asyncio
async def test_full_sync_returns_tuple(tmp_path: Path):
    (tmp_path / "a.txt").write_text("hi")
    sync = FakeSync(
        tmp_path,
        remote_listing=[{"path": "b.txt", "etag": "v1", "isdir": False}],
    )
    pushed, pulled = await sync.full_sync()
    assert pushed == ["a.txt"]
    assert pulled == ["b.txt"]


@pytest.mark.asyncio
async def test_ensure_remote_dirs_caches(tmp_path: Path):
    sync = FakeSync(tmp_path)
    await sync._ensure_remote_dirs("a/b/c")
    # Should have called mkdir for each segment
    assert sync.mkdirs == ["a", "a/b", "a/b/c"]
    sync.mkdirs.clear()
    # Second call, same path — cache kicks in
    await sync._ensure_remote_dirs("a/b/c")
    assert sync.mkdirs == []


@pytest.mark.asyncio
async def test_ensure_remote_dirs_never_caches_a_failed_mkdir(tmp_path: Path):
    sync = FakeSync(tmp_path)
    unavailable = True
    create = sync._ensure_remote_dir

    async def mkdir(rel_dir, *, before_write=None):
        if unavailable:  # "a" answers 503, so "a/b" then answers 409
            raise RuntimeError("MKCOL failed")
        await create(rel_dir, before_write=before_write)

    sync._ensure_remote_dir = mkdir
    await sync._ensure_remote_dirs("a/b")  # swallowed, as before
    assert sync._remote_dirs == set()
    unavailable = False
    await sync._ensure_remote_dirs("a/b")  # the retry asks again
    assert sync.mkdirs == ["a", "a/b"]
    assert sync._remote_dirs == {"a", "a/b"}


@pytest.mark.asyncio
async def test_poll_cancels_cleanly(tmp_path: Path):
    sync = FakeSync(tmp_path)
    sync._poll_interval = 1  # any positive value
    await sync.start_background_poll()
    # Give the task a chance to schedule but not complete a full sleep
    await asyncio.sleep(0.01)
    await sync.stop()
    assert sync._poll_task is None
    assert sync._running is False


# ---------------------------------------------------------------------------
# Virtual directories: cloud sync is not a tool-layer consumer and must never
# see (or write back into) a virtual prefix.
# knowledge-base/knowledge/features/virtual_directories.md
# ---------------------------------------------------------------------------


class _FakeContactsProvider:
    """Stand-in for ContactsProvider — a directory prefix carrying PII."""

    prefix = "contacts"
    is_dir = True
    writable = False

    def entries(self):
        from agent.core.backends.overlay import EntryMeta

        return {
            "README.md": EntryMeta(size=10),
            "anna-weber.md": EntryMeta(size=64),
        }

    def read(self, name):
        return "Anna Weber\nanna.weber@example.com\n+49 30 1234567\n"


def _overlay_backend(tmp_path: Path):
    from agent.core.backends.overlay import VirtualOverlayBackend
    from tests._fs_backend import FilesystemTestBackend

    overlay = VirtualOverlayBackend(FilesystemTestBackend(tmp_path))
    overlay.register(_FakeContactsProvider())
    return overlay


def test_virtual_provider_files_never_enter_the_sync_walk(tmp_path: Path):
    """Regression: contacts PII must not reach the user's cloud folder.

    The session handler hands ``workspace_manager.backend`` — the overlay — to
    the sync coordinator. Walking it merges virtual prefixes into the root
    listing, and SYNC_IGNORE_PATTERNS covers ``tools/`` but not ``contacts/``,
    so contact names, emails and phone numbers were uploaded. Adding names to
    the ignore list would leave the next provider to rediscover this, so the
    fix is structural: sync walks the REAL backend.
    """
    (tmp_path / "notes.md").write_text("real file")
    sync = FakeSync(tmp_path, workspace_backend=_overlay_backend(tmp_path))

    walked = sync._walk_backend_files()

    assert walked == ["notes.md"]
    assert not [p for p in walked if p.startswith("contacts")]


@pytest.mark.asyncio
async def test_pull_write_back_does_not_hit_the_virtual_prefix(tmp_path: Path):
    """A write-back into a virtual prefix raised VirtualPathError.

    The coordinator runs ``strict=True``, so that exception propagated out of
    the session's initial ``pull_all()`` and the handler set
    ``workspace_sync = None`` — cloud sync silently off for the session's whole
    life. Writing through the real backend cannot raise it.
    """
    sync = FakeSync(tmp_path, workspace_backend=_overlay_backend(tmp_path))

    await sync._pull_file_to_backend("contacts/anna-weber.md", "etag-1")

    assert (tmp_path / "contacts" / "anna-weber.md").read_text() == "remote-content"


# ---------------------------------------------------------------------------
# Fresh-pod economics: recursive remote tree, dedup seeding, reconcile.
# All dedup state is process memory, so a recycled agent pod used to
# re-upload (and re-download) the whole mount.
# knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
# ---------------------------------------------------------------------------


class DepthFakeSync(FakeSync):
    """Depth-1 primitive like real WebDAV: one directory level per call,
    keyed by ``rel_dir``, echoing the listed collection like real servers."""

    def __init__(self, *args, tree: dict[str, list[dict]], **kwargs):
        super().__init__(*args, **kwargs)
        self._tree = tree

    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        self.list_calls.append(rel_dir)
        echo = [{"path": rel_dir, "etag": "", "isdir": True}] if rel_dir else []
        return echo + list(self._tree.get(rel_dir, []))


@pytest.mark.asyncio
async def test_pull_walks_subdirectories(tmp_path: Path):
    """Cloud-side edits under output/ etc. must reach the agent. The old
    single Depth-1 listing meant pull only ever saw root-level files."""
    sync = DepthFakeSync(
        tmp_path,
        tree={
            "": [
                {"path": "a.txt", "etag": "v1", "isdir": False},
                {"path": "output/", "etag": "", "isdir": True},
            ],
            "output": [
                {"path": "output/nested.md", "etag": "v2", "isdir": False},
            ],
        },
    )
    pulled = await sync.pull()
    assert set(pulled) == {"a.txt", "output/nested.md"}
    assert sync.list_calls == ["", "output"]


@pytest.mark.asyncio
async def test_pull_prunes_ignored_subtrees(tmp_path: Path):
    """repos/, .git/ etc. are never descended into — no wasted PROPFINDs,
    no pulling content the push side refuses to touch."""
    sync = DepthFakeSync(
        tmp_path,
        tree={
            "": [
                {"path": "repos/", "etag": "", "isdir": True},
                {"path": "keep.txt", "etag": "v1", "isdir": False},
            ],
            "repos": [
                {"path": "repos/x.py", "etag": "v9", "isdir": False},
            ],
        },
    )
    pulled = await sync.pull()
    assert pulled == ["keep.txt"]
    assert "repos" not in sync.list_calls


@pytest.mark.asyncio
async def test_pull_reconciles_matching_size_without_download(tmp_path: Path):
    """Fresh pod, workspace already equals the cloud: pull must record the
    etags and sizes WITHOUT re-downloading the mount (or clobbering it)."""
    (tmp_path / "a.txt").write_text("hi")
    sync = FakeSync(
        tmp_path,
        remote_listing=[{"path": "a.txt", "etag": "v1", "isdir": False, "size": 2}],
    )
    pulled = await sync.pull()
    assert pulled == []
    assert sync.downloads == []
    assert sync._remote_state == {"a.txt": "v1"}
    assert sync._pushed_sizes == {"a.txt": 2}

    # The reconcile seeded push dedup too: nothing to upload.
    pushed = await sync.push()
    assert pushed == []

    # A REAL remote change (new etag) still pulls.
    sync._listing = [{"path": "a.txt", "etag": "v2", "isdir": False, "size": 2}]
    pulled = await sync.pull()
    assert pulled == ["a.txt"]


@pytest.mark.asyncio
async def test_pull_downloads_when_local_size_differs(tmp_path: Path):
    (tmp_path / "a.txt").write_text("hi")  # 2 bytes locally
    sync = FakeSync(
        tmp_path,
        remote_listing=[{"path": "a.txt", "etag": "v1", "isdir": False, "size": 7}],
    )
    pulled = await sync.pull()
    assert pulled == ["a.txt"]


@pytest.mark.asyncio
async def test_pull_downloads_when_local_file_missing(tmp_path: Path):
    """A zero-byte remote file with no local copy must still materialize —
    stat() reporting 0 for missing paths must not read as 'in sync'."""
    sync = FakeSync(
        tmp_path,
        remote_listing=[{"path": "empty.txt", "etag": "v1", "isdir": False, "size": 0}],
    )
    pulled = await sync.pull()
    assert pulled == ["empty.txt"]


@pytest.mark.asyncio
async def test_push_seeds_dedup_from_remote_sizes(tmp_path: Path):
    """Push before any successful pull (degraded attach): the one-shot seed
    primes size dedup from the remote listing, so a fresh pod skips every
    file the cloud already holds at the same size."""
    (tmp_path / "same.txt").write_text("hi")  # matches remote size 2
    (tmp_path / "changed.txt").write_text("hello")  # remote has 2, local 5
    sync = FakeSync(
        tmp_path,
        remote_listing=[
            {"path": "same.txt", "etag": "v1", "isdir": False, "size": 2},
            {"path": "changed.txt", "etag": "v2", "isdir": False, "size": 2},
        ],
    )
    pushed = await sync.push()
    assert pushed == ["changed.txt"]

    # One-shot: the listing is not re-fetched on later pushes.
    calls_after_first = len(sync.list_calls)
    await sync.push()
    assert len(sync.list_calls) == calls_after_first


@pytest.mark.asyncio
async def test_push_seeding_failure_falls_back_to_full_push(tmp_path: Path):
    """Listing failure (folder not provisioned yet) must not kill the push —
    it just runs unseeded, which is the old behavior."""
    (tmp_path / "a.txt").write_text("hi")

    class ListingFails(FakeSync):
        async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
            raise RuntimeError("PROPFIND 404")

    sync = ListingFails(tmp_path)
    pushed = await sync.push()
    assert pushed == ["a.txt"]


@pytest.mark.asyncio
async def test_push_backend_seeded_skip_uses_stat_not_read(tmp_path: Path):
    """Backend mode: a size-tracked, unchanged file costs one stat() and no
    read_file() — the per-turn walk must not transfer unchanged content."""
    from tests._fs_backend import FilesystemTestBackend

    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "big.bin").write_bytes(b"x" * 1024)
    backend = FilesystemTestBackend(ws)

    reads: list[str] = []
    orig_read = backend.read_file

    def counting_read(path, binary=False):
        reads.append(path)
        return orig_read(path, binary)

    backend.read_file = counting_read  # type: ignore[method-assign]

    sync = FakeSync(
        tmp_path,
        workspace_backend=backend,
        remote_listing=[
            {"path": "big.bin", "etag": "v1", "isdir": False, "size": 1024}
        ],
    )
    pushed = await sync.push()
    assert pushed == []
    assert reads == []

    # Size moved → read + upload again.
    (ws / "big.bin").write_bytes(b"x" * 2048)
    pushed = await sync.push()
    assert pushed == ["big.bin"]
    assert reads == ["big.bin"]
