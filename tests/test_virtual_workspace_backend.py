"""Contract tests for VirtualWorkspaceBackend.

VirtualWorkspaceBackend (the ``virtual`` no-workspace tier) implements the
WorkspaceBackend file contract as explicit object-store ops under a key prefix
(knowledge-base/knowledge/features/no_workspace_agent_mode.md §5). These tests run the full
contract over InMemoryObjectStore — the same contract the filesystem test
backend satisfies — plus the S3-isms the backend has to approximate
(empty-dir markers, read-modify-write append, read size guard, bounded
content search) and the prefix-isolation / boundary-escape guarantees.
"""

import pytest

from shared.runtime.core.backends.object_store import InMemoryObjectStore  # noqa: E402
from agent.core.backends.virtual import VirtualWorkspaceBackend  # noqa: E402
from shared.runtime.core.workspace_backend import WorkspaceBackend  # noqa: E402

PREFIX = "jobs/job-1/"


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


@pytest.fixture
def backend(store) -> VirtualWorkspaceBackend:
    return VirtualWorkspaceBackend(store, prefix=PREFIX)


# =============================================================================
# Init / properties
# =============================================================================


class TestInitAndProperties:
    def test_is_a_workspace_backend(self, backend):
        assert isinstance(backend, WorkspaceBackend)

    def test_instantiable_means_all_abstract_methods_implemented(self, store):
        # If any abstract method were missing, this would raise TypeError.
        VirtualWorkspaceBackend(store)

    def test_prefix_normalized_trailing_slash(self, store):
        b = VirtualWorkspaceBackend(store, prefix="jobs/x")
        assert b.root == "jobs/x/"

    def test_prefix_strips_leading_slash(self, store):
        b = VirtualWorkspaceBackend(store, prefix="/jobs/x/")
        assert b.root == "jobs/x/"

    def test_empty_prefix(self, store):
        b = VirtualWorkspaceBackend(store, prefix="")
        assert b.root == ""

    def test_supports_shell_is_false(self, backend):
        assert backend.supports_shell is False

    def test_host_is_none(self, backend):
        assert backend.host is None


# =============================================================================
# Path resolution / boundary escapes
# =============================================================================


class TestPathResolution:
    def test_resolve_path_prepends_prefix(self, backend):
        assert (
            backend.resolve_path("knowledge-base/knowledge/a.md")
            == PREFIX + "knowledge-base/knowledge/a.md"
        )

    def test_resolve_root(self, backend):
        assert backend.resolve_path("") == PREFIX

    def test_resolve_normalizes_dots(self, backend):
        assert backend.resolve_path("a/./b/../c") == PREFIX + "a/c"

    def test_resolve_rejects_parent_traversal(self, backend):
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend.resolve_path("../../etc/passwd")

    def test_resolve_rejects_absolute(self, backend):
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend.resolve_path("/etc/passwd")

    def test_read_rejects_traversal(self, backend):
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend.read_file("../secret")

    def test_write_rejects_traversal(self, backend):
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend.write_file("../escape", "x")


# =============================================================================
# read / write / append
# =============================================================================


class TestReadWrite:
    def test_write_then_read_text(self, backend):
        backend.write_file("notes.md", "hello")
        assert backend.read_file("notes.md") == "hello"

    def test_write_then_read_binary(self, backend):
        backend.write_file("img.bin", b"\x00\x01\x02")
        assert backend.read_file("img.bin", binary=True) == b"\x00\x01\x02"

    def test_write_text_stored_as_utf8(self, backend):
        backend.write_file("u.txt", "café")
        assert backend.read_file("u.txt", binary=True) == "café".encode("utf-8")

    def test_write_overwrites(self, backend):
        backend.write_file("f", "one")
        backend.write_file("f", "two")
        assert backend.read_file("f") == "two"

    def test_write_creates_nested_path_without_mkdir(self, backend):
        backend.write_file("a/b/c/deep.txt", "x")
        assert backend.read_file("a/b/c/deep.txt") == "x"
        assert backend.is_dir("a/b/c") is True

    def test_read_missing_raises_file_not_found(self, backend):
        with pytest.raises(FileNotFoundError):
            backend.read_file("ghost.txt")

    def test_read_directory_raises_value_error(self, backend):
        backend.write_file("dir/inner.txt", "x")
        with pytest.raises(ValueError, match="Not a file"):
            backend.read_file("dir")

    def test_write_to_root_rejected(self, backend):
        with pytest.raises(ValueError, match="root"):
            backend.write_file("", "x")

    def test_write_over_directory_rejected(self, backend):
        backend.write_file("d/inner.txt", "x")
        with pytest.raises(ValueError, match="Is a directory"):
            backend.write_file("d", "clobber")

    def test_read_size_guard(self, store):
        b = VirtualWorkspaceBackend(store, prefix=PREFIX, max_read_bytes=4)
        b.write_file("big.txt", "12345")  # 5 bytes > 4
        with pytest.raises(ValueError, match="exceeds the read limit"):
            b.read_file("big.txt")


class TestAppend:
    def test_append_creates_file(self, backend):
        backend.append_file("log.txt", "line1\n")
        assert backend.read_file("log.txt") == "line1\n"

    def test_append_extends_file(self, backend):
        backend.write_file("log.txt", "a\n")
        backend.append_file("log.txt", "b\n")
        assert backend.read_file("log.txt") == "a\nb\n"


# =============================================================================
# exists / is_file / is_dir
# =============================================================================


class TestExistence:
    def test_root_is_a_directory(self, backend):
        assert backend.is_dir("") is True
        assert backend.is_file("") is False

    def test_file_exists(self, backend):
        backend.write_file("f.txt", "x")
        assert backend.exists("f.txt") is True
        assert backend.is_file("f.txt") is True
        assert backend.is_dir("f.txt") is False

    def test_directory_exists_when_it_has_children(self, backend):
        backend.write_file("d/f.txt", "x")
        assert backend.is_dir("d") is True
        assert backend.is_file("d") is False
        assert backend.exists("d") is True

    def test_nonexistent(self, backend):
        assert backend.exists("nope") is False
        assert backend.is_file("nope") is False
        assert backend.is_dir("nope") is False


# =============================================================================
# mkdir + empty-dir (.keep) handling
# =============================================================================


class TestMkdir:
    def test_mkdir_makes_empty_dir_visible(self, backend):
        backend.mkdir("empty")
        assert backend.is_dir("empty") is True

    def test_mkdir_root_is_noop(self, backend):
        backend.mkdir("")  # must not raise
        assert backend.is_dir("") is True

    def test_mkdir_nested_creates_intermediate_dirs(self, backend):
        backend.mkdir("a/b/c")
        assert backend.is_dir("a") is True
        assert backend.is_dir("a/b") is True
        assert backend.is_dir("a/b/c") is True

    def test_keep_marker_hidden_from_listing(self, backend):
        backend.mkdir("empty")
        assert backend.list_dir("empty") == []  # .keep is filtered out

    def test_empty_dir_shows_in_parent_listing(self, backend):
        backend.mkdir("sub")
        assert "sub/" in backend.list_dir("")


# =============================================================================
# list_dir
# =============================================================================


class TestListDir:
    def test_lists_immediate_children_only(self, backend):
        backend.write_file("a.txt", "x")
        backend.write_file("sub/b.txt", "y")
        backend.write_file("sub/deep/c.txt", "z")
        result = backend.list_dir("")
        assert "a.txt" in result
        assert "sub/" in result
        assert "sub/b.txt" not in result  # not immediate

    def test_directories_have_trailing_slash(self, backend):
        backend.write_file("d/inner.txt", "x")
        assert "d/" in backend.list_dir("")

    def test_relative_paths_from_root(self, backend):
        backend.write_file("sub/b.txt", "y")
        assert backend.list_dir("sub") == ["sub/b.txt"]

    def test_pattern_filter(self, backend):
        backend.write_file("a.py", "x")
        backend.write_file("b.py", "y")
        backend.write_file("c.txt", "z")
        result = backend.list_dir("", pattern="*.py")
        assert "a.py" in result and "b.py" in result
        assert "c.txt" not in result

    def test_sorted(self, backend):
        backend.write_file("z.txt", "")
        backend.write_file("a.txt", "")
        backend.write_file("m.txt", "")
        assert backend.list_dir("") == sorted(backend.list_dir(""))

    def test_file_path_returns_itself(self, backend):
        backend.write_file("solo.txt", "x")
        assert backend.list_dir("solo.txt") == ["solo.txt"]

    def test_nonexistent_returns_empty(self, backend):
        assert backend.list_dir("ghost") == []


# =============================================================================
# search_files
# =============================================================================


class TestSearch:
    def test_finds_content_match(self, backend):
        backend.write_file("a.txt", "the quick brown fox\njumps over")
        results = backend.search_files("brown")
        assert len(results) == 1
        assert results[0]["path"] == "a.txt"
        assert results[0]["line_number"] == 1
        assert "brown" in results[0]["line"]

    def test_case_insensitive_default(self, backend):
        backend.write_file("a.txt", "HELLO World")
        assert len(backend.search_files("hello")) == 1

    def test_case_sensitive(self, backend):
        backend.write_file("a.txt", "HELLO World")
        assert backend.search_files("hello", case_sensitive=True) == []
        assert len(backend.search_files("HELLO", case_sensitive=True)) == 1

    def test_skips_keep_marker(self, backend):
        backend.mkdir("d")  # writes d/.keep (empty); must not match
        assert backend.search_files("") == [] or all(
            r["path"] != "d/.keep" for r in backend.search_files("")
        )

    def test_skips_binary_extensions(self, backend):
        backend.write_file("image.png", b"needle inside binary")
        assert backend.search_files("needle") == []

    def test_respects_file_size_cap(self, store):
        b = VirtualWorkspaceBackend(store, prefix=PREFIX, search_file_bytes=4)
        b.write_file("big.txt", "needle and more text")  # > 4 bytes
        assert b.search_files("needle") == []

    def test_scoped_to_path(self, backend):
        backend.write_file("keep/a.txt", "needle here")
        backend.write_file("other/b.txt", "needle here too")
        results = backend.search_files("needle", path="keep")
        assert len(results) == 1
        assert results[0]["path"] == "keep/a.txt"


# =============================================================================
# delete_file / delete_directory
# =============================================================================


class TestDelete:
    def test_delete_existing_file(self, backend):
        backend.write_file("f.txt", "x")
        assert backend.delete_file("f.txt") is True
        assert backend.exists("f.txt") is False

    def test_delete_missing_file_returns_false(self, backend):
        assert backend.delete_file("ghost.txt") is False

    def test_delete_empty_directory_via_delete_file(self, backend):
        backend.mkdir("empty")
        assert backend.delete_file("empty") is True
        assert backend.is_dir("empty") is False

    def test_delete_nonempty_directory_via_delete_file_raises(self, backend):
        backend.write_file("d/inner.txt", "x")
        with pytest.raises(ValueError, match="non-empty"):
            backend.delete_file("d")

    def test_delete_directory_removes_subtree(self, backend):
        backend.write_file("tree/a.txt", "x")
        backend.write_file("tree/sub/b.txt", "y")
        assert backend.delete_directory("tree") is True
        assert backend.is_dir("tree") is False
        assert backend.exists("tree/a.txt") is False
        assert backend.exists("tree/sub/b.txt") is False

    def test_delete_root_raises(self, backend):
        with pytest.raises(ValueError, match="workspace root"):
            backend.delete_directory("")

    def test_delete_directory_on_file_raises(self, backend):
        backend.write_file("f.txt", "x")
        with pytest.raises(ValueError, match="Not a directory"):
            backend.delete_directory("f.txt")

    def test_delete_nonexistent_directory_returns_false(self, backend):
        assert backend.delete_directory("ghost") is False


# =============================================================================
# move / copy
# =============================================================================


class TestMoveCopy:
    def test_move_file(self, backend):
        backend.write_file("old.txt", "data")
        backend.move("old.txt", "new.txt")
        assert backend.exists("old.txt") is False
        assert backend.read_file("new.txt") == "data"

    def test_move_directory_subtree(self, backend):
        backend.write_file("src/a.txt", "1")
        backend.write_file("src/sub/b.txt", "2")
        backend.move("src", "dst")
        assert backend.exists("src/a.txt") is False
        assert backend.read_file("dst/a.txt") == "1"
        assert backend.read_file("dst/sub/b.txt") == "2"

    def test_move_missing_raises(self, backend):
        with pytest.raises(FileNotFoundError):
            backend.move("ghost", "dst")

    def test_copy_file(self, backend):
        backend.write_file("src.txt", "data")
        backend.copy("src.txt", "dst.txt")
        assert backend.read_file("src.txt") == "data"  # source preserved
        assert backend.read_file("dst.txt") == "data"

    def test_copy_directory_raises(self, backend):
        backend.write_file("d/inner.txt", "x")
        with pytest.raises(ValueError, match="Cannot copy directory"):
            backend.copy("d", "d2")

    def test_copy_missing_raises(self, backend):
        with pytest.raises(FileNotFoundError):
            backend.copy("ghost", "dst")


# =============================================================================
# stat
# =============================================================================


class TestStat:
    def test_stat_file_returns_size(self, backend):
        backend.write_file("f.txt", "12345")
        assert backend.stat("f.txt") == 5

    def test_stat_directory_sums_children(self, backend):
        backend.write_file("d/a.txt", "123")  # 3
        backend.write_file("d/b.txt", "4567")  # 4
        assert backend.stat("d") == 7

    def test_stat_nonexistent_is_zero(self, backend):
        assert backend.stat("ghost") == 0


# =============================================================================
# Lifecycle
# =============================================================================


class TestLifecycle:
    def test_not_connected_initially(self, backend):
        assert backend.is_connected() is False

    def test_connect_then_connected(self, backend):
        backend.connect()
        assert backend.is_connected() is True

    def test_disconnect(self, backend):
        backend.connect()
        backend.disconnect()
        assert backend.is_connected() is False


# =============================================================================
# Capability boundaries (no shell, no home)
# =============================================================================


class TestNoHomeNoShell:
    def test_write_home_file_unsupported(self, backend):
        with pytest.raises(NotImplementedError):
            backend.write_home_file(".ssh/key", "secret")

    def test_resolve_home_path_unsupported(self, backend):
        with pytest.raises(NotImplementedError):
            backend.resolve_home_path(".ssh/key")

    def test_shell_run_unsupported(self, backend):
        with pytest.raises(NotImplementedError):
            backend.shell_run("ls")


# =============================================================================
# Prefix isolation (durability / multi-tenant separation in one store)
# =============================================================================


class TestPrefixIsolation:
    def test_files_land_under_prefix(self, backend, store):
        backend.write_file("a.txt", "x")
        assert store.head(PREFIX + "a.txt") == 1

    def test_distinct_prefixes_isolated(self, store):
        job1 = VirtualWorkspaceBackend(store, prefix="jobs/1/")
        job2 = VirtualWorkspaceBackend(store, prefix="jobs/2/")
        job1.write_file("secret.txt", "job1 only")
        assert job2.exists("secret.txt") is False
        assert job2.list_dir("") == []
        assert job1.read_file("secret.txt") == "job1 only"


# =============================================================================
# Scoped metadata index (begin_read_cache / end_read_cache)
#
# Every store op on this backend is a process spawn in production, so session
# setup opens a scoped index to answer its dozens of existence probes from one
# listing. The index must be INVISIBLE: identical answers to the uncached
# backend, kept exact across local mutations, and gone the moment the scope
# closes.
# =============================================================================


class _CountingStore(InMemoryObjectStore):
    """InMemoryObjectStore that counts the metadata ops the index removes."""

    def __init__(self):
        super().__init__()
        self.list_calls = 0
        self.head_calls = 0

    def list(self, prefix):
        self.list_calls += 1
        return super().list(prefix)

    def head(self, key):
        self.head_calls += 1
        return super().head(key)


@pytest.fixture
def counting_store() -> _CountingStore:
    return _CountingStore()


@pytest.fixture
def counting_backend(counting_store) -> VirtualWorkspaceBackend:
    return VirtualWorkspaceBackend(counting_store, prefix=PREFIX)


class TestScopedReadCache:
    def test_inactive_by_default(self, backend):
        assert backend._index is None

    def test_probes_hit_store_once_inside_scope(self, counting_backend, counting_store):
        counting_backend.write_file("a.txt", "x")
        counting_backend.write_file("dir/b.txt", "y")
        counting_store.list_calls = 0
        counting_store.head_calls = 0

        counting_backend.begin_read_cache()
        for _ in range(5):
            assert counting_backend.is_file("a.txt") is True
            assert counting_backend.is_file("missing.txt") is False
            assert counting_backend.is_dir("dir") is True
            assert counting_backend.exists("dir/b.txt") is True
        counting_backend.end_read_cache()

        # One priming listing for 30 probes, and no per-probe head at all.
        assert counting_store.list_calls == 1
        assert counting_store.head_calls == 0

    def test_answers_match_uncached_backend(self, backend):
        backend.write_file("a.txt", "hello")
        backend.write_file("dir/b.txt", "yy")
        backend.mkdir("empty")
        probes = ["a.txt", "dir", "dir/b.txt", "empty", "missing", "dir/missing"]

        uncached = {
            p: (backend.exists(p), backend.is_file(p), backend.is_dir(p))
            for p in probes
        }
        listings = {p: backend.list_dir(p) for p in ["", "dir", "empty"]}
        walked = backend.walk("")
        stats = {p: backend.stat(p) for p in ["a.txt", "dir"]}

        backend.begin_read_cache()
        try:
            assert {
                p: (backend.exists(p), backend.is_file(p), backend.is_dir(p))
                for p in probes
            } == uncached
            assert {p: backend.list_dir(p) for p in ["", "dir", "empty"]} == listings
            assert backend.walk("") == walked
            assert {p: backend.stat(p) for p in ["a.txt", "dir"]} == stats
        finally:
            backend.end_read_cache()

    def test_local_writes_visible_inside_scope(self, backend):
        backend.begin_read_cache()
        try:
            assert backend.exists("new.txt") is False
            backend.write_file("new.txt", "content")
            assert backend.is_file("new.txt") is True
            assert backend.stat("new.txt") == 7
            assert "new.txt" in backend.list_dir("")
            backend.append_file("new.txt", "!!")
            assert backend.stat("new.txt") == 9
        finally:
            backend.end_read_cache()
        assert backend.read_file("new.txt") == "content!!"

    def test_local_deletes_visible_inside_scope(self, backend):
        backend.write_file("gone.txt", "x")
        backend.write_file("tree/deep/f.txt", "y")
        backend.begin_read_cache()
        try:
            assert backend.delete_file("gone.txt") is True
            assert backend.exists("gone.txt") is False
            assert backend.delete_directory("tree") is True
            assert backend.is_dir("tree") is False
        finally:
            backend.end_read_cache()
        assert backend.exists("gone.txt") is False
        assert backend.is_dir("tree") is False

    def test_mkdir_and_move_and_copy_tracked(self, backend):
        backend.write_file("src.txt", "abc")
        backend.begin_read_cache()
        try:
            backend.mkdir("fresh")
            assert backend.is_dir("fresh") is True
            backend.copy("src.txt", "copy.txt")
            assert backend.is_file("copy.txt") is True
            assert backend.stat("copy.txt") == 3
            backend.move("src.txt", "moved.txt")
            assert backend.is_file("src.txt") is False
            assert backend.is_file("moved.txt") is True
        finally:
            backend.end_read_cache()
        assert backend.is_dir("fresh") is True
        assert backend.read_file("moved.txt") == "abc"
        assert backend.exists("src.txt") is False

    def test_external_write_seen_after_scope_closes(self, backend, store):
        """The documented boundary: another process writing to the same prefix
        mid-scope is picked up by the first op after the scope closes."""
        backend.begin_read_cache()
        try:
            assert backend.exists("uploaded.pdf") is False
            store.put(PREFIX + "uploaded.pdf", b"externally uploaded")
            assert backend.exists("uploaded.pdf") is False  # stale, by design
        finally:
            backend.end_read_cache()
        assert backend.exists("uploaded.pdf") is True

    def test_begin_is_idempotent(self, counting_backend, counting_store):
        counting_backend.write_file("a.txt", "x")
        counting_store.list_calls = 0
        counting_backend.begin_read_cache()
        counting_backend.begin_read_cache()
        counting_backend.begin_read_cache()
        try:
            assert counting_store.list_calls == 1
        finally:
            counting_backend.end_read_cache()

    def test_end_without_begin_is_safe(self, backend):
        backend.end_read_cache()
        assert backend._index is None

    def test_prefix_isolation_holds_inside_scope(self, store):
        job1 = VirtualWorkspaceBackend(store, prefix="jobs/1/")
        job2 = VirtualWorkspaceBackend(store, prefix="jobs/2/")
        job1.write_file("secret.txt", "job1 only")
        job2.begin_read_cache()
        try:
            assert job2.exists("secret.txt") is False
            assert job2.list_dir("") == []
        finally:
            job2.end_read_cache()

    def test_mkdir_skips_rewrite_for_existing_dir_in_scope(
        self, counting_backend, counting_store
    ):
        """Scaffolding re-creates the same directories on every attach; inside
        the scope that must cost zero store writes."""
        counting_backend.mkdir("output")
        counting_backend.write_file("notes/a.txt", "x")
        counting_backend.begin_read_cache()
        try:
            before = len(counting_store._data)
            counting_backend.mkdir("output")  # marker already there
            counting_backend.mkdir("notes")  # exists via its file
            assert len(counting_store._data) == before
            assert counting_backend.is_dir("output") is True
            assert counting_backend.is_dir("notes") is True
            counting_backend.mkdir("fresh")  # genuinely new → written
            assert counting_backend.is_dir("fresh") is True
        finally:
            counting_backend.end_read_cache()
        assert counting_backend.is_dir("fresh") is True


# =============================================================================
# Cloud folders: the mount table under cloud/<name>
# (features/cloud_folders_on_virtual_sessions.md § Agent)
# =============================================================================

from agent.core.backends.virtual import (  # noqa: E402
    BoundedListing,
    CloudFileChanged,
    CloudFolderError,
    CloudFolderReadOnly,
    CloudRemote,
)


def _cloud_backend(**kwargs):
    """A workspace prefix plus two cloud folders: ``cloud/outbox``
    (read-write) and ``cloud/proj`` (read-only), each its own store."""
    ws, outbox, proj = (
        InMemoryObjectStore(),
        InMemoryObjectStore(),
        InMemoryObjectStore(),
    )
    backend = VirtualWorkspaceBackend(
        ws,
        prefix=PREFIX,
        cloud_remotes=[
            CloudRemote(
                name="outbox",
                path="cloud/outbox",
                store=outbox,
                access="read_write",
                kind="cloud_outbox",
                mount_id="m-out",
                shared=True,
            ),
            CloudRemote(
                name="proj",
                path="cloud/proj",
                store=proj,
                access="read_only",
                kind="project",
                mount_id="m-proj",
            ),
        ],
        **kwargs,
    )
    return backend, ws, outbox, proj


class TestCloudRouting:
    def test_without_folders_cloud_is_an_ordinary_workspace_path(self, backend, store):
        backend.write_file("cloud/x.txt", "ws")
        assert store.get(PREFIX + "cloud/x.txt") == b"ws"
        assert backend.list_dir("") == ["cloud/"]

    def test_cloud_root_lists_the_bound_folders_and_nothing_else(self):
        backend, *_ = _cloud_backend()
        assert backend.list_dir("cloud") == ["cloud/outbox/", "cloud/proj/"]
        assert backend.list_dir("cloud", pattern="out*") == ["cloud/outbox/"]
        assert backend.is_dir("cloud") and not backend.is_file("cloud")
        assert backend.exists("cloud/outbox") and backend.is_dir("cloud/outbox")
        with pytest.raises(CloudFolderError, match="cloud' itself"):
            backend.write_file("cloud", "x")

    def test_the_workspace_root_lists_cloud_beside_its_own_files(self):
        backend, *_ = _cloud_backend()
        backend.write_file("plan.md", "p")
        assert backend.list_dir("") == ["cloud/", "plan.md"]
        # ... and the root walk never enters the cloud folders.
        backend.write_file("cloud/outbox/r.md", "r")
        assert backend.walk() == ["plan.md"]
        assert backend.list_files_with_sizes() == [("plan.md", 1)]

    def test_an_unknown_name_under_cloud_is_named_with_the_folders(self):
        backend, *_ = _cloud_backend()
        assert backend.exists("cloud/nothere") is False
        assert backend.list_dir("cloud/nothere") == []
        with pytest.raises(FileNotFoundError, match="cloud/outbox, cloud/proj"):
            backend.read_file("cloud/nothere/a.txt")
        with pytest.raises(CloudFolderError, match="no 'cloud/nothere'"):
            backend.write_file("cloud/nothere/a.txt", "x")

    def test_escapes_are_rejected_before_routing(self):
        backend, *_ = _cloud_backend()
        with pytest.raises(ValueError, match="escapes"):
            backend.read_file("cloud/outbox/../../../x")
        with pytest.raises(ValueError, match="escapes"):
            backend.write_file("/cloud/outbox/x", "x")

    def test_resolve_path_of_a_cloud_file_is_the_path_the_user_knows(self):
        backend, *_ = _cloud_backend()
        assert backend.resolve_path("cloud/outbox/./a/../r.md") == "cloud/outbox/r.md"
        assert backend.resolve_path("notes/a.md") == PREFIX + "notes/a.md"

    def test_duplicate_or_invalid_folder_names_are_refused(self, store):
        one = CloudRemote(name="a", path="cloud/a", store=InMemoryObjectStore())
        with pytest.raises(ValueError, match="bound twice"):
            VirtualWorkspaceBackend(store, cloud_remotes=[one, one])
        with pytest.raises(ValueError, match="not valid"):
            VirtualWorkspaceBackend(
                store,
                cloud_remotes=[
                    CloudRemote(
                        name="a/b", path="cloud/a/b", store=InMemoryObjectStore()
                    )
                ],
            )


class TestCloudFileOperations:
    def test_write_and_read_go_to_the_folders_own_store(self):
        backend, ws, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/report.md", "hello")
        assert outbox.get("report.md") == b"hello"
        assert ws.list("") == []  # nothing of it in the workspace prefix
        assert backend.read_file("cloud/outbox/report.md") == "hello"
        assert backend.read_file("cloud/outbox/report.md", binary=True) == b"hello"
        assert backend.is_file("cloud/outbox/report.md")
        assert backend.stat("cloud/outbox/report.md") == 5

    def test_the_humans_write_is_seen_at_the_next_read(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/r.md", "agent")
        outbox.put("r.md", b"human")  # the Nextcloud client wrote
        assert backend.read_file("cloud/outbox/r.md") == "human"

    def test_nested_paths_and_directories(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/a/b/c.txt", "c")
        assert backend.is_dir("cloud/outbox/a") and backend.is_dir("cloud/outbox/a/b")
        assert backend.list_dir("cloud/outbox") == ["cloud/outbox/a/"]
        assert backend.list_dir("cloud/outbox/a/b") == ["cloud/outbox/a/b/c.txt"]
        assert backend.list_dir("cloud/outbox/a/b/c.txt") == ["cloud/outbox/a/b/c.txt"]
        assert backend.walk("cloud/outbox") == ["cloud/outbox/a/b/c.txt"]
        assert backend.list_files_with_sizes("cloud/outbox/a") == [
            ("cloud/outbox/a/b/c.txt", 1)
        ]
        with pytest.raises(ValueError, match="Not a file"):
            backend.read_file("cloud/outbox/a")
        with pytest.raises(ValueError, match="Is a directory"):
            backend.write_file("cloud/outbox/a", "x")
        with pytest.raises(FileNotFoundError):
            backend.read_file("cloud/outbox/ghost.txt")

    def test_mkdir_creates_a_real_folder_and_no_marker_object(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.mkdir("cloud/outbox/drafts")
        assert backend.is_dir("cloud/outbox/drafts")
        assert backend.list_dir("cloud/outbox") == ["cloud/outbox/drafts/"]
        assert outbox.list("") == []  # no .keep in the user's cloud
        backend.mkdir("cloud/outbox/drafts")  # idempotent
        backend.mkdir("cloud/outbox")  # the root exists
        backend.write_file("cloud/outbox/f.txt", "f")
        with pytest.raises(ValueError, match="Not a directory"):
            backend.mkdir("cloud/outbox/f.txt")

    def test_delete_file_and_directory(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/d/a.txt", "a")
        backend.write_file("cloud/outbox/d/e/b.txt", "b")
        backend.mkdir("cloud/outbox/empty")
        with pytest.raises(ValueError, match="non-empty"):
            backend.delete_file("cloud/outbox/d")
        assert backend.delete_file("cloud/outbox/empty") is True
        assert backend.is_dir("cloud/outbox/empty") is False
        assert backend.delete_file("cloud/outbox/d/a.txt") is True
        assert backend.delete_file("cloud/outbox/d/a.txt") is False
        assert backend.delete_directory("cloud/outbox/d") is True
        assert outbox.list("") == [] and backend.is_dir("cloud/outbox/d") is False
        assert backend.delete_directory("cloud/outbox/d") is False
        with pytest.raises(CloudFolderError, match="itself"):
            backend.delete_directory("cloud/outbox")
        with pytest.raises(CloudFolderError, match="itself"):
            backend.delete_file("cloud/outbox")

    def test_append_is_a_conditional_write(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.append_file("cloud/outbox/log.md", "one\n")
        backend.append_file("cloud/outbox/log.md", "two\n")
        assert outbox.get("log.md") == b"one\ntwo\n"

    def test_a_whole_file_write_after_a_human_edit_is_refused_until_reread(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/r.md", "v1")
        backend.read_file("cloud/outbox/r.md")
        outbox.put("r.md", b"human")
        with pytest.raises(CloudFileChanged, match="changed in the cloud"):
            backend.write_file("cloud/outbox/r.md", "agent")
        assert outbox.get("r.md") == b"human"  # nothing written
        assert backend.read_file("cloud/outbox/r.md") == "human"
        backend.write_file("cloud/outbox/r.md", "agent")  # after the re-read
        assert outbox.get("r.md") == b"agent"

    def test_a_file_never_read_is_simply_overwritten(self):
        backend, _, outbox, _ = _cloud_backend()
        outbox.put("r.md", b"human")
        backend.write_file("cloud/outbox/r.md", "agent")  # ruling 5
        assert outbox.get("r.md") == b"agent"

    def test_every_mutation_is_journalled_with_versions(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/r.md", "v1")
        backend.append_file("cloud/outbox/r.md", "+")
        backend.delete_file("cloud/outbox/r.md")
        ops = [
            (c["op"], c["path"], c["version_before"], c["version_after"])
            for c in backend.cloud_changes
        ]
        assert ops == [
            ("write", "cloud/outbox/r.md", None, "1"),
            ("append", "cloud/outbox/r.md", "1", "2"),
            ("delete", "cloud/outbox/r.md", "2", None),
        ]
        assert backend.cloud_changes[0]["mount_id"] == "m-out"
        assert backend.cloud_changes[0]["bytes"] == 2

    def test_reads_above_the_ceiling_are_refused_with_the_size(self):
        backend, _, outbox, _ = _cloud_backend(max_read_bytes=4)
        outbox.put("big.bin", b"x" * 5)
        with pytest.raises(ValueError, match="5 bytes.*work on a part"):
            backend.read_file("cloud/outbox/big.bin")


class TestReadOnlyFolder:
    @pytest.mark.parametrize(
        "op",
        [
            lambda b: b.write_file("cloud/proj/a.txt", "x"),
            lambda b: b.append_file("cloud/proj/a.txt", "x"),
            lambda b: b.mkdir("cloud/proj/d"),
            lambda b: b.delete_file("cloud/proj/a.txt"),
            lambda b: b.delete_directory("cloud/proj/d"),
            lambda b: b.move("cloud/proj/a.txt", "cloud/proj/b.txt"),
            lambda b: b.move("cloud/outbox/o.txt", "cloud/proj/o.txt"),
            lambda b: b.copy("notes/n.txt", "cloud/proj/n.txt"),
        ],
    )
    def test_writes_are_refused_naming_the_folder_and_level(self, op):
        backend, _, outbox, proj = _cloud_backend()
        proj.put("a.txt", b"a")
        proj.put("d/x", b"x")
        outbox.put("o.txt", b"o")
        backend.write_file("notes/n.txt", "n")
        with pytest.raises(
            CloudFolderReadOnly, match="cloud/proj.*read_only"
        ) as excinfo:
            op(backend)
        assert isinstance(excinfo.value, ValueError)  # a tool error, not a crash
        assert proj.get("a.txt") == b"a" and proj.get("d/x") == b"x"

    def test_reads_and_copies_out_still_work(self):
        backend, _, _, proj = _cloud_backend()
        proj.put("a.txt", b"a")
        assert backend.read_file("cloud/proj/a.txt") == "a"
        backend.copy("cloud/proj/a.txt", "notes/a.txt")
        assert backend.read_file("notes/a.txt") == "a"
        backend.copy("cloud/proj/a.txt", "cloud/outbox/a.txt")
        assert backend.read_file("cloud/outbox/a.txt") == "a"


class TestCloudTransfers:
    def test_within_one_folder_the_store_copies(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/a.txt", "a")
        backend.copy("cloud/outbox/a.txt", "cloud/outbox/b.txt")
        backend.move("cloud/outbox/b.txt", "cloud/outbox/c.txt")
        assert sorted(i.key for i in outbox.list("")) == ["a.txt", "c.txt"]

    def test_a_directory_moves_within_one_folder(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.write_file("cloud/outbox/d/a.txt", "a")
        backend.write_file("cloud/outbox/d/e/b.txt", "b")
        backend.move("cloud/outbox/d", "cloud/outbox/f")
        assert backend.walk("cloud/outbox") == [
            "cloud/outbox/f/a.txt",
            "cloud/outbox/f/e/b.txt",
        ]
        assert backend.is_dir("cloud/outbox/d") is False

    def test_across_stores_a_file_goes_through_the_process(self):
        backend, ws, outbox, _ = _cloud_backend()
        backend.write_file("notes/n.txt", "n")
        backend.move("notes/n.txt", "cloud/outbox/n.txt")
        assert outbox.get("n.txt") == b"n" and not backend.exists("notes/n.txt")
        backend.copy("cloud/outbox/n.txt", "out/n.txt")
        assert ws.get(PREFIX + "out/n.txt") == b"n" and outbox.get("n.txt") == b"n"
        backend.move("cloud/outbox/n.txt", "cloud/outbox/sub/n.txt")
        assert backend.read_file("cloud/outbox/sub/n.txt") == "n"

    def test_a_directory_across_stores_is_refused_naming_both_places(self):
        backend, *_ = _cloud_backend()
        backend.write_file("notes/a.txt", "a")
        with pytest.raises(CloudFolderError, match="the workspace.*cloud/outbox"):
            backend.move("notes", "cloud/outbox/notes")
        backend.write_file("cloud/outbox/d/a.txt", "a")
        with pytest.raises(CloudFolderError, match="cloud/outbox.*the workspace"):
            backend.move("cloud/outbox/d", "notes2")

    def test_the_folder_itself_and_cloud_cannot_move(self):
        backend, *_ = _cloud_backend()
        with pytest.raises(CloudFolderError, match="itself"):
            backend.move("cloud/outbox", "cloud/outbox2")
        with pytest.raises(CloudFolderError, match="itself"):
            backend.copy("cloud", "elsewhere")


class TestCloudBounds:
    def test_search_under_a_folder_is_capped_and_says_so(self):
        backend, _, outbox, _ = _cloud_backend(cloud_search_files=3)
        for i in range(5):
            outbox.put(f"f{i}.txt", b"needle here")
        results = backend.search_files("needle", "cloud/outbox")
        assert isinstance(results, BoundedListing)
        assert len(results) == 3 and results.truncated and results.scanned == 3
        assert results.limit == 3
        assert all(r["path"].startswith("cloud/outbox/") for r in results)
        narrow = backend.search_files("needle", "cloud/outbox/f1.txt")
        assert len(narrow) == 1 and not narrow.truncated

    def test_search_skips_binary_names_big_files_and_excluded_dirs(self):
        backend, _, outbox, _ = _cloud_backend(search_file_bytes=20)
        outbox.put("a.txt", b"needle")
        outbox.put("img.png", b"needle")
        outbox.put("big.txt", b"needle" + b" " * 30)
        outbox.put("node_modules/x.txt", b"needle")
        found = backend.search_files(
            "needle", "cloud/outbox", exclude_dirs=["node_modules"]
        )
        assert [r["path"] for r in found] == ["cloud/outbox/a.txt"]

    def test_a_root_wide_search_is_refused_while_folders_are_bound(self):
        backend, *_ = _cloud_backend()
        backend.write_file("notes/a.txt", "needle")
        with pytest.raises(CloudFolderError, match="name a workspace folder"):
            backend.search_files("needle", "")
        with pytest.raises(CloudFolderError):
            backend.search_files("needle", "cloud")
        # A named workspace folder searches as before, never the cloud.
        assert [r["path"] for r in backend.search_files("needle", "notes")] == [
            "notes/a.txt"
        ]
        assert backend.search_files("needle", "cloud/nothere") == []

    def test_a_workspace_without_folders_still_searches_its_root(self, backend):
        backend.write_file("a.txt", "needle")
        assert len(backend.search_files("needle", "")) == 1

    def test_walk_under_a_folder_stops_at_the_cap(self):
        backend, _, outbox, _ = _cloud_backend(cloud_walk_entries=2)
        for i in range(4):
            outbox.put(f"d{i % 2}/f{i}.txt", b"x")
        files = backend.walk("cloud/outbox")
        assert len(files) == 2 and files.truncated and files.limit == 2
        assert backend.stat("cloud/outbox") == 2  # the bounded sum
        assert backend.walk("cloud/nothere") == []

    def test_cloud_paths_bypass_the_scoped_index(self):
        backend, _, outbox, _ = _cloud_backend()
        backend.begin_read_cache()
        try:
            outbox.put("late.md", b"written while the index is open")
            assert backend.is_file("cloud/outbox/late.md")  # live, not indexed
            assert backend.read_file("cloud/outbox/late.md").startswith("written")
            backend.write_file("cloud/outbox/new.md", "n")
            assert backend._index == {}  # untouched by cloud work
        finally:
            backend.end_read_cache()


class TestCloudFolderState:
    def test_rows_and_probe(self):
        backend, _, outbox, proj = _cloud_backend()
        rows = backend.cloud_folders()
        assert [(r["name"], r["path"], r["access"], r["state"]) for r in rows] == [
            ("outbox", "cloud/outbox", "read_write", "bound"),
            ("proj", "cloud/proj", "read_only", "bound"),
        ]
        assert rows[0]["delivery"] == "remote" and rows[0]["shared"] is True
        assert rows[0]["kind"] == "cloud_outbox" and rows[0]["mount_id"] == "m-out"

        class Broken(InMemoryObjectStore):
            def list_dir(self, prefix):
                raise RuntimeError("401 Unauthorized")

        proj_remote = backend.cloud_remotes[1]
        proj_remote.store = Broken()
        probed = backend.probe_cloud_folders()
        assert [(r["name"], r["state"], r["reason"]) for r in probed] == [
            ("outbox", "ready", None),
            ("proj", "failed", "mount_failed"),
        ]
        assert "401" in proj_remote.detail  # logged, never shown to the agent

    def test_connect_and_disconnect_reach_every_store(self):
        calls = []

        class Tracking(InMemoryObjectStore):
            def __init__(self, label):
                super().__init__()
                self.label = label

            def connect(self):
                calls.append(("connect", self.label))

            def close(self):
                calls.append(("close", self.label))

        backend = VirtualWorkspaceBackend(
            Tracking("ws"),
            prefix=PREFIX,
            cloud_remotes=[CloudRemote(name="o", path="cloud/o", store=Tracking("o"))],
        )
        backend.connect()
        backend.disconnect()
        assert calls == [
            ("connect", "ws"),
            ("connect", "o"),
            ("close", "ws"),
            ("close", "o"),
        ]

    def test_with_max_read_bytes_keeps_the_folders(self):
        backend, *_ = _cloud_backend()
        reader = backend.with_max_read_bytes(1)
        assert [r.name for r in reader.cloud_remotes] == ["outbox", "proj"]
