"""Virtual workspace backend — explicit object-store file ops, no workspace pod.

The ``virtual`` tier (``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §5) gives a
lite agent real file tools without provisioning a workspace container/VM: file
operations are explicit object-store ops against a per-job/per-thread key
prefix, so files are durable in object storage and survive pod death. There is
**no** FUSE, no mount, no privileged pod, and no shell.

This backend implements the full :class:`WorkspaceBackend` contract as prefix
math over an injected :class:`~shared.runtime.core.backends.object_store.ObjectStore`
(rclone in production, in-memory in tests — the logic is identical). Because S3
is a flat key space, a few POSIX semantics are approximated; each such
"S3-ism" is called out inline and pinned by the contract tests:

- **Empty directories** don't exist in object storage. ``mkdir`` writes a
  hidden ``.keep`` marker so a created-but-empty directory is still visible to
  ``is_dir``/``list_dir`` (and the marker is filtered out of listings/search).
- **``append_file``/``edit`` are read-modify-write** with no atomicity — safe
  under the tier's single-writer-per-prefix assumption (§5).
- **``search_files`` is bounded** content search (binary extensions skipped,
  per-file byte cap) — the hydration-cost lesson from rclone_cloud_mount §8 in
  miniature.
- **Reads are size-guarded** so a huge object can't OOM the agent pod.

``supports_shell`` is False and there is no ``$HOME`` concept — ``write_home_file``
and ``resolve_home_path`` inherit the base ``NotImplementedError`` (the lite
tier excludes shell/git/repository datasources by construction; §7).

Cloud folders (``features/cloud_folders_on_virtual_sessions.md`` § Agent)
-------------------------------------------------------------------------

A Session's attached cloud connectors arrive as further entries of
``workspace.mounts``; each becomes a :class:`CloudRemote` in this backend's
**mount table**, reachable under ``cloud/<name>`` from the file-tools root,
the path a Container's FUSE mount has. Resolution is first-segment: ``cloud``
is a read-only virtual directory listing the bound folders, ``cloud/<name>/…``
addresses that folder's own store with the rest of the path as the key, and
everything else is the workspace prefix as before. What differs for a cloud
path, by design:

- it is **always live**: the scoped metadata index (``begin_read_cache``)
  covers the workspace prefix only, and no cloud op consults or updates it;
- **directories are real** where the provider has them: ``mkdir`` creates a
  folder through the store and never writes a ``.keep`` object into a user's
  cloud; ``is_dir``/``exists``/``list_dir`` cost one bounded call;
- **listings, walks and searches are bounded**: ``walk`` stops after
  :data:`DEFAULT_CLOUD_WALK_ENTRIES` files, ``search_files`` after
  :data:`DEFAULT_CLOUD_SEARCH_FILES` candidates, and both say so
  (:class:`BoundedListing`); a root-wide search that would span the cloud
  folders is refused until a path names one;
- a **``read_only`` folder refuses writes here** as assistance (the credential
  is the guarantee), naming the folder and the level;
- **read-modify-write is checked**: a read remembers the object's version
  token, ``append_file`` writes with that precondition, and a whole-file
  write is refused when the object changed since this backend read it
  (:class:`CloudFileChanged`); every mutation is journalled with the version
  before and after (``cloud_changes``);
- **moves and copies across stores** are a download and an upload through
  this process; a directory across stores is refused, naming both folders.
"""

import fnmatch
import logging
import posixpath
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from shared.runtime.core.workspace_backend import WorkspaceBackend, search_excludes
from shared.runtime.core.backends.object_store import (
    InMemoryObjectStore,
    ObjectStat,
    ObjectStore,
    ObjectVersionMismatch,
)

logger = logging.getLogger(__name__)

# Hidden marker that keeps an otherwise-empty directory visible in a flat key
# space. Filtered out of list_dir() / search_files() so the agent never sees it.
_DIR_MARKER = ".keep"

# Single-object read guard (bytes). The word-count truncation lives higher up
# (file tools / max_read_words); this is the hard ceiling that stops a
# multi-GB object from being pulled into the agent pod's memory.
DEFAULT_MAX_READ_BYTES = 50 * 1024 * 1024  # 50 MiB

# search_files bounds (the hydration guard). Files larger than this are skipped
# for content search; name/path matching is unaffected.
DEFAULT_SEARCH_FILE_BYTES = 1 * 1024 * 1024  # 1 MiB

# The folder under the file-tools root that holds every bound cloud folder:
# ``cloud/<name>``, the same path a Container's mount has.
CLOUD_ROOT = "cloud"

# How many files one ``search_files`` under a cloud folder considers (ruling 6
# of the feature note): the result then says it was truncated.
DEFAULT_CLOUD_SEARCH_FILES = 200

# How many files a ``walk`` or size probe under a cloud folder lists before it
# stops and says so; a user's cloud is not a workspace.
DEFAULT_CLOUD_WALK_ENTRIES = 2000

# A walk under a cloud folder never descends deeper than this.
CLOUD_WALK_MAX_DEPTH = 32

# The state names of a cloud folder on this tier (the status record and the
# prompt rows use them, the Container's are ``mounted``/``pending``/
# ``unavailable``): ``bound`` = the descriptor arrived and the store exists,
# ``ready`` = one probe of the remote's root answered, ``failed`` = it did not.
CLOUD_STATE_BOUND = "bound"
CLOUD_STATE_READY = "ready"
CLOUD_STATE_FAILED = "failed"

# Extensions skipped during content search (binary / non-text payloads).
_BINARY_SEARCH_EXTS = frozenset(
    {
        ".pdf",
        ".docx",
        ".doc",
        ".xlsx",
        ".pptx",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".bmp",
        ".webp",
        ".ico",
        ".zip",
        ".gz",
        ".tar",
        ".bz2",
        ".7z",
        ".mp3",
        ".mp4",
        ".mov",
        ".avi",
        ".wav",
        ".so",
        ".bin",
        ".o",
        ".pyc",
        ".woff",
        ".woff2",
        ".ttf",
    }
)


class CloudFolderError(ValueError):
    """A file operation on a cloud folder that the backend refuses on
    purpose; the message is written for the agent (a tool error)."""


class CloudFolderReadOnly(CloudFolderError):
    """A write under a folder bound at the ``read_only`` level."""


class CloudFileChanged(CloudFolderError):
    """A read-modify-write found the cloud file at another version than the
    one this backend read; nothing was written."""


class BoundedListing(list):
    """A ``list`` result of a bounded cloud operation that also says whether
    the bound was hit (``truncated``), how many entries were considered
    (``scanned``) and what the bound was (``limit``). A plain list for every
    consumer that does not care."""

    truncated: bool = False
    scanned: int = 0
    limit: Optional[int] = None


@dataclass
class CloudRemote:
    """One cloud folder bound into the backend's mount table.

    Attributes:
        name: The folder's name under ``cloud/`` (``outbox``, the project's
            slug, ``my_cloud``), the sidecar plan's ``workspace_name``.
        path: Its path from the file-tools root (``cloud/<name>``).
        store: The folder's own object store; keys are relative to the
            folder's root (``""`` is the root).
        access: ``read_write`` or ``read_only`` (refused here as assistance).
        kind: The connector's ``mount_kind`` (``cloud_outbox``, ``project``,
            ``user_root``), for the prompt rows.
        mount_id: The connector id, for the status record.
        shared: For an outbox, whether SRW could share it with the user yet.
        state: :data:`CLOUD_STATE_BOUND`, ``ready`` or ``failed``.
        reason: A closed reason code when ``failed``.
    """

    name: str
    path: str
    store: ObjectStore
    access: str = "read_write"
    kind: str = ""
    mount_id: str = ""
    shared: Optional[bool] = None
    state: str = CLOUD_STATE_BOUND
    reason: Optional[str] = None
    detail: Optional[str] = field(default=None, repr=False)

    @property
    def read_only(self) -> bool:
        return self.access == "read_only"

    def key_prefix(self, key: str) -> str:
        """The listing prefix of the folder at ``key`` (``""`` for the root)."""
        return f"{key}/" if key else ""


# The route target for the ``cloud`` directory itself (and an unknown name
# under it): a virtual, read-only directory that lists the bound folders.
_CLOUD_DIR = object()


class VirtualWorkspaceBackend(WorkspaceBackend):
    """WorkspaceBackend backed by explicit object-store ops under a key prefix.

    Args:
        object_store: The blob transport (rclone in prod, in-memory in tests).
        prefix: Per-job/per-thread key prefix (e.g. ``jobs/<job_id>/``). Leading
            slashes are stripped and a trailing slash is enforced; an empty
            prefix means the whole store is the workspace (rare; for tests).
        max_read_bytes: Single-object read ceiling.
        search_file_bytes: Per-file content-search ceiling.
        cloud_remotes: The cloud folders bound under ``cloud/<name>`` (see the
            module docstring); none by default.
        cloud_search_files: How many files a search under a cloud folder
            considers before it stops and says so.
        cloud_walk_entries: How many files a walk under a cloud folder lists
            before it stops and says so.
    """

    def __init__(
        self,
        object_store: ObjectStore,
        prefix: str = "",
        *,
        max_read_bytes: int = DEFAULT_MAX_READ_BYTES,
        search_file_bytes: int = DEFAULT_SEARCH_FILE_BYTES,
        cloud_remotes: Optional[Iterable[CloudRemote]] = None,
        cloud_search_files: int = DEFAULT_CLOUD_SEARCH_FILES,
        cloud_walk_entries: int = DEFAULT_CLOUD_WALK_ENTRIES,
    ):
        self._store = object_store
        self._prefix = self._normalize_prefix(prefix)
        self._max_read_bytes = max_read_bytes
        self._search_file_bytes = search_file_bytes
        self._connected = False
        # Scoped metadata index — see begin_read_cache(). None = inactive,
        # which is the default and the state during all normal tool work.
        self._index: dict[str, int] | None = None
        # The mount table: name -> remote, in binding order.
        self._remotes: dict[str, CloudRemote] = {}
        for remote in cloud_remotes or ():
            if remote.name in self._remotes:
                raise ValueError(f"cloud folder {remote.name!r} is bound twice")
            if not remote.name or "/" in remote.name or remote.name in (".", ".."):
                raise ValueError(f"cloud folder name {remote.name!r} is not valid")
            self._remotes[remote.name] = remote
        self._cloud_search_files = cloud_search_files
        self._cloud_walk_entries = cloud_walk_entries
        # (folder name, key) -> the version token of the last read, so a
        # read-modify-write can tell that someone else wrote in between.
        self._cloud_versions: dict[tuple[str, str], Optional[str]] = {}
        #: Every mutation under ``cloud/`` this backend performed: dicts of
        #: ``path``, ``name``, ``mount_id``, ``op``, ``version_before``,
        #: ``version_after`` and ``bytes`` (the change record's agent half).
        self.cloud_changes: list[dict[str, Any]] = []

    # =========================================================================
    # Path / prefix math (escape-proof by construction — no real filesystem)
    # =========================================================================

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        prefix = (prefix or "").strip().lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return prefix

    def _normalize_rel(self, relative_path: str) -> str:
        """Normalize a workspace-relative path; raise if it escapes the root.

        Returns "" for the workspace root. The result never starts with "/"
        or "..".
        """
        if not relative_path:
            return ""
        p = relative_path.strip()
        if p in ("", ".", "./"):
            return ""
        if p.startswith("/"):
            raise ValueError(f"Path '{relative_path}' escapes workspace boundary")
        norm = posixpath.normpath(p)
        if norm == ".":
            return ""
        if norm == ".." or norm.startswith("../"):
            raise ValueError(f"Path '{relative_path}' escapes workspace boundary")
        return norm

    def _object_key(self, relative_path: str) -> str:
        """Full object key for a file at ``relative_path`` (prefix + normalized)."""
        return self._prefix + self._normalize_rel(relative_path)

    def _dir_prefix(self, relative_path: str) -> str:
        """Key prefix that contains the children of the directory at the path."""
        norm = self._normalize_rel(relative_path)
        if norm == "":
            return self._prefix
        return self._prefix + norm + "/"

    def _strip_prefix(self, key: str) -> str:
        """Object key -> workspace-root-relative path."""
        if self._prefix and key.startswith(self._prefix):
            return key[len(self._prefix) :]
        return key

    # -------------------------------------------------------------- routing

    def _route(self, relative_path: str) -> tuple[Any, str]:
        """``(target, rest)`` for a path: ``target`` is ``None`` for the
        workspace prefix (``rest`` = the normalized path), a
        :class:`CloudRemote` (``rest`` = the key inside it, ``""`` for its
        root) or :data:`_CLOUD_DIR` for ``cloud`` itself and an unknown name
        under it (``rest`` = what followed ``cloud/``). Escapes are rejected
        before matching. Without bound folders every path is the workspace's.
        """
        norm = self._normalize_rel(relative_path)
        if not self._remotes:
            return None, norm
        if norm == CLOUD_ROOT:
            return _CLOUD_DIR, ""
        if not norm.startswith(CLOUD_ROOT + "/"):
            return None, norm
        rest = norm[len(CLOUD_ROOT) + 1 :]
        name, _sep, inside = rest.partition("/")
        remote = self._remotes.get(name)
        if remote is None:
            return _CLOUD_DIR, rest
        return remote, inside

    @property
    def cloud_remotes(self) -> list[CloudRemote]:
        """The bound cloud folders, in binding order."""
        return list(self._remotes.values())

    def _cloud_paths_text(self) -> str:
        return ", ".join(remote.path for remote in self._remotes.values())

    def _no_such_folder(self, relative_path: str, rest: str) -> CloudFolderError:
        name = rest.partition("/")[0]
        return CloudFolderError(
            f"'{relative_path}' is not in a cloud folder of this session: there "
            f"is no '{CLOUD_ROOT}/{name}'. The cloud folders are: "
            f"{self._cloud_paths_text()}."
        )

    @staticmethod
    def _refuse_read_only(remote: CloudRemote, verb: str, relative_path: str) -> None:
        if remote.read_only:
            raise CloudFolderReadOnly(
                f"{remote.path} is a read-only cloud folder (level read_only): "
                f"cannot {verb} '{relative_path}'. Its account may only read "
                f"this folder; save your work in the workspace or in a "
                f"read-write folder instead."
            )

    def _cloud_kind(self, remote: CloudRemote, key: str) -> str:
        """``"file"``, ``"dir"`` or ``"absent"`` for ``key`` in ``remote``:
        one ``stat`` (which knows folders on a remote that has them) and, when
        that says nothing, one listing for a flat store."""
        if key == "":
            return "dir"
        info = remote.store.stat(key)
        if info is not None:
            return "dir" if info.is_dir else "file"
        if remote.store.list_dir(remote.key_prefix(key)):
            return "dir"
        return "absent"

    def _journal(
        self,
        remote: CloudRemote,
        relative_path: str,
        op: str,
        before: Optional[ObjectStat],
        after: Optional[ObjectStat],
        size: int,
    ) -> None:
        entry = {
            "path": relative_path,
            "name": remote.name,
            "mount_id": remote.mount_id,
            "op": op,
            "version_before": before.version if before is not None else None,
            "version_after": after.version if after is not None else None,
            "bytes": size,
        }
        self.cloud_changes.append(entry)
        logger.info(
            "cloud folder change: %s %s (%d bytes, version %s -> %s)",
            op,
            relative_path,
            size,
            entry["version_before"],
            entry["version_after"],
        )

    def _remember_version(
        self, remote: CloudRemote, key: str, info: Optional[ObjectStat]
    ) -> None:
        self._cloud_versions[(remote.name, key)] = (
            info.version if info is not None else None
        )

    # =========================================================================
    # Scoped metadata index
    # =========================================================================
    #
    # Every store op is a process spawn (rclone), so the op COUNT is the cost.
    # Session attach measured 51 spawns / 7.2s on k3d (2026-08-08), 36 of them
    # listings — because setup repeatedly asks "does this exist?" about one
    # small tree: workspace scaffolding, instruction files, skill files, the
    # legacy-tools sweep. One recursive listing answers all of them.
    #
    # The index is deliberately NOT ambient. It is opened around a known
    # burst of metadata probes and closed straight after, so ordinary tool
    # work keeps talking to the store directly and no caller ever has to
    # reason about staleness. Within the scope it stays exact: local
    # mutations update it in place (we know precisely which keys moved), and
    # the only writer that could invalidate it is another process touching
    # the same prefix mid-attach — whose write is picked up by the first
    # uncached op after the scope closes.
    #
    # Cloud folders are never indexed: a user's folder is live, large and
    # written by others; every cloud op below talks to its store directly.

    def begin_read_cache(self) -> None:
        """Open the scoped metadata index (idempotent). One store op."""
        if self._index is not None:
            return
        index: dict[str, int] = {}
        for info in self._store.list(self._prefix):
            index[info.key] = info.size
        self._index = index

    def end_read_cache(self) -> None:
        """Close the scope; subsequent probes hit the store again."""
        self._index = None

    def _indexed_size(self, key: str) -> int | None:
        return self._index.get(key) if self._index is not None else None

    def _indexed_has_prefix(self, prefix: str) -> bool:
        return self._index is not None and any(
            k.startswith(prefix) for k in self._index
        )

    def _index_put(self, key: str, size: int) -> None:
        if self._index is not None:
            self._index[key] = size

    def _index_drop(self, key: str) -> None:
        if self._index is not None:
            self._index.pop(key, None)

    def _index_drop_prefix(self, prefix: str) -> None:
        if self._index is not None:
            for k in [k for k in self._index if k.startswith(prefix)]:
                del self._index[k]

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def root(self) -> str:
        """The object-store key prefix that is this workspace's virtual root."""
        return self._prefix

    @property
    def supports_shell(self) -> bool:
        # Explicit: the virtual tier has no shell. ShellManager refuses to
        # construct without a shell-capable backend, so shell tools are absent.
        return False

    @property
    def supports_canvas_presentation(self) -> bool:
        """Whether another orchestrator process can materialize these files.

        The dev ``memory`` transport lives only inside this agent process. A
        Canvas gateway in the orchestrator cannot read it, so advertising
        ``set_canvas`` would create a guaranteed-dead presentation. Durable
        rclone-backed virtual stores remain eligible.
        """

        return not isinstance(self._store, InMemoryObjectStore)

    # =========================================================================
    # File operations
    # =========================================================================

    def read_file(self, path: str, binary: bool = False) -> str | bytes:
        target, rest = self._route(path)
        if target is not None:
            data = self._cloud_read(target, rest, path)
            return data if binary else data.decode("utf-8")
        key = self._object_key(path)
        size = self._store.head(key)
        if size is None:
            # No object at this exact key. Mirror the filesystem backend: a
            # directory path is a "Not a file" error, a truly absent path is
            # FileNotFoundError.
            if self.is_dir(path):
                raise ValueError(f"Not a file: {path}")
            raise FileNotFoundError(f"File not found: {path}")
        if size > self._max_read_bytes:
            raise ValueError(
                f"File '{path}' is {size} bytes, exceeds the read limit of "
                f"{self._max_read_bytes} bytes"
            )
        data = self._store.get(key)
        if binary:
            return data
        return data.decode("utf-8")

    def _cloud_read(self, target: Any, rest: str, path: str) -> bytes:
        if target is _CLOUD_DIR:
            if rest == "":
                raise ValueError(f"Not a file: {path}")
            raise FileNotFoundError(
                f"File not found: {path} (the cloud folders are: "
                f"{self._cloud_paths_text()})"
            )
        remote: CloudRemote = target
        if rest == "":
            raise ValueError(f"Not a file: {path}")
        info = remote.store.stat(rest)
        if info is None:
            if remote.store.list_dir(remote.key_prefix(rest)):
                raise ValueError(f"Not a file: {path}")
            raise FileNotFoundError(f"File not found: {path}")
        if info.is_dir:
            raise ValueError(f"Not a file: {path}")
        if info.size > self._max_read_bytes:
            raise ValueError(
                f"File '{path}' is {info.size} bytes, exceeds the read limit of "
                f"{self._max_read_bytes} bytes; work on a part of it (a section "
                f"copied into the workspace) instead"
            )
        data = remote.store.get(rest)
        self._remember_version(remote, rest, info)
        return data

    def write_file(self, path: str, content: str | bytes) -> None:
        target, rest = self._route(path)
        data = content.encode("utf-8") if isinstance(content, str) else content
        if target is not None:
            return self._cloud_write(target, rest, path, data)
        norm = rest
        if norm == "":
            raise ValueError("Cannot write to the workspace root as a file")
        # Refuse to clobber a directory with a file (matches POSIX semantics;
        # cheap because is_dir is a single prefix probe).
        if self.is_dir(path):
            raise ValueError(f"Is a directory: {path}")
        self._store.put(self._prefix + norm, data)
        self._index_put(self._prefix + norm, len(data))

    def _cloud_write(self, target: Any, rest: str, path: str, data: bytes) -> None:
        if target is _CLOUD_DIR:
            if rest == "":
                raise CloudFolderError(
                    f"Cannot write to '{CLOUD_ROOT}' itself: it only holds the "
                    f"cloud folders {self._cloud_paths_text()}"
                )
            raise self._no_such_folder(path, rest)
        remote: CloudRemote = target
        if rest == "":
            raise ValueError(
                f"Cannot write to the cloud folder {remote.path} as a file"
            )
        self._refuse_read_only(remote, "write", path)
        before = remote.store.stat(rest)
        if before is not None and before.is_dir:
            raise ValueError(f"Is a directory: {path}")
        expected = self._cloud_versions.get((remote.name, rest))
        if (
            before is not None
            and expected is not None
            and before.version is not None
            and before.version != expected
        ):
            raise CloudFileChanged(
                f"{path} changed in the cloud since you read it; read it again "
                f"before writing. Nothing was written."
            )
        remote.store.put(rest, data)
        after = remote.store.stat(rest)
        self._remember_version(remote, rest, after)
        self._journal(remote, path, "write", before, after, len(data))

    def append_file(self, path: str, content: str) -> None:
        target, rest = self._route(path)
        addition = content.encode("utf-8") if isinstance(content, str) else content
        if target is not None:
            return self._cloud_append(target, rest, path, addition)
        # Read-modify-write: not atomic. Safe under single-writer-per-prefix.
        norm = rest
        if norm == "":
            raise ValueError("Cannot append to the workspace root")
        key = self._prefix + norm
        existing = self._store.get(key) if self._store.head(key) is not None else b""
        self._store.put(key, existing + addition)
        self._index_put(key, len(existing) + len(addition))

    def _cloud_append(self, target: Any, rest: str, path: str, addition: bytes) -> None:
        if target is _CLOUD_DIR:
            if rest == "":
                raise CloudFolderError(f"Cannot append to '{CLOUD_ROOT}' itself")
            raise self._no_such_folder(path, rest)
        remote: CloudRemote = target
        if rest == "":
            raise ValueError(f"Cannot append to the cloud folder {remote.path}")
        self._refuse_read_only(remote, "append to", path)
        before = remote.store.stat(rest)
        if before is not None and before.is_dir:
            raise ValueError(f"Is a directory: {path}")
        existing = remote.store.get(rest) if before is not None else b""
        try:
            after = remote.store.put_if_match(
                rest,
                existing + addition,
                before.version if before is not None else None,
            )
        except ObjectVersionMismatch:
            raise CloudFileChanged(
                f"{path} changed in the cloud while you were appending to it; "
                f"read it again and retry. Nothing was written."
            ) from None
        self._remember_version(remote, rest, after)
        self._journal(remote, path, "append", before, after, len(addition))

    def exists(self, path: str) -> bool:
        return self.is_file(path) or self.is_dir(path)

    def is_file(self, path: str) -> bool:
        target, rest = self._route(path)
        if target is _CLOUD_DIR:
            return False
        if target is not None:
            return self._cloud_kind(target, rest) == "file"
        norm = rest
        if norm == "":
            return False  # the root is a directory, never a file
        key = self._prefix + norm
        if self._index is not None:
            return key in self._index
        return self._store.head(key) is not None

    def is_dir(self, path: str) -> bool:
        target, rest = self._route(path)
        if target is _CLOUD_DIR:
            return rest == ""
        if target is not None:
            return self._cloud_kind(target, rest) == "dir"
        norm = rest
        if norm == "":
            return True  # the workspace root always exists as a directory
        # A directory exists iff some object lives under "<prefix><norm>/".
        dir_prefix = self._prefix + norm + "/"
        if self._index is not None:
            return self._indexed_has_prefix(dir_prefix)
        return bool(self._store.list(dir_prefix))

    def _list_keys(self, prefix: str) -> list[tuple[str, int]]:
        """(key, size) pairs under ``prefix`` — from the scoped index when
        it is open, else one store listing."""
        if self._index is not None:
            return [(k, v) for k, v in self._index.items() if k.startswith(prefix)]
        return [(info.key, info.size) for info in self._store.list(prefix)]

    def list_dir(self, path: str = "", pattern: str = "*") -> list[str]:
        target, rest = self._route(path)
        if target is not None:
            return self._cloud_list_dir(target, rest, path, pattern)
        norm = rest
        if norm != "" and not self.is_dir(path):
            # Matches the filesystem backend: a file path lists itself; a
            # non-existent path lists nothing.
            return [norm] if self.is_file(path) else []

        dir_prefix = self._dir_prefix(path)
        rel_root = norm + "/" if norm else ""

        files: set[str] = set()
        dirs: set[str] = set()
        for key, _size in self._list_keys(dir_prefix):
            remainder = key[len(dir_prefix) :]
            if not remainder:
                continue
            head, slash, _ = remainder.partition("/")
            if slash:
                dirs.add(head)
            elif head != _DIR_MARKER:  # hide the empty-dir marker
                files.add(head)
        if norm == "" and self._remotes:
            # The bound cloud folders live under ``cloud/`` at the root.
            files.discard(CLOUD_ROOT)
            dirs.add(CLOUD_ROOT)

        results: list[str] = []
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                results.append(rel_root + name)
        for name in dirs:
            if fnmatch.fnmatch(name, pattern):
                results.append(rel_root + name + "/")
        return sorted(results)

    def _cloud_list_dir(
        self, target: Any, rest: str, path: str, pattern: str
    ) -> list[str]:
        if target is _CLOUD_DIR:
            if rest != "":
                return []
            return sorted(
                f"{CLOUD_ROOT}/{name}/"
                for name in self._remotes
                if fnmatch.fnmatch(name, pattern)
            )
        remote: CloudRemote = target
        kind = self._cloud_kind(remote, rest)
        norm = self._normalize_rel(path)
        if kind == "file":
            return [norm]
        if kind == "absent":
            return []
        prefix = remote.key_prefix(rest)
        results: list[str] = []
        for entry in remote.store.list_dir(prefix):
            name = entry.key[len(prefix) :]
            if not name or "/" in name or not fnmatch.fnmatch(name, pattern):
                continue
            results.append(f"{norm}/{name}/" if entry.is_dir else f"{norm}/{name}")
        return sorted(results)

    def walk(self, path: str = "") -> list[str]:
        """Flat recursive listing of every file under ``path`` (relative to the
        workspace root), via the object store's recursive ``list``.

        Overrides the base backend's list_dir-descent walk because ``list_dir``
        here collapses to a single level — the flat store already knows every
        key under the prefix in one call (``workspace_tier_upgrade.md`` §4.2
        S3a, the seed source).

        The workspace root's walk never enters the cloud folders (they are
        not the workspace's files: the upgrade seed and the attach sweeps
        must not copy or scan a user's cloud). A walk that names a cloud
        path is bounded (:data:`DEFAULT_CLOUD_WALK_ENTRIES`) and the result,
        a :class:`BoundedListing`, says when it stopped.
        """
        target, rest = self._route(path)
        if target is not None:
            listing = self._cloud_walk(target, rest, path)
            out = BoundedListing(rel for rel, _size in listing)
            out.truncated, out.scanned, out.limit = (
                listing.truncated,
                listing.scanned,
                listing.limit,
            )
            return out
        out: list[str] = []
        for key, _size in self._list_keys(self._dir_prefix(path)):
            if posixpath.basename(key) == _DIR_MARKER:
                continue  # hide the empty-dir markers
            out.append(self._strip_prefix(key))
        return sorted(out)

    def _cloud_walk(
        self, target: Any, rest: str, path: str, *, limit: Optional[int] = None
    ) -> BoundedListing:
        """``(relative path, size)`` of the files under a cloud path, breadth
        first through one-level listings, at most ``limit`` of them."""
        cap = self._cloud_walk_entries if limit is None else limit
        out = BoundedListing()
        out.limit = cap
        if target is _CLOUD_DIR:
            if rest != "":
                return out
            roots = [(remote, "") for remote in self._remotes.values()]
        else:
            remote: CloudRemote = target
            kind = self._cloud_kind(remote, rest)
            if kind == "absent":
                return out
            if kind == "file":
                info = remote.store.stat(rest)
                out.append((self._normalize_rel(path), info.size if info else 0))
                out.scanned = 1
                return out
            roots = [(remote, rest)]
        for remote, start in roots:
            queue: list[tuple[str, int]] = [(remote.key_prefix(start), 0)]
            while queue:
                prefix, depth = queue.pop(0)
                for entry in remote.store.list_dir(prefix):
                    if entry.is_dir:
                        if depth + 1 <= CLOUD_WALK_MAX_DEPTH:
                            queue.append((entry.key + "/", depth + 1))
                        continue
                    if len(out) >= cap:
                        out.truncated = True
                        return out
                    rel = f"{remote.path}/{entry.key}" if entry.key else remote.path
                    out.append((rel, entry.size))
                    out.scanned += 1
        return out

    def list_files_with_sizes(self, path: str = "") -> list[tuple[str, int]]:
        """``walk()`` that keeps each file's size — still ONE store round trip.

        Consumed by the cloud-sync layer (duck-typed capability probe): every
        store op here is an rclone subprocess spawn, so a sync pass that
        walks directories and then stats files one by one pays ~30 spawns
        per turn; this primitive collapses the local half of that to one.
        """
        target, rest = self._route(path)
        if target is not None:
            return self._cloud_walk(target, rest, path)
        out: list[tuple[str, int]] = []
        for key, size in self._list_keys(self._dir_prefix(path)):
            if posixpath.basename(key) == _DIR_MARKER:
                continue  # hide the empty-dir markers
            out.append((self._strip_prefix(key), size))
        return sorted(out)

    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> list[dict]:
        target, rest = self._route(path)
        if target is not None or (self._remotes and rest == ""):
            return self._cloud_search(
                target, rest, path, query, case_sensitive, exclude_dirs
            )
        # Bounded literal content search (hydration guard): binary extensions
        # skipped, per-file byte cap. Name/path search is implicit via list_dir.
        if self.is_file(path):
            keys = [self._object_key(path)]
        else:
            keys = [info.key for info in self._store.list(self._dir_prefix(path))]
        root = self._normalize_rel(path)

        needle = query if case_sensitive else query.lower()
        results: list[dict] = []
        for key in keys:
            name = posixpath.basename(key)
            if name == _DIR_MARKER:
                continue
            ext = posixpath.splitext(name)[1].lower()
            if ext in _BINARY_SEARCH_EXTS:
                continue
            rel = self._strip_prefix(key)
            # Keys were listed under root + "/"; a single-file root has nothing below.
            below = rel[len(root) + 1 :] if root and rel.startswith(root + "/") else ""
            if search_excludes(below if root else rel, exclude_dirs):
                continue
            size = self._store.head(key)
            if size is None or size > self._search_file_bytes:
                continue
            try:
                content = self._store.get(key).decode("utf-8")
            except (UnicodeDecodeError, FileNotFoundError):
                continue
            for i, line in enumerate(content.splitlines(), 1):
                hay = line if case_sensitive else line.lower()
                if needle in hay:
                    results.append(
                        {"path": rel, "line_number": i, "line": line.strip()}
                    )
        return results

    def _cloud_search(
        self,
        target: Any,
        rest: str,
        path: str,
        query: str,
        case_sensitive: bool,
        exclude_dirs: list[str] | None,
    ) -> list[dict]:
        if target is None or (target is _CLOUD_DIR and rest == ""):
            # A root-wide search would span every cloud folder (ruling 6):
            # the mount guard refuses the same on a Container.
            raise CloudFolderError(
                "search_files needs a path in this session: name a workspace "
                "folder or file (for example 'notes'), or one cloud folder such "
                f"as {next(iter(self._remotes.values())).path}; a search of the "
                "whole workspace would span the cloud folders "
                f"({self._cloud_paths_text()}), which are not scanned as a whole."
            )
        results = BoundedListing()
        if target is _CLOUD_DIR:
            return results  # an unknown name under cloud/: nothing to search
        remote: CloudRemote = target
        listing = self._cloud_walk(remote, rest, path, limit=self._cloud_search_files)
        results.truncated, results.scanned, results.limit = (
            listing.truncated,
            listing.scanned,
            listing.limit,
        )
        root = self._normalize_rel(path)
        needle = query if case_sensitive else query.lower()
        for rel, size in listing:
            name = posixpath.basename(rel)
            if posixpath.splitext(name)[1].lower() in _BINARY_SEARCH_EXTS:
                continue
            below = rel[len(root) + 1 :] if rel.startswith(root + "/") else ""
            if search_excludes(below, exclude_dirs):
                continue
            if size > self._search_file_bytes:
                continue
            key = rel[len(remote.path) + 1 :]
            try:
                content = remote.store.get(key).decode("utf-8")
            except (UnicodeDecodeError, FileNotFoundError):
                continue
            for i, line in enumerate(content.splitlines(), 1):
                hay = line if case_sensitive else line.lower()
                if needle in hay:
                    results.append(
                        {"path": rel, "line_number": i, "line": line.strip()}
                    )
        return results

    def mkdir(self, path: str) -> None:
        target, rest = self._route(path)
        if target is not None:
            return self._cloud_mkdir(target, rest, path)
        norm = rest
        if norm == "":
            return  # root always exists; nothing to create
        # Flat store has no real directories: drop a hidden marker so the
        # (possibly empty) directory is visible. Intermediate parents are
        # emergent from this key's prefix, so a single marker suffices.
        marker = self._prefix + norm + "/" + _DIR_MARKER
        # mkdir is contractually idempotent, and workspace scaffolding calls
        # it for the same directories on every attach. When the scoped index
        # is open we can tell for FREE that the directory is already there
        # and skip the write — the measured cost of those re-writes was 9
        # store spawns per attach. Without the index, probing first would
        # cost more than the write, so keep the unconditional put.
        if self._index is not None and self._indexed_has_prefix(
            self._prefix + norm + "/"
        ):
            return
        self._store.put(marker, b"")
        self._index_put(marker, 0)

    def _cloud_mkdir(self, target: Any, rest: str, path: str) -> None:
        if target is _CLOUD_DIR:
            if rest == "":
                return  # ``cloud`` always exists while folders are bound
            raise self._no_such_folder(path, rest)
        remote: CloudRemote = target
        if rest == "":
            return  # the folder's root exists
        self._refuse_read_only(remote, "create a directory at", path)
        kind = self._cloud_kind(remote, rest)
        if kind == "file":
            raise ValueError(f"Not a directory: {path}")
        if kind == "dir":
            return
        # A real folder on the provider; never a marker object in a user's cloud.
        remote.store.mkdir(remote.key_prefix(rest))

    def delete_file(self, path: str) -> bool:
        target, rest = self._route(path)
        if target is not None:
            return self._cloud_delete_file(target, rest, path)
        if self.is_file(path):
            self._store.delete(self._object_key(path))
            self._index_drop(self._object_key(path))
            return True
        if self.is_dir(path):
            # Only an empty directory (just its .keep marker) may go via
            # delete_file; a populated one is a ValueError, as on a filesystem.
            if self.list_dir(path):
                raise ValueError(f"Cannot delete non-empty directory: {path}")
            self._store.delete(self._dir_prefix(path) + _DIR_MARKER)
            self._index_drop(self._dir_prefix(path) + _DIR_MARKER)
            return True
        return False

    def _cloud_delete_file(self, target: Any, rest: str, path: str) -> bool:
        if target is _CLOUD_DIR:
            if rest == "":
                raise CloudFolderError(f"Cannot delete '{CLOUD_ROOT}' itself")
            return False
        remote: CloudRemote = target
        if rest == "":
            raise CloudFolderError(
                f"Cannot delete the cloud folder {remote.path} itself"
            )
        kind = self._cloud_kind(remote, rest)
        if kind == "absent":
            return False
        self._refuse_read_only(remote, "delete", path)
        if kind == "file":
            before = remote.store.stat(rest)
            remote.store.delete(rest)
            self._cloud_versions.pop((remote.name, rest), None)
            self._journal(remote, path, "delete", before, None, 0)
            return True
        if remote.store.list_dir(remote.key_prefix(rest)):
            raise ValueError(f"Cannot delete non-empty directory: {path}")
        remote.store.rmdir(remote.key_prefix(rest))
        return True

    def delete_directory(self, path: str) -> bool:
        target, rest = self._route(path)
        if target is not None:
            return self._cloud_delete_directory(target, rest, path)
        norm = rest
        if norm == "":
            raise ValueError("Cannot delete workspace root directory")
        if not self.is_dir(path):
            if self.is_file(path):
                raise ValueError(f"Not a directory: {path}")
            return False
        deleted = False
        for key, _size in self._list_keys(self._dir_prefix(path)):
            self._store.delete(key)
            self._index_drop(key)
            deleted = True
        return deleted

    def _cloud_delete_directory(self, target: Any, rest: str, path: str) -> bool:
        if target is _CLOUD_DIR:
            if rest == "":
                raise CloudFolderError(f"Cannot delete '{CLOUD_ROOT}' itself")
            return False
        remote: CloudRemote = target
        if rest == "":
            raise CloudFolderError(
                f"Cannot delete the cloud folder {remote.path} itself; delete "
                f"what is inside it instead"
            )
        kind = self._cloud_kind(remote, rest)
        if kind == "absent":
            return False
        if kind == "file":
            raise ValueError(f"Not a directory: {path}")
        self._refuse_read_only(remote, "delete", path)
        count = remote.store.delete_tree(remote.key_prefix(rest))
        for key in [k for k in self._cloud_versions if k[0] == remote.name]:
            if key[1].startswith(rest + "/"):
                self._cloud_versions.pop(key, None)
        self._journal(remote, path, "delete_tree", None, None, count)
        return True

    def move(self, src: str, dst: str) -> None:
        s_target, s_rest = self._route(src)
        d_target, d_rest = self._route(dst)
        if s_target is not None or d_target is not None:
            return self._cloud_transfer(
                s_target, s_rest, src, d_target, d_rest, dst, delete_source=True
            )
        if not self.exists(src):
            raise FileNotFoundError(f"Source not found: {src}")
        if self.is_file(src):
            src_key, dst_key = self._object_key(src), self._object_key(dst)
            self._store.copy(src_key, dst_key)
            self._store.delete(src_key)
            size = self._indexed_size(src_key)
            self._index_drop(src_key)
            if size is not None:
                self._index_put(dst_key, size)
            return
        # Directory move: relocate the whole subtree key-by-key.
        src_prefix = self._dir_prefix(src)
        dst_prefix = self._dir_prefix(dst)
        for key, size in self._list_keys(src_prefix):
            new_key = dst_prefix + key[len(src_prefix) :]
            self._store.copy(key, new_key)
            self._store.delete(key)
            self._index_drop(key)
            self._index_put(new_key, size)

    def copy(self, src: str, dst: str) -> None:
        s_target, s_rest = self._route(src)
        d_target, d_rest = self._route(dst)
        if s_target is not None or d_target is not None:
            return self._cloud_transfer(
                s_target, s_rest, src, d_target, d_rest, dst, delete_source=False
            )
        if not self.exists(src):
            raise FileNotFoundError(f"Source not found: {src}")
        if self.is_dir(src) and not self.is_file(src):
            raise ValueError(f"Cannot copy directory: {src}. Use move for directories.")
        src_key, dst_key = self._object_key(src), self._object_key(dst)
        self._store.copy(src_key, dst_key)
        size = self._indexed_size(src_key)
        if size is not None:
            self._index_put(dst_key, size)

    def _cloud_transfer(
        self,
        s_target: Any,
        s_rest: str,
        src: str,
        d_target: Any,
        d_rest: str,
        dst: str,
        *,
        delete_source: bool,
    ) -> None:
        """``move``/``copy`` when at least one side is a cloud path. Within one
        store the store copies (server-side where it can); across stores the
        bytes go through this process, one file at a time; a directory across
        stores is refused, naming both folders."""
        verb = "move" if delete_source else "copy"
        for target, rest, shown in ((s_target, s_rest, src), (d_target, d_rest, dst)):
            if target is _CLOUD_DIR:
                if rest == "":
                    raise CloudFolderError(f"Cannot {verb} '{CLOUD_ROOT}' itself")
                raise self._no_such_folder(shown, rest)
            if isinstance(target, CloudRemote) and rest == "":
                raise CloudFolderError(
                    f"Cannot {verb} the cloud folder {target.path} itself"
                )
        if isinstance(d_target, CloudRemote):
            self._refuse_read_only(d_target, f"{verb} into", dst)
        if delete_source and isinstance(s_target, CloudRemote):
            self._refuse_read_only(s_target, "move out of", src)
        if not self.exists(src):
            raise FileNotFoundError(f"Source not found: {src}")
        src_is_file = self.is_file(src)
        if s_target is d_target:
            remote: CloudRemote = s_target
            if src_is_file:
                before_dst = remote.store.stat(d_rest)
                remote.store.copy(s_rest, d_rest)
                after = remote.store.stat(d_rest)
                self._journal(
                    remote, dst, verb, before_dst, after, after.size if after else 0
                )
                if delete_source:
                    remote.store.delete(s_rest)
                    self._cloud_versions.pop((remote.name, s_rest), None)
                return
            if not delete_source:
                raise ValueError(
                    f"Cannot copy directory: {src}. Use move for directories."
                )
            listing = self._cloud_walk(remote, s_rest, src)
            if listing.truncated:
                raise CloudFolderError(
                    f"'{src}' holds more than {listing.limit} files; move a "
                    f"smaller folder or the files themselves"
                )
            for rel, _size in listing:
                key = rel[len(remote.path) + 1 :]
                new_key = d_rest + key[len(s_rest) :]
                remote.store.copy(key, new_key)
                remote.store.delete(key)
                self._cloud_versions.pop((remote.name, key), None)
            remote.store.rmdir(remote.key_prefix(s_rest))
            self._journal(remote, dst, "move_tree", None, None, len(listing))
            return
        if not src_is_file:
            raise CloudFolderError(
                f"Cannot {verb} the directory '{src}' to '{dst}': they are in "
                f"different places ({self._place(s_target)} and "
                f"{self._place(d_target)}); {verb} the files one by one"
            )
        data = self.read_file(src, binary=True)
        self.write_file(dst, data)
        if delete_source:
            self.delete_file(src)

    @staticmethod
    def _place(target: Any) -> str:
        return target.path if isinstance(target, CloudRemote) else "the workspace"

    def stat(self, path: str) -> int:
        target, rest = self._route(path)
        if target is not None:
            if target is _CLOUD_DIR:
                listing = self._cloud_walk(target, rest, path)
            else:
                info = target.store.stat(rest) if rest else None
                if info is not None and not info.is_dir:
                    return info.size
                listing = self._cloud_walk(target, rest, path)
            return sum(size for _rel, size in listing)
        key = self._object_key(path)
        if self._index is not None:
            size = self._index.get(key)
            if size is not None:
                return size
        else:
            size = self._store.head(key)
            if size is not None:
                return size
        total = 0
        found = False
        for _key, size in self._list_keys(self._dir_prefix(path)):
            total += size
            found = True
        return total if found else 0

    def resolve_path(self, relative_path: str) -> str:
        # Canonical form for a virtual workspace is the full object key; for
        # a cloud path it is the path itself, the name the user knows.
        target, rest = self._route(relative_path)
        if target is not None:
            return self._normalize_rel(relative_path)
        return self._object_key(relative_path)

    # =========================================================================
    # Cloud folder state
    # =========================================================================

    def cloud_folders(self) -> list[dict[str, Any]]:
        """One row per bound cloud folder, the status record's shape:
        ``name``, ``path``, ``kind``, ``access``, ``mount_id``, ``shared``,
        ``state`` (``bound``/``ready``/``failed``), ``reason`` and
        ``delivery: "remote"``."""
        return [
            {
                "name": remote.name,
                "path": remote.path,
                "kind": remote.kind,
                "access": remote.access,
                "mount_id": remote.mount_id,
                "shared": remote.shared,
                "state": remote.state,
                "reason": remote.reason,
                "delivery": "remote",
            }
            for remote in self._remotes.values()
        ]

    def probe_cloud_folders(self) -> list[dict[str, Any]]:
        """One bounded listing of each folder's root: ``ready`` when it
        answers, ``failed`` with a closed reason when it does not. Never
        raises; meant to run off the turn (the session reports the rows)."""
        for remote in self._remotes.values():
            try:
                remote.store.list_dir("")
            except Exception as exc:  # the provider's error is logged, never shown
                remote.state = CLOUD_STATE_FAILED
                remote.reason = "mount_failed"
                remote.detail = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "cloud folder %s did not answer: %s", remote.path, remote.detail
                )
            else:
                remote.state = CLOUD_STATE_READY
                remote.reason = None
                remote.detail = None
        return self.cloud_folders()

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def with_max_read_bytes(self, max_read_bytes: int) -> "VirtualWorkspaceBackend":
        """A reader over the same store and prefix with another read ceiling.

        Shares the (already connected) store: never ``disconnect`` it, that
        would close the live workspace's transport. The workspace upgrade's
        seed reads through one with the upload ceiling (C2).
        """
        return VirtualWorkspaceBackend(
            self._store,
            self._prefix,
            max_read_bytes=max_read_bytes,
            search_file_bytes=self._search_file_bytes,
            cloud_remotes=self._remotes.values(),
            cloud_search_files=self._cloud_search_files,
            cloud_walk_entries=self._cloud_walk_entries,
        )

    def connect(self) -> None:
        self._store.connect()
        for remote in self._remotes.values():
            try:
                remote.store.connect()
            except Exception as exc:
                remote.state = CLOUD_STATE_FAILED
                remote.reason = "mount_failed"
                remote.detail = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "cloud folder %s could not be bound: %s", remote.path, remote.detail
                )
        self._connected = True

    def disconnect(self) -> None:
        try:
            self._store.close()
            for remote in self._remotes.values():
                try:
                    remote.store.close()
                except Exception:
                    logger.debug("cloud folder %s did not close", remote.path)
        finally:
            self._connected = False

    def is_connected(self) -> bool:
        return self._connected
