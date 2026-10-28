"""Object-store seam for the ``virtual`` workspace backend.

``VirtualWorkspaceBackend`` (the no-workspace ``virtual`` tier — see
``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §5) translates the
``WorkspaceBackend`` file contract into flat object-store key operations.
This module defines that narrow seam so the backend's logic is
transport-agnostic:

- unit tests run the backend over :class:`InMemoryObjectStore`;
- production runs it over ``RcloneObjectStore`` (``rclone.py``), chosen for
  provider continuity with ``RcloneMountSpec`` (§5).

Keys are flat, posix-style strings (e.g. ``jobs/<id>/documents/x.md``). There
is **no** directory concept in the required surface — "directories" are
emergent from shared key prefixes, exactly as in S3. All path/prefix math
lives in the backend; the store only moves bytes by key. This is deliberately
the smallest surface that ``VirtualWorkspaceBackend`` needs, so swapping
rclone for boto3 later (the v1.1 revisit in §10) is a single new
``ObjectStore`` subclass.

Three optional operations sit on top of that surface for remotes that are a
user's cloud folder rather than SRW's own S3 prefix
(``features/cloud_folders_on_virtual_sessions.md`` § Agent):
:meth:`ObjectStore.stat` (one object's size, kind and version token),
:meth:`ObjectStore.list_dir` (one level of a folder in one call) and
:meth:`ObjectStore.put_if_match` (a write that refuses when the object changed
since it was read). Each has a default built from the required surface, so an
S3 prefix or an in-memory store needs nothing new; a transport that answers
them natively, or cheaper, overrides.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class ObjectInfo:
    """One object in the store.

    Attributes:
        key: The full, prefix-inclusive object key (not relative to any
            workspace prefix — the backend strips its own prefix).
        size: Object size in bytes.
    """

    key: str
    size: int


@dataclass(frozen=True)
class ObjectStat(ObjectInfo):
    """One entry as :meth:`ObjectStore.stat` and :meth:`ObjectStore.list_dir`
    report it.

    Attributes:
        is_dir: The entry is a folder of the remote (size 0, no version).
        version: An opaque token that changes whenever the object's content
            changes, or ``None`` when the transport has none. What it is
            made of is the transport's business (an ETag, a modification
            time with the size, a counter); callers only compare it.
        mtime: The modification time as the transport reported it (rclone:
            an RFC 3339 string), or ``None``.
    """

    is_dir: bool = False
    version: Optional[str] = None
    mtime: Optional[str] = None


class ObjectStoreError(Exception):
    """Transport-level failure talking to the object store.

    Distinct from ``FileNotFoundError`` (a missing object, which is a normal
    control-flow signal): this means the store itself could not be reached or
    returned an unexpected error.
    """


class ObjectVersionMismatch(Exception):
    """A conditional write found the object at another version than expected.

    Not an :class:`ObjectStoreError`: the store worked and refused on purpose.
    ``expected`` is the version the caller read; ``actual`` is what the store
    found (``None`` when the object is gone).
    """

    def __init__(self, key: str, expected: Optional[str], actual: Optional[str]):
        super().__init__(f"{key} is at version {actual!r}, expected {expected!r}")
        self.key = key
        self.expected = expected
        self.actual = actual


class ObjectStore(ABC):
    """Narrow flat key/value blob store the virtual backend builds file ops on.

    Implementations move opaque bytes addressed by a flat string key. They do
    not interpret keys, enforce boundaries, or model directories — that is the
    backend's job. ``get``/``head`` use ``FileNotFoundError`` to signal a
    missing object; genuine transport failures raise :class:`ObjectStoreError`.
    """

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Return the object's bytes.

        Raises:
            FileNotFoundError: If no object exists at ``key``.
            ObjectStoreError: On transport failure.
        """
        ...

    @abstractmethod
    def put(self, key: str, data: bytes) -> None:
        """Create or overwrite the object at ``key`` with ``data`` (bytes)."""
        ...

    @abstractmethod
    def head(self, key: str) -> Optional[int]:
        """Return the object's size in bytes, or ``None`` if it doesn't exist.

        Never raises ``FileNotFoundError`` — absence is reported as ``None`` so
        callers can probe existence in one call.
        """
        ...

    @abstractmethod
    def list(self, prefix: str) -> List[ObjectInfo]:
        """Return all objects whose key starts with ``prefix`` (recursive).

        An empty ``prefix`` lists the whole store. Order is not guaranteed by
        the contract, but implementations sort by key for determinism.
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete the object at ``key``.

        Returns:
            True if an object existed and was deleted, False if it was absent.
            Idempotent: deleting a missing key is not an error.
        """
        ...

    # --- optional, overridable for server-side efficiency ---

    def copy(self, src: str, dst: str) -> None:
        """Copy one object from ``src`` to ``dst``.

        Default implementation is a client-side read+write; transports with a
        server-side copy (S3 ``CopyObject``, ``rclone copyto``) should override.

        Raises:
            FileNotFoundError: If ``src`` does not exist.
        """
        self.put(dst, self.get(src))

    def stat(self, key: str) -> Optional[ObjectStat]:
        """Return one object's :class:`ObjectStat`, or ``None`` when absent.

        The default knows files only (through :meth:`head`) and reports no
        version; a transport that models folders or has a version token
        overrides. Never raises ``FileNotFoundError``.
        """
        size = self.head(key)
        if size is None:
            return None
        return ObjectStat(key=key, size=size)

    def list_dir(self, prefix: str) -> List[ObjectStat]:
        """Return the entries directly under ``prefix`` (``""`` or a key
        prefix ending in ``/``): each file as it is, and one ``is_dir``
        entry per distinct child prefix, sorted by key.

        The default folds the recursive :meth:`list` to one level, which
        costs the whole subtree; a transport with a one-level listing (S3
        delimiters, WebDAV ``PROPFIND`` depth 1) overrides.
        """
        files: List[ObjectStat] = []
        dirs: dict[str, ObjectStat] = {}
        for info in self.list(prefix):
            rest = info.key[len(prefix) :]
            head, sep, _ = rest.partition("/")
            if not sep:
                files.append(ObjectStat(key=info.key, size=info.size))
            elif head and head not in dirs:
                dirs[head] = ObjectStat(key=prefix + head, size=0, is_dir=True)
        return sorted([*files, *dirs.values()], key=lambda entry: entry.key)

    def put_if_match(
        self, key: str, data: bytes, expected_version: Optional[str]
    ) -> ObjectStat:
        """Write ``data`` to ``key`` only while the object is unchanged since
        the read that produced ``expected_version``, and return the object's
        :class:`ObjectStat` after the write.

        ``expected_version`` is the ``version`` a :meth:`stat` or
        :meth:`list_dir` of this store returned, ``None`` when it had none.
        An object that is gone is a mismatch whenever the caller holds a
        version; a store without version tokens can only check existence.

        The default checks with :meth:`stat` and then writes, which leaves a
        window between the two; a transport with server-side preconditions
        (``If-Match``) overrides and closes it.

        Raises:
            ObjectVersionMismatch: The object changed or disappeared.
            ObjectStoreError: On transport failure.
        """
        current = self.stat(key)
        if current is None:
            if expected_version is not None:
                raise ObjectVersionMismatch(key, expected_version, None)
        elif current.version is not None and current.version != expected_version:
            raise ObjectVersionMismatch(key, expected_version, current.version)
        self.put(key, data)
        after = self.stat(key)
        if after is None:
            raise ObjectStoreError(f"{key} was not found right after it was written")
        return after

    # --- lifecycle (default no-op) ---

    def connect(self) -> None:
        """Establish or verify the transport. Default no-op."""

    def close(self) -> None:
        """Release transport resources. Default no-op."""


_MISSING = object()


class InMemoryObjectStore(ObjectStore):
    """Process-local, dict-backed :class:`ObjectStore`.

    Used by the backend's contract tests and viable for lightweight dev, but
    **not a production transport**: it is per-process and non-durable, which
    defeats the "workspace survives pod death" property the ``virtual`` tier is
    built for (§5). Production uses ``RcloneObjectStore``.

    Every write bumps the object's version counter, so the seam's conditional
    write can be exercised without a remote.
    """

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._versions: dict[str, int] = {}

    def get(self, key: str) -> bytes:
        try:
            return self._data[key]
        except KeyError:
            raise FileNotFoundError(key) from None

    def put(self, key: str, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(
                f"ObjectStore.put requires bytes, got {type(data).__name__}"
            )
        self._data[key] = bytes(data)
        self._versions[key] = self._versions.get(key, 0) + 1

    def head(self, key: str) -> Optional[int]:
        value = self._data.get(key, _MISSING)
        return None if value is _MISSING else len(value)

    def stat(self, key: str) -> Optional[ObjectStat]:
        value = self._data.get(key, _MISSING)
        if value is _MISSING:
            return None
        return ObjectStat(key=key, size=len(value), version=str(self._versions[key]))

    def list(self, prefix: str) -> List[ObjectInfo]:
        return sorted(
            (
                ObjectInfo(key=k, size=len(v))
                for k, v in self._data.items()
                if k.startswith(prefix)
            ),
            key=lambda info: info.key,
        )

    def delete(self, key: str) -> bool:
        self._versions.pop(key, None)
        return self._data.pop(key, _MISSING) is not _MISSING
