"""VirtualOverlayBackend — agent-visible directories served from live state.

Wraps a real ``WorkspaceBackend`` and answers file operations for registered
virtual prefixes (``tools/``, ``contacts/``, ``instructions.md``) from
providers instead of the workspace filesystem. Everything else delegates.

Duck-typed proxy (not a ``WorkspaceBackend`` subclass), mirroring
``SubdirBackend``: anything not overridden passes through via ``__getattr__``.
No isinstance checks target backend types in the codebase, so this is safe.

See knowledge-base/knowledge/features/virtual_directories.md.
"""

import fnmatch
import logging
import posixpath
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, Tuple

from shared.runtime.core.workspace_backend import SEARCH_RESULT_HARD_CAP

logger = logging.getLogger(__name__)


class VirtualPathError(ValueError):
    """An operation is not permitted on a virtual path.

    Subclasses ``ValueError`` so the file tools' existing path-error handling
    surfaces it to the agent as a readable message.
    """


@dataclass(frozen=True)
class EntryMeta:
    """Metadata for one virtual entry. ``mtime`` is unused in Slice 1."""

    size: int
    mtime: Optional[float] = None


class VirtualDirProvider(Protocol):
    """Serves one virtual prefix. Flat — no subdirectories."""

    prefix: str
    is_dir: bool
    writable: bool

    def entries(self) -> Dict[str, EntryMeta]: ...

    def read(self, name: str) -> Optional[str]: ...

    def write(self, name: str, content: str) -> None: ...

    # Optional. Return every entry's content in ONE pass. Providers that render
    # the whole set per read (ToolsProvider, ContactsProvider) should implement
    # it; search is O(N^2) without it. Omitting it is safe — the overlay falls
    # back to entries() + read().
    def read_all(self) -> Dict[str, str]: ...


class VirtualOverlayBackend:
    """Routes registered prefixes to providers; delegates everything else."""

    def __init__(self, inner: Any):
        self._inner = inner
        self._providers: Dict[str, Any] = {}

    # --- registry ---------------------------------------------------------

    @property
    def inner(self) -> Any:
        """The unwrapped backend. Use for probes that must bypass virtual paths."""
        return self._inner

    @property
    def providers(self) -> Dict[str, Any]:
        return dict(self._providers)

    def register(self, provider: Any) -> None:
        self._providers[provider.prefix] = provider
        logger.debug("Registered virtual provider: %s", provider.prefix)

    def rebind(self, inner: Any) -> None:
        """Point the overlay at a new real backend, keeping the providers.

        A workspace-tier upgrade (virtual -> sandbox, container -> VM) replaces
        the real backend mid-run. Rebinding — rather than building a fresh
        overlay — preserves every already-registered provider, so the virtual
        paths keep serving across the swap instead of 404ing until (and unless)
        something re-registers them.
        """
        if inner is None:
            raise ValueError("VirtualOverlayBackend.rebind requires a backend")
        if inner is self:
            raise ValueError("VirtualOverlayBackend cannot wrap itself")
        self._inner = inner
        logger.debug("Virtual overlay rebound to %s", type(inner).__name__)

    # --- routing ----------------------------------------------------------

    @staticmethod
    def _normalize(path: str) -> str:
        p = (path or "").strip()
        while p.startswith("./"):
            p = p[2:]
        p = p.strip("/")
        if not p:
            return ""
        return posixpath.normpath(p)

    def _match(self, path: str) -> Optional[Tuple[Any, str]]:
        """Return ``(provider, entry_name)`` when *path* is virtual.

        ``entry_name`` is ``""`` for the directory itself. For a single-file
        provider the entry name is the prefix.
        """
        p = self._normalize(path)
        if not p:
            return None
        head = p.split("/", 1)[0]
        provider = self._providers.get(head)
        if provider is None:
            return None
        if not provider.is_dir:
            return (provider, p) if p == provider.prefix else (provider, "")
        return provider, p[len(head) :].lstrip("/")

    # --- provider calls, never allowed to crash the agent loop ------------

    def _entries(self, provider: Any) -> Dict[str, EntryMeta]:
        try:
            return provider.entries()
        except Exception as e:  # degrade to empty listing, never explode
            logger.warning("virtual entries() failed for %s: %s", provider.prefix, e)
            return {}

    def _read(self, provider: Any, name: str, path: str) -> Optional[str]:
        try:
            return provider.read(name)
        except Exception as e:
            logger.warning("virtual read failed for %s: %s", path, e)
            raise VirtualPathError(f"{path} is temporarily unavailable: {e}") from e

    def _write(self, provider: Any, name: str, content: str, path: str) -> None:
        """Mirror of ``_read``: a provider write must never crash the loop."""
        try:
            provider.write(name, content)
        except Exception as e:
            logger.warning("virtual write failed for %s: %s", path, e)
            raise VirtualPathError(f"{path} could not be written: {e}") from e

    def _read_all(self, provider: Any) -> Dict[str, str]:
        """Every entry's content, in ONE pass when the provider offers one.

        ``entries()`` + ``read()``-per-name is quadratic for providers that
        render the whole document set per call: ``ToolsProvider.read`` re-renders
        all ~40 tool docs to return one of them, so a root ``search_files``
        costs ~N^2 ``generate_tool_description`` invocations on the agent's
        request path. ``read_all()`` collapses that to a single render.

        Called fresh per overlay operation — nothing is memoized across calls,
        so content still reflects the current tool/contact list every time.
        """
        read_all = getattr(provider, "read_all", None)
        if callable(read_all):
            try:
                return dict(read_all())
            except Exception as e:
                logger.warning(
                    "virtual read_all() failed for %s: %s", provider.prefix, e
                )
                return {}
        docs: Dict[str, str] = {}
        for name in self._entries(provider):
            try:
                docs[name] = provider.read(name) or ""
            except Exception as e:
                logger.warning(
                    "virtual search skipped %s/%s: %s", provider.prefix, name, e
                )
        return docs

    # --- read path --------------------------------------------------------

    def read_file(self, path: str, binary: bool = False) -> Any:
        m = self._match(path)
        if m is None:
            return self._inner.read_file(path, binary=binary)
        provider, name = m
        content = self._read(provider, name, path) if name else None
        if content is None:
            raise FileNotFoundError(f"File not found: {path}")
        return content.encode("utf-8") if binary else content

    def exists(self, path: str) -> bool:
        m = self._match(path)
        if m is None:
            return self._inner.exists(path)
        provider, name = m
        if not name:
            return bool(provider.is_dir)
        return name in self._entries(provider)

    def is_file(self, path: str) -> bool:
        m = self._match(path)
        if m is None:
            return self._inner.is_file(path)
        provider, name = m
        return bool(name) and name in self._entries(provider)

    def is_dir(self, path: str) -> bool:
        m = self._match(path)
        if m is None:
            return self._inner.is_dir(path)
        provider, name = m
        return bool(provider.is_dir) and not name

    def stat(self, path: str) -> int:
        m = self._match(path)
        if m is None:
            return self._inner.stat(path)
        provider, name = m
        entries = self._entries(provider)
        if not name:
            return sum(meta.size for meta in entries.values())
        meta = entries.get(name)
        # Contract (workspace_backend.py:448): 0 for a missing path, never an
        # exception. Both real backends honour it; virtual paths must too.
        return meta.size if meta is not None else 0

    def list_dir(self, path: str = "", pattern: str = "*") -> list:
        m = self._match(path)
        if m is not None:
            provider, name = m
            if name or not provider.is_dir:
                return []  # flat: nothing lives below a virtual entry
            return [
                f"{provider.prefix}/{n}"
                for n in sorted(self._entries(provider))
                if fnmatch.fnmatch(n, pattern)
            ]

        results = list(self._inner.list_dir(path, pattern))
        if self._normalize(path):
            return results  # only the workspace root gains virtual entries

        seen = {r.rstrip("/") for r in results}
        for provider in self._providers.values():
            if provider.prefix in seen:
                continue
            if fnmatch.fnmatch(provider.prefix, pattern):
                results.append(
                    f"{provider.prefix}/" if provider.is_dir else provider.prefix
                )
        return results

    def _search_provider(self, provider: Any, query: str, case_sensitive: bool) -> list:
        needle = query if case_sensitive else query.lower()
        hits = []
        docs = self._read_all(provider)
        for name in sorted(docs):
            content = docs[name] or ""
            rel = f"{provider.prefix}/{name}" if provider.is_dir else name
            for lineno, line in enumerate(content.splitlines(), 1):
                haystack = line if case_sensitive else line.lower()
                if needle in haystack:
                    hits.append({"path": rel, "line_number": lineno, "line": line})
        return hits

    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: Optional[list] = None,
    ) -> list:
        m = self._match(path)
        if m is not None:
            provider, name = m
            hits = self._search_provider(provider, query, case_sensitive)
            if name:
                # A path naming one entry scopes the search to that entry —
                # discarding `name` here would report hits under sibling files
                # the caller never named.
                target = f"{provider.prefix}/{name}" if provider.is_dir else name
                hits = [hit for hit in hits if hit["path"] == target]
            return hits[:SEARCH_RESULT_HARD_CAP]

        results = list(
            self._inner.search_files(query, path, case_sensitive, exclude_dirs)
        )
        if not self._normalize(path):
            for provider in self._providers.values():
                # exclude_dirs are directory-name globs (the tool contract, and
                # search_excludes for real dirs); decided before any read.
                if (
                    exclude_dirs
                    and provider.is_dir
                    and any(
                        fnmatch.fnmatchcase(part, glob)
                        for part in provider.prefix.split("/")
                        for glob in exclude_dirs
                    )
                ):
                    continue
                results.extend(self._search_provider(provider, query, case_sensitive))
        return results[:SEARCH_RESULT_HARD_CAP]

    # --- mutation path (write operations) ---------------------------------

    _DENY_TEMPLATE = (
        "{path} is inside the virtual directory '{prefix}', which is generated "
        "from live state and cannot be {verb}. Copy a file out (copy "
        "'{path}' to a normal workspace path) if you want an editable version."
    )

    def _deny(self, path: str, provider: Any, verb: str) -> None:
        raise VirtualPathError(
            self._DENY_TEMPLATE.format(path=path, prefix=provider.prefix, verb=verb)
        )

    def write_file(self, path: str, content: Any) -> None:
        m = self._match(path)
        if m is None:
            return self._inner.write_file(path, content)
        provider, name = m
        if not provider.writable or not name:
            self._deny(path, provider, "written to")
        self._write(provider, name, content, path)

    def append_file(self, path: str, content: str) -> None:
        m = self._match(path)
        if m is None:
            return self._inner.append_file(path, content)
        provider, name = m
        if not provider.writable or not name:
            self._deny(path, provider, "appended to")
        existing = self._read(provider, name, path) or ""
        self._write(provider, name, existing + content, path)

    def mkdir(self, path: str) -> None:
        m = self._match(path)
        if m is None:
            return self._inner.mkdir(path)
        self._deny(path, m[0], "created")

    def delete_file(self, path: str) -> bool:
        m = self._match(path)
        if m is None:
            return self._inner.delete_file(path)
        self._deny(path, m[0], "deleted")

    def delete_directory(self, path: str) -> bool:
        m = self._match(path)
        if m is None:
            return self._inner.delete_directory(path)
        self._deny(path, m[0], "deleted")

    def move(self, src: str, dst: str) -> None:
        for candidate in (src, dst):
            m = self._match(candidate)
            if m is not None:
                self._deny(candidate, m[0], "moved")
        return self._inner.move(src, dst)

    def copy(self, src: str, dst: str) -> None:
        dst_match = self._match(dst)
        if dst_match is not None:
            self._deny(dst, dst_match[0], "copied onto")
        src_match = self._match(src)
        if src_match is None:
            return self._inner.copy(src, dst)
        # Copy-out: the escape hatch the denial message advertises.
        provider, name = src_match
        content = self._read(provider, name, src) if name else None
        if content is None:
            raise FileNotFoundError(f"File not found: {src}")
        self._inner.write_file(dst, content)

    # --- delegation -------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        # Private names are never delegated: this also stops __getattr__ from
        # recursing on self._inner before __init__ has run.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._inner, name)


def unwrap_backend(backend: Any) -> Any:
    """Return the real backend behind a virtual overlay.

    Two classes of caller need this:

    * **Sentinel probes** asking "does this workspace still hold its seeded
      files?" — a virtual file always exists, so probing through the overlay
      classifies a wiped workspace as seeded and skips re-seeding.
    * **Non-tool-layer consumers** such as cloud sync, which must operate on
      the real filesystem: virtual content is framework projection, not user
      data, and writing back into a virtual prefix raises ``VirtualPathError``.

    Typed on ``VirtualOverlayBackend`` rather than duck-typed on ``.inner``: a
    ``MagicMock`` auto-creates every attribute, so ``getattr(mock, "inner",
    mock)`` silently hands back a child mock and a test's stand-in backend
    stops being the object under test.

    Lives here, not in ``agent.core.virtual_dirs``, so importing it costs nothing
    — that package pulls in the whole tool registry (~13s to import).
    """
    return backend.inner if isinstance(backend, VirtualOverlayBackend) else backend
