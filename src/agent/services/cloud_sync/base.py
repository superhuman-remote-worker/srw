"""Base class for cloud-agnostic workspace sync.

The sync algorithm (walk workspace, diff against last-known state, push
modified files, poll for remote changes, honor ignore patterns) lives here
exactly once. Per-backend subclasses implement four transport primitives —
mkdir, upload, list, download — so the same algorithm runs for Nextcloud
WebDAV + basic auth, OpenCloud WebDAV + bearer token, or any future
transport (e.g. MS Graph for OneDrive).
"""

from __future__ import annotations

import abc
import asyncio
import hashlib
import json
import logging
import os
import random
import tempfile
import time as _time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, Iterable, Optional, TypeVar

from agent.api.lease_context import LeaseLostError
from agent.core.backends.overlay import unwrap_backend
from shared.cloud_sync_generations import (
    normalize_push_progress,
    EMPTY_BASELINE_SHA256,
    encode_cloud_sync_baseline,
    normalize_cloud_sync_baseline,
)

if TYPE_CHECKING:
    from shared.runtime.core.workspace_backend import WorkspaceBackend

logger = logging.getLogger(__name__)

SYNC_IGNORE_PATTERNS = [
    ".srw/",
    ".git/",
    "repos/",
    "projects/",
    "tools/",
    "todos.yaml",
    "archive/",
    "chunks/",
    "candidates/",
    "requirements/",
    "workspace.md",
    "spec_lock.md",
    "plan.md",
]

SYNC_GENERATION_MARKER_DIR = ".srw/sync-generations"

# Uploads (and directory creates) in flight per generation transmit. Each
# upload checks the DB fence a few times and the agent's pool tops out at 10;
# an SQLite-backed Nextcloud answers 423/503 under parallel writes; Nextcloud's
# desktop client runs 3 transfers at once and rclone defaults to 4.
CLOUD_PUSH_PARALLEL_UPLOADS = 4

# A write refused because the server is momentarily busy (Nextcloud's
# "database is locked" surfaces as 423 or 503) is retried after each of these
# delays, plus up to half again as jitter, so parallel workers do not retry in
# step. Anything else fails the write at once.
_BUSY_WRITE_STATUSES = frozenset({423, 503})
_BUSY_WRITE_RETRY_DELAYS = (0.5, 1.0, 2.0)

_T = TypeVar("_T")


class CloudSyncMarkerError(RuntimeError):
    """A resource-side generation marker was malformed or contradicted DB."""


@dataclass(frozen=True)
class CloudSyncMarker:
    thread_id: str
    mount_id: str
    generation: int
    lease_token: int
    workspace_generation: str
    sync_scope_sha256: str
    baseline_sha256: str = EMPTY_BASELINE_SHA256
    committed_manifest: object = field(default_factory=dict)
    committed_manifest_sha256: str = EMPTY_BASELINE_SHA256


@dataclass(frozen=True)
class CloudSyncGenerationCommit:
    """Resource bytes and membership durably represented by one marker."""

    paths: list[str]
    manifest: dict[str, dict[str, str]]
    manifest_sha256: str


class CloudSyncFenceLost(RuntimeError):
    """A conditional remote write was refused (HTTP 412) by a writer that is
    no longer the owner. Commit-then-effects
    (stateless_turn_resilience.md step 4a): the DB fence stops a stale owner
    at its next write; this is the hard stop for the writes already in
    flight when the fence flipped (at most ``CLOUD_PUSH_PARALLEL_UPLOADS``,
    each carrying its own precondition)."""


@dataclass
class StagedUpload:
    path: str
    tmp_path: str
    sha256: str
    size: int
    if_match: Optional[str] = None
    if_none_match: bool = False


@dataclass
class StagedDelete:
    path: str
    if_match: Optional[str] = None


@dataclass
class StagedGenerationDelta:
    """Everything a transmit needs that only the workspace could provide.

    Staging reads and hashes the durable workspace (SFTP) and copies every
    changed file into a local temp file; transmitting then needs only the
    cloud transport and the DB fence. That split is what lets the run_queue
    unit complete — and the pod detach its workspace — while the (slow)
    WebDAV writes continue off-slot, or resume on another pod from
    ``push_progress``.
    """

    uploads: list[StagedUpload]
    deletes: list[StagedDelete]
    manifest: dict[str, dict[str, str]]
    planned: int
    skipped_by_progress: int = 0
    files_walked: int = 0
    stage_seconds: float = 0.0

    def cleanup(self) -> None:
        for upload in self.uploads:
            try:
                os.unlink(upload.tmp_path)
            except OSError:
                pass
        self.uploads = []


def _sync_generation_marker_path(thread_id: str, sync_scope_sha256: str) -> str:
    """Return a path-safe marker name scoped to one thread/resource pair.

    Project and user-home mounts can point several threads at the same WebDAV
    root.  A single well-known marker there lets one thread overwrite another's
    acknowledgement.  Hashing both non-secret identities keeps the path safe
    even if a malformed identifier reaches this layer and prevents that shared-
    root collision without exposing either value in a remote filename.
    """

    identity = f"{thread_id}\0{sync_scope_sha256}".encode("utf-8")
    marker_name = hashlib.sha256(identity).hexdigest()
    return f"{SYNC_GENERATION_MARKER_DIR}/{marker_name}.json"


def _should_ignore(rel_path: str) -> bool:
    """Return True if ``rel_path`` matches any ignore pattern."""
    for pattern in SYNC_IGNORE_PATTERNS:
        if pattern.endswith("/"):
            if rel_path.startswith(pattern) or f"/{pattern}" in f"/{rel_path}":
                return True
        elif rel_path == pattern or rel_path.endswith(f"/{pattern}"):
            return True
    return False


def _write_all(fd: int, payload: bytes) -> None:
    """Write every byte to an already-open temporary file descriptor."""

    remaining = memoryview(payload)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("temporary cloud-sync staging write made no progress")
        remaining = remaining[written:]


def _reject_ignored_generation_paths(manifest: object) -> None:
    """Keep cooperative markers from authorizing out-of-scope mutations."""

    if not isinstance(manifest, dict):
        return
    ignored = sorted(path for path in manifest if _should_ignore(path))
    if ignored:
        raise CloudSyncMarkerError(
            "cloud sync generation manifest contains ignored path(s): "
            + ", ".join(ignored[:3])
        )


def _normalize_dav_listing(raw: list, webdav_base_path: str) -> list[dict]:
    """Shape a webdav3 ``list(get_info=True)`` result for the sync algorithm.

    Shared by the Nextcloud and OpenCloud transports (it was duplicated).
    Strips the server-absolute prefix down to mount-relative paths and
    coerces ``size`` (webdav3 reports it as a string, absent for
    collections) to ``int | None``.
    """
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path", "")
        if raw_path.startswith(webdav_base_path):
            rel = raw_path[len(webdav_base_path) :]
        else:
            rel = raw_path.strip("/")
        size = item.get("size")
        try:
            size_int: Optional[int] = int(size) if size not in (None, "") else None
        except (TypeError, ValueError):
            size_int = None
        out.append(
            {
                "path": rel,
                "etag": item.get("etag", "") or "",
                "isdir": bool(item.get("isdir")),
                "size": size_int,
            }
        )
    return out


def _is_fence_loss(exc: BaseException) -> bool:
    """The writer was fenced off: by the DB (lease/push owner) or by a 412."""

    return isinstance(exc, (CloudSyncFenceLost, LeaseLostError))


def _busy_write_refusal(exc: BaseException) -> bool:
    """A 423 or 503 from the transport: retried briefly, not a failure yet.

    webdav3 raises ``ResponseErrorCode`` (with ``code``) for a 503 and its
    own ``ResourceLocked`` (without one) for a 423.
    """

    if getattr(exc, "code", None) in _BUSY_WRITE_STATUSES:
        return True
    return type(exc).__name__ == "ResourceLocked"


def _parent_dirs_by_level(paths: Iterable[str]) -> list[list[str]]:
    """Every ancestor directory of ``paths``, grouped by depth, shallowest first."""

    levels: dict[int, set[str]] = {}
    for path in paths:
        parent = Path(path).parent
        while str(parent) not in ("", "."):
            levels.setdefault(len(parent.parts), set()).add(str(parent))
            parent = parent.parent
    return [sorted(levels[depth]) for depth in sorted(levels)]


async def _drain_bounded(
    items: list[_T],
    work: Callable[[_T, Callable[[], bool]], Awaitable[None]],
    *,
    limit: int,
) -> None:
    """Run ``work(item, stopping)`` over ``items``, at most ``limit`` at once.

    The staging-reader pattern: workers pull from one shared iterator and
    take no new item once any call has failed; ``stopping()`` lets a call cut
    its own retries short. The workers run under ``asyncio.shield``, so a
    cancelled caller still waits for every call in flight: cancelling
    ``to_thread`` does not stop its request, and a write must not be able to
    land after the caller was told the transmit stopped. After the drain a
    cancellation propagates; otherwise the first fence loss wins over the
    first other error, so a stale writer reports that it was fenced off
    rather than a side effect of being fenced off.
    """

    if not items:
        return
    remaining = iter(items)
    stopping = False
    errors: list[BaseException] = []

    def should_stop() -> bool:
        return stopping

    async def worker() -> None:
        nonlocal stopping
        while not stopping:
            item = next(remaining, None)
            if item is None:
                return
            try:
                await work(item, should_stop)
            except BaseException as exc:
                stopping = True
                errors.append(exc)
                return

    workers = asyncio.gather(
        *(worker() for _ in range(min(limit, len(items)))),
        return_exceptions=True,
    )
    try:
        await asyncio.shield(workers)
    except BaseException:
        stopping = True
        while not workers.done():
            try:
                await asyncio.shield(workers)
            except asyncio.CancelledError:
                # A second shutdown signal must not abandon a write in
                # flight either.
                continue
        raise
    if errors:
        raise next((exc for exc in errors if _is_fence_loss(exc)), errors[0])


class WorkspaceSyncBase(abc.ABC):
    """Bidirectional workspace ↔ cloud-folder sync.

    Subclasses implement transport primitives; this class owns the
    algorithm and background polling lifecycle.
    """

    def __init__(
        self,
        workspace_path: Path,
        *,
        poll_interval: int = 15,
        workspace_backend: Optional["WorkspaceBackend"] = None,
        mount_subdir: str = "",
    ) -> None:
        self._workspace_path = Path(workspace_path)
        self._poll_interval = poll_interval
        # Unwrap the virtual overlay once, here, so no method below can reach a
        # virtual path. Cloud sync is not a tool-layer consumer: virtual
        # prefixes (tools/, contacts/, instructions.md) are framework
        # projection, not user data. Walking the overlay merged contacts/ into
        # the root listing and uploaded contact names, emails and phone numbers
        # to the user's cloud folder — SYNC_IGNORE_PATTERNS lists tools/ but
        # nothing else — and the return pull then raised VirtualPathError,
        # which the coordinator's strict=True turned into "cloud sync disabled
        # for the rest of the session". Doing it structurally, rather than by
        # extending SYNC_IGNORE_PATTERNS, means a future provider cannot
        # reintroduce either failure.
        # See knowledge-base/knowledge/features/virtual_directories.md.
        self._backend = unwrap_backend(workspace_backend) if workspace_backend else None
        # Workspace-relative path of this mount inside the backend's filesystem.
        # Empty string means "the whole workspace" (legacy single-folder mount).
        # For project mounts, e.g. "projects/alpha". No leading/trailing slash.
        self._mount_subdir = mount_subdir.strip("/")

        self._local_state: dict[str, float] = {}
        self._remote_state: dict[str, str] = {}
        # ETag is optional in WebDAV listings. Keep presence separate so an
        # observed remote file with an empty ETag is still a clean baseline
        # entry, never misclassified as local-only and re-uploaded every turn.
        self._remote_known_paths: set[str] = set()
        # Content hashes from the predecessor's v3 commit marker (or a download
        # performed by this instance). They represent remote truth even when an
        # idle workspace process changed the local copy after the marker.
        self._remote_content_hashes: dict[str, str] = {}
        self._remote_dirs: set[str] = set()
        self._pushed_sizes: dict[str, int] = {}
        # One-shot guard for priming the dedup dicts from the remote tree.
        # All state above is process memory: a fresh agent pod used to
        # re-upload (and re-download) the whole mount because it had no way
        # to know the cloud already held identical content. Set by the first
        # pull (whose listing doubles as the seed) or by _seed_remote_state
        # on a push that runs before any pull.
        # knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md
        self._remote_seeded: bool = False

        self._poll_task: Optional[asyncio.Task] = None
        self._running = False

    # --------------------------------------------------------------- Primitives

    @abc.abstractmethod
    async def _ensure_ready(self) -> None:
        """Verify transport is ready (lazy client init, token fetch, etc.)."""

    @abc.abstractmethod
    async def _ensure_remote_dir(
        self,
        rel_dir: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        """Create a single remote directory. Idempotent; swallow "exists"."""

    @abc.abstractmethod
    async def _upload_file(
        self,
        rel_path: str,
        local_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> Optional[str]:
        """Upload ``local_path`` to remote ``rel_path``. Parent dirs already exist.

        ``if_match`` / ``if_none_match`` are RFC 4918 preconditions: a
        transport that supports them must send them and raise
        :class:`CloudSyncFenceLost` on a 412; one that cannot must ignore them
        (the DB fence still stops a stale owner at its next write). Returns
        the new ETag when the server reports one, else ``None``.
        """

    async def _delete_remote_file(
        self,
        rel_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
    ) -> None:
        """Delete one remote file for a generation delta.

        Legacy pinned sync never calls this method.  Stateless generation
        commits do so only for a path that existed in the armed baseline and
        disappeared from the durable workspace.  A transport that cannot
        implement deletion must fail closed rather than acknowledge a partial
        mirror generation. ``if_match`` follows ``_upload_file``.
        """

        raise NotImplementedError("cloud transport does not support fenced deletes")

    async def _remote_etag(self, rel_path: str) -> Optional[str]:
        """Current ETag of one remote file, ``None`` when absent or unknown.

        Used once, after a 412 that the DB fence says is not ours: the remote
        moved under us (a user edit, or a predecessor's identical bytes) and
        the retry needs a fresh precondition. Default: a parent listing.
        """

        parent = str(Path(rel_path).parent)
        try:
            items = await self._list_remote_files("" if parent == "." else parent)
        except Exception:
            return None
        for item in items:
            if item.get("path") == rel_path and not item.get("isdir"):
                etag = item.get("etag")
                return str(etag) if etag else None
        return None

    @abc.abstractmethod
    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        """List ONE remote directory level (``rel_dir`` is mount-relative).

        Returns a list of dicts shaped
        ``{"path": <rel>, "etag": <str>, "isdir": <bool>, "size": <int|None>}``
        where ``path`` is relative to the MOUNT ROOT, not to ``rel_dir``.
        WebDAV ``list`` is a Depth-1 PROPFIND, so this is a single level;
        recursion lives in ``_list_remote_tree``. Implementations returning
        more than the requested level (test doubles list recursively) are
        fine — the tree walk dedups.
        """

    @abc.abstractmethod
    async def _download_file(self, rel_path: str, local_path: str) -> None:
        """Download remote ``rel_path`` into ``local_path``."""

    async def aclose(self) -> None:
        """Release per-instance resources. Default: nothing to release."""
        return None

    # ----------------------------------------------------- Generation marker

    @staticmethod
    def _marker_missing(exc: BaseException) -> bool:
        """Best-effort cross-WebDAV missing-resource classification.

        Both production clients raise webdav3's RemoteResourceNotFound while
        the filesystem test transport raises FileNotFoundError. Other errors
        stay fail-closed: treating an auth/network failure as an absent marker
        would turn a read outage into an unsolicited recovery push.
        """

        if isinstance(exc, FileNotFoundError):
            return True
        name = type(exc).__name__.lower()
        return "notfound" in name or "not_found" in name

    async def read_sync_generation_marker(
        self,
        *,
        thread_id: str,
        sync_scope_sha256: str,
    ) -> Optional[CloudSyncMarker]:
        """Read and strictly validate the cloud-resource commit marker.

        The marker lives with the bytes it acknowledges, so it survives agent
        pod death. It is user-writable cloud data and therefore a cooperative
        correctness marker, not an authorization/security fence. The DB-side
        required generation remains lease-fenced and authoritative.
        """

        marker_path = _sync_generation_marker_path(thread_id, sync_scope_sha256)
        await self._ensure_ready()
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        try:
            try:
                await self._download_file(marker_path, tmp_path)
            except Exception as exc:
                if self._marker_missing(exc):
                    return None
                raise
            try:
                with open(tmp_path, "r", encoding="utf-8") as marker_file:
                    raw = json.load(marker_file)
                if not isinstance(raw, dict) or raw.get("version") != 3:
                    raise ValueError("unsupported marker version")
                thread_id = raw.get("thread_id")
                mount_id = raw.get("mount_id")
                generation = raw.get("generation")
                lease_token = raw.get("lease_token")
                workspace_generation = raw.get("workspace_generation")
                sync_scope_sha256 = raw.get("sync_scope_sha256")
                baseline_sha256 = raw.get("baseline_sha256")
                committed_manifest = raw.get("committed_manifest")
                committed_manifest_sha256 = raw.get("committed_manifest_sha256")
                if not isinstance(thread_id, str) or not thread_id:
                    raise ValueError("thread_id must be a non-empty string")
                if not isinstance(mount_id, str) or not mount_id:
                    raise ValueError("mount_id must be a non-empty string")
                if type(generation) is not int or generation < 1:
                    raise ValueError("generation must be a positive integer")
                if type(lease_token) is not int or lease_token < 1:
                    raise ValueError("lease_token must be a positive integer")
                if (
                    not isinstance(workspace_generation, str)
                    or not workspace_generation
                ):
                    raise ValueError("workspace_generation must be a non-empty string")
                if (
                    not isinstance(sync_scope_sha256, str)
                    or len(sync_scope_sha256) != 64
                    or any(char not in "0123456789abcdef" for char in sync_scope_sha256)
                ):
                    raise ValueError("sync_scope_sha256 must be a lowercase SHA-256")
                if (
                    not isinstance(baseline_sha256, str)
                    or len(baseline_sha256) != 64
                    or any(char not in "0123456789abcdef" for char in baseline_sha256)
                ):
                    raise ValueError("baseline_sha256 must be a lowercase SHA-256")
                (
                    committed_manifest,
                    _committed_encoded,
                    actual_committed_sha256,
                ) = encode_cloud_sync_baseline(committed_manifest)
                _reject_ignored_generation_paths(committed_manifest)
                if committed_manifest_sha256 != actual_committed_sha256:
                    raise ValueError("committed manifest digest mismatch")
                return CloudSyncMarker(
                    thread_id=thread_id,
                    mount_id=mount_id,
                    generation=generation,
                    lease_token=lease_token,
                    workspace_generation=workspace_generation,
                    sync_scope_sha256=sync_scope_sha256,
                    baseline_sha256=baseline_sha256,
                    committed_manifest=committed_manifest,
                    committed_manifest_sha256=actual_committed_sha256,
                )
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                raise CloudSyncMarkerError(
                    f"invalid cloud sync generation marker: {exc}"
                ) from exc
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    async def write_sync_generation_marker(
        self,
        marker: CloudSyncMarker,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        """Commit the marker last, without overwriting a successor's marker."""

        marker_path = _sync_generation_marker_path(
            marker.thread_id, marker.sync_scope_sha256
        )
        await self._ensure_ready()
        if before_write is not None:
            await before_write()
        await self._ensure_remote_dirs(
            str(Path(marker_path).parent), before_write=before_write
        )
        fd, tmp_path = tempfile.mkstemp()
        try:
            (
                committed_manifest,
                _committed_encoded,
                committed_manifest_sha256,
            ) = encode_cloud_sync_baseline(marker.committed_manifest or {})
            _reject_ignored_generation_paths(committed_manifest)
            if marker.committed_manifest_sha256 != committed_manifest_sha256:
                raise CloudSyncMarkerError(
                    "cloud sync committed manifest digest changed before marker write"
                )
            payload = json.dumps(
                {
                    "version": 3,
                    "thread_id": marker.thread_id,
                    "mount_id": marker.mount_id,
                    "generation": marker.generation,
                    "lease_token": marker.lease_token,
                    "workspace_generation": marker.workspace_generation,
                    "sync_scope_sha256": marker.sync_scope_sha256,
                    "baseline_sha256": marker.baseline_sha256,
                    "committed_manifest": committed_manifest,
                    "committed_manifest_sha256": committed_manifest_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            _write_all(fd, payload)
            os.close(fd)
            fd = -1  # The transport may reuse this descriptor across the awaits.
            # The final DB check and PUT are separate operations. Capture the
            # marker's ETag before that check so an old in-flight request
            # cannot regress the commit record after another owner advances it.
            etag = await self._remote_etag(marker_path)
            if before_write is not None:
                await before_write()
            await self._upload_file(
                marker_path,
                tmp_path,
                before_write=before_write,
                if_match=etag,
                if_none_match=etag is None,
            )
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ---------------------------------------------------- Generation baseline

    async def capture_generation_baseline(
        self,
    ) -> tuple[dict[str, dict[str, str]], str]:
        """Hash the durable workspace after pull and bind known remote etags.

        Stateless cloud ordering cannot use the legacy process-local
        size/mtime caches: those both reset on handoff and miss same-length
        edits.  The baseline is armed in Postgres before tool work and lets a
        successor recover only paths the predecessor actually changed.  That
        preserves cloud-side edits to unrelated paths and avoids a full PUT of
        the workspace on every turn.
        """

        started = _time.perf_counter()
        if self._backend is None:
            raise CloudSyncMarkerError(
                "stateless cloud generation requires a durable workspace backend"
            )
        local_files = {
            path
            for path in await self._walk_backend_files_async(strict=True)
            if not _should_ignore(path)
        }
        # A marker-proven remote file may have been deleted locally by an idle
        # workspace process after the predecessor committed. Keep that path in
        # the armed baseline so the next delta issues the remote DELETE. A true
        # cloud tombstone is removed from _remote_known_paths by pull first.
        files = sorted(local_files | self._remote_known_paths)
        walked = _time.perf_counter()
        semaphore = asyncio.Semaphore(8)

        async def _one(path: str) -> tuple[str, dict[str, str]]:
            remote_digest = self._remote_content_hashes.get(path)
            if path in self._remote_known_paths and remote_digest:
                return path, {
                    "sha256": remote_digest,
                    "remote_etag": str(self._remote_state.get(path) or ""),
                }
            async with semaphore:
                content = await asyncio.to_thread(
                    self._backend.read_file,
                    self._backend_path(path),
                    True,
                )
            if isinstance(content, str):
                content = content.encode("utf-8")
            return path, {
                "sha256": hashlib.sha256(content).hexdigest(),
                "remote_etag": str(self._remote_state.get(path) or ""),
            }

        # Only remote-proven entries are clean.  A local-only path deliberately
        # stays absent so the generation delta uploads it even when it already
        # existed before the turn.
        manifest = {
            path: entry
            for path, entry in await asyncio.gather(*(_one(path) for path in files))
            if path in self._remote_known_paths
        }
        normalized, _encoded, digest = encode_cloud_sync_baseline(manifest)
        logger.info(
            "cloud generation baseline detail: mount=%s walk=%.2fs files=%d "
            "remote_proven=%d hash=%.2fs total=%.2fs",
            self._mount_subdir or "<root>",
            walked - started,
            len(files),
            len(normalized),
            _time.perf_counter() - walked,
            _time.perf_counter() - started,
        )
        return normalized, digest

    def install_generation_baseline(self, manifest: object) -> None:
        """Seed cross-pod remote etags from a validated durable baseline."""

        normalized = normalize_cloud_sync_baseline(manifest)
        _reject_ignored_generation_paths(normalized)
        self._remote_state.clear()
        self._remote_known_paths = set(normalized)
        self._remote_content_hashes = {
            path: entry["sha256"] for path, entry in normalized.items()
        }
        for path, entry in normalized.items():
            if entry["remote_etag"]:
                self._remote_state[path] = entry["remote_etag"]

    async def stage_generation_delta(
        self,
        baseline: object,
        *,
        progress: object = None,
    ) -> StagedGenerationDelta:
        """Read the workspace once and decide every write of this generation.

        New and content-changed files (vs the durable turn-start baseline)
        are copied to local temp files; baseline files missing locally become
        deletes. ``progress`` — a predecessor's durable ``push_progress`` —
        overlays the baseline: a path whose recorded upload matches the
        current bytes is already remote and is skipped; a recorded delete is
        not re-issued. No remote I/O happens here.
        """

        started = _time.perf_counter()
        normalized = normalize_cloud_sync_baseline(baseline)
        _reject_ignored_generation_paths(normalized)
        done = normalize_push_progress(progress)["files"] if progress else {}
        committed_manifest = {path: dict(entry) for path, entry in normalized.items()}
        for path, entry in done.items():
            if entry["state"] == "uploaded":
                committed_manifest[path] = {
                    "sha256": entry["sha256"],
                    "remote_etag": entry["remote_etag"],
                }
            else:
                committed_manifest.pop(path, None)
        if self._backend is None:
            raise CloudSyncMarkerError(
                "stateless cloud generation requires a durable workspace backend"
            )
        index = await self._backend_file_index()
        listed = (
            index
            if index is not None
            else await self._walk_backend_files_async(strict=True)
        )
        current_paths = sorted({path for path in listed if not _should_ignore(path)})
        walked = _time.perf_counter()
        deletes: list[StagedDelete] = []
        skipped = 0
        for path in normalized:
            if path in current_paths:
                continue
            recorded = done.get(path)
            if recorded is not None and recorded["state"] == "deleted":
                skipped += 1
                continue
            deletes.append(
                StagedDelete(
                    path=path, if_match=normalized[path]["remote_etag"] or None
                )
            )
        uploads: list[StagedUpload] = []

        async def stage_one(path: str) -> None:
            nonlocal skipped
            content = await asyncio.to_thread(
                self._backend.read_file,  # type: ignore[union-attr]
                self._backend_path(path),
                True,
            )
            if isinstance(content, str):
                content = content.encode("utf-8")
            current_digest = hashlib.sha256(content).hexdigest()
            recorded = done.get(path)
            if (
                recorded is not None
                and recorded["state"] == "uploaded"
                and recorded["sha256"] == current_digest
            ):
                skipped += 1
                return
            previous = normalized.get(path)
            if (
                previous is not None
                and previous["sha256"] == current_digest
                and recorded is None
            ):
                return
            fd, tmp_path = tempfile.mkstemp()
            try:
                _write_all(fd, content)
            except BaseException:
                os.unlink(tmp_path)
                raise
            finally:
                os.close(fd)
            if_match: Optional[str] = None
            if_none_match = False
            if previous is not None and previous["remote_etag"]:
                if_match = previous["remote_etag"]
            elif recorded is not None and recorded["remote_etag"]:
                if_match = recorded["remote_etag"]
            elif previous is None and recorded is None:
                if_none_match = True
            uploads.append(
                StagedUpload(
                    path=path,
                    tmp_path=tmp_path,
                    sha256=current_digest,
                    size=len(content),
                    if_match=if_match,
                    if_none_match=if_none_match,
                )
            )

        # Four readers amortize object-store process/round-trip costs while
        # bounding resident file bytes. SFTP backends retain their own lock.
        remaining = iter(current_paths)
        stopping = False

        async def worker() -> None:
            nonlocal stopping
            try:
                while not stopping:
                    path = next(remaining, None)
                    if path is None:
                        return
                    await stage_one(path)
            except BaseException:
                stopping = True
                raise

        readers = asyncio.gather(
            *(worker() for _ in range(min(4, len(current_paths)))),
            return_exceptions=True,
        )
        try:
            # Cancelling to_thread does not stop its read. Drain the workers
            # before cleaning staged files or letting the caller detach.
            results = await asyncio.shield(readers)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        except BaseException:
            stopping = True
            while not readers.done():
                try:
                    await asyncio.shield(readers)
                except asyncio.CancelledError:
                    # A second shutdown signal must not cancel an in-flight
                    # synchronous backend read either.
                    continue
            for upload in uploads:
                try:
                    os.unlink(upload.tmp_path)
                except OSError:
                    pass
            raise
        uploads.sort(key=lambda upload: upload.path)
        logger.info(
            "cloud generation staged: mount=%s walk=%.2fs read=%.2fs files=%d uploads=%d deletes=%d",
            self._mount_subdir or "<root>",
            walked - started,
            _time.perf_counter() - walked,
            len(current_paths),
            len(uploads),
            len(deletes),
        )
        return StagedGenerationDelta(
            uploads=uploads,
            deletes=deletes,
            manifest=committed_manifest,
            planned=len(uploads) + len(deletes) + skipped,
            skipped_by_progress=skipped,
            files_walked=len(current_paths),
            stage_seconds=walked - started,
        )

    async def _put_retrying_busy(
        self,
        upload: StagedUpload,
        *,
        before_write: Callable[[], Awaitable[None]],
        if_match: Optional[str],
        if_none_match: bool,
        stopping: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """One conditional PUT, retried while the server says it is busy.

        A 423 or 503 is retried after each of ``_BUSY_WRITE_RETRY_DELAYS``;
        every attempt goes back through the fence and carries the same
        precondition, so a retry of a PUT that did land comes back as a 412
        for ``_write_conditionally`` to resolve. Any other error, a busy
        answer after the last delay, or one while the transmit is stopping,
        raises.
        """

        attempt = 0
        while True:
            try:
                return await self._upload_file(
                    upload.path,
                    upload.tmp_path,
                    before_write=before_write,
                    if_match=if_match,
                    if_none_match=if_none_match,
                )
            except Exception as exc:
                if (
                    attempt >= len(_BUSY_WRITE_RETRY_DELAYS)
                    or not _busy_write_refusal(exc)
                    or (stopping is not None and stopping())
                ):
                    raise
                delay = _BUSY_WRITE_RETRY_DELAYS[attempt]
                attempt += 1
                logger.info(
                    "cloud push write for %s refused as busy (%s %s); retry %d "
                    "in %.1fs",
                    upload.path,
                    type(exc).__name__,
                    getattr(exc, "code", 423),
                    attempt,
                    delay,
                )
            await asyncio.sleep(delay * random.uniform(1.0, 1.5))

    async def _write_conditionally(
        self,
        upload: StagedUpload,
        *,
        before_write: Callable[[], Awaitable[None]],
        stopping: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """One PUT with its precondition; the 412 policy in one place.

        A 412 means the remote moved under us. If the DB fence says we are no
        longer the owner, that is the stale-writer case the precondition
        exists for — stop. If we still own the push, the mover was a user edit
        or a predecessor's identical bytes: refresh the precondition from the
        remote once and retry; a second 412 is treated as fence loss.
        """

        try:
            return await self._put_retrying_busy(
                upload,
                before_write=before_write,
                if_match=upload.if_match,
                if_none_match=upload.if_none_match,
                stopping=stopping,
            )
        except CloudSyncFenceLost:
            await before_write()  # raises if the DB says we are stale
            fresh = await self._remote_etag(upload.path)
            logger.warning(
                "cloud push precondition failed for %s (remote moved); retrying "
                "with the current remote etag",
                upload.path,
            )
            return await self._put_retrying_busy(
                upload,
                before_write=before_write,
                if_match=fresh,
                if_none_match=fresh is None,
                stopping=stopping,
            )

    async def _delete_conditionally(
        self,
        delete: StagedDelete,
        *,
        before_write: Callable[[], Awaitable[None]],
    ) -> None:
        try:
            await self._delete_remote_file(
                delete.path, before_write=before_write, if_match=delete.if_match
            )
        except CloudSyncFenceLost:
            await before_write()
            fresh = await self._remote_etag(delete.path)
            if fresh is None:
                return  # already gone
            await self._delete_remote_file(
                delete.path, before_write=before_write, if_match=fresh
            )

    async def transmit_generation_delta(
        self,
        staged: StagedGenerationDelta,
        *,
        before_write: Callable[[], Awaitable[None]],
        progress_cb: Optional[
            Callable[[str, dict[str, object]], Awaitable[None]]
        ] = None,
    ) -> CloudSyncGenerationCommit:
        """Land a staged delta on the cloud: only transport + fence, no workspace.

        Every write is preceded by ``before_write`` (the DB fence) and carries
        its precondition; ``progress_cb`` is told after each write lands so
        the durable progress lets a successor resume. Temp files are removed
        whether or not the transmit completes.

        Order: every parent directory first, one level at a time; then the
        uploads, at most ``CLOUD_PUSH_PARALLEL_UPLOADS`` in flight; then, only
        once every upload has landed, the deletes one at a time. A failure
        or a fence loss stops new writes; the writes already in flight finish
        (each still guarded by its precondition) before the error is raised.
        """

        started = _time.perf_counter()
        committed_manifest = {
            path: dict(entry) for path, entry in staged.manifest.items()
        }
        # By staged index, so commit.paths keeps the staged (sorted) order
        # whatever order the parallel uploads land in.
        landed: list[Optional[str]] = [None] * len(staged.uploads)
        pushed: list[str] = []
        dirs_missing = 0
        mkdir_seconds = put_seconds = 0.0

        async def mkdir(rel_dir: str, _stopping: Callable[[], bool]) -> None:
            await before_write()
            await self._ensure_remote_dirs(rel_dir, before_write=before_write)

        async def put(
            item: tuple[int, StagedUpload], stopping: Callable[[], bool]
        ) -> None:
            index, upload = item
            await before_write()
            # Free when phase 1 made the parent; asks again if it could not.
            await self._ensure_remote_dirs(
                str(Path(upload.path).parent), before_write=before_write
            )
            etag = await self._write_conditionally(
                upload, before_write=before_write, stopping=stopping
            )
            # Landed: record it on the event loop, whatever happens to the
            # rest of the batch, so a successor can skip it.
            self._pushed_sizes[upload.path] = upload.size
            self._remote_known_paths.add(upload.path)
            self._remote_content_hashes[upload.path] = upload.sha256
            if etag:
                self._remote_state[upload.path] = etag
            else:
                self._remote_state.pop(upload.path, None)
            committed_manifest[upload.path] = {
                "sha256": upload.sha256,
                "remote_etag": etag or "",
            }
            landed[index] = upload.path
            if progress_cb is not None:
                await progress_cb(
                    upload.path,
                    {
                        "sha256": upload.sha256,
                        "size": upload.size,
                        "remote_etag": etag or "",
                        "state": "uploaded",
                    },
                )

        try:
            await before_write()
            await self._ensure_ready()
            phase = _time.perf_counter()
            for level in _parent_dirs_by_level(u.path for u in staged.uploads):
                missing = [d for d in level if d not in self._remote_dirs]
                dirs_missing += len(missing)
                await _drain_bounded(missing, mkdir, limit=CLOUD_PUSH_PARALLEL_UPLOADS)
            mkdir_seconds = _time.perf_counter() - phase
            phase = _time.perf_counter()
            await _drain_bounded(
                list(enumerate(staged.uploads)),
                put,
                limit=CLOUD_PUSH_PARALLEL_UPLOADS,
            )
            put_seconds = _time.perf_counter() - phase
            pushed = [path for path in landed if path is not None]
            for delete in staged.deletes:
                await before_write()
                await self._delete_conditionally(delete, before_write=before_write)
                self._remote_state.pop(delete.path, None)
                self._remote_known_paths.discard(delete.path)
                self._remote_content_hashes.pop(delete.path, None)
                self._pushed_sizes.pop(delete.path, None)
                committed_manifest.pop(delete.path, None)
                pushed.append(delete.path)
                if progress_cb is not None:
                    await progress_cb(
                        delete.path,
                        {
                            "sha256": "",
                            "size": 0,
                            "remote_etag": "",
                            "state": "deleted",
                        },
                    )
        finally:
            staged.cleanup()
        committed_manifest, _encoded, committed_manifest_sha256 = (
            encode_cloud_sync_baseline(committed_manifest)
        )
        logger.info(
            "cloud generation push detail: mount=%s walk=%.2fs files=%d "
            "uploads=%d deletes=%d resumed=%d total=%.2fs dirs=%d mkdir=%.2fs "
            "put=%.2fs parallel=%d",
            self._mount_subdir or "<root>",
            staged.stage_seconds,
            staged.files_walked,
            len(pushed) - len(staged.deletes),
            len(staged.deletes),
            staged.skipped_by_progress,
            _time.perf_counter() - started + staged.stage_seconds,
            dirs_missing,
            mkdir_seconds,
            put_seconds,
            CLOUD_PUSH_PARALLEL_UPLOADS,
        )
        return CloudSyncGenerationCommit(
            paths=pushed,
            manifest=committed_manifest,
            manifest_sha256=committed_manifest_sha256,
        )

    async def push_generation_delta(
        self,
        baseline: object,
        *,
        before_write: Callable[[], Awaitable[None]],
        progress: object = None,
        progress_cb: Optional[
            Callable[[str, dict[str, object]], Awaitable[None]]
        ] = None,
    ) -> CloudSyncGenerationCommit:
        """Commit only paths changed since the durable turn-start baseline.

        Stage (workspace) then transmit (cloud); see both halves. Baseline
        membership is the proof that a path existed remotely; an empty ETag is
        valid and does not by itself make an unchanged path dirty. Deletions
        of baseline files use the transport delete primitive; a transport that
        cannot prove that delete fails the generation instead of writing a
        lying marker.
        """

        await before_write()
        staged = await self.stage_generation_delta(baseline, progress=progress)
        return await self.transmit_generation_delta(
            staged, before_write=before_write, progress_cb=progress_cb
        )

    # --------------------------------------------------------------- Algorithm

    async def _ensure_remote_dirs(
        self,
        rel_dir: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        """Recursively create remote directories, caching the ones that exist.

        A directory is cached only when ``_ensure_remote_dir`` returned, i.e.
        the MKCOL created it or answered that it already exists (201 / 405
        for WebDAV). A failed MKCOL is swallowed, since the write below it
        reports the real error, but never cached: one bad entry used to fail
        every later upload under that directory, recovery retries included.
        """
        if not rel_dir or rel_dir == "." or rel_dir in self._remote_dirs:
            return
        parent = str(Path(rel_dir).parent)
        if parent and parent != rel_dir:
            await self._ensure_remote_dirs(parent, before_write=before_write)
        try:
            if before_write is not None:
                await before_write()
            await self._ensure_remote_dir(rel_dir, before_write=before_write)
        except Exception as exc:
            logger.debug("Remote mkdir %s not confirmed: %s", rel_dir, exc)
            return
        self._remote_dirs.add(rel_dir)

    def _backend_path(self, mount_rel_path: str) -> str:
        """Translate a mount-relative path to a workspace-backend path."""
        if not self._mount_subdir:
            return mount_rel_path
        return (
            f"{self._mount_subdir}/{mount_rel_path}"
            if mount_rel_path
            else self._mount_subdir
        )

    async def _list_remote_tree_fast(self) -> Optional[list[dict]]:
        """Optional transport primitive: the WHOLE mount tree in one shot.

        Return the complete recursive listing (files may include ignored
        paths and directory entries — the caller filters), or ``None`` when
        the transport has no such capability (permanently or for this
        server). The default has none. Motivation: the per-directory walk
        costs one ``_list_remote_files`` round trip per directory, measured
        at ~2.5s/dir via webdav3 (2026-08-08 baseline: 39.9s to list 16
        files); Nextcloud serves the same tree in ONE ``Depth: infinity``
        PROPFIND (~1s).
        """
        return None

    async def _list_remote_tree(self) -> list[dict]:
        """Recursively list the mount via per-directory ``_list_remote_files``.

        Tries the transport's one-shot ``_list_remote_tree_fast`` first;
        falls back to the walk when the transport reports no capability.
        Fast results pass through the same shaping the walk applies (files
        only, ignored subtrees dropped, deduped) — ``_should_ignore``'s
        directory patterns match by path prefix, so per-file filtering
        subsumes the walk's descend-time pruning.

        The old single ``_list_remote_files()`` call was documented as
        recursive but backed by a Depth-1 PROPFIND, which silently limited
        pull (and now dedup seeding) to ROOT-LEVEL files — cloud-side edits
        under ``output/`` etc. never reached the agent. Ignored subtrees are
        pruned before descending; the visited set drops entries echoing the
        listed collection itself (WebDAV servers include it) and keeps
        fully-recursive test doubles from double-listing.
        """
        fast = await self._list_remote_tree_fast()
        if fast is not None:
            out: list[dict] = []
            seen_fast: set[str] = set()
            for item in fast:
                path = (item.get("path") or "").strip("/")
                if not path or item.get("isdir") or _should_ignore(path):
                    continue
                if path not in seen_fast:
                    seen_fast.add(path)
                    out.append({**item, "path": path})
            return out
        out = []
        seen_files: set[str] = set()
        visited: set[str] = set()
        stack: list[str] = [""]
        while stack:
            rel_dir = stack.pop()
            if rel_dir in visited:
                continue
            visited.add(rel_dir)
            for item in await self._list_remote_files(rel_dir):
                path = (item.get("path") or "").strip("/")
                if not path or path == rel_dir:
                    continue
                if item.get("isdir"):
                    if not _should_ignore(f"{path}/") and path not in visited:
                        stack.append(path)
                    continue
                if path not in seen_files:
                    seen_files.add(path)
                    out.append({**item, "path": path})
        return out

    async def _seed_remote_state(self) -> None:
        """One-shot: prime push dedup from the remote tree's sizes.

        A fresh agent pod starts with empty in-memory dedup state and used
        to re-upload the entire mount on its first push — minutes of serial
        WebDAV round-trips. Recording each remote file's size as the
        last-pushed size makes the first push skip everything the cloud
        already holds at the same size: the documented size-dedup tradeoff
        (a same-size, different-content change is missed until the size
        moves), extended across pod recycles instead of newly invented.

        Normally the attach/turn-start pull has already seeded (shared
        ``_remote_seeded`` flag); this covers a push that runs before any
        pull succeeded. Fail-soft and single-shot: on listing failure the
        push simply runs unseeded, which is the old behavior.
        """
        if self._remote_seeded:
            return
        self._remote_seeded = True
        try:
            items = await self._list_remote_tree()
        except Exception as e:
            logger.debug("Remote dedup seeding skipped: %s", e)
            return
        seeded = 0
        for item in items:
            path = item.get("path", "")
            if not path or _should_ignore(path):
                continue
            size = item.get("size")
            if path not in self._pushed_sizes and isinstance(size, int) and size >= 0:
                self._pushed_sizes[path] = size
                seeded += 1
        if seeded:
            logger.info("Seeded push dedup from %d remote file size(s)", seeded)

    async def _local_size(self, rel_path: str) -> Optional[int]:
        """Byte size of the workspace copy of ``rel_path``, or None if absent.

        Backend ``stat()`` reports 0 for missing paths, so a 0 answer gets an
        ``is_file`` confirmation — otherwise an empty remote file would be
        recorded as in-sync against a local copy that does not exist.
        """
        if self._backend:
            backend_path = self._backend_path(rel_path)
            try:
                size = await asyncio.to_thread(self._backend.stat, backend_path)
                if size == 0 and not await asyncio.to_thread(
                    self._backend.is_file, backend_path
                ):
                    return None
                return size
            except Exception:
                return None
        local_path = self._workspace_path / rel_path
        try:
            return local_path.stat().st_size if local_path.is_file() else None
        except OSError:
            return None

    async def _workspace_content_sha256(self, rel_path: str) -> Optional[str]:
        """Hash one durable workspace file, or return None when it is absent."""

        if self._backend:
            try:
                content = await asyncio.to_thread(
                    self._backend.read_file,
                    self._backend_path(rel_path),
                    True,
                )
            except FileNotFoundError:
                return None
            if isinstance(content, str):
                content = content.encode("utf-8")
            return hashlib.sha256(content).hexdigest()
        local_path = self._workspace_path / rel_path
        try:
            if not local_path.is_file():
                return None
            return hashlib.sha256(local_path.read_bytes()).hexdigest()
        except FileNotFoundError:
            return None

    def _walk_backend_files(self) -> list[str]:
        """Recursively list files from the remote workspace backend.

        Returns *mount-relative* paths — i.e. with ``mount_subdir`` stripped
        so the rest of the algorithm operates in the mount's coordinate
        system, matching what the cloud-side ``webdav_url`` expects.
        """
        results: list[str] = []
        stack = [self._mount_subdir]
        prefix = f"{self._mount_subdir}/" if self._mount_subdir else ""
        while stack:
            dir_path = stack.pop()
            try:
                entries = self._backend.list_dir(dir_path)  # type: ignore[union-attr]
            except Exception:
                continue
            for entry in entries:
                if entry.endswith("/"):
                    stack.append(entry.rstrip("/"))
                else:
                    if prefix and entry.startswith(prefix):
                        results.append(entry[len(prefix) :])
                    elif not prefix:
                        results.append(entry)
        return results

    async def _walk_backend_files_async(self, *, strict: bool = False) -> list[str]:
        """Level-parallel ``_walk_backend_files`` (same semantics, same
        skip-dir-on-error behavior). One ``list_dir`` per directory is a
        backend round trip — S3 LIST for virtual workspaces, measured 5.5s
        serial for a 16-file tree — so directories of a level are listed
        concurrently under a small semaphore.
        """
        results: list[str] = []
        level = [self._mount_subdir]
        prefix = f"{self._mount_subdir}/" if self._mount_subdir else ""
        sem = asyncio.Semaphore(8)

        async def _list(dir_path: str) -> list[str]:
            async with sem:
                try:
                    return await asyncio.to_thread(
                        self._backend.list_dir,  # type: ignore[union-attr]
                        dir_path,
                    )
                except Exception:
                    if strict:
                        raise
                    return []

        while level:
            listings = await asyncio.gather(*[_list(d) for d in level])
            level = []
            for entries in listings:
                for entry in entries:
                    if entry.endswith("/"):
                        level.append(entry.rstrip("/"))
                    elif prefix and entry.startswith(prefix):
                        results.append(entry[len(prefix) :])
                    elif not prefix:
                        results.append(entry)
        return results

    async def _backend_file_index(self) -> Optional[dict[str, int]]:
        """Mount-relative path → size for every file under the mount, in ONE
        backend round trip — when the backend can do that.

        Capability probe by duck typing: :class:`VirtualWorkspaceBackend`
        exposes ``list_files_with_sizes`` (one recursive object-store list;
        every individual op there is an rclone subprocess spawn, measured
        ~0.3–0.6s each — a per-file stat pass costs seconds, this costs one
        spawn). SSH/remote backends have no such bulk primitive and answer
        ``None``, which keeps them on the per-file path. ``None`` on failure
        too — the caller's fallback re-runs the ordinary walk/stat path and
        surfaces the real error under its own strict rules.
        """
        lister = getattr(self._backend, "list_files_with_sizes", None)
        if lister is None:
            return None
        try:
            raw = await asyncio.to_thread(lister, self._mount_subdir)
        except Exception as e:
            logger.debug("Bulk file index failed (%s); per-file path", e)
            return None
        prefix = f"{self._mount_subdir}/" if self._mount_subdir else ""
        out: dict[str, int] = {}
        for path, size in raw:
            if prefix and path.startswith(prefix):
                out[path[len(prefix) :]] = size
            elif not prefix:
                out[path] = size
        return out

    async def _local_sizes(self, paths: list[str]) -> dict[str, Optional[int]]:
        """``_local_size`` for many paths with bounded concurrency.

        ``_local_size`` never raises (it answers None for anything it cannot
        stat), so the gather needs no exception shaping.
        """
        if not paths:
            return {}
        sem = asyncio.Semaphore(8)

        async def _one(path: str) -> tuple[str, Optional[int]]:
            async with sem:
                return path, await self._local_size(path)

        return dict(await asyncio.gather(*[_one(p) for p in paths]))

    async def _backend_stats(self, paths: list[str]) -> dict[str, object]:
        """Backend ``stat`` for many paths with bounded concurrency.

        Failures are returned as the exception object (not raised) so the
        caller can honor its own strict/lenient per-file policy.
        """
        if not paths:
            return {}
        sem = asyncio.Semaphore(8)

        async def _one(path: str) -> tuple[str, object]:
            async with sem:
                try:
                    return path, await asyncio.to_thread(
                        self._backend.stat,  # type: ignore[union-attr]
                        self._backend_path(path),
                    )
                except Exception as e:
                    return path, e

        return dict(await asyncio.gather(*[_one(p) for p in paths]))

    async def push(
        self,
        *,
        strict: bool = False,
        force: bool = False,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> list[str]:
        """Push locally modified/new files to the cloud.

        When ``strict`` is True, transport-level and per-file errors raise
        instead of being logged and swallowed. This is what the
        ``WorkspaceSyncCoordinator`` uses to enforce the raise-and-block
        policy at turn boundaries.
        """
        pushed: list[str] = []
        try:
            if before_write is not None:
                await before_write()
            await self._ensure_ready()
            if not force:
                await self._seed_remote_state()
            if self._backend:
                pushed = await self._push_via_backend(
                    strict=strict,
                    force=force,
                    before_write=before_write,
                )
            else:
                pushed = await self._push_local(
                    strict=strict,
                    force=force,
                    before_write=before_write,
                )
            if pushed:
                logger.info("Synced %d file(s) to cloud", len(pushed))
        except Exception as e:
            if strict:
                raise
            logger.warning("Push sync failed: %s", e)
        return pushed

    async def _push_via_backend(
        self,
        *,
        strict: bool = False,
        force: bool = False,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> list[str]:
        """Push files from a remote workspace backend. Uses size-based dedup."""
        pushed: list[str] = []
        _t0 = _time.perf_counter()
        # One bulk walk-with-sizes when the backend supports it (virtual/S3:
        # a single rclone spawn replaces the per-dir walk AND every per-file
        # size check). Fallback: level-parallel walk + batched stats.
        index = await self._backend_file_index()
        if index is not None:
            files = list(index)
            stat_results: dict[str, object] = dict(index)
            _n_stats = 0
        else:
            files = await self._walk_backend_files_async(strict=strict)
            # stat() is one round-trip with no payload — read the (possibly
            # large) file only when its size moved. All size-checks run up
            # front with bounded concurrency (measured 2.2s serial for 16
            # files on the virtual/S3 backend). stat() reports 0 for
            # vanished paths, which mismatches any tracked nonzero size and
            # falls through to the read; that read's failure is the per-file
            # skip below. A failed stat re-raises inside the per-file try so
            # the strict/lenient policy stays exactly per-file.
            tracked = [
                p
                for p in files
                if not _should_ignore(p) and self._pushed_sizes.get(p) is not None
            ]
            stat_results = await self._backend_stats(tracked)
            _n_stats = len(tracked)
        _t_walk = _time.perf_counter() - _t0

        for rel_path in files:
            if _should_ignore(rel_path):
                continue
            try:
                prev_size = self._pushed_sizes.get(rel_path)
                if prev_size is not None and not force:
                    cur_size = stat_results.get(rel_path)
                    if isinstance(cur_size, Exception):
                        raise cur_size
                    if cur_size == prev_size:
                        continue
                content = await asyncio.to_thread(
                    self._backend.read_file,  # type: ignore[union-attr]
                    self._backend_path(rel_path),
                    True,
                )
                if isinstance(content, str):
                    content = content.encode("utf-8")

                if not force and prev_size is not None and len(content) == prev_size:
                    continue

                fd, tmp_path = tempfile.mkstemp()
                try:
                    os.write(fd, content)
                    os.close(fd)
                    if before_write is not None:
                        await before_write()
                    remote_dir = str(Path(rel_path).parent)
                    await self._ensure_remote_dirs(
                        remote_dir, before_write=before_write
                    )
                    if before_write is not None:
                        await before_write()
                    await self._upload_file(
                        rel_path, tmp_path, before_write=before_write
                    )
                    self._pushed_sizes[rel_path] = len(content)
                    pushed.append(rel_path)
                finally:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
            except Exception as e:
                if strict:
                    raise
                logger.debug("Push failed for %s: %s", rel_path, e)
        logger.info(
            "push detail: mount=%s walk=%.2fs files=%d stats=%d uploads=%d total=%.2fs",
            self._mount_subdir or "<root>",
            _t_walk,
            len(files),
            _n_stats,
            len(pushed),
            _time.perf_counter() - _t0,
        )
        return pushed

    async def _push_local(
        self,
        *,
        strict: bool = False,
        force: bool = False,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> list[str]:
        """Push files from the local filesystem. Uses mtime-based dedup."""
        pushed: list[str] = []
        for root, _dirs, files in os.walk(self._workspace_path):
            for filename in files:
                local_path = Path(root) / filename
                rel_path = str(local_path.relative_to(self._workspace_path))

                if _should_ignore(rel_path):
                    continue

                mtime = local_path.stat().st_mtime
                prev_mtime = self._local_state.get(rel_path)
                if not force and prev_mtime is not None and mtime <= prev_mtime:
                    continue
                if not force and prev_mtime is None:
                    # First sighting by this instance. A seeded size match
                    # means the cloud already holds this content — record
                    # the mtime so the normal dedup takes over, skip the
                    # upload (fresh-pod full re-upload fix, local mode).
                    seeded = self._pushed_sizes.get(rel_path)
                    if seeded is not None and local_path.stat().st_size == seeded:
                        self._local_state[rel_path] = mtime
                        continue

                try:
                    if before_write is not None:
                        await before_write()
                    remote_dir = str(Path(rel_path).parent)
                    await self._ensure_remote_dirs(
                        remote_dir, before_write=before_write
                    )
                    if before_write is not None:
                        await before_write()
                    await self._upload_file(
                        rel_path, str(local_path), before_write=before_write
                    )
                    self._local_state[rel_path] = mtime
                    pushed.append(rel_path)
                except Exception as e:
                    if strict:
                        raise
                    logger.debug("Push failed for %s: %s", rel_path, e)
        return pushed

    async def pull(
        self,
        *,
        strict: bool = False,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        force_unknown: bool = False,
    ) -> list[str]:
        """Pull remotely modified/new files from the cloud into the workspace.

        When ``strict`` is True, transport-level and per-file errors raise
        instead of being logged and swallowed. Used by the coordinator at
        turn boundaries.
        """
        pulled: list[str] = []
        _t0 = _time.perf_counter()
        _t_list = 0.0
        _n_stats = 0
        try:
            await self._ensure_ready()
            # A v3 resource marker installs the exact set of files committed by
            # the predecessor. Only that durable membership proof authorizes a
            # missing remote path to act as a cloud tombstone. Fresh/legacy
            # pulls start empty and therefore never delete local-only files.
            previously_committed_paths = set(self._remote_known_paths)
            try:
                remote_files = await self._list_remote_tree()
            except Exception:
                if strict:
                    raise
                # Folder doesn't exist yet — nothing to pull
                return pulled
            _t_list = _time.perf_counter() - _t0
            # A successful listing doubles as the dedup seed (the reconcile
            # below records sizes/etags), so a later push needn't re-list.
            self._remote_seeded = True
            observed_remote_paths: set[str] = set()

            # First-sighting local size checks are one backend round trip
            # each (an rclone subprocess spawn on virtual workspaces;
            # measured 1.9s serial / 4.0s under thread contention for 16
            # files) — prefer ONE bulk walk-with-sizes, else resolve them
            # with bounded concurrency.
            _stat_paths = [
                item.get("path", "")
                for item in remote_files
                if item.get("path")
                and not item.get("isdir")
                and not _should_ignore(item.get("path", ""))
                and not self._remote_state.get(item.get("path", ""))
                and isinstance(item.get("size"), int)
                and item.get("size", -1) >= 0
            ]
            _index = await self._backend_file_index() if _stat_paths else None
            if _index is not None:
                _local_sizes: dict[str, Optional[int]] = {
                    p: _index.get(p) for p in _stat_paths
                }
                _n_stats = 1
            else:
                _local_sizes = await self._local_sizes(_stat_paths)
                _n_stats = len(_stat_paths)

            for item in remote_files:
                path = item.get("path", "")
                if not path or item.get("isdir"):
                    continue
                if _should_ignore(path):
                    continue
                observed_remote_paths.add(path)

                etag = item.get("etag", "")
                prev_etag = self._remote_state.get(path)
                if (
                    prev_etag
                    and etag == prev_etag
                    and (not force_unknown or path in self._remote_content_hashes)
                ):
                    continue

                if not prev_etag and not force_unknown:
                    # First sighting by this instance (fresh pod). If the
                    # workspace copy already matches by size, record it as
                    # in-sync instead of re-downloading the whole mount on
                    # every pod recycle — which also clobbered any unpushed
                    # local edit with a stale same-size cloud copy. Size is
                    # the same heuristic push dedup has always used.
                    size = item.get("size")
                    if isinstance(size, int) and size >= 0:
                        local_size = _local_sizes.get(path)
                        if local_size == size:
                            self._remote_state[path] = etag
                            self._pushed_sizes[path] = size
                            if not self._backend:
                                try:
                                    self._local_state[path] = (
                                        (self._workspace_path / path).stat().st_mtime
                                    )
                                except OSError:
                                    pass
                            continue

                try:
                    if self._backend:
                        changed = await self._pull_file_to_backend(
                            path,
                            etag,
                            before_write=before_write,
                            committed_sha256=(
                                self._remote_content_hashes.get(path)
                                if force_unknown
                                else None
                            ),
                        )
                    else:
                        changed = await self._pull_file_local(
                            path,
                            etag,
                            before_write=before_write,
                            committed_sha256=(
                                self._remote_content_hashes.get(path)
                                if force_unknown
                                else None
                            ),
                        )
                    if changed:
                        pulled.append(path)
                except Exception as e:
                    if strict:
                        raise
                    logger.debug("Pull failed for %s: %s", path, e)

            deleted = 0
            if force_unknown:
                for path in sorted(previously_committed_paths - observed_remote_paths):
                    try:
                        expected_digest = self._remote_content_hashes.get(path)
                        local_digest = await self._workspace_content_sha256(path)
                        self._remote_state.pop(path, None)
                        self._pushed_sizes.pop(path, None)
                        # The marker says what the remote held at commit time.
                        # If an idle process wrote a different local value after
                        # that marker, preserve it as a new local-only delta.
                        # Only the unchanged stale copy is deleted in response
                        # to the cloud tombstone. No digest means no authority
                        # to delete anything.
                        if expected_digest is None or (
                            local_digest is not None and local_digest != expected_digest
                        ):
                            continue
                        if before_write is not None:
                            await before_write()
                        removed = False
                        if self._backend:
                            removed = bool(
                                await asyncio.to_thread(
                                    self._backend.delete_file,
                                    self._backend_path(path),
                                )
                            )
                        else:
                            local_path = self._workspace_path / path
                            removed = local_path.exists()
                            local_path.unlink(missing_ok=True)
                        if removed:
                            pulled.append(path)
                            deleted += 1
                    except Exception as e:
                        if strict:
                            raise
                        logger.debug("Cloud tombstone apply failed for %s: %s", path, e)

            if pulled:
                logger.info("Pulled %d file(s) from cloud", len(pulled))
            self._remote_known_paths = observed_remote_paths
            self._remote_content_hashes = {
                path: digest
                for path, digest in self._remote_content_hashes.items()
                if path in observed_remote_paths
            }
            logger.info(
                "pull detail: mount=%s list=%.2fs files=%d reconcile=%.2fs "
                "stats=%d downloads=%d deletes=%d total=%.2fs",
                self._mount_subdir or "<root>",
                _t_list,
                len(remote_files),
                _time.perf_counter() - _t0 - _t_list,
                _n_stats,
                len(pulled) - deleted,
                deleted,
                _time.perf_counter() - _t0,
            )
        except Exception as e:
            if strict:
                raise
            logger.warning("Pull sync failed: %s", e)
        return pulled

    async def _pull_file_to_backend(
        self,
        path: str,
        etag: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        committed_sha256: Optional[str] = None,
    ) -> bool:
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        try:
            await self._download_file(path, tmp_path)
            with open(tmp_path, "rb") as f:
                content = f.read()
            remote_digest = hashlib.sha256(content).hexdigest()
            preserve_local = False
            if committed_sha256 and remote_digest == committed_sha256:
                try:
                    local_content = await asyncio.to_thread(
                        self._backend.read_file,  # type: ignore[union-attr]
                        self._backend_path(path),
                        True,
                    )
                    if isinstance(local_content, str):
                        local_content = local_content.encode("utf-8")
                    preserve_local = (
                        hashlib.sha256(local_content).hexdigest() != committed_sha256
                    )
                except FileNotFoundError:
                    preserve_local = False
            if before_write is not None:
                await before_write()
            # Never stream directly over the durable target: a pod/transport
            # death mid-SFTP write would leave a partial file with no armed
            # generation.  Stage under the ignored management tree, then use
            # the backend's replace/move primitive (POSIX rename for sandbox;
            # atomic destination PUT for the object-store backend).
            if not preserve_local:
                staged_path = f".srw/cloud-pull/{uuid.uuid4().hex}.tmp"
                try:
                    if await asyncio.to_thread(
                        self._backend.is_dir,  # type: ignore[union-attr]
                        self._backend_path(path),
                    ):
                        raise CloudSyncMarkerError(
                            f"cloud pull target is a directory: {path}"
                        )
                    await asyncio.to_thread(
                        self._backend.write_file,  # type: ignore[union-attr]
                        staged_path,
                        content,
                    )
                    if before_write is not None:
                        await before_write()
                    await asyncio.to_thread(
                        self._backend.replace_file,  # type: ignore[union-attr]
                        staged_path,
                        self._backend_path(path),
                    )
                    installed = await asyncio.to_thread(
                        self._backend.read_file,  # type: ignore[union-attr]
                        self._backend_path(path),
                        True,
                    )
                    if isinstance(installed, str):
                        installed = installed.encode("utf-8")
                    if hashlib.sha256(installed).hexdigest() != remote_digest:
                        raise CloudSyncMarkerError(
                            f"staged cloud pull verification failed for {path}"
                        )
                finally:
                    try:
                        await asyncio.to_thread(
                            self._backend.delete_file,  # type: ignore[union-attr]
                            staged_path,
                        )
                    except Exception:
                        pass
            self._remote_state[path] = etag
            self._remote_content_hashes[path] = remote_digest
            self._pushed_sizes[path] = len(content)
            return not preserve_local
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    async def _pull_file_local(
        self,
        path: str,
        etag: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        committed_sha256: Optional[str] = None,
    ) -> bool:
        local_path = self._workspace_path / path
        if before_write is not None:
            await before_write()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        fd, staged_path = tempfile.mkstemp(
            prefix=f".{local_path.name}.srw-pull-", dir=local_path.parent
        )
        os.close(fd)
        try:
            await self._download_file(path, staged_path)
            content = Path(staged_path).read_bytes()
            remote_digest = hashlib.sha256(content).hexdigest()
            preserve_local = False
            if (
                committed_sha256
                and remote_digest == committed_sha256
                and local_path.is_file()
            ):
                preserve_local = (
                    hashlib.sha256(local_path.read_bytes()).hexdigest()
                    != committed_sha256
                )
            if not preserve_local:
                if before_write is not None:
                    await before_write()
                os.replace(staged_path, local_path)
        finally:
            try:
                os.unlink(staged_path)
            except OSError:
                pass
        self._remote_state[path] = etag
        self._remote_content_hashes[path] = remote_digest
        self._pushed_sizes[path] = len(content)
        self._local_state[path] = local_path.stat().st_mtime
        return not preserve_local

    async def full_sync(self, *, strict: bool = False) -> tuple[list[str], list[str]]:
        """Push first, then pull."""
        pushed = await self.push(strict=strict)
        pulled = await self.pull(strict=strict)
        return pushed, pulled

    # ------------------------------------------------------------ Poll lifecycle

    async def start_background_poll(self) -> None:
        """Start the background pull loop."""
        if self._running:
            return
        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("Started cloud sync polling (interval=%ss)", self._poll_interval)

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._poll_interval)
                if not self._running:
                    break
                await self.pull()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Sync poll error: %s", e)

    async def stop(self) -> None:
        """Stop the background pull loop."""
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        logger.info("Stopped cloud sync polling")
