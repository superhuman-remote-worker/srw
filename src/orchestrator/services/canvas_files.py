"""Authorized workspace-file materialization for Dynamic Canvas.

The gateway is Canvas-scoped: callers provide an already authorized thread
plus the path selected in the authoritative Canvas row. It is not a general
thread filesystem browser. Its one other reader is the owner's download of a
single named file (``read_download``), which never renders the bytes.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import os
import posixpath
import secrets
import shutil
import stat
import threading
import warnings
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

try:
    import magic
except ImportError:  # pragma: no cover - deployment dependency guard
    magic = None  # type: ignore[assignment]

try:
    from PIL import Image, UnidentifiedImageError
except ImportError:  # pragma: no cover - deployment dependency guard
    Image = None  # type: ignore[assignment]
    UnidentifiedImageError = OSError  # type: ignore[assignment,misc]

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.canvas import (
    CanvasRenderer,
    CanvasRecord,
    WorkspaceFileSource,
)
from orchestrator.services.canvas_ssh import (
    PINNED_SSH_TRANSPORT_POOL,
    CanvasSSHError,
    PinnedSFTPPool,
    PinnedSSHClient,
    asyncssh,
    bound_workspace_generation,
    resolve_remote_workspace_target,
    workspace_metadata,
)
from orchestrator.services.virtual_workspace import virtual_workspace_rclone_spec
from orchestrator.services.workspace_binding import virtual_thread_backing_id

logger = logging.getLogger(__name__)

MAX_TEXT_BYTES = int(os.getenv("CANVAS_MAX_TEXT_BYTES", str(2 * 1024 * 1024)))
MAX_IMAGE_BYTES = int(os.getenv("CANVAS_MAX_IMAGE_BYTES", str(25 * 1024 * 1024)))
MAX_OFFICE_BYTES = int(os.getenv("CANVAS_MAX_OFFICE_BYTES", str(25 * 1024 * 1024)))
MAX_FILE_BYTES = int(os.getenv("CANVAS_MAX_FILE_BYTES", str(50 * 1024 * 1024)))
MAX_IMAGE_PIXELS = int(os.getenv("CANVAS_MAX_IMAGE_PIXELS", "40000000"))
MAX_IMAGE_FRAMES = int(os.getenv("CANVAS_MAX_IMAGE_FRAMES", "1000"))
MAX_REMOTE_MATERIALIZATIONS = int(os.getenv("CANVAS_MAX_REMOTE_MATERIALIZATIONS", "8"))
MAX_VIRTUAL_MATERIALIZATIONS = int(
    os.getenv("CANVAS_MAX_VIRTUAL_MATERIALIZATIONS", "8")
)
MAX_CANVAS_VALIDATIONS = int(os.getenv("CANVAS_MAX_VALIDATIONS", "4"))
MAX_FULL_MATERIALIZATIONS = int(os.getenv("CANVAS_MAX_FULL_MATERIALIZATIONS", "8"))
MAX_BUFFERED_RESPONSES = int(os.getenv("CANVAS_MAX_BUFFERED_RESPONSES", "8"))
MATERIALIZATION_QUEUE_TIMEOUT = float(
    os.getenv("CANVAS_MATERIALIZATION_QUEUE_TIMEOUT", "5")
)

_REMOTE_SEMAPHORE = asyncio.Semaphore(max(1, MAX_REMOTE_MATERIALIZATIONS))
_VIRTUAL_SEMAPHORE = asyncio.Semaphore(max(1, MAX_VIRTUAL_MATERIALIZATIONS))
_VALIDATION_SEMAPHORE = asyncio.Semaphore(max(1, MAX_CANVAS_VALIDATIONS))
_FULL_MATERIALIZATION_SEMAPHORE = asyncio.Semaphore(max(1, MAX_FULL_MATERIALIZATIONS))
_RESPONSE_BUFFER_SEMAPHORE = asyncio.Semaphore(max(1, MAX_BUFFERED_RESPONSES))
REMOTE_WORKSPACE_ROOT = "/home/agent-host/workspace"
# Compatibility aliases retained for focused Slice-1 tests and callers. The
# implementations now live in ``canvas_ssh`` and share authenticated transports
# with live-app direct channels.
_PinnedSSHClient = PinnedSSHClient
_PinnedSFTPPool = PinnedSFTPPool

_TEXT_EXTENSIONS = frozenset(
    {
        ".c",
        ".cc",
        ".conf",
        ".cpp",
        ".css",
        ".csv",
        ".go",
        ".h",
        ".hpp",
        ".ini",
        ".java",
        ".js",
        ".json",
        ".jsx",
        ".log",
        ".md",
        ".mjs",
        ".py",
        ".rb",
        ".rs",
        ".sh",
        ".sql",
        ".tex",
        ".toml",
        ".ts",
        ".tsx",
        ".txt",
        ".xml",
        ".yaml",
        ".yml",
    }
)
_HTML_EXTENSIONS = frozenset({".html", ".htm"})
_IMAGE_EXTENSIONS = frozenset({".gif", ".jpeg", ".jpg", ".png", ".webp"})
_OFFICE_MEDIA_BY_EXTENSION = {
    ".docx": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ),
    ".xlsx": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ".pptx": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ),
    ".odt": "application/vnd.oasis.opendocument.text",
    ".ods": "application/vnd.oasis.opendocument.spreadsheet",
    ".odp": "application/vnd.oasis.opendocument.presentation",
}
_OFFICE_EXTENSIONS = frozenset(_OFFICE_MEDIA_BY_EXTENSION)
_OFFICE_VENDOR_MEDIA = frozenset(_OFFICE_MEDIA_BY_EXTENSION.values())
_IMAGE_MEDIA = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "WEBP": "image/webp",
    "GIF": "image/gif",
}
_TEXT_APPLICATION_MEDIA = frozenset(
    {
        "application/javascript",
        "application/json",
        "application/x-empty",
        "application/x-javascript",
        "application/xml",
    }
)


class CanvasFileError(Exception):
    """Typed gateway failure mapped by the HTTP and tool adapters."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class CanvasResponseLease:
    """One bounded in-memory response slot, released after final ASGI send."""

    def __init__(self, semaphore: asyncio.Semaphore):
        self._semaphore = semaphore
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._semaphore.release()


async def acquire_canvas_response_lease() -> CanvasResponseLease:
    """Reserve response capacity before reading bytes into application memory."""

    try:
        await asyncio.wait_for(
            _RESPONSE_BUFFER_SEMAPHORE.acquire(),
            timeout=max(0.1, MATERIALIZATION_QUEUE_TIMEOUT),
        )
    except TimeoutError as exc:
        raise CanvasFileError(
            503,
            "canvas_capacity_exhausted",
            "Canvas response capacity is currently exhausted",
        ) from exc
    return CanvasResponseLease(_RESPONSE_BUFFER_SEMAPHORE)


@asynccontextmanager
async def _materialization_slot(semaphore: asyncio.Semaphore):
    """Acquire a backend slot with a bounded queue wait."""

    acquired = False
    try:
        try:
            await asyncio.wait_for(
                semaphore.acquire(),
                timeout=max(0.1, MATERIALIZATION_QUEUE_TIMEOUT),
            )
            acquired = True
        except TimeoutError as exc:
            raise CanvasFileError(
                503,
                "canvas_capacity_exhausted",
                "Canvas file materialization capacity is currently exhausted",
            ) from exc
        yield
    finally:
        if acquired:
            semaphore.release()


async def _wait_blocking_task_settled(task: asyncio.Task[Any]) -> None:
    """Delay slot release until an already-started worker actually exits."""

    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # A repeated caller cancellation must not orphan the worker and
            # free its capacity lease early.
            continue
        except Exception:
            break
    if task.done() and not task.cancelled():
        with suppress(Exception):
            task.exception()


async def _run_blocking(
    function: Callable[..., Any],
    *args: Any,
    timeout: float | None = None,
    cancellation_callback: Callable[[], None] | None = None,
) -> Any:
    """Run blocking work without releasing capacity on timeout/cancellation."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        if timeout is None:
            return await asyncio.shield(task)
        async with asyncio.timeout(timeout):
            return await asyncio.shield(task)
    except (asyncio.CancelledError, TimeoutError):
        if cancellation_callback is not None:
            cancellation_callback()
        await _wait_blocking_task_settled(task)
        raise


@dataclass(frozen=True, slots=True)
class RawWorkspaceFile:
    data: bytes
    last_modified: datetime | None = None


@dataclass(frozen=True, slots=True)
class ValidatedCanvasFile:
    path: str
    data: bytes
    media_type: str
    renderer: Literal["markdown", "text", "html", "html-interactive", "image", "office"]
    source_version: str
    last_modified: datetime | None

    @property
    def filename(self) -> str:
        return PurePosixPath(self.path).name


RemoteLoader = Callable[[dict[str, Any], str, UUID], Awaitable[RawWorkspaceFile]]
VirtualLoader = Callable[[dict[str, Any], str, UUID], Awaitable[RawWorkspaceFile]]
WorkspaceWriter = Callable[[dict[str, Any], str, UUID, bytes], Awaitable[None]]
ThreadLoader = Callable[[str], Awaitable[dict[str, Any] | None]]
GenerationResolver = Callable[[], Awaitable[dict[str, Any]]]


def _metadata(thread: dict[str, Any]) -> dict[str, Any]:
    return workspace_metadata(thread)


def current_workspace_generation(thread: dict[str, Any]) -> UUID:
    try:
        return bound_workspace_generation(thread)
    except CanvasSSHError as exc:
        raise CanvasFileError(
            exc.status_code,
            exc.code,
            exc.message,
        ) from exc


def canonical_workspace_path(path: str) -> str:
    """Return the one accepted relative POSIX spelling for a Canvas path."""

    if not isinstance(path, str) or not path or len(path) > 4096:
        raise CanvasFileError(422, "invalid_canvas_path", "File path is invalid")
    if (
        any(ord(char) < 32 or ord(char) == 127 for char in path)
        or "\\" in path
        or path.startswith("/")
    ):
        raise CanvasFileError(422, "invalid_canvas_path", "File path is invalid")
    pure = PurePosixPath(path)
    if any(part in {"", ".", ".."} for part in pure.parts):
        raise CanvasFileError(422, "invalid_canvas_path", "File path is invalid")
    canonical = str(pure)
    if canonical != path or canonical in {"", "."}:
        raise CanvasFileError(
            422,
            "invalid_canvas_path",
            "File path must use canonical relative POSIX syntax",
        )
    return canonical


def _workspace_read_limit(path: str) -> int:
    """Bound transport reads by the largest renderer this path can reach."""

    extension = PurePosixPath(path).suffix.lower()
    if extension in _TEXT_EXTENSIONS or extension in _HTML_EXTENSIONS:
        return min(MAX_TEXT_BYTES, MAX_FILE_BYTES)
    if extension in _OFFICE_EXTENSIONS:
        return min(MAX_OFFICE_BYTES, MAX_FILE_BYTES)
    if extension in _IMAGE_EXTENSIONS:
        return min(MAX_IMAGE_BYTES, MAX_FILE_BYTES)
    # Known raster extensions use the image ceiling. Unknown extensions can
    # only possibly validate as raster and therefore never need the larger
    # historical absolute ceiling either.
    return min(MAX_IMAGE_BYTES, MAX_FILE_BYTES)


def _backend(thread: dict[str, Any]) -> str | None:
    config = _metadata(thread).get("config_override") or {}
    if not isinstance(config, dict):
        return None
    workspace = config.get("workspace") or {}
    return workspace.get("backend") if isinstance(workspace, dict) else None


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _magic_media_type(data: bytes) -> str:
    if magic is None:
        raise CanvasFileError(
            503,
            "canvas_validator_unavailable",
            "Server byte-type detection is unavailable",
        )
    try:
        value = str(magic.from_buffer(data[: 256 * 1024], mime=True)).split(";", 1)[0]
    except Exception as exc:
        raise CanvasFileError(
            422, "unsupported_canvas_file", "File type could not be detected"
        ) from exc
    return value.lower().strip()


def _validate_image(data: bytes) -> tuple[str, int]:
    if Image is None:
        raise CanvasFileError(
            503,
            "canvas_validator_unavailable",
            "Server image validation is unavailable",
        )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with Image.open(io.BytesIO(data)) as image:
                image_format = str(image.format or "").upper()
                pixels = 0
                frame_count = int(getattr(image, "n_frames", 1))
                if frame_count > MAX_IMAGE_FRAMES:
                    raise CanvasFileError(
                        413,
                        "canvas_image_too_large",
                        f"Animated image exceeds the {MAX_IMAGE_FRAMES} frame limit",
                    )
                for frame in range(frame_count):
                    image.seek(frame)
                    width, height = image.size
                    pixels += width * height
                    if pixels > MAX_IMAGE_PIXELS:
                        raise CanvasFileError(
                            413,
                            "canvas_image_too_large",
                            f"Decoded image exceeds the {MAX_IMAGE_PIXELS} pixel limit",
                        )
                image.verify()
    except CanvasFileError:
        raise
    except (UnidentifiedImageError, OSError, ValueError, Warning) as exc:
        raise CanvasFileError(
            422, "invalid_canvas_image", "Image bytes are invalid or unsafe"
        ) from exc
    media_type = _IMAGE_MEDIA.get(image_format)
    if media_type is None:
        raise CanvasFileError(
            422,
            "unsupported_canvas_file",
            "Only PNG, JPEG, WebP, and GIF are supported",
        )
    if pixels > MAX_IMAGE_PIXELS:
        raise CanvasFileError(
            413,
            "canvas_image_too_large",
            f"Decoded image exceeds the {MAX_IMAGE_PIXELS} pixel limit",
        )
    return media_type, pixels


def _office_media_type(extension: str, detected: str) -> str | None:
    """Resolve one exact Office extension/MIME pair, rejecting cross-format lies."""

    expected = _OFFICE_MEDIA_BY_EXTENSION.get(extension)
    if expected is not None:
        if detected not in {expected, "application/zip"}:
            raise CanvasFileError(
                422,
                "mime_renderer_mismatch",
                "Office extension does not match the detected document bytes",
            )
        return expected
    if detected in _OFFICE_VENDOR_MEDIA:
        raise CanvasFileError(
            422,
            "mime_renderer_mismatch",
            "Office document bytes require a matching supported Office extension",
        )
    return None


def _validated_office_file(
    path: str,
    data: bytes,
    *,
    detected: str,
    expected_source_version: str | None = None,
) -> ValidatedCanvasFile:
    """Validate only the bounded binary Office branch (never the UTF-8 path)."""

    extension = PurePosixPath(path).suffix.lower()
    media_type = _office_media_type(extension, detected)
    if media_type is None:
        raise CanvasFileError(
            422,
            "unsupported_canvas_file",
            "File is not a supported Office document",
        )
    if len(data) > MAX_OFFICE_BYTES:
        raise CanvasFileError(
            413, "canvas_file_too_large", "Office document is too large"
        )
    source_version = _sha256(data)
    if (
        expected_source_version is not None
        and source_version != expected_source_version
    ):
        raise CanvasFileError(
            409,
            "source_changed",
            "Workspace bytes changed; republish the Canvas to adopt them",
        )
    return ValidatedCanvasFile(
        path=path,
        data=data,
        media_type=media_type,
        renderer="office",
        source_version=source_version,
        last_modified=None,
    )


def validate_canvas_bytes(
    path: str,
    data: bytes,
    *,
    requested_renderer: CanvasRenderer = "auto",
    alt_text: str | None = None,
) -> ValidatedCanvasFile:
    """Detect and validate the closed Slice-1 renderer compatibility set."""

    path = canonical_workspace_path(path)
    if len(data) > MAX_FILE_BYTES:
        raise CanvasFileError(413, "canvas_file_too_large", "File is too large")

    extension = PurePosixPath(path).suffix.lower()
    detected = _magic_media_type(data)
    renderer: Literal["markdown", "text", "html", "html-interactive", "image", "office"]
    media_type: str

    office_media_type = _office_media_type(extension, detected)
    if office_media_type is not None:
        office = _validated_office_file(path, data, detected=detected)
        renderer = office.renderer
        media_type = office.media_type
    elif detected.startswith("image/"):
        if len(data) > MAX_IMAGE_BYTES:
            raise CanvasFileError(
                413, "canvas_file_too_large", "Encoded image is too large"
            )
        media_type, _ = _validate_image(data)
        renderer = "image"
        if not alt_text or len(alt_text.strip()) < 3:
            raise CanvasFileError(
                422,
                "canvas_alt_text_required",
                "Raster images require meaningful alt text",
            )
    else:
        if len(data) > MAX_TEXT_BYTES:
            raise CanvasFileError(
                413, "canvas_file_too_large", "Text file is too large"
            )
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CanvasFileError(
                422,
                "unsupported_canvas_file",
                "Canvas text files must be valid UTF-8",
            ) from exc
        if "\x00" in text:
            raise CanvasFileError(
                422, "unsupported_canvas_file", "Binary content is not supported"
            )
        textish = detected.startswith("text/") or detected in _TEXT_APPLICATION_MEDIA
        if extension in _HTML_EXTENSIONS:
            if not textish:
                raise CanvasFileError(
                    422, "mime_renderer_mismatch", "HTML extension does not match bytes"
                )
            renderer, media_type = "html", "text/html"
        elif extension == ".md":
            if not textish:
                raise CanvasFileError(
                    422,
                    "mime_renderer_mismatch",
                    "Markdown extension does not match bytes",
                )
            renderer, media_type = "markdown", "text/markdown"
        elif extension in _TEXT_EXTENSIONS:
            if not textish:
                raise CanvasFileError(
                    422, "mime_renderer_mismatch", "Text extension does not match bytes"
                )
            renderer, media_type = "text", "text/plain; charset=utf-8"
        else:
            raise CanvasFileError(
                422,
                "unsupported_canvas_file",
                "File extension is not supported by a Slice 1 renderer",
            )

    compatible: dict[str, set[str]] = {
        "markdown": {"markdown", "text"},
        "text": {"text"},
        "html": {"html", "html-interactive", "text"},
        "image": {"image"},
        "office": {"office"},
    }
    if requested_renderer != "auto":
        if requested_renderer not in compatible[renderer]:
            raise CanvasFileError(
                422,
                "mime_renderer_mismatch",
                f"Renderer {requested_renderer!r} is incompatible with this file",
            )
        renderer = requested_renderer  # type: ignore[assignment]
        if renderer == "text":
            media_type = "text/plain; charset=utf-8"

    return ValidatedCanvasFile(
        path=path,
        data=data,
        media_type=media_type,
        renderer=renderer,
        source_version=_sha256(data),
        last_modified=None,
    )


def _validate_materialized_bytes(
    path: str,
    data: bytes,
    requested_renderer: CanvasRenderer,
    alt_text: str | None,
    expected_source_version: str | None,
) -> ValidatedCanvasFile:
    """CPU-bound hash/libmagic/Pillow work, always called off the event loop."""

    if expected_source_version is not None and _sha256(data) != expected_source_version:
        raise CanvasFileError(
            409,
            "source_changed",
            "Workspace bytes changed; republish the Canvas to adopt them",
        )
    return validate_canvas_bytes(
        path,
        data,
        requested_renderer=requested_renderer,
        alt_text=alt_text,
    )


async def _validate_materialized_bytes_async(
    path: str,
    data: bytes,
    requested_renderer: CanvasRenderer,
    alt_text: str | None,
    expected_source_version: str | None,
) -> ValidatedCanvasFile:
    async with _materialization_slot(_VALIDATION_SEMAPHORE):
        return await _run_blocking(
            _validate_materialized_bytes,
            path,
            data,
            requested_renderer,
            alt_text,
            expected_source_version,
        )


def _validate_materialized_office_bytes(
    path: str,
    data: bytes,
    expected_source_version: str | None,
) -> ValidatedCanvasFile:
    """CPU-bound Office-only magic/hash validation for the WOPI read path."""

    if len(data) > MAX_FILE_BYTES:
        raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
    return _validated_office_file(
        path,
        data,
        detected=_magic_media_type(data),
        expected_source_version=expected_source_version,
    )


async def _validate_materialized_office_bytes_async(
    path: str,
    data: bytes,
    expected_source_version: str | None,
) -> ValidatedCanvasFile:
    async with _materialization_slot(_VALIDATION_SEMAPHORE):
        return await _run_blocking(
            _validate_materialized_office_bytes,
            path,
            data,
            expected_source_version,
        )


_SFTP_POOL = _PinnedSFTPPool(transport_pool=PINNED_SSH_TRANSPORT_POOL)


async def _evict_sftp_generation(thread_id: str, generation: UUID) -> None:
    """Evict only a retired generation, with legacy test-pool compatibility."""

    evict_generation = getattr(_SFTP_POOL, "evict_generation", None)
    if callable(evict_generation):
        await evict_generation(thread_id, generation)
    else:  # pragma: no cover - compatibility for injected pre-Slice-3 test pools
        await _SFTP_POOL.evict_thread(thread_id)


async def _read_sftp_workspace_file(
    sftp: Any, *, root: str, path: str, read_limit: int | None = None
) -> RawWorkspaceFile:
    """Read one regular file while rejecting symlinks in root and source paths."""

    declared_root = posixpath.normpath(root)
    if not declared_root.startswith("/"):
        raise CanvasFileError(409, "workspace_unavailable", "Workspace root is invalid")
    root_cursor = "/"
    for component in PurePosixPath(declared_root).parts[1:]:
        root_cursor = posixpath.join(root_cursor, component)
        root_attrs = await sftp.lstat(root_cursor)
        permissions = root_attrs.permissions
        if permissions is None or stat.S_ISLNK(permissions):
            raise CanvasFileError(
                422,
                "canvas_symlink_rejected",
                "Workspace root may not contain symlinks",
            )
        if not stat.S_ISDIR(permissions):
            raise CanvasFileError(
                409, "workspace_unavailable", "Workspace root is invalid"
            )
    canonical_root = await sftp.realpath(declared_root)
    if canonical_root != declared_root:
        raise CanvasFileError(
            422,
            "canvas_symlink_rejected",
            "Workspace root may not contain symlinks",
        )

    path_parts = PurePosixPath(path).parts
    current = canonical_root.rstrip("/")
    attrs = None
    for index, component in enumerate(path_parts):
        current = posixpath.join(current, component)
        attrs = await sftp.lstat(current)
        permissions = attrs.permissions
        if permissions is None or stat.S_ISLNK(permissions):
            raise CanvasFileError(
                422,
                "canvas_symlink_rejected",
                "Canvas paths may not contain symlinks",
            )
        if index < len(path_parts) - 1:
            if not stat.S_ISDIR(permissions):
                raise CanvasFileError(404, "canvas_file_not_found", "File not found")
        elif not stat.S_ISREG(permissions):
            raise CanvasFileError(
                422,
                "canvas_regular_file_required",
                "Canvas sources must be regular files",
            )
        resolved = await sftp.realpath(current)
        if resolved != posixpath.normpath(current):
            raise CanvasFileError(
                422,
                "canvas_symlink_rejected",
                "Canvas paths may not contain symlinks",
            )

    if attrs is None:
        raise CanvasFileError(404, "canvas_file_not_found", "File not found")
    if read_limit is None:
        read_limit = _workspace_read_limit(path)
    if attrs.size is not None and attrs.size > read_limit:
        raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
    async with sftp.open(current, "rb") as remote_file:
        data = await remote_file.read(read_limit + 1)
    if len(data) > read_limit:
        raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
    modified = None
    if attrs.mtime is not None:
        modified = datetime.fromtimestamp(attrs.mtime, tz=timezone.utc)
    return RawWorkspaceFile(bytes(data), modified)


async def _write_sftp_workspace_file(
    sftp: Any, *, root: str, path: str, data: bytes
) -> None:
    """Replace one already-validated regular file via same-directory rename."""

    # Re-run the complete root/component/symlink validation immediately before
    # opening the temporary file. This also proves the destination still exists
    # as a regular file. SFTP cannot make the remaining check/rename interval as
    # strong as openat2; that limitation is part of Canvas's best-effort claim.
    await _read_sftp_workspace_file(sftp, root=root, path=path)
    destination = posixpath.join(posixpath.normpath(root), path)
    parent = posixpath.dirname(destination)
    filename = posixpath.basename(destination)
    temporary = posixpath.join(
        parent, f".{filename}.srw-canvas-{secrets.token_hex(12)}.tmp"
    )
    created = False
    try:
        async with sftp.open(temporary, "xb") as remote_file:
            created = True
            await remote_file.write(data)
        destination_attrs = await sftp.lstat(destination)
        permissions = destination_attrs.permissions
        if permissions is None or stat.S_ISLNK(permissions):
            raise CanvasFileError(
                422,
                "canvas_symlink_rejected",
                "Canvas paths may not contain symlinks",
            )
        if not stat.S_ISREG(permissions):
            raise CanvasFileError(
                422,
                "canvas_regular_file_required",
                "Canvas sources must be regular files",
            )
        # Replacing through rename must not silently remove executable or other
        # existing permission bits from editable code files.
        await sftp.chmod(temporary, stat.S_IMODE(permissions))
        # OpenSSH exposes posix-rename, which replaces the destination atomically
        # on the workspace filesystem. Fail closed if a future SFTP server does
        # not support that extension instead of degrading to copy/truncate.
        await sftp.posix_rename(temporary, destination)
        created = False
    finally:
        if created:
            with suppress(Exception):
                await sftp.remove(temporary)


def _remote_workspace_target(
    thread: dict[str, Any], expected_generation: UUID
) -> tuple[str, int, str, str]:
    """Resolve a ready endpoint only when it is paired to the same binding."""
    try:
        target = resolve_remote_workspace_target(thread, expected_generation)
    except CanvasSSHError as exc:
        raise CanvasFileError(
            exc.status_code,
            exc.code,
            exc.message,
        ) from exc
    return target.host, target.port, REMOTE_WORKSPACE_ROOT, target.fingerprint


def _require_same_remote_workspace(
    thread: dict[str, Any],
    expected_generation: UUID,
    expected_target: tuple[str, int, str, str],
) -> None:
    """Fail if either the binding or its paired endpoint changed."""

    current_target = _remote_workspace_target(thread, expected_generation)
    if current_target != expected_target:
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "The active workspace endpoint changed during materialization",
        )


async def _read_remote_default(
    thread: dict[str, Any],
    path: str,
    expected_generation: UUID,
    *,
    generation_resolver: GenerationResolver | None = None,
    read_limit: int | None = None,
) -> RawWorkspaceFile:
    if asyncssh is None:
        raise CanvasFileError(
            503, "workspace_unavailable", "Pinned SSH transport is unavailable"
        )
    if current_workspace_generation(thread) != expected_generation:
        if asyncssh is not None:
            await _evict_sftp_generation(
                str(thread.get("id") or ""), expected_generation
            )
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    target = _remote_workspace_target(thread, expected_generation)
    host, port, root, fingerprint = target
    key_path = resolve_ssh_key_path()
    if not key_path:
        raise CanvasFileError(
            503, "workspace_unavailable", "Workspace SSH key is not configured"
        )

    try:
        async with _materialization_slot(_REMOTE_SEMAPHORE), asyncio.timeout(45):
            async with _SFTP_POOL.checkout(
                thread_id=str(thread.get("id") or ""),
                generation=expected_generation,
                host=str(host),
                port=port,
                fingerprint=fingerprint,
                key_path=key_path,
            ) as sftp:
                # Revalidate after waiting for the pooled entry lock. A
                # generation rotation actively evicts all older channels.
                authoritative = (
                    await generation_resolver()
                    if generation_resolver is not None
                    else thread
                )
                try:
                    _require_same_remote_workspace(
                        authoritative, expected_generation, target
                    )
                except CanvasFileError:
                    await _evict_sftp_generation(
                        str(thread.get("id") or ""), expected_generation
                    )
                    raise
                result = await _read_sftp_workspace_file(
                    sftp, root=root, path=path, read_limit=read_limit
                )
                authoritative = (
                    await generation_resolver()
                    if generation_resolver is not None
                    else thread
                )
                try:
                    _require_same_remote_workspace(
                        authoritative, expected_generation, target
                    )
                except CanvasFileError:
                    await _evict_sftp_generation(
                        str(thread.get("id") or ""), expected_generation
                    )
                    raise
                return result
    except CanvasFileError:
        raise
    except (FileNotFoundError, asyncssh.SFTPNoSuchFile) as exc:
        raise CanvasFileError(404, "canvas_file_not_found", "File not found") from exc
    except Exception as exc:
        logger.warning(
            "Canvas pinned SFTP read failed for thread %s: %s", thread.get("id"), exc
        )
        raise CanvasFileError(
            409, "workspace_unavailable", "Workspace file could not be read"
        ) from exc


async def _write_remote_default(
    thread: dict[str, Any],
    path: str,
    expected_generation: UUID,
    data: bytes,
    *,
    generation_resolver: GenerationResolver | None = None,
) -> None:
    """Write through the same pinned, generation-bound SFTP transport as reads."""

    if asyncssh is None:
        raise CanvasFileError(
            503, "workspace_unavailable", "Pinned SSH transport is unavailable"
        )
    if current_workspace_generation(thread) != expected_generation:
        await _evict_sftp_generation(str(thread.get("id") or ""), expected_generation)
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    target = _remote_workspace_target(thread, expected_generation)
    host, port, root, fingerprint = target
    key_path = resolve_ssh_key_path()
    if not key_path:
        raise CanvasFileError(
            503, "workspace_unavailable", "Workspace SSH key is not configured"
        )

    try:
        async with _materialization_slot(_REMOTE_SEMAPHORE), asyncio.timeout(45):
            async with _SFTP_POOL.checkout(
                thread_id=str(thread.get("id") or ""),
                generation=expected_generation,
                host=str(host),
                port=port,
                fingerprint=fingerprint,
                key_path=key_path,
            ) as sftp:
                authoritative = (
                    await generation_resolver()
                    if generation_resolver is not None
                    else thread
                )
                _require_same_remote_workspace(
                    authoritative, expected_generation, target
                )
                await _write_sftp_workspace_file(sftp, root=root, path=path, data=data)
                authoritative = (
                    await generation_resolver()
                    if generation_resolver is not None
                    else thread
                )
                _require_same_remote_workspace(
                    authoritative, expected_generation, target
                )
    except CanvasFileError:
        raise
    except Exception as exc:
        logger.warning(
            "Canvas pinned SFTP write failed for thread %s: %s",
            thread.get("id"),
            exc,
        )
        raise CanvasFileError(
            409, "canvas_write_failed", "Workspace file could not be saved"
        ) from exc


async def _read_virtual_default(
    thread: dict[str, Any],
    path: str,
    expected_generation: UUID,
    *,
    read_limit: int | None = None,
) -> RawWorkspaceFile:
    if current_workspace_generation(thread) != expected_generation:
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    spec = virtual_workspace_rclone_spec()
    if not spec:
        raise CanvasFileError(
            409, "workspace_unavailable", "Virtual workspace storage is disabled"
        )
    if spec.get("type") == "memory":
        raise CanvasFileError(
            409,
            "workspace_unavailable",
            "Process-local virtual workspaces cannot be viewed by the orchestrator",
        )
    binding = _metadata(thread).get("_workspace_binding") or {}
    expected_backing_id = virtual_thread_backing_id(str(thread["id"]), spec)
    if (
        binding.get("kind") != "virtual"
        or binding.get("backing_id") != expected_backing_id
    ):
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "Virtual workspace backing changed and must be rebound",
        )
    if shutil.which("rclone") is None:
        raise CanvasFileError(
            503, "workspace_unavailable", "Virtual workspace transport is unavailable"
        )

    cancelled = threading.Event()
    if read_limit is None:
        read_limit = _workspace_read_limit(path)

    def read() -> bytes:
        from shared.runtime.core.backends.rclone import (
            RcloneObjectStore,
            RcloneSizeLimitExceeded,
        )

        store = RcloneObjectStore(
            remote_type=str(spec["type"]),
            config=spec.get("config") or {},
            root=str(spec.get("root") or ""),
            transfer_timeout=45,
            meta_timeout=15,
        )
        key = f"threads/{thread['id']}/{path}"
        try:
            size = store.head(key)
            if size is None:
                raise FileNotFoundError(key)
            if size > read_limit:
                raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
            try:
                data = store.get_bounded(key, read_limit, cancelled=cancelled.is_set)
            except RcloneSizeLimitExceeded:
                raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
            return data
        finally:
            store.close()

    try:
        async with _materialization_slot(_VIRTUAL_SEMAPHORE):
            return RawWorkspaceFile(
                await _run_blocking(
                    read,
                    timeout=50,
                    cancellation_callback=cancelled.set,
                )
            )
    except CanvasFileError:
        raise
    except FileNotFoundError as exc:
        raise CanvasFileError(404, "canvas_file_not_found", "File not found") from exc
    except Exception as exc:
        logger.warning(
            "Canvas virtual read failed for thread %s: %s", thread.get("id"), exc
        )
        raise CanvasFileError(
            409, "workspace_unavailable", "Virtual workspace file could not be read"
        ) from exc


async def _write_virtual_default(
    thread: dict[str, Any], path: str, expected_generation: UUID, data: bytes
) -> None:
    """Best-effort replace in the current durable virtual workspace backing."""

    if current_workspace_generation(thread) != expected_generation:
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    spec = virtual_workspace_rclone_spec()
    if not spec or spec.get("type") == "memory":
        raise CanvasFileError(
            409, "workspace_unavailable", "Virtual workspace storage is unavailable"
        )
    binding = _metadata(thread).get("_workspace_binding") or {}
    expected_backing_id = virtual_thread_backing_id(str(thread["id"]), spec)
    if (
        binding.get("kind") != "virtual"
        or binding.get("backing_id") != expected_backing_id
    ):
        raise CanvasFileError(
            409,
            "workspace_generation_changed",
            "Virtual workspace backing changed and must be rebound",
        )
    if shutil.which("rclone") is None:
        raise CanvasFileError(
            503, "workspace_unavailable", "Virtual workspace transport is unavailable"
        )

    def write() -> None:
        from shared.runtime.core.backends.rclone import RcloneObjectStore

        store = RcloneObjectStore(
            remote_type=str(spec["type"]),
            config=spec.get("config") or {},
            root=str(spec.get("root") or ""),
            transfer_timeout=45,
            meta_timeout=15,
        )
        key = f"threads/{thread['id']}/{path}"
        temporary = f"{key}.srw-canvas-{secrets.token_hex(12)}.tmp"
        try:
            store.put(temporary, data)
            store.copy(temporary, key)
        finally:
            with suppress(Exception):
                store.delete(temporary)
            store.close()

    try:
        async with _materialization_slot(_VIRTUAL_SEMAPHORE):
            await _run_blocking(write, timeout=50)
    except CanvasFileError:
        raise
    except Exception as exc:
        logger.warning(
            "Canvas virtual write failed for thread %s: %s", thread.get("id"), exc
        )
        raise CanvasFileError(
            409, "canvas_write_failed", "Virtual workspace file could not be saved"
        ) from exc


class ThreadWorkspaceFileGateway:
    """Resolve, bound-read, hash, and renderer-validate one Canvas file."""

    def __init__(
        self,
        *,
        remote_loader: RemoteLoader | None = None,
        virtual_loader: VirtualLoader | None = None,
        remote_writer: WorkspaceWriter | None = None,
        virtual_writer: WorkspaceWriter | None = None,
        thread_loader: ThreadLoader | None = None,
    ) -> None:
        self._remote_loader = remote_loader
        self._virtual_loader = virtual_loader or _read_virtual_default
        self._remote_writer = remote_writer
        self._virtual_writer = virtual_writer or _write_virtual_default
        self._thread_loader = thread_loader

    async def _authoritative_thread(self, thread: dict[str, Any]) -> dict[str, Any]:
        if self._thread_loader is None:
            return thread
        thread_id = str(thread.get("id") or "")
        fresh = await self._thread_loader(thread_id)
        if fresh is None:
            raise CanvasFileError(404, "workspace_unavailable", "Thread not found")
        return fresh

    async def validate_for_presentation(
        self,
        thread: dict[str, Any],
        path: str,
        *,
        requested_renderer: CanvasRenderer = "auto",
        alt_text: str | None = None,
    ) -> tuple[UUID, ValidatedCanvasFile]:
        generation = current_workspace_generation(thread)
        validated = await self._materialize(
            thread,
            path,
            generation,
            requested_renderer=requested_renderer,
            alt_text=alt_text,
        )
        return generation, validated

    async def materialize_current(
        self, thread: dict[str, Any], record: CanvasRecord
    ) -> ValidatedCanvasFile:
        source = record.source
        if not isinstance(source, WorkspaceFileSource):
            raise CanvasFileError(
                422, "canvas_not_file", "The current Canvas is not file-backed"
            )
        if current_workspace_generation(thread) != source.workspace_generation:
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The selected workspace generation is no longer current",
            )
        return await self._materialize(
            thread,
            source.path,
            source.workspace_generation,
            requested_renderer=record.renderer,
            alt_text=record.alt_text,
            expected_source_version=record.source_version,
        )

    async def materialize_binary(
        self, thread: dict[str, Any], record: CanvasRecord
    ) -> ValidatedCanvasFile:
        """Materialize one current Office file through the binary-only validator."""

        source = record.source
        if not isinstance(source, WorkspaceFileSource) or record.renderer != "office":
            raise CanvasFileError(
                422,
                "canvas_not_office",
                "The current Canvas is not an Office document",
            )
        if current_workspace_generation(thread) != source.workspace_generation:
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The selected workspace generation is no longer current",
            )
        return await self._materialize(
            thread,
            source.path,
            source.workspace_generation,
            requested_renderer="office",
            alt_text=None,
            expected_source_version=record.source_version,
            office_only=True,
        )

    async def materialize_for_refresh(
        self, thread: dict[str, Any], record: CanvasRecord
    ) -> ValidatedCanvasFile:
        """Validate current bytes without requiring the persisted old hash."""

        source = record.source
        if not isinstance(source, WorkspaceFileSource):
            raise CanvasFileError(
                422, "canvas_not_file", "The current Canvas is not file-backed"
            )
        if current_workspace_generation(thread) != source.workspace_generation:
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The selected workspace generation is no longer current",
            )
        return await self._materialize(
            thread,
            source.path,
            source.workspace_generation,
            requested_renderer=record.renderer,
            alt_text=record.alt_text,
        )

    def supports_editing(
        self, thread: dict[str, Any], record: CanvasRecord | None = None
    ) -> bool:
        """Return whether this bound production backing has a write adapter."""

        if record is not None:
            if not isinstance(record.source, WorkspaceFileSource):
                return False
            if record.renderer not in {
                "markdown",
                "text",
                "html",
                "html-interactive",
                "office",
            }:
                return False
            try:
                if (
                    current_workspace_generation(thread)
                    != record.source.workspace_generation
                ):
                    return False
            except CanvasFileError:
                return False
        if _backend(thread) == "virtual":
            spec = virtual_workspace_rclone_spec()
            return bool(
                spec
                and spec.get("type") != "memory"
                and shutil.which("rclone") is not None
            )
        if self._remote_writer is not None:
            return True
        try:
            generation = current_workspace_generation(thread)
            _remote_workspace_target(thread, generation)
        except CanvasFileError:
            return False
        return asyncssh is not None and bool(resolve_ssh_key_path())

    async def validate_edit_candidate(
        self, record: CanvasRecord, data: bytes
    ) -> ValidatedCanvasFile:
        """Validate bounded candidate bytes without crossing renderer branches."""

        if not record.editable or record.renderer not in {
            "markdown",
            "text",
            "html",
            "html-interactive",
            "office",
        }:
            raise CanvasFileError(
                409,
                "canvas_not_editable",
                "The current Canvas presentation is not editable",
            )
        source = record.source
        if not isinstance(source, WorkspaceFileSource):
            raise CanvasFileError(409, "canvas_replaced", "Canvas source was replaced")
        if record.renderer == "office":
            return await _validate_materialized_office_bytes_async(
                source.path,
                data,
                None,
            )
        return await _validate_materialized_bytes_async(
            source.path,
            data,
            record.renderer,
            record.alt_text,
            None,
        )

    async def replace_current(
        self,
        thread: dict[str, Any],
        record: CanvasRecord,
        data: bytes,
        *,
        expected_source_version: str,
    ) -> ValidatedCanvasFile:
        """Recheck, replace, and read back one coordinator-locked text file."""

        source = record.source
        if not isinstance(source, WorkspaceFileSource):
            raise CanvasFileError(409, "canvas_replaced", "Canvas source was replaced")
        if record.renderer not in {
            "markdown",
            "text",
            "html",
            "html-interactive",
        }:
            raise CanvasFileError(
                409,
                "canvas_not_editable",
                "The text Canvas content route cannot save this renderer",
            )
        await self.validate_edit_candidate(record, data)
        current = await self.materialize_for_refresh(thread, record)
        if not secrets.compare_digest(current.source_version, expected_source_version):
            raise CanvasFileError(
                412,
                "canvas_content_precondition_failed",
                "Canvas content changed; reload before saving",
            )
        await self._write(
            thread,
            source.path,
            source.workspace_generation,
            data,
        )
        return await self.materialize_for_refresh(thread, record)

    async def replace_current_binary(
        self,
        thread: dict[str, Any],
        record: CanvasRecord,
        data: bytes,
        *,
        expected_source_version: str,
    ) -> ValidatedCanvasFile:
        """Recheck, replace, and read back one coordinator-locked Office file."""

        source = record.source
        if (
            not isinstance(source, WorkspaceFileSource)
            or record.renderer != "office"
            or not record.editable
        ):
            raise CanvasFileError(
                409,
                "canvas_not_editable",
                "The current Canvas is not an editable Office document",
            )
        # Keep binary validation on the Office-only magic path. This repeats the
        # pre-lock admission check just as the text writer does; the immutable
        # private spool is cheap to revalidate and never enters UTF-8 handling.
        await self.validate_edit_candidate(record, data)
        current = await self._materialize(
            thread,
            source.path,
            source.workspace_generation,
            requested_renderer="office",
            alt_text=None,
            office_only=True,
        )
        if not secrets.compare_digest(
            current.source_version,
            expected_source_version,
        ):
            raise CanvasFileError(
                412,
                "canvas_content_precondition_failed",
                "Canvas content changed; reload before saving",
            )
        await self._write(
            thread,
            source.path,
            source.workspace_generation,
            data,
        )
        return await self._materialize(
            thread,
            source.path,
            source.workspace_generation,
            requested_renderer="office",
            alt_text=None,
            office_only=True,
        )

    async def _write(
        self,
        thread: dict[str, Any],
        path: str,
        generation: UUID,
        data: bytes,
    ) -> None:
        path = canonical_workspace_path(path)
        thread = await self._authoritative_thread(thread)

        async def resolve_generation() -> dict[str, Any]:
            return await self._authoritative_thread(thread)

        if current_workspace_generation(thread) != generation:
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The selected workspace generation is no longer current",
            )
        if _backend(thread) == "virtual":
            await self._virtual_writer(thread, path, generation, data)
        elif self._remote_writer is not None:
            await self._remote_writer(thread, path, generation, data)
        else:
            await _write_remote_default(
                thread,
                path,
                generation,
                data,
                generation_resolver=resolve_generation,
            )
        authoritative = await self._authoritative_thread(thread)
        if current_workspace_generation(authoritative) != generation:
            if asyncssh is not None:
                await _evict_sftp_generation(str(thread.get("id") or ""), generation)
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The selected workspace generation changed during the save",
            )

    async def read_download(
        self, thread: dict[str, Any], path: str
    ) -> tuple[str, RawWorkspaceFile]:
        """Bound-read one workspace file for its owner to save.

        Any file type up to the file ceiling: the bytes leave as an attachment
        and are never rendered, so the renderer validators don't apply. Path,
        symlink and generation checks are the same as for presentation.
        """
        path = canonical_workspace_path(path)
        thread = await self._authoritative_thread(thread)
        generation = current_workspace_generation(thread)

        async def resolve_generation() -> dict[str, Any]:
            return await self._authoritative_thread(thread)

        async with _materialization_slot(_FULL_MATERIALIZATION_SEMAPHORE):
            if _backend(thread) == "virtual":
                raw = await self._virtual_loader(
                    thread, path, generation, read_limit=MAX_FILE_BYTES
                )
            elif self._remote_loader is not None:
                raw = await self._remote_loader(
                    thread, path, generation, read_limit=MAX_FILE_BYTES
                )
            else:
                raw = await _read_remote_default(
                    thread,
                    path,
                    generation,
                    generation_resolver=resolve_generation,
                    read_limit=MAX_FILE_BYTES,
                )
            if len(raw.data) > MAX_FILE_BYTES:
                raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
            authoritative = await self._authoritative_thread(thread)
            if current_workspace_generation(authoritative) != generation:
                if asyncssh is not None:
                    await _evict_sftp_generation(
                        str(thread.get("id") or ""), generation
                    )
                raise CanvasFileError(
                    409,
                    "workspace_generation_changed",
                    "The selected workspace generation is no longer current",
                )
        return path, raw

    async def _materialize(
        self,
        thread: dict[str, Any],
        path: str,
        generation: UUID,
        *,
        requested_renderer: CanvasRenderer,
        alt_text: str | None,
        expected_source_version: str | None = None,
        office_only: bool = False,
    ) -> ValidatedCanvasFile:
        path = canonical_workspace_path(path)
        thread = await self._authoritative_thread(thread)

        async def resolve_generation() -> dict[str, Any]:
            return await self._authoritative_thread(thread)

        async with _materialization_slot(_FULL_MATERIALIZATION_SEMAPHORE):
            if _backend(thread) == "virtual":
                raw = await self._virtual_loader(thread, path, generation)
            elif self._remote_loader is not None:
                raw = await self._remote_loader(thread, path, generation)
            else:
                raw = await _read_remote_default(
                    thread,
                    path,
                    generation,
                    generation_resolver=resolve_generation,
                )
            if len(raw.data) > _workspace_read_limit(path):
                raise CanvasFileError(413, "canvas_file_too_large", "File is too large")
            authoritative = await self._authoritative_thread(thread)
            if current_workspace_generation(authoritative) != generation:
                if asyncssh is not None:
                    await _evict_sftp_generation(
                        str(thread.get("id") or ""), generation
                    )
                raise CanvasFileError(
                    409,
                    "workspace_generation_changed",
                    "The selected workspace generation is no longer current",
                )
            if office_only:
                validated = await _validate_materialized_office_bytes_async(
                    path,
                    raw.data,
                    expected_source_version,
                )
            else:
                validated = await _validate_materialized_bytes_async(
                    path,
                    raw.data,
                    requested_renderer,
                    alt_text,
                    expected_source_version,
                )
        return ValidatedCanvasFile(
            path=validated.path,
            data=validated.data,
            media_type=validated.media_type,
            renderer=validated.renderer,
            source_version=validated.source_version,
            last_modified=raw.last_modified,
        )


__all__ = [
    "CanvasFileError",
    "CanvasResponseLease",
    "RawWorkspaceFile",
    "ThreadWorkspaceFileGateway",
    "ValidatedCanvasFile",
    "acquire_canvas_response_lease",
    "canonical_workspace_path",
    "current_workspace_generation",
    "validate_canvas_bytes",
]
