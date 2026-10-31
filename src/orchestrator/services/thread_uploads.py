"""Upload files into a persistent thread's live workspace.

Files land in ``<workspace>/uploads/``. Which transport gets them there is
decided by the thread's workspace tier:

- ``sandbox`` / ``vm`` — SFTP into the workspace container or VM (paramiko).
- ``virtual`` — object-store ``put`` under ``threads/<id>/uploads/``, the same
  durable namespace the agent's lite backend is attached to at dispatch, so the
  agent's own ``read_file("uploads/x")`` resolves what we wrote.

``none`` (a disposable agent-local tmpdir) has no orchestrator-reachable
storage and is refused permanently rather than pretending to be transient.

Filenames are sanitized to prevent path traversal and collisions are resolved by
appending a counter.

A ``.zip`` upload is expanded into ``uploads/<stem>/…`` rather than stored as
a single opaque archive, mirroring the worker-job path's ``_extract_zip``
(``src/agent/agent.py:4067``) so an attached archive is immediately readable by
``read_file``/``list_files`` with no unzip capability required. See
``_expand_payloads_for_extraction`` below. When extraction is refused
(corrupt, unsafe entry, over a cap) the archive is stored verbatim under its
own claimed name plus a sidecar ``ZIP_REFUSAL_NOTE_SUFFIX`` file explaining
why — both flow through the normal upload-result plumbing, so they show up
like any other attached file in the HTTP response and the cockpit's
"Attached files:" hint, and ``read_file`` on the archive itself surfaces the
same note (see ``_read_zip_extraction_note`` in
``src/agent/tools/workspace/files.py``).

The agent learns about new uploads when the cockpit appends an
``Attached files: …`` hint to the next user message — see
``persistent-chat.service.ts``.

Uploads can also be removed again (``delete_file_from_thread_workspace``,
behind ``DELETE .../uploads/{path:path}``), because the cockpit uploads
*eagerly* — on attach, before the user commits to sending. Removing an
attachment chip therefore has to be able to remove bytes that already
landed. That path's whole security burden sits in ``_safe_upload_relpath``.
"""

from __future__ import annotations

import asyncio
import base64
import io
import logging
import mimetypes
import os
import posixpath
import re
import secrets
import stat as stat_module
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Iterable, Iterator
from uuid import UUID

try:
    import paramiko
except ImportError:  # pragma: no cover - import-time guard
    paramiko = None  # type: ignore[assignment]
    logging.getLogger(__name__).warning(
        "paramiko is not installed — thread workspace uploads "
        "(POST /api/persistent/threads/{id}/uploads) will fail with HTTP 503"
    )

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.canvas_ssh import (
    PINNED_SSH_TRANSPORT_POOL,
    CanvasSSHError,
    PinnedSFTPPool,
    asyncssh,
)
from orchestrator.services.ssh_helpers import _fingerprint_host_key
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from orchestrator.services import workspace_binding
from orchestrator.services.workspace_binding import (
    VirtualBackingUnavailable,
    resolve_virtual_thread_backing,
    virtual_thread_backing_id,
)
from orchestrator.services.blocking_effect import (
    joined_async_call,
    joined_blocking_call,
)

logger = logging.getLogger(__name__)

# Cap individual files at 100MB. The cockpit allows larger uploads for
# job creation (5GB), but those go into local orchestrator storage. Live
# workspace pushes flow through the orchestrator process into a container/VM or
# object store, where huge blobs are rarely useful and tie up that process.
MAX_FILE_SIZE = 100 * 1024 * 1024
MAX_FILES_PER_REQUEST = 20
MAX_TOTAL_UPLOAD_BYTES = 300 * 1024 * 1024

DEFAULT_USERNAME = "agent-host"
DEFAULT_WORKSPACE_PATH = "/home/agent-host/workspace"
UPLOADS_SUBDIR = "uploads"

# Tiers with no orchestrator-reachable workspace storage at all.
UNREACHABLE_BACKENDS = frozenset({"none"})

# Bound concurrent object-store uploads: each one holds a full request body in
# memory and spawns rclone children. Mirrors the ceiling Canvas puts on its own
# virtual materializations, with a bounded queue wait so a saturated
# orchestrator fails fast instead of piling up.
MAX_CONCURRENT_VIRTUAL_UPLOADS = int(os.getenv("THREAD_UPLOAD_MAX_CONCURRENT", "4"))
VIRTUAL_UPLOAD_QUEUE_TIMEOUT = float(os.getenv("THREAD_UPLOAD_QUEUE_TIMEOUT", "10"))
_VIRTUAL_UPLOAD_SEMAPHORE = asyncio.Semaphore(max(1, MAX_CONCURRENT_VIRTUAL_UPLOADS))

# Stateless sandbox uploads share Canvas's provisioner-pinned SSH transport,
# but use a runtime-incarnation-qualified pool identity below.  A same-name pod
# replacement can retain the workspace generation, IP, and deployment-wide SSH
# host key; including the immutable Kubernetes Pod UID prevents a pooled channel
# from silently crossing that replacement boundary.
_ATTESTED_SFTP_POOL = PinnedSFTPPool(transport_pool=PINNED_SSH_TRANSPORT_POOL)

RuntimeAuthorityProbe = Callable[[], Awaitable[str]]

_WORKSPACE_GENERATION_KEY = "_canvas_workspace_generation"
_RUNTIME_INCARNATION_KEY = "_runtime_incarnation"
_RESTORE_REQUIRED_KEY = "_snapshot_restore_required"


@dataclass
class UploadedFile:
    """One file successfully written into the workspace's uploads/ dir."""

    name: str
    size: int
    mime_type: str
    path: str  # Posix path relative to workspace root, e.g. "uploads/photo.jpg"


@dataclass
class _SshTarget:
    host: str
    port: int
    username: str
    key_path: str
    workspace_path: str
    # The control-plane-attested SSH host key this endpoint must present. It has
    # no default on purpose: a caller that cannot name the key it expects has no
    # way to tell the workspace apart from anything else answering on that
    # address, and must be refused rather than silently trusting the peer.
    host_key_fingerprint: str


@dataclass
class _VirtualTarget:
    """Object-store destination for a ``virtual`` (lite) thread."""

    spec: dict[str, Any]  # rclone {type, config, root}
    prefix: str  # "threads/<thread_id>/"


@dataclass(frozen=True, slots=True)
class _StatelessUploadAuthority:
    """Exact sandbox runtime tuple authorized to receive upload bytes."""

    thread_id: str
    workspace_generation: UUID
    runtime_incarnation: UUID
    host_key_fingerprint: str
    target: _SshTarget

    @property
    def transport_pool_thread_id(self) -> str:
        """Pool namespace which cannot be reused by a replacement Pod UID."""

        return f"{self.thread_id}:{self.runtime_incarnation}"


class ThreadUploadError(Exception):
    """Raised when a thread workspace upload cannot proceed.

    Carries an HTTP status code so the API layer can map it directly.
    """

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _HostKeyMismatch(Exception):
    """The peer's SSH host key is not the attested one. Never caught as I/O."""


if paramiko is not None:

    class _PinnedHostKeyPolicy(paramiko.MissingHostKeyPolicy):
        """Accept exactly the control-plane-attested host key, nothing else.

        Paramiko consults this hook once the key exchange has produced the
        server's host key but *before* it authenticates, so raising here means
        the workspace private key is never offered to a peer that failed to
        prove its identity. The digest covers the whole key blob, including its
        type, so matching an ed25519 pin is itself proof the peer presented the
        attested ed25519 key.
        """

        def __init__(self, expected_fingerprint: str):
            self._expected = expected_fingerprint

        def missing_host_key(self, client, hostname, key) -> None:
            presented = _fingerprint_host_key(
                base64.b64encode(key.asbytes()).decode("ascii")
            )
            if not secrets.compare_digest(presented, self._expected):
                raise _HostKeyMismatch(
                    f"host key for {hostname} does not match the attested key"
                )


def _pinned_ssh_client(target: _SshTarget) -> Any:
    """Build an SSHClient that will only complete against ``target``'s key."""

    if paramiko is None:  # pragma: no cover - import guard
        raise ThreadUploadError(
            status_code=503, detail="paramiko is not installed on the orchestrator"
        )
    if not _valid_ssh_fingerprint(target.host_key_fingerprint):
        # No usable pin means the peer's identity cannot be established at all.
        # Trusting it anyway is precisely the man-in-the-middle exposure this
        # refusal exists to close, so there is no fallback policy here.
        logger.warning(
            "Refusing SFTP to %s:%d — no attested SSH host key fingerprint",
            target.host,
            target.port,
        )
        raise ThreadUploadError(
            status_code=409,
            detail="Workspace SSH host key attestation is unavailable",
        )

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(
        _PinnedHostKeyPolicy(target.host_key_fingerprint)
    )
    return client


def _connect_pinned_sftp(client: Any, target: _SshTarget) -> None:
    """Connect ``client`` to ``target``, enforcing its pinned host key."""

    try:
        client.connect(
            hostname=target.host,
            port=target.port,
            username=target.username,
            key_filename=target.key_path,
            timeout=15,
            banner_timeout=15,
            auth_timeout=15,
            allow_agent=False,
            look_for_keys=False,
        )
    except _HostKeyMismatch as e:
        # An identity verdict, not a reachability one: the host answered and
        # presented the wrong key. Reporting it as "could not reach" would hide
        # an active man-in-the-middle behind a retryable-looking message.
        logger.error(
            "SSH host key mismatch for %s@%s:%d — refusing SFTP",
            target.username,
            target.host,
            target.port,
        )
        client.close()
        raise ThreadUploadError(
            status_code=502,
            detail=(
                f"Workspace SSH host key verification failed "
                f"({target.host}:{target.port})"
            ),
        ) from e
    except Exception as e:
        logger.warning(
            "SSH connect failed for %s@%s:%d (%s)",
            target.username,
            target.host,
            target.port,
            e,
        )
        client.close()
        raise ThreadUploadError(
            status_code=502,
            detail=f"Could not reach workspace ({target.host}:{target.port})",
        ) from e


def _sanitize_filename(filename: str) -> str:
    """Strip path components and dangerous characters from an upload name."""
    filename = PurePosixPath(filename).name
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", filename)
    filename = filename.strip(". ")
    if len(filename) > 200:
        p = PurePosixPath(filename)
        filename = f"{p.stem[:190]}{p.suffix}"
    return filename or "unnamed"


def _safe_upload_relpath(relpath: str) -> str | None:
    """Confine a caller-supplied path to ``uploads/``, or return None.

    The validator behind ``DELETE .../uploads/{path:path}``, and the only
    thing standing between that route and an arbitrary-file-deletion
    primitive — SFTP will ``remove`` any path the ``agent-host`` user can
    write, and the object store will delete any key under the prefix, so
    the check can never be delegated to the remote.

    ``relpath`` is relative to ``uploads/`` itself — ``"report.pdf"``, or
    ``"bundle/sub/a.txt"`` for a zip-extracted member — i.e. exactly
    ``UploadedFile.name``, NOT the workspace-relative ``UploadedFile.path``
    (which carries its own ``uploads/`` prefix and would resolve to
    ``uploads/uploads/...``).

    Posture is ``_safe_zip_member_path``'s, not ``_sanitize_filename``'s:

    - ``_sanitize_filename`` is unusable here. It flattens to
      ``PurePosixPath(name).name``, which destroys the ``bundle/sub/a.txt``
      shape an extracted member legitimately has — and, worse, a *sanitized*
      traversal is a delete silently redirected at some other file rather
      than a refusal.
    - ``posixpath.normpath`` runs BEFORE the prefix check, so
      ``uploads/../../.ssh/authorized_keys`` is resolved and then rejected
      instead of passing a naive "starts with uploads/" test.
    - Confinement is to the uploads *subdirectory*, not to the thread
      prefix, which is shared: Canvas writes thread state at
      ``threads/<id>/<path>`` (``services/canvas_files.py:1034``) and tool
      files live alongside it.

    Rejects: empty, NUL, absolute (leading ``/``), any backslash, a Windows
    drive letter, anything normalizing to ``uploads/`` itself (``"."``,
    ``"bundle/.."`` — removing one attachment never means removing the
    directory), and anything normalizing outside it.

    Returns:
        The **normalized** path relative to ``uploads/`` — the string the
        transports must act on — or None when the input is refused.
    """
    if not relpath or "\x00" in relpath or relpath.startswith("/"):
        return None
    if "\\" in relpath or re.match(r"^[A-Za-z]:", relpath):
        return None

    joined = posixpath.normpath(posixpath.join(UPLOADS_SUBDIR, relpath))
    if joined == UPLOADS_SUBDIR or not joined.startswith(UPLOADS_SUBDIR + "/"):
        return None
    return joined[len(UPLOADS_SUBDIR) + 1 :]


def _name_candidates(name: str) -> Iterator[str]:
    """Yield ``name``, then ``name_1.ext``, ``name_2.ext``, … indefinitely.

    Shared by both transports so SFTP and object-store uploads resolve
    collisions to the same names.
    """
    yield name
    p = PurePosixPath(name)
    stem, suffix = p.stem, p.suffix
    counter = 1
    while True:
        yield f"{stem}_{counter}{suffix}"
        counter += 1


def _claim_name(taken: set[str], name: str) -> str:
    """Pick the first free candidate and reserve it in ``taken``.

    Reserving as we go is what keeps two identically-named files *in one batch*
    from overwriting each other — the SFTP path gets that for free by stat-ing
    after each write, a flat key space does not.
    """
    for candidate in _name_candidates(name):
        if candidate not in taken:
            taken.add(candidate)
            return candidate
    raise AssertionError("unreachable: _name_candidates is infinite")


# =============================================================================
# Zip extraction
#
# The worker-job path already does this (``_extract_zip``, src/agent.py:4067,
# called at :2625/:2676/:2718): a `.zip` document is expanded into
# `documents/…` instead of left as an opaque archive. This mirrors that for
# session uploads, but hardens what it borrows: the worker's dotfile skip
# blocks ".." only *incidentally* (because ".." happens to start with ".")
# and it has no entry-count or size caps at all.
# =============================================================================


class ZipExtractionRefused(Exception):
    """A zip payload failed validation and must not be partially expanded.

    Raised for anything that makes safe, COMPLETE extraction impossible: a
    corrupt/non-zip payload, a path-traversal entry, too many entries, or an
    uncompressed size over either cap below. Callers catch this and fall
    back to storing the archive verbatim — a zip either extracts completely
    and safely, or the user keeps exactly what they uploaded. Never a
    silently half-extracted result.
    """


# MAX_FILE_SIZE (100MB, above) already bounds the compressed upload; these
# bound what it may explode into, so a corrupt or hostile archive fails
# cleanly instead of filling the workspace, blowing up orchestrator memory,
# or hanging the write loop.
#
#   MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES: no single extracted member may be
#   bigger than a standalone upload would have been allowed to be. Bounds
#   peak memory for decompressing any one entry.
#
#   MAX_ZIP_ENTRIES_PER_REQUEST / MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES_PER_REQUEST:
#   shared across *every* zip in one upload request (MAX_FILES_PER_REQUEST
#   payloads), not reset per archive. An earlier version of this module
#   capped these per zip, which a full batch multiplies by 20 — ~6GB
#   decompressed plus ~2GB still-referenced compressed payloads, ~24GB with
#   MAX_CONCURRENT_VIRTUAL_UPLOADS=4 concurrent requests. Sized instead for
#   the *virtual* transport's real per-entry cost: RcloneObjectStore.put
#   spawns one `rclone rcat` subprocess per key
#   (src/core/backends/rclone.py:256), ~100ms observed. 100 entries is ~10s
#   of worst-case spawn time for the *whole request* — comfortably under a
#   typical 30-60s proxy timeout, where 2,000 (~3.5 min) or a 20-zip batch
#   at that rate (~1hr+) would die mid-write with the workspace partially
#   populated, since the plan is atomic but the write loop is not. 300MB
#   (3x the compressed per-file cap) keeps the transient decompressed
#   buffer bounded to ~1.2GB across 4 concurrent requests. The SFTP
#   transport's per-write cost is much cheaper (no subprocess), but shares
#   the same cap rather than carrying a second number to maintain.
MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = MAX_FILE_SIZE
MAX_ZIP_ENTRIES_PER_REQUEST = 100
MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES_PER_REQUEST = 3 * MAX_FILE_SIZE

# Chunk size for the capped incremental read in _read_zip_entry_capped.
_ZIP_READ_CHUNK_SIZE = 65536

# Sidecar suffix written next to a zip that fell back to verbatim storage
# because extraction was refused (corrupt, traversal entry, over a cap).
# read_file (src/tools/workspace/files.py::_read_zip_extraction_note) checks
# for this exact suffix before listing a zip's entries — otherwise a merely
# *parseable* but refused archive reads as an ordinary, successfully
# extracted one: the agent sees a full entry listing with nothing anywhere
# (not the HTTP response, not the cockpit hint, not read_file) saying those
# entries were never separated into readable files. That is the motivating
# incident's dead end, recreated one layer down. No shared import exists
# between the orchestrator and agent processes; keep both ends of this
# string in sync by hand.
ZIP_REFUSAL_NOTE_SUFFIX = ".extraction-refused.txt"


def _is_symlink_zip_entry(info: zipfile.ZipInfo) -> bool:
    """True when a zip entry's Unix mode bits mark it as a symlink.

    Extraction never calls a symlink-creation API, but a symlink entry's
    "content" is just its target path string — writing that as if it were
    real file content would be misleading, so it's skipped like a directory
    entry instead of materialized as a bogus file.
    """
    mode = info.external_attr >> 16
    return stat_module.S_ISLNK(mode) if mode else False


def _safe_zip_member_path(stem: str, entry_name: str) -> str | None:
    """Resolve a zip entry name to a path under ``stem``, or None if unsafe.

    Deliberate, unlike the worker's ``_extract_zip`` (``src/agent/agent.py:4067``),
    whose dotfile skip incidentally also blocks ".." only because ".."
    happens to start with ".". Rejects absolute paths, Windows drive
    letters/backslash separators (never legitimate in a zip's stored POSIX
    names), and any entry whose normalized destination would resolve outside
    ``stem`` — the "Zip-Slip" family of path-traversal entries.
    """
    if not entry_name or "\x00" in entry_name or entry_name.startswith("/"):
        return None
    if "\\" in entry_name or re.match(r"^[A-Za-z]:", entry_name):
        return None

    joined = posixpath.normpath(posixpath.join(stem, entry_name))
    if joined == stem or not joined.startswith(stem + "/"):
        return None
    return joined


def _read_zip_entry_capped(fh: Any, cap: int, entry_name: str) -> bytes:
    """Read a zip entry's decompressed bytes, aborting past ``cap``.

    A zip's declared ``file_size``/CRC can lie about what its compressed
    stream actually decompresses to, so the real defense against a zip bomb
    is bounding bytes as they come out of the decompressor — not trusting
    the archive's own metadata (the pre-check against declared size in
    ``_plan_zip_extraction`` is a cheap fast-fail, not the backstop).
    """
    total = 0
    chunks: list[bytes] = []
    while True:
        chunk = fh.read(_ZIP_READ_CHUNK_SIZE)
        if not chunk:
            break
        total += len(chunk)
        if total > cap:
            raise ZipExtractionRefused(
                f"entry {entry_name!r} decompresses past the {cap}-byte per-entry limit"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _plan_zip_extraction(
    data: bytes,
    stem: str,
    *,
    max_entries: int | None = None,
    max_entry_bytes: int | None = None,
    max_total_bytes: int | None = None,
) -> list[tuple[str, bytes]]:
    """Validate and expand a zip's bytes into ``(name, content)`` pairs.

    ``name`` is relative to the uploads/ directory root (e.g.
    ``"myarchive/sub/file.txt"`` for ``stem="myarchive"``) — ready to become
    ``UploadedFile.name`` directly, exactly like a flat upload's name.

    The three ``max_*`` caps default to the module-level per-request
    constants, but ``_expand_payloads_for_extraction`` passes in whatever is
    actually left of that shared budget when a batch has more than one zip
    — the caps are enforced *per request*, not per archive.

    Raises ``ZipExtractionRefused`` for anything that makes safe, complete
    extraction impossible. Never returns a partial result: a caller either
    gets every safe entry or none of them ("rather than half-extracted").
    """
    try:
        return _plan_zip_extraction_unsafe(
            data, stem, max_entries, max_entry_bytes, max_total_bytes
        )
    except ZipExtractionRefused:
        raise
    except Exception as e:
        # zipfile can raise more than BadZipFile for a hostile or corrupt
        # archive (NotImplementedError for an exotic compression method,
        # RuntimeError for an encrypted entry, ...). None of those should
        # ever crash an upload request — they all mean the same thing here:
        # this zip cannot be safely and completely extracted.
        raise ZipExtractionRefused(f"could not process zip: {e}") from e


def _plan_zip_extraction_unsafe(
    data: bytes,
    stem: str,
    max_entries: int | None,
    max_entry_bytes: int | None,
    max_total_bytes: int | None,
) -> list[tuple[str, bytes]]:
    # Read fresh from the module globals on every call (rather than as
    # parameter defaults, which Python binds once at def time) so tests can
    # still monkeypatch the module-level constants directly.
    if max_entries is None:
        max_entries = MAX_ZIP_ENTRIES_PER_REQUEST
    if max_entry_bytes is None:
        max_entry_bytes = MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES
    if max_total_bytes is None:
        max_total_bytes = MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES_PER_REQUEST

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise ZipExtractionRefused(f"not a valid zip file: {e}") from e

    with zf:
        infos = zf.infolist()
        if len(infos) > max_entries:
            raise ZipExtractionRefused(
                f"{len(infos)} entries exceeds the {max_entries}-entry limit"
            )

        plan: list[tuple[str, bytes]] = []
        total_uncompressed = 0

        for info in infos:
            name = info.filename
            if info.is_dir():
                continue
            if not PurePosixPath(name).name:
                continue  # empty/degenerate filename — structural, not a
                # traversal attempt

            # Deliberate security boundary FIRST, independent of the content
            # filters below — otherwise a traversal entry built purely from
            # ".." components (e.g. "../../etc/passwd") would be silently
            # *skipped* by the dotfile filter instead of refusing the whole
            # archive, since ".." happens to start with ".". That is
            # precisely the worker's incidental behaviour this must not
            # reproduce.
            dest = _safe_zip_member_path(stem, name)
            if dest is None:
                raise ZipExtractionRefused(
                    f"entry {name!r} resolves outside the destination directory"
                )

            # Content filters: routine exclusions, not safety violations.
            parts = PurePosixPath(name).parts
            if any(part.startswith(".") for part in parts):
                continue  # dotfiles/hidden
            if "__MACOSX" in parts:
                continue
            if _is_symlink_zip_entry(info):
                continue

            if info.file_size > max_entry_bytes:
                raise ZipExtractionRefused(
                    f"entry {name!r} declares {info.file_size} bytes "
                    f"uncompressed, over the {max_entry_bytes} byte "
                    "per-entry limit"
                )

            with zf.open(info) as fh:
                content = _read_zip_entry_capped(fh, max_entry_bytes, name)

            total_uncompressed += len(content)
            if total_uncompressed > max_total_bytes:
                raise ZipExtractionRefused(
                    "uncompressed contents exceed the "
                    f"{max_total_bytes}-byte total limit"
                )

            plan.append((dest, content))

    return plan


def _expand_payloads_for_extraction(
    payloads: list[tuple[str, bytes, str]],
    taken: set[str],
) -> list[tuple[str, bytes, str]]:
    """Resolve final names for a batch of uploads, expanding any ``.zip``.

    ``taken`` is mutated as names are claimed — files and zip stems share one
    namespace (a directory and a file can't coexist under the same name in
    ``uploads/`` either way). Returns ``(name, data, mime_type)`` triples
    ready to write verbatim at ``uploads/<name>``: a regular upload gets its
    usual sanitized, collision-resolved flat name; a zip's members get
    ``<stem>/<relative path within the archive>``.

    A zip that fails validation (corrupt, unsafe entry, over a cap, or the
    request's shared extraction budget already spent — see below) falls
    back to its original bytes, claimed through the *same* ``_claim_name``
    check as every other name here — it must never land on a name nothing
    checked against ``taken``, or it can silently overwrite an unrelated
    existing upload. A sidecar note (``ZIP_REFUSAL_NOTE_SUFFIX``) is written
    alongside explaining why, because the stored bytes alone don't say
    extraction was attempted and refused — see the module-level comment on
    that constant. Never a half-extracted result, never a vanished upload
    (knowledge-base/knowledge/issues/session_uploads_never_extract_archives.md).

    Entry count and uncompressed bytes are capped *per call* (i.e. per
    upload request — this runs once per ``upload_files_to_thread_workspace``
    invocation), shared across every zip in ``payloads`` rather than reset
    for each one. See the ``MAX_ZIP_ENTRIES_PER_REQUEST`` comment for why a
    per-zip cap alone isn't enough once a batch can hold
    ``MAX_FILES_PER_REQUEST`` of them.
    """
    expanded: list[tuple[str, bytes, str]] = []
    remaining_entries = MAX_ZIP_ENTRIES_PER_REQUEST
    remaining_bytes = MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES_PER_REQUEST

    for filename, data, mime_type in payloads:
        safe_name = _sanitize_filename(filename)
        suffix = PurePosixPath(safe_name).suffix

        if suffix.lower() != ".zip":
            expanded.append((_claim_name(taken, safe_name), data, mime_type))
            continue

        # Claim BOTH names this payload could end up needing before
        # attempting extraction, so whichever outcome actually happens has
        # already been checked against `taken` — the fallback below must
        # never be assembled from a claimed name plus a suffix nobody
        # checked (that was CRITICAL 1 in review: an existing "bundle.zip"
        # got silently clobbered by a corrupt/refused re-upload of the same
        # name, because only the bare "bundle" stem was ever claimed). The
        # unused slot is a harmless over-reservation either way.
        stem = _claim_name(taken, PurePosixPath(safe_name).stem or "archive")
        fallback_name = _claim_name(taken, safe_name)

        plan: list[tuple[str, bytes]] = []
        refusal_reason: str | None = None
        if remaining_entries <= 0 or remaining_bytes <= 0:
            refusal_reason = (
                "this request's zip-extraction budget was already spent by "
                "an earlier archive in the same upload"
            )
            logger.warning(
                "Zip extraction skipped for %r — per-request extraction "
                "budget exhausted by earlier archives in this batch; "
                "storing as-is instead",
                filename,
            )
        else:
            try:
                plan = _plan_zip_extraction(
                    data,
                    stem,
                    max_entries=min(MAX_ZIP_ENTRIES_PER_REQUEST, remaining_entries),
                    max_entry_bytes=min(
                        MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES, remaining_bytes
                    ),
                    max_total_bytes=min(
                        MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES_PER_REQUEST, remaining_bytes
                    ),
                )
            except ZipExtractionRefused as e:
                refusal_reason = str(e)
                logger.warning(
                    "Zip extraction refused for %r (%s) — storing the upload "
                    "as-is instead of extracting it",
                    filename,
                    e,
                )

        if not plan:
            expanded.append((fallback_name, data, mime_type))
            if refusal_reason is not None:
                note_name = _claim_name(
                    taken, f"{fallback_name}{ZIP_REFUSAL_NOTE_SUFFIX}"
                )
                note_body = (
                    f"Extraction refused: {refusal_reason}\n\n"
                    "This archive's original bytes are stored as-is at the "
                    "path above. Reading it will show an entry listing (if "
                    "it can still be parsed as a zip), but none of its "
                    "members were separated into individually readable "
                    "files — ask for a fresh, smaller/safer upload if you "
                    "need a specific one.\n"
                ).encode("utf-8")
                expanded.append((note_name, note_body, "text/plain"))
            continue

        remaining_entries -= len(plan)
        remaining_bytes -= sum(len(content) for _, content in plan)

        for member_name, content in plan:
            mime, _ = mimetypes.guess_type(member_name)
            expanded.append((member_name, content, mime or "application/octet-stream"))

    return expanded


def _thread_metadata(thread: dict[str, Any]) -> dict[str, Any]:
    """Thread ``metadata`` as a dict, tolerating a JSON-string column."""
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        import json

        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            metadata = {}
    return metadata if isinstance(metadata, dict) else {}


def _workspace_backend(metadata: dict[str, Any]) -> str | None:
    """The thread's selected tier from ``config_override.workspace.backend``."""
    config_override = metadata.get("config_override")
    if not isinstance(config_override, dict):
        return None
    workspace = config_override.get("workspace")
    if not isinstance(workspace, dict):
        return None
    backend = workspace.get("backend")
    return backend if isinstance(backend, str) else None


def is_kubernetes_thread_upload_destination(thread: dict[str, Any]) -> bool:
    """Conservatively classify a sandbox endpoint as Kubernetes-backed."""

    metadata = _thread_metadata(thread)
    if _workspace_backend(metadata) != "sandbox":
        return False
    workspace = metadata.get("workspace_container")
    if not isinstance(workspace, dict):
        return False
    provisioner = str(workspace.get("provisioner") or "").strip().lower()
    if provisioner in {"k8s", "kubernetes"}:
        return True
    if provisioner:
        return False
    # Legacy K8s writers omitted provisioner. Pod/runtime coordinates are not
    # evidence for a non-Kubernetes transport, so ambiguous rows take the
    # attested path and fail closed if exact authority cannot be recovered.
    return any(
        key in workspace
        for key in (
            "pod_ip",
            "pod_name",
            "namespace",
            _RUNTIME_INCARNATION_KEY,
            _WORKSPACE_GENERATION_KEY,
        )
    )


def resolve_thread_upload_destination(
    thread: dict[str, Any],
) -> _SshTarget | _VirtualTarget:
    """Decide where this thread's uploads go, without moving any bytes.

    Exposed separately so the API layer can reject an impossible upload before
    materializing request bodies.

    Preference order for SSH tiers:
      1. ``metadata.vm`` (live VM via QEMU/NATS)
      2. ``metadata.workspace_container.host`` (Docker compose pool)
      3. ``metadata.workspace_container.pod_ip`` (K8s pod)

    Raises:
        ThreadUploadError: With a status and message honest about *why* — a
            permanent refusal for tiers that can never accept files, an
            operator-facing 503 for misconfigured storage, and the original
            transient 409 only where waiting can actually help.
    """
    metadata = _thread_metadata(thread)
    backend = _workspace_backend(metadata)

    if backend in UNREACHABLE_BACKENDS:
        raise ThreadUploadError(
            status_code=409,
            detail=(
                "This session has no workspace, so files cannot be attached to "
                "it. Start a session with a workspace to upload files."
            ),
        )

    if backend == "virtual":
        try:
            spec, prefix = resolve_virtual_thread_backing(thread)
        except VirtualBackingUnavailable as e:
            # not_configured / transport_missing are deployment problems the
            # user cannot retry their way out of; backing_changed needs a
            # reopened session, not a wait.
            status = 409 if e.reason == "backing_changed" else 503
            raise ThreadUploadError(status_code=status, detail=e.detail) from e
        return _VirtualTarget(spec=spec, prefix=prefix)

    vm_ctx = metadata.get("vm") or {}
    ws_ctx = metadata.get("workspace_container") or {}
    binding = metadata.get("_workspace_binding") or {}

    host: str | None = None
    port = 22
    workspace_path = DEFAULT_WORKSPACE_PATH
    host_key_fingerprint = ""

    if backend == "vm" and vm_ctx.get("status") == "ready":
        host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
        port = int(vm_ctx.get("ssh_port") or 22)
        workspace_path = vm_ctx.get("workspace_path") or DEFAULT_WORKSPACE_PATH
        host_key_fingerprint = vm_ctx.get("ssh_host_key_fingerprint") or ""
    elif backend in {None, "sandbox"} and ws_ctx.get("status") == "ready":
        host = ws_ctx.get("host") or ws_ctx.get("pod_ip")
        port = int(ws_ctx.get("port") or 22)
        workspace_path = ws_ctx.get("workspace_path") or DEFAULT_WORKSPACE_PATH
        host_key_fingerprint = ws_ctx.get("ssh_host_key_fingerprint") or ""

    # The binding is the control plane's own record of which key this workspace
    # was provisioned with, so it wins over whatever the endpoint context echoes.
    if isinstance(binding, dict) and binding.get("ssh_host_key_fingerprint"):
        host_key_fingerprint = str(binding["ssh_host_key_fingerprint"])

    if not host:
        # Genuinely transient now: this branch is only reachable for tiers that
        # do provision a pod/VM, so waiting is the right advice.
        raise ThreadUploadError(
            status_code=409,
            detail="Workspace is not ready — try again in a moment",
        )

    key_path = resolve_ssh_key_path()
    if not key_path:
        raise ThreadUploadError(
            status_code=503,
            detail="SSH key for workspace upload is not configured",
        )

    return _SshTarget(
        host=host,
        port=port,
        username=DEFAULT_USERNAME,
        key_path=key_path,
        workspace_path=workspace_path,
        host_key_fingerprint=host_key_fingerprint,
    )


def _parse_required_uuid(value: Any, *, label: str) -> UUID:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ThreadUploadError(
            status_code=409,
            detail=f"Stateless workspace {label} attestation is unavailable",
        ) from exc
    if parsed.int == 0:
        raise ThreadUploadError(
            status_code=409,
            detail=f"Stateless workspace {label} attestation is unavailable",
        )
    return parsed


def _valid_ssh_fingerprint(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and value.startswith("SHA256:")
        and len(value) > len("SHA256:")
        and len(value) <= 128
        and not any(char.isspace() for char in value)
    )


def _resolve_stateless_upload_authority(
    thread: dict[str, Any],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
) -> _StatelessUploadAuthority:
    """Validate one route-locked stateless sandbox upload authority.

    The three expected values are captured by the route from its fresh,
    lifecycle-locked row.  Requiring them separately means an accidental call
    with an old pre-lock destination cannot silently authorize the current
    endpoint.  This check deliberately accepts no VM/Docker compatibility
    shapes; those stay on ``upload_files_to_thread_workspace``'s legacy path.
    """

    if thread.get("execution_lane") != "stateless":
        raise ThreadUploadError(
            status_code=409,
            detail="Attested workspace upload requires a stateless session",
        )
    backend, refusal = stateless_session_workspace_check(thread)
    if backend != "sandbox" or refusal is not None:
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless sandbox workspace attestation is unavailable",
        )
    if not isinstance(destination, _SshTarget):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless sandbox upload destination is unavailable",
        )

    thread_id = str(thread.get("id") or "").strip()
    if not thread_id:
        raise ThreadUploadError(status_code=404, detail="Thread is unavailable")

    metadata = _thread_metadata(thread)
    workspace = metadata.get("workspace_container") or {}
    binding = metadata.get("_workspace_binding") or {}
    if not isinstance(workspace, dict) or not isinstance(binding, dict):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless sandbox workspace attestation is unavailable",
        )
    if (
        _RESTORE_REQUIRED_KEY in workspace
        and workspace[_RESTORE_REQUIRED_KEY] is not False
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace restore is not complete",
        )

    expected_generation = _parse_required_uuid(
        expected_workspace_generation, label="generation"
    )
    expected_runtime = _parse_required_uuid(
        expected_runtime_incarnation, label="runtime incarnation"
    )
    bound_generation = _parse_required_uuid(
        binding.get("generation"), label="generation"
    )
    endpoint_generation = _parse_required_uuid(
        workspace.get(_WORKSPACE_GENERATION_KEY), label="endpoint generation"
    )
    runtime_incarnation = _parse_required_uuid(
        workspace.get(_RUNTIME_INCARNATION_KEY), label="runtime incarnation"
    )
    if not (
        expected_generation == bound_generation == endpoint_generation
        and expected_runtime == runtime_incarnation
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace authority changed before upload",
        )

    bound_fingerprint = binding.get("ssh_host_key_fingerprint")
    if not _valid_ssh_fingerprint(expected_host_key_fingerprint) or not (
        _valid_ssh_fingerprint(bound_fingerprint)
        and secrets.compare_digest(
            expected_host_key_fingerprint,
            str(bound_fingerprint),
        )
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace SSH identity changed before upload",
        )

    # Resolve again from the exact row instead of trusting the caller's target.
    # This catches stale host, port, key-path, username, and workspace-root
    # values before any network I/O.
    resolved = resolve_thread_upload_destination(thread)
    if not isinstance(resolved, _SshTarget) or resolved != destination:
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace endpoint changed before upload",
        )
    if (
        destination.username != DEFAULT_USERNAME
        or destination.workspace_path != DEFAULT_WORKSPACE_PATH
        or not destination.host
        or any(ord(char) < 33 or ord(char) == 127 for char in destination.host)
        or not 1 <= destination.port <= 65535
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace endpoint attestation is invalid",
        )

    return _StatelessUploadAuthority(
        thread_id=thread_id,
        workspace_generation=expected_generation,
        runtime_incarnation=expected_runtime,
        host_key_fingerprint=expected_host_key_fingerprint,
        target=destination,
    )


def _resolve_pinned_k8s_upload_authority(
    thread: dict[str, Any],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
) -> _StatelessUploadAuthority:
    """Validate one freshly-read pinned Kubernetes workspace tuple."""

    if (
        thread.get("execution_lane") != "pinned"
        or thread.get("status") not in {"created", "active", "idle", "awaiting_user"}
        or thread.get("runtime_retirement_token") is not None
        or not is_kubernetes_thread_upload_destination(thread)
        or not isinstance(destination, _SshTarget)
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace authority is unavailable",
        )
    thread_id = str(thread.get("id") or "").strip()
    if not thread_id:
        raise ThreadUploadError(status_code=404, detail="Thread is unavailable")

    metadata = _thread_metadata(thread)
    workspace = metadata.get("workspace_container")
    binding = metadata.get("_workspace_binding")
    if not isinstance(workspace, dict) or not isinstance(binding, dict):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace attestation is unavailable",
        )
    backing_id = binding.get("backing_id")
    if not (
        workspace.get("status") == "ready"
        and workspace.get("provisioner") in {"k8s", "kubernetes"}
        and binding.get("kind") == "remote"
        and isinstance(backing_id, str)
        and backing_id.startswith(("k8s-pod:", "k8s-pvc:"))
        and (
            _RESTORE_REQUIRED_KEY not in workspace
            or workspace.get(_RESTORE_REQUIRED_KEY) is False
        )
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace attestation is unavailable",
        )

    expected_generation = _parse_required_uuid(
        expected_workspace_generation, label="generation"
    )
    expected_runtime = _parse_required_uuid(
        expected_runtime_incarnation, label="runtime incarnation"
    )
    bound_generation = _parse_required_uuid(
        binding.get("generation"), label="generation"
    )
    endpoint_generation = _parse_required_uuid(
        workspace.get(_WORKSPACE_GENERATION_KEY), label="endpoint generation"
    )
    runtime_incarnation = _parse_required_uuid(
        workspace.get(_RUNTIME_INCARNATION_KEY), label="runtime incarnation"
    )
    if not (
        expected_generation == bound_generation == endpoint_generation
        and expected_runtime == runtime_incarnation
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace authority changed before upload",
        )

    bound_fingerprint = binding.get("ssh_host_key_fingerprint")
    if not _valid_ssh_fingerprint(expected_host_key_fingerprint) or not (
        _valid_ssh_fingerprint(bound_fingerprint)
        and secrets.compare_digest(
            expected_host_key_fingerprint,
            str(bound_fingerprint),
        )
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace SSH identity changed before upload",
        )
    resolved = resolve_thread_upload_destination(thread)
    if not isinstance(resolved, _SshTarget) or resolved != destination:
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace endpoint changed before upload",
        )
    if (
        destination.username != DEFAULT_USERNAME
        or destination.workspace_path != DEFAULT_WORKSPACE_PATH
        or not destination.host
        or any(ord(char) < 33 or ord(char) == 127 for char in destination.host)
        or not 1 <= destination.port <= 65535
    ):
        raise ThreadUploadError(
            status_code=409,
            detail="Pinned Kubernetes workspace endpoint is invalid",
        )
    return _StatelessUploadAuthority(
        thread_id=thread_id,
        workspace_generation=expected_generation,
        runtime_incarnation=expected_runtime,
        host_key_fingerprint=expected_host_key_fingerprint,
        target=destination,
    )


def _resolve_vm_upload_authority(
    thread: dict[str, Any],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
) -> _StatelessUploadAuthority:
    """Validate the exact VM tuple captured by a durable operation lease."""

    if thread.get("execution_lane") != "pinned" or not isinstance(
        destination, _SshTarget
    ):
        raise ThreadUploadError(409, "Pinned VM upload authority is unavailable")
    metadata = _thread_metadata(thread)
    if _workspace_backend(metadata) != "vm":
        raise ThreadUploadError(409, "VM is not the selected workspace tier")
    vm = metadata.get("vm") or {}
    try:
        generation = UUID(str(vm.get("provision_generation")))
        launcher = UUID(str(vm.get("active_pod_uid")))
        expected_generation = UUID(str(expected_workspace_generation))
        expected_launcher = UUID(str(expected_runtime_incarnation))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ThreadUploadError(409, "Pinned VM identity is malformed") from exc
    if not (
        vm.get("status") == "ready"
        and vm.get("identity_authenticated") is True
        and str(vm.get("identity_provision_generation") or "") == str(generation)
        and generation == expected_generation
        and launcher == expected_launcher
        and secrets.compare_digest(
            str(vm.get("ssh_host_key_fingerprint") or ""),
            expected_host_key_fingerprint,
        )
    ):
        raise ThreadUploadError(409, "Pinned VM identity changed before upload")
    resolved = resolve_thread_upload_destination(thread)
    if resolved != destination:
        raise ThreadUploadError(409, "Pinned VM endpoint changed before upload")
    return _StatelessUploadAuthority(
        thread_id=str(thread.get("id") or ""),
        workspace_generation=generation,
        runtime_incarnation=launcher,
        host_key_fingerprint=expected_host_key_fingerprint,
        target=destination,
    )


async def _require_exact_live_runtime(authority_probe: RuntimeAuthorityProbe) -> None:
    """Require positive Kubernetes evidence for the exact immutable Pod UID."""

    try:
        state = await authority_probe()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise ThreadUploadError(
            status_code=503,
            detail="Stateless workspace runtime authority could not be verified",
        ) from exc
    if state == "exact_live":
        return
    if state in {"exact_absent", "replacement"}:
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless workspace runtime changed before upload completed",
        )
    raise ThreadUploadError(
        status_code=503,
        detail="Stateless workspace runtime authority is ambiguous",
    )


def _ensure_remote_dir(sftp: Any, path: str) -> None:
    """mkdir -p over SFTP. Tolerates parts that already exist."""
    parts = [p for p in path.split("/") if p]
    cursor = "/" if path.startswith("/") else ""
    for part in parts:
        cursor = posixpath.join(cursor, part) if cursor else part
        try:
            sftp.stat(cursor)
        except FileNotFoundError:
            sftp.mkdir(cursor)


def _sftp_write_files(
    target: _SshTarget,
    payloads: list[tuple[str, bytes, str]],
) -> list[UploadedFile]:
    """Synchronous helper that opens one SFTP session and writes all files.

    Each payload tuple is ``(filename, data, mime_type)``. A ``.zip`` payload
    is expanded into ``uploads/<stem>/…`` — see
    ``_expand_payloads_for_extraction``; a corrupt/unsafe zip falls back to
    being written verbatim like any other file.
    """
    client = _pinned_ssh_client(target)
    _connect_pinned_sftp(client, target)

    try:
        sftp = client.open_sftp()
        try:
            get_channel = getattr(sftp, "get_channel", None)
            if callable(get_channel):
                channel = get_channel()
                settimeout = getattr(channel, "settimeout", None)
                if callable(settimeout):
                    settimeout(45)
            uploads_dir = posixpath.join(target.workspace_path, UPLOADS_SUBDIR)
            _ensure_remote_dir(sftp, uploads_dir)

            # One listing, resolved in-memory from here — equivalent to the
            # old per-candidate sftp.stat() probe for this single connection
            # (nothing else writes through it concurrently), and it's what a
            # zip's stem name needs anyway: claiming a whole subtree can't be
            # done with a single stat probe the way one flat name can.
            taken: set[str] = set()
            try:
                taken.update(sftp.listdir(uploads_dir))
            except OSError:
                pass  # just created above; nothing to collide with yet

            expanded = _expand_payloads_for_extraction(payloads, taken)

            results: list[UploadedFile] = []
            for name, data, mime_type in expanded:
                remote_path = posixpath.join(uploads_dir, name)
                parent = posixpath.dirname(remote_path)
                if parent != uploads_dir:
                    _ensure_remote_dir(sftp, parent)
                with sftp.open(remote_path, "wb") as f:
                    f.write(data)
                results.append(
                    UploadedFile(
                        name=name,
                        size=len(data),
                        mime_type=mime_type or "application/octet-stream",
                        path=posixpath.join(UPLOADS_SUBDIR, name),
                    )
                )
                logger.info(
                    "Uploaded %s (%d bytes) to %s",
                    name,
                    len(data),
                    target.host,
                )
            return results
        finally:
            sftp.close()
    finally:
        client.close()


def _is_sftp_missing(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if asyncssh is None:
        return False
    return isinstance(
        exc,
        tuple(
            error_type
            for error_type in (
                getattr(asyncssh, "SFTPNoSuchFile", None),
                getattr(asyncssh, "SFTPNoSuchPath", None),
            )
            if isinstance(error_type, type)
        ),
    )


async def _ensure_remote_dir_async(sftp: Any, path: str) -> None:
    """AsyncSSH equivalent of ``_ensure_remote_dir`` for pinned SFTP."""

    parts = [part for part in path.split("/") if part]
    cursor = "/" if path.startswith("/") else ""
    for part in parts:
        cursor = posixpath.join(cursor, part) if cursor else part
        try:
            await sftp.stat(cursor)
        except Exception as exc:
            if not _is_sftp_missing(exc):
                raise
            await sftp.mkdir(cursor)


async def _remove_attested_uploads(sftp: Any, paths: Iterable[str]) -> None:
    """Best-effort removal when the exact Pod authority changes post-write."""

    for path in reversed(list(paths)):
        try:
            await sftp.remove(path)
        except Exception:
            logger.warning(
                "Could not remove upload %s after runtime authority changed",
                path,
                exc_info=True,
            )


async def _write_attested_sftp_files(
    sftp: Any,
    *,
    authority: _StatelessUploadAuthority,
    payloads: list[tuple[str, bytes, str]],
    authority_probe: RuntimeAuthorityProbe,
) -> list[UploadedFile]:
    """Write one batch only while the immutable Pod UID remains exact-live."""

    uploads_dir = posixpath.join(authority.target.workspace_path, UPLOADS_SUBDIR)
    await _ensure_remote_dir_async(sftp, uploads_dir)

    taken: set[str] = set()
    try:
        taken.update(await sftp.listdir(uploads_dir))
    except Exception as exc:
        if not _is_sftp_missing(exc):
            raise

    expanded = _expand_payloads_for_extraction(payloads, taken)

    # Directory setup/name planning can take long enough for a same-name Pod
    # replacement. This is the final positive UID proof immediately before the
    # first payload byte is opened for writing.
    await _require_exact_live_runtime(authority_probe)

    results: list[UploadedFile] = []
    created_paths: list[str] = []
    try:
        for name, data, mime_type in expanded:
            remote_path = posixpath.join(uploads_dir, name)
            parent = posixpath.dirname(remote_path)
            if parent != uploads_dir:
                await _ensure_remote_dir_async(sftp, parent)
            # Exclusive creation closes the small race between the directory
            # listing and open without ever overwriting an agent-created file.
            async with sftp.open(remote_path, "xb") as remote_file:
                created_paths.append(remote_path)
                await remote_file.write(data)
            results.append(
                UploadedFile(
                    name=name,
                    size=len(data),
                    mime_type=mime_type or "application/octet-stream",
                    path=posixpath.join(UPLOADS_SUBDIR, name),
                )
            )

        # A post-write control-plane proof detects a replacement racing the
        # batch. On failure, remove every newly claimed path over this same
        # authenticated channel before surfacing the refusal.
        await _require_exact_live_runtime(authority_probe)
    except asyncio.CancelledError:
        await _remove_attested_uploads(sftp, created_paths)
        raise
    except Exception:
        # Never leave successfully-created batch members behind after
        # cancellation, transport failure, or authority loss. Cleanup is
        # bounded by the outer operation timeout.
        await _remove_attested_uploads(sftp, created_paths)
        raise

    for result in results:
        logger.info(
            "Uploaded %s (%d bytes) to attested runtime %s",
            result.name,
            result.size,
            authority.runtime_incarnation,
        )
    return results


def _async_sftp_entry_type(attrs: Any) -> int:
    """Return an AsyncSSH file type without mistaking permissions for mode."""

    entry_type = getattr(attrs, "type", None)
    if entry_type in {
        asyncssh.FILEXFER_TYPE_DIRECTORY,
        asyncssh.FILEXFER_TYPE_SYMLINK,
        asyncssh.FILEXFER_TYPE_REGULAR,
        asyncssh.FILEXFER_TYPE_SPECIAL,
        asyncssh.FILEXFER_TYPE_SOCKET,
        asyncssh.FILEXFER_TYPE_CHAR_DEVICE,
        asyncssh.FILEXFER_TYPE_BLOCK_DEVICE,
        asyncssh.FILEXFER_TYPE_FIFO,
    }:
        return int(entry_type)
    # Compatibility for narrow test doubles and alternate SFTP clients, whose
    # st_mode combines kind and permission bits. AsyncSSH's permissions field
    # deliberately does not: in SFTP v4 it can contain only the low 12 bits.
    mode = getattr(attrs, "st_mode", None)
    if mode is not None:
        if stat_module.S_ISDIR(mode):
            return asyncssh.FILEXFER_TYPE_DIRECTORY
        if stat_module.S_ISLNK(mode):
            return asyncssh.FILEXFER_TYPE_SYMLINK
        return asyncssh.FILEXFER_TYPE_REGULAR
    raise ThreadUploadError(
        status_code=409,
        detail="Workspace did not report a known file type; refusing to delete",
    )


async def _resolve_attested_delete_target(
    sftp: Any, uploads_dir: str, relpath: str
) -> str | None:
    """Resolve one delete target while refusing symlinked parent components."""

    try:
        base = await sftp.lstat(uploads_dir)
    except Exception as exc:
        if _is_sftp_missing(exc):
            return None
        raise
    base_type = _async_sftp_entry_type(base)
    if base_type != asyncssh.FILEXFER_TYPE_DIRECTORY:
        raise ThreadUploadError(
            status_code=409,
            detail="Workspace uploads directory is not a directory",
        )

    parts = relpath.split("/")
    cursor = uploads_dir
    for index, part in enumerate(parts):
        cursor = posixpath.join(cursor, part)
        try:
            attrs = await sftp.lstat(cursor)
        except Exception as exc:
            if _is_sftp_missing(exc):
                return None
            raise
        entry_type = _async_sftp_entry_type(attrs)
        if index < len(parts) - 1:
            if entry_type == asyncssh.FILEXFER_TYPE_SYMLINK:
                raise ThreadUploadError(
                    status_code=409,
                    detail="Upload path traverses a symbolic link",
                )
            if entry_type != asyncssh.FILEXFER_TYPE_DIRECTORY:
                return None
    return cursor


async def _delete_attested_sftp_tree(sftp: Any, path: str, *, depth: int = 0) -> None:
    """AsyncSSH counterpart of ``_sftp_delete_tree`` without following links."""

    if depth > MAX_DELETE_TREE_DEPTH:
        raise ThreadUploadError(
            status_code=409,
            detail="Upload path is nested too deeply to delete",
        )
    try:
        attrs = await sftp.lstat(path)
    except Exception as exc:
        if _is_sftp_missing(exc):
            return
        raise
    if _async_sftp_entry_type(attrs) == asyncssh.FILEXFER_TYPE_DIRECTORY:
        for name in await sftp.listdir(path):
            if not isinstance(name, str) or name in {"", ".", ".."} or "/" in name:
                raise ThreadUploadError(
                    status_code=409,
                    detail="Workspace returned an invalid directory entry",
                )
            await _delete_attested_sftp_tree(
                sftp,
                posixpath.join(path, name),
                depth=depth + 1,
            )
        await sftp.rmdir(path)
        return
    await sftp.remove(path)


async def delete_file_from_attested_stateless_workspace(
    thread: dict[str, Any],
    relpath: str,
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
    _runtime_kind: str = "stateless",
) -> str | None:
    """Delete one upload only from the exact-live, pinned sandbox runtime."""

    safe_relpath = _safe_upload_relpath(relpath)
    if safe_relpath is None:
        raise ThreadUploadError(status_code=400, detail="Invalid upload path")
    resolver = (
        _resolve_pinned_k8s_upload_authority
        if _runtime_kind == "pinned"
        else _resolve_stateless_upload_authority
    )
    authority = resolver(
        thread,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    )

    async def _delete() -> str | None:
        await _require_exact_live_runtime(authority_probe)
        async with asyncio.timeout(45):
            async with _ATTESTED_SFTP_POOL.checkout(
                thread_id=authority.transport_pool_thread_id,
                generation=authority.workspace_generation,
                host=authority.target.host,
                port=authority.target.port,
                fingerprint=authority.host_key_fingerprint,
                key_path=authority.target.key_path,
            ) as sftp:
                await _require_exact_live_runtime(authority_probe)
                uploads_dir = posixpath.join(
                    authority.target.workspace_path, UPLOADS_SUBDIR
                )
                remote_path = await _resolve_attested_delete_target(
                    sftp, uploads_dir, safe_relpath
                )
                if remote_path is None:
                    return None
                # Component walking can await repeatedly. Re-probe immediately
                # before the first irreversible unlink/rmdir.
                await _require_exact_live_runtime(authority_probe)
                await _delete_attested_sftp_tree(sftp, remote_path)
                await _require_exact_live_runtime(authority_probe)
                return safe_relpath

    try:
        return await _joined_async_call(_delete())
    except asyncio.CancelledError:
        raise
    except ThreadUploadError:
        raise
    except TimeoutError as exc:
        raise ThreadUploadError(
            status_code=503,
            detail="Stateless workspace upload delete timed out",
        ) from exc
    except CanvasSSHError as exc:
        raise ThreadUploadError(
            status_code=exc.status_code,
            detail="Stateless workspace pinned SSH connection failed",
        ) from exc
    except Exception as exc:
        logger.warning(
            "Pinned SFTP delete failed for stateless thread %s: %s",
            authority.thread_id,
            exc,
        )
        raise ThreadUploadError(
            status_code=502,
            detail="Could not delete from the attested stateless workspace",
        ) from exc


async def delete_file_from_attested_k8s_workspace(
    thread: dict[str, Any],
    relpath: str,
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
) -> str | None:
    """Delete only through a freshly attested pinned Kubernetes runtime."""

    return await delete_file_from_attested_stateless_workspace(
        thread,
        relpath,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
        authority_probe=authority_probe,
        _runtime_kind="pinned",
    )


async def delete_file_from_attested_vm_workspace(
    thread: dict[str, Any],
    relpath: str,
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
) -> str | None:
    safe_relpath = _safe_upload_relpath(relpath)
    if safe_relpath is None:
        raise ThreadUploadError(400, "Invalid upload path")
    authority = _resolve_vm_upload_authority(
        thread,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    )

    async def _delete() -> str | None:
        await _require_exact_live_runtime(authority_probe)
        async with asyncio.timeout(45):
            async with _ATTESTED_SFTP_POOL.checkout(
                thread_id=authority.transport_pool_thread_id,
                generation=authority.workspace_generation,
                host=authority.target.host,
                port=authority.target.port,
                fingerprint=authority.host_key_fingerprint,
                key_path=authority.target.key_path,
            ) as sftp:
                await _require_exact_live_runtime(authority_probe)
                uploads_dir = posixpath.join(
                    authority.target.workspace_path, UPLOADS_SUBDIR
                )
                remote_path = await _resolve_attested_delete_target(
                    sftp, uploads_dir, safe_relpath
                )
                if remote_path is None:
                    return None
                await _require_exact_live_runtime(authority_probe)
                await _delete_attested_sftp_tree(sftp, remote_path)
                await _require_exact_live_runtime(authority_probe)
                return safe_relpath

    try:
        return await _joined_async_call(_delete())
    except asyncio.CancelledError:
        raise
    except ThreadUploadError:
        raise
    except TimeoutError as exc:
        raise ThreadUploadError(503, "Pinned VM upload delete timed out") from exc
    except CanvasSSHError as exc:
        raise ThreadUploadError(
            exc.status_code, "Pinned VM SSH connection failed"
        ) from exc
    except Exception as exc:
        raise ThreadUploadError(502, "Could not delete from the pinned VM") from exc


def _virtual_write_files(
    target: _VirtualTarget,
    payloads: list[tuple[str, bytes, str]],
    *,
    store: Any | None = None,
) -> list[UploadedFile]:
    """Synchronous helper that writes all files into the thread's object store.

    One store for the whole batch, and **one** listing to seed collision
    resolution — probing per file would cost an rclone subprocess per probe.
    A ``.zip`` payload is expanded into ``uploads/<stem>/…`` — see
    ``_expand_payloads_for_extraction``; a corrupt/unsafe zip falls back to
    being written verbatim like any other file.

    No directory marker is written: ``VirtualWorkspaceBackend.is_dir`` derives
    directories from key prefixes, so the first object makes ``uploads/`` exist
    for the agent's ``list_dir``/``walk``.

    Args:
        store: Injected object store, for tests. Built from ``target.spec`` and
            closed here when not supplied.
    """
    owned = store is None
    if store is None:
        from shared.runtime.core.backends.rclone import RcloneObjectStore

        store = RcloneObjectStore(
            remote_type=str(target.spec["type"]),
            config=target.spec.get("config") or {},
            root=str(target.spec.get("root") or ""),
        )

    uploads_prefix = f"{target.prefix}{UPLOADS_SUBDIR}/"
    try:
        # Top-level component of every existing key, flat or nested — a zip
        # extracted earlier left "bundle/..." keys with no "bundle" key of
        # its own, so a plain remainder-has-no-slash filter (the pre-zip
        # version of this loop) would miss it and let a new "bundle.zip"
        # collide with — and mix its members into — that directory.
        taken: set[str] = set()
        for info in store.list(uploads_prefix):
            remainder = info.key[len(uploads_prefix) :]
            if remainder:
                taken.add(remainder.split("/", 1)[0])

        expanded = _expand_payloads_for_extraction(payloads, taken)

        results: list[UploadedFile] = []
        for name, data, mime_type in expanded:
            store.put(f"{uploads_prefix}{name}", data)
            results.append(
                UploadedFile(
                    name=name,
                    size=len(data),
                    mime_type=mime_type or "application/octet-stream",
                    path=posixpath.join(UPLOADS_SUBDIR, name),
                )
            )
            logger.info(
                "Uploaded %s (%d bytes) to %s%s",
                name,
                len(data),
                uploads_prefix,
                name,
            )
        return results
    finally:
        if owned:
            store.close()


def _virtual_purge_prefix(
    target: _VirtualTarget,
    *,
    store: Any | None = None,
) -> bool:
    """Delete one exact virtual backing and prove its prefix is empty."""

    owned = store is None
    if store is None:
        from shared.runtime.core.backends.rclone import RcloneObjectStore

        store = RcloneObjectStore(
            remote_type=str(target.spec["type"]),
            config=target.spec.get("config") or {},
            root=str(target.spec.get("root") or ""),
        )
    try:
        for info in store.list(target.prefix):
            if store.delete(info.key) is not True:
                return False
        return not store.list(target.prefix)
    finally:
        if owned:
            store.close()


# =============================================================================
# Deletion
#
# Eager upload (the cockpit starts the transfer when a file is attached, not
# when the message is sent) makes "remove this attachment" a request that can
# arrive *after* the bytes have already landed. Without a delete, cancelling
# is a lie and attach/remove cycles pile up `_1`/`_2` copies in a directory
# the agent can list and read. See knowledge-base/knowledge/features/session_attachment_send_flow.md
# §9.1.
# =============================================================================

# A zip extracted into uploads/<stem>/… can nest arbitrarily, but not
# unboundedly: this caps the recursive SFTP walk so a pathological tree
# refuses cleanly instead of blowing the Python stack inside a request.
MAX_DELETE_TREE_DEPTH = 64


def _entry_mode(attrs: Any) -> int:
    """Return an SFTP entry's mode, refusing when the server did not report one.

    ``SSH_FILEXFER_ATTR_PERMISSIONS`` is **optional** in the SFTP protocol, so
    ``SFTPAttributes.st_mode`` is legitimately ``None`` against a server that
    omits it. The original ``attrs.st_mode or 0`` turned that into ``0``, and
    ``S_ISLNK(0)`` is False — so every symlink check below passed silently and
    the whole confinement guard failed **open**. Demonstrated, not inferred:
    a fixture whose ``lstat`` returns ``st_mode=None`` let
    ``uploads/escape/authorized_keys`` resolve and delete the key file.

    A guard may only fail closed. An entry whose type we cannot read is one we
    refuse to walk through, recurse into, or unlink.

    Raises:
        ThreadUploadError: 409 when the mode is unknown.
    """
    mode = getattr(attrs, "st_mode", None)
    if mode is None:
        raise ThreadUploadError(
            status_code=409,
            detail="Workspace did not report the file type; refusing to delete",
        )
    return int(mode)


def _sftp_resolve_delete_target(
    sftp: Any, uploads_dir: str, relpath: str
) -> str | None:
    """Walk ``relpath`` component by component, refusing symlinked parents.

    ``lstat`` declines to follow only the *final* component of a path — every
    component above it is resolved by the SFTP server before we ever see an
    answer. So a leaf-only guard is not a guard at all: with
    ``uploads/escape -> ~/.ssh`` planted by the agent, ``escape/authorized_keys``
    lstat's as a perfectly ordinary regular file and gets removed. Checking
    every component (plus ``uploads/`` itself, which the agent could replace
    with a link to ``$HOME``) is what actually confines the delete.

    The leaf may legitimately be a symlink; ``_sftp_delete_tree`` unlinks it
    as a link rather than following it.

    Returns:
        The absolute remote path of the leaf, or None when any component is
        missing — an absent file is a 404, not an error.

    Raises:
        ThreadUploadError: 409 when a component above the leaf, or
            ``uploads/`` itself, is a symlink.
    """
    try:
        base = sftp.lstat(uploads_dir)
    except OSError:
        return None  # nothing has ever been uploaded here
    if stat_module.S_ISLNK(_entry_mode(base)):
        raise ThreadUploadError(
            status_code=409,
            detail="Workspace uploads directory is not a directory",
        )

    parts = relpath.split("/")
    cursor = uploads_dir
    for index, part in enumerate(parts):
        cursor = posixpath.join(cursor, part)
        try:
            attrs = sftp.lstat(cursor)
        except OSError:
            return None
        # Read the mode for EVERY component, including the leaf: an entry whose
        # type the server declines to report is refused outright rather than
        # defaulted to "not a symlink" (see _entry_mode).
        mode = _entry_mode(attrs)
        if index < len(parts) - 1 and stat_module.S_ISLNK(mode):
            raise ThreadUploadError(
                status_code=409,
                detail="Upload path traverses a symbolic link",
            )
    return cursor


def _sftp_delete_tree(sftp: Any, path: str, *, depth: int = 0) -> None:
    """Remove ``path``, recursing first when it is a real directory.

    SFTP has no ``rm -rf``, so a zip-extracted ``uploads/<stem>/`` has to be
    walked explicitly and ``rmdir``'d bottom-up.

    ``lstat`` — never ``stat`` — decides what ``path`` is, so a symlink takes
    the ``remove`` branch (unlinking the link itself, exactly like ``rm``)
    instead of the directory branch. Following one would turn this into a
    recursive delete of whatever it points at. ``listdir_attr`` returns the
    server's readdir attributes, which are ``lstat``-equivalent, so a
    symlinked *child* inside the tree gets the same treatment.
    """
    if depth > MAX_DELETE_TREE_DEPTH:
        raise ThreadUploadError(
            status_code=409,
            detail="Upload path is nested too deeply to delete",
        )
    try:
        attrs = sftp.lstat(path)
    except OSError:
        return  # vanished under us (concurrent delete) — the desired state
    if stat_module.S_ISDIR(_entry_mode(attrs)):
        for entry in sftp.listdir_attr(path):
            _sftp_delete_tree(
                sftp, posixpath.join(path, entry.filename), depth=depth + 1
            )
        sftp.rmdir(path)
        return
    sftp.remove(path)


def _sftp_delete_file(target: _SshTarget, relpath: str) -> bool:
    """Synchronous helper that opens one SFTP session and deletes one entry.

    ``relpath`` must already have passed ``_safe_upload_relpath``. Connects
    exactly as ``_sftp_write_files`` does, including its 15s
    connect/banner/auth timeouts, so an unreachable workspace produces the
    same honest 502 rather than a hung request.

    Returns:
        True when something was removed, False when there was nothing there.
    """
    client = _pinned_ssh_client(target)
    _connect_pinned_sftp(client, target)

    try:
        sftp = client.open_sftp()
        try:
            get_channel = getattr(sftp, "get_channel", None)
            if callable(get_channel):
                channel = get_channel()
                settimeout = getattr(channel, "settimeout", None)
                if callable(settimeout):
                    settimeout(45)
            uploads_dir = posixpath.join(target.workspace_path, UPLOADS_SUBDIR)
            remote_path = _sftp_resolve_delete_target(sftp, uploads_dir, relpath)
            if remote_path is None:
                return False
            _sftp_delete_tree(sftp, remote_path)
            logger.info("Deleted %s from %s", remote_path, target.host)
            return True
        finally:
            sftp.close()
    finally:
        client.close()


def _virtual_delete_file(
    target: _VirtualTarget,
    relpath: str,
    *,
    store: Any | None = None,
) -> bool:
    """Synchronous helper that deletes one upload from the thread's store.

    ``relpath`` must already have passed ``_safe_upload_relpath``.

    A flat key space has no directories, so an extracted zip is just a set of
    keys sharing a prefix: removing the stem means listing ``<key>/`` and
    deleting each key individually. The listing is only reached when the
    exact key missed, so an ordinary flat upload still costs a single rclone
    subprocess. The trailing separator matters — a bare ``startswith`` would
    sweep ``bundle_1.txt`` away with ``bundle``.

    Args:
        store: Injected object store, for tests. Built from ``target.spec``
            and closed here when not supplied.

    Returns:
        True when at least one key was removed, False when nothing matched.
    """
    owned = store is None
    if store is None:
        from shared.runtime.core.backends.rclone import RcloneObjectStore

        store = RcloneObjectStore(
            remote_type=str(target.spec["type"]),
            config=target.spec.get("config") or {},
            root=str(target.spec.get("root") or ""),
        )
    key = f"{target.prefix}{UPLOADS_SUBDIR}/{relpath}"
    try:
        if store.delete(key):
            logger.info("Deleted %s", key)
            return True

        removed = 0
        for info in store.list(f"{key}/"):
            if store.delete(info.key):
                removed += 1
        if removed:
            logger.info("Deleted %d keys under %s/", removed, key)
        return removed > 0
    finally:
        if owned:
            store.close()


async def _joined_blocking_call(func, /, *args, **kwargs):
    """Do not release lifecycle ownership while a blocking effect still runs."""
    return await joined_blocking_call(func, *args, **kwargs)


async def _joined_async_call(awaitable):
    """Compatibility seam for the shared cancellation-deferred join."""

    return await joined_async_call(awaitable)


async def purge_attested_stateless_virtual_workspace(
    thread: dict[str, Any],
) -> bool:
    """Purge the exact virtual backing of a terminal stateless thread.

    The caller owns the stateless lifecycle advisory lock. Resolution is
    repeated from the authoritative ended row, so a configuration/backing
    mismatch fails before any rclone effect. Cancellation is delayed until the
    blocking store operation is terminal; a cancelled End can never leave a
    stale deleter racing Resume or a retry.
    """

    if thread.get("execution_lane") != "stateless" or thread.get("status") != "ended":
        return False
    backend, refusal = stateless_session_workspace_check(thread)
    if backend != "virtual" or refusal is not None:
        return False
    try:
        target = resolve_thread_upload_destination(thread)
    except ThreadUploadError:
        return False
    if not isinstance(target, _VirtualTarget):
        return False
    try:
        return bool(await _joined_blocking_call(_virtual_purge_prefix, target))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Failed to purge stateless virtual workspace for thread %s: %s",
            thread.get("id"),
            exc,
        )
        return False


async def purge_attested_pinned_virtual_workspace(
    thread: dict[str, Any],
    *,
    expected_runtime_generation: str,
    expected_retirement_token: str,
) -> bool:
    """Purge one exact pinned virtual prefix under authorized retirement.

    The caller holds the thread lifecycle lock.  The immutable T/G pair closes
    every upload/Resume path while the joined blocking rclone operation runs;
    the database cleanup CAS rechecks the same binding before removing it.
    """

    if (
        thread.get("execution_lane") != "pinned"
        or str(thread.get("runtime_generation") or "")
        != str(expected_runtime_generation)
        or str(thread.get("runtime_retirement_token") or "")
        != str(expected_retirement_token)
        or thread.get("runtime_retirement_authorized_at") is None
        or thread.get("runtime_retirement_permanent") is not True
    ):
        return False
    try:
        target = resolve_thread_upload_destination(thread)
    except ThreadUploadError:
        return False
    if not isinstance(target, _VirtualTarget):
        return False
    try:
        return bool(await _joined_blocking_call(_virtual_purge_prefix, target))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Failed to purge pinned virtual workspace for thread %s: %s",
            thread.get("id"),
            exc,
        )
        return False


def recorded_virtual_upgrade_bindings(
    metadata: dict[str, Any], *, vm: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """The virtual bindings a Session's records still name as an old workspace.

    C1 keeps ``workspace_container.seed_from`` until its copy is verified; C2
    keeps ``vm.upgrade_seed.source``; a pinned VM upgrade records
    ``vm.upgrade_source`` (and before E3 kept ``_workspace_binding`` beside the
    VM). ``vm`` overrides the metadata's, for a retirement that captured it.
    """

    def _get(value: Any, *path: str) -> Any:
        for key in path:
            if not isinstance(value, dict):
                return None
            value = value.get(key)
        return value

    vm_state = vm if vm is not None else _get(metadata, "vm")
    candidates = [
        _get(metadata, "workspace_container", "seed_from", "binding"),
        _get(vm_state, "upgrade_seed", "source", "binding"),
        _get(vm_state, "upgrade_source", "binding"),
        _get(metadata, "_workspace_binding"),
    ]
    return [
        binding
        for binding in candidates
        if isinstance(binding, dict) and binding.get("kind") == "virtual"
    ]


async def purge_virtual_workspace_leftover(
    thread: dict[str, Any],
    *,
    recorded_bindings: Iterable[dict[str, Any]] = (),
) -> bool:
    """Purge the ``threads/<id>/`` prefix a Session left in the virtual store.

    A workspace upgrade (Virtual to Container or VM) copies the files and keeps
    the old prefix as its fallback, so the Session's own virtual purge, which
    runs only while it is still virtual, never removes it. The prefix derives
    from the thread id, so this purges it even where no record of the upgrade
    survives (C1 clears its seed marker once the copy is verified). The caller
    holds the permanent delete's lifecycle fence. A recorded binding naming a
    different store fails closed, as the virtual purge does.
    """

    spec = workspace_binding.virtual_workspace_rclone_spec()
    if not spec or spec.get("type") == "memory":
        # No durable store, so nothing can have been left in one.
        return True
    thread_id = str(thread.get("id") or "")
    expected = virtual_thread_backing_id(thread_id, spec)
    if any(
        isinstance(binding, dict) and binding.get("backing_id") != expected
        for binding in recorded_bindings
    ):
        return False
    if workspace_binding.shutil.which("rclone") is None:
        return False
    target = _VirtualTarget(spec=spec, prefix=f"threads/{thread_id}/")
    try:
        return bool(await _joined_blocking_call(_virtual_purge_prefix, target))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Failed to purge the old virtual workspace of thread %s: %s",
            thread_id,
            exc,
        )
        return False


@asynccontextmanager
async def _virtual_upload_slot():
    """Acquire an object-store upload slot with a bounded queue wait."""
    acquired = False
    try:
        try:
            await asyncio.wait_for(
                _VIRTUAL_UPLOAD_SEMAPHORE.acquire(),
                timeout=max(0.1, VIRTUAL_UPLOAD_QUEUE_TIMEOUT),
            )
            acquired = True
        except (TimeoutError, asyncio.TimeoutError) as e:
            raise ThreadUploadError(
                status_code=503,
                detail="Upload capacity is currently exhausted — try again shortly",
            ) from e
        yield
    finally:
        if acquired:
            _VIRTUAL_UPLOAD_SEMAPHORE.release()


def _validate_upload_payloads(
    files: Iterable[tuple[str, bytes, str]],
) -> list[tuple[str, bytes, str]]:
    payloads = list(files)
    if not payloads:
        raise ThreadUploadError(status_code=400, detail="No files provided")
    if len(payloads) > MAX_FILES_PER_REQUEST:
        raise ThreadUploadError(
            status_code=400,
            detail=f"Maximum {MAX_FILES_PER_REQUEST} files per request",
        )
    for name, data, _ in payloads:
        if len(data) > MAX_FILE_SIZE:
            raise ThreadUploadError(
                status_code=413,
                detail=f"File '{name}' exceeds {MAX_FILE_SIZE // (1024 * 1024)}MB",
            )
    return payloads


async def upload_files_to_attested_stateless_workspace(
    thread: dict[str, Any],
    files: Iterable[tuple[str, bytes, str]],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
    _runtime_kind: str = "stateless",
) -> list[UploadedFile]:
    """Upload to one exact-live stateless Kubernetes sandbox over pinned SFTP.

    The caller must hold the distributed stateless workspace lifecycle lock and
    pass the values read from the fresh row under that lock.  This service then
    validates the complete tuple before network I/O, pins the SSH server key,
    qualifies pooled transports by immutable Pod UID, and positively probes
    that UID before connecting, immediately before the first byte, and after
    the final byte.  Virtual stateless workspaces continue through
    ``upload_files_to_thread_workspace`` while the route holds the same lock.
    """

    payloads = _validate_upload_payloads(files)
    resolver = (
        _resolve_pinned_k8s_upload_authority
        if _runtime_kind == "pinned"
        else _resolve_stateless_upload_authority
    )
    authority = resolver(
        thread,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    )

    async def _upload() -> list[UploadedFile]:
        # Do not even open a TCP connection without current positive UID evidence.
        await _require_exact_live_runtime(authority_probe)
        async with asyncio.timeout(45):
            async with _ATTESTED_SFTP_POOL.checkout(
                thread_id=authority.transport_pool_thread_id,
                generation=authority.workspace_generation,
                host=authority.target.host,
                port=authority.target.port,
                fingerprint=authority.host_key_fingerprint,
                key_path=authority.target.key_path,
            ) as sftp:
                # Pool checkout can wait behind another operation. Re-probe
                # before even creating upload directories on the connection.
                await _require_exact_live_runtime(authority_probe)
                return await _write_attested_sftp_files(
                    sftp,
                    authority=authority,
                    payloads=payloads,
                    authority_probe=authority_probe,
                )

    try:
        return await _joined_async_call(_upload())
    except asyncio.CancelledError:
        raise
    except ThreadUploadError:
        raise
    except TimeoutError as exc:
        raise ThreadUploadError(
            status_code=503,
            detail="Stateless workspace upload timed out",
        ) from exc
    except CanvasSSHError as exc:
        raise ThreadUploadError(
            status_code=exc.status_code,
            detail="Stateless workspace pinned SSH connection failed",
        ) from exc
    except Exception as exc:
        logger.warning(
            "Pinned SFTP upload failed for stateless thread %s: %s",
            authority.thread_id,
            exc,
        )
        raise ThreadUploadError(
            status_code=502,
            detail="Could not write to the attested stateless workspace",
        ) from exc


async def upload_files_to_attested_k8s_workspace(
    thread: dict[str, Any],
    files: Iterable[tuple[str, bytes, str]],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
) -> list[UploadedFile]:
    """Upload only through a freshly attested pinned Kubernetes runtime."""

    return await upload_files_to_attested_stateless_workspace(
        thread,
        files,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
        authority_probe=authority_probe,
        _runtime_kind="pinned",
    )


async def upload_files_to_attested_vm_workspace(
    thread: dict[str, Any],
    files: Iterable[tuple[str, bytes, str]],
    *,
    destination: _SshTarget | _VirtualTarget,
    expected_workspace_generation: str,
    expected_runtime_incarnation: str,
    expected_host_key_fingerprint: str,
    authority_probe: RuntimeAuthorityProbe,
) -> list[UploadedFile]:
    """Write through pinned SFTP while an exact VM lease remains renewable."""

    payloads = _validate_upload_payloads(files)
    authority = _resolve_vm_upload_authority(
        thread,
        destination=destination,
        expected_workspace_generation=expected_workspace_generation,
        expected_runtime_incarnation=expected_runtime_incarnation,
        expected_host_key_fingerprint=expected_host_key_fingerprint,
    )

    async def _upload() -> list[UploadedFile]:
        await _require_exact_live_runtime(authority_probe)
        async with asyncio.timeout(45):
            async with _ATTESTED_SFTP_POOL.checkout(
                thread_id=authority.transport_pool_thread_id,
                generation=authority.workspace_generation,
                host=authority.target.host,
                port=authority.target.port,
                fingerprint=authority.host_key_fingerprint,
                key_path=authority.target.key_path,
            ) as sftp:
                await _require_exact_live_runtime(authority_probe)
                return await _write_attested_sftp_files(
                    sftp,
                    authority=authority,
                    payloads=payloads,
                    authority_probe=authority_probe,
                )

    try:
        return await _joined_async_call(_upload())
    except asyncio.CancelledError:
        raise
    except ThreadUploadError:
        raise
    except TimeoutError as exc:
        raise ThreadUploadError(503, "Pinned VM workspace upload timed out") from exc
    except CanvasSSHError as exc:
        raise ThreadUploadError(
            exc.status_code, "Pinned VM SSH connection failed"
        ) from exc
    except Exception as exc:
        raise ThreadUploadError(502, "Could not write to the pinned VM") from exc


async def upload_files_to_thread_workspace(
    thread: dict[str, Any],
    files: Iterable[tuple[str, bytes, str]],
    *,
    destination: _SshTarget | _VirtualTarget | None = None,
) -> list[UploadedFile]:
    """Push files into the thread workspace's ``uploads/`` directory.

    Args:
        thread: Thread row (must contain ``metadata`` JSONB column).
        files: Iterable of ``(filename, content_bytes, mime_type)`` tuples.
        destination: Pre-resolved destination from
            ``resolve_thread_upload_destination``. Resolved here when omitted.

    Raises:
        ThreadUploadError: When the workspace is not reachable, the tier cannot
            accept files, the SSH key is missing, or per-file/total limits are
            exceeded.

    Returns:
        Metadata for each successfully uploaded file.
    """
    payloads = _validate_upload_payloads(files)

    if destination is None:
        destination = resolve_thread_upload_destination(thread)

    if isinstance(destination, _VirtualTarget):
        async with _virtual_upload_slot():
            if thread.get("execution_lane") == "stateless":
                return await _joined_blocking_call(
                    _virtual_write_files, destination, payloads
                )
            return await joined_blocking_call(
                _virtual_write_files, destination, payloads
            )
    if thread.get("execution_lane") == "stateless":
        raise ThreadUploadError(
            status_code=409,
            detail="Stateless sandbox uploads require exact runtime attestation",
        )
    return await joined_blocking_call(_sftp_write_files, destination, payloads)


async def delete_file_from_thread_workspace(
    thread: dict[str, Any],
    relpath: str,
    *,
    destination: _SshTarget | _VirtualTarget | None = None,
) -> str | None:
    """Remove one upload from the thread workspace's ``uploads/`` directory.

    ``relpath`` is relative to ``uploads/`` (``UploadedFile.name``, not the
    ``uploads/``-prefixed ``UploadedFile.path``). It may name a single file
    or the ``<stem>`` directory a zip was extracted into, in which case the
    whole subtree goes.

    The path is validated *before* the destination is resolved and before any
    connection is opened — the remote is never asked to police it.

    Args:
        thread: Thread row (must contain ``metadata`` JSONB column).
        relpath: Path under ``uploads/`` to remove.
        destination: Pre-resolved destination from
            ``resolve_thread_upload_destination``. Resolved here when omitted.

    Raises:
        ThreadUploadError: 400 for a path that escapes ``uploads/``, plus the
            same destination taxonomy the upload path uses (409 for a tier
            with no workspace or one that isn't ready, 502 for an unreachable
            one, 503 for missing config/capacity).

    Returns:
        The **normalized** path that was removed, relative to ``uploads/``
        (``bundle/sub/../a.txt`` in, ``bundle/a.txt`` out) — so the route can
        report what it actually deleted instead of echoing the caller's raw
        string back. None when there was nothing to remove, which the API
        layer turns into a 404.
    """
    safe_relpath = _safe_upload_relpath(relpath)
    if safe_relpath is None:
        raise ThreadUploadError(
            status_code=400,
            detail="Invalid upload path",
        )

    if destination is None:
        destination = resolve_thread_upload_destination(thread)

    if isinstance(destination, _VirtualTarget):
        # Each delete spawns another rclone subprocess — one per key when a
        # zip stem fans out — so it shares the writer's concurrency ceiling
        # rather than opening an unbounded second source of children.
        async with _virtual_upload_slot():
            if thread.get("execution_lane") == "stateless":
                removed = await _joined_blocking_call(
                    _virtual_delete_file, destination, safe_relpath
                )
            else:
                removed = await joined_blocking_call(
                    _virtual_delete_file, destination, safe_relpath
                )
    else:
        if thread.get("execution_lane") == "stateless":
            raise ThreadUploadError(
                status_code=409,
                detail="Stateless sandbox deletes require exact runtime attestation",
            )
        removed = await joined_blocking_call(
            _sftp_delete_file, destination, safe_relpath
        )
    return safe_relpath if removed else None
