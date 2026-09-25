"""Snapshot Service — S3-backed environment snapshot management.

Captures, stores, and retrieves agent VM/pod environment snapshots in
S3-compatible object storage (MinIO, AWS S3, etc.). Snapshots enable
on-demand IDE sessions and environment-aware job resume.

Architecture:
  - Each entity's snapshots live under ``s3://<bucket>/<entity_type>/<uuid>/``
    (``entity_type`` is ``jobs`` or ``threads``)
  - Phase-boundary snapshots are stored per-phase under ``phases/phase_<n>/``
  - The top-level ``manifest.json`` + ``env.tar.zst`` always point to the latest
  - ``history/<ts>/`` holds the last ``SNAPSHOT_KEEP_GENERATIONS`` prior
    canonical generations (§C3 no-clobber: the canonical write is staged,
    verified, then promoted — never overwritten in place)

Selection logic:
  - S3_ENDPOINT configured → S3 features enabled
  - No S3_ENDPOINT → gracefully disabled (all methods return None/False)
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import shlex
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.blocking_effect import (
    joined_async_call,
    joined_blocking_call,
)
from orchestrator.services.subprocess_effect import (
    SubprocessOutputLimit,
    communicate_bounded,
    create_owned_subprocess_exec,
    stop_and_reap,
)

logger = logging.getLogger(__name__)

SnapshotCaptureAuthority = Callable[[], Awaitable[bool]]

DEFAULT_BLOB_READ_MAX_BYTES = 64 * 1024 * 1024
MAX_SNAPSHOT_MANIFEST_BYTES = 16 * 1024 * 1024
_BLOB_STREAM_CHUNK_BYTES = 1024 * 1024


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_archive_max_bytes() -> int:
    try:
        gibibytes = int(os.environ.get("SNAPSHOT_MAX_SIZE_GB", "10"))
    except (TypeError, ValueError):
        gibibytes = 10
    return max(1, min(100, gibibytes)) * 1024 * 1024 * 1024


def _valid_ssh_sha256_fingerprint(value: object) -> bool:
    encoded = value[len("SHA256:") :] if isinstance(value, str) else ""
    return bool(
        isinstance(value, str)
        and value.startswith("SHA256:")
        and len(encoded) == 43
        and all(
            char in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            for char in encoded
        )
    )


def _terminal_generation_name(value: object) -> str:
    """Return the fixed S3 generation name for one completion command.

    Terminal workspace capture is replayed by a durable command.  A random or
    wall-clock history name would turn every replay into another snapshot, so
    this key accepts only a canonical UUID and is deliberately command-shaped.
    """

    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError("terminal snapshot generation is invalid")
    try:
        parsed = uuid.UUID(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("terminal snapshot generation is invalid") from exc
    if str(parsed) != value:
        raise ValueError("terminal snapshot generation is invalid")
    return f"completion-{value}"


def _snapshot_object_missing(exc: BaseException) -> bool:
    if not isinstance(exc, ClientError):
        return False
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}


async def _read_stream_tail(stream: Any, *, limit: int = 64 * 1024) -> bytes:
    """Drain a subprocess pipe without unbounded buffering, retaining its tail."""

    tail = bytearray()
    while True:
        chunk = await stream.read(16 * 1024)
        if not chunk:
            break
        tail.extend(chunk)
        if len(tail) > limit:
            del tail[: len(tail) - limit]
    return bytes(tail)


async def _joined_blocking_call(func, /, *args, **kwargs):
    """Run a blocking effect and never let cancellation orphan its thread.

    ``asyncio.to_thread`` cannot stop the underlying call. If terminal
    retirement releases its lifecycle lock while a cancelled S3 PUT is still
    running, that stale writer can overwrite a successor snapshot. Shield the
    worker, remember cancellation, and propagate it only after the effect has
    reached a real terminal result.
    """

    return await joined_blocking_call(func, *args, **kwargs)


def _snapshot_tar_pipeline(include_dirs: list[str], *, strict_terminal: bool) -> str:
    """Build the remote archive pipeline used by workspace snapshots.

    Terminal stateless snapshots are the only copy of an emptyDir workspace.
    They therefore retain Git object databases/repositories and run the whole
    tar-to-zstd pipeline under ``pipefail`` so a truncated producer can never
    be mistaken for a valid archive merely because zstd emitted some bytes.
    """

    exclude_patterns = [
        "--exclude=/var/cache/*",
        "--exclude=/tmp/*",
        "--exclude=*.pyc",
        "--exclude=__pycache__",
        "--exclude=node_modules/.cache",
        "--exclude=*/lost+found",
        "--exclude=*/node_modules/*",
    ]
    if not strict_terminal:
        exclude_patterns.extend(
            [
                "--exclude=.git/objects",
                "--exclude=*/repos/*",
            ]
        )
    stable_order = "--sort=name " if strict_terminal else ""
    pipeline = (
        f"tar {stable_order}--xattrs --xattrs-include='*' --acls -cf - "
        f"{' '.join(exclude_patterns)} {' '.join(shlex.quote(path) for path in include_dirs)} "
        "2>/dev/null | zstd -1 -T0"
    )
    return (
        f"bash -o pipefail -c {shlex.quote(pipeline)}" if strict_terminal else pipeline
    )


try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None  # type: ignore[assignment]
    BotoConfig = None  # type: ignore[misc,assignment]
    ClientError = Exception  # type: ignore[misc,assignment]


class SnapshotService:
    """S3-backed snapshot management for agent environments.

    All methods are async-safe (blocking S3 calls run in thread pool).
    Gracefully degrades when S3 is not configured or unavailable.
    """

    def __init__(self) -> None:
        self._s3: Any = None
        self._db: Any = None
        self._bucket: str = ""
        self._available: bool = False

    @property
    def is_available(self) -> bool:
        """True if S3 is configured and reachable."""
        return self._available

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def connect(self, db: Any) -> None:
        """Initialize the S3 client and ensure the bucket exists.

        Args:
            db: PostgresDB instance for job context updates.
        """
        self._db = db

        endpoint = os.environ.get("S3_ENDPOINT", "")
        if not endpoint or not BOTO3_AVAILABLE:
            # Warning, not info: everything downstream of this degrades
            # silently. Name the casualties so an operator reading startup logs
            # can tell this apart from a healthy deployment.
            reason = (
                "boto3 not installed" if not BOTO3_AVAILABLE else "S3_ENDPOINT not set"
            )
            logger.warning(
                "Snapshot service disabled (%s) — workspace snapshots will not "
                "capture, the virtual workspace tier is unwired, and Canvas "
                "presentations will NOT survive their workspace. Set "
                "s3.endpoint, or leave garage.enabled unset to run the bundled "
                "object store.",
                reason,
            )
            return

        access_key = os.environ.get("SNAPSHOT_S3_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("SNAPSHOT_S3_SECRET_ACCESS_KEY", "")
        self._bucket = os.environ.get("S3_BUCKET", "srw-snapshots")
        region = os.environ.get("S3_REGION", "us-east-1")

        try:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=BotoConfig(
                    signature_version="s3v4",
                    retries={"max_attempts": 3, "mode": "standard"},
                ),
            )

            # Ensure bucket exists (auto-create for dev)
            await _joined_blocking_call(self._ensure_bucket)
            self._available = True
            logger.info(
                "Snapshot service ready: endpoint=%s bucket=%s",
                endpoint,
                self._bucket,
            )
        except Exception as e:
            logger.warning("Snapshot service: S3 connection failed — disabled: %s", e)
            self._s3 = None
            self._available = False

    def _ensure_bucket(self) -> None:
        """Create the bucket if it doesn't exist (synchronous, run in thread)."""
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                self._s3.create_bucket(Bucket=self._bucket)
                logger.info("Created S3 bucket: %s", self._bucket)
            else:
                raise

    # =========================================================================
    # Upload / Capture
    # =========================================================================

    async def upload_snapshot(
        self,
        job_id: str,
        tar_path: str,
        manifest: dict[str, Any],
        phase_number: Optional[int] = None,
        entity_type: str = "jobs",
        terminal_generation: Optional[str] = None,
        publication_authority: Optional[SnapshotCaptureAuthority] = None,
    ) -> bool:
        """Upload a snapshot tarball + manifest to S3.

        The canonical write is staged (§C3, no-clobber): the tarball lands
        at a unique ``env.tar.zst.staging-<uuid>`` key first and is
        verified there (the §C2 size check); only a verified upload is
        promoted onto the canonical ``env.tar.zst`` and a new
        ``history/<ts>/`` generation (pruned to
        ``SNAPSHOT_KEEP_GENERATIONS``). A bad or truncated capture is
        therefore never able to overwrite the last known-good canonical
        archive in place — see
        knowledge-base/knowledge/features/workspace_durability_tiering.md §C3.

        Args:
            job_id: Job or thread UUID.
            tar_path: Local path to the env.tar.zst file.
            manifest: Manifest dict (will be serialized to JSON).
            phase_number: If set, store under phases/phase_<n>/ as well.
            entity_type: S3 prefix namespace ("jobs" or "threads").

        Returns:
            True if upload succeeded.
        """
        if not self._available:
            return False
        if publication_authority is not None and not await publication_authority():
            return False

        prefix = f"{entity_type}/{job_id}"
        phase_prefix = (
            f"{prefix}/phases/phase_{phase_number}"
            if phase_number is not None
            else None
        )

        try:
            # Compute checksum
            sha256 = await _joined_blocking_call(self._compute_sha256, tar_path)
            manifest["checksum_sha256"] = sha256

            manifest_bytes = json.dumps(manifest, indent=2).encode()

            # Upload tarball + manifest to phase-specific prefix
            if phase_prefix:
                if (
                    publication_authority is not None
                    and not await publication_authority()
                ):
                    return False
                await _joined_blocking_call(
                    self._s3.upload_file,
                    tar_path,
                    self._bucket,
                    f"{phase_prefix}/env.tar.zst",
                )
                await _joined_blocking_call(
                    self._s3.put_object,
                    Bucket=self._bucket,
                    Key=f"{phase_prefix}/manifest.json",
                    Body=manifest_bytes,
                    ContentType="application/json",
                )

            # §C3 no-clobber: upload the tarball to a UNIQUE STAGING key
            # first, verify it there, and only promote a verified upload
            # onto canonical. Nothing below this point writes to
            # `{prefix}/env.tar.zst` until the staged bytes are already
            # known-good — a truncated/partial multipart upload can no
            # longer clobber the last good archive the way a direct
            # canonical write followed by a post-hoc check could.
            staging_uuid = uuid.uuid4().hex
            staging_key = f"{prefix}/env.tar.zst.staging-{staging_uuid}"
            canonical_key = f"{prefix}/env.tar.zst"

            try:
                # The staging upload itself is INSIDE this guarded block:
                # if it raises after having created a partial object
                # (e.g. an aborted multipart upload that still left
                # something HEAD-able), `finally` below still deletes
                # `staging_key` — no leaked scaffolding on that path
                # either.
                await _joined_blocking_call(
                    self._s3.upload_file,
                    tar_path,
                    self._bucket,
                    staging_key,
                )

                # Verify the STAGING object (§C2's check), before any
                # canonical write. "When present": some manifests omit
                # size_compressed_bytes, and there's nothing to compare
                # against in that case, so the check is skipped rather
                # than failing closed on absence.
                expected_size = manifest.get("size_compressed_bytes")
                if expected_size:
                    head = await _joined_blocking_call(
                        self._s3.head_object,
                        Bucket=self._bucket,
                        Key=staging_key,
                    )
                    actual_size = head["ContentLength"]
                    if actual_size != expected_size:
                        logger.error(
                            "Snapshot upload size mismatch for %s %s: s3=%s manifest=%s",
                            entity_type.rstrip("s"),
                            job_id,
                            actual_size,
                            expected_size,
                        )
                        await self._set_snapshot_context(
                            job_id,
                            {
                                "status": "capture_failed",
                                "error": (
                                    f"post-upload size mismatch (s3={actual_size} "
                                    f"manifest={expected_size})"
                                ),
                            },
                            entity_type=entity_type,
                        )
                        # Canonical is untouched — nothing above this
                        # point ever wrote to it. `finally` below deletes
                        # the bad staging object.
                        return False

                # The SSH read and staging upload are external awaits. Re-prove
                # the exact runtime immediately before the first durable
                # canonical/history publication.
                if (
                    publication_authority is not None
                    and not await publication_authority()
                ):
                    return False

                # Promote (size-safe copy). Use the MANAGED `s3.copy` —
                # never `copy_object`: that's a single-part CopyObject API
                # call capped at 5 GB, and snapshots can reach
                # SNAPSHOT_MAX_SIZE_GB (default 10 GB). `.copy()` is
                # backed by boto3's TransferManager, which switches to a
                # multipart copy automatically above its threshold. The
                # staged bytes are already verified by this point, so
                # every write below only ever replaces its destination
                # with an equally-good object — S3 PUT/COPY is atomic at
                # the destination key, so a failure here (raised
                # exception) leaves whatever was already there, never a
                # half-written object.
                #
                # Order matters: history FIRST, canonical LAST. Canonical
                # is the object every reader (restore, verify_snapshot)
                # trusts, so it must be the last thing touched — a
                # failure anywhere in the history write (steps 1-2) then
                # leaves canonical entirely untouched (previous good
                # intact), instead of a half-promoted canonical whose tar
                # was already replaced but whose manifest wasn't. The
                # canonical tar+manifest pair still can't be updated as a
                # single atomic unit (separate S3 objects — no
                # cross-object transactions), so a failure between steps
                # 3 and 4 remains possible; that residual one-op gap is
                # fail-safe (verify_snapshot's deep hash catches a
                # tar/manifest mismatch) and recoverable from the
                # history/<ts>/ generation just written.
                ts = (
                    _terminal_generation_name(terminal_generation)
                    if terminal_generation is not None
                    else self._history_generation_stamp(manifest, staging_uuid)
                )
                history_prefix = f"{prefix}/history/{ts}"
                copy_source = {"Bucket": self._bucket, "Key": staging_key}

                await _joined_blocking_call(
                    self._s3.copy,
                    copy_source,
                    self._bucket,
                    f"{history_prefix}/env.tar.zst",
                )
                await _joined_blocking_call(
                    self._s3.put_object,
                    Bucket=self._bucket,
                    Key=f"{history_prefix}/manifest.json",
                    Body=manifest_bytes,
                    ContentType="application/json",
                )
                await _joined_blocking_call(
                    self._s3.copy, copy_source, self._bucket, canonical_key
                )
                await _joined_blocking_call(
                    self._s3.put_object,
                    Bucket=self._bucket,
                    Key=f"{prefix}/manifest.json",
                    Body=manifest_bytes,
                    ContentType="application/json",
                )
            finally:
                # Staging is single-use scaffolding: gone whether promote
                # succeeded or failed. On the failure path this also
                # completes the no-clobber guarantee — no `.staging-`
                # object is left behind masquerading as a real generation.
                await self._delete_staging_best_effort(staging_key)

            # Prune history to the newest SNAPSHOT_KEEP_GENERATIONS.
            # Best-effort: a prune hiccup must never undo an
            # already-durable promote — the new snapshot (canonical + its
            # own history generation) is safe regardless of whether old
            # generations get swept this round or the next.
            if terminal_generation is None:
                try:
                    await self._prune_history(prefix, self._keep_generations())
                except Exception:
                    logger.exception(
                        "History prune failed for %s %s (canonical + new "
                        "generation are unaffected)",
                        entity_type.rstrip("s"),
                        job_id,
                    )

            # Never project success onto the owner after its exact runtime
            # authority changed during S3 settlement.
            if publication_authority is not None and not await publication_authority():
                return False

            # Update entity context
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "available",
                    "source_type": manifest.get("source_type", "vm"),
                    "created_at": manifest.get(
                        "created_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "size_compressed_bytes": manifest.get("size_compressed_bytes", 0),
                    "phase_number": phase_number,
                    "checksum_sha256": sha256,
                },
                entity_type=entity_type,
            )

            logger.info(
                "Snapshot uploaded: %s=%s phase=%s size=%s",
                entity_type.rstrip("s"),
                job_id,
                phase_number,
                manifest.get("size_compressed_bytes", "?"),
            )
            return True

        except Exception as e:
            logger.error(
                "Snapshot upload failed for %s %s: %s",
                entity_type.rstrip("s"),
                job_id,
                e,
            )
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "capture_failed",
                    "error": str(e),
                },
                entity_type=entity_type,
            )
            return False

    # =========================================================================
    # Content-addressed blobs (Phase 3, D7 — cited cloud-document snapshots)
    # =========================================================================

    def _object_exists(self, key: str) -> bool:
        """True if an object exists at ``key`` (synchronous, run in a thread)."""
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    async def save_blob(
        self,
        data: bytes,
        *,
        prefix: str = "citations",
        content_type: str = "application/octet-stream",
    ) -> Optional[str]:
        """Store raw bytes content-addressed under ``<prefix>/<sha[:2]>/<sha>``.

        Used to snapshot the original bytes of a cited cloud document so the
        citation has a durable "view the original" backup (the agent has no S3
        credentials, so it reaches this via an orchestrator endpoint). Returns
        the object key, or ``None`` when the store is unavailable or the write
        fails. Idempotent: identical bytes hash to the same key, and an existing
        object is not re-uploaded (a cheap HEAD precedes the PUT).
        """
        if not self._available or not data:
            return None
        sha = hashlib.sha256(data).hexdigest()
        key = f"{prefix}/{sha[:2]}/{sha}"
        try:
            if not await _joined_blocking_call(self._object_exists, key):
                await _joined_blocking_call(
                    self._s3.put_object,
                    Bucket=self._bucket,
                    Key=key,
                    Body=data,
                    ContentType=content_type or "application/octet-stream",
                )
            return key
        except Exception as e:
            logger.error("save_blob failed (key=%s): %s", key, e)
            return None

    async def put_blob(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Store raw bytes at an explicit ``key``.

        Unlike ``save_blob`` (content-addressed, dedup-by-hash), callers pick
        the key — used by the job log archive, where the key encodes
        pod + timestamp and is stamped onto job/thread rows for retrieval.
        """
        if not self._available or not data:
            return False
        try:
            await _joined_blocking_call(
                self._s3.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type or "application/octet-stream",
            )
            return True
        except Exception as e:
            logger.error("put_blob failed (key=%s): %s", key, e)
            return False

    async def get_blob(
        self, key: str, *, max_bytes: int = DEFAULT_BLOB_READ_MAX_BYTES
    ) -> Optional[bytes]:
        """Fetch the raw bytes for a blob ``key``; ``None`` if missing/unavailable."""
        if not self._available or not key:
            return None
        try:
            resp = await _joined_blocking_call(
                self._s3.get_object, Bucket=self._bucket, Key=key
            )
            body = resp["Body"]

            def _read() -> bytes:
                result = bytearray()
                try:
                    while True:
                        chunk = body.read(_BLOB_STREAM_CHUNK_BYTES)
                        if not chunk:
                            return bytes(result)
                        result.extend(chunk)
                        if len(result) > max_bytes:
                            raise ValueError("S3 object exceeds byte-read cap")
                finally:
                    close = getattr(body, "close", None)
                    if callable(close):
                        close()

            return await _joined_blocking_call(_read)
        except Exception as e:
            logger.debug("get_blob miss (key=%s): %s", key, e)
            return None

    async def upload_blob_file(self, key: str, local_path: str) -> bool:
        """Upload a local file to an arbitrary bucket key (staging tars).

        Unlike ``save_blob``, this targets an explicit ``key`` rather than a
        content-addressed one — callers (e.g. cloud_staging.stage) need a
        deterministic, overwritable location.
        """
        if not self._available:
            return False
        try:
            await _joined_blocking_call(
                self._s3.upload_file, local_path, self._bucket, key
            )
            return True
        except Exception as e:
            logger.error(f"S3 upload_blob_file failed for {key}: {e}")
            return False

    async def download_blob_file(
        self, key: str, local_path: str, *, max_bytes: int
    ) -> bool:
        """Stream one S3 object to disk with a hard cap and owned cleanup."""

        if not self._available or not key or max_bytes <= 0:
            return False

        def _download() -> bool:
            try:
                response = self._s3.get_object(Bucket=self._bucket, Key=key)
            except ClientError:
                return False
            body = response["Body"]
            total = 0
            try:
                with open(local_path, "wb") as output:
                    while True:
                        chunk = body.read(_BLOB_STREAM_CHUNK_BYTES)
                        if not chunk:
                            return True
                        total += len(chunk)
                        if total > max_bytes:
                            raise ValueError("S3 object exceeds download cap")
                        output.write(chunk)
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()

        try:
            return bool(await _joined_blocking_call(_download))
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                logger.warning("S3 download failed for %s: %s", key, exc)
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            return False

    async def delete_blob(self, key: str) -> bool:
        """Delete the object at an arbitrary bucket ``key``."""
        if not self._available:
            return False
        try:
            await _joined_blocking_call(
                self._s3.delete_object, Bucket=self._bucket, Key=key
            )
            return True
        except Exception as e:
            logger.error(f"S3 delete_blob failed for {key}: {e}")
            return False

    async def capture_vm_snapshot(
        self,
        job_id: str,
        ssh_host: str,
        ssh_port: int,
        phase_number: Optional[int] = None,
        source_type: str = "vm",
        agent_config: str = "worker_base",
        entity_type: str = "jobs",
        work_marker: Optional[int] = None,
        expected_host_key_fingerprint: Optional[str] = None,
        strict_terminal: bool = False,
        terminal_generation: Optional[str] = None,
        terminal_created_at: Optional[str] = None,
        expected_runtime_incarnation: Optional[str] = None,
        capture_authority: Optional[SnapshotCaptureAuthority] = None,
    ) -> bool:
        """Capture a VM environment snapshot via SSH tar and upload to S3.

        SSHs into the VM, tars key directories with zstd compression,
        streams to a local temp file, then uploads to S3.

        Args:
            job_id: Job or thread UUID.
            ssh_host: VM SSH host.
            ssh_port: VM SSH port.
            phase_number: Current phase number (for per-phase storage).
            source_type: "vm" or "pod".
            agent_config: Agent config name for manifest.
            entity_type: S3 prefix namespace ("jobs" or "threads").

        Returns:
            True if capture + upload succeeded.
        """
        if not self._available:
            return False
        if capture_authority is not None and not await capture_authority():
            return False

        if terminal_generation is not None:
            try:
                _terminal_generation_name(terminal_generation)
            except ValueError as exc:
                logger.error("Terminal snapshot refused: %s", exc)
                return False
            if (
                not strict_terminal
                or source_type != "pod"
                or not isinstance(terminal_created_at, str)
                or not terminal_created_at
                or not isinstance(expected_runtime_incarnation, str)
                or not expected_runtime_incarnation
            ):
                logger.error(
                    "Command-keyed terminal snapshot refused without strict "
                    "runtime identity for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False
            reconciled, _ = await self.reconcile_terminal_snapshot_generation(
                job_id,
                terminal_generation=terminal_generation,
                entity_type=entity_type,
                expected_runtime_incarnation=expected_runtime_incarnation,
                expected_host_key_fingerprint=expected_host_key_fingerprint,
            )
            if reconciled:
                return True

        if strict_terminal and not _valid_ssh_sha256_fingerprint(
            expected_host_key_fingerprint
        ):
            logger.error(
                "Strict terminal snapshot refused without an exact SSH host key "
                "fingerprint for %s %s",
                entity_type.rstrip("s"),
                job_id,
            )
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "capture_failed",
                    "error": "strict snapshot host identity is unavailable",
                },
                entity_type=entity_type,
            )
            return False

        from orchestrator.services.ssh_helpers import orchestrator_can_reach

        if not orchestrator_can_reach(ssh_host):
            # Tailnet target (VM workspace) — SSH from the orchestrator would
            # black-hole. Skip visibly instead of hanging on a doomed connect;
            # snapshots are not supported on the VM backend (see knowledge-base/knowledge/issues/
            # vm_ssh_readiness_probe_unroutable_from_orchestrator.md).
            logger.info(
                "Skipping snapshot capture for %s %s (%s:%d): orchestrator "
                "has no route to tailnet targets",
                entity_type,
                job_id,
                ssh_host,
                ssh_port,
            )
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "capture_skipped",
                    "error": "unroutable tailnet target from orchestrator",
                },
                entity_type=entity_type,
            )
            return False

        import tempfile

        # Remembered before "capturing" overwrites it: a failed re-capture must
        # hand an existing snapshot back rather than bury it under an error.
        had_available_snapshot = await self._snapshot_is_available(job_id, entity_type)
        if capture_authority is not None and not await capture_authority():
            return False
        await self._set_snapshot_context(
            job_id, {"status": "capturing"}, entity_type=entity_type
        )

        tar_path = None
        known_hosts_path = None
        process: asyncio.subprocess.Process | None = None
        scan_process: asyncio.subprocess.Process | None = None
        verify_process: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task[bytes] | None = None
        verify_stderr_task: asyncio.Task[bytes] | None = None
        try:
            # Create temp file for the tarball
            with tempfile.NamedTemporaryFile(
                suffix=".tar.zst", delete=False, prefix=f"snapshot_{job_id[:8]}_"
            ) as tmp:
                tar_path = tmp.name

            # Directories to capture (exclude patterns handled by tar)
            include_dirs = [
                "/home/agent-host/",
                "/usr/local/",
            ]
            if strict_terminal and source_type == "pod":
                # Workspace images provide /usr/local as immutable root-owned
                # image content, and the stateless restore principal cannot
                # overwrite it. The only mutable authority required for shell,
                # files, tasks, Git/undo, and caches is agent-host's home.
                include_dirs = ["/home/agent-host/"]
            exclude_patterns = [
                # System/build caches
                "--exclude=/var/cache/*",
                "--exclude=/tmp/*",
                "--exclude=*.pyc",
                "--exclude=__pycache__",
                "--exclude=node_modules/.cache",
                "--exclude=.git/objects",
                # An ext4-formatted PVC mounts a root-owned 0700 ``lost+found``
                # at its volume root — which, for a session/job workspace, IS
                # ``/home/agent-host``. The capture tar runs as the unprivileged
                # ``agent-host`` SSH user and cannot open that directory, so
                # WITHOUT this exclude the whole ``tar`` exits rc>=2 and the C1b
                # accept gate (correctly) rejects the archive — breaking EVERY
                # PVC-backed capture, so idle-suspend can never complete and
                # reclaim-on-idle can never fire. ``lost+found`` is an fsck
                # artifact, never workspace data. Confirmed on the dev cluster:
                # ``tar: /home/agent-host/lost+found: Cannot open: Permission
                # denied`` was the sole cause of a real capture rc=2. See
                # knowledge-base/knowledge/features/workspace_durability_tiering.md §C1.
                "--exclude=*/lost+found",
                # Workspace content re-cloned/regenerated on restore
                "--exclude=*/repos/*",
                "--exclude=*/node_modules/*",
            ]
            if strict_terminal:
                # This is the sole durable copy of an emptyDir workspace.
                # Retain Git objects and repository state; the cache and
                # lost+found exclusions remain safe.
                exclude_patterns = [
                    pattern
                    for pattern in exclude_patterns
                    if pattern not in {"--exclude=.git/objects", "--exclude=*/repos/*"}
                ]
            # Build SSH tar command. --xattrs/--acls so fuse-overlayfs opaque-dir
            # xattrs + whiteouts survive capture/restore (protected cloud mode,
            # design §11.3). The capture roots already EXCLUDE the merged overlay
            # mount (it lives at /cloud/merged, outside /home/agent-host) and
            # INCLUDE the upperdir at /home/agent-host/.overlay/upper.
            pipeline = (
                'tar --xattrs --xattrs-include="*" --acls -cf - '
                f"{' '.join(exclude_patterns)} {' '.join(include_dirs)} 2>/dev/null "
                "| zstd -1 -T0"
            )
            # A shell pipeline's exit code is only the LAST stage's (zstd) —
            # a failing/truncated tar upstream would be masked and a partial
            # archive accepted as good. bash -c makes PIPESTATUS available
            # (pipefail alone can't distinguish tar rc==1 from rc>=2) so both
            # stage codes collapse to one honest code: 0 = clean, 1 = tar
            # warned ("file changed as we read it" — routine on a live
            # workspace, NOT a failure), 2 = fatal (truncated tar or any
            # zstd failure). The -c body is single-quoted, so
            # --xattrs-include uses \"*\" (double quotes still block glob
            # expansion — tar still receives the literal '*'). The exclude/
            # include lists must never contain a single quote — the
            # command-shape test asserts this by counting quotes in the
            # final command.
            # PIPESTATUS must be snapshotted into an array in ONE command
            # before anything reads it: a bare assignment (e.g.
            # `__t=${PIPESTATUS[0]}`) is itself a simple command and
            # immediately resets PIPESTATUS to its own exit status, so a
            # second assignment reading PIPESTATUS[1] would see it already
            # clobbered (silently re-masking every zstd failure). Capturing
            # `"${PIPESTATUS[@]}"` into `__ps` first avoids that.
            if strict_terminal:
                tar_cmd = _snapshot_tar_pipeline(include_dirs, strict_terminal=True)
            else:
                tar_cmd = (
                    "bash -c '" + pipeline + "; "
                    '__ps=("${PIPESTATUS[@]}"); __t=${__ps[0]}; __z=${__ps[1]}; '
                    'if [ "$__z" -ne 0 ] || [ "$__t" -ge 2 ]; then exit 2; '
                    'elif [ "$__t" -eq 1 ]; then exit 1; else exit 0; fi\''
                )
            key_path = resolve_ssh_key_path()
            if not key_path:
                logger.warning(
                    "No SSH key available for snapshot capture (%s %s)",
                    entity_type.rstrip("s"),
                    job_id,
                )
            host_key_options = [
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]
            if expected_host_key_fingerprint is not None:
                scan_process = await create_owned_subprocess_exec(
                    "ssh-keyscan",
                    "-T",
                    "10",
                    "-p",
                    str(ssh_port),
                    ssh_host,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    scanned, _ = await communicate_bounded(
                        scan_process,
                        timeout=15,
                        stdout_limit=1024 * 1024,
                        stderr_limit=64 * 1024,
                    )
                except SubprocessOutputLimit as exc:
                    raise RuntimeError(
                        "workspace SSH host-key scan exceeded output limit"
                    ) from exc
                matching_lines: list[bytes] = []
                for line in scanned.splitlines():
                    fields = line.split()
                    if len(fields) < 3 or line.lstrip().startswith(b"#"):
                        continue
                    try:
                        key_bytes = base64.b64decode(fields[2], validate=True)
                    except (ValueError, TypeError):
                        continue
                    observed = "SHA256:" + base64.b64encode(
                        hashlib.sha256(key_bytes).digest()
                    ).decode("ascii").rstrip("=")
                    if observed == expected_host_key_fingerprint:
                        matching_lines.append(line)
                if not matching_lines:
                    raise RuntimeError(
                        "workspace SSH host identity changed before snapshot"
                    )
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, prefix="snapshot_known_hosts_"
                ) as known_hosts:
                    known_hosts.write(b"\n".join(matching_lines) + b"\n")
                    known_hosts_path = known_hosts.name
                host_key_options = [
                    "-o",
                    "StrictHostKeyChecking=yes",
                    "-o",
                    f"UserKnownHostsFile={known_hosts_path}",
                ]
            ssh_cmd = [
                "ssh",
                *(["-i", key_path] if key_path else []),
                *host_key_options,
                "-o",
                "ConnectTimeout=10",
                "-p",
                str(ssh_port),
                f"agent-host@{ssh_host}",
                tar_cmd,
            ]

            # Run SSH tar → local file
            max_size = _snapshot_archive_max_bytes()
            if capture_authority is not None and not await capture_authority():
                return False
            process = await create_owned_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(_read_stream_tail(process.stderr))
            timeout_env = (
                "STATELESS_TERMINAL_SNAPSHOT_TIMEOUT_S"
                if strict_terminal
                else "SNAPSHOT_CAPTURE_TIMEOUT_S"
            )
            try:
                capture_timeout_s = max(
                    0.01,
                    float(os.environ.get(timeout_env, "300")),
                )
            except (TypeError, ValueError):
                capture_timeout_s = 300.0
            capture_deadline = asyncio.get_running_loop().time() + capture_timeout_s

            total_bytes = 0
            with open(tar_path, "wb") as f:
                while True:
                    read = process.stdout.read(1024 * 1024)  # 1 MB chunks
                    remaining = capture_deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    chunk = await asyncio.wait_for(read, timeout=remaining)
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    if total_bytes > max_size:
                        logger.warning(
                            "Snapshot too large for %s %s (>%s GB), aborting",
                            entity_type.rstrip("s"),
                            job_id,
                            os.environ.get("SNAPSHOT_MAX_SIZE_GB", "10"),
                        )
                        await self._set_snapshot_context(
                            job_id,
                            {
                                "status": "capture_failed",
                                "error": "Snapshot exceeds size limit",
                            },
                            entity_type=entity_type,
                        )
                        return False
                    f.write(chunk)

            remaining = capture_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(process.wait(), timeout=remaining)
            remaining = capture_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            stderr_tail = await asyncio.wait_for(stderr_task, timeout=remaining)

            # Honest accept gate: tar rc==1 ("file changed as we read it") is
            # a routine warning on a live workspace, not a failure — only
            # rc>=2 (fatal tar error or any zstd failure, per the PIPESTATUS
            # discrimination above) or a totally empty stream mean the
            # archive is not trustworthy.
            if (
                (strict_terminal and process.returncode != 0)
                or (not strict_terminal and process.returncode >= 2)
                or total_bytes == 0
            ):
                stderr = stderr_tail.decode(errors="replace")
                logger.error(
                    "Snapshot capture failed for %s %s (rc=%d, bytes=%d): %s",
                    entity_type.rstrip("s"),
                    job_id,
                    process.returncode,
                    total_bytes,
                    stderr[:500],
                )
                await self._record_capture_failure(
                    job_id,
                    f"SSH tar/zstd failed (rc={process.returncode})",
                    entity_type=entity_type,
                    previously_available=had_available_snapshot,
                )
                return False
            if strict_terminal:
                if total_bytes == 0:
                    raise RuntimeError("terminal snapshot archive is empty")
                verify_process = await create_owned_subprocess_exec(
                    "bash",
                    "-o",
                    "pipefail",
                    "-c",
                    f"zstd -t -- {shlex.quote(tar_path)} >/dev/null && "
                    f"zstd -dc -- {shlex.quote(tar_path)} | tar -tf - >/dev/null",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                verify_stderr_task = asyncio.create_task(
                    _read_stream_tail(verify_process.stderr)
                )
                await asyncio.wait_for(verify_process.wait(), timeout=120)
                verify_stderr = await verify_stderr_task
                if verify_process.returncode != 0:
                    raise RuntimeError(
                        "terminal snapshot archive validation failed: "
                        + verify_stderr.decode(errors="replace")[:300]
                    )
            elif process.returncode == 1:
                logger.warning(
                    "Snapshot capture for %s %s: tar reported files changed "
                    "during read (rc=1) — archive is complete, accepting",
                    entity_type.rstrip("s"),
                    job_id,
                )

            # Collect package manifests via SSH
            env_info = (
                {}
                if strict_terminal
                else await self._collect_environment_info(
                    ssh_host,
                    ssh_port,
                    known_hosts_path=known_hosts_path,
                    capture_authority=capture_authority,
                )
            )

            # Build manifest
            manifest: dict[str, Any] = {
                "version": 1,
                "job_id": job_id,
                "source_type": source_type,
                "created_at": (
                    terminal_created_at
                    if terminal_generation is not None
                    else datetime.now(timezone.utc).isoformat()
                ),
                "agent_config": agent_config,
                "compression": "zstd",
                "size_compressed_bytes": total_bytes,
                "capture_method": "ssh_tar",
                "captured_paths": include_dirs,
                "environment": env_info,
                "restore": {
                    "min_cpu": 2,
                    "min_memory": "4Gi",
                    "disk_size": "20G",
                    "estimated_boot_seconds": 25,
                },
            }

            if phase_number is not None:
                manifest["phase_number"] = phase_number
            if strict_terminal:
                manifest["strict_terminal"] = True
                manifest["sha256_compressed"] = await _joined_blocking_call(
                    _sha256_file, tar_path
                )
            if terminal_generation is not None:
                manifest.update(
                    {
                        "terminal_generation": terminal_generation,
                        "runtime_incarnation": expected_runtime_incarnation,
                        "ssh_host_key_fingerprint": expected_host_key_fingerprint,
                    }
                )

            # Upload to S3
            uploaded = await self.upload_snapshot(
                job_id=job_id,
                tar_path=tar_path,
                manifest=manifest,
                phase_number=phase_number,
                entity_type=entity_type,
                terminal_generation=terminal_generation,
                publication_authority=capture_authority,
            )
            # Record the work-marker (turn count at capture) into the workspace
            # context so the lifecycle reaper's is_dirty can tell whether new
            # work has happened since this snapshot. Written under
            # workspace_container — the same key is_dirty reads.
            if uploaded and work_marker is not None and self._db is not None:
                marker = {"last_snapshot_turns": work_marker}
                try:
                    if entity_type == "threads":
                        await self._db.merge_thread_workspace_context(job_id, marker)
                    else:
                        await self._db.merge_workspace_container_context(job_id, marker)
                except Exception:
                    logger.exception("Failed to stamp work-marker for %s", job_id)
            return uploaded

        except Exception as e:
            logger.error(
                "Snapshot capture failed for %s %s: %s",
                entity_type.rstrip("s"),
                job_id,
                e,
                exc_info=True,
            )
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "capture_failed",
                    "error": str(e),
                },
                entity_type=entity_type,
            )
            return False
        finally:

            async def _cleanup_processes() -> None:
                # Readers stay active until their producers are terminal so a
                # full pipe cannot deadlock cleanup. Reap every started child
                # before the lifecycle owner can release its authority lock.
                for child in (scan_process, process, verify_process):
                    if child is not None and child.returncode is None:
                        await stop_and_reap(child)
                tasks = tuple(
                    task
                    for task in (stderr_task, verify_stderr_task)
                    if task is not None and not task.done()
                )
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            await joined_async_call(_cleanup_processes())
            # Clean up temp file
            if tar_path:
                try:
                    os.unlink(tar_path)
                except OSError:
                    pass
            if known_hosts_path:
                try:
                    os.unlink(known_hosts_path)
                except OSError:
                    pass

    async def _collect_environment_info(
        self,
        ssh_host: str,
        ssh_port: int,
        *,
        known_hosts_path: str | None = None,
        capture_authority: Optional[SnapshotCaptureAuthority] = None,
    ) -> dict[str, Any]:
        """Collect package manifests from the VM via SSH.

        Returns a dict suitable for the manifest's ``environment`` key.
        Non-fatal: returns partial info on failure.
        """
        info: dict[str, Any] = {}

        commands = {
            "pip_freeze": "pip freeze 2>/dev/null",
            "node_version": "node --version 2>/dev/null",
            "python_version": "python3 --version 2>/dev/null | awk '{print $2}'",
        }

        key_path = resolve_ssh_key_path()
        host_key_options = (
            [
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                f"UserKnownHostsFile={known_hosts_path}",
            ]
            if known_hosts_path
            else [
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]
        )
        for key, cmd in commands.items():
            proc: asyncio.subprocess.Process | None = None
            try:
                # The package probes are separate SSH connections after the
                # archive stream. Re-prove the same durable VM claim before
                # each one; a post-publication check alone would still permit
                # reading a same-address successor into the local manifest.
                if capture_authority is not None and not await capture_authority():
                    return info
                proc = await create_owned_subprocess_exec(
                    "ssh",
                    *(["-i", key_path] if key_path else []),
                    *host_key_options,
                    "-o",
                    "ConnectTimeout=5",
                    "-p",
                    str(ssh_port),
                    f"agent-host@{ssh_host}",
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await communicate_bounded(
                    proc,
                    timeout=15,
                    stdout_limit=8 * 1024 * 1024,
                    stderr_limit=64 * 1024,
                )
                output = stdout.decode(errors="replace").strip()
                if output:
                    if key == "pip_freeze":
                        info[key] = output.splitlines()
                    else:
                        info[key] = output
            except Exception:
                pass
            finally:
                if proc is not None and proc.returncode is None:
                    await joined_async_call(stop_and_reap(proc))

        return info

    # =========================================================================
    # Download / Retrieve
    # =========================================================================

    async def get_manifest(
        self,
        job_id: str,
        phase_number: Optional[int] = None,
        entity_type: str = "jobs",
    ) -> Optional[dict[str, Any]]:
        """Retrieve the manifest for a snapshot.

        Args:
            job_id: Job or thread UUID.
            phase_number: If set, get the phase-specific manifest.
            entity_type: S3 prefix namespace ("jobs" or "threads").

        Returns:
            Parsed manifest dict, or None if not found.
        """
        if not self._available:
            return None

        if phase_number is not None:
            key = f"{entity_type}/{job_id}/phases/phase_{phase_number}/manifest.json"
        else:
            key = f"{entity_type}/{job_id}/manifest.json"

        try:
            body = await self.get_blob(key, max_bytes=MAX_SNAPSHOT_MANIFEST_BYTES)
            if body is None:
                return None
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else None
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                return None
            logger.error("Failed to get manifest for job %s: %s", job_id, e)
            return None
        except (TypeError, ValueError):
            logger.warning("Snapshot manifest is malformed for %s", job_id)
            return None

    async def reconcile_terminal_snapshot_generation(
        self,
        job_id: str,
        *,
        terminal_generation: str,
        entity_type: str = "jobs",
        expected_runtime_incarnation: str,
        expected_host_key_fingerprint: object,
    ) -> tuple[bool, str]:
        """Prove one command-keyed strict snapshot and repair its canonical alias.

        The history pair is the durable idempotency record.  Both objects must
        exist, the manifest must name the exact completion command/runtime/SSH
        key, and the archived bytes must match its digest.  A one-object crash
        residue is reported as ``partial`` so the finalizer retries capture; a
        complete pair is safely copied over the ordinary canonical read keys.
        """

        if not self._available:
            return False, "unavailable"
        try:
            generation_name = _terminal_generation_name(terminal_generation)
        except ValueError:
            return False, "invalid generation"
        if (
            not isinstance(expected_runtime_incarnation, str)
            or not expected_runtime_incarnation
            or not _valid_ssh_sha256_fingerprint(expected_host_key_fingerprint)
        ):
            return False, "invalid runtime identity"

        prefix = f"{entity_type}/{job_id}"
        history_prefix = f"{prefix}/history/{generation_name}"
        archive_key = f"{history_prefix}/env.tar.zst"
        manifest_key = f"{history_prefix}/manifest.json"

        archive_head: dict[str, Any] | None = None
        manifest_bytes: bytes | None = None
        try:
            archive_head = await _joined_blocking_call(
                self._s3.head_object, Bucket=self._bucket, Key=archive_key
            )
        except ClientError as exc:
            if not _snapshot_object_missing(exc):
                return False, f"probe error: {exc}"
        except Exception as exc:
            return False, f"probe error: {exc}"
        try:
            manifest_bytes = await self.get_blob(
                manifest_key, max_bytes=MAX_SNAPSHOT_MANIFEST_BYTES
            )
        except ClientError as exc:
            if not _snapshot_object_missing(exc):
                return False, f"probe error: {exc}"
        except Exception as exc:
            return False, f"probe error: {exc}"

        if archive_head is None and manifest_bytes is None:
            return False, "missing"
        if archive_head is None or manifest_bytes is None:
            return False, "partial"
        try:
            manifest = json.loads(manifest_bytes)
        except (TypeError, ValueError):
            return False, "partial: malformed manifest"
        if not isinstance(manifest, dict):
            return False, "partial: malformed manifest"

        digest = manifest.get("sha256_compressed")
        checksum = manifest.get("checksum_sha256")
        size = manifest.get("size_compressed_bytes")
        if (
            manifest.get("strict_terminal") is not True
            or manifest.get("terminal_generation") != terminal_generation
            or manifest.get("runtime_incarnation") != expected_runtime_incarnation
            or manifest.get("ssh_host_key_fingerprint") != expected_host_key_fingerprint
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or checksum != digest
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
            or size > _snapshot_archive_max_bytes()
            or archive_head.get("ContentLength") != size
        ):
            return False, "partial: identity or integrity metadata mismatch"
        try:
            observed_digest = await _joined_blocking_call(
                self._streaming_sha256_from_s3, archive_key
            )
        except Exception as exc:
            return False, f"probe error: {exc}"
        if observed_digest != digest:
            return False, "partial: sha256 mismatch"

        # History is authoritative.  Repair a crash between history promotion
        # and the two ordinary canonical writes before teardown is authorized.
        try:
            await _joined_blocking_call(
                self._s3.copy,
                {"Bucket": self._bucket, "Key": archive_key},
                self._bucket,
                f"{prefix}/env.tar.zst",
            )
            await _joined_blocking_call(
                self._s3.put_object,
                Bucket=self._bucket,
                Key=f"{prefix}/manifest.json",
                Body=manifest_bytes,
                ContentType="application/json",
            )
        except Exception as exc:
            return False, f"canonical repair error: {exc}"
        await self._set_snapshot_context(
            job_id,
            {
                "status": "available",
                "source_type": "pod",
                "created_at": manifest.get("created_at"),
                "size_compressed_bytes": size,
                "phase_number": None,
                "checksum_sha256": digest,
            },
            entity_type=entity_type,
        )
        return True, "complete"

    async def download_snapshot(
        self,
        job_id: str,
        dest_path: str,
        phase_number: Optional[int] = None,
        entity_type: str = "jobs",
        require_strict_terminal: bool = False,
    ) -> bool:
        """Download a snapshot tarball from S3.

        Args:
            job_id: Job or thread UUID.
            dest_path: Local path to write the tarball.
            phase_number: If set, download the phase-specific snapshot.
            entity_type: S3 prefix namespace ("jobs" or "threads").

        Returns:
            True if download succeeded.
        """
        if not self._available:
            return False

        if phase_number is not None:
            key = f"{entity_type}/{job_id}/phases/phase_{phase_number}/env.tar.zst"
        else:
            key = f"{entity_type}/{job_id}/env.tar.zst"

        succeeded = False
        try:
            manifest = await self.get_manifest(
                job_id,
                phase_number=phase_number,
                entity_type=entity_type,
            )
            archive_cap = _snapshot_archive_max_bytes()
            expected_size = (
                manifest.get("size_compressed_bytes")
                if isinstance(manifest, dict)
                else None
            )
            if expected_size is not None and (
                isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
                or not 0 < expected_size <= archive_cap
            ):
                logger.error(
                    "Snapshot size metadata is invalid for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False
            if not await self.download_blob_file(key, dest_path, max_bytes=archive_cap):
                return False
            actual_size = await _joined_blocking_call(os.path.getsize, dest_path)
            if expected_size is not None and actual_size != expected_size:
                logger.error(
                    "Snapshot size mismatch for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False
            manifest_is_strict = bool(
                isinstance(manifest, dict) and manifest.get("strict_terminal") is True
            )
            if require_strict_terminal and not manifest_is_strict:
                logger.error(
                    "Strict terminal snapshot manifest missing for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False
            expected_digest = (
                manifest.get("sha256_compressed") if manifest_is_strict else None
            )
            if require_strict_terminal and (
                not isinstance(expected_digest, str)
                or len(expected_digest) != 64
                or any(c not in "0123456789abcdef" for c in expected_digest)
            ):
                logger.error(
                    "Strict terminal snapshot checksum is missing or malformed for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False
            if expected_digest is not None:
                if (
                    not isinstance(expected_digest, str)
                    or len(expected_digest) != 64
                    or await _joined_blocking_call(_sha256_file, dest_path)
                    != expected_digest
                ):
                    logger.error(
                        "Strict snapshot checksum mismatch for %s %s",
                        entity_type.rstrip("s"),
                        job_id,
                    )
                    return False
            succeeded = True
            return True
        except ClientError as e:
            logger.error("Failed to download snapshot for job %s: %s", job_id, e)
            return False
        finally:
            if not succeeded:
                try:
                    os.unlink(dest_path)
                except FileNotFoundError:
                    pass

    async def list_phase_snapshots(self, job_id: str) -> list[dict[str, Any]]:
        """List all phase snapshots for a job.

        Returns:
            List of manifest dicts, sorted by phase number.
        """
        if not self._available:
            return []

        prefix = f"jobs/{job_id}/phases/"
        manifests = []

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)

            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith("manifest.json"):
                        try:
                            manifest = await self.get_manifest(
                                job_id,
                                phase_number=int(
                                    obj["Key"].split("/phase_")[1].split("/")[0]
                                ),
                            )
                            if manifest:
                                manifests.append(manifest)
                        except (ValueError, IndexError):
                            pass

        except ClientError as e:
            logger.error("Failed to list phase snapshots for job %s: %s", job_id, e)

        return sorted(manifests, key=lambda m: m.get("phase_number", 0))

    # =========================================================================
    # Verify (C2 — reclaim gate)
    # =========================================================================

    async def verify_snapshot(
        self,
        job_id: str,
        *,
        entity_type: str = "jobs",
        deep: Optional[bool] = None,
    ) -> tuple[bool, str]:
        """Confirm a snapshot is durably and correctly stored in S3.

        The single gate a later task (C4) calls before the irreversible
        ``delete_workspace_pvc`` — FAIL-SAFE by design: anything
        unverifiable (no manifest, missing object, size mismatch, deep
        verify with no stored checksum, hash mismatch) returns
        ``(False, reason)``; only an all-good result returns
        ``(True, "ok")``. A buggy ``True`` here would let a caller destroy
        the only remaining copy of unrecoverable data.

        Two checks:
          - size (always, cheap): ``HeadObject`` content-length vs.
            ``manifest.size_compressed_bytes`` (skipped if the manifest
            doesn't carry a size).
          - hash (when ``deep``, default from ``SNAPSHOT_VERIFY_DEEP``): a
            streamed re-hash of the S3 object compared against
            ``manifest.checksum_sha256`` — this is the only integrity
            oracle. Never compare the object's ``ETag``: these archives are
            multipart-uploaded, so the ETag is
            ``md5(concat(part-md5s))-N``, not the object's hash.

        Args:
            job_id: Job or thread UUID.
            entity_type: S3 prefix namespace ("jobs" or "threads").
            deep: Force the hash re-check on/off. ``None`` (default) reads
                ``SNAPSHOT_VERIFY_DEEP`` (default ``"true"``).

        Returns:
            ``(ok, reason)`` — ``reason`` is always a short human-readable
            string, e.g. ``"ok"``, ``"no manifest"``, ``"object missing"``.

        Never raises: this is the gate a caller uses to authorize an
        irreversible PVC delete, so it must own every failure itself
        (manifest lookup errors, and any S3/network error during the deep
        re-hash — e.g. a TOCTOU delete between the HEAD and the GET, or a
        transient 5xx/timeout/reset) rather than let it escape to a
        possibly-unguarded caller. Any such exception is treated as
        unverifiable, same as every other failure branch here.
        """
        if not self._available:
            return False, "s3 unavailable"

        try:
            manifest = await self.get_manifest(job_id, entity_type=entity_type)
        except Exception as e:
            return False, f"verify error: {e}"
        if not manifest:
            return False, "no manifest"

        want_sha = manifest.get("checksum_sha256")
        want_size = manifest.get("size_compressed_bytes")
        key = f"{entity_type}/{job_id}/env.tar.zst"

        try:
            head = await _joined_blocking_call(
                self._s3.head_object, Bucket=self._bucket, Key=key
            )
        except ClientError:
            return False, "object missing"
        except Exception as e:
            # ClientError only covers service-error responses (NoSuchKey/
            # 404/5xx) — BotoCoreError (EndpointConnectionError,
            # ConnectTimeoutError, ReadTimeoutError, ConnectionClosedError)
            # and a bubbled ConnectionResetError are plain Exceptions, not
            # ClientErrors, and must be owned here too. Order matters:
            # ClientError above keeps NoSuchKey's precise "object missing"
            # reason; this is the catch-all for everything else.
            return False, f"verify error: {e}"

        if want_size and head["ContentLength"] != want_size:
            return False, (
                f"size mismatch (s3={head['ContentLength']} manifest={want_size})"
            )

        if deep is None:
            deep = os.environ.get("SNAPSHOT_VERIFY_DEEP", "true").strip().lower() in (
                "true",
                "1",
                "yes",
                "on",
            )

        if deep:
            if not want_sha:
                # FAIL-SAFE: no stored checksum to compare against means
                # this snapshot can't be proven intact — never treat
                # "nothing to check" as "checked and fine".
                return False, "no checksum in manifest (unverifiable)"
            try:
                got = await _joined_blocking_call(self._streaming_sha256_from_s3, key)
            except Exception as e:
                # Covers both a TOCTOU ClientError (object vanished between
                # the HEAD above and this GET) and any other transient S3
                # failure — either way, unverifiable, never a raise.
                return False, f"verify error: {e}"
            if got != want_sha:
                return False, "sha256 mismatch"

        return True, "ok"

    # =========================================================================
    # Delete / GC
    # =========================================================================

    async def delete_snapshot(self, job_id: str, entity_type: str = "jobs") -> bool:
        """Delete all snapshots for an entity from S3.

        Returns:
            True if deletion succeeded (or nothing to delete).
        """
        if not self._available:
            return False

        prefix = f"{entity_type}/{job_id}/"

        try:
            # List all objects under the job prefix
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)

            objects_to_delete = []
            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                return True

            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                deleted = await _joined_blocking_call(
                    self._s3.delete_objects,
                    Bucket=self._bucket,
                    Delete={"Objects": batch},
                )
                if not isinstance(deleted, dict) or deleted.get("Errors"):
                    logger.error(
                        "Snapshot prefix delete reported per-object failures for %s %s",
                        entity_type.rstrip("s"),
                        job_id,
                    )
                    return False

            # S3-compatible stores provide strongly consistent LIST after
            # DELETE. Prove the terminal prefix is empty before allowing its
            # ownership row/tombstone to disappear.
            verify_pages = self._s3.get_paginator("list_objects_v2").paginate(
                Bucket=self._bucket,
                Prefix=prefix,
            )
            remaining = [
                obj
                for page in await _joined_blocking_call(lambda: list(verify_pages))
                for obj in page.get("Contents", [])
            ]
            if remaining:
                logger.error(
                    "Snapshot prefix remained non-empty after delete for %s %s",
                    entity_type.rstrip("s"),
                    job_id,
                )
                return False

            # Clear snapshot context
            await self._set_snapshot_context(
                job_id, {"status": "deleted"}, entity_type=entity_type
            )

            logger.info(
                "Deleted %d snapshot objects for %s %s",
                len(objects_to_delete),
                entity_type.rstrip("s"),
                job_id,
            )
            return True

        except ClientError as e:
            logger.error("Failed to delete snapshot for job %s: %s", job_id, e)
            return False

    async def get_snapshot_status(self, job_id: str) -> dict[str, Any]:
        """Get snapshot status for a job, combining DB context and S3.

        Returns:
            Dict with status, source_type, created_at, size, pinned, etc.
        """
        if not self._db:
            return {"status": "unavailable"}

        try:
            job = await self._db.get_job(job_id)
            if not job:
                return {"status": "unavailable"}

            ctx = job.get("context") or {}
            if isinstance(ctx, str):
                ctx = json.loads(ctx)

            snapshot_ctx = ctx.get("snapshot", {})
            status = snapshot_ctx.get("status", "none")

            if status in ("available", "capturing"):
                # Enrich with manifest data from S3 if available
                manifest = await self.get_manifest(job_id)
                if manifest:
                    return {
                        "status": status,
                        "source_type": manifest.get("source_type", "unknown"),
                        "created_at": manifest.get("created_at"),
                        "size_compressed_bytes": manifest.get("size_compressed_bytes"),
                        "pinned": snapshot_ctx.get("pinned", False),
                        "phase_number": snapshot_ctx.get("phase_number"),
                        "checksum_sha256": manifest.get("checksum_sha256"),
                        "environment_summary": {
                            "python_packages": len(
                                manifest.get("environment", {}).get("pip_freeze", [])
                            ),
                            "python_version": manifest.get("environment", {}).get(
                                "python_version"
                            ),
                            "node_version": manifest.get("environment", {}).get(
                                "node_version"
                            ),
                        },
                    }

            return {
                "status": status if status != "none" else "unavailable",
                "error": snapshot_ctx.get("error"),
            }

        except Exception as e:
            logger.error("Failed to get snapshot status for job %s: %s", job_id, e)
            return {"status": "unavailable", "error": str(e)}

    # =========================================================================
    # Pin
    # =========================================================================

    async def toggle_pin(self, job_id: str) -> bool:
        """Toggle pin state on a snapshot. Returns new pinned value."""
        if not self._db:
            return False

        job = await self._db.get_job(job_id)
        if not job:
            return False

        ctx = job.get("context") or {}
        if isinstance(ctx, str):
            ctx = json.loads(ctx)

        current = ctx.get("snapshot", {}).get("pinned", False)
        new_value = not current
        await self._set_snapshot_context(job_id, {"pinned": new_value})
        return new_value

    # =========================================================================
    # Garbage Collection
    # =========================================================================

    async def run_gc(self) -> dict[str, int]:
        """Run snapshot garbage collection.

        1. List all job snapshots in S3
        2. Cross-reference with job records in PostgreSQL
        3. Apply retention rules (age, pin status)
        4. Move expired snapshots to gc/pending_delete/
        5. Purge items in pending_delete/ older than 7 days

        Returns:
            Dict with counts: soft_deleted, purged, errors.
        """
        if not self._available or not self._db:
            return {"soft_deleted": 0, "purged": 0, "errors": 0}

        retention_days = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "90"))
        grace_days = 7
        now = datetime.now(timezone.utc)
        stats = {"soft_deleted": 0, "purged": 0, "errors": 0}

        try:
            # Phase 1: Soft-delete expired snapshots
            await self._gc_soft_delete(now, retention_days, stats)

            # Phase 2: Purge items past the grace period
            await self._gc_purge(now, grace_days, stats)

        except Exception:
            logger.exception("Error during snapshot GC")
            stats["errors"] += 1

        if stats["soft_deleted"] or stats["purged"]:
            logger.info(
                "Snapshot GC complete: soft_deleted=%d, purged=%d, errors=%d",
                stats["soft_deleted"],
                stats["purged"],
                stats["errors"],
            )

        return stats

    async def _gc_soft_delete(
        self, now: datetime, retention_days: int, stats: dict[str, int]
    ) -> None:
        """Move expired snapshots to gc/pending_delete/."""
        cutoff = now - timedelta(days=retention_days)

        # List all job snapshot manifests
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix="jobs/")

            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Only check top-level manifests (not phase sub-manifests)
                    if not key.endswith("/manifest.json") or "/phases/" in key:
                        continue

                    # Extract job_id from key: jobs/<uuid>/manifest.json
                    parts = key.split("/")
                    if len(parts) < 3:
                        continue
                    job_id = parts[1]

                    try:
                        # Check if pinned
                        job = await self._db.get_job(job_id)
                        if job:
                            ctx = job.get("context") or {}
                            if isinstance(ctx, str):
                                ctx = json.loads(ctx)
                            if ctx.get("snapshot", {}).get("pinned", False):
                                continue

                        # Check age from manifest
                        manifest = await self.get_manifest(job_id)
                        if not manifest:
                            continue

                        created_at = manifest.get("created_at", "")
                        if not created_at:
                            continue

                        created = datetime.fromisoformat(created_at)
                        if created > cutoff:
                            continue

                        # Soft-delete: move to gc/pending_delete/
                        await self._soft_delete_snapshot(job_id, now)
                        stats["soft_deleted"] += 1

                    except Exception as e:
                        logger.warning("GC error for job %s: %s", job_id, e)
                        stats["errors"] += 1

        except ClientError as e:
            logger.error("GC listing failed: %s", e)
            stats["errors"] += 1

    async def _gc_purge(
        self, now: datetime, grace_days: int, stats: dict[str, int]
    ) -> None:
        """Purge items in gc/pending_delete/ past the grace period."""
        cutoff = now - timedelta(days=grace_days)

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix="gc/pending_delete/")

            objects_to_purge = []
            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    if (
                        obj.get("LastModified")
                        and obj["LastModified"].replace(tzinfo=timezone.utc) < cutoff
                    ):
                        objects_to_purge.append({"Key": obj["Key"]})

            if not objects_to_purge:
                return

            for i in range(0, len(objects_to_purge), 1000):
                batch = objects_to_purge[i : i + 1000]
                await _joined_blocking_call(
                    self._s3.delete_objects,
                    Bucket=self._bucket,
                    Delete={"Objects": batch},
                )

            stats["purged"] += len(objects_to_purge)

        except ClientError as e:
            logger.error("GC purge failed: %s", e)
            stats["errors"] += 1

    async def _soft_delete_snapshot(self, job_id: str, now: datetime) -> None:
        """Move a job's snapshots from jobs/ to gc/pending_delete/."""
        src_prefix = f"jobs/{job_id}/"
        dst_prefix = f"gc/pending_delete/{job_id}/"

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=src_prefix)

            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    src_key = obj["Key"]
                    dst_key = src_key.replace(src_prefix, dst_prefix, 1)

                    # Copy to gc/
                    await _joined_blocking_call(
                        self._s3.copy_object,
                        Bucket=self._bucket,
                        CopySource={"Bucket": self._bucket, "Key": src_key},
                        Key=dst_key,
                    )

                    # Delete original
                    await _joined_blocking_call(
                        self._s3.delete_object,
                        Bucket=self._bucket,
                        Key=src_key,
                    )

            # Update job context
            await self._set_snapshot_context(
                job_id,
                {
                    "status": "gc_deleted",
                    "gc_deleted_at": now.isoformat(),
                },
            )

            logger.info("Soft-deleted snapshot for job %s", job_id)

        except ClientError as e:
            logger.error("Soft-delete failed for job %s: %s", job_id, e)
            raise

    # =========================================================================
    # Storage Stats
    # =========================================================================

    async def get_storage_stats(self) -> dict[str, Any]:
        """Get aggregate snapshot storage statistics.

        Returns:
            Dict with total_snapshots, total_size_bytes, gc_pending_count, etc.
        """
        if not self._available:
            return {"available": False}

        stats: dict[str, Any] = {
            "available": True,
            "total_snapshots": 0,
            "total_size_bytes": 0,
            "gc_pending_count": 0,
            "gc_pending_size_bytes": 0,
        }

        try:
            paginator = self._s3.get_paginator("list_objects_v2")

            # Count active snapshots. §C3: history/ generations are prior
            # rollback copies of the SAME snapshot, not additional ones —
            # excluded here so they don't inflate the count, but their
            # bytes still consume real storage (counted unconditionally
            # below).
            pages = paginator.paginate(Bucket=self._bucket, Prefix="jobs/")
            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    if (
                        obj["Key"].endswith("/manifest.json")
                        and "/phases/" not in obj["Key"]
                        and "/history/" not in obj["Key"]
                    ):
                        stats["total_snapshots"] += 1
                    stats["total_size_bytes"] += obj.get("Size", 0)

            # Count GC pending
            pages = paginator.paginate(Bucket=self._bucket, Prefix="gc/pending_delete/")
            for page in await _joined_blocking_call(lambda: list(pages)):
                for obj in page.get("Contents", []):
                    stats["gc_pending_count"] += 1
                    stats["gc_pending_size_bytes"] += obj.get("Size", 0)

        except ClientError as e:
            logger.error("Failed to get storage stats: %s", e)
            stats["error"] = str(e)

        return stats

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _snapshot_is_available(self, entity_id: str, entity_type: str) -> bool:
        """True when the entity already carries an ``available`` snapshot."""
        try:
            if entity_type == "threads":
                row = await self._db.get_thread(entity_id)
                container = (row or {}).get("metadata")
            else:
                row = await self._db.get_job(entity_id)
                container = (row or {}).get("context")
            if isinstance(container, str):
                container = json.loads(container)
            snapshot = (container or {}).get("snapshot") or {}
            return snapshot.get("status") == "available"
        except Exception:
            logger.debug(
                "Could not read snapshot context for %s %s",
                entity_type.rstrip("s"),
                entity_id,
                exc_info=True,
            )
            return False

    async def _record_capture_failure(
        self,
        entity_id: str,
        error: str,
        *,
        entity_type: str = "jobs",
        previously_available: bool = False,
    ) -> None:
        """Stamp a capture failure without downgrading a snapshot that exists.

        A retried terminal teardown re-captures against a VM that is already
        going away. The first attempt's archive is still in S3 and restorable,
        so a later failure must not flip it to ``capture_failed`` — that would
        hide a good snapshot behind an error while keeping its checksum.
        ``previously_available`` is the state observed before this capture
        wrote ``capturing`` over it; the manifest fields survive the merge, so
        restoring ``available`` hands the earlier snapshot back intact.
        """
        if previously_available:
            logger.warning(
                "Snapshot re-capture failed for %s %s but an earlier snapshot "
                "is available; keeping it: %s",
                entity_type.rstrip("s"),
                entity_id,
                error,
            )
            await self._set_snapshot_context(
                entity_id,
                {
                    "status": "available",
                    "last_capture_error": error,
                    "last_capture_failed_at": datetime.now(timezone.utc).isoformat(),
                },
                entity_type=entity_type,
            )
            return
        await self._set_snapshot_context(
            entity_id,
            {"status": "capture_failed", "error": error},
            entity_type=entity_type,
        )

    async def _set_snapshot_context(
        self, entity_id: str, updates: dict, entity_type: str = "jobs"
    ) -> None:
        """Atomically merge updates into the entity's snapshot context key."""
        if not self._db:
            return

        try:
            if entity_type == "threads":
                await self._db.merge_thread_snapshot_context(entity_id, updates)
            else:
                await self._db.merge_snapshot_context(entity_id, updates)
        except Exception:
            logger.exception(
                "Failed to update snapshot context for %s %s",
                entity_type.rstrip("s"),
                entity_id,
            )

    @staticmethod
    def _compute_sha256(file_path: str) -> str:
        """Compute SHA-256 hash of a file (synchronous, run in thread)."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _streaming_sha256_from_s3(self, key: str) -> str:
        """SHA-256 of an S3 object, streamed in 1 MB chunks (O(1) memory).

        Synchronous (run via the joined blocking-effect helper) — mirrors
        ``_compute_sha256`` but reads the S3 object body instead of a local
        file, so a possibly multi-GB object is never buffered in memory at
        once. Backs ``verify_snapshot``'s deep check; never use the
        object's ``ETag`` in its place — these archives are
        multipart-uploaded, so the ETag is ``md5(concat(part-md5s))-N``,
        not the object's hash.
        """
        h = hashlib.sha256()
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        body = resp["Body"]
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _keep_generations() -> int:
        """``SNAPSHOT_KEEP_GENERATIONS`` (default 3), read once per upload
        and clamped to at least 1 — 0 or negative would prune away the
        generation the same upload just promoted.
        """
        return max(1, int(os.environ.get("SNAPSHOT_KEEP_GENERATIONS", "3")))

    @staticmethod
    def _history_generation_stamp(manifest: dict[str, Any], unique_suffix: str) -> str:
        """Build a lexically-sortable, filesystem-safe ``history/`` generation id.

        Derived from the manifest's ``created_at`` (falling back to "now"
        when absent) with ``:``/``+`` sanitized out — S3 keys tolerate
        both characters, but ``:`` reads awkwardly in tooling and ``+``
        gets URL-decoded to a space by some clients/proxies. A short
        unique suffix (the staging upload's own uuid) is always appended
        so two generations can never collide even when ``created_at``
        repeats (clock resolution) or is absent from the manifest
        entirely.
        """
        created_at = (
            manifest.get("created_at") or datetime.now(timezone.utc).isoformat()
        )
        sanitized = str(created_at).replace(":", "-").replace("+", "_")
        return f"{sanitized}-{unique_suffix[:8]}"

    async def _delete_staging_best_effort(self, key: str) -> None:
        """Delete a staging object, swallowing failures.

        Called from a ``finally`` around the verify/promote block in
        ``upload_snapshot`` — a stray ``.staging-*`` object is inert
        clutter (no other code path ever reads it), so a delete failure
        here must never shadow the upload's real success/failure outcome.
        """
        try:
            await _joined_blocking_call(
                self._s3.delete_object, Bucket=self._bucket, Key=key
            )
        except Exception:
            logger.warning("Failed to delete staging object %s (non-fatal)", key)

    async def _prune_history(self, prefix: str, keep: int) -> None:
        """Delete all but the newest ``keep`` generations under ``{prefix}/history/``.

        Each generation is named ``<ts>`` (see
        ``_history_generation_stamp``) and holds exactly ``env.tar.zst`` +
        ``manifest.json``. Generation ids sort lexically oldest-first, so
        the newest ``keep`` are simply the tail of a plain sort. Called
        only after a successful promote; the caller treats this as
        best-effort — a prune failure must never undo an already-durable
        capture.
        """
        history_prefix = f"{prefix}/history/"
        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=history_prefix)

        generations: set[str] = set()
        for page in await _joined_blocking_call(lambda: list(pages)):
            for obj in page.get("Contents", []):
                rest = obj["Key"][len(history_prefix) :]
                generation = rest.split("/", 1)[0]
                if generation and not generation.startswith("completion-"):
                    generations.add(generation)

        if len(generations) <= keep:
            return

        stale = sorted(generations)[: len(generations) - keep]
        for generation in stale:
            for name in ("env.tar.zst", "manifest.json"):
                key = f"{history_prefix}{generation}/{name}"
                try:
                    await _joined_blocking_call(
                        self._s3.delete_object, Bucket=self._bucket, Key=key
                    )
                except Exception:
                    logger.warning("Failed to prune history object %s", key)


# Module-level singleton
snapshot_service = SnapshotService()


async def snapshot_gc_sweeper(
    shutdown_event: asyncio.Event, *, snapshots: SnapshotService
) -> None:
    """Background task that runs snapshot garbage collection daily.

    Applies retention policies, soft-deletes expired snapshots, and
    purges items past the 7-day grace period.
    """
    logger.info("Snapshot GC sweeper started")
    gc_interval = 24 * 3600  # 24 hours

    while not shutdown_event.is_set():
        try:
            if snapshots.is_available:
                stats = await snapshots.run_gc()
                if stats.get("soft_deleted") or stats.get("purged"):
                    logger.info("Snapshot GC: %s", stats)
        except Exception as e:
            logger.error("Error in snapshot GC sweeper: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=gc_interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Snapshot GC sweeper stopped")
