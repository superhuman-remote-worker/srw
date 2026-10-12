"""The job outbox — copy a job's deliverables into a shared cloud folder.

Mode B of the job cloud workflow, extracted from ``orchestrator.main``
(R1.B04 lane C), became the ``cloud_outbox`` connector's job delivery (main
cloud slice 6.2): :func:`deliver_job_outbox` is the copy itself, which the
completion effect ``job_outbox_delivery`` (S16b, :func:`run_job_outbox_effect`)
runs for a root job with its owner's outbox attached, and the "re-sync"
endpoint (:func:`export_job_to_shared_folder`) runs on demand for any job,
whether or not its project has a cloud folder. Properties that are
load-bearing:

* **A job copies only into a folder recorded as its own.** A new copy
  creates ``<slug>-<the job's full id>`` exclusively
  (``ensure_session_folder(exclusive=True)``) and records it before the
  first file lands (``job_outbox.state: copying``); a folder of that name
  that exists already is refused (``folder_failed``), never reused: it is
  not this job's, and its share may be someone else's. A job that already
  carries a handle (``exported_folder_handle``, or the record's) reuses it,
  so a naming-scheme change cannot strand an existing share and a copy
  interrupted mid-way resumes into its own folder.
* **The outbox is shared with the job's owner only.** Whoever re-syncs it,
  the share goes to the owner (``job_outbox.shared_with``): another member's
  re-sync can neither take the folder nor drop the owner's share, and a
  re-sync re-creates a share the owner removed (a cloud's refusal of a
  second share of a folder it recorded as shared stands for the share).
* **Copy failures are fail-soft only where the original made them so.** A
  declared-but-missing deliverable is logged and skipped; a missing file in
  the ``output/`` fallback fails the copy. Everything else — an unexpected
  error, a cloud error — fails it with the count that actually landed.
* **The job is stamped only after a successful copy**, so a retry is safe.
* **An unshared folder is still a success.** The files landed; ``shared:
  false`` says why the owner cannot see them yet (``unshared``: no cloud
  account yet, the share rate limit, a share error), and a re-sync shares
  it. The share never blocks: Nextcloud's share bucket is waited on for a
  bounded time only (``cloud.retry.bounded_wait``).
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    SessionFolderHandle,
)
from orchestrator.services.cloud.retry import BucketFull, bounded_wait

logger = logging.getLogger(__name__)

_EXPORT_SLUG_STRIP = re.compile(r"[^a-z0-9]+")
_EXPORT_SLUG_MAX = 40
#: The statuses whose output is stable enough to copy.
EXPORTABLE_STATUSES: tuple[str, ...] = ("completed", "pending_review", "reviewing")
#: Why a copy failed, as closed codes (``context.job_outbox.reason``).
FAILURE_REASONS: tuple[str, ...] = (
    "backend_unavailable",
    "forge_unavailable",
    "folder_failed",
    "file_missing",
    "copy_failed",
    "owner_unavailable",
)
#: Why a copied folder is not shared with the owner yet
#: (``context.job_outbox.unshared``).
UNSHARED_REASONS: tuple[str, ...] = ("no_account", "rate_limited", "share_failed")
#: How long the re-sync endpoint waits on Nextcloud's share bucket; the
#: completion effect does not wait at all.
RESYNC_SHARE_WAIT_SECONDS = 15.0


@dataclass(frozen=True)
class JobExportDependencies:
    """Collaborators for one export, resolved per invocation.

    ``store``, ``forge`` and ``cloud_router`` are rebound during ``lifespan``;
    ``resolve_job_repo`` reads the store, so the application rebuilds this
    dataclass per request rather than capturing it at import.
    """

    store: Any
    forge: Any
    cloud_router: Any
    resolve_job_repo: Callable[[str], Awaitable[tuple[str, str | None]]]


def _job_export_folder_name(job_id: str, description: str | None) -> str:
    """Human-readable folder name for a new job outbox.

    A cloud root full of bare ``job-<id>`` folders is unnavigable after a
    handful of exports, so lead with a slug of the job's prompt, then the
    job's **full** id (two jobs can share a description, and a short suffix
    collides: 32 random bits) — e.g.
    ``you-maintain-a-daily-digest-a6fa6f2a91014c1e9b9e0000000000c1``. The
    folder is created exclusively under that name, so it is the job's own.

    Deterministic (``description`` is fixed at job creation). Output is
    restricted to ``[a-z0-9-]``, which is a safe single path segment for
    every backend (both build ``sessions/<name>`` over WebDAV).
    """
    short = job_id.replace("-", "")
    # splitlines() on a blank string yields [], so index defensively.
    lines = (description or "").strip().splitlines()
    first_line = lines[0] if lines else ""
    # Fold accents so "Führe" slugs to "fuhre", not "f-hre".
    folded = (
        unicodedata.normalize("NFKD", first_line)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    slug = _EXPORT_SLUG_STRIP.sub("-", folded.lower()).strip("-")
    if len(slug) > _EXPORT_SLUG_MAX:
        slug = slug[:_EXPORT_SLUG_MAX].rsplit("-", 1)[0].strip("-")
    if not slug:
        # Descriptions are NOT NULL but can be punctuation-only in theory.
        return f"job-{short}"
    return f"{slug}-{short}"


def _common_dir_prefix(paths: list[str]) -> str:
    """Longest whole-segment directory prefix shared by every path.

    ``["output/digest.md"]`` -> ``"output"``;
    ``["repo/src/a.py", "repo/tests/b.py"]`` -> ``"repo"``;
    ``["spec.yaml", "repo/a.py"]`` -> ``""``.

    Segment-wise, so ``["out/a.md", "output/b.md"]`` shares nothing rather than
    the string prefix ``"out"``. The last component of each path is the
    filename and never counts.
    """
    if not paths:
        return ""
    common = paths[0].split("/")[:-1]
    for path in paths[1:]:
        segments = path.split("/")[:-1]
        keep = 0
        while (
            keep < len(common)
            and keep < len(segments)
            and common[keep] == segments[keep]
        ):
            keep += 1
        common = common[:keep]
        if not common:
            break
    return "/".join(common)


class JobOutboxError(Exception):
    """A copy that did not land. ``reason`` is one of
    :data:`FAILURE_REASONS`; ``status_code`` and ``detail`` are what the
    re-sync endpoint answers with. ``handle`` is the job's own folder when
    it was created before the copy failed: the record keeps it, so the next
    copy resumes into it."""

    def __init__(
        self, reason: str, status_code: int, detail: str, *, handle: str | None = None
    ) -> None:
        super().__init__(detail)
        self.reason = reason
        self.status_code = status_code
        self.detail = detail
        self.handle = handle


def _job_context(job: dict[str, Any]) -> dict[str, Any]:
    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            return {}
    return context if isinstance(context, dict) else {}


def _recorded_folder(job: dict[str, Any]) -> str | None:
    """The folder recorded as this job's own: its stamp, else the handle the
    latest copy's record kept (a copy that created it and then failed or
    was interrupted)."""
    if job.get("exported_folder_handle"):
        return str(job["exported_folder_handle"])
    record = _job_context(job).get("job_outbox")
    handle = record.get("handle") if isinstance(record, dict) else None
    return str(handle) if handle else None


def _recorded_share(job: dict[str, Any], handle: str) -> tuple[bool, str | None]:
    """Whether an earlier copy recorded ``handle`` as shared, and with whom
    (``None``: a record from before the recipient was kept, unless a
    completion made it, which shared with the owner)."""
    record = _job_context(job).get("job_outbox")
    if not (
        isinstance(record, dict)
        and record.get("shared") is True
        and record.get("handle") == handle
    ):
        return False, None
    shared_with = record.get("shared_with")
    if not shared_with and record.get("trigger") == "completion":
        shared_with = job.get("user_id")
    return True, (str(shared_with) if shared_with else None)


#: A cloud's refusal of a second share of one folder to one user.
_DUPLICATE_SHARE_KINDS = frozenset(
    {CloudBackendErrorKind.ALREADY_EXISTS, CloudBackendErrorKind.PERMISSION_DENIED}
)


async def _share_with_owner(
    backend: Any,
    folder_handle: SessionFolderHandle,
    owner: dict[str, Any],
    *,
    share_wait_seconds: float,
    folder_session_id: str,
    recorded_share: bool = False,
) -> str | None:
    """Share the folder with the owner; ``None`` when shared, else why not
    (:data:`UNSHARED_REASONS`). With ``recorded_share`` (an earlier copy
    recorded this folder as shared, with the owner or with no recorded
    recipient) a cloud that refuses a second share keeps the one it has."""
    try:
        resolved_user_id = await backend.ensure_user(
            sub=owner.get("keycloak_sub") or "",
            issuer=getattr(backend, "_keycloak_issuer", "") or "",
            email=owner.get("email"),
            display_name=owner.get("display_name"),
            preferred_username=owner.get("preferred_username"),
        )
    except CloudBackendError:
        logger.warning(
            "Job outbox: could not resolve %s's %s account for folder %s",
            owner.get("id"),
            backend.backend_id,
            folder_session_id,
            exc_info=True,
        )
        return "share_failed"
    if not resolved_user_id:
        # Not fatal — the files still land in the agent's cloud home and a
        # later re-sync shares them once the account exists. But it IS the
        # difference between "the folder opens" and "the folder 404s", so
        # say so loudly here and hand the caller a `shared: false` it can
        # surface. Backends that cannot provision accounts themselves
        # (Nextcloud: user_oidc materialises the account on the user's
        # first browser login) resolve to None until the user has signed
        # in to the cloud at least once.
        logger.warning(
            "Job outbox: no %s account for user %s (email=%r) — folder "
            "%s created but NOT shared; the user must sign in to the cloud "
            "once before it becomes visible to them",
            backend.backend_id,
            owner.get("id"),
            owner.get("email"),
            folder_session_id,
        )
        return "no_account"
    try:
        with bounded_wait(share_wait_seconds):
            await backend.share_session_folder(folder_handle, resolved_user_id)
    except BucketFull as exc:
        logger.info(
            "Job outbox: share of folder %s is rate limited (capacity again "
            "in %.0fs); a re-sync shares it",
            folder_session_id,
            exc.retry_after,
        )
        return "rate_limited"
    except CloudBackendError as exc:
        if recorded_share and exc.kind in _DUPLICATE_SHARE_KINDS:
            logger.info(
                "Job outbox: folder %s is shared with user %s already",
                folder_session_id,
                owner.get("id"),
            )
            return None
        logger.warning(
            "Job outbox: sharing folder %s with user %s failed",
            folder_session_id,
            owner.get("id"),
            exc_info=True,
        )
        return "share_failed"
    return None


async def deliver_job_outbox(
    *,
    job: dict[str, Any],
    owner: dict[str, Any],
    dependencies: JobExportDependencies,
    share_wait_seconds: float = 0.0,
    reshare: bool = False,
    trigger: str = "completion",
) -> dict[str, Any]:
    """Copy a job's deliverables into its outbox folder and share it with
    its owner; return what landed.

    ``owner`` is the job's owner, whoever asked for the copy. A folder
    recorded as shared with them is not shared again unless ``reshare`` (a
    re-sync, which re-creates a share the owner removed).

    Copies the agent's declared deliverables (from
    ``freeze_data.deliverables``; falls back to ``output/`` for jobs without
    a deliverables list) into a per-job session-style cloud folder shared
    with ``owner``. Workspace-relative paths are kept **except** for the
    leading directories every deliverable shares, which are collapsed
    (``_common_dir_prefix``) so a lone ``output/digest.md`` opens as
    ``digest.md`` instead of hiding a level down.

    Re-syncable: a repeat call overwrites the same folder and re-stamps
    ``exported_at`` as "last synced at" (e.g. after resume-with-feedback). The
    folder name is derived deterministically from the job, and a job that has
    been exported before reuses its stored handle (and its share, when an
    earlier copy recorded one). v1 overwrites in place and does not prune
    files removed between syncs — note that a re-sync after the deliverable
    set changes can therefore leave files from the previous shape behind,
    since the collapsed prefix is recomputed per call.

    Returns ``{job_id, files_copied, shared, shared_with, unshared?, folder,
    handle}``; raises :class:`JobOutboxError` when the copy did not land.
    Neither the status nor anything else of the job changes, apart from its
    exported folder stamp and, for a new folder, the record that it is the
    job's (``job_outbox.state: copying``) before the first file lands.

    See knowledge-history/done/job_cloud_export.md §3.2.
    """
    job_id = str(job["id"])
    # Fresh export folder — no project/thread row to pin to, so resolve via
    # the owner seam (returns the active backend today; per-org under
    # multi-tenancy). Issue 16, knowledge-base/knowledge/issues/main_cloud.md.
    try:
        backend = dependencies.cloud_router.for_owner(owner)
    except Exception as exc:
        # No main cloud serves the owner (no active installation authority):
        # a recorded refusal, never an exception past the copy's record.
        raise JobOutboxError(
            "backend_unavailable", 503, "Cloud backend not available."
        ) from exc
    if not backend.is_initialized:
        raise JobOutboxError("backend_unavailable", 503, "Cloud backend not available.")
    if not dependencies.forge.is_initialized:
        raise JobOutboxError("forge_unavailable", 503, "Gitea not available.")

    repo_name, branch = await dependencies.resolve_job_repo(job_id)

    # 1) Provision the folder. A job that has been exported before keeps the
    #    folder it already has — re-deriving the name would strand the old
    #    folder (and its share) whenever the naming scheme changes, which it did
    #    once already when these went from `job-<uuid>` to slugged names. A new
    #    one is created exclusively: a folder of that name that exists already
    #    is not recorded as this job's, so nothing is copied into it.
    previous_handle = _recorded_folder(job)
    if previous_handle:
        folder_session_id = (
            SessionFolderHandle.from_db(previous_handle, backend=backend.backend_id)
            .native_id.rstrip("/")
            .split("/")[-1]
        )
    else:
        folder_session_id = _job_export_folder_name(job_id, job.get("description"))
    try:
        folder_handle = await backend.ensure_session_folder(
            session_id=folder_session_id, exclusive=not previous_handle
        )
    except CloudBackendError as e:
        if e.kind is CloudBackendErrorKind.ALREADY_EXISTS and not previous_handle:
            logger.error(
                "Job outbox: folder %s exists but is not recorded as job %s's; "
                "nothing copied",
                folder_session_id,
                job_id,
            )
            raise JobOutboxError(
                "folder_failed",
                409,
                f"A cloud folder named '{folder_session_id}' already exists and "
                "is not recorded as this job's; nothing was copied into it.",
            ) from e
        logger.exception("Job outbox: folder provisioning failed for job %s", job_id)
        raise JobOutboxError(
            "folder_failed", 502, f"Cloud folder provisioning failed: {e}"
        ) from e
    handle_db = folder_handle.to_db()
    if not previous_handle:
        # The folder is the job's from here: a copy that fails or is cut off
        # before its stamp resumes into it rather than finding it "taken".
        await _record(
            dependencies.store,
            job_id,
            {
                "trigger": trigger,
                "at": datetime.now(timezone.utc).isoformat(),
                "state": "copying",
                "folder": folder_session_id,
                "handle": handle_db,
            },
        )

    # 2) Copy the job's deliverables from Gitea → cloud, bytes-faithful via
    #    get_file_bytes so binary outputs (PDFs, images) survive the round trip
    #    and the declared workspace-relative paths are preserved. The agent's
    #    declared deliverables (validated non-empty at freeze time) are the
    #    curated result set — for code jobs they live under ``repo/`` and the
    #    workspace root, not ``output/``. Jobs without a deliverables list
    #    (older jobs) fall back to copying ``output/`` wholesale. Per-file read
    #    failures in the deliverables path are logged and skipped (fail-soft) so
    #    one missing artifact doesn't sink the whole export; ``files_copied``
    #    reflects what actually landed.
    files_copied = 0

    # Declared deliverables from freeze_data (JSONB may arrive as a str).
    freeze_data = job.get("freeze_data")
    if isinstance(freeze_data, str):
        try:
            freeze_data = json.loads(freeze_data)
        except (json.JSONDecodeError, TypeError):
            freeze_data = None
    deliverables: list[str] = []
    if isinstance(freeze_data, dict) and isinstance(
        freeze_data.get("deliverables"), list
    ):
        deliverables = [
            str(p).strip() for p in freeze_data["deliverables"] if str(p).strip()
        ]

    async def _list_tree(src_dir: str) -> list[str]:
        """Repo-relative paths of every file under ``src_dir``, recursively."""
        found: list[str] = []
        entries = await dependencies.forge.list_contents(repo_name, src_dir, ref=branch)
        for entry in entries or []:
            entry_type = entry.get("type")
            if entry_type == "dir":
                found.extend(await _list_tree(entry["path"]))
            elif entry_type == "file":
                found.append(entry["path"])
        return found

    async def _copy(rel: str, dest: str, *, required: bool) -> None:
        nonlocal files_copied
        file_bytes = await dependencies.forge.get_file_bytes(repo_name, rel, ref=branch)
        if file_bytes is None:
            if required:
                raise JobOutboxError(
                    "file_missing", 502, f"Failed to read '{rel}' from Gitea."
                )
            # Declared but absent from the repo (e.g. a directory entry or a
            # workspace-only artifact). Skip fail-soft rather than fail.
            logger.warning(
                "Job outbox: deliverable %r not found in repo %s; skipping",
                rel,
                repo_name,
            )
            return
        await backend.put_session_file(folder_handle, path=dest, content=file_bytes)
        files_copied += 1

    try:
        if deliverables:
            # Declared paths are workspace-root-relative, matching the job's
            # Gitea repo layout. Reject path escapes defensively.
            sources: list[str] = []
            for declared in deliverables:
                rel = declared.lstrip("/")
                if not rel or ".." in rel.split("/"):
                    logger.warning(
                        "Job outbox: skipping unsafe deliverable path %r", declared
                    )
                    continue
                sources.append(rel)
            # A declared-but-missing deliverable is not fatal (see _copy).
            required = False
        else:
            # No declared deliverables (older jobs) — copy output/ wholesale.
            sources = await _list_tree("output")
            required = True

        # Collapse the wrapper directories every file shares, so a job whose one
        # deliverable is output/digest.md lands as digest.md at the folder root
        # rather than behind two clicks. Structure that actually distinguishes
        # files survives — repo/src/a.py + repo/tests/b.py only lose `repo/`.
        prefix = _common_dir_prefix(sources)
        for rel in sources:
            await _copy(
                rel,
                rel[len(prefix) + 1 :] if prefix else rel,
                required=required,
            )
    except JobOutboxError as e:
        e.handle = handle_db
        raise
    except CloudBackendError as e:
        logger.exception(
            "Job outbox: cloud upload failed for job %s (copied %d)",
            job_id,
            files_copied,
        )
        raise JobOutboxError(
            "copy_failed",
            502,
            f"File copy to cloud failed after {files_copied} files: {e}",
            handle=handle_db,
        ) from e
    except Exception as e:
        logger.exception("Job outbox: unexpected failure for job %s", job_id)
        raise JobOutboxError(
            "copy_failed", 502, f"Export failed: {e}", handle=handle_db
        ) from e

    # 3) Stamp the job — only on success so retries are safe.
    await dependencies.store.update_job_exported_folder(job_id, handle=handle_db)

    # 4) Share the folder with the owner, after the files landed: a folder
    #    that cannot be shared yet still holds them, and a re-sync shares it.
    owner_id = str(owner["id"])
    recorded, shared_with = _recorded_share(job, handle_db)
    if recorded and shared_with == owner_id and not reshare:
        unshared = None
    else:
        unshared = await _share_with_owner(
            backend,
            folder_handle,
            owner,
            share_wait_seconds=share_wait_seconds,
            folder_session_id=folder_session_id,
            recorded_share=recorded and shared_with in (None, owner_id),
        )

    result: dict[str, Any] = {
        "job_id": job_id,
        "files_copied": files_copied,
        # False = copied, but the folder is not visible to the owner yet
        # (``unshared`` says why). The cockpit turns this into a distinct
        # toast instead of claiming an unqualified success.
        "shared": unshared is None,
        # Always the job's owner: the outbox is theirs, whoever re-synced it.
        "shared_with": owner_id if unshared is None else None,
        "folder": {
            "name": folder_session_id,
            # Where the owner will actually find it: the share lands at the
            # root of their own cloud drive, so the folder name IS the path.
            # Sent explicitly so the cockpit doesn't have to assume that.
            "path": f"/{folder_session_id}",
            "browser_url": backend.get_session_folder_browser_url(folder_handle),
            "webdav_url": backend.get_session_folder_webdav_url(folder_handle),
        },
        "handle": handle_db,
    }
    if unshared is not None:
        result["unshared"] = unshared
    return result


def outbox_record(
    result: dict[str, Any] | None = None,
    *,
    error: JobOutboxError | None = None,
    trigger: str,
) -> dict[str, Any]:
    """``context.job_outbox``: what the latest copy did, bounded."""
    record: dict[str, Any] = {
        "trigger": trigger,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    if error is not None:
        record.update(
            state="failed", reason=error.reason, detail=str(error.detail)[:500]
        )
        if error.handle:
            record["handle"] = error.handle
        return record
    if result is None:
        raise ValueError("an outbox record needs a result or an error")
    record.update(
        state="delivered",
        files=int(result["files_copied"]),
        shared=bool(result["shared"]),
        folder=result["folder"]["name"],
        handle=result["handle"],
    )
    if result.get("shared_with"):
        record["shared_with"] = str(result["shared_with"])
    if result.get("unshared"):
        record["unshared"] = result["unshared"]
    return record


async def _record(store: Any, job_id: str, record: dict[str, Any]) -> None:
    try:
        await store.merge_job_context(job_id, {"job_outbox": record})
    except Exception:
        logger.warning(
            "Job outbox: could not record job %s's copy", job_id, exc_info=True
        )


async def export_job_to_shared_folder(
    *,
    job_id: str,
    user: dict[str, Any],
    authorized_job: dict[str, Any],
    dependencies: JobExportDependencies,
) -> dict[str, Any]:
    """Re-sync a job's outbox now: :func:`deliver_job_outbox`, as an HTTP
    answer ("Open cloud folder").

    Valid for any job whose output is stable (``completed``,
    ``pending_review`` or ``reviewing``), whether or not its project has a
    cloud folder: the outbox is the job's deliverables, never the project
    folder's copy, which goes through the diff review. Any member who can
    see the job may re-sync it (``user``), but the folder is the job
    owner's and is shared with them only, re-created when they removed it;
    a job whose owner is gone is refused (409). A failed copy is the HTTP
    error :class:`JobOutboxError` carries.
    """
    job = authorized_job

    # Status gate — in-flight, failed or cancelled jobs have no stable
    # output to copy. In-review (pending_review, reviewing) export lets the
    # user preview the deliverables in the cloud before approving.
    if job.get("status") not in EXPORTABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job is in status '{job.get('status')}'; "
                "only completed or in-review jobs can be exported."
            ),
        )

    # The outbox is the job owner's, whoever re-syncs it.
    owner_id = str(job.get("user_id") or "")
    owner = await dependencies.store.get_user(owner_id) if owner_id else None
    if not owner:
        raise HTTPException(
            status_code=409,
            detail="The job's owner is gone; its outbox is shared with its owner only.",
        )
    if str(user.get("id") or "") != owner_id:
        logger.info(
            "Job outbox: user %s re-syncs job %s's outbox for its owner %s",
            user.get("id"),
            job_id,
            owner_id,
        )

    # No idempotency refusal: this endpoint is re-syncable. A repeat call
    # overwrites the job's own folder and re-stamps ``exported_at`` as "last
    # synced at".
    try:
        result = await deliver_job_outbox(
            job={**job, "id": job_id},
            owner=owner,
            dependencies=dependencies,
            share_wait_seconds=RESYNC_SHARE_WAIT_SECONDS,
            reshare=True,
            trigger="resync",
        )
    except JobOutboxError as exc:
        # Not recorded: a failed re-sync leaves the last copy's record (and
        # a new folder's ``copying`` record) as it was.
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    await _record(dependencies.store, job_id, outbox_record(result, trigger="resync"))
    result = dict(result)
    result.pop("handle", None)
    return result


async def run_job_outbox_effect(
    *, job_id: str, dependencies: JobExportDependencies
) -> dict[str, Any]:
    """The completion effect ``job_outbox_delivery`` (S16b): copy a root
    job's deliverables into its owner's outbox.

    Best-effort: the job's status never depends on it. What happened goes
    into ``context.job_outbox`` and the effect's small output; a failure is
    recorded with its closed reason, and the re-sync endpoint retries it.
    The share does not wait on Nextcloud's bucket: a full one leaves the
    folder ``unshared: rate_limited``.
    """
    job = await dependencies.store.get_job(job_id)
    owner = (
        await dependencies.store.get_user(str(job["user_id"]))
        if job and job.get("user_id")
        else None
    )
    if not job or not owner:
        error = JobOutboxError("owner_unavailable", 409, "The job or its owner is gone")
        await _record(
            dependencies.store, job_id, outbox_record(error=error, trigger="completion")
        )
        return {"state": "failed", "reason": error.reason}
    try:
        result = await deliver_job_outbox(
            job=job, owner=owner, dependencies=dependencies, share_wait_seconds=0.0
        )
    except JobOutboxError as exc:
        await _record(
            dependencies.store, job_id, outbox_record(error=exc, trigger="completion")
        )
        return {"state": "failed", "reason": exc.reason, "error": exc.detail}
    await _record(
        dependencies.store, job_id, outbox_record(result, trigger="completion")
    )
    return {
        "state": "delivered",
        "files": int(result["files_copied"]),
        "shared": bool(result["shared"]),
        "unshared": result.get("unshared"),
    }
