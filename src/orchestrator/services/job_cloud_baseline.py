"""Mode A baseline seeding + diff capture for the job cloud workflow.

See ``knowledge-history/done/job_cloud_export.md`` §3 for the design. This module
owns the cloud→Gitea seed at job-start and the Gitea-diff capture at
job-completion. Both run from the orchestrator side; the agent stays
unchanged (it picks the seeded ``projects/<slug>/`` up via its existing
clone-on-start of the job's Gitea repo).

The seed is async via ``asyncio.create_task`` from the job-create
handler. While it runs, the job carries
``context.cloud_baseline = {state: 'seeding'}``; the dispatcher gate
in ``get_dispatchable_jobs`` skips jobs in that state so the agent
never starts on an incomplete baseline.

Compatibility limitation:

* Ordinary human-reviewed Mode A jobs retain the original text-only baseline
  behavior. Strict loop baselines include binary files byte-for-byte because a
  file-producing loop cannot silently start from an incomplete project folder.

Both the OpenCloud and Nextcloud backends implement the byte-level
project-folder methods this module relies on
(``list_project_folder`` / ``get_project_folder_file_bytes`` /
``put_project_folder_file_bytes`` / ``delete_project_folder_file``), so
Mode A works on either active backend.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from orchestrator.services.cloud import (
    CloudBackendError,
    MainCloudRouter,
    ProjectFolderEntry,
    ProjectFolderHandle,
)

logger = logging.getLogger(__name__)


async def _permit(authority_check: Callable[[], Awaitable[None]] | None) -> None:
    if authority_check is not None:
        await authority_check()


def _job_context(job: dict[str, Any]) -> dict[str, Any]:
    """Return ``job['context']`` as a dict.

    asyncpg returns JSONB columns as Python strings (no codec is
    registered project-wide), so callers manually ``json.loads`` it.
    Mirrors the pattern in ``src/orchestrator/main.py`` everywhere ``context``
    is read.
    """
    raw = job.get("context")
    if not raw:
        return {}
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return raw if isinstance(raw, dict) else {}


def slugify_project_name(name: str) -> str:
    """Same algorithm as ``main._slugify_mount_name`` — kept here to
    avoid the circular import. Workspace-safe slug for a project name.
    """
    out = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    return out or "project"


def project_folder_slug(job: dict[str, Any], project: dict[str, Any]) -> str:
    """Return the immutable seed-time cloud checkout slug for a job.

    A project may be renamed while a job is running. Re-deriving the path from
    the current name at completion would skip every diff path under the old
    slug while falsely reporting a successful apply. New baselines persist the
    seed-time slug; legacy baselines fall back to the current project name.
    """
    baseline = _job_context(job).get("cloud_baseline") or {}
    if isinstance(baseline, dict) and baseline.get("project_slug"):
        return str(baseline["project_slug"])
    return slugify_project_name(str(project.get("name") or "project"))


_BASELINE_COMMIT_MESSAGE = (
    "Mode A baseline: seed project folder for job {job_short}\n\n"
    "Seeded by orchestrator from project '{project_name}' cloud folder. "
    "This is the diff baseline; agent edits on top become the diff at "
    "job completion. See knowledge-history/done/job_cloud_export.md."
)


#: What a finished seed walk records in ``context.cloud_baseline``: files
#: copied into the job's repository, binary files left out (a text-only
#: legacy copy), and files that could not be read or written.
SEED_COUNT_KEYS: tuple[str, ...] = ("seeded", "skipped_binary", "failed")

#: How long a copy may stay ``seeding`` before the stale-seed sweep (the
#: leader's stale detector) fails it, so the job dispatches without it. A
#: seed records when it started (``cloud_baseline.since``); a copy no seed
#: started yet (admission's stamp, a request cut off before the seed was
#: fired) counts from the job's last write. Far above a real walk: a seed
#: the sweep overtakes still finishes, and would then push onto a branch the
#: agent already works on.
SEED_STALE_AFTER_SECONDS = 30 * 60

#: What the sweep records as the failed copy's ``error``.
SEED_STALE_REASON = (
    "the project folder copy did not finish within 30 minutes (its seed was "
    "lost, e.g. with a restarted orchestrator); the job runs without it"
)


async def seed_project_folder_baseline(
    *,
    job_id: str,
    project: dict[str, Any],
    repo_name: str,
    branch: str | None,
    postgres_db: Any,
    gitea_client: Any,
    main_cloud_router: MainCloudRouter,
    require_complete: bool = False,
    include_binary: bool = False,
    authority_check: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Walk the project's cloud folder, push files into Gitea, stamp baseline.

    Sets ``jobs.cloud_diff_baseline_commit`` on success.
    Updates ``jobs.context.cloud_baseline.state`` to ``'seeding'`` while
    running, then ``'ready'`` or ``'failed'`` at termination. The
    dispatcher gate watches that field. A finished walk also records what it
    did (``seeded``, ``skipped_binary``, ``failed``), which the agent's task
    brief shows: a copy that silently lacks files is a copy the agent cannot
    be held to.

    ``include_binary`` seeds binary files byte for byte, as a loop does
    (``require_complete``), without failing the seed on a file it cannot
    copy: the project-folder checkout connector's copy. Without either, a
    human-reviewed legacy job keeps its text-only copy.

    Idempotency: if ``cloud_diff_baseline_commit`` is already set, this
    is a no-op (already seeded). Safe to invoke twice (e.g. on a retry).
    """
    job_short = job_id[:8]
    project_name = str(project.get("name") or "project")
    slug = slugify_project_name(project_name)
    target_subpath = f"projects/{slug}"

    async def _set_state(
        state: str,
        *,
        error: str | None = None,
        entries: dict[str, str] | None = None,
        project_slug: str | None = None,
        counts: dict[str, int] | None = None,
    ) -> None:
        """Merge ``cloud_baseline`` state at the top level. The merge is
        a JSONB ``||`` so we replace the whole ``cloud_baseline`` object
        each call; pass ``entries`` to persist the path→etag map and
        ``counts`` (:data:`SEED_COUNT_KEYS`) for what a walk did.
        """
        payload: dict[str, Any] = {
            "state": state,
            "project_slug": project_slug or slug,
        }
        if state == "seeding":
            # The stale-seed sweep counts from here (SEED_STALE_AFTER_SECONDS).
            payload["since"] = datetime.now(timezone.utc).isoformat()
        if error:
            payload["error"] = error
        if entries is not None:
            payload["entries"] = entries
        if counts is not None:
            payload.update({key: int(counts.get(key) or 0) for key in SEED_COUNT_KEYS})
        await _permit(authority_check)
        try:
            await postgres_db.merge_job_context(job_id, {"cloud_baseline": payload})
        except Exception:
            logger.exception(
                "Mode A: failed to update baseline state for job %s", job_id
            )
        await _permit(authority_check)

    try:
        # Idempotency: a re-run after a successful seed should no-op.
        await _permit(authority_check)
        existing = await postgres_db.get_job(job_id)
        await _permit(authority_check)
        if existing and existing.get("cloud_diff_baseline_commit"):
            existing_baseline = _job_context(existing).get("cloud_baseline") or {}
            if not isinstance(existing_baseline, dict):
                existing_baseline = {}
            recorded = {
                key: existing_baseline[key]
                for key in SEED_COUNT_KEYS
                if isinstance(existing_baseline.get(key), int)
            }
            await _set_state(
                "ready",
                entries=existing_baseline.get("entries") or {},
                project_slug=existing_baseline.get("project_slug") or slug,
                counts=recorded or None,
            )
            return

        await _set_state("seeding")

        # Resolve the cloud handle.
        handle_db = project.get("main_cloud_folder_handle")
        backend_id = project.get("main_cloud_backend")
        if not handle_db or not backend_id:
            logger.info(
                "Mode A: job %s — project %s has no cloud folder; "
                "skipping baseline seed (will run as loose job).",
                job_short,
                project_name,
            )
            await _set_state("ready")
            return

        try:
            backend = main_cloud_router.for_backend(backend_id)
        except Exception as e:
            logger.warning(
                "Mode A: job %s — backend %s not available; skipping seed (%s)",
                job_short,
                backend_id,
                e,
            )
            await _set_state("failed", error=f"backend unavailable: {e}")
            return

        if not getattr(backend, "is_initialized", False):
            logger.warning(
                "Mode A: job %s — backend %s not initialized; skipping seed",
                job_short,
                backend_id,
            )
            await _set_state("failed", error="backend not initialized")
            return

        handle = ProjectFolderHandle.from_db(str(handle_db), backend=backend_id)

        # 1. Enumerate everything under the project folder.
        try:
            await _permit(authority_check)
            entries: list[ProjectFolderEntry] = await backend.list_project_folder(
                handle
            )
            await _permit(authority_check)
        except CloudBackendError as e:
            logger.warning(
                "Mode A: job %s — list_project_folder failed (%s); skipping seed",
                job_short,
                e,
            )
            await _set_state("failed", error=f"list_project_folder: {e}")
            return

        # 2. Filter to files. Walk dirs are no-ops in git.
        files = [e for e in entries if not e.is_dir]
        # Path → etag map of every file we observed in the cloud at seed
        # time. Persisted into context so the accept-time external-mod
        # check can compare against it. We capture ALL files here, not
        # just the UTF-8 ones we seed into Gitea — a binary file the
        # agent never sees can still be evidence of an external edit.
        entries_map: dict[str, str] = {e.path: e.etag for e in files}
        if not files:
            logger.info(
                "Mode A: job %s — project folder is empty; no baseline files",
                job_short,
            )
            # Still record a baseline commit hash so completion-time diff
            # has something to diff against (the empty tree).
            # We do this by capturing the current HEAD of the job branch.
            await _permit(authority_check)
            head_sha = await _read_head_commit(gitea_client, repo_name, branch)
            await _permit(authority_check)
            if not head_sha:
                await _set_state("failed", error="could not read empty baseline HEAD")
                return
            await postgres_db.update_job_cloud_diff(job_id, baseline_commit=head_sha)
            await _set_state("ready", entries=entries_map, counts={})
            return

        seeded = 0
        skipped_binary = 0
        failed_files = 0
        for entry in files:
            try:
                await _permit(authority_check)
                blob = await backend.get_project_folder_file_bytes(
                    handle, path=entry.path
                )
                await _permit(authority_check)
            except CloudBackendError as e:
                logger.warning(
                    "Mode A: job %s — failed to fetch %s (%s); skipping",
                    job_short,
                    entry.path,
                    e,
                )
                failed_files += 1
                continue
            # Human-reviewed Mode A keeps its original text-only baseline.
            # Loops are strict: seed binary content byte-for-byte through
            # Gitea's ChangeFiles API so the cloned workspace is a complete
            # project-folder snapshot and generated binary artifacts can make
            # the reverse trip to cloud storage.
            try:
                content_text = blob.decode("utf-8")
            except UnicodeDecodeError:
                if require_complete or include_binary:
                    await _permit(authority_check)
                    ok = await gitea_client.change_files(
                        repo_name,
                        branch or "main",
                        [
                            {
                                "path": f"{target_subpath}/{entry.path}",
                                "content_b64": base64.b64encode(blob).decode("ascii"),
                            }
                        ],
                        message=_BASELINE_COMMIT_MESSAGE.format(
                            job_short=job_short, project_name=project_name
                        ),
                    )
                    await _permit(authority_check)
                    if ok:
                        seeded += 1
                    else:
                        logger.warning(
                            "Mode A: job %s — gitea binary write failed for %s",
                            job_short,
                            entry.path,
                        )
                        failed_files += 1
                else:
                    skipped_binary += 1
                    logger.debug(
                        "Mode A: job %s — skipping binary %s (%d bytes)",
                        job_short,
                        entry.path,
                        len(blob),
                    )
                continue
            gitea_path = f"{target_subpath}/{entry.path}"
            await _permit(authority_check)
            ok = await gitea_client.create_or_update_file(
                repo_name,
                gitea_path,
                content_text,
                _BASELINE_COMMIT_MESSAGE.format(
                    job_short=job_short, project_name=project_name
                ),
                branch=branch,
            )
            await _permit(authority_check)
            if ok:
                seeded += 1
            else:
                logger.warning(
                    "Mode A: job %s — gitea write failed for %s",
                    job_short,
                    gitea_path,
                )
                failed_files += 1

        counts = {
            "seeded": seeded,
            "skipped_binary": skipped_binary,
            "failed": failed_files,
        }
        if require_complete and failed_files:
            await _set_state(
                "failed",
                error=(
                    "loop cloud baseline was incomplete: "
                    f"{failed_files} file read/write failure(s)"
                ),
                entries=entries_map,
                counts=counts,
            )
            return

        # 3. Capture the head of the branch as the baseline commit.
        await _permit(authority_check)
        baseline_sha = await _read_head_commit(gitea_client, repo_name, branch)
        await _permit(authority_check)
        if not baseline_sha:
            logger.warning(
                "Mode A: job %s — could not read HEAD after seeding; "
                "baseline commit hash not recorded",
                job_short,
            )
            await _set_state(
                "failed", error="could not read HEAD after seed", counts=counts
            )
            return

        await _permit(authority_check)
        await postgres_db.update_job_cloud_diff(job_id, baseline_commit=baseline_sha)
        await _permit(authority_check)
        logger.info(
            "Mode A: job %s — seeded %d file(s), skipped %d binary, "
            "failed %d, baseline=%s",
            job_short,
            seeded,
            skipped_binary,
            failed_files,
            baseline_sha[:8],
        )
        await _set_state("ready", entries=entries_map, counts=counts)
    except Exception as e:
        if authority_check is not None:
            # Durable project-loop handoff authority failures are fail-closed:
            # never convert lease loss into a baseline "failed" write by the
            # stale owner, and never let provisioning continue to later work.
            from orchestrator.services.project_loop_atomic import (
                ProjectLoopHandoffAuthorityLost,
            )

            if isinstance(e, ProjectLoopHandoffAuthorityLost):
                raise
        logger.exception(
            "Mode A: job %s — baseline seed unexpectedly failed", job_short
        )
        await _set_state("failed", error=str(e))


async def _read_head_commit(
    gitea_client: Any, repo_name: str, branch: str | None
) -> str | None:
    """Return the SHA of HEAD on ``branch`` (or ``main`` if ``None``).

    Uses Gitea's ``GET /branches/{branch}`` endpoint, which reliably
    returns the head commit regardless of branch name (including names
    containing ``/`` like ``job/abc123``) — the precise single-branch
    lookup, vs. ``get_commits`` which lists a page of history.
    """
    return await gitea_client.get_branch_head_sha(repo_name, branch or "main")


def fire_baseline_seed(
    *,
    job_id: str,
    project: dict[str, Any],
    repo_name: str,
    branch: str | None,
    postgres_db: Any,
    gitea_client: Any,
    main_cloud_router: MainCloudRouter,
    require_complete: bool = False,
    include_binary: bool = False,
) -> asyncio.Task:
    """Fire-and-forget wrapper around :func:`seed_project_folder_baseline`.

    Used from the job-create endpoint where we don't want to block the
    user's request while the cloud folder walk runs. Returns the task
    so the caller can attach error logging or await in tests.
    """
    return asyncio.create_task(
        seed_project_folder_baseline(
            job_id=job_id,
            project=project,
            repo_name=repo_name,
            branch=branch,
            postgres_db=postgres_db,
            gitea_client=gitea_client,
            main_cloud_router=main_cloud_router,
            require_complete=require_complete,
            include_binary=include_binary,
        )
    )


async def _diff_files_by_tree(
    *,
    gitea_client: Any,
    repo_name: str,
    baseline: str,
    head: str,
) -> list[dict[str, str]] | None:
    """Compute the file-level Mode A diff via Gitea tree comparison.

    Gitea 1.22's ``compare/{base}...{head}.diff`` returns 404
    ``BaseNotExist`` for raw commit SHAs (only branches/tags work
    there — see gitea#19797 + linked issues). Tree comparison gives us
    the same triage cheaply: list ``git/trees/{sha}?recursive=true`` at
    both ends, build ``{path: blob_sha}`` maps, set-diff. Result is
    scoped to ``projects/<slug>/*`` — anything outside isn't the
    agent's project-folder edit.

    Returns:
        List of ``{path, status}`` (``added`` / ``modified`` /
        ``deleted``), already filtered to ``projects/`` paths. ``None``
        if a tree fetch failed.
    """
    base_tree = await gitea_client.list_tree(repo_name, baseline)
    head_tree = await gitea_client.list_tree(repo_name, head)
    if base_tree is None or head_tree is None:
        return None

    def _blobs(tree: list[dict[str, str]]) -> dict[str, str]:
        return {
            str(e.get("path", "")): str(e.get("sha", ""))
            for e in tree
            if e.get("type") == "blob" and e.get("path")
        }

    base_blobs = _blobs(base_tree)
    head_blobs = _blobs(head_tree)
    files: list[dict[str, str]] = []
    for path in sorted(set(base_blobs.keys()) | set(head_blobs.keys())):
        if not path.startswith("projects/"):
            continue
        in_base = path in base_blobs
        in_head = path in head_blobs
        if in_base and in_head:
            if base_blobs[path] != head_blobs[path]:
                files.append({"path": path, "status": "modified"})
        elif in_head:
            files.append({"path": path, "status": "added"})
        else:
            files.append({"path": path, "status": "deleted"})
    return files


#: What a completion-time capture found, recorded in
#: ``context.cloud_checkout.capture.state``: a diff under the copy is waiting
#: for review (``captured``), the copy is unchanged (``no_changes``), the job
#: has no baseline to compare with (``no_baseline``: its seed failed or never
#: ran), or Gitea could not be read (``head_unreadable``).
CAPTURE_STATES: tuple[str, ...] = (
    "captured",
    "no_changes",
    "no_baseline",
    "head_unreadable",
)


async def capture_checkout_diff(
    *,
    job: dict[str, Any],
    postgres_db: Any,
    gitea_client: Any,
) -> dict[str, Any]:
    """At job completion, compute the project-folder copy's diff.

    Reads the job's ``cloud_diff_baseline_commit`` and the current HEAD of
    its branch, asks Gitea for tree listings at both ends, and sets
    ``jobs.diff_status = 'pending'`` when any file under the copy changed.
    The copy is ``projects/<seed-time slug>/`` when the baseline recorded
    its slug (every baseline since the slug was stamped), else any path
    under ``projects/`` (an older baseline): only paths an accept can write
    back count as a change.

    Returns ``{"state": <one of CAPTURE_STATES>, "files": <count>}``. The
    caller is responsible for the status transition itself — this helper
    only flips the ``diff_status`` flag.
    """
    job_id = str(job["id"])
    baseline = job.get("cloud_diff_baseline_commit")
    repo_name = job.get("repo_name")
    branch = job.get("branch_name")
    if not baseline or not repo_name:
        logger.debug(
            "Mode A: job %s has no baseline or no repo_name; skipping diff capture",
            job_id,
        )
        return {"state": "no_baseline", "files": 0}
    head = await _read_head_commit(gitea_client, repo_name, branch)
    if not head:
        logger.warning("Mode A: job %s — couldn't read HEAD for diff capture", job_id)
        return {"state": "head_unreadable", "files": 0}
    if head == baseline:
        return {"state": "no_changes", "files": 0}
    files = await _diff_files_by_tree(
        gitea_client=gitea_client,
        repo_name=repo_name,
        baseline=baseline,
        head=head,
    )
    if files is None:
        logger.warning(
            "Mode A: job %s — couldn't compare baseline=%s with head=%s",
            job_id,
            str(baseline)[:8],
            head[:8],
        )
        return {"state": "head_unreadable", "files": 0}
    recorded = _job_context(job).get("cloud_baseline") or {}
    slug = recorded.get("project_slug") if isinstance(recorded, dict) else None
    if slug:
        files = [
            entry
            for entry in files
            if _strip_project_prefix(entry["path"], str(slug)) is not None
        ]
    if not files:
        logger.info(
            "Mode A: job %s — no project-folder changes between baseline=%s "
            "and head=%s; skipping diff capture",
            job_id,
            str(baseline)[:8],
            head[:8],
        )
        return {"state": "no_changes", "files": 0}
    await postgres_db.update_job_cloud_diff(job_id, diff_status="pending")
    logger.info(
        "Mode A: job %s — captured %d-file diff baseline=%s head=%s; "
        "status -> pending_review",
        job_id,
        len(files),
        str(baseline)[:8],
        head[:8],
    )
    return {"state": "captured", "files": len(files)}


async def capture_diff_for_mode_a_job(
    *,
    job: dict[str, Any],
    postgres_db: Any,
    gitea_client: Any,
) -> bool:
    """:func:`capture_checkout_diff`, as whether a non-empty diff was
    captured (``jobs.diff_status`` set to ``'pending'``)."""
    captured = await capture_checkout_diff(
        job=job, postgres_db=postgres_db, gitea_client=gitea_client
    )
    return captured["state"] == "captured"


async def get_diff_summary(
    *,
    job: dict[str, Any],
    gitea_client: Any,
) -> dict[str, Any] | None:
    """Build a file-tree summary of the Mode A diff for a job.

    Returns ``{"baseline_commit": ..., "head_commit": ..., "files":
    [{"path": ..., "status": "added"|"modified"|"deleted"}]}`` or
    ``None`` if the job has no baseline / no repo. Per-file diff
    content is served lazily via the sibling endpoint.
    """
    baseline = job.get("cloud_diff_baseline_commit")
    if not baseline:
        return None
    repo_name = job.get("repo_name")
    branch = job.get("branch_name")
    if not repo_name:
        return None
    head = await _read_head_commit(gitea_client, repo_name, branch)
    if not head or head == baseline:
        return {
            "baseline_commit": baseline,
            "head_commit": head or baseline,
            "files": [],
        }
    files = await _diff_files_by_tree(
        gitea_client=gitea_client,
        repo_name=repo_name,
        baseline=baseline,
        head=head,
    )
    return {
        "baseline_commit": baseline,
        "head_commit": head,
        "files": files or [],
    }


def _strip_project_prefix(gitea_path: str, slug: str) -> str | None:
    """``projects/<slug>/sub/file.md`` → ``sub/file.md``.

    Returns ``None`` if the path is outside the slug — defensive guard
    against a diff that somehow includes paths from a different slug.
    """
    prefix = f"projects/{slug}/"
    if not gitea_path.startswith(prefix):
        return None
    return gitea_path[len(prefix) :]


async def detect_external_mods(
    *,
    job: dict[str, Any],
    project: dict[str, Any],
    main_cloud_router: MainCloudRouter,
    scope_paths: set[str] | None = None,
    strict: bool = False,
) -> list[dict[str, str]]:
    """Check whether the cloud folder has been modified since seed.

    Compares the path→etag map persisted in
    ``context.cloud_baseline.entries`` against a fresh PROPFIND of the
    project folder, scoped to paths the agent's diff actually touches.
    Files the user didn't ask the agent to change are deliberately not
    checked — they can't conflict with the accept apply.

    Returns a list of ``{path, kind}`` dicts (one per diverged path),
    where ``kind`` is ``etag_mismatch`` / ``missing_at_cloud`` /
    ``unexpected_at_cloud``. Empty list means the apply is safe.

    ``scope_paths`` limits the comparison to cloud-relative paths touched by
    the job. Passing ``None`` retains the legacy whole-folder check.

    ``strict=True`` is for unattended loop delivery. It raises when the live
    folder cannot be enumerated instead of treating an unknown conflict state
    as clean. The human-reviewed compatibility path keeps the legacy
    fail-through behavior with the default ``False``.
    """
    baseline_entries = _job_context(job).get("cloud_baseline", {}).get("entries") or {}
    handle_db = project.get("main_cloud_folder_handle")
    backend_id = project.get("main_cloud_backend")
    if not handle_db or not backend_id:
        if strict:
            raise RuntimeError("project cloud folder is unavailable")
        # Legacy compatibility: no cloud folder was treated as clean and the
        # apply step surfaced the actual failure.
        return []
    try:
        backend = main_cloud_router.for_backend(backend_id)
    except Exception:
        if strict:
            raise
        return []
    if not getattr(backend, "is_initialized", False):
        if strict:
            raise RuntimeError(f"cloud backend {backend_id!r} is not initialized")
        # Backend unavailable — caller decides whether that's fatal.
        # For external-mod detection we err on "we can't tell," so
        # return an empty list. The apply call will fail loudly when
        # it can't talk to the backend, which is the right place to
        # surface that.
        return []
    handle = ProjectFolderHandle.from_db(str(handle_db), backend=backend_id)
    return await detect_external_mods_against_baseline(
        baseline_entries=baseline_entries,
        backend=backend,
        handle=handle,
        scope_paths=scope_paths,
        strict=strict,
    )


async def deliver_loop_diff_to_cloud(
    *,
    job: dict[str, Any],
    project: dict[str, Any],
    postgres_db: Any,
    gitea_client: Any,
    main_cloud_router: MainCloudRouter,
    completion_command_id: str | None = None,
) -> dict[str, Any]:
    """Apply a loop job's isolated project-file diff to its cloud folder.

    This is the unattended counterpart to the human accept endpoint. It is
    deliberately fail-closed: only a completely readable, conflict-free diff
    advances automatically. Every ambiguous outcome returns ``needs_review``
    so the completion handler can park the job at ``pending_review`` and keep
    the loop barrier intact.

    Returns ``{delivery_status, needs_review, delivery_sha, notes, ...}``.
    Stable delivery statuses are ``no-changes`` and ``cloud-applied``;
    review statuses name the reason (``cloud-conflict``, ``cloud-partial``,
    ``cloud-unavailable``). When ``completion_command_id`` is present, a
    bounded command-owned apply intent closes the apply-before-final-stamp
    crash window; omitting it preserves the legacy path exactly.
    """
    job_id = str(job.get("id"))
    existing_delivery = _job_context(job).get("loop_cloud_delivery") or {}
    if not isinstance(existing_delivery, dict):
        existing_delivery = {}
    existing_status = str(
        job.get("merge_status")
        or existing_delivery.get("delivery_status")
        or ("cloud-applied" if job.get("diff_status") == "accepted" else "")
    )
    if existing_status in {"cloud-applied", "no-changes"}:
        # Completion callbacks are at-least-once. Once delivery is durably
        # stamped, never re-apply the same isolated diff or reinterpret our own
        # prior cloud write as an external conflict.
        return {
            "delivery_status": existing_status,
            "needs_review": False,
            "delivery_sha": existing_delivery.get("delivery_sha"),
            "notes": [str(note) for note in (existing_delivery.get("notes") or [])],
            "applied": int(existing_delivery.get("applied") or 0),
            "deleted": int(existing_delivery.get("deleted") or 0),
        }

    baseline = job.get("cloud_diff_baseline_commit")
    repo_name = job.get("repo_name")
    branch = job.get("branch_name") or "main"
    if not baseline or not repo_name:
        return {
            "delivery_status": "cloud-unavailable",
            "needs_review": True,
            "delivery_sha": None,
            "notes": ["loop job has no complete cloud baseline or isolated repo"],
        }

    head = await _read_head_commit(gitea_client, repo_name, branch)
    if not head:
        return {
            "delivery_status": "cloud-unavailable",
            "needs_review": True,
            "delivery_sha": None,
            "notes": ["could not read the isolated job repository HEAD"],
        }

    files = await _diff_files_by_tree(
        gitea_client=gitea_client,
        repo_name=repo_name,
        baseline=str(baseline),
        head=head,
    )
    if files is None:
        return {
            "delivery_status": "cloud-unavailable",
            "needs_review": True,
            "delivery_sha": head,
            "notes": ["could not compare the cloud baseline with job HEAD"],
        }
    slug = project_folder_slug(job, project)
    project_files = [
        (entry, rel)
        for entry in files
        if (rel := _strip_project_prefix(entry["path"], slug)) is not None
    ]
    if not project_files:
        # Framework/output commits remain useful in the isolated execution
        # audit trail, but only paths under projects/<seed-time-slug>/ are
        # deliverable project-cloud changes.
        return {
            "delivery_status": "no-changes",
            "needs_review": False,
            "delivery_sha": head,
            "notes": [],
            "applied": 0,
            "deleted": 0,
        }

    await postgres_db.update_job_cloud_diff(job_id, diff_status="pending")
    job["diff_status"] = "pending"

    scope_paths = {rel for _entry, rel in project_files}
    resume_own_apply = bool(
        completion_command_id
        and existing_delivery.get("delivery_status") == "cloud-applying"
        and str(existing_delivery.get("completion_command_id") or "")
        == completion_command_id
        and str(existing_delivery.get("baseline_commit") or "") == str(baseline)
        and str(existing_delivery.get("delivery_sha") or "") == head
    )
    if not resume_own_apply:
        try:
            diverged = await detect_external_mods(
                job=job,
                project=project,
                main_cloud_router=main_cloud_router,
                scope_paths=scope_paths,
                strict=True,
            )
        except Exception as e:
            return {
                "delivery_status": "cloud-unavailable",
                "needs_review": True,
                "delivery_sha": head,
                "notes": [f"could not verify the live cloud baseline: {e}"],
            }
        if diverged:
            return {
                "delivery_status": "cloud-conflict",
                "needs_review": True,
                "delivery_sha": head,
                "notes": [
                    f"cloud changed since baseline: {item['path']}" for item in diverged
                ],
                "diverged": diverged,
            }

        if completion_command_id:
            # Register a bounded, command-owned intent only after the live
            # divergence check is clean and before the first WebDAV mutation.
            # A crash after any PUT/DELETE can then replay the same idempotent
            # diff without mistaking those writes for somebody else's edit.
            # Per-path inventory remains in the established cloud baseline and
            # final loop_cloud_delivery context; never copy it into this stamp.
            delivery_intent = {
                "delivery_status": "cloud-applying",
                "completion_command_id": completion_command_id,
                "baseline_commit": str(baseline),
                "delivery_sha": head,
            }
            intent_persisted = await postgres_db.merge_job_context(
                job_id, {"loop_cloud_delivery": delivery_intent}
            )
            if not intent_persisted:
                raise RuntimeError("could not persist loop cloud delivery intent")
            context = dict(_job_context(job))
            context["loop_cloud_delivery"] = delivery_intent
            job["context"] = context

    applied = await apply_diff_to_cloud(
        job=job,
        project=project,
        gitea_client=gitea_client,
        main_cloud_router=main_cloud_router,
    )
    errors = [str(item) for item in (applied.get("errors") or [])]
    if errors:
        return {
            "delivery_status": "cloud-partial",
            "needs_review": True,
            "delivery_sha": head,
            "notes": errors,
            "applied": int(applied.get("applied") or 0),
            "deleted": int(applied.get("deleted") or 0),
        }

    await postgres_db.update_job_cloud_diff(job_id, diff_status="accepted")
    job["diff_status"] = "accepted"
    return {
        "delivery_status": "cloud-applied",
        "needs_review": False,
        "delivery_sha": head,
        "notes": [],
        "applied": int(applied.get("applied") or 0),
        "deleted": int(applied.get("deleted") or 0),
    }


async def detect_external_mods_against_baseline(
    *,
    baseline_entries: dict[str, str],
    backend: Any,
    handle: Any,
    scope_paths: set[str] | None = None,
    strict: bool = False,
) -> list[dict[str, str]]:
    """Live-compare a path→etag baseline against a fresh cloud folder listing.

    Extracted from :func:`detect_external_mods` (Task 10) so the protected
    cloud mode apply engine (``services.cloud_staging.apply``) can reuse the
    same divergence logic scoped to only the paths a staged diff actually
    touches — spec §7. ``detect_external_mods`` above keeps its original
    signature/behavior and delegates here with ``scope_paths=None`` (whole
    project-folder comparison), so Mode A's accept flow is unaffected.

    Returns a list of ``{path, kind}`` dicts, ``kind`` one of
    ``etag_mismatch`` / ``missing_at_cloud`` / ``unexpected_at_cloud``. Empty
    list means clean (safe to apply/accept). A ``CloudBackendError`` from the
    listing call is swallowed the same way the caller-facing wrapper always
    has — the write path that follows will fail loudly with the real reason.
    """
    try:
        live_entries: list[ProjectFolderEntry] = await backend.list_project_folder(
            handle
        )
    except CloudBackendError:
        if strict:
            raise
        # Same logic as above — apply will fail with the real reason.
        return []
    live_map = {e.path: e.etag for e in live_entries if not e.is_dir}
    if scope_paths is not None:
        live_map = {p: t for p, t in live_map.items() if p in scope_paths}
        baseline_entries = {
            p: t for p, t in baseline_entries.items() if p in scope_paths
        }
    diverged: list[dict[str, str]] = []
    # 1. Every baseline path that's now missing OR mismatched.
    for path, baseline_etag in baseline_entries.items():
        live_etag = live_map.get(path)
        if live_etag is None:
            diverged.append({"path": path, "kind": "missing_at_cloud"})
        elif live_etag != baseline_etag:
            diverged.append({"path": path, "kind": "etag_mismatch"})
    # 2. Paths that appeared in the cloud since seed.
    for path in live_map.keys():
        if path not in baseline_entries:
            diverged.append({"path": path, "kind": "unexpected_at_cloud"})
    return diverged


async def apply_diff_to_cloud(
    *,
    job: dict[str, Any],
    project: dict[str, Any],
    gitea_client: Any,
    main_cloud_router: MainCloudRouter,
) -> dict[str, Any]:
    """Walk the Mode A diff and write each change back to the cloud.

    For each file in the diff under ``projects/<slug>/``:

    * ``added`` / ``modified``: GET ``new_content`` from Gitea at HEAD
      and PUT to the cloud folder (creating parents as needed).
    * ``deleted``: DELETE from the cloud folder (``if_exists=True``).

    Returns ``{applied: int, deleted: int, errors: [...]}`` — partial
    failures don't abort the walk so the user can see which files made
    it across and which didn't. v2 may want to be transactional, but
    v1 fail-soft matches the "best-effort apply" semantics of WebDAV.
    """
    job_id = str(job["id"])
    job_short = job_id[:8]
    repo_name = job.get("repo_name")
    branch = job.get("branch_name") or "main"
    slug = project_folder_slug(job, project)
    if not repo_name:
        return {"applied": 0, "deleted": 0, "errors": ["job has no repo_name"]}

    handle_db = project.get("main_cloud_folder_handle")
    backend_id = project.get("main_cloud_backend")
    if not handle_db or not backend_id:
        return {
            "applied": 0,
            "deleted": 0,
            "errors": ["project has no cloud folder"],
        }
    try:
        backend = main_cloud_router.for_backend(backend_id)
    except Exception as e:
        return {"applied": 0, "deleted": 0, "errors": [f"backend unavailable: {e}"]}
    if not getattr(backend, "is_initialized", False):
        return {"applied": 0, "deleted": 0, "errors": ["backend not initialized"]}

    handle = ProjectFolderHandle.from_db(str(handle_db), backend=backend_id)
    baseline = job.get("cloud_diff_baseline_commit")
    if not baseline:
        return {"applied": 0, "deleted": 0, "errors": ["job has no baseline commit"]}
    head = await _read_head_commit(gitea_client, repo_name, branch)
    if not head:
        return {"applied": 0, "deleted": 0, "errors": ["could not read HEAD"]}

    files = await _diff_files_by_tree(
        gitea_client=gitea_client,
        repo_name=repo_name,
        baseline=baseline,
        head=head,
    )
    if files is None:
        return {
            "applied": 0,
            "deleted": 0,
            "errors": ["could not read git tree at baseline or head"],
        }
    applied = 0
    deleted = 0
    errors: list[str] = []
    for entry in files:
        gitea_path = entry["path"]
        status = entry["status"]
        rel = _strip_project_prefix(gitea_path, slug)
        if rel is None:
            # Skip paths outside the slug — shouldn't happen but log.
            logger.info(
                "Mode A: job %s — skipping diff path outside slug %r: %s",
                job_short,
                slug,
                gitea_path,
            )
            continue
        try:
            if status == "deleted":
                await backend.delete_project_folder_file(
                    handle, path=rel, if_exists=True
                )
                deleted += 1
            else:
                # Added or modified — preserve the exact Gitea bytes. Loop
                # deliverables commonly include PDFs, images, or office files;
                # decoding those as UTF-8 would turn a valid artifact into a
                # partial-write review.
                new_content = await gitea_client.get_file_bytes(
                    repo_name, gitea_path, ref=head
                )
                if new_content is None:
                    errors.append(f"{gitea_path}: file missing in Gitea HEAD")
                    continue
                await backend.put_project_folder_file_bytes(
                    handle, path=rel, content=new_content
                )
                applied += 1
        except CloudBackendError as e:
            errors.append(f"{gitea_path}: {e}")
        except Exception as e:
            errors.append(f"{gitea_path}: {e}")
    logger.info(
        "Mode A: job %s — applied %d, deleted %d, errors=%d",
        job_short,
        applied,
        deleted,
        len(errors),
    )
    return {"applied": applied, "deleted": deleted, "errors": errors}
