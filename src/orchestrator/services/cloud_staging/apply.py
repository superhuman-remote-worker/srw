"""Protected cloud mode (Slice C, Task 10) — apply / reject engine.

The security-critical write path for the whole feature: the ONLY place a
staged agent diff (fuse-overlayfs upperdir, captured by
``services.cloud_staging.stage``) reaches the user's real cloud folder, and
strictly on explicit user action (owner-only endpoints in ``main.py``).

Both flows share the same epoch pin (spec §7): the caller passes the
``epoch`` it last observed via the summary endpoint (Task 8), and a stale
epoch — someone else applied/rejected/restaged since — is a hard 409 before
any read or write happens. That pin is checked twice: once against the DB
row (``_load_pinned_mount_row``, before any S3 read) and again against the
S3 manifest's own recorded ``epoch`` once ``summary()`` returns (post-review
hardening, 2026-07-12) — a concurrent turn-end stage can overwrite the S3
manifest/tar pair (and bump the DB row's epoch) in the gap between those two
reads, and without the second check apply would silently apply a diff that
no longer matches the epoch it validated against. Apply additionally
re-checks the cloud folder against the mount's etag baseline, scoped to only
the paths the staged diff touches (``detect_external_mods_against_baseline``,
``services.job_cloud_baseline``); any divergence is a hard 409 with no force
flag — the user must resolve manually (restage or investigate) and retry.

Apply writes deletes before creates (whiteout-before-create) and is
sequential/fail-soft: each write is attempted independently, errors are
collected, and a partial failure returns them without touching staging state
at all — the retry is safe because every write here is idempotent (PUT
overwrite, DELETE ``if_exists=True``). Only full success re-captures the
etag baseline, deletes the staged S3 blobs, and advances the staging epoch
(clearing ``staged_summary``).

Reject never touches the cloud — it just resets the agent-side overlay
(best-effort), drops the staged S3 blobs, and advances the epoch the same
way. No baseline re-capture (nothing changed on the cloud side).

Known v1 limitation: apply/reject resets the *live* agent's overlay via an
HTTP call to the AGENT pod's app — ``reset_agent_overlay`` resolves the
bound agent's ``pod_ip``/``pod_port`` via ``get_agent`` (see
``main._reset_thread_overlay``); the agent app then tears down the overlay
it manages. The call is a no-op when that pod is dead — normal, not fatal.
If the pod is dead at apply/reject time, the overlay inside the
*last workspace snapshot* still carries the old upperdir; a later resume
restores that snapshot, and the next turn-end stage push re-stages the
already-applied (or already-rejected) content against the fresh baseline —
surfacing as modified-with-identical-content entries the user re-rejects.
Accepted for v1 (spec §7); a v2 fix would need the resume path itself to
clear the upperdir before the workspace comes back up.

Known v1 limitation: full-success tail-crash window. The cloud writes
(deletes/creates) commit before the baseline is re-captured and staging is
cleared (see the ordering comment on the full-success tail below). If
``capture_etag_baseline``/``update_ro_mount_baseline`` raises or returns
False after the writes already landed (backend blip mid-tail), the row is
left with the OLD (pre-apply) baseline and the OLD ``staged_summary``/
``staged_epoch``, even though the cloud already reflects the new content.
**Restage alone does not clear this wedge**: ``reset_agent_overlay`` already
ran (it's first in the tail) and succeeded, so the upperdir is empty and a
restage diffs against cloud content that now matches what was just
applied — it stages an *empty* diff and clears ``staged_summary``, but it
never touches ``etag_baseline``, so the stale baseline persists and keeps
false-409ing (``external_modifications_detected``) against any future real
diff. The correct recovery is to END the thread and RESUME it: re-engage
(``services.cloud.ro_engage.engage_ro_mount``) re-captures the baseline from
the live cloud content whenever the row's ``staged_summary`` is ``None`` —
its guard only *skips* recapture while a staging is still live, precisely
so it never clobbers the baseline a pending review is classifying against
(see that module's docstring). Accepted for v1; a v2 fix would retry the
baseline recapture until it succeeds as part of the tail itself, or expose
an explicit "repair" endpoint, instead of requiring an end/resume
round-trip.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from orchestrator.services.cloud import protected_provider
from orchestrator.services.diff_source import UpperdirDiffSource
from orchestrator.services.job_cloud_baseline import (
    detect_external_mods_against_baseline,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.cloud_staging.stage import (
    staging_manifest_key,
    staging_tar_key,
)

logger = logging.getLogger(__name__)


class StagedApplyError(Exception):
    """Carries an HTTP status + JSON detail body; endpoints map it verbatim."""

    def __init__(self, status_code: int, detail: dict[str, Any]) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


async def _load_pinned_mount_row(
    *, thread_id: str, epoch: int, postgres_db: Any
) -> dict[str, Any]:
    """Steps shared by apply and reject: load the mount row, then the two
    gates that must run before ANY read or write — "nothing staged" and the
    epoch pin. Raises :class:`StagedApplyError` on either gate.
    """
    row = await postgres_db.get_ro_mount_by_thread(thread_id)
    if not row or row.get("staged_summary") is None:
        raise StagedApplyError(409, {"code": "nothing_staged"})
    if epoch != row["staged_epoch"]:
        raise StagedApplyError(
            409, {"code": "epoch_stale", "staged_epoch": row["staged_epoch"]}
        )
    return row


async def apply_staged_diff(
    *,
    thread_id: str,
    epoch: int,
    postgres_db: Any,
    main_cloud_router: Any,
    snapshot_service: Any,
    reset_agent_overlay: Callable[[], Awaitable[bool]],
) -> dict[str, Any]:
    """Write the staged diff to the cloud, whole-diff, epoch-pinned.

    Returns ``{"applied", "deleted", "errors": [...]}`` on partial failure
    (staging state left untouched — retry is safe) or ``{"applied",
    "deleted", "errors": [], "epoch", "overlay_reset"}`` on full success.
    Raises :class:`StagedApplyError` for every gate short-circuit (409/410).
    """
    row = await _load_pinned_mount_row(
        thread_id=thread_id, epoch=epoch, postgres_db=postgres_db
    )

    source = ProtectedMountSourceIdentity.from_binding(
        row.get("source_binding"),
        expected_sha256=str(row.get("source_binding_sha256") or ""),
    )
    staged_summary = row.get("staged_summary")
    if (
        source is None
        or not isinstance(staged_summary, dict)
        or not (
            staged_summary.get("source_binding") == source.binding
            and staged_summary.get("source_binding_sha256") == source.sha256
        )
    ):
        raise StagedApplyError(409, {"code": "staged_source_invalid"})
    expected_backend_id: str | None = None
    try:
        # A source whose provider offers no protected level refuses like an
        # unavailable installation (409), never as an unhandled error.
        expected_backend_id = protected_provider(source.backend)
        backend = main_cloud_router.for_backend_instance(
            source.backend_instance_id,
            expected_backend_id=expected_backend_id,
        )
    except Exception:
        authority = (
            await postgres_db.get_main_cloud_backend_instance(
                source.backend_instance_id,
                expected_backend_id=expected_backend_id,
            )
            if expected_backend_id is not None
            else None
        )
        if authority is None:
            raise StagedApplyError(
                409, {"code": "staged_backend_unavailable"}
            ) from None
        backend = await main_cloud_router.resolve_backend_instance(authority)
    handle = source.to_project_folder_handle()

    src = UpperdirDiffSource(
        thread_id=thread_id,
        mount_row=row,
        backend=backend,
        handle=handle,
        snapshot_service=snapshot_service,
    )
    summary = await src.summary()
    if summary is None:
        # DB says staged but the manifest blob is gone (or unparseable). The
        # user restages. NOTE: since summary() reads only the manifest
        # (post-review hardening — see UpperdirDiffSource docstring), a torn
        # tar/manifest content-binding mismatch does NOT surface here
        # anymore; the ``ensure_tar_bound()`` probe below is what catches
        # that, before any write.
        raise StagedApplyError(410, {"code": "staging_missing"})
    if not (
        summary.meta.get("source_binding") == source.binding
        and summary.meta.get("source_binding_sha256") == source.sha256
    ):
        raise StagedApplyError(409, {"code": "staged_source_invalid"})

    # Second epoch pin: ``_load_pinned_mount_row`` above validated the
    # caller's epoch against the DB row BEFORE any S3 read. A concurrent
    # turn-end stage can overwrite the S3 manifest/tar pair (and bump the
    # row's epoch) in the gap between that DB read and this S3 read — so
    # re-validate the manifest's own recorded epoch against the same row
    # snapshot. A mismatch here means the summary we just read no longer
    # corresponds to the epoch we're pinned to; the caller must refetch.
    if summary.meta.get("epoch") != row["staged_epoch"]:
        raise StagedApplyError(
            409, {"code": "epoch_stale", "staged_epoch": row["staged_epoch"]}
        )

    touched_paths = {f.path for f in summary.files}
    diverged = await detect_external_mods_against_baseline(
        baseline_entries=row.get("etag_baseline") or {},
        backend=backend,
        handle=handle,
        scope_paths=touched_paths,
    )
    if diverged:
        raise StagedApplyError(
            409,
            {
                "code": "external_modifications_detected",
                "message": (
                    "Cloud folder was modified externally since staging. "
                    "Resolve manually before applying."
                ),
                "diverged": diverged,
            },
        )

    # Force tar materialization + content-binding verification NOW, before
    # any write. summary() (above) is manifest-only and can't catch a torn
    # tar/manifest pair (post-review hardening — see UpperdirDiffSource
    # docstring); without this probe a torn tar would only be discovered
    # per-file, mid-sequence, via raw_new_bytes() returning None during the
    # create loop — AFTER the delete loop (which runs first) had already
    # committed. This preserves "a torn staging writes NOTHING."
    if not await src.ensure_tar_bound():
        raise StagedApplyError(410, {"code": "staging_missing"})

    # Deletes first (whiteout-before-create), children before parents —
    # harmless ordering for plain files, correct if a future backend ever
    # deletes directories. Sequential + fail-soft: every write is
    # independently idempotent, so collecting errors and returning them
    # (instead of aborting) lets the caller retry safely.
    deletes = sorted(
        (f.path for f in summary.files if f.status == "deleted"), reverse=True
    )
    creates = [f.path for f in summary.files if f.status != "deleted"]

    applied = 0
    deleted = 0
    errors: list[str] = []
    for path in deletes:
        try:
            await backend.delete_project_folder_file(handle, path=path, if_exists=True)
            deleted += 1
        except Exception as e:  # noqa: BLE001 - fail-soft by design
            errors.append(f"{path}: {e}")
    for path in creates:
        try:
            raw = await src.raw_new_bytes(path)
            if raw is None:
                errors.append(f"{path}: staged content missing from upperdir tar")
                continue
            await backend.put_project_folder_file_bytes(handle, path=path, content=raw)
            applied += 1
        except Exception as e:  # noqa: BLE001 - fail-soft by design
            errors.append(f"{path}: {e}")

    if errors:
        # Partial failure: leave staging state exactly as it was. Every
        # write above is idempotent, so a retry (same epoch, same staged
        # content) is always safe.
        logger.warning(
            "apply: thread %s partial failure (%d applied, %d deleted, %d errors)",
            thread_id,
            applied,
            deleted,
            len(errors),
        )
        return {"applied": applied, "deleted": deleted, "errors": errors}

    # Full success. Order matters: reset the live agent overlay first (it's
    # best-effort and independent of the rest), then re-derive the new
    # baseline from what's now actually on the cloud, persist it, drop the
    # staged blobs, and only then clear the staging row — so a crash
    # mid-sequence never leaves staging cleared without a fresh baseline.
    #
    # require_active=False: this bookkeeping must still run on a REVOKED
    # row. The idle-drain reconciler revokes an ended thread's
    # cloud_ro_mounts row within ~15min regardless of a pending review
    # (reads already tolerate a revoked row — Task 8), so a user reviewing
    # an ended thread can still apply; without this the UPDATE would
    # silently no-op (WHERE status='active' matches nothing) and leave
    # staging/baseline stuck even though the cloud writes above already
    # landed.
    overlay_reset = await reset_agent_overlay()
    new_baseline = await backend.capture_etag_baseline(handle)
    if not await postgres_db.update_ro_mount_baseline(
        row["id"],
        new_baseline,
        require_active=False,
        expected_engage_attempt=str(row.get("engage_attempt") or ""),
        expected_source_binding_sha256=source.sha256,
        expected_staged_epoch=row["staged_epoch"],
    ):
        logger.warning(
            "apply: thread %s baseline update matched no row (id=%s) — "
            "row may have been deleted out from under us",
            thread_id,
            row["id"],
        )
        return {
            "applied": applied,
            "deleted": deleted,
            "errors": ["staging authority changed before baseline publication"],
        }
    await snapshot_service.delete_blob(
        staging_tar_key(thread_id, row.get("staged_summary"))
    )
    await snapshot_service.delete_blob(
        staging_manifest_key(thread_id, row.get("staged_summary"))
    )
    new_epoch = row["staged_epoch"] + 1
    if not await postgres_db.update_ro_mount_staging(
        row["id"],
        staged_epoch=new_epoch,
        staged_summary=None,
        require_active=False,
        expected_engage_attempt=str(row.get("engage_attempt") or ""),
        expected_source_binding_sha256=source.sha256,
    ):
        logger.warning(
            "apply: thread %s staging update matched no row (id=%s) — "
            "row may have been deleted out from under us",
            thread_id,
            row["id"],
        )
        return {
            "applied": applied,
            "deleted": deleted,
            "errors": ["staging authority changed before review settlement"],
        }
    logger.info(
        "apply: thread %s applied %d, deleted %d, epoch -> %d (overlay_reset=%s)",
        thread_id,
        applied,
        deleted,
        new_epoch,
        overlay_reset,
    )
    return {
        "applied": applied,
        "deleted": deleted,
        "errors": [],
        "epoch": new_epoch,
        "overlay_reset": overlay_reset,
    }


async def reject_staged_diff(
    *,
    thread_id: str,
    epoch: int,
    postgres_db: Any,
    snapshot_service: Any,
    reset_agent_overlay: Callable[[], Awaitable[bool]],
) -> dict[str, Any]:
    """Discard the staged diff without ever touching the cloud.

    Same pins as apply (nothing-staged / epoch-stale), then: reset the agent
    overlay (best-effort), drop both staged S3 blobs, and advance the
    staging epoch. No baseline re-capture — the cloud folder never changed.
    """
    row = await _load_pinned_mount_row(
        thread_id=thread_id, epoch=epoch, postgres_db=postgres_db
    )

    overlay_reset = await reset_agent_overlay()
    new_epoch = row["staged_epoch"] + 1
    # require_active=False: same rationale as apply's bookkeeping — the
    # idle-drain reconciler can revoke this row before the user reviews an
    # ended thread's staged diff; reject must still be able to clear it.
    source = ProtectedMountSourceIdentity.from_binding(
        row.get("source_binding"),
        expected_sha256=str(row.get("source_binding_sha256") or ""),
    )
    exact_kwargs = (
        {
            "expected_engage_attempt": str(row.get("engage_attempt") or ""),
            "expected_source_binding_sha256": source.sha256,
        }
        if source is not None
        else {}
    )
    if not await postgres_db.update_ro_mount_staging(
        row["id"],
        staged_epoch=new_epoch,
        staged_summary=None,
        require_active=False,
        **exact_kwargs,
    ):
        logger.warning(
            "reject: thread %s staging update matched no row (id=%s) — "
            "row may have been deleted out from under us",
            thread_id,
            row["id"],
        )
        raise StagedApplyError(409, {"code": "staging_authority_changed"})
    await snapshot_service.delete_blob(
        staging_tar_key(thread_id, row.get("staged_summary"))
    )
    await snapshot_service.delete_blob(
        staging_manifest_key(thread_id, row.get("staged_summary"))
    )
    logger.info(
        "reject: thread %s rejected staged diff, epoch -> %d (overlay_reset=%s)",
        thread_id,
        new_epoch,
        overlay_reset,
    )
    return {"rejected": True, "epoch": new_epoch, "overlay_reset": overlay_reset}
