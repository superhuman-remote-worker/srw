"""Decide a project delete once, then finish it whatever stops the request.

A delete has a decision point. :func:`begin_project_deletion` runs in one
transaction: the catalog lock, the project row ``FOR UPDATE``, the refusal
checks, the Project resource version when the manifest API asks, and then
the mark (``projects.deletion_requested_at``) plus a ``project_deletions``
row. Nothing outside the database has been touched when it commits.

From then on the project refuses new work and changes (the
``project_deletion_in_progress`` triggers, and :data:`PROJECT_DELETING_DETAIL`
at the request gates), so the row delete cannot be refused after the vault
is gone. :func:`drive_project_deletion` then removes, in order, the managed
repositories, the folder grants and the row; the row transaction records
what is left outside the database in the ``project_deletions`` row, and the
steps after the row (search index, Keycloak group, main-cloud folder) run
until each has succeeded. The request drives it first; the leader-gated
:func:`project_deletion_sweep_loop` takes over whatever it left.

Design: knowledge-base/knowledge/issues/manifest_project_delete_skips_external_cleanup.md
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, NamedTuple
from uuid import UUID

import asyncpg
from fastapi import HTTPException

from orchestrator.services import knowledge_graph_cleanup
from orchestrator.services.cloud import ProjectFolderHandle, folder_grants
from orchestrator.services.kb_reindex import kb_index_lock
from orchestrator.services.managed_repository_authority import (
    revoke_and_delete_managed_repository,
)
from orchestrator.services.manifest_execution_retirement import (
    lock_manifest_execution_catalog,
)
from orchestrator.services.manifest_retirement import assert_project_retirable
from orchestrator.services.project_status import (
    PROJECT_DELETING_DETAIL as PROJECT_DELETING_DETAIL,
    project_is_deleting as project_is_deleting,
)

logger = logging.getLogger(__name__)

#: The fence trigger's constraint name (migration 0565).
PROJECT_DELETION_CONSTRAINT = "project_deletion_in_progress"

#: The repository fence of migration 0176: an owner row cannot go while its
#: managed repository is not contained.
REPOSITORY_CLEANUP_CONSTRAINT = "managed_repository_cleanup_required"

#: The external state the row transaction records and the steps remove.
STEPS_AFTER_ROW = ("index", "keycloak_group", "cloud_folder")

#: How long a claim keeps the sweeper off a deletion. A drive's external calls
#: each have their own timeouts, far below this.
LEASE_SECONDS = 600

#: Backoff after a failed pass: 30 s, doubling, at most an hour.
FIRST_RETRY_SECONDS = 30
MAX_RETRY_SECONDS = 3600

SWEEP_SECONDS = 30.0
SWEEP_BATCH = 20
RETENTION_DAYS = 30

DELETED = "deleted"
DELETING = "deleting"


class DeletionStepFailed(Exception):
    """A step that a later pass may finish; the message is recorded."""


class DeletionOutcome(NamedTuple):
    #: :data:`DELETED` once the project row is gone, else :data:`DELETING`.
    status: str
    #: Why this pass stopped short, when it did.
    error: str | None = None


def retry_delay_seconds(attempts: int) -> int:
    """The wait after the ``attempts``-th failed pass."""
    return min(FIRST_RETRY_SECONDS * 2 ** max(attempts - 1, 0), MAX_RETRY_SECONDS)


# =============================================================================
# The decision and the records (run on the application database)
# =============================================================================


async def begin_project_deletion(
    conn,
    project_id: UUID,
    *,
    requested_by: str | None = None,
    expected_resource: tuple[str, int] | None = None,
) -> dict | None:
    """Decide the delete in the caller's transaction.

    Returns the new ``project_deletions`` row when this call decided it; the
    caller then holds its lease and drives the teardown. Returns None when an
    earlier decision stands: nothing is checked again, because the fence has
    admitted nothing since. Raises 404 (no project), 400 (a default project)
    or 409 (the refusal checks, or ``expected_resource``'s version is not
    current).
    """
    await lock_manifest_execution_catalog(conn)
    # Locked before the refusal checks: a writer that took the project row
    # FOR SHARE (the fence triggers) commits first and its work is seen
    # below, or waits and then reads the mark.
    row = await conn.fetchrow(
        "SELECT is_default, deletion_requested_at FROM projects WHERE id=$1 FOR UPDATE",
        project_id,
    )
    if row is None:
        raise HTTPException(404, f"Project '{project_id}' not found")
    if row["deletion_requested_at"] is not None:
        return None
    if row["is_default"]:
        raise HTTPException(400, "Cannot delete a default project")
    await assert_project_retirable(conn, project_id)
    if expected_resource is not None:
        uid, version = expected_resource
        current = await conn.fetchval(
            """SELECT resource_version FROM srw_resources
            WHERE id=$1 AND kind='Project' AND linked_id=$2 AND deleted_at IS NULL""",
            UUID(str(uid)),
            project_id,
        )
        if current is None or current != version:
            raise HTTPException(
                409, "Resource version changed; read the current resource and retry."
            )
    await conn.execute(
        "UPDATE projects SET deletion_requested_at=now() WHERE id=$1", project_id
    )
    return _deletion(
        await conn.fetchrow(
            """INSERT INTO project_deletions(project_id,requested_by,attempts,next_attempt_at)
            VALUES($1,$2,1,now()+make_interval(secs => $3)) RETURNING *""",
            project_id,
            UUID(str(requested_by)) if requested_by else None,
            LEASE_SECONDS,
        )
    )


async def claim_project_deletion(conn, project_id: UUID) -> dict | None:
    """Take the lease on an unfinished, due deletion, or return None."""
    row = await conn.fetchrow(
        """UPDATE project_deletions
        SET attempts=attempts+1, next_attempt_at=now()+make_interval(secs => $2)
        WHERE project_id=$1 AND completed_at IS NULL AND next_attempt_at<=now()
        RETURNING *""",
        project_id,
        LEASE_SECONDS,
    )
    return _deletion(row)


async def get_project_deletion(conn, project_id: UUID) -> dict | None:
    return _deletion(
        await conn.fetchrow(
            "SELECT * FROM project_deletions WHERE project_id=$1", project_id
        )
    )


async def record_row_deleted(conn, project_id: UUID, project: dict) -> None:
    """In the row's transaction: what the steps after it need.

    A delete with no ``project_deletions`` row (a direct store call) records
    nothing.
    """
    targets = {
        "cloud": {
            key: (str(project[key]) if project.get(key) is not None else None)
            for key in (
                "main_cloud_backend",
                "main_cloud_backend_instance_id",
                "main_cloud_folder_handle",
                "nextcloud_folder_id",
            )
        }
    }
    await conn.execute(
        """UPDATE project_deletions
        SET row_deleted_at=now(), targets=$2::jsonb, pending_steps=$3::text[]
        WHERE project_id=$1""",
        project_id,
        json.dumps(targets),
        list(STEPS_AFTER_ROW),
    )


async def settle_project_deletion(
    conn,
    project_id: UUID,
    *,
    pending_steps: list[str] | None,
    error: str | None,
) -> None:
    """Close the deletion, or release the lease until the backoff is due.

    ``pending_steps`` None keeps the recorded steps (the pass stopped before
    the row).
    """
    await conn.execute(
        """UPDATE project_deletions SET
            pending_steps=COALESCE($2::text[], pending_steps),
            last_error=$3,
            completed_at=CASE
                WHEN $3::text IS NULL AND row_deleted_at IS NOT NULL
                     AND cardinality(COALESCE($2::text[], pending_steps))=0
                THEN now() END,
            next_attempt_at=CASE
                WHEN $3::text IS NULL THEN next_attempt_at
                ELSE now()+make_interval(secs => LEAST(
                    $4::double precision * power(2, GREATEST(attempts-1, 0)),
                    $5::double precision))
                END
        WHERE project_id=$1""",
        project_id,
        pending_steps,
        error,
        FIRST_RETRY_SECONDS,
        MAX_RETRY_SECONDS,
    )


async def due_project_deletions(conn, *, limit: int = SWEEP_BATCH) -> list[UUID]:
    rows = await conn.fetch(
        """SELECT project_id FROM project_deletions
        WHERE completed_at IS NULL AND next_attempt_at<=now()
        ORDER BY next_attempt_at LIMIT $1""",
        limit,
    )
    return [row["project_id"] for row in rows]


async def purge_completed_project_deletions(conn, *, days: int = RETENTION_DAYS) -> int:
    result = await conn.execute(
        """DELETE FROM project_deletions
        WHERE completed_at IS NOT NULL AND completed_at<now()-make_interval(days => $1)""",
        days,
    )
    return int(result.split()[-1]) if result else 0


def _deletion(row) -> dict | None:
    if row is None:
        return None
    deletion = dict(row)
    targets = deletion.get("targets")
    if isinstance(targets, str):
        deletion["targets"] = json.loads(targets)
    deletion["pending_steps"] = list(deletion.get("pending_steps") or [])
    return deletion


# =============================================================================
# The teardown
# =============================================================================


async def drive_project_deletion(
    project_id: str,
    *,
    dependencies: Any,
    claimed: dict | None = None,
) -> DeletionOutcome:
    """Run one pass of a decided delete. Its status is :data:`DELETED` once
    the project row is gone (steps after it may still be pending: a later
    pass finishes them), else :data:`DELETING`.

    ``claimed`` is the deletion row whose lease the caller already holds.
    Without it the pass claims the lease itself; when another pass holds
    it, this one does nothing.

    ``dependencies`` carries ``store``, ``vector_db``, ``forge``,
    ``keycloak_groups``, ``main_cloud_router``, ``graph`` and ``logger``
    (``services.projects.ProjectDependencies``).
    """
    store = dependencies.store
    deletion = claimed or await store.claim_project_deletion(project_id)
    if deletion is None:
        current = await store.get_project_deletion(project_id) or {}
        return DeletionOutcome(
            DELETED if current.get("row_deleted_at") else DELETING,
            current.get("last_error"),
        )

    pending: list[str] | None = None
    error: str | None = None
    try:
        if deletion.get("row_deleted_at") is None:
            await _delete_through_the_row(project_id, dependencies)
            deletion = await store.get_project_deletion(project_id) or deletion
            # Not a recorded step: the orphan sweep removes what this misses.
            await knowledge_graph_cleanup.delete_project_graph_knowledge(
                project_id, graph=dependencies.graph
            )
        pending, error = await _run_steps_after_row(
            project_id, deletion.get("targets") or {}, deletion, dependencies
        )
    except DeletionStepFailed as exc:
        error = str(exc)
    except Exception as exc:  # recorded, then retried; never lost
        logger.exception("Project %s deletion pass failed", project_id)
        error = f"{type(exc).__name__}: {exc}"

    await store.settle_project_deletion(project_id, pending_steps=pending, error=error)
    if error:
        logger.warning(
            "Project %s deletion is not finished; a later pass retries: %s",
            project_id,
            error,
        )
    row_deleted = deletion.get("row_deleted_at") is not None
    return DeletionOutcome(DELETED if row_deleted else DELETING, error)


async def _delete_through_the_row(project_id: str, dependencies: Any) -> None:
    store = dependencies.store
    # The fence makes these the project's last repositories.
    for repo in await store.get_project_repositories(project_id):
        if repo.get("is_managed"):
            if not await revoke_and_delete_managed_repository(
                store, dependencies.forge, repo["name"]
            ):
                raise DeletionStepFailed(
                    "Repository credential revocation is retryable"
                )

    # The project's per-folder principals go before its folder does (their
    # rows outlive the project; what this cannot finish, the sweep does).
    await folder_grants.revoke_folder_grants(
        store,
        dependencies.main_cloud_router,
        reason="project_deleted",
        project_id=project_id,
    )

    try:
        deleted = await store.delete_project(project_id)
    except HTTPException as exc:
        # The fence should make this impossible; record it and retry.
        raise DeletionStepFailed(f"The row delete was refused: {exc.detail}") from exc
    except asyncpg.CheckViolationError as exc:
        if getattr(exc, "constraint_name", None) == REPOSITORY_CLEANUP_CONSTRAINT:
            raise DeletionStepFailed(
                "Repository containment changed; the next pass retries"
            ) from exc
        raise
    if not deleted:
        raise DeletionStepFailed("The project row was not deleted")


async def _run_steps_after_row(
    project_id: str, targets: dict, deletion: dict, dependencies: Any
) -> tuple[list[str], str | None]:
    remaining: list[str] = []
    errors: list[str] = []
    for step in deletion.get("pending_steps") or []:
        runner = _STEP_RUNNERS.get(step)
        try:
            if runner is None:
                raise DeletionStepFailed(f"unknown step {step!r}")
            await runner(project_id, targets, dependencies)
        except Exception as exc:
            remaining.append(step)
            errors.append(f"{step}: {exc}")
    return remaining, "; ".join(errors) or None


async def _delete_index(project_id: str, _targets: dict, dependencies: Any) -> None:
    """The vault's search rows and its index watermark (``kb_id`` is the
    project id). Connector notes go with their links, by the datasource
    reconciler."""
    uuid_val = UUID(project_id)
    # After any reindex already in flight, which would otherwise write rows
    # back after this delete.
    async with kb_index_lock(dependencies.store, uuid_val, wait=True):
        async with dependencies.vector_db.acquire() as conn:
            await conn.execute(
                "DELETE FROM knowledge_index WHERE project_id = $1", uuid_val
            )
            await conn.execute(
                "DELETE FROM kb_index_watermark WHERE kb_id = $1", uuid_val
            )


async def _delete_keycloak_group(
    project_id: str, _targets: dict, dependencies: Any
) -> None:
    groups = dependencies.keycloak_groups
    if not getattr(groups, "is_configured", False):
        return  # no Keycloak administration, so no group was ever made
    if not groups.is_initialized and not await groups.ensure_initialized():
        raise DeletionStepFailed("Keycloak is unavailable")
    if not await groups.delete_project_group(project_id):
        raise DeletionStepFailed("the Keycloak group delete failed")


async def _delete_cloud_folder(
    project_id: str, targets: dict, dependencies: Any
) -> None:
    cloud = dict(targets.get("cloud") or {})
    handle = cloud.get("main_cloud_folder_handle") or cloud.get("nextcloud_folder_id")
    if not handle:
        return
    # The raising form: a delete against a guessed installation is worse
    # than a folder left for an operator.
    backend = dependencies.main_cloud_router.for_project({"id": project_id, **cloud})
    if not backend.is_initialized:
        raise DeletionStepFailed("the main-cloud backend is not initialized")
    await backend.delete_project_folder(
        ProjectFolderHandle.from_db(
            str(handle), backend=cloud.get("main_cloud_backend") or backend.backend_id
        )
    )


_STEP_RUNNERS = {
    "index": _delete_index,
    "keycloak_group": _delete_keycloak_group,
    "cloud_folder": _delete_cloud_folder,
}


# =============================================================================
# The sweeper
# =============================================================================


async def sweep_project_deletions(*, dependencies: Any) -> int:
    """One pass over every due deletion. Returns how many were driven."""
    store = dependencies.store
    driven = 0
    for project_id in await store.due_project_deletions():
        deletion = await store.claim_project_deletion(str(project_id))
        if deletion is None:
            continue  # a request took it in between
        await drive_project_deletion(
            str(project_id), dependencies=dependencies, claimed=deletion
        )
        driven += 1
    await store.purge_completed_project_deletions()
    return driven


async def project_deletion_sweep_loop(
    shutdown_event: asyncio.Event,
    *,
    dependencies: Any,
    interval_seconds: float = SWEEP_SECONDS,
) -> None:
    """Leader-gated loop: a pass when leadership starts, then every interval.

    ``dependencies`` is a zero-argument callable that builds the project
    dependencies per pass, because the application rebinds its stores.
    A failed pass is logged and the next one runs on time. Returns only on
    shutdown, because ``run_when_leader`` re-creates a loop that returns.
    """
    while not shutdown_event.is_set():
        try:
            await sweep_project_deletions(dependencies=dependencies())
        except Exception as exc:
            logger.warning(
                "Project deletion sweep failed (%s); next pass in %.0fs",
                type(exc).__name__,
                interval_seconds,
            )
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
