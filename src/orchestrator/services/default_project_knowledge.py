"""Every approved user's default project has a knowledge vault.

A bare ``kb_write`` (no ``kb=``) lands in the caller's default project, so a
default project without a vault refuses every note with ``no-repo``. Only the
admin ``create_user`` path used to provision one; a user admitted by first
OIDC login or by app-side approval — the bootstrap admin included — got none,
so on a fresh install nobody could write a note (vault issue
``self_registered_users_personal_project_has_no_vault``).

Two entry points share one idempotent, best-effort ensure,
:func:`ensure_default_project_knowledge`:

* the admission paths (first login, the legacy-role write-through, app-side
  approval, the OAuth bridge's JIT user) call
  :func:`schedule_default_project_knowledge`, which runs the ensure in the
  background and never raises — a login is never failed or slowed by it;
* :func:`default_project_knowledge_backfill_loop`, leader-gated, runs one
  :func:`backfill_default_project_knowledge` pass per leadership tenure once
  the forge answers, then parks until shutdown.

The ensure is a no-op for a project that already has a vault (a
``knowledge``- or legacy ``jobs``-role repository, the same rule
``kb_reindex.resolve_kb_repo`` resolves). It skips, and logs, a project whose
``knowledge_index`` still holds active pathless user notes: once a vault
exists, the first reindex sweep's ``reconcile_orphans`` archives every such
row whose slug is not in the (new, empty) tree. Those notes need an
operator's decision, not an automatic vault. The connector notes the
knowledge projection writes (``datasource``, pathless by design) are not user
notes and never block.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.project_provisioning import (
    ProjectProvisioningDependencies,
    fire_background_repair,
    provision_project_knowledge_repo,
)

#: What one ensure did. ``legacy_rows`` and ``failed`` are the ones an operator
#: may need to look at; every other outcome is final or not applicable.
OUTCOMES: tuple[str, ...] = (
    "provisioned",
    "present",
    "legacy_rows",
    "no_forge",
    "no_project",
    "not_approved",
    "failed",
)

#: How long the backfill waits before asking an unready forge again, or before
#: retrying a pass that could not even list its candidates.
BACKFILL_RETRY_SECONDS = 60.0

# One key space for the per-project lock and the background-repair throttle,
# apart from the cloud heal's (which is keyed on the bare project id).
_KEY_PREFIX = "default-project-knowledge:"


async def project_has_knowledge_vault(store: Any, project_id: str) -> bool:
    """Whether the project already has a repository that hosts its vault.

    The roles are ``kb_reindex``'s own, so this agrees with ``resolve_kb_repo``
    (and the sweep) on what counts as a vault: a project whose notes still live
    in its legacy ``jobs`` repository has one, and a new ``knowledge`` repository
    would take precedence over it and hide those notes.
    """
    from orchestrator.services.kb_reindex import _KB_REPO_ROLES

    for role in _KB_REPO_ROLES:
        if await store.get_project_repositories(project_id, role=role):
            return True
    return False


async def project_has_unadopted_legacy_rows(vector_db: Any, project_id: str) -> bool:
    """Whether the project's index holds user notes a new vault would archive.

    ``KnowledgeStore.reconcile_orphans``' predicate, without its tree and
    grace: active rows with no ``path``, keyed on ``project_id`` because an
    un-adopted row has no ``kb_id``. Rows of a generated type are not user
    notes and never block: every project a connector reaches holds a pathless
    ``datasource`` note by design. Served by the ``(project_id, status)``
    index.
    """
    from shared.runtime.services.knowledge_store import (
        GENERATED_PATHLESS_NOTE_TYPES,
    )

    return bool(
        await vector_db.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM knowledge_index
                 WHERE project_id = $1
                   AND path IS NULL
                   AND status = 'active'
                   AND note_type <> ALL($2::text[])
            )
            """,
            UUID(str(project_id)),
            list(GENERATED_PATHLESS_NOTE_TYPES),
        )
    )


async def ensure_default_project_knowledge(
    user: Mapping[str, Any],
    *,
    dependencies: ProjectProvisioningDependencies,
) -> str:
    """Give an approved user's default project its knowledge vault, once.

    Returns one of :data:`OUTCOMES`; never raises (cancellation excepted). The
    per-project lock keeps two admission events, or an admission and the
    backfill, from both creating; across replicas the repository's durable
    creation intent refuses the second.
    """
    log = dependencies.logger
    user_id = user.get("id")
    project_id = user.get("default_project_id")
    if not project_id:
        return "no_project"
    if not user.get("is_approved"):
        return "not_approved"
    project_id = str(project_id)
    if not dependencies.forge.is_initialized:
        return "no_forge"
    lock = dependencies.repair.heal_locks.setdefault(
        _KEY_PREFIX + project_id, asyncio.Lock()
    )
    try:
        async with lock:
            if await project_has_knowledge_vault(dependencies.store, project_id):
                return "present"
            if await project_has_unadopted_legacy_rows(
                dependencies.knowledge_index.vector_db, project_id
            ):
                log.warning(
                    "Default project %s (user %s) has no knowledge vault, but its "
                    "index holds active notes with no file; not provisioning one "
                    "automatically, because the first reindex would archive "
                    "them. Attach or provision its vault deliberately.",
                    project_id,
                    user_id,
                )
                return "legacy_rows"
            project = await dependencies.store.get_project(project_id)
            if not project:
                return "no_project"
            datasource = await provision_project_knowledge_repo(
                project,
                str(user_id) if user_id else None,
                dependencies=dependencies,
            )
            return "provisioned" if datasource else "failed"
    except Exception as exc:
        log.warning(
            "Knowledge vault for default project %s (user %s) not ensured: %s",
            project_id,
            user_id,
            exc,
        )
        return "failed"


def schedule_default_project_knowledge(
    user: Mapping[str, Any],
    *,
    dependencies: ProjectProvisioningDependencies,
) -> bool:
    """Run :func:`ensure_default_project_knowledge` in the background.

    For the admission paths: returns at once and never raises. The task is the
    application's (``ProjectRepairState`` holds it and drains it at shutdown)
    and throttled per project like the other background repairs; whatever it
    misses, the startup backfill retries. Returns whether a task was started.
    """
    coro = None
    try:
        project_id = user.get("default_project_id")
        if not project_id or not user.get("is_approved"):
            return False
        coro = ensure_default_project_knowledge(dict(user), dependencies=dependencies)
        return fire_background_repair(
            _KEY_PREFIX + str(project_id), coro, dependencies=dependencies
        )
    except Exception as exc:
        if coro is not None:
            coro.close()
        dependencies.logger.warning(
            "Knowledge vault for the default project of user %s not scheduled: %s",
            user.get("id"),
            exc,
        )
        return False


async def _default_projects_without_vault(store: Any) -> list[dict[str, Any]]:
    """Approved users whose default project has no vault repository.

    One row per project (its oldest user). Starting from ``users`` is what
    leaves out a default project nobody points at any more; the role filter is
    :func:`project_has_knowledge_vault`'s, which the ensure re-checks under its
    lock.
    """
    from orchestrator.services.kb_reindex import _KB_REPO_ROLES

    rows = await store.fetch(
        """
        SELECT DISTINCT ON (u.default_project_id)
               u.id, u.email, u.default_project_id, u.is_approved
          FROM users AS u
          JOIN projects AS p ON p.id = u.default_project_id
         WHERE u.is_approved
           AND NOT EXISTS (
                 SELECT 1
                   FROM project_repositories AS pr
                  WHERE pr.project_id = u.default_project_id
                    AND pr.role = ANY($1::text[])
           )
         ORDER BY u.default_project_id, u.created_at ASC
        """,
        list(_KB_REPO_ROLES),
    )
    return [dict(row) for row in rows]


async def backfill_default_project_knowledge(
    *,
    dependencies: ProjectProvisioningDependencies,
    stop: asyncio.Event | None = None,
) -> dict[str, int]:
    """One pass: every approved user's vault-less default project gets one.

    Idempotent: a second pass finds nothing to do but the projects it skipped.
    Stops between projects once ``stop`` is set. Logs one count line, a warning
    when a project was skipped for legacy rows or failed. Raises only when the
    candidates cannot be listed.
    """
    counts = dict.fromkeys(OUTCOMES, 0)
    for user in await _default_projects_without_vault(dependencies.store):
        if stop is not None and stop.is_set():
            break
        outcome = await ensure_default_project_knowledge(
            user, dependencies=dependencies
        )
        counts[outcome] += 1
    log = dependencies.logger
    (log.warning if counts["legacy_rows"] or counts["failed"] else log.info)(
        "Default project knowledge vaults: %s", counts
    )
    return counts


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def default_project_knowledge_backfill_loop(
    stop: asyncio.Event,
    *,
    dependencies: ProjectProvisioningDependencies,
    retry_seconds: float = BACKFILL_RETRY_SECONDS,
) -> None:
    """Leader-gated one-shot: one backfill pass once the forge answers.

    Started through ``run_when_leader``, so the pass runs once per leadership
    tenure, on one replica. It waits for an unready forge (Gitea may still be
    starting) and retries a pass that could not list its candidates, then
    parks until ``stop`` instead of returning: ``run_when_leader`` re-creates a
    loop that returns on its next poll (R1.B12).
    """
    log = dependencies.logger
    forge = dependencies.forge
    warned_unready = False
    while not stop.is_set():
        if not forge.is_initialized and getattr(forge, "is_configured", False):
            try:
                await forge.ensure_initialized()
            except Exception as exc:
                log.debug("Forge initialisation for the vault backfill failed: %s", exc)
        if forge.is_initialized:
            try:
                await backfill_default_project_knowledge(
                    dependencies=dependencies, stop=stop
                )
                break
            except Exception:
                log.exception(
                    "Default project knowledge backfill failed; retrying in %ss",
                    retry_seconds,
                )
        elif not getattr(forge, "is_configured", False):
            log.info("Default project knowledge backfill: no forge, nothing to do")
            break
        elif not warned_unready:
            warned_unready = True
            log.warning(
                "Default project knowledge backfill waits for the forge; "
                "retrying every %ss",
                retry_seconds,
            )
        await _wait(stop, retry_seconds)
    await stop.wait()


__all__ = [
    "BACKFILL_RETRY_SECONDS",
    "OUTCOMES",
    "backfill_default_project_knowledge",
    "default_project_knowledge_backfill_loop",
    "ensure_default_project_knowledge",
    "project_has_knowledge_vault",
    "project_has_unadopted_legacy_rows",
    "schedule_default_project_knowledge",
]
