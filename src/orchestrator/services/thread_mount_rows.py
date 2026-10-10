"""Thread cloud mount rows, and the project scope derived from them.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). This is the
leaf of B05's session half: it answers "which projects is this thread attached
to, and what cloud surface does each one expose", and it is called from both
directions — session configuration resolution reads
:func:`thread_project_ids`, and admission writes the rows. Since main cloud
slice 3 an ordinary session mounts its attached cloud connectors instead
(``cloud_connector_mounts``); the rows still choose a protected session's
folder (until slice 5) and give a legacy multi-project session its scope.

Three properties are load-bearing and moved unchanged:

* **A missing cloud transport is a missing row, never a partial one.** Every
  resolution failure here — no owner, an owner who has never completed an SSO
  login, an unresolvable installation authority, a backend that is down —
  returns ``None`` for that project rather than a half-built row.
* **Connector authorization does not depend on mount rows.**
  :func:`thread_project_ids` answers from ``threads.project_id``, the one
  project a Session belongs to (single_project_sessions.md), so the project
  list that gates datasource access is the same with or without the thread's
  ``thread_mounts`` rows and whether or not the cloud is up. Only a legacy
  multi-project Session (a NULL column, or rows naming other projects than
  its column) still reads its list from the legacy places
  (main_cloud_as_connectors.md, slice 1).
* **``project_default`` rows count as project attachments** of such a legacy
  Session. Only sessions created before main cloud slice 4 have them: a
  default project's cloud is now its owner's own root, the user-root
  connector, and no row is written for it. The shape excluded from the
  project scope is ``repo``.

Collaborators arrive through :class:`ThreadMountDependencies`, rebuilt per
invocation by the application rather than captured at import: ``store`` and
``cloud_router`` are both rebound during ``lifespan``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

from orchestrator.services.cloud import ProjectFolderHandle

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadMountDependencies:
    """Collaborators for one mount-row resolution, resolved per invocation.

    ``store`` is main's ``postgres_db`` and ``cloud_router`` its
    ``main_cloud_router``; both are rebound during ``lifespan``, so the
    application rebuilds this dataclass per call rather than capturing it at
    import.

    ``resolve_authorized_thread_datasources`` is B06's and
    ``build_datasources_payload`` is B05's job-preparation lane; both are
    injected so this module never imports across a batch boundary it does not
    own.
    """

    store: Any
    cloud_router: Any
    externalize_gitea_url: Callable[[str], str]
    resolve_authorized_thread_datasources: Callable[..., Awaitable[Any]]
    build_datasources_payload: Callable[[Any], Any]
    build_workspace_ssh_identities: Callable[[Any], Any]
    cloud_workspace_driver: Callable[[], str]


def slugify_mount_name(name: str) -> str:
    """Workspace-safe slug for a mount's target_path."""
    out = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    return out or "project"


def project_ids_from_mounts(mounts: list[dict[str, Any]]) -> list[str]:
    """Pick out project ``source_ref``s from a list of mount rows.

    Both ``project`` (non-default, mounted under ``projects/<slug>/``) and
    ``project_default`` (a default project's, written before main cloud
    slice 4) rows contribute — the default project is still a project
    attachment for datasource resolution and visibility.
    """
    out: list[str] = []
    for m in mounts:
        if m.get("mount_kind") not in {"project", "project_default"}:
            continue
        ref = m.get("source_ref")
        if ref:
            out.append(str(ref))
    return out


def durable_project_ids(
    thread: dict[str, Any] | None,
    *,
    legacy_mounts: list[dict[str, Any]] | None = None,
) -> list[str]:
    """The project scope of ``thread``, from durable state only.

    A Session belongs to one project at most, and that project is
    ``threads.project_id`` (single_project_sessions.md): every create path
    writes it, so the column is the whole answer for every Session that has a
    project. The rest are **legacy multi-project Sessions**, created before
    that rule, whose list lives in the legacy places; they keep working
    exactly as before, so the ``project`` and ``project_default`` rows of
    ``thread_mounts`` (``legacy_mounts``) still answer for them, in their
    historical order:

    * a NULL column (two or more projects): the rows, else
      ``metadata.project_ids`` (before Phase 1 of the cloud collaboration
      model);
    * a set column with rows naming other projects too (``project_id`` plus
      ``project_ids`` before 2026-10-06): the rows and the column, so such a
      Session's scope is not narrowed to the column.

    For a Session whose rows name only its own project, or none, the rows
    change nothing: its eligibility is the same with them emptied.
    """
    if not thread:
        return []
    column = str(thread["project_id"]) if thread.get("project_id") else None
    rows = project_ids_from_mounts(legacy_mounts or [])
    if column is not None:
        if any(project != column for project in rows):
            return list(dict.fromkeys([*rows, column]))
        return [column]
    legacy = rows
    if not legacy:
        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        values = metadata.get("project_ids") if isinstance(metadata, dict) else None
        legacy = [str(value) for value in values or [] if value]
    return list(dict.fromkeys(legacy))


async def thread_project_ids(
    thread_id: str, *, dependencies: ThreadMountDependencies
) -> list[str]:
    """The project-attachment list of a thread: :func:`durable_project_ids`.

    Connector authorization reads this, so its answer never depends on the
    thread's ``thread_mounts`` rows (main_cloud_as_connectors.md, slice 1):
    a Session's connector eligibility is the same with those rows emptied.

    **Mount backfill (delivery only, transitional):** a thread whose scope
    has no mount row (created while the cloud was down, or before Phase 1)
    gets its rows built here, best effort, because workspace delivery reads
    them right after this call. The result is never read back for the scope,
    and a failure is logged and retried on the next access. It goes with
    ``thread_mounts`` when cloud folders become connectors.
    """
    store = dependencies.store
    thread = await store.get_thread(thread_id)
    if not thread:
        return []
    mounts = await store.list_thread_mounts(thread_id)
    scope = durable_project_ids(thread, legacy_mounts=mounts)
    if scope and not project_ids_from_mounts(mounts):
        try:
            rows = await build_thread_mount_rows(scope, dependencies=dependencies)
            if rows:
                await store.replace_thread_mounts(thread_id, rows)
                logger.info(
                    "Thread %s: backfilled %d thread_mounts row(s) from "
                    "durable project scope",
                    thread_id,
                    len(rows),
                )
        except Exception as e:
            logger.warning(
                "Thread %s: thread_mounts backfill failed (%s); "
                "the project scope is unaffected",
                thread_id,
                e,
            )
    return scope


def _project_folder_transport(
    project_id: str,
    project: dict[str, Any],
    *,
    dependencies: ThreadMountDependencies,
) -> dict[str, Any]:
    """Resolve the transport columns of a non-default project's mount row.

    Returns the four columns a ``project`` row carries; any of them may be
    ``None``. A project with no provider or folder handle has no transport at
    all. A project whose installation the router refuses — a pre-0186 row, an
    instance this replica has not cached, a backend that is down — keeps its
    identity columns but resolves no ``webdav_url``; the caller decides what
    such a partial transport means.
    """
    backend_id = project.get("main_cloud_backend")
    handle_str = project.get("main_cloud_folder_handle")
    webdav_url: str | None = None
    if backend_id and handle_str:
        try:
            backend = dependencies.cloud_router.for_project(project)
            if backend.is_initialized:
                handle = ProjectFolderHandle.from_db(
                    handle_str, backend=backend.backend_id
                )
                webdav_url = backend.get_project_folder_webdav_url(handle)
        except Exception as e:
            logger.warning(
                "Project %s: failed to resolve webdav URL for thread mount: %s",
                project_id,
                e,
            )
    return {
        "backend_id": backend_id,
        "backend_instance_id": project.get("main_cloud_backend_instance_id"),
        "cloud_handle": handle_str,
        "webdav_url": webdav_url,
    }


def transport_is_complete(row: dict[str, Any]) -> bool:
    """Whether a mount row carries everything workspace delivery needs.

    ``_build_agent_cloud_mount`` resolves a row through
    ``for_backend_instance(backend_instance_id)`` and mounts its
    ``webdav_url``; a row missing either fails there, and — the delivery
    being all-or-fallback — takes every other mount of the thread down with
    it. This is the shape the transport repair looks for and refuses to write.
    """
    return bool(
        row.get("backend_id")
        and row.get("backend_instance_id")
        and row.get("webdav_url")
    )


async def build_project_mount_row(
    project_id: str,
    project: dict[str, Any],
    *,
    dependencies: ThreadMountDependencies,
) -> Optional[dict[str, Any]]:
    """One *complete* mount row for ``project``, or ``None``.

    The single-project form of :func:`build_thread_mount_rows`, used by the
    transport repair. A default project yields nothing: its cloud is its
    owner's own root, the user-root connector (main cloud slice 4), never a
    mount row. Any other project yields a ``project`` row only when every
    transport column resolved. A partial transport is ``None`` here on
    purpose: the repair exists to remove that shape and must never mint it.
    ``target_path`` is the uncollided slug — a caller repairing a persisted
    row keeps the ``target_path`` decided at create time, suffix and all.
    """
    if project.get("is_default"):
        return None
    transport = _project_folder_transport(
        project_id, project, dependencies=dependencies
    )
    if not transport_is_complete(transport):
        return None
    return {
        "mount_kind": "project",
        "target_path": f"projects/{slugify_mount_name(project.get('name', ''))}",
        "source_kind": "project_folder",
        "source_ref": project_id,
        **transport,
    }


async def build_thread_mount_rows(
    project_ids: list[str], *, dependencies: ThreadMountDependencies
) -> list[dict[str, Any]]:
    """Resolve mount-row payloads for the given project_ids.

    Each row carries everything ``replace_thread_mounts`` needs. A
    non-default project produces a ``project`` row that mounts at
    ``projects/<slug>/``. A default project produces none: its cloud is its
    owner's own root, the user-root connector (main cloud slice 4); the
    ``project_default`` rows written before stay readable for the scope and
    End's reconstruction.

    Multi-project collisions (two attached projects whose slugified names
    are identical, including case-insensitive matches since the slugifier
    lowercases) are resolved by suffixing ``-2``, ``-3``, ... on the
    target_path so the ``UNIQUE (thread_id, target_path)`` constraint at
    persistence time always holds.
    """
    store = dependencies.store
    rows: list[dict[str, Any]] = []
    used_paths: set[str] = set()
    seen_project_ids: set[str] = set()
    for project_id in project_ids:
        if project_id in seen_project_ids:
            continue
        seen_project_ids.add(project_id)
        project = await store.get_project(project_id)
        if not project:
            continue
        if project.get("is_default"):
            continue
        transport = _project_folder_transport(
            project_id, project, dependencies=dependencies
        )
        base_path = f"projects/{slugify_mount_name(project.get('name', ''))}"
        target_path = base_path
        suffix = 2
        while target_path in used_paths:
            target_path = f"{base_path}-{suffix}"
            suffix += 1
        used_paths.add(target_path)
        rows.append(
            {
                "mount_kind": "project",
                "target_path": target_path,
                "source_kind": "project_folder",
                "source_ref": project_id,
                **transport,
            }
        )
    return rows


async def resolve_thread_datasources(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    project_ids: list[str] | None = None,
    dependencies: ThreadMountDependencies,
) -> list[dict[str, Any]] | None:
    """Resolve and build datasource payload for a thread.

    ``project_ids`` is the canonical input — derived from ``thread_mounts``
    by the caller. ``metadata`` still carries the explicit ``datasource_ids``
    list.
    """
    resolved = await dependencies.resolve_authorized_thread_datasources(
        thread,
        metadata.get("datasource_ids"),
        target_project_ids=project_ids,
    )
    return dependencies.build_datasources_payload(resolved) if resolved else None


async def resolve_thread_datasource_delivery(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    project_ids: list[str] | None = None,
    dependencies: ThreadMountDependencies,
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None]:
    """``(datasources, workspace_ssh_identities)`` from ONE authorization.

    The workspace delivery needs both halves of an SSH connector: the
    ``datasources`` entry that names its alias and the hidden key that the
    agent loads into the workspace ssh-agent. Building both from the same
    resolution keeps them from disagreeing about which connectors exist.
    """
    resolved = await dependencies.resolve_authorized_thread_datasources(
        thread,
        metadata.get("datasource_ids"),
        target_project_ids=project_ids,
    )
    if not resolved:
        return None, None
    return (
        dependencies.build_datasources_payload(resolved),
        dependencies.build_workspace_ssh_identities(resolved),
    )


async def resolve_thread_repositories(
    project_ids: list[str] | None,
    *,
    externalize_urls: bool = False,
    dependencies: ThreadMountDependencies,
) -> list[dict[str, Any]] | None:
    """Resolve raw project repositories for an internal thread agent.

    Public project repository endpoints redact credentials and externalize
    Gitea URLs. Session checkout needs the same internal/raw payload that worker
    job dispatch receives, but only across the internal agent boundary. VM
    sessions still need Gitea URLs rewritten to the ingress-routable host,
    matching worker dispatch.
    """
    if not project_ids:
        return None

    payload: list[dict[str, Any]] = []
    for project_id in project_ids:
        try:
            repos = await dependencies.store.get_project_repositories(str(project_id))
        except Exception as e:
            logger.warning(
                "Failed to resolve project repos for thread project %s: %s",
                project_id,
                e,
            )
            continue
        for r in repos:
            repo_url = r.get("repo_url")
            if externalize_urls and repo_url:
                repo_url = dependencies.externalize_gitea_url(repo_url)
            payload.append(
                {
                    "id": str(r["id"]),
                    "project_id": str(r["project_id"]),
                    "name": r["name"],
                    "role": r["role"],
                    "repo_url": repo_url,
                    "read_only": r["read_only"],
                    "branch": r.get("branch", "main"),
                    "clone_path": r.get("clone_path"),
                    "credentials": r.get("credentials"),
                    "is_managed": bool(r.get("is_managed")),
                }
            )

    return payload or None


__all__ = [
    "ThreadMountDependencies",
    "build_project_mount_row",
    "build_thread_mount_rows",
    "durable_project_ids",
    "project_ids_from_mounts",
    "resolve_thread_datasource_delivery",
    "resolve_thread_datasources",
    "resolve_thread_repositories",
    "slugify_mount_name",
    "thread_project_ids",
    "transport_is_complete",
]
