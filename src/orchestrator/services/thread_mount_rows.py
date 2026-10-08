"""Thread cloud mount rows, and the project scope derived from them.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). This is the
leaf of B05's session half: it answers "which projects is this thread attached
to, and what cloud surface does each one expose", and it is called from both
directions — session configuration resolution reads
:func:`thread_project_ids`, and thread workspace delivery builds the mount rows
an agent receives.

Three properties are load-bearing and moved unchanged:

* **A missing cloud transport is a missing row, never a partial one.** Every
  resolution failure here — no owner, an owner who has never completed an SSO
  login, an unresolvable installation authority, a backend that is down —
  returns ``None`` for that project so the caller falls back to the legacy
  session folder. A thread must never end up with zero cloud surfaces because a
  transient failure was treated as an answer.
* **Connector authorization does not depend on mount rows.**
  :func:`thread_project_ids` answers from ``threads.project_id``, the one
  project a Session belongs to (single_project_sessions.md), so the project
  list that gates datasource access is the same with or without the thread's
  ``thread_mounts`` rows and whether or not the cloud is up. Only a legacy
  multi-project Session, whose column is NULL, still reads its list from the
  legacy places (main_cloud_as_connectors.md, slice 1).
* **``project_default`` rows count as project attachments** of such a legacy
  Session. They mount the owner's cloud home at the workspace root rather
  than under ``projects/``. The shape excluded from the project scope is
  ``repo``.

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
    resolve_user_identity_cached: Callable[..., Awaitable[Any]]
    externalize_gitea_url: Callable[[str], str]
    resolve_authorized_thread_datasources: Callable[..., Awaitable[Any]]
    build_datasources_payload: Callable[[Any], Any]
    build_workspace_ssh_identities: Callable[[Any], Any]
    cloud_workspace_driver: Callable[[], str]


def slugify_mount_name(name: str) -> str:
    """Workspace-safe slug for a mount's target_path."""
    out = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    return out or "project"


def should_skip_session_folder(
    mounts: list[dict[str, Any]], *, dependencies: ThreadMountDependencies
) -> bool:
    """Phase 4 (cloud_collaboration_model.md §9): is the legacy per-session
    cloud folder redundant for this thread?

    If at least one ``thread_mounts`` row has a working ``webdav_url`` —
    any kind (``project``, ``project_default``, ``repo``) — the thread
    already has a user-visible cloud surface. Provisioning a per-session
    folder on top would create a parallel sync target the user has no
    reason to use.

    Returns False when no mount can be observed (no rows, or every row
    failed to resolve a transport). That falls through to legacy
    session-folder provisioning so the thread never ends up with zero
    cloud surfaces — important for unattached sessions and for transient
    backend failures during mount resolution.
    """
    if dependencies.cloud_workspace_driver() == "rclone_mount":
        # The rclone driver falls back to mounting the regular session folder
        # when a user-home/project mount cannot be represented safely. Keep that
        # fallback provisioned instead of treating a WebDAV URL as proof that
        # the runtime can mount the surface.
        return False

    for m in mounts:
        if m.get("webdav_url"):
            return True
    return False


def project_ids_from_mounts(mounts: list[dict[str, Any]]) -> list[str]:
    """Pick out project ``source_ref``s from a list of mount rows.

    Both ``project`` (non-default, mounted under ``projects/<slug>/``) and
    ``project_default`` (default project, mounted at workspace root via the
    user's cloud home) rows contribute — the default project is still a
    project attachment for datasource resolution and visibility.
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
    project. A NULL column means no project, except on a **legacy
    multi-project Session** created before that rule (the column was NULL for
    two or more projects). Its list lives only in the legacy places, read in
    their historical order: ``metadata.project_ids`` (before Phase 1 of the
    cloud collaboration model), else the ``project`` and ``project_default``
    rows of ``thread_mounts`` (``legacy_mounts``). Those Sessions keep
    working as legacy; nothing else reads the mount rows for scope.
    """
    if not thread:
        return []
    if thread.get("project_id"):
        return [str(thread["project_id"])]
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    legacy = [
        str(value)
        for value in (
            metadata.get("project_ids") if isinstance(metadata, dict) else None
        )
        or []
        if value
    ]
    if not legacy:
        legacy = project_ids_from_mounts(legacy_mounts or [])
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


async def build_default_project_mount_row(
    project_id: str,
    project: dict[str, Any],
    *,
    dependencies: ThreadMountDependencies,
) -> Optional[dict[str, Any]]:
    """Shape a ``project_default`` mount row for a default project.

    Resolves the project's owner on the cloud backend and queries their
    personal home Space. The mount targets the workspace root (``target_path
    = ""``) — the agent's workspace and the user's cloud home become two
    views of the same surface. Phase 2 of cloud_collaboration_model.md §9.

    Also stashes the owner's Keycloak ``sub`` on the row (``target_user_sub``)
    so the agent can mint a user-scoped token via RFC 8693 token-exchange
    at WebDAV-call time. Without that, the agent's service-account
    bearer token gets a 404 on PROPFIND against the user's Personal Space
    (owned by exactly one user, not shared with the service account).

    Returns ``None`` when the user-home can't be resolved (no owner, owner
    missing from the cloud backend, no webdav URL, no keycloak_sub on the
    owner, backend down). The caller treats ``None`` as "fall back to the
    legacy session folder" so a transient resolution failure never leaves
    the thread with zero mounts.
    """
    store = dependencies.store
    router = dependencies.cloud_router
    backend = (
        router.for_project_optional(project)
        if project.get("main_cloud_backend")
        else router.for_owner()
    )
    # An unresolvable installation authority is one more "can't resolve the
    # user-home" case, which this function is documented to report as None so
    # the caller falls back to the legacy session folder — not an exception
    # that strands the thread with zero mounts.
    if backend is None or not backend.is_initialized:
        return None
    members = await store.get_project_members(project_id)
    owner = next((m for m in members if m.get("role") == "owner"), None)
    if not owner:
        return None
    owner_email = owner.get("email")
    if not owner_email:
        return None
    owner_user = await store.get_user(str(owner["user_id"]))
    target_user_sub = (owner_user or {}).get("keycloak_sub")
    if not target_user_sub:
        # Owner has never completed an SSO login, so we don't have their
        # Keycloak sub yet. Token-exchange impersonation needs a real sub —
        # bail out and let the caller fall back to the legacy session folder.
        return None
    # Identity args come from the member row (email + display_name); the
    # users row fetched above only vouches for the keycloak_sub.
    owner_identity = {
        "id": owner["user_id"],
        "email": owner_email,
        "display_name": owner.get("display_name"),
    }
    resolved = await dependencies.resolve_user_identity_cached(
        store, owner_identity, backend
    )
    if not resolved:
        return None
    home = await backend.get_user_home(resolved)
    if not home or not home.webdav_url:
        return None
    return {
        "mount_kind": "project_default",
        "target_path": "",
        "source_kind": "user_home",
        "source_ref": project_id,
        "backend_id": backend.backend_id,
        "backend_instance_id": backend.backend_instance_id,
        "cloud_handle": home.handle.to_db(),
        "webdav_url": home.webdav_url,
        "target_user_sub": target_user_sub,
    }


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
    transport repair. A default project yields its ``project_default`` row
    through :func:`build_default_project_mount_row`; any other project yields
    a ``project`` row only when every transport column resolved. A partial
    transport is ``None`` here on purpose: the repair exists to remove that
    shape and must never mint it. ``target_path`` is the uncollided slug — a
    caller repairing a persisted row keeps the ``target_path`` decided at
    create time, suffix and all.
    """
    if project.get("is_default"):
        return await build_default_project_mount_row(
            project_id, project, dependencies=dependencies
        )
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

    Each row carries everything ``replace_thread_mounts`` needs. Default
    projects (Phase 2) produce a ``project_default`` row that mounts the
    owner's cloud home at the workspace root; non-default projects produce
    a ``project`` row that mounts at ``projects/<slug>/``. Projects whose
    cloud transport can't be resolved are skipped — the mount-row entry is
    not partially filled, the caller observes a missing row.

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
            try:
                default_row = await build_default_project_mount_row(
                    project_id, project, dependencies=dependencies
                )
            except Exception as e:
                logger.warning(
                    "Project %s (default): failed to resolve user-home mount row: %s",
                    project_id,
                    e,
                )
                continue
            if default_row:
                rows.append(default_row)
                used_paths.add(default_row.get("target_path", ""))
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
    "build_default_project_mount_row",
    "build_project_mount_row",
    "build_thread_mount_rows",
    "durable_project_ids",
    "project_ids_from_mounts",
    "resolve_thread_datasource_delivery",
    "resolve_thread_datasources",
    "resolve_thread_repositories",
    "should_skip_session_folder",
    "slugify_mount_name",
    "thread_project_ids",
    "transport_is_complete",
]
