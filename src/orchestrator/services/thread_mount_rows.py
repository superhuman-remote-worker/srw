"""A thread's project scope, and what the legacy ``thread_mounts`` rows
still say about it.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). This is the
leaf of B05's session half: it answers "which projects is this thread attached
to", and session configuration resolution reads :func:`thread_project_ids`.
Since main cloud slice 3 a session mounts its attached cloud connectors
(``cloud_connector_mounts``), since slice 5 a protected session's folder is
its ``thread_protected_sources`` row, and since slice 8 nothing writes a
``thread_mounts`` row any more: neither admission nor a backfill here. The
rows sessions created before still exist, and give a legacy multi-project
session its scope until the table goes (main_cloud_as_connectors.md,
"Next release: drops").

Two properties are load-bearing:

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
  connector. The shape excluded from the project scope is ``repo``.

Collaborators arrive through :class:`ThreadMountDependencies`, rebuilt per
invocation by the application rather than captured at import: ``store`` and
``cloud_router`` are both rebound during ``lifespan``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

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
    It reads the rows only for a legacy multi-project Session, and never
    writes one: a session's cloud folders are its connectors (slice 8).
    """
    store = dependencies.store
    thread = await store.get_thread(thread_id)
    if not thread:
        return []
    mounts = await store.list_thread_mounts(thread_id)
    return durable_project_ids(thread, legacy_mounts=mounts)


async def resolve_thread_datasources(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    project_ids: list[str] | None = None,
    dependencies: ThreadMountDependencies,
) -> list[dict[str, Any]] | None:
    """Resolve and build datasource payload for a thread.

    ``project_ids`` is the canonical input — the caller's
    :func:`thread_project_ids`. ``metadata`` still carries the explicit
    ``datasource_ids`` list.
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
    "durable_project_ids",
    "project_ids_from_mounts",
    "resolve_thread_datasource_delivery",
    "resolve_thread_datasources",
    "resolve_thread_repositories",
    "slugify_mount_name",
    "thread_project_ids",
]
