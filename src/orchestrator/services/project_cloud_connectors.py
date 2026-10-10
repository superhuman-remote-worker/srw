"""A project's own cloud folder connector (main cloud slice 3).

A project with a folder in the main cloud has one ``cloud_folder`` connector
SRW keeps for it, under the managed key ``project-cloud:<project>``
(``shared.connectors.platform``). It is linked to its project only, a
default of its project's work (``auto_attach``; the driver offers it to
sessions only), never tagged read-only, and recorded as created by the
project's owner. Its config names the folder with the project row's own
columns, verbatim; the credential stays the main cloud's, resolved when the
folder is mounted. The API refuses to create, edit, link, unlink or delete
it (``managed_refusal``).

:func:`ensure_project_cloud_connector` keeps the row in step with its
project. ``ensure_project_cloud_resources`` calls it once the folder exists,
so a project gets its row when it is created, on the project page's
background heal and on promote; the row's config is repointed when the
project's folder handle or installation was repaired. A project whose folder
lives in another installation than the active one gets nothing: no other
folder takes its place. :func:`backfill_project_cloud_connectors` runs it
for every project at startup and logs one count line.

Nothing here fails its caller: a failure is logged and the next pass
retries it.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Slice 3 plan (2026-10-10)", 3.3.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Mapping

from orchestrator.services.connector_drivers.platform import (
    validate_platform_connector,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from shared.connectors.platform import project_cloud_key

logger = logging.getLogger(__name__)

#: The stored type of the main cloud's folders (``srw.cloud-folder/v1``).
CLOUD_FOLDER_TYPE = "cloud_folder"
#: What :func:`ensure_project_cloud_connector` did, as the backfill counts it.
OUTCOMES: tuple[str, ...] = (
    "created",
    "present",
    "repointed",
    "skipped_default",
    "skipped_inactive",
    "skipped_unrouted",
    "deferred",
)


@functools.cache
def _builtin_registry() -> ConnectorDriverRegistry:
    return builtin_connector_drivers()


def _registry(store: Any) -> ConnectorDriverRegistry:
    """The application's drivers (lifecycle binds them to the store)."""
    registry = getattr(store, "connector_drivers", None)
    if isinstance(registry, ConnectorDriverRegistry):
        return registry
    return _builtin_registry()


def skip_reason(project: Mapping[str, Any], active_instance_id: Any) -> str | None:
    """Why a project gets no cloud folder connector now, or ``None``.

    ``skipped_default``: a default project's cloud is its owner's personal
    root, which is the user's connector, not the project's.
    ``skipped_unrouted``: the row names no folder the driver can mount (only
    a legacy Nextcloud folder id, or no provider). ``skipped_inactive``: its
    folder lives in an installation other than the active one, or in none
    SRW recorded.
    """
    if project.get("is_default"):
        return "skipped_default"
    if not project.get("main_cloud_folder_handle") or not project.get(
        "main_cloud_backend"
    ):
        return "skipped_unrouted"
    instance = project.get("main_cloud_backend_instance_id")
    if not instance or not active_instance_id:
        return "skipped_inactive"
    if str(instance) != str(active_instance_id):
        return "skipped_inactive"
    return None


def project_cloud_folder_config(project: Mapping[str, Any]) -> dict[str, Any]:
    """The config of ``project``'s cloud folder connector: the driver's
    schema, from the project row's columns."""
    return {
        "folder_kind": "project",
        "project_id": str(project["id"]),
        "backend_id": project["main_cloud_backend"],
        "backend_instance_id": str(project["main_cloud_backend_instance_id"]),
        "folder_handle": project["main_cloud_folder_handle"],
    }


async def _project_owner_id(store: Any, project: Mapping[str, Any]) -> str | None:
    """The project's earliest-added owner, for the row's record."""
    if "owner_id" in project:
        owner = project.get("owner_id")
        return str(owner) if owner else None
    members = await store.get_project_members(str(project["id"]))
    owners = sorted(
        (member for member in members if member.get("role") == "owner"),
        key=lambda member: (
            str(member.get("joined_at") or ""),
            str(member.get("user_id")),
        ),
    )
    return str(owners[0]["user_id"]) if owners else None


async def _ensure(
    store: Any, project: Mapping[str, Any], *, active_instance_id: Any
) -> str:
    reason = skip_reason(project, active_instance_id)
    if reason is not None:
        return reason
    project_id = str(project["id"])
    key = project_cloud_key(project_id)
    name = f"{project['name']} cloud folder ({project_id[:8]})"
    normalized = await validate_platform_connector(
        _registry(store),
        CLOUD_FOLDER_TYPE,
        name=name,
        connection_url=None,
        config=project_cloud_folder_config(project),
    )
    config = normalized.config or {}
    existing = await store.get_datasource_by_managed_key(key)
    if existing is not None:
        if (existing.get("config") or {}) == config:
            return "present"
        # The project's folder handle or installation was repaired: the row
        # names the folder the project names. The platform write path, since
        # the driver refuses a config edit through the API.
        await store.update_datasource(str(existing["id"]), config=config)
        return "repointed"
    created = await store.create_datasource(
        name=name,
        ds_type=CLOUD_FOLDER_TYPE,
        connection_url=None,
        description=(
            "This project's folder in the main cloud, mounted in its sessions."
        ),
        credentials=normalized.credentials,
        config=config,
        created_by=await _project_owner_id(store, project),
        read_only=None,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project_id],
        managed_key=key,
    )
    # None: another pass (a replica, a rerun) created it first.
    return "created" if created else "present"


async def ensure_project_cloud_connector(
    store: Any,
    project: Mapping[str, Any],
    *,
    active_instance_id: Any,
    log: Any = None,
) -> str:
    """Create or repoint ``project``'s own cloud folder connector.

    Returns one of :data:`OUTCOMES`. A default project, or one whose folder
    the active installation cannot name (:func:`skip_reason`), is skipped;
    a failure is logged and answers ``deferred``. Never raises.
    """
    log = log or logger
    try:
        return await _ensure(store, project, active_instance_id=active_instance_id)
    except Exception as exc:
        log.warning(
            "Project %s: cloud folder connector not ensured (%s); the next "
            "provisioning pass retries it",
            project.get("id"),
            exc,
        )
        return "deferred"


async def backfill_project_cloud_connectors(
    store: Any, *, active_instance_id: Any, log: Any = None
) -> dict[str, int]:
    """Give every project with a folder in the active installation its cloud
    folder connector, and repoint the ones whose folder moved.

    Idempotent: a second pass finds every row ``present``. Archived projects
    are included (the row goes with its project). Logs one count line, an
    error when a project was ``deferred``.
    """
    log = log or logger
    counts = dict.fromkeys(OUTCOMES, 0)
    for project in await store.list_project_cloud_folders():
        outcome = await ensure_project_cloud_connector(
            store, project, active_instance_id=active_instance_id, log=log
        )
        counts[outcome] += 1
    (log.error if counts["deferred"] else log.info)(
        "Project cloud folder connectors: %s", counts
    )
    return counts


__all__ = [
    "CLOUD_FOLDER_TYPE",
    "OUTCOMES",
    "backfill_project_cloud_connectors",
    "ensure_project_cloud_connector",
    "project_cloud_folder_config",
    "skip_reason",
]
