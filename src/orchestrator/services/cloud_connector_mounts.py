"""Which cloud folders a session mounts: its attached cloud connectors.

Main cloud slice 3 (and 6.1 for the outbox). A session's cloud folders come
from the cloud connectors in its selection (``metadata.datasource_ids``),
each bound on its own: a project's folder (``cloud_folder``, folder kind
``project``), the owner's session outbox (``cloud_outbox``) and, from slice
4, the owner's personal root (``cloud_folder``, folder kind ``user_root``).
A connector that cannot be mounted is an exclusion with a closed reason;
nothing takes its place, and the others mount anyway. This replaces the
all-or-fallback rule over ``thread_mounts`` rows, which a protected session
still reads (slice 5 moves it).

This module answers *what*: the selection's cloud connectors, whether each
is still authorised for the session's owner, and per folder kind the
``thread_mounts``-shaped row the rclone builder reads
(:data:`FOLDER_KIND_BINDERS`). ``agent_cloud_mounts`` builds the mounts.

The metadata it keeps on a session:

* ``cloud_mount_rule: "connectors"`` (:data:`CLOUD_MOUNT_RULE_KEY`): the
  session's folders come from its connectors. Admission writes it on every
  new session; an older one gets it from its one-time conversion. End's
  terminal reconstruction of an in-workspace mount reads it: without it the
  resident rclone processes were mounted by the old rule, which End then
  rebuilds byte for byte to adopt them.
* ``cloud_outbox`` (:data:`CLOUD_OUTBOX_KEY`): the session's outbox
  folder, ``{connector_id, state, reason?, share_retry_at?}``.
* ``datasource_selection.attached_live`` (:data:`ATTACHED_LIVE_KEY`): each
  cloud connector a live edit attached while the session's workspace ran,
  with that workspace's runtime. Its driver attaches nothing live
  (``live_attach=False``): the running workspace never gets it
  (``mount_unsupported``, ``next_start``), the next one mounts it.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Slice 3 plan (2026-10-10)" 3.5 and "Overnight run: slices 3 to 8".
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from orchestrator.services.cloud.base import CloudMountSubject
from orchestrator.services.cloud_user_root import (
    MOUNT_NAME as USER_ROOT_MOUNT_NAME,
    user_root_mount,
)
from orchestrator.services.connector_drivers.cloud_folder import (
    CloudFolderDriver,
    CloudFolderMount,
    CloudFolderUnavailable,
)
from orchestrator.services.cloud_mount_status import STATUS_KEY
from orchestrator.services.connector_drivers.cloud_outbox import CloudOutboxDriver
from shared.connectors.builtin import (
    CLOUD_FOLDER_SPEC,
    CLOUD_OUTBOX_SPEC,
    spec_for_row,
)

logger = logging.getLogger(__name__)

#: The metadata key saying which rule a session's cloud folders follow.
CLOUD_MOUNT_RULE_KEY = "cloud_mount_rule"
#: Its one value: each attached cloud connector on its own.
CONNECTOR_RULE = "connectors"
#: The metadata key of a session's outbox folder.
CLOUD_OUTBOX_KEY = "cloud_outbox"
#: What a session's outbox folder may record as ``cloud_outbox.reason``
#: when SRW could not create it: no main cloud served the session, the
#: provider refused the folder, a folder was already at its name (never
#: taken over: a new session's folder must be new), or something else
#: failed.
OUTBOX_FAILURE_REASONS: tuple[str, ...] = (
    "backend_unavailable",
    "create_failed",
    "folder_exists",
    "error",
)

#: Why an attached cloud connector mounts nothing, as closed codes. The
#: drivers' own (``access_unavailable``, ``config_invalid``, ``not_eligible``,
#: ``no_session_folder``, ``backend_unavailable``), ``unbuildable`` for a
#: mount the provider could not build (``detail`` says why, a closed code),
#: ``beside_protected`` beside a protected session's folder,
#: ``too_many_mounts`` past the workspace's limit, ``grant_unavailable``
#: / ``reader_unavailable`` for a project folder whose per-folder grant (a
#: writer, a reader) is not active (``detail``: the failed mint's code, or
#: ``not_ready``), ``cloud_not_connected`` / ``cloud_grant_revoked`` for
#: a user's own root its user has not connected, or whose credential the
#: cloud refused (``cloud_user_root``), ``delivery_unsupported`` for a
#: cloud folder of a deployment that syncs files instead of mounting them
#: (``CLOUD_WORKSPACE_DRIVER=sync``), which delivers no cloud folder, and
#: ``mount_unsupported`` for one of a workspace that can mount none
#: (``detail``: ``container_mounts_disabled``, ``fuse_denied``), or that
#: never gets this one: a Pod of the in-pod plane created without a plan,
#: which never gets a folder its sidecars would have held the password of
#: (``needs_new_pod``, ``plan_failed``), and a folder attached while the
#: workspace ran (``next_start``).
EXCLUSION_REASONS: tuple[str, ...] = (
    "access_unavailable",
    "config_invalid",
    "not_eligible",
    "no_session_folder",
    "backend_unavailable",
    "unbuildable",
    "beside_protected",
    "too_many_mounts",
    "grant_unavailable",
    "reader_unavailable",
    "cloud_not_connected",
    "cloud_grant_revoked",
    "delivery_unsupported",
    "mount_unsupported",
)

_DRIVERS: dict[str, CloudFolderDriver | CloudOutboxDriver] = {
    CLOUD_FOLDER_SPEC.name: CloudFolderDriver(),
    CLOUD_OUTBOX_SPEC.name: CloudOutboxDriver(),
}


def uses_connector_rule(metadata: Mapping[str, Any] | None) -> bool:
    """Whether a session's cloud folders come from its connectors."""
    return isinstance(metadata, Mapping) and (
        metadata.get(CLOUD_MOUNT_RULE_KEY) == CONNECTOR_RULE
    )


def outbox_record(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    """A session's ``cloud_outbox`` record (empty without one)."""
    value = metadata.get(CLOUD_OUTBOX_KEY) if isinstance(metadata, Mapping) else None
    return dict(value) if isinstance(value, Mapping) else {}


def selected_ids(metadata: Mapping[str, Any] | None) -> list[str]:
    """A session's connector selection, in order, once each."""
    values = metadata.get("datasource_ids") if isinstance(metadata, Mapping) else None
    if not isinstance(values, list):
        return []
    return list(dict.fromkeys(str(value) for value in values if value))


def outbox_attached(metadata: Mapping[str, Any] | None) -> bool:
    """Whether the session's selection holds the outbox its folder is for.

    Read without the database: the outbox's connector id is recorded with
    its folder (``cloud_outbox.connector_id``). A session under the old
    rule keeps its session folder as before (``True`` when it has one).
    """
    if not uses_connector_rule(metadata):
        return True
    connector_id = outbox_record(metadata).get("connector_id")
    return bool(connector_id) and str(connector_id) in selected_ids(metadata)


def cloud_mount_driver(
    row: Mapping[str, Any] | None,
) -> CloudFolderDriver | CloudOutboxDriver | None:
    """The driver of a connector the workspace mounts, or ``None`` for one
    delivered otherwise."""
    spec = spec_for_row(row)
    return _DRIVERS.get(spec.name) if spec is not None else None


#: The ``datasource_selection`` key of the cloud connectors attached live.
ATTACHED_LIVE_KEY = "attached_live"


def workspace_runtime_identity(metadata: Mapping[str, Any] | None) -> str | None:
    """The session's current workspace runtime: its ready VM's, else its
    container Pod's UID; ``None`` with neither. A new Pod or VM is a new
    runtime."""
    if not isinstance(metadata, Mapping):
        return None
    vm = metadata.get("vm")
    if isinstance(vm, Mapping) and vm.get("status") == "ready" and vm.get("ssh_host"):
        incarnation = vm.get("_runtime_incarnation") or vm.get("vm_uid")
        return f"vm:{incarnation}" if incarnation else None
    workspace = metadata.get("workspace_container")
    if isinstance(workspace, Mapping) and workspace.get("_runtime_incarnation"):
        return f"pod:{workspace['_runtime_incarnation']}"
    return None


def _attached_live_record(metadata: Mapping[str, Any] | None) -> dict[str, str]:
    selection = (
        metadata.get("datasource_selection") if isinstance(metadata, Mapping) else None
    )
    record = (
        selection.get(ATTACHED_LIVE_KEY) if isinstance(selection, Mapping) else None
    )
    if not isinstance(record, Mapping):
        return {}
    return {str(cid): str(runtime) for cid, runtime in record.items() if runtime}


def attached_live(metadata: Mapping[str, Any] | None) -> frozenset[str]:
    """The cloud connectors attached while the current workspace ran: it
    never gets them."""
    runtime = workspace_runtime_identity(metadata)
    if runtime is None:
        return frozenset()
    return frozenset(
        cid
        for cid, attached_on in _attached_live_record(metadata).items()
        if attached_on == runtime
    )


def attached_live_after_edit(
    metadata: Mapping[str, Any] | None,
    *,
    previous_ids: set[str],
    selected_rows: list[Mapping[str, Any]],
) -> dict[str, str]:
    """``attached_live`` after a live edit to ``selected_rows`` (the new
    selection's connectors): what the running workspace already withholds
    and still has attached, and each cloud connector the edit adds. Empty
    while no workspace runs, whose next one mounts them all."""
    runtime = workspace_runtime_identity(metadata)
    if runtime is None:
        return {}
    selected = {str(row.get("id") or "") for row in selected_rows}
    record = {
        cid: attached_on
        for cid, attached_on in _attached_live_record(metadata).items()
        if attached_on == runtime and cid in selected
    }
    for row in selected_rows:
        cid = str(row.get("id") or "")
        if cid and cid not in previous_ids and cloud_mount_driver(row) is not None:
            record[cid] = runtime
    return record


@dataclass(frozen=True)
class SelectedCloudConnector:
    """One cloud connector of a session's selection.

    ``row`` is the connector as stored, with its managed key and its project
    link's ``project_read_only`` and ``project_access``. ``eligible`` is
    False when the owner may no longer attach it (a revoked link or
    membership, a deleted project). ``requested`` is the level the session
    chose for it (``metadata.datasource_access``, only where its origin was
    a choice: a default is resolved again at each attach, ``None`` here),
    ``ceiling`` the highest level its owner may have now (the policy's
    verdict: a viewer of the folder's project, ``read_only``).
    """

    row: dict[str, Any]
    driver: CloudFolderDriver | CloudOutboxDriver
    eligible: bool
    requested: str | None = None
    ceiling: str | None = None

    @property
    def connector_id(self) -> str:
        return str(self.row.get("id") or "")

    @property
    def folder_kind(self) -> str:
        """The mount kind an exclusion names: the folder's kind, or
        ``cloud_outbox``."""
        if isinstance(self.driver, CloudOutboxDriver):
            return "cloud_outbox"
        config = self.row.get("config")
        kind = config.get("folder_kind") if isinstance(config, Mapping) else None
        return str(kind or "project")


async def selected_cloud_connectors(
    thread: Mapping[str, Any],
    *,
    datasource_ids: list[str],
    project_ids: list[str],
    store: Any,
) -> list[SelectedCloudConnector]:
    """The cloud connectors of ``datasource_ids``, in selection order.

    A deleted connector is simply not there. Each one is classified for the
    session's owner as attach does (``classify_datasource_selection``),
    per connector: one that is no longer authorised is reported, never
    allowed to fail the others.
    """
    if not datasource_ids:
        return []
    rows = await store.resolve_datasources_for_thread(
        datasource_ids=datasource_ids, project_ids=project_ids
    )
    found: dict[str, tuple[dict[str, Any], CloudFolderDriver | CloudOutboxDriver]] = {}
    for row in rows:
        driver = cloud_mount_driver(row)
        if driver is None:
            continue
        connector_id = str(row.get("id") or "")
        if "managed_key" not in row:
            # The thread resolver does not read the key; the driver binds
            # only a row SRW created, which the key says.
            full = await store.get_datasource(connector_id)
            if full is None:
                continue
            row = {
                **full,
                **{
                    key: row[key]
                    for key in ("project_read_only", "project_access")
                    if key in row
                },
            }
        found[connector_id] = (dict(row), driver)
    if not found:
        return []
    verdicts = await _verdicts(thread, list(found), project_ids, store)
    access = _requested_access(thread)
    selected: list[SelectedCloudConnector] = []
    for cid in datasource_ids:
        if cid not in found:
            continue
        row, driver = found[cid]
        denied, ceiling = verdicts.get(cid, (False, None))
        selected.append(
            SelectedCloudConnector(
                row=row,
                driver=driver,
                eligible=not denied,
                requested=access.get(cid),
                ceiling=ceiling,
            )
        )
    return selected


#: The origins of a stored level that were a default, not a choice
#: (``shared.connectors.contract.ACCESS_ORIGINS``): such a level is resolved
#: again at each attach, within what the tier offers and the owner may have
#: now, so a tier change or a demotion lowers it instead of dropping the
#: folder.
DEFAULT_ACCESS_ORIGINS = frozenset(
    {"link_default", "config", "driver_default", "clamped"}
)


def _requested_access(thread: Mapping[str, Any]) -> dict[str, str]:
    """The level the session chose per connector, to bind as asked or not at
    all: its stored levels (``metadata.datasource_access``) except the ones
    whose origin (``datasource_selection.access``) was a default. A level
    of unknown origin counts as chosen."""
    from orchestrator.services.stateless_workspace_gate import (
        thread_metadata_object,
    )

    metadata = thread_metadata_object(thread)
    stored = metadata.get("datasource_access")
    if not isinstance(stored, Mapping):
        return {}
    selection = metadata.get("datasource_selection")
    origins = selection.get("access") if isinstance(selection, Mapping) else None
    chosen: dict[str, str] = {}
    for cid, level in stored.items():
        if not level:
            continue
        record = origins.get(str(cid)) if isinstance(origins, Mapping) else None
        origin = record.get("origin") if isinstance(record, Mapping) else None
        if origin in DEFAULT_ACCESS_ORIGINS:
            continue
        chosen[str(cid)] = str(level)
    return chosen


async def _verdicts(
    thread: Mapping[str, Any],
    connector_ids: list[str],
    project_ids: list[str],
    store: Any,
) -> dict[str, tuple[bool, str | None]]:
    """Per connector: whether the session's owner may no longer attach it,
    and the highest level they may have (``access_ceiling``, where the
    policy gives one)."""
    verdicts = await _classified(thread, connector_ids, project_ids, store)
    if verdicts is None:
        return {cid: (True, None) for cid in connector_ids}
    found = {str(verdict.datasource_id): verdict for verdict in verdicts}
    return {
        cid: (
            bool(getattr(found.get(cid), "denied", False)),
            getattr(found.get(cid), "access_ceiling", None) or None,
        )
        for cid in connector_ids
    }


async def _classified(
    thread: Mapping[str, Any],
    connector_ids: list[str],
    project_ids: list[str],
    store: Any,
) -> list[Any] | None:
    """The policy's verdicts for the session's owner, as attach classifies;
    ``None`` when the owner is gone or the selection is malformed (every
    connector refused)."""
    from orchestrator.services.datasource_policy import (
        DatasourceUnavailableError,
        classify_datasource_selection,
    )

    owner_id = thread.get("user_id")
    try:
        if not owner_id:
            verdicts, _ = await classify_datasource_selection(
                store,
                None,
                None,
                connector_ids,
                project_ids,
                None,
                trusted_system_inheritance=True,
            )
        else:
            owner = await store.get_user(str(owner_id))
            if owner is None:
                return None
            verdicts, _ = await classify_datasource_selection(
                store,
                owner,
                str(owner_id),
                connector_ids,
                project_ids,
                None,
                allow_admin_explicit_override=True,
            )
    except DatasourceUnavailableError:
        return None
    return list(verdicts)


def excluded_entry(
    connector: SelectedCloudConnector,
    reason: str,
    *,
    name: str = "",
    detail: str | None = None,
    level: str | None = None,
) -> dict[str, str]:
    """An exclusion of the plan: which connector, what it was, why, the
    name it would have been mounted under (empty when it never got one),
    and the level it would have bound at, once that was resolved."""
    entry = {
        "source_ref": connector.connector_id,
        "mount_kind": connector.folder_kind,
        "reason": reason,
        "name": name,
    }
    if detail:
        entry["detail"] = detail
    if level:
        entry["level"] = level
    return entry


#: ``mount_unsupported``'s detail for a Virtual Session: a folder it cannot
#: serve at the level requested (``protected``, or a provider the Virtual
#: delivery does not serve yet) was left out when it was created; an upgrade
#: to a Container restores it (``restore_virtual_exclusions``).
VIRTUAL_WORKSPACE_DETAIL = "virtual_workspace"


def virtual_workspace_exclusion(row: Mapping[str, Any]) -> dict[str, str]:
    """A cloud connector (a datasource row) a Virtual Session left out at
    creation, as an exclusion of its record (owner 2026-10-21; since
    2026-10-28 only the rows Virtual cannot serve at the requested level)."""
    if isinstance(cloud_mount_driver(row), CloudOutboxDriver):
        kind = "cloud_outbox"
    else:
        config = row.get("config")
        kind = config.get("folder_kind") if isinstance(config, Mapping) else None
    return {
        "source_ref": str(row.get("id") or ""),
        "mount_kind": str(kind or "project"),
        "reason": "mount_unsupported",
        "detail": VIRTUAL_WORKSPACE_DETAIL,
        "title": str(row.get("name") or ""),
    }


def virtual_workspace_exclusions(
    metadata: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """What a Virtual Session's record says it left out at creation."""
    status = metadata.get(STATUS_KEY) if isinstance(metadata, Mapping) else None
    excluded = status.get("excluded") if isinstance(status, Mapping) else None
    return [
        dict(entry)
        for entry in excluded or []
        if isinstance(entry, Mapping)
        and entry.get("detail") == VIRTUAL_WORKSPACE_DETAIL
    ]


# --------------------------------------------------------------------------- #
# One folder kind each
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FolderBindRequest:
    """What a folder kind's binder gets for one attached ``cloud_folder``
    connector: the session, the connector row, and the driver's binding (its
    level resolved, its key checked). ``store`` and ``cloud_router`` are the
    application's."""

    thread: Mapping[str, Any]
    row: Mapping[str, Any]
    binding: CloudFolderMount
    store: Any
    slugify: Callable[[str], str]
    cloud_router: Any = None


@dataclass(frozen=True)
class FolderBinding:
    """A folder ready for the rclone builder: ``row`` in the shape of a
    ``thread_mounts`` row (``CloudFolderMount.mount_row``; ``target_user_sub``
    or any other subject field the builder reads may be added), and the
    workspace ``name`` it asks for, which the caller suffixes on a clash.
    ``subject`` is the principal the folder is mounted as when the binder
    names one (a user's own root: its user's own credential, never printed);
    a project folder's comes from its per-folder grant instead."""

    row: dict[str, Any]
    name: str
    subject: CloudMountSubject | None = None


FolderBinder = Callable[
    [FolderBindRequest], Awaitable[FolderBinding | CloudFolderUnavailable]
]


async def bind_project_folder(
    request: FolderBindRequest,
) -> FolderBinding | CloudFolderUnavailable:
    """A project's folder, mounted under the project's name.

    The project's own columns name the folder: the connector's config is
    SRW's copy of them, which the project's provisioning pass repoints when
    the project's folder or installation is repaired. A copy behind the
    project would otherwise mount a folder the project no longer names, or
    exclude the project's folder until the next pass; the key names the
    project, so its current folder is the connector's. A project that names
    no folder now mounts nothing (``unbuildable``, ``no_transport``): the
    stale copy is never used in its place.
    """
    binding = request.binding
    project = (
        await request.store.get_project(binding.project_id)
        if binding.project_id
        else None
    )
    if not project:
        return CloudFolderUnavailable(
            binding.connector_id,
            "not_eligible",
            "The project of this cloud folder no longer exists",
        )
    current = {
        "backend_id": project.get("main_cloud_backend"),
        "backend_instance_id": (
            str(project["main_cloud_backend_instance_id"])
            if project.get("main_cloud_backend_instance_id")
            else None
        ),
        "cloud_handle": project.get("main_cloud_folder_handle"),
    }
    row = binding.mount_row()
    if (
        current["backend_id"],
        current["backend_instance_id"],
        current["cloud_handle"],
    ) != (
        binding.backend_id,
        binding.backend_instance_id,
        binding.folder_handle,
    ):
        logger.info(
            "Cloud folder connector %s: its project's folder moved; mounting the "
            "project's current folder",
            binding.connector_id,
        )
    row.update(current)
    return FolderBinding(row=row, name=request.slugify(str(project.get("name") or "")))


async def bind_user_root_folder(
    request: FolderBindRequest,
) -> FolderBinding | CloudFolderUnavailable:
    """A user's own root, mounted at ``cloud/my_cloud`` as its user's own
    account with its own credential (``cloud_user_root``).

    Only the session owner's own root mounts (``not_eligible`` otherwise);
    one its user has not connected is ``cloud_not_connected``, one whose
    credential the cloud refused ``cloud_grant_revoked``. Nothing takes its
    place. Reads the database only.
    """
    owner = request.thread.get("user_id")
    built = await user_root_mount(
        request.row,
        owner_id=str(owner) if owner else None,
        store=request.store,
        cloud_router=request.cloud_router,
    )
    if isinstance(built, CloudFolderUnavailable):
        return built
    row, credential = built
    row["access"] = request.binding.access
    return FolderBinding(
        row=row,
        name=USER_ROOT_MOUNT_NAME,
        subject=CloudMountSubject(
            user_id=str(owner),
            username=credential.login_name,
            credential=credential.credential,
        ),
    )


#: The binder of each ``cloud_folder`` kind. A kind without one mounts
#: nothing (``config_invalid``).
FOLDER_KIND_BINDERS: dict[str, FolderBinder] = {
    "project": bind_project_folder,
    "user_root": bind_user_root_folder,
}


__all__ = [
    "CLOUD_MOUNT_RULE_KEY",
    "CLOUD_OUTBOX_KEY",
    "CONNECTOR_RULE",
    "ATTACHED_LIVE_KEY",
    "EXCLUSION_REASONS",
    "FOLDER_KIND_BINDERS",
    "OUTBOX_FAILURE_REASONS",
    "FolderBindRequest",
    "FolderBinder",
    "FolderBinding",
    "SelectedCloudConnector",
    "attached_live",
    "attached_live_after_edit",
    "bind_project_folder",
    "bind_user_root_folder",
    "cloud_mount_driver",
    "excluded_entry",
    "outbox_attached",
    "outbox_record",
    "selected_cloud_connectors",
    "selected_ids",
    "uses_connector_rule",
    "workspace_runtime_identity",
]
