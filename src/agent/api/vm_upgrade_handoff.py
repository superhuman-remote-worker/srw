"""The pinned agent's half of a stateless Session's VM upgrade (C2).

After the orchestrator switches a stateless Session to the pinned lane, the
first pinned owner adopts the ``upgrade-to-workspace`` request in its first
inbox drain. What it does depends on how it attached (spec §3 consumer table):

* lite attach, same runtime generation: ask for the VM, receipt ``.started``,
  run the hot swap;
* lite attach, another generation (End, then Resume): ``upgrade_cancelled``;
* VM attach: the copy was made before the swap, or at owner start by
  ``recover_vm_upgrade_seed``; receipt ``.complete``. A VM attach that finds no
  marker naming the request reports ``upgrade_interrupted``, and so does one
  whose recovery found the copy's source permanently gone (the marker is then
  ``interrupted``, so no later attach retries the copy).

These helpers take explicit arguments; ``persistent_app`` wires them to its
session globals. The recovery copy runs C1's copy → commit → record step
(``copy_lite_workspace_and_commit``) and then records the VM marker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any, Optional, TypeVar
from uuid import UUID

from agent.api.orchestrator_client import (
    WorkspaceUpgradeRouteMissing,
    WorkspaceUpgradeRouteRefused,
)
from agent.api.workspace_upgrade_seed import (
    WorkspaceUpgradeSeedError,
    copy_lite_workspace_and_commit,
)
from agent.core.backends.seed import reported_skipped_files
from shared.workspace_upgrade import (
    VM_UPGRADE_SEED_PENDING,
    VM_UPGRADE_SEED_SEEDED,
    WORKSPACE_UPGRADE_PAYLOAD_KEYS,
    WORKSPACE_UPGRADE_SEED_VERSION,
    vm_provision_generation,
    vm_upgrade_seed,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

ROW_REQUEST = "request"
ROW_CANCEL = "cancel"
ROW_BUSY = "busy"
ROW_COMPLETE = "complete"
ROW_INTERRUPTED = "interrupted"
ROW_REFUSE_TIER = "refuse_tier"

#: Seed-route refusals that no retry can fix: ``seed-source`` can't build the
#: old workspace's source any more (its object store binding changed, or no
#: store is configured). Every other refusal (``no_pending_upgrade_seed``, an
#: identity race, ``upgrade_seed_fence_lost``, a 5xx, an old replica) aborts
#: the attach and the next attach retries.
PERMANENT_SEED_REFUSALS = frozenset({"upgrade_seed_source_changed"})

#: Where a cloud mount manager links its mounts into the workspace
#: (``RcloneMountManager`` / ``SidecarMountWatcher``: ``workspace/cloud``).
_CLOUD_LINK = "cloud"


class VmUpgradeRecoveryError(RuntimeError):
    """The recovery copy can't be made on this attach; the marker stays pending."""


def vm_upgrade_payload(payload: Any) -> Optional[tuple[str, Optional[str]]]:
    """``(admission runtime generation, template)`` of a C2 request, else None."""
    if not isinstance(payload, Mapping) or not set(payload) <= (
        WORKSPACE_UPGRADE_PAYLOAD_KEYS
    ):
        return None
    if payload.get("target_tier") not in (None, "vm"):
        return None
    template = payload.get("template")
    if template is not None and not (isinstance(template, str) and template):
        return None
    try:
        generation = str(UUID(str(payload.get("runtime_generation"))))
    except (TypeError, ValueError):
        return None
    return generation, template


def vm_upgrade_consumer_row(
    *,
    attached_tier: str,
    payload_generation: str,
    current_generation: str,
    marker: Optional[Mapping[str, Any]],
    request_id: str,
    recovered: Optional[Mapping[str, Any]],
    in_flight: bool,
) -> str:
    """Which row of the spec §3 consumer table this request is in."""
    if attached_tier in ("virtual", "none"):
        if payload_generation != current_generation:
            return ROW_CANCEL
        return ROW_BUSY if in_flight else ROW_REQUEST
    if attached_tier == "vm":
        if recovered is not None and recovered.get("control_request_id") == request_id:
            return ROW_COMPLETE
        if (
            marker is not None
            and marker.get("control_request_id") == request_id
            and marker.get("state") == VM_UPGRADE_SEED_SEEDED
        ):
            return ROW_COMPLETE
        return ROW_INTERRUPTED
    return ROW_REFUSE_TIER


def _relative(target: Any, root: str) -> Optional[str]:
    """``target`` relative to the workspace ``root``; None when it lies outside.

    A ``..`` segment can't be placed: ``VmUpgradeRecoveryError``.
    """
    target = str(target or "")
    if ".." in target.split("/"):
        raise VmUpgradeRecoveryError(f"cannot place cloud mount {target!r}")
    if os.path.isabs(target):
        base = root.rstrip("/")
        if not base or not (target == base or target.startswith(base + "/")):
            return None
        target = target[len(base) :]
    return target.strip("/")


def _planned_mount_targets(manager: Any) -> list[Any]:
    """Every mount a cloud mount manager planned, live or not.

    ``SidecarMountWatcher.mounts`` lists only the folders mounted at its last
    read (``planned`` is all of them); ``RcloneMountManager`` plans
    ``cloud_cfg["mounts"]`` (a mount without ``target_path`` lands at
    ``/cloud/<name>``, outside the workspace).
    """
    targets: list[Any] = [
        getattr(mount, "target_path", None)
        for mount in [
            *(getattr(manager, "planned", None) or []),
            *(getattr(manager, "mounts", None) or []),
        ]
    ]
    cloud_cfg = getattr(manager, "cloud_cfg", None)
    if isinstance(cloud_cfg, Mapping):
        targets += [
            mount.get("target_path") if isinstance(mount, Mapping) else None
            for mount in cloud_cfg.get("mounts") or []
        ]
    return targets


def _links_skipped(manager: Any) -> bool:
    skip = getattr(manager, "skip_links", None)
    if skip is not None:
        return bool(skip)
    cloud_cfg = getattr(manager, "cloud_cfg", None)
    return isinstance(cloud_cfg, Mapping) and bool(
        cloud_cfg.get("skip_workspace_links")
    )


def cloud_mount_targets(session: Any) -> list[str]:
    """Workspace-relative roots a cloud mount or sync owns on this attach.

    The recovery copy writes nothing under them (``in_cloud_root``): through
    ``workspace/cloud`` a write lands in the user's live cloud, or fails on a
    read-only view. A mount manager links every planned mount, mounted or
    not, at ``cloud`` unless its links are skipped; a planned mount inside
    the workspace is a root at its own path. A sync root outside the
    workspace, or any root with a ``..`` segment, can't be placed and fails
    closed.
    """
    targets: list[str] = []

    def add(relative: str) -> None:
        if relative not in targets:
            targets.append(relative)

    sync = getattr(session, "workspace_sync", None)
    sync_root = str(
        getattr(getattr(session, "workspace_manager", None), "path", "") or ""
    )
    for mount in getattr(sync, "mounts", None) or []:
        target = getattr(mount, "target_path", "")
        relative = _relative(target, sync_root)
        if relative is None:
            raise VmUpgradeRecoveryError(
                f"cannot place cloud sync root {target!r} in the workspace"
            )
        add(relative)
    manager = getattr(session, "cloud_mount_manager", None)
    if manager is None:
        if getattr(session, "cloud_mount_error", None):
            # The attach asked for a mount that didn't start: a link an
            # earlier attach installed may still point into the cloud.
            add(_CLOUD_LINK)
        return targets
    manager_root = str(getattr(manager, "workspace_root", "") or "")
    planned = _planned_mount_targets(manager)
    for target in planned:
        if target:
            relative = _relative(target, manager_root)
            if relative is not None:
                add(relative)
    if planned and not _links_skipped(manager):
        add(_CLOUD_LINK)
    return targets


def in_cloud_root(rel: str, roots: Iterable[str]) -> bool:
    """True when ``rel`` is a cloud root or lies under one (plain prefix).

    Unlike C1's ``cloud_synced_path``, nothing under a root is copied, not
    even a path cloud sync ignores: the recovery copy writes through the
    workspace's live cloud link.
    """
    for root in roots:
        root = str(root or "").strip("/")
        if root == "" or rel == root or rel.startswith(f"{root}/"):
            return True
    return False


async def call_with_route_retry(
    call: Callable[[], Awaitable[T]], *, attempts: int = 5, delay_s: float = 2.0
) -> T:
    """Retry a seed route an old orchestrator replica doesn't have yet (404).

    A rolling deploy routes some calls to a replica without the C2 routes;
    after the bounded window the caller fails the upgrade before any swap.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await call()
        except WorkspaceUpgradeRouteMissing:
            if attempt == attempts:
                raise
            await asyncio.sleep(delay_s)
    raise AssertionError("unreachable")


def _matching_seed_workspace(
    spec: Any, *, control_request_id: str, provision_generation: str
) -> dict[str, Any]:
    """The lite workspace a seed-source answer names, if it is this marker's."""
    workspace = spec.get("workspace") if isinstance(spec, Mapping) else None
    if (
        not isinstance(workspace, Mapping)
        or workspace.get("backend") not in ("virtual", "none")
        or spec.get("version") != WORKSPACE_UPGRADE_SEED_VERSION
        or spec.get("control_request_id") != control_request_id
        or spec.get("provision_generation") != provision_generation
    ):
        raise VmUpgradeRecoveryError("the seed source doesn't match the pending marker")
    return dict(workspace)


async def recover_vm_upgrade_seed(
    *, session: Any, client: Any, thread_id: str
) -> Optional[dict[str, Any]]:
    """Copy a lost predecessor's VM upgrade files in, then mark them seeded.

    Runs at owner start on a VM attach, before the first drain and before the
    Session is ready, so no turn can have written to the VM yet: the copy
    can't overwrite newer VM files. Returns ``{"control_request_id",
    "seeded_files"}`` (plus ``skipped_files`` when a file was over the upload
    ceiling) when it ran, ``{"control_request_id", "interrupted":
    True}`` when a permanent refusal (``PERMANENT_SEED_REFUSALS``) ended it,
    None when nothing is owed. Any other failure raises before the marker is
    recorded, so a later attach copies again.
    """
    thread = await session.postgres_conn.get_thread(str(thread_id))
    metadata = (thread or {}).get("metadata")
    marker = vm_upgrade_seed(metadata)
    if marker is None or marker["state"] != VM_UPGRADE_SEED_PENDING:
        return None
    provision_generation = vm_provision_generation(metadata)
    if provision_generation is None:
        raise VmUpgradeRecoveryError(
            "the pending VM upgrade has no provision generation"
        )
    backend = getattr(getattr(session, "workspace_manager", None), "backend", None)
    if backend is None or not getattr(backend, "supports_shell", False):
        raise VmUpgradeRecoveryError("the VM workspace is not attached")
    control_request_id = marker["control_request_id"]
    files = 0
    oversized: list[str] = []
    if marker["source"] is not None:
        try:
            spec = await call_with_route_retry(
                lambda: client.get_workspace_upgrade_seed_source(str(thread_id))
            )
        except WorkspaceUpgradeRouteRefused as exc:
            if exc.code not in PERMANENT_SEED_REFUSALS:
                raise
            # No attach can ever make this copy: end it, so the Session can
            # attach (and End) and the upgrade reads as interrupted.
            await call_with_route_retry(
                lambda: client.mark_workspace_upgrade_interrupted(
                    str(thread_id),
                    provision_generation=provision_generation,
                    control_request_id=control_request_id,
                )
            )
            logger.warning(
                "VM upgrade recovery copy interrupted: thread=%s request=%s code=%s",
                thread_id,
                control_request_id,
                exc.code,
            )
            return {"control_request_id": control_request_id, "interrupted": True}
        workspace = _matching_seed_workspace(
            spec,
            control_request_id=control_request_id,
            provision_generation=provision_generation,
        )
        targets = cloud_mount_targets(session)
        try:
            files = await copy_lite_workspace_and_commit(
                session,
                workspace,
                thread_id=str(thread_id),
                exclude=lambda rel: in_cloud_root(rel, targets),
                oversized=oversized,
            )
        except WorkspaceUpgradeSeedError as exc:
            raise VmUpgradeRecoveryError(str(exc)) from exc
    await call_with_route_retry(
        lambda: client.mark_workspace_upgrade_seeded(
            str(thread_id),
            provision_generation=provision_generation,
            control_request_id=control_request_id,
        )
    )
    logger.info(
        "VM upgrade recovery copy: thread=%s request=%s files=%d skipped=%d",
        thread_id,
        control_request_id,
        files,
        len(oversized),
    )
    result: dict[str, Any] = {
        "control_request_id": control_request_id,
        "seeded_files": int(files),
    }
    if oversized:
        # Over the upload ceiling: the old prefix keeps them (Important 3).
        result["skipped_files"] = reported_skipped_files(oversized)
    return result


__all__ = [
    "PERMANENT_SEED_REFUSALS",
    "ROW_BUSY",
    "ROW_CANCEL",
    "ROW_COMPLETE",
    "ROW_INTERRUPTED",
    "ROW_REFUSE_TIER",
    "ROW_REQUEST",
    "VmUpgradeRecoveryError",
    "call_with_route_retry",
    "cloud_mount_targets",
    "in_cloud_root",
    "recover_vm_upgrade_seed",
    "vm_upgrade_consumer_row",
    "vm_upgrade_payload",
]
