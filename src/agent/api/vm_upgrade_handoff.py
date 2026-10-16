"""The pinned agent's half of a stateless Session's VM upgrade (C2).

After the orchestrator switches a stateless Session to the pinned lane, the
first pinned owner adopts the ``upgrade-to-workspace`` request in its first
inbox drain. What it does depends on how it attached (spec §3 consumer table):

* lite attach, same runtime generation: ask for the VM, receipt ``.started``,
  run the hot swap;
* lite attach, another generation (End, then Resume): ``upgrade_cancelled``;
* VM attach: the copy was made before the swap, or at owner start by
  ``recover_vm_upgrade_seed``; receipt ``.complete``. A VM attach that finds no
  marker naming the request reports ``upgrade_interrupted``.

These helpers take explicit arguments; ``persistent_app`` wires them to its
session globals. The recovery copy runs C1's copy → commit → record step
(``copy_lite_workspace_and_commit``) and then records the VM marker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Optional, TypeVar
from uuid import UUID

from agent.api.orchestrator_client import WorkspaceUpgradeRouteMissing
from agent.api.workspace_upgrade_seed import (
    WorkspaceUpgradeSeedError,
    copy_lite_workspace_and_commit,
)
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


def cloud_mount_targets(session: Any) -> list[str]:
    """Workspace-relative roots a cloud mount or sync owns on this attach.

    The recovery copy must not write into them: a live rclone mount would push
    the old copies to the cloud, and the first pull brings the files back. A
    mount manager's mount outside the workspace (a VM's ``/cloud/<name>``) is
    linked in at ``cloud``; a sync root outside the workspace, or any root
    with a ``..`` segment, can't be placed and fails closed.
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
    manager_root = str(getattr(manager, "workspace_root", "") or "")
    for mount in getattr(manager, "mounts", None) or []:
        relative = _relative(getattr(mount, "target_path", ""), manager_root)
        add(_CLOUD_LINK if relative is None else relative)
    return targets


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
    "seeded_files"}`` when it ran, None when nothing is owed. Any failure
    raises before the marker is recorded, so a later attach copies again.
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
    if marker["source"] is not None:
        spec = await call_with_route_retry(
            lambda: client.get_workspace_upgrade_seed_source(str(thread_id))
        )
        workspace = _matching_seed_workspace(
            spec,
            control_request_id=control_request_id,
            provision_generation=provision_generation,
        )
        targets = cloud_mount_targets(session)
        try:
            files = await copy_lite_workspace_and_commit(
                session, workspace, thread_id=str(thread_id), targets=targets
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
        "VM upgrade recovery copy: thread=%s request=%s files=%d",
        thread_id,
        control_request_id,
        files,
    )
    return {"control_request_id": control_request_id, "seeded_files": int(files)}


__all__ = [
    "ROW_BUSY",
    "ROW_CANCEL",
    "ROW_COMPLETE",
    "ROW_INTERRUPTED",
    "ROW_REFUSE_TIER",
    "ROW_REQUEST",
    "VmUpgradeRecoveryError",
    "call_with_route_retry",
    "cloud_mount_targets",
    "recover_vm_upgrade_seed",
    "vm_upgrade_consumer_row",
    "vm_upgrade_payload",
]
