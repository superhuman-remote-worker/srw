"""The stateless claimant's half of a workspace upgrade (C1).

The claim bundle carries the old lite workspace beside ``attach`` while the
Session still owes its copy. Before any control or input, the executor copies
it into the freshly attached container, commits the copy in the workspace Git
(so ``/undo`` of the first container turn keeps the carried files), records
that commit for the current transcript position, and clears ``seed_from``
under its lease. The control drain then journals ``workspace_upgrade.complete``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from agent.core.backends.overlay import unwrap_backend
from agent.core.backends.seed import seed_from_lite_workspace
from agent.services.cloud_sync.base import _should_ignore
from shared.workspace_upgrade import (
    WORKSPACE_UPGRADE_SEED_KEY,
    WORKSPACE_UPGRADE_SEED_VERSION,
)

logger = logging.getLogger(__name__)

# The file count of the last copy this process made, keyed by its claim; the
# control drain takes it once for the ``workspace_upgrade.complete`` receipt.
_SEED_RESULTS: dict[tuple[str, int], int] = {}


class WorkspaceUpgradeSeedError(RuntimeError):
    """The copy can't be made on this claim; the request stays pending."""


def claim_bundle_workspace_upgrade_seed(bundle: Mapping[str, Any]) -> Optional[dict]:
    """The seed spec the claim bundle carries beside ``attach``, or None."""
    value = bundle.get(WORKSPACE_UPGRADE_SEED_KEY)
    return dict(value) if isinstance(value, Mapping) else None


def cloud_synced_path(rel: str, targets: Iterable[str]) -> bool:
    """True when a cloud mount mirrors ``rel``; the first pull brings it back.

    Excluding these keeps the container's new cloud-sync baseline clean (spec
    "Seed before any input"). Paths the sync ignores are still copied.
    """
    for target in targets:
        target = str(target or "").strip("/")
        if target == "":
            inner = rel
        elif rel == target or rel.startswith(f"{target}/"):
            inner = rel[len(target) + 1 :]
        else:
            continue
        if not _should_ignore(inner):
            return True
    return False


def take_seed_result(thread_id: str, lease_token: int) -> Optional[int]:
    """The file count this claim copied, once, for the control receipt."""
    return _SEED_RESULTS.pop((str(thread_id), int(lease_token)), None)


def _validated(spec: Mapping[str, Any]) -> tuple[dict, dict]:
    seed_from = spec.get("seed_from")
    workspace = spec.get("workspace")
    if (
        spec.get("version") != WORKSPACE_UPGRADE_SEED_VERSION
        or not isinstance(seed_from, Mapping)
        or not isinstance(workspace, Mapping)
        or workspace.get("backend") not in ("virtual", "none")
    ):
        raise WorkspaceUpgradeSeedError("unsupported workspace upgrade seed")
    return dict(seed_from), dict(workspace)


async def seed_upgraded_workspace(
    session: Any, spec: Mapping[str, Any], *, thread_id: str, lease_token: int
) -> int:
    """Copy the old lite workspace into the attached container; clear the marker.

    Order matters: copy (verified), commit, record the commit, then clear
    ``seed_from`` under the lease. Any failure before the clear raises and
    leaves the marker, so the next claim copies again. A lost lease raises
    ``LeaseLostError`` from the fenced writes.
    """
    seed_from, workspace = _validated(spec)
    manager = getattr(session, "workspace_manager", None)
    backend = getattr(manager, "backend", None)
    if backend is None or not getattr(backend, "supports_shell", False):
        raise WorkspaceUpgradeSeedError("the upgraded workspace is not attached")
    sync = getattr(session, "workspace_sync", None)
    targets = [mount.target_path for mount in (sync.mounts if sync else [])]
    files = await asyncio.to_thread(
        seed_from_lite_workspace,
        workspace,
        owner_id=str(thread_id),
        destination=unwrap_backend(backend),
        exclude=lambda rel: cloud_synced_path(rel, targets),
    )
    git = getattr(manager, "git_manager", None)
    if git is not None and git.is_active:
        if not await asyncio.to_thread(
            git.commit, "Carry over files from the previous workspace"
        ):
            raise WorkspaceUpgradeSeedError("the carried files could not be committed")
        head = await asyncio.to_thread(git.get_current_commit)
        if not head:
            raise WorkspaceUpgradeSeedError("the carried files have no commit")
        await session.postgres_conn.record_turn_commit(str(thread_id), head)
    await session.postgres_conn.clear_workspace_upgrade_seed(
        str(thread_id), expected_seed_from=seed_from
    )
    _SEED_RESULTS.clear()
    _SEED_RESULTS[(str(thread_id), int(lease_token))] = int(files)
    logger.info(
        "workspace upgrade seed: thread=%s token=%d files=%d",
        thread_id,
        lease_token,
        files,
    )
    return int(files)


__all__ = [
    "WorkspaceUpgradeSeedError",
    "claim_bundle_workspace_upgrade_seed",
    "cloud_synced_path",
    "seed_upgraded_workspace",
    "take_seed_result",
]
