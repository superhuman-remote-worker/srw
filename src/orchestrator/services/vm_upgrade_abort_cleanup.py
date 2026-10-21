"""The VM release of a failed workspace upgrade's abort (E4).

The abort route admits the VM's cleanup under its own source. When the route
can't finish (its release raised, or the agent stopped asking), that admission
stays open and refuses every later cleanup of the VM's disk, crash recovery's
among them. Crash recovery then finishes it with the route's own steps: the
same admission (its request ID derives from the owner, the VM identity and the
source), the same release, then completion.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping

from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
    completed_cleanup_outcome,
    vm_cleanup_kwargs,
    vm_cleanup_request_identity,
)

VM_UPGRADE_ABORT_CLEANUP_SOURCE = "abort_thread_vm_upgrade"

# E4b: End's VM cleanup candidate carries this key when its Session's own
# completed abort already purged the disk (SQL
# vm_thread_cleanup_disk_purged_by_upgrade_abort, 0557).
DISK_PURGED_BY_UPGRADE_ABORT = "disk_purged_by_upgrade_abort"


def cleanup_stop_pvc_disposition(candidate: Mapping[str, Any]) -> str:
    """The disk disposition a cleanup stop proves for ``candidate``."""
    if candidate["purge_disk"]:
        return "purged"
    if candidate.get(DISK_PURGED_BY_UPGRADE_ABORT) is True:
        return "purged_by_upgrade_abort"
    return "retained"


async def release_aborted_upgrade_vm(
    recovery_store: Any, vm_provisioner: Any, thread_id: str, identity: Any
) -> str:
    """Release the upgrade's VM and its disk under the abort's admission.

    Returns the release disposition; ``completed`` and ``identity_superseded``
    complete the admission. Raises when workspace recovery holds the VM.
    """
    cleanup = await acquire_vm_cleanup_permit(
        recovery_store,
        owner_kind="thread",
        owner_id=thread_id,
        identity=identity,
        source=VM_UPGRADE_ABORT_CLEANUP_SOURCE,
        purge_disk=True,
    )
    if not cleanup.allowed:
        raise RuntimeError("VM cleanup held for workspace recovery")
    disposition = completed_cleanup_outcome(cleanup)
    if disposition is None:
        outcome = await vm_provisioner.release_vm_captured(
            thread_id,
            identity,
            entity_type="thread",
            purge_disk=True,
            capture_snapshot=False,
            **vm_cleanup_kwargs(cleanup),
        )
        disposition = outcome.disposition
        if disposition in {"completed", "identity_superseded"}:
            # Completes the admission only: a Session VM's resource charge is
            # released by End's own VM cleanup (see undo_thread_vm_upgrade).
            await complete_vm_cleanup_permit(
                recovery_store, cleanup, outcome=disposition
            )
    return disposition


async def upgrade_abort_purged_disk(conn: Any, authority: Mapping[str, Any]) -> bool:
    """Whether its Session's own completed abort purged this End's VM disk (E4b).

    ``authority`` is End's ``vm_resource_thread_cleanup_authorities`` row. SQL
    ``vm_thread_cleanup_disk_purged_by_upgrade_abort`` (0557) is the same test
    without the request ID and validates the stop, so a disagreement fails
    closed.
    """
    if authority["purge_disk"]:
        return False
    identity = SimpleNamespace(
        provision_generation=str(authority["provision_generation"]),
        vm_uid=str(authority["vm_uid"]),
        rootdisk_pvc_uid=str(authority["pvc_uid"]),
    )
    owner, pvc, request_id, intent_digest, _ = vm_cleanup_request_identity(
        owner_kind="thread",
        owner_id=authority["thread_id"],
        identity=identity,
        source=VM_UPGRADE_ABORT_CLEANUP_SOURCE,
        purge_disk=True,
    )
    return bool(
        await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='thread' AND owner_id=$1 AND pvc_uid=$2 AND source=$3 "
            "AND request_id=$4 AND intent_digest=$5 AND parent_admission_id IS NULL "
            "AND completed_at IS NOT NULL AND outcome='completed')",
            owner,
            pvc,
            VM_UPGRADE_ABORT_CLEANUP_SOURCE,
            request_id,
            intent_digest,
        )
    )


async def is_open_upgrade_abort_admission(
    recovery_store: Any, thread_id: str, identity: Any, admission_id: Any
) -> bool:
    """Whether ``admission_id`` is the abort's own open admission for this VM."""
    if admission_id is None:
        return False
    _, _, request_id, intent_digest, _ = vm_cleanup_request_identity(
        owner_kind="thread",
        owner_id=thread_id,
        identity=identity,
        source=VM_UPGRADE_ABORT_CLEANUP_SOURCE,
        purge_disk=True,
    )
    async with recovery_store.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT source, request_id, intent_digest, completed_at "
            "FROM vm_workspace_cleanup_admissions WHERE id=$1",
            admission_id,
        )
    return bool(
        row is not None
        and row["completed_at"] is None
        and row["source"] == VM_UPGRADE_ABORT_CLEANUP_SOURCE
        and row["request_id"] == request_id
        and row["intent_digest"] == intent_digest
    )


__all__ = [
    "DISK_PURGED_BY_UPGRADE_ABORT",
    "VM_UPGRADE_ABORT_CLEANUP_SOURCE",
    "cleanup_stop_pvc_disposition",
    "is_open_upgrade_abort_admission",
    "release_aborted_upgrade_vm",
    "upgrade_abort_purged_disk",
]
