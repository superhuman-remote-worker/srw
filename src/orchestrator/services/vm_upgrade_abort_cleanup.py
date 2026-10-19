"""The VM release of a failed workspace upgrade's abort (E4).

The abort route admits the VM's cleanup under its own source. When the route
can't finish (its release raised, or the agent stopped asking), that admission
stays open and refuses every later cleanup of the VM's disk, crash recovery's
among them. Crash recovery then finishes it with the route's own steps: the
same admission (its request ID derives from the owner, the VM identity and the
source), the same release, then completion.
"""

from __future__ import annotations

from typing import Any

from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
    completed_cleanup_outcome,
    vm_cleanup_kwargs,
    vm_cleanup_request_identity,
)

VM_UPGRADE_ABORT_CLEANUP_SOURCE = "abort_thread_vm_upgrade"


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
            # A quota-charged (v3) VM's permit completes only with the
            # provisioner's stop attestation.
            await complete_vm_cleanup_permit(
                recovery_store,
                cleanup,
                outcome=disposition,
                provisioner=vm_provisioner,
            )
    return disposition


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
    "VM_UPGRADE_ABORT_CLEANUP_SOURCE",
    "is_open_upgrade_abort_admission",
    "release_aborted_upgrade_vm",
]
