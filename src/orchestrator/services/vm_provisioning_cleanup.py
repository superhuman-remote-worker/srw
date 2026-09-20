"""Exact provisioning cleanup with durable, bounded retry frequency."""

from __future__ import annotations

import logging
from typing import Any

from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
    completed_cleanup_outcome,
)

logger = logging.getLogger(__name__)


async def recycle_provisioning_vm(
    job_id: str,
    vm: dict[str, Any],
    *,
    db: Any,
    provisioner: Any,
    recovery_store: Any,
    now: float,
) -> str:
    """Retire one captured generation, retaining its disk and any cleanup hold.

    Incomplete retirement does not spend a VM boot attempt or complete its
    admission. Persist a separate retry clock so a late guest can still recover
    after a restart, without attempting SSH on every dispatcher tick.
    """
    generation = vm.get("provision_generation")
    if not generation:
        return "identity_invalid"
    try:
        identity = await provisioner.capture_vm_teardown_identity(
            job_id, entity_type="job"
        )
        if identity.provision_generation != generation:
            return "identity_superseded"
        # Write before acquiring the durable permit: a rejected CAS must not
        # strand an admission for a generation the dispatcher no longer owns.
        # The delete transport can publish `deleted` before physical absence;
        # this marker keeps replacement fenced until admission completion.
        if not await db.merge_vm_context_if_provision_generation(
            job_id, generation, {"retirement_cleanup_pending": True}
        ):
            return "authority_changed"
        cleanup = await acquire_vm_cleanup_permit(
            recovery_store,
            owner_kind="job",
            owner_id=job_id,
            identity=identity,
            source="dispatcher_vm_recycle",
            purge_disk=False,
        )
        if not cleanup.allowed:
            disposition = "recovery_held"
        else:
            disposition = completed_cleanup_outcome(cleanup)
            if disposition is None:
                outcome = await provisioner.release_vm_captured(
                    job_id,
                    identity,
                    entity_type="job",
                    purge_disk=False,
                    capture_snapshot=False,
                )
                disposition = outcome.disposition
                if disposition in {"completed", "identity_superseded"}:
                    await complete_vm_cleanup_permit(
                        recovery_store, cleanup, outcome=disposition
                    )
            if disposition in {"completed", "identity_superseded"}:
                await db.merge_vm_context_if_provision_generation(
                    job_id,
                    generation,
                    {
                        "retirement_cleanup_pending": False,
                        "retirement_last_result": None,
                        "retirement_retry_after": None,
                        **({"status": "deleted"} if disposition == "completed" else {}),
                    },
                )
                return disposition
    except Exception as exc:
        # Controller exceptions can contain signed URLs. Record only the
        # exception class and a fixed diagnostic, never transport credentials.
        logger.warning(
            "VM cleanup for job %s unavailable (%s)", job_id, type(exc).__name__
        )
        disposition = "cleanup_unavailable"

    attempts = vm.get("retirement_attempts")
    attempts = attempts if type(attempts) is int and attempts >= 0 else 0
    delay = min(60 * 2 ** min(attempts, 3), 300)
    await db.merge_vm_context_if_provision_generation(
        job_id,
        generation,
        {
            "retirement_attempts": attempts + 1,
            "retirement_last_result": disposition,
            "retirement_retry_after": now + delay,
        },
    )
    logger.warning(
        "VM cleanup for job %s blocked (%s); disk retained, retry in %ss",
        job_id,
        disposition,
        delay,
    )
    return disposition
