"""Dispatcher adapter for creation holds and the separate Ready release."""

from collections.abc import Mapping
import json
from uuid import UUID

from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore


async def handle_creation_pending(job, *, db):
    """True consumes this pass; False continues ordinary workspace preflight.

    Exactly adopted VMs still need phase/boot/cleanup policy while their worker
    hold remains. A Ready release consumes the stale job snapshot so the next
    pass repeats ordinary dispatch admission from freshly persisted context.
    """
    context = job.get("context") or {}
    context = json.loads(context) if isinstance(context, str) else context
    if not isinstance(context, Mapping) or "_vm_creation_pending" not in context:
        return False
    try:
        request_id = UUID(context["_vm_creation_pending"])
        vm = context.get("vm")
        if not isinstance(vm, Mapping):
            return True
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT job_id,provision_generation,state,observed_vm_uid,observed_pvc_uid FROM vm_creation_retries WHERE request_id=$1",
                request_id,
            )
        if (
            not row
            or row["state"] != "succeeded"
            or str(row["job_id"]) != str(job["id"])
            or str(row["provision_generation"]) != vm.get("provision_generation")
            or str(row["observed_vm_uid"]) != vm.get("vm_uid")
            or str(row["observed_pvc_uid"]) != vm.get("rootdisk_pvc_uid")
            or vm.get("identity_authenticated") is not True
        ):
            return True
        if vm.get("status") == "ready":
            await VMCreationReadinessStore(db).release(request_id=str(request_id))
            return True
        return False
    except (ValueError, TypeError, KeyError, AttributeError):
        return True
