"""Exact completed-teardown recovery of an immutable non-quota VM refusal.

This is a new, durably linked completion, never a retry of destructive effects.
The old admission and its controller child remain byte-preserved.
"""

import json
from uuid import NAMESPACE_URL, UUID, uuid5

from orchestrator.services.vm_workspace_recovery_store import (
    CleanupPermit,
    bind_vm_cleanup_permit,
    completed_cleanup_outcome,
)


async def recover_completed_thread_vm_refusal(store, provisioner, permit, retirement):
    if completed_cleanup_outcome(permit) != "identity_superseded":
        return permit
    proof = permit.parent_cleanup
    if not proof or proof["intent"].get("purge_disk") is not True:
        return permit
    intent = proof["intent"]
    if intent.get("source") != "pinned_thread_retirement":
        return permit
    owner, pvc = UUID(intent["owner_id"]), UUID(intent["pvc_uid"])
    generation, token = UUID(retirement["generation"]), UUID(retirement["token"])
    vm = retirement["context"].get("vm") or {}
    candidate = {
        "owner_kind": "thread",
        "owner_id": str(owner),
        "provision_generation": intent["provision_generation"],
        "vm_uid": intent["vm_uid"],
        "pvc_uid": str(pvc),
        "vmi_uid": vm.get("vmi_uid"),
        "launcher_uid": vm.get("active_pod_uid"),
        "purge_disk": True,
    }
    if not await store.db.managed_repository_workspace_process_zero_is_current(
        str(owner),
        owner_kind="thread",
        scope="vm",
        provisioner="vm",
        runtime_incarnation=intent["provision_generation"],
    ):
        return permit
    # Fresh authenticated VM/VMI/launcher/disk absence outside database locks.
    # No deletion, endpoint discovery or recording of process-zero occurs here.
    evidence = await provisioner.attest_vm_cleanup_stop(candidate)
    if evidence is None:
        return permit
    request_id = uuid5(
        NAMESPACE_URL,
        f"vm-thread-cleanup-refusal-recovery:{permit.admission_id}:{generation}:{token}",
    )
    async with store.db.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:thread:{owner}",
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )
        await conn.fetchrow("SELECT id FROM threads WHERE id=$1 FOR UPDATE", owner)
        # Immutable evidence references the adopted source and its exact child,
        # never a reusable resource name or a caller-supplied successor UID.
        source_id = await conn.fetchval(
            "SELECT request_id FROM vm_creation_retries WHERE owner_kind='thread' "
            "AND thread_id=$1 AND provision_generation=$2 FOR UPDATE",
            owner,
            UUID(intent["provision_generation"]),
        )
        child_id = await conn.fetchval(
            "SELECT id FROM vm_workspace_cleanup_admissions "
            "WHERE parent_admission_id=$1 AND source='controller_rootdisk_delete' "
            "AND completed_at IS NOT NULL AND outcome='deleted' "
            "ORDER BY admitted_at,id LIMIT 1 FOR UPDATE",
            permit.admission_id,
        )
        if source_id is None or child_id is None:
            return permit
        # The ordinary admission retains access/recovery/other-cleanup holds.
        successor = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="thread",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request_id,
            source=intent["source"],
            intent_digest=proof["intent_digest"],
            revalidate_completed=True,
        )
        if not successor.allowed:
            return permit
        receipt = await conn.fetchrow(
            "SELECT * FROM vm_thread_cleanup_refusal_recoveries "
            "WHERE refused_admission_id=$1 FOR UPDATE",
            permit.admission_id,
        )
        if receipt is None:
            # The insertion trigger checks current G/T, source, actor, captured
            # context, child completion, process-zero and physical proof again.
            await conn.execute(
                "INSERT INTO vm_thread_cleanup_refusal_recoveries "
                "(refused_admission_id,successor_admission_id,child_admission_id,"
                "creation_request_id,thread_id,runtime_generation,retirement_token,"
                "provision_generation,vm_uid,pvc_uid,physical_stop) "
                "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)",
                permit.admission_id,
                successor.admission_id,
                child_id,
                source_id,
                owner,
                generation,
                token,
                UUID(intent["provision_generation"]),
                UUID(intent["vm_uid"]),
                pvc,
                json.dumps(evidence),
            )
        elif (
            receipt["successor_admission_id"] != successor.admission_id
            or receipt["runtime_generation"] != generation
            or receipt["retirement_token"] != token
        ):
            raise RuntimeError("VM cleanup refusal recovery authority changed")
        # Validate replay under the same owner locks as initial admission. A
        # stored recovery does not authorize settling a successor runtime.
        await conn.fetchval(
            "SELECT public.validate_vm_thread_cleanup_refusal_recovery(r) "
            "FROM vm_thread_cleanup_refusal_recoveries r WHERE refused_admission_id=$1",
            permit.admission_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
            "outcome='completed' WHERE id=$1 AND completed_at IS NULL",
            successor.admission_id,
        )
    return bind_vm_cleanup_permit(
        CleanupPermit(
            True,
            successor.admission_id,
            completed_outcome="completed",
            request_id=request_id,
        ),
        request_id=request_id,
        intent=intent,
    )
