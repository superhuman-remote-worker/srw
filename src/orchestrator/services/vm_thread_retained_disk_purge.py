"""Permanent disposition of the exact disk retained by a settled soft VM End.

0295 owns the immutable compute debit. This protocol only authorizes and proves
its later disk purge, under the new permanent retirement token.
"""

import json
from uuid import NAMESPACE_URL, UUID, uuid5

from shared.vm_resource_admission import ResourceAdmissionError
from orchestrator.services.vm_resource_reservation_store import _json
from orchestrator.services.vm_workspace_recovery_store import (
    CleanupPermit,
    bind_vm_cleanup_permit,
    cleanup_intent_digest,
)

SOURCE = "pinned_thread_retained_disk_purge"


async def _lock_owner(conn, owner, pvc):
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:thread:{owner}",
    )
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery-pvc:{pvc}",
    )


def _intent(a, generation, token):
    return {
        "owner_kind": "thread",
        "owner_id": str(a["thread_id"]),
        "provision_generation": str(a["provision_generation"]),
        "vm_uid": str(a["vm_uid"]),
        "pvc_uid": str(a["pvc_uid"]),
        "purge_disk": True,
        "resource": "vm_workspace",
        "source": SOURCE,
        "runtime_generation": str(generation),
        "retirement_token": str(token),
        "compute_cleanup_admission_id": str(a["cleanup_admission_id"]),
    }


async def acquire_retained_disk_purge(store, *, thread_id, identity, generation, token):
    """None means legacy/non-v3; a refused recognized source never falls through."""
    owner, pvc = UUID(thread_id), UUID(identity.rootdisk_pvc_uid)
    generation, token = UUID(generation), UUID(token)
    async with store.db.acquire() as conn, conn.transaction():
        await _lock_owner(conn, owner, pvc)
        thread = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", owner
        )
        if thread is None:
            return CleanupPermit(allowed=False, reason="retained_disk_owner_missing")
        vm = _json(thread["metadata"]).get("vm") or {}
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id::text=$1 "
            "AND owner_kind='thread' AND thread_id=$2 FOR UPDATE",
            vm.get("creation_request_id"),
            owner,
        )
        if (
            source is None
            or _json(source["controller_configuration"]).get("version") != 3
        ):
            # Missing/replaced metadata is not legacy when an adopted v3 source
            # exists. This query recognizes refusal only; it selects no authority.
            recognized = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE owner_kind='thread' "
                "AND thread_id=$1 AND controller_configuration->>'version'='3')",
                owner,
            )
            return (
                CleanupPermit(allowed=False, reason="retained_disk_source_unproven")
                if recognized
                else None
            )
        a = await conn.fetchrow(
            "SELECT * FROM vm_resource_thread_cleanup_authorities "
            "WHERE request_id=$1 AND runtime_generation=$2 AND NOT purge_disk",
            source["request_id"],
            generation,
        )
        if a is None or any(
            str(a[key]) != str(value)
            for key, value in (
                ("provision_generation", identity.provision_generation),
                ("vm_uid", identity.vm_uid),
                ("pvc_uid", pvc),
            )
        ):
            return CleanupPermit(
                allowed=False, reason="retained_disk_predecessor_unproven"
            )
        intent = _intent(a, generation, token)
        digest = cleanup_intent_digest(intent)
        request_id = uuid5(NAMESPACE_URL, f"vm-thread-retained-disk-purge:{digest}")
        permit = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="thread",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request_id,
            source=SOURCE,
            intent_digest=digest,
        )
        if not permit.allowed:
            return permit
        await conn.execute(
            "INSERT INTO vm_thread_retained_disk_purge_authorities "
            "(cleanup_admission_id,compute_cleanup_admission_id,runtime_generation,retirement_token,"
            "source_revision,cleanup_request_id,intent_digest,retirement_context) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
            permit.admission_id,
            a["cleanup_admission_id"],
            generation,
            token,
            source["revision"],
            request_id,
            digest,
            thread["runtime_retirement_context"],
        )
        bound = bind_vm_cleanup_permit(permit, request_id=request_id, intent=intent)
        await _validated_candidate(
            conn, permit.admission_id, proof=bound.parent_cleanup
        )
        return bound


async def _validated_candidate(conn, admission_id, *, proof=None):
    row = await conn.fetchrow(
        "SELECT d.*,a.thread_id,a.provision_generation,a.vm_uid,a.pvc_uid,a.vmi_uid,a.launcher_uid,"
        "c.completed_at,c.outcome,p.purge_evidence "
        "FROM vm_thread_retained_disk_purge_authorities d "
        "JOIN vm_resource_thread_cleanup_authorities a ON a.cleanup_admission_id=d.compute_cleanup_admission_id "
        "JOIN vm_workspace_cleanup_admissions c ON c.id=d.cleanup_admission_id "
        "LEFT JOIN vm_thread_retained_disk_purge_receipts p ON p.cleanup_admission_id=d.cleanup_admission_id "
        "WHERE d.cleanup_admission_id=$1",
        admission_id,
    )
    if row is None:
        raise ResourceAdmissionError("retained_disk_authority_unproven")
    await _lock_owner(conn, row["thread_id"], row["pvc_uid"])
    # The initial read is only a locator for immutable authority. Re-read the
    # completion state after the shared validator has taken the owner lock.
    await conn.fetchval(
        "SELECT public.validate_vm_thread_retained_disk_purge(d,true) "
        "FROM vm_thread_retained_disk_purge_authorities d WHERE cleanup_admission_id=$1",
        admission_id,
    )
    a = {**dict(row), "cleanup_admission_id": row["compute_cleanup_admission_id"]}
    intent = _intent(a, row["runtime_generation"], row["retirement_token"])
    if proof is not None and proof != {
        "admission_id": str(admission_id),
        "request_id": str(row["cleanup_request_id"]),
        "intent_digest": cleanup_intent_digest(intent),
        "intent": intent,
    }:
        raise ResourceAdmissionError("retained_disk_intent_unproven")
    state = await conn.fetchrow(
        "SELECT c.completed_at,c.outcome,p.purge_evidence FROM vm_workspace_cleanup_admissions c "
        "LEFT JOIN vm_thread_retained_disk_purge_receipts p ON p.cleanup_admission_id=c.id WHERE c.id=$1",
        admission_id,
    )
    if state["purge_evidence"] is not None:
        await conn.fetchval(
            "SELECT public.validate_vm_thread_retained_disk_purge_receipt(d,$2::jsonb,true) "
            "FROM vm_thread_retained_disk_purge_authorities d WHERE cleanup_admission_id=$1",
            admission_id,
            state["purge_evidence"],
        )
    if state["completed_at"] is not None and (
        state["outcome"] != "completed" or state["purge_evidence"] is None
    ):
        raise ResourceAdmissionError("retained_disk_receipt_unproven")
    candidate = {
        "owner_kind": "thread",
        "owner_id": str(row["thread_id"]),
        "purge_disk": True,
        **{
            key: str(row[key]) if row[key] is not None else None
            for key in (
                "provision_generation",
                "vm_uid",
                "pvc_uid",
                "vmi_uid",
                "launcher_uid",
            )
        },
    }
    return candidate, state


async def validate_retained_disk_parent(conn, admission_id):
    """Under caller transaction; used for this source's child and carrier only."""
    await _validated_candidate(conn, admission_id)


async def complete_retained_disk_purge(store, permit, *, outcome, provisioner):
    if outcome != "completed":
        raise ResourceAdmissionError("retained_disk_purge_unproven")
    async with store.db.acquire() as conn, conn.transaction():
        candidate, state = await _validated_candidate(
            conn, permit.admission_id, proof=permit.parent_cleanup
        )
        if state["completed_at"] is not None:
            return
    if provisioner is None:
        raise ResourceAdmissionError("retained_disk_purge_unproven")
    # No owner transaction spans the fresh authenticated controller observation.
    evidence = await provisioner.attest_vm_cleanup_stop(candidate)
    if evidence is None:
        raise ResourceAdmissionError("retained_disk_purge_unproven")
    async with store.db.acquire() as conn, conn.transaction():
        _, state = await _validated_candidate(
            conn, permit.admission_id, proof=permit.parent_cleanup
        )
        await conn.fetchval(
            "SELECT public.validate_vm_thread_retained_disk_purge_receipt(d,$2::jsonb,true) "
            "FROM vm_thread_retained_disk_purge_authorities d WHERE cleanup_admission_id=$1",
            permit.admission_id,
            json.dumps(evidence),
        )
        if state["purge_evidence"] is not None:
            if _json(state["purge_evidence"]) != evidence:
                raise ResourceAdmissionError("retained_disk_receipt_changed")
        else:
            await conn.execute(
                "INSERT INTO vm_thread_retained_disk_purge_receipts(cleanup_admission_id,purge_evidence) "
                "VALUES($1,$2::jsonb)",
                permit.admission_id,
                json.dumps(evidence),
            )
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='completed' "
            "WHERE id=$1 AND completed_at IS NULL",
            permit.admission_id,
        )
