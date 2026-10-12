"""Later Job disk purge after an immutable retained physical stop.

The old compute stop and charge remain unchanged. PostgreSQL validates the
complete retained predecessor chain and the final signed purge receipt.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from uuid import UUID

from shared.vm_resource_admission import ResourceAdmissionError

from orchestrator.services.vm_workspace_recovery_store import (
    CleanupPermit,
    bind_vm_cleanup_permit,
    cleanup_intent_digest,
    vm_cleanup_request_identity,
)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


async def _lock_owner(conn, owner: UUID, pvc: UUID | None = None) -> None:
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:job:{owner}",
    )
    if pvc is not None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )


async def execute_job_retained_disk_purge(
    store, provisioner, *, job_id, identity, permit
):
    """Actuate only the separately admitted permanent Delete and its exact proof."""
    if not permit.allowed:
        raise ResourceAdmissionError("retained_disk_purge_authority_held")
    candidate = await read_job_retained_disk_purge_candidate(store, permit)
    if permit.completed_outcome is not None:
        if permit.completed_outcome != "completed":
            raise ResourceAdmissionError("retained_disk_parent_unproven")
    else:
        if candidate.get("binding_kind") == "bound":
            # Workspace Release already deleted this exact bound generation;
            # the same signed physical absence still has to settle below.
            disposition = "completed"
        elif candidate.get("binding_kind") == "unbound":
            outcome = await provisioner.delete_vm_captured(
                job_id,
                identity,
                entity_type="job",
                purge_disk=True,
                parent_cleanup=permit.parent_cleanup,
            )
            disposition = outcome.disposition
        else:
            raise ResourceAdmissionError("retained_disk_binding_unproven")
        if disposition != "completed":
            raise ResourceAdmissionError("retained_disk_physical_purge_pending")
        await complete_job_retained_disk_purge(
            store, permit, outcome=disposition, provisioner=provisioner
        )


async def acquire_job_retained_disk_purge(store, *, job_id: str, identity):
    """Return None only when this Job has no typed retained physical stop.

    A recognized but incomplete chain returns a refusal or raises, never
    falls through to generic cleanup. The SQL validator owns all predecessor,
    workspace and charge authority checks under the same locks.
    """

    try:
        owner = UUID(job_id)
    except (TypeError, ValueError, AttributeError):
        return CleanupPermit(allowed=False, reason="retained_disk_identity_unproven")
    async with store.db.acquire() as conn, conn.transaction():
        await _lock_owner(conn, owner)
        recognized = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_resource_cleanup_stop_receipts s "
            "JOIN vm_workspace_cleanup_admissions c ON c.id=s.cleanup_admission_id "
            "WHERE s.job_id=$1 AND c.owner_kind='job' AND c.owner_id=$1 "
            "AND s.stop_evidence->>'pvc_disposition'='retained')",
            owner,
        )
        if not recognized:
            return None
        try:
            pvc = UUID(identity.rootdisk_pvc_uid)
            generation = UUID(identity.provision_generation)
            vm_uid = UUID(identity.vm_uid)
        except (TypeError, ValueError, AttributeError):
            return CleanupPermit(
                allowed=False, reason="retained_disk_identity_unproven"
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )
        if (
            await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
            is None
        ):
            return CleanupPermit(allowed=False, reason="retained_disk_owner_missing")
        owner_id, pvc_uid, request_id, digest, intent = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="public_vm_delete",
            purge_disk=True,
        )
        if owner_id != owner or pvc_uid != pvc:
            return CleanupPermit(
                allowed=False, reason="retained_disk_identity_unproven"
            )
        final = await conn.fetchrow(
            "SELECT r.request_id,r.canonical_request,s.vmi_uid,s.launcher_uid "
            "FROM vm_creation_retries r JOIN vm_resource_cleanup_stop_receipts s "
            "ON s.request_id=r.request_id "
            "WHERE r.owner_kind='job' AND r.job_id=$1 AND "
            "r.provision_generation=$2 AND r.observed_vm_uid=$3 "
            "AND r.observed_pvc_uid=$4 AND s.vm_uid=$3 AND s.pvc_uid=$4 "
            "AND s.stop_evidence->>'pvc_disposition'='retained'",
            owner,
            generation,
            vm_uid,
            pvc,
        )
        if final is None:
            return CleanupPermit(
                allowed=False, reason="retained_disk_final_stop_unproven"
            )
        request = _json(final["canonical_request"])
        binding = (
            request.get("workspace_storage") if isinstance(request, Mapping) else None
        )
        if binding is not None and not isinstance(binding, Mapping):
            return CleanupPermit(allowed=False, reason="retained_disk_binding_unproven")
        kind = "bound" if binding is not None else "unbound"
        try:
            instance_id = UUID(binding["uid"]) if binding is not None else None
            instance_generation = binding["generation"] if binding is not None else None
            if binding is not None and (
                type(instance_generation) is not int or instance_generation < 1
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return CleanupPermit(allowed=False, reason="retained_disk_binding_unproven")
        bootstrap = None
        retained_terminal = None
        if await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_resumes') IS NOT NULL"
        ):
            retained_terminal = await conn.fetchval(
                "SELECT t.id FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
                "JOIN vm_job_retained_resume_terminals t ON t.resume_id=op.id "
                "WHERE j.id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
                owner,
            )
        if await conn.fetchval(
            "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
        ):
            from orchestrator.services.vm_job_cancel_retention import (
                JobRetainedPurgeBootstrap,
            )

            protected = await conn.fetch(
                "SELECT cleanup_admission_id FROM vm_job_cancel_retention_authorities "
                "WHERE (job_id=$1 OR pvc_uid=$2) AND NOT "
                "public.vm_job_cancel_retention_discharged(cleanup_admission_id)",
                owner,
                pvc,
            )
            if protected:
                bootstrap = JobRetainedPurgeBootstrap(
                    job_id=owner,
                    pvc_uid=pvc,
                    final_request_id=final["request_id"],
                    provision_generation=generation,
                    cleanup_request_id=request_id,
                    intent_digest=digest,
                    retention_admission_ids=frozenset(
                        row["cleanup_admission_id"] for row in protected
                    ),
                )
        permit = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="job",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request_id,
            source="public_vm_delete",
            intent_digest=digest,
            _retained_purge_bootstrap=bootstrap,
        )
        if not permit.allowed:
            return permit
        bound = bind_vm_cleanup_permit(permit, request_id=request_id, intent=intent)
        if permit.completed_outcome is not None:
            # A completed historical parent without our immutable receipt is
            # invalid; the validator refuses it without a second delete.
            if (
                await conn.fetchval(
                    "SELECT public.validate_vm_job_retained_disk_purge($1,true)",
                    permit.admission_id,
                )
                is not True
            ):
                raise ResourceAdmissionError("retained_disk_receipt_unproven")
            return bound
        tail_column = ",retained_terminal_id" if retained_terminal is not None else ""
        tail_value = ",$14" if retained_terminal is not None else ""
        await conn.execute(
            "INSERT INTO vm_job_retained_disk_purge_authorities "
            "(cleanup_admission_id,job_id,final_request_id,provision_generation,"
            "vm_uid,vmi_uid,launcher_uid,pvc_uid,binding_kind,workspace_instance_id,"
            f"workspace_generation,cleanup_request_id,intent_digest{tail_column}) "
            f"VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13{tail_value}) "
            "ON CONFLICT DO NOTHING",
            permit.admission_id,
            owner,
            final["request_id"],
            generation,
            vm_uid,
            final["vmi_uid"],
            final["launcher_uid"],
            pvc,
            kind,
            instance_id,
            instance_generation,
            request_id,
            digest,
            *((retained_terminal,) if retained_terminal is not None else ()),
        )
        predecessors = await conn.fetch(
            "SELECT s.request_id,s.cleanup_admission_id,s.reservation_id,"
            "v.revision,'sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex') "
            "AS evidence_digest FROM vm_resource_cleanup_stop_receipts s "
            "JOIN vm_resource_reservations v ON v.id=s.reservation_id "
            "WHERE s.job_id=$1 AND s.stop_evidence->>'pvc_disposition'='retained' "
            "ORDER BY s.request_id",
            owner,
        )
        for predecessor in predecessors:
            await conn.execute(
                "INSERT INTO vm_job_retained_disk_purge_predecessors "
                "(cleanup_admission_id,source_request_id,old_cleanup_admission_id,"
                "reservation_id,reservation_revision,stop_evidence_digest) "
                "VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
                permit.admission_id,
                predecessor["request_id"],
                predecessor["cleanup_admission_id"],
                predecessor["reservation_id"],
                predecessor["revision"],
                predecessor["evidence_digest"],
            )
        if (
            await conn.fetchval(
                "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
                permit.admission_id,
            )
            is not True
        ):
            raise ResourceAdmissionError("retained_disk_authority_unproven")
        return bound


async def _candidate(conn, permit):
    proof = permit.parent_cleanup
    if not isinstance(proof, Mapping) or not isinstance(proof.get("intent"), Mapping):
        raise ResourceAdmissionError("retained_disk_parent_unproven")
    if (
        proof.get("admission_id") != str(permit.admission_id)
        or proof["intent"].get("source") != "public_vm_delete"
        or proof["intent"].get("owner_kind") != "job"
        or proof["intent"].get("purge_disk") is not True
        or cleanup_intent_digest(proof["intent"]) != proof.get("intent_digest")
    ):
        raise ResourceAdmissionError("retained_disk_parent_unproven")
    try:
        owner = UUID(proof["intent"]["owner_id"])
        pvc = UUID(proof["intent"]["pvc_uid"])
        request_id = UUID(proof["request_id"])
    except (KeyError, TypeError, ValueError):
        raise ResourceAdmissionError("retained_disk_parent_unproven") from None
    await _lock_owner(conn, owner, pvc)
    if await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner) is None:
        raise ResourceAdmissionError("retained_disk_owner_missing")
    parent = await conn.fetchrow(
        "SELECT owner_kind,owner_id,pvc_uid,source,request_id,intent_digest "
        "FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR UPDATE",
        permit.admission_id,
    )
    if (
        parent is None
        or parent["owner_kind"] != "job"
        or parent["owner_id"] != owner
        or parent["pvc_uid"] != pvc
        or parent["source"] != "public_vm_delete"
        or parent["request_id"] != request_id
        or parent["intent_digest"] != proof["intent_digest"]
    ):
        raise ResourceAdmissionError("retained_disk_parent_unproven")
    value = await conn.fetchval(
        "SELECT public.vm_job_retained_disk_purge_candidate($1)", permit.admission_id
    )
    candidate = _json(value)
    if not isinstance(candidate, dict):
        raise ResourceAdmissionError("retained_disk_candidate_unproven")
    return candidate


async def read_job_retained_disk_purge_candidate(store, permit):
    """Read the SQL-validated final physical target before an external effect."""

    async with store.db.acquire() as conn, conn.transaction():
        return await _candidate(conn, permit)


async def read_current_retained_purge(
    db, *, job_id, generation, vm_uid, pvc_uid, parent_cleanup=None, candidate=None
):
    """Prove the committed physical target of a typed logical-tail Delete.

    None is an unrelated owner. A recognized continuation must carry either its
    exact signed parent or its complete native attestation candidate. The native
    validator proves current Delete/no successor and every physical predecessor's
    process-zero receipt without rewriting the logical runtime projection.
    All locks end before the caller performs external I/O.
    """
    try:
        owner = UUID(str(job_id))
    except (TypeError, ValueError, AttributeError):
        return None
    async with db.acquire() as conn, conn.transaction():
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_resumes') IS NOT NULL"
        ):
            return None
        await _lock_owner(conn, owner)
        locator = await conn.fetchrow(
            "SELECT pvc_uid FROM vm_job_retained_resumes WHERE job_id=$1 LIMIT 1",
            owner,
        )
        if locator is None:
            return None
        await _lock_owner(conn, owner, locator["pvc_uid"])
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        # Re-read identity and authorization after every owner/PVC/queue/Job wait.
        rows = await conn.fetch(
            "SELECT d.* FROM vm_job_retained_disk_purge_authorities d "
            "WHERE d.job_id=$1 AND d.provision_generation::text=$2 "
            "AND d.vm_uid::text=$3 AND d.pvc_uid::text=$4 "
            "AND d.pvc_uid=$5 AND d.retained_terminal_id IS NOT NULL "
            "AND d.admitted_xact_id<>pg_current_xact_id() "
            "AND NOT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_predecessors p "
            "WHERE p.cleanup_admission_id=d.cleanup_admission_id "
            "AND p.admitted_xact_id=pg_current_xact_id())",
            owner,
            str(generation),
            str(vm_uid),
            str(pvc_uid),
            locator["pvc_uid"],
        )
        if len(rows) != 1:
            raise ResourceAdmissionError("retained_purge_committed_authority_unproven")
        authority = rows[0]
        intent = {
            "owner_kind": "job",
            "owner_id": str(owner),
            "source": "public_vm_delete",
            "resource": "vm_workspace",
            "provision_generation": str(authority["provision_generation"]),
            "vm_uid": str(authority["vm_uid"]),
            "pvc_uid": str(authority["pvc_uid"]),
            "purge_disk": True,
        }
        permit = bind_vm_cleanup_permit(
            CleanupPermit(allowed=True, admission_id=authority["cleanup_admission_id"]),
            request_id=authority["cleanup_request_id"],
            intent=intent,
        )
        if candidate is None and parent_cleanup != permit.parent_cleanup:
            raise ResourceAdmissionError("retained_purge_parent_changed")
        current = await _candidate(conn, permit)
        if candidate is not None and candidate != current:
            raise ResourceAdmissionError("retained_purge_candidate_changed")
        return current


async def has_job_retained_disk_purge_authority(store, admission_id):
    if admission_id is None:
        return False
    async with store.db.acquire() as conn:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_job_retained_disk_purge_authorities') "
            "IS NOT NULL"
        ):
            return False
        return bool(
            await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_authorities "
                "WHERE cleanup_admission_id=$1)",
                admission_id,
            )
        )


async def validate_job_retained_disk_parent(conn, admission_id):
    """Fence an exact child of a typed late-purge parent."""

    if (
        await conn.fetchval(
            "SELECT public.validate_vm_job_retained_disk_purge($1,false)", admission_id
        )
        is not True
    ):
        raise ResourceAdmissionError("retained_disk_parent_unproven")


async def complete_job_retained_disk_purge(store, permit, *, outcome, provisioner):
    if outcome != "completed" or provisioner is None:
        raise ResourceAdmissionError("retained_disk_purge_unproven")
    async with store.db.acquire() as conn, conn.transaction():
        candidate = await _candidate(conn, permit)
    evidence = await provisioner.attest_vm_cleanup_stop(candidate)
    if evidence is None:
        raise ResourceAdmissionError("retained_disk_purge_unproven")
    async with store.db.acquire() as conn, conn.transaction():
        current = await _candidate(conn, permit)
        if current != candidate:
            raise ResourceAdmissionError("retained_disk_candidate_changed")
        row = await conn.fetchrow(
            "SELECT purge_evidence,chain_digest FROM vm_job_retained_disk_purge_receipts "
            "WHERE cleanup_admission_id=$1",
            permit.admission_id,
        )
        if row is not None:
            if (
                _json(row["purge_evidence"]) != evidence
                or row["chain_digest"] != candidate["chain_digest"]
            ):
                raise ResourceAdmissionError("retained_disk_receipt_changed")
        else:
            await conn.execute(
                "INSERT INTO vm_job_retained_disk_purge_receipts "
                "(cleanup_admission_id,purge_evidence,chain_digest) "
                "VALUES($1,$2::jsonb,$3)",
                permit.admission_id,
                json.dumps(evidence),
                candidate["chain_digest"],
            )
        if (
            await conn.fetchval(
                "SELECT public.validate_vm_job_retained_disk_purge($1,true)",
                permit.admission_id,
            )
            is not True
        ):
            raise ResourceAdmissionError("retained_disk_receipt_unproven")
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions "
            "SET completed_at=clock_timestamp(),outcome='completed' "
            "WHERE id=$1 AND completed_at IS NULL",
            permit.admission_id,
        )
        if await conn.fetchval(
            "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
        ) and await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities WHERE job_id=$1 "
            "AND provision_generation=$2 AND public.vm_job_cancel_retention_discharged(cleanup_admission_id))",
            UUID(candidate["job_id"]),
            UUID(candidate["provision_generation"]),
        ):
            # Project only alongside the validated purge receipt and completion.
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm}',(context->'vm')||"
                '\'{"status":"deleted","compute_released":true,"disk_kept":false}\'::jsonb) '
                "WHERE id=$1 AND context->'vm'->>'provision_generation'=$2",
                UUID(candidate["job_id"]),
                candidate["provision_generation"],
            )
