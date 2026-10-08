"""Explicit owner Resume continues an immutable same-PVC Job keep."""

from __future__ import annotations

import json
from uuid import UUID, uuid4, uuid5

from orchestrator.services.vm_job_cancel_retention import (
    cancel_retention_admission_enabled,
)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


async def operation_on_conn(conn, job, *, allow_terminal=False):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    context = _json(job["context"]) or {}
    operation_id = context.get("_vm_job_retained_resume")
    if operation_id is None:
        return None
    if not await installed(conn):
        raise VMCreationRetryConflict("retained_resume_unproven")
    operation = await conn.fetchrow(
        "SELECT * FROM vm_job_retained_resumes WHERE id::text=$1 AND job_id=$2",
        operation_id,
        job["id"],
    )
    if operation is None or (
        not allow_terminal
        and await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_retained_resume_terminals WHERE resume_id=$1)",
            operation["id"],
        )
    ):
        raise VMCreationRetryConflict("retained_resume_terminal")
    if not await conn.fetchval(
        "SELECT public.vm_job_cancel_retention_settled($1) AND NOT "
        "public.vm_job_cancel_retention_discharged($2)",
        operation["physical_cleanup_admission_id"],
        operation["root_retention_admission_id"],
    ):
        raise VMCreationRetryConflict("retained_resume_predecessor_changed")
    return operation


async def retained_resume_predecessor_on_conn(conn, job):
    """Resolve logical continuation to its immutable, physical kept ancestor."""
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    operation = await operation_on_conn(conn, job)
    if operation is None:
        return None
    retained_vm = _json(operation["retained_vm"])
    proposal = {
        "expected_pvc_uid": str(operation["pvc_uid"]),
        "predecessor_cleanup_admission_id": str(
            operation["physical_cleanup_admission_id"]
        ),
        "predecessor_evidence": {
            "provision_generation": retained_vm["provision_generation"],
            "vm_uid": retained_vm["vm_uid"],
        },
    }
    # This is a read-only view of immutable evidence, not an owner projection
    # rewrite. The existing physical validator still proves receipt and intent.
    evidence, cleanup_id = await VMCreationRetryStore._own_predecessor(
        conn,
        {**dict(job), "context": {"last_vm": retained_vm}},
        operation["pvc_uid"],
        proposal,
    )
    return {
        **proposal,
        "predecessor_evidence": evidence,
        "predecessor_cleanup_admission_id": str(cleanup_id),
    }


async def complete_source_absent_cancel(db, job_id, *, clear_pending=True):
    """Close an accepted Resume with no source; never borrow its disk's VM zero."""
    owner = UUID(str(job_id))
    async with db.acquire() as conn, conn.transaction():
        if not await installed(conn):
            return None
        locator = await conn.fetchrow(
            "SELECT op.* FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
            "WHERE j.id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
            owner,
        )
        if locator is None:
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{locator['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner)
        if job is None or (_json(job["context"]) or {}).get(
            "_vm_job_retained_resume"
        ) != str(locator["id"]):
            return False
        terminal = await conn.fetchrow(
            "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
            locator["id"],
        )
        if terminal is not None:
            if terminal["terminal_kind"] != "source_absent":
                return None
            if not await conn.fetchval(
                "SELECT public.vm_job_retained_terminal_is_current($1)", owner
            ):
                return False
            await conn.execute(
                "UPDATE jobs SET context=CASE WHEN jsonb_typeof(context->'vm')='object' "
                "THEN jsonb_set(context,'{vm,status}','\"deleted\"'::jsonb) ELSE context END WHERE id=$1",
                owner,
            )
            if clear_pending:
                await conn.execute(
                    "UPDATE jobs SET context=context-'_stateless_cancel_cleanup_pending'-'_vm_creation_pending',"
                    "updated_at=clock_timestamp() WHERE id=$1",
                    owner,
                )
            return True
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE request_id=$1 "
            "OR (owner_kind='job' AND job_id=$2 AND provision_generation=$3))",
            locator["request_id"],
            owner,
            locator["provision_generation"],
        ):
            return None
        evidence = await conn.fetchval(
            "SELECT public.vm_job_retained_source_absent_evidence($1)", locator["id"]
        )
        if evidence is None:
            return False
        await conn.execute(
            "INSERT INTO vm_job_retained_resume_terminals "
            "(id,resume_id,allocated_request_id,terminal_kind,physical_cleanup_admission_id,evidence) "
            "VALUES($1,$2,$3,'source_absent',$4,$5::jsonb)",
            uuid5(locator["id"], "terminal"),
            locator["id"],
            locator["request_id"],
            locator["physical_cleanup_admission_id"],
            evidence if isinstance(evidence, str) else json.dumps(evidence),
        )
        await conn.execute(
            "UPDATE jobs SET context=CASE WHEN jsonb_typeof(context->'vm')='object' "
            "THEN jsonb_set(context,'{vm,status}','\"deleted\"'::jsonb) ELSE context END WHERE id=$1",
            owner,
        )
        if clear_pending:
            await conn.execute(
                "UPDATE jobs SET context=context-'_stateless_cancel_cleanup_pending'-'_vm_creation_pending',"
                "updated_at=clock_timestamp() WHERE id=$1",
                owner,
            )
        return True


async def installed(conn):
    return await conn.fetchval(
        "SELECT to_regclass('public.vm_job_retained_resumes') IS NOT NULL"
    )


async def prepare_owner_resume(
    db,
    *,
    job_id,
    context_merge,
    expected_status,
    requested_by,
    completion_commands_enabled,
    lift_operator_pause_hold,
):
    """None is unrelated; any recognized protected owner must hold or continue.

    This branch acquires the canonical disk scope before the queue/Job locks.
    Only the public access-checked owner writer supplies requested_by; generic
    Resume cannot shed the old VM and manufacture creation authority.
    """
    from orchestrator.database.postgres import _stateless_resume_context
    from shared.worker_queue import hold_worker_batch_for_preflight

    class ResumeChanged(Exception):
        pass

    try:
        async with db.acquire() as conn, conn.transaction():
            if not await conn.fetchval(
                "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
            ):
                return None
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery:job:{job_id}",
            )
            roots = await conn.fetch(
                "SELECT * FROM vm_job_cancel_retention_authorities WHERE job_id=$1 "
                "AND NOT public.vm_job_cancel_retention_discharged(cleanup_admission_id) "
                "ORDER BY admitted_at,cleanup_admission_id",
                job_id,
            )
            if not roots:
                return None
            if requested_by is None or not cancel_retention_admission_enabled():
                return False
            if not await installed(conn):
                return False
            root = roots[0]
            if any(row["pvc_uid"] != root["pvc_uid"] for row in roots):
                return False
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery-pvc:{root['pvc_uid']}",
            )
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
            )
            job = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id
            )
            if (
                job is None
                or job["status"] != expected_status
                or job["user_id"] != UUID(str(requested_by))
            ):
                return False
            if (
                completion_commands_enabled
                and await db._completion_resume_blocked_on_conn(conn, job_id)
            ):
                return False
            context = _json(job["context"]) or {}
            previous_id = context.get("_vm_job_retained_resume")
            terminal = None
            retained_vm = context.get("vm")
            if previous_id is not None:
                previous = await conn.fetchrow(
                    "SELECT * FROM vm_job_retained_resumes WHERE id::text=$1 AND job_id=$2",
                    previous_id,
                    job_id,
                )
                terminal = await conn.fetchrow(
                    "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
                    previous["id"] if previous else None,
                )
                if terminal is None:
                    return False
                physical = await conn.fetchrow(
                    "SELECT * FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
                    terminal["physical_cleanup_admission_id"],
                )
                retained_vm = (
                    context.get("vm")
                    if terminal["terminal_kind"] == "kept_compute"
                    else _json(previous["retained_vm"])
                )
            else:
                physical = root
            if not isinstance(retained_vm, dict) or physical is None:
                return False
            source = await conn.fetchrow(
                "SELECT revision FROM vm_creation_retries WHERE request_id=$1",
                physical["creation_request_id"],
            )
            if source is None:
                return False
            merged = _stateless_resume_context(context_merge)
            operation = uuid4()
            await conn.execute(
                "INSERT INTO vm_job_retained_resumes "
                "(id,job_id,root_retention_admission_id,physical_cleanup_admission_id,"
                "predecessor_terminal_id,predecessor_request_id,predecessor_generation,"
                "pvc_uid,request_id,provision_generation,explicit_resume_id,requested_by,"
                "source_revision,retained_vm) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)",
                operation,
                job_id,
                root["cleanup_admission_id"],
                physical["cleanup_admission_id"],
                terminal["id"] if terminal else None,
                physical["creation_request_id"],
                physical["provision_generation"],
                root["pvc_uid"],
                uuid5(operation, "source"),
                uuid5(operation, "provision"),
                UUID(merged["worker_resume_id"]),
                UUID(str(requested_by)),
                source["revision"],
                json.dumps(retained_vm),
            )
            context.pop("vm", None)
            context.pop("_vm_creation_pending", None)
            context["last_vm"] = retained_vm
            context["_vm_job_retained_resume"] = str(operation)
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                job_id,
                json.dumps(context),
            )
            await hold_worker_batch_for_preflight(conn, job_id=job_id)
            row = await db._queue_job_for_resume_on_conn(
                conn,
                job_id,
                merged,
                void_completion_decision=True,
                stateless_only=True,
                expected_status=expected_status,
                completion_commands_enabled=completion_commands_enabled,
                lift_operator_pause_hold=lift_operator_pause_hold,
            )
            if row is None:
                raise ResumeChanged
            return True
    except ResumeChanged:
        return False


async def read_current_ready_preflight(db, parent_cleanup, *, job_id, generation):
    """None means legacy; a recognized Ready continuation must prove its permit.

    Every call finishes its transaction before the caller starts SSH or status
    transport. Recognition precedes inspecting supplied proof, so omission does
    not fall through to the older release path.
    """
    from shared.vm_resource_admission import ResourceAdmissionError

    owner = UUID(str(job_id))
    async with db.acquire() as conn, conn.transaction():
        if not await installed(conn):
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        locator = await conn.fetchrow(
            "SELECT op.id,op.pvc_uid FROM vm_job_retained_resumes op "
            "JOIN jobs j ON j.id=op.job_id JOIN vm_creation_retries r ON r.job_retained_resume_id=op.id "
            "WHERE op.job_id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text "
            "AND r.ready_at IS NOT NULL",
            owner,
        )
        if locator is None:
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{locator['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        if (
            isinstance(parent_cleanup, dict)
            and isinstance(parent_cleanup.get("intent"), dict)
            and parent_cleanup["intent"].get("purge_disk") is True
        ):
            from orchestrator.services.vm_workspace_recovery_store import (
                CleanupPermit,
                bind_vm_cleanup_permit,
            )

            purge = await conn.fetchrow(
                "SELECT d.*,c.request_id,c.intent_digest AS parent_digest FROM vm_job_retained_disk_purge_authorities d "
                "JOIN vm_workspace_cleanup_admissions c ON c.id=d.cleanup_admission_id "
                "WHERE d.cleanup_admission_id::text=$1 AND d.job_id=$2 AND d.pvc_uid=$3 "
                "AND d.provision_generation::text=$4 AND d.retained_terminal_id IS NOT NULL "
                "AND d.admitted_xact_id<>pg_current_xact_id() "
                "AND NOT EXISTS(SELECT 1 FROM vm_job_retained_disk_purge_predecessors p "
                "WHERE p.cleanup_admission_id=d.cleanup_admission_id AND p.admitted_xact_id=pg_current_xact_id())",
                parent_cleanup.get("admission_id"),
                owner,
                locator["pvc_uid"],
                str(generation),
            )
            if purge is None:
                raise ResourceAdmissionError("retained_ready_purge_unproven")
            intent = {
                "owner_kind": "job",
                "owner_id": str(owner),
                "source": "public_vm_delete",
                "resource": "vm_workspace",
                "provision_generation": str(purge["provision_generation"]),
                "vm_uid": str(purge["vm_uid"]),
                "pvc_uid": str(purge["pvc_uid"]),
                "purge_disk": True,
            }
            expected = bind_vm_cleanup_permit(
                CleanupPermit(allowed=True, admission_id=purge["cleanup_admission_id"]),
                request_id=purge["cleanup_request_id"],
                intent=intent,
            ).parent_cleanup
            if parent_cleanup != expected or not await conn.fetchval(
                "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
                purge["cleanup_admission_id"],
            ):
                raise ResourceAdmissionError("retained_ready_purge_changed")
            # No Ready keep witness is attached to the separate, exact purge.
            # Its native committed chain and signed parent authorize that path.
            return None
        authority = await conn.fetchrow(
            "SELECT a.*,c.completed_at,c.retained_resume_admitted_xact_id<>pg_current_xact_id() AS parent_committed, "
            "a.admitted_xact_id<>pg_current_xact_id() AS authority_committed, "
            "op.admitted_xact_id<>pg_current_xact_id() AS operation_committed, "
            "r.job_retained_resume_admitted_xact_id<>pg_current_xact_id() AS source_committed "
            "FROM vm_job_cancel_retention_authorities a JOIN vm_job_retained_resumes op "
            "ON op.id=a.job_retained_resume_id JOIN vm_creation_retries r ON r.request_id=op.request_id "
            "JOIN jobs j ON j.id=op.job_id JOIN vm_workspace_cleanup_admissions c ON c.id=a.cleanup_admission_id "
            "WHERE op.id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text AND r.ready_at IS NOT NULL",
            locator["id"],
        )
        if (
            authority is None
            or authority["policy_version"] != 2
            or str(authority["provision_generation"]) != str(generation)
            or authority["pvc_uid"] != locator["pvc_uid"]
            or authority["completed_at"] is not None
            or not all(
                authority[key] is True
                for key in (
                    "parent_committed",
                    "authority_committed",
                    "operation_committed",
                    "source_committed",
                )
            )
        ):
            raise ResourceAdmissionError("retained_ready_retention_unproven")
        from shared.vm_cancel_retention import valid_ready_retention_preflight
        from orchestrator.services.vm_workspace_recovery_store import (
            CleanupPermit,
            bind_vm_cleanup_permit,
        )

        proof = _json(authority["ready_retention_preflight"])
        candidate = _json(
            await conn.fetchval(
                "SELECT public.vm_job_retained_ready_candidate(a) FROM vm_job_cancel_retention_authorities a "
                "WHERE cleanup_admission_id=$1",
                authority["cleanup_admission_id"],
            )
        )
        expected = bind_vm_cleanup_permit(
            CleanupPermit(allowed=True, admission_id=authority["cleanup_admission_id"]),
            request_id=authority["cleanup_request_id"],
            intent=_json(authority["retaining_intent"]),
        ).parent_cleanup
        if (
            not valid_ready_retention_preflight(proof, candidate)
            or parent_cleanup != {**expected, "retention_preflight": proof}
            or not await conn.fetchval(
                "SELECT public.validate_vm_job_cancel_retention($1,false)",
                authority["cleanup_admission_id"],
            )
        ):
            raise ResourceAdmissionError("retained_ready_retention_changed")
        return proof


async def _retained_stop_scope(conn, *, job_id, identity):
    from shared.vm_resource_admission import ResourceAdmissionError

    if not await installed(conn):
        return None
    owner = UUID(str(job_id))
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:job:{owner}",
    )
    locator = await conn.fetchrow(
        "SELECT op.* FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
        "WHERE op.job_id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
        owner,
    )
    if locator is None:
        return None
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery-pvc:{locator['pvc_uid']}",
    )
    await conn.fetchrow(
        "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
    )
    job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner)
    operation = await operation_on_conn(conn, job, allow_terminal=True)
    if (
        operation is None
        or operation["id"] != locator["id"]
        or str(operation["provision_generation"]) != identity.provision_generation
        or str(operation["pvc_uid"]) != identity.rootdisk_pvc_uid
    ):
        raise ResourceAdmissionError("retained_continuation_identity_changed")
    retry = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
        operation["request_id"],
    )
    authority = await conn.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities WHERE job_retained_resume_id=$1",
        operation["id"],
    )
    if retry is None or str(retry["observed_vm_uid"]) != identity.vm_uid:
        raise ResourceAdmissionError("retained_continuation_source_unproven")
    candidate = _json(
        await conn.fetchval(
            "SELECT public.vm_job_retained_stop_candidate($1,$2,$3,$4,NULL,$5,$6)",
            owner,
            operation["provision_generation"],
            UUID(identity.vm_uid),
            operation["pvc_uid"],
            authority["cleanup_admission_id"] if authority else None,
            authority is not None,
        )
    )
    return operation, retry, authority, candidate


def _ready_candidate(operation, identity, candidate, request_id, digest):
    return {
        "version": 1,
        "kind": "vm_job_retained_ready_stop_candidate_v1",
        "owner_kind": "job",
        "job_id": str(operation["job_id"]),
        "namespace": candidate["namespace"],
        "cluster_id": candidate["cluster_id"],
        "continuation_id": str(operation["id"]),
        "request_id": str(operation["request_id"]),
        "provision_generation": identity.provision_generation,
        "reservation_id": candidate["reservation_id"],
        "reservation_revision": candidate["reservation_revision"],
        "vm_uid": identity.vm_uid,
        "vmi_uid": candidate["vmi_uid"],
        "launcher_uid": candidate["launcher_uid"],
        "node_uid": candidate["node_uid"],
        "pvc_uid": identity.rootdisk_pvc_uid,
        "cleanup_request_id": str(request_id),
        "cleanup_intent_digest": digest,
    }


async def retained_ready_candidate(db, *, job_id, identity):
    """Capture a Ready witness request without retaining SQL locks over status I/O."""
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        vm_cleanup_request_identity,
    )

    async with db.acquire() as conn, conn.transaction():
        scoped = await _retained_stop_scope(conn, job_id=job_id, identity=identity)
        if scoped is None:
            return None
        operation, retry, authority, candidate = scoped
        if retry["ready_at"] is None:
            return None
        if authority is not None:
            proof = _json(authority["ready_retention_preflight"])
            if not proof:
                raise ResourceAdmissionError("retained_ready_retention_unproven")
            return proof["frozen"]
        if candidate is None:
            raise ResourceAdmissionError("retained_continuation_scope_unproven")
        _, _, request_id, digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=job_id,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=False,
        )
        return _ready_candidate(operation, identity, candidate, request_id, digest)


async def acquire_retained_resume_cleanup(
    store, *, job_id, identity, retention_preflight=None
):
    from shared.vm_cancel_retention import valid_ready_retention_preflight
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_workspace_recovery_store import (
        CleanupPermit,
        bind_vm_cleanup_permit,
        vm_cleanup_request_identity,
    )

    async with store.db.acquire() as conn, conn.transaction():
        scoped = await _retained_stop_scope(conn, job_id=job_id, identity=identity)
        if scoped is None:
            return None
        operation, retry, authority, candidate = scoped
        if authority is not None:
            if not await conn.fetchval(
                "SELECT public.validate_vm_job_cancel_retention($1,false)",
                authority["cleanup_admission_id"],
            ):
                raise ResourceAdmissionError("retained_continuation_authority_changed")
            parent = await conn.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                authority["cleanup_admission_id"],
            )
            permit = bind_vm_cleanup_permit(
                CleanupPermit(
                    allowed=True,
                    admission_id=parent["id"],
                    completed_outcome=parent["outcome"],
                ),
                request_id=authority["cleanup_request_id"],
                intent=_json(authority["retaining_intent"]),
            )
            proof = _json(authority["ready_retention_preflight"])
            if proof is not None:
                from dataclasses import replace

                permit = replace(
                    permit,
                    parent_cleanup={
                        **permit.parent_cleanup,
                        "retention_preflight": proof,
                    },
                )
            return permit
        if candidate is None:
            return CleanupPermit(
                allowed=False, reason="retained_continuation_scope_unproven"
            )
        _, _, request_id, digest, intent = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=job_id,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=False,
        )
        if retry["ready_at"] is not None:
            expected = _ready_candidate(
                operation, identity, candidate, request_id, digest
            )
            if not valid_ready_retention_preflight(
                retention_preflight, frozen=expected
            ):
                return CleanupPermit(
                    allowed=False, reason="retained_ready_retention_unproven"
                )
        elif retention_preflight is not None:
            return CleanupPermit(
                allowed=False, reason="retained_continuation_stop_policy_changed"
            )
        permit = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="job",
            owner_id=operation["job_id"],
            pvc_uid=operation["pvc_uid"],
            request_id=request_id,
            source="job_terminal_vm_release",
            intent_digest=digest,
        )
        if not permit.allowed:
            raise ResourceAdmissionError("retained_continuation_parent_refused")
        await conn.execute(
            "INSERT INTO vm_job_cancel_retention_authorities(cleanup_admission_id,job_id,creation_request_id,"
            "provision_generation,reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
            "namespace,cluster_id,cleanup_request_id,intent_digest,retaining_intent,policy_version,job_retained_resume_id,ready_retention_preflight) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,2,$17,$18::jsonb)",
            permit.admission_id,
            operation["job_id"],
            operation["request_id"],
            operation["provision_generation"],
            UUID(candidate["reservation_id"]),
            candidate["reservation_revision"],
            UUID(identity.vm_uid),
            UUID(candidate["vmi_uid"]),
            UUID(candidate["launcher_uid"]),
            operation["pvc_uid"],
            UUID(candidate["node_uid"]),
            candidate["namespace"],
            candidate["cluster_id"],
            request_id,
            digest,
            json.dumps(intent),
            operation["id"],
            json.dumps(retention_preflight)
            if retention_preflight is not None
            else None,
        )
        permit = bind_vm_cleanup_permit(permit, request_id=request_id, intent=intent)
        if retention_preflight is not None:
            from dataclasses import replace

            permit = replace(
                permit,
                parent_cleanup={
                    **permit.parent_cleanup,
                    "retention_preflight": retention_preflight,
                },
            )
        return permit


async def acquire_retained_terminal_cleanup(store, provisioner, *, job_id, identity):
    """Admit or replay the exact keep, qualifying Ready storage before effects.

    Candidate capture and signed status I/O are separated by the transaction
    boundary. Admission rechecks every identity after the external response.
    """
    from orchestrator.services.vm_job_cancel_retention import acquire_cancel_retention

    permit = await acquire_cancel_retention(store, job_id=job_id, identity=identity)
    if (
        permit is None
        or permit.allowed
        or permit.reason != "retained_ready_retention_unproven"
    ):
        return permit
    candidate = await retained_ready_candidate(
        store.db, job_id=job_id, identity=identity
    )
    if candidate is None:
        return permit
    proof = await provisioner.qualify_retained_ready_stop(candidate)
    return await acquire_cancel_retention(
        store, job_id=job_id, identity=identity, retention_preflight=proof
    )


async def retention_preflight_on_conn(conn, admission_id):
    if await installed(conn):
        proof = await conn.fetchval(
            "SELECT ready_retention_preflight FROM vm_job_cancel_retention_authorities "
            "WHERE cleanup_admission_id=$1 AND policy_version=2",
            admission_id,
        )
        if proof is not None:
            return _json(proof)
    return _json(
        await conn.fetchval(
            "SELECT retention_preflight FROM vm_pre_ssh_stop_intents WHERE cleanup_admission_id=$1",
            admission_id,
        )
    )


async def complete_retained_cancel(db, job_id, *, clear_pending=True):
    absent = await complete_source_absent_cancel(
        db, job_id, clear_pending=clear_pending
    )
    if absent is not None:
        return absent
    owner = UUID(str(job_id))
    async with db.acquire() as conn, conn.transaction():
        if not await installed(conn):
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        locator = await conn.fetchrow(
            "SELECT op.* FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
            "WHERE j.id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
            owner,
        )
        if locator is None:
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{locator['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        terminal = await conn.fetchrow(
            "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
            locator["id"],
        )
        if terminal is not None:
            if not await conn.fetchval(
                "SELECT public.vm_job_retained_terminal_is_current($1)", owner
            ):
                return False
            if clear_pending:
                await conn.execute(
                    "UPDATE jobs SET context=context-'_stateless_cancel_cleanup_pending'-'_vm_creation_pending',"
                    "updated_at=clock_timestamp() WHERE id=$1",
                    owner,
                )
            return True
        proof = _json(
            await conn.fetchval(
                "SELECT public.vm_job_retained_kept_evidence($1)", locator["id"]
            )
        )
        kind = "kept_compute"
        if proof is None:
            proof = _json(
                await conn.fetchval(
                    "SELECT public.vm_job_retained_noeffect_evidence($1)", locator["id"]
                )
            )
            if proof is None:
                return False
            kind = proof["terminal_kind"]
        terminal = await conn.fetchrow(
            "SELECT * FROM vm_job_retained_resume_terminals WHERE resume_id=$1",
            locator["id"],
        )
        if terminal is not None:
            if (
                terminal["terminal_kind"] != kind
                or _json(terminal["evidence"]) != proof
            ):
                return False
        else:
            await conn.execute(
                "INSERT INTO vm_job_retained_resume_terminals(id,resume_id,allocated_request_id,source_request_id,"
                "terminal_kind,physical_cleanup_admission_id,evidence) VALUES($1,$2,$3,$3,$6,$4,$5::jsonb)",
                uuid5(locator["id"], "terminal"),
                locator["id"],
                locator["request_id"],
                UUID(proof["physical_cleanup_admission_id"]),
                json.dumps(proof),
                kind,
            )
        if kind != "kept_compute":
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,status}','\"deleted\"'::jsonb) WHERE id=$1",
                owner,
            )
        if clear_pending:
            await conn.execute(
                "UPDATE jobs SET context=context-'_stateless_cancel_cleanup_pending'-'_vm_creation_pending',"
                "updated_at=clock_timestamp() WHERE id=$1",
                owner,
            )
        return True


async def settle_retained_no_compute(db, job_id):
    """Route a typed nonissued Cancel before Q1's unrelated null-PVC policy.

    None denotes adopted compute which still needs its exact physical stop.
    False is recognized uncertainty and must never fall through to Q1/purge.
    """
    absent = await complete_source_absent_cancel(db, job_id, clear_pending=False)
    if absent is not None:
        return absent
    async with db.acquire() as conn:
        if not await installed(conn):
            return None
        retry = await conn.fetchrow(
            "SELECT r.* FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
            "JOIN vm_creation_retries r ON r.job_retained_resume_id=op.id "
            "WHERE op.job_id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
            UUID(str(job_id)),
        )
        if retry is None or retry["state"] == "succeeded":
            return None
    if retry["state"] == "cancel_requested":
        from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

        result = await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(retry["request_id"])
        )
        if not result.get("settled"):
            return False
    return await complete_retained_cancel(db, job_id, clear_pending=False)


async def retained_delete_identity(db, job_id):
    """Resolve the current typed terminal to a physical disk, without projection.

    None means unrelated. A recognized logical continuation with no exact
    terminal or explicit Delete remains held, even when its VM context is null.
    """
    from shared.vm_resource_admission import ResourceAdmissionError
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    owner = UUID(str(job_id))
    async with db.acquire() as conn, conn.transaction():
        if not await installed(conn):
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        locator = await conn.fetchrow(
            "SELECT op.* FROM vm_job_retained_resumes op JOIN jobs j ON j.id=op.job_id "
            "WHERE j.id=$1 AND j.context->>'_vm_job_retained_resume'=op.id::text",
            owner,
        )
        recognized = await conn.fetchval(
            "SELECT context ? '_vm_job_retained_resume' FROM jobs WHERE id=$1", owner
        )
        if locator is None:
            if recognized:
                raise ResourceAdmissionError("retained_delete_tail_unproven")
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{locator['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner)
        context = _json(job["context"]) if job else {}
        if (
            context.get("_vm_job_retained_resume") != str(locator["id"])
            or context.get("_stateless_delete_pending") is not True
        ):
            raise ResourceAdmissionError("retained_delete_tail_unproven")
        if not await conn.fetchval(
            "SELECT public.vm_job_retained_terminal_is_current($1)", owner
        ):
            raise ResourceAdmissionError("retained_delete_tail_unproven")
        physical = await conn.fetchrow(
            "SELECT a.* FROM vm_job_retained_resume_terminals t "
            "JOIN vm_job_cancel_retention_authorities a ON a.cleanup_admission_id=t.physical_cleanup_admission_id "
            "WHERE t.resume_id=$1",
            locator["id"],
        )
        return VMTeardownIdentity(
            str(physical["provision_generation"]),
            str(physical["vm_uid"]),
            str(physical["pvc_uid"]),
        )
