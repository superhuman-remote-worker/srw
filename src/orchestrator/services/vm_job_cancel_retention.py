"""Immutable retention admission for an exact cancelled, never-ready Job VM.

The rollout flag admits new authority only. Persisted authority, its deletion
fence, and typed permanent Delete remain effective when admission is disabled.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid5

from shared.vm_resource_admission import ResourceAdmissionError

from orchestrator.services.vm_workspace_recovery_store import (
    CleanupPermit,
    bind_vm_cleanup_permit,
    prepare_vm_cleanup_resource,
    vm_cleanup_request_identity,
)


def cancel_retention_admission_enabled(environ=None) -> bool:
    source = os.environ if environ is None else environ
    value = source.get("VM_JOB_CANCEL_RETENTION_ENABLED", "false").strip().lower()
    if value not in {"true", "false"}:
        raise ValueError("VM_JOB_CANCEL_RETENTION_ENABLED must be true or false")
    return value == "true"


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _uuid(value):
    parsed = UUID(str(value))
    if str(parsed) != str(value):
        raise ValueError("noncanonical retention identity")
    return parsed


async def _installed(conn):
    return await conn.fetchval(
        "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
    )


async def retention_for_admission_on_conn(conn, admission_id: UUID):
    if not await _installed(conn):
        return None
    row = await conn.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities "
        "WHERE cleanup_admission_id=$1 OR superseded_admission_id=$1",
        admission_id,
    )
    return dict(row) if row else None


@dataclass(frozen=True)
class JobRetainedPurgeBootstrap:
    """Private transaction-only proposal; never physical effect authority."""

    job_id: UUID
    pvc_uid: UUID
    final_request_id: UUID
    provision_generation: UUID
    cleanup_request_id: UUID
    intent_digest: str
    retention_admission_ids: frozenset[UUID]


async def guard_retention_cleanup_on_conn(
    conn,
    *,
    owner_kind: str,
    owner_id: UUID,
    pvc_uid: UUID | None,
    source: str,
    request_id: UUID,
    intent_digest: str,
    parent_admission_id: UUID | None,
    bootstrap=None,
) -> None:
    """Called under owner/PVC locks before replay, creation, or resumed effect."""

    if not await _installed(conn):
        return
    if await conn.fetchval(
        "SELECT public.vm_job_cancel_retention_cleanup_allowed($1,$2,$3,$4,$5,$6,$7,false)",
        owner_kind,
        owner_id,
        pvc_uid,
        source,
        request_id,
        intent_digest,
        parent_admission_id,
    ):
        return
    # Only the dedicated service constructs this after locked exact reads.
    # The deferred SQL constraint independently requires its complete chain.
    if (
        type(bootstrap) is JobRetainedPurgeBootstrap
        and owner_kind == "job"
        and parent_admission_id is None
        and source == "public_vm_delete"
        and bootstrap.job_id == owner_id
        and bootstrap.pvc_uid == pvc_uid
        and bootstrap.cleanup_request_id == request_id
        and bootstrap.intent_digest == intent_digest
    ):
        rows = await conn.fetch(
            "SELECT cleanup_admission_id,job_id,pvc_uid FROM vm_job_cancel_retention_authorities "
            "WHERE (job_id=$1 OR pvc_uid=$2) AND NOT "
            "public.vm_job_cancel_retention_discharged(cleanup_admission_id)",
            owner_id,
            pvc_uid,
        )
        if (
            rows
            and {r["cleanup_admission_id"] for r in rows}
            == bootstrap.retention_admission_ids
            and all(r["job_id"] == owner_id and r["pvc_uid"] == pvc_uid for r in rows)
            and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE request_id=$1 "
                "AND owner_kind='job' AND job_id=$2 AND provision_generation=$3 "
                "AND observed_pvc_uid=$4 AND state='succeeded')",
                bootstrap.final_request_id,
                owner_id,
                bootstrap.provision_generation,
                pvc_uid,
            )
        ):
            return
    raise ResourceAdmissionError("cancel_retention_disk_protected")


async def retention_settlement_is_current_on_conn(
    conn,
    *,
    job_id: UUID,
    generation: UUID,
    admission_id: UUID,
) -> bool:
    if not await _installed(conn):
        return False
    return bool(
        await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities a "
            "JOIN jobs j ON j.id=a.job_id JOIN run_queue q ON q.unit_id=j.id "
            "JOIN vm_creation_retries r ON r.request_id=a.creation_request_id "
            "WHERE a.cleanup_admission_id=$1 AND a.job_id=$2 AND a.provision_generation=$3 "
            "AND j.status='cancelled' AND j.execution_lane='stateless' "
            "AND j.parent_job_id IS NULL AND j.assigned_agent_id IS NULL "
            "AND j.context->'vm'->>'provision_generation'=a.provision_generation::text "
            "AND j.context->'vm'->>'vm_uid'=a.vm_uid::text "
            "AND j.context->'vm'->>'rootdisk_pvc_uid'=a.pvc_uid::text "
            "AND q.unit_kind='worker_batch' AND q.state='done' "
            "AND q.leased_by IS NULL AND q.leased_until IS NULL "
            "AND r.state='succeeded' AND r.ready_at IS NULL "
            "AND NOT EXISTS(SELECT 1 FROM vm_creation_retries later WHERE later.owner_kind='job' "
            "AND later.job_id=a.job_id AND (later.created_at,later.request_id)>(r.created_at,r.request_id))) "
            "AND public.vm_job_cancel_retention_settled($1)",
            admission_id,
            job_id,
            generation,
        )
    )


async def acquire_cancel_retention(
    store, *, job_id: str, identity, retention_preflight=None
):
    """None is reserved for clearly unrelated work, never a recognized refusal."""

    from orchestrator.services.vm_job_retained_resume import (
        acquire_retained_resume_cleanup,
    )

    try:
        owner = _uuid(job_id)
    except (TypeError, ValueError, AttributeError):
        return CleanupPermit(allowed=False, reason="cancel_retention_identity_unproven")
    continuation = await acquire_retained_resume_cleanup(
        store,
        job_id=job_id,
        identity=identity,
        retention_preflight=retention_preflight,
    )
    if continuation is not None:
        return continuation
    async with store.db.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        prior = None
        if await _installed(conn):
            prior = await conn.fetchrow(
                "SELECT a.*,c.completed_at,c.outcome FROM vm_job_cancel_retention_authorities a "
                "JOIN vm_workspace_cleanup_admissions c ON c.id=a.cleanup_admission_id "
                "WHERE a.job_id=$1 ORDER BY a.admitted_at DESC LIMIT 1",
                owner,
            )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", owner)
        context = _json(job["context"]) if job else None
        vm = context.get("vm") if isinstance(context, dict) else None
        if prior is None and (
            job is None
            or job["status"] != "cancelled"
            or job["execution_lane"] != "stateless"
            or not isinstance(vm, dict)
        ):
            return None
        try:
            generation = _uuid(identity.provision_generation)
            vm_uid = _uuid(identity.vm_uid)
            pvc = _uuid(identity.rootdisk_pvc_uid)
        except (TypeError, ValueError, AttributeError):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_identity_unproven"
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )
        if (
            prior is None
            and vm.get("status") == "ready"
            and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE owner_kind='job' "
                "AND job_id=$1 AND provision_generation=$2 AND observed_vm_uid=$3 "
                "AND observed_pvc_uid=$4 AND ready_at IS NOT NULL)",
                owner,
                generation,
                vm_uid,
                pvc,
            )
        ):
            # An exact currently and durably Ready source is outside this policy.
            # Ready history appearing on an already-retiring candidate is drift;
            # missing/uncertain sources remain recognized refusals below.
            return None
        if prior is not None:
            if (
                prior["provision_generation"] != generation
                or prior["vm_uid"] != vm_uid
                or prior["pvc_uid"] != pvc
            ):
                return CleanupPermit(
                    allowed=False, reason="cancel_retention_identity_changed"
                )
            # Existing immutable authority wins even when flag text is invalid.
            if prior["completed_at"] is not None:
                valid = await retention_settlement_is_current_on_conn(
                    conn,
                    job_id=owner,
                    generation=generation,
                    admission_id=prior["cleanup_admission_id"],
                )
            else:
                valid = await conn.fetchval(
                    "SELECT public.validate_vm_job_cancel_retention($1,false)",
                    prior["cleanup_admission_id"],
                )
            if not valid:
                return CleanupPermit(
                    allowed=False, reason="cancel_retention_authority_changed"
                )
            return bind_vm_cleanup_permit(
                CleanupPermit(
                    allowed=True,
                    admission_id=prior["cleanup_admission_id"],
                    completed_outcome=prior["outcome"],
                ),
                request_id=prior["cleanup_request_id"],
                intent=_json(prior["retaining_intent"]),
            )
        if not cancel_retention_admission_enabled():
            return CleanupPermit(allowed=False, reason="cancel_retention_not_activated")
        if not await _installed(conn):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_schema_unavailable"
            )
        _, _, old_request, old_digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
        )
        _, _, request_id, digest, intent = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=False,
        )
        old = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND request_id=$2 FOR UPDATE",
            owner,
            old_request,
        )
        if old is not None and (
            old["source"] != "job_terminal_vm_release"
            or old["pvc_uid"] != pvc
            or old["intent_digest"] != old_digest
            or old["completed_at"] is not None
            or old["parent_admission_id"] is not None
        ):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_old_parent_changed"
            )
        from orchestrator.services.vm_creation_retry_store import (
            VMCreationRetryConflict,
            VMCreationRetryStore,
        )

        try:
            job = await VMCreationRetryStore(store.db)._scope(
                conn,
                owner,
                pvc,
                own_admission=old["id"] if old else None,
                hold_queue=False,
            )
        except VMCreationRetryConflict as exc:
            return CleanupPermit(allowed=False, reason=str(exc))
        candidate = await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_candidate($1,$2,$3,$4,$5)",
            owner,
            generation,
            vm_uid,
            pvc,
            old["id"] if old else None,
        )
        candidate = _json(candidate)
        if not isinstance(candidate, dict):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_scope_unproven"
            )
        if old is not None:
            request_id = uuid5(
                NAMESPACE_URL,
                f"vm-job-cancel-retain-v1:{old['id']}:{digest}",
            )
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET "
                "completed_at=clock_timestamp(),outcome='superseded_by_retention' WHERE id=$1",
                old["id"],
            )
        permit = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="job",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request_id,
            source="job_terminal_vm_release",
            intent_digest=digest,
        )
        if not permit.allowed:
            # Never commit a superseded old parent without its successor.
            raise ResourceAdmissionError("cancel_retention_successor_refused")
        permit = bind_vm_cleanup_permit(permit, request_id=request_id, intent=intent)
        await conn.execute(
            "INSERT INTO vm_job_cancel_retention_authorities "
            "(cleanup_admission_id,superseded_admission_id,job_id,creation_request_id,"
            "provision_generation,reservation_id,reservation_revision,vm_uid,vmi_uid,"
            "launcher_uid,pvc_uid,node_uid,namespace,cluster_id,cleanup_request_id,"
            "intent_digest,retaining_intent,superseded_request_id,superseded_intent_digest) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,$18,$19)",
            permit.admission_id,
            old["id"] if old else None,
            owner,
            UUID(candidate["creation_request_id"]),
            generation,
            UUID(candidate["reservation_id"]),
            candidate["reservation_revision"],
            vm_uid,
            UUID(candidate["vmi_uid"]),
            UUID(candidate["launcher_uid"]),
            pvc,
            UUID(candidate["node_uid"]),
            candidate["namespace"],
            candidate["cluster_id"],
            request_id,
            digest,
            json.dumps(intent),
            old["request_id"] if old else None,
            old["intent_digest"] if old else None,
        )
        if old is not None:
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,"
                "'{_job_terminal_vm_cleanup,admission_id}',to_jsonb($3::text)) "
                "WHERE id=$1 AND context->'_job_terminal_vm_cleanup'->>'admission_id'=$2",
                owner,
                str(old["id"]),
                str(permit.admission_id),
            )
        await prepare_vm_cleanup_resource(store, permit, _conn=conn)
        return permit


async def complete_cancel_retention_marker(db, job_id: str):
    """None means legacy cleanup; a retaining owner needs its full settled proof."""
    owner = _uuid(job_id)
    async with db.acquire() as conn, conn.transaction():
        if not await _installed(conn):
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        authority = await conn.fetchrow(
            "SELECT * FROM vm_job_cancel_retention_authorities WHERE job_id=$1 ORDER BY admitted_at DESC LIMIT 1",
            owner,
        )
        if authority is None:
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{authority['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT id FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR SHARE",
            authority["cleanup_admission_id"],
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        job = await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        if job is None:
            return True
        if not await retention_settlement_is_current_on_conn(
            conn,
            job_id=owner,
            generation=authority["provision_generation"],
            admission_id=authority["cleanup_admission_id"],
        ):
            return False
        disk_kept = not await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_discharged($1)",
            authority["cleanup_admission_id"],
        )
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context-'_stateless_cancel_cleanup_pending','{vm}',"
            "(context->'vm')||$2::jsonb),updated_at=clock_timestamp() WHERE id=$1",
            owner,
            json.dumps(
                {"status": "deleted", "compute_released": True, "disk_kept": disk_kept}
            ),
        )
        return True
