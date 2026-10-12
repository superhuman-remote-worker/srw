"""Exact replay selection for an admitted, retired container completion."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest


@dataclass(frozen=True, slots=True)
class CancelledContainerCompletionReplay:
    job_id: UUID
    admission_id: UUID
    request_id: UUID
    intent_digest: str
    runtime_incarnation: UUID
    creation_id: UUID
    cleanup_id: UUID
    queue_token: int
    context_digest: str


def legacy_completion_cleanup_identity(job_id: UUID) -> tuple[UUID, str]:
    request_id = uuid5(
        uuid5(NAMESPACE_URL, f"completion-cleanup:{job_id}"), "legacy_workspace:none"
    )
    digest = cleanup_intent_digest(
        {
            "purge_workspace": True,
            "owner_kind": "job",
            "owner_id": str(job_id),
            "pvc_uid": "",
            "resource": "legacy_workspace",
            "source": "completion_workspace_teardown",
        }
    )
    return request_id, digest


async def select_cancelled_container_completion_replay(
    conn: Any,
    job_id: UUID,
    *,
    expected: CancelledContainerCompletionReplay | None = None,
    allow_completed: bool = False,
) -> CancelledContainerCompletionReplay | None:
    """Read one coherent proof; selection alone grants no external effect.

    Callers own either a read-only repeatable-read transaction or the original
    admission transaction with queue/Job locks. No locks span external I/O.
    """
    from orchestrator.database.postgres import _completion_control_active_sql

    request_id, intent_digest = legacy_completion_cleanup_identity(job_id)
    row = await conn.fetchrow(
        f"""
        SELECT j.context, q.lease_token, a.id AS admission_id,
               c.id AS creation_id, i.id AS cleanup_id, i.runtime_incarnation
          FROM jobs j
          JOIN run_queue q ON q.unit_id=j.id AND q.unit_kind='worker_batch'
          JOIN vm_workspace_cleanup_admissions a
            ON a.owner_kind='job' AND a.owner_id=j.id
           AND a.request_id=$2 AND a.intent_digest=$3
          JOIN managed_repository_workspace_creation_reservations c
            ON c.owner_kind='job' AND c.owner_id=j.id
           AND c.scope='workspace_container'
           AND c.id::text=j.context #>> '{{workspace_container,_creation_reservation_id}}'
           AND c.claim_token::text=j.context #>> '{{workspace_container,_creation_claim_token}}'
           AND c.runtime_incarnation::text=j.context #>> '{{workspace_container,_runtime_incarnation}}'
          JOIN managed_repository_workspace_cleanup_intents i
            ON i.owner_kind='job' AND i.owner_id=j.id
           AND i.scope='workspace_container' AND i.runtime_incarnation=c.runtime_incarnation
         WHERE j.id=$1 AND j.status='cancelled' AND j.execution_lane='stateless'
           AND j.assigned_agent_id IS NULL AND j.lease_expires_at IS NULL
           AND j.parent_job_id IS NULL
           AND jsonb_typeof(j.context)='object'
           AND COALESCE(j.context->'inherits_parent_workspace','false'::jsonb)='false'::jsonb
           AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb
           AND NOT j.context ?| ARRAY['_stateless_resume_pending','_vm_creation_pending',
                                     '_worker_execution_hold']
           AND COALESCE(j.context->'vm','{{}}'::jsonb)='{{}}'::jsonb
           AND COALESCE(j.context->'ide_session','{{}}'::jsonb)='{{}}'::jsonb
           AND NOT ({_completion_control_active_sql("j.context")})
           AND j.context #>> '{{workspace_container,provisioner}}'='k8s'
           AND j.context #>> '{{workspace_container,status}}'='deleted'
           AND q.state='done' AND q.leased_by IS NULL AND q.leased_until IS NULL
           AND q.lease_token>0
           AND a.source='completion_workspace_teardown'
           AND a.pvc_uid IS NULL AND a.parent_admission_id IS NULL
           AND (a.completed_at IS NULL OR ($4 AND a.outcome='completed'))
           AND c.settled_at IS NOT NULL AND c.phase='settled' AND c.result_kind='settled'
           AND i.result_kind='settled' AND i.phase='settled'
           AND i.settled_at IS NOT NULL AND i.cleanup_completed_at IS NOT NULL
           AND i.projection_transaction_id IS NOT NULL
           AND i.target_disposition='deleted' AND i.resource_policy='terminal_reclaim'
           AND i.reclaim_shared_resources AND i.capture_complete
           AND i.resources_captured_at IS NOT NULL AND i.pod_uid=i.runtime_incarnation
           AND i.terminal_queue_token=0
           AND (c.pod_uid IS NULL OR c.pod_uid=i.pod_uid)
           AND (c.pvc_uid IS NULL OR c.pvc_uid=i.pvc_uid)
           AND (c.service_uid IS NULL OR c.service_uid=i.service_uid)
           AND (c.seed_configmap_uid IS NULL OR c.seed_configmap_uid=i.seed_configmap_uid)
           AND i.claimed_by IS NULL AND i.claim_expires_at IS NULL
           AND managed_repository_workspace_cleanup_projection_is_settled(
               'job',j.id,'workspace_container',i.runtime_incarnation::text,
               j.context #>> '{{workspace_container,_runtime_incarnation}}',
               j.context #>> '{{workspace_container,status}}')
           AND EXISTS (
               SELECT 1 FROM managed_repository_process_zero_receipts p
                WHERE p.owner_kind='job' AND p.owner_id=j.id
                  AND p.scope='workspace_container' AND p.provisioner='k8s'
                  AND p.runtime_incarnation=i.runtime_incarnation::text)
           AND NOT EXISTS (
               SELECT 1 FROM managed_repository_workspace_creation_reservations other
                WHERE other.owner_kind='job' AND other.owner_id=j.id
                  AND (other.settled_at IS NULL OR other.reservation_generation>c.reservation_generation))
           AND NOT EXISTS (
               SELECT 1 FROM managed_repository_workspace_cleanup_intents other
                WHERE other.owner_kind='job' AND other.owner_id=j.id
                  AND (other.settled_at IS NULL OR other.intent_generation>i.intent_generation))
           AND NOT EXISTS (
               SELECT 1 FROM vm_workspace_cleanup_admissions other
                WHERE other.owner_kind='job' AND other.owner_id=j.id
                  AND other.id<>a.id AND other.completed_at IS NULL)
           AND NOT EXISTS (
               SELECT 1 FROM vm_idle_access_leases l
                WHERE l.owner_kind='job' AND l.owner_id=j.id
                  AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
           AND NOT EXISTS (
               SELECT 1 FROM vm_workspace_recoveries r
               LEFT JOIN vm_workspace_recovery_retention_pins p
                 ON p.recovery_id=r.id AND p.released_at IS NULL
                WHERE r.resolved_at IS NULL
                  AND ((r.owner_kind='job' AND r.owner_id=j.id) OR p.pvc_uid=i.pvc_uid))
           AND NOT EXISTS (
               SELECT 1 FROM vm_workspace_recovery_jobs rj
                WHERE rj.job_id=j.id AND rj.resolved_at IS NULL)
         ORDER BY i.intent_generation DESC LIMIT 1
        """,
        job_id,
        request_id,
        intent_digest,
        allow_completed,
    )
    if row is None:
        return None
    context = row["context"]
    if isinstance(context, str):
        context = json.loads(context)
    proof = CancelledContainerCompletionReplay(
        job_id=job_id,
        admission_id=row["admission_id"],
        request_id=request_id,
        intent_digest=intent_digest,
        runtime_incarnation=row["runtime_incarnation"],
        creation_id=row["creation_id"],
        cleanup_id=row["cleanup_id"],
        queue_token=row["lease_token"],
        context_digest=hashlib.sha256(
            json.dumps(context, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )
    return proof if expected is None or proof == expected else None


async def lock_completion_replay_owner(
    conn: Any, proof: CancelledContainerCompletionReplay
) -> None:
    """Fence the same owner and queue using the original admission protocol."""
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:job:{proof.job_id}",
    )
    await conn.fetchrow(
        "SELECT unit_id FROM run_queue WHERE unit_id=$1 AND unit_kind='worker_batch' FOR UPDATE",
        proof.job_id,
    )
    await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", proof.job_id)
