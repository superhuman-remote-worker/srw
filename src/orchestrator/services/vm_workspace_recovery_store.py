"""Short PostgreSQL transactions for durable VM workspace recovery."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping
from uuid import UUID, uuid4

from shared.workspace_recovery import (
    RecoveryAttemptDisposition,
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


@dataclass(frozen=True, slots=True)
class RecoveryClaim:
    operation_id: UUID
    version: int
    claim_token: int
    deadline_at: datetime
    remaining_seconds: float
    captured_identity: Mapping[str, Any]


class VMWorkspaceRecoveryStore:
    def __init__(self, db: Any, *, worker_id: str | None = None) -> None:
        self.db = db
        self.worker_id = worker_id or os.getenv("HOSTNAME", "vm-workspace-recovery")

    @staticmethod
    async def _accepted_request(
        conn: Any,
        *,
        job_id: UUID,
        request_id: UUID,
        intent_digest: str,
    ) -> WorkspaceRecoveryDisposition | None:
        prior = await conn.fetchrow(
            """
            SELECT intent_digest, accepted_result
              FROM vm_workspace_recovery_requests
             WHERE scope_kind='job' AND scope_id=$1 AND request_id=$2
            """,
            job_id,
            request_id,
        )
        if prior is None:
            return None
        if prior["intent_digest"] != intent_digest:
            raise RuntimeError(
                "workspace recovery request ID reused with different intent"
            )
        result = _json(prior["accepted_result"])
        return WorkspaceRecoveryDisposition(
            code=WorkspaceRecoveryCode(result["code"]),
            action=result["action"],
            operation_id=UUID(result["operation_id"]),
            accepted_lease_token=int(result["accepted_lease_token"]),
            hold_lease_token=int(result["hold_lease_token"]),
        )

    @staticmethod
    async def _pause_locked(
        conn: Any,
        *,
        operation_id: UUID,
        code: WorkspaceRecoveryCode,
        diagnostic: Mapping[str, Any],
    ) -> None:
        await conn.execute(
            """
            UPDATE vm_workspace_recoveries
               SET phase='paused_attention', reason_code=$2,
                   latest_diagnostic=$3::jsonb,
                   claimed_by=NULL, claimed_until=NULL,
                   version=version+1
             WHERE id=$1 AND phase='recovering' AND resolved_at IS NULL
            """,
            operation_id,
            code.value,
            json.dumps(dict(diagnostic)),
        )
        await conn.execute(
            """
            UPDATE vm_workspace_recovery_jobs
               SET participation='attention'
             WHERE recovery_id=$1 AND resolved_at IS NULL
            """,
            operation_id,
        )

    async def admit_hold(
        self,
        *,
        job_id: UUID,
        accepted_lease_token: int,
        owner_kind: str,
        owner_id: UUID,
        workspace_contract_digest: str,
        provision_generation: UUID,
        cluster_name: str,
        namespace: str,
        vm_uid: UUID,
        prior_vmi_uid: UUID,
        prior_launcher_uid: UUID,
        root_pvc_uid: UUID,
        code: WorkspaceRecoveryCode,
        request_id: UUID,
        actor_kind: str,
        actor_id: str,
        intent_digest: str,
        original_cause: Mapping[str, Any] | None = None,
        checkpoint_id: str | None = None,
        checkpoint_namespace: str | None = None,
    ) -> WorkspaceRecoveryDisposition:
        """Fence one leased worker batch and commit its recovery hold."""

        async with self.db.acquire() as conn:
            async with conn.transaction():
                prior = await self._accepted_request(
                    conn,
                    job_id=job_id,
                    request_id=request_id,
                    intent_digest=intent_digest,
                )
                if prior is not None:
                    return prior

                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"workspace-recovery:{owner_kind}:{owner_id}",
                )
                prior = await self._accepted_request(
                    conn,
                    job_id=job_id,
                    request_id=request_id,
                    intent_digest=intent_digest,
                )
                if prior is not None:
                    return prior
                queue = await conn.fetchrow(
                    "SELECT * FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                    job_id,
                )
                if (
                    queue is None
                    or queue["unit_kind"] != "worker_batch"
                    or queue["state"] != "leased"
                    or queue["lease_token"] != accepted_lease_token
                ):
                    raise RuntimeError("worker batch lease is no longer current")
                job = await conn.fetchrow(
                    "SELECT status, freeze_data, context FROM jobs "
                    "WHERE id=$1 FOR UPDATE",
                    job_id,
                )
                if job is None:
                    raise RuntimeError("recovery job does not exist")
                attempt = await conn.fetchrow(
                    """
                    SELECT job_id
                      FROM worker_batch_attempts
                     WHERE job_id=$1 AND lease_token=$2
                     FOR UPDATE
                    """,
                    job_id,
                    accepted_lease_token,
                )
                attention_required = attempt is None
                disposition_code = (
                    WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                    if attention_required
                    else code
                )
                phase = "paused_attention" if attention_required else "recovering"
                diagnostic = (
                    {"reason": "worker_batch_attempt_missing"}
                    if attention_required
                    else None
                )

                recovery_id = uuid4()
                await conn.execute(
                    """
                    INSERT INTO vm_workspace_recoveries (
                        id, owner_kind, owner_id, workspace_contract_digest,
                        provision_generation, cluster_name, namespace, vm_uid,
                        prior_vmi_uid, prior_launcher_uid, root_pvc_uid,
                        phase, reason_code, original_cause, latest_diagnostic
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,
                        $15::jsonb
                    )
                    """,
                    recovery_id,
                    owner_kind,
                    owner_id,
                    workspace_contract_digest,
                    provision_generation,
                    cluster_name,
                    namespace,
                    vm_uid,
                    prior_vmi_uid,
                    prior_launcher_uid,
                    root_pvc_uid,
                    phase,
                    disposition_code.value,
                    json.dumps(dict(original_cause or {})),
                    json.dumps(diagnostic),
                )
                hold_token = accepted_lease_token + 1
                await conn.execute(
                    """
                    UPDATE run_queue
                       SET state='parked', lease_token=$2, leased_by=NULL,
                           leased_until=NULL, park_reason='workspace_recovery',
                           parked_at=clock_timestamp()
                     WHERE unit_id=$1
                    """,
                    job_id,
                    hold_token,
                )
                await conn.execute(
                    """
                    UPDATE jobs SET status='paused', assigned_agent_id=NULL,
                        freeze_data=jsonb_build_object(
                            'freeze_type','workspace_recovery',
                            'recovery_id',$2::text,
                            'hold_lease_token',$3::bigint)
                    WHERE id=$1
                    """,
                    job_id,
                    str(recovery_id),
                    hold_token,
                )
                await conn.execute(
                    """
                    INSERT INTO vm_workspace_recovery_jobs (
                        recovery_id, job_id, accepted_lease_token, hold_lease_token,
                        prior_queue_state, prior_job_status, prior_freeze_reference,
                        checkpoint_id, checkpoint_namespace, participation
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)
                    """,
                    recovery_id,
                    job_id,
                    accepted_lease_token,
                    hold_token,
                    queue["state"],
                    job["status"],
                    json.dumps(_json(job["freeze_data"])),
                    checkpoint_id,
                    checkpoint_namespace,
                    "attention" if attention_required else "held",
                )
                disposition_factory = (
                    WorkspaceRecoveryDisposition.paused_attention
                    if attention_required
                    else WorkspaceRecoveryDisposition.hold_committed
                )
                disposition = disposition_factory(
                    operation_id=recovery_id,
                    accepted_lease_token=accepted_lease_token,
                    hold_lease_token=hold_token,
                    code=disposition_code,
                )
                receipt = {
                    "code": disposition.code.value,
                    "action": disposition.action,
                    "operation_id": str(disposition.operation_id),
                    "accepted_lease_token": disposition.accepted_lease_token,
                    "hold_lease_token": disposition.hold_lease_token,
                }
                if attempt is not None:
                    updated = await conn.execute(
                        """
                        UPDATE worker_batch_attempts
                           SET disposition=$3::jsonb, recovery_id=$4
                         WHERE job_id=$1 AND lease_token=$2
                        """,
                        job_id,
                        accepted_lease_token,
                        json.dumps(receipt),
                        recovery_id,
                    )
                    if updated != "UPDATE 1":
                        raise RuntimeError("worker batch attempt evidence changed")
                await conn.execute(
                    """
                    INSERT INTO vm_workspace_recovery_requests (
                        scope_kind, scope_id, request_id, actor_kind, actor_id,
                        intent_digest, recovery_id, accepted_result
                    ) VALUES ('job',$1,$2,$3,$4,$5,$6,$7::jsonb)
                    """,
                    job_id,
                    request_id,
                    actor_kind,
                    actor_id,
                    intent_digest,
                    recovery_id,
                    json.dumps(receipt),
                )
                return disposition

    async def get_attempt_disposition(
        self, conn: Any, *, job_id: UUID, lease_token: int
    ) -> RecoveryAttemptDisposition | None:
        row = await conn.fetchrow(
            """
            SELECT job_id, lease_token, bundle_authorized_at, authority_digest,
                   disposition, recovery_id, refunded_at
              FROM worker_batch_attempts
             WHERE job_id=$1 AND lease_token=$2
            """,
            job_id,
            lease_token,
        )
        if row is None:
            return None
        return RecoveryAttemptDisposition(
            job_id=row["job_id"],
            lease_token=int(row["lease_token"]),
            bundle_authorized=row["bundle_authorized_at"] is not None,
            authority_digest=row["authority_digest"],
            disposition=_json(row["disposition"]),
            recovery_id=row["recovery_id"],
            refunded=row["refunded_at"] is not None,
        )

    async def record_bundle_authorized(
        self,
        conn: Any,
        *,
        job_id: UUID,
        lease_token: int,
        authority_digest: str,
    ) -> bool:
        row = await conn.fetchval(
            """
            UPDATE worker_batch_attempts
               SET bundle_authorized_at=clock_timestamp(), authority_digest=$3
             WHERE job_id=$1 AND lease_token=$2
               AND bundle_authorized_at IS NULL AND authority_digest IS NULL
            RETURNING 1
            """,
            job_id,
            lease_token,
            authority_digest,
        )
        return row is not None

    async def claim_due(
        self, operation_id: UUID, *, ttl_seconds: float = 30
    ) -> RecoveryClaim | None:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                expired = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='paused_attention',
                           reason_code='workspace_recovery_deadline_exceeded',
                           latest_diagnostic=jsonb_build_object(
                               'reason', 'deadline_elapsed_before_claim',
                               'observed_at', clock_timestamp()),
                           claimed_by=NULL, claimed_until=NULL,
                           version=version+1
                     WHERE id=$1 AND phase='recovering' AND resolved_at IS NULL
                       AND deadline_at <= clock_timestamp()
                    RETURNING id
                    """,
                    operation_id,
                )
                if expired is not None:
                    await conn.execute(
                        """
                        UPDATE vm_workspace_recovery_jobs
                           SET participation='attention'
                         WHERE recovery_id=$1 AND resolved_at IS NULL
                        """,
                        operation_id,
                    )
                    return None
                row = await conn.fetchrow(
                    """
                    UPDATE vm_workspace_recoveries
                       SET claimed_by=$2,
                           claimed_until=clock_timestamp()
                               + make_interval(secs => $3::double precision),
                           claim_token=claim_token+1,
                           version=version+1
                     WHERE id=$1 AND phase='recovering' AND resolved_at IS NULL
                       AND next_check_at <= clock_timestamp()
                       AND deadline_at > clock_timestamp()
                       AND (claimed_until IS NULL OR claimed_until <= clock_timestamp())
                    RETURNING id, version, claim_token, deadline_at,
                              extract(epoch FROM
                                  (deadline_at-clock_timestamp()))::float8
                                  AS remaining_seconds,
                              provision_generation, cluster_name, namespace,
                              vm_uid, prior_vmi_uid, prior_launcher_uid, root_pvc_uid
                    """,
                    operation_id,
                    self.worker_id,
                    ttl_seconds,
                )
        if row is None:
            return None
        return RecoveryClaim(
            operation_id=row["id"],
            version=int(row["version"]),
            claim_token=int(row["claim_token"]),
            deadline_at=row["deadline_at"],
            remaining_seconds=max(0.0, float(row["remaining_seconds"])),
            captured_identity={
                "provision_generation": row["provision_generation"],
                "cluster_name": row["cluster_name"],
                "namespace": row["namespace"],
                "vm_uid": row["vm_uid"],
                "prior_vmi_uid": row["prior_vmi_uid"],
                "prior_launcher_uid": row["prior_launcher_uid"],
                "root_pvc_uid": row["root_pvc_uid"],
            },
        )

    async def apply_observation(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        observation: Mapping[str, Any],
        next_check_seconds: float = 5,
    ) -> bool:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                changed = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recoveries
                       SET latest_observation=$4::jsonb,
                           last_progress_at=clock_timestamp(),
                           next_check_at=clock_timestamp()
                               + make_interval(secs => $5::double precision),
                           claimed_by=NULL, claimed_until=NULL,
                           version=version+1
                     WHERE id=$1 AND version=$2 AND claim_token=$3
                       AND phase='recovering' AND resolved_at IS NULL
                    RETURNING 1
                    """,
                    operation_id,
                    version,
                    claim_token,
                    json.dumps(dict(observation)),
                    next_check_seconds,
                )
        return changed is not None

    async def pause_for_attention(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        code: WorkspaceRecoveryCode,
        diagnostic: Mapping[str, Any],
    ) -> bool:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                changed = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='paused_attention', reason_code=$4,
                           latest_diagnostic=$5::jsonb,
                           claimed_by=NULL, claimed_until=NULL,
                           version=version+1
                     WHERE id=$1 AND version=$2 AND claim_token=$3
                       AND phase='recovering' AND resolved_at IS NULL
                    RETURNING 1
                    """,
                    operation_id,
                    version,
                    claim_token,
                    code.value,
                    json.dumps(dict(diagnostic)),
                )
        return changed is not None

    async def release_recovered(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        resume_receipt: Mapping[str, Any],
    ) -> bool:
        """Release exact participant holds with queue-before-job lock ordering."""

        async with self.db.acquire() as conn:
            participants = await conn.fetch(
                "SELECT job_id, hold_lease_token FROM vm_workspace_recovery_jobs "
                "WHERE recovery_id=$1 AND resolved_at IS NULL ORDER BY job_id",
                operation_id,
            )
            async with conn.transaction():
                queues: dict[UUID, Any] = {}
                for participant in participants:
                    queues[participant["job_id"]] = await conn.fetchrow(
                        "SELECT state, lease_token, park_reason FROM run_queue "
                        "WHERE unit_id=$1 FOR UPDATE",
                        participant["job_id"],
                    )
                jobs: dict[UUID, Any] = {}
                for participant in participants:
                    jobs[participant["job_id"]] = await conn.fetchrow(
                        "SELECT status, freeze_data FROM jobs WHERE id=$1 FOR UPDATE",
                        participant["job_id"],
                    )
                operation = await conn.fetchrow(
                    "SELECT id, deadline_at <= clock_timestamp() AS deadline_expired "
                    "FROM vm_workspace_recoveries "
                    "WHERE id=$1 AND version=$2 AND claim_token=$3 "
                    "AND phase='recovering' AND resolved_at IS NULL FOR UPDATE",
                    operation_id,
                    version,
                    claim_token,
                )
                if operation is None:
                    return False
                if operation["deadline_expired"]:
                    await self._pause_locked(
                        conn,
                        operation_id=operation_id,
                        code=WorkspaceRecoveryCode.DEADLINE_EXCEEDED,
                        diagnostic={"reason": "deadline_elapsed_before_release"},
                    )
                    return False
                projection_errors: list[dict[str, str]] = []
                if not participants:
                    projection_errors.append(
                        {"reason": "recovery_participants_missing"}
                    )
                for participant in participants:
                    job_id = participant["job_id"]
                    hold_token = int(participant["hold_lease_token"])
                    queue = queues[job_id]
                    job = jobs[job_id]
                    queue_valid = bool(
                        queue is not None
                        and queue["state"] == "parked"
                        and queue["park_reason"] == "workspace_recovery"
                        and int(queue["lease_token"]) == hold_token
                    )
                    freeze = _json(job["freeze_data"]) if job is not None else None
                    job_valid = bool(
                        job is not None
                        and job["status"] == "paused"
                        and isinstance(freeze, dict)
                        and freeze.get("freeze_type") == "workspace_recovery"
                        and freeze.get("recovery_id") == str(operation_id)
                        and type(freeze.get("hold_lease_token")) is int
                        and freeze["hold_lease_token"] == hold_token
                    )
                    if not queue_valid or not job_valid:
                        projection_errors.append(
                            {
                                "reason": "participant_projection_mismatch",
                                "job_id": str(job_id),
                                "queue_valid": str(queue_valid).lower(),
                                "job_valid": str(job_valid).lower(),
                            }
                        )
                if projection_errors:
                    await self._pause_locked(
                        conn,
                        operation_id=operation_id,
                        code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                        diagnostic={"projection_errors": projection_errors},
                    )
                    return False
                for participant in participants:
                    changed = await conn.fetchval(
                        """
                        UPDATE run_queue SET state='queued', run_after=clock_timestamp(),
                            park_reason=NULL, parked_at=NULL
                        WHERE unit_id=$1 AND state='parked'
                          AND park_reason='workspace_recovery' AND lease_token=$2
                        RETURNING 1
                        """,
                        participant["job_id"],
                        participant["hold_lease_token"],
                    )
                    if changed is None:
                        raise RuntimeError("workspace recovery hold token changed")
                    job_changed = await conn.fetchval(
                        "UPDATE jobs SET freeze_data=NULL "
                        "WHERE id=$1 AND status='paused' "
                        "AND freeze_data->>'freeze_type'='workspace_recovery' "
                        "AND freeze_data->>'recovery_id'=$2 "
                        "AND freeze_data->>'hold_lease_token'=$3 "
                        "RETURNING 1",
                        participant["job_id"],
                        str(operation_id),
                        str(participant["hold_lease_token"]),
                    )
                    if job_changed is None:
                        raise RuntimeError("workspace recovery job projection changed")
                await conn.execute(
                    """
                    UPDATE vm_workspace_recovery_jobs
                       SET participation='released', resolved_at=clock_timestamp(),
                           resume_receipt=$2::jsonb
                     WHERE recovery_id=$1 AND resolved_at IS NULL
                    """,
                    operation_id,
                    json.dumps(dict(resume_receipt)),
                )
                await conn.execute(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='recovered', resolved_at=clock_timestamp(),
                           claimed_by=NULL, claimed_until=NULL, version=version+1
                     WHERE id=$1 AND version=$2 AND claim_token=$3
                       AND phase='recovering' AND resolved_at IS NULL
                    """,
                    operation_id,
                    version,
                    claim_token,
                )
        return True


__all__ = ["RecoveryClaim", "VMWorkspaceRecoveryStore"]
