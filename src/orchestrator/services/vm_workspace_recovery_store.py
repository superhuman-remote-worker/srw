"""Short PostgreSQL transactions for durable VM workspace recovery."""

from __future__ import annotations

import json
import hashlib
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from shared.worker_queue import (
    get_worker_attempt_disposition,
    park_worker_batch_for_workspace_recovery,
    record_worker_bundle_authorized,
    release_worker_batch_from_workspace_recovery,
)
from shared.workspace_recovery import (
    RecoveryAttemptDisposition,
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _job_workspace_owner(job_id: UUID, job: Any) -> tuple[UUID, bool]:
    """Resolve the supported direct-parent contract without guessing malformed flags."""
    if job is None:
        return job_id, True
    context = _json(job["context"]) if job["context"] is not None else {}
    if not isinstance(context, dict):
        return job_id, True
    inherits = context.get("inherits_parent_workspace", False)
    if inherits is False:
        return job_id, False
    parent_id = job["parent_job_id"]
    if (inherits is True or inherits == "true") and parent_id is not None:
        return parent_id, False
    # Conservatively contain the possible parent workspace as attention-only.
    return parent_id or job_id, True


@dataclass(frozen=True, slots=True)
class RecoveryClaim:
    operation_id: UUID
    version: int
    claim_token: int
    deadline_at: datetime
    remaining_seconds: float
    captured_identity: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CleanupPermit:
    allowed: bool
    admission_id: UUID | None = None
    recovery_id: UUID | None = None
    reason: str | None = None


class WorkspaceRecoveryControlConflict(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _cleanup_uuid(value: Any, *, namespace: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return uuid5(NAMESPACE_URL, f"{namespace}:{value}")


async def acquire_vm_cleanup_permit(
    recovery_store: Any,
    *,
    owner_kind: str,
    owner_id: str | UUID,
    identity: Any,
    source: str,
) -> CleanupPermit:
    """Admit one exact VM/PVC cleanup intent through recovery authority."""

    canonical_owner = _cleanup_uuid(owner_id, namespace=f"{owner_kind}-owner")
    raw_pvc_uid = getattr(identity, "rootdisk_pvc_uid", None)
    pvc_uid = (
        _cleanup_uuid(raw_pvc_uid, namespace="rootdisk-pvc")
        if raw_pvc_uid is not None
        else None
    )
    intent = ":".join(
        (
            source,
            owner_kind,
            str(canonical_owner),
            str(getattr(identity, "provision_generation", "")),
            str(getattr(identity, "vm_uid", "")),
            str(pvc_uid or ""),
        )
    )
    return await recovery_store.acquire_cleanup_permit(
        owner_kind=owner_kind,
        owner_id=canonical_owner,
        pvc_uid=pvc_uid,
        request_id=uuid5(NAMESPACE_URL, f"vm-workspace-cleanup:{intent}"),
        source=source,
    )


async def complete_vm_cleanup_permit(
    recovery_store: Any,
    permit: CleanupPermit | Any,
    *,
    outcome: str,
) -> None:
    admission_id = getattr(permit, "admission_id", None)
    if admission_id is not None:
        await recovery_store.complete_cleanup_permit(admission_id, outcome=outcome)


class VMWorkspaceRecoveryStore:
    def __init__(self, db: Any, *, worker_id: str | None = None) -> None:
        self.db = db
        self.worker_id = worker_id or os.getenv("HOSTNAME", "vm-workspace-recovery")

    @asynccontextmanager
    async def _connection(self, conn: Any | None):
        if conn is not None:
            yield conn
        else:
            async with self.db.acquire() as acquired:
                yield acquired

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
        provision_generation: UUID | None,
        cluster_name: str,
        namespace: str | None,
        vm_uid: UUID | None,
        prior_vmi_uid: UUID | None,
        prior_launcher_uid: UUID | None,
        root_pvc_uid: UUID | None,
        code: WorkspaceRecoveryCode,
        request_id: UUID,
        actor_kind: str,
        actor_id: str,
        intent_digest: str,
        original_cause: Mapping[str, Any] | None = None,
        checkpoint_id: str | None = None,
        checkpoint_namespace: str | None = None,
        _conn: Any | None = None,
        expired_grace_seconds: float | None = None,
    ) -> WorkspaceRecoveryDisposition:
        """Fence the reporter and all current members under the workspace lock."""

        async with self._connection(_conn) as conn:
            async with conn.transaction():
                prior = await self._accepted_request(
                    conn,
                    job_id=job_id,
                    request_id=request_id,
                    intent_digest=intent_digest,
                )
                if prior is not None:
                    return prior

                selected_owner = (owner_kind, owner_id)
                membership = await conn.fetchrow(
                    "SELECT parent_job_id, context FROM jobs WHERE id=$1", job_id
                )
                # Both sides of an inheritance toggle must be locked before
                # any queue/job row. A caller's pre-network owner selection is
                # not authoritative after waiting for the membership writer.
                owner_locks = {selected_owner, ("job", job_id)}
                if membership is not None and membership["parent_job_id"] is not None:
                    owner_locks.add(("job", membership["parent_job_id"]))
                for kind, identifier in sorted(owner_locks):
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"workspace-recovery:{kind}:{identifier}",
                    )
                if root_pvc_uid is not None:
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"workspace-recovery-pvc:{root_pvc_uid}",
                    )
                membership = await conn.fetchrow(
                    "SELECT parent_job_id, context FROM jobs WHERE id=$1", job_id
                )
                current_owner, owner_ambiguous = _job_workspace_owner(
                    job_id, membership
                )
                if ("job", current_owner) not in owner_locks:
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_owner_changed",
                        "Canonical workspace ownership changed during recovery admission.",
                    )
                elif current_owner != job_id:
                    parent = await conn.fetchrow(
                        "SELECT parent_job_id, context FROM jobs WHERE id=$1",
                        current_owner,
                    )
                    parent_owner, parent_ambiguous = _job_workspace_owner(
                        current_owner, parent
                    )
                    owner_ambiguous |= parent_ambiguous or parent_owner != current_owner
                owner_conflict = owner_ambiguous or selected_owner != (
                    "job",
                    current_owner,
                )
                owner_kind, owner_id = "job", current_owner
                cleanup = await conn.fetchrow(
                    "SELECT id FROM vm_workspace_cleanup_admissions "
                    "WHERE ((owner_kind=$1 AND owner_id=$2) "
                    "OR ($3::uuid IS NOT NULL AND pvc_uid=$3)) "
                    "AND completed_at IS NULL FOR UPDATE",
                    owner_kind,
                    owner_id,
                    root_pvc_uid,
                )
                if cleanup is not None:
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_cleanup_already_admitted",
                        "Workspace cleanup crossed its admission boundary before recovery.",
                    )
                prior = await self._accepted_request(
                    conn,
                    job_id=job_id,
                    request_id=request_id,
                    intent_digest=intent_digest,
                )
                if prior is not None:
                    return prior
                members = await conn.fetch(
                    "SELECT id, execution_lane FROM jobs WHERE id=$1 OR ("
                    "$2='job' "
                    "AND status NOT IN ('completed','failed','cancelled') AND ("
                    "id=$3 OR (parent_job_id=$3 AND "
                    "context->>'inherits_parent_workspace'='true'))) ORDER BY id",
                    job_id,
                    owner_kind,
                    owner_id,
                )
                queues: dict[UUID, Any] = {}
                missing_queues: set[UUID] = set()
                # Insert inert rows and lock existing queues in the same UUID
                # order. No jobs-row lock is taken until every queue is held.
                for member in members:
                    member_id = member["id"]
                    if member["execution_lane"] == "stateless":
                        inserted = await conn.fetchval(
                            "INSERT INTO run_queue (unit_id, unit_kind, state, run_after) "
                            "VALUES ($1, 'worker_batch', 'parked', 'infinity') "
                            "ON CONFLICT (unit_id) DO NOTHING RETURNING 1",
                            member_id,
                        )
                        if inserted:
                            missing_queues.add(member_id)
                    queues[member_id] = await conn.fetchrow(
                        "SELECT * FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                        member_id,
                    )
                jobs: dict[UUID, Any] = {}
                for member in members:
                    jobs[member["id"]] = await conn.fetchrow(
                        "SELECT status, freeze_data, context, execution_lane, parent_job_id "
                        "FROM jobs WHERE id=$1 FOR UPDATE",
                        member["id"],
                    )
                queue = queues.get(job_id)
                if (
                    queue is None
                    or queue["unit_kind"] != "worker_batch"
                    or queue["state"] != "leased"
                    or queue["lease_token"] != accepted_lease_token
                ):
                    raise RuntimeError("worker batch lease is no longer current")
                job = jobs.get(job_id)
                if job is None:
                    raise RuntimeError("recovery job does not exist")
                if expired_grace_seconds is not None:
                    expired = await conn.fetchval(
                        "SELECT leased_until < clock_timestamp() - "
                        "make_interval(secs => $3::float8) FROM run_queue "
                        "WHERE unit_id=$1 AND lease_token=$2 AND state='leased'",
                        job_id,
                        accepted_lease_token,
                        expired_grace_seconds,
                    )
                    if expired is not True:
                        raise RuntimeError(
                            "worker lease renewed before recovery admission"
                        )
                    # Initial scans are hints; authorization can commit while
                    # this transaction waits for the workspace/queue locks.
                    exact = await self.get_attempt_disposition(
                        conn, job_id=job_id, lease_token=accepted_lease_token
                    )
                    if exact is None or exact.bundle_authorized:
                        code = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                locked_owner, locked_ambiguous = _job_workspace_owner(job_id, job)
                owner_conflict |= locked_ambiguous or locked_owner != owner_id
                attempts: dict[UUID, Any] = {}
                references: dict[UUID, Any] = {}
                uncertain_members: list[str] = []
                for member in members:
                    member_id = member["id"]
                    if jobs[member_id]["execution_lane"] != "stateless":
                        continue
                    if queues[member_id]["unit_kind"] != "worker_batch":
                        raise RuntimeError("workspace recovery queue kind changed")
                    member_queue = queues[member_id]
                    exact_attempt = await conn.fetchrow(
                        "SELECT * FROM worker_batch_attempts "
                        "WHERE job_id=$1 AND lease_token=$2 FOR UPDATE",
                        member_id,
                        member_queue["lease_token"],
                    )
                    latest_attempt = await conn.fetchrow(
                        "SELECT * FROM worker_batch_attempts WHERE job_id=$1 "
                        "ORDER BY lease_token DESC LIMIT 1 FOR UPDATE",
                        member_id,
                    )
                    context = _json(jobs[member_id]["context"]) or {}
                    never_started = bool(
                        jobs[member_id]["status"] == "created"
                        and latest_attempt is None
                        and member_queue["lease_token"] == 0
                        and member_queue["attempts_since_completion"] == 0
                        and member_queue["park_reason"] is None
                        and (
                            member_id in missing_queues
                            or member_queue["state"] == "queued"
                        )
                        and isinstance(context, dict)
                        and "_workspace_dispatch_authority" not in context
                    )
                    references[member_id] = {
                        "queue": dict(member_queue)
                        if member_id not in missing_queues
                        else {"state": "absent"},
                        "attempt": dict(exact_attempt)
                        if exact_attempt is not None
                        else None,
                        "latest_attempt": dict(latest_attempt)
                        if latest_attempt is not None
                        else None,
                        "never_started": never_started,
                    }
                    if member_queue["state"] == "leased":
                        attempts[member_id] = exact_attempt
                    elif not never_started:
                        # A queued/parked row does not prove execution safety.
                        # Missing rows/history or an operator park retain debt.
                        uncertain_members.append(str(member_id))
                attention_required = bool(uncertain_members) or any(
                    value is None for value in attempts.values()
                )
                # A pre-existing user/completion freeze is independent intent;
                # preserve its reference and keep the entire workspace held.
                frozen_members = [
                    str(member_id)
                    for member_id, member_job in jobs.items()
                    if member_job["freeze_data"] is not None
                    or member_job["status"] not in {"created", "processing", "paused"}
                ]
                attention_required = attention_required or bool(frozen_members)
                unsupported_writers = [
                    str(member_id)
                    for member_id, member_job in jobs.items()
                    if member_job["execution_lane"] != "stateless"
                ]
                missing_identity_fields = [
                    key
                    for key, value in {
                        "prior_vmi_uid": prior_vmi_uid,
                        "prior_launcher_uid": prior_launcher_uid,
                        "provision_generation": provision_generation,
                        "namespace": namespace,
                        "vm_uid": vm_uid,
                        "root_pvc_uid": root_pvc_uid,
                    }.items()
                    if value is None
                ]
                missing_runtime_identity = bool(missing_identity_fields)
                reported_attention = code in {
                    WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                    WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
                    WorkspaceRecoveryCode.CHECKPOINT_UNAVAILABLE,
                }
                attention_required = (
                    attention_required
                    or bool(unsupported_writers)
                    or owner_conflict
                    or missing_runtime_identity
                    or reported_attention
                )
                disposition_code = (
                    WorkspaceRecoveryCode.IDENTITY_CONFLICT
                    if owner_conflict or missing_runtime_identity
                    else WorkspaceRecoveryCode.SHARED_WRITERS_UNFENCED
                    if unsupported_writers
                    else WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                    if attention_required and not reported_attention
                    else code
                )
                if (
                    expired_grace_seconds is not None
                    and code == WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                ):
                    # Missing identity remains diagnostic debt, but it must
                    # not hide the reaper's stronger unknown-execution hold.
                    disposition_code = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                phase = "paused_attention" if attention_required else "recovering"
                diagnostic = (
                    {
                        "reason": "canonical_workspace_owner_changed_or_ambiguous",
                        "selected_owner": {
                            "kind": selected_owner[0],
                            "id": str(selected_owner[1]),
                        },
                        "observed_owner": {"kind": owner_kind, "id": str(owner_id)},
                        "ambiguous": owner_ambiguous or locked_ambiguous,
                    }
                    if owner_conflict
                    else {
                        "reason": "captured_runtime_identity_incomplete",
                        "missing_identity_fields": missing_identity_fields,
                    }
                    if missing_runtime_identity
                    else {
                        "reason": "shared_workspace_writers_unfenced",
                        "job_ids": unsupported_writers,
                    }
                    if unsupported_writers
                    else {
                        "reason": "dependent_execution_evidence_unresolved",
                        "job_ids": uncertain_members,
                    }
                    if uncertain_members
                    else {
                        "reason": "participant_control_requires_attention",
                        "job_ids": frozen_members,
                    }
                    if frozen_members
                    else {"reason": code.value}
                    if reported_attention
                    else {"reason": "worker_batch_attempt_missing"}
                    if attention_required
                    else None
                )
                if missing_runtime_identity:
                    diagnostic = {
                        **(diagnostic or {}),
                        "missing_identity_fields": missing_identity_fields,
                    }
                if (
                    expired_grace_seconds is not None
                    and code == WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                ):
                    recovery_codes = [WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN.value]
                    if owner_conflict or missing_runtime_identity:
                        recovery_codes.append(
                            WorkspaceRecoveryCode.IDENTITY_CONFLICT.value
                        )
                    if unsupported_writers:
                        recovery_codes.append(
                            WorkspaceRecoveryCode.SHARED_WRITERS_UNFENCED.value
                        )
                    diagnostic = {
                        **(diagnostic or {}),
                        "recovery_codes": recovery_codes,
                        "attempt_ledger_present": exact is not None,
                        "bundle_authorized": exact.bundle_authorized if exact else None,
                        "unsupported_writer_job_ids": unsupported_writers,
                        "unresolved_member_job_ids": uncertain_members,
                        "control_blocked_job_ids": frozen_members,
                    }

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
                if provision_generation is not None and root_pvc_uid is not None:
                    await conn.execute(
                        "INSERT INTO vm_workspace_recovery_retention_pins "
                        "(recovery_id,pvc_uid,provision_generation) VALUES ($1,$2,$3)",
                        recovery_id,
                        root_pvc_uid,
                        provision_generation,
                    )
                hold_tokens: dict[UUID, int] = {}
                for member in members:
                    member_id = member["id"]
                    member_queue = queues[member_id]
                    if jobs[member_id]["execution_lane"] != "stateless":
                        # Unsupported writers have no worker hold authority.
                        # Never fabricate a token or alter their job projection.
                        await conn.execute(
                            "INSERT INTO vm_workspace_recovery_jobs ("
                            "recovery_id, job_id, accepted_lease_token, hold_lease_token, "
                            "prior_queue_state, prior_job_status, prior_freeze_reference, "
                            "participation, outcome) VALUES ($1,$2,NULL,NULL,'non_worker',$3,$4::jsonb, "
                            "'attention','{\"reason\":\"shared_workspace_writers_unfenced\"}'::jsonb)",
                            recovery_id,
                            member_id,
                            jobs[member_id]["status"],
                            json.dumps(_json(jobs[member_id]["freeze_data"])),
                        )
                        continue
                    if member_queue["state"] == "leased":
                        held = await park_worker_batch_for_workspace_recovery(
                            conn,
                            job_id=member_id,
                            accepted_lease_token=member_queue["lease_token"],
                            recovery_id=recovery_id,
                        )
                        if held is None:
                            raise RuntimeError(
                                "worker batch lease is no longer current"
                            )
                        hold_tokens[member_id] = held.hold_lease_token
                    else:
                        hold_tokens[member_id] = await conn.fetchval(
                            "UPDATE run_queue SET state='parked', lease_token=lease_token+1, "
                            "leased_by=NULL, last_leased_by=NULL, leased_until=NULL, "
                            "interrupt_admission_lease_token=NULL, interrupt_admission_turn_id=NULL, "
                            "run_after='infinity', park_reason=COALESCE(park_reason,'workspace_recovery'), "
                            "parked_at=COALESCE(parked_at,clock_timestamp()) WHERE unit_id=$1 RETURNING lease_token",
                            member_id,
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
                        member_id,
                        str(recovery_id),
                        hold_tokens[member_id],
                    )
                    await conn.execute(
                        """
                    INSERT INTO vm_workspace_recovery_jobs (
                        recovery_id, job_id, accepted_lease_token, hold_lease_token,
                        prior_queue_state, prior_job_status, prior_freeze_reference,
                        checkpoint_id, checkpoint_namespace, participation, prior_control_reference
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11::jsonb)
                    """,
                        recovery_id,
                        member_id,
                        member_queue["lease_token"]
                        if member_queue["lease_token"] > 0
                        else None,
                        hold_tokens[member_id],
                        "absent"
                        if member_id in missing_queues
                        else member_queue["state"],
                        jobs[member_id]["status"],
                        json.dumps(_json(jobs[member_id]["freeze_data"])),
                        checkpoint_id if member_id == job_id else None,
                        checkpoint_namespace if member_id == job_id else None,
                        "attention" if attention_required else "held",
                        json.dumps(references[member_id], default=str),
                    )
                hold_token = hold_tokens[job_id]
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
                # Every active participant needs its own immutable accepted
                # token receipt when renewal is fenced, including a sibling
                # that never made the reporting request itself.
                for member_id, member_attempt in attempts.items():
                    member_receipt = {
                        **receipt,
                        "accepted_lease_token": queues[member_id]["lease_token"],
                        "hold_lease_token": hold_tokens[member_id],
                    }
                    await conn.execute(
                        "UPDATE vm_workspace_recovery_jobs SET outcome=$3::jsonb "
                        "WHERE recovery_id=$1 AND job_id=$2",
                        recovery_id,
                        member_id,
                        json.dumps({"disposition": member_receipt}),
                    )
                    if member_attempt is None:
                        continue
                    updated = await conn.execute(
                        """
                        UPDATE worker_batch_attempts
                           SET disposition=$3::jsonb, recovery_id=$4
                         WHERE job_id=$1 AND lease_token=$2
                        """,
                        member_id,
                        queues[member_id]["lease_token"],
                        json.dumps(member_receipt),
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

    async def unresolved_participation(self, job_id: UUID) -> dict[str, Any] | None:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT r.id AS operation_id, r.owner_kind, r.owner_id, r.phase, "
                "r.deadline_at, rj.participation FROM vm_workspace_recovery_jobs rj "
                "JOIN vm_workspace_recoveries r ON r.id=rj.recovery_id "
                "WHERE rj.job_id=$1 AND rj.resolved_at IS NULL AND r.resolved_at IS NULL",
                job_id,
            )
        return dict(row) if row is not None else None

    async def acquire_cleanup_permit(
        self,
        *,
        owner_kind: str,
        owner_id: UUID,
        pvc_uid: UUID | None,
        request_id: UUID,
        source: str,
    ) -> CleanupPermit:
        """Serialize destructive admission with recovery and its exact disk pin."""

        async with self.db.acquire() as conn:
            async with conn.transaction():
                owner_locks = {(owner_kind, owner_id)}
                if owner_kind == "job":
                    membership = await conn.fetchrow(
                        "SELECT parent_job_id,context FROM jobs WHERE id=$1",
                        owner_id,
                    )
                    owner_locks.add(("job", owner_id))
                    if (
                        membership is not None
                        and membership["parent_job_id"] is not None
                    ):
                        owner_locks.add(("job", membership["parent_job_id"]))
                for kind, identifier in sorted(owner_locks):
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"workspace-recovery:{kind}:{identifier}",
                    )
                if pvc_uid is not None:
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"workspace-recovery-pvc:{pvc_uid}",
                    )
                if owner_kind == "job":
                    membership = await conn.fetchrow(
                        "SELECT parent_job_id,context FROM jobs WHERE id=$1",
                        owner_id,
                    )
                    if membership is not None:
                        canonical_owner, ambiguous = _job_workspace_owner(
                            owner_id, membership
                        )
                        if ambiguous or ("job", canonical_owner) not in owner_locks:
                            raise WorkspaceRecoveryControlConflict(
                                "workspace_owner_changed",
                                "Canonical workspace ownership changed during cleanup admission.",
                            )
                        owner_id = canonical_owner
                prior = await conn.fetchrow(
                    "SELECT id,completed_at,pvc_uid,source "
                    "FROM vm_workspace_cleanup_admissions "
                    "WHERE owner_kind=$1 AND owner_id=$2 AND request_id=$3 FOR UPDATE",
                    owner_kind,
                    owner_id,
                    request_id,
                )
                if prior is not None:
                    if prior["pvc_uid"] != pvc_uid or prior["source"] != source:
                        raise WorkspaceRecoveryControlConflict(
                            "cleanup_request_id_reused",
                            "Cleanup request ID was already used with different resource intent.",
                        )
                    return CleanupPermit(
                        allowed=prior["completed_at"] is None,
                        admission_id=prior["id"],
                        reason=(
                            None
                            if prior["completed_at"] is None
                            else "cleanup_request_already_completed"
                        ),
                    )
                active_cleanup = await conn.fetchrow(
                    "SELECT id FROM vm_workspace_cleanup_admissions "
                    "WHERE ((owner_kind=$1 AND owner_id=$2) "
                    "OR ($3::uuid IS NOT NULL AND pvc_uid=$3)) "
                    "AND completed_at IS NULL FOR UPDATE",
                    owner_kind,
                    owner_id,
                    pvc_uid,
                )
                if active_cleanup is not None:
                    return CleanupPermit(
                        allowed=False,
                        admission_id=active_cleanup["id"],
                        reason="workspace_cleanup_already_admitted",
                    )
                recovery = await conn.fetchrow(
                    "SELECT r.id FROM vm_workspace_recoveries r "
                    "LEFT JOIN vm_workspace_recovery_retention_pins pin "
                    "ON pin.recovery_id=r.id AND pin.released_at IS NULL "
                    "WHERE r.resolved_at IS NULL AND ((r.owner_kind=$1 AND r.owner_id=$2) "
                    "OR ($3::uuid IS NOT NULL AND pin.pvc_uid=$3)) FOR UPDATE OF r",
                    owner_kind,
                    owner_id,
                    pvc_uid,
                )
                if recovery is not None:
                    return CleanupPermit(
                        allowed=False,
                        recovery_id=recovery["id"],
                        reason="workspace_recovery_unresolved",
                    )
                admission_id = uuid4()
                await conn.execute(
                    "INSERT INTO vm_workspace_cleanup_admissions "
                    "(id,owner_kind,owner_id,pvc_uid,source,request_id) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    admission_id,
                    owner_kind,
                    owner_id,
                    pvc_uid,
                    source,
                    request_id,
                )
                return CleanupPermit(allowed=True, admission_id=admission_id)

    async def complete_cleanup_permit(
        self, admission_id: UUID, *, outcome: str
    ) -> bool:
        if not outcome:
            raise ValueError("cleanup outcome must be nonempty")
        async with self.db.acquire() as conn:
            changed = await conn.fetchval(
                "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(), "
                "outcome=$2 WHERE id=$1 AND completed_at IS NULL RETURNING 1",
                admission_id,
                outcome,
            )
        return changed is not None

    async def retry_paused(
        self,
        *,
        job_id: UUID,
        operation_id: UUID,
        request_id: UUID,
        actor_kind: str,
        actor_id: str,
    ) -> dict[str, Any]:
        """Atomically supersede a paused owner recovery without releasing holds."""

        digest = hashlib.sha256(f"retry:{job_id}:{operation_id}".encode()).hexdigest()
        async with self.db.acquire() as conn:
            async with conn.transaction():
                prior = await conn.fetchrow(
                    "SELECT intent_digest,accepted_result FROM vm_workspace_recovery_requests "
                    "WHERE scope_kind='recovery' AND scope_id=$1 AND request_id=$2",
                    operation_id,
                    request_id,
                )
                if prior is not None:
                    if prior["intent_digest"] != digest:
                        raise WorkspaceRecoveryControlConflict(
                            "request_id_reused",
                            "Recovery request ID was already used with different intent.",
                        )
                    return dict(_json(prior["accepted_result"]))
                observed = await conn.fetchrow(
                    "SELECT owner_kind,owner_id FROM vm_workspace_recoveries WHERE id=$1",
                    operation_id,
                )
                if observed is None:
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_recovery_not_found",
                        "Workspace recovery was not found.",
                    )
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"workspace-recovery:{observed['owner_kind']}:{observed['owner_id']}",
                )
                prior = await conn.fetchrow(
                    "SELECT intent_digest,accepted_result FROM vm_workspace_recovery_requests "
                    "WHERE scope_kind='recovery' AND scope_id=$1 AND request_id=$2",
                    operation_id,
                    request_id,
                )
                if prior is not None:
                    if prior["intent_digest"] != digest:
                        raise WorkspaceRecoveryControlConflict(
                            "request_id_reused",
                            "Recovery request ID was already used with different intent.",
                        )
                    return dict(_json(prior["accepted_result"]))
                roster = await conn.fetch(
                    "SELECT job_id FROM vm_workspace_recovery_jobs "
                    "WHERE recovery_id=$1 AND resolved_at IS NULL ORDER BY job_id",
                    operation_id,
                )
                for participant in roster:
                    await conn.fetchrow(
                        "SELECT state FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                        participant["job_id"],
                    )
                for participant in roster:
                    await conn.fetchrow(
                        "SELECT status FROM jobs WHERE id=$1 FOR UPDATE",
                        participant["job_id"],
                    )
                recovery = await conn.fetchrow(
                    "SELECT * FROM vm_workspace_recoveries WHERE id=$1 FOR UPDATE",
                    operation_id,
                )
                if recovery is None or recovery["resolved_at"] is not None:
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_recovery_resolved",
                        "Workspace recovery is already resolved.",
                    )
                if recovery["phase"] != "paused_attention":
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_recovery_not_paused",
                        "Workspace recovery is already running.",
                    )
                if recovery["owner_kind"] != "job" or recovery["owner_id"] != job_id:
                    raise WorkspaceRecoveryControlConflict(
                        "workspace_recovery_owner_required",
                        "Only the canonical workspace owner can retry recovery.",
                    )
                participants = await conn.fetch(
                    "SELECT * FROM vm_workspace_recovery_jobs WHERE recovery_id=$1 "
                    "AND resolved_at IS NULL ORDER BY job_id FOR UPDATE",
                    operation_id,
                )
                successor_id = uuid4()
                await conn.execute(
                    "UPDATE vm_workspace_recovery_jobs SET participation='transferred', "
                    "resolved_at=clock_timestamp(),outcome=COALESCE(outcome,'{}'::jsonb) "
                    "|| jsonb_build_object('successor_operation_id',$2::text) "
                    "WHERE recovery_id=$1 AND resolved_at IS NULL",
                    operation_id,
                    str(successor_id),
                )
                await conn.execute(
                    "UPDATE vm_workspace_recoveries SET phase='cancelled', "
                    "resolved_at=clock_timestamp(),claimed_by=NULL,claimed_until=NULL, "
                    "version=version+1 WHERE id=$1",
                    operation_id,
                )
                await conn.execute(
                    "INSERT INTO vm_workspace_recoveries (id,protocol_version,owner_kind,owner_id,"
                    "workspace_contract_digest,provision_generation,cluster_name,namespace,vm_uid,"
                    "prior_vmi_uid,prior_launcher_uid,root_pvc_uid,phase,reason_code,original_cause,"
                    "latest_diagnostic) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'recovering',"
                    "$13,$14,$15)",
                    successor_id,
                    recovery["protocol_version"],
                    recovery["owner_kind"],
                    recovery["owner_id"],
                    recovery["workspace_contract_digest"],
                    recovery["provision_generation"],
                    recovery["cluster_name"],
                    recovery["namespace"],
                    recovery["vm_uid"],
                    recovery["prior_vmi_uid"],
                    recovery["prior_launcher_uid"],
                    recovery["root_pvc_uid"],
                    recovery["reason_code"],
                    recovery["original_cause"],
                    recovery["latest_diagnostic"],
                )
                await conn.execute(
                    "UPDATE vm_workspace_recoveries SET phase='superseded',superseded_by=$2 "
                    "WHERE id=$1",
                    operation_id,
                    successor_id,
                )
                for participant in participants:
                    await conn.execute(
                        "INSERT INTO vm_workspace_recovery_jobs (recovery_id,job_id,"
                        "accepted_lease_token,hold_lease_token,prior_queue_state,prior_job_status,"
                        "prior_control_reference,prior_freeze_reference,checkpoint_id,checkpoint_namespace,"
                        "participation,outcome,resume_receipt) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                        successor_id,
                        participant["job_id"],
                        participant["accepted_lease_token"],
                        participant["hold_lease_token"],
                        participant["prior_queue_state"],
                        participant["prior_job_status"],
                        participant["prior_control_reference"],
                        participant["prior_freeze_reference"],
                        participant["checkpoint_id"],
                        participant["checkpoint_namespace"],
                        participant["participation"],
                        participant["outcome"],
                        participant["resume_receipt"],
                    )
                    if participant["hold_lease_token"] is not None:
                        await conn.execute(
                            "UPDATE jobs SET freeze_data=jsonb_set(freeze_data,'{recovery_id}',"
                            "to_jsonb($2::text),false) WHERE id=$1 AND status='paused' "
                            "AND freeze_data->>'recovery_id'=$3",
                            participant["job_id"],
                            str(successor_id),
                            str(operation_id),
                        )
                await conn.execute(
                    "UPDATE vm_workspace_recovery_retention_pins SET released_at=clock_timestamp() "
                    "WHERE recovery_id=$1 AND released_at IS NULL",
                    operation_id,
                )
                if (
                    recovery["root_pvc_uid"] is not None
                    and recovery["provision_generation"] is not None
                ):
                    await conn.execute(
                        "INSERT INTO vm_workspace_recovery_retention_pins "
                        "(recovery_id,pvc_uid,provision_generation) VALUES ($1,$2,$3)",
                        successor_id,
                        recovery["root_pvc_uid"],
                        recovery["provision_generation"],
                    )
                deadline_at = await conn.fetchval(
                    "SELECT deadline_at FROM vm_workspace_recoveries WHERE id=$1",
                    successor_id,
                )
                result = {
                    "status": "recovering_workspace",
                    "operation_id": str(successor_id),
                    "supersedes_operation_id": str(operation_id),
                    "deadline_at": deadline_at.isoformat(),
                }
                await conn.execute(
                    "INSERT INTO vm_workspace_recovery_requests (scope_kind,scope_id,request_id,"
                    "actor_kind,actor_id,intent_digest,recovery_id,accepted_result) "
                    "VALUES ('recovery',$1,$2,$3,$4,$5,$6,$7::jsonb)",
                    operation_id,
                    request_id,
                    actor_kind,
                    actor_id,
                    digest,
                    successor_id,
                    json.dumps(result),
                )
                return result

    async def admit_hold_from_reaper(
        self,
        conn: Any,
        *,
        disposition: RecoveryAttemptDisposition | None,
        job_id: UUID,
        lease_token: int,
        grace_seconds: float,
    ) -> bool:
        """Contain expired VM claims before generic retry/exhaustion.

        No missing/post-authorization receipt establishes replay safety.
        Local death or lease expiry also says nothing about remote commands.
        """
        if disposition is not None:
            if disposition.job_id != job_id or disposition.lease_token != lease_token:
                raise RuntimeError("reaper attempt identity mismatch")
            if disposition.requires_recovery_hold:
                return True
        from shared.workspace_contract import resolve_workspace_contract

        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
        if job is None or job["execution_lane"] != "stateless":
            return False
        contract = resolve_workspace_contract(dict(job))
        if contract.assigned_backend != "vm":
            return False
        owner_id, _ = _job_workspace_owner(job_id, job)
        owner = (
            job
            if owner_id == job_id
            else await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", owner_id)
        )
        context = _json(owner["context"]) if owner else {}
        vm = context.get("vm", {}) if isinstance(context, dict) else {}
        if not isinstance(vm, dict):
            vm = {}

        def identifier(key: str) -> UUID | None:
            try:
                return UUID(str(vm.get(key)))
            except (ValueError, TypeError):
                return None

        request_id = uuid5(NAMESPACE_URL, f"workspace-reaper:{job_id}:{lease_token}")
        digest = hashlib.sha256(
            json.dumps(
                contract.to_context(), sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        await self.admit_hold(
            job_id=job_id,
            accepted_lease_token=lease_token,
            owner_kind="job",
            owner_id=owner_id,
            workspace_contract_digest=digest,
            provision_generation=identifier("provision_generation"),
            cluster_name=os.getenv("VM_CLUSTER_NAME", "local").strip() or "local",
            namespace=vm.get("namespace"),
            vm_uid=identifier("vm_uid"),
            prior_vmi_uid=identifier("vmi_uid"),
            prior_launcher_uid=identifier("active_pod_uid"),
            root_pvc_uid=identifier("rootdisk_pvc_uid"),
            code=(
                WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
                if disposition is None or disposition.bundle_authorized
                else WorkspaceRecoveryCode.RUNTIME_NOT_READY
            ),
            request_id=request_id,
            actor_kind="reaper",
            actor_id=self.worker_id,
            intent_digest=str(request_id),
            _conn=conn,
            expired_grace_seconds=grace_seconds,
        )
        return True

    async def get_attempt_disposition(
        self, conn: Any, *, job_id: UUID, lease_token: int
    ) -> RecoveryAttemptDisposition | None:
        return await get_worker_attempt_disposition(
            conn,
            job_id=job_id,
            lease_token=lease_token,
        )

    async def record_bundle_authorized(
        self,
        conn: Any,
        *,
        job_id: UUID,
        lease_token: int,
        authority_digest: str,
    ) -> bool:
        return await record_worker_bundle_authorized(
            conn,
            job_id=job_id,
            lease_token=lease_token,
            authority_digest=authority_digest,
        )

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
                "SELECT job_id, hold_lease_token, prior_job_status FROM vm_workspace_recovery_jobs "
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
                    changed = await release_worker_batch_from_workspace_recovery(
                        conn,
                        job_id=participant["job_id"],
                        recovery_id=operation_id,
                        hold_lease_token=participant["hold_lease_token"],
                        version=version,
                        claim_token=claim_token,
                        resume_receipt=dict(resume_receipt),
                    )
                    if not changed:
                        raise RuntimeError("workspace recovery hold token changed")
                    job_changed = await conn.fetchval(
                        "UPDATE jobs SET freeze_data=NULL, "
                        "status=CASE WHEN $4::text='created' THEN 'created' "
                        "ELSE status END "
                        "WHERE id=$1 AND status='paused' "
                        "AND freeze_data->>'freeze_type'='workspace_recovery' "
                        "AND freeze_data->>'recovery_id'=$2 "
                        "AND freeze_data->>'hold_lease_token'=$3 "
                        "RETURNING 1",
                        participant["job_id"],
                        str(operation_id),
                        str(participant["hold_lease_token"]),
                        participant["prior_job_status"],
                    )
                    if job_changed is None:
                        raise RuntimeError("workspace recovery job projection changed")
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


__all__ = [
    "CleanupPermit",
    "RecoveryClaim",
    "VMWorkspaceRecoveryStore",
    "WorkspaceRecoveryControlConflict",
    "acquire_vm_cleanup_permit",
    "complete_vm_cleanup_permit",
]
