"""Short PostgreSQL transactions for durable VM workspace recovery."""

from __future__ import annotations

import hashlib
import json
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


def _participant_continuation_safe(participant: Mapping[str, Any]) -> bool:
    if (
        participant.get("checkpoint_id") is not None
        and participant.get("checkpoint_namespace") is not None
    ):
        return True
    reference = _json(participant.get("prior_control_reference"))
    if not isinstance(reference, Mapping):
        return False
    if reference.get("never_started") is True:
        return True
    attempt = reference.get("attempt")
    return bool(
        isinstance(attempt, Mapping) and attempt.get("bundle_authorized_at") is None
    )


def _required_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _observation_authority_error(
    operation: Mapping[str, Any], observation: object
) -> str | None:
    if not isinstance(observation, Mapping):
        return "attestation_missing"
    for key in (
        "owner_kind",
        "owner_id",
        "provision_generation",
        "vm_uid",
        "root_pvc_uid",
    ):
        if str(observation.get(key) or "") != str(operation.get(key) or ""):
            return f"attestation_{key}_mismatch"
    if observation.get("ambiguous") is not False:
        return "attestation_ambiguous"
    prior_runtime = observation.get("prior_runtime")
    if prior_runtime not in {"stopped", "same_runtime"}:
        return "prior_runtime_not_fenced"
    if observation.get("ready") is not True:
        return "successor_not_ready"
    if observation.get("authenticated") is not True:
        return "successor_not_authenticated"
    if observation.get("continuation") not in {"safe", "not_started"}:
        return "continuation_not_safe"
    if observation.get("remote_operations") != "settled":
        return "remote_operations_unsettled"
    successor = observation.get("successor")
    if not isinstance(successor, Mapping):
        return "successor_authority_missing"
    for key in (
        "vmi_uid",
        "launcher_uid",
        "node_uid",
        "pod_ip",
        "ssh_registration_id",
    ):
        if not _required_text(successor.get(key)):
            return f"successor_{key}_missing"
    for key in ("vmi_uid", "launcher_uid"):
        try:
            UUID(str(successor[key]))
        except (TypeError, ValueError, AttributeError):
            return f"successor_{key}_malformed"
    replacing = str(successor["launcher_uid"]) != str(
        operation.get("prior_launcher_uid") or ""
    )
    if replacing:
        if prior_runtime != "stopped":
            return "replacement_prior_runtime_not_stopped"
        if not _required_text(observation.get("stop_receipt_digest")):
            return "replacement_stop_receipt_missing"
    return None


def _attestation_authority_key(observation: Mapping[str, Any]) -> tuple[str, ...]:
    successor = observation["successor"]
    assert isinstance(successor, Mapping)
    return tuple(
        str(value or "")
        for value in (
            observation.get("owner_kind"),
            observation.get("owner_id"),
            observation.get("provision_generation"),
            observation.get("vm_uid"),
            observation.get("root_pvc_uid"),
            observation.get("prior_runtime"),
            observation.get("stop_receipt_digest"),
            successor.get("vmi_uid"),
            successor.get("launcher_uid"),
            successor.get("node_uid"),
            successor.get("pod_ip"),
            successor.get("ssh_registration_id"),
        )
    )


def _attestation_error_code(error: str) -> WorkspaceRecoveryCode:
    if error.startswith(("prior_runtime", "replacement_")):
        return WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED
    if error.startswith(("continuation_", "remote_operations_")):
        return WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    if error.startswith("successor_not_"):
        return WorkspaceRecoveryCode.RUNTIME_NOT_READY
    return WorkspaceRecoveryCode.IDENTITY_CONFLICT


@dataclass(frozen=True, slots=True)
class RecoveryClaim:
    operation_id: UUID
    version: int
    claim_token: int
    deadline_at: datetime
    remaining_seconds: float
    captured_identity: Mapping[str, Any]
    attempt: int = 1
    global_slot: int | None = None
    node_key: str | None = None


@dataclass(frozen=True, slots=True)
class CleanupPermit:
    allowed: bool
    admission_id: UUID | None = None
    recovery_id: UUID | None = None
    reason: str | None = None
    completed_outcome: str | None = None


@dataclass(frozen=True, slots=True)
class RetentionPinCommand:
    recovery_id: UUID
    pvc_uid: UUID
    provision_generation: UUID
    desired_state: str
    owner_kind: str
    owner_id: UUID
    namespace: str
    controller_pin_uid: str | None = None
    controller_pin_resource_version: str | None = None


class WorkspaceRecoveryControlConflict(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _RecoveryClaimLost(RuntimeError):
    """Abort a final transaction whose durable claim expired mid-commit."""


def _cleanup_uuid(value: Any, *, namespace: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return uuid5(NAMESPACE_URL, f"{namespace}:{value}")


def cleanup_intent_digest(intent: Mapping[str, Any]) -> str:
    """Return a stable digest for the complete destructive resource intent."""

    encoded = json.dumps(
        intent,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def completed_cleanup_outcome(permit: Any) -> str | None:
    """Read a durable replay outcome without trusting loose test doubles."""

    value = getattr(permit, "completed_outcome", None)
    return value if isinstance(value, str) and value else None


async def acquire_vm_cleanup_permit(
    recovery_store: Any,
    *,
    owner_kind: str,
    owner_id: str | UUID,
    identity: Any,
    source: str,
    purge_disk: bool,
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
    intent_digest = cleanup_intent_digest(
        {
            "owner_kind": owner_kind,
            "owner_id": str(canonical_owner),
            "provision_generation": str(getattr(identity, "provision_generation", "")),
            "vm_uid": str(getattr(identity, "vm_uid", "") or ""),
            "pvc_uid": str(pvc_uid or ""),
            "purge_disk": bool(purge_disk),
            "resource": "vm_workspace",
            "source": source,
        }
    )
    return await recovery_store.acquire_cleanup_permit(
        owner_kind=owner_kind,
        owner_id=canonical_owner,
        pvc_uid=pvc_uid,
        request_id=uuid5(NAMESPACE_URL, f"vm-workspace-cleanup:{intent}"),
        source=source,
        intent_digest=intent_digest,
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
             WHERE id=$1 AND phase IN (
                 'recovering','observing','waiting_runtime','verifying_stop',
                 'attesting','reconciling_outcome'
             ) AND resolved_at IS NULL
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
        await conn.execute(
            "DELETE FROM vm_workspace_recovery_probe_slots WHERE recovery_id=$1",
            operation_id,
        )

    @staticmethod
    async def _pause_expired_claim_locked(
        conn: Any,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        worker_id: str,
    ) -> bool:
        changed = await conn.fetchval(
            """
            UPDATE vm_workspace_recoveries
               SET phase='paused_attention',
                   reason_code='workspace_recovery_deadline_exceeded',
                   latest_diagnostic=jsonb_build_object(
                       'reason','deadline_elapsed_before_probe_result',
                       'observed_at',clock_timestamp()),
                   claimed_by=NULL,claimed_until=NULL,version=version+1
             WHERE id=$1 AND version=$2 AND claim_token=$3
               AND phase IN (
                   'recovering','observing','waiting_runtime','verifying_stop',
                   'attesting','reconciling_outcome'
               ) AND resolved_at IS NULL
               AND deadline_at <= clock_timestamp()
               AND claimed_by=$4 AND claimed_until > clock_timestamp()
               AND EXISTS (
                   SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                    WHERE slot.recovery_id=vm_workspace_recoveries.id
                      AND slot.claim_token=$3
                      AND slot.leased_until > clock_timestamp()
               )
            RETURNING 1
            """,
            operation_id,
            version,
            claim_token,
            worker_id,
        )
        if changed is None:
            return False
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='attention' "
            "WHERE recovery_id=$1 AND resolved_at IS NULL",
            operation_id,
        )
        await conn.execute(
            "DELETE FROM vm_workspace_recovery_probe_slots "
            "WHERE recovery_id=$1 AND claim_token=$2",
            operation_id,
            claim_token,
        )
        return True

    @staticmethod
    async def _pause_claim_locked(
        conn: Any,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        worker_id: str,
        code: WorkspaceRecoveryCode,
        diagnostic: Mapping[str, Any],
    ) -> bool:
        """Pause only while the exact row and durable probe slot remain live."""

        changed = await conn.fetchval(
            """
            UPDATE vm_workspace_recoveries
               SET phase='paused_attention', reason_code=$5,
                   latest_diagnostic=$6::jsonb,
                   claimed_by=NULL, claimed_until=NULL,
                   version=version+1
             WHERE id=$1 AND version=$2 AND claim_token=$3
               AND claimed_by=$4 AND claimed_until > clock_timestamp()
               AND phase IN (
                   'recovering','observing','waiting_runtime','verifying_stop',
                   'attesting','reconciling_outcome'
               ) AND resolved_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                    WHERE slot.recovery_id=vm_workspace_recoveries.id
                      AND slot.claim_token=$3
                      AND slot.leased_until > clock_timestamp()
               )
            RETURNING 1
            """,
            operation_id,
            version,
            claim_token,
            worker_id,
            code.value,
            json.dumps(dict(diagnostic)),
        )
        if changed is None:
            return False
        await conn.execute(
            "UPDATE vm_workspace_recovery_jobs SET participation='attention' "
            "WHERE recovery_id=$1 AND resolved_at IS NULL",
            operation_id,
        )
        await conn.execute(
            "DELETE FROM vm_workspace_recovery_probe_slots "
            "WHERE recovery_id=$1 AND claim_token=$2",
            operation_id,
            claim_token,
        )
        return True

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
        intent_digest: str,
    ) -> CleanupPermit:
        """Serialize destructive admission with recovery and its exact disk pin."""

        if not isinstance(intent_digest, str) or not intent_digest:
            raise ValueError("cleanup intent digest must be nonempty")

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
                    "SELECT id,completed_at,pvc_uid,source,intent_digest,outcome "
                    "FROM vm_workspace_cleanup_admissions "
                    "WHERE owner_kind=$1 AND owner_id=$2 AND request_id=$3 FOR UPDATE",
                    owner_kind,
                    owner_id,
                    request_id,
                )
                if prior is not None:
                    if (
                        prior["pvc_uid"] != pvc_uid
                        or prior["source"] != source
                        or prior["intent_digest"] != intent_digest
                    ):
                        raise WorkspaceRecoveryControlConflict(
                            "cleanup_request_id_reused",
                            "Cleanup request ID was already used with different resource intent.",
                        )
                    return CleanupPermit(
                        allowed=True,
                        admission_id=prior["id"],
                        reason=(
                            None
                            if prior["completed_at"] is None
                            else "cleanup_request_already_completed"
                        ),
                        completed_outcome=(
                            prior["outcome"]
                            if prior["completed_at"] is not None
                            else None
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
                    "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    admission_id,
                    owner_kind,
                    owner_id,
                    pvc_uid,
                    source,
                    request_id,
                    intent_digest,
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
                unresolved_attention = any(
                    participant["hold_lease_token"] is None
                    for participant in participants
                )
                successor_phase = (
                    "paused_attention" if unresolved_attention else "recovering"
                )
                await conn.execute(
                    "INSERT INTO vm_workspace_recoveries (id,protocol_version,owner_kind,owner_id,"
                    "workspace_contract_digest,provision_generation,cluster_name,namespace,vm_uid,"
                    "prior_vmi_uid,prior_launcher_uid,root_pvc_uid,phase,reason_code,original_cause,"
                    "latest_diagnostic) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,"
                    "$14,$15,$16)",
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
                    successor_phase,
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
                        "held"
                        if participant["hold_lease_token"] is not None
                        else "attention",
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

    async def list_retention_pin_commands(
        self, *, limit: int = 32
    ) -> list[RetentionPinCommand]:
        """Return durable controller pin work without claiming it as completed."""

        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT pin.recovery_id,pin.pvc_uid,pin.provision_generation,
                       pin.released_at,pin.controller_pinned_at,
                       pin.controller_pin_uid,pin.controller_pin_resource_version,
                       recovery.owner_kind,recovery.owner_id,recovery.namespace
                  FROM vm_workspace_recovery_retention_pins pin
                  JOIN vm_workspace_recoveries recovery ON recovery.id=pin.recovery_id
                 WHERE pin.controller_sync_after <= clock_timestamp()
                   AND ((pin.released_at IS NULL
                         AND pin.controller_pinned_at IS NULL)
                        OR (pin.released_at IS NOT NULL
                            AND pin.controller_pinned_at IS NOT NULL
                            AND pin.controller_released_at IS NULL))
                 ORDER BY pin.controller_sync_after,pin.pinned_at
                 LIMIT $1
                """,
                max(1, limit),
            )
        return [
            RetentionPinCommand(
                recovery_id=row["recovery_id"],
                pvc_uid=row["pvc_uid"],
                provision_generation=row["provision_generation"],
                desired_state="released" if row["released_at"] else "active",
                owner_kind=row["owner_kind"],
                owner_id=row["owner_id"],
                namespace=row["namespace"],
                controller_pin_uid=row["controller_pin_uid"],
                controller_pin_resource_version=row["controller_pin_resource_version"],
            )
            for row in rows
        ]

    async def retention_pin_command(
        self, claim: RecoveryClaim
    ) -> RetentionPinCommand | None:
        """Return the exact active-pin command for one current recovery claim."""

        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT pin.recovery_id,pin.pvc_uid,pin.provision_generation,
                       pin.controller_pinned_at,pin.controller_pin_uid,
                       pin.controller_pin_resource_version,
                       recovery.owner_kind,recovery.owner_id,recovery.namespace
                  FROM vm_workspace_recovery_retention_pins pin
                  JOIN vm_workspace_recoveries recovery ON recovery.id=pin.recovery_id
                  JOIN vm_workspace_recovery_probe_slots slot
                    ON slot.recovery_id=recovery.id
                   AND slot.claim_token=recovery.claim_token
                 WHERE recovery.id=$1 AND recovery.version=$2
                   AND recovery.claim_token=$3 AND recovery.claimed_by=$4
                   AND recovery.claimed_until > clock_timestamp()
                   AND slot.leased_until > clock_timestamp()
                   AND recovery.resolved_at IS NULL
                   AND pin.released_at IS NULL
                   AND pin.pvc_uid=$5 AND pin.provision_generation=$6
                """,
                claim.operation_id,
                claim.version,
                claim.claim_token,
                self.worker_id,
                claim.captured_identity.get("root_pvc_uid"),
                claim.captured_identity.get("provision_generation"),
            )
        if row is None:
            return None
        return RetentionPinCommand(
            recovery_id=row["recovery_id"],
            pvc_uid=row["pvc_uid"],
            provision_generation=row["provision_generation"],
            desired_state="active",
            owner_kind=row["owner_kind"],
            owner_id=row["owner_id"],
            namespace=row["namespace"],
            controller_pin_uid=row["controller_pin_uid"],
            controller_pin_resource_version=row["controller_pin_resource_version"],
        )

    async def acknowledge_retention_pin(
        self, command: RetentionPinCommand, result: Mapping[str, Any]
    ) -> bool:
        """Persist only an exact controller acknowledgement of the desired state."""

        if result.get("state") != command.desired_state:
            return False
        if str(result.get("recovery_id") or "") != str(command.recovery_id):
            return False
        if str(result.get("pvc_uid") or "") != str(command.pvc_uid):
            return False
        if str(result.get("provision_generation") or "") != str(
            command.provision_generation
        ):
            return False
        pin_uid = result.get("pin_uid")
        resource_version = result.get("resource_version")
        if not isinstance(pin_uid, str) or not pin_uid:
            return False
        if not isinstance(resource_version, str) or not resource_version:
            return False
        async with self.db.acquire() as conn:
            if command.desired_state == "active":
                changed = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recovery_retention_pins
                       SET controller_pinned_at=COALESCE(controller_pinned_at,
                                                        clock_timestamp()),
                           controller_pin_uid=$4,
                           controller_pin_resource_version=$5,
                           controller_sync_attempts=controller_sync_attempts+1,
                           controller_sync_error=NULL,
                           controller_sync_after=clock_timestamp()
                     WHERE recovery_id=$1 AND pvc_uid=$2
                       AND provision_generation=$3 AND released_at IS NULL
                    RETURNING 1
                    """,
                    command.recovery_id,
                    command.pvc_uid,
                    command.provision_generation,
                    pin_uid,
                    resource_version,
                )
            else:
                changed = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recovery_retention_pins
                       SET controller_release_requested_at=COALESCE(
                               controller_release_requested_at,clock_timestamp()),
                           controller_released_at=clock_timestamp(),
                           controller_sync_attempts=controller_sync_attempts+1,
                           controller_sync_error=NULL,
                           controller_sync_after=clock_timestamp()
                     WHERE recovery_id=$1 AND pvc_uid=$2
                       AND provision_generation=$3 AND released_at IS NOT NULL
                       AND controller_released_at IS NULL
                       AND controller_pin_uid=$4
                    RETURNING 1
                    """,
                    command.recovery_id,
                    command.pvc_uid,
                    command.provision_generation,
                    pin_uid,
                )
        return changed is not None

    async def defer_retention_pin_command(
        self, command: RetentionPinCommand, *, error: str
    ) -> None:
        async with self.db.acquire() as conn:
            await conn.execute(
                """
                UPDATE vm_workspace_recovery_retention_pins
                   SET controller_sync_attempts=controller_sync_attempts+1,
                       controller_sync_error=$4,
                       controller_sync_after=clock_timestamp()+interval '10 seconds'
                 WHERE recovery_id=$1 AND pvc_uid=$2 AND provision_generation=$3
                   AND controller_released_at IS NULL
                """,
                command.recovery_id,
                command.pvc_uid,
                command.provision_generation,
                error[:500],
            )

    async def retention_pin_is_acknowledged(self, claim: RecoveryClaim) -> bool:
        async with self.db.acquire() as conn:
            return bool(
                await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1
                          FROM vm_workspace_recovery_retention_pins pin
                          JOIN vm_workspace_recoveries recovery
                            ON recovery.id=pin.recovery_id
                          JOIN vm_workspace_recovery_probe_slots slot
                            ON slot.recovery_id=recovery.id
                           AND slot.claim_token=recovery.claim_token
                         WHERE recovery.id=$1 AND recovery.version=$2
                           AND recovery.claim_token=$3
                           AND recovery.claimed_by=$4
                           AND recovery.claimed_until > clock_timestamp()
                           AND slot.leased_until > clock_timestamp()
                           AND recovery.resolved_at IS NULL
                           AND pin.pvc_uid=$5 AND pin.provision_generation=$6
                           AND pin.released_at IS NULL
                           AND pin.controller_pinned_at IS NOT NULL
                           AND pin.controller_released_at IS NULL
                    )
                    """,
                    claim.operation_id,
                    claim.version,
                    claim.claim_token,
                    self.worker_id,
                    claim.captured_identity.get("root_pvc_uid"),
                    claim.captured_identity.get("provision_generation"),
                )
            )

    async def accept_stop_evidence(
        self, claim: RecoveryClaim, evidence: Mapping[str, Any]
    ) -> str | None:
        """Append exact controller evidence while the accepting term is live."""

        required = (
            "vm_uid",
            "vmi_uid",
            "launcher_uid",
            "container_id",
            "root_pvc_uid",
            "controller_identity",
            "observed_at",
        )
        if evidence.get("protocol_version") != 1 or any(
            not isinstance(evidence.get(key), str) or not evidence.get(key)
            for key in required
        ):
            return None
        if any(
            str(evidence.get(key)) != str(claim.captured_identity.get(captured))
            for key, captured in (
                ("vm_uid", "vm_uid"),
                ("vmi_uid", "prior_vmi_uid"),
                ("launcher_uid", "prior_launcher_uid"),
                ("root_pvc_uid", "root_pvc_uid"),
            )
        ):
            return None
        canonical = dict(evidence)
        supplied_digest = canonical.pop("evidence_digest", None)
        digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode("utf-8")
            ).hexdigest()
        )
        if supplied_digest not in {None, digest}:
            return None
        try:
            observed_at = datetime.fromisoformat(
                str(evidence["observed_at"]).replace("Z", "+00:00")
            )
        except ValueError:
            return None
        async with self.db.acquire() as conn:
            inserted = await conn.fetchval(
                """
                INSERT INTO vm_workspace_recovery_stop_receipts (
                    recovery_id,accepted_claim_token,vm_uid,vmi_uid,launcher_uid,
                    container_id,root_pvc_uid,controller_identity,observed_at,
                    evidence,evidence_digest
                )
                SELECT recovery.id,$3,recovery.vm_uid,recovery.prior_vmi_uid,
                       recovery.prior_launcher_uid,$5,recovery.root_pvc_uid,$6,$7,
                       $8::jsonb,$9
                  FROM vm_workspace_recoveries recovery
                  JOIN vm_workspace_recovery_probe_slots slot
                    ON slot.recovery_id=recovery.id
                   AND slot.claim_token=recovery.claim_token
                 WHERE recovery.id=$1 AND recovery.version=$2
                   AND recovery.claim_token=$3 AND recovery.claimed_by=$4
                   AND recovery.claimed_until > clock_timestamp()
                   AND slot.leased_until > clock_timestamp()
                   AND recovery.resolved_at IS NULL
                   AND recovery.vm_uid=$10 AND recovery.prior_vmi_uid=$11
                   AND recovery.prior_launcher_uid=$12
                   AND recovery.root_pvc_uid=$13
                ON CONFLICT (recovery_id,evidence_digest) DO NOTHING
                RETURNING evidence_digest
                """,
                claim.operation_id,
                claim.version,
                claim.claim_token,
                self.worker_id,
                evidence["container_id"],
                evidence["controller_identity"],
                observed_at,
                json.dumps(canonical),
                digest,
                claim.captured_identity.get("vm_uid"),
                claim.captured_identity.get("prior_vmi_uid"),
                claim.captured_identity.get("prior_launcher_uid"),
                claim.captured_identity.get("root_pvc_uid"),
            )
            if inserted is not None:
                return str(inserted)
            return await conn.fetchval(
                """
                SELECT receipt.evidence_digest
                  FROM vm_workspace_recovery_stop_receipts receipt
                  JOIN vm_workspace_recoveries recovery
                    ON recovery.id=receipt.recovery_id
                  JOIN vm_workspace_recovery_probe_slots slot
                    ON slot.recovery_id=recovery.id
                   AND slot.claim_token=recovery.claim_token
                 WHERE recovery.id=$1 AND recovery.version=$3
                   AND recovery.claim_token=$4 AND recovery.claimed_by=$5
                   AND recovery.claimed_until > clock_timestamp()
                   AND slot.leased_until > clock_timestamp()
                   AND recovery.resolved_at IS NULL
                   AND receipt.evidence_digest=$2
                   AND receipt.vm_uid=recovery.vm_uid
                   AND receipt.vmi_uid=recovery.prior_vmi_uid
                   AND receipt.launcher_uid=recovery.prior_launcher_uid
                   AND receipt.root_pvc_uid=recovery.root_pvc_uid
                """,
                claim.operation_id,
                digest,
                claim.version,
                claim.claim_token,
                self.worker_id,
            )

    async def trusted_stop_receipt(self, claim: RecoveryClaim) -> str | None:
        """Read historical positive evidence for the exact captured incarnation."""

        async with self.db.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT receipt.evidence_digest
                  FROM vm_workspace_recovery_stop_receipts receipt
                  JOIN vm_workspace_recoveries recovery
                    ON recovery.id=receipt.recovery_id
                  JOIN vm_workspace_recovery_probe_slots slot
                    ON slot.recovery_id=recovery.id
                   AND slot.claim_token=recovery.claim_token
                 WHERE recovery.id=$1 AND recovery.version=$2
                   AND recovery.claim_token=$3 AND recovery.claimed_by=$4
                   AND recovery.claimed_until > clock_timestamp()
                   AND slot.leased_until > clock_timestamp()
                   AND recovery.resolved_at IS NULL
                   AND receipt.vm_uid=recovery.vm_uid
                   AND receipt.vmi_uid=recovery.prior_vmi_uid
                   AND receipt.launcher_uid=recovery.prior_launcher_uid
                   AND receipt.root_pvc_uid=recovery.root_pvc_uid
                 ORDER BY receipt.accepted_at
                 LIMIT 1
                """,
                claim.operation_id,
                claim.version,
                claim.claim_token,
                self.worker_id,
            )

    async def claim_due(
        self,
        operation_id: UUID,
        *,
        ttl_seconds: float = 30,
        max_global_probes: int = 4,
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
                     WHERE id=$1 AND phase IN (
                         'recovering','observing','waiting_runtime','verifying_stop',
                         'attesting','reconciling_outcome'
                     ) AND resolved_at IS NULL
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
                # Slot selection spans two UNIQUE dimensions. Serialize only
                # this tiny database allocation section so overlapping leaders
                # cannot both choose the same free integer before either insert.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    "vm-workspace-recovery-probe-slots",
                )
                await conn.execute(
                    "DELETE FROM vm_workspace_recovery_probe_slots "
                    "WHERE leased_until <= clock_timestamp()"
                )
                candidate = await conn.fetchrow(
                    """
                    SELECT id,
                           COALESCE(
                               latest_observation->'successor'->>'node_uid',
                               latest_observation->>'node_uid',
                               'unknown:' || id::text
                           ) AS node_key
                      FROM vm_workspace_recoveries
                     WHERE id=$1 AND phase IN (
                         'recovering','observing','waiting_runtime','verifying_stop',
                         'attesting','reconciling_outcome'
                     ) AND resolved_at IS NULL
                       AND next_check_at <= clock_timestamp()
                       AND deadline_at > clock_timestamp()
                       AND (claimed_until IS NULL OR claimed_until <= clock_timestamp())
                       AND NOT EXISTS (
                           SELECT 1 FROM vm_workspace_recovery_jobs participant
                            WHERE participant.recovery_id=vm_workspace_recoveries.id
                              AND participant.resolved_at IS NULL
                              AND participant.participation='attention'
                       )
                     FOR UPDATE
                    """,
                    operation_id,
                )
                if candidate is None:
                    return None
                node_busy = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_probe_slots "
                    "WHERE node_key=$1 AND leased_until > clock_timestamp())",
                    candidate["node_key"],
                )
                if node_busy:
                    return None
                global_slot = await conn.fetchval(
                    """
                    SELECT slot
                      FROM generate_series(0, $1::integer - 1) AS slots(slot)
                     WHERE NOT EXISTS (
                         SELECT 1 FROM vm_workspace_recovery_probe_slots active
                          WHERE active.global_slot=slot
                            AND active.leased_until > clock_timestamp()
                     )
                     ORDER BY slot
                     LIMIT 1
                    """,
                    max(1, max_global_probes),
                )
                if global_slot is None:
                    return None
                row = await conn.fetchrow(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='observing', claimed_by=$2,
                           claimed_until=clock_timestamp()
                               + make_interval(secs => $3::double precision),
                           claim_token=claim_token+1,
                           recovery_attempts=recovery_attempts+1,
                           version=version+1
                     WHERE id=$1 AND resolved_at IS NULL
                    RETURNING id, version, claim_token, deadline_at,
                              extract(epoch FROM
                                  (deadline_at-clock_timestamp()))::float8
                                  AS remaining_seconds,
                              recovery_attempts,
                              owner_kind, owner_id,
                              provision_generation, cluster_name, namespace,
                              vm_uid, prior_vmi_uid, prior_launcher_uid, root_pvc_uid
                    """,
                    operation_id,
                    self.worker_id,
                    ttl_seconds,
                )
                if row is not None:
                    await conn.execute(
                        "INSERT INTO vm_workspace_recovery_probe_slots "
                        "(recovery_id,global_slot,node_key,claim_token,leased_until) "
                        "VALUES ($1,$2,$3,$4,clock_timestamp() "
                        "+ make_interval(secs => $5::double precision))",
                        operation_id,
                        global_slot,
                        candidate["node_key"],
                        row["claim_token"],
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
                "owner_kind": row["owner_kind"],
                "owner_id": row["owner_id"],
                "provision_generation": row["provision_generation"],
                "cluster_name": row["cluster_name"],
                "namespace": row["namespace"],
                "vm_uid": row["vm_uid"],
                "prior_vmi_uid": row["prior_vmi_uid"],
                "prior_launcher_uid": row["prior_launcher_uid"],
                "root_pvc_uid": row["root_pvc_uid"],
            },
            attempt=int(row["recovery_attempts"]),
            global_slot=int(global_slot),
            node_key=str(candidate["node_key"]),
        )

    async def list_due_operation_ids(self, *, limit: int = 32) -> list[UUID]:
        """Return hints for due automatic work; claims remain authoritative."""

        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id
                  FROM vm_workspace_recoveries
                 WHERE phase IN (
                     'recovering','observing','waiting_runtime','verifying_stop',
                     'attesting','reconciling_outcome'
                 ) AND resolved_at IS NULL
                   AND next_check_at <= clock_timestamp()
                 ORDER BY next_check_at, deadline_at, id
                 LIMIT $1
                """,
                max(1, limit),
            )
        return [row["id"] for row in rows]

    async def claim_is_current(self, claim: RecoveryClaim) -> bool:
        async with self.db.acquire() as conn:
            return bool(
                await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1
                          FROM vm_workspace_recoveries r
                          JOIN vm_workspace_recovery_probe_slots slot
                            ON slot.recovery_id=r.id
                         WHERE r.id=$1 AND r.version=$2 AND r.claim_token=$3
                           AND r.claimed_by=$4
                           AND r.claimed_until > clock_timestamp()
                           AND r.resolved_at IS NULL
                           AND slot.claim_token=$3
                           AND slot.leased_until > clock_timestamp()
                    )
                    """,
                    claim.operation_id,
                    claim.version,
                    claim.claim_token,
                    self.worker_id,
                )
            )

    async def renew_claim(
        self, claim: RecoveryClaim, *, ttl_seconds: float = 30
    ) -> RecoveryClaim | None:
        """Extend one live row+slot claim without changing its fencing term."""

        async with self.db.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow(
                    """
                    SELECT r.deadline_at,
                           extract(epoch FROM
                               (r.deadline_at-clock_timestamp()))::float8
                               AS remaining_seconds
                      FROM vm_workspace_recoveries r
                      JOIN vm_workspace_recovery_probe_slots slot
                        ON slot.recovery_id=r.id AND slot.claim_token=r.claim_token
                     WHERE r.id=$1 AND r.version=$2 AND r.claim_token=$3
                       AND r.claimed_by=$4
                       AND r.claimed_until > clock_timestamp()
                       AND slot.leased_until > clock_timestamp()
                       AND r.deadline_at > clock_timestamp()
                       AND r.resolved_at IS NULL
                     FOR UPDATE OF r,slot
                    """,
                    claim.operation_id,
                    claim.version,
                    claim.claim_token,
                    self.worker_id,
                )
                if current is None:
                    return None
                remaining = max(0.0, float(current["remaining_seconds"]))
                extension = min(max(0.1, ttl_seconds), remaining)
                await conn.execute(
                    "UPDATE vm_workspace_recoveries "
                    "SET claimed_until=clock_timestamp()+make_interval(secs=>$2::float8) "
                    "WHERE id=$1",
                    claim.operation_id,
                    extension,
                )
                await conn.execute(
                    "UPDATE vm_workspace_recovery_probe_slots "
                    "SET leased_until=clock_timestamp()+make_interval(secs=>$2::float8) "
                    "WHERE recovery_id=$1 AND claim_token=$3",
                    claim.operation_id,
                    extension,
                    claim.claim_token,
                )
        return RecoveryClaim(
            operation_id=claim.operation_id,
            version=claim.version,
            claim_token=claim.claim_token,
            deadline_at=current["deadline_at"],
            remaining_seconds=remaining,
            captured_identity=claim.captured_identity,
            attempt=claim.attempt,
            global_slot=claim.global_slot,
            node_key=claim.node_key,
        )

    async def recovery_preconditions(
        self, claim: RecoveryClaim
    ) -> Mapping[str, str] | None:
        """Read DB-owned continuation evidence for one still-current claim."""

        async with self.db.acquire() as conn:
            current = await conn.fetchrow(
                """
                SELECT owner_kind,owner_id
                  FROM vm_workspace_recoveries
                 WHERE id=$1 AND version=$2 AND claim_token=$3
                   AND claimed_by=$4 AND claimed_until > clock_timestamp()
                   AND resolved_at IS NULL AND deadline_at > clock_timestamp()
                   AND EXISTS (
                       SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                        WHERE slot.recovery_id=vm_workspace_recoveries.id
                          AND slot.claim_token=$3
                          AND slot.leased_until > clock_timestamp()
                   )
                """,
                claim.operation_id,
                claim.version,
                claim.claim_token,
                self.worker_id,
            )
            if current is None:
                return None
            participants = await conn.fetch(
                "SELECT participation,checkpoint_id,checkpoint_namespace,"
                "prior_control_reference FROM vm_workspace_recovery_jobs "
                "WHERE recovery_id=$1 AND resolved_at IS NULL",
                claim.operation_id,
            )
            unsettled_remote = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_remote_operation_leases "
                "WHERE owner_kind=$1 AND owner_id=$2 AND settled_at IS NULL)",
                current["owner_kind"],
                current["owner_id"],
            )
        continuation_safe = bool(participants) and all(
            participant["participation"] == "held"
            and _participant_continuation_safe(participant)
            for participant in participants
        )
        return {
            "continuation": "safe" if continuation_safe else "unknown",
            "remote_operations": "pending" if unsettled_remote else "settled",
        }

    async def _finish_claim(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        phase: str,
        observation: Mapping[str, Any] | None,
        diagnostic: Mapping[str, Any] | None,
        next_check_seconds: float,
        retain_claim: bool,
    ) -> RecoveryClaim | bool | None:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase=$4,
                           latest_observation=COALESCE($5::jsonb, latest_observation),
                           latest_diagnostic=COALESCE($6::jsonb, latest_diagnostic),
                           last_progress_at=clock_timestamp(),
                           next_check_at=clock_timestamp()
                               + make_interval(secs => $7::double precision),
                           claimed_by=CASE WHEN $8 THEN claimed_by ELSE NULL END,
                           claimed_until=CASE WHEN $8 THEN claimed_until ELSE NULL END,
                           version=version+1
                     WHERE id=$1 AND version=$2 AND claim_token=$3
                       AND resolved_at IS NULL
                       AND deadline_at > clock_timestamp()
                       AND claimed_by=$9
                       AND claimed_until > clock_timestamp()
                       AND EXISTS (
                           SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                            WHERE slot.recovery_id=vm_workspace_recoveries.id
                              AND slot.claim_token=$3
                              AND slot.leased_until > clock_timestamp()
                       )
                    RETURNING id,version,claim_token,deadline_at,
                              extract(epoch FROM
                                  (deadline_at-clock_timestamp()))::float8
                                  AS remaining_seconds,
                              recovery_attempts,owner_kind,owner_id,
                              provision_generation,cluster_name,namespace,vm_uid,
                              prior_vmi_uid,prior_launcher_uid,root_pvc_uid
                    """,
                    operation_id,
                    version,
                    claim_token,
                    phase,
                    json.dumps(dict(observation)) if observation is not None else None,
                    json.dumps(dict(diagnostic)) if diagnostic is not None else None,
                    max(0.0, next_check_seconds),
                    retain_claim,
                    self.worker_id,
                )
                if row is None:
                    await self._pause_expired_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                    )
                    return None if retain_claim else False
                if not retain_claim:
                    await conn.execute(
                        "DELETE FROM vm_workspace_recovery_probe_slots "
                        "WHERE recovery_id=$1 AND claim_token=$2",
                        operation_id,
                        claim_token,
                    )
                    return True
                slot = await conn.fetchrow(
                    "SELECT global_slot,node_key FROM vm_workspace_recovery_probe_slots "
                    "WHERE recovery_id=$1 AND claim_token=$2",
                    operation_id,
                    claim_token,
                )
        if slot is None:
            return None
        return RecoveryClaim(
            operation_id=row["id"],
            version=int(row["version"]),
            claim_token=int(row["claim_token"]),
            deadline_at=row["deadline_at"],
            remaining_seconds=max(0.0, float(row["remaining_seconds"])),
            captured_identity={
                "owner_kind": row["owner_kind"],
                "owner_id": row["owner_id"],
                "provision_generation": row["provision_generation"],
                "cluster_name": row["cluster_name"],
                "namespace": row["namespace"],
                "vm_uid": row["vm_uid"],
                "prior_vmi_uid": row["prior_vmi_uid"],
                "prior_launcher_uid": row["prior_launcher_uid"],
                "root_pvc_uid": row["root_pvc_uid"],
            },
            attempt=int(row["recovery_attempts"]),
            global_slot=int(slot["global_slot"]),
            node_key=str(slot["node_key"]),
        )

    async def defer_claim(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        phase: str,
        observation: Mapping[str, Any] | None = None,
        diagnostic: Mapping[str, Any] | None = None,
        next_check_seconds: float,
    ) -> bool:
        changed = await self._finish_claim(
            operation_id=operation_id,
            version=version,
            claim_token=claim_token,
            phase=phase,
            observation=observation,
            diagnostic=diagnostic,
            next_check_seconds=next_check_seconds,
            retain_claim=False,
        )
        return changed is True

    async def stage_observation(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        phase: str,
        observation: Mapping[str, Any],
    ) -> RecoveryClaim | None:
        changed = await self._finish_claim(
            operation_id=operation_id,
            version=version,
            claim_token=claim_token,
            phase=phase,
            observation=observation,
            diagnostic=None,
            next_check_seconds=0,
            retain_claim=True,
        )
        return changed if isinstance(changed, RecoveryClaim) else None

    async def pause_automatic_disabled(self) -> int:
        """Make feature-off behavior visible while preserving every hold."""

        async with self.db.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='paused_attention',
                           latest_diagnostic=jsonb_build_object(
                               'reason','automatic_recovery_disabled',
                               'observed_at',clock_timestamp()),
                           claimed_by=NULL,claimed_until=NULL,version=version+1
                     WHERE phase IN (
                         'recovering','observing','waiting_runtime','verifying_stop',
                         'attesting','reconciling_outcome'
                     ) AND resolved_at IS NULL
                    RETURNING id
                    """
                )
                if rows:
                    await conn.execute(
                        "UPDATE vm_workspace_recovery_jobs SET participation='attention' "
                        "WHERE recovery_id=ANY($1::uuid[]) AND resolved_at IS NULL",
                        [row["id"] for row in rows],
                    )
                    await conn.execute(
                        "DELETE FROM vm_workspace_recovery_probe_slots "
                        "WHERE recovery_id=ANY($1::uuid[])",
                        [row["id"] for row in rows],
                    )
        return len(rows)

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
                       AND phase IN (
                           'recovering','observing','waiting_runtime','verifying_stop',
                           'attesting','reconciling_outcome'
                       ) AND resolved_at IS NULL
                       AND deadline_at > clock_timestamp()
                       AND claimed_by=$6 AND claimed_until > clock_timestamp()
                       AND EXISTS (
                           SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                            WHERE slot.recovery_id=vm_workspace_recoveries.id
                              AND slot.claim_token=$3
                              AND slot.leased_until > clock_timestamp()
                       )
                    RETURNING 1
                    """,
                    operation_id,
                    version,
                    claim_token,
                    json.dumps(dict(observation)),
                    next_check_seconds,
                    self.worker_id,
                )
                if changed is not None:
                    await conn.execute(
                        "DELETE FROM vm_workspace_recovery_probe_slots "
                        "WHERE recovery_id=$1 AND claim_token=$2",
                        operation_id,
                        claim_token,
                    )
                else:
                    await self._pause_expired_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
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
                       AND phase IN (
                           'recovering','observing','waiting_runtime','verifying_stop',
                           'attesting','reconciling_outcome'
                       ) AND resolved_at IS NULL
                       AND deadline_at > clock_timestamp()
                       AND claimed_by=$6 AND claimed_until > clock_timestamp()
                       AND EXISTS (
                           SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                            WHERE slot.recovery_id=vm_workspace_recoveries.id
                              AND slot.claim_token=$3
                              AND slot.leased_until > clock_timestamp()
                       )
                    RETURNING 1
                    """,
                    operation_id,
                    version,
                    claim_token,
                    code.value,
                    json.dumps(dict(diagnostic)),
                    self.worker_id,
                )
                if changed is not None:
                    await conn.execute(
                        "UPDATE vm_workspace_recovery_jobs SET participation='attention' "
                        "WHERE recovery_id=$1 AND resolved_at IS NULL",
                        operation_id,
                    )
                    await conn.execute(
                        "DELETE FROM vm_workspace_recovery_probe_slots "
                        "WHERE recovery_id=$1 AND claim_token=$2",
                        operation_id,
                        claim_token,
                    )
                else:
                    await self._pause_expired_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                    )
        return changed is not None

    async def release_recovered(
        self,
        *,
        operation_id: UUID,
        version: int,
        claim_token: int,
        resume_receipt: Mapping[str, Any],
        initial_observation: Mapping[str, Any],
        final_observation: Mapping[str, Any],
    ) -> bool:
        """Release exact participant holds with queue-before-job lock ordering."""

        try:
            async with self.db.acquire() as conn, conn.transaction():
                participants = await conn.fetch(
                    "SELECT job_id,hold_lease_token,prior_job_status,participation,"
                    "checkpoint_id,checkpoint_namespace,prior_control_reference "
                    "FROM vm_workspace_recovery_jobs "
                    "WHERE recovery_id=$1 AND resolved_at IS NULL ORDER BY job_id",
                    operation_id,
                )
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
                        "SELECT status, freeze_data, context FROM jobs "
                        "WHERE id=$1 FOR UPDATE",
                        participant["job_id"],
                    )
                operation = await conn.fetchrow(
                    "SELECT *, deadline_at <= clock_timestamp() AS deadline_expired "
                    "FROM vm_workspace_recoveries "
                    "WHERE id=$1 AND version=$2 AND claim_token=$3 "
                    "AND claimed_by=$4 AND claimed_until > clock_timestamp() "
                    "AND phase IN ('recovering','observing','waiting_runtime',"
                    "'verifying_stop','attesting','reconciling_outcome') "
                    "AND resolved_at IS NULL AND EXISTS ("
                    "SELECT 1 FROM vm_workspace_recovery_probe_slots slot "
                    "WHERE slot.recovery_id=vm_workspace_recoveries.id "
                    "AND slot.claim_token=$3 "
                    "AND slot.leased_until > clock_timestamp()) FOR UPDATE",
                    operation_id,
                    version,
                    claim_token,
                    self.worker_id,
                )
                if operation is None:
                    return False
                if operation["deadline_expired"]:
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=WorkspaceRecoveryCode.DEADLINE_EXCEEDED,
                        diagnostic={"reason": "deadline_elapsed_before_release"},
                    )
                    return False
                participants = await conn.fetch(
                    "SELECT job_id,hold_lease_token,prior_job_status,participation,"
                    "checkpoint_id,checkpoint_namespace,prior_control_reference "
                    "FROM vm_workspace_recovery_jobs "
                    "WHERE recovery_id=$1 AND resolved_at IS NULL "
                    "ORDER BY job_id FOR UPDATE",
                    operation_id,
                )
                if not participants or not all(
                    participant["participation"] == "held"
                    and _participant_continuation_safe(participant)
                    for participant in participants
                ):
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
                        diagnostic={
                            "reason": "continuation_evidence_changed_or_unknown"
                        },
                    )
                    return False
                initial_error = _observation_authority_error(
                    operation, initial_observation
                )
                final_error = _observation_authority_error(operation, final_observation)
                evidence_error = initial_error or final_error
                if evidence_error is not None:
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=_attestation_error_code(evidence_error),
                        diagnostic={"reason": evidence_error},
                    )
                    return False
                initial = dict(initial_observation)
                final = dict(final_observation)
                if _attestation_authority_key(initial) != _attestation_authority_key(
                    final
                ):
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                        diagnostic={
                            "reason": "final_re_attestation_or_identity_changed"
                        },
                    )
                    return False
                successor = final["successor"]
                assert isinstance(successor, Mapping)
                replacing = str(successor["launcher_uid"]) != str(
                    operation["prior_launcher_uid"] or ""
                )
                if replacing:
                    receipt = await conn.fetchval(
                        """
                        SELECT EXISTS(
                            SELECT 1 FROM vm_workspace_recovery_stop_receipts
                             WHERE recovery_id=$1 AND accepted_claim_token <= $2
                               AND vm_uid=$3 AND vmi_uid=$4
                               AND launcher_uid=$5 AND root_pvc_uid=$6
                               AND evidence_digest=$7
                        )
                        """,
                        operation_id,
                        claim_token,
                        operation["vm_uid"],
                        operation["prior_vmi_uid"],
                        operation["prior_launcher_uid"],
                        operation["root_pvc_uid"],
                        final["stop_receipt_digest"],
                    )
                    if not receipt:
                        await self._pause_claim_locked(
                            conn,
                            operation_id=operation_id,
                            version=version,
                            claim_token=claim_token,
                            worker_id=self.worker_id,
                            code=WorkspaceRecoveryCode.PRIOR_RUNTIME_UNFENCED,
                            diagnostic={"reason": "exact_stop_receipt_not_committed"},
                        )
                        return False
                unsettled_remote = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_remote_operation_leases "
                    "WHERE owner_kind=$1 AND owner_id=$2 AND settled_at IS NULL)",
                    operation["owner_kind"],
                    operation["owner_id"],
                )
                if unsettled_remote:
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
                        diagnostic={"reason": "remote_operation_unsettled_at_release"},
                    )
                    return False
                if operation["owner_kind"] == "job":
                    owner_job = jobs.get(operation["owner_id"])
                    owner_context = (
                        _json(owner_job["context"]) if owner_job is not None else None
                    )
                    owner_vm = (
                        owner_context.get("vm")
                        if isinstance(owner_context, Mapping)
                        else None
                    )
                    projection_current = bool(
                        isinstance(owner_vm, Mapping)
                        and str(owner_vm.get("provision_generation") or "")
                        == str(operation["provision_generation"])
                        and str(owner_vm.get("vm_uid") or "")
                        == str(operation["vm_uid"])
                        and str(owner_vm.get("rootdisk_pvc_uid") or "")
                        == str(operation["root_pvc_uid"])
                    )
                    if not projection_current:
                        await self._pause_claim_locked(
                            conn,
                            operation_id=operation_id,
                            version=version,
                            claim_token=claim_token,
                            worker_id=self.worker_id,
                            code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                            diagnostic={"reason": "owner_vm_projection_changed"},
                        )
                        return False
                retention_pin = await conn.fetchrow(
                    "SELECT pvc_uid,provision_generation "
                    "FROM vm_workspace_recovery_retention_pins "
                    "WHERE recovery_id=$1 AND pvc_uid=$2 "
                    "AND provision_generation=$3 AND released_at IS NULL FOR UPDATE",
                    operation_id,
                    operation["root_pvc_uid"],
                    operation["provision_generation"],
                )
                if retention_pin is None:
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
                        code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                        diagnostic={
                            "reason": "exact_retention_pin_missing_or_released"
                        },
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
                    await self._pause_claim_locked(
                        conn,
                        operation_id=operation_id,
                        version=version,
                        claim_token=claim_token,
                        worker_id=self.worker_id,
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
                        claimed_by=self.worker_id,
                        resume_receipt=dict(resume_receipt),
                    )
                    if not changed:
                        raise _RecoveryClaimLost
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
                if operation["owner_kind"] == "job":
                    projected = {
                        "status": "ready",
                        "provision_generation": str(operation["provision_generation"]),
                        "vm_uid": str(operation["vm_uid"]),
                        "rootdisk_pvc_uid": str(operation["root_pvc_uid"]),
                        "vmi_uid": str(successor.get("vmi_uid") or ""),
                        "active_pod_uid": str(successor.get("launcher_uid") or ""),
                        "pod_ip": successor.get("pod_ip"),
                        "ssh_host": successor.get("pod_ip"),
                        "ssh_port": 22,
                        "ssh_registration_id": successor.get("ssh_registration_id"),
                        "ssh_ready_source": "workspace_recovery",
                        "recovering": False,
                    }
                    bound = await conn.fetchval(
                        """
                        UPDATE jobs
                           SET context=jsonb_set(
                               COALESCE(context,'{}'::jsonb),'{vm}',
                               COALESCE(context->'vm','{}'::jsonb) || $2::jsonb
                           )
                         WHERE id=$1
                           AND context->'vm'->>'provision_generation'=$3
                           AND context->'vm'->>'vm_uid'=$4
                           AND context->'vm'->>'rootdisk_pvc_uid'=$5
                        RETURNING 1
                        """,
                        operation["owner_id"],
                        json.dumps(projected),
                        str(operation["provision_generation"]),
                        str(operation["vm_uid"]),
                        str(operation["root_pvc_uid"]),
                    )
                    if bound is None:
                        await self._pause_claim_locked(
                            conn,
                            operation_id=operation_id,
                            version=version,
                            claim_token=claim_token,
                            worker_id=self.worker_id,
                            code=WorkspaceRecoveryCode.IDENTITY_CONFLICT,
                            diagnostic={"reason": "owner_vm_projection_changed"},
                        )
                        return False
                pin_released = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recovery_retention_pins pin
                       SET released_at=clock_timestamp()
                     WHERE pin.recovery_id=$1 AND pin.pvc_uid=$2
                       AND pin.provision_generation=$3 AND pin.released_at IS NULL
                       AND EXISTS (
                           SELECT 1 FROM vm_workspace_recoveries recovery
                           JOIN vm_workspace_recovery_probe_slots slot
                             ON slot.recovery_id=recovery.id
                            AND slot.claim_token=recovery.claim_token
                          WHERE recovery.id=$1 AND recovery.version=$4
                            AND recovery.claim_token=$5
                            AND recovery.claimed_by=$6
                            AND recovery.claimed_until > clock_timestamp()
                            AND recovery.deadline_at > clock_timestamp()
                            AND recovery.resolved_at IS NULL
                            AND slot.leased_until > clock_timestamp()
                       )
                    RETURNING 1
                    """,
                    operation_id,
                    operation["root_pvc_uid"],
                    operation["provision_generation"],
                    version,
                    claim_token,
                    self.worker_id,
                )
                if pin_released is None:
                    raise _RecoveryClaimLost
                recovered = await conn.fetchval(
                    """
                    UPDATE vm_workspace_recoveries
                       SET phase='recovered', resolved_at=clock_timestamp(),
                           claimed_by=NULL, claimed_until=NULL, version=version+1
                     WHERE id=$1 AND version=$2 AND claim_token=$3
                       AND phase IN (
                           'recovering','observing','waiting_runtime','verifying_stop',
                           'attesting','reconciling_outcome'
                       ) AND resolved_at IS NULL
                       AND claimed_by=$4 AND claimed_until > clock_timestamp()
                       AND deadline_at > clock_timestamp()
                       AND EXISTS (
                           SELECT 1 FROM vm_workspace_recovery_probe_slots slot
                            WHERE slot.recovery_id=vm_workspace_recoveries.id
                              AND slot.claim_token=$3
                              AND slot.leased_until > clock_timestamp()
                       )
                    RETURNING 1
                    """,
                    operation_id,
                    version,
                    claim_token,
                    self.worker_id,
                )
                if recovered is None:
                    raise _RecoveryClaimLost
                slot_released = await conn.fetchval(
                    "DELETE FROM vm_workspace_recovery_probe_slots "
                    "WHERE recovery_id=$1 AND claim_token=$2 RETURNING 1",
                    operation_id,
                    claim_token,
                )
                if slot_released is None:
                    raise _RecoveryClaimLost
        except _RecoveryClaimLost:
            return False
        return True


__all__ = [
    "CleanupPermit",
    "RecoveryClaim",
    "RetentionPinCommand",
    "VMWorkspaceRecoveryStore",
    "WorkspaceRecoveryControlConflict",
    "acquire_vm_cleanup_permit",
    "cleanup_intent_digest",
    "completed_cleanup_outcome",
    "complete_vm_cleanup_permit",
]
