"""Restriction-only worker loss barrier. Phase A has no settlement writer.

This record never proves process-zero and never authorizes cleanup or replay.
Even malformed presence blocks execution until a separately reviewed protocol
can establish the previous executor and command disposition.
"""

from __future__ import annotations

import json
import os
from typing import Any, Literal
from uuid import UUID, uuid4

from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
    vm_mode_from_env,
    workspace_runtime_authority_digest,
)

WORKER_EXECUTION_HOLD_KEY = "_worker_execution_hold"
WORKER_EXECUTION_HOLD_REASON = "worker_execution_outcome_unknown"
WORKER_EXECUTION_HOLD_MESSAGE = (
    "Execution was interrupted and the command outcome is unknown. "
    "The workspace is preserved. Resume is blocked until the previous executor "
    "and command disposition are proven; Cancel remains available."
)
HoldDecision = Literal["held", "not_applicable", "superseded", "blocked"]


def worker_execution_held_sql(context: str = "context") -> str:
    return f"(COALESCE({context}, '{{}}'::jsonb) ? '{WORKER_EXECUTION_HOLD_KEY}')"


def worker_execution_held(context: Any) -> bool:
    return WORKER_EXECUTION_HOLD_KEY in _object(context)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return dict(value) if isinstance(value, dict) else {}


def _vm_recovery_flag() -> bool | None:
    """Distinguish explicit OFF from ON; a malformed value cannot enable retry."""
    value = os.getenv("VM_WORKSPACE_RECOVERY_ENABLED", "false").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


async def hold_container_worker_attempt(
    conn: Any,
    *,
    job_id: UUID | str,
    lease_token: int,
    reason: Literal[
        "typed_report_unaccepted",
        "post_bundle_executor_loss",
        "node_execution_interrupted",
    ],
    grace_seconds: float | None = None,
) -> HoldDecision:
    """Atomically revoke one current container attempt without physical effects.

    Only ``not_applicable`` allows ordinary retry. Ambiguous issued authority
    never falls through to generic requeue. Queue -> Job -> attempt ordering
    serializes with completion acceptance, claim, Resume and Cancel.
    """
    if reason not in {
        "typed_report_unaccepted",
        "post_bundle_executor_loss",
        "node_execution_interrupted",
    }:
        raise ValueError("unknown worker hold reason")
    from shared.worker_queue import _CONTROL_CLAIM_ACTIVE_SQL

    owner = UUID(str(job_id))
    async with conn.transaction():
        queue = await conn.fetchrow(
            "SELECT *, leased_until < clock_timestamp() - "
            "make_interval(secs => $2::float8) AS expired FROM run_queue "
            "WHERE unit_id=$1 AND unit_kind='worker_batch' FOR UPDATE",
            owner,
            float(grace_seconds or 0),
        )
        if (
            queue is None
            or queue["lease_token"] != lease_token
            or queue["state"] != "leased"
        ):
            # Accepted B4 and later owners already own disposition. In
            # particular, queue done is not synthesized into an acceptance.
            return "superseded"
        if grace_seconds is not None and not queue["expired"]:
            return "blocked"
        job = await conn.fetchrow(
            f"SELECT job.*, ({_CONTROL_CLAIM_ACTIVE_SQL}) AS control_active "
            "FROM jobs AS job WHERE id=$1 FOR UPDATE",
            owner,
        )
        if job is None or job["status"] in {"completed", "failed", "cancelled"}:
            return "superseded"
        if job["control_active"]:
            return "blocked"
        context = _object(job["context"])
        if WORKER_EXECUTION_HOLD_KEY in context:
            return "held"
        attempt = await conn.fetchrow(
            "SELECT * FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2 FOR UPDATE",
            owner,
            lease_token,
        )
        if (
            attempt is not None
            and attempt["bundle_authorized_at"] is None
            and attempt["authority_digest"] is None
        ):
            return "not_applicable"
        authority = _object(context.get("_workspace_dispatch_authority"))
        workspace = _object(context.get("workspace_container"))
        dispatched_container = (
            authority.get("assigned_backend") == "sandbox"
            and workspace.get("provisioner") == "k8s"
        )
        try:
            contract = resolve_workspace_contract(dict(job))
            digest = workspace_runtime_authority_digest(
                dict(job), vm_mode=vm_mode_from_env()
            )
        except (WorkspaceContractError, ValueError, TypeError):
            if not dispatched_container:
                return "blocked"
            contract, digest = None, None
        recorded_digest = attempt["authority_digest"] if attempt is not None else None
        current_container = dispatched_container or (
            contract is not None
            and contract.assigned_backend == "sandbox"
            and workspace.get("provisioner") == "k8s"
        )
        # Issued sandbox custody outranks a later mutable Job config drift.
        # The container branch below still checks its exact attempt/authority.
        current_vm = (
            not current_container
            and contract is not None
            and contract.assigned_backend == "vm"
        )
        if current_vm:
            recovery = _vm_recovery_flag()
            if recovery is True:
                # The enabled native VM recovery store keeps its exact existing
                # authority and disposition path.
                return "not_applicable"
            if recovery is None:
                return "blocked"
            vm = _object(context.get("vm"))
            recorded_digest = (
                attempt["authority_digest"] if attempt is not None else None
            )
            if (
                attempt is None
                or attempt["bundle_authorized_at"] is None
                or not isinstance(recorded_digest, str)
                or not recorded_digest
                or attempt["recovery_id"] is not None
                or attempt["refunded_at"] is not None
                or job["parent_job_id"] is not None
                or vm_mode_from_env() != "same-cluster"
                or vm.get("status") != "ready"
                or vm.get("identity_authenticated") is not True
                or vm.get("identity_provision_generation")
                != vm.get("provision_generation")
                or "_vm_creation_pending" in context
            ):
                return "blocked"
            if (
                job["execution_lane"] != "stateless"
                or job["assigned_agent_id"] is not None
                or job["status"] not in {"processing", "paused"}
                or type(authority.get("version")) is not int
                or authority["version"] != 1
                or authority.get("dispatch_kind") != "stateless"
                or authority.get("assigned_backend") != "vm"
                or type(authority.get("queue_lease_token")) is not int
                or authority.get("queue_lease_token") != lease_token
                or authority.get("worker_pod") != queue["leased_by"]
                or not isinstance(queue["leased_by"], str)
                or not queue["leased_by"]
            ):
                return "blocked"
            binding = await conn.fetchrow(
                "SELECT b.request_id,b.provision_generation,b.vm_uid,b.pvc_uid,"
                "b.authority_digest FROM vm_job_worker_delivery_bindings b "
                "JOIN vm_creation_retries r ON r.request_id=b.request_id "
                "WHERE b.job_id=$1 AND b.lease_token=$2 "
                "AND b.authority_digest=$3 AND r.owner_kind='job' "
                "AND r.job_id=b.job_id AND r.provision_generation=b.provision_generation "
                "AND r.state='succeeded' AND r.reason='creation_adopted' "
                "AND r.ready_at IS NOT NULL AND r.creation_admission_id IS NOT NULL "
                "AND r.observed_vm_uid=b.vm_uid AND r.observed_pvc_uid=b.pvc_uid "
                "AND NOT EXISTS (SELECT 1 FROM vm_creation_retries later "
                "WHERE later.owner_kind='job' AND later.job_id=b.job_id "
                "AND (later.created_at,later.request_id)>(r.created_at,r.request_id))",
                owner,
                lease_token,
                recorded_digest,
            )
            if (
                binding is None
                or any(
                    vm.get(key) != str(binding[column])
                    for key, column in (
                        ("creation_request_id", "request_id"),
                        ("provision_generation", "provision_generation"),
                        ("vm_uid", "vm_uid"),
                        ("rootdisk_pvc_uid", "pvc_uid"),
                    )
                )
                or await conn.fetchval(
                    "SELECT 1 FROM job_completion_commands WHERE job_id=$1 "
                    "AND accepted_lease_token=$2 LIMIT 1",
                    owner,
                    lease_token,
                )
                is not None
            ):
                return "blocked"
            hold_id = str(uuid4())
            marker = {
                "version": 1,
                "hold_id": hold_id,
                "job_id": str(owner),
                "lease_token": lease_token,
                "revoked_lease_token": lease_token + 1,
                "bundle_authority_digest": recorded_digest,
                "current_digest_matches": digest == recorded_digest,
                "worker_pod": queue["leased_by"],
                "executor_pod_uid": None,
                "request_id": str(binding["request_id"]),
                "provision_generation": str(binding["provision_generation"]),
                "vm_uid": str(binding["vm_uid"]),
                "pvc_uid": str(binding["pvc_uid"]),
                "reason": reason,
                "phase": "pending",
            }
            await _commit_worker_hold(conn, owner, marker, hold_id)
            return "held"
        if not current_container:
            # Known other backends retain their own recovery contracts. A
            # sandbox with missing/unknown provisioner is not such proof.
            return (
                "not_applicable"
                if contract is not None
                and (
                    contract.assigned_backend != "sandbox"
                    or workspace.get("provisioner") == "docker"
                )
                else "blocked"
            )
        if attempt is not None and (
            attempt["recovery_id"] is not None or attempt["refunded_at"] is not None
        ):
            return "blocked"
        if (
            job["execution_lane"] != "stateless"
            or job["assigned_agent_id"] is not None
            or job["status"] not in {"processing", "paused"}
            or type(authority.get("version")) is not int
            or authority["version"] != 1
            or authority.get("dispatch_kind") != "stateless"
            or type(authority.get("queue_lease_token")) is not int
            or authority.get("queue_lease_token") != lease_token
            or authority.get("worker_pod") != queue["leased_by"]
        ):
            return "blocked"
        # Inheritance uses a live parent overlay during bundle assembly. Its
        # raw child digest may differ; this marker grants no source authority
        # and therefore conservatively holds only the current exact attempt.
        hold_id = str(uuid4())
        marker = {
            "version": 1,
            "hold_id": hold_id,
            "job_id": str(owner),
            "lease_token": lease_token,
            "revoked_lease_token": lease_token + 1,
            "bundle_authority_digest": recorded_digest,
            "current_digest_matches": digest is not None and digest == recorded_digest,
            "worker_pod": queue["leased_by"],
            "executor_pod_uid": None,
            "reason": reason,
            "phase": "pending",
        }
        await _commit_worker_hold(conn, owner, marker, hold_id)
        return "held"


async def _commit_worker_hold(
    conn: Any, owner: UUID, marker: dict[str, Any], hold_id: str
) -> None:
    """Keep the existing queue/Job restriction write identical across backends."""
    await conn.execute(
        "UPDATE run_queue SET state='parked', lease_token=lease_token+1, "
        "park_reason=$2, parked_at=clock_timestamp(), leased_by=NULL, "
        "last_leased_by=NULL, leased_until=NULL, "
        "interrupt_admission_lease_token=NULL, interrupt_admission_turn_id=NULL "
        "WHERE unit_id=$1",
        owner,
        WORKER_EXECUTION_HOLD_REASON,
    )
    await conn.execute(
        "UPDATE jobs SET status='paused', lease_expires_at=NULL, freeze_data=NULL, "
        "error_message=$4, context=COALESCE(context,'{}'::jsonb) "
        "|| CASE WHEN freeze_data IS NULL THEN '{}'::jsonb ELSE "
        "jsonb_build_object('last_freeze_data',freeze_data) END "
        "|| jsonb_build_object('_worker_execution_hold',$2::jsonb) "
        "|| CASE WHEN context ? '_operator_pause_hold' THEN '{}'::jsonb ELSE "
        "jsonb_build_object('_operator_pause_hold',jsonb_build_object("
        "'version',1,'hold_id',$3::text,'source','worker_execution_outcome_unknown',"
        "'paused_by',NULL,'paused_at',clock_timestamp(),'attention',$4::text)) END, "
        "updated_at=clock_timestamp() WHERE id=$1",
        owner,
        json.dumps(marker),
        hold_id,
        WORKER_EXECUTION_HOLD_MESSAGE,
    )
