"""Server-owned completion evidence; no physical idle-release authority.

Capture runs after the existing queue -> Job completion fences. Missing historical
proof is unsupported metadata, not a reason to reject a valid completion report.
"""

from dataclasses import asdict
import hashlib
import json
import os
from uuid import UUID

from orchestrator.services.vm_remote_operation import (
    VMRemoteOperationUnavailable,
    _identity_from_row,
)
from orchestrator.services.vm_workspace_recovery_store import _job_workspace_owner
from shared.workspace_contract import (
    vm_mode_from_env,
    workspace_runtime_authority_digest,
)
from shared.workspace_idle_completion import classify_completion_wait


ACCEPTED_IDLE_WAIT_SOURCE_KEY = "_accepted_idle_wait_source"


def completion_runtime_evidence(job):
    """Prove the selected own Ready VM and return bounded, coordinate-free facts."""
    job = dict(job)
    for key in ("context", "config_override", "resolved_config"):
        value = job.get(key)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                return None
        if not isinstance(value, dict):
            return None
        job[key] = value
    job_id = UUID(str(job["id"]))
    owner, ambiguous = _job_workspace_owner(job_id, job)
    if ambiguous or owner != job_id:
        return None
    vm = job["context"].get("vm")
    if not isinstance(vm, dict) or vm.get("status") != "ready":
        return None
    try:
        pvc_uid = UUID(str(vm.get("rootdisk_pvc_uid")))
        if str(pvc_uid) != vm.get("rootdisk_pvc_uid"):
            return None
    except (TypeError, ValueError):
        return None
    try:
        identity = _identity_from_row(
            job, owner_kind="job", owner_id=str(job_id), operation_kind="idle_policy"
        )
        if str(UUID(identity.vm_uid)) != identity.vm_uid:
            return None
    except (VMRemoteOperationUnavailable, ValueError):
        return None
    delivered_digest = workspace_runtime_authority_digest(
        job, vm_mode=vm_mode_from_env()
    )
    if delivered_digest is None:
        return None
    # The existing bundle digest proves generation/endpoint delivery, but does
    # not include VM/launcher UIDs or SSH pin. Capture those now for subsequent
    # drift checks; do not describe this hash as original UID-attested delivery.
    identity_digest = hashlib.sha256(
        json.dumps(asdict(identity), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "runtime_identity": {
            "owner_kind": "job",
            "owner_id": str(job_id),
            "backend": "vm",
            "runtime_generation": identity.workspace_generation,
            "runtime_uid": identity.vm_uid,
        },
        "launcher_uid": identity.launcher_pod_uid,
        "rootdisk_pvc_uid": str(pvc_uid),
        "identity_digest": identity_digest,
        "runtime_authority_digest": delivered_digest,
    }


async def capture_completion_wait_on_conn(
    conn, *, job, report, lease_token, decision_tool_call_id=None
):
    """Capture once at fresh admission, under the already-held queue/Job locks."""
    if os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true":
        return None
    if job["execution_lane"] != "stateless" or lease_token is None:
        # Pinned dispatch has no durable delivered-generation receipt yet.
        return None
    semantics = classify_completion_wait(
        job=dict(job), report=report, decision_tool_call_id=decision_tool_call_id
    )
    if semantics is None:
        return None
    runtime = completion_runtime_evidence(job)
    if runtime is None:
        return None
    authorized = await conn.fetchval(
        "SELECT 1 FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=$2 "
        "AND bundle_authorized_at IS NOT NULL AND authority_digest=$3 "
        "AND refunded_at IS NULL AND recovery_id IS NULL AND NOT EXISTS ("
        "SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
        job["id"],
        lease_token,
        runtime["runtime_authority_digest"],
    )
    if authorized is None:
        return None
    return {"version": 1, "semantics": semantics, **runtime}


def completion_wait_branch(command, status):
    """Carry only the original server-captured branch through finalizer gates."""
    if status != "pending_review" or not isinstance(command, dict):
        return None
    payload = command.get("payload")
    source = (
        payload.get(ACCEPTED_IDLE_WAIT_SOURCE_KEY)
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(source, dict) or source.get("version") != 1:
        return None
    semantics = source.get("semantics")
    branch = semantics.get("branch") if isinstance(semantics, dict) else None
    return branch if branch in ("phase_approval", "final_human_review") else None


async def record_completion_wait_on_conn(
    conn, *, job_id, command_id, finalizing_by, branch
):
    """Publish after the successful S17 status CAS, inside its transaction.

    A metadata failure rolls back the status write and effect marker. No new
    queue/worker lock is taken after Job; source evidence was frozen at accept.
    """
    if (
        os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true"
        or branch is None
    ):
        return False
    if not conn.is_in_transaction():
        raise RuntimeError("completion idle publication requires the S17 transaction")
    row = await conn.fetchrow(
        "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", UUID(str(job_id))
    )
    if row is None or row["status"] != "pending_review":
        return False
    command = await conn.fetchrow(
        "SELECT payload,report_seq FROM job_completion_commands WHERE id=$1 AND job_id=$2 "
        "AND state='finalizing' AND finalizing_by=$3 AND lease_expires_at>clock_timestamp() "
        "AND accepted_lease_token IS NOT NULL",
        UUID(str(command_id)),
        UUID(str(job_id)),
        str(finalizing_by),
    )
    if command is None or command["report_seq"] != row["completion_seq_hwm"]:
        return False
    payload = command["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        return False
    source = payload.get(ACCEPTED_IDLE_WAIT_SOURCE_KEY)
    if not isinstance(source, dict) or source.get("version") != 1:
        return False
    semantics = source.get("semantics")
    if not isinstance(semantics, dict) or semantics.get("branch") != branch:
        return False
    job = dict(row)
    freeze = job.get("freeze_data")
    if isinstance(freeze, str):
        freeze = json.loads(freeze)
    if branch == "final_human_review":
        context = job.get("context")
        if isinstance(context, str):
            context = json.loads(context)
        decision = (
            context.get("completion_decision") if isinstance(context, dict) else None
        )
        decision_id = (
            decision.get("tool_call_id") if isinstance(decision, dict) else None
        )
        if not isinstance(decision_id, str) or decision_id.strip() != semantics.get(
            "decision_tool_call_id"
        ):
            return False
    current_semantics = classify_completion_wait(
        job=job,
        report={**payload, "freeze_data": freeze},
        decision_tool_call_id=semantics.get("decision_tool_call_id"),
    )
    if current_semantics != semantics:
        return False
    runtime = completion_runtime_evidence(job)
    if runtime is None or any(
        source.get(key) != value for key, value in runtime.items()
    ):
        return False
    from shared.workspace_idle_policy import RuntimeIdentity, read_episode
    from shared.workspace_idle_store import apply_idle_transition_on_conn

    document = job["workspace_idle_episode"]
    if isinstance(document, str):
        document = json.loads(document)
    prior = read_episode(document, revision=job["workspace_idle_revision"])
    await apply_idle_transition_on_conn(
        conn,
        runtime=RuntimeIdentity(**runtime["runtime_identity"]),
        event="enter",
        expected_revision=job["workspace_idle_revision"],
        expected_episode_id=prior.episode_id if prior else None,
        wait_kind=semantics["wait_kind"],
        wait_key=str(command_id),
    )
    return True
