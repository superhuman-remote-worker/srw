"""Bounded, authoritative waiter maintenance; never releases physical capacity.

The policy cursor only nominates IDs. Each mutation reacquires the target's
owner/PVC and workflow authority before the policy. No exception from creation
admission is interpreted as cleanup evidence. Runtime wiring remains gated.
"""

from uuid import UUID

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    _record,
)
from orchestrator.services.vm_resource_reservation_store import _json
from orchestrator.services.vm_workspace_recovery_store import _job_workspace_owner
from shared.vm_resource_admission import ResourceAdmissionError


async def _lineage_snapshot(conn, job_id):
    from orchestrator.services.vm_creation_lineage import discover

    try:
        return await discover(conn, job_id), False
    except VMCreationRetryConflict as exc:
        if exc.reason != "creation_attachment_lineage_unproven":
            raise
        # This permits only quarantining our own waiter. It is never source,
        # owner, cancellation, reactivation, or physical-absence evidence.
        return None, True


async def _scope(conn, request_id):
    """Inspect blockers under normal locks, without granting creation authority."""
    from orchestrator.database.postgres import _completion_control_active_sql

    prior = _record(
        await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", request_id
        )
    )
    if prior is None:
        raise VMCreationRetryConflict("retry_request_missing")
    if prior["owner_kind"] == "thread":
        thread = await conn.fetchrow(
            "SELECT * FROM threads WHERE id=$1 FOR UPDATE", prior["thread_id"],
        )
        retry = _record(await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            request_id,
        ))
        if retry is None or any(retry[key] != prior[key] for key in (
            "owner_kind", "thread_id", "thread_runtime_generation",
            "thread_agent_id", "thread_attach_token", "thread_wake_operation_id",
            "provision_generation", "expected_pvc_uid", "observed_pvc_uid",
        )):
            raise VMCreationRetryConflict("scope_raced")
        effects = await conn.fetch(
            "SELECT * FROM vm_creation_effects WHERE request_id=$1 "
            "ORDER BY effect_number FOR UPDATE", request_id,
        )
        metadata = _json(thread["metadata"]) if thread is not None else None
        vm = metadata.get("vm") if isinstance(metadata, dict) else None
        blocker = None
        if (
            thread is None or thread["execution_lane"] != "pinned"
            or thread["runtime_generation"] != retry["thread_runtime_generation"]
            or thread["agent_id"] != retry["thread_agent_id"]
            or thread["runtime_attach_token"] != retry["thread_attach_token"]
            or thread["runtime_retirement_token"] is not None
            or thread["pinned_idle_terminal_intent_at"] is not None
            or not isinstance(vm, dict)
            or vm.get("provision_generation") != str(retry["provision_generation"])
            or vm.get("creation_request_id") != str(retry["request_id"])
        ):
            blocker = "thread_changed"
        elif await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_recoveries "
            "WHERE owner_kind='thread' AND owner_id=$1 AND resolved_at IS NULL) "
            "OR EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='thread' AND owner_id=$1 AND completed_at IS NULL "
            "AND id IS DISTINCT FROM $2)",
            retry["thread_id"], retry["creation_admission_id"],
        ):
            blocker = "workspace_recovery_held"
        return retry, thread, None, effects, blocker
    job_id = prior["job_id"]
    membership = await conn.fetchrow(
        "SELECT parent_job_id FROM jobs WHERE id=$1", job_id
    )
    if membership is None:
        raise VMCreationRetryConflict("job_changed")
    lineage, quarantine = await _lineage_snapshot(conn, job_id)
    owners = {job_id, *(UUID(v) for v in lineage["owners"])} if lineage else {job_id}
    if membership["parent_job_id"]:
        owners.add(membership["parent_job_id"])
    owners = sorted(owners)
    pvc = prior["expected_pvc_uid"] or prior["observed_pvc_uid"]
    for owner in owners:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
    if pvc:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )
    cleanups = await conn.fetch(
        "SELECT id FROM vm_workspace_cleanup_admissions WHERE completed_at IS NULL "
        "AND ((owner_kind='job' AND owner_id=ANY($1::uuid[])) OR ($2::uuid IS NOT NULL AND pvc_uid=$2)) ORDER BY id FOR UPDATE",
        owners,
        pvc,
    )
    recoveries = await conn.fetch(
        "SELECT r.id FROM vm_workspace_recoveries r WHERE r.resolved_at IS NULL AND "
        "((r.owner_kind='job' AND r.owner_id=ANY($1::uuid[])) OR "
        "EXISTS(SELECT 1 FROM vm_workspace_recovery_retention_pins p WHERE p.recovery_id=r.id "
        "AND p.released_at IS NULL AND p.pvc_uid=$2)) ORDER BY r.id FOR UPDATE",
        owners,
        pvc,
    )
    queues = await conn.fetch(
        "SELECT unit_id,state FROM run_queue WHERE unit_id=ANY($1::uuid[]) ORDER BY unit_id FOR UPDATE",
        owners,
    )
    jobs = await conn.fetch(
        "SELECT * FROM jobs WHERE id=ANY($1::uuid[]) ORDER BY id FOR UPDATE", owners
    )
    job = next((row for row in jobs if row["id"] == job_id), None)
    if job is None or job["parent_job_id"] != membership["parent_job_id"]:
        raise VMCreationRetryConflict("scope_raced")
    retry = _record(
        await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            request_id,
        )
    )
    if retry is None or any(
        retry[key] != prior[key]
        for key in (
            "job_id",
            "expected_pvc_uid",
            "observed_pvc_uid",
            "creation_admission_id",
        )
    ):
        raise VMCreationRetryConflict("scope_raced")
    executions = await conn.fetch(
        "SELECT *,CASE WHEN resolved->'spec'->>'timeoutSeconds' IS NULL THEN NULL "
        "ELSE created_at+((resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') END AS deadline "
        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=ANY($1::uuid[]) ORDER BY id FOR SHARE",
        owners,
    )
    execution = next((row for row in executions if row["work_id"] == job_id), None)
    effects = await conn.fetch(
        "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number FOR UPDATE",
        request_id,
    )
    if await _lineage_snapshot(conn, job_id) != (lineage, quarantine):
        raise VMCreationRetryConflict("scope_raced")
    owner, ambiguous = _job_workspace_owner(job_id, job)
    blocker = None
    if quarantine:
        blocker = "creation_attachment_lineage_unproven"
    elif (
        ambiguous
        or owner != job_id
        or (lineage and str(pvc) != lineage["binding"]["pvc_uid"])
    ):
        blocker = "runtime_identity_unproven"
    elif any(r["id"] != retry["creation_admission_id"] for r in cleanups):
        blocker = "workspace_cleanup_held"
    elif recoveries or await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=ANY($1::uuid[]) AND resolved_at IS NULL)",
        owners,
    ):
        blocker = "workspace_recovery_held"
    elif any(row["state"] == "leased" for row in queues):
        blocker = "worker_lease_active"
    elif await conn.fetchval(
        "SELECT (" + _completion_control_active_sql("context") + ") OR "
        "EXISTS(SELECT 1 FROM job_completion_sweep_exclusions WHERE job_id=$1) FROM jobs WHERE id=$1",
        job_id,
    ):
        blocker = "job_control_busy"
    return retry, job, execution, effects, blocker


def _outcome(retry, job, execution, effects, blocker, policy_digest, waiter, now):
    if retry["owner_kind"] == "thread":
        if effects or retry["state"] == "succeeded" or retry["observed_vm_uid"]:
            return "parked", "creation_effect_present"
        if blocker == "thread_changed":
            return "cancelled", blocker
        if blocker:
            return "parked", blocker
        vm = _json(job["metadata"]).get("vm")
        if (
            job["status"] not in {"created", "active", "awaiting_user", "suspended"}
            or not isinstance(vm, dict)
            or vm.get("status") in {"deleting", "deleted", "suspending", "suspended"}
        ):
            return "cancelled", "thread_changed"
        if waiter["policy_digest"] != policy_digest:
            return "cancelled", "policy_superseded"
        if retry["state"] in {"cancel_requested", "settled"}:
            return "cancelled", "creation_cancelled"
        if retry["state"] == "attention":
            return "parked", "retry_attention"
        return ("waiting", None) if retry["state"] in {"queued", "reconciling"} else (
            "parked", "runtime_identity_unproven"
        )
    if blocker == "creation_attachment_lineage_unproven":
        # A narrower, known target scope can only remove eligibility. Full
        # lineage authority must be re-established before every other action.
        return "parked", blocker
    context = _json(job["context"])
    vm = context.get("vm") if isinstance(context, dict) else None
    # Partial effects are a separate cleanup protocol, even for terminal jobs.
    if effects or retry["state"] == "succeeded" or retry["observed_vm_uid"]:
        return "parked", "creation_effect_present"
    if (
        not isinstance(vm, dict)
        or not isinstance(vm.get("status"), str)
        or vm.get("vm_uid")
        or vm.get("ssh_host")
    ):
        return "parked", "runtime_identity_unproven"
    if job["status"] in {"completed", "cancelled"} or any(
        key in context
        for key in (
            "_stateless_delete_pending",
            "_stateless_cancel_cleanup_pending",
        )
    ):
        return "cancelled", "job_cancelled"
    if vm.get("provision_generation") != str(retry["provision_generation"]):
        return "cancelled", "generation_changed"
    if (
        not execution
        or execution["harness_adapter"] != "srw/v1"
        or any(
            execution[key] != retry[other]
            for key, other in (
                ("id", "execution_id"),
                ("revision", "execution_revision"),
                ("generation", "execution_generation"),
                ("deadline", "admission_deadline"),
            )
        )
    ):
        return "cancelled", "execution_manifest_changed"
    if retry["admission_deadline"] and retry["admission_deadline"] <= now:
        return "cancelled", "admission_deadline_expired"
    if retry["state"] in {"cancel_requested", "settled"}:
        return "cancelled", "creation_cancelled"
    if waiter["policy_digest"] != policy_digest:
        return "cancelled", "policy_superseded"
    if blocker:
        return "parked", blocker
    if vm.get("retirement_cleanup_pending") is True or vm.get("status") in {
        "retiring_process_zero",
        "retired",
        "deleting",
        "deleted",
        "delete_failed",
    }:
        return "parked", "vm_retirement_pending"
    if retry["state"] == "attention":
        return "parked", "retry_attention"
    if retry["state"] not in {"queued", "reconciling"}:
        return "parked", "runtime_identity_unproven"
    return "waiting", None


class VMResourceWaiterMaintenance:
    def __init__(self, reservations):
        self.reservations = reservations
        self.db = reservations.db

    async def candidates(self, *, limit):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("maintenance_limit")
        async with self.db.acquire() as conn, conn.transaction():
            policy = await self.reservations._lock_policy(conn, allow_drain=True)
            rows = await conn.fetch(
                "SELECT request_id,enqueued_at FROM vm_resource_waiters WHERE cluster_id=$1 "
                "AND state IN ('waiting','nonfit','parked') AND ($2::timestamptz IS NULL OR "
                "(enqueued_at,request_id)>($2,$3::uuid)) ORDER BY enqueued_at,request_id LIMIT $4",
                policy["cluster_id"],
                policy["maintenance_enqueued_at"],
                policy["maintenance_request_id"],
                limit,
            )
            tail = rows[-1] if rows else None
            await conn.execute(
                "UPDATE vm_resource_admission_policy SET maintenance_enqueued_at=$2,maintenance_request_id=$3 WHERE cluster_id=$1",
                policy["cluster_id"],
                tail["enqueued_at"] if tail else None,
                tail["request_id"] if tail else None,
            )
            return [str(row["request_id"]) for row in rows]

    async def maintain(self, *, request_id):
        try:
            async with self.db.acquire() as conn, conn.transaction():
                return await self._maintain(conn, UUID(request_id))
        except (VMCreationRetryConflict, ResourceAdmissionError) as exc:
            return {"action": "unavailable", "reason": str(exc)}

    async def _maintain(self, conn, request_id):
        retry, job, execution, effects, blocker = await _scope(conn, request_id)
        policy = await self.reservations._lock_policy(conn, allow_drain=True)
        waiter = await conn.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            request_id,
        )
        if (
            waiter is None
            or (waiter["owner_kind"] if "owner_kind" in waiter else "job")
            != retry["owner_kind"]
            or (waiter["thread_id"] if "thread_id" in waiter else None)
            != retry["thread_id"]
            or any(waiter[key] != retry[key] for key in (
                "job_id", "provision_generation", "request_digest",
            ))
            or waiter["cluster_id"] != policy["cluster_id"]
        ):
            raise ResourceAdmissionError("resource_waiter_changed")
        held = await conn.fetchrow(
            "SELECT id FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released' FOR UPDATE",
            request_id,
        )
        now = await conn.fetchval("SELECT clock_timestamp()")
        if held or waiter["state"] == "admitted":
            return {"action": "unchanged", "reason": "held_or_admitted"}
        if waiter["state"] in {"cancelled", "released"}:
            return {"action": "unchanged", "reason": "terminal_waiter"}
        state, reason = _outcome(
            retry,
            job,
            execution,
            effects,
            blocker,
            policy["policy_digest"],
            waiter,
            now,
        )
        if state == "cancelled" and retry["state"] in {
            "queued",
            "reconciling",
            "attention",
        }:
            await conn.execute(
                "UPDATE vm_creation_retries SET state='cancel_requested',revision=revision+1,claim_token=NULL,claim_expires_at=NULL,"
                "reason=$2,next_probe_at=clock_timestamp(),updated_at=clock_timestamp() WHERE request_id=$1",
                request_id,
                reason,
            )
        if state == "waiting" and waiter["state"] != "parked":
            return {"action": "unchanged", "reason": "authority_current"}
        if (
            waiter["state"] != state
            or waiter["reason"] != reason
            or (state == "parked" and waiter["evaluated_snapshot_id"] is not None)
        ):
            await conn.execute(
                "UPDATE vm_resource_waiters SET state=$2,reason=$3,revision=revision+1,"
                "evaluated_snapshot_id=CASE WHEN $2='cancelled' THEN evaluated_snapshot_id ELSE NULL END WHERE request_id=$1",
                request_id,
                state,
                reason,
            )
        return {
            "action": "reactivated" if state == "waiting" else state,
            "reason": reason,
        }
