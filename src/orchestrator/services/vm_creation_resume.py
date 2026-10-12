"""Resume existing creation intent without replacing its execution authority."""

import os
from uuid import UUID, uuid4

from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from orchestrator.services.vm_creation_preflight import (
    VMCreationPreflightStore,
    _execution_binding,
    _preflight,
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
    _json,
)


def _object(value):
    if not isinstance(value, dict):
        raise VMCreationRetryConflict("creation_request_unproven")
    return value


async def settled_never_issued_resume_allowed(conn, job_id: UUID) -> bool:
    """Classify a completed logical Cancel; this never mutates its source.

    The route uses this only to select ordinary Resume. A caller removing the
    pending marker must first lock the queue and Job, then recheck this proof
    in the same transaction as workspace shedding and the normal Resume CAS.
    Historical terminal evidence alone is not current execution authority.
    """
    if os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() != "true":
        # Shedding a durable source while admission is disabled would expose
        # an empty VM context to the legacy creation path.
        return False
    job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
    if (
        job is None
        or job["status"] != "cancelled"
        or job["execution_lane"] != "stateless"
        or job["parent_job_id"] is not None
        or job["assigned_agent_id"] is not None
    ):
        return False
    try:
        context = _object(_json(job["context"]))
        vm = _object(context["vm"])
        preflight = _preflight(vm)
        if preflight is None:
            return False
        generation = UUID(vm["provision_generation"])
        request_id = UUID(preflight["request_id"])
        if (
            str(generation) != vm["provision_generation"]
            or str(request_id) != preflight["request_id"]
            or (
                "_vm_creation_pending" in context
                and context["_vm_creation_pending"] != str(request_id)
            )
            or (
                "creation_request_id" in vm
                and vm["creation_request_id"] != str(request_id)
            )
            or vm.get("status") != "deleted"
            or type(vm.get("provision_attempts")) is not int
            or vm.get("provision_attempts") != 0
            or vm.get("identity_authenticated") is not False
            or vm.get("retirement_cleanup_pending") is True
            or any(
                key in context
                for key in (
                    "_stateless_cancel_cleanup_pending",
                    "_stateless_delete_pending",
                    "_completion_control_claim",
                )
            )
            or any(
                vm.get(key) is not None
                for key in (
                    "identity_provision_generation",
                    "vm_uid",
                    "vmi_uid",
                    "active_pod_uid",
                    "_runtime_incarnation",
                    "rootdisk_pvc_uid",
                    "cloud_init_secret_uid",
                    "ssh_host",
                    "ssh_port",
                    "preparation_request",
                    "preparation",
                    "workspace_storage",
                    "idle_wake_operation_id",
                    "_suspend_remote_io_closed",
                )
            )
            or any(
                context.get(key) is not None
                for key in (
                    "workspace_container",
                    "ide_session",
                )
            )
        ):
            return False
        row = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 "
            "AND job_id=$2 AND owner_kind='job' AND provision_generation=$3",
            request_id,
            job_id,
            generation,
        )
        if row is None or row["state"] != "settled":
            return False
        execution = await conn.fetchrow(
            "SELECT id,revision,generation,harness_adapter,clock_timestamp() AS database_now,"
            "CASE WHEN resolved->'spec'->>'timeoutSeconds' IS NULL THEN NULL "
            "ELSE created_at+((resolved->'spec'->>'timeoutSeconds')::double precision "
            "* interval '1 second') END AS deadline "
            "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
            job_id,
        )
        if (
            execution is None
            or execution["harness_adapter"] != "srw/v1"
            or any(
                execution[key] != row[other]
                for key, other in (
                    ("id", "execution_id"),
                    ("revision", "execution_revision"),
                    ("generation", "execution_generation"),
                    ("deadline", "admission_deadline"),
                )
            )
            or (
                execution["deadline"] is not None
                and execution["deadline"] <= execution["database_now"]
            )
        ):
            return False
        captured = _object(vm["creation_request"])
        original = _object(preflight["request"])
        request = _object(_json(row["canonical_request"]))
        configuration = _object(_json(row["controller_configuration"]))
        if not (
            preflight.get("state") == "admitted"
            and preflight.get("request_id") == str(request_id)
            and preflight.get("job_id") == str(job_id)
            and preflight.get("expected_pvc_uid") is None
            and preflight.get("predecessor_cleanup_admission_id") is None
            and preflight.get("predecessor_evidence") in (None, {})
            and original.get("entity_type") == "job"
            and original.get("job_id") == str(job_id)
            and original.get("provision_generation") == str(generation)
            and original.get("preparation") is None
            and original.get("workspace_storage") is None
            and canonical_request_digest(original) == preflight["request_digest"]
            and _execution_binding(preflight)
            == {
                key: row[key]
                for key in (
                    "execution_id",
                    "execution_revision",
                    "execution_generation",
                    "admission_deadline",
                )
            }
            and type(captured.get("version")) is int
            and captured["version"] == 1
            and captured.get("provision_generation") == str(generation)
            and captured.get("initial_request") is True
            and captured.get("issuance_authority_bound") is False
            and captured.get("controller_configuration_authenticated") is True
            and captured.get("request") == request
            and captured.get("request_digest") == row["request_digest"]
            and canonical_request_digest(captured["request"]) == row["request_digest"]
            and canonical_request_digest(request) == row["request_digest"]
            and captured.get("controller_configuration") == configuration
            and captured.get("controller_configuration_digest")
            == row["controller_configuration_digest"]
            and canonical_configuration_digest(captured["controller_configuration"])
            == row["controller_configuration_digest"]
            and canonical_configuration_digest(configuration)
            == row["controller_configuration_digest"]
        ):
            return False
        return (
            await conn.fetchval(
                """
            SELECT public.vm_job_terminal_packet_evidence($2)->>'kind'='never_issued'
              AND public.job_vm_never_issued_repository_safe($1)
              AND public.job_vm_creation_never_issued_predecessors($1,$3)
              AND NOT EXISTS (SELECT 1 FROM vm_workspace_cleanup_admissions
                  WHERE owner_kind='job' AND owner_id=$1 AND completed_at IS NULL)
              AND NOT EXISTS (SELECT 1 FROM vm_idle_operations
                  WHERE owner_kind='job' AND owner_id=$1 AND provision_generation::text=$3)
              AND NOT EXISTS (SELECT 1 FROM vm_idle_access_leases
                  WHERE owner_kind='job' AND owner_id=$1 AND closed_at IS NULL)
              AND NOT EXISTS (SELECT 1 FROM vm_workspace_recoveries
                  WHERE owner_kind='job' AND owner_id=$1 AND resolved_at IS NULL)
              AND NOT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs
                  WHERE job_id=$1 AND resolved_at IS NULL)
            """,
                job_id,
                request_id,
                str(generation),
            )
            is True
        )
    except (VMCreationRetryConflict, ValueError, TypeError, KeyError, AttributeError):
        return False


async def resume_pending_creation(
    db,
    *,
    job_id,
    feedback=None,
    feedback_reason=None,
    lift_operator_pause_hold: str | None = None,
):
    """Return an acknowledgement, or None for ordinary runtime Resume.

    Caller owns public access and grant checks. The database transaction owns
    current control, worker-lease, recovery and immutable execution checks. No
    public request can supply a generation, disk, configuration or create UUID.
    Only the authorized public caller passes its observed operator hold token;
    background/reconciler admission keeps any hold intact.
    """
    feedback_merge = (
        {
            "queued_feedback": feedback,
            "queued_feedback_reason": feedback_reason,
            "queued_feedback_delivery_id": str(uuid4()),
        }
        if feedback
        else None
    )
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                job_uuid = UUID(job_id)
                raw = await conn.fetchrow(
                    "SELECT context FROM jobs WHERE id=$1", job_uuid
                )
                if raw is None:
                    raise VMCreationRetryConflict("job_changed")
                value = _json(raw["context"])
                context = _object({} if value is None else value)
                value = context.get("vm")
                vm = _object({} if value is None else value)
                pending_id = context.get("_vm_creation_pending")
                if (
                    pending_id is None
                    and "creation_preflight" not in vm
                    and "creation_request_id" not in vm
                ):
                    if (
                        vm.get("status") == "failed"
                        and vm.get("provision_generation")
                        and not (
                            vm.get("vm_uid")
                            and vm.get("identity_authenticated") is True
                            and vm.get("identity_provision_generation")
                            == vm.get("provision_generation")
                        )
                    ):
                        # Historical failed successors cannot borrow their
                        # predecessor's stop receipt or today's create defaults.
                        raise VMCreationRetryConflict("creation_request_unproven")
                    return None
                generation = UUID(vm["provision_generation"])
                row = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2",
                    job_uuid,
                    generation,
                )
                # Adoption, boot/init failure and executed runtime Resume use
                # the existing phase and exact retirement policy.
                if row and row["state"] == "succeeded":
                    return None
                if (
                    row
                    and row["state"] == "settled"
                    and await settled_never_issued_resume_allowed(conn, job_uuid)
                ):
                    return None
                if not pending_id:
                    raise VMCreationRetryConflict("creation_request_unproven")
                request_id = UUID(pending_id)
                if str(request_id) != pending_id:
                    raise VMCreationRetryConflict("creation_request_unproven")
                if os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() != "true":
                    raise VMCreationRetryConflict("vm_creation_retry_disabled")
                if row:
                    if row["request_id"] != request_id:
                        raise VMCreationRetryConflict("creation_request_unproven")
                    await VMCreationRetryStore(db).admit_on_conn(
                        conn,
                        job_id=job_id,
                        expected_generation=str(generation),
                        request_id=str(request_id),
                        proposal={
                            "origin": "resume",
                            "request_digest": row["request_digest"],
                            "controller_configuration_digest": row[
                                "controller_configuration_digest"
                            ],
                            "expected_pvc_uid": str(row["expected_pvc_uid"])
                            if row["expected_pvc_uid"]
                            else None,
                        },
                        lift_operator_pause_hold=lift_operator_pause_hold,
                        resume_context_merge=feedback_merge,
                    )
                else:
                    preflight = VMCreationPreflightStore(db)
                    job, context, vm, value = await preflight._lock(conn, job_uuid)
                    _object(context.get("vm"))
                    _object(vm.get("creation_preflight"))
                    if (
                        not value
                        or value["request_id"] != str(request_id)
                        or value["request"]["provision_generation"] != str(generation)
                        or value["state"] not in {"queued", "resolving", "attention"}
                        or await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
                            job_uuid,
                            generation,
                        )
                    ):
                        raise VMCreationRetryConflict("creation_request_unproven")
                    await preflight.retry._validate_resume_on_conn(
                        conn, job, request_id
                    )
                    await preflight.retry._current(
                        conn, job, generation, retry=_execution_binding(value)
                    )
                    if value["state"] == "attention":
                        now = await conn.fetchval(
                            "SELECT extract(epoch FROM clock_timestamp())::double precision"
                        )
                        value.update(
                            state="queued",
                            revision=value["revision"] + 1,
                            attempt=0,
                            next_probe_at=now,
                            claim_token=None,
                            claim_expires_at=None,
                            outage_started_at=None,
                            reason=None,
                        )
                        await preflight._write(conn, job_uuid, value)
                    await preflight.retry._resume_on_conn(
                        conn,
                        job,
                        request_id,
                        lift_operator_pause_hold=lift_operator_pause_hold,
                        context_merge=feedback_merge,
                    )
                return {
                    "status": "queued",
                    "message": "VM creation queued; worker dispatch waits for workspace readiness",
                    "job_id": job_id,
                    "vm_creation_retry_request_id": str(request_id),
                }
    except (ValueError, TypeError, KeyError) as exc:
        raise VMCreationRetryConflict("creation_request_unproven") from exc
