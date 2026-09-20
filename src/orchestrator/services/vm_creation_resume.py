"""Resume existing creation intent without replacing its execution authority."""

import json
import os
from uuid import UUID, uuid4

from orchestrator.services.vm_creation_preflight import (
    VMCreationPreflightStore,
    _execution_binding,
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


async def resume_pending_creation(db, *, job_id, feedback=None, feedback_reason=None):
    """Return an acknowledgement, or None for ordinary runtime Resume.

    Caller owns public access and grant checks. The database transaction owns
    current control, worker-lease, recovery and immutable execution checks. No
    public request can supply a generation, disk, configuration or create UUID.
    """
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
                    await preflight.retry._resume_on_conn(conn, job, request_id)
                if feedback:
                    # Existing stateless delivery deduplicates feedback by this
                    # server-created identity. No worker claim is made here.
                    await conn.execute(
                        "UPDATE jobs SET context=context || $2::jsonb,updated_at=clock_timestamp() WHERE id=$1",
                        job_uuid,
                        json.dumps(
                            {
                                "queued_feedback": feedback,
                                "queued_feedback_reason": feedback_reason,
                                "queued_feedback_delivery_id": str(uuid4()),
                            }
                        ),
                    )
                return {
                    "status": "queued",
                    "message": "VM creation queued; worker dispatch waits for workspace readiness",
                    "job_id": job_id,
                    "vm_creation_retry_request_id": str(request_id),
                }
    except (ValueError, TypeError, KeyError) as exc:
        raise VMCreationRetryConflict("creation_request_unproven") from exc
