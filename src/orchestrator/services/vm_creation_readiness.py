"""Release a creation worker hold only from current final prober Ready evidence."""

import json
import math
from datetime import datetime
from uuid import UUID, NAMESPACE_URL, uuid5

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
    _json,
    _creation_intent,
)
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest


def _object(value):
    if not isinstance(value, dict):
        raise VMCreationRetryConflict("creation_ready_unproven")
    return value


def _uuid(value):
    try:
        return isinstance(value, str) and UUID(value).hex == value.replace("-", "")
    except (ValueError, AttributeError):
        return False


def _verified_epoch(value):
    # VMReadinessService persists timezone-aware ISO timestamps. Retain finite
    # numeric compatibility without accepting booleans or naive local times.
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
            value = parsed.timestamp()
        except (ValueError, OverflowError):
            return None
    return value if type(value) in (int, float) and math.isfinite(value) else None


def _ready(vm, row, evidence, now):
    verified_at = _verified_epoch(vm.get("ssh_verified_at"))
    if (
        vm.get("status") != "ready"
        or vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation") != str(row["provision_generation"])
        or vm.get("creation_request_id") != str(row["request_id"])
        or vm.get("ssh_ready_source") != "provisioner_probe"
        or not _uuid(vm.get("ssh_registration_id"))
        or not _uuid(vm.get("active_pod_uid"))
        or type(vm.get("ssh_port")) is not int
        or not 1 <= vm["ssh_port"] <= 65535
        or not isinstance(vm.get("ssh_host"), str)
        or not vm["ssh_host"]
        or vm["ssh_host"].strip() != vm["ssh_host"]
        or vm.get("ssh_host") != vm.get("pod_ip")
        or verified_at is None
        or not 0 < verified_at <= now
        or vm.get("retirement_cleanup_pending") is True
        or vm.get("provisioning_attention_reason") is not None
    ):
        return False
    for current, proven in (
        ("vm_uid", "uid"),
        ("rootdisk_pvc_uid", "pvc_uid"),
        ("vm_name", "name"),
        ("namespace", "namespace"),
        ("cloud_init_secret_uid", "cloud_init_uid"),
        ("ssh_host_key_fingerprint", "ssh_host_key_fingerprint"),
    ):
        if not evidence.get(proven) or vm.get(current) != evidence[proven]:
            return False
    if vm.get("provisioning") is not None:
        from orchestrator.services.dispatch_guards import vm_phase_decision

        if vm_phase_decision(vm, now=now, timeout_s=600).action == "attention":
            return False
    request = _object(_json(row["canonical_request"]))
    initialization = request.get("initialization")
    if vm.get("initialization") != initialization:
        return False
    if initialization is not None:
        from shared.workspace_initialization import (
            initialization_receipt,
            validate_initialization_request,
        )

        initialization = validate_initialization_request(initialization)
        storage = request.get("workspace_storage")
        owner = _object(storage)["uid"] if storage is not None else str(
            row["thread_id"] if row["owner_kind"] == "thread" else row["job_id"]
        )
        receipt = initialization_receipt(
            vm.get("initialization_receipt"),
            owner_id=owner,
            revision=initialization["revision"],
        )
        if receipt["phase"] != "Succeeded" or receipt["step"] != len(
            initialization["steps"]
        ):
            return False
    return True


class VMCreationReadinessStore:
    def __init__(self, db):
        self.db = db
        self.retry = VMCreationRetryStore(db)

    async def release(self, *, request_id):
        """Clear only this pending marker. Ordinary dispatch still owns enqueue.

        Readiness does not bypass repository/credential preflight, reset worker
        attempts or issue a worker lease. Repeated release never mutates a queue.
        """
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    row, job = await self.retry._effect_scope(conn, request_id)
                    if row["ready_at"] is not None:
                        return True  # Historical result only, no dispatch authority.
                    if (
                        row["state"] != "succeeded"
                        or row["reason"] != "creation_adopted"
                        or row["boot_counted"] is not True
                        or not row["observed_vm_uid"]
                        or not row["observed_pvc_uid"]
                        or job["status"] not in {"created", "paused"}
                        or job["assigned_agent_id"] is not None
                    ):
                        return False
                    context, vm, _ = await self.retry._current(
                        conn, job, row["provision_generation"], retry=row
                    )
                    context, vm = _object(context), _object(vm)
                    if context.get("_vm_creation_pending") != str(row["request_id"]):
                        return False
                    # A different owner's recovery can enroll this job while
                    # _scope waits on its row lock. Read again after that wait.
                    if await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
                        row["job_id"],
                    ):
                        return False
                    queue_state = await conn.fetchval(
                        "SELECT state FROM run_queue WHERE unit_id=$1", row["job_id"]
                    )
                    if queue_state in {"leased", "parked"}:
                        return False
                    permit = await conn.fetchrow(
                        "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                        row["creation_admission_id"],
                    )
                    if (
                        not permit
                        or permit["completed_at"] is None
                        or permit["outcome"] != "adopted"
                        or permit["owner_kind"] != "job"
                        or permit["owner_id"] != row["job_id"]
                        or permit["source"] != "controller_vm_create"
                        or permit["pvc_uid"] != row["observed_pvc_uid"]
                        or permit["request_id"]
                        != uuid5(NAMESPACE_URL, "vm-create:" + str(row["request_id"]))
                        or permit["intent_digest"]
                        != cleanup_intent_digest(_creation_intent(row))
                    ):
                        return False
                    evidence = _object(
                        _json(
                            await conn.fetchval(
                                "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='vm' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                                row["request_id"],
                            )
                        )
                    )
                    if evidence.get("uid") != str(
                        row["observed_vm_uid"]
                    ) or evidence.get("pvc_uid") != str(row["observed_pvc_uid"]):
                        return False
                    now = await conn.fetchval(
                        "SELECT extract(epoch FROM clock_timestamp())::double precision"
                    )
                    if (
                        row["admission_deadline"]
                        and row["admission_deadline"].timestamp() <= now
                    ):
                        return False
                    if not _ready(vm, row, evidence, now):
                        return False
                    del context["_vm_creation_pending"]
                    vm["provision_attempts"] = 0
                    await conn.execute(
                        "UPDATE jobs SET context=$2::jsonb,updated_at=clock_timestamp() WHERE id=$1",
                        row["job_id"],
                        json.dumps(context),
                    )
                    await conn.execute(
                        "UPDATE vm_creation_retries SET ready_at=clock_timestamp(),revision=revision+1,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                    )
                    return True
        except (VMCreationRetryConflict, ValueError, KeyError, TypeError):
            return False
