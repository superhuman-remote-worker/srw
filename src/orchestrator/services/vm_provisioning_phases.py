"""Atomically persist authenticated phase observations with their VM identity."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from uuid import UUID

from shared.vm_provisioning_phases import observe_provisioning
from shared.vm_admission_accounting import admission_updates


@dataclass(frozen=True, slots=True)
class PhaseObservationToken:
    job_id: UUID
    generation: str
    revision: int


def _object(value) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _nested_object(value) -> dict:
    """Optional nested authority must be a JSON object, never encoded JSON."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("invalid nested VM authority")
    return dict(value)


def _revision(vm: Mapping) -> int | None:
    value = vm.get("provisioning_revision", 0)
    return value if type(value) is int and 0 <= value < 2**63 - 1 else None


def _uuid(value: object) -> bool:
    try:
        return type(value) is str and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


class VMProvisioningPhaseStore:
    """Job-only phase observations; controller MAC validation belongs to caller.

    Capture revision before external I/O. apply_status performs no external I/O,
    and commits phase and identity together. It never acquires an owner/queue
    lock after a job lock, promotes Ready, or changes a provision generation.
    """

    def __init__(self, db):
        self.db = db

    async def publish_ready(self, job_id, generation, registration, vm_uid, updates):
        """Commit the final prober result and legacy budget reset together.

        This is called only after SSH, initialization and mutation attestation.
        A1 owns its separate worker-hold release and budget reset. No queue or
        execution state is changed here, and no I/O occurs under the job lock.
        """
        from orchestrator.services.vm_creation_readiness import _verified_epoch
        from orchestrator.services.dispatch_guards import vm_phase_decision

        if not _uuid(job_id) or not _uuid(generation) or not _uuid(vm_uid):
            return False
        if not isinstance(updates, Mapping) or updates.get("status") != "ready":
            return False
        verified = _verified_epoch(updates.get("ssh_verified_at"))
        if verified is None or updates.get("ssh_ready_source") != "provisioner_probe":
            return False
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await self._current(conn, UUID(job_id), generation, lock=True)
                if (
                    row is None
                    or await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
                        UUID(job_id),
                    )
                    or await self.db._completion_resume_blocked_on_conn(
                        conn, UUID(job_id)
                    )
                ):
                    return False
                vm = _object(_object(row["context"]).get("vm"))
                deadline = await conn.fetchval(
                    "SELECT extract(epoch FROM created_at + (resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
                    UUID(job_id),
                )
                now = await conn.fetchval(
                    "SELECT extract(epoch FROM clock_timestamp())::double precision"
                )
                if (
                    not 0 < verified <= now
                    or deadline is not None
                    and deadline <= now
                    or vm.get("vm_uid") != vm_uid
                    or vm.get("identity_authenticated") is not True
                    or vm.get("identity_provision_generation") != generation
                    or vm.get("ssh_registration_id") != registration
                    or updates.get("ssh_registration_id") != registration
                    or vm_phase_decision(vm, now=now, timeout_s=600).action
                    == "attention"
                ):
                    return False
                for field in (
                    "active_pod_uid",
                    "ssh_host",
                    "pod_ip",
                    "ssh_port",
                    "ssh_host_key_fingerprint",
                ):
                    if not updates.get(field) or vm.get(field) != updates[field]:
                        return False
                try:
                    initialization = vm.get("initialization")
                    if initialization is not None:
                        from shared.workspace_initialization import (
                            initialization_receipt,
                            validate_initialization_request,
                        )

                        initialization = validate_initialization_request(initialization)
                        receipt = initialization_receipt(
                            vm.get("initialization_receipt"),
                            owner_id=(vm.get("workspace_storage") or {}).get(
                                "uid", job_id
                            ),
                            revision=initialization["revision"],
                        )
                        if receipt["phase"] != "Succeeded" or receipt["step"] != len(
                            initialization["steps"]
                        ):
                            return False
                    a1_owned = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
                        UUID(job_id),
                        UUID(generation),
                    )
                    resource_retry = await conn.fetchrow(
                        "SELECT * FROM vm_creation_retries WHERE job_id=$1 "
                        "AND provision_generation=$2 FOR UPDATE",
                        UUID(job_id), UUID(generation),
                    )
                    from orchestrator.services.vm_resource_job_runtime import (
                        installed_job_resource_store,
                    )
                    from shared.vm_resource_admission import ResourceAdmissionError
                    from shared.vm_resource_inventory import InventoryError

                    try:
                        resource = await installed_job_resource_store(
                            conn, self.db,
                            resource_retry["controller_configuration"]
                            if resource_retry is not None else None,
                            fresh=False,
                        )
                        if resource is not None and (
                            resource_retry is None
                            or not await resource.bind_ready_on_conn(
                                conn, retry=resource_retry, vm=vm,
                                job_id=job_id, generation=generation,
                            )
                        ):
                            return False
                    except (ResourceAdmissionError, InventoryError):
                        return False
                    delta = dict(updates)
                    if not a1_owned:
                        delta.update(
                            admission_updates(
                                {**vm, "status": "ready"},
                                generation=generation,
                                vm_uid=vm_uid,
                            )
                        )
                        delta["provision_attempts"] = 0
                except (ValueError, TypeError, KeyError, AttributeError):
                    return False
                await conn.execute(
                    "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm' || $2::jsonb),updated_at=clock_timestamp() WHERE id=$1",
                    UUID(job_id),
                    json.dumps(delta),
                )
                return True

    async def admit_boot_cleanup(
        self, job_id, snapshot, *, identity, boot_timeout_s, rootdisk_stall_timeout_s
    ):
        """Linearize a phase timeout with its existing cleanup reservation.

        Reserve under owner/PVC locks before locking the job. A lost phase CAS
        rolls the whole reservation back; a winner fences later phase/Ready
        writes before releasing locks. Network teardown happens after commit.
        """
        from orchestrator.services.dispatch_guards import vm_phase_decision
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
            acquire_vm_cleanup_permit,
        )

        class Refused(Exception):
            pass

        if not _uuid(job_id) or _revision(snapshot) is None:
            return None
        generation = snapshot.get("provision_generation")
        if not _uuid(generation) or generation != identity.provision_generation:
            return None
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    permit = await acquire_vm_cleanup_permit(
                        VMWorkspaceRecoveryStore(self.db),
                        owner_kind="job",
                        owner_id=job_id,
                        identity=identity,
                        source="dispatcher_vm_recycle",
                        purge_disk=False,
                        _conn=conn,
                    )
                    if not permit.allowed or permit.completed_outcome is not None:
                        raise Refused
                    row = await self._current(conn, UUID(job_id), generation, lock=True)
                    if row is None:
                        raise Refused
                    vm = _object(_object(row["context"]).get("vm"))
                    if (
                        _revision(vm) != _revision(snapshot)
                        or vm.get("status") == "ready"
                        or vm.get("initialization_started_at") is not None
                        or vm.get("vm_uid") != identity.vm_uid
                        or vm.get("rootdisk_pvc_uid") != identity.rootdisk_pvc_uid
                        or await self.db._completion_resume_blocked_on_conn(
                            conn, UUID(job_id)
                        )
                    ):
                        raise Refused
                    if await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
                        UUID(job_id),
                    ):
                        raise Refused
                    now = await conn.fetchval(
                        "SELECT extract(epoch FROM clock_timestamp())::double precision"
                    )
                    deadline = await conn.fetchval(
                        "SELECT extract(epoch FROM created_at + (resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') "
                        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
                        UUID(job_id),
                    )
                    if deadline is not None and deadline <= now:
                        raise Refused
                    phase = vm_phase_decision(
                        vm,
                        now=now,
                        timeout_s=boot_timeout_s,
                        rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
                    )
                    state = vm.get("provisioning")
                    phase_identity = (
                        state.get("identity", {}) if isinstance(state, Mapping) else {}
                    )
                    if (
                        phase.action != "boot_timeout"
                        or phase_identity.get("owner_id") != job_id
                    ):
                        raise Refused
                    await conn.execute(
                        "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm' || "
                        "'{\"retirement_cleanup_pending\":true}'::jsonb),updated_at=clock_timestamp() WHERE id=$1",
                        UUID(job_id),
                    )
                    return permit
        except Refused:
            return None

    async def _current(self, conn, job_id, generation, *, lock=False):
        from orchestrator.database.postgres import _completion_control_active_sql

        return await conn.fetchrow(
            "SELECT context,extract(epoch FROM clock_timestamp())::double precision AS now "
            "FROM jobs WHERE id=$1 AND context->'vm'->>'provision_generation'=$2 "
            "AND status NOT IN ('completed','cancelled','failed') "
            "AND NOT (context ? '_stateless_delete_pending') "
            "AND NOT (context ? '_stateless_cancel_cleanup_pending') "
            "AND COALESCE(context->'vm'->>'retirement_cleanup_pending','false') <> 'true' "
            "AND COALESCE(context->'vm'->>'status','') NOT IN "
            "('retiring_process_zero','retired','deleting','deleted','delete_failed','suspending','suspended') "
            "AND NOT (" + _completion_control_active_sql("context") + ") "
            "AND NOT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs p "
            "WHERE p.job_id=jobs.id AND p.resolved_at IS NULL) "
            "AND NOT EXISTS (SELECT 1 FROM srw_execution_specs s "
            "WHERE s.work_kind='Job' AND s.work_id=jobs.id "
            "AND s.resolved->'spec'->>'timeoutSeconds' IS NOT NULL "
            "AND s.created_at + (s.resolved->'spec'->>'timeoutSeconds')::double precision "
            "* interval '1 second' <= clock_timestamp()) "
            + ("FOR UPDATE" if lock else ""),
            job_id,
            generation,
        )

    async def capture(
        self, job_id: str, generation: str
    ) -> PhaseObservationToken | None:
        if not _uuid(job_id) or not _uuid(generation):
            return None
        async with self.db.acquire() as conn:
            row = await self._current(conn, UUID(job_id), generation)
        if row is None:
            return None
        revision = _revision(_object(_object(row["context"]).get("vm")))
        if revision is None:
            return None
        return PhaseObservationToken(UUID(job_id), generation, revision)

    @staticmethod
    def _identity_conflict(
        vm: dict, status: Mapping, token: PhaseObservationToken
    ) -> bool:
        if status.get("provision_generation") != token.generation or not _uuid(
            status.get("vm_uid")
        ):
            return True
        pvc_uid = status.get("rootdisk_pvc_uid")
        if pvc_uid is not None and not _uuid(pvc_uid):
            return True
        if status.get("vm_name") != f"agent-vm-{token.job_id}" or not isinstance(
            status.get("namespace"), str
        ):
            return True
        for field in ("vm_uid", "rootdisk_pvc_uid", "namespace", "vm_name"):
            if vm.get(field) is not None and vm[field] != status.get(field):
                return True
        snapshot = _nested_object(vm.get("creation_request"))
        request = _nested_object(snapshot.get("request"))
        for storage in (
            _nested_object(vm.get("workspace_storage")),
            _nested_object(request.get("workspace_storage")),
        ):
            if storage.get("pvc_uid") is not None and storage["pvc_uid"] != pvc_uid:
                return True
        nested = status.get("provisioning")
        if nested is not None:
            if not isinstance(nested, Mapping) or (
                nested.get("owner_kind") != "job"
                or nested.get("owner_id") != str(token.job_id)
            ):
                return True
            for field in (
                "provision_generation",
                "vm_uid",
                "namespace",
                "rootdisk_pvc_uid",
            ):
                if nested.get(field) != status.get(field):
                    return True
        return False

    async def apply_status(
        self,
        token: PhaseObservationToken,
        status: Mapping,
        *,
        identity_updates: Mapping | None = None,
    ) -> str:
        """Return observed/unproven, or conflict/stale/held (withhold reply).

        Unproven legacy phase status may refresh matching identity, preserving
        existing clocks; the dispatcher must withhold destructive phase actions.
        Conflict retains old identity and publishes a bounded attention reason.
        """
        if not isinstance(token, PhaseObservationToken) or not isinstance(
            status, Mapping
        ):
            return "held"
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await self._current(
                    conn, token.job_id, token.generation, lock=True
                )
                if row is None:
                    return "held"
                # The locking SELECT can wait with a snapshot that predates
                # recovery admission. Admission also locks this job; a fresh
                # read after acquiring it sees any hold committed meanwhile.
                if await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM vm_workspace_recovery_jobs "
                    "WHERE job_id=$1 AND resolved_at IS NULL)",
                    token.job_id,
                ):
                    return "held"
                vm = _object(_object(row["context"]).get("vm"))
                if _revision(vm) != token.revision:
                    return "stale"
                if await self.db._completion_resume_blocked_on_conn(conn, token.job_id):
                    return "held"
                updates = {"provisioning_revision": token.revision + 1}
                try:
                    conflict = self._identity_conflict(vm, status, token)
                except ValueError:
                    conflict = True
                # The ordinary retained-rootdisk lane has no workspace_storage
                # field. Its separate A1 authority still pins first PVC binding.
                creation = await conn.fetchrow(
                    "SELECT expected_pvc_uid::text FROM vm_creation_retries "
                    "WHERE job_id=$1 AND provision_generation=$2",
                    token.job_id,
                    UUID(token.generation),
                )
                expected_pvc = creation["expected_pvc_uid"] if creation else None
                if expected_pvc is not None and expected_pvc != status.get(
                    "rootdisk_pvc_uid"
                ):
                    conflict = True
                deadline = await conn.fetchval(
                    "SELECT extract(epoch FROM created_at + "
                    "(resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') "
                    "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1",
                    token.job_id,
                )
                # SELECT-list clock expressions may be evaluated before a
                # FOR UPDATE wait. Sample only after all authority reads/locks.
                now = await conn.fetchval(
                    "SELECT extract(epoch FROM clock_timestamp())::double precision"
                )
                if deadline is not None and deadline <= now:
                    return "held"
                disposition = "conflict" if conflict else "unproven"
                reason = (
                    "vm_phase_identity_conflict" if conflict else "vm_phase_unproven"
                )
                if not conflict:
                    observation = status.get("provisioning")
                    if isinstance(observation, Mapping):
                        try:
                            phase = observe_provisioning(
                                vm.get("provisioning"),
                                observation,
                                now=now,
                            )
                            if creation is None:
                                updates.update(
                                    admission_updates(
                                        vm,
                                        generation=token.generation,
                                        vm_uid=status["vm_uid"],
                                    )
                                )
                            updates["provisioning"] = phase
                            disposition, reason = "observed", None
                        except (ValueError, TypeError, KeyError):
                            # Reject this reply as a whole: a changed nested VMI
                            # cannot leak into readiness after recording attention.
                            disposition, reason = "conflict", "vm_phase_unproven"
                    if disposition != "conflict":
                        for field in (
                            "vm_uid",
                            "vm_name",
                            "namespace",
                            "rootdisk_pvc_uid",
                        ):
                            if status.get(field) is not None:
                                updates[field] = status[field]
                        updates.update(
                            identity_authenticated=True,
                            identity_provision_generation=token.generation,
                        )
                        # The provisioner supplies its existing validated pin
                        # and runtime-potential fields. They must still equal
                        # this exact signed response, never a separate read.
                        for field in (
                            "ssh_host_key_fingerprint",
                            "credential_runtime_started",
                        ):
                            if identity_updates and field in identity_updates:
                                if identity_updates[field] != status.get(field):
                                    return "held"
                                updates[field] = identity_updates[field]
                updates["provisioning_attention_reason"] = reason
                await conn.execute(
                    "UPDATE jobs SET context=jsonb_set(context,'{vm}',context->'vm' || $2::jsonb), "
                    "updated_at=clock_timestamp() WHERE id=$1",
                    token.job_id,
                    json.dumps(updates),
                )
                return disposition
