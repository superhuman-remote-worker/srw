"""Real KubeVirt/Longhorn acceptance scenario for durable VM recovery.

The Helm chart exposes this command only behind the disposable acceptance
gate. It provisions one real VM, writes marker and checkpoint files through
SSH, admits recovery through the production PostgreSQL store, crashes the
KubeVirt launcher, and waits for the production reconciler to release the held
queue. It also creates bounded database fixtures for duplicate response,
leader overlap, deadline, missing stop evidence, and forced-deletion cases.
Every reported value is re-read from Kubernetes, PostgreSQL, or the running
application API. The outer gate independently validates the result.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import time
from typing import Any
from uuid import UUID, uuid4


PROTOCOL_VERSION = 1
CONFIRMATION = "disposable-vm-workspace-recovery-gate-v1"
REQUIRED_SCENARIOS = (
    "response_loss",
    "leader_overlap",
    "slow_boot",
    "deadline",
    "missing_stop_evidence",
    "forced_deletion",
    "replacement",
)
_OUTPUT_ROOT = Path("/tmp/srw-vm-workspace-recovery-gate")


class AcceptanceFailure(RuntimeError):
    """A live authority did not prove an acceptance invariant."""


class GateLeaderLease:
    """A real session advisory-lock boundary owned by one gate reconciler."""

    def __init__(self, db: Any, *, lock_id: int, identity: str) -> None:
        self.db = db
        self.lock_id = lock_id
        self.identity = identity
        self.connection: Any | None = None
        self.backend_pid: int | None = None
        self.lock_held = False

    async def acquire(self) -> bool:
        pool = getattr(self.db, "_pool", None)
        if pool is None:
            raise AcceptanceFailure("PostgreSQL pool is unavailable for leadership")
        connection = await pool.acquire()
        acquired = bool(
            await connection.fetchval("SELECT pg_try_advisory_lock($1)", self.lock_id)
        )
        if not acquired:
            await pool.release(connection)
            return False
        self.connection = connection
        self.backend_pid = int(await connection.fetchval("SELECT pg_backend_pid()"))
        self.lock_held = True
        return True

    async def unlock(self) -> bool:
        """Drop leadership while retaining the backend session for evidence."""

        connection = self.connection
        if connection is None or not self.lock_held:
            return False
        released = bool(
            await connection.fetchval("SELECT pg_advisory_unlock($1)", self.lock_id)
        )
        if released:
            self.lock_held = False
        return released

    async def close(self) -> None:
        connection = self.connection
        if connection is None:
            return
        pool = self.db._pool
        try:
            if self.lock_held:
                await self.unlock()
        finally:
            self.connection = None
            await pool.release(connection)

    async def release(self) -> bool:
        released = await self.unlock()
        await self.close()
        return released


def require_execution_guard(
    environ: Mapping[str, str], *, confirmation: str, protocol_version: int
) -> None:
    if environ.get("VM_WORKSPACE_RECOVERY_ACCEPTANCE_GATE_ENABLED") != "true":
        raise RuntimeError("disposable chart gate is not enabled")
    if confirmation != CONFIRMATION:
        raise RuntimeError("destructive confirmation does not match")
    if protocol_version != PROTOCOL_VERSION:
        raise RuntimeError("scenario protocol version does not match")


def _object(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _require_uuid(value: object, label: str) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError) as exc:
        raise AcceptanceFailure(f"{label} is unavailable") from exc


class RecoveryObservationBarrier:
    """Gate-only barrier around the production controller observation API."""

    def __init__(self, delegate: Any, *, finish_after_cancellation: bool = False):
        self.delegate = delegate
        self.finish_after_cancellation = finish_after_cancellation
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()
        self.observation_returned = False

    async def observe_workspace_recovery(
        self, identity: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            if not self.finish_after_cancellation:
                raise
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
            await self.release.wait()
        try:
            value = await self.delegate.observe_workspace_recovery(identity)
            if not isinstance(value, Mapping):
                raise AcceptanceFailure("controller observation is not an object")
            self.observation_returned = True
            return value
        finally:
            self.finished.set()

    async def reconcile_workspace_recovery_pin(self, command: Any) -> Any:
        return await self.delegate.reconcile_workspace_recovery_pin(command)


class StaleEvidenceStore:
    """Record the first production store fence rejecting an old leader."""

    def __init__(
        self,
        delegate: Any,
        *,
        claim_check_gate: asyncio.Event | None = None,
    ) -> None:
        self.delegate = delegate
        self.claim_check_gate = claim_check_gate
        self.boundary_checked = asyncio.Event()
        self.rejected_boundary: str | None = None
        self.stage_attempted = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    def _record_rejection(self, boundary: str, rejected: bool) -> None:
        if rejected and self.rejected_boundary is None:
            self.rejected_boundary = boundary
            self.boundary_checked.set()

    async def claim_is_current(self, claim: Any) -> bool:
        if self.claim_check_gate is not None:
            await self.claim_check_gate.wait()
        result = bool(await self.delegate.claim_is_current(claim))
        self._record_rejection("claim_is_current", not result)
        return result

    async def accept_stop_evidence(
        self, claim: Any, evidence: Mapping[str, Any]
    ) -> Any:
        result = await self.delegate.accept_stop_evidence(claim, evidence)
        self._record_rejection("accept_stop_evidence", result is None)
        return result

    async def trusted_stop_receipt(self, claim: Any) -> Any:
        result = await self.delegate.trusted_stop_receipt(claim)
        self._record_rejection("trusted_stop_receipt", result is None)
        return result

    async def recovery_preconditions(self, claim: Any) -> Any:
        result = await self.delegate.recovery_preconditions(claim)
        self._record_rejection("recovery_preconditions", result is None)
        return result

    async def stage_observation(self, **kwargs: Any) -> Any:
        self.stage_attempted = True
        result = await self.delegate.stage_observation(**kwargs)
        self._record_rejection("stage_observation", result is None)
        return result


class LiveScenario:
    def __init__(self, run_id: str) -> None:
        from orchestrator.database.postgres import PostgresDB
        from orchestrator.services.vm_provisioner import VMProvisioner
        from orchestrator.services.vm_workspace_recovery_config import (
            VMWorkspaceRecoverySettings,
        )

        self.run_id = run_id
        self.db = PostgresDB(min_connections=1, max_connections=6)
        self.provisioner = VMProvisioner()
        self.namespace = os.environ.get("VM_NAMESPACE", "agent-vms")
        self.settings = VMWorkspaceRecoverySettings.from_env()
        self.protocol_fixture = os.environ.get("VM_CREATION_RETRY_ENABLED") == "true"
        self.profiled_fixture = os.environ.get("VM_NETWORK_PROFILE_ENABLED") == "true"
        self.fixture_image = (
            os.environ.get("VM_WORKSPACE_RECOVERY_GATE_VM_IMAGE") or None
        )
        self.slow_boot_delay_seconds = int(
            os.environ.get("VM_WORKSPACE_RECOVERY_GATE_SLOW_BOOT_SECONDS", "15")
        )
        if not 1 <= self.slow_boot_delay_seconds <= 60:
            raise AcceptanceFailure(
                "slow-boot gate delay must be between 1 and 60 seconds"
            )
        self.job_id: UUID | None = None
        self.gate_user_id: UUID | None = None
        self._core: Any = None
        self._custom: Any = None

    async def connect(self) -> None:
        from kubernetes import client, config

        await self.db.connect()
        self.provisioner.connect(self.db)
        config.load_incluster_config()
        configuration = client.Configuration.get_default_copy()
        configuration.retries = 0
        self._gate_api_client = client.ApiClient(configuration=configuration)
        self._core = client.CoreV1Api(self._gate_api_client)
        self._custom = client.CustomObjectsApi(self._gate_api_client)

    async def close(self) -> None:
        await self.provisioner.disconnect()
        await self.db.close()
        if getattr(self, "_gate_api_client", None) is not None:
            self._gate_api_client.close()

    @asynccontextmanager
    async def _gate_owned_reconciler(self, label: str):
        """Run one generic reconciler only around an uncontrolled scenario."""

        from orchestrator.services.vm_workspace_recovery import (
            VMWorkspaceRecoveryService,
        )
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        shutdown = asyncio.Event()
        store = VMWorkspaceRecoveryStore(
            self.db, worker_id=f"gate-owned-{label}:{self.run_id}"
        )
        service = VMWorkspaceRecoveryService.from_settings(
            store,
            self.provisioner,
            settings=self.settings,
            claim_poll_seconds=0.05,
            scan_interval_seconds=0.05,
        )
        task = asyncio.create_task(service.run(shutdown))
        await asyncio.sleep(0)
        try:
            yield service
        finally:
            shutdown.set()
            await asyncio.wait_for(
                task,
                timeout=max(10, self.settings.external_call_timeout_seconds * 3),
            )

    async def _sync_gate_retention_pins(self, label: str, operation_id: UUID) -> None:
        """Acknowledge controller disk protection before injecting a fault."""

        from orchestrator.services.vm_workspace_recovery import (
            VMWorkspaceRecoveryService,
        )
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        service = VMWorkspaceRecoveryService.from_settings(
            VMWorkspaceRecoveryStore(
                self.db, worker_id=f"gate-pin-sync-{label}:{self.run_id}"
            ),
            self.provisioner,
            settings=self.settings,
        )
        await service._reconcile_retention_pins()
        acknowledged = await self._sql(
            "SELECT controller_pinned_at IS NOT NULL "
            "AND controller_pin_uid IS NOT NULL "
            "AND controller_pin_resource_version IS NOT NULL "
            "FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=$1 AND released_at IS NULL",
            operation_id,
        )
        if acknowledged is not True:
            raise AcceptanceFailure(
                f"controller retention pin was not acknowledged for {label}"
            )

    async def _sql(self, query: str, *values: object) -> Any:
        async with self.db.acquire() as conn:
            return await conn.fetchval(query, *values)

    async def _row(self, query: str, *values: object) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(query, *values)
        return dict(row) if row is not None else {}

    async def _ensure_gate_user(self) -> UUID:
        """Create one approved non-admin principal for the disposable gate."""

        if self.gate_user_id is not None:
            return self.gate_user_id
        user_id = uuid4()
        async with self.db.acquire() as conn, conn.transaction():
            if getattr(self, "protocol_fixture", False):
                await conn.execute(
                    "INSERT INTO users(id,display_name,is_approved,is_admin,can_use_vm) "
                    "VALUES ($1,$2,true,false,true)",
                    user_id,
                    f"VM recovery gate {self.run_id}",
                )
                await conn.execute(
                    "INSERT INTO capability_grants(scope_kind,scope_id,key,value_json) "
                    "VALUES ('user',$1,'vm_workspace','true'::jsonb)",
                    user_id,
                )
            else:
                await conn.execute(
                    "INSERT INTO users(id,display_name,is_approved,is_admin) "
                    "VALUES ($1,$2,true,false)",
                    user_id,
                    f"VM recovery gate {self.run_id}",
                )
        self.gate_user_id = user_id
        return user_id

    async def _create_job(self) -> UUID:
        gate_user_id = await self._ensure_gate_user()
        job_id = uuid4()
        options: dict[str, Any] = {}
        if getattr(self, "protocol_fixture", False):
            if getattr(self, "fixture_image", None):
                options["config_override"] = {
                    "workspace": {
                        "backend": "vm",
                        "vm": {
                            "image": self.fixture_image,
                            "cpu_cores": 2,
                            "memory": "2Gi",
                            "disk_size": "12Gi",
                        },
                    }
                }
                options["requested_workspace_backend"] = "vm"
        created = await self.db.create_job(
            description=f"[vm-recovery-gate:{self.run_id}] retained disk fixture",
            context={"vm_workspace_recovery_acceptance_gate": self.run_id},
            origin="lifecycle",
            status="processing"
            if not getattr(self, "protocol_fixture", False)
            else "paused",
            execution_lane="stateless",
            user_id=str(gate_user_id),
            job_id=job_id,
            **options,
        )
        if UUID(str(created["id"])) != job_id:
            raise AcceptanceFailure("job helper changed the preallocated fixture ID")
        self.job_id = job_id
        return job_id

    def _require_fixture_configuration(self) -> None:
        from shared.vm_network_profile import compatible_image

        if self.profiled_fixture and not self.protocol_fixture:
            raise AcceptanceFailure(
                "profiled recovery gate requires VM_CREATION_RETRY_ENABLED=true"
            )
        if self.protocol_fixture and (
            self.provisioner.mode != "same-cluster" or not self.fixture_image
        ):
            raise AcceptanceFailure(
                "creation-retry recovery gate requires same-cluster mode and "
                "vmController.defaultVmImage"
            )
        if self.profiled_fixture and not compatible_image(self.fixture_image):
            raise AcceptanceFailure(
                "profiled recovery gate image must be an exact allowlisted digest"
            )

    async def _require_creation_ack(self, job_id: UUID, created: object) -> None:
        from orchestrator.services.manifest_execution_snapshot import (
            read_execution,
            srw_snapshot_config,
        )
        from orchestrator.services.vm_creation_preflight import _preflight
        from shared.vm_network_profile import NETWORK_PROFILE

        if not isinstance(created, Mapping) or (
            created.get("creation_retry_protocol") != 1
            or created.get("status") != "creation_pending"
            or created.get("job_id") != str(job_id)
        ):
            raise AcceptanceFailure(
                "fixture did not enter durable VM creation preflight"
            )
        context = _object(
            (await self._row("SELECT context FROM jobs WHERE id=$1", job_id)).get(
                "context"
            )
        )
        vm = _object(context.get("vm"))
        preflight = _preflight(vm)
        request = preflight["request"] if preflight else {}
        snapshot = await read_execution(self.db, "Job", str(job_id))
        try:
            if snapshot is None:
                raise ValueError("execution snapshot missing")
            _, policy = srw_snapshot_config(snapshot)
            frozen_image = (policy.get("workspace") or {}).get("vm", {}).get("image")
        except (TypeError, KeyError, ValueError) as exc:
            raise AcceptanceFailure("fixture has no frozen VM workspace image") from exc
        if (
            preflight is None
            or preflight["request_id"] != created.get("request_id")
            or request.get("provision_generation")
            != created.get("provision_generation")
            or request.get("job_id") != str(job_id)
            or request.get("vm_image") != self.fixture_image
            or frozen_image != self.fixture_image
            or str(snapshot.get("id")) != preflight["execution_id"]
            or snapshot.get("revision") != preflight["execution_revision"]
            or snapshot.get("generation") != preflight["execution_generation"]
            or snapshot.get("work_kind") != "Job"
            or snapshot.get("work_id") != job_id
            or snapshot.get("owner_id") != self.gate_user_id
            or (
                self.profiled_fixture
                and request.get("network_profile") != NETWORK_PROFILE
            )
            or context.get("_vm_creation_pending") != preflight["request_id"]
        ):
            raise AcceptanceFailure("fixture frozen creation image/profile changed")
        self.fixture_request_id = preflight["request_id"]
        self.fixture_generation = request["provision_generation"]

    async def _issue_fixture_lease(
        self, job_id: UUID, identity: Mapping[str, Any] | None = None
    ) -> int:
        """Issue the synthetic worker lease only after VM and SSH readiness."""

        lease_token = 27
        worker = f"vm-recovery-gate:{self.run_id}"
        async with self.db.acquire() as conn, conn.transaction():
            if getattr(self, "protocol_fixture", False):
                from shared.worker_queue import _CAS_JOB_SQL

                # Preflight installed this hold. Reusing it preserves the
                # monotonic queue token and closes the dispatch gap.
                queue = await conn.fetchrow(
                    "SELECT * FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
                )
                current = await conn.fetchrow(
                    "SELECT id,status,execution_lane,assigned_agent_id,user_id,"
                    "context,config_override,freeze_data,lease_expires_at "
                    "FROM jobs WHERE id=$1 FOR UPDATE",
                    job_id,
                )
                retry = await conn.fetchrow(
                    "SELECT request_id,job_id,provision_generation,state,reason,ready_at,"
                    "canonical_request,request_digest,observed_vm_uid,"
                    "observed_pvc_uid,execution_id,"
                    "execution_revision,execution_generation "
                    "FROM vm_creation_retries "
                    "WHERE request_id=$1",
                    UUID(self.fixture_request_id),
                )
                if (
                    identity is None
                    or queue is None
                    or current is None
                    or queue["unit_kind"] != "worker_batch"
                    or queue["state"] != "done"
                    or queue["leased_by"] is not None
                    or queue["leased_until"] is not None
                    or queue["last_leased_by"] is not None
                    or queue["attempts_since_completion"] != 0
                    or queue["lease_token"] != 0
                    or queue["input_seq"] is not None
                    or queue["consumed_seq"] is not None
                    or queue["control_input_seq"] != 0
                    or queue["control_consumed_seq"] != 0
                    or queue["interrupt_admission_lease_token"] is not None
                    or queue["interrupt_admission_turn_id"] is not None
                    or queue["input_delivery_capable_lease_token"] is not None
                    or queue["park_reason"] is not None
                    or queue["parked_at"] is not None
                    or current["freeze_data"] is not None
                    or current["lease_expires_at"] is not None
                    or not self._fixture_authority_matches(
                        job_id, dict(current), dict(retry) if retry else {}, identity
                    )
                    or await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1) "
                        "OR EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs "
                        "WHERE job_id=$1 AND resolved_at IS NULL) "
                        "OR EXISTS(SELECT 1 FROM job_completion_sweep_exclusions "
                        "WHERE job_id=$1)",
                        job_id,
                    )
                ):
                    raise AcceptanceFailure(
                        "synthetic worker lease could not be issued after readiness"
                    )
                leased_until = await conn.fetchval(
                    "UPDATE run_queue SET state='leased',lease_token=$2,leased_by=$3,"
                    "last_leased_by=$3,leased_until=clock_timestamp()+interval '5 minutes',"
                    "run_after=clock_timestamp(),input_seq=1,consumed_seq=0,"
                    "attempts_since_completion=1 "
                    "WHERE unit_id=$1 AND unit_kind='worker_batch' AND state='done' "
                    "AND lease_token=0 AND leased_by IS NULL AND leased_until IS NULL "
                    "AND last_leased_by IS NULL AND attempts_since_completion=0 "
                    "AND input_seq IS NULL AND consumed_seq IS NULL "
                    "AND control_input_seq=0 AND control_consumed_seq=0 "
                    "AND interrupt_admission_lease_token IS NULL "
                    "AND interrupt_admission_turn_id IS NULL "
                    "AND input_delivery_capable_lease_token IS NULL "
                    "AND park_reason IS NULL AND parked_at IS NULL "
                    "RETURNING leased_until",
                    job_id,
                    lease_token,
                    worker,
                )
                if (
                    leased_until is None
                    or await conn.fetchval(
                        _CAS_JOB_SQL,
                        job_id,
                        "paused",
                        worker,
                        lease_token,
                        leased_until,
                    )
                    != job_id
                ):
                    raise AcceptanceFailure(
                        "synthetic worker lease could not be issued after readiness"
                    )
                await conn.execute(
                    "INSERT INTO worker_batch_attempts "
                    "(job_id,lease_token,claimed_attempt) VALUES ($1,$2,1)",
                    job_id,
                    lease_token,
                )
                return lease_token
            current = await conn.fetchrow(
                "SELECT status,execution_lane,user_id,"
                "context->>'vm_workspace_recovery_acceptance_gate' AS gate_run "
                "FROM jobs WHERE id=$1 FOR UPDATE",
                job_id,
            )
            if (
                current is None
                or current["status"] != "processing"
                or current["execution_lane"] != "stateless"
                or current["user_id"] != self.gate_user_id
                or current["gate_run"] != self.run_id
            ):
                raise AcceptanceFailure(
                    "synthetic worker lease could not be issued after readiness"
                )
            issued = await conn.fetchval(
                "INSERT INTO run_queue (unit_id,unit_kind,state,lease_token,"
                "leased_by,leased_until,input_seq,consumed_seq,"
                "attempts_since_completion) SELECT "
                "$1,'worker_batch','leased',$2,$3,"
                "clock_timestamp()+interval '5 minutes',1,0,1 "
                "FROM jobs WHERE id=$1 AND status='processing' AND user_id=$4 "
                "AND context->>'vm_workspace_recovery_acceptance_gate'=$5 "
                "AND NOT EXISTS (SELECT 1 FROM run_queue WHERE unit_id=$1) "
                "RETURNING lease_token",
                job_id,
                lease_token,
                worker,
                self.gate_user_id,
                self.run_id,
            )
            if issued != lease_token:
                raise AcceptanceFailure(
                    "synthetic worker lease could not be issued after readiness"
                )
            await conn.execute(
                "INSERT INTO worker_batch_attempts "
                "(job_id,lease_token,claimed_attempt) VALUES ($1,$2,1)",
                job_id,
                lease_token,
            )
        return lease_token

    async def _wait(self, label: str, probe: Any, timeout: float = 900) -> Any:
        stop = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < stop:
            try:
                result = await probe()
                if result:
                    return result
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(1)
        detail = f": {last_error}" if last_error is not None else ""
        raise AcceptanceFailure(f"timed out waiting for {label}{detail}")

    async def _ready_identity(self, job_id: UUID) -> dict[str, Any] | None:
        status = await self.provisioner.query_status(str(job_id), timeout=10)
        row = await self._row("SELECT context FROM jobs WHERE id=$1", job_id)
        context = _object(_object(row.get("context")).get("vm"))
        status = dict(status or {})
        live_provisioning = _object(status.get("provisioning"))
        stored_provisioning = _object(context.get("provisioning"))
        stored_identity = _object(stored_provisioning.get("identity"))
        live_vmi_uid = live_provisioning.get("vmi_uid")
        if not live_vmi_uid or stored_identity.get("vmi_uid") != live_vmi_uid:
            return None
        identity = {
            "owner_kind": "job",
            "owner_id": str(job_id),
            "provision_generation": context.get("provision_generation"),
            "namespace": context.get("namespace") or self.namespace,
            "vm_uid": context.get("vm_uid") or status.get("vm_uid"),
            "prior_vmi_uid": live_vmi_uid,
            "prior_launcher_uid": context.get("active_pod_uid")
            or status.get("active_pod_uid"),
            "root_pvc_uid": context.get("rootdisk_pvc_uid")
            or status.get("rootdisk_pvc_uid"),
            "pod_ip": context.get("pod_ip") or status.get("pod_ip"),
            "ssh_registration_id": context.get("ssh_registration_id")
            or status.get("ssh_registration_id"),
            "ssh_host_key_fingerprint": context.get("ssh_host_key_fingerprint")
            or status.get("ssh_host_key_fingerprint"),
        }
        required = (
            "provision_generation",
            "vm_uid",
            "prior_vmi_uid",
            "prior_launcher_uid",
            "root_pvc_uid",
            "pod_ip",
            "ssh_registration_id",
            "ssh_host_key_fingerprint",
        )
        if status.get("ready") is not True or any(
            not identity[key] for key in required
        ):
            return None
        for key in (
            "provision_generation",
            "vm_uid",
            "prior_vmi_uid",
            "prior_launcher_uid",
            "root_pvc_uid",
        ):
            _require_uuid(identity[key], key)
        return identity

    async def _fixture_ready_identity(self, job_id: UUID) -> dict[str, Any] | None:
        identity = await self._ready_identity(job_id)
        if identity is None or not getattr(self, "protocol_fixture", False):
            return identity
        live = _object(await self.provisioner.query_status(str(job_id), timeout=10))
        if (
            live.get("ready") is not True
            or live.get("provision_generation") != identity["provision_generation"]
            or live.get("vm_uid") != identity["vm_uid"]
            or live.get("rootdisk_pvc_uid") != identity["root_pvc_uid"]
            or live.get("active_pod_uid") != identity["prior_launcher_uid"]
            or _object(live.get("provisioning")).get("vmi_uid")
            != identity["prior_vmi_uid"]
        ):
            return None
        job = await self._row(
            "SELECT id,status,execution_lane,assigned_agent_id,user_id,context,"
            "config_override,freeze_data,lease_expires_at FROM jobs WHERE id=$1",
            job_id,
        )
        retry = await self._row(
            "SELECT request_id,job_id,provision_generation,state,reason,ready_at,"
            "canonical_request,request_digest,observed_vm_uid,"
            "observed_pvc_uid,execution_id,"
            "execution_revision,execution_generation FROM vm_creation_retries "
            "WHERE request_id=$1",
            UUID(self.fixture_request_id),
        )
        return (
            identity
            if self._fixture_authority_matches(job_id, job, retry, identity)
            else None
        )

    def _fixture_authority_matches(
        self,
        job_id: UUID,
        job: Mapping[str, Any],
        retry: Mapping[str, Any],
        identity: Mapping[str, Any],
    ) -> bool:
        """Match one frozen, authenticated Ready result to this gate Job."""
        from orchestrator.services.vm_creation_preflight import _preflight
        from shared.vm_creation_retry import canonical_request_digest
        from shared.vm_network_profile import NETWORK_PROFILE, reusable_profile_evidence

        context = _object(job.get("context"))
        vm = _object(context.get("vm"))
        contract = _object(context.get("_workspace_contract"))
        workspace = _object(_object(job.get("config_override")).get("workspace"))
        vm_config = _object(workspace.get("vm"))
        preflight = _preflight(vm)
        if (
            job.get("id") != job_id
            or job.get("status") != "paused"
            or job.get("execution_lane") != "stateless"
            or job.get("assigned_agent_id") is not None
            or job.get("user_id") != self.gate_user_id
            or context.get("vm_workspace_recovery_acceptance_gate") != self.run_id
            or contract.get("version") != 1
            or contract.get("assigned_backend") != "vm"
            or contract.get("requested_backend") != "vm"
            or workspace.get("backend") != "vm"
            or vm_config.get("image") != self.fixture_image
            or vm_config.get("cpu_cores") != 2
            or vm_config.get("memory") != "2Gi"
            or vm_config.get("disk_size") != "12Gi"
            or any(
                key in context
                for key in (
                    "_workspace_dispatch_authority",
                    "_completion_control_claim",
                    "_stateless_control_claim",
                    "_operator_pause_hold",
                )
            )
            or context.get("_vm_creation_pending") is not None
            or vm.get("status") != "ready"
            or preflight is None
            or preflight["request_id"] != self.fixture_request_id
            or preflight["request"].get("provision_generation")
            != self.fixture_generation
            or preflight["request"].get("vm_image") != self.fixture_image
            or vm.get("creation_request_id") != self.fixture_request_id
            or vm.get("identity_authenticated") is not True
            or vm.get("identity_provision_generation") != self.fixture_generation
            or identity.get("owner_kind") != "job"
            or identity.get("owner_id") != str(job_id)
            or identity.get("provision_generation") != self.fixture_generation
            or vm.get("vm_uid") != identity.get("vm_uid")
            or vm.get("rootdisk_pvc_uid") != identity.get("root_pvc_uid")
            or vm.get("active_pod_uid") != identity.get("prior_launcher_uid")
            or _object(_object(vm.get("provisioning")).get("identity")).get("vmi_uid")
            != identity.get("prior_vmi_uid")
        ):
            return False
        canonical = _object(retry.get("canonical_request"))
        if (
            str(retry.get("request_id")) != self.fixture_request_id
            or retry.get("job_id") != job_id
            or str(retry.get("provision_generation")) != self.fixture_generation
            or retry.get("state") != "succeeded"
            or retry.get("reason") != "creation_adopted"
            or retry.get("ready_at") is None
            or str(retry.get("observed_vm_uid")) != identity["vm_uid"]
            or str(retry.get("observed_pvc_uid")) != identity["root_pvc_uid"]
            or canonical != preflight["request"]
            or not canonical
            or canonical_request_digest(canonical) != retry.get("request_digest")
            or str(retry.get("execution_id")) != preflight["execution_id"]
            or retry.get("execution_revision") != preflight["execution_revision"]
            or retry.get("execution_generation") != preflight["execution_generation"]
        ):
            return False
        if getattr(self, "profiled_fixture", False) and (
            canonical.get("network_profile") != NETWORK_PROFILE
            or not reusable_profile_evidence(
                vm.get("network_profile_evidence"),
                NETWORK_PROFILE,
                provision_generation=self.fixture_generation,
                vm_uid=identity["vm_uid"],
                pvc_uid=identity["root_pvc_uid"],
                vmi_uid=identity["prior_vmi_uid"],
                launcher_uid=identity["prior_launcher_uid"],
            )
        ):
            return False
        return True

    async def _application_api_evidence(self, job_id: UUID) -> dict[str, Any]:
        import httpx

        base = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8085").rstrip("/")
        key = os.environ.get("MCP_INTERNAL_KEY", "")
        user_id = await self._ensure_gate_user()
        owner = await self._row("SELECT user_id FROM jobs WHERE id=$1", job_id)
        if str(owner.get("user_id") or "") != str(user_id):
            raise AcceptanceFailure("recovery gate user does not own the fixture job")
        if not key:
            raise AcceptanceFailure("MCP_INTERNAL_KEY is not configured")
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{base}/api/jobs/{job_id}",
                headers={
                    "X-Internal-Key": key,
                    "X-MCP-User-Id": str(user_id),
                },
            )
        return {
            "status_code": response.status_code,
            "job_visible": response.status_code == 200 and str(job_id) in response.text,
        }

    async def _executor_occupied(self, job_id: UUID) -> bool:
        """Read the durable queue authority for stateless executor ownership."""

        row = await self._row(
            "SELECT state,leased_by,leased_until FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        return row.get("state") == "leased" or row.get("leased_by") is not None

    async def _ssh_file(
        self, identity: Mapping[str, Any], path: str, value: str | None
    ) -> str:
        from orchestrator.services import resolve_ssh_key_path
        from orchestrator.services.ssh_helpers import pinned_agent_ssh_command
        from orchestrator.services.subprocess_effect import (
            communicate_bounded,
            create_owned_subprocess_exec,
        )

        host_key = identity.get("ssh_host_key_fingerprint")
        if not isinstance(host_key, str) or not host_key.strip():
            raise AcceptanceFailure("captured SSH host-key fingerprint is unavailable")
        if not path.startswith("/home/agent-host/.srw-recovery-gate/"):
            raise AcceptanceFailure("gate file path is outside the fixture directory")
        quoted_path = shlex.quote(path)
        if value is not None:
            if not re.fullmatch(r"[A-Za-z0-9:._-]{1,256}", value):
                raise AcceptanceFailure("gate marker contains unsafe shell characters")
            command = (
                "mkdir -p /home/agent-host/.srw-recovery-gate && "
                f"printf %s {shlex.quote(value)} > {quoted_path}"
            )
        else:
            command = f"cat -- {quoted_path}"
        async with pinned_agent_ssh_command(
            str(identity["pod_ip"]),
            22,
            command,
            expected_host_key_fingerprint=host_key,
            key_path=resolve_ssh_key_path(),
            connect_timeout_s=10,
            batch_mode=True,
        ) as argv:
            process = await create_owned_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await communicate_bounded(
                process,
                timeout=20,
                stdout_limit=4096,
                stderr_limit=4096,
            )
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace")[:300]
            raise AcceptanceFailure(f"pinned SSH gate file operation failed: {detail}")
        return stdout.decode("utf-8", errors="strict").strip()

    async def _admit(
        self,
        *,
        identity: Mapping[str, Any],
        lease_token: int,
        request_id: UUID,
        code: Any,
        checkpoint: str,
    ) -> Any:
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        store = VMWorkspaceRecoveryStore(self.db, worker_id=f"gate-{self.run_id}")
        return await store.admit_hold(
            job_id=UUID(str(identity["owner_id"])),
            accepted_lease_token=lease_token,
            owner_kind="job",
            owner_id=UUID(str(identity["owner_id"])),
            workspace_contract_digest="sha256:vm-recovery-gate-v1",
            provision_generation=UUID(str(identity["provision_generation"])),
            cluster_name=os.environ.get("SRW_CLUSTER_NAME", "gate"),
            namespace=str(identity["namespace"]),
            vm_uid=UUID(str(identity["vm_uid"])),
            prior_vmi_uid=UUID(str(identity["prior_vmi_uid"])),
            prior_launcher_uid=UUID(str(identity["prior_launcher_uid"])),
            root_pvc_uid=UUID(str(identity["root_pvc_uid"])),
            code=code,
            request_id=request_id,
            actor_kind="system",
            actor_id="vm-workspace-recovery-gate",
            intent_digest=f"sha256:{request_id.hex}",
            original_cause={"gate": self.run_id},
            checkpoint_id=checkpoint,
            checkpoint_namespace="vm-workspace-recovery-gate",
        )

    async def _reset_lease(self, job_id: UUID) -> int:
        async with self.db.acquire() as conn, conn.transaction():
            token = await conn.fetchval(
                "UPDATE run_queue SET state='leased',lease_token=lease_token+1,"
                "leased_by=$2,leased_until=clock_timestamp()+interval '5 minutes',"
                "run_after=clock_timestamp(),park_reason=NULL,parked_at=NULL "
                "WHERE unit_id=$1 RETURNING lease_token",
                job_id,
                f"vm-recovery-gate:{self.run_id}",
            )
            await conn.execute(
                "INSERT INTO worker_batch_attempts (job_id,lease_token,claimed_attempt) "
                "VALUES ($1,$2,1) ON CONFLICT DO NOTHING",
                job_id,
                token,
            )
        return int(token)

    async def _resolve_fixture_recovery(self, operation_id: UUID, job_id: UUID) -> None:
        async with self.db.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE vm_workspace_recovery_jobs SET participation='cancelled',"
                "resolved_at=clock_timestamp() WHERE recovery_id=$1 AND resolved_at IS NULL",
                operation_id,
            )
            await conn.execute(
                "UPDATE vm_workspace_recoveries SET phase='cancelled',"
                "resolved_at=clock_timestamp(),claimed_by=NULL,claimed_until=NULL "
                "WHERE id=$1 AND resolved_at IS NULL",
                operation_id,
            )
            await conn.execute(
                "UPDATE vm_workspace_recovery_retention_pins SET released_at=clock_timestamp() "
                "WHERE recovery_id=$1 AND released_at IS NULL",
                operation_id,
            )
            await conn.execute(
                "UPDATE jobs SET freeze_data=NULL,status='processing' WHERE id=$1",
                job_id,
            )

    async def _operation(self, operation_id: UUID) -> dict[str, Any]:
        return await self._row(
            "SELECT phase,reason_code,deadline_at,resolved_at FROM vm_workspace_recoveries "
            "WHERE id=$1",
            operation_id,
        )

    async def _wait_phase(
        self, operation_id: UUID, phases: set[str]
    ) -> dict[str, Any] | None:
        row = await self._operation(operation_id)
        return row if row.get("phase") in phases else None

    @asynccontextmanager
    async def _positive_stop_retention(self, identity, operation_id):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateStopControl,
            GateStopError,
            GateStopStore,
            KubernetesStopObjects,
            joined_call,
            validate_document,
        )

        if (
            identity.get("owner_kind") != "job"
            or identity.get("namespace") != self.namespace
        ):
            raise GateStopError("stop_fixture_changed")
        pods = await joined_call(
            self._core.list_namespaced_pod,
            namespace=self.namespace,
            label_selector=f"vm.kubevirt.io/name=agent-vm-{identity['owner_id']}",
            _request_timeout=(3, 5),
        )
        if (
            len(pods.items) != 1
            or str(pods.items[0].metadata.uid) != identity["prior_launcher_uid"]
        ):
            raise GateStopError("stop_launcher_ambiguous")
        doc = {
            "version": 1,
            "run_id": self.run_id,
            "user_id": str(self.gate_user_id),
            "job_id": identity["owner_id"],
            "operation_id": str(operation_id),
            "generation": identity["provision_generation"],
            "namespace": self.namespace,
            "vm_name": "agent-vm-" + identity["owner_id"],
            "vm_uid": identity["vm_uid"],
            "vmi_name": "agent-vm-" + identity["owner_id"],
            "vmi_uid": identity["prior_vmi_uid"],
            "pod_name": pods.items[0].metadata.name,
            "pod_uid": identity["prior_launcher_uid"],
            "pvc_uid": identity["root_pvc_uid"],
            "original_strategy": "RerunOnFailure",
            "stage": "planned",
        }
        validate_document(doc)
        store = GateStopStore(self.db, self.run_id)
        control = GateStopControl(KubernetesStopObjects(self._core), store)
        original_error = None
        try:
            await control.prepare(doc)
            self._gate_stop = (control, doc)
            yield control, doc
        except BaseException as exc:
            original_error = exc
            raise
        finally:
            self._gate_stop = None

            # A failed write-ahead creates no object effects. A lost save reply
            # is settled from SQL rather than the in-memory stage.
            async def abort_unfinished():
                stored = next(
                    (
                        item
                        for item in await store.load()
                        if item["operation_id"] == doc["operation_id"]
                    ),
                    None,
                )
                if stored and stored["stage"] not in {"restored", "aborted"}:
                    await control.abort(stored)
                    return True
                return False

            try:
                # Shield the SQL reconstruction too, not only Kubernetes cleanup.
                cleanup = asyncio.create_task(abort_unfinished())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                aborted = cleanup.result()
                if aborted and original_error is None:
                    raise GateStopError("stop_control_not_restored")
            except BaseException as cleanup_error:
                if original_error is None:
                    raise
                original_error.add_note(
                    "Gate stop-retention cleanup failed; rerun --cleanup-only for this run. "
                    + type(cleanup_error).__name__
                )

    async def _cleanup_stop_retention(self):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateStopControl,
            GateStopStore,
            KubernetesStopObjects,
        )

        store = GateStopStore(self.db, self.run_id)
        control = GateStopControl(KubernetesStopObjects(self._core), store)
        for doc in await store.load():
            if doc["stage"] not in {"restored", "aborted"}:
                await control.abort(doc)

    async def _crash_launcher(self, identity: Mapping[str, Any]) -> None:
        from kubernetes.stream import stream
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            FINALIZER,
            GateStopError,
            joined_call,
        )

        pair = getattr(self, "_gate_stop", None)
        if pair is None:
            raise GateStopError("stop_retention_missing")
        control, doc = pair
        if identity["prior_launcher_uid"] != doc["pod_uid"]:
            raise GateStopError("stop_launcher_changed")
        async with control.store.authorized(doc) as check:
            await control.kube.assert_quiet(doc)
            vm = control._exact(doc, "vm", await control.kube.read(doc, "vm"))
            if (
                vm is None
                or vm.get("spec", {}).get("runStrategy") != "Manual"
                or vm.get("status", {}).get("stateChangeRequests")
                or type(vm.get("metadata", {}).get("generation")) is not int
                or vm.get("status", {}).get("desiredGeneration")
                != vm["metadata"]["generation"]
            ):
                raise GateStopError("stop_strategy_changed")
            for kind in ("vmi", "pod"):
                obj = control._exact(doc, kind, await control.kube.read(doc, kind))
                if obj is None or FINALIZER not in obj["metadata"].get(
                    "finalizers", []
                ):
                    raise GateStopError("stop_retention_missing")
            if check is not None:
                await check()
            await joined_call(
                stream,
                self._core.connect_get_namespaced_pod_exec,
                doc["pod_name"],
                self.namespace,
                container="compute",
                command=["/bin/sh", "-c", "kill -TERM 1"],
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _request_timeout=8,
            )

    async def _force_delete_vmi(self, job_id: UUID) -> None:
        from kubernetes.client import V1DeleteOptions

        await asyncio.to_thread(
            self._custom.delete_namespaced_custom_object,
            group="kubevirt.io",
            version="v1",
            namespace=self.namespace,
            plural="virtualmachineinstances",
            name=f"agent-vm-{job_id}",
            body=V1DeleteOptions(grace_period_seconds=0),
            grace_period_seconds=0,
        )

    async def _set_vm_run_strategy(self, job_id: UUID, strategy: str) -> None:
        if strategy not in {"Halted", "RerunOnFailure"}:
            raise AcceptanceFailure("unsupported gate VM run strategy")
        await asyncio.to_thread(
            self._custom.patch_namespaced_custom_object,
            group="kubevirt.io",
            version="v1",
            namespace=self.namespace,
            plural="virtualmachines",
            name=f"agent-vm-{job_id}",
            body={"spec": {"runStrategy": strategy}},
        )

    async def _longhorn_volume_evidence(
        self, job_id: UUID, root_pvc_uid: str
    ) -> dict[str, Any]:
        pvcs = await asyncio.to_thread(
            self._core.list_namespaced_persistent_volume_claim,
            namespace=self.namespace,
            field_selector=f"metadata.name=agent-vm-{job_id}-rootdisk",
        )
        pvc = next(
            (item for item in pvcs.items if str(item.metadata.uid) == root_pvc_uid),
            None,
        )
        if pvc is None or not pvc.spec.volume_name:
            raise AcceptanceFailure("exact root PVC is not bound to a PV")
        pv = await asyncio.to_thread(
            self._core.read_persistent_volume, pvc.spec.volume_name
        )
        driver = str(getattr(pv.spec.csi, "driver", "") if pv.spec.csi else "")
        volume = await asyncio.to_thread(
            self._custom.get_namespaced_custom_object,
            group="longhorn.io",
            version="v1beta2",
            namespace="longhorn-system",
            plural="volumes",
            name=pvc.spec.volume_name,
        )
        status = _object(volume.get("status"))
        healthy = status.get("robustness") == "healthy" and status.get("state") in {
            "attached",
            "detached",
        }
        return {
            "root_pv_name": pvc.spec.volume_name,
            "root_pv_csi_driver": driver,
            "longhorn_volume_healthy": healthy,
        }

    async def _deployed_revisions(
        self, job_id: UUID, root_pvc_uid: str
    ) -> dict[str, str]:
        namespace_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        release_namespace = namespace_path.read_text(encoding="utf-8").strip()
        own_name = os.environ.get("HOSTNAME", "")
        own = await asyncio.to_thread(
            self._core.read_namespaced_pod, own_name, release_namespace
        )
        application = next(
            (
                status.image_id
                for status in own.status.container_statuses or []
                if status.name == "orchestrator" and status.image_id
            ),
            None,
        )
        controllers = await asyncio.to_thread(
            self._core.list_namespaced_pod,
            namespace=self.namespace,
            label_selector="app.kubernetes.io/component=vm-controller",
        )
        controller_ids = {
            status.image_id
            for pod in controllers.items
            for status in pod.status.container_statuses or []
            if status.name == "vm-controller" and status.image_id
        }
        pvcs = await asyncio.to_thread(
            self._core.list_namespaced_persistent_volume_claim,
            namespace=self.namespace,
            field_selector=f"metadata.name=agent-vm-{job_id}-rootdisk",
        )
        pvc = next(
            (
                item
                for item in pvcs.items
                if str(item.metadata.uid) == str(root_pvc_uid)
            ),
            None,
        )
        owner = (pvc.metadata.owner_references or [None])[0] if pvc else None
        if application is None or len(controller_ids) != 1 or owner is None:
            raise AcceptanceFailure("deployed image revision evidence is incomplete")
        dv = await asyncio.to_thread(
            self._custom.get_namespaced_custom_object,
            group="cdi.kubevirt.io",
            version="v1beta1",
            namespace=self.namespace,
            plural="datavolumes",
            name=owner.name,
        )
        source = _object(_object(dv.get("spec")).get("source"))
        source_revision = json.dumps(source, sort_keys=True, separators=(",", ":"))
        if not source_revision or not dv.get("metadata", {}).get("uid"):
            raise AcceptanceFailure("guest DataVolume revision is unavailable")
        return {
            "application": str(application),
            "controller": next(iter(controller_ids)),
            "guest_image": f"{dv['metadata']['uid']}:{source_revision}",
        }

    async def _probe_limits(self) -> tuple[int, int, bool]:
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        ids = [uuid4() for _ in range(self.settings.max_global_probes + 2)]
        owner_ids = [uuid4() for _ in ids]
        async with self.db.acquire() as conn:
            before = datetime.now(timezone.utc)
            for index, (operation_id, owner_id) in enumerate(zip(ids, owner_ids)):
                node = "shared-node" if index < 2 else f"node-{index}"
                await conn.execute(
                    "INSERT INTO vm_workspace_recoveries "
                    "(id,owner_kind,owner_id,workspace_contract_digest,cluster_name,phase,"
                    "reason_code,latest_observation) VALUES "
                    "($1,'job',$2,'sha256:gate','gate','recovering',"
                    "'workspace_runtime_not_ready',jsonb_build_object('node_uid',$3))",
                    operation_id,
                    owner_id,
                    node,
                )
            original_deadline = await conn.fetchval(
                "SELECT deadline_at FROM vm_workspace_recoveries WHERE id=$1", ids[0]
            )
        stores = [
            VMWorkspaceRecoveryStore(self.db, worker_id=f"gate-probe-{index}")
            for index in range(len(ids))
        ]
        claims = await asyncio.gather(
            *(
                store.claim_due(
                    operation_id,
                    ttl_seconds=self.settings.claim_ttl_seconds,
                    permit_ttl_seconds=self.settings.permit_ttl_seconds,
                    max_global_probes=self.settings.max_global_probes,
                    max_probes_per_node=self.settings.max_probes_per_node,
                )
                for store, operation_id in zip(stores, ids)
            )
        )
        async with self.db.acquire() as conn:
            slots = await conn.fetch(
                "SELECT global_slot,node_key FROM vm_workspace_recovery_probe_slots "
                "WHERE recovery_id=ANY($1::uuid[])",
                ids,
            )
            final_deadline = await conn.fetchval(
                "SELECT deadline_at FROM vm_workspace_recoveries WHERE id=$1", ids[0]
            )
            await conn.execute(
                "DELETE FROM vm_workspace_recovery_probe_slots WHERE recovery_id=ANY($1::uuid[])",
                ids,
            )
            await conn.execute(
                "DELETE FROM vm_workspace_recoveries WHERE id=ANY($1::uuid[])", ids
            )
        counts: dict[str, int] = {}
        for slot in slots:
            counts[str(slot["node_key"])] = counts.get(str(slot["node_key"]), 0) + 1
        return (
            sum(claim is not None for claim in claims),
            max(counts.values(), default=0),
            original_deadline == final_deadline and original_deadline >= before,
        )

    async def _leader_handoff_scenario(
        self,
        *,
        job_id: UUID,
        identity: Mapping[str, Any],
        lease_token: int,
        checkpoint: str,
    ) -> dict[str, Any]:
        """Transfer one live claim while its first controller probe is stale."""

        from orchestrator.services.vm_workspace_recovery import (
            VMWorkspaceRecoveryService,
        )
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )
        from shared.workspace_recovery import WorkspaceRecoveryCode

        disposition = await self._admit(
            identity=identity,
            lease_token=lease_token,
            request_id=uuid4(),
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        deadline_before = await self._sql(
            "SELECT deadline_at FROM vm_workspace_recoveries WHERE id=$1",
            disposition.operation_id,
        )
        stale_claim_check = asyncio.Event()
        leader_a_store = StaleEvidenceStore(
            VMWorkspaceRecoveryStore(self.db, worker_id=f"gate-leader-a:{self.run_id}"),
            claim_check_gate=stale_claim_check,
        )
        leader_b_store = VMWorkspaceRecoveryStore(
            self.db, worker_id=f"gate-leader-b:{self.run_id}"
        )
        leader_a_identity = leader_a_store.worker_id
        leader_b_identity = leader_b_store.worker_id
        lock_digest = hashlib.sha256(
            f"vm-workspace-recovery-gate:{self.run_id}".encode()
        ).digest()
        leader_lock_id = int.from_bytes(lock_digest[:8], "big") & ((1 << 63) - 1)
        leader_a_lease = GateLeaderLease(
            self.db, lock_id=leader_lock_id, identity=leader_a_identity
        )
        leader_b_lease = GateLeaderLease(
            self.db, lock_id=leader_lock_id, identity=leader_b_identity
        )

        stale_barrier = RecoveryObservationBarrier(
            self.provisioner, finish_after_cancellation=True
        )
        winning_barrier = RecoveryObservationBarrier(self.provisioner)
        leader_a = VMWorkspaceRecoveryService.from_settings(
            leader_a_store,
            stale_barrier,
            settings=self.settings,
            claim_poll_seconds=0.05,
            scan_interval_seconds=0.05,
        )
        leader_b = VMWorkspaceRecoveryService.from_settings(
            leader_b_store,
            winning_barrier,
            settings=self.settings,
            claim_poll_seconds=0.05,
            scan_interval_seconds=0.05,
        )
        leader_a_shutdown = asyncio.Event()
        leader_b_shutdown = asyncio.Event()
        stale_task: asyncio.Task[Any] | None = None
        winning_task: asyncio.Task[Any] | None = None
        leader_a_released = False
        leader_b_acquired = False
        contender_was_blocked = False
        try:
            if not await leader_a_lease.acquire():
                raise AcceptanceFailure("first gate leader could not acquire its lock")
            stale_task = asyncio.create_task(leader_a.run(leader_a_shutdown))
            await asyncio.wait_for(stale_barrier.started.wait(), timeout=30)
            stale_claim = await self._row(
                "SELECT claim_token,claimed_by FROM vm_workspace_recoveries "
                "WHERE id=$1",
                disposition.operation_id,
            )
            if stale_claim.get("claimed_by") != leader_a_identity:
                raise AcceptanceFailure("first reconciler did not own the live claim")

            contender_was_blocked = not await leader_b_lease.acquire()
            if not contender_was_blocked:
                raise AcceptanceFailure("two gate leaders held the advisory lock")

            # Release leadership while leaving A's in-flight loop alive for the
            # ordinary run_when_leader polling window. B can win during that
            # window, and A's late result must hit a durable claim fence.
            leader_a_released = await leader_a_lease.unlock()
            leader_b_acquired = await leader_b_lease.acquire()
            if not leader_a_released or not leader_b_acquired:
                raise AcceptanceFailure("gate leadership did not transfer")
            leader_a_backend_pid = leader_a_lease.backend_pid
            leader_b_backend_pid = leader_b_lease.backend_pid
            if (
                leader_a_backend_pid is None
                or leader_b_backend_pid is None
                or leader_a_backend_pid == leader_b_backend_pid
            ):
                raise AcceptanceFailure(
                    "gate leaders did not use distinct PostgreSQL sessions"
                )
            await leader_a_lease.close()
            leadership_transferred_at = await self._sql("SELECT clock_timestamp()")

            # Expire only the exact application claim held by the old leader.
            # The new leader must mint a higher fencing token through the
            # production store before it can observe or release anything.
            async with self.db.acquire() as conn, conn.transaction():
                expired = await conn.fetchval(
                    "UPDATE vm_workspace_recoveries SET "
                    "claimed_until=clock_timestamp()-interval '1 second' "
                    "WHERE id=$1 AND claim_token=$2 AND claimed_by=$3 RETURNING id",
                    disposition.operation_id,
                    stale_claim["claim_token"],
                    stale_claim["claimed_by"],
                )
                if expired is None:
                    raise AcceptanceFailure(
                        "controlled leader handoff lost its source claim"
                    )
                await conn.execute(
                    "UPDATE vm_workspace_recovery_probe_slots SET "
                    "leased_until=clock_timestamp()-interval '1 second' "
                    "WHERE recovery_id=$1 AND claim_token=$2",
                    disposition.operation_id,
                    stale_claim["claim_token"],
                )

            winning_task = asyncio.create_task(leader_b.run(leader_b_shutdown))
            await asyncio.wait_for(winning_barrier.started.wait(), timeout=30)
            handoff = await self._row(
                "SELECT clock_timestamp() AS observed_at,claim_token,claimed_by,"
                "deadline_at FROM vm_workspace_recoveries WHERE id=$1",
                disposition.operation_id,
            )
            if handoff.get("claimed_by") != leader_b_identity:
                raise AcceptanceFailure("second reconciler did not acquire the handoff")
            # Let A perform its production claim-current check only after B has
            # minted and durably owns the successor claim. This deterministically
            # proves the exact fence that discards A's still-blocked probe.
            stale_claim_check.set()
            await asyncio.wait_for(
                leader_a_store.boundary_checked.wait(),
                timeout=self.settings.external_call_timeout_seconds,
            )
            permit_sample = await self._row(
                "SELECT count(*)::integer AS global_count,"
                "count(DISTINCT node_key)::integer AS node_count "
                "FROM vm_workspace_recovery_probe_slots "
                "WHERE leased_until>clock_timestamp()"
            )
            active_operations = int(
                await self._sql(
                    "SELECT count(*) FROM vm_workspace_recoveries "
                    "WHERE owner_kind='job' AND owner_id=$1 AND resolved_at IS NULL",
                    job_id,
                )
            )
            winning_barrier.release.set()
            recovered = await self._wait(
                "leader handoff recovery",
                lambda: self._wait_phase(disposition.operation_id, {"recovered"}),
                timeout=self.settings.external_call_timeout_seconds * 3,
            )
            dispatches = int(
                await self._sql(
                    "SELECT count(*) FROM vm_workspace_recovery_jobs "
                    "WHERE recovery_id=$1 AND resume_receipt IS NOT NULL",
                    disposition.operation_id,
                )
            )
            resume_receipt = _object(
                await self._sql(
                    "SELECT resume_receipt FROM vm_workspace_recovery_jobs "
                    "WHERE recovery_id=$1 AND job_id=$2",
                    disposition.operation_id,
                    job_id,
                )
            )

            stale_barrier.release.set()
            await asyncio.wait_for(
                stale_barrier.finished.wait(),
                timeout=self.settings.external_call_timeout_seconds,
            )
            stale_finished_at = await self._sql("SELECT clock_timestamp()")
            leader_a_shutdown.set()
            await asyncio.wait_for(
                stale_task,
                timeout=self.settings.external_call_timeout_seconds * 3,
            )
            leader_b_shutdown.set()
            await asyncio.wait_for(
                winning_task,
                timeout=self.settings.external_call_timeout_seconds * 3,
            )
            await leader_b_lease.release()
            leader_b_acquired = False
        finally:
            stale_barrier.release.set()
            winning_barrier.release.set()
            leader_a_shutdown.set()
            leader_b_shutdown.set()
            for task in (stale_task, winning_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (stale_task, winning_task) if task is not None),
                return_exceptions=True,
            )
            if leader_a_lease.connection is not None:
                await leader_a_lease.close()
            if leader_b_lease.connection is not None:
                await leader_b_lease.close()

        deadline_after = await self._sql(
            "SELECT deadline_at FROM vm_workspace_recoveries WHERE id=$1",
            disposition.operation_id,
        )
        max_global, max_node, probe_deadline_preserved = await self._probe_limits()
        winning_token = int(resume_receipt.get("claim_token") or 0)

        return {
            "fault_injection": "controlled_leader_handoff",
            "active_operations": active_operations,
            "leader_instances": 2,
            "leader_a_identity": leader_a_identity,
            "leader_b_identity": leader_b_identity,
            "leader_a_backend_pid": leader_a_backend_pid,
            "leader_b_backend_pid": leader_b_backend_pid,
            "leadership_transfer_succeeded": bool(
                contender_was_blocked
                and leader_a_released
                and leadership_transferred_at <= handoff["observed_at"]
            ),
            "max_global_probes": max(
                max_global, int(permit_sample.get("global_count") or 0)
            ),
            "max_node_probes": max(max_node, int(permit_sample.get("node_count") or 0)),
            "configured_global_probe_limit": self.settings.max_global_probes,
            "configured_node_probe_limit": self.settings.max_probes_per_node,
            "deadline_preserved": deadline_before
            == deadline_after
            == handoff.get("deadline_at")
            and probe_deadline_preserved,
            "stale_probe_finished_after_handoff": bool(
                stale_barrier.observation_returned
                and stale_finished_at > handoff["observed_at"]
            ),
            "stale_store_boundary": leader_a_store.rejected_boundary,
            "stale_store_boundary_rejected": bool(leader_a_store.rejected_boundary),
            "stale_stage_attempted": leader_a_store.stage_attempted,
            "stale_result_rejected": bool(
                recovered.get("phase") == "recovered"
                and dispatches == 1
                and bool(leader_a_store.rejected_boundary)
                and int(stale_claim.get("claim_token") or 0) < winning_token
            ),
            "successor_dispatches": dispatches,
        }

    async def _deadline_barrier_scenario(
        self,
        *,
        job_id: UUID,
        identity: Mapping[str, Any],
        lease_token: int,
        checkpoint: str,
        marker_path: str,
        checkpoint_path: str,
        marker: str,
    ) -> dict[str, Any]:
        """Hold a real controller observation across the immutable DB deadline."""

        from orchestrator.services.vm_workspace_recovery import (
            VMWorkspaceRecoveryService,
        )
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )
        from shared.workspace_recovery import WorkspaceRecoveryCode

        class DeadlineEvidenceStore(VMWorkspaceRecoveryStore):
            """Record which production deadline boundary rejects the result."""

            precondition_check_rejected = False
            stage_observation_attempted = False
            release_attempted = False
            release_recovered_succeeded: bool | None = None

            async def recovery_preconditions(self, claim: Any) -> Any:
                result = await super().recovery_preconditions(claim)
                self.precondition_check_rejected = result is None
                return result

            async def stage_observation(self, **kwargs: Any) -> Any:
                self.stage_observation_attempted = True
                return await super().stage_observation(**kwargs)

            async def release_recovered(self, **kwargs: Any) -> bool:
                self.release_attempted = True
                result = await super().release_recovered(**kwargs)
                self.release_recovered_succeeded = result
                return result

        disposition = await self._admit(
            identity=identity,
            lease_token=lease_token,
            request_id=uuid4(),
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        store = DeadlineEvidenceStore(self.db, worker_id=f"gate-deadline:{self.run_id}")
        # The acceptance-only wait hook lets the real controller call return
        # after the deadline. PostgreSQL still applies the production CAS and
        # must reject the late observation without releasing the participant.
        barrier = RecoveryObservationBarrier(
            self.provisioner, finish_after_cancellation=True
        )
        service = VMWorkspaceRecoveryService.from_settings(
            store,
            barrier,
            settings=self.settings,
            claim_poll_seconds=0.05,
            allow_observation_past_deadline_for_acceptance=True,
        )
        await service._reconcile_retention_pins()

        async def inside_probe_window() -> dict[str, Any] | None:
            row = await self._row(
                "SELECT deadline_at,clock_timestamp() AS observed_at,"
                "extract(epoch FROM (deadline_at-clock_timestamp()))::float8 "
                "AS remaining_seconds FROM vm_workspace_recoveries WHERE id=$1",
                disposition.operation_id,
            )
            remaining = float(row.get("remaining_seconds") or 0)
            return row if 0 < remaining <= 8 else None

        await self._wait(
            "immutable deadline probe window",
            inside_probe_window,
            timeout=self.settings.deadline_seconds + 30,
        )
        probe_task = asyncio.create_task(
            service.reconcile_once(disposition.operation_id)
        )
        await asyncio.wait_for(barrier.started.wait(), timeout=10)
        started = await self._row(
            "SELECT clock_timestamp() AS observed_at,deadline_at "
            "FROM vm_workspace_recoveries WHERE id=$1",
            disposition.operation_id,
        )

        async def deadline_crossed() -> dict[str, Any] | None:
            row = await self._row(
                "SELECT clock_timestamp() AS observed_at,deadline_at "
                "FROM vm_workspace_recoveries WHERE id=$1",
                disposition.operation_id,
            )
            return row if row["observed_at"] >= row["deadline_at"] else None

        crossed = await self._wait(
            "database deadline crossing", deadline_crossed, timeout=15
        )
        barrier.release.set()
        await asyncio.wait_for(
            barrier.finished.wait(),
            timeout=self.settings.external_call_timeout_seconds,
        )
        finished_at = await self._sql("SELECT clock_timestamp()")
        await asyncio.wait_for(
            probe_task,
            timeout=self.settings.external_call_timeout_seconds,
        )
        # The late observation is rejected by recovery_preconditions(). A
        # normal subsequent claim pass materializes the durable deadline pause.
        await store.claim_due(
            disposition.operation_id,
            ttl_seconds=self.settings.claim_ttl_seconds,
            permit_ttl_seconds=self.settings.permit_ttl_seconds,
            max_global_probes=self.settings.max_global_probes,
            max_probes_per_node=self.settings.max_probes_per_node,
        )
        outcome = await self._row(
            "SELECT recovery.phase,recovery.reason_code,participant.resume_receipt,"
            "participant.checkpoint_id,queue.state AS queue_state,"
            "queue.park_reason FROM vm_workspace_recoveries recovery "
            "JOIN vm_workspace_recovery_jobs participant "
            "ON participant.recovery_id=recovery.id "
            "JOIN run_queue queue ON queue.unit_id=participant.job_id "
            "WHERE recovery.id=$1 AND participant.job_id=$2",
            disposition.operation_id,
            job_id,
        )
        dispatches = int(
            await self._sql(
                "SELECT count(*) FROM vm_workspace_recovery_jobs "
                "WHERE recovery_id=$1 AND resume_receipt IS NOT NULL",
                disposition.operation_id,
            )
        )
        marker_after = await self._ssh_file(identity, marker_path, None)
        checkpoint_after = await self._ssh_file(identity, checkpoint_path, None)

        return {
            "fault_injection": "live_claim_deadline_barrier",
            "state": outcome.get("phase"),
            "reason_code": outcome.get("reason_code"),
            "probe_started_before_deadline": bool(
                started["observed_at"] < started["deadline_at"]
            ),
            "probe_finished_after_deadline": bool(
                barrier.observation_returned and finished_at >= crossed["deadline_at"]
            ),
            "precondition_check_rejected": store.precondition_check_rejected,
            "stage_observation_attempted": store.stage_observation_attempted,
            "release_attempted": store.release_attempted,
            "final_release_succeeded": bool(store.release_recovered_succeeded),
            "successor_dispatches": dispatches,
            "queue_still_parked": outcome.get("queue_state") == "parked"
            and outcome.get("park_reason") == "workspace_recovery",
            "disk_retained": marker_after == marker,
            "checkpoint_retained": checkpoint_after == checkpoint
            and outcome.get("checkpoint_id") == checkpoint,
            "late_probe_released": outcome.get("phase") == "recovered",
        }

    async def execute(self) -> dict[str, Any]:
        from shared.workspace_recovery import WorkspaceRecoveryCode

        if getattr(self, "profiled_fixture", False) or getattr(
            self, "protocol_fixture", False
        ):
            self._require_fixture_configuration()
        job_id = await self._create_job()
        created = await self.provisioner.create_vm(
            str(job_id),
            cpu_cores=2,
            memory="2Gi",
            disk_size="12Gi",
            **(
                {"vm_image": self.fixture_image}
                if getattr(self, "protocol_fixture", False)
                else {}
            ),
        )
        if not created:
            raise AcceptanceFailure("controller refused the real VM fixture")
        if getattr(self, "protocol_fixture", False):
            await self._require_creation_ack(job_id, created)
        identity = await self._wait(
            "initial VM readiness",
            lambda: self._fixture_ready_identity(job_id),
            timeout=1200,
        )
        application_api_evidence = await self._application_api_evidence(job_id)
        if application_api_evidence["job_visible"] is not True:
            raise AcceptanceFailure(
                "fixture is not visible through the application API"
            )
        postgresql_evidence = await self._row(
            "SELECT current_database() AS database,"
            "current_setting('server_version_num')::integer AS server_version_num"
        )

        marker_path = "/home/agent-host/.srw-recovery-gate/marker"
        checkpoint_path = "/home/agent-host/.srw-recovery-gate/checkpoint"
        marker = f"marker:{self.run_id}:{secrets.token_hex(8)}"
        checkpoint = f"checkpoint:{self.run_id}:{secrets.token_hex(8)}"
        await self._ssh_file(identity, marker_path, marker)
        await self._ssh_file(identity, checkpoint_path, checkpoint)

        if getattr(self, "protocol_fixture", False):
            lease_token = await self._issue_fixture_lease(job_id, identity)
        else:
            lease_token = await self._issue_fixture_lease(job_id)
        request_id = uuid4()
        disposition = await self._admit(
            identity=identity,
            lease_token=lease_token,
            request_id=request_id,
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        replay = await self._admit(
            identity=identity,
            lease_token=lease_token,
            request_id=request_id,
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        response_loss = {
            "fault_injection": "committed_response_replay",
            "recovery_rows": int(
                await self._sql(
                    "SELECT count(*) FROM vm_workspace_recoveries WHERE id=$1",
                    disposition.operation_id,
                )
            ),
            "request_receipts": int(
                await self._sql(
                    "SELECT count(*) FROM vm_workspace_recovery_requests "
                    "WHERE scope_kind='job' AND scope_id=$1 AND request_id=$2",
                    job_id,
                    request_id,
                )
            ),
            "terminal_reports": int(
                await self._sql(
                    "SELECT count(*) FROM jobs WHERE id=$1 "
                    "AND status IN ('completed','failed','cancelled')",
                    job_id,
                )
            ),
            "queue_token_before": lease_token,
            "queue_token_after": replay.hold_lease_token,
        }

        recovery_started = time.monotonic()
        executor_occupancy_samples = [await self._executor_occupied(job_id)]
        await self._sync_gate_retention_pins("replacement", disposition.operation_id)
        async with self._positive_stop_retention(
            identity, disposition.operation_id
        ) as (stop_control, stop_document):
            await self._crash_launcher(identity)
            async with self._gate_owned_reconciler("replacement"):
                stop_receipt = await self._wait(
                    "exact stop receipt",
                    lambda: self._row(
                        "SELECT id,evidence_digest,accepted_claim_token,vm_uid,vmi_uid,"
                        "launcher_uid,root_pvc_uid,container_id,controller_identity "
                        "FROM vm_workspace_recovery_stop_receipts "
                        "WHERE recovery_id=$1 ORDER BY accepted_at DESC LIMIT 1",
                        disposition.operation_id,
                    ),
                    timeout=180,
                )
                # A gate-only patch keeps the real KubeVirt VM halted long enough to
                # exercise recovery waiting without occupying the fenced queue lease.
                await stop_control.release(stop_document)
                await asyncio.sleep(self.slow_boot_delay_seconds)
                executor_occupancy_samples.append(await self._executor_occupied(job_id))
                await stop_control.restore(stop_document)
                recovered = await self._wait(
                    "replacement recovery",
                    lambda: self._wait_phase(disposition.operation_id, {"recovered"}),
                    timeout=1200,
                )
        replacement_identity = await self._wait(
            "replacement VM readiness",
            lambda: self._ready_identity(job_id),
            timeout=600,
        )
        executor_occupancy_samples.append(await self._executor_occupied(job_id))
        marker_after = await self._ssh_file(replacement_identity, marker_path, None)
        checkpoint_after = await self._ssh_file(
            replacement_identity, checkpoint_path, None
        )
        pin = await self._row(
            "SELECT controller_pin_uid,controller_pin_resource_version,"
            "controller_pinned_at,controller_released_at,released_at "
            "FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            disposition.operation_id,
        )
        pin["controller_state"] = (
            "released" if pin.get("controller_released_at") else "active"
        )
        dispatches = int(
            await self._sql(
                "SELECT count(*) FROM vm_workspace_recovery_jobs WHERE recovery_id=$1 "
                "AND resume_receipt IS NOT NULL",
                disposition.operation_id,
            )
        )
        release_evidence = await self._row(
            "SELECT r.owner_id,r.provision_generation,r.vm_uid,r.root_pvc_uid,"
            "p.resume_receipt,j.context FROM vm_workspace_recoveries r "
            "JOIN vm_workspace_recovery_jobs p ON p.recovery_id=r.id "
            "JOIN jobs j ON j.id=p.job_id "
            "WHERE r.id=$1 AND p.job_id=$2",
            disposition.operation_id,
            job_id,
        )
        resume_receipt = _object(release_evidence.get("resume_receipt"))
        resume_successor = _object(resume_receipt.get("successor"))
        guest_network = _object(resume_successor.get("guest_network"))
        final_context = _object(_object(release_evidence.get("context")).get("vm"))
        pinned_identity = all(
            str(left or "") == str(right or "")
            for left, right in (
                (release_evidence.get("owner_id"), job_id),
                (
                    release_evidence.get("provision_generation"),
                    replacement_identity.get("provision_generation"),
                ),
                (release_evidence.get("vm_uid"), replacement_identity.get("vm_uid")),
                (
                    release_evidence.get("root_pvc_uid"),
                    replacement_identity.get("root_pvc_uid"),
                ),
                (
                    resume_successor.get("ssh_registration_id"),
                    final_context.get("ssh_registration_id"),
                ),
            )
        )
        network_qualified = bool(
            guest_network.get("interfaces")
            and guest_network.get("routes")
            and guest_network.get("default_route")
            and guest_network.get("dns")
            and (
                guest_network.get("netplan_sha256")
                or guest_network.get("networkd_sha256")
            )
        )
        ssh_host_fingerprint_pinned = bool(
            identity.get("ssh_host_key_fingerprint")
            and identity.get("ssh_host_key_fingerprint")
            == replacement_identity.get("ssh_host_key_fingerprint")
            == final_context.get("ssh_host_key_fingerprint")
        )
        trusted_stop_receipt = bool(
            stop_receipt.get("id")
            and re.fullmatch(
                r"sha256:[a-f0-9]{64}",
                str(stop_receipt.get("evidence_digest") or ""),
            )
            and int(stop_receipt.get("accepted_claim_token") or 0) > 0
            and stop_receipt.get("container_id")
            and stop_receipt.get("controller_identity")
            and all(
                str(stop_receipt.get(receipt_key) or "")
                == str(identity.get(identity_key) or "")
                for receipt_key, identity_key in (
                    ("vm_uid", "vm_uid"),
                    ("vmi_uid", "prior_vmi_uid"),
                    ("launcher_uid", "prior_launcher_uid"),
                    ("root_pvc_uid", "root_pvc_uid"),
                )
            )
        )

        # Missing exact stop evidence: a fabricated predecessor UUID is valid
        # syntax but has no append-only receipt, so replacement must pause.
        lease_token = await self._reset_lease(job_id)
        missing_identity = {
            **replacement_identity,
            "prior_vmi_uid": str(uuid4()),
            "prior_launcher_uid": str(uuid4()),
        }
        missing = await self._admit(
            identity=missing_identity,
            lease_token=lease_token,
            request_id=uuid4(),
            code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
            checkpoint=checkpoint,
        )
        async with self._gate_owned_reconciler("missing-stop-evidence"):
            missing_row = await self._wait(
                "missing-stop-evidence pause",
                lambda: self._wait_phase(missing.operation_id, {"paused_attention"}),
                timeout=120,
            )
        missing_dispatches = int(
            await self._sql(
                "SELECT count(*) FROM vm_workspace_recovery_jobs "
                "WHERE recovery_id=$1 AND resume_receipt IS NOT NULL",
                missing.operation_id,
            )
        )
        await self._resolve_fixture_recovery(missing.operation_id, job_id)

        leader_identity = await self._wait(
            "pre-handoff VM readiness",
            lambda: self._ready_identity(job_id),
            timeout=600,
        )
        lease_token = await self._reset_lease(job_id)
        leader_overlap = await self._leader_handoff_scenario(
            job_id=job_id,
            identity=leader_identity,
            lease_token=lease_token,
            checkpoint=checkpoint,
        )

        # Forced VMI deletion must never be promoted to exact termination.
        lease_token = await self._reset_lease(job_id)
        current = await self._wait(
            "pre-delete VM readiness", lambda: self._ready_identity(job_id), timeout=600
        )
        forced = await self._admit(
            identity=current,
            lease_token=lease_token,
            request_id=uuid4(),
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        await self._sync_gate_retention_pins("forced-deletion", forced.operation_id)
        await self._force_delete_vmi(job_id)
        async with self._gate_owned_reconciler("forced-deletion"):
            forced_row = await self._wait(
                "forced-deletion attention pause",
                lambda: self._wait_phase(forced.operation_id, {"paused_attention"}),
                timeout=180,
            )
        forced_dispatches = int(
            await self._sql(
                "SELECT count(*) FROM vm_workspace_recovery_jobs "
                "WHERE recovery_id=$1 AND resume_receipt IS NOT NULL",
                forced.operation_id,
            )
        )
        await self._resolve_fixture_recovery(forced.operation_id, job_id)

        # Hold an actual controller observation across the immutable 15-minute
        # database deadline. The service's final CAS must retain every hold.
        replacement_identity = await self._wait(
            "post-delete VM readiness",
            lambda: self._ready_identity(job_id),
            timeout=900,
        )
        lease_token = await self._reset_lease(job_id)
        deadline_evidence = await self._deadline_barrier_scenario(
            job_id=job_id,
            identity=replacement_identity,
            lease_token=lease_token,
            checkpoint=checkpoint,
            marker_path=marker_path,
            checkpoint_path=checkpoint_path,
            marker=marker,
        )
        revisions = await self._deployed_revisions(
            job_id, str(replacement_identity["root_pvc_uid"])
        )
        longhorn = await self._longhorn_volume_evidence(
            job_id, str(replacement_identity["root_pvc_uid"])
        )
        authority_evidence = {
            "application_api": application_api_evidence,
            "kubernetes_api": {
                "vm_uid": replacement_identity["vm_uid"],
                "pvc_uid": replacement_identity["root_pvc_uid"],
            },
            "postgresql": postgresql_evidence,
        }

        return {
            "authority_evidence": authority_evidence,
            "driver": {
                "protocol_version": PROTOCOL_VERSION,
                "live_sources": sorted(authority_evidence),
                "scenarios": list(REQUIRED_SCENARIOS),
            },
            "response_loss": response_loss,
            "leader_overlap": leader_overlap,
            "slow_boot": {
                "state": recovered["phase"],
                "age_seconds": round(time.monotonic() - recovery_started, 3),
                "injected_delay_seconds": self.slow_boot_delay_seconds,
                "executor_occupied_while_waiting": any(executor_occupancy_samples),
                "executor_occupancy_samples": executor_occupancy_samples,
                "attested": dispatches == 1,
            },
            "deadline": deadline_evidence,
            "missing_stop_evidence": {
                "state": missing_row.get("phase"),
                "reason_code": missing_row.get("reason_code"),
                "successor_dispatched": missing_dispatches > 0,
            },
            "forced_deletion": {
                "state": forced_row.get("phase"),
                "reason_code": forced_row.get("reason_code"),
                "successor_dispatched": forced_dispatches > 0,
            },
            "replacement": {
                "state": recovered["phase"],
                "pvc_uid_before": identity["root_pvc_uid"],
                "pvc_uid_after": replacement_identity["root_pvc_uid"],
                "marker_before": marker,
                "marker_after": marker_after,
                "checkpoint_before": checkpoint,
                "checkpoint_after": checkpoint_after,
                "pin_acknowledged": bool(pin.get("controller_pin_uid"))
                and bool(pin.get("controller_pin_resource_version"))
                and bool(pin.get("controller_pinned_at"))
                and pin.get("controller_state") in {"active", "released"},
                "trusted_stop_receipt": trusted_stop_receipt,
                "stop_receipt": stop_receipt,
                "resume_receipt": resume_receipt,
                "retention_pin": pin,
                "pinned_identity": pinned_identity,
                "network_qualified": network_qualified,
                "ssh_host_fingerprint_pinned": ssh_host_fingerprint_pinned,
                "successor_dispatches": dispatches,
                **longhorn,
            },
            "revisions": revisions,
        }

    async def cleanup(self) -> None:
        await self._cleanup_stop_retention()
        rows: list[dict[str, Any]] = []
        async with self.db.acquire() as conn:
            records = await conn.fetch(
                "SELECT j.id FROM jobs j JOIN users u ON u.id=j.user_id "
                "WHERE j.description=$1 AND j.execution_lane='stateless' "
                "AND j.context->>'vm_workspace_recovery_acceptance_gate'=$2 "
                "AND u.display_name=$3 AND u.is_admin=false",
                f"[vm-recovery-gate:{self.run_id}] retained disk fixture",
                self.run_id,
                f"VM recovery gate {self.run_id}",
            )
            rows = [dict(record) for record in records]
        for row in rows:
            job_id = row["id"]
            async with self.db.acquire() as conn, conn.transaction():
                await conn.execute(
                    "UPDATE vm_workspace_recovery_jobs SET participation='cancelled',"
                    "resolved_at=COALESCE(resolved_at,clock_timestamp()) "
                    "WHERE job_id=$1 AND resolved_at IS NULL",
                    job_id,
                )
                await conn.execute(
                    "UPDATE vm_workspace_recoveries r SET phase='cancelled',"
                    "resolved_at=COALESCE(r.resolved_at,clock_timestamp()) "
                    "WHERE r.id IN (SELECT recovery_id FROM vm_workspace_recovery_jobs "
                    "WHERE job_id=$1)",
                    job_id,
                )
                await conn.execute(
                    "UPDATE vm_workspace_recovery_retention_pins p "
                    "SET released_at=COALESCE(p.released_at,clock_timestamp()) "
                    "WHERE p.recovery_id IN (SELECT recovery_id "
                    "FROM vm_workspace_recovery_jobs WHERE job_id=$1)",
                    job_id,
                )
            await self._purge_fixture(job_id)

    async def _purge_fixture(self, job_id):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateFixturePurge,
            GateStopError,
            KubernetesStopObjects,
        )

        # Capture before Kubernetes inventory. The later normal release must
        # use this identity, never recapture a same-name replacement.
        try:
            identity = await self.provisioner.capture_vm_teardown_identity(str(job_id))
        except Exception:
            identity = None
        anchor = (
            {
                "generation": identity.provision_generation,
                "vm": identity.vm_uid,
                "pvc": identity.rootdisk_pvc_uid,
            }
            if identity is not None
            else None
        )

        async def release_captured(job, *, purge_disk):
            if identity is None:
                raise GateStopError("purge_identity_unproven")
            result = await self.provisioner.release_vm_captured(
                job,
                identity,
                purge_disk=purge_disk,
                capture_snapshot=False,
            )
            return result.disposition == "completed"

        await GateFixturePurge(
            self.db,
            KubernetesStopObjects(self._core),
            release_captured,
            self.run_id,
            self.namespace,
            anchor=anchor,
        ).run(job_id)


def _output_path(value: str) -> Path:
    path = Path(value)
    try:
        path.resolve().relative_to(_OUTPUT_ROOT.resolve())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "output must be under the gate temp directory"
        ) from exc
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--cleanup-only", action="store_true")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--protocol-version", required=True, type=int)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=_output_path)
    return parser


async def _async_main(args: argparse.Namespace) -> None:
    require_execution_guard(
        os.environ,
        confirmation=args.confirm,
        protocol_version=args.protocol_version,
    )
    if args.execute and args.output is None:
        raise RuntimeError("--execute requires --output")
    scenario = LiveScenario(args.run_id)
    await scenario.connect()
    try:
        if args.cleanup_only:
            await scenario.cleanup()
            return
        evidence = await scenario.execute()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(evidence, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        temporary.replace(args.output)
    finally:
        await scenario.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asyncio.run(_async_main(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
