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
from contextlib import suppress
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
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
        self.slow_boot_delay_seconds = int(
            os.environ.get("VM_WORKSPACE_RECOVERY_GATE_SLOW_BOOT_SECONDS", "15")
        )
        if not 1 <= self.slow_boot_delay_seconds <= 60:
            raise AcceptanceFailure(
                "slow-boot gate delay must be between 1 and 60 seconds"
            )
        self.job_id: UUID | None = None
        self._core: Any = None
        self._custom: Any = None

    async def connect(self) -> None:
        from kubernetes import client, config

        await self.db.connect()
        self.provisioner.connect(self.db)
        config.load_incluster_config()
        self._core = client.CoreV1Api()
        self._custom = client.CustomObjectsApi()

    async def close(self) -> None:
        await self.provisioner.disconnect()
        await self.db.close()

    async def _sql(self, query: str, *values: object) -> Any:
        async with self.db.acquire() as conn:
            return await conn.fetchval(query, *values)

    async def _row(self, query: str, *values: object) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(query, *values)
        return dict(row) if row is not None else {}

    async def _create_job(self) -> tuple[UUID, int]:
        job_id = uuid4()
        lease_token = 27
        created = await self.db.create_job(
            description=f"[vm-recovery-gate:{self.run_id}] retained disk fixture",
            context={"vm_workspace_recovery_acceptance_gate": self.run_id},
            origin="lifecycle",
            status="processing",
            execution_lane="stateless",
            job_id=job_id,
        )
        if UUID(str(created["id"])) != job_id:
            raise AcceptanceFailure("job helper changed the preallocated fixture ID")
        async with self.db.acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO run_queue (unit_id,unit_kind,state,lease_token,"
                "leased_by,leased_until,input_seq,consumed_seq,attempts_since_completion) "
                "VALUES ($1,'worker_batch','leased',$2,$3,clock_timestamp()+interval '5 minutes',1,0,1)",
                job_id,
                lease_token,
                f"vm-recovery-gate:{self.run_id}",
            )
            await conn.execute(
                "INSERT INTO worker_batch_attempts "
                "(job_id,lease_token,claimed_attempt) VALUES ($1,$2,1)",
                job_id,
                lease_token,
            )
        self.job_id = job_id
        return job_id, lease_token

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
        identity = {
            "owner_kind": "job",
            "owner_id": str(job_id),
            "provision_generation": context.get("provision_generation"),
            "namespace": context.get("namespace") or self.namespace,
            "vm_uid": context.get("vm_uid") or status.get("vm_uid"),
            "prior_vmi_uid": context.get("active_vmi_uid") or status.get("vmi_uid"),
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

    async def _application_api_evidence(self, job_id: UUID) -> dict[str, Any]:
        import httpx

        base = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8085").rstrip("/")
        key = os.environ.get("MCP_INTERNAL_KEY", "")
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{base}/api/jobs/{job_id}", headers={"X-Internal-Key": key}
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
        import asyncssh
        from orchestrator.services import resolve_ssh_key_path

        async with asyncssh.connect(
            str(identity["pod_ip"]),
            port=22,
            username="agent-host",
            client_keys=[resolve_ssh_key_path()],
            known_hosts=None,
            login_timeout=20,
        ) as connection:
            if value is not None:
                if not re.fullmatch(r"[A-Za-z0-9:._-]{1,256}", value):
                    raise AcceptanceFailure(
                        "gate marker contains unsafe shell characters"
                    )
                command = (
                    "mkdir -p /home/agent-host/.srw-recovery-gate && "
                    f"printf %s '{value}' > {path}"
                )
                result = await connection.run(command, check=True)
            else:
                result = await connection.run(f"cat {path}", check=True)
        return str(result.stdout).strip()

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

    async def _crash_launcher(self, identity: Mapping[str, Any]) -> None:
        from kubernetes.stream import stream

        pods = await asyncio.to_thread(
            self._core.list_namespaced_pod,
            namespace=self.namespace,
            label_selector=f"vm.kubevirt.io/name=agent-vm-{identity['owner_id']}",
        )
        candidates = [
            pod
            for pod in pods.items
            if str(pod.metadata.uid) == str(identity["prior_launcher_uid"])
        ]
        if len(candidates) != 1:
            raise AcceptanceFailure("exact launcher pod is unavailable")
        await asyncio.to_thread(
            stream,
            self._core.connect_get_namespaced_pod_exec,
            candidates[0].metadata.name,
            self.namespace,
            container="compute",
            command=["/bin/sh", "-c", "kill -TERM 1"],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
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

    async def execute(self) -> dict[str, Any]:
        from shared.workspace_recovery import WorkspaceRecoveryCode
        from orchestrator.services.vm_workspace_recovery_store import (
            VMWorkspaceRecoveryStore,
        )

        job_id, lease_token = await self._create_job()
        created = await self.provisioner.create_vm(
            str(job_id), cpu_cores=2, memory="2Gi", disk_size="12Gi"
        )
        if not created:
            raise AcceptanceFailure("controller refused the real VM fixture")
        identity = await self._wait(
            "initial VM readiness", lambda: self._ready_identity(job_id), timeout=1200
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
        await self._crash_launcher(identity)
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
        await self._set_vm_run_strategy(job_id, "Halted")
        await asyncio.sleep(self.slow_boot_delay_seconds)
        executor_occupancy_samples.append(await self._executor_occupied(job_id))
        await self._set_vm_run_strategy(job_id, "RerunOnFailure")
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
        store_a = VMWorkspaceRecoveryStore(self.db, worker_id="gate-leader-a")
        store_b = VMWorkspaceRecoveryStore(self.db, worker_id="gate-leader-b")
        retry_id = uuid4()
        retry_results = await asyncio.gather(
            store_a.retry_paused(
                job_id=job_id,
                operation_id=missing.operation_id,
                request_id=retry_id,
                actor_kind="system",
                actor_id="gate-leader-a",
            ),
            store_b.retry_paused(
                job_id=job_id,
                operation_id=missing.operation_id,
                request_id=retry_id,
                actor_kind="system",
                actor_id="gate-leader-b",
            ),
        )
        retry_receipts = int(
            await self._sql(
                "SELECT count(*) FROM vm_workspace_recovery_requests "
                "WHERE scope_kind='recovery' AND scope_id=$1 AND request_id=$2",
                missing.operation_id,
                retry_id,
            )
        )
        await self._resolve_fixture_recovery(
            UUID(retry_results[0]["operation_id"]), job_id
        )
        max_global, max_node, deadline_preserved = await self._probe_limits()

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
        await self._force_delete_vmi(job_id)
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

        # Expired deadline pauses before any probe and leaves the exact disk,
        # checkpoint reference, and marker intact.
        replacement_identity = await self._wait(
            "post-delete VM readiness",
            lambda: self._ready_identity(job_id),
            timeout=900,
        )
        lease_token = await self._reset_lease(job_id)
        deadline_disposition = await self._admit(
            identity=replacement_identity,
            lease_token=lease_token,
            request_id=uuid4(),
            code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            checkpoint=checkpoint,
        )
        await self._sql(
            "UPDATE vm_workspace_recoveries SET "
            "first_observed_at=clock_timestamp()-interval '901 seconds',"
            "deadline_at=clock_timestamp()-interval '1 second' "
            "WHERE id=$1 RETURNING id",
            deadline_disposition.operation_id,
        )
        await VMWorkspaceRecoveryStore(self.db, worker_id="gate-deadline").claim_due(
            deadline_disposition.operation_id
        )
        deadline_row = await self._operation(deadline_disposition.operation_id)
        marker_deadline = await self._ssh_file(replacement_identity, marker_path, None)
        checkpoint_deadline = await self._ssh_file(
            replacement_identity, checkpoint_path, None
        )
        checkpoint_reference = await self._sql(
            "SELECT checkpoint_id FROM vm_workspace_recovery_jobs "
            "WHERE recovery_id=$1 AND job_id=$2",
            deadline_disposition.operation_id,
            job_id,
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
            "leader_overlap": {
                "fault_injection": "concurrent_store_callers",
                "active_operations": len(
                    {result["operation_id"] for result in retry_results}
                ),
                "accepted_retry_results": retry_receipts,
                "max_global_probes": max_global,
                "max_node_probes": max_node,
                "configured_global_probe_limit": self.settings.max_global_probes,
                "configured_node_probe_limit": self.settings.max_probes_per_node,
                "deadline_preserved": deadline_preserved,
            },
            "slow_boot": {
                "state": recovered["phase"],
                "age_seconds": round(time.monotonic() - recovery_started, 3),
                "injected_delay_seconds": self.slow_boot_delay_seconds,
                "executor_occupied_while_waiting": any(executor_occupancy_samples),
                "executor_occupancy_samples": executor_occupancy_samples,
                "attested": dispatches == 1,
            },
            "deadline": {
                "fault_injection": "expired_deadline_test_hook",
                "state": deadline_row.get("phase"),
                "reason_code": deadline_row.get("reason_code"),
                "disk_retained": marker_deadline == marker,
                "checkpoint_retained": checkpoint_deadline == checkpoint
                and checkpoint_reference == checkpoint,
                "late_probe_released": deadline_row.get("phase") == "recovered",
            },
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
        rows: list[dict[str, Any]] = []
        async with self.db.acquire() as conn:
            records = await conn.fetch(
                "SELECT id FROM jobs WHERE description LIKE $1",
                f"[vm-recovery-gate:{self.run_id}]%",
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
            with suppress(Exception):
                await self.provisioner.delete_vm(str(job_id), purge_disk=True)


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
