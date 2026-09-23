"""Disposable, owner-authorized retained-disk Resume acceptance command.

The companion host wrapper owns the temporary ResourceQuota. This command
never modifies a Kubernetes quota or invents a queue lease/cleanup receipt.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import sys
import time
from typing import Any
from uuid import UUID


PROTOCOL_VERSION = 1
CONFIRMATION = "disposable-vm-retained-resume-gate-v1"
OUTPUT_ROOT = Path("/tmp/srw-vm-retained-resume-gate")
_RUN_ID = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")
_OWNED_NAME = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")
_IMMUTABLE_RETRY_FIELDS = (
    "request_id", "job_id", "provision_generation", "origin",
    "canonical_request", "request_digest", "controller_configuration_digest",
    "execution_id", "execution_revision", "execution_generation",
    "admission_deadline", "expected_pvc_uid", "predecessor_evidence",
    "predecessor_cleanup_admission_id", "created_at",
)


class AcceptanceFailure(RuntimeError):
    """An exact live authority or required observation is unavailable."""


def _uuid(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise AcceptanceFailure("required UUID identity is unavailable") from exc


def _object(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


def require_execution_guard(values: Mapping[str, Any], env: Mapping[str, str]) -> None:
    if env.get("VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED") != "true":
        raise AcceptanceFailure("disposable retained Resume gate is disabled")
    if values.get("confirm") != CONFIRMATION or values.get("protocol_version") != 1:
        raise AcceptanceFailure("disposable confirmation or protocol changed")
    run_id, namespace, context = (
        values.get("run_id"), values.get("namespace"), values.get("context")
    )
    if (
        not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id)
        or not isinstance(namespace, str)
        or not (_OWNED_NAME.fullmatch(namespace) or namespace == "srw")
        or not isinstance(context, str) or not _OWNED_NAME.fullmatch(context)
    ):
        raise AcceptanceFailure("run, namespace or context is not gate-owned")
    _uuid(values.get("job_id"))
    _uuid(values.get("expected_owner_id"))
    _uuid(values.get("expected_pvc_uid"))
    _uuid(values.get("cluster_uid"))
    if not values.get("cleanup_only"):
        _uuid(values.get("expected_pause_hold_id"))
    output = values.get("output")
    if not isinstance(output, Path):
        raise AcceptanceFailure("private output path is required")
    try:
        output.resolve().relative_to((OUTPUT_ROOT / run_id).resolve())
    except ValueError as exc:
        raise AcceptanceFailure("output is outside the private run directory") from exc
    expected_name = "cleanup.json" if values.get("cleanup_only") else "result.json"
    if output.name != expected_name:
        raise AcceptanceFailure("output filename is not the protocol result")
    if not values.get("cleanup_only") and output.exists():
        raise AcceptanceFailure("run result already exists")


def validate_fixture_snapshot(
    job: Mapping[str, Any], user: Mapping[str, Any], queue: Mapping[str, Any],
    *, run_id: str, expected_owner_id: str, expected_pvc_uid: str,
    expected_pause_hold_id: str, now: datetime,
) -> None:
    from orchestrator.operator_cli.vm_retained_resume_pause import (
        OwnerPauseHoldError, owned_pause_hold_id,
    )

    context = _object(job.get("context"))
    try:
        owned_pause_hold_id(
            context, owner_id=expected_owner_id,
            expected_hold_id=expected_pause_hold_id,
        )
    except OwnerPauseHoldError as exc:
        raise AcceptanceFailure("fixture owner Pause hold changed") from exc
    vm = _object(context.get("vm"))
    created = job.get("created_at")
    _uuid(job.get("id"))
    if (
        job.get("user_id") != user.get("id")
        or str(user.get("id")) != expected_owner_id
        or user.get("display_name") != f"A1 retained Resume gate {run_id}"
        or user.get("is_approved") is not True
        or user.get("is_admin") is not False
        or user.get("can_use_vm") is not True
        or job.get("status") != "paused"
        or job.get("execution_lane") != "stateless"
        or job.get("assigned_agent_id") is not None
        or context.get("vm_retained_resume_acceptance_gate") != run_id
        or context.get("_vm_creation_pending") is not None
        or any(context.get(key) is not None for key in (
            "_workspace_dispatch_authority", "_completion_control_claim",
            "_stateless_control_claim", "last_operator_pause_hold",
        ))
        or vm.get("status") != "ready"
        or _uuid(vm.get("rootdisk_pvc_uid")) != expected_pvc_uid
        or not isinstance(created, datetime)
        or created.tzinfo is None
        or not now - timedelta(hours=6) <= created <= now
        or queue.get("state") != "done"
        or queue.get("leased_by") is not None
        or queue.get("lease_token") != 0
    ):
        raise AcceptanceFailure("Job is not a fresh, quiescent owned fixture")
    _uuid(vm.get("provision_generation"))


def validate_retained_retry(
    before: Mapping[str, Any], after: Mapping[str, Any],
    *, job_id: str, expected_pvc_uid: str,
) -> None:
    if (
        str(before.get("job_id")) != job_id
        or str(before.get("expected_pvc_uid")) != expected_pvc_uid
        or before.get("admission_deadline") is None
        or before.get("origin") not in (None, "resume")
        or any(before.get(key) != after.get(key) for key in _IMMUTABLE_RETRY_FIELDS)
        or after.get("state") != "succeeded"
        or str(after.get("observed_pvc_uid")) != expected_pvc_uid
        or after.get("observed_vm_uid") is None
        or after.get("ready_at") is None
    ):
        raise AcceptanceFailure("retained creation retry changed immutable authority")
    _uuid(before.get("request_id"))
    _uuid(before.get("provision_generation"))
    _uuid(after.get("observed_vm_uid"))


def validate_worker_evidence(
    queue: Mapping[str, Any], attempt: Mapping[str, Any], pod: Mapping[str, Any],
    *, job_id: str, run_id: str,
) -> None:
    del run_id  # The immutable Job/source marker binds the run, not a pool Pod label.
    metadata = _object(pod.get("metadata"))
    labels = _object(metadata.get("labels"))
    if (
        str(queue.get("unit_id")) != job_id
        or queue.get("unit_kind") != "worker_batch"
        or queue.get("state") not in {"leased", "done"}
        or not isinstance(queue.get("lease_token"), int)
        or queue["lease_token"] <= 0
        or str(attempt.get("job_id")) != job_id
        or attempt.get("lease_token") != queue["lease_token"]
        or not isinstance(attempt.get("claimed_attempt"), int)
        or attempt["claimed_attempt"] <= 0
        or attempt.get("bundle_authorized_at") is None
        or not isinstance(attempt.get("authority_digest"), str)
        or not attempt["authority_digest"].startswith("sha256:")
        or not metadata.get("name")
        or str(queue.get("last_leased_by") or queue.get("leased_by") or "") != metadata.get("name")
        or labels.get("srw/class") != "agent-stateless"
        or labels.get("app.kubernetes.io/component") != "agent-stateless"
        or _object(pod.get("status")).get("phase") not in {"Running", "Succeeded"}
    ):
        raise AcceptanceFailure("actual worker claimant or bundle proof is unavailable")
    _uuid(metadata.get("uid"))


def _atomic_result(output: Path, value: Mapping[str, Any]) -> None:
    parent = output.parent
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if parent.is_symlink() or parent.stat().st_mode & 0o077:
        raise AcceptanceFailure("output directory is not private")
    temporary = parent / (".result-" + secrets.token_hex(8))
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


class LiveScenario:
    def __init__(self, args: argparse.Namespace):
        from orchestrator.database.postgres import PostgresDB
        from orchestrator.services.vm_provisioner import VMProvisioner

        self.args = args
        self.db = PostgresDB(min_connections=1, max_connections=4)
        self.provisioner = VMProvisioner()
        self.vm_namespace = os.environ.get("VM_NAMESPACE", args.namespace)
        self.core: Any = None
        self._api_client: Any = None

    async def connect(self) -> None:
        from kubernetes import client, config

        namespace = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        if namespace.read_text(encoding="utf-8").strip() != self.args.namespace:
            raise AcceptanceFailure("service-account namespace changed")
        config.load_incluster_config()
        self._api_client = client.ApiClient()
        self.core = client.CoreV1Api(self._api_client)
        await self.db.connect()
        self.provisioner.connect(self.db)
        if (
            self.provisioner.mode != "same-cluster"
            or self.vm_namespace != self.args.namespace
            or os.environ.get("VM_CREATION_RETRY_ENABLED") != "true"
            or os.environ.get("VM_NETWORK_PROFILE_ENABLED") != "true"
        ):
            raise AcceptanceFailure("retained profiled creation protocol is unavailable")

    async def close(self) -> None:
        await self.provisioner.disconnect()
        await self.db.close()
        if self._api_client is not None:
            self._api_client.close()

    async def row(self, query: str, *values: object) -> dict[str, Any]:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(query, *values)
        return dict(row) if row is not None else {}

    async def value(self, query: str, *values: object) -> Any:
        async with self.db.acquire() as conn:
            return await conn.fetchval(query, *values)

    async def wait(self, label: str, probe: Any, *, seconds: int = 900) -> Any:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = await probe()
            if value:
                return value
            await asyncio.sleep(1)
        raise AcceptanceFailure(f"timed out waiting for {label}")

    async def fixture_snapshot(self) -> tuple[dict[str, Any], dict[str, Any]]:
        job_id = UUID(self.args.job_id)
        job = await self.row(
            "SELECT id,user_id,status::text AS status,execution_lane,"
            "assigned_agent_id,created_at,context,config_override "
            "FROM jobs WHERE id=$1", job_id,
        )
        user = await self.row(
            "SELECT id,display_name,is_approved,is_admin,can_use_vm FROM users WHERE id=$1",
            job.get("user_id"),
        ) if job else {}
        queue = await self.row(
            "SELECT state,leased_by,lease_token FROM run_queue "
            "WHERE unit_id=$1 AND unit_kind='worker_batch'", job_id,
        )
        validate_fixture_snapshot(
            job, user, queue, run_id=self.args.run_id,
            expected_owner_id=self.args.expected_owner_id,
            expected_pvc_uid=self.args.expected_pvc_uid,
            expected_pause_hold_id=self.args.expected_pause_hold_id,
            now=await self.value("SELECT clock_timestamp()"),
        )
        if await self.value(
            "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)",
            job_id,
        ):
            raise AcceptanceFailure("fixture has prior worker attempts")
        other_queued = await self.value(
            "SELECT count(*) FROM run_queue WHERE unit_kind='worker_batch' "
            "AND state IN ('queued','leased') AND unit_id<>$1", job_id,
        )
        if other_queued:
            raise AcceptanceFailure("unrelated worker batches are runnable")
        await self.exclusive_vm_creation()
        vm = _object(_object(job["context"]).get("vm"))
        from orchestrator.services.vm_creation_preflight import _preflight

        preflight = _preflight(vm)
        if preflight is None:
            raise AcceptanceFailure("predecessor lacks an immutable creation request")
        original = await self.retry_row(preflight["request_id"])
        if (
            original.get("state") != "succeeded"
            or original.get("ready_at") is None
            or original.get("job_id") != job_id
            or str(original.get("observed_pvc_uid")) != self.args.expected_pvc_uid
            or str(original.get("provision_generation")) != vm.get("provision_generation")
        ):
            raise AcceptanceFailure("predecessor creation is not Ready under its frozen request")
        return job, original

    async def exclusive_vm_creation(self) -> None:
        if await self.value(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries "
            "WHERE state IN ('queued','reconciling','attention','cancel_requested') "
            "AND job_id<>$1)", UUID(self.args.job_id),
        ):
            raise AcceptanceFailure("quota could affect another VM creation retry")
        if await self.value(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id<>$1 "
            "AND context ? '_vm_creation_pending')", UUID(self.args.job_id),
        ):
            raise AcceptanceFailure("quota could affect another pending VM Job")

    async def retry_row(self, request_id: str) -> dict[str, Any]:
        return await self.row(
            "SELECT request_id,job_id,provision_generation,origin,"
            "canonical_request,request_digest,controller_configuration_digest,"
            "execution_id,execution_revision,execution_generation,"
            "admission_deadline,expected_pvc_uid,predecessor_evidence,"
            "predecessor_cleanup_admission_id,created_at,state,reason,"
            "observed_vm_uid,observed_pvc_uid,ready_at "
            "FROM vm_creation_retries WHERE request_id=$1", UUID(str(request_id)),
        )

    async def ready_identity(self, expected_generation: str) -> dict[str, Any] | None:
        from shared.vm_network_profile import NETWORK_PROFILE, reusable_profile_evidence

        status = _object(await self.provisioner.query_status(self.args.job_id, timeout=10))
        job = await self.row("SELECT context FROM jobs WHERE id=$1", UUID(self.args.job_id))
        vm = _object(_object(job.get("context")).get("vm"))
        provisioning = _object(vm.get("provisioning"))
        proof = _object(provisioning.get("identity"))
        live_provisioning = _object(status.get("provisioning"))
        identity = {
            "provision_generation": vm.get("provision_generation"),
            "vm_uid": vm.get("vm_uid"),
            "vmi_uid": proof.get("vmi_uid"),
            "launcher_uid": vm.get("active_pod_uid"),
            "root_pvc_uid": vm.get("rootdisk_pvc_uid"),
            "pod_ip": vm.get("pod_ip") or status.get("pod_ip"),
            "ssh_host_key_fingerprint": vm.get("ssh_host_key_fingerprint")
            or status.get("ssh_host_key_fingerprint"),
        }
        if (
            status.get("ready") is not True
            or vm.get("status") != "ready"
            or vm.get("identity_authenticated") is not True
            or vm.get("identity_provision_generation") != expected_generation
            or identity["provision_generation"] != expected_generation
            or identity["vm_uid"] != status.get("vm_uid")
            or identity["vmi_uid"] != live_provisioning.get("vmi_uid")
            or identity["launcher_uid"] != status.get("active_pod_uid")
            or identity["root_pvc_uid"] != status.get("rootdisk_pvc_uid")
            or identity["root_pvc_uid"] != self.args.expected_pvc_uid
            or not identity["pod_ip"]
            or not identity["ssh_host_key_fingerprint"]
            or not reusable_profile_evidence(
                vm.get("network_profile_evidence"), NETWORK_PROFILE,
                provision_generation=expected_generation,
                vm_uid=identity["vm_uid"], pvc_uid=identity["root_pvc_uid"],
                vmi_uid=identity["vmi_uid"], launcher_uid=identity["launcher_uid"],
            )
        ):
            return None
        for key in ("provision_generation", "vm_uid", "vmi_uid", "launcher_uid", "root_pvc_uid"):
            _uuid(identity[key])
        return identity

    async def pvc_pv_identity(self) -> dict[str, str]:
        pvc = await asyncio.to_thread(
            self.core.read_namespaced_persistent_volume_claim,
            f"agent-vm-{self.args.job_id}-rootdisk", self.vm_namespace,
        )
        if str(pvc.metadata.uid) != self.args.expected_pvc_uid:
            raise AcceptanceFailure("retained PVC UID changed")
        name = pvc.spec.volume_name
        if not name:
            raise AcceptanceFailure("retained PVC has no bound PV")
        pv = await asyncio.to_thread(self.core.read_persistent_volume, name)
        if not pv.metadata.uid or pv.spec.claim_ref.uid != pvc.metadata.uid:
            raise AcceptanceFailure("retained PV claim identity changed")
        return {"pvc_uid": str(pvc.metadata.uid), "pv_uid": str(pv.metadata.uid),
                "pv_name": name}

    async def revisions(self) -> dict[str, Any]:
        own = await asyncio.to_thread(
            self.core.read_namespaced_pod, os.environ.get("HOSTNAME", ""),
            self.args.namespace,
        )
        application = next((item.image_id for item in own.status.container_statuses or []
                            if item.name == "orchestrator" and item.image_id), None)
        controllers = await asyncio.to_thread(
            self.core.list_namespaced_pod, namespace=self.vm_namespace,
            label_selector="app.kubernetes.io/component=vm-controller",
        )
        images = {item.image_id for pod in controllers.items
                  for item in pod.status.container_statuses or []
                  if item.name == "vm-controller" and item.image_id}
        migration = await self.value(
            "SELECT filename FROM schema_migrations WHERE success=true "
            "ORDER BY filename DESC LIMIT 1",
        )
        if not application or len(images) != 1 or not migration:
            raise AcceptanceFailure("image or migration revision is unavailable")
        return {"orchestrator_image_id": application,
                "vm_controller_image_id": next(iter(images)),
                "migration_head": migration,
                "creation_retry_enabled": os.environ.get("VM_CREATION_RETRY_ENABLED"),
                "network_profile_enabled": os.environ.get("VM_NETWORK_PROFILE_ENABLED"),
                "idle_release_enabled": os.environ.get("WORKSPACE_IDLE_RELEASE_ENABLED")}

    async def ssh(self, identity: Mapping[str, Any], command: str) -> None:
        from orchestrator.services import resolve_ssh_key_path
        from orchestrator.services.ssh_helpers import pinned_agent_ssh_command
        from orchestrator.services.subprocess_effect import (
            communicate_bounded, create_owned_subprocess_exec,
        )

        async with pinned_agent_ssh_command(
            str(identity["pod_ip"]), 22, command,
            expected_host_key_fingerprint=str(identity["ssh_host_key_fingerprint"]),
            key_path=resolve_ssh_key_path(), connect_timeout_s=10, batch_mode=True,
        ) as argv:
            process = await create_owned_subprocess_exec(
                *argv, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await communicate_bounded(
                process, timeout=20, stdout_limit=2048, stderr_limit=2048,
            )
        if process.returncode != 0:
            raise AcceptanceFailure("pinned SSH sentinel operation failed")

    async def sentinel_write(self, identity: Mapping[str, Any]) -> str:
        value = secrets.token_hex(32)
        digest = hashlib.sha256(value.encode()).hexdigest()
        path = f"/home/agent-host/workspace/.srw-a1-gate/{self.args.run_id}/sentinel"
        await self.ssh(identity, " && ".join((
            f"mkdir -p {shlex.quote(str(Path(path).parent))}",
            f"test ! -e {shlex.quote(path)}",
            f"printf %s {shlex.quote(value)} > {shlex.quote(path)}",
        )))
        await self.sentinel_verify(identity, digest)
        return digest

    async def sentinel_verify(self, identity: Mapping[str, Any], digest: str) -> None:
        path = f"/home/agent-host/workspace/.srw-a1-gate/{self.args.run_id}/sentinel"
        relative = f".srw-a1-gate/{self.args.run_id}/sentinel"
        command = " && ".join((
            f"test -f {shlex.quote(path)}",
            f"test \"$(sha256sum {shlex.quote(path)} | cut -d' ' -f1)\" = {shlex.quote(digest)}",
            "test \"$(git -C /home/agent-host/workspace rev-parse --is-inside-work-tree)\" = true",
            "! git -C /home/agent-host/workspace ls-files --error-unmatch -- "
            f"{shlex.quote(relative)} >/dev/null 2>&1",
        ))
        await self.ssh(identity, command)

    async def owner_resume(self, *, expected_status: int) -> dict[str, Any]:
        """Use the public owner route with a short-lived, revocable user token."""
        import httpx

        owner = await self.value("SELECT user_id FROM jobs WHERE id=$1", UUID(self.args.job_id))
        if owner is None or str(owner) != self.args.expected_owner_id:
            raise AcceptanceFailure("fixture owner disappeared")
        raw_token = "srw_" + secrets.token_urlsafe(32)
        issued = await self.db.create_mcp_token(
            user_id=str(owner), name=f"a1-{self.args.run_id}",
            token_hash=hashlib.sha256(raw_token.encode()).hexdigest(),
            token_prefix=raw_token[:12], scope="user",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
            origin="vm-retained-resume-acceptance",
        )
        base = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8085").rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(
                    f"{base}/api/jobs/{self.args.job_id}/resume",
                    headers={"Authorization": f"Bearer {raw_token}"}, json={},
                )
            if response.status_code != expected_status:
                raise AcceptanceFailure("ordinary owner Resume returned an unexpected status")
            return _object(response.json()) if expected_status == 200 else {}
        finally:
            if not await self.db.revoke_mcp_token(str(issued["id"]), str(owner)):
                raise AcceptanceFailure("owner route token could not be revoked")

    async def verify_owner_pause_lift(self) -> None:
        context = _object(await self.value(
            "SELECT context FROM jobs WHERE id=$1 AND user_id=$2",
            UUID(self.args.job_id), UUID(self.args.expected_owner_id),
        ))
        last = _object(context.get("last_operator_pause_hold"))
        if (
            "_operator_pause_hold" in context
            or context.get("vm_retained_resume_acceptance_gate") != self.args.run_id
            or last.get("hold_id") != self.args.expected_pause_hold_id
            or last.get("paused_by") != self.args.expected_owner_id
            or last.get("source") != "public_pause"
            or last.get("version") != 1
            or not isinstance(last.get("lifted_at"), str)
        ):
            raise AcceptanceFailure("owner Resume did not lift the exact fixture Pause")

    async def retire_predecessor(self, vm: dict[str, Any]) -> dict[str, Any]:
        """Pause only the gate call before the production physical-stop transport."""
        from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm
        from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore

        entered, release = asyncio.Event(), asyncio.Event()
        delegate = self.provisioner

        class PauseBeforeStop:
            async def capture_vm_teardown_identity(self, owner_id: str, *, entity_type: str):
                return await delegate.capture_vm_teardown_identity(
                    owner_id, entity_type=entity_type,
                )

            async def release_vm_captured(self, *args: Any, **kwargs: Any):
                entered.set()
                await release.wait()
                return await delegate.release_vm_captured(*args, **kwargs)

        task = asyncio.create_task(recycle_provisioning_vm(
            self.args.job_id, vm, db=self.db, provisioner=PauseBeforeStop(),
            recovery_store=VMWorkspaceRecoveryStore(self.db),
            now=time.time(), phase_timeout=False,
        ))
        open_cleanup: dict[str, Any] = {}
        try:
            await asyncio.wait_for(entered.wait(), timeout=30)
            open_cleanup = await self.row(
                "SELECT id,pvc_uid,completed_at FROM vm_workspace_cleanup_admissions "
                "WHERE owner_kind='job' AND owner_id=$1 "
                "AND source='dispatcher_vm_recycle' AND completed_at IS NULL "
                "ORDER BY admitted_at DESC LIMIT 1",
                UUID(self.args.job_id),
            )
            if (
                str(open_cleanup.get("pvc_uid")) != self.args.expected_pvc_uid
                or open_cleanup.get("completed_at") is not None
            ):
                raise AcceptanceFailure("exact retained cleanup admission did not open")
            await self.owner_resume(expected_status=409)
        finally:
            release.set()
            # Do not leave a supported cleanup task running against a closing
            # DB/provisioner if the HTTP negative assertion fails. Preserve
            # the exception while still allowing its physical stop to settle.
            result = await asyncio.wait_for(task, timeout=300)
        if result != "completed":
            raise AcceptanceFailure("supported predecessor retirement did not settle")
        settled = await self.row(
            "SELECT completed_at,outcome,pvc_uid FROM vm_workspace_cleanup_admissions "
            "WHERE id=$1", open_cleanup["id"],
        )
        if (
            settled.get("completed_at") is None
            or settled.get("outcome") != "completed"
            or str(settled.get("pvc_uid")) != self.args.expected_pvc_uid
        ):
            raise AcceptanceFailure("predecessor cleanup receipt is incomplete")
        process_zero = await self.row(
            "SELECT id,observed_at FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
            "AND provisioner='vm' AND runtime_incarnation=$2",
            UUID(self.args.job_id), str(vm["provision_generation"]),
        )
        if not process_zero or process_zero.get("observed_at") is None:
            raise AcceptanceFailure("exact predecessor process-zero receipt is unavailable")
        return {"cleanup_admission_id": str(open_cleanup["id"]),
                "process_zero_receipt_id": str(process_zero["id"]),
                "process_zero_observed_at": process_zero["observed_at"]}

    async def begin_replacement(self, old_request: Mapping[str, Any]) -> dict[str, Any]:
        from orchestrator.services.retained_vm_workspaces import provision_authority
        from shared.vm_network_profile import NETWORK_PROFILE

        request = _object(old_request.get("canonical_request"))
        image = request.get("vm_image")
        if (
            not isinstance(image, str)
            or re.search(r"@sha256:[0-9a-f]{64}$", image) is None
            or request.get("network_profile") != NETWORK_PROFILE
        ):
            raise AcceptanceFailure("predecessor image or network profile is unpinned")
        authority = await provision_authority(self.db, self.args.job_id)
        if (
            authority is None
            or authority.get("network_profile") != NETWORK_PROFILE
            or _object(authority.get("storage")).get("pvc_uid") != self.args.expected_pvc_uid
        ):
            raise AcceptanceFailure("retained storage/profile authority changed")
        result = await self.provisioner.create_vm(
            self.args.job_id,
            agent_config=str(request["agent_config"]),
            vm_image=image,
            cpu_cores=int(request["cpu_cores"]),
            memory=str(request["memory"]),
            description=str(request.get("description") or ""),
            fresh=True,
            disk_size=str(request["disk_size"]),
            initialization=request.get("initialization"),
            workspace_storage=authority["storage"],
            preparation=request.get("preparation"),
            network_profile=NETWORK_PROFILE,
        )
        if not isinstance(result, Mapping) or result.get("status") != "creation_pending":
            raise AcceptanceFailure("replacement did not enter durable creation preflight")
        request_id = _uuid(result.get("request_id"))

        async def admitted() -> dict[str, Any] | None:
            row = await self.retry_row(request_id)
            return row if row and row.get("expected_pvc_uid") is not None else None

        row = await self.wait("retained creation admission", admitted, seconds=120)
        if (
            str(row.get("expected_pvc_uid")) != self.args.expected_pvc_uid
            or row.get("origin") != "resume"
            or row.get("predecessor_cleanup_admission_id") is None
            or row.get("admission_deadline") is None
            or _object(row.get("canonical_request")).get("network_profile") != NETWORK_PROFILE
        ):
            raise AcceptanceFailure("new creation request is not retained-profiled")
        return row

    async def rejected_vm_effect(self, request_id: str) -> bool:
        from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

        state = await VMCreationRetryStore(self.db).inspect(request_id=request_id)
        effects = state.get("effects") or []
        return any(
            _object(effect.get("carrier_intent")).get("effect_kind") == "vm"
            and effect.get("state") == "rejected"
            for effect in effects if isinstance(effect, Mapping)
        )

    async def capture_worker(self) -> dict[str, Any] | None:
        """Require a live, authorized lease before terminal rotation hides it."""
        from kubernetes.client.exceptions import ApiException

        queue = await self.row(
            "SELECT unit_id,unit_kind,state,lease_token,leased_by,last_leased_by "
            "FROM run_queue WHERE unit_id=$1", UUID(self.args.job_id),
        )
        if queue.get("state") != "leased" or not queue.get("leased_by"):
            if queue.get("state") == "done" and queue.get("lease_token", 0) > 0:
                raise AcceptanceFailure("worker finished before live claimant proof")
            return None
        attempt = await self.row(
            "SELECT job_id,lease_token,claimed_attempt,bundle_authorized_at,"
            "authority_digest FROM worker_batch_attempts "
            "WHERE job_id=$1 AND lease_token=$2",
            UUID(self.args.job_id), queue["lease_token"],
        )
        if attempt.get("bundle_authorized_at") is None:
            return None
        try:
            obj = await asyncio.to_thread(
                self.core.read_namespaced_pod, queue["leased_by"], self.args.namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
        pod = obj.to_dict()
        # Kubernetes model to_dict uses Python field names, and UUID/name are
        # the actual ApiServer object's UID/name. Normalize only this shape.
        metadata = _object(pod.get("metadata"))
        status = _object(pod.get("status"))
        evidence = {
            "metadata": {"name": metadata.get("name"), "uid": metadata.get("uid"),
                         "labels": metadata.get("labels")},
            "status": {"phase": status.get("phase")},
        }
        validate_worker_evidence(queue, attempt, evidence,
                                 job_id=self.args.job_id, run_id=self.args.run_id)
        return {"queue": queue, "attempt": attempt,
                "pod_uid": str(metadata["uid"]), "pod_name": str(metadata["name"])}

    async def terminal_worker(self, captured: Mapping[str, Any]) -> dict[str, Any] | None:
        queue = await self.row(
            "SELECT unit_id,unit_kind,state,lease_token,leased_by,last_leased_by "
            "FROM run_queue WHERE unit_id=$1", UUID(self.args.job_id),
        )
        job = await self.row(
            "SELECT status::text AS status,completed_at,context "
            "FROM jobs WHERE id=$1", UUID(self.args.job_id),
        )
        if job.get("status") != "completed" or queue.get("state") != "done":
            return None
        attempt = await self.row(
            "SELECT job_id,lease_token,claimed_attempt,bundle_authorized_at,"
            "authority_digest,refunded_at FROM worker_batch_attempts "
            "WHERE job_id=$1 AND lease_token=$2",
            UUID(self.args.job_id), captured["queue"]["lease_token"],
        )
        command = await self.row(
            "SELECT id,state,outcome,finalized_at,accepted_lease_token "
            "FROM job_completion_commands WHERE job_id=$1 "
            "AND accepted_lease_token=$2 ORDER BY report_seq DESC LIMIT 1",
            UUID(self.args.job_id), captured["queue"]["lease_token"],
        )
        if (
            queue.get("lease_token") != captured["queue"]["lease_token"]
            or queue.get("last_leased_by") != captured["pod_name"]
            or attempt.get("authority_digest") != captured["attempt"]["authority_digest"]
            or attempt.get("bundle_authorized_at") != captured["attempt"]["bundle_authorized_at"]
            or attempt.get("refunded_at") is not None
            or command.get("state") != "done"
            or command.get("outcome") is None
            or command.get("finalized_at") is None
            or command.get("accepted_lease_token") != captured["queue"]["lease_token"]
            or job.get("completed_at") is None
        ):
            raise AcceptanceFailure("terminal Job lacks its captured worker result")
        return {"pod_name": captured["pod_name"], "pod_uid": captured["pod_uid"],
                "lease_token": queue["lease_token"],
                "authority_digest": attempt["authority_digest"],
                "completion_command_id": str(command["id"]),
                "completed_at": job["completed_at"]}

    async def execute(self) -> dict[str, Any]:
        from shared.vm_network_profile import NETWORK_PROFILE

        started = datetime.now(timezone.utc)
        job, predecessor_retry = await self.fixture_snapshot()
        revisions = await self.revisions()
        old = _object(_object(job["context"]).get("vm"))
        predecessor = await self.ready_identity(str(predecessor_retry["provision_generation"]))
        if predecessor is None or predecessor["vm_uid"] != str(predecessor_retry["observed_vm_uid"]):
            raise AcceptanceFailure("predecessor Ready identity changed")
        storage_before = await self.pvc_pv_identity()
        digest = await self.sentinel_write(predecessor)
        host_exchange("ARM", digest)
        cleanup = await self.retire_predecessor(old)
        if await self.pvc_pv_identity() != storage_before:
            raise AcceptanceFailure("predecessor retirement changed retained PVC/PV")
        await self.exclusive_vm_creation()
        host_exchange("QUOTA_INSTALL", cleanup["cleanup_admission_id"])
        before = await self.begin_replacement(predecessor_retry)
        request_id = str(before["request_id"])
        await self.wait(
            "quota-rejected VM POST", lambda: self.rejected_vm_effect(request_id),
            seconds=180,
        )
        resumed = await self.owner_resume(expected_status=200)
        if resumed.get("vm_creation_retry_request_id") != request_id:
            raise AcceptanceFailure("ordinary Resume did not join the failed create")
        await self.verify_owner_pause_lift()
        held = await self.retry_row(request_id)
        if any(before.get(key) != held.get(key) for key in _IMMUTABLE_RETRY_FIELDS):
            raise AcceptanceFailure("ordinary Resume changed frozen creation intent")
        host_exchange("QUOTA_RELEASE", request_id)
        worker_task = asyncio.create_task(self.wait(
            "authorized real worker lease", self.capture_worker, seconds=900,
        ))

        async def ready() -> dict[str, Any] | None:
            after = await self.retry_row(request_id)
            if after.get("state") != "succeeded" or after.get("ready_at") is None:
                return None
            identity = await self.ready_identity(str(before["provision_generation"]))
            if identity is None or identity["vm_uid"] != str(after.get("observed_vm_uid")):
                return None
            validate_retained_retry(before, after, job_id=self.args.job_id,
                                    expected_pvc_uid=self.args.expected_pvc_uid)
            return {"retry": after, "identity": identity}

        try:
            observed = await self.wait("retained Ready successor", ready, seconds=900)
            if await self.pvc_pv_identity() != storage_before:
                raise AcceptanceFailure("Ready successor changed the retained PVC/PV")
            await self.sentinel_verify(observed["identity"], digest)
            worker = await worker_task
            # The provider holds job_complete until this real claimant and
            # pinned-SSH file proof are captured. Normal S36 completion may
            # purge the guest immediately after that barrier is released.
            await self.sentinel_verify(observed["identity"], digest)
            host_exchange("PROVIDER_BARRIER", digest)
        finally:
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
        terminal = await self.wait(
            "terminal Job result", lambda: self.terminal_worker(worker), seconds=900,
        )
        host_exchange("PROVIDER_VERIFY", digest)
        return {
            "protocol_version": 1, "run_id": self.args.run_id,
            "outcome": "passed", "job_id": self.args.job_id,
            "owner_id": self.args.expected_owner_id,
            "cluster_uid": self.args.cluster_uid, "namespace": self.args.namespace,
            "started_at": started, "completed_at": datetime.now(timezone.utc),
            "predecessor": predecessor,
            "predecessor_request_id": str(predecessor_retry["request_id"]),
            "predecessor_cleanup": cleanup,
            "storage": storage_before,
            "sentinel_sha256": digest,
            "network_profile": NETWORK_PROFILE,
            "replacement_request_id": request_id,
            "replacement_generation": str(before["provision_generation"]),
            "request_digest": before["request_digest"],
            "controller_configuration_digest": before["controller_configuration_digest"],
            "execution_id": str(before["execution_id"]),
            "execution_revision": before["execution_revision"],
            "execution_generation": before["execution_generation"],
            "admission_deadline": before["admission_deadline"],
            "ready_successor": observed["identity"],
            "ready_at": observed["retry"]["ready_at"],
            "worker": terminal,
            "revisions": revisions,
            "assertions": {
                "owner_http_open_cleanup_refused": True,
                "owner_http_retained_resume_accepted": True,
                "quota_vm_effect_rejected": True,
                "frozen_request_preserved": True,
                "same_pvc_pv": True,
                "pinned_ssh_sentinel_pre_terminal_worker": True,
                "normal_terminal_cleanup_allowed": True,
                "real_worker_bundle_and_terminal_result": True,
                "provider_tool_sequence_verified_by_host": True,
            },
        }

    async def cleanup_only(self) -> dict[str, Any]:
        """Request normal owner deletion only for this exact disposable Job."""
        import httpx

        job = await self.row(
            "SELECT id,user_id,status::text AS status,execution_lane,context "
            "FROM jobs WHERE id=$1", UUID(self.args.job_id),
        )
        context = _object(job.get("context"))
        vm = _object(context.get("vm"))
        if (
            context.get("vm_retained_resume_acceptance_gate") != self.args.run_id
            or job.get("execution_lane") != "stateless"
            or str(vm.get("rootdisk_pvc_uid")) != self.args.expected_pvc_uid
            or job.get("status") not in {"paused", "completed", "failed", "cancelled"}
        ):
            raise AcceptanceFailure("cleanup target is not this exact disposable Job")
        queue = await self.row(
            "SELECT state,leased_by FROM run_queue WHERE unit_id=$1",
            UUID(self.args.job_id),
        )
        if queue.get("state") not in {"done", "parked"} or queue.get("leased_by"):
            raise AcceptanceFailure("cleanup target has live worker authority")
        if await self.value(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND completed_at IS NULL)",
            UUID(self.args.job_id),
        ):
            raise AcceptanceFailure("cleanup target has an open physical-stop admission")
        if await self.value(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 "
            "AND state IN ('queued','reconciling','attention','cancel_requested'))",
            UUID(self.args.job_id),
        ):
            raise AcceptanceFailure("cleanup target has an unresolved creation retry")
        owner = await self.row(
            "SELECT id,display_name,is_approved,is_admin FROM users WHERE id=$1",
            job.get("user_id"),
        )
        if (
            str(owner.get("id")) != self.args.expected_owner_id
            or owner.get("display_name") != f"A1 retained Resume gate {self.args.run_id}"
            or owner.get("is_approved") is not True
            or owner.get("is_admin") is not False
        ):
            raise AcceptanceFailure("cleanup owner changed")
        raw_token = "srw_" + secrets.token_urlsafe(32)
        issued = await self.db.create_mcp_token(
            user_id=str(owner["id"]), name=f"a1-cleanup-{self.args.run_id}",
            token_hash=hashlib.sha256(raw_token.encode()).hexdigest(),
            token_prefix=raw_token[:12], scope="user",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            origin="vm-retained-resume-acceptance",
        )
        base = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8085").rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.delete(
                    f"{base}/api/jobs/{self.args.job_id}",
                    headers={"Authorization": f"Bearer {raw_token}"},
                )
            if response.status_code != 200:
                raise AcceptanceFailure("normal owner Job deletion was not accepted")
        finally:
            if not await self.db.revoke_mcp_token(str(issued["id"]), str(owner["id"])):
                raise AcceptanceFailure("cleanup route token could not be revoked")
        return {"protocol_version": 1, "run_id": self.args.run_id,
                "job_id": self.args.job_id, "outcome": "cleanup_requested",
                "owner_id": self.args.expected_owner_id,
                "route": "DELETE /api/jobs/{job_id}",
                "physical_settlement": "pending_production_authority"}


def host_exchange(stage: str, value: str) -> str:
    """One bounded host-owned control barrier; no token or content in stdout."""
    if stage not in {"ARM", "QUOTA_INSTALL", "QUOTA_RELEASE",
                     "PROVIDER_BARRIER", "PROVIDER_VERIFY"}:
        raise AcceptanceFailure("unsupported host barrier")
    if len(value) > 128 or "\n" in value or "\r" in value:
        raise AcceptanceFailure("host barrier value is malformed")
    print(f"SRW_A1_{stage}:{value}", flush=True)
    line = sys.stdin.readline(256).strip()
    if line != f"SRW_A1_ACK:{stage}":
        raise AcceptanceFailure("host barrier acknowledgement is missing")
    return line


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--cleanup-only", action="store_true")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--expected-owner-id", required=True)
    parser.add_argument("--expected-pause-hold-id", default="")
    parser.add_argument("--expected-pvc-uid", required=True)
    parser.add_argument("--cluster-uid", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--protocol-version", type=int, required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    require_execution_guard(vars(args), os.environ)
    scenario = LiveScenario(args)
    try:
        await scenario.connect()
        result = await (scenario.execute() if args.execute else scenario.cleanup_only())
        _atomic_result(args.output, result)
        return 0
    except Exception as exc:
        # Controller/API exceptions may contain signed URLs or credentials.
        # Persist only a closed error class and the run identity.
        if not args.output.exists():
            _atomic_result(args.output, {
                "protocol_version": 1, "run_id": args.run_id,
                "job_id": args.job_id, "outcome": "held",
                "error_class": type(exc).__name__,
            })
        return 1
    finally:
        await scenario.close()


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_async_main(build_parser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
