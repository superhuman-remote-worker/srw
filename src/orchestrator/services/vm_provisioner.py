"""VM lifecycle management for explicit same-cluster and external modes."""

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import ipaddress
import json
import logging
import os
import time
from typing import Any, Optional
from uuid import UUID, uuid4

import httpx

from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from orchestrator.services.managed_repository_process_retirement import (
    retire_managed_repository_processes,
)

from orchestrator.services.container_provisioner import (
    DEFAULT_NETWORK_TIER,
    WorkspaceRuntimeAttestation,
    WorkspaceRuntimeAuthorityError,
    WorkspaceRuntimeRecoveryRequired,
)
from shared.workspace_recovery import WorkspaceRecoveryCode
from orchestrator.services.nats_bridge import nats_bridge
from orchestrator.services.vm_lifecycle_auth import (
    AUTH_FIELD,
    configured_secret,
    sign_payload,
    unsigned_payload,
    verify_payload,
)

logger = logging.getLogger(__name__)

_VALID_VM_MODES = frozenset({"off", "same-cluster", "external"})
_EXTERNAL_VM_PROVISIONING_UNAVAILABLE = (
    "external VM provisioning is disabled until an authenticated "
    "guest-management transport is available"
)
_warned_unset_vm_mode = False
_warned_invalid_vm_mode = False


@dataclass(frozen=True, slots=True)
class VMTeardownIdentity:
    """Immutable VM incarnation captured before a destructive lifecycle call."""

    provision_generation: str
    vm_uid: str | None
    rootdisk_pvc_uid: str | None
    ssh_host: str | None = None
    ssh_port: int | None = None
    ssh_host_key_fingerprint: str | None = None
    credential_runtime_started: bool | None = None


@dataclass(frozen=True, slots=True)
class VMTeardownResult:
    """Bounded result distinguishing completion from identity supersession."""

    disposition: str
    deleted: bool


@dataclass(frozen=True, slots=True)
class _VMTeardownProbe:
    """Authenticated observation used to reconcile an ambiguous delete."""

    disposition: str
    identity: VMTeardownIdentity | None = None
    rootdisk_identity_known: bool = False


def _provision_generation(value: object) -> str | None:
    if not isinstance(value, str) or len(value) != 36:
        return None
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        return None
    return value if str(parsed) == value else None


def _safe_vm_uid(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 256
        or any(character.isspace() for character in value)
    ):
        return None
    return value


def _safe_ssh_host_key_fingerprint(value: object) -> str | None:
    """Accept only a canonical OpenSSH SHA256 fingerprint."""

    if not isinstance(value, str) or not value.startswith("SHA256:"):
        return None
    encoded = value.removeprefix("SHA256:")
    if len(encoded) != 43:
        return None
    try:
        digest = base64.b64decode((encoded + "=").encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError):
        return None
    return value if len(digest) == 32 else None


def _extract_vm_context(job: dict) -> dict:
    """Extract the vm sub-dict from a job's context."""
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    return ctx.get("vm", {})


def _extract_thread_vm_context(thread: dict) -> dict:
    """Extract the VM projection from one thread's durable metadata."""

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    return metadata.get("vm", {}) if isinstance(metadata, Mapping) else {}


def _http_lifecycle_query(
    payload: Mapping[str, Any], *, operation: str, secret: bytes | None
) -> dict[str, str]:
    """Encode an authenticated envelope into stable HTTP query fields."""

    signed = sign_payload(
        payload,
        direction="request",
        operation=operation,
        secret=secret,
    )
    auth = signed.get(AUTH_FIELD)
    if not isinstance(auth, Mapping):
        return {}
    return {
        "lifecycle_auth": str(auth["signature"]),
        "lifecycle_auth_issued_at": str(auth["issued_at"]),
        "lifecycle_auth_request_id": str(auth["request_id"]),
    }


def vm_persistent_rootdisk_enabled() -> bool:
    """Whether the VM controller keeps rootdisks across VM deletion.

    Mirrors the controller's own ``VM_PERSISTENT_ROOTDISK``; the orchestrator
    cannot observe the controller's config, so the two are set from the same
    Helm value and this is the orchestrator's copy. Read at call time rather
    than import so tests (and a config reload) see changes.

    **Enable the controller first.** Turning this on against a controller that
    still cascade-deletes disks would let VM session suspend tear a workspace
    down believing the files survive — they would not. The reverse order is
    harmless: the controller keeps disks nobody asks it to keep, and the
    delete-status handler records what actually happened either way.

    knowledge-base/knowledge/features/vm_persistent_rootdisk.md
    """
    return os.environ.get("VM_PERSISTENT_ROOTDISK", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )


class VMProvisioner:
    """Unified VM provisioner selected by the explicit ``VM_MODE`` contract."""

    def __init__(self):
        self._db: Optional[Any] = None
        self._phase_store = None
        self._snapshot_service: Optional[Any] = None
        self._vm_namespace: str = os.environ.get("VM_NAMESPACE", "agent-vms")
        self._default_vm_image: str = os.environ.get(
            "DEFAULT_VM_IMAGE",
            "ghcr.io/superhuman-remote-worker/srw-agent-vm-base:latest",
        )
        # HTTP controller transport (same-cluster, no NATS).
        self._controller_url: str = os.environ.get("VM_CONTROLLER_URL", "").rstrip("/")
        self._http_client: Optional[httpx.AsyncClient] = None
        self._http_timeout: float = float(os.environ.get("VM_CONTROLLER_TIMEOUT", "30"))
        self._lifecycle_hmac_secret = configured_secret()

    @property
    def is_available(self) -> bool:
        """Whether the backend required by the configured VM mode is available."""
        if self.mode == "same-cluster":
            return self._http_available
        if self.mode == "external":
            # Guest management/sudo over the legacy external NATS path is
            # intentionally contained.  A connected broker proves transport
            # reachability, not authenticated authority for the selected VM.
            # Existing exact-generation lifecycle probes may still use the
            # bridge during a bounded cleanup, but no new VM may be admitted.
            return False
        return False

    @property
    def unavailable_reason(self) -> str | None:
        """Return a stable operator-facing reason when provisioning is closed."""

        mode = self.mode
        if mode == "external":
            return _EXTERNAL_VM_PROVISIONING_UNAVAILABLE
        if mode == "same-cluster" and not self._http_available:
            return "same-cluster VM controller URL is not configured"
        if mode == "off":
            return "VM provisioning is disabled by VM_MODE"
        return None

    @property
    def lifecycle_available(self) -> bool:
        """Whether an authenticated controller can manage an existing VM.

        External creation is contained above, but exact-generation status,
        deletion and orphan reaping remain necessary to retire VMs which
        predate that containment.  Callers must never use this wider signal
        to admit creation.
        """

        if self.mode == "external":
            return self._nats_available
        if self.mode == "same-cluster":
            return self._http_available
        return False

    @property
    def _http_available(self) -> bool:
        """True if a co-located VM controller HTTP endpoint is configured."""
        return self.mode == "same-cluster" and bool(self._controller_url)

    @property
    def _nats_available(self) -> bool:
        """True if external lifecycle transport is connected *and* authenticated.

        Broker reachability alone is not controller authority. Keep this check
        at the shared transport seam so direct list/status/delete callers cannot
        accidentally emit or trust the legacy unsigned lifecycle protocol.
        """

        return bool(
            self.mode == "external"
            and nats_bridge.is_available
            and nats_bridge.lifecycle_identity_authenticated is True
        )

    @property
    def _docker_available(self) -> bool:
        """True if QEMU-in-Docker VMs are configured."""
        from orchestrator.services.docker_provisioner import docker_provisioner

        return len(docker_provisioner.vm_hosts) > 0

    @property
    def mode(self) -> str:
        """Return ``off``, ``same-cluster``, or ``external`` from ``VM_MODE``."""
        global _warned_invalid_vm_mode, _warned_unset_vm_mode

        raw_mode = os.environ.get("VM_MODE")
        if raw_mode is None:
            if not _warned_unset_vm_mode:
                logger.warning("VM_MODE is unset; VM provisioning is disabled")
                _warned_unset_vm_mode = True
            return "off"
        mode = raw_mode.strip().lower()
        if mode not in _VALID_VM_MODES:
            if not _warned_invalid_vm_mode:
                logger.warning(
                    "Invalid VM_MODE=%r; VM provisioning is disabled", raw_mode
                )
                _warned_invalid_vm_mode = True
            return "off"
        return mode

    async def _current_provision_generation(
        self, entity_type: str, entity_id: str
    ) -> str | None:
        """Read the durable generation used to fence lifecycle commands."""

        if not self._db:
            return None
        try:
            if entity_type == "thread":
                row = await self._db.get_thread(entity_id)
                metadata = row.get("metadata") if isinstance(row, Mapping) else None
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                context = metadata.get("vm") if isinstance(metadata, Mapping) else None
            else:
                row = await self._db.get_job(entity_id)
                context = _extract_vm_context(row) if isinstance(row, dict) else None
            if not isinstance(context, Mapping):
                return None
            return _provision_generation(context.get("provision_generation"))
        except Exception:
            logger.exception(
                "Could not read current VM provision generation for %s %s",
                entity_type,
                entity_id,
            )
            return None

    async def _set_context_if_generation(
        self,
        entity_type: str,
        entity_id: str,
        generation: str,
        updates: dict,
        *,
        require_status_not_ready: bool = False,
    ) -> bool:
        if entity_type == "thread":
            return await self._set_thread_vm_context_if_generation(
                entity_id,
                generation,
                updates,
                require_status_not_ready=require_status_not_ready,
            )
        return await self._set_vm_context_if_generation(
            entity_id,
            generation,
            updates,
            require_status_not_ready=require_status_not_ready,
        )

    async def _persist_status_identity(
        self,
        entity_type: str,
        entity_id: str,
        data: Mapping[str, Any],
        *,
        phase_token=None,
    ) -> bool:
        """Persist query-discovered identities only from authenticated evidence."""

        if data.get("_identity_authenticated") is not True:
            return False
        generation = _provision_generation(data.get("provision_generation"))
        if generation is None:
            return False
        updates: dict[str, Any] = {}
        for key in ("vm_name", "namespace"):
            if isinstance(data.get(key), str):
                updates[key] = data[key]
        vm_uid = _safe_vm_uid(data.get("vm_uid"))
        if vm_uid is not None:
            updates.update(
                {
                    "vm_uid": vm_uid,
                    "identity_authenticated": True,
                    "identity_provision_generation": generation,
                }
            )
        if (root_uid := _safe_vm_uid(data.get("rootdisk_pvc_uid"))) is not None:
            updates["rootdisk_pvc_uid"] = root_uid
        if (
            fingerprint := _safe_ssh_host_key_fingerprint(
                data.get("ssh_host_key_fingerprint")
            )
        ) is not None:
            updates["ssh_host_key_fingerprint"] = fingerprint
        if type(data.get("credential_runtime_started")) is bool:
            updates["credential_runtime_started"] = data["credential_runtime_started"]
        if entity_type == "job" and self._phase_store is not None:
            if phase_token is None:
                return False
            disposition = await self._phase_store.apply_status(
                phase_token,
                data,
                identity_updates=updates,
            )
            merged = disposition in {"observed", "unproven"}
        else:
            merged = await self._set_context_if_generation(
                entity_type,
                entity_id,
                generation,
                updates,
            )

        if merged and entity_type == "job" and root_uid is not None:
            binding = await self._storage_context(entity_id)
            if binding is not None:
                from orchestrator.services.retained_vm_workspaces import record_created

                await record_created(
                    self._db,
                    entity_id,
                    binding,
                    root_uid,
                    namespace=data.get("namespace"),
                )
        return merged

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def connect(
        self,
        db: Any,
        snapshot_service: Optional[Any] = None,
    ) -> None:
        """Initialize the provisioner.

        Args:
            db: PostgresDB instance for job context updates.
            snapshot_service: Optional SnapshotService for archival before deletion.
        """
        self._db = db
        self._snapshot_service = snapshot_service
        if getattr(db, "supports_vm_phase_observations", False) is True:
            from orchestrator.services.vm_provisioning_phases import (
                VMProvisioningPhaseStore,
            )

            self._phase_store = VMProvisioningPhaseStore(db)

        if self._http_available:
            self._http_client = httpx.AsyncClient(
                base_url=self._controller_url,
                timeout=self._http_timeout,
            )

        mode = self.mode
        logger.info("VM provisioner configured with VM_MODE=%s", mode)
        if mode == "external":
            logger.warning("VM provisioner unavailable: %s", self.unavailable_reason)
        elif self._http_available:
            logger.info(
                "VM provisioner ready: same-cluster HTTP mode (controller=%s)",
                self._controller_url,
            )
        else:
            logger.info("VM provisioner unavailable for VM_MODE=%s", mode)

    async def disconnect(self) -> None:
        """Close the HTTP client (if any)."""
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:
                logger.debug("Error closing VM controller HTTP client", exc_info=True)
            self._http_client = None

    async def _set_vm_context_if_generation(
        self,
        job_id: str,
        generation: str,
        updates: dict,
        *,
        require_status_not_ready: bool = False,
    ) -> bool:
        if not self._db:
            return False
        try:
            if not require_status_not_ready:
                return bool(
                    await self._db.merge_vm_context_if_provision_generation(
                        job_id, generation, updates
                    )
                )
            return bool(
                await self._db.merge_vm_context_if_provision_generation(
                    job_id,
                    generation,
                    updates,
                    require_status_not_ready=require_status_not_ready,
                )
            )
        except Exception:
            logger.exception(
                "Failed to update generation-guarded VM context for job %s", job_id
            )
            return False

    # =========================================================================
    # Public API
    # =========================================================================

    async def attest_workspace_runtime(
        self,
        job_id: str,
        *,
        entity_type: str = "job",
    ) -> WorkspaceRuntimeAttestation:
        """Attest one VM endpoint and exact live incarnation.

        The lifecycle controller is the Kubernetes authority here instead of a
        second direct custom-object client in the orchestrator.  Its signed
        status operation freshly reads the VM and VMI (including
        ``activePods``), while the durable row supplies the server-owned SSH
        endpoint and admitted host-key pin.  A second durable reread after the
        controller observation closes the ordinary stale-read window before a
        caller uses this attestation for remote mutation.
        """

        if (
            not (self._http_available or self._nats_available)
            or self._lifecycle_hmac_secret is None
            or self._db is None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "VM Kubernetes authority is unavailable"
            )

        if entity_type not in {"job", "thread"}:
            raise WorkspaceRuntimeAuthorityError("VM workspace owner type is invalid")

        try:
            row = (
                await self._db.get_thread(job_id)
                if entity_type == "thread"
                else await self._db.get_job(job_id)
            )
        except Exception as exc:
            raise WorkspaceRuntimeAuthorityError(
                "VM workspace context probe failed"
            ) from exc
        if not isinstance(row, dict):
            raise WorkspaceRuntimeAuthorityError("VM workspace owner is unavailable")
        context = (
            _extract_thread_vm_context(row)
            if entity_type == "thread"
            else _extract_vm_context(row)
        )

        generation = _provision_generation(context.get("provision_generation"))
        if generation is None:
            raise WorkspaceRuntimeAuthorityError("VM provision generation is malformed")
        if (
            context.get("identity_authenticated") is not True
            or _provision_generation(context.get("identity_provision_generation"))
            != generation
        ):
            raise WorkspaceRuntimeAuthorityError(
                "VM workspace identity is unauthenticated"
            )
        expected_vm_uid = _safe_vm_uid(context.get("vm_uid"))
        if expected_vm_uid is None:
            raise WorkspaceRuntimeAuthorityError("VM UID is unavailable")
        expected_launcher_uid = _provision_generation(context.get("active_pod_uid"))
        if expected_launcher_uid is None:
            raise WorkspaceRuntimeAuthorityError("VM launcher Pod UID is malformed")
        expected_registration_id = context.get("ssh_registration_id")
        if (
            not isinstance(expected_registration_id, str)
            or not expected_registration_id
            or expected_registration_id != expected_registration_id.strip()
            or len(expected_registration_id) > 128
        ):
            raise WorkspaceRuntimeAuthorityError(
                "VM SSH registration identity is unavailable"
            )
        fingerprint = _safe_ssh_host_key_fingerprint(
            context.get("ssh_host_key_fingerprint")
        )
        if fingerprint is None:
            raise WorkspaceRuntimeAuthorityError(
                "VM SSH host-key fingerprint is malformed"
            )

        # Do not use query_status(): it intentionally strips the authenticated
        # response marker and persists selected telemetry. Mutation attestation
        # is read-only and must see the transport's proof directly.
        if self._http_available:
            observed = await self._query_http(
                job_id,
                provision_generation=generation,
            )
        else:
            observed = await nats_bridge.query_vm_status(
                job_id,
                provision_generation=generation,
            )
        if not isinstance(observed, Mapping) or (
            observed.get("_identity_authenticated") is not True
        ):
            raise WorkspaceRuntimeAuthorityError(
                "VM controller status is unauthenticated"
            )
        observed_generation = _provision_generation(
            observed.get("provision_generation")
        )
        if observed_generation is not None and observed_generation != generation:
            raise WorkspaceRuntimeRecoveryRequired(
                "VM provision generation changed",
                recovery_code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
            )
        if observed_generation != generation:
            raise WorkspaceRuntimeAuthorityError("VM provision generation changed")
        observed_vm_uid = _safe_vm_uid(observed.get("vm_uid"))
        if observed_vm_uid is not None and observed_vm_uid != expected_vm_uid:
            raise WorkspaceRuntimeRecoveryRequired(
                "VM UID changed",
                recovery_code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
            )
        if observed_vm_uid != expected_vm_uid:
            raise WorkspaceRuntimeAuthorityError("VM UID changed")
        if observed.get("ready") is False:
            raise WorkspaceRuntimeRecoveryRequired(
                "VM is not Kubernetes-ready",
                recovery_code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            )
        if observed.get("ready") is not True:
            raise WorkspaceRuntimeAuthorityError("VM is not Kubernetes-ready")

        launcher_uid = _provision_generation(observed.get("active_pod_uid"))
        if launcher_uid is not None and launcher_uid != expected_launcher_uid:
            raise WorkspaceRuntimeRecoveryRequired(
                "VM launcher Pod UID changed",
                recovery_code=WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
            )
        if launcher_uid != expected_launcher_uid:
            raise WorkspaceRuntimeAuthorityError("VM launcher Pod UID changed")

        observed_pod_ip = observed.get("pod_ip")
        if (
            not isinstance(observed_pod_ip, str)
            or not observed_pod_ip
            or observed_pod_ip != observed_pod_ip.strip()
        ):
            raise WorkspaceRuntimeAuthorityError("VM pod IP is unavailable")
        try:
            canonical_pod_ip = str(ipaddress.ip_address(observed_pod_ip))
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError("VM pod IP is malformed") from exc
        if canonical_pod_ip != observed_pod_ip:
            raise WorkspaceRuntimeAuthorityError("VM pod IP is malformed")

        raw_host = context.get("ssh_host")
        if self._http_available:
            host = observed_pod_ip
            if raw_host != host or context.get("pod_ip") not in {None, host}:
                raise WorkspaceRuntimeAuthorityError("VM SSH endpoint changed")
        else:
            # External mode reaches the guest through its generation-CASed
            # daemon endpoint, not the launcher Pod IP. The exact admitted host
            # key still cryptographically binds that endpoint to this VM.
            host = raw_host
        if (
            not isinstance(host, str)
            or not host
            or host != host.strip()
            or len(host) > 512
            or any(character.isspace() for character in host)
        ):
            raise WorkspaceRuntimeAuthorityError("VM SSH endpoint is unavailable")
        raw_port = context.get("ssh_port")
        if (
            isinstance(raw_port, bool)
            or not isinstance(raw_port, (int, str))
            or not str(raw_port).isdigit()
            or not 1 <= int(raw_port) <= 65535
        ):
            raise WorkspaceRuntimeAuthorityError("VM SSH port is unavailable")
        port = int(raw_port)

        # The controller read above is external I/O. Re-read the durable owner
        # immediately afterwards and require every authority-bearing field to
        # remain exact before returning a mutation target.
        try:
            current_row = (
                await self._db.get_thread(job_id)
                if entity_type == "thread"
                else await self._db.get_job(job_id)
            )
        except Exception as exc:
            raise WorkspaceRuntimeAuthorityError(
                "VM workspace authority revalidation failed"
            ) from exc
        if not isinstance(current_row, dict):
            raise WorkspaceRuntimeAuthorityError("VM workspace owner changed")
        current = (
            _extract_thread_vm_context(current_row)
            if entity_type == "thread"
            else _extract_vm_context(current_row)
        )
        if (
            _provision_generation(current.get("provision_generation")) != generation
            or current.get("identity_authenticated") is not True
            or _provision_generation(current.get("identity_provision_generation"))
            != generation
            or _safe_vm_uid(current.get("vm_uid")) != expected_vm_uid
            or _provision_generation(current.get("active_pod_uid")) != launcher_uid
            or current.get("ssh_registration_id") != expected_registration_id
            or _safe_ssh_host_key_fingerprint(current.get("ssh_host_key_fingerprint"))
            != fingerprint
            or current.get("ssh_host") != host
            or int(current.get("ssh_port") or 0) != port
        ):
            raise WorkspaceRuntimeAuthorityError("VM workspace authority changed")

        return WorkspaceRuntimeAttestation(
            backing_id=f"k8s-vmi:{launcher_uid}",
            workspace_generation=generation,
            runtime_incarnation=launcher_uid,
            ssh_host_key_fingerprint=fingerprint,
            host=host,
            pod_ip=observed_pod_ip,
            port=port,
            vm_uid=expected_vm_uid,
            launcher_pod_uid=launcher_uid,
        )

    async def _storage_context(self, job_id):
        if self._db is None or not callable(getattr(self._db, "get_job", None)):
            return None
        job = await self._db.get_job(job_id)
        if not isinstance(job, Mapping):
            return None
        value = _extract_vm_context(job).get("workspace_storage")
        if value is None:
            return None
        from shared.vm_workspace_storage import storage_binding

        binding = storage_binding(value)
        # Context carries transport data, not the right to select a disk. Prove
        # this Job's durable reservation before signing any storage reference.
        row = await self._db.fetchrow(
            """SELECT i.generation,i.pvc_uid,i.backend_state FROM srw_execution_specs s
            JOIN srw_execution_workspace_bindings b ON b.execution_id=s.id
            JOIN srw_workspace_instances i ON i.id=b.instance_id
            WHERE s.work_kind='Job' AND s.work_id=$1 AND i.id=$2 AND i.recipe->>'backend'='vm'""",
            UUID(str(job_id)),
            UUID(binding["uid"]),
        )
        from orchestrator.services.retained_vm_workspaces import object_value

        recorded = object_value(row["backend_state"]).get("storage", {}) if row else {}
        if (
            not row
            or binding["generation"] > row["generation"]
            or any(
                binding[key] != recorded.get(key)
                for key in ("uid", "owner_id", "owner_kind")
            )
        ):
            raise ValueError(
                "VM context lacks retained workspace reservation authority."
            )
        # The authenticated response pins the PVC after its first allocation.
        uid = _extract_vm_context(job).get("rootdisk_pvc_uid")
        if uid:
            binding["pvc_uid"] = uid
        if (
            row["pvc_uid"]
            and binding["pvc_uid"]
            and row["pvc_uid"] != binding["pvc_uid"]
        ):
            raise ValueError(
                "Retained VM context has a different captured PVC identity."
            )
        binding["pvc_uid"] = row["pvc_uid"] or binding["pvc_uid"]
        return storage_binding(binding)

    async def release_workspace_storage(self, binding):
        return await self._workspace_storage_action(binding, "release-workspace")

    async def _workspace_storage_action(self, binding, operation):
        from shared.vm_workspace_storage import storage_binding

        if not self._http_available or self._lifecycle_hmac_secret is None:
            return False
        payload = sign_payload(
            {"workspace_storage": storage_binding(binding)},
            direction="request",
            operation=operation,
            secret=self._lifecycle_hmac_secret,
        )
        response = await self._http_client.post(
            "/workspace-disks/"
            + ("release" if operation == "release-workspace" else "detach"),
            json=payload,
        )
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation=operation,
            secret=self._lifecycle_hmac_secret,
            expected_correlation_id=payload[AUTH_FIELD]["request_id"],
        ):
            return False
        response.raise_for_status()
        return data.get("deleted") is True

    async def _record_retained_detach(self, job_id, binding):
        if binding is not None:
            job = await self._db.get_job(job_id)
            if not job or job["status"] not in {"completed", "failed", "cancelled"}:
                return
            pending = await self._db.fetchval(
                "SELECT EXISTS(SELECT 1 FROM srw_workspace_instances i JOIN srw_execution_specs s ON s.id=i.execution_id WHERE i.id=$1 AND i.generation=$2 AND i.status IN ('Reserved','Attached') AND s.work_kind='Job' AND s.work_id=$3)",
                UUID(binding["uid"]),
                binding["generation"],
                UUID(str(job_id)),
            )
            if not pending or not await self._workspace_storage_action(
                binding, "detach-workspace"
            ):
                return
            from orchestrator.services.retained_vm_workspaces import record_detached

            await record_detached(self._db, job_id, binding)

    async def _validate_preparation(self, entity_id, entity_type, preparation):
        from shared.workspace_preparation import validate_request
        from orchestrator.services.vm_workspace_config import vm_provisioning_options

        preparation = validate_request(preparation)
        if (
            self.mode != "same-cluster"
            or self._lifecycle_hmac_secret is None
            or self._db is None
        ):
            raise ValueError(
                "Workspace preparation requires authenticated same-cluster hosting."
            )
        work = await (
            self._db.get_job(entity_id)
            if entity_type == "job"
            else self._db.get_thread(entity_id)
        )
        if not isinstance(work, Mapping):
            raise ValueError("Preparation execution is unavailable.")
        if entity_type == "job" and work.get("status") not in {
            "created",
            "processing",
            "paused",
        }:
            raise ValueError("Terminal work cannot start workspace preparation.")
        options = await vm_provisioning_options(
            self._db, "Job" if entity_type == "job" else "Session", work
        )
        if options.get("preparation") != preparation:
            raise ValueError("Preparation does not match the admitted execution.")
        return preparation

    async def preparation_operation(self, action, values):
        if (
            action not in {"list", "delete", "cancel", "prepare"}
            or not self._http_available
            or self._lifecycle_hmac_secret is None
        ):
            raise ValueError("Authenticated VM preparation hosting is unavailable.")
        operation = "preparation-" + action
        payload = sign_payload(
            values,
            direction="request",
            operation=operation,
            secret=self._lifecycle_hmac_secret,
        )
        response = await self._http_client.post(
            "/workspace-preparations/" + action, json=payload
        )
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation=operation,
            secret=self._lifecycle_hmac_secret,
            expected_correlation_id=payload[AUTH_FIELD]["request_id"],
        ):
            raise ValueError("Preparation response authentication failed.")
        response.raise_for_status()
        return unsigned_payload(data)

    async def create_vm(
        self,
        job_id: str,
        agent_config: str = "worker_base",
        vm_image: Optional[str] = None,
        cpu_cores: int = 8,
        memory: str = "16Gi",
        description: str = "",
        fresh: bool = True,
        disk_size: Optional[str] = None,
        initialization: dict | None = None,
        workspace_storage: dict | None = None,
        preparation: dict | None = None,
    ) -> bool | dict[str, Any]:
        """Create a VM for a job.

        Uses the transport selected by ``VM_MODE``. The compose-only Docker
        fallback remains available for its existing local development path.

        Args:
            fresh: True (default) for a genuine (re)provision. False for a
                deferred-create poll re-issue — the controller answered
                ``waiting_golden`` (a shared golden image is importing) or
                ``waiting_capacity`` (the cluster VM cap is full) or
                ``waiting_headscale`` (the mesh VPN is down, so a VM would be
                unreachable), no VM exists yet, and the dispatcher re-sends
                create as the poll. A poll must NOT reset the provision
                context: ``golden_wait_started_at`` /
                ``headscale_wait_started_at`` anchor those budgets across
                polls, and the context status must stay ``waiting_*`` (not
                flip to 'provisioning') so the decision logic keeps polling
                instead of burning boot-budget waits.
                ``provisioned_at`` alone is rolled forward so that when the
                golden completes and the create finally builds the VM, the boot
                budget starts from ≈ that moment, not from the first poll.

        Returns:
            The controller response when HTTP accepted the request, otherwise
            the transport's boolean acknowledgement.
        """
        if preparation is not None:
            preparation = await self._validate_preparation(job_id, "job", preparation)
        if workspace_storage is not None:
            from orchestrator.services.retained_vm_workspaces import provision_binding
            from shared.vm_workspace_storage import storage_binding

            workspace_storage = storage_binding(workspace_storage)
            if self.mode != "same-cluster" or self._lifecycle_hmac_secret is None:
                raise ValueError(
                    "Retained workspaces require authenticated same-cluster VM hosting."
                )
            if await provision_binding(self._db, job_id) != workspace_storage:
                raise ValueError("Retained workspace attachment authority changed.")
        if initialization is not None:
            from shared.workspace_initialization import validate_initialization_request

            initialization = validate_initialization_request(initialization)
            if self.mode != "same-cluster":
                raise ValueError(
                    "Workspace initialization requires same-cluster VM hosting."
                )
        if self.mode == "external":
            # Refuse before generating/storing a provision generation.  A
            # false availability probe followed by a direct create call must
            # not leave a durable row pretending an unusable VM is pending.
            logger.warning("VM create refused: %s", self.unavailable_reason)
            return False
        # A (re)provisioned VM must start with a CLEAN reap counter and no stale
        # SSH endpoint. context.vm is *merged* (not replaced) across provisions,
        # so a prior incarnation's snapshot_attempts — which reaches the reaper's
        # max and makes attempts_exhausted instantly true, force-deleting the new
        # VM on its first tick — and its dead ssh_host would otherwise leak into
        # this fresh VM. provisioned_at anchors the dispatcher's provisioning
        # timeout. Runs before backend dispatch so every transport inherits it.
        if fresh:
            fresh_context = self._fresh_provision_ctx()
            fresh_context.update(
                initialization=initialization,
                workspace_storage=workspace_storage,
                preparation_request=preparation,
                preparation=None,
                preparation_wait_started_at=time.time()
                if preparation is not None
                else None,
                initialization_receipt=None,
                initialization_started_at=None,
            )
            generation = fresh_context["provision_generation"]
            await self._set_vm_context(job_id, fresh_context)
        else:
            await self._set_vm_context(job_id, {"provisioned_at": time.time()})
            generation = await self._current_provision_generation("job", job_id)
        if self._nats_available:
            return await nats_bridge.request_vm_create(
                job_id=job_id,
                agent_config=agent_config,
                vm_image=vm_image,
                cpu_cores=cpu_cores,
                memory=memory,
                description=description,
                entity_type="job",
                set_provisioning=fresh,
                provision_generation=generation,
                **({"disk_size": disk_size} if disk_size is not None else {}),
                **(
                    {"initialization": initialization}
                    if initialization is not None
                    else {}
                ),
            )

        if self._http_available:
            return await self._create_http(
                job_id=job_id,
                agent_config=agent_config,
                vm_image=vm_image,
                cpu_cores=cpu_cores,
                memory=memory,
                disk_size=disk_size,
                **(
                    {"workspace_storage": workspace_storage}
                    if workspace_storage is not None
                    else {}
                ),
                **({"preparation": preparation} if preparation is not None else {}),
                **(
                    {"initialization": initialization}
                    if initialization is not None
                    else {}
                ),
                description=description,
                entity_type="job",
                set_provisioning=fresh,
                provision_generation=generation,
            )

        # Docker Compose mode: assign from QEMU-in-Docker pool
        if self._docker_available:
            from orchestrator.services.docker_provisioner import docker_provisioner

            result = await docker_provisioner.assign_vm(job_id)
            return result is not None

        return False

    async def capture_vm_teardown_identity(
        self,
        job_id: str,
        *,
        entity_type: str = "job",
    ) -> VMTeardownIdentity:
        """Capture one authenticated VM generation/UID tuple for replay.

        The provision generation is mandatory on every backend.  Immutable VM
        and rootdisk UIDs are included only when the lifecycle controller has
        authenticated them for that exact generation; an unauthenticated guest
        report can never become teardown authority.
        """

        if not self._db:
            raise RuntimeError("VM teardown identity database is unavailable")
        if entity_type not in {"job", "thread"}:
            raise ValueError("VM teardown entity type is invalid")
        row = (
            await self._db.get_thread(job_id)
            if entity_type == "thread"
            else await self._db.get_job(job_id)
        )
        if not isinstance(row, dict):
            raise RuntimeError(f"VM teardown {entity_type} no longer exists")
        if entity_type == "thread":
            metadata = row.get("metadata") or {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            context = metadata.get("vm") if isinstance(metadata, Mapping) else None
            if not isinstance(context, Mapping):
                context = {}
        else:
            context = _extract_vm_context(row)
        generation = _provision_generation(context.get("provision_generation"))
        if generation is None:
            raise RuntimeError("VM teardown provision generation is unavailable")

        authenticated = (
            context.get("identity_authenticated") is True
            and _provision_generation(context.get("identity_provision_generation"))
            == generation
        )
        vm_uid = _safe_vm_uid(context.get("vm_uid")) if authenticated else None
        rootdisk_uid = (
            _safe_vm_uid(context.get("rootdisk_pvc_uid")) if authenticated else None
        )
        ssh_host = context.get("ssh_host")
        if (
            not isinstance(ssh_host, str)
            or not ssh_host
            or ssh_host != ssh_host.strip()
            or len(ssh_host) > 512
            or any(character.isspace() for character in ssh_host)
        ):
            ssh_host = None
        raw_ssh_port = context.get("ssh_port")
        ssh_port = (
            int(raw_ssh_port)
            if not isinstance(raw_ssh_port, bool)
            and isinstance(raw_ssh_port, (int, str))
            and str(raw_ssh_port).isdigit()
            and 1 <= int(raw_ssh_port) <= 65535
            else None
        )
        host_key_fingerprint = (
            _safe_ssh_host_key_fingerprint(context.get("ssh_host_key_fingerprint"))
            if authenticated
            else None
        )
        if vm_uid is None or rootdisk_uid is None:
            probe = await self._probe_vm_teardown_identity(job_id, generation)
            if probe.disposition == "unknown":
                raise RuntimeError("VM teardown identity probe is unavailable")
            if probe.disposition == "superseded":
                raise RuntimeError("VM teardown provision generation changed")
            if probe.identity is not None:
                probed_vm_uid = probe.identity.vm_uid
                probed_rootdisk_uid = probe.identity.rootdisk_pvc_uid
                if (
                    vm_uid is not None
                    and probed_vm_uid is not None
                    and vm_uid != probed_vm_uid
                ) or (
                    rootdisk_uid is not None
                    and probe.rootdisk_identity_known
                    and rootdisk_uid != probed_rootdisk_uid
                ):
                    raise RuntimeError("VM teardown immutable identity changed")
                if vm_uid is None:
                    vm_uid = probed_vm_uid
                if rootdisk_uid is None and probe.rootdisk_identity_known:
                    rootdisk_uid = probed_rootdisk_uid
            if probe.disposition == "present" and (
                not probe.rootdisk_identity_known or rootdisk_uid is None
            ):
                raise RuntimeError("VM teardown rootdisk identity is unavailable")
        return VMTeardownIdentity(
            provision_generation=generation,
            vm_uid=vm_uid,
            rootdisk_pvc_uid=rootdisk_uid,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_host_key_fingerprint=host_key_fingerprint,
        )

    async def _probe_vm_teardown_identity(
        self,
        job_id: str,
        generation: str,
    ) -> _VMTeardownProbe:
        """Probe the exact backend without collapsing absence into transport loss."""

        result: Mapping[str, Any] | None
        if self._nats_available:
            result = await nats_bridge.query_vm_status(
                job_id,
                provision_generation=generation,
                exact_absence=True,
            )
        elif self._http_available:
            result = await self._query_http(
                job_id,
                provision_generation=generation,
                exact_absence=True,
            )
        else:
            return _VMTeardownProbe("unknown")

        if not isinstance(result, Mapping):
            return _VMTeardownProbe("unknown")
        if result.get("_identity_authenticated") is not True:
            return _VMTeardownProbe("unknown")
        observed_generation = _provision_generation(result.get("provision_generation"))
        if observed_generation != generation:
            return _VMTeardownProbe("superseded")
        status = str(result.get("status") or "")
        rootdisk_known = result.get("rootdisk_identity_known") is True
        # A VM may boot after retirement started, before readiness ever stored
        # an endpoint. Keep this controller observation local to teardown: it
        # must not promote or publish a retiring runtime as ready.
        pod_ip = None
        if (
            self.mode == "same-cluster"
            and result.get("ready") is True
            and _safe_vm_uid(result.get("active_pod_uid")) is not None
        ):
            raw_ip = result.get("pod_ip")
            try:
                address = (
                    ipaddress.ip_address(raw_ip) if isinstance(raw_ip, str) else None
                )
            except ValueError:
                address = None
            if (
                address is not None
                and str(address) == raw_ip
                and not (
                    address.is_unspecified
                    or address.is_loopback
                    or address.is_link_local
                    or address.is_multicast
                    or "%" in raw_ip
                )
            ):
                pod_ip = raw_ip
        identity = VMTeardownIdentity(
            provision_generation=generation,
            vm_uid=_safe_vm_uid(result.get("vm_uid")),
            rootdisk_pvc_uid=_safe_vm_uid(result.get("rootdisk_pvc_uid")),
            ssh_host=pod_ip,
            ssh_port=22 if pod_ip is not None else None,
            credential_runtime_started=(
                result.get("credential_runtime_started")
                if type(result.get("credential_runtime_started")) is bool
                else None
            ),
        )
        if status == "not_found":
            return _VMTeardownProbe(
                "absent",
                identity,
                rootdisk_identity_known=rootdisk_known,
            )
        if status in {"query_failed", "delete_failed"} or identity.vm_uid is None:
            return _VMTeardownProbe("unknown")
        return _VMTeardownProbe(
            "present",
            identity,
            rootdisk_identity_known=rootdisk_known,
        )

    async def revalidate_vm_teardown_identity(
        self,
        job_id: str,
        identity: VMTeardownIdentity,
        *,
        entity_type: str = "job",
    ) -> str:
        """Re-prove an exact VM incarnation immediately before snapshot I/O."""

        generation = _provision_generation(identity.provision_generation)
        if generation is None or entity_type not in {"job", "thread"}:
            return "unknown"
        if await self._current_provision_generation(entity_type, job_id) != generation:
            return "superseded"
        probe = await self._probe_vm_teardown_identity(job_id, generation)
        classification = self._classify_captured_probe(probe, identity, purge_disk=True)
        if classification == "matched":
            return "matched"
        if classification in {"superseded", "completed"}:
            return "superseded"
        return "unknown"

    @staticmethod
    def _classify_captured_probe(
        probe: _VMTeardownProbe,
        identity: VMTeardownIdentity,
        *,
        purge_disk: bool = True,
    ) -> str:
        """Map one authenticated observation to completed/matched/superseded."""

        if probe.disposition in {"unknown", "superseded"}:
            return probe.disposition
        current = probe.identity
        if current is None:
            return "unknown"
        if probe.disposition == "present":
            if identity.vm_uid is None or current.vm_uid != identity.vm_uid:
                return "superseded"
            if purge_disk:
                if not probe.rootdisk_identity_known:
                    return "unknown"
                if identity.rootdisk_pvc_uid is None:
                    return "unknown"
                if current.rootdisk_pvc_uid != identity.rootdisk_pvc_uid:
                    return "superseded"
            return "matched"
        if not probe.rootdisk_identity_known:
            return "unknown"
        if current.rootdisk_pvc_uid is None:
            # VM and rootdisk both proven absent: a lost delete response is
            # exact idempotent success even when DB context is still stale.
            return "completed"
        if identity.rootdisk_pvc_uid is None:
            return "unknown"
        if current.rootdisk_pvc_uid != identity.rootdisk_pvc_uid:
            return "superseded"
        return "matched" if purge_disk else "completed"

    async def delete_vm_captured(
        self,
        job_id: str,
        identity: VMTeardownIdentity,
        *,
        purge_disk: bool = True,
        entity_type: str = "job",
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> VMTeardownResult:
        """Delete only the VM/rootdisk incarnation captured in an intent."""

        binding = await self._storage_context(job_id) if entity_type == "job" else None
        if binding is not None:
            purge_disk = False
        generation = _provision_generation(identity.provision_generation)
        if generation is None:
            return VMTeardownResult("identity_invalid", False)
        if entity_type not in {"job", "thread"}:
            return VMTeardownResult("identity_invalid", False)
        current_generation = await self._current_provision_generation(
            entity_type, job_id
        )
        if current_generation != generation:
            return VMTeardownResult("identity_superseded", False)
        if (
            not self._db
            or not await self._db.managed_repository_workspace_process_zero_is_current(
                job_id,
                owner_kind=entity_type,
                scope="vm",
                provisioner="vm",
                runtime_incarnation=generation,
            )
        ):
            return VMTeardownResult("process_zero_unproven", False)
        probe = await self._probe_vm_teardown_identity(job_id, generation)
        classification = self._classify_captured_probe(
            probe, identity, purge_disk=purge_disk
        )
        if classification == "superseded":
            return VMTeardownResult("identity_superseded", False)
        if classification == "completed":
            await self._record_retained_detach(job_id, binding)
            return VMTeardownResult("completed", True)
        if classification != "matched":
            return VMTeardownResult("identity_unknown", False)
        await self._delete_vm_with_identity(
            job_id,
            purge_disk=purge_disk,
            provision_generation=generation,
            expected_vm_uid=_safe_vm_uid(identity.vm_uid),
            expected_rootdisk_pvc_uid=_safe_vm_uid(identity.rootdisk_pvc_uid),
            entity_type=entity_type,
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )
        reprobe = await self._probe_vm_teardown_identity(job_id, generation)
        reclassification = self._classify_captured_probe(
            reprobe, identity, purge_disk=purge_disk
        )
        if reclassification == "completed":
            await self._record_retained_detach(job_id, binding)
            return VMTeardownResult("completed", True)
        if reclassification == "superseded":
            return VMTeardownResult("identity_superseded", False)
        return VMTeardownResult("retry_pending", False)

    async def _terminal_snapshot_already_captured(
        self,
        job_id: str,
        *,
        entity_type: str = "job",
    ) -> bool:
        """True when this VM incarnation's terminal snapshot is already in S3.

        A captured teardown is retried whenever the controller has not yet
        confirmed the exact incarnation gone; re-capturing on every retry
        SSHes into a VM that is already shutting down. The snapshot from the
        first attempt is keyed to this incarnation when it was created after
        the VM was provisioned, so reuse it instead.
        """
        if not self._db:
            return False
        try:
            row = (
                await self._db.get_thread(job_id)
                if entity_type == "thread"
                else await self._db.get_job(job_id)
            )
            if not isinstance(row, dict):
                return False
            ctx = (
                row.get("metadata") or {}
                if entity_type == "thread"
                else row.get("context") or {}
            )
            if isinstance(ctx, str):
                ctx = json.loads(ctx)
            snapshot = ctx.get("snapshot") or {}
            vm_ctx = (
                ctx.get("vm") or {}
                if entity_type == "thread" and isinstance(ctx, Mapping)
                else _extract_vm_context(row)
            )
            if (
                snapshot.get("status") != "available"
                or snapshot.get("source_type") != "vm"
                or snapshot.get("phase_number") is not None
            ):
                return False
            created_at = datetime.fromisoformat(str(snapshot.get("created_at")))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            provisioned_at = float(vm_ctx.get("provisioned_at") or 0)
            reusable = created_at.timestamp() >= provisioned_at
            if reusable:
                logger.info(
                    "Reusing the terminal snapshot already captured for job %s "
                    "(created %s); not re-capturing on teardown retry",
                    job_id,
                    snapshot.get("created_at"),
                )
            return reusable
        except Exception:
            logger.debug(
                "Could not evaluate the existing snapshot for job %s",
                job_id,
                exc_info=True,
            )
            return False

    async def _retire_unallocated_preparation(
        self, entity_id, entity_type, identity, probe
    ) -> bool:
        """Record zero only for a fenced preparation that never issued a disk."""
        from shared.workspace_preparation import validate_request

        generation = identity.provision_generation

        def fully_absent(observed):
            return bool(
                observed.disposition == "absent"
                and observed.rootdisk_identity_known
                and observed.identity is not None
                and observed.identity.provision_generation == generation
                and observed.identity.vm_uid is None
                and observed.identity.rootdisk_pvc_uid is None
            )

        if (
            not self._db
            or identity.vm_uid is not None
            or identity.rootdisk_pvc_uid is not None
            or not fully_absent(probe)
        ):
            return False

        async def current_request():
            row = (
                await self._db.get_job(entity_id)
                if entity_type == "job"
                else await self._db.get_thread(entity_id)
            )
            terminal = (
                {"completed", "failed", "cancelled"}
                if entity_type == "job"
                else {"ended"}
            )
            if not row or row.get("status") not in terminal:
                return None
            state = row.get("context" if entity_type == "job" else "metadata") or {}
            if isinstance(state, str):
                state = json.loads(state)
            vm = state.get("vm") or {}
            if (
                vm.get("provision_generation") != generation
                or vm.get("identity_authenticated") is not False
                or any(
                    vm.get(key) is not None
                    for key in (
                        "vm_uid",
                        "rootdisk_pvc_uid",
                        "_runtime_incarnation",
                        "identity_provision_generation",
                        "ssh_host",
                        "ssh_port",
                        "ssh_host_key_fingerprint",
                        "ssh_registration_id",
                        "initialization_receipt",
                    )
                )
            ):
                return None
            try:
                request = validate_request(vm.get("preparation_request"))
            except (ValueError, TypeError, KeyError):
                return None
            if request["allocationId"] != entity_id or request["ownerKind"] != (
                "job" if entity_type == "job" else "session"
            ):
                return None
            return request

        request = await current_request()
        if (
            request is None
            or not await self._db.claim_managed_repository_workspace_retirement(
                entity_id,
                owner_kind=entity_type,
                scope="vm",
                provisioner="vm",
                runtime_incarnation=generation,
            )
        ):
            return False
        result = await self.preparation_operation("cancel", {"preparation": request})
        if (
            result.get("cancelled") is not True
            or result.get("workspaceNeverIssued") is not True
        ):
            return False
        if await current_request() != request or not fully_absent(
            await self._probe_vm_teardown_identity(entity_id, generation)
        ):
            return False
        if not await self._db.record_managed_repository_workspace_process_zero(
            entity_id,
            owner_kind=entity_type,
            scope="vm",
            provisioner="vm",
            runtime_incarnation=generation,
        ):
            return False
        return await self._set_context_if_generation(
            entity_type,
            entity_id,
            generation,
            {
                "status": "deleted",
                "preparation_cancelled_revision": request["revision"],
            },
        )

    async def release_vm_captured(
        self,
        job_id: str,
        identity: VMTeardownIdentity,
        *,
        ssh_host: str | None = None,
        ssh_port: int | None = None,
        entity_type: str = "job",
        purge_disk: bool = True,
        capture_snapshot: bool = True,
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> VMTeardownResult:
        """Best-effort archive, then release only the captured VM incarnation."""

        binding = await self._storage_context(job_id) if entity_type == "job" else None
        if binding is not None:
            purge_disk = False
        generation = _provision_generation(identity.provision_generation)
        if generation is None:
            return VMTeardownResult("identity_invalid", False)
        if entity_type not in {"job", "thread"}:
            return VMTeardownResult("identity_invalid", False)
        if await self._current_provision_generation(entity_type, job_id) != generation:
            return VMTeardownResult("identity_superseded", False)
        if (
            ssh_host is not None
            and identity.ssh_host is not None
            and (ssh_host != identity.ssh_host)
        ):
            return VMTeardownResult("identity_invalid", False)
        if (
            ssh_port is not None
            and identity.ssh_port is not None
            and (int(ssh_port) != identity.ssh_port)
        ):
            return VMTeardownResult("identity_invalid", False)
        effective_ssh_host = identity.ssh_host or ssh_host
        effective_ssh_port = identity.ssh_port or ssh_port
        probe = await self._probe_vm_teardown_identity(job_id, generation)
        classification = self._classify_captured_probe(
            probe,
            identity,
            purge_disk=purge_disk,
        )
        if classification == "superseded":
            return VMTeardownResult("identity_superseded", False)
        if classification == "completed":
            contained = bool(
                self._db
                and await self._db.managed_repository_workspace_process_zero_is_current(
                    job_id,
                    owner_kind=entity_type,
                    scope="vm",
                    provisioner="vm",
                    runtime_incarnation=generation,
                )
            )
            if not contained:
                contained = await self._retire_unallocated_preparation(
                    job_id, entity_type, identity, probe
                )
            if contained:
                await self._record_retained_detach(job_id, binding)
            return VMTeardownResult(
                "completed" if contained else "process_zero_unproven",
                contained,
            )
        if classification != "matched":
            return VMTeardownResult("identity_unknown", False)

        if (
            self._db
            and await self._db.managed_repository_workspace_process_zero_is_current(
                job_id,
                owner_kind=entity_type,
                scope="vm",
                provisioner="vm",
                runtime_incarnation=generation,
            )
        ):
            return await self.delete_vm_captured(
                job_id,
                identity,
                purge_disk=purge_disk,
                entity_type=entity_type,
                **(
                    {"parent_cleanup": parent_cleanup}
                    if parent_cleanup is not None
                    else {}
                ),
            )

        if (
            self._snapshot_service
            and self._snapshot_service.is_available
            and capture_snapshot
            and effective_ssh_host
            and effective_ssh_port
            and not await self._terminal_snapshot_already_captured(
                job_id,
                entity_type=entity_type,
            )
        ):
            try:
                if (
                    not identity.ssh_host_key_fingerprint
                    or await self.revalidate_vm_teardown_identity(
                        job_id,
                        identity,
                        entity_type=entity_type,
                    )
                    != "matched"
                ):
                    return VMTeardownResult("identity_unknown", False)

                async def capture_authority() -> bool:
                    return (
                        await self.revalidate_vm_teardown_identity(
                            job_id,
                            identity,
                            entity_type=entity_type,
                        )
                        == "matched"
                    )

                captured = await self._snapshot_service.capture_vm_snapshot(
                    job_id=job_id,
                    ssh_host=effective_ssh_host,
                    ssh_port=int(effective_ssh_port),
                    source_type="vm",
                    expected_host_key_fingerprint=(identity.ssh_host_key_fingerprint),
                    capture_authority=capture_authority,
                    **({"entity_type": "threads"} if entity_type == "thread" else {}),
                )
                if captured:
                    logger.info(
                        "VM snapshot captured for %s %s before exact release",
                        entity_type,
                        job_id,
                    )
                else:
                    logger.warning(
                        "Captured VM snapshot skipped for job %s; deleting exact "
                        "incarnation under terminal teardown policy",
                        job_id,
                    )
            except Exception:
                logger.exception(
                    "Captured VM snapshot failed for job %s; deleting exact "
                    "incarnation under terminal teardown policy",
                    job_id,
                )
            if (
                await self.revalidate_vm_teardown_identity(
                    job_id,
                    identity,
                    entity_type=entity_type,
                )
                != "matched"
            ):
                return VMTeardownResult("identity_superseded", False)

        if (
            self._db is None
            or not await self._db.claim_managed_repository_workspace_retirement(
                job_id,
                owner_kind=entity_type,
                scope="vm",
                provisioner="vm",
                runtime_incarnation=generation,
            )
        ):
            return VMTeardownResult("process_zero_unproven", False)
        current_identity = probe.identity
        never_started = bool(
            current_identity is not None
            and current_identity.credential_runtime_started is False
        )
        discovered_endpoint = False
        if not never_started:
            if (
                not effective_ssh_host
                and not effective_ssh_port
                and self.mode == "same-cluster"
                and current_identity is not None
                and self._classify_captured_probe(probe, identity, purge_disk=True)
                == "matched"
            ):
                effective_ssh_host = current_identity.ssh_host
                effective_ssh_port = current_identity.ssh_port
                discovered_endpoint = bool(effective_ssh_host and effective_ssh_port)
            if (
                not effective_ssh_host
                or not effective_ssh_port
                or not identity.ssh_host_key_fingerprint
            ):
                return VMTeardownResult("process_zero_unproven", False)
            # Never learn a new host key from the candidate endpoint. The SSH
            # actuator authenticates the captured, controller-admitted pin.
            retired = await retire_managed_repository_processes(
                host=effective_ssh_host,
                port=int(effective_ssh_port),
                host_key_fingerprint=identity.ssh_host_key_fingerprint,
                operation="VM managed repository process retirement",
            )
            if not retired:
                return VMTeardownResult("process_zero_unproven", False)
        reprobe = await self._probe_vm_teardown_identity(job_id, generation)
        if (
            self._classify_captured_probe(
                reprobe,
                identity,
                purge_disk=purge_disk or discovered_endpoint,
            )
            != "matched"
        ):
            return VMTeardownResult("process_zero_unproven", False)
        if (
            not self._db
            or not await self._db.record_managed_repository_workspace_process_zero(
                job_id,
                owner_kind=entity_type,
                scope="vm",
                provisioner="vm",
                runtime_incarnation=generation,
            )
        ):
            return VMTeardownResult("process_zero_unproven", False)
        return await self.delete_vm_captured(
            job_id,
            identity,
            purge_disk=purge_disk,
            entity_type=entity_type,
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )

    async def delete_orphan_vm_captured(
        self,
        job_id: str,
        identity: VMTeardownIdentity,
        *,
        purge_disk: bool = True,
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> VMTeardownResult:
        """Delete an inventory-proven VM after both owning rows are absent."""

        generation = _provision_generation(identity.provision_generation)
        vm_uid = _safe_vm_uid(identity.vm_uid)
        rootdisk_uid = _safe_vm_uid(identity.rootdisk_pvc_uid)
        if (
            generation is None
            or vm_uid is None
            or (purge_disk and rootdisk_uid is None)
        ):
            return VMTeardownResult("identity_unknown", False)
        probe = await self._probe_vm_teardown_identity(job_id, generation)
        classification = self._classify_captured_probe(
            probe, identity, purge_disk=purge_disk
        )
        if classification == "completed":
            return VMTeardownResult("completed", True)
        if classification == "superseded":
            return VMTeardownResult("identity_superseded", False)
        if classification != "matched":
            return VMTeardownResult("identity_unknown", False)
        current_identity = probe.identity
        if (
            current_identity is None
            or current_identity.credential_runtime_started is not False
        ):
            # With no owning row there is no endpoint/grant authority from
            # which to prove resident process-zero. Preserve a credential-
            # capable historical VM rather than turning inventory ownership
            # or API deletion into containment evidence.
            return VMTeardownResult("process_zero_unproven", False)
        await self._delete_vm_with_identity(
            job_id,
            purge_disk=purge_disk,
            provision_generation=generation,
            expected_vm_uid=vm_uid,
            expected_rootdisk_pvc_uid=rootdisk_uid,
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )
        reprobe = await self._probe_vm_teardown_identity(job_id, generation)
        reclassification = self._classify_captured_probe(
            reprobe, identity, purge_disk=purge_disk
        )
        if reclassification == "completed":
            return VMTeardownResult("completed", True)
        if reclassification == "superseded":
            return VMTeardownResult("identity_superseded", False)
        return VMTeardownResult("retry_pending", False)

    async def delete_vm(self, job_id: str, purge_disk: bool = True) -> bool:
        """Delete a VM for a job.

        Args:
            job_id: Job UUID.
            purge_disk: False when a recreate is expected (crash recovery, the
                reconciler giving up on a dirty VM) — the controller keeps the
                persistent rootdisk and the Headscale node so the next create
                reattaches the same files. Default True: terminal, everything
                goes. Honoured by the lifecycle controller over NATS or HTTP.

        Returns:
            True if the request was accepted, False otherwise.
        """
        try:
            identity = await self.capture_vm_teardown_identity(job_id)
        except Exception:
            logger.warning(
                "VM delete refused without captured repository-process authority "
                "for job %s",
                job_id,
                exc_info=True,
            )
            return False
        outcome = await self.release_vm_captured(
            job_id,
            identity,
            purge_disk=purge_disk,
            capture_snapshot=False,
        )
        return outcome.disposition == "completed"

    async def _delete_vm_with_identity(
        self,
        job_id: str,
        *,
        purge_disk: bool,
        provision_generation: str | None,
        expected_vm_uid: str | None = None,
        expected_rootdisk_pvc_uid: str | None = None,
        entity_type: str = "job",
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> bool:
        generation = _provision_generation(provision_generation)
        if self._nats_available:
            kwargs: dict[str, Any] = {
                "purge_disk": purge_disk,
                "provision_generation": generation,
                "entity_type": entity_type,
            }
            if expected_vm_uid is not None:
                kwargs["expected_vm_uid"] = expected_vm_uid
            if expected_rootdisk_pvc_uid is not None:
                kwargs["expected_rootdisk_pvc_uid"] = expected_rootdisk_pvc_uid
            if parent_cleanup is not None:
                kwargs["parent_cleanup"] = dict(parent_cleanup)
            return await nats_bridge.request_vm_delete(job_id, **kwargs)

        if self._http_available:
            kwargs = {
                "entity_type": entity_type,
                "purge_disk": purge_disk,
                "provision_generation": generation,
            }
            if expected_vm_uid is not None:
                kwargs["expected_vm_uid"] = expected_vm_uid
            if expected_rootdisk_pvc_uid is not None:
                kwargs["expected_rootdisk_pvc_uid"] = expected_rootdisk_pvc_uid
            if parent_cleanup is not None:
                kwargs["parent_cleanup"] = dict(parent_cleanup)
            return await self._delete_http(job_id, **kwargs)

        return False

    async def delete_thread_vm_exact(
        self,
        thread_id: str,
        *,
        provision_generation: str,
        expected_vm_uid: str,
        expected_rootdisk_pvc_uid: str | None,
        purge_disk: bool,
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> bool:
        """Delete one captured thread VM incarnation, never its successor."""

        generation = _provision_generation(provision_generation)
        vm_uid = _safe_vm_uid(expected_vm_uid)
        rootdisk_uid = _safe_vm_uid(expected_rootdisk_pvc_uid)
        if (
            generation is None
            or vm_uid is None
            or (purge_disk and rootdisk_uid is None)
        ):
            return False
        if await self._current_provision_generation("thread", thread_id) != generation:
            return False
        return await self._delete_vm_with_identity(
            thread_id,
            purge_disk=purge_disk,
            provision_generation=generation,
            expected_vm_uid=vm_uid,
            expected_rootdisk_pvc_uid=rootdisk_uid,
            entity_type="thread",
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )

    async def release_vm(
        self,
        job_id: str,
        ssh_host: Optional[str] = None,
        ssh_port: Optional[int] = None,
    ) -> bool:
        """Snapshot a job VM to S3, then delete it.

        Args:
            job_id: Job UUID.
            ssh_host: SSH host for snapshot (read from DB context if omitted).
            ssh_port: SSH port for snapshot (read from DB context if omitted).

        Returns:
            True if deletion succeeded (snapshot failure is non-fatal).
        """
        try:
            identity = await self.capture_vm_teardown_identity(job_id)
        except Exception:
            logger.warning(
                "VM release refused without exact mutation identity for job %s",
                job_id,
                exc_info=True,
            )
            return False
        outcome = await self.release_vm_captured(
            job_id,
            identity,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
        )
        return outcome.disposition == "completed"

    async def release_thread_vm(
        self,
        thread_id: str,
        ssh_host: Optional[str] = None,
        ssh_port: Optional[int] = None,
    ) -> bool:
        """Snapshot a thread VM to S3, then delete it.

        Args:
            thread_id: Thread UUID.
            ssh_host: SSH host for snapshot (read from DB if omitted).
            ssh_port: SSH port for snapshot (read from DB if omitted).

        Returns:
            True if deletion succeeded (snapshot failure is non-fatal).
        """
        try:
            identity = await self.capture_vm_teardown_identity(
                thread_id,
                entity_type="thread",
            )
        except Exception:
            logger.warning(
                "VM release refused without exact mutation identity for thread %s",
                thread_id,
                exc_info=True,
            )
            return False
        outcome = await self.release_vm_captured(
            thread_id,
            identity,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            entity_type="thread",
        )
        return outcome.disposition == "completed"

    async def query_status(
        self,
        job_id: str,
        timeout: float = 5.0,
        entity_type: str = "job",
    ) -> Optional[dict]:
        """Query live VM status.

        External mode uses NATS request/reply. Same-cluster mode uses the HTTP
        controller's ``GET /vms/{job_id}`` route.

        Returns:
            Status dict or None if unavailable.
        """
        generation = await self._current_provision_generation(entity_type, job_id)
        phase_token = None
        if entity_type == "job" and self._phase_store is not None:
            try:
                phase_token = await self._phase_store.capture(job_id, generation)
            except Exception:
                logger.exception(
                    "Could not capture VM phase observation for %s", job_id
                )
                return None
            if phase_token is None:
                return None
        result: Optional[dict]
        if self._nats_available:
            result = await nats_bridge.query_vm_status(
                job_id,
                timeout,
                provision_generation=generation,
            )

        elif self._http_available:
            result = await self._query_http(
                job_id,
                timeout,
                provision_generation=generation,
            )

        else:
            return None
        if result is not None:
            if result.get("status") == "not_found":
                return None
            authenticated = result.get("_identity_authenticated") is True
            current = await self._persist_status_identity(
                entity_type,
                job_id,
                result,
                **({"phase_token": phase_token} if phase_token is not None else {}),
            )
            if authenticated and not current:
                return None
            result.pop("_identity_authenticated", None)
        return result

    async def reconcile_workspace_recovery_pin(self, command: Any) -> Mapping[str, Any]:
        """Project one durable DB pin into the controller's Lease registry."""

        if not self._http_available or self._http_client is None:
            raise RuntimeError("same-cluster recovery pin transport is unavailable")
        payload = {
            "recovery_id": str(command.recovery_id),
            "pvc_uid": str(command.pvc_uid),
            "provision_generation": str(command.provision_generation),
            "state": command.desired_state,
            "owner_kind": command.owner_kind,
            "owner_id": str(command.owner_id),
            "namespace": command.namespace,
        }
        if command.controller_pin_uid is not None:
            payload["pin_uid"] = command.controller_pin_uid
        if command.controller_pin_resource_version is not None:
            payload["resource_version"] = command.controller_pin_resource_version
        signed = sign_payload(
            payload,
            direction="request",
            operation="recovery-pin",
            secret=self._lifecycle_hmac_secret,
        )
        auth = signed.get(AUTH_FIELD)
        request_id = auth.get("request_id") if isinstance(auth, Mapping) else None
        response = await self._http_client.post(
            "/workspace-recovery/pins", json=signed, timeout=self._http_timeout
        )
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation="recovery-pin",
            secret=self._lifecycle_hmac_secret,
            expected_correlation_id=request_id,
        ):
            raise RuntimeError("workspace recovery pin response is unauthenticated")
        response.raise_for_status()
        return unsigned_payload(data)

    async def observe_workspace_recovery(
        self, captured_identity: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Observe recovery authority without calling status persistence."""

        if not self._http_available or self._http_client is None:
            raise RuntimeError("same-cluster recovery observation is unavailable")
        payload = {key: str(value) for key, value in captured_identity.items()}
        signed = sign_payload(
            payload,
            direction="request",
            operation="recovery-observe",
            secret=self._lifecycle_hmac_secret,
        )
        auth = signed.get(AUTH_FIELD)
        request_id = auth.get("request_id") if isinstance(auth, Mapping) else None
        response = await self._http_client.post(
            "/workspace-recovery/observe", json=signed, timeout=self._http_timeout
        )
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation="recovery-observe",
            secret=self._lifecycle_hmac_secret,
            expected_correlation_id=request_id,
        ):
            raise RuntimeError("workspace recovery observation is unauthenticated")
        response.raise_for_status()
        observation = dict(unsigned_payload(data))
        owner_kind = str(captured_identity.get("owner_kind") or "")
        owner_id = str(captured_identity.get("owner_id") or "")
        try:
            row = (
                await self._db.get_thread(owner_id)
                if owner_kind == "thread"
                else await self._db.get_job(owner_id)
            )
        except Exception as exc:
            raise RuntimeError("workspace recovery owner read failed") from exc
        if not isinstance(row, Mapping):
            raise RuntimeError("workspace recovery owner is unavailable")
        context = (
            _extract_thread_vm_context(row)
            if owner_kind == "thread"
            else _extract_vm_context(dict(row))
        )
        if any(
            str(context.get(context_key) or "")
            != str(captured_identity.get(captured_key) or "")
            for context_key, captured_key in (
                ("provision_generation", "provision_generation"),
                ("vm_uid", "vm_uid"),
                ("rootdisk_pvc_uid", "root_pvc_uid"),
            )
        ):
            observation["ambiguous"] = True
            return observation
        successor = observation.get("successor")
        if observation.get("ready") is not True or not isinstance(successor, Mapping):
            return observation
        fingerprint = _safe_ssh_host_key_fingerprint(
            context.get("ssh_host_key_fingerprint")
        )
        if fingerprint is None:
            return observation
        from orchestrator.services.vm_readiness import qualify_recovery_successor

        qualified = await qualify_recovery_successor(
            successor,
            host_key_fingerprint=fingerprint,
        )
        if qualified is None:
            return observation
        successor = {**dict(successor), **qualified}
        observation.update(
            {
                "authenticated": True,
                "successor": successor,
                "network_qualification": {
                    **dict(observation.get("network_qualification") or {}),
                    **dict(qualified.get("guest_network") or {}),
                    "address": successor.get("pod_ip"),
                    "cloud_init_cache": "untouched",
                    "legacy_cloud_init_cache_cleaned": False,
                    "qualified": True,
                },
            }
        )
        return observation

    async def list_vms(
        self, *, include_teardown_identity: bool = False
    ) -> Optional[list]:
        """Enumerate the VMs the backend is actually running.

        Inventory source for the lifecycle VM orphan sweep — the DB-derived
        instance view can't see a VM whose owning row was deleted. Returns a
        list of ``{vm_name, entity_id, created_at, phase}`` dicts, or None
        when no transport can answer (docker pool has no dynamic VMs; an old
        controller without the list op times out / 404s). None means
        "unknown", never "no VMs" — callers must not reap on it.
        """
        if self._nats_available:
            if include_teardown_identity:
                return await nats_bridge.request_vm_list(include_teardown_identity=True)
            return await nats_bridge.request_vm_list()

        if self._http_available:
            return await self._list_http(
                include_teardown_identity=include_teardown_identity
            )

        return None

    # HTTP controller backend (same-cluster, controller-mediated)
    #
    # The controller still owns KubeVirt RBAC and the VM template; this
    # transport just swaps NATS for HTTP. Status updates that NATS pushed
    # asynchronously now arrive synchronously in the response body, so
    # context updates happen here in the orchestrator instead of via the
    # vm.lifecycle.status subscription.
    #
    # Caveat: in-VM daemon events (register/heartbeat/freeze/resume) are
    # NOT carried by this transport. Same-cluster deployments that need
    # those still need NATS (or a future HTTP webhook from the daemon).
    # =========================================================================

    async def _create_http(
        self,
        job_id: str,
        agent_config: str,
        vm_image: Optional[str],
        cpu_cores: int,
        memory: str,
        description: str,
        entity_type: str = "job",
        set_provisioning: bool = True,
        provision_generation: str | None = None,
        disk_size: Optional[str] = None,
        initialization: dict | None = None,
        workspace_storage: dict | None = None,
        preparation: dict | None = None,
    ) -> bool | dict[str, Any]:
        """Create a VM by POSTing to the co-located VM controller.

        ``set_provisioning=False`` marks a deferred-create poll re-issue: skip the
        interim 'provisioning' context write so the status stays
        ``waiting_*`` between polls (the response merge below still
        records whatever the controller answered).
        """
        if self._http_client is None:
            return False

        from orchestrator.services.vm_creation_request import (
            build_vm_creation_request,
            capture_vm_creation_request,
        )

        generation = _provision_generation(provision_generation)
        if self._lifecycle_hmac_secret is not None and generation is None:
            logger.error("Refusing authenticated HTTP VM create without a generation")
            return False
        # Only Job creates with a durable store participate in snapshot capture.
        # Legacy no-store and thread transports retain their existing behavior.
        capture_request = (
            entity_type == "job" and generation is not None and self._db is not None
        )
        snapshot = None
        try:
            if capture_request:
                snapshot = await capture_vm_creation_request(
                    self._db,
                    job_id=job_id,
                    generation=generation,
                )
            if snapshot is None:
                network_tier = DEFAULT_NETWORK_TIER
                if self._db is not None:
                    try:
                        network_tier = (
                            await self._db.get_workspace_network_tier(
                                job_id, entity_type
                            )
                            or DEFAULT_NETWORK_TIER
                        )
                    except Exception:
                        logger.exception(
                            "Failed to resolve network_tier for %s=%s; using default",
                            entity_type,
                            job_id,
                        )
                payload = build_vm_creation_request(
                    job_id=job_id,
                    agent_config=agent_config,
                    vm_image=vm_image,
                    cpu_cores=cpu_cores,
                    memory=memory,
                    description=description,
                    entity_type=entity_type,
                    network_tier=network_tier,
                    provision_generation=generation,
                    orchestrator_url=os.getenv("ORCHESTRATOR_URL"),
                    disk_size=disk_size,
                    initialization=initialization,
                    workspace_storage=workspace_storage,
                    preparation=preparation,
                )
                if capture_request:
                    snapshot = await capture_vm_creation_request(
                        self._db,
                        job_id=job_id,
                        generation=generation,
                        request=payload,
                        initial_request=set_provisioning,
                    )
                    if snapshot is None:
                        return False
            if snapshot is not None:
                payload = snapshot["request"]
                preparation = payload.get("preparation")
                workspace_storage = payload.get("workspace_storage")
        except Exception:
            # Capture failure cannot degrade into an uncaptured create. Do not
            # log option values (initialization/description may contain secrets).
            logger.error(
                "VM creation request capture refused for %s %s", entity_type, job_id
            )
            return False
        payload = sign_payload(
            payload,
            direction="request",
            operation="create",
            secret=self._lifecycle_hmac_secret,
        )
        request_auth = payload.get(AUTH_FIELD)
        request_id = (
            request_auth.get("request_id")
            if isinstance(request_auth, Mapping)
            else None
        )

        response_authenticated = False
        observation = {
            "version": 1,
            "provision_generation": generation,
            "outcome": "response_unproven",
            "authenticated": False,
        }
        try:
            if set_provisioning:
                if generation is not None:
                    persisted = await self._set_context_if_generation(
                        entity_type,
                        job_id,
                        generation,
                        {"status": "provisioning"},
                    )
                    if self._db is not None and not persisted:
                        return False
                else:
                    await self._set_context(
                        entity_type, job_id, {"status": "provisioning"}
                    )
            resp = await self._http_client.post("/vms", json=payload)
            data = resp.json()
            if not isinstance(data, Mapping) or not verify_payload(
                data,
                direction="response",
                operation="create",
                secret=self._lifecycle_hmac_secret,
                expected_correlation_id=request_id,
            ):
                raise RuntimeError(
                    "VM controller create response authentication failed"
                )
            data = unsigned_payload(data)
            response_authenticated = (
                self._lifecycle_hmac_secret is not None
                and generation is not None
                and data.get("provision_generation") == generation
            )
            observation.update(
                authenticated=response_authenticated,
                outcome="observed" if response_authenticated else "response_unproven",
            )
            resp.raise_for_status()
            if preparation is not None and data.get("status") == "created":
                receipt = data.get("preparation") or {}
                if (
                    receipt.get("phase") not in {"Succeeded", "ExistingWorkspace"}
                    or receipt.get("allocationId") != job_id
                ):
                    raise RuntimeError(
                        "VM controller did not attest workspace preparation."
                    )
            if workspace_storage is not None and data.get("status") == "created":
                expected_storage = {
                    **workspace_storage,
                    "pvc_uid": data.get("rootdisk_pvc_uid"),
                }
                if data.get("workspace_storage") != expected_storage:
                    raise RuntimeError(
                        "VM controller did not attest the retained workspace binding."
                    )

            updates = {
                "status": data.get("status", "created"),
                "vm_name": data.get("vm_name"),
                "namespace": data.get("namespace"),
                "provisioned_by": "http",
                "creation_observation": observation,
            }
            # New controllers return the immutable admitted VM UID. During a
            # rolling upgrade an older controller may omit it; the fresh
            # context reset leaves vm_uid=None so metering classifies that VM
            # as legacy-unknown instead of trusting its reusable name.
            # Deferred-create responses carry telemetry instead of a VM name
            # (golden import progress, or why the mesh VPN is unreachable);
            # surface it for the dispatcher's poll logging and park message.
            for key in (
                "golden",
                "golden_phase",
                "golden_progress",
                "headscale_error",
                "running_vms",
                "max_concurrent_vms",
                "preparation",
                "error",
            ):
                if data.get(key) is not None:
                    updates[key] = data[key]
            response_generation = _provision_generation(
                data.get("provision_generation")
            )
            identity_updates: dict[str, Any] = {}
            if (
                self._lifecycle_hmac_secret is not None
                and response_generation is not None
                and response_generation == generation
            ):
                if (vm_uid := _safe_vm_uid(data.get("vm_uid"))) is not None:
                    identity_updates["vm_uid"] = vm_uid
                if (
                    rootdisk_pvc_uid := _safe_vm_uid(data.get("rootdisk_pvc_uid"))
                ) is not None:
                    identity_updates["rootdisk_pvc_uid"] = rootdisk_pvc_uid
                if (
                    host_key_fingerprint := _safe_ssh_host_key_fingerprint(
                        data.get("ssh_host_key_fingerprint")
                    )
                ) is not None:
                    # The controller created this pin before the VM and Secret
                    # admission result crossed the authenticated transport. It
                    # belongs in this generation-CAS merge with vm_uid so a
                    # stale response can never arm readiness for a new guest.
                    identity_updates["ssh_host_key_fingerprint"] = host_key_fingerprint
                if identity_updates:
                    identity_updates.update(
                        {
                            "identity_authenticated": True,
                            "identity_provision_generation": response_generation,
                        }
                    )
            if response_generation and response_generation == generation:
                merged = await self._set_context_if_generation(
                    entity_type,
                    job_id,
                    response_generation,
                    {**updates, **identity_updates},
                )
                if (
                    merged
                    and workspace_storage is not None
                    and identity_updates.get("rootdisk_pvc_uid")
                ):
                    from orchestrator.services.retained_vm_workspaces import (
                        record_created,
                    )

                    await record_created(
                        self._db,
                        job_id,
                        workspace_storage,
                        identity_updates["rootdisk_pvc_uid"],
                        namespace=data.get("namespace"),
                    )
                if not merged:
                    if self._lifecycle_hmac_secret is not None:
                        logger.warning(
                            "Ignoring stale authenticated HTTP create response for "
                            "%s %s",
                            entity_type,
                            job_id,
                        )
                    else:
                        await self._set_context(entity_type, job_id, updates)
            else:
                if self._lifecycle_hmac_secret is not None:
                    logger.warning(
                        "Ignoring authenticated HTTP create response with a stale or "
                        "missing provision generation for %s %s",
                        entity_type,
                        job_id,
                    )
                else:
                    await self._set_context(entity_type, job_id, updates)
            if data.get("status") == "waiting_preparation":
                logger.info(
                    "VM create deferred for workspace preparation (%s %s)",
                    entity_type,
                    job_id,
                )
            elif data.get("status") == "waiting_golden":
                logger.info(
                    "VM create deferred (http): golden %s importing (%s %s)",
                    data.get("golden"),
                    entity_type,
                    job_id,
                )
            elif data.get("status") == "waiting_capacity":
                logger.info(
                    "VM create deferred (http): capacity %s/%s (%s %s)",
                    data.get("running_vms"),
                    data.get("max_concurrent_vms"),
                    entity_type,
                    job_id,
                )
            elif data.get("status") == "waiting_headscale":
                logger.info(
                    "VM create deferred (http): Headscale unavailable (%s) (%s %s)",
                    data.get("headscale_error"),
                    entity_type,
                    job_id,
                )
            else:
                logger.info(
                    "VM created (http): %s (%s %s)",
                    data.get("vm_name"),
                    entity_type,
                    job_id,
                )
            return dict(data)
        except httpx.HTTPStatusError as e:
            error = _extract_http_error(e.response)
            logger.error(
                "VM controller rejected create for %s %s: %s",
                entity_type,
                job_id,
                error,
            )
            observation.update(
                outcome="rejected" if response_authenticated else "response_unproven",
                http_status=e.response.status_code,
            )
            failure = {
                "status": "failed",
                "error": error,
                "provisioned_by": "http",
                "creation_observation": observation,
            }
            if generation is not None:
                await self._set_context_if_generation(
                    entity_type, job_id, generation, failure
                )
            else:
                await self._set_context(entity_type, job_id, failure)
            return False
        except httpx.RequestError:
            observation.update(outcome="transport_unknown", authenticated=False)
            if preparation is not None and generation is not None:
                # The controller may already have persisted the build or VM.
                # Poll the SAME admitted allocation; a lost reply is neither a
                # successful build nor permission for another disk writer.
                waiting = {
                    "status": "waiting_preparation",
                    "creation_observation": observation,
                    "preparation": {
                        "allocationId": preparation["allocationId"],
                        "phase": "Pending",
                    },
                }
                if await self._set_context_if_generation(
                    entity_type, job_id, generation, waiting
                ):
                    return waiting
                return False
            failure = {
                "status": "failed",
                "error": "VM controller transport unavailable",
                "provisioned_by": "http",
                "creation_observation": observation,
            }
            if generation is not None:
                await self._set_context_if_generation(
                    entity_type, job_id, generation, failure
                )
            else:
                await self._set_context(entity_type, job_id, failure)
            return False
        except Exception as e:
            observation["outcome"] = "response_unproven"
            logger.error("HTTP create failed for %s %s: %s", entity_type, job_id, e)
            failure = {
                "status": "failed",
                "error": str(e),
                "provisioned_by": "http",
                "creation_observation": observation,
            }
            if generation is not None:
                await self._set_context_if_generation(
                    entity_type, job_id, generation, failure
                )
            else:
                await self._set_context(entity_type, job_id, failure)
            return False

    async def _delete_http(
        self,
        job_id: str,
        entity_type: str = "job",
        purge_disk: bool = True,
        provision_generation: str | None = None,
        expected_vm_uid: str | None = None,
        expected_rootdisk_pvc_uid: str | None = None,
        parent_cleanup: Mapping[str, Any] | None = None,
    ) -> bool:
        """Delete a VM by sending DELETE to the co-located VM controller."""
        if self._http_client is None:
            return False

        generation = _provision_generation(provision_generation)
        if self._lifecycle_hmac_secret is not None and generation is None:
            logger.error(
                "Refusing authenticated HTTP VM delete for %s without a current "
                "provision generation",
                job_id,
            )
            return False
        signed_payload = {
            "job_id": job_id,
            "purge_disk": purge_disk,
            "provision_generation": generation,
        }
        if entity_type != "job":
            signed_payload["entity_type"] = entity_type
        if expected_vm_uid is not None:
            signed_payload["expected_vm_uid"] = expected_vm_uid
        if expected_rootdisk_pvc_uid is not None:
            signed_payload["expected_rootdisk_pvc_uid"] = expected_rootdisk_pvc_uid
        params: dict[str, str] = {}
        if parent_cleanup is not None:
            signed_payload["parent_cleanup"] = json.dumps(
                parent_cleanup, sort_keys=True, separators=(",", ":")
            )
            params["parent_cleanup"] = signed_payload["parent_cleanup"]
        if entity_type != "job":
            params["entity_type"] = entity_type
        if not purge_disk:
            params["purge_disk"] = "false"
        if generation is not None:
            params["provision_generation"] = generation
        if expected_vm_uid is not None:
            params["expected_vm_uid"] = expected_vm_uid
        if expected_rootdisk_pvc_uid is not None:
            params["expected_rootdisk_pvc_uid"] = expected_rootdisk_pvc_uid
        binding = await self._storage_context(job_id)
        if binding is not None:
            signed_payload["workspace_storage"] = json.dumps(
                binding, sort_keys=True, separators=(",", ":")
            )
            params["workspace_storage"] = signed_payload["workspace_storage"]
        params.update(
            _http_lifecycle_query(
                signed_payload,
                operation="delete",
                secret=self._lifecycle_hmac_secret,
            )
        )
        request_id = params.get("lifecycle_auth_request_id")
        try:
            resp = await self._http_client.delete(f"/vms/{job_id}", params=params)
            data: Mapping[str, Any] | None = None
            if self._lifecycle_hmac_secret is not None or resp.status_code != 404:
                candidate = resp.json()
                if not isinstance(candidate, Mapping) or not verify_payload(
                    candidate,
                    direction="response",
                    operation="delete",
                    secret=self._lifecycle_hmac_secret,
                    expected_correlation_id=request_id,
                ):
                    raise RuntimeError(
                        "VM controller delete response authentication failed"
                    )
                data = unsigned_payload(candidate)
            if resp.status_code != 404:
                resp.raise_for_status()
            if self._lifecycle_hmac_secret is not None:
                response_generation = _provision_generation(
                    data.get("provision_generation") if data else None
                )
                if response_generation != generation:
                    raise RuntimeError(
                        "VM controller delete response generation mismatch"
                    )
            if generation is not None:
                await self._set_context_if_generation(
                    entity_type, job_id, generation, {"status": "deleted"}
                )
            else:
                await self._set_context(entity_type, job_id, {"status": "deleted"})
            logger.info("VM deleted (http): %s %s", entity_type, job_id)
            return True
        except httpx.HTTPStatusError as e:
            error = _extract_http_error(e.response)
            logger.error(
                "VM controller rejected delete for %s %s: %s",
                entity_type,
                job_id,
                error,
            )
            return False
        except Exception as e:
            logger.error("HTTP delete failed for %s %s: %s", entity_type, job_id, e)
            return False

    async def _query_http(
        self,
        job_id: str,
        timeout: float = 5.0,
        provision_generation: str | None = None,
        *,
        exact_absence: bool = False,
    ) -> Optional[dict]:
        """Query VM status via the co-located VM controller."""
        if self._http_client is None:
            return None

        generation = _provision_generation(provision_generation)
        if self._lifecycle_hmac_secret is not None and generation is None:
            return None
        signed_payload = {
            "job_id": job_id,
            "provision_generation": generation,
        }
        params: dict[str, str] = {}
        if generation is not None:
            params["provision_generation"] = generation
        if exact_absence:
            signed_payload["exact_absence"] = True
            params["exact_absence"] = "true"
        binding = await self._storage_context(job_id)
        if binding is not None:
            signed_payload["workspace_storage"] = json.dumps(
                binding, sort_keys=True, separators=(",", ":")
            )
            params["workspace_storage"] = signed_payload["workspace_storage"]
        params.update(
            _http_lifecycle_query(
                signed_payload,
                operation="status",
                secret=self._lifecycle_hmac_secret,
            )
        )
        request_id = params.get("lifecycle_auth_request_id")
        try:
            resp = await self._http_client.get(
                f"/vms/{job_id}", params=params, timeout=timeout
            )
            data = resp.json()
            if not isinstance(data, Mapping) or not verify_payload(
                data,
                direction="response",
                operation="status",
                secret=self._lifecycle_hmac_secret,
                expected_correlation_id=request_id,
            ):
                return None
            result = unsigned_payload(data)
            if (
                self._lifecycle_hmac_secret is not None
                and _provision_generation(result.get("provision_generation"))
                != generation
            ):
                return None
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            if self._lifecycle_hmac_secret is not None:
                result["_identity_authenticated"] = True
            return result
        except Exception as e:
            logger.debug("HTTP status query failed for job %s: %s", job_id, e)
            return None

    async def _list_http(
        self, *, include_teardown_identity: bool = False
    ) -> Optional[list]:
        """List managed VMs via the co-located VM controller.

        A 404 means the controller predates the list op — unknown, not empty.
        """
        if self._http_client is None:
            return None

        try:
            signed_payload = {
                **(
                    {"include_teardown_identity": True}
                    if include_teardown_identity
                    else {}
                )
            }
            params = _http_lifecycle_query(
                signed_payload, operation="list", secret=self._lifecycle_hmac_secret
            )
            if include_teardown_identity:
                params["include_teardown_identity"] = "true"
            resp = await self._http_client.get(
                "/vms",
                params=params,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, Mapping) or not verify_payload(
                data,
                direction="response",
                operation="list",
                secret=self._lifecycle_hmac_secret,
                expected_correlation_id=params.get("lifecycle_auth_request_id"),
            ):
                return None
            vms = unsigned_payload(data).get("vms")
            return vms if isinstance(vms, list) else None
        except Exception as e:
            logger.debug("HTTP VM list failed: %s", e)
            return None

    async def _set_context(
        self, entity_type: str, entity_id: str, updates: dict
    ) -> None:
        """Route context updates to the right table based on entity type."""
        if entity_type == "thread":
            await self._set_thread_vm_context(entity_id, updates)
        else:
            await self._set_vm_context(entity_id, updates)

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _fresh_provision_ctx() -> dict:
        """Reap-counter/endpoint reset + provision timestamp for a new VM.

        Merged into context.vm at the start of every (re)provision so the new
        incarnation does not inherit the previous one's snapshot_attempts (which
        would make the lifecycle reaper's attempts_exhausted instantly true), a
        dead ssh_host, or stale SSH-readiness probe identity. ``provisioned_at``
        (epoch seconds) anchors the dispatcher's provisioning-timeout escalation.
        """
        return {
            "snapshot_attempts": 0,
            "ssh_host": None,
            "ssh_port": None,
            "ssh_registration_id": None,
            "registered_at": None,
            "ssh_verified_at": None,
            "ssh_probe_attempts": 0,
            "ssh_probe_error": None,
            "ssh_probe_failed_at": None,
            # A fresh provision gets a fresh controller-owned keypair. Leave
            # readiness fail-closed until the admitted controller response
            # installs the matching public fingerprint for this generation.
            "ssh_host_key_fingerprint": None,
            # Never let a previous VM incarnation's UID authenticate the next
            # one between create dispatch and the admitted controller result.
            "vm_uid": None,
            "_runtime_incarnation": None,
            # A PVC name is reusable.  Only the newly admitted immutable UID
            # may authenticate this incarnation's rootdisk for metering.
            "rootdisk_pvc_uid": None,
            "identity_authenticated": False,
            "identity_provision_generation": None,
            "creation_request": None,
            "creation_observation": None,
            "provisioning": None,
            "provisioning_revision": 0,
            "provisioning_attention_reason": None,
            # Opaque incarnation nonce. Controller identities are merged only
            # through a DB-side compare-and-merge against this exact value.
            "provision_generation": str(uuid4()),
            # The provisioner now owns and attests the guest host key. Canvas
            # remains closed until its separate workspace-generation binding
            # is implemented; key ownership alone does not enable that gate.
            CANVAS_WORKSPACE_GENERATION_KEY: None,
            # Golden-wait anchor from a previous incarnation must not cap this
            # provision's patience for a cold golden import (dispatcher stamps
            # it again on the first waiting_golden it sees). Same for the
            # Headscale-wait anchor.
            "golden_wait_started_at": None,
            "capacity_wait_started_at": None,
            "headscale_wait_started_at": None,
            "preparation_wait_started_at": None,
            "preparation_request": None,
            "preparation": None,
            # Same for the teardown anchor: a stale one would make this
            # incarnation read as instantly-stuck the moment it enters
            # 'deleting', and the dispatcher would recycle it on sight.
            "deleting_started_at": None,
            "retirement_attempts": 0,
            "retirement_cleanup_pending": False,
            "retirement_last_result": None,
            "retirement_retry_after": None,
            "headscale_error": None,
            "provisioned_at": time.time(),
        }

    async def _set_vm_context(self, job_id: str, updates: dict) -> None:
        """Atomically merge updates into the job's context.vm key."""
        if not self._db:
            return

        try:
            await self._db.merge_vm_context(job_id, updates)
        except Exception:
            logger.exception("Failed to update VM context for job %s", job_id)

    # =========================================================================
    # Thread VM support (persistent agent sessions)
    # =========================================================================

    async def create_thread_vm(
        self,
        thread_id: str,
        agent_config: str = "worker_base",
        vm_image: Optional[str] = None,
        cpu_cores: int = 8,
        memory: str = "16Gi",
        description: str = "",
        *,
        disk_size: Optional[str] = None,
        initialization: dict | None = None,
        preparation: dict | None = None,
        expected_runtime_generation: str | None = None,
        expected_agent_id: str | None = None,
        expected_attach_token: str | None = None,
        expected_vm_context: Mapping[str, Any] | None = None,
        poll: bool = False,
    ) -> bool | dict[str, Any]:
        """Create a VM for a persistent thread.

        Mirrors create_vm() but routes context to threads.metadata.vm
        and uses entity_type="thread" for NATS bridge routing.

        Returns:
            True if the request was accepted, False otherwise.
        """
        if preparation is not None:
            preparation = await self._validate_preparation(
                thread_id, "thread", preparation
            )
        if initialization is not None:
            from shared.workspace_initialization import validate_initialization_request

            initialization = validate_initialization_request(initialization)
            if self.mode != "same-cluster":
                raise ValueError(
                    "Workspace initialization requires same-cluster VM hosting."
                )
        if self.mode == "external":
            logger.warning("Thread VM create refused: %s", self.unavailable_reason)
            return False
        preparation_context = None
        if preparation is not None and not any(
            (expected_vm_context or {}).get(key)
            for key in ("vm_uid", "rootdisk_pvc_uid", "provision_generation")
        ):
            preparation_context, waiting = await self._prepare_thread_workspace(
                thread_id,
                preparation,
                expected_runtime_generation=expected_runtime_generation,
                expected_agent_id=expected_agent_id,
                expected_attach_token=expected_attach_token,
                expected_vm_context=expected_vm_context,
            )
            if preparation_context is None:
                return waiting
        # Thread VM creation is a lifecycle effect, not a best-effort metadata
        # merge.  Install its authenticated provision generation under the
        # exact open pinned T/G/actor tuple before NATS or HTTP can observe a
        # request.  A stale route read, End, Resume, rebind, or DB failure is a
        # hard refusal with zero external calls.
        fresh_context = self._fresh_provision_ctx()
        fresh_context.update(
            initialization=initialization,
            preparation_request=preparation,
            preparation=None,
            preparation_wait_started_at=time.time()
            if preparation is not None
            else None,
            initialization_receipt=None,
            initialization_started_at=None,
        )
        fresh_context["status"] = "provisioning"
        if poll:
            if not isinstance(
                expected_vm_context, Mapping
            ) or not _provision_generation(
                expected_vm_context.get("provision_generation")
            ):
                return False
            fresh_context["provision_generation"] = expected_vm_context[
                "provision_generation"
            ]
            for key in (
                "preparation_wait_started_at",
                "golden_wait_started_at",
                "capacity_wait_started_at",
                "headscale_wait_started_at",
            ):
                fresh_context[key] = expected_vm_context.get(
                    key
                ) or expected_vm_context.get("provisioned_at")
        generation = fresh_context["provision_generation"]
        if preparation_context is not None:
            fresh_context["preparation_wait_started_at"] = preparation_context[
                "preparation_wait_started_at"
            ]
        if self._db is None or expected_runtime_generation is None:
            return False
        begin_impl = getattr(self._db, "begin_pinned_thread_vm_provisioning", None)
        if not callable(begin_impl):
            return False
        try:
            persisted = await begin_impl(
                thread_id,
                expected_runtime_generation=expected_runtime_generation,
                expected_agent_id=expected_agent_id,
                expected_attach_token=expected_attach_token,
                expected_vm_context=expected_vm_context,
                provision_context=fresh_context,
                **({"poll": True} if poll else {}),
                **(
                    {"expected_preparation_context": preparation_context}
                    if preparation_context is not None
                    else {}
                ),
            )
        except Exception:
            logger.exception(
                "Failed to install pinned VM provision authority for %s",
                thread_id,
            )
            return False
        if not persisted:
            return False

        result: bool | dict[str, Any]
        if self._nats_available:
            result = await nats_bridge.request_vm_create(
                job_id=thread_id,
                agent_config=agent_config,
                vm_image=vm_image,
                cpu_cores=cpu_cores,
                memory=memory,
                description=description,
                entity_type="thread",
                set_provisioning=False,
                provision_generation=generation,
                **({"disk_size": disk_size} if disk_size is not None else {}),
                **(
                    {"initialization": initialization}
                    if initialization is not None
                    else {}
                ),
            )
        elif self._http_available:
            result = await self._create_http(
                job_id=thread_id,
                agent_config=agent_config,
                vm_image=vm_image,
                cpu_cores=cpu_cores,
                memory=memory,
                disk_size=disk_size,
                **({"preparation": preparation} if preparation is not None else {}),
                **(
                    {"initialization": initialization}
                    if initialization is not None
                    else {}
                ),
                description=description,
                entity_type="thread",
                set_provisioning=False,
                provision_generation=generation,
            )
        else:
            result = False
        if not result:
            await self._set_thread_vm_context_if_generation(
                thread_id,
                generation,
                {
                    "status": "failed",
                    "error": "VM create dispatch was not accepted",
                },
            )
        return result

    async def _prepare_thread_workspace(self, thread_id, preparation, **authority):
        """Admit cache work separately from the physical VM lifecycle."""
        if self._db is None or authority.get("expected_runtime_generation") is None:
            return None, False
        context = self._fresh_provision_ctx()
        context.update(
            status="provisioning",
            preparation_request=preparation,
            preparation_wait_started_at=time.time(),
            preparation_only=True,
        )
        begin = self._db.begin_pinned_thread_vm_provisioning
        stage = await begin(
            thread_id, **authority, provision_context=context, preparation_only=True
        )
        if not stage:
            # A resumed runtime must retire the old cache allocation before
            # replacing its durable cancellation record.
            thread = await self._db.get_thread(thread_id)
            from orchestrator.services.stateless_workspace_gate import (
                thread_metadata_object,
            )

            metadata = thread_metadata_object(thread or {})
            previous = (metadata.get("workspace_preparation") or {}).get(
                "preparation_request"
            )
            if (
                isinstance(previous, dict)
                and previous.get("allocationId") == thread_id
                and previous.get("ownerKind") == "session"
                and previous.get("runtimeGeneration")
                != preparation["runtimeGeneration"]
            ):
                result = await self.preparation_operation(
                    "cancel", {"preparation": previous}
                )
                if result.get("cancelled") is True:
                    await self._db.acknowledge_vm_preparation_cancelled(
                        "thread", thread_id, previous
                    )
                    stage = await begin(
                        thread_id,
                        **authority,
                        provision_context=context,
                        preparation_only=True,
                    )
            if not stage:
                return None, False
        from orchestrator.services.dispatch_guards import vm_provisioning_decision

        decision = vm_provisioning_decision(
            stage,
            provision_attempts=0,
            max_provision_attempts=1,
            now=time.time(),
            timeout_s=900,
        )
        if decision.startswith("park"):
            waiting = {"status": "failed", "error": "VM preparation deadline exceeded"}
        else:
            try:
                result = await self.preparation_operation(
                    "prepare", {"preparation": preparation}
                )
            except httpx.RequestError:
                # The controller may have accepted the allocation. Preserve
                # its identity and fixed deadline across a lost response.
                return None, {"status": "waiting_preparation"}
            if isinstance(result.get("source"), dict):
                return stage, None
            waiting = result.get("waiting")
            if not isinstance(waiting, dict) or waiting.get("status") not in {
                "waiting_preparation",
                "failed",
            }:
                raise ValueError("Invalid preparation progress response")
        updated = await self._db.merge_thread_preparation_if_current(
            thread_id, authority["expected_runtime_generation"], stage, waiting
        )
        return None, waiting if updated else False

    async def poll_thread_vm(self, thread_id: str, generation: str) -> None:
        """Resume an admitted import/build without allocating a new VM generation."""
        from orchestrator.services.session_runtime_admission import (
            thread_runtime_authority,
        )
        from orchestrator.services.stateless_workspace_gate import (
            thread_metadata_object,
        )
        from orchestrator.services.vm_workspace_config import vm_provisioning_options
        from orchestrator.services.dispatch_guards import vm_provisioning_decision

        async with self._db.thread_advisory_lock(thread_id):
            thread = await self._db.get_thread(thread_id)
            authority = thread_runtime_authority(thread)
            metadata = thread_metadata_object(thread or {})
            actual_vm = metadata.get("vm")
            vm = metadata.get("workspace_preparation") or actual_vm
            if (
                authority is None
                or not isinstance(vm, dict)
                or vm.get("provision_generation") != generation
            ):
                return
            decision = vm_provisioning_decision(
                vm,
                provision_attempts=0,
                max_provision_attempts=1,
                now=time.time(),
                timeout_s=900,
            )
            if decision.startswith("park") and not vm.get("preparation_only"):
                await self._set_thread_vm_context_if_generation(
                    thread_id,
                    generation,
                    {
                        "status": "failed",
                        "error": "VM preparation or import deadline exceeded",
                    },
                )
                return
            options = await vm_provisioning_options(
                self._db, "Session", thread, fallback=metadata.get("config_override")
            )
            await self.create_thread_vm(
                thread_id,
                **options,
                expected_runtime_generation=authority.generation,
                expected_agent_id=str(thread["agent_id"])
                if thread.get("agent_id")
                else None,
                expected_attach_token=str(thread["runtime_attach_token"])
                if thread.get("runtime_attach_token")
                else None,
                expected_vm_context=actual_vm,
                poll=not vm.get("preparation_only", False),
            )

    async def delete_thread_vm(self, thread_id: str, purge_disk: bool = True) -> bool:
        """Delete a VM for a persistent thread.

        Args:
            thread_id: Thread UUID.
            purge_disk: False when the session expects to come back (suspend) —
                see ``delete_vm``.

        Returns:
            True if the request was accepted, False otherwise.
        """
        try:
            identity = await self.capture_vm_teardown_identity(
                thread_id,
                entity_type="thread",
            )
        except Exception:
            logger.warning(
                "VM delete refused without captured repository-process authority "
                "for thread %s",
                thread_id,
                exc_info=True,
            )
            return False
        outcome = await self.release_vm_captured(
            thread_id,
            identity,
            entity_type="thread",
            purge_disk=purge_disk,
            capture_snapshot=False,
        )
        return outcome.disposition == "completed"

    async def _set_thread_vm_context(self, thread_id: str, updates: dict) -> None:
        """Atomically merge updates into thread's metadata.vm key."""
        if not self._db:
            return

        try:
            await self._db.merge_thread_vm_context(thread_id, updates)
        except Exception:
            logger.exception("Failed to update thread VM context for %s", thread_id)

    async def _set_thread_vm_context_if_generation(
        self,
        thread_id: str,
        generation: str,
        updates: dict,
        *,
        require_status_not_ready: bool = False,
    ) -> bool:
        if not self._db:
            return False
        try:
            if not require_status_not_ready:
                return bool(
                    await self._db.merge_thread_vm_context_if_provision_generation(
                        thread_id, generation, updates
                    )
                )
            return bool(
                await self._db.merge_thread_vm_context_if_provision_generation(
                    thread_id,
                    generation,
                    updates,
                    require_status_not_ready=require_status_not_ready,
                )
            )
        except Exception:
            logger.exception(
                "Failed to update generation-guarded thread VM context for %s",
                thread_id,
            )
            return False


def _extract_http_error(response: httpx.Response) -> str:
    """Pull a useful error message out of a controller HTTP error response."""
    try:
        data = response.json()
        if isinstance(data, dict) and data.get("error"):
            return str(data["error"])
    except Exception:
        pass
    text = (response.text or "").strip()
    return text or f"HTTP {response.status_code}"


# Module-level singleton
vm_provisioner = VMProvisioner()
