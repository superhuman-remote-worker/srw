"""Container Provisioner — Per-job workspace container lifecycle management.

Creates lightweight workspace containers as an alternative to KubeVirt VMs.
Workspace containers run on the same cluster as the orchestrator and provide:
  - SSH server (for RemoteBackend file/shell operations)
  - tmux (for persistent shell sessions)
  - code-server (for IDE access)
  - Dev tools (git, build-essential, node, python, etc.)

The agent connects to workspace containers via SSH, identical to VMs.
From the agent's perspective, there is no difference.

Selection logic:
  - kubernetes client available → container provisioning enabled
  - No kubernetes client → disabled (falls back to local backend)
"""

import asyncio
import hashlib
import inspect
import json
import logging
import os
import re
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal, Optional
from uuid import UUID, uuid4
from shared.workspace_recovery import WorkspaceRecoveryCode

from orchestrator.services import resolve_ssh_key_path, workspace_metering
from orchestrator.services.blocking_effect import joined_blocking_call
from orchestrator.services.ssh_helpers import (
    SSHPrivateKeyError,
    wait_for_agent_ssh,
    workspace_private_key_fingerprint,
)
from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from orchestrator.services.ide_credentials import IDE_CREDENTIAL_ENV, ide_credential
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.services.managed_repository_process_retirement import (
    retire_managed_repository_processes,
)
from orchestrator.services.sandbox_workspace_settings import (
    SandboxImagePolicy,
    SandboxPodProfile,
    SandboxSettings,
    classify_image_pull,
    image_repository,
    pod_admission_rejection,
    resolve_sandbox_settings,
    sandbox_pod_profile,
)

logger = logging.getLogger(__name__)

try:
    from kubernetes import client as k8s_client, config as k8s_config
    from kubernetes.stream import stream as k8s_stream

    K8S_AVAILABLE = True
except ImportError:
    k8s_client = None
    k8s_config = None
    k8s_stream = None
    K8S_AVAILABLE = False


def _isolated_pod_exec(*args: Any, **kwargs: Any) -> Any:
    """Run one pod exec without mutating the provisioner's shared API client.

    ``kubernetes.stream.stream`` temporarily replaces ``ApiClient.request``
    with its websocket transport.  Reusing the provisioner's client therefore
    lets concurrent ordinary Kubernetes calls observe the websocket request
    function.  A dedicated client confines that mutation to this exec call.
    """

    if k8s_client is None or k8s_stream is None:
        raise RuntimeError("Kubernetes exec transport is unavailable")
    api_client = k8s_client.ApiClient()
    try:
        api = k8s_client.CoreV1Api(api_client)
        return k8s_stream(api.connect_get_namespaced_pod_exec, *args, **kwargs)
    finally:
        api_client.close()


# Pod-network egress tier applied to workspaces whose owning project has
# no resolvable tier (no DB, no project, or pre-migration project rows).
# Matches the projects.network_tier column default; the closed CHECK set
# in 0016_project_network_tier.sql is the source of truth for valid names.
DEFAULT_NETWORK_TIER = "internet-only"

# Private control-plane attestation paired with the stable workspace backing
# generation. Unlike the PVC-backed generation, this value changes whenever
# Kubernetes replaces the workspace pod.
WORKSPACE_RUNTIME_INCARNATION_KEY = "_runtime_incarnation"
WORKSPACE_RUNTIME_CREATION_KEY = "_runtime_creation"
WORKSPACE_RUNTIME_CREATION_ANNOTATION = "srw.io/runtime-creation-generation"
WORKSPACE_CREATION_RESERVATION_ANNOTATION = "srw.io/workspace-creation-reservation"
WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY = "_creation_reservation_id"
WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY = "_creation_claim_token"
WORKSPACE_PROVISION_ATTEMPT_LABEL = "srw.io/workspace-provision-attempt"
WORKSPACE_PROVISION_GENERATION_LABEL = "srw.io/runtime-generation"
WORKSPACE_PROVISION_FENCE_LABEL = "srw.io/workspace-provision-fence"
PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS", "30")
)
PINNED_K8S_CREATE_FENCE_HORIZON_SECONDS = int(
    os.getenv("PINNED_K8S_CREATE_FENCE_HORIZON_SECONDS", "600")
)
# Bump whenever any rendered pinned PVC/ConfigMap/Pod/Service field changes.
# The durable fingerprint makes cross-process replay fail closed unless the
# complete create contract is byte-for-byte equivalent; it is not a loose
# config checksum.
PINNED_WORKSPACE_PROVISION_RENDER_CONTRACT_VERSION = 2
if PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS < 1:
    raise RuntimeError("PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS must be positive")
if PINNED_K8S_CREATE_FENCE_HORIZON_SECONDS < (
    PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS + 60
):
    raise RuntimeError(
        "PINNED_K8S_CREATE_FENCE_HORIZON_SECONDS must exceed the bounded "
        "Kubernetes mutation timeout by at least 60 seconds"
    )
STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER = "lifecycle.srw.dev/stateless-process-zero"
_UNSPECIFIED_RESOURCE_BINDING = object()


class WorkspaceSSHAuthenticationError(RuntimeError):
    """A K8s-ready workspace rejected the configured SSH identity."""


class WorkspaceRuntimeAuthorityError(RuntimeError):
    """A deterministic Pod name no longer identifies the authorized runtime."""


class WorkspaceImagePullError(RuntimeError):
    """A custom workspace image could not be pulled within its budget."""


class WorkspaceRuntimeRecoveryRequired(WorkspaceRuntimeAuthorityError):
    """An explicitly observed, allowlisted runtime recovery condition."""

    def __init__(self, detail: str, *, recovery_code: WorkspaceRecoveryCode) -> None:
        self.recovery_code = WorkspaceRecoveryCode(recovery_code)
        if self.recovery_code not in {
            WorkspaceRecoveryCode.RUNTIME_NOT_READY,
            WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE,
            WorkspaceRecoveryCode.REPLACEMENT_OBSERVED,
        }:
            raise ValueError("Unsupported runtime recovery condition")
        super().__init__(detail)


def _canonical_manifest_digest(payload: dict[str, Any]) -> str:
    """Hash the complete server-resolved physical creation plan."""

    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RuntimeDeletionOutcome:
    state: Literal["current_deleted", "stale_target_settled", "refused"]

    @property
    def current_deleted(self) -> bool:
        return self.state == "current_deleted"

    @property
    def stale_target_settled(self) -> bool:
        return self.state == "stale_target_settled"

    def __bool__(self) -> bool:
        raise TypeError("RuntimeDeletionOutcome.state must be inspected explicitly")


_CURRENT_RUNTIME_DELETED = RuntimeDeletionOutcome("current_deleted")
_STALE_RUNTIME_SETTLED = RuntimeDeletionOutcome("stale_target_settled")
_RUNTIME_DELETION_REFUSED = RuntimeDeletionOutcome("refused")


@dataclass(frozen=True)
class SharedResourceDeletionOutcome:
    state: Literal["captured_absent", "replacement_present", "refused"]

    @property
    def captured_absent(self) -> bool:
        return self.state == "captured_absent"

    @property
    def replacement_present(self) -> bool:
        return self.state == "replacement_present"

    def __bool__(self) -> bool:
        raise TypeError(
            "SharedResourceDeletionOutcome.state must be inspected explicitly"
        )


_SHARED_RESOURCE_ABSENT = SharedResourceDeletionOutcome("captured_absent")
_SHARED_RESOURCE_REPLACED = SharedResourceDeletionOutcome("replacement_present")
_SHARED_RESOURCE_REFUSED = SharedResourceDeletionOutcome("refused")


@dataclass(frozen=True)
class WorkspaceCleanupOutcome:
    state: Literal["settled", "superseded", "retryable"]
    intent_generation: int | None = None

    @property
    def settled(self) -> bool:
        return self.state == "settled"

    @property
    def superseded(self) -> bool:
        return self.state == "superseded"

    @property
    def retryable(self) -> bool:
        return self.state == "retryable"

    def __bool__(self) -> bool:
        raise TypeError("WorkspaceCleanupOutcome.state must be inspected explicitly")


_WORKSPACE_CLEANUP_SETTLED = WorkspaceCleanupOutcome("settled")
_WORKSPACE_CLEANUP_SUPERSEDED = WorkspaceCleanupOutcome("superseded")
_WORKSPACE_CLEANUP_RETRYABLE = WorkspaceCleanupOutcome("retryable")


@dataclass(frozen=True)
class WorkspaceRuntimeAttestation:
    """Exact control-plane identity for one live workspace SSH endpoint."""

    backing_id: str
    workspace_generation: str
    runtime_incarnation: str
    ssh_host_key_fingerprint: str
    host: str
    pod_ip: str
    port: int = 30022
    # VM-only identities. Kubernetes container callers leave both unset; VM
    # remote-operation leases require them in addition to the provision and
    # launcher generations so a same-endpoint replacement cannot inherit I/O.
    vm_uid: str | None = None
    launcher_pod_uid: str | None = None
    vmi_uid: str | None = None
    rootdisk_pvc_uid: str | None = None


@dataclass(frozen=True)
class WorkspaceTeardownIdentity:
    """Immutable Kubernetes identities captured before terminal teardown.

    A deterministic resource name is not authority: Kubernetes may recreate a
    Pod, PVC, or Service with the same name after a finalizer crash.  S36 keeps
    this fixed-cardinality record in the completion effect intent and every
    destructive call later carries the corresponding UID precondition.
    """

    pod_uid: str | None
    pvc_uid: str | None
    service_uid: str | None
    seed_configmap_uid: str | None = None
    pod_ip: str | None = None
    ssh_host_key_fingerprint: str | None = None
    ssh_port: int = 30022


def _canonical_runtime_uuid(value: Any, *, label: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{label} is invalid")
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    canonical = str(parsed)
    if value != canonical:
        raise ValueError(f"{label} is invalid")
    return canonical


def _resource_field(resource: Any, *names: str) -> Any:
    """Read one Kubernetes model/dict field without MagicMock coercion."""

    for name in names:
        if isinstance(resource, dict):
            if name in resource:
                return resource[name]
        elif resource is not None and hasattr(resource, name):
            return getattr(resource, name)
    return None


def _all_pod_container_statuses_terminated(pod: Any) -> bool:
    """Prove every declared/observed Pod container has terminated.

    Kubernetes reports regular, restartable-init, and ephemeral/debug
    containers in separate status arrays. Looking only at
    ``container_statuses`` can therefore declare a Pod quiescent while an init
    sidecar or injected debug container is still running. Missing status for a
    declared container is equally ambiguous. Only a non-empty, complete set of
    exact terminated states is positive proof.
    """

    status_obj = getattr(pod, "status", None)
    spec_obj = getattr(pod, "spec", None)
    observed_any = False
    for status_field, spec_field in (
        ("container_statuses", "containers"),
        ("init_container_statuses", "init_containers"),
        ("ephemeral_container_statuses", "ephemeral_containers"),
    ):
        raw_statuses = getattr(status_obj, status_field, None)
        if raw_statuses is None:
            statuses: list[Any] = []
        elif isinstance(raw_statuses, (list, tuple)):
            statuses = list(raw_statuses)
        else:
            return False

        raw_declared = getattr(spec_obj, spec_field, None)
        if isinstance(raw_declared, (list, tuple)) and raw_declared:
            declared_names = {
                str(getattr(container, "name", "") or "") for container in raw_declared
            }
            observed_names = {
                str(getattr(container, "name", "") or "") for container in statuses
            }
            if "" in declared_names or declared_names != observed_names:
                return False

        if statuses:
            observed_any = True
        if any(
            getattr(getattr(container, "state", None), "terminated", None) is None
            for container in statuses
        ):
            return False
    return observed_any


def _deleting_pod_was_never_scheduled(pod: Any) -> bool:
    """Prove an exact deleting unscheduled Pod never gained process authority.

    A finalizer is installed before scheduling, so a Pod deleted while the
    scheduler has not assigned ``spec.nodeName`` can never acquire a kubelet
    sandbox or container process: the binding subresource refuses a Pod that
    is being deleted. Keep the exception deliberately narrow — deleting, no
    node, and any contradictory observed running state makes the result
    ambiguous — but do not require phase ``Pending``: PodGC's
    unscheduled-terminating sweep flips exactly these Pods to ``Failed``
    before its force delete, and a phase gate would then retain them forever.
    """

    metadata = getattr(pod, "metadata", None)
    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    if getattr(metadata, "deletion_timestamp", None) is None:
        return False
    if _resource_field(spec, "node_name", "nodeName") not in (None, ""):
        return False
    for status_field in (
        "container_statuses",
        "init_container_statuses",
        "ephemeral_container_statuses",
    ):
        raw_statuses = getattr(status, status_field, None)
        if raw_statuses is None:
            continue
        if not isinstance(raw_statuses, (list, tuple)):
            return False
        for container in raw_statuses:
            state = getattr(container, "state", None)
            if state is None:
                return False
            if (
                getattr(state, "running", None) is not None
                or getattr(container, "ready", None) is True
                or getattr(container, "started", None) is True
            ):
                return False
            if (
                _resource_field(container, "container_id", "containerID")
                and getattr(state, "terminated", None) is None
            ):
                return False
    return True


def _pod_has_exact_process_zero(pod: Any) -> bool:
    return _all_pod_container_statuses_terminated(
        pod
    ) or _deleting_pod_was_never_scheduled(pod)


def _container_never_started(container: Any) -> bool:
    state = getattr(container, "state", None)
    last_state = getattr(container, "last_state", None)
    return not (
        getattr(state, "running", None) is not None
        or getattr(state, "terminated", None) is not None
        or getattr(container, "restart_count", 0) not in (0, None)
        or any(
            getattr(last_state, field, None) is not None
            for field in ("waiting", "running", "terminated")
        )
        or getattr(container, "started", None) is True
        or getattr(container, "ready", None) is True
        # The runtime assigns an ID once it creates the container; a pull or
        # config failure never gets that far.
        or _resource_field(container, "container_id", "containerID")
    )


def _pod_never_started_a_container(pod: Any) -> bool:
    """Prove from Pod status that no container of this Pod has run a process.

    Narrower than process-zero: it is a point-in-time fact about a *Pending*
    Pod.  It only decides that a pull-failed Job creation may settle on that
    Pod, and that the Job's terminal cleanup needs no SSH attestation (there
    is no endpoint and nothing to snapshot); the finalizer protocol still
    proves every container terminated before release.  Every reported
    container (regular, init and ephemeral/debug) must never have been
    running or terminated, never restarted, carry no ``lastState`` and no
    container ID, and not report started or ready.  A Pod
    with no container status yet qualifies; any other phase, or unreadable
    status arrays, does not.  Terminated states are rejected even when they
    look synthetic (``ContainerStatusUnknown`` after deletion): terminal
    evidence belongs to the finalizer protocol, not to this shortcut.
    """

    status = getattr(pod, "status", None)
    if getattr(status, "phase", None) != "Pending":
        return False
    for status_field in (
        "container_statuses",
        "init_container_statuses",
        "ephemeral_container_statuses",
    ):
        statuses = getattr(status, status_field, None)
        if statuses is None:
            continue
        if not isinstance(statuses, (list, tuple)):
            return False
        if not all(_container_never_started(container) for container in statuses):
            return False
    return True


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _pvc_name_for(owner: WorkspaceOwner) -> str:
    """Deterministic PVC name for an owner's workspace volume.

    The SINGLE source of truth for both the create side (``create_workspace``)
    and the reclaim side (``delete_workspace_pvc``, and through it the lifecycle
    GC). Neither side may spell the name itself: a create/delete drift is the
    worst failure mode in this file — it silently leaks volumes until the
    storage quota rejects new PVCs with a 403 and provisioning fails closed, or
    it deletes some other owner's data.

    Jobs keep the historical ``pvc-workspace-<id[:12]>``; sessions get
    ``pvc-ws-thread-<id[:12]>``, mirroring the pod-name split in
    ``WorkspaceOwner.pod_name``. Truncation matches the pod name so the two
    stay legible side by side in ``kubectl get pod,pvc``.
    """
    prefix = "pvc-workspace" if owner.kind == "job" else "pvc-ws-thread"
    return f"{prefix}-{owner.id[:12]}"


class ContainerProvisioner:
    """Workspace container provisioner using Kubernetes CoreV1Api.

    Creates per-owner pods (job or session) with SSH server + code-server.
    Workspace storage is ``emptyDir`` by default (dies with the pod). When
    ``WORKSPACE_PVC_ENABLED`` is set, BOTH kinds are PVC-backed (Branch a): the
    volume is named after the owner UUID (``_pvc_name_for``), survives pod
    crashes, and reattaches by that deterministic name on recreate. PVCs are
    reclaimed when the owning work reaches a terminal state (completed/failed/
    cancelled job; a session only once its thread is genuinely done, since an
    ``ended`` thread is still resumable) and retained across idle reaps,
    suspend/restore and crash recovery.
    See knowledge-base/knowledge/features/workspace_pvc_branch_a_implementation.md.
    """

    def __init__(self):
        self._db: Optional[Any] = None
        self._snapshot_service: Optional[Any] = None
        self._core_api: Optional[Any] = None
        self._k8s_available: bool = False
        self._in_cluster: bool = False
        self._namespace: str = os.environ.get(
            "WORKSPACE_NAMESPACE", "superhuman-remote-worker"
        )
        self._workspace_image: str = os.environ.get(
            "WORKSPACE_IMAGE",
            "ghcr.io/superhuman-remote-worker/srw-workspace:latest",
        )
        self._ssh_secret_name: str = os.environ.get(
            "WORKSPACE_SSH_SECRET", "vm-ssh-key"
        )
        self._ssh_auth_ready_timeout: float = float(
            os.environ.get("WORKSPACE_SSH_AUTH_READY_TIMEOUT_S", "30")
        )
        self._ssh_auth_connect_timeout: int = int(
            os.environ.get("WORKSPACE_SSH_AUTH_CONNECT_TIMEOUT_S", "5")
        )
        self._ssh_auth_poll_interval: float = float(
            os.environ.get("WORKSPACE_SSH_AUTH_POLL_INTERVAL_S", "2")
        )
        self._storage_class: str = os.environ.get(
            "WORKSPACE_STORAGE_CLASS", "longhorn-ephemeral"
        )
        # Branch (a): PVC-backed workspaces. Default off → emptyDir (today's
        # behavior). Covers BOTH owner kinds. Sessions were scoped out in v1 on
        # the theory that they rehydrate from Postgres — but Postgres only holds
        # the conversation, not the working tree, and a session's pod is reaped
        # when it goes idle while the thread stays resumable. On emptyDir the
        # user reopens a session whose files silently vanished. The PVC name is
        # deterministic on the owner UUID (``_pvc_name_for``), so a recreated pod
        # reattaches the same volume; GC happens when the owning work is
        # terminal. See knowledge-base/knowledge/features/workspace_pvc_branch_a_implementation.md.
        self._pvc_enabled: bool = _env_flag("WORKSPACE_PVC_ENABLED", False)
        self._pvc_size: str = os.environ.get("WORKSPACE_PVC_SIZE", "10Gi")
        # Single-replica node-loss fallback (Phase 3b). A REATTACH gets this
        # longer ready-wait so a transient node reboot (the replica's node coming
        # back) recovers without discarding data; past it, a still-wedged reattach
        # whose holdup is a volume-attach failure means the lone replica's node is
        # gone — discard the PVC and recover onto a fresh volume (the agent then
        # clones from Gitea + resumes the checkpoint; unpushed files are lost).
        # `_fresh_fallback_enabled` is the kill-switch (the discard is the only
        # data-destructive recovery path, so keep it disablable).
        self._reattach_ready_timeout: int = int(
            os.environ.get("WORKSPACE_REATTACH_READY_TIMEOUT", "180")
        )
        self._fresh_fallback_enabled: bool = _env_flag(
            "WORKSPACE_REATTACH_FRESH_FALLBACK", False
        )
        self._workspace_cleanup_reconciliation_enabled: bool = _env_flag(
            "WORKSPACE_CLEANUP_RECONCILIATION_ENABLED", False
        )
        # rclone-backed cloud workspaces are the default container path. They
        # need FUSE inside the workspace pod because the agent shell runs there.
        self._fuse_enabled: bool = _env_flag("WORKSPACE_FUSE_ENABLED", True)
        # In k3d/containerd and many managed clusters, /dev/fuse + SYS_ADMIN is
        # still not enough because the default runtime profile blocks FUSE
        # mounts. Keep this explicit so restricted deployments can opt out.
        self._fuse_privileged: bool = self._fuse_enabled and _env_flag(
            "WORKSPACE_FUSE_PRIVILEGED", True
        )
        # Slice A1: which images keep the privileged FUSE profile and how long a
        # custom image may take to pull. See sandbox_workspace_settings.
        self._custom_image_policy = SandboxImagePolicy.from_env()
        self._image_pull_timeout: int = self._custom_image_policy.pull_timeout_seconds

    @property
    def is_available(self) -> bool:
        """True if container provisioning is available."""
        return self._k8s_available

    @property
    def in_cluster(self) -> bool:
        """True if connected via in-cluster config (running inside K8s)."""
        return self._in_cluster

    def _image_policy(self) -> SandboxImagePolicy:
        """The installation policy bound to this provisioner's live settings."""
        return replace(
            self._custom_image_policy,
            default_image=self._workspace_image,
            trusted_repositories=self._custom_image_policy.trusted_repositories
            | {image_repository(self._workspace_image)},
            fuse_enabled=self._fuse_enabled,
            fuse_privileged=self._fuse_privileged,
        )

    async def _sandbox_profile(
        self,
        owner: WorkspaceOwner,
        *,
        cpu: str,
        memory: str,
        cpu_limit: str,
        memory_limit: str,
        image: str | None,
    ) -> SandboxPodProfile:
        """Resolve the owner's frozen template settings into pod inputs.

        The snapshot is immutable, so every create, restore, give-up and
        stateless generation of this owner computes the same profile.
        """
        settings = SandboxSettings()
        if self._db is not None and callable(getattr(type(self._db), "fetchrow", None)):
            settings = await resolve_sandbox_settings(self._db, owner.kind, owner.id)
        return sandbox_pod_profile(
            settings,
            self._image_policy(),
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            image=image,
        )

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def _workspace_creation_plan(
        self,
        owner: WorkspaceOwner,
        *,
        profile: SandboxPodProfile,
        stateless_creation_generation: str | None,
    ) -> dict[str, Any]:
        """Resolve every physical input before reserving a generation."""

        strict_stateless = stateless_creation_generation is not None
        seed_files = await self._resolve_ide_seed_files(owner)
        seed_exts = await self._resolve_ide_extensions(owner)
        seed_needs_state = (
            False
            if strict_stateless
            else await self._resolve_ide_needs_state(owner, seed_exts)
        )
        plan: dict[str, Any] = {
            "version": 1,
            "scope": "workspace_container",
            "owner_kind": owner.kind,
            "image": profile.image,
            "cpu": profile.cpu,
            "memory": profile.memory,
            "cpu_limit": profile.cpu_limit,
            "memory_limit": profile.memory_limit,
            "network_tier": await self._resolve_network_tier(
                owner.id, kind=owner.network_tier_kind
            ),
            "pvc": {
                "enabled": self._pvc_enabled,
                "size": (profile.storage or self._pvc_size)
                if self._pvc_enabled
                else None,
                "storage_class": self._storage_class if self._pvc_enabled else None,
            },
            "seed_files": seed_files,
            "seed_extensions": seed_exts,
            "seed_needs_state": seed_needs_state,
            "stateless_generation": stateless_creation_generation,
            "runtime_policy": {
                "ssh_secret_name": self._ssh_secret_name,
                "fuse_enabled": profile.fuse_enabled,
                "fuse_privileged": profile.fuse_privileged,
            },
        }
        extension = profile.plan_extension()
        if extension is not None:
            plan["sandbox"] = extension
        plan["digest"] = _canonical_manifest_digest(plan)
        return plan

    async def _ide_creation_plan(
        self,
        job_id: str,
        *,
        cpu: str,
        memory: str,
        cpu_limit: str,
        memory_limit: str,
    ) -> dict[str, Any]:
        owner = WorkspaceOwner.job(job_id)
        seed_files = await self._resolve_ide_seed_files(owner)
        seed_exts = await self._resolve_ide_extensions(owner)
        plan: dict[str, Any] = {
            "version": 1,
            "scope": "ide",
            "owner_kind": "job",
            "image": self._workspace_image,
            "cpu": cpu,
            "memory": memory,
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "network_tier": await self._resolve_network_tier(job_id, kind="job"),
            "pvc": {"enabled": False},
            "seed_files": seed_files,
            "seed_extensions": seed_exts,
            "seed_needs_state": await self._resolve_ide_needs_state(owner, seed_exts),
            "runtime_policy": {
                "ssh_secret_name": self._ssh_secret_name,
                "fuse_enabled": self._fuse_enabled,
                "fuse_privileged": self._fuse_privileged,
            },
        }
        plan["digest"] = _canonical_manifest_digest(plan)
        return plan

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
        self._init_k8s()

        if self._k8s_available:
            logger.info(
                "Container provisioner ready (namespace=%s, image=%s, in_cluster=%s)",
                self._namespace,
                self._workspace_image,
                self._in_cluster,
            )
        else:
            logger.info(
                "Container provisioner: not available (kubernetes client unavailable)"
            )

    def _init_k8s(self) -> None:
        """Try to initialize the Kubernetes client.

        Important: ``load_kube_config()`` can succeed even when pointing at a
        dead cluster (stale kubeconfig).  We follow up with an actual API call
        (list namespaces) to confirm connectivity.
        """
        if not K8S_AVAILABLE:
            return

        try:
            in_cluster = False
            try:
                k8s_config.load_incluster_config()
                in_cluster = True
            except k8s_config.ConfigException:
                try:
                    k8s_config.load_kube_config()
                except k8s_config.ConfigException:
                    logger.debug(
                        "Kubernetes not available (no in-cluster or kubeconfig)"
                    )
                    return

            api = k8s_client.CoreV1Api()

            # Verify connectivity with a namespace-scoped call (works with
            # Role-based RBAC; the old list_namespace required ClusterRole)
            api.list_namespaced_pod(self._namespace, limit=1, _request_timeout=5)

            self._core_api = api
            self._k8s_available = True
            self._in_cluster = in_cluster
        except Exception as e:
            logger.debug("Container provisioning not available: %s", e)
            self._core_api = None
            self._k8s_available = False

    # =========================================================================
    # Public API
    # =========================================================================

    async def attest_workspace_runtime(
        self, owner: WorkspaceOwner
    ) -> WorkspaceRuntimeAttestation:
        """Attest the exact live backing, Pod, endpoint, and SSH host identity.

        This is deliberately a fresh control-plane read, not a projection of
        ``jobs.context``.  The immutable Pod/PVC/Service/seed ownership checks
        and the host-key exec are performed by ``_trusted_pod_ssh_identity``;
        its post-exec re-reads close deterministic-name replacement races.
        """

        if not self._k8s_available or self._core_api is None:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Kubernetes authority is unavailable"
            )

        expected_network_tier = await self._resolve_network_tier(
            owner.id, kind=owner.network_tier_kind
        )
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod authority probe failed"
            ) from exc

        runtime_incarnation = self._require_workspace_pod_owner(
            pod,
            owner=owner,
            allow_owner_unlabeled=False,
            expected_network_tier=expected_network_tier,
        )
        status = getattr(pod, "status", None)
        pod_ip = str(getattr(status, "pod_ip", "") or "")
        container_statuses = getattr(status, "container_statuses", None)
        if (
            getattr(status, "phase", None) != "Running"
            or not pod_ip
            or not isinstance(container_statuses, (list, tuple))
            or not container_statuses
            or any(
                getattr(item, "ready", None) is not True for item in container_statuses
            )
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod is not exactly Kubernetes-ready"
            )

        pvc_name = self._workspace_pvc_name_from_pod(pod, owner=owner)
        try:
            (
                backing_id,
                fingerprint,
                confirmed_runtime,
            ) = await self._trusted_pod_ssh_identity(
                owner.pod_name,
                pvc_name=pvc_name,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_network_tier=expected_network_tier,
            )
        except WorkspaceRuntimeAuthorityError:
            raise
        except Exception as exc:
            raise WorkspaceRuntimeAuthorityError(
                "workspace SSH identity attestation failed"
            ) from exc

        if confirmed_runtime != runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError("workspace Pod UID changed")
        expected_backing_kind = "pvc" if pvc_name else "pod"
        expected_prefix = f"k8s-{expected_backing_kind}:{self._namespace}:"
        if not backing_id.startswith(expected_prefix):
            raise WorkspaceRuntimeAuthorityError(
                "workspace backing identity is malformed"
            )
        try:
            workspace_generation = _canonical_runtime_uuid(
                backing_id.removeprefix(expected_prefix),
                label="workspace backing UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc
        if re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", fingerprint) is None:
            raise WorkspaceRuntimeAuthorityError(
                "workspace SSH host-key fingerprint is malformed"
            )

        return WorkspaceRuntimeAttestation(
            backing_id=backing_id,
            workspace_generation=workspace_generation,
            runtime_incarnation=runtime_incarnation,
            ssh_host_key_fingerprint=fingerprint,
            host=self._workspace_dns(owner) if pvc_name else pod_ip,
            pod_ip=pod_ip,
        )

    async def create_pinned_thread_workspace(
        self,
        thread_id: str,
        *,
        cpu: str = "500m",
        memory: str = "1Gi",
        cpu_limit: str = "2000m",
        memory_limit: str = "4Gi",
        image: Optional[str] = None,
        runtime_lock_held: bool = False,
    ) -> bool:
        """Create one pinned workspace under its cross-replica lifecycle lock.

        Route reads are not effect authority.  This wrapper refreshes the
        exact ``(T, G, agent, attach, workspace, binding)`` snapshot while the
        same advisory lock used by End/Resume is held, then delegates to the
        row-locked provision-intent admission in :meth:`create_workspace`.
        """

        if self._db is None:
            return False
        if not runtime_lock_held:
            lock_impl = getattr(type(self._db), "thread_advisory_lock", None)
            if not callable(lock_impl):
                return False
            async with lock_impl(self._db, thread_id) as lock_owner:
                if not lock_owner:
                    return False
                return await self.create_pinned_thread_workspace(
                    thread_id,
                    cpu=cpu,
                    memory=memory,
                    cpu_limit=cpu_limit,
                    memory_limit=memory_limit,
                    image=image,
                    runtime_lock_held=True,
                )

        current = await self._db.get_thread(thread_id)
        if (
            not isinstance(current, Mapping)
            or current.get("execution_lane") != "pinned"
        ):
            return False
        try:
            runtime_generation = _canonical_runtime_uuid(
                str(current.get("runtime_generation") or ""),
                label="pinned workspace runtime generation",
            )
        except ValueError:
            return False
        if current.get("runtime_retirement_token") is not None or str(
            current.get("status") or ""
        ) not in {"created", "active", "awaiting_user", "suspended"}:
            return False
        agent_id = current.get("agent_id")
        attach_token = current.get("runtime_attach_token")
        if (agent_id is None) != (attach_token is None):
            return False

        metadata = current.get("metadata")
        metadata = {} if metadata is None else metadata
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                return False
        if not isinstance(metadata, Mapping):
            return False
        raw_workspace = metadata.get("workspace_container")
        raw_binding = metadata.get("_workspace_binding")
        if not all(
            value is None or isinstance(value, Mapping)
            for value in (raw_workspace, raw_binding)
        ):
            return False
        return await self._create_pinned_workspace_legacy(
            WorkspaceOwner.session(thread_id),
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            image=image,
            pinned_runtime_generation=runtime_generation,
            pinned_agent_id=str(agent_id) if agent_id is not None else None,
            pinned_attach_token=(
                str(attach_token) if attach_token is not None else None
            ),
            expected_workspace_context=(
                dict(raw_workspace) if raw_workspace is not None else None
            ),
            expected_binding_context=(
                dict(raw_binding) if raw_binding is not None else None
            ),
            pinned_runtime_lock_held=True,
        )

    async def create_workspace(
        self,
        owner: WorkspaceOwner,
        cpu: str = "500m",
        memory: str = "1Gi",
        cpu_limit: str = "2000m",
        memory_limit: str = "4Gi",
        image: Optional[str] = None,
        fresh: bool = False,
        stateless_creation_generation: str | None = None,
        allow_stateless_create: bool = False,
        operation_kind: Literal["create", "restore", "reattach", "adopt"] = "create",
        operation_id: str | None = None,
    ) -> bool:
        """Create under one durable DB reservation held across Kubernetes I/O."""

        if not self._k8s_available or self._db is None:
            return False
        # The former node-loss fallback deleted a deterministic PVC without
        # process-zero or exact-volume recovery authority.  Keep this callable
        # surface fail closed until a distinct fresh-recovery protocol exists.
        if fresh:
            return False
        reserve = getattr(
            type(self._db),
            "reserve_managed_repository_workspace_creation",
            None,
        )
        settle = getattr(
            type(self._db),
            "settle_managed_repository_workspace_creation_reservation",
            None,
        )
        if not callable(reserve) or not callable(settle):
            return False
        # The stateless generation is already the durable, one-shot operation
        # identity.  Reuse it as the reservation claimant so a lost create
        # response can re-enter the same generation instead of waiting for a
        # random claimant's lease to expire.
        if operation_id is None and stateless_creation_generation is not None:
            operation_id = stateless_creation_generation
        if operation_id is not None:
            try:
                operation_id = _canonical_runtime_uuid(
                    operation_id, label="workspace creation operation"
                )
            except ValueError:
                return False
        claimant = (
            f"container-{operation_kind}:{operation_id}"
            if operation_id is not None
            else f"container-{operation_kind}:{uuid4()}"
        )
        try:
            profile = await self._sandbox_profile(
                owner,
                cpu=cpu,
                memory=memory,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                image=image,
            )
        except Exception:
            logger.exception(
                "Could not read the frozen workspace settings for %s %s",
                owner.kind,
                owner.id,
            )
            return False
        creation_plan = await self._workspace_creation_plan(
            owner,
            profile=profile,
            stateless_creation_generation=stateless_creation_generation,
        )
        reservation = await reserve(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            claimant=claimant,
            lease_seconds=1800,
            operation_kind=operation_kind,
            desired_manifest_digest=str(creation_plan["digest"]),
        )
        if not isinstance(reservation, dict):
            return False
        # The reservation is durable before any Kubernetes side effect.  The
        # dedicated session-level advisory guard then stays held until every
        # accepted UID has been persisted, so cleanup/cancellation cannot
        # observe a half-published generation while a client thread is still
        # completing an ambiguous API call.
        async with self._workspace_mutation_guard(
            owner, scope="workspace_container"
        ) as mutation_owned:
            if not mutation_owned:
                return False
            created = await self._create_workspace_reserved(
                owner,
                cpu=cpu,
                memory=memory,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                image=image,
                fresh=fresh,
                stateless_creation_generation=stateless_creation_generation,
                allow_stateless_create=allow_stateless_create,
                _creation_reservation=reservation,
                _creation_plan=creation_plan,
                _creation_profile=profile,
            )
        if not created:
            if reservation.get("external_mutation_started_at") is None:
                abort = getattr(
                    type(self._db),
                    "abort_managed_repository_workspace_creation_reservation",
                    None,
                )
                if callable(abort):
                    await abort(
                        self._db,
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope="workspace_container",
                        reservation_generation=int(
                            reservation["reservation_generation"]
                        ),
                        claimant=str(reservation["claimed_by"]),
                        claim_token=int(reservation["claim_token"]),
                    )
            return False
        runtime = reservation.get("runtime_incarnation")
        if runtime is None:
            return False
        return bool(
            await settle(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope="workspace_container",
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
                runtime_incarnation=str(runtime),
            )
        )

    async def get_workspace_creation_result(
        self,
        owner: WorkspaceOwner,
        *,
        operation_kind: Literal["create", "restore", "reattach", "adopt"],
        operation_id: str,
    ) -> dict[str, Any] | None:
        """Read the exact durable generation used by a retrying operation."""

        if self._db is None:
            return None
        try:
            operation_id = _canonical_runtime_uuid(
                operation_id, label="workspace creation operation"
            )
        except ValueError:
            return None
        read = getattr(
            type(self._db),
            "get_managed_repository_workspace_creation_result",
            None,
        )
        if not callable(read):
            return None
        return await read(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            claimant=f"container-{operation_kind}:{operation_id}",
            operation_kind=operation_kind,
        )

    async def get_current_workspace_creation_result(
        self,
        owner: WorkspaceOwner,
        *,
        operation_kind: Literal["create", "restore", "reattach", "adopt"],
    ) -> dict[str, Any] | None:
        """Read only the creation row named by the current runtime markers."""

        if self._db is None:
            return None
        read = getattr(
            type(self._db),
            "get_current_managed_repository_workspace_creation_result",
            None,
        )
        if not callable(read):
            return None
        return await read(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            operation_kind=operation_kind,
        )

    async def claim_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        claimant: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        """Lease exact-B workspace extraction work across replicas."""

        return await self._claim_restore_work(
            owner,
            scope="workspace_container",
            claimant=claimant,
            lease_seconds=lease_seconds,
        )

    async def claim_ide_restore_work(
        self,
        job_id: str,
        *,
        claimant: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        """Lease exact-B IDE repository/profile work across replicas."""

        return await self._claim_restore_work(
            WorkspaceOwner.job(job_id),
            scope="ide",
            claimant=claimant,
            lease_seconds=lease_seconds,
        )

    async def _claim_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        scope: Literal["workspace_container", "ide"],
        claimant: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if self._db is None or (owner.kind == "session" and scope == "ide"):
            return None
        claim = getattr(
            type(self._db),
            "claim_current_managed_repository_workspace_restore_work",
            None,
        )
        if not callable(claim):
            return None
        return await claim(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            claimant=claimant,
            lease_seconds=lease_seconds,
        )

    async def release_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        retry_seconds: int = 30,
    ) -> bool:
        return await self._release_restore_work(
            owner,
            scope="workspace_container",
            restore_work=restore_work,
            claimant=claimant,
            retry_seconds=retry_seconds,
        )

    async def renew_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        return await self._renew_restore_work(
            owner,
            scope="workspace_container",
            restore_work=restore_work,
            claimant=claimant,
            lease_seconds=lease_seconds,
        )

    async def renew_ide_restore_work(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any] | None:
        return await self._renew_restore_work(
            WorkspaceOwner.job(job_id),
            scope="ide",
            restore_work=restore_work,
            claimant=claimant,
            lease_seconds=lease_seconds,
        )

    async def _renew_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        scope: Literal["workspace_container", "ide"],
        restore_work: dict[str, Any],
        claimant: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        renew = getattr(
            type(self._db),
            "renew_managed_repository_workspace_restore_work",
            None,
        )
        if not callable(renew):
            return None
        try:
            runtime = str(restore_work["runtime_incarnation"])
            reservation_id = str(restore_work["id"])
            work_claim_token = int(restore_work["restore_work_claim_token"])
        except (KeyError, TypeError, ValueError):
            return None
        return await renew(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            reservation_id=reservation_id,
            runtime_incarnation=runtime,
            claimant=claimant,
            work_claim_token=work_claim_token,
            lease_seconds=lease_seconds,
        )

    async def release_ide_restore_work(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        retry_seconds: int = 30,
    ) -> bool:
        return await self._release_restore_work(
            WorkspaceOwner.job(job_id),
            scope="ide",
            restore_work=restore_work,
            claimant=claimant,
            retry_seconds=retry_seconds,
        )

    async def _release_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        scope: Literal["workspace_container", "ide"],
        restore_work: dict[str, Any],
        claimant: str,
        retry_seconds: int,
    ) -> bool:
        release = getattr(
            type(self._db),
            "release_managed_repository_workspace_restore_work",
            None,
        )
        if not callable(release):
            return False
        try:
            runtime = str(restore_work["runtime_incarnation"])
            reservation_id = str(restore_work["id"])
            work_claim_token = int(restore_work["restore_work_claim_token"])
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            await release(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_id=reservation_id,
                runtime_incarnation=runtime,
                claimant=claimant,
                work_claim_token=work_claim_token,
                retry_seconds=retry_seconds,
            )
        )

    async def complete_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        success: bool,
        error: str | None = None,
    ) -> bool:
        return await self._complete_restore_work(
            owner,
            scope="workspace_container",
            restore_work=restore_work,
            claimant=claimant,
            result_kind="ready" if success else "failed",
            error=error,
        )

    async def complete_strict_thread_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        workspace_generation: str,
        endpoint_generation: str,
        backing_id: str,
        host_key_fingerprint: str,
        pod_ip: str,
        port: int,
        expected_workspace_status: str,
    ) -> bool:
        """Settle restore work with the complete strict thread authority tuple."""

        if owner.kind != "session":
            return False
        complete = getattr(
            type(self._db),
            "complete_stateless_thread_workspace_restore_work",
            None,
        )
        if not callable(complete):
            return False
        try:
            runtime = str(restore_work["runtime_incarnation"])
            reservation_id = str(restore_work["id"])
            work_claim_token = int(restore_work["restore_work_claim_token"])
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            await complete(
                self._db,
                owner.id,
                reservation_id=reservation_id,
                runtime_incarnation=runtime,
                claimant=claimant,
                work_claim_token=work_claim_token,
                workspace_generation=workspace_generation,
                endpoint_generation=endpoint_generation,
                backing_id=backing_id,
                host_key_fingerprint=host_key_fingerprint,
                pod_ip=pod_ip,
                port=port,
                expected_workspace_status=expected_workspace_status,
            )
        )

    async def complete_ide_restore_work(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        success: bool,
        code_server_url: str | None = None,
        last_activity: str | None = None,
        error: str | None = None,
    ) -> bool:
        return await self._complete_restore_work(
            WorkspaceOwner.job(job_id),
            scope="ide",
            restore_work=restore_work,
            claimant=claimant,
            result_kind="active" if success else "failed",
            code_server_url=code_server_url,
            last_activity=last_activity,
            error=error,
        )

    async def _complete_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        scope: Literal["workspace_container", "ide"],
        restore_work: dict[str, Any],
        claimant: str,
        result_kind: Literal["ready", "active", "failed"],
        code_server_url: str | None = None,
        last_activity: str | None = None,
        error: str | None = None,
    ) -> bool:
        complete = getattr(
            type(self._db),
            "complete_managed_repository_workspace_restore_work",
            None,
        )
        if not callable(complete):
            return False
        try:
            runtime = str(restore_work["runtime_incarnation"])
            reservation_id = str(restore_work["id"])
            work_claim_token = int(restore_work["restore_work_claim_token"])
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            await complete(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_id=reservation_id,
                runtime_incarnation=runtime,
                claimant=claimant,
                work_claim_token=work_claim_token,
                result_kind=result_kind,
                code_server_url=code_server_url,
                last_activity=last_activity,
                error=error,
            )
        )

    async def _start_workspace_creation_reservation(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
    ) -> bool:
        start = getattr(
            type(self._db),
            "mark_managed_repository_workspace_creation_started",
            None,
        )
        if not callable(start):
            return False
        started = await start(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
        )
        if not isinstance(started, dict):
            return False
        reservation.update(started)
        return True

    @asynccontextmanager
    async def _workspace_mutation_guard(
        self,
        owner: WorkspaceOwner,
        *,
        scope: Literal["workspace_container", "ide"],
    ):
        """Own the cross-replica physical-mutation domain for one scope."""

        lock = getattr(type(self._db), "workspace_runtime_mutation_lock", None)
        if not callable(lock):
            yield False
            return
        async with lock(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            wait=True,
            wait_timeout_s=120.0,
        ) as acquired:
            yield bool(acquired)

    async def _begin_workspace_creation_effect(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        resource_kind: Literal["pod", "seed", "pvc", "service"],
    ) -> bool:
        begin = getattr(
            type(self._db),
            "begin_managed_repository_workspace_creation_effect",
            None,
        )
        if not callable(begin):
            return False
        row = await begin(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
            resource_kind=resource_kind,
            ambiguity_seconds=90,
        )
        if not isinstance(row, dict):
            return False
        reservation.update(row)
        return True

    async def _workspace_cleanup_automatic_admission_is_safe(self) -> bool:
        """Require both the rollout flag and a clean server-owned inventory."""

        if not self._workspace_cleanup_reconciliation_enabled or self._db is None:
            return False
        inventory = getattr(
            type(self._db),
            "managed_repository_workspace_cleanup_activation_inventory",
            None,
        )
        if not callable(inventory):
            return False
        observed = await inventory(self._db)
        return bool(isinstance(observed, dict) and observed.get("safe") is True)

    @asynccontextmanager
    async def _workspace_creation_effect(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        resource_kind: Literal["pod", "seed", "pvc", "service"],
    ):
        # The public creation entry points hold the owner/scope guard for the
        # complete generation.  This small context records the particular
        # external edge immediately before it is crossed; reacquiring the same
        # session advisory lock on another connection here would deadlock.
        if not await self._begin_workspace_creation_effect(
            owner,
            reservation,
            scope=scope,
            resource_kind=resource_kind,
        ):
            yield False
            return
        yield True

    @staticmethod
    async def _bounded_kubernetes_call(
        call: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        """Bound and join one authority-sensitive Kubernetes client call.

        ``asyncio.to_thread`` does not stop its worker when the waiter is
        cancelled.  The shared join helper absorbs repeated cancellation until
        the worker is terminal, so an advisory owner/scope guard cannot be
        released while the external effect is still able to commit.
        """

        kwargs.setdefault("_request_timeout", (5, 30))
        return await joined_blocking_call(call, *args, **kwargs)

    @staticmethod
    async def _bounded_kubernetes_mutation(
        call: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        """Semantic mutation alias for the bounded, joined call runner."""

        return await ContainerProvisioner._bounded_kubernetes_call(
            call, *args, **kwargs
        )

    async def _workspace_creation_reservation_is_current(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
    ) -> bool:
        check = getattr(
            type(self._db),
            "managed_repository_workspace_creation_claim_is_current",
            None,
        )
        return bool(
            callable(check)
            and await check(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
            )
        )

    async def _authorize_workspace_creation_runtime(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        runtime_incarnation: str,
    ) -> bool:
        authorize = getattr(
            type(self._db),
            "authorize_managed_repository_workspace_creation_runtime",
            None,
        )
        authorized = bool(
            callable(authorize)
            and await authorize(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
                runtime_incarnation=runtime_incarnation,
            )
        )
        if authorized:
            reservation["runtime_incarnation"] = runtime_incarnation
            reservation["pod_uid"] = runtime_incarnation
            reservation["phase"] = "runtime_bound"
        return authorized

    async def _authorize_cancelled_creation_runtime(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        runtime_incarnation: str,
    ) -> bool:
        authorize = getattr(
            type(self._db),
            "authorize_cancelled_workspace_creation_runtime_for_reconciliation",
            None,
        )
        authorized = bool(
            callable(authorize)
            and await authorize(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
                runtime_incarnation=runtime_incarnation,
            )
        )
        if authorized:
            reservation["runtime_incarnation"] = runtime_incarnation
            reservation["pod_uid"] = runtime_incarnation
            reservation["phase"] = "runtime_bound"
        return authorized

    async def _record_workspace_creation_resources(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        runtime_incarnation: str,
        pod_name: str,
        seed_configmap: str | None,
        pvc_name: str | None,
    ) -> bool:
        if not await self._workspace_creation_reservation_is_current(
            owner, reservation, scope=scope
        ):
            return False
        try:
            seed_uid = None
            if seed_configmap is not None:
                seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_configmap,
                    namespace=self._namespace,
                )
                seed_uid = self._require_stateless_seed_configmap_identity(
                    seed,
                    owner=owner,
                    pod_name=pod_name,
                    creation_reservation_id=str(reservation["id"]),
                )
                self._require_seed_configmap_pod_owner_reference(
                    seed,
                    pod_name=pod_name,
                    runtime_incarnation=runtime_incarnation,
                )
            pvc_uid = None
            service_uid = None
            if pvc_name is not None:
                claim = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
                pvc_uid = self._require_stateless_pvc_identity(
                    claim,
                    owner=owner,
                    pvc_name=pvc_name,
                    # The exact Pod is the immutable storage-plan authority.
                    # A retry may run after the configured StorageClass
                    # changes; recording its already-bound PVC must not
                    # reinterpret that rollout drift as foreign ownership.
                    allow_any_storage_class=True,
                )
                service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
                service_uid = self._require_stateless_service_identity(
                    service,
                    owner=owner,
                )
        except Exception:
            return False
        record = getattr(
            type(self._db),
            "record_managed_repository_workspace_creation_resources",
            None,
        )
        if not callable(record):
            return False
        recorded = await record(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=runtime_incarnation,
            seed_configmap_uid=seed_uid,
            pvc_uid=pvc_uid,
            service_uid=service_uid,
        )
        if recorded:
            reservation.update(
                {
                    "seed_configmap_uid": seed_uid,
                    "pvc_uid": pvc_uid,
                    "service_uid": service_uid,
                }
            )
        return bool(recorded)

    async def _record_workspace_creation_resource(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        resource_kind: Literal["pod", "seed", "pvc", "service"],
        resource_uid: str,
    ) -> bool:
        record = getattr(
            type(self._db),
            "record_managed_repository_workspace_creation_resource",
            None,
        )
        if not callable(record):
            return False
        recorded = await record(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope=scope,
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
            resource_kind=resource_kind,
            resource_uid=resource_uid,
        )
        if recorded:
            reservation[
                {
                    "pod": "pod_uid",
                    "seed": "seed_configmap_uid",
                    "pvc": "pvc_uid",
                    "service": "service_uid",
                }[resource_kind]
            ] = resource_uid
            if resource_kind == "pod":
                reservation["runtime_incarnation"] = resource_uid
        return bool(recorded)

    async def _cancelled_creation_claim_is_current(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
    ) -> bool:
        check = getattr(
            type(self._db),
            "managed_repository_workspace_creation_reconciliation_claim_is_current",
            None,
        )
        return bool(
            callable(check)
            and await check(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
            )
        )

    async def _terminal_cancelled_creation_claim_is_current(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
    ) -> bool:
        check = getattr(
            type(self._db),
            "terminal_cancelled_workspace_creation_claim_is_current",
            None,
        )
        return bool(
            callable(check)
            and await check(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope="workspace_container",
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
            )
        )

    async def _record_cancelled_creation_resource(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        scope: Literal["workspace_container", "ide"],
        resource_kind: Literal["pod", "seed", "pvc", "service"],
        resource_uid: str,
    ) -> bool:
        record = getattr(
            type(self._db),
            "record_cancelled_workspace_creation_resource_for_reconciliation",
            None,
        )
        recorded = bool(
            callable(record)
            and await record(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
                resource_kind=resource_kind,
                resource_uid=resource_uid,
            )
        )
        if recorded:
            reservation[
                {
                    "pod": "pod_uid",
                    "seed": "seed_configmap_uid",
                    "pvc": "pvc_uid",
                    "service": "service_uid",
                }[resource_kind]
            ] = resource_uid
            if resource_kind == "pod":
                reservation["runtime_incarnation"] = resource_uid
        return recorded

    async def _create_workspace_reserved(
        self,
        owner: WorkspaceOwner,
        cpu: str = "500m",
        memory: str = "1Gi",
        cpu_limit: str = "2000m",
        memory_limit: str = "4Gi",
        image: Optional[str] = None,
        fresh: bool = False,
        stateless_creation_generation: str | None = None,
        allow_stateless_create: bool = False,
        _creation_reservation: dict[str, Any] | None = None,
        _creation_plan: dict[str, Any] | None = None,
        _creation_profile: SandboxPodProfile | None = None,
    ) -> bool:
        """Create a workspace container for a job or persistent thread.

        Args:
            owner: WorkspaceOwner identifying the job or session.
            cpu: CPU request.
            memory: Memory request.
            cpu_limit: CPU limit.
            memory_limit: Memory limit.
            image: Workspace image override (defaults to WORKSPACE_IMAGE env).
            fresh: Reserved for a future exact fresh-recovery authority.  It is
                rejected by the public entry point.

        Returns:
            True if pod creation succeeded, False otherwise.
        """
        if not self._k8s_available:
            return False
        if not isinstance(_creation_reservation, dict):
            return False
        if not isinstance(_creation_plan, dict) or str(
            _creation_plan.get("digest") or ""
        ) != str(_creation_reservation.get("desired_manifest_digest") or ""):
            return False

        strict_stateless = stateless_creation_generation is not None
        cpu = str(_creation_plan["cpu"])
        memory = str(_creation_plan["memory"])
        cpu_limit = str(_creation_plan["cpu_limit"])
        memory_limit = str(_creation_plan["memory_limit"])
        if not strict_stateless and owner.kind == "session":
            lane_impl = (
                getattr(
                    type(self._db),
                    "stateless_thread_workspace_creation_requires_authority",
                    None,
                )
                if self._db is not None
                else None
            )
            try:
                requires_authority = (
                    await lane_impl(self._db, owner.id) if callable(lane_impl) else None
                )
            except Exception:
                requires_authority = None
            if requires_authority is not False:
                logger.error(
                    "Direct workspace create refused without stateless nonce "
                    "authority for thread %s",
                    owner.id,
                )
                return False
        if strict_stateless:
            if owner.kind != "session" or self._db is None:
                return False
            if fresh:
                return False
            try:
                stateless_creation_generation = _canonical_runtime_uuid(
                    stateless_creation_generation,
                    label="stateless workspace creation generation",
                )
            except ValueError:
                return False
            if type(allow_stateless_create) is not bool:
                return False
            if not allow_stateless_create:
                if not await self._start_workspace_creation_reservation(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                ):
                    return False
                return await self.continue_stateless_workspace_creation(
                    owner,
                    generation=stateless_creation_generation,
                    expected_runtime_incarnation=None,
                    cpu=cpu,
                    memory=memory,
                    cpu_limit=cpu_limit,
                    memory_limit=memory_limit,
                    image=image,
                    _creation_reservation=_creation_reservation,
                )
            validate_impl = getattr(
                type(self._db),
                "validate_stateless_thread_workspace_creation_attempt",
                None,
            )
            if not callable(validate_impl) or not await validate_impl(
                self._db,
                owner.id,
                generation=stateless_creation_generation,
                attempted=False,
            ):
                return False

        if not await self._start_workspace_creation_reservation(
            owner,
            _creation_reservation,
            scope="workspace_container",
        ):
            return False

        async def mutation_authority() -> bool:
            return await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="workspace_container",
            )

        pod_name = owner.pod_name
        workspace_image = str(_creation_plan["image"])
        network_tier = str(_creation_plan["network_tier"])

        # Workspace storage (Branch a): PVC-backed for BOTH owner kinds when
        # WORKSPACE_PVC_ENABLED — the volume is named by the owner UUID, survives
        # pod crashes, and reattaches by that deterministic name on recreate
        # (drift recovery, suspend/restore, give_up all funnel back through
        # create_workspace, so 409-reuse here IS the resume path). Sessions need
        # this at least as much as jobs: their pod is reaped on idle while the
        # thread stays resumable, so emptyDir destroys the working tree of state
        # a user can still reopen. Otherwise emptyDir — storage dies with the
        # pod; isolation is the pod boundary. Created BEFORE the seed ConfigMap
        # so a PVC failure leaves nothing to clean up — it is the provisioning
        # prerequisite (and it fails closed: never silently downgrade to
        # emptyDir, that would trade a visible failure for silent data loss).
        pvc_name: Optional[str] = None
        pvc_reattach = False
        pvc_plan = _creation_plan.get("pvc") or {}
        if pvc_plan.get("enabled") is True:
            pvc_name = _pvc_name_for(owner)
            async with self._workspace_creation_effect(
                owner,
                _creation_reservation,
                scope="workspace_container",
                resource_kind="pvc",
            ) as effect_owned:
                if not effect_owned:
                    return False
                pvc_status = await self._create_pvc(
                    pvc_name,
                    size=str(pvc_plan["size"]),
                    storage_class=str(pvc_plan["storage_class"]),
                    labels={owner.label_key: owner.id},
                    expected_owner=owner,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    mutation_authority=mutation_authority,
                )
                if not pvc_status:
                    return False
                try:
                    reservation_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    reservation_pvc_uid = self._require_stateless_pvc_identity(
                        reservation_claim,
                        owner=owner,
                        pvc_name=pvc_name,
                        allow_any_storage_class=True,
                    )
                except Exception:
                    return False
                if not await self._record_workspace_creation_resource(
                    owner=owner,
                    reservation=_creation_reservation,
                    scope="workspace_container",
                    resource_kind="pvc",
                    resource_uid=reservation_pvc_uid,
                ):
                    return False
            # "reused" (409) = an EXISTING volume reattached — the only case where
            # the single-replica node-loss wedge can occur. `fresh` recreates a NEW
            # volume, so it is never itself a reattach (prevents the fallback below
            # from re-firing → no recursion).
            pvc_reattach = pvc_status == "reused" and not fresh

        # Seed the user's saved code-server config (theme/keybindings/snippets)
        # into the pod before it starts. Best-effort — never blocks provisioning.
        seed_files = _creation_plan.get("seed_files") or {}
        seed_exts = _creation_plan.get("seed_extensions") or {}
        seed_needs_state = bool(_creation_plan.get("seed_needs_state"))
        if not await self._workspace_creation_reservation_is_current(
            owner,
            _creation_reservation,
            scope="workspace_container",
        ):
            return False
        if seed_files or seed_exts:
            if not await self._begin_workspace_creation_effect(
                owner,
                _creation_reservation,
                scope="workspace_container",
                resource_kind="seed",
            ):
                return False
        try:
            seed_cm = await self._create_seed_configmap(
                pod_name,
                seed_files,
                seed_exts,
                needs_state=seed_needs_state,
                expected_owner=owner,
                expected_creation_generation=(
                    stateless_creation_generation if strict_stateless else None
                ),
                creation_reservation_id=str(_creation_reservation["id"]),
                mutation_authority=mutation_authority,
            )
        except WorkspaceRuntimeAuthorityError as error:
            logger.error(
                "Stateless seed ConfigMap preparation failed for %s: %s",
                owner.id,
                error,
            )
            return False
        if seed_cm is not None:
            try:
                reservation_seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_cm,
                    namespace=self._namespace,
                )
                reservation_seed_uid = self._require_stateless_seed_configmap_identity(
                    reservation_seed,
                    owner=owner,
                    pod_name=pod_name,
                    creation_reservation_id=str(_creation_reservation["id"]),
                )
            except Exception:
                return False
            if not await self._record_workspace_creation_resource(
                owner,
                _creation_reservation,
                scope="workspace_container",
                resource_kind="seed",
                resource_uid=reservation_seed_uid,
            ):
                return False

        pod_manifest = self._build_pod_manifest(
            pod_name=pod_name,
            owner=owner,
            image=workspace_image,
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            network_tier=network_tier,
            pvc_name=pvc_name,
            seed_configmap=seed_cm,
            stateless_creation_generation=stateless_creation_generation,
            creation_reservation_id=str(_creation_reservation["id"]),
            profile=_creation_profile,
        )

        # Once the durable false->true attempt CAS is submitted, its outcome is
        # the authority boundary for the one permitted Kubernetes create. A
        # timeout/cancellation/non-409 error can occur after the apiserver has
        # accepted an exact Pod that already references this ConfigMap. Never
        # delete the seed on that ambiguous side of the edge: all retries are
        # read/adopt-only and cannot recreate it. Exact terminal workspace
        # cleanup owns the deterministic ConfigMap thereafter.
        reused_existing_pod = False
        metered_cpu, metered_memory = cpu, memory
        try:
            if not await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="workspace_container",
            ):
                return False
            if not await self._begin_workspace_creation_effect(
                owner,
                _creation_reservation,
                scope="workspace_container",
                resource_kind="pod",
            ):
                return False
            if strict_stateless:
                claim_impl = getattr(
                    type(self._db),
                    "claim_stateless_thread_workspace_creation_attempt",
                    None,
                )
                if not callable(claim_impl):
                    return False
                if not await claim_impl(
                    self._db,
                    owner.id,
                    generation=stateless_creation_generation,
                ):
                    return False
                created_pod = await self._create_stateless_pod_once(
                    owner,
                    pod_manifest,
                    generation=stateless_creation_generation,
                    expected_network_tier=network_tier,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=seed_cm,
                    mutation_authority=mutation_authority,
                )
                if created_pod is None:
                    return False
                runtime_incarnation = self._require_stateless_pod_identity(
                    created_pod,
                    owner=owner,
                    generation=stateless_creation_generation,
                    expected_network_tier=network_tier,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=seed_cm,
                )
                self._require_workspace_creation_reservation_annotation(
                    created_pod,
                    reservation_id=str(_creation_reservation["id"]),
                )
                metered_cpu, metered_memory = (
                    self._workspace_resource_requests_from_pod(created_pod)
                )
                if not await self._authorize_workspace_creation_runtime(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                    runtime_incarnation=runtime_incarnation,
                ):
                    return False
                publish_impl = getattr(
                    type(self._db),
                    "publish_stateless_thread_workspace_runtime",
                    None,
                )
                if not callable(publish_impl) or not await publish_impl(
                    self._db,
                    owner.id,
                    generation=stateless_creation_generation,
                    runtime_incarnation=runtime_incarnation,
                    pod_name=pod_name,
                    namespace=self._namespace,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    creation_claim_token=int(_creation_reservation["claim_token"]),
                ):
                    logger.error(
                        "Lost stateless runtime publication authority for thread %s",
                        owner.id,
                    )
                    return False
            else:
                (
                    created_pod,
                    reused_existing_pod,
                ) = await self._create_pod_resolving_teardown(
                    pod_manifest,
                    pod_name,
                    owner=owner,
                    mutation_authority=mutation_authority,
                )
                if created_pod is None:
                    return False
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                )
                self._require_workspace_creation_reservation_annotation(
                    created_pod,
                    reservation_id=str(_creation_reservation["id"]),
                )
                if not await self._authorize_workspace_creation_runtime(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                    runtime_incarnation=runtime_incarnation,
                ):
                    return False
                observed_seed = self._require_stateless_pod_storage_binding(
                    created_pod,
                    owner=owner,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=(
                        _UNSPECIFIED_RESOURCE_BINDING
                        if reused_existing_pod
                        else seed_cm
                    ),
                )
                if reused_existing_pod:
                    if observed_seed is not None:
                        seed_configmap = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_config_map,
                            name=observed_seed,
                            namespace=self._namespace,
                        )
                        self._require_stateless_seed_configmap_identity(
                            seed_configmap,
                            owner=owner,
                            pod_name=pod_name,
                            creation_reservation_id=str(_creation_reservation["id"]),
                        )
                        seed_cm = observed_seed
                    elif seed_cm is not None:
                        return False
            # Make the pod own the seed ConfigMap so K8s GCs it on teardown.
            # created_pod is None only for the idempotent live-pod case, where
            # the existing pod already owns its (same-named) ConfigMap.
            if created_pod is not None:
                if not await self._workspace_creation_reservation_is_current(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                ):
                    return False
                adopted_seed = await self._adopt_configmap(
                    seed_cm,
                    created_pod,
                    expected_owner=owner,
                    expected_creation_generation=(
                        stateless_creation_generation if strict_stateless else None
                    ),
                    creation_reservation_id=str(_creation_reservation["id"]),
                    mutation_authority=mutation_authority,
                )
                if adopted_seed is not True:
                    return False
            # PVC-backed workspaces (jobs AND sessions) get a stable headless
            # Service so the agent dials a constant DNS name that survives pod
            # recreates (reattach/recovery) instead of an ephemeral pod IP. Same
            # gate as the PVC; kept across idle reaps, dropped on release.
            if pvc_name:
                if not await self._workspace_creation_reservation_is_current(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                ):
                    return False
                if not await self._begin_workspace_creation_effect(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                    resource_kind="service",
                ):
                    return False
                service_created = await self._create_service(
                    owner,
                    require_exact_owner=True,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    mutation_authority=mutation_authority,
                )
                if not service_created:
                    # Ready publishes this stable DNS name. Preserve the exact
                    # UID+attempt marker until the idempotent Service exists;
                    # continuation retries it without another Pod create.
                    return False
                reservation_service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
                reservation_service_uid = self._require_stateless_service_identity(
                    reservation_service, owner=owner
                )
                if not await self._record_workspace_creation_resource(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                    resource_kind="service",
                    resource_uid=reservation_service_uid,
                ):
                    return False
            if not await self._record_workspace_creation_resources(
                owner,
                _creation_reservation,
                scope="workspace_container",
                runtime_incarnation=runtime_incarnation,
                pod_name=pod_name,
                seed_configmap=seed_cm,
                pvc_name=pvc_name,
            ):
                return False
            logger.info(
                "Workspace container created: %s (%s %s)",
                pod_name,
                owner.kind,
                owner.id,
            )
            if not strict_stateless:
                if not await self._workspace_creation_reservation_is_current(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                ) or not await self._set_context(
                    owner,
                    {
                        "status": "created",
                        "provisioner": "k8s",
                        "pod_name": pod_name,
                        "namespace": self._namespace,
                        WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                        WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY: str(
                            _creation_reservation["id"]
                        ),
                        WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY: str(
                            _creation_reservation["claim_token"]
                        ),
                        **(
                            {
                                CANVAS_WORKSPACE_GENERATION_KEY: None,
                            }
                            if owner.kind == "session"
                            else {}
                        ),
                    },
                ):
                    return False

            # Open a compute-metering interval (Slice 4b) — best-effort, billed on
            # the accepted Pod's immutable requests for strict stateless
            # runtimes. Admission may mutate those values, and a later retry
            # must not bill using caller/config drift. Pinned/job behavior keeps
            # its legacy requested-value semantics.
            await workspace_metering.open_interval(
                self._db,
                owner,
                tier="sandbox",
                cpu=metered_cpu,
                memory=metered_memory,
            )

            # Wait for pod IP. A reattach gets the longer window so a transient
            # node reboot recovers without discarding data; a fresh create keeps
            # the standard 120s.
            ready_timeout = self._reattach_ready_timeout if pvc_reattach else 120
            pod_ip = await self._wait_for_ready(
                pod_name,
                timeout=ready_timeout,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_creation_generation=(
                    stateless_creation_generation if strict_stateless else None
                ),
                expected_network_tier=network_tier,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=seed_cm,
                pull_image=(
                    _creation_profile.image
                    if _creation_profile is not None
                    and _creation_profile.image != self._workspace_image
                    else None
                ),
            )
            if pod_ip:
                if seed_cm is not None:
                    ready_seed = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=seed_cm,
                        namespace=self._namespace,
                    )
                    self._require_stateless_seed_configmap_identity(
                        ready_seed,
                        owner=owner,
                        generation=(
                            stateless_creation_generation if strict_stateless else None
                        ),
                        pod_name=pod_name,
                    )
                    self._require_seed_configmap_pod_owner_reference(
                        ready_seed,
                        pod_name=pod_name,
                        runtime_incarnation=runtime_incarnation,
                    )
                if pvc_name is not None:
                    ready_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    self._require_stateless_pvc_identity(
                        ready_claim,
                        owner=owner,
                        pvc_name=pvc_name,
                    )
                    ready_service = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_service,
                        name=owner.pod_name,
                        namespace=self._namespace,
                    )
                    self._require_stateless_service_identity(
                        ready_service,
                        owner=owner,
                    )
                canvas_generation = None
                if owner.kind == "session" and self._db:
                    try:
                        # Now that sessions can be PVC-backed, this resolves the
                        # trusted identity to the VOLUME (`k8s-pvc:…`) rather
                        # than the pod — which is the point: the backing_id (and
                        # so the Canvas workspace_generation) stops churning on
                        # every pod recreate. A session that predates the PVC
                        # switch re-binds once, from pod UID to PVC UID.
                        (
                            backing_id,
                            fingerprint,
                            trusted_runtime_incarnation,
                        ) = await self._trusted_pod_ssh_identity(
                            pod_name,
                            pvc_name=pvc_name,
                            expected_owner=owner,
                            expected_runtime_incarnation=runtime_incarnation,
                            expected_creation_generation=(
                                stateless_creation_generation
                                if strict_stateless
                                else None
                            ),
                            expected_network_tier=network_tier,
                            expected_seed_configmap=seed_cm,
                        )
                        if strict_stateless:
                            complete_impl = getattr(
                                type(self._db),
                                "complete_stateless_thread_workspace_creation",
                                None,
                            )
                            completed = (
                                await complete_impl(
                                    self._db,
                                    owner.id,
                                    generation=stateless_creation_generation,
                                    runtime_incarnation=(trusted_runtime_incarnation),
                                    backing_id=backing_id,
                                    ssh_host_key_fingerprint=fingerprint,
                                    pod_ip=pod_ip,
                                    port=30022,
                                    creation_reservation_id=str(
                                        _creation_reservation["id"]
                                    ),
                                    creation_claim_token=int(
                                        _creation_reservation["claim_token"]
                                    ),
                                    host=(
                                        self._workspace_dns(owner) if pvc_name else None
                                    ),
                                )
                                if callable(complete_impl)
                                else None
                            )
                            if not completed:
                                logger.error(
                                    "Lost exact Ready publication authority for "
                                    "stateless thread %s",
                                    owner.id,
                                )
                                return False
                            canvas_generation = completed.get("workspace_generation")
                            runtime_incarnation = trusted_runtime_incarnation
                        else:
                            binding = await self._db.bind_thread_workspace_backing(
                                owner.id,
                                backing_kind="remote",
                                backing_id=backing_id,
                                ssh_host_key_fingerprint=fingerprint,
                            )
                            if binding:
                                canvas_generation = binding.get("workspace_generation")
                                if canvas_generation:
                                    runtime_incarnation = trusted_runtime_incarnation
                    except Exception:
                        # The workspace remains usable by its agent, but Canvas
                        # file serving fails closed until a trusted binding is
                        # available. Never substitute SSH TOFU here.
                        logger.exception(
                            "Failed to bind trusted Canvas SSH identity for session %s",
                            owner.id,
                        )
                ready_ctx = {
                    "status": "ready",
                    "provisioner": "k8s",
                    "pod_ip": pod_ip,
                    "port": 30022,
                    WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY: str(
                        _creation_reservation["id"]
                    ),
                    WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY: str(
                        _creation_reservation["claim_token"]
                    ),
                }
                if owner.kind == "job":
                    # Jobs do not use the Canvas backing bind, but their worker
                    # bundle still needs immutable endpoint provenance.  This
                    # is the exact Pod UID already verified above and used by
                    # deletion/reattach fencing.
                    ready_ctx[WORKSPACE_RUNTIME_INCARNATION_KEY] = runtime_incarnation
                if owner.kind == "session":
                    # Pair this endpoint with the exact binding minted above.
                    # A failed/missing bind publishes null and Canvas fails closed.
                    ready_ctx[CANVAS_WORKSPACE_GENERATION_KEY] = canvas_generation
                    ready_ctx[WORKSPACE_RUNTIME_INCARNATION_KEY] = runtime_incarnation
                # Hand the agent the STABLE Service DNS (not the ephemeral IP) so
                # a reattached/recovered pod is reachable at the same address.
                # The dispatch + resume paths prefer this `host` over `pod_ip`.
                if pvc_name:
                    ready_ctx["host"] = self._workspace_dns(owner)
                if not strict_stateless:
                    # Phase-B state is part of this exact creation generation.
                    # The old detached raw-IP task could deliver profile bytes
                    # into a same-name successor after this request returned.
                    # Attest B, re-lock the reservation, run the pinned seed
                    # synchronously, then post-attest before publishing Ready.
                    if seed_needs_state:
                        seed_attestation = await self.attest_workspace_runtime(owner)
                        if (
                            seed_attestation.runtime_incarnation != runtime_incarnation
                            or seed_attestation.pod_ip != pod_ip
                            or not await self._workspace_creation_reservation_is_current(
                                owner,
                                _creation_reservation,
                                scope="workspace_container",
                            )
                        ):
                            return False
                        if not await self._seed_workspace_state(
                            owner,
                            seed_attestation,
                            scope="workspace_container",
                            mutation_authority=mutation_authority,
                        ):
                            return False
                    if not await self._workspace_creation_reservation_is_current(
                        owner,
                        _creation_reservation,
                        scope="workspace_container",
                    ) or not await self._set_context(owner, ready_ctx):
                        return False
                logger.info(
                    "Workspace container ready: %s @ %s (%s %s)",
                    pod_name,
                    pod_ip,
                    owner.kind,
                    owner.id,
                )
            elif (
                not strict_stateless
                and pvc_reattach
                and self._fresh_fallback_enabled
                and await self._pod_volume_attach_failing(pod_name)
            ):
                # Single-replica node-loss fallback (Phase 3b): the reattached
                # volume can't attach here and won't until the dead node holding
                # its lone replica returns. Discard the wedged PVC and recover onto
                # a FRESH empty volume — the agent then clones from Gitea and
                # resumes the Postgres checkpoint (unpushed working-tree files are
                # lost: the accepted cost of single-replica + node loss). Recurses
                # once with fresh=True, which creates a NEW volume (not a reattach),
                # so this branch cannot re-fire.
                logger.warning(
                    "Workspace %s reattach wedged — volume unattachable (likely a "
                    "dead node holding the single replica). Discarding the PVC and "
                    "recovering onto a fresh volume; unpushed files are lost (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                # The original create reservation still owns this Pod and its
                # stable names. A destructive fresh fallback needs a separate
                # cleanup intent followed by a new reservation; never overlap
                # those two authorities inside one request.
                logger.warning(
                    "Deferring fresh workspace fallback until the current "
                    "creation reservation is reconciled (%s %s)",
                    owner.kind,
                    owner.id,
                )
                return False
            else:
                logger.warning(
                    "Workspace container created but not ready within timeout: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                if not strict_stateless:
                    if not await self._workspace_creation_reservation_is_current(
                        owner,
                        _creation_reservation,
                        scope="workspace_container",
                    ) or not await self._set_context(owner, {"status": "creating"}):
                        return False

            return True
        except Exception as e:
            logger.error(
                "Failed to create workspace container for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            await self._record_creation_diagnostic(
                owner,
                _creation_reservation,
                e,
                strict_stateless=strict_stateless,
            )
            # Once the durable reservation crosses its external-effect edge,
            # every response is potentially ambiguous.  Do not perform
            # name-based rollback or publish a failure projection here; the
            # reservation reconciler owns the exact observed resources. The
            # one exception is a Job's classified pull failure on a Pod that
            # never ran: that outcome is exact, so the generation settles on it
            # before the Job's terminal transition can wedge the reservation.
            await self._settle_failed_job_creation(
                owner,
                _creation_reservation,
                e,
                strict_stateless=strict_stateless,
                fresh_storage=not pvc_reattach,
            )
            return False

    async def _create_pinned_workspace_legacy(
        self,
        owner: WorkspaceOwner,
        cpu: str = "500m",
        memory: str = "1Gi",
        cpu_limit: str = "2000m",
        memory_limit: str = "4Gi",
        image: Optional[str] = None,
        fresh: bool = False,
        stateless_creation_generation: str | None = None,
        allow_stateless_create: bool = False,
        pinned_runtime_generation: str | None = None,
        pinned_agent_id: str | None = None,
        pinned_attach_token: str | None = None,
        expected_workspace_context: dict[str, Any] | None = None,
        expected_binding_context: dict[str, Any] | None = None,
        pinned_runtime_lock_held: bool = False,
    ) -> bool:
        """Create a workspace container for a job or persistent thread.

        Args:
            owner: WorkspaceOwner identifying the job or session.
            cpu: CPU request.
            memory: Memory request.
            cpu_limit: CPU limit.
            memory_limit: Memory limit.
            image: Workspace image override (defaults to WORKSPACE_IMAGE env).
            fresh: Single-replica node-loss recovery (Phase 3b) — force a clean
                empty PVC by deleting any existing (wedged) one first, instead of
                reattaching it. Set only by the internal fallback below.

        Returns:
            True if pod creation succeeded, False otherwise.
        """
        if not self._k8s_available:
            return False

        strict_stateless = stateless_creation_generation is not None
        strict_pinned = pinned_runtime_generation is not None
        if strict_stateless and strict_pinned:
            return False
        if not strict_stateless and owner.kind == "session":
            lane_impl = (
                getattr(
                    type(self._db),
                    "stateless_thread_workspace_creation_requires_authority",
                    None,
                )
                if self._db is not None
                else None
            )
            try:
                requires_authority = (
                    await lane_impl(self._db, owner.id) if callable(lane_impl) else None
                )
            except Exception:
                requires_authority = None
            if requires_authority is not False or not strict_pinned:
                logger.error(
                    "Direct workspace create refused without exact lane "
                    "authority for thread %s",
                    owner.id,
                )
                return False
            if (
                self._db is None
                or not pinned_runtime_lock_held
                or fresh
                or (pinned_agent_id is None) != (pinned_attach_token is None)
            ):
                return False
            try:
                pinned_runtime_generation = _canonical_runtime_uuid(
                    pinned_runtime_generation,
                    label="pinned workspace runtime generation",
                )
                if pinned_agent_id is not None:
                    pinned_agent_id = _canonical_runtime_uuid(
                        pinned_agent_id,
                        label="pinned workspace agent",
                    )
                    pinned_attach_token = _canonical_runtime_uuid(
                        pinned_attach_token,
                        label="pinned workspace attach token",
                    )
            except ValueError:
                return False
        if strict_stateless:
            if owner.kind != "session" or self._db is None:
                return False
            if fresh:
                return False
            try:
                stateless_creation_generation = _canonical_runtime_uuid(
                    stateless_creation_generation,
                    label="stateless workspace creation generation",
                )
            except ValueError:
                return False
            if type(allow_stateless_create) is not bool:
                return False
            if not allow_stateless_create:
                return await self.continue_stateless_workspace_creation(
                    owner,
                    generation=stateless_creation_generation,
                    expected_runtime_incarnation=None,
                    cpu=cpu,
                    memory=memory,
                    cpu_limit=cpu_limit,
                    memory_limit=memory_limit,
                    image=image,
                )
            validate_impl = getattr(
                type(self._db),
                "validate_stateless_thread_workspace_creation_attempt",
                None,
            )
            if not callable(validate_impl) or not await validate_impl(
                self._db,
                owner.id,
                generation=stateless_creation_generation,
                attempted=False,
            ):
                return False

        pod_name = owner.pod_name
        try:
            profile = await self._sandbox_profile(
                owner,
                cpu=cpu,
                memory=memory,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                image=image,
            )
        except Exception:
            logger.exception(
                "Could not read the frozen workspace settings for %s %s",
                owner.kind,
                owner.id,
            )
            return False
        workspace_image = profile.image
        cpu, memory = profile.cpu, profile.memory
        cpu_limit, memory_limit = profile.cpu_limit, profile.memory_limit
        network_tier = await self._resolve_network_tier(
            owner.id, kind=owner.network_tier_kind
        )

        # Resolve every deterministic Kubernetes name before the first create.
        # Reads/config rendering are side-effect free; the row-locked pinned
        # intent below is the effect admission boundary for PVC, ConfigMap,
        # Pod, and Service alike.
        seed_files = await self._resolve_ide_seed_files(owner)
        seed_exts = await self._resolve_ide_extensions(owner)
        seed_needs_state = (
            False
            if strict_stateless
            else await self._resolve_ide_needs_state(owner, seed_exts)
        )
        pvc_name: Optional[str] = _pvc_name_for(owner) if self._pvc_enabled else None
        seed_cm_name = (
            self._seed_configmap_name(pod_name) if seed_files or seed_exts else None
        )
        service_name = owner.pod_name if pvc_name is not None else None
        manifest_fingerprint = self._pinned_workspace_provision_fingerprint(
            owner=owner,
            pod_name=pod_name,
            pvc_name=pvc_name,
            seed_configmap_name=seed_cm_name,
            service_name=service_name,
            network_tier=network_tier,
            workspace_image=workspace_image,
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            seed_files=seed_files,
            seed_extensions=seed_exts,
            seed_needs_state=seed_needs_state,
            profile=profile,
        )
        pinned_intent: dict[str, Any] | None = None
        pinned_attempt_id: str | None = None
        pinned_pod_uid: str | None = None
        pinned_pvc_uid: str | None = None
        pinned_seed_configmap_uid: str | None = None
        pinned_service_uid: str | None = None
        retained_service_uid: str | None = None
        if strict_pinned:
            captured_attempt_id: str | None = None
            if isinstance(expected_workspace_context, Mapping):
                raw_attempt = expected_workspace_context.get(
                    "_workspace_provision_attempt"
                )
                raw_generation = expected_workspace_context.get(
                    "_workspace_provision_generation"
                )
                if raw_attempt is not None or raw_generation is not None:
                    try:
                        captured_attempt_id = _canonical_runtime_uuid(
                            raw_attempt,
                            label="pinned workspace provision attempt",
                        )
                        captured_generation = _canonical_runtime_uuid(
                            raw_generation,
                            label="pinned workspace provision generation",
                        )
                    except ValueError:
                        return False
                    if captured_generation != pinned_runtime_generation:
                        return False
            if service_name is not None:
                try:
                    existing_service = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_service,
                        name=service_name,
                        namespace=self._namespace,
                    )
                except Exception as exc:
                    if getattr(exc, "status", None) != 404:
                        return False
                else:
                    if captured_attempt_id is not None:
                        try:
                            self._require_pinned_workspace_resource_identity(
                                existing_service,
                                resource="service",
                                owner=owner,
                                expected_name=service_name,
                                expected_runtime_generation=(pinned_runtime_generation),
                                expected_attempt_id=captured_attempt_id,
                            )
                        except WorkspaceRuntimeAuthorityError:
                            try:
                                retained_service_uid = (
                                    self._require_stateless_service_identity(
                                        existing_service,
                                        owner=owner,
                                    )
                                )
                            except WorkspaceRuntimeAuthorityError:
                                return False
                    else:
                        try:
                            retained_service_uid = (
                                self._require_stateless_service_identity(
                                    existing_service,
                                    owner=owner,
                                )
                            )
                        except WorkspaceRuntimeAuthorityError:
                            return False
            reserve_impl = getattr(
                type(self._db),
                "reserve_pinned_thread_workspace_provision_intent",
                None,
            )
            if not callable(reserve_impl):
                return False
            proposed_attempt = str(uuid4())
            pinned_intent = await reserve_impl(
                self._db,
                owner.id,
                expected_runtime_generation=pinned_runtime_generation,
                expected_agent_id=pinned_agent_id,
                expected_attach_token=pinned_attach_token,
                expected_workspace_context=expected_workspace_context,
                expected_binding_context=expected_binding_context,
                attempt_id=proposed_attempt,
                namespace=self._namespace,
                pod_name=pod_name,
                pvc_name=pvc_name,
                seed_configmap_name=seed_cm_name,
                service_name=service_name,
                retained_service_uid=retained_service_uid,
                network_tier=network_tier,
                manifest_fingerprint=manifest_fingerprint,
            )
            if not isinstance(pinned_intent, dict):
                return False
            pinned_attempt_id = str(pinned_intent.get("attempt_id") or "")
            if not (
                pinned_attempt_id
                and str(pinned_intent.get("runtime_generation") or "")
                == pinned_runtime_generation
                and str(pinned_intent.get("pod_name") or "") == pod_name
                and (pinned_intent.get("pvc_name") or None) == pvc_name
                and (pinned_intent.get("seed_configmap_name") or None) == seed_cm_name
                and (pinned_intent.get("service_name") or None) == service_name
            ):
                return False

        # Workspace storage (Branch a): PVC-backed for BOTH owner kinds when
        # WORKSPACE_PVC_ENABLED — the volume is named by the owner UUID, survives
        # pod crashes, and reattaches by that deterministic name on recreate
        # (drift recovery, suspend/restore, give_up all funnel back through
        # create_workspace, so 409-reuse here IS the resume path). Sessions need
        # this at least as much as jobs: their pod is reaped on idle while the
        # thread stays resumable, so emptyDir destroys the working tree of state
        # a user can still reopen. Otherwise emptyDir — storage dies with the
        # pod; isolation is the pod boundary. Created BEFORE the seed ConfigMap
        # so a PVC failure leaves nothing to clean up — it is the provisioning
        # prerequisite (and it fails closed: never silently downgrade to
        # emptyDir, that would trade a visible failure for silent data loss).
        pvc_reattach = False
        if self._pvc_enabled:
            # Fresh recovery (Phase 3b single-replica fallback): the prior reattach
            # was wedged because the PVC's only replica is on a dead node. Delete
            # the stuck PVC (and wait for it to release) so the create below makes
            # a brand-new empty volume under the same deterministic name.
            if fresh:
                if not await self._delete_pvc_and_wait(
                    pvc_name,
                    expected_owner=owner,
                ):
                    return False
            retained_pvc_uid = (
                str((pinned_intent or {}).get("retained_pvc_uid") or "") or None
            )
            if strict_pinned and retained_pvc_uid is not None:
                try:
                    retained_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    observed_retained_uid = self._require_stateless_pvc_identity(
                        retained_claim,
                        owner=owner,
                        pvc_name=pvc_name,
                    )
                except Exception:
                    return False
                pvc_status = (
                    "reused" if observed_retained_uid == retained_pvc_uid else None
                )
            else:
                pvc_labels = {owner.label_key: owner.id}
                if strict_pinned:
                    pvc_labels.update(
                        {
                            WORKSPACE_PROVISION_ATTEMPT_LABEL: pinned_attempt_id,
                            WORKSPACE_PROVISION_GENERATION_LABEL: (
                                pinned_runtime_generation
                            ),
                        }
                    )
                pvc_status = await self._create_pvc(
                    pvc_name,
                    size=profile.storage or self._pvc_size,
                    # Owner label lets the backstop reaper resolve PVC → owner.
                    labels=pvc_labels,
                    expected_owner=owner,
                )
            if not pvc_status:
                logger.error(
                    "Workspace PVC create failed for %s %s — aborting provision",
                    owner.kind,
                    owner.id,
                )
                if not strict_stateless and not strict_pinned:
                    await self._set_context(
                        owner, {"status": "failed", "error": "PVC creation failed"}
                    )
                return False
            if strict_pinned:
                try:
                    pinned_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    pvc_uid = self._require_pinned_workspace_resource_identity(
                        pinned_claim,
                        resource="pvc",
                        owner=owner,
                        expected_name=pvc_name,
                        expected_runtime_generation=pinned_runtime_generation,
                        expected_attempt_id=pinned_attempt_id,
                        retained_uid=retained_pvc_uid,
                    )
                except Exception:
                    return False
                publish_impl = getattr(
                    type(self._db),
                    "publish_pinned_thread_workspace_provision_resource",
                    None,
                )
                if not callable(publish_impl) or not await publish_impl(
                    self._db,
                    owner.id,
                    expected_runtime_generation=pinned_runtime_generation,
                    attempt_id=pinned_attempt_id,
                    resource="pvc",
                    resource_uid=pvc_uid,
                ):
                    return False
                pinned_pvc_uid = pvc_uid
            # "reused" (409) = an EXISTING volume reattached — the only case where
            # the single-replica node-loss wedge can occur. `fresh` recreates a NEW
            # volume, so it is never itself a reattach (prevents the fallback below
            # from re-firing → no recursion).
            pvc_reattach = pvc_status == "reused" and not fresh

        try:
            seed_cm = await self._create_seed_configmap(
                pod_name,
                seed_files,
                seed_exts,
                needs_state=seed_needs_state,
                expected_owner=owner,
                expected_creation_generation=(
                    stateless_creation_generation if strict_stateless else None
                ),
                expected_provision_attempt=(
                    pinned_attempt_id if strict_pinned else None
                ),
                expected_runtime_generation=(
                    pinned_runtime_generation if strict_pinned else None
                ),
            )
        except WorkspaceRuntimeAuthorityError as error:
            logger.error(
                "Stateless seed ConfigMap preparation failed for %s: %s",
                owner.id,
                error,
            )
            return False
        if strict_pinned and seed_cm_name is not None:
            if seed_cm != seed_cm_name:
                return False
            try:
                pinned_seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_cm_name,
                    namespace=self._namespace,
                )
                seed_uid = self._require_pinned_workspace_resource_identity(
                    pinned_seed,
                    resource="seed_configmap",
                    owner=owner,
                    expected_name=seed_cm_name,
                    expected_runtime_generation=pinned_runtime_generation,
                    expected_attempt_id=pinned_attempt_id,
                )
            except Exception:
                return False
            publish_impl = getattr(
                type(self._db),
                "publish_pinned_thread_workspace_provision_resource",
                None,
            )
            if not callable(publish_impl) or not await publish_impl(
                self._db,
                owner.id,
                expected_runtime_generation=pinned_runtime_generation,
                attempt_id=pinned_attempt_id,
                resource="seed_configmap",
                resource_uid=seed_uid,
            ):
                return False
            pinned_seed_configmap_uid = seed_uid

        pod_manifest = self._build_pod_manifest(
            pod_name=pod_name,
            owner=owner,
            image=workspace_image,
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            network_tier=network_tier,
            pvc_name=pvc_name,
            seed_configmap=seed_cm,
            stateless_creation_generation=stateless_creation_generation,
            pinned_runtime_generation=(
                pinned_runtime_generation if strict_pinned else None
            ),
            pinned_provision_attempt=(pinned_attempt_id if strict_pinned else None),
            profile=profile,
        )

        # Once the durable false->true attempt CAS is submitted, its outcome is
        # the authority boundary for the one permitted Kubernetes create. A
        # timeout/cancellation/non-409 error can occur after the apiserver has
        # accepted an exact Pod that already references this ConfigMap. Never
        # delete the seed on that ambiguous side of the edge: all retries are
        # read/adopt-only and cannot recreate it. Exact terminal workspace
        # cleanup owns the deterministic ConfigMap thereafter.
        stateless_attempt_may_have_committed = False
        existing_seed_bound = False
        reused_existing_pod = False
        metered_cpu, metered_memory = cpu, memory
        try:
            if strict_stateless:
                claim_impl = getattr(
                    type(self._db),
                    "claim_stateless_thread_workspace_creation_attempt",
                    None,
                )
                if not callable(claim_impl):
                    return False
                stateless_attempt_may_have_committed = True
                if not await claim_impl(
                    self._db,
                    owner.id,
                    generation=stateless_creation_generation,
                ):
                    return False
                created_pod = await self._create_stateless_pod_once(
                    owner,
                    pod_manifest,
                    generation=stateless_creation_generation,
                    expected_network_tier=network_tier,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=seed_cm,
                )
                if created_pod is None:
                    return False
                runtime_incarnation = self._require_stateless_pod_identity(
                    created_pod,
                    owner=owner,
                    generation=stateless_creation_generation,
                    expected_network_tier=network_tier,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=seed_cm,
                )
                metered_cpu, metered_memory = (
                    self._workspace_resource_requests_from_pod(created_pod)
                )
                publish_impl = getattr(
                    type(self._db),
                    "publish_stateless_thread_workspace_runtime",
                    None,
                )
                if not callable(publish_impl) or not await publish_impl(
                    self._db,
                    owner.id,
                    generation=stateless_creation_generation,
                    runtime_incarnation=runtime_incarnation,
                    pod_name=pod_name,
                    namespace=self._namespace,
                ):
                    logger.error(
                        "Lost stateless runtime publication authority for thread %s",
                        owner.id,
                    )
                    return False
            else:
                (
                    created_pod,
                    reused_existing_pod,
                ) = await self._create_pod_resolving_teardown(
                    pod_manifest,
                    pod_name,
                    owner=owner,
                    expected_provision_attempt=(
                        pinned_attempt_id if strict_pinned else None
                    ),
                    expected_runtime_generation=(
                        pinned_runtime_generation if strict_pinned else None
                    ),
                )
                if created_pod is None:
                    return False
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                )
                if strict_pinned:
                    runtime_incarnation = (
                        self._require_pinned_workspace_resource_identity(
                            created_pod,
                            resource="pod",
                            owner=owner,
                            expected_name=pod_name,
                            expected_runtime_generation=pinned_runtime_generation,
                            expected_attempt_id=pinned_attempt_id,
                        )
                    )
                    publish_impl = getattr(
                        type(self._db),
                        "publish_pinned_thread_workspace_provision_resource",
                        None,
                    )
                    if not callable(publish_impl) or not await publish_impl(
                        self._db,
                        owner.id,
                        expected_runtime_generation=pinned_runtime_generation,
                        attempt_id=pinned_attempt_id,
                        resource="pod",
                        resource_uid=runtime_incarnation,
                    ):
                        return False
                    pinned_pod_uid = runtime_incarnation
                observed_seed = self._require_stateless_pod_storage_binding(
                    created_pod,
                    owner=owner,
                    expected_pvc_name=pvc_name,
                    expected_seed_configmap=(
                        _UNSPECIFIED_RESOURCE_BINDING
                        if reused_existing_pod
                        else seed_cm
                    ),
                )
                if reused_existing_pod:
                    if observed_seed is not None:
                        existing_seed_bound = True
                        seed_configmap = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_config_map,
                            name=observed_seed,
                            namespace=self._namespace,
                        )
                        try:
                            self._require_stateless_seed_configmap_identity(
                                seed_configmap,
                                owner=owner,
                                pod_name=pod_name,
                            )
                        except WorkspaceRuntimeAuthorityError:
                            await self._require_legacy_seed_configmap_migration(
                                seed_configmap,
                                owner=owner,
                                pod_name=pod_name,
                            )
                        seed_cm = observed_seed
                    elif seed_cm is not None:
                        if not await self._delete_seed_configmap(
                            pod_name,
                            expected_owner=owner,
                        ):
                            return False
                        seed_cm = None
            # Make the pod own the seed ConfigMap so K8s GCs it on teardown.
            # created_pod is None only for the idempotent live-pod case, where
            # the existing pod already owns its (same-named) ConfigMap.
            if created_pod is not None:
                adopted_seed = await self._adopt_configmap(
                    seed_cm,
                    created_pod,
                    expected_owner=owner,
                    expected_creation_generation=(
                        stateless_creation_generation if strict_stateless else None
                    ),
                    expected_provision_attempt=(
                        pinned_attempt_id if strict_pinned else None
                    ),
                    expected_runtime_generation=(
                        pinned_runtime_generation if strict_pinned else None
                    ),
                )
                if adopted_seed is not True:
                    return False
            # PVC-backed workspaces (jobs AND sessions) get a stable headless
            # Service so the agent dials a constant DNS name that survives pod
            # recreates (reattach/recovery) instead of an ephemeral pod IP. Same
            # gate as the PVC; kept across idle reaps, dropped on release.
            if pvc_name:
                retained_service = (
                    str((pinned_intent or {}).get("retained_service_uid") or "") or None
                )
                service_created = bool(retained_service) if strict_pinned else False
                if not service_created:
                    service_created = await self._create_service(
                        owner,
                        require_exact_owner=True,
                        expected_provision_attempt=(
                            pinned_attempt_id if strict_pinned else None
                        ),
                        expected_runtime_generation=(
                            pinned_runtime_generation if strict_pinned else None
                        ),
                    )
                if not service_created:
                    # Ready publishes this stable DNS name. Preserve the exact
                    # UID+attempt marker until the idempotent Service exists;
                    # continuation retries it without another Pod create.
                    return False
                if strict_pinned:
                    try:
                        pinned_service = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_service,
                            name=owner.pod_name,
                            namespace=self._namespace,
                        )
                        service_uid = self._require_pinned_workspace_resource_identity(
                            pinned_service,
                            resource="service",
                            owner=owner,
                            expected_name=owner.pod_name,
                            expected_runtime_generation=(pinned_runtime_generation),
                            expected_attempt_id=pinned_attempt_id,
                            retained_uid=(
                                str(
                                    (pinned_intent or {}).get("retained_service_uid")
                                    or ""
                                )
                                or None
                            ),
                        )
                    except Exception:
                        return False
                    publish_impl = getattr(
                        type(self._db),
                        "publish_pinned_thread_workspace_provision_resource",
                        None,
                    )
                    if not callable(publish_impl) or not await publish_impl(
                        self._db,
                        owner.id,
                        expected_runtime_generation=pinned_runtime_generation,
                        attempt_id=pinned_attempt_id,
                        resource="service",
                        resource_uid=service_uid,
                    ):
                        return False
                    pinned_service_uid = service_uid
            logger.info(
                "Workspace container created: %s (%s %s)",
                pod_name,
                owner.kind,
                owner.id,
            )
            if not strict_stateless and not strict_pinned:
                await self._set_context(
                    owner,
                    {
                        "status": "created",
                        "provisioner": "k8s",
                        "pod_name": pod_name,
                        "namespace": self._namespace,
                        WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                        **(
                            {
                                CANVAS_WORKSPACE_GENERATION_KEY: None,
                            }
                            if owner.kind == "session"
                            else {}
                        ),
                    },
                )

            # Open a compute-metering interval (Slice 4b) — best-effort, billed on
            # the accepted Pod's immutable requests for strict stateless
            # runtimes. Admission may mutate those values, and a later retry
            # must not bill using caller/config drift. Pinned/job behavior keeps
            # its legacy requested-value semantics.
            await workspace_metering.open_interval(
                self._db,
                owner,
                tier="sandbox",
                cpu=metered_cpu,
                memory=metered_memory,
            )

            # Wait for pod IP. A reattach gets the longer window so a transient
            # node reboot recovers without discarding data; a fresh create keeps
            # the standard 120s.
            ready_timeout = self._reattach_ready_timeout if pvc_reattach else 120
            pod_ip = await self._wait_for_ready(
                pod_name,
                timeout=ready_timeout,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_creation_generation=(
                    stateless_creation_generation if strict_stateless else None
                ),
                expected_network_tier=network_tier,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=seed_cm,
                pull_image=(
                    profile.image if profile.image != self._workspace_image else None
                ),
            )
            if pod_ip:
                if seed_cm is not None:
                    ready_seed = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=seed_cm,
                        namespace=self._namespace,
                    )
                    self._require_stateless_seed_configmap_identity(
                        ready_seed,
                        owner=owner,
                        generation=(
                            stateless_creation_generation if strict_stateless else None
                        ),
                        pod_name=pod_name,
                    )
                    self._require_seed_configmap_pod_owner_reference(
                        ready_seed,
                        pod_name=pod_name,
                        runtime_incarnation=runtime_incarnation,
                    )
                if pvc_name is not None:
                    ready_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    self._require_stateless_pvc_identity(
                        ready_claim,
                        owner=owner,
                        pvc_name=pvc_name,
                    )
                    ready_service = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_service,
                        name=owner.pod_name,
                        namespace=self._namespace,
                    )
                    self._require_stateless_service_identity(
                        ready_service,
                        owner=owner,
                    )
                canvas_generation = None
                if owner.kind == "session" and self._db:
                    try:
                        # Now that sessions can be PVC-backed, this resolves the
                        # trusted identity to the VOLUME (`k8s-pvc:…`) rather
                        # than the pod — which is the point: the backing_id (and
                        # so the Canvas workspace_generation) stops churning on
                        # every pod recreate. A session that predates the PVC
                        # switch re-binds once, from pod UID to PVC UID.
                        (
                            backing_id,
                            fingerprint,
                            trusted_runtime_incarnation,
                        ) = await self._trusted_pod_ssh_identity(
                            pod_name,
                            pvc_name=pvc_name,
                            expected_owner=owner,
                            expected_runtime_incarnation=(
                                pinned_pod_uid if strict_pinned else runtime_incarnation
                            ),
                            expected_creation_generation=(
                                stateless_creation_generation
                                if strict_stateless
                                else None
                            ),
                            expected_network_tier=network_tier,
                            expected_seed_configmap=seed_cm,
                            expected_pvc_uid=(
                                pinned_pvc_uid if strict_pinned else None
                            ),
                            expected_provision_attempt=(
                                pinned_attempt_id if strict_pinned else None
                            ),
                            expected_runtime_generation=(
                                pinned_runtime_generation if strict_pinned else None
                            ),
                            expected_seed_configmap_uid=(
                                pinned_seed_configmap_uid if strict_pinned else None
                            ),
                            expected_service_uid=(
                                pinned_service_uid if strict_pinned else None
                            ),
                            expected_retained_pvc_uid=(
                                str((pinned_intent or {}).get("retained_pvc_uid") or "")
                                or None
                                if strict_pinned
                                else None
                            ),
                            expected_retained_service_uid=(
                                str(
                                    (pinned_intent or {}).get("retained_service_uid")
                                    or ""
                                )
                                or None
                                if strict_pinned
                                else None
                            ),
                        )
                        if strict_stateless:
                            complete_impl = getattr(
                                type(self._db),
                                "complete_stateless_thread_workspace_creation",
                                None,
                            )
                            completed = (
                                await complete_impl(
                                    self._db,
                                    owner.id,
                                    generation=stateless_creation_generation,
                                    runtime_incarnation=(trusted_runtime_incarnation),
                                    backing_id=backing_id,
                                    ssh_host_key_fingerprint=fingerprint,
                                    pod_ip=pod_ip,
                                    port=30022,
                                    host=(
                                        self._workspace_dns(owner) if pvc_name else None
                                    ),
                                )
                                if callable(complete_impl)
                                else None
                            )
                            if not completed:
                                logger.error(
                                    "Lost exact Ready publication authority for "
                                    "stateless thread %s",
                                    owner.id,
                                )
                                return False
                            canvas_generation = completed.get("workspace_generation")
                            runtime_incarnation = trusted_runtime_incarnation
                        elif strict_pinned:
                            complete_impl = getattr(
                                type(self._db),
                                "complete_pinned_thread_workspace_provision_intent",
                                None,
                            )
                            completed = (
                                await complete_impl(
                                    self._db,
                                    owner.id,
                                    expected_runtime_generation=(
                                        pinned_runtime_generation
                                    ),
                                    attempt_id=pinned_attempt_id,
                                    expected_pod_uid=pinned_pod_uid,
                                    expected_pvc_uid=pinned_pvc_uid,
                                    expected_seed_configmap_uid=(
                                        pinned_seed_configmap_uid
                                    ),
                                    expected_service_uid=pinned_service_uid,
                                    pod_ip=pod_ip,
                                    ssh_host_key_fingerprint=fingerprint,
                                    port=30022,
                                )
                                if callable(complete_impl)
                                else None
                            )
                            if not isinstance(completed, dict):
                                logger.error(
                                    "Lost exact Ready publication authority for "
                                    "pinned thread %s",
                                    owner.id,
                                )
                                return False
                            if (
                                str(completed.get("runtime_incarnation") or "")
                                != trusted_runtime_incarnation
                                or str(completed.get("backing_id") or "") != backing_id
                            ):
                                return False
                            canvas_generation = completed.get("workspace_generation")
                            runtime_incarnation = trusted_runtime_incarnation
                        else:
                            binding = await self._db.bind_thread_workspace_backing(
                                owner.id,
                                backing_kind="remote",
                                backing_id=backing_id,
                                ssh_host_key_fingerprint=fingerprint,
                            )
                            if binding:
                                canvas_generation = binding.get("workspace_generation")
                                if canvas_generation:
                                    runtime_incarnation = trusted_runtime_incarnation
                    except Exception:
                        # Legacy workspaces may remain usable while Canvas fails
                        # closed.  A strict attempt has not published Ready yet:
                        # treating this as success strands its durable planned
                        # intent and lets callers skip the only continuation path.
                        logger.exception(
                            "Failed to bind trusted Canvas SSH identity for session %s",
                            owner.id,
                        )
                        if strict_stateless or strict_pinned:
                            return False
                ready_ctx = {
                    "status": "ready",
                    "provisioner": "k8s",
                    "pod_ip": pod_ip,
                    "port": 30022,
                }
                if owner.kind == "job":
                    # Jobs do not use the Canvas backing bind, but their worker
                    # bundle still needs immutable endpoint provenance.  This
                    # is the exact Pod UID already verified above and used by
                    # deletion/reattach fencing.
                    ready_ctx[WORKSPACE_RUNTIME_INCARNATION_KEY] = runtime_incarnation
                if owner.kind == "session":
                    # Pair this endpoint with the exact binding minted above.
                    # A failed/missing bind publishes null and Canvas fails closed.
                    ready_ctx[CANVAS_WORKSPACE_GENERATION_KEY] = canvas_generation
                    ready_ctx[WORKSPACE_RUNTIME_INCARNATION_KEY] = runtime_incarnation
                # Hand the agent the STABLE Service DNS (not the ephemeral IP) so
                # a reattached/recovered pod is reachable at the same address.
                # The dispatch + resume paths prefer this `host` over `pod_ip`.
                if pvc_name:
                    ready_ctx["host"] = self._workspace_dns(owner)
                if not strict_stateless and not strict_pinned:
                    await self._set_context(owner, ready_ctx)
                logger.info(
                    "Workspace container ready: %s @ %s (%s %s)",
                    pod_name,
                    pod_ip,
                    owner.kind,
                    owner.id,
                )
                # The legacy Phase-B SFTP task is name-based and detached from
                # lifecycle authority. Do not let it outlive a stateless Ready
                # CAS and race a replacement or terminal retirement.
                if not strict_stateless and not strict_pinned:
                    asyncio.create_task(self._seed_workspace_state(owner, pod_ip))
            elif (
                not strict_stateless
                and not strict_pinned
                and pvc_reattach
                and self._fresh_fallback_enabled
                and await self._pod_volume_attach_failing(pod_name)
            ):
                # Single-replica node-loss fallback (Phase 3b): the reattached
                # volume can't attach here and won't until the dead node holding
                # its lone replica returns. Discard the wedged PVC and recover onto
                # a FRESH empty volume — the agent then clones from Gitea and
                # resumes the Postgres checkpoint (unpushed working-tree files are
                # lost: the accepted cost of single-replica + node loss). Recurses
                # once with fresh=True, which creates a NEW volume (not a reattach),
                # so this branch cannot re-fire.
                logger.warning(
                    "Workspace %s reattach wedged — volume unattachable (likely a "
                    "dead node holding the single replica). Discarding the PVC and "
                    "recovering onto a fresh volume; unpushed files are lost (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                try:
                    await self.delete_workspace(owner)
                except Exception:
                    logger.exception(
                        "Error deleting wedged pod before fresh recovery (%s)",
                        pod_name,
                    )
                fresh_ok = await self.create_workspace(
                    owner,
                    cpu=cpu,
                    memory=memory,
                    cpu_limit=cpu_limit,
                    memory_limit=memory_limit,
                    image=image,
                    fresh=True,
                )
                # Record that the working tree was reset (observability for the
                # dispatch/UI: this resume starts from the last push, not the disk).
                await self._set_context(owner, {"workspace_reset": True})
                return fresh_ok
            else:
                logger.warning(
                    "Workspace container created but not ready within timeout: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                if not strict_stateless and not strict_pinned:
                    await self._set_context(owner, {"status": "creating"})
                else:
                    return False

            return True
        except Exception as e:
            logger.error(
                "Failed to create workspace container for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            # Legacy/job failures can discard an unreferenced seed. For a
            # submitted stateless attempt, API acceptance is ambiguous and the
            # exact Pod may already depend on it; preserve it for read/adopt
            # continuation and let terminal workspace cleanup remove it.
            if not (
                existing_seed_bound
                or reused_existing_pod
                or (strict_stateless and stateless_attempt_may_have_committed)
                or strict_pinned
            ):
                await self._delete_seed_configmap(
                    pod_name,
                    expected_owner=owner,
                )
            if not strict_stateless and not strict_pinned:
                await self._set_context(
                    owner,
                    {"status": "failed", "error": str(e)},
                )
            return False

    async def prepare_stateless_workspace_recreation(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        mode: str,
    ) -> str | None:
        """Rotate create authority only after the caller proves exact-terminal.

        The marker is persisted before the UID-preconditioned delete.  Its
        attempt bit remains false until create_workspace has completed all
        non-Pod preparation, so a crash before Pod actuation remains safely
        recoverable by a later lifecycle owner.
        """

        if owner.kind != "session" or not self._k8s_available or self._db is None:
            return None
        try:
            expected_runtime_incarnation = _canonical_runtime_uuid(
                expected_runtime_incarnation,
                label="stateless workspace runtime incarnation",
            )
        except ValueError:
            return None
        if mode not in {"create", "restore"}:
            return None
        if (
            await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
            != "exact_terminal"
        ):
            return None
        prepare_impl = getattr(
            type(self._db), "prepare_stateless_thread_workspace_creation", None
        )
        if not callable(prepare_impl):
            return None
        generation = str(uuid4())
        plan = await prepare_impl(
            self._db,
            owner.id,
            proposed_generation=generation,
            mode=mode,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
        if (
            not isinstance(plan, dict)
            or plan.get("state") != "prepared"
            or not isinstance(plan.get("creation"), dict)
            or plan["creation"].get("attempted") is not False
        ):
            return None
        try:
            generation = _canonical_runtime_uuid(
                plan["creation"].get("generation"),
                label="stateless thread runtime generation",
            )
        except ValueError:
            return None
        deleted = await self._delete_prepared_terminal_workspace_runtime(
            owner,
            generation=generation,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
        return generation if deleted is True else None

    async def finalize_stateless_workspace_recreation_deletion(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str,
    ) -> bool:
        """Recover the DB edge after a proven-terminal delete reached 404."""

        if owner.kind != "session" or self._db is None:
            return False
        try:
            generation = _canonical_runtime_uuid(
                generation,
                label="stateless workspace creation generation",
            )
            expected_runtime_incarnation = _canonical_runtime_uuid(
                expected_runtime_incarnation,
                label="stateless workspace runtime incarnation",
            )
        except ValueError:
            return False
        if (
            await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
            != "exact_absent"
        ):
            return False
        return await self._clear_prepared_terminal_workspace_runtime(
            owner,
            generation=generation,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )

    async def _delete_prepared_terminal_workspace_runtime(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str,
    ) -> bool:
        """UID-delete and observe 404 before clearing durable runtime identity."""

        validate_impl = getattr(
            type(self._db),
            "validate_stateless_thread_workspace_creation_attempt",
            None,
        )
        valid_creation = bool(
            callable(validate_impl)
            and await validate_impl(
                self._db,
                owner.id,
                generation=generation,
                attempted=False,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
        )
        if not valid_creation:
            replay_authority = (
                await self._managed_repository_process_zero_replay_authority(
                    owner,
                    scope="workspace_container",
                    runtime_incarnation=expected_runtime_incarnation,
                )
            )
            if replay_authority != "stale":
                return False
            try:
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
            except Exception as exc:
                return getattr(exc, "status", None) == 404
            return False
        # The typed cleanup reconciler owns every external mutation and checks
        # the exact claim immediately before Pod/finalizer/seed operations.
        # Keeping terminal recreation on that one path prevents a second,
        # subtly weaker delete protocol from bypassing migration 0197.
        return await self._clear_prepared_terminal_workspace_runtime(
            owner,
            generation=generation,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )

    @staticmethod
    def _has_stateless_process_zero_finalizer(pod: Any) -> bool:
        finalizers = getattr(getattr(pod, "metadata", None), "finalizers", None)
        return isinstance(finalizers, (list, tuple)) and (
            STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER in finalizers
        )

    async def _wait_for_exact_workspace_pod_terminal(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        timeout: float,
    ) -> bool:
        """Wait for positive process-zero evidence on one retained Pod UID."""

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            authority = await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
            if authority == "exact_terminal":
                return True
            if authority in {"exact_absent", "replacement"}:
                return False
            await asyncio.sleep(1)
        return False

    async def release_stateless_workspace_process_zero_finalizer(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        _mutation_guard_held: bool = False,
    ) -> bool:
        """Release the workspace finalizer after exact terminal proof.

        The public name is retained for compatibility with the original
        stateless-only lifecycle owner.  New pinned job/session workspace and
        IDE Pods use the same exact-UID protocol through the private helper.
        """

        return await self._release_process_zero_finalizer(
            owner,
            pod_name=owner.pod_name,
            expected_runtime_incarnation=expected_runtime_incarnation,
            scope="workspace_container",
            _mutation_guard_held=_mutation_guard_held,
        )

    async def _release_process_zero_finalizer(
        self,
        owner: WorkspaceOwner,
        *,
        pod_name: str,
        expected_runtime_incarnation: str,
        scope: str,
        expected_component: str | None = None,
        admission_source: Literal["automatic", "explicit"] = "explicit",
        _mutation_guard_held: bool = False,
    ) -> bool:
        """Remove only our finalizer after exact terminal-container proof.

        The JSON Patch tests both immutable Pod UID and the selected finalizer
        slot.  A reordered list, a same-name replacement, API absence, or a
        non-terminal Pod is a retryable refusal rather than deletion authority.
        """

        if not self._k8s_available or self._db is None:
            return False
        if scope not in {"workspace_container", "ide"}:
            return False
        non_pinned = True
        if owner.kind == "session":
            lane_probe = getattr(
                type(self._db),
                "stateless_thread_workspace_creation_requires_authority",
                None,
            )
            if not callable(lane_probe):
                return False
            non_pinned = await lane_probe(self._db, owner.id)
            if non_pinned is None:
                return False
        if non_pinned and not _mutation_guard_held:
            async with self._workspace_mutation_guard(owner, scope=scope) as owned:
                if not owned:
                    return False
                return await self._release_process_zero_finalizer(
                    owner,
                    pod_name=pod_name,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    scope=scope,
                    expected_component=expected_component,
                    admission_source=admission_source,
                    _mutation_guard_held=True,
                )
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            observed = self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
                expected_pod_name=pod_name,
                expected_component=expected_component,
            )
            if observed != expected_runtime_incarnation:
                return False
            if not _pod_has_exact_process_zero(pod):
                return False
            recorded = await self._db.record_managed_repository_workspace_process_zero(
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                provisioner="k8s",
                runtime_incarnation=expected_runtime_incarnation,
            )
            if non_pinned and not recorded:
                record_orphan = getattr(
                    type(self._db),
                    "record_orphan_managed_repository_workspace_process_zero",
                    None,
                )
                if callable(record_orphan):
                    recorded = await record_orphan(
                        self._db,
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope=scope,
                        provisioner="k8s",
                        runtime_incarnation=expected_runtime_incarnation,
                    )
            if non_pinned and not recorded:
                record_stale = getattr(
                    type(self._db),
                    "record_stale_managed_repository_workspace_process_zero",
                    None,
                )
                if callable(record_stale):
                    recorded = await record_stale(
                        self._db,
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope=scope,
                        provisioner="k8s",
                        runtime_incarnation=expected_runtime_incarnation,
                    )
            if not recorded:
                return False

            captured: dict[str, Any] | None = None
            if non_pinned and scope == "workspace_container":
                read_intent = getattr(
                    type(self._db),
                    "get_managed_repository_workspace_cleanup_intent",
                    None,
                )
                intent = (
                    await read_intent(
                        self._db,
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope=scope,
                        runtime_incarnation=expected_runtime_incarnation,
                    )
                    if callable(read_intent)
                    else None
                )
                target = (
                    str(intent.get("target_disposition"))
                    if isinstance(intent, dict)
                    and intent.get("target_disposition") in {"deleted", "suspended"}
                    else "deleted"
                )
                reclaim = bool(
                    isinstance(intent, dict)
                    and intent.get("reclaim_shared_resources") is True
                )
                captured = await self.prepare_workspace_cleanup_intent(
                    owner,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    target_disposition=target,
                    reclaim_shared_resources=reclaim,
                    suspended_at=(
                        intent.get("suspended_at")
                        if isinstance(intent, dict)
                        and isinstance(intent.get("suspended_at"), str)
                        else None
                    ),
                    admission_source=admission_source,
                    _mutation_guard_held=True,
                )
                if captured is None:
                    captured = await self.prepare_workspace_cleanup_intent(
                        owner,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        target_disposition="deleted",
                        reclaim_shared_resources=False,
                        allow_stale_predecessor=True,
                        admission_source=admission_source,
                        _mutation_guard_held=True,
                    )
                if captured is None:
                    captured = await self.prepare_workspace_cleanup_intent(
                        owner,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        target_disposition="deleted",
                        reclaim_shared_resources=False,
                        allow_orphan=True,
                        admission_source=admission_source,
                        _mutation_guard_held=True,
                    )
            elif non_pinned:
                captured = await self.prepare_ide_cleanup_intent(
                    owner.id,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    target_disposition="expired",
                    admission_source=admission_source,
                )
                if captured is None:
                    captured = await self.prepare_ide_cleanup_intent(
                        owner.id,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        target_disposition="expired",
                        allow_stale_predecessor=True,
                        admission_source=admission_source,
                    )
                if captured is None:
                    captured = await self.prepare_ide_cleanup_intent(
                        owner.id,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        target_disposition="expired",
                        allow_orphan=True,
                        admission_source=admission_source,
                    )
            if non_pinned and (
                not isinstance(captured, dict)
                or captured.get("resources_captured_at") is None
            ):
                return False
            # Preserve the original stateless convenience receipt used to
            # settle a committed finalizer-patch whose response is lost.  A
            # pinned session simply declines this optional second projection.
            if owner.kind == "session" and scope == "workspace_container":
                record_terminal = getattr(
                    type(self._db),
                    "record_stateless_thread_workspace_process_zero",
                    None,
                )
                if callable(record_terminal):
                    await record_terminal(
                        self._db,
                        owner.id,
                        runtime_incarnation=expected_runtime_incarnation,
                    )
            finalizers = getattr(getattr(pod, "metadata", None), "finalizers", None)
            if finalizers is None:
                return True
            if not isinstance(finalizers, (list, tuple)):
                return False
            matching = [
                index
                for index, value in enumerate(finalizers)
                if value == STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER
            ]
            if not matching:
                return True
            if len(matching) != 1:
                return False
            if non_pinned:
                claimant = str(captured.get("claimed_by") or "")
                if not claimant or not await self._cleanup_claim_is_current(
                    captured,
                    claimant=claimant,
                ):
                    return False
            index = matching[0]
            patch = [
                {
                    "op": "test",
                    "path": "/metadata/uid",
                    "value": expected_runtime_incarnation,
                },
                {
                    "op": "test",
                    "path": f"/metadata/finalizers/{index}",
                    "value": STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER,
                },
                {
                    "op": "remove",
                    "path": f"/metadata/finalizers/{index}",
                },
            ]
            if non_pinned:
                await self._bounded_kubernetes_mutation(
                    self._core_api.patch_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    body=patch,
                )
            else:
                await self._bounded_kubernetes_call(
                    self._core_api.patch_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    body=patch,
                )
            return True
        except Exception:
            return False

    async def _wait_for_exact_workspace_pod_absent(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        timeout: float,
    ) -> bool:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    return True
                return False
            observed = str(getattr(getattr(pod, "metadata", None), "uid", "") or "")
            if observed != expected_runtime_incarnation:
                return False
            await asyncio.sleep(1)
        return False

    async def _clear_prepared_terminal_workspace_runtime(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str,
    ) -> bool:
        prepared = await self.prepare_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
            target_disposition="deleted",
            reclaim_shared_resources=False,
            admission_source="explicit",
        )
        if not isinstance(prepared, dict):
            return False
        cleanup = await self.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
            intent_generation=int(prepared["intent_generation"]),
        )
        if not cleanup.settled:
            return False
        clear_impl = getattr(
            type(self._db),
            "clear_stateless_thread_workspace_runtime_for_recreation",
            None,
        )
        if not callable(clear_impl) or not await clear_impl(
            self._db,
            owner.id,
            generation=generation,
            expected_runtime_incarnation=expected_runtime_incarnation,
        ):
            return False
        await workspace_metering.close_interval(self._db, owner)
        return True

    async def _continue_stateless_workspace_creation_reservation(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str,
    ) -> bool:
        """Resume only the creation row already bound to the caller's exact UID."""

        async with self._workspace_mutation_guard(
            owner, scope="workspace_container"
        ) as owned:
            if not owned:
                return False
            validate = getattr(
                type(self._db),
                "validate_stateless_thread_workspace_creation_attempt",
                None,
            )
            if not callable(validate) or not await validate(
                self._db,
                owner.id,
                generation=generation,
                attempted=True,
                expected_runtime_incarnation=expected_runtime_incarnation,
            ):
                return False
            current = await self._db.get_thread(owner.id)
            metadata = current.get("metadata") if current else None
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            workspace = (metadata or {}).get("workspace_container") or {}
            operation_kind = (workspace.get("_runtime_creation") or {}).get("mode")
            if operation_kind not in {"create", "restore"}:
                return False
            captured = await self.get_current_workspace_creation_result(
                owner, operation_kind=operation_kind
            )
            if (
                not isinstance(captured, dict)
                or str(captured.get("thread_runtime_generation") or "") != generation
                or str(captured.get("runtime_incarnation") or "")
                != expected_runtime_incarnation
                or captured.get("phase") != "runtime_bound"
                or captured.get("settled_at") is not None
                or captured.get("cancel_requested_at") is not None
            ):
                return False
            # The guard prevents retirement or a successor from changing the
            # captured row between this read and same-row lease reacquisition.
            # Preserve restore's suspension-derived claimant and manifest too.
            reservation = await self._db.reserve_managed_repository_workspace_creation(
                owner.id,
                owner_kind="thread",
                scope="workspace_container",
                claimant=str(captured["claimed_by"]),
                lease_seconds=1800,
                operation_kind=operation_kind,
                desired_manifest_digest=str(captured["desired_manifest_digest"]),
            )
            if (
                not isinstance(reservation, dict)
                or reservation.get("id") != captured["id"]
                or reservation.get("reservation_generation")
                != captured["reservation_generation"]
                or str(reservation.get("runtime_incarnation") or "")
                != expected_runtime_incarnation
                or not await self._start_workspace_creation_reservation(
                    owner, reservation, scope="workspace_container"
                )
            ):
                return False
            if not await self.continue_stateless_workspace_creation(
                owner,
                generation=generation,
                expected_runtime_incarnation=expected_runtime_incarnation,
                _creation_reservation=reservation,
            ):
                return False
            current = await self._db.get_thread(owner.id)
            metadata = current.get("metadata") if current else None
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            workspace = (metadata or {}).get("workspace_container") or {}
            if (
                workspace.get("status") != "ready"
                or "_runtime_creation" in workspace
                or workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
                != expected_runtime_incarnation
            ):
                return False
            return bool(
                await self._db.settle_managed_repository_workspace_creation_reservation(
                    owner.id,
                    owner_kind="thread",
                    scope="workspace_container",
                    reservation_generation=int(reservation["reservation_generation"]),
                    claimant=str(reservation["claimed_by"]),
                    claim_token=int(reservation["claim_token"]),
                    runtime_incarnation=expected_runtime_incarnation,
                )
            )

    async def continue_stateless_workspace_creation(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str | None,
        cpu: str = "500m",
        memory: str = "1Gi",
        cpu_limit: str = "2000m",
        memory_limit: str = "4Gi",
        image: Optional[str] = None,
        _creation_reservation: dict[str, Any] | None = None,
    ) -> bool:
        """Resume one attempted create without ever issuing create-by-name."""

        del cpu_limit, memory_limit, image
        if owner.kind != "session" or not self._k8s_available or self._db is None:
            return False
        try:
            generation = _canonical_runtime_uuid(
                generation,
                label="stateless workspace creation generation",
            )
            expected_runtime = (
                _canonical_runtime_uuid(
                    expected_runtime_incarnation,
                    label="stateless workspace runtime incarnation",
                )
                if expected_runtime_incarnation is not None
                else None
            )
        except ValueError:
            return False
        if not isinstance(_creation_reservation, dict):
            if expected_runtime is None:
                return False
            return await self._continue_stateless_workspace_creation_reservation(
                owner,
                generation=generation,
                expected_runtime_incarnation=expected_runtime,
            )
        validate_impl = getattr(
            type(self._db),
            "validate_stateless_thread_workspace_creation_attempt",
            None,
        )
        if not callable(validate_impl) or not await validate_impl(
            self._db,
            owner.id,
            generation=generation,
            attempted=True,
            expected_runtime_incarnation=expected_runtime,
        ):
            return False
        try:
            network_tier = await self._resolve_network_tier(
                owner.id,
                kind=owner.network_tier_kind,
            )
            pod = await self._read_stateless_creation_pod(
                owner,
                generation=generation,
                expected_runtime_incarnation=expected_runtime,
                expected_network_tier=network_tier,
                expected_pvc_name=_UNSPECIFIED_RESOURCE_BINDING,
                expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
            )
            if pod is None:
                # Once attempted, Kubernetes absence is an absorbing safe hold:
                # the original API call might have reached a partitioned node.
                return False
            runtime_incarnation = self._require_stateless_pod_identity(
                pod,
                owner=owner,
                generation=generation,
                expected_runtime_incarnation=expected_runtime,
                expected_network_tier=network_tier,
                expected_pvc_name=_UNSPECIFIED_RESOURCE_BINDING,
                expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
            )
            self._require_workspace_creation_reservation_annotation(
                pod,
                reservation_id=str(_creation_reservation["id"]),
            )
            if not await self._authorize_workspace_creation_runtime(
                owner,
                _creation_reservation,
                scope="workspace_container",
                runtime_incarnation=runtime_incarnation,
            ):
                return False
            pvc_name = self._workspace_pvc_name_from_pod(pod, owner=owner)
            actual_cpu, actual_memory = self._workspace_resource_requests_from_pod(pod)
            pvc_uid: str | None = None
            pvc_storage_class: str | None = None
            if pvc_name is not None:
                claim = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
                pvc_uid = self._require_stateless_pvc_identity(
                    claim,
                    owner=owner,
                    pvc_name=pvc_name,
                    allow_any_storage_class=True,
                )
                pvc_storage_class = str(
                    getattr(getattr(claim, "spec", None), "storage_class_name", "")
                    or ""
                )
            seed_configmap = self._require_stateless_pod_storage_binding(
                pod,
                owner=owner,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
            )
            publish_impl = getattr(
                type(self._db),
                "publish_stateless_thread_workspace_runtime",
                None,
            )
            if not callable(publish_impl) or not await publish_impl(
                self._db,
                owner.id,
                generation=generation,
                runtime_incarnation=runtime_incarnation,
                pod_name=owner.pod_name,
                namespace=self._namespace,
                creation_reservation_id=str(_creation_reservation["id"]),
                creation_claim_token=int(_creation_reservation["claim_token"]),
            ):
                return False

            if not await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="workspace_container",
            ) or (
                await self._adopt_configmap(
                    seed_configmap,
                    pod,
                    expected_owner=owner,
                    expected_creation_generation=generation,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    mutation_authority=lambda: self._workspace_creation_reservation_is_current(
                        owner,
                        _creation_reservation,
                        scope="workspace_container",
                    ),
                )
                is not True
            ):
                return False

            if pvc_name:
                if (
                    not await self._workspace_creation_reservation_is_current(
                        owner,
                        _creation_reservation,
                        scope="workspace_container",
                    )
                    or not await self._begin_workspace_creation_effect(
                        owner,
                        _creation_reservation,
                        scope="workspace_container",
                        resource_kind="service",
                    )
                    or not await self._create_service(
                        owner,
                        require_exact_owner=True,
                        creation_reservation_id=str(_creation_reservation["id"]),
                        mutation_authority=lambda: self._workspace_creation_reservation_is_current(
                            owner,
                            _creation_reservation,
                            scope="workspace_container",
                        ),
                    )
                ):
                    return False
                reservation_service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
                reservation_service_uid = self._require_stateless_service_identity(
                    reservation_service, owner=owner
                )
                if not await self._record_workspace_creation_resource(
                    owner,
                    _creation_reservation,
                    scope="workspace_container",
                    resource_kind="service",
                    resource_uid=reservation_service_uid,
                ):
                    return False
            if not await self._record_workspace_creation_resources(
                owner,
                _creation_reservation,
                scope="workspace_container",
                runtime_incarnation=runtime_incarnation,
                pod_name=owner.pod_name,
                seed_configmap=seed_configmap,
                pvc_name=pvc_name,
            ):
                return False
            await workspace_metering.open_interval(
                self._db,
                owner,
                tier="sandbox",
                cpu=actual_cpu,
                memory=actual_memory,
            )
            ready_timeout = self._reattach_ready_timeout if pvc_name else 120
            settings = SandboxSettings()
            if callable(getattr(type(self._db), "fetchrow", None)):
                settings = await resolve_sandbox_settings(
                    self._db, owner.kind, owner.id
                )
            pod_ip = await self._wait_for_ready(
                owner.pod_name,
                timeout=ready_timeout,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_creation_generation=generation,
                expected_network_tier=network_tier,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=seed_configmap,
                pull_image=settings.image,
                pull_started_at=getattr(pod.metadata, "creation_timestamp", None),
            )
            if not pod_ip:
                return True
            (
                backing_id,
                fingerprint,
                trusted_runtime,
            ) = await self._trusted_pod_ssh_identity(
                owner.pod_name,
                pvc_name=pvc_name,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_creation_generation=generation,
                expected_network_tier=network_tier,
                expected_seed_configmap=seed_configmap,
                expected_pvc_uid=pvc_uid,
                expected_pvc_storage_class=pvc_storage_class,
            )
            complete_impl = getattr(
                type(self._db),
                "complete_stateless_thread_workspace_creation",
                None,
            )
            completed = (
                await complete_impl(
                    self._db,
                    owner.id,
                    generation=generation,
                    runtime_incarnation=trusted_runtime,
                    backing_id=backing_id,
                    ssh_host_key_fingerprint=fingerprint,
                    pod_ip=pod_ip,
                    port=30022,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    creation_claim_token=int(_creation_reservation["claim_token"]),
                    host=self._workspace_dns(owner) if pvc_name else None,
                )
                if callable(complete_impl)
                else None
            )
            if not completed:
                return False
            logger.info(
                "Stateless workspace create continued to Ready: %s (%s)",
                owner.pod_name,
                owner.id,
            )
            # Phase-B seeding is intentionally skipped for stateless sessions:
            # its legacy SFTP task is name-based, unpinned, and detached from
            # lifecycle authority. Pinned/job behavior remains unchanged.
            return True
        except (WorkspaceRuntimeAuthorityError, WorkspaceSSHAuthenticationError):
            return False
        except Exception:
            logger.exception(
                "Failed to continue exact stateless workspace %s", owner.id
            )
            return False

    async def _read_stateless_creation_pod(
        self,
        owner: WorkspaceOwner,
        *,
        generation: str,
        expected_runtime_incarnation: str | None,
        expected_network_tier: str,
        expected_pvc_name: str | None,
        expected_seed_configmap: str | None | object,
    ) -> Any | None:
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod authority probe failed"
            ) from exc
        self._require_stateless_pod_identity(
            pod,
            owner=owner,
            generation=generation,
            expected_runtime_incarnation=expected_runtime_incarnation,
            expected_network_tier=expected_network_tier,
            expected_pvc_name=expected_pvc_name,
            expected_seed_configmap=expected_seed_configmap,
        )
        seed_configmap = self._require_stateless_pod_storage_binding(
            pod,
            owner=owner,
            expected_pvc_name=expected_pvc_name,
            expected_seed_configmap=expected_seed_configmap,
        )
        if seed_configmap is not None:
            try:
                configmap = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_configmap,
                    namespace=self._namespace,
                )
                self._require_stateless_seed_configmap_identity(
                    configmap,
                    owner=owner,
                    generation=generation,
                )
            except Exception as error:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace seed ConfigMap authority changed"
                ) from error
        if _pod_has_exact_process_zero(pod):
            raise WorkspaceRuntimeAuthorityError("authorized workspace Pod is terminal")
        return pod

    async def _create_stateless_pod_once(
        self,
        owner: WorkspaceOwner,
        pod_manifest: dict,
        *,
        generation: str,
        expected_network_tier: str,
        expected_pvc_name: str | None,
        expected_seed_configmap: str | None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> Any | None:
        """Issue the one authorized create, adopting only its exact response."""

        try:
            if mutation_authority is not None and not await mutation_authority():
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation authority expired before stateless Pod create"
                )
            pod = await self._bounded_kubernetes_mutation(
                self._core_api.create_namespaced_pod,
                namespace=self._namespace,
                body=pod_manifest,
            )
        except Exception as exc:
            if getattr(exc, "status", None) != 409:
                raise
            # A client-side retry may surface 409 after the original create
            # reached the apiserver.  Adopt only the exact annotated object;
            # never wait out or replace an unrelated deterministic-name Pod.
            return await self._read_stateless_creation_pod(
                owner,
                generation=generation,
                expected_runtime_incarnation=None,
                expected_network_tier=expected_network_tier,
                expected_pvc_name=expected_pvc_name,
                expected_seed_configmap=expected_seed_configmap,
            )
        if pod is None:
            # Some Kubernetes mocks/clients omit the response body. Re-read the
            # object, still under the exact annotation/owner fence.
            return await self._read_stateless_creation_pod(
                owner,
                generation=generation,
                expected_runtime_incarnation=None,
                expected_network_tier=expected_network_tier,
                expected_pvc_name=expected_pvc_name,
                expected_seed_configmap=expected_seed_configmap,
            )
        self._require_stateless_pod_identity(
            pod,
            owner=owner,
            generation=generation,
            expected_network_tier=expected_network_tier,
            expected_pvc_name=expected_pvc_name,
            expected_seed_configmap=expected_seed_configmap,
        )
        return pod

    def _require_stateless_pod_identity(
        self,
        pod: Any,
        *,
        owner: WorkspaceOwner,
        generation: str,
        expected_runtime_incarnation: str | None = None,
        expected_network_tier: str | None = None,
        expected_pvc_name: str | None | object = _UNSPECIFIED_RESOURCE_BINDING,
        expected_seed_configmap: str | None | object = (_UNSPECIFIED_RESOURCE_BINDING),
    ) -> str:
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None)
        annotations = getattr(metadata, "annotations", None)
        opposite_owner_label = (
            "srw/job-id" if owner.kind == "session" else "srw/thread-id"
        )
        if not isinstance(labels, dict) or not isinstance(annotations, dict):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod identity metadata is malformed"
            )
        if (
            str(getattr(metadata, "name", "") or "") != owner.pod_name
            or str(getattr(metadata, "namespace", "") or "") != self._namespace
            or labels.get(owner.label_key) != owner.id
            or labels.get("app") != "srw-workspace"
            or labels.get("srw/component") != owner.component_label
            or labels.get("srw.io/component") != "agent-workspace"
            or WORKSPACE_PROVISION_FENCE_LABEL in labels
            or opposite_owner_label in labels
            or (
                expected_network_tier is not None
                and labels.get("srw.io/network-tier") != expected_network_tier
            )
            or annotations.get(WORKSPACE_RUNTIME_CREATION_ANNOTATION) != generation
            or getattr(metadata, "deletion_timestamp", None) is not None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod owner or creation authority changed"
            )
        self._require_stateless_pod_storage_binding(
            pod,
            owner=owner,
            expected_pvc_name=expected_pvc_name,
            expected_seed_configmap=expected_seed_configmap,
        )
        try:
            runtime_incarnation = _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace Pod UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc
        if (
            expected_runtime_incarnation is not None
            and runtime_incarnation != expected_runtime_incarnation
        ):
            raise WorkspaceRuntimeAuthorityError("workspace Pod UID changed")
        return runtime_incarnation

    @staticmethod
    def _require_workspace_creation_reservation_annotation(
        pod: Any,
        *,
        reservation_id: str,
    ) -> None:
        """Require the exact durable creation reservation on an observed Pod."""

        try:
            expected = str(UUID(str(reservation_id)))
        except (TypeError, ValueError) as exc:
            raise WorkspaceRuntimeAuthorityError(
                "workspace creation reservation is malformed"
            ) from exc
        annotations = getattr(getattr(pod, "metadata", None), "annotations", None)
        if (
            not isinstance(annotations, dict)
            or annotations.get(WORKSPACE_CREATION_RESERVATION_ANNOTATION) != expected
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod creation reservation changed"
            )

    def _require_workspace_pod_owner(
        self,
        pod: Any,
        *,
        owner: WorkspaceOwner,
        allow_owner_unlabeled: bool,
        allow_terminating: bool = False,
        expected_network_tier: str | None = None,
        expected_pod_name: str | None = None,
        expected_component: str | None = None,
    ) -> str:
        """Fence deterministic-name Pod reuse/deletion across owner prefixes."""

        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None)
        observed_namespace = getattr(metadata, "namespace", None)
        if (
            str(getattr(metadata, "name", "") or "")
            != (expected_pod_name or owner.pod_name)
            or (
                isinstance(observed_namespace, str)
                and observed_namespace
                and observed_namespace != self._namespace
            )
            or (not allow_owner_unlabeled and observed_namespace != self._namespace)
            or (
                not allow_terminating
                and getattr(metadata, "deletion_timestamp", None) is not None
            )
        ):
            raise WorkspaceRuntimeAuthorityError("workspace Pod identity changed")
        if not isinstance(labels, dict):
            if labels is not None or not allow_owner_unlabeled:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod owner labels are malformed"
                )
        else:
            owner_labels_present = any(
                key in labels for key in ("srw/job-id", "srw/thread-id")
            )
            if not owner_labels_present:
                if not allow_owner_unlabeled:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Pod owner labels are missing"
                    )
            elif (
                labels.get(owner.label_key) != owner.id
                or ("srw/job-id" if owner.kind == "session" else "srw/thread-id")
                in labels
                or labels.get("app") != "srw-workspace"
                or labels.get("srw/component")
                != (expected_component or owner.component_label)
                or labels.get("srw.io/component") != "agent-workspace"
                or WORKSPACE_PROVISION_FENCE_LABEL in labels
                or (
                    expected_network_tier is not None
                    and labels.get("srw.io/network-tier") != expected_network_tier
                )
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod belongs to another owner"
                )
        try:
            return _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace Pod UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    def _require_stateless_pod_storage_binding(
        self,
        pod: Any,
        *,
        owner: WorkspaceOwner,
        expected_pvc_name: str | None | object,
        expected_seed_configmap: str | None | object,
        expected_pod_name: str | None = None,
    ) -> str | None:
        spec = getattr(pod, "spec", None)
        volumes = getattr(spec, "volumes", None)
        containers = getattr(spec, "containers", None)
        if not isinstance(volumes, (list, tuple)) or not isinstance(
            containers, (list, tuple)
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod storage declaration is malformed"
            )
        workspace_volumes = [
            volume
            for volume in volumes
            if _resource_field(volume, "name") == "workspace-data"
        ]
        workspace_containers = [
            container
            for container in containers
            if _resource_field(container, "name") == "workspace"
        ]
        if len(workspace_volumes) != 1 or len(workspace_containers) != 1:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod storage identity changed"
            )
        workspace_volume = workspace_volumes[0]
        pvc_source = _resource_field(
            workspace_volume,
            "persistent_volume_claim",
            "persistentVolumeClaim",
        )
        empty_dir_source = _resource_field(
            workspace_volume,
            "empty_dir",
            "emptyDir",
        )
        if expected_pvc_name is _UNSPECIFIED_RESOURCE_BINDING:
            observed_claim_name = (
                _resource_field(pvc_source, "claim_name", "claimName")
                if pvc_source is not None
                else None
            )
            if (pvc_source is None) == (empty_dir_source is None) or (
                pvc_source is not None
                and (
                    observed_claim_name != _pvc_name_for(owner)
                    or _resource_field(pvc_source, "read_only", "readOnly")
                    not in {None, False}
                )
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod storage source changed"
                )
        else:
            if expected_pvc_name is None:
                if empty_dir_source is None or pvc_source is not None:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Pod emptyDir binding changed"
                    )
            elif (
                pvc_source is None
                or empty_dir_source is not None
                or _resource_field(pvc_source, "claim_name", "claimName")
                != expected_pvc_name
                or _resource_field(pvc_source, "read_only", "readOnly")
                not in {None, False}
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod PVC binding changed"
                )

        # Volume names are only aliases; the source is the storage authority.
        # Admission or a sidecar must not add a second name for the same PVC and
        # consume it outside the workspace container.
        observed_claim_name = (
            _resource_field(pvc_source, "claim_name", "claimName")
            if pvc_source is not None
            else None
        )
        workspace_source_names = {
            str(_resource_field(volume, "name") or "")
            for volume in volumes
            if observed_claim_name is not None
            and _resource_field(
                _resource_field(
                    volume,
                    "persistent_volume_claim",
                    "persistentVolumeClaim",
                ),
                "claim_name",
                "claimName",
            )
            == observed_claim_name
        }
        if observed_claim_name is not None and workspace_source_names != {
            "workspace-data"
        }:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod PVC source has an alias"
            )
        mounts = _resource_field(
            workspace_containers[0],
            "volume_mounts",
            "volumeMounts",
        )
        if not isinstance(mounts, (list, tuple)):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod volume mounts are malformed"
            )
        workspace_mounts = [
            mount
            for mount in mounts
            if _resource_field(mount, "name") == "workspace-data"
        ]
        if (
            len(workspace_mounts) != 1
            or _resource_field(workspace_mounts[0], "mount_path", "mountPath")
            != "/home/agent-host"
            or _resource_field(workspace_mounts[0], "sub_path", "subPath")
            not in {None, ""}
            or _resource_field(
                workspace_mounts[0],
                "sub_path_expr",
                "subPathExpr",
            )
            not in {None, ""}
            or _resource_field(workspace_mounts[0], "read_only", "readOnly")
            not in {None, False}
            or _resource_field(
                workspace_mounts[0],
                "mount_propagation",
                "mountPropagation",
            )
            is not None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod workspace mount changed"
            )

        workspace_devices = (
            _resource_field(
                workspace_containers[0],
                "volume_devices",
                "volumeDevices",
            )
            or []
        )
        if not isinstance(workspace_devices, (list, tuple)) or any(
            _resource_field(device, "name") in workspace_source_names
            or _resource_field(device, "name") == "workspace-data"
            for device in workspace_devices
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod workspace volume device changed"
            )

        seed_volumes = [
            volume
            for volume in volumes
            if _resource_field(volume, "name") == "code-server-config"
        ]
        seed_mounts = [
            mount
            for mount in mounts
            if _resource_field(mount, "name") == "code-server-config"
        ]
        deterministic_seed_name = self._seed_configmap_name(
            expected_pod_name or owner.pod_name
        )
        seed_source_names = {
            str(_resource_field(volume, "name") or "")
            for volume in volumes
            if _resource_field(
                _resource_field(volume, "config_map", "configMap"),
                "name",
            )
            == deterministic_seed_name
        }
        if seed_source_names and seed_source_names != {"code-server-config"}:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod seed ConfigMap source has an alias"
            )
        sensitive_volume_names = {
            "workspace-data",
            "code-server-config",
            *workspace_source_names,
            *seed_source_names,
        }
        for container_field in (
            "containers",
            "init_containers",
            "ephemeral_containers",
        ):
            raw_containers = getattr(spec, container_field, None) or []
            if not isinstance(raw_containers, (list, tuple)):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod container declaration is malformed"
                )
            for container in raw_containers:
                if container is workspace_containers[0]:
                    continue
                for mount in (
                    _resource_field(
                        container,
                        "volume_mounts",
                        "volumeMounts",
                    )
                    or []
                ):
                    if _resource_field(mount, "name") in sensitive_volume_names:
                        raise WorkspaceRuntimeAuthorityError(
                            "workspace Pod storage has another consumer"
                        )
                for device in (
                    _resource_field(
                        container,
                        "volume_devices",
                        "volumeDevices",
                    )
                    or []
                ):
                    if _resource_field(device, "name") in sensitive_volume_names:
                        raise WorkspaceRuntimeAuthorityError(
                            "workspace Pod storage has another consumer"
                        )
        if not seed_volumes and not seed_mounts:
            if (
                expected_seed_configmap is not _UNSPECIFIED_RESOURCE_BINDING
                and expected_seed_configmap is not None
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod seed ConfigMap binding disappeared"
                )
            return None
        if len(seed_volumes) != 1 or len(seed_mounts) != 1:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod seed ConfigMap binding changed"
            )
        config_map_source = _resource_field(
            seed_volumes[0],
            "config_map",
            "configMap",
        )
        seed_name = _resource_field(config_map_source, "name")
        deterministic_name = deterministic_seed_name
        if (
            config_map_source is None
            or seed_name != deterministic_name
            or _resource_field(seed_mounts[0], "mount_path", "mountPath")
            != "/mnt/code-server-config"
            or _resource_field(seed_mounts[0], "read_only", "readOnly") is not True
            or _resource_field(seed_mounts[0], "sub_path", "subPath") not in {None, ""}
            or _resource_field(seed_mounts[0], "sub_path_expr", "subPathExpr")
            not in {None, ""}
            or (
                expected_seed_configmap is not _UNSPECIFIED_RESOURCE_BINDING
                and expected_seed_configmap != seed_name
            )
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod seed ConfigMap binding changed"
            )

        return str(seed_name)

    def _workspace_pvc_name_from_pod(
        self,
        pod: Any,
        *,
        owner: WorkspaceOwner,
    ) -> str | None:
        """Return an exact Pod's immutable storage plan after validating it."""

        self._require_stateless_pod_storage_binding(
            pod,
            owner=owner,
            expected_pvc_name=_UNSPECIFIED_RESOURCE_BINDING,
            expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
        )
        spec = getattr(pod, "spec", None)
        volumes = getattr(spec, "volumes", None) or []
        workspace_volume = next(
            volume
            for volume in volumes
            if _resource_field(volume, "name") == "workspace-data"
        )
        pvc_source = _resource_field(
            workspace_volume,
            "persistent_volume_claim",
            "persistentVolumeClaim",
        )
        if pvc_source is None:
            return None
        return str(_resource_field(pvc_source, "claim_name", "claimName"))

    def _workspace_resource_requests_from_pod(self, pod: Any) -> tuple[str, str]:
        containers = getattr(getattr(pod, "spec", None), "containers", None)
        if not isinstance(containers, (list, tuple)):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod resource declaration is malformed"
            )
        workspaces = [
            container
            for container in containers
            if _resource_field(container, "name") == "workspace"
        ]
        if len(workspaces) != 1:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod resource identity changed"
            )
        requests = _resource_field(
            _resource_field(workspaces[0], "resources"),
            "requests",
        )
        if not isinstance(requests, dict):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod resource requests are malformed"
            )
        cpu = requests.get("cpu")
        memory = requests.get("memory")
        if not all(
            isinstance(value, str) and value and "\x00" not in value
            for value in (cpu, memory)
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod resource requests are malformed"
            )
        return cpu, memory

    def _require_workspace_pod_connection_identity(
        self,
        pod: Any,
        *,
        owner: WorkspaceOwner,
        expected_runtime_incarnation: str,
        expected_creation_generation: str | None,
        expected_network_tier: str | None,
        expected_pvc_name: str | None | object,
        expected_seed_configmap: str | None | object,
        expected_pod_name: str | None = None,
        expected_component: str | None = None,
    ) -> str:
        if expected_creation_generation is not None:
            return self._require_stateless_pod_identity(
                pod,
                owner=owner,
                generation=expected_creation_generation,
                expected_runtime_incarnation=expected_runtime_incarnation,
                expected_network_tier=expected_network_tier,
                expected_pvc_name=expected_pvc_name,
                expected_seed_configmap=expected_seed_configmap,
            )
        observed_runtime = self._require_workspace_pod_owner(
            pod,
            owner=owner,
            allow_owner_unlabeled=False,
            expected_network_tier=expected_network_tier,
            expected_pod_name=expected_pod_name,
            expected_component=expected_component,
        )
        if observed_runtime != expected_runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError("workspace Pod UID changed")
        self._require_stateless_pod_storage_binding(
            pod,
            owner=owner,
            expected_pvc_name=expected_pvc_name,
            expected_seed_configmap=expected_seed_configmap,
            expected_pod_name=expected_pod_name,
        )
        return observed_runtime

    async def _managed_repository_process_zero_replay_authority(
        self,
        owner: WorkspaceOwner,
        *,
        scope: str,
        runtime_incarnation: str,
    ) -> str | None:
        """Classify one exact normal or stale Kubernetes process-zero receipt.

        A committed finalizer patch can lose its response and leave a later
        cleanup attempt observing only Pod 404. Normal receipts remain valid
        while the owner points at that runtime. Migration 0197 additionally
        permits an exact stale receipt while the locked owner points at a
        different, structurally valid Kubernetes runtime. Do not probe a
        dynamically fabricated stale method on test doubles: production
        fallback authority must be implemented on the database class itself.
        """

        if self._db is None or scope not in {"workspace_container", "ide"}:
            return None
        arguments = {
            "owner_kind": ("thread" if owner.kind == "session" else "job"),
            "scope": scope,
            "provisioner": "k8s",
            "runtime_incarnation": runtime_incarnation,
        }
        current = getattr(
            self._db,
            "managed_repository_workspace_process_zero_is_current",
            None,
        )
        if callable(current):
            current_result = current(owner.id, **arguments)
            if inspect.isawaitable(current_result) and await current_result:
                return "current"
        stale = getattr(
            type(self._db),
            "stale_managed_repository_workspace_process_zero_is_current",
            None,
        )
        if callable(stale):
            stale_result = stale(self._db, owner.id, **arguments)
            if inspect.isawaitable(stale_result) and await stale_result:
                return "stale"
        orphan = getattr(
            type(self._db),
            "orphan_managed_repository_workspace_process_zero_is_current",
            None,
        )
        if callable(orphan):
            orphan_result = orphan(self._db, owner.id, **arguments)
            if inspect.isawaitable(orphan_result) and await orphan_result:
                return "orphan"
        return None

    async def _managed_repository_process_zero_replay_is_current(
        self,
        owner: WorkspaceOwner,
        *,
        scope: str,
        runtime_incarnation: str,
    ) -> bool:
        return (
            await self._managed_repository_process_zero_replay_authority(
                owner,
                scope=scope,
                runtime_incarnation=runtime_incarnation,
            )
            is not None
        )

    async def _settle_current_workspace_context_after_process_zero(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
    ) -> bool:
        """CAS the exact current runtime to deleted after process-zero proof."""

        settle_current = getattr(
            type(self._db),
            "settle_managed_repository_workspace_after_process_zero",
            None,
        )
        if not callable(settle_current):
            return False
        return bool(
            await settle_current(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                runtime_incarnation=runtime_incarnation,
            )
        )

    async def _workspace_process_zero_cleanup_state(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
    ) -> (
        Literal["capture_pending", "current", "pending", "settled", "superseded"] | None
    ):
        """Read exact cleanup-fence state without accepting permissive mocks."""

        read_state = getattr(
            type(self._db),
            "managed_repository_workspace_process_zero_cleanup_state",
            None,
        )
        if not callable(read_state):
            return None
        state = await read_state(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            runtime_incarnation=runtime_incarnation,
        )
        return (
            state
            if state
            in {"capture_pending", "current", "pending", "settled", "superseded"}
            else None
        )

    def workspace_cleanup_location(self, owner: WorkspaceOwner) -> dict[str, str]:
        """The exact names this provisioner observes during cleanup capture."""

        return {
            "namespace": self._namespace,
            "pod": owner.pod_name,
            "seedConfigMap": self._seed_configmap_name(owner.pod_name),
            "pvc": _pvc_name_for(owner),
            "service": owner.pod_name,
        }

    async def prepare_workspace_cleanup_intent(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        target_disposition: Literal["deleted", "suspended"],
        reclaim_shared_resources: bool,
        suspended_at: str | None = None,
        identity: WorkspaceTeardownIdentity | None = None,
        snapshot_restore_required: bool = False,
        allow_orphan: bool = False,
        allow_stale_predecessor: bool = False,
        admission_source: Literal["automatic", "explicit"] = "explicit",
        _mutation_guard_held: bool = False,
    ) -> dict[str, Any] | None:
        """Capture and persist exact cleanup authority before Kubernetes I/O."""

        if self._db is None or not self._k8s_available:
            return None
        try:
            runtime = _canonical_runtime_uuid(
                expected_runtime_incarnation,
                label="workspace cleanup runtime",
            )
        except ValueError:
            return None
        get_intent = getattr(
            type(self._db),
            "get_managed_repository_workspace_cleanup_intent",
            None,
        )
        if callable(get_intent):
            existing = await get_intent(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope="workspace_container",
                runtime_incarnation=runtime,
            )
            if (
                isinstance(existing, dict)
                and existing.get("resources_captured_at") is not None
                and str(existing.get("target_disposition") or "") == target_disposition
                and bool(existing.get("reclaim_shared_resources"))
                is reclaim_shared_resources
            ):
                return existing
        automatic_admission_enabled = False
        if admission_source == "automatic":
            automatic_admission_enabled = (
                await self._workspace_cleanup_automatic_admission_is_safe()
            )
            if not automatic_admission_enabled:
                return None
        if not allow_stale_predecessor:
            # Reconciliation, finalizer release and exact deletion already own
            # this physical mutation domain on a dedicated DB connection.
            # Re-entering the public wrapper would wait on that same owner.
            cancel_creation = (
                self._request_workspace_creation_cancellation_guarded
                if _mutation_guard_held
                else self.request_workspace_creation_cancellation
            )
            cancellation = await cancel_creation(
                owner,
                target_disposition=target_disposition,
                reclaim_shared_resources=reclaim_shared_resources,
                suspended_at=suspended_at,
                snapshot_restore_required=snapshot_restore_required,
            )
            if isinstance(cancellation, dict):
                outcome = str(cancellation.get("reconciliation_outcome") or "")
                if outcome == "retryable":
                    return None
                if outcome == "handed_off" and callable(get_intent):
                    handed_off = await get_intent(
                        self._db,
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope="workspace_container",
                        runtime_incarnation=runtime,
                    )
                    if isinstance(handed_off, dict):
                        return handed_off
        prepare = getattr(
            type(self._db),
            "prepare_managed_repository_workspace_cleanup_intent",
            None,
        )
        if not callable(prepare):
            return None
        prepared = await prepare(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            runtime_incarnation=runtime,
            target_disposition=target_disposition,
            reclaim_shared_resources=reclaim_shared_resources,
            pod_uid=runtime,
            resources_captured=False,
            suspended_at=suspended_at,
            snapshot_restore_required=snapshot_restore_required,
            allow_orphan=allow_orphan,
            allow_stale_predecessor=allow_stale_predecessor,
            admission_source=admission_source,
            automatic_admission_enabled=automatic_admission_enabled,
        )
        if not isinstance(prepared, dict):
            return None
        claim = getattr(
            type(self._db),
            "claim_managed_repository_workspace_cleanup_intent",
            None,
        )
        record_resources = getattr(
            type(self._db),
            "record_managed_repository_workspace_cleanup_resources",
            None,
        )
        if not callable(claim) or not callable(record_resources):
            return prepared
        claimant = str(prepared.get("claimed_by") or f"cleanup-capture:{uuid4()}")
        claimed = await claim(
            self._db,
            str(prepared["id"]),
            claimant=claimant,
            lease_seconds=300,
        )
        if not isinstance(claimed, dict):
            return prepared
        if allow_stale_predecessor:
            # A replacement B may already own every deterministic name.  The
            # stale generation records only A's immutable Pod UID and gains no
            # deletion authority over seed/PVC/Service objects.
            captured = await record_resources(
                self._db,
                str(prepared["id"]),
                claimant=claimant,
                claim_token=int(claimed["claim_token"]),
                pod_uid=runtime,
                seed_configmap_uid=None,
                pvc_uid=None,
                service_uid=None,
            )
            return captured if isinstance(captured, dict) else claimed
        if identity is None:
            try:
                identity = await self.capture_workspace_teardown_identity(
                    owner,
                    expected_runtime_incarnation=runtime,
                )
            except (WorkspaceRuntimeAuthorityError, ValueError):
                return claimed
        if identity.pod_uid not in {None, runtime}:
            return claimed
        captured = await record_resources(
            self._db,
            str(prepared["id"]),
            claimant=claimant,
            claim_token=int(claimed["claim_token"]),
            pod_uid=runtime,
            seed_configmap_uid=identity.seed_configmap_uid,
            pvc_uid=identity.pvc_uid,
            service_uid=identity.service_uid,
            resource_location=self.workspace_cleanup_location(owner),
        )
        return captured if isinstance(captured, dict) else claimed

    async def request_workspace_creation_cancellation(
        self,
        owner: WorkspaceOwner,
        *,
        target_disposition: Literal["deleted", "suspended"],
        reclaim_shared_resources: bool,
        suspended_at: str | None = None,
        snapshot_restore_required: bool = False,
    ) -> dict[str, Any] | None:
        """Fence a pre-Pod generation for same-generation reconciliation."""

        if self._db is None:
            return None
        async with self._workspace_mutation_guard(
            owner, scope="workspace_container"
        ) as mutation_owned:
            if not mutation_owned:
                return None
            return await self._request_workspace_creation_cancellation_guarded(
                owner,
                target_disposition=target_disposition,
                reclaim_shared_resources=reclaim_shared_resources,
                suspended_at=suspended_at,
                snapshot_restore_required=snapshot_restore_required,
            )

    async def _request_workspace_creation_cancellation_guarded(
        self,
        owner: WorkspaceOwner,
        *,
        target_disposition: Literal["deleted", "suspended"],
        reclaim_shared_resources: bool,
        suspended_at: str | None = None,
        snapshot_restore_required: bool = False,
    ) -> dict[str, Any] | None:
        """Cancel while the physical owner/scope mutation guard is held."""

        cancel = getattr(
            type(self._db),
            "request_managed_repository_workspace_creation_cancellation",
            None,
        )
        if not callable(cancel):
            return None
        claimant = f"creation-cancellation:{uuid4()}"
        reservation = await cancel(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            target_disposition=target_disposition,
            reclaim_shared_resources=reclaim_shared_resources,
            claimant=claimant,
            suspended_at=suspended_at,
            snapshot_restore_required=snapshot_restore_required,
        )
        if not isinstance(reservation, dict):
            return None
        if reservation.get("settled_at") is not None:
            return {**reservation, "reconciliation_outcome": "aborted"}
        outcome = await self._reconcile_claimed_workspace_creation_reservation(
            reservation, _mutation_guard_held=True
        )
        return {**reservation, "reconciliation_outcome": outcome}

    async def get_settled_workspace_suspension(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
    ) -> dict[str, Any] | None:
        """Return only an exact settled preserve suspension generation."""

        if self._db is None:
            return None
        try:
            runtime = _canonical_runtime_uuid(
                expected_runtime_incarnation, label="suspended workspace runtime"
            )
        except ValueError:
            return None
        read = getattr(
            type(self._db),
            "get_managed_repository_workspace_cleanup_intent",
            None,
        )
        if not callable(read):
            return None
        intent = await read(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            runtime_incarnation=runtime,
        )
        if not isinstance(intent, dict) or (
            str(intent.get("target_disposition") or "") != "suspended"
            or str(intent.get("resource_policy") or "") != "preserve"
            or str(intent.get("result_kind") or "") != "settled"
            or intent.get("cleanup_completed_at") is None
            or intent.get("settled_at") is None
        ):
            return None
        return intent

    async def _cleanup_claim_is_current(
        self,
        intent: dict[str, Any],
        *,
        claimant: str,
    ) -> bool:
        check = getattr(
            type(self._db),
            "managed_repository_workspace_cleanup_claim_is_current",
            None,
        )
        return bool(
            callable(check)
            and await check(
                self._db,
                str(intent["id"]),
                claimant=claimant,
                claim_token=int(intent["claim_token"]),
            )
        )

    async def _terminal_cleanup_claim_is_current(
        self,
        intent: dict[str, Any],
        *,
        claimant: str,
    ) -> bool:
        check = getattr(
            type(self._db),
            "terminal_workspace_cleanup_claim_is_current",
            None,
        )
        return bool(
            callable(check)
            and await check(
                self._db,
                str(intent["id"]),
                claimant=claimant,
                claim_token=int(intent["claim_token"]),
            )
        )

    async def _captured_workspace_resource_is_absent(
        self,
        owner: WorkspaceOwner,
        *,
        resource: Literal["pvc", "service"],
    ) -> bool:
        try:
            if resource == "pvc":
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=_pvc_name_for(owner),
                    namespace=self._namespace,
                )
            else:
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
        except Exception as error:
            return getattr(error, "status", None) == 404
        return False

    async def _exact_workspace_pod_never_started(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
    ) -> bool:
        """Whether the exact, retained, non-deleting Pod never ran a container."""

        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
            observed = self._require_workspace_pod_owner(
                pod, owner=owner, allow_owner_unlabeled=False
            )
        except Exception:
            return False
        return bool(
            observed == runtime_incarnation
            and self._has_stateless_process_zero_finalizer(pod)
            and _pod_never_started_a_container(pod)
        )

    async def replay_terminal_workspace_cleanup(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
    ) -> WorkspaceCleanupOutcome | None:
        """Retry a captured job cleanup after its exact Pod has disappeared.

        A fresh/live cleanup still uses terminal identity and SSH capture.
        An absent Pod may instead replay the existing terminal-reclaim tuple,
        but only with its exact current process-zero receipt. No new intent or
        resource identity is admitted for an absent Pod.

        One live case needs no SSH either: the exact Pod never started a
        container (a custom image that could not be pulled). It has no endpoint
        to attest and nothing to snapshot, so the terminal-reclaim intent is
        reconciled as the lifecycle sweep would: live identity capture, then
        the finalizer protocol, which still waits for every container to be
        observed terminated before the receipt and the finalizer release.
        """

        read = getattr(
            type(self._db), "get_managed_repository_workspace_cleanup_intent", None
        )
        if owner.kind != "job" or not callable(read) or not self._k8s_available:
            return None
        intent = await read(
            self._db,
            owner.id,
            owner_kind="job",
            scope="workspace_container",
            runtime_incarnation=expected_runtime_incarnation,
        )
        if not isinstance(intent, dict) or (
            intent.get("target_disposition") != "deleted"
            or intent.get("resource_policy") != "terminal_reclaim"
            or intent.get("reclaim_shared_resources") is not True
        ):
            return None
        authority = await self.workspace_pod_authority(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
        if authority == "exact_live" and await self._exact_workspace_pod_never_started(
            owner, runtime_incarnation=expected_runtime_incarnation
        ):
            return await self.reconcile_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                intent_generation=int(intent["intent_generation"]),
            )
        if (
            intent.get("capture_complete") is not True
            or intent.get("resources_captured_at") is None
        ):
            return None
        if authority in {"exact_live", "exact_terminal"}:
            return None
        if authority != "exact_absent" or (
            await self._managed_repository_process_zero_replay_authority(
                owner,
                scope="workspace_container",
                runtime_incarnation=expected_runtime_incarnation,
            )
            != "current"
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        return await self.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
            intent_generation=int(intent["intent_generation"]),
        )

    async def reconcile_workspace_cleanup_intent(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        intent_generation: int | None = None,
    ) -> WorkspaceCleanupOutcome:
        """Idempotently reconcile one durable cleanup intent to settlement."""

        if self._db is None or not self._k8s_available:
            return _WORKSPACE_CLEANUP_RETRYABLE
        async with self._workspace_mutation_guard(
            owner, scope="workspace_container"
        ) as mutation_owned:
            if not mutation_owned:
                return _WORKSPACE_CLEANUP_RETRYABLE
            return await self._reconcile_workspace_cleanup_intent_guarded(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                intent_generation=intent_generation,
            )

    async def _reconcile_workspace_cleanup_intent_guarded(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str,
        intent_generation: int | None = None,
    ) -> WorkspaceCleanupOutcome:
        """Reconcile after taking the dedicated cross-replica mutation guard."""

        get_intent = getattr(
            type(self._db),
            "get_managed_repository_workspace_cleanup_intent",
            None,
        )
        claim_intent = getattr(
            type(self._db),
            "claim_managed_repository_workspace_cleanup_intent",
            None,
        )
        settle_intent = getattr(
            type(self._db),
            "settle_managed_repository_workspace_cleanup_intent",
            None,
        )
        supersede_intent = getattr(
            type(self._db),
            "supersede_managed_repository_workspace_cleanup_intent",
            None,
        )
        if not all(
            callable(method)
            for method in (get_intent, claim_intent, settle_intent, supersede_intent)
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        intent = await get_intent(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            runtime_incarnation=expected_runtime_incarnation,
            intent_generation=intent_generation,
        )
        if not isinstance(intent, dict):
            return _WORKSPACE_CLEANUP_RETRYABLE
        if intent.get("result_kind") == "settled":
            if (
                owner.kind == "session"
                and intent.get("resource_policy") == "terminal_reclaim"
            ):
                if not await self._db.restore_settled_thread_workspace_cleanup_projection(
                    owner.id,
                    runtime_incarnation=expected_runtime_incarnation,
                    intent_generation=int(intent["intent_generation"]),
                ):
                    return _WORKSPACE_CLEANUP_RETRYABLE
            return WorkspaceCleanupOutcome("settled", int(intent["intent_generation"]))
        if intent.get("result_kind") == "superseded":
            return WorkspaceCleanupOutcome(
                "superseded", int(intent["intent_generation"])
            )
        if str(intent.get("target_disposition") or "") == "ambiguous":
            return _WORKSPACE_CLEANUP_RETRYABLE

        if intent.get("resources_captured_at") is None:
            suspended_value = intent.get("suspended_at")
            intent = await self.prepare_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                target_disposition=str(intent["target_disposition"]),
                reclaim_shared_resources=bool(intent.get("reclaim_shared_resources")),
                suspended_at=(
                    suspended_value.isoformat()
                    if hasattr(suspended_value, "isoformat")
                    else suspended_value
                ),
                snapshot_restore_required=bool(intent.get("snapshot_restore_required")),
                allow_orphan=(str(intent.get("intent_source")) == "orphan"),
                _mutation_guard_held=True,
            )
            if (
                not isinstance(intent, dict)
                or intent.get("resources_captured_at") is None
            ):
                return _WORKSPACE_CLEANUP_RETRYABLE

        claimant = str(intent.get("claimed_by") or f"container-provisioner:{uuid4()}")
        claimed = await claim_intent(
            self._db,
            str(intent["id"]),
            claimant=claimant,
            lease_seconds=300,
        )
        if not isinstance(claimed, dict):
            return _WORKSPACE_CLEANUP_RETRYABLE
        intent = claimed
        captured_pod_uid = intent.get("pod_uid")
        if captured_pod_uid is not None and str(captured_pod_uid) != str(
            expected_runtime_incarnation
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        deletion = await self.delete_workspace_with_outcome(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
            wait_for_exact_absence=True,
            **(
                {"captured_teardown_uid": str(captured_pod_uid)}
                if captured_pod_uid is not None
                else {}
            ),
            cleanup_intent=intent,
            _mutation_guard_held=True,
        )
        if deletion.stale_target_settled:
            if await supersede_intent(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope="workspace_container",
                runtime_incarnation=expected_runtime_incarnation,
                claimant=claimant,
                claim_token=int(intent["claim_token"]),
            ):
                return WorkspaceCleanupOutcome(
                    "superseded", int(intent["intent_generation"])
                )
            return _WORKSPACE_CLEANUP_RETRYABLE
        if not deletion.current_deleted or not await self._cleanup_claim_is_current(
            intent, claimant=claimant
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE

        if bool(intent.get("reclaim_shared_resources")):
            # A terminal reclaim waits for every pre-intent create request to
            # cross its bounded Kubernetes call, then checks the stable name
            # before each irreversible delete. New create paths are fenced by
            # the pending DB intent; a surviving same-name Pod fails closed.
            if (
                not await self._terminal_cleanup_claim_is_current(
                    intent, claimant=claimant
                )
                or await self.workspace_pod_authority(
                    owner,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
                != "exact_absent"
            ):
                return _WORKSPACE_CLEANUP_RETRYABLE
            pvc_uid = intent.get("pvc_uid")
            if pvc_uid is None:
                pvc_absent = await self._captured_workspace_resource_is_absent(
                    owner, resource="pvc"
                )
            else:
                pvc_outcome = await self._delete_pvc_outcome(
                    _pvc_name_for(owner),
                    expected_owner=owner,
                    expected_uid=str(pvc_uid),
                )
                pvc_absent = pvc_outcome.captured_absent
            if not pvc_absent or not await self._cleanup_claim_is_current(
                intent, claimant=claimant
            ):
                return _WORKSPACE_CLEANUP_RETRYABLE
            if (
                not await self._terminal_cleanup_claim_is_current(
                    intent, claimant=claimant
                )
                or await self.workspace_pod_authority(
                    owner,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
                != "exact_absent"
            ):
                return _WORKSPACE_CLEANUP_RETRYABLE
            service_uid = intent.get("service_uid")
            if service_uid is None:
                service_absent = await self._captured_workspace_resource_is_absent(
                    owner, resource="service"
                )
            else:
                service_outcome = await self._delete_service_outcome(
                    owner,
                    require_exact_owner=True,
                    expected_uid=str(service_uid),
                )
                service_absent = service_outcome.captured_absent
            if not service_absent:
                return _WORKSPACE_CLEANUP_RETRYABLE
        else:
            # The headless Service goes on EVERY settled cleanup, not only a
            # terminal reclaim. ``release_workspace`` rules on this in its own
            # docstring — it is 409-idempotent to recreate on the next
            # ``create_workspace``, so unlike the volume it costs nothing to
            # lose — and both sibling teardown paths already honour it
            # (``release_absent_workspace`` unconditionally,
            # ``_release_pinned_retirement_workspace`` on the captured uid
            # alone). Keeping it only here left a Service outliving its Pod
            # after every non-permanent End, and a Service that survives its
            # Pod is a stale selector waiting to name a successor.
            #
            # The volume is the opposite case and stays gated: an ``ended``
            # thread is resumable, so its PVC is reclaimed only once the thread
            # row itself is gone (``workspace_manager._is_volume_reclaimable``).
            #
            # Fences, in order: the Pod for this exact incarnation is already
            # proven gone (``deletion.current_deleted`` above came from a
            # ``wait_for_exact_absence`` delete), the mutation guard is held,
            # and the delete names the captured Service uid so a successor's
            # Service is never touched. ``_delete_service`` — not the stricter
            # ``captured_absent`` the terminal branch needs — is deliberate and
            # matches ``_release_pinned_retirement_workspace``: a same-name
            # replacement means our captured Service is already gone, which is
            # success here, while a refused API call still fails closed. The
            # terminal branch must instead prove exact absence, because settling
            # a reclaim is irreversible.
            service_uid = intent.get("service_uid")
            if service_uid is not None and not await self._delete_service(
                owner,
                require_exact_owner=True,
                expected_uid=str(service_uid),
            ):
                return _WORKSPACE_CLEANUP_RETRYABLE

        if not await self._cleanup_claim_is_current(intent, claimant=claimant):
            return _WORKSPACE_CLEANUP_RETRYABLE
        settled = await settle_intent(
            self._db,
            owner.id,
            owner_kind=("thread" if owner.kind == "session" else "job"),
            scope="workspace_container",
            runtime_incarnation=expected_runtime_incarnation,
            intent_generation=int(intent["intent_generation"]),
            claimant=claimant,
            claim_token=int(intent["claim_token"]),
        )
        if not settled:
            return _WORKSPACE_CLEANUP_RETRYABLE
        await workspace_metering.close_interval(self._db, owner)
        return WorkspaceCleanupOutcome("settled", int(intent["intent_generation"]))

    async def prepare_ide_cleanup_intent(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str,
        target_disposition: Literal["expired", "deleted"] = "expired",
        allow_orphan: bool = False,
        allow_stale_predecessor: bool = False,
        admission_source: Literal["automatic", "explicit"] = "explicit",
    ) -> dict[str, Any] | None:
        """Persist and capture one exact IDE cleanup generation DB-first."""

        if self._db is None or not self._k8s_available:
            return None
        try:
            runtime = _canonical_runtime_uuid(
                expected_runtime_incarnation, label="IDE cleanup runtime"
            )
        except ValueError:
            return None
        owner = WorkspaceOwner.job(job_id)
        get_intent = getattr(
            type(self._db),
            "get_managed_repository_workspace_cleanup_intent",
            None,
        )
        prepare = getattr(
            type(self._db),
            "prepare_managed_repository_workspace_cleanup_intent",
            None,
        )
        claim = getattr(
            type(self._db),
            "claim_managed_repository_workspace_cleanup_intent",
            None,
        )
        capture = getattr(
            type(self._db),
            "record_managed_repository_workspace_cleanup_resources",
            None,
        )
        if not all(callable(method) for method in (prepare, claim, capture)):
            return None
        existing = (
            await get_intent(
                self._db,
                job_id,
                owner_kind="job",
                scope="ide",
                runtime_incarnation=runtime,
            )
            if callable(get_intent)
            else None
        )
        automatic_admission_enabled = False
        if not isinstance(existing, dict) and admission_source == "automatic":
            automatic_admission_enabled = (
                await self._workspace_cleanup_automatic_admission_is_safe()
            )
            if not automatic_admission_enabled:
                return None
        prepared = existing
        if not isinstance(prepared, dict):
            prepared = await prepare(
                self._db,
                job_id,
                owner_kind="job",
                scope="ide",
                runtime_incarnation=runtime,
                target_disposition=target_disposition,
                reclaim_shared_resources=False,
                pod_uid=runtime,
                resources_captured=False,
                allow_orphan=allow_orphan,
                allow_stale_predecessor=allow_stale_predecessor,
                admission_source=admission_source,
                automatic_admission_enabled=automatic_admission_enabled,
            )
        if not isinstance(prepared, dict):
            return None
        if (
            prepared.get("resources_captured_at") is not None
            and isinstance(prepared.get("claimed_by"), str)
            and prepared.get("claimed_by")
            and await self._cleanup_claim_is_current(
                prepared, claimant=str(prepared["claimed_by"])
            )
        ):
            return prepared
        claimant = f"ide-cleanup-capture:{uuid4()}"
        claimed = await claim(
            self._db,
            str(prepared["id"]),
            claimant=claimant,
            lease_seconds=300,
        )
        if not isinstance(claimed, dict):
            return None
        if allow_stale_predecessor:
            captured = await capture(
                self._db,
                str(prepared["id"]),
                claimant=claimant,
                claim_token=int(claimed["claim_token"]),
                pod_uid=runtime,
                seed_configmap_uid=None,
                pvc_uid=None,
                service_uid=None,
            )
            return captured if isinstance(captured, dict) else None
        pod_name = f"ide-{job_id[:12]}"
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            observed = self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
                expected_pod_name=pod_name,
                expected_component="ide-session",
            )
            if observed != runtime:
                return None
            seed_name = self._require_stateless_pod_storage_binding(
                pod,
                owner=owner,
                expected_pvc_name=None,
                expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
                expected_pod_name=pod_name,
            )
            seed_uid = None
            if seed_name is not None:
                seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_name,
                    namespace=self._namespace,
                )
                seed_uid = self._require_stateless_seed_configmap_identity(
                    seed, owner=owner, pod_name=pod_name
                )
                self._require_seed_configmap_pod_owner_reference(
                    seed,
                    pod_name=pod_name,
                    runtime_incarnation=runtime,
                )
        except Exception:
            return None
        captured = await capture(
            self._db,
            str(prepared["id"]),
            claimant=claimant,
            claim_token=int(claimed["claim_token"]),
            pod_uid=runtime,
            seed_configmap_uid=seed_uid,
            pvc_uid=None,
            service_uid=None,
        )
        return captured if isinstance(captured, dict) else None

    async def reconcile_ide_cleanup_intent(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str,
        intent_generation: int | None = None,
    ) -> WorkspaceCleanupOutcome:
        """Delete and atomically settle one exact IDE cleanup generation."""

        if self._db is None or not self._k8s_available:
            return _WORKSPACE_CLEANUP_RETRYABLE
        owner = WorkspaceOwner.job(job_id)
        async with self._workspace_mutation_guard(owner, scope="ide") as mutation_owned:
            if not mutation_owned:
                return _WORKSPACE_CLEANUP_RETRYABLE
            return await self._reconcile_ide_cleanup_intent_guarded(
                job_id,
                expected_runtime_incarnation=expected_runtime_incarnation,
                intent_generation=intent_generation,
            )

    async def _reconcile_ide_cleanup_intent_guarded(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str,
        intent_generation: int | None = None,
    ) -> WorkspaceCleanupOutcome:
        """Reconcile IDE deletion with the owner/scope guard held."""

        get_intent = getattr(
            type(self._db),
            "get_managed_repository_workspace_cleanup_intent",
            None,
        )
        claim = getattr(
            type(self._db),
            "claim_managed_repository_workspace_cleanup_intent",
            None,
        )
        settle = getattr(
            type(self._db),
            "settle_managed_repository_workspace_cleanup_intent",
            None,
        )
        supersede = getattr(
            type(self._db),
            "supersede_managed_repository_workspace_cleanup_intent",
            None,
        )
        if not all(
            callable(method) for method in (get_intent, claim, settle, supersede)
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        intent = await get_intent(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            runtime_incarnation=expected_runtime_incarnation,
            intent_generation=intent_generation,
        )
        if not isinstance(intent, dict):
            return _WORKSPACE_CLEANUP_RETRYABLE
        if intent.get("result_kind") in {"settled", "superseded"}:
            return WorkspaceCleanupOutcome(
                str(intent["result_kind"]), int(intent["intent_generation"])
            )
        claimant = str(intent.get("claimed_by") or f"ide-cleanup:{uuid4()}")
        claimed = await claim(
            self._db,
            str(intent["id"]),
            claimant=claimant,
            lease_seconds=300,
        )
        if not isinstance(claimed, dict):
            return _WORKSPACE_CLEANUP_RETRYABLE
        deletion = await self.delete_ide_pod_with_outcome(
            job_id,
            expected_runtime_incarnation=expected_runtime_incarnation,
            cleanup_intent=claimed,
            _mutation_guard_held=True,
        )
        if deletion.stale_target_settled:
            if await supersede(
                self._db,
                job_id,
                owner_kind="job",
                scope="ide",
                runtime_incarnation=expected_runtime_incarnation,
                claimant=claimant,
                claim_token=int(claimed["claim_token"]),
            ):
                return WorkspaceCleanupOutcome(
                    "superseded", int(claimed["intent_generation"])
                )
            return _WORKSPACE_CLEANUP_RETRYABLE
        if not deletion.current_deleted or not await self._cleanup_claim_is_current(
            claimed, claimant=claimant
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        if not await settle(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            runtime_incarnation=expected_runtime_incarnation,
            intent_generation=int(claimed["intent_generation"]),
            claimant=claimant,
            claim_token=int(claimed["claim_token"]),
        ):
            return _WORKSPACE_CLEANUP_RETRYABLE
        return WorkspaceCleanupOutcome("settled", int(claimed["intent_generation"]))

    async def reconcile_pending_workspace_cleanup_intents(
        self,
        *,
        limit: int = 25,
    ) -> dict[str, int]:
        """Boundedly retry durable cleanup intents after process restart."""

        # The rollout flag controls discovery/start of autonomous historical
        # work.  It must not strand a generation that an explicit lifecycle
        # request already committed before its process crashed.
        await self.reconcile_pending_workspace_creation_reservations(limit=limit)
        if await self._workspace_cleanup_automatic_admission_is_safe():
            await self._reconcile_stale_or_orphan_workspace_finalizers(limit=limit)
        list_pending = getattr(
            type(self._db),
            "list_pending_managed_repository_workspace_cleanup_intents",
            None,
        )
        if not callable(list_pending):
            return {"settled": 0, "superseded": 0, "retryable": 0}
        rows = await list_pending(self._db, limit=limit)
        counts = {"settled": 0, "superseded": 0, "retryable": 0}
        for intent in rows:
            # A durable generation owns cleanup independently of the current
            # rollout flag and source classification. Dark mode prevents new
            # automatic admission/discovery above; it must never strand an
            # already-committed historical or orphan generation mid-delete.
            owner = (
                WorkspaceOwner.session(str(intent["owner_id"]))
                if intent["owner_kind"] == "thread"
                else WorkspaceOwner.job(str(intent["owner_id"]))
            )
            if intent["scope"] == "workspace_container":
                outcome = await self.reconcile_workspace_cleanup_intent(
                    owner,
                    expected_runtime_incarnation=str(intent["runtime_incarnation"]),
                    intent_generation=int(intent["intent_generation"]),
                )
            else:
                outcome = await self.reconcile_ide_cleanup_intent(
                    str(intent["owner_id"]),
                    expected_runtime_incarnation=str(intent["runtime_incarnation"]),
                    intent_generation=int(intent["intent_generation"]),
                )
            counts[outcome.state] += 1
        return counts

    async def _reconcile_stale_or_orphan_workspace_finalizers(
        self,
        *,
        limit: int,
    ) -> None:
        """Discover retained exact terminal Pods missing a 0197 intent.

        Only an absent owner or an owner already naming a different valid
        runtime is eligible.  Current runtimes—including unresolved terminal
        thread retirement—remain fail-closed for their explicit lifecycle
        owner instead of being guessed clean by the historical sweeper.
        """

        if self._db is None or not self._k8s_available or limit < 1:
            return
        owner_exists = getattr(
            type(self._db), "managed_repository_workspace_owner_exists", None
        )
        stale = getattr(
            type(self._db),
            "managed_repository_workspace_runtime_is_different_valid",
            None,
        )
        if not callable(owner_exists) or not callable(stale):
            return
        try:
            listed = await self._bounded_kubernetes_call(
                self._core_api.list_namespaced_pod,
                namespace=self._namespace,
                label_selector="srw.io/component=agent-workspace",
            )
        except Exception:
            return
        considered = 0
        for pod in getattr(listed, "items", None) or []:
            if considered >= limit:
                break
            finalizers = getattr(getattr(pod, "metadata", None), "finalizers", None)
            if not isinstance(finalizers, (list, tuple)) or (
                STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER not in finalizers
            ):
                continue
            labels = getattr(getattr(pod, "metadata", None), "labels", None)
            if not isinstance(labels, dict):
                continue
            job_id = labels.get("srw/job-id")
            thread_id = labels.get("srw/thread-id")
            if bool(job_id) is bool(thread_id):
                continue
            owner = (
                WorkspaceOwner.session(str(thread_id))
                if thread_id
                else WorkspaceOwner.job(str(job_id))
            )
            try:
                owner_id = str(UUID(owner.id))
                runtime = _canonical_runtime_uuid(
                    str(getattr(getattr(pod, "metadata", None), "uid", "") or ""),
                    label="retained workspace Pod UID",
                )
            except (TypeError, ValueError):
                continue
            component = str(labels.get("srw/component") or "")
            if component == "ide-session" and owner.kind != "job":
                continue
            scope = "ide" if component == "ide-session" else "workspace_container"
            expected_name = f"ide-{owner_id[:12]}" if scope == "ide" else owner.pod_name
            if str(getattr(getattr(pod, "metadata", None), "name", "") or "") != (
                expected_name
            ):
                continue
            exists = await owner_exists(
                self._db,
                owner_id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
            )
            eligible = exists is False
            if exists is True:
                eligible = bool(
                    await stale(
                        self._db,
                        owner_id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope=scope,
                        runtime_incarnation=runtime,
                    )
                )
            if not eligible:
                continue
            considered += 1
            await self._release_process_zero_finalizer(
                owner,
                pod_name=expected_name,
                expected_runtime_incarnation=runtime,
                scope=scope,
                expected_component=("ide-session" if scope == "ide" else None),
                admission_source="automatic",
            )

    async def _reconcile_claimed_workspace_creation_reservation(
        self,
        reservation: dict[str, Any],
        *,
        _mutation_guard_held: bool = False,
    ) -> Literal["handed_off", "aborted", "retryable"]:
        """Reconcile one freshly claimed cancellation, independent of sweeps."""

        if self._db is None:
            return "retryable"
        owner = (
            WorkspaceOwner.session(str(reservation["owner_id"]))
            if reservation["owner_kind"] == "thread"
            else WorkspaceOwner.job(str(reservation["owner_id"]))
        )
        if not _mutation_guard_held:
            scope = str(reservation.get("scope") or "")
            if scope not in {"workspace_container", "ide"}:
                return "retryable"
            async with self._workspace_mutation_guard(owner, scope=scope) as owned:
                if not owned:
                    return "retryable"
                return await self._reconcile_claimed_workspace_creation_reservation(
                    reservation, _mutation_guard_held=True
                )
        quiescent = getattr(
            type(self._db),
            "managed_repository_workspace_creation_effects_are_quiescent",
            None,
        )
        if not callable(quiescent) or not await quiescent(
            self._db,
            owner.id,
            owner_kind=str(reservation["owner_kind"]),
            scope=str(reservation["scope"]),
            reservation_generation=int(reservation["reservation_generation"]),
        ):
            return "retryable"
        if (
            str(reservation.get("phase") or "") == "reserved"
            and reservation.get("external_mutation_started_at") is None
        ):
            if reservation.get("settled_at") is not None:
                return "aborted"
            abort = getattr(
                type(self._db),
                "abort_managed_repository_workspace_creation_reservation",
                None,
            )
            return (
                "aborted"
                if callable(abort)
                and await abort(
                    self._db,
                    owner.id,
                    owner_kind=str(reservation["owner_kind"]),
                    scope=str(reservation["scope"]),
                    reservation_generation=int(reservation["reservation_generation"]),
                    claimant=str(reservation["claimed_by"]),
                    claim_token=int(reservation["claim_token"]),
                )
                else "retryable"
            )

        pod_name = (
            f"ide-{owner.id[:12]}" if reservation["scope"] == "ide" else owner.pod_name
        )
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            runtime = self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
                expected_pod_name=pod_name,
                expected_component=(
                    "ide-session" if reservation["scope"] == "ide" else None
                ),
            )
            self._require_workspace_creation_reservation_annotation(
                pod,
                reservation_id=str(reservation["id"]),
            )
        except Exception as error:
            if getattr(error, "status", None) == 404 and (
                await self._reconcile_cancelled_pre_pod_creation(owner, reservation)
            ):
                return "aborted"
            return "retryable"
        if not await self._authorize_cancelled_creation_runtime(
            owner,
            reservation,
            scope=str(reservation["scope"]),
            runtime_incarnation=runtime,
        ):
            return "retryable"
        if not await self._capture_cancelled_runtime_resources(
            owner,
            reservation,
            pod=pod,
            runtime_incarnation=runtime,
        ):
            return "retryable"
        convert = getattr(
            type(self._db),
            "convert_cancelled_workspace_creation_to_cleanup_intent",
            None,
        )
        if not callable(convert):
            return "retryable"
        intent = await convert(
            self._db,
            owner.id,
            owner_kind=str(reservation["owner_kind"]),
            scope=str(reservation["scope"]),
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=runtime,
            allow_existing_terminal_intent=(
                await self._cancelled_creation_has_fresh_unstarted_runtime(
                    owner, reservation, pod=pod
                )
            ),
        )
        return "handed_off" if isinstance(intent, dict) else "retryable"

    async def _cancelled_creation_has_fresh_unstarted_runtime(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        pod: Any,
    ) -> bool:
        """Gate terminal-intent reuse without bypassing a kept volume's archive.

        Called under the creation reconciler's mutation guard after exact Pod
        and reservation checks. Never-started status is not process-zero: the
        ordinary cleanup finalizer still proves termination before deletion.
        """

        if (
            owner.kind != "job"
            or reservation.get("scope") != "workspace_container"
            or reservation.get("operation_kind") != "create"
            or not self._has_stateless_process_zero_finalizer(pod)
            or not _pod_never_started_a_container(pod)
        ):
            return False
        try:
            pvc_name = self._workspace_pvc_name_from_pod(pod, owner=owner)
            if pvc_name is None:
                return reservation.get("pvc_uid") is None
            pvc = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=self._namespace,
            )
            pvc_uid = self._require_stateless_pvc_identity(
                pvc, owner=owner, pvc_name=pvc_name, allow_any_storage_class=True
            )
            self._require_workspace_creation_reservation_annotation(
                pvc, reservation_id=str(reservation["id"])
            )
            return str(reservation.get("pvc_uid") or "") == pvc_uid
        except Exception:
            return False

    async def reconcile_pending_workspace_creation_reservations(
        self,
        *,
        limit: int = 25,
    ) -> dict[str, int]:
        """Reclaim cancelled post-effect generations without minting a new one.

        Only explicitly cancelled generations are eligible.  The rollout flag
        gates discovery of historical work, not completion of a cancellation
        already committed by a supported lifecycle request.
        """

        counts = {"handed_off": 0, "aborted": 0, "retryable": 0}
        if self._db is None or not self._k8s_available:
            return counts
        list_pending = getattr(
            type(self._db),
            "list_pending_managed_repository_workspace_creation_reservations",
            None,
        )
        claim = getattr(
            type(self._db),
            "claim_managed_repository_workspace_creation_reconciliation",
            None,
        )
        if not all(callable(method) for method in (list_pending, claim)):
            return counts
        rows = await list_pending(self._db, limit=limit)
        for observed in rows:
            if observed.get("cancel_requested_at") is None:
                continue
            claimant = f"creation-reconciler:{uuid4()}"
            reservation = await claim(
                self._db,
                str(observed["id"]),
                claimant=claimant,
                lease_seconds=300,
            )
            if not isinstance(reservation, dict):
                counts["retryable"] += 1
                continue
            outcome = await self._reconcile_claimed_workspace_creation_reservation(
                reservation
            )
            counts[outcome] += 1
        return counts

    async def _reconcile_cancelled_pre_pod_creation(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
    ) -> bool:
        """Clean exact partial resources for a generation with no accepted Pod.

        Pod absence is not treated as no-effect.  The deterministic seed,
        PVC, and Service are each inspected and their immutable UID is written
        to the still-claimed reservation before deletion.  Preserve-mode
        cleanup removes only the reservation-owned seed; terminal reclaim may
        additionally remove the captured shared resources.  The database then
        closes the cancellation and publishes its no-runtime projection in one
        owner-locked transaction.
        """

        if self._db is None:
            return False
        scope = str(reservation.get("scope") or "")
        if scope not in {"workspace_container", "ide"}:
            return False
        if (
            reservation.get("runtime_incarnation") is not None
            or reservation.get("pod_uid") is not None
        ):
            return False
        if not await self._cancelled_creation_claim_is_current(
            owner, reservation, scope=scope
        ):
            return False

        pod_name = f"ide-{owner.id[:12]}" if scope == "ide" else owner.pod_name
        seed_name = self._seed_configmap_name(pod_name)
        seed_uid = reservation.get("seed_configmap_uid")
        try:
            seed = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_config_map,
                name=seed_name,
                namespace=self._namespace,
            )
        except Exception as error:
            if getattr(error, "status", None) != 404:
                return False
            seed = None
        if seed is not None:
            try:
                observed_seed_uid = self._require_stateless_seed_configmap_identity(
                    seed,
                    owner=owner,
                    pod_name=pod_name,
                    creation_reservation_id=str(reservation["id"]),
                )
            except WorkspaceRuntimeAuthorityError:
                return False
            if seed_uid is not None and str(seed_uid) != observed_seed_uid:
                return False
            if seed_uid is None and not await self._record_cancelled_creation_resource(
                owner,
                reservation,
                scope=scope,
                resource_kind="seed",
                resource_uid=observed_seed_uid,
            ):
                return False
            seed_uid = observed_seed_uid
        elif seed_uid is not None:
            # Exact captured absence is sufficient; a same-name replacement
            # would have been visible above and refused by UID/annotation.
            seed_uid = str(seed_uid)

        if seed is not None:
            if not await self._cancelled_creation_claim_is_current(
                owner, reservation, scope=scope
            ) or not await self._delete_seed_configmap(
                pod_name,
                expected_owner=owner,
                expected_creation_reservation_id=str(reservation["id"]),
                expected_configmap_uid=str(seed_uid),
            ):
                return False

        policy = str(reservation.get("cancel_resource_policy") or "")
        if policy not in {"preserve", "terminal_reclaim"}:
            return False
        if scope == "workspace_container" and policy == "terminal_reclaim":
            # Use the deterministic resource identity, not today's PVC feature
            # flag: a successor may reconcile a generation created under a
            # different rollout/config value.
            pvc_name = _pvc_name_for(owner)
            if pvc_name:
                try:
                    pvc = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        return False
                    pvc = None
                if pvc is not None:
                    try:
                        observed_pvc_uid = self._require_stateless_pvc_identity(
                            pvc,
                            owner=owner,
                            pvc_name=pvc_name,
                            allow_any_storage_class=True,
                        )
                    except WorkspaceRuntimeAuthorityError:
                        return False
                    recorded_pvc = reservation.get("pvc_uid")
                    if recorded_pvc is not None and str(recorded_pvc) != (
                        observed_pvc_uid
                    ):
                        return False
                    if (
                        recorded_pvc is None
                        and not await self._record_cancelled_creation_resource(
                            owner,
                            reservation,
                            scope=scope,
                            resource_kind="pvc",
                            resource_uid=observed_pvc_uid,
                        )
                    ):
                        return False
                    if not await self._terminal_cancelled_creation_claim_is_current(
                        owner, reservation
                    ):
                        return False
                    pvc_outcome = await self._delete_pvc_outcome(
                        pvc_name,
                        expected_owner=owner,
                        expected_uid=observed_pvc_uid,
                    )
                    if not pvc_outcome.captured_absent:
                        return False

            try:
                service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    return False
                service = None
            if service is not None:
                try:
                    observed_service_uid = self._require_stateless_service_identity(
                        service, owner=owner
                    )
                except WorkspaceRuntimeAuthorityError:
                    return False
                recorded_service = reservation.get("service_uid")
                if recorded_service is not None and str(recorded_service) != (
                    observed_service_uid
                ):
                    return False
                if (
                    recorded_service is None
                    and not await self._record_cancelled_creation_resource(
                        owner,
                        reservation,
                        scope=scope,
                        resource_kind="service",
                        resource_uid=observed_service_uid,
                    )
                ):
                    return False
                if not await self._terminal_cancelled_creation_claim_is_current(
                    owner, reservation
                ):
                    return False
                service_outcome = await self._delete_service_outcome(
                    owner,
                    require_exact_owner=True,
                    expected_uid=observed_service_uid,
                )
                if not service_outcome.captured_absent:
                    return False

        settle_partial = getattr(
            type(self._db),
            "settle_cancelled_partial_workspace_creation_reservation",
            None,
        )
        return bool(
            callable(settle_partial)
            and await settle_partial(
                self._db,
                owner.id,
                owner_kind=("thread" if owner.kind == "session" else "job"),
                scope=scope,
                reservation_generation=int(reservation["reservation_generation"]),
                claimant=str(reservation["claimed_by"]),
                claim_token=int(reservation["claim_token"]),
            )
        )

    async def _capture_cancelled_runtime_resources(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        *,
        pod: Any,
        runtime_incarnation: str,
    ) -> bool:
        """Capture the immutable resource plan of a cancelled accepted Pod."""

        scope = str(reservation.get("scope") or "")
        pod_name = f"ide-{owner.id[:12]}" if scope == "ide" else owner.pod_name
        try:
            seed_name = self._require_stateless_pod_storage_binding(
                pod,
                owner=owner,
                expected_pvc_name=(
                    None if scope == "ide" else _UNSPECIFIED_RESOURCE_BINDING
                ),
                expected_seed_configmap=_UNSPECIFIED_RESOURCE_BINDING,
                expected_pod_name=pod_name,
            )
            if seed_name is not None:
                try:
                    seed = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=seed_name,
                        namespace=self._namespace,
                    )
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        raise
                else:
                    seed_uid = self._require_stateless_seed_configmap_identity(
                        seed,
                        owner=owner,
                        pod_name=pod_name,
                        creation_reservation_id=str(reservation["id"]),
                    )
                    self._require_seed_configmap_pod_owner_reference(
                        seed,
                        pod_name=pod_name,
                        runtime_incarnation=runtime_incarnation,
                    )
                    if not await self._record_cancelled_creation_resource(
                        owner,
                        reservation,
                        scope=scope,
                        resource_kind="seed",
                        resource_uid=seed_uid,
                    ):
                        return False
            if scope == "workspace_container":
                pvc_name = self._workspace_pvc_name_from_pod(pod, owner=owner)
                if pvc_name is not None:
                    try:
                        pvc = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_persistent_volume_claim,
                            name=pvc_name,
                            namespace=self._namespace,
                        )
                    except Exception as error:
                        if getattr(error, "status", None) != 404:
                            raise
                    else:
                        pvc_uid = self._require_stateless_pvc_identity(
                            pvc,
                            owner=owner,
                            pvc_name=pvc_name,
                            allow_any_storage_class=True,
                        )
                        if not await self._record_cancelled_creation_resource(
                            owner,
                            reservation,
                            scope=scope,
                            resource_kind="pvc",
                            resource_uid=pvc_uid,
                        ):
                            return False
                    try:
                        service = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_service,
                            name=owner.pod_name,
                            namespace=self._namespace,
                        )
                    except Exception as error:
                        if getattr(error, "status", None) != 404:
                            raise
                    else:
                        service_uid = self._require_stateless_service_identity(
                            service, owner=owner
                        )
                        if not await self._record_cancelled_creation_resource(
                            owner,
                            reservation,
                            scope=scope,
                            resource_kind="service",
                            resource_uid=service_uid,
                        ):
                            return False
        except Exception:
            return False
        return await self._cancelled_creation_claim_is_current(
            owner, reservation, scope=scope
        )

    async def delete_workspace_with_outcome(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str | None = None,
        wait_for_exact_absence: bool = False,
        exact_absence_timeout_seconds: float = 30.0,
        captured_teardown_uid: str | None = None,
        target_disposition: Literal["deleted", "suspended"] = "deleted",
        reclaim_shared_resources: bool = False,
        suspended_at: str | None = None,
        cleanup_intent: dict[str, Any] | None = None,
        _mutation_guard_held: bool = False,
    ) -> RuntimeDeletionOutcome:
        """Delete one exact workspace runtime and preserve stale successors."""
        if not self._k8s_available:
            return _RUNTIME_DELETION_REFUSED
        if not _mutation_guard_held:
            async with self._workspace_mutation_guard(
                owner, scope="workspace_container"
            ) as mutation_owned:
                if not mutation_owned:
                    return _RUNTIME_DELETION_REFUSED
                return await self.delete_workspace_with_outcome(
                    owner,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    wait_for_exact_absence=wait_for_exact_absence,
                    exact_absence_timeout_seconds=exact_absence_timeout_seconds,
                    captured_teardown_uid=captured_teardown_uid,
                    target_disposition=target_disposition,
                    reclaim_shared_resources=reclaim_shared_resources,
                    suspended_at=suspended_at,
                    cleanup_intent=cleanup_intent,
                    _mutation_guard_held=True,
                )
        if wait_for_exact_absence and expected_runtime_incarnation is None:
            return _RUNTIME_DELETION_REFUSED
        if captured_teardown_uid is not None and (
            expected_runtime_incarnation != captured_teardown_uid
        ):
            return _RUNTIME_DELETION_REFUSED
        if (
            cleanup_intent is None
            and expected_runtime_incarnation is not None
            and self._db is not None
        ):
            read_intent = getattr(
                type(self._db),
                "get_managed_repository_workspace_cleanup_intent",
                None,
            )
            if callable(read_intent):
                cleanup_intent = await read_intent(
                    self._db,
                    owner.id,
                    owner_kind=("thread" if owner.kind == "session" else "job"),
                    scope="workspace_container",
                    runtime_incarnation=str(expected_runtime_incarnation),
                )

        pod_name = owner.pod_name
        already_absent = False
        retained_for_process_zero = False
        observed_runtime_uid: str | None = None
        try:
            observed_pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            observed_runtime_uid = self._require_workspace_pod_owner(
                observed_pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
            retained_for_process_zero = self._has_stateless_process_zero_finalizer(
                observed_pod
            )
            if expected_runtime_incarnation is not None and observed_runtime_uid != str(
                expected_runtime_incarnation
            ):
                if (
                    isinstance(cleanup_intent, dict)
                    and str(cleanup_intent.get("runtime_incarnation") or "")
                    == str(expected_runtime_incarnation)
                    and str(cleanup_intent.get("resource_policy") or "") == "preserve"
                    and cleanup_intent.get("resources_captured_at") is not None
                    and isinstance(cleanup_intent.get("claimed_by"), str)
                    and cleanup_intent.get("claimed_by")
                    and await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                    and await self._managed_repository_process_zero_replay_authority(
                        owner,
                        scope="workspace_container",
                        runtime_incarnation=str(expected_runtime_incarnation),
                    )
                    == "stale"
                ):
                    return _STALE_RUNTIME_SETTLED
                return _RUNTIME_DELETION_REFUSED
            if cleanup_intent is None and self._db is not None:
                cleanup_intent = await self.prepare_workspace_cleanup_intent(
                    owner,
                    expected_runtime_incarnation=observed_runtime_uid,
                    target_disposition=target_disposition,
                    reclaim_shared_resources=reclaim_shared_resources,
                    suspended_at=suspended_at,
                    _mutation_guard_held=True,
                )
                if cleanup_intent is None:
                    cleanup_intent = await self.prepare_workspace_cleanup_intent(
                        owner,
                        expected_runtime_incarnation=observed_runtime_uid,
                        target_disposition="deleted",
                        reclaim_shared_resources=False,
                        allow_stale_predecessor=True,
                        _mutation_guard_held=True,
                    )
                if (
                    not isinstance(cleanup_intent, dict)
                    or cleanup_intent.get("resources_captured_at") is None
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
            if self._db is not None and (
                not isinstance(cleanup_intent, dict)
                or cleanup_intent.get("resources_captured_at") is None
                or not isinstance(cleanup_intent.get("claimed_by"), str)
                or not cleanup_intent.get("claimed_by")
                or not await self._cleanup_claim_is_current(
                    cleanup_intent,
                    claimant=str(cleanup_intent["claimed_by"]),
                )
            ):
                return _RUNTIME_DELETION_REFUSED
            if (
                not retained_for_process_zero
                and not await self._ensure_managed_repository_process_zero_before_delete(
                    owner,
                    observed_pod,
                    observed_runtime_uid,
                )
            ):
                return _RUNTIME_DELETION_REFUSED
        except Exception as error:
            if getattr(error, "status", None) == 404:
                if self._db is not None and (
                    not isinstance(cleanup_intent, dict)
                    or cleanup_intent.get("resources_captured_at") is None
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                process_zero_uid: str | None = None
                if owner.kind == "session":
                    read_terminal = getattr(
                        type(self._db),
                        "get_stateless_thread_workspace_process_zero",
                        None,
                    )
                    if callable(read_terminal):
                        process_zero_uid = await read_terminal(
                            self._db,
                            owner.id,
                            expected_runtime_incarnation=(
                                str(expected_runtime_incarnation)
                                if expected_runtime_incarnation is not None
                                else None
                            ),
                        )
                if process_zero_uid is not None:
                    observed_runtime_uid = process_zero_uid
                elif (
                    owner.kind == "session"
                    and expected_runtime_incarnation is not None
                    and isinstance(cleanup_intent, dict)
                    and callable(
                        reclaim_receipt := getattr(
                            type(self._db),
                            "terminal_workspace_cleanup_process_zero_is_current",
                            None,
                        )
                    )
                    and await reclaim_receipt(
                        self._db,
                        owner.id,
                        runtime_incarnation=str(expected_runtime_incarnation),
                        intent_id=str(cleanup_intent["id"]),
                        claimant=str(cleanup_intent["claimed_by"]),
                        claim_token=int(cleanup_intent["claim_token"]),
                    )
                ):
                    # Soft End cleared the live UID. Only the locked permanent
                    # reclaim generation may consume its settled predecessor's
                    # exact receipt; ordinary current/stale callers gain nothing.
                    observed_runtime_uid = str(expected_runtime_incarnation)
                elif expected_runtime_incarnation is not None and self._db:
                    replay_authority = (
                        await self._managed_repository_process_zero_replay_authority(
                            owner,
                            scope="workspace_container",
                            runtime_incarnation=str(expected_runtime_incarnation),
                        )
                    )
                    if replay_authority == "stale":
                        # The exact predecessor is already absent and the owner
                        # now names a different protected runtime. Settle the
                        # lost response without clearing successor context or
                        # deleting its deterministic seed ConfigMap.
                        return _STALE_RUNTIME_SETTLED
                    if replay_authority == "current":
                        observed_runtime_uid = str(expected_runtime_incarnation)
                    else:
                        return _RUNTIME_DELETION_REFUSED
                else:
                    return _RUNTIME_DELETION_REFUSED
                already_absent = True
            else:
                logger.error(
                    "Refusing workspace Pod cleanup for %s %s: %s",
                    owner.kind,
                    owner.id,
                    error,
                )
                return _RUNTIME_DELETION_REFUSED
        try:
            if not already_absent:
                if (
                    not isinstance(cleanup_intent, dict)
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                delete_body: dict[str, Any] = {
                    "preconditions": {"uid": observed_runtime_uid}
                }
                if captured_teardown_uid is not None:
                    # Some apiservers do not honor the query parameter alone.
                    # Carry the same bound in DeleteOptions for durable S36 so
                    # it cannot inherit the Pod's ordinary 120-second grace
                    # and outlive the pinned agent's 60-second report timeout.
                    # Legacy/default-off callers retain their exact body.
                    delete_body["gracePeriodSeconds"] = 10
                await self._bounded_kubernetes_mutation(
                    self._core_api.delete_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    grace_period_seconds=10,
                    body=delete_body,
                )
                logger.info(
                    "Workspace container deletion accepted: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                already_absent = True
                logger.debug(
                    "Workspace container already deleted: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
            elif (
                hasattr(e, "status")
                and e.status == 409
                and captured_teardown_uid is not None
            ):
                # The apiserver applied the UID precondition atomically. A
                # conflict means the captured Pod is gone and a same-name
                # replacement now owns the name; never mutate that replacement
                # or its durable endpoint context.
                logger.debug(
                    "Captured workspace Pod already gone; preserving replacement: "
                    "%s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                # The captured Pod is gone, but this grouped teardown must not
                # interpret that as authority over the captured PVC/Service:
                # the replacement may already be using those same objects.
                # Returning False makes S36 preserve the entire resource set.
                return _RUNTIME_DELETION_REFUSED
            else:
                logger.error(
                    "Failed to delete workspace container for %s %s: %s",
                    owner.kind,
                    owner.id,
                    e,
                )
                return _RUNTIME_DELETION_REFUSED

        if retained_for_process_zero and not already_absent:
            assert observed_runtime_uid is not None
            if not await self._wait_for_exact_workspace_pod_terminal(
                owner,
                expected_runtime_incarnation=observed_runtime_uid,
                timeout=exact_absence_timeout_seconds,
            ):
                return _RUNTIME_DELETION_REFUSED
            if (
                not isinstance(cleanup_intent, dict)
                or not isinstance(cleanup_intent.get("claimed_by"), str)
                or not cleanup_intent.get("claimed_by")
                or not await self._cleanup_claim_is_current(
                    cleanup_intent,
                    claimant=str(cleanup_intent["claimed_by"]),
                )
            ):
                return _RUNTIME_DELETION_REFUSED
            released_finalizer = (
                await self.release_stateless_workspace_process_zero_finalizer(
                    owner,
                    expected_runtime_incarnation=observed_runtime_uid,
                    _mutation_guard_held=True,
                )
            )
            if not released_finalizer:
                # A committed JSON Patch may lose its response. Re-probe only
                # the exact UID and its durable receipt before deciding.
                if (
                    await self.workspace_pod_authority(
                        owner,
                        expected_runtime_incarnation=observed_runtime_uid,
                    )
                    != "exact_absent"
                ):
                    return _RUNTIME_DELETION_REFUSED
                replay_authority = (
                    await self._managed_repository_process_zero_replay_authority(
                        owner,
                        scope="workspace_container",
                        runtime_incarnation=observed_runtime_uid,
                    )
                )
                if replay_authority == "stale":
                    return _STALE_RUNTIME_SETTLED
                if replay_authority != "current":
                    return _RUNTIME_DELETION_REFUSED
                already_absent = True
            # A finalizer is a deliberate process-zero retention boundary, so
            # even historical non-strict callers must not publish deletion
            # before its exact object has actually disappeared.
            wait_for_exact_absence = True

        if wait_for_exact_absence and not already_absent:
            absence_runtime_uid = (
                str(expected_runtime_incarnation)
                if expected_runtime_incarnation is not None
                else str(observed_runtime_uid or "")
            )
            if not absence_runtime_uid:
                return _RUNTIME_DELETION_REFUSED
            if not await self._wait_for_exact_workspace_pod_absent(
                owner,
                expected_runtime_incarnation=absence_runtime_uid,
                timeout=exact_absence_timeout_seconds,
            ):
                # DELETE acceptance is not runtime absence. Keep the immutable
                # UID and endpoint durable so terminal retirement cannot settle
                # or Resume consume a new one-shot create against a terminating
                # same-name object.
                return _RUNTIME_DELETION_REFUSED

        if observed_runtime_uid is not None:
            replay_authority = (
                await self._managed_repository_process_zero_replay_authority(
                    owner,
                    scope="workspace_container",
                    runtime_incarnation=observed_runtime_uid,
                )
            )
            if replay_authority == "stale":
                return _STALE_RUNTIME_SETTLED

        if (
            not isinstance(cleanup_intent, dict)
            or not isinstance(cleanup_intent.get("claimed_by"), str)
            or not cleanup_intent.get("claimed_by")
            or not await self._cleanup_claim_is_current(
                cleanup_intent,
                claimant=str(cleanup_intent["claimed_by"]),
            )
        ):
            return _RUNTIME_DELETION_REFUSED
        seed_deleted = await self._delete_seed_configmap(
            pod_name,
            expected_owner=owner,
            expected_pod_uid=observed_runtime_uid,
            expected_configmap_uid=(
                cleanup_intent.get("seed_configmap_uid")
                if isinstance(cleanup_intent, dict)
                else _UNSPECIFIED_RESOURCE_BINDING
            ),
        )
        if not seed_deleted:
            return _RUNTIME_DELETION_REFUSED
        # This operation owns only the exact Pod and its Pod-owned seed. Durable
        # context and shared PVC/Service settlement belong to the caller's
        # lifecycle-authorized release step. Keeping ``retiring_process_zero``
        # here lets migration 0197 fence every replacement bind until that step
        # has succeeded.
        return _CURRENT_RUNTIME_DELETED

    async def delete_workspace(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str | None = None,
        wait_for_exact_absence: bool = False,
        exact_absence_timeout_seconds: float = 30.0,
        captured_teardown_uid: str | None = None,
        defer_context_clear: bool = False,
    ) -> bool:
        """Legacy wrapper; exact deletion now requires an explicit settlement.

        Pod/seed cleanup deliberately leaves the owner in
        ``retiring_process_zero``. A boolean caller cannot express the required
        shared-resource cleanup plus exact context CAS, so it must fail closed.
        """

        if owner.kind == "session" and self._db is not None:
            lane_probe = getattr(
                type(self._db),
                "stateless_thread_workspace_creation_requires_authority",
                None,
            )
            if callable(lane_probe):
                stateless = await lane_probe(self._db, owner.id)
                if stateless is False:
                    return await self._delete_pinned_workspace_legacy(
                        owner,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        wait_for_exact_absence=wait_for_exact_absence,
                        exact_absence_timeout_seconds=exact_absence_timeout_seconds,
                        captured_teardown_uid=captured_teardown_uid,
                        defer_context_clear=defer_context_clear,
                    )
                if stateless is None:
                    return False
        del (
            expected_runtime_incarnation,
            wait_for_exact_absence,
            exact_absence_timeout_seconds,
            captured_teardown_uid,
            defer_context_clear,
        )
        logger.warning(
            "Refusing legacy boolean workspace deletion for %s %s; callers "
            "must use the typed deletion and exact settlement protocol",
            owner.kind,
            owner.id,
        )
        return False

    async def _delete_pinned_workspace_legacy(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str | None = None,
        wait_for_exact_absence: bool = False,
        exact_absence_timeout_seconds: float = 30.0,
        captured_teardown_uid: str | None = None,
        defer_context_clear: bool = False,
    ) -> bool:
        """Delete the workspace container for a job or persistent thread.

        Returns:
            True if deleted, False otherwise.
        """
        if not self._k8s_available:
            return False
        if wait_for_exact_absence and expected_runtime_incarnation is None:
            return False
        if captured_teardown_uid is not None and (
            expected_runtime_incarnation != captured_teardown_uid
        ):
            return False

        pod_name = owner.pod_name
        already_absent = False
        retained_for_process_zero = False
        observed_runtime_uid: str | None = None
        try:
            observed_pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            observed_runtime_uid = self._require_workspace_pod_owner(
                observed_pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
            retained_for_process_zero = self._has_stateless_process_zero_finalizer(
                observed_pod
            )
            if expected_runtime_incarnation is not None and observed_runtime_uid != str(
                expected_runtime_incarnation
            ):
                return False
            if (
                not retained_for_process_zero
                and not await self._ensure_managed_repository_process_zero_before_delete(
                    owner,
                    observed_pod,
                    observed_runtime_uid,
                )
            ):
                return False
        except Exception as error:
            if getattr(error, "status", None) == 404:
                process_zero_uid: str | None = None
                if owner.kind == "session":
                    read_terminal = getattr(
                        type(self._db),
                        "get_stateless_thread_workspace_process_zero",
                        None,
                    )
                    if callable(read_terminal):
                        process_zero_uid = await read_terminal(
                            self._db,
                            owner.id,
                            expected_runtime_incarnation=(
                                str(expected_runtime_incarnation)
                                if expected_runtime_incarnation is not None
                                else None
                            ),
                        )
                if process_zero_uid is not None:
                    observed_runtime_uid = process_zero_uid
                elif expected_runtime_incarnation is not None and self._db:
                    generic_zero = await self._db.managed_repository_workspace_process_zero_is_current(
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope="workspace_container",
                        provisioner="k8s",
                        runtime_incarnation=str(expected_runtime_incarnation),
                    )
                    if generic_zero:
                        observed_runtime_uid = str(expected_runtime_incarnation)
                    else:
                        return False
                else:
                    return False
                already_absent = True
            else:
                logger.error(
                    "Refusing workspace Pod cleanup for %s %s: %s",
                    owner.kind,
                    owner.id,
                    error,
                )
                return False
        try:
            if not already_absent:
                delete_body: dict[str, Any] = {
                    "preconditions": {"uid": observed_runtime_uid}
                }
                if captured_teardown_uid is not None:
                    # Some apiservers do not honor the query parameter alone.
                    # Carry the same bound in DeleteOptions for durable S36 so
                    # it cannot inherit the Pod's ordinary 120-second grace
                    # and outlive the pinned agent's 60-second report timeout.
                    # Legacy/default-off callers retain their exact body.
                    delete_body["gracePeriodSeconds"] = 10
                await self._bounded_kubernetes_call(
                    self._core_api.delete_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    grace_period_seconds=10,
                    body=delete_body,
                )
                logger.info(
                    "Workspace container deletion accepted: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                already_absent = True
                logger.debug(
                    "Workspace container already deleted: %s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
            elif (
                hasattr(e, "status")
                and e.status == 409
                and captured_teardown_uid is not None
            ):
                # The apiserver applied the UID precondition atomically. A
                # conflict means the captured Pod is gone and a same-name
                # replacement now owns the name; never mutate that replacement
                # or its durable endpoint context.
                logger.debug(
                    "Captured workspace Pod already gone; preserving replacement: "
                    "%s (%s %s)",
                    pod_name,
                    owner.kind,
                    owner.id,
                )
                # The captured Pod is gone, but this grouped teardown must not
                # interpret that as authority over the captured PVC/Service:
                # the replacement may already be using those same objects.
                # Returning False makes S36 preserve the entire resource set.
                return False
            else:
                logger.error(
                    "Failed to delete workspace container for %s %s: %s",
                    owner.kind,
                    owner.id,
                    e,
                )
                return False

        if retained_for_process_zero and not already_absent:
            assert observed_runtime_uid is not None
            if not await self._wait_for_exact_workspace_pod_terminal(
                owner,
                expected_runtime_incarnation=observed_runtime_uid,
                timeout=exact_absence_timeout_seconds,
            ):
                return False
            if not await self.release_stateless_workspace_process_zero_finalizer(
                owner,
                expected_runtime_incarnation=observed_runtime_uid,
            ):
                return False
            # A finalizer is a deliberate process-zero retention boundary, so
            # even historical non-strict callers must not publish deletion
            # before its exact object has actually disappeared.
            wait_for_exact_absence = True

        if wait_for_exact_absence and not already_absent:
            absence_runtime_uid = (
                str(expected_runtime_incarnation)
                if expected_runtime_incarnation is not None
                else str(observed_runtime_uid or "")
            )
            if not absence_runtime_uid:
                return False
            if not await self._wait_for_exact_workspace_pod_absent(
                owner,
                expected_runtime_incarnation=absence_runtime_uid,
                timeout=exact_absence_timeout_seconds,
            ):
                # DELETE acceptance is not runtime absence. Keep the immutable
                # UID and endpoint durable so terminal retirement cannot settle
                # or Resume consume a new one-shot create against a terminating
                # same-name object.
                return False

        seed_deleted = await self._delete_seed_configmap(
            pod_name,
            expected_owner=owner,
            expected_pod_uid=observed_runtime_uid,
        )
        if wait_for_exact_absence and not seed_deleted:
            return False
        # A 404 (or the exact wait above) is now authoritative. Clear any stale
        # ready endpoint only after the object is gone; otherwise immediate
        # Resume can race the terminating old name and consume its one attempt.
        if not defer_context_clear:
            await self._set_context(
                owner,
                {
                    "status": "deleted",
                    "pod_ip": None,
                    **(
                        {WORKSPACE_RUNTIME_INCARNATION_KEY: None}
                        if owner.kind == "session"
                        else {}
                    ),
                },
            )
        await workspace_metering.close_interval(self._db, owner)
        return True

    async def _ensure_managed_repository_process_zero_before_delete(
        self,
        owner: WorkspaceOwner,
        pod: Any,
        runtime_incarnation: str,
    ) -> bool:
        """Enforce credential-process containment at the lowest Pod boundary."""

        if not self._db:
            return False
        owner_kind = "thread" if owner.kind == "session" else "job"
        if await self._db.managed_repository_workspace_process_zero_is_current(
            owner.id,
            owner_kind=owner_kind,
            scope="workspace_container",
            provisioner="k8s",
            runtime_incarnation=runtime_incarnation,
        ):
            return True

        process_zero = _pod_has_exact_process_zero(pod)
        if not process_zero:
            if not await self._db.claim_managed_repository_workspace_retirement(
                owner.id,
                owner_kind=owner_kind,
                scope="workspace_container",
                provisioner="k8s",
                runtime_incarnation=runtime_incarnation,
            ):
                return False
            try:
                attestation = await self.attest_workspace_runtime(owner)
            except Exception:
                logger.warning(
                    "Workspace delete refused without an exact repository-agent "
                    "retirement endpoint for %s %s",
                    owner.kind,
                    owner.id,
                )
                return False
            if attestation.runtime_incarnation != runtime_incarnation:
                return False
            process_zero = await self._retire_managed_repository_agents(
                WorkspaceTeardownIdentity(
                    pod_uid=runtime_incarnation,
                    pvc_uid=None,
                    service_uid=None,
                    pod_ip=attestation.pod_ip,
                    ssh_host_key_fingerprint=(attestation.ssh_host_key_fingerprint),
                    ssh_port=attestation.port,
                )
            )
            if process_zero:
                process_zero = await self.workspace_pod_authority(
                    owner,
                    expected_runtime_incarnation=runtime_incarnation,
                ) in {"exact_live", "exact_terminal"}
        if not process_zero:
            return False
        return bool(
            await self._db.record_managed_repository_workspace_process_zero(
                owner.id,
                owner_kind=owner_kind,
                scope="workspace_container",
                provisioner="k8s",
                runtime_incarnation=runtime_incarnation,
            )
        )

    async def delete_workspace_pvc(
        self,
        owner: WorkspaceOwner,
        *,
        require_exact_owner: bool = False,
        expected_uid: str | None = None,
    ) -> bool:
        """Reclaim the PVC backing a job or session workspace, if one exists.

        Both names are LIVE under ``WORKSPACE_PVC_ENABLED`` — jobs are
        ``pvc-workspace-*`` and sessions ``pvc-ws-thread-*`` — so this is the
        reclaim half of provisioning, not legacy cleanup. The name comes from
        ``_pvc_name_for``, the same helper ``create_workspace`` uses, so the
        create and delete sides cannot drift onto different volumes.

        Destructive and irreversible: call it only when the owning work is
        genuinely finished. An emptyDir workspace has no PVC, and a PVC already
        gone is a 404 — both are idempotent successes.
        """
        delete_kwargs: dict[str, Any] = {
            "expected_owner": owner if require_exact_owner else None,
        }
        if expected_uid is not None:
            delete_kwargs["expected_uid"] = expected_uid
        return await self._delete_pvc(_pvc_name_for(owner), **delete_kwargs)

    async def capture_workspace_teardown_identity(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str | None = None,
    ) -> WorkspaceTeardownIdentity:
        """Capture S36's exact Pod/PVC/Service identities without SSH.

        A Pod 404 does not imply that its PVC and Service are gone. In that
        case ``pod_uid`` is ``None`` and the residual deterministic resources
        are captured independently under their full owner/spec authority. The
        Pod name is re-read after those captures so a concurrent replacement
        cannot donate its PVC or Service identity to this teardown intent.
        Every ambiguous API failure raises rather than granting cleanup.
        """

        if not self._k8s_available:
            raise WorkspaceRuntimeAuthorityError(
                "Kubernetes workspace identity capture is unavailable"
            )
        expected_runtime = (
            _canonical_runtime_uuid(
                expected_runtime_incarnation,
                label="workspace cleanup runtime",
            )
            if expected_runtime_incarnation is not None
            else None
        )
        pod_absent = False
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                pod_absent = True
                pod_uid = None
                pvc_name = _pvc_name_for(owner)
            else:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod identity capture failed"
                ) from exc
        else:
            pod_uid = self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
            if expected_runtime is not None and pod_uid != expected_runtime:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod changed before cleanup intent capture"
                )
            pvc_name = self._workspace_pvc_name_from_pod(pod, owner=owner)

        seed_configmap_uid: str | None = None
        try:
            configmap = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_config_map,
                name=self._seed_configmap_name(owner.pod_name),
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) != 404:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace seed ConfigMap identity capture failed"
                ) from exc
        else:
            seed_configmap_uid = self._require_stateless_seed_configmap_identity(
                configmap,
                owner=owner,
                pod_name=owner.pod_name,
            )
            seed_owner_runtime = pod_uid or expected_runtime
            if seed_owner_runtime is None:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace seed ConfigMap lacks captured Pod authority"
                )
            self._require_seed_configmap_pod_owner_reference(
                configmap,
                pod_name=owner.pod_name,
                runtime_incarnation=seed_owner_runtime,
            )

        pvc_uid: str | None = None
        if pvc_name is not None:
            try:
                claim = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
            except Exception as exc:
                if not (pod_absent and getattr(exc, "status", None) == 404):
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace PVC identity capture failed"
                    ) from exc
            else:
                pvc_uid = self._require_stateless_pvc_identity(
                    claim,
                    owner=owner,
                    pvc_name=pvc_name,
                    allow_any_storage_class=True,
                )

        service_uid: str | None
        try:
            service = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_service,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                service_uid = None
            else:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Service identity capture failed"
                ) from exc
        else:
            service_uid = self._require_stateless_service_identity(service, owner=owner)

        if pod_absent:
            # The first 404 and this final 404 bracket the residual captures.
            # Any same-name Pod observed here may already consume those
            # resources, so its appearance makes the whole capture ambiguous.
            try:
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
            except Exception as exc:
                if getattr(exc, "status", None) != 404:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Pod absence recheck failed"
                    ) from exc
            else:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod appeared during teardown identity capture"
                )
        else:
            # Bracket the PVC/Service reads with the exact Pod UID. Without
            # this second read, a same-name replacement could donate its
            # freshly-created named resources to the old teardown intent.
            try:
                confirmed_pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=owner.pod_name,
                    namespace=self._namespace,
                )
                confirmed_uid = self._require_workspace_pod_owner(
                    confirmed_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    allow_terminating=True,
                )
            except Exception as exc:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod identity recheck failed"
                ) from exc
            if confirmed_uid != pod_uid:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace Pod changed during teardown identity capture"
                )

        return WorkspaceTeardownIdentity(
            pod_uid=pod_uid or expected_runtime,
            pvc_uid=pvc_uid,
            service_uid=service_uid,
            seed_configmap_uid=seed_configmap_uid,
        )

    async def capture_terminal_workspace_identity(
        self,
        owner: WorkspaceOwner,
    ) -> WorkspaceTeardownIdentity:
        """Capture S36's UID tuple plus the exact SSH snapshot authority."""

        identity = await self.capture_workspace_teardown_identity(owner)
        if identity.pod_uid is None:
            return identity
        authority = await self.workspace_pod_authority(
            owner,
            expected_runtime_incarnation=identity.pod_uid,
        )
        if authority == "exact_terminal":
            return identity
        if authority != "exact_live":
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod is not available for terminal authority capture"
            )
        attestation = await self.attest_workspace_runtime(owner)
        if identity.pod_uid != attestation.runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError(
                "workspace Pod changed between SSH and teardown attestation"
            )
        return WorkspaceTeardownIdentity(
            pod_uid=identity.pod_uid,
            pvc_uid=identity.pvc_uid,
            service_uid=identity.service_uid,
            pod_ip=attestation.pod_ip,
            ssh_host_key_fingerprint=attestation.ssh_host_key_fingerprint,
            ssh_port=attestation.port,
        )

    async def classify_workspace_teardown_identity(
        self,
        owner: WorkspaceOwner,
        identity: WorkspaceTeardownIdentity,
    ) -> str:
        """Classify a failed captured release without adopting stable names.

        ``identity_superseded`` is reserved for a proven Pod/PVC/Service UID
        replacement.  API ambiguity remains ``unknown`` so the S36 journal
        retries; exact captured or absent resources remain ``matched``.
        """

        if not self._k8s_available:
            return "unknown"
        if identity.pod_uid is None:
            pod_authority = (
                "exact_absent"
                if await self._captured_teardown_pod_is_absent(owner)
                else "unknown"
            )
        else:
            pod_authority = await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=identity.pod_uid,
            )
        if pod_authority == "replacement":
            return "identity_superseded"
        if pod_authority == "unknown":
            return "unknown"

        async def _resource_matches(
            *,
            expected_uid: str | None,
            read: Callable[[], Awaitable[Any]],
            authenticate: Callable[[Any], str],
        ) -> str:
            try:
                resource = await read()
            except Exception as exc:
                return "matched" if getattr(exc, "status", None) == 404 else "unknown"
            try:
                observed_uid = authenticate(resource)
            except WorkspaceRuntimeAuthorityError:
                return "identity_superseded"
            if expected_uid is None or observed_uid != expected_uid:
                return "identity_superseded"
            return "matched"

        async def _read_pvc() -> Any:
            return await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_persistent_volume_claim,
                name=_pvc_name_for(owner),
                namespace=self._namespace,
            )

        async def _read_service() -> Any:
            return await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_service,
                name=owner.pod_name,
                namespace=self._namespace,
            )

        pvc = await _resource_matches(
            expected_uid=identity.pvc_uid,
            read=_read_pvc,
            authenticate=lambda resource: self._require_stateless_pvc_identity(
                resource,
                owner=owner,
                pvc_name=_pvc_name_for(owner),
                allow_any_storage_class=True,
            ),
        )
        if pvc != "matched":
            return pvc
        return await _resource_matches(
            expected_uid=identity.service_uid,
            read=_read_service,
            authenticate=lambda resource: self._require_stateless_service_identity(
                resource,
                owner=owner,
            ),
        )

    async def release_workspace(
        self,
        owner: "WorkspaceOwner",
        *,
        reclaim_volume: bool = True,
        require_snapshot: bool = False,
        expected_runtime_incarnation: str | None = None,
        expected_host_key_fingerprint: str | None = None,
        on_snapshot_captured: Callable[[], Awaitable[bool]] | None = None,
        capture_snapshot: bool = True,
        strict_terminal_snapshot: bool = False,
        terminal_snapshot_generation: str | None = None,
        terminal_snapshot_created_at: str | None = None,
        strict: bool = False,
        teardown_identity: WorkspaceTeardownIdentity | None = None,
        exact_absence_timeout_seconds: float = 30.0,
        pinned_retirement: Mapping[str, Any] | None = None,
    ) -> bool:
        """Snapshot a workspace to S3, then delete the pod (and, by default, its PVC).

        Owner-keyed: serves both jobs and sessions. On an emptyDir workspace the
        data dies with the pod, so snapshotting first is what enables resume (a
        fresh pod restores from S3). Snapshot failure is non-fatal.

        Args:
            reclaim_volume: When False, keep the PVC — snapshot and delete the
                pod (and its Service), but leave the volume so a later recreate
                reattaches the real working tree instead of an S3 approximation.
                Required for a PVC-backed session: an ``ended`` thread is still
                RESUMABLE, so unconditionally deleting its volume is data
                destruction on live state a user can legitimately reopen. Pass
                True (the default) only when the owning work is truly terminal.
            exact_absence_timeout_seconds: Maximum time to prove the exact Pod
                UID absent after Kubernetes accepts a strict delete. Existing
                callers retain the historical 30-second bound; captured S36
                supplies 45 seconds around its explicit 10-second grace.
            pinned_retirement: Exact Begin context and G/T for the pinned End
                flow, which holds the thread advisory lock through settlement.
                Uses pinned process-zero cleanup and best-effort snapshots.

        The headless Service is dropped either way: it is 409-idempotent to
        recreate on the next ``create_workspace``, so unlike the volume it costs
        nothing to lose.

        Returns:
            True if pod deletion succeeded.
        """
        if not self._k8s_available:
            return False

        if pinned_retirement is not None:
            if teardown_identity is None or not strict or require_snapshot:
                return False
            return await self._release_pinned_retirement_workspace(
                owner,
                teardown_identity,
                retirement=pinned_retirement,
                reclaim_volume=reclaim_volume,
                capture_snapshot=capture_snapshot,
                exact_absence_timeout_seconds=exact_absence_timeout_seconds,
            )

        if teardown_identity is not None:
            return await self._release_captured_workspace(
                owner,
                teardown_identity,
                reclaim_volume=reclaim_volume,
                require_snapshot=require_snapshot,
                expected_runtime_incarnation=expected_runtime_incarnation,
                expected_host_key_fingerprint=expected_host_key_fingerprint,
                on_snapshot_captured=on_snapshot_captured,
                capture_snapshot=capture_snapshot,
                strict_terminal_snapshot=strict_terminal_snapshot,
                terminal_snapshot_generation=terminal_snapshot_generation,
                terminal_snapshot_created_at=terminal_snapshot_created_at,
                strict=strict,
                exact_absence_timeout_seconds=exact_absence_timeout_seconds,
            )

        if expected_runtime_incarnation is not None:
            # Shell retirement and archive are two separate network effects.
            # Re-prove the exact Pod UID at their handoff so a replacement at
            # the stable Service name is never captured or deleted as the old
            # runtime.
            exact_live = await self.workspace_pod_live(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
            if exact_live is not True:
                logger.warning(
                    "Workspace runtime changed before strict release for %s %s",
                    owner.kind,
                    owner.id,
                )
                return False

        status = await self.get_workspace_status(owner)
        if expected_runtime_incarnation is not None and (
            status is None
            or str(status.get("runtime_incarnation") or "")
            != str(expected_runtime_incarnation)
        ):
            return False
        effective_runtime_incarnation = (
            str(expected_runtime_incarnation)
            if expected_runtime_incarnation is not None
            else str((status or {}).get("runtime_incarnation") or "")
        )
        if not effective_runtime_incarnation:
            return False
        if (
            await self.workspace_pod_live(
                owner,
                expected_runtime_incarnation=effective_runtime_incarnation,
            )
            is not True
        ):
            return False
        pod_ip = status.get("pod_ip") if status else None
        ready = status.get("ready") if status else False

        if (
            capture_snapshot
            and self._snapshot_service
            and self._snapshot_service.is_available
            and pod_ip
            and ready
        ):
            try:
                capture_kwargs: dict[str, Any] = {
                    "job_id": owner.id,
                    "ssh_host": pod_ip,
                    "ssh_port": 30022,
                    "source_type": "pod",
                    "entity_type": ("threads" if owner.kind == "session" else "jobs"),
                    "expected_host_key_fingerprint": expected_host_key_fingerprint,
                }
                if strict_terminal_snapshot:
                    capture_kwargs["strict_terminal"] = True
                captured = bool(
                    await self._snapshot_service.capture_vm_snapshot(**capture_kwargs)
                )
                if captured:
                    if (
                        on_snapshot_captured is not None
                        and not await on_snapshot_captured()
                    ):
                        logger.error(
                            "Snapshot acknowledgement lost lifecycle authority "
                            "for %s %s",
                            owner.kind,
                            owner.id,
                        )
                        return False
                    logger.info(
                        "Workspace snapshot captured for %s %s before release",
                        owner.kind,
                        owner.id,
                    )
                elif require_snapshot:
                    logger.error(
                        "Required workspace snapshot was not captured for %s %s",
                        owner.kind,
                        owner.id,
                    )
                    return False
            except Exception:
                if require_snapshot:
                    logger.exception(
                        "Required workspace snapshot failed for %s %s",
                        owner.kind,
                        owner.id,
                    )
                    return False
                logger.exception(
                    "Workspace snapshot failed for %s %s — deleting anyway",
                    owner.kind,
                    owner.id,
                )
        elif require_snapshot:
            logger.error(
                "Required workspace snapshot is unavailable for %s %s",
                owner.kind,
                owner.id,
            )
            return False

        if (
            await self.workspace_pod_live(
                owner,
                expected_runtime_incarnation=effective_runtime_incarnation,
            )
            is not True
        ):
            return False
        delete_kwargs = {
            "expected_runtime_incarnation": effective_runtime_incarnation,
            **(
                {
                    "wait_for_exact_absence": True,
                    "exact_absence_timeout_seconds": exact_absence_timeout_seconds,
                }
                if strict
                else {}
            ),
        }
        deletion = await self.delete_workspace_with_outcome(owner, **delete_kwargs)
        if not deletion.current_deleted:
            return False
        return await self.release_absent_workspace(
            owner,
            reclaim_volume=reclaim_volume,
            expected_runtime_incarnation=effective_runtime_incarnation,
            strict=strict,
        )

    async def _release_pinned_retirement_workspace(
        self,
        owner: WorkspaceOwner,
        identity: WorkspaceTeardownIdentity,
        *,
        retirement: Mapping[str, Any],
        reclaim_volume: bool,
        capture_snapshot: bool,
        exact_absence_timeout_seconds: float,
    ) -> bool:
        """Release a pinned sandbox under its existing immutable G/T authority.

        The End flow holds the thread advisory lock, validates the captured
        backing, and settles only after this grouped release. Keep the pinned
        Pod finalizer/process-zero actuator; S36 intents serve non-pinned work.
        """

        if owner.kind != "session" or self._db is None:
            return False

        async def current() -> dict[str, Any] | None:
            authority = await self._db.get_pinned_workspace_cleanup_authority(
                owner.id,
                runtime_generation=str(retirement.get("generation") or ""),
                retirement_token=str(retirement.get("token") or ""),
            )
            if (
                authority is None
                or authority["context"] != retirement.get("context")
                or authority["permanent"] != reclaim_volume
                or bool(retirement.get("permanent")) != reclaim_volume
            ):
                return None
            return authority

        authority = await current()
        if authority is None:
            return False
        context = authority["context"]
        workspace = context.get("workspace_container") or {}
        binding = context.get("workspace_binding") or {}
        runtime = workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
        retained = context.get("retained_soft_workspace") or {}
        if (
            workspace.get("provisioner") != "k8s"
            or workspace.get("namespace") != self._namespace
        ):
            return False
        if identity.pod_uid not in (None, runtime):
            return False
        if identity.pvc_uid is not None and binding.get("backing_id") != (
            f"k8s-pvc:{self._namespace}:{identity.pvc_uid}"
        ):
            return False
        if runtime is None:
            # A same-generation soft outcome is the only authority for a
            # Pod-less retained PVC. Never infer process zero from Pod 404.
            if (
                not reclaim_volume
                or not retained
                or context.get("entry_status") != "ended"
                or retained.get("pvc_uid")
                != binding.get("backing_id", "").rsplit(":", 1)[-1]
                or identity.service_uid is not None
                or not await self._captured_teardown_pod_is_absent(owner)
            ):
                return False
        else:
            pod_authority = await self.workspace_pod_authority(
                owner, expected_runtime_incarnation=runtime
            )
            if pod_authority in {"unknown", "replacement"}:
                return False
            if pod_authority == "exact_absent":
                if not authority["process_zero"]:
                    return False
                if not await self._delete_seed_configmap(
                    owner.pod_name, expected_owner=owner, expected_pod_uid=runtime
                ):
                    return False
            else:
                if (
                    capture_snapshot
                    and self._snapshot_service
                    and self._snapshot_service.is_available
                    and pod_authority == "exact_live"
                    and binding.get("ssh_host_key_fingerprint")
                ):
                    status = await self.get_workspace_status(owner)
                    if not status or status.get("runtime_incarnation") != runtime:
                        return False
                    if status.get("ready") and status.get("pod_ip"):
                        try:
                            await self._snapshot_service.capture_vm_snapshot(
                                job_id=owner.id,
                                ssh_host=status["pod_ip"],
                                ssh_port=30022,
                                source_type="pod",
                                entity_type="threads",
                                expected_host_key_fingerprint=binding.get(
                                    "ssh_host_key_fingerprint"
                                ),
                            )
                        except Exception:
                            logger.exception(
                                "Pinned workspace snapshot failed for %s", owner.id
                            )
                if (
                    not await current()
                    or not await self._delete_pinned_workspace_legacy(
                        owner,
                        expected_runtime_incarnation=runtime,
                        captured_teardown_uid=runtime,
                        wait_for_exact_absence=True,
                        exact_absence_timeout_seconds=exact_absence_timeout_seconds,
                        defer_context_clear=True,
                    )
                ):
                    return False

        # Recheck the whole group after Pod deletion and before each shared
        # effect. Same-name successors and a changed G/T always retain it.
        if not await current() or not await self._captured_teardown_pod_is_absent(
            owner
        ):
            return False
        if reclaim_volume and identity.pvc_uid is not None:
            if not await self.delete_workspace_pvc(
                owner, require_exact_owner=True, expected_uid=identity.pvc_uid
            ):
                return False
        if not await current() or not await self._captured_teardown_pod_is_absent(
            owner
        ):
            return False
        if identity.service_uid is not None:
            if not await self._delete_service(
                owner, require_exact_owner=True, expected_uid=identity.service_uid
            ):
                return False
        if not await current():
            return False
        # Leave the original endpoint durable on every partial failure. This
        # last projection is retryable from the same G/T and saved zero receipt.
        cleared = await self._set_context(
            owner,
            {
                "status": "deleted",
                "pod_ip": None,
                WORKSPACE_RUNTIME_INCARNATION_KEY: None,
            },
        )
        await workspace_metering.close_interval(self._db, owner)
        return cleared

    async def _release_captured_workspace(
        self,
        owner: WorkspaceOwner,
        identity: WorkspaceTeardownIdentity,
        *,
        reclaim_volume: bool,
        require_snapshot: bool,
        expected_runtime_incarnation: str | None,
        expected_host_key_fingerprint: str | None,
        on_snapshot_captured: Callable[[], Awaitable[bool]] | None,
        capture_snapshot: bool,
        strict_terminal_snapshot: bool,
        terminal_snapshot_generation: str | None,
        terminal_snapshot_created_at: str | None,
        strict: bool,
        exact_absence_timeout_seconds: float,
    ) -> bool:
        """Release only the Kubernetes objects captured in an S36 intent."""

        if (
            expected_runtime_incarnation is not None
            and expected_runtime_incarnation != identity.pod_uid
        ):
            return False
        if expected_host_key_fingerprint is not None and (
            expected_host_key_fingerprint != identity.ssh_host_key_fingerprint
        ):
            return False

        snapshot_captured = False
        if require_snapshot:
            if (
                not strict_terminal_snapshot
                or identity.pod_uid is None
                or not identity.pod_ip
                or not identity.ssh_host_key_fingerprint
                or terminal_snapshot_generation is None
                or terminal_snapshot_created_at is None
                or not self._snapshot_service
                or not self._snapshot_service.is_available
            ):
                return False
            (
                snapshot_captured,
                _,
            ) = await self._snapshot_service.reconcile_terminal_snapshot_generation(
                owner.id,
                terminal_generation=terminal_snapshot_generation,
                entity_type=("threads" if owner.kind == "session" else "jobs"),
                expected_runtime_incarnation=identity.pod_uid,
                expected_host_key_fingerprint=(identity.ssh_host_key_fingerprint),
            )
        if identity.pod_uid is None:
            if (
                require_snapshot and not snapshot_captured
            ) or not await self._captured_teardown_pod_is_absent(owner):
                return False
            # A Pod 404 cannot prove that a partitioned node stopped the
            # workspace's repo-key ssh-agent, and this intent has no exact UID
            # with which to validate a prior receipt. Never turn absence into
            # credential-process authority.
            return False
        else:
            authority = await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=identity.pod_uid,
            )
            if authority == "unknown":
                return False
            if authority == "replacement":
                # A same-name successor may legitimately reuse the captured
                # PVC and Service.  Preserve the entire resource set; proving
                # only that the old Pod UID is gone is not teardown authority
                # over resources attached to its replacement.
                return False

            if authority == "exact_live":
                status = await self.get_workspace_status(owner)
                if (
                    status is None
                    or str(status.get("runtime_incarnation") or "") != identity.pod_uid
                ):
                    return False
                pod_ip = status.get("pod_ip")
                ready = status.get("ready")
                if identity.pod_ip is not None and pod_ip != identity.pod_ip:
                    return False
                if (
                    not snapshot_captured
                    and capture_snapshot
                    and self._snapshot_service
                    and self._snapshot_service.is_available
                    and pod_ip
                    and ready
                ):
                    capture_kwargs: dict[str, Any] = {
                        "job_id": owner.id,
                        "ssh_host": identity.pod_ip or pod_ip,
                        "ssh_port": identity.ssh_port,
                        "source_type": "pod",
                        "entity_type": (
                            "threads" if owner.kind == "session" else "jobs"
                        ),
                        "expected_host_key_fingerprint": (
                            identity.ssh_host_key_fingerprint
                        ),
                    }
                    if strict_terminal_snapshot:
                        capture_kwargs["strict_terminal"] = True
                    if terminal_snapshot_generation is not None:
                        capture_kwargs.update(
                            {
                                "terminal_generation": terminal_snapshot_generation,
                                "terminal_created_at": terminal_snapshot_created_at,
                                "expected_runtime_incarnation": identity.pod_uid,
                            }
                        )
                    try:
                        captured = bool(
                            await self._snapshot_service.capture_vm_snapshot(
                                **capture_kwargs
                            )
                        )
                    except Exception:
                        if require_snapshot:
                            logger.exception(
                                "Required captured workspace snapshot failed for %s %s",
                                owner.kind,
                                owner.id,
                            )
                            return False
                        logger.exception(
                            "Captured workspace snapshot failed for %s %s",
                            owner.kind,
                            owner.id,
                        )
                        captured = False
                    if captured and on_snapshot_captured is not None:
                        if not await on_snapshot_captured():
                            return False
                    if not captured and require_snapshot:
                        return False
                    snapshot_captured = snapshot_captured or captured
                elif require_snapshot and not snapshot_captured:
                    return False
            elif require_snapshot and not snapshot_captured:
                # A terminal/absent/replacement Pod can no longer produce the
                # required final snapshot. Never call SSH through the stable name.
                return False

        if require_snapshot and not snapshot_captured:
            return False

        # Persist the exact cleanup generation, disposition, and captured
        # resource tuple before retiring any credential process or mutating a
        # Kubernetes object. A crash after the SSH acknowledgement therefore
        # replays this same generation instead of leaving an unowned effect.
        intent = await self.prepare_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=identity.pod_uid,
            target_disposition="deleted",
            reclaim_shared_resources=reclaim_volume,
            identity=identity,
            admission_source="explicit",
        )
        if not isinstance(intent, dict):
            return False

        # Snapshot first, then contain the exact credential-bearing process
        # namespace before asking Kubernetes to delete it. API acceptance or
        # 404 is not process-zero under a partitioned node. The receipt commits
        # before DELETE so a lost response can be reconciled only against this
        # same Pod UID.
        process_zero = False
        replay_process_zero_authority: str | None = None
        if identity.pod_uid is not None:
            replay_process_zero_authority = (
                await self._managed_repository_process_zero_replay_authority(
                    owner,
                    scope="workspace_container",
                    runtime_incarnation=identity.pod_uid,
                )
            )
            process_zero = replay_process_zero_authority is not None
            authority = await self.workspace_pod_authority(
                owner,
                expected_runtime_incarnation=identity.pod_uid,
            )
            if not process_zero and authority == "exact_terminal":
                process_zero = True
            elif not process_zero and authority == "exact_live":
                if not identity.pod_ip or not identity.ssh_host_key_fingerprint:
                    return False
                process_zero = await self._retire_managed_repository_agents(identity)
                if process_zero:
                    authority = await self.workspace_pod_authority(
                        owner,
                        expected_runtime_incarnation=identity.pod_uid,
                    )
                    process_zero = authority in {"exact_live", "exact_terminal"}
            elif authority == "exact_absent":
                replay_process_zero_authority = (
                    await self._managed_repository_process_zero_replay_authority(
                        owner,
                        scope="workspace_container",
                        runtime_incarnation=identity.pod_uid,
                    )
                )
                process_zero = replay_process_zero_authority is not None
            if process_zero and authority != "exact_absent":
                process_zero = bool(
                    self._db
                    and await self._db.record_managed_repository_workspace_process_zero(
                        owner.id,
                        owner_kind=("thread" if owner.kind == "session" else "job"),
                        scope="workspace_container",
                        provisioner="k8s",
                        runtime_incarnation=identity.pod_uid,
                    )
                )
        if not process_zero:
            logger.warning(
                "Refusing Kubernetes teardown without exact managed-repository "
                "process-zero for %s %s",
                owner.kind,
                owner.id,
            )
            return False

        outcome = await self.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=identity.pod_uid,
            intent_generation=int(intent["intent_generation"]),
        )
        # Superseded means A is durably gone while B owns the stable names;
        # the typed reconciler deliberately performed no shared cleanup.
        return outcome.settled or outcome.superseded

    async def _retire_managed_repository_agents(
        self,
        identity: WorkspaceTeardownIdentity,
    ) -> bool:
        """Retire the exact credential-agent namespace on a captured Pod."""

        if not identity.pod_ip or not identity.ssh_host_key_fingerprint:
            return False
        return await retire_managed_repository_processes(
            host=identity.pod_ip,
            port=identity.ssh_port,
            host_key_fingerprint=identity.ssh_host_key_fingerprint,
            operation="Kubernetes managed repository process retirement",
        )

    async def _captured_teardown_pod_is_absent(
        self,
        owner: WorkspaceOwner,
    ) -> bool:
        """Re-prove a Pod-less S36 intent without adopting a same-name Pod."""

        try:
            await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            return getattr(exc, "status", None) == 404
        return False

    async def release_absent_workspace(
        self,
        owner: "WorkspaceOwner",
        *,
        reclaim_volume: bool = False,
        expected_runtime_incarnation: str | None = None,
        strict: bool = False,
    ) -> bool:
        """Finish cleanup only after Kubernetes proves the Pod exactly absent.

        The caller must already hold terminal lifecycle authority preventing a
        same-name successor from being admitted.  This method independently
        requires an ``exact_absent`` Pod-authority result (a Kubernetes 404)
        before it clears stale context or mutates the Service/PVC.  A live or
        terminal object, a replacement UID, and an ambiguous API result all
        fail closed without effects.
        """

        if not self._k8s_available:
            return False
        authority = await self.workspace_pod_authority(
            owner,
            expected_runtime_incarnation=str(expected_runtime_incarnation or ""),
        )
        if authority != "exact_absent":
            logger.warning(
                "Refusing absent-workspace cleanup for %s %s: Pod authority "
                "is %s for retired runtime %r",
                owner.kind,
                owner.id,
                authority,
                expected_runtime_incarnation,
            )
            return False

        # A lost Pod DELETE response can arrive while its durable cleanup is
        # still pending. Settle that exact intent before changing the guarded
        # projection; the legacy context merge cannot publish this authority.
        get_intent = getattr(
            type(self._db), "get_managed_repository_workspace_cleanup_intent", None
        )
        if expected_runtime_incarnation and callable(get_intent):
            intent = await get_intent(
                self._db,
                owner.id,
                owner_kind="thread" if owner.kind == "session" else "job",
                scope="workspace_container",
                runtime_incarnation=expected_runtime_incarnation,
            )
            if isinstance(intent, dict):
                if (
                    intent.get("target_disposition") != "deleted"
                    or bool(intent.get("reclaim_shared_resources")) != reclaim_volume
                ):
                    return False
                outcome = await self.reconcile_workspace_cleanup_intent(
                    owner,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    intent_generation=int(intent["intent_generation"]),
                )
                return outcome.settled

        # Mirror delete_workspace's 404 branch: absence still needs to clear a
        # stale ready endpoint and close its metering interval.  This happens
        # only after the tri-state probe above has distinguished 404 from API
        # failure; get_workspace_status() deliberately cannot make that claim.
        seed_deleted = await self._delete_seed_configmap(
            owner.pod_name,
            expected_owner=owner,
            expected_pod_uid=expected_runtime_incarnation,
        )
        if strict and not seed_deleted:
            return False

        await self._set_context(
            owner,
            {
                "status": "deleted",
                "pod_ip": None,
                **(
                    {WORKSPACE_RUNTIME_INCARNATION_KEY: None}
                    if owner.kind == "session"
                    else {}
                ),
            },
        )
        await workspace_metering.close_interval(self._db, owner)

        volume_deleted = True
        if reclaim_volume:
            volume_deleted = await self.delete_workspace_pvc(
                owner,
                require_exact_owner=True,
            )
        service_deleted = await self._delete_service(
            owner,
            require_exact_owner=True,
        )
        if strict:
            return bool(seed_deleted and volume_deleted and service_deleted)
        return True

    def _workspace_provision_fence_labels(
        self,
        *,
        owner: WorkspaceOwner,
        runtime_generation: str,
        attempt_id: str,
        resource: str,
    ) -> dict[str, str]:
        component = {
            "pod": "workspace-provision-fence-pod",
            "pvc": "workspace-provision-fence-pvc",
            "seed_configmap": "workspace-provision-fence-seed",
            "service": "workspace-provision-fence-svc",
        }[resource]
        return {
            # Fence objects deliberately do not satisfy any ordinary workspace,
            # lifecycle-reaper, adoption, or routing selector.  The exact
            # post-horizon UID GC below is their sole deletion owner.
            "app": "srw-workspace-fence",
            "srw/component": component,
            "srw.io/component": "workspace-provision-fence",
            owner.label_key: owner.id,
            WORKSPACE_PROVISION_ATTEMPT_LABEL: attempt_id,
            WORKSPACE_PROVISION_GENERATION_LABEL: runtime_generation,
            WORKSPACE_PROVISION_FENCE_LABEL: "true",
        }

    async def _workspace_provision_resource_authority(
        self,
        *,
        owner: WorkspaceOwner,
        resource: str,
        name: str,
        namespace: str,
        runtime_generation: str,
        attempt_id: str,
        network_tier: str,
    ) -> dict[str, str | None]:
        readers = {
            "pod": self._core_api.read_namespaced_pod,
            "pvc": self._core_api.read_namespaced_persistent_volume_claim,
            "seed_configmap": self._core_api.read_namespaced_config_map,
            "service": self._core_api.read_namespaced_service,
        }
        reader = readers.get(resource)
        if reader is None or not self._k8s_available:
            return {"state": "unknown", "uid": None}
        try:
            observed = await self._bounded_kubernetes_call(
                reader,
                name=name,
                namespace=namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return {"state": "exact_absent", "uid": None}
            return {"state": "unknown", "uid": None}
        metadata = getattr(observed, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        uid = str(getattr(metadata, "uid", "") or "")
        exact = bool(
            uid
            and str(getattr(metadata, "name", "") or "") == name
            and str(getattr(metadata, "namespace", "") or "") == namespace
            and labels.get(owner.label_key) == owner.id
            and labels.get(WORKSPACE_PROVISION_ATTEMPT_LABEL) == attempt_id
            and labels.get(WORKSPACE_PROVISION_GENERATION_LABEL) == runtime_generation
        )
        if not exact:
            return {"state": "replacement", "uid": None}
        if getattr(metadata, "deletion_timestamp", None) is not None:
            return {"state": "exact_deleting", "uid": uid}
        opposite_owner = "srw/job-id" if owner.kind == "session" else "srw/thread-id"
        fence_labels = self._workspace_provision_fence_labels(
            owner=owner,
            runtime_generation=runtime_generation,
            attempt_id=attempt_id,
            resource=resource,
        )
        normal_component = {
            "pod": owner.component_label,
            "pvc": "workspace-pvc",
            "seed_configmap": "workspace-seed",
            "service": "workspace-svc",
        }[resource]
        fence = labels.get(WORKSPACE_PROVISION_FENCE_LABEL) == "true"
        if (
            opposite_owner in labels
            or (
                fence
                and any(labels.get(key) != value for key, value in fence_labels.items())
            )
            or (
                not fence
                and (
                    WORKSPACE_PROVISION_FENCE_LABEL in labels
                    or labels.get("app") != "srw-workspace"
                    or labels.get("srw/component") != normal_component
                    or labels.get("srw.io/component") != "agent-workspace"
                    or (
                        resource == "pod"
                        and labels.get("srw.io/network-tier") != network_tier
                    )
                )
            )
        ):
            return {"state": "replacement", "uid": None}
        return {"state": "exact_fence" if fence else "exact_original", "uid": uid}

    async def _delete_workspace_provision_resource_exact(
        self, *, resource: str, name: str, namespace: str, uid: str
    ) -> bool:
        deleters = {
            "pod": self._core_api.delete_namespaced_pod,
            "pvc": self._core_api.delete_namespaced_persistent_volume_claim,
            "seed_configmap": self._core_api.delete_namespaced_config_map,
            "service": self._core_api.delete_namespaced_service,
        }
        deleter = deleters.get(resource)
        if deleter is None or not name or not uid:
            return False
        kwargs: dict[str, Any] = {
            "name": name,
            "namespace": namespace,
            "body": {"preconditions": {"uid": uid}},
        }
        if resource == "pod":
            kwargs["grace_period_seconds"] = 0
        try:
            await self._bounded_kubernetes_call(deleter, **kwargs)
            return True
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return True
            # A 409 is an exact-UID precondition refusal.  The caller observes
            # the name again and must classify the replacement before proceeding.
            return False

    async def _wait_workspace_provision_resource_absent(
        self,
        *,
        resource: str,
        name: str,
        namespace: str,
        expected_uid: str,
    ) -> bool:
        """Wait for exact deletion without accepting a same-name replacement."""

        readers = {
            "pod": self._core_api.read_namespaced_pod,
            "pvc": self._core_api.read_namespaced_persistent_volume_claim,
            "seed_configmap": self._core_api.read_namespaced_config_map,
            "service": self._core_api.read_namespaced_service,
        }
        reader = readers.get(resource)
        if reader is None or not name or not expected_uid:
            return False
        for _ in range(40):
            try:
                observed = await self._bounded_kubernetes_call(
                    reader,
                    name=name,
                    namespace=namespace,
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    return True
                return False
            observed_uid = str(
                getattr(getattr(observed, "metadata", None), "uid", "") or ""
            )
            if observed_uid != expected_uid:
                return False
            await asyncio.sleep(0.25)
        return False

    def _workspace_provision_fence_manifest(
        self,
        *,
        owner: WorkspaceOwner,
        resource: str,
        name: str,
        namespace: str,
        runtime_generation: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        labels = self._workspace_provision_fence_labels(
            owner=owner,
            runtime_generation=runtime_generation,
            attempt_id=attempt_id,
            resource=resource,
        )
        metadata = {
            "name": name,
            "namespace": namespace,
            "labels": labels,
        }
        if resource == "pod":
            return {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": metadata,
                "spec": {
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "schedulerName": "srw-retirement-fence",
                    "containers": [
                        {
                            "name": "fence",
                            "image": self._workspace_image,
                            "command": ["/bin/sh", "-c", "exit 0"],
                        }
                    ],
                },
            }
        if resource == "pvc":
            return {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": metadata,
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "",
                    "resources": {"requests": {"storage": "1Mi"}},
                },
            }
        if resource == "seed_configmap":
            return {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": metadata,
                "data": {},
            }
        if resource == "service":
            return {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": metadata,
                "spec": {
                    "clusterIP": "None",
                    "selector": {
                        "srw.io/workspace-fence-never": attempt_id,
                    },
                },
            }
        raise WorkspaceRuntimeAuthorityError(
            "workspace provision fence kind is invalid"
        )

    async def _fence_workspace_provision_resource(
        self,
        *,
        owner: WorkspaceOwner,
        resource: str,
        name: str,
        namespace: str,
        runtime_generation: str,
        attempt_id: str,
        network_tier: str,
    ) -> str | None:
        creators = {
            "pod": self._core_api.create_namespaced_pod,
            "pvc": self._core_api.create_namespaced_persistent_volume_claim,
            "seed_configmap": self._core_api.create_namespaced_config_map,
            "service": self._core_api.create_namespaced_service,
        }
        creator = creators.get(resource)
        if creator is None:
            return None
        manifest = self._workspace_provision_fence_manifest(
            owner=owner,
            resource=resource,
            name=name,
            namespace=namespace,
            runtime_generation=runtime_generation,
            attempt_id=attempt_id,
        )
        # Each pass either wins the name with the fence or deletes the exact
        # original UID.  A foreign replacement, ambiguous observation, or a
        # deletion that does not become visible fails closed for leader retry.
        for _ in range(4):
            try:
                created = await self._bounded_kubernetes_call(
                    creator,
                    namespace=namespace,
                    body=manifest,
                    _request_timeout=(
                        5,
                        PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS,
                    ),
                )
                uid = str(getattr(getattr(created, "metadata", None), "uid", "") or "")
                if not uid:
                    observed = await self._workspace_provision_resource_authority(
                        owner=owner,
                        resource=resource,
                        name=name,
                        namespace=namespace,
                        runtime_generation=runtime_generation,
                        attempt_id=attempt_id,
                        network_tier=network_tier,
                    )
                    return (
                        str(observed.get("uid") or "") or None
                        if observed.get("state") == "exact_fence"
                        else None
                    )
                observed = await self._workspace_provision_resource_authority(
                    owner=owner,
                    resource=resource,
                    name=name,
                    namespace=namespace,
                    runtime_generation=runtime_generation,
                    attempt_id=attempt_id,
                    network_tier=network_tier,
                )
                return (
                    uid
                    if observed.get("state") == "exact_fence"
                    and str(observed.get("uid") or "") == uid
                    else None
                )
            except Exception:
                observed = await self._workspace_provision_resource_authority(
                    owner=owner,
                    resource=resource,
                    name=name,
                    namespace=namespace,
                    runtime_generation=runtime_generation,
                    attempt_id=attempt_id,
                    network_tier=network_tier,
                )
                state = observed.get("state")
                observed_uid = str(observed.get("uid") or "")
                if state == "exact_fence" and observed_uid:
                    return observed_uid
                if state != "exact_original" or not observed_uid:
                    return None
                if not await self._delete_workspace_provision_resource_exact(
                    resource=resource,
                    name=name,
                    namespace=namespace,
                    uid=observed_uid,
                ):
                    return None
                for _wait in range(20):
                    after = await self._workspace_provision_resource_authority(
                        owner=owner,
                        resource=resource,
                        name=name,
                        namespace=namespace,
                        runtime_generation=runtime_generation,
                        attempt_id=attempt_id,
                        network_tier=network_tier,
                    )
                    if after.get("state") == "exact_absent":
                        break
                    if after.get("state") in {"replacement", "unknown"}:
                        return None
                    await asyncio.sleep(0.25)
                else:
                    return None
        return None

    async def fence_pinned_workspace_provision_intent(
        self,
        intent: Mapping[str, Any],
        *,
        permanent: bool,
    ) -> dict[str, str | None] | None:
        """Close every potential create from one revoked pinned attempt."""

        try:
            thread_id = str(UUID(str(intent.get("thread_id") or "")))
            runtime_generation = str(UUID(str(intent.get("runtime_generation") or "")))
            attempt_id = str(UUID(str(intent.get("attempt_id") or "")))
        except (TypeError, ValueError):
            return None
        namespace = str(intent.get("namespace") or "").strip()
        network_tier = str(intent.get("network_tier") or "").strip()
        if not namespace or not network_tier:
            return None
        owner = WorkspaceOwner.session(thread_id)
        names = {
            "pod": str(intent.get("pod_name") or "") or None,
            "pvc": str(intent.get("pvc_name") or "") or None,
            "seed_configmap": (str(intent.get("seed_configmap_name") or "") or None),
            "service": str(intent.get("service_name") or "") or None,
        }
        retained = {
            "pvc": str(intent.get("retained_pvc_uid") or "") or None,
            "service": str(intent.get("retained_service_uid") or "") or None,
        }
        status = str(intent.get("status") or "")
        if status not in {"revoking", "fenced", "retired"}:
            return None
        if not names["pod"]:
            return None
        fences: dict[str, str | None] = {
            "fence_pod_uid": str(intent.get("fence_pod_uid") or "") or None,
            "fence_pvc_uid": str(intent.get("fence_pvc_uid") or "") or None,
            "fence_configmap_uid": (
                str(intent.get("fence_configmap_uid") or "") or None
            ),
            "fence_service_uid": (str(intent.get("fence_service_uid") or "") or None),
        }
        fence_fields = {
            "pod": "fence_pod_uid",
            "pvc": "fence_pvc_uid",
            "seed_configmap": "fence_configmap_uid",
            "service": "fence_service_uid",
        }
        for resource, name in names.items():
            if name is None:
                continue
            fence_field = fence_fields[resource]
            existing_fence_uid = fences.get(fence_field)
            if status != "retired" and existing_fence_uid is not None:
                observed = await self._workspace_provision_resource_authority(
                    owner=owner,
                    resource=resource,
                    name=name,
                    namespace=namespace,
                    runtime_generation=runtime_generation,
                    attempt_id=attempt_id,
                    network_tier=network_tier,
                )
                if not (
                    observed.get("state") == "exact_fence"
                    and str(observed.get("uid") or "") == existing_fence_uid
                ):
                    return None
                continue
            retained_uid = retained.get(resource)
            if retained_uid is not None:
                if not permanent:
                    continue
                # The namespace is immutable provision authority captured before
                # the first create.  Never redirect a retirement to today's
                # configured namespace (``_delete_pvc``/``_delete_service`` use
                # ``self._namespace``); an operator namespace change must not
                # leave the captured PVC/Service live or delete a same-name
                # replacement elsewhere.
                deleted = await self._delete_workspace_provision_resource_exact(
                    resource=resource,
                    name=name,
                    namespace=namespace,
                    uid=retained_uid,
                )
                if (
                    not deleted
                    or not await self._wait_workspace_provision_resource_absent(
                        resource=resource,
                        name=name,
                        namespace=namespace,
                        expected_uid=retained_uid,
                    )
                ):
                    return None
                # This attempt never submitted CREATE for a retained object.
                # Once its other fences have survived the request horizon, an
                # exact UID delete followed by observed absence is causal proof
                # for a permanent follow-up; do not create an unrecorded fence
                # after the intent is terminal.
                if status == "retired":
                    continue
            if status == "retired":
                continue
            fence_uid = await self._fence_workspace_provision_resource(
                owner=owner,
                resource=resource,
                name=name,
                namespace=namespace,
                runtime_generation=runtime_generation,
                attempt_id=attempt_id,
                network_tier=network_tier,
            )
            if not fence_uid:
                return None
            fences[fence_field] = fence_uid
        return fences

    async def delete_pinned_workspace_provision_fences_exact(
        self, intent: Mapping[str, Any]
    ) -> bool:
        """GC only the recorded post-horizon fence UIDs."""

        names = {
            "pod": str(intent.get("pod_name") or "") or None,
            "pvc": str(intent.get("pvc_name") or "") or None,
            "seed_configmap": (str(intent.get("seed_configmap_name") or "") or None),
            "service": str(intent.get("service_name") or "") or None,
        }
        namespace = str(intent.get("namespace") or "").strip()
        if not namespace:
            return False
        fence_fields = {
            "pod": "fence_pod_uid",
            "pvc": "fence_pvc_uid",
            "seed_configmap": "fence_configmap_uid",
            "service": "fence_service_uid",
        }
        for resource, name in names.items():
            uid = str(intent.get(fence_fields[resource]) or "") or None
            if name is None or uid is None:
                continue
            if not await self._delete_workspace_provision_resource_exact(
                resource=resource,
                name=name,
                namespace=namespace,
                uid=uid,
            ):
                return False
            if not await self._wait_workspace_provision_resource_absent(
                resource=resource,
                name=name,
                namespace=namespace,
                expected_uid=uid,
            ):
                return False
        return True

    async def get_workspace_status(self, owner: WorkspaceOwner) -> Optional[dict]:
        """Query the workspace container status.

        Returns:
            Status dict or None if not found.
        """
        if not self._k8s_available:
            return None

        pod_name = owner.pod_name

        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
            phase = pod.status.phase  # Pending, Running, Succeeded, Failed
            pod_ip = pod.status.pod_ip

            ready = False
            if pod.status.container_statuses:
                ready = all(cs.ready for cs in pod.status.container_statuses)

            return {
                "owner_id": owner.id,
                "pod_name": pod_name,
                "phase": phase,
                "pod_ip": pod_ip,
                "ready": ready,
                "runtime_incarnation": str(
                    getattr(getattr(pod, "metadata", None), "uid", "") or ""
                ),
            }
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                return None
            logger.debug(
                "Workspace status query failed for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return None

    async def get_last_termination(self, owner: WorkspaceOwner) -> Optional[dict]:
        """Read the workspace pod's terminated-container cause, for legibility.

        Called on workspace loss BEFORE ``delete_workspace`` (while the Failed
        pod tombstone still exists) so a resource kill surfaces its true cause
        (``OOMKilled`` / ``Evicted``) instead of the opaque downstream SSH error
        the agent happened to hit. Mirrors the agent-pod reap classifier in
        ``agent_provisioner`` — the kill reason lives in ``state.terminated`` or,
        if the container restarted, ``last_state.terminated``.

        Returns ``{phase, pod_reason, container_reason, exit_code, signal}`` or
        ``None`` when the pod is already gone (404) or K8s is unavailable.
        """
        if not self._k8s_available:
            return None
        pod_name = owner.pod_name
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
        except Exception as e:
            if getattr(e, "status", None) == 404:
                return None
            logger.debug(
                "Termination read failed for %s %s: %s", owner.kind, owner.id, e
            )
            return None
        try:
            self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
        except WorkspaceRuntimeAuthorityError:
            return None

        status = pod.status
        exit_code: Any = None
        container_reason: Any = None
        signal: Any = None
        for cs in getattr(status, "container_statuses", None) or []:
            if cs.name != "workspace":
                continue
            terminated = getattr(getattr(cs, "state", None), "terminated", None)
            if terminated is None:
                # Container restarted (e.g. OOMKilled then restarted): the kill
                # reason lives in last_state.terminated, not state.
                terminated = getattr(
                    getattr(cs, "last_state", None), "terminated", None
                )
            if terminated is not None:
                exit_code = getattr(terminated, "exit_code", None)
                container_reason = getattr(terminated, "reason", None)
                signal = getattr(terminated, "signal", None)
            break
        return {
            "phase": getattr(status, "phase", None),
            # pod-level status.reason is "Evicted" on node-pressure eviction
            "pod_reason": getattr(status, "reason", None),
            "container_reason": container_reason,
            "exit_code": exit_code,
            "signal": signal,
        }

    async def workspace_pod_live(
        self,
        owner: "WorkspaceOwner",
        *,
        expected_runtime_incarnation: str | None = None,
    ) -> Optional[bool]:
        """Drift probe: is the owner's exact workspace pod actually alive?

        Returns:
            ``True``  — pod exists and is ``Running``/``Pending`` (usable or
                        still coming up), and its Kubernetes UID matches
                        ``expected_runtime_incarnation`` when supplied.
            ``False`` — pod is confirmed gone (404), or a terminal tombstone
                        whose containers are all terminated. A same-name
                        replacement is also not the requested incarnation.
            ``None``  — can't tell: no k8s client, or a transient API error.

        Mutation callers MUST treat ``None`` as "assume live" so a probe blip
        (or a non-k8s backend) never triggers a false recreate of a healthy
        workspace. Credential-delivery callers may instead fail closed without
        mutating durable state.
        """
        if not self._k8s_available:
            return None
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as e:
            if getattr(e, "status", None) == 404:
                return False
            logger.debug(
                "workspace_pod_live probe failed for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return None
        try:
            self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
        except WorkspaceRuntimeAuthorityError:
            return False
        if (
            getattr(getattr(pod, "metadata", None), "deletion_timestamp", None)
            is not None
        ):
            # A terminating Pod may retain running containers throughout its
            # grace period. It is neither stable-live nor quiescence proof.
            return None
        phase = getattr(pod.status, "phase", None)
        if expected_runtime_incarnation is not None:
            runtime_incarnation = str(getattr(pod.metadata, "uid", "") or "")
            if runtime_incarnation != str(expected_runtime_incarnation):
                return False
        if phase in ("Running", "Pending"):
            return True
        if phase in ("Failed", "Succeeded"):
            statuses = getattr(pod.status, "container_statuses", None) or []
            if statuses and all(
                getattr(getattr(status, "state", None), "terminated", None) is not None
                for status in statuses
            ):
                return False
        # Unknown, missing status, or a terminal-looking Pod without complete
        # container termination evidence remains ambiguous under partitions.
        return None

    async def workspace_pod_authority(
        self,
        owner: "WorkspaceOwner",
        *,
        expected_runtime_incarnation: str,
    ) -> str:
        """Classify one exact terminal Pod authority without conflating drift.

        Returns ``exact_live``, ``exact_terminal``, ``exact_absent``,
        ``replacement``, or ``unknown``.  API-object absence is distinct from
        an observed exact UID whose containers are all terminated: only the
        latter proves resident processes have stopped.
        """

        if not self._k8s_available:
            return "unknown"
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return "exact_absent"
            return "unknown"
        observed = str(getattr(getattr(pod, "metadata", None), "uid", "") or "")
        try:
            self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )
        except WorkspaceRuntimeAuthorityError:
            return "replacement"
        if observed != str(expected_runtime_incarnation):
            return "replacement"
        phase = getattr(pod.status, "phase", None)
        if _pod_has_exact_process_zero(pod):
            return "exact_terminal"
        if getattr(pod.metadata, "deletion_timestamp", None) is not None:
            return "unknown"
        if phase in ("Running", "Pending"):
            return "exact_live"
        return "unknown"

    async def wait_for_workspace_code_server(
        self,
        owner: "WorkspaceOwner",
        *,
        expected_runtime_incarnation: str,
        timeout: float = 45.0,
    ) -> bool:
        """Prove code-server answers from one exact workspace Pod UID.

        Kubernetes readiness currently attests SSH.  A delivered warm-attach
        abort's all-UID zero proof also kills code-server, so successor
        recovery has the stronger obligation to recreate the Pod and verify
        the IDE listener before publishing G2 provisioning.  The Pod identity
        is checked both before and after the HTTP probe; replacement, absence,
        or an ambiguous control-plane read fails closed.
        """

        if not self._k8s_available:
            return False
        try:
            expected_runtime_incarnation = str(UUID(str(expected_runtime_incarnation)))
        except (TypeError, ValueError):
            return False

        import httpx

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        async with httpx.AsyncClient(timeout=3.0) as client:
            while loop.time() < deadline:
                try:
                    if (
                        await self.workspace_pod_authority(
                            owner,
                            expected_runtime_incarnation=(expected_runtime_incarnation),
                        )
                        != "exact_live"
                    ):
                        return False
                    pod = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_pod,
                        name=owner.pod_name,
                        namespace=self._namespace,
                    )
                    observed = self._require_workspace_pod_owner(
                        pod,
                        owner=owner,
                        allow_owner_unlabeled=False,
                    )
                    if observed != expected_runtime_incarnation:
                        return False
                    pod_ip = str(getattr(pod.status, "pod_ip", "") or "")
                    if not pod_ip:
                        return False
                    response = await client.get(f"http://{pod_ip}:38080/healthz")
                    if response.status_code < 500:
                        return (
                            await self.workspace_pod_authority(
                                owner,
                                expected_runtime_incarnation=(
                                    expected_runtime_incarnation
                                ),
                            )
                            == "exact_live"
                        )
                except Exception:
                    pass
                await asyncio.sleep(1)
        return False

    # =========================================================================
    # IDE session pods (on-demand code-server for workspace browsing)
    # =========================================================================

    async def create_ide_pod(
        self,
        job_id: str,
        cpu: str = "250m",
        memory: str = "512Mi",
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
        *,
        operation_id: str | None = None,
    ) -> Optional[str]:
        """Create an IDE only under one durable owner/scope reservation."""

        if not self._k8s_available or self._db is None:
            return None
        reserve = getattr(
            type(self._db),
            "reserve_managed_repository_workspace_creation",
            None,
        )
        settle = getattr(
            type(self._db),
            "settle_managed_repository_workspace_creation_reservation",
            None,
        )
        if not callable(reserve) or not callable(settle):
            return None
        if operation_id is not None:
            try:
                operation_id = _canonical_runtime_uuid(
                    operation_id, label="IDE creation operation"
                )
            except ValueError:
                return None
        claimant = f"ide-restore:{operation_id or uuid4()}"
        creation_plan = await self._ide_creation_plan(
            job_id,
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
        )
        reservation = await reserve(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            claimant=claimant,
            lease_seconds=1800,
            operation_kind="restore",
            desired_manifest_digest=str(creation_plan["digest"]),
        )
        if not isinstance(reservation, dict):
            return None
        owner = WorkspaceOwner.job(job_id)
        async with self._workspace_mutation_guard(owner, scope="ide") as mutation_owned:
            if not mutation_owned:
                return None
            pod_ip = await self._create_ide_pod_reserved(
                job_id,
                cpu=cpu,
                memory=memory,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                _creation_reservation=reservation,
                _creation_plan=creation_plan,
            )
        if pod_ip is None:
            if reservation.get("external_mutation_started_at") is None:
                abort = getattr(
                    type(self._db),
                    "abort_managed_repository_workspace_creation_reservation",
                    None,
                )
                if callable(abort):
                    await abort(
                        self._db,
                        job_id,
                        owner_kind="job",
                        scope="ide",
                        reservation_generation=int(
                            reservation["reservation_generation"]
                        ),
                        claimant=str(reservation["claimed_by"]),
                        claim_token=int(reservation["claim_token"]),
                    )
            return None
        runtime = reservation.get("runtime_incarnation")
        if runtime is None or not await settle(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            reservation_generation=int(reservation["reservation_generation"]),
            claimant=str(reservation["claimed_by"]),
            claim_token=int(reservation["claim_token"]),
            runtime_incarnation=str(runtime),
        ):
            return None
        return pod_ip

    async def get_ide_creation_result(
        self,
        job_id: str,
        *,
        operation_id: str,
    ) -> dict[str, Any] | None:
        """Read one exact caller-owned IDE restore generation."""

        if self._db is None:
            return None
        try:
            operation_id = _canonical_runtime_uuid(
                operation_id, label="IDE creation operation"
            )
        except ValueError:
            return None
        read = getattr(
            type(self._db),
            "get_managed_repository_workspace_creation_result",
            None,
        )
        if not callable(read):
            return None
        return await read(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            claimant=f"ide-restore:{operation_id}",
            operation_kind="restore",
        )

    async def get_current_ide_creation_result(
        self,
        job_id: str,
    ) -> dict[str, Any] | None:
        """Read only the restore reservation named by current IDE runtime B."""

        if self._db is None:
            return None
        read = getattr(
            type(self._db),
            "get_current_managed_repository_workspace_creation_result",
            None,
        )
        if not callable(read):
            return None
        return await read(
            self._db,
            job_id,
            owner_kind="job",
            scope="ide",
            operation_kind="restore",
        )

    async def _create_ide_pod_reserved(
        self,
        job_id: str,
        cpu: str = "250m",
        memory: str = "512Mi",
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
        *,
        _creation_reservation: dict[str, Any] | None = None,
        _creation_plan: dict[str, Any] | None = None,
    ) -> Optional[str]:
        """Create a lightweight IDE pod for browsing a job's workspace.

        Unlike ``create_workspace`` (which provisions the agent's execution
        environment), this creates a short-lived pod purely for code-server
        access. The Gitea repo is cloned after the pod is ready via SSH.

        Args:
            job_id: Job UUID.
            cpu/memory: Resource requests (lower than workspace defaults).
            cpu_limit/memory_limit: Resource limits.

        Returns:
            Pod IP if the pod became ready, None on failure.
        """
        if not self._k8s_available:
            return None
        if self._db is None or not isinstance(_creation_reservation, dict):
            return None
        if not isinstance(_creation_plan, dict) or str(
            _creation_plan.get("digest") or ""
        ) != str(_creation_reservation.get("desired_manifest_digest") or ""):
            return None

        pod_name = f"ide-{job_id[:12]}"
        network_tier = str(_creation_plan["network_tier"])

        owner = WorkspaceOwner.job(job_id)
        if not await self._start_workspace_creation_reservation(
            owner,
            _creation_reservation,
            scope="ide",
        ):
            return None

        async def mutation_authority() -> bool:
            return await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="ide",
            )

        cpu = str(_creation_plan["cpu"])
        memory = str(_creation_plan["memory"])
        cpu_limit = str(_creation_plan["cpu_limit"])
        memory_limit = str(_creation_plan["memory_limit"])
        seed_files = _creation_plan.get("seed_files") or {}
        seed_exts = _creation_plan.get("seed_extensions") or {}
        seed_needs_state = bool(_creation_plan.get("seed_needs_state"))
        if not await self._workspace_creation_reservation_is_current(
            owner,
            _creation_reservation,
            scope="ide",
        ):
            return None
        if seed_files or seed_exts:
            if not await self._begin_workspace_creation_effect(
                owner,
                _creation_reservation,
                scope="ide",
                resource_kind="seed",
            ):
                return None
        seed_cm = await self._create_seed_configmap(
            pod_name,
            seed_files,
            seed_exts,
            needs_state=seed_needs_state,
            expected_owner=owner,
            creation_reservation_id=str(_creation_reservation["id"]),
            mutation_authority=mutation_authority,
        )
        if seed_cm is not None:
            try:
                reservation_seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_cm,
                    namespace=self._namespace,
                )
                reservation_seed_uid = self._require_stateless_seed_configmap_identity(
                    reservation_seed,
                    owner=owner,
                    pod_name=pod_name,
                    creation_reservation_id=str(_creation_reservation["id"]),
                )
            except Exception:
                return None
            if not await self._record_workspace_creation_resource(
                owner,
                _creation_reservation,
                scope="ide",
                resource_kind="seed",
                resource_uid=reservation_seed_uid,
            ):
                return None

        pod_manifest = self._build_pod_manifest(
            pod_name=pod_name,
            owner=owner,
            image=str(_creation_plan["image"]),
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            network_tier=network_tier,
            seed_configmap=seed_cm,
            creation_reservation_id=str(_creation_reservation["id"]),
        )

        # Override labels to distinguish IDE pods from workspace pods
        pod_manifest["metadata"]["labels"]["srw/component"] = "ide-session"

        try:
            if not await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="ide",
            ):
                return None
            if not await self._begin_workspace_creation_effect(
                owner,
                _creation_reservation,
                scope="ide",
                resource_kind="pod",
            ):
                return None
            reused_existing_pod = False
            try:
                created_pod = await self._bounded_kubernetes_mutation(
                    self._core_api.create_namespaced_pod,
                    namespace=self._namespace,
                    body=pod_manifest,
                )
                if created_pod is None:
                    created_pod = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_pod,
                        name=pod_name,
                        namespace=self._namespace,
                    )
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
            except Exception as create_error:
                if getattr(create_error, "status", None) != 409:
                    raise
                reused_existing_pod = True
                created_pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
            self._require_workspace_creation_reservation_annotation(
                created_pod,
                reservation_id=str(_creation_reservation["id"]),
            )
            if not await self._authorize_workspace_creation_runtime(
                owner,
                _creation_reservation,
                scope="ide",
                runtime_incarnation=runtime_incarnation,
            ):
                return None
            if not await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="ide",
            ) or not await self._db.merge_ide_session_context(
                job_id,
                {
                    WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                    WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY: str(
                        _creation_reservation["id"]
                    ),
                    WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY: str(
                        _creation_reservation["claim_token"]
                    ),
                    "container_name": pod_name,
                    "restore_type": "k8s_container",
                },
            ):
                return None
            observed_seed = self._require_stateless_pod_storage_binding(
                created_pod,
                owner=owner,
                expected_pvc_name=None,
                expected_seed_configmap=(
                    _UNSPECIFIED_RESOURCE_BINDING if reused_existing_pod else seed_cm
                ),
                expected_pod_name=pod_name,
            )
            if reused_existing_pod:
                # Settings can drift while an exact-owner IDE Pod remains
                # live. Its immutable mount plan, not today's desired seed,
                # remains authoritative. Never delete an incumbent-bound CM.
                if observed_seed is not None:
                    existing_seed = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=observed_seed,
                        namespace=self._namespace,
                    )
                    self._require_stateless_seed_configmap_identity(
                        existing_seed,
                        owner=owner,
                        pod_name=pod_name,
                        creation_reservation_id=str(_creation_reservation["id"]),
                    )
                elif seed_cm is not None:
                    # A response may be ambiguous after seed creation. Leave
                    # the exact reservation-owned ConfigMap for reconciliation
                    # instead of deleting by deterministic name.
                    return None
                seed_cm = observed_seed
            elif not await self._workspace_creation_reservation_is_current(
                owner,
                _creation_reservation,
                scope="ide",
            ) or (
                await self._adopt_configmap(
                    seed_cm,
                    created_pod,
                    expected_owner=owner,
                    creation_reservation_id=str(_creation_reservation["id"]),
                    mutation_authority=mutation_authority,
                )
                is not True
            ):
                return None
            if not await self._record_workspace_creation_resources(
                owner,
                _creation_reservation,
                scope="ide",
                runtime_incarnation=runtime_incarnation,
                pod_name=pod_name,
                seed_configmap=seed_cm,
                pvc_name=None,
            ):
                return None
            logger.info("IDE pod created: %s (job %s)", pod_name, job_id)

            pod_ip = await self._wait_for_ready(
                pod_name,
                timeout=90,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_network_tier=network_tier,
                expected_pvc_name=None,
                expected_seed_configmap=seed_cm,
                expected_pod_name=pod_name,
                expected_component="ide-session",
            )
            if pod_ip:
                if self._db is None:
                    return None
                (
                    backing_id,
                    host_fingerprint,
                    confirmed_runtime,
                ) = await self._trusted_pod_ssh_identity(
                    pod_name,
                    expected_owner=owner,
                    expected_runtime_incarnation=runtime_incarnation,
                    expected_network_tier=network_tier,
                    expected_seed_configmap=seed_cm,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
                if (
                    confirmed_runtime != runtime_incarnation
                    or not backing_id.startswith("k8s-pod:")
                ):
                    return None
                if seed_needs_state:
                    seed_attestation = await self.attest_ide_runtime(
                        job_id,
                        expected_runtime_incarnation=runtime_incarnation,
                    )
                    if not (
                        seed_attestation.runtime_incarnation == runtime_incarnation
                        and seed_attestation.pod_ip == pod_ip
                        and seed_attestation.backing_id == backing_id
                        and seed_attestation.ssh_host_key_fingerprint
                        == host_fingerprint
                        and await self._workspace_creation_reservation_is_current(
                            owner,
                            _creation_reservation,
                            scope="ide",
                        )
                    ):
                        return None
                    if not await self._seed_workspace_state(
                        owner,
                        seed_attestation,
                        scope="ide",
                        mutation_authority=mutation_authority,
                    ):
                        return None
                if not await self._workspace_creation_reservation_is_current(
                    owner,
                    _creation_reservation,
                    scope="ide",
                ) or not await self._db.merge_ide_session_context(
                    job_id,
                    {
                        WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                        WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY: str(
                            _creation_reservation["id"]
                        ),
                        WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY: str(
                            _creation_reservation["claim_token"]
                        ),
                        "ssh_host_key_fingerprint": host_fingerprint,
                        "pod_ip": pod_ip,
                    },
                ):
                    return None
                logger.info("IDE pod ready: %s @ %s (job %s)", pod_name, pod_ip, job_id)
                return pod_ip

            logger.warning(
                "IDE pod created but not ready within timeout: %s (job %s)",
                pod_name,
                job_id,
            )
            return None
        except Exception as e:
            logger.error("Failed to create IDE pod for job %s: %s", job_id, e)
            return None

    async def delete_ide_pod_with_outcome(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str | None = None,
        cleanup_intent: dict[str, Any] | None = None,
        _mutation_guard_held: bool = False,
    ) -> RuntimeDeletionOutcome:
        """Delete an IDE Pod only after exact repository-agent process-zero."""
        if not self._k8s_available:
            return _RUNTIME_DELETION_REFUSED

        pod_name = f"ide-{job_id[:12]}"
        owner = WorkspaceOwner.job(job_id)
        if not _mutation_guard_held:
            async with self._workspace_mutation_guard(owner, scope="ide") as owned:
                if not owned:
                    return _RUNTIME_DELETION_REFUSED
                return await self.delete_ide_pod_with_outcome(
                    job_id,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                    cleanup_intent=cleanup_intent,
                    _mutation_guard_held=True,
                )
        if (
            cleanup_intent is None
            and expected_runtime_incarnation is not None
            and self._db is not None
        ):
            read_intent = getattr(
                type(self._db),
                "get_managed_repository_workspace_cleanup_intent",
                None,
            )
            if callable(read_intent):
                cleanup_intent = await read_intent(
                    self._db,
                    job_id,
                    owner_kind="job",
                    scope="ide",
                    runtime_incarnation=expected_runtime_incarnation,
                )
        runtime_uid: str | None = None
        already_absent = False
        retained_for_process_zero = False

        try:
            try:
                pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                runtime_uid = self._require_workspace_pod_owner(
                    pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    allow_terminating=True,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
                if expected_runtime_incarnation is not None and runtime_uid != str(
                    expected_runtime_incarnation
                ):
                    if (
                        isinstance(cleanup_intent, dict)
                        and str(cleanup_intent.get("runtime_incarnation") or "")
                        == str(expected_runtime_incarnation)
                        and str(cleanup_intent.get("resource_policy") or "")
                        == "preserve"
                        and cleanup_intent.get("resources_captured_at") is not None
                        and isinstance(cleanup_intent.get("claimed_by"), str)
                        and cleanup_intent.get("claimed_by")
                        and await self._cleanup_claim_is_current(
                            cleanup_intent,
                            claimant=str(cleanup_intent["claimed_by"]),
                        )
                        and await self._managed_repository_process_zero_replay_authority(
                            owner,
                            scope="ide",
                            runtime_incarnation=str(expected_runtime_incarnation),
                        )
                        == "stale"
                    ):
                        return _STALE_RUNTIME_SETTLED
                    return _RUNTIME_DELETION_REFUSED
                if cleanup_intent is None:
                    cleanup_intent = await self.prepare_ide_cleanup_intent(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                        target_disposition="expired",
                    )
                if cleanup_intent is None:
                    cleanup_intent = await self.prepare_ide_cleanup_intent(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                        target_disposition="expired",
                        allow_stale_predecessor=True,
                    )
                if (
                    not isinstance(cleanup_intent, dict)
                    or cleanup_intent.get("resources_captured_at") is None
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                retained_for_process_zero = self._has_stateless_process_zero_finalizer(
                    pod
                )
                authority = self._classify_exact_ide_pod(pod, runtime_uid)
                if retained_for_process_zero:
                    # The finalizer makes Kubernetes termination—not an SSH
                    # race—the process-zero boundary for all newly created IDE
                    # Pods.  Persist the receipt only after this exact UID is
                    # terminal below.
                    process_zero = False
                elif authority == "exact_terminal":
                    process_zero = True
                elif authority == "exact_live":
                    if (
                        self._db is None
                        or not await self._db.claim_managed_repository_workspace_retirement(
                            job_id,
                            owner_kind="job",
                            scope="ide",
                            provisioner="k8s",
                            runtime_incarnation=runtime_uid,
                        )
                    ):
                        return _RUNTIME_DELETION_REFUSED
                    attestation = await self.attest_ide_runtime(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                    )
                    process_zero = await retire_managed_repository_processes(
                        host=attestation.pod_ip,
                        port=attestation.port,
                        host_key_fingerprint=(attestation.ssh_host_key_fingerprint),
                        operation="IDE managed repository process retirement",
                    )
                    if process_zero:
                        process_zero = await self._ide_pod_authority(
                            job_id,
                            expected_runtime_incarnation=runtime_uid,
                        ) in {"exact_live", "exact_terminal"}
                else:
                    return _RUNTIME_DELETION_REFUSED
                if not retained_for_process_zero:
                    if (
                        not process_zero
                        or self._db is None
                        or not await self._db.record_managed_repository_workspace_process_zero(
                            job_id,
                            owner_kind="job",
                            scope="ide",
                            provisioner="k8s",
                            runtime_incarnation=runtime_uid,
                        )
                    ):
                        return _RUNTIME_DELETION_REFUSED
            except Exception as read_error:
                if getattr(read_error, "status", None) != 404:
                    raise
                if self._db is None:
                    return _RUNTIME_DELETION_REFUSED
                if (
                    not isinstance(cleanup_intent, dict)
                    or cleanup_intent.get("resources_captured_at") is None
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                job = await self._db.get_job(job_id)
                context = job.get("context") if isinstance(job, dict) else None
                if isinstance(context, str):
                    context = json.loads(context)
                ide = context.get("ide_session") if isinstance(context, dict) else None
                current_runtime_uid = (
                    str(ide.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
                    if isinstance(ide, dict)
                    else ""
                )
                runtime_uid = str(
                    expected_runtime_incarnation or current_runtime_uid or ""
                )
                if not runtime_uid:
                    return _RUNTIME_DELETION_REFUSED
                replay_authority = (
                    await self._managed_repository_process_zero_replay_authority(
                        owner,
                        scope="ide",
                        runtime_incarnation=runtime_uid,
                    )
                )
                if replay_authority is None:
                    return _RUNTIME_DELETION_REFUSED
                if replay_authority == "stale":
                    # The replacement IDE owns the deterministic seed name;
                    # settle only the predecessor replay.
                    return _STALE_RUNTIME_SETTLED
                already_absent = True
            if not already_absent:
                if (
                    not isinstance(cleanup_intent, dict)
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                await self._bounded_kubernetes_mutation(
                    self._core_api.delete_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    grace_period_seconds=5,
                    body={"preconditions": {"uid": runtime_uid}},
                )
            if retained_for_process_zero and not already_absent:
                deadline = asyncio.get_event_loop().time() + 30
                while asyncio.get_event_loop().time() < deadline:
                    authority = await self._ide_pod_authority(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                    )
                    if authority == "exact_terminal":
                        break
                    if authority in {"replacement", "exact_absent"}:
                        return _RUNTIME_DELETION_REFUSED
                    await asyncio.sleep(1)
                else:
                    return _RUNTIME_DELETION_REFUSED
                if (
                    not isinstance(cleanup_intent, dict)
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                released_finalizer = await self._release_process_zero_finalizer(
                    owner,
                    pod_name=pod_name,
                    expected_runtime_incarnation=runtime_uid,
                    scope="ide",
                    expected_component="ide-session",
                    _mutation_guard_held=True,
                )
                if not released_finalizer:
                    if (
                        await self._ide_pod_authority(
                            job_id,
                            expected_runtime_incarnation=runtime_uid,
                        )
                        != "exact_absent"
                    ):
                        return _RUNTIME_DELETION_REFUSED
                    replay_authority = (
                        await self._managed_repository_process_zero_replay_authority(
                            owner,
                            scope="ide",
                            runtime_incarnation=runtime_uid,
                        )
                    )
                    if replay_authority == "stale":
                        return _STALE_RUNTIME_SETTLED
                    if replay_authority != "current":
                        return _RUNTIME_DELETION_REFUSED
                    already_absent = True
                if not already_absent:
                    deadline = asyncio.get_event_loop().time() + 30
                    while asyncio.get_event_loop().time() < deadline:
                        if (
                            await self._ide_pod_authority(
                                job_id,
                                expected_runtime_incarnation=runtime_uid,
                            )
                            == "exact_absent"
                        ):
                            break
                        await asyncio.sleep(1)
                    else:
                        return _RUNTIME_DELETION_REFUSED
            replay_authority = (
                await self._managed_repository_process_zero_replay_authority(
                    owner,
                    scope="ide",
                    runtime_incarnation=str(runtime_uid or ""),
                )
            )
            if replay_authority == "stale":
                return _STALE_RUNTIME_SETTLED
            logger.info("IDE pod deleted: %s (job %s)", pod_name, job_id)
            if (
                not isinstance(cleanup_intent, dict)
                or not isinstance(cleanup_intent.get("claimed_by"), str)
                or not cleanup_intent.get("claimed_by")
                or not await self._cleanup_claim_is_current(
                    cleanup_intent,
                    claimant=str(cleanup_intent["claimed_by"]),
                )
            ):
                return _RUNTIME_DELETION_REFUSED
            return (
                _CURRENT_RUNTIME_DELETED
                if await self._delete_seed_configmap(
                    pod_name,
                    expected_owner=owner,
                    expected_pod_uid=runtime_uid,
                    expected_configmap_uid=(
                        cleanup_intent.get("seed_configmap_uid")
                        if isinstance(cleanup_intent, dict)
                        else _UNSPECIFIED_RESOURCE_BINDING
                    ),
                )
                else _RUNTIME_DELETION_REFUSED
            )
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                replay_authority = (
                    await self._managed_repository_process_zero_replay_authority(
                        owner,
                        scope="ide",
                        runtime_incarnation=str(runtime_uid or ""),
                    )
                )
                if replay_authority == "stale":
                    return _STALE_RUNTIME_SETTLED
                if replay_authority != "current":
                    return _RUNTIME_DELETION_REFUSED
                if (
                    not isinstance(cleanup_intent, dict)
                    or not isinstance(cleanup_intent.get("claimed_by"), str)
                    or not cleanup_intent.get("claimed_by")
                    or not await self._cleanup_claim_is_current(
                        cleanup_intent,
                        claimant=str(cleanup_intent["claimed_by"]),
                    )
                ):
                    return _RUNTIME_DELETION_REFUSED
                return (
                    _CURRENT_RUNTIME_DELETED
                    if await self._delete_seed_configmap(
                        pod_name,
                        expected_owner=owner,
                        expected_pod_uid=runtime_uid,
                        expected_configmap_uid=(
                            cleanup_intent.get("seed_configmap_uid")
                            if isinstance(cleanup_intent, dict)
                            else _UNSPECIFIED_RESOURCE_BINDING
                        ),
                    )
                    else _RUNTIME_DELETION_REFUSED
                )
            logger.error("Failed to delete IDE pod for job %s: %s", job_id, e)
            return _RUNTIME_DELETION_REFUSED

    async def delete_ide_pod(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str | None = None,
    ) -> bool:
        """Compatibility wrapper that completes the durable IDE intent."""

        runtime = expected_runtime_incarnation
        if runtime is None:
            if not self._k8s_available:
                return False
            pod_name = f"ide-{job_id[:12]}"
            try:
                pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                runtime = self._require_workspace_pod_owner(
                    pod,
                    owner=WorkspaceOwner.job(job_id),
                    allow_owner_unlabeled=False,
                    allow_terminating=True,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
            except Exception:
                return False
        intent = await self.prepare_ide_cleanup_intent(
            job_id,
            expected_runtime_incarnation=str(runtime),
            target_disposition="expired",
        )
        if not isinstance(intent, dict):
            return False
        outcome = await self.reconcile_ide_cleanup_intent(
            job_id,
            expected_runtime_incarnation=str(runtime),
            intent_generation=int(intent["intent_generation"]),
        )
        return outcome.state == "settled"

    @staticmethod
    async def _create_ide_pod_legacy(
        self,
        job_id: str,
        cpu: str = "250m",
        memory: str = "512Mi",
        cpu_limit: str = "1000m",
        memory_limit: str = "2Gi",
    ) -> Optional[str]:
        """Create a lightweight IDE pod for browsing a job's workspace.

        Unlike ``create_workspace`` (which provisions the agent's execution
        environment), this creates a short-lived pod purely for code-server
        access. The Gitea repo is cloned after the pod is ready via SSH.

        Args:
            job_id: Job UUID.
            cpu/memory: Resource requests (lower than workspace defaults).
            cpu_limit/memory_limit: Resource limits.

        Returns:
            Pod IP if the pod became ready, None on failure.
        """
        if not self._k8s_available:
            return None

        pod_name = f"ide-{job_id[:12]}"
        network_tier = await self._resolve_network_tier(job_id, kind="job")

        owner = WorkspaceOwner.job(job_id)
        seed_files = await self._resolve_ide_seed_files(owner)
        seed_exts = await self._resolve_ide_extensions(owner)
        seed_needs_state = await self._resolve_ide_needs_state(owner, seed_exts)
        seed_cm = await self._create_seed_configmap(
            pod_name,
            seed_files,
            seed_exts,
            needs_state=seed_needs_state,
            expected_owner=owner,
        )

        pod_manifest = self._build_pod_manifest(
            pod_name=pod_name,
            owner=owner,
            image=self._workspace_image,
            cpu=cpu,
            memory=memory,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            network_tier=network_tier,
            seed_configmap=seed_cm,
        )

        # Override labels to distinguish IDE pods from workspace pods
        pod_manifest["metadata"]["labels"]["srw/component"] = "ide-session"

        try:
            reused_existing_pod = False
            try:
                created_pod = await self._bounded_kubernetes_call(
                    self._core_api.create_namespaced_pod,
                    namespace=self._namespace,
                    body=pod_manifest,
                )
                if created_pod is None:
                    created_pod = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_pod,
                        name=pod_name,
                        namespace=self._namespace,
                    )
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
            except Exception as create_error:
                if getattr(create_error, "status", None) != 409:
                    raise
                reused_existing_pod = True
                created_pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                runtime_incarnation = self._require_workspace_pod_owner(
                    created_pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    expected_network_tier=network_tier,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
            if self._db is None or not await self._db.merge_ide_session_context(
                job_id,
                {
                    WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                    "container_name": pod_name,
                    "restore_type": "k8s_container",
                },
            ):
                return None
            observed_seed = self._require_stateless_pod_storage_binding(
                created_pod,
                owner=owner,
                expected_pvc_name=None,
                expected_seed_configmap=(
                    _UNSPECIFIED_RESOURCE_BINDING if reused_existing_pod else seed_cm
                ),
                expected_pod_name=pod_name,
            )
            if reused_existing_pod:
                # Settings can drift while an exact-owner IDE Pod remains
                # live. Its immutable mount plan, not today's desired seed,
                # remains authoritative. Never delete an incumbent-bound CM.
                if observed_seed is not None:
                    existing_seed = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=observed_seed,
                        namespace=self._namespace,
                    )
                    try:
                        self._require_stateless_seed_configmap_identity(
                            existing_seed,
                            owner=owner,
                            pod_name=pod_name,
                        )
                    except WorkspaceRuntimeAuthorityError:
                        await self._require_legacy_seed_configmap_migration(
                            existing_seed,
                            owner=owner,
                            pod_name=pod_name,
                        )
                elif seed_cm is not None:
                    if not await self._delete_seed_configmap(
                        pod_name,
                        expected_owner=owner,
                    ):
                        return None
                seed_cm = observed_seed
            elif (
                await self._adopt_configmap(
                    seed_cm,
                    created_pod,
                    expected_owner=owner,
                )
                is not True
            ):
                return None
            logger.info("IDE pod created: %s (job %s)", pod_name, job_id)

            pod_ip = await self._wait_for_ready(
                pod_name,
                timeout=90,
                expected_owner=owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_network_tier=network_tier,
                expected_pvc_name=None,
                expected_seed_configmap=seed_cm,
                expected_pod_name=pod_name,
                expected_component="ide-session",
            )
            if pod_ip:
                if self._db is None:
                    return None
                (
                    backing_id,
                    host_fingerprint,
                    confirmed_runtime,
                ) = await self._trusted_pod_ssh_identity(
                    pod_name,
                    expected_owner=owner,
                    expected_runtime_incarnation=runtime_incarnation,
                    expected_network_tier=network_tier,
                    expected_seed_configmap=seed_cm,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
                if (
                    confirmed_runtime != runtime_incarnation
                    or not backing_id.startswith("k8s-pod:")
                ):
                    return None
                await self._db.merge_ide_session_context(
                    job_id,
                    {
                        WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
                        "ssh_host_key_fingerprint": host_fingerprint,
                        "pod_ip": pod_ip,
                    },
                )
                logger.info("IDE pod ready: %s @ %s (job %s)", pod_name, pod_ip, job_id)
                # Stream license/globalStorage state in (Phase B); fire-and-forget.
                asyncio.create_task(self._seed_workspace_state(owner, pod_ip))
                return pod_ip

            logger.warning(
                "IDE pod created but not ready within timeout: %s (job %s)",
                pod_name,
                job_id,
            )
            return None
        except Exception as e:
            logger.error("Failed to create IDE pod for job %s: %s", job_id, e)
            return None

    async def _delete_ide_pod_legacy(self, job_id: str) -> bool:
        """Delete an IDE Pod only after exact repository-agent process-zero."""
        if not self._k8s_available:
            return False

        pod_name = f"ide-{job_id[:12]}"
        owner = WorkspaceOwner.job(job_id)
        runtime_uid: str | None = None
        already_absent = False
        retained_for_process_zero = False

        try:
            try:
                pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                runtime_uid = self._require_workspace_pod_owner(
                    pod,
                    owner=owner,
                    allow_owner_unlabeled=False,
                    allow_terminating=True,
                    expected_pod_name=pod_name,
                    expected_component="ide-session",
                )
                retained_for_process_zero = self._has_stateless_process_zero_finalizer(
                    pod
                )
                authority = self._classify_exact_ide_pod(pod, runtime_uid)
                if retained_for_process_zero:
                    # The finalizer makes Kubernetes termination—not an SSH
                    # race—the process-zero boundary for all newly created IDE
                    # Pods.  Persist the receipt only after this exact UID is
                    # terminal below.
                    process_zero = False
                elif authority == "exact_terminal":
                    process_zero = True
                elif authority == "exact_live":
                    if (
                        self._db is None
                        or not await self._db.claim_managed_repository_workspace_retirement(
                            job_id,
                            owner_kind="job",
                            scope="ide",
                            provisioner="k8s",
                            runtime_incarnation=runtime_uid,
                        )
                    ):
                        return False
                    attestation = await self.attest_ide_runtime(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                    )
                    process_zero = await retire_managed_repository_processes(
                        host=attestation.pod_ip,
                        port=attestation.port,
                        host_key_fingerprint=(attestation.ssh_host_key_fingerprint),
                        operation="IDE managed repository process retirement",
                    )
                    if process_zero:
                        process_zero = await self._ide_pod_authority(
                            job_id,
                            expected_runtime_incarnation=runtime_uid,
                        ) in {"exact_live", "exact_terminal"}
                else:
                    return False
                if not retained_for_process_zero:
                    if (
                        not process_zero
                        or self._db is None
                        or not await self._db.record_managed_repository_workspace_process_zero(
                            job_id,
                            owner_kind="job",
                            scope="ide",
                            provisioner="k8s",
                            runtime_incarnation=runtime_uid,
                        )
                    ):
                        return False
            except Exception as read_error:
                if getattr(read_error, "status", None) != 404:
                    raise
                if self._db is None:
                    return False
                job = await self._db.get_job(job_id)
                context = job.get("context") if isinstance(job, dict) else None
                if isinstance(context, str):
                    context = json.loads(context)
                ide = context.get("ide_session") if isinstance(context, dict) else None
                runtime_uid = (
                    str(ide.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
                    if isinstance(ide, dict)
                    else ""
                )
                if (
                    not runtime_uid
                    or not await self._db.managed_repository_workspace_process_zero_is_current(
                        job_id,
                        owner_kind="job",
                        scope="ide",
                        provisioner="k8s",
                        runtime_incarnation=runtime_uid,
                    )
                ):
                    return False
                already_absent = True
            if not already_absent:
                await self._bounded_kubernetes_call(
                    self._core_api.delete_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                    grace_period_seconds=5,
                    body={"preconditions": {"uid": runtime_uid}},
                )
            if retained_for_process_zero and not already_absent:
                deadline = asyncio.get_event_loop().time() + 30
                while asyncio.get_event_loop().time() < deadline:
                    authority = await self._ide_pod_authority(
                        job_id,
                        expected_runtime_incarnation=runtime_uid,
                    )
                    if authority == "exact_terminal":
                        break
                    if authority in {"replacement", "exact_absent"}:
                        return False
                    await asyncio.sleep(1)
                else:
                    return False
                if not await self._release_process_zero_finalizer(
                    owner,
                    pod_name=pod_name,
                    expected_runtime_incarnation=runtime_uid,
                    scope="ide",
                    expected_component="ide-session",
                ):
                    return False
                deadline = asyncio.get_event_loop().time() + 30
                while asyncio.get_event_loop().time() < deadline:
                    if (
                        await self._ide_pod_authority(
                            job_id,
                            expected_runtime_incarnation=runtime_uid,
                        )
                        == "exact_absent"
                    ):
                        break
                    await asyncio.sleep(1)
                else:
                    return False
            logger.info("IDE pod deleted: %s (job %s)", pod_name, job_id)
            return await self._delete_seed_configmap(
                pod_name,
                expected_owner=owner,
                expected_pod_uid=runtime_uid,
            )
        except Exception as e:
            if hasattr(e, "status") and e.status == 404:
                return await self._delete_seed_configmap(
                    pod_name,
                    expected_owner=owner,
                    expected_pod_uid=runtime_uid,
                )
            logger.error("Failed to delete IDE pod for job %s: %s", job_id, e)
            return False

    @staticmethod
    def _classify_exact_ide_pod(pod: Any, runtime_uid: str) -> str:
        if str(getattr(getattr(pod, "metadata", None), "uid", "") or "") != str(
            runtime_uid
        ):
            return "replacement"
        if _pod_has_exact_process_zero(pod):
            return "exact_terminal"
        if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None):
            return "unknown"
        if getattr(getattr(pod, "status", None), "phase", None) in {
            "Running",
            "Pending",
        }:
            return "exact_live"
        return "unknown"

    async def _ide_pod_authority(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str,
    ) -> str:
        if not self._k8s_available:
            return "unknown"
        pod_name = f"ide-{job_id[:12]}"
        owner = WorkspaceOwner.job(job_id)
        try:
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            observed = self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
                expected_pod_name=pod_name,
                expected_component="ide-session",
            )
        except Exception as exc:
            return "exact_absent" if getattr(exc, "status", None) == 404 else "unknown"
        if observed != expected_runtime_incarnation:
            return "replacement"
        return self._classify_exact_ide_pod(pod, observed)

    async def attest_ide_runtime(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str,
    ) -> WorkspaceRuntimeAttestation:
        """Attest one live IDE Pod and its pinned SSH identity."""

        pod_name = f"ide-{job_id[:12]}"
        owner = WorkspaceOwner.job(job_id)
        network_tier = await self._resolve_network_tier(job_id, kind="job")
        pod = await self._bounded_kubernetes_call(
            self._core_api.read_namespaced_pod,
            name=pod_name,
            namespace=self._namespace,
        )
        observed = self._require_workspace_pod_owner(
            pod,
            owner=owner,
            allow_owner_unlabeled=False,
            expected_network_tier=network_tier,
            expected_pod_name=pod_name,
            expected_component="ide-session",
        )
        if observed != expected_runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError("IDE Pod UID changed")
        status = getattr(pod, "status", None)
        pod_ip = str(getattr(status, "pod_ip", "") or "")
        statuses = getattr(status, "container_statuses", None)
        if (
            getattr(status, "phase", None) != "Running"
            or not pod_ip
            or not statuses
            or any(getattr(item, "ready", None) is not True for item in statuses)
        ):
            raise WorkspaceRuntimeAuthorityError("IDE Pod is not ready")
        backing_id, fingerprint, confirmed = await self._trusted_pod_ssh_identity(
            pod_name,
            expected_owner=owner,
            expected_runtime_incarnation=observed,
            expected_network_tier=network_tier,
            pvc_name=None,
            expected_pod_name=pod_name,
            expected_component="ide-session",
        )
        if confirmed != observed or not backing_id.startswith("k8s-pod:"):
            raise WorkspaceRuntimeAuthorityError("IDE Pod identity changed")
        return WorkspaceRuntimeAttestation(
            backing_id=backing_id,
            workspace_generation=observed,
            runtime_incarnation=observed,
            ssh_host_key_fingerprint=fingerprint,
            host=pod_ip,
            pod_ip=pod_ip,
        )

    # =========================================================================
    # Internal helpers
    # =========================================================================

    async def _create_pvc(
        self,
        pvc_name: str,
        size: str = "10Gi",
        labels: Optional[dict] = None,
        *,
        storage_class: str | None = None,
        expected_owner: WorkspaceOwner | None = None,
        creation_reservation_id: str | None = None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> Optional[str]:
        """Create a PVC for workspace data. Idempotent.

        Returns ``"created"`` for a new volume, ``"reused"`` if the PVC already
        existed (409 — i.e. a reattach), or ``None`` on failure.
        """
        if not self._k8s_available:
            return None
        resolved_storage_class = storage_class or self._storage_class

        pvc_labels = {
            "app": "srw-workspace",
            "srw/component": "workspace-pvc",
            "srw.io/component": "agent-workspace",
        }
        if labels:
            pvc_labels.update(labels)

        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "namespace": self._namespace,
                "labels": pvc_labels,
                **(
                    {
                        "annotations": {
                            WORKSPACE_CREATION_RESERVATION_ANNOTATION: str(
                                UUID(creation_reservation_id)
                            )
                        }
                    }
                    if creation_reservation_id is not None
                    else {}
                ),
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": resolved_storage_class,
                "resources": {"requests": {"storage": size}},
            },
        }

        try:
            if mutation_authority is not None and not await mutation_authority():
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation authority expired before PVC create"
                )
            created = await self._bounded_kubernetes_mutation(
                self._core_api.create_namespaced_persistent_volume_claim,
                namespace=self._namespace,
                body=pvc_manifest,
            )
            if expected_owner is not None:
                try:
                    claim = created or await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                    self._require_stateless_pvc_identity(
                        claim,
                        owner=expected_owner,
                        pvc_name=pvc_name,
                        expected_storage_class=resolved_storage_class,
                    )
                    if created is not None and creation_reservation_id is not None:
                        self._require_workspace_creation_reservation_annotation(
                            claim,
                            reservation_id=creation_reservation_id,
                        )
                except Exception as authority_error:
                    logger.error(
                        "Stateless workspace PVC authority failed for %s %s: %s",
                        expected_owner.kind,
                        expected_owner.id,
                        authority_error,
                    )
                    return None
            logger.info(
                "PVC created: %s (storageClass=%s)", pvc_name, resolved_storage_class
            )
            return "created"
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                if expected_owner is not None:
                    try:
                        existing = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_persistent_volume_claim,
                            name=pvc_name,
                            namespace=self._namespace,
                        )
                        self._require_stateless_pvc_identity(
                            existing,
                            owner=expected_owner,
                            pvc_name=pvc_name,
                            expected_storage_class=resolved_storage_class,
                        )
                        if (
                            mutation_authority is not None
                            and not await mutation_authority()
                        ):
                            raise WorkspaceRuntimeAuthorityError(
                                "workspace creation authority expired before PVC reuse"
                            )
                    except Exception as authority_error:
                        logger.error(
                            "Refusing stateless PVC reuse for %s %s: %s",
                            expected_owner.kind,
                            expected_owner.id,
                            authority_error,
                        )
                        return None
                    # Advisory only: computed after the authority try/except so a
                    # problem here can never turn a legitimate reuse into a
                    # "Refusing stateless PVC reuse" abort.
                    requests = (
                        getattr(
                            getattr(getattr(existing, "spec", None), "resources", None),
                            "requests",
                            None,
                        )
                        or {}
                    )
                    existing_size = requests.get("storage")
                    if existing_size is not None and existing_size != size:
                        logger.warning(
                            "Reusing PVC %s at %s although %s was requested; "
                            "existing workspace volumes are never resized.",
                            pvc_name,
                            existing_size,
                            size,
                        )
                logger.debug("PVC already exists: %s", pvc_name)
                return "reused"
            # A 403 here is the workspace capacity guard (Phase 3a): the
            # orchestrator SA is allowed to create PVCs, so the only Forbidden
            # it hits is a ResourceQuota "exceeded quota" rejection. Surface it
            # distinctly so an operator/alert can tell "fleet at capacity" from a
            # genuine infra failure. The caller still fails closed (no emptyDir
            # fallback) — capacity exhaustion must not silently drop durability.
            if hasattr(e, "status") and e.status == 403:
                logger.error(
                    "Workspace capacity quota exceeded — PVC %s rejected by "
                    "ResourceQuota; raise workspace.resourceQuota.maxStorage/"
                    "maxCount or wait for jobs to free PVCs: %s",
                    pvc_name,
                    getattr(e, "body", e),
                )
                return None
            logger.error("Failed to create PVC %s: %s", pvc_name, e)
            return None

    def _require_stateless_pvc_identity(
        self,
        claim: Any,
        *,
        owner: WorkspaceOwner,
        pvc_name: str,
        expected_storage_class: str | None = None,
        allow_any_storage_class: bool = False,
    ) -> str:
        """Bind a reused PVC to the full stateless owner, not its name prefix."""

        metadata = getattr(claim, "metadata", None)
        labels = getattr(metadata, "labels", None)
        opposite_owner_label = (
            "srw/job-id" if owner.kind == "session" else "srw/thread-id"
        )
        agent_claim = pvc_name.startswith("pvc-agent-s-")
        expected_app = "srw-agent" if agent_claim else "srw-workspace"
        expected_component = "agent-workspace-pvc" if agent_claim else "workspace-pvc"
        if (
            str(getattr(metadata, "name", "") or "") != pvc_name
            or str(getattr(metadata, "namespace", "") or "") != self._namespace
            or getattr(metadata, "deletion_timestamp", None) is not None
            or not isinstance(labels, dict)
            or labels.get(owner.label_key) != owner.id
            or labels.get("app") != expected_app
            or labels.get("srw/component") != expected_component
            or labels.get("srw.io/component") != "agent-workspace"
            or WORKSPACE_PROVISION_FENCE_LABEL in labels
            or opposite_owner_label in labels
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace PVC owner authority changed"
            )
        spec = getattr(claim, "spec", None)
        access_modes = getattr(spec, "access_modes", None)
        observed_storage_class = getattr(spec, "storage_class_name", None)
        required_storage_class = expected_storage_class or self._storage_class
        if (
            not isinstance(access_modes, (list, tuple))
            or list(access_modes) != ["ReadWriteOnce"]
            or (
                not allow_any_storage_class
                and observed_storage_class != required_storage_class
            )
            or (
                allow_any_storage_class
                and (
                    not isinstance(observed_storage_class, str)
                    or not observed_storage_class
                    or "\x00" in observed_storage_class
                )
            )
            or getattr(spec, "volume_mode", None) not in {None, "Filesystem"}
            or getattr(spec, "selector", None) is not None
            or getattr(spec, "data_source", None) is not None
            or getattr(spec, "data_source_ref", None) is not None
        ):
            raise WorkspaceRuntimeAuthorityError("workspace PVC spec changed")
        try:
            return _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace PVC UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    async def _delete_pvc_outcome(
        self,
        pvc_name: str,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_uid: str | None = None,
    ) -> SharedResourceDeletionOutcome:
        """Delete an exact PVC without equating replacement with absence."""
        if not self._k8s_available:
            return _SHARED_RESOURCE_REFUSED

        claim_uid: str | None = None
        if expected_owner is not None:
            try:
                claim = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
                claim_uid = self._require_stateless_pvc_identity(
                    claim,
                    owner=expected_owner,
                    pvc_name=pvc_name,
                )
                if expected_uid is not None and claim_uid != expected_uid:
                    logger.info(
                        "Captured workspace PVC %s is already gone; refusing "
                        "same-name replacement UID %s",
                        expected_uid,
                        claim_uid,
                    )
                    return _SHARED_RESOURCE_REPLACED
            except Exception as error:
                if getattr(error, "status", None) == 404:
                    return _SHARED_RESOURCE_ABSENT
                logger.error(
                    "Refusing stateless PVC cleanup for %s %s: %s",
                    expected_owner.kind,
                    expected_owner.id,
                    error,
                )
                return _SHARED_RESOURCE_REFUSED

        try:
            await self._bounded_kubernetes_mutation(
                self._core_api.delete_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=self._namespace,
                **(
                    {"body": {"preconditions": {"uid": claim_uid}}}
                    if claim_uid is not None
                    else {}
                ),
            )
            if expected_owner is not None:
                try:
                    current_claim = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_persistent_volume_claim,
                        name=pvc_name,
                        namespace=self._namespace,
                    )
                except Exception as error:
                    if getattr(error, "status", None) == 404:
                        logger.info("PVC deleted: %s", pvc_name)
                        return _SHARED_RESOURCE_ABSENT
                else:
                    try:
                        current_uid = self._require_stateless_pvc_identity(
                            current_claim,
                            owner=expected_owner,
                            pvc_name=pvc_name,
                        )
                    except Exception:
                        current_uid = None
                    if expected_uid is not None and current_uid != expected_uid:
                        return _SHARED_RESOURCE_REPLACED
                return _SHARED_RESOURCE_REFUSED
            logger.info("PVC deleted: %s", pvc_name)
            return _SHARED_RESOURCE_ABSENT
        except Exception as e:
            if getattr(e, "status", None) == 404:
                logger.debug("PVC already deleted: %s", pvc_name)
                return _SHARED_RESOURCE_ABSENT
            if getattr(e, "status", None) == 409 and expected_uid is not None:
                return _SHARED_RESOURCE_REPLACED
            logger.error("Failed to delete PVC %s: %s", pvc_name, e)
            return _SHARED_RESOURCE_REFUSED

    async def _delete_pvc(
        self,
        pvc_name: str,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_uid: str | None = None,
    ) -> bool:
        """Compatibility wrapper; callers needing settlement inspect outcome."""

        outcome = await self._delete_pvc_outcome(
            pvc_name,
            expected_owner=expected_owner,
            expected_uid=expected_uid,
        )
        return outcome.state in {"captured_absent", "replacement_present"}

    async def _delete_pvc_and_wait(
        self,
        pvc_name: str,
        timeout: int = 90,
        *,
        expected_owner: WorkspaceOwner | None = None,
    ) -> bool:
        """Delete a PVC and wait (bounded) for it to fully release.

        The single-replica fallback recreates a fresh PVC under the SAME
        deterministic name, so the old (wedged) one must be gone first — else the
        create 409-reuses the dead volume. Best-effort: on timeout we proceed
        anyway (the recreate's 409 path is still safe, just not fresh).
        """
        if not self._k8s_available:
            return False
        if not await self._delete_pvc(pvc_name, expected_owner=expected_owner):
            return False
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
            except Exception as e:
                if hasattr(e, "status") and e.status == 404:
                    return True  # fully released
            await asyncio.sleep(2)
        logger.warning(
            "PVC %s still present after %ss — refusing fresh recovery",
            pvc_name,
            timeout,
        )
        return False

    async def _pod_volume_attach_failing(self, pod_name: str) -> bool:
        """True if the pod is wedged on a PVC volume that won't attach/mount.

        The single-replica node-loss fallback uses this to CONFIRM — before
        discarding a PVC (the only data-destructive recovery path) — that the
        holdup really is the volume (its lone replica on a dead node), not an
        unrelated stall (image pull, resource pressure, scheduling). Substrate-
        generic: matches the Kubernetes attach/mount failure events rather than
        any Longhorn-specific state, so it also covers Ceph / EBS / etc.
        """
        if not self._k8s_available:
            return False
        try:
            events = await self._bounded_kubernetes_call(
                self._core_api.list_namespaced_event,
                namespace=self._namespace,
                field_selector=f"involvedObject.name={pod_name}",
            )
        except Exception:
            logger.exception("Could not read events for pod %s", pod_name)
            return False
        for ev in getattr(events, "items", None) or []:
            reason = getattr(ev, "reason", "") or ""
            message = (getattr(ev, "message", "") or "").lower()
            if (
                reason in ("FailedAttachVolume", "FailedMount")
                or "multi-attach" in message
            ):
                return True
        return False

    async def _create_service(
        self,
        owner: WorkspaceOwner,
        *,
        require_exact_owner: bool = False,
        expected_provision_attempt: str | None = None,
        expected_runtime_generation: str | None = None,
        retained_uid: str | None = None,
        creation_reservation_id: str | None = None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> bool:
        """Create a headless Service giving the workspace a STABLE DNS name.

        The pod IP changes on every recreate; the agent caches its SSH dial
        target, so a recreated pod (PVC reattach / crash recovery) leaves the
        agent dialing a dead IP and the work churns to fail-loud (see
        knowledge-base/knowledge/issues/workspace_reattach_ephemeral_ip_reconnect_churn.md). A
        headless Service named after the pod gives a stable address
        ``<pod_name>.<ns>.svc:30022`` that always resolves (selector-matched) to
        the *current* pod — so reattach/recovery reconnects with no IP
        propagation. Headless (clusterIP=None) keeps traffic pod->pod (no DNAT),
        so workspace NetworkPolicies are unaffected, and DNS only resolves to a
        Ready pod (closes the sshd-readiness gap). Idempotent — 409 = success.
        Created with the PVC (both owner kinds) and kept across idle reaps;
        dropped on release/terminal. Cheap to lose — the next create_workspace
        recreates it — which is why ``release_workspace`` may drop the Service
        while KEEPING a still-resumable owner's PVC.
        """
        if not self._k8s_available:
            return False
        if (
            (expected_provision_attempt is None)
            != (expected_runtime_generation is None)
            or retained_uid is not None
            and expected_provision_attempt is None
            or creation_reservation_id is not None
            and expected_provision_attempt is not None
        ):
            return False
        svc_name = owner.pod_name  # DNS: <svc_name>.<ns>.svc.cluster.local
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": svc_name,
                "namespace": self._namespace,
                "labels": {
                    "app": "srw-workspace",
                    "srw/component": "workspace-svc",
                    "srw.io/component": "agent-workspace",
                    owner.label_key: owner.id,
                    **(
                        {
                            WORKSPACE_PROVISION_ATTEMPT_LABEL: (
                                expected_provision_attempt
                            ),
                            WORKSPACE_PROVISION_GENERATION_LABEL: (
                                expected_runtime_generation
                            ),
                        }
                        if expected_provision_attempt is not None
                        else {}
                    ),
                },
                **(
                    {
                        "annotations": {
                            WORKSPACE_CREATION_RESERVATION_ANNOTATION: str(
                                UUID(creation_reservation_id)
                            )
                        }
                    }
                    if creation_reservation_id is not None
                    else {}
                ),
            },
            "spec": {
                "clusterIP": "None",  # headless → A-record to the current pod IP
                "selector": {"app": "srw-workspace", owner.label_key: owner.id},
                "ports": [
                    {
                        "name": "ssh",
                        "port": 30022,
                        "targetPort": 30022,
                        "protocol": "TCP",
                    },
                    {
                        "name": "code-server",
                        "port": 38080,
                        "targetPort": 38080,
                        "protocol": "TCP",
                    },
                    {
                        "name": "cdp",
                        "port": 9222,
                        "targetPort": 9222,
                        "protocol": "TCP",
                    },
                ],
            },
        }
        try:
            if mutation_authority is not None and not await mutation_authority():
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation authority expired before Service create"
                )
            created = await self._bounded_kubernetes_mutation(
                self._core_api.create_namespaced_service,
                namespace=self._namespace,
                body=manifest,
            )
            if require_exact_owner:
                try:
                    service = created or await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_service,
                        name=svc_name,
                        namespace=self._namespace,
                    )
                    if expected_provision_attempt is not None:
                        self._require_pinned_workspace_resource_identity(
                            service,
                            resource="service",
                            owner=owner,
                            expected_name=svc_name,
                            expected_runtime_generation=expected_runtime_generation,
                            expected_attempt_id=expected_provision_attempt,
                            retained_uid=retained_uid,
                        )
                    else:
                        self._require_stateless_service_identity(service, owner=owner)
                        if created is not None and creation_reservation_id is not None:
                            self._require_workspace_creation_reservation_annotation(
                                service,
                                reservation_id=creation_reservation_id,
                            )
                except Exception as authority_error:
                    logger.error(
                        "Stateless workspace Service authority failed for %s: %s",
                        owner.id,
                        authority_error,
                    )
                    return False
            logger.info("Workspace Service created: %s", svc_name)
            return True
        except Exception as e:
            if hasattr(e, "status") and e.status == 409:
                if require_exact_owner:
                    try:
                        service = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_service,
                            name=svc_name,
                            namespace=self._namespace,
                        )
                        if expected_provision_attempt is not None:
                            self._require_pinned_workspace_resource_identity(
                                service,
                                resource="service",
                                owner=owner,
                                expected_name=svc_name,
                                expected_runtime_generation=(
                                    expected_runtime_generation
                                ),
                                expected_attempt_id=expected_provision_attempt,
                                retained_uid=retained_uid,
                            )
                        else:
                            self._require_stateless_service_identity(
                                service, owner=owner
                            )
                            if (
                                mutation_authority is not None
                                and not await mutation_authority()
                            ):
                                raise WorkspaceRuntimeAuthorityError(
                                    "workspace creation authority expired before Service reuse"
                                )
                    except Exception as authority_error:
                        logger.error(
                            "Refusing stateless Service reuse for %s: %s",
                            owner.id,
                            authority_error,
                        )
                        return False
                logger.debug("Workspace Service already exists: %s", svc_name)
                return True
            logger.error("Failed to create workspace Service %s: %s", svc_name, e)
            return False

    async def _delete_service_outcome(
        self,
        owner: WorkspaceOwner,
        *,
        require_exact_owner: bool = False,
        expected_uid: str | None = None,
    ) -> SharedResourceDeletionOutcome:
        """Delete an exact Service without treating replacement as absence."""
        if not self._k8s_available:
            return _SHARED_RESOURCE_REFUSED
        svc_name = owner.pod_name
        service_uid: str | None = None
        if require_exact_owner:
            try:
                service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=svc_name,
                    namespace=self._namespace,
                )
                service_uid = self._require_stateless_service_identity(
                    service, owner=owner
                )
                if expected_uid is not None and service_uid != expected_uid:
                    logger.info(
                        "Captured workspace Service %s is already gone; refusing "
                        "same-name replacement UID %s",
                        expected_uid,
                        service_uid,
                    )
                    return _SHARED_RESOURCE_REPLACED
            except Exception as e:
                if getattr(e, "status", None) == 404:
                    return _SHARED_RESOURCE_ABSENT
                logger.error(
                    "Refusing stateless Service cleanup for %s: %s", owner.id, e
                )
                return _SHARED_RESOURCE_REFUSED
        try:
            await self._bounded_kubernetes_mutation(
                self._core_api.delete_namespaced_service,
                name=svc_name,
                namespace=self._namespace,
                **(
                    {"body": {"preconditions": {"uid": service_uid}}}
                    if service_uid is not None
                    else {}
                ),
            )
            if require_exact_owner:
                try:
                    current_service = await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_service,
                        name=svc_name,
                        namespace=self._namespace,
                    )
                except Exception as e:
                    if getattr(e, "status", None) == 404:
                        logger.info("Workspace Service deleted: %s", svc_name)
                        return _SHARED_RESOURCE_ABSENT
                else:
                    try:
                        current_uid = self._require_stateless_service_identity(
                            current_service, owner=owner
                        )
                    except Exception:
                        current_uid = None
                    if expected_uid is not None and current_uid != expected_uid:
                        return _SHARED_RESOURCE_REPLACED
                return _SHARED_RESOURCE_REFUSED
            logger.info("Workspace Service deleted: %s", svc_name)
            return _SHARED_RESOURCE_ABSENT
        except Exception as e:
            if getattr(e, "status", None) == 404:
                logger.debug("Workspace Service already deleted: %s", svc_name)
                return _SHARED_RESOURCE_ABSENT
            if getattr(e, "status", None) == 409 and expected_uid is not None:
                return _SHARED_RESOURCE_REPLACED
            logger.error("Failed to delete workspace Service %s: %s", svc_name, e)
            return _SHARED_RESOURCE_REFUSED

    async def _delete_service(
        self,
        owner: WorkspaceOwner,
        *,
        require_exact_owner: bool = False,
        expected_uid: str | None = None,
    ) -> bool:
        """Compatibility wrapper; authoritative settlement uses typed outcome."""

        outcome = await self._delete_service_outcome(
            owner,
            require_exact_owner=require_exact_owner,
            expected_uid=expected_uid,
        )
        return outcome.state in {"captured_absent", "replacement_present"}

    def _require_stateless_service_identity(
        self,
        service: Any,
        *,
        owner: WorkspaceOwner,
    ) -> str:
        metadata = getattr(service, "metadata", None)
        labels = getattr(metadata, "labels", None)
        opposite_owner_label = (
            "srw/job-id" if owner.kind == "session" else "srw/thread-id"
        )
        if (
            str(getattr(metadata, "name", "") or "") != owner.pod_name
            or str(getattr(metadata, "namespace", "") or "") != self._namespace
            or getattr(metadata, "deletion_timestamp", None) is not None
            or not isinstance(labels, dict)
            or labels.get(owner.label_key) != owner.id
            or labels.get("app") != "srw-workspace"
            or labels.get("srw/component") != "workspace-svc"
            or labels.get("srw.io/component") != "agent-workspace"
            or WORKSPACE_PROVISION_FENCE_LABEL in labels
            or opposite_owner_label in labels
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace Service owner authority changed"
            )
        spec = getattr(service, "spec", None)
        selector = getattr(spec, "selector", None)
        ports = getattr(spec, "ports", None)
        if not isinstance(ports, (list, tuple)):
            raise WorkspaceRuntimeAuthorityError("workspace Service spec changed")
        observed_ports = {
            (
                str(getattr(port, "name", "") or ""),
                getattr(port, "port", None),
                getattr(port, "target_port", None),
                getattr(port, "protocol", None),
            )
            for port in ports
        }
        expected_ports = {
            ("ssh", 30022, 30022, "TCP"),
            ("code-server", 38080, 38080, "TCP"),
            ("cdp", 9222, 9222, "TCP"),
        }
        if (
            getattr(spec, "cluster_ip", None) != "None"
            or selector != {"app": "srw-workspace", owner.label_key: owner.id}
            or observed_ports != expected_ports
            or getattr(spec, "type", None) not in {None, "ClusterIP"}
        ):
            raise WorkspaceRuntimeAuthorityError("workspace Service spec changed")
        try:
            return _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace Service UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    def _require_pinned_workspace_resource_identity(
        self,
        resource_object: Any,
        *,
        resource: str,
        owner: WorkspaceOwner,
        expected_name: str,
        expected_runtime_generation: str,
        expected_attempt_id: str,
        retained_uid: str | None = None,
    ) -> str:
        """Prove one pinned create result belongs to its durable attempt.

        A retained PVC/Service predates the attempt and is admitted only by its
        already-captured immutable UID.  Every newly creatable object must carry
        the exact T/G/attempt labels written before the API call.
        """

        if resource == "pod":
            uid = self._require_workspace_pod_owner(
                resource_object,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=False,
                expected_pod_name=expected_name,
            )
        elif resource == "pvc":
            uid = self._require_stateless_pvc_identity(
                resource_object,
                owner=owner,
                pvc_name=expected_name,
            )
        elif resource == "seed_configmap":
            uid = self._require_stateless_seed_configmap_identity(
                resource_object,
                owner=owner,
                pod_name=owner.pod_name,
            )
        elif resource == "service":
            uid = self._require_stateless_service_identity(
                resource_object,
                owner=owner,
            )
        else:
            raise WorkspaceRuntimeAuthorityError(
                "workspace provision resource kind is invalid"
            )
        if (
            str(getattr(getattr(resource_object, "metadata", None), "name", "") or "")
            != expected_name
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace provision resource name changed"
            )
        if retained_uid is not None:
            if uid != str(retained_uid):
                raise WorkspaceRuntimeAuthorityError(
                    "retained workspace resource identity changed"
                )
            return uid
        labels = dict(
            getattr(getattr(resource_object, "metadata", None), "labels", None) or {}
        )
        if (
            labels.get(WORKSPACE_PROVISION_ATTEMPT_LABEL) != str(expected_attempt_id)
            or labels.get(WORKSPACE_PROVISION_GENERATION_LABEL)
            != str(expected_runtime_generation)
            or labels.get(owner.label_key) != owner.id
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace provision attempt identity changed"
            )
        return uid

    def _workspace_dns(self, owner: WorkspaceOwner) -> str:
        """Stable in-cluster DNS for the workspace's headless Service."""
        return f"{owner.pod_name}.{self._namespace}.svc.cluster.local"

    def _build_workspace_labels(
        self,
        owner: WorkspaceOwner,
        network_tier: str = DEFAULT_NETWORK_TIER,
        image: str | None = None,
    ) -> dict[str, str]:
        labels = {
            "app": "srw-workspace",
            owner.label_key: owner.id,
            "srw/component": owner.component_label,
            # Fleet-wide selector shared with KubeVirt VM workspaces.
            # See knowledge-base/knowledge/features/workspace_network_policy_unification.md
            "srw.io/component": "agent-workspace",
            # Per-project egress tier — selected by one NetworkPolicy per
            # tier in helm. See knowledge-base/knowledge/features/workspace_network_isolation.md §3.
            "srw.io/network-tier": network_tier,
        }
        # Phase 2a: build SHA label parity with agent pods. Lets the
        # lifecycle reconciler enumerate stale workspaces by selector
        # without joining to the jobs table. Only the installation image
        # carries it: a template's own image is not replaced on rollouts.
        if ":sha-" in self._workspace_image and image in (None, self._workspace_image):
            labels["srw/build-sha"] = self._workspace_image.rsplit(":sha-", 1)[-1]
        return labels

    def _pinned_workspace_provision_fingerprint(
        self,
        *,
        owner: WorkspaceOwner,
        pod_name: str,
        pvc_name: str | None,
        seed_configmap_name: str | None,
        service_name: str | None,
        network_tier: str,
        workspace_image: str,
        cpu: str,
        memory: str,
        cpu_limit: str,
        memory_limit: str,
        seed_files: Mapping[str, Any],
        seed_extensions: Mapping[str, Any],
        seed_needs_state: bool,
        profile: SandboxPodProfile | None = None,
    ) -> str:
        """Digest the complete deterministic pinned create rendering contract.

        The intent stores only this digest, so a retry may replay an unresolved
        attempt only while every manifest-affecting input is still exact.  A
        deployment/config drift returns no admission from the DB and leaves the
        old attempt for the retirement/fence reconciler; it must never silently
        render a different object under the old attempt identity.
        """

        contract = {
            "render_contract_version": (
                PINNED_WORKSPACE_PROVISION_RENDER_CONTRACT_VERSION
            ),
            "owner_kind": owner.kind,
            "owner_id": owner.id,
            "namespace": self._namespace,
            "pod_name": pod_name,
            "pvc_name": pvc_name,
            "seed_configmap_name": seed_configmap_name,
            "service_name": service_name,
            "network_tier": network_tier,
            "workspace_image": workspace_image,
            # _build_workspace_labels derives srw/build-sha from the configured
            # default image, and only emits it when the pod's actual image
            # matches that default — a template's own custom image never
            # carries the label. Track the default image here regardless, so
            # a same-namespace image rollout still invalidates a stale
            # pinned digest.
            "workspace_label_image": self._workspace_image,
            "cpu": cpu,
            "memory": memory,
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "pvc_size": (
                ((profile.storage if profile else None) or self._pvc_size)
                if pvc_name is not None
                else None
            ),
            "storage_class": self._storage_class if pvc_name is not None else None,
            "fuse_enabled": profile.fuse_enabled if profile else self._fuse_enabled,
            "fuse_privileged": (
                profile.fuse_privileged if profile else self._fuse_privileged
            ),
            "workspace_capabilities": self._workspace_capabilities(
                profile.fuse_enabled if profile else None
            ),
            "ssh_secret_name": self._ssh_secret_name,
            "seed_files": seed_files,
            "seed_extensions": seed_extensions,
            "seed_needs_state": seed_needs_state,
            # These manifests are static today, but explicit versions make a
            # future field change review-visible even when its inputs do not.
            "pvc_manifest_contract": 1,
            "seed_configmap_manifest_contract": 1,
            "pod_manifest_contract": 1,
            "service_manifest_contract": 1,
        }
        extension = profile.plan_extension() if profile else None
        if extension is not None:
            contract["sandbox"] = extension
        return hashlib.sha256(
            json.dumps(
                contract,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    async def _resolve_network_tier(self, work_id: str, kind: str) -> str:
        """Resolve the egress tier for a job/thread, falling back to the default.

        DB-less environments (tests, local dev without a project linkage)
        and pre-migration deployments both surface as a ``None`` row and
        get the default tier. The helm chart always emits the default-tier
        policy, so an unlabeled pod is never reachability-broken — it just
        inherits the strictest policy.
        """
        if self._db is None:
            return DEFAULT_NETWORK_TIER
        try:
            tier = await self._db.get_workspace_network_tier(work_id, kind)
        except Exception:
            logger.exception(
                "Failed to resolve network_tier for %s=%s; using default",
                kind,
                work_id,
            )
            return DEFAULT_NETWORK_TIER
        return tier or DEFAULT_NETWORK_TIER

    # =========================================================================
    # code-server settings seeding
    # =========================================================================

    async def _resolve_ide_seed_files(self, owner: WorkspaceOwner) -> dict:
        """Fetch the owner-user's stored code-server config. ``{}`` on any miss."""
        if not self._db:
            return {}
        try:
            if owner.kind == "job":
                row = await self._db.get_job(owner.id)
            else:
                row = await self._db.get_thread(owner.id)
            if not isinstance(row, dict):
                return {}
            user_id = row.get("user_id")
            if not user_id:
                return {}
            from orchestrator.services.ide_settings import IdeSettingsStore

            return await IdeSettingsStore(self._db).get_ide_files(str(user_id))
        except Exception as e:
            logger.warning(
                "ide seed: failed to resolve config for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return {}

    async def _resolve_ide_extensions(self, owner: WorkspaceOwner) -> dict:
        """Fetch the owner-user's stored extension manifest items. ``{}`` on miss."""
        if not self._db:
            return {}
        try:
            if owner.kind == "job":
                row = await self._db.get_job(owner.id)
            else:
                row = await self._db.get_thread(owner.id)
            if not isinstance(row, dict):
                return {}
            user_id = row.get("user_id")
            if not user_id:
                return {}
            from orchestrator.services.ide_settings import IdeSettingsStore

            return await IdeSettingsStore(self._db).get_extensions(str(user_id))
        except Exception as e:
            logger.warning(
                "ide seed: failed to resolve extensions for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return {}

    async def _owner_user_id(self, owner: WorkspaceOwner) -> Optional[str]:
        """Resolve the owning user_id for a job/thread workspace. None on miss."""
        if not self._db:
            return None
        try:
            if owner.kind == "job":
                row = await self._db.get_job(owner.id)
            else:
                row = await self._db.get_thread(owner.id)
            user_id = row.get("user_id") if isinstance(row, dict) else None
            return str(user_id) if user_id else None
        except Exception as e:
            logger.warning(
                "ide seed: failed to resolve user for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return None

    async def _resolve_ide_needs_state(
        self, owner: WorkspaceOwner, extensions: dict
    ) -> bool:
        """True when the orchestrator should stream license/globalStorage state
        into this pod once Ready (and thus the entrypoint should wait on the
        sentinel). True if any extension needs byte-copy, or the user has a
        captured globalStorage signature (e.g. a paid theme's license)."""
        if not extensions:
            return False
        if any(v.get("source") == "bytes" for v in extensions.values()):
            return True
        user_id = await self._owner_user_id(owner)
        if not user_id:
            return False
        try:
            from orchestrator.services.ide_settings import IdeSettingsStore

            return bool(await IdeSettingsStore(self._db).get_ext_signature(user_id))
        except Exception as e:
            logger.warning(
                "ide seed: needs-state resolve failed for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
            return False

    async def _seed_workspace_state(
        self,
        owner: WorkspaceOwner,
        attestation: WorkspaceRuntimeAttestation,
        *,
        scope: Literal["workspace_container", "ide"],
        mutation_authority: Callable[[], Awaitable[bool]],
    ) -> bool:
        """Seed Phase-B state under one exact, freshly attested authority."""

        if not self._db or not attestation.pod_ip:
            return False

        async def current_mutation_target() -> tuple[str, int, str] | None:
            try:
                current = (
                    await self.attest_ide_runtime(
                        owner.id,
                        expected_runtime_incarnation=(attestation.runtime_incarnation),
                    )
                    if scope == "ide"
                    else await self.attest_workspace_runtime(owner)
                )
            except Exception:
                return None
            if current != attestation or not await mutation_authority():
                return None
            return (
                current.pod_ip,
                current.port,
                current.ssh_host_key_fingerprint,
            )

        snap = self._snapshot_service
        if not snap or not snap.is_available:
            return True
        user_id = await self._owner_user_id(owner)
        if not user_id:
            return True
        try:
            from orchestrator.services.ide_profile_store import IdeProfileStore
            from orchestrator.services.ide_settings import (
                IdeSettingsStore,
                seed_ide_profile,
            )

            settings_store = IdeSettingsStore(self._db)
            items = await settings_store.get_extensions(user_id)
            profile_pointers = await settings_store.get_profile_pointers(user_id)
            profile = IdeProfileStore(snap._s3, snap._bucket)
            await seed_ide_profile(
                user_id=user_id,
                ssh_host=attestation.pod_ip,
                ssh_port=attestation.port,
                profile_store=profile,
                ext_items=items,
                profile_pointers=profile_pointers,
                expected_host_key_fingerprint=(attestation.ssh_host_key_fingerprint),
                mutation_authority=current_mutation_target,
            )
        except Exception as e:
            logger.warning(
                "ide seed: stream state failed for %s %s: %s",
                owner.kind,
                owner.id,
                e,
            )
        # Profile bytes are best-effort, but authority is mandatory even when
        # no optional payload existed or its transfer failed.
        return await current_mutation_target() is not None

    def _seed_configmap_name(self, pod_name: str) -> str:
        return f"code-server-config-{pod_name}"

    async def _create_seed_configmap(
        self,
        pod_name: str,
        files: dict,
        extensions: Optional[dict] = None,
        needs_state: bool = False,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_creation_generation: str | None = None,
        expected_provision_attempt: str | None = None,
        expected_runtime_generation: str | None = None,
        creation_reservation_id: str | None = None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> Optional[str]:
        """Create a ConfigMap carrying a self-contained ``seed.sh`` for the pod.

        ``seed.sh`` writes the user's config files and installs their Open-VSX
        extensions (theme-first). When ``needs_state`` is set, an ``expect-state``
        marker is added so the entrypoint waits (bounded) for the orchestrator to
        stream license/globalStorage state in (Phase B). Returns the ConfigMap
        name, or None when there is nothing to seed (or on failure — seeding is
        best-effort and must never block provisioning).
        """
        if (not files and not extensions) or not self._core_api:
            return None
        if expected_owner is None and expected_creation_generation is not None:
            raise WorkspaceRuntimeAuthorityError(
                "stateless seed ConfigMap authority is incomplete"
            )
        if expected_owner is None and creation_reservation_id is not None:
            raise WorkspaceRuntimeAuthorityError(
                "workspace seed ConfigMap reservation authority is incomplete"
            )
        if creation_reservation_id is not None:
            try:
                creation_reservation_id = str(UUID(str(creation_reservation_id)))
            except (TypeError, ValueError) as exc:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation reservation is malformed"
                ) from exc
        if (
            (expected_provision_attempt is None)
            != (expected_runtime_generation is None)
            or expected_provision_attempt is not None
            and expected_owner is None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "pinned seed ConfigMap authority is incomplete"
            )
        if expected_creation_generation is not None:
            try:
                expected_creation_generation = _canonical_runtime_uuid(
                    expected_creation_generation,
                    label="stateless workspace creation generation",
                )
            except ValueError as exc:
                raise WorkspaceRuntimeAuthorityError(str(exc)) from exc
        from orchestrator.services.ide_settings import (
            build_extension_install_script,
            build_seed_script,
        )

        cm_name = self._seed_configmap_name(pod_name)
        seed_sh = (
            build_seed_script(files)
            + "\n"
            + build_extension_install_script(extensions or {})
        )
        data = {"seed.sh": seed_sh}
        if needs_state:
            data["expect-state"] = "1"
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": cm_name,
                "namespace": self._namespace,
                **(
                    {
                        "labels": {
                            "app": "srw-workspace",
                            "srw/component": "workspace-seed",
                            "srw.io/component": "agent-workspace",
                            expected_owner.label_key: expected_owner.id,
                            **(
                                {
                                    WORKSPACE_PROVISION_ATTEMPT_LABEL: (
                                        expected_provision_attempt
                                    ),
                                    WORKSPACE_PROVISION_GENERATION_LABEL: (
                                        expected_runtime_generation
                                    ),
                                }
                                if expected_provision_attempt is not None
                                else {}
                            ),
                        },
                        **(
                            {
                                "annotations": {
                                    **(
                                        {
                                            WORKSPACE_RUNTIME_CREATION_ANNOTATION: (
                                                expected_creation_generation
                                            )
                                        }
                                        if expected_creation_generation is not None
                                        else {}
                                    ),
                                    **(
                                        {
                                            WORKSPACE_CREATION_RESERVATION_ANNOTATION: (
                                                creation_reservation_id
                                            )
                                        }
                                        if creation_reservation_id is not None
                                        else {}
                                    ),
                                }
                            }
                            if expected_creation_generation is not None
                            or creation_reservation_id is not None
                            else {}
                        ),
                    }
                    if expected_owner is not None
                    else {}
                ),
            },
            "data": data,
        }
        try:
            if mutation_authority is not None and not await mutation_authority():
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation authority expired before seed create"
                )
            created = await self._bounded_kubernetes_mutation(
                self._core_api.create_namespaced_config_map,
                namespace=self._namespace,
                body=body,
            )
            if expected_owner is not None:
                observed = created or await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                )
                self._require_stateless_seed_configmap_identity(
                    observed,
                    owner=expected_owner,
                    generation=expected_creation_generation,
                    pod_name=pod_name,
                    creation_reservation_id=creation_reservation_id,
                )
                if expected_provision_attempt is not None:
                    self._require_pinned_workspace_resource_identity(
                        observed,
                        resource="seed_configmap",
                        owner=expected_owner,
                        expected_name=cm_name,
                        expected_runtime_generation=expected_runtime_generation,
                        expected_attempt_id=expected_provision_attempt,
                    )
            return cm_name
        except Exception as e:
            if getattr(e, "status", None) == 409:
                # Stale ConfigMap from a prior attempt — refresh its contents.
                try:
                    if expected_owner is not None:
                        existing = await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_config_map,
                            name=cm_name,
                            namespace=self._namespace,
                        )
                        try:
                            self._require_stateless_seed_configmap_identity(
                                existing,
                                owner=expected_owner,
                                generation=expected_creation_generation,
                                pod_name=pod_name,
                                creation_reservation_id=creation_reservation_id,
                            )
                        except WorkspaceRuntimeAuthorityError:
                            if expected_creation_generation is not None:
                                raise
                            await self._require_legacy_seed_configmap_migration(
                                existing,
                                owner=expected_owner,
                                pod_name=pod_name,
                            )
                        if expected_provision_attempt is not None:
                            self._require_pinned_workspace_resource_identity(
                                existing,
                                resource="seed_configmap",
                                owner=expected_owner,
                                expected_name=cm_name,
                                expected_runtime_generation=(
                                    expected_runtime_generation
                                ),
                                expected_attempt_id=expected_provision_attempt,
                            )
                        resource_version = str(
                            getattr(
                                getattr(existing, "metadata", None),
                                "resource_version",
                                "",
                            )
                            or ""
                        )
                        if not resource_version:
                            raise WorkspaceRuntimeAuthorityError(
                                "workspace seed ConfigMap resource version is missing"
                            )
                        body["metadata"]["resourceVersion"] = resource_version
                    if (
                        mutation_authority is not None
                        and not await mutation_authority()
                    ):
                        raise WorkspaceRuntimeAuthorityError(
                            "workspace creation authority expired before seed replace"
                        )
                    replaced = await self._bounded_kubernetes_mutation(
                        self._core_api.replace_namespaced_config_map,
                        name=cm_name,
                        namespace=self._namespace,
                        body=body,
                    )
                    if expected_owner is not None:
                        observed = replaced or await self._bounded_kubernetes_call(
                            self._core_api.read_namespaced_config_map,
                            name=cm_name,
                            namespace=self._namespace,
                        )
                        self._require_stateless_seed_configmap_identity(
                            observed,
                            owner=expected_owner,
                            generation=expected_creation_generation,
                            pod_name=pod_name,
                            creation_reservation_id=creation_reservation_id,
                        )
                        if expected_provision_attempt is not None:
                            self._require_pinned_workspace_resource_identity(
                                observed,
                                resource="seed_configmap",
                                owner=expected_owner,
                                expected_name=cm_name,
                                expected_runtime_generation=(
                                    expected_runtime_generation
                                ),
                                expected_attempt_id=expected_provision_attempt,
                            )
                    return cm_name
                except Exception as e2:
                    if expected_owner is not None:
                        raise WorkspaceRuntimeAuthorityError(
                            "workspace seed ConfigMap authority changed"
                        ) from e2
                    logger.warning(
                        "ide seed: replace configmap %s failed: %s", cm_name, e2
                    )
                    return None
            if isinstance(e, WorkspaceRuntimeAuthorityError):
                raise
            if (
                expected_creation_generation is not None
                or expected_provision_attempt is not None
                or creation_reservation_id is not None
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace seed ConfigMap create failed"
                ) from e
            logger.warning("ide seed: create configmap %s failed: %s", cm_name, e)
            return None

    async def _adopt_configmap(
        self,
        cm_name: Optional[str],
        pod_obj: Any,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_creation_generation: str | None = None,
        expected_provision_attempt: str | None = None,
        expected_runtime_generation: str | None = None,
        creation_reservation_id: str | None = None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> bool:
        """Set the pod as the ConfigMap's owner so K8s GCs it on teardown.

        Best-effort: a failure here just means we fall back to the explicit
        delete in ``delete_workspace``/``delete_ide_pod``.
        """
        if not cm_name:
            return True
        if (expected_provision_attempt is None) != (
            expected_runtime_generation is None
        ):
            return False
        if not self._core_api or pod_obj is None:
            return False
        try:
            uid = pod_obj.metadata.uid
            name = pod_obj.metadata.name
        except Exception:
            return False
        if not uid:
            return False
        configmap_uid: str | None = None
        resource_version: str | None = None
        if expected_owner is not None:
            try:
                pod_labels = getattr(getattr(pod_obj, "metadata", None), "labels", None)
                opposite_owner_label = (
                    "srw/job-id"
                    if expected_owner.kind == "session"
                    else "srw/thread-id"
                )
                if (
                    str(name or "")
                    not in {expected_owner.pod_name, f"ide-{expected_owner.id[:12]}"}
                    or not isinstance(pod_labels, dict)
                    or pod_labels.get(expected_owner.label_key) != expected_owner.id
                    or opposite_owner_label in pod_labels
                    or expected_provision_attempt is not None
                    and (
                        pod_labels.get(WORKSPACE_PROVISION_ATTEMPT_LABEL)
                        != expected_provision_attempt
                        or pod_labels.get(WORKSPACE_PROVISION_GENERATION_LABEL)
                        != expected_runtime_generation
                    )
                ):
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Pod owner changed before ConfigMap adoption"
                    )
                configmap = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                )
                try:
                    configmap_uid = self._require_stateless_seed_configmap_identity(
                        configmap,
                        owner=expected_owner,
                        generation=expected_creation_generation,
                        pod_name=str(name),
                        creation_reservation_id=creation_reservation_id,
                    )
                except WorkspaceRuntimeAuthorityError:
                    if (
                        expected_creation_generation is not None
                        or creation_reservation_id is not None
                    ):
                        raise
                    await self._require_legacy_seed_configmap_migration(
                        configmap,
                        owner=expected_owner,
                        pod_name=str(name),
                    )
                    configmap_uid = _canonical_runtime_uuid(
                        str(
                            getattr(
                                getattr(configmap, "metadata", None),
                                "uid",
                                "",
                            )
                            or ""
                        ),
                        label="workspace seed ConfigMap UID",
                    )
                if expected_provision_attempt is not None:
                    configmap_uid = self._require_pinned_workspace_resource_identity(
                        configmap,
                        resource="seed_configmap",
                        owner=expected_owner,
                        expected_name=cm_name,
                        expected_runtime_generation=(expected_runtime_generation),
                        expected_attempt_id=expected_provision_attempt,
                    )
                resource_version = str(
                    getattr(
                        getattr(configmap, "metadata", None),
                        "resource_version",
                        "",
                    )
                    or ""
                )
                if not resource_version:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace seed ConfigMap resource version is missing"
                    )
            except Exception as error:
                logger.error(
                    "Refusing workspace seed ConfigMap adoption for %s: %s",
                    expected_owner.id,
                    error,
                )
                return False
        patch = {
            "metadata": {
                **(
                    {"resourceVersion": resource_version}
                    if resource_version is not None
                    else {}
                ),
                **(
                    {
                        "labels": {
                            "app": "srw-workspace",
                            "srw/component": "workspace-seed",
                            "srw.io/component": "agent-workspace",
                            expected_owner.label_key: expected_owner.id,
                            **(
                                {
                                    WORKSPACE_PROVISION_ATTEMPT_LABEL: (
                                        expected_provision_attempt
                                    ),
                                    WORKSPACE_PROVISION_GENERATION_LABEL: (
                                        expected_runtime_generation
                                    ),
                                }
                                if expected_provision_attempt is not None
                                else {}
                            ),
                        }
                    }
                    if expected_owner is not None
                    else {}
                ),
                "ownerReferences": [
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": name,
                        "uid": uid,
                        "controller": True,
                        "blockOwnerDeletion": False,
                    }
                ],
            }
        }
        try:
            if mutation_authority is not None and not await mutation_authority():
                return False
            if mutation_authority is not None:
                await self._bounded_kubernetes_mutation(
                    self._core_api.patch_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                    body=patch,
                )
            else:
                await self._bounded_kubernetes_call(
                    self._core_api.patch_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                    body=patch,
                )
            if expected_owner is not None:
                confirmed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                )
                if (
                    self._require_stateless_seed_configmap_identity(
                        confirmed,
                        owner=expected_owner,
                        generation=expected_creation_generation,
                        pod_name=str(name),
                        creation_reservation_id=creation_reservation_id,
                    )
                    != configmap_uid
                ):
                    return False
                if expected_provision_attempt is not None:
                    if (
                        self._require_pinned_workspace_resource_identity(
                            confirmed,
                            resource="seed_configmap",
                            owner=expected_owner,
                            expected_name=cm_name,
                            expected_runtime_generation=(expected_runtime_generation),
                            expected_attempt_id=expected_provision_attempt,
                        )
                        != configmap_uid
                    ):
                        return False
                owner_references = getattr(
                    getattr(confirmed, "metadata", None),
                    "owner_references",
                    None,
                )
                if not isinstance(owner_references, (list, tuple)) or not any(
                    str(_resource_field(reference, "uid") or "") == str(uid)
                    and str(_resource_field(reference, "name") or "") == str(name)
                    and _resource_field(reference, "controller") is True
                    for reference in owner_references
                ):
                    return False
            return True
        except Exception as e:
            logger.debug("ide seed: adopt configmap %s failed: %s", cm_name, e)
            return False

    async def _delete_seed_configmap(
        self,
        pod_name: str,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_pod_uid: str | None = None,
        expected_creation_reservation_id: str | None = None,
        expected_configmap_uid: str | None | object = _UNSPECIFIED_RESOURCE_BINDING,
    ) -> bool:
        """Delete a pod's seed ConfigMap with an explicit residue result."""
        if not self._core_api:
            return False
        cm_name = self._seed_configmap_name(pod_name)
        configmap_uid: str | None = None
        if expected_owner is not None:
            try:
                configmap = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=cm_name,
                    namespace=self._namespace,
                )
                try:
                    configmap_uid = self._require_stateless_seed_configmap_identity(
                        configmap,
                        owner=expected_owner,
                        pod_name=pod_name,
                        creation_reservation_id=(expected_creation_reservation_id),
                    )
                    if expected_pod_uid is not None:
                        self._require_seed_configmap_pod_owner_reference(
                            configmap,
                            pod_name=pod_name,
                            runtime_incarnation=expected_pod_uid,
                        )
                except WorkspaceRuntimeAuthorityError:
                    if expected_pod_uid is None:
                        raise
                    configmap_uid = (
                        self._require_legacy_seed_configmap_delete_authority(
                            configmap,
                            pod_name=pod_name,
                            expected_pod_uid=expected_pod_uid,
                        )
                    )
                if expected_configmap_uid is None:
                    # A durable cleanup intent captured absence. Never adopt a
                    # same-name ConfigMap that appeared after that snapshot.
                    return False
                if expected_configmap_uid is not _UNSPECIFIED_RESOURCE_BINDING and (
                    configmap_uid != str(expected_configmap_uid)
                ):
                    return False
            except Exception as error:
                if getattr(error, "status", None) == 404:
                    return True
                logger.error(
                    "Refusing stateless seed ConfigMap cleanup for %s: %s",
                    expected_owner.id,
                    error,
                )
                return False
        try:
            await self._bounded_kubernetes_mutation(
                self._core_api.delete_namespaced_config_map,
                name=cm_name,
                namespace=self._namespace,
                **(
                    {"body": {"preconditions": {"uid": configmap_uid}}}
                    if configmap_uid is not None
                    else {}
                ),
            )
            if expected_owner is not None:
                try:
                    await self._bounded_kubernetes_call(
                        self._core_api.read_namespaced_config_map,
                        name=cm_name,
                        namespace=self._namespace,
                    )
                except Exception as error:
                    if getattr(error, "status", None) == 404:
                        return True
                return False
            return True
        except Exception as e:
            if getattr(e, "status", None) == 404:
                return True
            logger.debug("ide seed: delete configmap %s failed: %s", cm_name, e)
            return False

    def _require_stateless_seed_configmap_identity(
        self,
        configmap: Any,
        *,
        owner: WorkspaceOwner,
        generation: str | None = None,
        pod_name: str | None = None,
        creation_reservation_id: str | None = None,
    ) -> str:
        metadata = getattr(configmap, "metadata", None)
        labels = getattr(metadata, "labels", None)
        annotations = getattr(metadata, "annotations", None)
        opposite_owner_label = (
            "srw/job-id" if owner.kind == "session" else "srw/thread-id"
        )
        observed_generation: Any = (
            annotations.get(WORKSPACE_RUNTIME_CREATION_ANNOTATION)
            if isinstance(annotations, dict)
            else None
        )
        observed_reservation: Any = (
            annotations.get(WORKSPACE_CREATION_RESERVATION_ANNOTATION)
            if isinstance(annotations, dict)
            else None
        )
        if observed_generation is not None or generation is not None:
            try:
                observed_generation = _canonical_runtime_uuid(
                    observed_generation,
                    label="workspace seed ConfigMap creation generation",
                )
            except ValueError as exc:
                raise WorkspaceRuntimeAuthorityError(str(exc)) from exc
        if observed_reservation is not None or creation_reservation_id is not None:
            try:
                observed_reservation = (
                    str(UUID(str(observed_reservation)))
                    if observed_reservation is not None
                    else None
                )
                expected_reservation = (
                    str(UUID(str(creation_reservation_id)))
                    if creation_reservation_id is not None
                    else None
                )
            except (TypeError, ValueError) as exc:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace seed ConfigMap reservation is malformed"
                ) from exc
        else:
            expected_reservation = None
        if (
            str(getattr(metadata, "name", "") or "")
            != self._seed_configmap_name(pod_name or owner.pod_name)
            or str(getattr(metadata, "namespace", "") or "") != self._namespace
            or getattr(metadata, "deletion_timestamp", None) is not None
            or not isinstance(labels, dict)
            or labels.get(owner.label_key) != owner.id
            or labels.get("app") != "srw-workspace"
            or labels.get("srw/component") != "workspace-seed"
            or labels.get("srw.io/component") != "agent-workspace"
            or WORKSPACE_PROVISION_FENCE_LABEL in labels
            or opposite_owner_label in labels
            or (generation is not None and observed_generation != generation)
            or (
                creation_reservation_id is not None
                and observed_reservation != expected_reservation
            )
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace seed ConfigMap owner authority changed"
            )
        try:
            return _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace seed ConfigMap UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    async def _require_legacy_seed_configmap_migration(
        self,
        configmap: Any,
        *,
        owner: WorkspaceOwner,
        pod_name: str,
    ) -> None:
        """Prove the one supported unlabeled HEAD seed before relabeling it."""

        metadata = getattr(configmap, "metadata", None)
        labels = getattr(metadata, "labels", None)
        annotations = getattr(metadata, "annotations", None)
        if labels not in (None, {}) or annotations not in (None, {}):
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap is not migration-safe"
            )
        expected_pod_uid = self._legacy_seed_configmap_controller_uid(
            configmap,
            pod_name=pod_name,
        )
        pod = await self._bounded_kubernetes_call(
            self._core_api.read_namespaced_pod,
            name=pod_name,
            namespace=self._namespace,
        )
        component = "ide-session" if pod_name.startswith("ide-") else None
        observed_pod_uid = self._require_workspace_pod_owner(
            pod,
            owner=owner,
            allow_owner_unlabeled=False,
            expected_pod_name=pod_name,
            expected_component=component,
        )
        if observed_pod_uid != expected_pod_uid:
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap Pod UID changed"
            )
        if not str(getattr(metadata, "resource_version", "") or ""):
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap resource version is missing"
            )

    def _require_legacy_seed_configmap_delete_authority(
        self,
        configmap: Any,
        *,
        pod_name: str,
        expected_pod_uid: str,
    ) -> str:
        metadata = getattr(configmap, "metadata", None)
        labels = getattr(metadata, "labels", None)
        annotations = getattr(metadata, "annotations", None)
        if labels not in (None, {}) or annotations not in (None, {}):
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap is not deletion-safe"
            )
        if (
            self._legacy_seed_configmap_controller_uid(
                configmap,
                pod_name=pod_name,
            )
            != expected_pod_uid
        ):
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap Pod UID changed"
            )
        try:
            return _canonical_runtime_uuid(
                str(getattr(metadata, "uid", "") or ""),
                label="workspace seed ConfigMap UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    def _legacy_seed_configmap_controller_uid(
        self,
        configmap: Any,
        *,
        pod_name: str,
    ) -> str:
        metadata = getattr(configmap, "metadata", None)
        if (
            str(getattr(metadata, "name", "") or "")
            != self._seed_configmap_name(pod_name)
            or str(getattr(metadata, "namespace", "") or "") != self._namespace
            or getattr(metadata, "deletion_timestamp", None) is not None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap identity changed"
            )
        references = getattr(metadata, "owner_references", None)
        matching = [
            reference
            for reference in references or []
            if str(_resource_field(reference, "name") or "") == pod_name
            and _resource_field(reference, "controller") is True
        ]
        if len(matching) != 1:
            raise WorkspaceRuntimeAuthorityError(
                "legacy workspace seed ConfigMap owner reference is invalid"
            )
        try:
            return _canonical_runtime_uuid(
                str(_resource_field(matching[0], "uid") or ""),
                label="workspace seed ConfigMap Pod UID",
            )
        except ValueError as exc:
            raise WorkspaceRuntimeAuthorityError(str(exc)) from exc

    def _require_seed_configmap_pod_owner_reference(
        self,
        configmap: Any,
        *,
        pod_name: str,
        runtime_incarnation: str,
    ) -> None:
        references = getattr(
            getattr(configmap, "metadata", None),
            "owner_references",
            None,
        )
        if not isinstance(references, (list, tuple)) or not any(
            str(_resource_field(reference, "uid") or "") == runtime_incarnation
            and str(_resource_field(reference, "name") or "") == pod_name
            and _resource_field(reference, "controller") is True
            for reference in references
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace seed ConfigMap Pod ownership changed"
            )

    def _build_pod_manifest(
        self,
        pod_name: str,
        owner: WorkspaceOwner,
        image: str,
        cpu: str,
        memory: str,
        cpu_limit: str,
        memory_limit: str,
        network_tier: str = DEFAULT_NETWORK_TIER,
        pvc_name: Optional[str] = None,
        seed_configmap: Optional[str] = None,
        stateless_creation_generation: str | None = None,
        creation_reservation_id: str | None = None,
        pinned_runtime_generation: str | None = None,
        pinned_provision_attempt: str | None = None,
        profile: SandboxPodProfile | None = None,
    ) -> dict:
        """Build the Kubernetes Pod manifest for a workspace container.

        When ``seed_configmap`` is given, the named ConfigMap (carrying a
        ``seed.sh`` that writes the user's code-server config) is mounted at
        ``/mnt/code-server-config`` so the entrypoint can apply it before
        code-server starts.
        """
        code_server_credential = ide_credential(
            namespace=self._namespace,
            owner_kind=owner.kind,
            owner_id=owner.id,
            pod_name=pod_name,
        )
        fuse_enabled = self._fuse_enabled if profile is None else profile.fuse_enabled
        fuse_privileged = (
            self._fuse_privileged if profile is None else profile.fuse_privileged
        )
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": self._namespace,
                "labels": self._build_workspace_labels(
                    owner, network_tier, image=image
                ),
                # GC backstop hook: marks the pod as owned by the lifecycle
                # reconciler so a future K8s TTL/GC controller (or an age
                # sweep) can reclaim a tail the reconciler missed. Bare pods
                # have no ownerReference, so without this nothing external can
                # ever clean them up.
                "annotations": {
                    "srw.io/managed-by": "lifecycle-reconciler",
                    **(
                        {
                            WORKSPACE_RUNTIME_CREATION_ANNOTATION: (
                                stateless_creation_generation
                            )
                        }
                        if stateless_creation_generation is not None
                        else {}
                    ),
                    **(
                        {
                            WORKSPACE_CREATION_RESERVATION_ANNOTATION: (
                                creation_reservation_id
                            )
                        }
                        if creation_reservation_id is not None
                        else {}
                    ),
                },
                # Every credential-capable workspace Pod retains its exact API
                # object until this controller has observed every container
                # terminal and persisted process-zero for the immutable UID.
                # Stateless Pods were the first users of this finalizer; the
                # literal stays stable for rolling compatibility.
                "finalizers": [STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER],
            },
            "spec": {
                "restartPolicy": "Never",
                # Grace period for agent to checkpoint and push artifacts
                # before the pod is killed on deletion.
                "terminationGracePeriodSeconds": 120,
                # Explicit ClusterFirst DNS so workspace pods can resolve
                # in-cluster services (e.g. srw-gitea) even if
                # WORKSPACE_NAMESPACE differs from the service namespace.
                "dnsPolicy": "ClusterFirst",
                "dnsConfig": {
                    "searches": [
                        "superhuman-remote-worker.svc.cluster.local",
                    ],
                },
                # Pod-level security: run SSHD as root for user session
                # management (su to agent-host), but restrict everything else.
                "securityContext": {
                    "seccompProfile": {
                        "type": "Unconfined" if fuse_privileged else "RuntimeDefault"
                    },
                },
                "containers": [
                    {
                        "name": "workspace",
                        "image": image,
                        "ports": [
                            {"containerPort": 30022, "name": "ssh"},
                            {"containerPort": 38080, "name": "code-server"},
                            {"containerPort": 9222, "name": "cdp"},
                        ],
                        "env": [
                            {
                                "name": "SRW_WORKSPACE_OWNER_KIND",
                                "value": owner.kind,
                            },
                            {
                                "name": "SRW_WORKSPACE_OWNER_ID",
                                "value": owner.id,
                            },
                            {
                                "name": "SRW_WORKSPACE_RUNTIME_UID",
                                "valueFrom": {
                                    "fieldRef": {
                                        "fieldPath": "metadata.uid",
                                    }
                                },
                            },
                            # code-server's recipient binding. Absent, the
                            # entrypoint refuses to start code-server at all
                            # rather than serving an `auth: none` IDE that any
                            # caller reaching the Pod IP could read — including
                            # a proxy that dialled a reused address. See
                            # services/ide_credentials.py.
                            *(
                                [
                                    {
                                        "name": IDE_CREDENTIAL_ENV,
                                        "value": code_server_credential,
                                    }
                                ]
                                if code_server_credential
                                else []
                            ),
                        ],
                        "resources": {
                            "requests": {"cpu": cpu, "memory": memory},
                            "limits": {
                                "cpu": cpu_limit,
                                "memory": memory_limit,
                            },
                        },
                        # Container security profile:
                        # - Drop all capabilities, add back only what SSHD needs
                        #   plus SYS_ADMIN for rclone/FUSE mounts when enabled.
                        # - SETUID/SETGID: user session switching
                        # - NET_BIND_SERVICE: bind to privileged ports (<1024)
                        # - CHOWN/DAC_OVERRIDE/FOWNER: file ownership for sessions
                        # - SYS_CHROOT: SSHD privilege separation
                        # - SYS_ADMIN: required for container FUSE mounts
                        # - KILL: signal management
                        # - AUDIT_WRITE: PAM audit logging
                        # - allowPrivilegeEscalation: true (required for SSHD setuid)
                        # - sudo is NOT installed — agent-host cannot escalate
                        "securityContext": {
                            "capabilities": {
                                "drop": ["ALL"],
                                "add": self._workspace_capabilities(fuse_enabled),
                            },
                            "allowPrivilegeEscalation": True,
                        },
                        "volumeMounts": [
                            {
                                "name": "workspace-data",
                                "mountPath": "/home/agent-host",
                            },
                            {
                                "name": "ssh-pubkey",
                                "mountPath": "/tmp/ssh-pubkey",
                                "readOnly": True,
                            },
                            {
                                "name": "workspace-identity",
                                "mountPath": "/var/lib/srw-system",
                            },
                        ],
                        "readinessProbe": {
                            "tcpSocket": {"port": 30022},
                            "initialDelaySeconds": 3,
                            "periodSeconds": 5,
                        },
                        "livenessProbe": {
                            "tcpSocket": {"port": 30022},
                            "initialDelaySeconds": 10,
                            "periodSeconds": 30,
                        },
                    }
                ],
                "volumes": [
                    {
                        "name": "workspace-data",
                        "persistentVolumeClaim": {"claimName": pvc_name},
                    }
                    if pvc_name
                    else {
                        "name": "workspace-data",
                        "emptyDir": {"sizeLimit": "10Gi"},
                    },
                    {
                        "name": "ssh-pubkey",
                        "secret": {
                            "secretName": self._ssh_secret_name,
                            "items": [
                                {
                                    "key": "ssh-publickey",
                                    "path": "ssh-publickey",
                                    "mode": 0o600,
                                },
                                {
                                    "key": "user-ca.pub",
                                    "path": "user-ca.pub",
                                    "mode": 0o644,
                                },
                            ],
                            # A deployment whose ssh-gateway secret predates
                            # the CA key (or has no ssh-gateway at all) must
                            # still start: the projected CA key is additive,
                            # not required. The entrypoint already treats
                            # its absence as "certificate auth unavailable",
                            # not fatal.
                            # Kubernetes has no per-item "optional": this
                            # also relaxes the pre-existing "ssh-publickey"
                            # item, which predates this feature and used to
                            # be required. A genuinely missing/deleted
                            # Secret (not just a missing key within it) no
                            # longer produces a named kubelet
                            # CreateContainerConfigError; the pod instead
                            # starts with an empty ssh-pubkey volume and
                            # whatever consumes it (sshd's AuthorizedKeysFile)
                            # times out ssh-auth-readiness after ~30s
                            # instead. If you're debugging one of those
                            # timeouts, check whether the secret itself
                            # still exists before looking anywhere else.
                            "optional": True,
                            "defaultMode": 0o600,
                        },
                    },
                    {
                        "name": "workspace-identity",
                        "emptyDir": {},
                    },
                ],
            },
        }
        if pinned_provision_attempt is not None:
            if pinned_runtime_generation is None:
                raise WorkspaceRuntimeAuthorityError(
                    "pinned workspace Pod authority is incomplete"
                )
            manifest["metadata"]["labels"].update(
                {
                    WORKSPACE_PROVISION_ATTEMPT_LABEL: pinned_provision_attempt,
                    WORKSPACE_PROVISION_GENERATION_LABEL: pinned_runtime_generation,
                }
            )
        if fuse_enabled:
            if fuse_privileged:
                manifest["spec"]["containers"][0]["securityContext"]["privileged"] = (
                    True
                )
            manifest["spec"]["containers"][0]["volumeMounts"].append(
                {
                    "name": "dev-fuse",
                    "mountPath": "/dev/fuse",
                }
            )
            manifest["spec"]["volumes"].append(
                {
                    "name": "dev-fuse",
                    "hostPath": {"path": "/dev/fuse", "type": "CharDevice"},
                }
            )
        if seed_configmap:
            manifest["spec"]["containers"][0]["volumeMounts"].append(
                {
                    "name": "code-server-config",
                    "mountPath": "/mnt/code-server-config",
                    "readOnly": True,
                }
            )
            manifest["spec"]["volumes"].append(
                {
                    "name": "code-server-config",
                    "configMap": {"name": seed_configmap, "defaultMode": 0o644},
                }
            )
        workspace_container = manifest["spec"]["containers"][0]
        if profile is not None and profile.pull_policy:
            workspace_container["imagePullPolicy"] = profile.pull_policy
        if profile is not None and profile.storage and not pvc_name:
            manifest["spec"]["volumes"][0]["emptyDir"]["sizeLimit"] = profile.storage
            workspace_container["resources"]["requests"]["ephemeral-storage"] = (
                profile.storage
            )
        return manifest

    def _workspace_capabilities(self, fuse_enabled: bool | None = None) -> list[str]:
        capabilities = [
            "CHOWN",
            "DAC_OVERRIDE",
            "FOWNER",
            "SETGID",
            "SETUID",
            "NET_BIND_SERVICE",
            "SYS_CHROOT",
            "KILL",
            "AUDIT_WRITE",
        ]
        if self._fuse_enabled if fuse_enabled is None else fuse_enabled:
            capabilities.append("SYS_ADMIN")
        return capabilities

    async def _create_pod_resolving_teardown(
        self,
        pod_manifest: dict,
        pod_name: str,
        *,
        owner: WorkspaceOwner,
        expected_provision_attempt: str | None = None,
        expected_runtime_generation: str | None = None,
        mutation_authority: Callable[[], Awaitable[bool]] | None = None,
    ) -> tuple[Any, bool]:
        """Create the workspace pod, resolving a suspend/resume teardown race.

        Returns ``(pod, reused_existing)`` so the caller retains the incumbent
        UID and its actual seed-volume binding across a 409 retry.

        The race this fixes (issue
        ``session_agent_drift_drain_kills_idle_sessions``, option c): a
        just-suspended session leaves its old workspace pod — same
        deterministic name (``ws-thread-<tid>``) — ``Terminating`` inside its
        delete grace window. A fast resume (drift-drain → suspend → user
        sends a message seconds later) then 409s on create, which previously
        bubbled to the dispatcher as a failed restore and surfaced to the
        user as a 503 "session ended". We distinguish:

          * incumbent pod ``Terminating`` (``deletion_timestamp`` set, or
            already 404 by the time we look) → wait for it to fully
            disappear, then create the fresh pod;
          * incumbent pod live (no deletion timestamp) → genuine idempotent
            hit, adopt it (mirrors the IDE-pod path).
        """
        if (expected_provision_attempt is None) != (
            expected_runtime_generation is None
        ):
            raise WorkspaceRuntimeAuthorityError(
                "pinned workspace Pod authority is incomplete"
            )

        def _require_created_identity(pod: Any) -> str:
            if expected_provision_attempt is not None:
                return self._require_pinned_workspace_resource_identity(
                    pod,
                    resource="pod",
                    owner=owner,
                    expected_name=pod_name,
                    expected_runtime_generation=expected_runtime_generation,
                    expected_attempt_id=expected_provision_attempt,
                )
            return self._require_workspace_pod_owner(
                pod,
                owner=owner,
                allow_owner_unlabeled=False,
                expected_network_tier=(
                    pod_manifest.get("metadata", {})
                    .get("labels", {})
                    .get("srw.io/network-tier")
                ),
            )

        try:
            if mutation_authority is not None and not await mutation_authority():
                raise WorkspaceRuntimeAuthorityError(
                    "workspace creation authority expired before Pod create"
                )
            if mutation_authority is not None:
                created = await self._bounded_kubernetes_mutation(
                    self._core_api.create_namespaced_pod,
                    namespace=self._namespace,
                    body=pod_manifest,
                )
            else:
                created = await self._bounded_kubernetes_call(
                    self._core_api.create_namespaced_pod,
                    namespace=self._namespace,
                    body=pod_manifest,
                    _request_timeout=(
                        5,
                        PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS,
                    ),
                )
            if created is None:
                created = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
            _require_created_identity(created)
            return created, False
        except Exception as e:
            if getattr(e, "status", None) != 409:
                raise

        # 409 — inspect the incumbent pod sharing this deterministic name.
        try:
            existing = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
        except Exception as e:
            if getattr(e, "status", None) != 404:
                raise
            existing = None  # vanished between create and read — recreate below

        if existing is not None:
            self._require_workspace_pod_owner(
                existing,
                owner=owner,
                allow_owner_unlabeled=False,
                allow_terminating=True,
            )

        terminating = (
            existing is None or existing.metadata.deletion_timestamp is not None
        )
        if not terminating:
            _require_created_identity(existing)
            # Live pod already present (two creates raced for one owner) —
            # treat as idempotent; the existing pod owns its ConfigMap.
            logger.info(
                "Workspace pod %s already exists and is live — adopting", pod_name
            )
            return existing, True

        # Old pod still draining its delete grace (suspend→resume race).
        # Wait it out, then create the fresh pod on the freed name.
        logger.info(
            "Workspace pod %s is terminating (suspend/resume race) — waiting "
            "for teardown before recreate",
            pod_name,
        )
        if not await self._wait_for_pod_gone(pod_name, timeout=30):
            raise RuntimeError(
                f"workspace pod {pod_name} still terminating after 30s; "
                "cannot recreate for resume"
            )
        if mutation_authority is not None and not await mutation_authority():
            raise WorkspaceRuntimeAuthorityError(
                "workspace creation authority expired before Pod recreate"
            )
        if mutation_authority is not None:
            created = await self._bounded_kubernetes_mutation(
                self._core_api.create_namespaced_pod,
                namespace=self._namespace,
                body=pod_manifest,
            )
        else:
            created = await self._bounded_kubernetes_call(
                self._core_api.create_namespaced_pod,
                namespace=self._namespace,
                body=pod_manifest,
                _request_timeout=(
                    5,
                    PINNED_K8S_MUTATION_REQUEST_TIMEOUT_SECONDS,
                ),
            )
        if created is None:
            created = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
        _require_created_identity(created)
        return created, False

    async def _wait_for_pod_gone(self, pod_name: str, timeout: int = 30) -> bool:
        """Poll until the named pod no longer exists (404). True if gone."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
            except Exception as e:
                if getattr(e, "status", None) == 404:
                    return True
                # Transient read error — keep polling until the deadline.
            await asyncio.sleep(1)
        return False

    async def _wait_for_ready(
        self,
        pod_name: str,
        timeout: int = 120,
        *,
        expected_owner: WorkspaceOwner | None = None,
        expected_runtime_incarnation: str | None = None,
        expected_creation_generation: str | None = None,
        expected_network_tier: str | None = None,
        expected_pvc_name: str | None | object = _UNSPECIFIED_RESOURCE_BINDING,
        expected_seed_configmap: str | None | object = (_UNSPECIFIED_RESOURCE_BINDING),
        expected_pod_name: str | None = None,
        expected_component: str | None = None,
        pull_image: str | None = None,
        pull_started_at: datetime | None = None,
    ) -> Optional[str]:
        """Poll until the pod is ready and accepts the configured SSH key.

        Returns:
            Pod IP after authenticated readiness, None on Kubernetes timeout.

        Raises:
            WorkspaceSSHAuthenticationError: the pod is Kubernetes-ready but
                the configured private key is unusable or authentication does
                not succeed within its bounded readiness window.
            WorkspaceImagePullError: ``pull_image`` (a custom image) cannot be
                pulled: at once for an invalid reference, otherwise once the
                pull budget is spent. While it is still pulling, the wait
                extends up to that budget.
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        pull_remaining = self._image_pull_timeout
        if pull_started_at is not None:
            pull_remaining = max(
                0,
                pull_remaining
                - max(
                    0, (datetime.now(timezone.utc) - pull_started_at).total_seconds()
                ),
            )
        pull_deadline = loop.time() + pull_remaining if pull_image is not None else None
        pull_observation: Any = None

        while loop.time() < deadline:
            try:
                pod = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_pod,
                    name=pod_name,
                    namespace=self._namespace,
                )
                if expected_owner is not None:
                    if expected_runtime_incarnation is None:
                        raise WorkspaceRuntimeAuthorityError(
                            "workspace runtime incarnation is missing"
                        )
                    self._require_workspace_pod_connection_identity(
                        pod,
                        owner=expected_owner,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        expected_creation_generation=expected_creation_generation,
                        expected_network_tier=expected_network_tier,
                        expected_pvc_name=expected_pvc_name,
                        expected_seed_configmap=expected_seed_configmap,
                        expected_pod_name=expected_pod_name,
                        expected_component=expected_component,
                    )
                if pull_image is not None:
                    # Classified only after the identity fence above, so a pod
                    # that is not this runtime stays an authority error.
                    pull_observation = pod
                    verdict = classify_image_pull(
                        pod,
                        image=pull_image,
                        now=datetime.now(timezone.utc),
                        pull_timeout_seconds=self._image_pull_timeout,
                    )
                    if verdict.state == "failed":
                        raise WorkspaceImagePullError(verdict.message)
                    if verdict.state == "pulling" and pull_deadline is not None:
                        deadline = max(deadline, pull_deadline)
                if pod.status.phase == "Running" and pod.status.pod_ip:
                    # Check container readiness
                    if pod.status.container_statuses and all(
                        cs.ready for cs in pod.status.container_statuses
                    ):
                        key_path = resolve_ssh_key_path()
                        try:
                            fingerprint = workspace_private_key_fingerprint(key_path)
                        except SSHPrivateKeyError as exc:
                            raise WorkspaceSSHAuthenticationError(str(exc)) from exc

                        ready, attempts, last_error = await wait_for_agent_ssh(
                            pod.status.pod_ip,
                            30022,
                            deadline_s=self._ssh_auth_ready_timeout,
                            connect_timeout_s=self._ssh_auth_connect_timeout,
                            interval_s=self._ssh_auth_poll_interval,
                            key_path=key_path,
                        )
                        if ready:
                            if expected_owner is not None:
                                confirmed = await self._bounded_kubernetes_call(
                                    self._core_api.read_namespaced_pod,
                                    name=pod_name,
                                    namespace=self._namespace,
                                )
                                self._require_workspace_pod_connection_identity(
                                    confirmed,
                                    owner=expected_owner,
                                    expected_runtime_incarnation=(
                                        expected_runtime_incarnation
                                    ),
                                    expected_creation_generation=(
                                        expected_creation_generation
                                    ),
                                    expected_network_tier=expected_network_tier,
                                    expected_pvc_name=expected_pvc_name,
                                    expected_seed_configmap=(expected_seed_configmap),
                                    expected_pod_name=expected_pod_name,
                                    expected_component=expected_component,
                                )
                            logger.info(
                                "Workspace SSH authenticated: %s @ %s:30022 "
                                "(attempts=%d, key=%s)",
                                pod_name,
                                pod.status.pod_ip,
                                attempts,
                                fingerprint,
                            )
                            return pod.status.pod_ip
                        raise WorkspaceSSHAuthenticationError(
                            "Workspace pod became Kubernetes-ready but rejected "
                            f"the configured SSH key after {attempts} attempt(s) "
                            f"(key={fingerprint}): {last_error or 'authentication failed'}"
                        )
            except WorkspaceSSHAuthenticationError:
                raise
            except WorkspaceRuntimeAuthorityError:
                raise
            except WorkspaceImagePullError:
                raise
            except Exception:
                pass

            await asyncio.sleep(2)

        if (
            pull_image is not None
            and pull_deadline is not None
            and pull_observation is not None
            and loop.time() >= pull_deadline
        ):
            # The pull budget is spent, yet the pod's age (stamped by the API
            # server just before this wait began) can read a poll short of it.
            # Judge the last observation as out of budget so a pod stuck in
            # ImagePullBackOff fails instead of being reported "not ready yet".
            verdict = classify_image_pull(
                pull_observation,
                image=pull_image,
                now=datetime.now(timezone.utc),
                pull_timeout_seconds=0,
            )
            if verdict.state == "failed":
                raise WorkspaceImagePullError(verdict.message)
        return None

    async def _trusted_pod_ssh_identity(
        self,
        pod_name: str,
        *,
        pvc_name: str | None = None,
        expected_owner: WorkspaceOwner | None = None,
        expected_runtime_incarnation: str | None = None,
        expected_creation_generation: str | None = None,
        expected_network_tier: str | None = None,
        expected_seed_configmap: str | None | object = (_UNSPECIFIED_RESOURCE_BINDING),
        expected_pvc_uid: str | None = None,
        expected_pvc_storage_class: str | None = None,
        expected_provision_attempt: str | None = None,
        expected_runtime_generation: str | None = None,
        expected_seed_configmap_uid: str | None = None,
        expected_service_uid: str | None = None,
        expected_retained_pvc_uid: str | None = None,
        expected_retained_service_uid: str | None = None,
        expected_pod_name: str | None = None,
        expected_component: str | None = None,
    ) -> tuple[str, str, str]:
        """Read backing identity, host key, and Pod UID from the control plane."""

        if self._core_api is None or k8s_stream is None:
            raise RuntimeError("Kubernetes exec transport is unavailable")
        pinned_attempt = expected_provision_attempt is not None
        if pinned_attempt != (expected_runtime_generation is not None) or (
            not pinned_attempt
            and any(
                value is not None
                for value in (
                    expected_seed_configmap_uid,
                    expected_service_uid,
                    expected_retained_pvc_uid,
                    expected_retained_service_uid,
                )
            )
        ):
            raise WorkspaceRuntimeAuthorityError(
                "workspace provision attestation authority is incomplete"
            )
        if pinned_attempt:
            if expected_owner is None or expected_runtime_incarnation is None:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace provision attestation owner is incomplete"
                )
            if expected_seed_configmap is _UNSPECIFIED_RESOURCE_BINDING or not (
                expected_seed_configmap is None
                or isinstance(expected_seed_configmap, str)
                and bool(expected_seed_configmap)
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace provision ConfigMap authority is incomplete"
                )
            if (
                (pvc_name is None) != (expected_pvc_uid is None)
                or (pvc_name is None) != (expected_service_uid is None)
                or (expected_seed_configmap is None)
                != (expected_seed_configmap_uid is None)
                or pvc_name is None
                and (
                    expected_retained_pvc_uid is not None
                    or expected_retained_service_uid is not None
                )
            ):
                raise WorkspaceRuntimeAuthorityError(
                    "workspace provision resource UID authority is incomplete"
                )
            try:
                expected_provision_attempt = _canonical_runtime_uuid(
                    expected_provision_attempt,
                    label="workspace provision attempt",
                )
                expected_runtime_generation = _canonical_runtime_uuid(
                    expected_runtime_generation,
                    label="workspace provision runtime generation",
                )
                expected_runtime_incarnation = _canonical_runtime_uuid(
                    expected_runtime_incarnation,
                    label="workspace provision Pod UID",
                )
                for label, value in (
                    ("workspace provision PVC UID", expected_pvc_uid),
                    (
                        "workspace provision seed ConfigMap UID",
                        expected_seed_configmap_uid,
                    ),
                    ("workspace provision Service UID", expected_service_uid),
                    ("retained workspace PVC UID", expected_retained_pvc_uid),
                    (
                        "retained workspace Service UID",
                        expected_retained_service_uid,
                    ),
                ):
                    if value is not None:
                        _canonical_runtime_uuid(value, label=label)
            except ValueError as exc:
                raise WorkspaceRuntimeAuthorityError(str(exc)) from exc
        pod = await self._bounded_kubernetes_call(
            self._core_api.read_namespaced_pod,
            name=pod_name,
            namespace=self._namespace,
        )
        if expected_owner is not None:
            if expected_runtime_incarnation is None:
                raise WorkspaceRuntimeAuthorityError(
                    "workspace runtime incarnation is missing"
                )
            runtime_incarnation = self._require_workspace_pod_connection_identity(
                pod,
                owner=expected_owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                expected_creation_generation=expected_creation_generation,
                expected_network_tier=expected_network_tier,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=expected_seed_configmap,
                expected_pod_name=expected_pod_name,
                expected_component=expected_component,
            )
            if pinned_attempt and (
                self._require_pinned_workspace_resource_identity(
                    pod,
                    resource="pod",
                    owner=expected_owner,
                    expected_name=pod_name,
                    expected_runtime_generation=str(expected_runtime_generation),
                    expected_attempt_id=str(expected_provision_attempt),
                )
                != runtime_incarnation
            ):
                raise WorkspaceRuntimeAuthorityError("workspace Pod UID changed")
        else:
            runtime_incarnation = str(getattr(pod.metadata, "uid", "") or "")
        if not runtime_incarnation:
            raise RuntimeError("workspace pod has no Kubernetes UID")
        trusted_seed_uid: str | None = None
        seed_configmap = None
        if expected_owner is not None:
            seed_configmap = self._require_stateless_pod_storage_binding(
                pod,
                owner=expected_owner,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=expected_seed_configmap,
                expected_pod_name=expected_pod_name,
            )
            if seed_configmap is not None:
                seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_configmap,
                    namespace=self._namespace,
                )
                trusted_seed_uid = (
                    self._require_pinned_workspace_resource_identity(
                        seed,
                        resource="seed_configmap",
                        owner=expected_owner,
                        expected_name=seed_configmap,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                    )
                    if pinned_attempt
                    else self._require_stateless_seed_configmap_identity(
                        seed,
                        owner=expected_owner,
                        generation=expected_creation_generation,
                    )
                )
                if pinned_attempt and (
                    expected_seed_configmap_uid is None
                    or trusted_seed_uid != expected_seed_configmap_uid
                ):
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace seed ConfigMap UID changed"
                    )
                self._require_seed_configmap_pod_owner_reference(
                    seed,
                    pod_name=pod_name,
                    runtime_incarnation=runtime_incarnation,
                )
        backing_kind = "pod"
        backing_uid = runtime_incarnation
        trusted_claim_uid: str | None = None
        trusted_service_uid: str | None = None
        if pvc_name:
            claim = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_persistent_volume_claim,
                name=pvc_name,
                namespace=self._namespace,
            )
            backing_kind = "pvc"
            if expected_owner is not None:
                trusted_claim_uid = (
                    self._require_pinned_workspace_resource_identity(
                        claim,
                        resource="pvc",
                        owner=expected_owner,
                        expected_name=pvc_name,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                        retained_uid=expected_retained_pvc_uid,
                    )
                    if pinned_attempt
                    else self._require_stateless_pvc_identity(
                        claim,
                        owner=expected_owner,
                        pvc_name=pvc_name,
                        expected_storage_class=expected_pvc_storage_class,
                    )
                )
                if (
                    expected_pvc_uid is not None
                    and trusted_claim_uid != expected_pvc_uid
                ):
                    raise WorkspaceRuntimeAuthorityError("workspace PVC UID changed")
                backing_uid = trusted_claim_uid
            else:
                backing_uid = str(getattr(claim.metadata, "uid", "") or "")
            if expected_owner is not None:
                service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=expected_owner.pod_name,
                    namespace=self._namespace,
                )
                trusted_service_uid = (
                    self._require_pinned_workspace_resource_identity(
                        service,
                        resource="service",
                        owner=expected_owner,
                        expected_name=expected_owner.pod_name,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                        retained_uid=expected_retained_service_uid,
                    )
                    if pinned_attempt
                    else self._require_stateless_service_identity(
                        service,
                        owner=expected_owner,
                    )
                )
                if pinned_attempt and (
                    expected_service_uid is None
                    or trusted_service_uid != expected_service_uid
                ):
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Service UID changed"
                    )
        if not backing_uid:
            raise RuntimeError("workspace pod has no Kubernetes UID")
        output = await self._bounded_kubernetes_call(
            _isolated_pod_exec,
            pod_name,
            self._namespace,
            command=[
                "ssh-keygen",
                "-lf",
                "/var/lib/srw-system/ssh/ssh_host_ed25519_key.pub",
                "-E",
                "sha256",
            ],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _request_timeout=10,
        )
        fields = str(output).strip().split()
        fingerprint = next(
            (field for field in fields if field.startswith("SHA256:")), None
        )
        if not fingerprint:
            raise RuntimeError("workspace host-key fingerprint was not reported")
        if expected_owner is not None:
            confirmed = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=self._namespace,
            )
            self._require_workspace_pod_connection_identity(
                confirmed,
                owner=expected_owner,
                expected_runtime_incarnation=runtime_incarnation,
                expected_creation_generation=expected_creation_generation,
                expected_network_tier=expected_network_tier,
                expected_pvc_name=pvc_name,
                expected_seed_configmap=expected_seed_configmap,
                expected_pod_name=expected_pod_name,
                expected_component=expected_component,
            )
            if pinned_attempt and (
                self._require_pinned_workspace_resource_identity(
                    confirmed,
                    resource="pod",
                    owner=expected_owner,
                    expected_name=pod_name,
                    expected_runtime_generation=str(expected_runtime_generation),
                    expected_attempt_id=str(expected_provision_attempt),
                )
                != runtime_incarnation
            ):
                raise WorkspaceRuntimeAuthorityError("workspace Pod UID changed")
            if pvc_name:
                confirmed_claim = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_persistent_volume_claim,
                    name=pvc_name,
                    namespace=self._namespace,
                )
                confirmed_claim_uid = (
                    self._require_pinned_workspace_resource_identity(
                        confirmed_claim,
                        resource="pvc",
                        owner=expected_owner,
                        expected_name=pvc_name,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                        retained_uid=expected_retained_pvc_uid,
                    )
                    if pinned_attempt
                    else self._require_stateless_pvc_identity(
                        confirmed_claim,
                        owner=expected_owner,
                        pvc_name=pvc_name,
                        expected_storage_class=expected_pvc_storage_class,
                    )
                )
                if confirmed_claim_uid != trusted_claim_uid:
                    raise WorkspaceRuntimeAuthorityError("workspace PVC UID changed")
                confirmed_service = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_service,
                    name=expected_owner.pod_name,
                    namespace=self._namespace,
                )
                confirmed_service_uid = (
                    self._require_pinned_workspace_resource_identity(
                        confirmed_service,
                        resource="service",
                        owner=expected_owner,
                        expected_name=expected_owner.pod_name,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                        retained_uid=expected_retained_service_uid,
                    )
                    if pinned_attempt
                    else self._require_stateless_service_identity(
                        confirmed_service,
                        owner=expected_owner,
                    )
                )
                if confirmed_service_uid != trusted_service_uid:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace Service UID changed"
                    )
            if seed_configmap is not None:
                confirmed_seed = await self._bounded_kubernetes_call(
                    self._core_api.read_namespaced_config_map,
                    name=seed_configmap,
                    namespace=self._namespace,
                )
                confirmed_seed_uid = (
                    self._require_pinned_workspace_resource_identity(
                        confirmed_seed,
                        resource="seed_configmap",
                        owner=expected_owner,
                        expected_name=seed_configmap,
                        expected_runtime_generation=str(expected_runtime_generation),
                        expected_attempt_id=str(expected_provision_attempt),
                    )
                    if pinned_attempt
                    else self._require_stateless_seed_configmap_identity(
                        confirmed_seed,
                        owner=expected_owner,
                        generation=expected_creation_generation,
                    )
                )
                if confirmed_seed_uid != trusted_seed_uid:
                    raise WorkspaceRuntimeAuthorityError(
                        "workspace seed ConfigMap UID changed"
                    )
                self._require_seed_configmap_pod_owner_reference(
                    confirmed_seed,
                    pod_name=pod_name,
                    runtime_incarnation=runtime_incarnation,
                )
        return (
            f"k8s-{backing_kind}:{self._namespace}:{backing_uid}",
            fingerprint,
            runtime_incarnation,
        )

    async def _record_creation_diagnostic(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        exc: BaseException,
        *,
        strict_stateless: bool,
    ) -> None:
        """Give a Job its pull or admission error; never a lifecycle projection.

        Only ``error`` is written, and only while this reservation is current.
        Sessions log only (A1 refinement 1), and strict stateless creations
        keep their ambiguity rules. Best effort: it runs inside the creation's
        failure handler, so it never raises there.
        """
        if owner.kind != "job" or strict_stateless:
            return
        message = (
            str(exc)
            if isinstance(exc, WorkspaceImagePullError)
            else pod_admission_rejection(exc)
        )
        if message is None:
            return
        try:
            current = await self._workspace_creation_reservation_is_current(
                owner, reservation, scope="workspace_container"
            )
        except Exception:
            logger.exception(
                "Could not record the workspace creation error for %s %s",
                owner.kind,
                owner.id,
            )
            return
        if current:
            await self._set_context(owner, {"error": message})

    async def _settle_failed_job_creation(
        self,
        owner: WorkspaceOwner,
        reservation: dict[str, Any],
        exc: BaseException,
        *,
        strict_stateless: bool,
        fresh_storage: bool,
    ) -> bool:
        """Settle a pull-failed Job's reservation on its never-started Pod.

        The dispatcher fails the Job once this creation returns False. If the
        reservation were still open, that terminal transition would cancel it
        *and* admit a cleanup intent for the same runtime, and the two refuse
        each other forever: a cancelled reservation is handed to cleanup only
        while no cleanup intent is open, and the intent captures resources only
        once the reservation is handed off. The Pod, its Service, its PVC and
        its finalizer would stay, and the Job could not be deleted.

        The reservation stays open on failure because a failed response is
        normally ambiguous. Here it is not: the waiter just observed the exact
        Pod, and its UID is already bound in the Job's context. So settle the
        generation on that exact runtime, as the not-ready-timeout path already
        does, while this creation still holds the owner/scope mutation guard and
        the reservation. The Job's terminal transition then admits an ordinary
        terminal cleanup, which retires the Pod through the finalizer protocol
        (UID-preconditioned delete, every container observed terminated,
        process-zero receipt, finalizer release) and reclaims its PVC and
        Service.

        Only a fresh ``create`` whose volume was made by this same attempt (or
        an emptyDir workspace) is settled, and only on a Pod that never started
        a container. That terminal reclaim deletes the volume, so a restore,
        reattach or recreate over a kept claim, which may hold user data that
        still needs its archive, keeps today's data-preserving behaviour, as
        does any other failure. Best effort and never raises: it runs inside
        the creation's failure handler, which returns False either way.
        """

        if (
            owner.kind != "job"
            or strict_stateless
            or not isinstance(exc, WorkspaceImagePullError)
            or str(reservation.get("operation_kind") or "") != "create"
            or fresh_storage is not True
        ):
            return False
        settle = getattr(
            type(self._db),
            "settle_managed_repository_workspace_creation_reservation",
            None,
        )
        if not callable(settle):
            return False
        try:
            # The reservation row may carry the UID as a uuid.UUID.
            runtime = _canonical_runtime_uuid(
                str(reservation.get("runtime_incarnation") or ""),
                label="failed workspace creation runtime",
            )
            if not await self._workspace_creation_reservation_is_current(
                owner, reservation, scope="workspace_container"
            ):
                return False
            pod = await self._bounded_kubernetes_call(
                self._core_api.read_namespaced_pod,
                name=owner.pod_name,
                namespace=self._namespace,
            )
            if (
                self._require_workspace_pod_owner(
                    pod, owner=owner, allow_owner_unlabeled=False
                )
                != runtime
            ):
                return False
            self._require_workspace_creation_reservation_annotation(
                pod, reservation_id=str(reservation["id"])
            )
            if not _pod_never_started_a_container(pod):
                logger.warning(
                    "Leaving the pull-failed workspace creation open: Pod %s "
                    "may have run a container (%s %s)",
                    owner.pod_name,
                    owner.kind,
                    owner.id,
                )
                return False
            settled = bool(
                await settle(
                    self._db,
                    owner.id,
                    owner_kind="job",
                    scope="workspace_container",
                    reservation_generation=int(reservation["reservation_generation"]),
                    claimant=str(reservation["claimed_by"]),
                    claim_token=int(reservation["claim_token"]),
                    runtime_incarnation=runtime,
                )
            )
        except Exception:
            logger.exception(
                "Could not settle the pull-failed workspace creation for %s %s",
                owner.kind,
                owner.id,
            )
            return False
        if settled:
            logger.info(
                "Settled the pull-failed workspace creation on never-started "
                "Pod %s; the Job's terminal cleanup retires it (%s %s)",
                owner.pod_name,
                owner.kind,
                owner.id,
            )
        return settled

    async def _set_context(self, owner: WorkspaceOwner, updates: dict) -> bool:
        """Atomically merge updates into the workspace context for a job or session."""
        if not self._db:
            return False

        try:
            if owner.kind == "job":
                return bool(
                    await self._db.merge_workspace_container_context(owner.id, updates)
                )
            return bool(
                await self._db.merge_thread_workspace_context(owner.id, updates)
            )
        except Exception:
            logger.exception(
                "Failed to update workspace container context for %s %s",
                owner.kind,
                owner.id,
            )
            return False


# Module-level singleton
container_provisioner = ContainerProvisioner()
