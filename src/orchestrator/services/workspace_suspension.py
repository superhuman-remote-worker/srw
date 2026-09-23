"""Workspace Container Idle Suspension — S3 snapshot + pod lifecycle.

Detects idle workspace containers (pods for paused/frozen jobs), captures
their environment to S3 via SnapshotService, deletes the pod, and restores
on demand when the job needs to run again.

Requires both ContainerProvisioner (K8s) and SnapshotService (S3) to be
available. Gracefully degrades: when S3 is unavailable, containers stay alive.
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID, uuid4

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.container_provisioner import (
    WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY,
    WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY,
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceCleanupOutcome,
    WorkspaceRuntimeAttestation,
    WorkspaceRuntimeAuthorityError,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.ssh_helpers import (
    EXTRACT_HOME_REMOTE_CMD,
    EXTRACT_REMOTE_CMD,
    stream_extract_snapshot,
)
from orchestrator.services.restore_work_lease import (
    RestoreWorkLeaseHeartbeat,
    RestoreWorkLeaseLost,
)
from orchestrator.services.vm_provisioner import (
    VMTeardownIdentity,
    VMTeardownResult,
    vm_persistent_rootdisk_enabled,
)
from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner

logger = logging.getLogger(__name__)


# Durable thread-workspace intent.  A restore can fail after creating a fresh
# emptyDir/PVC-backed pod; a later reconcile must retry snapshot extraction,
# not fall through to generic create and publish that empty/partial workspace as
# Ready.  The marker is set before every thread restore attempt and cleared only
# after the snapshot/reattach path has completed successfully.
WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY = "_snapshot_restore_required"


@dataclass(frozen=True, slots=True)
class _StrictSessionRestoreAuthority:
    """Exact post-create authority retained across a terminal snapshot extract."""

    workspace_generation: str
    runtime_incarnation: str
    endpoint_generation: str
    backing_id: str
    host_key_fingerprint: str
    ssh_host: str
    ssh_port: int
    workspace_status: str


def _thread_metadata(thread: dict[str, Any]) -> dict[str, Any]:
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("thread workspace metadata is malformed") from exc
    if not isinstance(metadata, dict):
        raise RuntimeError("thread workspace metadata is malformed")
    return metadata


def _canonical_uuid(value: Any, *, label: str) -> str:
    try:
        canonical = str(UUID(str(value)))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"strict restore {label} is unavailable") from exc
    if value != canonical:
        raise RuntimeError(f"strict restore {label} is not canonical")
    return canonical


def _captured_workspace_runtime(workspace: Any) -> str | None:
    """Return one exact Kubernetes runtime UID, or refuse legacy ambiguity."""

    if not isinstance(workspace, dict):
        return None
    value = workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
    if not isinstance(value, str):
        return None
    try:
        canonical = str(UUID(value))
    except (TypeError, ValueError):
        return None
    return canonical if value == canonical else None


def _settled_restore_runtime(
    workspace: Any,
    creation: Any,
    *,
    retired_runtime: str | None = None,
) -> str | None:
    """Validate the current B projection against its exact settled receipt."""

    if not isinstance(workspace, dict) or not isinstance(creation, dict):
        return None
    if (
        str(creation.get("operation_kind") or "") != "restore"
        or str(creation.get("result_kind") or "") != "settled"
        or creation.get("settled_at") is None
        or workspace.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY) is not True
        or workspace.get(WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY)
        != str(creation.get("id") or "")
        or workspace.get(WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY)
        != str(creation.get("claim_token") or "")
    ):
        return None
    runtime = _captured_workspace_runtime(workspace)
    try:
        receipt_runtime = _canonical_uuid(
            creation.get("runtime_incarnation"), label="restore runtime"
        )
    except RuntimeError:
        return None
    if runtime != receipt_runtime or runtime == retired_runtime:
        return None
    return runtime


def _claimed_restore_work(
    restore_work: Any,
    *,
    runtime_incarnation: str,
    claimant: str,
) -> bool:
    """Validate the exact-B work lease returned by the server-owned ledger."""

    if not isinstance(restore_work, dict):
        return False
    try:
        reservation_id = str(UUID(str(restore_work.get("id"))))
        work_runtime = str(UUID(str(restore_work.get("runtime_incarnation"))))
        work_token = int(restore_work.get("restore_work_claim_token"))
    except (TypeError, ValueError):
        return False
    return bool(
        reservation_id
        and work_runtime == runtime_incarnation
        and work_token > 0
        and restore_work.get("restore_work_claimed_by") == claimant
        and restore_work.get("restore_work_completed_at") is None
    )


def _strict_session_restore_authority(
    thread: dict[str, Any],
) -> _StrictSessionRestoreAuthority:
    """Parse the exact provisioner-attested tuple used by a strict restore."""

    metadata = _thread_metadata(thread)
    workspace = metadata.get("workspace_container")
    binding = metadata.get("_workspace_binding")
    if not isinstance(workspace, dict) or not isinstance(binding, dict):
        raise RuntimeError("strict restore workspace authority is malformed")
    if workspace.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY) is not True:
        raise RuntimeError("strict restore intent is not armed")
    workspace_status = workspace.get("status")
    if not isinstance(workspace_status, str) or workspace_status not in {
        "ready",
        "failed",
        "restoring",
        "deleted",
        "suspended",
        "created",
        "creating",
        "pending",
    }:
        raise RuntimeError("strict restore endpoint status is not quarantined")
    if binding.get("kind") != "remote":
        raise RuntimeError("strict restore backing is not remote")

    workspace_generation = _canonical_uuid(
        binding.get("generation"), label="workspace generation"
    )
    endpoint_generation = _canonical_uuid(
        workspace.get(CANVAS_WORKSPACE_GENERATION_KEY),
        label="endpoint generation",
    )
    if endpoint_generation != workspace_generation:
        raise RuntimeError("strict restore endpoint generation changed")
    runtime_incarnation = _canonical_uuid(
        workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY),
        label="runtime incarnation",
    )

    backing_id = binding.get("backing_id")
    if not isinstance(backing_id, str) or not backing_id.startswith(
        ("k8s-pod:", "k8s-pvc:")
    ):
        raise RuntimeError("strict restore backing identity is unavailable")
    fingerprint = binding.get("ssh_host_key_fingerprint")
    if (
        not isinstance(fingerprint, str)
        or not fingerprint.startswith("SHA256:")
        or len(fingerprint) > 128
        or any(char.isspace() for char in fingerprint)
    ):
        raise RuntimeError("strict restore SSH host-key fingerprint is unavailable")
    ssh_host = workspace.get("pod_ip")
    if (
        not isinstance(ssh_host, str)
        or not ssh_host
        or any(ord(char) < 33 or ord(char) == 127 for char in ssh_host)
    ):
        raise RuntimeError("strict restore SSH endpoint is unavailable")
    ssh_port_raw = workspace.get("port")
    if type(ssh_port_raw) is not int or not 1 <= ssh_port_raw <= 65535:
        raise RuntimeError("strict restore SSH endpoint port is invalid")

    return _StrictSessionRestoreAuthority(
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
        endpoint_generation=endpoint_generation,
        backing_id=backing_id,
        host_key_fingerprint=fingerprint,
        ssh_host=ssh_host,
        ssh_port=ssh_port_raw,
        workspace_status=workspace_status,
    )


def _reclaim_on_idle_enabled() -> bool:
    """Opt-in gate for dropping a session's hot-cache PVC on idle-suspend.

    Default OFF: today's retain-on-idle behavior (snapshot + delete pod, keep
    the PVC) is unchanged unless an operator turns this on. When enabled, the
    PVC is dropped only after ``verify_snapshot`` confirms the S3 archive is
    restorable — see the call site in ``suspend_thread_workspace``.
    Truthy-token parsing mirrors ``canvas_snapshots.snapshots_enabled()``.
    See knowledge-base/knowledge/features/workspace_durability_tiering.md §D3.
    """
    return os.environ.get("WORKSPACE_RECLAIM_ON_IDLE", "false").strip().lower() in (
        "true",
        "1",
        "yes",
        "on",
    )


def _thread_is_vm_tier(metadata: dict, ws_ctx: dict, vm_ctx: dict) -> bool:
    """Is this THREAD's workspace a VM rather than a container?

    Ask the resolved tier; never infer it from which metadata keys exist.
    ``metadata.workspace_container`` is present on *every* thread because
    ``_setup_gitea`` writes ``git_remote_url``/``repo_name`` there for all tiers,
    so "the key exists" does NOT mean a container was provisioned. Reading
    presence made every VM session look pod-tier, which made
    ``suspend_thread_workspace`` bail before doing anything — VM sessions could
    never be suspended and their VMs ran until the session ended.
    knowledge-base/knowledge/issues/workspace_suspension_infers_tier_from_metadata_presence.md

    Jobs are deliberately NOT covered by this: a job's
    ``context.workspace_container`` is written only by container provisioning
    (its git_remote_url lives at the context root), so presence really does imply
    pod-tier there and the job paths keep their existing checks.
    """
    from shared.backend_kinds import is_vm_backend

    # A materialized VM beats the declared backend, because upgrade-to-VM
    # provisions metadata.vm WITHOUT rewriting config_override.workspace.backend
    # — an upgraded session still declares 'sandbox' or 'virtual' forever. Pod
    # *state* (not mere presence) is what rules a container in: a git-only
    # workspace_container has no 'status', and _setup_gitea writes one of those
    # for every tier.
    #
    # Checked FIRST. It used to sit behind `if backend: return
    # is_vm_backend(backend)`, which returned early for any non-empty string and
    # so made this branch unreachable in exactly the case it was written for —
    # every VM-upgraded thread read as pod-tier and refused to suspend
    # (live-gate finding, thread b4ae24bb).
    if vm_ctx.get("status") and not ws_ctx.get("status"):
        return True

    backend = ((metadata.get("config_override") or {}).get("workspace") or {}).get(
        "backend"
    )
    if backend:
        return is_vm_backend(backend)
    return False


# ``ContainerProvisioner._trusted_pod_ssh_identity`` mints the thread's
# ``_workspace_binding.backing_id`` from the PVC when — and only when — the
# workspace pod mounts one, as ``k8s-pvc:<namespace>:<pvc-uid>``; an emptyDir
# pod is bound to ``k8s-pod:<namespace>:<pod-uid>`` instead. That makes the
# string the authoritative reattach signal: the PVC's UID is stable across every
# pod recreate that reattaches the SAME volume, and changes the moment a new
# volume is minted (first PVC-backed create, a rollback to emptyDir, or the
# single-replica node-loss fallback discarding a wedged PVC).
_K8S_PVC_BACKING_PREFIX = "k8s-pvc:"


def _workspace_backing_id(metadata: dict) -> str:
    """The thread's currently bound workspace backing id ("" when unbound)."""
    binding = metadata.get("_workspace_binding")
    if not isinstance(binding, dict):
        return ""
    backing_id = binding.get("backing_id")
    return backing_id if isinstance(backing_id, str) else ""


def _volume_survived_teardown(
    prior_backing_id: str, current_backing_id: str
) -> tuple[bool, str]:
    """Did the restored pod come back on the volume it already had?

    The container-tier twin of the VM path's ``rootdisk == "kept"``: when the
    answer is yes, the disk already holds the workspace and extracting the older
    S3 tarball over it would replace newer files with older ones.

    Returns ``(reattached, reason)`` — the reason is log copy for the skip.
    """
    if not prior_backing_id.startswith(_K8S_PVC_BACKING_PREFIX):
        # emptyDir (or never bound at all): storage died with the pod, so the
        # S3 snapshot is the only copy and must be unrolled. This is also the
        # mixed-fleet upgrade case — a session suspended before PVCs were
        # enabled restores onto a brand-new empty volume.
        return False, ""
    if not current_backing_id:
        # The post-create rebind is best-effort (create_workspace logs and
        # continues when it raises), so its absence is ambiguous rather than
        # informative. The tie goes to the volume: the generation we came in
        # with was PVC-backed, a PVC survives every non-permanent teardown, and
        # unrolling a stale tarball over live files is the unrecoverable
        # mistake — a skipped extract is not.
        return True, "binding unavailable, prior generation was PVC-backed"
    if current_backing_id == prior_backing_id:
        return True, "same PVC reattached"
    # A different backing id means a different volume (or an emptyDir pod after
    # a flag rollback): whatever the pod is mounting now, it is not the tree we
    # suspended, so restore it from S3.
    return False, ""


def _resolve_ssh_port(ws_ctx: dict, vm_ctx: dict, is_vm: Optional[bool] = None) -> int:
    """Resolve the snapshot SSH port by workspace kind.

    Container/pod workspaces run sshd on 30022; only true VM contexts use the
    VM ssh_port (default 22). Previously both fell through to a VM-shaped 22
    default, which silently broke pod snapshots when ``port`` was absent from
    the stored context (the cause of the dev-cluster leaked-pod incident).

    ``is_vm`` is the caller's resolved tier. Thread callers pass it because
    ``ws_ctx`` is truthy for every thread (git coordinates), so the presence
    fallback would hand a VM the pod port 30022. Job callers omit it and keep the
    presence behaviour, which is correct for them.
    """
    if is_vm is True:
        return int(vm_ctx.get("ssh_port", 22))
    if is_vm is False:
        return int(ws_ctx.get("port", 30022))
    if ws_ctx:
        return int(ws_ctx.get("port", 30022))
    return int(vm_ctx.get("ssh_port", 22))


class WorkspaceSuspensionService:
    """Coordinates idle suspension between SnapshotService and ContainerProvisioner.

    Status transitions for workspace_container.status:
        ready → suspending → suspended → restoring → ready
    """

    def __init__(self):
        self._db: Optional[Any] = None
        self._snapshot_service: Optional[Any] = None
        self._container_provisioner: Optional[Any] = None
        self._docker_provisioner: Optional[Any] = None
        self._vm_provisioner: Optional[Any] = None
        self._agent_provisioner: Optional[Any] = None
        self._workspace_recovery_store: Optional[Any] = None

    def connect(
        self,
        db: Any,
        snapshot_service: Any,
        container_provisioner: Any,
        docker_provisioner: Any = None,
        vm_provisioner: Any = None,
        agent_provisioner: Any = None,
    ) -> None:
        self._db = db
        self._workspace_recovery_store = VMWorkspaceRecoveryStore(db)
        self._snapshot_service = snapshot_service
        self._container_provisioner = container_provisioner
        self._docker_provisioner = docker_provisioner
        self._vm_provisioner = vm_provisioner
        self._agent_provisioner = agent_provisioner

        if self.is_enabled:
            backends = []
            if self._container_provisioner and self._container_provisioner.is_available:
                backends.append("k8s")
            if self._vm_provisioner and self._vm_provisioner.is_available:
                backends.append("vm")
            logger.info(
                "Workspace suspension enabled (idle_timeout=%dm, backends=%s)",
                self.idle_timeout_minutes,
                ",".join(backends),
            )
        else:
            logger.info("Workspace suspension disabled (S3 or provisioner unavailable)")

    @property
    def is_enabled(self) -> bool:
        """True if S3 snapshots and at least one provisioner are available."""
        if not self._snapshot_service or not self._snapshot_service.is_available:
            return False
        return (
            self._container_provisioner is not None
            and self._container_provisioner.is_available
        ) or (self._vm_provisioner is not None and self._vm_provisioner.is_available)

    @property
    def idle_timeout_minutes(self) -> int:
        return int(os.environ.get("WORKSPACE_IDLE_TIMEOUT", "30"))

    async def _claim_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
    ) -> tuple[str, dict[str, Any]] | None:
        """Claim B's post-create effects, replaying one lost claim response."""

        claimant = f"workspace-restore:{uuid4()}"
        for _attempt in range(2):
            try:
                restore_work = (
                    await self._container_provisioner.claim_workspace_restore_work(
                        owner,
                        claimant=claimant,
                        lease_seconds=300,
                    )
                )
            except Exception:
                logger.warning(
                    "Workspace restore-work claim response was ambiguous for %s %s",
                    owner.kind,
                    owner.id,
                )
                continue
            if _claimed_restore_work(
                restore_work,
                runtime_incarnation=runtime_incarnation,
                claimant=claimant,
            ):
                return claimant, restore_work
            return None
        return None

    async def _release_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> None:
        try:
            await self._container_provisioner.release_workspace_restore_work(
                owner,
                restore_work=restore_work,
                claimant=claimant,
                retry_seconds=30,
            )
        except Exception:
            # The bounded lease remains the crash-recovery authority.
            logger.warning(
                "Workspace restore-work release response was ambiguous for %s %s",
                owner.kind,
                owner.id,
            )

    async def _complete_workspace_restore_work(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> bool:
        """Complete exact-B Ready publication, replaying a lost response."""

        for _attempt in range(2):
            try:
                if await self._container_provisioner.complete_workspace_restore_work(
                    owner,
                    restore_work=restore_work,
                    claimant=claimant,
                    success=True,
                ):
                    return True
            except Exception:
                logger.warning(
                    "Workspace restore-work completion response was ambiguous for "
                    "%s %s",
                    owner.kind,
                    owner.id,
                )
        return False

    def _workspace_restore_heartbeat(
        self,
        owner: WorkspaceOwner,
        *,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> RestoreWorkLeaseHeartbeat:
        async def renew() -> object:
            return await self._container_provisioner.renew_workspace_restore_work(
                owner,
                restore_work=restore_work,
                claimant=claimant,
                lease_seconds=300,
            )

        return RestoreWorkLeaseHeartbeat(renew, interval_seconds=60)

    @staticmethod
    def _vm_capture_tuple_matches(
        identity: VMTeardownIdentity,
        attestation: WorkspaceRuntimeAttestation,
    ) -> bool:
        """Bind destructive VM identity to the exact attested SSH target."""

        return bool(
            identity.provision_generation == attestation.workspace_generation
            and identity.ssh_host == attestation.host
            and identity.ssh_port == attestation.port
            and identity.ssh_host_key_fingerprint
            == attestation.ssh_host_key_fingerprint
            and identity.vm_uid
            and identity.rootdisk_pvc_uid
        )

    async def _capture_vm_capture_authority(
        self,
        owner_id: str,
        *,
        entity_type: str,
    ) -> tuple[VMTeardownIdentity, WorkspaceRuntimeAttestation] | None:
        """Capture one immutable VM generation and its fresh live endpoint."""

        if not self._vm_provisioner or not self._vm_provisioner.is_available:
            return None
        try:
            identity = await self._vm_provisioner.capture_vm_teardown_identity(
                owner_id,
                entity_type=entity_type,
            )
            attestation = await self._vm_provisioner.attest_workspace_runtime(
                owner_id,
                entity_type=entity_type,
            )
            if not self._vm_capture_tuple_matches(identity, attestation):
                return None
            if (
                await self._vm_provisioner.revalidate_vm_teardown_identity(
                    owner_id,
                    identity,
                    entity_type=entity_type,
                )
                != "matched"
            ):
                return None
            return identity, attestation
        except Exception:
            logger.warning(
                "VM capture authority unavailable for %s %s",
                entity_type,
                owner_id,
                exc_info=True,
            )
            return None

    async def _revalidate_vm_capture_target(
        self,
        owner_id: str,
        *,
        entity_type: str,
        identity: VMTeardownIdentity,
        attestation: WorkspaceRuntimeAttestation,
    ) -> tuple[str, int, str] | None:
        """Freshly re-prove the same VM immediately before one external read."""

        try:
            current = await self._vm_provisioner.attest_workspace_runtime(
                owner_id,
                entity_type=entity_type,
            )
            if current != attestation:
                return None
            if (
                await self._vm_provisioner.revalidate_vm_teardown_identity(
                    owner_id,
                    identity,
                    entity_type=entity_type,
                )
                != "matched"
            ):
                return None
            return (
                attestation.host,
                attestation.port,
                attestation.ssh_host_key_fingerprint,
            )
        except Exception:
            return None

    async def _attest_claimed_workspace_restore_target(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> WorkspaceRuntimeAttestation:
        """Attest exact B and immediately re-lock its durable work token."""

        attestation = await self._container_provisioner.attest_workspace_runtime(owner)
        if attestation.runtime_incarnation != runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError("workspace restore Pod UID changed")
        renewed = await self._container_provisioner.renew_workspace_restore_work(
            owner,
            restore_work=restore_work,
            claimant=claimant,
            lease_seconds=300,
        )
        if not _claimed_restore_work(
            renewed,
            runtime_incarnation=runtime_incarnation,
            claimant=claimant,
        ) or any(
            str(renewed.get(key) or "") != str(restore_work.get(key) or "")
            for key in ("id", "restore_work_claim_token")
        ):
            raise RestoreWorkLeaseLost(
                "workspace restore authority changed after runtime attestation"
            )
        return attestation

    # =========================================================================
    # Suspend: snapshot → delete pod
    # =========================================================================

    async def _settle_container_suspension(
        self,
        owner: WorkspaceOwner,
        *,
        runtime_incarnation: str,
        suspended_at: str,
        snapshot_restore_required: bool,
    ) -> bool:
        """Retire one exact Pod through the durable preserve-only protocol.

        Snapshot capture intentionally happens before this helper: it is a
        read-only operation and must succeed before cleanup gains authority.
        The post-snapshot identity probe prevents a late A→B replacement from
        turning A's snapshot into deletion authority over B.  Projection to
        ``suspended`` is owned by cleanup settlement; callers never hand-merge
        lifecycle state around the intent.
        """

        identity = (
            await self._container_provisioner.capture_workspace_teardown_identity(
                owner,
                expected_runtime_incarnation=runtime_incarnation,
            )
        )
        if (
            not isinstance(identity, WorkspaceTeardownIdentity)
            or identity.pod_uid != runtime_incarnation
        ):
            return False
        intent = await self._container_provisioner.prepare_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime_incarnation,
            target_disposition="suspended",
            reclaim_shared_resources=False,
            suspended_at=suspended_at,
            identity=identity,
            snapshot_restore_required=snapshot_restore_required,
        )
        if not isinstance(intent, dict):
            return False
        outcome = await self._container_provisioner.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime_incarnation,
            intent_generation=int(intent["intent_generation"]),
        )
        if not isinstance(outcome, WorkspaceCleanupOutcome):
            raise RuntimeError("workspace suspension returned an untyped outcome")
        if outcome.superseded:
            logger.info(
                "Workspace suspension target %s became stale; preserving its successor",
                runtime_incarnation,
            )
            return False
        return outcome.settled

    async def suspend_workspace(self, job_id: str) -> bool:
        """Capture snapshot to S3, then tear down the workspace.

        VM workspaces retain the existing snapshot → delete flow. Kubernetes
        capture is temporarily refused before I/O because no durable
        pre-delete capture authority exists yet.

        Returns True if snapshot + teardown succeeded. Failures before deletion
        restore ``ready`` only while the captured runtime is still current;
        stale or already-deleted runtimes never project over a successor.
        """
        if not self.is_enabled or not self._db:
            return False

        job = await self._db.get_job(job_id)
        if not job:
            return False

        ctx = job.get("context") or {}
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                ctx = {}

        ws_ctx = ctx.get("workspace_container", {})
        vm_ctx = ctx.get("vm", {})
        provisioner_type = ws_ctx.get("provisioner")
        if provisioner_type == "docker":
            logger.warning(
                "Static Docker workspace suspension is disabled; safe reuse "
                "requires controller-attested container recreation (job %s)",
                job_id,
            )
            return False

        from orchestrator.services.ide_settings import is_kubernetes_capture_context

        if is_kubernetes_capture_context(ctx) or not vm_ctx:
            # Temporary containment: a stale Kubernetes endpoint (including a
            # stable Service whose selector now names successor B) is not
            # capture authority for predecessor A.  Migration 0197 cleanup
            # intents start at process-zero teardown and therefore cannot make
            # the preceding snapshot/profile reads crash-safe.  Refuse before
            # resolving/dialing any endpoint or mutating owner state; a future
            # durable pre-delete capture authority must replace this guard.
            logger.warning(
                "Kubernetes workspace suspension capture is disabled pending "
                "durable exact-runtime capture authority (job %s)",
                job_id,
            )
            return False

        # The containment guard above leaves only the explicit VM path.  A VM
        # owner may still carry a git-only ``workspace_container`` object, so
        # its mere presence must not redirect status/capture to the Pod path.
        ws_status = vm_ctx.get("status")
        if ws_status != "ready":
            return False

        authority = await self._capture_vm_capture_authority(
            job_id,
            entity_type="job",
        )
        if authority is None:
            # A raw/stale VM endpoint is not capture authority.  Preserve the
            # live workspace before reading even one settings/snapshot byte.
            return False
        vm_identity, vm_attestation = authority
        # Reserve suspension before the first IDE/settings byte. The exact VM
        # operation is then claimed from the reserved state; default-dark or
        # contended rollouts restore Ready without touching the guest.
        if not await self._db.merge_vm_context_if_provision_generation(
            job_id,
            vm_identity.provision_generation,
            {"status": "suspending", "_suspend_remote_io_closed": None},
        ):
            return False

        from orchestrator.services.vm_remote_operation import (
            VMRemoteOperationLeaseLost,
            VMRemoteOperationUnavailable,
            claim_vm_remote_operation,
        )

        try:
            lease = await claim_vm_remote_operation(
                db=self._db,
                provisioner=self._vm_provisioner,
                owner_id=job_id,
                owner_kind="job",
                operation_kind="ide_profile",
                lease_seconds=300,
            )
        except VMRemoteOperationUnavailable:
            try:
                await self._db.merge_vm_context_if_provision_generation(
                    job_id,
                    vm_identity.provision_generation,
                    {"status": "ready", "_suspend_remote_io_closed": None},
                )
            except Exception:
                logger.warning(
                    "VM suspension reservation could not be rolled back for %s",
                    job_id,
                    exc_info=True,
                )
            return False

        ssh_host = lease.identity.ssh_host
        ssh_port = lease.identity.ssh_port
        fingerprint = lease.identity.ssh_host_key_fingerprint
        disk_survives_teardown = vm_persistent_rootdisk_enabled()
        remote_io_closed = False
        try:
            async with lease:
                if not (
                    vm_identity.provision_generation
                    == lease.identity.workspace_generation
                    and vm_identity.vm_uid == lease.identity.vm_uid
                    and vm_identity.ssh_host == ssh_host
                    and vm_identity.ssh_port == ssh_port
                    and vm_identity.ssh_host_key_fingerprint == fingerprint
                    and vm_attestation.runtime_incarnation
                    == lease.identity.launcher_pod_uid
                ):
                    raise VMRemoteOperationLeaseLost(
                        "VM suspension target changed before remote capture"
                    )

                async def capture_target() -> tuple[str, int, str] | None:
                    return await lease.revalidate()

                async def snapshot_authority() -> bool:
                    return await capture_target() is not None

                # Opportunistic final code-server config/profile pull while the
                # exact renewable operation still owns the guest filesystem.
                try:
                    user_id = job.get("user_id")
                    if user_id:
                        from orchestrator.services.ide_settings import (
                            IdeSettingsStore,
                            pull_ide_config,
                        )

                        store = IdeSettingsStore(self._db)
                        pulled = await pull_ide_config(
                            ssh_host,
                            int(ssh_port),
                            expected_host_key_fingerprint=fingerprint,
                            capture_authority=capture_target,
                        )
                        if await capture_target() is None:
                            raise VMRemoteOperationLeaseLost(
                                "VM authority changed during IDE settings capture"
                            )
                        if pulled:
                            await store.apply_pulled_files(str(user_id), pulled)
                        if (
                            self._snapshot_service
                            and self._snapshot_service.is_available
                        ):
                            from orchestrator.services.ide_profile_store import (
                                IdeProfileStore,
                            )
                            from orchestrator.services.ide_settings import (
                                capture_ide_profile,
                            )

                            profile = IdeProfileStore(
                                self._snapshot_service._s3,
                                self._snapshot_service._bucket,
                            )
                            await capture_ide_profile(
                                store,
                                str(user_id),
                                ssh_host,
                                int(ssh_port),
                                profile,
                                expected_host_key_fingerprint=fingerprint,
                                capture_authority=capture_target,
                                vm_lease=lease,
                            )
                except VMRemoteOperationLeaseLost:
                    raise
                except Exception:
                    logger.debug(
                        "ide settings teardown pull failed for job %s",
                        job_id,
                        exc_info=True,
                    )

                ok = await self._snapshot_service.capture_vm_snapshot(
                    job_id=job_id,
                    ssh_host=ssh_host,
                    ssh_port=int(ssh_port),
                    source_type="vm",
                    expected_host_key_fingerprint=fingerprint,
                    capture_authority=snapshot_authority,
                )
                if await capture_target() is None:
                    raise VMRemoteOperationLeaseLost(
                        "VM authority changed during suspension snapshot"
                    )

                # Close admission for any successor remote operation before
                # this lease settles. Renewal/success settlement deliberately
                # ignore this auxiliary marker; fresh claims do not.
                remote_io_closed = bool(
                    await self._db.merge_vm_context_if_provision_generation(
                        job_id,
                        vm_identity.provision_generation,
                        {
                            "_suspend_remote_io_closed": str(lease.receipt["id"]),
                        },
                    )
                )
                if not remote_io_closed:
                    raise VMRemoteOperationLeaseLost(
                        "VM suspension could not close subsequent remote I/O"
                    )

            # Same reasoning as the thread path below: with a persistent
            # rootdisk the disk carries the workspace across the teardown, so a
            # capture that can never succeed for a VM stops being a
            # precondition. A pod has no disk to keep and stays fail-closed.
            if not ok:
                if not disk_survives_teardown:
                    logger.warning(
                        "Snapshot capture failed for job %s — keeping workspace alive",
                        job_id,
                    )
                    await self._db.merge_vm_context_if_provision_generation(
                        job_id,
                        vm_identity.provision_generation,
                        {"status": "ready", "_suspend_remote_io_closed": None},
                    )
                    return False
                logger.info(
                    "Snapshot capture unavailable for job %s — suspending anyway; "
                    "the persistent rootdisk carries the workspace across teardown",
                    job_id,
                )

            # Tear down based on provisioner type
            suspended_ctx: dict[str, Any] = {
                "status": "suspended",
                "suspended_at": datetime.now(timezone.utc).isoformat(),
                "_suspend_remote_io_closed": str(lease.receipt["id"]),
            }

            if self._vm_provisioner and self._vm_provisioner.is_available:
                cleanup = await acquire_vm_cleanup_permit(
                    self._workspace_recovery_store,
                    owner_kind="job",
                    owner_id=job_id,
                    identity=vm_identity,
                    source="job_workspace_suspension",
                    purge_disk=not disk_survives_teardown,
                )
                if not cleanup.allowed:
                    raise RuntimeError("VM suspension held for workspace recovery")
                replayed = completed_cleanup_outcome(cleanup)
                if replayed is not None:
                    outcome = VMTeardownResult(replayed, replayed == "completed")
                else:
                    outcome = await self._vm_provisioner.release_vm_captured(
                        job_id,
                        vm_identity,
                        purge_disk=not disk_survives_teardown,
                        capture_snapshot=False,
                        entity_type="job",
                        **vm_cleanup_kwargs(cleanup),
                    )
                    if outcome.disposition in {"completed", "identity_superseded"}:
                        await complete_vm_cleanup_permit(
                            self._workspace_recovery_store,
                            cleanup,
                            outcome=outcome.disposition,
                            provisioner=self._vm_provisioner,
                        )
                if replayed == "completed":
                    await complete_vm_cleanup_permit(
                        self._workspace_recovery_store, cleanup,
                        outcome=replayed,
                        provisioner=self._vm_provisioner,
                    )
                if (
                    not isinstance(outcome, VMTeardownResult)
                    or outcome.disposition != "completed"
                ):
                    raise RuntimeError(
                        "VM suspension process-zero retirement is incomplete"
                    )
                if disk_survives_teardown:
                    suspended_ctx["rootdisk"] = "kept"
                if not await self._db.merge_vm_context_if_provision_generation(
                    job_id,
                    vm_identity.provision_generation,
                    suspended_ctx,
                ):
                    raise RuntimeError("VM suspension successor replaced the target")
            else:
                raise RuntimeError("VM suspension provisioner is unavailable")

            logger.info("Workspace suspended to S3 for job %s", job_id)
            return True

        except Exception:
            logger.exception("Failed to suspend workspace for job %s", job_id)
            try:
                if vm_ctx and (
                    await self._revalidate_vm_capture_target(
                        job_id,
                        entity_type="job",
                        identity=vm_identity,
                        attestation=vm_attestation,
                    )
                    is not None
                ):
                    await self._db.merge_vm_context_if_provision_generation(
                        job_id,
                        vm_identity.provision_generation,
                        {"status": "ready", "_suspend_remote_io_closed": None},
                    )
            except Exception:
                pass
            return False

    # =========================================================================
    # Restore: create pod → extract snapshot
    # =========================================================================

    async def restore_workspace(self, job_id: str) -> bool:
        """Provision a fresh workspace and extract the S3 snapshot into it.

        Dispatches to the correct provisioner based on workspace metadata:
        - K8s container: create pod → SSH extract
        - VM: create VM → SSH extract

        Returns True if provisioning + snapshot extraction succeeded.
        """
        if not self.is_enabled or not self._db:
            return False

        job = await self._db.get_job(job_id)
        if not job:
            return False

        ctx = job.get("context") or {}
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, TypeError):
                ctx = {}

        ws_ctx = ctx.get("workspace_container", {})
        vm_ctx = ctx.get("vm", {})
        provisioner_type = ws_ctx.get("provisioner")
        if provisioner_type == "docker":
            logger.warning(
                "Static Docker workspace restore is disabled; safe reuse "
                "requires controller-attested container recreation (job %s)",
                job_id,
            )
            return False

        if not ws_ctx and vm_ctx:
            await self._db.merge_vm_context(job_id, {"status": "restoring"})

        restored_runtime: str | None = None
        restore_work: dict[str, Any] | None = None
        restore_claimant: str | None = None
        restore_work_finished = False
        restore_owner: WorkspaceOwner | None = None
        try:
            ssh_host = None
            # Pod vs VM decides the extract scope below: a pod extract runs as
            # the unprivileged agent-host user and must NOT try to overwrite the
            # image-provided, root-owned /usr/local (scoped_home), while a VM
            # extract (root, the snapshot IS the disk) restores the whole tree.
            restoring_vm = bool(
                vm_ctx and self._vm_provisioner and self._vm_provisioner.is_available
            )

            if restoring_vm:
                # VM: create a fresh VM
                ok = await self._vm_provisioner.create_vm(job_id)
                if not ok:
                    logger.error("Failed to create VM for restore of job %s", job_id)
                    await self._db.merge_vm_context(
                        job_id,
                        {"status": "failed", "error": "VM creation failed on restore"},
                    )
                    return False
                if vm_ctx.get("rootdisk") == "kept":
                    logger.info(
                        "VM restore for job %s is reusing its kept rootdisk; "
                        "readiness is owned by the VM prober",
                        job_id,
                    )
                    return True
                error_msg = (
                    "VM restore without a kept rootdisk requires a post-readiness "
                    "snapshot extract (unsupported in same-cluster mode v1)"
                )
                logger.error("%s (job %s)", error_msg, job_id)
                await self._db.merge_vm_context(
                    job_id, {"status": "failed", "error": error_msg}
                )
                return False

            else:
                # K8s restore starts from one exact, settled suspension of A.
                # No ``restoring`` write is made over A: creation reserves and
                # publishes a distinct B, and only B may receive extraction
                # status below.
                owner = WorkspaceOwner.job(job_id)
                retired_runtime: str | None = None
                creation: dict[str, Any] | None = None
                if ws_ctx.get("status") == "suspended":
                    retired_runtime = _captured_workspace_runtime(ws_ctx)
                    if (
                        retired_runtime is None
                        or ws_ctx.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY)
                        is not True
                    ):
                        return False
                    suspension = await self._container_provisioner.get_settled_workspace_suspension(
                        owner,
                        expected_runtime_incarnation=retired_runtime,
                    )
                    if not isinstance(suspension, dict):
                        return False
                    try:
                        operation_id = str(UUID(str(suspension["id"])))
                    except (KeyError, TypeError, ValueError):
                        return False
                    try:
                        await self._container_provisioner.create_workspace(
                            owner,
                            operation_kind="restore",
                            operation_id=operation_id,
                        )
                    except Exception:
                        # The response may be lost after the reservation
                        # settled. Read the durable exact result before deciding
                        # failure.
                        logger.warning(
                            "Workspace restore create response was ambiguous for "
                            "job %s",
                            job_id,
                        )
                    creation = (
                        await self._container_provisioner.get_workspace_creation_result(
                            owner,
                            operation_kind="restore",
                            operation_id=operation_id,
                        )
                    )
                else:
                    # A retry after process restart sees B, not retired A.  It
                    # may continue only when B's current projection names the
                    # exact settled restore reservation.
                    creation = await self._container_provisioner.get_current_workspace_creation_result(
                        owner,
                        operation_kind="restore",
                    )
                job = await self._db.get_job(job_id)
                if not job:
                    return False
                current_ctx = job.get("context") or {}
                if isinstance(current_ctx, str):
                    try:
                        current_ctx = json.loads(current_ctx)
                    except (json.JSONDecodeError, TypeError):
                        return False
                ws_ctx = current_ctx.get("workspace_container", {})
                restored_runtime = _settled_restore_runtime(
                    ws_ctx,
                    creation,
                    retired_runtime=retired_runtime,
                )
                if restored_runtime is None:
                    logger.error("Failed to create pod for restore of job %s", job_id)
                    return False
                ssh_host = ws_ctx.get("pod_ip")

            if not restoring_vm:
                restore_owner = WorkspaceOwner.job(job_id)
                claimed = await self._claim_workspace_restore_work(
                    restore_owner,
                    runtime_incarnation=str(restored_runtime),
                )
                if claimed is None:
                    return False
                restore_claimant, restore_work = claimed

            try:
                heartbeat = (
                    self._workspace_restore_heartbeat(
                        restore_owner,
                        restore_work=restore_work,
                        claimant=restore_claimant,
                    )
                    if restore_owner is not None
                    and restore_work is not None
                    and restore_claimant is not None
                    else None
                )
                if heartbeat is None:
                    # VM restore paths retain their existing non-Kubernetes
                    # authority and never reach exact-B work leasing.
                    return False
                async with heartbeat:
                    attestation = await self._attest_claimed_workspace_restore_target(
                        restore_owner,
                        runtime_incarnation=str(restored_runtime),
                        restore_work=restore_work,
                        claimant=restore_claimant,
                    )

                    async def mutation_authority() -> WorkspaceRuntimeAttestation:
                        return await self._attest_claimed_workspace_restore_target(
                            restore_owner,
                            runtime_incarnation=str(restored_runtime),
                            restore_work=restore_work,
                            claimant=restore_claimant,
                        )

                    ssh_host = attestation.pod_ip
                    if not ssh_host:
                        error_msg = "no SSH host after provisioning for restore"
                        logger.error("%s (job %s)", error_msg, job_id)
                        await self._db.merge_workspace_container_context_if_runtime(
                            job_id,
                            {"status": "failed", "error": error_msg},
                            expected_runtime_incarnation=restored_runtime,
                        )
                        return False

                    # Extract snapshot into the workspace. A failed extract is a
                    # failed restore: the workspace is empty or half-populated.
                    ssh_port = attestation.port
                    if not await self._extract_snapshot(
                        job_id,
                        ssh_host,
                        ssh_port=ssh_port,
                        scoped_home=True,
                        expected_host_key_fingerprint=(
                            attestation.ssh_host_key_fingerprint
                        ),
                        mutation_authority=mutation_authority,
                    ):
                        error_msg = "snapshot extraction failed on restore"
                        logger.error("%s (job %s)", error_msg, job_id)
                        await self._db.merge_workspace_container_context_if_runtime(
                            job_id,
                            {"status": "failed", "error": error_msg},
                            expected_runtime_incarnation=restored_runtime,
                        )
                        return False

                    # Re-attest the complete endpoint/key identity and re-lock
                    # the work token after the last byte, then retain the
                    # cheap UID liveness probe immediately before settlement.
                    if await mutation_authority() != attestation:
                        return False
                    exact_live = await self._container_provisioner.workspace_pod_live(
                        restore_owner,
                        expected_runtime_incarnation=restored_runtime,
                    )
                    if exact_live is not True:
                        return False
            except (RestoreWorkLeaseLost, WorkspaceRuntimeAuthorityError):
                logger.warning(
                    "Workspace restore-work authority changed for job %s", job_id
                )
                return False

            if not await self._complete_workspace_restore_work(
                restore_owner,
                restore_work=restore_work,
                claimant=restore_claimant,
            ):
                return False
            restore_work_finished = True

            logger.info(
                "Workspace restored from S3 for job %s (ssh_host=%s)",
                job_id,
                ssh_host,
            )
            return True

        except Exception:
            logger.exception("Failed to restore workspace for job %s", job_id)
            if not ws_ctx and vm_ctx:
                await self._db.merge_vm_context(
                    job_id, {"status": "failed", "error": "restore exception"}
                )
            elif restored_runtime is not None:
                await self._db.merge_workspace_container_context_if_runtime(
                    job_id,
                    {"status": "failed", "error": "restore exception"},
                    expected_runtime_incarnation=restored_runtime,
                )
            return False
        finally:
            if (
                not restore_work_finished
                and restore_owner is not None
                and restore_work is not None
                and restore_claimant is not None
            ):
                await self._release_workspace_restore_work(
                    restore_owner,
                    restore_work=restore_work,
                    claimant=restore_claimant,
                )

    async def _finish_container_thread_restore(
        self,
        thread_id: str,
        *,
        owner: WorkspaceOwner,
        restored_runtime: str,
        ws_ctx: dict[str, Any],
        strict_authority: _StrictSessionRestoreAuthority | None,
        strict_terminal_snapshot: bool,
        volume_reattached: bool,
        reattach_reason: str,
    ) -> bool:
        """Run and atomically settle exact-B thread restore effects.

        Creation settlement proves which Pod is B; this second durable lease
        serializes snapshot extraction across orchestrator replicas. Its
        heartbeat spans every unbounded remote effect, and final Ready
        publication consumes the same claim token. Strict stateless restores
        additionally retain the full binding/endpoint/fingerprint tuple.
        """

        claimed = await self._claim_workspace_restore_work(
            owner,
            runtime_incarnation=restored_runtime,
        )
        if claimed is None:
            return False
        claimant, restore_work = claimed
        restore_work_finished = False
        try:
            ssh_host = (
                strict_authority.ssh_host
                if strict_authority is not None
                else ws_ctx.get("pod_ip")
            )
            if not isinstance(ssh_host, str) or not ssh_host:
                if not strict_terminal_snapshot:
                    await self._db.merge_thread_workspace_context_if_runtime(
                        thread_id,
                        {
                            "status": "failed",
                            "error": "no SSH host after provisioning for restore",
                        },
                        expected_runtime_incarnation=restored_runtime,
                    )
                return False
            ssh_port = (
                strict_authority.ssh_port
                if strict_authority is not None
                else _resolve_ssh_port(ws_ctx, {})
            )

            try:
                async with self._workspace_restore_heartbeat(
                    owner,
                    restore_work=restore_work,
                    claimant=claimant,
                ):
                    attestation = await self._attest_claimed_workspace_restore_target(
                        owner,
                        runtime_incarnation=restored_runtime,
                        restore_work=restore_work,
                        claimant=claimant,
                    )

                    async def mutation_authority() -> WorkspaceRuntimeAttestation:
                        return await self._attest_claimed_workspace_restore_target(
                            owner,
                            runtime_incarnation=restored_runtime,
                            restore_work=restore_work,
                            claimant=claimant,
                        )

                    if strict_authority is not None and not (
                        attestation.runtime_incarnation
                        == strict_authority.runtime_incarnation
                        and attestation.backing_id == strict_authority.backing_id
                        and attestation.pod_ip == strict_authority.ssh_host
                        and attestation.port == strict_authority.ssh_port
                        and attestation.ssh_host_key_fingerprint
                        == strict_authority.host_key_fingerprint
                    ):
                        raise WorkspaceRuntimeAuthorityError(
                            "strict workspace restore attestation changed"
                        )
                    ssh_host = attestation.pod_ip
                    ssh_port = attestation.port
                    if volume_reattached:
                        logger.info(
                            "Workspace restore for thread %s rides the reattached "
                            "volume (%s) — no snapshot extract",
                            thread_id,
                            reattach_reason,
                        )
                    else:
                        extract_kwargs: dict[str, Any] = {
                            "scoped_home": True,
                            "expected_host_key_fingerprint": (
                                attestation.ssh_host_key_fingerprint
                            ),
                            "mutation_authority": mutation_authority,
                        }
                        if strict_authority is not None:
                            extract_kwargs = {
                                "expected_host_key_fingerprint": (
                                    strict_authority.host_key_fingerprint
                                ),
                                "mutation_authority": mutation_authority,
                                "remote_cmd": EXTRACT_HOME_REMOTE_CMD,
                                "require_pipefail": True,
                            }
                        extracted = await self._extract_snapshot(
                            thread_id,
                            ssh_host,
                            ssh_port=ssh_port,
                            entity_type="threads",
                            **extract_kwargs,
                        )
                        if not extracted:
                            logger.error(
                                "snapshot extraction failed on restore (thread %s)",
                                thread_id,
                            )
                            if not strict_terminal_snapshot:
                                await (
                                    self._db.merge_thread_workspace_context_if_runtime(
                                        thread_id,
                                        {
                                            "status": "failed",
                                            "error": (
                                                "snapshot extraction failed on restore"
                                            ),
                                        },
                                        expected_runtime_incarnation=restored_runtime,
                                    )
                                )
                            return False

                    if await mutation_authority() != attestation:
                        return False
                    exact_live = await self._container_provisioner.workspace_pod_live(
                        owner,
                        expected_runtime_incarnation=restored_runtime,
                    )
                    if exact_live is not True:
                        return False
            except (RestoreWorkLeaseLost, WorkspaceRuntimeAuthorityError):
                logger.warning(
                    "Workspace restore-work authority changed for thread %s",
                    thread_id,
                )
                return False

            if strict_authority is not None:
                for _attempt in range(2):
                    try:
                        completed = await self._container_provisioner.complete_strict_thread_restore_work(
                            owner,
                            restore_work=restore_work,
                            claimant=claimant,
                            workspace_generation=(
                                strict_authority.workspace_generation
                            ),
                            endpoint_generation=(strict_authority.endpoint_generation),
                            backing_id=strict_authority.backing_id,
                            host_key_fingerprint=(
                                strict_authority.host_key_fingerprint
                            ),
                            pod_ip=strict_authority.ssh_host,
                            port=strict_authority.ssh_port,
                            expected_workspace_status=(
                                strict_authority.workspace_status
                            ),
                        )
                    except Exception:
                        logger.warning(
                            "Strict thread restore completion response was "
                            "ambiguous for %s",
                            thread_id,
                        )
                        continue
                    if completed:
                        restore_work_finished = True
                        break
                if not restore_work_finished:
                    return False
            else:
                if not await self._complete_workspace_restore_work(
                    owner,
                    restore_work=restore_work,
                    claimant=claimant,
                ):
                    return False
                restore_work_finished = True

            logger.info(
                "%s workspace restored for thread %s from %s (runtime=%s)",
                "Strict terminal" if strict_authority is not None else "Pinned",
                thread_id,
                "its own volume" if volume_reattached else "S3",
                restored_runtime,
            )
            return True
        finally:
            if not restore_work_finished:
                await self._release_workspace_restore_work(
                    owner,
                    restore_work=restore_work,
                    claimant=claimant,
                )

    async def _commit_strict_thread_restore_ready(
        self,
        thread_id: str,
        expected: _StrictSessionRestoreAuthority,
    ) -> bool:
        """CAS one exact restored runtime from quarantined to Ready.

        The Kubernetes UID probe happens immediately before this call. The row
        lock then re-reads the complete binding/runtime/endpoint tuple, and the
        guarded UPDATE clears restore intent in the same transaction which
        publishes Ready. A concurrent replacement or lifecycle transition
        therefore wins cleanly and this restore remains quarantined.
        """

        restored_at = datetime.now(timezone.utc).isoformat()
        async with self._db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT id::text AS id, status::text AS status, "
                    "execution_lane, metadata FROM threads "
                    "WHERE id = $1::uuid FOR UPDATE",
                    thread_id,
                )
                if (
                    row is None
                    or str(row.get("execution_lane") or "") != "stateless"
                    or str(row.get("status") or "")
                    not in {"created", "active", "awaiting_user"}
                ):
                    return False
                try:
                    current = _strict_session_restore_authority(dict(row))
                except RuntimeError:
                    return False
                if current != expected:
                    return False

                updated = await conn.fetchval(
                    f"""
                    UPDATE threads
                    SET metadata = jsonb_set(
                            metadata,
                            '{{workspace_container}}',
                            (metadata->'workspace_container') ||
                                jsonb_build_object(
                                    'status', 'ready',
                                    'restored_at', $9::text,
                                    '{WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY}',
                                    false
                                ),
                            false
                        ),
                        last_activity = CURRENT_TIMESTAMP
                    WHERE id = $1::uuid
                      AND execution_lane = 'stateless'
                      AND status IN ('created', 'active', 'awaiting_user')
                      AND metadata #>
                            '{{workspace_container,{WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY}}}'
                            = 'true'::jsonb
                      AND metadata #>> '{{_workspace_binding,kind}}' = 'remote'
                      AND metadata #>> '{{_workspace_binding,generation}}' = $2::text
                      AND metadata #>> '{{workspace_container,{CANVAS_WORKSPACE_GENERATION_KEY}}}'
                            = $3::text
                      AND metadata #>> '{{workspace_container,{WORKSPACE_RUNTIME_INCARNATION_KEY}}}'
                            = $4::text
                      AND metadata #>> '{{_workspace_binding,backing_id}}' = $5::text
                      AND metadata #>>
                            '{{_workspace_binding,ssh_host_key_fingerprint}}'
                            = $6::text
                      AND metadata #>> '{{workspace_container,pod_ip}}' = $7::text
                      AND metadata #> '{{workspace_container,port}}'
                            = to_jsonb($8::integer)
                      AND metadata #>> '{{workspace_container,status}}' = $10::text
                    RETURNING id
                    """,
                    thread_id,
                    expected.workspace_generation,
                    expected.endpoint_generation,
                    expected.runtime_incarnation,
                    expected.backing_id,
                    expected.host_key_fingerprint,
                    expected.ssh_host,
                    expected.ssh_port,
                    restored_at,
                    expected.workspace_status,
                )
                return updated is not None

    async def _extract_snapshot(
        self,
        entity_id: str,
        ssh_host: str,
        ssh_port: int = 22,
        entity_type: str = "jobs",
        *,
        scoped_home: Optional[bool] = None,
        expected_host_key_fingerprint: Optional[str] = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
        remote_cmd: Optional[str] = None,
        require_pipefail: bool = False,
    ) -> bool:
        """Download snapshot from S3 and extract into the pod via SSH.

        Mirrors ide_session.py:_extract_snapshot_to_vm (lines 782-836).

        ``scoped_home`` selects the extract command. A snapshot carries both
        ``/home/agent-host`` and ``/usr/local``; for a **container/pod** target
        the extract runs as the unprivileged ``agent-host`` user, which cannot
        overwrite the image-provided, root-owned ``/usr/local`` files — the full
        extract then exits rc=2 and the restore is (correctly) reported failed,
        so pod restore-from-S3 silently never worked. Pods pass
        ``scoped_home=True`` to extract only ``home/agent-host`` (the pod image
        already supplies ``/usr/local``), matching the proven
        ``ide_session`` k8s-pod path. VMs (root, the snapshot IS the disk) keep
        the full extract. See knowledge-base/knowledge/features/workspace_durability_tiering.md §C1.

        Returns:
            True when the snapshot was downloaded AND unpacked cleanly; False
            when the download failed or tar exited non-zero. Callers MUST NOT
            report the workspace as restored on False — this used to return
            None on every path, so a failed restore still stamped
            ``status: "ready"`` and the agent went to work on an empty or
            half-populated tree.
        """
        with tempfile.NamedTemporaryFile(
            suffix=".tar.zst", delete=True, prefix="restore_"
        ) as tmp:
            tar_path = tmp.name

            ok = await self._snapshot_service.download_snapshot(
                entity_id,
                tar_path,
                entity_type=entity_type,
                require_strict_terminal=require_pipefail,
            )
            if not ok:
                logger.warning(
                    "Failed to download snapshot for %s %s",
                    entity_type.rstrip("s"),
                    entity_id,
                )
                return False

            # The download is local. Freshly attest and re-lock exact B at
            # the boundary where archive bytes can leave for a reusable IP.
            if mutation_authority is not None:
                attestation = await mutation_authority()
                ssh_host = attestation.pod_ip
                ssh_port = attestation.port
                expected_host_key_fingerprint = attestation.ssh_host_key_fingerprint

            key_path = resolve_ssh_key_path()
            if not key_path:
                logger.warning(
                    "No SSH key available for snapshot extraction (%s %s)",
                    entity_type.rstrip("s"),
                    entity_id,
                )
            extract_kwargs: dict[str, Any] = {
                "key_path": key_path,
                "require_pipefail": require_pipefail,
            }
            if expected_host_key_fingerprint is not None:
                extract_kwargs.update(
                    {
                        "expected_host_key_fingerprint": (
                            expected_host_key_fingerprint
                        ),
                    }
                )
            if remote_cmd is not None:
                extract_kwargs["remote_cmd"] = remote_cmd
            elif scoped_home is not None:
                extract_kwargs["remote_cmd"] = (
                    EXTRACT_HOME_REMOTE_CMD if scoped_home else EXTRACT_REMOTE_CMD
                )
            rc, stderr = await stream_extract_snapshot(
                ssh_host,
                ssh_port,
                tar_path,
                **extract_kwargs,
            )

            if rc != 0:
                logger.warning(
                    "Snapshot extraction had errors for %s %s (rc=%d): %s",
                    entity_type.rstrip("s"),
                    entity_id,
                    rc,
                    stderr.decode(errors="replace")[:500],
                )
                return False

            return True

    async def restore(
        self,
        owner: WorkspaceOwner,
        *,
        expected_runtime_incarnation: str | None = None,
        stateless_creation_generation: str | None = None,
        allow_stateless_create: bool = False,
        wake_operation_id: str | None = None,
        _pinned_runtime_lock_held: bool = False,
    ) -> bool:
        """Owner-keyed restore: job -> restore_workspace, session -> restore_thread_workspace."""
        if owner.kind == "job":
            if (
                expected_runtime_incarnation is not None
                or stateless_creation_generation is not None
                or allow_stateless_create
                or wake_operation_id is not None
            ):
                return False
            return await self.restore_workspace(owner.id)
        if (
            expected_runtime_incarnation is None
            and stateless_creation_generation is None
            and not allow_stateless_create
        ):
            wake_kwargs = (
                {"wake_operation_id": wake_operation_id}
                if wake_operation_id is not None else {}
            )
            if not _pinned_runtime_lock_held:
                return await self.restore_thread_workspace(
                    owner.id, **wake_kwargs,
                )
            return await self.restore_thread_workspace(
                owner.id, **wake_kwargs,
                _pinned_runtime_lock_held=True,
            )
        if wake_operation_id is not None:
            return False
        if stateless_creation_generation is None and not allow_stateless_create:
            kwargs: dict[str, Any] = {
                "expected_runtime_incarnation": expected_runtime_incarnation
            }
            if _pinned_runtime_lock_held:
                kwargs["_pinned_runtime_lock_held"] = True
            return await self.restore_thread_workspace(owner.id, **kwargs)
        return await self.restore_thread_workspace(
            owner.id,
            expected_runtime_incarnation=expected_runtime_incarnation,
            stateless_creation_generation=stateless_creation_generation,
            allow_stateless_create=allow_stateless_create,
            _pinned_runtime_lock_held=_pinned_runtime_lock_held,
        )

    # =========================================================================
    # Thread suspension (mirrors job suspension for persistent agent threads)
    # =========================================================================

    async def suspend_thread_workspace(self, thread_id: str) -> bool:
        """Capture thread workspace snapshot to S3, then tear down.

        VM workspaces retain the existing snapshot → delete flow. Kubernetes
        capture is temporarily refused before I/O because no durable
        pre-delete capture authority exists yet.

        Returns True if snapshot + teardown succeeded. Failures before deletion
        restore ``ready`` only while the captured runtime is still current;
        stale or already-deleted runtimes never project over a successor.
        """
        if not self.is_enabled or not self._db:
            return False

        thread = await self._db.get_thread(thread_id)
        if not thread:
            return False

        # Stateless session teardown is owned by the acknowledged retirement
        # protocol.  The legacy idle-suspension path has no claim/incarnation
        # authority and must never race that protocol by snapshotting or
        # deleting its physical workspace (including an already-ended thread
        # whose retirement marker is still pending).
        if thread.get("execution_lane") == "stateless":
            logger.info(
                "Legacy workspace suspension refused for stateless thread %s",
                thread_id,
            )
            return False

        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (ValueError, TypeError):
                metadata = {}
        ws_ctx = metadata.get("workspace_container", {})
        vm_ctx = metadata.get("vm", {})
        provisioner_type = ws_ctx.get("provisioner")
        if provisioner_type == "docker":
            logger.warning(
                "Static Docker workspace suspension is disabled; safe reuse "
                "requires controller-attested container recreation (thread %s)",
                thread_id,
            )
            return False

        # Resolve the tier ONCE, explicitly. Everything below keys off this
        # instead of asking whether workspace_container happens to exist.
        is_vm = _thread_is_vm_tier(metadata, ws_ctx, vm_ctx)

        if not is_vm:
            # Temporary containment: neither a raw Pod IP nor stable Service
            # DNS proves that reads still target this thread's exact runtime A.
            # The existing cleanup intent begins after capture, so it cannot
            # fence a crash/replacement during snapshot/profile reads.  Keep
            # the Kubernetes workspace live and untouched until a durable,
            # renewable pre-delete capture authority exists.
            logger.warning(
                "Kubernetes thread suspension capture is disabled pending "
                "durable exact-runtime capture authority (thread %s)",
                thread_id,
            )
            return False

        ws_status = vm_ctx.get("status")
        if ws_status in ("suspended", "suspending"):
            # A concurrent/earlier suspend already handled (or is handling)
            # this thread. Returning False here made the caller's fallback
            # path log "suspend unavailable or failed" and delete the agent
            # pod a second time right after a successful suspend
            # (knowledge-base/knowledge/issues/session_silent_failure_audit.md #13).
            logger.info(
                "Workspace for thread %s already %s — skipping duplicate suspend",
                thread_id,
                ws_status,
            )
            return True
        if ws_status != "ready":
            return False

        authority = await self._capture_vm_capture_authority(
            thread_id,
            entity_type="thread",
        )
        if authority is None:
            return False
        vm_identity, vm_attestation = authority

        # Resolve from the fresh attestation only. A VM-assigned thread can
        # retain stale Ready container residue; no raw metadata endpoint is an
        # external-read authority.
        ssh_host = vm_attestation.host
        ssh_port = vm_attestation.port
        fingerprint = vm_attestation.ssh_host_key_fingerprint

        if not await self._db.merge_thread_vm_context_if_provision_generation(
            thread_id,
            vm_identity.provision_generation,
            {"status": "suspending", "_suspend_remote_io_closed": None},
        ):
            return False
        from orchestrator.services.vm_remote_operation import (
            VMRemoteOperationLeaseLost,
            VMRemoteOperationUnavailable,
            claim_vm_remote_operation,
        )

        try:
            vm_lease = await claim_vm_remote_operation(
                db=self._db,
                provisioner=self._vm_provisioner,
                owner_id=thread_id,
                owner_kind="thread",
                operation_kind="snapshot_capture",
                lease_seconds=300,
            )
        except VMRemoteOperationUnavailable:
            await self._db.merge_thread_vm_context_if_provision_generation(
                thread_id,
                vm_identity.provision_generation,
                {"status": "ready", "_suspend_remote_io_closed": None},
            )
            return False

        ssh_host = vm_lease.identity.ssh_host
        ssh_port = vm_lease.identity.ssh_port
        fingerprint = vm_lease.identity.ssh_host_key_fingerprint

        async def capture_target() -> tuple[str, int, str] | None:
            return await vm_lease.revalidate()

        async def snapshot_authority() -> bool:
            return await capture_target() is not None

        async def capture_snapshot() -> bool:
            return await self._snapshot_service.capture_vm_snapshot(
                job_id=thread_id,
                ssh_host=ssh_host,
                ssh_port=int(ssh_port),
                source_type="vm",
                entity_type="threads",
                expected_host_key_fingerprint=fingerprint,
                capture_authority=snapshot_authority,
            )

        try:
            # Slice C (design §5): stage the protected session's upperdir
            # diff to S3 BEFORE the teardown snapshot — this is the last
            # chance to capture it while the overlay is still mounted.
            # ``metadata`` here is already the parsed dict (str-JSON handled
            # above), so reuse it rather than re-reading + re-parsing
            # ``thread["metadata"]``. Best-effort only: a staging failure
            # must never block or fail the teardown — the VM snapshot below
            # is the durable, load-bearing path and always still runs.
            if metadata.get("protected_cloud"):
                # Protected-cloud staging has its own multi-step signature,
                # tar, S3 and DB settlement protocol.  It cannot borrow this
                # one-shot suspension attestation safely, so leave it live for
                # the dedicated durable capture-authority follow-up rather
                # than dialing a raw/reused VM endpoint here.
                logger.warning(
                    "teardown cloud-stage skipped for %s: durable VM capture "
                    "authority is unavailable",
                    thread_id,
                )

            async with vm_lease:
                if not (
                    vm_identity.provision_generation
                    == vm_lease.identity.workspace_generation
                    and vm_identity.vm_uid == vm_lease.identity.vm_uid
                    and vm_identity.ssh_host == ssh_host
                    and vm_identity.ssh_port == ssh_port
                    and vm_identity.ssh_host_key_fingerprint == fingerprint
                    and vm_attestation.runtime_incarnation
                    == vm_lease.identity.launcher_pod_uid
                ):
                    raise VMRemoteOperationLeaseLost(
                        "thread VM suspension target changed before snapshot"
                    )
                ok = await capture_snapshot()
                if await capture_target() is None:
                    raise VMRemoteOperationLeaseLost(
                        "VM authority changed during thread suspension snapshot"
                    )
                if not await self._db.merge_thread_vm_context_if_provision_generation(
                    thread_id,
                    vm_identity.provision_generation,
                    {"_suspend_remote_io_closed": str(vm_lease.receipt["id"])},
                ):
                    raise VMRemoteOperationLeaseLost(
                        "thread VM suspension could not close subsequent remote I/O"
                    )
            # With a persistent rootdisk the VM's disk outlives the VM, so the
            # snapshot stops being the only copy of the workspace and stops
            # being a precondition for tearing down. That matters because for a
            # VM the capture can never succeed from here: it SSHes from the
            # orchestrator, and a VM workspace is only reachable over the
            # tailnet the orchestrator has no route to. Fail-closed on a gate
            # that always fails is why VM sessions never suspended at all.
            # A pod has no disk to keep, so it stays fail-closed.
            disk_survives_teardown = vm_persistent_rootdisk_enabled()

            if not ok:
                if not disk_survives_teardown:
                    logger.warning(
                        "Snapshot capture failed for thread %s — keeping workspace alive",
                        thread_id,
                    )
                    await self._db.merge_thread_vm_context_if_provision_generation(
                        thread_id,
                        vm_identity.provision_generation,
                        {"status": "ready", "_suspend_remote_io_closed": None},
                    )
                    return False
                logger.info(
                    "Snapshot capture unavailable for thread %s — suspending anyway; "
                    "the persistent rootdisk carries the workspace across teardown",
                    thread_id,
                )

            # Tear down based on provisioner type
            suspended_ctx: dict[str, Any] = {
                "status": "suspended",
                "suspended_at": datetime.now(timezone.utc).isoformat(),
            }
            suspended_ctx["_suspend_remote_io_closed"] = str(vm_lease.receipt["id"])

            if self._vm_provisioner and self._vm_provisioner.is_available:
                cleanup = await acquire_vm_cleanup_permit(
                    self._workspace_recovery_store,
                    owner_kind="thread",
                    owner_id=thread_id,
                    identity=vm_identity,
                    source="thread_workspace_suspension",
                    purge_disk=False,
                )
                if not cleanup.allowed:
                    raise RuntimeError(
                        "thread VM suspension held for workspace recovery"
                    )
                replayed = completed_cleanup_outcome(cleanup)
                if replayed is not None:
                    outcome = VMTeardownResult(replayed, replayed == "completed")
                else:
                    outcome = await self._vm_provisioner.release_vm_captured(
                        thread_id,
                        vm_identity,
                        entity_type="thread",
                        # Soft End is resumable. Even when the snapshot succeeded,
                        # deleting the backing disk here would turn a later restore
                        # failure into irreversible data loss. Permanent End owns
                        # the only purge_disk=True path.
                        purge_disk=False,
                        capture_snapshot=False,
                    )
                    if outcome.disposition in {"completed", "identity_superseded"}:
                        await complete_vm_cleanup_permit(
                            self._workspace_recovery_store,
                            cleanup,
                            outcome=outcome.disposition,
                        )
                if (
                    not isinstance(outcome, VMTeardownResult)
                    or outcome.disposition != "completed"
                ):
                    raise RuntimeError(
                        "thread VM suspension process-zero retirement is incomplete"
                    )
                # Optimistic; the vm.lifecycle.status handler overwrites it
                # with what the controller actually did. Restore reads it to
                # decide whether to extract a snapshot over the disk.
                suspended_ctx["rootdisk"] = "kept"
                if not await self._db.merge_thread_vm_context_if_provision_generation(
                    thread_id,
                    vm_identity.provision_generation,
                    suspended_ctx,
                ):
                    raise RuntimeError(
                        "thread VM suspension successor replaced the target"
                    )
            else:
                raise RuntimeError("thread VM suspension provisioner is unavailable")

            # The workspace snapshot/retirement owns no immutable agent-Pod
            # identity. Pod teardown is performed by the caller's durable
            # pinned-thread retirement generation. A legacy caller without
            # that authority leaks the old agent safely instead of deleting a
            # same-name successor selected by labels.

            logger.info("Workspace suspended to S3 for thread %s", thread_id)
            return True

        except Exception:
            logger.exception("Failed to suspend workspace for thread %s", thread_id)
            try:
                # A completed delete followed by a lost projection response
                # must never advertise the stopped guest as Ready. Restore
                # that state only from a fresh controller-attested exact live
                # VM/VMI/launcher and unchanged retained PVC.
                ready = await self._vm_provisioner.attest_workspace_runtime(
                    thread_id, entity_type="thread",
                )
                if (
                    ready.workspace_generation == vm_identity.provision_generation
                    and ready.vm_uid == vm_identity.vm_uid
                    and ready.rootdisk_pvc_uid == vm_identity.rootdisk_pvc_uid
                    and ready.vmi_uid == vm_attestation.vmi_uid
                    and ready.launcher_pod_uid
                        == vm_attestation.launcher_pod_uid
                ):
                    await self._db.merge_thread_vm_context_if_provision_generation(
                        thread_id, vm_identity.provision_generation,
                        {"status": "ready", "_suspend_remote_io_closed": None},
                    )
            except Exception:
                pass
            return False

    async def restore_thread_workspace(
        self,
        thread_id: str,
        *,
        expected_runtime_incarnation: str | None = None,
        stateless_creation_generation: str | None = None,
        allow_stateless_create: bool = False,
        wake_operation_id: str | None = None,
        _pinned_runtime_lock_held: bool = False,
    ) -> bool:
        """Provision a fresh workspace and extract the S3 snapshot into it.

        Dispatches to the correct provisioner based on workspace metadata.

        Returns True if provisioning + snapshot extraction succeeded.
        """
        if not self._db or (
            wake_operation_id is None and not self.is_enabled
        ):
            return False

        thread = await self._db.get_thread(thread_id)
        if not thread:
            return False

        if thread.get("execution_lane") == "pinned" and not _pinned_runtime_lock_held:
            lock_impl = getattr(type(self._db), "thread_advisory_lock", None)
            if callable(lock_impl):
                async with lock_impl(self._db, thread_id) as lock_acquired:
                    if lock_acquired is not True:
                        logger.error(
                            "Refusing pinned workspace restore without owning "
                            "the runtime advisory lock (thread %s)",
                            thread_id,
                        )
                        return False
                    return await self.restore_thread_workspace(
                        thread_id,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                        stateless_creation_generation=stateless_creation_generation,
                        allow_stateless_create=allow_stateless_create,
                        wake_operation_id=wake_operation_id,
                        _pinned_runtime_lock_held=True,
                    )
            logger.error(
                "Refusing pinned workspace restore without the runtime "
                "advisory-lock implementation (thread %s)",
                thread_id,
            )
            return False

        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (ValueError, TypeError):
                metadata = {}

        ws_ctx = metadata.get("workspace_container", {})
        vm_ctx = metadata.get("vm", {})
        provisioner_type = ws_ctx.get("provisioner")
        if provisioner_type == "docker":
            logger.warning(
                "Static Docker workspace restore is disabled; safe reuse "
                "requires controller-attested container recreation (thread %s)",
                thread_id,
            )
            return False

        # Same explicit tier read as suspend — presence of workspace_container
        # says nothing about the tier (see _thread_is_vm_tier).
        is_vm = _thread_is_vm_tier(metadata, ws_ctx, vm_ctx)

        # Read BEFORE provisioning: vm_ctx is re-read from fresh metadata after
        # create_thread_vm, by which point this key reflects the new VM's
        # lifecycle rather than the suspend that put the thread here.
        rootdisk_kept = vm_ctx.get("rootdisk") == "kept"
        if (
            rootdisk_kept and thread.get("execution_lane") == "pinned"
            and callable(getattr(type(self._db), "fetchval", None))
        ):
            open_idle = await self._db.fetchval(
                "SELECT id FROM vm_idle_operations WHERE owner_kind='thread' "
                "AND owner_id=$1::uuid AND closed_at IS NULL", thread_id,
            )
            if open_idle is not None and str(open_idle) != wake_operation_id:
                return False
        if wake_operation_id is not None and (
            not is_vm or not rootdisk_kept
            or thread.get("execution_lane") != "pinned"
        ):
            return False

        # Same "read it first" rule, container tier: create_workspace rebinds
        # _workspace_binding to whatever the new pod mounts, so the identity of
        # the volume we are coming BACK to only exists before that call.
        prior_backing_id = _workspace_backing_id(metadata)

        # A stateless sandbox resume after terminal retirement is a stricter
        # protocol than legacy idle-suspension restore. The exact-true marker
        # is its durable proof that an S3 extract (or same-PVC reattach) is
        # required before this endpoint may be published. Do not manufacture
        # that proof from truthiness or from entering this method.
        strict_terminal_snapshot = bool(
            thread.get("execution_lane") == "stateless" and not is_vm
        )
        if strict_terminal_snapshot:
            raw_restore_required = ws_ctx.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY)
            if raw_restore_required is not True:
                logger.error(
                    "Strict terminal restore refused without exact snapshot "
                    "intent (thread %s)",
                    thread_id,
                )
                return False
        elif expected_runtime_incarnation is not None:
            # Exact-runtime reuse is a stateless terminal-restore primitive;
            # legacy pinned/VM callers must not silently change semantics.
            return False
        if stateless_creation_generation is not None and not strict_terminal_snapshot:
            return False
        if allow_stateless_create and stateless_creation_generation is None:
            return False

        reuse_existing_runtime = expected_runtime_incarnation is not None
        if (
            strict_terminal_snapshot
            and not reuse_existing_runtime
            and stateless_creation_generation is None
        ):
            logger.error(
                "Strict terminal restore refused without durable create authority "
                "(thread %s)",
                thread_id,
            )
            return False
        if reuse_existing_runtime:
            current_runtime = ws_ctx.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
            if current_runtime != expected_runtime_incarnation:
                logger.error(
                    "Strict restore cached runtime changed for thread %s",
                    thread_id,
                )
                return False
            authority_probe = getattr(
                self._container_provisioner, "workspace_pod_authority", None
            )
            if not callable(authority_probe):
                return False
            try:
                authority = await authority_probe(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
            except Exception:
                logger.exception(
                    "Strict restore cached runtime probe failed for thread %s",
                    thread_id,
                )
                return False
            if authority != "exact_live":
                logger.error(
                    "Strict restore cached runtime is no longer exact-live for "
                    "thread %s",
                    thread_id,
                )
                return False

        restore_operation_id: str | None = None
        restore_creation: dict[str, Any] | None = None
        retired_runtime: str | None = None
        restored_runtime: str | None = None
        owner = WorkspaceOwner.session(thread_id)
        if not is_vm:
            if reuse_existing_runtime:
                restore_creation = await self._container_provisioner.get_current_workspace_creation_result(
                    owner,
                    operation_kind="restore",
                )
                restored_runtime = _settled_restore_runtime(ws_ctx, restore_creation)
                if restored_runtime != expected_runtime_incarnation:
                    return False
            else:
                retired_runtime = _captured_workspace_runtime(ws_ctx)
                if (
                    retired_runtime is None
                    or ws_ctx.get("status") != "suspended"
                    or ws_ctx.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY) is not True
                ):
                    return False
                suspension = (
                    await self._container_provisioner.get_settled_workspace_suspension(
                        owner,
                        expected_runtime_incarnation=retired_runtime,
                    )
                )
                if not isinstance(suspension, dict):
                    return False
                try:
                    restore_operation_id = str(UUID(str(suspension["id"])))
                except (KeyError, TypeError, ValueError):
                    return False

        try:
            ssh_host = None
            strict_authority: Optional[_StrictSessionRestoreAuthority] = None
            # Container tier only; a VM restore that reaches the extract below
            # never rides a kept disk (rootdisk_kept returned early).
            volume_reattached = False
            reattach_reason = ""

            if is_vm and self._vm_provisioner and self._vm_provisioner.is_available:
                generation = str(thread.get("runtime_generation") or "")
                try:
                    if generation != str(UUID(generation)):
                        return False
                except (TypeError, ValueError):
                    return False
                raw_vm = metadata.get("vm")
                if raw_vm is not None and not isinstance(raw_vm, dict):
                    return False
                options = await vm_provisioning_options(
                    self._db,
                    "Session",
                    thread,
                    fallback=metadata.get("config_override"),
                )
                if wake_operation_id is not None:
                    # Reattach the retained disk; first-create preparation
                    # and initialization are not replayed over user data.
                    options.pop("preparation", None)
                    options.pop("initialization", None)
                if wake_operation_id is not None:
                    options["wake_operation_id"] = wake_operation_id
                ok = await self._vm_provisioner.create_thread_vm(
                    thread_id,
                    **options,
                    expected_runtime_generation=generation,
                    expected_agent_id=(
                        str(thread["agent_id"])
                        if thread.get("agent_id") is not None
                        else None
                    ),
                    expected_attach_token=(
                        str(thread["runtime_attach_token"])
                        if thread.get("runtime_attach_token") is not None
                        else None
                    ),
                    expected_vm_context=(dict(raw_vm) if raw_vm is not None else None),
                )
                if not ok:
                    logger.error(
                        "Failed to create VM for restore of thread %s", thread_id
                    )
                    if wake_operation_id is None:
                        await self._db.merge_thread_vm_context(
                            thread_id,
                            {
                                "status": "failed",
                                "error": "VM creation failed on restore",
                            },
                        )
                    return False

                # Kept rootdisk: the restore IS the create. The reattached disk
                # already holds the workspace (extracting a snapshot over it
                # would replace newer files with older ones), and there is no
                # SSH to wait for — VM creation is async over NATS, so
                # ssh coordinates arrive minutes later via the daemon's
                # register, which also supplies the real 'ready'. The
                # container-era tail below demanded an ssh_host synchronously
                # and read its absence as failure, stamping a transient
                # vm.status='failed' that a declared-vm thread's attach poll
                # treats as fatal (live-gate finding, thread a1240add).
                if rootdisk_kept:
                    logger.info(
                        "VM restore for thread %s rides the kept rootdisk — no "
                        "snapshot extract; readiness arrives via the daemon",
                        thread_id,
                    )
                    return True

                thread = await self._db.get_thread(thread_id)
                metadata = thread.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (ValueError, TypeError):
                        metadata = {}
                vm_ctx = metadata.get("vm", {})
                ssh_host = vm_ctx.get("ssh_host")

            else:
                # K8s container (default)
                if not reuse_existing_runtime:
                    if thread.get("execution_lane") == "pinned":
                        ok = await self._container_provisioner.create_pinned_thread_workspace(
                            thread_id,
                            runtime_lock_held=True,
                        )
                        if not ok:
                            logger.error(
                                "Failed to create pinned workspace for thread %s",
                                thread_id,
                            )
                            return False
                    else:
                        create_kwargs: dict[str, Any] = {}
                        if stateless_creation_generation is not None:
                            create_kwargs = {
                                "stateless_creation_generation": (
                                    stateless_creation_generation
                                ),
                                "allow_stateless_create": allow_stateless_create,
                            }
                        try:
                            await self._container_provisioner.create_workspace(
                                owner,
                                operation_kind="restore",
                                operation_id=restore_operation_id,
                                **create_kwargs,
                            )
                        except Exception:
                            logger.warning(
                                "Workspace restore create response was ambiguous for "
                                "thread %s",
                                thread_id,
                            )
                        restore_creation = await self._container_provisioner.get_workspace_creation_result(
                            owner,
                            operation_kind="restore",
                            operation_id=str(restore_operation_id),
                        )
                        if (
                            not isinstance(restore_creation, dict)
                            or str(restore_creation.get("result_kind") or "")
                            != "settled"
                        ):
                            logger.error(
                                "Failed to create pod for restore of thread %s",
                                thread_id,
                            )
                            return False

                thread = await self._db.get_thread(thread_id)
                metadata = thread.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (ValueError, TypeError):
                        metadata = {}
                ws_ctx = metadata.get("workspace_container", {})
                if thread.get("execution_lane") != "pinned":
                    restored_runtime = _settled_restore_runtime(
                        ws_ctx,
                        restore_creation,
                        retired_runtime=retired_runtime,
                    )
                    if restored_runtime is None or (
                        reuse_existing_runtime
                        and restored_runtime != expected_runtime_incarnation
                    ):
                        logger.error(
                            "Strict restore runtime changed before extraction for "
                            "thread %s",
                            thread_id,
                        )
                        return False
                ssh_host = ws_ctx.get("pod_ip")
                volume_reattached, reattach_reason = _volume_survived_teardown(
                    prior_backing_id, _workspace_backing_id(metadata)
                )
                if strict_terminal_snapshot:
                    try:
                        strict_authority = _strict_session_restore_authority(thread)
                    except RuntimeError as exc:
                        logger.error(
                            "Strict terminal restore has no exact post-create "
                            "authority for thread %s: %s",
                            thread_id,
                            exc,
                        )
                        return False
                    ssh_host = strict_authority.ssh_host

                # K8s B now owns a durable post-create work lease. The helper
                # keeps that lease renewed through extraction and consumes it
                # in the same transaction that publishes Ready. VM restore
                # retains its independent lifecycle path below.
                if thread.get("execution_lane") != "pinned":
                    return await self._finish_container_thread_restore(
                        thread_id,
                        owner=owner,
                        restored_runtime=restored_runtime,
                        ws_ctx=ws_ctx,
                        strict_authority=strict_authority,
                        strict_terminal_snapshot=strict_terminal_snapshot,
                        volume_reattached=volume_reattached,
                        reattach_reason=reattach_reason,
                    )

            if not ssh_host:
                error_msg = "no SSH host after provisioning for restore"
                logger.error("%s (thread %s)", error_msg, thread_id)
                if is_vm:
                    await self._db.merge_thread_vm_context(
                        thread_id, {"status": "failed", "error": error_msg}
                    )
                elif restored_runtime is not None:
                    await self._db.merge_thread_workspace_context_if_runtime(
                        thread_id,
                        {"status": "failed", "error": error_msg},
                        expected_runtime_incarnation=restored_runtime,
                    )
                return False

            # Extract snapshot into the workspace — unless the storage that came
            # back already holds the live tree. Kept-rootdisk VM restores never
            # reach this point (they returned right after the create above), and
            # a PVC-backed session pod that REATTACHED its volume is that exact
            # situation one tier down: the volume survived the teardown, so
            # unrolling the older S3 tarball over it would replace newer files
            # with older ones. Everything else — emptyDir pods, a freshly minted
            # (empty) PVC, a volume discarded by the single-replica node-loss
            # fallback — has the S3 snapshot as its only copy and must extract.
            ssh_port = (
                strict_authority.ssh_port
                if strict_authority is not None
                else _resolve_ssh_port(ws_ctx, vm_ctx, is_vm=is_vm)
            )
            if volume_reattached:
                logger.info(
                    "Workspace restore for thread %s rides the reattached "
                    "volume (%s) — no snapshot extract",
                    thread_id,
                    reattach_reason,
                )
            else:
                extract_kwargs: dict[str, Any] = {"scoped_home": not is_vm}
                if strict_authority is not None:
                    extract_kwargs = {
                        "expected_host_key_fingerprint": (
                            strict_authority.host_key_fingerprint
                        ),
                        # The workspace image owns /usr/local as root. The
                        # stateless SSH principal restores only its durable
                        # home tree; VM/legacy restores retain the full-root
                        # command below.
                        "remote_cmd": EXTRACT_HOME_REMOTE_CMD,
                        "require_pipefail": True,
                    }
                extracted = await self._extract_snapshot(
                    thread_id,
                    ssh_host,
                    ssh_port=ssh_port,
                    entity_type="threads",
                    **extract_kwargs,
                )
                if not extracted:
                    # Same reasoning as the job path: a workspace that failed
                    # to restore must not advertise itself as ready. A strict
                    # restore leaves the exact-true intent armed; the internal
                    # credential gate consequently keeps this ready-looking
                    # provisioner row quarantined and the next ensure retries.
                    error_msg = "snapshot extraction failed on restore"
                    logger.error("%s (thread %s)", error_msg, thread_id)
                    if strict_terminal_snapshot:
                        return False
                    if is_vm:
                        await self._db.merge_thread_vm_context(
                            thread_id, {"status": "failed", "error": error_msg}
                        )
                    elif restored_runtime is not None:
                        await self._db.merge_thread_workspace_context_if_runtime(
                            thread_id,
                            {"status": "failed", "error": error_msg},
                            expected_runtime_incarnation=restored_runtime,
                        )
                    return False

            if strict_authority is not None:
                # Extraction success alone is not authority: the deterministic
                # Pod name may have been deleted/replaced while bytes streamed.
                # Re-probe the exact UID, then CAS the same binding/runtime/
                # endpoint tuple under the thread row lock before clearing the
                # restore marker and publishing Ready.
                probe = getattr(self._container_provisioner, "workspace_pod_live", None)
                if not callable(probe):
                    logger.error(
                        "Strict terminal restore cannot verify runtime UID for "
                        "thread %s",
                        thread_id,
                    )
                    return False
                try:
                    exact_live = await probe(
                        WorkspaceOwner.session(thread_id),
                        expected_runtime_incarnation=(
                            strict_authority.runtime_incarnation
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Strict terminal restore runtime re-probe failed for thread %s",
                        thread_id,
                    )
                    return False
                if exact_live is not True:
                    logger.error(
                        "Strict terminal restore runtime changed or is ambiguous "
                        "for thread %s",
                        thread_id,
                    )
                    return False
                if not await self._commit_strict_thread_restore_ready(
                    thread_id,
                    strict_authority,
                ):
                    logger.error(
                        "Strict terminal restore authority changed before Ready "
                        "commit for thread %s",
                        thread_id,
                    )
                    return False

                logger.info(
                    "Strict terminal workspace restored for thread %s from %s "
                    "(runtime=%s)",
                    thread_id,
                    "its own volume" if volume_reattached else "S3",
                    strict_authority.runtime_incarnation,
                )
                return True

            # Legacy job/VM/pinned-session semantics remain below. Strict
            # terminal restores returned only after the transactional CAS.
            if not is_vm:
                if (
                    restored_runtime is None
                    or not await self._container_provisioner.workspace_pod_live(
                        owner,
                        expected_runtime_incarnation=restored_runtime,
                    )
                ):
                    return False
            restored_ctx = {
                "status": "ready",
                "restored_at": datetime.now(timezone.utc).isoformat(),
            }
            if not is_vm:
                restored_ctx[WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY] = False
            if is_vm:
                await self._db.merge_thread_vm_context(thread_id, restored_ctx)
            else:
                if (
                    restored_runtime is None
                    or not await self._db.merge_thread_workspace_context_if_runtime(
                        thread_id,
                        restored_ctx,
                        expected_runtime_incarnation=restored_runtime,
                    )
                ):
                    return False

            logger.info(
                "Workspace restored for thread %s from %s (ssh_host=%s)",
                thread_id,
                "its own volume" if volume_reattached else "S3",
                ssh_host,
            )
            return True

        except Exception:
            logger.exception("Failed to restore workspace for thread %s", thread_id)
            if strict_terminal_snapshot:
                # Keep the exact-true marker armed. A racing replacement must
                # not be stamped failed (or Ready) by this stale restore.
                return False
            if is_vm:
                await self._db.merge_thread_vm_context(
                    thread_id, {"status": "failed", "error": "restore exception"}
                )
            elif restored_runtime is not None:
                await self._db.merge_thread_workspace_context_if_runtime(
                    thread_id,
                    {"status": "failed", "error": "restore exception"},
                    expected_runtime_incarnation=restored_runtime,
                )
            return False

    async def check_idle_threads(self) -> int:
        """Sweep idle thread workspaces (containers + VMs) and suspend them.

        A thread workspace is idle if:
        - Thread status is 'ended' (no active WebSocket; agent detached or orphaned)
        - Workspace or VM status is 'ready'
        - last_activity is older than idle_timeout_minutes

        Returns the count of workspaces suspended.
        """
        if not self.is_enabled or not self._db:
            return 0

        suspended_count = 0
        timeout = self.idle_timeout_minutes
        now = datetime.now(timezone.utc)

        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, metadata, last_activity
                    FROM threads
                    WHERE status = 'ended'
                      AND (
                          metadata->'workspace_container'->>'status' = 'ready'
                          OR metadata->'vm'->>'status' = 'ready'
                      )
                    """,
                )
        except Exception:
            logger.exception("Failed to query ended thread workspace containers")
            return 0

        for row in rows:
            thread_id = str(row["id"])
            last_activity = row.get("last_activity", now)

            if last_activity and last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)

            idle_minutes = (now - last_activity).total_seconds() / 60

            if idle_minutes >= timeout:
                logger.info(
                    "Suspending idle workspace for thread %s (idle %.0fm, timeout %dm)",
                    thread_id,
                    idle_minutes,
                    timeout,
                )
                ok = await self.suspend_thread_workspace(thread_id)
                if ok:
                    suspended_count += 1

        return suspended_count

    # =========================================================================
    # Idle sweep (jobs)
    # =========================================================================

    async def check_idle_all(self) -> int:
        """Sweep all ready workspaces (containers + VMs) and suspend idle ones.

        A workspace is idle if:
        - The job is in paused/pending_review/waiting_for_reply status
        - last_activity (or updated_at) is older than idle_timeout_minutes

        Returns the count of workspaces suspended.
        """
        if not self.is_enabled or not self._db:
            return 0

        suspended_count = 0
        timeout = self.idle_timeout_minutes
        now = datetime.now(timezone.utc)

        try:
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, context, updated_at
                    FROM jobs
                    WHERE status IN ('paused', 'pending_review', 'waiting_for_reply')
                      AND (
                          context->'workspace_container'->>'status' = 'ready'
                          OR context->'vm'->>'status' = 'ready'
                      )
                    """,
                )
        except Exception:
            logger.exception("Failed to query idle workspaces")
            return 0

        for row in rows:
            job_id = str(row["id"])
            ctx = row.get("context") or {}
            if isinstance(ctx, str):
                try:
                    ctx = json.loads(ctx)
                except (ValueError, TypeError):
                    ctx = {}
            if not isinstance(ctx, dict):
                ctx = {}

            # Check both workspace_container and vm contexts
            ws_ctx = ctx.get("workspace_container", {})
            vm_ctx = ctx.get("vm", {})
            last_activity_str = ws_ctx.get("last_activity") or vm_ctx.get(
                "last_activity"
            )

            if last_activity_str:
                try:
                    last_activity = datetime.fromisoformat(last_activity_str)
                except (ValueError, TypeError):
                    last_activity = row.get("updated_at", now)
            else:
                last_activity = row.get("updated_at", now)

            if last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)

            idle_minutes = (now - last_activity).total_seconds() / 60

            if idle_minutes >= timeout:
                logger.info(
                    "Suspending idle workspace for job %s (idle %.0fm, timeout %dm)",
                    job_id,
                    idle_minutes,
                    timeout,
                )
                ok = await self.suspend_workspace(job_id)
                if ok:
                    suspended_count += 1

        return suspended_count


# Module-level singleton
workspace_suspension_service = WorkspaceSuspensionService()
